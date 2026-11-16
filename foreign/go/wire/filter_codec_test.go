package wire_test

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// codecCorpus mirrors wire/fixtures/filter_codec_cases.json: the shared
// decoder corpus every client's payload decoders must agree on.
type codecCorpus struct {
	Schemas []jsonSchemaDef `json:"schemas"`
	Cases   []codecCase     `json:"cases"`
}

type jsonSchemaDef struct {
	ID     uint32 `json:"id"`
	Source struct {
		Kind          string `json:"kind"`
		Schema        string `json:"schema"`
		DescriptorSet []byte `json:"descriptor_set"`
		MessageType   string `json:"message_type"`
	} `json:"source"`
}

type codecCase struct {
	Name            string       `json:"name"`
	Filter          jsonFilter   `json:"filter"`
	PayloadHex      string       `json:"payload_hex"`
	Headers         []jsonHeader `json:"headers"`
	Expected        string       `json:"expected"`
	Fault           *string      `json:"fault"`
	MaxDepth        int          `json:"max_depth"`
	MaxPayloadBytes int          `json:"max_payload_bytes"`
}

func (def jsonSchemaDef) schemaDef(t *testing.T) wire.SchemaDef {
	t.Helper()
	switch def.Source.Kind {
	case "avro":
		return wire.SchemaDef{ID: def.ID, Source: wire.AvroSchema(def.Source.Schema)}
	case "protobuf":
		return wire.SchemaDef{ID: def.ID, Source: wire.ProtobufSchema(
			def.Source.DescriptorSet, def.Source.MessageType)}
	default:
		t.Fatalf("the corpus uses an unported schema source %q", def.Source.Kind)
		return wire.SchemaDef{}
	}
}

// The decoder corpus is behaviour, not encoding: a record written in Avro,
// Protobuf or CBOR must read as the same values in every client, and the same
// payloads must fault for the same reasons. A divergence here means a Go
// consumer reads a different field value than a Rust one from the same bytes.
func TestGivenTheFilterCodecCorpusWhenDecodedThenShouldMatchEveryVerdict(t *testing.T) {
	var corpus codecCorpus
	if err := json.Unmarshal(readFixture(t, "filter_codec_cases.json"), &corpus); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}
	if len(corpus.Cases) == 0 {
		t.Fatal("the corpus lists cases")
	}
	schemas := make([]wire.SchemaDef, len(corpus.Schemas))
	for index, def := range corpus.Schemas {
		schemas[index] = def.schemaDef(t)
	}

	for _, testCase := range corpus.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			payload, err := hex.DecodeString(testCase.PayloadHex)
			if err != nil {
				t.Fatalf("decode the payload: %v", err)
			}
			compiled, err := wire.CompileFilterWithSchemas(
				testCase.Filter.consumerFilter(t), schemas)
			if err != nil {
				t.Fatalf("compile the filter: %v", err)
			}
			headers := make([]wire.FilterHeader, len(testCase.Headers))
			for index, header := range testCase.Headers {
				headers[index] = wire.FilterHeader{
					Key: header.Key, Value: header.Value.headerScalar(t),
				}
			}
			limits := wire.DecodeLimits{
				MaxPayloadBytes: testCase.MaxPayloadBytes, MaxDepth: testCase.MaxDepth,
			}

			verdict, fault := compiled.EvaluateWithFault(
				wire.FilterRecord{Payload: payload, Headers: headers}, limits)
			if string(verdict) != testCase.Expected {
				t.Errorf("verdict is %q, the corpus expects %q", verdict, testCase.Expected)
			}
			if testCase.Fault != nil && string(fault) != *testCase.Fault {
				t.Errorf("fault is %q, the corpus expects %q", fault, *testCase.Fault)
			}
		})
	}
}

// A filter must not compile against a schema set that cannot serve it, so a
// missing or mismatched writer schema is a configuration error rather than a
// per-record surprise.
func TestGivenAFilterNamingAnAbsentSchemaWhenCompiledThenShouldRefuseIt(t *testing.T) {
	filter := wire.NewConsumerFilter(
		wire.ExprPredicate("counter", wire.CmpEq, wire.LongTyped(1)), wire.CodecAvro)
	filter.SchemaRefs = []uint32{9}

	if _, err := wire.CompileFilterWithSchemas(filter, nil); err == nil {
		t.Fatal("a filter naming an unsupplied schema must be refused")
	}

	// A schema of the wrong family for the codec is refused too.
	wrongFamily := []wire.SchemaDef{{
		ID: 9, Source: wire.ProtobufSchema([]byte{}, "fleet.Change"),
	}}
	if _, err := wire.CompileFilterWithSchemas(filter, wrongFamily); err == nil {
		t.Fatal("a schema that does not match the codec must be refused")
	}
}

package wire_test

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// writerSchemaCorpus mirrors wire/fixtures/writer_schema_cases.json: the
// shared record-decode corpus every client must agree on.
type writerSchemaCorpus struct {
	Schemas []jsonSchemaDef `json:"schemas"`
	Cases   []struct {
		Name       string           `json:"name"`
		Schema     uint32           `json:"schema"`
		PayloadHex string           `json:"payload_hex"`
		Expected   *json.RawMessage `json:"expected"`
		Fault      string           `json:"fault"`
		Refused    bool             `json:"refused"`
	} `json:"cases"`
}

// The record-decode corpus is behaviour, not encoding: the same body under the
// same writer schema must decode to the same value, or fail with the same
// fault, in every client. A divergence here means a Go projector materializes
// a row a Rust projector rejects, or reads a different value from one it
// accepts.
func TestGivenTheWriterSchemaCorpusWhenDecodedThenShouldMatchEveryValueAndFault(t *testing.T) {
	var corpus writerSchemaCorpus
	if err := json.Unmarshal(readFixture(t, "writer_schema_cases.json"), &corpus); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}
	if len(corpus.Cases) == 0 {
		t.Fatal("the corpus lists cases")
	}

	sources := make(map[uint32]wire.SchemaSource, len(corpus.Schemas))
	for _, definition := range corpus.Schemas {
		sources[definition.ID] = definition.writerSource(t)
	}

	for _, testCase := range corpus.Cases {
		t.Run(testCase.Name, func(t *testing.T) {
			source, named := sources[testCase.Schema]
			if !named {
				t.Fatalf("the case names schema %d, which the corpus does not define",
					testCase.Schema)
			}

			// A refused case must not compile at all. A JSON Schema has no
			// writer decoder, so its refusal is its own validation.
			if testCase.Refused {
				var err error
				if source.Kind == wire.SchemaJSON {
					err = source.Validate()
				} else {
					_, err = wire.CompileWriterSchema(source)
				}
				if err == nil {
					t.Error("the corpus refuses this schema")
				}
				return
			}

			// A JSON Schema validates payloads rather than describing how they
			// were written, so the corpus only asks that its source validates.
			if source.Kind == wire.SchemaJSON {
				if err := source.Validate(); err != nil {
					t.Errorf("the corpus schema must validate: %v", err)
				}
				return
			}

			compiled, err := wire.CompileWriterSchema(source)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			payload, err := hex.DecodeString(testCase.PayloadHex)
			if err != nil {
				t.Fatalf("the payload is hex: %v", err)
			}

			decoded, fault := compiled.Decode(payload, wire.RecordDecodeLimits)
			switch {
			case testCase.Fault != "":
				if string(fault) != testCase.Fault {
					t.Errorf("want the fault %q, got %q", testCase.Fault, fault)
				}
			case testCase.Expected != nil:
				if fault != "" {
					t.Fatalf("want a value, got the fault %q", fault)
				}
				want := canonicalJSON(t, *testCase.Expected)
				if got := decoded.JSON(); got != want {
					t.Errorf("decoded differently:\n got %s\nwant %s", got, want)
				}
			default:
				t.Fatal("the case names neither a value nor a fault")
			}
		})
	}
}

// canonicalJSON renders an expected value the way a decode renders itself:
// object keys sorted, numbers in the text they were written as. Comparing two
// renderings is how one decode is checked against another without a float
// losing a 64-bit integer on the way.
func canonicalJSON(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	decoded, fault := wire.DecodeJSONPayload(raw, wire.RecordDecodeLimits)
	if fault != "" {
		t.Fatalf("the expected value is not valid JSON: %q", fault)
	}
	return decoded.JSON()
}

// writerSource reads one corpus schema source, including the JSON Schema kind
// the codec corpus never names.
func (def jsonSchemaDef) writerSource(t *testing.T) wire.SchemaSource {
	t.Helper()
	if def.Source.Kind == "json_schema" {
		return wire.JSONSchema(def.Source.Schema)
	}
	return def.schemaDef(t).Source
}

// The shared JSON Schema corpus. Each case is a schema plus the values it
// accepts and rejects.
//
// Validating those values needs a JSON Schema implementation, which is an SDK
// concern: the contract crate validates the schema source (its dialect and its
// bounds) and leaves the values to the client that compiles it. This port has
// no SDK yet, so it asserts the half it owns, and the accepted and rejected
// values wait for the compiled-schema surface.
func TestGivenTheSchemaValidationCorpusWhenCheckedThenEverySchemaShouldValidateAsASource(
	t *testing.T,
) {
	var cases []struct {
		Name   string          `json:"name"`
		Schema json.RawMessage `json:"schema"`
	}
	if err := json.Unmarshal(readFixture(t, "schema_validation_cases.json"), &cases); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}
	if len(cases) == 0 {
		t.Fatal("the corpus lists cases")
	}

	for _, testCase := range cases {
		t.Run(testCase.Name, func(t *testing.T) {
			source := wire.JSONSchema(string(testCase.Schema))
			if err := source.Validate(); err != nil {
				t.Errorf("a corpus schema must register: %v", err)
			}
			// A JSON Schema describes how to validate a payload rather than
			// how one was written, so it never compiles to a writer decoder.
			if _, err := wire.CompileWriterSchema(source); err == nil {
				t.Error("a JSON Schema has no writer decoder")
			}
		})
	}
}

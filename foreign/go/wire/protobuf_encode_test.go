package wire_test

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenTheCodecCorpusWhenADecodedProtobufBodyIsEncodedThenShouldDecodeToTheSameValue(t *testing.T) {
	var corpus codecCorpus
	if err := json.Unmarshal(readFixture(t, "filter_codec_cases.json"), &corpus); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}
	schemas := map[uint32]*wire.WriterSchema{}
	for _, def := range corpus.Schemas {
		if def.Source.Kind != "protobuf" {
			continue
		}
		compiled, err := wire.CompileWriterSchema(def.schemaDef(t).Source)
		if err != nil {
			t.Fatalf("compile schema %d: %v", def.ID, err)
		}
		schemas[def.ID] = compiled
	}
	encoded := 0
	for _, testCase := range corpus.Cases {
		if testCase.Filter.Codec != "protobuf" || len(testCase.Filter.SchemaRefs) != 1 {
			continue
		}
		schema := schemas[testCase.Filter.SchemaRefs[0]]
		payload, err := hex.DecodeString(testCase.PayloadHex)
		if err != nil {
			t.Fatalf("%s: decode the payload: %v", testCase.Name, err)
		}
		decoded, fault := schema.Decode(payload, wire.RecordDecodeLimits)
		if fault != "" {
			continue
		}
		descriptor, _ := schema.Protobuf()

		again, err := wire.EncodeProtobufJSON(descriptor, []byte(decoded.JSON()))

		if err != nil {
			t.Fatalf("%s: encode %s: %v", testCase.Name, decoded.JSON(), err)
		}
		back, fault := schema.Decode(again, wire.RecordDecodeLimits)
		if fault != "" || back.JSON() != decoded.JSON() {
			t.Fatalf("%s: want %s back, got %s (%s)", testCase.Name, decoded.JSON(), back.JSON(), fault)
		}
		encoded++
	}
	if encoded < 5 {
		t.Fatalf("want the corpus to exercise the encoder, encoded %d bodies", encoded)
	}
}

func TestGivenAProtobufSchemaWhenABodyIsEncodedThenShouldWriteTheWireBytesOfItsFields(t *testing.T) {
	descriptor := corpusProtobufDescriptor(t, 2)

	body, err := wire.EncodeProtobufJSON(descriptor, []byte(`{"counter":42,"mode":"safe"}`))

	if err != nil || hex.EncodeToString(body) != "082a120473616665" {
		t.Fatalf("want the bytes 082a120473616665, got %x, %v", body, err)
	}
}

func TestGivenABodyThatDoesNotFitWhenEncodedAsProtobufThenShouldNameTheField(t *testing.T) {
	descriptor := corpusProtobufDescriptor(t, 2)
	for name, body := range map[string]string{
		"text in an integer field":     `{"counter":"many"}`,
		"a fraction in an integer":     `{"counter":1.5}`,
		"a number in a string field":   `{"mode":7}`,
		"an object in a list field":    `{"values":{"a":1}}`,
		"a body that is not a message": `[1,2]`,
	} {
		_, err := wire.EncodeProtobufJSON(descriptor, []byte(body))

		if err == nil {
			t.Errorf("%s: want a refusal", name)
			continue
		}
		if name != "a body that is not a message" && !strings.Contains(err.Error(), "`") {
			t.Errorf("%s: want the field named, got %v", name, err)
		}
	}
}

func TestGivenAFieldTheSchemaDoesNotDeclareWhenEncodedAsProtobufThenShouldLeaveItOut(t *testing.T) {
	descriptor := corpusProtobufDescriptor(t, 2)

	body, err := wire.EncodeProtobufJSON(descriptor, []byte(`{"counter":42,"note":"extra","mode":null}`))

	if err != nil || hex.EncodeToString(body) != "082a" {
		t.Fatalf("want only the declared field written, got %x, %v", body, err)
	}
}

func corpusProtobufDescriptor(t *testing.T, id uint32) protoreflect.MessageDescriptor {
	t.Helper()
	var corpus codecCorpus
	if err := json.Unmarshal(readFixture(t, "filter_codec_cases.json"), &corpus); err != nil {
		t.Fatalf("decode the corpus: %v", err)
	}
	for _, def := range corpus.Schemas {
		if def.ID != id {
			continue
		}
		compiled, err := wire.CompileWriterSchema(def.schemaDef(t).Source)
		if err != nil {
			t.Fatalf("compile schema %d: %v", id, err)
		}
		descriptor, ok := compiled.Protobuf()
		if !ok {
			t.Fatalf("schema %d is not Protobuf", id)
		}
		return descriptor
	}
	t.Fatalf("the corpus has no schema %d", id)
	return nil
}

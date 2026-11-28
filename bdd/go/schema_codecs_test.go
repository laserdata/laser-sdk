package bdd_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedSchemaCodecScenariosWhenRunThenShouldHold runs the writer
// schema feature.
//
// A writer schema accepts exactly one complete payload. Accepting a trailing
// byte, or reading a different value from a body another client accepts, would
// mean a Go projector materializes a row the Rust projector rejects.
func TestGivenTheSharedSchemaCodecScenariosWhenRunThenShouldHold(t *testing.T) {
	runLocalFeature(t, "schema_codecs.feature", time.Minute, schemaCodecStep)
}

// corpusPath is the shared writer schema corpus, read from the fixture
// directory the Rust crate owns so every client decodes the same bytes.
const corpusPath = "../../wire/fixtures/writer_schema_cases.json"

type schemaCorpus struct {
	Schemas []corpusSchema `json:"schemas"`
	Cases   []struct {
		Name       string           `json:"name"`
		Schema     uint32           `json:"schema"`
		PayloadHex string           `json:"payload_hex"`
		Expected   *json.RawMessage `json:"expected"`
		Fault      string           `json:"fault"`
		Refused    bool             `json:"refused"`
	} `json:"cases"`
}

type corpusSchema struct {
	ID     uint32 `json:"id"`
	Source struct {
		Kind          string `json:"kind"`
		Schema        string `json:"schema"`
		DescriptorSet []byte `json:"descriptor_set"`
		MessageType   string `json:"message_type"`
	} `json:"source"`
}

func (definition corpusSchema) source() (wire.SchemaSource, error) {
	switch definition.Source.Kind {
	case "avro":
		return wire.AvroSchema(definition.Source.Schema), nil
	case "json_schema":
		return wire.JSONSchema(definition.Source.Schema), nil
	case "protobuf":
		return wire.ProtobufSchema(
			definition.Source.DescriptorSet, definition.Source.MessageType), nil
	default:
		return wire.SchemaSource{}, fmt.Errorf(
			"the corpus names the schema kind %q", definition.Source.Kind)
	}
}

func schemaCodecStep(_ context.Context, state *world, step bdd.Step) error {
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "a \"") && strings.HasSuffix(text, "\" writer schema"):
		return chooseWriterSchema(state, arguments)

	case strings.HasPrefix(text, "I decode schema payload"):
		return decodePayload(state, arguments)

	case strings.HasPrefix(text, "the schema payload is "):
		return checkOutcome(state, strings.TrimPrefix(text, "the schema payload is "))

	case text == "I decode every case of the shared writer schema corpus":
		return decodeCorpus(state)

	case text == "every corpus case matches its expected value":
		failures, found := state.get("corpus failures")
		if !found {
			return fmt.Errorf("the corpus was never decoded")
		}
		if names := failures.([]string); len(names) != 0 {
			return fmt.Errorf("these cases decoded differently: %s",
				strings.Join(names, ", "))
		}
		return nil

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

func chooseWriterSchema(state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one codec, got %v", arguments)
	}
	var source wire.SchemaSource
	switch arguments[0] {
	case "avro_long":
		source = wire.AvroSchema(`"long"`)
	case "json_string":
		source = wire.JSONSchema(`{"type":"string"}`)
	default:
		return fmt.Errorf("the scenarios name no codec %q", arguments[0])
	}
	state.put("schema source", source)
	return nil
}

// decodePayload reads one payload under the chosen schema.
//
// A JSON Schema describes what a payload must satisfy rather than how it was
// written, so there is no writer decoder for it and the payload is read as
// plain JSON and then checked against the schema.
func decodePayload(state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one payload, got %v", arguments)
	}
	value, found := state.get("schema source")
	if !found {
		return fmt.Errorf("the scenario chose no writer schema")
	}
	source := value.(wire.SchemaSource)

	var numbers []byte
	if err := json.Unmarshal([]byte(arguments[0]), &numbers); err != nil {
		return fmt.Errorf("the payload is a byte list: %w", err)
	}

	compiled, err := laser.CompileSchema(wire.SchemaDef{ID: 1, Source: source})
	if err != nil {
		return err
	}
	state.put("rejected", !compiled.Validate(numbers))
	return nil
}

func checkOutcome(state *world, outcome string) error {
	value, found := state.get("rejected")
	if !found {
		return fmt.Errorf("no payload was decoded")
	}
	if want := outcome == "rejected"; value.(bool) != want {
		return fmt.Errorf("want the payload %s, got the opposite", outcome)
	}
	return nil
}

// decodeCorpus walks every shared case, gathering the ones that disagree
// rather than stopping at the first, so one run names every divergence.
func decodeCorpus(state *world) error {
	raw, err := os.ReadFile(filepath.Clean(corpusPath))
	if err != nil {
		return err
	}
	var corpus schemaCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		return err
	}
	if len(corpus.Cases) == 0 {
		return fmt.Errorf("the corpus lists no cases, so this would pass vacuously")
	}
	sources := make(map[uint32]wire.SchemaSource, len(corpus.Schemas))
	for _, definition := range corpus.Schemas {
		source, err := definition.source()
		if err != nil {
			return err
		}
		sources[definition.ID] = source
	}

	var failures []string
	for _, sample := range corpus.Cases {
		source, named := sources[sample.Schema]
		if !named {
			return fmt.Errorf("%s names schema %d, which the corpus does not define",
				sample.Name, sample.Schema)
		}
		if reason := corpusCase(source, sample.Refused, sample.Fault,
			sample.Expected, sample.PayloadHex); reason != "" {
			failures = append(failures, sample.Name+" ("+reason+")")
		}
	}
	state.put("corpus failures", failures)
	return nil
}

// corpusCase reports why one case disagreed, empty when it matched.
func corpusCase(
	source wire.SchemaSource, refused bool, fault string,
	expected *json.RawMessage, payloadHex string,
) string {
	compiled, err := laser.CompileSchema(wire.SchemaDef{ID: 1, Source: source})
	if refused {
		if err == nil {
			return "refused schema compiled"
		}
		return ""
	}
	if err != nil {
		return "compile: " + err.Error()
	}
	payload, err := hex.DecodeString(payloadHex)
	if err != nil {
		return err.Error()
	}
	var actual json.RawMessage
	err = compiled.Decode(payload, &actual)
	if fault != "" {
		if err == nil {
			return "expected a decoding failure"
		}
		if source.Kind != wire.SchemaJSON {
			writer, err := wire.CompileWriterSchema(source)
			if err != nil {
				return err.Error()
			}
			_, reason := writer.Decode(payload, wire.RecordDecodeLimits)
			if string(reason) != fault {
				return fmt.Sprintf("want %s, got %s", fault, reason)
			}
		}
		return ""
	}
	if err != nil {
		return err.Error()
	}
	if expected == nil {
		return "missing expected value"
	}
	want, faulted := wire.DecodeJSONPayload(*expected, wire.RecordDecodeLimits)
	if faulted != "" {
		return "malformed expected value"
	}
	decoded, reason := wire.DecodeJSONPayload(actual, wire.RecordDecodeLimits)
	if reason != "" || decoded.JSON() != want.JSON() {
		return fmt.Sprintf("decoded %s, expected %s", actual, *expected)
	}
	if source.Kind == wire.SchemaAvro {
		encoded, err := compiled.EncodeAvro(*expected)
		if err != nil {
			return err.Error()
		}
		var restored json.RawMessage
		if err := compiled.Decode(encoded, &restored); err != nil || !bytes.Equal(restored, actual) {
			return fmt.Sprintf("Avro encode/decode: %s, %v", restored, err)
		}
	}
	return ""
}

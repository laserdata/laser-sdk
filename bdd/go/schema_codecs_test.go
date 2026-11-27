package bdd_test

import (
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

	if source.Kind == wire.SchemaJSON {
		_, fault := wire.DecodeJSONPayload(numbers, wire.RecordDecodeLimits)
		state.put("rejected", fault != "")
		return nil
	}
	compiled, err := wire.CompileWriterSchema(source)
	if err != nil {
		return err
	}
	_, fault := compiled.Decode(numbers, wire.RecordDecodeLimits)
	state.put("rejected", fault != "")
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
	// A refused case must not compile at all. A JSON Schema has no writer
	// decoder, so its own validation is the refusal.
	if refused {
		var err error
		if source.Kind == wire.SchemaJSON {
			err = source.Validate()
		} else {
			_, err = wire.CompileWriterSchema(source)
		}
		if err == nil {
			return "the corpus refuses this schema but it compiled"
		}
		return ""
	}
	if source.Kind == wire.SchemaJSON {
		if err := source.Validate(); err != nil {
			return "the schema must validate: " + err.Error()
		}
		return ""
	}

	compiled, err := wire.CompileWriterSchema(source)
	if err != nil {
		return "compile: " + err.Error()
	}
	payload, err := hex.DecodeString(payloadHex)
	if err != nil {
		return "the payload is hex: " + err.Error()
	}
	decoded, got := compiled.Decode(payload, wire.RecordDecodeLimits)
	switch {
	case fault != "":
		if string(got) != fault {
			return fmt.Sprintf("want the fault %q, got %q", fault, got)
		}
	case expected != nil:
		if got != "" {
			return fmt.Sprintf("want a value, got the fault %q", got)
		}
		want, faulted := wire.DecodeJSONPayload(*expected, wire.RecordDecodeLimits)
		if faulted != "" {
			return "the expected value is not valid JSON"
		}
		if decoded.JSON() != want.JSON() {
			return "decoded " + decoded.JSON() + ", want " + want.JSON()
		}
	default:
		return "the case names neither a value nor a fault"
	}
	return ""
}

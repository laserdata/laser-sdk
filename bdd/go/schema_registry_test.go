package bdd_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedSchemaRegistryScenariosWhenRunThenShouldHold runs the
// per-stream writer schema registry feature: each stream numbers its schemas
// independently, and an id read in one stream never names another's.
func TestGivenTheSharedSchemaRegistryScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "schema_registry.feature", time.Minute, schemaRegistryStep)
}

// registeredSchema is one registration: the stream it went to, the name, and
// the id the registry allocated.
type registeredSchema struct {
	stream string
	name   string
	id     uint32
}

// schemaFlow is the registrations a scenario made.
type schemaFlow struct {
	registered []registeredSchema
	// second is the other stream the scenario created, kept so its own
	// registry can be read back.
	second string
}

func schemaFlowOf(state *world) *schemaFlow {
	if existing, found := state.get("schemas"); found {
		return existing.(*schemaFlow)
	}
	flow := &schemaFlow{}
	state.put("schemas", flow)
	return flow
}

// avroFor is a minimal valid Avro record schema named after the subject, so
// two registrations differ by name rather than by shape.
func avroFor(name string) string {
	return `{"type":"record","name":"` + name +
		`","fields":[{"name":"id","type":"string"}]}`
}

func schemaRegistryStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	flow := schemaFlowOf(state)
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.Contains(text, "writer schema") && strings.Contains(text, "in a second stream"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one schema name, got %v", arguments)
		}
		return registerInSecondStream(ctx, state, flow, arguments[0])

	case strings.Contains(text, "writer schema") && strings.Contains(text, "registered in the stream"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one schema name, got %v", arguments)
		}
		return registerInStream(ctx, state, flow, state.stream, arguments[0])

	case text == "both registrations answer schema id 1":
		return bothAnsweredIDOne(flow)

	case text == "each stream reads schema 1 as its own writer schema":
		return eachStreamReadsItsOwn(ctx, state, flow)

	case text == "each stream lists only its own writer schema":
		return eachStreamListsItsOwn(ctx, state, flow)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// registerInStream registers one Avro writer schema against a stream.
func registerInStream(
	ctx context.Context, state *world, flow *schemaFlow, stream, name string,
) error {
	id, err := state.client.WithDefaultStream(stream).Schemas().
		Register(wire.AvroSchema(avroFor(name))).
		Name(name).
		Send(ctx)
	if err != nil {
		return err
	}
	flow.registered = append(flow.registered,
		registeredSchema{stream: stream, name: name, id: id})
	return nil
}

// registerInSecondStream creates the other stream and registers there, which
// is what makes the two registries independent.
func registerInSecondStream(
	ctx context.Context, state *world, flow *schemaFlow, name string,
) error {
	second := state.stream + "-second"
	stream := state.client.Stream(second)
	if err := stream.Ensure(ctx); err != nil {
		return err
	}
	// Deleted with the scenario, the same way the harness disposes of its
	// own stream, so a run leaves nothing behind.
	state.t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := stream.Delete(cleanup); err != nil {
			state.t.Logf("could not delete %s: %v", second, err)
		}
	})
	flow.second = second
	return registerInStream(ctx, state, flow, second, name)
}

// bothAnsweredIDOne asserts each stream numbered its first schema from one,
// which is what "numbered independently" means.
func bothAnsweredIDOne(flow *schemaFlow) error {
	if len(flow.registered) != 2 {
		return fmt.Errorf("want two registrations, got %d", len(flow.registered))
	}
	for _, registered := range flow.registered {
		if registered.id != 1 {
			return fmt.Errorf("want schema %q in stream %q to be id 1, got %d",
				registered.name, registered.stream, registered.id)
		}
	}
	return nil
}

// eachStreamReadsItsOwn asserts id 1 reads back as that stream's own schema,
// never the other's.
func eachStreamReadsItsOwn(
	ctx context.Context, state *world, flow *schemaFlow,
) error {
	for _, registered := range flow.registered {
		held, err := state.client.WithDefaultStream(registered.stream).
			Schemas().Get(ctx, registered.id)
		if err != nil {
			return err
		}
		if held == nil {
			return fmt.Errorf("want stream %q to read schema %d, got nothing",
				registered.stream, registered.id)
		}
		if held.Schema.Name == nil || *held.Schema.Name != registered.name {
			return fmt.Errorf("want stream %q to read schema %d as %q, got %v",
				registered.stream, registered.id, registered.name, held.Schema.Name)
		}
	}
	return nil
}

// eachStreamListsItsOwn asserts a listing carries that stream's schema and
// not the other stream's.
func eachStreamListsItsOwn(
	ctx context.Context, state *world, flow *schemaFlow,
) error {
	for _, registered := range flow.registered {
		page, err := state.client.WithDefaultStream(registered.stream).
			Schemas().List(ctx, nil, 0, 50)
		if err != nil {
			return err
		}
		names := map[string]bool{}
		for _, schema := range page.Items {
			if schema.Schema.Name != nil {
				names[*schema.Schema.Name] = true
			}
		}
		if !names[registered.name] {
			return fmt.Errorf("want stream %q to list %q, got %v",
				registered.stream, registered.name, names)
		}
		for _, other := range flow.registered {
			if other.stream == registered.stream {
				continue
			}
			if names[other.name] {
				return fmt.Errorf("want stream %q not to list %q from stream %q",
					registered.stream, other.name, other.stream)
			}
		}
	}
	return nil
}

package bdd_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedCapabilityScenariosWhenRunThenShouldHold runs the
// capability negotiation feature.
//
// It is the boundary that keeps a managed call from quietly degrading: on open
// Apache Iggy every managed surface must refuse cleanly rather than fall back,
// answer partially, or fail as a transport error that says nothing about why.
func TestGivenTheSharedCapabilityScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "capabilities.feature", 2*time.Minute, capabilityStep)
}

// coded is how a caller asks a refused call for the one code every surface
// projects onto. The other clients expose this as a single function over any
// client error.
type coded interface{ ResultCode() wire.ResultCode }

func capabilityStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case text == "I read the negotiated capabilities":
		state.put("capabilities", state.client.RefreshCapabilities(ctx))
		return nil

	case text == "managed query is unavailable":
		return unavailable(state, "query", func(c laser.Capabilities) bool {
			return c.Query.Available
		})

	case text == "managed key-value is unavailable":
		return unavailable(state, "key-value", func(c laser.Capabilities) bool {
			return c.Kv.Available
		})

	case text == "forks are unavailable":
		return unavailable(state, "forks", func(c laser.Capabilities) bool {
			return c.Forks
		})

	case text == "the coordination features are unavailable":
		return coordinationUnavailable(state)

	case strings.HasPrefix(text, "I run a read-your-writes query against topic"):
		return runQuery(ctx, state, arguments, true)

	case strings.HasPrefix(text, "I run a query against topic"):
		return runQuery(ctx, state, arguments, false)

	case strings.HasPrefix(text, "I compare-and-swap key"):
		return compareAndSwap(ctx, state, arguments)

	case strings.HasPrefix(text, "I call the managed "):
		return callManaged(ctx, state, strings.TrimSuffix(
			strings.TrimPrefix(text, "I call the managed "), " surface"))

	case text == "the call fails as unsupported":
		var unsupported *laser.UnsupportedError
		if !errors.As(state.failure, &unsupported) {
			return fmt.Errorf("want an unsupported refusal, got %v", state.failure)
		}
		return nil

	case text == "the unified result code is unsupported":
		return unifiedCode(state)

	// The streaming steps this feature reuses keep one implementation, so a
	// publish behaves here exactly as it does in streaming.feature.
	case strings.HasPrefix(text, "I publish a JSON event to topic"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one topic, got %v", arguments)
		}
		return publishEvents(ctx, state, arguments[0], 1)

	case text == "the publish succeeds":
		return visible(ctx, state, 1)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

func capabilitiesOf(state *world) (laser.Capabilities, error) {
	value, found := state.get("capabilities")
	if !found {
		return laser.Capabilities{}, fmt.Errorf("the capabilities were never read")
	}
	return value.(laser.Capabilities), nil
}

func unavailable(
	state *world, surface string, available func(laser.Capabilities) bool,
) error {
	capabilities, err := capabilitiesOf(state)
	if err != nil {
		return err
	}
	if available(capabilities) {
		return fmt.Errorf("open Apache Iggy advertised the managed %s surface", surface)
	}
	return nil
}

// coordinationUnavailable checks the two bits a coordinating caller needs:
// a conditional write, and a read that sees the caller's own writes.
func coordinationUnavailable(state *world) error {
	capabilities, err := capabilitiesOf(state)
	if err != nil {
		return err
	}
	if capabilities.Kv.CAS {
		return fmt.Errorf("open Apache Iggy advertised compare-and-swap")
	}
	if capabilities.Consistency() != wire.ConsistencyEventual {
		return fmt.Errorf("want the weakest consistency, got %q",
			capabilities.Consistency())
	}
	return nil
}

func runQuery(
	ctx context.Context, state *world, arguments []string, readYourWrites bool,
) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one topic, got %v", arguments)
	}
	request := state.client.Query(arguments[0])
	if readYourWrites {
		request = request.ReadYourWrites()
	}
	_, state.failure = request.Fetch(ctx)
	return nil
}

func compareAndSwap(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want a key and a namespace, got %v", arguments)
	}
	key, namespace := arguments[0], arguments[1]
	_, state.failure = state.client.Kv(namespace).
		Set([]byte(key)).Bytes([]byte{1}).ExpectAbsent().Commit(ctx)
	return nil
}

// callManaged reaches one managed surface by the name the scenario gives it.
//
// Every one of them is expected to refuse before a round trip, so the argument
// values only have to be well-formed enough to reach the gate.
func callManaged(ctx context.Context, state *world, surface string) error {
	client := state.client
	switch surface {
	case "projection registration":
		state.failure = client.Projections().Register(ctx, wire.Projection{
			ID:          wire.ProjectionID("reading.v1"),
			Name:        "reading",
			Version:     1,
			ContentType: wire.ContentJSON,
		})
	case "materialization":
		_, state.failure = state.topic("events").Materialize().Apply(ctx)
	case "schema registration":
		_, state.failure = client.Schemas().
			Register(wire.JSONSchema(`{"type":"string"}`)).Send(ctx)
	case "graph upsert":
		state.failure = client.Graph("kg").Upsert(ctx, nil, nil)
	case "fork creation":
		_, state.failure = client.Fork("experiment").Create().Send(ctx)
	case "change feed":
		_, state.failure = client.Watch().Records(ctx)
	case "session read":
		_, state.failure = client.Sessions().Get(ctx, laser.NewConversationID())
	case "lease acquisition":
		// Kv.Lease is the managed lease acquire. This step was plan-skipped
		// for want of it, and stayed skipped after it shipped: the claim
		// was checked again and is no longer true.
		_, state.failure = client.Kv("locks").
			Lease(ctx, []byte("a-key"), "holder", time.Minute)
	case "batch":
		return blocked("the Go client has no managed batch surface: " +
			"Topic.Batch is a plain multi-record publish, not the batch command")
	default:
		return fmt.Errorf("this runner does not reach the %q surface", surface)
	}
	return nil
}

// unifiedCode asks the refusal for the shared code it projects onto.
func unifiedCode(state *world) error {
	if state.failure == nil {
		return fmt.Errorf("no call failed")
	}
	var carrier coded
	if !errors.As(state.failure, &carrier) {
		// UnsupportedError, CommandFailure, EdgeDenial, FilterError,
		// SessionError and StaleLaneError all answer a code. A refusal that
		// answers none is a gap in whichever error type this surface
		// raised, named here rather than passed over.
		return blocked(fmt.Sprintf("this refusal projects no unified result "+
			"code: %T has no ResultCode method", state.failure))
	}
	if got := carrier.ResultCode(); got != wire.ResultUnsupported {
		return fmt.Errorf("want the unsupported code, got %v", got)
	}
	return nil
}

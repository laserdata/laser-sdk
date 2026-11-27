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

// TestGivenTheSharedConsistencyScenariosWhenRunThenShouldHold runs the
// read-your-writes feature: every managed write reply carries the position
// its fold applied, the client keeps the highest per surface, and a later
// read of that surface carries it so the answering node has applied at
// least this client's own writes.
func TestGivenTheSharedConsistencyScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "consistency.feature", time.Minute, consistencyStep)
}

// consistencyFlow is the key-value position a scenario noted, so a later
// step can check the next write moved past it.
type consistencyFlow struct {
	noted    *wire.MutationPosition
	hasNoted bool
}

func consistencyFlowOf(state *world) *consistencyFlow {
	if existing, found := state.get("consistency"); found {
		return existing.(*consistencyFlow)
	}
	flow := &consistencyFlow{}
	state.put("consistency", flow)
	return flow
}

func consistencyStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	flow := consistencyFlowOf(state)
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "I write ") && strings.Contains(text, "managed key"):
		if len(arguments) != 3 {
			return fmt.Errorf("want a value, a key and a namespace, got %v", arguments)
		}
		return writeManagedKey(ctx, state, arguments[0], arguments[1], arguments[2])

	case text == "I note the client's key-value position":
		position := state.client.SeenPositions().Kv
		if position == nil {
			return fmt.Errorf("want a key-value position to note, got none")
		}
		noted := *position
		flow.noted, flow.hasNoted = &noted, true
		return nil

	case text == "the client has seen a key-value position":
		if state.client.SeenPositions().Kv == nil {
			return fmt.Errorf("want the write to have given the client a position")
		}
		return nil

	case text == "the client's key-value position moved past the noted one":
		return positionMovedPast(state, flow)

	case strings.HasPrefix(text, "the managed key") && strings.Contains(text, " reads "):
		if len(arguments) != 3 {
			return fmt.Errorf("want a key, a namespace and a value, got %v", arguments)
		}
		return managedKeyReads(ctx, state, arguments[0], arguments[1], arguments[2])

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

func writeManagedKey(
	ctx context.Context, state *world, value, key, namespace string,
) error {
	return state.client.Kv(namespace).Set([]byte(key)).Bytes([]byte(value)).Send(ctx)
}

func managedKeyReads(
	ctx context.Context, state *world, key, namespace, expected string,
) error {
	held, found, err := state.client.Kv(namespace).Get(ctx, []byte(key))
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("want the key %q to hold %q, it is absent", key, expected)
	}
	if string(held) != expected {
		return fmt.Errorf("want the key %q to read %q, got %q", key, expected, held)
	}
	return nil
}

// positionMovedPast checks the second write moved the client's own frontier
// forward, which is what makes a later read carry the newer position.
func positionMovedPast(state *world, flow *consistencyFlow) error {
	if !flow.hasNoted {
		return fmt.Errorf("the scenario noted no earlier position")
	}
	current := state.client.SeenPositions().Kv
	if current == nil {
		return fmt.Errorf("want a current key-value position, got none")
	}
	// Comparable reports whether the two sit in one generation at all: a
	// recreated topic restarts the offsets, so positions across a
	// generation boundary are not ordered rather than being out of order.
	earlier, comparable := flow.noted.Precedes(*current)
	if !comparable {
		return fmt.Errorf("want both positions in one topic generation, got %+v and %+v",
			*flow.noted, *current)
	}
	if !earlier {
		return fmt.Errorf("want the position to move past %+v, got %+v",
			*flow.noted, *current)
	}
	return nil
}

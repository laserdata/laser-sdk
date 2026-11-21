package bdd_test

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// TestGivenTheSharedScratchStreamScenariosWhenRunThenShouldHold runs the
// scratch stream feature.
//
// A scratch stream gives one program run its own name, so repeated runs never
// collide and never leave data behind.
func TestGivenTheSharedScratchStreamScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "scratch_stream.feature", time.Minute, scratchStreamStep)
}

// scratchName is the shape a scratch name must have: the prefix, a hyphen and
// eight lowercase hex characters.
var scratchName = regexp.MustCompile(`^(.+)-([0-9a-f]{8})$`)

func scratchStreamStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "I open two scratch streams"):
		return openTwoScratch(ctx, state, arguments)

	case strings.HasPrefix(text, "I open a scratch stream"):
		return openScratch(ctx, state, arguments)

	case strings.HasPrefix(text, "the scratch stream is named"):
		return checkScratchName(state, arguments)

	case text == "the scratch stream exists":
		return scratchExists(ctx, state, true)

	case text == "the scratch handle's default stream is the scratch stream":
		return scratchIsDefault(state)

	case text == "I delete the scratch stream":
		scratch, err := scratchOf(state)
		if err != nil {
			return err
		}
		return scratch.Delete(ctx)

	case text == "the scratch stream no longer exists":
		return scratchExists(ctx, state, false)

	case text == "deleting the scratch stream again succeeds":
		scratch, err := scratchOf(state)
		if err != nil {
			return err
		}
		// A delete that failed because the stream is already gone would make
		// cleanup unsafe to retry after a partial failure.
		return scratch.Delete(ctx)

	case text == "the two scratch streams have different names":
		return differentScratchNames(state)

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

func scratchOf(state *world) (*laser.ScratchStream, error) {
	value, found := state.get("scratch")
	if !found {
		return nil, fmt.Errorf("the scenario opened no scratch stream")
	}
	return value.(*laser.ScratchStream), nil
}

// openScratch opens one scratch stream and arranges for it to go away.
//
// Nothing deletes it when the handle falls out of scope, because Go has no
// destructor, so the cleanup is explicit here exactly as a caller's would be.
func openScratch(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one prefix, got %v", arguments)
	}
	scratch, err := state.client.ScratchStream(ctx, arguments[0])
	if err != nil {
		return err
	}
	state.put("scratch", scratch)
	state.put("prefix", arguments[0])
	state.t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = scratch.Delete(cleanup)
	})
	return nil
}

func openTwoScratch(ctx context.Context, state *world, arguments []string) error {
	if err := openScratch(ctx, state, arguments); err != nil {
		return err
	}
	first, err := scratchOf(state)
	if err != nil {
		return err
	}
	state.put("first scratch", first)
	return openScratch(ctx, state, arguments)
}

func checkScratchName(state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one prefix, got %v", arguments)
	}
	scratch, err := scratchOf(state)
	if err != nil {
		return err
	}
	// The scenario writes the prefix with its hyphen already on it.
	prefix := strings.TrimSuffix(arguments[0], "-")
	parts := scratchName.FindStringSubmatch(scratch.Name())
	if parts == nil {
		return fmt.Errorf("the scratch name %q is not a prefix and eight "+
			"lowercase hex characters", scratch.Name())
	}
	if parts[1] != prefix {
		return fmt.Errorf("want the prefix %q, got %q", prefix, parts[1])
	}
	return nil
}

func scratchExists(ctx context.Context, state *world, want bool) error {
	scratch, err := scratchOf(state)
	if err != nil {
		return err
	}
	exists, err := state.client.Stream(scratch.Name()).Exists(ctx)
	if err != nil {
		return err
	}
	if exists != want {
		return fmt.Errorf("want the scratch stream to exist %v, got %v",
			want, exists)
	}
	return nil
}

// scratchIsDefault checks the handle addresses the scratch stream without the
// caller naming it, which is what makes a scratch stream worth having.
func scratchIsDefault(state *world) error {
	scratch, err := scratchOf(state)
	if err != nil {
		return err
	}
	if got := scratch.Laser().DefaultStream(); got != scratch.Name() {
		return fmt.Errorf("want the default stream %q, got %q",
			scratch.Name(), got)
	}
	return nil
}

func differentScratchNames(state *world) error {
	value, found := state.get("first scratch")
	if !found {
		return fmt.Errorf("the scenario opened only one scratch stream")
	}
	second, err := scratchOf(state)
	if err != nil {
		return err
	}
	if value.(*laser.ScratchStream).Name() == second.Name() {
		return fmt.Errorf("two scratch streams took one name")
	}
	return nil
}

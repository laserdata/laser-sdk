package bdd_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
)

// TestGivenTheSharedDefaultScenariosWhenRunThenShouldHold runs the defaults
// feature: a first program needs the fewest calls, and every error such a
// program meets names the call that fixes it.
func TestGivenTheSharedDefaultScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "defaults.feature", time.Minute, defaultsStep)
}

func defaultsStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)
	counts := numbersOf(step)

	switch {
	case text == "I bootstrap the stream with the defaults":
		// Each client spells the defaults its own way. Rust passes
		// TopicRetention::default(), which is a day. Go has no named default
		// and its zero value is refused outright, so the day is spelled here.
		return state.client.Bootstrap(ctx, 1, laser.ExpireAfter(24*time.Hour))

	case strings.HasPrefix(text, "topic \"") && strings.Contains(text, "has ") &&
		strings.Contains(text, "expires records after"):
		return topicDefaults(ctx, state, arguments, counts)

	case text == "a client without a default stream":
		// Connecting again rather than unsetting the stream, because a handle
		// that never had one is what a first program actually holds.
		return state.connect(ctx)

	case strings.HasPrefix(text, "I publish ") &&
		strings.HasSuffix(text, "of the default stream"):
		return publishWithoutDefault(ctx, state, arguments)

	case text == "the error names the call that sets a default stream":
		return namesDefaultStreamCall(state)

	case strings.HasPrefix(text, "the workflow "),
		text == "the error names the agent capabilities and the fixed inbox route":
		return blocked("the Go client has no workflow engine: there is no " +
			"Workflow type and no journal, so a workflow step has nothing to " +
			"run and no error to name the two fixes")

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// topicDefaults reads back what bootstrap with no arguments actually created.
func topicDefaults(
	ctx context.Context, state *world, arguments []string, counts []uint64,
) error {
	if len(arguments) != 1 || len(counts) != 2 {
		return fmt.Errorf("want a topic, a partition count and an expiry, "+
			"got %v %v", arguments, counts)
	}
	info, err := state.topic(arguments[0]).Get(ctx)
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("bootstrap created no topic %q", arguments[0])
	}
	if uint64(info.PartitionsCount) != counts[0] {
		return fmt.Errorf("want %d partitions on %s, got %d",
			counts[0], arguments[0], info.PartitionsCount)
	}
	want := time.Duration(counts[1]) * time.Second
	if info.MessageExpiry != want {
		return fmt.Errorf("want records on %s to expire after %s, got %s",
			arguments[0], want, info.MessageExpiry)
	}
	return nil
}

func publishWithoutDefault(
	ctx context.Context, state *world, arguments []string,
) error {
	if len(arguments) != 2 {
		return fmt.Errorf("want a payload and a topic, got %v", arguments)
	}
	_, state.failure = state.client.Topic(arguments[1]).
		Publish().Payload([]byte(arguments[0])).Send(ctx)
	if state.failure == nil {
		return fmt.Errorf("a publish with no default stream must not succeed")
	}
	return nil
}

// namesDefaultStreamCall checks the error tells the caller what to do next.
//
// An error that only says a stream is missing leaves a first program stuck,
// which is the whole point of this scenario.
func namesDefaultStreamCall(state *world) error {
	if state.failure == nil {
		return fmt.Errorf("no publish failed")
	}
	reason := state.failure.Error()
	for _, named := range []string{"DefaultStream", "Stream(name).Topic(name)"} {
		if !strings.Contains(reason, named) {
			return fmt.Errorf("want the error to name %q, got %q", named, reason)
		}
	}
	return nil
}

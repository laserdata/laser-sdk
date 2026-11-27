package bdd_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	bdd "github.com/laserdata/laser-sdk/bdd/go"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// TestGivenTheSharedContextWindowScenariosWhenRunThenShouldHold runs the
// context-read window feature: a read covers the newest records of each
// partition, so a record sitting behind more traffic than that is outside
// the context in every client.
//
// The timeout is generous because the scenario publishes a full window of
// traffic, which is the point of it.
func TestGivenTheSharedContextWindowScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "context_window.feature", 5*time.Minute, contextWindowStep)
}

func contextWindowStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)

	switch {
	case strings.HasPrefix(text, "agent ") && strings.Contains(text, " starts the session "):
		return startSession(ctx, state, text, arguments)

	case strings.HasPrefix(text, "the session records the message"):
		return recordMessage(ctx, state, arguments)

	case strings.HasPrefix(text, "I publish") && strings.Contains(text, "unrelated records"):
		return publishUnrelated(ctx, state, step, arguments)

	case strings.HasPrefix(text, "the session context holds only the message"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one expected message, got %v", arguments)
		}
		return contextHoldsOnly(ctx, state, arguments[0])

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// publishUnrelated fills a topic with records of other conversations, which
// is what pushes the session's own earlier record out of the read window.
//
// They are published in batches rather than one at a time: a window's worth
// of single publishes would dominate the scenario's runtime without testing
// anything the batches do not.
func publishUnrelated(
	ctx context.Context, state *world, step bdd.Step, arguments []string,
) error {
	counts := numbersOf(step)
	if len(counts) != 1 {
		return fmt.Errorf("want one record count, got %v", counts)
	}
	if len(arguments) != 1 {
		return fmt.Errorf("want one topic, got %v", arguments)
	}
	topic := state.client.Stream(state.stream).Topic(arguments[0])
	const batch = uint64(500)
	wanted := counts[0]
	for sent := uint64(0); sent < wanted; sent += batch {
		size := batch
		if remaining := wanted - sent; remaining < size {
			size = remaining
		}
		messages := make([][]byte, 0, size)
		for index := uint64(0); index < size; index++ {
			messages = append(messages, []byte(fmt.Sprintf("filler %d", sent+index)))
		}
		// Each filler rides its own conversation, so none of them belongs to
		// the session under test: they are traffic, not history.
		if err := publishFiller(ctx, topic, messages); err != nil {
			return err
		}
	}
	return nil
}

// publishFiller appends one batch of unrelated records.
//
// Through the batching producer, which is both the cheap way to fill a topic
// and what an application filling one would actually use.
func publishFiller(ctx context.Context, topic *laser.Topic, bodies [][]byte) error {
	producer := topic.Batching().Build()
	defer func() { _ = producer.Close(ctx) }()
	for _, body := range bodies {
		if err := producer.Send(ctx, body, nil); err != nil {
			return err
		}
	}
	return producer.Flush(ctx)
}

// contextWindowReadAll is a bound high enough not to narrow the read, so
// what the assertion sees is the read window's own limit rather than this
// step's.
const contextWindowReadAll = 1 << 20

// contextHoldsOnly asserts the session's assembled context holds exactly one
// message, the one that arrived after the traffic.
func contextHoldsOnly(ctx context.Context, state *world, expected string) error {
	session, err := sessionOf(state)
	if err != nil {
		return err
	}
	// Every message the window still holds, not "the last zero": Fetch's n
	// is a count, and a zero there keeps nothing rather than everything.
	messages, err := state.client.Context(session.Conversation()).
		FetchWith(ctx, []string{wire.AgentSessions}, laser.LastN(contextWindowReadAll))
	if err != nil {
		return err
	}
	var bodies []string
	for _, message := range messages {
		bodies = append(bodies, string(message.Body()))
	}
	if len(bodies) != 1 || bodies[0] != expected {
		return fmt.Errorf("want the context to hold only %q, got %v", expected, bodies)
	}
	return nil
}

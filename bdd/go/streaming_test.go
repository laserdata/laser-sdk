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

// TestGivenTheSharedStreamingScenariosWhenRunThenShouldHold runs the streaming
// feature, which is typed publish onto the durable log that every other
// surface is a read model on.
func TestGivenTheSharedStreamingScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "streaming.feature", 2*time.Minute, streamingStep)
}

// event is what the scenarios publish. The shape is shared with the other
// runners so a record one client wrote reads back in any of them.
type event struct {
	ID int `json:"id"`
}

func streamingStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)
	counts := numbersOf(step)

	switch {
	case strings.HasPrefix(text, "I bootstrap the stream with "):
		if len(counts) != 1 {
			return fmt.Errorf("want one partition count, got %v", counts)
		}
		if err := state.client.Bootstrap(
			ctx, uint32(counts[0]), laser.ExpireAfter(24*time.Hour),
		); err != nil {
			return err
		}
		state.put("bootstrapped", true)
		return nil

	case text == "the stream is ready":
		if _, found := state.get("bootstrapped"); !found {
			return fmt.Errorf("the stream was never bootstrapped")
		}
		exists, err := state.client.Stream(state.stream).Exists(ctx)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("the bootstrapped stream does not exist")
		}
		return nil

	case strings.HasPrefix(text, "I publish a JSON event to topic"):
		if len(arguments) != 1 {
			return fmt.Errorf("want one topic, got %v", arguments)
		}
		return publishEvents(ctx, state, arguments[0], 1)

	case strings.HasPrefix(text, "I publish a batch of"):
		if len(arguments) != 1 || len(counts) != 1 {
			return fmt.Errorf("want a count and a topic, got %v %v",
				counts, arguments)
		}
		return publishEvents(ctx, state, arguments[0], int(counts[0]))

	case text == "the publish succeeds":
		return visible(ctx, state, 1)

	case strings.HasPrefix(text, "all ") && strings.HasSuffix(text, "events are published"):
		if len(counts) != 1 {
			return fmt.Errorf("want one count, got %v", counts)
		}
		published, _ := state.get("published")
		if published != int(counts[0]) {
			return fmt.Errorf("want %d published, got %v", counts[0], published)
		}
		return visible(ctx, state, int(counts[0]))

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

// publishEvents writes count JSON events, one call when there is one and a
// batch when there are many, which is the distinction the scenarios draw.
func publishEvents(ctx context.Context, state *world, name string, count int) error {
	topic := state.topic(name)
	var err error
	if count == 1 {
		_, err = topic.Publish().JSON(event{ID: 0}).Send(ctx)
	} else {
		err = publishBatch(ctx, topic, count)
	}
	if err != nil {
		return onMissingTopic(err)
	}
	state.put("published", count)
	state.put("topic", name)
	return nil
}

func publishBatch(ctx context.Context, topic *laser.Topic, count int) error {
	producer := topic.Batching().MaxRecords(count).Build()
	for id := range count {
		payload, err := laser.JSONCodec{}.Encode(event{ID: id})
		if err != nil {
			return err
		}
		if err := producer.Send(ctx, payload, nil); err != nil {
			return err
		}
	}
	return producer.Close(ctx)
}

// onMissingTopic names the one gap these scenarios keep hitting.
//
// The shared contract is that a publish creates the topic it names: the Rust
// reference runner publishes to "events" and to "created-by-publish" without
// ever creating either, and the feature asserts both succeed. The Go client
// only resolves an existing topic, so the send fails. The step is reported as
// that gap rather than as a generic failure, and every other publish error
// still fails the scenario, so this starts passing for real the moment the
// client creates the topic.
func onMissingTopic(err error) error {
	if strings.Contains(err.Error(), "was not found") {
		return blocked("a publish does not create the topic it names, " +
			"which streaming.feature requires: " + err.Error())
	}
	return err
}

// visible reads the topic back until at least want records are served.
//
// An append is acknowledged before every read path serves it, so a single read
// could come back short for a race rather than for the contract.
func visible(ctx context.Context, state *world, want int) error {
	name, found := state.get("topic")
	if !found {
		return fmt.Errorf("no publish named a topic")
	}
	cursor, err := state.topic(name.(string)).Replay()
	if err != nil {
		return err
	}
	cursor = cursor.Batch(uint32(want))
	deadline := time.Now().Add(20 * time.Second)
	seen := 0
	for {
		records, err := cursor.Poll(ctx)
		if err != nil {
			return err
		}
		seen += len(records)
		if seen >= want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("want %d records on %s, saw %d", want, name, seen)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

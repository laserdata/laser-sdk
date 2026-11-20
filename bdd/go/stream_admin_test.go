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

// TestGivenTheSharedStreamAdminScenariosWhenRunThenShouldHold runs the stream
// administration feature, which every client must serve through the SDK alone
// rather than by reaching past it to the transport.
func TestGivenTheSharedStreamAdminScenariosWhenRunThenShouldHold(t *testing.T) {
	runFeature(t, "stream_admin.feature", time.Minute, streamAdminStep)
}

// adminStream is the stream these scenarios create and delete.
//
// It is named off the scenario's own fresh stream rather than being the fresh
// stream itself, because the scenarios delete it and then assert it is gone:
// deleting the one the harness cleans up would make the cleanup the thing
// under test.
func adminStream(state *world) string { return state.stream + "-admin" }

func adminTopic(state *world, name string) *laser.Topic {
	return state.client.Stream(adminStream(state)).Topic(name)
}

func streamAdminStep(ctx context.Context, state *world, step bdd.Step) error {
	handled, err := background(ctx, state, step)
	if handled {
		return err
	}
	text := step.Text
	arguments := argumentsOf(step)
	counts := numbersOf(step)

	switch {
	case strings.HasPrefix(text, "I create the unique stream with topic"):
		if len(arguments) != 1 || len(counts) != 1 {
			return fmt.Errorf("want a topic and a partition count, got %v %v",
				arguments, counts)
		}
		if err := adminTopic(state, arguments[0]).
			Ensure(ctx, uint32(counts[0]), nil); err != nil {
			return err
		}
		// The scenarios delete this stream themselves, but one that fails
		// halfway would otherwise leave it behind for every later run.
		state.t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, _ = state.client.Stream(adminStream(state)).Delete(cleanup)
		})
		return nil

	case text == "the stream listing includes the unique stream":
		return listed(ctx, state, true)

	case text == "the stream listing omits the unique stream":
		return listed(ctx, state, false)

	case strings.HasPrefix(text, "the unique stream exists with"):
		return existsWithTopics(ctx, state, counts)

	case strings.HasPrefix(text, "the unique stream lists topic"):
		return listsTopic(ctx, state, arguments, counts)

	case text == "I delete the unique stream":
		deleted, err := state.client.Stream(adminStream(state)).Delete(ctx)
		if err != nil {
			return err
		}
		if !deleted {
			return fmt.Errorf("the delete reported the stream was already gone")
		}
		return nil

	case text == "the unique stream no longer exists":
		return streamAbsent(ctx, state)

	case strings.HasPrefix(text, "I add ") && strings.Contains(text, "partitions to topic"):
		if len(arguments) != 1 || len(counts) != 1 {
			return fmt.Errorf("want a count and a topic, got %v %v",
				counts, arguments)
		}
		return adminTopic(state, arguments[0]).AddPartitions(ctx, uint32(counts[0]))

	case strings.Contains(text, "has ") && strings.HasSuffix(text, "partitions"):
		return topicPartitions(ctx, state, arguments, counts)

	case strings.HasPrefix(text, "I delete topic"):
		return deleteTopic(ctx, state, arguments, true)

	case strings.HasSuffix(text, "no longer exists") && strings.HasPrefix(text, "topic"):
		return topicAbsent(ctx, state, arguments)

	case strings.HasPrefix(text, "a second delete of topic"):
		return deleteTopic(ctx, state, arguments, false)

	case strings.Contains(text, "consumer group"):
		// Listing a topic's groups is served, but creating and deleting one
		// is not, and this feature's own narrative is that every client does
		// all of it "through the SDK alone".
		return blocked("the Go client creates and deletes no consumer group: " +
			"Topic.ConsumerGroup only joins one, and there is no delete")

	default:
		return fmt.Errorf("this runner does not implement the step")
	}
}

func listed(ctx context.Context, state *world, want bool) error {
	streams, err := state.client.Streams(ctx)
	if err != nil {
		return err
	}
	found := false
	for _, stream := range streams {
		if stream.Name == adminStream(state) {
			found = true
		}
	}
	if found != want {
		return fmt.Errorf("want listed %v, got %v", want, found)
	}
	return nil
}

func existsWithTopics(ctx context.Context, state *world, counts []uint64) error {
	if len(counts) != 1 {
		return fmt.Errorf("want one topic count, got %v", counts)
	}
	stream := state.client.Stream(adminStream(state))
	exists, err := stream.Exists(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("the unique stream does not exist")
	}
	info, err := stream.Get(ctx)
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("the unique stream reads as absent")
	}
	if uint64(info.TopicsCount) != counts[0] {
		return fmt.Errorf("want %d topics, got %d", counts[0], info.TopicsCount)
	}
	return nil
}

func listsTopic(
	ctx context.Context, state *world, arguments []string, counts []uint64,
) error {
	if len(arguments) != 1 || len(counts) != 1 {
		return fmt.Errorf("want a topic and a partition count, got %v %v",
			arguments, counts)
	}
	topics, err := state.client.Stream(adminStream(state)).Topics(ctx)
	if err != nil {
		return err
	}
	for _, info := range topics {
		if info.Name != arguments[0] {
			continue
		}
		if uint64(info.PartitionsCount) != counts[0] {
			return fmt.Errorf("want %d partitions on %s, got %d",
				counts[0], info.Name, info.PartitionsCount)
		}
		return nil
	}
	return fmt.Errorf("the listing holds no topic %q", arguments[0])
}

func streamAbsent(ctx context.Context, state *world) error {
	stream := state.client.Stream(adminStream(state))
	exists, err := stream.Exists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("the deleted stream still exists")
	}
	// A missing stream reads as absent rather than as an error, which is what
	// lets a caller ask without first knowing the answer.
	info, err := stream.Get(ctx)
	if err != nil {
		return err
	}
	if info != nil {
		return fmt.Errorf("the deleted stream still reads back: %v", info.Name)
	}
	return nil
}

func topicPartitions(
	ctx context.Context, state *world, arguments []string, counts []uint64,
) error {
	if len(arguments) != 1 || len(counts) != 1 {
		return fmt.Errorf("want a topic and a count, got %v %v", arguments, counts)
	}
	topic := adminTopic(state, arguments[0])
	info, err := topic.Get(ctx)
	if err != nil {
		return err
	}
	if info == nil {
		return fmt.Errorf("the topic %q reads as absent", arguments[0])
	}
	if uint64(info.PartitionsCount) != counts[0] {
		return fmt.Errorf("want %d partitions, got %d", counts[0], info.PartitionsCount)
	}
	// The count the topic reports and the partitions it lists must agree, or
	// one of the two is reading something that is not there.
	partitions, err := topic.Partitions(ctx)
	if err != nil {
		return err
	}
	if uint64(len(partitions)) != counts[0] {
		return fmt.Errorf("want %d partitions listed, got %d",
			counts[0], len(partitions))
	}
	return nil
}

func deleteTopic(
	ctx context.Context, state *world, arguments []string, want bool,
) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one topic, got %v", arguments)
	}
	deleted, err := adminTopic(state, arguments[0]).Delete(ctx)
	if err != nil {
		return err
	}
	if deleted != want {
		return fmt.Errorf("want the delete to report %v, got %v", want, deleted)
	}
	return nil
}

func topicAbsent(ctx context.Context, state *world, arguments []string) error {
	if len(arguments) != 1 {
		return fmt.Errorf("want one topic, got %v", arguments)
	}
	topic := adminTopic(state, arguments[0])
	exists, err := topic.Exists(ctx)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("the deleted topic still exists")
	}
	info, err := topic.Get(ctx)
	if err != nil {
		return err
	}
	if info != nil {
		return fmt.Errorf("the deleted topic still reads back")
	}
	return nil
}

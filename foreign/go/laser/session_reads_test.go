package laser_test

import (
	"context"
	"errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// readAtServer answers GetTopic with configurable metadata and PollMessages
// from a fixed set of messages, so ReadAt's own two-snapshot check (before
// the poll, and again after) can be exercised without a real deployment.
type readAtServer struct {
	clusterOfOne
	id         uint32
	generation uint64
	name       string
	missing    bool
	// recreateAfterFirstCall bumps the generation GetTopic reports starting
	// on its second call, simulating a topic recreated mid-read.
	recreateAfterFirstCall bool
	calls                  int
	messages               []iggcon.IggyMessage
}

func (server *readAtServer) Close() error                  { return nil }
func (server *readAtServer) Connect(context.Context) error { return nil }

func (server *readAtServer) GetTopic(
	_ context.Context, _, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	if server.missing {
		return nil, nil
	}
	server.calls++
	generation := server.generation
	if server.recreateAfterFirstCall && server.calls > 1 {
		generation++
	}
	details := &iggcon.TopicDetails{}
	details.Id = server.id
	details.CreatedAt = generation
	details.Name = server.name
	details.PartitionsCount = 1
	return details, nil
}

func (server *readAtServer) PollMessages(
	_ context.Context, _, _ iggcon.Identifier, _ uint32,
	_ iggcon.Consumer, strategy iggcon.PollingStrategy, _ iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	for _, message := range server.messages {
		if message.Header.Offset == strategy.Value {
			return &iggcon.PolledMessage{Messages: []iggcon.IggyMessage{message}}, nil
		}
	}
	return &iggcon.PolledMessage{}, nil
}

func readAtMessage(t *testing.T, offset uint64, payload []byte) iggcon.IggyMessage {
	t.Helper()
	message, err := iggcon.NewIggyMessage(payload)
	if err != nil {
		t.Fatalf("build a message: %v", err)
	}
	message.Header.Offset = offset
	return message
}

func TestGivenAMatchingReferenceWhenReadAtThenShouldReturnTheMessage(t *testing.T) {
	server := &readAtServer{
		id: 7, generation: 100, name: "agent.sessions",
		messages: []iggcon.IggyMessage{readAtMessage(t, 3, []byte("hello"))},
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	generation := uint64(100)

	message, err := handle.ReadAt(context.Background(), wire.NewMessageSource(wire.MessageSource{
		Stream: 1, Topic: 7, Partition: 0, Offset: 3, Generation: &generation,
	}))
	if err != nil {
		t.Fatalf("read at: %v", err)
	}
	if message == nil {
		t.Fatal("want the message, got nil")
	}
	if string(message.Body()) != "hello" {
		t.Errorf("want the payload, got %q", message.Body())
	}
	if message.Topic != "agent.sessions" {
		t.Errorf("want the topic name, got %q", message.Topic)
	}
}

func TestGivenAStaleGenerationWhenReadAtThenShouldReturnNil(t *testing.T) {
	server := &readAtServer{
		id: 7, generation: 100, name: "agent.sessions",
		messages: []iggcon.IggyMessage{readAtMessage(t, 3, []byte("hello"))},
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	stale := uint64(99)

	message, err := handle.ReadAt(context.Background(), wire.NewMessageSource(wire.MessageSource{
		Stream: 1, Topic: 7, Partition: 0, Offset: 3, Generation: &stale,
	}))
	if err != nil {
		t.Fatalf("read at: %v", err)
	}
	if message != nil {
		t.Error("want nil for a reference minted against a since-recreated topic")
	}
}

func TestGivenATopicRecreatedMidReadWhenReadAtThenShouldReturnNil(t *testing.T) {
	server := &readAtServer{
		id: 7, generation: 100, name: "agent.sessions",
		recreateAfterFirstCall: true,
		messages:               []iggcon.IggyMessage{readAtMessage(t, 3, []byte("hello"))},
	}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	message, err := handle.ReadAt(context.Background(), wire.NewMessageSource(wire.MessageSource{
		Stream: 1, Topic: 7, Partition: 0, Offset: 3,
	}))
	if err != nil {
		t.Fatalf("read at: %v", err)
	}
	if message != nil {
		t.Error("want nil when the topic's generation changed between the two checks")
	}
}

func TestGivenAMissingOffsetWhenReadAtThenShouldReturnNil(t *testing.T) {
	server := &readAtServer{id: 7, generation: 100, name: "agent.sessions"}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	message, err := handle.ReadAt(context.Background(), wire.NewMessageSource(wire.MessageSource{
		Stream: 1, Topic: 7, Partition: 0, Offset: 3,
	}))
	if err != nil {
		t.Fatalf("read at: %v", err)
	}
	if message != nil {
		t.Error("want nil for an offset nothing was polled at")
	}
}

func TestGivenAMissingTopicWhenReadAtThenShouldReturnNil(t *testing.T) {
	server := &readAtServer{missing: true}
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	message, err := handle.ReadAt(context.Background(), wire.NewMessageSource(wire.MessageSource{
		Stream: 1, Topic: 7, Partition: 0, Offset: 3,
	}))
	if err != nil {
		t.Fatalf("read at: %v", err)
	}
	if message != nil {
		t.Error("want nil when the topic is gone")
	}
}

func TestGivenANonMessageReferenceWhenReadAtThenShouldRefuse(t *testing.T) {
	handle := laser.FromClient(&readAtServer{}, laser.Options{DefaultStream: "agents"})

	_, err := handle.ReadAt(context.Background(), wire.NewKvSource("ns", "key"))

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an invalid-argument refusal, got %T: %v", err, err)
	}
}

package laser_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenASnapshotWhenResumingThenShouldStartOnePastEachFoldedOffset(t *testing.T) {
	snapshot := wire.FoldSnapshot{Stream: "agents", Fold: "plan", AsOf: []wire.SnapshotOffset{
		wire.NewSnapshotOffset(3, 100, 0, 41),
		wire.NewSnapshotOffset(3, 100, 2, 0),
		wire.NewSnapshotOffset(4, 200, 0, ^uint64(0)),
	}}

	resumed := laser.ResumeOffsets(snapshot)

	want := []wire.SnapshotOffset{
		wire.NewSnapshotOffset(3, 100, 0, 42),
		wire.NewSnapshotOffset(3, 100, 2, 1),
		wire.NewSnapshotOffset(4, 200, 0, ^uint64(0)),
	}
	if !reflect.DeepEqual(resumed, want) {
		t.Fatalf("want %+v, got %+v", want, resumed)
	}
}

func TestGivenACheckpointWhenSnapshottedThenShouldRecordEverySourceByIdAndGeneration(t *testing.T) {
	server := newSourceServer()
	client := scopeHandle(server)
	session := conversation(6)
	checkpoint := &laser.ContextCheckpoint{Offsets: map[string][]uint64{
		"agent.sessions": {5, 0, 9},
		"agent.memory":   {1},
	}}

	snapshot, err := laser.SnapshotFromCheckpoint(context.Background(), client, session, "plan", checkpoint, []byte(`{"n":1}`))

	want := &wire.FoldSnapshot{
		Stream: "agents", StreamID: 7, StreamCreatedAtMicros: 70, Conversation: session, Fold: "plan",
		AsOf: []wire.SnapshotOffset{
			wire.NewSnapshotOffset(2, 20, 0, 0),
			wire.NewSnapshotOffset(3, 30, 0, 4),
			wire.NewSnapshotOffset(3, 30, 2, 8),
		},
		State: []byte(`{"n":1}`),
	}
	if err != nil || !reflect.DeepEqual(snapshot, want) {
		t.Fatalf("want %+v, got %+v err=%v", want, snapshot, err)
	}
}

func TestGivenACheckpointOfAMissingTopicWhenSnapshottedThenShouldRefuseIt(t *testing.T) {
	client := scopeHandle(newSourceServer())
	checkpoint := &laser.ContextCheckpoint{Offsets: map[string][]uint64{"agent.gone": {1}}}

	_, err := laser.SnapshotFromCheckpoint(context.Background(), client, conversation(6), "plan", checkpoint, nil)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) || !strings.Contains(invalid.Reason, "agent.gone") {
		t.Fatalf("want the missing topic named, got %v", err)
	}
}

func TestGivenASnapshotWhenTurnedIntoACheckpointThenShouldResumeEveryPartition(t *testing.T) {
	server := newSourceServer()
	client := scopeHandle(server)
	snapshot := sourceSnapshot(conversation(6), wire.NewSnapshotOffset(3, 30, 0, 4), wire.NewSnapshotOffset(3, 30, 2, 8))

	checkpoint, err := laser.CheckpointFromSnapshot(context.Background(), client, snapshot, []string{"agent.sessions", "agent.memory"})

	want := &laser.ContextCheckpoint{Offsets: map[string][]uint64{
		"agent.sessions": {5, 0, 9},
		"agent.memory":   {0},
	}}
	if err != nil || !reflect.DeepEqual(checkpoint, want) {
		t.Fatalf("want %+v, got %+v err=%v", want, checkpoint, err)
	}
}

func TestGivenASnapshotOfARecreatedSourceWhenTurnedIntoACheckpointThenShouldRefuseIt(t *testing.T) {
	session := conversation(6)
	entry := wire.NewSnapshotOffset(3, 30, 0, 4)
	cases := map[string]struct {
		change   func(server *sourceServer, snapshot *wire.FoldSnapshot)
		topics   []string
		expected string
	}{
		"another stream": {
			change:   func(_ *sourceServer, snapshot *wire.FoldSnapshot) { snapshot.Stream = "fleet" },
			topics:   []string{"agent.sessions"},
			expected: "snapshot stream does not match the client",
		},
		"a recreated stream": {
			change:   func(server *sourceServer, _ *wire.FoldSnapshot) { server.stream.CreatedAt = 71 },
			topics:   []string{"agent.sessions"},
			expected: "snapshot stream generation changed",
		},
		"a missing stream": {
			change:   func(server *sourceServer, _ *wire.FoldSnapshot) { server.stream.Name = "" },
			topics:   []string{"agent.sessions"},
			expected: "snapshot stream no longer exists",
		},
		"a missing topic": {
			change:   func(server *sourceServer, _ *wire.FoldSnapshot) { delete(server.topics, "agent.sessions") },
			topics:   []string{"agent.sessions"},
			expected: "snapshot source topic agent.sessions is missing",
		},
		"a recreated topic": {
			change: func(server *sourceServer, _ *wire.FoldSnapshot) {
				server.topics["agent.sessions"] = iggcon.Topic{Id: 3, CreatedAt: 31, PartitionsCount: 3}
			},
			topics:   []string{"agent.sessions"},
			expected: "snapshot source topic generation changed",
		},
		"a source the fold no longer reads": {
			change:   func(*sourceServer, *wire.FoldSnapshot) {},
			topics:   []string{"agent.memory"},
			expected: "snapshot source topic generation changed",
		},
	}
	for name, scenario := range cases {
		server := newSourceServer()
		snapshot := sourceSnapshot(session, entry)
		scenario.change(server, &snapshot)

		_, err := laser.CheckpointFromSnapshot(context.Background(), scopeHandle(server), snapshot, scenario.topics)

		var invalid *laser.InvalidError
		if !errors.As(err, &invalid) || invalid.Reason != scenario.expected {
			t.Fatalf("%s: want %q, got %v", name, scenario.expected, err)
		}
	}
}

func TestGivenNoSnapshotWhenLoadingThroughAStoreThenShouldFoldTheWholeHistory(t *testing.T) {
	server := newSourceServer()
	session := conversation(6)
	for _, body := range []string{"one", "two", "three"} {
		server.appendRecord(t, session, "planner", body)
	}

	state, err := laser.LoadConversationStateWith(context.Background(), scopeHandle(server), &snapshotShelf{},
		session, []string{"agent.sessions"}, []string{"seed"}, appendBody)

	if err != nil || !reflect.DeepEqual(state, []string{"seed", "one", "two", "three"}) {
		t.Fatalf("state=%v err=%v", state, err)
	}
}

func TestGivenASnapshotWhenLoadingThroughAStoreThenShouldSeedItsStateAndFoldOnlyTheTail(t *testing.T) {
	server := newSourceServer()
	session := conversation(6)
	for _, body := range []string{"one", "two", "three"} {
		server.appendRecord(t, session, "planner", body)
	}
	snapshot := sourceSnapshot(session, wire.NewSnapshotOffset(3, 30, 0, 0))
	snapshot.State = []byte(`["from the snapshot"]`)
	shelf := &snapshotShelf{latest: &snapshot}
	scope := scopeHandle(server).Context(session)

	state, err := laser.StateWith(context.Background(), scope, shelf, []string{"agent.sessions"}, []string{"unused"}, appendBody)

	if err != nil || !reflect.DeepEqual(state, []string{"from the snapshot", "two", "three"}) {
		t.Fatalf("state=%v err=%v", state, err)
	}
	if !reflect.DeepEqual(shelf.asked, []wire.ConversationID{session}) {
		t.Fatalf("want the store asked for this conversation once, got %v", shelf.asked)
	}
}

func TestGivenASnapshotThatCannotSeedTheFoldWhenLoadingThenShouldRefuseIt(t *testing.T) {
	session := conversation(6)
	other := sourceSnapshot(conversation(9), wire.NewSnapshotOffset(3, 30, 0, 0))
	other.State = []byte(`[]`)
	broken := sourceSnapshot(session, wire.NewSnapshotOffset(3, 30, 0, 0))
	broken.State = []byte(`not json`)
	failing := errors.New("store offline")

	var invalid *laser.InvalidError
	_, err := laser.LoadConversationStateWith(context.Background(), scopeHandle(newSourceServer()),
		&snapshotShelf{latest: &other}, session, []string{"agent.sessions"}, []string(nil), appendBody)
	if !errors.As(err, &invalid) || invalid.Reason != "snapshot conversation does not match the fold" {
		t.Fatalf("want another conversation's snapshot refused, got %v", err)
	}
	var codec *laser.CodecError
	_, err = laser.LoadConversationStateWith(context.Background(), scopeHandle(newSourceServer()),
		&snapshotShelf{latest: &broken}, session, []string{"agent.sessions"}, []string(nil), appendBody)
	if !errors.As(err, &codec) {
		t.Fatalf("want an undecodable state reported as a codec error, got %v", err)
	}
	_, err = laser.LoadConversationStateWith(context.Background(), scopeHandle(newSourceServer()),
		&snapshotShelf{fails: failing}, session, []string{"agent.sessions"}, []string(nil), appendBody)
	if !errors.Is(err, failing) {
		t.Fatalf("want the store's failure reported, got %v", err)
	}
}

func appendBody(state []string, message laser.ContextMessage) []string {
	return append(state, string(message.Body()))
}

func sourceSnapshot(session wire.ConversationID, asOf ...wire.SnapshotOffset) wire.FoldSnapshot {
	return wire.FoldSnapshot{
		Stream: "agents", StreamID: 7, StreamCreatedAtMicros: 70, Conversation: session, Fold: "plan",
		AsOf: asOf, State: []byte(`null`),
	}
}

// snapshotShelf is a snapshot store holding at most one snapshot.
type snapshotShelf struct {
	latest *wire.FoldSnapshot
	fails  error
	asked  []wire.ConversationID
}

func (shelf *snapshotShelf) Latest(_ context.Context, conversation wire.ConversationID) (*wire.FoldSnapshot, error) {
	shelf.asked = append(shelf.asked, conversation)
	return shelf.latest, shelf.fails
}

func (shelf *snapshotShelf) Save(_ context.Context, snapshot wire.FoldSnapshot) error {
	shelf.latest = &snapshot
	return nil
}

// sourceServer is a feed whose stream and topics have an id and a creation time.
type sourceServer struct {
	*scopeServer
	stream iggcon.Stream
	topics map[string]iggcon.Topic
}

func newSourceServer() *sourceServer {
	return &sourceServer{
		scopeServer: newScope(),
		stream:      iggcon.Stream{Id: 7, Name: "agents", CreatedAt: 70},
		topics: map[string]iggcon.Topic{
			"agent.memory":   {Id: 2, CreatedAt: 20, PartitionsCount: 1},
			"agent.sessions": {Id: 3, CreatedAt: 30, PartitionsCount: 3},
		},
	}
}

func (server *sourceServer) GetStream(_ context.Context, _ iggcon.Identifier) (*iggcon.StreamDetails, error) {
	if server.stream.Name == "" {
		return nil, ierror.StreamNameNotFound{}
	}
	return &iggcon.StreamDetails{Stream: server.stream}, nil
}

func (server *sourceServer) GetTopic(_ context.Context, _, topic iggcon.Identifier) (*iggcon.TopicDetails, error) {
	name, err := topic.String()
	if err != nil {
		return nil, err
	}
	details, known := server.topics[name]
	if !known {
		return nil, ierror.TopicNameNotFound{}
	}
	details.Name = name
	return &iggcon.TopicDetails{Topic: details}, nil
}

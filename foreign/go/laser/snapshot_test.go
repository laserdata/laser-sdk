package laser_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// snapshotFor is one fold's checkpoint of one conversation.
func snapshotFor(
	conversation wire.ConversationID, fold, state string,
) wire.FoldSnapshot {
	return wire.FoldSnapshot{
		Stream: "agents", StreamID: 1, StreamCreatedAtMicros: 100,
		Conversation: conversation, Fold: fold,
		AsOf:  []wire.SnapshotOffset{wire.NewSnapshotOffset(2, 20, 0, 41)},
		State: []byte(state),
	}
}

// A snapshot round-trips through its bytes, which is what makes one written
// by any client readable by every other.
func TestGivenASnapshotWhenRoundTrippedThenShouldBeUnchanged(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	original := snapshotFor(conversation, "planner", "the folded state")

	encoded, err := laser.EncodeSnapshot(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := laser.DecodeSnapshot(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.Conversation != conversation || decoded.Fold != "planner" {
		t.Errorf("want the identity kept, got %+v", decoded)
	}
	if string(decoded.State) != "the folded state" {
		t.Errorf("want the state kept, got %q", decoded.State)
	}
	// The resume offsets are the point: without them the fold would replay
	// everything it already folded.
	if len(decoded.AsOf) != 1 {
		t.Fatalf("want the resume offsets kept, got %+v", decoded.AsOf)
	}
	// It resumes one past what it folded, because the record at that offset
	// is already in the state.
	if decoded.ResumeOffset(2, 20, 0) != 42 {
		t.Errorf("want it to resume past what it folded, got %d",
			decoded.ResumeOffset(2, 20, 0))
	}
	// A partition the snapshot does not name resumes from the beginning,
	// since nothing of it is in the state.
	if decoded.ResumeOffset(2, 20, 9) != 0 {
		t.Error("want an unnamed partition to resume from the beginning")
	}
}

// A snapshot the contract refuses is refused where it is encoded, rather than
// written somewhere nothing can read it back.
func TestGivenAnInvalidSnapshotWhenEncodedThenShouldRefuseIt(t *testing.T) {
	_, err := laser.EncodeSnapshot(wire.FoldSnapshot{})

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	// And bytes that are not a snapshot do not become one.
	if _, err := laser.DecodeSnapshot([]byte("not a snapshot")); err == nil {
		t.Error("want a non-snapshot refused")
	}
}

// The managed store is a point read and a point write: a conversation's
// checkpoint is one key.
func TestGivenAManagedStoreWhenSavedThenShouldReadItBack(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	store := newKeyStore()
	handle := kvHandle(store, laser.Options{DefaultStream: "agents"})
	snapshots := handle.SnapshotsInKv("planner")

	if err := snapshots.Save(context.Background(),
		snapshotFor(conversation, "planner", "folded")); err != nil {
		t.Fatalf("save: %v", err)
	}
	latest, err := snapshots.Latest(context.Background(), conversation)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}

	if latest == nil || string(latest.State) != "folded" {
		t.Fatalf("want the saved state, got %+v", latest)
	}

	// A conversation that was never snapshotted starts from the beginning
	// rather than failing.
	other := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 8})}
	missing, err := snapshots.Latest(context.Background(), other)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if missing != nil {
		t.Errorf("want nothing for an unsnapshotted conversation, got %+v", missing)
	}
}

// Saving one fold's state under another's key would resume a fold from state
// it never produced, which is worse than resuming from the beginning.
func TestGivenASnapshotOfAnotherFoldWhenSavedThenShouldRefuseIt(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	handle := kvHandle(newKeyStore(), laser.Options{DefaultStream: "agents"})

	for name, snapshot := range map[string]wire.FoldSnapshot{
		"another fold":   snapshotFor(conversation, "executor", "theirs"),
		"another stream": {Stream: "elsewhere", Fold: "planner"},
	} {
		t.Run(name, func(t *testing.T) {
			err := handle.SnapshotsInKv("planner").
				Save(context.Background(), snapshot)

			var invalid *laser.InvalidError
			if !errors.As(err, &invalid) {
				t.Fatalf("want an InvalidError, got %T: %v", err, err)
			}
		})
	}
}

// A snapshot filed under this key that names another conversation is one
// somebody moved, and resuming from it would be resuming from the wrong
// state.
func TestGivenAMisfiledSnapshotWhenReadThenShouldRefuseIt(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	other := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 8})}
	store := newKeyStore()
	handle := kvHandle(store, laser.Options{DefaultStream: "agents"})
	// Written directly, as only a mover or a forger could.
	encoded, err := laser.EncodeSnapshot(snapshotFor(other, "planner", "theirs"))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	store.put(laser.SnapshotKey("agents", conversation, "planner"), encoded)

	_, err = handle.SnapshotsInKv("planner").
		Latest(context.Background(), conversation)

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
}

// The log-native store works where there is no managed backend at all, and a
// read walks back to the newest checkpoint rather than the oldest.
func TestGivenALogStoreWhenSavedThenShouldReadBackTheNewest(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	server := newSnapshotTopic(4)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	snapshots := handle.SnapshotsOnLog("planner")

	for _, state := range []string{"first", "second", "third"} {
		if err := snapshots.Save(context.Background(),
			snapshotFor(conversation, "planner", state)); err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	latest, err := snapshots.Latest(context.Background(), conversation)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}

	if latest == nil || string(latest.State) != "third" {
		t.Fatalf("want the newest checkpoint, got %+v", latest)
	}
}

// Every save for one conversation and fold routes to one partition, which is
// the only one a read has to walk.
func TestGivenAConversationWhenSnapshottedThenShouldPinItToOnePartition(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	server := newSnapshotTopic(8)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	for range 5 {
		if err := handle.SnapshotsOnLog("planner").Save(context.Background(),
			snapshotFor(conversation, "planner", "state")); err != nil {
			t.Fatalf("save: %v", err)
		}
	}

	keys := server.written()
	if len(keys) != 5 {
		t.Fatalf("want five saves, got %d", len(keys))
	}
	for _, key := range keys {
		if string(key.Value) != string(keys[0].Value) {
			t.Error("want one conversation and fold pinned to one key")
		}
	}

	// The partition a read walks is the one the key routes to, computed the
	// way the server computes it.
	wanted := wire.PartitionForKey(keys[0].Value, 8)
	if _, err := handle.SnapshotsOnLog("planner").
		Latest(context.Background(), conversation); err != nil {
		t.Fatalf("latest: %v", err)
	}
	if read := server.lastRead(); read != wanted {
		t.Errorf("want partition %d read, got %d", wanted, read)
	}
}

// Another conversation's checkpoints are on the same topic, so a read steps
// over them rather than resuming from one.
func TestGivenAnotherConversationOnTheTopicWhenReadThenShouldStepOverIt(t *testing.T) {
	mine := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	theirs := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 8})}
	server := newSnapshotTopic(1)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	snapshots := handle.SnapshotsOnLog("planner")

	if err := snapshots.Save(context.Background(),
		snapshotFor(mine, "planner", "mine")); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := snapshots.Save(context.Background(),
		snapshotFor(theirs, "planner", "theirs")); err != nil {
		t.Fatalf("save: %v", err)
	}

	latest, err := snapshots.Latest(context.Background(), mine)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if latest == nil || string(latest.State) != "mine" {
		t.Fatalf("want this conversation's own checkpoint, got %+v", latest)
	}
}

// A topic nobody has written to yet means nothing was ever saved, which is
// not a failure: the fold starts from the beginning.
func TestGivenNoSnapshotTopicWhenReadThenShouldStartFromTheBeginning(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	server := newSnapshotTopic(1)
	server.missing = true
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	latest, err := handle.SnapshotsOnLog("planner").
		Latest(context.Background(), conversation)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if latest != nil {
		t.Errorf("want nothing, got %+v", latest)
	}

	// And a topic that exists but holds nothing.
	empty := newSnapshotTopic(1)
	if latest, err := laser.FromClient(empty,
		laser.Options{DefaultStream: "agents"}).SnapshotsOnLog("planner").
		Latest(context.Background(), conversation); err != nil || latest != nil {
		t.Errorf("want nothing from an empty topic, got %+v %v", latest, err)
	}
}

// A record on the topic that is not a snapshot is stepped over rather than
// failing the read.
func TestGivenOtherTrafficOnTheTopicWhenReadThenShouldStepOverIt(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	server := newSnapshotTopic(1)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	server.appendRaw(0, []byte("not a snapshot"))
	if err := handle.SnapshotsOnLog("planner").Save(context.Background(),
		snapshotFor(conversation, "planner", "mine")); err != nil {
		t.Fatalf("save: %v", err)
	}
	server.appendRaw(0, []byte("also not a snapshot"))

	latest, err := handle.SnapshotsOnLog("planner").
		Latest(context.Background(), conversation)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if latest == nil || string(latest.State) != "mine" {
		t.Fatalf("want the snapshot past the noise, got %+v", latest)
	}
}

// Both stores need a stream to keep their checkpoints on.
func TestGivenNoStreamWhenSnapshottingThenShouldRefuseIt(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	kv := laser.FromClient(newKeyStore(), laser.Options{})
	log := laser.FromClient(newSnapshotTopic(1), laser.Options{})

	if _, err := kv.SnapshotsInKv("planner").
		Latest(context.Background(), conversation); err == nil {
		t.Error("a managed read without a stream must be refused")
	}
	if err := kv.SnapshotsInKv("planner").Save(context.Background(),
		snapshotFor(conversation, "planner", "s")); err == nil {
		t.Error("a managed save without a stream must be refused")
	}
	if _, err := log.SnapshotsOnLog("planner").
		Latest(context.Background(), conversation); err == nil {
		t.Error("a log read without a stream must be refused")
	}
	if err := log.SnapshotsOnLog("planner").Save(context.Background(),
		snapshotFor(conversation, "planner", "s")); err == nil {
		t.Error("a log save without a stream must be refused")
	}
}

// Both backends answer the same contract, so a caller swaps one for the other
// without changing anything else.
func TestGivenEitherStoreWhenUsedThenShouldAnswerTheSameContract(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	handles := map[string]laser.SnapshotStore{
		"managed": kvHandle(newKeyStore(),
			laser.Options{DefaultStream: "agents"}).SnapshotsInKv("planner"),
		"log native": laser.FromClient(newSnapshotTopic(2),
			laser.Options{DefaultStream: "agents"}).SnapshotsOnLog("planner"),
	}

	for name, store := range handles {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if latest, err := store.Latest(ctx, conversation); err != nil ||
				latest != nil {
				t.Fatalf("want nothing yet, got %+v %v", latest, err)
			}
			if err := store.Save(ctx,
				snapshotFor(conversation, "planner", "state")); err != nil {
				t.Fatalf("save: %v", err)
			}
			latest, err := store.Latest(ctx, conversation)
			if err != nil || latest == nil {
				t.Fatalf("want it back, got %+v %v", latest, err)
			}
			if string(latest.State) != "state" {
				t.Errorf("want the saved state, got %q", latest.State)
			}
		})
	}
}

// snapshotTopic is a partitioned topic that keeps what was written to it.
type snapshotTopic struct {
	clusterOfOne
	guard      sync.Mutex
	partitions uint32
	records    map[uint32][]iggcon.IggyMessage
	keys       []iggcon.Partitioning
	missing    bool
	// silent answers every read with nothing, the way a topic that holds
	// nothing does.
	silent        bool
	readPartition uint32
	scans         int
}

func newSnapshotTopic(partitions uint32) *snapshotTopic {
	return &snapshotTopic{
		partitions: partitions,
		records:    map[uint32][]iggcon.IggyMessage{},
	}
}

func (server *snapshotTopic) Close() error { return nil }

// written is every partitioning a save carried.
func (server *snapshotTopic) written() []iggcon.Partitioning {
	server.guard.Lock()
	defer server.guard.Unlock()
	return append([]iggcon.Partitioning(nil), server.keys...)
}

// windows is how many backward windows a read walked.
func (server *snapshotTopic) windows() int {
	server.guard.Lock()
	defer server.guard.Unlock()
	return server.scans
}

// lastRead is the partition the most recent read walked.
func (server *snapshotTopic) lastRead() uint32 {
	server.guard.Lock()
	defer server.guard.Unlock()
	return server.readPartition
}

func (server *snapshotTopic) SendBinaryRequest(
	_ context.Context, _ uint32, _ []byte,
) ([]byte, error) {
	return nil, ierror.InvalidCommand{}
}

// appendRaw puts bytes on a partition directly, for traffic a snapshot store
// would never write.
func (server *snapshotTopic) appendRaw(partition uint32, payload []byte) {
	server.guard.Lock()
	defer server.guard.Unlock()
	server.place(partition, payload)
}

// place stores one payload at the partition's next offset.
func (server *snapshotTopic) place(partition uint32, payload []byte) {
	message, err := iggcon.NewIggyMessage(payload)
	if err != nil {
		panic(err)
	}
	message.Header.Offset = uint64(len(server.records[partition]))
	server.records[partition] = append(server.records[partition], message)
}

func (server *snapshotTopic) GetTopic(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	server.guard.Lock()
	defer server.guard.Unlock()
	if server.missing {
		return nil, ierror.TopicIdNotFound{}
	}
	return &iggcon.TopicDetails{
		Topic: iggcon.Topic{PartitionsCount: server.partitions},
	}, nil
}

func (server *snapshotTopic) SendMessages(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier,
	partitioning iggcon.Partitioning, messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	server.guard.Lock()
	defer server.guard.Unlock()
	server.keys = append(server.keys, partitioning)
	// The server routes a key the way this client computes it, which is the
	// property the read depends on.
	partition := wire.PartitionForKey(partitioning.Value, server.partitions)
	for _, message := range messages {
		server.place(partition, message.Payload)
	}
	return &iggcon.SendMessagesResponse{}, nil
}

func (server *snapshotTopic) PollMessages(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier, partition uint32,
	_ iggcon.Consumer, strategy iggcon.PollingStrategy, options iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	server.guard.Lock()
	defer server.guard.Unlock()
	server.readPartition = partition
	if server.silent {
		return &iggcon.PolledMessage{PartitionId: partition}, nil
	}
	if strategy.Kind != iggcon.POLLING_LAST {
		server.scans++
	}
	held := server.records[partition]
	if strategy.Kind == iggcon.POLLING_LAST {
		if len(held) == 0 {
			return &iggcon.PolledMessage{PartitionId: partition}, nil
		}
		return &iggcon.PolledMessage{
			PartitionId: partition,
			Messages:    []iggcon.IggyMessage{held[len(held)-1]},
		}, nil
	}
	var from []iggcon.IggyMessage
	for _, message := range held {
		if message.Header.Offset >= strategy.Value {
			from = append(from, message)
		}
	}
	if uint32(len(from)) > options.Count {
		from = from[:options.Count]
	}
	return &iggcon.PolledMessage{PartitionId: partition, Messages: from}, nil
}

// The exact key bytes matter beyond this client. The storage key decides
// where a checkpoint is filed and the partition key decides which partition
// it routes to, so a client spelling either differently would write a
// checkpoint where another client never looks.
func TestGivenASnapshotKeyWhenBuiltThenShouldMatchTheSharedSpelling(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}

	// The stream's own length prefixes the storage key, so a stream whose
	// name contains the separator cannot collide with another stream's.
	storage := laser.SnapshotKey("agents", conversation, "planner")
	if storage != "6:agents:"+conversation.String()+":planner" {
		t.Errorf("want the shared storage spelling, got %q", storage)
	}
	// Two streams that would run together without the prefix do not.
	first := laser.SnapshotKey("a", conversation, "f")
	second := laser.SnapshotKey("a:"+conversation.String(), conversation, "f")
	if first == second {
		t.Error("want two streams to key apart")
	}

	// The fold's own length prefixes the partition key for the same reason.
	routing := laser.SnapshotPartitionKey(conversation, "planner")
	if routing != conversation.String()+":7:planner" {
		t.Errorf("want the shared routing spelling, got %q", routing)
	}
	// Two folds route apart, so one fold's checkpoints never land where
	// another's are read.
	if laser.SnapshotPartitionKey(conversation, "planner") ==
		laser.SnapshotPartitionKey(conversation, "executor") {
		t.Error("want two folds to route apart")
	}
}

// A topic read that answers with nothing at all means nothing was ever saved,
// the same as a topic that is not there.
func TestGivenATopicThatAnswersNothingWhenReadThenShouldStartFromTheBeginning(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	server := newSnapshotTopic(1)
	server.silent = true
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})

	latest, err := handle.SnapshotsOnLog("planner").
		Latest(context.Background(), conversation)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if latest != nil {
		t.Errorf("want nothing, got %+v", latest)
	}

	// And a topic that reports no partitions has nowhere to have saved one.
	bare := newSnapshotTopic(0)
	if latest, err := laser.FromClient(bare,
		laser.Options{DefaultStream: "agents"}).SnapshotsOnLog("planner").
		Latest(context.Background(), conversation); err != nil || latest != nil {
		t.Errorf("want nothing from a topic with no partitions, got %+v %v",
			latest, err)
	}
}

// A conversation whose newest checkpoint is older than one scan window is
// still found, because the walk goes back window by window.
func TestGivenACheckpointBeyondOneWindowWhenReadThenShouldWalkBackToIt(t *testing.T) {
	mine := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	theirs := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 8})}
	server := newSnapshotTopic(1)
	handle := laser.FromClient(server, laser.Options{DefaultStream: "agents"})
	snapshots := handle.SnapshotsOnLog("planner")

	if err := snapshots.Save(context.Background(),
		snapshotFor(mine, "planner", "deep")); err != nil {
		t.Fatalf("save: %v", err)
	}
	// More than one window of another conversation's checkpoints on top.
	for range int(laser.SnapshotScanBatch) + 10 {
		if err := snapshots.Save(context.Background(),
			snapshotFor(theirs, "planner", "theirs")); err != nil {
			t.Fatalf("save: %v", err)
		}
	}

	latest, err := snapshots.Latest(context.Background(), mine)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}

	if latest == nil || string(latest.State) != "deep" {
		t.Fatalf("want the checkpoint past one window, got %+v", latest)
	}
	// It walked back rather than reading the whole partition at once.
	if server.windows() < 2 {
		t.Errorf("want more than one window read, got %d", server.windows())
	}
}

// A topic read that answers with no details at all is a topic that is not
// there, so nothing was ever saved.
func TestGivenNoTopicDetailsWhenReadThenShouldStartFromTheBeginning(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	handle := laser.FromClient(&detaillessTopic{},
		laser.Options{DefaultStream: "agents"})

	latest, err := handle.SnapshotsOnLog("planner").
		Latest(context.Background(), conversation)
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if latest != nil {
		t.Errorf("want nothing, got %+v", latest)
	}
}

// detaillessTopic answers a topic read with nothing, the way a server does
// for a topic that is not there.
type detaillessTopic struct{ iggcon.Client }

func (server *detaillessTopic) Close() error { return nil }

func (server *detaillessTopic) SendBinaryRequest(
	_ context.Context, _ uint32, _ []byte,
) ([]byte, error) {
	return nil, ierror.InvalidCommand{}
}

func (server *detaillessTopic) GetTopic(
	_ context.Context, _ iggcon.Identifier, _ iggcon.Identifier,
) (*iggcon.TopicDetails, error) {
	return nil, nil
}

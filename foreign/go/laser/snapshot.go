package laser

import (
	"cmp"
	"context"
	"slices"
	"strconv"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The defaults a snapshot store keeps its checkpoints under.
const (
	// DefaultSnapshotNamespace is the key-value namespace.
	DefaultSnapshotNamespace = "agent.snapshots"
	// DefaultSnapshotTopic is the topic.
	DefaultSnapshotTopic = "agent.snapshots"
	// SnapshotScanBatch is how many records each backward window reads while
	// looking for a conversation's newest checkpoint.
	SnapshotScanBatch uint32 = 256
)

// SnapshotStore is where fold snapshots live, so a long conversation resumes
// from its last checkpoint instead of replaying every record.
//
// It is a seam with two honest backends rather than one implementation: the
// managed store is a point read and a point write, and the topic store works
// on Apache Iggy with no managed backend at all. Neither difference is
// visible to a caller.
type SnapshotStore interface {
	// Latest is the newest snapshot for a conversation, nil when it has
	// never been snapshotted and the fold starts from the beginning.
	Latest(ctx context.Context, conversation wire.ConversationID) (*wire.FoldSnapshot, error)
	// Save persists a snapshot as the conversation's newest checkpoint.
	Save(ctx context.Context, snapshot wire.FoldSnapshot) error
}

// EncodeSnapshot is a snapshot's canonical bytes, for a key-value value or a
// snapshot-topic body.
func EncodeSnapshot(snapshot wire.FoldSnapshot) ([]byte, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	encoded, err := wire.Marshal(snapshot)
	if err != nil {
		return nil, &CodecError{Reason: "encode the snapshot", Cause: err}
	}
	return encoded, nil
}

// DecodeSnapshot reads a snapshot back from stored bytes.
func DecodeSnapshot(payload []byte) (*wire.FoldSnapshot, error) {
	var snapshot wire.FoldSnapshot
	if err := wire.Unmarshal(payload, &snapshot); err != nil {
		return nil, &CodecError{Reason: "decode the snapshot", Cause: err}
	}
	if err := snapshot.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	return &snapshot, nil
}

// KvSnapshotStore keeps fold snapshots in the managed key-value store, one
// key per conversation in a namespace of its own.
//
// A point read and a point write, with no scan. It needs the key-value
// surface, so a deployment without one is refused rather than silently
// resuming every fold from the beginning.
type KvSnapshotStore struct {
	laser     *Laser
	namespace string
	fold      string
}

// SnapshotsInKv opens a managed snapshot store for one fold.
func (laser *Laser) SnapshotsInKv(fold string) *KvSnapshotStore {
	return laser.SnapshotsInNamespace(DefaultSnapshotNamespace, fold)
}

// SnapshotsInNamespace opens one over a namespace of its own, for keeping
// several folds' snapshots apart.
func (laser *Laser) SnapshotsInNamespace(
	namespace, fold string,
) *KvSnapshotStore {
	return &KvSnapshotStore{laser: laser, namespace: namespace, fold: fold}
}

// Latest reads the conversation's newest checkpoint.
func (store *KvSnapshotStore) Latest(
	ctx context.Context, conversation wire.ConversationID,
) (*wire.FoldSnapshot, error) {
	if store.laser.defaultStream == "" {
		return nil, errNoStream()
	}
	key := SnapshotKey(store.laser.defaultStream, conversation, store.fold)
	value, present, err := store.laser.Kv(store.namespace).
		Get(ctx, []byte(key))
	if err != nil || !present {
		return nil, err
	}
	snapshot, err := DecodeSnapshot(value)
	if err != nil {
		return nil, err
	}
	// A snapshot filed under this key that names another conversation or
	// fold is one somebody moved, and resuming a fold from another fold's
	// state would be worse than resuming from the beginning.
	if snapshot.Conversation != conversation || snapshot.Fold != store.fold {
		return nil, &InvalidError{
			Reason: "the snapshot's identity does not match the store"}
	}
	return snapshot, nil
}

// Save writes the conversation's newest checkpoint.
func (store *KvSnapshotStore) Save(
	ctx context.Context, snapshot wire.FoldSnapshot,
) error {
	if store.laser.defaultStream == "" {
		return errNoStream()
	}
	if err := validSnapshotIdentity(
		store.laser.defaultStream, store.fold, snapshot); err != nil {
		return err
	}
	payload, err := EncodeSnapshot(snapshot)
	if err != nil {
		return err
	}
	key := SnapshotKey(snapshot.Stream, snapshot.Conversation, snapshot.Fold)
	return store.laser.Kv(store.namespace).Set([]byte(key)).
		Bytes(payload).Send(ctx)
}

// TopicSnapshotStore keeps fold snapshots as records on a topic of their own,
// partitioned by conversation so one conversation's checkpoints stay ordered.
//
// It is log-native, so it works where there is no managed backend at all.
// Reading walks one partition backward from its tail and stops at the first
// record for the conversation, so a hit costs the distance back to the last
// checkpoint. A conversation that was never snapshotted walks that partition
// fully before answering: keep the topic on retention, because its history is
// checkpoints rather than truth.
type TopicSnapshotStore struct {
	laser *Laser
	topic string
	fold  string
}

// SnapshotsOnLog opens a log-native snapshot store for one fold.
func (laser *Laser) SnapshotsOnLog(fold string) *TopicSnapshotStore {
	return laser.SnapshotsOnTopic(DefaultSnapshotTopic, fold)
}

// SnapshotsOnTopic opens one over a topic of its own.
func (laser *Laser) SnapshotsOnTopic(topic, fold string) *TopicSnapshotStore {
	return &TopicSnapshotStore{laser: laser, topic: topic, fold: fold}
}

// Latest walks back from the topic's tail to the conversation's newest
// checkpoint.
func (store *TopicSnapshotStore) Latest(
	ctx context.Context, conversation wire.ConversationID,
) (*wire.FoldSnapshot, error) {
	stream := store.laser.defaultStream
	if stream == "" {
		return nil, errNoStream()
	}
	client, err := store.laser.connected()
	if err != nil {
		return nil, err
	}
	streamID, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return nil, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(store.topic)
	if err != nil {
		return nil, &InvalidError{Reason: "topic name: " + err.Error()}
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil {
		if isMissingResource(err) {
			return nil, nil
		}
		return nil, &TransportError{Reason: "read the snapshot topic", Cause: err}
	}
	// No topic yet means nothing was ever saved. A topic reporting no
	// partitions needs no separate check: routing a key into none answers
	// zero, and a partition that holds nothing reads as nothing.
	if details == nil {
		return nil, nil
	}
	// Every save for this conversation and fold routed to one partition, so
	// that is the only one worth reading.
	partition := wire.PartitionForKey(
		[]byte(SnapshotPartitionKey(conversation, store.fold)),
		details.PartitionsCount)
	consumer, err := iggcon.NewIdentifier("laser-snapshot-reader")
	if err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	reader := iggcon.NewSingleConsumer(consumer)
	end, found, err := snapshotTail(ctx, client, streamID, topicID, partition, reader)
	if err != nil || !found {
		return nil, err
	}
	// Backward in windows: the newest window first, and within a window the
	// highest matching offset wins, so the scan stops at the most recent
	// checkpoint rather than the oldest one.
	for end > 0 {
		start := uint64(0)
		if end > uint64(SnapshotScanBatch) {
			start = end - uint64(SnapshotScanBatch)
		}
		window, err := snapshotWindow(
			ctx, client, streamID, topicID, partition, reader, start)
		if err != nil {
			return nil, err
		}
		for index := len(window) - 1; index >= 0; index-- {
			snapshot, err := DecodeSnapshot(window[index].Payload)
			if err != nil {
				continue
			}
			if snapshot.Conversation == conversation &&
				snapshot.Fold == store.fold && snapshot.Stream == stream {
				return snapshot, nil
			}
		}
		end = start
	}
	return nil, nil
}

// snapshotTail is the offset past the partition's last record, and whether it
// holds anything at all.
func snapshotTail(
	ctx context.Context, client iggcon.Client,
	streamID, topicID iggcon.Identifier, partition uint32,
	reader iggcon.Consumer,
) (uint64, bool, error) {
	options := iggcon.DefaultPollOptions()
	options.Count = 1
	polled, err := client.PollMessages(ctx, streamID, topicID, partition,
		reader, iggcon.LastPollingStrategy(), options)
	if err != nil {
		if isMissingResource(err) {
			return 0, false, nil
		}
		return 0, false, &TransportError{
			Reason: "read the snapshot tail", Cause: err}
	}
	if polled == nil || len(polled.Messages) == 0 {
		return 0, false, nil
	}
	last := polled.Messages[len(polled.Messages)-1]
	return last.Header.Offset + 1, true, nil
}

// snapshotWindow reads one window of a partition from an offset, in offset
// order.
//
// A window may overlap the one read before it, because the count a poll takes
// is a hint rather than a bound the server has to honour exactly. That costs
// a re-scan and nothing else: windows are walked newest first and the first
// match returns, so re-reading records an earlier window already rejected
// cannot change the answer.
func snapshotWindow(
	ctx context.Context, client iggcon.Client,
	streamID, topicID iggcon.Identifier, partition uint32,
	reader iggcon.Consumer, start uint64,
) ([]iggcon.IggyMessage, error) {
	options := iggcon.DefaultPollOptions()
	options.Count = SnapshotScanBatch
	polled, err := client.PollMessages(ctx, streamID, topicID, partition,
		reader, iggcon.OffsetPollingStrategy(start), options)
	if err != nil {
		if isMissingResource(err) {
			return nil, nil
		}
		return nil, &TransportError{
			Reason: "read a snapshot window", Cause: err}
	}
	if polled == nil {
		return nil, nil
	}
	return polled.Messages, nil
}

// Save appends the conversation's newest checkpoint.
func (store *TopicSnapshotStore) Save(
	ctx context.Context, snapshot wire.FoldSnapshot,
) error {
	stream := store.laser.defaultStream
	if stream == "" {
		return errNoStream()
	}
	if err := validSnapshotIdentity(stream, store.fold, snapshot); err != nil {
		return err
	}
	payload, err := EncodeSnapshot(snapshot)
	if err != nil {
		return err
	}
	_, err = store.laser.Stream(stream).Topic(store.topic).Send(ctx, payload, nil,
		SnapshotPartitionKey(snapshot.Conversation, snapshot.Fold))
	return err
}

// SnapshotKey is where one conversation's checkpoint for one fold is stored.
//
// The stream's own length prefixes it, so a stream whose name contains the
// separator cannot collide with another stream's key. Like the partition key,
// the exact bytes are shared with every other Laser client.
func SnapshotKey(
	stream string, conversation wire.ConversationID, fold string,
) string {
	return strconv.Itoa(len(stream)) + ":" + stream + ":" +
		conversation.String() + ":" + fold
}

// SnapshotPartitionKey pins one conversation and fold to one partition, which
// is the only one a read has to walk.
//
// The exact bytes matter beyond this client: the key decides the partition,
// so a client spelling it differently would write a checkpoint where another
// client never looks. It is the spelling every Laser client uses.
func SnapshotPartitionKey(
	conversation wire.ConversationID, fold string,
) string {
	return conversation.String() + ":" + strconv.Itoa(len(fold)) + ":" + fold
}

// ResumeOffsets is where a fold resumes after snapshot: one past each partition's last folded offset.
func ResumeOffsets(snapshot wire.FoldSnapshot) []wire.SnapshotOffset {
	resumed := make([]wire.SnapshotOffset, 0, len(snapshot.AsOf))
	for _, entry := range snapshot.AsOf {
		next := entry.Offset
		if next != ^uint64(0) {
			next++
		}
		resumed = append(resumed, wire.NewSnapshotOffset(entry.TopicID, entry.TopicCreatedAtMicros, entry.PartitionID, next))
	}
	return resumed
}

// SnapshotFromCheckpoint builds a snapshot of state folded up to checkpoint. It records the
// stream and every checkpointed topic by id and creation time, and leaves empty partitions out.
func SnapshotFromCheckpoint(
	ctx context.Context, laser *Laser, conversation wire.ConversationID, fold string,
	checkpoint *ContextCheckpoint, state []byte,
) (*wire.FoldSnapshot, error) {
	if laser == nil || checkpoint == nil {
		return nil, &InvalidError{Reason: "a snapshot needs a client and a checkpoint"}
	}
	if laser.defaultStream == "" {
		return nil, errNoStream()
	}
	stream, err := laser.Stream(laser.defaultStream).Get(ctx)
	if err != nil {
		return nil, err
	}
	if stream == nil {
		return nil, &InvalidError{Reason: "stream `" + laser.defaultStream + "` does not exist"}
	}
	var asOf []wire.SnapshotOffset
	for topic, partitions := range checkpoint.Offsets {
		details, err := laser.Topic(topic).Get(ctx)
		if err != nil {
			return nil, err
		}
		if details == nil {
			return nil, &InvalidError{Reason: "topic `" + topic + "` does not exist"}
		}
		for partition, next := range partitions {
			if next > 0 {
				asOf = append(asOf, wire.NewSnapshotOffset(details.ID, details.CreatedAtMicros, uint32(partition), next-1))
			}
		}
	}
	slices.SortFunc(asOf, func(left, right wire.SnapshotOffset) int {
		return cmp.Or(cmp.Compare(left.TopicID, right.TopicID),
			cmp.Compare(left.TopicCreatedAtMicros, right.TopicCreatedAtMicros),
			cmp.Compare(left.PartitionID, right.PartitionID))
	})
	snapshot := &wire.FoldSnapshot{
		Stream: laser.defaultStream, StreamID: stream.ID, StreamCreatedAtMicros: stream.CreatedAtMicros,
		Conversation: conversation, Fold: fold, AsOf: asOf, State: state,
	}
	if err := snapshot.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	return snapshot, nil
}

// CheckpointFromSnapshot is the checkpoint a fold resumes after snapshot from. It refuses a
// snapshot whose stream or source topics were recreated since.
func CheckpointFromSnapshot(
	ctx context.Context, laser *Laser, snapshot wire.FoldSnapshot, topics []string,
) (*ContextCheckpoint, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	if laser == nil || laser.defaultStream == "" {
		return nil, errNoStream()
	}
	if snapshot.Stream != laser.defaultStream {
		return nil, &InvalidError{Reason: "snapshot stream does not match the client"}
	}
	stream, err := laser.Stream(laser.defaultStream).Get(ctx)
	if err != nil {
		return nil, err
	}
	if stream == nil {
		return nil, &InvalidError{Reason: "snapshot stream no longer exists"}
	}
	if snapshot.StreamID != stream.ID || snapshot.StreamCreatedAtMicros != stream.CreatedAtMicros {
		return nil, &InvalidError{Reason: "snapshot stream generation changed"}
	}
	type source struct {
		topic      uint32
		generation uint64
	}
	current := make(map[source]struct{}, len(topics))
	checkpoint := &ContextCheckpoint{Offsets: make(map[string][]uint64, len(topics))}
	for _, topic := range topics {
		details, err := laser.Topic(topic).Get(ctx)
		if err != nil {
			return nil, err
		}
		if details == nil {
			return nil, &InvalidError{Reason: "snapshot source topic " + topic + " is missing"}
		}
		current[source{details.ID, details.CreatedAtMicros}] = struct{}{}
		offsets := make([]uint64, boundedPartitions(details.PartitionsCount))
		for partition := range offsets {
			offsets[partition] = snapshot.ResumeOffset(details.ID, details.CreatedAtMicros, uint32(partition))
		}
		checkpoint.Offsets[topic] = offsets
	}
	for _, entry := range snapshot.AsOf {
		if _, known := current[source{entry.TopicID, entry.TopicCreatedAtMicros}]; !known {
			return nil, &InvalidError{Reason: "snapshot source topic generation changed"}
		}
	}
	return checkpoint, nil
}

// validSnapshotIdentity refuses a snapshot that belongs to another store.
//
// Saving one fold's state under another's key would resume a fold from state
// it never produced, which is worse than resuming from the beginning.
func validSnapshotIdentity(
	stream, fold string, snapshot wire.FoldSnapshot,
) error {
	if snapshot.Stream != stream || snapshot.Fold != fold {
		return &InvalidError{
			Reason: "the snapshot's identity does not match the store"}
	}
	return nil
}

package laser

import (
	"context"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// WatchWaitInterval is how often a bounded wait looks again while nothing
// new has landed.
const WatchWaitInterval = 100 * time.Millisecond

// Watch is a fluent read of the change feed: one record per committed
// projector batch, for a binding that opted into notifications.
//
// It turns "query after my data landed" from sleep-and-retry into
// await-then-query. It composes with read-your-writes rather than replacing
// it: the feed says a batch landed, and the frontier says a read will see it.
type Watch struct {
	laser   *Laser
	index   string
	fromEnd bool
}

// Watch opens the change-feed accessor.
func (laser *Laser) Watch() *Watch { return &Watch{laser: laser} }

// Index keeps only advancements of one materialized index, named the way
// every other managed resource is named.
func (watch *Watch) Index(index string) *Watch {
	watch.index = watch.laser.ResourceName(index)
	return watch
}

// FromEnd starts at the feed's current end, so the reader receives only what
// is published after it opens.
//
// Without it a reader starts at the first retained change, which is what a
// caller catching up on a backlog wants.
func (watch *Watch) FromEnd() *Watch {
	watch.fromEnd = true
	return watch
}

// Records opens the feed reader.
//
// A deployment that does not publish the feed is refused rather than left
// polling a topic nothing writes to: a consumer waiting on silence looks
// identical to one waiting on work that never comes.
func (watch *Watch) Records(ctx context.Context) (*WatchReader, error) {
	if !watch.laser.capabilities.Watch {
		return nil, &UnsupportedError{Surface: "the change feed"}
	}
	stream, topic := watch.laser.changeFeed()
	cursor, err := watch.laser.Stream(stream).Topic(topic).ReplayAs(watchReaderName)
	if err != nil {
		return nil, err
	}
	reader := &WatchReader{
		laser:      watch.laser,
		opsStream:  stream,
		feedTopic:  topic,
		cursor:     cursor,
		index:      watch.index,
		unanchored: map[uint32]struct{}{},
	}
	if watch.fromEnd {
		if err := reader.startAtEnd(ctx); err != nil {
			return nil, err
		}
	}
	return reader, nil
}

// watchReaderName is how a feed reader names itself to the server. The
// offsets stay client-owned, so the name only identifies the reader.
const watchReaderName = "laser-watch"

// changeFeed is the stream and topic the change records ride.
//
// Under stream tenancy each stream's feed rides its own ops topic, so a
// scoped handle reads its own stream's rather than a shared one it may not
// even be allowed to see.
func (laser *Laser) changeFeed() (stream, topic string) {
	if scoped := laser.resourceStream(); scoped != "" && laser.capabilities.StreamTenancy {
		return wire.OpsStream, wire.StreamOpsTopic(scoped, wire.ChangesTopic)
	}
	return wire.OpsStream, wire.ChangesTopic
}

// WatchedChange is one change record with its place on the feed.
type WatchedChange struct {
	// Change is what the projector committed.
	Change wire.ChangeRecord
	// MessageID is the feed record's own id. A retried batch publishes its
	// change again under the same id, so a reader drops a copy it has seen.
	MessageID iggcon.MessageID
	// PartitionID and Offset are where the record sits on the feed.
	PartitionID uint32
	Offset      uint64
}

// WatchReader is a resumable change-feed reader.
//
// Each poll drains what landed since the last one. Persist the offsets and
// seed a fresh reader to resume across a restart, exactly like any cursor.
type WatchReader struct {
	laser     *Laser
	opsStream string
	feedTopic string
	cursor    *Cursor
	index     string
	// unanchored are the partitions this reader started at the end of while
	// they held nothing, so the first record they carry says nothing about
	// records it missed.
	unanchored map[uint32]struct{}
	missed     uint64
	skipped    uint64
}

// FromOffsets resumes from a previous reader's offsets.
func (reader *WatchReader) FromOffsets(offsets []uint64) *WatchReader {
	reader.cursor = reader.cursor.FromOffsets(offsets)
	clear(reader.unanchored)
	return reader
}

// Offsets is the resume state: the next offset to read on each partition.
func (reader *WatchReader) Offsets() []uint64 { return reader.cursor.Offsets() }

// Missed is how many feed records retention removed before this reader
// reached them, counted from where it started or resumed.
//
// Above zero means changes were missed, so a caller reads the view itself
// rather than trusting the feed to have told it everything.
func (reader *WatchReader) Missed() uint64 { return reader.missed }

// Skipped is how many feed records were not change records.
//
// The changes topic is overridable, so other traffic on it must not wedge the
// reader: it is counted and stepped over rather than failing the poll.
func (reader *WatchReader) Skipped() uint64 { return reader.skipped }

// Poll drains the changes that landed since the last poll, narrowed to the
// watched index when one was set.
//
// A change record of another version stops its partition where it sits: the
// changes before it come back, and a poll with nothing else to return fails.
// This build never guesses at a newer record, and never steps over one.
func (reader *WatchReader) Poll(ctx context.Context) ([]WatchedChange, error) {
	resumed := reader.cursor.Offsets()
	records, err := reader.cursor.Poll(ctx)
	if err != nil {
		return nil, err
	}
	offsets := reader.cursor.Offsets()
	anchored := map[uint32]struct{}{}
	stopped := map[uint32]uint32{}
	changes := make([]WatchedChange, 0, len(records))

	for _, record := range records {
		partition := record.PartitionID
		if _, halted := stopped[partition]; halted {
			continue
		}
		offset := record.Offset()
		if _, counted := anchored[partition]; !counted {
			anchored[partition] = struct{}{}
			reader.countMissed(partition, offset, resumed)
		}
		version, named := feedVersion(record.Payload())
		if !named {
			reader.skipped++
			continue
		}
		if version != wire.ChangeOpVersion {
			// The partition stops here rather than stepping over the record,
			// so the reader resumes at it once this build understands it.
			stopped[partition] = version
			if int(partition) < len(offsets) {
				offsets[partition] = offset
			}
			continue
		}
		var change wire.ChangeRecord
		if err := wire.Unmarshal(record.Payload(), &change); err != nil {
			reader.skipped++
			continue
		}
		if reader.index != "" && change.Index != reader.index {
			continue
		}
		changes = append(changes, WatchedChange{
			Change:      change,
			MessageID:   record.Message.Header.Id,
			PartitionID: partition,
			Offset:      offset,
		})
	}

	if len(stopped) > 0 {
		// The cursor is rebuilt at the offsets the stop decided, so the next
		// poll starts at the record this build could not read rather than
		// past it.
		rebuilt, err := reader.laser.Stream(reader.opsStream).
			Topic(reader.feedTopic).ReplayAs(watchReaderName)
		if err != nil {
			return nil, err
		}
		reader.cursor = rebuilt.FromOffsets(offsets)
	}
	if len(changes) == 0 {
		for _, version := range stopped {
			return nil, &VersionError{
				Surface: "the change feed",
				Pinned:  wire.ChangeOpVersion,
				Served:  version,
			}
		}
	}
	return changes, nil
}

// countMissed adds what retention removed before this reader reached a
// partition.
//
// A partition this reader started at the end of while it held nothing is
// skipped: its first record is the first record there ever was, so nothing
// was missed before it.
func (reader *WatchReader) countMissed(
	partition uint32, offset uint64, resumed []uint64,
) {
	if _, unanchored := reader.unanchored[partition]; unanchored {
		delete(reader.unanchored, partition)
		return
	}
	expected := uint64(0)
	if int(partition) < len(resumed) {
		expected = resumed[partition]
	}
	if offset > expected {
		reader.missed += offset - expected
	}
}

// PollWithin polls, waiting up to a bound for the first change to land. An
// empty batch means none landed in time.
func (reader *WatchReader) PollWithin(
	ctx context.Context, bound time.Duration,
) ([]WatchedChange, error) {
	deadline := time.Now().Add(bound)
	for {
		changes, err := reader.Poll(ctx)
		if err != nil || len(changes) > 0 {
			return changes, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return changes, nil
		}
		if err := sleepWithin(ctx, min(WatchWaitInterval, remaining)); err != nil {
			return nil, err
		}
	}
}

// Each polls until the reader is caught up or visit returns false.
//
// A caller that stops early is left positioned at the first change it did not
// take, so the offsets it persists mean what they say. Without that, stopping
// mid-batch would silently skip the rest of it: the cursor has already read
// those records even though the caller never saw them.
func (reader *WatchReader) Each(
	ctx context.Context, visit func(WatchedChange) bool,
) error {
	for {
		changes, err := reader.Poll(ctx)
		if err != nil {
			return err
		}
		if len(changes) == 0 {
			return nil
		}
		for index, change := range changes {
			if !visit(change) {
				return reader.rewindTo(changes[index+1:])
			}
		}
	}
}

// rewindTo positions the reader at the earliest change it did not hand over,
// per partition.
//
// A record the poll skipped or filtered out between two changes is read again
// by the next poll, which costs a decode and counts a skip twice. That is the
// cheaper half of the trade: the alternative is a caller believing it read
// through changes it never saw.
func (reader *WatchReader) rewindTo(remaining []WatchedChange) error {
	if len(remaining) == 0 {
		return nil
	}
	offsets := reader.cursor.Offsets()
	earliest := map[uint32]uint64{}
	for _, change := range remaining {
		if at, seen := earliest[change.PartitionID]; !seen || change.Offset < at {
			earliest[change.PartitionID] = change.Offset
		}
	}
	for partition, offset := range earliest {
		if int(partition) < len(offsets) {
			offsets[partition] = offset
		}
	}
	rebuilt, err := reader.laser.Stream(reader.opsStream).
		Topic(reader.feedTopic).ReplayAs(watchReaderName)
	if err != nil {
		return err
	}
	reader.cursor = rebuilt.FromOffsets(offsets)
	// Those partitions were anchored by the poll that read them, so a rewind
	// must not make their next first record look like a missed range.
	for partition := range earliest {
		delete(reader.unanchored, partition)
	}
	return nil
}

// startAtEnd seeds every partition at the offset it writes next.
func (reader *WatchReader) startAtEnd(ctx context.Context) error {
	client, err := reader.laser.connected()
	if err != nil {
		return err
	}
	streamID, err := iggcon.NewIdentifier(reader.opsStream)
	if err != nil {
		return &InvalidError{Reason: "ops stream: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(reader.feedTopic)
	if err != nil {
		return &InvalidError{Reason: "changes topic: " + err.Error()}
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil {
		return &TransportError{Reason: "read the change feed topic", Cause: err}
	}
	if details == nil {
		// A feed nobody has written to yet starts at zero, which is both its
		// end and its beginning.
		return nil
	}
	partitions := boundedPartitions(details.PartitionsCount)
	offsets := make([]uint64, 0, partitions)
	for partition := uint32(0); partition < partitions; partition++ {
		tail, err := reader.tailOffset(ctx, client, streamID, topicID, partition)
		if err != nil {
			return err
		}
		if tail == 0 {
			// Nothing is there, so the first record this reader sees is the
			// first record there ever was.
			reader.unanchored[partition] = struct{}{}
		}
		offsets = append(offsets, tail)
	}
	rebuilt, err := reader.laser.Stream(reader.opsStream).
		Topic(reader.feedTopic).ReplayAs(watchReaderName)
	if err != nil {
		return err
	}
	reader.cursor = rebuilt.FromOffsets(offsets)
	return nil
}

// tailOffset is the offset a partition writes next.
//
// It is read as the last record's offset plus one rather than from a topic
// listing, because a listing reports what the partition holds and a reader
// needs where it will write.
func (reader *WatchReader) tailOffset(
	ctx context.Context, client iggcon.Client,
	streamID, topicID iggcon.Identifier, partition uint32,
) (uint64, error) {
	options := iggcon.DefaultPollOptions()
	options.Count = 1
	polled, err := client.PollMessages(ctx, streamID, topicID, partition,
		iggcon.NewSingleConsumer(reader.cursor.consumer.Id),
		iggcon.LastPollingStrategy(), options)
	if err != nil {
		if isMissingResource(err) {
			return 0, nil
		}
		return 0, &TransportError{Reason: "read the feed tail", Cause: err}
	}
	if polled == nil || len(polled.Messages) == 0 {
		return 0, nil
	}
	last := polled.Messages[len(polled.Messages)-1]
	return last.Header.Offset + 1, nil
}

// feedVersion reads the version a feed record names, reporting whether it
// names one at all.
//
// The version is read as an optional field rather than a plain one, because
// absent and zero are different things here: a payload that is not a change
// record at all is skipped, while one that claims version zero is a change
// record this build cannot read and stops its partition. Reading them the
// same way would let other traffic on the topic wedge the reader.
func feedVersion(payload []byte) (uint32, bool) {
	var named struct {
		V *uint32 `cbor:"v"`
	}
	if err := wire.Unmarshal(payload, &named); err != nil || named.V == nil {
		return 0, false
	}
	return *named.V, true
}

package laser

import (
	"context"
	"sort"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

const (
	// DefaultCursorBatch is how many records one server read asks for per
	// partition.
	DefaultCursorBatch uint32 = 1000
	// MaxDrainRecords bounds how many records one Poll takes from a single
	// partition, so draining a long partition cannot grow without limit.
	// Persist the offsets and poll again to continue past it.
	MaxDrainRecords = 10_000
	// MaxTopicPartitions is a defensive ceiling on a server-reported partition
	// count. It sizes allocations and loop counts, so it is treated as
	// untrusted input: a corrupt reply must not turn a topic lookup into a
	// multi-gigabyte reservation.
	MaxTopicPartitions uint32 = 1024
)

// Cursor is a resumable positional reader over one topic.
//
// It owns its offsets rather than storing them server-side, so a caller
// persists them and resumes exactly where it left off. It is the bounded read
// below a consumer: it drains what is there and stops, rather than following
// the topic.
type Cursor struct {
	topic    *Topic
	consumer iggcon.Consumer
	offsets  []uint64
	batch    uint32
}

// Replay opens a cursor over this topic.
func (topic *Topic) Replay() (*Cursor, error) {
	return topic.ReplayAs("laser-cursor")
}

// ReplayAs opens a cursor that names itself to the server. The offsets stay
// client-owned either way: the identity only names the reader server-side.
func (topic *Topic) ReplayAs(name string) (*Cursor, error) {
	if topic.stream == "" {
		return nil, errNoStream()
	}
	identifier, err := iggcon.NewIdentifier(name)
	if err != nil {
		return nil, &InvalidError{Reason: "reader name: " + err.Error()}
	}
	return &Cursor{
		topic:    topic,
		consumer: iggcon.NewSingleConsumer(identifier),
		batch:    DefaultCursorBatch,
	}, nil
}

// FromOffsets resumes from per-partition offsets an earlier Offsets returned.
//
// A shorter list than the topic has partitions is padded with zero, so those
// partitions are read from the start rather than skipped.
func (cursor *Cursor) FromOffsets(offsets []uint64) *Cursor {
	cursor.offsets = append([]uint64(nil), offsets...)
	return cursor
}

// Batch sizes each server read, per partition.
//
// This is the size of one request, not a ceiling on one Poll: a poll keeps
// reading until it reaches the partition's tail or the drain ceiling.
func (cursor *Cursor) Batch(batch uint32) *Cursor {
	if batch < 1 {
		batch = 1
	}
	cursor.batch = batch
	return cursor
}

// Offsets is the next offset to read on each partition. Persist it to resume
// later with FromOffsets.
func (cursor *Cursor) Offsets() []uint64 {
	return append([]uint64(nil), cursor.offsets...)
}

// Poll drains everything appended since the last poll and advances the cursor.
//
// Records come back ordered by the server's own timestamp, then partition and
// offset, so a merge across partitions is deterministic. An empty result means
// the reader is caught up, or that the topic does not exist yet: a reader
// started before its writer is not an error.
func (cursor *Cursor) Poll(ctx context.Context) ([]*Record, error) {
	client, err := cursor.topic.laser.connected()
	if err != nil {
		return nil, err
	}
	streamID, topicID, err := cursor.topic.identifiers()
	if err != nil {
		return nil, err
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil {
		if isMissingResource(err) {
			return nil, nil
		}
		return nil, &TransportError{Reason: "read the topic", Cause: err}
	}
	if details == nil {
		return nil, nil
	}

	partitions := boundedPartitions(details.PartitionsCount)
	next := cursor.offsets
	for uint32(len(next)) < partitions {
		next = append(next, 0)
	}

	// Collected with the sort key alongside, so the merge across partitions is
	// ordered by the server's single clock rather than by read order.
	type positioned struct {
		timestamp uint64
		partition uint32
		offset    uint64
		message   iggcon.IggyMessage
	}
	var collected []positioned
	for partition := uint32(0); partition < partitions; partition++ {
		messages, nextOffset, err := cursor.drain(
			ctx, client, streamID, topicID, partition, next[partition])
		if err != nil {
			return nil, err
		}
		next[partition] = nextOffset
		for _, message := range messages {
			collected = append(collected, positioned{
				timestamp: message.Header.Timestamp,
				partition: partition,
				offset:    message.Header.Offset,
				message:   message,
			})
		}
	}
	sort.SliceStable(collected, func(first, second int) bool {
		left, right := collected[first], collected[second]
		if left.timestamp != right.timestamp {
			return left.timestamp < right.timestamp
		}
		if left.partition != right.partition {
			return left.partition < right.partition
		}
		return left.offset < right.offset
	})

	cursor.offsets = next
	records := make([]*Record, 0, len(collected))
	for _, item := range collected {
		record, err := OpenRecord(item.message)
		if err != nil {
			return nil, err
		}
		record.PartitionID = item.partition
		records = append(records, record)
	}
	return records, nil
}

// Each drains the topic and calls visit once per record, stopping early when
// visit returns false. It ends once the reader is caught up.
func (cursor *Cursor) Each(
	ctx context.Context, visit func(*Record) bool,
) error {
	for {
		records, err := cursor.Poll(ctx)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return nil
		}
		for _, record := range records {
			if !visit(record) {
				return nil
			}
		}
	}
}

// drain reads one partition from an offset toward its tail, bounded by the
// drain ceiling so one call cannot read an unbounded partition into memory.
func (cursor *Cursor) drain(
	ctx context.Context, client iggcon.Client, streamID, topicID iggcon.Identifier,
	partition uint32, from uint64,
) ([]iggcon.IggyMessage, uint64, error) {
	var drained []iggcon.IggyMessage
	next := from
	for len(drained) < MaxDrainRecords {
		options := iggcon.DefaultPollOptions()
		options.Count = cursor.batch
		if remaining := MaxDrainRecords - len(drained); uint32(remaining) < options.Count {
			options.Count = uint32(remaining)
		}
		polled, err := client.PollMessages(ctx, streamID, topicID, partition,
			cursor.consumer, iggcon.OffsetPollingStrategy(next), options)
		if err != nil {
			if isMissingResource(err) {
				return drained, next, nil
			}
			return nil, 0, &TransportError{Reason: "read records", Cause: err}
		}
		if polled == nil || len(polled.Messages) == 0 {
			return drained, next, nil
		}
		for _, message := range polled.Messages {
			drained = append(drained, message)
			// The next offset is one past the highest seen, so a resumed read
			// does not deliver the last record twice.
			if message.Header.Offset+1 > next {
				next = message.Header.Offset + 1
			}
		}
	}
	return drained, next, nil
}

// boundedPartitions clamps a server-reported partition count before it sizes
// an allocation or a loop.
func boundedPartitions(reported uint32) uint32 {
	if reported > MaxTopicPartitions {
		return MaxTopicPartitions
	}
	return reported
}

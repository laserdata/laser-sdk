package laser

import (
	"context"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// WarmDedupPolicy decides which committed records refill a runtime's dedup
// window when it starts with RuntimeOptions.WarmDedup set. A record with no
// deadline always refills its key, whichever policy is named.
//
// Ported from sdk/src/agent/consumer.rs's WarmDedupPolicy.
type WarmDedupPolicy int

const (
	// WarmDedupDeadLetters leaves out a copy a DeadlineExceeded dead letter
	// names, reading the dead-letter topic to find them. When the dead
	// letters cannot all be read, it falls back to WarmDedupUnexpired. The
	// zero value, so RuntimeOptions{} without naming one still gets it.
	WarmDedupDeadLetters WarmDedupPolicy = iota
	// WarmDedupUnexpired leaves out every copy whose deadline passed before
	// the refill ran, without reading dead letters.
	WarmDedupUnexpired
	// WarmDedupAll refills every key, including a copy dropped past its
	// deadline, so a resend after an expiry is still recognized as a repeat.
	WarmDedupAll
)

const (
	// warmDedupBatch is how many records one read asks for while draining a
	// partition's history or the dead-letter topic.
	warmDedupBatch uint32 = 1000
	// warmDedupReaderName is the throwaway reader identity the refill uses.
	// It never commits a position, so it leaves nothing for the server to
	// store: the group's own stored offset is read, never written, by this
	// pass.
	warmDedupReaderName = "laser-dedup-warmer"
	// deadLetterClockMargin widens the dead-letter scan's start time behind
	// the oldest deadline-bearing seed, so a dead letter published slightly
	// before the seed it names (server clock skew between the two writes) is
	// not missed.
	deadLetterClockMargin = 60 * time.Second
)

// warmDedupSeed is one committed record whose key can refill the dedup
// window.
type warmDedupSeed struct {
	key        string
	partition  uint32
	offset     uint64
	appendedAt time.Time
	deadline   *time.Time
}

// warmDedupPosition is where one copy sits, the shape a dead letter's own
// source names it by.
type warmDedupPosition struct {
	partition uint32
	offset    uint64
}

// warmUpDedup runs the configured WarmDedup refill once, before Run starts
// reading, when the runtime was built with it on. It is a no-op otherwise.
func (runtime *AgentRuntime) warmUpDedup(ctx context.Context) error {
	if !runtime.warmDedup {
		return nil
	}
	runtime.resolveSource(ctx)
	return runtime.warmDedupWindow(ctx)
}

// warmDedupWindow replays this runtime's group's assigned partitions' recent
// history into its dedup window, so a restart does not reprocess duplicates
// still inside the window. It reads only up to the group's own stored
// (already consumed) offset, and at most the dedup window's own capacity
// worth of records per partition: reading past the stored offset would
// pre-mark unconsumed messages and skip them (data loss).
//
// Ported from warm_dedup_window in sdk/src/agent/consumer.rs. Unlike Rust's
// ReliableConsumer, which joins its own consumer group and so always knows
// its name, Go's AgentRuntime is handed an already-open RecordSource and
// never learns what group it came from -- this is why WarmDedup needs
// RuntimeOptions.Group named explicitly.
func (runtime *AgentRuntime) warmDedupWindow(ctx context.Context) error {
	topic := runtime.laser.Stream(runtime.stream).Topic(runtime.topic)
	client, err := runtime.laser.connected()
	if err != nil {
		return err
	}
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return err
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil {
		if isMissingResource(err) {
			return nil
		}
		return &TransportError{Reason: "read the topic", Cause: err}
	}
	if details == nil {
		return nil
	}
	groupIdentifier, err := iggcon.NewIdentifier(runtime.group)
	if err != nil {
		return &InvalidError{Reason: "warm dedup group: " + err.Error()}
	}
	group := iggcon.NewGroupConsumer(groupIdentifier)
	readerIdentifier, err := iggcon.NewIdentifier(warmDedupReaderName)
	if err != nil {
		return &InvalidError{Reason: "warm dedup reader: " + err.Error()}
	}
	reader := iggcon.NewSingleConsumer(readerIdentifier)

	depth := uint64(runtime.agent.dedup.limit)
	var seeds []warmDedupSeed
	for partition := range boundedPartitions(details.PartitionsCount) {
		stored, err := client.GetConsumerOffset(ctx, group, streamID, topicID, &partition)
		if err != nil {
			if isMissingOffset(err) {
				continue
			}
			return &TransportError{Reason: "read the consumer offset", Cause: err}
		}
		if stored == nil {
			continue
		}
		from := uint64(0)
		if stored.StoredOffset+1 > depth {
			from = stored.StoredOffset + 1 - depth
		}
		for from <= stored.StoredOffset {
			messages, next, err := warmDrainRange(
				ctx, client, streamID, topicID, partition, reader, from, stored.StoredOffset)
			if err != nil {
				return err
			}
			if len(messages) == 0 {
				break
			}
			for _, message := range messages {
				if seed, ok := decodeWarmSeed(message, partition, runtime.identity); ok {
					seeds = append(seeds, seed)
				}
			}
			from = next
		}
	}

	var oldestWithDeadline *time.Time
	for _, seed := range seeds {
		if seed.deadline == nil {
			continue
		}
		if oldestWithDeadline == nil || seed.appendedAt.Before(*oldestWithDeadline) {
			appendedAt := seed.appendedAt
			oldestWithDeadline = &appendedAt
		}
	}

	var expired map[warmDedupPosition]struct{}
	if runtime.warmDedupPolicy == WarmDedupDeadLetters && oldestWithDeadline != nil &&
		runtime.source.StreamID != 0 && runtime.source.TopicID != 0 {
		since := oldestWithDeadline.Add(-deadLetterClockMargin)
		// A scan that could not be trusted (too many to bound, retention
		// already past since, or a transport failure) leaves expired nil:
		// refills below falls back to the deadline-only check, the same
		// outcome WarmDedupUnexpired gives.
		if found, scanErr := runtime.expiredDeadLetters(
			ctx, client, streamID, reader, since, depth); scanErr == nil {
			expired = found
		}
	}

	now := time.Now()
	for _, seed := range seeds {
		if warmDedupRefills(runtime.warmDedupPolicy, seed, expired, now) {
			runtime.agent.dedup.Accept(seed.key)
		}
	}
	return nil
}

// warmDrainRange reads from toward until (inclusive) in one batch, returning
// what it read and the offset to resume from. An empty result means the
// partition has nothing left in range.
func warmDrainRange(
	ctx context.Context, client iggcon.Client, streamID, topicID iggcon.Identifier,
	partition uint32, consumer iggcon.Consumer, from, until uint64,
) ([]iggcon.IggyMessage, uint64, error) {
	options := iggcon.DefaultPollOptions()
	options.Count = warmDedupBatch
	if span := until - from + 1; span < uint64(options.Count) {
		options.Count = uint32(span)
	}
	polled, err := client.PollMessages(ctx, streamID, topicID, partition,
		consumer, iggcon.OffsetPollingStrategy(from), options)
	if err != nil {
		if isMissingResource(err) {
			return nil, until + 1, nil
		}
		return nil, 0, &TransportError{Reason: "read records", Cause: err}
	}
	if polled == nil || len(polled.Messages) == 0 {
		return nil, until + 1, nil
	}
	messages := polled.Messages
	next := from
	for index, message := range messages {
		if message.Header.Offset > until {
			messages = messages[:index]
			break
		}
		if message.Header.Offset+1 > next {
			next = message.Header.Offset + 1
		}
	}
	if next <= from {
		next = until + 1
	}
	return messages, next, nil
}

// decodeWarmSeed decodes message into a warmDedupSeed, when it carries a
// usable dedup key and is not addressed to an agent other than identity. A
// record with no usable key (no spine, or no idempotency key) is left out:
// there is nothing for it to refill.
func decodeWarmSeed(
	message iggcon.IggyMessage, partition uint32, identity wire.AgentID,
) (warmDedupSeed, bool) {
	record, err := OpenRecord(message)
	if err != nil || record.Provenance == nil {
		return warmDedupSeed{}, false
	}
	if target := record.Provenance.TargetAgent; target != nil && !identity.IsZero() &&
		*target != identity {
		return warmDedupSeed{}, false
	}
	key := DedupKey(record.Provenance)
	if key == "" {
		return warmDedupSeed{}, false
	}
	return warmDedupSeed{
		key:        key,
		partition:  partition,
		offset:     message.Header.Offset,
		appendedAt: time.UnixMicro(int64(message.Header.Timestamp)),
		deadline:   record.Provenance.DeadlineAt,
	}, true
}

// expiredDeadLetters finds the position of every copy this runtime's topic's
// dead letters say was dropped past its deadline, published since since. It
// reports a nil map, rather than an error, when a dead-letter partition holds
// more than limit of them since that point, or retention has already removed
// some: either means the result cannot be trusted as complete.
func (runtime *AgentRuntime) expiredDeadLetters(
	ctx context.Context, client iggcon.Client, streamID iggcon.Identifier,
	reader iggcon.Consumer, since time.Time, limit uint64,
) (map[warmDedupPosition]struct{}, error) {
	dlqID, err := iggcon.NewIdentifier(wire.AgentDLQ)
	if err != nil {
		return nil, &InvalidError{Reason: "dead letter topic: " + err.Error()}
	}
	details, err := client.GetTopic(ctx, streamID, dlqID)
	if err != nil {
		if isMissingResource(err) {
			return map[warmDedupPosition]struct{}{}, nil
		}
		return nil, &TransportError{Reason: "read the dead letter topic", Cause: err}
	}
	if details == nil {
		return map[warmDedupPosition]struct{}{}, nil
	}
	one := iggcon.DefaultPollOptions()
	one.Count = 1
	sinceMicros := uint64(since.UnixMicro())
	expired := map[warmDedupPosition]struct{}{}
	var total uint64
	for partition := range boundedPartitions(details.PartitionsCount) {
		oldest, err := client.PollMessages(ctx, streamID, dlqID, partition, reader,
			iggcon.FirstPollingStrategy(), one)
		if err != nil {
			return nil, &TransportError{Reason: "read the dead letter topic", Cause: err}
		}
		if oldest == nil || len(oldest.Messages) == 0 {
			continue
		}
		first := oldest.Messages[0]
		if first.Header.Offset > 0 && first.Header.Timestamp > sinceMicros {
			// Retention already dropped some of what this scan needs.
			return nil, nil
		}
		start, err := client.PollMessages(ctx, streamID, dlqID, partition, reader,
			iggcon.TimestampPollingStrategy(sinceMicros), one)
		if err != nil {
			return nil, &TransportError{Reason: "read the dead letter topic", Cause: err}
		}
		if start == nil || len(start.Messages) == 0 {
			continue
		}
		from := start.Messages[0].Header.Offset
		for {
			options := iggcon.DefaultPollOptions()
			options.Count = warmDedupBatch
			polled, err := client.PollMessages(ctx, streamID, dlqID, partition, reader,
				iggcon.OffsetPollingStrategy(from), options)
			if err != nil {
				return nil, &TransportError{Reason: "read the dead letter topic", Cause: err}
			}
			if polled == nil || len(polled.Messages) == 0 {
				break
			}
			total += uint64(len(polled.Messages))
			if total > limit {
				return nil, nil
			}
			for _, message := range polled.Messages {
				var letter wire.AgentDeadLetter
				if err := wire.Unmarshal(message.Payload, &letter); err != nil {
					continue
				}
				if letter.Reason == wire.DeadLetterDeadlineExceeded &&
					letter.Source.StreamID == runtime.source.StreamID &&
					letter.Source.TopicID == runtime.source.TopicID {
					expired[warmDedupPosition{letter.Source.PartitionID, letter.Source.Offset}] =
						struct{}{}
				}
				if message.Header.Offset+1 > from {
					from = message.Header.Offset + 1
				}
			}
			if uint32(len(polled.Messages)) < options.Count {
				break
			}
		}
	}
	return expired, nil
}

// warmDedupRefills reports whether seed should refill the dedup window under
// policy. The deadline gate drops only a copy whose deadline has passed, so a
// copy with none always refills. expired holds the positions dead letters
// name, nil when they could not all be read.
func warmDedupRefills(
	policy WarmDedupPolicy, seed warmDedupSeed,
	expired map[warmDedupPosition]struct{}, now time.Time,
) bool {
	if seed.deadline == nil {
		return true
	}
	switch {
	case policy == WarmDedupAll:
		return true
	case policy == WarmDedupDeadLetters && expired != nil:
		_, named := expired[warmDedupPosition{seed.partition, seed.offset}]
		return !named
	default:
		return !now.After(*seed.deadline)
	}
}

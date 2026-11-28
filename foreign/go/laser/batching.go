package laser

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// The batching bounds a producer flushes on, unless a caller sets its own.
const (
	// DefaultMaxRecords is how many queued records trip a flush.
	DefaultMaxRecords = 512
	// DefaultMaxBytes is how many queued payload bytes trip one.
	DefaultMaxBytes = 1 << 20
	// DefaultLinger is how long a non-empty queue waits at most, so a trickle
	// of records never sits behind a batch that never fills.
	DefaultLinger = 5 * time.Millisecond
	// MinLinger is the shortest the timer runs at. A sub-millisecond linger
	// spins the timer hot for no gain on a batching producer, so a zero or
	// tiny value is floored here.
	MinLinger = time.Millisecond
)

// PublishFailure reports a publish that did not fully land, with what is
// known about what did.
//
// The two lists are the point. A caller that replayed everything would double
// the records already on the log, and one that replayed nothing would lose
// the records that never arrived. Committed names what must never be
// replayed, and Unconfirmed names what may be: records whose acknowledgment
// was lost and records never sent. They keep the ids their attempt used, so a
// resend stays deduplicated on a server that deduplicates.
type PublishFailure struct {
	// Cause is what went wrong.
	Cause error
	// Stream and Topic are where it went wrong.
	Stream string
	Topic  string
	// Committed are the confirmations that arrived before the failure.
	Committed []iggcon.SendMessagesResponse
	// Unconfirmed are the records with no confirmation.
	Unconfirmed []iggcon.IggyMessage
}

func (e *PublishFailure) Error() string {
	text := "publish failed to " + e.Stream + "/" + e.Topic + ": " + e.Cause.Error()
	if IsStreamOrTopicNotFound(e.Cause) {
		text += ": " + missingTopicFix
	}
	return text
}

// missingTopicFix names the calls that create a missing topic, wherever one is missing.
const missingTopicFix = "the topic does not exist: create the agent topics with Bootstrap, or a memory topic with MemoryTopic"

func (e *PublishFailure) Unwrap() error { return e.Cause }

// BatchingProducerBuilder configures a batching producer.
//
// Every bound is explicit, and the batch flushes on whichever trips first.
type BatchingProducerBuilder struct {
	topic        *Topic
	partitionKey string
	hasKey       bool
	maxRecords   int
	maxBytes     int
	linger       time.Duration
}

// Batching opens a batching producer builder for this topic.
//
// The Apache Iggy Go SDK has no high-level producer, so the batching, the
// linger timer and the failure accounting are this client's own. The
// unbatched publish path is untouched: batching is opted into.
func (topic *Topic) Batching() *BatchingProducerBuilder {
	return &BatchingProducerBuilder{
		topic:      topic,
		maxRecords: DefaultMaxRecords,
		maxBytes:   DefaultMaxBytes,
		linger:     DefaultLinger,
	}
}

// MaxRecords flushes once this many records are queued.
func (builder *BatchingProducerBuilder) MaxRecords(count int) *BatchingProducerBuilder {
	builder.maxRecords = max(count, 1)
	return builder
}

// MaxBytes flushes once the queued payload bytes reach this bound.
func (builder *BatchingProducerBuilder) MaxBytes(bytes int) *BatchingProducerBuilder {
	builder.maxBytes = max(bytes, 1)
	return builder
}

// Linger flushes a non-empty queue after at most this long.
func (builder *BatchingProducerBuilder) Linger(
	linger time.Duration,
) *BatchingProducerBuilder {
	builder.linger = linger
	return builder
}

// PartitionKey pins every record in this handle to one partition.
//
// One key per handle by construction: a batch is flushed whole under one
// partitioning, so ordering within a key is never silently interleaved by
// records that went out under another. Without a key the server's balanced
// partitioner spreads each flushed batch.
func (builder *BatchingProducerBuilder) PartitionKey(
	key string,
) *BatchingProducerBuilder {
	builder.partitionKey, builder.hasKey = key, true
	return builder
}

// Build starts the handle and its linger timer.
func (builder *BatchingProducerBuilder) Build() *BatchingProducer {
	producer := &BatchingProducer{
		topic:        builder.topic,
		partitionKey: builder.partitionKey,
		hasKey:       builder.hasKey,
		maxRecords:   builder.maxRecords,
		maxBytes:     builder.maxBytes,
		linger:       max(builder.linger, MinLinger),
		stop:         make(chan struct{}),
		stopped:      make(chan struct{}),
	}
	go producer.lingerUntilStopped(producer.linger)
	return producer
}

// BatchingProducer batches records over one topic and appends each batch as
// one call.
//
// Flush is the guaranteed path, and Close is the graceful one. A failed
// linger flush never stops the timer: its failure is kept and reported by the
// next Send, Flush or Close, whichever comes first. A Send that finds one
// refuses its own record rather than queueing behind a failed batch, so a
// caller is never left adding to a queue nobody will drain successfully. When
// several batches fail before anyone asks, the report is one failure listing
// the records of all of them.
type BatchingProducer struct {
	topic        *Topic
	partitionKey string
	hasKey       bool
	maxRecords   int
	maxBytes     int
	linger       time.Duration

	// queue guards what is waiting, and sending guards the append so batches
	// reach the log in the order they were queued.
	queue    sync.Mutex
	messages []iggcon.IggyMessage
	payload  int

	sending sync.Mutex
	kept    error
	// failed mirrors whether a failure is kept, so a sender can check without
	// waiting on a flush already in flight.
	failed atomic.Bool

	stop     chan struct{}
	stopOnce sync.Once
	stopped  chan struct{}
}

// lingerUntilStopped flushes on the timer until the handle is closed.
//
// A close lets a flush in flight finish rather than cancelling it: a
// cancelled flush would drop a batch already taken out of the queue, losing
// it silently. Close waits for this goroutine to exit and then flushes, so
// nothing is left queued and no flush starts after Close has returned.
func (producer *BatchingProducer) lingerUntilStopped(linger time.Duration) {
	defer close(producer.stopped)
	ticker := time.NewTicker(linger)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// A timer flush keeps its failure for the caller, so there is
			// nothing here to return it to.
			_ = producer.flush(context.Background(), triggerTimer)
		case <-producer.stop:
			return
		}
	}
}

// flushTrigger is who asked for a flush, which decides who hears about it.
type flushTrigger uint8

const (
	// triggerSend is a sender's own inline flush, so the sender hears it.
	triggerSend flushTrigger = iota
	// triggerTimer is the linger timer, which keeps a failure for the caller.
	triggerTimer
	// triggerCaller is an explicit flush, which hears everything kept so far
	// along with its own.
	triggerCaller
)

// Send enqueues one record, flushing inline when a size bound trips so the
// backpressure lands on the sender rather than on the timer.
//
// A record is governed before it is queued, because a policy that refused it
// must refuse it before it is batched with records that were allowed.
func (producer *BatchingProducer) Send(
	ctx context.Context, payload []byte, headers []iggcon.HeaderEntry,
) error {
	governed, err := producer.topic.laser.governPublish(
		ctx, producer.topic.stream, producer.topic.name, payload, headers)
	if err != nil {
		return err
	}
	if governed != nil {
		payload = governed
	}
	// The id is minted here rather than left for the transport, so a record
	// reported unconfirmed carries the id its attempt used and a resend of it
	// stays deduplicated on a server that deduplicates.
	message, err := iggcon.NewIggyMessage(payload,
		iggcon.WithUserHeaders(headers),
		iggcon.WithID(iggcon.MessageID(NewOperationID())))
	if err != nil {
		return &InvalidError{Reason: "build the record: " + err.Error()}
	}
	if kept := producer.takeKept(); kept != nil {
		// Nothing queues behind a failed batch, so this record is refused
		// rather than accepted into a queue the caller has not heard about.
		return producer.refuse(kept, message)
	}
	producer.queue.Lock()
	producer.messages = append(producer.messages, message)
	producer.payload += len(message.Payload)
	full := len(producer.messages) >= producer.maxRecords ||
		producer.payload >= producer.maxBytes
	producer.queue.Unlock()
	if full {
		return producer.flush(ctx, triggerSend)
	}
	return nil
}

// Flush appends everything queued as one batch, then reports the failure an
// earlier linger flush left. An empty queue appends nothing.
func (producer *BatchingProducer) Flush(ctx context.Context) error {
	return producer.flush(ctx, triggerCaller)
}

// Close flushes, stops the linger timer, and reports whatever is left.
//
// The timer is signalled and waited for rather than cancelled, so its final
// drain completes, and a last flush covers anything enqueued in the meantime.
func (producer *BatchingProducer) Close(ctx context.Context) error {
	producer.stopOnce.Do(func() { close(producer.stop) })
	<-producer.stopped
	return producer.flush(ctx, triggerCaller)
}

// Bounds are the record and byte counts this producer flushes on, and the
// linger it waits at most.
//
// A caller watching its own backpressure reads them back rather than
// remembering what it asked for, and a bound below one reads back as one:
// flushing on nothing is not a bound.
func (producer *BatchingProducer) Bounds() (records, bytes int, linger time.Duration) {
	return producer.maxRecords, producer.maxBytes, producer.linger
}

// Queued is how many records are waiting, for a caller watching its own
// backpressure.
func (producer *BatchingProducer) Queued() int {
	producer.queue.Lock()
	defer producer.queue.Unlock()
	return len(producer.messages)
}

// flush appends one batch and settles who hears the result.
func (producer *BatchingProducer) flush(
	ctx context.Context, trigger flushTrigger,
) error {
	// Held across the append, so batches reach the log in queue order.
	producer.sending.Lock()
	defer producer.sending.Unlock()

	producer.queue.Lock()
	batch := producer.messages
	producer.messages, producer.payload = nil, 0
	producer.queue.Unlock()

	var sent error
	if len(batch) > 0 {
		response, err := producer.topic.sendMessages(
			ctx, batch, producer.partitionKey, producer.hasKey)
		if err != nil {
			sent = producer.failure(err, response, batch)
		}
	}
	settled := producer.settle(trigger, sent)
	producer.failed.Store(producer.kept != nil)
	return settled
}

// failure is what a failed append reports: what was confirmed, and the
// records that were not.
func (producer *BatchingProducer) failure(
	cause error, response *iggcon.SendMessagesResponse, batch []iggcon.IggyMessage,
) error {
	failure := &PublishFailure{
		Cause:       cause,
		Stream:      producer.topic.stream,
		Topic:       producer.topic.name,
		Unconfirmed: batch,
	}
	if response != nil {
		failure.Committed = []iggcon.SendMessagesResponse{*response}
	}
	return failure
}

// settle decides who hears about one flush result.
//
// A sender hears its own inline flush, since it is the one being pushed back
// on. The timer has nobody to tell, so it keeps the failure. An explicit
// flush hears everything kept so far together with its own result, which is
// what makes a kept failure reportable exactly once.
func (producer *BatchingProducer) settle(trigger flushTrigger, sent error) error {
	switch trigger {
	case triggerSend:
		return sent
	case triggerTimer:
		if sent != nil {
			producer.kept = mergeFailures(producer.kept, sent)
		}
		return nil
	default:
		kept := producer.kept
		producer.kept = nil
		switch {
		case kept == nil:
			return sent
		case sent == nil:
			return kept
		default:
			return mergeFailures(kept, sent)
		}
	}
}

// takeKept is the failure a linger flush left, taken so it is reported once.
func (producer *BatchingProducer) takeKept() error {
	if !producer.failed.Load() {
		return nil
	}
	producer.sending.Lock()
	defer producer.sending.Unlock()
	kept := producer.kept
	producer.kept = nil
	producer.failed.Store(false)
	return kept
}

// refuse reports a kept failure to a sender, with the sender's own record
// added to the unconfirmed ones because it was never queued.
func (producer *BatchingProducer) refuse(
	kept error, record iggcon.IggyMessage,
) error {
	var failure *PublishFailure
	if errors.As(kept, &failure) {
		failure.Unconfirmed = append(failure.Unconfirmed, record)
		return failure
	}
	return &PublishFailure{
		Cause:       kept,
		Stream:      producer.topic.stream,
		Topic:       producer.topic.name,
		Unconfirmed: []iggcon.IggyMessage{record},
	}
}

// mergeFailures folds two failed batches into one report, so a caller hears
// about every record once rather than one batch at a time.
//
// The fold is narrow on purpose: nothing queues behind a failure, so a second
// batch can only fail while the first failure is still held if a send queued
// its record in the window between finding no kept failure and the timer
// leaving one. That window is small and real, and losing those records would
// be worse than the branch being hard to reach.
func mergeFailures(earlier, later error) error {
	if earlier == nil {
		return later
	}
	var first, next *PublishFailure
	if !errors.As(earlier, &first) || !errors.As(later, &next) {
		// A failure that carries no records cannot be folded into one that
		// does, so the first cause stands rather than being replaced.
		return earlier
	}
	first.Committed = append(first.Committed, next.Committed...)
	first.Unconfirmed = append(first.Unconfirmed, next.Unconfirmed...)
	return first
}

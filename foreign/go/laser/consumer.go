package laser

import (
	"context"
	"errors"
	"time"

	"github.com/apache/iggy/foreign/go/client/reader"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// DefaultConsumerPollWait is how long a consumer waits for records before
// answering empty, when the caller names no wait.
const DefaultConsumerPollWait = time.Second

// Record is one record a consumer handed over, with the spine already read off
// its headers.
//
// Provenance is nil when the record carries none, which is the normal case for
// a plain data topic. A record that carries a broken spine is reported rather
// than delivered with it silently missing, because the fence and the deadline
// are both gates: losing them would let a record through that should not pass.
type Record struct {
	PartitionID uint32
	Message     iggcon.IggyMessage
	Provenance  *Provenance
	// Checkpoint is the position after this record, which a commit stores.
	Checkpoint iggcon.Checkpoint
	// EndsBatch marks the last record of the batch the reader read, which is
	// where a caller can commit without splitting a batch.
	EndsBatch bool
}

// Payload is the record's body.
func (record *Record) Payload() []byte { return record.Message.Payload }

// Offset is the record's position in its partition.
func (record *Record) Offset() uint64 { return record.Message.Header.Offset }

// Expired reports a record past its deadline, which a consumer dead-letters
// rather than working on: nobody is waiting for the answer any more.
func (record *Record) Expired(now time.Time) bool {
	if record.Provenance == nil || record.Provenance.DeadlineAt == nil {
		return false
	}
	return now.After(*record.Provenance.DeadlineAt)
}

// ConsumerOptions configure a consumer. The zero value takes every default.
type ConsumerOptions struct {
	// Partitions narrows a standalone consumer to these partitions. A group
	// member leaves it empty and is assigned partitions by the server.
	Partitions []uint32
	// Start is where a consumer with no stored offset begins. The default
	// resumes from the stored offset, or the first record when there is none.
	Start *iggcon.PollingStrategy
	// PollWait is how long one read waits for records before answering empty.
	PollWait time.Duration
	// Count is the most records one read returns.
	Count uint32
	// AutoCommit stores the consumer's position as it goes. Nil commits
	// nothing, leaving the position to explicit Commit calls, which is what a
	// handler with side effects wants: a record is then re-delivered after a
	// crash rather than silently skipped.
	AutoCommit *iggcon.AutoCommit
	// Encryption opens each record's payload and user-header block with this
	// key, for a topic published under the same one. Nil reads records as
	// they are stored.
	Encryption *Encryption
}

// PollOptions renders the read limits a consumer opened with these options
// will run under, so a caller can see them without opening one.
//
// The transport's minimum batch size is one, and a named count is at least
// one, so the two bounds cannot cross and are not clamped here.
func (options ConsumerOptions) PollOptions() iggcon.PollOptions {
	poll := iggcon.DefaultPollOptions()
	poll.MaxWait = DefaultConsumerPollWait
	if options.PollWait > 0 {
		poll.MaxWait = options.PollWait
	}
	if options.Count > 0 {
		poll.Count = options.Count
	}
	if options.AutoCommit != nil {
		poll.Commit = iggcon.AutoCommitPolicy(*options.AutoCommit)
	}
	return poll
}

func (options ConsumerOptions) readerOptions() []reader.Option {
	built := []reader.Option{reader.WithPollOptions(options.PollOptions())}
	if len(options.Partitions) > 0 {
		built = append(built, reader.WithPartitions(options.Partitions...))
	}
	if options.Start != nil {
		built = append(built, reader.WithStart(*options.Start))
	}
	return built
}

// Consumer reads a topic's records one at a time, as a standalone consumer or
// as a member of a consumer group.
//
// One caller at a time reads from it. Reading from two goroutines would
// interleave the commits as well as the records, so a consumer is handed to one
// worker rather than shared.
type Consumer struct {
	inner      *reader.IggyConsumer
	topic      *Topic
	encryption *Encryption
}

// Consumer opens a standalone consumer on this topic under a name, which is
// what the server stores its offset against.
//
// A standalone consumer names the partitions it reads, because nothing assigns
// them to it: without that it would read none. A consumer that should be
// assigned partitions joins a group instead.
func (topic *Topic) Consumer(
	ctx context.Context, name string, options ConsumerOptions,
) (*Consumer, error) {
	if len(options.Partitions) == 0 {
		return nil, &InvalidError{Reason: "a standalone consumer names the partitions " +
			"it reads: set ConsumerOptions.Partitions, or join a group to be assigned them"}
	}
	identifier, err := iggcon.NewIdentifier(name)
	if err != nil {
		return nil, &InvalidError{Reason: "consumer name: " + err.Error()}
	}
	return topic.openConsumer(ctx, iggcon.NewSingleConsumer(identifier), options)
}

// ConsumerGroup joins a consumer group on this topic.
//
// The server assigns partitions across the group's members and rebalances as
// members come and go, so a group member names no partitions of its own.
func (topic *Topic) ConsumerGroup(
	ctx context.Context, group string, options ConsumerOptions,
) (*Consumer, error) {
	if len(options.Partitions) > 0 {
		return nil, &InvalidError{Reason: "a consumer group member is assigned its " +
			"partitions by the server, so it must not name its own"}
	}
	identifier, err := iggcon.NewIdentifier(group)
	if err != nil {
		return nil, &InvalidError{Reason: "consumer group name: " + err.Error()}
	}
	return topic.openConsumer(ctx, iggcon.NewGroupConsumer(identifier), options)
}

func (topic *Topic) openConsumer(
	ctx context.Context, consumer iggcon.Consumer, options ConsumerOptions,
) (*Consumer, error) {
	if topic.stream == "" {
		return nil, errNoStream()
	}
	client, err := topic.laser.connected()
	if err != nil {
		return nil, err
	}
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return nil, err
	}
	inner, err := reader.NewIggyConsumer(ctx, client, streamID, topicID, consumer,
		options.readerOptions()...)
	if err != nil {
		return nil, &TransportError{Reason: "open the consumer", Cause: err}
	}
	return &Consumer{inner: inner, topic: topic, encryption: options.Encryption}, nil
}

// Next hands over the next record, reading its provenance off the headers.
//
// A read that failed is reported and the consumer goes on, so a caller that
// retries keeps its place rather than restarting.
func (consumer *Consumer) Next(ctx context.Context) (*Record, error) {
	received, err := consumer.inner.Next(ctx)
	if err != nil {
		return nil, consumeError(err)
	}
	return openRecord(received, consumer.encryption)
}

// Each reads records until the context ends or handle returns an error, which
// it passes back.
//
// Under auto-commit a record counts once its handler returns successfully, so
// a handler that fails leaves it uncounted and it is delivered again.
func (consumer *Consumer) Each(
	ctx context.Context, handle func(context.Context, *Record) error,
) error {
	err := consumer.inner.Consume(ctx, func(inner context.Context, received reader.ReceivedMessage) error {
		record, err := openRecord(received, consumer.encryption)
		if err != nil {
			return err
		}
		return handle(inner, record)
	})
	if err != nil && !errors.Is(err, context.Canceled) &&
		!errors.Is(err, context.DeadlineExceeded) {
		return consumeError(err)
	}
	return nil
}

// Commit stores a position, so a restart resumes after it.
func (consumer *Consumer) Commit(ctx context.Context, checkpoint iggcon.Checkpoint) error {
	if err := consumer.inner.Commit(ctx, checkpoint); err != nil {
		return &TransportError{Reason: "commit the consumer position", Cause: err}
	}
	return nil
}

// Mark counts a position under auto-commit without storing it immediately,
// which is how records processed concurrently count once each is done.
func (consumer *Consumer) Mark(checkpoint iggcon.Checkpoint) error {
	if err := consumer.inner.Mark(checkpoint); err != nil {
		return &InvalidError{Reason: "mark the consumer position: " + err.Error()}
	}
	return nil
}

// Seek moves one partition's read position.
func (consumer *Consumer) Seek(
	ctx context.Context, partitionID uint32, start iggcon.PollingStrategy,
) error {
	if err := consumer.inner.Seek(ctx, partitionID, start); err != nil {
		return &TransportError{Reason: "seek the consumer", Cause: err}
	}
	return nil
}

// Partitions are the partitions this consumer currently reads. For a group
// member they change as the server rebalances.
func (consumer *Consumer) Partitions() []uint32 { return consumer.inner.Partitions() }

// Topic is the topic this consumer reads.
func (consumer *Consumer) Topic() *Topic { return consumer.topic }

// Close stops the consumer, storing its position first under auto-commit.
func (consumer *Consumer) Close(ctx context.Context) error {
	if err := consumer.inner.Shutdown(ctx); err != nil {
		return &TransportError{Reason: "close the consumer", Cause: err}
	}
	return nil
}

// OpenRecord reads the agentic spine off a record, for a caller holding one
// from any path rather than from a consumer.
//
// A record carrying no conversation carries no spine, which is the normal case
// on a plain data topic, so it is returned with none rather than refused. A
// spine that is present but broken is a different thing and is reported: the
// fence and the deadline are both gates, and handing over a record with them
// silently missing would let through what the gates exist to stop.
func OpenRecord(message iggcon.IggyMessage) (*Record, error) {
	record := &Record{Message: message}
	provenance, err := ProvenanceFrom(&message)
	switch {
	case err == nil:
		record.Provenance = provenance
	case errors.As(err, new(*NoProvenanceError)):
		// No spine at all, which a plain data topic is full of.
	default:
		return nil, err
	}
	return record, nil
}

// DeliveredRecord opens a record a reader handed over, adding what only the
// delivery knows: where the record came from and where to commit.
//
// It is the form a caller driving its own reader wants, which a RecordSource
// feeding an AgentRuntime is. A record whose spine did not decode is refused,
// but the refusal carries the record: a runtime has to be able to keep those
// bytes on the dead-letter topic, and a refusal naming only a reason would
// lose the work inside them.
func DeliveredRecord(
	partitionID uint32, message iggcon.IggyMessage,
	checkpoint iggcon.Checkpoint, endsBatch bool,
) (*Record, error) {
	record, err := OpenRecord(message)
	if err != nil {
		return nil, &UndecodableRecord{
			PartitionID: partitionID,
			Message:     message,
			Checkpoint:  checkpoint,
			Cause:       err,
		}
	}
	record.PartitionID = partitionID
	record.Checkpoint = checkpoint
	record.EndsBatch = endsBatch
	return record, nil
}

// openRecord opens what this consumer's reader handed over.
func openRecord(
	received reader.ReceivedMessage, encryption *Encryption,
) (*Record, error) {
	message := received.Message
	if encryption != nil {
		opened, err := encryption.open(message)
		if err != nil {
			return nil, err
		}
		message = opened
	}
	return DeliveredRecord(received.PartitionID, message,
		received.Checkpoint, received.EndsBatch)
}

func consumeError(err error) error {
	var readError *reader.ReadError
	if errors.As(err, &readError) {
		return &TransportError{Reason: "read records", Cause: err}
	}
	return err
}

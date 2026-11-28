package laser

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/apache/iggy/foreign/go/client/reader"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// DefaultConsumerPollWait is how long the server holds a read that finds
// nothing new, when the caller names no wait. A record ends the wait at once.
const DefaultConsumerPollWait = time.Second

// DefaultConsumerPollRetryInterval is how long a failed read waits before it is sent again, when the caller names no interval.
const DefaultConsumerPollRetryInterval = time.Second

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
	// PollWait is how long the server holds a read that finds nothing new.
	// Zero takes DefaultConsumerPollWait.
	PollWait time.Duration
	// Count is the most records one read returns.
	Count uint32
	// MinCount is the minimum record count for a server-held native poll.
	MinCount uint32
	// MaxBytes caps the reply bytes of native and filtered reads.
	MaxBytes uint32
	// AutoCommit controls when processed offsets are stored. The default
	// stores at the end of each processed batch.
	AutoCommit *iggcon.AutoCommit
	// ManualCommit leaves the position to explicit Commit calls.
	ManualCommit bool
	// Encryption opens each record's payload and user-header block with this
	// key, for a topic published under the same one. Nil reads records as
	// they are stored.
	Encryption *Encryption
	// SkipGroupCreate joins a consumer group without creating it when it is
	// absent, so a member refuses rather than bringing a group into being.
	//
	// The default creates it, matching the Rust client's own `create_group`
	// default: a server holds no group until someone makes one, so a join
	// that never created would answer "consumer group not found" and leave
	// every caller to create it first. Set this where only an operator may
	// define the group.
	SkipGroupCreate bool
	// PollRetryInterval is how long a failed read waits before it is sent
	// again. The default is DefaultConsumerPollRetryInterval.
	PollRetryInterval time.Duration
	// InitRetries is how many times a failed open is tried again when the
	// failure is retryable, each after InitRetryInterval.
	InitRetries uint32
	// InitRetryInterval is the wait between two tries of the open.
	InitRetryInterval time.Duration
	// AllowReplay lets StoreOffset move a stored offset back. Without it an
	// offset at or below the last one this consumer stored is not sent.
	AllowReplay bool
}

// validate refuses retry settings no open can run under.
func (options ConsumerOptions) validate() error {
	if options.PollRetryInterval < 0 {
		return &InvalidError{Reason: "consumer polling retry interval must not be negative"}
	}
	if options.InitRetries > 0 && options.InitRetryInterval <= 0 {
		return &InvalidError{Reason: "consumer initialization retry interval must be greater than zero"}
	}
	return nil
}

// pollRetryInterval is the wait before a failed read is sent again.
func (options ConsumerOptions) pollRetryInterval() time.Duration {
	if options.PollRetryInterval > 0 {
		return options.PollRetryInterval
	}
	return DefaultConsumerPollRetryInterval
}

// retryingOpen runs an open again after a retryable failure, as often as the options allow.
func retryingOpen[T any](ctx context.Context, options ConsumerOptions, open func() (T, error)) (T, error) {
	for left := options.InitRetries; ; left-- {
		opened, err := open()
		if err == nil || left == 0 || !RetryableError(err) {
			return opened, err
		}
		if err := sleepWithin(ctx, options.InitRetryInterval); err != nil {
			return opened, err
		}
	}
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
	if options.MinCount > 0 {
		poll.MinCount = options.MinCount
	}
	if options.MaxBytes > 0 {
		poll.MaxBytes = options.MaxBytes
	}
	if !options.ManualCommit {
		auto := iggcon.AutoCommit{Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryBatch}, After: iggcon.CommitAfterProcessing}
		if options.AutoCommit != nil {
			auto = *options.AutoCommit
		}
		poll.Commit = iggcon.AutoCommitPolicy(auto)
	}
	return poll
}

func (options ConsumerOptions) readerOptions() []reader.Option {
	built := []reader.Option{reader.WithPollOptions(options.PollOptions()), reader.WithRetryInterval(options.pollRetryInterval())}
	if len(options.Partitions) > 0 {
		built = append(built, reader.WithPartitions(options.Partitions...))
	}
	if options.Start != nil {
		built = append(built, reader.WithStart(*options.Start))
	}
	return built
}

type consumerReader interface {
	Next(context.Context) (reader.ReceivedMessage, error)
	Consume(context.Context, func(context.Context, reader.ReceivedMessage) error) error
	Commit(context.Context, iggcon.Checkpoint) error
	Mark(iggcon.Checkpoint) error
	Seek(context.Context, uint32, iggcon.PollingStrategy) error
	Partitions() []uint32
	Shutdown(context.Context) error
}

// Consumer reads a topic's records one at a time, as a standalone consumer or
// as a member of a consumer group.
//
// One caller at a time reads from it. Reading from two goroutines would
// interleave the commits as well as the records, so a consumer is handed to one
// worker rather than shared.
type Consumer struct {
	inner consumerReader
	topic *Topic
	// identity is who the server stores offsets against, the consumer name
	// or the group this member joined.
	identity    iggcon.Consumer
	encryption  *Encryption
	allowReplay bool
	offsets     consumerOffsets
}

// consumerOffsets is the consumer's own record of what it handed over and stored.
type consumerOffsets struct {
	mutex    sync.Mutex
	consumed map[uint32]uint64
	stored   map[uint32]uint64
	latest   *uint32
}

func (offsets *consumerOffsets) delivered(partition uint32, offset uint64) {
	offsets.mutex.Lock()
	defer offsets.mutex.Unlock()
	if offsets.consumed == nil {
		offsets.consumed = make(map[uint32]uint64)
	}
	offsets.consumed[partition] = offset
	offsets.latest = &partition
}

func (offsets *consumerOffsets) store(partition uint32, offset uint64) {
	offsets.mutex.Lock()
	defer offsets.mutex.Unlock()
	if offsets.stored == nil {
		offsets.stored = make(map[uint32]uint64)
	}
	offsets.stored[partition] = offset
}

func (offsets *consumerOffsets) forget(partition *uint32, consumed bool) {
	offsets.mutex.Lock()
	defer offsets.mutex.Unlock()
	if partition == nil {
		partition = offsets.latest
	}
	if partition == nil {
		return
	}
	if consumed {
		delete(offsets.consumed, *partition)
		return
	}
	delete(offsets.stored, *partition)
}

func (offsets *consumerOffsets) read(partition uint32, stored bool) (uint64, bool) {
	offsets.mutex.Lock()
	defer offsets.mutex.Unlock()
	from := offsets.consumed
	if stored {
		from = offsets.stored
	}
	offset, known := from[partition]
	return offset, known
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
	return topic.Group(group).Consumer(ctx, options)
}

func (topic *Topic) openConsumer(
	ctx context.Context, consumer iggcon.Consumer, options ConsumerOptions,
) (*Consumer, error) {
	if topic.stream == "" {
		return nil, errNoStream()
	}
	if err := options.validate(); err != nil {
		return nil, err
	}
	client, err := topic.laser.connected()
	if err != nil {
		return nil, err
	}
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return nil, err
	}
	inner, err := retryingOpen(ctx, options, func() (*reader.IggyConsumer, error) {
		opened, err := reader.NewIggyConsumer(ctx, client, streamID, topicID, consumer, options.readerOptions()...)
		if err != nil {
			return nil, &TransportError{Reason: "open the consumer", Cause: err}
		}
		return opened, nil
	})
	if err != nil {
		return nil, err
	}
	return &Consumer{inner: inner, topic: topic, identity: consumer,
		encryption: options.Encryption, allowReplay: options.AllowReplay}, nil
}

// Next hands over the next record, reading its provenance off the headers.
//
// A read that failed is reported and the consumer goes on, so a caller that
// retries keeps its place rather than restarting. A partition its reader
// stopped is resumed first, after its stored offset or past a record that
// does not decode.
func (consumer *Consumer) Next(ctx context.Context) (*Record, error) {
	for {
		received, err := consumer.inner.Next(ctx)
		if err == nil {
			consumer.offsets.delivered(received.PartitionID, received.Message.Header.Offset)
			return openRecord(received, consumer.encryption)
		}
		if reported := consumer.recovered(ctx, err); reported != nil {
			return nil, reported
		}
	}
}

// Each reads records until the context ends or handle returns an error, which
// it passes back.
//
// Under auto-commit a record counts once its handler returns successfully. A
// handler that fails leaves it uncounted, so a restart delivers it again
// unless a later record of its partition counted first. A read that failed
// is reported as Next reports it, after the partition it stopped is resumed.
func (consumer *Consumer) Each(
	ctx context.Context, handle func(context.Context, *Record) error,
) error {
	for {
		err := consumer.inner.Consume(ctx, func(inner context.Context, received reader.ReceivedMessage) error {
			consumer.offsets.delivered(received.PartitionID, received.Message.Header.Offset)
			record, err := openRecord(received, consumer.encryption)
			if err != nil {
				return err
			}
			return handle(inner, record)
		})
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		if reported := consumer.recovered(ctx, err); reported != nil {
			return reported
		}
	}
}

// recovered resumes the partition a read error stopped and returns the error
// the caller still sees, nil when the read goes on.
func (consumer *Consumer) recovered(ctx context.Context, err error) error {
	var failure *reader.ReadError
	if !errors.As(err, &failure) {
		return err
	}
	recovery, operation := recoveryOf(failure)
	if recovery == recoveryReport {
		return consumeError(err)
	}
	partition := *failure.PartitionID
	start := iggcon.NextPollingStrategy()
	var reported error
	switch recovery {
	case recoveryAbandon:
		slog.WarnContext(ctx, fmt.Sprintf("Abandoning operation %d with an unknown outcome, which blocks partition %d. "+
			"The partition resumes after its stored offset", operation, partition),
			"partition_id", partition, "operation", operation, "error", failure.Err)
		consumer.abandon(ctx, partition, operation)
	case recoverySkip:
		slog.WarnContext(ctx, fmt.Sprintf("Skipping a record that does not decode in partition %d", partition),
			"partition_id", partition, "error", failure.Err)
		start, reported = *failure.Skip, consumeError(err)
	case recoveryResume:
		slog.WarnContext(ctx, fmt.Sprintf("Partition %d has a new history. It resumes after its stored offset", partition),
			"partition_id", partition, "error", failure.Err)
	case recoveryStopped:
		slog.WarnContext(ctx, fmt.Sprintf("The reader stopped partition %d after lasting refusals. "+
			"It resumes after its stored offset", partition), "partition_id", partition, "error", failure.Err)
		reported = consumeError(err)
	}
	if failed := consumer.inner.Seek(ctx, partition, start); failed != nil {
		slog.WarnContext(ctx, fmt.Sprintf("Resuming partition %d failed", partition),
			"partition_id", partition, "error", failed)
		if reported == nil {
			reported = &TransportError{Reason: "resume the partition", Cause: failed}
		}
	}
	return reported
}

// abandon releases a read with an unknown outcome, which frees the partition it blocks.
func (consumer *Consumer) abandon(ctx context.Context, partition uint32, operation uint64) {
	// The reader's own report of a lost read names no operation, since it abandoned it already.
	if operation == 0 {
		return
	}
	client, err := consumer.topic.laser.connected()
	if err == nil {
		err = client.AbandonOperation(ctx, operation)
	}
	// The reader reports a lost read for each of its partitions, and the first abandon frees them all.
	if err != nil && !errors.Is(err, ierror.ErrOperationNotRetained) {
		slog.WarnContext(ctx, fmt.Sprintf("Abandoning operation %d failed", operation),
			"partition_id", partition, "operation", operation, "error", err)
	}
}

// Commit stores a position, so a restart resumes after it. A position at or
// below the last one this consumer stored is not sent unless
// ConsumerOptions.AllowReplay is set.
func (consumer *Consumer) Commit(ctx context.Context, checkpoint iggcon.Checkpoint) error {
	// A group that runs a policy records its own stores, since it drops the commit of a partition it lost.
	_, policy := consumer.inner.(*policyConsumer)
	if !policy && !consumer.allowReplay {
		if stored, known := consumer.LastStoredOffset(checkpoint.PartitionID()); known && stored >= checkpoint.Offset() {
			return nil
		}
	}
	if err := consumer.inner.Commit(ctx, checkpoint); err != nil {
		return &TransportError{Reason: "commit the consumer position", Cause: err}
	}
	if !policy {
		consumer.offsets.store(checkpoint.PartitionID(), checkpoint.Offset())
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
	consumer.offsets.forget(&partitionID, true)
	return nil
}

// LastConsumedOffset is the offset of the last record this consumer handed
// over on a partition, and whether it handed one over at all.
func (consumer *Consumer) LastConsumedOffset(partition uint32) (uint64, bool) {
	return consumer.offsets.read(partition, false)
}

// LastStoredOffset is the last offset this consumer stored on a partition by
// Commit, StoreOffset, or the acknowledgments of a group that runs a policy.
// It is local bookkeeping: StoredOffset reads what the server holds, and the
// automatic commits of a consumer without a group policy are not seen here.
func (consumer *Consumer) LastStoredOffset(partition uint32) (uint64, bool) {
	return consumer.offsets.read(partition, true)
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

// readRecovery is how the consumer resumes a partition that a read error stopped.
type readRecovery uint8

const (
	// recoveryReport is an error the reader retries by itself.
	recoveryReport readRecovery = iota
	// recoveryAbandon is a read with an unknown outcome that blocks its partition.
	recoveryAbandon
	// recoverySkip is a record that does not decode, reported once and read past.
	recoverySkip
	// recoveryResume is a partition with a new history, resumed without a report.
	recoveryResume
	// recoveryStopped is a partition the reader stopped after lasting refusals.
	recoveryStopped
)

// recoveryOf classifies a read error and names the operation an abandon releases.
func recoveryOf(failure *reader.ReadError) (readRecovery, uint64) {
	if failure.PartitionID == nil {
		return recoveryReport, 0
	}
	var unknown ierror.FetchProgressUncertain
	if errors.As(failure.Err, &unknown) {
		return recoveryAbandon, unknown.OperationID
	}
	var blocked ierror.PartitionLaneBlocked
	if errors.As(failure.Err, &blocked) {
		return recoveryAbandon, blocked.OperationID
	}
	// The reader counts every refusal the server coded toward stopping a partition.
	var refused interface{ Code() ierror.Code }
	switch {
	case failure.Skip != nil:
		return recoverySkip, 0
	case errors.Is(failure.Err, ierror.ErrHistoryUnavailable):
		return recoveryResume, 0
	case errors.As(failure.Err, &refused):
		return recoveryStopped, 0
	}
	return recoveryReport, 0
}

func consumeError(err error) error {
	var readError *reader.ReadError
	if errors.As(err, &readError) {
		return &TransportError{Reason: "read records", Cause: err}
	}
	return err
}

// StoredOffset is what the server holds for a consumer on one partition.
type StoredOffset struct {
	// Offset is the offset the server stored for the consumer.
	Offset uint64
	// CurrentOffset is the partition's current head, so a caller can see how
	// far behind the stored position is.
	CurrentOffset uint64
}

// StoredOffset reads the offset the server stores for this consumer, or for
// its group, on a partition, with the partition's current head.
//
// It is a server read rather than this client's own bookkeeping, so it
// answers what a restart would resume from, including what another member of
// the group stored. Nil when the server holds nothing for it yet.
func (consumer *Consumer) StoredOffset(
	ctx context.Context, partition uint32,
) (*StoredOffset, error) {
	client, streamID, topicID, err := consumer.offsetTarget()
	if err != nil {
		return nil, err
	}
	info, err := client.GetConsumerOffset(
		ctx, consumer.identity, streamID, topicID, &partition)
	if err != nil {
		if isMissingOffset(err) {
			return nil, nil
		}
		return nil, &TransportError{Reason: "read the stored offset", Cause: err}
	}
	if info == nil {
		return nil, nil
	}
	return &StoredOffset{Offset: info.StoredOffset, CurrentOffset: info.CurrentOffset}, nil
}

// StoreOffset stores an explicit offset for a partition, nil for the
// consumer's own.
//
// This is the escape hatch, not the usual path: it names a position nothing
// delivered, so a caller that stores one past what it handled skips records
// silently. Commit a delivered record instead, and keep this for a
// deliberate reposition. An offset at or below the last one this consumer
// stored is not sent unless ConsumerOptions.AllowReplay is set.
func (consumer *Consumer) StoreOffset(
	ctx context.Context, offset uint64, partition *uint32,
) error {
	named := partition
	if named == nil {
		consumer.offsets.mutex.Lock()
		named = consumer.offsets.latest
		consumer.offsets.mutex.Unlock()
	}
	if named != nil && !consumer.allowReplay {
		if stored, known := consumer.LastStoredOffset(*named); known && stored >= offset {
			return nil
		}
	}
	client, streamID, topicID, err := consumer.offsetTarget()
	if err != nil {
		return err
	}
	if err := client.StoreConsumerOffset(
		ctx, consumer.identity, streamID, topicID, offset, partition); err != nil {
		return &TransportError{Reason: "store the consumer offset", Cause: err}
	}
	if named != nil {
		consumer.offsets.store(*named, offset)
	}
	return nil
}

// DeleteOffset removes the server's stored offset for a partition, nil for
// the consumer's own.
//
// A consumer with no stored offset starts where its options say rather than
// where it left off, so this is how a reader is sent back to the beginning
// of what the topic still holds.
func (consumer *Consumer) DeleteOffset(ctx context.Context, partition *uint32) error {
	client, streamID, topicID, err := consumer.offsetTarget()
	if err != nil {
		return err
	}
	if err := client.DeleteConsumerOffset(
		ctx, consumer.identity, streamID, topicID, partition); err != nil {
		if isMissingOffset(err) {
			return nil
		}
		return &TransportError{Reason: "delete the consumer offset", Cause: err}
	}
	consumer.offsets.forget(partition, false)
	return nil
}

// offsetTarget is the live client and the identifiers an offset call needs.
func (consumer *Consumer) offsetTarget() (
	iggcon.Client, iggcon.Identifier, iggcon.Identifier, error,
) {
	client, err := consumer.topic.laser.connected()
	if err != nil {
		return nil, iggcon.Identifier{}, iggcon.Identifier{}, err
	}
	streamID, topicID, err := consumer.topic.identifiers()
	if err != nil {
		return nil, iggcon.Identifier{}, iggcon.Identifier{}, err
	}
	return client, streamID, topicID, nil
}

// isMissingOffset reports a server that holds no offset for a consumer,
// which is absence rather than failure: offset zero is a stored position
// like any other, so it cannot stand in for "none".
func isMissingOffset(err error) bool {
	return errors.Is(err, ierror.ConsumerOffsetNotFound{}) || isMissingResource(err)
}

package laser

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/apache/iggy/foreign/go/client/reader"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// FilterFaultError identifies a record that stopped a policy-aware consumer.
type FilterFaultError struct {
	PartitionID uint32
	Offset      uint64
	Reason      wire.FaultReason
	Oversized   bool
	// Page is the blocked page a group reader passes to Skip, nil from a consumer.
	Page *MatchedPage
}

// Error names the partition, the offset and the reason the policy stopped at.
func (err *FilterFaultError) Error() string {
	return "group policy stopped on partition " + strconv.FormatUint(uint64(err.PartitionID), 10) + " at offset " + strconv.FormatUint(err.Offset, 10) + ": " + string(err.Reason)
}

// Skip acknowledges exactly the record that stopped this filtered partition.
func (consumer *Consumer) Skip(ctx context.Context, partition uint32, offset uint64) error {
	p, ok := consumer.inner.(*policyConsumer)
	if !ok {
		return &UnsupportedError{Surface: "filtered consumer skip"}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	part := p.partitions[partition]
	if part == nil || part.blocked == nil {
		return &InvalidError{Reason: "the partition is not blocked on a filter fault"}
	}
	if err := p.member.Skip(ctx, part.blocked, offset); err != nil {
		return err
	}
	part.blocked = nil
	return nil
}

type policyPartition struct {
	read         *FilteredReadRequest
	delivered    []*policyDelivery
	byCheckpoint map[iggcon.Checkpoint]*policyDelivery
	pending      int
	counted      int
	blocked      *MatchedPage
	expected     *iggcon.PartitionContext
	stored       *iggcon.Checkpoint
}

type policyDelivery struct {
	page       *MatchedPage
	checkpoint iggcon.Checkpoint
	marked     bool
	endsBatch  bool
}

type policyConsumer struct {
	member          *GroupReader
	consumer        *Consumer
	options         ConsumerOptions
	poll            iggcon.PollOptions
	mu              sync.Mutex
	assigned        []uint32
	partitions      map[uint32]*policyPartition
	turn            int
	syncedAt        time.Time
	syncDue         bool
	rejoins         uint64
	lost            map[uint32]bool
	ready           map[uint32]bool
	batch           *MatchedPage
	batchDeliveries []policyDelivery
	index           int
	previous        *policyDelivery
	backgroundErr   error
	marked          chan struct{}
	lifetime        context.Context
	stop            context.CancelFunc
	done            chan struct{}
}

func openPolicyConsumer(ctx context.Context, group *ConsumerGroup, identity iggcon.Consumer, options ConsumerOptions) (*Consumer, error) {
	poll := options.PollOptions()
	if err := poll.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	if err := options.validate(); err != nil {
		return nil, err
	}
	if _, err := filterStart(0, options.Start, false); err != nil {
		return nil, err
	}
	member, err := retryingOpen(ctx, options, func() (*GroupReader, error) { return group.OpenReader(ctx) })
	if err != nil {
		return nil, err
	}
	member.retry = options.pollRetryInterval()
	ctxLife, cancel := context.WithCancel(context.Background())
	c := &Consumer{topic: group.topic, identity: identity, encryption: options.Encryption, allowReplay: options.AllowReplay}
	p := &policyConsumer{member: member, consumer: c, poll: poll, options: options, partitions: make(map[uint32]*policyPartition), lost: make(map[uint32]bool), ready: make(map[uint32]bool), lifetime: ctxLife, stop: cancel, done: make(chan struct{}), marked: make(chan struct{}, 1)}
	c.inner = p
	if err := p.sync(ctx); err != nil {
		cancel()
		return nil, errors.Join(err, member.Close(ctx))
	}
	if poll.Commit.Kind == iggcon.CommitAuto && (poll.Commit.Auto.Interval > 0 || poll.Commit.Auto.After == iggcon.CommitAfterMarked) {
		go p.flushPeriodically()
	} else {
		close(p.done)
	}
	return c, nil
}

func filterStart(partition uint32, start *iggcon.PollingStrategy, validatePartition bool) (wire.FilteredStart, error) {
	if start == nil {
		return wire.StartAtNext(), nil
	}
	if validatePartition {
		if err := start.RequirePartition(partition); err != nil {
			return wire.FilteredStart{}, err
		}
	}
	switch start.Kind {
	case iggcon.POLLING_FIRST:
		return wire.StartAtFirst(), nil
	case iggcon.POLLING_LAST:
		return wire.StartAtLast(), nil
	case iggcon.POLLING_NEXT:
		return wire.StartAtNext(), nil
	case iggcon.POLLING_OFFSET:
		return wire.StartFromOffset(start.Value), nil
	case iggcon.POLLING_TIMESTAMP:
		return wire.StartFromTimestamp(start.Value), nil
	default:
		return wire.FilteredStart{}, &InvalidError{Reason: "unknown polling strategy"}
	}
}

func (p *policyConsumer) newPartition(id uint32, start wire.FilteredStart) *policyPartition {
	read := p.member.Read(id, wire.GroupPolicy()).From(start).
		Count(min(p.poll.Count, wire.MaxFilteredPageRecords)).
		MaxExamined(min(p.poll.Count, wire.MaxFilteredPageExamined)).
		MaxReplyBytes(min(p.poll.MaxBytes, wire.MaxFilteredPageBytes))
	return &policyPartition{read: read, byCheckpoint: make(map[iggcon.Checkpoint]*policyDelivery)}
}

func (p *policyConsumer) sync(ctx context.Context) error {
	assigned, err := p.member.Partitions(ctx)
	if err != nil {
		return err
	}
	p.syncedAt, p.syncDue = time.Now(), false
	// A rejoined member holds no page of its earlier membership.
	rejoins := p.member.rejoinCount()
	for id := range p.partitions {
		if rejoins != p.rejoins || !slices.Contains(assigned, id) {
			p.dropPartition(id)
		}
	}
	p.rejoins = rejoins
	for _, id := range assigned {
		if p.partitions[id] != nil {
			continue
		}
		start := wire.StartAtNext()
		if p.options.Start != nil && p.options.Start.Kind != iggcon.POLLING_NEXT {
			stored, err := p.consumer.StoredOffset(ctx, id)
			if err != nil {
				return err
			}
			if stored == nil {
				start, err = filterStart(id, p.options.Start, true)
				if err != nil {
					return err
				}
			}
		}
		partition := p.newPartition(id, start)
		if p.options.Start != nil && p.options.Start.Context != nil && start.Kind != wire.StartNext {
			expected := *p.options.Start.Context
			partition.expected = &expected
		}
		p.partitions[id] = partition
	}
	p.assigned = assigned
	return nil
}

// dropPartition forgets a partition this member no longer reads, with the page and the delivery buffered from it.
func (p *policyConsumer) dropPartition(id uint32) {
	delete(p.partitions, id)
	delete(p.ready, id)
	if p.batch != nil && p.batch.PartitionID == id {
		p.batch, p.batchDeliveries = nil, nil
	}
	if p.previous != nil && p.previous.checkpoint.PartitionID() == id {
		p.previous = nil
	}
}

func (p *policyConsumer) Partitions() []uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.assigned)
}

func (p *policyConsumer) Seek(ctx context.Context, id uint32, strategy iggcon.PollingStrategy) error {
	start, err := filterStart(id, &strategy, true)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.sync(ctx); err != nil {
		return err
	}
	if p.partitions[id] == nil {
		return &InvalidError{Reason: "the consumer does not hold the partition"}
	}

	p.member.resetPartition(id)
	p.dropPartition(id)
	p.partitions[id] = p.newPartition(id, start)
	if strategy.Context != nil {
		expected := *strategy.Context
		p.partitions[id].expected = &expected
	}
	return nil
}

func deliveryContext(page *MatchedPage) iggcon.PartitionContext {
	return iggcon.PartitionContext{Incarnation: page.Page.Generation.PartitionIncarnation, OwnerGeneration: page.Page.AckContext.OwnerGeneration, MetadataOp: page.Page.AckContext.MetadataOp}
}

func (p *policyConsumer) Next(ctx context.Context) (reader.ReceivedMessage, error) {
	return p.next(ctx, true)
}

func (p *policyConsumer) next(ctx context.Context, countAfterProcessing bool) (reader.ReceivedMessage, error) {
	var message reader.ReceivedMessage
	if err := ctx.Err(); err != nil {
		return message, err
	}
	p.mu.Lock()
	// Most calls take another delivery from the current page. Keep the
	// cancellation bridge for calls that may block on the server.
	buffered := p.batch != nil && (p.poll.Commit.Kind != iggcon.CommitAuto || p.poll.Commit.Auto.After == iggcon.CommitAfterMarked ||
		(p.poll.Commit.Auto.After == iggcon.CommitAfterProcessing && p.poll.Commit.Auto.Every.Kind == iggcon.CommitEveryBatch && (p.previous == nil || !p.previous.endsBatch)))
	if buffered {
		if err := p.lifetime.Err(); err != nil {
			p.mu.Unlock()
			return message, err
		}
		_, _, err := p.round(ctx, countAfterProcessing, &message)
		p.mu.Unlock()
		return message, err
	}
	p.mu.Unlock()
	readCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.lifetime, cancel)
	defer func() { stop(); cancel() }()
	for {
		if err := readCtx.Err(); err != nil {
			return message, err
		}
		p.mu.Lock()
		delivered, more, err := p.round(readCtx, countAfterProcessing, &message)
		p.mu.Unlock()
		if err != nil {
			return message, err
		}
		if delivered {
			return message, nil
		}
		if !more {
			p.idle(readCtx)
		}
	}
}

// idle holds the server's filtered wait after a round that found nothing new, so the next round reads only what became ready.
func (p *policyConsumer) idle(ctx context.Context) {
	ready, moved := p.member.waitForRecords(ctx, min(p.poll.MaxWait, p.member.idle))
	p.mu.Lock()
	defer p.mu.Unlock()
	p.syncDue = p.syncDue || moved
	for _, id := range ready {
		p.ready[id] = true
	}
}

func (p *policyConsumer) round(ctx context.Context, countAfterProcessing bool, message *reader.ReceivedMessage) (delivered, moreReady bool, err error) {
	if p.backgroundErr != nil {
		err := p.backgroundErr
		p.backgroundErr = nil
		return false, false, err
	}
	if previous := p.previous; previous != nil {
		p.previous = nil
		if err := p.count(ctx, previous.checkpoint); err != nil {
			return false, false, err
		}
	}
	if p.batch == nil {
		if p.syncDue || time.Since(p.syncedAt) >= p.member.refresh {
			if err := p.sync(ctx); err != nil {
				return false, false, err
			}
		}
		more, failed := p.read(ctx)
		if p.batch == nil {
			return false, more, failed
		}
		// A page is delivered first and the failure of another partition follows it.
		if failed != nil && p.backgroundErr == nil {
			p.backgroundErr = failed
		}
	}
	page := p.batch
	record := page.Records[p.index]
	batch := iggcon.PolledMessage{PartitionId: page.PartitionID, Context: deliveryContext(page), Messages: []iggcon.IggyMessage{record.Message}}
	checkpoint, _ := batch.CheckpointAt(0)
	ends := p.index+1 == len(page.Records)
	if p.index == 0 {
		p.batchDeliveries = make([]policyDelivery, len(page.Records))
	}
	delivery := &p.batchDeliveries[p.index]
	*delivery = policyDelivery{page: page, checkpoint: checkpoint, endsBatch: ends}
	partition := p.partitions[page.PartitionID]
	if partition == nil {
		p.batch, p.batchDeliveries = nil, nil
		return false, true, nil
	}
	partition.byCheckpoint[checkpoint] = delivery
	partition.delivered = append(partition.delivered, delivery)
	p.index++
	if ends {
		p.batch = nil
		p.batchDeliveries = nil
	}
	if p.poll.Commit.Kind == iggcon.CommitAuto {
		switch p.poll.Commit.Auto.After {
		case iggcon.CommitAfterProcessing:
			if countAfterProcessing {
				p.previous = delivery
			}
		case iggcon.CommitAfterDelivery:
			// The record is handed over, so a failed store is reported by the next read.
			if err := p.count(ctx, checkpoint); err != nil && p.backgroundErr == nil {
				p.backgroundErr = err
			}
		}
	}
	*message = reader.ReceivedMessage{Message: record.Message, PartitionID: page.PartitionID, Checkpoint: checkpoint, EndsBatch: ends}
	return true, true, nil
}

// read asks each readable partition for one page until one returns records.
// A partition that fails or blocks waits out a delay, so it neither starves
// the others nor spins the round.
func (p *policyConsumer) read(ctx context.Context) (more bool, failed error) {
	readable := p.assigned
	if ready := slices.DeleteFunc(slices.Clone(readable), func(id uint32) bool { return !p.ready[id] }); len(ready) > 0 {
		readable = ready
	}
	clear(p.ready)
	now := time.Now()
	for range readable {
		p.turn %= len(readable)
		id := readable[p.turn]
		p.turn++
		partition := p.partitions[id]
		if partition == nil || p.member.partitionDeferred(id, now) {
			continue
		}
		page, err := partition.read.Fetch(ctx)
		if err != nil {
			again, handled := p.afterRefusal(id, err)
			more = more || again
			if !handled {
				p.member.deferRead(partition.read, now.Add(p.member.retry))
				if failed == nil {
					failed = err
				}
			}
			continue
		}
		delete(p.lost, id)
		if expected := partition.expected; expected != nil {
			actual := deliveryContext(page)
			if (expected.Incarnation != 0 && expected.Incarnation != actual.Incarnation) || (expected.OwnerGeneration != 0 && expected.OwnerGeneration != actual.OwnerGeneration) {
				return false, &InvalidError{Reason: "the checkpoint belongs to a previous partition or group assignment"}
			}
			partition.expected = nil
		}
		partition.blocked = nil
		if len(page.Records) > 0 {
			p.batch, p.index = page, 0
			return true, failed
		}
		if page.Page.Fault != nil {
			partition.blocked = page
			p.member.deferRead(partition.read, now.Add(p.member.retry))
			if failed == nil {
				failed = &FilterFaultError{PartitionID: id, Offset: page.Page.Fault.Offset, Reason: page.Page.Fault.Reason, Oversized: page.Page.Stop == wire.StopOversizedRecord}
			}
			continue
		}
		// A failed store of an empty page is sent again with the next acknowledgment.
		if err := p.member.Ack(ctx, page); err != nil {
			p.resetOnRefusal(id, err)
		}
		more = more || (!page.Exhausted() && page.Examined() > 0)
	}
	return more, failed
}

// afterRefusal restarts a partition whose read the server fenced. A changed
// policy reads on at once under the new one, and a lost assignment reads on
// after the next sync, so only a second loss in a row reaches the caller.
func (p *policyConsumer) afterRefusal(id uint32, err error) (again, handled bool) {
	var refusal *FilterError
	if !errors.As(err, &refusal) {
		return false, false
	}
	p.resetOnRefusal(id, err)
	switch refusal.Reason() {
	case wire.ReasonConflict:
		return true, true
	case wire.ReasonMembershipStale:
		if p.lost[id] {
			return false, false
		}
		p.lost[id] = true
		return true, true
	default:
		return false, false
	}
}

func (p *policyConsumer) Consume(ctx context.Context, handle func(context.Context, reader.ReceivedMessage) error) error {
	for {
		next, err := p.next(ctx, false)
		if err != nil {
			return err
		}
		if err := handle(ctx, next); err != nil {
			return err
		}
		if p.poll.Commit.Kind == iggcon.CommitAuto && p.poll.Commit.Auto.After == iggcon.CommitAfterProcessing {
			p.mu.Lock()
			err = p.count(ctx, next.Checkpoint)
			p.mu.Unlock()
			if err != nil {
				return err
			}
		}
	}
}

func (p *policyConsumer) Commit(ctx context.Context, checkpoint iggcon.Checkpoint) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	partition := p.partitions[checkpoint.PartitionID()]
	if partition != nil && partition.stored != nil && *partition.stored == checkpoint {
		return nil
	}
	if partition == nil || partition.byCheckpoint[checkpoint] == nil {
		return &InvalidError{Reason: "the checkpoint is not an outstanding delivery of this consumer"}
	}
	err := p.storeThrough(ctx, partition, partition.byCheckpoint[checkpoint])
	// The new owner of a lost partition reads the record again.
	var refusal *FilterError
	if errors.As(err, &refusal) && refusal.Reason() == wire.ReasonMembershipStale {
		return nil
	}
	return err
}

func (p *policyConsumer) resetOnRefusal(id uint32, err error) {
	var refusal *FilterError
	if !errors.As(err, &refusal) {
		return
	}
	switch refusal.Reason() {
	case wire.ReasonConflict, wire.ReasonMembershipStale, wire.ReasonSourceChanged:
		p.member.resetPartition(id)
		p.dropPartition(id)
		p.syncDue = true
	}
}

// storeHandled stores a handled prefix for an automatic commit. A changed
// policy or a lost partition fences the prefix, and the records are read
// again from the stored offset, so neither is an error of the consumer.
func (p *policyConsumer) storeHandled(ctx context.Context, partition *policyPartition, delivery *policyDelivery) error {
	err := p.storeThrough(ctx, partition, delivery)
	var refusal *FilterError
	if errors.As(err, &refusal) && (refusal.Reason() == wire.ReasonConflict || refusal.Reason() == wire.ReasonMembershipStale) {
		return nil
	}
	if errors.Is(err, errPageOfPreviousAssignment) {
		p.dropPartition(delivery.checkpoint.PartitionID())
		p.syncDue = true
		return nil
	}
	return err
}

func (p *policyConsumer) storeThrough(ctx context.Context, partition *policyPartition, delivery *policyDelivery) error {
	if err := p.member.AckThrough(ctx, delivery.page, delivery.checkpoint.Offset()); err != nil {
		p.resetOnRefusal(delivery.checkpoint.PartitionID(), err)
		return err
	}
	count := 0
	for _, d := range partition.delivered {
		count++
		delete(partition.byCheckpoint, d.checkpoint)
		if p.previous == d {
			p.previous = nil
		}
		if d == delivery {
			break
		}
	}
	clear(partition.delivered[:count])
	partition.delivered = partition.delivered[count:]
	partition.pending = max(0, partition.pending-count)
	partition.counted = 0
	stored := delivery.checkpoint
	partition.stored = &stored
	p.consumer.offsets.store(stored.PartitionID(), stored.Offset())
	return nil
}

func (p *policyConsumer) count(ctx context.Context, checkpoint iggcon.Checkpoint) error {
	partition := p.partitions[checkpoint.PartitionID()]
	if partition == nil {
		return nil
	}
	d := partition.byCheckpoint[checkpoint]
	if d == nil {
		return nil
	}
	// One caller reads at a time, so counting a record counts every record handed over before it.
	for _, earlier := range partition.delivered {
		earlier.marked = true
		if earlier == d {
			break
		}
	}
	partition.mark(d)
	return p.storeIfDue(ctx, partition)
}

func (partition *policyPartition) mark(d *policyDelivery) {
	d.marked = true
	for partition.pending < len(partition.delivered) && partition.delivered[partition.pending].marked {
		partition.pending++
		partition.counted++
	}
}

func (p *policyConsumer) storeIfDue(ctx context.Context, partition *policyPartition) error {
	if partition.pending == 0 {
		return nil
	}
	latest := partition.delivered[partition.pending-1]
	every := p.poll.Commit.Auto.Every
	due := every.Kind == iggcon.CommitEveryMessage || (every.Kind == iggcon.CommitEveryMessages && partition.counted >= int(every.Messages)) || (every.Kind == iggcon.CommitEveryBatch && latest.endsBatch)
	if due {
		return p.storeHandled(ctx, partition, latest)
	}
	return nil
}

func (p *policyConsumer) Mark(checkpoint iggcon.Checkpoint) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.poll.Commit.Kind != iggcon.CommitAuto || p.poll.Commit.Auto.After != iggcon.CommitAfterMarked {
		return &InvalidError{Reason: "mark requires commit after marked"}
	}
	partition := p.partitions[checkpoint.PartitionID()]
	if partition == nil || partition.byCheckpoint[checkpoint] == nil {
		return &InvalidError{Reason: "checkpoint was not delivered by this consumer"}
	}
	partition.mark(partition.byCheckpoint[checkpoint])
	select {
	case p.marked <- struct{}{}:
	default:
	}
	return nil
}

func (p *policyConsumer) flush(ctx context.Context) error {
	var err error
	for _, partition := range p.partitions {
		if partition.pending != 0 {
			err = errors.Join(err, p.storeHandled(ctx, partition, partition.delivered[partition.pending-1]))
		}
	}
	return err
}

func (p *policyConsumer) flushPeriodically() {
	defer close(p.done)
	var ticks <-chan time.Time
	if interval := p.poll.Commit.Auto.Interval; interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		var periodic bool
		select {
		case <-p.lifetime.Done():
			return
		case <-ticks:
			periodic = true
		case <-p.marked:
		}
		p.mu.Lock()
		var err error
		if periodic {
			err = p.flush(p.lifetime)
		} else {
			for _, partition := range p.partitions {
				err = errors.Join(err, p.storeIfDue(p.lifetime, partition))
			}
		}
		if err != nil && p.backgroundErr == nil {
			p.backgroundErr = err
		}
		p.mu.Unlock()
	}
}

func (p *policyConsumer) Shutdown(ctx context.Context) error {
	p.stop()
	select {
	case <-p.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	// A read or a commit still in flight holds the lock until its own context ends.
	for !p.mu.TryLock() {
		if err := sleepWithin(ctx, time.Millisecond); err != nil {
			return err
		}
	}
	defer p.mu.Unlock()
	var err error
	if previous := p.previous; previous != nil {
		p.previous = nil
		err = p.count(ctx, previous.checkpoint)
	}
	return errors.Join(err, p.flush(ctx), p.member.Close(ctx))
}

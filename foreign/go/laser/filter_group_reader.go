package laser

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	iggy "github.com/apache/iggy/foreign/go/client"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

const (
	// defaultFilteredMaxWait is the longest one filtered wait is held, at most what the server announces.
	defaultFilteredMaxWait = 5 * time.Second
	// filteredWaitReplyMargin is how much longer than its wait a held filtered wait has to answer.
	filteredWaitReplyMargin = 5 * time.Second
	// filteredWaitCancelTimeout bounds the cancel of the waits still held.
	filteredWaitCancelTimeout = 2 * time.Second
	// assignmentRefresh is how often a reader reads its assignment again.
	assignmentRefresh = 2 * time.Second
	// filteredIdleInterval paces idle rounds on a server that holds no filtered wait.
	filteredIdleInterval = 250 * time.Millisecond
	// defaultMaxUnackedPages bounds the pages one partition holds unacknowledged unless the reader names another bound.
	defaultMaxUnackedPages = 1024
)

// routeRecheck is how long a read waits on a partition's known primary before it asks the cluster whether another node leads it now.
var routeRecheck = time.Second

// nextFilteredWaitID keeps wait ids unique across every reader of the process, because a wait is named on its connection.
var nextFilteredWaitID atomic.Uint64

// errPageOfPreviousAssignment refuses a page read before the partition was reset or reassigned.
var errPageOfPreviousAssignment = &InvalidError{Reason: "page belongs to a previous assignment"}

// GroupReader owns a group assignment and reads filtered pages under that membership.
// Close it when it finishes reading so the coordinator can reassign its partitions.
type GroupReader struct {
	*Filters
	// client is held behind a pointer, so no fmt verb expands the upstream client and its credentials.
	client                     *heldClient
	group                      *ConsumerGroup
	streamID, topicID, groupID iggcon.Identifier
	ownsClient                 bool
	pinned                     *sourceIncarnation
	maxWait, refresh, idle     time.Duration
	// retry is how long a partition whose read failed or blocked waits before it is read again.
	retry      time.Duration
	maxUnacked int
	options    GroupReaderOptions
	// storeDeadline bounds how long an acknowledgment that finds no primary is sent again, ManagedDeadline when zero.
	storeDeadline time.Duration

	mu         sync.Mutex
	closed     bool
	leaders    map[uint32]uint8
	assigned   []uint32
	rejoins    uint64
	syncedAt   time.Time
	syncDue    bool
	progressMu sync.Mutex
	progress   map[uint32]*partitionProgress
	sequence   uint64
	// unread are the partitions held at the open that still take the configured start.
	unread map[uint32]struct{}

	roundMu  sync.Mutex
	cursor   int
	ready    map[uint32]struct{}
	lost     map[uint32]struct{}
	examined uint64
	deferred error
	buffered []MatchedRecord
	guarded  bool
	guardMu  sync.Mutex
	guard    *localGuard

	waitMu           sync.Mutex
	announcedWait    time.Duration
	waitRefused      bool
	readyUnconfirmed atomic.Bool
	endWait          atomic.Pointer[context.CancelFunc]
}

// heldClient is the upstream client a reader sends through.
type heldClient struct{ iggcon.Client }

// OpenReader joins this existing group for server-filtered reads. A connection
// opened by Laser gives each reader its own member identity. With FromClient,
// the supplied connection holds the membership.
func (group *ConsumerGroup) OpenReader(ctx context.Context) (*GroupReader, error) {
	return group.OpenReaderWith(ctx, GroupReaderOptions{})
}

// OpenReaderWith is OpenReader under the options given.
func (group *ConsumerGroup) OpenReaderWith(ctx context.Context, options GroupReaderOptions) (*GroupReader, error) {
	options, err := options.resolved()
	if err != nil {
		return nil, err
	}
	laser := group.laser()
	caps := laser.Capabilities()
	if !caps.Filters.GroupPolicyReads && caps.Hello == HelloUnknown {
		caps = laser.RefreshCapabilities(ctx)
	}
	if !caps.Filters.GroupPolicyReads {
		if !caps.Filters.Native && (caps.Hello == HelloFailed || caps.Hello == HelloUnknown) {
			return nil, &TransportError{Reason: "could not establish group-aware filter capabilities", Cause: context.DeadlineExceeded}
		}
		return nil, &UnsupportedError{Surface: "group-aware consumer filters"}
	}
	stream, topic, err := group.topic.identifiers()
	if err != nil {
		return nil, err
	}
	id, err := group.identifier()
	if err != nil {
		return nil, err
	}
	client, err := laser.connected()
	if err != nil {
		return nil, err
	}
	reader := &GroupReader{Filters: group.Filters(), group: group, streamID: stream, topicID: topic, groupID: id,
		client: &heldClient{client}, leaders: make(map[uint32]uint8), progress: make(map[uint32]*partitionProgress),
		maxWait: options.MaxWait, refresh: assignmentRefresh, idle: options.IdleInterval, retry: options.IdleInterval,
		maxUnacked: options.MaxUnackedPages, options: options, guarded: options.LocalGuard, announcedWait: caps.Filters.MaxWait,
		unread: make(map[uint32]struct{}), ready: make(map[uint32]struct{}), lost: make(map[uint32]struct{})}
	if !group.target.byName {
		pinned, err := group.topic.sourceIncarnation(ctx)
		if err != nil {
			return nil, err
		}
		reader.pinned = &pinned
	}
	if config := laser.Connection(); config != nil {
		options, err := config.TCPOptions()
		if err != nil {
			return nil, err
		}
		coordinator, err := iggy.NewIggyClient(iggy.WithTcp(options...))
		if err != nil {
			return nil, err
		}
		connectCtx, cancel := context.WithTimeout(ctx, DefaultConnectTimeout)
		err = coordinator.Connect(connectCtx)
		cancel()
		if err != nil {
			_ = coordinator.Close()
			return nil, &TransportError{Reason: "connect the group reader", Cause: err}
		}
		reader.client = &heldClient{coordinator}
		reader.ownsClient = true
	}
	if err := reader.client.JoinConsumerGroup(ctx, stream, topic, id); err != nil {
		if reader.ownsClient {
			_ = reader.client.Close()
		}
		return nil, &TransportError{Reason: "join the filtered group", Cause: err}
	}
	reader.Filters.member = reader
	if err := reader.waitForAssignment(ctx); err != nil {
		_ = reader.Close(ctx)
		return nil, err
	}
	reader.mu.Lock()
	for _, partition := range reader.assigned {
		reader.unread[partition] = struct{}{}
	}
	reader.mu.Unlock()
	return reader, nil
}

// Partitions synchronizes the member's assignment with the coordinator.
func (reader *GroupReader) Partitions(ctx context.Context) ([]uint32, error) {
	reader.progressMu.Lock()
	defer reader.progressMu.Unlock()
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return nil, &InvalidError{Reason: "the group reader is closed"}
	}
	assignment, err := reader.client.SyncConsumerGroup(ctx, reader.streamID, reader.topicID, reader.groupID)
	if errors.Is(err, ierror.ErrConsumerGroupMemberNotFound) {
		if reader.pinned != nil {
			current, err := reader.group.topic.sourceIncarnation(ctx)
			if err != nil {
				return nil, err
			}
			if *reader.pinned != current {
				return nil, reader.group.notFound()
			}
		}
		if err := reader.client.JoinConsumerGroup(ctx, reader.streamID, reader.topicID, reader.groupID); err != nil {
			return nil, &TransportError{Reason: "rejoin the filtered group", Cause: err}
		}
		clear(reader.progress)
		reader.rejoins++
		assignment, err = reader.client.SyncConsumerGroup(ctx, reader.streamID, reader.topicID, reader.groupID)
	}
	if err != nil {
		return nil, &TransportError{Reason: "sync the filtered group", Cause: err}
	}
	if assignment == nil {
		return nil, &ProtocolError{Reason: "group sync returned no assignment"}
	}
	for id := range reader.progress {
		if !slices.Contains(assignment.Partitions, id) {
			delete(reader.progress, id)
		}
	}
	reader.assigned = slices.Clone(assignment.Partitions)
	reader.syncedAt, reader.syncDue = time.Now(), false
	return slices.Clone(assignment.Partitions), nil
}

// Close leaves the group and releases the dedicated connection.
func (reader *GroupReader) Close(ctx context.Context) error {
	reader.mu.Lock()
	if reader.closed {
		reader.mu.Unlock()
		return nil
	}
	reader.closed = true
	reader.mu.Unlock()
	// A held wait is cancelled on the server before the membership goes.
	if end := reader.endWait.Load(); end != nil {
		(*end)()
	}
	reader.waitMu.Lock()
	// The lock frees once the held wait ended.
	reader.waitMu.Unlock()
	reader.mu.Lock()
	defer reader.mu.Unlock()
	err := reader.client.LeaveConsumerGroup(ctx, reader.streamID, reader.topicID, reader.groupID)
	if isMissingResource(err) || isMissingGroup(err) || errors.Is(err, ierror.ErrConsumerGroupMemberNotFound) {
		err = nil
	}
	if reader.ownsClient {
		err = errors.Join(err, reader.client.Close())
	}
	return err
}

// AckThrough marks the page prefix through offset and every earlier page handled.
func (reader *GroupReader) AckThrough(ctx context.Context, page *MatchedPage, offset uint64) error {
	return reader.ack(ctx, page, &offset)
}

// AckRecord marks one returned record handled and stores the progress this completes.
func (reader *GroupReader) AckRecord(ctx context.Context, page *MatchedPage, offset uint64) error {
	reader.progressMu.Lock()
	defer reader.progressMu.Unlock()
	progress, err := reader.pageProgress(page)
	if err != nil {
		return err
	}
	progress.completeRecord(page.sequence, offset)
	return reader.store(ctx, progress)
}

// Skip advances past a stop fault or oversized record. Earlier unread pages
// must be acknowledged before this position is stored.
func (reader *GroupReader) Skip(ctx context.Context, page *MatchedPage, offset uint64) error {
	reader.progressMu.Lock()
	defer reader.progressMu.Unlock()
	if page == nil || page.owner == nil || page.owner.filters.member != reader {
		return &InvalidError{Reason: "page belongs to another group reader"}
	}
	progress := reader.progress[page.PartitionID]
	if progress == nil || page.Page.Fault == nil || page.Page.Fault.Offset != offset {
		return &InvalidError{Reason: "page did not stop at the requested offset"}
	}
	reader.sequence++
	if !progress.skip(reader.sequence, offset) {
		return &InvalidError{Reason: "partition is not stopped at the requested offset"}
	}
	page.owner.start = progress.readStart()
	return reader.store(ctx, progress)
}

// WaitForRecords waits after a round that found nothing new and returns the
// partitions that hold a record past what was examined. The server holds one
// filtered wait per node that serves this reader's partitions. On a server
// without filtered waits it sleeps one idle interval and returns none.
func (reader *GroupReader) WaitForRecords(ctx context.Context) ([]uint32, error) {
	ready := reader.awaitRecords(ctx)
	return ready, ctx.Err()
}

func (reader *GroupReader) waitForAssignment(ctx context.Context) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		assigned, err := reader.Partitions(ctx)
		if err != nil || len(assigned) > 0 {
			return err
		}
		group, err := reader.client.GetConsumerGroup(ctx, reader.streamID, reader.topicID, reader.groupID)
		if err != nil {
			return err
		}
		if group == nil {
			return reader.group.notFound()
		}
		var installed uint32
		for _, member := range group.Members {
			installed += uint32(len(member.Partitions))
		}
		if installed >= group.PartitionsCount || !time.Now().Before(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (reader *GroupReader) resetPartition(partition uint32) {
	reader.progressMu.Lock()
	defer reader.progressMu.Unlock()
	delete(reader.progress, partition)
	delete(reader.unread, partition)
}

// deferPartition keeps a failing or blocked partition unread until the delay passes.
func (reader *GroupReader) deferPartition(partition uint32, until time.Time) {
	reader.deferRead(&FilteredReadRequest{partitionID: partition, start: wire.StartAtNext()}, until)
}

// deferRead defers the partition of a read, also one whose first read failed and left no progress yet.
func (reader *GroupReader) deferRead(request *FilteredReadRequest, until time.Time) {
	reader.progressMu.Lock()
	defer reader.progressMu.Unlock()
	progress := reader.progress[request.partitionID]
	if progress == nil {
		progress = newPartitionProgress(request.start)
		reader.progress[request.partitionID] = progress
	}
	progress.deferUntil(until)
}

func (reader *GroupReader) partitionDeferred(partition uint32, now time.Time) bool {
	reader.progressMu.Lock()
	defer reader.progressMu.Unlock()
	progress := reader.progress[partition]
	return progress != nil && progress.waiting(now)
}

func (reader *GroupReader) rejoinCount() uint64 {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.rejoins
}

func (reader *GroupReader) exchange(ctx context.Context, partition, code uint32, payload []byte) ([]byte, error) {
	rerouted, followed := false, false
	for {
		target, err := reader.route(ctx, partition)
		if err != nil {
			return nil, err
		}
		raw, moved, err := reader.sendUnlessMoved(ctx, partition, code, payload, target)
		if moved {
			if followed {
				return nil, &FilterError{Failure: wire.FilterError{
					Code: wire.ReasonUnavailable.Code(), Reason: wire.ReasonUnavailable,
					Message: fmt.Sprintf("partition %d moved again during the read, retry", partition),
				}}
			}
			followed = true
			continue
		}
		if err != nil {
			stale := isRouteStale(err)
			if stale {
				reader.forgetLeader(partition)
			}
			if !stale || rerouted {
				return nil, &TransportError{Reason: "send group filter request", Cause: err}
			}
			rerouted = true
			continue
		}
		// A backup refuses a partition it no longer leads as unavailable, not as a transport error.
		if !rerouted && refusedUnavailable(raw) {
			reader.forgetLeader(partition)
			rerouted = true
			continue
		}
		return raw, nil
	}
}

// sendUnlessMoved sends to the partition's known primary and looks the route up again every routeRecheck while it has not answered. It reports moved when another node leads the partition now, and the request is dropped.
func (reader *GroupReader) sendUnlessMoved(
	ctx context.Context, partition, code uint32, payload []byte, target waitTarget,
) (raw []byte, moved bool, err error) {
	if !target.known {
		raw, err = reader.client.SendBinaryRequest(ctx, code, payload)
		return raw, false, err
	}
	sendCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type answer struct {
		raw []byte
		err error
	}
	answered := make(chan answer, 1)
	go func() {
		raw, err := reader.client.SendBinaryRequestToReplica(sendCtx, code, payload, target.replica)
		answered <- answer{raw: raw, err: err}
	}()
	recheck := time.NewTicker(routeRecheck)
	defer recheck.Stop()
	for {
		select {
		case sent := <-answered:
			return sent.raw, false, sent.err
		case <-recheck.C:
			primary, lookupErr := partitionPrimary(ctx, reader.client, reader.streamID, reader.topicID, partition)
			if lookupErr != nil || primary == target.replica {
				continue
			}
			reader.mu.Lock()
			for held, leader := range reader.leaders {
				if leader == target.replica {
					delete(reader.leaders, held)
				}
			}
			reader.leaders[partition] = primary
			reader.mu.Unlock()
			return nil, true, nil
		}
	}
}

// refusedUnavailable reports a filter reply that refuses the request as unavailable.
func refusedUnavailable(raw []byte) bool {
	var reply wire.FilterReply
	if err := wire.Unmarshal(raw, &reply); err != nil {
		return false
	}
	return reply.Err != nil && reply.Err.Reason == wire.ReasonUnavailable
}

// route names the node that leads a partition, any node while its primary is unknown.
func (reader *GroupReader) route(ctx context.Context, partition uint32) (waitTarget, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return waitTarget{}, &InvalidError{Reason: "the group reader is closed"}
	}
	leader, known := reader.leaders[partition]
	if !known {
		if replica, err := partitionPrimary(ctx, reader.client, reader.streamID, reader.topicID, partition); err == nil {
			reader.leaders[partition] = replica
			leader, known = replica, true
		}
	}
	return waitTarget{replica: leader, known: known}, nil
}

func (reader *GroupReader) pageProgress(page *MatchedPage) (*partitionProgress, error) {
	if page == nil || page.owner == nil || page.owner.filters.member != reader {
		return nil, &InvalidError{Reason: "page belongs to another group reader"}
	}
	progress := reader.progress[page.PartitionID]
	if progress == nil || !progress.knows(page.sequence) {
		return nil, errPageOfPreviousAssignment
	}
	return progress, nil
}

func (reader *GroupReader) ack(ctx context.Context, page *MatchedPage, offset *uint64) error {
	reader.progressMu.Lock()
	defer reader.progressMu.Unlock()
	progress, err := reader.pageProgress(page)
	if err != nil {
		return err
	}
	if offset == nil {
		progress.completePage(page.sequence)
	} else if !progress.completeThrough(page.sequence, *offset) {
		return &InvalidError{Reason: "offset exceeds the page's safe acknowledgment"}
	}
	return reader.store(ctx, progress)
}

func (reader *GroupReader) store(ctx context.Context, progress *partitionProgress) error {
	target := progress.unstoredTarget()
	if target == nil {
		return nil
	}
	ack := wire.FilteredAck{
		V: wire.FilterOpVersion, Source: reader.source, Consumer: reader.consumer,
		PartitionID: target.Generation.PartitionID, GroupID: target.GroupID,
		Generation: target.Generation, AckContext: target.AckContext, Offset: target.Offset,
		Digest: target.Policy.Digest, Mode: target.Policy.Mode, PolicyGeneration: target.Policy.PolicyGeneration,
	}
	outcome, err := reader.executeUntilPrimary(ctx, ack)
	if err != nil {
		return err
	}
	if outcome.Kind != wire.FilterAcknowledged || outcome.Receipt == nil || !progress.acknowledged(target, *outcome.Receipt) {
		return &ProtocolError{Reason: "filtered ack receipt does not cover the requested offset and generation"}
	}
	return nil
}

// executeUntilPrimary sends one acknowledgment, and sends the same target again
// on the managed backoff while a failover leaves its partition without a primary.
func (reader *GroupReader) executeUntilPrimary(ctx context.Context, ack wire.FilteredAck) (*wire.FilterOutcome, error) {
	bound := reader.storeDeadline
	if bound <= 0 {
		bound = ManagedDeadline
	}
	deadline := time.Now().Add(bound)
	for attempt := uint32(1); ; attempt++ {
		outcome, err := reader.execute(ctx, wire.AgdxFilteredAckCode, ack)
		if err == nil || !routeMoved(err) || !time.Now().Before(deadline) {
			return outcome, err
		}
		if sleepWithin(ctx, ManagedRetry.Delay(attempt)) != nil {
			return nil, err
		}
	}
}

// routeMoved reports a request that found no primary for its partition.
func routeMoved(err error) bool {
	var refusal *FilterError
	if errors.As(err, &refusal) {
		return refusal.Reason() == wire.ReasonUnavailable
	}
	return isRouteStale(err)
}

// waitForRecords also reports an assignment that moved while the waits held.
func (reader *GroupReader) waitForRecords(ctx context.Context, pacing time.Duration) (ready []uint32, moved bool) {
	reader.waitMu.Lock()
	defer reader.waitMu.Unlock()
	held, cancel := context.WithCancel(ctx)
	defer cancel()
	reader.endWait.Store(&cancel)
	defer reader.endWait.Store(nil)
	reader.mu.Lock()
	closed := reader.closed
	reader.mu.Unlock()
	if closed {
		return nil, false
	}
	wait := min(reader.maxWait, reader.announcedWait)
	// A ready answer the next read could not confirm would repeat at once.
	unconfirmed := reader.readyUnconfirmed.Swap(false)
	if wait < time.Millisecond || unconfirmed {
		_ = sleepWithin(held, pacing)
		return nil, false
	}
	ready, answer := reader.holdWaits(held, wait)
	switch answer {
	case waitReady:
		reader.readyUnconfirmed.Store(true)
	case waitMoved:
		return nil, true
	case waitPaced:
		_ = sleepWithin(held, pacing)
	case waitUnsupported:
		reader.announcedWait = 0
		_ = sleepWithin(held, pacing)
	}
	return ready, false
}

// holdWaits sends one filtered wait per node and slice of partitions and takes the first answer.
func (reader *GroupReader) holdWaits(ctx context.Context, wait time.Duration) ([]uint32, waitAnswer) {
	assigned, byNode := reader.waitPartitions(ctx)
	if len(byNode) == 0 {
		return nil, waitPaced
	}
	sendCtx, cancel := context.WithTimeout(ctx, wait+filteredWaitReplyMargin)
	defer cancel()
	held := make(map[uint64]waitTarget)
	var requests []wire.FilteredWaitRequest
	for target, partitions := range byNode {
		for slice := range slices.Chunk(partitions, wire.MaxFilteredWaitPartitions) {
			request := wire.FilteredWaitRequest{
				V: wire.FilterOpVersion, Source: reader.source, Consumer: reader.consumer,
				Partitions: slice, MaxWaitMs: uint32(wait / time.Millisecond), WaitID: nextFilteredWaitID.Add(1),
			}
			if request.Validate() != nil {
				return nil, waitPaced
			}
			held[request.WaitID] = target
			requests = append(requests, request)
		}
	}
	answers := make(chan waitReply, len(requests))
	for _, request := range requests {
		payload, err := wire.Marshal(request)
		if err != nil {
			cancel()
			return nil, waitPaced
		}
		go func() {
			reply := waitReply{id: request.WaitID, target: held[request.WaitID], sent: time.Now()}
			reply.ready, reply.err = reader.sendWait(sendCtx, reply.target, payload)
			answers <- reply
		}()
	}
	refresh := time.NewTimer(reader.refresh)
	defer refresh.Stop()
	var first waitReply
	for answered := false; !answered; {
		select {
		case first = <-answers:
			delete(held, first.id)
			answered = true
		case <-refresh.C:
			// Partitions gained or lost while every wait holds are read at once.
			current, err := reader.Partitions(sendCtx)
			if err != nil || !sameAssignment(current, assigned) {
				reader.cancelWaits(ctx, held)
				return nil, waitMoved
			}
			refresh.Reset(reader.refresh)
		case <-sendCtx.Done():
			reader.cancelWaits(ctx, held)
			return nil, waitElapsed
		}
	}
	defer func() { reader.cancelWaits(ctx, held) }()
	if first.err != nil {
		refused := waitRefused(first.err, first.target)
		if refused && first.target.known {
			reader.forgetReplica(first.target.replica)
		}
		// A node that moved is asked again at once, and only a second refusal is paced.
		if refused && !reader.waitRefused {
			reader.waitRefused = true
			return nil, waitElapsed
		}
		if isUnsupportedWait(first.err) {
			return nil, waitUnsupported
		}
		return nil, waitPaced
	}
	reader.waitRefused = false
	if len(first.ready) == 0 {
		if time.Since(first.sent) >= wait/2 {
			return nil, waitElapsed
		}
		return nil, waitPaced
	}
	ready := first.ready
	// Answers already in keep their ready partitions instead of costing another round.
	for drained := false; !drained; {
		select {
		case more := <-answers:
			delete(held, more.id)
			if more.err == nil {
				ready = append(ready, more.ready...)
			}
		default:
			drained = true
		}
	}
	return slices.DeleteFunc(ready, func(id uint32) bool { return !slices.Contains(assigned, id) }), waitReady
}

// waitPartitions groups the readable partitions by the node that serves them, each with the offset its wait holds from.
func (reader *GroupReader) waitPartitions(ctx context.Context) ([]uint32, map[waitTarget][]wire.FilteredWaitPartition) {
	now := time.Now()
	reader.mu.Lock()
	assigned := slices.Clone(reader.assigned)
	reader.mu.Unlock()
	reader.progressMu.Lock()
	var partitions []wire.FilteredWaitPartition
	for _, id := range assigned {
		if progress := reader.progress[id]; progress != nil && !progress.waiting(now) {
			partitions = append(partitions, wire.FilteredWaitPartition{PartitionID: id, Offset: progress.waitOffset()})
		}
	}
	reader.progressMu.Unlock()
	byNode := make(map[waitTarget][]wire.FilteredWaitPartition)
	for _, partition := range partitions {
		target, err := reader.route(ctx, partition.PartitionID)
		if err != nil {
			return assigned, nil
		}
		byNode[target] = append(byNode[target], partition)
	}
	return assigned, byNode
}

func (reader *GroupReader) sendWait(ctx context.Context, target waitTarget, payload []byte) ([]uint32, error) {
	outcome, err := reader.sendTo(ctx, target, wire.AgdxFilteredWaitCode, payload)
	if err != nil {
		return nil, err
	}
	if outcome.Kind != wire.FilterReady || outcome.WaitResult == nil {
		return nil, unexpectedFilterOutcome("wait", outcome.Kind)
	}
	return outcome.WaitResult.Ready, nil
}

// sendTo sends one filter command to a node without holding the reader's request lock, so a held wait never delays a read or an acknowledgment.
func (reader *GroupReader) sendTo(ctx context.Context, target waitTarget, code uint32, payload []byte) (*wire.FilterOutcome, error) {
	var raw []byte
	var err error
	if target.known {
		raw, err = reader.client.SendBinaryRequestToReplica(ctx, code, payload, target.replica)
	} else {
		raw, err = reader.client.SendBinaryRequest(ctx, code, payload)
	}
	if err != nil {
		return nil, err
	}
	var reply wire.FilterReply
	if err := decodeManagedReply(raw, &reply, "decode filter reply"); err != nil {
		return nil, err
	}
	if !reply.IsOk() {
		if reply.Err == nil {
			return nil, &CodecError{Reason: "filter reply carried neither an outcome nor an error"}
		}
		return nil, &FilterError{Failure: *reply.Err}
	}
	return reply.Outcome, nil
}

// cancelWaits ends every wait still held, so its slot frees at once instead of at its deadline.
func (reader *GroupReader) cancelWaits(ctx context.Context, held map[uint64]waitTarget) {
	if len(held) == 0 {
		return
	}
	byNode := make(map[waitTarget][]uint64)
	for id, target := range held {
		byNode[target] = append(byNode[target], id)
	}
	clear(held)
	cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), filteredWaitCancelTimeout)
	defer cancel()
	var sent sync.WaitGroup
	for target, ids := range byNode {
		for slice := range slices.Chunk(ids, wire.MaxFilteredWaitCancelIDs) {
			payload, err := wire.Marshal(wire.FilteredWaitCancel{V: wire.FilterOpVersion, WaitIDs: slice})
			if err != nil {
				continue
			}
			// A failed cancel is dropped, the server still ends the wait at its deadline.
			sent.Go(func() { _, _ = reader.sendTo(cancelCtx, target, wire.AgdxFilteredWaitCancelCode, payload) })
		}
	}
	sent.Wait()
}

func (reader *GroupReader) forgetReplica(replica uint8) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	for partition, leader := range reader.leaders {
		if leader == replica {
			delete(reader.leaders, partition)
		}
	}
}

// waitAnswer is what one answered filtered wait means for the next round.
type waitAnswer uint8

const (
	// waitReady found a record on at least one partition.
	waitReady waitAnswer = iota
	// waitElapsed held to its end without a record.
	waitElapsed
	// waitMoved ended because the assignment changed.
	waitMoved
	// waitPaced failed or answered empty long before its time.
	waitPaced
	// waitUnsupported means the server holds no filtered wait.
	waitUnsupported
)

// waitTarget is the node one filtered wait is sent to, any node when the primary is unknown.
type waitTarget struct {
	replica uint8
	known   bool
}

type waitReply struct {
	id     uint64
	target waitTarget
	sent   time.Time
	ready  []uint32
	err    error
}

// waitRefused reports a wait refused by a node that no longer serves its partitions.
func waitRefused(err error, target waitTarget) bool {
	var refusal *FilterError
	if errors.As(err, &refusal) {
		return refusal.Reason() == wire.ReasonUnavailable
	}
	return target.known && isRouteStale(err)
}

func isUnsupportedWait(err error) bool {
	var refusal *FilterError
	if errors.As(err, &refusal) {
		return refusal.ResultCode() == wire.ResultUnsupported
	}
	return errors.Is(err, ierror.InvalidCommand{})
}

func sameAssignment(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for _, id := range left {
		if !slices.Contains(right, id) {
			return false
		}
	}
	return true
}

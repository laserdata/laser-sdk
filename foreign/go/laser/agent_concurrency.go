package laser

import (
	"context"
	"errors"
	"sync"
)

// ConcurrencyPolicy is how AgentRuntime.Run schedules message handling across
// the partitions this agent is assigned.
//
// The zero value is Serial, so RuntimeOptions{} keeps today's behavior
// without a caller having to name it.
type ConcurrencyPolicy struct {
	perPartition  bool
	maxPartitions int
}

// SerialConcurrency processes one message at a time across every partition,
// in the order the source hands them over. The safe, fully ordered default: a
// slow or retrying message holds up every partition until it finishes. This
// is also ConcurrencyPolicy's zero value.
func SerialConcurrency() ConcurrencyPolicy { return ConcurrencyPolicy{} }

// SerialPerPartitionConcurrency runs one lane per partition: messages from
// different partitions are handled concurrently, but within one partition a
// lane still handles its records strictly in order, one at a time, so
// per-partition ordering and the committed prefix hold. Retry backoff is
// lane-local, so a poison message on one partition never stalls another.
//
// maxPartitions bounds how many lanes run at once. A message for a partition
// with no lane yet, arriving at the cap, retires an idle lane (one with
// nothing in flight) to make room, or waits for one to go idle if every lane
// is busy. A value below 1 is treated as 1.
func SerialPerPartitionConcurrency(maxPartitions int) ConcurrencyPolicy {
	if maxPartitions < 1 {
		maxPartitions = 1
	}
	return ConcurrencyPolicy{perPartition: true, maxPartitions: maxPartitions}
}

func (policy ConcurrencyPolicy) isSerialPerPartition() bool { return policy.perPartition }

// errCleanStop unwinds runPerPartition's helpers to a clean nil return, the
// same outcome a plain Run gives when a context ends while it is backing off
// a commit failure: that is a stop, not a failure to report.
var errCleanStop = errors.New("agent runtime: clean stop")

// partitionLane is one partition's serial queue of records.
//
// busy is read by the routing loop and written by the commit loop, the two
// goroutines that both need to know it, so it carries its own lock; nothing
// else on a lane is touched by more than one goroutine; the routing loop owns
// the lanes map and queue/exited lifecycle, the lane's own goroutine owns
// reading queue and calling deliver, and the commit loop owns calling Commit
// and reporting fatal failures.
type partitionLane struct {
	partition uint32
	queue     chan *Record
	exited    chan struct{}

	mutex sync.Mutex
	busy  bool
}

func (lane *partitionLane) setBusy(busy bool) {
	lane.mutex.Lock()
	lane.busy = busy
	lane.mutex.Unlock()
}

func (lane *partitionLane) isBusy() bool {
	lane.mutex.Lock()
	defer lane.mutex.Unlock()
	return lane.busy
}

// laneCompletion is what a lane reports once it has handled one record: the
// lane it came from (to clear its busy flag and commit its record), and the
// handling error, if any. The lane is carried by pointer rather than by
// partition id so the commit loop never has to read the lanes map, which only
// the routing loop may safely mutate.
type laneCompletion struct {
	lane   *partitionLane
	record *Record
	err    error
}

// signal is a single-slot wake-up: a send that is dropped rather than
// blocked when the slot already holds one, since it is a hint ("something
// changed, look again") rather than a count.
func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// runPerPartition is ConcurrencyPolicy.SerialPerPartition's loop.
//
// Two goroutines besides this one's callers run for its duration: each open
// lane has its own (reading its queue, calling deliver, reporting the
// result), and one commit loop is the sole owner of source.Commit, settling
// completions as they arrive regardless of whether this routing loop is
// blocked in source.Next at the time. Routing and committing being separate
// is what keeps a quiet topic from holding a finished lane's commit hostage
// to the next poll wait.
func (runtime *AgentRuntime) runPerPartition(
	ctx context.Context, source RecordSource, handler AgentHandler, maxPartitions int,
) error {
	if source == nil {
		return &InvalidError{Reason: "an agent runtime reads from a source, so it needs one"}
	}
	if handler == nil {
		return &InvalidError{Reason: "an agent runtime hands work to a handler, so it needs one"}
	}
	work, release := runtime.workContext(ctx)
	defer release()
	runtime.resolveSource(ctx)

	lanes := map[uint32]*partitionLane{}
	// Buffered to the lane cap: a lane holds at most one outstanding
	// completion (it does not pull its next queued record until the last
	// one's completion is sent), so this can never fill up and block a lane
	// on send, whether or not the commit loop is still reading it.
	completions := make(chan laneCompletion, maxPartitions)
	fatal := make(chan error, 1)
	freed := make(chan struct{}, 1)
	stopCommit := make(chan struct{})
	commitDone := make(chan struct{})

	go runtime.commitLoop(ctx, work, source, completions, stopCommit, commitDone, fatal, freed)

	// Every lane still open when this returns is closed and drained, so a
	// lane holding a record mid-handle finishes it (workContext's own grace
	// bounds that) rather than being leaked. Lanes are stopped, and therefore
	// finished sending whatever completion they owe, before the commit loop
	// is told to stop, so it drains the last of them rather than losing them.
	defer func() {
		for _, lane := range lanes {
			close(lane.queue)
		}
		for _, lane := range lanes {
			<-lane.exited
		}
		close(stopCommit)
		<-commitDone
	}()

	openLane := func(partition uint32) *partitionLane {
		lane := &partitionLane{partition: partition, queue: make(chan *Record, 1), exited: make(chan struct{})}
		lanes[partition] = lane
		go func() {
			defer close(lane.exited)
			for record := range lane.queue {
				err := runtime.deliver(work, record, handler)
				completions <- laneCompletion{lane: lane, record: record, err: err}
			}
		}()
		return lane
	}

	// route finds or opens the lane for record, waiting out the cap on the
	// freed signal (never by reading completions directly, which only the
	// commit loop may do) when every lane is busy.
	route := func(record *Record) (*partitionLane, error) {
		if lane, open := lanes[record.PartitionID]; open {
			return lane, nil
		}
		for len(lanes) >= maxPartitions {
			retired := false
			for partition, lane := range lanes {
				if lane.isBusy() {
					continue
				}
				close(lane.queue)
				<-lane.exited
				delete(lanes, partition)
				retired = true
				break
			}
			if retired {
				break
			}
			select {
			case err := <-fatal:
				return nil, err
			case <-freed:
			case <-ctx.Done():
				return nil, errCleanStop
			}
		}
		return openLane(record.PartitionID), nil
	}

	// source.Next is only ever called from this dedicated goroutine, one call
	// at a time, paced by askNext: a plain "call Next again right away" loop
	// would let it block for arbitrarily long (a quiet topic, or this source
	// exhausted with nothing left to read) with no way for the routing loop
	// below to notice a fatal completion in the meantime, since that loop can
	// only react to channels it is actually selecting on. stopNext guards both
	// of its channel operations so it can never leak blocked on a send nobody
	// will ever receive once this function returns.
	type nextOutcome struct {
		record *Record
		err    error
	}
	askNext := make(chan struct{}, 1)
	nextOutcomes := make(chan nextOutcome)
	stopNext := make(chan struct{})
	defer close(stopNext)
	go func() {
		for {
			select {
			case <-askNext:
			case <-stopNext:
				return
			}
			record, err := source.Next(ctx)
			select {
			case nextOutcomes <- nextOutcome{record: record, err: err}:
			case <-stopNext:
				return
			}
		}
	}()
	askNext <- struct{}{}

	var failures uint32
	for {
		select {
		case err := <-fatal:
			return err
		case <-ctx.Done():
			return nil
		case outcome := <-nextOutcomes:
			if outcome.err != nil {
				if ctx.Err() != nil {
					return nil
				}
				settled, failure := runtime.settleUnreadable(work, source, outcome.err)
				if failure != nil {
					return failure
				}
				if settled {
					failures = 0
					askNext <- struct{}{}
					continue
				}
				failures++
				if !recoverableConsumerFailure(outcome.err, failures) {
					return outcome.err
				}
				if !sleepFor(ctx, ConsumerBackoff.Delay(failures)) {
					return nil
				}
				askNext <- struct{}{}
				continue
			}
			failures = 0
			lane, err := route(outcome.record)
			if err != nil {
				if errors.Is(err, errCleanStop) {
					return nil
				}
				return err
			}
			lane.setBusy(true)
			lane.queue <- outcome.record
			askNext <- struct{}{}
		}
	}
}

// commitLoop is the sole owner of source.Commit and of clearing a lane's busy
// flag once its record is settled, so per-partition commit order holds
// without the routing loop ever touching either. A handling failure or an
// unrecoverable commit failure is reported on fatal (at most once; the
// routing loop is the only reader and only needs the first) and ends this
// loop's own processing, but it keeps draining completions already queued
// once stopCommit closes, so a lane that finished before the fatal reason
// reached the routing loop is still committed rather than silently dropped.
func (runtime *AgentRuntime) commitLoop(
	ctx context.Context, work context.Context, source RecordSource,
	completions <-chan laneCompletion, stopCommit <-chan struct{}, commitDone chan<- struct{},
	fatal chan<- error, freed chan struct{},
) {
	defer close(commitDone)
	var commitFailures uint32
	report := func(err error) {
		select {
		case fatal <- err:
		default:
		}
	}
	settle := func(completion laneCompletion) {
		defer func() {
			completion.lane.setBusy(false)
			signal(freed)
		}()
		if completion.err != nil {
			report(completion.err)
			return
		}
		if err := source.Commit(work, completion.record.Checkpoint); err != nil {
			if work.Err() != nil {
				report(&ShutdownTimeoutError{Grace: runtime.grace})
				return
			}
			commitFailures++
			if !recoverableConsumerFailure(err, commitFailures) {
				report(err)
				return
			}
			// A context that ends during this backoff is not reported as a
			// failure: the routing loop's own ctx.Err() check ends the run.
			sleepFor(ctx, ConsumerBackoff.Delay(commitFailures))
			return
		}
		commitFailures = 0
		runtime.endBatch(completion.record)
	}
	for {
		select {
		case completion := <-completions:
			settle(completion)
		case <-stopCommit:
			for {
				select {
				case completion := <-completions:
					settle(completion)
				default:
					return
				}
			}
		}
	}
}

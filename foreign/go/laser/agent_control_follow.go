package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// controlFollowInterval is the least time between two reads of an
// agent.control that has nothing new, so a server that answers a held read at
// once cannot spin the follower.
const controlFollowInterval = 500 * time.Millisecond

// controlDiscoveryStart is the first wait before looking again for an
// agent.control that did not exist when the agent started.
const controlDiscoveryStart = 100 * time.Millisecond

// controlDiscoveryMax is the longest wait between two looks for it.
const controlDiscoveryMax = 30 * time.Second

// controlFollower keeps a ControlBook current from agent.control.
//
// Without one the book reports nothing and every session reads as
// uncontrolled, which is why a mandatory pause gate needs its own follower
// rather than trusting a caller to fold the topic in: a gate that only works
// when somebody remembered to feed it is a cooperative gate wearing a
// mandatory name.
type controlFollower struct {
	book   *ControlBook
	cancel context.CancelFunc
	done   chan struct{}
	// topicID and generation identify the agent.control the book was read
	// from, so a recreated topic is noticed.
	topicID    uint32
	generation uint64
	// report is told about a read that keeps failing. Nil reports nothing.
	report func(error)
}

// controlTopicCheck is how often an idle follower checks that agent.control
// is still the topic it reads.
var controlTopicCheck = 5 * time.Second

// followControl starts a follower over agent.control on the handle's default
// stream, loading what is already there before reporting the book live.
//
// Reporting live only after the first full read is what makes the gate
// safe: until then the book cannot tell "no pause" from "not read yet", and
// a gate acting on that difference would let a paused session's work
// through.
func followControl(
	ctx context.Context, laser *Laser, book *ControlBook, me wire.AgentID,
	report func(error),
) (*controlFollower, error) {
	topic := laser.Stream(laser.defaultStream).Topic(wire.AgentControl)
	follower := &controlFollower{book: book, done: make(chan struct{}), report: report}
	// The first read is bounded, synchronous, and loaded rather than
	// observed: Load is what marks the book as holding the history, and
	// until it does every live request is queued behind a fold that never
	// comes. A caller that gets a follower back holds a book that already
	// knows what happened before it started.
	cursor, err := follower.load(ctx, topic, me)
	if err != nil {
		return nil, err
	}
	book.SetLive(true)

	followCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	follower.cancel = cancel
	go follower.run(followCtx, cursor, me)
	return follower, nil
}

// load reads the newest records of agent.control into the book and returns
// the cursor to follow from.
//
// The read is bounded to the tail of each partition, so a long control
// history costs one window rather than a replay from the start.
func (follower *controlFollower) load(
	ctx context.Context, topic *Topic, me wire.AgentID,
) (*Cursor, error) {
	cursor, err := topic.ReplayAs(controlFollowReader)
	if err != nil {
		return nil, err
	}
	if err := follower.loadSource(ctx, topic); err != nil {
		return nil, err
	}
	if err := cursor.SeekToWindow(ctx, MaxDrainRecords); err != nil {
		return nil, err
	}
	truncated := false
	for _, offset := range cursor.Offsets() {
		truncated = truncated || offset > 0
	}
	follower.book.mutex.Lock()
	follower.book.truncated = truncated
	follower.book.mutex.Unlock()
	if err := foldControlInto(ctx, follower.book, cursor, me, true); err != nil {
		return nil, err
	}
	return cursor, nil
}

// loadSource records which agent.control the book is read from. A topic that
// is not there yet leaves the source unset.
func (follower *controlFollower) loadSource(ctx context.Context, topic *Topic) error {
	details, err := topic.Get(ctx)
	if err != nil || details == nil {
		return err
	}
	stream, err := topic.laser.Stream(topic.stream).Get(ctx)
	if err != nil {
		return err
	}
	if stream == nil {
		return errNoStream()
	}
	follower.topicID, follower.generation = details.ID, details.CreatedAtMicros
	book := follower.book
	book.mutex.Lock()
	book.partitions = details.PartitionsCount
	book.controlSource = &wire.LogPosition{StreamID: stream.ID, TopicID: details.ID}
	book.mutex.Unlock()
	return nil
}

// recreated reports whether agent.control is no longer the topic the book was
// read from.
func (follower *controlFollower) recreated(ctx context.Context, topic *Topic) bool {
	details, err := topic.Get(ctx)
	if err != nil || details == nil {
		return false
	}
	return details.ID != follower.topicID || details.CreatedAtMicros != follower.generation
}

// controlFollowReader is the reader identity the follower names itself to
// the server. It stores no offset: the cursor owns its positions.
const controlFollowReader = "laser-control-follower"

// run reads new control records until the context ends.
//
// Between reads it holds one read per partition on the server, so a request
// is seen when it lands. A topic that is not there yet is looked for again
// after a wait that doubles up to controlDiscoveryMax.
//
// A read failure is not fatal: the follower outlives any one request, and a
// transient refusal must not leave the book stale forever. It tries again
// from the same offsets after a growing delay, and a read that keeps failing
// is reported once, because the book is stale until it recovers.
func (follower *controlFollower) run(ctx context.Context, cursor *Cursor, me wire.AgentID) {
	defer close(follower.done)
	topic := cursor.topic
	var failures uint32
	discovery := controlDiscoveryStart
	checked := time.Now()
	for {
		switch {
		case failures > 0:
			_ = sleepWithin(ctx, max(controlFollowInterval, ConsumerBackoff.Delay(failures)))
		case follower.missing():
			_ = sleepWithin(ctx, Jittered(discovery))
			discovery = nextControlDiscovery(discovery)
		default:
			discovery = controlDiscoveryStart
			_ = sleepWithin(ctx, controlFollowInterval)
		}
		if ctx.Err() != nil {
			return
		}
		read, err := follower.step(ctx, topic, cursor, me)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			failures++
			follower.noteFailure(failures, err)
			continue
		}
		failures = 0
		if read > 0 || time.Since(checked) < controlTopicCheck {
			continue
		}
		checked = time.Now()
		// A recreated agent.control starts every session's control history
		// again, so the book starts again from the new topic.
		if follower.recreated(ctx, topic) {
			follower.book.Reset()
			if reloaded, err := follower.load(ctx, topic, me); err == nil {
				cursor = reloaded
			}
		}
	}
}

// missing reports an agent.control that has not been found yet.
func (follower *controlFollower) missing() bool {
	follower.book.mutex.Lock()
	defer follower.book.mutex.Unlock()
	return follower.book.controlSource == nil
}

// nextControlDiscovery doubles the wait between two looks for agent.control, up to the longest.
func nextControlDiscovery(wait time.Duration) time.Duration {
	return min(wait*2, controlDiscoveryMax)
}

// noteFailure reports a read that has failed as often in a row as a consumer
// tolerates, once per outage.
func (follower *controlFollower) noteFailure(failures uint32, err error) {
	if failures != ConsumerFailureBudget || follower.report == nil {
		return
	}
	follower.report(&TransportError{
		Reason: "follow agent.control, operator requests are not being read", Cause: err})
}

// step folds what one read of agent.control found, reporting how many records
// it read.
func (follower *controlFollower) step(
	ctx context.Context, topic *Topic, cursor *Cursor, me wire.AgentID,
) (int, error) {
	if follower.missing() {
		if err := follower.loadSource(ctx, topic); err != nil {
			return 0, err
		}
	}
	return foldControlBatch(ctx, follower.book, cursor, me, false)
}

// stop ends the follower and waits for its goroutine to finish.
func (follower *controlFollower) stop() {
	if follower == nil {
		return
	}
	follower.cancel()
	<-follower.done
}

// foldControlInto reads what the cursor has not seen and folds it into the
// book, in log order.
//
// A record that is not a control request belongs to another writer and is
// skipped rather than refused: the topic is readable by anyone.
func foldControlInto(
	ctx context.Context, book *ControlBook, cursor *Cursor, me wire.AgentID, load bool,
) error {
	for {
		count, err := foldControlBatch(ctx, book, cursor, me, load)
		if err != nil || !load || count < MaxDrainRecords {
			return err
		}
	}
}

func foldControlBatch(ctx context.Context, book *ControlBook, cursor *Cursor, me wire.AgentID, load bool) (int, error) {
	records, err := cursor.Poll(ctx)
	if err != nil {
		return 0, err
	}
	folded := make([]ControlRecord, 0, len(records))
	for _, record := range records {
		message, err := OpenAgdx(record)
		if err != nil {
			continue
		}
		request, carried := ControlRequestOf(&message.Envelope, &me)
		if !carried {
			continue
		}
		folded = append(folded, ControlRecord{
			Conversation: message.Envelope.Conversation,
			At: ControlPosition{
				Partition: record.PartitionID,
				Offset:    record.Offset(),
			},
			Request: request,
		})
	}
	if load {
		// Load even when nothing was read: "the history is empty" is a
		// different answer from "the history has not been read", and only
		// the first lets the gate act.
		book.Load(folded)
		return len(records), nil
	}
	for _, record := range folded {
		book.Observe(record)
	}
	return len(records), nil
}

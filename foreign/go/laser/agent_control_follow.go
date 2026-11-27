package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// controlFollowInterval is how often the follower reads agent.control for
// new requests once it has caught up.
//
// An operator pausing a session waits this long at worst before the agents
// stop taking its work, which is the latency that matters here: a pause is
// an instruction, not an interrupt, and seconds are what an operator
// expects.
const controlFollowInterval = 500 * time.Millisecond

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
}

// followControl starts a follower over agent.control on the handle's default
// stream, loading what is already there before reporting the book live.
//
// Reporting live only after the first full read is what makes the gate
// safe: until then the book cannot tell "no pause" from "not read yet", and
// a gate acting on that difference would let a paused session's work
// through.
func followControl(
	ctx context.Context, laser *Laser, book *ControlBook, me wire.AgentID,
) (*controlFollower, error) {
	topic := laser.Stream(laser.defaultStream).Topic(wire.AgentControl)
	cursor, err := topic.ReplayAs(controlFollowReader)
	if err != nil {
		return nil, err
	}
	details, err := topic.Get(ctx)
	if err != nil {
		return nil, err
	}
	if details != nil {
		book.SetPartitions(details.PartitionsCount)
	}

	// The first read is bounded, synchronous, and loaded rather than
	// observed: Load is what marks the book as holding the history, and
	// until it does every live request is queued behind a fold that never
	// comes. A caller that gets a follower back holds a book that already
	// knows what happened before it started.
	if err := foldControlInto(ctx, book, cursor, me, true); err != nil {
		return nil, err
	}
	book.SetLive(true)

	followCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	follower := &controlFollower{book: book, cancel: cancel, done: make(chan struct{})}
	go follower.run(followCtx, cursor, me)
	return follower, nil
}

// controlFollowReader is the reader identity the follower names itself to
// the server. It stores no offset: the cursor owns its positions.
const controlFollowReader = "laser-control-follower"

// run reads new control records until the context ends.
//
// A read failure is not fatal: the follower outlives any one request, and a
// transient refusal must not leave the book stale forever. The next tick
// tries again from the same offsets.
func (follower *controlFollower) run(ctx context.Context, cursor *Cursor, me wire.AgentID) {
	defer close(follower.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(controlFollowInterval):
		}
		if ctx.Err() != nil {
			return
		}
		_ = foldControlInto(ctx, follower.book, cursor, me, false)
	}
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
	records, err := cursor.Poll(ctx)
	if err != nil {
		return err
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
		return nil
	}
	for _, record := range folded {
		book.Observe(record)
	}
	return nil
}

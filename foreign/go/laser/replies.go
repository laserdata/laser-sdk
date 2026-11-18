package laser

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ExpectedReply is what a waiter will accept as the answer to its request.
//
// A correlation alone is not enough to match. Two conversations can reuse one
// correlation, and a shared reply topic carries every agent's answers, so the
// session and the requester are checked too: without them a waiter can be
// woken by somebody else's reply and act on it.
type ExpectedReply struct {
	// Session is the conversation the reply must belong to.
	Session wire.ConversationID
	// Requester is the agent the reply must be addressed to. Empty accepts a
	// reply addressed to anyone, which is what a sole consumer of a private
	// reply topic wants.
	Requester wire.AgentID
	// Command is the record the reply must answer, when the waiter knows it.
	Command *wire.RecordID
}

// Accepts reports whether a message answers this request.
func (expected ExpectedReply) Accepts(message *AgdxMessage) bool {
	if message.Record == nil || message.Record.Provenance == nil {
		return false
	}
	if message.Record.Provenance.Conversation != expected.Session {
		return false
	}
	// Only an answer answers. A command or an event on the same correlation is
	// somebody else's traffic in the same exchange.
	if message.Envelope.Kind != wire.KindResponse && message.Envelope.Kind != wire.KindError {
		return false
	}
	if expected.Command != nil && message.Envelope.Cause != nil &&
		*message.Envelope.Cause != *expected.Command {
		return false
	}
	// An unaddressed reply on a shared topic could be for anyone, so a waiter
	// that knows who it is will not take it. A waiter that does not care takes
	// whatever arrives.
	if expected.Requester.IsZero() {
		return true
	}
	if message.Envelope.Target == nil {
		return false
	}
	return *message.Envelope.Target == expected.Requester
}

// Acknowledges reports whether a message is the target saying it has started
// work, which is not the answer but means the request was picked up.
//
// A waiter uses it to tell "nobody is listening" from "somebody is working on
// it", so a slow handler is waited out rather than retried into a duplicate.
func (expected ExpectedReply) Acknowledges(message *AgdxMessage) bool {
	if message.Record == nil || message.Record.Provenance == nil {
		return false
	}
	if message.Record.Provenance.Conversation != expected.Session {
		return false
	}
	return message.Envelope.Kind == wire.KindStatus &&
		message.Envelope.Operation != nil &&
		*message.Envelope.Operation == wire.OperationTask &&
		message.Envelope.TaskState != nil &&
		*message.Envelope.TaskState == wire.TaskWorking
}

// ReplyHub matches incoming replies to the requests waiting for them.
//
// One hub reads a reply topic once for every waiter, so a hundred concurrent
// requests share one read rather than each scanning the topic.
type ReplyHub struct {
	mutex   sync.Mutex
	waiters map[wire.CorrelationID]*replyWaiter
}

type replyWaiter struct {
	expected ExpectedReply
	answer   chan *AgdxMessage
	// progress carries acknowledgments. It is a buffered signal rather than a
	// closed channel because a target may report progress many times, and the
	// waiter has to be able to see each one without the channel being
	// replaced underneath a concurrent delivery.
	progress     chan struct{}
	acknowledged atomic.Bool
}

// NewReplyHub opens a hub with nobody waiting.
func NewReplyHub() *ReplyHub {
	return &ReplyHub{waiters: map[wire.CorrelationID]*replyWaiter{}}
}

// Ticket is one registered wait.
type Ticket struct {
	hub         *ReplyHub
	correlation wire.CorrelationID
	waiter      *replyWaiter
}

// Subscribe registers a wait before the request is sent.
//
// Registering first is what makes the wait reliable: the reply cannot exist
// until the request is sent, so a hub that only started listening afterwards
// could miss an answer that arrived in between.
func (hub *ReplyHub) Subscribe(
	correlation wire.CorrelationID, expected ExpectedReply,
) (*Ticket, error) {
	hub.mutex.Lock()
	defer hub.mutex.Unlock()
	if _, waiting := hub.waiters[correlation]; waiting {
		return nil, &InvalidError{
			Reason: "a request is already waiting on this correlation"}
	}
	waiter := &replyWaiter{
		expected: expected,
		answer:   make(chan *AgdxMessage, 1),
		progress: make(chan struct{}, 1),
	}
	hub.waiters[correlation] = waiter
	return &Ticket{hub: hub, correlation: correlation, waiter: waiter}, nil
}

// Deliver offers a message to whoever is waiting for it, reporting whether it
// was taken.
//
// A message nobody is waiting for, or one that does not satisfy the waiter, is
// left alone: it belongs to another exchange and some other reader's business.
func (hub *ReplyHub) Deliver(message *AgdxMessage) bool {
	if message.Envelope.Correlation == nil {
		return false
	}
	// The expectation is copied under the lock rather than read through the
	// waiter afterwards: a requester narrows it to the command it sent once
	// the send returns, which races a delivery that is already in flight.
	hub.mutex.Lock()
	waiter, waiting := hub.waiters[*message.Envelope.Correlation]
	var expected ExpectedReply
	if waiting {
		expected = waiter.expected
	}
	hub.mutex.Unlock()
	if !waiting {
		return false
	}

	if expected.Acknowledges(message) {
		waiter.acknowledged.Store(true)
		// A dropped signal is harmless: one unread acknowledgment is already
		// pending, and the waiter only needs to know work is happening.
		select {
		case waiter.progress <- struct{}{}:
		default:
		}
		return true
	}
	if !expected.Accepts(message) {
		return false
	}
	select {
	case waiter.answer <- message:
		return true
	default:
		// The answer is already in hand. A second one is a duplicate of a
		// settled exchange, so it is left rather than overwriting the first.
		return false
	}
}

// Waiting is how many requests the hub is holding answers for.
func (hub *ReplyHub) Waiting() int {
	hub.mutex.Lock()
	defer hub.mutex.Unlock()
	return len(hub.waiters)
}

// Await blocks until the reply arrives, the context ends, or the deadline
// passes.
//
// An acknowledgment extends the wait rather than ending it: the target said it
// is working, so the deadline is for a reply that never comes rather than for
// a handler that is slow.
func (ticket *Ticket) Await(ctx context.Context, timeout time.Duration) (*AgdxMessage, error) {
	defer ticket.Close()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case message := <-ticket.waiter.answer:
			return message, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticket.waiter.progress:
			// The target is working, so the deadline is for a reply that never
			// comes rather than for a handler that is slow.
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(timeout)
		case <-timer.C:
			return nil, &TimeoutError{Correlation: ticket.correlation, Waited: timeout}
		}
	}
}

// Acknowledged reports whether the target has said it is working on this
// request.
func (ticket *Ticket) Acknowledged() bool {
	return ticket.waiter.acknowledged.Load()
}

// Expect narrows the wait to the command that was sent.
//
// A requester only learns the record id after the send, and the wait has to be
// registered before it, so the expectation is narrowed here rather than being
// complete at subscribe time.
func (ticket *Ticket) Expect(command wire.RecordID) {
	ticket.hub.mutex.Lock()
	defer ticket.hub.mutex.Unlock()
	ticket.waiter.expected.Command = &command
}

// Correlation is what this ticket waits on.
func (ticket *Ticket) Correlation() wire.CorrelationID { return ticket.correlation }

// Close gives up the wait. It is safe to call more than once.
func (ticket *Ticket) Close() {
	ticket.hub.mutex.Lock()
	defer ticket.hub.mutex.Unlock()
	delete(ticket.hub.waiters, ticket.correlation)
}

// TimeoutError reports a request whose reply never arrived.
//
// It names the correlation, because a caller running many requests needs to
// know which one gave up.
type TimeoutError struct {
	Correlation wire.CorrelationID
	Waited      time.Duration
}

func (e *TimeoutError) Error() string {
	return "no reply on correlation " + e.Correlation.String() +
		" within " + e.Waited.String()
}

// Timeout marks this as a deadline rather than a fault, so a caller can retry
// on it without matching the type.
func (e *TimeoutError) Timeout() bool { return true }

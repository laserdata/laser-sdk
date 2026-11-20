package laser

import (
	"context"
	"errors"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Verdict is what an agent runtime decided to do with a record before any
// handler saw it.
type Verdict int

const (
	// Deliver means the record reaches the handler.
	Deliver Verdict = iota
	// Skip means the record was already handled, so applying it again would
	// double an effect the caller already had.
	Skip
	// DeadLetter means the record cannot be handled and must not be retried:
	// it expired, it was fenced out, or it asks for something this build does
	// not implement.
	DeadLetter
)

func (verdict Verdict) String() string {
	switch verdict {
	case Skip:
		return "skip"
	case DeadLetter:
		return "dead letter"
	default:
		return "deliver"
	}
}

// Decision is a verdict and why it was reached.
type Decision struct {
	Verdict Verdict
	// Reason is the dead-letter code, meaningful only when the verdict is
	// DeadLetter.
	Reason wire.DeadLetterReason
	// Detail says what happened, for a log or a dead-letter capsule.
	Detail string
}

// AgentOptions configure a running agent.
type AgentOptions struct {
	// Understood is the set of must-understand feature bits this build
	// implements. A record asking for a bit outside it is dead-lettered
	// rather than mis-handled.
	Understood uint64
	// Dedup remembers handled operations. Nil opens a default window.
	Dedup *Deduplicator
	// Fence drops a stale holder's replay. Nil opens a default gate.
	Fence *FenceGate
	// Now reads the clock, for a test that needs to control it.
	Now func() time.Time
	// AcceptBroadcast delivers records addressed to every agent. On a shared
	// session topic most records are, so this defaults on.
	//
	// Set it false for an agent that should only see work addressed to it by
	// name.
	AcceptBroadcast *bool
}

// Agent applies the gates every running agent shares, deciding what reaches a
// handler.
//
// It is the half of a runtime that is pure policy: it reads records and
// returns decisions, so the gates can be exercised without a server and a
// caller can wire them into whatever loop it already has.
type Agent struct {
	identity        wire.AgentID
	understood      uint64
	dedup           *Deduplicator
	fence           *FenceGate
	now             func() time.Time
	acceptBroadcast bool
}

// Agent opens the gate set for one agent identity.
func (laser *Laser) Agent(identity wire.AgentID, options AgentOptions) *Agent {
	agent := &Agent{
		identity:        identity,
		understood:      options.Understood,
		dedup:           options.Dedup,
		fence:           options.Fence,
		now:             options.Now,
		acceptBroadcast: true,
	}
	if agent.dedup == nil {
		agent.dedup = NewDeduplicator(DedupWindow)
	}
	if agent.fence == nil {
		agent.fence = NewFenceGate()
	}
	if agent.now == nil {
		agent.now = time.Now
	}
	if options.AcceptBroadcast != nil {
		agent.acceptBroadcast = *options.AcceptBroadcast
	}
	return agent
}

// Identity is the agent these gates run for.
func (agent *Agent) Identity() wire.AgentID { return agent.identity }

// Fence is the gate this agent applies, for a caller that needs to read its
// high water directly.
func (agent *Agent) Fence() *FenceGate { return agent.fence }

// Dedup is the window this agent remembers, for a caller that wants to seed it
// from its own store.
func (agent *Agent) Dedup() *Deduplicator { return agent.dedup }

// Understood is the must-understand feature set this build implements, which a
// runtime checks a message against before it decodes anything else.
func (agent *Agent) Understood() uint64 { return agent.understood }

// AcceptsBroadcast reports whether this agent takes records addressed to every
// agent rather than only work addressed to it by name.
func (agent *Agent) AcceptsBroadcast() bool { return agent.acceptBroadcast }

// Decide applies every gate to a record, in the order that matters.
//
// Expiry and the must-understand set come first, because a record that fails
// either can never be handled and burning a dedup key on it would suppress a
// later record that could be. The fence comes next, because a stale holder's
// replay must not advance anything. Dedup comes last, so a key is spent only
// on a record that would otherwise have been handled.
func (agent *Agent) Decide(record *Record) Decision {
	now := agent.now()

	if record.Expired(now) {
		return Decision{
			Verdict: DeadLetter,
			Reason:  wire.DeadLetterDeadlineExceeded,
			Detail:  "the record passed its deadline before it was handled",
		}
	}
	if !agent.fence.Accept(agent.task(record), agent.fenceOf(record), now) {
		return Decision{
			Verdict: DeadLetter,
			Reason:  wire.DeadLetterRejected,
			Detail:  "the record's fence is behind the highest accepted for its task",
		}
	}
	if !agent.dedup.Accept(DedupKey(record.Provenance)) {
		return Decision{
			Verdict: Skip,
			Detail:  "the record's operation was already handled",
		}
	}
	return Decision{Verdict: Deliver}
}

// DecideMessage applies every gate to an agent message, including the two that
// only an envelope can answer: whether this build understands what the message
// requires, and whether the message is addressed here at all.
func (agent *Agent) DecideMessage(message *AgdxMessage) Decision {
	if !message.Understood(agent.understood) {
		return Decision{
			Verdict: DeadLetter,
			Reason:  wire.DeadLetterRejected,
			Detail:  "the message requires a feature this build does not implement",
		}
	}
	if !agent.addressed(message) {
		return Decision{
			Verdict: Skip,
			Detail:  "the message is addressed to another agent",
		}
	}
	return agent.Decide(message.Record)
}

// addressed reports whether this agent should act on a message.
//
// A message naming this agent is always for it. A message naming nobody is a
// broadcast, which on a shared session topic most records are, so it is
// delivered unless the caller asked for addressed work only.
func (agent *Agent) addressed(message *AgdxMessage) bool {
	if message.Envelope.Target != nil {
		return *message.Envelope.Target == agent.identity
	}
	return agent.acceptBroadcast
}

// task is what a fence is scoped to: the conversation the record belongs to.
func (agent *Agent) task(record *Record) wire.ConversationID {
	if record.Provenance == nil {
		return wire.ConversationID{}
	}
	return record.Provenance.Conversation
}

func (agent *Agent) fenceOf(record *Record) *uint64 {
	if record.Provenance == nil {
		return nil
	}
	return record.Provenance.FenceToken
}

// DeadLetterCapsule builds the record a dead letter is reported as.
//
// The payload is kept so the work can be replayed once the cause is fixed, and
// the source names where it sat so a reader can find the original rather than
// only the copy.
func DeadLetterCapsule(
	source wire.LogPosition, record *Record, decision Decision, attempts uint32,
) wire.AgentDeadLetter {
	capsule := wire.AgentDeadLetter{
		Source:   source,
		Reason:   decision.Reason,
		Attempts: attempts,
		Payload:  record.Payload(),
	}
	if decision.Detail != "" {
		detail := decision.Detail
		capsule.Detail = &detail
	}
	return capsule
}

// DeadLetter publishes a capsule on the stream's dead-letter topic.
//
// A record the gates turned away is kept rather than dropped: the payload is
// on the dead-letter topic so the work can be replayed once the cause is
// fixed, and the source names where the original sat so a reader can find it.
//
// It is partitioned by the record's conversation, so one conversation's dead
// letters stay in the order they failed.
func (laser *Laser) DeadLetter(
	ctx context.Context, record *Record, decision Decision, attempts uint32,
) error {
	if laser.defaultStream == "" {
		return errNoStream()
	}
	source := wire.LogPosition{PartitionID: record.PartitionID, Offset: record.Offset()}
	capsule := DeadLetterCapsule(source, record, decision, attempts)
	payload, err := wire.Marshal(capsule)
	if err != nil {
		return &CodecError{Reason: "encode the dead letter", Cause: err}
	}
	key := ""
	if record.Provenance != nil {
		key = record.Provenance.Conversation.String()
	}
	publish := laser.Stream(laser.defaultStream).Topic(wire.AgentDLQ).
		Publish().Payload(payload)
	if key != "" {
		publish = publish.Key(key)
	}
	_, err = publish.Send(ctx)
	return err
}

// Run reads a consumer and hands each record that passes the gates to handle.
//
// A handler that fails is reported through onFailure and the loop goes on, so
// one bad record does not stop an agent. Passing nil for onFailure ends the
// loop on the first failure instead, which is what a caller that cannot
// tolerate a lost record wants.
//
// Decisions other than Deliver are reported through onDecision, so a caller
// can log them or write dead letters without reimplementing the gates.
func (agent *Agent) Run(
	ctx context.Context,
	consumer *Consumer,
	handle func(context.Context, *Record) error,
	onDecision func(*Record, Decision),
	onFailure func(*Record, error),
) error {
	for {
		record, err := consumer.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return err
		}
		decision := agent.Decide(record)
		if decision.Verdict != Deliver {
			if onDecision != nil {
				onDecision(record, decision)
			}
			continue
		}
		if err := handle(ctx, record); err != nil {
			if onFailure == nil {
				return err
			}
			onFailure(record, err)
		}
	}
}

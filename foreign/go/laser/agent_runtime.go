package laser

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// DefaultShutdownGrace is how long a stopping runtime lets the record in its
// hands finish before it gives up on it.
const DefaultShutdownGrace = 30 * time.Second

// The retry defaults a runtime takes when its caller names none.
const (
	// DefaultRetryAttempts is how many times a retryable handler failure is
	// tried before the record is dead-lettered.
	DefaultRetryAttempts uint32 = 5
	// DefaultRetryBaseDelay is the first backoff between those attempts,
	// doubled on each one.
	DefaultRetryBaseDelay = 200 * time.Millisecond
)

// AgentMessage is one record delivered to a handler: the record it arrived on,
// the AGDX envelope when it carries one, and what the body is encoded as.
//
// Its spine is already decoded: for an AGDX record it is synthesized from the
// envelope, so routing, the gates and the deadline read one shape whichever
// way the record was produced.
type AgentMessage struct {
	// Record is where this message sits on the log, with its spine.
	Record *Record
	// Envelope is the AGDX envelope the record carries, nil for a record
	// produced by a plain agent send.
	Envelope *wire.AgentEnvelope
	// ContentType is how Body is encoded, raw when the producer stamped
	// nothing.
	ContentType wire.ContentType
}

// Body is the task body whichever shape the message arrived in: the AGDX
// envelope's body, or the raw payload of a plain record.
//
// A handler reads this so it never has to know whether it was reached by an
// AGDX command or by a plain send.
func (message *AgentMessage) Body() []byte {
	if message.Envelope != nil {
		return message.Envelope.Body
	}
	return message.Record.Payload()
}

// ID is where the message sits on the log.
func (message *AgentMessage) ID() MessageID {
	return NewMessageID(message.Record.PartitionID, message.Record.Offset())
}

// Provenance is the message's spine.
func (message *AgentMessage) Provenance() *Provenance { return message.Record.Provenance }

// Conversation is the conversation this message belongs to.
func (message *AgentMessage) Conversation() wire.ConversationID {
	if message.Record.Provenance == nil {
		return wire.ConversationID{}
	}
	return message.Record.Provenance.Conversation
}

// Agdx is the message as a received agent message, for a surface that reads
// one, and false when the record carries no envelope.
func (message *AgentMessage) Agdx() (*AgdxMessage, bool) {
	if message.Envelope == nil {
		return nil, false
	}
	return &AgdxMessage{Envelope: *message.Envelope, Record: message.Record}, true
}

// AgentHandler is what an agent does with one message.
type AgentHandler interface {
	Handle(ctx context.Context, message *AgentMessage, agent *AgentCtx) error
}

// HandlerFunc adapts a function to AgentHandler.
type HandlerFunc func(ctx context.Context, message *AgentMessage, agent *AgentCtx) error

// Handle runs the function.
func (handle HandlerFunc) Handle(
	ctx context.Context, message *AgentMessage, agent *AgentCtx,
) error {
	return handle(ctx, message, agent)
}

// AgentMiddleware wraps every dispatch, for authorization, metrics and tracing
// without touching the handler.
//
// BeforeHandle runs ahead of the retry loop and may refuse the message: a
// refusal a repeat cannot help dead-letters it without the handler ever
// running, and a retryable one is retried under the runtime's retry policy.
// AfterHandle sees every attempt's outcome and its one-based number, so a
// metrics sink counts retries as well as outcomes.
type AgentMiddleware interface {
	BeforeHandle(ctx context.Context, message *AgentMessage) error
	AfterHandle(ctx context.Context, message *AgentMessage, result error, attempt uint32)
}

// DeadLetterSink is told about every capsule the runtime writes, with the
// outcome of publishing it.
//
// A publish that failed after the in-place retries stops the runtime with the
// poison record uncommitted, so it is read again on the next run. This is the
// seam an operator wires an alert to rather than grepping logs.
type DeadLetterSink func(
	ctx context.Context, message *AgentMessage,
	capsule wire.AgentDeadLetter, published error,
)

// RuntimeObserver watches what a runtime did with a record without changing
// it. Every hook is optional.
type RuntimeObserver struct {
	// OnSkip reports a record that was committed without reaching the
	// handler because it is not this agent's work.
	OnSkip func(message *AgentMessage, dispatch wire.Dispatch)
	// OnDrop reports a record a gate turned away.
	OnDrop func(message *AgentMessage, decision Decision)
	// OnError reports a best-effort side action that did not happen: a
	// pickup acknowledgment, or the error answering a dead-lettered command.
	// The record is still handled and committed.
	OnError func(message *AgentMessage, err error)
}

// RetryPolicy is how a runtime retries a retryable handler failure.
type RetryPolicy struct {
	// MaxAttempts is the total attempts before the record is dead-lettered.
	MaxAttempts uint32
	// BaseDelay is the first backoff, doubled on each later attempt.
	BaseDelay time.Duration
}

func (policy RetryPolicy) withDefaults() RetryPolicy {
	if policy.MaxAttempts == 0 {
		policy.MaxAttempts = DefaultRetryAttempts
	}
	if policy.BaseDelay <= 0 {
		policy.BaseDelay = DefaultRetryBaseDelay
	}
	return policy
}

// DelayFor is the backoff before the attempt after this zero-based one.
//
// The doubling is capped at sixteen shifts, which is already far past any
// deadline a record carries, so a long-lived retry loop cannot overflow its
// delay into a negative one.
func (policy RetryPolicy) DelayFor(attempt uint32) time.Duration {
	shift := attempt
	if shift > 16 {
		shift = 16
	}
	delay := policy.BaseDelay * time.Duration(1<<shift)
	if delay <= 0 {
		return policy.BaseDelay * time.Duration(1<<16)
	}
	return delay
}

// RecordSource is what a runtime reads records from and stores its position
// against. A Consumer is one.
//
// It is a seam rather than a concrete consumer so the runtime can be driven
// by whatever already reads the topic, and so its gates can be exercised
// without a server.
type RecordSource interface {
	Next(ctx context.Context) (*Record, error)
	Commit(ctx context.Context, checkpoint iggcon.Checkpoint) error
}

// RuntimeOptions configure an agent runtime.
type RuntimeOptions struct {
	// Topic is the topic the runtime reads. It decides classification: a
	// control operation counts as control only on the control topic.
	Topic string
	// Stream is where dead letters and answers ride. Empty takes the
	// handle's default stream.
	Stream string
	// RespondOn is the topic this agent answers on. Empty leaves a handler
	// unable to respond, which is what a one-way worker wants.
	RespondOn string
	// Operations are the command operations the handler serves. Nil serves
	// every operation, so a model or tool record an agent writes for its own
	// timeline never becomes its own work.
	Operations []string
	// Retry is how a retryable handler failure is retried.
	Retry RetryPolicy
	// AckOnPickup emits a working task status the moment an AGDX command is
	// taken, before the handler runs, so a caller can tell a command that was
	// consumed from one that expired unconsumed. It needs RespondOn.
	AckOnPickup bool
	// ShutdownGrace is how long a stopping runtime lets the record in its
	// hands finish. Zero takes DefaultShutdownGrace.
	ShutdownGrace time.Duration
	// Middleware wrap each dispatch, in order.
	Middleware []AgentMiddleware
	// OnDeadLetter is told about every capsule the runtime writes.
	OnDeadLetter DeadLetterSink
	// Budget refuses work for a session over its budget. Nil checks none,
	// which is what a deployment without the session index has anyway.
	Budget *BudgetGate
	// Control is the operator control a handler reads through its context.
	// Nil opens an empty book, which reports nothing until a caller folds
	// agent.control into it.
	Control *ControlBook
	// Sessions is the session surface a handler reads through its context.
	// Nil opens the handle's default.
	Sessions *Sessions
	// Observer watches what the runtime did with each record.
	Observer RuntimeObserver
}

// AgentRuntime is the reliable consumer: it reads a topic, decodes and gates
// each record, hands the work to a handler, commits what it handled, and
// dead-letters what cannot be handled.
//
// One runtime drives one topic for one agent. The gates it applies are the
// Agent's, so a caller that already holds one keeps a single dedup window and
// fence high water across whatever else reads through it.
type AgentRuntime struct {
	laser    *Laser
	agent    *Agent
	identity wire.AgentID
	topic    string
	stream   string

	respondOn   string
	handled     wire.HandledOperations
	retry       RetryPolicy
	ackOnPickup bool
	grace       time.Duration

	middleware   []AgentMiddleware
	onDeadLetter DeadLetterSink
	observer     RuntimeObserver

	budget   *BudgetGate
	control  *ControlBook
	sessions *Sessions
	replies  *ReplyHub

	// source names the stream and topic the runtime reads, resolved once at
	// start so every capsule can locate its poison record without a round
	// trip per failure.
	source wire.LogPosition
	// batches counts the poll batches read per partition, which scopes the
	// budget gate's cached verdicts to one batch the way a head offset does
	// for the Rust client.
	batches map[uint32]uint64
}

// AgentRuntime opens the reliable consumer runtime for one agent on one topic.
func (laser *Laser) AgentRuntime(
	agent *Agent, options RuntimeOptions,
) (*AgentRuntime, error) {
	if agent == nil {
		return nil, &InvalidError{Reason: "an agent runtime runs one agent's gates, so it needs one"}
	}
	if options.Topic == "" {
		return nil, &InvalidError{Reason: "an agent runtime names the topic it reads"}
	}
	if options.AckOnPickup && options.RespondOn == "" {
		return nil, &InvalidError{Reason: "acknowledging a pickup needs RespondOn, " +
			"the topic the acknowledgment is sent on"}
	}
	stream := options.Stream
	if stream == "" {
		stream = laser.defaultStream
	}
	if stream == "" {
		return nil, errNoStream()
	}
	sessions := options.Sessions
	if sessions == nil {
		sessions = laser.SessionsIn(stream)
	}
	control := options.Control
	if control == nil {
		control = NewControlBook()
	}
	handled := wire.AnyOperation()
	if options.Operations != nil {
		handled = wire.OnlyOperations(options.Operations...)
	}
	grace := options.ShutdownGrace
	if grace <= 0 {
		grace = DefaultShutdownGrace
	}
	return &AgentRuntime{
		laser:        laser,
		agent:        agent,
		identity:     agent.Identity(),
		topic:        options.Topic,
		stream:       stream,
		respondOn:    options.RespondOn,
		handled:      handled,
		retry:        options.Retry.withDefaults(),
		ackOnPickup:  options.AckOnPickup,
		grace:        grace,
		middleware:   options.Middleware,
		onDeadLetter: options.OnDeadLetter,
		observer:     options.Observer,
		budget:       options.Budget,
		control:      control,
		sessions:     sessions,
		replies:      NewReplyHub(),
		batches:      map[uint32]uint64{},
	}, nil
}

// Replies is the hub this runtime offers every reply it reads to, which is
// what lets a handler's own request be answered by the loop that is already
// reading the topic.
func (runtime *AgentRuntime) Replies() *ReplyHub { return runtime.replies }

// Control is the operator control book handlers read through their context.
func (runtime *AgentRuntime) Control() *ControlBook { return runtime.control }

// Topic is the topic this runtime reads.
func (runtime *AgentRuntime) Topic() string { return runtime.topic }

// Run reads source until the context ends, handing each record that is this
// agent's work to handler.
//
// It returns nil on a clean stop. It returns an error only when a record could
// not be settled: a dead letter that could not be published leaves its record
// uncommitted, so the work is read again rather than silently lost, and the
// runtime stops rather than carrying on past it.
//
// A cancelled context stops the loop, and the record in flight keeps running
// for the shutdown grace so a handler is not cut off mid-step. A record still
// running when the grace passes ends the run with a ShutdownTimeoutError.
func (runtime *AgentRuntime) Run(
	ctx context.Context, source RecordSource, handler AgentHandler,
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

	var failures uint32
	for {
		if ctx.Err() != nil {
			return nil
		}
		record, err := source.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			settled, failure := runtime.settleUnreadable(work, source, err)
			if failure != nil {
				return failure
			}
			if settled {
				failures = 0
				continue
			}
			failures++
			if !recoverableConsumerFailure(err, failures) {
				return err
			}
			if !sleepFor(ctx, ConsumerBackoff.Delay(failures)) {
				return nil
			}
			continue
		}
		failures = 0
		if err := runtime.deliver(work, record, handler); err != nil {
			return err
		}
		if err := source.Commit(work, record.Checkpoint); err != nil {
			if work.Err() != nil {
				return &ShutdownTimeoutError{Grace: runtime.grace}
			}
			// The record was handled but its position was not stored, so it
			// is read again and the dedup window answers for it. That is the
			// at-least-once contract rather than a failure of this run.
			failures++
			if !recoverableConsumerFailure(err, failures) {
				return err
			}
			if !sleepFor(ctx, ConsumerBackoff.Delay(failures)) {
				return nil
			}
		}
		runtime.endBatch(record)
	}
}

// settleUnreadable dead-letters a record whose spine did not decode, reporting
// whether it dealt with the failure at all.
//
// The payload rides into the capsule verbatim, so nothing is lost and a fixed
// reader can redrive the bytes. The position is then committed: reading it
// again would only fail to decode again.
func (runtime *AgentRuntime) settleUnreadable(
	ctx context.Context, source RecordSource, err error,
) (bool, error) {
	var unreadable *UndecodableRecord
	if !errors.As(err, &unreadable) {
		return false, nil
	}
	record := &Record{
		PartitionID: unreadable.PartitionID,
		Message:     unreadable.Message,
		Checkpoint:  unreadable.Checkpoint,
	}
	if failure := runtime.deadLetterRaw(ctx, record, unreadable.Cause); failure != nil {
		return false, failure
	}
	if failure := source.Commit(ctx, unreadable.Checkpoint); failure != nil {
		return false, failure
	}
	return true, nil
}

// deliver settles one record: gate it, hand it to the handler, or dead-letter
// it. An error means the record could not be settled and must not be
// committed.
func (runtime *AgentRuntime) deliver(
	ctx context.Context, record *Record, handler AgentHandler,
) error {
	message, opened, err := runtime.open(ctx, record)
	if err != nil || !opened {
		return err
	}
	if !runtime.addressedHere(message) {
		runtime.skipped(message, wire.DispatchForeign)
		return nil
	}
	if dispatch := runtime.classify(message); dispatch != wire.DispatchWork {
		// A reply is offered to the hub before it is skipped, so a request a
		// handler made is answered by the loop already reading this topic.
		if dispatch == wire.DispatchReply {
			if agdx, carried := message.Agdx(); carried {
				runtime.replies.Deliver(agdx)
			}
		}
		runtime.skipped(message, dispatch)
		return nil
	}
	if !runtime.admit(ctx, message) {
		runtime.skipped(message, wire.DispatchObservational)
		return nil
	}
	decision := runtime.agent.Decide(record)
	switch decision.Verdict {
	case Skip:
		runtime.dropped(message, decision)
		return nil
	case DeadLetter:
		runtime.dropped(message, decision)
		return runtime.deadLetter(ctx, message, decision.Reason, 0, decision.Detail)
	}
	runtime.acknowledgePickup(ctx, message)
	return runtime.handle(ctx, message, handler)
}

// open decodes a record into the message a handler sees, reporting false when
// the record was dead-lettered instead.
//
// An AGDX record's spine is synthesized from its envelope rather than read off
// the headers, which is what lets the gates, the routing filter and the
// deadline read one shape for both record shapes.
func (runtime *AgentRuntime) open(
	ctx context.Context, record *Record,
) (*AgentMessage, bool, error) {
	stamp, err := envelopeStamp(record)
	if err != nil {
		return nil, false, runtime.deadLetterRaw(ctx, record, err)
	}
	message := &AgentMessage{Record: record, ContentType: stamp.contentType}
	if !stamp.present {
		if record.Provenance == nil {
			return nil, false, runtime.deadLetterRaw(ctx, record,
				&NoProvenanceError{})
		}
		return message, true, nil
	}
	if stamp.version != wire.AgentOpVersion {
		return nil, false, runtime.deadLetterRaw(ctx, record, &InvalidError{
			Reason: "the record carries agent envelope version " +
				itoa(int(stamp.version)) + ", and this client speaks " +
				itoa(int(wire.AgentOpVersion))})
	}
	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(record.Payload(), &envelope); err != nil {
		return nil, false, runtime.deadLetterRaw(ctx, record,
			&CodecError{Reason: "decode the agent envelope", Cause: err})
	}
	if err := envelope.Validate(); err != nil {
		return nil, false, runtime.deadLetterRaw(ctx, record,
			&InvalidError{Reason: "agent envelope: " + err.Error()})
	}
	if unmet := envelope.UnmetRequirements(runtime.agent.Understood()); unmet != 0 {
		return nil, false, runtime.deadLetterRaw(ctx, record, &UnderstoodError{Unmet: unmet})
	}
	message.Envelope = &envelope
	record.Provenance = ProvenanceFromEnvelope(&envelope)
	return message, true, nil
}

// addressedHere reports whether this agent should act on a message.
//
// A record addressed to another agent is skipped rather than handled, which
// bites when a publisher mis-addresses it or when two identities joined one
// consumer group by mistake. Losing the other identity's record in that second
// case is deliberate: the fix is one group per agent, not this client quietly
// doing another agent's work.
func (runtime *AgentRuntime) addressedHere(message *AgentMessage) bool {
	target := message.Record.Provenance.TargetAgent
	if target == nil {
		return runtime.agent.AcceptsBroadcast()
	}
	if runtime.identity.IsZero() {
		return true
	}
	return *target == runtime.identity
}

// classify is what this agent does with a message, under the dispatch rules
// every client and the session fold share.
func (runtime *AgentRuntime) classify(message *AgentMessage) wire.Dispatch {
	me := runtime.identity
	if me.IsZero() && message.Record.Provenance.TargetAgent != nil {
		// A runtime without an identity classifies a record as its own
		// addressee would, so it reads the work that agent would read.
		me = *message.Record.Provenance.TargetAgent
	}
	if message.Envelope != nil {
		return wire.Classify(message.Envelope, runtime.topic, me, runtime.handled)
	}
	provenance := message.Record.Provenance
	var addressee *string
	if provenance.TargetAgent != nil {
		named := provenance.TargetAgent.String()
		addressee = &named
	}
	return wire.ClassifyGeneric(addressee, provenance.CausalParent != nil,
		provenance.CorrelationID != nil, runtime.topic, me)
}

// admit reports whether a session's work may reach the handler, under its
// budget.
//
// A deployment without the session index cannot answer, and work is admitted
// there rather than held: a budget nobody can check is not a reason to drop
// work.
func (runtime *AgentRuntime) admit(ctx context.Context, message *AgentMessage) bool {
	if runtime.budget == nil || runtime.identity.IsZero() {
		return true
	}
	session := runtime.sessions.Open(message.Conversation(), "").As(runtime.identity)
	return runtime.budget.Admit(ctx, session, message.Record.PartitionID,
		runtime.batches[message.Record.PartitionID])
}

// endBatch counts a finished poll batch, which is what scopes the budget
// gate's cached verdicts: a later batch reads the index again.
func (runtime *AgentRuntime) endBatch(record *Record) {
	if record.EndsBatch {
		runtime.batches[record.PartitionID]++
	}
}

// handle runs the middleware and the handler, retrying a retryable failure and
// dead-lettering what it cannot get through.
func (runtime *AgentRuntime) handle(
	ctx context.Context, message *AgentMessage, handler AgentHandler,
) error {
	agentCtx := runtime.contextFor(message)
	for _, middleware := range runtime.middleware {
		settled, err := runtime.admitMiddleware(ctx, middleware, message)
		if err != nil || settled {
			return err
		}
	}
	var attempt uint32
	for {
		result := runtime.invoke(ctx, handler, message, agentCtx)
		for _, middleware := range runtime.middleware {
			middleware.AfterHandle(ctx, message, result, attempt+1)
		}
		if result == nil {
			return nil
		}
		if !RetryableError(result) {
			return runtime.deadLetter(ctx, message, wire.DeadLetterRejected,
				attempt+1, result.Error())
		}
		if attempt+1 >= runtime.retry.MaxAttempts {
			return runtime.deadLetter(ctx, message, wire.DeadLetterRetryExhausted,
				attempt+1, result.Error())
		}
		if !sleepFor(ctx, runtime.retry.DelayFor(attempt)) {
			return &ShutdownTimeoutError{Grace: runtime.grace}
		}
		attempt++
	}
}

// admitMiddleware runs one middleware ahead of the handler, reporting whether
// it settled the record itself by refusing it.
func (runtime *AgentRuntime) admitMiddleware(
	ctx context.Context, middleware AgentMiddleware, message *AgentMessage,
) (bool, error) {
	var attempt uint32
	for {
		err := middleware.BeforeHandle(ctx, message)
		if err == nil {
			return false, nil
		}
		if !RetryableError(err) {
			return true, runtime.deadLetter(ctx, message, wire.DeadLetterRejected,
				0, err.Error())
		}
		if attempt+1 >= runtime.retry.MaxAttempts {
			return true, runtime.deadLetter(ctx, message,
				wire.DeadLetterRetryExhausted, attempt+1, err.Error())
		}
		if !sleepFor(ctx, runtime.retry.DelayFor(attempt)) {
			return true, &ShutdownTimeoutError{Grace: runtime.grace}
		}
		attempt++
	}
}

// invoke runs the handler, turning a panic into a refusal.
//
// A panicking handler is a bug in that handler rather than a transient
// condition, so the record is dead-lettered as rejected and the runtime keeps
// reading instead of taking the process down with it.
func (runtime *AgentRuntime) invoke(
	ctx context.Context, handler AgentHandler,
	message *AgentMessage, agentCtx *AgentCtx,
) (err error) {
	defer func() {
		if panicked := recover(); panicked != nil {
			err = &InvalidError{
				Reason: fmt.Sprintf("the handler panicked: %v", panicked)}
		}
	}()
	return handler.Handle(ctx, message, agentCtx)
}

// acknowledgePickup reports a command as working the moment it is taken, so a
// caller can tell a command that was consumed from one that expired before
// anybody read it.
//
// Best effort: the command survived every gate, so a failed acknowledgment is
// reported and the work goes ahead rather than being refused for want of a
// status nobody is required to read.
func (runtime *AgentRuntime) acknowledgePickup(ctx context.Context, message *AgentMessage) {
	if !runtime.ackOnPickup || runtime.identity.IsZero() {
		return
	}
	envelope := message.Envelope
	if envelope == nil || envelope.Kind != wire.KindCommand || envelope.Correlation == nil {
		return
	}
	send := runtime.answering(envelope, runtime.respondOn).
		TaskStatus(*envelope.Correlation, wire.TaskWorking).
		WithTarget(envelope.Source)
	if _, err := send.Send(ctx); err != nil {
		runtime.failed(message, err)
	}
}

// answering opens the producer that answers one envelope, carrying its
// ancestry so the answer stays in the conversation tree it belongs to.
func (runtime *AgentRuntime) answering(
	envelope *wire.AgentEnvelope, topic string,
) *Agdx {
	producer := runtime.laser.AgdxOn(
		runtime.stream, topic, runtime.identity, envelope.Conversation)
	producer.parent, producer.root = envelope.Parent, envelope.Root
	return producer
}

func (runtime *AgentRuntime) skipped(message *AgentMessage, dispatch wire.Dispatch) {
	if runtime.observer.OnSkip != nil {
		runtime.observer.OnSkip(message, dispatch)
	}
}

func (runtime *AgentRuntime) dropped(message *AgentMessage, decision Decision) {
	if runtime.observer.OnDrop != nil {
		runtime.observer.OnDrop(message, decision)
	}
}

func (runtime *AgentRuntime) failed(message *AgentMessage, err error) {
	if runtime.observer.OnError != nil {
		runtime.observer.OnError(message, err)
	}
}

// workContext is the context the handler, the commits and the dead letters run
// under. It outlives a cancelled run context by the shutdown grace, so a
// stopping runtime lets the record in its hands finish rather than cutting it
// off mid-step.
func (runtime *AgentRuntime) workContext(
	ctx context.Context,
) (context.Context, func()) {
	work, cancel := context.WithCancel(context.WithoutCancel(ctx))
	released := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			grace := time.NewTimer(runtime.grace)
			defer grace.Stop()
			select {
			case <-grace.C:
			case <-released:
			}
		case <-released:
		}
		cancel()
	}()
	return work, func() {
		close(released)
		cancel()
	}
}

// resolveSource reads the numeric ids of the stream and topic once, so every
// capsule can name where its poison record sits without a round trip per
// failure.
//
// A lookup that fails leaves the ids at zero: the partition and offset, the
// half that locates a record within its topic, stay right either way, and
// refusing to run for want of a locator would cost more than it saves.
func (runtime *AgentRuntime) resolveSource(ctx context.Context) {
	topic := runtime.laser.Stream(runtime.stream).Topic(runtime.topic)
	client, err := runtime.laser.connected()
	if err != nil {
		return
	}
	streamID, topicID, err := topic.identifiers()
	if err != nil {
		return
	}
	if details, err := client.GetStream(ctx, streamID); err == nil && details != nil {
		runtime.source.StreamID = details.Id
	}
	if details, err := client.GetTopic(ctx, streamID, topicID); err == nil && details != nil {
		runtime.source.TopicID = details.Id
	}
}

// position is where a record sits, named fully enough for a redrive to find
// it again.
func (runtime *AgentRuntime) position(record *Record) wire.LogPosition {
	return wire.LogPosition{
		StreamID:    runtime.source.StreamID,
		TopicID:     runtime.source.TopicID,
		PartitionID: record.PartitionID,
		Offset:      record.Offset(),
	}
}

// deadLetter writes the capsule of a decoded message and answers its sender.
//
// The capsule carries the poison record's position, the reason, the attempts
// it cost and the original payload verbatim, so a redrive is a republish of
// those same bytes.
func (runtime *AgentRuntime) deadLetter(
	ctx context.Context, message *AgentMessage,
	reason wire.DeadLetterReason, attempts uint32, detail string,
) error {
	capsule := DeadLetterCapsule(runtime.position(message.Record), message.Record,
		Decision{Verdict: DeadLetter, Reason: reason, Detail: detail}, attempts)
	// The original spine rides along for inspection, repointed at the poison
	// record. The deadline is cleared: a deadline-bound reader of the
	// dead-letter topic must not drop the capsule for the very deadline that
	// killed the original.
	spine := *message.Record.Provenance
	cause := message.ID()
	spine.CausalParent = &cause
	spine.DeadlineAt = nil
	if err := runtime.publishCapsule(ctx, &spine, capsule, message); err != nil {
		return err
	}
	runtime.answerDeadLetter(ctx, message, reason, detail)
	return nil
}

// deadLetterRaw writes the capsule of a record that did not decode.
//
// The payload rides verbatim so nothing is lost. The capsule keeps the
// record's own conversation when the header still reads, so the dead letter
// stays on that session's timeline, and a record without one gets a
// conversation derived from its position, which is stable across
// redeliveries.
func (runtime *AgentRuntime) deadLetterRaw(
	ctx context.Context, record *Record, cause error,
) error {
	position := runtime.position(record)
	capsule := wire.AgentDeadLetter{
		Source:  position,
		Reason:  wire.DeadLetterDecodeFailed,
		Payload: record.Payload(),
	}
	if cause != nil {
		detail := cause.Error()
		capsule.Detail = &detail
	}
	spine := &Provenance{Conversation: deadLetterConversation(record, position)}
	source := NewMessageID(record.PartitionID, record.Offset())
	spine.CausalParent = &source
	return runtime.publishCapsule(ctx, spine, capsule, nil)
}

// deadLetterConversation keeps an undecodable record on its own session's
// timeline when its header still names one, and otherwise derives one from
// where the record sits, which is stable across redeliveries.
func deadLetterConversation(record *Record, position wire.LogPosition) wire.ConversationID {
	if record.Provenance != nil {
		return record.Provenance.Conversation
	}
	return DeriveConversationID("dead-letter\x1f" +
		itoa(int(position.StreamID)) + "\x1f" + itoa(int(position.TopicID)) + "\x1f" +
		itoa(int(position.PartitionID)) + "\x1f" + utoa(position.Offset))
}

// publishCapsule writes one capsule on the dead-letter topic, retrying a
// retryable failure in place so a brief outage neither loses the record nor
// stops the runtime.
func (runtime *AgentRuntime) publishCapsule(
	ctx context.Context, spine *Provenance,
	capsule wire.AgentDeadLetter, message *AgentMessage,
) error {
	payload, err := wire.Marshal(capsule)
	if err != nil {
		err = &CodecError{Reason: "encode the dead letter", Cause: err}
		runtime.notifyDeadLetter(ctx, message, capsule, err)
		return err
	}
	var failures uint32
	for {
		// The body is marked as CBOR so a reader of the dead-letter topic is
		// self describing without having to guess at the capsule.
		err = runtime.publishedCapsule(ctx, spine, payload)
		if err == nil || !RetryableError(err) ||
			failures+1 >= ConsumerFailureBudget {
			break
		}
		failures++
		if !sleepFor(ctx, ConsumerBackoff.Delay(failures)) {
			break
		}
	}
	runtime.notifyDeadLetter(ctx, message, capsule, err)
	return err
}

func (runtime *AgentRuntime) publishedCapsule(
	ctx context.Context, spine *Provenance, payload []byte,
) error {
	headers, err := spine.Headers()
	if err != nil {
		return err
	}
	// The capsule body is marked as the contract's CBOR, in the kind the
	// header dictionary spells it, so a reader of the dead-letter topic is
	// self describing rather than having to guess at the capsule.
	key, err := iggcon.NewHeaderKeyString(wire.HeaderContentType)
	if err != nil {
		return &InvalidError{Reason: "content type header: " + err.Error()}
	}
	code, _ := wire.ContentCBOR.Code()
	headers = append(headers, iggcon.HeaderEntry{
		Key:   key,
		Value: iggcon.HeaderValue{Kind: iggcon.Uint8, Value: []byte{code}},
	})
	_, err = runtime.laser.Stream(runtime.stream).Topic(wire.AgentDLQ).
		Send(ctx, payload, headers, spine.PartitionKey())
	return err
}

func (runtime *AgentRuntime) notifyDeadLetter(
	ctx context.Context, message *AgentMessage,
	capsule wire.AgentDeadLetter, published error,
) {
	if runtime.onDeadLetter != nil {
		runtime.onDeadLetter(ctx, message, capsule, published)
	}
}

// answerDeadLetter tells the sender of a dead-lettered command that its work
// will not happen, so a caller waiting on a reply reads the refusal at once
// instead of waiting out its deadline.
//
// Best effort: the capsule is already written, so a failed answer is reported
// and the record still commits.
func (runtime *AgentRuntime) answerDeadLetter(
	ctx context.Context, message *AgentMessage,
	reason wire.DeadLetterReason, detail string,
) {
	envelope := message.Envelope
	if envelope == nil || runtime.respondOn == "" || runtime.identity.IsZero() {
		return
	}
	if envelope.Kind != wire.KindCommand || envelope.Correlation == nil {
		return
	}
	send := runtime.answering(envelope, runtime.respondOn).
		Fail(*envelope.Correlation, DeadLetterErrorBody(reason, detail)).
		WithTarget(envelope.Source)
	if envelope.Record != nil {
		at := runtime.position(message.Record)
		send = send.WithCause(*envelope.Record, &at)
	}
	if _, err := send.Send(ctx); err != nil {
		runtime.failed(message, err)
	}
}

// DeadLetterErrorBody is the failure a dead-lettered command answers its
// sender with.
//
// The reason code rides in the detail, so a caller can tell a command that was
// dropped unconsumed from one a handler refused, which the error code alone
// does not say.
func DeadLetterErrorBody(
	reason wire.DeadLetterReason, detail string,
) wire.AgentErrorBody {
	code := wire.ErrorInvalidRequest
	switch reason {
	case wire.DeadLetterDeadlineExceeded:
		code = wire.ErrorDeadlineExceeded
	case wire.DeadLetterRetryExhausted:
		code = wire.ErrorInternal
	}
	body := wire.AgentErrorBody{
		Code:      code,
		Retryable: reason == wire.DeadLetterRetryExhausted,
		Detail: map[string]wire.Value{
			DeadLetterReasonDetail: wire.UintValue(uint64(reason)),
		},
	}
	if detail != "" {
		message := detail
		body.Message = &message
	}
	return body
}

// DeadLetterReasonDetail is the error detail key carrying the dead-letter
// reason code on the error a dead-lettered command answers its sender with.
const DeadLetterReasonDetail = "dead_letter_reason"

// ProvenanceFromEnvelope is the spine an AGDX record carries, read off its
// envelope rather than its headers.
//
// The envelope is the authority for an agent record: it is what a reader
// validates against, and the typed fields a header block spells as text are
// already decoded here. Deriving the spine from it is what lets the gates, the
// routing filter and the deadline read one shape for both record shapes, and
// is why an agent record need not carry the spine headers at all.
func ProvenanceFromEnvelope(envelope *wire.AgentEnvelope) *Provenance {
	source := envelope.Source
	provenance := &Provenance{
		Conversation:       envelope.Conversation,
		ParentConversation: envelope.Parent,
		RootConversation:   envelope.Root,
		Agent:              &source,
		TargetAgent:        envelope.Target,
	}
	if envelope.IdempotencyKey != nil {
		key := envelope.IdempotencyKey.String()
		provenance.IdempotencyKey = &key
	}
	if envelope.Correlation != nil {
		correlation := envelope.Correlation.String()
		provenance.CorrelationID = &correlation
	}
	if envelope.DeadlineMicros != nil {
		deadline := time.UnixMicro(int64(*envelope.DeadlineMicros))
		provenance.DeadlineAt = &deadline
	}
	// The spine's causal parent is where the answered record sits on the log,
	// which only cause_at carries. The envelope's own cause is a record id and
	// stays in the envelope.
	if envelope.CauseAt != nil {
		cause := NewMessageID(envelope.CauseAt.PartitionID, envelope.CauseAt.Offset)
		provenance.CausalParent = &cause
	}
	if usage := envelope.Usage; usage != nil {
		input, output := usage.InputTokens, usage.OutputTokens
		provenance.Usage = &LlmUsage{InputTokens: &input, OutputTokens: &output}
		// Cost rides the envelope in micro-units and the spine in whole
		// currency, so it is converted rather than copied.
		if usage.CostMicros != nil {
			cost := float64(*usage.CostMicros) / 1_000_000
			provenance.Usage.CostUSD = &cost
		}
	}
	// A fenced effect carries its fence as envelope metadata, so the gate
	// reads it the same way it reads the header on a record without an
	// envelope.
	if fence, named := envelope.Metadata[wire.HeaderFence]; named {
		switch fence.Kind {
		case wire.ValueUint:
			token := fence.Uint
			provenance.FenceToken = &token
		case wire.ValueInt:
			if fence.Int >= 0 {
				token := uint64(fence.Int)
				provenance.FenceToken = &token
			}
		}
	}
	return provenance
}

// envelopeStamps are the envelope markers a record's headers carry.
type envelopeStamps struct {
	present     bool
	version     uint32
	contentType wire.ContentType
}

// envelopeStamp reads the envelope markers off a record's headers.
//
// The version header is what says a record carries an envelope at all, so a
// record without it is a plain one rather than a broken agent record.
func envelopeStamp(record *Record) (envelopeStamps, error) {
	stamps := envelopeStamps{contentType: wire.ContentRaw}
	entries, err := iggcon.DeserializeHeaders(record.Message.UserHeaders)
	if err != nil {
		return stamps, &CodecError{Reason: "read the record's headers", Cause: err}
	}
	for _, entry := range entries {
		switch string(entry.Key.Value) {
		case wire.HeaderAgentVersion:
			version, err := uint64Value(entry.Value, wire.HeaderAgentVersion)
			if err != nil {
				return stamps, err
			}
			stamps.present, stamps.version = true, uint32(version)
		case wire.HeaderContentType:
			if len(entry.Value.Value) != 1 {
				return stamps, invalidHeader(wire.HeaderContentType)
			}
			if contentType, named := wire.ContentTypeFromCode(
				entry.Value.Value[0]); named {
				stamps.contentType = contentType
			}
		}
	}
	return stamps, nil
}

// UndecodableRecord reports a record whose header spine did not decode.
//
// It carries the record itself, which a runtime needs to dead-letter the bytes
// verbatim: a record refused with nothing but a reason could not be kept, and
// the work in it would be lost.
type UndecodableRecord struct {
	PartitionID uint32
	Message     iggcon.IggyMessage
	Checkpoint  iggcon.Checkpoint
	Cause       error
}

func (e *UndecodableRecord) Error() string {
	return "the record's spine does not decode: " + e.Cause.Error()
}

func (e *UndecodableRecord) Unwrap() error { return e.Cause }

// UnderstoodError reports a record asking for a protocol feature this build
// does not implement.
//
// It is never retryable: the same build will not understand it on a second
// reading either, so the record is kept on the dead-letter topic for a build
// that does.
type UnderstoodError struct{ Unmet uint64 }

func (e *UnderstoodError) Error() string {
	return "the record requires agent features 0x" + hex64(e.Unmet) +
		", which this build does not implement"
}

// ShutdownTimeoutError reports a record still in flight when a stopping
// runtime's grace ran out.
type ShutdownTimeoutError struct{ Grace time.Duration }

func (e *ShutdownTimeoutError) Error() string {
	return "the record in flight did not finish within the shutdown grace " +
		e.Grace.String()
}

// Timeout marks this as a deadline rather than a fault.
func (e *ShutdownTimeoutError) Timeout() bool { return true }

// RetryableError reports an error a repeat may survive.
//
// A handler failure is retryable unless it says otherwise, which is the Rust
// client's rule: a handler that knows a repeat cannot help says so by refusing
// with a reason that is not retryable, such as an InvalidError, and its record
// is dead-lettered without burning the retry budget.
func RetryableError(err error) bool {
	if err == nil {
		return false
	}
	var classified interface{ Retryable() bool }
	if errors.As(err, &classified) {
		return classified.Retryable()
	}
	var expired interface{ Timeout() bool }
	if errors.As(err, &expired) && expired.Timeout() {
		return true
	}
	switch {
	case errors.As(err, new(*InvalidError)), errors.As(err, new(*CodecError)),
		errors.As(err, new(*ConfigError)), errors.As(err, new(*UnsupportedError)),
		errors.As(err, new(*NoProvenanceError)), errors.As(err, new(*UnderstoodError)):
		return false
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return false
	}
	return true
}

// recoverableConsumerFailure reports a read or commit failure worth carrying
// on past, which is a retryable one inside the failure budget.
func recoverableConsumerFailure(err error, failures uint32) bool {
	return RetryableError(err) && failures < ConsumerFailureBudget
}

// sleepFor waits out a delay, reporting false when the context ended first.
func sleepFor(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// utoa spells an unsigned number in base ten.
func utoa(value uint64) string { return strconv.FormatUint(value, 10) }

// hex64 spells a feature bitset the way the contract names one.
func hex64(value uint64) string {
	const digits = "0123456789abcdef"
	spelled := make([]byte, 16)
	for index := 15; index >= 0; index-- {
		spelled[index] = digits[value&0xf]
		value >>= 4
	}
	return string(spelled)
}

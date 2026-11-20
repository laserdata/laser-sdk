package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// AgentCtx is what a handler answers through.
//
// It is handed to a handler so it can reply, send, request and open its
// session without holding the connection itself. Causality is wired off the
// record being handled: a reply names it as its cause, stays in its
// conversation, and carries the correlation its caller is waiting on.
type AgentCtx struct {
	runtime *AgentRuntime
	message *AgentMessage
}

// contextFor opens the handler context of one message.
func (runtime *AgentRuntime) contextFor(message *AgentMessage) *AgentCtx {
	return &AgentCtx{runtime: runtime, message: message}
}

// Laser is the connection, for everything the helpers here do not cover.
func (agent *AgentCtx) Laser() *Laser { return agent.runtime.laser }

// Message is the message being handled.
func (agent *AgentCtx) Message() *AgentMessage { return agent.message }

// Identity is the agent handling the record.
func (agent *AgentCtx) Identity() wire.AgentID { return agent.runtime.identity }

// RequestAt is where the handled record sits on the log, which a typed reply
// carries as its causal position.
func (agent *AgentCtx) RequestAt() wire.LogPosition {
	return agent.runtime.position(agent.message.Record)
}

// Session is the handled record's session, written as this agent.
//
// It is a lens only: it holds no lease and starts no heartbeat, so opening it
// costs nothing and a handler that only reads pays for nothing more.
func (agent *AgentCtx) Session() *Session {
	session := agent.runtime.sessions.Open(agent.message.Conversation(), "")
	if agent.runtime.identity.IsZero() {
		return session
	}
	return session.As(agent.runtime.identity)
}

// PendingControl is the operator requests the runtime has observed for this
// session, and false while they must be folded off agent.control first.
//
// Nothing here interrupts a running handler. A pause an operator asked for is
// a request to the work, which the handler reads and acts on where it is safe
// to stop, rather than a signal that cuts it off mid-step.
func (agent *AgentCtx) PendingControl() (PendingControl, bool) {
	return agent.runtime.control.Pending(agent.message.Conversation())
}

// Respond answers the handled message on the agent's reply topic.
//
// A typed request gets a typed answer: correlated, addressed back to its
// sender, and naming the request as its cause. A plain request gets a plain
// reply matched by its correlation. Either way the caller's waiter can pair
// the answer with what it asked.
func (agent *AgentCtx) Respond(ctx context.Context, payload []byte) error {
	if agent.runtime.respondOn == "" {
		return &NoRespondTopicError{}
	}
	return agent.respondOn(ctx, agent.runtime.respondOn, payload)
}

// RespondVia answers the handled message on a named topic rather than on the
// agent's configured reply topic.
func (agent *AgentCtx) RespondVia(
	ctx context.Context, topic string, payload []byte,
) error {
	if topic == "" {
		return &NoRespondTopicError{}
	}
	return agent.respondOn(ctx, topic, payload)
}

func (agent *AgentCtx) respondOn(
	ctx context.Context, topic string, payload []byte,
) error {
	envelope := agent.message.Envelope
	if envelope != nil && envelope.Correlation != nil {
		if agent.runtime.identity.IsZero() {
			return &InvalidError{
				Reason: "an agent that answers a typed request needs an identity, " +
					"because the answer names its author"}
		}
		send := agent.runtime.answering(envelope, topic).
			Respond(*envelope.Correlation, payload).
			WithTarget(envelope.Source)
		if envelope.Record != nil {
			at := agent.RequestAt()
			send = send.WithCause(*envelope.Record, &at)
		}
		_, err := send.Send(ctx)
		return err
	}
	return agent.sendReply(ctx, topic, payload)
}

// RespondInput answers the input request being handled, on the topic the
// waiting caller reads.
//
// It is how a handler that was asked for a decision gives one: the answer
// carries the request's own correlation, so it reaches the caller that is
// paused on it rather than any other waiter sharing the topic.
//
// A message that is not a correlated agent message cannot be answered this
// way and is refused, rather than being sent as a plain record that nothing
// correlates and nobody is waiting for.
func (agent *AgentCtx) RespondInput(
	ctx context.Context, replyTopic string, response []byte,
) error {
	if replyTopic == "" {
		return &NoRespondTopicError{}
	}
	envelope := agent.message.Envelope
	if envelope == nil {
		return &InvalidError{Reason: "the handled message carries no agent " +
			"envelope, so there is no input request to answer"}
	}
	if envelope.Correlation == nil {
		return &InvalidError{Reason: "the handled input request carries no " +
			"correlation, so an answer could not be paired with it"}
	}
	if agent.runtime.identity.IsZero() {
		return &InvalidError{Reason: "an agent that answers an input request " +
			"needs an identity, because the answer names its author"}
	}
	return agent.respondOn(ctx, replyTopic, response)
}

// ApprovalGate asks an approver to decide and waits for the answer.
//
// The session is marked as waiting for input, the prompt goes out as an input
// request addressed to the approver, and the decision is the approver's
// correlated reply. The wait is registered before the prompt is sent, because
// the answer cannot exist until it is and a gate that listened afterwards
// could miss one that arrived in between.
//
// Only the named approver decides. A reply on this correlation from anyone
// else is a fault rather than a decision, since the correlation is minted here
// and sent to nobody but the approver, so it is refused rather than waited
// past.
//
// Waiting in place is the whole of this gate: the Rust client can instead park
// the work and replay the handler once the decision lands, which frees the
// consumer for the session's other work. That runtime is not ported, so a gate
// held open holds its handler open.
func (agent *AgentCtx) ApprovalGate(
	ctx context.Context, approver wire.AgentID, prompt []byte, timeout time.Duration,
) (*ApprovalGrant, error) {
	if approver.IsZero() {
		return nil, &InvalidError{Reason: "an approval gate must name its approver"}
	}
	if agent.runtime.identity.IsZero() {
		return nil, &InvalidError{Reason: "an agent that asks for approval needs " +
			"an identity, because the decision is addressed back to it"}
	}
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	conversation := agent.message.Conversation()
	interrupt := wire.CorrelationID{ID: wire.IDFromBytes(NewOperationID())}
	ticket, err := agent.runtime.replies.Subscribe(interrupt, ExpectedReply{
		Session: conversation, Requester: agent.runtime.identity,
	})
	if err != nil {
		return nil, err
	}
	if err := agent.askApproval(ctx, approver, interrupt, prompt, timeout); err != nil {
		// The wait is released here rather than left for the deadline: a
		// prompt that never left should not hold a correlation.
		ticket.Close()
		return nil, err
	}
	reply, err := ticket.Await(ctx, timeout)
	if err != nil {
		return nil, err
	}
	if reply.Envelope.Source != approver {
		return nil, &InvalidError{Reason: "the decision on this approval came " +
			"from " + reply.Envelope.Source.String() + " rather than from " +
			approver.String() + ", so it decides nothing"}
	}
	// A refusal is a decision too, and it is not a grant. Turning one into a
	// grant would let the work the gate exists to hold go ahead on the
	// strength of the approver having said no.
	if reply.Envelope.Kind == wire.KindError {
		return nil, &ApprovalRefusedError{
			Approver: approver, Reason: refusalReason(reply.Envelope.Body)}
	}
	return GrantFrom(approver, conversation, reply)
}

// refusalReason is what an approver said when refusing, and a plain statement
// of the refusal when the body does not say.
func refusalReason(body []byte) string {
	var failure wire.AgentErrorBody
	if err := wire.Unmarshal(body, &failure); err == nil && failure.Message != nil {
		return *failure.Message
	}
	return "the input request was rejected"
}

// ApprovalRefusedError reports an approver that decided against the work.
//
// It is kept apart from a gate that failed or timed out, because the three
// want different handling: a refusal is an answer and must not be retried into
// a second asking, while a timeout may be.
type ApprovalRefusedError struct {
	Approver wire.AgentID
	Reason   string
}

func (e *ApprovalRefusedError) Error() string {
	return "the approval was refused by " + e.Approver.String() + ": " + e.Reason
}

// Retryable reports that asking again will not change the answer.
func (e *ApprovalRefusedError) Retryable() bool { return false }

// askApproval marks the session as waiting and sends the prompt.
//
// The status goes first, so a reader of the session's timeline sees it waiting
// even if the prompt itself does not land.
func (agent *AgentCtx) askApproval(
	ctx context.Context, approver wire.AgentID,
	interrupt wire.CorrelationID, prompt []byte, timeout time.Duration,
) error {
	producer := agent.runtime.laser.AgdxOn(agent.runtime.stream,
		agent.runtime.topic, agent.runtime.identity, agent.message.Conversation())
	if _, err := producer.TaskStatus(
		interrupt, wire.TaskInputRequired).Send(ctx); err != nil {
		return err
	}
	_, err := producer.Command(interrupt, prompt).
		WithOperation(wire.OperationInputRequest).
		WithTarget(approver).
		WithDeadline(time.Now().Add(timeout)).
		Send(ctx)
	return err
}

// ReplyOn sends a reply to the handled message on a named topic, chaining its
// causality and routing it back to the sender.
//
// It is the plain form: no envelope, just the spine. A handler answering a
// typed request wants Respond, which keeps the exchange typed.
func (agent *AgentCtx) ReplyOn(
	ctx context.Context, topic string, payload []byte,
) error {
	return agent.sendReply(ctx, topic, payload)
}

func (agent *AgentCtx) sendReply(
	ctx context.Context, topic string, payload []byte,
) error {
	provenance := agent.ReplyProvenance()
	if source := agent.message.Record.Provenance.Agent; source != nil {
		To(*source).Apply(provenance)
	}
	_, err := agent.runtime.laser.SendAgent(ctx, topic, payload, provenance)
	return err
}

// Send appends a payload to a topic under a provenance the handler built, with
// no causality wired in. A handler that wants the reply chaining uses ReplyOn.
func (agent *AgentCtx) Send(
	ctx context.Context, topic string, payload []byte, provenance *Provenance,
) error {
	_, err := agent.runtime.laser.SendAgent(ctx, topic, payload, provenance)
	return err
}

// Request sends a command from inside a handler and waits for its answer.
//
// The answer is matched by the loop that is already reading this agent's
// topic, so a handler does not need a reader of its own. That only works when
// the reply lands where the runtime reads: a request on another topic is
// refused here rather than left to time out, and such a caller builds its own
// Requester with its own reader.
func (agent *AgentCtx) Request(
	ctx context.Context, topic string, payload []byte, options RequestOptions,
) (*AgdxMessage, error) {
	if agent.runtime.identity.IsZero() {
		return nil, &InvalidError{
			Reason: "an agent that requests needs an identity, because the reply is addressed to it"}
	}
	if topic != agent.runtime.topic {
		return nil, &InvalidError{
			Reason: "a handler's request is answered by the loop reading " +
				agent.runtime.topic + ", so it cannot await a reply on " + topic +
				": open a Requester with its own reader for that"}
	}
	requester := agent.runtime.requester(topic)
	return requester.Request(ctx, agent.message.Conversation(), payload, options)
}

// requester opens a request surface whose waits are served by this runtime's
// own hub, so the loop already reading the topic answers them.
func (runtime *AgentRuntime) requester(topic string) *Requester {
	requester := runtime.laser.RequesterOn(runtime.stream, topic, runtime.identity)
	requester.hub = runtime.replies
	return requester
}

// SpawnSubconversation derives the provenance of a child conversation of the
// handled one, linked by its parent and root so the branch can be traced back
// to the work that spawned it.
func (agent *AgentCtx) SpawnSubconversation() (*Provenance, error) {
	if agent.runtime.identity.IsZero() {
		return nil, &InvalidError{
			Reason: "an agent that spawns a sub-conversation needs an identity, " +
				"because the child names its author"}
	}
	return agent.runtime.laser.SpawnSubconversation(
		agent.message.Record.Provenance, agent.runtime.identity), nil
}

// ReplyProvenance is the spine of a plain reply to the handled message.
//
// The request's correlation is echoed back so the caller's correlator can
// identify this reply unambiguously: without it, a reply matching only on the
// conversation could be taken by the wrong waiter when several agents share a
// reply topic. A request that carried no correlation gets this record's own
// position as one, so the reply names both its cause and a correlation and
// every reader classifies it as a reply rather than as new work.
//
// The request's idempotency key is deliberately not echoed: the reply is its
// own operation, and reusing the key would let a reply suppress one.
func (agent *AgentCtx) ReplyProvenance() *Provenance {
	handled := agent.message.Record.Provenance
	cause := agent.message.ID()
	correlation := cause.String()
	if handled.CorrelationID != nil {
		correlation = *handled.CorrelationID
	}
	provenance := &Provenance{
		Conversation:       handled.Conversation,
		CausalParent:       &cause,
		ParentConversation: handled.ParentConversation,
		RootConversation:   handled.RootConversation,
		CorrelationID:      &correlation,
	}
	if !agent.runtime.identity.IsZero() {
		author := agent.runtime.identity
		provenance.Agent = &author
	}
	return provenance
}

// NoRespondTopicError reports a handler that tried to answer without a reply
// topic to answer on.
//
// The topic is named when the agent is built rather than guessed here,
// because where an answer belongs is a deployment's routing decision and a
// guess would send it somewhere nobody reads.
type NoRespondTopicError struct{}

func (e *NoRespondTopicError) Error() string {
	return "this agent was built without a reply topic, so it cannot respond: " +
		"set RuntimeOptions.RespondOn"
}

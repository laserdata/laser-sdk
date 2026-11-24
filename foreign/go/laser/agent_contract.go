package laser

import (
	"context"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// DefaultContractDeadline bounds a contract whose caller names none.
const DefaultContractDeadline = 30 * time.Second

// contractWaitReader is the consumer name a contract's reply wait reads
// under. It stores no offset: the wait is a bounded live poll, not a
// subscription, so concurrent contracts never fight over one group's cursor.
const contractWaitReader = "laser-contract-wait"

// contractClockSlack covers clock skew between this client and the server:
// the poll starts a little before the command was sent rather than exactly
// at it, so a reply the server stamped a moment earlier is still seen.
const contractClockSlack = 2 * time.Second

// contractWaitInterval is how long a round with nothing new sleeps before
// trying again.
const contractWaitInterval = 100 * time.Millisecond

// contractWaitBatch bounds one poll.
const contractWaitBatch = 100

// ContractState is the terminal state a Contract settles in.
type ContractState int

const (
	// ContractCompleted is a non-error reply within the deadline.
	ContractCompleted ContractState = iota
	// ContractFailed is a terminal error reply within the deadline.
	ContractFailed
	// ContractNotConsumed is the target answering that its consumer read the
	// command past its own expiry and dropped it before any handler ran.
	// Reported only on that answer, which a target whose RuntimeOptions name
	// a RespondOn topic sends, never from a missing pickup acknowledgment.
	ContractNotConsumed
	// ContractTimedOut is consumed (or no expiry was set) but no terminal
	// reply landed within the deadline.
	ContractTimedOut
)

// String names the state the way a log line or an error message wants it.
func (state ContractState) String() string {
	switch state {
	case ContractCompleted:
		return "completed"
	case ContractFailed:
		return "failed"
	case ContractNotConsumed:
		return "not consumed"
	default:
		return "timed out"
	}
}

// Contract is the outcome of a ContractBuilder.Send: the terminal state a
// directed request to one agent settled in, and the reply that settled it,
// set for Completed and Failed.
type Contract struct {
	State ContractState
	Reply *AgdxMessage
}

// ContractBuilder is a directed request to one agent, bundling resolution, a
// deadline, and an optional consumption expiry into one call: hand a task to
// one agent (or one capability) and learn the reply, whether it was picked
// up, or that it did not finish in time, without hand-rolling correlation ids
// and timers.
//
// Built by Laser.Contract, then Send. It is directed messaging with
// confirmation over the log, never a bespoke consensus protocol: a thin
// client-side wait over a live poll of the reply topic and two durations.
//
// Unlike Requester, Send drives its own wait rather than requiring a caller
// to already be reading the reply topic and feeding a ReplyHub: a contract is
// meant to be usable standalone, by an orchestrator with no running consumer
// of its own. It does not resolve a separate per-agent inbox topic the way
// Rust's InboxRoute does: Go's agent registry carries no such address, so the
// command is published and the reply awaited on the one topic named at
// Laser.Contract, the same single-lane shape Requester and the MCP bridge's
// tool-call wait already use.
type ContractBuilder struct {
	laser        *Laser
	router       Router
	topic        string
	registry     *AgentRegistry
	from         wire.AgentID
	payload      []byte
	expiry       time.Duration
	deadline     time.Duration
	fence        *uint64
	conversation *wire.ConversationID
	parent, root *wire.ConversationID
	correlation  *wire.CorrelationID
	idempotency  *wire.IdempotencyKey
	resumeFrom   []uint64
}

// Contract opens a ContractBuilder addressed by router (one agent via To, or
// one capability via ToCapable/Sticky/WithScorer), publishing the command and
// awaiting its reply on topic. A broadcast or all-capable route is refused at
// send time, since a contract is directed to exactly one target.
func (laser *Laser) Contract(router Router, topic string) *ContractBuilder {
	return &ContractBuilder{
		laser: laser, router: router, topic: topic,
		deadline: DefaultContractDeadline,
	}
}

// From names the agent the contract sends as. Required: the command is an
// AGDX command, which carries a source.
func (builder *ContractBuilder) From(source wire.AgentID) *ContractBuilder {
	derived := *builder
	derived.from = source
	return &derived
}

// Payload is the task body sent to the target.
func (builder *ContractBuilder) Payload(payload []byte) *ContractBuilder {
	derived := *builder
	derived.payload = payload
	return &derived
}

// Registry is where a capability route (ToCapable, Sticky, WithScorer) is
// resolved against. Unneeded for a route that names an agent directly (To).
func (builder *ContractBuilder) Registry(registry *AgentRegistry) *ContractBuilder {
	derived := *builder
	derived.registry = registry
	return &derived
}

// ExpireIfNotConsumed drops the command if it is not consumed within expiry:
// rides as the command's deadline when it is earlier than Deadline, so the
// target's reliable consumer drops an expired command before the handler
// runs. Reported as ContractNotConsumed when the target answers that it
// dropped the command.
func (builder *ContractBuilder) ExpireIfNotConsumed(expiry time.Duration) *ContractBuilder {
	derived := *builder
	derived.expiry = expiry
	return &derived
}

// Deadline is how long to await a terminal reply before returning
// ContractTimedOut (default 30s). The command carries it as its own
// deadline too, so a target that has not picked the command up by then
// drops it instead of working on a task its caller gave up on.
func (builder *ContractBuilder) Deadline(deadline time.Duration) *ContractBuilder {
	derived := *builder
	derived.deadline = deadline
	return &derived
}

// Conversation pins the conversation the command rides, instead of a fresh
// one per send. Fence is only meaningful with a pinned, stable conversation:
// a fenced task that may be re-dispatched (a reassignment, a retry) must use
// the same conversation on every attempt.
func (builder *ContractBuilder) Conversation(conversation wire.ConversationID) *ContractBuilder {
	derived := *builder
	derived.conversation = &conversation
	return &derived
}

// Fence stamps a fence token (agdx.fence) on the command, from a lease grant.
// A consumer drops a command whose fence is behind the highest it has
// accepted for the task, so a stale holder cannot act after a reassignment.
func (builder *ContractBuilder) Fence(fence uint64) *ContractBuilder {
	derived := *builder
	derived.fence = &fence
	return &derived
}

// Parent runs the contract as a child session of parent, in the tree rooted
// at root (the parent itself when it has no parent of its own). Send writes
// the child's submitted start on agent.sessions before the command, stamps
// the ancestry on the command, and ends the child by the outcome: completed
// on a reply, failed otherwise, including a command that failed to publish.
func (builder *ContractBuilder) Parent(parent, root wire.ConversationID) *ContractBuilder {
	derived := *builder
	derived.parent, derived.root = &parent, &root
	return &derived
}

// Correlation pins the correlation a resend of the same logical request
// carries, instead of minting one, so a reply to an earlier send still
// answers this one. Unexported callers (the workflow engine) only: ordinary
// use mints a fresh correlation per send, which is what keeps two contracts
// on the same conversation from answering each other.
func (builder *ContractBuilder) Correlation(correlation wire.CorrelationID) *ContractBuilder {
	derived := *builder
	derived.correlation = &correlation
	return &derived
}

// IdempotencyKey stamps the command's business idempotency key, so a
// target's duplicate suppression drops a repeat of a send it already
// handled.
func (builder *ContractBuilder) IdempotencyKey(key wire.IdempotencyKey) *ContractBuilder {
	derived := *builder
	derived.idempotency = &key
	return &derived
}

// ResumeFrom looks for a terminal already landed at or after positions (one
// offset per reply-topic partition, as Laser.ReplyPositions reports) before
// sending anything. A terminal found there settles the contract without a
// new send; finding none still sends the command (idempotently, when
// IdempotencyKey is also set) and waits fresh, since a resumed caller cannot
// tell "never sent" from "sent, no reply yet" without trying again.
func (builder *ContractBuilder) ResumeFrom(positions []uint64) *ContractBuilder {
	derived := *builder
	derived.resumeFrom = positions
	return &derived
}

// Send resolves the target, sends the command, and awaits the contract's
// terminal state.
func (builder *ContractBuilder) Send(ctx context.Context) (*Contract, error) {
	_, contract, err := builder.SendResolved(ctx)
	return contract, err
}

// SendResolved is Send, also naming the agent the route resolved.
func (builder *ContractBuilder) SendResolved(ctx context.Context) (wire.AgentID, *Contract, error) {
	if builder.from.IsZero() {
		return wire.AgentID{}, nil, &InvalidError{Reason: "a contract requires From(source agent id)"}
	}
	target, err := builder.resolve()
	if err != nil {
		return wire.AgentID{}, nil, err
	}

	conversation := NewConversationID()
	if builder.conversation != nil {
		conversation = *builder.conversation
	}

	var child *Session
	if builder.parent != nil {
		// The child's start lands before its command, so a reader never sees
		// work for a session that does not exist yet.
		child, err = builder.laser.Sessions().OpenChild(
			ctx, conversation, builder.from, target, *builder.parent, *builder.root)
		if err != nil {
			return wire.AgentID{}, nil, err
		}
	}

	outcome, sendErr := builder.sendAndAwait(ctx, conversation, target)

	// End the child by the outcome before returning. A lifecycle record that
	// fails to publish surfaces as the error, except when the contract itself
	// already erred.
	if child != nil {
		var endErr error
		switch {
		case sendErr != nil:
			_, endErr = child.Fail(ctx, contractFailure(sendErr.Error()))
		case outcome.State == ContractCompleted:
			_, endErr = child.Complete(ctx, "")
		default:
			_, endErr = child.Fail(ctx, contractFailure(contractFailureReason(outcome.State)))
		}
		if sendErr == nil && endErr != nil {
			return wire.AgentID{}, nil, endErr
		}
	}
	if sendErr != nil {
		return wire.AgentID{}, nil, sendErr
	}
	return target, outcome, nil
}

// resolve names the one target this contract is directed to.
func (builder *ContractBuilder) resolve() (wire.AgentID, error) {
	if builder.router.Kind == RouteBroadcast || builder.router.Kind == RouteAllCapable {
		return wire.AgentID{}, &InvalidError{Reason: "a contract is directed to " +
			"one agent, not a broadcast or all-capable route"}
	}
	targets, err := builder.router.ResolveTargets(builder.registry, time.Now())
	if err != nil {
		return wire.AgentID{}, err
	}
	if len(targets) == 0 {
		return wire.AgentID{}, &InvalidError{Reason: "the contract route resolved no target"}
	}
	return targets[0], nil
}

// sendAndAwait publishes the command and awaits its correlated terminal.
//
// When ResumeFrom named positions, a terminal already landed there settles
// the contract without sending anything; only finding none there falls
// through to an (idempotent, when IdempotencyKey is also set) send.
func (builder *ContractBuilder) sendAndAwait(
	ctx context.Context, conversation wire.ConversationID, target wire.AgentID,
) (*Contract, error) {
	deadline := builder.deadline
	if deadline <= 0 {
		deadline = DefaultContractDeadline
	}
	lapse := deadline
	if builder.expiry > 0 && builder.expiry < deadline {
		lapse = builder.expiry
	}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(NewOperationID())}
	if builder.correlation != nil {
		correlation = *builder.correlation
	}
	expected := ExpectedReply{Session: conversation, Requester: builder.from}

	if builder.resumeFrom != nil {
		landed, err := builder.laser.awaitContractReply(
			ctx, builder.laser.defaultStream, builder.topic, correlation,
			builder.resumeFrom, expected, time.Now())
		if err != nil {
			return nil, err
		}
		if landed != nil {
			return classifyContract(landed), nil
		}
	}

	producer := builder.laser.AgdxOn(
		builder.laser.defaultStream, builder.topic, builder.from, conversation)
	if builder.parent != nil {
		producer = producer.WithAncestry(*builder.parent, *builder.root)
	}
	send := producer.Command(correlation, builder.payload).
		WithTarget(target).
		WithDeadline(time.Now().Add(lapse))
	if builder.fence != nil {
		send = send.WithMetadata(wire.HeaderFence, wire.UintValue(*builder.fence))
	}
	if builder.parent != nil {
		send = send.WithMetadata(wire.MetadataSubmitted, wire.BoolValue(true))
	}
	if builder.idempotency != nil {
		send = send.WithIdempotencyKey(*builder.idempotency)
	}
	receipt, err := send.Send(ctx)
	if err != nil {
		return nil, err
	}

	expected.Command = &receipt.Record
	deadlineAt := time.Now().Add(deadline)
	message, err := builder.laser.awaitContractReply(
		ctx, builder.laser.defaultStream, builder.topic, correlation, nil, expected, deadlineAt)
	if err != nil {
		return nil, err
	}
	if message == nil {
		return &Contract{State: ContractTimedOut}, nil
	}
	return classifyContract(message), nil
}

// classifyContract reads a terminal reply into the state it settles the
// contract in.
func classifyContract(message *AgdxMessage) *Contract {
	if message.Envelope.Kind == wire.KindError {
		if droppedUnconsumed(&message.Envelope) {
			return &Contract{State: ContractNotConsumed, Reply: message}
		}
		return &Contract{State: ContractFailed, Reply: message}
	}
	return &Contract{State: ContractCompleted, Reply: message}
}

// droppedUnconsumed reports a reply naming the dead-letter reason a target's
// reliable consumer stamps when it reads a command past its own deadline and
// drops it before any handler ran, as opposed to a handler's own failure.
func droppedUnconsumed(envelope *wire.AgentEnvelope) bool {
	if envelope.Kind != wire.KindError {
		return false
	}
	var body wire.AgentErrorBody
	if err := wire.Unmarshal(envelope.Body, &body); err != nil {
		return false
	}
	value, named := body.Detail[DeadLetterReasonDetail]
	if !named {
		return false
	}
	switch value.Kind {
	case wire.ValueUint:
		return wire.DeadLetterReason(value.Uint) == wire.DeadLetterDeadlineExceeded
	case wire.ValueInt:
		return value.Int >= 0 &&
			wire.DeadLetterReason(value.Int) == wire.DeadLetterDeadlineExceeded
	default:
		return false
	}
}

// contractFailure is the structured error a child session ends with when a
// contract cannot be completed.
func contractFailure(message string) wire.AgentErrorBody {
	return wire.AgentErrorBody{Code: wire.ErrorInternal, Message: &message, Retryable: false}
}

// contractFailureReason explains a non-completing terminal for the child
// session's own failure record.
func contractFailureReason(state ContractState) string {
	switch state {
	case ContractFailed:
		return "the target replied with a terminal error"
	case ContractNotConsumed:
		return "the command was not consumed within the expiry"
	default:
		return "no terminal reply landed within the deadline"
	}
}

// awaitContractReply polls topic for the correlated answer a contract's
// command expects, until deadline.
//
// startAt, when set, seeds each partition at its own saved offset instead of
// the tail-minus-slack timestamp, for a resumed contract's catch-up scan
// (ResumeFrom): a deadline of now makes this one bounded sweep over what is
// already there rather than an open-ended wait.
//
// There is no durable subscription behind a contract's answer: like the MCP
// bridge's tool-call wait, this reads forward live, which is what keeps a
// standalone directed request cheap on a topic that otherwise holds unrelated
// traffic, and usable without a caller already running a consumer loop on it.
func (laser *Laser) awaitContractReply(
	ctx context.Context, stream, topic string, correlation wire.CorrelationID,
	startAt []uint64, expected ExpectedReply, deadline time.Time,
) (*AgdxMessage, error) {
	client := laser.Client()
	if client == nil {
		return nil, &InvalidError{Reason: "a contract needs a connected client"}
	}
	streamID, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return nil, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(topic)
	if err != nil {
		return nil, &InvalidError{Reason: "topic name: " + err.Error()}
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil {
		return nil, &InvalidError{Reason: "read the contract's reply topic: " + err.Error()}
	}
	if details == nil {
		return nil, &InvalidError{Reason: "the contract's reply topic is not there"}
	}
	reader, err := iggcon.NewIdentifier(contractWaitReader)
	if err != nil {
		return nil, &InvalidError{Reason: "reader name: " + err.Error()}
	}
	consumer := iggcon.NewSingleConsumer(reader)
	cursors := make([]iggcon.PollingStrategy, details.PartitionsCount)
	since := time.Now().Add(-contractClockSlack)
	for partition := range cursors {
		switch {
		case uint32(len(startAt)) > uint32(partition):
			cursors[partition] = iggcon.OffsetPollingStrategy(startAt[partition])
		default:
			cursors[partition] = iggcon.TimestampPollingStrategy(uint64(since.UnixMicro()))
		}
	}
	options := iggcon.DefaultPollOptions()
	options.Count = contractWaitBatch

	for {
		for partition := uint32(0); partition < details.PartitionsCount; partition++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			polled, err := client.PollMessages(
				ctx, streamID, topicID, partition, consumer, cursors[partition], options)
			if err != nil {
				return nil, &InvalidError{Reason: "read the contract's reply: " + err.Error()}
			}
			if polled == nil || len(polled.Messages) == 0 {
				continue
			}
			for index := range polled.Messages {
				record, err := DeliveredRecord(partition, polled.Messages[index], iggcon.Checkpoint{}, false)
				if err != nil {
					continue
				}
				message, err := OpenAgdx(record)
				if err != nil {
					continue
				}
				// The correlation is the exchange's identity; Accepts alone
				// checks only the session, kind, cause and addressee, which a
				// reply on an unrelated correlation of the same conversation
				// can still satisfy.
				if message.Envelope.Correlation == nil ||
					*message.Envelope.Correlation != correlation {
					continue
				}
				if expected.Accepts(message) {
					return message, nil
				}
			}
			last := polled.Messages[len(polled.Messages)-1]
			cursors[partition] = iggcon.OffsetPollingStrategy(last.Header.Offset + 1)
		}
		if !time.Now().Before(deadline) {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(contractWaitInterval):
		}
	}
}

// ReplyPositions is the next offset of every partition of topic on the
// handle's default stream, the point a reply to a request sent now lands at
// or after. A caller journals this before a dispatch it may need to resume,
// so a later attempt's ResumeFrom replays only what arrived since rather
// than rescanning the topic from the start.
func (laser *Laser) ReplyPositions(ctx context.Context, topic string) ([]uint64, error) {
	client := laser.Client()
	if client == nil {
		return nil, &InvalidError{Reason: "reply positions need a connected client"}
	}
	streamID, err := iggcon.NewIdentifier(laser.defaultStream)
	if err != nil {
		return nil, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(topic)
	if err != nil {
		return nil, &InvalidError{Reason: "topic name: " + err.Error()}
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil {
		return nil, &InvalidError{Reason: "read the reply topic: " + err.Error()}
	}
	if details == nil {
		// Not there yet: every partition starts at zero, so a scan still
		// finds the reply once the topic exists.
		return nil, nil
	}
	reader, err := iggcon.NewIdentifier(contractWaitReader)
	if err != nil {
		return nil, &InvalidError{Reason: "reader name: " + err.Error()}
	}
	consumer := iggcon.NewSingleConsumer(reader)
	options := iggcon.DefaultPollOptions()
	options.Count = 1
	positions := make([]uint64, details.PartitionsCount)
	for partition := range positions {
		polled, err := client.PollMessages(
			ctx, streamID, topicID, uint32(partition), consumer,
			iggcon.LastPollingStrategy(), options)
		if err != nil {
			return nil, &InvalidError{Reason: "read the reply topic's tail: " + err.Error()}
		}
		if polled != nil && len(polled.Messages) > 0 {
			last := polled.Messages[len(polled.Messages)-1]
			positions[partition] = last.Header.Offset + 1
		}
	}
	return positions, nil
}

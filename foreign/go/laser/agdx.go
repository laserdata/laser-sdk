package laser

import (
	"context"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Agdx sends AGDX agent messages on one conversation.
//
// Every record rides the session topic and carries both the envelope and the
// provenance headers, so a reader can route and audit without decoding the
// body. The conversation is the partition key, which is what keeps one
// exchange ordered.
type Agdx struct {
	laser        *Laser
	stream       string
	topic        string
	source       wire.AgentID
	conversation wire.ConversationID
	parent       *wire.ConversationID
	root         *wire.ConversationID
}

// Agdx opens the agent messaging surface for one conversation, authored by one
// agent.
//
// The records ride agent.sessions on the handle's default stream unless the
// caller names another through AgdxOn.
func (laser *Laser) Agdx(source wire.AgentID, conversation wire.ConversationID) *Agdx {
	return laser.AgdxOn(laser.defaultStream, wire.AgentSessions, source, conversation)
}

// AgdxOn is Agdx on a named stream and topic, for a deployment that routes
// agent traffic somewhere other than the default session topic.
func (laser *Laser) AgdxOn(
	stream, topic string, source wire.AgentID, conversation wire.ConversationID,
) *Agdx {
	return &Agdx{
		laser:        laser,
		stream:       stream,
		topic:        topic,
		source:       source,
		conversation: conversation,
	}
}

// WithAncestry places this conversation in its tree, so a reply can be traced
// back to the exchange that spawned it.
func (agdx *Agdx) WithAncestry(parent, root wire.ConversationID) *Agdx {
	copied := *agdx
	copied.parent, copied.root = &parent, &root
	return &copied
}

// Conversation is the conversation these messages belong to.
func (agdx *Agdx) Conversation() wire.ConversationID { return agdx.conversation }

// Source is the agent authoring these messages.
func (agdx *Agdx) Source() wire.AgentID { return agdx.source }

// Command asks another agent to do something, under a correlation the reply
// will carry back.
func (agdx *Agdx) Command(correlation wire.CorrelationID, body []byte) *AgdxSend {
	return agdx.send(wire.KindCommand).WithCorrelation(correlation).Body(body)
}

// Respond answers a command, carrying the correlation the command minted so
// the caller can pair them.
func (agdx *Agdx) Respond(correlation wire.CorrelationID, body []byte) *AgdxSend {
	return agdx.send(wire.KindResponse).WithCorrelation(correlation).Body(body)
}

// Emit announces something that expects no answer.
func (agdx *Agdx) Emit(body []byte) *AgdxSend {
	return agdx.send(wire.KindEvent).Body(body)
}

// TaskStatus reports where one task stands.
//
// A task status needs both the correlation it belongs to and the state it
// reports, so both are parameters rather than chained steps: a signature that
// cannot express the requirement turns a contract rule into a runtime failure.
func (agdx *Agdx) TaskStatus(
	correlation wire.CorrelationID, state wire.TaskState,
) *AgdxSend {
	return agdx.send(wire.KindStatus).
		WithCorrelation(correlation).
		WithOperation(wire.OperationTask).
		WithTaskState(state)
}

// SessionStarted opens a session, carrying what it was started with.
//
// A session status belongs to the session rather than to one exchange, so it
// names no correlation. Its body is a typed lifecycle record whose shape the
// contract ties to the state, which is why each transition has its own
// constructor: a caller cannot pair a state with a body that does not belong
// to it.
func (agdx *Agdx) SessionStarted(start wire.SessionStart) *AgdxSend {
	return agdx.sessionStatus(wire.TaskSubmitted, start)
}

// SessionProgressed reports a session moving along, carrying what changed.
func (agdx *Agdx) SessionProgressed(transition wire.SessionTransition) *AgdxSend {
	return agdx.sessionStatus(wire.TaskWorking, transition)
}

// SessionEnded closes a session in a terminal state, carrying how it ended.
//
// A state that is not terminal is refused here rather than at the server,
// because the contract requires the terminal flag and the state to agree and
// this is the call that sets the flag.
func (agdx *Agdx) SessionEnded(state wire.TaskState, end wire.SessionEnd) *AgdxSend {
	if !state.IsTerminal() {
		return agdx.send(wire.KindStatus).fail(&InvalidError{
			Reason: "a session ends in a terminal state, and " +
				string(state) + " is not one"})
	}
	return agdx.sessionStatus(state, end)
}

// sessionStatus stamps the one rule every session status shares: the terminal
// flag is derived from the state rather than taken from the caller, which is
// the only way the two cannot drift apart.
func (agdx *Agdx) sessionStatus(state wire.TaskState, body any) *AgdxSend {
	send := agdx.send(wire.KindStatus).
		WithOperation(wire.OperationSession).
		WithTaskState(state)
	encoded, err := wire.Marshal(body)
	if err != nil {
		return send.fail(&CodecError{Reason: "encode the session record", Cause: err})
	}
	send.envelope.Body = encoded
	send.envelope.Last = state.IsTerminal()
	send.contentType = wire.ContentCBOR
	return send
}

// Status reports a lifecycle signal that is neither a task nor a session: a
// card, progress, a quarantine or an unquarantine.
//
// Those two have their own constructors, because the contract requires a state
// on them and this shape cannot carry one. The operation is a closed
// vocabulary, which the contract checks before the record leaves.
func (agdx *Agdx) Status(operation string) *AgdxSend {
	return agdx.send(wire.KindStatus).WithOperation(operation)
}

// Fail reports a terminal failure, carrying the structured error body a reader
// classifies on rather than a message it would have to parse.
func (agdx *Agdx) Fail(correlation wire.CorrelationID, failure wire.AgentErrorBody) *AgdxSend {
	send := agdx.send(wire.KindError).WithCorrelation(correlation)
	encoded, err := wire.Marshal(failure)
	if err != nil {
		return send.fail(&CodecError{Reason: "encode the failure", Cause: err})
	}
	return send.Body(encoded).ContentType(wire.ContentCBOR)
}

// Chunk is one piece of a stream, ordered by its sequence within a channel.
//
// A chunk is identified by its channel and sequence rather than by a record
// id, which is why it is the one kind that carries none.
func (agdx *Agdx) Chunk(
	correlation wire.CorrelationID, channel wire.ChannelID, sequence uint64, body []byte,
) *AgdxSend {
	send := agdx.send(wire.KindChunk).WithCorrelation(correlation).Body(body)
	send.envelope.Record = nil
	send.envelope.Channel = &channel
	send.envelope.Sequence = &sequence
	return send
}

func (agdx *Agdx) send(kind wire.AgentKind) *AgdxSend {
	record := wire.RecordID{ID: wire.IDFromBytes(NewOperationID())}
	return &AgdxSend{
		agdx: agdx,
		envelope: wire.AgentEnvelope{
			Kind:         kind,
			Record:       &record,
			Conversation: agdx.conversation,
			Parent:       agdx.parent,
			Root:         agdx.root,
			Source:       agdx.source,
		},
		contentType: wire.ContentJSON,
	}
}

// AgdxSend is an unsent agent message. Nothing reaches the log until Send.
type AgdxSend struct {
	agdx        *Agdx
	envelope    wire.AgentEnvelope
	contentType wire.ContentType
	usage       *LlmUsage
	deadlineAt  *time.Time
	broadcast   bool
	err         error
}

func (send *AgdxSend) fail(err error) *AgdxSend {
	if send.err == nil {
		send.err = err
	}
	return send
}

// Body sets the content.
func (send *AgdxSend) Body(body []byte) *AgdxSend {
	send.envelope.Body = body
	return send
}

// JSON encodes a value as the body and stamps the JSON content type.
func (send *AgdxSend) JSON(value any) *AgdxSend {
	encoded, err := JSONCodec{}.Encode(value)
	if err != nil {
		return send.fail(&CodecError{Reason: "encode the message body", Cause: err})
	}
	return send.Body(encoded).ContentType(wire.ContentJSON)
}

// ContentType names how the body is encoded.
func (send *AgdxSend) ContentType(contentType wire.ContentType) *AgdxSend {
	send.contentType = contentType
	return send
}

// WithTarget addresses one agent.
//
// The topic is the primary address and the only confidentiality boundary, so
// this refines routing within a shared topic rather than hiding the record
// from anyone else reading it.
func (send *AgdxSend) WithTarget(target wire.AgentID) *AgdxSend {
	send.envelope.Target = &target
	return send
}

// Broadcast addresses every agent, which every record on a shared session
// topic must do when it names no one target.
func (send *AgdxSend) Broadcast() *AgdxSend {
	send.broadcast = true
	return send
}

// WithCause names the record this one answers, and optionally where it sat.
//
// The record id is stable across replication while the position is
// deployment-local, which is why both are carried rather than one.
func (send *AgdxSend) WithCause(cause wire.RecordID, at *wire.LogPosition) *AgdxSend {
	send.envelope.Cause = &cause
	send.envelope.CauseAt = at
	return send
}

// WithCorrelation pairs this message with the exchange it belongs to.
func (send *AgdxSend) WithCorrelation(correlation wire.CorrelationID) *AgdxSend {
	send.envelope.Correlation = &correlation
	return send
}

// WithIdempotencyKey suppresses a duplicate of the same logical operation.
func (send *AgdxSend) WithIdempotencyKey(key wire.IdempotencyKey) *AgdxSend {
	send.envelope.IdempotencyKey = &key
	return send
}

// WithDeadline sets the drop-dead time. A consumer past it dead-letters the
// message rather than working on something nobody is waiting for.
func (send *AgdxSend) WithDeadline(deadline time.Time) *AgdxSend {
	micros := uint64(deadline.UnixMicro())
	send.envelope.DeadlineMicros = &micros
	send.deadlineAt = &deadline
	return send
}

// WithTaskState reports where a task stands.
func (send *AgdxSend) WithTaskState(state wire.TaskState) *AgdxSend {
	send.envelope.TaskState = &state
	return send
}

// WithOperation names the operation, which a status message is discriminated
// by.
func (send *AgdxSend) WithOperation(operation string) *AgdxSend {
	send.envelope.Operation = &operation
	return send
}

// WithFinishReason names why generation stopped, in the OTel vocabulary.
func (send *AgdxSend) WithFinishReason(reason string) *AgdxSend {
	send.envelope.FinishReason = &reason
	return send
}

// WithTool names the tool a command or result belongs to.
func (send *AgdxSend) WithTool(tool string) *AgdxSend {
	send.envelope.Tool = &tool
	return send
}

// WithUsage carries the token and cost usage of this step, both in the
// envelope and on the headers, so a rollup can read it without decoding
// bodies.
func (send *AgdxSend) WithUsage(usage wire.TokenUsage) *AgdxSend {
	send.envelope.Usage = &usage
	input, output := usage.InputTokens, usage.OutputTokens
	send.usage = &LlmUsage{InputTokens: &input, OutputTokens: &output}
	// Cost rides the envelope in integer micro-units of the deployment's
	// currency, and the header in whole currency, so it is converted rather
	// than copied: the two spell the same amount differently on purpose.
	if usage.CostMicros != nil {
		cost := float64(*usage.CostMicros) / 1_000_000
		send.usage.CostUSD = &cost
	}
	return send
}

// WithMetadata adds one scalar extension value. It is AGDX-native context, not
// a place to carry another protocol's metadata.
func (send *AgdxSend) WithMetadata(key string, value wire.Value) *AgdxSend {
	if send.envelope.Metadata == nil {
		send.envelope.Metadata = map[string]wire.Value{}
	}
	send.envelope.Metadata[key] = value
	return send
}

// Last marks the terminal message of a stream. Only true has protocol meaning,
// so there is no way to set it back.
func (send *AgdxSend) Last() *AgdxSend {
	send.envelope.Last = true
	return send
}

// Envelope renders the message as it will be sent, without sending it.
func (send *AgdxSend) Envelope() wire.AgentEnvelope { return send.envelope }

// AgdxReceipt is where a sent message landed, so a caller can name it as the
// cause of what comes next.
type AgdxReceipt struct {
	Record   wire.RecordID
	Envelope wire.AgentEnvelope
	Response *iggcon.SendMessagesResponse
}

// Send appends the message and returns where it landed.
func (send *AgdxSend) Send(ctx context.Context) (*AgdxReceipt, error) {
	if send.err != nil {
		return nil, send.err
	}
	if send.agdx.stream == "" {
		return nil, errNoStream()
	}
	if err := send.envelope.Validate(); err != nil {
		return nil, &InvalidError{Reason: err.Error()}
	}
	// The envelope stamps carry the version and content type, and the
	// provenance carries the spine. Both ride the same block, so a reader can
	// route and audit without decoding the body.
	block := wire.HeadersForEnvelope(&send.envelope, send.contentType, send.broadcast)
	headers, err := EncodeHeaderBlock(block)
	if err != nil {
		return nil, err
	}
	spine, err := send.provenance().Headers()
	if err != nil {
		return nil, err
	}
	headers = mergeHeaders(headers, spine)

	// The whole envelope is the body, not just its own body field: a reader
	// needs the routing and causality to decide anything at all.
	payload, err := wire.Marshal(send.envelope)
	if err != nil {
		return nil, &CodecError{Reason: "encode the agent envelope", Cause: err}
	}
	topic := send.agdx.laser.Stream(send.agdx.stream).Topic(send.agdx.topic)
	response, err := topic.Send(ctx, payload, headers,
		send.envelope.Conversation.String())
	if err != nil {
		return nil, err
	}
	receipt := &AgdxReceipt{Envelope: send.envelope, Response: response}
	if send.envelope.Record != nil {
		receipt.Record = *send.envelope.Record
	}
	return receipt, nil
}

// provenance is the spine this message carries, derived from the envelope so
// the two cannot disagree about who sent what to whom.
func (send *AgdxSend) provenance() *Provenance {
	provenance := &Provenance{
		Conversation:       send.envelope.Conversation,
		ParentConversation: send.envelope.Parent,
		RootConversation:   send.envelope.Root,
		Agent:              &send.envelope.Source,
		TargetAgent:        send.envelope.Target,
		CausalParent:       send.envelope.Cause,
		DeadlineAt:         send.deadlineAt,
		Usage:              send.usage,
	}
	if send.envelope.Correlation != nil {
		correlation := send.envelope.Correlation.String()
		provenance.CorrelationID = &correlation
	}
	if send.envelope.IdempotencyKey != nil {
		key := send.envelope.IdempotencyKey.String()
		provenance.IdempotencyKey = &key
	}
	return provenance
}

// mergeHeaders folds the spine into the envelope block, letting the envelope's
// own stamps win.
//
// The two blocks overlap on the conversation and the agent ids, and the
// envelope is the authority there: it is what a reader validates against.
func mergeHeaders(block, spine []iggcon.HeaderEntry) []iggcon.HeaderEntry {
	present := make(map[string]bool, len(block))
	for _, entry := range block {
		present[string(entry.Key.Value)] = true
	}
	for _, entry := range spine {
		if present[string(entry.Key.Value)] {
			continue
		}
		block = append(block, entry)
	}
	return block
}

// AgdxMessage is a received agent message: the envelope, the spine, and the
// record it arrived on.
type AgdxMessage struct {
	Envelope wire.AgentEnvelope
	Record   *Record
}

// OpenAgdx reads an agent envelope off a record.
//
// A record that is not an agent message is reported rather than decoded into
// an empty envelope, because an empty envelope would route as a command from
// nobody.
func OpenAgdx(record *Record) (*AgdxMessage, error) {
	var envelope wire.AgentEnvelope
	if err := wire.Unmarshal(record.Payload(), &envelope); err != nil {
		return nil, &CodecError{Reason: "decode the agent envelope", Cause: err}
	}
	if err := envelope.Validate(); err != nil {
		return nil, &InvalidError{Reason: "agent envelope: " + err.Error()}
	}
	return &AgdxMessage{Envelope: envelope, Record: record}, nil
}

// Understood reports whether this build can safely process the message.
//
// A message carrying a must-understand bit the receiver does not implement is
// rejected or dead-lettered rather than mis-handled, which is the whole point
// of the bitset.
func (message *AgdxMessage) Understood(understood uint64) bool {
	return message.Envelope.UnmetRequirements(understood) == 0
}

// AddressedTo reports whether this message is for an agent: one addressed to
// it, or one addressed to nobody in particular.
func (message *AgdxMessage) AddressedTo(agent wire.AgentID) bool {
	if message.Envelope.Target == nil {
		return true
	}
	return *message.Envelope.Target == agent
}

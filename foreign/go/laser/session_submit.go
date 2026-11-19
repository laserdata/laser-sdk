package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// SessionEndedError reports a session that cannot be resumed.
//
// A canceled session is a decision somebody made, so resuming it under the
// same label would quietly undo that decision.
type SessionEndedError struct {
	Session wire.ConversationID
	Status  wire.SessionStatus
}

func (e *SessionEndedError) Error() string {
	return "the session " + e.Session.String() + " ended " + string(e.Status)
}

// Submitted is what a submission wrote: the session and the command's
// correlation.
type Submitted struct {
	Session     wire.ConversationID
	Correlation wire.CorrelationID
}

// SubmitBuilder hands a new session to an agent with a first command.
type SubmitBuilder struct {
	sessions  *Sessions
	agent     wire.AgentID
	input     []byte
	submitter wire.AgentID
	operation string
	label     string
	namespace string
	budget    *wire.Budget
	tags      []string
	// fresh is the id of an unlabeled submission, minted once so ID names
	// the session Send writes rather than a different one each time it is
	// asked.
	fresh wire.ConversationID
}

// Submit hands a new session to agent with input as its first command.
func (sessions *Sessions) Submit(agent wire.AgentID, input []byte) *SubmitBuilder {
	return &SubmitBuilder{
		sessions:  sessions,
		agent:     agent,
		input:     input,
		operation: wire.OperationInvokeAgent,
		fresh:     NewConversationID(),
	}
}

// From names the agent that submits the session. It is required.
func (builder *SubmitBuilder) From(submitter wire.AgentID) *SubmitBuilder {
	derived := *builder
	derived.submitter = submitter
	return &derived
}

// Operation is the command operation, invoke_agent unless set.
func (builder *SubmitBuilder) Operation(operation string) *SubmitBuilder {
	derived := *builder
	derived.operation = operation
	return &derived
}

// Label names the session, deriving its id from the stream, the namespace and
// the label, so the same label reaches the same session.
func (builder *SubmitBuilder) Label(label string) *SubmitBuilder {
	derived := *builder
	derived.label = label
	return &derived
}

// Namespace is what a labeled session id derives under.
func (builder *SubmitBuilder) Namespace(namespace string) *SubmitBuilder {
	derived := *builder
	derived.namespace = namespace
	return &derived
}

// Budget is the token and cost ceiling a reader compares the session's usage
// with.
func (builder *SubmitBuilder) Budget(budget wire.Budget) *SubmitBuilder {
	derived := *builder
	derived.budget = &budget
	return &derived
}

// Tag adds one searchable tag.
func (builder *SubmitBuilder) Tag(tag string) *SubmitBuilder {
	derived := *builder
	derived.tags = append(append([]string(nil), builder.tags...), tag)
	return &derived
}

// ID is the session Send writes: the label's derived id, or the one fresh id
// minted for this submission.
//
// A caller can take it before sending, which is what lets it watch the
// session it is about to create.
func (builder *SubmitBuilder) ID() wire.ConversationID {
	if builder.label == "" {
		return builder.fresh
	}
	return DeriveSessionID(
		builder.sessions.config.Stream, builder.namespace, builder.label)
}

// Send writes the submitted status and the command.
//
// A labeled session that ended completed or failed resumes. One an agent or
// an operator canceled is refused, because a cancel is a decision somebody
// made and resuming under the same label would undo it quietly.
//
// When the command cannot be published after the status was written, the
// session is ended failed rather than left submitted with no work in it: a
// session nobody is working on that still reads as submitted is worse than
// one that visibly failed.
func (builder *SubmitBuilder) Send(ctx context.Context) (*Submitted, error) {
	if builder.submitter.IsZero() {
		return nil, &InvalidError{Reason: "a submission needs From(submitter)"}
	}
	if builder.agent.IsZero() {
		return nil, &InvalidError{Reason: "a submission needs the agent it hands work to"}
	}
	idle := builder.sessions.config.IdleTimeout
	if idle <= 0 {
		return nil, &InvalidError{Reason: "a session idle timeout must be positive"}
	}
	session := builder.ID()
	micros := uint64(idle.Microseconds())
	start := wire.SessionStart{
		Agent:             builder.agent,
		SDK:               builder.sessions.config.SDK,
		IdleTimeoutMicros: &micros,
		Budget:            builder.budget,
		Tags:              builder.tags,
	}
	if builder.label != "" {
		label := builder.label
		start.Label = &label
	}
	if builder.namespace != "" {
		namespace := builder.namespace
		start.Namespace = &namespace
	}
	// The start is not validated here: the send validates the envelope it
	// goes in, body included, and refuses the same start with the same
	// invalid-argument class before anything is published.
	handle := builder.sessions.Open(session, builder.label).As(builder.submitter)
	if builder.label != "" {
		ended, err := handle.EndedAs(ctx)
		if err != nil {
			return nil, err
		}
		if ended == wire.SessionCanceled {
			return nil, &SessionEndedError{Session: session, Status: ended}
		}
	}
	lane, err := handle.Agdx()
	if err != nil {
		return nil, err
	}
	body, err := wire.Marshal(start)
	if err != nil {
		return nil, &CodecError{Reason: "encode the session start", Cause: err}
	}
	if _, err := lane.Status(wire.OperationSession).
		WithTaskState(wire.TaskSubmitted).
		Body(body).
		ContentType(wire.ContentCBOR).
		Send(ctx); err != nil {
		return nil, err
	}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(NewOperationID())}
	_, sent := lane.Command(correlation, builder.input).
		WithOperation(builder.operation).
		WithTarget(builder.agent).
		WithMetadata(wire.MetadataSubmitted, wire.BoolValue(true)).
		Send(ctx)
	if sent != nil {
		message := "the submitted command was not published: " + sent.Error()
		// Ending the session is best effort: the publish failure is what the
		// caller asked about, and reporting a second one instead would hide
		// it. A session left submitted is picked up by its idle timeout.
		_, _ = handle.Fail(ctx, wire.AgentErrorBody{
			Code: wire.ErrorInternal, Message: &message, Retryable: true,
		})
		return nil, sent
	}
	return &Submitted{Session: session, Correlation: correlation}, nil
}

// EndedAs is the terminal status this session reached, empty while it is
// still open.
//
// A forced cancel on the control topic ends a session without its agents
// writing anything, so the control route is read too: a session whose agent
// is gone has no lane record saying it ended.
func (session *Session) EndedAs(ctx context.Context) (wire.SessionStatus, error) {
	envelopes, err := session.laneEnvelopes(ctx)
	if err != nil {
		return "", err
	}
	lifecycle := FoldLaneLifecycle(envelopes)
	forced, err := session.forcedCancel(ctx)
	if err != nil {
		return "", err
	}
	if forced {
		return wire.SessionCanceled, nil
	}
	return lifecycle.Ended, nil
}

// forcedCancel reports an operator cancel on this session's control route.
func (session *Session) forcedCancel(ctx context.Context) (bool, error) {
	scope := session.laser.WithDefaultStream(session.config.Stream).
		Context(session.conversation)
	records, err := scope.read(ctx, wire.AgentControl)
	if err != nil {
		return false, err
	}
	for _, record := range records {
		if record == nil || record.Provenance == nil ||
			record.Provenance.Conversation != session.conversation {
			continue
		}
		var envelope wire.AgentEnvelope
		if err := wire.Unmarshal(record.Payload(), &envelope); err != nil {
			continue
		}
		if envelope.Kind == wire.KindStatus && envelope.TaskState != nil &&
			*envelope.TaskState == wire.TaskCanceled {
			return true, nil
		}
	}
	return false, nil
}

// LaneLifecycle is what a session's lane says about its life: whether it
// started, the terminal status it reached, and the budget its start record
// asked for.
type LaneLifecycle struct {
	Started bool
	// Ended is the terminal status, empty while the session is open.
	Ended  wire.SessionStatus
	Budget *wire.Budget
}

// FoldLaneLifecycle folds a session's lane records into its lifecycle.
//
// A start record after a completed or failed end resumes the session, and
// takes the budget that start asked for: a resumed session is the same
// session under a ceiling its resumer chose. A start after a cancel does not
// resume, because a cancel is a decision rather than an outcome.
func FoldLaneLifecycle(envelopes []wire.AgentEnvelope) LaneLifecycle {
	var lifecycle LaneLifecycle
	for _, envelope := range envelopes {
		if envelope.Kind != wire.KindStatus || envelope.Operation == nil ||
			*envelope.Operation != wire.OperationSession ||
			envelope.TaskState == nil {
			continue
		}
		switch *envelope.TaskState {
		case wire.TaskCompleted, wire.TaskFailed, wire.TaskRejected,
			wire.TaskCanceled:
			if lifecycle.Ended != "" {
				continue
			}
			switch *envelope.TaskState {
			case wire.TaskCompleted:
				lifecycle.Ended = wire.SessionCompleted
			case wire.TaskCanceled:
				lifecycle.Ended = wire.SessionCanceled
			default:
				lifecycle.Ended = wire.SessionFailed
			}
		case wire.TaskSubmitted, wire.TaskWorking, wire.TaskInputRequired,
			wire.TaskAuthRequired:
			var start wire.SessionStart
			if err := wire.Unmarshal(envelope.Body, &start); err != nil {
				continue
			}
			switch {
			case !lifecycle.Started:
				lifecycle.Started = true
				lifecycle.Budget = start.Budget
			case lifecycle.Ended == wire.SessionCompleted ||
				lifecycle.Ended == wire.SessionFailed:
				lifecycle.Ended = ""
				lifecycle.Budget = start.Budget
			}
		}
	}
	return lifecycle
}

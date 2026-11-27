package laser

import (
	"context"
	"sort"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// SessionControl is operator control of one session, sent on agent.control.
//
// Only accounts with send permission on that topic can use it, which is what
// separates an operator from the agents doing the work: a handler cannot
// pause or cancel its own session by writing a record.
type SessionControl struct {
	laser        *Laser
	session      wire.ConversationID
	operator     wire.AgentID
	participants []wire.AgentID
	// declared reports that the caller named the participants, so an empty
	// set stays empty rather than being read off the lane.
	declared bool
	key      *SigningKey
}

// Control opens operator control of one session on a named stream.
func (sessions *Sessions) Control(
	stream string, session wire.ConversationID,
) *SessionControl {
	return &SessionControl{
		laser:   sessions.laser.WithDefaultStream(stream),
		session: session,
	}
}

// AsOperator names the operator identity the control records carry. It is
// required: a control record nobody signed for is a record nobody can be
// asked about.
func (control *SessionControl) AsOperator(operator wire.AgentID) *SessionControl {
	derived := *control
	derived.operator = operator
	return &derived
}

// SignedBy signs every control record with key, so a verifying reader can
// prove which operator sent it.
func (control *SessionControl) SignedBy(key *SigningKey) *SessionControl {
	derived := *control
	derived.key = key
	return &derived
}

// Participants are the agents whose acknowledgments complete a pause,
// instead of the set Pause reads from the session lane.
func (control *SessionControl) Participants(
	participants ...wire.AgentID,
) *SessionControl {
	derived := *control
	derived.participants = append([]wire.AgentID(nil), participants...)
	derived.declared = true
	return &derived
}

// Pause asks the session's agents to pause.
//
// The request names the agents whose acknowledgments complete the pause: the
// set given to Participants, or else the agents the session lane shows
// working on the session. Every agent that receives work for the session
// holds it until the resume, named or not, because a pause is about the
// session and not about who happens to be listed.
func (control *SessionControl) Pause(ctx context.Context) (*AgdxReceipt, error) {
	participants := control.participants
	if !control.declared {
		found, err := control.LaneParticipants(ctx)
		if err != nil {
			return nil, err
		}
		participants = found
	}
	body, err := wire.MarshalJSON(
		wire.SessionPauseRequest{Participants: participants})
	if err != nil {
		return nil, &CodecError{Reason: "encode the pause request", Cause: err}
	}
	return control.requestWith(ctx, wire.OperationSessionPause, body)
}

// Resume asks a paused session's agents to resume.
func (control *SessionControl) Resume(ctx context.Context) (*AgdxReceipt, error) {
	return control.request(ctx, wire.OperationSessionResume)
}

// Cancel asks the session's agents to end it as canceled at their next
// boundary.
func (control *SessionControl) Cancel(ctx context.Context) (*AgdxReceipt, error) {
	return control.request(ctx, wire.OperationSessionCancel)
}

// ForceCancel ends the session as canceled without its agents, for a session
// whose agent is gone.
//
// It is the blunt one: a cancel asks and a force tells. Nothing acknowledges
// it, so a session whose agent is in fact alive would find its own lifecycle
// ended underneath it.
func (control *SessionControl) ForceCancel(ctx context.Context) (*AgdxReceipt, error) {
	producer, err := control.producer()
	if err != nil {
		return nil, err
	}
	reason := "forced"
	body, err := wire.Marshal(wire.SessionEnd{Reason: &reason})
	if err != nil {
		return nil, &CodecError{Reason: "encode the session end", Cause: err}
	}
	send := producer.Status(wire.OperationSession).
		WithTaskState(wire.TaskCanceled).
		Body(body).
		ContentType(wire.ContentCBOR).
		Last()
	return control.signed(send).Send(ctx)
}

// LaneParticipants are the agents the session lane shows working on the
// session: the addressees of its work commands and the agents that picked it
// up.
//
// Model and tool operations are not work handed to an agent, so they name
// nobody, and neither do the control operations themselves: an operator
// pausing a session is not a participant in it.
func (control *SessionControl) LaneParticipants(
	ctx context.Context,
) ([]wire.AgentID, error) {
	// Every topic the session's records ride, not just the shared lane: a
	// per-agent topic layout moves addressed work off it, which is exactly
	// the work that names a participant.
	envelopes, err := laneEnvelopesOf(
		ctx, control.laser, control.laser.defaultStream, control.session)
	if err != nil {
		return nil, err
	}
	found := map[wire.AgentID]struct{}{}
	for _, envelope := range envelopes {
		operation := ""
		if envelope.Operation != nil {
			operation = *envelope.Operation
		}
		switch {
		case envelope.Kind == wire.KindCommand && isWorkOperation(operation):
			if envelope.Target != nil {
				found[*envelope.Target] = struct{}{}
			}
		case envelope.Kind == wire.KindStatus && operation == wire.OperationSession:
			var transition wire.SessionTransition
			if err := wire.Unmarshal(envelope.Body, &transition); err == nil &&
				transition.Actor != nil {
				found[*transition.Actor] = struct{}{}
			}
		}
	}
	participants := make([]wire.AgentID, 0, len(found))
	for agent := range found {
		participants = append(participants, agent)
	}
	// A fixed order, so two operators reading the same lane write the same
	// pause request and a reader can compare them.
	sort.Slice(participants, func(left, right int) bool {
		return participants[left].String() < participants[right].String()
	})
	return participants, nil
}

// isWorkOperation reports an operation that hands work to an agent.
//
// The control operations are the operator's own, and the model and tool
// operations are the session talking to something that is not an agent, so
// neither makes anyone a participant.
func isWorkOperation(operation string) bool {
	for _, control := range wire.ControlOperations {
		if operation == control {
			return false
		}
	}
	switch operation {
	case wire.OperationChat, wire.OperationTextCompletion,
		wire.OperationGenerateContent, wire.OperationExecuteTool:
		return false
	default:
		return true
	}
}

func (control *SessionControl) request(
	ctx context.Context, operation string,
) (*AgdxReceipt, error) {
	return control.requestWith(ctx, operation, []byte("{}"))
}

func (control *SessionControl) requestWith(
	ctx context.Context, operation string, body []byte,
) (*AgdxReceipt, error) {
	producer, err := control.producer()
	if err != nil {
		return nil, err
	}
	correlation := wire.CorrelationID{ID: wire.IDFromBytes(NewOperationID())}
	send := producer.Command(correlation, body).
		WithOperation(operation).
		ContentType(wire.ContentJSON)
	return control.signed(send).Send(ctx)
}

func (control *SessionControl) signed(send *AgdxSend) *AgdxSend {
	if control.key == nil {
		return send
	}
	return send.Signed(control.key)
}

func (control *SessionControl) producer() (*Agdx, error) {
	if control.operator.IsZero() {
		return nil, &InvalidError{
			Reason: "session control needs AsOperator(id)"}
	}
	// The send itself refuses a handle with no stream, so this does not
	// check again for one.
	return control.laser.AgdxOn(control.laser.defaultStream, wire.AgentControl,
		control.operator, control.session), nil
}

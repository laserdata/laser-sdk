package laser

import (
	"bytes"
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// ApprovalGrant is an approver's decision, minted only by an approving reply.
//
// A governed effect carries it with AgdxSend.WithGrant, so a governor's
// step-up in the same conversation is answered by this approval rather than
// by stepping up again. It is not a token a caller can construct: the only
// way to hold one is to have received the reply that granted it.
type ApprovalGrant struct {
	approver     wire.AgentID
	conversation wire.ConversationID
	correlation  wire.CorrelationID
	decision     []byte
}

// Approver is the agent that approved.
func (grant ApprovalGrant) Approver() wire.AgentID { return grant.approver }

// Conversation is the conversation the approval was asked in.
//
// A grant answers a step-up only inside it, because an approval given for one
// piece of work is not an approval for another.
func (grant ApprovalGrant) Conversation() wire.ConversationID {
	return grant.conversation
}

// Correlation is the request the approval answered.
func (grant ApprovalGrant) Correlation() wire.CorrelationID { return grant.correlation }

// Decision is the approver's own body, which carries whatever the approver
// said beyond the fact of approving.
func (grant ApprovalGrant) Decision() []byte {
	return bytes.Clone(grant.decision)
}

// ApprovalGate asks an approver to decide, and waits in place for the answer.
//
// The prompt is sent as a command on the requester's topic and the reply is
// the grant. A reply from anyone but the named approver does not decide, so a
// third party cannot answer an approval it was not asked for.
//
// This gate waits in place, so a gate held open holds its caller open. A
// handler gates through AgentCtx.ApprovalGate, which parks the record and
// replays the handler once the decision lands.
func (requester *Requester) ApprovalGate(
	ctx context.Context, approver wire.AgentID,
	conversation wire.ConversationID, prompt []byte, timeout time.Duration,
) (*ApprovalGrant, error) {
	if approver.IsZero() {
		return nil, &InvalidError{Reason: "an approval gate must name its approver"}
	}
	reply, err := requester.Request(ctx, conversation, prompt, RequestOptions{
		Target: approver, Timeout: timeout,
	})
	if err != nil {
		return nil, err
	}
	return GrantFrom(approver, conversation, reply)
}

// GrantFrom mints a grant from an approving reply, for a caller driving its
// own approval exchange rather than using ApprovalGate.
//
// The reply has to come from the approver that was asked and carry the
// correlation it answers: a grant built from anything else would let a
// step-up be satisfied by a message nobody approved.
func GrantFrom(
	approver wire.AgentID, conversation wire.ConversationID, reply *AgdxMessage,
) (*ApprovalGrant, error) {
	if reply == nil {
		return nil, &InvalidError{Reason: "an approval grant needs a reply"}
	}
	if reply.Envelope.Source != approver {
		return nil, &InvalidError{
			Reason: "the reply came from " + reply.Envelope.Source.String() +
				" rather than from the approver " + approver.String()}
	}
	if reply.Envelope.Conversation != conversation {
		return nil, &InvalidError{
			Reason: "the reply belongs to another conversation"}
	}
	if reply.Envelope.Correlation == nil {
		return nil, &InvalidError{
			Reason: "an approval reply carries no correlation"}
	}
	return &ApprovalGrant{
		approver:     approver,
		conversation: conversation,
		correlation:  *reply.Envelope.Correlation,
		decision:     bytes.Clone(reply.Envelope.Body),
	}, nil
}

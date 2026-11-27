package laser

import (
	"context"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// approvalReplayFunc hands a record parked on an approval back to its
// handler with the decision, reporting whether the handler parked again.
type approvalReplayFunc func(
	ctx context.Context, record *Record, resume *approvalResume,
) (bool, error)

// parkApproval parks the record whose handler stopped at an approval gate.
//
// The record is written to the session lane under the prompt's position, so
// a reader can see which approval is holding it and any later process can
// pick it up. The deadline is remembered so the session can be woken when
// the approval expires undecided, rather than waiting for a decision that is
// never coming.
func (pause *sessionPause) parkApproval(
	ctx context.Context, message *AgentMessage, interrupt approvalInterrupt,
) error {
	session, named := sessionOf(message)
	if !named {
		return &InvalidError{
			Reason: "a record parked on an approval must belong to a session"}
	}
	key, parked, err := pause.parkAt(ctx, session, message, interrupt.prompt)
	if err != nil || !parked {
		return err
	}
	// Only the record that actually prompted carries a deadline. The work
	// held behind it rides the same prompt position but is waiting on this
	// record, not on the approver.
	held := pause.role(session)
	pause.mutex.Lock()
	if held.approvalDeadlines == nil {
		held.approvalDeadlines = map[parkingKey]uint64{}
	}
	held.approvalDeadlines[key] = interrupt.deadlineMicros
	pause.mutex.Unlock()
	return nil
}

// parkAt parks one record under a named request position, reporting whether
// it was newly parked.
//
// The position is what a reader follows to find what is holding the record:
// a control request for a pause, the prompt for an approval.
func (pause *sessionPause) parkAt(
	ctx context.Context, session wire.ConversationID,
	message *AgentMessage, request wire.LogPosition,
) (parkingKey, bool, error) {
	key, addressed := pause.keyOf(ctx, message)
	if !addressed {
		return parkingKey{}, false, &InvalidError{
			Reason: "a parked record must name the log position it came from"}
	}
	held := pause.role(session)
	pause.mutex.Lock()
	_, alreadyParked := held.parked[key]
	alreadyHandled := held.handled[key]
	pause.mutex.Unlock()
	if alreadyParked || alreadyHandled {
		return key, false, nil
	}

	source, err := pause.sourceOf(ctx, session, message)
	if err != nil {
		return parkingKey{}, false, err
	}
	parking := wire.SessionParking{Source: source, Role: pause.me, Request: request}
	body, err := wire.Marshal(parking)
	if err != nil {
		return parkingKey{}, false, &CodecError{
			Reason: "encode the session parking", Cause: err}
	}
	lane, err := pause.lane(session)
	if err != nil {
		return parkingKey{}, false, err
	}
	send := lane.Emit(body).
		WithOperation(wire.OperationSessionParked).
		ContentType(wire.ContentCBOR)
	if message.Envelope != nil && message.Envelope.Record != nil {
		send = send.WithCause(*message.Envelope.Record, nil)
	}
	if _, err := send.Send(ctx); err != nil {
		return parkingKey{}, false, err
	}
	pause.mutex.Lock()
	held.parked[key] = parking
	pause.mutex.Unlock()
	return key, true, nil
}

// isApproval separates a record held for an approval from one held by a
// pause.
//
// Both ride the same parking record; what differs is the request they name.
// A pause names the control request that stopped the session, which carries
// no stream or topic because the control position is read off the control
// lane; an approval names where its prompt landed, which always does.
func isApprovalParking(parking wire.SessionParking) bool {
	return parking.Request.StreamID != 0 || parking.Request.TopicID != 0
}

// approvalOf is the record this session has parked on the approval that
// correlation answers, if any.
//
// The correlation is derived from the record's own position, so this is the
// reverse of that derivation rather than a lookup of something remembered:
// a process that never sent the prompt still finds the record.
func (pause *sessionPause) approvalOf(
	session wire.ConversationID, correlation wire.CorrelationID,
) (parkingKey, wire.SessionParking, bool) {
	held := pause.role(session)
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	for key, parking := range held.parked {
		if !isApprovalParking(parking) {
			continue
		}
		if parking.Source.Message == nil {
			continue
		}
		source := parking.Source.Message
		at := wire.LogPosition{
			StreamID:    source.Stream,
			TopicID:     source.Topic,
			PartitionID: source.Partition,
			Offset:      source.Offset,
		}
		if approvalCorrelation(at) == correlation {
			return key, parking, true
		}
	}
	return parkingKey{}, wire.SessionParking{}, false
}

// resumeApproval hands a parked record back when its approval is decided.
//
// A reply that answers no approval this role parked on is left alone: the
// topic carries every kind of traffic, and a runtime must not claim replies
// that are not its own.
func (pause *sessionPause) resumeApproval(
	ctx context.Context, reply *AgentMessage, replay approvalReplayFunc,
) error {
	correlation, answers := approvalReplyOf(reply, pause.me)
	if !answers {
		return nil
	}
	session, named := sessionOf(reply)
	if !named {
		return nil
	}
	gate := pause.gate(session)
	gate.Lock()
	defer gate.Unlock()
	if err := pause.ensureRecovered(ctx); err != nil {
		return err
	}
	key, parking, parked := pause.approvalOf(session, correlation)
	if !parked {
		return nil
	}
	return pause.drainApproval(ctx, session, key, parking,
		&approvalResume{decided: agdxOf(reply)}, replay)
}

// approvalReplyOf reads the approval correlation a reply addressed to me
// answers, reporting false for every other record on the topic.
func approvalReplyOf(
	reply *AgentMessage, me wire.AgentID,
) (wire.CorrelationID, bool) {
	if reply == nil || reply.Envelope == nil {
		return wire.CorrelationID{}, false
	}
	envelope := reply.Envelope
	if envelope.Kind != wire.KindResponse && envelope.Kind != wire.KindError {
		return wire.CorrelationID{}, false
	}
	// Addressed to this role: an approval decision is sent back to the agent
	// that asked, and another agent's decision is not this runtime's to act
	// on even on a topic it shares.
	if envelope.Target == nil || *envelope.Target != me {
		return wire.CorrelationID{}, false
	}
	if envelope.Correlation == nil || !isApprovalCorrelation(*envelope.Correlation) {
		return wire.CorrelationID{}, false
	}
	return *envelope.Correlation, true
}

// agdxOf is the decision message a replayed gate answers with.
func agdxOf(reply *AgentMessage) *AgdxMessage {
	if reply == nil || reply.Envelope == nil {
		return nil
	}
	return &AgdxMessage{Record: reply.Record, Envelope: *reply.Envelope}
}

// expireApprovals wakes every approval of the session whose deadline passed
// without a decision, so work is not held for an answer nobody is going to
// give.
func (pause *sessionPause) expireApprovals(
	ctx context.Context, session wire.ConversationID, replay approvalReplayFunc,
) error {
	now := uint64(time.Now().UnixMicro())
	held := pause.role(session)
	pause.mutex.Lock()
	expired := make([]parkingKey, 0, len(held.approvalDeadlines))
	for key, deadline := range held.approvalDeadlines {
		if _, stillParked := held.parked[key]; stillParked && deadline <= now {
			expired = append(expired, key)
		}
	}
	pause.mutex.Unlock()

	for _, key := range expired {
		pause.mutex.Lock()
		parking, stillParked := held.parked[key]
		pause.mutex.Unlock()
		if !stillParked {
			continue
		}
		if err := pause.drainApproval(
			ctx, session, key, parking, &approvalResume{}, replay); err != nil {
			return err
		}
	}
	return nil
}

// drainApproval replays the approval's own record with the decision, then
// hands back the work held behind it, oldest first.
//
// A record that parks again on a new approval stops the drain: the session's
// later work belongs behind that approval now, exactly as it belonged behind
// this one.
func (pause *sessionPause) drainApproval(
	ctx context.Context, session wire.ConversationID,
	key parkingKey, parking wire.SessionParking,
	resume *approvalResume, replay approvalReplayFunc,
) error {
	record, found, err := pause.fetchSource(ctx, parking)
	if err != nil {
		return err
	}
	if !found {
		// The source expired or its topic was recreated, so the work cannot
		// be handed back. Dropped from what this process holds, and left
		// listed on the lane as held.
		pause.forgetApproval(session, key)
		pause.markUnrecoverable(session, key)
		return nil
	}
	if replay != nil {
		reparked, err := replay(ctx, record, resume)
		if err != nil {
			return err
		}
		if reparked {
			// The handler asked for another approval. The new parking has
			// replaced this one, so only the old deadline is dropped.
			pause.forgetApproval(session, key)
			return nil
		}
	}
	if err := pause.unpark(ctx, session, parking); err != nil {
		return err
	}
	pause.forgetApproval(session, key)
	pause.markHandled(session, key)

	// The session's other held work was behind this approval, so it runs now
	// that the approval is answered. A pause takes precedence: an operator
	// who stopped the session in the meantime meant it.
	if pending, known := pause.book.Pending(session); known && pending.PauseRequested {
		return nil
	}
	return pause.drain(ctx, session, func(ctx context.Context, held *Record) error {
		_, err := replay(ctx, held, nil)
		return err
	})
}

// forgetApproval drops the deadline of an approval that is no longer held.
func (pause *sessionPause) forgetApproval(
	session wire.ConversationID, key parkingKey,
) {
	held := pause.role(session)
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	delete(held.approvalDeadlines, key)
}

// awaitingApproval is the prompt position the session is waiting on, when it
// is holding work for an approval.
//
// Later work is parked under the same position, so the decision resumes the
// session as it left it rather than running its newer records first.
func (pause *sessionPause) awaitingApproval(
	session wire.ConversationID,
) (wire.LogPosition, bool) {
	held := pause.role(session)
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	for key, parking := range held.parked {
		if !isApprovalParking(parking) {
			continue
		}
		if _, waiting := held.approvalDeadlines[key]; waiting {
			return parking.Request, true
		}
	}
	return wire.LogPosition{}, false
}

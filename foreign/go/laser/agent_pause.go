package laser

import (
	"context"
	"sync"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// holdDecision is what the pause gate decided about one work record.
type holdDecision int

const (
	// holdHandle runs the handler, the ordinary path.
	holdHandle holdDecision = iota
	// holdCommit commits the record without handling it: it is durably
	// parked, and the resume is what hands it back.
	holdCommit
)

// parkingKey identifies one held record by the log position it came from.
//
// The source position, not the record id: two deliveries of one record are
// one piece of held work, and a redelivery after a crash between the parking
// and the commit must not park it twice.
//
// The topic is its numeric id, not its name, because this key has to be
// derivable from two places that know different things: a delivered record,
// which knows the topic it was consumed from, and a parking record read back
// off the lane, which carries only the numeric id. Keying on the name would
// give recovery a key space that could never match a live one.
type parkingKey struct {
	topic     uint32
	partition uint32
	offset    uint64
}

// parkedRole is what one agent role holds for one session.
type parkedRole struct {
	// parked is the held work this role has not handled.
	parked map[parkingKey]wire.SessionParking
	// handled is what it has handled since, so a redelivery of a record the
	// resume already replayed is committed rather than run twice.
	handled map[parkingKey]bool
	// ack is the control request this role last acknowledged on the lane.
	ack *acknowledged
	// approvalDeadlines is when each record held for an approval stops
	// waiting for one. Only approval parkings appear here, so its keys are
	// also what separates them from the records a pause holds.
	approvalDeadlines map[parkingKey]uint64
	resolvedPrompts   map[wire.LogPosition]bool
	// terminal reports that this role ended the paused session canceled.
	terminal bool
}

func (role *parkedRole) resolvePrompt(prompt wire.LogPosition) {
	if role.resolvedPrompts == nil {
		role.resolvedPrompts = make(map[wire.LogPosition]bool)
	}
	role.resolvedPrompts[prompt] = true
}

// sessionPause is the mandatory pause gate of one agent runtime.
//
// A paused session's work is durably parked on the session lane before its
// source offset is committed, and a resume hands the held records back
// before any new work for that session. This is what makes an operator's
// pause binding rather than advisory: nothing reaches a handler while the
// session is paused, whether or not the handler ever asks.
type sessionPause struct {
	laser    *Laser
	sessions *Sessions
	me       wire.AgentID
	book     *ControlBook
	// stream and topic are what this runtime consumes, which is where every
	// record it parks came from.
	stream string
	topic  string
	// ids is the consumed topic's numeric identity, resolved once.
	ids *sourceIDs

	mutex sync.Mutex
	gates map[wire.ConversationID]*sync.Mutex
	roles map[wire.ConversationID]*parkedRole

	// recoverMutex guards the one-time rebuild of held work from the lane.
	// Separate from mutex, which merge takes while holding this one.
	recoverMutex sync.Mutex
	recovered    bool
	// partitions are the ones records have arrived from, the observable
	// half of this member's consumer-group assignment.
	partitions map[uint32]struct{}
	assigned   map[uint32]struct{}
}

// newSessionPause opens a gate over book.
func newSessionPause(
	laser *Laser, sessions *Sessions, me wire.AgentID, book *ControlBook,
	stream, topic string,
) *sessionPause {
	return &sessionPause{
		laser: laser, sessions: sessions, me: me, book: book,
		stream: stream, topic: topic,
		gates: map[wire.ConversationID]*sync.Mutex{},
		roles: map[wire.ConversationID]*parkedRole{},
	}
}

// gate serializes the work of one session, so a pause taken while a record
// of that session is being decided comes after it rather than beside it.
func (pause *sessionPause) gate(session wire.ConversationID) *sync.Mutex {
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	gate, open := pause.gates[session]
	if !open {
		gate = &sync.Mutex{}
		pause.gates[session] = gate
	}
	return gate
}

// role is one session's held work, opened on first use.
func (pause *sessionPause) role(session wire.ConversationID) *parkedRole {
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	held, open := pause.roles[session]
	if !open {
		held = &parkedRole{
			parked:            map[parkingKey]wire.SessionParking{},
			handled:           map[parkingKey]bool{},
			approvalDeadlines: map[parkingKey]uint64{},
		}
		pause.roles[session] = held
	}
	return held
}

// replayFunc hands one held record back to the handler.
type replayFunc func(ctx context.Context, record *Record) error

// hold decides what to do with one work record, under its session's gate.
//
// A paused session parks the record and commits it. A resumed session hands
// back what it held, oldest first, before this record runs.
func (pause *sessionPause) hold(
	ctx context.Context, message *AgentMessage, replay replayFunc,
) (holdDecision, error) {
	session, named := sessionOf(message)
	if !named {
		// Work outside any session has no control state to read, so there
		// is nothing an operator could have paused.
		return holdHandle, nil
	}
	// Rebuilt before the first decision, so a process that restarted knows
	// what it was already holding rather than deciding as though it held
	// nothing, and again when this member starts reading a partition it has
	// not read before.
	if message.Record != nil {
		pause.observePartition(message.Record.PartitionID)
	}
	if err := pause.ensureRecovered(ctx); err != nil {
		return holdHandle, err
	}
	gate := pause.gate(session)
	gate.Lock()
	defer gate.Unlock()

	// A session waiting on an approval holds its later work behind it, in
	// order, so the decision resumes the session as it left it rather than
	// running the records that arrived while the approver was deciding.
	// Checked before the pause, because work held this way is held whatever
	// the control state says.
	if request, waiting := pause.awaitingApproval(session); waiting {
		if _, _, err := pause.parkAt(ctx, session, message, request); err != nil {
			return holdHandle, err
		}
		return holdCommit, nil
	}

	for {
		pending, known := pause.book.Pending(session)
		if !known {
			// The book cannot tell "not paused" from "not read yet", and acting
			// on that difference is how a paused session's work gets through.
			// A book that is not live is the un-ported state, where this gate
			// is not in force at all.
			return holdHandle, nil
		}
		if pending.PauseRequested {
			if ack, named := pause.book.PauseAck(session); named {
				// Acknowledged before the record is parked, so an operator
				// watching the lane sees this role stop before it sees what it
				// stopped holding.
				if err := pause.acknowledge(ctx, session, wire.TaskPaused, ack); err != nil {
					return holdHandle, err
				}
			}
			if pending.CancelRequested {
				pause.cancelPaused(ctx, session, true)
			}
			if err := pause.park(ctx, session, message); err != nil {
				return holdHandle, err
			}
			return holdCommit, nil
		}
		// A resume is acknowledged only by a role that acknowledged the pause:
		// an agent that never stopped has nothing to say it has started again,
		// and saying so would read as a participant the pause never had.
		if !pending.CancelRequested {
			if err := pause.acknowledgeResume(ctx, session); err != nil {
				return holdHandle, err
			}
		}

		key, addressed := pause.keyOf(ctx, message)
		if addressed && pause.handledAlready(session, key) {
			// Redelivered after a crash between the parking and the commit, and
			// already handled through the parking.
			return holdCommit, nil
		}
		if pending.CancelRequested {
			// A canceled session's held records stay unprocessed.
			return holdHandle, nil
		}
		// Unfiltered by assignment: this member is reading the session's work, so
		// what it holds for the session runs first whatever partition it came from.
		interrupted, err := pause.drain(ctx, session, false, replay)
		if err != nil {
			return holdHandle, err
		}
		if !interrupted {
			return holdHandle, nil
		}
		// A pause or a cancel landed mid-drain, so the record is decided again.
	}
}

// involved reports whether this role acknowledged a request of the session or
// holds work for it.
func (pause *sessionPause) involved(session wire.ConversationID) bool {
	held := pause.role(session)
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	return held.ack != nil || len(held.parked) > 0
}

// cancelPaused ends a paused session canceled as this role, once.
//
// Best effort: a failed write is retried by the next record or reconcile.
func (pause *sessionPause) cancelPaused(
	ctx context.Context, session wire.ConversationID, named bool,
) {
	held := pause.role(session)
	pause.mutex.Lock()
	involved := held.ack != nil || len(held.parked) > 0
	terminal := held.terminal
	pause.mutex.Unlock()
	if terminal || !(named || involved) {
		return
	}
	lens := pause.sessions.Open(session, "").As(pause.me)
	if _, err := lens.End(ctx, wire.TaskCanceled, wire.SessionEnd{}); err != nil {
		return
	}
	pause.mutex.Lock()
	held.terminal = true
	pause.mutex.Unlock()
}

// handledAlready reports whether this role already handled the record at key.
func (pause *sessionPause) handledAlready(session wire.ConversationID, key parkingKey) bool {
	held := pause.role(session)
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	return held.handled[key]
}

// park writes the held record on the session lane and records it, so the
// work survives this process and a reader can see what is being held.
func (pause *sessionPause) park(
	ctx context.Context, session wire.ConversationID, message *AgentMessage,
) error {
	key, addressed := pause.keyOf(ctx, message)
	if !addressed {
		return &InvalidError{
			Reason: "a record parked by a pause must name the log position it came from"}
	}
	held := pause.role(session)
	pause.mutex.Lock()
	_, alreadyParked := held.parked[key]
	alreadyHandled := held.handled[key]
	pause.mutex.Unlock()
	if alreadyParked || alreadyHandled {
		return nil
	}

	parking, err := pause.parkingOf(ctx, session, message)
	if err != nil {
		return err
	}
	body, err := wire.Marshal(parking)
	if err != nil {
		return &CodecError{Reason: "encode the session parking", Cause: err}
	}
	lane, err := pause.lane(session)
	if err != nil {
		return err
	}
	// An event, not a status: the contract fixes which operations a status
	// may name, and parking is not one of them. Rust writes it the same way.
	send := lane.Emit(body).
		WithOperation(wire.OperationSessionParked).
		ContentType(wire.ContentCBOR)
	if message.Envelope != nil && message.Envelope.Record != nil {
		send = send.WithCause(*message.Envelope.Record, nil)
	}
	if _, err := send.Send(ctx); err != nil {
		return err
	}
	pause.mutex.Lock()
	held.parked[key] = parking
	pause.mutex.Unlock()
	return nil
}

// drain hands back every record this role is holding for the session,
// oldest source first, and writes each one off the lane as it goes.
//
// It stops at the first record the session is paused again or canceled for,
// reporting that it was interrupted, so a request that lands mid-drain holds
// the rest rather than pushing through. With assignedOnly it leaves records
// of partitions this member does not read to the member that does.
func (pause *sessionPause) drain(
	ctx context.Context, session wire.ConversationID, assignedOnly bool, replay replayFunc,
) (bool, error) {
	for {
		key, parking, more := pause.nextHeld(session, assignedOnly)
		if !more {
			return false, nil
		}
		if pending, known := pause.book.Pending(session); known &&
			(pending.PauseRequested || pending.CancelRequested) {
			return true, nil
		}
		record, found, err := pause.fetchSource(ctx, parking)
		if err != nil {
			return false, err
		}
		if !found {
			// The source expired or its topic was recreated, so the work
			// cannot be recovered. It stays listed as held rather than
			// being quietly forgotten, and the drain moves on.
			pause.markUnrecoverable(session, key)
			continue
		}
		if replay != nil {
			if err := replay(ctx, record); err != nil {
				return false, err
			}
		}
		if err := pause.unpark(ctx, session, parking); err != nil {
			return false, err
		}
		pause.markHandled(session, key)
	}
}

// unpark writes the held record off the lane, so a reader folding the lane
// sees it handled rather than still held.
func (pause *sessionPause) unpark(
	ctx context.Context, session wire.ConversationID, parking wire.SessionParking,
) error {
	body, err := wire.Marshal(parking)
	if err != nil {
		return &CodecError{Reason: "encode the session parking", Cause: err}
	}
	lane, err := pause.lane(session)
	if err != nil {
		return err
	}
	_, err = lane.Emit(body).
		WithOperation(wire.OperationSessionUnparked).
		ContentType(wire.ContentCBOR).
		Send(ctx)
	return err
}

// nextHeld is the oldest record still held for the session.
func (pause *sessionPause) nextHeld(
	session wire.ConversationID, assignedOnly bool,
) (parkingKey, wire.SessionParking, bool) {
	held := pause.role(session)
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	var (
		oldest  parkingKey
		parking wire.SessionParking
		found   bool
	)
	for key, candidate := range held.parked {
		if _, owned := pause.assigned[key.partition]; assignedOnly && pause.assigned != nil && !owned {
			continue
		}
		if pause.isApprovalParking(candidate) && !held.resolvedPrompts[candidate.Request] {
			continue
		}
		if !found || olderParking(key, oldest) {
			oldest, parking, found = key, candidate, true
		}
	}
	return oldest, parking, found
}

// olderParking orders held records by their source position, so a drain
// hands them back in the order they arrived rather than map order.
func olderParking(candidate, current parkingKey) bool {
	if candidate.topic != current.topic {
		return candidate.topic < current.topic
	}
	if candidate.partition != current.partition {
		return candidate.partition < current.partition
	}
	return candidate.offset < current.offset
}

// markHandled moves a held record to handled.
func (pause *sessionPause) markHandled(session wire.ConversationID, key parkingKey) {
	held := pause.role(session)
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	if parking, ok := held.parked[key]; ok && pause.isApprovalParking(parking) {
		held.resolvePrompt(parking.Request)
	}
	delete(held.parked, key)
	held.handled[key] = true
}

// markUnrecoverable drops a held record this process can no longer read
// back, leaving it listed on the lane as held.
func (pause *sessionPause) markUnrecoverable(session wire.ConversationID, key parkingKey) {
	held := pause.role(session)
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	delete(held.parked, key)
}

// fetchSource reads a held record back from the log, reporting false when it
// can no longer be read.
func (pause *sessionPause) fetchSource(
	ctx context.Context, parking wire.SessionParking,
) (*Record, bool, error) {
	source := parking.Source.Message
	if source == nil {
		return nil, false, nil
	}
	ids, err := pause.topicIDs(ctx)
	if err != nil {
		return nil, false, err
	}
	if source.Stream != ids.stream || source.Topic != ids.topic {
		return nil, false, nil
	}
	message, err := pause.laser.ReadAt(ctx, parking.Source)
	if err != nil {
		return nil, false, err
	}
	if message == nil || message.Record == nil {
		return nil, false, nil
	}
	return message.Record, true, nil
}

// keyOf is the held-work identity of one message: the log position it came
// from, which is stable across redeliveries.
func (pause *sessionPause) keyOf(
	ctx context.Context, message *AgentMessage,
) (parkingKey, bool) {
	if message.Record == nil {
		return parkingKey{}, false
	}
	ids, err := pause.topicIDs(ctx)
	if err != nil {
		return parkingKey{}, false
	}
	return parkingKey{
		topic:     ids.topic,
		partition: message.Record.PartitionID,
		offset:    message.Record.Offset(),
	}, true
}

// sourceOf names the log record a message came from, with the numeric ids
// and topic generation that let it be read back after a topic is recreated.
//
// The ids are resolved once and cached: they are the consumed topic's, the
// same for every record this runtime reads.
func (pause *sessionPause) sourceOf(
	ctx context.Context, session wire.ConversationID, message *AgentMessage,
) (wire.SourceRef, error) {
	if message.Record == nil {
		return wire.SourceRef{}, &InvalidError{
			Reason: "a parked record must name the log record it came from"}
	}
	ids, err := pause.topicIDs(ctx)
	if err != nil {
		return wire.SourceRef{}, err
	}
	conversation := session.String()
	generation := ids.generation
	return wire.SourceRef{
		Kind: wire.SourceMessage,
		Message: &wire.MessageSource{
			Stream:       ids.stream,
			Topic:        ids.topic,
			Partition:    message.Record.PartitionID,
			Offset:       message.Record.Offset(),
			Generation:   &generation,
			Conversation: &conversation,
		},
	}, nil
}

// sourceIDs is the consumed topic's numeric identity.
type sourceIDs struct {
	stream     uint32
	topic      uint32
	generation uint64
}

// topicIDs resolves the consumed topic's numeric ids and generation once.
func (pause *sessionPause) topicIDs(ctx context.Context) (sourceIDs, error) {
	pause.mutex.Lock()
	if pause.ids != nil {
		ids := *pause.ids
		pause.mutex.Unlock()
		return ids, nil
	}
	pause.mutex.Unlock()

	client := pause.laser.Client()
	if client == nil {
		return sourceIDs{}, &InvalidError{Reason: "parking a record needs a connected client"}
	}
	streamID, err := iggcon.NewIdentifier(pause.stream)
	if err != nil {
		return sourceIDs{}, &InvalidError{Reason: "stream name: " + err.Error()}
	}
	topicID, err := iggcon.NewIdentifier(pause.topic)
	if err != nil {
		return sourceIDs{}, &InvalidError{Reason: "topic name: " + err.Error()}
	}
	details, err := client.GetTopic(ctx, streamID, topicID)
	if err != nil {
		return sourceIDs{}, &InvalidError{Reason: "read the consumed topic: " + err.Error()}
	}
	if details == nil {
		return sourceIDs{}, &InvalidError{Reason: "the consumed topic is not there"}
	}
	stream, err := pause.laser.Stream(pause.stream).Get(ctx)
	if err != nil || stream == nil {
		return sourceIDs{}, &InvalidError{Reason: "read the consumed stream"}
	}
	ids := sourceIDs{stream: stream.ID, topic: details.Id, generation: details.CreatedAt}
	pause.mutex.Lock()
	pause.ids = &ids
	pause.mutex.Unlock()
	return ids, nil
}

// parkingOf builds the parking record for one message.
func (pause *sessionPause) parkingOf(
	ctx context.Context, session wire.ConversationID, message *AgentMessage,
) (wire.SessionParking, error) {
	source, err := pause.sourceOf(ctx, session, message)
	if err != nil {
		return wire.SessionParking{}, err
	}
	parking := wire.SessionParking{Source: source, Role: pause.me}
	if at, paused := pause.book.PausedAt(session); paused {
		parking.Request = pause.controlPosition(at)
	}
	return parking, nil
}

// lane is the session's own writer on agent.sessions.
func (pause *sessionPause) lane(session wire.ConversationID) (*Agdx, error) {
	return pause.sessions.Open(session, "").As(pause.me).Agdx()
}

// sessionOf is the conversation a work record belongs to.
func sessionOf(message *AgentMessage) (wire.ConversationID, bool) {
	if message.Envelope != nil && !message.Envelope.Conversation.IsZero() {
		return message.Envelope.Conversation, true
	}
	if message.Record != nil && message.Record.Provenance != nil {
		conversation := message.Record.Provenance.Conversation
		if !conversation.IsZero() {
			return conversation, true
		}
	}
	return wire.ConversationID{}, false
}

// acknowledged is the control request this role has already acknowledged on
// the lane for one session, so one request is acknowledged once.
type acknowledged struct {
	state wire.TaskState
	at    ControlPosition
}

// acknowledge writes this role's acknowledgment of a control request on the
// session lane, unless it has already acknowledged that same request.
//
// The acknowledgment is what lets an operator see which agents have actually
// stopped, rather than only that a pause was asked for. It carries the
// position of the request it answers, so an operator can tell this pause's
// acknowledgment from the next one's.
func (pause *sessionPause) acknowledge(
	ctx context.Context, session wire.ConversationID,
	state wire.TaskState, ack ControlAck,
) error {
	held := pause.role(session)
	pause.mutex.Lock()
	already := held.ack != nil && held.ack.state == state && held.ack.at == ack.At
	pause.mutex.Unlock()
	if already {
		return nil
	}
	at := pause.controlPosition(ack.At)
	transition := wire.SessionTransition{Actor: &pause.me, Acknowledges: &at}
	body, err := wire.Marshal(transition)
	if err != nil {
		return &CodecError{Reason: "encode the session transition", Cause: err}
	}
	lane, err := pause.lane(session)
	if err != nil {
		return err
	}
	send := lane.Status(wire.OperationSession).
		WithTaskState(state).
		Body(body).
		ContentType(wire.ContentCBOR)
	if ack.Record != nil {
		send = send.WithCause(*ack.Record, &at)
	}
	if _, err := send.Send(ctx); err != nil {
		return err
	}
	pause.mutex.Lock()
	held.ack = &acknowledged{state: state, at: ack.At}
	pause.mutex.Unlock()
	return nil
}

// acknowledgeResume acknowledges the resume that lifted a pause this role
// acknowledged, and does nothing otherwise.
func (pause *sessionPause) acknowledgeResume(
	ctx context.Context, session wire.ConversationID,
) error {
	held := pause.role(session)
	pause.mutex.Lock()
	stopped := held.ack != nil && held.ack.state == wire.TaskPaused
	pause.mutex.Unlock()
	if !stopped {
		return nil
	}
	ack, lifted := pause.book.ResumeAck(session)
	if !lifted {
		return nil
	}
	return pause.acknowledge(ctx, session, wire.TaskWorking, ack)
}

// controlPosition is where a control request sits, as a log position.
func (pause *sessionPause) controlPosition(at ControlPosition) wire.LogPosition {
	position := wire.LogPosition{PartitionID: at.Partition, Offset: at.Offset}
	pause.book.mutex.Lock()
	defer pause.book.mutex.Unlock()
	if source := pause.book.controlSource; source != nil {
		position.StreamID, position.TopicID = source.StreamID, source.TopicID
	}
	return position
}

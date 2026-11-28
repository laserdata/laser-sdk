package laser

import (
	"context"
	"maps"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// laneScan is the held work one read of the session lane found, per session.
type laneScan map[wire.ConversationID]*parkedRole

// ensureRecovered rebuilds this role's held work from the session lane,
// once, before the gate decides anything.
//
// A parking is durable, so work held by a process that then restarted is
// still on the lane. Without this the new process would not know it was
// holding anything and the work would stay listed as held forever. The scan
// runs only when some session has a pause history at all, so a deployment
// that never pauses pays one book read rather than a lane scan.
//
// A failed scan is not cached: the next record tries again rather than
// leaving the gate permanently blind.
func (pause *sessionPause) ensureRecovered(ctx context.Context) error {
	pause.recoverMutex.Lock()
	defer pause.recoverMutex.Unlock()
	if pause.recovered {
		return nil
	}
	if !pause.book.HasPauseHistory() {
		// Nothing was ever paused on this deployment, so there is no held
		// work to look for. Re-checked on the next record until something
		// is, since the follower may not have read a pause yet.
		return nil
	}
	scan, err := pause.scanLane(ctx)
	if err != nil {
		return err
	}
	pause.merge(scan)
	pause.recovered = true
	return nil
}

// merge folds a scan into what this process already holds.
//
// Handled wins over parked: a record this role wrote off the lane is done,
// whichever order the two records were read in.
func (pause *sessionPause) merge(scan laneScan) {
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	for session, found := range scan {
		held, open := pause.roles[session]
		if !open {
			held = &parkedRole{
				parked:  map[parkingKey]wire.SessionParking{},
				handled: map[parkingKey]bool{},
			}
			pause.roles[session] = held
		}
		for prompt := range found.resolvedPrompts {
			held.resolvePrompt(prompt)
		}
		for key := range found.handled {
			delete(held.parked, key)
			delete(held.approvalDeadlines, key)
			held.handled[key] = true
		}
		for key, parking := range found.parked {
			if held.handled[key] {
				continue
			}
			if _, already := held.parked[key]; !already {
				held.parked[key] = parking
			}
		}
	}
}

// scanLane reads the session lane and folds the work this role is holding,
// per session.
//
// It reads the lane once for every session rather than once per session: a
// recovering process does not yet know which sessions it holds work for,
// which is the whole reason it is reading.
func (pause *sessionPause) scanLane(ctx context.Context) (laneScan, error) {
	cursor, err := pause.laser.Stream(pause.stream).Topic(wire.AgentSessions).
		ReplayAs(parkedScanReader)
	if err != nil {
		return nil, err
	}
	if err := cursor.SeekToWindow(ctx, MaxDrainRecords); err != nil {
		return nil, err
	}
	records, err := cursor.Poll(ctx)
	if err != nil {
		return nil, err
	}
	scan := laneScan{}
	for _, record := range records {
		message, err := OpenAgdx(record)
		if err != nil {
			continue
		}
		envelope := &message.Envelope
		if envelope.Operation == nil {
			continue
		}
		operation := *envelope.Operation
		if operation != wire.OperationSessionParked &&
			operation != wire.OperationSessionUnparked {
			continue
		}
		var parking wire.SessionParking
		if err := wire.Unmarshal(envelope.Body, &parking); err != nil {
			continue
		}
		if err := parking.Validate(); err != nil {
			continue
		}
		// Another role's held work is that role's to replay. Taking it on
		// would hand one agent's work to another.
		if parking.Role != pause.me {
			continue
		}
		session := envelope.Conversation
		held, open := scan[session]
		if !open {
			held = &parkedRole{
				parked:  map[parkingKey]wire.SessionParking{},
				handled: map[parkingKey]bool{},
			}
			scan[session] = held
		}
		key := parkingKeyOf(parking)
		switch operation {
		case wire.OperationSessionParked:
			if !held.handled[key] {
				held.parked[key] = parking
			}
		case wire.OperationSessionUnparked:
			if pause.isApprovalParking(parking) {
				held.resolvePrompt(parking.Request)
			}
			delete(held.parked, key)
			held.handled[key] = true
		}
	}
	return scan, nil
}

// parkedScanReader is how a recovery read names itself to the server. The
// offsets stay client-owned, so the name only identifies the reader.
const parkedScanReader = "laser-parked-recovery"

// observePartition notes which partition a record arrived from, and marks
// the held work for rebuilding when this runtime starts reading one it has
// not read before.
//
// A source that reports no assignment leaves the partitions records arrive
// from as the only sign of one: a partition this member did not read before
// is one another member may have parked work for. A source that does report
// its assignment is followed through observeAssignment.
func (pause *sessionPause) observePartition(partition uint32) {
	pause.mutex.Lock()
	if pause.partitions == nil {
		pause.partitions = map[uint32]struct{}{}
	}
	_, known := pause.partitions[partition]
	first := len(pause.partitions) == 0
	if !known {
		pause.partitions[partition] = struct{}{}
	}
	pause.mutex.Unlock()
	if known || first {
		// The first partition is this runtime's own starting assignment,
		// which the initial recovery already covers.
		return
	}
	pause.recoverMutex.Lock()
	pause.recovered = false
	pause.recoverMutex.Unlock()
}

// observeAssignment notes the partitions this member reads, and marks the
// held work for rebuilding when they change.
func (pause *sessionPause) observeAssignment(partitions []uint32) {
	current := make(map[uint32]struct{}, len(partitions))
	for _, partition := range partitions {
		current[partition] = struct{}{}
	}
	pause.recoverMutex.Lock()
	defer pause.recoverMutex.Unlock()
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	if pause.assigned != nil && !maps.Equal(current, pause.assigned) {
		pause.recovered = false
	}
	pause.assigned = current
}

// reconcilable is every session this role holds work for or the book has a
// pause or resume request for.
func (pause *sessionPause) reconcilable() []wire.ConversationID {
	requested := pause.book.PausedOrResumed()
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	seen := make(map[wire.ConversationID]struct{}, len(pause.roles)+len(requested))
	sessions := make([]wire.ConversationID, 0, len(pause.roles)+len(requested))
	for session, held := range pause.roles {
		if len(held.parked) > 0 {
			seen[session] = struct{}{}
			sessions = append(sessions, session)
		}
	}
	for _, session := range requested {
		if _, listed := seen[session]; !listed {
			sessions = append(sessions, session)
		}
	}
	return sessions
}

// reconcile brings every session up to date with its control requests,
// without waiting for new work to arrive for it.
//
// Without this a pause is only acknowledged, and a resume only takes effect,
// when the session's next record turns up, so a session an operator resumed
// and nobody then wrote to would keep its work held indefinitely.
func (pause *sessionPause) reconcile(
	ctx context.Context, replay replayFunc, decided approvalReplayFunc,
) {
	if err := pause.ensureRecovered(ctx); err != nil {
		return
	}
	for _, session := range pause.reconcilable() {
		if ctx.Err() != nil {
			return
		}
		gate := pause.gate(session)
		gate.Lock()
		pause.reconcileLocked(ctx, session, replay, decided)
		gate.Unlock()
	}
}

// reconcileLocked acknowledges a pause this role is named in or already takes
// part in, ends a paused session canceled, acknowledges a resume, and hands
// back held records on partitions this member reads.
//
// A failure is left for the next pass rather than reported: this runs outside
// any caller's request, and the work stays held either way.
func (pause *sessionPause) reconcileLocked(
	ctx context.Context, session wire.ConversationID,
	replay replayFunc, decided approvalReplayFunc,
) {
	// Expiries come first, and whatever the control state says: an approval
	// nobody answered has to stop holding the session even while it is paused.
	_ = pause.expireApprovals(ctx, session, decided)
	pending, known := pause.book.Pending(session)
	if !known {
		return
	}
	if pending.PauseRequested {
		ack, placed := pause.book.PauseAck(session)
		switch {
		case !placed:
		case pending.CancelRequested:
			pause.cancelPaused(ctx, session, ack.Named)
		case ack.Named || pause.involved(session):
			_ = pause.acknowledge(ctx, session, wire.TaskPaused, ack)
		}
		return
	}
	if pending.CancelRequested {
		return
	}
	_ = pause.acknowledgeResume(ctx, session)
	_, _ = pause.drain(ctx, session, true, replay)
}

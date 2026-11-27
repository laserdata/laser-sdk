package laser

import (
	"context"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// laneScan is the held work one read of the session lane found, per session.
type laneScan map[wire.ConversationID]*parkedRole

// ensureRecovered rebuilds this role's held work from the session lane,
// once, before the gate decides anything.
//
// A parking is durable, so work held by a process that then restarted is
// still on the lane; without this the new process would not know it was
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
	scan, err := scanLaneFor(ctx, pause.laser, pause.stream, pause.me)
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
		for key := range found.handled {
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

// scanLaneFor reads the session lane and folds the work role is holding,
// per session.
//
// It reads the lane once for every session rather than once per session: a
// recovering process does not yet know which sessions it holds work for,
// which is the whole reason it is reading.
func scanLaneFor(
	ctx context.Context, laser *Laser, stream string, role wire.AgentID,
) (laneScan, error) {
	cursor, err := laser.Stream(stream).Topic(wire.AgentSessions).
		ReplayAs(parkedScanReader)
	if err != nil {
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
		if parking.Role != role {
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
// Rust triggers the same re-read off its consumer-group assignment, which
// this client's runtime is not handed. The partitions records actually
// arrive from are the observable half of that assignment: a partition this
// member did not read before is one another member may have parked work for,
// which is exactly the case the re-read exists to catch. It fires later than
// Rust's (on the first record of the new partition rather than at the
// rebalance itself) and not at all for a partition that is assigned and then
// stays silent, both of which are recorded rather than papered over.
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

// heldSessions is every session this role is holding work for.
func (pause *sessionPause) heldSessions() []wire.ConversationID {
	pause.mutex.Lock()
	defer pause.mutex.Unlock()
	sessions := make([]wire.ConversationID, 0, len(pause.roles))
	for session, held := range pause.roles {
		if len(held.parked) > 0 {
			sessions = append(sessions, session)
		}
	}
	return sessions
}

// reconcile hands back the held work of every session that is no longer
// paused, without waiting for new work to arrive for it.
//
// Without this a resume only takes effect when the session's next record
// turns up, so a session an operator resumed and nobody then wrote to would
// keep its work held indefinitely. A paused session is left alone, and so is
// one whose held work cannot be read back.
func (pause *sessionPause) reconcile(
	ctx context.Context, replay replayFunc, decided approvalReplayFunc,
) {
	if err := pause.ensureRecovered(ctx); err != nil {
		return
	}
	for _, session := range pause.heldSessions() {
		if ctx.Err() != nil {
			return
		}
		// Expiries come first, and whatever the control state says: an
		// approval nobody answered has to stop holding the session even
		// while it is paused, or a pause would strand the work behind a
		// deadline that already passed.
		gate := pause.gate(session)
		gate.Lock()
		_ = pause.expireApprovals(ctx, session, decided)
		gate.Unlock()

		pending, known := pause.book.Pending(session)
		if !known || pending.PauseRequested {
			continue
		}
		gate = pause.gate(session)
		gate.Lock()
		// A failed drain is left for the next pass rather than reported:
		// this runs outside any caller's request, so there is nobody to
		// report it to, and the work stays held either way.
		_ = pause.drain(ctx, session, replay)
		gate.Unlock()
	}
}

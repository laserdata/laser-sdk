package laser

import (
	"sync"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// PendingControl is the operator requests a session has on agent.control, as
// this process observed them.
//
// A handler reads them and decides itself when to stop. Nothing here
// interrupts a running handler: a pause an operator asked for is a request to
// the work, not a signal that cuts it off mid-step.
type PendingControl struct {
	// PauseRequested reports a pause that no later resume lifted.
	PauseRequested bool
	// CancelRequested reports a cancel, or an operator forcing the session
	// canceled.
	CancelRequested bool
	// ControlConflict reports a control request that arrived on a partition
	// of agent.control other than the one the session's conversation routes
	// to. Such a request is never applied, here or by the managed backend,
	// because a session's control records have one home and a record
	// somewhere else cannot be ordered against them.
	ControlConflict bool
}

// The control actions a session record can carry.
const (
	controlPause = iota
	controlResume
	controlCancel
)

// ControlRequest is one control request as one agent reads it.
type ControlRequest struct {
	action int
	// Named reports that the request names this agent: it is addressed to
	// it, or it is a pause listing it among the participants whose
	// acknowledgments it needs.
	Named bool
	// Record is the request's own record id, for the causal link of an
	// acknowledgment.
	Record *wire.RecordID
}

// Action is the request kind: "pause", "resume" or "cancel".
func (request ControlRequest) Action() string {
	switch request.action {
	case controlPause:
		return "pause"
	case controlResume:
		return "resume"
	default:
		return "cancel"
	}
}

// ControlRequestOf is the control request an envelope read from agent.control
// makes, and false when it makes none.
//
// A forced cancel is a canceled session status on the control topic. A pause
// applies to every agent it reaches, and its participant list only decides
// which agents it names. A request addressed to another agent is not this
// agent's to read.
func ControlRequestOf(
	envelope *wire.AgentEnvelope, me *wire.AgentID,
) (ControlRequest, bool) {
	if envelope == nil {
		return ControlRequest{}, false
	}
	if envelope.Target != nil && me != nil && *envelope.Target != *me {
		return ControlRequest{}, false
	}
	var action int
	switch {
	case envelope.Kind == wire.KindCommand:
		if envelope.Operation == nil {
			return ControlRequest{}, false
		}
		switch *envelope.Operation {
		case wire.OperationSessionPause:
			action = controlPause
		case wire.OperationSessionResume:
			action = controlResume
		case wire.OperationSessionCancel:
			action = controlCancel
		default:
			return ControlRequest{}, false
		}
	case envelope.Kind == wire.KindStatus && envelope.TaskState != nil &&
		*envelope.TaskState == wire.TaskCanceled:
		action = controlCancel
	default:
		return ControlRequest{}, false
	}
	addressed := envelope.Target != nil && me != nil
	listed := action == controlPause && me != nil && pauseLists(envelope.Body, *me)
	return ControlRequest{
		action: action,
		Named:  addressed || listed,
		Record: envelope.Record,
	}, true
}

// pauseLists reports whether a pause request names agent among the
// participants it waits for.
func pauseLists(body []byte, agent wire.AgentID) bool {
	var request wire.SessionPauseRequest
	if err := wire.Unmarshal(body, &request); err != nil {
		return false
	}
	for _, participant := range request.Participants {
		if participant == agent {
			return true
		}
	}
	return false
}

// ControlPosition is where a control record sits: its partition and offset on
// agent.control.
type ControlPosition struct {
	Partition uint32
	Offset    uint64
}

// newer reports whether at follows last.
//
// Offsets order records within one partition, and a session's control records
// share one partition, so a record on another partition is taken as newer
// rather than silently dropped: it is already flagged as a conflict, and
// treating it as stale would hide it.
func (at ControlPosition) newer(last ControlPosition) bool {
	return at.Partition != last.Partition || at.Offset > last.Offset
}

// ControlPartition is the agent.control partition a session's control records
// belong on: the partition Apache Iggy routes the conversation key to.
//
// The managed backend applies the same rule, so both refuse a record on any
// other partition. The second result is false when the partition count is
// unknown, where every partition is on route.
func ControlPartition(
	conversation wire.ConversationID, partitions uint32,
) (uint32, bool) {
	if partitions == 0 {
		return 0, false
	}
	return wire.XXHash32([]byte(conversation.String()), 0) % partitions, true
}

// placedRequest is a control request at its position on agent.control.
type placedRequest struct {
	at     ControlPosition
	named  bool
	record *wire.RecordID
}

// controlState is one session's folded control state.
type controlState struct {
	flags PendingControl
	// pause is the request in force while a pause is requested.
	pause *placedRequest
	// resume is the latest resume request.
	resume *placedRequest
}

func (state *controlState) apply(at ControlPosition, request ControlRequest) {
	placed := &placedRequest{at: at, named: request.Named, record: request.Record}
	switch request.action {
	case controlPause:
		state.flags.PauseRequested = true
		state.pause = placed
	case controlResume:
		state.flags.PauseRequested = false
		state.pause = nil
		state.resume = placed
	default:
		state.flags.CancelRequested = true
	}
}

// controlEntry is one session's state in the book.
type controlEntry struct {
	state controlState
	last  *ControlPosition
	// folded reports that this entry holds the session's history, rather
	// than only the live records seen since the process started.
	folded bool
	// pending are the live requests that arrived before the fold they belong
	// on top of.
	pending []pendingRequest
	// conflict reports a request that arrived off the session's control
	// route.
	conflict bool
}

type pendingRequest struct {
	at      ControlPosition
	request ControlRequest
}

func (entry *controlEntry) apply(at ControlPosition, request ControlRequest) {
	if entry.last != nil && !at.newer(*entry.last) {
		return
	}
	entry.state.apply(at, request)
	position := at
	entry.last = &position
}

func (entry *controlEntry) withConflict() PendingControl {
	flags := entry.state.flags
	flags.ControlConflict = flags.ControlConflict || entry.conflict
	return flags
}

// ControlBook is the control state of the sessions one process has seen.
//
// The Rust client drives this from its own runtime: a bounded read of
// agent.control at startup loads it, and a follower keeps it current. That
// runtime is not ported here, so a caller that reads agent.control feeds this
// book itself with Load and Observe. The folding rules are the same either
// way, which is what lets a Go process and a Rust one agree about whether a
// session is paused.
type ControlBook struct {
	mutex   sync.Mutex
	entries map[wire.ConversationID]*controlEntry
	// live reports that a follower keeps folded entries current. Without one
	// a read cannot trust what the book holds.
	live bool
	// loaded reports that a startup read covered every session, so a session
	// without an entry has no requests rather than an unknown history.
	loaded bool
	// partitions is the partition count of agent.control, zero while it is
	// unknown.
	partitions uint32
}

// NewControlBook opens an empty book.
func NewControlBook() *ControlBook {
	return &ControlBook{entries: map[wire.ConversationID]*controlEntry{}}
}

// SetPartitions records the partition count of agent.control, which fixes
// each session's control route.
func (book *ControlBook) SetPartitions(partitions uint32) {
	book.mutex.Lock()
	defer book.mutex.Unlock()
	book.partitions = partitions
}

// SetLive records whether a follower is keeping this book current.
func (book *ControlBook) SetLive(live bool) {
	book.mutex.Lock()
	defer book.mutex.Unlock()
	book.live = live
}

// IsLive reports whether a follower is keeping this book current.
func (book *ControlBook) IsLive() bool {
	book.mutex.Lock()
	defer book.mutex.Unlock()
	return book.live
}

// Reset forgets every session, for a recreated agent.control whose history
// starts again.
func (book *ControlBook) Reset() {
	book.mutex.Lock()
	defer book.mutex.Unlock()
	book.entries = map[wire.ConversationID]*controlEntry{}
	book.loaded = false
}

// ControlRecord is one control request at its place in the log.
type ControlRecord struct {
	Conversation wire.ConversationID
	At           ControlPosition
	Request      ControlRequest
}

// Load folds the requests a bounded read of agent.control found, in log
// order.
//
// Every session is folded afterwards: one without an entry has no requests,
// rather than a history nobody has read.
func (book *ControlBook) Load(records []ControlRecord) {
	book.mutex.Lock()
	defer book.mutex.Unlock()
	for _, record := range records {
		onRoute := book.onRoute(record.Conversation, record.At.Partition)
		entry := book.entry(record.Conversation)
		entry.folded = true
		if onRoute {
			entry.apply(record.At, record.Request)
		} else {
			entry.conflict = true
		}
	}
	book.loaded = true
}

// Observe records one live control request.
func (book *ControlBook) Observe(record ControlRecord) {
	book.mutex.Lock()
	defer book.mutex.Unlock()
	onRoute := book.onRoute(record.Conversation, record.At.Partition)
	entry := book.entry(record.Conversation)
	switch {
	case !onRoute:
		entry.conflict = true
	case entry.folded || book.loaded:
		entry.folded = true
		entry.apply(record.At, record.Request)
	default:
		// The session's history has not been read yet, so this request is
		// kept and replayed on top of the fold when it arrives. Applying it
		// to nothing would report a resume as though no pause preceded it.
		entry.pending = append(entry.pending,
			pendingRequest{at: record.At, request: record.Request})
	}
}

// Pending is the folded control state of one session, and false when it must
// be folded from the log first.
func (book *ControlBook) Pending(
	conversation wire.ConversationID,
) (PendingControl, bool) {
	book.mutex.Lock()
	defer book.mutex.Unlock()
	if !book.live {
		return PendingControl{}, false
	}
	entry, known := book.entries[conversation]
	switch {
	case known && entry.folded:
		return entry.withConflict(), true
	case known:
		return PendingControl{}, false
	default:
		return PendingControl{}, book.loaded
	}
}

// HasPauseHistory reports whether any session has a pause or resume request,
// so a caller knows whether there is held work to look for at all.
func (book *ControlBook) HasPauseHistory() bool {
	book.mutex.Lock()
	defer book.mutex.Unlock()
	for _, entry := range book.entries {
		if entry.state.pause != nil || entry.state.resume != nil {
			return true
		}
	}
	return false
}

// PausedOrResumed is the sessions with a pause or resume request.
func (book *ControlBook) PausedOrResumed() []wire.ConversationID {
	book.mutex.Lock()
	defer book.mutex.Unlock()
	var sessions []wire.ConversationID
	for conversation, entry := range book.entries {
		if entry.state.pause != nil || entry.state.resume != nil {
			sessions = append(sessions, conversation)
		}
	}
	return sessions
}

// Install folds a session's own history into the book and replays the live
// requests that arrived while it was being read.
//
// An entry a live follower is already keeping current is kept as it stands: a
// fold read from the log is older than what the follower has seen.
func (book *ControlBook) Install(
	conversation wire.ConversationID, records []ControlRecord, route *uint32,
) PendingControl {
	folded := foldControl(records, route)
	book.mutex.Lock()
	defer book.mutex.Unlock()
	entry := book.entry(conversation)
	if entry.folded && book.live {
		return entry.withConflict()
	}
	pending := entry.pending
	entry.pending = nil
	entry.state = folded.state
	entry.conflict = entry.conflict || folded.conflict
	entry.last = folded.last
	entry.folded = true
	for _, held := range pending {
		entry.apply(held.at, held.request)
	}
	return entry.withConflict()
}

// entry is one session's book entry, created when it is first seen. The
// caller holds the lock.
func (book *ControlBook) entry(conversation wire.ConversationID) *controlEntry {
	if book.entries == nil {
		book.entries = map[wire.ConversationID]*controlEntry{}
	}
	entry, known := book.entries[conversation]
	if !known {
		entry = &controlEntry{}
		book.entries[conversation] = entry
	}
	return entry
}

// onRoute reports whether partition is the control route of conversation.
// Every partition is while the count is unknown. The caller holds the lock.
func (book *ControlBook) onRoute(
	conversation wire.ConversationID, partition uint32,
) bool {
	route, known := ControlPartition(conversation, book.partitions)
	return !known || route == partition
}

// FoldControl folds control records in log order into one session's pending
// control, and the position of the last record it applied.
//
// A record off route, the session's control partition, is not applied and
// flags the result instead. Applying it would order a record against records
// it shares no partition with.
func FoldControl(
	records []ControlRecord, route *uint32,
) (PendingControl, *ControlPosition) {
	folded := foldControl(records, route)
	return folded.withConflict(), folded.last
}

// foldControl is FoldControl keeping the whole entry, which the book needs
// and a caller does not.
func foldControl(records []ControlRecord, route *uint32) controlEntry {
	var folded controlEntry
	for _, record := range records {
		if route == nil || *route == record.At.Partition {
			folded.apply(record.At, record.Request)
		} else {
			folded.conflict = true
		}
	}
	return folded
}

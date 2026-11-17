package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// SessionStatus is the session lifecycle exposed by managed reads.
type SessionStatus string

const (
	SessionSubmitted SessionStatus = "submitted"
	SessionActive    SessionStatus = "active"
	SessionPaused    SessionStatus = "paused"
	SessionCompleted SessionStatus = "completed"
	SessionFailed    SessionStatus = "failed"
	SessionCanceled  SessionStatus = "canceled"
	// SessionUnrecognized is a status this build does not name. A newer peer
	// may report one, and a reader passes it through rather than failing.
	SessionUnrecognized SessionStatus = "unrecognized"
)

// IsNamed reports whether this build names the status.
func (status SessionStatus) IsNamed() bool {
	switch status {
	case SessionSubmitted, SessionActive, SessionPaused,
		SessionCompleted, SessionFailed, SessionCanceled:
		return true
	default:
		return false
	}
}

// MarshalCBOR writes the snake_case name.
func (status SessionStatus) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(string(status)), nil
}

// UnmarshalCBOR reads the name, mapping an unknown one to the passthrough.
func (status *SessionStatus) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a session status", item)
	}
	candidate := SessionStatus(text)
	if !candidate.IsNamed() {
		candidate = SessionUnrecognized
	}
	*status = candidate
	return nil
}

// LinkSurface is the resource surface used to narrow a session link read.
type LinkSurface string

const (
	LinkMemory     LinkSurface = "memory"
	LinkKv         LinkSurface = "kv"
	LinkGraphNode  LinkSurface = "graph_node"
	LinkGraphEdge  LinkSurface = "graph_edge"
	LinkProjection LinkSurface = "projection"
	LinkChild      LinkSurface = "child"
)

// LinkRelation is the relationship a session has with a linked resource.
type LinkRelation string

const (
	RelationWrote    LinkRelation = "wrote"
	RelationRecalled LinkRelation = "recalled"
	RelationTouched  LinkRelation = "touched"
)

// The session read requests. Every request names the stream before any other
// field, because the index is per stream.
type (
	// SessionGetRequest reads one session summary.
	SessionGetRequest struct {
		Stream string         `cbor:"stream"`
		ID     ConversationID `cbor:"id"`
	}

	// SessionListRequest lists session summaries, filtered and paged.
	SessionListRequest struct {
		Stream string         `cbor:"stream"`
		Status *SessionStatus `cbor:"status,omitempty"`
		// Root keeps only sessions in the tree rooted at this session.
		Root *ConversationID `cbor:"root,omitempty"`
		// LabelPrefix keeps only sessions whose label starts with it.
		LabelPrefix *string  `cbor:"label_prefix,omitempty"`
		Agent       *AgentID `cbor:"agent,omitempty"`
		Text        *string  `cbor:"text,omitempty"`
		// Page is 1-based. Zero reads the first page.
		Page uint32 `cbor:"page"`
		// Results is the sessions per page. Zero leaves it to the server.
		Results uint32 `cbor:"results"`
		// AsOf bounds a walk: the value the walk's first page returned. Later
		// pages pass it back so sessions folded after it stay out.
		AsOf *uint64 `cbor:"as_of,omitempty"`
		// Series is the change series AsOf belongs to, from the same first
		// page.
		Series *uint64 `cbor:"series,omitempty"`
	}

	// SessionEventsRequest reads a session timeline page.
	SessionEventsRequest struct {
		Stream string         `cbor:"stream"`
		ID     ConversationID `cbor:"id"`
		// Page is 1-based. Zero reads the first page.
		Page uint32 `cbor:"page"`
		// Results is the events per page. Zero leaves it to the server.
		Results uint32 `cbor:"results"`
		// AsOf bounds a walk: the value the walk's first page returned. Later
		// pages pass it back so events folded after it stay out.
		AsOf *uint64 `cbor:"as_of,omitempty"`
		// Series is the change series AsOf belongs to, from the same first
		// page.
		Series *uint64 `cbor:"series,omitempty"`
	}

	// SessionStateRequest reads a session's folded state document.
	SessionStateRequest struct {
		Stream string         `cbor:"stream"`
		ID     ConversationID `cbor:"id"`
		// HistoryLimit is the most history rows. Zero leaves it to the server.
		HistoryLimit uint32 `cbor:"history_limit"`
	}

	// SessionLinksRequest reads the resources a session touched.
	SessionLinksRequest struct {
		Stream  string         `cbor:"stream"`
		ID      ConversationID `cbor:"id"`
		Surface *LinkSurface   `cbor:"surface,omitempty"`
	}

	// SessionSourcesRequest reads the partitions holding a session's records.
	SessionSourcesRequest struct {
		Stream string         `cbor:"stream"`
		ID     ConversationID `cbor:"id"`
		// LaneOnly returns only the registered lane identity for a native
		// lane writer.
		LaneOnly bool `cbor:"lane_only,omitempty"`
	}

	// SessionChangesRequest reads a page of a stream's change feed.
	//
	// Page p of size r holds the changes with sequence (p-1)*r+1 through p*r,
	// so appends and pruning never shift a page.
	SessionChangesRequest struct {
		Stream string `cbor:"stream"`
		// Page is 1-based. Zero reads the first page.
		Page uint32 `cbor:"page"`
		// Results is the changes per page. Zero leaves it to the server.
		Results uint32 `cbor:"results"`
	}
)

// SourceFrontier is how far the session index has folded one source
// partition.
//
// Offsets are never compared across topics, and the generation is the topic
// creation time, so a recreated topic with a reused id is a different source.
type SourceFrontier struct {
	TopicID         uint32 `cbor:"topic_id"`
	TopicGeneration uint64 `cbor:"topic_generation"`
	PartitionID     uint32 `cbor:"partition_id"`
	// Folded is the last offset folded into the index, absent before the
	// first record.
	Folded *uint64 `cbor:"folded,omitempty"`
	// Head is the live partition head when the read probed it.
	Head *uint64 `cbor:"head,omitempty"`
	// RetainedFrom is the first offset still retained by the source.
	RetainedFrom *uint64 `cbor:"retained_from,omitempty"`
}

// GapReason says why part of a session timeline is missing.
type GapReason string

const (
	// GapExpiredBeforeFold means the source expired these offsets before the
	// index folded them.
	GapExpiredBeforeFold GapReason = "expired_before_fold"
	// GapPruned means the index pruned these offsets after the source
	// expired them.
	GapPruned GapReason = "pruned"
	// GapTruncated means the session reached its event cap, so these records
	// were counted only.
	GapTruncated GapReason = "truncated"
	// GapRebuilding means the source is being rebuilt after a reset.
	GapRebuilding GapReason = "rebuilding"
	// GapUnrecognized is a reason this build does not name.
	GapUnrecognized GapReason = "unrecognized"
)

// IsNamed reports whether this build names the reason.
func (reason GapReason) IsNamed() bool {
	switch reason {
	case GapExpiredBeforeFold, GapPruned, GapTruncated, GapRebuilding:
		return true
	default:
		return false
	}
}

// MarshalCBOR writes the snake_case name.
func (reason GapReason) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(string(reason)), nil
}

// UnmarshalCBOR reads the name, mapping an unknown one to the passthrough.
func (reason *GapReason) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a gap reason", item)
	}
	candidate := GapReason(text)
	if !candidate.IsNamed() {
		candidate = GapUnrecognized
	}
	*reason = candidate
	return nil
}

// SourceGap is a missing inclusive offset range on one source partition.
type SourceGap struct {
	TopicID         uint32    `cbor:"topic_id"`
	TopicGeneration uint64    `cbor:"topic_generation"`
	PartitionID     uint32    `cbor:"partition_id"`
	From            uint64    `cbor:"from"`
	To              uint64    `cbor:"to"`
	Reason          GapReason `cbor:"reason"`
}

// PayloadRange is the inclusive offset range on one source partition holding
// the payloads of a timeline page.
//
// It is a hint for bounded polls, not a promise that every offset in it
// belongs to the session.
type PayloadRange struct {
	TopicID         uint32 `cbor:"topic_id"`
	TopicGeneration uint64 `cbor:"topic_generation"`
	PartitionID     uint32 `cbor:"partition_id"`
	First           uint64 `cbor:"first"`
	Last            uint64 `cbor:"last"`
}

// StateOutcome is what the fold did with one state record.
type StateOutcome string

const (
	// StateApplied means the patch or snapshot changed the document.
	StateApplied StateOutcome = "applied"
	// StateRejected means a patch operation failed, so the whole patch was
	// rejected.
	StateRejected StateOutcome = "rejected"
	// StateStale means the base revision did not match the applied revision.
	StateStale StateOutcome = "stale"
	// StateDuplicate means the operation id was already applied.
	StateDuplicate StateOutcome = "duplicate"
	// StateUnrecognized is an outcome this build does not name.
	StateUnrecognized StateOutcome = "unrecognized"
)

// IsNamed reports whether this build names the outcome.
func (outcome StateOutcome) IsNamed() bool {
	switch outcome {
	case StateApplied, StateRejected, StateStale, StateDuplicate:
		return true
	default:
		return false
	}
}

// MarshalCBOR writes the snake_case name.
func (outcome StateOutcome) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(string(outcome)), nil
}

// UnmarshalCBOR reads the name, mapping an unknown one to the passthrough.
func (outcome *StateOutcome) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a state outcome", item)
	}
	candidate := StateOutcome(text)
	if !candidate.IsNamed() {
		candidate = StateUnrecognized
	}
	*outcome = candidate
	return nil
}

// StateChange is one entry in the state history of a session.
type StateChange struct {
	// Revision is the document revision after this record.
	Revision  uint64       `cbor:"revision"`
	OpID      *string      `cbor:"op_id,omitempty"`
	Outcome   StateOutcome `cbor:"outcome"`
	At        LogPosition  `cbor:"at"`
	BrokerTS  uint64       `cbor:"broker_ts"`
	OldDigest *Digest32    `cbor:"old_digest,omitempty"`
	NewDigest *Digest32    `cbor:"new_digest,omitempty"`
	Reason    *string      `cbor:"reason,omitempty"`
}

// SessionFlags are the conditions that make a session summary partial.
type SessionFlags struct {
	LabelTruncated  bool `cbor:"label_truncated,omitempty"`
	Overflow        bool `cbor:"overflow,omitempty"`
	EventsTruncated bool `cbor:"events_truncated,omitempty"`
	LaneConflict    bool `cbor:"lane_conflict,omitempty"`
	// ControlConflict means a control record arrived on a partition other
	// than the session's control route and was not applied.
	ControlConflict bool `cbor:"control_conflict,omitempty"`
	Rebuilding      bool `cbor:"rebuilding,omitempty"`
	// LivenessUnknown means the heartbeat tail has not caught up, so the idle
	// flag is not meaningful.
	LivenessUnknown bool `cbor:"liveness_unknown,omitempty"`
}

// SessionInfo is the summary of one session.
type SessionInfo struct {
	Stream         string          `cbor:"stream"`
	ID             ConversationID  `cbor:"id"`
	Label          *string         `cbor:"label,omitempty"`
	Namespace      *string         `cbor:"namespace,omitempty"`
	Agent          *AgentID        `cbor:"agent,omitempty"`
	Parent         *ConversationID `cbor:"parent,omitempty"`
	Root           *ConversationID `cbor:"root,omitempty"`
	Status         SessionStatus   `cbor:"status"`
	Idle           bool            `cbor:"idle"`
	OverBudget     bool            `cbor:"over_budget"`
	PauseRequested bool            `cbor:"pause_requested"`
	// Awaiting is the participants of the pause in force that have not
	// acknowledged it.
	Awaiting        []AgentID    `cbor:"awaiting,omitempty"`
	CancelRequested bool         `cbor:"cancel_requested"`
	StartedAt       *uint64      `cbor:"started_at,omitempty"`
	EndedAt         *uint64      `cbor:"ended_at,omitempty"`
	FirstEventAt    *uint64      `cbor:"first_event_at,omitempty"`
	LastEventAt     *uint64      `cbor:"last_event_at,omitempty"`
	LastHeartbeatAt *uint64      `cbor:"last_heartbeat_at,omitempty"`
	Events          uint64       `cbor:"events"`
	ModelCalls      uint64       `cbor:"model_calls"`
	ToolCalls       uint64       `cbor:"tool_calls"`
	TokensIn        uint64       `cbor:"tokens_in"`
	TokensOut       uint64       `cbor:"tokens_out"`
	CostMicros      uint64       `cbor:"cost_micros"`
	Errors          uint64       `cbor:"errors"`
	Budget          *Budget      `cbor:"budget,omitempty"`
	SDK             *SdkInfo     `cbor:"sdk,omitempty"`
	Flags           SessionFlags `cbor:"flags"`
	// Held is work records kept while the session was paused and not yet
	// handled.
	Held     uint64           `cbor:"held,omitempty"`
	Frontier []SourceFrontier `cbor:"frontier,omitempty"`
}

// SessionPage is one page of session summaries.
type SessionPage struct {
	Items        []SessionInfo `cbor:"items"`
	Page         uint32        `cbor:"page"`
	TotalPages   uint32        `cbor:"total_pages"`
	TotalResults uint32        `cbor:"total_results"`
	// AsOf is the newest change the page covers. Pass it back with the series
	// for the next page of the same walk.
	AsOf uint64 `cbor:"as_of"`
	// Series is the change series of the serving index.
	Series uint64 `cbor:"series"`
	// Searched is the rows a substring search examined.
	Searched *uint64 `cbor:"searched,omitempty"`
	// Truncated means a substring search filled its scan window, so the total
	// counts only the matches inside it.
	Truncated bool `cbor:"truncated"`
	// Rebuilding means the index is rebuilding on this node, so the items and
	// total can be incomplete.
	Rebuilding bool `cbor:"rebuilding,omitempty"`
}

// SessionEvent is one indexed record on a session timeline. The summary never
// holds prompt text or tool argument values.
type SessionEvent struct {
	At          SourceRef      `cbor:"at"`
	Session     ConversationID `cbor:"session"`
	BrokerTS    uint64         `cbor:"broker_ts"`
	Kind        string         `cbor:"kind"`
	Operation   *string        `cbor:"operation,omitempty"`
	Display     string         `cbor:"display"`
	Agent       *AgentID       `cbor:"agent,omitempty"`
	Addressee   *string        `cbor:"addressee,omitempty"`
	Correlation *CorrelationID `cbor:"correlation,omitempty"`
	Cause       *LogPosition   `cbor:"cause,omitempty"`
	Tool        *string        `cbor:"tool,omitempty"`
	Usage       *TokenUsage    `cbor:"usage,omitempty"`
	AfterEnd    bool           `cbor:"after_end,omitempty"`
	// VerifiedActor is the principal whose enrolled key verified this
	// record's signature.
	VerifiedActor *string             `cbor:"verified_actor,omitempty"`
	Summary       map[string]DynValue `cbor:"summary,omitempty"`
}

// SessionEventsPage is one page of a session timeline.
type SessionEventsPage struct {
	Items        []SessionEvent `cbor:"items"`
	Page         uint32         `cbor:"page"`
	TotalPages   uint32         `cbor:"total_pages"`
	TotalResults uint32         `cbor:"total_results"`
	// AsOf is the newest change the page covers. Pass it back with the series
	// for the next page of the same walk.
	AsOf uint64 `cbor:"as_of"`
	// Series is the change series of the serving index.
	Series   uint64           `cbor:"series"`
	Ranges   []PayloadRange   `cbor:"ranges,omitempty"`
	Frontier []SourceFrontier `cbor:"frontier,omitempty"`
	Gaps     []SourceGap      `cbor:"gaps,omitempty"`
}

// SessionStateView is the folded state document of a session.
type SessionStateView struct {
	Revision uint64          `cbor:"revision"`
	Document DynValue        `cbor:"document"`
	History  []StateChange   `cbor:"history,omitempty"`
	Frontier *SourceFrontier `cbor:"frontier,omitempty"`
	Complete bool            `cbor:"complete"`
	// HeadOpID is the operation id of the delta that produced the revision,
	// when known.
	HeadOpID *string `cbor:"head_op_id,omitempty"`
}

// SessionLink is one resource a session wrote, recalled or touched.
type SessionLink struct {
	Surface  LinkSurface  `cbor:"surface"`
	Resource string       `cbor:"resource"`
	Item     string       `cbor:"item"`
	Relation LinkRelation `cbor:"relation"`
	First    LogPosition  `cbor:"first"`
	Last     LogPosition  `cbor:"last"`
}

// SessionLinksView is the resources linked to one session.
type SessionLinksView struct {
	Links     []SessionLink    `cbor:"links"`
	Frontier  []SourceFrontier `cbor:"frontier,omitempty"`
	Truncated bool             `cbor:"truncated"`
}

// LaneIdentity is a registered lane's topic id, creation generation and
// partition count. It rides as a three-element array, the Rust tuple form.
type LaneIdentity struct {
	TopicID    uint32
	Generation uint64
	Partitions uint32
}

// MarshalCBOR writes the three-element array.
func (lane LaneIdentity) MarshalCBOR() (cborform.Value, error) {
	return cborform.Array{
		cborform.Uint(uint64(lane.TopicID)),
		cborform.Uint(lane.Generation),
		cborform.Uint(uint64(lane.Partitions)),
	}, nil
}

// UnmarshalCBOR reads the three-element array.
func (lane *LaneIdentity) UnmarshalCBOR(item cborform.Value) error {
	fields, ok := item.(cborform.Array)
	if !ok || len(fields) != 3 {
		return fmt.Errorf("wire: a lane identity is a three-element array, got %T", item)
	}
	numbers := make([]uint64, 3)
	for i, field := range fields {
		number, ok := field.(cborform.Uint)
		if !ok {
			return fmt.Errorf("wire: lane identity element %d is not an integer", i)
		}
		numbers[i] = uint64(number)
	}
	*lane = LaneIdentity{
		TopicID: uint32(numbers[0]), Generation: numbers[1], Partitions: uint32(numbers[2]),
	}
	return nil
}

// SessionSources is the source partitions that hold records of one session.
type SessionSources struct {
	Sources []SourceFrontier `cbor:"sources"`
	Lane    *LaneIdentity    `cbor:"lane,omitempty"`
}

// SessionChangeOrigin says what made the sessions of one change row change.
type SessionChangeOrigin string

const (
	// OriginRecords is records folded from the session sources. The default.
	OriginRecords SessionChangeOrigin = "records"
	// OriginLinks is links written by managed mutations or projections.
	OriginLinks SessionChangeOrigin = "links"
	// OriginMaintenance is a source partition reset or removed from the
	// registration, with its sessions rebuilt.
	OriginMaintenance SessionChangeOrigin = "maintenance"
	// OriginExpiry is sessions idle past their retention being removed.
	OriginExpiry SessionChangeOrigin = "expiry"
)

// IsRecords reports the default origin, which is omitted on the wire.
func (origin SessionChangeOrigin) IsRecords() bool {
	return origin == OriginRecords || origin == ""
}

// SessionChangeRow is one committed change batch in a stream.
type SessionChangeRow struct {
	Seq      uint64           `cbor:"seq"`
	Sessions []ConversationID `cbor:"sessions"`
	// Positions are the records behind the change: the folded records for a
	// records origin, the link source records for links, and for an expiry
	// the folded record whose time expired the sessions. Empty for
	// maintenance and for an expiry the clock decided.
	Positions []LogPosition       `cbor:"positions,omitempty"`
	Truncated bool                `cbor:"truncated"`
	Origin    SessionChangeOrigin `cbor:"origin,omitempty"`
}

// SessionChanges is the change rows after a sequence.
//
// On a series mismatch the floor is the newest sequence and resync is set.
// Otherwise the floor is the oldest retained sequence, or the next one when
// none is retained, and resync is set when the cursor fell behind the floor
// even with rows present. On a resync the reader adopts the series and floor
// and lists sessions again.
type SessionChanges struct {
	Items        []SessionChangeRow `cbor:"items"`
	Page         uint32             `cbor:"page"`
	TotalPages   uint32             `cbor:"total_pages"`
	TotalResults uint64             `cbor:"total_results"`
	// Results is the page size the server used, which fixes the sequence
	// range of a page.
	Results uint32 `cbor:"results"`
	Floor   uint64 `cbor:"floor"`
	// Series is the registration's change series.
	Series uint64 `cbor:"series"`
}

// RequiredFieldNames refuses a change page without its series.
//
// A missing series would read as series zero and let a follower continue
// across an index rebuild it should have noticed, silently skipping every row
// the rebuilt series renumbered.
func (*SessionChanges) RequiredFieldNames() []string { return []string{"series"} }

// SessionHeartbeat is the body of one process heartbeat: the sessions in one
// stream that the process holds leases on. A process with sessions in several
// streams writes one heartbeat per stream.
type SessionHeartbeat struct {
	Process  string           `cbor:"process"`
	Stream   string           `cbor:"stream"`
	Sessions []ConversationID `cbor:"sessions"`
	// StreamGeneration is the creation time of the stream the leases were
	// taken on. Absent when the writer does not know it, so a heartbeat that
	// names none counts for whichever generation is live.
	StreamGeneration *uint64 `cbor:"stream_generation,omitempty"`
}

// CountsFor reports whether this heartbeat counts toward a stream
// generation. One that names no generation counts for any, so a heartbeat
// written before generations existed is not discarded.
func (heartbeat SessionHeartbeat) CountsFor(streamGeneration uint64) bool {
	return heartbeat.StreamGeneration == nil ||
		*heartbeat.StreamGeneration == streamGeneration
}

// Validate bounds how many sessions one heartbeat lists.
func (heartbeat SessionHeartbeat) Validate() error {
	if len(heartbeat.Sessions) > MaxHeartbeatSessions {
		return &InvalidError{Reason: "heartbeat lists too many sessions"}
	}
	return nil
}

// IsDefaultValue reports the default change origin, omitted on the wire.
func (origin SessionChangeOrigin) IsDefaultValue() bool { return origin.IsRecords() }

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *LinkRelation) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "link relation", RelationWrote, RelationRecalled, RelationTouched)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *LinkSurface) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "link surface", LinkMemory, LinkKv, LinkGraphNode, LinkGraphEdge, LinkProjection, LinkChild)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *SessionChangeOrigin) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "session change origin", OriginRecords, OriginLinks, OriginMaintenance, OriginExpiry)
}

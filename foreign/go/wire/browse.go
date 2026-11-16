package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// ProjectionInfo is one registered projection with the bindings that admit it.
type ProjectionInfo struct {
	Projection Projection          `cbor:"projection"`
	Bindings   []ProjectionBinding `cbor:"bindings,omitempty"`
	// Status is how far each binding has materialized on the answering node,
	// present only when the request asked for it.
	Status []BindingStatus `cbor:"status,omitempty"`
}

// RealizationState is whether a binding's destination table exists.
type RealizationState string

const (
	// RealizationPending means the table is not there yet.
	RealizationPending RealizationState = "pending"
	// RealizationAwaitingOwner means another node owns the shared
	// destination and creates the table.
	RealizationAwaitingOwner RealizationState = "awaiting_owner"
	// RealizationRealized means the table exists.
	RealizationRealized RealizationState = "realized"
	// RealizationUnknown is a state a newer server named that this build does
	// not know.
	RealizationUnknown RealizationState = "unknown"
)

// UnmarshalCBOR reads the state, degrading an unknown name to the passthrough.
func (state *RealizationState) UnmarshalCBOR(item cborform.Value) error {
	name, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a realization state", item)
	}
	switch RealizationState(name) {
	case RealizationPending, RealizationAwaitingOwner, RealizationRealized:
		*state = RealizationState(name)
	default:
		*state = RealizationUnknown
	}
	return nil
}

// FailureClass is why a destination table could not be created.
type FailureClass string

const (
	// FailureConfiguration means the binding names a backend the server
	// cannot resolve.
	FailureConfiguration FailureClass = "configuration"
	// FailureSchema means the destination refused the table definition.
	FailureSchema FailureClass = "schema"
	// FailurePoison means the destination refused the statement itself.
	FailurePoison FailureClass = "poison"
	// FailureFatal means the destination failed in a way no retry recovers.
	FailureFatal FailureClass = "fatal"
	// FailureTransient means the destination was briefly out of reach.
	FailureTransient FailureClass = "transient"
	// FailureCapacity means the destination ran out of capacity.
	FailureCapacity FailureClass = "capacity"
	// FailureReadOnly means the destination accepts no writes for now.
	FailureReadOnly FailureClass = "read_only"
	// FailureUnknown is a class a newer server named that this build does not
	// know.
	FailureUnknown FailureClass = "unknown"
)

// IsPermanent reports a failure that lasts until the binding or its backend
// changes, so a caller stops waiting and fixes the setup instead.
func (class FailureClass) IsPermanent() bool {
	switch class {
	case FailureConfiguration, FailureSchema, FailurePoison, FailureFatal:
		return true
	default:
		return false
	}
}

// UnmarshalCBOR reads the class, degrading an unknown name to the passthrough.
func (class *FailureClass) UnmarshalCBOR(item cborform.Value) error {
	name, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a failure class", item)
	}
	switch FailureClass(name) {
	case FailureConfiguration, FailureSchema, FailurePoison, FailureFatal,
		FailureTransient, FailureCapacity, FailureReadOnly:
		*class = FailureClass(name)
	default:
		*class = FailureUnknown
	}
	return nil
}

// StallReason is what keeps a binding from visible progress when no record
// holds it.
type StallReason string

const (
	// StallSourceMissing means the source topic does not exist.
	StallSourceMissing StallReason = "source_missing"
	// StallSourceUnreadable means the source partition heads could not be
	// read.
	StallSourceUnreadable StallReason = "source_unreadable"
	// StallDestinationUnreadable means the destination's progress could not
	// be read.
	StallDestinationUnreadable StallReason = "destination_unreadable"
	// StallBackpressure means the destination refuses writes for now, so the
	// projector waits.
	StallBackpressure StallReason = "backpressure"
	// StallUnknown is a reason a newer server named that this build does not
	// know.
	StallUnknown StallReason = "unknown"
)

// UnmarshalCBOR reads the reason, degrading an unknown name to the passthrough.
func (reason *StallReason) UnmarshalCBOR(item cborform.Value) error {
	name, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a stall reason", item)
	}
	switch StallReason(name) {
	case StallSourceMissing, StallSourceUnreadable,
		StallDestinationUnreadable, StallBackpressure:
		*reason = StallReason(name)
	default:
		*reason = StallUnknown
	}
	return nil
}

// HoldCause is what holds a partition at a record.
type HoldCause string

const (
	HoldExtraction  HoldCause = "extraction"
	HoldSchema      HoldCause = "schema"
	HoldDestination HoldCause = "destination"
	HoldOversize    HoldCause = "oversize"
	// HoldMissingHistory means the history the binding needs is gone.
	HoldMissingHistory HoldCause = "missing_history"
	// HoldRetiredIncarnation means the binding was applied for an earlier
	// incarnation of its source stream. Apply it again to materialize the
	// live one.
	HoldRetiredIncarnation HoldCause = "retired_incarnation"
	// HoldUnknown is a cause a newer server named that this build does not
	// know.
	HoldUnknown HoldCause = "unknown"
)

// UnmarshalCBOR reads the cause, degrading an unknown name to the passthrough.
func (cause *HoldCause) UnmarshalCBOR(item cborform.Value) error {
	name, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a hold cause", item)
	}
	switch HoldCause(name) {
	case HoldExtraction, HoldSchema, HoldDestination, HoldOversize,
		HoldMissingHistory, HoldRetiredIncarnation:
		*cause = HoldCause(name)
	default:
		*cause = HoldUnknown
	}
	return nil
}

// PartitionProgress is one source partition's progress into a binding's
// destination.
type PartitionProgress struct {
	PartitionID uint32 `cbor:"partition_id"`
	// HeadOffset is the last offset the source partition holds.
	HeadOffset *uint64 `cbor:"head_offset,omitempty"`
	// AppliedOffset is the last offset the destination committed.
	AppliedOffset *uint64 `cbor:"applied_offset,omitempty"`
	// FirstOffset is the first offset the binding materialized for this
	// incarnation, when the answering node observed it. Above zero, the
	// history started late. Absent means unknown, never complete.
	FirstOffset *uint64 `cbor:"first_offset,omitempty"`
}

// Hold is why a partition does not advance.
type Hold struct {
	PartitionID uint32    `cbor:"partition_id"`
	Offset      uint64    `cbor:"offset"`
	Cause       HoldCause `cbor:"cause"`
	Reason      string    `cbor:"reason"`
}

// MaterializationReadiness is how far a materialization setup has come,
// weakest first.
type MaterializationReadiness uint8

const (
	// ReadinessAccepted means the definitions are durably appended.
	ReadinessAccepted MaterializationReadiness = iota
	// ReadinessApplied means the answering node applied the definitions.
	ReadinessApplied
	// ReadinessRealized means the destination table exists.
	ReadinessRealized
	// ReadinessCaughtUp means the destination reached the source heads
	// captured after apply.
	ReadinessCaughtUp
)

// readinessNames spells the ladder on the wire.
var readinessNames = [...]string{"accepted", "applied", "realized", "caught_up"}

// MarshalCBOR writes the level's name.
func (level MaterializationReadiness) MarshalCBOR() (cborform.Value, error) {
	if int(level) >= len(readinessNames) {
		return nil, fmt.Errorf("wire: unknown materialization readiness %d", level)
	}
	return cborform.Text(readinessNames[level]), nil
}

// UnmarshalCBOR reads the level's name.
func (level *MaterializationReadiness) UnmarshalCBOR(item cborform.Value) error {
	name, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a materialization readiness", item)
	}
	for index, candidate := range readinessNames {
		if string(name) == candidate {
			*level = MaterializationReadiness(index)
			return nil
		}
	}
	return fmt.Errorf("wire: unknown materialization readiness %q", name)
}

// String renders the level's name.
func (level MaterializationReadiness) String() string {
	if int(level) >= len(readinessNames) {
		return "unknown"
	}
	return readinessNames[level]
}

// BindingStatus is one binding's materialization as the answering node sees
// it.
type BindingStatus struct {
	History ProjectionHistory `cbor:"history,omitempty"`
	Source  SourceSelector    `cbor:"source"`
	Index   string            `cbor:"index"`

	Realization       RealizationState `cbor:"realization"`
	RealizationReason *string          `cbor:"realization_reason,omitempty"`
	// Failure is the class of the last failed attempt to create the
	// destination table, present while realization is pending after a
	// failure.
	Failure *FailureClass `cbor:"failure,omitempty"`
	// Stall says why the binding makes no visible progress although no record
	// holds it.
	Stall *StallReason `cbor:"stall,omitempty"`
	// SourceIncarnation is the creation time of the source topic incarnation
	// being materialized.
	SourceIncarnation *uint64             `cbor:"source_incarnation,omitempty"`
	Partitions        []PartitionProgress `cbor:"partitions,omitempty"`
	Holds             []Hold              `cbor:"holds,omitempty"`
	AcceptedGaps      []GapAcceptance     `cbor:"accepted_gaps,omitempty"`
	SkippedRecords    uint64              `cbor:"skipped_records"`
}

// Readiness is the level this status proves on its own: realized once the
// table exists, else applied. Catch-up needs a captured target, so CaughtUpTo
// answers that.
func (status BindingStatus) Readiness() MaterializationReadiness {
	if status.Realization == RealizationRealized {
		return ReadinessRealized
	}
	return ReadinessApplied
}

// IsBlocked reports a record or a history gap stopping a partition.
func (status BindingStatus) IsBlocked() bool { return len(status.Holds) != 0 }

// IsFailed reports a destination table that failed in a way no retry
// recovers, so the binding or its backend must change.
func (status BindingStatus) IsFailed() bool {
	return status.Failure != nil && status.Failure.IsPermanent()
}

// CaughtUpTo reports whether this status reached every head the target
// captured, on the same source incarnation. A partition the target saw empty
// needs nothing.
func (status BindingStatus) CaughtUpTo(target BindingStatus) bool {
	if status.Realization != RealizationRealized || status.IsBlocked() {
		return false
	}
	if status.SourceIncarnation == nil || target.SourceIncarnation == nil ||
		*status.SourceIncarnation != *target.SourceIncarnation {
		return false
	}
	if status.Source != target.Source || status.Index != target.Index ||
		status.History.Kind != target.History.Kind {
		return false
	}
	for _, wanted := range target.Partitions {
		if wanted.HeadOffset == nil {
			continue
		}
		reached := false
		for _, seen := range status.Partitions {
			if seen.PartitionID == wanted.PartitionID && seen.AppliedOffset != nil &&
				*seen.AppliedOffset >= *wanted.HeadOffset {
				reached = true
				break
			}
		}
		if !reached {
			return false
		}
	}
	return true
}

// HistoryComplete reports whether the destination provably holds every record
// of the source incarnation it has passed.
//
// Every partition with progress started at offset zero, no gap was accepted,
// no record was skipped and, under the referencing oversize policy, no body is
// kept only by reference. An unknown first offset is never complete, and a
// partition the binding never read is complete only while no first offset
// above zero says its history expired unread.
func (status BindingStatus) HistoryComplete(oversize OversizePolicy) bool {
	if !status.History.IsAppend() || status.Realization != RealizationRealized ||
		status.SourceIncarnation == nil || status.IsBlocked() ||
		oversize == OversizeReference || len(status.AcceptedGaps) != 0 ||
		status.SkippedRecords != 0 {
		return false
	}
	for _, partition := range status.Partitions {
		startedAtZero := partition.FirstOffset != nil && *partition.FirstOffset == 0
		neverRead := partition.AppliedOffset == nil && partition.FirstOffset == nil
		if !startedAtZero && !neverRead {
			return false
		}
	}
	return true
}

// SchemaInfo is a writer schema plus its lifecycle state.
//
// A dropped schema is hidden from the active set and its id rejects
// re-registration with a different definition, but records stamped with the id
// keep decoding, because ids are permanent.
type SchemaInfo struct {
	Schema  SchemaDef `cbor:"schema"`
	Dropped bool      `cbor:"dropped"`
}

// The registry browse requests.
type (
	// GetProjection reads one projection's details by id.
	GetProjection struct {
		V  uint32 `cbor:"v"`
		ID string `cbor:"id"`
		// Status also reports each binding's materialization status.
		Status bool `cbor:"status,omitempty"`
	}

	// ListProjections lists registered projections, optionally filtered.
	//
	// Empty filters list every projection. Topics keeps projections bound to
	// any named source topic, NameContains and IDPrefix match the name and
	// id, and Search is the single-box convenience matching either. The
	// filters compose.
	ListProjections struct {
		V            uint32           `cbor:"v"`
		Topics       []string         `cbor:"topics,omitempty"`
		NameContains *string          `cbor:"name_contains,omitempty"`
		IDPrefix     *string          `cbor:"id_prefix,omitempty"`
		Search       *string          `cbor:"search,omitempty"`
		Kinds        []ProjectionKind `cbor:"kinds,omitempty"`
		// Stream is the stream whose resources the listing covers. It is
		// required in stream tenancy, where the server authorizes the stream
		// and lists only its resources.
		Stream *string `cbor:"stream,omitempty"`
		// Page is 1-based. Zero reads the first page.
		Page uint32 `cbor:"page,omitempty"`
		// Results is the projections per page. Zero leaves it to the server.
		Results uint32 `cbor:"results,omitempty"`
	}

	// GetSchema reads one registered writer schema by id.
	GetSchema struct {
		V  uint32 `cbor:"v"`
		ID uint32 `cbor:"id"`
		// Stream is the registry holding the id. Absent reads the
		// deployment-wide registry.
		Stream *string `cbor:"stream,omitempty"`
	}

	// ListSchemas lists registered writer schemas, optionally filtered.
	//
	// The filter lives here rather than only on the HTTP query, so the server
	// pushes it down instead of the client paging the whole set.
	ListSchemas struct {
		V            uint32  `cbor:"v"`
		NameContains *string `cbor:"name_contains,omitempty"`
		Stream       *string `cbor:"stream,omitempty"`
		// Page is 1-based. Zero reads the first page.
		Page uint32 `cbor:"page,omitempty"`
		// Results is the schemas per page. Zero leaves it to the server.
		Results uint32 `cbor:"results,omitempty"`
	}

	// RegisterSchema is the synchronous register request: no id, the server
	// validates the definition, allocates the next free id and durably
	// appends the control event.
	RegisterSchema struct {
		V       uint32       `cbor:"v"`
		Source  SchemaSource `cbor:"source"`
		Name    *string      `cbor:"name,omitempty"`
		Version *uint32      `cbor:"version,omitempty"`
		// Stream is the registry that allocates the id.
		Stream *string `cbor:"stream,omitempty"`
	}

	// DecodeRecord decodes one record body under the schema registered for an
	// id, the same decode the projector runs on a stamped record.
	//
	// Read-only convenience for consoles and tools holding a schema-first
	// payload that want its JSON form without re-implementing the codecs.
	DecodeRecord struct {
		V       uint32  `cbor:"v"`
		ID      uint32  `cbor:"id"`
		Payload []byte  `cbor:"payload"`
		Stream  *string `cbor:"stream,omitempty"`
	}
)

// ProjectionPage is one page of a projection listing. Pages count from one.
type ProjectionPage struct {
	Items        []ProjectionInfo `cbor:"items"`
	Page         uint32           `cbor:"page"`
	TotalPages   uint32           `cbor:"total_pages"`
	TotalResults uint32           `cbor:"total_results"`
}

// SchemaPage is one page of a writer-schema listing, ordered by id. Pages
// count from one.
type SchemaPage struct {
	Items        []SchemaInfo `cbor:"items"`
	Page         uint32       `cbor:"page"`
	TotalPages   uint32       `cbor:"total_pages"`
	TotalResults uint32       `cbor:"total_results"`
}

// BrowseOutcomeKind discriminates a registry browse result.
type BrowseOutcomeKind uint8

const (
	// BrowseProjections is one page of the matching projections.
	BrowseProjections BrowseOutcomeKind = iota
	// BrowseProjection is one projection, or none when the id is unknown.
	BrowseProjection
	// BrowseSchemas is one page of writer schemas, active and tombstoned.
	BrowseSchemas
	// BrowseSchema is the schema occupying an id, or none.
	BrowseSchema
	// BrowseSchemaRegistered is the allocated id, already durably appended to
	// the control topic. Visibility follows within the apply latency.
	BrowseSchemaRegistered
	// BrowseDecoded is the payload's decoded form, or none when the body does
	// not decode under the requested schema.
	BrowseDecoded
)

// BrowseOutcome is the result of a registry browse, shaped per request.
type BrowseOutcome struct {
	Kind        BrowseOutcomeKind
	Projections ProjectionPage
	Projection  *ProjectionInfo
	Schemas     SchemaPage
	Schema      *SchemaInfo
	SchemaID    uint32
	Decoded     *DynValue
}

// The externally tagged browse outcome names.
const (
	browseProjectionsVariant      = "Projections"
	browseProjectionVariant       = "Projection"
	browseSchemasVariant          = "Schemas"
	browseSchemaVariant           = "Schema"
	browseSchemaRegisteredVariant = "SchemaRegistered"
	browseDecodedVariant          = "Decoded"
)

// MarshalCBOR writes the externally tagged outcome.
//
// The three optional results ride an explicit null when absent, so an unknown
// id is a routine miss rather than an error.
func (outcome BrowseOutcome) MarshalCBOR() (cborform.Value, error) {
	switch outcome.Kind {
	case BrowseProjections:
		payload, err := toValueOf(outcome.Projections)
		if err != nil {
			return nil, err
		}
		return taggedVariant(browseProjectionsVariant, payload), nil
	case BrowseProjection:
		if outcome.Projection == nil {
			return taggedVariant(browseProjectionVariant, cborform.Null{}), nil
		}
		return marshalVariantStruct(browseProjectionVariant, *outcome.Projection)
	case BrowseSchemas:
		payload, err := toValueOf(outcome.Schemas)
		if err != nil {
			return nil, err
		}
		return taggedVariant(browseSchemasVariant, payload), nil
	case BrowseSchema:
		if outcome.Schema == nil {
			return taggedVariant(browseSchemaVariant, cborform.Null{}), nil
		}
		return marshalVariantStruct(browseSchemaVariant, *outcome.Schema)
	case BrowseSchemaRegistered:
		return taggedVariant(browseSchemaRegisteredVariant,
			cborform.Uint(uint64(outcome.SchemaID))), nil
	case BrowseDecoded:
		if outcome.Decoded == nil {
			return taggedVariant(browseDecodedVariant, cborform.Null{}), nil
		}
		payload, err := outcome.Decoded.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(browseDecodedVariant, payload), nil
	default:
		return nil, fmt.Errorf("wire: unknown browse outcome %d", outcome.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged outcome.
func (outcome *BrowseOutcome) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("BrowseOutcome", name, payload); err != nil {
		return err
	}
	_, isNull := payload.(cborform.Null)

	switch name {
	case browseProjectionsVariant:
		var page ProjectionPage
		if err := fromValueInto(payload, &page); err != nil {
			return err
		}
		*outcome = BrowseOutcome{Kind: BrowseProjections, Projections: page}
	case browseProjectionVariant:
		if isNull {
			*outcome = BrowseOutcome{Kind: BrowseProjection}
			return nil
		}
		var projection ProjectionInfo
		if err := fromValueInto(payload, &projection); err != nil {
			return err
		}
		*outcome = BrowseOutcome{Kind: BrowseProjection, Projection: &projection}
	case browseSchemasVariant:
		var page SchemaPage
		if err := fromValueInto(payload, &page); err != nil {
			return err
		}
		*outcome = BrowseOutcome{Kind: BrowseSchemas, Schemas: page}
	case browseSchemaVariant:
		if isNull {
			*outcome = BrowseOutcome{Kind: BrowseSchema}
			return nil
		}
		var schema SchemaInfo
		if err := fromValueInto(payload, &schema); err != nil {
			return err
		}
		*outcome = BrowseOutcome{Kind: BrowseSchema, Schema: &schema}
	case browseSchemaRegisteredVariant:
		number, ok := payload.(cborform.Uint)
		if !ok {
			return fmt.Errorf("wire: a registration carries an id, got %T", payload)
		}
		*outcome = BrowseOutcome{Kind: BrowseSchemaRegistered, SchemaID: uint32(number)}
	case browseDecodedVariant:
		if isNull {
			*outcome = BrowseOutcome{Kind: BrowseDecoded}
			return nil
		}
		decoded := NewDynValue(payload)
		*outcome = BrowseOutcome{Kind: BrowseDecoded, Decoded: &decoded}
	default:
		return unknownVariant("BrowseOutcome", name)
	}
	return nil
}

// BrowseReply is the reply to a registry browse.
type BrowseReply struct {
	Err     *QueryError
	Outcome *BrowseOutcome
}

// IsOk reports a successful reply.
func (reply BrowseReply) IsOk() bool { return reply.Err == nil && reply.Outcome != nil }

// MarshalCBOR writes the externally tagged reply.
func (reply BrowseReply) MarshalCBOR() (cborform.Value, error) {
	switch {
	case reply.Err != nil && reply.Outcome != nil:
		return nil, fmt.Errorf("wire: a browse reply is either an outcome or an error")
	case reply.Err != nil:
		payload, err := reply.Err.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(kvErrVariant, payload), nil
	case reply.Outcome != nil:
		payload, err := reply.Outcome.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(kvOkVariant, payload), nil
	default:
		return nil, fmt.Errorf("wire: an empty browse reply")
	}
}

// UnmarshalCBOR reads the externally tagged reply.
func (reply *BrowseReply) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("BrowseReply", name, payload); err != nil {
		return err
	}
	switch name {
	case kvOkVariant:
		var outcome BrowseOutcome
		if err := outcome.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = BrowseReply{Outcome: &outcome}
		return nil
	case kvErrVariant:
		var failure QueryError
		if err := failure.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = BrowseReply{Err: &failure}
		return nil
	default:
		return unknownVariant("BrowseReply", name)
	}
}

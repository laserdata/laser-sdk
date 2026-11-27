package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// FilterState is a saved filter's lifecycle state.
type FilterState string

const (
	// FilterActive can be revised and bound.
	FilterActive FilterState = "active"
	// FilterArchived is hidden from new bindings. Existing bindings keep
	// executing.
	FilterArchived FilterState = "archived"
	// FilterDropped is a tombstone: the id, name and revisions are never
	// reused.
	FilterDropped FilterState = "dropped"
)

// FilterGroupIdentity is a consumer group's exact identity, stamped by the
// streaming server from its own metadata.
//
// A recreated stream, topic or group with the same names gets a different
// identity, so it never inherits an old binding.
type FilterGroupIdentity struct {
	StreamID              uint32 `cbor:"stream_id"`
	StreamCreatedAtMicros uint64 `cbor:"stream_created_at_micros"`
	TopicID               uint32 `cbor:"topic_id"`
	TopicCreatedAtMicros  uint64 `cbor:"topic_created_at_micros"`
	GroupID               uint64 `cbor:"group_id"`
}

// FilterGroupRef names a consumer group by name.
type FilterGroupRef struct {
	Stream string `cbor:"stream"`
	Topic  string `cbor:"topic"`
	Group  string `cbor:"group"`
}

// FilterOwner is the consumer group a group policy filter belongs to: the
// exact incarnation and its names when the policy was configured.
//
// Iggy has no group rename, so the group name stays current. Stream and topic
// names can change, so a reader resolves them live from the identity.
type FilterOwner struct {
	Identity FilterGroupIdentity `cbor:"identity"`
	Group    FilterGroupRef      `cbor:"group"`
}

// FilterRevisionInfo is one immutable revision of a saved filter.
type FilterRevisionInfo struct {
	// Enabled defaults to true, so a revision written before pausing existed
	// reads back as live rather than paused.
	Enabled         bool           `cbor:"enabled"`
	Revision        uint32         `cbor:"revision"`
	Digest          Digest32       `cbor:"digest"`
	Filter          ConsumerFilter `cbor:"filter"`
	CreatedAtMicros uint64         `cbor:"created_at_micros"`
}

// UnmarshalCBOR decodes a revision with enabled defaulting to true.
//
// The default matters: a missing field must not read as paused, which would
// silently stop a reader that was running fine.
func (info *FilterRevisionInfo) UnmarshalCBOR(item cborform.Value) error {
	type plain FilterRevisionInfo
	decoded := plain{Enabled: true}
	if err := fromValueInto(item, &decoded); err != nil {
		return err
	}
	*info = FilterRevisionInfo(decoded)
	return nil
}

// FilterSummary is a saved filter as a list row.
type FilterSummary struct {
	ID             uint32      `cbor:"id"`
	Name           string      `cbor:"name"`
	Description    string      `cbor:"description"`
	State          FilterState `cbor:"state"`
	LatestRevision uint32      `cbor:"latest_revision"`
	LatestDigest   Digest32    `cbor:"latest_digest"`
	Codec          FilterCodec `cbor:"codec"`
	// Bindings is how many consumer groups are bound to any revision.
	Bindings        uint32 `cbor:"bindings"`
	CreatedAtMicros uint64 `cbor:"created_at_micros"`
	UpdatedAtMicros uint64 `cbor:"updated_at_micros"`
	// Owner is the consumer group whose own policy this filter is. Absent
	// for a registered filter.
	Owner *FilterOwner `cbor:"owner,omitempty"`
}

// FilterBinding is a consumer group pinned to one revision.
//
// The streaming server applies that revision to every filtered read of the
// group, and rejects a reader that brings a different filter.
type FilterBinding struct {
	Group         FilterGroupRef      `cbor:"group"`
	Identity      FilterGroupIdentity `cbor:"identity"`
	FilterID      uint32              `cbor:"filter_id"`
	Revision      uint32              `cbor:"revision"`
	Digest        Digest32            `cbor:"digest"`
	BoundAtMicros uint64              `cbor:"bound_at_micros"`
	// PolicyGeneration is the group's policy generation this binding
	// established. Every effective policy change increments it, so a page or
	// acknowledgment names the exact policy it ran under.
	PolicyGeneration uint64 `cbor:"policy_generation"`
}

// FilterDetail is a saved filter with its latest revision and bindings.
type FilterDetail struct {
	Summary  FilterSummary      `cbor:"summary"`
	Latest   FilterRevisionInfo `cbor:"latest"`
	Bindings []FilterBinding    `cbor:"bindings"`
}

// GroupFilterSpec is the policy a group is configured with in one step: a
// named definition saved for this group alone.
//
// It stays an externally tagged enum of one variant, because the contract
// keeps room for other ways of supplying a policy.
type GroupFilterSpec struct {
	Name   string
	Filter ConsumerFilter
}

// DefinitionSpec saves a named definition for this group alone.
func DefinitionSpec(name string, filter ConsumerFilter) GroupFilterSpec {
	return GroupFilterSpec{Name: name, Filter: filter}
}

const specDefinitionVariant = "definition"

type definitionSpecFields struct {
	Name   string         `cbor:"name"`
	Filter ConsumerFilter `cbor:"filter"`
}

// MarshalCBOR writes the externally tagged spec.
func (spec GroupFilterSpec) MarshalCBOR() (cborform.Value, error) {
	return marshalVariantStruct(specDefinitionVariant, definitionSpecFields{
		Name: spec.Name, Filter: spec.Filter,
	})
}

// UnmarshalCBOR reads the externally tagged spec.
func (spec *GroupFilterSpec) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("GroupFilterSpec", name, payload); err != nil {
		return err
	}
	if name != specDefinitionVariant {
		return unknownVariant("GroupFilterSpec", name)
	}
	var fields definitionSpecFields
	if err := fromValueInto(payload, &fields); err != nil {
		return err
	}
	*spec = DefinitionSpec(fields.Name, fields.Filter)
	return nil
}

// GroupPolicyUnbound is a verified consumer group that has no filter policy.
//
// Readers of the group receive every record. The policy generation fences a
// later binding against acknowledgments of pages delivered unfiltered.
type GroupPolicyUnbound struct {
	Identity         FilterGroupIdentity `cbor:"identity"`
	PolicyGeneration uint64              `cbor:"policy_generation"`
}

// FilterMutationKind discriminates one catalog change.
type FilterMutationKind uint8

const (
	// MutateSetRevisionEnabled pauses or resumes new reads without changing
	// the revision's policy.
	MutateSetRevisionEnabled FilterMutationKind = iota
	// MutateRegister saves a new filter as revision 1. The name must be
	// unused.
	MutateRegister
	// MutateRevise adds a revision. The expected revision must be the latest.
	MutateRevise
	// MutateDescribe replaces the description, leaving executable content
	// unchanged.
	MutateDescribe
	// MutateArchive hides the filter from new bindings.
	MutateArchive
	// MutateDrop tombstones the filter. Every bound group is released first,
	// each release advancing that group's policy generation like an unbind.
	MutateDrop
	// MutateBind pins a consumer group to a revision. The group must exist.
	MutateBind
	// MutateUnbind releases a group, conditional on the digest it is bound to.
	MutateUnbind
	// MutateConfigureGroup configures a group's policy in one catalog
	// transaction. A repeat with the same digest returns the existing
	// binding.
	MutateConfigureGroup
)

type setRevisionEnabledFields struct {
	FilterID uint32 `cbor:"filter_id"`
	Revision uint32 `cbor:"revision"`
	Enabled  bool   `cbor:"enabled"`
}

type registerFields struct {
	Name        string         `cbor:"name"`
	Description string         `cbor:"description"`
	Filter      ConsumerFilter `cbor:"filter"`
}

type reviseFields struct {
	FilterID         uint32         `cbor:"filter_id"`
	ExpectedRevision uint32         `cbor:"expected_revision"`
	Filter           ConsumerFilter `cbor:"filter"`
}

type describeFields struct {
	FilterID    uint32 `cbor:"filter_id"`
	Description string `cbor:"description"`
}

type filterIDFields struct {
	FilterID uint32 `cbor:"filter_id"`
}

type bindFields struct {
	Group    FilterGroupRef `cbor:"group"`
	FilterID uint32         `cbor:"filter_id"`
	Revision uint32         `cbor:"revision"`
}

type unbindFields struct {
	Group            FilterGroupRef       `cbor:"group"`
	ExpectedDigest   Digest32             `cbor:"expected_digest"`
	ExpectedIdentity *FilterGroupIdentity `cbor:"expected_identity,omitempty"`
}

type configureGroupFields struct {
	Group  FilterGroupRef  `cbor:"group"`
	Policy GroupFilterSpec `cbor:"policy"`
	// ExpectedIdentity refuses a deleted or recreated group before
	// configuring its policy.
	ExpectedIdentity *FilterGroupIdentity `cbor:"expected_identity,omitempty"`
}

// FilterMutation is one catalog change.
type FilterMutation struct {
	Kind             FilterMutationKind
	FilterID         uint32
	Revision         uint32
	ExpectedRevision uint32
	Enabled          bool
	Name             string
	Description      string
	Filter           *ConsumerFilter
	Group            *FilterGroupRef
	ExpectedDigest   *Digest32
	ExpectedIdentity *FilterGroupIdentity
	Policy           *GroupFilterSpec
}

// The externally tagged, snake_case mutation names.
const (
	mutateSetRevisionEnabledVariant = "set_revision_enabled"
	mutateRegisterVariant           = "register"
	mutateReviseVariant             = "revise"
	mutateDescribeVariant           = "describe"
	mutateArchiveVariant            = "archive"
	mutateDropVariant               = "drop"
	mutateBindVariant               = "bind"
	mutateUnbindVariant             = "unbind"
	mutateConfigureGroupVariant     = "configure_group"
)

// MarshalCBOR writes the externally tagged mutation.
func (mutation FilterMutation) MarshalCBOR() (cborform.Value, error) {
	switch mutation.Kind {
	case MutateSetRevisionEnabled:
		return marshalVariantStruct(mutateSetRevisionEnabledVariant, setRevisionEnabledFields{
			FilterID: mutation.FilterID, Revision: mutation.Revision, Enabled: mutation.Enabled,
		})
	case MutateRegister:
		if mutation.Filter == nil {
			return nil, fmt.Errorf("wire: a registration carries no filter")
		}
		return marshalVariantStruct(mutateRegisterVariant, registerFields{
			Name: mutation.Name, Description: mutation.Description, Filter: *mutation.Filter,
		})
	case MutateRevise:
		if mutation.Filter == nil {
			return nil, fmt.Errorf("wire: a revision carries no filter")
		}
		return marshalVariantStruct(mutateReviseVariant, reviseFields{
			FilterID: mutation.FilterID, ExpectedRevision: mutation.ExpectedRevision,
			Filter: *mutation.Filter,
		})
	case MutateDescribe:
		return marshalVariantStruct(mutateDescribeVariant, describeFields{
			FilterID: mutation.FilterID, Description: mutation.Description,
		})
	case MutateArchive:
		return marshalVariantStruct(mutateArchiveVariant,
			filterIDFields{FilterID: mutation.FilterID})
	case MutateDrop:
		return marshalVariantStruct(mutateDropVariant,
			filterIDFields{FilterID: mutation.FilterID})
	case MutateBind:
		if mutation.Group == nil {
			return nil, fmt.Errorf("wire: a binding names no group")
		}
		return marshalVariantStruct(mutateBindVariant, bindFields{
			Group: *mutation.Group, FilterID: mutation.FilterID, Revision: mutation.Revision,
		})
	case MutateUnbind:
		if mutation.Group == nil || mutation.ExpectedDigest == nil {
			return nil, fmt.Errorf("wire: an unbind names no group or expected digest")
		}
		return marshalVariantStruct(mutateUnbindVariant, unbindFields{
			Group: *mutation.Group, ExpectedDigest: *mutation.ExpectedDigest,
			ExpectedIdentity: mutation.ExpectedIdentity,
		})
	case MutateConfigureGroup:
		if mutation.Group == nil || mutation.Policy == nil {
			return nil, fmt.Errorf("wire: a group configuration names no group or policy")
		}
		return marshalVariantStruct(mutateConfigureGroupVariant, configureGroupFields{
			Group: *mutation.Group, Policy: *mutation.Policy,
			ExpectedIdentity: mutation.ExpectedIdentity,
		})
	default:
		return nil, fmt.Errorf("wire: unknown filter mutation %d", mutation.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged mutation.
func (mutation *FilterMutation) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("FilterMutation", name, payload); err != nil {
		return err
	}

	switch name {
	case mutateSetRevisionEnabledVariant:
		var fields setRevisionEnabledFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*mutation = FilterMutation{
			Kind: MutateSetRevisionEnabled, FilterID: fields.FilterID,
			Revision: fields.Revision, Enabled: fields.Enabled,
		}
	case mutateRegisterVariant:
		var fields registerFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*mutation = FilterMutation{
			Kind: MutateRegister, Name: fields.Name,
			Description: fields.Description, Filter: &fields.Filter,
		}
	case mutateReviseVariant:
		var fields reviseFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*mutation = FilterMutation{
			Kind: MutateRevise, FilterID: fields.FilterID,
			ExpectedRevision: fields.ExpectedRevision, Filter: &fields.Filter,
		}
	case mutateDescribeVariant:
		var fields describeFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*mutation = FilterMutation{
			Kind: MutateDescribe, FilterID: fields.FilterID, Description: fields.Description,
		}
	case mutateArchiveVariant, mutateDropVariant:
		var fields filterIDFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		kind := MutateArchive
		if name == mutateDropVariant {
			kind = MutateDrop
		}
		*mutation = FilterMutation{Kind: kind, FilterID: fields.FilterID}
	case mutateBindVariant:
		var fields bindFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*mutation = FilterMutation{
			Kind: MutateBind, Group: &fields.Group,
			FilterID: fields.FilterID, Revision: fields.Revision,
		}
	case mutateUnbindVariant:
		var fields unbindFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*mutation = FilterMutation{
			Kind: MutateUnbind, Group: &fields.Group,
			ExpectedDigest: &fields.ExpectedDigest, ExpectedIdentity: fields.ExpectedIdentity,
		}
	case mutateConfigureGroupVariant:
		var fields configureGroupFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*mutation = FilterMutation{
			Kind: MutateConfigureGroup, Group: &fields.Group,
			Policy: &fields.Policy, ExpectedIdentity: fields.ExpectedIdentity,
		}
	default:
		return unknownVariant("FilterMutation", name)
	}
	return nil
}

// FilterMutationRequest is a catalog mutation with the caller's idempotency
// key. A retry with the same operation id returns the first attempt's outcome.
type FilterMutationRequest struct {
	V           uint32         `cbor:"v"`
	OperationID U128           `cbor:"operation_id"`
	Mutation    FilterMutation `cbor:"mutation"`
}

// FilterCatalogLimits are operational limits captured when the backend accepts
// a catalog command. Zero disables a limit, and older commands keep the
// original defaults on replay.
type FilterCatalogLimits struct {
	MaxDefinitions     uint32 `cbor:"max_definitions"`
	MaxTombstones      uint32 `cbor:"max_tombstones"`
	MaxRevisions       uint32 `cbor:"max_revisions"`
	MaxGroupIdentities uint32 `cbor:"max_group_identities"`
	// MaxCatalogBytes bounds the encoded bytes of every stored revision
	// together, which the count limits alone would let grow to gigabytes.
	MaxCatalogBytes uint64 `cbor:"max_catalog_bytes"`
}

// DefaultFilterCatalogLimits are the limits a catalog applies when a command
// names none.
func DefaultFilterCatalogLimits() FilterCatalogLimits {
	return FilterCatalogLimits{
		MaxDefinitions:     1_024,
		MaxTombstones:      4_096,
		MaxRevisions:       256,
		MaxGroupIdentities: 4_096,
		MaxCatalogBytes:    64 * 1024 * 1024,
	}
}

// UnmarshalCBOR decodes limits with the catalog byte cap defaulting, so a
// command written before that cap existed keeps a real bound rather than zero,
// which would disable it.
func (limits *FilterCatalogLimits) UnmarshalCBOR(item cborform.Value) error {
	type plain FilterCatalogLimits
	decoded := plain{MaxCatalogBytes: DefaultFilterCatalogLimits().MaxCatalogBytes}
	if err := fromValueInto(item, &decoded); err != nil {
		return err
	}
	*limits = FilterCatalogLimits(decoded)
	return nil
}

// FilterCatalogCommand is the mutation the catalog appends to its control log.
//
// The streaming server stamps, for a bind or unbind, the group's verified
// identity. The backend then stamps the caller's grants and,
// for a registration, the allocated filter id. Applying it is deterministic,
// so every replica and every replay records the same outcome.
type FilterCatalogCommand struct {
	OperationID U128                 `cbor:"operation_id"`
	Mutation    FilterMutation       `cbor:"mutation"`
	Identity    *FilterGroupIdentity `cbor:"identity,omitempty"`
	// FilterID is the id a registration takes, allocated before the append so
	// a replay of any part of the log assigns the same id.
	FilterID *uint32 `cbor:"filter_id,omitempty"`
	// Grants are re-checked when the mutation applies, against the catalog as
	// it stands at that point of the log.
	Grants []Grant              `cbor:"grants,omitempty"`
	Limits *FilterCatalogLimits `cbor:"limits,omitempty"`
}

// FilterRevisionRef is a saved revision, by identity and digest.
type FilterRevisionRef struct {
	FilterID uint32   `cbor:"filter_id"`
	Revision uint32   `cbor:"revision"`
	Digest   Digest32 `cbor:"digest"`
}

// FilterMutationResultKind discriminates what an applied mutation produced.
type FilterMutationResultKind uint8

const (
	// ResultRevisionState means the revision now accepts or refuses new reads.
	ResultRevisionState FilterMutationResultKind = iota
	// ResultRegistered is the first immutable revision of a new filter.
	ResultRegistered
	// ResultRevised is the newly appended immutable revision.
	ResultRevised
	ResultDescribed
	ResultArchived
	// ResultDropped means the filter was dropped and its name remains
	// reserved.
	ResultDropped
	// ResultBound is the exact group identity and immutable policy binding.
	ResultBound
	// ResultUnbound is the exact binding that was released.
	ResultUnbound
)

// FilterMutationResult is what an applied mutation produced.
type FilterMutationResult struct {
	Kind        FilterMutationResultKind
	FilterID    uint32
	Revision    uint32
	Enabled     bool
	RevisionRef *FilterRevisionRef
	Binding     *FilterBinding
}

// The externally tagged, snake_case result names.
const (
	resultRevisionStateVariant = "revision_state"
	resultRegisteredVariant    = "registered"
	resultRevisedVariant       = "revised"
	resultDescribedVariant     = "described"
	resultArchivedVariant      = "archived"
	resultDroppedVariant       = "dropped"
	resultBoundVariant         = "bound"
	resultUnboundVariant       = "unbound"
)

// MarshalCBOR writes the externally tagged result.
func (result FilterMutationResult) MarshalCBOR() (cborform.Value, error) {
	switch result.Kind {
	case ResultRevisionState:
		return marshalVariantStruct(resultRevisionStateVariant, setRevisionEnabledFields{
			FilterID: result.FilterID, Revision: result.Revision, Enabled: result.Enabled,
		})
	case ResultRegistered, ResultRevised:
		if result.RevisionRef == nil {
			return nil, fmt.Errorf("wire: a revision result carries no reference")
		}
		name := resultRegisteredVariant
		if result.Kind == ResultRevised {
			name = resultRevisedVariant
		}
		return marshalVariantStruct(name, *result.RevisionRef)
	case ResultDescribed, ResultArchived, ResultDropped:
		name := resultDescribedVariant
		switch result.Kind {
		case ResultArchived:
			name = resultArchivedVariant
		case ResultDropped:
			name = resultDroppedVariant
		}
		return marshalVariantStruct(name, filterIDFields{FilterID: result.FilterID})
	case ResultBound, ResultUnbound:
		if result.Binding == nil {
			return nil, fmt.Errorf("wire: a binding result carries no binding")
		}
		name := resultBoundVariant
		if result.Kind == ResultUnbound {
			name = resultUnboundVariant
		}
		return marshalVariantStruct(name, *result.Binding)
	default:
		return nil, fmt.Errorf("wire: unknown mutation result %d", result.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged result.
func (result *FilterMutationResult) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("FilterMutationResult", name, payload); err != nil {
		return err
	}
	switch name {
	case resultRevisionStateVariant:
		var fields setRevisionEnabledFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*result = FilterMutationResult{
			Kind: ResultRevisionState, FilterID: fields.FilterID,
			Revision: fields.Revision, Enabled: fields.Enabled,
		}
	case resultRegisteredVariant, resultRevisedVariant:
		var ref FilterRevisionRef
		if err := fromValueInto(payload, &ref); err != nil {
			return err
		}
		kind := ResultRegistered
		if name == resultRevisedVariant {
			kind = ResultRevised
		}
		*result = FilterMutationResult{Kind: kind, RevisionRef: &ref,
			FilterID: ref.FilterID, Revision: ref.Revision}
	case resultDescribedVariant, resultArchivedVariant, resultDroppedVariant:
		var fields filterIDFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		kind := ResultDescribed
		switch name {
		case resultArchivedVariant:
			kind = ResultArchived
		case resultDroppedVariant:
			kind = ResultDropped
		}
		*result = FilterMutationResult{Kind: kind, FilterID: fields.FilterID}
	case resultBoundVariant, resultUnboundVariant:
		var binding FilterBinding
		if err := fromValueInto(payload, &binding); err != nil {
			return err
		}
		kind := ResultBound
		if name == resultUnboundVariant {
			kind = ResultUnbound
		}
		*result = FilterMutationResult{Kind: kind, Binding: &binding}
	default:
		return unknownVariant("FilterMutationResult", name)
	}
	return nil
}

// FilterMutationStatusKind is where a mutation stands.
type FilterMutationStatusKind uint8

const (
	// StatusPending means it entered the control log but has not been applied
	// yet: read its outcome later by operation id.
	StatusPending FilterMutationStatusKind = iota
	StatusApplied
	StatusRejected
)

// FilterMutationStatus is where a mutation stands.
type FilterMutationStatus struct {
	Kind     FilterMutationStatusKind
	Applied  *FilterMutationResult
	Rejected *FilterError
}

const (
	statusPendingVariant  = "pending"
	statusAppliedVariant  = "applied"
	statusRejectedVariant = "rejected"
)

// MarshalCBOR writes the externally tagged status.
func (status FilterMutationStatus) MarshalCBOR() (cborform.Value, error) {
	switch status.Kind {
	case StatusPending:
		return unitVariant(statusPendingVariant), nil
	case StatusApplied:
		if status.Applied == nil {
			return nil, fmt.Errorf("wire: an applied status carries no result")
		}
		payload, err := status.Applied.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(statusAppliedVariant, payload), nil
	case StatusRejected:
		if status.Rejected == nil {
			return nil, fmt.Errorf("wire: a rejected status carries no error")
		}
		payload, err := toValueOf(*status.Rejected)
		if err != nil {
			return nil, err
		}
		return taggedVariant(statusRejectedVariant, payload), nil
	default:
		return nil, fmt.Errorf("wire: unknown mutation status %d", status.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged status.
func (status *FilterMutationStatus) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	switch name {
	case statusPendingVariant:
		*status = FilterMutationStatus{Kind: StatusPending}
		return nil
	case statusAppliedVariant:
		if err := requirePayload("FilterMutationStatus", name, payload); err != nil {
			return err
		}
		var result FilterMutationResult
		if err := result.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*status = FilterMutationStatus{Kind: StatusApplied, Applied: &result}
		return nil
	case statusRejectedVariant:
		if err := requirePayload("FilterMutationStatus", name, payload); err != nil {
			return err
		}
		var failure FilterError
		if err := fromValueInto(payload, &failure); err != nil {
			return err
		}
		*status = FilterMutationStatus{Kind: StatusRejected, Rejected: &failure}
		return nil
	default:
		return unknownVariant("FilterMutationStatus", name)
	}
}

// FilterMutationOutcome is the authoritative outcome of one mutation.
type FilterMutationOutcome struct {
	V           uint32               `cbor:"v"`
	OperationID U128                 `cbor:"operation_id"`
	Status      FilterMutationStatus `cbor:"status"`
	// CatalogPosition is where the fold applied the mutation, absent while it
	// is pending.
	CatalogPosition *CatalogPosition `cbor:"catalog_position,omitempty"`
}

// The catalog read requests.
type (
	// GetFilter reads one saved filter.
	GetFilter struct {
		V        uint32 `cbor:"v"`
		FilterID uint32 `cbor:"filter_id"`
	}

	// ListFilters lists saved filters, newest first by id.
	ListFilters struct {
		V            uint32       `cbor:"v"`
		NameContains *string      `cbor:"name_contains,omitempty"`
		State        *FilterState `cbor:"state,omitempty"`
		// Owner keeps only the own filter of this consumer group
		// incarnation, bound or not.
		Owner *FilterGroupIdentity `cbor:"owner,omitempty"`
		// Page is 1-based. Zero reads the first page too.
		Page uint32 `cbor:"page"`
		// Results is the page length.
		Results uint32 `cbor:"results"`
	}

	// ListFilterRevisions lists one filter's revisions, newest first.
	ListFilterRevisions struct {
		V        uint32 `cbor:"v"`
		FilterID uint32 `cbor:"filter_id"`
		Page     uint32 `cbor:"page"`
		Results  uint32 `cbor:"results"`
	}

	// GetFilterBinding reads one consumer group's binding. The streaming
	// server stamps the identity from the names before forwarding.
	GetFilterBinding struct {
		V        uint32               `cbor:"v"`
		Group    FilterGroupRef       `cbor:"group"`
		Identity *FilterGroupIdentity `cbor:"identity,omitempty"`
	}

	// ListFilterBindings lists bindings, optionally narrowed.
	ListFilterBindings struct {
		V        uint32  `cbor:"v"`
		FilterID *uint32 `cbor:"filter_id,omitempty"`
		Stream   *string `cbor:"stream,omitempty"`
		Topic    *string `cbor:"topic,omitempty"`
		Page     uint32  `cbor:"page"`
		Results  uint32  `cbor:"results"`
	}

	// GetFilterOperation reads a mutation's outcome by its operation id.
	GetFilterOperation struct {
		V           uint32 `cbor:"v"`
		OperationID U128   `cbor:"operation_id"`
	}

	// WatchFilterCatalog waits until the catalog version differs, or the wait
	// elapses. A zero wait confirms the current version before a cached read.
	WatchFilterCatalog struct {
		V         uint32 `cbor:"v"`
		Since     uint64 `cbor:"since"`
		MaxWaitMs uint32 `cbor:"max_wait_ms"`
	}
)

// The catalog pages.
type (
	// FilterPage is a page of saved filters.
	FilterPage struct {
		Items        []FilterSummary `cbor:"items"`
		Page         uint32          `cbor:"page"`
		TotalPages   uint32          `cbor:"total_pages"`
		TotalResults uint32          `cbor:"total_results"`
	}

	// FilterRevisionPage is a page of revisions.
	FilterRevisionPage struct {
		FilterID     uint32               `cbor:"filter_id"`
		Items        []FilterRevisionInfo `cbor:"items"`
		Page         uint32               `cbor:"page"`
		TotalPages   uint32               `cbor:"total_pages"`
		TotalResults uint32               `cbor:"total_results"`
	}

	// FilterBindingPage is a page of bindings.
	FilterBindingPage struct {
		Items        []FilterBinding `cbor:"items"`
		Page         uint32          `cbor:"page"`
		TotalPages   uint32          `cbor:"total_pages"`
		TotalResults uint32          `cbor:"total_results"`
	}
)

// FilterPolicyRefKind discriminates the policy a filtered read resolves.
type FilterPolicyRefKind uint8

const (
	PolicyByRevision FilterPolicyRefKind = iota
	PolicyByBinding
)

// FilterPolicyRef is the policy a filtered read resolves: a saved revision, or
// a group binding.
type FilterPolicyRef struct {
	Kind     FilterPolicyRefKind
	FilterID uint32
	Revision uint32
	Identity *FilterGroupIdentity
}

const (
	policyRevisionVariant = "revision"
	policyBindingVariant  = "binding"
)

// MarshalCBOR writes the externally tagged reference.
func (ref FilterPolicyRef) MarshalCBOR() (cborform.Value, error) {
	switch ref.Kind {
	case PolicyByRevision:
		return marshalVariantStruct(policyRevisionVariant, revisionRefFields{
			FilterID: ref.FilterID, Revision: ref.Revision,
		})
	case PolicyByBinding:
		if ref.Identity == nil {
			return nil, fmt.Errorf("wire: a binding policy names no group identity")
		}
		return marshalVariantStruct(policyBindingVariant, *ref.Identity)
	default:
		return nil, fmt.Errorf("wire: unknown filter policy reference %d", ref.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged reference.
func (ref *FilterPolicyRef) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("FilterPolicyRef", name, payload); err != nil {
		return err
	}
	switch name {
	case policyRevisionVariant:
		var fields revisionRefFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*ref = FilterPolicyRef{
			Kind: PolicyByRevision, FilterID: fields.FilterID, Revision: fields.Revision,
		}
		return nil
	case policyBindingVariant:
		var identity FilterGroupIdentity
		if err := fromValueInto(payload, &identity); err != nil {
			return err
		}
		*ref = FilterPolicyRef{Kind: PolicyByBinding, Identity: &identity}
		return nil
	default:
		return unknownVariant("FilterPolicyRef", name)
	}
}

// ResolveFilterPolicy resolves a read's execution policy.
type ResolveFilterPolicy struct {
	AllowDisabled bool            `cbor:"allow_disabled"`
	V             uint32          `cbor:"v"`
	Policy        FilterPolicyRef `cbor:"policy"`
	// AllowUnbound lets a binding lookup answer unbound for a group without a
	// binding. Without it the lookup fails as not found, the strict behaviour.
	AllowUnbound bool `cbor:"allow_unbound"`
	// MinCatalogPosition answers only once the backend has examined the
	// control log through this position.
	MinCatalogPosition *CatalogPosition `cbor:"min_catalog_position,omitempty"`
	// ConfirmHead answers only once the backend has examined the control log
	// through the head it reads from the control partition primary now.
	ConfirmHead bool `cbor:"confirm_head"`
}

// FilterCatalogVersion is an opaque catalog version. It changes on each
// control-log fold and starts with a random token on each backend process, to
// fence old caches.
type FilterCatalogVersion struct {
	V       uint32 `cbor:"v"`
	Version uint64 `cbor:"version"`
}

// ResolvedFilterPolicy is an execution policy: exactly one revision, with its
// lifecycle state.
type ResolvedFilterPolicy struct {
	FilterID uint32         `cbor:"filter_id"`
	Revision uint32         `cbor:"revision"`
	Digest   Digest32       `cbor:"digest"`
	State    FilterState    `cbor:"state"`
	Filter   ConsumerFilter `cbor:"filter"`
	// PolicyGeneration is the group's policy generation, zero for a revision
	// lookup.
	PolicyGeneration uint64 `cbor:"policy_generation"`
}

// FilterCatalogOutcomeKind discriminates a successful catalog reply.
type FilterCatalogOutcomeKind uint8

const (
	CatalogFilter FilterCatalogOutcomeKind = iota
	CatalogFilters
	CatalogRevisions
	CatalogBinding
	CatalogBindings
	CatalogMutation
	CatalogPolicy
	// CatalogUnbound means the group exists and has no policy. It is answered
	// only to a binding lookup that allowed it.
	CatalogUnbound
)

// FilterCatalogOutcome is a successful catalog reply.
type FilterCatalogOutcome struct {
	Kind      FilterCatalogOutcomeKind
	Filter    *FilterDetail
	Filters   *FilterPage
	Revisions *FilterRevisionPage
	Binding   *FilterBinding
	Bindings  *FilterBindingPage
	Mutation  *FilterMutationOutcome
	Policy    *ResolvedFilterPolicy
	Unbound   *GroupPolicyUnbound
}

// The externally tagged, snake_case catalog outcome names.
const (
	catalogFilterVariant    = "filter"
	catalogFiltersVariant   = "filters"
	catalogRevisionsVariant = "revisions"
	catalogBindingVariant   = "binding"
	catalogBindingsVariant  = "bindings"
	catalogMutationVariant  = "mutation"
	catalogPolicyVariant    = "policy"
	catalogUnboundVariant   = "unbound"
)

// MarshalCBOR writes the externally tagged outcome.
func (outcome FilterCatalogOutcome) MarshalCBOR() (cborform.Value, error) {
	switch outcome.Kind {
	case CatalogFilter:
		if outcome.Filter == nil {
			return nil, fmt.Errorf("wire: a filter outcome carries no detail")
		}
		return marshalVariantStruct(catalogFilterVariant, *outcome.Filter)
	case CatalogFilters:
		if outcome.Filters == nil {
			return nil, fmt.Errorf("wire: a filters outcome carries no page")
		}
		return marshalVariantStruct(catalogFiltersVariant, *outcome.Filters)
	case CatalogRevisions:
		if outcome.Revisions == nil {
			return nil, fmt.Errorf("wire: a revisions outcome carries no page")
		}
		return marshalVariantStruct(catalogRevisionsVariant, *outcome.Revisions)
	case CatalogBinding:
		if outcome.Binding == nil {
			return nil, fmt.Errorf("wire: a binding outcome carries no binding")
		}
		return marshalVariantStruct(catalogBindingVariant, *outcome.Binding)
	case CatalogBindings:
		if outcome.Bindings == nil {
			return nil, fmt.Errorf("wire: a bindings outcome carries no page")
		}
		return marshalVariantStruct(catalogBindingsVariant, *outcome.Bindings)
	case CatalogMutation:
		if outcome.Mutation == nil {
			return nil, fmt.Errorf("wire: a mutation outcome carries no outcome")
		}
		return marshalVariantStruct(catalogMutationVariant, *outcome.Mutation)
	case CatalogPolicy:
		if outcome.Policy == nil {
			return nil, fmt.Errorf("wire: a policy outcome carries no policy")
		}
		return marshalVariantStruct(catalogPolicyVariant, *outcome.Policy)
	case CatalogUnbound:
		if outcome.Unbound == nil {
			return nil, fmt.Errorf("wire: an unbound outcome carries no group")
		}
		return marshalVariantStruct(catalogUnboundVariant, *outcome.Unbound)
	default:
		return nil, fmt.Errorf("wire: unknown catalog outcome %d", outcome.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged outcome.
func (outcome *FilterCatalogOutcome) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("FilterCatalogOutcome", name, payload); err != nil {
		return err
	}
	switch name {
	case catalogFilterVariant:
		var detail FilterDetail
		if err := fromValueInto(payload, &detail); err != nil {
			return err
		}
		*outcome = FilterCatalogOutcome{Kind: CatalogFilter, Filter: &detail}
	case catalogFiltersVariant:
		var page FilterPage
		if err := fromValueInto(payload, &page); err != nil {
			return err
		}
		*outcome = FilterCatalogOutcome{Kind: CatalogFilters, Filters: &page}
	case catalogRevisionsVariant:
		var page FilterRevisionPage
		if err := fromValueInto(payload, &page); err != nil {
			return err
		}
		*outcome = FilterCatalogOutcome{Kind: CatalogRevisions, Revisions: &page}
	case catalogBindingVariant:
		var binding FilterBinding
		if err := fromValueInto(payload, &binding); err != nil {
			return err
		}
		*outcome = FilterCatalogOutcome{Kind: CatalogBinding, Binding: &binding}
	case catalogBindingsVariant:
		var page FilterBindingPage
		if err := fromValueInto(payload, &page); err != nil {
			return err
		}
		*outcome = FilterCatalogOutcome{Kind: CatalogBindings, Bindings: &page}
	case catalogMutationVariant:
		var mutation FilterMutationOutcome
		if err := fromValueInto(payload, &mutation); err != nil {
			return err
		}
		*outcome = FilterCatalogOutcome{Kind: CatalogMutation, Mutation: &mutation}
	case catalogPolicyVariant:
		var policy ResolvedFilterPolicy
		if err := fromValueInto(payload, &policy); err != nil {
			return err
		}
		*outcome = FilterCatalogOutcome{Kind: CatalogPolicy, Policy: &policy}
	case catalogUnboundVariant:
		var unbound GroupPolicyUnbound
		if err := fromValueInto(payload, &unbound); err != nil {
			return err
		}
		*outcome = FilterCatalogOutcome{Kind: CatalogUnbound, Unbound: &unbound}
	default:
		return unknownVariant("FilterCatalogOutcome", name)
	}
	return nil
}

// FilterCatalogReply is the reply to every catalog command.
type FilterCatalogReply struct {
	Err     *FilterError
	Outcome *FilterCatalogOutcome
}

// IsOk reports a successful reply.
func (reply FilterCatalogReply) IsOk() bool {
	return reply.Err == nil && reply.Outcome != nil
}

// RegisteredDigest is the digest of a newly registered or revised filter, when
// this reply carries one.
func (reply FilterCatalogReply) RegisteredDigest() *Digest32 {
	if !reply.IsOk() || reply.Outcome.Kind != CatalogMutation {
		return nil
	}
	status := reply.Outcome.Mutation.Status
	if status.Kind != StatusApplied || status.Applied.RevisionRef == nil {
		return nil
	}
	return &status.Applied.RevisionRef.Digest
}

// MarshalCBOR writes the externally tagged reply.
func (reply FilterCatalogReply) MarshalCBOR() (cborform.Value, error) {
	switch {
	case reply.Err != nil && reply.Outcome != nil:
		return nil, fmt.Errorf("wire: a catalog reply is either an outcome or an error")
	case reply.Err != nil:
		payload, err := toValueOf(*reply.Err)
		if err != nil {
			return nil, err
		}
		return taggedVariant(filterErrVariant, payload), nil
	case reply.Outcome != nil:
		payload, err := reply.Outcome.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(filterOkVariant, payload), nil
	default:
		return nil, fmt.Errorf("wire: an empty catalog reply")
	}
}

// UnmarshalCBOR reads the externally tagged reply.
func (reply *FilterCatalogReply) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("FilterCatalogReply", name, payload); err != nil {
		return err
	}
	switch name {
	case filterOkVariant:
		var outcome FilterCatalogOutcome
		if err := outcome.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = FilterCatalogReply{Outcome: &outcome}
		return nil
	case filterErrVariant:
		var failure FilterError
		if err := fromValueInto(payload, &failure); err != nil {
			return err
		}
		*reply = FilterCatalogReply{Err: &failure}
		return nil
	default:
		return unknownVariant("FilterCatalogReply", name)
	}
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *FilterState) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "filter state", FilterActive, FilterArchived, FilterDropped)
}

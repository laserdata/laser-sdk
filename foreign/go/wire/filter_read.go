package wire

import (
	"fmt"
	"math"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// CatalogPosition is a position in the catalog's control log: the partition
// and offset of the record whose fold produced an outcome.
//
// A group read may require the backend to have examined the log through this
// position before answering, which gives the application that configured the
// group read-your-writes through any node.
type CatalogPosition struct {
	PartitionID uint32 `cbor:"partition_id"`
	Offset      uint64 `cbor:"offset"`
	// OperationID is the durable operation whose outcome proves this position
	// was folded, which keeps read-your-writes valid when the control log is
	// recreated.
	OperationID *U128 `cbor:"operation_id,omitempty"`
	// Revision is how many catalog commands the catalog had applied when this
	// outcome was folded. It keeps counting across a recreated control log,
	// so it orders outcomes whose offsets restarted.
	Revision *uint64 `cbor:"revision,omitempty"`
}

// FilterSource is the stream and topic a filtered read or preview addresses.
type FilterSource struct {
	Stream string `cbor:"stream"`
	Topic  string `cbor:"topic"`
}

// FilterConsumerKind discriminates whose progress a filtered read follows.
type FilterConsumerKind string

const (
	// ConsumerIndependent follows an independent consumer by name.
	ConsumerIndependent FilterConsumerKind = "consumer"
	// ConsumerGroupName follows a consumer group by name.
	ConsumerGroupName FilterConsumerKind = "group"
	// ConsumerGroupID follows a consumer group by numeric id.
	ConsumerGroupID FilterConsumerKind = "group_id"
)

// FilterConsumer is whose progress a filtered read follows. It is internally
// tagged, and a group accepts either a name or a numeric id.
type FilterConsumer struct {
	Kind FilterConsumerKind
	Name string
	ID   uint64
}

// IndependentConsumer follows an independent consumer.
func IndependentConsumer(name string) FilterConsumer {
	return FilterConsumer{Kind: ConsumerIndependent, Name: name}
}

// NamedGroup follows a consumer group by name.
func NamedGroup(name string) FilterConsumer {
	return FilterConsumer{Kind: ConsumerGroupName, Name: name}
}

// GroupByID follows a consumer group by numeric id.
func GroupByID(id uint64) FilterConsumer {
	return FilterConsumer{Kind: ConsumerGroupID, ID: id}
}

// IsGroup reports whether the read follows a consumer group.
func (consumer FilterConsumer) IsGroup() bool {
	return consumer.Kind == ConsumerGroupName || consumer.Kind == ConsumerGroupID
}

// String renders the selector for display.
func (consumer FilterConsumer) String() string {
	if consumer.Kind == ConsumerGroupID {
		return fmt.Sprintf("%d", consumer.ID)
	}
	return consumer.Name
}

type namedConsumerFields struct {
	Name string `cbor:"name"`
}

type idConsumerFields struct {
	ID uint64 `cbor:"id"`
}

// MarshalCBOR writes the internally tagged selector.
func (consumer FilterConsumer) MarshalCBOR() (cborform.Value, error) {
	switch consumer.Kind {
	case ConsumerIndependent, ConsumerGroupName:
		return marshalInternallyTagged(string(consumer.Kind),
			namedConsumerFields{Name: consumer.Name})
	case ConsumerGroupID:
		return marshalInternallyTagged(string(ConsumerGroupID),
			idConsumerFields{ID: consumer.ID})
	default:
		return nil, fmt.Errorf("wire: unknown filter consumer %q", string(consumer.Kind))
	}
}

// UnmarshalCBOR reads the internally tagged selector.
func (consumer *FilterConsumer) UnmarshalCBOR(item cborform.Value) error {
	kind, entries, err := readInternalTag(item)
	if err != nil {
		return err
	}
	switch FilterConsumerKind(kind) {
	case ConsumerIndependent, ConsumerGroupName:
		var fields namedConsumerFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		*consumer = FilterConsumer{Kind: FilterConsumerKind(kind), Name: fields.Name}
		return nil
	case ConsumerGroupID:
		var fields idConsumerFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		*consumer = GroupByID(fields.ID)
		return nil
	default:
		return fmt.Errorf("wire: unknown filter consumer %q", kind)
	}
}

// FilterRefKind discriminates which filter a read executes.
type FilterRefKind uint8

const (
	// RefInline carries the full filter in the request, needing no catalog. A
	// consumer group still needs a binding with the same digest.
	RefInline FilterRefKind = iota
	// RefRevision names a saved revision from the catalog.
	RefRevision
	// RefBound runs whatever revision the group is bound to. Groups only, so an
	// unbound group is refused.
	RefBound
	// RefGroup runs whatever policy the group has: a bound group is filtered
	// by its revision, an unbound group receives every record. It needs a
	// server advertising group policy reads.
	RefGroup
)

type revisionRefFields struct {
	FilterID uint32 `cbor:"filter_id"`
	Revision uint32 `cbor:"revision"`
}

// FilterRef is which filter a read executes.
type FilterRef struct {
	Kind     FilterRefKind
	Inline   *ConsumerFilter
	FilterID uint32
	Revision uint32
}

// InlineFilter carries the full filter in the request.
func InlineFilter(filter ConsumerFilter) FilterRef {
	return FilterRef{Kind: RefInline, Inline: &filter}
}

// SavedRevision names a saved revision.
func SavedRevision(filterID, revision uint32) FilterRef {
	return FilterRef{Kind: RefRevision, FilterID: filterID, Revision: revision}
}

// BoundRevision runs whatever revision the group is bound to.
func BoundRevision() FilterRef { return FilterRef{Kind: RefBound} }

// GroupPolicy runs whatever policy the group has.
func GroupPolicy() FilterRef { return FilterRef{Kind: RefGroup} }

// The externally tagged, snake_case filter reference names.
const (
	refInlineVariant   = "inline"
	refRevisionVariant = "revision"
	refBoundVariant    = "bound"
	refGroupVariant    = "group"
)

// MarshalCBOR writes the externally tagged reference.
func (ref FilterRef) MarshalCBOR() (cborform.Value, error) {
	switch ref.Kind {
	case RefInline:
		if ref.Inline == nil {
			return nil, fmt.Errorf("wire: an inline reference carries no filter")
		}
		return marshalVariantStruct(refInlineVariant, *ref.Inline)
	case RefRevision:
		return marshalVariantStruct(refRevisionVariant, revisionRefFields{
			FilterID: ref.FilterID, Revision: ref.Revision,
		})
	case RefBound:
		return unitVariant(refBoundVariant), nil
	case RefGroup:
		return unitVariant(refGroupVariant), nil
	default:
		return nil, fmt.Errorf("wire: unknown filter reference %d", ref.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged reference.
func (ref *FilterRef) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	switch name {
	case refInlineVariant:
		if err := requirePayload("FilterRef", name, payload); err != nil {
			return err
		}
		var filter ConsumerFilter
		if err := fromValueInto(payload, &filter); err != nil {
			return err
		}
		*ref = FilterRef{Kind: RefInline, Inline: &filter}
		return nil
	case refRevisionVariant:
		if err := requirePayload("FilterRef", name, payload); err != nil {
			return err
		}
		var fields revisionRefFields
		if err := fromValueInto(payload, &fields); err != nil {
			return err
		}
		*ref = SavedRevision(fields.FilterID, fields.Revision)
		return nil
	case refBoundVariant:
		*ref = BoundRevision()
		return nil
	case refGroupVariant:
		*ref = GroupPolicy()
		return nil
	default:
		return unknownVariant("FilterRef", name)
	}
}

// ExecutionMode is how the server selected the records of a page.
//
// Unfiltered is the explicit policy of a group without a binding, never a
// fallback: it carries no digest and the page holds every record the owner
// read returned.
type ExecutionMode string

const (
	// ModeFiltered is the default.
	ModeFiltered   ExecutionMode = "filtered"
	ModeUnfiltered ExecutionMode = "unfiltered"
)

// IsFiltered reports the default mode.
func (mode ExecutionMode) IsFiltered() bool {
	return mode == ModeFiltered || mode == ""
}

// validatePolicyIdentity enforces that a filtered policy names its digest and
// an unfiltered one carries none. Without this a page could claim to be
// filtered while naming no filter.
func validatePolicyIdentity(mode ExecutionMode, digest *Digest32) error {
	switch {
	case mode.IsFiltered() && digest != nil:
		return digest.Validate()
	case mode.IsFiltered():
		return &InvalidError{Reason: "a filtered policy names its digest"}
	case digest != nil:
		return &InvalidError{Reason: "an unfiltered policy carries no digest"}
	default:
		return nil
	}
}

// SourceGeneration is the exact source history a page was read from.
//
// A recreated stream, topic or partition changes it, so a continuation or
// acknowledgment from an older history is rejected rather than applied to new
// records.
type SourceGeneration struct {
	StreamID              uint32 `cbor:"stream_id"`
	StreamCreatedAtMicros uint64 `cbor:"stream_created_at_micros"`
	TopicID               uint32 `cbor:"topic_id"`
	TopicCreatedAtMicros  uint64 `cbor:"topic_created_at_micros"`
	PartitionID           uint32 `cbor:"partition_id"`
	PartitionIncarnation  uint64 `cbor:"partition_incarnation"`
}

// AckContext is the partition owner a page was read under.
//
// The reader stores the page's offset with it and the page's partition
// incarnation, so the store is refused once the partition was recreated or its
// group assignment moved.
type AckContext struct {
	// OwnerGeneration is the consumer group owner generation, zero for an
	// independent consumer.
	OwnerGeneration uint64 `cbor:"owner_generation"`
	// MetadataOp is the metadata operation the serving node had applied when
	// it read the page.
	MetadataOp uint64 `cbor:"metadata_op"`
}

// Continuation is the resume point a page hands to the next request.
type Continuation struct {
	GroupID        *uint64          `cbor:"group_id,omitempty"`
	NextScanOffset uint64           `cbor:"next_scan_offset"`
	Generation     SourceGeneration `cbor:"generation"`
	// Digest is the filter the page ran, absent for an unfiltered page.
	Digest *Digest32     `cbor:"digest,omitempty"`
	Mode   ExecutionMode `cbor:"mode"`
	// PolicyGeneration is the group's policy generation the page ran under,
	// zero for an independent consumer.
	PolicyGeneration uint64 `cbor:"policy_generation"`
}

// FilteredStartKind discriminates where a filtered read starts.
type FilteredStartKind uint8

const (
	// StartNext resumes after the consumer's stored offset.
	StartNext FilteredStartKind = iota
	StartFirst
	StartLast
	StartAtOffset
	// StartAtTimestamp reads records at or after a broker timestamp.
	StartAtTimestamp
	// StartContinue resumes exactly where the previous page stopped.
	StartContinue
)

// FilteredStart is where a filtered read starts.
type FilteredStart struct {
	Kind         FilteredStartKind
	Offset       uint64
	Timestamp    uint64
	Continuation *Continuation
}

// The start constructors.
func StartAtNext() FilteredStart  { return FilteredStart{Kind: StartNext} }
func StartAtFirst() FilteredStart { return FilteredStart{Kind: StartFirst} }
func StartAtLast() FilteredStart  { return FilteredStart{Kind: StartLast} }
func StartFromOffset(offset uint64) FilteredStart {
	return FilteredStart{Kind: StartAtOffset, Offset: offset}
}
func StartFromTimestamp(micros uint64) FilteredStart {
	return FilteredStart{Kind: StartAtTimestamp, Timestamp: micros}
}
func StartFromContinuation(continuation Continuation) FilteredStart {
	return FilteredStart{Kind: StartContinue, Continuation: &continuation}
}

// The externally tagged, snake_case start names.
const (
	startNextVariant      = "next"
	startFirstVariant     = "first"
	startLastVariant      = "last"
	startOffsetVariant    = "offset"
	startTimestampVariant = "timestamp"
	startContinueVariant  = "continue"
)

// MarshalCBOR writes the externally tagged start.
func (start FilteredStart) MarshalCBOR() (cborform.Value, error) {
	switch start.Kind {
	case StartNext:
		return unitVariant(startNextVariant), nil
	case StartFirst:
		return unitVariant(startFirstVariant), nil
	case StartLast:
		return unitVariant(startLastVariant), nil
	case StartAtOffset:
		return taggedVariant(startOffsetVariant, cborform.Uint(start.Offset)), nil
	case StartAtTimestamp:
		return taggedVariant(startTimestampVariant, cborform.Uint(start.Timestamp)), nil
	case StartContinue:
		if start.Continuation == nil {
			return nil, fmt.Errorf("wire: a continue start carries no continuation")
		}
		return marshalVariantStruct(startContinueVariant, *start.Continuation)
	default:
		return nil, fmt.Errorf("wire: unknown filtered start %d", start.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged start.
func (start *FilteredStart) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	switch name {
	case startNextVariant:
		*start = StartAtNext()
		return nil
	case startFirstVariant:
		*start = StartAtFirst()
		return nil
	case startLastVariant:
		*start = StartAtLast()
		return nil
	case startOffsetVariant, startTimestampVariant:
		if err := requirePayload("FilteredStart", name, payload); err != nil {
			return err
		}
		number, ok := payload.(cborform.Uint)
		if !ok {
			return fmt.Errorf("wire: %q carries an integer, got %T", name, payload)
		}
		if name == startOffsetVariant {
			*start = StartFromOffset(uint64(number))
		} else {
			*start = StartFromTimestamp(uint64(number))
		}
		return nil
	case startContinueVariant:
		if err := requirePayload("FilteredStart", name, payload); err != nil {
			return err
		}
		var continuation Continuation
		if err := fromValueInto(payload, &continuation); err != nil {
			return err
		}
		*start = StartFromContinuation(continuation)
		return nil
	default:
		return unknownVariant("FilteredStart", name)
	}
}

// FilteredPollRequest reads one bounded page of matching records from one
// partition.
type FilteredPollRequest struct {
	V           uint32         `cbor:"v"`
	Source      FilterSource   `cbor:"source"`
	PartitionID uint32         `cbor:"partition_id"`
	Consumer    FilterConsumer `cbor:"consumer"`
	Filter      FilterRef      `cbor:"filter"`
	Start       FilteredStart  `cbor:"start"`
	// Count is the most matching records to return.
	Count uint32 `cbor:"count"`
	// MaxReplyBytes is the most record bytes to return. The server may lower it.
	MaxReplyBytes uint32 `cbor:"max_reply_bytes"`
	// MaxExamined is the most source records to examine. The server lowers it
	// to its own budget, and absent means the server budget alone. A normal
	// consumer sets it to the count, so a batch of 100 examines at most 100.
	MaxExamined *uint32 `cbor:"max_examined,omitempty"`
	// MinCatalogPosition serves only once the catalog has been examined
	// through this position, so a reader sees the configuration its
	// application just made.
	MinCatalogPosition *CatalogPosition `cbor:"min_catalog_position,omitempty"`
}

// StopReason is why a page stopped scanning.
type StopReason string

const (
	// StopFilled means the requested number of matches was reached.
	StopFilled StopReason = "filled"
	// StopBudget means a record, byte or round budget was spent. More records
	// may follow.
	StopBudget StopReason = "budget"
	// StopEndOfVisible means no more records are visible on the serving
	// replica right now.
	StopEndOfVisible StopReason = "end_of_visible"
	// StopFault means a record failed to decode under the stop fault policy.
	StopFault StopReason = "fault"
	// StopOversizedRecord means one matching record alone exceeds the reply cap.
	StopOversizedRecord StopReason = "oversized_record"
)

// FaultReason is why a record could not be evaluated.
type FaultReason string

const (
	// FaultMissingSchema means no valid schema header identifies the writer
	// schema.
	FaultMissingSchema FaultReason = "missing_schema"
	// FaultSchemaNotAllowed means the record schema is not permitted by this
	// filter.
	FaultSchemaNotAllowed FaultReason = "schema_not_allowed"
	// FaultSchemaMismatch means the registered schema family differs from the
	// payload codec.
	FaultSchemaMismatch FaultReason = "schema_mismatch"
	// FaultMalformed means the payload is not valid for the filter's codec.
	FaultMalformed FaultReason = "malformed"
	// FaultTooLarge means the payload exceeds the decoder's size limit.
	FaultTooLarge FaultReason = "too_large"
	// FaultTooDeep means the payload nests deeper than the depth limit.
	FaultTooDeep FaultReason = "too_deep"
	// FaultForeignCodec means the record's content-type header names another
	// codec than the filter's.
	FaultForeignCodec FaultReason = "foreign_codec"
	// FaultTypeMismatch means a predicate's value exists but has a type the
	// predicate cannot compare.
	FaultTypeMismatch FaultReason = "type_mismatch"
)

// IsForeign reports whether the record is in another format than the filter
// reads, which follows the filter's foreign policy rather than its fault
// policy.
func (reason FaultReason) IsForeign() bool {
	switch reason {
	case FaultForeignCodec, FaultMissingSchema, FaultSchemaNotAllowed, FaultSchemaMismatch:
		return true
	default:
		return false
	}
}

// RecordFault is one record the filter could not evaluate.
type RecordFault struct {
	Offset uint64      `cbor:"offset"`
	Reason FaultReason `cbor:"reason"`
}

// FilterDecodeLimits are the decoder limits the server applied to unevaluated
// records in a page.
type FilterDecodeLimits struct {
	MaxPayloadBytes uint32 `cbor:"max_payload_bytes"`
	MaxDepth        uint32 `cbor:"max_depth"`
}

// AppliedPolicy is the filter a page actually executed.
type AppliedPolicy struct {
	GroupID *uint64 `cbor:"group_id,omitempty"`
	// Digest is the filter that ran, absent for an unfiltered page.
	Digest   *Digest32     `cbor:"digest,omitempty"`
	FilterID *uint32       `cbor:"filter_id,omitempty"`
	Revision *uint32       `cbor:"revision,omitempty"`
	Mode     ExecutionMode `cbor:"mode"`
	// PolicyGeneration is the group's policy generation, zero for an
	// independent consumer.
	PolicyGeneration uint64 `cbor:"policy_generation"`
}

// FilteredPolicy is the policy an independent consumer or a bound group ran:
// one filter.
func FilteredPolicy(groupID *uint64, digest Digest32) AppliedPolicy {
	return AppliedPolicy{GroupID: groupID, Digest: &digest, Mode: ModeFiltered}
}

// UnfilteredPolicy is the policy of a group without a binding: every record.
func UnfilteredPolicy(groupID, policyGeneration uint64) AppliedPolicy {
	return AppliedPolicy{
		GroupID: &groupID, Mode: ModeUnfiltered, PolicyGeneration: policyGeneration,
	}
}

// Validate checks that the policy names a filter exactly when it is filtered,
// and that only a group runs unfiltered.
func (policy AppliedPolicy) Validate() error {
	if err := validatePolicyIdentity(policy.Mode, policy.Digest); err != nil {
		return err
	}
	if !policy.Mode.IsFiltered() {
		if policy.GroupID == nil {
			return &InvalidError{Reason: "only a consumer group runs unfiltered"}
		}
		if policy.FilterID != nil || policy.Revision != nil {
			return &InvalidError{Reason: "an unfiltered policy has no filter reference"}
		}
	}
	return nil
}

// FilteredPage is one page of a filtered read.
//
// Records is a standard polled-messages body: its head names the partition,
// the head offset, the record count and the partition context the page was
// served under, and the matching records keep their original offsets, ids,
// timestamps, headers and payload bytes.
type FilteredPage struct {
	V           uint32           `cbor:"v"`
	PartitionID uint32           `cbor:"partition_id"`
	Policy      AppliedPolicy    `cbor:"policy"`
	Generation  SourceGeneration `cbor:"generation"`
	AckContext  AckContext       `cbor:"ack_context"`
	// NextScanOffset is the first record the server has not examined, absent
	// when nothing was examined and the start selector must be sent again.
	NextScanOffset *uint64 `cbor:"next_scan_offset,omitempty"`
	// SafeAckOffset is the last examined offset. The reader may store it,
	// with the ack context, only after every returned record at or below it
	// is fully handled.
	SafeAckOffset *uint64 `cbor:"safe_ack_offset,omitempty"`
	// Frontier is the primary's partition head when the page was read. It is
	// reported, never used as scan progress.
	Frontier uint64       `cbor:"frontier"`
	Examined uint32       `cbor:"examined"`
	Matched  uint32       `cbor:"matched"`
	Stop     StopReason   `cbor:"stop"`
	Fault    *RecordFault `cbor:"fault,omitempty"`
	// Unevaluated are the returned records the filter could not evaluate,
	// under the pass fault policy, each with why it was not evaluated. A bare
	// offset would leave a reader unable to tell a malformed body from one
	// that outran the decode bounds.
	Unevaluated      []RecordFault       `cbor:"unevaluated,omitempty"`
	EvaluationLimits *FilterDecodeLimits `cbor:"evaluation_limits,omitempty"`
	Records          []byte              `cbor:"records"`
}

// NextStart is the start of the next request: continue after this page, or
// repeat the original selector when nothing was examined.
func (page FilteredPage) NextStart(original FilteredStart) FilteredStart {
	if page.NextScanOffset == nil {
		return original
	}
	return StartFromContinuation(Continuation{
		GroupID:          page.Policy.GroupID,
		NextScanOffset:   *page.NextScanOffset,
		Generation:       page.Generation,
		Digest:           page.Policy.Digest,
		Mode:             page.Policy.Mode,
		PolicyGeneration: page.Policy.PolicyGeneration,
	})
}

// FilteredAck acknowledges a filtered page through its safe offset.
//
// The server stores the offset only while the source still has the generation
// the page was read from, and only while the consumer still owns the
// partition.
type FilteredAck struct {
	GroupID     *uint64          `cbor:"group_id,omitempty"`
	V           uint32           `cbor:"v"`
	Source      FilterSource     `cbor:"source"`
	PartitionID uint32           `cbor:"partition_id"`
	Consumer    FilterConsumer   `cbor:"consumer"`
	Generation  SourceGeneration `cbor:"generation"`
	AckContext  AckContext       `cbor:"ack_context"`
	// Digest is the filter the page ran, absent for an unfiltered page.
	Digest *Digest32     `cbor:"digest,omitempty"`
	Offset uint64        `cbor:"offset"`
	Mode   ExecutionMode `cbor:"mode"`
	// PolicyGeneration is the group's policy generation the page ran under.
	// The server refuses the acknowledgment when the group's generation moved
	// on.
	PolicyGeneration uint64 `cbor:"policy_generation"`
}

// AckReceipt is a stored acknowledgment.
type AckReceipt struct {
	PartitionID uint32           `cbor:"partition_id"`
	Offset      uint64           `cbor:"offset"`
	Generation  SourceGeneration `cbor:"generation"`
}

// validateFilterVersion refuses a filter request of another op version.
func validateFilterVersion(v uint32) error {
	if v != FilterOpVersion {
		return &InvalidError{Reason: fmt.Sprintf(
			"filter request version %d is not supported, expected %d", v, FilterOpVersion)}
	}
	return nil
}

// validateFilterName is the shared bound on a filter request's names.
func validateFilterName(label, name string) error {
	if name == "" || len(name) > MaxFilterSourceNameBytes {
		return &InvalidError{Reason: fmt.Sprintf(
			"%s must be 1..=%d bytes", label, MaxFilterSourceNameBytes)}
	}
	return nil
}

// Validate bounds the source names.
func (source FilterSource) Validate() error {
	if err := validateFilterName("stream", source.Stream); err != nil {
		return err
	}
	return validateFilterName("topic", source.Topic)
}

// Validate bounds the consumer name, or the group id against the native
// offset-key range.
func (consumer FilterConsumer) Validate() error {
	switch consumer.Kind {
	case ConsumerIndependent:
		return validateFilterName("consumer", consumer.Name)
	case ConsumerGroupName:
		return validateFilterName("consumer group", consumer.Name)
	case ConsumerGroupID:
		if consumer.ID > math.MaxUint32 {
			return &InvalidError{
				Reason: "consumer group id exceeds the native offset-key range",
			}
		}
		return nil
	default:
		return &InvalidError{Reason: fmt.Sprintf(
			"unknown filter consumer kind %q", string(consumer.Kind))}
	}
}

// FilteredWaitRequest holds until one of its partitions has a record at or
// after its offset, or the wait elapses.
//
// The server lowers the wait to its announced limit and reads nothing: the
// reader then sends a filtered poll to the ready partitions. The wait id names
// the wait on its connection, so a cancel can end it early.
type FilteredWaitRequest struct {
	V          uint32                  `cbor:"v"`
	Source     FilterSource            `cbor:"source"`
	Consumer   FilterConsumer          `cbor:"consumer"`
	Partitions []FilteredWaitPartition `cbor:"partitions"`
	MaxWaitMs  uint32                  `cbor:"max_wait_ms"`
	WaitID     uint64                  `cbor:"wait_id"`
}

// FilteredWaitPartition is one partition of a filtered wait and the offset its
// next read starts at.
type FilteredWaitPartition struct {
	PartitionID uint32 `cbor:"partition_id"`
	Offset      uint64 `cbor:"offset"`
}

// Validate bounds the wait and checks it names each partition once.
func (request FilteredWaitRequest) Validate() error {
	if err := validateFilterVersion(request.V); err != nil {
		return err
	}
	if err := request.Source.Validate(); err != nil {
		return err
	}
	if err := request.Consumer.Validate(); err != nil {
		return err
	}
	if len(request.Partitions) == 0 || len(request.Partitions) > MaxFilteredWaitPartitions {
		return &InvalidError{Reason: fmt.Sprintf(
			"a filtered wait names 1..=%d partitions", MaxFilteredWaitPartitions)}
	}
	seen := make(map[uint32]bool, len(request.Partitions))
	for _, partition := range request.Partitions {
		if seen[partition.PartitionID] {
			return &InvalidError{Reason: "a filtered wait names each partition once"}
		}
		seen[partition.PartitionID] = true
	}
	if request.MaxWaitMs > MaxFilteredWaitMs {
		return &InvalidError{Reason: fmt.Sprintf(
			"max_wait_ms must be 0..=%d", MaxFilteredWaitMs)}
	}
	return nil
}

// FilteredWaitCancel ends the filtered waits this connection holds under the
// named wait ids. Each cancelled wait answers at once with no ready partition.
type FilteredWaitCancel struct {
	V       uint32   `cbor:"v"`
	WaitIDs []uint64 `cbor:"wait_ids"`
}

// Validate bounds the cancel and checks it names each wait once.
func (cancel FilteredWaitCancel) Validate() error {
	if err := validateFilterVersion(cancel.V); err != nil {
		return err
	}
	if len(cancel.WaitIDs) == 0 || len(cancel.WaitIDs) > MaxFilteredWaitCancelIDs {
		return &InvalidError{Reason: fmt.Sprintf(
			"a filtered wait cancel names 1..=%d waits", MaxFilteredWaitCancelIDs)}
	}
	seen := make(map[uint64]bool, len(cancel.WaitIDs))
	for _, waitID := range cancel.WaitIDs {
		if seen[waitID] {
			return &InvalidError{Reason: "a filtered wait cancel names each wait once"}
		}
		seen[waitID] = true
	}
	return nil
}

// FilteredWaitCancelled names the waits a cancel ended. A wait that had
// already answered, or was never held, is left out.
type FilteredWaitCancelled struct {
	Cancelled []uint64 `cbor:"cancelled"`
}

// FilteredWaitResult names the partitions that hold a record at or after their
// offset. It is empty when the wait ended first.
type FilteredWaitResult struct {
	Ready []uint32 `cbor:"ready"`
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *ExecutionMode) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "execution mode", ModeFiltered, ModeUnfiltered)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *FaultReason) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "fault reason", FaultMissingSchema, FaultSchemaNotAllowed, FaultSchemaMismatch, FaultMalformed, FaultTooLarge, FaultTooDeep, FaultForeignCodec, FaultTypeMismatch)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *StopReason) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "stop reason", StopFilled, StopBudget, StopEndOfVisible, StopFault, StopOversizedRecord)
}

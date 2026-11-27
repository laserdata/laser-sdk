package wire

import (
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// FilterPreviewRequest previews a filter over stored records of one
// partition.
//
// It is observational: it joins no group, stores no offset and changes no
// consumer state.
type FilterPreviewRequest struct {
	V           uint32       `cbor:"v"`
	Source      FilterSource `cbor:"source"`
	PartitionID uint32       `cbor:"partition_id"`
	// Filter must be inline or a revision. A bound reference is rejected,
	// since a preview joins no group.
	Filter      FilterRef `cbor:"filter"`
	FromOffset  uint64    `cbor:"from_offset"`
	MaxExamined uint32    `cbor:"max_examined"`
	MaxRecords  uint32    `cbor:"max_records"`
	// Explain includes a per-predicate explanation for every returned record.
	Explain bool `cbor:"explain"`
}

// Verdict is how the filter judged one previewed record.
type Verdict string

const (
	VerdictSelected Verdict = "selected"
	VerdictRejected Verdict = "rejected"
	VerdictFault    Verdict = "fault"
)

// Truth is three-valued comparison truth.
//
// A missing field or a type mismatch is unknown, a negation keeps unknown, and
// an unknown root selects nothing.
type Truth string

const (
	TruthMatch   Truth = "match"
	TruthNoMatch Truth = "no_match"
	TruthUnknown Truth = "unknown"
)

// ExplainNode is one node of an explanation tree: what it tested and how it
// resolved. The truth is absent when the payload could not be decoded for it.
type ExplainNode struct {
	Label    string        `cbor:"label"`
	Truth    *Truth        `cbor:"truth,omitempty"`
	Children []ExplainNode `cbor:"children,omitempty"`
}

// FilterExplanation is why a record was or was not selected.
type FilterExplanation struct {
	Verdict Verdict      `cbor:"verdict"`
	Fault   *FaultReason `cbor:"fault,omitempty"`
	Root    ExplainNode  `cbor:"root"`
}

// PreviewRecord is one previewed record: identity, verdict and a bounded text
// view.
type PreviewRecord struct {
	Offset          uint64       `cbor:"offset"`
	TimestampMicros uint64       `cbor:"timestamp_micros"`
	Verdict         Verdict      `cbor:"verdict"`
	Fault           *FaultReason `cbor:"fault,omitempty"`
	// PayloadText is the payload as lossy UTF-8, cut at the preview cap.
	PayloadText      string             `cbor:"payload_text"`
	PayloadTruncated bool               `cbor:"payload_truncated"`
	Explanation      *FilterExplanation `cbor:"explanation,omitempty"`
}

// FilterPreview is a preview of one partition range.
type FilterPreview struct {
	V           uint32        `cbor:"v"`
	PartitionID uint32        `cbor:"partition_id"`
	Policy      AppliedPolicy `cbor:"policy"`
	Examined    uint32        `cbor:"examined"`
	Matched     uint32        `cbor:"matched"`
	Faults      uint32        `cbor:"faults"`
	Stop        StopReason    `cbor:"stop"`
	NextOffset  *uint64       `cbor:"next_offset,omitempty"`
	Frontier    uint64        `cbor:"frontier"`
	// Records lists selected records first, capped at the request's maximum.
	// Rejected records appear only when an explanation was requested.
	Records []PreviewRecord `cbor:"records"`
}

// HeaderScalarKind discriminates a typed user header value.
type HeaderScalarKind string

const (
	ScalarBool   HeaderScalarKind = "bool"
	ScalarInt    HeaderScalarKind = "int"
	ScalarUint   HeaderScalarKind = "uint"
	ScalarFloat  HeaderScalarKind = "float"
	ScalarString HeaderScalarKind = "string"
	// ScalarRaw carries bytes. Integer kinds wider than 64 bits arrive here.
	ScalarRaw HeaderScalarKind = "raw"
)

// HeaderScalar is a typed user header value as the evaluator reads it.
//
// Integer kinds up to 64 bits map to int or uint, and 128-bit integers arrive
// as raw bytes. It is adjacently tagged, like a typed value.
type HeaderScalar struct {
	Kind  HeaderScalarKind
	Bool  bool
	Int   int64
	Uint  uint64
	Float float64
	Str   string
	Raw   []byte
}

// The header scalar constructors.
func BoolHeaderScalar(value bool) HeaderScalar {
	return HeaderScalar{Kind: ScalarBool, Bool: value}
}
func IntHeaderScalar(value int64) HeaderScalar {
	return HeaderScalar{Kind: ScalarInt, Int: value}
}
func UintHeaderScalar(value uint64) HeaderScalar {
	return HeaderScalar{Kind: ScalarUint, Uint: value}
}
func FloatHeaderScalar(value float64) HeaderScalar {
	return HeaderScalar{Kind: ScalarFloat, Float: value}
}
func StringHeaderScalar(value string) HeaderScalar {
	return HeaderScalar{Kind: ScalarString, Str: value}
}
func RawHeaderScalar(value []byte) HeaderScalar {
	return HeaderScalar{Kind: ScalarRaw, Raw: value}
}

// MarshalCBOR writes the adjacently tagged scalar.
func (scalar HeaderScalar) MarshalCBOR() (cborform.Value, error) {
	var payload cborform.Value
	switch scalar.Kind {
	case ScalarBool:
		payload = cborform.Bool(scalar.Bool)
	case ScalarInt:
		payload = signedValue(scalar.Int)
	case ScalarUint:
		payload = cborform.Uint(scalar.Uint)
	case ScalarFloat:
		payload = cborform.Float{Value: scalar.Float, Bits: 64}
	case ScalarString:
		payload = cborform.Text(scalar.Str)
	case ScalarRaw:
		payload = cborform.Bytes(scalar.Raw)
	default:
		return nil, fmt.Errorf("wire: unknown header scalar %q", string(scalar.Kind))
	}
	return cborform.Map{
		{Key: cborform.Text(typedTagKey), Value: cborform.Text(string(scalar.Kind))},
		{Key: cborform.Text(typedContentKey), Value: payload},
	}, nil
}

// UnmarshalCBOR reads the adjacently tagged scalar.
func (scalar *HeaderScalar) UnmarshalCBOR(item cborform.Value) error {
	entries, ok := item.(cborform.Map)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a header scalar", item)
	}
	var kind HeaderScalarKind
	var payload cborform.Value
	for _, entry := range entries {
		key, ok := entry.Key.(cborform.Text)
		if !ok {
			continue
		}
		switch string(key) {
		case typedTagKey:
			name, ok := entry.Value.(cborform.Text)
			if !ok {
				return fmt.Errorf("wire: a header scalar kind must be text")
			}
			kind = HeaderScalarKind(name)
		case typedContentKey:
			payload = entry.Value
		}
	}
	if payload == nil {
		return fmt.Errorf("wire: a header scalar carries a value")
	}

	switch kind {
	case ScalarBool:
		flag, ok := payload.(cborform.Bool)
		if !ok {
			return fmt.Errorf("wire: a bool header must be a bool, got %T", payload)
		}
		*scalar = BoolHeaderScalar(bool(flag))
	case ScalarInt:
		number, err := readSigned(payload)
		if err != nil {
			return err
		}
		*scalar = IntHeaderScalar(number)
	case ScalarUint:
		number, ok := payload.(cborform.Uint)
		if !ok {
			return fmt.Errorf("wire: a uint header must be an integer, got %T", payload)
		}
		*scalar = UintHeaderScalar(uint64(number))
	case ScalarFloat:
		number, ok := payload.(cborform.Float)
		if !ok {
			return fmt.Errorf("wire: a float header must be a float, got %T", payload)
		}
		*scalar = FloatHeaderScalar(number.Value)
	case ScalarString:
		text, ok := payload.(cborform.Text)
		if !ok {
			return fmt.Errorf("wire: a string header must be text, got %T", payload)
		}
		*scalar = StringHeaderScalar(string(text))
	case ScalarRaw:
		var raw []byte
		if err := fromValueInto(payload, &raw); err != nil {
			return err
		}
		*scalar = RawHeaderScalar(raw)
	default:
		return fmt.Errorf("wire: unknown header scalar %q", string(kind))
	}
	return nil
}

// FilterHeader is one user header of a record.
type FilterHeader struct {
	Key   string       `cbor:"key"`
	Value HeaderScalar `cbor:"value"`
}

// FilterTestRequest evaluates a filter against one supplied record. Nothing is
// read from a stream and nothing is stored.
type FilterTestRequest struct {
	V uint32 `cbor:"v"`
	// Filter must be inline or a revision. A bound reference is rejected.
	Filter  FilterRef      `cbor:"filter"`
	Payload []byte         `cbor:"payload"`
	Headers []FilterHeader `cbor:"headers,omitempty"`
}

// FilterTestResult is the result of a sample test.
type FilterTestResult struct {
	V           uint32            `cbor:"v"`
	Policy      AppliedPolicy     `cbor:"policy"`
	Explanation FilterExplanation `cbor:"explanation"`
}

// FilterValidation is a filter that validated and compiled.
type FilterValidation struct {
	V      uint32   `cbor:"v"`
	Digest Digest32 `cbor:"digest"`
	// ReadsPayload says whether evaluating the filter can decode the payload.
	ReadsPayload bool `cbor:"reads_payload"`
	// ReadsHeaders says whether evaluating the filter can read user headers.
	ReadsHeaders bool `cbor:"reads_headers"`
}

// FilterErrorReason is the typed cause of a consumer-filter failure. Each maps
// to one shared result code, so a generic client can still classify it.
type FilterErrorReason string

const (
	// ReasonInvalidRequest means the filter or request is malformed or out of
	// range.
	ReasonInvalidRequest FilterErrorReason = "invalid_request"
	// ReasonUnsupported means the server or its backend does not serve this
	// operation.
	ReasonUnsupported FilterErrorReason = "unsupported"
	// ReasonVersionSkew means the wire version is not accepted.
	ReasonVersionSkew FilterErrorReason = "version_skew"
	// ReasonNotFound means the source, filter, revision, binding or operation
	// does not exist.
	ReasonNotFound FilterErrorReason = "not_found"
	// ReasonConflict means a policy or mutation precondition does not hold.
	ReasonConflict FilterErrorReason = "conflict"
	// ReasonSourceChanged means the source history changed since the page or
	// continuation was issued.
	ReasonSourceChanged FilterErrorReason = "source_changed"
	// ReasonMembershipStale means the consumer no longer owns the partition,
	// or its session ended.
	ReasonMembershipStale FilterErrorReason = "membership_stale"
	// ReasonCatalogUnavailable means the catalog cannot confirm the policy
	// right now.
	ReasonCatalogUnavailable FilterErrorReason = "catalog_unavailable"
	// ReasonRevisionDisabled means the saved revision is paused. Pending
	// records may still be acknowledged.
	ReasonRevisionDisabled FilterErrorReason = "revision_disabled"
	// ReasonTooLarge means the request is too large.
	ReasonTooLarge        FilterErrorReason = "too_large"
	ReasonUnauthenticated FilterErrorReason = "unauthenticated"
	ReasonForbidden       FilterErrorReason = "forbidden"
	// ReasonUnavailable is a transient failure, so the same request may succeed
	// later.
	ReasonUnavailable FilterErrorReason = "unavailable"
	// ReasonCapacityExhausted means a configured catalog resource limit is
	// reached.
	ReasonCapacityExhausted FilterErrorReason = "capacity_exhausted"
	// ReasonBackend is an unexpected failure the caller cannot retry away.
	ReasonBackend FilterErrorReason = "backend"
	// ReasonUnknown is a reason this build does not know. The error's code
	// still classifies it.
	ReasonUnknown FilterErrorReason = "unknown"
)

// IsNamed reports whether this build names the reason.
func (reason FilterErrorReason) IsNamed() bool {
	_, known := filterReasonCodes[reason]
	return known
}

// filterReasonCodes is the projection onto the shared outcome dictionary.
var filterReasonCodes = map[FilterErrorReason]ResultCode{
	ReasonInvalidRequest:     ResultInvalidArgument,
	ReasonUnsupported:        ResultUnsupported,
	ReasonVersionSkew:        ResultVersionSkew,
	ReasonNotFound:           ResultNotFound,
	ReasonConflict:           ResultConflict,
	ReasonSourceChanged:      ResultStaleGeneration,
	ReasonMembershipStale:    ResultStale,
	ReasonCatalogUnavailable: ResultUnavailable,
	ReasonUnavailable:        ResultUnavailable,
	ReasonRevisionDisabled:   ResultUnavailable,
	ReasonTooLarge:           ResultTooLarge,
	ReasonUnauthenticated:    ResultUnauthenticated,
	ReasonForbidden:          ResultForbidden,
	ReasonCapacityExhausted:  ResultResourceLimit,
	ReasonBackend:            ResultBackend,
}

// Code is the shared result code this reason maps to. A reason this build does
// not name classifies as a backend failure.
func (reason FilterErrorReason) Code() ResultCode {
	if code, known := filterReasonCodes[reason]; known {
		return code
	}
	return ResultBackend
}

// MarshalCBOR writes the snake_case name.
func (reason FilterErrorReason) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(string(reason)), nil
}

// UnmarshalCBOR reads the name, mapping an unknown one to the passthrough so
// the error still classifies through its code.
func (reason *FilterErrorReason) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a filter error reason", item)
	}
	candidate := FilterErrorReason(text)
	if !candidate.IsNamed() {
		candidate = ReasonUnknown
	}
	*reason = candidate
	return nil
}

// FilterError is a consumer-filter failure.
type FilterError struct {
	Code    ResultCode        `cbor:"code"`
	Reason  FilterErrorReason `cbor:"reason"`
	Message string            `cbor:"message"`
}

// ResultCode is the shared code this failure classifies as, so a filter
// failure is branched on the same way as every other managed failure.
func (filterError FilterError) ResultCode() ResultCode { return filterError.Code }

// NewFilterError builds a failure, deriving its shared code from the reason.
func NewFilterError(reason FilterErrorReason, message string) FilterError {
	return FilterError{Code: reason.Code(), Reason: reason, Message: message}
}

// Error renders the failure.
func (filterError FilterError) Error() string {
	return fmt.Sprintf("%s: %s", filterError.Reason, filterError.Message)
}

// CheckFilterVersion refuses a request of another filter op version as a
// version skew, before its shape is validated, so a client of another build
// can tell a version gap from a malformed request.
func CheckFilterVersion(v uint32) error {
	if v == FilterOpVersion {
		return nil
	}
	return NewFilterError(ReasonVersionSkew, fmt.Sprintf(
		"filter request version %d is not supported, expected %d", v, FilterOpVersion))
}

// FilterOutcomeKind discriminates a successful filter command reply.
type FilterOutcomeKind uint8

const (
	FilterPageOutcome FilterOutcomeKind = iota
	FilterAcknowledged
	FilterPreviewOutcome
	FilterTested
	FilterValidated
	// FilterReady names the partitions a filtered wait found holding a
	// record at or after their offset.
	FilterReady
	// FilterCancelled names the waits a cancel ended.
	FilterCancelled
)

// FilterOutcome is a successful reply of a server-native consumer-filter
// command.
type FilterOutcome struct {
	Kind       FilterOutcomeKind
	Page       *FilteredPage
	Receipt    *AckReceipt
	Preview    *FilterPreview
	TestResult *FilterTestResult
	Validation *FilterValidation
	WaitResult *FilteredWaitResult
	Cancelled  *FilteredWaitCancelled
}

// The externally tagged, snake_case outcome names.
const (
	filterPageVariant         = "page"
	filterAcknowledgedVariant = "acknowledged"
	filterPreviewVariant      = "preview"
	filterTestedVariant       = "tested"
	filterValidatedVariant    = "validated"
	filterReadyVariant        = "ready"
	filterCancelledVariant    = "cancelled"
)

// MarshalCBOR writes the externally tagged outcome.
func (outcome FilterOutcome) MarshalCBOR() (cborform.Value, error) {
	switch outcome.Kind {
	case FilterPageOutcome:
		if outcome.Page == nil {
			return nil, fmt.Errorf("wire: a page outcome carries no page")
		}
		return marshalVariantStruct(filterPageVariant, *outcome.Page)
	case FilterAcknowledged:
		if outcome.Receipt == nil {
			return nil, fmt.Errorf("wire: an acknowledgment carries no receipt")
		}
		return marshalVariantStruct(filterAcknowledgedVariant, *outcome.Receipt)
	case FilterPreviewOutcome:
		if outcome.Preview == nil {
			return nil, fmt.Errorf("wire: a preview outcome carries no preview")
		}
		return marshalVariantStruct(filterPreviewVariant, *outcome.Preview)
	case FilterTested:
		if outcome.TestResult == nil {
			return nil, fmt.Errorf("wire: a test outcome carries no result")
		}
		return marshalVariantStruct(filterTestedVariant, *outcome.TestResult)
	case FilterValidated:
		if outcome.Validation == nil {
			return nil, fmt.Errorf("wire: a validation outcome carries no validation")
		}
		return marshalVariantStruct(filterValidatedVariant, *outcome.Validation)
	case FilterReady:
		if outcome.WaitResult == nil {
			return nil, fmt.Errorf("wire: a ready outcome carries no partitions")
		}
		return marshalVariantStruct(filterReadyVariant, *outcome.WaitResult)
	case FilterCancelled:
		if outcome.Cancelled == nil {
			return nil, fmt.Errorf("wire: a cancelled outcome carries no waits")
		}
		return marshalVariantStruct(filterCancelledVariant, *outcome.Cancelled)
	default:
		return nil, fmt.Errorf("wire: unknown filter outcome %d", outcome.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged outcome.
func (outcome *FilterOutcome) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("FilterOutcome", name, payload); err != nil {
		return err
	}
	switch name {
	case filterPageVariant:
		var page FilteredPage
		if err := fromValueInto(payload, &page); err != nil {
			return err
		}
		*outcome = FilterOutcome{Kind: FilterPageOutcome, Page: &page}
	case filterAcknowledgedVariant:
		var receipt AckReceipt
		if err := fromValueInto(payload, &receipt); err != nil {
			return err
		}
		*outcome = FilterOutcome{Kind: FilterAcknowledged, Receipt: &receipt}
	case filterPreviewVariant:
		var preview FilterPreview
		if err := fromValueInto(payload, &preview); err != nil {
			return err
		}
		*outcome = FilterOutcome{Kind: FilterPreviewOutcome, Preview: &preview}
	case filterTestedVariant:
		var result FilterTestResult
		if err := fromValueInto(payload, &result); err != nil {
			return err
		}
		*outcome = FilterOutcome{Kind: FilterTested, TestResult: &result}
	case filterValidatedVariant:
		var validation FilterValidation
		if err := fromValueInto(payload, &validation); err != nil {
			return err
		}
		*outcome = FilterOutcome{Kind: FilterValidated, Validation: &validation}
	case filterReadyVariant:
		var ready FilteredWaitResult
		if err := fromValueInto(payload, &ready); err != nil {
			return err
		}
		*outcome = FilterOutcome{Kind: FilterReady, WaitResult: &ready}
	case filterCancelledVariant:
		var cancelled FilteredWaitCancelled
		if err := fromValueInto(payload, &cancelled); err != nil {
			return err
		}
		*outcome = FilterOutcome{Kind: FilterCancelled, Cancelled: &cancelled}
	default:
		return unknownVariant("FilterOutcome", name)
	}
	return nil
}

// FilterReply is the reply to every server-native consumer-filter command.
type FilterReply struct {
	Err     *FilterError
	Outcome *FilterOutcome
}

// IsOk reports a successful reply.
func (reply FilterReply) IsOk() bool { return reply.Err == nil && reply.Outcome != nil }

// The externally tagged, snake_case reply names. The filter surface spells
// these in lower case where the managed surfaces use PascalCase.
const (
	filterOkVariant  = "ok"
	filterErrVariant = "err"
)

// MarshalCBOR writes the externally tagged reply.
func (reply FilterReply) MarshalCBOR() (cborform.Value, error) {
	switch {
	case reply.Err != nil && reply.Outcome != nil:
		return nil, fmt.Errorf("wire: a filter reply is either an outcome or an error")
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
		return nil, fmt.Errorf("wire: an empty filter reply")
	}
}

// UnmarshalCBOR reads the externally tagged reply.
func (reply *FilterReply) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("FilterReply", name, payload); err != nil {
		return err
	}
	switch name {
	case filterOkVariant:
		var outcome FilterOutcome
		if err := outcome.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*reply = FilterReply{Outcome: &outcome}
		return nil
	case filterErrVariant:
		var filterError FilterError
		if err := fromValueInto(payload, &filterError); err != nil {
			return err
		}
		*reply = FilterReply{Err: &filterError}
		return nil
	default:
		return unknownVariant("FilterReply", name)
	}
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *Truth) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "truth", TruthMatch, TruthNoMatch, TruthUnknown)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *Verdict) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "verdict", VerdictSelected, VerdictRejected, VerdictFault)
}

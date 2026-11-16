package wire

import (
	"crypto/sha256"
	"fmt"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// FilterEvaluatorVersion is the version of the evaluation rules a filter was
// written against. It is part of the digest, so a rule change can never
// silently reinterpret a saved revision.
const FilterEvaluatorVersion uint32 = 1

// filterDigestDomain prefixes the canonical bytes of a consumer filter.
var filterDigestDomain = []byte("agdx.consumer-filter.v1\x00")

// CoerceKind discriminates an explicit value coercion.
type CoerceKind string

const (
	// CoerceTimestamp compares as an instant in microseconds.
	CoerceTimestamp CoerceKind = "timestamp"
	// CoerceNumber compares as an exact decimal, accepting numbers and
	// decimal strings.
	CoerceNumber CoerceKind = "number"
)

// Coerce is an explicit value coercion. A value that does not parse under it
// compares as unknown rather than failing the record.
type Coerce struct {
	Kind   CoerceKind
	Format TimestampFormat
}

// TimestampCoercion compares as an instant read under a format.
func TimestampCoercion(format TimestampFormat) Coerce {
	return Coerce{Kind: CoerceTimestamp, Format: format}
}

// NumberCoercion compares as an exact decimal.
func NumberCoercion() Coerce { return Coerce{Kind: CoerceNumber} }

type timestampCoerceFields struct {
	Format TimestampFormat `cbor:"format"`
}

// MarshalCBOR writes the internally tagged coercion.
func (coerce Coerce) MarshalCBOR() (cborform.Value, error) {
	switch coerce.Kind {
	case CoerceTimestamp:
		if !coerce.Format.IsKnown() {
			return nil, fmt.Errorf("wire: unknown timestamp format %q", string(coerce.Format))
		}
		return marshalInternallyTagged(string(CoerceTimestamp),
			timestampCoerceFields{Format: coerce.Format})
	case CoerceNumber:
		return cborform.Map{{
			Key:   cborform.Text(internalTagKey),
			Value: cborform.Text(string(CoerceNumber)),
		}}, nil
	default:
		return nil, fmt.Errorf("wire: unknown coercion %q", string(coerce.Kind))
	}
}

// UnmarshalCBOR reads the internally tagged coercion.
func (coerce *Coerce) UnmarshalCBOR(item cborform.Value) error {
	kind, entries, err := readInternalTag(item)
	if err != nil {
		return err
	}
	switch CoerceKind(kind) {
	case CoerceTimestamp:
		var fields timestampCoerceFields
		if err := fromValueInto(entries, &fields); err != nil {
			return err
		}
		if !fields.Format.IsKnown() {
			return fmt.Errorf("wire: unknown timestamp format %q", string(fields.Format))
		}
		*coerce = TimestampCoercion(fields.Format)
		return nil
	case CoerceNumber:
		*coerce = NumberCoercion()
		return nil
	default:
		return fmt.Errorf("wire: unknown coercion %q", kind)
	}
}

// CoercedPredicate is a payload comparison whose field and literal are both
// coerced before the compare, so an instant orders as an instant and a decimal
// string orders as a number. The coercion is part of the expression and its
// digest.
type CoercedPredicate struct {
	Pred   Predicate `cbor:"pred"`
	Coerce Coerce    `cbor:"coerce"`
}

// HeaderPredicate is a comparison against one user header, keyed exactly.
type HeaderPredicate struct {
	Key   string     `cbor:"key"`
	Op    CmpOp      `cbor:"op"`
	Value TypedValue `cbor:"value"`
}

// FilterCodec is how a record payload is decoded before payload predicates
// run.
//
// The filter chooses it explicitly and a content-type header never overrides
// it: a record whose codec header names another codec is never decoded and
// follows the filter's foreign policy instead.
type FilterCodec string

const (
	CodecJSON     FilterCodec = "json"
	CodecCBOR     FilterCodec = "cbor"
	CodecAvro     FilterCodec = "avro"
	CodecProtobuf FilterCodec = "protobuf"
	// CodecHeadersOnly never decodes the payload, so only header predicates
	// are allowed and the payload may be in any format.
	CodecHeadersOnly FilterCodec = "headers_only"
	// CodecUnknown is a codec this build does not name, such as one a newer
	// catalog saved. It decodes so listings stay readable, but it never
	// validates or compiles.
	CodecUnknown FilterCodec = "unknown"
)

// NeedsSchemaRefs reports the codecs that resolve a writer schema.
func (codec FilterCodec) NeedsSchemaRefs() bool {
	return codec == CodecAvro || codec == CodecProtobuf
}

// IsNamed reports whether this build names the codec.
func (codec FilterCodec) IsNamed() bool {
	switch codec {
	case CodecJSON, CodecCBOR, CodecAvro, CodecProtobuf, CodecHeadersOnly:
		return true
	default:
		return false
	}
}

// MarshalCBOR writes the snake_case name.
func (codec FilterCodec) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(string(codec)), nil
}

// UnmarshalCBOR reads the snake_case name, mapping a name this build does not
// know to the unknown codec rather than failing, so a newer catalog's filter
// still lists.
func (codec *FilterCodec) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a filter codec", item)
	}
	candidate := FilterCodec(text)
	if !candidate.IsNamed() {
		candidate = CodecUnknown
	}
	*codec = candidate
	return nil
}

// RecordPolicy is what happens to a record the filter cannot judge on its own
// terms.
type RecordPolicy string

const (
	// PolicyReject skips the record. It is not selected and never a fault.
	// The default.
	PolicyReject RecordPolicy = "reject"
	// PolicyPass delivers the record marked unevaluated, so the consumer
	// decides.
	PolicyPass RecordPolicy = "pass"
)

// IsReject reports the default policy, which is omitted on the wire.
func (policy RecordPolicy) IsReject() bool {
	return policy == PolicyReject || policy == ""
}

// FaultPolicy is what happens to a record whose payload cannot be decoded. A
// valid record missing a field is not a fault.
type FaultPolicy string

const (
	// FaultStop stops the page before the record, leaving it unacknowledged
	// until the reader changes policy or the record ages out. The default.
	FaultStop FaultPolicy = "stop"
	// FaultPass delivers the record marked unevaluated.
	FaultPass FaultPolicy = "pass"
	// FaultDrop skips the record, counted as a fault.
	FaultDrop FaultPolicy = "drop"
)

// FilterExprKind discriminates a filter expression node.
type FilterExprKind uint8

const (
	ExprAll FilterExprKind = iota
	ExprAny
	ExprNot
	// ExprPred is a payload comparison, whose field is a field-path text form.
	ExprPred
	// ExprPredAs is a payload comparison after an explicit coercion.
	ExprPredAs
	// ExprPresent tests that a payload path exists. An explicit null is
	// present.
	ExprPresent
	// ExprAbsent tests that a payload path does not exist.
	ExprAbsent
	// ExprHeader is a typed user-header comparison, keyed exactly.
	ExprHeader
	// ExprText is a text match on a payload field.
	ExprText
	// ExprHeaderText is a text match on one user header, keyed exactly.
	ExprHeaderText
)

// FilterExpr is a declarative predicate over one record: payload fields, typed
// user headers, presence tests, and boolean composition. It never runs user
// code.
//
// Comparison truth is three-valued. A missing field or a type mismatch is
// unknown, a negation keeps unknown, and an unknown root selects nothing. A
// decode fault is separate from truth and follows the filter's fault policy.
type FilterExpr struct {
	Kind     FilterExprKind
	Children []FilterExpr
	Negated  *FilterExpr
	Pred     *Predicate
	PredAs   *CoercedPredicate
	Path     *FieldPath
	Header   *HeaderPredicate
	Text     *TextPredicate
}

// The expression constructors.
func ExprAllOf(children ...FilterExpr) FilterExpr {
	return FilterExpr{Kind: ExprAll, Children: children}
}

func ExprAnyOf(children ...FilterExpr) FilterExpr {
	return FilterExpr{Kind: ExprAny, Children: children}
}

func ExprNegate(child FilterExpr) FilterExpr {
	return FilterExpr{Kind: ExprNot, Negated: &child}
}

func ExprPredicate(field string, op CmpOp, value TypedValue) FilterExpr {
	return FilterExpr{Kind: ExprPred, Pred: &Predicate{Field: field, Op: op, Value: value}}
}

func ExprCoerced(field string, op CmpOp, value TypedValue, coerce Coerce) FilterExpr {
	return FilterExpr{Kind: ExprPredAs, PredAs: &CoercedPredicate{
		Pred: Predicate{Field: field, Op: op, Value: value}, Coerce: coerce,
	}}
}

// ExprPresentPath tests that a payload path exists.
func ExprPresentPath(path string) (FilterExpr, error) {
	parsed, err := ParseFieldPath(path)
	if err != nil {
		return FilterExpr{}, err
	}
	return FilterExpr{Kind: ExprPresent, Path: &parsed}, nil
}

// ExprAbsentPath tests that a payload path does not exist.
func ExprAbsentPath(path string) (FilterExpr, error) {
	parsed, err := ParseFieldPath(path)
	if err != nil {
		return FilterExpr{}, err
	}
	return FilterExpr{Kind: ExprAbsent, Path: &parsed}, nil
}

func ExprHeaderPredicate(key string, op CmpOp, value TypedValue) FilterExpr {
	return FilterExpr{Kind: ExprHeader, Header: &HeaderPredicate{Key: key, Op: op, Value: value}}
}

func ExprTextMatch(field string, kind TextMatch, pattern string) FilterExpr {
	return FilterExpr{Kind: ExprText, Text: &TextPredicate{
		Field: field, Kind: kind, Pattern: pattern,
	}}
}

func ExprHeaderTextMatch(key string, kind TextMatch, pattern string) FilterExpr {
	return FilterExpr{Kind: ExprHeaderText, Text: &TextPredicate{
		Field: key, Kind: kind, Pattern: pattern,
	}}
}

// CaseInsensitive makes a text match case-insensitive. Any other node is
// returned unchanged.
func (expr FilterExpr) CaseInsensitive() FilterExpr {
	if (expr.Kind == ExprText || expr.Kind == ExprHeaderText) && expr.Text != nil {
		insensitive := *expr.Text
		insensitive.CaseInsensitive = true
		expr.Text = &insensitive
	}
	return expr
}

// The externally tagged, snake_case expression variant names.
const (
	exprAllVariant        = "all"
	exprAnyVariant        = "any"
	exprNotVariant        = "not"
	exprPredVariant       = "pred"
	exprPredAsVariant     = "pred_as"
	exprPresentVariant    = "present"
	exprAbsentVariant     = "absent"
	exprHeaderVariant     = "header"
	exprTextVariant       = "text"
	exprHeaderTextVariant = "header_text"
)

// MarshalCBOR writes the externally tagged expression.
func (expr FilterExpr) MarshalCBOR() (cborform.Value, error) {
	switch expr.Kind {
	case ExprAll, ExprAny:
		name := exprAllVariant
		if expr.Kind == ExprAny {
			name = exprAnyVariant
		}
		children := make(cborform.Array, 0, len(expr.Children))
		for i, child := range expr.Children {
			encoded, err := child.MarshalCBOR()
			if err != nil {
				return nil, fmt.Errorf("expression child %d: %w", i, err)
			}
			children = append(children, encoded)
		}
		return taggedVariant(name, children), nil

	case ExprNot:
		if expr.Negated == nil {
			return nil, fmt.Errorf("wire: a negation carries no expression")
		}
		inner, err := expr.Negated.MarshalCBOR()
		if err != nil {
			return nil, err
		}
		return taggedVariant(exprNotVariant, inner), nil

	case ExprPred:
		if expr.Pred == nil {
			return nil, fmt.Errorf("wire: a predicate node carries no predicate")
		}
		return marshalVariantStruct(exprPredVariant, *expr.Pred)

	case ExprPredAs:
		if expr.PredAs == nil {
			return nil, fmt.Errorf("wire: a coerced predicate node carries no predicate")
		}
		return marshalVariantStruct(exprPredAsVariant, *expr.PredAs)

	case ExprPresent, ExprAbsent:
		if expr.Path == nil {
			return nil, fmt.Errorf("wire: a presence test carries no path")
		}
		name := exprPresentVariant
		if expr.Kind == ExprAbsent {
			name = exprAbsentVariant
		}
		return taggedVariant(name, cborform.Text(expr.Path.String())), nil

	case ExprHeader:
		if expr.Header == nil {
			return nil, fmt.Errorf("wire: a header node carries no predicate")
		}
		return marshalVariantStruct(exprHeaderVariant, *expr.Header)

	case ExprText, ExprHeaderText:
		if expr.Text == nil {
			return nil, fmt.Errorf("wire: a text node carries no predicate")
		}
		name := exprTextVariant
		if expr.Kind == ExprHeaderText {
			name = exprHeaderTextVariant
		}
		return marshalVariantStruct(name, *expr.Text)

	default:
		return nil, fmt.Errorf("wire: unknown filter expression kind %d", expr.Kind)
	}
}

// UnmarshalCBOR reads the externally tagged expression.
func (expr *FilterExpr) UnmarshalCBOR(item cborform.Value) error {
	name, payload, err := readVariant(item)
	if err != nil {
		return err
	}
	if err := requirePayload("FilterExpr", name, payload); err != nil {
		return err
	}

	switch name {
	case exprAllVariant, exprAnyVariant:
		items, ok := payload.(cborform.Array)
		if !ok {
			return fmt.Errorf("wire: %q carries a list of expressions, got %T", name, payload)
		}
		children := make([]FilterExpr, len(items))
		for i, child := range items {
			if err := children[i].UnmarshalCBOR(child); err != nil {
				return fmt.Errorf("expression child %d: %w", i, err)
			}
		}
		kind := ExprAll
		if name == exprAnyVariant {
			kind = ExprAny
		}
		*expr = FilterExpr{Kind: kind, Children: children}
		return nil

	case exprNotVariant:
		var inner FilterExpr
		if err := inner.UnmarshalCBOR(payload); err != nil {
			return err
		}
		*expr = FilterExpr{Kind: ExprNot, Negated: &inner}
		return nil

	case exprPredVariant:
		var predicate Predicate
		if err := fromValueInto(payload, &predicate); err != nil {
			return err
		}
		*expr = FilterExpr{Kind: ExprPred, Pred: &predicate}
		return nil

	case exprPredAsVariant:
		var coerced CoercedPredicate
		if err := fromValueInto(payload, &coerced); err != nil {
			return err
		}
		*expr = FilterExpr{Kind: ExprPredAs, PredAs: &coerced}
		return nil

	case exprPresentVariant, exprAbsentVariant:
		var path FieldPath
		if err := path.UnmarshalCBOR(payload); err != nil {
			return err
		}
		kind := ExprPresent
		if name == exprAbsentVariant {
			kind = ExprAbsent
		}
		*expr = FilterExpr{Kind: kind, Path: &path}
		return nil

	case exprHeaderVariant:
		var header HeaderPredicate
		if err := fromValueInto(payload, &header); err != nil {
			return err
		}
		*expr = FilterExpr{Kind: ExprHeader, Header: &header}
		return nil

	case exprTextVariant, exprHeaderTextVariant:
		var text TextPredicate
		if err := fromValueInto(payload, &text); err != nil {
			return err
		}
		kind := ExprText
		if name == exprHeaderTextVariant {
			kind = ExprHeaderText
		}
		*expr = FilterExpr{Kind: kind, Text: &text}
		return nil

	default:
		return unknownVariant("FilterExpr", name)
	}
}

// ConsumerFilter is one consumer filter: the expression plus everything that
// changes what it selects. Its digest identifies those semantics.
type ConsumerFilter struct {
	V                uint32       `cbor:"v"`
	EvaluatorVersion uint32       `cbor:"evaluator_version"`
	Expr             FilterExpr   `cbor:"expr"`
	Codec            FilterCodec  `cbor:"codec"`
	FaultPolicy      FaultPolicy  `cbor:"fault_policy"`
	ForeignPolicy    RecordPolicy `cbor:"foreign_policy,omitempty"`
	MismatchPolicy   RecordPolicy `cbor:"mismatch_policy,omitempty"`
	// SchemaRefs are the immutable writer schema ids permitted for Avro or
	// Protobuf payloads. A record selects one of these with its schema header.
	SchemaRefs []uint32 `cbor:"schema_refs,omitempty"`
	// SchemaStream is the stream whose writer-schema registry holds the refs.
	// Absent resolves them in the deployment-wide registry and leaves the
	// encoding and the digest unchanged.
	SchemaStream *string `cbor:"schema_stream,omitempty"`
}

// NewConsumerFilter builds a filter at the current versions.
func NewConsumerFilter(expr FilterExpr, codec FilterCodec) ConsumerFilter {
	return ConsumerFilter{
		V:                FilterOpVersion,
		EvaluatorVersion: FilterEvaluatorVersion,
		Expr:             expr,
		Codec:            codec,
		FaultPolicy:      FaultStop,
		ForeignPolicy:    PolicyReject,
		MismatchPolicy:   PolicyReject,
	}
}

// Digest identifies the filter's semantics.
//
// It is taken over the filter's canonical JSON encoding, not its CBOR, so the
// catalog gives two filters with identical semantics the same identity in every
// client.
func (filter ConsumerFilter) Digest() (Digest32, error) {
	encoded, err := MarshalCanonicalJSON(filter)
	if err != nil {
		return Digest32{}, err
	}
	hasher := sha256.New()
	hasher.Write(filterDigestDomain)
	hasher.Write(encoded)
	var digest [DigestBytes]byte
	copy(digest[:], hasher.Sum(nil))
	return NewDigest32(digest), nil
}

// Validate checks the filter's versions and the codec's schema requirement.
func (filter ConsumerFilter) Validate() error {
	if filter.V != FilterOpVersion {
		return &InvalidError{Reason: fmt.Sprintf(
			"filter version %d is not supported, expected %d", filter.V, FilterOpVersion)}
	}
	if filter.EvaluatorVersion != FilterEvaluatorVersion {
		return &InvalidError{Reason: fmt.Sprintf(
			"evaluator version %d is not supported, expected %d",
			filter.EvaluatorVersion, FilterEvaluatorVersion)}
	}
	// A schema codec needs refs, and a non-schema codec must carry none.
	if filter.Codec.NeedsSchemaRefs() == (len(filter.SchemaRefs) == 0) {
		return &InvalidError{Reason: fmt.Sprintf(
			"codec %q and its schema references disagree", string(filter.Codec))}
	}
	return nil
}

// IsDefaultValue reports the default policy, which is omitted on the wire.
// The contract skips a record policy when it rejects, so a policy set
// explicitly to reject must encode exactly as an absent one.
func (policy RecordPolicy) IsDefaultValue() bool { return policy.IsReject() }

// ReadsPayload reports whether evaluating this node can require the record
// payload. A filter that reads none never decodes one.
func (expr FilterExpr) ReadsPayload() bool {
	switch expr.Kind {
	case ExprAll, ExprAny:
		for _, child := range expr.Children {
			if child.ReadsPayload() {
				return true
			}
		}
		return false
	case ExprNot:
		return expr.Negated != nil && expr.Negated.ReadsPayload()
	case ExprHeader, ExprHeaderText:
		return false
	default:
		return true
	}
}

// ReadsHeaders reports whether evaluating this node can require the record
// headers.
func (expr FilterExpr) ReadsHeaders() bool {
	switch expr.Kind {
	case ExprAll, ExprAny:
		for _, child := range expr.Children {
			if child.ReadsHeaders() {
				return true
			}
		}
		return false
	case ExprNot:
		return expr.Negated != nil && expr.Negated.ReadsHeaders()
	case ExprHeader, ExprHeaderText:
		return true
	default:
		return false
	}
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *FaultPolicy) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "fault policy", FaultStop, FaultPass, FaultDrop)
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *RecordPolicy) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "record policy", PolicyReject, PolicyPass)
}

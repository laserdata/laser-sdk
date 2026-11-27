package wire

import (
	"fmt"
	"math/bits"
	"strconv"
)

// FilterRecord is one record as the evaluator sees it.
//
// Headers are read only when the filter has header predicates, so a caller may
// decode them lazily. HeaderNeed says which ones are needed at all.
type FilterRecord struct {
	Payload []byte
	Headers []FilterHeader
}

// HeaderNeed is what a caller decodes from a record's header block before
// evaluating.
type HeaderNeed uint8

const (
	// NeedNoHeaders means the filter reads neither headers nor the payload.
	NeedNoHeaders HeaderNeed = iota
	// NeedContentType means only agdx.ct, to keep a record in another codec
	// out of the decoder.
	NeedContentType
	// NeedAllHeaders means the filter compares user headers.
	NeedAllHeaders
)

// CompiledFilter is a validated filter prepared for repeated evaluation.
// Compile once per digest, evaluate per record.
//
// A compiled filter holds no mutable state, so one may be shared across
// goroutines and evaluated concurrently.
type CompiledFilter struct {
	filter       ConsumerFilter
	digest       Digest32
	root         evalNode
	readsHeaders bool
	decoders     *PayloadDecoders
}

// CompileFilter validates a filter and compiles it.
//
// A filter that names writer schemas needs them supplied, so use
// CompileFilterWithSchemas for the Avro and Protobuf codecs.
func CompileFilter(filter ConsumerFilter) (*CompiledFilter, error) {
	return CompileFilterWithSchemas(filter, nil)
}

// CompileFilterWithSchemas compiles against the immutable writer schemas the
// caller loaded.
//
// Every schema the filter names must be present and must match its codec, so
// a record can never select a schema the filter was not compiled against.
func CompileFilterWithSchemas(
	filter ConsumerFilter, schemas []SchemaDef,
) (*CompiledFilter, error) {
	if err := filter.Validate(); err != nil {
		return nil, err
	}
	decoders, err := CompilePayloadDecoders(filter, schemas)
	if err != nil {
		return nil, err
	}
	digest, err := filter.Digest()
	if err != nil {
		return nil, err
	}
	root, err := compileNode(filter.Expr)
	if err != nil {
		return nil, err
	}
	return &CompiledFilter{
		filter:       filter,
		digest:       digest,
		root:         root,
		readsHeaders: filter.Expr.ReadsHeaders() || len(filter.SchemaRefs) > 0,
		decoders:     decoders,
	}, nil
}

// Digest is the filter's content digest.
func (compiled *CompiledFilter) Digest() Digest32 { return compiled.digest }

// Filter is the filter the evaluator was compiled from.
func (compiled *CompiledFilter) Filter() ConsumerFilter { return compiled.filter }

// FaultPolicy is what the filter does with a record it could not evaluate.
func (compiled *CompiledFilter) FaultPolicy() FaultPolicy {
	return compiled.filter.FaultPolicy
}

// ReadsPayload reports whether any predicate reads the record payload.
func (compiled *CompiledFilter) ReadsPayload() bool { return compiled.root.readsPayload }

// ReadsHeaders reports whether evaluating can require the record headers.
func (compiled *CompiledFilter) ReadsHeaders() bool { return compiled.readsHeaders }

// HeaderNeed is which headers a caller has to decode before evaluating.
func (compiled *CompiledFilter) HeaderNeed() HeaderNeed {
	switch {
	case compiled.readsHeaders:
		return NeedAllHeaders
	case compiled.root.readsPayload:
		return NeedContentType
	default:
		return NeedNoHeaders
	}
}

// RecordPolicy is the policy covering a fault reason, and false for a decode
// fault, which follows the filter's fault policy instead.
func (compiled *CompiledFilter) RecordPolicy(reason FaultReason) (RecordPolicy, bool) {
	switch {
	case reason.IsForeign():
		return compiled.filter.ForeignPolicy, true
	case reason == FaultTypeMismatch:
		return compiled.filter.MismatchPolicy, true
	default:
		return "", false
	}
}

// PolicyFor is the policy a record that faulted for this reason follows.
//
// A record in another format follows the foreign policy, a type mismatch the
// mismatch policy, and a payload broken in the filter's own codec the fault
// policy. A reject record policy never reaches a fault, so it maps to drop.
func (compiled *CompiledFilter) PolicyFor(reason FaultReason) FaultPolicy {
	policy, named := compiled.RecordPolicy(reason)
	switch {
	case named && policy.IsReject():
		return FaultDrop
	case named:
		return FaultPass
	default:
		return compiled.filter.FaultPolicy
	}
}

// Evaluate reports whether the record is selected, rejected, or could not be
// decoded.
//
// Header predicates run before the payload is touched, and the payload is
// decoded at most once, only when the headers do not already decide.
func (compiled *CompiledFilter) Evaluate(
	record FilterRecord, limits DecodeLimits,
) Verdict {
	verdict, _ := compiled.EvaluateWithFault(record, limits)
	return verdict
}

// EvaluateWithFault evaluates once and retains the decode fault without
// parsing the payload again. The reason is empty when the record decoded.
func (compiled *CompiledFilter) EvaluateWithFault(
	record FilterRecord, limits DecodeLimits,
) (Verdict, FaultReason) {
	context := compiled.context(record, limits)
	truth, fault := context.truth(compiled.root)
	return compiled.evaluated(truth, fault, context.mismatched)
}

// Explain reports each node's truth, for sample tests and previews.
//
// The verdict is the delivery verdict of Evaluate, so headers that decide the
// record still win over an undecodable payload. Every node is evaluated, and a
// payload node carries no truth when the payload does not decode.
func (compiled *CompiledFilter) Explain(
	record FilterRecord, limits DecodeLimits,
) FilterExplanation {
	context := compiled.context(record, limits)
	truth, fault := context.truth(compiled.root)
	verdict, reason := compiled.evaluated(truth, fault, context.mismatched)
	explanation := FilterExplanation{Verdict: verdict, Root: context.explain(compiled.root)}
	if reason != "" {
		explanation.Fault = &reason
	}
	return explanation
}

// evaluated turns a root truth into a delivery verdict.
//
// A record policy of reject decides here: the record is rejected, never
// counted as a fault, and the reason travels with the verdict so a preview or
// sample test can show why it was skipped. A pass policy and every decode
// fault surface as a fault with the reason, which the caller resolves through
// PolicyFor.
func (compiled *CompiledFilter) evaluated(
	truth Truth, fault FaultReason, mismatched bool,
) (Verdict, FaultReason) {
	reason := fault
	if reason == "" {
		switch {
		case truth == TruthMatch:
			return VerdictSelected, ""
		case truth == TruthNoMatch:
			return VerdictRejected, ""
		case !mismatched:
			// An unknown root with nothing mismatched selects nothing, and
			// that is an ordinary rejection rather than a fault.
			return VerdictRejected, ""
		default:
			reason = FaultTypeMismatch
		}
	}
	if policy, named := compiled.RecordPolicy(reason); named && policy.IsReject() {
		return VerdictRejected, reason
	}
	return VerdictFault, reason
}

// evalNode is one compiled predicate.
type evalNode struct {
	kind         FilterExprKind
	readsPayload bool
	children     []evalNode
	path         FieldPath
	op           CmpOp
	value        literal
	coerce       Coerce
	coerced      coercedLiteral
	key          string
	matcher      *TextMatcher
	label        string
}

func compileNode(expr FilterExpr) (evalNode, error) {
	node := evalNode{kind: expr.Kind, readsPayload: expr.ReadsPayload()}
	switch expr.Kind {
	case ExprAll, ExprAny:
		node.children = make([]evalNode, len(expr.Children))
		for index, child := range expr.Children {
			compiled, err := compileNode(child)
			if err != nil {
				return evalNode{}, err
			}
			node.children[index] = compiled
		}
	case ExprNot:
		if expr.Negated == nil {
			return evalNode{}, &InvalidError{Reason: "a negation needs a child expression"}
		}
		child, err := compileNode(*expr.Negated)
		if err != nil {
			return evalNode{}, err
		}
		node.children = []evalNode{child}
	case ExprPred:
		if expr.Pred == nil {
			return evalNode{}, &InvalidError{Reason: "a comparison needs a predicate"}
		}
		path, err := ParseFieldPath(expr.Pred.Field)
		if err != nil {
			return evalNode{}, err
		}
		value, err := compileLiteral(expr.Pred.Value)
		if err != nil {
			return evalNode{}, err
		}
		node.path, node.op, node.value = path, expr.Pred.Op, value
		node.label = fmt.Sprintf("%s %s %s", path, expr.Pred.Op, value)
	case ExprPredAs:
		if expr.PredAs == nil {
			return evalNode{}, &InvalidError{Reason: "a coerced comparison needs a predicate"}
		}
		path, err := ParseFieldPath(expr.PredAs.Pred.Field)
		if err != nil {
			return evalNode{}, err
		}
		coerced, err := compileCoercedLiteral(expr.PredAs.Coerce, expr.PredAs.Pred.Value)
		if err != nil {
			return evalNode{}, err
		}
		node.path, node.op = path, expr.PredAs.Pred.Op
		node.coerce, node.coerced = expr.PredAs.Coerce, coerced
		node.label = fmt.Sprintf("%s as %s %s %s",
			path, expr.PredAs.Coerce.Kind, expr.PredAs.Pred.Op, coerced)
	case ExprPresent, ExprAbsent:
		if expr.Path == nil {
			return evalNode{}, &InvalidError{Reason: "a presence test needs a path"}
		}
		node.path = *expr.Path
		node.label = fmt.Sprintf("%s %s", presenceLabel(expr.Kind), node.path)
	case ExprHeader:
		if expr.Header == nil {
			return evalNode{}, &InvalidError{Reason: "a header comparison needs a predicate"}
		}
		value, err := compileLiteral(expr.Header.Value)
		if err != nil {
			return evalNode{}, err
		}
		node.key, node.op, node.value = expr.Header.Key, expr.Header.Op, value
		node.label = fmt.Sprintf("header %q %s %s", expr.Header.Key, expr.Header.Op, value)
	case ExprText, ExprHeaderText:
		if expr.Text == nil {
			return evalNode{}, &InvalidError{Reason: "a text match needs a predicate"}
		}
		matcher, err := CompileTextMatcher(*expr.Text)
		if err != nil {
			return evalNode{}, err
		}
		node.matcher = matcher
		if expr.Kind == ExprHeaderText {
			node.key = expr.Text.Field
			node.label = fmt.Sprintf("header %q %s %s",
				expr.Text.Field, expr.Text.Kind, matcher.Label())
		} else {
			path, err := ParseFieldPath(expr.Text.Field)
			if err != nil {
				return evalNode{}, err
			}
			node.path = path
			node.label = fmt.Sprintf("%s %s %s", path, expr.Text.Kind, matcher.Label())
		}
	default:
		return evalNode{}, &InvalidError{
			Reason: fmt.Sprintf("unknown filter expression kind %d", expr.Kind)}
	}
	return node, nil
}

func presenceLabel(kind FilterExprKind) string {
	if kind == ExprAbsent {
		return "absent"
	}
	return "present"
}

// evalContext carries the per-record state of one evaluation.
type evalContext struct {
	record   FilterRecord
	limits   DecodeLimits
	codec    FilterCodec
	decoders *PayloadDecoders
	payload  payloadValue
	fault    FaultReason
	decoded  bool
	// mismatched records that a predicate saw a value of a type it cannot
	// compare, which is a type mismatch rather than a plain unknown.
	mismatched bool
}

func (compiled *CompiledFilter) context(
	record FilterRecord, limits DecodeLimits,
) *evalContext {
	return &evalContext{
		record: record, limits: limits,
		codec: compiled.filter.Codec, decoders: compiled.decoders,
	}
}

// decodedPayload decodes the record payload at most once per record.
func (context *evalContext) decodedPayload() (payloadValue, FaultReason) {
	if !context.decoded {
		context.decoded = true
		if context.declaresForeignCodec() {
			context.fault = FaultForeignCodec
		} else {
			context.payload, context.fault = context.decode()
		}
	}
	return context.payload, context.fault
}

func (context *evalContext) decode() (payloadValue, FaultReason) {
	switch context.codec {
	case CodecJSON:
		return decodeJSONPayload(context.record.Payload, context.limits)
	case CodecCBOR:
		return decodeCBORPayload(context.record.Payload, context.limits)
	case CodecAvro, CodecProtobuf:
		if len(context.record.Payload) > context.limits.MaxPayloadBytes {
			return payloadValue{}, FaultTooLarge
		}
		return context.decoders.decodeSchemaPayload(
			context.codec, context.record, context.limits)
	default:
		// A headers-only or unrecognized codec never yields a payload, so a
		// payload predicate under one cannot be answered.
		return payloadValue{}, FaultMalformed
	}
}

// declaresForeignCodec reports whether the record names a codec other than the
// filter's.
//
// The sentinel and codes this build does not know decode as usual: the content
// type dictionary grows, and an unknown code never rejects a record.
func (context *evalContext) declaresForeignCodec() bool {
	header, found := context.header(HeaderContentType)
	if !found {
		return false
	}
	var code uint8
	switch header.Value.Kind {
	case ScalarUint:
		if header.Value.Uint > 255 {
			return false
		}
		code = uint8(header.Value.Uint)
	case ScalarInt:
		if header.Value.Int < 0 || header.Value.Int > 255 {
			return false
		}
		code = uint8(header.Value.Int)
	default:
		return false
	}
	declared, known := ContentTypeFromCode(code)
	if !known || declared == ContentAny {
		return false
	}
	var expected ContentType
	switch context.codec {
	case CodecJSON:
		expected = ContentJSON
	case CodecCBOR:
		expected = ContentCBOR
	case CodecAvro:
		expected = ContentAvro
	case CodecProtobuf:
		expected = ContentProtobuf
	default:
		return false
	}
	return declared != expected
}

// header finds one user header. The last entry with a key wins, as the header
// decoder and the schema id lookup already resolve duplicates.
func (context *evalContext) header(key string) (FilterHeader, bool) {
	for index := len(context.record.Headers) - 1; index >= 0; index-- {
		if context.record.Headers[index].Key == key {
			return context.record.Headers[index], true
		}
	}
	return FilterHeader{}, false
}

// truth evaluates one node. A non-empty fault reason means the payload could
// not be decoded for it.
func (context *evalContext) truth(node evalNode) (Truth, FaultReason) {
	switch node.kind {
	case ExprAll:
		return context.combine(node.children, TruthNoMatch, TruthMatch)
	case ExprAny:
		return context.combine(node.children, TruthMatch, TruthNoMatch)
	case ExprNot:
		truth, fault := context.truth(node.children[0])
		return negate(truth), fault
	case ExprHeader:
		truth, mismatched := context.headerTruth(node)
		context.mismatched = context.mismatched || mismatched
		return truth, ""
	case ExprHeaderText:
		truth, mismatched := context.headerTextTruth(node)
		context.mismatched = context.mismatched || mismatched
		return truth, ""
	default:
		payload, fault := context.decodedPayload()
		if fault != "" {
			return TruthUnknown, fault
		}
		truth, mismatched := payloadTruth(node, payload)
		context.mismatched = context.mismatched || mismatched
		return truth, ""
	}
}

// combine folds a conjunction or disjunction. The decisive truth ends the
// combination at once: no-match for all, match for any.
//
// Children that read no payload run first, so headers can decide the node
// before the payload is ever decoded.
func (context *evalContext) combine(
	children []evalNode, decisive, otherwise Truth,
) (Truth, FaultReason) {
	unknown := false
	for _, headerFirst := range []bool{true, false} {
		for _, child := range children {
			if child.readsPayload == headerFirst {
				continue
			}
			truth, fault := context.truth(child)
			if fault != "" {
				return TruthUnknown, fault
			}
			switch truth {
			case decisive:
				return decisive, ""
			case TruthUnknown:
				unknown = true
			}
		}
	}
	if unknown {
		return TruthUnknown, ""
	}
	return otherwise, ""
}

// headerTruth is the truth of a header comparison, and whether the header
// exists with a type the predicate cannot compare.
func (context *evalContext) headerTruth(node evalNode) (Truth, bool) {
	header, found := context.header(node.key)
	if !found {
		return TruthUnknown, false
	}
	value := headerScalarOf(header.Value)
	truth := compareScalar(value, payloadValue{}, node.op, node.value)
	mismatched := truth == TruthUnknown &&
		!comparableKinds(value, payloadValue{}, node.op, node.value)
	return truth, mismatched
}

func (context *evalContext) headerTextTruth(node evalNode) (Truth, bool) {
	header, found := context.header(node.key)
	if !found {
		return TruthUnknown, false
	}
	value := headerScalarOf(header.Value)
	if value.kind != scalarText {
		return TruthUnknown, true
	}
	return truthOf(node.matcher.Matches(value.text)), false
}

func (context *evalContext) explain(node evalNode) ExplainNode {
	switch node.kind {
	case ExprAll, ExprAny, ExprNot:
	default:
		explained := ExplainNode{Label: node.label}
		switch node.kind {
		case ExprHeader:
			truth, _ := context.headerTruth(node)
			explained.Truth = &truth
		case ExprHeaderText:
			truth, _ := context.headerTextTruth(node)
			explained.Truth = &truth
		default:
			if payload, fault := context.decodedPayload(); fault == "" {
				truth, _ := payloadTruth(node, payload)
				explained.Truth = &truth
			}
		}
		return explained
	}

	children := make([]ExplainNode, len(node.children))
	truths := make([]Truth, 0, len(node.children))
	for index, child := range node.children {
		children[index] = context.explain(child)
		if children[index].Truth != nil {
			truths = append(truths, *children[index].Truth)
		}
	}
	explained := ExplainNode{Label: nodeLabel(node.kind), Children: children}
	// A node whose children did not all resolve has no truth of its own.
	if len(truths) == len(children) {
		var truth Truth
		switch node.kind {
		case ExprAll:
			truth = fold(truths, TruthNoMatch, TruthMatch)
		case ExprAny:
			truth = fold(truths, TruthMatch, TruthNoMatch)
		default:
			truth = negate(truths[0])
		}
		explained.Truth = &truth
	}
	return explained
}

func nodeLabel(kind FilterExprKind) string {
	switch kind {
	case ExprAll:
		return "all"
	case ExprAny:
		return "any"
	default:
		return "not"
	}
}

func fold(truths []Truth, decisive, otherwise Truth) Truth {
	unknown := false
	for _, truth := range truths {
		switch truth {
		case decisive:
			return decisive
		case TruthUnknown:
			unknown = true
		}
	}
	if unknown {
		return TruthUnknown
	}
	return otherwise
}

func negate(truth Truth) Truth {
	switch truth {
	case TruthMatch:
		return TruthNoMatch
	case TruthNoMatch:
		return TruthMatch
	default:
		return TruthUnknown
	}
}

func truthOf(matched bool) Truth {
	if matched {
		return TruthMatch
	}
	return TruthNoMatch
}

// payloadTruth is the truth of one payload leaf, and whether the field exists,
// is not null, and has a type the predicate cannot compare. A missing or null
// field is plain unknown.
func payloadTruth(node evalNode, payload payloadValue) (Truth, bool) {
	switch node.kind {
	case ExprPred:
		field, present := resolvePath(payload, node.path)
		if !present {
			return missingTruth(node.op, node.value), false
		}
		value := scalarOf(field)
		truth := compareScalar(value, field, node.op, node.value)
		mismatched := truth == TruthUnknown &&
			!comparableKinds(value, field, node.op, node.value)
		return truth, mismatched
	case ExprPredAs:
		field, present := resolvePath(payload, node.path)
		if !present {
			return TruthUnknown, false
		}
		coerced, ok := coerceValue(node.coerce, field)
		if !ok {
			return TruthUnknown, !field.isNull()
		}
		return compareCoerced(coerced, node.op, node.coerced), false
	case ExprText:
		field, present := resolvePath(payload, node.path)
		switch {
		case !present || field.isNull():
			return TruthUnknown, false
		case field.kind == payloadText:
			return truthOf(node.matcher.Matches(field.text)), false
		default:
			return TruthUnknown, true
		}
	case ExprPresent:
		_, present := resolvePath(payload, node.path)
		return truthOf(present), false
	case ExprAbsent:
		_, present := resolvePath(payload, node.path)
		return truthOf(!present), false
	default:
		return TruthUnknown, false
	}
}

// missingTruth is the truth of a comparison against a missing field. A missing
// field is unknown, except for the null tests: eq null matches a missing field
// and ne null does not.
func missingTruth(op CmpOp, value literal) Truth {
	if value.kind != litNull {
		return TruthUnknown
	}
	switch op {
	case CmpEq:
		return TruthMatch
	case CmpNe:
		return TruthNoMatch
	default:
		return TruthUnknown
	}
}

// scalarKind is how a decoded value compares.
type scalarKind uint8

const (
	scalarNull scalarKind = iota
	scalarBool
	scalarInt
	scalarFloat
	scalarText
	// scalarComposite is an array, an object, or a value with no scalar
	// meaning in this position.
	scalarComposite
)

// scalar is a value read from the payload or a header, ready to compare.
type scalar struct {
	kind    scalarKind
	flag    bool
	integer wideInt
	float   float64
	text    string
}

// wideInt holds any signed or unsigned 64-bit integer exactly, so a header
// above the signed range and a negative payload field compare in one domain
// without either losing its value to a double.
type wideInt struct {
	negative  bool
	magnitude uint64
}

func intFromSigned(value int64) wideInt {
	if value < 0 {
		// Negating the minimum directly would overflow, so the magnitude is
		// built from the value plus one.
		return wideInt{negative: true, magnitude: uint64(-(value + 1)) + 1}
	}
	return wideInt{magnitude: uint64(value)}
}

func intFromUnsigned(value uint64) wideInt { return wideInt{magnitude: value} }

func (value wideInt) compare(other wideInt) int {
	switch {
	case value.negative != other.negative:
		if value.negative {
			return -1
		}
		return 1
	case value.magnitude == other.magnitude:
		return 0
	case (value.magnitude > other.magnitude) != value.negative:
		return 1
	default:
		return -1
	}
}

// exactDouble is the value as a double, and false when the conversion would
// lose a significant bit. A comparison that cannot be made exactly is
// undecidable rather than silently rounded.
func (value wideInt) exactDouble() (float64, bool) {
	significant := 64 - bits.LeadingZeros64(value.magnitude)
	if significant > 53 && bits.TrailingZeros64(value.magnitude) < significant-53 {
		return 0, false
	}
	converted := float64(value.magnitude)
	if value.negative {
		converted = -converted
	}
	return converted, true
}

// scalarOf reads a decoded payload value as a comparable scalar.
func scalarOf(value payloadValue) scalar {
	switch value.kind {
	case payloadNull:
		return scalar{kind: scalarNull}
	case payloadBool:
		return scalar{kind: scalarBool, flag: value.flag}
	case payloadText:
		return scalar{kind: scalarText, text: value.text}
	case payloadNumber:
		if value.isFloat {
			return scalar{kind: scalarFloat, float: floatOf(value.number)}
		}
		// An integer that fits stays an integer, so exact comparison holds.
		if signed, err := strconv.ParseInt(value.number, 10, 64); err == nil {
			return scalar{kind: scalarInt, integer: intFromSigned(signed)}
		}
		if unsigned, err := strconv.ParseUint(value.number, 10, 64); err == nil {
			return scalar{kind: scalarInt, integer: intFromUnsigned(unsigned)}
		}
		return scalar{kind: scalarFloat, float: floatOf(value.number)}
	default:
		return scalar{kind: scalarComposite}
	}
}

// headerScalarOf reads a typed header value as a comparable scalar.
func headerScalarOf(value HeaderScalar) scalar {
	switch value.Kind {
	case ScalarBool:
		return scalar{kind: scalarBool, flag: value.Bool}
	case ScalarInt:
		return scalar{kind: scalarInt, integer: intFromSigned(value.Int)}
	case ScalarUint:
		return scalar{kind: scalarInt, integer: intFromUnsigned(value.Uint)}
	case ScalarFloat:
		return scalar{kind: scalarFloat, float: value.Float}
	case ScalarString:
		return scalar{kind: scalarText, text: value.Str}
	case ScalarRaw:
		// Raw bytes that spell UTF-8 compare as text, which the shared corpus
		// pins. Anything else has no scalar meaning here.
		if text := string(value.Raw); isValidUTF8Bytes(value.Raw) {
			return scalar{kind: scalarText, text: text}
		}
		return scalar{kind: scalarComposite}
	default:
		return scalar{kind: scalarComposite}
	}
}

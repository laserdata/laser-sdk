use super::codecs::PayloadDecoders;
use crate::content::ContentType;
use crate::control::SchemaDef;
use crate::error::InvalidError;
use crate::filter::coerce::{ExactDecimal, TimestampFormat};
use crate::filter::expr::{
    Coerce, ConsumerFilter, FaultPolicy, FilterCodec, FilterExpr, RecordPolicy,
};
use crate::filter::path::{FieldPath, PathSegment};
use crate::filter::read::{
    ExplainNode, FaultReason, FilterExplanation, FilterHeader, HeaderScalar, Truth, Verdict,
};
use crate::filter::text::TextMatcher;
use crate::headers::CONTENT_TYPE;
use crate::limits::MAX_FILTER_PARSE_DEPTH;
use crate::query::CmpOp;
use crate::schema::{Digest32, TypedValue};
use crate::validate::Validate;
use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;

/// Bounds on decoding one payload. Checked before the payload is parsed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeLimits {
    pub max_payload_bytes: usize,
    pub max_depth: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 1024 * 1024,
            max_depth: 64,
        }
    }
}

/// One record as the evaluator sees it. Headers are read only when the filter
/// has header predicates, so a caller may pass them lazily decoded. Header
/// keys and values are borrowed, so a caller does not copy them per record.
#[derive(Clone, Copy, Debug)]
pub struct FilterRecord<'a> {
    pub payload: &'a [u8],
    pub headers: &'a [HeaderRef<'a>],
}

/// One user header of a record, borrowed from wherever the caller holds it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HeaderRef<'a> {
    pub key: &'a str,
    pub value: HeaderValueRef<'a>,
}

/// A typed user header value, borrowed. Mirrors [`HeaderScalar`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum HeaderValueRef<'a> {
    Bool(bool),
    Int(i64),
    Uint(u64),
    Float(f64),
    String(&'a str),
    Raw(&'a [u8]),
}

impl<'a> From<&'a FilterHeader> for HeaderRef<'a> {
    fn from(header: &'a FilterHeader) -> Self {
        Self {
            key: &header.key,
            value: match &header.value {
                HeaderScalar::Bool(value) => HeaderValueRef::Bool(*value),
                HeaderScalar::Int(value) => HeaderValueRef::Int(*value),
                HeaderScalar::Uint(value) => HeaderValueRef::Uint(*value),
                HeaderScalar::Float(value) => HeaderValueRef::Float(*value),
                HeaderScalar::String(value) => HeaderValueRef::String(value),
                HeaderScalar::Raw(bytes) => HeaderValueRef::Raw(bytes),
            },
        }
    }
}

/// What a caller decodes from a record's header block before evaluating.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HeaderNeed {
    None,
    /// Only `agdx.ct`, to keep a record in another codec out of the decoder.
    ContentType,
    All,
}

/// A validated filter compiled for repeated evaluation. Compile once per
/// digest, evaluate per record.
#[derive(Clone, Debug)]
pub struct CompiledFilter {
    filter: ConsumerFilter,
    digest: Digest32,
    root: Node,
    paths: PathTrie,
    reads_headers: bool,
    decoders: PayloadDecoders,
}

/// The payload paths a filter reads. Decoding keeps only the values on these
/// paths, so a filter that reads four fields of a wide change record does not
/// build the rest of the document. Skipped values are still parsed, so a
/// malformed payload stays a fault.
// Children are a short vector searched in order. A filter reads a handful of
// keys per level, so this beats hashing every key of every record.
#[derive(Clone, Debug, Default)]
pub(super) struct PathTrie {
    pub(super) whole: bool,
    indices: Vec<(u32, PathTrie)>,
    keys: Vec<(String, PathTrie)>,
}

#[derive(Clone, Debug)]
struct Node {
    kind: NodeKind,
    reads_payload: bool,
}

#[derive(Clone, Debug)]
enum NodeKind {
    All(Vec<Node>),
    Any(Vec<Node>),
    Not(Box<Node>),
    Compare {
        path: FieldPath,
        op: CmpOp,
        literal: Literal,
    },
    Coerced {
        path: FieldPath,
        op: CmpOp,
        coerce: Coerce,
        literal: CoercedLiteral,
    },
    Present(FieldPath),
    Absent(FieldPath),
    Header {
        key: String,
        op: CmpOp,
        literal: Literal,
    },
    Text {
        path: FieldPath,
        matcher: TextMatcher,
    },
    HeaderText {
        key: String,
        matcher: TextMatcher,
    },
}

#[derive(Clone, Debug, PartialEq)]
enum Literal {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Text(String),
    List(Vec<Literal>),
}

#[derive(Clone, Debug, PartialEq)]
enum CoercedLiteral {
    Instant(i64),
    Decimal(ExactDecimal),
    List(Vec<CoercedLiteral>),
}

#[derive(Clone, Debug, PartialEq)]
enum Comparable {
    Instant(i64),
    Decimal(ExactDecimal),
}

/// A value read from the payload or a header, ready to compare.
enum Scalar<'a> {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Text(&'a str),
    Composite,
}

impl CompiledFilter {
    /// Validate and compile.
    pub fn compile(filter: &ConsumerFilter) -> Result<Self, InvalidError> {
        Self::compile_with_schemas(filter, &[])
    }

    /// Compile against immutable writer schemas loaded by the caller.
    pub fn compile_with_schemas(
        filter: &ConsumerFilter,
        schemas: &[SchemaDef],
    ) -> Result<Self, InvalidError> {
        filter.validate()?;
        let decoders = PayloadDecoders::compile(filter, schemas)?;
        let mut paths = PathTrie::default();
        collect_paths(&filter.expr, &mut paths)?;
        Ok(Self {
            filter: filter.clone(),
            digest: filter.digest(),
            root: compile_node(&filter.expr)?,
            paths,
            reads_headers: filter.expr.reads_headers() || !filter.schema_refs.is_empty(),
            decoders,
        })
    }

    pub fn digest(&self) -> &Digest32 {
        &self.digest
    }

    pub fn filter(&self) -> &ConsumerFilter {
        &self.filter
    }

    pub fn fault_policy(&self) -> FaultPolicy {
        self.filter.fault_policy
    }

    /// The policy a record that faulted for `reason` follows. A record in
    /// another format follows the foreign policy, a type mismatch the mismatch
    /// policy, and a payload broken in the filter's own codec the fault policy.
    /// A `reject` record policy never reaches a fault, so it maps to `drop`.
    pub fn policy_for(&self, reason: FaultReason) -> FaultPolicy {
        match self.record_policy(reason) {
            Some(RecordPolicy::Reject) => FaultPolicy::Drop,
            Some(RecordPolicy::Pass) => FaultPolicy::Pass,
            None => self.filter.fault_policy,
        }
    }

    /// The record policy that covers `reason`, or `None` for a decode fault.
    pub fn record_policy(&self, reason: FaultReason) -> Option<RecordPolicy> {
        if reason.is_foreign() {
            Some(self.filter.foreign_policy)
        } else if reason == FaultReason::TypeMismatch {
            Some(self.filter.mismatch_policy)
        } else {
            None
        }
    }

    pub fn reads_payload(&self) -> bool {
        self.root.reads_payload
    }

    pub fn reads_headers(&self) -> bool {
        self.reads_headers
    }

    /// Which headers a caller has to decode before evaluating. A payload filter
    /// without header predicates needs only `agdx.ct`, which
    /// [`headers::content_type`](crate::filter::headers::content_type) reads
    /// without decoding the rest of the block.
    pub fn header_need(&self) -> HeaderNeed {
        if self.reads_headers {
            HeaderNeed::All
        } else if self.root.reads_payload {
            HeaderNeed::ContentType
        } else {
            HeaderNeed::None
        }
    }

    /// Whether the record is selected, rejected, or could not be decoded.
    /// Header predicates run before the payload is touched, and the payload is
    /// decoded at most once, only when the headers do not already decide.
    pub fn evaluate(&self, record: &FilterRecord<'_>, limits: &DecodeLimits) -> Verdict {
        self.evaluate_with_fault(record, limits).0
    }

    /// Evaluate once and retain the decode fault without parsing the payload again.
    pub fn evaluate_with_fault(
        &self,
        record: &FilterRecord<'_>,
        limits: &DecodeLimits,
    ) -> (Verdict, Option<FaultReason>) {
        let mut context = Context::new(
            record,
            limits,
            self.filter.codec,
            &self.paths,
            &self.decoders,
        );
        let truth = context.truth(&self.root);
        self.evaluated(truth, context.mismatched)
    }

    /// Report each node's truth, for sample tests and previews. The verdict is
    /// the delivery verdict of [`evaluate`](Self::evaluate), so headers that
    /// decide the record still win over an undecodable payload. The tree
    /// evaluates every node, and payload nodes carry no truth when the payload
    /// does not decode.
    pub fn explain(&self, record: &FilterRecord<'_>, limits: &DecodeLimits) -> FilterExplanation {
        let mut context = Context::new(
            record,
            limits,
            self.filter.codec,
            &self.paths,
            &self.decoders,
        );
        let truth = context.truth(&self.root);
        let (verdict, fault) = self.evaluated(truth, context.mismatched);
        let root = context.explain(&self.root);
        FilterExplanation {
            verdict,
            fault,
            root,
        }
    }
}

impl CompiledFilter {
    // A record policy of `reject` decides here: the record is rejected, never
    // counted as a fault, and the reason travels with the verdict so a preview
    // or sample test can show why it was skipped. `pass` and every decode
    // fault surface as a fault with the reason, which the caller resolves
    // through `policy_for`.
    fn evaluated(
        &self,
        truth: Result<Truth, FaultReason>,
        mismatched: bool,
    ) -> (Verdict, Option<FaultReason>) {
        let reason = match truth {
            Ok(Truth::Match) => return (Verdict::Selected, None),
            Ok(Truth::NoMatch) => return (Verdict::Rejected, None),
            Ok(Truth::Unknown) if !mismatched => return (Verdict::Rejected, None),
            Ok(Truth::Unknown) => FaultReason::TypeMismatch,
            Err(reason) => reason,
        };
        match self.record_policy(reason) {
            Some(RecordPolicy::Reject) => (Verdict::Rejected, Some(reason)),
            Some(RecordPolicy::Pass) | None => (Verdict::Fault, Some(reason)),
        }
    }
}

fn compile_node(expr: &FilterExpr) -> Result<Node, InvalidError> {
    let kind = match expr {
        FilterExpr::All(children) => NodeKind::All(
            children
                .iter()
                .map(compile_node)
                .collect::<Result<_, _>>()?,
        ),
        FilterExpr::Any(children) => NodeKind::Any(
            children
                .iter()
                .map(compile_node)
                .collect::<Result<_, _>>()?,
        ),
        FilterExpr::Not(child) => NodeKind::Not(Box::new(compile_node(child)?)),
        FilterExpr::Pred(predicate) => NodeKind::Compare {
            path: FieldPath::parse(&predicate.field)?,
            op: predicate.op,
            literal: compile_literal(&predicate.value)?,
        },
        FilterExpr::PredAs(coerced) => NodeKind::Coerced {
            path: FieldPath::parse(&coerced.pred.field)?,
            op: coerced.pred.op,
            coerce: coerced.coerce,
            literal: compile_coerced_literal(coerced.coerce, &coerced.pred.value)?,
        },
        FilterExpr::Present(path) => NodeKind::Present(path.clone()),
        FilterExpr::Absent(path) => NodeKind::Absent(path.clone()),
        FilterExpr::Header(header) => NodeKind::Header {
            key: header.key.clone(),
            op: header.op,
            literal: compile_literal(&header.value)?,
        },
        FilterExpr::Text(predicate) => NodeKind::Text {
            path: FieldPath::parse(&predicate.field)?,
            matcher: TextMatcher::compile(predicate)?,
        },
        FilterExpr::HeaderText(predicate) => NodeKind::HeaderText {
            key: predicate.field.clone(),
            matcher: TextMatcher::compile(predicate)?,
        },
    };
    Ok(Node {
        kind,
        reads_payload: expr.reads_payload(),
    })
}

fn compile_literal(value: &TypedValue) -> Result<Literal, InvalidError> {
    Ok(match value {
        TypedValue::Null => Literal::Null,
        TypedValue::Boolean(value) => Literal::Bool(*value),
        TypedValue::Int(value) => Literal::Int(i128::from(*value)),
        TypedValue::Long(value) => Literal::Int(i128::from(*value)),
        TypedValue::Double(value) => Literal::Float(*value),
        TypedValue::String(value) => Literal::Text(value.clone()),
        TypedValue::List(items) => Literal::List(
            items
                .iter()
                .map(compile_literal)
                .collect::<Result<_, _>>()?,
        ),
        other => {
            return Err(InvalidError::new(format!(
                "unsupported filter literal {}",
                other.diagnostic_text()
            )));
        }
    })
}

fn compile_coerced_literal(
    coerce: Coerce,
    value: &TypedValue,
) -> Result<CoercedLiteral, InvalidError> {
    if let TypedValue::List(items) = value {
        return items
            .iter()
            .map(|item| compile_coerced_literal(coerce, item))
            .collect::<Result<_, _>>()
            .map(CoercedLiteral::List);
    }
    let comparable = match (coerce, value) {
        (Coerce::Timestamp { format }, TypedValue::String(text)) => {
            format.micros_from_text(text).map(Comparable::Instant)
        }
        (Coerce::Timestamp { format }, TypedValue::Int(number)) => format
            .micros_from_integer(i128::from(*number))
            .map(Comparable::Instant),
        (Coerce::Timestamp { format }, TypedValue::Long(number)) => format
            .micros_from_integer(i128::from(*number))
            .map(Comparable::Instant),
        (Coerce::Number, TypedValue::String(text)) => {
            ExactDecimal::parse(text).map(Comparable::Decimal)
        }
        (Coerce::Number, TypedValue::Int(number)) => Some(Comparable::Decimal(
            ExactDecimal::from_integer(i128::from(*number)),
        )),
        (Coerce::Number, TypedValue::Long(number)) => Some(Comparable::Decimal(
            ExactDecimal::from_integer(i128::from(*number)),
        )),
        _ => None,
    };
    match comparable {
        Some(Comparable::Instant(micros)) => Ok(CoercedLiteral::Instant(micros)),
        Some(Comparable::Decimal(decimal)) => Ok(CoercedLiteral::Decimal(decimal)),
        None => Err(InvalidError::new(format!(
            "literal {} does not parse under its coercion",
            value.diagnostic_text()
        ))),
    }
}

struct Context<'a> {
    record: &'a FilterRecord<'a>,
    limits: &'a DecodeLimits,
    codec: FilterCodec,
    paths: &'a PathTrie,
    decoders: &'a PayloadDecoders,
    payload: Option<Result<Value, FaultReason>>,
    // A predicate saw a value of a type it cannot compare.
    mismatched: bool,
}

impl<'a> Context<'a> {
    fn new(
        record: &'a FilterRecord<'a>,
        limits: &'a DecodeLimits,
        codec: FilterCodec,
        paths: &'a PathTrie,
        decoders: &'a PayloadDecoders,
    ) -> Self {
        Self {
            record,
            limits,
            codec,
            paths,
            decoders,
            payload: None,
            mismatched: false,
        }
    }

    fn payload(&mut self) -> Result<&Value, FaultReason> {
        if self.payload.is_none() {
            let decoded = if self.declares_foreign_codec() {
                Err(FaultReason::ForeignCodec)
            } else {
                self.decode()
            };
            self.payload = Some(decoded);
        }
        match self.payload.as_ref().expect("the payload was just decoded") {
            Ok(value) => Ok(value),
            Err(reason) => Err(*reason),
        }
    }

    // `any` and codes this build does not know decode as usual: the content
    // type dictionary grows, and an unknown code never rejects a record.
    fn declares_foreign_codec(&self) -> bool {
        let Some(header) = self.header(CONTENT_TYPE) else {
            return false;
        };
        let code = match header.value {
            HeaderValueRef::Uint(code) => u8::try_from(code).ok(),
            HeaderValueRef::Int(code) => u8::try_from(code).ok(),
            _ => None,
        };
        let declared = match code.and_then(ContentType::from_code) {
            None | Some(ContentType::Any) => return false,
            Some(declared) => declared,
        };
        let expected = match self.codec {
            FilterCodec::Json => ContentType::Json,
            FilterCodec::Cbor => ContentType::Cbor,
            FilterCodec::Avro => ContentType::Avro,
            FilterCodec::Protobuf => ContentType::Protobuf,
            FilterCodec::HeadersOnly | FilterCodec::Unknown => return false,
        };
        declared != expected
    }

    fn decode(&self) -> Result<Value, FaultReason> {
        match self.codec {
            FilterCodec::Json => decode_json_paths(self.record.payload, self.limits, self.paths),
            FilterCodec::Cbor | FilterCodec::Avro | FilterCodec::Protobuf => self
                .decoders
                .decode_paths(self.codec, self.record, self.limits, self.paths),
            FilterCodec::HeadersOnly | FilterCodec::Unknown => Err(FaultReason::Malformed),
        }
    }

    fn truth(&mut self, node: &Node) -> Result<Truth, FaultReason> {
        match &node.kind {
            NodeKind::All(children) => self.combine(children, Truth::NoMatch, Truth::Match),
            NodeKind::Any(children) => self.combine(children, Truth::Match, Truth::NoMatch),
            NodeKind::Not(child) => self.truth(child).map(negate),
            NodeKind::Header { key, op, literal } => {
                let (truth, mismatched) = self.header_truth(key, *op, literal);
                self.mismatched |= mismatched;
                Ok(truth)
            }
            NodeKind::HeaderText { key, matcher } => {
                let (truth, mismatched) = self.header_text_truth(key, matcher);
                self.mismatched |= mismatched;
                Ok(truth)
            }
            leaf => {
                let value = self.payload()?;
                let (truth, mismatched) = payload_truth(leaf, value);
                self.mismatched |= mismatched;
                Ok(truth)
            }
        }
    }

    // `decisive` ends the combination at once (`no_match` for all, `match` for
    // any). Children that read no payload run first, so headers can decide the
    // node before the payload is ever decoded.
    fn combine(
        &mut self,
        children: &[Node],
        decisive: Truth,
        otherwise: Truth,
    ) -> Result<Truth, FaultReason> {
        let mut unknown = false;
        let header_first = children
            .iter()
            .filter(|child| !child.reads_payload)
            .chain(children.iter().filter(|child| child.reads_payload));
        for child in header_first {
            match self.truth(child)? {
                truth if truth == decisive => return Ok(decisive),
                Truth::Unknown => unknown = true,
                _ => {}
            }
        }
        Ok(if unknown { Truth::Unknown } else { otherwise })
    }

    // The truth, and whether the header exists with a type the predicate
    // cannot compare.
    fn header_truth(&self, key: &str, op: CmpOp, literal: &Literal) -> (Truth, bool) {
        let Some(header) = self.header(key) else {
            return (Truth::Unknown, false);
        };
        let scalar = header_scalar(header.value);
        let truth = compare_scalar(&scalar, None, op, literal);
        (
            truth,
            truth == Truth::Unknown && !comparable(&scalar, None, op, literal),
        )
    }

    fn header_text_truth(&self, key: &str, matcher: &TextMatcher) -> (Truth, bool) {
        let Some(header) = self.header(key) else {
            return (Truth::Unknown, false);
        };
        match header_scalar(header.value) {
            Scalar::Text(text) => (truth_of(matcher.matches(text)), false),
            _ => (Truth::Unknown, true),
        }
    }

    // The last entry with a key wins, as the header decoder and the schema id
    // lookup already resolve duplicates.
    fn header(&self, key: &str) -> Option<&HeaderRef<'a>> {
        self.record
            .headers
            .iter()
            .rev()
            .find(|header| header.key == key)
    }

    fn explain(&mut self, node: &Node) -> ExplainNode {
        let (label, children) = match &node.kind {
            NodeKind::All(children) => ("all".to_owned(), children.as_slice()),
            NodeKind::Any(children) => ("any".to_owned(), children.as_slice()),
            NodeKind::Not(child) => ("not".to_owned(), std::slice::from_ref(child.as_ref())),
            leaf => {
                let truth = match leaf {
                    NodeKind::Header { key, op, literal } => {
                        Some(self.header_truth(key, *op, literal).0)
                    }
                    NodeKind::HeaderText { key, matcher } => {
                        Some(self.header_text_truth(key, matcher).0)
                    }
                    _ => self
                        .payload()
                        .ok()
                        .map(|value| payload_truth(leaf, value).0),
                };
                return ExplainNode {
                    label: leaf_label(leaf),
                    truth,
                    children: Vec::new(),
                };
            }
        };
        let explained: Vec<ExplainNode> =
            children.iter().map(|child| self.explain(child)).collect();
        let truths: Option<Vec<Truth>> = explained.iter().map(|child| child.truth).collect();
        let truth = truths.map(|truths| match &node.kind {
            NodeKind::All(_) => fold(&truths, Truth::NoMatch, Truth::Match),
            NodeKind::Any(_) => fold(&truths, Truth::Match, Truth::NoMatch),
            _ => negate(truths[0]),
        });
        ExplainNode {
            label,
            truth,
            children: explained,
        }
    }
}

fn fold(truths: &[Truth], decisive: Truth, otherwise: Truth) -> Truth {
    if truths.contains(&decisive) {
        decisive
    } else if truths.contains(&Truth::Unknown) {
        Truth::Unknown
    } else {
        otherwise
    }
}

const fn negate(truth: Truth) -> Truth {
    match truth {
        Truth::Match => Truth::NoMatch,
        Truth::NoMatch => Truth::Match,
        Truth::Unknown => Truth::Unknown,
    }
}

// The truth, and whether the field exists, is not null, and has a type the
// predicate cannot compare. A missing or null field is plain unknown.
fn payload_truth(leaf: &NodeKind, value: &Value) -> (Truth, bool) {
    match leaf {
        NodeKind::Compare { path, op, literal } => match resolve(value, path) {
            None => (missing_truth(*op, literal), false),
            Some(field) => {
                let scalar = scalar_of(field);
                let truth = compare_scalar(&scalar, Some(field), *op, literal);
                let mismatched =
                    truth == Truth::Unknown && !comparable(&scalar, Some(field), *op, literal);
                (truth, mismatched)
            }
        },
        NodeKind::Coerced {
            path,
            op,
            coerce,
            literal,
        } => match resolve(value, path) {
            None => (Truth::Unknown, false),
            Some(field) => match coerce_value(*coerce, field) {
                Some(comparable) => (compare_coerced(&comparable, *op, literal), false),
                None => (Truth::Unknown, !field.is_null()),
            },
        },
        NodeKind::Text { path, matcher } => match resolve(value, path) {
            None | Some(Value::Null) => (Truth::Unknown, false),
            Some(Value::String(text)) => (truth_of(matcher.matches(text)), false),
            Some(_) => (Truth::Unknown, true),
        },
        NodeKind::Present(path) => (truth_of(resolve(value, path).is_some()), false),
        NodeKind::Absent(path) => (truth_of(resolve(value, path).is_none()), false),
        NodeKind::All(_)
        | NodeKind::Any(_)
        | NodeKind::Not(_)
        | NodeKind::Header { .. }
        | NodeKind::HeaderText { .. } => unreachable!("only payload leaves read the payload"),
    }
}

fn header_scalar(value: HeaderValueRef<'_>) -> Scalar<'_> {
    match value {
        HeaderValueRef::Bool(value) => Scalar::Bool(value),
        HeaderValueRef::Int(value) => Scalar::Int(i128::from(value)),
        HeaderValueRef::Uint(value) => Scalar::Int(i128::from(value)),
        HeaderValueRef::Float(value) => Scalar::Float(value),
        HeaderValueRef::String(value) => Scalar::Text(value),
        // Raw bytes that spell UTF-8 compare as text, pinned by the shared
        // corpus. Anything else has no scalar meaning here.
        HeaderValueRef::Raw(bytes) => match std::str::from_utf8(bytes) {
            Ok(text) => Scalar::Text(text),
            Err(_) => Scalar::Composite,
        },
    }
}

const fn truth_of(matched: bool) -> Truth {
    if matched {
        Truth::Match
    } else {
        Truth::NoMatch
    }
}

// Whether the value's kind can be compared with the literal's kind at all. A
// comparison that stays unknown between comparable kinds, such as an integer
// beyond 2^53 against a double or a float header holding NaN, is undecidable,
// not a type mismatch. A null value is never a mismatch.
fn comparable(scalar: &Scalar<'_>, field: Option<&Value>, op: CmpOp, literal: &Literal) -> bool {
    match (scalar, literal) {
        (Scalar::Null, _) | (_, Literal::Null) => true,
        (Scalar::Text(_), Literal::Text(_)) | (Scalar::Bool(_), Literal::Bool(_)) => true,
        (Scalar::Int(_) | Scalar::Float(_), Literal::Int(_) | Literal::Float(_)) => true,
        (scalar, Literal::List(items)) => {
            items.iter().any(|item| comparable(scalar, field, op, item))
        }
        (Scalar::Composite, _) => op == CmpOp::Contains && matches!(field, Some(Value::Array(_))),
        _ => false,
    }
}

// A missing field is unknown, except for the null tests: `eq null` matches a
// missing field and `ne null` does not.
fn missing_truth(op: CmpOp, literal: &Literal) -> Truth {
    match (op, literal) {
        (CmpOp::Eq, Literal::Null) => Truth::Match,
        (CmpOp::Ne, Literal::Null) => Truth::NoMatch,
        _ => Truth::Unknown,
    }
}

fn compare_scalar(
    scalar: &Scalar<'_>,
    field: Option<&Value>,
    op: CmpOp,
    literal: &Literal,
) -> Truth {
    match (scalar, literal) {
        (Scalar::Null, Literal::Null) => return truth_of(op == CmpOp::Eq),
        (Scalar::Null, _) => return Truth::Unknown,
        (_, Literal::Null) => return truth_of(op == CmpOp::Ne),
        _ => {}
    }
    match op {
        CmpOp::Eq => scalar_eq(scalar, literal).map_or(Truth::Unknown, truth_of),
        CmpOp::Ne => scalar_eq(scalar, literal).map_or(Truth::Unknown, |equal| truth_of(!equal)),
        CmpOp::Lt | CmpOp::Lte | CmpOp::Gt | CmpOp::Gte => match scalar_cmp(scalar, literal) {
            Some(ordering) => truth_of(match op {
                CmpOp::Lt => ordering == Ordering::Less,
                CmpOp::Lte => ordering != Ordering::Greater,
                CmpOp::Gt => ordering == Ordering::Greater,
                _ => ordering != Ordering::Less,
            }),
            None => Truth::Unknown,
        },
        CmpOp::In => {
            let Literal::List(items) = literal else {
                return Truth::Unknown;
            };
            let mut unknown = false;
            for item in items {
                match scalar_eq(scalar, item) {
                    Some(true) => return Truth::Match,
                    Some(false) => {}
                    None => unknown = true,
                }
            }
            if unknown {
                Truth::Unknown
            } else {
                Truth::NoMatch
            }
        }
        CmpOp::Contains => match (scalar, literal, field) {
            (Scalar::Text(text), Literal::Text(needle), _) => {
                truth_of(text.contains(needle.as_str()))
            }
            (Scalar::Composite, _, Some(Value::Array(elements))) => truth_of(
                elements
                    .iter()
                    .any(|element| scalar_eq(&scalar_of(element), literal) == Some(true)),
            ),
            _ => Truth::Unknown,
        },
        CmpOp::Prefix => match (scalar, literal) {
            (Scalar::Text(text), Literal::Text(prefix)) => {
                truth_of(text.starts_with(prefix.as_str()))
            }
            _ => Truth::Unknown,
        },
    }
}

fn scalar_eq(scalar: &Scalar<'_>, literal: &Literal) -> Option<bool> {
    match (scalar, literal) {
        (Scalar::Bool(left), Literal::Bool(right)) => Some(left == right),
        (Scalar::Text(left), Literal::Text(right)) => Some(*left == right.as_str()),
        _ => scalar_cmp(scalar, literal).map(|ordering| ordering == Ordering::Equal),
    }
}

fn scalar_cmp(scalar: &Scalar<'_>, literal: &Literal) -> Option<Ordering> {
    match (scalar, literal) {
        (Scalar::Int(left), Literal::Int(right)) => Some(left.cmp(right)),
        (Scalar::Float(left), Literal::Float(right)) => left.partial_cmp(right),
        (Scalar::Int(left), Literal::Float(right)) => exact_double(*left)?.partial_cmp(right),
        (Scalar::Float(left), Literal::Int(right)) => left.partial_cmp(&exact_double(*right)?),
        (Scalar::Text(left), Literal::Text(right)) => Some(left.as_bytes().cmp(right.as_bytes())),
        _ => None,
    }
}

#[allow(clippy::cast_precision_loss)]
fn exact_double(value: i128) -> Option<f64> {
    let magnitude = value.unsigned_abs();
    let significant_bits = u128::BITS - magnitude.leading_zeros();
    (significant_bits <= 53 || magnitude.trailing_zeros() >= significant_bits - 53)
        .then_some(value as f64)
}

fn compare_coerced(comparable: &Comparable, op: CmpOp, literal: &CoercedLiteral) -> Truth {
    let ordering = |literal: &CoercedLiteral| match (comparable, literal) {
        (Comparable::Instant(left), CoercedLiteral::Instant(right)) => Some(left.cmp(right)),
        (Comparable::Decimal(left), CoercedLiteral::Decimal(right)) => Some(left.cmp(right)),
        _ => None,
    };
    match (op, literal) {
        (CmpOp::In, CoercedLiteral::List(items)) => truth_of(
            items
                .iter()
                .any(|item| ordering(item) == Some(Ordering::Equal)),
        ),
        (_, literal) => match ordering(literal) {
            Some(ordering) => truth_of(match op {
                CmpOp::Eq => ordering == Ordering::Equal,
                CmpOp::Ne => ordering != Ordering::Equal,
                CmpOp::Lt => ordering == Ordering::Less,
                CmpOp::Lte => ordering != Ordering::Greater,
                CmpOp::Gt => ordering == Ordering::Greater,
                CmpOp::Gte => ordering != Ordering::Less,
                CmpOp::In | CmpOp::Contains | CmpOp::Prefix => return Truth::Unknown,
            }),
            None => Truth::Unknown,
        },
    }
}

fn coerce_value(coerce: Coerce, field: &Value) -> Option<Comparable> {
    match coerce {
        Coerce::Timestamp { format } => {
            let micros = match (format, field) {
                (_, Value::String(text)) => format.micros_from_text(text),
                (TimestampFormat::Rfc3339, _) => None,
                (_, Value::Number(number)) => {
                    let integer = number
                        .as_i64()
                        .map(i128::from)
                        .or_else(|| number.as_u64().map(i128::from))?;
                    format.micros_from_integer(integer)
                }
                _ => None,
            }?;
            Some(Comparable::Instant(micros))
        }
        Coerce::Number => {
            let decimal = match field {
                Value::String(text) => ExactDecimal::parse(text),
                Value::Number(number) => number
                    .as_i64()
                    .map(|value| ExactDecimal::from_integer(i128::from(value)))
                    .or_else(|| {
                        number
                            .as_u64()
                            .map(|value| ExactDecimal::from_integer(i128::from(value)))
                    })
                    .or_else(|| number.as_f64().and_then(ExactDecimal::from_f64)),
                _ => None,
            }?;
            Some(Comparable::Decimal(decimal))
        }
    }
}

fn scalar_of(value: &Value) -> Scalar<'_> {
    match value {
        Value::Null => Scalar::Null,
        Value::Bool(value) => Scalar::Bool(*value),
        Value::Number(number) => number
            .as_i64()
            .map(|value| Scalar::Int(i128::from(value)))
            .or_else(|| number.as_u64().map(|value| Scalar::Int(i128::from(value))))
            .or_else(|| number.as_f64().map(Scalar::Float))
            .unwrap_or(Scalar::Composite),
        Value::String(text) => Scalar::Text(text),
        Value::Array(_) | Value::Object(_) => Scalar::Composite,
    }
}

fn resolve<'v>(value: &'v Value, path: &FieldPath) -> Option<&'v Value> {
    path.segments()
        .iter()
        .try_fold(value, |current, segment| match (segment, current) {
            (PathSegment::Key(key), Value::Object(map)) => map.get(key),
            (PathSegment::Index(index), Value::Array(items)) => items.get(*index as usize),
            _ => None,
        })
}

/// Decode a JSON payload within `limits`. The depth is checked by a scan over
/// the raw bytes before any value is built, so a hostile payload cannot make
/// the parser recurse or allocate past the limit.
pub fn decode_json(payload: &[u8], limits: &DecodeLimits) -> Result<Value, FaultReason> {
    decode_json_paths(payload, limits, &PathTrie::whole())
}

// The depth scan also bounds subtrees that the filter does not retain.
fn decode_json_paths(
    payload: &[u8],
    limits: &DecodeLimits,
    paths: &PathTrie,
) -> Result<Value, FaultReason> {
    if payload.len() > limits.max_payload_bytes {
        return Err(FaultReason::TooLarge);
    }
    if !json_depth_within(payload, limits.max_depth.min(MAX_FILTER_PARSE_DEPTH)) {
        return Err(FaultReason::TooDeep);
    }
    let mut deserializer = serde_json::Deserializer::from_slice(payload);
    let value = Pruned(paths)
        .deserialize(&mut deserializer)
        .map_err(|_| FaultReason::Malformed)?;
    deserializer.end().map_err(|_| FaultReason::Malformed)?;
    Ok(value)
}

impl PathTrie {
    pub(super) fn whole() -> Self {
        Self {
            whole: true,
            ..Self::default()
        }
    }

    pub(super) fn child(&self, key: &str) -> Option<&PathTrie> {
        self.keys
            .iter()
            .find_map(|(name, child)| (name == key).then_some(child))
    }

    fn child_mut(&mut self, key: &str) -> &mut PathTrie {
        let index = match self.keys.iter().position(|(name, _)| name == key) {
            Some(index) => index,
            None => {
                self.keys.push((key.to_owned(), PathTrie::default()));
                self.keys.len() - 1
            }
        };
        &mut self.keys[index].1
    }

    pub(super) fn index(&self, index: u32) -> Option<&PathTrie> {
        self.indices
            .iter()
            .find_map(|(number, child)| (*number == index).then_some(child))
    }

    fn index_mut(&mut self, index: u32) -> &mut PathTrie {
        let position = match self.indices.iter().position(|(number, _)| *number == index) {
            Some(position) => position,
            None => {
                self.indices.push((index, Self::default()));
                self.indices.len() - 1
            }
        };
        &mut self.indices[position].1
    }

    fn insert(&mut self, path: &FieldPath) {
        let mut node = self;
        for segment in path.segments() {
            if node.whole {
                return;
            }
            match segment {
                PathSegment::Key(key) => node = node.child_mut(key),
                PathSegment::Index(index) => node = node.index_mut(*index),
            }
        }
        node.whole = true;
        node.keys.clear();
        node.indices.clear();
    }
}

fn collect_paths(expr: &FilterExpr, paths: &mut PathTrie) -> Result<(), InvalidError> {
    match expr {
        FilterExpr::All(children) | FilterExpr::Any(children) => {
            for child in children {
                collect_paths(child, paths)?;
            }
        }
        FilterExpr::Not(child) => collect_paths(child, paths)?,
        FilterExpr::Pred(predicate) => paths.insert(&FieldPath::parse(&predicate.field)?),
        FilterExpr::PredAs(coerced) => paths.insert(&FieldPath::parse(&coerced.pred.field)?),
        FilterExpr::Present(path) | FilterExpr::Absent(path) => paths.insert(path),
        FilterExpr::Text(predicate) => paths.insert(&FieldPath::parse(&predicate.field)?),
        FilterExpr::Header(_) | FilterExpr::HeaderText(_) => {}
    }
    Ok(())
}

// Builds a `serde_json::Value` that holds only the values on the trie's paths.
// Values off the paths are validated without retention. Array positions stay
// stable, and a repeated key keeps its last value, as `Value` does.
struct Pruned<'t>(&'t PathTrie);

impl<'de> DeserializeSeed<'de> for Pruned<'_> {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        if self.0.whole {
            return Value::deserialize(deserializer);
        }
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Pruned<'_> {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Value, E> {
        Ok(Value::from(value))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Value, E> {
        Ok(Value::from(value))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Value, E> {
        Ok(serde_json::Number::from_f64(value).map_or(Value::Null, Value::Number))
    }

    fn visit_str<E>(self, _value: &str) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut items = Vec::new();
        let ignored = PathTrie::default();
        let mut index = 0_u32;
        while let Some(item) =
            seq.next_element_seed(Pruned(self.0.index(index).unwrap_or(&ignored)))?
        {
            if self.0.index(index).is_some() {
                items.resize(index as usize, Value::Null);
                items.push(item);
            }
            index = index.saturating_add(1);
        }
        Ok(Value::Array(items))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut object = Map::new();
        let ignored = PathTrie::default();
        while let Some(JsonKey(key)) = map.next_key::<JsonKey<'_>>()? {
            match self.0.child(key.as_ref()) {
                Some(child) => {
                    let value = map.next_value_seed(Pruned(child))?;
                    object.insert(key.into_owned(), value);
                }
                None => {
                    // IgnoredAny accepts overflowing numbers and lone
                    // surrogates. Unused fields must obey the same JSON
                    // rules as fields the filter reads.
                    map.next_value_seed(Pruned(&ignored))?;
                }
            }
        }
        Ok(Value::Object(object))
    }
}

struct JsonKey<'a>(Cow<'a, str>);

impl<'de> Deserialize<'de> for JsonKey<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;
        impl<'de> Visitor<'de> for KeyVisitor {
            type Value = JsonKey<'de>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object key")
            }
            fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
                Ok(JsonKey(Cow::Borrowed(value)))
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
                Ok(JsonKey(Cow::Owned(value.to_owned())))
            }
            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(JsonKey(Cow::Owned(value)))
            }
        }
        deserializer.deserialize_str(KeyVisitor)
    }
}

// String bodies are skipped with memchr, which finds the next quote or
// backslash many bytes at a time. Most payload bytes sit inside strings.
fn json_depth_within(payload: &[u8], max_depth: usize) -> bool {
    let mut depth = 0usize;
    let mut index = 0;
    while index < payload.len() {
        match payload[index] {
            b'"' => {
                index += 1;
                loop {
                    let Some(found) = memchr::memchr2(b'"', b'\\', &payload[index..]) else {
                        return true;
                    };
                    index += found;
                    if payload[index] == b'"' {
                        break;
                    }
                    index += 2;
                    if index >= payload.len() {
                        return true;
                    }
                }
            }
            b'[' | b'{' => {
                depth += 1;
                if depth > max_depth {
                    return false;
                }
            }
            b']' | b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
        index += 1;
    }
    true
}

fn leaf_label(leaf: &NodeKind) -> String {
    match leaf {
        NodeKind::Compare { path, op, literal } => {
            format!("{path} {op} {}", literal_label(literal))
        }
        NodeKind::Coerced {
            path,
            op,
            coerce,
            literal,
        } => {
            let coerce = match coerce {
                Coerce::Timestamp { format } => format!("timestamp({format})"),
                Coerce::Number => "number".to_owned(),
            };
            format!("{coerce}({path}) {op} {}", coerced_label(literal))
        }
        NodeKind::Present(path) => format!("{path} is present"),
        NodeKind::Absent(path) => format!("{path} is absent"),
        NodeKind::Header { key, op, literal } => {
            format!("header {key} {op} {}", literal_label(literal))
        }
        NodeKind::Text { path, matcher } => {
            format!("{path} {} {}", matcher.kind(), matcher.label())
        }
        NodeKind::HeaderText { key, matcher } => {
            format!("header {key} {} {}", matcher.kind(), matcher.label())
        }
        NodeKind::All(_) | NodeKind::Any(_) | NodeKind::Not(_) => unreachable!("not a leaf"),
    }
}

fn literal_label(literal: &Literal) -> String {
    match literal {
        Literal::Null => "null".to_owned(),
        Literal::Bool(value) => value.to_string(),
        Literal::Int(value) => value.to_string(),
        Literal::Float(value) => value.to_string(),
        Literal::Text(value) => format!("{value:?}"),
        Literal::List(items) => format!(
            "[{}]",
            items
                .iter()
                .map(literal_label)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn coerced_label(literal: &CoercedLiteral) -> String {
    match literal {
        CoercedLiteral::Instant(micros) => format!("{micros}us"),
        CoercedLiteral::Decimal(decimal) => format!("{decimal:?}"),
        CoercedLiteral::List(items) => format!(
            "[{}]",
            items
                .iter()
                .map(coerced_label)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::text::TextMatch;

    const SAFE_MODE: &str = r#"{"op":"u","table":"satellites","changed":["mode"],"after":{"id":"sat-042","name":"Kestrel-42","mode":"safe","orbit":"leo","battery_pct":61}}"#;
    const SAFE_MODE_VALUES: &str = r#"{"op":"u","table":"satellites","after":{"id":"sat-042","name":"Kestrel-42","mode":"safe","orbit":"leo","battery_pct":61}}"#;
    const DECOMMISSION: &str = r#"{"op":"d","table":"satellites","before":{"id":"sat-042"}}"#;
    const TELEMETRY: &str = r#"{"event":"satellite.telemetry_changed","satellite_id":"sat-042","fields":{"mode":null}}"#;
    const GROUND_STATION: &str = r#"{"op":"u","table":"ground_stations","changed":["status"],"after":{"id":"svalbard","status":"online"}}"#;

    fn verdict(filter: &ConsumerFilter, payload: &str) -> Verdict {
        CompiledFilter::compile(filter).expect("compiles").evaluate(
            &FilterRecord {
                payload: payload.as_bytes(),
                headers: &[],
            },
            &DecodeLimits::default(),
        )
    }

    fn strict() -> ConsumerFilter {
        ConsumerFilter::json(FilterExpr::any([
            FilterExpr::all([
                FilterExpr::pred("table", CmpOp::Eq, "satellites"),
                FilterExpr::pred("op", CmpOp::Eq, "u"),
                FilterExpr::pred("changed", CmpOp::Contains, "mode"),
                FilterExpr::pred("after.mode", CmpOp::Eq, "safe"),
            ]),
            FilterExpr::all([
                FilterExpr::pred("table", CmpOp::Eq, "satellites"),
                FilterExpr::pred("op", CmpOp::Eq, "d"),
            ]),
            FilterExpr::all([
                FilterExpr::pred("event", CmpOp::Eq, "satellite.telemetry_changed"),
                FilterExpr::present("fields.mode"),
            ]),
        ]))
    }

    fn legacy() -> ConsumerFilter {
        ConsumerFilter::json(FilterExpr::all([
            FilterExpr::pred("table", CmpOp::Eq, "satellites"),
            FilterExpr::pred("op", CmpOp::Eq, "u"),
            FilterExpr::pred("after.mode", CmpOp::Eq, "safe"),
        ]))
    }

    #[test]
    fn given_an_indexed_array_path_when_decoding_then_should_retain_only_the_requested_field_and_validate_the_rest()
     {
        let mut paths = PathTrie::default();
        paths.insert(&FieldPath::parse("items[1].id").expect("path"));
        let value = decode_json_paths(
            br#"{"items":[{"other":"unused"},{"id":"chosen","unused":[1,2,3]},{"id":"unread"}]}"#,
            &DecodeLimits::default(),
            &paths,
        )
        .expect("decoded");
        assert_eq!(value, serde_json::json!({"items":[null,{"id":"chosen"}]}));
        assert_eq!(
            decode_json_paths(
                br#"{"items":[{"bad":"\ud800"},{"id":"chosen"}]}"#,
                &DecodeLimits::default(),
                &paths
            ),
            Err(FaultReason::Malformed)
        );
    }

    #[test]
    fn given_the_safe_mode_fixtures_when_evaluated_then_should_match_the_scenario_table() {
        let expected = [
            (SAFE_MODE, Verdict::Selected, Verdict::Selected),
            (SAFE_MODE_VALUES, Verdict::Rejected, Verdict::Selected),
            (DECOMMISSION, Verdict::Selected, Verdict::Rejected),
            (TELEMETRY, Verdict::Selected, Verdict::Rejected),
            (GROUND_STATION, Verdict::Rejected, Verdict::Rejected),
        ];
        for (payload, strict_verdict, legacy_verdict) in expected {
            assert_eq!(
                verdict(&strict(), payload),
                strict_verdict,
                "strict on {payload}"
            );
            assert_eq!(
                verdict(&legacy(), payload),
                legacy_verdict,
                "legacy on {payload}"
            );
        }
    }

    #[test]
    fn given_missing_and_null_fields_when_compared_then_should_follow_the_null_rules() {
        let eq_null =
            ConsumerFilter::json(FilterExpr::pred("missing", CmpOp::Eq, TypedValue::Null));
        let ne_null =
            ConsumerFilter::json(FilterExpr::pred("missing", CmpOp::Ne, TypedValue::Null));
        let not_eq = ConsumerFilter::json(FilterExpr::negate(FilterExpr::pred(
            "missing",
            CmpOp::Eq,
            "x",
        )));
        let present_null = ConsumerFilter::json(FilterExpr::present("value"));
        let ne_null_on_null =
            ConsumerFilter::json(FilterExpr::pred("value", CmpOp::Ne, TypedValue::Null));
        assert_eq!(verdict(&eq_null, r#"{"value":null}"#), Verdict::Selected);
        assert_eq!(verdict(&ne_null, r#"{"value":null}"#), Verdict::Rejected);
        assert_eq!(
            verdict(&not_eq, r#"{}"#),
            Verdict::Rejected,
            "not(unknown) is unknown"
        );
        assert_eq!(
            verdict(&present_null, r#"{"value":null}"#),
            Verdict::Selected
        );
        assert_eq!(
            verdict(&ne_null_on_null, r#"{"value":null}"#),
            Verdict::Rejected
        );
    }

    #[test]
    fn given_numbers_when_compared_then_should_stay_exact() {
        let big =
            ConsumerFilter::json(FilterExpr::pred("id", CmpOp::Eq, 9_007_199_254_740_993_i64));
        assert_eq!(
            verdict(&big, r#"{"id":9007199254740993}"#),
            Verdict::Selected
        );
        assert_eq!(
            verdict(&big, r#"{"id":9007199254740992}"#),
            Verdict::Rejected
        );
        let mixed = ConsumerFilter::json(FilterExpr::pred("port", CmpOp::Gte, 5061.0));
        assert_eq!(verdict(&mixed, r#"{"port":5061}"#), Verdict::Selected);
        let inexact = ConsumerFilter::json(FilterExpr::pred("id", CmpOp::Eq, 1.0));
        assert_eq!(
            verdict(&inexact, r#"{"id":9007199254740993}"#),
            Verdict::Rejected
        );
        let text_vs_number = ConsumerFilter::json(FilterExpr::pred("port", CmpOp::Eq, "5061"));
        assert_eq!(
            verdict(&text_vs_number, r#"{"port":5061}"#),
            Verdict::Rejected
        );
    }

    #[test]
    fn given_contains_and_prefix_when_compared_then_should_be_literal() {
        let contains = ConsumerFilter::json(FilterExpr::pred("name", CmpOp::Contains, "%"));
        assert_eq!(verdict(&contains, r#"{"name":"100%"}"#), Verdict::Selected);
        assert_eq!(verdict(&contains, r#"{"name":"100"}"#), Verdict::Rejected);
        let array = ConsumerFilter::json(FilterExpr::pred("tags", CmpOp::Contains, 2));
        assert_eq!(verdict(&array, r#"{"tags":[1,2,3]}"#), Verdict::Selected);
        assert_eq!(verdict(&array, r#"{"tags":["2"]}"#), Verdict::Rejected);
        let prefix = ConsumerFilter::json(FilterExpr::pred("name", CmpOp::Prefix, "eci_"));
        assert_eq!(verdict(&prefix, r#"{"name":"eci_x"}"#), Verdict::Selected);
        assert_eq!(verdict(&prefix, r#"{"name":"ecix"}"#), Verdict::Rejected);
    }

    #[test]
    fn given_in_lists_when_compared_then_should_match_any_item() {
        let filter = ConsumerFilter::json(FilterExpr::pred(
            "table",
            CmpOp::In,
            TypedValue::List(vec!["satellites".into(), "ground_stations".into()]),
        ));
        assert_eq!(
            verdict(&filter, r#"{"table":"ground_stations"}"#),
            Verdict::Selected
        );
        assert_eq!(
            verdict(&filter, r#"{"table":"launches"}"#),
            Verdict::Rejected
        );
    }

    #[test]
    fn given_coercions_when_compared_then_should_use_instants_and_exact_decimals() {
        let after = ConsumerFilter::json(FilterExpr::pred_as(
            "at",
            CmpOp::Gt,
            "2026-09-21T18:00:00Z",
            Coerce::Timestamp {
                format: TimestampFormat::Rfc3339,
            },
        ));
        assert_eq!(
            verdict(&after, r#"{"at":"2026-09-21T20:04:12+02:00"}"#),
            Verdict::Selected
        );
        assert_eq!(
            verdict(&after, r#"{"at":"2026-09-21T19:59:59+02:00"}"#),
            Verdict::Rejected
        );
        assert_eq!(
            verdict(&after, r#"{"at":"2026-09-21T18:04:12"}"#),
            Verdict::Rejected,
            "no zone is unknown"
        );
        let millis = ConsumerFilter::json(FilterExpr::pred_as(
            "timestamp_ms",
            CmpOp::Lt,
            "2026-09-21T16:04:13Z",
            Coerce::Timestamp {
                format: TimestampFormat::EpochMillis,
            },
        ));
        assert!(
            CompiledFilter::compile(&millis).is_err(),
            "an epoch literal must be an integer"
        );
        let cpu = ConsumerFilter::json(FilterExpr::pred_as(
            "cpu",
            CmpOp::Gt,
            "9007199254740992",
            Coerce::Number,
        ));
        assert_eq!(
            verdict(&cpu, r#"{"cpu":"9007199254740993"}"#),
            Verdict::Selected
        );
        assert_eq!(
            verdict(&cpu, r#"{"cpu":"9007199254740992.0"}"#),
            Verdict::Rejected
        );
        assert_eq!(verdict(&cpu, r#"{"cpu":"abc"}"#), Verdict::Rejected);
    }

    #[test]
    fn given_header_predicates_when_they_decide_then_should_skip_the_payload() {
        let filter = ConsumerFilter::json(FilterExpr::all([
            FilterExpr::pred("op", CmpOp::Eq, "d"),
            FilterExpr::header("table", CmpOp::Eq, "satellites"),
        ]));
        let compiled = CompiledFilter::compile(&filter).expect("compiles");
        let ground_station = [HeaderRef {
            key: "table",
            value: HeaderValueRef::String("ground_stations"),
        }];
        let rejected = compiled.evaluate(
            &FilterRecord {
                payload: b"not json at all",
                headers: &ground_station,
            },
            &DecodeLimits::default(),
        );
        assert_eq!(
            rejected,
            Verdict::Rejected,
            "the header decides before any decode"
        );
        let satellite = [HeaderRef {
            key: "table",
            value: HeaderValueRef::String("satellites"),
        }];
        let fault = compiled.evaluate(
            &FilterRecord {
                payload: b"not json at all",
                headers: &satellite,
            },
            &DecodeLimits::default(),
        );
        assert_eq!(
            fault,
            Verdict::Fault,
            "the payload is needed and does not decode"
        );
    }

    #[test]
    fn given_payload_limits_when_exceeded_then_should_fault_without_parsing() {
        let filter = ConsumerFilter::json(FilterExpr::present("a"));
        let compiled = CompiledFilter::compile(&filter).expect("compiles");
        let limits = DecodeLimits {
            max_payload_bytes: 64,
            max_depth: 3,
        };
        let deep = r#"{"a":{"b":{"c":{"d":1}}}}"#;
        let record = FilterRecord {
            payload: deep.as_bytes(),
            headers: &[],
        };
        assert_eq!(compiled.evaluate(&record, &limits), Verdict::Fault);
        assert_eq!(
            compiled.evaluate_with_fault(&record, &limits).1,
            Some(FaultReason::TooDeep)
        );
        let braces_in_strings = r#"{"a":"{{{{{{{{"}"#;
        let record = FilterRecord {
            payload: braces_in_strings.as_bytes(),
            headers: &[],
        };
        assert_eq!(compiled.evaluate(&record, &limits), Verdict::Selected);
        let large = format!(r#"{{"a":"{}"}}"#, "x".repeat(100));
        let record = FilterRecord {
            payload: large.as_bytes(),
            headers: &[],
        };
        assert_eq!(
            compiled.evaluate_with_fault(&record, &limits).1,
            Some(FaultReason::TooLarge)
        );
    }

    #[test]
    fn given_a_record_when_explained_then_should_report_every_node() {
        let compiled = CompiledFilter::compile(&strict()).expect("compiles");
        let explanation = compiled.explain(
            &FilterRecord {
                payload: SAFE_MODE_VALUES.as_bytes(),
                headers: &[],
            },
            &DecodeLimits::default(),
        );
        assert_eq!(explanation.verdict, Verdict::Rejected);
        assert_eq!(explanation.root.label, "any");
        assert_eq!(explanation.root.children.len(), 3);
        let snapshot_branch = &explanation.root.children[0];
        assert_eq!(snapshot_branch.truth, Some(Truth::Unknown));
        assert_eq!(
            snapshot_branch.children[2].label,
            r#"changed contains "mode""#
        );
        assert_eq!(snapshot_branch.children[2].truth, Some(Truth::Unknown));
        let malformed = compiled.explain(
            &FilterRecord {
                payload: b"{",
                headers: &[],
            },
            &DecodeLimits::default(),
        );
        assert_eq!(malformed.verdict, Verdict::Fault);
        assert_eq!(malformed.fault, Some(FaultReason::Malformed));
    }

    #[test]
    fn given_duplicate_keys_when_decoded_then_should_keep_the_last_value() {
        let filter = ConsumerFilter::json(FilterExpr::pred("kind", CmpOp::Eq, "B"));
        assert_eq!(
            verdict(&filter, r#"{"kind":"A","kind":"B"}"#),
            Verdict::Selected
        );
    }

    fn decided(
        filter: &ConsumerFilter,
        payload: &[u8],
        headers: &[HeaderRef<'_>],
    ) -> (Verdict, Option<FaultReason>) {
        CompiledFilter::compile(filter)
            .expect("compiles")
            .evaluate_with_fault(&FilterRecord { payload, headers }, &DecodeLimits::default())
    }

    fn content_type(code: u64) -> HeaderRef<'static> {
        HeaderRef {
            key: CONTENT_TYPE,
            value: HeaderValueRef::Uint(code),
        }
    }

    #[test]
    fn given_a_record_declared_in_another_codec_when_a_json_filter_reads_it_then_should_reject_without_decoding()
     {
        let filter = ConsumerFilter::json(FilterExpr::pred("type", CmpOp::Eq, "metrics"));
        let protobuf = content_type(u64::from(ContentType::Protobuf.code()));
        assert_eq!(
            decided(&filter, b"\x08\x01", &[protobuf]),
            (Verdict::Rejected, Some(FaultReason::ForeignCodec))
        );
        let passing = filter.clone().with_foreign_policy(RecordPolicy::Pass);
        assert_eq!(
            decided(&passing, b"\x08\x01", &[protobuf]),
            (Verdict::Fault, Some(FaultReason::ForeignCodec))
        );
        let compiled = CompiledFilter::compile(&passing).expect("compiles");
        assert_eq!(
            compiled.policy_for(FaultReason::ForeignCodec),
            FaultPolicy::Pass
        );
        let json = content_type(u64::from(ContentType::Json.code()));
        assert_eq!(
            decided(&filter, br#"{"type":"metrics"}"#, &[json]),
            (Verdict::Selected, None)
        );
        assert_eq!(
            decided(&filter, br#"{"type":"metrics"}"#, &[content_type(255)]),
            (Verdict::Selected, None)
        );
        assert_eq!(
            decided(&filter, br#"{"type":"metrics"}"#, &[content_type(200)]),
            (Verdict::Selected, None)
        );
        assert_eq!(
            decided(&filter, b"not json", &[]),
            (Verdict::Fault, Some(FaultReason::Malformed))
        );
    }

    #[test]
    fn given_a_value_of_another_type_when_compared_then_should_follow_the_mismatch_policy() {
        let filter = ConsumerFilter::json(FilterExpr::pred("xyz", CmpOp::Eq, "Abc"));
        for payload in [
            r#"{"xyz":1}"#,
            r#"{"xyz":{"a":1}}"#,
            r#"{"xyz":[1]}"#,
            r#"{"xyz":true}"#,
        ] {
            assert_eq!(
                decided(&filter, payload.as_bytes(), &[]),
                (Verdict::Rejected, Some(FaultReason::TypeMismatch)),
                "{payload}"
            );
        }
        let passing = filter.with_mismatch_policy(RecordPolicy::Pass);
        assert_eq!(
            decided(&passing, br#"{"xyz":1}"#, &[]),
            (Verdict::Fault, Some(FaultReason::TypeMismatch))
        );
        assert_eq!(
            decided(&passing, br#"{"xyz":"Abc"}"#, &[]),
            (Verdict::Selected, None)
        );
        assert_eq!(
            decided(&passing, br#"{"xyz":"Abd"}"#, &[]),
            (Verdict::Rejected, None)
        );
        assert_eq!(
            decided(&passing, br#"{"other":1}"#, &[]),
            (Verdict::Rejected, None)
        );
        assert_eq!(
            decided(&passing, br#"{"xyz":null}"#, &[]),
            (Verdict::Rejected, None)
        );
    }

    #[test]
    fn given_text_predicates_on_a_mixed_log_when_evaluated_then_should_select_the_subdomain() {
        let filter = ConsumerFilter::json(FilterExpr::any([
            FilterExpr::text("type", TextMatch::Contains, ".v1."),
            FilterExpr::text("type", TextMatch::Glob, "ALERTS.*").case_insensitive(),
        ]));
        assert_eq!(
            verdict(&filter, r#"{"type":"metrics.cpu.v1.reported"}"#),
            Verdict::Selected
        );
        assert_eq!(
            verdict(&filter, r#"{"type":"alerts.disk.v2.raised"}"#),
            Verdict::Selected
        );
        assert_eq!(
            verdict(&filter, r#"{"type":"metrics.cpu.v2.reported"}"#),
            Verdict::Rejected
        );
        let headers = ConsumerFilter::headers_only(FilterExpr::header_text(
            "event.type",
            TextMatch::Regex,
            r"^metrics\.[a-z]+\.v1\.",
        ));
        let typed = [HeaderRef {
            key: "event.type",
            value: HeaderValueRef::String("metrics.cpu.v1.reported"),
        }];
        assert_eq!(
            decided(&headers, b"\xff\xfe anything", &typed),
            (Verdict::Selected, None)
        );
        let numeric = [HeaderRef {
            key: "event.type",
            value: HeaderValueRef::Uint(7),
        }];
        assert_eq!(
            decided(&headers, b"", &numeric),
            (Verdict::Rejected, Some(FaultReason::TypeMismatch))
        );
        let passing = headers.with_mismatch_policy(RecordPolicy::Pass);
        assert_eq!(
            decided(&passing, b"", &numeric),
            (Verdict::Fault, Some(FaultReason::TypeMismatch))
        );
    }

    #[test]
    fn given_escaped_quotes_and_deep_nesting_when_depth_is_checked_then_should_count_only_structure()
     {
        assert!(json_depth_within(br#"{"a":"[[[[\"{{{{"}"#, 1));
        assert!(!json_depth_within(br#"{"a":[[1]]}"#, 2));
        assert!(json_depth_within(br#"{"a":"\\"}"#, 1));
        assert!(json_depth_within(br#"{"a":"unterminated"#, 1));
    }
}

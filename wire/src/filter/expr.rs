use crate::codes::FILTER_OP_VERSION;
use crate::error::InvalidError;
use crate::filter::coerce::{ExactDecimal, TimestampFormat};
use crate::filter::path::FieldPath;
use crate::filter::text::{MAX_REGEX_PREDICATES, TextMatch, TextPredicate};
use crate::limits::{
    MAX_FILTER_BYTES, MAX_FILTER_DEPTH, MAX_FILTER_LIST_ITEMS, MAX_FILTER_NODES,
    MAX_FILTER_PATH_BYTES, MAX_FILTER_STRING_BYTES,
};
use crate::query::{CmpOp, Predicate};
use crate::schema::{Digest32, TypedValue};
use crate::validate::Validate;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// Version of the evaluation rules a filter was written against. Part of the
/// digest, so a rule change can never silently reinterpret a saved revision.
pub const FILTER_EVALUATOR_VERSION: u32 = 1;

const DIGEST_DOMAIN: &[u8] = b"agdx.consumer-filter.v1\0";

/// A declarative predicate over one record: payload fields, typed user
/// headers, presence tests, and boolean composition. It never runs user code.
///
/// Comparison truth is three-valued. A missing field or a type mismatch is
/// unknown, `not` keeps unknown, and an unknown root selects nothing. A decode
/// fault is separate from truth and follows the filter's [`FaultPolicy`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterExpr {
    All(Vec<FilterExpr>),
    Any(Vec<FilterExpr>),
    Not(Box<FilterExpr>),
    /// A payload comparison. The field is a [`FieldPath`] text form.
    Pred(Predicate),
    /// A payload comparison after an explicit value coercion.
    PredAs(CoercedPredicate),
    /// The payload path exists. An explicit `null` value is present.
    Present(FieldPath),
    /// The payload path does not exist.
    Absent(FieldPath),
    /// A typed user-header comparison. The key is matched exactly.
    Header(HeaderPredicate),
    /// A text match on a payload field: equals, prefix, suffix, contains,
    /// glob, or regex, optionally case-insensitive.
    Text(TextPredicate),
    /// A text match on one user header, keyed exactly.
    HeaderText(TextPredicate),
}

/// A payload comparison whose field and literal are both coerced before the
/// compare, so `"2026-09-21T18:04:12Z"` orders as an instant and `"12.50"` as a
/// number. The coercion is part of the expression and its digest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CoercedPredicate {
    pub pred: Predicate,
    pub coerce: Coerce,
}

/// An explicit value coercion. A value that does not parse under it compares
/// as unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Coerce {
    /// Compare as an instant in microseconds.
    Timestamp { format: TimestampFormat },
    /// Compare as an exact decimal. Accepts numbers and decimal strings.
    Number,
}

/// A comparison against one user header, keyed exactly (`agdx.ct` is one key).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HeaderPredicate {
    pub key: String,
    pub op: CmpOp,
    pub value: TypedValue,
}

/// How the record payload is decoded before payload predicates run. Chosen
/// explicitly by the filter: a content-type header never overrides it. A
/// record whose `agdx.ct` header names another codec is never decoded, it
/// follows the filter's `foreign_policy` instead.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum FilterCodec {
    /// Decode the payload as JSON.
    Json,
    /// Decode one CBOR data item.
    Cbor,
    /// Decode a raw Avro datum under its registered writer schema.
    Avro,
    /// Decode a Protobuf message under its registered descriptor.
    Protobuf,
    /// Never decode the payload. Only header predicates are allowed, so the
    /// payload may be in any format.
    HeadersOnly,
    /// A codec this build does not know, for example one a newer catalog
    /// saved. It decodes so that listings stay readable, but it never
    /// validates or compiles.
    #[serde(other)]
    Unknown,
}

/// What happens to a record the filter cannot judge on its own terms.
///
/// `foreign_policy` covers a record in another format: its `agdx.ct` header
/// names another codec, or its writer schema is missing, not listed by the
/// filter, or of another schema family. `mismatch_policy` covers a record
/// where a predicate's value exists but has a type the predicate cannot
/// compare, so the filter could not decide. A mixed log keeps both at
/// `reject`. A consumer that wants to see edge cases sets `pass`.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum RecordPolicy {
    /// Skip the record. It is not selected and never a fault.
    #[default]
    Reject,
    /// Deliver the record marked unevaluated, so the consumer decides.
    Pass,
}

impl RecordPolicy {
    pub const fn is_reject(&self) -> bool {
        matches!(self, RecordPolicy::Reject)
    }
}

/// What happens to a record whose payload cannot be decoded (malformed, over
/// the size or depth limit). A valid record missing a field is not a fault.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum FaultPolicy {
    /// Stop the page before the record. It stays unacknowledged until the
    /// reader changes policy or the record ages out.
    #[default]
    Stop,
    /// Deliver the record marked unevaluated.
    Pass,
    /// Skip the record, counted as a fault.
    Drop,
}

/// One consumer filter: the expression plus everything that changes what it
/// selects. Its [`digest`](Self::digest) identifies these semantics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ConsumerFilter {
    pub v: u32,
    pub evaluator_version: u32,
    pub expr: FilterExpr,
    pub codec: FilterCodec,
    #[serde(default)]
    pub fault_policy: FaultPolicy,
    #[serde(default, skip_serializing_if = "RecordPolicy::is_reject")]
    pub foreign_policy: RecordPolicy,
    #[serde(default, skip_serializing_if = "RecordPolicy::is_reject")]
    pub mismatch_policy: RecordPolicy,
    /// Immutable writer schema IDs permitted for Avro or Protobuf payloads.
    /// A record selects one of these with its agdx.sid header.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema_refs: Vec<u32>,
}

impl FilterExpr {
    pub fn all(children: impl IntoIterator<Item = FilterExpr>) -> Self {
        Self::All(children.into_iter().collect())
    }

    pub fn any(children: impl IntoIterator<Item = FilterExpr>) -> Self {
        Self::Any(children.into_iter().collect())
    }

    pub fn negate(child: FilterExpr) -> Self {
        Self::Not(Box::new(child))
    }

    pub fn pred(field: impl Into<String>, op: CmpOp, value: impl Into<TypedValue>) -> Self {
        Self::Pred(Predicate {
            field: field.into(),
            op,
            value: value.into(),
        })
    }

    pub fn pred_as(
        field: impl Into<String>,
        op: CmpOp,
        value: impl Into<TypedValue>,
        coerce: Coerce,
    ) -> Self {
        Self::PredAs(CoercedPredicate {
            pred: Predicate {
                field: field.into(),
                op,
                value: value.into(),
            },
            coerce,
        })
    }

    /// Presence of a payload path. Returns the parse error of an invalid
    /// path, so it suits untrusted input where [`Self::present`] would panic.
    pub fn try_present(path: &str) -> Result<Self, InvalidError> {
        FieldPath::parse(path).map(Self::Present)
    }

    /// Presence of a trusted path. Panics if it is invalid.
    pub fn present(path: &str) -> Self {
        Self::Present(FieldPath::parse(path).expect("a valid field path"))
    }

    /// Absence of a payload path. Returns the parse error of an invalid
    /// path, so it suits untrusted input where [`Self::absent`] would panic.
    pub fn try_absent(path: &str) -> Result<Self, InvalidError> {
        FieldPath::parse(path).map(Self::Absent)
    }

    /// Absence of a trusted path. Panics if it is invalid.
    pub fn absent(path: &str) -> Self {
        Self::Absent(FieldPath::parse(path).expect("a valid field path"))
    }

    pub fn header(key: impl Into<String>, op: CmpOp, value: impl Into<TypedValue>) -> Self {
        Self::Header(HeaderPredicate {
            key: key.into(),
            op,
            value: value.into(),
        })
    }

    pub fn text(field: impl Into<String>, kind: TextMatch, pattern: impl Into<String>) -> Self {
        Self::Text(TextPredicate {
            field: field.into(),
            kind,
            pattern: pattern.into(),
            case_insensitive: false,
        })
    }

    pub fn header_text(
        key: impl Into<String>,
        kind: TextMatch,
        pattern: impl Into<String>,
    ) -> Self {
        Self::HeaderText(TextPredicate {
            field: key.into(),
            kind,
            pattern: pattern.into(),
            case_insensitive: false,
        })
    }

    /// Make a text match case-insensitive. Any other node is returned as is.
    #[must_use]
    pub fn case_insensitive(mut self) -> Self {
        if let Self::Text(predicate) | Self::HeaderText(predicate) = &mut self {
            predicate.case_insensitive = true;
        }
        self
    }

    /// Whether evaluating this node can require the decoded payload.
    pub fn reads_payload(&self) -> bool {
        match self {
            Self::All(children) | Self::Any(children) => children.iter().any(Self::reads_payload),
            Self::Not(child) => child.reads_payload(),
            Self::Pred(_)
            | Self::PredAs(_)
            | Self::Present(_)
            | Self::Absent(_)
            | Self::Text(_) => true,
            Self::Header(_) | Self::HeaderText(_) => false,
        }
    }

    /// Whether evaluating this node can require the record headers.
    pub fn reads_headers(&self) -> bool {
        match self {
            Self::All(children) | Self::Any(children) => children.iter().any(Self::reads_headers),
            Self::Not(child) => child.reads_headers(),
            Self::Header(_) | Self::HeaderText(_) => true,
            Self::Pred(_)
            | Self::PredAs(_)
            | Self::Present(_)
            | Self::Absent(_)
            | Self::Text(_) => false,
        }
    }
}

impl ConsumerFilter {
    /// A JSON-decoding filter with the default `stop` fault policy.
    pub fn json(expr: FilterExpr) -> Self {
        Self::new(expr, FilterCodec::Json)
    }

    /// Decode a self-describing CBOR payload.
    pub fn cbor(expr: FilterExpr) -> Self {
        Self::new(expr, FilterCodec::Cbor)
    }

    /// Decode a raw Avro datum with one of the listed writer schemas.
    pub fn avro(expr: FilterExpr, schema_refs: impl IntoIterator<Item = u32>) -> Self {
        Self::schema_codec(expr, FilterCodec::Avro, schema_refs)
    }

    /// Decode a Protobuf message with one of the listed writer schemas.
    pub fn protobuf(expr: FilterExpr, schema_refs: impl IntoIterator<Item = u32>) -> Self {
        Self::schema_codec(expr, FilterCodec::Protobuf, schema_refs)
    }

    fn schema_codec(
        expr: FilterExpr,
        codec: FilterCodec,
        schema_refs: impl IntoIterator<Item = u32>,
    ) -> Self {
        let mut filter = Self::new(expr, codec);
        filter.schema_refs = schema_refs.into_iter().collect();
        filter.schema_refs.sort_unstable();
        filter.schema_refs.dedup();
        filter
    }

    /// A header-only filter. The payload is never decoded.
    pub fn headers_only(expr: FilterExpr) -> Self {
        Self::new(expr, FilterCodec::HeadersOnly)
    }

    fn new(expr: FilterExpr, codec: FilterCodec) -> Self {
        Self {
            v: FILTER_OP_VERSION,
            evaluator_version: FILTER_EVALUATOR_VERSION,
            expr,
            codec,
            fault_policy: FaultPolicy::Stop,
            foreign_policy: RecordPolicy::Reject,
            mismatch_policy: RecordPolicy::Reject,
            schema_refs: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_fault_policy(mut self, fault_policy: FaultPolicy) -> Self {
        self.fault_policy = fault_policy;
        self
    }

    #[must_use]
    pub fn with_foreign_policy(mut self, foreign_policy: RecordPolicy) -> Self {
        self.foreign_policy = foreign_policy;
        self
    }

    #[must_use]
    pub fn with_mismatch_policy(mut self, mismatch_policy: RecordPolicy) -> Self {
        self.mismatch_policy = mismatch_policy;
        self
    }

    /// SHA-256 over a domain tag and the JSON encoding of the filter. Every
    /// semantic field is covered. Two filters get the same digest exactly when
    /// their encodings are equal: logically equivalent but differently written
    /// expressions keep distinct digests.
    pub fn digest(&self) -> Digest32 {
        let encoded = serde_json::to_vec(self).expect("a consumer filter always encodes as JSON");
        let mut hasher = Sha256::new();
        hasher.update(DIGEST_DOMAIN);
        hasher.update(&encoded);
        Digest32::new(hasher.finalize().into())
    }
}

impl Validate for ConsumerFilter {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.v != FILTER_OP_VERSION {
            return Err(InvalidError::new(format!(
                "filter version {} is not supported, expected {FILTER_OP_VERSION}",
                self.v
            )));
        }
        if self.evaluator_version != FILTER_EVALUATOR_VERSION {
            return Err(InvalidError::new(format!(
                "evaluator version {} is not supported, expected {FILTER_EVALUATOR_VERSION}",
                self.evaluator_version
            )));
        }
        let schema_codec = matches!(self.codec, FilterCodec::Avro | FilterCodec::Protobuf);
        if schema_codec == self.schema_refs.is_empty() {
            return Err(InvalidError::new(
                "Avro and Protobuf require schema_refs, other codecs require no schema_refs",
            ));
        }
        if self.schema_refs.windows(2).any(|ids| ids[0] >= ids[1]) {
            return Err(InvalidError::new(
                "schema_refs must be sorted and contain no duplicates",
            ));
        }
        let encoded_bytes = serde_json::to_vec(self)
            .map_err(|error| InvalidError::new(format!("filter does not encode: {error}")))?
            .len();
        if encoded_bytes > MAX_FILTER_BYTES {
            return Err(InvalidError::new(format!(
                "filter is {encoded_bytes}B, exceeds cap {MAX_FILTER_BYTES}B"
            )));
        }
        if self.codec == FilterCodec::Unknown {
            return Err(InvalidError::new(
                "the filter codec is not supported by this build",
            ));
        }
        if self.codec == FilterCodec::HeadersOnly && self.expr.reads_payload() {
            return Err(InvalidError::new(
                "a headers_only filter cannot use payload predicates",
            ));
        }
        let mut nodes = 0;
        validate_node(&self.expr, 1, &mut nodes)?;
        if regex_predicates(&self.expr) > MAX_REGEX_PREDICATES {
            return Err(InvalidError::new(format!(
                "a filter holds at most {MAX_REGEX_PREDICATES} compiled text predicates (glob or regex)"
            )));
        }
        Ok(())
    }
}

// Every regex compiles to a bounded program, and this bounds their number, so
// one filter and the per-shard cache of compiled filters stay bounded too.
fn regex_predicates(expr: &FilterExpr) -> usize {
    match expr {
        FilterExpr::All(children) | FilterExpr::Any(children) => {
            children.iter().map(regex_predicates).sum()
        }
        FilterExpr::Not(child) => regex_predicates(child),
        FilterExpr::Text(predicate) | FilterExpr::HeaderText(predicate) => {
            usize::from(matches!(predicate.kind, TextMatch::Regex | TextMatch::Glob))
        }
        FilterExpr::Pred(_)
        | FilterExpr::PredAs(_)
        | FilterExpr::Present(_)
        | FilterExpr::Absent(_)
        | FilterExpr::Header(_) => 0,
    }
}

fn validate_node(expr: &FilterExpr, depth: usize, nodes: &mut usize) -> Result<(), InvalidError> {
    if depth > MAX_FILTER_DEPTH {
        return Err(InvalidError::new(format!(
            "filter nesting exceeds depth {MAX_FILTER_DEPTH}"
        )));
    }
    count_nodes(nodes, 1)?;
    match expr {
        FilterExpr::All(children) | FilterExpr::Any(children) => {
            if children.is_empty() {
                return Err(InvalidError::new("`all` and `any` need at least one child"));
            }
            children
                .iter()
                .try_for_each(|child| validate_node(child, depth + 1, nodes))
        }
        FilterExpr::Not(child) => validate_node(child, depth + 1, nodes),
        FilterExpr::Pred(predicate) => {
            FieldPath::parse(&predicate.field)?;
            validate_literal(predicate.op, &predicate.value, LiteralSite::Payload, nodes)
        }
        FilterExpr::PredAs(coerced) => {
            FieldPath::parse(&coerced.pred.field)?;
            validate_coerced(coerced, nodes)
        }
        FilterExpr::Present(_) | FilterExpr::Absent(_) => Ok(()),
        FilterExpr::Header(header) => {
            validate_header_key(&header.key)?;
            validate_literal(header.op, &header.value, LiteralSite::Header, nodes)
        }
        FilterExpr::Text(predicate) => {
            FieldPath::parse(&predicate.field)?;
            predicate.validate()
        }
        FilterExpr::HeaderText(predicate) => {
            validate_header_key(&predicate.field)?;
            predicate.validate()
        }
    }
}

fn validate_header_key(key: &str) -> Result<(), InvalidError> {
    if key.is_empty() || key.len() > MAX_FILTER_PATH_BYTES {
        return Err(InvalidError::new(format!(
            "header key must be 1..={MAX_FILTER_PATH_BYTES} bytes"
        )));
    }
    Ok(())
}

fn count_nodes(nodes: &mut usize, added: usize) -> Result<(), InvalidError> {
    *nodes += added;
    if *nodes > MAX_FILTER_NODES {
        return Err(InvalidError::new(format!(
            "filter has more than {MAX_FILTER_NODES} nodes"
        )));
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LiteralSite {
    Payload,
    Header,
}

fn validate_literal(
    op: CmpOp,
    value: &TypedValue,
    site: LiteralSite,
    nodes: &mut usize,
) -> Result<(), InvalidError> {
    match (op, value) {
        (CmpOp::Eq | CmpOp::Ne, TypedValue::Null) if site == LiteralSite::Payload => Ok(()),
        (_, TypedValue::Null) => Err(InvalidError::new(match site {
            LiteralSite::Payload => "a null literal only supports eq and ne",
            LiteralSite::Header => "a header predicate cannot compare against null",
        })),
        (CmpOp::In, TypedValue::List(items)) => {
            if items.is_empty() || items.len() > MAX_FILTER_LIST_ITEMS {
                return Err(InvalidError::new(format!(
                    "an `in` list needs 1..={MAX_FILTER_LIST_ITEMS} items"
                )));
            }
            count_nodes(nodes, items.len())?;
            items.iter().try_for_each(|item| {
                if matches!(item, TypedValue::Null | TypedValue::List(_)) {
                    return Err(InvalidError::new(
                        "`in` list items must be non-null scalars",
                    ));
                }
                validate_scalar(item)
            })
        }
        (CmpOp::In, _) => Err(InvalidError::new("`in` expects a list literal")),
        (_, TypedValue::List(_)) => Err(InvalidError::new("a list literal only supports `in`")),
        (CmpOp::Lt | CmpOp::Lte | CmpOp::Gt | CmpOp::Gte, TypedValue::Boolean(_)) => Err(
            InvalidError::new("ordered comparisons need a number or string literal"),
        ),
        (CmpOp::Prefix, value) if !matches!(value, TypedValue::String(_)) => {
            Err(InvalidError::new("`prefix` expects a string literal"))
        }
        (_, value) => validate_scalar(value),
    }
}

fn validate_scalar(value: &TypedValue) -> Result<(), InvalidError> {
    match value {
        TypedValue::Boolean(_) | TypedValue::Int(_) | TypedValue::Long(_) => Ok(()),
        TypedValue::Double(number) if number.is_finite() => Ok(()),
        TypedValue::Double(_) => Err(InvalidError::new("a double literal must be finite")),
        TypedValue::String(text) if text.len() <= MAX_FILTER_STRING_BYTES => Ok(()),
        TypedValue::String(_) => Err(InvalidError::new(format!(
            "a string literal exceeds {MAX_FILTER_STRING_BYTES}B"
        ))),
        _ => Err(InvalidError::new(
            "supported literals are null, boolean, int, long, double, string, and list",
        )),
    }
}

fn validate_coerced(coerced: &CoercedPredicate, nodes: &mut usize) -> Result<(), InvalidError> {
    let op = coerced.pred.op;
    if matches!(op, CmpOp::Contains | CmpOp::Prefix) {
        return Err(InvalidError::new(
            "a coerced predicate supports eq, ne, lt, lte, gt, gte, and in",
        ));
    }
    let items: Vec<&TypedValue> = match (&coerced.pred.value, op) {
        (TypedValue::List(items), CmpOp::In) => {
            if items.is_empty() || items.len() > MAX_FILTER_LIST_ITEMS {
                return Err(InvalidError::new(format!(
                    "an `in` list needs 1..={MAX_FILTER_LIST_ITEMS} items"
                )));
            }
            count_nodes(nodes, items.len())?;
            items.iter().collect()
        }
        (_, CmpOp::In) => return Err(InvalidError::new("`in` expects a list literal")),
        (TypedValue::List(_), _) => {
            return Err(InvalidError::new("a list literal only supports `in`"));
        }
        (value, _) => vec![value],
    };
    items.into_iter().try_for_each(|item| {
        if coerced_literal_parses(coerced.coerce, item) {
            Ok(())
        } else {
            Err(InvalidError::new(format!(
                "literal {} does not parse under the {} coercion",
                item.diagnostic_text(),
                coerce_label(coerced.coerce)
            )))
        }
    })
}

pub(crate) fn coerced_literal_parses(coerce: Coerce, value: &TypedValue) -> bool {
    match coerce {
        Coerce::Timestamp { format } => match value {
            TypedValue::String(text) => format.micros_from_text(text).is_some(),
            TypedValue::Int(number) => format.micros_from_integer(i128::from(*number)).is_some(),
            TypedValue::Long(number) => format.micros_from_integer(i128::from(*number)).is_some(),
            _ => false,
        },
        Coerce::Number => match value {
            TypedValue::String(text) => ExactDecimal::parse(text).is_some(),
            TypedValue::Int(_) | TypedValue::Long(_) => true,
            _ => false,
        },
    }
}

fn coerce_label(coerce: Coerce) -> String {
    match coerce {
        Coerce::Timestamp { format } => format!("timestamp ({format})"),
        Coerce::Number => "number".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn safe_mode_filter() -> ConsumerFilter {
        ConsumerFilter::json(FilterExpr::all([
            FilterExpr::pred("table", CmpOp::Eq, "satellites"),
            FilterExpr::pred("changed", CmpOp::Contains, "mode"),
            FilterExpr::pred("after.mode", CmpOp::Eq, "safe"),
        ]))
    }

    #[test]
    fn given_a_well_formed_filter_when_validated_then_should_pass() {
        safe_mode_filter().validate().expect("the filter is valid");
    }

    #[test]
    fn given_compiled_text_predicates_when_the_filter_budget_is_exceeded_then_should_reject() {
        for kind in [TextMatch::Glob, TextMatch::Regex] {
            let children = (0..4).map(|_| FilterExpr::text("name", kind, "safe"));
            ConsumerFilter::json(FilterExpr::all(children))
                .validate()
                .expect("four programs fit");
            let children = (0..5).map(|_| FilterExpr::text("name", kind, "safe"));
            assert!(
                ConsumerFilter::json(FilterExpr::all(children))
                    .validate()
                    .is_err()
            );
        }
    }

    #[test]
    fn given_equal_filters_when_digested_then_should_match_and_differ_on_any_change() {
        let digest = safe_mode_filter().digest();
        assert_eq!(digest, safe_mode_filter().digest());
        assert_eq!(digest.as_bytes().len(), Digest32::BYTES);
        let dropped = safe_mode_filter().with_fault_policy(FaultPolicy::Drop);
        assert_ne!(digest, dropped.digest(), "the fault policy is semantic");
        let reordered = ConsumerFilter::json(FilterExpr::all([
            FilterExpr::pred("changed", CmpOp::Contains, "mode"),
            FilterExpr::pred("table", CmpOp::Eq, "satellites"),
            FilterExpr::pred("after.mode", CmpOp::Eq, "safe"),
        ]));
        assert_ne!(digest, reordered.digest(), "expression order is preserved");
    }

    #[test]
    fn given_payload_nodes_in_a_headers_only_filter_when_validated_then_should_be_rejected() {
        let filter = ConsumerFilter::headers_only(FilterExpr::any([
            FilterExpr::header("table", CmpOp::Eq, "satellites"),
            FilterExpr::present("kind"),
        ]));
        assert!(filter.validate().is_err());
        let filter =
            ConsumerFilter::headers_only(FilterExpr::header("table", CmpOp::Eq, "satellites"));
        filter.validate().expect("header predicates are allowed");
    }

    #[test]
    fn given_invalid_literals_when_validated_then_should_be_rejected() {
        let cases = [
            FilterExpr::pred("a", CmpOp::Gt, TypedValue::Null),
            FilterExpr::pred("a", CmpOp::In, "x"),
            FilterExpr::pred("a", CmpOp::Eq, TypedValue::List(vec!["x".into()])),
            FilterExpr::pred("a", CmpOp::In, TypedValue::List(Vec::new())),
            FilterExpr::pred("a", CmpOp::In, TypedValue::List(vec![TypedValue::Null])),
            FilterExpr::pred("a", CmpOp::Prefix, 5),
            FilterExpr::pred("a", CmpOp::Lt, true),
            FilterExpr::pred("a", CmpOp::Eq, f64::NAN),
            FilterExpr::pred("a", CmpOp::Eq, TypedValue::Date(3)),
            FilterExpr::pred("a..b", CmpOp::Eq, 1),
            FilterExpr::header("", CmpOp::Eq, "x"),
            FilterExpr::header("kind", CmpOp::Eq, TypedValue::Null),
            FilterExpr::all([]),
        ];
        for expr in cases {
            let filter = ConsumerFilter::json(expr.clone());
            assert!(filter.validate().is_err(), "{expr:?} must be rejected");
        }
    }

    #[test]
    fn given_coerced_literals_when_validated_then_should_parse_under_their_coercion() {
        let valid = [
            FilterExpr::pred_as(
                "date",
                CmpOp::Gte,
                "2026-09-21T00:00:00Z",
                Coerce::Timestamp {
                    format: TimestampFormat::Rfc3339,
                },
            ),
            FilterExpr::pred_as(
                "timestamp_ms",
                CmpOp::Lt,
                1_758_470_652_331_i64,
                Coerce::Timestamp {
                    format: TimestampFormat::EpochMillis,
                },
            ),
            FilterExpr::pred_as("amount", CmpOp::Gt, "12.50", Coerce::Number),
            FilterExpr::pred_as(
                "amount",
                CmpOp::In,
                TypedValue::List(vec!["1".into(), 2_i64.into()]),
                Coerce::Number,
            ),
        ];
        for expr in valid {
            ConsumerFilter::json(expr.clone())
                .validate()
                .unwrap_or_else(|error| panic!("{expr:?} must be valid: {error}"));
        }
        let invalid = [
            FilterExpr::pred_as(
                "date",
                CmpOp::Gte,
                "2026-09-21T00:00:00",
                Coerce::Timestamp {
                    format: TimestampFormat::Rfc3339,
                },
            ),
            FilterExpr::pred_as("amount", CmpOp::Gt, "1e5", Coerce::Number),
            FilterExpr::pred_as("amount", CmpOp::Contains, "1", Coerce::Number),
            FilterExpr::pred_as("amount", CmpOp::Gt, 1.5, Coerce::Number),
        ];
        for expr in invalid {
            assert!(
                ConsumerFilter::json(expr.clone()).validate().is_err(),
                "{expr:?} must be rejected"
            );
        }
    }

    #[test]
    fn given_limits_when_exceeded_then_should_be_rejected() {
        let mut expr = FilterExpr::pred("a", CmpOp::Eq, 1);
        for _ in 0..MAX_FILTER_DEPTH {
            expr = FilterExpr::negate(expr);
        }
        assert!(ConsumerFilter::json(expr).validate().is_err(), "too deep");
        let wide = FilterExpr::any(
            (0..MAX_FILTER_NODES).map(|index| FilterExpr::pred(format!("f{index}"), CmpOp::Eq, 1)),
        );
        assert!(
            ConsumerFilter::json(wide).validate().is_err(),
            "too many nodes"
        );
        let mut filter = safe_mode_filter();
        filter.schema_refs = vec![7];
        assert!(
            filter.validate().is_err(),
            "schema references are not served yet"
        );
        let mut filter = safe_mode_filter();
        filter.evaluator_version = 2;
        assert!(filter.validate().is_err(), "unknown evaluator version");
    }

    #[test]
    fn given_a_filter_when_json_round_tripped_then_should_use_typed_value_tags() {
        let filter = ConsumerFilter::json(FilterExpr::all([
            FilterExpr::pred("op", CmpOp::Eq, "d"),
            FilterExpr::present("fields.mode"),
        ]));
        let json = serde_json::to_value(&filter).expect("encodes");
        assert_eq!(
            json,
            serde_json::json!({
                "v": 1,
                "evaluator_version": 1,
                "expr": {"all": [
                    {"pred": {"field": "op", "op": "eq", "value": {"kind": "string", "value": "d"}}},
                    {"present": "fields.mode"}
                ]},
                "codec": "json",
                "fault_policy": "stop"
            })
        );
        let back: ConsumerFilter = serde_json::from_value(json).expect("decodes");
        assert_eq!(back, filter);
    }
}

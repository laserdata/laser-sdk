use crate::codes::QUERY_OP_VERSION;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One exact-match constraint: the indexed `field` must equal `value`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyMatch {
    pub field: String,
    pub value: String,
}

impl KeyMatch {
    /// An exact-match predicate, `field == value`.
    pub fn new(field: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            value: value.into(),
        }
    }
}

/// A query against a materialized index. Build it fluently via the SDK's
/// `Laser::query`, or directly through [`Query::builder`].
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "builders", derive(bon::Builder))]
pub struct Query {
    // A materialized index name (produced by a projection), not a raw topic.
    #[cfg_attr(feature = "builders", builder(into))]
    pub index: String,
    #[cfg_attr(feature = "builders", builder(default))]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub by_key: Vec<KeyMatch>,
    #[cfg_attr(feature = "builders", builder(into))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_type: Option<String>,
    // (start, end) in epoch microseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_range: Option<(u64, u64)>,
    // Predicate tree. `None` plus empty sugar is an unfiltered scan. Build
    // trees with `Filter::all`/`any`/`not`/`pred`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorQuery>,
    #[cfg_attr(feature = "builders", builder(default))]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order: Vec<Sort>,
    // NOTE the asymmetry, current behavior moved as-is: the builder defaults
    // `limit` to 50, serde to 0 (a `0` limit means "a full page" managed-side).
    #[cfg_attr(feature = "builders", builder(default = 50))]
    pub limit: usize,
    #[cfg_attr(feature = "builders", builder(default))]
    #[serde(default)]
    pub offset: usize,
    // Analytics, mutually exclusive with row selection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aggregate: Option<Aggregate>,
    // Filter on aggregate output (predicate fields reference an alias or group
    // key). Only meaningful with `aggregate`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub having: Option<Filter>,
    // DISTINCT over the selected fields.
    #[cfg_attr(feature = "builders", builder(default))]
    #[serde(default, skip_serializing_if = "is_false")]
    pub distinct: bool,
    #[cfg_attr(feature = "builders", builder(default))]
    #[serde(default)]
    pub select: Select,
    // Resolve against a fork's copy-on-write view (trunk overlaid with the fork's
    // speculative rows) instead of the trunk. Absent on the wire for a trunk
    // query, so the pre-fork contract is unchanged.
    #[cfg_attr(feature = "builders", builder(into))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork: Option<String>,
    // Opt-in raw-SQL escape hatch. SQL backends only, read-only single SELECT.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_sql: Option<RawSql>,
    // Read-consistency level. Absent on the wire for the default (`Eventual`),
    // so the pre-consistency contract is unchanged.
    #[cfg_attr(feature = "builders", builder(default))]
    #[serde(default, skip_serializing_if = "Consistency::is_eventual")]
    pub consistency: Consistency,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// How fresh a query's view of the materialized index must be. A materialized
/// view is a read model a projector builds by tailing the log, so it is
/// eventually consistent: a record is queryable once the projector has applied
/// it, not the instant it is appended. This level says what the query requires
/// of that lag, and the contract is fail-not-downgrade: a level that cannot be
/// met returns [`QueryError::Stale`] rather than silently serving older data.
// `Ord` follows the declaration order, which is the strength ladder
// (Eventual < ReadYourWrites < Strong), so a stronger level compares greater and
// a capability check is `want <= served`. A new variant must be appended to keep
// the order meaningful.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Consistency {
    /// Serve from the index as-is, whatever the projector has applied so far.
    /// The default and the cheapest: no wait, best for dashboards and scans
    /// where a little lag is fine.
    #[default]
    Eventual,
    /// Wait until the projector has applied the source log up to its current
    /// head before serving, so a query issued after a publish sees that write
    /// (read-your-writes). Bounded: if the projector cannot catch up within the
    /// managed deadline the query returns [`QueryError::Stale`] instead of
    /// downgrading to a stale read. Backend-gated by `read_your_writes`.
    ReadYourWrites,
    /// The strongest level: a linearizable read across replicas. Backend-gated
    /// by `strong_consistency`. Where unavailable the query returns a clean
    /// unsupported error. Semantics past read-your-writes are still being
    /// pinned, so treat it as read-your-writes plus cross-replica agreement.
    Strong,
}

impl Consistency {
    /// Whether this is the default `Eventual` level (omitted on the wire).
    pub fn is_eventual(&self) -> bool {
        matches!(self, Consistency::Eventual)
    }
}

/// The server-side gate that enforces a [`Consistency`] level the same way on
/// every backend. The client refuses an unadvertised level before sending (A12),
/// but a backend that does advertise `read_your_writes` or `strong_consistency`
/// still has to honor the level, and the rule is fail-not-downgrade: serve only
/// when the projector's `applied` offset for the queried source has reached the
/// `required` offset (the source log head at query time), else return
/// [`QueryError::Stale`] rather than a silently older read.
///
/// This is the offset obligation common to both non-`Eventual` levels.
/// `Strong` is read-your-writes plus cross-replica agreement, so a backend
/// serving `Strong` layers its own cross-replica check on top of a passing
/// gate. `Eventual` always passes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsistencyGate {
    /// The projector's applied offset for the queried source.
    pub applied: u64,
    /// The offset the read must reach before serving (the source log head).
    pub required: u64,
}

impl ConsistencyGate {
    /// A gate for a source whose projector has applied up to `applied` against a
    /// head of `required`.
    pub fn new(applied: u64, required: u64) -> Self {
        Self { applied, required }
    }

    /// Whether the projector has caught up to the required offset.
    pub fn is_caught_up(&self) -> bool {
        self.applied >= self.required
    }

    /// Enforce `level` for the source named `what`. `Eventual` always passes. A
    /// non-`Eventual` level passes only when [`is_caught_up`](Self::is_caught_up),
    /// else returns [`QueryError::Stale`] carrying the offsets so the caller can
    /// retry while the projector catches up.
    pub fn check(&self, level: Consistency, what: impl Into<String>) -> Result<(), QueryError> {
        if level.is_eventual() || self.is_caught_up() {
            return Ok(());
        }
        Err(QueryError::Stale {
            what: what.into(),
            applied: self.applied,
            required: self.required,
        })
    }
}

/// A predicate tree. `All`/`Any` are n-ary, `Not` negates, `Pred` is a single
/// comparison leaf. Externally tagged on the wire:
/// `{"all":[{"pred":{...}}]}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Filter {
    All(Vec<Filter>),
    Any(Vec<Filter>),
    Not(Box<Filter>),
    Pred(Predicate),
}

impl Filter {
    /// AND of `filters`.
    pub fn all(filters: impl IntoIterator<Item = Filter>) -> Self {
        Filter::All(filters.into_iter().collect())
    }

    /// OR of `filters`.
    pub fn any(filters: impl IntoIterator<Item = Filter>) -> Self {
        Filter::Any(filters.into_iter().collect())
    }

    /// Negate `filter`.
    pub fn negate(filter: Filter) -> Self {
        Filter::Not(Box::new(filter))
    }

    /// A single comparison leaf, `field op value`.
    pub fn pred(field: impl Into<String>, op: CmpOp, value: impl Into<Value>) -> Self {
        Filter::Pred(Predicate {
            field: field.into(),
            op,
            value: value.into(),
        })
    }
}

/// A filter leaf: a field, a comparison op, and a value.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Predicate {
    pub field: String,
    pub op: CmpOp,
    pub value: Value,
}

/// Raw-SQL escape hatch. `sql` must be a single read-only SELECT. `params` bind
/// positionally. SQL backends only.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RawSql {
    pub sql: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<Value>,
}

/// A comparison operator for a `Predicate`.
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
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    Contains,
    Prefix,
}

/// An order-by clause: a field and a direction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Sort {
    pub field: String,
    #[serde(default)]
    pub dir: Dir,
}

/// Sort direction (ascending or descending).
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
pub enum Dir {
    #[default]
    Asc,
    Desc,
}

/// A nearest-neighbour search: the query embedding and how many rows to return.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorQuery {
    pub field: String,
    pub embedding: Vec<f32>,
    pub top_k: usize,
}

/// A grouped aggregation carrying one or more [`AggCall`]s, so a single query
/// can return several aggregates grouped by the same keys. An optional `window`
/// adds a time-bucket key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Aggregate {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    pub funcs: Vec<AggCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<Window>,
}

/// One aggregate in an [`Aggregate`]. `field` is `None` only for `Count`, and `arg`
/// is the fraction for `Percentile` (e.g. 0.95). `alias` is the output header
/// key on each result row.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggCall {
    pub func: AggFunc,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg: Option<f64>,
    pub alias: String,
}

/// An aggregate function. `Percentile` and `StdDev` are backend-gated (the
/// embedded engine does not provide them, a columnar backend does).
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
pub enum AggFunc {
    Count,
    CountDistinct,
    Sum,
    Avg,
    Min,
    Max,
    Percentile,
    StdDev,
}

/// A tumbling window of `every_micros` over the timestamp `field`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Window {
    pub field: String,
    pub every_micros: u64,
}

/// Which columns and payload a query returns.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Select {
    // Empty selects every indexed field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
    // Return the opaque payload bytes alongside the indexed fields.
    #[serde(default)]
    pub payload: bool,
}

/// A scalar value in a predicate or result row. `#[serde(untagged)]`, riding the
/// wire as a bare scalar. Variant order matters for untagged decode: `Int`
/// before `Uint` keeps small/negative integers as `i64`, and a value past
/// `i64::MAX` falls through to `Uint` before `Float` (never a lossy `f64`).
/// `Null` is a unit variant matching a bare `null`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Value {
    Str(String),
    Int(i64),
    Uint(u64),
    Float(f64),
    Bool(bool),
    Null,
    List(Vec<Value>),
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::Str(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::Str(value)
    }
}

impl From<&String> for Value {
    fn from(value: &String) -> Self {
        Self::Str(value.clone())
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::Uint(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::Int(value as i64)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self::Int(value as i64)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Self::Float(value as f64)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(values: Vec<T>) -> Self {
        Self::List(values.into_iter().map(Into::into).collect())
    }
}

impl Value {
    /// Infer a scalar from a user-typed string, the inverse of [`Display`] for a
    /// UI input box. The narrowest type wins: `"null"` is [`Value::Null`],
    /// `"true"`/`"false"` are [`Value::Bool`], a bare integer is
    /// [`Value::Int`] (or [`Value::Uint`] past `i64::MAX`), a digits-and-dot
    /// decimal is [`Value::Float`], and everything else is [`Value::Str`].
    /// Lists are built structurally (e.g. for [`CmpOp::In`]), never inferred
    /// here, so this never fails. Round-trips for every non-string scalar. A
    /// string that happens to look like a number narrows to that number (so
    /// `Display` then `from_input` is not the identity for a [`Value::Str`] of
    /// numeric text, by design).
    pub fn from_input(input: &str) -> Self {
        match input {
            "null" => return Value::Null,
            "true" => return Value::Bool(true),
            "false" => return Value::Bool(false),
            _ => {}
        }
        if let Ok(int) = input.parse::<i64>() {
            return Value::Int(int);
        }
        if let Ok(uint) = input.parse::<u64>() {
            return Value::Uint(uint);
        }
        // Only digit-and-dot decimals narrow to a float. This rejects the
        // float parser's `inf`/`nan`/`1e9` surprises a plain word would hit.
        if !input.is_empty()
            && input
                .bytes()
                .all(|b| b.is_ascii_digit() || b == b'.' || b == b'-' || b == b'+')
            && let Ok(float) = input.parse::<f64>()
        {
            return Value::Float(float);
        }
        Value::Str(input.to_owned())
    }
}

impl std::fmt::Display for Value {
    /// Renders a scalar as its bare form (no quotes), so it reads naturally in a
    /// UI cell or a predicate echo. A [`Value::List`] renders as `[a, b, c]`
    /// over its elements' own `Display`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Str(value) => f.write_str(value),
            Value::Int(value) => write!(f, "{value}"),
            Value::Uint(value) => write!(f, "{value}"),
            Value::Float(value) => write!(f, "{value}"),
            Value::Bool(value) => write!(f, "{value}"),
            Value::Null => f.write_str("null"),
            Value::List(values) => {
                f.write_str("[")?;
                for (index, value) in values.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{value}")?;
                }
                f.write_str("]")
            }
        }
    }
}

impl std::str::FromStr for Value {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Value::from_input(s))
    }
}

/// A page of result rows plus pagination info.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QueryResult {
    pub rows: Vec<Row>,
    // Pagination metadata for this page of `rows` (total matches, more available).
    #[serde(default)]
    pub page: Page,
}

/// Pagination info for a query result (offset, limit, total, has_more).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page {
    // The offset this page started at, echoed back.
    pub offset: usize,
    // The effective limit applied (the query's `limit`, clamped to the page cap).
    pub limit: usize,
    // Total rows matching the query before `offset`/`limit` - the count to page over.
    pub total: usize,
    // Whether rows beyond this page exist (`offset + rows.len() < total`).
    pub has_more: bool,
}

impl Page {
    /// Total pages at this page's `limit` (0 when `limit` is 0).
    pub fn total_pages(&self) -> usize {
        if self.limit == 0 {
            0
        } else {
            self.total.div_ceil(self.limit)
        }
    }
}

/// One materialized row: indexed fields, metadata, log position, and optional payload/score.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Row {
    // Indexed fields + provenance, keyed by the name after `agdx.idx.`.
    pub headers: BTreeMap<String, String>,
    // Ride-along publisher headers (content_type, schema_id, any custom user
    // metadata). Always returned - free to inspect even when the payload was
    // not requested.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    // The Iggy partition this row was projected from. Skipped when
    // serializing if the backend does not populate it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<u32>,
    // The Iggy offset this row was projected from.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    // Inline payload bytes, present only when the publisher inlined the body
    // AND the query asked for it. Owned `Vec<u8>` so the public API never
    // leaks the `bytes` crate. On the wire it is a CBOR byte string.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub payload: Option<Vec<u8>>,
    // Set for vector queries: the similarity score of the row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
}

/// Internal on-wire envelope: a versioned wrapper around `Query`. Workers and
/// clients use it, app code does not.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct QueryEnvelope {
    pub v: u32,
    pub query: Query,
}

impl QueryEnvelope {
    /// Constructor for the non-exhaustive wire struct.
    pub fn new(query: Query) -> Self {
        Self {
            v: QUERY_OP_VERSION,
            query,
        }
    }
}

/// A query reply: `Ok(QueryResult)` or `Err(QueryError)`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum QueryReply {
    Ok(QueryResult),
    Err(QueryError),
}

/// Why a query failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[non_exhaustive]
pub enum QueryError {
    #[error("query not supported: {0}")]
    Unsupported(String),
    #[error("index not found: {0}")]
    IndexNotFound(String),
    #[error("fork not found: {0}")]
    ForkNotFound(String),
    #[error("backend error: {0}")]
    Backend(String),
    /// The query asked for more than a single reply may carry: a `limit`
    /// above the page cap, or a result whose inline payloads exceed the
    /// LaserData Cloud's reply-byte budget. `what` names the bound hit ("limit" /
    /// "reply bytes"), `size` is what was requested or reached, `cap` is the
    /// ceiling. Page with `limit`/`offset` (or drop the payload request)
    /// rather than retrying unchanged.
    #[error("result too large: {what} {size} exceeds cap {cap}")]
    TooLarge {
        what: String,
        size: usize,
        cap: usize,
    },
    #[error("unsupported envelope version (expected {expected}, got {got})")]
    Version { expected: u32, got: u32 },
    /// A [`Consistency`] level could not be met within the managed deadline: the
    /// projector's applied offset for the queried source sits at `applied` while
    /// the level required `required`. Fail-not-downgrade, so the caller retries
    /// (the projector is catching up) rather than unknowingly reading stale
    /// data. `what` names the source (index or partition) that lagged.
    #[error("stale read: {what} applied {applied}, required {required}")]
    Stale {
        what: String,
        applied: u64,
        required: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_dsl_enums_when_displayed_then_should_be_snake_case() {
        assert_eq!(CmpOp::Gte.to_string(), "gte");
        assert_eq!(CmpOp::Prefix.to_string(), "prefix");
        assert_eq!("ne".parse::<CmpOp>().expect("ne parses"), CmpOp::Ne);
        assert_eq!(Dir::Desc.to_string(), "desc");
        assert_eq!(AggFunc::Count.to_string(), "count");
    }

    #[test]
    fn given_a_consistency_gate_when_checked_then_should_fail_not_downgrade() {
        // Eventual always passes, regardless of lag.
        assert!(
            ConsistencyGate::new(0, 100)
                .check(Consistency::Eventual, "orders")
                .is_ok()
        );
        // A non-Eventual level passes only once caught up.
        assert!(
            ConsistencyGate::new(100, 100)
                .check(Consistency::ReadYourWrites, "orders")
                .is_ok()
        );
        let stale = ConsistencyGate::new(41, 57)
            .check(Consistency::Strong, "orders")
            .expect_err("a lagging projector must fail, never downgrade");
        assert!(matches!(
            stale,
            QueryError::Stale {
                applied: 41,
                required: 57,
                ..
            }
        ));
    }

    #[test]
    fn given_a_page_when_computing_total_pages_then_should_divide_by_limit() {
        let page = Page {
            offset: 0,
            limit: 3,
            total: 10,
            has_more: true,
        };
        assert_eq!(page.total_pages(), 4);
        assert_eq!(Page::default().total_pages(), 0);
    }
}

#[cfg(all(test, feature = "codecs"))]
mod serde_tests {
    use super::*;
    #[cfg(feature = "builders")]
    use crate::codes::QUERY_OP_VERSION;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_dsl_enums_when_serialized_then_serde_should_match_display() {
        assert_eq!(
            serde_json::to_string(&CmpOp::Lte).expect("CmpOp serializes"),
            "\"lte\""
        );
        assert_eq!(
            serde_json::from_str::<CmpOp>("\"in\"").expect("CmpOp deserializes"),
            CmpOp::In
        );
        assert_eq!(
            serde_json::to_string(&Dir::Asc).expect("Dir serializes"),
            "\"asc\""
        );
    }

    #[test]
    #[cfg(feature = "builders")]
    fn given_a_query_when_round_tripped_through_the_envelope_then_should_be_unchanged() {
        let query = Query::builder()
            .index("orders")
            .by_key(vec![KeyMatch::new("customer_id", "abc")])
            .filter(Filter::pred("status", CmpOp::Eq, "paid"))
            .order(vec![Sort {
                field: "ts".to_owned(),
                dir: Dir::Desc,
            }])
            .limit(20)
            .build();
        let request = QueryEnvelope::new(query);

        let json = serde_json::to_string(&request).expect("the request serializes");
        let back: QueryEnvelope = serde_json::from_str(&json).expect("the request deserializes");
        assert_eq!(back.v, QUERY_OP_VERSION);
        assert_eq!(back.query.index, "orders");
        assert_eq!(back.query.limit, 20);
        assert_eq!(back.query.by_key, vec![KeyMatch::new("customer_id", "abc")]);
        let Some(Filter::Pred(predicate)) = &back.query.filter else {
            panic!("expected a single predicate filter");
        };
        assert_eq!(predicate.value, Value::Str("paid".to_owned()));
        assert_eq!(back.query.order[0].dir, Dir::Desc);
    }

    #[test]
    #[cfg(feature = "builders")]
    fn given_each_consistency_level_when_round_tripped_then_should_preserve_it_and_skip_eventual() {
        for level in [
            Consistency::Eventual,
            Consistency::ReadYourWrites,
            Consistency::Strong,
        ] {
            let query = Query::builder().index("orders").consistency(level).build();
            let bytes = encode_named(&QueryEnvelope::new(query)).expect("serializes");
            let back: QueryEnvelope = decode_named(&bytes).expect("deserializes");
            assert_eq!(back.query.consistency, level);
        }
        // The default `Eventual` is omitted on the wire so the pre-consistency
        // contract stays byte-identical.
        let default = Query::builder().index("orders").build();
        assert_eq!(default.consistency, Consistency::Eventual);
        let json = serde_json::to_string(&default).expect("json");
        assert!(
            !json.contains("consistency"),
            "default Eventual must be omitted: {json}"
        );
    }

    #[test]
    fn given_a_stale_reply_when_round_tripped_then_should_preserve_the_offsets() {
        let reply = QueryReply::Err(QueryError::Stale {
            what: "orders".to_owned(),
            applied: 41,
            required: 57,
        });
        let bytes = encode_named(&reply).expect("serializes");
        let back: QueryReply = decode_named(&bytes).expect("deserializes");
        let QueryReply::Err(QueryError::Stale {
            what,
            applied,
            required,
        }) = back
        else {
            panic!("expected a Stale error");
        };
        assert_eq!((what.as_str(), applied, required), ("orders", 41, 57));
    }

    #[test]
    #[cfg(feature = "builders")]
    fn given_a_vector_query_when_round_tripped_then_should_preserve_the_embedding() {
        let query = Query::builder()
            .index("mem:conv-1")
            .vector(VectorQuery {
                field: "embedding".to_owned(),
                embedding: vec![0.1, 0.2, 0.3],
                top_k: 5,
            })
            .build();
        let json = serde_json::to_string(&query).expect("the query serializes");
        let back: Query = serde_json::from_str(&json).expect("the query deserializes");
        let vector = back.vector.expect("the vector survives the round-trip");
        assert_eq!(vector.embedding, vec![0.1, 0.2, 0.3]);
        assert_eq!(vector.top_k, 5);
    }

    #[test]
    fn given_a_reply_with_a_payload_row_when_round_tripped_then_should_preserve_the_bytes() {
        let mut headers = BTreeMap::new();
        headers.insert("order_id".to_owned(), "123".to_owned());
        let reply = QueryReply::Ok(QueryResult {
            rows: vec![Row {
                headers,
                metadata: BTreeMap::from([("agdx.ct".to_owned(), "1".to_owned())]),
                partition: Some(2),
                offset: Some(17),
                payload: Some(b"{\"total\":42}".to_vec()),
                score: None,
            }],
            page: Page {
                offset: 0,
                limit: 50,
                total: 1,
                has_more: false,
            },
        });
        let bytes = encode_named(&reply).expect("the reply serializes");
        let back: QueryReply = decode_named(&bytes).expect("the reply deserializes");
        let QueryReply::Ok(result) = back else {
            panic!("the reply should decode as Ok");
        };
        assert_eq!(result.rows[0].headers["order_id"], "123");
        assert_eq!(
            result.rows[0].payload.as_deref(),
            Some(b"{\"total\":42}".as_ref())
        );
        assert_eq!(result.page.total, 1);
        assert!(!result.page.has_more);
    }
}

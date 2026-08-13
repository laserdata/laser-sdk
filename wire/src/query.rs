use crate::agent::wire_id;
use crate::codes::QUERY_OP_VERSION;
use crate::destination::{BackendResourceId, DestinationId};
use crate::error::InvalidError;
use crate::limits::{
    MAX_PAGE_SIZE, MAX_QUERY_CURSOR_BYTES, MAX_QUERY_FIELDS, MAX_QUERY_NAME_BYTES,
    MAX_QUERY_PARAMETERS, MAX_QUERY_PREDICATES, MAX_RAW_SQL_BYTES,
};
use crate::schema::{Digest32, LogicalField, TypedValue, UuidValue, validate_result_fields};
use crate::validate::Validate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

wire_id!(
    /// Unguessable identity used for query status, paging, and cancellation.
    QueryExecutionId
);

/// The resource against which a query is authorized and resolved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum QueryTarget {
    Operational {
        index: String,
    },
    Lakehouse {
        destination_id: DestinationId,
        destination_generation: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        snapshot: Option<SnapshotSelector>,
    },
}

impl QueryTarget {
    pub fn operational(index: impl Into<String>) -> Self {
        Self::Operational {
            index: index.into(),
        }
    }
}

/// An optional time-travel selector resolved to one exact Iceberg snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SnapshotSelector {
    SnapshotId(i64),
    TimestampMicros(i64),
}

/// SQL grammar accepted by the selected query backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SqlDialect {
    DataFusion,
    Postgres,
    MySql,
    Sqlite,
}

/// One exact-match constraint: the indexed `field` must equal `value`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyMatch {
    pub field: String,
    pub value: TypedValue,
}

impl KeyMatch {
    /// An exact-match predicate, `field == value`.
    pub fn new(field: impl Into<String>, value: impl Into<TypedValue>) -> Self {
        Self {
            field: field.into(),
            value: value.into(),
        }
    }
}

/// A query against a materialized index. Build it fluently via the SDK's
/// `Laser::query`, or directly through [`Query::builder`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "builders", derive(bon::Builder))]
pub struct Query {
    pub execution_id: QueryExecutionId,
    pub target: QueryTarget,
    // Absolute Unix deadline. A server never extends it while queueing or paging.
    pub deadline_micros: u64,
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
    // Lexical relevance search over the text-hinted indexed fields, additive
    // like `vector`. An unaware server would silently drop it (the A8 additive
    // hazard), so the client refuses an unadvertised `text` before sending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<TextQuery>,
    #[cfg_attr(feature = "builders", builder(default))]
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order: Vec<Sort>,
    #[cfg_attr(feature = "builders", builder(default))]
    #[serde(default)]
    pub page: QueryPageRequest,
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

impl Query {
    /// Create a query for an explicit target and execution boundary.
    pub fn new(execution_id: QueryExecutionId, target: QueryTarget, deadline_micros: u64) -> Self {
        Self {
            execution_id,
            target,
            deadline_micros,
            by_key: Vec::new(),
            message_type: None,
            time_range: None,
            filter: None,
            vector: None,
            text: None,
            order: Vec::new(),
            page: QueryPageRequest::default(),
            aggregate: None,
            having: None,
            distinct: false,
            select: Select::default(),
            fork: None,
            raw_sql: None,
            consistency: Consistency::Eventual,
        }
    }

    /// Create an operational query for one materialized index.
    pub fn operational(
        execution_id: QueryExecutionId,
        index: impl Into<String>,
        deadline_micros: u64,
    ) -> Self {
        Self::new(
            execution_id,
            QueryTarget::operational(index),
            deadline_micros,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPageRequest {
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub want_total: bool,
}

impl Default for QueryPageRequest {
    fn default() -> Self {
        Self {
            limit: 50,
            offset: Some(0),
            cursor: None,
            want_total: false,
        }
    }
}

impl Validate for Query {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.execution_id.as_u128() == 0 {
            return Err(InvalidError::new("query execution id must be nonzero"));
        }
        if self.deadline_micros == 0 {
            return Err(InvalidError::new("query deadline must be nonzero"));
        }
        match &self.target {
            QueryTarget::Operational { index } => validate_query_name("index", index)?,
            QueryTarget::Lakehouse {
                destination_id,
                destination_generation,
                snapshot,
            } => {
                if destination_id.as_u128() == 0 || *destination_generation == 0 {
                    return Err(InvalidError::new(
                        "lakehouse destination identity and generation must be nonzero",
                    ));
                }
                if let Some(selector) = snapshot {
                    match selector {
                        SnapshotSelector::SnapshotId(value)
                        | SnapshotSelector::TimestampMicros(value)
                            if *value <= 0 =>
                        {
                            return Err(InvalidError::new(
                                "snapshot selector must contain a positive value",
                            ));
                        }
                        _ => {}
                    }
                }
                if self.fork.is_some() {
                    return Err(InvalidError::new(
                        "a lakehouse query cannot resolve against an operational fork",
                    ));
                }
            }
        }
        if self.page.limit == 0 || self.page.limit > MAX_PAGE_SIZE as u32 {
            return Err(InvalidError::new(format!(
                "query limit {} is outside 1..={MAX_PAGE_SIZE}",
                self.page.limit
            )));
        }
        for (label, count) in [
            ("exact matches", self.by_key.len()),
            ("sort fields", self.order.len()),
            ("selected fields", self.select.fields.len()),
        ] {
            if count > MAX_QUERY_FIELDS {
                return Err(InvalidError::new(format!(
                    "query {label} count {count} exceeds cap {MAX_QUERY_FIELDS}"
                )));
            }
        }
        if self.page.offset.is_some() && self.page.cursor.is_some() {
            return Err(InvalidError::new(
                "query page cannot contain both an offset and a cursor",
            ));
        }
        if let Some(cursor) = &self.page.cursor
            && (cursor.is_empty()
                || cursor.len() > MAX_QUERY_CURSOR_BYTES
                || cursor.chars().any(char::is_control))
        {
            return Err(InvalidError::new(
                "query cursor must contain 1..=512 bytes without control characters",
            ));
        }
        for key_match in &self.by_key {
            validate_query_name("key-match field", &key_match.field)?;
            key_match.value.validate()?;
        }
        if let Some(message_type) = &self.message_type {
            validate_query_name("message type", message_type)?;
        }
        if let Some((start, end)) = self.time_range
            && start >= end
        {
            return Err(InvalidError::new(
                "query time range must be a nonempty half-open interval",
            ));
        }
        for sort in &self.order {
            validate_query_name("sort field", &sort.field)?;
        }
        for field in &self.select.fields {
            validate_query_name("selected field", field)?;
        }
        if let Some(text) = &self.text {
            text.validate()?;
        }
        if let Some(vector) = &self.vector {
            vector.validate()?;
        }
        validate_filter(self.filter.as_ref())?;
        validate_filter(self.having.as_ref())?;
        if self.having.is_some() && self.aggregate.is_none() {
            return Err(InvalidError::new(
                "query having filter requires an aggregate",
            ));
        }
        if let Some(aggregate) = &self.aggregate {
            aggregate.validate()?;
        }
        if self.aggregate.is_some() && (!self.select.fields.is_empty() || self.select.payload) {
            return Err(InvalidError::new(
                "aggregate queries cannot request row selection",
            ));
        }
        if let Some(raw_sql) = &self.raw_sql {
            raw_sql.validate()?;
            if !self.by_key.is_empty()
                || self.message_type.is_some()
                || self.time_range.is_some()
                || self.filter.is_some()
                || self.vector.is_some()
                || self.text.is_some()
                || !self.order.is_empty()
                || self.aggregate.is_some()
                || self.having.is_some()
                || self.distinct
                || !self.select.fields.is_empty()
                || self.select.payload
            {
                return Err(InvalidError::new(
                    "raw SQL cannot be combined with the structured query expression",
                ));
            }
        }
        Ok(())
    }
}

impl Validate for Aggregate {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.funcs.is_empty() || self.funcs.len() > MAX_QUERY_PARAMETERS {
            return Err(InvalidError::new(format!(
                "aggregate function count must be in 1..={MAX_QUERY_PARAMETERS}"
            )));
        }
        for field in &self.group_by {
            validate_query_name("group field", field)?;
        }
        if self.group_by.len() > MAX_QUERY_FIELDS {
            return Err(InvalidError::new(format!(
                "aggregate group field count exceeds cap {MAX_QUERY_FIELDS}"
            )));
        }
        let mut aliases = BTreeSet::new();
        for call in &self.funcs {
            validate_query_name("aggregate alias", &call.alias)?;
            if !aliases.insert(&call.alias) {
                return Err(InvalidError::new(format!(
                    "aggregate repeats alias `{}`",
                    call.alias
                )));
            }
            if let Some(field) = &call.field {
                validate_query_name("aggregate field", field)?;
            }
            match call.func {
                AggFunc::Count if call.arg.is_none() => {}
                AggFunc::Percentile
                    if call.field.is_some()
                        && call.arg.is_some_and(|value| {
                            value.is_finite() && (0.0..=1.0).contains(&value)
                        }) => {}
                AggFunc::Percentile => {
                    return Err(InvalidError::new(
                        "percentile requires a field and a finite fraction in 0..=1",
                    ));
                }
                _ if call.field.is_some() && call.arg.is_none() => {}
                _ => {
                    return Err(InvalidError::new(
                        "aggregate field and argument do not match the selected function",
                    ));
                }
            }
        }
        if let Some(window) = &self.window {
            validate_query_name("window field", &window.field)?;
            if window.every_micros == 0 {
                return Err(InvalidError::new("window duration must be nonzero"));
            }
        }
        Ok(())
    }
}

impl Validate for RawSql {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.sql.trim().is_empty() || self.sql.len() > MAX_RAW_SQL_BYTES {
            return Err(InvalidError::new(format!(
                "raw SQL must contain 1..={MAX_RAW_SQL_BYTES} bytes"
            )));
        }
        if self
            .sql
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
        {
            return Err(InvalidError::new("raw SQL contains a control character"));
        }
        if self.params.len() > MAX_QUERY_PARAMETERS {
            return Err(InvalidError::new(format!(
                "raw SQL has {} parameters, exceeds cap {MAX_QUERY_PARAMETERS}",
                self.params.len()
            )));
        }
        for param in &self.params {
            param.validate()?;
        }
        Ok(())
    }
}

fn validate_query_name(label: &str, value: &str) -> Result<(), InvalidError> {
    if value.is_empty() || value.len() > MAX_QUERY_NAME_BYTES {
        return Err(InvalidError::new(format!(
            "{label} must contain 1..={MAX_QUERY_NAME_BYTES} bytes"
        )));
    }
    if value.trim() != value || value.chars().any(char::is_control) {
        return Err(InvalidError::new(format!(
            "{label} contains surrounding whitespace or a control character"
        )));
    }
    Ok(())
}

fn validate_filter(filter: Option<&Filter>) -> Result<(), InvalidError> {
    fn visit(filter: &Filter, depth: usize, count: &mut usize) -> Result<(), InvalidError> {
        *count += 1;
        if *count > MAX_QUERY_PREDICATES {
            return Err(InvalidError::new(format!(
                "query filter node count exceeds cap {MAX_QUERY_PREDICATES}"
            )));
        }
        if depth > 64 {
            return Err(InvalidError::new("query filter depth exceeds cap 64"));
        }
        match filter {
            Filter::All(filters) | Filter::Any(filters) => {
                if filters.is_empty() {
                    return Err(InvalidError::new(
                        "query filter conjunction must not be empty",
                    ));
                }
                for filter in filters {
                    visit(filter, depth + 1, count)?;
                }
                Ok(())
            }
            Filter::Not(filter) => visit(filter, depth + 1, count),
            Filter::Pred(predicate) => {
                validate_query_name("predicate field", &predicate.field)?;
                predicate.value.validate()?;
                if predicate.op == CmpOp::In
                    && !matches!(&predicate.value, TypedValue::List(values) if !values.is_empty())
                {
                    return Err(InvalidError::new(
                        "query IN predicate requires a nonempty typed list",
                    ));
                }
                if predicate.op == CmpOp::Prefix
                    && !matches!(&predicate.value, TypedValue::String(_))
                {
                    return Err(InvalidError::new(
                        "query prefix predicate requires a string value",
                    ));
                }
                Ok(())
            }
        }
    }

    let mut count = 0;
    filter.map_or(Ok(()), |filter| visit(filter, 1, &mut count))
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
/// every backend. The client refuses an unadvertised level before sending,
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
    pub fn pred(field: impl Into<String>, op: CmpOp, value: impl Into<TypedValue>) -> Self {
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
    pub value: TypedValue,
}

/// Raw-SQL escape hatch scoped to [`Query::target`]. The server validates both
/// the parsed statement and the resulting scan plan against that target.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RawSql {
    pub dialect: SqlDialect,
    pub sql: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<TypedValue>,
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

/// A lexical relevance search: the text to match and, optionally, the one
/// indexed field to match it in (`None` searches every text-hinted field).
/// Relevance is returned as an ordinary typed result field. Capability-gated by
/// `KEYWORD_SEARCH`: a backend without a lexical index answers unsupported,
/// never a contains approximation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TextQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub query: String,
}

/// A nearest-neighbour search: the query embedding and how many rows to return.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VectorQuery {
    pub field: String,
    pub embedding: Vec<f32>,
    pub top_k: u32,
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
/// is the fraction for `Percentile` (e.g. 0.95). `alias` names the output field
/// in each result row.
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

/// A scalar value used by the graph and older non-query document surfaces. Query
/// predicates and results use the tagged [`TypedValue`] contract. This remains
/// untagged because it is part of the separate graph wire surface.
///
/// `#[serde(untagged)]`, riding the
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
    /// Infer a scalar from a user-typed string, the inverse of [`std::fmt::Display`] for a
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

/// A typed page. Each row is positionally aligned with `fields`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryResult {
    pub fields: Vec<LogicalField>,
    pub rows: Vec<Row>,
    #[serde(default)]
    pub page: Page,
    pub context: QueryContext,
}

/// One typed query row. Values are ordered exactly like [`QueryResult::fields`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<TypedValue>,
    /// Backend-native relevance for vector or lexical search. Vector backends
    /// return distance and lexical backends return their declared rank.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
}

impl Validate for QueryResult {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_result_fields(&self.fields)?;
        for row in &self.rows {
            if row.values.len() != self.fields.len() {
                return Err(InvalidError::new(
                    "query row value count does not match the result schema",
                ));
            }
            for (value, field) in row.values.iter().zip(&self.fields) {
                value.validate_against(&field.field_type, field.required)?;
            }
            if row.score.is_some_and(|score| !score.is_finite()) {
                return Err(InvalidError::new("query row score must be finite"));
            }
        }
        if self.context.execution_id.as_u128() == 0 {
            return Err(InvalidError::new(
                "query result execution id must be nonzero",
            ));
        }
        if self.context.row_count != self.rows.len() as u64 {
            return Err(InvalidError::new(
                "query result row count does not match the page rows",
            ));
        }
        self.context.validate()?;
        self.page.validate()?;
        if self.page.has_more != self.page.next_cursor.is_some() {
            return Err(InvalidError::new(
                "query page has_more and next_cursor must agree",
            ));
        }
        Ok(())
    }
}

impl Validate for QueryContext {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.execution_id.as_u128() == 0 {
            return Err(InvalidError::new(
                "query context execution id must be nonzero",
            ));
        }
        validate_query_name("query engine name", &self.engine.name)?;
        validate_query_name("query engine version", &self.engine.version)?;
        if self.delivered_consistency < self.requested_consistency {
            return Err(InvalidError::new(
                "delivered consistency is weaker than requested consistency",
            ));
        }
        match &self.resolved_target {
            ResolvedQueryTarget::Operational {
                index,
                backend_resource_id,
                backend_generation,
                runtime_configuration_revision,
            } => {
                validate_query_name("resolved index", index)?;
                if backend_resource_id.as_u128() == 0
                    || *backend_generation == 0
                    || *runtime_configuration_revision == 0
                {
                    return Err(InvalidError::new(
                        "operational query context is missing backend evidence",
                    ));
                }
                if self.boundary.is_some()
                    || self.checkpoint_revision.is_some()
                    || self.global_state_revision.is_some()
                {
                    return Err(InvalidError::new(
                        "operational query context cannot carry lakehouse checkpoint evidence",
                    ));
                }
            }
            ResolvedQueryTarget::Lakehouse {
                destination_id,
                destination_generation,
                backend_resource_id,
                backend_generation,
                runtime_configuration_revision,
                table_uuid,
                namespace,
                table,
                snapshot_id,
                schema_id,
                partition_spec_id,
            } => {
                if destination_id.as_u128() == 0
                    || *destination_generation == 0
                    || backend_resource_id.as_u128() == 0
                    || *backend_generation == 0
                    || *runtime_configuration_revision == 0
                    || table_uuid.as_bytes().len() != UuidValue::BYTES
                    || namespace.is_empty()
                    || *snapshot_id <= 0
                    || *schema_id < 0
                    || *partition_spec_id < 0
                    || self.boundary.is_none()
                    || self.checkpoint_revision.is_none()
                    || self.global_state_revision.is_none()
                {
                    return Err(InvalidError::new(
                        "lakehouse query context is missing resolved target evidence",
                    ));
                }
                for part in namespace {
                    validate_query_name("resolved table namespace", part)?;
                }
                validate_query_name("resolved table", table)?;
            }
        }
        if let Some(boundary) = &self.boundary {
            boundary.digest.validate()?;
        }
        Ok(())
    }
}

impl QueryResult {
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.name == name)
    }

    pub fn value<'a>(&self, row: &'a Row, field: &str) -> Option<&'a TypedValue> {
        self.field_index(field)
            .and_then(|index| row.values.get(index))
    }

    pub fn value_text(&self, row: &Row, field: &str) -> Option<String> {
        self.value(row, field).map(TypedValue::diagnostic_text)
    }

    pub fn value_u64(&self, row: &Row, field: &str) -> Option<u64> {
        self.value(row, field).and_then(TypedValue::as_u64)
    }

    pub fn value_i64(&self, row: &Row, field: &str) -> Option<i64> {
        self.value(row, field).and_then(TypedValue::as_i64)
    }
}

/// Target and execution evidence returned with every page.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct QueryContext {
    pub execution_id: QueryExecutionId,
    pub engine: QueryEngine,
    pub resolved_target: ResolvedQueryTarget,
    pub requested_consistency: Consistency,
    pub delivered_consistency: Consistency,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boundary: Option<MaterializationBoundary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_state_revision: Option<u64>,
    pub truncated: bool,
    pub elapsed_micros: u64,
    pub scanned_bytes: u64,
    pub produced_bytes: u64,
    pub row_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryEngine {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dialect: Option<SqlDialect>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResolvedQueryTarget {
    Operational {
        index: String,
        backend_resource_id: BackendResourceId,
        backend_generation: u64,
        runtime_configuration_revision: u64,
    },
    Lakehouse {
        destination_id: DestinationId,
        destination_generation: u64,
        backend_resource_id: BackendResourceId,
        backend_generation: u64,
        runtime_configuration_revision: u64,
        table_uuid: UuidValue,
        namespace: Vec<String>,
        table: String,
        snapshot_id: i64,
        schema_id: i32,
        partition_spec_id: i32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationBoundary {
    pub digest: Digest32,
    pub relation_to_current: BoundaryRelation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BoundaryRelation {
    Current,
    Historical,
    AheadOfObservedCheckpoint,
}

/// Pagination info for a query result.
///
/// The default reply costs one page: the server fetches `limit + 1` rows and
/// answers `has_more` exactly from the probe row, never counting the rest.
/// `total` is present only when the request set `want_total`, which runs a
/// real `COUNT(*)` over the filter (unbounded work on a large index). A
/// caller that never asked cannot misread a page-bounded number as a count:
/// there is no number.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    // The effective limit applied after validation against the page cap.
    pub limit: u32,
    // Exact match count, present only when the request set `want_total`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    // Whether rows beyond this page exist. Always exact, always free.
    pub has_more: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

impl Page {
    /// Rows known to exist so far: the position one past this page's last
    /// row. A cheap display lower bound (`"1234+ rows"`), paired with
    /// [`Self::has_more`] for the trailing `+`.
    pub fn at_least(&self, rows_on_page: usize) -> Option<u64> {
        self.offset
            .and_then(|offset| offset.checked_add(rows_on_page as u64))
    }

    /// Pages implied by the exact total, when one was requested. `None`
    /// without `want_total` or with a zero limit (page math is undefined
    /// without a page size).
    pub fn total_pages(&self) -> Option<u64> {
        match (self.total, self.limit) {
            (Some(total), limit) if limit > 0 => Some(total.div_ceil(u64::from(limit))),
            _ => None,
        }
    }
}

impl Validate for Page {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.limit == 0 || self.limit > MAX_PAGE_SIZE as u32 {
            return Err(InvalidError::new(format!(
                "query result page limit exceeds cap {MAX_PAGE_SIZE}"
            )));
        }
        if self.has_more != self.next_cursor.is_some() {
            return Err(InvalidError::new(
                "query page has_more and next_cursor must agree",
            ));
        }
        if let Some(cursor) = &self.next_cursor
            && (cursor.is_empty()
                || cursor.len() > MAX_QUERY_CURSOR_BYTES
                || cursor.chars().any(char::is_control))
        {
            return Err(InvalidError::new("query result cursor is invalid"));
        }
        Ok(())
    }
}

/// Internal on-wire envelope: a versioned wrapper around `Query`. Workers and
/// clients use it, app code does not.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

impl Validate for QueryEnvelope {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.v != QUERY_OP_VERSION {
            return Err(InvalidError::new(format!(
                "query version must be {QUERY_OP_VERSION}, got {}",
                self.v
            )));
        }
        self.query.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPageEnvelope {
    pub v: u32,
    pub execution_id: QueryExecutionId,
    pub cursor: String,
    pub deadline_micros: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryCancelEnvelope {
    pub v: u32,
    pub execution_id: QueryExecutionId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryStatusEnvelope {
    pub v: u32,
    pub execution_id: QueryExecutionId,
}

impl QueryPageEnvelope {
    pub fn new(
        execution_id: QueryExecutionId,
        cursor: impl Into<String>,
        deadline_micros: u64,
    ) -> Self {
        Self {
            v: QUERY_OP_VERSION,
            execution_id,
            cursor: cursor.into(),
            deadline_micros,
        }
    }
}

impl QueryCancelEnvelope {
    pub const fn new(execution_id: QueryExecutionId) -> Self {
        Self {
            v: QUERY_OP_VERSION,
            execution_id,
        }
    }
}

impl QueryStatusEnvelope {
    pub const fn new(execution_id: QueryExecutionId) -> Self {
        Self {
            v: QUERY_OP_VERSION,
            execution_id,
        }
    }
}

impl Validate for QueryPageEnvelope {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_query_control(self.v, self.execution_id)?;
        if self.cursor.is_empty()
            || self.cursor.len() > MAX_QUERY_CURSOR_BYTES
            || self.cursor.chars().any(char::is_control)
            || self.deadline_micros == 0
        {
            return Err(InvalidError::new(
                "query page cursor or deadline is invalid",
            ));
        }
        Ok(())
    }
}

impl Validate for QueryCancelEnvelope {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_query_control(self.v, self.execution_id)
    }
}

impl Validate for QueryStatusEnvelope {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_query_control(self.v, self.execution_id)
    }
}

fn validate_query_control(v: u32, execution_id: QueryExecutionId) -> Result<(), InvalidError> {
    if v != QUERY_OP_VERSION {
        return Err(InvalidError::new(format!(
            "query version must be {QUERY_OP_VERSION}, got {v}"
        )));
    }
    if execution_id.as_u128() == 0 {
        return Err(InvalidError::new("query execution id must be nonzero"));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryExecutionStatus {
    pub execution_id: QueryExecutionId,
    pub state: QueryExecutionState,
    pub started_at_micros: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_micros: Option<u64>,
    pub scanned_bytes: u64,
    pub produced_bytes: u64,
    pub row_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<QueryError>,
}

impl Validate for QueryExecutionStatus {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.execution_id.as_u128() == 0 || self.started_at_micros == 0 {
            return Err(InvalidError::new(
                "query execution identity and start time must be nonzero",
            ));
        }
        let terminal = matches!(
            self.state,
            QueryExecutionState::Completed
                | QueryExecutionState::Cancelled
                | QueryExecutionState::Failed
                | QueryExecutionState::Expired
        );
        match (terminal, self.finished_at_micros) {
            (true, Some(finished)) if finished >= self.started_at_micros => {}
            (false, None) => {}
            _ => Err(InvalidError::new(
                "query execution finish time does not match its state",
            ))?,
        }
        if matches!(self.state, QueryExecutionState::Failed) != self.error.is_some() {
            return Err(InvalidError::new(
                "query execution error must be present exactly for failed state",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum QueryExecutionState {
    Queued,
    Planning,
    Running,
    Completed,
    Cancelled,
    Failed,
    Expired,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum QueryStatusReply {
    Ok(QueryExecutionStatus),
    Err(QueryError),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum QueryCancelReply {
    Ok(QueryExecutionStatus),
    Err(QueryError),
}

/// A query reply: `Ok(QueryResult)` or `Err(QueryError)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum QueryReply {
    Ok(Box<QueryResult>),
    Err(QueryError),
}

/// Why a query failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[non_exhaustive]
pub enum QueryError {
    #[error("query not supported: {0}")]
    Unsupported(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("index not found: {0}")]
    IndexNotFound(String),
    #[error("fork not found: {0}")]
    ForkNotFound(String),
    #[error("backend error: {0}")]
    Backend(String),
    /// The request could not be served right now and the identical request may
    /// succeed later: the store was momentarily out of reach, a connection was
    /// refused or dropped, or a concurrency conflict was aborted. Retry with
    /// backoff rather than treating it as a fault.
    #[error("temporarily unavailable: {0}")]
    Unavailable(String),
    /// The query asked for more than a single reply may carry: a `limit`
    /// above the page cap, or a result whose inline payloads exceed the
    /// LaserData Cloud's reply-byte budget. `what` names the bound hit ("limit" /
    /// "reply bytes"), `size` is what was requested or reached, `cap` is the
    /// ceiling. Page with the returned cursor (or drop the payload request)
    /// rather than retrying unchanged.
    #[error("result too large: {what} {size} exceeds cap {cap}")]
    TooLarge { what: String, size: u64, cap: u64 },
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
    #[error("query {execution_id} was cancelled")]
    Cancelled { execution_id: QueryExecutionId },
    #[error("query {execution_id} exceeded its deadline")]
    DeadlineExceeded { execution_id: QueryExecutionId },
    #[error("snapshot {snapshot_id} is no longer available")]
    ExpiredSnapshot { snapshot_id: i64 },
    #[error("stale {what} generation: requested {requested}, observed {observed}")]
    StaleGeneration {
        what: String,
        requested: u64,
        observed: u64,
    },
    #[error("query target unavailable: {reason}")]
    TargetUnavailable { reason: String },
    #[error("query resource limit exceeded: {resource} used {observed}, limit {limit}")]
    ResourceLimit {
        resource: String,
        observed: u64,
        limit: u64,
    },
}

/// Stable machine-readable classification for [`QueryError`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum QueryErrorCode {
    Unsupported,
    Unauthorized,
    IndexNotFound,
    ForkNotFound,
    Backend,
    Unavailable,
    TooLarge,
    Version,
    Stale,
    Cancelled,
    DeadlineExceeded,
    ExpiredSnapshot,
    StaleGeneration,
    TargetUnavailable,
    ResourceLimit,
}

impl QueryError {
    pub fn code(&self) -> QueryErrorCode {
        match self {
            Self::Unsupported(_) => QueryErrorCode::Unsupported,
            Self::Unauthorized(_) => QueryErrorCode::Unauthorized,
            Self::IndexNotFound(_) => QueryErrorCode::IndexNotFound,
            Self::ForkNotFound(_) => QueryErrorCode::ForkNotFound,
            Self::Backend(_) => QueryErrorCode::Backend,
            Self::Unavailable(_) => QueryErrorCode::Unavailable,
            Self::TooLarge { .. } => QueryErrorCode::TooLarge,
            Self::Version { .. } => QueryErrorCode::Version,
            Self::Stale { .. } => QueryErrorCode::Stale,
            Self::Cancelled { .. } => QueryErrorCode::Cancelled,
            Self::DeadlineExceeded { .. } => QueryErrorCode::DeadlineExceeded,
            Self::ExpiredSnapshot { .. } => QueryErrorCode::ExpiredSnapshot,
            Self::StaleGeneration { .. } => QueryErrorCode::StaleGeneration,
            Self::TargetUnavailable { .. } => QueryErrorCode::TargetUnavailable,
            Self::ResourceLimit { .. } => QueryErrorCode::ResourceLimit,
        }
    }
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
            offset: Some(0),
            limit: 3,
            total: Some(10),
            has_more: true,
            next_cursor: Some("next".to_owned()),
        };
        assert_eq!(page.total_pages(), Some(4));
        assert_eq!(page.at_least(3), Some(3), "offset 0 plus this page's rows");
        // Without a requested exact total there is no number to misread.
        assert_eq!(Page::default().total_pages(), None);
    }
}

#[cfg(all(test, feature = "codecs"))]
mod serde_tests {
    use super::*;
    use crate::codes::QUERY_OP_VERSION;
    use crate::framing::{decode_named, encode_named};

    fn query() -> Query {
        Query {
            execution_id: QueryExecutionId::from_u128(1),
            target: QueryTarget::operational("orders"),
            deadline_micros: 1_000_000,
            by_key: Vec::new(),
            message_type: None,
            time_range: None,
            filter: None,
            vector: None,
            text: None,
            order: Vec::new(),
            page: QueryPageRequest::default(),
            aggregate: None,
            having: None,
            distinct: false,
            select: Select::default(),
            fork: None,
            raw_sql: None,
            consistency: Consistency::Eventual,
        }
    }

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
    fn given_a_query_when_round_tripped_through_the_envelope_then_should_be_unchanged() {
        let mut query = query();
        query.by_key = vec![KeyMatch::new("customer_id", "abc")];
        query.filter = Some(Filter::pred("status", CmpOp::Eq, "paid"));
        query.order = vec![Sort {
            field: "ts".to_owned(),
            dir: Dir::Desc,
        }];
        query.page.limit = 20;
        let request = QueryEnvelope::new(query);

        let json = serde_json::to_string(&request).expect("the request serializes");
        let back: QueryEnvelope = serde_json::from_str(&json).expect("the request deserializes");
        assert_eq!(back.v, QUERY_OP_VERSION);
        assert_eq!(back.query.target, QueryTarget::operational("orders"));
        assert_eq!(back.query.page.limit, 20);
        assert_eq!(back.query.by_key, vec![KeyMatch::new("customer_id", "abc")]);
        let Some(Filter::Pred(predicate)) = &back.query.filter else {
            panic!("expected a single predicate filter");
        };
        assert_eq!(predicate.value, TypedValue::String("paid".to_owned()));
        assert_eq!(back.query.order[0].dir, Dir::Desc);
    }

    #[test]
    fn given_each_consistency_level_when_round_tripped_then_should_preserve_it_and_skip_eventual() {
        for level in [
            Consistency::Eventual,
            Consistency::ReadYourWrites,
            Consistency::Strong,
        ] {
            let mut query = query();
            query.consistency = level;
            let bytes = encode_named(&QueryEnvelope::new(query)).expect("serializes");
            let back: QueryEnvelope = decode_named(&bytes).expect("deserializes");
            assert_eq!(back.query.consistency, level);
        }
        // The default `Eventual` is omitted on the wire so the pre-consistency
        // contract stays byte-identical.
        let default = query();
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
    fn given_a_vector_query_when_round_tripped_then_should_preserve_the_embedding() {
        let mut query = query();
        query.target = QueryTarget::operational("mem:conv-1");
        query.vector = Some(VectorQuery {
            field: "embedding".to_owned(),
            embedding: vec![0.1, 0.2, 0.3],
            top_k: 5,
        });
        let json = serde_json::to_string(&query).expect("the query serializes");
        let back: Query = serde_json::from_str(&json).expect("the query deserializes");
        let vector = back.vector.expect("the vector survives the round-trip");
        assert_eq!(vector.embedding, vec![0.1, 0.2, 0.3]);
        assert_eq!(vector.top_k, 5);
    }

    #[test]
    fn given_a_typed_reply_when_round_tripped_then_should_preserve_the_values() {
        let reply = QueryReply::Ok(Box::new(QueryResult {
            fields: vec![LogicalField {
                id: 1,
                name: "order_id".to_owned(),
                required: true,
                field_type: crate::schema::LogicalType::String,
                doc: None,
            }],
            rows: vec![Row {
                values: vec![TypedValue::String("123".to_owned())],
                score: Some(0.25),
            }],
            page: Page {
                offset: Some(0),
                limit: 50,
                total: Some(1),
                has_more: false,
                next_cursor: None,
            },
            context: QueryContext {
                execution_id: QueryExecutionId::from_u128(1),
                engine: QueryEngine {
                    name: "datafusion".to_owned(),
                    version: "50.0.0".to_owned(),
                    dialect: Some(SqlDialect::DataFusion),
                },
                resolved_target: ResolvedQueryTarget::Operational {
                    index: "orders".to_owned(),
                    backend_resource_id: BackendResourceId::from_u128(2),
                    backend_generation: 1,
                    runtime_configuration_revision: 1,
                },
                requested_consistency: Consistency::Eventual,
                delivered_consistency: Consistency::Eventual,
                boundary: None,
                checkpoint_revision: None,
                global_state_revision: None,
                truncated: false,
                elapsed_micros: 10,
                scanned_bytes: 128,
                produced_bytes: 32,
                row_count: 1,
            },
        }));
        let bytes = encode_named(&reply).expect("the reply serializes");
        let back: QueryReply = decode_named(&bytes).expect("the reply deserializes");
        let QueryReply::Ok(result) = back else {
            panic!("the reply should decode as Ok");
        };
        assert_eq!(
            result.rows[0].values,
            vec![TypedValue::String("123".to_owned())]
        );
        assert_eq!(result.page.total, Some(1));
        assert!(!result.page.has_more);
    }
}

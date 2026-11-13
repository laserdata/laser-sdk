use crate::agent::wire_id;
use crate::codes::QUERY_OP_VERSION;
use crate::destination::BackendResourceId;
use crate::error::InvalidError;
use crate::limits::{
    MAX_PAGE_SIZE, MAX_QUERY_FIELDS, MAX_QUERY_NAME_BYTES, MAX_QUERY_OFFSET, MAX_QUERY_PARAMETERS,
    MAX_QUERY_PREDICATES, MAX_RAW_SQL_BYTES,
};
use crate::schema::{LogicalField, TypedValue, validate_result_fields};
use crate::validate::Validate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

wire_id!(
    /// Unguessable identity of one query execution, echoed by its reply.
    QueryExecutionId
);

/// The resource against which a query is authorized and resolved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum QueryTarget {
    Operational { index: String },
}

impl QueryTarget {
    pub fn operational(index: impl Into<String>) -> Self {
        Self::Operational {
            index: index.into(),
        }
    }
}

/// SQL grammar accepted by the selected query backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum SqlDialect {
    Postgres,
    MySql,
    Sqlite,
}

/// One exact-match constraint: the indexed `field` must equal `value`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
    // (start, end) in epoch microseconds, both bounds inclusive: the managed backend
    // keeps rows with `start <= ts <= end`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_range: Option<(u64, u64)>,
    // Predicate tree. `None` plus empty sugar is an unfiltered scan. Build
    // trees with `Filter::all`/`any`/`not`/`pred`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorQuery>,
    // Refuse unadvertised text search because an unaware server could silently drop the filter.
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct QueryPageRequest {
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub want_total: bool,
}

impl Default for QueryPageRequest {
    fn default() -> Self {
        Self {
            limit: 50,
            offset: Some(0),
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
        if let Some(offset) = self.page.offset
            && offset > MAX_QUERY_OFFSET
        {
            return Err(InvalidError::new(format!(
                "query offset {offset} exceeds cap {MAX_QUERY_OFFSET}"
            )));
        }
        for key_match in &self.by_key {
            validate_query_name("key-match field", &key_match.field)?;
            key_match.value.validate()?;
        }
        if let Some(message_type) = &self.message_type {
            validate_query_name("message type", message_type)?;
        }
        if let Some((start, end)) = self.time_range
            && start > end
        {
            return Err(InvalidError::new(
                "query time range start must not be after its end, and both bounds are inclusive",
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
        let shapes = [
            self.text.is_some(),
            self.vector.is_some(),
            self.aggregate.is_some(),
            self.distinct,
        ];
        if shapes.into_iter().filter(|set| *set).count() > 1 {
            return Err(InvalidError::new(
                "a query takes at most one of text, vector, aggregate and distinct",
            ));
        }
        if self.text.is_some() && (!self.order.is_empty() || self.page.want_total) {
            return Err(InvalidError::new(
                "a text query ranks by relevance and takes no order or total",
            ));
        }
        if self.vector.is_some()
            && (!self.order.is_empty()
                || self.page.want_total
                || self.page.offset.is_some_and(|offset| offset > 0))
        {
            return Err(InvalidError::new(
                "a vector query returns its nearest rows and takes no order, offset or total",
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
// (Eventual < ReadYourWrites), so a stronger level compares greater and a
// capability check is `want <= served`. A new variant must be appended to keep
// the order meaningful.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Consistency {
    /// Serve from the index as-is, whatever the projector has applied so far.
    /// The default and the cheapest: no wait, best for dashboards and scans
    /// where a little lag is fine.
    #[default]
    Eventual,
    /// Wait until the projector has applied every source partition up to the
    /// head the server read when the query arrived, so a query issued after a
    /// publish was acknowledged sees that write (read-your-writes). Bounded: if
    /// the projector cannot catch up before the query deadline the query
    /// returns [`QueryError::Stale`] instead of downgrading to a stale read.
    /// Backend-gated by `read_your_writes`.
    ReadYourWrites,
}

impl Consistency {
    /// Whether this is the default `Eventual` level (omitted on the wire).
    pub fn is_eventual(&self) -> bool {
        matches!(self, Consistency::Eventual)
    }
}

/// The server-side gate that enforces a [`Consistency`] level the same way on
/// every backend. The client refuses an unadvertised level before sending,
/// but a backend that does advertise `read_your_writes` still has to honor the
/// level, and the rule is fail-not-downgrade: serve only when the projector's
/// `applied` offset for the queried source has reached the `required` offset
/// (the source log head when the query arrived), else return
/// [`QueryError::Stale`] rather than a silently older read. `Eventual` always
/// passes.
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Predicate {
    pub field: String,
    pub op: CmpOp,
    pub value: TypedValue,
}

/// Raw-SQL escape hatch scoped to [`Query::target`]. The server validates both
/// the parsed statement and the resulting scan plan against that target.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct TextQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    pub query: String,
}

/// A nearest-neighbour search: the query embedding and how many rows to return.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct VectorQuery {
    pub field: String,
    pub embedding: Vec<f32>,
    pub top_k: u32,
}

/// A grouped aggregation carrying one or more [`AggCall`]s, so a single query
/// can return several aggregates grouped by the same keys. An optional `window`
/// adds a time-bucket key.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct Window {
    pub field: String,
    pub every_micros: u64,
}

/// Which columns and payload a query returns.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct QueryResult {
    pub fields: Vec<LogicalField>,
    pub rows: Vec<Row>,
    #[serde(default)]
    pub page: Page,
    pub context: QueryContext,
}

/// One typed query row. Values are ordered exactly like [`QueryResult::fields`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
        self.page.validate()
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
            }
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct QueryContext {
    pub execution_id: QueryExecutionId,
    pub engine: QueryEngine,
    pub resolved_target: ResolvedQueryTarget,
    pub requested_consistency: Consistency,
    pub delivered_consistency: Consistency,
    pub truncated: bool,
    pub elapsed_micros: u64,
    pub scanned_bytes: u64,
    pub produced_bytes: u64,
    pub row_count: u64,
    /// How far the store that served the query had applied each source
    /// partition of the index. A reply from another node can sit at another
    /// point of the same log, so a caller compares positions, not wall time.
    /// Empty when the store reports no positions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frontier: Vec<QueryFrontier>,
}

/// One source partition's applied position in the store that served a query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct QueryFrontier {
    pub stream: String,
    pub topic: String,
    pub partition_id: u32,
    /// Creation time of the topic incarnation the rows come from, when the
    /// serving node knows it. Offsets of different incarnations do not
    /// compare.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_created_at_micros: Option<u64>,
    /// The last offset the store applied.
    pub applied_offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct QueryEngine {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dialect: Option<SqlDialect>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResolvedQueryTarget {
    Operational {
        index: String,
        backend_resource_id: BackendResourceId,
        backend_generation: u64,
        runtime_configuration_revision: u64,
    },
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
        Ok(())
    }
}

/// Internal on-wire envelope: a versioned wrapper around `Query`. Workers and
/// clients use it, app code does not.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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

/// A query reply: `Ok(QueryResult)` or `Err(QueryError)`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub enum QueryReply {
    Ok(Box<QueryResult>),
    Err(QueryError),
}

/// Why a query failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub enum QueryError {
    #[error("query not supported: {0}")]
    Unsupported(String),
    /// The request itself is malformed: it failed validation, did not decode,
    /// or is a raw SQL statement the server refuses to run. Fix the request
    /// rather than retrying it.
    #[error("invalid query: {0}")]
    Invalid(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("index not found: {0}")]
    IndexNotFound(String),
    #[error("fork not found: {0}")]
    ForkNotFound(String),
    #[error("writer schema {0} is not registered")]
    SchemaNotFound(u32),
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
    /// ceiling. Page with a smaller limit (or drop the payload request) rather
    /// than retrying unchanged.
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
    #[error("query {execution_id} exceeded its deadline")]
    DeadlineExceeded { execution_id: QueryExecutionId },
    #[error("stale {what} generation: requested {requested}, observed {observed}")]
    StaleGeneration {
        what: String,
        requested: u64,
        observed: u64,
    },
}

/// Stable machine-readable classification for [`QueryError`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum QueryErrorCode {
    Unsupported,
    Invalid,
    Unauthorized,
    IndexNotFound,
    ForkNotFound,
    SchemaNotFound,
    Backend,
    Unavailable,
    TooLarge,
    Version,
    Stale,
    DeadlineExceeded,
    StaleGeneration,
}

impl QueryError {
    pub fn code(&self) -> QueryErrorCode {
        match self {
            Self::Unsupported(_) => QueryErrorCode::Unsupported,
            Self::Invalid(_) => QueryErrorCode::Invalid,
            Self::Unauthorized(_) => QueryErrorCode::Unauthorized,
            Self::IndexNotFound(_) => QueryErrorCode::IndexNotFound,
            Self::ForkNotFound(_) => QueryErrorCode::ForkNotFound,
            Self::SchemaNotFound(_) => QueryErrorCode::SchemaNotFound,
            Self::Backend(_) => QueryErrorCode::Backend,
            Self::Unavailable(_) => QueryErrorCode::Unavailable,
            Self::TooLarge { .. } => QueryErrorCode::TooLarge,
            Self::Version { .. } => QueryErrorCode::Version,
            Self::Stale { .. } => QueryErrorCode::Stale,
            Self::DeadlineExceeded { .. } => QueryErrorCode::DeadlineExceeded,
            Self::StaleGeneration { .. } => QueryErrorCode::StaleGeneration,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::LogicalType;

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
                .check(Consistency::Eventual, "readings")
                .is_ok()
        );
        // A non-Eventual level passes only once caught up.
        assert!(
            ConsistencyGate::new(100, 100)
                .check(Consistency::ReadYourWrites, "readings")
                .is_ok()
        );
        let stale = ConsistencyGate::new(41, 57)
            .check(Consistency::ReadYourWrites, "readings")
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
    fn given_a_query_with_more_than_one_shape_when_validated_then_should_refuse_it() {
        let vector = VectorQuery {
            field: "embedding".to_owned(),
            embedding: vec![0.1, 0.2],
            top_k: 10,
        };
        let text = TextQuery {
            field: None,
            query: "incident".to_owned(),
        };
        let aggregate = Aggregate {
            group_by: Vec::new(),
            funcs: vec![AggCall {
                func: AggFunc::Count,
                field: None,
                arg: None,
                alias: "n".to_owned(),
            }],
            window: None,
        };
        let base = || Query::operational(QueryExecutionId::from_u128(1), "docs", 1_000_000);
        let shapes: [(&str, Query); 4] = [
            (
                "vector and aggregate",
                Query {
                    vector: Some(vector.clone()),
                    aggregate: Some(aggregate.clone()),
                    ..base()
                },
            ),
            (
                "text and vector",
                Query {
                    text: Some(text.clone()),
                    vector: Some(vector.clone()),
                    ..base()
                },
            ),
            (
                "text and distinct",
                Query {
                    text: Some(text),
                    distinct: true,
                    ..base()
                },
            ),
            (
                "aggregate and distinct",
                Query {
                    aggregate: Some(aggregate),
                    distinct: true,
                    ..base()
                },
            ),
        ];
        for (label, query) in shapes {
            let error = query
                .validate()
                .expect_err("a query with two shapes must be refused");
            assert!(error.0.contains("one of"), "{label}: {error:?}");
        }
        assert!(
            Query {
                vector: Some(vector),
                ..base()
            }
            .validate()
            .is_ok()
        );
    }

    #[test]
    fn given_a_ranked_query_with_paging_it_cannot_honour_when_validated_then_should_refuse_it() {
        let base = || Query::operational(QueryExecutionId::from_u128(1), "docs", 1_000_000);
        let text = || {
            Some(TextQuery {
                field: None,
                query: "incident".to_owned(),
            })
        };
        let vector = || {
            Some(VectorQuery {
                field: "embedding".to_owned(),
                embedding: vec![0.1, 0.2],
                top_k: 10,
            })
        };
        let order = || {
            vec![Sort {
                field: "ts".to_owned(),
                dir: Dir::Desc,
            }]
        };
        let total = QueryPageRequest {
            want_total: true,
            ..QueryPageRequest::default()
        };
        let refused: [(&str, Query); 5] = [
            (
                "text with an order",
                Query {
                    text: text(),
                    order: order(),
                    ..base()
                },
            ),
            (
                "text with a total",
                Query {
                    text: text(),
                    page: total.clone(),
                    ..base()
                },
            ),
            (
                "vector with an order",
                Query {
                    vector: vector(),
                    order: order(),
                    ..base()
                },
            ),
            (
                "vector with a total",
                Query {
                    vector: vector(),
                    page: total,
                    ..base()
                },
            ),
            (
                "vector with an offset",
                Query {
                    vector: vector(),
                    page: QueryPageRequest {
                        offset: Some(10),
                        ..QueryPageRequest::default()
                    },
                    ..base()
                },
            ),
        ];
        for (label, query) in refused {
            let error = query
                .validate()
                .expect_err("a ranked query cannot honour this paging");
            assert!(error.0.contains("takes no"), "{label}: {error:?}");
        }
        let next_text_page = Query {
            text: text(),
            page: QueryPageRequest {
                offset: Some(50),
                ..QueryPageRequest::default()
            },
            ..base()
        };
        assert!(next_text_page.validate().is_ok());
    }

    #[test]
    fn given_a_page_when_computing_total_pages_then_should_divide_by_limit() {
        let page = Page {
            offset: Some(0),
            limit: 3,
            total: Some(10),
            has_more: true,
        };
        assert_eq!(page.total_pages(), Some(4));
        assert_eq!(page.at_least(3), Some(3), "offset 0 plus this page's rows");
        // Without a requested exact total there is no number to misread.
        assert_eq!(Page::default().total_pages(), None);
    }

    #[test]
    fn given_query_bounds_when_validated_then_should_accept_each_cap_and_refuse_one_past_it() {
        let paged = |limit: u32, offset: u64| Query {
            page: QueryPageRequest {
                limit,
                offset: Some(offset),
                want_total: false,
            },
            ..query()
        };
        let selecting = |fields: usize| Query {
            select: Select {
                fields: vec!["host".to_owned(); fields],
                payload: false,
            },
            ..query()
        };
        let ranged = |start: u64, end: u64| Query {
            time_range: Some((start, end)),
            ..query()
        };
        for accepted in [
            paged(MAX_PAGE_SIZE as u32, MAX_QUERY_OFFSET),
            selecting(MAX_QUERY_FIELDS),
            ranged(1, 2),
            ranged(2, 2),
        ] {
            accepted.validate().expect("a query at its caps");
        }
        let refused = [
            (
                "a zero execution id",
                Query {
                    execution_id: QueryExecutionId::from_u128(0),
                    ..query()
                },
            ),
            (
                "a zero deadline",
                Query {
                    deadline_micros: 0,
                    ..query()
                },
            ),
            ("a zero page limit", paged(0, 0)),
            (
                "a page limit past the cap",
                paged(MAX_PAGE_SIZE as u32 + 1, 0),
            ),
            ("an offset past the cap", paged(1, MAX_QUERY_OFFSET + 1)),
            (
                "selected fields past the cap",
                selecting(MAX_QUERY_FIELDS + 1),
            ),
            ("a range that ends before it starts", ranged(3, 2)),
        ];
        for (case, rejected) in refused {
            assert!(rejected.validate().is_err(), "{case} must be refused");
        }
    }

    #[test]
    fn given_aggregate_clauses_when_combined_then_should_refuse_lone_having_and_row_selection() {
        let aggregate = Aggregate {
            group_by: Vec::new(),
            funcs: vec![AggCall {
                func: AggFunc::Count,
                field: None,
                arg: None,
                alias: "rows".to_owned(),
            }],
            window: None,
        };
        Query {
            aggregate: Some(aggregate.clone()),
            ..query()
        }
        .validate()
        .expect("an aggregate alone");
        let refused = [
            (
                "a having filter without an aggregate",
                Query {
                    having: Some(Filter::pred("rows", CmpOp::Gt, 1i64)),
                    ..query()
                },
            ),
            (
                "an aggregate with selected fields",
                Query {
                    aggregate: Some(aggregate),
                    select: Select {
                        fields: vec!["host".to_owned()],
                        payload: false,
                    },
                    ..query()
                },
            ),
        ];
        for (case, rejected) in refused {
            assert!(rejected.validate().is_err(), "{case} must be refused");
        }
    }

    #[test]
    fn given_raw_sql_with_any_structured_clause_when_validated_then_should_refuse_it() {
        let raw_sql = Query {
            raw_sql: Some(RawSql {
                dialect: SqlDialect::Postgres,
                sql: "SELECT 1".to_owned(),
                params: Vec::new(),
            }),
            ..query()
        };
        raw_sql.validate().expect("raw SQL alone");
        let aggregate = Aggregate {
            group_by: Vec::new(),
            funcs: vec![AggCall {
                func: AggFunc::Count,
                field: None,
                arg: None,
                alias: "rows".to_owned(),
            }],
            window: None,
        };
        // One clause per row, so every operand of the refusal is exercised alone.
        let refused = [
            (
                "an exact match",
                Query {
                    by_key: vec![KeyMatch::new("host", "a")],
                    ..raw_sql.clone()
                },
            ),
            (
                "a message type",
                Query {
                    message_type: Some("reading".to_owned()),
                    ..raw_sql.clone()
                },
            ),
            (
                "a time range",
                Query {
                    time_range: Some((1, 2)),
                    ..raw_sql.clone()
                },
            ),
            (
                "a filter",
                Query {
                    filter: Some(Filter::pred("host", CmpOp::Eq, "a")),
                    ..raw_sql.clone()
                },
            ),
            (
                "a vector search",
                Query {
                    vector: Some(VectorQuery {
                        field: "embedding".to_owned(),
                        embedding: vec![0.5],
                        top_k: 1,
                    }),
                    ..raw_sql.clone()
                },
            ),
            (
                "a text search",
                Query {
                    text: Some(TextQuery {
                        field: None,
                        query: "incident".to_owned(),
                    }),
                    ..raw_sql.clone()
                },
            ),
            (
                "a sort",
                Query {
                    order: vec![Sort {
                        field: "ts".to_owned(),
                        dir: Dir::Desc,
                    }],
                    ..raw_sql.clone()
                },
            ),
            (
                "an aggregate",
                Query {
                    aggregate: Some(aggregate.clone()),
                    ..raw_sql.clone()
                },
            ),
            (
                "a having filter",
                Query {
                    aggregate: Some(aggregate),
                    having: Some(Filter::pred("rows", CmpOp::Gt, 1i64)),
                    ..raw_sql.clone()
                },
            ),
            (
                "distinct rows",
                Query {
                    distinct: true,
                    ..raw_sql.clone()
                },
            ),
            (
                "selected fields",
                Query {
                    select: Select {
                        fields: vec!["host".to_owned()],
                        payload: false,
                    },
                    ..raw_sql.clone()
                },
            ),
            (
                "the payload",
                Query {
                    select: Select {
                        fields: Vec::new(),
                        payload: true,
                    },
                    ..raw_sql
                },
            ),
        ];
        for (clause, rejected) in refused {
            assert!(
                rejected.validate().is_err(),
                "raw SQL with {clause} must be refused"
            );
        }
    }

    #[test]
    fn given_a_query_envelope_when_validated_then_should_require_the_current_version() {
        let envelope = QueryEnvelope::new(query());
        envelope.validate().expect("the current version");
        let newer = QueryEnvelope {
            v: QUERY_OP_VERSION + 1,
            ..envelope
        };
        assert!(newer.validate().is_err());
    }

    #[test]
    fn given_a_query_name_when_validated_then_should_refuse_empty_oversized_or_padded_text() {
        let indexed = |index: &str| Query {
            target: QueryTarget::operational(index),
            ..query()
        };
        indexed(&"i".repeat(MAX_QUERY_NAME_BYTES))
            .validate()
            .expect("a name at the cap");
        for index in [
            String::new(),
            "i".repeat(MAX_QUERY_NAME_BYTES + 1),
            " readings".to_owned(),
        ] {
            assert!(
                indexed(&index).validate().is_err(),
                "{index:?} must be refused"
            );
        }
    }

    #[test]
    fn given_in_and_prefix_filters_when_validated_then_should_need_a_list_and_a_string() {
        let filtered = |filter: Filter| Query {
            filter: Some(filter),
            ..query()
        };
        filtered(Filter::all([
            Filter::pred("status", CmpOp::Eq, 5i64),
            Filter::pred("host", CmpOp::In, vec!["a", "b"]),
            Filter::pred("host", CmpOp::Prefix, "node-"),
        ]))
        .validate()
        .expect("well-typed operators");
        let refused = [
            (
                "an empty IN list",
                Filter::pred("host", CmpOp::In, Vec::<&str>::new()),
            ),
            (
                "a numeric prefix",
                Filter::pred("host", CmpOp::Prefix, 5i64),
            ),
        ];
        for (case, filter) in refused {
            assert!(
                filtered(filter).validate().is_err(),
                "{case} must be refused"
            );
        }
    }

    #[test]
    fn given_a_filter_at_its_node_and_depth_caps_when_validated_then_should_refuse_one_more() {
        let filtered = |filter: Filter| Query {
            filter: Some(filter),
            ..query()
        };
        let leaf = Filter::pred("host", CmpOp::Eq, "a");
        let wide = |leaves: usize| Filter::All(vec![leaf.clone(); leaves]);
        let nested = |depth: usize, wrap: fn(Filter) -> Filter| {
            (1..depth).fold(leaf.clone(), |filter, _| wrap(filter))
        };
        let conjunction: fn(Filter) -> Filter = |filter| Filter::All(vec![filter]);
        // The conjunction is a node too, so it plus 4095 leaves reach the node cap.
        for accepted in [
            wide(MAX_QUERY_PREDICATES - 1),
            nested(64, Filter::negate),
            nested(64, conjunction),
        ] {
            filtered(accepted).validate().expect("a filter at its caps");
        }
        for refused in [
            wide(MAX_QUERY_PREDICATES),
            nested(65, Filter::negate),
            nested(65, conjunction),
        ] {
            assert!(filtered(refused).validate().is_err(), "one past a cap");
        }
    }

    #[test]
    fn given_aggregate_calls_when_validated_then_should_match_each_function_to_its_inputs() {
        let aggregate = |func: AggFunc, field: Option<&str>, arg: Option<f64>| Aggregate {
            group_by: Vec::new(),
            funcs: vec![AggCall {
                func,
                field: field.map(str::to_owned),
                arg,
                alias: "value".to_owned(),
            }],
            window: None,
        };
        for accepted in [
            aggregate(AggFunc::Count, None, None),
            aggregate(AggFunc::Percentile, Some("latency"), Some(0.95)),
            aggregate(AggFunc::Sum, Some("bytes"), None),
        ] {
            accepted.validate().expect("a well-formed call");
        }
        let refused = [
            (
                "a count with an argument",
                aggregate(AggFunc::Count, None, Some(0.5)),
            ),
            (
                "a percentile past one",
                aggregate(AggFunc::Percentile, Some("latency"), Some(1.5)),
            ),
            (
                "a percentile without a fraction",
                aggregate(AggFunc::Percentile, Some("latency"), None),
            ),
            ("a sum without a field", aggregate(AggFunc::Sum, None, None)),
        ];
        for (case, rejected) in refused {
            assert!(rejected.validate().is_err(), "{case} must be refused");
        }
    }

    #[test]
    fn given_an_aggregate_when_validated_then_should_bound_its_size_and_need_unique_aliases() {
        let counts = |calls: usize| {
            (0..calls)
                .map(|index| AggCall {
                    func: AggFunc::Count,
                    field: None,
                    arg: None,
                    alias: format!("count_{index}"),
                })
                .collect::<Vec<_>>()
        };
        let shaped = |funcs: Vec<AggCall>, groups: usize, every_micros: u64| Aggregate {
            group_by: vec!["host".to_owned(); groups],
            funcs,
            window: Some(Window {
                field: "ts".to_owned(),
                every_micros,
            }),
        };
        shaped(counts(MAX_QUERY_PARAMETERS), MAX_QUERY_FIELDS, 60_000_000)
            .validate()
            .expect("an aggregate at its caps");
        let refused = [
            ("no calls", shaped(Vec::new(), 0, 60_000_000)),
            (
                "calls past the cap",
                shaped(counts(MAX_QUERY_PARAMETERS + 1), 0, 60_000_000),
            ),
            (
                "groups past the cap",
                shaped(counts(1), MAX_QUERY_FIELDS + 1, 60_000_000),
            ),
            (
                "a repeated alias",
                shaped([counts(1), counts(1)].concat(), 0, 60_000_000),
            ),
            ("a zero window", shaped(counts(1), 0, 0)),
        ];
        for (case, rejected) in refused {
            assert!(rejected.validate().is_err(), "{case} must be refused");
        }
    }

    #[test]
    fn given_raw_sql_when_validated_then_should_bound_its_text_and_parameters() {
        let raw = |sql: String, params: usize| RawSql {
            dialect: SqlDialect::Postgres,
            sql,
            params: vec![TypedValue::Long(1); params],
        };
        for accepted in [
            raw("x".repeat(MAX_RAW_SQL_BYTES), MAX_QUERY_PARAMETERS),
            raw("SELECT\n\t1\r".to_owned(), 0),
        ] {
            accepted.validate().expect("raw SQL within its caps");
        }
        let refused = [
            ("blank text", raw("  ".to_owned(), 0)),
            (
                "text past the cap",
                raw("x".repeat(MAX_RAW_SQL_BYTES + 1), 0),
            ),
            ("a control character", raw("SELECT 1\u{7}".to_owned(), 0)),
            (
                "parameters past the cap",
                raw("SELECT 1".to_owned(), MAX_QUERY_PARAMETERS + 1),
            ),
        ];
        for (case, rejected) in refused {
            assert!(rejected.validate().is_err(), "{case} must be refused");
        }
    }

    #[test]
    fn given_distinct_and_want_total_when_round_tripped_then_should_omit_them_only_when_false() {
        let flagged = Query {
            distinct: true,
            page: QueryPageRequest {
                want_total: true,
                ..QueryPageRequest::default()
            },
            ..query()
        };
        let json = serde_json::to_value(&flagged).expect("encode a flagged query");
        let decoded: Query = serde_json::from_value(json).expect("decode a flagged query");
        assert_eq!(decoded, flagged);
        let plain = serde_json::to_value(query()).expect("encode a plain query");
        assert_eq!(
            (plain.get("distinct"), plain["page"].get("want_total")),
            (None, None)
        );
    }

    #[test]
    fn given_typed_input_when_parsed_then_should_narrow_keywords_and_signed_decimals() {
        assert_eq!(
            ["null", "true", "false", "1.5", "-1.5", "+1.5"].map(Value::from_input),
            [
                Value::Null,
                Value::Bool(true),
                Value::Bool(false),
                Value::Float(1.5),
                Value::Float(-1.5),
                Value::Float(1.5),
            ]
        );
    }

    #[test]
    fn given_a_list_value_when_displayed_then_should_separate_its_bare_elements() {
        let list = Value::List(vec![Value::Int(1), Value::from("a"), Value::Null]);
        assert_eq!(list.to_string(), "[1, a, null]");
    }

    #[test]
    fn given_a_query_result_when_validated_then_should_match_rows_to_the_schema_and_the_count() {
        let valid = result();
        valid.validate().expect("a well-formed result");
        let refused = [
            (
                "a row missing values",
                QueryResult {
                    rows: vec![Row {
                        values: Vec::new(),
                        score: None,
                    }],
                    ..result()
                },
            ),
            (
                "a score that is not a number",
                QueryResult {
                    rows: vec![Row {
                        score: Some(f32::NAN),
                        ..valid.rows[0].clone()
                    }],
                    ..result()
                },
            ),
            (
                "a row count that disagrees with the rows",
                QueryResult {
                    context: QueryContext {
                        row_count: 2,
                        ..context()
                    },
                    ..result()
                },
            ),
        ];
        for (case, rejected) in refused {
            assert!(rejected.validate().is_err(), "{case} must be refused");
        }
    }

    #[test]
    fn given_a_query_context_when_validated_then_should_need_evidence_and_enough_consistency() {
        for accepted in [
            context(),
            QueryContext {
                delivered_consistency: Consistency::ReadYourWrites,
                ..context()
            },
        ] {
            accepted
                .validate()
                .expect("a delivery at least as strong as requested");
        }
        let evidence = |resource: u128, generation: u64, revision: u64| QueryContext {
            resolved_target: ResolvedQueryTarget::Operational {
                index: "readings".to_owned(),
                backend_resource_id: BackendResourceId::from_u128(resource),
                backend_generation: generation,
                runtime_configuration_revision: revision,
            },
            ..context()
        };
        let refused = [
            (
                "a zero execution id",
                QueryContext {
                    execution_id: QueryExecutionId::from_u128(0),
                    ..context()
                },
            ),
            (
                "a weaker delivery than requested",
                QueryContext {
                    requested_consistency: Consistency::ReadYourWrites,
                    ..context()
                },
            ),
            ("a zero backend resource id", evidence(0, 1, 1)),
            ("a zero backend generation", evidence(2, 0, 1)),
            ("a zero configuration revision", evidence(2, 1, 0)),
        ];
        for (case, rejected) in refused {
            assert!(rejected.validate().is_err(), "{case} must be refused");
        }
    }

    #[test]
    fn given_a_result_row_when_read_by_field_name_then_should_return_its_typed_values() {
        let reply = result();
        let row = &reply.rows[0];
        assert_eq!(
            (
                reply.field_index("delta"),
                reply.value(row, "count"),
                reply.value_text(row, "reading_id"),
                reply.value_u64(row, "count"),
                reply.value_i64(row, "delta"),
            ),
            (
                Some(2),
                Some(&TypedValue::Long(7)),
                Some("r-1".to_owned()),
                Some(7),
                Some(-3),
            )
        );
    }

    #[test]
    fn given_a_page_limit_when_validated_then_should_accept_one_to_the_cap() {
        let page = |limit: u32| Page {
            offset: Some(0),
            limit,
            total: None,
            has_more: false,
        };
        page(MAX_PAGE_SIZE as u32)
            .validate()
            .expect("a limit at the cap");
        for limit in [0, MAX_PAGE_SIZE as u32 + 1] {
            assert!(
                page(limit).validate().is_err(),
                "limit {limit} must be refused"
            );
        }
    }

    #[test]
    fn given_a_total_without_a_page_size_when_counting_pages_then_should_have_none() {
        let page = Page {
            offset: Some(0),
            limit: 0,
            total: Some(10),
            has_more: false,
        };
        assert_eq!(page.total_pages(), None);
    }

    fn query() -> Query {
        Query::operational(QueryExecutionId::from_u128(1), "readings", 1_000_000)
    }

    fn result() -> QueryResult {
        let field = |id: u32, name: &str, field_type: LogicalType| LogicalField {
            id,
            name: name.to_owned(),
            required: true,
            field_type,
            doc: None,
        };
        QueryResult {
            fields: vec![
                field(1, "reading_id", LogicalType::String),
                field(2, "count", LogicalType::Long),
                field(3, "delta", LogicalType::Long),
            ],
            rows: vec![Row {
                values: vec![
                    TypedValue::from("r-1"),
                    TypedValue::Long(7),
                    TypedValue::Long(-3),
                ],
                score: Some(0.25),
            }],
            page: Page {
                offset: Some(0),
                limit: 50,
                total: None,
                has_more: false,
            },
            context: context(),
        }
    }

    fn context() -> QueryContext {
        QueryContext {
            execution_id: QueryExecutionId::from_u128(1),
            engine: QueryEngine {
                name: "postgres".to_owned(),
                version: "17.0".to_owned(),
                dialect: Some(SqlDialect::Postgres),
            },
            resolved_target: ResolvedQueryTarget::Operational {
                index: "readings".to_owned(),
                backend_resource_id: BackendResourceId::from_u128(2),
                backend_generation: 1,
                runtime_configuration_revision: 1,
            },
            requested_consistency: Consistency::Eventual,
            delivered_consistency: Consistency::Eventual,
            truncated: false,
            elapsed_micros: 10,
            scanned_bytes: 128,
            produced_bytes: 32,
            row_count: 1,
            frontier: Vec::new(),
        }
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
            target: QueryTarget::operational("readings"),
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
        query.by_key = vec![KeyMatch::new("host_id", "abc")];
        query.filter = Some(Filter::pred("status", CmpOp::Eq, "degraded"));
        query.order = vec![Sort {
            field: "ts".to_owned(),
            dir: Dir::Desc,
        }];
        query.page.limit = 20;
        let request = QueryEnvelope::new(query);

        let json = serde_json::to_string(&request).expect("the request serializes");
        let back: QueryEnvelope = serde_json::from_str(&json).expect("the request deserializes");
        assert_eq!(back.v, QUERY_OP_VERSION);
        assert_eq!(back.query.target, QueryTarget::operational("readings"));
        assert_eq!(back.query.page.limit, 20);
        assert_eq!(back.query.by_key, vec![KeyMatch::new("host_id", "abc")]);
        let Some(Filter::Pred(predicate)) = &back.query.filter else {
            panic!("expected a single predicate filter");
        };
        assert_eq!(predicate.value, TypedValue::String("degraded".to_owned()));
        assert_eq!(back.query.order[0].dir, Dir::Desc);
    }

    #[test]
    fn given_each_consistency_level_when_round_tripped_then_should_preserve_it_and_skip_eventual() {
        for level in [Consistency::Eventual, Consistency::ReadYourWrites] {
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
            what: "readings".to_owned(),
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
        assert_eq!((what.as_str(), applied, required), ("readings", 41, 57));
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
                name: "reading_id".to_owned(),
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
            },
            context: QueryContext {
                execution_id: QueryExecutionId::from_u128(1),
                engine: QueryEngine {
                    name: "postgres".to_owned(),
                    version: "17.0".to_owned(),
                    dialect: Some(SqlDialect::Postgres),
                },
                resolved_target: ResolvedQueryTarget::Operational {
                    index: "readings".to_owned(),
                    backend_resource_id: BackendResourceId::from_u128(2),
                    backend_generation: 1,
                    runtime_configuration_revision: 1,
                },
                requested_consistency: Consistency::Eventual,
                delivered_consistency: Consistency::Eventual,
                truncated: false,
                elapsed_micros: 10,
                scanned_bytes: 128,
                produced_bytes: 32,
                row_count: 1,
                frontier: Vec::new(),
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

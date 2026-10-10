use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{codec_decode, json_to_py, py_to_de, py_to_typed_value, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::LaserError;
use laser_sdk::laser::Laser;
use laser_sdk::query::{
    AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, Filter, KeyMatch, Query,
    QueryExecutionId, QueryPageRequest, QueryResult, QueryTarget, RawSql, ResultCode, Row, Select,
    SnapshotSelector, Sort, SqlDialect, TextQuery, TypedValue, VectorQuery, Window,
};
use laser_sdk::stream::{Decoder, Json};
use laser_sdk::types::MintUlid;
use laser_sdk::wire::error::DecodeError;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;

fn parse_consistency(level: &str) -> PyResult<Consistency> {
    match level {
        "eventual" => Ok(Consistency::Eventual),
        "read_your_writes" => Ok(Consistency::ReadYourWrites),
        "strong" => Ok(Consistency::Strong),
        other => Err(InvalidError::new_err(format!(
            "consistency must be 'eventual', 'read_your_writes', or 'strong', got '{other}'"
        ))),
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Start a query over the materialized `index`. Chain filters / sorts /
    /// aggregates, then `await` a terminal (`fetch`, `fetch_all`, `fetch_typed`,
    /// `fetch_one`). Query is a managed feature: against Apache Iggy it raises
    /// `UnsupportedError`.
    fn query(&self, index: String) -> PyQuery {
        let index = self.inner.resource_name(&index);
        PyQuery::new(self.inner.clone(), QueryTarget::operational(index))
    }

    /// Start a query against an explicit operational or lakehouse target dict.
    /// An operational index is scoped like `query(index)`.
    fn query_target(&self, target: &Bound<'_, PyAny>) -> PyResult<PyQuery> {
        let target = match py_to_de(target)? {
            QueryTarget::Operational { index } => {
                QueryTarget::operational(self.inner.resource_name(&index))
            }
            other => other,
        };
        Ok(PyQuery::new(self.inner.clone(), target))
    }

    /// Start a query against one materialization destination generation.
    fn query_lakehouse(
        &self,
        destination_id: String,
        destination_generation: u64,
    ) -> PyResult<PyQuery> {
        let destination_id = laser_sdk::wire::destination::DestinationId::from_str(&destination_id)
            .map_err(|error| InvalidError::new_err(error.to_string()))?;
        Ok(PyQuery::new(
            self.inner.clone(),
            QueryTarget::Lakehouse {
                destination_id,
                destination_generation,
                snapshot: None,
            },
        ))
    }
}

/// One typed query row. Values are positionally aligned with `QueryResult.fields`.
#[gen_stub_pyclass]
#[pyclass(name = "Row", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyRow {
    inner: Row,
}

impl From<Row> for PyRow {
    fn from(row: Row) -> Self {
        Self { inner: row }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRow {
    /// Tagged values in result-field order. Binary, decimal, UUID, and temporal values stay typed.
    #[getter]
    fn values(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.values)
    }

    /// Backend-native vector distance or lexical rank.
    #[getter]
    fn score(&self) -> Option<f32> {
        self.inner.score
    }

    fn __repr__(&self) -> String {
        format!(
            "Row(values={} fields, score={:?})",
            self.inner.values.len(),
            self.inner.score
        )
    }
}

/// A page of query rows with its logical schema, page metadata, and result
/// evidence.
#[gen_stub_pyclass]
#[pyclass(name = "QueryResult", frozen)]
pub struct PyQueryResult {
    inner: QueryResult,
}

impl From<QueryResult> for PyQueryResult {
    fn from(inner: QueryResult) -> Self {
        Self { inner }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQueryResult {
    /// Ordered logical field metadata for every row.
    #[getter]
    fn fields(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.fields)
    }

    /// The rows of this page, each positionally aligned with `fields`.
    #[getter]
    fn rows(&self) -> Vec<PyRow> {
        self.inner.rows.iter().cloned().map(PyRow::from).collect()
    }

    /// Paging metadata: offset, limit, optional exact total, and the cursor
    /// for the next page.
    #[getter]
    fn page(&self) -> PyPage {
        PyPage {
            inner: self.inner.page.clone(),
        }
    }

    /// Engine, resolved target, consistency, checkpoint, and resource evidence.
    #[getter]
    fn context(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.context)
    }

    /// The position of the logical field `name` in every row, or `None`.
    fn field_index(&self, name: &str) -> Option<usize> {
        self.inner.field_index(name)
    }

    /// Read one tagged value by logical field name.
    fn value(&self, py: Python<'_>, row: &PyRow, field: &str) -> PyResult<Py<PyAny>> {
        match self.inner.value(&row.inner, field) {
            Some(value) => ser_to_py(py, value),
            None => Ok(py.None()),
        }
    }

    /// Read one value by logical field name in its stable diagnostic form.
    fn value_text(&self, row: &PyRow, field: &str) -> Option<String> {
        self.inner.value_text(&row.inner, field)
    }

    /// Read one integer value by logical field name as an unsigned integer,
    /// or `None` when it is absent, negative, or not an integer.
    fn value_u64(&self, row: &PyRow, field: &str) -> Option<u64> {
        self.inner.value_u64(&row.inner, field)
    }

    /// Read one integer value by logical field name as a signed integer, or
    /// `None` when it is absent, out of range, or not an integer.
    fn value_i64(&self, row: &PyRow, field: &str) -> Option<i64> {
        self.inner.value_i64(&row.inner, field)
    }

    fn __len__(&self) -> usize {
        self.inner.rows.len()
    }
}

/// Pagination info for a query result. `has_more` is exact and free. `total`
/// is present only when the query asked for it with `with_total()`.
#[gen_stub_pyclass]
#[pyclass(name = "Page", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyPage {
    inner: laser_sdk::query::Page,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyPage {
    /// The offset this page started at, absent for a cursor page.
    #[getter]
    fn offset(&self) -> Option<u64> {
        self.inner.offset
    }

    /// The effective page size after the server applied the page cap.
    #[getter]
    fn limit(&self) -> u32 {
        self.inner.limit
    }

    /// The exact match count, present only when the query used `with_total()`.
    #[getter]
    fn total(&self) -> Option<u64> {
        self.inner.total
    }

    /// Whether rows beyond this page exist.
    #[getter]
    fn has_more(&self) -> bool {
        self.inner.has_more
    }

    /// The opaque cursor for the next page, present exactly when `has_more`.
    #[getter]
    fn next_cursor(&self) -> Option<String> {
        self.inner.next_cursor.clone()
    }

    /// Rows known to exist so far, one past this page's last row, or `None`
    /// for a cursor page.
    fn at_least(&self, rows_on_page: usize) -> Option<u64> {
        self.inner.at_least(rows_on_page)
    }

    /// Pages implied by the exact total, or `None` without `with_total()`.
    fn total_pages(&self) -> Option<u64> {
        self.inner.total_pages()
    }

    fn __repr__(&self) -> String {
        format!(
            "Page(offset={:?}, limit={}, total={:?}, has_more={})",
            self.inner.offset, self.inner.limit, self.inner.total, self.inner.has_more
        )
    }
}

/// Fluent query builder over a materialized index. Mutates an owned `Query` and
/// executes it through the managed query command at a terminal.
#[gen_stub_pyclass]
#[pyclass(name = "QueryRequest")]
pub struct PyQuery {
    laser: Laser,
    query: Query,
    max_rows: Option<usize>,
}

impl PyQuery {
    fn new(laser: Laser, target: QueryTarget) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();
        let deadline_micros = u64::try_from(now)
            .unwrap_or(u64::MAX)
            .saturating_add(30_000_000);
        let query = Query::builder()
            .execution_id(laser_sdk::query::QueryExecutionId::mint())
            .target(target)
            .deadline_micros(deadline_micros)
            .build();
        Self {
            laser,
            query,
            max_rows: None,
        }
    }

    fn and_filter(&mut self, filter: Filter) {
        self.query.filter = Some(match self.query.filter.take() {
            None => filter,
            Some(Filter::All(mut existing)) => {
                existing.push(filter);
                Filter::All(existing)
            }
            Some(other) => Filter::All(vec![other, filter]),
        });
    }

    fn push_agg(&mut self, call: AggCall) {
        match self.query.aggregate.as_mut() {
            Some(aggregate) => aggregate.funcs.push(call),
            None => {
                self.query.aggregate = Some(Aggregate {
                    group_by: Vec::new(),
                    funcs: vec![call],
                    window: None,
                });
            }
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQuery {
    /// Exact-match on an indexed field (point lookup).
    fn where_eq<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.query
            .by_key
            .push(KeyMatch::new(field, py_to_typed_value(value)?));
        Ok(slf)
    }

    /// Narrow to the rows a single conversation produced (the conversation
    /// lens): an exact-match on the auto-projected `conversation_id` field a
    /// deployment materializes from every message's `gen_ai.conversation.id`
    /// header. Works on any projection, no producer-side index header needed.
    fn conversation<'py>(
        mut slf: PyRefMut<'py, Self>,
        conversation: String,
    ) -> PyRefMut<'py, Self> {
        slf.query.by_key.push(KeyMatch::new(
            laser_sdk::wire::headers::CONVERSATION_FIELD,
            conversation,
        ));
        slf
    }

    /// Resolve against a fork's copy-on-write view instead of the trunk.
    fn fork<'py>(mut slf: PyRefMut<'py, Self>, fork_id: String) -> PyRefMut<'py, Self> {
        slf.query.fork = Some(slf.laser.resource_name(&fork_id));
        slf
    }

    /// AND `filter` (a `Filter`) into the query's predicate tree. The
    /// `filter_*` helpers route through the same conjunction. Build `any` and
    /// `negate` subtrees with `Filter` and pass them here.
    fn filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        filter: PyRef<'_, PyQueryFilter>,
    ) -> PyRefMut<'py, Self> {
        let filter = filter.inner.clone();
        slf.and_filter(filter);
        slf
    }

    /// Keep only aggregate groups matching `filter`. Predicate fields reference
    /// an aggregate alias (for example `count`) or a group key, not raw row
    /// fields.
    fn having<'py>(
        mut slf: PyRefMut<'py, Self>,
        filter: PyRef<'_, PyQueryFilter>,
    ) -> PyRefMut<'py, Self> {
        slf.query.having = Some(filter.inner.clone());
        slf
    }

    /// Add an aggregate with an explicit output `alias`, to return several
    /// aggregates of the same kind or to name the output column. `func` is
    /// `count`, `sum`, `avg`, `min`, `max`, `count_distinct`, `stddev`, or
    /// `percentile` (which needs `fraction`).
    #[pyo3(signature = (func, alias, *, field=None, fraction=None))]
    fn agg_as<'py>(
        mut slf: PyRefMut<'py, Self>,
        func: &str,
        alias: String,
        field: Option<String>,
        fraction: Option<f64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let func = parse_agg_func(func)?;
        slf.push_agg(AggCall {
            func,
            field,
            arg: fraction,
            alias,
        });
        Ok(slf)
    }

    /// Cap the total rows `rows()` and `rows_typed()` may return. Explicit by
    /// design: a paged walk with no ceiling is an unbounded read.
    fn max_rows(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.max_rows = Some(n);
        slf
    }

    /// This query's execution id, the identity `status` and `cancel` use.
    #[getter]
    fn execution_id(&self) -> String {
        self.query.execution_id.to_string()
    }

    /// Set the execution deadline `timeout_ms` milliseconds from now
    /// (default 30,000).
    fn deadline(mut slf: PyRefMut<'_, Self>, timeout_ms: f64) -> PyResult<PyRefMut<'_, Self>> {
        let wait = crate::convert::duration_ms(timeout_ms, "timeout_ms")?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();
        slf.query.deadline_micros = u64::try_from(now)
            .unwrap_or(u64::MAX)
            .saturating_add(u64::try_from(wait.as_micros()).unwrap_or(u64::MAX));
        Ok(slf)
    }

    /// Replace the absolute execution deadline in epoch microseconds.
    fn deadline_micros(mut slf: PyRefMut<'_, Self>, deadline_micros: u64) -> PyRefMut<'_, Self> {
        slf.query.deadline_micros = deadline_micros;
        slf
    }

    /// Select one exact Iceberg snapshot for a lakehouse target.
    fn at_snapshot(mut slf: PyRefMut<'_, Self>, snapshot_id: i64) -> PyResult<PyRefMut<'_, Self>> {
        let QueryTarget::Lakehouse { snapshot, .. } = &mut slf.query.target else {
            return Err(InvalidError::new_err(
                "snapshot selection requires a lakehouse query target",
            ));
        };
        *snapshot = Some(SnapshotSelector::SnapshotId(snapshot_id));
        Ok(slf)
    }

    /// Select the retained Iceberg snapshot current at an epoch timestamp.
    fn at_timestamp_micros(
        mut slf: PyRefMut<'_, Self>,
        timestamp_micros: i64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let QueryTarget::Lakehouse { snapshot, .. } = &mut slf.query.target else {
            return Err(InvalidError::new_err(
                "timestamp selection requires a lakehouse query target",
            ));
        };
        *snapshot = Some(SnapshotSelector::TimestampMicros(timestamp_micros));
        Ok(slf)
    }

    fn filter_eq<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_typed_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Eq, value));
        Ok(slf)
    }

    fn filter_ne<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_typed_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Ne, value));
        Ok(slf)
    }

    fn filter_gt<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_typed_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Gt, value));
        Ok(slf)
    }

    fn filter_gte<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_typed_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Gte, value));
        Ok(slf)
    }

    fn filter_lt<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_typed_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Lt, value));
        Ok(slf)
    }

    fn filter_lte<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_typed_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Lte, value));
        Ok(slf)
    }

    fn filter_in<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_typed_value(values)?;
        slf.and_filter(Filter::pred(field, CmpOp::In, value));
        Ok(slf)
    }

    fn filter_contains<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: String,
    ) -> PyRefMut<'py, Self> {
        slf.and_filter(Filter::pred(field, CmpOp::Contains, value));
        slf
    }

    fn filter_prefix<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: String,
    ) -> PyRefMut<'py, Self> {
        slf.and_filter(Filter::pred(field, CmpOp::Prefix, value));
        slf
    }

    /// Filter on the indexed message type.
    fn message_type<'py>(mut slf: PyRefMut<'py, Self>, value: String) -> PyRefMut<'py, Self> {
        slf.query.message_type = Some(value);
        slf
    }

    /// Filter rows whose timestamp (epoch micros) falls in [start, end], both
    /// bounds inclusive.
    fn time_range<'py>(mut slf: PyRefMut<'py, Self>, start: u64, end: u64) -> PyRefMut<'py, Self> {
        slf.query.time_range = Some((start, end));
        slf
    }

    fn order_asc<'py>(mut slf: PyRefMut<'py, Self>, field: String) -> PyRefMut<'py, Self> {
        slf.query.order.push(Sort {
            field,
            dir: Dir::Asc,
        });
        slf
    }

    fn order_desc<'py>(mut slf: PyRefMut<'py, Self>, field: String) -> PyRefMut<'py, Self> {
        slf.query.order.push(Sort {
            field,
            dir: Dir::Desc,
        });
        slf
    }

    fn limit(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.query.page.limit = u32::try_from(n).unwrap_or(u32::MAX);
        slf
    }

    fn offset(mut slf: PyRefMut<'_, Self>, n: u64) -> PyRefMut<'_, Self> {
        slf.query.page.offset = Some(n);
        slf.query.page.cursor = None;
        slf
    }

    /// Resume from an opaque cursor returned by the server. Replaces any offset.
    fn cursor<'py>(mut slf: PyRefMut<'py, Self>, cursor: String) -> PyRefMut<'py, Self> {
        slf.query.page.cursor = Some(cursor);
        slf.query.page.offset = None;
        slf
    }

    /// Return the opaque payload bytes on each row.
    fn with_payload(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.query.select.payload = true;
        slf
    }

    /// Request an exact `page.total` (a `COUNT(*)` on the server). Default
    /// off: without it, `page.total` is `None` and `page.has_more` is still
    /// exact. Ask for this only when the exact count itself matters, it costs
    /// a full scan on a wide filter.
    fn with_total(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.query.page.want_total = true;
        slf
    }

    /// Project only the named indexed fields into each row.
    fn select_fields<'py>(
        mut slf: PyRefMut<'py, Self>,
        fields: Vec<String>,
    ) -> PyRefMut<'py, Self> {
        slf.query.select.fields = fields;
        slf
    }

    /// Require read-your-writes consistency.
    fn read_your_writes(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.query.consistency = Consistency::ReadYourWrites;
        slf
    }

    /// Lexical relevance search over every text-hinted indexed field. Relevance
    /// lands in each row's `score` like vector distance. Gated on the `keyword`
    /// capability and refused locally when unadvertised.
    fn text<'py>(mut slf: PyRefMut<'py, Self>, query: String) -> PyRefMut<'py, Self> {
        slf.query.text = Some(TextQuery { field: None, query });
        slf
    }

    /// Lexical relevance search narrowed to one indexed `field`.
    fn text_in<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        query: String,
    ) -> PyRefMut<'py, Self> {
        slf.query.text = Some(TextQuery {
            field: Some(field),
            query,
        });
        slf
    }

    /// Set the read-consistency level: 'eventual', 'read_your_writes', or 'strong'.
    fn consistency<'py>(
        mut slf: PyRefMut<'py, Self>,
        level: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.query.consistency = parse_consistency(level)?;
        Ok(slf)
    }

    /// Return only distinct rows over the projected fields (needs select_fields).
    fn distinct(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.query.distinct = true;
        slf
    }

    fn count(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.push_agg(agg_call(AggFunc::Count, None, None, "count"));
        slf
    }

    fn sum<'py>(mut slf: PyRefMut<'py, Self>, field: String) -> PyRefMut<'py, Self> {
        slf.push_agg(agg_call(AggFunc::Sum, Some(field), None, "sum"));
        slf
    }

    fn avg<'py>(mut slf: PyRefMut<'py, Self>, field: String) -> PyRefMut<'py, Self> {
        slf.push_agg(agg_call(AggFunc::Avg, Some(field), None, "avg"));
        slf
    }

    fn min<'py>(mut slf: PyRefMut<'py, Self>, field: String) -> PyRefMut<'py, Self> {
        slf.push_agg(agg_call(AggFunc::Min, Some(field), None, "min"));
        slf
    }

    fn max<'py>(mut slf: PyRefMut<'py, Self>, field: String) -> PyRefMut<'py, Self> {
        slf.push_agg(agg_call(AggFunc::Max, Some(field), None, "max"));
        slf
    }

    fn count_distinct<'py>(mut slf: PyRefMut<'py, Self>, field: String) -> PyRefMut<'py, Self> {
        slf.push_agg(agg_call(
            AggFunc::CountDistinct,
            Some(field),
            None,
            "count_distinct",
        ));
        slf
    }

    fn stddev<'py>(mut slf: PyRefMut<'py, Self>, field: String) -> PyRefMut<'py, Self> {
        slf.push_agg(agg_call(AggFunc::StdDev, Some(field), None, "stddev"));
        slf
    }

    fn percentile<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        fraction: f64,
    ) -> PyRefMut<'py, Self> {
        slf.push_agg(agg_call(
            AggFunc::Percentile,
            Some(field),
            Some(fraction),
            "percentile",
        ));
        slf
    }

    /// Group the aggregate by the named fields.
    fn group_by<'py>(mut slf: PyRefMut<'py, Self>, fields: Vec<String>) -> PyRefMut<'py, Self> {
        match slf.query.aggregate.as_mut() {
            Some(aggregate) => aggregate.group_by = fields,
            None => {
                slf.query.aggregate = Some(Aggregate {
                    group_by: fields,
                    funcs: Vec::new(),
                    window: None,
                });
            }
        }
        slf
    }

    /// Bucket the aggregate into tumbling windows of `every_micros` over `field`.
    fn window<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        every_micros: u64,
    ) -> PyRefMut<'py, Self> {
        let window = Some(Window {
            field,
            every_micros,
        });
        match slf.query.aggregate.as_mut() {
            Some(aggregate) => aggregate.window = window,
            None => {
                slf.query.aggregate = Some(Aggregate {
                    group_by: Vec::new(),
                    funcs: Vec::new(),
                    window,
                });
            }
        }
        slf
    }

    /// Raw-SQL escape hatch with an explicit backend dialect and typed parameters.
    #[pyo3(signature = (sql, params=None, *, dialect="data_fusion"))]
    fn raw_sql<'py>(
        mut slf: PyRefMut<'py, Self>,
        sql: String,
        params: Option<&Bound<'_, PyAny>>,
        dialect: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let dialect = match dialect {
            "data_fusion" => SqlDialect::DataFusion,
            "postgres" => SqlDialect::Postgres,
            "my_sql" => SqlDialect::MySql,
            "sqlite" => SqlDialect::Sqlite,
            other => {
                return Err(InvalidError::new_err(format!(
                    "unknown SQL dialect '{other}'"
                )));
            }
        };
        let params = match params {
            Some(values) => match py_to_typed_value(values)? {
                TypedValue::List(values) => values,
                _ => {
                    return Err(InvalidError::new_err(
                        "raw SQL params must be a list or tuple",
                    ));
                }
            },
            None => Vec::new(),
        };
        slf.query.raw_sql = Some(RawSql {
            dialect,
            sql,
            params,
        });
        Ok(slf)
    }

    /// Approximate nearest-neighbour search on `field` (default "embedding").
    #[pyo3(signature = (embedding, top_k, *, field=None))]
    fn nearest<'py>(
        mut slf: PyRefMut<'py, Self>,
        embedding: Vec<f32>,
        top_k: u32,
        field: Option<String>,
    ) -> PyRefMut<'py, Self> {
        let field = field.unwrap_or_else(|| laser_sdk::query::VECTOR_FIELD.to_owned());
        slf.query.vector = Some(VectorQuery {
            field,
            embedding,
            top_k,
        });
        slf
    }

    /// Run the query and return one page plus pagination metadata.
    fn fetch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let query = self.query.clone();
        future_into_py(py, async move {
            let result = laser.execute_query(query).await.map_err(to_pyerr)?;
            Ok(PyQueryResult::from(result))
        })
    }

    /// Run the query and return every matching row, auto-paginating internally.
    fn fetch_all<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let mut walk = RowWalk::new(self.query.clone(), usize::MAX);
        future_into_py(py, async move {
            let mut rows = Vec::new();
            while let Some(row) = walk.next(&laser).await.map_err(to_pyerr)? {
                rows.push(PyRow::from(row));
            }
            Ok(rows)
        })
    }

    /// Run the query, decoding every row's JSON payload into a Python value.
    /// A row without its original payload raises `ConfigError`.
    fn fetch_typed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let mut query = self.query.clone();
        query.select.payload = true;
        future_into_py(py, async move {
            let result = laser.execute_query(query).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                let mut decoded = Vec::with_capacity(result.rows.len());
                for row in &result.rows {
                    decoded.push(decode_row(py, &result.fields, row)?);
                }
                Ok(decoded.into_pyobject(py)?.unbind().into_any())
            })
        })
    }

    /// Run the query and decode every matching row's JSON payload into a
    /// Python value, walking pages internally. A row without its original
    /// payload raises `ConfigError`.
    fn fetch_all_typed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let mut query = self.query.clone();
        query.select.payload = true;
        let mut walk = RowWalk::new(query, usize::MAX);
        future_into_py(py, async move {
            let mut decoded = Vec::new();
            while let Some(row) = walk.next(&laser).await.map_err(to_pyerr)? {
                decoded.push(Python::attach(|py| decode_row(py, &walk.fields, &row))?);
            }
            Ok(decoded)
        })
    }

    /// Run the query capped at one row, decoding its JSON payload, or `None`
    /// when nothing matches.
    fn fetch_one<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let mut query = self.query.clone();
        query.select.payload = true;
        query.page.limit = 1;
        future_into_py(py, async move {
            let result = laser.execute_query(query).await.map_err(to_pyerr)?;
            Python::attach(|py| match result.rows.first() {
                Some(row) => decode_row(py, &result.fields, row),
                None => Ok(py.None()),
            })
        })
    }

    /// Like `fetch_typed` but each row's payload is decoded by a user `codec`
    /// (any object with `decode(data) -> value`) instead of JSON. A codec
    /// failure raises `CodecError` with the codec's exception as its cause.
    fn fetch_typed_with<'py>(
        &self,
        py: Python<'py>,
        codec: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let mut query = self.query.clone();
        query.select.payload = true;
        future_into_py(py, async move {
            let result = laser.execute_query(query).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                let codec = codec.bind(py);
                let mut decoded = Vec::with_capacity(result.rows.len());
                for row in &result.rows {
                    decoded.push(decode_row_with(&result.fields, row, |payload| {
                        codec_decode(codec, payload)
                    })?);
                }
                Ok(decoded.into_pyobject(py)?.unbind().into_any())
            })
        })
    }

    /// Like `fetch_one` but the payload is decoded by a user `codec`.
    fn fetch_one_with<'py>(
        &self,
        py: Python<'py>,
        codec: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let mut query = self.query.clone();
        query.select.payload = true;
        query.page.limit = 1;
        future_into_py(py, async move {
            let result = laser.execute_query(query).await.map_err(to_pyerr)?;
            Python::attach(|py| match result.rows.first() {
                Some(row) => decode_row_with(&result.fields, row, |payload| {
                    codec_decode(codec.bind(py), payload)
                }),
                None => Ok(py.None()),
            })
        })
    }

    /// Walk matching rows across pages, bounded by an explicit `max_rows`.
    /// `async for row in query.max_rows(n).rows()` yields one row at a time and
    /// stops at the cap or after the last page, whichever comes first. Pages
    /// are fetched only as the walk reaches them. Raises `InvalidError` at once
    /// without a `max_rows` ceiling.
    fn rows(&self) -> PyResult<PyQueryRows> {
        let cap = self.row_ceiling("rows")?;
        Ok(PyQueryRows {
            laser: self.laser.clone(),
            walk: Arc::new(AsyncMutex::new(RowWalk::new(self.query.clone(), cap))),
        })
    }

    /// Like `rows` but each yield is the row's JSON payload decoded into a
    /// Python value, under the same explicit `max_rows` ceiling. The payload
    /// is requested automatically, and the publisher must have inlined it: a
    /// row without it raises `ConfigError`.
    fn rows_typed(&self) -> PyResult<PyTypedQueryRows> {
        let cap = self.row_ceiling("rows_typed")?;
        let mut query = self.query.clone();
        query.select.payload = true;
        Ok(PyTypedQueryRows {
            laser: self.laser.clone(),
            walk: Arc::new(AsyncMutex::new(RowWalk::new(query, cap))),
        })
    }

    /// Read the current execution state for this query identity.
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let execution_id = self.query.execution_id;
        future_into_py(py, async move {
            let status = laser.query_status(execution_id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &status))
        })
    }

    /// Request cancellation for this query identity and return its observed state.
    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let execution_id = self.query.execution_id;
        future_into_py(py, async move {
            let status = laser.cancel_query(execution_id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &status))
        })
    }

    /// The query this request built, as the dict `Laser.execute_query` takes.
    // A Python method cannot consume its receiver, so this keeps the Rust
    // name and borrows.
    #[allow(clippy::wrong_self_convention)]
    fn into_query(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.query)
    }
}

impl PyQuery {
    fn row_ceiling(&self, verb: &str) -> PyResult<usize> {
        self.max_rows.ok_or_else(|| {
            InvalidError::new_err(format!(
                "{verb}() needs an explicit ceiling: chain .max_rows(n) first"
            ))
        })
    }
}

fn parse_agg_func(func: &str) -> PyResult<AggFunc> {
    Ok(match func {
        "count" => AggFunc::Count,
        "sum" => AggFunc::Sum,
        "avg" => AggFunc::Avg,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        "count_distinct" => AggFunc::CountDistinct,
        "stddev" => AggFunc::StdDev,
        "percentile" => AggFunc::Percentile,
        other => {
            return Err(InvalidError::new_err(format!(
                "unknown aggregate '{other}' (expected count, sum, avg, min, max, count_distinct, stddev, or percentile)"
            )));
        }
    })
}

fn parse_cmp_op(op: &str) -> PyResult<CmpOp> {
    Ok(match op {
        "eq" => CmpOp::Eq,
        "ne" => CmpOp::Ne,
        "lt" => CmpOp::Lt,
        "lte" => CmpOp::Lte,
        "gt" => CmpOp::Gt,
        "gte" => CmpOp::Gte,
        "in" => CmpOp::In,
        "contains" => CmpOp::Contains,
        "prefix" => CmpOp::Prefix,
        other => {
            return Err(InvalidError::new_err(format!(
                "unknown comparison '{other}' (expected eq, ne, lt, lte, gt, gte, in, contains, or prefix)"
            )));
        }
    })
}

/// A query predicate tree, the Python form of the query `Filter`: a
/// comparison leaf (`pred`), a conjunction (`all`), a disjunction (`any`), or
/// a negation (`negate`). Pass it to `QueryRequest.filter`,
/// `QueryRequest.having`, or `Graph.query(start_match=..)`. Consumer filters use
/// the separate `FilterExpr`.
#[gen_stub_pyclass]
#[pyclass(name = "Filter", frozen)]
pub struct PyQueryFilter {
    pub(crate) inner: Filter,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQueryFilter {
    /// A single comparison leaf, `field op value`. `op` is `eq`, `ne`, `lt`,
    /// `lte`, `gt`, `gte`, `in`, `contains`, or `prefix`.
    #[staticmethod]
    fn pred(field: String, op: &str, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: Filter::pred(field, parse_cmp_op(op)?, py_to_typed_value(value)?),
        })
    }

    /// The AND of `filters`.
    #[staticmethod]
    fn all(filters: Vec<PyRef<'_, PyQueryFilter>>) -> Self {
        Self {
            inner: Filter::all(filters.iter().map(|filter| filter.inner.clone())),
        }
    }

    /// The OR of `filters`.
    #[staticmethod]
    fn any(filters: Vec<PyRef<'_, PyQueryFilter>>) -> Self {
        Self {
            inner: Filter::any(filters.iter().map(|filter| filter.inner.clone())),
        }
    }

    /// The negation of `filter`.
    #[staticmethod]
    fn negate(filter: PyRef<'_, PyQueryFilter>) -> Self {
        Self {
            inner: Filter::negate(filter.inner.clone()),
        }
    }

    /// The filter as its wire dict, with each comparison leaf as a
    /// `{"field", "op", "value"}` predicate.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner)
    }
}

/// A builder for the query dict `Laser.execute_query` takes, field for field
/// like the Rust `Query::builder()`. `execution_id`, `target`, and
/// `deadline_micros` are required. Nested values are their wire dicts
/// (`key_match_new` and `query_target_operational` build the common ones) and
/// filters are `Filter` values. `build()` returns the query dict.
#[gen_stub_pyclass]
#[pyclass(name = "QueryBuilder")]
#[derive(Default)]
pub struct PyQueryBuilder {
    execution_id: Option<QueryExecutionId>,
    target: Option<QueryTarget>,
    deadline_micros: Option<u64>,
    by_key: Vec<KeyMatch>,
    message_type: Option<String>,
    time_range: Option<(u64, u64)>,
    filter: Option<Filter>,
    vector: Option<VectorQuery>,
    text: Option<TextQuery>,
    order: Vec<Sort>,
    page: Option<QueryPageRequest>,
    aggregate: Option<Aggregate>,
    having: Option<Filter>,
    distinct: bool,
    select: Option<Select>,
    fork: Option<String>,
    raw_sql: Option<RawSql>,
    consistency: Option<Consistency>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQueryBuilder {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    /// The execution id, a ULID string, that status, paging, and cancel use.
    fn execution_id<'py>(
        mut slf: PyRefMut<'py, Self>,
        execution_id: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.execution_id = Some(parse_execution_id(execution_id)?);
        Ok(slf)
    }

    /// The query target dict, such as `query_target_operational(index)`.
    fn target<'py>(
        mut slf: PyRefMut<'py, Self>,
        target: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.target = Some(py_to_de(target)?);
        Ok(slf)
    }

    /// The absolute execution deadline in epoch microseconds.
    fn deadline_micros(mut slf: PyRefMut<'_, Self>, deadline_micros: u64) -> PyRefMut<'_, Self> {
        slf.deadline_micros = Some(deadline_micros);
        slf
    }

    /// Exact-match key dicts, such as `key_match_new(field, value)`.
    fn by_key<'py>(
        mut slf: PyRefMut<'py, Self>,
        by_key: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.by_key = py_to_de(by_key)?;
        Ok(slf)
    }

    /// The indexed message type, or `None` for any.
    #[pyo3(signature = (message_type))]
    fn message_type(
        mut slf: PyRefMut<'_, Self>,
        message_type: Option<String>,
    ) -> PyRefMut<'_, Self> {
        slf.message_type = message_type;
        slf
    }

    /// The `(start, end)` epoch microsecond range, both bounds inclusive, or
    /// `None` for any time.
    #[pyo3(signature = (time_range))]
    fn time_range(
        mut slf: PyRefMut<'_, Self>,
        time_range: Option<(u64, u64)>,
    ) -> PyRefMut<'_, Self> {
        slf.time_range = time_range;
        slf
    }

    /// The row predicate tree, or `None` for an unfiltered scan.
    #[pyo3(signature = (filter))]
    fn filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        filter: Option<PyRef<'_, PyQueryFilter>>,
    ) -> PyRefMut<'py, Self> {
        slf.filter = filter.map(|filter| filter.inner.clone());
        slf
    }

    /// The nearest-neighbour search dict, or `None`.
    #[pyo3(signature = (vector))]
    fn vector<'py>(
        mut slf: PyRefMut<'py, Self>,
        vector: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.vector = vector.map(py_to_de).transpose()?;
        Ok(slf)
    }

    /// The lexical relevance search dict, or `None`.
    #[pyo3(signature = (text))]
    fn text<'py>(
        mut slf: PyRefMut<'py, Self>,
        text: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.text = text.map(py_to_de).transpose()?;
        Ok(slf)
    }

    /// The sort key dicts, `{"field", "dir"}`, in priority order.
    fn order<'py>(
        mut slf: PyRefMut<'py, Self>,
        order: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.order = py_to_de(order)?;
        Ok(slf)
    }

    /// The page request dict: `limit`, `offset`, `cursor`, `want_total`.
    fn page<'py>(
        mut slf: PyRefMut<'py, Self>,
        page: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.page = Some(py_to_de(page)?);
        Ok(slf)
    }

    /// The aggregate dict, or `None` for row selection.
    #[pyo3(signature = (aggregate))]
    fn aggregate<'py>(
        mut slf: PyRefMut<'py, Self>,
        aggregate: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.aggregate = aggregate.map(py_to_de).transpose()?;
        Ok(slf)
    }

    /// The predicate over aggregate output, or `None`.
    #[pyo3(signature = (having))]
    fn having<'py>(
        mut slf: PyRefMut<'py, Self>,
        having: Option<PyRef<'_, PyQueryFilter>>,
    ) -> PyRefMut<'py, Self> {
        slf.having = having.map(|having| having.inner.clone());
        slf
    }

    /// Whether to return only distinct rows over the selected fields.
    fn distinct(mut slf: PyRefMut<'_, Self>, distinct: bool) -> PyRefMut<'_, Self> {
        slf.distinct = distinct;
        slf
    }

    /// The selection dict: `fields` and `payload`.
    fn select<'py>(
        mut slf: PyRefMut<'py, Self>,
        select: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.select = Some(py_to_de(select)?);
        Ok(slf)
    }

    /// The fork whose view to read, or `None` for the trunk.
    #[pyo3(signature = (fork))]
    fn fork(mut slf: PyRefMut<'_, Self>, fork: Option<String>) -> PyRefMut<'_, Self> {
        slf.fork = fork;
        slf
    }

    /// The raw SQL dict: `dialect`, `sql`, and `params`, or `None`.
    #[pyo3(signature = (raw_sql))]
    fn raw_sql<'py>(
        mut slf: PyRefMut<'py, Self>,
        raw_sql: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.raw_sql = raw_sql.map(py_to_de).transpose()?;
        Ok(slf)
    }

    /// The read-consistency level: 'eventual', 'read_your_writes', or 'strong'.
    fn consistency<'py>(
        mut slf: PyRefMut<'py, Self>,
        consistency: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.consistency = Some(parse_consistency(consistency)?);
        Ok(slf)
    }

    /// The query dict. Raises `InvalidError` when `execution_id`, `target`,
    /// or `deadline_micros` is unset.
    fn build(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let missing = |name: &str| InvalidError::new_err(format!("query builder needs {name}"));
        let execution_id = self.execution_id.ok_or_else(|| missing("execution_id"))?;
        let target = self.target.clone().ok_or_else(|| missing("target"))?;
        let deadline_micros = self
            .deadline_micros
            .ok_or_else(|| missing("deadline_micros"))?;
        let query = Query::builder()
            .execution_id(execution_id)
            .target(target)
            .deadline_micros(deadline_micros)
            .by_key(self.by_key.clone())
            .maybe_message_type(self.message_type.clone())
            .maybe_time_range(self.time_range)
            .maybe_filter(self.filter.clone())
            .maybe_vector(self.vector.clone())
            .maybe_text(self.text.clone())
            .order(self.order.clone())
            .page(self.page.clone().unwrap_or_default())
            .maybe_aggregate(self.aggregate.clone())
            .maybe_having(self.having.clone())
            .distinct(self.distinct)
            .select(self.select.clone().unwrap_or_default())
            .maybe_fork(self.fork.clone())
            .maybe_raw_sql(self.raw_sql.clone())
            .consistency(self.consistency.unwrap_or_default())
            .build();
        ser_to_py(py, &query)
    }
}

/// A query dict for an explicit target dict and execution boundary.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn query_new(
    py: Python<'_>,
    execution_id: &str,
    target: &Bound<'_, PyAny>,
    deadline_micros: u64,
) -> PyResult<Py<PyAny>> {
    let query = Query::new(
        parse_execution_id(execution_id)?,
        py_to_de(target)?,
        deadline_micros,
    );
    ser_to_py(py, &query)
}

/// An operational query dict over one materialized index.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn query_operational(
    py: Python<'_>,
    execution_id: &str,
    index: String,
    deadline_micros: u64,
) -> PyResult<Py<PyAny>> {
    let query = Query::operational(parse_execution_id(execution_id)?, index, deadline_micros);
    ser_to_py(py, &query)
}

/// The operational query target dict for one materialized index.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn query_target_operational(py: Python<'_>, index: String) -> PyResult<Py<PyAny>> {
    ser_to_py(py, &QueryTarget::operational(index))
}

/// An exact-match key dict, `field == value`, for `QueryBuilder.by_key`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn key_match_new(
    py: Python<'_>,
    field: String,
    value: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    ser_to_py(py, &KeyMatch::new(field, py_to_typed_value(value)?))
}

/// The numeric wire code of a result code name such as `NotFound`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn result_code_code(code: &str) -> PyResult<u16> {
    Ok(parse_result_code(code)?.code())
}

/// The result code name for a numeric wire code. An unknown code reads as
/// `Unrecognized(<code>)`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn result_code_from_code(code: u16) -> String {
    format!("{:?}", ResultCode::from_code(code))
}

/// The HTTP status a result code name maps onto.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn result_code_http_status(code: &str) -> PyResult<u16> {
    Ok(parse_result_code(code)?.http_status())
}

/// Whether a failure with this result code name may succeed when retried.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn result_code_is_retryable(code: &str) -> PyResult<bool> {
    Ok(parse_result_code(code)?.is_retryable())
}

fn parse_execution_id(value: &str) -> PyResult<QueryExecutionId> {
    QueryExecutionId::from_str(value).map_err(|error| InvalidError::new_err(error.to_string()))
}

// A result code name is the Rust variant name `LaserError.code` reports, so
// the parse walks the codes the SDK knows and reads the catch-all's number.
fn parse_result_code(name: &str) -> PyResult<ResultCode> {
    if let Some(code) = name
        .strip_prefix("Unrecognized(")
        .and_then(|rest| rest.strip_suffix(')'))
    {
        return code
            .parse::<u16>()
            .map(ResultCode::from_code)
            .map_err(|_| InvalidError::new_err(format!("unknown result code '{name}'")));
    }
    (0..=u16::from(u8::MAX))
        .map(ResultCode::from_code)
        .take_while(|code| !matches!(code, ResultCode::Unrecognized(_)))
        .find(|code| format!("{code:?}") == name)
        .ok_or_else(|| InvalidError::new_err(format!("unknown result code '{name}'")))
}

fn agg_call(func: AggFunc, field: Option<String>, arg: Option<f64>, alias: &str) -> AggCall {
    AggCall {
        func,
        field,
        arg,
        alias: alias.to_owned(),
    }
}

/// The bounded row walk returned by `QueryRequest.rows()`. Iterate it with
/// `async for`, or await `next()` until it returns `None`. Drive one walk from
/// one task.
#[gen_stub_pyclass]
#[pyclass(name = "QueryRows")]
pub struct PyQueryRows {
    laser: Laser,
    walk: Arc<AsyncMutex<RowWalk>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQueryRows {
    /// The next row, or `None` at the ceiling or after the last page.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let walk = self.walk.clone();
        future_into_py(py, async move {
            let row = walk.lock().await.next(&laser).await.map_err(to_pyerr)?;
            Ok(row.map(PyRow::from))
        })
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let walk = self.walk.clone();
        future_into_py(py, async move {
            match walk.lock().await.next(&laser).await.map_err(to_pyerr)? {
                Some(row) => Ok(PyRow::from(row)),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }
}

/// The bounded typed row walk returned by `QueryRequest.rows_typed()`: each
/// yield is the row's JSON payload decoded into a Python value. Iterate it
/// with `async for`, or await `next()` until it returns `None`. Drive one walk
/// from one task.
#[gen_stub_pyclass]
#[pyclass(name = "TypedQueryRows")]
pub struct PyTypedQueryRows {
    laser: Laser,
    walk: Arc<AsyncMutex<RowWalk>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTypedQueryRows {
    /// The next decoded payload, or `None` at the ceiling or after the last
    /// page. A payload that is JSON `null` also reads as `None`, and
    /// `async for` tells the two apart.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let walk = self.walk.clone();
        future_into_py(py, async move {
            let mut walk = walk.lock().await;
            match walk.next(&laser).await.map_err(to_pyerr)? {
                Some(row) => Python::attach(|py| decode_row(py, &walk.fields, &row)),
                None => Ok(Python::attach(|py| py.None())),
            }
        })
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let walk = self.walk.clone();
        future_into_py(py, async move {
            let mut walk = walk.lock().await;
            match walk.next(&laser).await.map_err(to_pyerr)? {
                Some(row) => Python::attach(|py| decode_row(py, &walk.fields, &row)),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }
}

// The SDK's bounded page walk behind `rows()`: pages fetch on demand, an empty
// page or `has_more = false` ends it, aggregate and vector queries are single
// page, and the walk continues with the server cursor. A continuation with no
// cursor would restart the result, so it ends the walk too.
struct RowWalk {
    query: Query,
    remaining: usize,
    finished: bool,
    single_page: bool,
    fields: Vec<laser_sdk::wire::schema::LogicalField>,
    buffer: std::vec::IntoIter<Row>,
}

impl RowWalk {
    fn new(mut query: Query, max_rows: usize) -> Self {
        if query.page.limit == 0 {
            query.page.limit = laser_sdk::query::DEFAULT_STREAM_PAGE_SIZE as u32;
        }
        let single_page = query.aggregate.is_some() || query.vector.is_some();
        Self {
            query,
            remaining: max_rows,
            finished: false,
            single_page,
            fields: Vec::new(),
            buffer: Vec::new().into_iter(),
        }
    }

    async fn next(&mut self, laser: &Laser) -> Result<Option<Row>, LaserError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let row = match self.buffer.next() {
            Some(row) => Some(row),
            None if self.finished => None,
            None => {
                let page = laser.execute_query(self.query.clone()).await?;
                let fetched = page.rows.len();
                self.finished = self.single_page
                    || fetched == 0
                    || !page.page.has_more
                    || page.page.next_cursor.is_none()
                    || page.page.next_cursor == self.query.page.cursor;
                self.query.page.cursor = page.page.next_cursor;
                self.query.page.offset = None;
                self.fields = page.fields;
                self.buffer = page.rows.into_iter();
                self.buffer.next()
            }
        };
        if row.is_some() {
            self.remaining -= 1;
        }
        Ok(row)
    }
}

// Decode one row's original payload as JSON exactly as the SDK's typed reads
// do.
fn decode_row(
    py: Python<'_>,
    fields: &[laser_sdk::wire::schema::LogicalField],
    row: &Row,
) -> PyResult<Py<PyAny>> {
    decode_row_with(fields, row, |payload| {
        let value: serde_json::Value = <Json as Decoder<serde_json::Value>>::decode(payload)
            .map_err(|error| to_pyerr(LaserError::from(error)))?;
        json_to_py(py, &value)
    })
}

// Find a row's original payload like the SDK's `decode_row` and hand it to
// `decode`: a result without the payload field, or a row whose payload is
// null, is a missing payload, and a short row or a non-binary payload is
// server skew.
fn decode_row_with(
    fields: &[laser_sdk::wire::schema::LogicalField],
    row: &Row,
    decode: impl FnOnce(&[u8]) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let index = fields
        .iter()
        .position(|field| field.name == laser_sdk::wire::schema::ORIGINAL_PAYLOAD_FIELD_NAME)
        .ok_or_else(|| {
            to_pyerr(LaserError::from(DecodeError::MissingPayload(
                "query result does not contain the original payload field",
            )))
        })?;
    let value = row.values.get(index).ok_or_else(|| {
        to_pyerr(LaserError::Protocol(
            "query row is shorter than its declared result schema".to_owned(),
        ))
    })?;
    match value {
        TypedValue::Binary(payload) => decode(&payload.0),
        TypedValue::Null => Err(to_pyerr(LaserError::from(DecodeError::MissingPayload(
            "query row has no original payload",
        )))),
        _ => Err(to_pyerr(LaserError::Protocol(
            "query original payload field is not binary".to_owned(),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_sdk::wire::schema::{LogicalField, LogicalType, ORIGINAL_PAYLOAD_FIELD_NAME};

    fn walk(max_rows: usize) -> (Laser, RowWalk) {
        let laser = Laser::from_client(laser_sdk::iggy::prelude::IggyClient::default());
        let query = Query::builder()
            .execution_id(laser_sdk::query::QueryExecutionId::mint())
            .target(QueryTarget::operational("readings"))
            .deadline_micros(u64::MAX)
            .build();
        (laser, RowWalk::new(query, max_rows))
    }

    fn payload_field() -> LogicalField {
        LogicalField {
            id: 1,
            name: ORIGINAL_PAYLOAD_FIELD_NAME.to_owned(),
            required: false,
            field_type: LogicalType::Binary,
            doc: None,
        }
    }

    fn row(values: Vec<TypedValue>) -> Row {
        Row {
            values,
            score: None,
        }
    }

    #[test]
    fn given_a_zero_row_ceiling_when_walked_then_should_end_without_querying() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let (laser, mut empty) = walk(0);
        assert!(
            runtime
                .block_on(empty.next(&laser))
                .expect("a zero ceiling never queries")
                .is_none()
        );
        let (laser, mut one) = walk(1);
        assert!(
            runtime.block_on(one.next(&laser)).is_err(),
            "a non-zero ceiling queries the unconnected client"
        );
    }

    #[test]
    fn given_a_zero_page_limit_when_walked_then_should_page_by_the_stream_default() {
        let (_, template) = walk(5);
        let mut query = template.query;
        query.page.limit = 0;
        let walk = RowWalk::new(query, 5);
        assert_eq!(
            walk.query.page.limit,
            laser_sdk::query::DEFAULT_STREAM_PAGE_SIZE as u32
        );
        assert!(!walk.single_page);
    }

    #[test]
    fn given_rows_without_a_usable_payload_when_decoded_then_should_raise_like_the_sdk() {
        Python::initialize();
        Python::attach(|py| {
            let fields = [payload_field()];
            let missing = decode_row(py, &[], &row(vec![TypedValue::Null])).unwrap_err();
            assert!(missing.is_instance_of::<crate::errors::ConfigError>(py));
            let null = decode_row(py, &fields, &row(vec![TypedValue::Null])).unwrap_err();
            assert!(null.is_instance_of::<crate::errors::ConfigError>(py));
            let short = decode_row(py, &fields, &row(Vec::new())).unwrap_err();
            assert!(short.is_instance_of::<crate::errors::ProtocolError>(py));
            let text =
                decode_row(py, &fields, &row(vec![TypedValue::String("x".into())])).unwrap_err();
            assert!(text.is_instance_of::<crate::errors::ProtocolError>(py));
            let broken = decode_row(
                py,
                &fields,
                &row(vec![TypedValue::Binary(
                    laser_sdk::wire::schema::BinaryValue(b"{".to_vec()),
                )]),
            )
            .unwrap_err();
            assert!(broken.is_instance_of::<crate::errors::CodecError>(py));
            let decoded = decode_row(
                py,
                &fields,
                &row(vec![TypedValue::Binary(
                    laser_sdk::wire::schema::BinaryValue(br#"{"cpu": 82}"#.to_vec()),
                )]),
            )
            .expect("a JSON payload decodes");
            let cpu: i64 = decoded
                .bind(py)
                .get_item("cpu")
                .and_then(|value| value.extract())
                .expect("cpu field");
            assert_eq!(cpu, 82);
        });
    }
}

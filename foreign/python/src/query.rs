use crate::async_bridge::{Awaitable, awaitable};
use crate::client::PyLaser;
use crate::convert::{codec_decode, json_to_py, py_to_de, py_to_typed_value, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::LaserError;
use laser_sdk::laser::Laser;
use laser_sdk::query::{
    AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, Filter, KeyMatch, Query,
    QueryExecutionId, QueryFrontier, QueryPageRequest, QueryResult, QueryRow, QueryTarget, RawSql,
    ResultCode, Row, Select, Sort, SqlDialect, TextQuery, TypedValue, VectorQuery, Window,
};
use laser_sdk::stream::{Decoder, Json};
use laser_sdk::types::MintUlid;
use laser_sdk::wire::error::DecodeError;
use pyo3::exceptions::{PyIndexError, PyKeyError, PyStopAsyncIteration, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex as AsyncMutex;

/// The default and longest query budget in the shared retry table.
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(25);

fn parse_consistency(level: &str) -> PyResult<Consistency> {
    match level {
        "eventual" => Ok(Consistency::Eventual),
        "read_your_writes" => Ok(Consistency::ReadYourWrites),
        other => Err(InvalidError::new_err(format!(
            "consistency must be 'eventual' or 'read_your_writes', got '{other}'"
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

    /// Start a query against an explicit target dict. An operational index is
    /// scoped like `query(index)`.
    fn query_target(&self, target: &Bound<'_, PyAny>) -> PyResult<PyQuery> {
        let target = match py_to_de(target)? {
            QueryTarget::Operational { index } => {
                QueryTarget::operational(self.inner.resource_name(&index))
            }
            other => other,
        };
        Ok(PyQuery::new(self.inner.clone(), target))
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

/// One query row bound to the fields of the page it came on. Read a tagged
/// value by field name or position with `row["name"]` or `row[0]`, like
/// `sqlite3.Row`.
#[gen_stub_pyclass]
#[pyclass(name = "QueryRow", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyQueryRow {
    inner: QueryRow,
}

impl From<QueryRow> for PyQueryRow {
    fn from(inner: QueryRow) -> Self {
        Self { inner }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQueryRow {
    /// Every row of one page, each bound to the page's fields. Takes the
    /// fields and rows as `QueryResult.fields` and the wire rows read them.
    #[staticmethod]
    fn from_page(fields: &Bound<'_, PyAny>, rows: &Bound<'_, PyAny>) -> PyResult<Vec<Self>> {
        Ok(QueryRow::from_page(py_to_de(fields)?, py_to_de(rows)?)
            .into_iter()
            .map(Self::from)
            .collect())
    }

    /// Ordered logical field metadata of the page this row came on.
    #[getter]
    fn fields(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.fields())
    }

    /// Tagged values in field order.
    #[getter]
    fn values(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.values())
    }

    /// Backend-native vector distance or lexical rank.
    #[getter]
    fn score(&self) -> Option<f32> {
        self.inner.score()
    }

    /// The positional row, for `QueryResult`-style access.
    #[getter]
    fn row(&self) -> PyRow {
        PyRow::from(self.inner.row().clone())
    }

    /// The field names, in value order.
    fn keys(&self) -> Vec<String> {
        self.inner
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect()
    }

    /// The position of the field `name`, or `None`.
    fn field_index(&self, name: &str) -> Option<usize> {
        self.inner.field_index(name)
    }

    /// One tagged value by field name, or `None`.
    fn value(&self, py: Python<'_>, field: &str) -> PyResult<Py<PyAny>> {
        match self.inner.value(field) {
            Some(value) => ser_to_py(py, value),
            None => Ok(py.None()),
        }
    }

    /// One value by field name in its stable diagnostic form.
    fn value_text(&self, field: &str) -> Option<String> {
        self.inner.value_text(field)
    }

    /// One integer value by field name as an unsigned integer, or `None`.
    fn value_u64(&self, field: &str) -> Option<u64> {
        self.inner.value_u64(field)
    }

    /// One integer value by field name as a signed integer, or `None`.
    fn value_i64(&self, field: &str) -> Option<i64> {
        self.inner.value_i64(field)
    }

    fn __getitem__(
        &self,
        py: Python<'_>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.int", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let values = self.inner.values();
        let value = if let Ok(name) = key.extract::<String>() {
            self.inner
                .value(&name)
                .ok_or_else(|| PyKeyError::new_err(name))?
        } else if let Ok(position) = key.extract::<isize>() {
            let index = if position < 0 {
                values.len().checked_sub(position.unsigned_abs())
            } else {
                Some(position.unsigned_abs())
            };
            index
                .and_then(|index| values.get(index))
                .ok_or_else(|| PyIndexError::new_err("query row index out of range"))?
        } else {
            return Err(PyTypeError::new_err(
                "a query row is read by field name or position",
            ));
        };
        ser_to_py(py, value)
    }

    fn __len__(&self) -> usize {
        self.inner.values().len()
    }

    fn __repr__(&self) -> String {
        format!(
            "QueryRow(fields={:?}, score={:?})",
            self.keys(),
            self.inner.score()
        )
    }
}

/// Every row `fetch_all()` read, each bound to its fields, and whether a page
/// after the first was served at another frontier, so rows may repeat or be
/// missing. Iterates, indexes and measures like a list of `QueryRow`.
#[gen_stub_pyclass]
#[pyclass(name = "QueryRowSet", frozen)]
pub struct PyQueryRowSet {
    rows: Vec<PyQueryRow>,
    frontier_moved: bool,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQueryRowSet {
    /// The rows, in walk order.
    #[getter]
    fn rows(&self) -> Vec<PyQueryRow> {
        self.rows.clone()
    }

    /// Whether a page after the first was served at another frontier.
    #[getter]
    fn frontier_moved(&self) -> bool {
        self.frontier_moved
    }

    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn __getitem__(&self, index: isize) -> PyResult<PyQueryRow> {
        let position = if index < 0 {
            self.rows.len().checked_sub(index.unsigned_abs())
        } else {
            Some(index.unsigned_abs())
        };
        position
            .and_then(|position| self.rows.get(position))
            .cloned()
            .ok_or_else(|| PyIndexError::new_err("query row set index out of range"))
    }

    #[gen_stub(override_return_type(type_repr = "typing.Iterator[QueryRow]", imports = ("typing",)))]
    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        Ok(PyList::new(py, self.rows.clone())?
            .try_iter()?
            .unbind()
            .into_any())
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

    /// Paging metadata: offset, limit, optional exact total, and whether more
    /// rows follow.
    #[getter]
    fn page(&self) -> PyPage {
        PyPage {
            inner: self.inner.page.clone(),
        }
    }

    /// Engine, resolved target, consistency, resource, and frontier evidence.
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
    /// The offset this page started at.
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

    /// Rows known to exist so far, one past this page's last row.
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
    timeout: Option<Duration>,
}

impl PyQuery {
    fn new(laser: Laser, target: QueryTarget) -> Self {
        let query = Query::builder()
            .execution_id(QueryExecutionId::mint())
            .target(target)
            .deadline_micros(deadline_from_now(DEFAULT_QUERY_TIMEOUT))
            .build();
        Self {
            laser,
            query,
            max_rows: None,
            timeout: Some(DEFAULT_QUERY_TIMEOUT),
        }
    }

    // The query the next call sends, with a deadline counted from now under a
    // relative budget. The request then moves to a fresh execution id.
    fn next_call(&mut self) -> Query {
        let query = self.stamped();
        self.query.execution_id = QueryExecutionId::mint();
        query
    }

    fn stamped(&self) -> Query {
        let mut query = self.query.clone();
        if let Some(timeout) = self.timeout {
            query.deadline_micros = deadline_from_now(timeout);
        }
        query
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

    /// The execution id the next call sends and its reply echoes back. Every
    /// call and every page of a walk takes a fresh one.
    #[getter]
    fn execution_id(&self) -> String {
        self.query.execution_id.to_string()
    }

    /// Set the budget of each call and each page of a walk, `deadline_ms`
    /// milliseconds from when it starts (default and longest 25,000). A
    /// longer one fails the fetch.
    fn deadline(mut slf: PyRefMut<'_, Self>, deadline_ms: f64) -> PyResult<PyRefMut<'_, Self>> {
        slf.timeout = Some(crate::convert::duration_ms(deadline_ms, "deadline_ms")?);
        Ok(slf)
    }

    /// Set one absolute deadline in epoch microseconds for every later call
    /// and every page of a walk.
    fn deadline_at_micros(
        mut slf: PyRefMut<'_, Self>,
        deadline_at_micros: u64,
    ) -> PyRefMut<'_, Self> {
        slf.query.deadline_micros = deadline_at_micros;
        slf.timeout = None;
        slf
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
    /// capability and refused locally when unadvertised. Rows come in relevance
    /// order and page by offset, so the query takes no `order` and no total.
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

    /// Set the read-consistency level: 'eventual' or 'read_your_writes'.
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

    /// Bucket the aggregate into tumbling windows `every_ms` milliseconds wide
    /// over `field`.
    fn window<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        every_ms: f64,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let every = crate::convert::duration_ms(every_ms, "every_ms")?;
        let window = Some(Window {
            field,
            every_micros: u64::try_from(every.as_micros()).unwrap_or(u64::MAX),
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
        Ok(slf)
    }

    /// Raw-SQL escape hatch with an explicit backend dialect and typed parameters.
    #[pyo3(signature = (dialect, sql, params=None))]
    fn raw_sql<'py>(
        mut slf: PyRefMut<'py, Self>,
        dialect: &str,
        sql: String,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let dialect = match dialect {
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
    /// The `top_k` nearest rows are the whole answer, one page with no `order`,
    /// offset or total.
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
    fn fetch<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Awaitable<'py, PyQueryResult>> {
        let laser = slf.laser.clone();
        let query = slf.next_call();
        awaitable(py, async move {
            let result = laser.execute_query(query).await.map_err(to_pyerr)?;
            Ok(PyQueryResult::from(result))
        })
    }

    /// Run the query and return every matching row, auto-paginating
    /// internally, each bound to its fields, with whether the frontier moved
    /// between pages.
    fn fetch_all<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Awaitable<'py, PyQueryRowSet>> {
        let laser = slf.laser.clone();
        let mut walk = RowWalk::new(slf.next_call(), slf.timeout, usize::MAX);
        awaitable(py, async move {
            let mut rows = Vec::new();
            while let Some(row) = walk.next(&laser).await.map_err(to_pyerr)? {
                rows.push(PyQueryRow::from(row));
            }
            Ok(PyQueryRowSet {
                rows,
                frontier_moved: walk.frontier_moved.load(Ordering::Relaxed),
            })
        })
    }

    /// Run the query, decoding every row's JSON payload into a Python value.
    /// A row without its original payload raises `ConfigError`.
    fn fetch_typed<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = slf.laser.clone();
        let mut query = slf.next_call();
        query.select.payload = true;
        awaitable(py, async move {
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
    fn fetch_all_typed<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Awaitable<'py, Vec<Py<PyAny>>>> {
        let laser = slf.laser.clone();
        let mut query = slf.next_call();
        query.select.payload = true;
        let mut walk = RowWalk::new(query, slf.timeout, usize::MAX);
        awaitable(py, async move {
            let mut decoded = Vec::new();
            while let Some(row) = walk.next(&laser).await.map_err(to_pyerr)? {
                decoded.push(Python::attach(|py| {
                    decode_row(py, row.fields(), row.row())
                })?);
            }
            Ok(decoded)
        })
    }

    /// Run the query capped at one row, decoding its JSON payload, or `None`
    /// when nothing matches.
    fn fetch_one<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = slf.laser.clone();
        let mut query = slf.next_call();
        query.select.payload = true;
        query.page.limit = 1;
        awaitable(py, async move {
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
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
        codec: Py<PyAny>,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = slf.laser.clone();
        let mut query = slf.next_call();
        query.select.payload = true;
        awaitable(py, async move {
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
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
        codec: Py<PyAny>,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = slf.laser.clone();
        let mut query = slf.next_call();
        query.select.payload = true;
        query.page.limit = 1;
        awaitable(py, async move {
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
    fn rows(mut slf: PyRefMut<'_, Self>) -> PyResult<PyQueryRows> {
        let cap = slf.row_ceiling("rows")?;
        let walk = RowWalk::new(slf.next_call(), slf.timeout, cap);
        Ok(PyQueryRows {
            laser: slf.laser.clone(),
            frontier_moved: walk.frontier_moved.clone(),
            walk: Arc::new(AsyncMutex::new(walk)),
        })
    }

    /// Like `rows` but each yield is the row's JSON payload decoded into a
    /// Python value, under the same explicit `max_rows` ceiling. The payload
    /// is requested automatically, and the publisher must have inlined it: a
    /// row without it raises `ConfigError`.
    fn rows_typed(mut slf: PyRefMut<'_, Self>) -> PyResult<PyTypedQueryRows> {
        let cap = slf.row_ceiling("rows_typed")?;
        let mut query = slf.next_call();
        query.select.payload = true;
        let walk = RowWalk::new(query, slf.timeout, cap);
        Ok(PyTypedQueryRows {
            laser: slf.laser.clone(),
            frontier_moved: walk.frontier_moved.clone(),
            walk: Arc::new(AsyncMutex::new(walk)),
        })
    }

    /// The query this request built, as the dict `Laser.execute_query` takes.
    // A Python method cannot consume its receiver, so this keeps the Rust
    // name and borrows.
    #[allow(clippy::wrong_self_convention)]
    fn into_query(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.stamped())
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

    /// The page request dict: `limit`, `offset`, `want_total`.
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

    /// The read-consistency level: 'eventual' or 'read_your_writes'.
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

/// Whether a mutation with this result code name was accepted with an
/// outcome that is still unknown. Only the same operation identity may repeat it.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn result_code_is_pending(code: &str) -> PyResult<bool> {
    Ok(parse_result_code(code)?.is_pending())
}

/// Whether this result code name means the node's own managed backend did not answer.
/// The request may still apply, so only the same operation identity repeats it.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn result_code_is_backend_unavailable(code: &str) -> PyResult<bool> {
    Ok(parse_result_code(code)?.is_backend_unavailable())
}

/// Whether a failure with this result code name could not be served right
/// now: the retryable codes other than a lagging read model.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn result_code_is_unavailable(code: &str) -> PyResult<bool> {
    Ok(parse_result_code(code)?.is_unavailable())
}

/// Whether a failure with this result code name refused the caller's
/// credential or grant.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn result_code_is_permission_denied(code: &str) -> PyResult<bool> {
    Ok(parse_result_code(code)?.is_permission_denied())
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
    frontier_moved: Arc<AtomicBool>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQueryRows {
    /// The next row bound to its fields, or `None` at the ceiling or after the
    /// last page.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Option<PyQueryRow>>> {
        let laser = self.laser.clone();
        let walk = self.walk.clone();
        awaitable(py, async move {
            let row = walk.lock().await.next(&laser).await.map_err(to_pyerr)?;
            Ok(row.map(PyQueryRow::from))
        })
    }

    /// Whether a page after the first was served at another frontier, so rows
    /// may repeat or be missing.
    #[getter]
    fn frontier_moved(&self) -> bool {
        self.frontier_moved.load(Ordering::Relaxed)
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyQueryRow>> {
        let laser = self.laser.clone();
        let walk = self.walk.clone();
        awaitable(py, async move {
            match walk.lock().await.next(&laser).await.map_err(to_pyerr)? {
                Some(row) => Ok(PyQueryRow::from(row)),
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
    frontier_moved: Arc<AtomicBool>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTypedQueryRows {
    /// The next decoded payload, or `None` at the ceiling or after the last
    /// page. A payload that is JSON `null` also reads as `None`, and
    /// `async for` tells the two apart.
    fn next<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.laser.clone();
        let walk = self.walk.clone();
        awaitable(py, async move {
            match walk.lock().await.next(&laser).await.map_err(to_pyerr)? {
                Some(row) => Python::attach(|py| decode_row(py, row.fields(), row.row())),
                None => Ok(Python::attach(|py| py.None())),
            }
        })
    }

    /// Whether a page after the first was served at another frontier, so rows
    /// may repeat or be missing.
    #[getter]
    fn frontier_moved(&self) -> bool {
        self.frontier_moved.load(Ordering::Relaxed)
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.laser.clone();
        let walk = self.walk.clone();
        awaitable(py, async move {
            match walk.lock().await.next(&laser).await.map_err(to_pyerr)? {
                Some(row) => Python::attach(|py| decode_row(py, row.fields(), row.row())),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }
}

// The SDK's bounded page walk behind `rows()`: pages fetch on demand, an empty
// page or `has_more = false` ends it, aggregate and vector queries are single
// page, and the walk moves its offset past the rows it read.
struct RowWalk {
    query: Query,
    timeout: Option<Duration>,
    pages_sent: usize,
    remaining: usize,
    finished: bool,
    single_page: bool,
    buffer: std::vec::IntoIter<QueryRow>,
    // The first page's frontier, which every later page is compared with.
    frontier: Option<Vec<QueryFrontier>>,
    frontier_moved: Arc<AtomicBool>,
}

impl RowWalk {
    fn new(query: Query, timeout: Option<Duration>, max_rows: usize) -> Self {
        let single_page = query.aggregate.is_some() || query.vector.is_some();
        Self {
            query,
            timeout,
            pages_sent: 0,
            remaining: max_rows,
            finished: false,
            single_page,
            buffer: Vec::new().into_iter(),
            frontier: None,
            frontier_moved: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn next(&mut self, laser: &Laser) -> Result<Option<QueryRow>, LaserError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let row = match self.buffered() {
            Some(row) => Some(row),
            None if self.finished => None,
            None => {
                let page = laser.execute_query(self.page_query()).await?;
                self.absorb(page);
                self.buffered()
            }
        };
        if row.is_some() {
            self.remaining -= 1;
        }
        Ok(row)
    }

    fn buffered(&mut self) -> Option<QueryRow> {
        self.buffer.next()
    }

    // Take a fetched page: move the offset past its rows, compare its frontier
    // with the first page's, and buffer its rows bound to its fields.
    fn absorb(&mut self, page: QueryResult) {
        let fetched = page.rows.len();
        self.finished = self.single_page || fetched == 0 || !page.page.has_more;
        self.query.page.offset = Some(
            self.query
                .page
                .offset
                .unwrap_or(0)
                .saturating_add(u64::try_from(fetched).unwrap_or(u64::MAX)),
        );
        match &self.frontier {
            None => self.frontier = Some(page.context.frontier),
            Some(first)
                if first.len() != page.context.frontier.len()
                    || page
                        .context
                        .frontier
                        .iter()
                        .any(|position| !first.contains(position)) =>
            {
                self.frontier_moved.store(true, Ordering::Relaxed);
            }
            Some(_) => {}
        }
        self.buffer = QueryRow::from_page(page.fields, page.rows).into_iter();
    }

    // The request for the next page: its own deadline counted from now under
    // a relative budget, and a fresh execution id after the first page.
    fn page_query(&mut self) -> Query {
        let mut query = self.query.clone();
        if let Some(timeout) = self.timeout {
            query.deadline_micros = deadline_from_now(timeout);
        }
        if self.pages_sent > 0 {
            query.execution_id = QueryExecutionId::mint();
        }
        self.pages_sent += 1;
        query
    }
}

fn deadline_from_now(timeout: Duration) -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .saturating_add(timeout);
    u64::try_from(now.as_micros()).unwrap_or(u64::MAX)
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
        (laser, RowWalk::new(query, None, max_rows))
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
    fn given_walked_pages_when_read_then_should_bind_each_row_to_its_fields_and_report_a_moved_frontier()
     {
        let (_, mut walk) = walk(10);
        walk.absorb(walked_page(1, 5, true));
        let first = walk.buffered().expect("the first page holds a row");
        assert_eq!(first.value_text("id").as_deref(), Some("o-1"));
        assert!(!walk.frontier_moved.load(Ordering::Relaxed));
        walk.absorb(walked_page(2, 5, true));
        assert!(!walk.frontier_moved.load(Ordering::Relaxed));
        walk.absorb(walked_page(3, 6, false));
        assert!(walk.frontier_moved.load(Ordering::Relaxed));
        assert!(walk.finished);
        Python::initialize();
        Python::attach(|py| {
            let row = Bound::new(py, PyQueryRow::from(first)).expect("the row binds");
            let text = |key: Bound<'_, PyAny>| {
                row.get_item(key)
                    .and_then(|value| value.get_item("value"))
                    .and_then(|value| value.extract::<String>())
            };
            assert_eq!(
                text("id".into_pyobject(py).expect("key").into_any()).expect("by name"),
                "o-1"
            );
            assert_eq!(
                text(0_i64.into_pyobject(py).expect("index").into_any()).expect("by position"),
                "o-1"
            );
            assert!(
                row.get_item("absent")
                    .is_err_and(|error| error.is_instance_of::<pyo3::exceptions::PyKeyError>(py))
            );
            assert_eq!(row.len().expect("length"), 1);
            let keys: Vec<String> = row
                .call_method0("keys")
                .and_then(|keys| keys.extract())
                .expect("keys");
            assert_eq!(keys, vec!["id".to_owned()]);
        });
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

    // One page holding the row `o-<row>` with `has_more`, served at the applied
    // offset `applied` of `shop/orders` partition 0.
    fn walked_page(row: u32, applied: u64, has_more: bool) -> QueryResult {
        QueryResult {
            fields: vec![LogicalField {
                id: 1,
                name: "id".to_owned(),
                required: false,
                field_type: LogicalType::String,
                doc: None,
            }],
            rows: vec![self::row(vec![TypedValue::String(format!("o-{row}"))])],
            page: laser_sdk::query::Page {
                offset: Some(u64::from(row) - 1),
                limit: 1,
                total: None,
                has_more,
            },
            context: laser_sdk::query::QueryContext {
                execution_id: QueryExecutionId::from_u128(1),
                engine: laser_sdk::query::QueryEngine {
                    name: "embedded".to_owned(),
                    version: "1".to_owned(),
                    dialect: None,
                },
                resolved_target: laser_sdk::query::ResolvedQueryTarget::Operational {
                    index: "readings".to_owned(),
                    backend_resource_id: laser_sdk::wire::destination::BackendResourceId::from_u128(
                        1,
                    ),
                    backend_generation: 1,
                    runtime_configuration_revision: 1,
                },
                requested_consistency: Consistency::Eventual,
                delivered_consistency: Consistency::Eventual,
                truncated: false,
                elapsed_micros: 1,
                scanned_bytes: 0,
                produced_bytes: 0,
                row_count: 1,
                frontier: vec![QueryFrontier {
                    stream: "shop".to_owned(),
                    topic: "orders".to_owned(),
                    partition_id: 0,
                    topic_created_at_micros: None,
                    applied_offset: applied,
                }],
            },
        }
    }
}

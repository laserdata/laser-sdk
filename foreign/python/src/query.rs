use crate::client::PyLaser;
use crate::convert::{json_to_py, py_to_de, py_to_value, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::laser::Laser;
use laser_sdk::query::{
    AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, Filter, KeyMatch, ProjectionBinding,
    Query, QueryResult, RawSql, Row, Sort, SourceSelector, VectorQuery, Window,
};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::BTreeMap;

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
    /// `fetch_one`). Query is a managed feature: against raw Apache Iggy it raises
    /// `UnsupportedError`.
    fn query(&self, index: String) -> PyQuery {
        PyQuery::new(self.inner.clone(), index)
    }

    /// Register a projection from a Python dict matching the projection schema.
    /// Applied asynchronously by the managed host (202-accepted): poll
    /// `get_projection(id)` to observe the apply.
    fn register_projection<'py>(
        &self,
        py: Python<'py>,
        projection: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let projection = py_to_de(projection)?;
        future_into_py(py, async move {
            laser
                .projections()
                .register(projection)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Drop a projection by id. The managed host stops applying it. Existing rows stay.
    fn drop_projection<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            laser.projections().drop(id).await.map_err(to_pyerr)
        })
    }

    /// Read one projection's details by id, or `None` when no projection has it.
    fn get_projection<'py>(&self, py: Python<'py>, id: String) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let info = laser.projections().get(id).await.map_err(to_pyerr)?;
            Python::attach(|py| match info {
                Some(info) => ser_to_py(py, &info),
                None => Ok(py.None()),
            })
        })
    }

    /// List projections, optionally narrowed by topic / name substring / id prefix.
    #[pyo3(signature = (*, topic=None, name_contains=None, id_prefix=None))]
    fn list_projections<'py>(
        &self,
        py: Python<'py>,
        topic: Option<String>,
        name_contains: Option<String>,
        id_prefix: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let mut request = laser.projections().list();
            if let Some(topic) = topic {
                request = request.for_topic(topic);
            }
            if let Some(name_contains) = name_contains {
                request = request.name_contains(name_contains);
            }
            if let Some(id_prefix) = id_prefix {
                request = request.id_prefix(id_prefix);
            }
            let list = request.fetch().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &list))
        })
    }

    /// Apply a projection binding from a dict, routing a (stream, topic) source
    /// into registered projections.
    fn apply_binding<'py>(
        &self,
        py: Python<'py>,
        binding: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let binding: ProjectionBinding = py_to_de(binding)?;
        future_into_py(py, async move {
            laser.bindings().apply(binding).await.map_err(to_pyerr)
        })
    }

    /// Remove a binding for `source` (a {"stream","topic"} dict), optionally
    /// scoped to one `projection_ref`.
    #[pyo3(signature = (source, *, projection_ref=None))]
    fn remove_binding<'py>(
        &self,
        py: Python<'py>,
        source: &Bound<'_, PyAny>,
        projection_ref: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source: SourceSelector = py_to_de(source)?;
        future_into_py(py, async move {
            laser
                .bindings()
                .remove(source, projection_ref)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Register a writer schema (Avro / Protobuf) from a source dict. Synchronous:
    /// returns the managed-allocated schema id.
    #[pyo3(signature = (source, *, name=None, version=None))]
    fn register_schema<'py>(
        &self,
        py: Python<'py>,
        source: &Bound<'_, PyAny>,
        name: Option<String>,
        version: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = py_to_de(source)?;
        future_into_py(py, async move {
            let schemas = laser.schemas();
            let mut request = schemas.register(source);
            if let Some(name) = name {
                request = request.name(name);
            }
            if let Some(version) = version {
                request = request.version(version);
            }
            request.send().await.map_err(to_pyerr)
        })
    }

    /// Drop the writer schema at `id` (tombstone: existing records keep decoding).
    fn drop_schema<'py>(&self, py: Python<'py>, id: u32) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            laser.schemas().drop(id).await.map_err(to_pyerr)
        })
    }

    /// Read the writer schema at `id`, or `None` when the id is free.
    fn get_schema<'py>(&self, py: Python<'py>, id: u32) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let info = laser.schemas().get(id).await.map_err(to_pyerr)?;
            Python::attach(|py| match info {
                Some(info) => ser_to_py(py, &info),
                None => Ok(py.None()),
            })
        })
    }

    /// List every known writer schema (active and tombstoned).
    fn list_schemas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let list = laser.schemas().list().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &list))
        })
    }
}

/// One materialized query row: indexed fields, ride-along metadata, log position,
/// optional inlined payload, and vector score.
#[gen_stub_pyclass]
#[pyclass(name = "Row", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyRow {
    #[pyo3(get)]
    pub headers: BTreeMap<String, String>,
    #[pyo3(get)]
    pub metadata: BTreeMap<String, String>,
    #[pyo3(get)]
    pub partition: Option<u32>,
    #[pyo3(get)]
    pub offset: Option<u64>,
    #[pyo3(get)]
    pub payload: Option<Vec<u8>>,
    #[pyo3(get)]
    pub score: Option<f32>,
}

impl From<Row> for PyRow {
    fn from(row: Row) -> Self {
        Self {
            headers: row.headers,
            metadata: row.metadata,
            partition: row.partition,
            offset: row.offset,
            payload: row.payload,
            score: row.score,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRow {
    /// Decode the inlined payload as JSON into a Python value, or `None` when the
    /// row carries no payload.
    fn json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.payload {
            Some(bytes) => {
                let value: serde_json::Value = serde_json::from_slice(bytes)
                    .map_err(|error| crate::errors::CodecError::new_err(error.to_string()))?;
                json_to_py(py, &value)
            }
            None => Ok(py.None()),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Row(headers={} fields, partition={:?}, offset={:?}, has_payload={}, score={:?})",
            self.headers.len(),
            self.partition,
            self.offset,
            self.payload.is_some(),
            self.score
        )
    }
}

/// A page of query rows plus pagination metadata.
#[gen_stub_pyclass]
#[pyclass(name = "QueryResult", frozen)]
pub struct PyQueryResult {
    #[pyo3(get)]
    pub rows: Vec<PyRow>,
    #[pyo3(get)]
    pub offset: usize,
    #[pyo3(get)]
    pub limit: usize,
    #[pyo3(get)]
    pub total: usize,
    #[pyo3(get)]
    pub has_more: bool,
}

impl From<QueryResult> for PyQueryResult {
    fn from(result: QueryResult) -> Self {
        Self {
            rows: result.rows.into_iter().map(PyRow::from).collect(),
            offset: result.page.offset,
            limit: result.page.limit,
            total: result.page.total,
            has_more: result.page.has_more,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQueryResult {
    fn __len__(&self) -> usize {
        self.rows.len()
    }
}

/// Fluent query builder over a materialized index. Mutates an owned `Query` and
/// executes it through the managed query command at a terminal.
#[gen_stub_pyclass]
#[pyclass(name = "QueryRequest")]
pub struct PyQuery {
    laser: Laser,
    query: Query,
}

impl PyQuery {
    fn new(laser: Laser, index: String) -> Self {
        // The wire builder defaults the page size to 50, matching the Rust SDK's
        // QueryRequest (a serde-default 0 would mean "a full page" managed-side).
        let query = Query::builder().index(index).build();
        Self { laser, query }
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
        value: String,
    ) -> PyRefMut<'py, Self> {
        slf.query.by_key.push(KeyMatch::new(field, value));
        slf
    }

    /// Resolve against a fork's copy-on-write view instead of the trunk.
    fn fork<'py>(mut slf: PyRefMut<'py, Self>, fork_id: String) -> PyRefMut<'py, Self> {
        slf.query.fork = Some(fork_id);
        slf
    }

    fn filter_eq<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Eq, value));
        Ok(slf)
    }

    fn filter_ne<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Ne, value));
        Ok(slf)
    }

    fn filter_gt<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Gt, value));
        Ok(slf)
    }

    fn filter_gte<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Gte, value));
        Ok(slf)
    }

    fn filter_lt<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Lt, value));
        Ok(slf)
    }

    fn filter_lte<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_value(value)?;
        slf.and_filter(Filter::pred(field, CmpOp::Lte, value));
        Ok(slf)
    }

    fn filter_in<'py>(
        mut slf: PyRefMut<'py, Self>,
        field: String,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let value = py_to_value(values)?;
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

    /// Filter rows whose timestamp (epoch micros) falls in [start, end].
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
        slf.query.limit = n;
        slf
    }

    fn offset(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.query.offset = n;
        slf
    }

    /// Return the opaque payload bytes on each row.
    fn with_payload(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.query.select.payload = true;
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

    /// Raw-SQL escape hatch (single read-only SELECT, SQL backends only).
    fn raw_sql<'py>(mut slf: PyRefMut<'py, Self>, sql: String) -> PyRefMut<'py, Self> {
        slf.query.raw_sql = Some(RawSql {
            sql,
            params: Vec::new(),
        });
        slf
    }

    /// Approximate nearest-neighbour search on `field` (default "embedding").
    #[pyo3(signature = (embedding, top_k, *, field=None))]
    fn nearest<'py>(
        mut slf: PyRefMut<'py, Self>,
        embedding: Vec<f32>,
        top_k: usize,
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
        let query = self.query.clone();
        future_into_py(py, async move {
            let rows = collect_all(&laser, query).await.map_err(to_pyerr)?;
            Ok(rows.into_iter().map(PyRow::from).collect::<Vec<_>>())
        })
    }

    /// Run the query, decoding every row's JSON payload into a Python value.
    fn fetch_typed<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let mut query = self.query.clone();
        query.select.payload = true;
        future_into_py(py, async move {
            let result = laser.execute_query(query).await.map_err(to_pyerr)?;
            decode_rows_json(result.rows)
        })
    }

    /// Run the query capped at one row, decoding its JSON payload, or `None`.
    fn fetch_one<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let mut query = self.query.clone();
        query.select.payload = true;
        query.limit = 1;
        future_into_py(py, async move {
            let result = laser.execute_query(query).await.map_err(to_pyerr)?;
            Python::attach(|py| match result.rows.into_iter().next() {
                Some(row) => decode_one_json(py, &row),
                None => Ok(py.None()),
            })
        })
    }

    /// The raw query as a dict (debugging).
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.query)
    }
}

fn agg_call(func: AggFunc, field: Option<String>, arg: Option<f64>, alias: &str) -> AggCall {
    AggCall {
        func,
        field,
        arg,
        alias: alias.to_owned(),
    }
}

// Walk every page of `query`, mirroring the SDK's `QueryStream`: aggregate /
// vector queries are single-page, everything else advances `offset` until the
// page reports no more (or comes back empty).
async fn collect_all(laser: &Laser, mut query: Query) -> Result<Vec<Row>, laser_sdk::LaserError> {
    if query.limit == 0 {
        query.limit = 100;
    }
    let single_page = query.aggregate.is_some() || query.vector.is_some();
    let mut rows = Vec::new();
    loop {
        let page = laser.execute_query(query.clone()).await?;
        let fetched = page.rows.len();
        rows.extend(page.rows);
        let done = single_page || fetched == 0 || !page.page.has_more;
        if done {
            break;
        }
        query.offset = query.offset.saturating_add(fetched);
    }
    Ok(rows)
}

fn decode_rows_json(rows: Vec<Row>) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let mut decoded = Vec::with_capacity(rows.len());
        for row in &rows {
            decoded.push(decode_one_json(py, row)?);
        }
        Ok(decoded.into_pyobject(py)?.unbind().into_any())
    })
}

fn decode_one_json(py: Python<'_>, row: &Row) -> PyResult<Py<PyAny>> {
    match &row.payload {
        Some(bytes) => {
            let value: serde_json::Value = serde_json::from_slice(bytes)
                .map_err(|error| crate::errors::CodecError::new_err(error.to_string()))?;
            json_to_py(py, &value)
        }
        None => Ok(py.None()),
    }
}

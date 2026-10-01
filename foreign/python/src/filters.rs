use crate::async_bridge::{Undelivered, future_into_py, future_into_py_returning};
use crate::client::PyLaser;
use crate::convert::{duration_seconds, payload_bytes, py_to_de, py_to_typed_value, ser_to_py};
use crate::errors::{ConfigError, InvalidError, to_pyerr};
use crate::transport::PyConsumerMessage;
use laser_sdk::filters::{
    Coerce, ConsumerFilter, FaultPolicy, FilterExpr, FilterGroupRef, FilterHeader, FilterRef,
    FilterState, FilteredReader, FilteredStart, HeaderScalar, MatchedPage, MatchedRecord, ReadMode,
    RecordPolicy, TextMatch, TimestampFormat,
};
use laser_sdk::laser::Laser;
use laser_sdk::query::CmpOp;
use laser_sdk::wire::filter::FieldPath;
use laser_sdk::wire::filter::FilterMutation;
use laser_sdk::wire::filter::eval::{CompiledFilter, DecodeLimits, FilterRecord, HeaderRef};
use laser_sdk::wire::schema::Digest32;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as SyncMutex, OnceLock};
use std::time::Duration;
use tokio::sync::Mutex;

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Server-side consumer filters: filtered readers, previews, sample tests,
    /// and the saved-filter catalog. Native reads need a server that serves
    /// consumer filters. The catalog also needs a managed plane. Either missing
    /// raises `UnsupportedError`.
    fn filters(&self) -> PyFilters {
        PyFilters {
            laser: self.inner.clone(),
        }
    }
}

/// One predicate or a composition of predicates over a record. Build it with
/// the static constructors: `FilterExpr.pred("after.mode", "eq", "safe")`.
#[gen_stub_pyclass]
#[pyclass(name = "FilterExpr", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyFilterExpr {
    inner: FilterExpr,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFilterExpr {
    /// Every child matches.
    #[staticmethod]
    fn all(children: Vec<PyFilterExpr>) -> Self {
        Self {
            inner: FilterExpr::all(children.into_iter().map(|child| child.inner)),
        }
    }

    /// At least one child matches.
    #[staticmethod]
    fn any(children: Vec<PyFilterExpr>) -> Self {
        Self {
            inner: FilterExpr::any(children.into_iter().map(|child| child.inner)),
        }
    }

    /// The child does not match. Unknown stays unknown.
    #[staticmethod]
    fn negate(child: PyFilterExpr) -> Self {
        Self {
            inner: FilterExpr::negate(child.inner),
        }
    }

    /// Compare a payload field (`a.b[0]`) with `op` (`eq`, `ne`, `lt`, `lte`,
    /// `gt`, `gte`, `in`, `contains`, `prefix`) against `value`.
    #[staticmethod]
    fn pred(field: String, op: String, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: FilterExpr::pred(field, cmp_op(&op)?, py_to_typed_value(value)?),
        })
    }

    /// Compare after an explicit coercion: `number`, `rfc3339`,
    /// `epoch_seconds`, `epoch_millis`, or `epoch_micros`.
    #[staticmethod]
    fn pred_as(
        field: String,
        op: String,
        value: &Bound<'_, PyAny>,
        coerce: String,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: FilterExpr::pred_as(
                field,
                cmp_op(&op)?,
                py_to_typed_value(value)?,
                coercion(&coerce)?,
            ),
        })
    }

    /// The payload path exists. An explicit `null` counts as present.
    #[staticmethod]
    fn present(path: &str) -> PyResult<Self> {
        Ok(Self {
            inner: FilterExpr::Present(field_path(path)?),
        })
    }

    /// The payload path does not exist.
    #[staticmethod]
    fn absent(path: &str) -> PyResult<Self> {
        Ok(Self {
            inner: FilterExpr::Absent(field_path(path)?),
        })
    }

    /// Compare one typed user header, keyed exactly.
    #[staticmethod]
    fn header(key: String, op: String, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: FilterExpr::header(key, cmp_op(&op)?, py_to_typed_value(value)?),
        })
    }

    /// Match a text payload field: `kind` is `equals`, `prefix`, `suffix`,
    /// `contains`, `glob`, or `regex`. A value of another type is a type
    /// mismatch, which follows the filter's mismatch policy.
    #[staticmethod]
    #[pyo3(signature = (field, kind, pattern, case_insensitive=false))]
    fn text(field: String, kind: &str, pattern: String, case_insensitive: bool) -> PyResult<Self> {
        let expr = FilterExpr::text(field, text_match(kind)?, pattern);
        Ok(Self {
            inner: if case_insensitive {
                expr.case_insensitive()
            } else {
                expr
            },
        })
    }

    /// Match one text user header, keyed exactly, the way `text` matches a field.
    #[staticmethod]
    #[pyo3(signature = (key, kind, pattern, case_insensitive=false))]
    fn header_text(
        key: String,
        kind: &str,
        pattern: String,
        case_insensitive: bool,
    ) -> PyResult<Self> {
        let expr = FilterExpr::header_text(key, text_match(kind)?, pattern);
        Ok(Self {
            inner: if case_insensitive {
                expr.case_insensitive()
            } else {
                expr
            },
        })
    }

    /// Rebuild an expression from its wire dict.
    #[staticmethod]
    fn from_dict(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: py_to_de(value)?,
        })
    }

    /// The expression as its wire dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner)
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

/// One consumer filter: an expression, the payload codec, and the fault
/// policy. Its `digest` identifies exactly these semantics.
#[gen_stub_pyclass]
#[pyclass(name = "ConsumerFilter", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyConsumerFilter {
    inner: ConsumerFilter,
    // Compiled on the first local evaluation and reused after it.
    compiled: OnceLock<Arc<CompiledFilter>>,
}

impl From<ConsumerFilter> for PyConsumerFilter {
    fn from(inner: ConsumerFilter) -> Self {
        Self {
            inner,
            compiled: OnceLock::new(),
        }
    }
}

impl PyConsumerFilter {
    fn compiled(&self, schemas: Option<&Bound<'_, PyAny>>) -> PyResult<Arc<CompiledFilter>> {
        if let Some(schemas) = schemas {
            let schemas: Vec<laser_sdk::wire::control::SchemaDef> = py_to_de(schemas)?;
            return Ok(Arc::new(
                CompiledFilter::compile_with_schemas(&self.inner, &schemas)
                    .map_err(|error| InvalidError::new_err(error.to_string()))?,
            ));
        }
        if let Some(compiled) = self.compiled.get() {
            return Ok(Arc::clone(compiled));
        }
        let compiled = Arc::new(
            CompiledFilter::compile(&self.inner)
                .map_err(|error| InvalidError::new_err(error.to_string()))?,
        );
        Ok(Arc::clone(self.compiled.get_or_init(|| compiled)))
    }
}

fn decode_limits(max_payload_bytes: Option<usize>, max_depth: Option<usize>) -> DecodeLimits {
    let defaults = DecodeLimits::default();
    DecodeLimits {
        max_payload_bytes: max_payload_bytes.unwrap_or(defaults.max_payload_bytes),
        max_depth: max_depth.unwrap_or(defaults.max_depth),
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyConsumerFilter {
    /// Decode the payload as JSON. `fault_policy` is `stop` (the default),
    /// `pass`, or `drop`.
    #[staticmethod]
    #[pyo3(signature = (expr, fault_policy="stop"))]
    fn json(expr: PyFilterExpr, fault_policy: &str) -> PyResult<Self> {
        Ok(ConsumerFilter::json(expr.inner)
            .with_fault_policy(fault(fault_policy)?)
            .into())
    }

    /// Decode a CBOR payload with bounded depth and size.
    #[staticmethod]
    #[pyo3(signature = (expr, fault_policy="stop"))]
    fn cbor(expr: PyFilterExpr, fault_policy: &str) -> PyResult<Self> {
        Ok(ConsumerFilter::cbor(expr.inner)
            .with_fault_policy(fault(fault_policy)?)
            .into())
    }

    /// Decode a raw Avro datum selected by its agdx.sid header.
    #[staticmethod]
    #[pyo3(signature = (expr, schema_refs, fault_policy="stop"))]
    fn avro(expr: PyFilterExpr, schema_refs: Vec<u32>, fault_policy: &str) -> PyResult<Self> {
        Ok(ConsumerFilter::avro(expr.inner, schema_refs)
            .with_fault_policy(fault(fault_policy)?)
            .into())
    }

    /// Decode a Protobuf message selected by its agdx.sid header.
    #[staticmethod]
    #[pyo3(signature = (expr, schema_refs, fault_policy="stop"))]
    fn protobuf(expr: PyFilterExpr, schema_refs: Vec<u32>, fault_policy: &str) -> PyResult<Self> {
        Ok(ConsumerFilter::protobuf(expr.inner, schema_refs)
            .with_fault_policy(fault(fault_policy)?)
            .into())
    }

    /// Never decode the payload. Only header predicates are allowed.
    #[staticmethod]
    #[pyo3(signature = (expr, fault_policy="stop"))]
    fn headers_only(expr: PyFilterExpr, fault_policy: &str) -> PyResult<Self> {
        Ok(ConsumerFilter::headers_only(expr.inner)
            .with_fault_policy(fault(fault_policy)?)
            .into())
    }

    /// A copy with the malformed-payload policy: stop, pass, or drop.
    fn with_fault_policy(&self, policy: &str) -> PyResult<Self> {
        Ok(self.inner.clone().with_fault_policy(fault(policy)?).into())
    }

    /// A copy whose records in another format (another `agdx.ct` codec, or a
    /// writer schema the filter does not list) are skipped (`reject`, the
    /// default) or delivered unevaluated (`pass`).
    fn with_foreign_policy(&self, policy: &str) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .with_foreign_policy(record_policy(policy)?)
            .into())
    }

    /// A copy whose records with a value of a type a predicate cannot compare
    /// are skipped (`reject`, the default) or delivered unevaluated (`pass`).
    fn with_mismatch_policy(&self, policy: &str) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .with_mismatch_policy(record_policy(policy)?)
            .into())
    }

    /// Rebuild a filter from its wire dict.
    #[staticmethod]
    fn from_dict(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(py_to_de::<ConsumerFilter>(value)?.into())
    }

    /// The filter as its wire dict.
    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner)
    }

    /// Evaluate the filter locally with the evaluator the server runs:
    /// `selected`, `rejected`, or `fault`. `headers` carries typed user headers.
    #[pyo3(signature = (payload, headers=None, *, schemas=None, max_payload_bytes=None, max_depth=None))]
    fn evaluate(
        &self,
        py: Python<'_>,
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
        schemas: Option<&Bound<'_, PyAny>>,
        max_payload_bytes: Option<usize>,
        max_depth: Option<usize>,
    ) -> PyResult<String> {
        let compiled = self.compiled(schemas)?;
        let payload = payload_bytes(payload)?;
        let headers = filter_headers(headers)?;
        let headers: Vec<HeaderRef<'_>> = headers.iter().map(HeaderRef::from).collect();
        let limits = decode_limits(max_payload_bytes, max_depth);
        let verdict = py.detach(|| {
            compiled.evaluate(
                &FilterRecord {
                    payload: &payload,
                    headers: &headers,
                },
                &limits,
            )
        });
        Ok(verdict.to_string())
    }

    /// Evaluate the filter locally and explain the verdict as the server
    /// would: a dict with `verdict`, an optional `fault`, and the `root`
    /// explanation tree with one node per predicate.
    #[pyo3(signature = (payload, headers=None, *, schemas=None, max_payload_bytes=None, max_depth=None))]
    fn explain(
        &self,
        py: Python<'_>,
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
        schemas: Option<&Bound<'_, PyAny>>,
        max_payload_bytes: Option<usize>,
        max_depth: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        let compiled = self.compiled(schemas)?;
        let payload = payload_bytes(payload)?;
        let headers = filter_headers(headers)?;
        let headers: Vec<HeaderRef<'_>> = headers.iter().map(HeaderRef::from).collect();
        let limits = decode_limits(max_payload_bytes, max_depth);
        let explanation = py.detach(|| {
            compiled.explain(
                &FilterRecord {
                    payload: &payload,
                    headers: &headers,
                },
                &limits,
            )
        });
        ser_to_py(py, &explanation)
    }

    /// The SHA-256 digest of the filter's semantics.
    #[getter]
    fn digest<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.inner.digest().as_bytes())
    }

    #[getter]
    fn codec(&self) -> String {
        self.inner.codec.to_string()
    }

    #[getter]
    fn fault_policy(&self) -> String {
        self.inner.fault_policy.to_string()
    }

    #[getter]
    fn expr(&self) -> PyFilterExpr {
        PyFilterExpr {
            inner: self.inner.expr.clone(),
        }
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

/// Consumer filters on one connection. Build it with `Laser.filters()`.
/// Catalog replies are dicts in the wire shape.
#[gen_stub_pyclass]
#[pyclass(name = "Filters", frozen)]
pub struct PyFilters {
    laser: Laser,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFilters {
    /// A filtered reader over `stream` / `topic`. Name an independent
    /// `consumer` (every partition unless `partitions` is given) or a
    /// consumer `group` or numeric `group_id` (its assigned partitions). Pass
    /// an inline `filter`, a
    /// saved `filter_id` and `revision`, or nothing for a group's binding.
    /// `start` is `next` (the default), `first`, or `last`, or use
    /// `start_offset` / `start_timestamp_micros`.
    /// `read_mode` is primary or local. Local reads cannot acknowledge.
    /// `local_guard` checks delivered records against the filter locally.
    /// `idle_interval` is seconds, finite and non-negative.
    /// `count` bounds records per page, default 100.
    /// `max_reply_bytes` bounds record bytes per page.
    /// `max_unacked_pages` bounds outstanding pages per partition, default 1024.
    /// A numeric `group_id` identifies an existing group in this source incarnation.
    #[pyo3(signature = (
        stream,
        topic,
        *,
        consumer=None,
        group=None,
        group_id=None,
        partitions=None,
        filter=None,
        filter_id=None,
        revision=None,
        start="next",
        start_offset=None,
        start_timestamp_micros=None,
        count=None,
        max_reply_bytes=None,
        max_unacked_pages=None,
        read_mode="primary",
        local_guard=false,
        idle_interval=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn reader<'py>(
        &self,
        py: Python<'py>,
        stream: String,
        topic: String,
        consumer: Option<String>,
        group: Option<String>,
        group_id: Option<u64>,
        partitions: Option<Vec<u32>>,
        filter: Option<PyConsumerFilter>,
        filter_id: Option<u32>,
        revision: Option<u32>,
        start: &str,
        start_offset: Option<u64>,
        start_timestamp_micros: Option<u64>,
        count: Option<u32>,
        max_reply_bytes: Option<u32>,
        max_unacked_pages: Option<usize>,
        read_mode: &str,
        local_guard: bool,
        idle_interval: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let filter = filter_ref(filter, filter_id, revision)?;
        let start = filtered_start(start, start_offset, start_timestamp_micros)?;
        let read_mode = match read_mode {
            "primary" => ReadMode::Primary,
            "local" => ReadMode::Local,
            other => {
                return Err(InvalidError::new_err(format!(
                    "read_mode must be `primary` or `local`, got `{other}`"
                )));
            }
        };
        let idle_interval = idle_interval
            .map(|seconds| duration_seconds(seconds, "idle_interval"))
            .transpose()?;
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let filters = laser.filters();
            let mut builder = filters
                .reader(stream, topic)
                .start(start)
                .read_mode(read_mode)
                .local_guard(local_guard);
            builder = match (consumer, group, group_id) {
                (Some(consumer), None, None) => builder.consumer(consumer),
                (None, Some(group), None) => builder.group(group),
                (None, None, Some(group_id)) => builder.group_id(group_id),
                _ => {
                    return Err(ConfigError::new_err(
                        "name exactly one of `consumer`, `group`, or `group_id`",
                    ));
                }
            };
            for partition_id in partitions.unwrap_or_default() {
                builder = builder.partition(partition_id);
            }
            if let Some(filter) = filter {
                builder = builder.filter(filter);
            }
            if let Some(count) = count {
                builder = builder.count(count);
            }
            if let Some(max_reply_bytes) = max_reply_bytes {
                builder = builder.max_reply_bytes(max_reply_bytes);
            }
            if let Some(pages) = max_unacked_pages {
                builder = builder.max_unacked_pages(pages);
            }
            if let Some(idle_interval) = idle_interval {
                builder = builder.idle_interval(idle_interval);
            }
            let reader = builder.build().await.map_err(to_pyerr)?;
            let examined = Arc::new(AtomicU64::new(0));
            Ok(PyFilteredReader {
                examined: examined.clone(),
                state: Arc::new(Mutex::new(ReaderState {
                    idle_interval: reader.idle_interval(),
                    more: false,
                    examined,
                    reader: Some(reader),
                    buffered: VecDeque::new(),
                })),
                returned: Arc::new(SyncMutex::new(VecDeque::new())),
                returned_pages: Arc::new(SyncMutex::new(VecDeque::new())),
            })
        })
    }

    /// Validate and compile `filter` on the server without running it.
    fn validate<'py>(
        &self,
        py: Python<'py>,
        filter: PyConsumerFilter,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let validation = laser
                .filters()
                .validate(&filter.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &validation))
        })
    }

    /// Evaluate a filter against one supplied `payload` and optional typed
    /// `headers`, and explain the verdict. Nothing is read or stored.
    #[pyo3(signature = (payload, *, filter=None, filter_id=None, revision=None, headers=None))]
    fn test<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'_, PyAny>,
        filter: Option<PyConsumerFilter>,
        filter_id: Option<u32>,
        revision: Option<u32>,
        headers: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let filter = filter_ref(filter, filter_id, revision)?
            .ok_or_else(|| InvalidError::new_err("pass `filter` or `filter_id` and `revision`"))?;
        let payload = payload_bytes(payload)?;
        let headers = filter_headers(headers)?;
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let result = laser
                .filters()
                .test(filter, payload, headers)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// Preview a filter over stored records of one partition. A preview joins
    /// no group and stores no offset.
    #[pyo3(signature = (
        stream,
        topic,
        partition_id,
        *,
        filter=None,
        filter_id=None,
        revision=None,
        from_offset=0,
        max_examined=None,
        max_records=None,
        explain=false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn preview<'py>(
        &self,
        py: Python<'py>,
        stream: String,
        topic: String,
        partition_id: u32,
        filter: Option<PyConsumerFilter>,
        filter_id: Option<u32>,
        revision: Option<u32>,
        from_offset: u64,
        max_examined: Option<u32>,
        max_records: Option<u32>,
        explain: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let filter = filter_ref(filter, filter_id, revision)?
            .ok_or_else(|| InvalidError::new_err("pass `filter` or `filter_id` and `revision`"))?;
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let mut request = laser
                .filters()
                .preview(stream, topic, partition_id, filter)
                .from_offset(from_offset)
                .explain(explain);
            if let Some(max_examined) = max_examined {
                request = request.max_examined(max_examined);
            }
            if let Some(max_records) = max_records {
                request = request.max_records(max_records);
            }
            let preview = request.send().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &preview))
        })
    }

    /// One saved filter with its latest revision and bindings.
    fn get<'py>(&self, py: Python<'py>, filter_id: u32) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let detail = laser.filters().get(filter_id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &detail))
        })
    }

    /// One page of saved filters, newest first. Pass the last `id` of a page
    /// as `before_id` to read the next one stably while the catalog changes.
    #[pyo3(signature = (*, name_contains=None, state=None, before_id=None, page=0, page_size=50))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        name_contains: Option<String>,
        state: Option<String>,
        before_id: Option<u32>,
        page: u32,
        page_size: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = state
            .map(|state| {
                state.parse::<FilterState>().map_err(|_| {
                    InvalidError::new_err("state must be `active`, `archived`, or `dropped`")
                })
            })
            .transpose()?;
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let filters = laser.filters();
            let mut request = filters.list().page(page, page_size);
            if let Some(name_contains) = name_contains {
                request = request.name_contains(name_contains);
            }
            if let Some(state) = state {
                request = request.state(state);
            }
            if let Some(before_id) = before_id {
                request = request.before(before_id);
            }
            let listed = request.send().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &listed))
        })
    }

    /// One page of a filter's revisions, newest first.
    #[pyo3(signature = (filter_id, *, page=0, page_size=50))]
    fn revisions<'py>(
        &self,
        py: Python<'py>,
        filter_id: u32,
        page: u32,
        page_size: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let revisions = laser
                .filters()
                .revisions(filter_id, page, page_size)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &revisions))
        })
    }

    /// The binding of one consumer group. Raises `FilterError` with reason
    /// `not_found` when the group is unbound.
    fn binding<'py>(
        &self,
        py: Python<'py>,
        stream: String,
        topic: String,
        group: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let binding = laser
                .filters()
                .binding(FilterGroupRef {
                    stream,
                    topic,
                    group,
                })
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &binding))
        })
    }

    /// One page of bindings, optionally narrowed to one filter, one
    /// `stream`, or one `stream` and `topic`.
    #[pyo3(signature = (*, filter_id=None, stream=None, topic=None, page=0, page_size=50))]
    fn bindings<'py>(
        &self,
        py: Python<'py>,
        filter_id: Option<u32>,
        stream: Option<String>,
        topic: Option<String>,
        page: u32,
        page_size: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let bindings = laser
                .filters()
                .bindings(
                    filter_id,
                    stream.as_deref(),
                    topic.as_deref(),
                    page,
                    page_size,
                )
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &bindings))
        })
    }

    /// Apply one catalog `mutation` (a wire dict such as
    /// `{"archive": {"filter_id": 7}}`) under a fresh operation id, wait for
    /// its outcome, and return the applied result dict. A rejection raises
    /// `FilterError`, and an outcome still pending after the wait raises an
    /// error that names the operation id.
    fn apply<'py>(
        &self,
        py: Python<'py>,
        mutation: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mutation: FilterMutation = py_to_de(mutation)?;
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let result = laser.filters().apply(mutation).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// apply under a caller-chosen `operation_id`, so a
    /// caller that records the id first can resume the same mutation after a
    /// crash.
    fn apply_as<'py>(
        &self,
        py: Python<'py>,
        operation_id: u128,
        mutation: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mutation: FilterMutation = py_to_de(mutation)?;
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let result = laser
                .filters()
                .apply_as(operation_id, mutation)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// Send one catalog `mutation` (a wire dict such as
    /// `{"archive": {"filter_id": 7}}`) under a caller-chosen `operation_id`
    /// and return its outcome dict, which may be `pending`. Retrying with the
    /// same id never applies it twice.
    fn mutate<'py>(
        &self,
        py: Python<'py>,
        operation_id: u128,
        mutation: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mutation: FilterMutation = py_to_de(mutation)?;
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let outcome = laser
                .filters()
                .mutate(operation_id, mutation)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &outcome))
        })
    }

    /// Wait up to `timeout` seconds for the outcome of `operation_id`, for
    /// example the id an ambiguous mutation error names. Returns the applied
    /// result dict, raises `FilterError` for a rejection, and
    /// raises an error while it stays pending.
    #[pyo3(signature = (operation_id, *, timeout=30.0))]
    fn wait_for_outcome<'py>(
        &self,
        py: Python<'py>,
        operation_id: u128,
        timeout: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let timeout = duration_seconds(timeout, "timeout")?;
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let result = laser
                .filters()
                .wait_for_outcome(operation_id, timeout)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// The recorded outcome of the mutation `operation_id`.
    fn operation<'py>(&self, py: Python<'py>, operation_id: u128) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let outcome = laser
                .filters()
                .operation(operation_id)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &outcome))
        })
    }

    /// Save a new filter as revision 1 and wait for the applied outcome.
    /// Returns the revision dict (`filter_id`, `revision`, `digest`).
    #[pyo3(signature = (name, filter, *, description=String::new()))]
    fn register<'py>(
        &self,
        py: Python<'py>,
        name: String,
        filter: PyConsumerFilter,
        description: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let revision = laser
                .filters()
                .register(name, filter.inner, description)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &revision))
        })
    }

    /// Add a revision. `expected_revision` must still be the latest one.
    fn revise<'py>(
        &self,
        py: Python<'py>,
        filter_id: u32,
        expected_revision: u32,
        filter: PyConsumerFilter,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let revision = laser
                .filters()
                .revise(filter_id, expected_revision, filter.inner)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &revision))
        })
    }

    /// Replace a filter's description.
    fn describe<'py>(
        &self,
        py: Python<'py>,
        filter_id: u32,
        description: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            laser
                .filters()
                .describe(filter_id, description)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Hide a filter from new bindings. Existing bindings keep executing.
    fn archive<'py>(&self, py: Python<'py>, filter_id: u32) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            laser.filters().archive(filter_id).await.map_err(to_pyerr)
        })
    }

    /// Delete a filter. Raises `FilterError` with reason `conflict` while a
    /// consumer group is bound to it.
    fn delete<'py>(&self, py: Python<'py>, filter_id: u32) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            laser.filters().delete(filter_id).await.map_err(to_pyerr)
        })
    }

    /// Pin an existing consumer group to one revision. Returns the binding dict.
    fn bind<'py>(
        &self,
        py: Python<'py>,
        stream: String,
        topic: String,
        group: String,
        filter_id: u32,
        revision: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let binding = laser
                .filters()
                .bind(
                    FilterGroupRef {
                        stream,
                        topic,
                        group,
                    },
                    filter_id,
                    revision,
                )
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &binding))
        })
    }

    /// Create the consumer group if absent and bind it to a saved revision.
    /// A failed bind can leave an unbound group. Returns the binding dict.
    fn create_consumer_group<'py>(
        &self,
        py: Python<'py>,
        stream: String,
        topic: String,
        group: String,
        filter_id: u32,
        revision: u32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let binding = laser
                .filters()
                .create_consumer_group(
                    FilterGroupRef {
                        stream,
                        topic,
                        group,
                    },
                    filter_id,
                    revision,
                )
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &binding))
        })
    }

    /// Pause or resume a revision while preserving its content and digest.
    fn set_revision_enabled<'py>(
        &self,
        py: Python<'py>,
        filter_id: u32,
        revision: u32,
        enabled: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            laser
                .filters()
                .set_revision_enabled(filter_id, revision, enabled)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Release exactly the group incarnation a binding dict (from `binding` or
    /// `bindings`) names, by its stored identity and digest. A group deleted
    /// and recreated under the same name keeps its own binding.
    fn unbind_binding<'py>(
        &self,
        py: Python<'py>,
        binding: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let binding: laser_sdk::wire::filter::FilterBinding = py_to_de(binding)?;
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let released = laser
                .filters()
                .unbind_binding(binding)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &released))
        })
    }

    /// Release the currently named group, conditional on its digest.
    fn unbind<'py>(
        &self,
        py: Python<'py>,
        stream: String,
        topic: String,
        group: String,
        expected_digest: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let expected_digest = Digest32::new(expected_digest.try_into().map_err(|_| {
            InvalidError::new_err("expected_digest must be the 32-byte filter digest")
        })?);
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let binding = laser
                .filters()
                .unbind(
                    FilterGroupRef {
                        stream,
                        topic,
                        group,
                    },
                    expected_digest,
                )
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &binding))
        })
    }
}

/// Reads only the records a consumer filter selects and stores progress
/// through fenced acknowledgments. Build it with `Filters.reader`. Drive one
/// reader from one task. `async for record in reader` yields matching records
/// until the task is cancelled. A record whose `next_record` call is cancelled
/// after it was read is yielded again by the next call, so a cancellation never
/// strands a record the partition's progress waits for.
#[gen_stub_pyclass]
#[pyclass(name = "FilteredReader", frozen)]
pub struct PyFilteredReader {
    examined: Arc<AtomicU64>,
    state: Arc<Mutex<ReaderState>>,
    // Records a cancelled call read but never handed over, yielded first.
    returned: Arc<SyncMutex<VecDeque<PyMatchedRecord>>>,
    returned_pages: Arc<SyncMutex<VecDeque<Arc<SharedPage>>>>,
}

struct ReaderState {
    examined: Arc<AtomicU64>,
    reader: Option<FilteredReader>,
    buffered: VecDeque<PyMatchedRecord>,
    idle_interval: Duration,
    more: bool,
}

// A page shared by its record handles, with each record's message decoded once
// on first access.
struct SharedPage {
    page: MatchedPage,
    messages: Box<[OnceLock<Py<PyConsumerMessage>>]>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFilteredReader {
    /// The next page with at least one match. Waits while nothing is new,
    /// without holding the reader, so `ack` and `close` run meanwhile.
    fn next_page<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let returned_pages = self.returned_pages.clone();
        future_into_py_returning(
            py,
            async move {
                loop {
                    let idle_interval = {
                        let mut state = state.lock().await;
                        if let Some(page) = state.next_page(&returned_pages).await? {
                            return Ok(page);
                        }
                        if state.more {
                            Duration::ZERO
                        } else {
                            state.idle_interval
                        }
                    };
                    if idle_interval.is_zero() {
                        tokio::task::yield_now().await;
                    } else {
                        tokio::time::sleep(idle_interval).await;
                    }
                }
            },
            give_back_page(self.returned_pages.clone()),
        )
    }

    /// Read until a page matches or nothing is new. `None` when nothing is new.
    fn try_next_page<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let returned_pages = self.returned_pages.clone();
        future_into_py_returning(
            py,
            async move {
                loop {
                    {
                        let mut state = state.lock().await;
                        let page = state.next_page(&returned_pages).await?;
                        if page.is_some() || !state.more {
                            return Ok(page);
                        }
                    }
                    tokio::task::yield_now().await;
                }
            },
            give_back_page(self.returned_pages.clone()),
        )
    }

    /// One bounded round without an idle wait. Returns (page, more).
    /// Read examined_in_round() for the count of source records examined.
    fn read_round<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let returned_pages = self.returned_pages.clone();
        let recover = give_back_page(self.returned_pages.clone());
        future_into_py_returning(
            py,
            async move {
                let mut state = state.lock().await;
                let page = state.next_page(&returned_pages).await?;
                Ok((page, state.more))
            },
            Box::new(move |py, value| {
                if let Ok(page) = value.bind(py).get_item(0) {
                    recover(py, page.unbind());
                }
            }),
        )
    }

    /// Source records examined in the last bounded round, including empty pages.
    fn examined_in_round(&self) -> u64 {
        self.examined.load(Ordering::Relaxed)
    }

    /// The next matching record.
    fn next_record<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (state, returned) = (self.state.clone(), self.returned.clone());
        let returned_pages = self.returned_pages.clone();
        future_into_py_returning(
            py,
            next_record(state, returned, returned_pages),
            give_back(self.returned.clone()),
        )
    }

    /// Mark one record handled and store the progress this completes.
    fn ack<'py>(
        &self,
        py: Python<'py>,
        record: PyRef<'_, PyMatchedRecord>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let (page, index) = (record.page.clone(), record.index);
        future_into_py(py, async move {
            let mut state = state.lock().await;
            state
                .open()?
                .ack(&page.page.records[index])
                .await
                .map_err(to_pyerr)
        })
    }

    /// Mark all preceding records on this partition through `record` handled.
    /// Process the prefix first. Later records in the same page stay pending.
    fn ack_through<'py>(
        &self,
        py: Python<'py>,
        record: PyRef<'_, PyMatchedRecord>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let (page, index) = (record.page.clone(), record.index);
        future_into_py(py, async move {
            let mut state = state.lock().await;
            state
                .open()?
                .ack_through(&page.page.records[index])
                .await
                .map_err(to_pyerr)
        })
    }

    /// Mark every record of `page` handled and store the progress this
    /// completes.
    fn ack_page<'py>(
        &self,
        py: Python<'py>,
        page: PyRef<'_, PyMatchedPage>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let page = page.page.clone();
        future_into_py(py, async move {
            let mut state = state.lock().await;
            state.open()?.ack_page(&page.page).await.map_err(to_pyerr)
        })
    }

    /// The partitions this reader reads now.
    fn partitions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        future_into_py(
            py,
            async move { Ok(state.lock().await.open()?.partitions()) },
        )
    }

    /// How long this reader waits, in seconds, when nothing is new.
    fn idle_interval<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        future_into_py(py, async move {
            Ok(state.lock().await.open()?.idle_interval().as_secs_f64())
        })
    }

    /// Whether `record` was read by this reader in its current membership, so
    /// it can still be acknowledged. A rejoin retires every earlier page.
    fn owns<'py>(
        &self,
        py: Python<'py>,
        record: PyRef<'_, PyMatchedRecord>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        let page = Arc::clone(&record.page);
        future_into_py(py, async move {
            Ok(state.lock().await.open()?.owns(&page.page))
        })
    }

    /// Data connections this reader opened to partition primaries, including
    /// routes later retired. A healthy reader opens one per node and keeps it.
    fn data_connections_opened<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.state.clone();
        future_into_py(py, async move {
            Ok(state.lock().await.open()?.data_connections_opened())
        })
    }

    /// Store completed progress, leave the group, and close the data
    /// connections. Closing twice is a no-op.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (state, returned) = (self.state.clone(), self.returned.clone());
        let returned_pages = self.returned_pages.clone();
        future_into_py(py, async move {
            let taken = {
                let mut state = state.lock().await;
                if let Some(reader) = state.reader.as_mut() {
                    reader.flush_completed().await.map_err(to_pyerr)?;
                }
                state.buffered.clear();
                returned
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear();
                returned_pages
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear();
                state.reader.take()
            };
            match taken {
                Some(reader) => reader.close().await.map_err(to_pyerr),
                None => Ok(()),
            }
        })
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (state, returned) = (self.state.clone(), self.returned.clone());
        let returned_pages = self.returned_pages.clone();
        future_into_py_returning(
            py,
            async move {
                if state.lock().await.reader.is_none() {
                    return Err(PyStopAsyncIteration::new_err(()));
                }
                next_record(state, returned, returned_pages).await
            },
            give_back(self.returned.clone()),
        )
    }
}

impl ReaderState {
    async fn next_page(
        &mut self,
        returned: &SyncMutex<VecDeque<Arc<SharedPage>>>,
    ) -> PyResult<Option<PyMatchedPage>> {
        self.examined.store(0, Ordering::Relaxed);
        let reader = self.open()?;
        loop {
            let page = returned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front();
            match page {
                Some(page) if reader.owns(&page.page) => return Ok(Some(PyMatchedPage { page })),
                Some(_) => continue,
                None => break,
            }
        }
        let (page, more) = reader.read_round().await.map_err(to_pyerr)?;
        let examined = reader.examined_in_round();
        self.examined.store(examined, Ordering::Relaxed);
        self.more = more;
        Ok(page.map(|page| PyMatchedPage {
            page: SharedPage::new(page),
        }))
    }

    fn open(&mut self) -> PyResult<&mut FilteredReader> {
        self.reader
            .as_mut()
            .ok_or_else(|| ConfigError::new_err("the filtered reader is closed"))
    }

    // Records of a membership the reader has since rejoined can no longer be
    // acknowledged, so they are dropped before anything is yielded. Pages
    // enter the buffer in read order and a rejoin retires every earlier page,
    // so retired records only ever lead the buffer.
    fn drop_retired(&mut self) {
        let Some(reader) = self.reader.as_ref() else {
            return;
        };
        self.buffered
            .retain(|record| reader.owns(&record.page.page));
    }
}

impl SharedPage {
    fn new(page: MatchedPage) -> Arc<Self> {
        let messages = (0..page.records.len()).map(|_| OnceLock::new()).collect();
        Arc::new(Self { page, messages })
    }
}

// The next record: one a cancelled call gave back, then the buffer, then a new
// page. The reader is held only while it reads, never across the idle wait.
async fn next_record(
    state: Arc<Mutex<ReaderState>>,
    returned: Arc<SyncMutex<VecDeque<PyMatchedRecord>>>,
    returned_pages: Arc<SyncMutex<VecDeque<Arc<SharedPage>>>>,
) -> PyResult<PyMatchedRecord> {
    loop {
        let idle_interval = {
            let mut state = state.lock().await;
            let reader = state.open()?;
            let given_back = returned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front();
            if let Some(record) = given_back.filter(|record| reader.owns(&record.page.page)) {
                return Ok(record);
            }
            state.drop_retired();
            if let Some(record) = state.buffered.pop_front() {
                return Ok(record);
            }
            match state.next_page(&returned_pages).await? {
                Some(page) => {
                    let page = page.page;
                    state
                        .buffered
                        .extend((0..page.page.records.len()).map(|index| PyMatchedRecord {
                            page: Arc::clone(&page),
                            index,
                        }));
                    continue;
                }
                None => {
                    if state.more {
                        Duration::ZERO
                    } else {
                        state.idle_interval
                    }
                }
            }
        };
        if idle_interval.is_zero() {
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(idle_interval).await;
        }
    }
}

// Put a record whose call was cancelled after the read back in front, so the
// next call yields it again.
fn give_back(returned: Arc<SyncMutex<VecDeque<PyMatchedRecord>>>) -> Undelivered {
    Box::new(move |py, value| {
        if let Ok(record) = value.bind(py).extract::<PyRef<'_, PyMatchedRecord>>() {
            returned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push_front(PyMatchedRecord {
                    page: Arc::clone(&record.page),
                    index: record.index,
                });
        }
    })
}

fn give_back_page(returned: Arc<SyncMutex<VecDeque<Arc<SharedPage>>>>) -> Undelivered {
    Box::new(move |py, value| {
        if let Ok(page) = value.bind(py).extract::<PyRef<'_, PyMatchedPage>>() {
            returned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push_front(Arc::clone(&page.page));
        }
    })
}

/// One page of matching records from one partition.
#[gen_stub_pyclass]
#[pyclass(name = "MatchedPage", frozen)]
pub struct PyMatchedPage {
    page: Arc<SharedPage>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMatchedPage {
    #[getter]
    fn partition_id(&self) -> u32 {
        self.page.page.partition_id
    }

    #[getter]
    fn records(&self) -> Vec<PyMatchedRecord> {
        (0..self.page.page.records.len())
            .map(|index| PyMatchedRecord {
                page: Arc::clone(&self.page),
                index,
            })
            .collect()
    }

    /// Why the page stopped: `filled`, `budget`, `end_of_visible`, `fault`,
    /// or `oversized_record`.
    #[getter]
    fn stop(&self) -> String {
        self.page.page.stop.to_string()
    }

    #[getter]
    fn examined(&self) -> u32 {
        self.page.page.examined
    }

    #[getter]
    fn frontier(&self) -> u64 {
        self.page.page.frontier
    }

    #[getter]
    fn safe_ack_offset(&self) -> Option<u64> {
        self.page.page.safe_ack_offset
    }

    /// The executed filter as a dict: `digest`, and `filter_id` / `revision`
    /// for a saved one.
    #[getter]
    fn policy(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.page.page.policy)
    }

    /// The source history the page was read from, as a dict.
    #[getter]
    fn generation(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.page.page.generation)
    }
}

/// One matching record, unchanged from the stream.
#[gen_stub_pyclass]
#[pyclass(name = "MatchedRecord", frozen)]
pub struct PyMatchedRecord {
    page: Arc<SharedPage>,
    index: usize,
}

impl PyMatchedRecord {
    fn record(&self) -> &MatchedRecord {
        &self.page.page.records[self.index]
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMatchedRecord {
    #[getter]
    fn partition_id(&self) -> u32 {
        self.record().partition_id
    }

    #[getter]
    fn offset(&self) -> u64 {
        self.record().offset
    }

    /// The partition frontier observed with this record.
    #[getter]
    fn frontier(&self) -> u64 {
        self.record().frontier
    }

    /// False when a pass fault, foreign-codec, or type-mismatch policy
    /// delivered the record without evaluation.
    #[getter]
    fn evaluated(&self) -> bool {
        self.record().evaluated
    }

    /// True when a header entry or block structure is malformed. Valid
    /// headers and payload remain readable. Unknown value kinds remain raw.
    /// A truncated block has empty `message.headers`.
    #[getter]
    fn headers_malformed(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.message(py)?.borrow(py).headers_malformed)
    }

    /// The record as a `ConsumerMessage`: payload, typed headers, id, and
    /// timestamps. Decoded once, then shared.
    #[getter]
    fn message(&self, py: Python<'_>) -> PyResult<Py<PyConsumerMessage>> {
        if let Some(message) = self.page.messages[self.index].get() {
            return Ok(message.clone_ref(py));
        }
        let record = self.record();
        let message = Py::new(
            py,
            PyConsumerMessage::of(&record.message, record.partition_id, record.frontier)
                .map_err(|error| to_pyerr(error.into()))?,
        )?;
        Ok(self.page.messages[self.index]
            .get_or_init(|| message)
            .clone_ref(py))
    }
}

fn filter_ref(
    filter: Option<PyConsumerFilter>,
    filter_id: Option<u32>,
    revision: Option<u32>,
) -> PyResult<Option<FilterRef>> {
    match (filter, filter_id, revision) {
        (Some(filter), None, None) => Ok(Some(FilterRef::Inline(filter.inner))),
        (None, Some(filter_id), Some(revision)) => Ok(Some(FilterRef::Revision {
            filter_id,
            revision,
        })),
        (None, None, None) => Ok(None),
        _ => Err(InvalidError::new_err(
            "pass either `filter`, or both `filter_id` and `revision`",
        )),
    }
}

fn filtered_start(
    start: &str,
    offset: Option<u64>,
    timestamp_micros: Option<u64>,
) -> PyResult<FilteredStart> {
    match (offset, timestamp_micros) {
        (Some(offset), None) => return Ok(FilteredStart::Offset(offset)),
        (None, Some(micros)) => return Ok(FilteredStart::Timestamp(micros)),
        (Some(_), Some(_)) => {
            return Err(InvalidError::new_err(
                "pass at most one of `start_offset` and `start_timestamp_micros`",
            ));
        }
        (None, None) => {}
    }
    match start {
        "next" => Ok(FilteredStart::Next),
        "first" => Ok(FilteredStart::First),
        "last" => Ok(FilteredStart::Last),
        other => Err(InvalidError::new_err(format!(
            "start must be `next`, `first`, or `last`, got `{other}`"
        ))),
    }
}

fn cmp_op(op: &str) -> PyResult<CmpOp> {
    serde_json::from_value(serde_json::Value::String(op.to_owned())).map_err(|_| {
        InvalidError::new_err(format!(
            "op must be one of eq, ne, lt, lte, gt, gte, in, contains, prefix, got `{op}`"
        ))
    })
}

fn coercion(coerce: &str) -> PyResult<Coerce> {
    let format = match coerce {
        "number" => return Ok(Coerce::Number),
        "rfc3339" => TimestampFormat::Rfc3339,
        "epoch_seconds" => TimestampFormat::EpochSeconds,
        "epoch_millis" => TimestampFormat::EpochMillis,
        "epoch_micros" => TimestampFormat::EpochMicros,
        other => {
            return Err(InvalidError::new_err(format!(
                "coerce must be number, rfc3339, epoch_seconds, epoch_millis, or epoch_micros, got `{other}`"
            )));
        }
    };
    Ok(Coerce::Timestamp { format })
}

fn fault(policy: &str) -> PyResult<FaultPolicy> {
    policy.parse().map_err(|_| {
        InvalidError::new_err(format!(
            "fault_policy must be `stop`, `pass`, or `drop`, got `{policy}`"
        ))
    })
}

fn record_policy(policy: &str) -> PyResult<RecordPolicy> {
    policy.parse().map_err(|_| {
        InvalidError::new_err(format!(
            "a record policy must be `reject` or `pass`, got `{policy}`"
        ))
    })
}

fn text_match(kind: &str) -> PyResult<TextMatch> {
    kind.parse().map_err(|_| {
        InvalidError::new_err(format!(
            "kind must be `equals`, `prefix`, `suffix`, `contains`, `glob`, or `regex`, got `{kind}`"
        ))
    })
}

fn field_path(path: &str) -> PyResult<FieldPath> {
    FieldPath::parse(path).map_err(|error| InvalidError::new_err(error.to_string()))
}

// Typed sample headers: `bool`, `int`, `float`, `str`, or `bytes`. An `int`
// above the signed range is unsigned, as the server reads a `uint64` header.
fn filter_headers(headers: Option<&Bound<'_, PyDict>>) -> PyResult<Vec<FilterHeader>> {
    let Some(headers) = headers else {
        return Ok(Vec::new());
    };
    headers
        .iter()
        .map(|(key, value)| {
            let invalid_header = |error: laser_sdk::iggy::prelude::IggyError| {
                InvalidError::new_err(error.to_string())
            };
            let header = crate::transport::header_value(&value)?;
            let value = match header.kind() {
                laser_sdk::iggy::prelude::HeaderKind::Bool => {
                    HeaderScalar::Bool(header.as_bool().map_err(invalid_header)?)
                }
                laser_sdk::iggy::prelude::HeaderKind::String => {
                    HeaderScalar::String(header.as_str().map_err(invalid_header)?.to_owned())
                }
                laser_sdk::iggy::prelude::HeaderKind::Int8 => {
                    HeaderScalar::Int(i64::from(header.as_int8().map_err(invalid_header)?))
                }
                laser_sdk::iggy::prelude::HeaderKind::Int16 => {
                    HeaderScalar::Int(i64::from(header.as_int16().map_err(invalid_header)?))
                }
                laser_sdk::iggy::prelude::HeaderKind::Int32 => {
                    HeaderScalar::Int(i64::from(header.as_int32().map_err(invalid_header)?))
                }
                laser_sdk::iggy::prelude::HeaderKind::Int64 => {
                    HeaderScalar::Int(header.as_int64().map_err(invalid_header)?)
                }
                laser_sdk::iggy::prelude::HeaderKind::Uint8 => {
                    HeaderScalar::Uint(u64::from(header.as_uint8().map_err(invalid_header)?))
                }
                laser_sdk::iggy::prelude::HeaderKind::Uint16 => {
                    HeaderScalar::Uint(u64::from(header.as_uint16().map_err(invalid_header)?))
                }
                laser_sdk::iggy::prelude::HeaderKind::Uint32 => {
                    HeaderScalar::Uint(u64::from(header.as_uint32().map_err(invalid_header)?))
                }
                laser_sdk::iggy::prelude::HeaderKind::Uint64 => {
                    HeaderScalar::Uint(header.as_uint64().map_err(invalid_header)?)
                }
                laser_sdk::iggy::prelude::HeaderKind::Float32 => {
                    HeaderScalar::Float(f64::from(header.as_float32().map_err(invalid_header)?))
                }
                laser_sdk::iggy::prelude::HeaderKind::Float64 => {
                    HeaderScalar::Float(header.as_float64().map_err(invalid_header)?)
                }
                _ => HeaderScalar::Raw(header.as_bytes().to_vec()),
            };
            Ok(FilterHeader {
                key: key.extract()?,
                value,
            })
        })
        .collect()
}

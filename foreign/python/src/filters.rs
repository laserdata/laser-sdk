use crate::async_bridge::{Awaitable, Undelivered, awaitable, awaitable_returning};
use crate::convert::{json_to_py, payload_bytes, py_to_de, py_to_typed_value, ser_to_py};
use crate::errors::{ConfigError, InvalidError, to_pyerr};
use crate::transport::PyConsumerMessage;
use laser_sdk::LaserError;
use laser_sdk::filters::{
    Coerce, ConsumerFilter, ExecutionMode, FaultPolicy, FilterError, FilterErrorReason, FilterExpr,
    FilterHeader, FilteredReader, FilteredStart, HeaderScalar, MatchedPage, MatchedRecord,
    RecordEvaluation, RecordPolicy, TextMatch, TimestampFormat,
};
use laser_sdk::query::CmpOp;
use laser_sdk::wire::filter::eval::{
    CompiledFilter, DecodeLimits, FilterRecord, HeaderNeed, HeaderRef,
};
use laser_sdk::wire::filter::{AppliedPolicy, FaultReason, FieldPath, PathSegment, TextPredicate};
use laser_sdk::wire::schema::Digest32;
use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::collections::VecDeque;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as SyncMutex, OnceLock};
use tokio::sync::{Mutex, MutexGuard, Notify};

#[gen_stub_pyclass]
#[pyclass(name = "AckContext", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyAckContext {
    #[pyo3(get)]
    owner_generation: u64,
    #[pyo3(get)]
    metadata_op: u64,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAckContext {
    #[new]
    fn new(owner_generation: u64, metadata_op: u64) -> Self {
        Self {
            owner_generation,
            metadata_op,
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
        #[gen_stub(override_type(
            type_repr = "typing.Literal['number', 'rfc3339', 'epoch_seconds', 'epoch_millis', 'epoch_micros']",
            imports = ("typing",)
        ))]
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

    /// The payload path exists. An explicit `null` counts as present. An
    /// invalid path raises `InvalidError`.
    #[staticmethod]
    fn present(path: &str) -> PyResult<Self> {
        Self::try_present(path)
    }

    /// The payload path exists, for an untrusted `path`. An invalid path
    /// raises `InvalidError`.
    #[staticmethod]
    fn try_present(path: &str) -> PyResult<Self> {
        FilterExpr::try_present(path)
            .map(|inner| Self { inner })
            .map_err(|error| InvalidError::new_err(error.to_string()))
    }

    /// The payload path does not exist. An invalid path raises
    /// `InvalidError`.
    #[staticmethod]
    fn absent(path: &str) -> PyResult<Self> {
        Self::try_absent(path)
    }

    /// The payload path does not exist, for an untrusted `path`. An invalid
    /// path raises `InvalidError`.
    #[staticmethod]
    fn try_absent(path: &str) -> PyResult<Self> {
        FilterExpr::try_absent(path)
            .map(|inner| Self { inner })
            .map_err(|error| InvalidError::new_err(error.to_string()))
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
    fn text(
        field: String,
        #[gen_stub(override_type(
            type_repr = "typing.Literal['equals', 'prefix', 'suffix', 'contains', 'glob', 'regex']",
            imports = ("typing",)
        ))]
        kind: &str,
        pattern: String,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: FilterExpr::text(field, text_match(kind)?, pattern),
        })
    }

    /// Match one text user header, keyed exactly, the way `text` matches a field.
    #[staticmethod]
    fn header_text(
        key: String,
        #[gen_stub(override_type(
            type_repr = "typing.Literal['equals', 'prefix', 'suffix', 'contains', 'glob', 'regex']",
            imports = ("typing",)
        ))]
        kind: &str,
        pattern: String,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: FilterExpr::header_text(key, text_match(kind)?, pattern),
        })
    }

    /// A copy whose text match ignores case. Any other node is returned as is.
    fn case_insensitive(&self) -> Self {
        Self {
            inner: self.inner.clone().case_insensitive(),
        }
    }

    /// Whether evaluating this node can require the decoded payload.
    #[getter]
    fn reads_payload(&self) -> bool {
        self.inner.reads_payload()
    }

    /// Whether evaluating this node can require the record headers.
    #[getter]
    fn reads_headers(&self) -> bool {
        self.inner.reads_headers()
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

/// A validated location inside a decoded payload: object keys separated by
/// `.` and array indexes as `[n]`, such as `after.ground_stations[0]`. Build
/// it with `FieldPath.parse`.
#[gen_stub_pyclass]
#[pyclass(name = "FieldPath", frozen, eq, skip_from_py_object)]
#[derive(Clone, PartialEq, Eq)]
pub struct PyFieldPath {
    inner: FieldPath,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFieldPath {
    /// Parse the text form. An invalid path raises `InvalidError`.
    #[staticmethod]
    fn parse(text: &str) -> PyResult<Self> {
        Ok(Self {
            inner: field_path(text)?,
        })
    }

    /// The path steps: a `str` object key or an `int` array index each.
    #[getter]
    fn segments(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.inner
            .segments()
            .iter()
            .map(|segment| match segment {
                PathSegment::Key(key) => Ok(key.into_pyobject(py)?.into_any().unbind()),
                PathSegment::Index(index) => Ok(index.into_pyobject(py)?.into_any().unbind()),
            })
            .collect()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("FieldPath({})", self.inner)
    }
}

/// One consumer filter: an expression, the payload codec, and the fault
/// policy. Its `digest` identifies exactly these semantics.
#[gen_stub_pyclass]
#[pyclass(name = "ConsumerFilter", frozen, from_py_object)]
#[derive(Clone)]
pub struct PyConsumerFilter {
    pub(crate) inner: ConsumerFilter,
}

impl From<ConsumerFilter> for PyConsumerFilter {
    fn from(inner: ConsumerFilter) -> Self {
        Self { inner }
    }
}

fn decode_limits(max_payload_bytes: Option<usize>, max_depth: Option<usize>) -> DecodeLimits {
    let defaults = DecodeLimits::default();
    DecodeLimits {
        max_payload_bytes: max_payload_bytes.unwrap_or(defaults.max_payload_bytes),
        max_depth: max_depth.unwrap_or(defaults.max_depth),
    }
}

// Run `evaluate` over one sample record with the interpreter released, since a
// large payload can take a while to decode.
fn on_record<T: Send>(
    py: Python<'_>,
    payload: &Bound<'_, PyAny>,
    headers: Option<&Bound<'_, PyDict>>,
    evaluate: impl FnOnce(&FilterRecord<'_>) -> T + Send,
) -> PyResult<T> {
    let payload = payload_bytes(payload)?;
    let headers = filter_headers(headers)?;
    let headers: Vec<HeaderRef<'_>> = headers.iter().map(HeaderRef::from).collect();
    Ok(py.detach(|| {
        evaluate(&FilterRecord {
            payload: &payload,
            headers: &headers,
        })
    }))
}

#[gen_stub_pymethods]
#[pymethods]
impl PyConsumerFilter {
    /// Decode the payload as JSON. `fault_policy` is `stop` (the default),
    /// `pass`, or `drop`.
    #[staticmethod]
    #[pyo3(signature = (expr, fault_policy="stop"))]
    fn json(
        expr: PyFilterExpr,
        #[gen_stub(override_type(type_repr = "typing.Literal['stop', 'pass', 'drop']", imports = ("typing",)))]
        fault_policy: &str,
    ) -> PyResult<Self> {
        Ok(ConsumerFilter::json(expr.inner)
            .with_fault_policy(fault(fault_policy)?)
            .into())
    }

    /// Decode a CBOR payload with bounded depth and size.
    #[staticmethod]
    #[pyo3(signature = (expr, fault_policy="stop"))]
    fn cbor(
        expr: PyFilterExpr,
        #[gen_stub(override_type(type_repr = "typing.Literal['stop', 'pass', 'drop']", imports = ("typing",)))]
        fault_policy: &str,
    ) -> PyResult<Self> {
        Ok(ConsumerFilter::cbor(expr.inner)
            .with_fault_policy(fault(fault_policy)?)
            .into())
    }

    /// Decode a raw Avro datum selected by its agdx.sid header.
    #[staticmethod]
    #[pyo3(signature = (expr, schema_refs, fault_policy="stop"))]
    fn avro(
        expr: PyFilterExpr,
        schema_refs: Vec<u32>,
        #[gen_stub(override_type(type_repr = "typing.Literal['stop', 'pass', 'drop']", imports = ("typing",)))]
        fault_policy: &str,
    ) -> PyResult<Self> {
        Ok(ConsumerFilter::avro(expr.inner, schema_refs)
            .with_fault_policy(fault(fault_policy)?)
            .into())
    }

    /// Decode a Protobuf message selected by its agdx.sid header.
    #[staticmethod]
    #[pyo3(signature = (expr, schema_refs, fault_policy="stop"))]
    fn protobuf(
        expr: PyFilterExpr,
        schema_refs: Vec<u32>,
        #[gen_stub(override_type(type_repr = "typing.Literal['stop', 'pass', 'drop']", imports = ("typing",)))]
        fault_policy: &str,
    ) -> PyResult<Self> {
        Ok(ConsumerFilter::protobuf(expr.inner, schema_refs)
            .with_fault_policy(fault(fault_policy)?)
            .into())
    }

    /// Never decode the payload. Only header predicates are allowed.
    #[staticmethod]
    #[pyo3(signature = (expr, fault_policy="stop"))]
    fn headers_only(
        expr: PyFilterExpr,
        #[gen_stub(override_type(type_repr = "typing.Literal['stop', 'pass', 'drop']", imports = ("typing",)))]
        fault_policy: &str,
    ) -> PyResult<Self> {
        Ok(ConsumerFilter::headers_only(expr.inner)
            .with_fault_policy(fault(fault_policy)?)
            .into())
    }

    /// A copy whose `schema_refs` resolve in the schema registry of `stream`.
    fn with_schema_stream(&self, stream: String) -> Self {
        self.inner.clone().with_schema_stream(stream).into()
    }

    /// The stream whose schema registry resolves `schema_refs`, or None for
    /// the deployment registry.
    #[getter]
    fn schema_stream(&self) -> Option<String> {
        self.inner.schema_stream.clone()
    }

    /// A copy with the malformed-payload policy: stop, pass, or drop.
    fn with_fault_policy(
        &self,
        #[gen_stub(override_type(type_repr = "typing.Literal['stop', 'pass', 'drop']", imports = ("typing",)))]
        fault_policy: &str,
    ) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .with_fault_policy(fault(fault_policy)?)
            .into())
    }

    /// A copy whose records in another format (another `agdx.ct` codec, or a
    /// writer schema the filter does not list) are skipped (`reject`, the
    /// default) or delivered unevaluated (`pass`).
    fn with_foreign_policy(&self, foreign_policy: &str) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .with_foreign_policy(record_policy(foreign_policy)?)
            .into())
    }

    /// A copy whose records with a value of a type a predicate cannot compare
    /// are skipped (`reject`, the default) or delivered unevaluated (`pass`).
    fn with_mismatch_policy(&self, mismatch_policy: &str) -> PyResult<Self> {
        Ok(self
            .inner
            .clone()
            .with_mismatch_policy(record_policy(mismatch_policy)?)
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

    /// The SHA-256 digest of the filter's semantics.
    #[getter]
    fn digest<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.inner.digest().as_bytes())
    }

    /// The filter op version.
    #[getter]
    const fn v(&self) -> u32 {
        self.inner.v
    }

    /// The evaluator version the filter was built for.
    #[getter]
    const fn evaluator_version(&self) -> u32 {
        self.inner.evaluator_version
    }

    #[getter]
    fn codec(&self) -> String {
        self.inner.codec.to_string()
    }

    #[getter]
    fn fault_policy(&self) -> String {
        self.inner.fault_policy.to_string()
    }

    /// `reject` or `pass` for records in another format.
    #[getter]
    fn foreign_policy(&self) -> String {
        self.inner.foreign_policy.to_string()
    }

    /// `reject` or `pass` for records whose value type a predicate cannot
    /// compare.
    #[getter]
    fn mismatch_policy(&self) -> String {
        self.inner.mismatch_policy.to_string()
    }

    /// Immutable writer schema ids permitted for Avro or Protobuf payloads.
    #[getter]
    fn schema_refs(&self) -> Vec<u32> {
        self.inner.schema_refs.clone()
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

/// A validated filter compiled once for repeated local evaluation with the
/// evaluator the server runs. Build it with `CompiledFilter.compile`.
#[gen_stub_pyclass]
#[pyclass(name = "CompiledFilter", frozen)]
pub struct PyCompiledFilter {
    inner: Arc<CompiledFilter>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCompiledFilter {
    /// Validate and compile `filter`. `schemas` lists the writer schema dicts
    /// an Avro or Protobuf filter decodes with. An invalid filter raises
    /// `InvalidError`.
    #[staticmethod]
    #[pyo3(signature = (filter, schemas=None))]
    fn compile(filter: &PyConsumerFilter, schemas: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        let compiled = match schemas {
            Some(schemas) => {
                let schemas: Vec<laser_sdk::wire::control::SchemaDef> = py_to_de(schemas)?;
                CompiledFilter::compile_with_schemas(&filter.inner, &schemas)
            }
            None => CompiledFilter::compile(&filter.inner),
        }
        .map_err(|error| InvalidError::new_err(error.to_string()))?;
        Ok(Self {
            inner: Arc::new(compiled),
        })
    }

    /// The SHA-256 digest of the compiled filter's semantics.
    #[getter]
    fn digest<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.inner.digest().as_bytes())
    }

    /// The filter this was compiled from.
    #[getter]
    fn filter(&self) -> PyConsumerFilter {
        self.inner.filter().clone().into()
    }

    /// The malformed-payload policy: `stop`, `pass`, or `drop`.
    #[getter]
    fn fault_policy(&self) -> String {
        self.inner.fault_policy().to_string()
    }

    /// The fault policy a record that faulted for `reason` follows. A record
    /// in another format follows the foreign policy, a type mismatch the
    /// mismatch policy, and a broken payload the fault policy.
    fn policy_for(&self, reason: &str) -> PyResult<String> {
        Ok(self.inner.policy_for(fault_reason(reason)?).to_string())
    }

    /// The record policy (`reject` or `pass`) that covers `reason`, or None
    /// for a decode fault.
    fn record_policy(&self, reason: &str) -> PyResult<Option<String>> {
        Ok(self
            .inner
            .record_policy(fault_reason(reason)?)
            .map(|policy| policy.to_string()))
    }

    /// Whether evaluation decodes the payload.
    #[getter]
    fn reads_payload(&self) -> bool {
        self.inner.reads_payload()
    }

    /// Whether evaluation reads user headers.
    #[getter]
    fn reads_headers(&self) -> bool {
        self.inner.reads_headers()
    }

    /// Which headers to decode before evaluating: `none`, `content_type`
    /// (only `agdx.ct`), or `all`.
    #[getter]
    fn header_need(&self) -> &'static str {
        match self.inner.header_need() {
            HeaderNeed::None => "none",
            HeaderNeed::ContentType => "content_type",
            HeaderNeed::All => "all",
        }
    }

    /// `selected`, `rejected`, or `fault` for one record. `headers` carries
    /// typed user headers.
    #[pyo3(signature = (payload, headers=None, *, max_payload_bytes=None, max_depth=None))]
    fn evaluate(
        &self,
        py: Python<'_>,
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
        max_payload_bytes: Option<usize>,
        max_depth: Option<usize>,
    ) -> PyResult<String> {
        let limits = decode_limits(max_payload_bytes, max_depth);
        let verdict = on_record(py, payload, headers, |record| {
            self.inner.evaluate(record, &limits)
        })?;
        Ok(verdict.to_string())
    }

    /// The verdict plus the fault reason when the record could not be
    /// evaluated, as `(verdict, reason)`.
    #[pyo3(signature = (payload, headers=None, *, max_payload_bytes=None, max_depth=None))]
    fn evaluate_with_fault(
        &self,
        py: Python<'_>,
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
        max_payload_bytes: Option<usize>,
        max_depth: Option<usize>,
    ) -> PyResult<(String, Option<String>)> {
        let limits = decode_limits(max_payload_bytes, max_depth);
        let (verdict, fault) = on_record(py, payload, headers, |record| {
            self.inner.evaluate_with_fault(record, &limits)
        })?;
        Ok((verdict.to_string(), fault.map(|reason| reason.to_string())))
    }

    /// Explain the verdict: a dict with `verdict`, an optional `fault`, and
    /// the `root` explanation tree with one node per predicate.
    #[pyo3(signature = (payload, headers=None, *, max_payload_bytes=None, max_depth=None))]
    fn explain(
        &self,
        py: Python<'_>,
        payload: &Bound<'_, PyAny>,
        headers: Option<&Bound<'_, PyDict>>,
        max_payload_bytes: Option<usize>,
        max_depth: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        let limits = decode_limits(max_payload_bytes, max_depth);
        let explanation = on_record(py, payload, headers, |record| {
            self.inner.explain(record, &limits)
        })?;
        ser_to_py(py, &explanation)
    }

    fn __repr__(&self) -> String {
        format!(
            "CompiledFilter(codec={}, fault_policy={})",
            self.inner.filter().codec,
            self.inner.fault_policy()
        )
    }
}

/// Reads the records a consumer group's filter selects and stores progress
/// through fenced acknowledgments. Build it with `ConsumerGroup.reader`. Drive one
/// reader from one task. `async for record in reader` yields matching records
/// until the task is cancelled. A record whose `next_record` call is cancelled
/// after it was read is yielded again by the next call, so a cancellation never
/// strands a record the partition's progress waits for.
#[gen_stub_pyclass]
#[pyclass(name = "FilteredReader", frozen)]
pub struct PyFilteredReader {
    examined: Arc<AtomicU64>,
    state: Arc<Mutex<ReaderState>>,
    interrupt: Arc<Interrupt>,
    // Records a cancelled call read but never handed over, yielded first.
    returned: Arc<SyncMutex<VecDeque<PyMatchedRecord>>>,
    returned_pages: Arc<SyncMutex<VecDeque<Arc<SharedPage>>>>,
}

struct ReaderState {
    examined: Arc<AtomicU64>,
    reader: Option<FilteredReader>,
    buffered: VecDeque<PyMatchedRecord>,
    more: bool,
}

// Ends an idle wait that holds the reader, so another call takes it at once.
// `wanted` counts the calls still queued for the reader, so a wait that would
// start before they get it returns at once.
#[derive(Default)]
struct Interrupt {
    wanted: AtomicUsize,
    notify: Notify,
}

// One queued call, counted until it holds the reader or is cancelled.
struct Queued<'a>(&'a AtomicUsize);

impl Drop for Queued<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl PyFilteredReader {
    pub(crate) fn new(reader: FilteredReader) -> Self {
        let examined = Arc::new(AtomicU64::new(0));
        Self {
            examined: examined.clone(),
            state: Arc::new(Mutex::new(ReaderState {
                more: false,
                examined,
                reader: Some(reader),
                buffered: VecDeque::new(),
            })),
            interrupt: Arc::default(),
            returned: Arc::new(SyncMutex::new(VecDeque::new())),
            returned_pages: Arc::new(SyncMutex::new(VecDeque::new())),
        }
    }

    // The work `close` and an `async with` exit share.
    fn closing(&self) -> impl Future<Output = PyResult<()>> + Send + 'static {
        let (state, returned) = (self.state.clone(), self.returned.clone());
        let (returned_pages, interrupt) = (self.returned_pages.clone(), self.interrupt.clone());
        async move {
            let taken = {
                let mut state = take(&state, &interrupt).await;
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
        }
    }
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
    /// The next page with at least one match. While nothing is new it waits
    /// the way the Rust reader does, in a filtered wait the server holds or
    /// one idle interval on a server that holds none. `ack` and `close` end
    /// that wait, so they run meanwhile.
    fn next_page<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyMatchedPage>> {
        let (state, interrupt) = (self.state.clone(), self.interrupt.clone());
        let returned_pages = self.returned_pages.clone();
        awaitable_returning(
            py,
            async move {
                loop {
                    {
                        let mut state = state.lock().await;
                        if let Some(page) = state.next_page(&returned_pages).await? {
                            return Ok(page);
                        }
                        if !state.more {
                            state.wait_for_records(&interrupt).await?;
                            continue;
                        }
                    }
                    tokio::task::yield_now().await;
                }
            },
            give_back_page(self.returned_pages.clone()),
        )
    }

    /// Read until a page matches or nothing is new. `None` when nothing is new.
    fn try_next_page<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Awaitable<'py, Option<PyMatchedPage>>> {
        let (state, interrupt) = (self.state.clone(), self.interrupt.clone());
        let returned_pages = self.returned_pages.clone();
        awaitable_returning(
            py,
            async move {
                loop {
                    {
                        let mut state = take(&state, &interrupt).await;
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
    fn read_round<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Awaitable<'py, (Option<PyMatchedPage>, bool)>> {
        let (state, interrupt) = (self.state.clone(), self.interrupt.clone());
        let returned_pages = self.returned_pages.clone();
        let recover = give_back_page(self.returned_pages.clone());
        awaitable_returning(
            py,
            async move {
                let mut state = take(&state, &interrupt).await;
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

    /// Wait after a round that found nothing new, for a caller that drives
    /// `try_next_page` or `read_round` itself. The server holds one filtered
    /// wait until one of this reader's partitions has a new record, at most
    /// `max_wait_ms` and what it announces, and the next round reads only the
    /// ready partitions. On a server that holds no filtered wait it sleeps one
    /// idle interval. Any other call on the reader ends the wait.
    fn wait_for_records<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, ()>> {
        let (state, interrupt) = (self.state.clone(), self.interrupt.clone());
        awaitable(py, async move {
            state.lock().await.wait_for_records(&interrupt).await
        })
    }

    /// Source records examined in the last bounded round, including empty pages.
    fn examined_in_round(&self) -> u64 {
        self.examined.load(Ordering::Relaxed)
    }

    /// The next matching record.
    fn next_record<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyMatchedRecord>> {
        let (state, returned) = (self.state.clone(), self.returned.clone());
        let returned_pages = self.returned_pages.clone();
        awaitable_returning(
            py,
            next_record(state, self.interrupt.clone(), returned, returned_pages),
            give_back(self.returned.clone()),
        )
    }

    /// Mark one record handled and store the progress this completes.
    fn ack<'py>(
        &self,
        py: Python<'py>,
        record: PyRef<'_, PyMatchedRecord>,
    ) -> PyResult<Awaitable<'py, ()>> {
        let (state, interrupt) = (self.state.clone(), self.interrupt.clone());
        let (page, index) = (record.page.clone(), record.index);
        awaitable(py, async move {
            let mut state = take(&state, &interrupt).await;
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
    ) -> PyResult<Awaitable<'py, ()>> {
        let (state, interrupt) = (self.state.clone(), self.interrupt.clone());
        let (page, index) = (record.page.clone(), record.index);
        awaitable(py, async move {
            let mut state = take(&state, &interrupt).await;
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
    ) -> PyResult<Awaitable<'py, ()>> {
        let (state, interrupt) = (self.state.clone(), self.interrupt.clone());
        let page = page.page.clone();
        awaitable(py, async move {
            let mut state = take(&state, &interrupt).await;
            state.open()?.ack_page(&page.page).await.map_err(to_pyerr)
        })
    }

    /// Pass the record a `FilterFaultError` or a `FilterOversizedRecordError`
    /// stopped `partition_id` at, and read on after it, the way a native
    /// consumer seeks past a record. The skip is a fenced acknowledgment of
    /// `offset` under the policy and the owner the stopped page was read
    /// under, stored once every earlier record of the partition is handled.
    /// Raises `InvalidError` when the partition is not stopped at `offset`.
    fn skip<'py>(
        &self,
        py: Python<'py>,
        partition_id: u32,
        offset: u64,
    ) -> PyResult<Awaitable<'py, ()>> {
        let (state, interrupt) = (self.state.clone(), self.interrupt.clone());
        awaitable(py, async move {
            let mut state = take(&state, &interrupt).await;
            state
                .open()?
                .skip(partition_id, offset)
                .await
                .map_err(to_pyerr)
        })
    }

    /// The partitions this reader reads now.
    fn partitions<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Vec<u32>>> {
        let (state, interrupt) = (self.state.clone(), self.interrupt.clone());
        awaitable(py, async move {
            Ok(take(&state, &interrupt).await.open()?.partitions())
        })
    }

    /// How long this reader waits, in milliseconds, when nothing is new on a
    /// server that holds no filtered wait.
    fn idle_interval_ms<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, f64>> {
        let (state, interrupt) = (self.state.clone(), self.interrupt.clone());
        awaitable(py, async move {
            Ok(take(&state, &interrupt)
                .await
                .open()?
                .idle_interval()
                .as_secs_f64()
                * 1000.0)
        })
    }

    /// Whether `page` was read by this reader in its current membership, so
    /// its records can still be acknowledged. A rejoin retires every earlier
    /// page.
    fn owns<'py>(
        &self,
        py: Python<'py>,
        page: PyRef<'_, PyMatchedPage>,
    ) -> PyResult<Awaitable<'py, bool>> {
        let (state, interrupt) = (self.state.clone(), self.interrupt.clone());
        let page = Arc::clone(&page.page);
        awaitable(py, async move {
            Ok(take(&state, &interrupt).await.open()?.owns(&page.page))
        })
    }

    /// Store completed progress and leave the group. Closing twice is a no-op.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, ()>> {
        awaitable(py, self.closing())
    }

    /// Enter `async with`: yield the reader. Exiting the block closes it.
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Awaitable<'py, Py<Self>>> {
        awaitable(py, async move { Ok(slf) })
    }

    /// Exit `async with`: `close`, then return `False` so an exception in the
    /// body is not suppressed.
    #[pyo3(signature = (_exc_type, _exc_value, _traceback))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Awaitable<'py, bool>> {
        let closing = self.closing();
        awaitable(py, async move {
            closing.await?;
            Ok(false)
        })
    }

    fn __aiter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyMatchedRecord>> {
        let (state, returned) = (self.state.clone(), self.returned.clone());
        let (returned_pages, interrupt) = (self.returned_pages.clone(), self.interrupt.clone());
        awaitable_returning(
            py,
            async move {
                if state.lock().await.reader.is_none() {
                    return Err(PyStopAsyncIteration::new_err(()));
                }
                next_record(state, interrupt, returned, returned_pages).await
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

    // The Rust reader's wait after a round that found nothing new. Its held
    // waits outlive an interrupted call, so the next wait picks them up.
    async fn wait_for_records(&mut self, interrupt: &Interrupt) -> PyResult<()> {
        let reader = self.open()?;
        // Registered before the count is read, so a call that queues after
        // the read still ends this wait.
        let mut interrupted = pin!(interrupt.notify.notified());
        interrupted.as_mut().enable();
        if interrupt.wanted.load(Ordering::SeqCst) > 0 {
            return Ok(());
        }
        tokio::select! {
            () = reader.wait_for_records() => {}
            () = interrupted => {}
        }
        Ok(())
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
// page. Any other call on the reader ends its idle wait.
async fn next_record(
    state: Arc<Mutex<ReaderState>>,
    interrupt: Arc<Interrupt>,
    returned: Arc<SyncMutex<VecDeque<PyMatchedRecord>>>,
    returned_pages: Arc<SyncMutex<VecDeque<Arc<SharedPage>>>>,
) -> PyResult<PyMatchedRecord> {
    loop {
        {
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
                None if state.more => {}
                None => {
                    state.wait_for_records(&interrupt).await?;
                    continue;
                }
            }
        }
        tokio::task::yield_now().await;
    }
}

// Take the reader for a call that must not wait out an idle wait.
async fn take<'a>(
    state: &'a Mutex<ReaderState>,
    interrupt: &Interrupt,
) -> MutexGuard<'a, ReaderState> {
    interrupt.wanted.fetch_add(1, Ordering::SeqCst);
    let _queued = Queued(&interrupt.wanted);
    interrupt.notify.notify_waiters();
    state.lock().await
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

    /// How the group's policy treated the record: `selected` when the filter
    /// evaluated and selected it, `unevaluated` when a pass fault,
    /// foreign-record, or mismatch policy delivered it without evaluating it,
    /// and `unfiltered` when the group has no policy and receives every
    /// record.
    #[getter]
    #[gen_stub(override_return_type(
        type_repr = "typing.Literal['selected', 'unevaluated', 'unfiltered']",
        imports = ("typing",)
    ))]
    fn evaluation(&self) -> &'static str {
        match self.record().evaluation {
            RecordEvaluation::Selected => "selected",
            RecordEvaluation::Unevaluated(_) => "unevaluated",
            RecordEvaluation::Unfiltered => "unfiltered",
        }
    }

    /// Why the record was not evaluated when `evaluation` is `unevaluated`:
    /// `malformed`, `too_large` or `too_deep` under a pass fault policy,
    /// `foreign_codec`, `missing_schema`, `schema_not_allowed` or
    /// `schema_mismatch` under a pass foreign-record policy, and
    /// `type_mismatch` under a pass mismatch policy. None otherwise.
    #[getter]
    #[gen_stub(override_return_type(
        type_repr = "typing.Optional[typing.Literal['missing_schema', 'schema_not_allowed', 'schema_mismatch', 'malformed', 'too_large', 'too_deep', 'foreign_codec', 'type_mismatch']]",
        imports = ("typing",)
    ))]
    fn unevaluated_reason(&self) -> Option<String> {
        match self.record().evaluation {
            RecordEvaluation::Unevaluated(reason) => Some(reason.to_string()),
            RecordEvaluation::Selected | RecordEvaluation::Unfiltered => None,
        }
    }

    /// True when a header entry or block structure is malformed. Valid
    /// headers and payload remain readable. Unknown value kinds remain raw.
    /// A truncated block has empty `message.headers`.
    #[getter]
    fn headers_malformed(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(self.message(py)?.borrow(py).headers_malformed)
    }

    /// Decode the record's payload as JSON.
    fn json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value: serde_json::Value = self.record().json().map_err(to_pyerr)?;
        json_to_py(py, &value)
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

// `next`, `first`, `last`, or the serde form of another start, such as
// `{"offset": 5}` or `{"timestamp": 1700000000000000}`.
pub(crate) fn filtered_start(start: &Bound<'_, PyAny>) -> PyResult<FilteredStart> {
    py_to_de(start)
}

/// Whether the execution `mode` (`filtered` or `unfiltered`) evaluates a filter.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn execution_mode_is_filtered(mode: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(py_to_de::<ExecutionMode>(mode)?.is_filtered())
}

/// Whether the fault `reason` means the record is in another format, which
/// the foreign policy covers.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn fault_reason_is_foreign(reason: &str) -> PyResult<bool> {
    Ok(fault_reason(reason)?.is_foreign())
}

/// Whether the record `policy` (`reject` or `pass`) skips the record.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn record_policy_is_reject(policy: &str) -> PyResult<bool> {
    Ok(record_policy(policy)?.is_reject())
}

/// The result code a filter error `reason` classifies as, such as
/// `InvalidArgument`.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn filter_error_reason_code(reason: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(format!(
        "{:?}",
        py_to_de::<FilterErrorReason>(reason)?.code()
    ))
}

// `FilterError.invalid`: the `FilterError` an `InvalidError` becomes, with
// reason `invalid_request`, returned without raising it. The exception class
// cannot carry Rust methods, so `errors::register` attaches it.
#[pyfunction]
pub(crate) fn filter_error_invalid(
    py: Python<'_>,
    error: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    if !error.is_instance_of::<InvalidError>() {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "error must be an InvalidError",
        ));
    }
    let message = error.str()?.to_string();
    let filter = FilterError::invalid(laser_sdk::wire::error::InvalidError::new(message));
    Ok(to_pyerr(LaserError::Filter(filter))
        .into_value(py)
        .into_any())
}

// `FilterError.check_version`: raise `FilterError` with reason
// `version_skew` when `v` is not the filter op version this build speaks.
#[pyfunction]
pub(crate) fn filter_error_check_version(v: u32) -> PyResult<()> {
    FilterError::check_version(v).map_err(|error| to_pyerr(LaserError::Filter(error)))
}

/// Epoch microseconds of an integer timestamp in `format` (`epoch_seconds`,
/// `epoch_millis`, or `epoch_micros`), or None when it is out of range or the
/// format reads text.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn timestamp_format_micros_from_integer(
    format: &Bound<'_, PyAny>,
    value: i128,
) -> PyResult<Option<i64>> {
    Ok(py_to_de::<TimestampFormat>(format)?.micros_from_integer(value))
}

/// Epoch microseconds of a text timestamp in `format`, or None when the text
/// does not parse.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn timestamp_format_micros_from_text(
    format: &Bound<'_, PyAny>,
    text: &str,
) -> PyResult<Option<i64>> {
    Ok(py_to_de::<TimestampFormat>(format)?.micros_from_text(text))
}

/// Validate a text predicate dict (`field`, `kind`, `pattern`, and an
/// optional `case_insensitive`) by compiling its pattern. An invalid one
/// raises `InvalidError`.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn text_predicate_validate(predicate: &Bound<'_, PyAny>) -> PyResult<()> {
    py_to_de::<TextPredicate>(predicate)?
        .validate()
        .map_err(|error| InvalidError::new_err(error.to_string()))
}

/// The applied-policy dict of an independent consumer or a bound group that
/// ran the filter with the 32-byte `digest`.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn applied_policy_filtered(
    py: Python<'_>,
    group_id: Option<u64>,
    #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
    digest: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let digest = <[u8; Digest32::BYTES]>::try_from(payload_bytes(digest)?.as_slice())
        .map_err(|_| InvalidError::new_err("digest must be 32 bytes"))?;
    ser_to_py(
        py,
        &AppliedPolicy::filtered(group_id, Digest32::new(digest)),
    )
}

/// The applied-policy dict of a group without a binding, which reads every
/// record.
#[gen_stub_pyfunction]
#[pyfunction]
pub(crate) fn applied_policy_unfiltered(
    py: Python<'_>,
    group_id: u64,
    policy_generation: u64,
) -> PyResult<Py<PyAny>> {
    ser_to_py(py, &AppliedPolicy::unfiltered(group_id, policy_generation))
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

fn fault_reason(reason: &str) -> PyResult<FaultReason> {
    serde_json::from_value(serde_json::Value::String(reason.to_owned())).map_err(|_| {
        InvalidError::new_err(format!(
            "reason must be `missing_schema`, `schema_not_allowed`, `schema_mismatch`, `malformed`, `too_large`, `too_deep`, `foreign_codec`, or `type_mismatch`, got `{reason}`"
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
pub(crate) fn filter_headers(headers: Option<&Bound<'_, PyDict>>) -> PyResult<Vec<FilterHeader>> {
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

use crate::agdx::PyAgdxReceipt;
use crate::async_bridge::future_into_py;
use crate::context::{PyContextMessage, PyScopedMemory, context_policy, take_failure};
use crate::convert::{json_to_py, payload_bytes, py_to_de, py_to_json, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use crate::memory::Backend;
use crate::session::{PySession, PySessions};
use laser_sdk::agent::{
    AssembledContext, ModelCall, ModelRequest, ModelResponse, SessionState, SubmitBuilder, ToolCall,
};
use laser_sdk::types::ConversationId;
use laser_sdk::wire::agent::{AgentErrorBody, AgentId};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

fn agent(value: &str) -> PyResult<AgentId> {
    value
        .parse()
        .map_err(|_| InvalidError::new_err(format!("invalid agent id `{value}`")))
}

/// A copy of `value` with the values of keys such as `authorization`,
/// `api_key`, `token`, `password`, `secret`, and `cookie` replaced at any
/// depth. A convenience, not a guarantee: a secret under another key is
/// published as it is.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn default_redact(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let mut json = py_to_json(value)?;
    laser_sdk::agent::default_redact(&mut json);
    json_to_py(py, &json)
}

/// One model call request: `model`, the prompt `body` (str or bytes), the
/// serving `provider`, and the model `operation`, `chat` unless set. The SDK
/// never calls a model: the application calls its provider and records the
/// call through the session.
#[gen_stub_pyclass]
#[pyclass(name = "ModelRequest", frozen)]
pub struct PyModelRequest {
    inner: ModelRequest,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyModelRequest {
    #[new]
    #[pyo3(signature = (model, body, *, provider=None, operation=None))]
    fn new(
        model: String,
        body: &Bound<'_, PyAny>,
        provider: Option<String>,
        operation: Option<String>,
    ) -> PyResult<Self> {
        let mut inner = ModelRequest::new(model, payload_bytes(body)?);
        if let Some(provider) = provider {
            inner = inner.provider(provider);
        }
        if let Some(operation) = operation {
            inner = inner.operation(operation);
        }
        Ok(Self { inner })
    }

    #[getter]
    fn model(&self) -> &str {
        &self.inner.model
    }

    #[getter]
    fn provider(&self) -> Option<&str> {
        self.inner.provider.as_deref()
    }

    #[getter]
    fn operation(&self) -> &str {
        &self.inner.operation
    }

    #[getter]
    fn body(&self) -> Vec<u8> {
        self.inner.body.clone()
    }
}

/// One model call result: the response `body`, the answering `model`, the
/// `finish_reason`, the token `usage` dict, and the call `duration_ms` when
/// the application measured it itself. Left unset, `record_model_call`
/// records a duration of 0.
#[gen_stub_pyclass]
#[pyclass(name = "ModelResponse", frozen)]
pub struct PyModelResponse {
    inner: ModelResponse,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyModelResponse {
    #[new]
    #[pyo3(signature = (body, *, model=None, finish_reason=None, usage=None, duration_ms=None))]
    fn new(
        body: &Bound<'_, PyAny>,
        model: Option<String>,
        finish_reason: Option<String>,
        usage: Option<&Bound<'_, PyAny>>,
        duration_ms: Option<f64>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: ModelResponse {
                body: payload_bytes(body)?,
                model,
                finish_reason,
                usage: usage.map(py_to_de).transpose()?,
                duration: duration_ms
                    .map(|ms| crate::convert::duration_ms(ms, "duration_ms"))
                    .transpose()?,
            },
        })
    }

    #[getter]
    fn body(&self) -> Vec<u8> {
        self.inner.body.clone()
    }

    #[getter]
    fn model(&self) -> Option<&str> {
        self.inner.model.as_deref()
    }

    #[getter]
    fn finish_reason(&self) -> Option<&str> {
        self.inner.finish_reason.as_deref()
    }

    #[getter]
    fn usage(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.inner
            .usage
            .as_ref()
            .map(|usage| ser_to_py(py, usage))
            .transpose()
    }

    /// How long the model call took, in milliseconds.
    #[getter]
    fn duration_ms(&self) -> Option<f64> {
        self.inner
            .duration
            .map(|duration| duration.as_secs_f64() * 1000.0)
    }
}

/// The context a model call received, assembled without writing anything:
/// the kept `fragments` and the `manifest` dict that describes them.
#[gen_stub_pyclass]
#[pyclass(name = "AssembledContext", frozen)]
pub struct PyAssembledContext {
    inner: AssembledContext,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAssembledContext {
    /// The kept messages, in log order.
    #[getter]
    fn fragments(&self) -> Vec<PyContextMessage> {
        self.inner
            .fragments
            .iter()
            .cloned()
            .map(PyContextMessage::new)
            .collect()
    }

    /// The context manifest as a dict.
    #[getter]
    fn manifest(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.manifest)
    }

    /// The fragments' payloads as UTF-8, one per line.
    fn text(&self) -> String {
        self.inner.text()
    }
}

/// A model call in flight. Finish it with `complete` or `fail`.
#[gen_stub_pyclass]
#[pyclass(name = "ModelCall")]
pub struct PyModelCall {
    inner: Mutex<Option<ModelCall>>,
    correlation: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyModelCall {
    /// The call's correlation, shared by its request, manifest, and result.
    #[getter]
    fn correlation(&self) -> &str {
        &self.correlation
    }

    /// Record the model's `response`. Returns an `AgdxReceipt`.
    fn complete<'py>(
        &self,
        py: Python<'py>,
        response: &PyModelResponse,
    ) -> PyResult<Bound<'py, PyAny>> {
        let call = take(&self.inner, "model call")?;
        let response = response.inner.clone();
        future_into_py(py, async move {
            let receipt = call.complete(response).await.map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }

    /// Record that the call failed with `error`, an `AgentErrorBody` dict.
    fn fail<'py>(&self, py: Python<'py>, error: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let call = take(&self.inner, "model call")?;
        let error: AgentErrorBody = py_to_de(error)?;
        future_into_py(py, async move {
            let receipt = call.fail(error).await.map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }
}

/// A tool call in flight. Finish it with `complete` or `fail`.
#[gen_stub_pyclass]
#[pyclass(name = "ToolCall")]
pub struct PyToolCall {
    inner: Mutex<Option<ToolCall>>,
    correlation: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyToolCall {
    /// The call's correlation, shared by its command and result.
    #[getter]
    fn correlation(&self) -> &str {
        &self.correlation
    }

    /// Record the tool's `result` (str or bytes) and the measured duration.
    /// Returns an `AgdxReceipt`.
    fn complete<'py>(
        &self,
        py: Python<'py>,
        result: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let call = take(&self.inner, "tool call")?;
        let result = payload_bytes(result)?;
        future_into_py(py, async move {
            let receipt = call.complete(result).await.map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }

    /// Record that the tool failed with `error`, an `AgentErrorBody` dict.
    fn fail<'py>(&self, py: Python<'py>, error: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let call = take(&self.inner, "tool call")?;
        let error: AgentErrorBody = py_to_de(error)?;
        future_into_py(py, async move {
            let receipt = call.fail(error).await.map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }
}

fn take<T>(slot: &Mutex<Option<T>>, what: &str) -> PyResult<T> {
    slot.lock()
        .expect("call lock")
        .take()
        .ok_or_else(|| InvalidError::new_err(format!("this {what} already finished")))
}

/// The session's state document: JSON patches on the session lane, applied in
/// lane order, with snapshots as checkpoints.
#[gen_stub_pyclass]
#[pyclass(name = "SessionState", frozen)]
pub struct PySessionState {
    inner: Arc<SessionState>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessionState {
    /// Set `key` to the JSON-compatible `value`. Returns an `AgdxReceipt`.
    fn set<'py>(
        &self,
        py: Python<'py>,
        key: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.inner.clone();
        let value = py_to_json(value)?;
        future_into_py(py, async move {
            let receipt = state.set(&key, value).await.map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }

    /// Apply `patch`, a list of RFC 6902 operation dicts, atomically against
    /// the revision this handle last saw. Append success does not prove the
    /// patch applied: a reader learns the outcome from the folded state.
    fn patch<'py>(&self, py: Python<'py>, patch: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.inner.clone();
        let patch = py_to_de(patch)?;
        future_into_py(py, async move {
            let receipt = state.patch(patch).await.map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }

    /// Replace the whole document with `document`, a dict, as one
    /// revision-guarded patch. Returns an `AgdxReceipt`.
    fn replace<'py>(
        &self,
        py: Python<'py>,
        document: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let state = self.inner.clone();
        let document = py_to_json(document)?;
        future_into_py(py, async move {
            let receipt = state.replace(document).await.map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }

    /// Write the whole document this handle holds as a snapshot.
    fn snapshot<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.inner.clone();
        future_into_py(py, async move {
            let receipt = state.snapshot().await.map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }

    /// Fold the state from the retained session lane, as a `SessionStateView`
    /// dict. `complete` is false when the lane no longer holds the records the
    /// document starts from.
    fn get<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let state = self.inner.clone();
        future_into_py(py, async move {
            let view = state.get().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &view))
        })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PySession {
    /// This session as a `{"stream", "session"}` dict, for writes that land
    /// outside the session's own stream.
    fn reference(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.reference().map_err(to_pyerr)?)
    }

    /// This handle with `redactor(value) -> value` applied to tool arguments
    /// and JSON model request bodies before they are published, in place of
    /// `default_redact`. A redactor that raises publishes `[redacted]`.
    fn redact(&self, redactor: Py<PyAny>) -> PySession {
        let redactor = Arc::new(redactor);
        PySession::new(self.inner.clone().redact(move |value| {
            let redacted = Python::attach(|py| -> PyResult<serde_json::Value> {
                let given = json_to_py(py, value)?;
                py_to_json(redactor.call1(py, (given,))?.bind(py))
            });
            *value =
                redacted.unwrap_or_else(|_| serde_json::Value::String("[redacted]".to_owned()));
        }))
    }

    /// This handle with `source`, a source reference dict, as the record it
    /// acts on: stamped as the source of graph writes and the origin of
    /// remembered items.
    fn acting_on(&self, source: &Bound<'_, PyAny>) -> PyResult<PySession> {
        Ok(PySession::new(
            self.inner.clone().acting_on(py_to_de(source)?),
        ))
    }

    /// Whether an operator asked to cancel this session on `agent.control`,
    /// by a cancel request or a forced cancel. Reads the retained control
    /// records of this session, so it answers on open Apache Iggy too.
    fn cancel_requested<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        future_into_py(py, async move {
            session.cancel_requested().await.map_err(to_pyerr)
        })
    }

    /// The operator requests recorded for this session on `agent.control`, as
    /// a `PendingControl`. Inside an agent runtime the control follower keeps
    /// it current. The handler decides when to stop.
    fn pending_control<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        future_into_py(py, async move {
            let pending = session.pending_control().await.map_err(to_pyerr)?;
            Ok(PyPendingControl {
                pause_requested: pending.pause_requested,
                cancel_requested: pending.cancel_requested,
            })
        })
    }

    /// This handle signing its terminal record with `key`, so a verifying
    /// reader can prove which agent ended the session.
    fn signed_by(&self, key: &crate::sign::PySigningKey) -> PySession {
        PySession::new(self.inner.clone().signed_by(key.inner.as_ref().clone()))
    }

    /// The work records this session's agents held while it was paused and
    /// have not reported handled, as a `ParkedRecords`.
    fn parked<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        future_into_py(py, async move {
            let parked = session.parked().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                Ok(PyParkedRecords {
                    records: parked
                        .records
                        .iter()
                        .map(|record| ser_to_py(py, record))
                        .collect::<PyResult<Vec<_>>>()?,
                    complete: parked.complete,
                })
            })
        })
    }

    /// The session's state document.
    fn state(&self) -> PySessionState {
        PySessionState {
            inner: Arc::new(self.inner.state()),
        }
    }

    /// Assemble the model context under `policy` from the session lane and
    /// describe it in a manifest. Nothing is written. Returns an
    /// `AssembledContext`.
    fn assemble<'py>(
        &self,
        py: Python<'py>,
        policy: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let (policy, failure) = context_policy(policy)?;
        future_into_py(py, async move {
            let assembled = session.assemble(policy).await.map_err(to_pyerr)?;
            if let Some(error) = take_failure(&failure) {
                return Err(error);
            }
            Ok(PyAssembledContext { inner: assembled })
        })
    }

    /// Record a model `request` addressed to this agent, and the context it
    /// received when `assembled` is given. Finish the returned `ModelCall`
    /// when the application's provider answers.
    #[pyo3(signature = (request, assembled=None))]
    fn model<'py>(
        &self,
        py: Python<'py>,
        request: &PyModelRequest,
        assembled: Option<&PyAssembledContext>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let request = request.inner.clone();
        let assembled = assembled.map(|assembled| assembled.inner.clone());
        future_into_py(py, async move {
            let call = session
                .model(request, assembled.as_ref())
                .await
                .map_err(to_pyerr)?;
            Ok(PyModelCall {
                correlation: call.correlation().to_string(),
                inner: Mutex::new(Some(call)),
            })
        })
    }

    /// Record a call of the tool `name` addressed to this agent, with the
    /// JSON-compatible `args` redacted. Returns a `ToolCall`.
    fn tool<'py>(
        &self,
        py: Python<'py>,
        name: String,
        args: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let args = py_to_json(args)?;
        future_into_py(py, async move {
            let call = session.tool(name, args).await.map_err(to_pyerr)?;
            Ok(PyToolCall {
                correlation: call.correlation().to_string(),
                inner: Mutex::new(Some(call)),
            })
        })
    }

    /// Record a model call that already happened: the `request`, the context
    /// when `assembled` is given, and the `response`. A response without
    /// `duration_ms` records a duration of 0. Returns an `AgdxReceipt`.
    #[pyo3(signature = (request, response, assembled=None))]
    fn record_model_call<'py>(
        &self,
        py: Python<'py>,
        request: &PyModelRequest,
        response: &PyModelResponse,
        assembled: Option<&PyAssembledContext>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let request = request.inner.clone();
        let response = response.inner.clone();
        let assembled = assembled.map(|assembled| assembled.inner.clone());
        future_into_py(py, async move {
            let receipt = session
                .record_model_call(request, response, assembled.as_ref())
                .await
                .map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }

    /// Record that `items`, a list of `MemoryItem` recalled for `query` when
    /// there was one, entered the session's context. Returns an
    /// `AgdxReceipt`.
    #[pyo3(signature = (query, items))]
    fn record_retrieval<'py>(
        &self,
        py: Python<'py>,
        query: Option<String>,
        items: Vec<PyRef<'_, crate::memory::PyMemoryItem>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let items: Vec<_> = items.iter().map(|item| item.inner.clone()).collect();
        future_into_py(py, async move {
            let receipt = session
                .record_retrieval(query, &items)
                .await
                .map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }

    /// Record that a summary replaced the covered records of the session's
    /// context. `compaction` is a `ContextCompaction` dict with `summary_at`,
    /// `covered`, and `summarizer`. Returns an `AgdxReceipt`.
    fn record_compaction<'py>(
        &self,
        py: Python<'py>,
        compaction: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let compaction = py_to_de(compaction)?;
        future_into_py(py, async move {
            let receipt = session
                .record_compaction(compaction)
                .await
                .map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }

    /// The key-value namespace `namespace` with every write linked to this
    /// session.
    fn kv(&self, namespace: String) -> PyResult<crate::kv::PyKv> {
        Ok(crate::kv::PyKv::linked(
            self.inner.scope().laser().clone(),
            namespace,
            self.inner.reference().map_err(to_pyerr)?,
        ))
    }

    /// The knowledge graph `name` with every upsert linked to this session,
    /// stamped with this agent as producer and the current record as source.
    fn linked_graph(&self, name: String) -> crate::graph::PyGraph {
        crate::graph::PyGraph::linked(self.inner.clone(), name)
    }

    /// This session's memory with every remembered item stamped with this
    /// agent as producer and the current record as origin.
    fn linked_memory(&self) -> PyScopedMemory {
        let linked = self.inner.linked_memory();
        PyScopedMemory::new(
            Backend::new(
                self.inner
                    .scope()
                    .laser()
                    .memory(self.inner.config().memory_namespace_name()),
            ),
            self.inner.conversation(),
        )
        .lineage(linked.origin().cloned(), linked.producer().cloned())
    }
}

/// The held work of one session that no agent reported handled.
#[gen_stub_pyclass]
#[pyclass(name = "ParkedRecords", frozen)]
pub struct PyParkedRecords {
    /// The held records, each a `SessionParking` dict, by source position.
    #[pyo3(get)]
    records: Vec<Py<PyAny>>,
    /// False when older held records may be missing from the bounded read.
    #[pyo3(get)]
    complete: bool,
}

/// The operator requests recorded for one session.
#[gen_stub_pyclass]
#[pyclass(name = "PendingControl", frozen)]
pub struct PyPendingControl {
    /// A pause was requested and no later resume lifted it.
    #[pyo3(get)]
    pause_requested: bool,
    /// A cancel was requested, or an operator forced the session canceled.
    #[pyo3(get)]
    cancel_requested: bool,
}

/// A session handed to an agent: written as submitted on the lane, with a
/// command in the agent's inbox. Chain the settings, then `await send()`.
#[gen_stub_pyclass]
#[pyclass(name = "SubmitBuilder")]
pub struct PySubmitBuilder {
    inner: Mutex<Option<SubmitBuilder>>,
}

impl PySubmitBuilder {
    fn update(&self, change: impl FnOnce(SubmitBuilder) -> SubmitBuilder) -> PyResult<()> {
        let mut inner = self.inner.lock().expect("submit builder lock");
        let builder = inner
            .take()
            .ok_or_else(|| InvalidError::new_err("this submission was already sent"))?;
        *inner = Some(change(builder));
        Ok(())
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PySubmitBuilder {
    /// The agent that submits the session. Required.
    fn from_<'py>(slf: PyRef<'py, Self>, submitter: &str) -> PyResult<PyRef<'py, Self>> {
        let submitter = agent(submitter)?;
        slf.update(|builder| builder.from(submitter))?;
        Ok(slf)
    }

    /// The command operation, `invoke_agent` unless set.
    fn operation<'py>(slf: PyRef<'py, Self>, operation: String) -> PyResult<PyRef<'py, Self>> {
        slf.update(|builder| builder.operation(operation))?;
        Ok(slf)
    }

    /// Name the session, deriving its id from the stream, namespace, and label.
    fn label<'py>(slf: PyRef<'py, Self>, label: String) -> PyResult<PyRef<'py, Self>> {
        slf.update(|builder| builder.label(label))?;
        Ok(slf)
    }

    /// The namespace a labeled session id derives under.
    fn namespace<'py>(slf: PyRef<'py, Self>, namespace: String) -> PyResult<PyRef<'py, Self>> {
        slf.update(|builder| builder.namespace(namespace))?;
        Ok(slf)
    }

    /// The token and cost ceiling a reader compares the session's usage with.
    fn budget<'py>(
        slf: PyRef<'py, Self>,
        budget: crate::session::PyBudget,
    ) -> PyResult<PyRef<'py, Self>> {
        slf.update(|builder| builder.budget(budget.inner))?;
        Ok(slf)
    }

    /// One searchable tag.
    fn tag<'py>(slf: PyRef<'py, Self>, tag: String) -> PyResult<PyRef<'py, Self>> {
        slf.update(|builder| builder.tag(tag))?;
        Ok(slf)
    }

    /// Write the submitted status and the command. Returns `Submitted`.
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let builder = self
            .inner
            .lock()
            .expect("submit builder lock")
            .take()
            .ok_or_else(|| InvalidError::new_err("this submission was already sent"))?;
        future_into_py(py, async move {
            let submitted = builder.send().await.map_err(to_pyerr)?;
            Ok(PySubmitted {
                session: submitted.session.to_string(),
                correlation: submitted.correlation.to_string(),
            })
        })
    }
}

/// What a submission wrote: the session id and the command's correlation.
#[gen_stub_pyclass]
#[pyclass(name = "Submitted", frozen)]
pub struct PySubmitted {
    session: String,
    correlation: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySubmitted {
    #[getter]
    fn session(&self) -> &str {
        &self.session
    }

    #[getter]
    fn correlation(&self) -> &str {
        &self.correlation
    }

    fn __repr__(&self) -> String {
        format!(
            "Submitted(session={:?}, correlation={:?})",
            self.session, self.correlation
        )
    }
}

/// Operator control of one session, sent on `agent.control`. Only accounts
/// with send permission on that topic can use it.
#[gen_stub_pyclass]
#[pyclass(name = "SessionControl", frozen)]
pub struct PySessionControl {
    sessions: laser_sdk::agent::Sessions,
    stream: String,
    session: ConversationId,
    operator: Option<AgentId>,
    key: Option<laser_sdk::sign::SigningKey>,
    participants: Option<Vec<AgentId>>,
}

impl PySessionControl {
    fn verb<'py>(
        &self,
        py: Python<'py>,
        verb: fn(laser_sdk::agent::SessionControl) -> ControlFuture,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut control = self.sessions.control(&self.stream, self.session);
        if let Some(operator) = &self.operator {
            control = control.as_operator(operator.clone());
        }
        if let Some(key) = &self.key {
            control = control.signed_by(key.clone());
        }
        if let Some(participants) = &self.participants {
            control = control.participants(participants.clone());
        }
        future_into_py(py, async move {
            let receipt = verb(control).await.map_err(to_pyerr)?;
            Ok(PyAgdxReceipt::from(receipt))
        })
    }
}

type ControlFuture = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<laser_sdk::agent::AgdxReceipt, laser_sdk::error::LaserError>,
            > + Send,
    >,
>;

#[gen_stub_pymethods]
#[pymethods]
impl PySessionControl {
    /// The operator identity the control records carry. Required.
    fn as_operator(&self, operator: &str) -> PyResult<PySessionControl> {
        Ok(PySessionControl {
            sessions: self.sessions.clone(),
            stream: self.stream.clone(),
            session: self.session,
            operator: Some(agent(operator)?),
            key: self.key.clone(),
            participants: self.participants.clone(),
        })
    }

    /// The agents whose acknowledgments complete a pause, in place of the
    /// set read from the session lane.
    fn participants(&self, participants: Vec<String>) -> PyResult<PySessionControl> {
        Ok(PySessionControl {
            sessions: self.sessions.clone(),
            stream: self.stream.clone(),
            session: self.session,
            operator: self.operator.clone(),
            key: self.key.clone(),
            participants: Some(
                participants
                    .iter()
                    .map(|name| agent(name))
                    .collect::<PyResult<Vec<_>>>()?,
            ),
        })
    }

    /// Sign every control record with `key`, so a verifying reader can prove
    /// which operator sent it.
    fn signed_by(&self, key: &crate::sign::PySigningKey) -> PySessionControl {
        PySessionControl {
            sessions: self.sessions.clone(),
            stream: self.stream.clone(),
            session: self.session,
            operator: self.operator.clone(),
            key: Some(key.inner.as_ref().clone()),
            participants: self.participants.clone(),
        }
    }

    /// Ask the session's agents to pause. Returns an `AgdxReceipt`.
    fn pause<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.verb(py, |control| Box::pin(async move { control.pause().await }))
    }

    /// Ask a paused session's agents to resume.
    fn resume<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.verb(py, |control| {
            Box::pin(async move { control.resume().await })
        })
    }

    /// Ask the session's agents to end it as canceled at their next boundary.
    fn cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.verb(py, |control| {
            Box::pin(async move { control.cancel().await })
        })
    }

    /// End the session as canceled without its agents, for a session whose
    /// agent is gone.
    fn force_cancel<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.verb(py, |control| {
            Box::pin(async move { control.force_cancel().await })
        })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessions {
    /// Hand a new session to `agent` with `input` (str or bytes) as its first
    /// command. Chain `from_(submitter)` and `await send()`.
    fn submit(&self, agent_id: &str, input: &Bound<'_, PyAny>) -> PyResult<PySubmitBuilder> {
        Ok(PySubmitBuilder {
            inner: Mutex::new(Some(
                self.inner.submit(agent(agent_id)?, payload_bytes(input)?),
            )),
        })
    }

    /// Operator control of the session `id` in `stream`.
    fn control(&self, stream: String, id: &str) -> PyResult<PySessionControl> {
        Ok(PySessionControl {
            sessions: self.inner.clone(),
            stream,
            session: ConversationId::from_str(id).map_err(|error| to_pyerr(error.into()))?,
            operator: None,
            key: None,
            participants: None,
        })
    }
}

use crate::agent_runtime::static_topic;
use crate::async_bridge::future_into_py;
use crate::blob::PyBlobStore;
use crate::client::PyLaser;
use crate::convert::{duration_seconds, payload_bytes, py_to_de, py_to_value, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use crate::sign::envelope_of;
use laser_sdk::LaserError;
use laser_sdk::agent::{Agdx, AgdxSend, AgdxStream};
use laser_sdk::wire::agent::{
    AgentEnvelope, AgentErrorBody, AgentId, ChannelId, ConversationId, CorrelationId,
    IdempotencyKey, LogPosition, RecordId, Signature, TaskState, TokenUsage,
};
use laser_sdk::wire::content::ContentType;
use laser_sdk::wire::query::Value;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

fn wire_agent(value: &str) -> PyResult<AgentId> {
    value.parse().map_err(|e| to_pyerr(LaserError::from(e)))
}

fn wire_conversation(value: &str) -> PyResult<ConversationId> {
    ConversationId::from_str(value)
        .map_err(|e| InvalidError::new_err(format!("invalid conversation id: {e}")))
}

fn wire_record(value: &str) -> PyResult<RecordId> {
    RecordId::from_str(value).map_err(|e| InvalidError::new_err(format!("invalid record id: {e}")))
}

fn wire_correlation(value: &str) -> PyResult<CorrelationId> {
    CorrelationId::from_str(value)
        .map_err(|e| InvalidError::new_err(format!("invalid correlation id: {e}")))
}

fn parse_content_type(value: Option<String>) -> PyResult<Option<ContentType>> {
    match value {
        Some(value) => ContentType::from_str(&value)
            .map(Some)
            .map_err(|_| InvalidError::new_err(format!("unknown content type '{value}'"))),
        None => Ok(None),
    }
}

// The envelope refinements every body-carrying verb shares.
struct SendOptions {
    cause: Option<RecordId>,
    cause_at: Option<LogPosition>,
    deadline_micros: Option<u64>,
    idempotency_key: Option<IdempotencyKey>,
    metadata: Vec<(String, Value)>,
    tool: Option<String>,
    usage: Option<TokenUsage>,
    claim_check: Option<(PyBlobStore, usize)>,
    signed_by: Option<Arc<laser_sdk::sign::SigningKey>>,
}

impl SendOptions {
    #[allow(clippy::too_many_arguments)]
    fn new(
        cause: Option<String>,
        cause_at: Option<PyLogPosition>,
        deadline_micros: Option<u64>,
        idempotency_key: Option<String>,
        metadata: Option<&Bound<'_, PyDict>>,
        tool: Option<String>,
        usage: Option<&Bound<'_, PyAny>>,
        claim_check: Option<(Py<PyAny>, usize)>,
        signed_by: Option<PyRef<'_, crate::sign::PySigningKey>>,
    ) -> PyResult<Self> {
        let cause = cause
            .map(|value| {
                RecordId::from_str(&value)
                    .map_err(|e| InvalidError::new_err(format!("invalid cause record id: {e}")))
            })
            .transpose()?;
        if cause_at.is_some() && cause.is_none() {
            return Err(InvalidError::new_err("cause_at requires cause"));
        }
        let cause_at = cause_at.map(|position| position.inner);
        let idempotency_key = idempotency_key
            .map(|value| {
                IdempotencyKey::from_str(&value)
                    .map_err(|e| InvalidError::new_err(format!("invalid idempotency key: {e}")))
            })
            .transpose()?;
        let metadata = match metadata {
            Some(entries) => entries
                .iter()
                .map(|(key, value)| Ok((key.extract::<String>()?, py_to_value(&value)?)))
                .collect::<PyResult<Vec<_>>>()?,
            None => Vec::new(),
        };
        Ok(Self {
            cause,
            cause_at,
            deadline_micros,
            idempotency_key,
            metadata,
            tool,
            usage: usage.map(py_to_de).transpose()?,
            claim_check: claim_check.map(|(hooks, threshold)| (PyBlobStore { hooks }, threshold)),
            signed_by: signed_by.map(|key| key.inner.clone()),
        })
    }

    fn apply<'a>(&'a self, mut send: AgdxSend<'a>) -> AgdxSend<'a> {
        if let Some(cause) = self.cause {
            send = send.with_cause(cause, self.cause_at);
        }
        if let Some(deadline_micros) = self.deadline_micros {
            send = send.with_deadline_micros(deadline_micros);
        }
        if let Some(key) = &self.idempotency_key {
            send = send.with_idempotency_key(key.clone());
        }
        for (key, value) in &self.metadata {
            send = send.with_metadata(key.clone(), value.clone());
        }
        if let Some(tool) = &self.tool {
            send = send.with_tool(tool.clone());
        }
        if let Some(usage) = self.usage {
            send = send.with_usage(usage);
        }
        if let Some((store, threshold)) = &self.claim_check {
            send = send.claim_check(store, *threshold);
        }
        // A per-send key replaces the producer's `signing_key` for this send.
        if let Some(key) = &self.signed_by {
            send = send.signed_by(key);
        }
        send
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A typed Agent Data Exchange Protocol producer publishing as `source`
    /// within `conversation_id` on `topic`. Every send is a validated AGDX
    /// envelope. A `signing_key` signs `command`, `respond`, `emit`, `status`, and `fail` sends.
    /// Chunk streams and `request_input` use the Rust unsigned helpers.
    #[pyo3(signature = (topic, source, conversation_id, *, signing_key=None))]
    fn agdx(
        &self,
        topic: String,
        source: String,
        conversation_id: String,
        signing_key: Option<&crate::sign::PySigningKey>,
    ) -> PyResult<PyAgdx> {
        let source = wire_agent(&source)?;
        let conversation = wire_conversation(&conversation_id)?;
        Ok(PyAgdx {
            inner: self.inner.agdx(static_topic(topic)?, source, conversation),
            signing_key: signing_key.map(|key| key.inner.clone()),
        })
    }
}

/// A2A's task lifecycle, riding the wire as a permanent `u8` code. The variants
/// are class attributes (`TaskState.Working`), an unknown code passes through
/// as `TaskState.Unrecognized(code)`, and `str()` is the A2A kebab-case name.
#[gen_stub_pyclass]
#[pyclass(name = "TaskState", frozen, eq, hash, from_py_object)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PyTaskState {
    pub(crate) inner: TaskState,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTaskState {
    #[classattr]
    #[pyo3(name = "Submitted")]
    fn submitted() -> PyTaskState {
        PyTaskState::from(TaskState::Submitted)
    }

    #[classattr]
    #[pyo3(name = "Working")]
    fn working() -> PyTaskState {
        PyTaskState::from(TaskState::Working)
    }

    #[classattr]
    #[pyo3(name = "InputRequired")]
    fn input_required() -> PyTaskState {
        PyTaskState::from(TaskState::InputRequired)
    }

    #[classattr]
    #[pyo3(name = "Completed")]
    fn completed() -> PyTaskState {
        PyTaskState::from(TaskState::Completed)
    }

    #[classattr]
    #[pyo3(name = "Canceled")]
    fn canceled() -> PyTaskState {
        PyTaskState::from(TaskState::Canceled)
    }

    #[classattr]
    #[pyo3(name = "Failed")]
    fn failed() -> PyTaskState {
        PyTaskState::from(TaskState::Failed)
    }

    #[classattr]
    #[pyo3(name = "Rejected")]
    fn rejected() -> PyTaskState {
        PyTaskState::from(TaskState::Rejected)
    }

    #[classattr]
    #[pyo3(name = "AuthRequired")]
    fn auth_required() -> PyTaskState {
        PyTaskState::from(TaskState::AuthRequired)
    }

    #[classattr]
    #[pyo3(name = "Unknown")]
    fn unknown() -> PyTaskState {
        PyTaskState::from(TaskState::Unknown)
    }

    /// A code this build does not know: passed through, treated as
    /// non-terminal.
    #[staticmethod]
    #[pyo3(name = "Unrecognized")]
    fn unrecognized(code: u8) -> PyTaskState {
        PyTaskState::from(TaskState::Unrecognized(code))
    }

    /// The state for a wire code. Unknown codes become `Unrecognized`.
    #[staticmethod]
    fn from_code(code: u8) -> PyTaskState {
        PyTaskState::from(TaskState::from_code(code))
    }

    /// The pinned wire code.
    fn code(&self) -> u8 {
        self.inner.code()
    }

    /// Whether this state ends the task (A2A's terminal set). Unrecognized
    /// codes are non-terminal.
    fn is_terminal(&self) -> bool {
        self.inner.is_terminal()
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("TaskState.{:?}", self.inner)
    }
}

impl From<TaskState> for PyTaskState {
    fn from(inner: TaskState) -> Self {
        Self { inner }
    }
}

/// Build a `command` envelope dict (expects a reply or effect under
/// `correlation`). The keywords refine it: `target`, `cause` with
/// `cause_at` (a `LogPosition`), `idempotency_key`,
/// `deadline_micros`, `terminal` (the finish reason, marking it last),
/// `task_state`, `operation`, `tool`, `usage`, `metadata`, the must-understand
/// bits `requiring`, and a `signature` dict.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (record, conversation, source, correlation, body, *, target=None, cause=None, cause_at=None, idempotency_key=None, deadline_micros=None, terminal=None, task_state=None, operation=None, tool=None, usage=None, metadata=None, requiring=0, signature=None))]
#[allow(clippy::too_many_arguments)]
pub fn command_envelope(
    py: Python<'_>,
    record: String,
    conversation: String,
    source: String,
    correlation: String,
    body: &Bound<'_, PyAny>,
    target: Option<String>,
    cause: Option<String>,
    cause_at: Option<PyLogPosition>,
    idempotency_key: Option<String>,
    deadline_micros: Option<u64>,
    terminal: Option<String>,
    task_state: Option<PyTaskState>,
    operation: Option<String>,
    tool: Option<String>,
    usage: Option<&Bound<'_, PyAny>>,
    metadata: Option<&Bound<'_, PyDict>>,
    requiring: u64,
    signature: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let envelope = AgentEnvelope::command(
        wire_record(&record)?,
        wire_conversation(&conversation)?,
        wire_agent(&source)?,
        wire_correlation(&correlation)?,
        payload_bytes(body)?,
    );
    let refinements = Refinements {
        target,
        cause,
        cause_at,
        correlation: None,
        idempotency_key,
        deadline_micros,
        terminal,
        task_state,
        operation,
        tool,
        usage,
        metadata,
        requiring,
        signature,
    };
    ser_to_py(py, &refinements.apply(envelope)?)
}

/// Build a `response` envelope dict, the paired answer to a command (same
/// `correlation`). Takes the `command_envelope` refinement keywords.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (record, conversation, source, correlation, body, *, target=None, cause=None, cause_at=None, idempotency_key=None, deadline_micros=None, terminal=None, task_state=None, operation=None, tool=None, usage=None, metadata=None, requiring=0, signature=None))]
#[allow(clippy::too_many_arguments)]
pub fn response_envelope(
    py: Python<'_>,
    record: String,
    conversation: String,
    source: String,
    correlation: String,
    body: &Bound<'_, PyAny>,
    target: Option<String>,
    cause: Option<String>,
    cause_at: Option<PyLogPosition>,
    idempotency_key: Option<String>,
    deadline_micros: Option<u64>,
    terminal: Option<String>,
    task_state: Option<PyTaskState>,
    operation: Option<String>,
    tool: Option<String>,
    usage: Option<&Bound<'_, PyAny>>,
    metadata: Option<&Bound<'_, PyDict>>,
    requiring: u64,
    signature: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let envelope = AgentEnvelope::response(
        wire_record(&record)?,
        wire_conversation(&conversation)?,
        wire_agent(&source)?,
        wire_correlation(&correlation)?,
        payload_bytes(body)?,
    );
    let refinements = Refinements {
        target,
        cause,
        cause_at,
        correlation: None,
        idempotency_key,
        deadline_micros,
        terminal,
        task_state,
        operation,
        tool,
        usage,
        metadata,
        requiring,
        signature,
    };
    ser_to_py(py, &refinements.apply(envelope)?)
}

/// Build an `error` terminal envelope dict for `correlation`. `body` is the
/// encoded `AgentErrorBody`. Takes the `command_envelope` refinement keywords.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (record, conversation, source, correlation, body, *, target=None, cause=None, cause_at=None, idempotency_key=None, deadline_micros=None, terminal=None, task_state=None, operation=None, tool=None, usage=None, metadata=None, requiring=0, signature=None))]
#[allow(clippy::too_many_arguments)]
pub fn error_envelope(
    py: Python<'_>,
    record: String,
    conversation: String,
    source: String,
    correlation: String,
    body: &Bound<'_, PyAny>,
    target: Option<String>,
    cause: Option<String>,
    cause_at: Option<PyLogPosition>,
    idempotency_key: Option<String>,
    deadline_micros: Option<u64>,
    terminal: Option<String>,
    task_state: Option<PyTaskState>,
    operation: Option<String>,
    tool: Option<String>,
    usage: Option<&Bound<'_, PyAny>>,
    metadata: Option<&Bound<'_, PyDict>>,
    requiring: u64,
    signature: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let envelope = AgentEnvelope::error(
        wire_record(&record)?,
        wire_conversation(&conversation)?,
        wire_agent(&source)?,
        wire_correlation(&correlation)?,
        payload_bytes(body)?,
    );
    let refinements = Refinements {
        target,
        cause,
        cause_at,
        correlation: None,
        idempotency_key,
        deadline_micros,
        terminal,
        task_state,
        operation,
        tool,
        usage,
        metadata,
        requiring,
        signature,
    };
    ser_to_py(py, &refinements.apply(envelope)?)
}

/// Build an `event` envelope dict (expects nothing). Takes the
/// `command_envelope` refinement keywords plus an optional `correlation`.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (record, conversation, source, body, *, target=None, cause=None, cause_at=None, correlation=None, idempotency_key=None, deadline_micros=None, terminal=None, task_state=None, operation=None, tool=None, usage=None, metadata=None, requiring=0, signature=None))]
#[allow(clippy::too_many_arguments)]
pub fn event_envelope(
    py: Python<'_>,
    record: String,
    conversation: String,
    source: String,
    body: &Bound<'_, PyAny>,
    target: Option<String>,
    cause: Option<String>,
    cause_at: Option<PyLogPosition>,
    correlation: Option<String>,
    idempotency_key: Option<String>,
    deadline_micros: Option<u64>,
    terminal: Option<String>,
    task_state: Option<PyTaskState>,
    operation: Option<String>,
    tool: Option<String>,
    usage: Option<&Bound<'_, PyAny>>,
    metadata: Option<&Bound<'_, PyDict>>,
    requiring: u64,
    signature: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let envelope = AgentEnvelope::event(
        wire_record(&record)?,
        wire_conversation(&conversation)?,
        wire_agent(&source)?,
        payload_bytes(body)?,
    );
    let refinements = Refinements {
        target,
        cause,
        cause_at,
        correlation,
        idempotency_key,
        deadline_micros,
        terminal,
        task_state,
        operation,
        tool,
        usage,
        metadata,
        requiring,
        signature,
    };
    ser_to_py(py, &refinements.apply(envelope)?)
}

/// Build a `chunk` envelope dict of the stream `channel`, ordered by
/// `sequence`. Mark the final one with `terminal`. Takes the
/// `command_envelope` refinement keywords.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (conversation, source, correlation, channel, sequence, body, *, target=None, cause=None, cause_at=None, idempotency_key=None, deadline_micros=None, terminal=None, task_state=None, operation=None, tool=None, usage=None, metadata=None, requiring=0, signature=None))]
#[allow(clippy::too_many_arguments)]
pub fn chunk_envelope(
    py: Python<'_>,
    conversation: String,
    source: String,
    correlation: String,
    channel: String,
    sequence: u64,
    body: &Bound<'_, PyAny>,
    target: Option<String>,
    cause: Option<String>,
    cause_at: Option<PyLogPosition>,
    idempotency_key: Option<String>,
    deadline_micros: Option<u64>,
    terminal: Option<String>,
    task_state: Option<PyTaskState>,
    operation: Option<String>,
    tool: Option<String>,
    usage: Option<&Bound<'_, PyAny>>,
    metadata: Option<&Bound<'_, PyDict>>,
    requiring: u64,
    signature: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let channel = ChannelId::from_str(&channel)
        .map_err(|e| InvalidError::new_err(format!("invalid channel id: {e}")))?;
    let envelope = AgentEnvelope::chunk(
        wire_conversation(&conversation)?,
        wire_agent(&source)?,
        wire_correlation(&correlation)?,
        channel,
        sequence,
        payload_bytes(body)?,
    );
    let refinements = Refinements {
        target,
        cause,
        cause_at,
        correlation: None,
        idempotency_key,
        deadline_micros,
        terminal,
        task_state,
        operation,
        tool,
        usage,
        metadata,
        requiring,
        signature,
    };
    ser_to_py(py, &refinements.apply(envelope)?)
}

/// Build a `status` envelope dict discriminated by `operation` (`task`,
/// `card`, `progress`). Task updates also need `correlation` and
/// `task_state`. Takes the `command_envelope` refinement keywords.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (record, conversation, source, operation, *, target=None, cause=None, cause_at=None, correlation=None, idempotency_key=None, deadline_micros=None, terminal=None, task_state=None, tool=None, usage=None, metadata=None, requiring=0, signature=None))]
#[allow(clippy::too_many_arguments)]
pub fn status_envelope(
    py: Python<'_>,
    record: String,
    conversation: String,
    source: String,
    operation: String,
    target: Option<String>,
    cause: Option<String>,
    cause_at: Option<PyLogPosition>,
    correlation: Option<String>,
    idempotency_key: Option<String>,
    deadline_micros: Option<u64>,
    terminal: Option<String>,
    task_state: Option<PyTaskState>,
    tool: Option<String>,
    usage: Option<&Bound<'_, PyAny>>,
    metadata: Option<&Bound<'_, PyDict>>,
    requiring: u64,
    signature: Option<&Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    let envelope = AgentEnvelope::status(
        wire_record(&record)?,
        wire_conversation(&conversation)?,
        wire_agent(&source)?,
        operation,
    );
    let refinements = Refinements {
        target,
        cause,
        cause_at,
        correlation,
        idempotency_key,
        deadline_micros,
        terminal,
        task_state,
        operation: None,
        tool,
        usage,
        metadata,
        requiring,
        signature,
    };
    ser_to_py(py, &refinements.apply(envelope)?)
}

/// The must-understand bits of `envelope` (a dict or its encoded bytes) that
/// are missing from the receiver's `understood` set. Non-zero means the
/// receiver must reject or dead-letter the message.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn unmet_requirements(envelope: &Bound<'_, PyAny>, understood: u64) -> PyResult<u64> {
    Ok(envelope_of(envelope)?.unmet_requirements(understood))
}

/// Check a `signature` dict against its scheme's registered lengths. Unknown
/// scheme codes pass. Raises `ValidateError` on a violation.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn validate_signature(signature: &Bound<'_, PyAny>) -> PyResult<()> {
    let signature: Signature = py_to_de(signature)?;
    signature
        .validate()
        .map_err(|error| crate::errors::validate_error(&error))
}

// The builder refinements every envelope constructor takes as keywords.
struct Refinements<'a, 'py> {
    target: Option<String>,
    cause: Option<String>,
    cause_at: Option<PyLogPosition>,
    correlation: Option<String>,
    idempotency_key: Option<String>,
    deadline_micros: Option<u64>,
    terminal: Option<String>,
    task_state: Option<PyTaskState>,
    operation: Option<String>,
    tool: Option<String>,
    usage: Option<&'a Bound<'py, PyAny>>,
    metadata: Option<&'a Bound<'py, PyDict>>,
    requiring: u64,
    signature: Option<&'a Bound<'py, PyAny>>,
}

impl Refinements<'_, '_> {
    fn apply(self, mut envelope: AgentEnvelope) -> PyResult<AgentEnvelope> {
        let options = SendOptions::new(
            self.cause,
            self.cause_at,
            self.deadline_micros,
            self.idempotency_key,
            self.metadata,
            self.tool,
            self.usage,
            None,
            None,
        )?;
        if let Some(target) = self.target {
            envelope = envelope.with_target(wire_agent(&target)?);
        }
        if let Some(cause) = options.cause {
            envelope = envelope.with_cause(cause, options.cause_at);
        }
        if let Some(correlation) = self.correlation {
            envelope = envelope.with_correlation(wire_correlation(&correlation)?);
        }
        if let Some(key) = options.idempotency_key {
            envelope = envelope.with_idempotency_key(key);
        }
        if let Some(deadline_micros) = options.deadline_micros {
            envelope = envelope.with_deadline_micros(deadline_micros);
        }
        if let Some(finish_reason) = self.terminal {
            envelope = envelope.terminal(finish_reason);
        }
        if let Some(state) = self.task_state {
            envelope = envelope.with_task_state(state.inner);
        }
        if let Some(operation) = self.operation {
            envelope = envelope.with_operation(operation);
        }
        if let Some(tool) = options.tool {
            envelope = envelope.with_tool(tool);
        }
        if let Some(usage) = options.usage {
            envelope = envelope.with_usage(usage);
        }
        for (key, value) in options.metadata {
            envelope = envelope.with_metadata(key, value);
        }
        if self.requiring != 0 {
            envelope = envelope.requiring(self.requiring);
        }
        if let Some(signature) = self.signature {
            envelope = envelope.with_signature(py_to_de(signature)?);
        }
        Ok(envelope)
    }
}

/// The typed AGDX producer over one topic and conversation.
#[gen_stub_pyclass]
#[pyclass(name = "Agdx", frozen)]
pub struct PyAgdx {
    inner: Agdx,
    signing_key: Option<Arc<laser_sdk::sign::SigningKey>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAgdx {
    /// Publish a `command` (expects a reply or effect under `correlation`).
    /// Returns the minted record id. `cause_at` is a `LogPosition` and requires `cause`.
    /// `cause`, `deadline_micros`,
    /// `idempotency_key`, `metadata`, `tool`, and `usage` refine the envelope.
    /// `claim_check=(store, threshold_bytes)` externalizes a large body.
    /// `signed_by` signs this one send, in place of the producer's
    /// `signing_key`, on every verb that publishes an envelope.
    #[pyo3(signature = (correlation, body, *, operation=None, content_type=None, target=None, cause=None, cause_at=None, deadline_micros=None, idempotency_key=None, metadata=None, tool=None, usage=None, claim_check=None, signed_by=None))]
    #[allow(clippy::too_many_arguments)]
    fn command<'py>(
        &self,
        py: Python<'py>,
        correlation: String,
        body: &Bound<'_, PyAny>,
        operation: Option<String>,
        content_type: Option<String>,
        target: Option<String>,
        cause: Option<String>,
        cause_at: Option<PyLogPosition>,
        deadline_micros: Option<u64>,
        idempotency_key: Option<String>,
        metadata: Option<&Bound<'_, PyDict>>,
        tool: Option<String>,
        usage: Option<&Bound<'_, PyAny>>,
        claim_check: Option<(Py<PyAny>, usize)>,
        signed_by: Option<PyRef<'_, crate::sign::PySigningKey>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let agdx = self.inner.clone();
        let options = SendOptions::new(
            cause,
            cause_at,
            deadline_micros,
            idempotency_key,
            metadata,
            tool,
            usage,
            claim_check,
            signed_by,
        )?;
        let signing_key = self.signing_key.clone();
        let correlation = wire_correlation(&correlation)?;
        let body = payload_bytes(body)?;
        let content_type = parse_content_type(content_type)?;
        let target = target.map(|t| wire_agent(&t)).transpose()?;
        future_into_py(py, async move {
            let mut send = agdx.command(correlation, body);
            if let Some(key) = signing_key.as_ref() {
                send = send.signed_by(key);
            }
            if let Some(operation) = operation {
                send = send.with_operation(operation);
            }
            if let Some(content_type) = content_type {
                send = send.content_type(content_type);
            }
            if let Some(target) = target {
                send = send.with_target(target);
            }
            send = options.apply(send);
            let record = send.send().await.map_err(to_pyerr)?;
            Ok(record.map(|id| id.to_string()))
        })
    }

    /// Publish a `response` (the paired answer to a command, same `correlation`).
    #[pyo3(signature = (correlation, body, *, operation=None, content_type=None, target=None, cause=None, cause_at=None, deadline_micros=None, idempotency_key=None, metadata=None, tool=None, usage=None, claim_check=None, signed_by=None))]
    #[allow(clippy::too_many_arguments)]
    fn respond<'py>(
        &self,
        py: Python<'py>,
        correlation: String,
        body: &Bound<'_, PyAny>,
        operation: Option<String>,
        content_type: Option<String>,
        target: Option<String>,
        cause: Option<String>,
        cause_at: Option<PyLogPosition>,
        deadline_micros: Option<u64>,
        idempotency_key: Option<String>,
        metadata: Option<&Bound<'_, PyDict>>,
        tool: Option<String>,
        usage: Option<&Bound<'_, PyAny>>,
        claim_check: Option<(Py<PyAny>, usize)>,
        signed_by: Option<PyRef<'_, crate::sign::PySigningKey>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let agdx = self.inner.clone();
        let options = SendOptions::new(
            cause,
            cause_at,
            deadline_micros,
            idempotency_key,
            metadata,
            tool,
            usage,
            claim_check,
            signed_by,
        )?;
        let signing_key = self.signing_key.clone();
        let correlation = wire_correlation(&correlation)?;
        let body = payload_bytes(body)?;
        let content_type = parse_content_type(content_type)?;
        let target = target.map(|t| wire_agent(&t)).transpose()?;
        future_into_py(py, async move {
            let mut send = agdx.respond(correlation, body);
            if let Some(key) = signing_key.as_ref() {
                send = send.signed_by(key);
            }
            if let Some(operation) = operation {
                send = send.with_operation(operation);
            }
            if let Some(content_type) = content_type {
                send = send.content_type(content_type);
            }
            if let Some(target) = target {
                send = send.with_target(target);
            }
            send = options.apply(send);
            let record = send.send().await.map_err(to_pyerr)?;
            Ok(record.map(|id| id.to_string()))
        })
    }

    /// Publish an `event` (expects nothing back).
    #[pyo3(signature = (body, *, operation=None, content_type=None, target=None, cause=None, cause_at=None, deadline_micros=None, idempotency_key=None, metadata=None, tool=None, usage=None, claim_check=None, signed_by=None))]
    #[allow(clippy::too_many_arguments)]
    fn emit<'py>(
        &self,
        py: Python<'py>,
        body: &Bound<'_, PyAny>,
        operation: Option<String>,
        content_type: Option<String>,
        target: Option<String>,
        cause: Option<String>,
        cause_at: Option<PyLogPosition>,
        deadline_micros: Option<u64>,
        idempotency_key: Option<String>,
        metadata: Option<&Bound<'_, PyDict>>,
        tool: Option<String>,
        usage: Option<&Bound<'_, PyAny>>,
        claim_check: Option<(Py<PyAny>, usize)>,
        signed_by: Option<PyRef<'_, crate::sign::PySigningKey>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let agdx = self.inner.clone();
        let options = SendOptions::new(
            cause,
            cause_at,
            deadline_micros,
            idempotency_key,
            metadata,
            tool,
            usage,
            claim_check,
            signed_by,
        )?;
        let signing_key = self.signing_key.clone();
        let body = payload_bytes(body)?;
        let content_type = parse_content_type(content_type)?;
        let target = target.map(|t| wire_agent(&t)).transpose()?;
        future_into_py(py, async move {
            let mut send = agdx.emit(body);
            if let Some(key) = signing_key.as_ref() {
                send = send.signed_by(key);
            }
            if let Some(operation) = operation {
                send = send.with_operation(operation);
            }
            if let Some(content_type) = content_type {
                send = send.content_type(content_type);
            }
            if let Some(target) = target {
                send = send.with_target(target);
            }
            send = options.apply(send);
            let record = send.send().await.map_err(to_pyerr)?;
            Ok(record.map(|id| id.to_string()))
        })
    }

    /// Publish a `status` signal. Task status updates require both
    /// `correlation` and `task_state`. Set `last` for a terminal update.
    #[pyo3(signature = (operation, *, correlation=None, task_state=None, body=None, content_type=None, target=None, last=false, cause=None, cause_at=None, deadline_micros=None, idempotency_key=None, metadata=None, tool=None, usage=None, claim_check=None, signed_by=None))]
    #[allow(clippy::too_many_arguments)]
    fn status<'py>(
        &self,
        py: Python<'py>,
        operation: String,
        correlation: Option<String>,
        task_state: Option<PyTaskState>,
        body: Option<&Bound<'_, PyAny>>,
        content_type: Option<String>,
        target: Option<String>,
        last: bool,
        cause: Option<String>,
        cause_at: Option<PyLogPosition>,
        deadline_micros: Option<u64>,
        idempotency_key: Option<String>,
        metadata: Option<&Bound<'_, PyDict>>,
        tool: Option<String>,
        usage: Option<&Bound<'_, PyAny>>,
        claim_check: Option<(Py<PyAny>, usize)>,
        signed_by: Option<PyRef<'_, crate::sign::PySigningKey>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let agdx = self.inner.clone();
        let options = SendOptions::new(
            cause,
            cause_at,
            deadline_micros,
            idempotency_key,
            metadata,
            tool,
            usage,
            claim_check,
            signed_by,
        )?;
        let correlation = correlation
            .map(|value| wire_correlation(&value))
            .transpose()?;
        let task_state = task_state.map(|state| state.inner);
        let signing_key = self.signing_key.clone();
        let body = body.map(payload_bytes).transpose()?;
        let content_type = parse_content_type(content_type)?;
        let target = target.map(|value| wire_agent(&value)).transpose()?;
        future_into_py(py, async move {
            let mut send = agdx.status(operation);
            if let Some(key) = signing_key.as_ref() {
                send = send.signed_by(key);
            }
            if let Some(correlation) = correlation {
                send = send.with_correlation(correlation);
            }
            if let Some(task_state) = task_state {
                send = send.with_task_state(task_state);
            }
            if let Some(body) = body {
                send = send.body(body);
            }
            if let Some(content_type) = content_type {
                send = send.content_type(content_type);
            }
            if let Some(target) = target {
                send = send.with_target(target);
            }
            if last {
                send = send.last();
            }
            send = options.apply(send);
            let record = send.send().await.map_err(to_pyerr)?;
            Ok(record.map(|id| id.to_string()))
        })
    }

    /// Publish a structured `error` terminal for `correlation`.
    #[pyo3(signature = (correlation, error, *, target=None, cause=None, cause_at=None, deadline_micros=None, idempotency_key=None, metadata=None, tool=None, usage=None, claim_check=None, signed_by=None))]
    #[allow(clippy::too_many_arguments)]
    fn fail<'py>(
        &self,
        py: Python<'py>,
        correlation: String,
        error: &Bound<'_, PyAny>,
        target: Option<String>,
        cause: Option<String>,
        cause_at: Option<PyLogPosition>,
        deadline_micros: Option<u64>,
        idempotency_key: Option<String>,
        metadata: Option<&Bound<'_, PyDict>>,
        tool: Option<String>,
        usage: Option<&Bound<'_, PyAny>>,
        claim_check: Option<(Py<PyAny>, usize)>,
        signed_by: Option<PyRef<'_, crate::sign::PySigningKey>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let agdx = self.inner.clone();
        let options = SendOptions::new(
            cause,
            cause_at,
            deadline_micros,
            idempotency_key,
            metadata,
            tool,
            usage,
            claim_check,
            signed_by,
        )?;
        let signing_key = self.signing_key.clone();
        let correlation = wire_correlation(&correlation)?;
        let error: AgentErrorBody = py_to_de(error)?;
        let target = target.map(|value| wire_agent(&value)).transpose()?;
        future_into_py(py, async move {
            let mut send = agdx.fail(correlation, &error).map_err(to_pyerr)?;
            if let Some(key) = signing_key.as_ref() {
                send = send.signed_by(key);
            }
            if let Some(target) = target {
                send = send.with_target(target);
            }
            send = options.apply(send);
            let record = send.send().await.map_err(to_pyerr)?;
            Ok(record.map(|id| id.to_string()))
        })
    }

    /// Open a chunk-stream writer under `correlation`. `purpose` is the
    /// chunk-stream vocabulary ('chat', 'reasoning', or 'tool_args').
    fn stream(&self, correlation: String, purpose: String) -> PyResult<PyAgdxStream> {
        let correlation = wire_correlation(&correlation)?;
        Ok(PyAgdxStream {
            inner: Arc::new(Mutex::new(Some(self.inner.stream(correlation, purpose)))),
        })
    }

    /// Human-in-the-loop interrupt/resume: publish a prompt `command` under a
    /// fresh correlation on this producer's topic, then await the human's
    /// correlated `response` on `reply_topic` up to `timeout_secs` and return
    /// its body bytes. A responder answers with `AgentCtx.respond_input`, or
    /// rejects with an error which raises here. Blocks the caller until the
    /// response lands or the timeout elapses, which is the point: the task is
    /// genuinely paused on a human.
    #[pyo3(signature = (reply_topic, prompt, *, timeout_secs=30.0))]
    fn request_input<'py>(
        &self,
        py: Python<'py>,
        reply_topic: String,
        prompt: &Bound<'_, PyAny>,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let agdx = self.inner.clone();
        let prompt = payload_bytes(prompt)?;
        future_into_py(py, async move {
            let body = agdx
                .request_input(
                    static_topic(reply_topic)?,
                    prompt,
                    duration_seconds(timeout_secs, "timeout_secs")?,
                )
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Ok(PyBytes::new(py, &body).into_any().unbind()))
        })
    }
}

/// A chunk-stream writer: `write` each chunk, then one terminal (`finish` or
/// `fail`). The opening chunk carries the purpose.
#[gen_stub_pyclass]
#[pyclass(name = "AgdxStream")]
pub struct PyAgdxStream {
    inner: Arc<Mutex<Option<AgdxStream>>>,
}

impl PyAgdxStream {
    // The builder verbs consume the Rust writer and hand it back, so they run
    // against the stored writer in place. They are synchronous and refuse while
    // a write or finish holds the writer.
    fn configure(&self, apply: impl FnOnce(AgdxStream) -> AgdxStream) -> PyResult<()> {
        let mut guard = self
            .inner
            .try_lock()
            .map_err(|_| InvalidError::new_err("the stream is busy with a write"))?;
        let stream = guard.take().ok_or_else(|| {
            to_pyerr(LaserError::HandlerConfig(
                "the stream is already finished".to_owned(),
            ))
        })?;
        *guard = Some(apply(stream));
        Ok(())
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAgdxStream {
    /// The stream's channel id.
    #[getter]
    fn channel(&self) -> PyResult<String> {
        let guard = self
            .inner
            .try_lock()
            .map_err(|_| InvalidError::new_err("the stream is busy with a write"))?;
        guard
            .as_ref()
            .map(|stream| stream.channel().to_string())
            .ok_or_else(|| {
                to_pyerr(LaserError::HandlerConfig(
                    "the stream is already finished".to_owned(),
                ))
            })
    }

    /// Declare the reader-local abandonment bound. It rides the opening chunk,
    /// so set it before the first write.
    fn with_deadline_micros(
        slf: PyRef<'_, Self>,
        deadline_micros: u64,
    ) -> PyResult<PyRef<'_, Self>> {
        slf.configure(|stream| stream.with_deadline_micros(deadline_micros))?;
        Ok(slf)
    }

    /// Narrow delivery to one agent within the shared topic.
    fn with_target(slf: PyRef<'_, Self>, target: String) -> PyResult<PyRef<'_, Self>> {
        let target = wire_agent(&target)?;
        slf.configure(|stream| stream.with_target(target))?;
        Ok(slf)
    }

    /// Declare the chunk bodies' codec (`agdx.ct`, for example 'json' or
    /// 'cbor'). Defaults to raw.
    fn content_type(slf: PyRef<'_, Self>, content_type: String) -> PyResult<PyRef<'_, Self>> {
        let content_type =
            parse_content_type(Some(content_type))?.expect("a given content type parses to one");
        slf.configure(|stream| stream.content_type(content_type))?;
        Ok(slf)
    }

    /// Buffer writes and append them as one batch when `max_chunks` fill or
    /// `linger_ms` has passed since the first buffered chunk, whichever comes
    /// first. The linger is checked at write, so a stalled producer holds its
    /// buffered chunks until the next `write`, an explicit `flush`, or the
    /// terminal, which always flushes.
    fn buffered(
        slf: PyRef<'_, Self>,
        max_chunks: usize,
        linger_ms: u64,
    ) -> PyResult<PyRef<'_, Self>> {
        slf.configure(|stream| {
            stream.buffered(max_chunks, std::time::Duration::from_millis(linger_ms))
        })?;
        Ok(slf)
    }

    /// Append everything buffered as one batch. A no-op when unbuffered or
    /// empty.
    fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let cell = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = cell.lock().await;
            let stream = guard.as_mut().ok_or_else(|| {
                to_pyerr(LaserError::HandlerConfig(
                    "the stream is already finished".to_owned(),
                ))
            })?;
            stream.flush().await.map_err(to_pyerr)
        })
    }
    /// Publish the next chunk (str, bytes, or bytearray).
    fn write<'py>(&self, py: Python<'py>, body: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let cell = self.inner.clone();
        let body = payload_bytes(body)?;
        future_into_py(py, async move {
            let mut guard = cell.lock().await;
            let stream = guard.as_mut().ok_or_else(|| {
                to_pyerr(LaserError::HandlerConfig(
                    "the stream is already finished".to_owned(),
                ))
            })?;
            stream.write(body).await.map_err(to_pyerr)
        })
    }

    /// Publish the terminal chunk with the reason the stream ended (default
    /// 'stop') and the token `usage` dict the stream consumed, when given.
    #[pyo3(signature = (*, finish_reason="stop".to_owned(), usage=None))]
    fn finish<'py>(
        &self,
        py: Python<'py>,
        finish_reason: String,
        usage: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let cell = self.inner.clone();
        let usage: Option<TokenUsage> = usage.map(py_to_de).transpose()?;
        future_into_py(py, async move {
            let stream = cell.lock().await.take().ok_or_else(|| {
                to_pyerr(LaserError::HandlerConfig(
                    "the stream is already finished".to_owned(),
                ))
            })?;
            stream.finish(finish_reason, usage).await.map_err(to_pyerr)
        })
    }

    /// Publish a structured `error` terminal for the stream.
    fn fail<'py>(&self, py: Python<'py>, error: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let cell = self.inner.clone();
        let error: AgentErrorBody = py_to_de(error)?;
        future_into_py(py, async move {
            let stream = cell.lock().await.take().ok_or_else(|| {
                to_pyerr(LaserError::HandlerConfig(
                    "the stream is already finished".to_owned(),
                ))
            })?;
            stream.fail(&error).await.map_err(to_pyerr)
        })
    }
}

/// A message's place on the log as a four-level Iggy locator: stream, topic,
/// partition, and offset. Rides an envelope's `cause_at` as 20 packed
/// big-endian bytes.
#[gen_stub_pyclass]
#[pyclass(name = "LogPosition", frozen, eq, hash, from_py_object)]
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PyLogPosition {
    pub(crate) inner: LogPosition,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLogPosition {
    /// A locator at `(stream_id, topic_id, partition_id, offset)`.
    #[new]
    fn new(stream_id: u32, topic_id: u32, partition_id: u32, offset: u64) -> Self {
        Self {
            inner: LogPosition::new(stream_id, topic_id, partition_id, offset),
        }
    }

    #[getter]
    fn stream_id(&self) -> u32 {
        self.inner.stream_id
    }

    #[getter]
    fn topic_id(&self) -> u32 {
        self.inner.topic_id
    }

    #[getter]
    fn partition_id(&self) -> u32 {
        self.inner.partition_id
    }

    #[getter]
    fn offset(&self) -> u64 {
        self.inner.offset
    }

    /// The locator packed as 20 big-endian bytes (the opaque wire form).
    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.to_bytes())
    }

    /// Unpack a locator from its 20 big-endian bytes.
    #[staticmethod]
    fn from_bytes(payload: Vec<u8>) -> PyResult<Self> {
        let payload = payload.as_slice().try_into().map_err(|_| {
            InvalidError::new_err(format!("a log position is 20 bytes, got {}", payload.len()))
        })?;
        Ok(Self {
            inner: LogPosition::from_bytes(payload),
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "LogPosition(stream_id={}, topic_id={}, partition_id={}, offset={})",
            self.inner.stream_id, self.inner.topic_id, self.inner.partition_id, self.inner.offset
        )
    }
}

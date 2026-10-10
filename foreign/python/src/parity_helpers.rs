use crate::async_bridge::future_into_py;
use crate::blob::PyBlobStore;
use crate::client::PyLaser;
use crate::context::read_topics;
use crate::convert::{json_to_py, payload_bytes, py_to_de, py_to_json, ser_to_py};
use crate::errors::{CodecError, InvalidError, to_pyerr};
use crate::memory::PyMemoryItem;
use crate::session::PyCheckpoint;
use crate::sign::{PyKeyRegistry, PySigningKey, envelope_of};
use crate::snapshot::{snapshot_from_py, snapshot_to_py};
use laser_sdk::agent::{Clock, SystemClock, TestClock};
use laser_sdk::sign::{KeyKind, KeyRecord};
use laser_sdk::wire::agent::{
    AgentId, ContextCompaction, ContextManifest, ContextRetrieval, ConversationId, CorrelationId,
    RecordId, StateDelta, StateSnapshot,
};
use laser_sdk::wire::framing::{decode_named, encode_named};
use laser_sdk::wire::session::SessionReply;
use laser_sdk::wire::session::request as session_request;
use laser_sdk::wire::validate::Validate;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};

/// A source of the current time in epoch microseconds. Subclass it and
/// override `now_micros` for a custom clock.
#[gen_stub_pyclass]
#[pyclass(name = "Clock", subclass, frozen)]
pub struct PyClock;

#[gen_stub_pymethods]
#[pymethods]
impl PyClock {
    #[new]
    fn new() -> Self {
        Self
    }

    /// The current time, epoch microseconds.
    fn now_micros(&self) -> PyResult<u64> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "a Clock subclass must override now_micros",
        ))
    }
}

/// The current time in epoch microseconds from the native Rust clock.
#[gen_stub_pyclass]
#[pyclass(name = "SystemClock", extends = PyClock, frozen)]
pub struct PySystemClock {
    inner: SystemClock,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySystemClock {
    #[new]
    fn new() -> PyClassInitializer<PySystemClock> {
        PyClassInitializer::from(PyClock).add_subclass(PySystemClock { inner: SystemClock })
    }

    fn now_micros(&self) -> u64 {
        self.inner.now_micros()
    }
}

/// A shared test clock with native unsigned 64-bit time and wrapping advance.
#[gen_stub_pyclass]
#[pyclass(name = "TestClock", extends = PyClock, frozen)]
pub struct PyTestClock {
    inner: TestClock,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTestClock {
    #[new]
    #[pyo3(signature = (start_micros=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.int", imports = ("builtins",)))]
        start_micros: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyClassInitializer<PyTestClock>> {
        let start = start_micros.map(clock_micros).transpose()?.unwrap_or(0);
        Ok(PyClassInitializer::from(PyClock).add_subclass(PyTestClock {
            inner: TestClock::new(start),
        }))
    }

    fn now_micros(&self) -> u64 {
        self.inner.now_micros()
    }

    /// Move the clock forward by `by_micros`, wrapping at the unsigned 64-bit
    /// ceiling like Rust and TypeScript.
    fn advance(
        &self,
        #[gen_stub(override_type(type_repr = "builtins.int", imports = ("builtins",)))]
        by_micros: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.inner.advance(clock_micros(by_micros)?);
        Ok(())
    }

    /// Set the absolute time.
    fn set(
        &self,
        #[gen_stub(override_type(type_repr = "builtins.int", imports = ("builtins",)))]
        now_micros: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.inner.set(clock_micros(now_micros)?);
        Ok(())
    }
}

// A clock argument must fit an unsigned 64-bit microsecond count. Anything
// else is `InvalidError`, like TypeScript, rather than PyO3's `OverflowError`.
fn clock_micros(value: &Bound<'_, PyAny>) -> PyResult<u64> {
    value
        .extract::<u64>()
        .map_err(|_| crate::errors::InvalidError::new_err("clock microseconds must fit u64"))
}

/// Sign an A2A card value with the native detached JWS format.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn sign_card_value(
    py: Python<'_>,
    key: PyRef<'_, PySigningKey>,
    card: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let signature =
        laser_sdk::sign::sign_card_value(&key.inner, &py_to_json(card)?).map_err(to_pyerr)?;
    ser_to_py(py, &signature)
}

/// Verify a detached A2A card signature against a 32-byte public key.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn verify_card(
    card: &Bound<'_, PyAny>,
    signature: &Bound<'_, PyAny>,
    verifying: Vec<u8>,
) -> PyResult<()> {
    let key =
        KeyRecord::from_verifying_bytes("card", &verifying, KeyKind::Agent).map_err(to_pyerr)?;
    laser_sdk::sign::verify_card(&py_to_json(card)?, &py_to_de(signature)?, &key.verifying)
        .map_err(to_pyerr)
}

/// Verify the signer and return its signed delegated user, or None.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn verify_delegation(
    registry: PyRef<'_, PyKeyRegistry>,
    envelope: &Bound<'_, PyAny>,
) -> PyResult<Option<(String, String)>> {
    laser_sdk::sign::verify_delegation(&registry.snapshot(), &envelope_of(envelope)?)
        .map_err(to_pyerr)
}

/// Encode a snapshot dict as the native named-field CBOR storage bytes.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_snapshot(py: Python<'_>, snapshot: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    let payload = laser_sdk::snapshot::encode(&snapshot_from_py(snapshot)?).map_err(to_pyerr)?;
    Ok(PyBytes::new(py, &payload).unbind())
}

/// Decode snapshot storage bytes to a dict with conversation, as_of, and state.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_snapshot(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let snapshot = laser_sdk::snapshot::decode(&payload_bytes(payload)?).map_err(to_pyerr)?;
    Ok(snapshot_to_py(py, &snapshot)?.unbind())
}

fn encode_wire<T>(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>>
where
    T: serde::Serialize + serde::de::DeserializeOwned + Validate,
{
    let value: T = py_to_de(value)?;
    value
        .validate()
        .map_err(|error| InvalidError::new_err(error.to_string()))?;
    let payload = encode_named(&value).map_err(|error| CodecError::new_err(error.to_string()))?;
    Ok(PyBytes::new(py, &payload).unbind())
}

fn decode_wire<T: serde::de::DeserializeOwned + serde::Serialize + Validate>(
    py: Python<'_>,
    payload: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let value: T = decode_named(&payload_bytes(payload)?)
        .map_err(|error| CodecError::new_err(error.to_string()))?;
    value
        .validate()
        .map_err(|error| InvalidError::new_err(error.to_string()))?;
    ser_to_py(py, &value)
}

fn encode_simple_wire<T: serde::de::DeserializeOwned + serde::Serialize>(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Py<PyBytes>> {
    let value: T = py_to_de(value)?;
    let payload = encode_named(&value).map_err(|error| CodecError::new_err(error.to_string()))?;
    Ok(PyBytes::new(py, &payload).unbind())
}

fn decode_simple_wire<T: serde::de::DeserializeOwned + serde::Serialize>(
    py: Python<'_>,
    payload: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let value: T = decode_named(&payload_bytes(payload)?)
        .map_err(|error| CodecError::new_err(error.to_string()))?;
    ser_to_py(py, &value)
}

/// Encode a stream-scoped session get request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_session_get(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_simple_wire::<session_request::SessionGet>(py, value)
}

/// Decode a stream-scoped session get request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_session_get(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_simple_wire::<session_request::SessionGet>(py, payload)
}

/// Encode a stream-scoped session list request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_session_list(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_simple_wire::<session_request::SessionList>(py, value)
}

/// Decode a stream-scoped session list request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_session_list(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_simple_wire::<session_request::SessionList>(py, payload)
}

/// Encode a stream-scoped session events request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_session_events(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_simple_wire::<session_request::SessionEvents>(py, value)
}

/// Decode a stream-scoped session events request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_session_events(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_simple_wire::<session_request::SessionEvents>(py, payload)
}

/// Encode a stream-scoped session state request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_session_state(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_simple_wire::<session_request::SessionState>(py, value)
}

/// Decode a stream-scoped session state request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_session_state(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_simple_wire::<session_request::SessionState>(py, payload)
}

/// Encode a stream-scoped session links request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_session_links(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_simple_wire::<session_request::SessionLinks>(py, value)
}

/// Decode a stream-scoped session links request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_session_links(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_simple_wire::<session_request::SessionLinks>(py, payload)
}

/// Encode a stream-scoped session sources request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_session_sources(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_simple_wire::<session_request::SessionSources>(py, value)
}

/// Decode a stream-scoped session sources request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_session_sources(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_simple_wire::<session_request::SessionSources>(py, payload)
}

/// Encode a stream-scoped session changes request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_session_changes(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_simple_wire::<session_request::SessionChanges>(py, value)
}

/// Decode a stream-scoped session changes request.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_session_changes(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_simple_wire::<session_request::SessionChanges>(py, payload)
}

/// Encode a session read reply.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_session_reply(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_simple_wire::<SessionReply>(py, value)
}

/// Decode a session read reply.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_session_reply(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_simple_wire::<SessionReply>(py, payload)
}

/// Encode a session start body.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_session_start(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_simple_wire::<laser_sdk::wire::agent::SessionStart>(py, value)
}

/// Decode a session start body.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_session_start(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_simple_wire::<laser_sdk::wire::agent::SessionStart>(py, payload)
}

/// Encode a session transition body.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_session_transition(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Py<PyBytes>> {
    encode_simple_wire::<laser_sdk::wire::agent::SessionTransition>(py, value)
}

/// Decode a session transition body.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_session_transition(
    py: Python<'_>,
    payload: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    decode_simple_wire::<laser_sdk::wire::agent::SessionTransition>(py, payload)
}

/// Encode a session end body.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_session_end(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_simple_wire::<laser_sdk::wire::agent::SessionEnd>(py, value)
}

/// Decode a session end body.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_session_end(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_simple_wire::<laser_sdk::wire::agent::SessionEnd>(py, payload)
}

/// Encode a context manifest with the shared fragment limit.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_context_manifest(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_wire::<ContextManifest>(py, value)
}

/// Decode a context manifest and check its fragment limit.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_context_manifest(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_wire::<ContextManifest>(py, payload)
}

/// Encode a context compaction record.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_context_compaction(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<Py<PyBytes>> {
    let value: ContextCompaction = py_to_de(value)?;
    let payload = encode_named(&value).map_err(|error| CodecError::new_err(error.to_string()))?;
    Ok(PyBytes::new(py, &payload).unbind())
}

/// Decode a context compaction record.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_context_compaction(
    py: Python<'_>,
    payload: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let value: ContextCompaction = decode_named(&payload_bytes(payload)?)
        .map_err(|error| CodecError::new_err(error.to_string()))?;
    ser_to_py(py, &value)
}

/// Encode a context retrieval record.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_context_retrieval(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    let value: ContextRetrieval = py_to_de(value)?;
    let payload = encode_named(&value).map_err(|error| CodecError::new_err(error.to_string()))?;
    Ok(PyBytes::new(py, &payload).unbind())
}

/// Decode a context retrieval record.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_context_retrieval(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    let value: ContextRetrieval = decode_named(&payload_bytes(payload)?)
        .map_err(|error| CodecError::new_err(error.to_string()))?;
    ser_to_py(py, &value)
}

/// Encode a revision-guarded state patch with the shared limits.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_state_delta(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_wire::<StateDelta>(py, value)
}

/// Decode a revision-guarded state patch and check its limits.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_state_delta(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_wire::<StateDelta>(py, payload)
}

/// Encode a complete state document with the shared byte limit.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encode_state_snapshot(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<Py<PyBytes>> {
    encode_wire::<StateSnapshot>(py, value)
}

/// Decode a complete state document and check its byte limit.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decode_state_snapshot(py: Python<'_>, payload: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
    decode_wire::<StateSnapshot>(py, payload)
}

/// Apply an RFC 6902 patch to a copy of a JSON document.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn apply_json_patch(
    py: Python<'_>,
    document: &Bound<'_, PyAny>,
    patch: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let document = py_to_json(document)?;
    let patch: Vec<_> = py_to_de(patch)?;
    let result = laser_sdk::wire::agent::apply_json_patch(&document, &patch)
        .map_err(|error| InvalidError::new_err(error.to_string()))?;
    json_to_py(py, &result)
}

/// Return one past each last folded offset, saturating at the native u64 ceiling.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn resume_offsets(snapshot: &Bound<'_, PyAny>) -> PyResult<Vec<(u32, u64, u32, u64)>> {
    Ok(
        laser_sdk::agent::resume_offsets(&snapshot_from_py(snapshot)?)
            .into_iter()
            .map(|entry| {
                (
                    entry.topic_id,
                    entry.topic_created_at_micros,
                    entry.partition_id,
                    entry.offset,
                )
            })
            .collect(),
    )
}

#[gen_stub_pyfunction]
#[pyfunction]
pub fn checkpoint_from_snapshot<'py>(
    py: Python<'py>,
    laser: &PyLaser,
    snapshot: &Bound<'_, PyAny>,
    topics: Vec<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let laser = laser.inner.clone();
    let snapshot = snapshot_from_py(snapshot)?;
    let topics = read_topics(Some(topics))?;
    future_into_py(py, async move {
        let checkpoint = laser_sdk::agent::checkpoint_from_snapshot(&laser, &snapshot, &topics)
            .await
            .map_err(to_pyerr)?;
        Ok(PyCheckpoint::new(checkpoint))
    })
}

/// A snapshot of `state` folded up to `checkpoint` for `conversation` under
/// the fold named `fold`, recording the stream and every checkpointed topic
/// by id and creation time. Returns the snapshot dict `decode_snapshot` returns.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn snapshot_from_checkpoint<'py>(
    py: Python<'py>,
    laser: &PyLaser,
    conversation: &str,
    fold: String,
    checkpoint: &PyCheckpoint,
    state: &Bound<'_, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let laser = laser.inner.clone();
    let conversation =
        <laser_sdk::types::ConversationId as std::str::FromStr>::from_str(conversation)
            .map_err(|error| to_pyerr(error.into()))?;
    let checkpoint = checkpoint.inner().clone();
    let state = payload_bytes(state)?;
    future_into_py(py, async move {
        let snapshot = laser_sdk::agent::snapshot_from_checkpoint(
            &laser,
            conversation,
            &fold,
            &checkpoint,
            state,
        )
        .await
        .map_err(to_pyerr)?;
        Python::attach(|py| Ok(snapshot_to_py(py, &snapshot)?.unbind()))
    })
}

/// Fuse ranked signals by native reciprocal rank, preserving each item's attribution.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn fuse_reciprocal_rank(
    signals: Vec<Vec<PyRef<'_, PyMemoryItem>>>,
    limit: usize,
) -> Vec<PyMemoryItem> {
    let signals = signals
        .into_iter()
        .map(|ranked| ranked.into_iter().map(|item| item.inner.clone()).collect())
        .collect();
    laser_sdk::memory::fuse_reciprocal_rank(signals, limit)
        .into_iter()
        .map(PyMemoryItem::from)
        .collect()
}

fn record_id(value: &str) -> PyResult<RecordId> {
    value
        .parse::<RecordId>()
        .map_err(|error| crate::errors::InvalidError::new_err(error.to_string()))
}

fn conversation_id(value: &str) -> PyResult<ConversationId> {
    value
        .parse::<ConversationId>()
        .map_err(|error| crate::errors::InvalidError::new_err(error.to_string()))
}

fn agent_id(value: &str) -> PyResult<AgentId> {
    value
        .parse::<AgentId>()
        .map_err(|error| crate::errors::InvalidError::new_err(error.to_string()))
}

fn correlation_id(value: &str) -> PyResult<CorrelationId> {
    value
        .parse::<CorrelationId>()
        .map_err(|error| crate::errors::InvalidError::new_err(error.to_string()))
}

/// Construct an A2A command and keep the original params bytes unchanged.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn command_from_message_send(
    py: Python<'_>,
    record: String,
    conversation: String,
    source: String,
    correlation: String,
    params_json: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let envelope = laser_sdk::a2a::command_from_message_send(
        record_id(&record)?,
        conversation_id(&conversation)?,
        agent_id(&source)?,
        correlation_id(&correlation)?,
        payload_bytes(params_json)?,
    );
    ser_to_py(py, &envelope)
}

/// Render an AGDX envelope as the native A2A task view.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn task_from_envelope(
    py: Python<'_>,
    task_id: String,
    envelope: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    ser_to_py(
        py,
        &laser_sdk::a2a::task_from_envelope(task_id, &envelope_of(envelope)?),
    )
}

/// Construct an MCP tool command and keep the original params bytes unchanged.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn tool_call_from_request(
    py: Python<'_>,
    record: String,
    conversation: String,
    source: String,
    correlation: String,
    tool_name: String,
    params_json: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let envelope = laser_sdk::mcp::tool_call_from_request(
        record_id(&record)?,
        conversation_id(&conversation)?,
        agent_id(&source)?,
        correlation_id(&correlation)?,
        tool_name,
        payload_bytes(params_json)?,
    );
    ser_to_py(py, &envelope)
}

/// Render an AGDX envelope as the native MCP content and error view.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn tool_result_from_envelope(
    py: Python<'_>,
    envelope: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    ser_to_py(
        py,
        &laser_sdk::mcp::tool_result_from_envelope(&envelope_of(envelope)?),
    )
}

/// Claim-check payloads at or above the threshold through an async blob store.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn check_in<'py>(
    py: Python<'py>,
    store: Py<PyAny>,
    threshold_bytes: usize,
    payload: &Bound<'_, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let payload = payload_bytes(payload)?;
    future_into_py(py, async move {
        let store = PyBlobStore { hooks: store };
        let (payload, content_type) = laser_sdk::blob::check_in(&store, threshold_bytes, payload)
            .await
            .map_err(to_pyerr)?;
        Python::attach(|py| {
            Ok((
                PyBytes::new(py, &payload).unbind(),
                content_type.map(|value| value.to_string()),
            ))
        })
    })
}

/// Resolve a body-reference capsule and verify its native size and digest checks.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn resolve_body<'py>(
    py: Python<'py>,
    store: Py<PyAny>,
    payload: &Bound<'_, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let payload = payload_bytes(payload)?;
    future_into_py(py, async move {
        let store = PyBlobStore { hooks: store };
        let payload = laser_sdk::blob::resolve_body(&store, &payload)
            .await
            .map_err(to_pyerr)?;
        Python::attach(|py| Ok(PyBytes::new(py, &payload).unbind()))
    })
}

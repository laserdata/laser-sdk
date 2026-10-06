use crate::async_bridge::future_into_py;
use crate::blob::PyBlobStore;
use crate::convert::{payload_bytes, py_to_de, py_to_json, ser_to_py};
use crate::errors::to_pyerr;
use crate::memory::PyMemoryItem;
use crate::sign::{PyKeyRegistry, PySigningKey, envelope_of};
use crate::snapshot::{snapshot_from_py, snapshot_to_py};
use laser_sdk::agent::{Clock, SystemClock, TestClock};
use laser_sdk::sign::{KeyKind, KeyRecord};
use laser_sdk::wire::agent::{AgentId, ConversationId, CorrelationId, RecordId};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::collections::BTreeMap;

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
    #[pyo3(signature = (start_micros=0))]
    fn new(start_micros: u64) -> PyClassInitializer<PyTestClock> {
        PyClassInitializer::from(PyClock).add_subclass(PyTestClock {
            inner: TestClock::new(start_micros),
        })
    }

    fn now_micros(&self) -> u64 {
        self.inner.now_micros()
    }

    fn advance(&self, by_micros: u64) {
        self.inner.advance(by_micros);
    }

    fn set(&self, now_micros: u64) {
        self.inner.set(now_micros);
    }
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

/// Return one past each last folded offset, saturating at the native u64 ceiling.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn resume_offsets(snapshot: &Bound<'_, PyAny>) -> PyResult<BTreeMap<u32, u64>> {
    Ok(laser_sdk::agent::resume_offsets(&snapshot_from_py(
        snapshot,
    )?))
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

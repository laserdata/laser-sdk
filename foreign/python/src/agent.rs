use crate::client::PyLaser;
use crate::convert::{json_to_py, payload_bytes, py_to_de, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use iggy::prelude::{Identifier, IggyTimestamp};
use laser_sdk::agent::AgentMessage;
use laser_sdk::provenance::{AgentTopic, LlmUsage, Provenance};
use laser_sdk::types::{AgentId, ConversationId, MessageId};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{future_into_py, get_current_locals, scope};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;
use std::time::Duration;

/// Build an owned `AgentTopic` from a topic name. Every topic (well-known or
/// custom) routes by its string name, so `Custom` over the name is fully general.
fn topic_from_name(name: &str) -> PyResult<Identifier> {
    Identifier::named(name).map_err(|e| InvalidError::new_err(e.to_string()))
}

fn parse_conversation_id(value: Option<String>) -> PyResult<ConversationId> {
    match value {
        Some(value) => ConversationId::from_str(&value).map_err(|e| to_pyerr(e.into())),
        None => Ok(ConversationId::new()),
    }
}

// One argument per optional provenance field, mirroring the Python constructor's
// keyword arguments one-to-one.
#[allow(clippy::too_many_arguments)]
fn build_provenance(
    conversation_id: Option<String>,
    causal_parent: Option<String>,
    parent_conversation_id: Option<String>,
    root_conversation_id: Option<String>,
    agent: Option<String>,
    target_agent_id: Option<String>,
    idempotency_key: Option<String>,
    correlation_id: Option<String>,
    deadline_micros: Option<u64>,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cost_usd: Option<f64>,
) -> PyResult<Provenance> {
    let conversation_id = parse_conversation_id(conversation_id)?;
    let causal_parent = match causal_parent {
        Some(value) => Some(MessageId::from_str(&value).map_err(|e| to_pyerr(e.into()))?),
        None => None,
    };
    let parent_conversation_id = match parent_conversation_id {
        Some(value) => Some(ConversationId::from_str(&value).map_err(|e| to_pyerr(e.into()))?),
        None => None,
    };
    let root_conversation_id = match root_conversation_id {
        Some(value) => Some(ConversationId::from_str(&value).map_err(|e| to_pyerr(e.into()))?),
        None => None,
    };
    let agent = match agent {
        Some(value) => Some(AgentId::new(value).map_err(|e| to_pyerr(e.into()))?),
        None => None,
    };
    let target_agent_id = match target_agent_id {
        Some(value) => Some(AgentId::new(value).map_err(|e| to_pyerr(e.into()))?),
        None => None,
    };
    let usage = if input_tokens.is_some() || output_tokens.is_some() || cost_usd.is_some() {
        Some(
            LlmUsage::builder()
                .maybe_input_tokens(input_tokens)
                .maybe_output_tokens(output_tokens)
                .maybe_cost_usd(cost_usd)
                .build(),
        )
    } else {
        None
    };
    Ok(Provenance::builder()
        .conversation_id(conversation_id)
        .maybe_causal_parent(causal_parent)
        .maybe_parent_conversation_id(parent_conversation_id)
        .maybe_root_conversation_id(root_conversation_id)
        .maybe_agent(agent)
        .maybe_target_agent_id(target_agent_id)
        .maybe_idempotency_key(idempotency_key)
        .maybe_correlation_id(correlation_id)
        .maybe_deadline(deadline_micros.map(IggyTimestamp::from))
        .maybe_usage(usage)
        .build())
}

/// The agentic message spine: conversation, causality, routing, usage. Stamped on
/// every agent message as headers.
#[gen_stub_pyclass]
#[pyclass(name = "Provenance", skip_from_py_object)]
#[derive(Clone)]
pub struct PyProvenance {
    pub(crate) inner: Provenance,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyProvenance {
    #[new]
    #[pyo3(signature = (
        *,
        conversation_id=None,
        causal_parent=None,
        parent_conversation_id=None,
        root_conversation_id=None,
        agent=None,
        target_agent_id=None,
        idempotency_key=None,
        correlation_id=None,
        deadline_micros=None,
        input_tokens=None,
        output_tokens=None,
        cost_usd=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        conversation_id: Option<String>,
        causal_parent: Option<String>,
        parent_conversation_id: Option<String>,
        root_conversation_id: Option<String>,
        agent: Option<String>,
        target_agent_id: Option<String>,
        idempotency_key: Option<String>,
        correlation_id: Option<String>,
        deadline_micros: Option<u64>,
        input_tokens: Option<u64>,
        output_tokens: Option<u64>,
        cost_usd: Option<f64>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: build_provenance(
                conversation_id,
                causal_parent,
                parent_conversation_id,
                root_conversation_id,
                agent,
                target_agent_id,
                idempotency_key,
                correlation_id,
                deadline_micros,
                input_tokens,
                output_tokens,
                cost_usd,
            )?,
        })
    }

    #[getter]
    fn conversation_id(&self) -> String {
        self.inner.conversation_id.to_string()
    }

    #[getter]
    fn parent_conversation_id(&self) -> Option<String> {
        self.inner.parent_conversation_id.map(|id| id.to_string())
    }

    #[getter]
    fn root_conversation_id(&self) -> Option<String> {
        self.inner.root_conversation_id.map(|id| id.to_string())
    }

    #[getter]
    fn causal_parent(&self) -> Option<String> {
        self.inner.causal_parent.map(|id| id.to_string())
    }

    #[getter]
    fn agent(&self) -> Option<String> {
        self.inner.agent.as_ref().map(|a| a.as_str().to_owned())
    }

    #[getter]
    fn target_agent_id(&self) -> Option<String> {
        self.inner
            .target_agent_id
            .as_ref()
            .map(|a| a.as_str().to_owned())
    }

    #[getter]
    fn idempotency_key(&self) -> Option<String> {
        self.inner.idempotency_key.clone()
    }

    #[getter]
    fn correlation_id(&self) -> Option<String> {
        self.inner.correlation_id.clone()
    }

    #[getter]
    fn input_tokens(&self) -> Option<u64> {
        self.inner.usage.as_ref().and_then(|u| u.input_tokens)
    }

    #[getter]
    fn output_tokens(&self) -> Option<u64> {
        self.inner.usage.as_ref().and_then(|u| u.output_tokens)
    }

    #[getter]
    fn cost_usd(&self) -> Option<f64> {
        self.inner.usage.as_ref().and_then(|u| u.cost_usd)
    }

    fn __repr__(&self) -> String {
        format!(
            "Provenance(conversation_id={}, agent={:?}, idempotency_key={:?})",
            self.inner.conversation_id,
            self.inner.agent.as_ref().map(|a| a.as_str()),
            self.inner.idempotency_key,
        )
    }
}

/// A message delivered to / received by an agent: decoded provenance, raw
/// payload, log position, and the AGDX envelope when present.
#[gen_stub_pyclass]
#[pyclass(name = "AgentMessage", frozen)]
pub struct PyAgentMessage {
    pub(crate) inner: AgentMessage,
}

impl PyAgentMessage {
    pub(crate) fn from_inner(inner: AgentMessage) -> Self {
        Self { inner }
    }

    // The decoded AGDX envelope, for the chunk reassembler to feed the stream
    // state machine. `None` for a plain (non-AGDX) message.
    pub(crate) fn agdx_envelope(&self) -> Option<&laser_sdk::wire::agent::AgentEnvelope> {
        self.inner.envelope.as_ref()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAgentMessage {
    #[getter]
    fn payload(&self) -> Vec<u8> {
        self.inner.payload.clone()
    }

    /// The enrolled principal that signed this verified contract reply.
    #[getter]
    fn verified_principal(&self) -> Option<String> {
        self.inner.verified_principal.clone()
    }

    /// The task body regardless of message shape: the AGDX envelope body for a
    /// command or response (its payload is the encoded envelope), else the raw
    /// payload. A handler reached by a contract or workflow reads this.
    fn body(&self) -> Vec<u8> {
        self.inner.body().to_vec()
    }

    /// The real body of a claim-checked message: when the content type is `ref`,
    /// decode the `BodyRef` capsule, fetch the referenced bytes from `store`, and
    /// verify their SHA-256 against the capsule digest before returning them (a
    /// mismatch raises, never unverified bytes). Any other content type returns
    /// the payload as-is. `store` is a Python object with `async def get(reference:
    /// str) -> bytes` (and, for the publish side, `async def put(data: bytes) ->
    /// str`). The consume-side pairing of the publish builder's claim-check.
    fn resolve_body<'py>(&self, py: Python<'py>, store: Py<PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let message = self.inner.clone();
        let locals = get_current_locals(py)?;
        future_into_py(
            py,
            scope(locals, async move {
                let store = crate::blob::PyBlobStore { hooks: store };
                message
                    .resolve_body(&store)
                    .await
                    .map(|bytes| bytes.to_vec())
                    .map_err(to_pyerr)
            }),
        )
    }

    #[getter]
    fn message_id(&self) -> String {
        self.inner.id.to_string()
    }

    #[getter]
    fn provenance(&self) -> PyProvenance {
        PyProvenance {
            inner: self.inner.provenance.clone(),
        }
    }

    #[getter]
    fn conversation_id(&self) -> String {
        self.inner.provenance.conversation_id.to_string()
    }

    #[getter]
    fn agent(&self) -> Option<String> {
        self.inner
            .provenance
            .agent
            .as_ref()
            .map(|a| a.as_str().to_owned())
    }

    #[getter]
    fn idempotency_key(&self) -> Option<String> {
        self.inner.provenance.idempotency_key.clone()
    }

    #[getter]
    fn correlation_id(&self) -> Option<String> {
        self.inner.provenance.correlation_id.clone()
    }

    /// The decoded AGDX envelope as a dict, or `None` for a plain message.
    #[getter]
    fn envelope(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.inner.envelope {
            Some(envelope) => ser_to_py(py, envelope),
            None => Ok(py.None()),
        }
    }

    /// The decoded AGDX envelope's body bytes, or `None` for a plain message.
    /// Unlike `payload` (the raw CBOR envelope for an AGDX message), this is the
    /// inner body the producer sent.
    #[getter]
    fn agdx_body(&self) -> Option<Vec<u8>> {
        self.inner
            .envelope
            .as_ref()
            .map(|envelope| envelope.body.clone())
    }

    /// Decode the payload as JSON into a Python value.
    fn json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value: serde_json::Value = serde_json::from_slice(&self.inner.payload)
            .map_err(|e| crate::errors::CodecError::new_err(e.to_string()))?;
        json_to_py(py, &value)
    }

    fn __repr__(&self) -> String {
        format!(
            "AgentMessage(id={}, conversation_id={}, bytes={})",
            self.inner.id,
            self.inner.provenance.conversation_id,
            self.inner.payload.len()
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Create the default data stream and the well-known agent topics, `partitions`
    /// each. Idempotent. Requires a default stream.
    fn bootstrap<'py>(&self, py: Python<'py>, partitions: u32) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            laser.bootstrap(partitions).await.map_err(to_pyerr)
        })
    }

    /// Append `payload` to an agent `topic`, stamping `provenance` as headers and
    /// keying the partition by conversation.
    fn send_agent<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        payload: &Bound<'_, PyAny>,
        provenance: &PyProvenance,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let payload = payload_bytes(payload)?;
        let provenance = provenance.inner.clone();
        future_into_py(py, async move {
            let id = topic_from_name(&topic)?;
            let agent_topic = AgentTopic::Custom(&id);
            laser
                .send_agent(agent_topic, payload, &provenance)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Send a request and await its correlated reply on `reply_topic`, up to
    /// `timeout_secs`. Correlation is a fresh correlation id the responder echoes
    /// (the business idempotency key is left untouched).
    #[pyo3(signature = (request_topic, reply_topic, payload, provenance, *, timeout_secs=30.0))]
    fn request<'py>(
        &self,
        py: Python<'py>,
        request_topic: String,
        reply_topic: String,
        payload: &Bound<'_, PyAny>,
        provenance: &PyProvenance,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let payload = payload_bytes(payload)?;
        let provenance = provenance.inner.clone();
        future_into_py(py, async move {
            let request_id = topic_from_name(&request_topic)?;
            let reply_id = topic_from_name(&reply_topic)?;
            let reply = laser
                .request(
                    AgentTopic::Custom(&request_id),
                    AgentTopic::Custom(&reply_id),
                    payload,
                    &provenance,
                    Duration::from_secs_f64(timeout_secs),
                )
                .await
                .map_err(to_pyerr)?;
            Ok(PyAgentMessage::from_inner(reply))
        })
    }

    /// A fresh child conversation of `parent`, carrying parent / root ids.
    fn spawn_subconversation(&self, parent: &PyProvenance) -> PyProvenance {
        PyProvenance {
            inner: self.inner.spawn_subconversation(&parent.inner),
        }
    }

    /// Redrive a dead-lettered message from a capsule dict (an AGDX
    /// dead-letter): re-read the original record and republish it verbatim so
    /// a fixed handler reprocesses it. The dict mirrors `AgentDeadLetter`
    /// field for field: `source` is the 20 big-endian packed locator bytes
    /// (`stream_id: u32`, `topic_id: u32`, `partition_id: u32`, `offset: u64`,
    /// e.g. `struct.pack(">IIIQ", stream_id, topic_id, partition_id, offset)`
    /// in Python), `reason` is the dead-letter reason code (`1` retry
    /// exhausted, `2` rejected, `3` decode failed, `4` deadline exceeded),
    /// `attempts` an int, `detail` an optional string, and `payload` the
    /// poison message's raw bytes.
    fn redrive_dead_letter<'py>(
        &self,
        py: Python<'py>,
        capsule: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let capsule = py_to_de(capsule)?;
        future_into_py(py, async move {
            laser.redrive_dead_letter(&capsule).await.map_err(to_pyerr)
        })
    }
}

/// The well-known agent topic names, so callers reference `Topics.COMMANDS`
/// instead of retyping the string. Any other name works too: the agent methods
/// take a plain topic string.
#[gen_stub_pyclass]
#[pyclass(name = "Topics", frozen)]
pub struct PyTopics;

#[gen_stub_pymethods]
#[pymethods]
impl PyTopics {
    #[classattr]
    const COMMANDS: &'static str = "agent.commands";
    #[classattr]
    const RESPONSES: &'static str = "agent.responses";
    #[classattr]
    const TOOL_CALLS: &'static str = "agent.tool_calls";
    #[classattr]
    const TOOL_RESULTS: &'static str = "agent.tool_results";
    #[classattr]
    const LLM_IO: &'static str = "agent.llm_io";
    #[classattr]
    const HUMAN_INPUT: &'static str = "agent.human_input";
    #[classattr]
    const AUDIT: &'static str = "agent.audit";
    #[classattr]
    const DLQ: &'static str = "agent.dlq";
}

// A fresh, random conversation id (a time-ordered ULID).
#[gen_stub_pyfunction]
#[pyfunction]
pub fn new_conversation_id() -> String {
    ConversationId::new().to_string()
}

// A fresh request/reply correlation id, for the typed AGDX `command` / `respond`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn new_correlation_id() -> String {
    use laser_sdk::types::MintUlid;
    laser_sdk::wire::agent::CorrelationId::mint().to_string()
}

/// A stable conversation id derived from `seed` (the same seed always yields the
/// same id), for per-seed ordering and isolation without coordination.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn derive_conversation_id(seed: &str) -> String {
    ConversationId::derive(seed).to_string()
}

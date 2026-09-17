use crate::agent::PyAgentMessage;
use crate::agent_runtime::static_topic;
use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::context::{PyContextScope, PyScopedMemory, bounded_policy};
use crate::convert::payload_bytes;
use crate::errors::to_pyerr;
use crate::memory::{Backend, PyMemory};
use laser_sdk::agent::{Session, SessionConfig, SessionTurn, SessionTurnKind, Sessions};
use laser_sdk::context::Checkpoint;
use laser_sdk::memory::LogMemory;
use laser_sdk::types::ConversationId;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// The session accessor: one conversation seen as typed turns, a
    /// model-ready context, scoped memory, and checkpointed replay. Built on
    /// `Laser.context`, so a session is never a second store. Free and
    /// synchronous, IO at the verbs. The layout is configurable: `stream`
    /// puts the sessions on another stream, `topics` maps a turn kind to the
    /// topic it rides (`{"instruction": "support.turns"}`), and
    /// `memory_namespace`, `context_turns`, `context_tokens` replace the
    /// defaults (`agent.session`, 50, 4000).
    #[pyo3(signature = (*, stream=None, topics=None, memory_namespace=None, context_turns=None, context_tokens=None))]
    fn sessions(
        &self,
        stream: Option<String>,
        topics: Option<HashMap<String, String>>,
        memory_namespace: Option<String>,
        context_turns: Option<usize>,
        context_tokens: Option<usize>,
    ) -> PyResult<PySessions> {
        let mut config = SessionConfig::new();
        if let Some(stream) = stream {
            config = config.stream(stream);
        }
        for (kind, topic) in topics.unwrap_or_default() {
            config = config.topic(turn_kind(&kind)?, static_topic(topic)?);
        }
        if let Some(namespace) = memory_namespace {
            config = config.memory_namespace(namespace);
        }
        if let Some(turns) = context_turns {
            config = config.context_turns(turns);
        }
        if let Some(tokens) = context_tokens {
            config = config.context_tokens(tokens);
        }
        Ok(PySessions {
            inner: self.inner.sessions_with(config).map_err(to_pyerr)?,
        })
    }
}

/// The session factory. Build it with `Laser.sessions`.
#[gen_stub_pyclass]
#[pyclass(name = "Sessions")]
pub struct PySessions {
    inner: Sessions,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessions {
    /// The durable session named `id`. The conversation derives from `id`, so
    /// the same id always reaches the same history, and nothing is created on
    /// the server until a turn is appended.
    fn create(&self, id: String) -> PySession {
        PySession::new(self.inner.create(id))
    }

    /// A fresh anonymous session. Keep `Session.conversation` to `open` it
    /// again later.
    fn start(&self) -> PySession {
        PySession::new(self.inner.start())
    }

    /// The session over an existing conversation id: one minted by `start`,
    /// carried by an inbound message's provenance, or a sub-conversation.
    fn open(&self, conversation_id: String) -> PyResult<PySession> {
        let conversation =
            ConversationId::from_str(&conversation_id).map_err(|e| to_pyerr(e.into()))?;
        Ok(PySession::new(self.inner.open(conversation)))
    }
}

/// One agent session over a conversation. Turns are agent messages on the
/// conversation-level topics, the context is a bounded assembly of them,
/// memory is the conversation's scoped memory, and a `Checkpoint` bounds
/// point-in-time and incremental replay. Build it with `Laser.sessions`.
#[gen_stub_pyclass]
#[pyclass(name = "Session")]
pub struct PySession {
    inner: Session,
}

impl PySession {
    fn new(inner: Session) -> Self {
        Self { inner }
    }
}

fn turn_kind(kind: &str) -> PyResult<SessionTurnKind> {
    SessionTurnKind::from_str(kind).map_err(|_| {
        crate::errors::InvalidError::new_err(format!(
            "unknown session turn kind '{kind}' (expected instruction, response, model.response, tool.call, tool.result, or human.input)"
        ))
    })
}

#[gen_stub_pymethods]
#[pymethods]
impl PySession {
    /// This session's conversation id.
    #[getter]
    fn conversation(&self) -> String {
        self.inner.conversation().to_string()
    }

    /// The underlying `ContextScope`, for a topic outside the session's set,
    /// an explicit read shape, or the knowledge graph.
    fn scope(&self) -> PyContextScope {
        PyContextScope::new(self.inner.scope().clone())
    }

    /// Append one turn. `kind` is one of `instruction`, `response`,
    /// `model.response`, `tool.call`, `tool.result`, or `human.input`, and
    /// `data` is str, bytes, or bytearray.
    fn append<'py>(
        &self,
        py: Python<'py>,
        kind: &str,
        data: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let kind = turn_kind(kind)?;
        let data = payload_bytes(data)?;
        future_into_py(py, async move {
            session.append(kind, data).await.map_err(to_pyerr)
        })
    }

    /// The model-ready context: the last `last_n` turns across the session's
    /// topics, trimmed to `token_budget` estimated tokens. Both default to the
    /// factory's configured bounds (50 turns, 4000 tokens unless changed).
    #[pyo3(signature = (*, last_n=None, token_budget=None))]
    fn context<'py>(
        &self,
        py: Python<'py>,
        last_n: Option<usize>,
        token_budget: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let config = self.inner.config();
        let policy = bounded_policy(
            last_n.unwrap_or(config.context_turn_bound()),
            Some(token_budget.unwrap_or(config.context_token_bound())),
        );
        future_into_py(py, async move {
            let turns = session.context_with(policy).await.map_err(to_pyerr)?;
            Ok(turns
                .into_iter()
                .map(PySessionTurn::from)
                .collect::<Vec<_>>())
        })
    }

    /// This session's memory, scoped to the conversation: `memory` defaults to
    /// `laser.memory(<configured namespace>)`, or pass any handle from
    /// `laser.memory`/`memory_on_topic`/`memory_topic`/`vector_memory`.
    #[pyo3(signature = (memory=None))]
    fn memory(&self, memory: Option<&PyMemory>) -> PyScopedMemory {
        let backend = match memory {
            Some(memory) => memory.backend(),
            None => Backend::Log(Arc::new(LogMemory::in_namespace(
                self.inner.scope().laser().clone(),
                self.inner.config().memory_namespace_name().to_owned(),
            ))),
        };
        PyScopedMemory::new(backend, self.inner.conversation())
    }

    /// Where this session's topics end right now. Persist it with
    /// `Checkpoint.to_json` and hand it to `turns_at`, `turns_since`,
    /// `state_at`, or `replay`.
    fn checkpoint<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        future_into_py(py, async move {
            let checkpoint = session.checkpoint().await.map_err(to_pyerr)?;
            Ok(PyCheckpoint { inner: checkpoint })
        })
    }

    /// The turns up to `checkpoint`.
    fn turns_at<'py>(
        &self,
        py: Python<'py>,
        checkpoint: &PyCheckpoint,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let checkpoint = checkpoint.inner.clone();
        future_into_py(py, async move {
            let turns = session.turns_at(checkpoint).await.map_err(to_pyerr)?;
            Ok(turns
                .into_iter()
                .map(PySessionTurn::from)
                .collect::<Vec<_>>())
        })
    }

    /// The turns appended after `checkpoint`.
    fn turns_since<'py>(
        &self,
        py: Python<'py>,
        checkpoint: &PyCheckpoint,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let checkpoint = checkpoint.inner.clone();
        future_into_py(py, async move {
            let turns = session.turns_since(checkpoint).await.map_err(to_pyerr)?;
            Ok(turns
                .into_iter()
                .map(PySessionTurn::from)
                .collect::<Vec<_>>())
        })
    }

    /// Fold the turns up to `checkpoint` with `fold(state, turn) -> state`,
    /// starting from `initial`: state as it stood then.
    fn state_at<'py>(
        &self,
        py: Python<'py>,
        checkpoint: &PyCheckpoint,
        initial: Py<PyAny>,
        fold: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let checkpoint = checkpoint.inner.clone();
        future_into_py(py, async move {
            let turns = session.turns_at(checkpoint).await.map_err(to_pyerr)?;
            fold_turns(turns, initial, fold)
        })
    }

    /// Fold the turns appended after `checkpoint` with `fold(state, turn) ->
    /// state`, starting from `initial`: bring state saved at that checkpoint
    /// up to date.
    fn replay<'py>(
        &self,
        py: Python<'py>,
        checkpoint: &PyCheckpoint,
        initial: Py<PyAny>,
        fold: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let checkpoint = checkpoint.inner.clone();
        future_into_py(py, async move {
            let turns = session.turns_since(checkpoint).await.map_err(to_pyerr)?;
            fold_turns(turns, initial, fold)
        })
    }
}

fn fold_turns(turns: Vec<SessionTurn>, initial: Py<PyAny>, fold: Py<PyAny>) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let mut state = initial;
        for turn in turns {
            let turn = Py::new(py, PySessionTurn::from(turn))?;
            state = fold.call1(py, (state, turn))?;
        }
        Ok(state)
    })
}

/// One turn read back from a `Session`: the message plus the kind its topic
/// implies.
#[gen_stub_pyclass]
#[pyclass(name = "SessionTurn", frozen)]
pub struct PySessionTurn {
    kind: SessionTurnKind,
    message: PyAgentMessage,
}

impl From<SessionTurn> for PySessionTurn {
    fn from(turn: SessionTurn) -> Self {
        Self {
            kind: turn.kind,
            message: PyAgentMessage::from_context(turn.message),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessionTurn {
    /// The turn kind: `instruction`, `response`, `model.response`,
    /// `tool.call`, `tool.result`, or `human.input`.
    #[getter]
    fn kind(&self) -> String {
        self.kind.to_string()
    }

    /// The raw payload.
    #[getter]
    fn payload(&self) -> Vec<u8> {
        self.message.inner.payload.clone()
    }

    /// The payload as UTF-8, lossy.
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.message.inner.payload).into_owned()
    }

    /// The message off the log, with its provenance and topic.
    #[getter]
    fn message(&self) -> PyAgentMessage {
        PyAgentMessage {
            inner: self.message.inner.clone(),
            topic: self.message.topic.clone(),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "SessionTurn(kind={:?}, text={:?})",
            self.kind.to_string(),
            self.text()
        )
    }
}

/// A point in a session's log: the next offset each partition of each topic
/// will write, as of `Session.checkpoint`. A client-side bookmark, never a
/// record on the log. Round-trip it through `to_json`/`from_json` to persist
/// it.
#[gen_stub_pyclass]
#[pyclass(name = "Checkpoint", frozen)]
pub struct PyCheckpoint {
    inner: Checkpoint,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCheckpoint {
    /// True when no topic was checkpointed.
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// This checkpoint as JSON.
    fn to_json(&self) -> PyResult<String> {
        serde_json::to_string(&self.inner)
            .map_err(|error| crate::errors::CodecError::new_err(error.to_string()))
    }

    /// A checkpoint from `to_json` output.
    #[staticmethod]
    fn from_json(json: &str) -> PyResult<Self> {
        let inner = serde_json::from_str(json)
            .map_err(|error| crate::errors::CodecError::new_err(error.to_string()))?;
        Ok(Self { inner })
    }

    fn __repr__(&self) -> String {
        format!("Checkpoint({})", self.to_json().unwrap_or_default())
    }
}

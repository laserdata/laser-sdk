use crate::agent_runtime::static_topic;
use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::context::{
    PyContextMessage, PyContextScope, PyScopedMemory, bounded_policy, context_policy, take_failure,
};
use crate::convert::payload_bytes;
use crate::errors::to_pyerr;
use crate::memory::{Backend, PyMemory};
use laser_sdk::agent::{
    Session, SessionConfig, SessionPolicy, SessionTurn, SessionTurnKind, Sessions,
};
use laser_sdk::context::{Checkpoint, ContextMessage};
use laser_sdk::types::ConversationId;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::collections::HashMap;
use std::str::FromStr;

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
    /// The default topic a turn `kind` is recorded on.
    #[staticmethod]
    fn turn_topic(kind: &str) -> PyResult<String> {
        Ok(turn_kind(kind)?.topic().topic_string())
    }

    /// The turn kind recorded on `topic` by default, or None for any other topic.
    #[staticmethod]
    fn turn_kind(topic: &str) -> Option<String> {
        SessionTurnKind::for_topic(topic).map(|kind| kind.to_string())
    }

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

    /// The layout every session of this factory uses.
    #[getter]
    fn config(&self) -> PySessionConfig {
        PySessionConfig {
            inner: self.inner.config().clone(),
        }
    }
}

/// How a `Sessions` factory lays its sessions out on the log: the stream, the
/// topic each turn kind rides, the memory namespace, and the context bounds.
/// Read it from `Sessions.config` or `Session.config`, set it through the
/// `Laser.sessions` keywords.
#[gen_stub_pyclass]
#[pyclass(name = "SessionConfig", frozen)]
pub struct PySessionConfig {
    inner: SessionConfig,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessionConfig {
    /// The stream sessions ride, or `None` for the connection's default stream.
    #[getter]
    fn stream_name(&self) -> Option<&str> {
        self.inner.stream_name()
    }

    /// The memory namespace `Session.memory` opens.
    #[getter]
    fn memory_namespace_name(&self) -> &str {
        self.inner.memory_namespace_name()
    }

    /// The turn bound of `Session.context`.
    #[getter]
    fn context_turn_bound(&self) -> usize {
        self.inner.context_turn_bound()
    }

    /// The estimated token bound of `Session.context`.
    #[getter]
    fn context_token_bound(&self) -> usize {
        self.inner.context_token_bound()
    }

    /// Every topic this layout reads, in turn kind order.
    #[getter]
    fn topics(&self) -> Vec<String> {
        self.inner
            .topics()
            .iter()
            .map(|topic| topic.topic_string())
            .collect()
    }

    /// The topic the turn `kind` rides under this layout.
    fn topic_for(&self, kind: &str) -> PyResult<String> {
        Ok(self.inner.topic_for(turn_kind(kind)?).topic_string())
    }

    /// The turn kind that rides `topic` under this layout, or None.
    fn kind_for(&self, topic: &str) -> Option<String> {
        self.inner.kind_for(topic).map(|kind| kind.to_string())
    }

    fn __repr__(&self) -> String {
        format!(
            "SessionConfig(stream={:?}, memory_namespace={:?}, context_turns={}, context_tokens={})",
            self.inner.stream_name(),
            self.inner.memory_namespace_name(),
            self.inner.context_turn_bound(),
            self.inner.context_token_bound()
        )
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

/// The conversation id a session `policy` maps `key` to: `per_call` mints a
/// fresh one on every call, `per_user` derives the same one for the same key.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn session_policy_conversation_for(policy: &str, key: &str) -> PyResult<String> {
    let policy = match policy {
        "per_call" => SessionPolicy::PerCall,
        "per_user" => SessionPolicy::PerUser,
        other => {
            return Err(crate::errors::InvalidError::new_err(format!(
                "unknown session policy '{other}' (expected per_call or per_user)"
            )));
        }
    };
    Ok(policy.conversation_for(key).to_string())
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

    /// The layout this session uses, its factory's `config`.
    #[getter]
    fn config(&self) -> PySessionConfig {
        PySessionConfig {
            inner: self.inner.config().clone(),
        }
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

    /// The context under an explicit policy: `LastN`, `TokenBudget`,
    /// `RoleFilter`, or a `Chain` of them, the same values
    /// `ContextScope.fetch_with` takes.
    fn context_with<'py>(
        &self,
        py: Python<'py>,
        policy: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let (policy, failure) = context_policy(policy)?;
        future_into_py(py, async move {
            let turns = session.context_with(policy).await.map_err(to_pyerr)?;
            if let Some(error) = take_failure(&failure) {
                return Err(error);
            }
            Ok(turns
                .into_iter()
                .map(PySessionTurn::from)
                .collect::<Vec<_>>())
        })
    }

    /// The knowledge graph `name`, the same graph `laser.graph(name)` returns.
    /// It is shared across conversations, so the session does not narrow it.
    fn graph(&self, name: String) -> crate::graph::PyGraph {
        crate::graph::PyGraph::new(self.inner.scope().laser().clone(), name)
    }

    /// This session's memory, scoped to the conversation: `memory` defaults to
    /// `laser.memory(<configured namespace>)`, or pass any handle from
    /// `laser.memory`/`memory_on_topic`/`memory_topic`/`memory_with` or `VectorMemory.governed(laser, embedder)`.
    #[pyo3(signature = (memory=None))]
    fn memory(&self, memory: Option<&PyMemory>) -> PyScopedMemory {
        let backend = match memory {
            Some(memory) => memory.backend(),
            None => Backend::new(
                self.inner
                    .scope()
                    .laser()
                    .memory(self.inner.config().memory_namespace_name()),
            ),
        };
        PyScopedMemory::new(backend, self.inner.conversation())
    }

    /// This session's memory in an explicit `namespace`, scoped to the
    /// conversation.
    fn memory_in(&self, namespace: String) -> PyScopedMemory {
        PyScopedMemory::new(
            Backend::new(self.inner.scope().laser().memory(namespace)),
            self.inner.conversation(),
        )
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
    /// starting from `init`: state as it stood then.
    fn state_at<'py>(
        &self,
        py: Python<'py>,
        checkpoint: &PyCheckpoint,
        init: Py<PyAny>,
        fold: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let checkpoint = checkpoint.inner.clone();
        future_into_py(py, async move {
            let turns = session.turns_at(checkpoint).await.map_err(to_pyerr)?;
            fold_turns(turns, init, fold)
        })
    }

    /// Fold the turns appended after `checkpoint` with `fold(state, turn) ->
    /// state`, starting from `init`: bring state saved at that checkpoint
    /// up to date.
    fn replay<'py>(
        &self,
        py: Python<'py>,
        checkpoint: &PyCheckpoint,
        init: Py<PyAny>,
        fold: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let session = self.inner.clone();
        let checkpoint = checkpoint.inner.clone();
        future_into_py(py, async move {
            let turns = session.turns_since(checkpoint).await.map_err(to_pyerr)?;
            fold_turns(turns, init, fold)
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
    message: ContextMessage,
}

impl From<SessionTurn> for PySessionTurn {
    fn from(turn: SessionTurn) -> Self {
        Self {
            kind: turn.kind,
            message: turn.message,
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

    /// The payload as UTF-8, lossy.
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.message.payload).into_owned()
    }

    /// The message off the log, with its provenance and topic.
    #[getter]
    fn message(&self) -> PyContextMessage {
        PyContextMessage::new(self.message.clone())
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

impl PyCheckpoint {
    pub(crate) fn new(inner: Checkpoint) -> Self {
        Self { inner }
    }

    pub(crate) fn inner(&self) -> &Checkpoint {
        &self.inner
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCheckpoint {
    /// True when no topic was checkpointed.
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// The checkpointed offsets of `topic` as a dict partition -> offset, or
    /// None when the topic was not checkpointed.
    fn topic_offsets(&self, topic: &str) -> Option<std::collections::BTreeMap<u32, u64>> {
        self.inner.topic_offsets(topic).cloned()
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

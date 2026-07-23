use crate::agent::PyAgentMessage;
use crate::agent_runtime::static_topic;
use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::payload_bytes;
use crate::errors::to_pyerr;
use crate::memory::{Backend, PyMemory, PyMemoryItem, build_scope, map_strategy};
use laser_sdk::agent::AgentMessage;
use laser_sdk::laser::Laser;
use laser_sdk::memory::{MemoryQuery, RecallStrategy};
use laser_sdk::types::ConversationId;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// The context accessor: one conversation's working record on the log.
    /// `append` publishes into the conversation, `fetch` reads it back bounded,
    /// and `memory` scopes a memory handle to the same conversation so recall
    /// and remember never repeat the id. Free and synchronous, IO at the verbs.
    fn context(&self, conversation_id: String) -> PyResult<PyContextScope> {
        let conversation =
            ConversationId::from_str(&conversation_id).map_err(|e| to_pyerr(e.into()))?;
        Ok(PyContextScope {
            laser: self.inner.clone(),
            conversation,
        })
    }
}

/// One conversation's working context. Build it with `Laser.context`.
#[gen_stub_pyclass]
#[pyclass(name = "ContextScope")]
pub struct PyContextScope {
    laser: Laser,
    conversation: ConversationId,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyContextScope {
    /// Append `payload` (str, bytes, or bytearray) to `topic` within this
    /// conversation. The provenance is pinned to the conversation so a later
    /// `fetch` reads it back in order.
    fn append<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let conversation = self.conversation;
        let topic = static_topic(topic);
        let payload = payload_bytes(payload)?;
        future_into_py(py, async move {
            laser
                .context(conversation)
                .append(topic, payload)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Read this conversation's history from `topics`, bounded to the last
    /// `last_n` messages (default 50). The bound is required by construction:
    /// an unbounded read of a long conversation is a replay nobody asked for.
    #[pyo3(signature = (*, topics=None, last_n=None))]
    fn fetch<'py>(
        &self,
        py: Python<'py>,
        topics: Option<Vec<String>>,
        last_n: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let conversation = self.conversation;
        let topics: Vec<_> = topics
            .unwrap_or_default()
            .into_iter()
            .map(static_topic)
            .collect();
        let last_n = last_n.unwrap_or(50);
        future_into_py(py, async move {
            let messages = laser
                .context(conversation)
                .fetch(topics, last_n)
                .await
                .map_err(to_pyerr)?;
            Ok(messages
                .into_iter()
                .map(|message| {
                    PyAgentMessage::from_inner(AgentMessage {
                        provenance: message.provenance,
                        payload: message.payload,
                        id: message.id,
                        envelope: message.envelope,
                        content_type: None,
                        verified_principal: None,
                    })
                })
                .collect::<Vec<_>>())
        })
    }

    /// The last `last_n` messages rendered as one newline-joined text block,
    /// the prompt-ready form.
    #[pyo3(signature = (*, topics=None, last_n=None))]
    fn block<'py>(
        &self,
        py: Python<'py>,
        topics: Option<Vec<String>>,
        last_n: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let conversation = self.conversation;
        let topics: Vec<_> = topics
            .unwrap_or_default()
            .into_iter()
            .map(static_topic)
            .collect();
        let last_n = last_n.unwrap_or(50);
        future_into_py(py, async move {
            laser
                .context(conversation)
                .block(topics, last_n)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Scope `memory` (any handle from `laser.memory`/`memory_on_topic`/
    /// `memory_topic`/`vector_memory`) to this conversation: the returned view's
    /// `recall` and `remember` bake the conversation in, so the session's
    /// messages and its memory share one scope. Durable facts and the graph
    /// stay cross-conversation and are reached through the unscoped handle.
    fn memory(&self, memory: &PyMemory) -> PyScopedMemory {
        PyScopedMemory {
            backend: memory.backend(),
            conversation: self.conversation,
        }
    }

    /// The knowledge graph `name`, reached from this scope for the common flow
    /// where a task streams messages, keeps session memory, and resolves the
    /// dependencies between them. Returned unnarrowed on purpose: a dependency
    /// or knowledge graph is shared across conversations, so scoping it to one
    /// would hide the relationships the caller wants. Identical to
    /// `Laser.graph`, offered here so one scope reaches every primitive.
    fn graph(&self, name: String) -> crate::graph::PyGraph {
        crate::graph::PyGraph::new(self.laser.clone(), name)
    }

    /// This context's conversation id.
    #[getter]
    fn conversation(&self) -> String {
        self.conversation.to_string()
    }
}

/// One conversation's memory: recall and remember with the conversation already
/// applied. Build it with `ContextScope.memory`.
#[gen_stub_pyclass]
#[pyclass(name = "ScopedMemory")]
pub struct PyScopedMemory {
    backend: Backend,
    conversation: ConversationId,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyScopedMemory {
    /// Recall up to `limit` items within this conversation. Pass `semantic`
    /// text to rank by similarity (backends that embed), or a `strategy`.
    #[pyo3(signature = (*, limit=50, semantic=None, strategy=None))]
    fn recall<'py>(
        &self,
        py: Python<'py>,
        limit: usize,
        semantic: Option<String>,
        strategy: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.backend.clone();
        let scope = build_scope(None, Some(self.conversation.to_string()))?;
        let strategy = match strategy {
            Some(name) => map_strategy(&name)?,
            None if semantic.is_some() => RecallStrategy::Semantic,
            None => RecallStrategy::Auto,
        };
        let query = MemoryQuery::builder()
            .limit(limit)
            .maybe_semantic(semantic)
            .strategy(strategy)
            .build();
        future_into_py(py, async move {
            let items = backend.recall(scope, query).await.map_err(to_pyerr)?;
            Ok(items
                .into_iter()
                .map(PyMemoryItem::from)
                .collect::<Vec<_>>())
        })
    }

    /// Remember `payload` (str, bytes, or bytearray) in this conversation's
    /// session scope. Returns the new item's id.
    fn remember<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.backend.clone();
        let scope = build_scope(None, Some(self.conversation.to_string()))?;
        let payload = payload_bytes(payload)?;
        future_into_py(py, async move {
            let id = backend.remember(scope, payload).await.map_err(to_pyerr)?;
            Ok(id.to_string())
        })
    }

    /// This scoped memory's conversation id.
    #[getter]
    fn conversation(&self) -> String {
        self.conversation.to_string()
    }
}

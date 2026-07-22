use crate::agent::PyProvenance;
use crate::client::PyLaser;
use crate::convert::{json_to_py, payload_bytes};
use crate::errors::to_pyerr;
use laser_sdk::error::LaserError;
use laser_sdk::memory::{
    Embedder, Feedback, LogMemory, Memory, MemoryId, MemoryItem, MemoryKind, MemoryQuery,
    MemoryScope, RecallStrategy, VectorMemory,
};
use laser_sdk::types::{AgentId, ConversationId};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{future_into_py, into_future};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

// A Python `async def embed(text) -> list[float]` driving the SDK `Embedder`
// trait. Holds the captured event loop via `into_future`, so it runs inside the
// future scoped by `future_into_py` when a memory call needs an embedding. The
// callback is `Arc`-wrapped because `Py<PyAny>` is not `Clone` without the GIL.
#[derive(Clone)]
pub(crate) struct PyEmbedder {
    callback: Arc<Py<PyAny>>,
}

impl Embedder for PyEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError> {
        let text = text.to_owned();
        let future = Python::attach(|py| -> PyResult<_> {
            let coroutine = self.callback.bind(py).call1((text,))?;
            into_future(coroutine)
        })
        .map_err(|error| LaserError::Handler(format!("embedder call failed: {error}")))?;
        let value = future
            .await
            .map_err(|error| LaserError::Handler(format!("embedder await failed: {error}")))?;
        Python::attach(|py| value.bind(py).extract::<Vec<f32>>()).map_err(|error| {
            LaserError::Handler(format!("embedder must return list[float]: {error}"))
        })
    }
}

// The concrete backend behind a `PyMemory`, cheap to clone. One durable model -
// the log backend, held long-lived so its incremental recall cursor survives
// across calls (never rescanning the audit log from offset zero). `Vector` is the
// in-process similarity index.
#[derive(Clone)]
pub(crate) enum Backend {
    Log(Arc<LogMemory>),
    Vector(Arc<VectorMemory<PyEmbedder>>),
}

// Resolve a `Backend` to a concrete `&impl Memory` and run `$op` against it,
// so the operations share one dispatch instead of repeating the match.
macro_rules! on_backend {
    ($self:expr, |$memory:ident| $op:expr) => {
        match $self {
            Backend::Log(memory) => {
                let $memory = memory.as_ref();
                $op
            }
            Backend::Vector(memory) => {
                let $memory = memory.as_ref();
                $op
            }
        }
    };
}

impl Backend {
    pub(crate) async fn remember(
        &self,
        scope: MemoryScope,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        on_backend!(self, |memory| Memory::remember(memory, &scope, payload)
            .await)
    }

    pub(crate) async fn recall(
        &self,
        scope: MemoryScope,
        query: MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        on_backend!(self, |memory| Memory::recall(memory, &scope, &query).await)
    }

    // Recall by folding the topic in process (the opt-in). The log backend folds,
    // the vector backend is already in process, so it recalls as usual.
    pub(crate) async fn recall_folded(
        &self,
        scope: MemoryScope,
        query: MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        match self {
            Backend::Log(memory) => memory.recall_folded(&scope, &query).await,
            Backend::Vector(memory) => Memory::recall(memory.as_ref(), &scope, &query).await,
        }
    }

    async fn forget(&self, scope: MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        on_backend!(self, |memory| Memory::forget(memory, &scope, id).await)
    }

    async fn improve(
        &self,
        scope: MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        on_backend!(self, |memory| Memory::improve(memory, &scope, feedback)
            .await)
    }
}

// Map a recall-strategy string (the Python surface uses string literals, mapped
// to the typed `RecallStrategy`) to the enum, erroring on an unknown value.
pub(crate) fn map_strategy(strategy: &str) -> PyResult<RecallStrategy> {
    Ok(match strategy {
        "auto" => RecallStrategy::Auto,
        "recent" => RecallStrategy::Recent,
        "semantic" => RecallStrategy::Semantic,
        "keyword" => RecallStrategy::Keyword,
        "graph" => RecallStrategy::Graph,
        "temporal" => RecallStrategy::Temporal,
        "hybrid" => RecallStrategy::Hybrid,
        other => {
            return Err(crate::errors::CodecError::new_err(format!(
                "unknown recall strategy '{other}'"
            )));
        }
    })
}

pub(crate) fn build_scope(
    agent: Option<String>,
    conversation: Option<String>,
) -> PyResult<MemoryScope> {
    let agent = match agent {
        Some(value) => Some(AgentId::new(value).map_err(|e| to_pyerr(e.into()))?),
        None => None,
    };
    let conversation = match conversation {
        Some(value) => Some(ConversationId::from_str(&value).map_err(|e| to_pyerr(e.into()))?),
        None => None,
    };
    Ok(MemoryScope::builder()
        .maybe_agent(agent)
        .maybe_conversation(conversation)
        .build())
}

/// Agent memory: `remember` a payload, `recall` the most relevant items, and
/// `forget` one by id. The backend decides how recall ranks: the vector backend
/// scores by semantic similarity to a query, the log and managed backends return
/// the most recent. Scope every call to an agent and/or a conversation. Construct
/// one from a `Laser` (`laser.memory(namespace)`, `laser.memory_on_topic(...)`,
/// `laser.memory_topic(...)`, or `laser.vector_memory(embedder)`).
#[gen_stub_pyclass]
#[pyclass(name = "Memory", frozen)]
pub struct PyMemory {
    inner: Backend,
}

impl PyMemory {
    pub(crate) fn backend(&self) -> Backend {
        self.inner.clone()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMemory {
    /// The simple altitude: write named point state under your own `key` (the
    /// working-note shape). Every write is a durable event on the memory topic,
    /// like the rest of memory. The vector handle raises `UnsupportedError`.
    fn set<'py>(
        &self,
        py: Python<'py>,
        key: String,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let memory = self.log_memory("set")?;
        let payload = payload_bytes(payload)?;
        future_into_py(py, async move {
            memory.set_named(&key, payload).await.map_err(to_pyerr)
        })
    }

    /// Point-read the named item written by `set`, or `None`.
    fn fetch<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let memory = self.log_memory("fetch")?;
        future_into_py(py, async move {
            memory.fetch_named(&key).await.map_err(to_pyerr)
        })
    }

    /// Point-read the named item by folding the topic in process, the opt-in for
    /// a deployment with no managed read view. Prefer `fetch`, which reads it.
    fn fetch_folded<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let memory = self.log_memory("fetch_folded")?;
        future_into_py(py, async move {
            memory.fetch_named_folded(&key).await.map_err(to_pyerr)
        })
    }

    /// Merge-patch the named item (RFC 7386 over a JSON value): fields in
    /// `patch` overwrite, `None` removes. For a whole-value overwrite use `set`.
    fn update<'py>(
        &self,
        py: Python<'py>,
        key: String,
        patch: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let memory = self.log_memory("update")?;
        let patch = payload_bytes(patch)?;
        future_into_py(py, async move {
            memory.update_named(&key, patch).await.map_err(to_pyerr)
        })
    }

    /// Delete the named item. Idempotent: removing an absent key is fine.
    fn remove<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let memory = self.log_memory("remove")?;
        future_into_py(py, async move {
            memory.forget_named(&key).await.map_err(to_pyerr)
        })
    }

    /// Remember `payload` (`str`, `bytes`, or `bytearray`) under the given scope.
    /// Returns the new item's id (a time-ordered ULID string).
    #[pyo3(signature = (payload, *, agent=None, conversation=None))]
    fn remember<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'_, PyAny>,
        agent: Option<String>,
        conversation: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let scope = build_scope(agent, conversation)?;
        let payload = payload_bytes(payload)?;
        future_into_py(py, async move {
            let id = backend.remember(scope, payload).await.map_err(to_pyerr)?;
            Ok(id.to_string())
        })
    }

    /// Recall up to `limit` items under the scope. Pass `semantic` text to rank by
    /// similarity (the vector backend embeds and scores it, the others ignore it
    /// and return the most recent).
    #[pyo3(signature = (*, limit=50, agent=None, conversation=None, semantic=None, strategy=None, folded=false))]
    #[allow(clippy::too_many_arguments)]
    fn recall<'py>(
        &self,
        py: Python<'py>,
        limit: usize,
        agent: Option<String>,
        conversation: Option<String>,
        semantic: Option<String>,
        strategy: Option<String>,
        folded: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let scope = build_scope(agent, conversation)?;
        // An explicit strategy wins. Otherwise a `semantic` query implies the
        // semantic strategy, and a plain recall stays `Auto`.
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
            let items = if folded {
                backend.recall_folded(scope, query).await
            } else {
                backend.recall(scope, query).await
            }
            .map_err(to_pyerr)?;
            Ok(items
                .into_iter()
                .map(PyMemoryItem::from)
                .collect::<Vec<_>>())
        })
    }

    /// Record feedback on a recalled item, the signal a ranking backend folds
    /// into future recall. `weight` is positive to promote, negative to demote.
    /// Returns the feedback record's id.
    #[pyo3(signature = (memory_id, weight, *, agent=None, conversation=None))]
    fn improve<'py>(
        &self,
        py: Python<'py>,
        memory_id: String,
        weight: f32,
        agent: Option<String>,
        conversation: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let scope = build_scope(agent, conversation)?;
        let target = MemoryId::from_str(&memory_id).map_err(|e| to_pyerr(e.into()))?;
        future_into_py(py, async move {
            let id = backend
                .improve(scope, Feedback::new(target, weight))
                .await
                .map_err(to_pyerr)?;
            Ok(id.to_string())
        })
    }

    /// Forget the item with id `memory_id` (a ULID string) under the scope.
    #[pyo3(signature = (memory_id, *, agent=None, conversation=None))]
    fn forget<'py>(
        &self,
        py: Python<'py>,
        memory_id: String,
        agent: Option<String>,
        conversation: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let scope = build_scope(agent, conversation)?;
        let id = MemoryId::from_str(&memory_id).map_err(|e| to_pyerr(e.into()))?;
        future_into_py(py, async move {
            backend.forget(scope, id).await.map_err(to_pyerr)
        })
    }
}

impl PyMemory {
    // The named-item altitude rides the memory topic like every other write:
    // only the durable log handle has a key space, so the vector handle refuses
    // typed rather than inventing a key scheme content-addressed storage cannot
    // honor.
    fn log_memory(&self, verb: &str) -> PyResult<Arc<LogMemory>> {
        match &self.inner {
            Backend::Log(memory) => Ok(memory.clone()),
            _ => Err(to_pyerr(laser_sdk::LaserError::unsupported(
                "memory",
                format!(
                    "{verb}(key) is the named-item altitude and needs the durable memory handle"
                ),
            ))),
        }
    }
}

/// One remembered item: its id, the raw payload bytes, and the provenance it was
/// stored under (its scope's agent and conversation).
#[gen_stub_pyclass]
#[pyclass(name = "MemoryItem", frozen)]
pub struct PyMemoryItem {
    inner: MemoryItem,
}

impl From<MemoryItem> for PyMemoryItem {
    fn from(inner: MemoryItem) -> Self {
        Self { inner }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMemoryItem {
    #[getter]
    fn id(&self) -> String {
        self.inner.id.to_string()
    }

    #[getter]
    fn payload(&self) -> Vec<u8> {
        self.inner.payload.clone()
    }

    /// The payload decoded from UTF-8, raising if it is not valid text.
    #[getter]
    fn text(&self) -> PyResult<String> {
        String::from_utf8(self.inner.payload.clone())
            .map_err(|e| crate::errors::CodecError::new_err(e.to_string()))
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

    /// What the item is, as a string (`fact` / `message` / `summary` / `entity`
    /// / `feedback` / `procedure`).
    #[getter]
    fn kind(&self) -> String {
        match self.inner.kind {
            MemoryKind::Fact => "fact",
            MemoryKind::Message => "message",
            MemoryKind::Summary => "summary",
            MemoryKind::Entity => "entity",
            MemoryKind::Feedback => "feedback",
            MemoryKind::Procedure => "procedure",
        }
        .to_owned()
    }

    /// The recall score from a ranking strategy, or `None` for an unranked recall.
    #[getter]
    fn score(&self) -> Option<f32> {
        self.inner.score
    }

    /// The origin log record this item was folded from, as
    /// `(stream, topic, partition, offset, conversation)`, or `None`. Points back
    /// to the source message while it is still on the log.
    #[getter]
    fn source(&self) -> Option<(u32, u32, u32, u64, Option<String>)> {
        match &self.inner.source {
            Some(laser_sdk::wire::graph::SourceRef::Message {
                stream,
                topic,
                partition,
                offset,
                conversation,
            }) => Some((*stream, *topic, *partition, *offset, conversation.clone())),
            _ => None,
        }
    }

    /// Which signals produced this candidate: `(strategy, rank, score)` tuples,
    /// the per-signal attribution a fused recall keeps (empty when unranked).
    #[getter]
    fn signals(&self) -> Vec<(String, usize, Option<f32>)> {
        self.inner
            .signals
            .iter()
            .map(|signal| {
                (
                    format!("{:?}", signal.strategy).to_lowercase(),
                    signal.rank,
                    signal.score,
                )
            })
            .collect()
    }

    /// Decode the payload as JSON into a Python value.
    fn json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value: serde_json::Value = serde_json::from_slice(&self.inner.payload)
            .map_err(|e| crate::errors::CodecError::new_err(e.to_string()))?;
        json_to_py(py, &value)
    }

    fn __repr__(&self) -> String {
        format!(
            "MemoryItem(id={}, conversation_id={}, bytes={})",
            self.inner.id,
            self.inner.provenance.conversation_id,
            self.inner.payload.len()
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Agent memory in `namespace`: `remember` publishes to the memory topic (the
    /// durable audit), `forget` appends a tombstone, `recall` folds it back (most
    /// recent first, `semantic` ignored). One model: a deployment materializes
    /// the topic into the versioned key-value read view. `namespace` prefixes the
    /// named-item altitude's keys (`set`/`fetch`). For an isolated per-topic
    /// memory stream use `memory_on_topic` or `memory_topic`. Works on raw Apache
    /// Iggy. Reuse the handle: each `recall` folds only what is new.
    fn memory(&self, namespace: String) -> PyMemory {
        PyMemory {
            inner: Backend::Log(Arc::new(LogMemory::in_namespace(
                self.inner.clone(),
                namespace,
            ))),
        }
    }

    /// Agent memory on a caller-named topic (and optional stream), so a
    /// deployment can keep several isolated memory streams. Ensure the topic up
    /// front like any other, for example `await laser.topic(name).ensure(...)`.
    #[pyo3(signature = (topic, *, stream=None))]
    fn memory_on_topic(&self, topic: String, stream: Option<String>) -> PyResult<PyMemory> {
        let memory = LogMemory::on_stream_topic_named(self.inner.clone(), stream, &topic)
            .map_err(to_pyerr)?;
        Ok(PyMemory {
            inner: Backend::Log(Arc::new(memory)),
        })
    }

    /// Configure and ensure a memory topic, then return memory on it: the
    /// stream, the partition count (each scope keyed to one partition), and the
    /// stream message-expiry. `ttl_secs` defaults to thirty days. Pass `0` to
    /// keep the history until topic retention rotates it out.
    #[pyo3(signature = (topic, *, stream=None, partitions=1, ttl_secs=None))]
    fn memory_topic<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        stream: Option<String>,
        partitions: u32,
        ttl_secs: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let mut builder = laser.memory_topic(&topic).partitions(partitions);
            if let Some(stream) = &stream {
                builder = builder.stream(stream);
            }
            builder = match ttl_secs {
                None => builder,
                Some(seconds) if seconds <= 0.0 => builder.no_expiry(),
                Some(seconds) => builder.ttl(Duration::from_secs_f64(seconds)),
            };
            builder.build().await.map_err(to_pyerr)?;
            let memory = LogMemory::on_stream_topic_named(laser.clone(), stream, &topic)
                .map_err(to_pyerr)?;
            Ok(PyMemory {
                inner: Backend::Log(Arc::new(memory)),
            })
        })
    }

    /// In-process semantic memory: `embedder` is an `async def embed(text) ->
    /// list[float]`. `recall` with `semantic` ranks items by cosine similarity to
    /// the query embedding. Self-contained, so it works anywhere.
    fn vector_memory(&self, embedder: Py<PyAny>) -> PyMemory {
        PyMemory {
            inner: Backend::Vector(Arc::new(VectorMemory::governed(
                self.inner.clone(),
                PyEmbedder {
                    callback: Arc::new(embedder),
                },
            ))),
        }
    }
}

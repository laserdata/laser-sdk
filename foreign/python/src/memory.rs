use crate::agent::PyProvenance;
use crate::async_bridge::{HookLoop, PyHook, call_hook, future_into_py, hook_callable};
use crate::client::PyLaser;
use crate::convert::{duration_ms, json_to_py, payload_bytes};
use crate::errors::to_pyerr;
use laser_sdk::error::LaserError;
use laser_sdk::memory::{
    ConsolidationReport, Consolidator, DefaultConsolidator, Embedder, Feedback, LogMemory, Memory,
    MemoryBackend, MemoryHandle, MemoryId, MemoryItem, MemoryKind, MemoryQuery, MemoryScope,
    RecallSignal, RecallStrategy, Summarizer,
};
use laser_sdk::types::{AgentId, ConversationId};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::Arc;

// An `Embedder` over a Python callable or an object with `embed(text)`,
// returning `list[float]` directly or through an awaitable.

// A message source as Python sees it: stream, topic, partition, offset, topic
// generation, and conversation.
type MessageSource = (u32, u32, u32, u64, Option<u64>, Option<String>);

#[derive(Clone)]
pub(crate) struct PyEmbedder {
    hook: PyHook,
}

impl PyEmbedder {
    fn new(embedder: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            hook: PyHook::new(embedder, "embed", "an embedder")?,
        })
    }
}

impl Embedder for PyEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, LaserError> {
        let text = text.to_owned();
        let value = self
            .hook
            .call(|py| (text,).into_pyobject(py))
            .await
            .map_err(crate::errors::from_callback_error)?;
        Python::attach(|py| value.bind(py).extract::<Vec<f32>>()).map_err(|error| {
            LaserError::HandlerConfig(format!("embedder must return list[float]: {error}"))
        })
    }
}

// A Python reranker: a callable or an object with `rerank(query, items)`,
// returning the candidates reordered (and optionally dropped or rescored),
// directly or through an awaitable. The second stage `MemoryHandle::reranker`
// adds in Rust.
#[derive(Clone)]
pub(crate) struct PyReranker {
    hook: PyHook,
}

impl PyReranker {
    async fn rerank(
        &self,
        query: &str,
        items: Vec<MemoryItem>,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        let query = query.to_owned();
        let value = self
            .hook
            .call(|py| {
                let items = items
                    .into_iter()
                    .map(|item| Py::new(py, PyMemoryItem::from(item)))
                    .collect::<PyResult<Vec<_>>>()?;
                (query, items).into_pyobject(py)
            })
            .await
            .map_err(crate::errors::from_callback_error)?;
        Python::attach(|py| memory_items(value.bind(py), None)).map_err(|error| {
            LaserError::HandlerConfig(format!("reranker must return MemoryItem values: {error}"))
        })
    }
}

// A `Summarizer` over a Python callable or an object with `summarize(bodies)`,
// taking the session bodies as `list[bytes]` and returning the summary body,
// directly or through an awaitable.
pub(crate) struct PySummarizer {
    pub(crate) callback: Py<PyAny>,
}

impl Summarizer for PySummarizer {
    async fn summarize(&self, bodies: Vec<Vec<u8>>) -> Result<Vec<u8>, LaserError> {
        let value = call_hook(&HookLoop::default(), |call| {
            let py = call.py();
            let summarize = hook_callable(self.callback.bind(py), "summarize", "a summarizer")?;
            let bodies = pyo3::types::PyList::new(
                py,
                bodies
                    .iter()
                    .map(|body| pyo3::types::PyBytes::new(py, body)),
            )?;
            call.call(&summarize, (bodies,))
        })
        .await
        .map_err(crate::errors::from_callback_error)?;
        Python::attach(|py| payload_bytes(value.bind(py))).map_err(|error| {
            LaserError::HandlerConfig(format!("summarizer must return bytes: {error}"))
        })
    }
}

// Items a Python hook returned: `MemoryItem` objects, or dicts with `payload`
// and optional `id`, `kind`, `score`, `conversation`. A dict item without a
// conversation takes the scope's, or a fresh one.
fn memory_items(
    value: &Bound<'_, PyAny>,
    scope: Option<&MemoryScope>,
) -> PyResult<Vec<MemoryItem>> {
    value
        .try_iter()?
        .map(|item| {
            let item = item?;
            if let Ok(item) = item.cast::<PyMemoryItem>() {
                return Ok(item.get().inner.clone());
            }
            let dict = item.cast::<pyo3::types::PyDict>()?;
            let payload = match dict.get_item("payload")? {
                Some(payload) => payload_bytes(&payload)?,
                None => {
                    return Err(crate::errors::InvalidError::new_err(
                        "a memory item dict needs a payload",
                    ));
                }
            };
            let id = match dict.get_item("id")? {
                Some(id) => MemoryId::from_str(&id.extract::<String>()?)
                    .map_err(|error| crate::errors::InvalidError::new_err(error.to_string()))?,
                None => MemoryId::new(),
            };
            let kind = match dict.get_item("kind")? {
                Some(kind) => map_kind(&kind.extract::<String>()?)?,
                None => MemoryKind::Fact,
            };
            let score = dict
                .get_item("score")?
                .map(|score| score.extract::<f32>())
                .transpose()?;
            let conversation = match dict.get_item("conversation")? {
                Some(conversation) => ConversationId::from_str(&conversation.extract::<String>()?)
                    .map_err(|error| to_pyerr(error.into()))?,
                None => scope
                    .and_then(|scope| scope.conversation)
                    .unwrap_or_default(),
            };
            let provenance = laser_sdk::provenance::Provenance::builder()
                .conversation_id(conversation)
                .maybe_agent(scope.and_then(|scope| scope.agent.clone()))
                .build();
            Ok(MemoryItem {
                id,
                payload,
                provenance,
                kind,
                score,
                signals: Vec::new(),
                source: None,
                origin: None,
                producer: None,
            })
        })
        .collect()
}

// A `Memory` backend implemented by a Python object with `remember(scope,
// payload) -> id`, `recall(scope, query) -> items`, `improve(scope, feedback)
// -> id`, and `forget(scope, id)`, each returning directly or through an
// awaitable. Scopes, queries, and feedback arrive as dicts.
pub(crate) struct PyCustomMemory {
    hooks: Arc<Py<PyAny>>,
    fallback: HookLoop,
}

pub(crate) fn scope_dict(py: Python<'_>, scope: &MemoryScope) -> PyResult<Py<PyAny>> {
    let dict = pyo3::types::PyDict::new(py);
    dict.set_item("stream", scope.stream.clone())?;
    dict.set_item("user", scope.user.clone())?;
    dict.set_item("agent", scope.agent.as_ref().map(ToString::to_string))?;
    dict.set_item("conversation", scope.conversation.map(|id| id.to_string()))?;
    dict.set_item("application", scope.app.clone())?;
    dict.set_item(
        "lifetime",
        match scope.lifetime {
            laser_sdk::memory::Lifetime::Durable => "durable",
            _ => "session",
        },
    )?;
    Ok(dict.into_any().unbind())
}

fn strategy_word(strategy: RecallStrategy) -> &'static str {
    match strategy {
        RecallStrategy::Recent => "recent",
        RecallStrategy::Semantic => "semantic",
        RecallStrategy::Keyword => "keyword",
        RecallStrategy::Graph => "graph",
        RecallStrategy::Temporal => "temporal",
        RecallStrategy::Hybrid => "hybrid",
        _ => "auto",
    }
}

impl PyCustomMemory {
    async fn hook(
        &self,
        name: &'static str,
        args: impl FnOnce(Python<'_>) -> PyResult<Vec<Py<PyAny>>>,
    ) -> Result<Py<PyAny>, LaserError> {
        call_hook(&self.fallback, |call| {
            let py = call.py();
            let args = pyo3::types::PyTuple::new(py, args(py)?)?;
            call.call_method(self.hooks.bind(py), name, args)
        })
        .await
        .map_err(crate::errors::from_callback_error)
    }

    fn memory_id(value: Py<PyAny>, name: &str) -> Result<MemoryId, LaserError> {
        Python::attach(|py| value.bind(py).extract::<String>())
            .map_err(|error| {
                LaserError::HandlerConfig(format!(
                    "custom memory {name} must return an id string: {error}"
                ))
            })
            .and_then(|text| {
                MemoryId::from_str(&text).map_err(|error| {
                    LaserError::HandlerConfig(format!(
                        "custom memory {name} returned an invalid id: {error}"
                    ))
                })
            })
    }
}

impl Memory for PyCustomMemory {
    async fn append(
        &self,
        scope: &MemoryScope,
        id: MemoryId,
        kind: MemoryKind,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        let typed = Python::attach(|py| self.hooks.bind(py).hasattr("append"))
            .map_err(crate::errors::from_callback_error)?;
        if !typed {
            return self.remember(scope, payload).await;
        }
        let kind = match kind {
            MemoryKind::Fact => "fact",
            MemoryKind::Message => "message",
            MemoryKind::Summary => "summary",
            MemoryKind::Entity => "entity",
            MemoryKind::Feedback => "feedback",
            MemoryKind::Procedure => "procedure",
        };
        let value = self
            .hook("append", |py| {
                Ok(vec![
                    scope_dict(py, scope)?,
                    pyo3::types::PyString::new(py, &id.to_string())
                        .into_any()
                        .unbind(),
                    pyo3::types::PyString::new(py, kind).into_any().unbind(),
                    pyo3::types::PyBytes::new(py, &payload).into_any().unbind(),
                ])
            })
            .await?;
        Self::memory_id(value, "append")
    }

    async fn remember(
        &self,
        scope: &MemoryScope,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        let value = self
            .hook("remember", |py| {
                Ok(vec![
                    scope_dict(py, scope)?,
                    pyo3::types::PyBytes::new(py, &payload).into_any().unbind(),
                ])
            })
            .await?;
        Self::memory_id(value, "remember")
    }

    async fn recall(
        &self,
        scope: &MemoryScope,
        query: &MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        let value = self
            .hook("recall", |py| {
                let dict = pyo3::types::PyDict::new(py);
                dict.set_item("limit", query.limit)?;
                dict.set_item("token_budget", query.token_budget)?;
                dict.set_item("agent", query.agent.as_ref().map(ToString::to_string))?;
                dict.set_item("semantic", query.semantic.clone())?;
                dict.set_item("strategy", strategy_word(query.strategy))?;
                Ok(vec![scope_dict(py, scope)?, dict.into_any().unbind()])
            })
            .await?;
        Python::attach(|py| memory_items(value.bind(py), Some(scope))).map_err(|error| {
            LaserError::HandlerConfig(format!("custom memory recall must return items: {error}"))
        })
    }

    async fn improve(
        &self,
        scope: &MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        let value = self
            .hook("improve", |py| {
                let dict = pyo3::types::PyDict::new(py);
                dict.set_item("target", feedback.target.to_string())?;
                dict.set_item("weight", feedback.weight)?;
                dict.set_item("note", feedback.note.clone())?;
                Ok(vec![scope_dict(py, scope)?, dict.into_any().unbind()])
            })
            .await?;
        Self::memory_id(value, "improve")
    }

    async fn forget(&self, scope: &MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        self.hook("forget", |py| {
            Ok(vec![
                scope_dict(py, scope)?,
                id.to_string().into_pyobject(py)?.into_any().unbind(),
            ])
        })
        .await
        .map(|_| ())
    }
}

// The memory handle behind a `PyMemory`, shared so handles stay cheap to clone
// and the log backend's incremental recall cursor survives across calls (never
// rescanning the audit log from offset zero). An optional reranker reorders
// recall candidates when the query carries `semantic` text, exactly the
// second stage `MemoryHandle::reranker` composes in Rust.
#[derive(Clone)]
pub(crate) struct Backend {
    handle: Arc<MemoryHandle>,
    reranker: Option<PyReranker>,
}

// The scope and item options of one remember, mirroring `RememberBuilder`.
#[derive(Default)]
pub(crate) struct RememberOptions {
    pub(crate) agent: Option<AgentId>,
    pub(crate) conversation: Option<ConversationId>,
    pub(crate) stream: Option<String>,
    pub(crate) user: Option<String>,
    pub(crate) application: Option<String>,
    pub(crate) kind: MemoryKind,
    pub(crate) durable: bool,
    pub(crate) dedup: bool,
    pub(crate) origin: Option<laser_sdk::wire::graph::SourceRef>,
    pub(crate) producer: Option<laser_sdk::wire::graph::ProducerInfo>,
}

// The lineage keywords of a remember call: `origin` as the `SourceRef` dict
// and `producer` as a `{"name", "version"}` dict.
pub(crate) fn lineage(
    origin: Option<&Bound<'_, PyAny>>,
    producer: Option<&Bound<'_, PyAny>>,
) -> PyResult<(
    Option<laser_sdk::wire::graph::SourceRef>,
    Option<laser_sdk::wire::graph::ProducerInfo>,
)> {
    Ok((
        origin.map(crate::convert::py_to_de).transpose()?,
        producer.map(crate::convert::py_to_de).transpose()?,
    ))
}

impl Backend {
    pub(crate) fn new(handle: MemoryHandle) -> Self {
        Self {
            handle: Arc::new(handle),
            reranker: None,
        }
    }

    fn with_handle(&self, handle: MemoryHandle) -> Self {
        Self {
            handle: Arc::new(handle),
            reranker: self.reranker.clone(),
        }
    }

    fn with_reranker(&self, reranker: PyReranker) -> Self {
        Self {
            handle: self.handle.clone(),
            reranker: Some(reranker),
        }
    }

    async fn reranked(
        &self,
        query: &MemoryQuery,
        candidates: Vec<MemoryItem>,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        match (&self.reranker, &query.semantic) {
            (Some(reranker), Some(text)) => reranker.rerank(text, candidates).await,
            _ => Ok(candidates),
        }
    }

    pub(crate) async fn remember(
        &self,
        options: RememberOptions,
        payload: Vec<u8>,
    ) -> Result<MemoryId, LaserError> {
        let mut builder = self.handle.remember(payload).kind(options.kind);
        if let Some(agent) = options.agent {
            builder = builder.agent(agent);
        }
        if let Some(conversation) = options.conversation {
            builder = builder.scope(conversation);
        }
        if let Some(stream) = options.stream {
            builder = builder.stream(stream);
        }
        if let Some(user) = options.user {
            builder = builder.user(user);
        }
        if let Some(application) = options.application {
            builder = builder.application(application);
        }
        if options.durable {
            builder = builder.durable();
        }
        if options.dedup {
            builder = builder.dedup();
        }
        if let Some(origin) = options.origin {
            builder = builder.origin(origin);
        }
        if let Some(producer) = options.producer {
            builder = builder.producer(producer);
        }
        builder.send().await
    }

    pub(crate) async fn recall(
        &self,
        scope: MemoryScope,
        query: MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        let candidates = Memory::recall(self.handle.as_ref(), &scope, &query).await?;
        self.reranked(&query, candidates).await
    }

    // Recall by folding the topic in process (the opt-in). The log backend folds,
    // the vector backend is already in process, so it recalls as usual.
    pub(crate) async fn recall_folded(
        &self,
        scope: MemoryScope,
        query: MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        let candidates = self.handle.recall_folded(&scope, &query).await?;
        self.reranked(&query, candidates).await
    }

    pub(crate) async fn forget(&self, scope: MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        self.handle.forget(&scope, id).await
    }

    pub(crate) async fn improve(
        &self,
        scope: MemoryScope,
        feedback: Feedback,
    ) -> Result<MemoryId, LaserError> {
        self.handle.improve(&scope, feedback).await
    }

    pub(crate) async fn context(
        &self,
        conversation: ConversationId,
        token_budget: Option<usize>,
    ) -> Result<String, LaserError> {
        self.handle.context(conversation, token_budget).await
    }

    pub(crate) async fn consolidate(
        &self,
        scope: MemoryScope,
        max_items: usize,
        summarizer: Option<PySummarizer>,
        prune_summarized: bool,
    ) -> Result<ConsolidationReport, LaserError> {
        let mut consolidator = DefaultConsolidator::new(self.handle(), max_items);
        if prune_summarized {
            consolidator = consolidator.prune_summarized();
        }
        match summarizer {
            Some(summarizer) => {
                consolidator
                    .with_summarizer(summarizer)
                    .consolidate(&scope)
                    .await
            }
            None => consolidator.consolidate(&scope).await,
        }
    }

    fn handle(&self) -> &MemoryHandle {
        &self.handle
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

// Map a kind word to `MemoryKind`, refusing an unknown word instead of the
// read path's degrade-to-`Fact`, so a typo never stores the wrong kind.
pub(crate) fn map_kind(kind: &str) -> PyResult<MemoryKind> {
    Ok(match kind {
        "fact" => MemoryKind::Fact,
        "message" => MemoryKind::Message,
        "summary" => MemoryKind::Summary,
        "entity" => MemoryKind::Entity,
        "feedback" => MemoryKind::Feedback,
        "procedure" => MemoryKind::Procedure,
        other => {
            return Err(crate::errors::InvalidError::new_err(format!(
                "unknown memory kind '{other}' (expected fact, message, summary, entity, feedback, or procedure)"
            )));
        }
    })
}

pub(crate) fn parse_agent(agent: Option<String>) -> PyResult<Option<AgentId>> {
    agent
        .map(|value| AgentId::new(value).map_err(|e| to_pyerr(e.into())))
        .transpose()
}

pub(crate) fn parse_conversation(conversation: Option<String>) -> PyResult<Option<ConversationId>> {
    conversation
        .map(|value| ConversationId::from_str(&value).map_err(|e| to_pyerr(e.into())))
        .transpose()
}

pub(crate) fn build_full_scope(
    agent: Option<String>,
    conversation: Option<String>,
    user: Option<String>,
    application: Option<String>,
    stream: Option<String>,
    durable: bool,
) -> PyResult<MemoryScope> {
    Ok(MemoryScope::builder()
        .maybe_agent(parse_agent(agent)?)
        .maybe_conversation(parse_conversation(conversation)?)
        .maybe_user(user)
        .maybe_app(application)
        .maybe_stream(stream)
        .lifetime(if durable {
            laser_sdk::memory::Lifetime::Durable
        } else {
            laser_sdk::memory::Lifetime::Session
        })
        .build())
}

/// What one consolidation pass changed. All counts are best effort and
/// advisory.
#[gen_stub_pyclass]
#[pyclass(name = "ConsolidationReport", frozen)]
pub struct PyConsolidationReport {
    inner: ConsolidationReport,
}

impl From<ConsolidationReport> for PyConsolidationReport {
    fn from(inner: ConsolidationReport) -> Self {
        Self { inner }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyConsolidationReport {
    /// Items folded into durable summaries.
    #[getter]
    fn summarized(&self) -> usize {
        self.inner.summarized
    }

    /// Items whose recall weight was adjusted from feedback.
    #[getter]
    fn reweighted(&self) -> usize {
        self.inner.reweighted
    }

    /// Stale items pruned.
    #[getter]
    fn pruned(&self) -> usize {
        self.inner.pruned
    }

    /// New facts or edges derived.
    #[getter]
    fn derived(&self) -> usize {
        self.inner.derived
    }

    fn __repr__(&self) -> String {
        format!(
            "ConsolidationReport(summarized={}, reweighted={}, pruned={}, derived={})",
            self.inner.summarized, self.inner.reweighted, self.inner.pruned, self.inner.derived
        )
    }
}

/// Agent memory: `remember` a payload, `recall` the most relevant items, and
/// `forget` one by id. The backend decides how recall ranks: the vector backend
/// scores by semantic similarity to a query, the durable log backend reads the
/// managed key-value view (or folds the topic with `folded=True`). Scope every
/// call to an agent, a conversation, a user, or an application. `LogMemory`,
/// `VectorMemory`, and `RerankedMemory` are handles over one backend each, and
/// `Laser.memory` builds the durable one.
#[gen_stub_pyclass]
#[pyclass(name = "MemoryHandle", frozen, subclass)]
pub struct PyMemory {
    inner: Backend,
}

impl PyMemory {
    pub(crate) fn backend(&self) -> Backend {
        self.inner.clone()
    }

    pub(crate) fn from_handle(handle: MemoryHandle) -> Self {
        Self::from_backend(Backend::new(handle))
    }

    pub(crate) fn from_backend(inner: Backend) -> Self {
        Self { inner }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMemory {
    /// Standalone in-process semantic memory, with no connection and no
    /// governance. The embedder is a callable or an object with `embed(text)`
    /// and returns a `list[float]` directly or through an awaitable. An
    /// asynchronous embedder runs on the caller's event loop. A synchronous one
    /// runs on an SDK worker thread with the caller's context variables, so it
    /// must not block or use the event loop. For a governed handle use
    /// `VectorMemory.governed(laser, embedder)`.
    #[staticmethod]
    fn vector(embedder: &Bound<'_, PyAny>) -> PyResult<PyMemory> {
        Ok(PyMemory::from_handle(MemoryHandle::vector(
            PyEmbedder::new(embedder)?,
        )))
    }

    /// This handle with a rerank second stage: `reranker(query, items)`, or an
    /// object with `rerank(query, items)`, returns the candidates reordered
    /// (and may drop or rescore them), directly or through an awaitable.
    /// Recall runs the backend's retrieval, then the reranker when the query
    /// carries `semantic` text. Writes are unchanged.
    fn reranker(&self, reranker: &Bound<'_, PyAny>) -> PyResult<PyMemory> {
        Ok(PyMemory::from_backend(self.inner.with_reranker(
            PyReranker {
                hook: PyHook::new(reranker, "rerank", "a reranker")?,
            },
        )))
    }

    /// A handle over a custom backend: an object with `remember(scope,
    /// payload) -> id`, `recall(scope, query) -> items`, `improve(scope,
    /// feedback) -> id`, and `forget(scope, id)`, as `Laser.memory_custom`
    /// takes it. It needs no connection.
    #[staticmethod]
    fn custom(backend: &Bound<'_, PyAny>) -> PyMemory {
        PyMemory::from_handle(MemoryHandle::Custom(custom_backend(backend)))
    }

    /// The backend this handle resolved to: `log`, `vector`, or `custom`.
    #[getter(backend)]
    fn backend_kind(&self) -> String {
        self.inner.handle().backend().to_owned()
    }

    /// This handle with `embedder` as its embedding seam. A vector handle
    /// gets a fresh index over the new embedder and keeps its governance. A
    /// log or custom handle recalls without embeddings, so it is unchanged.
    fn embedder(&self, embedder: &Bound<'_, PyAny>) -> PyResult<PyMemory> {
        let embedder = PyEmbedder::new(embedder)?;
        Ok(match self.inner.handle() {
            MemoryHandle::Vector(memory) => PyMemory::from_backend(
                self.inner
                    .with_handle(MemoryHandle::Vector(memory.clone()).embedder(embedder)),
            ),
            _ => PyMemory::from_backend(self.inner.clone()),
        })
    }

    /// The simple altitude: write named point state under your own `key` (the
    /// working-note shape). Every write is a durable event on the memory topic,
    /// like the rest of memory. The vector handle raises `UnsupportedError`.
    fn set<'py>(
        &self,
        py: Python<'py>,
        key: String,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let payload = payload_bytes(payload)?;
        future_into_py(py, async move {
            backend.handle().set(&key, payload).await.map_err(to_pyerr)
        })
    }

    /// Point-read the named item written by `set`, or `None`. Reads the managed
    /// key-value view.
    fn fetch<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        future_into_py(py, async move {
            backend.handle().fetch(&key).await.map_err(to_pyerr)
        })
    }

    /// Point-read the named item by folding the topic in process, the opt-in for
    /// a deployment with no managed read view. Prefer `fetch`, which reads it.
    fn fetch_folded<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        future_into_py(py, async move {
            backend.handle().fetch_folded(&key).await.map_err(to_pyerr)
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
        let backend = self.inner.clone();
        let patch = payload_bytes(patch)?;
        future_into_py(py, async move {
            backend.handle().update(&key, patch).await.map_err(to_pyerr)
        })
    }

    /// Delete the named item. Idempotent: removing an absent key is fine.
    fn remove<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        future_into_py(py, async move {
            backend.handle().remove(&key).await.map_err(to_pyerr)
        })
    }

    /// Remember `payload` (`str`, `bytes`, or `bytearray`). Returns the new
    /// item's id (a ULID string). `kind` is `fact` (default), `message`,
    /// `summary`, `entity`, `feedback`, or `procedure`. `durable=True` sets the
    /// scope lifetime for custom backends. Built-ins do not persist or filter
    /// that value. Omit `conversation` on recall to read across conversations.
    /// `dedup=True` derives the id from the owner scope (stream, agent, user, and
    /// application), the kind, and the body, so remembering the same body twice
    /// stores it once. `origin` is the `SourceRef` dict of the session record
    /// that motivated the item, and `producer` is a `{"name", "version"}` dict.
    #[pyo3(signature = (payload, *, agent=None, conversation=None, stream=None, user=None, application=None, kind="fact", durable=false, dedup=false, origin=None, producer=None))]
    #[allow(clippy::too_many_arguments)]
    fn remember<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'_, PyAny>,
        agent: Option<String>,
        conversation: Option<String>,
        stream: Option<String>,
        user: Option<String>,
        application: Option<String>,
        kind: &str,
        durable: bool,
        dedup: bool,
        origin: Option<&Bound<'_, PyAny>>,
        producer: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let (origin, producer) = lineage(origin, producer)?;
        let options = RememberOptions {
            origin,
            producer,
            agent: parse_agent(agent)?,
            conversation: parse_conversation(conversation)?,
            stream,
            user,
            application,
            kind: map_kind(kind)?,
            durable,
            dedup,
        };
        let payload = payload_bytes(payload)?;
        future_into_py(py, async move {
            let id = backend.remember(options, payload).await.map_err(to_pyerr)?;
            Ok(id.to_string())
        })
    }

    /// Append with an explicit ID and kind. Built-ins preserve both. A custom backend uses its optional `append` callback, or falls back to `remember` when absent.
    #[pyo3(signature = (id, payload, *, kind="fact", agent=None, conversation=None, user=None, application=None, stream=None, durable=false))]
    #[allow(clippy::too_many_arguments)]
    fn append<'py>(
        &self,
        py: Python<'py>,
        id: String,
        payload: &Bound<'_, PyAny>,
        kind: &str,
        agent: Option<String>,
        conversation: Option<String>,
        user: Option<String>,
        application: Option<String>,
        stream: Option<String>,
        durable: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let id = MemoryId::from_str(&id).map_err(|error| to_pyerr(error.into()))?;
        let kind = map_kind(kind)?;
        let payload = payload_bytes(payload)?;
        let scope = build_full_scope(agent, conversation, user, application, stream, durable)?;
        future_into_py(py, async move {
            let stored = Memory::append(backend.handle(), &scope, id, kind, payload)
                .await
                .map_err(to_pyerr)?;
            Ok(stored.to_string())
        })
    }

    /// Recall up to `limit` items under the scope. Pass `semantic` text to rank
    /// by similarity, or a `strategy` (`auto`, `recent`, `semantic`, `keyword`,
    /// `graph`, `temporal`, `hybrid`) with the query text in `semantic`. The
    /// log backend reads the managed key-value view by default. `folded=True`
    /// rebuilds memory from the topic in process instead. `block=True` renders
    /// the recalled items into one prompt-ready string under `token_budget`,
    /// like `to_context_block`.
    #[pyo3(signature = (*, limit=50, agent=None, conversation=None, user=None, application=None, semantic=None, strategy=None, folded=false, stream=None, durable=false, token_budget=None, block=false))]
    #[allow(clippy::too_many_arguments)]
    fn recall<'py>(
        &self,
        py: Python<'py>,
        limit: usize,
        agent: Option<String>,
        conversation: Option<String>,
        user: Option<String>,
        application: Option<String>,
        semantic: Option<String>,
        strategy: Option<String>,
        folded: bool,
        stream: Option<String>,
        durable: bool,
        token_budget: Option<usize>,
        block: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let scope = build_full_scope(agent, conversation, user, application, stream, durable)?;
        let query_budget = if block { None } else { token_budget };
        let query = recall_query(&scope, limit, semantic, strategy, query_budget)?;
        future_into_py(py, async move {
            let items = if folded {
                backend.recall_folded(scope, query).await
            } else {
                backend.recall(scope, query).await
            }
            .map_err(to_pyerr)?;
            Python::attach(|py| {
                if block {
                    let text = laser_sdk::memory::to_context_block(&items, token_budget);
                    return Ok(text.into_pyobject(py)?.into_any().unbind());
                }
                Ok(items
                    .into_iter()
                    .map(PyMemoryItem::from)
                    .collect::<Vec<_>>()
                    .into_pyobject(py)?
                    .into_any()
                    .unbind())
            })
        })
    }

    /// The one-call context altitude: recall `conversation`'s most relevant
    /// items and render them as one prompt-ready block under an optional
    /// `token_budget` (estimated at about 4 bytes per token). At the budget the
    /// remaining items are dropped and an omission marker is appended.
    #[pyo3(signature = (conversation, *, token_budget=None))]
    fn context<'py>(
        &self,
        py: Python<'py>,
        conversation: String,
        token_budget: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let conversation =
            ConversationId::from_str(&conversation).map_err(|e| to_pyerr(e.into()))?;
        future_into_py(py, async move {
            backend
                .context(conversation, token_budget)
                .await
                .map_err(to_pyerr)
        })
    }

    /// One consolidation pass over at most 10,000 scoped items. Keeps the newest `max_items` and prunes the rest.
    /// `summarizer(bodies: list[bytes])`, or an object with `summarize(bodies)`, returns a body directly or through an awaitable. The pass folds `message` items into one `summary` with a durable scope and the same conversation.
    /// `prune_summarized` removes the included messages. Built-in recall ignores lifetime. Custom callbacks receive it.
    #[pyo3(signature = (max_items, *, agent=None, conversation=None, user=None, application=None, summarizer=None, prune_summarized=false, stream=None, durable=false))]
    #[allow(clippy::too_many_arguments)]
    fn consolidate<'py>(
        &self,
        py: Python<'py>,
        max_items: usize,
        agent: Option<String>,
        conversation: Option<String>,
        user: Option<String>,
        application: Option<String>,
        summarizer: Option<Py<PyAny>>,
        prune_summarized: bool,
        stream: Option<String>,
        durable: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let scope = build_full_scope(agent, conversation, user, application, stream, durable)?;
        let summarizer = summarizer.map(|callback| PySummarizer { callback });
        future_into_py(py, async move {
            let report = backend
                .consolidate(scope, max_items, summarizer, prune_summarized)
                .await
                .map_err(to_pyerr)?;
            Ok(PyConsolidationReport::from(report))
        })
    }

    /// Record feedback on the recalled item `target`, the signal a ranking
    /// backend folds into future recall. `weight` is positive to promote,
    /// negative to demote.
    /// Returns the feedback record's id.
    #[pyo3(signature = (target, weight, *, agent=None, conversation=None, user=None, application=None, stream=None, durable=false, note=None))]
    #[allow(clippy::too_many_arguments)]
    fn improve<'py>(
        &self,
        py: Python<'py>,
        target: String,
        weight: f32,
        agent: Option<String>,
        conversation: Option<String>,
        user: Option<String>,
        application: Option<String>,
        stream: Option<String>,
        durable: bool,
        note: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let scope = build_full_scope(agent, conversation, user, application, stream, durable)?;
        let target = MemoryId::from_str(&target).map_err(|e| to_pyerr(e.into()))?;
        future_into_py(py, async move {
            let id = backend
                .improve(
                    scope,
                    Feedback {
                        target,
                        weight,
                        note,
                    },
                )
                .await
                .map_err(to_pyerr)?;
            Ok(id.to_string())
        })
    }

    /// Forget the item with id `id` (a ULID string) under the scope.
    #[pyo3(signature = (id, *, agent=None, conversation=None, user=None, application=None, stream=None, durable=false))]
    #[allow(clippy::too_many_arguments)]
    fn forget<'py>(
        &self,
        py: Python<'py>,
        id: String,
        agent: Option<String>,
        conversation: Option<String>,
        user: Option<String>,
        application: Option<String>,
        stream: Option<String>,
        durable: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let scope = build_full_scope(agent, conversation, user, application, stream, durable)?;
        let id = MemoryId::from_str(&id).map_err(|e| to_pyerr(e.into()))?;
        future_into_py(py, async move {
            backend.forget(scope, id).await.map_err(to_pyerr)
        })
    }
}

// The recall query behind every Python recall: an explicit strategy wins,
// otherwise a `semantic` query implies the semantic strategy, and a plain recall
// stays `Auto`. The scope's agent narrows the query, as the Rust builder does.
pub(crate) fn recall_query(
    scope: &MemoryScope,
    limit: usize,
    semantic: Option<String>,
    strategy: Option<String>,
    token_budget: Option<usize>,
) -> PyResult<MemoryQuery> {
    let strategy = match strategy {
        Some(name) => map_strategy(&name)?,
        None if semantic.is_some() => RecallStrategy::Semantic,
        None => RecallStrategy::Auto,
    };
    Ok(MemoryQuery::builder()
        .limit(limit)
        .maybe_token_budget(token_budget)
        .maybe_semantic(semantic)
        .strategy(strategy)
        .maybe_agent(scope.agent.clone())
        .build())
}

/// One remembered item: its id, the raw payload bytes, and the provenance it was
/// stored under (its scope's agent and conversation).
#[gen_stub_pyclass]
#[pyclass(name = "MemoryItem", frozen)]
pub struct PyMemoryItem {
    pub(crate) inner: MemoryItem,
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

    /// The payload as UTF-8, with invalid bytes replaced.
    fn text(&self) -> String {
        self.inner.text()
    }

    #[getter]
    fn provenance(&self) -> PyProvenance {
        PyProvenance {
            inner: self.inner.provenance.clone(),
        }
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

    /// The session record that motivated the item, as the `SourceRef` dict, or None.
    #[getter]
    fn origin(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.inner
            .origin
            .as_ref()
            .map(|origin| crate::convert::ser_to_py(py, origin))
            .transpose()
    }

    /// The component that produced the item, as `{"name", "version"}`, or None.
    #[getter]
    fn producer(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.inner
            .producer
            .as_ref()
            .map(|producer| crate::convert::ser_to_py(py, producer))
            .transpose()
    }

    /// The origin log record this item was folded from, as
    /// `(stream, topic, partition, offset, generation, conversation)`, or `None`. Points back
    /// to the source message while it is still on the log.
    #[getter]
    fn source(&self) -> Option<MessageSource> {
        match &self.inner.source {
            Some(laser_sdk::wire::graph::SourceRef::Message {
                stream,
                topic,
                partition,
                offset,
                generation,
                conversation,
            }) => Some((
                *stream,
                *topic,
                *partition,
                *offset,
                *generation,
                conversation.clone(),
            )),
            _ => None,
        }
    }

    /// Which signals produced this candidate, the per-signal attribution a
    /// fused recall keeps (empty when unranked).
    #[getter]
    fn signals(&self) -> Vec<PyRecallSignal> {
        self.inner
            .signals
            .iter()
            .cloned()
            .map(|inner| PyRecallSignal { inner })
            .collect()
    }

    /// Decode the payload as JSON into a Python value, raising `CodecError`
    /// when it is not JSON.
    fn json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value: serde_json::Value = self.inner.json().map_err(to_pyerr)?;
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
    /// Agent memory in `namespace`: `remember` publishes to the memory topic
    /// (the durable audit), `forget` appends a tombstone, and a deployment
    /// materializes the topic into the versioned key-value read view. Default
    /// `recall` and `fetch` read that managed view, so they need Laser Stack or
    /// LaserData Cloud and raise `UnsupportedError` otherwise. `recall(folded=True)`
    /// and `fetch_folded` rebuild memory from the topic in process and work on
    /// plain Apache Iggy. `namespace` prefixes the named-item keys (`set`,
    /// `fetch`). For an isolated per-topic memory stream use `memory_on_topic`
    /// or `memory_topic`. Reuse the handle: each folded recall reads only what
    /// is new.
    fn memory(&self, namespace: String) -> PyMemory {
        PyMemory::from_handle(self.inner.memory(namespace))
    }

    /// Memory in `namespace` on an explicit backend: `auto` and `log` are the
    /// durable stream model `memory` uses, `vector` is the governed in-process
    /// similarity index and needs `embedder` (a callable returning
    /// `list[float]`, directly or through an awaitable).
    #[pyo3(signature = (namespace, backend="auto", *, embedder=None))]
    fn memory_with(
        &self,
        namespace: String,
        backend: &str,
        embedder: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyMemory> {
        let kind = match backend {
            "auto" => MemoryBackend::Auto,
            "log" => MemoryBackend::Log,
            "vector" => MemoryBackend::Vector,
            other => {
                return Err(crate::errors::InvalidError::new_err(format!(
                    "unknown memory backend '{other}' (expected auto, log, or vector)"
                )));
            }
        };
        let mut handle = self.inner.memory_with(namespace, kind);
        match (kind, embedder) {
            (MemoryBackend::Vector, Some(embedder)) => {
                handle = handle.embedder(PyEmbedder::new(embedder)?);
            }
            (MemoryBackend::Vector, None) => {
                return Err(crate::errors::InvalidError::new_err(
                    "the vector backend needs an embedder",
                ));
            }
            (_, Some(_)) => {
                return Err(crate::errors::InvalidError::new_err(
                    "an embedder applies to the vector backend only",
                ));
            }
            (_, None) => {}
        }
        Ok(PyMemory::from_handle(handle))
    }

    /// Agent memory on a caller-named topic (and optional stream), so a
    /// deployment can keep several isolated memory streams. Ensure the topic up
    /// front like any other, for example `await laser.topic(name).ensure(...)`.
    #[pyo3(signature = (topic, *, stream=None))]
    fn memory_on_topic(&self, topic: String, stream: Option<String>) -> PyResult<PyMemory> {
        let memory = LogMemory::on_stream_topic_named(self.inner.clone(), stream, &topic)
            .map_err(to_pyerr)?;
        Ok(PyMemory::from_handle(MemoryHandle::Log(memory)))
    }

    /// Configure and ensure a memory topic, then return memory on it: the
    /// stream, the partition count (each scope keyed to one partition), and the
    /// stream message-expiry. `ttl_ms` (milliseconds) defaults to thirty days. Pass `0` to
    /// keep the history until topic retention rotates it out.
    #[pyo3(signature = (topic, *, stream=None, partitions=1, ttl_ms=None))]
    fn memory_topic<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        stream: Option<String>,
        partitions: u32,
        ttl_ms: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let mut builder = laser.memory_topic(&topic).partitions(partitions);
            if let Some(stream) = &stream {
                builder = builder.stream(stream);
            }
            builder = match ttl_ms {
                None => builder,
                Some(ms) if ms <= 0.0 => builder.no_expiry(),
                Some(ms) => builder.ttl(duration_ms(ms, "ttl_ms")?),
            };
            builder.build().await.map_err(to_pyerr)?;
            let memory = LogMemory::on_stream_topic_named(laser.clone(), stream, &topic)
                .map_err(to_pyerr)?;
            Ok(PyMemory::from_handle(MemoryHandle::Log(memory)))
        })
    }

    /// Memory on your own durable backend: `backend` is an object with
    /// `remember(scope, payload) -> id`, `recall(scope, query) -> items`,
    /// `improve(scope, feedback) -> id`, and `forget(scope, id)`, each returning
    /// directly or through an awaitable. Scopes, queries, and feedback arrive as
    /// dicts. `recall` returns `MemoryItem` objects or dicts with `payload` and
    /// optional `id`, `kind`, `score`, `conversation`. The backend owns its own
    /// scoping and id minting. Named items (`set`, `fetch`) raise
    /// `UnsupportedError` on it. Synchronous methods run on an SDK worker
    /// thread with the caller's context variables, so they must not block.
    fn memory_custom(&self, backend: &Bound<'_, PyAny>) -> PyMemory {
        PyMemory::from_handle(self.inner.memory_custom(custom_backend(backend)))
    }
}

/// The durable stream memory: every write publishes to the memory topic, and
/// a deployment materializes it into the versioned key-value read view.
/// `LogMemory(laser)` rides the default memory topic. Reuse one instance so a
/// folded recall reads only what is new.
#[gen_stub_pyclass]
#[pyclass(name = "LogMemory", extends = PyMemory, frozen)]
pub struct PyLogMemory;

#[gen_stub_pymethods]
#[pymethods]
impl PyLogMemory {
    /// Log memory on the default memory topic and namespace.
    #[new]
    fn new(laser: PyRef<'_, PyLaser>) -> PyClassInitializer<PyLogMemory> {
        log_initializer(LogMemory::new(laser.inner.clone()))
    }

    /// Log memory on the default memory topic with `namespace` as the prefix
    /// of the named-item keys. `Laser.memory(namespace)` builds the same.
    #[staticmethod]
    fn in_namespace(
        py: Python<'_>,
        laser: PyRef<'_, PyLaser>,
        namespace: String,
    ) -> PyResult<Py<PyLogMemory>> {
        Py::new(
            py,
            log_initializer(LogMemory::in_namespace(laser.inner.clone(), namespace)),
        )
    }

    /// Log memory on the topic named `topic`, which is also the namespace.
    #[staticmethod]
    fn on_topic(
        py: Python<'_>,
        laser: PyRef<'_, PyLaser>,
        topic: String,
    ) -> PyResult<Py<PyLogMemory>> {
        let memory = LogMemory::on_topic_named(laser.inner.clone(), &topic).map_err(to_pyerr)?;
        Py::new(py, log_initializer(memory))
    }

    /// Log memory on the topic named `topic` in `stream`, or in the default
    /// stream when `stream` is `None`.
    #[staticmethod]
    fn on_stream_topic(
        py: Python<'_>,
        laser: PyRef<'_, PyLaser>,
        stream: Option<String>,
        topic: String,
    ) -> PyResult<Py<PyLogMemory>> {
        let memory = LogMemory::on_stream_topic_named(laser.inner.clone(), stream, &topic)
            .map_err(to_pyerr)?;
        Py::new(py, log_initializer(memory))
    }

    /// Write the named item `key` with `body`, the low-level form of `set`.
    fn set_named<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        key: String,
        body: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = slf.as_super().inner.handle.clone();
        let body = payload_bytes(body)?;
        future_into_py(py, async move {
            log_memory(&handle)?
                .set_named(&key, body)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Merge-patch the named item `key`, the low-level form of `update`.
    fn update_named<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        key: String,
        patch: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = slf.as_super().inner.handle.clone();
        let patch = payload_bytes(patch)?;
        future_into_py(py, async move {
            log_memory(&handle)?
                .update_named(&key, patch)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Delete the named item `key`, the low-level form of `remove`.
    fn forget_named<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = slf.as_super().inner.handle.clone();
        future_into_py(py, async move {
            log_memory(&handle)?
                .forget_named(&key)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Point-read the named item `key` from the managed key-value view, the
    /// low-level form of `fetch`.
    fn fetch_named<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = slf.as_super().inner.handle.clone();
        future_into_py(py, async move {
            log_memory(&handle)?
                .fetch_named(&key)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Point-read the named item `key` by folding the topic in process, the
    /// low-level form of `fetch_folded`.
    fn fetch_named_folded<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        key: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = slf.as_super().inner.handle.clone();
        future_into_py(py, async move {
            log_memory(&handle)?
                .fetch_named_folded(&key)
                .await
                .map_err(to_pyerr)
        })
    }
}

/// In-process semantic memory over an embedder: a callable or an object with
/// `embed(text)` returning `list[float]` directly or through an awaitable.
/// `VectorMemory(embedder)` has no connection and no governance.
/// `VectorMemory.governed(laser, embedder)` applies the connection's governor.
#[gen_stub_pyclass]
#[pyclass(name = "VectorMemory", extends = PyMemory, frozen)]
pub struct PyVectorMemory;

#[gen_stub_pymethods]
#[pymethods]
impl PyVectorMemory {
    /// Standalone vector memory with no connection and no governance.
    #[new]
    fn new(embedder: &Bound<'_, PyAny>) -> PyResult<PyClassInitializer<PyVectorMemory>> {
        Ok(
            PyClassInitializer::from(PyMemory::from_handle(MemoryHandle::vector(
                PyEmbedder::new(embedder)?,
            )))
            .add_subclass(PyVectorMemory),
        )
    }

    /// Vector memory governed by `laser`'s governor, the same as
    /// `laser.memory_with(namespace, "vector", embedder=embedder)`.
    #[staticmethod]
    fn governed(
        py: Python<'_>,
        laser: PyRef<'_, PyLaser>,
        embedder: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyVectorMemory>> {
        let handle = laser
            .inner
            .memory_with("vector", MemoryBackend::Vector)
            .embedder(PyEmbedder::new(embedder)?);
        Py::new(
            py,
            PyClassInitializer::from(PyMemory::from_handle(handle)).add_subclass(PyVectorMemory),
        )
    }
}

/// `inner` with a rerank second stage: the reranker is a callable
/// `(query, items) -> items` or an object with `rerank(query, items)`,
/// returning directly or through an awaitable. Recall runs the inner
/// retrieval, then the reranker when the query carries `semantic` text.
/// Writes are unchanged. `inner.reranker(reranker)` builds the same.
#[gen_stub_pyclass]
#[pyclass(name = "RerankedMemory", extends = PyMemory, frozen)]
pub struct PyRerankedMemory;

#[gen_stub_pymethods]
#[pymethods]
impl PyRerankedMemory {
    #[new]
    fn new(
        inner: PyRef<'_, PyMemory>,
        reranker: &Bound<'_, PyAny>,
    ) -> PyResult<PyClassInitializer<PyRerankedMemory>> {
        let reranker = PyReranker {
            hook: PyHook::new(reranker, "rerank", "a reranker")?,
        };
        Ok(
            PyClassInitializer::from(PyMemory::from_backend(inner.inner.with_reranker(reranker)))
                .add_subclass(PyRerankedMemory),
        )
    }
}

/// One signal's contribution to a recalled item: the strategy that surfaced
/// it, its rank within that signal (0 is best), and that signal's own score
/// before fusion.
#[gen_stub_pyclass]
#[pyclass(name = "RecallSignal", frozen, eq)]
#[derive(PartialEq)]
pub struct PyRecallSignal {
    inner: RecallSignal,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRecallSignal {
    /// The strategy that produced the candidate: `auto`, `recent`, `semantic`,
    /// `keyword`, `graph`, `temporal`, or `hybrid`.
    #[getter]
    fn strategy(&self) -> &'static str {
        strategy_word(self.inner.strategy)
    }

    /// The candidate's rank within this signal, 0 is best.
    #[getter]
    fn rank(&self) -> usize {
        self.inner.rank
    }

    /// The signal's own score before fusion, or `None`.
    #[getter]
    fn score(&self) -> Option<f32> {
        self.inner.score
    }

    fn __repr__(&self) -> String {
        format!(
            "RecallSignal(strategy={}, rank={}, score={:?})",
            strategy_word(self.inner.strategy),
            self.inner.rank,
            self.inner.score
        )
    }
}

/// Render these recalled items into one prompt-ready block, in order, filling
/// up to `token_budget` (estimated at about 4 bytes per token). At the budget
/// the remaining items are dropped and an omission marker is appended.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (items, token_budget=None))]
pub fn to_context_block(
    items: Vec<PyRef<'_, PyMemoryItem>>,
    token_budget: Option<usize>,
) -> String {
    let items = items
        .into_iter()
        .map(|item| item.inner.clone())
        .collect::<Vec<_>>();
    laser_sdk::memory::to_context_block(&items, token_budget)
}

/// The content-derived memory id of `body` stored as `kind` under the owner
/// `stream`, `agent`, `user`, and `application`. The same inputs always give
/// the same id, in every SDK.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (kind, body, *, stream=None, agent=None, user=None, application=None))]
pub fn memory_id_content(
    kind: &str,
    body: &Bound<'_, PyAny>,
    stream: Option<String>,
    agent: Option<String>,
    user: Option<String>,
    application: Option<String>,
) -> PyResult<String> {
    let owner = build_full_scope(agent, None, user, application, stream, false)?;
    Ok(MemoryId::content(&owner, map_kind(kind)?, &payload_bytes(body)?).to_string())
}

/// The memory class of `kind`: `episodic`, `semantic`, or `procedural`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn memory_kind_class(kind: &str) -> PyResult<String> {
    match serde_json::to_value(map_kind(kind)?.class()) {
        Ok(serde_json::Value::String(class)) => Ok(class),
        _ => Err(crate::errors::CodecError::new_err(
            "memory class is not a word",
        )),
    }
}

/// The stable one-byte code of `kind`, the discriminator a content-derived
/// memory id mixes in.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn memory_kind_code(kind: &str) -> PyResult<u8> {
    Ok(map_kind(kind)?.code())
}

fn custom_backend(backend: &Bound<'_, PyAny>) -> Arc<dyn laser_sdk::memory::DynMemory> {
    Arc::new(PyCustomMemory {
        hooks: Arc::new(backend.clone().unbind()),
        fallback: HookLoop::capture(backend.py()),
    })
}

fn log_initializer(memory: LogMemory) -> PyClassInitializer<PyLogMemory> {
    PyClassInitializer::from(PyMemory::from_handle(MemoryHandle::Log(memory)))
        .add_subclass(PyLogMemory)
}

// The log memory behind a `LogMemory` handle, which is always the log variant.
fn log_memory(handle: &MemoryHandle) -> PyResult<&LogMemory> {
    match handle {
        MemoryHandle::Log(memory) => Ok(memory),
        other => Err(crate::errors::UnsupportedError::new_err(format!(
            "the named-item verbs need the log backend, not `{}`",
            other.backend()
        ))),
    }
}

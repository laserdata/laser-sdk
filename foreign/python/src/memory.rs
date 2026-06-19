use crate::agent::PyProvenance;
use crate::client::PyLaser;
use crate::convert::{json_to_py, payload_bytes};
use crate::errors::to_pyerr;
use laser_sdk::error::LaserError;
use laser_sdk::laser::Laser;
use laser_sdk::memory::{
    Embedder, KvMemory, LogMemory, Memory, MemoryId, MemoryItem, MemoryQuery, MemoryScope,
    QueryMemory, VectorMemory,
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
struct PyEmbedder {
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

// The concrete backend behind a `PyMemory`, cheap to clone. The log and vector
// backends are held long-lived (the log backend keeps its incremental recall
// cursor across calls, never rescanning the audit log from offset zero). The
// query and key-value backends borrow the connection, so they are rebuilt per
// call: their truth lives server-side, so there is no client state to keep.
#[derive(Clone)]
enum Backend {
    Log(Arc<LogMemory>),
    Vector(Arc<VectorMemory<PyEmbedder>>),
    Query {
        laser: Laser,
        embedder: PyEmbedder,
        topic: String,
    },
    Kv {
        laser: Laser,
        namespace: String,
        ttl: Option<Duration>,
    },
}

// Resolve a `Backend` to a concrete `&impl Memory` and run `$op` against it,
// so the three operations share one dispatch instead of repeating the match.
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
            Backend::Query {
                laser,
                embedder,
                topic,
            } => {
                let built = QueryMemory::new(laser, embedder.clone(), topic.clone());
                let $memory = &built;
                $op
            }
            Backend::Kv {
                laser,
                namespace,
                ttl,
            } => {
                let mut built = KvMemory::new(laser, namespace.clone());
                if let Some(ttl) = ttl {
                    built = built.with_ttl(*ttl);
                }
                let $memory = &built;
                $op
            }
        }
    };
}

impl Backend {
    async fn remember(&self, scope: MemoryScope, payload: Vec<u8>) -> Result<MemoryId, LaserError> {
        on_backend!(self, |memory| Memory::remember(memory, &scope, payload)
            .await)
    }

    async fn recall(
        &self,
        scope: MemoryScope,
        query: MemoryQuery,
    ) -> Result<Vec<MemoryItem>, LaserError> {
        on_backend!(self, |memory| Memory::recall(memory, &scope, &query).await)
    }

    async fn forget(&self, scope: MemoryScope, id: MemoryId) -> Result<(), LaserError> {
        on_backend!(self, |memory| Memory::forget(memory, &scope, id).await)
    }
}

fn build_scope(agent: Option<String>, conversation: Option<String>) -> PyResult<MemoryScope> {
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
/// one from a `Laser` (`laser.memory()`, `laser.vector_memory(embedder)`,
/// `laser.query_memory(...)`, `laser.kv_memory(...)`).
#[gen_stub_pyclass]
#[pyclass(name = "Memory", frozen)]
pub struct PyMemory {
    inner: Backend,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMemory {
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
    #[pyo3(signature = (*, limit=50, agent=None, conversation=None, semantic=None))]
    fn recall<'py>(
        &self,
        py: Python<'py>,
        limit: usize,
        agent: Option<String>,
        conversation: Option<String>,
        semantic: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.inner.clone();
        let scope = build_scope(agent, conversation)?;
        let query = MemoryQuery::builder()
            .limit(limit)
            .maybe_semantic(semantic)
            .build();
        future_into_py(py, async move {
            let items = backend.recall(scope, query).await.map_err(to_pyerr)?;
            Ok(items
                .into_iter()
                .map(PyMemoryItem::from)
                .collect::<Vec<_>>())
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
    /// Log-backed memory over the durable agent audit log: `remember` appends,
    /// `forget` appends a tombstone, `recall` folds the log (most recent first,
    /// `semantic` ignored). The log is the source of truth, so this works on raw
    /// Apache Iggy. The returned handle holds its incremental recall cursor, so
    /// reuse it: each `recall` folds only what was appended since the last one.
    fn memory(&self) -> PyMemory {
        PyMemory {
            inner: Backend::Log(Arc::new(LogMemory::new(self.inner.clone()))),
        }
    }

    /// In-process semantic memory: `embedder` is an `async def embed(text) ->
    /// list[float]`. `recall` with `semantic` ranks items by cosine similarity to
    /// the query embedding. Self-contained, so it works anywhere.
    fn vector_memory(&self, embedder: Py<PyAny>) -> PyMemory {
        PyMemory {
            inner: Backend::Vector(Arc::new(VectorMemory::new(PyEmbedder {
                callback: Arc::new(embedder),
            }))),
        }
    }

    /// Managed semantic memory backed by the query index on `topic`, embedding
    /// with `embedder` (an `async def embed(text) -> list[float]`). A managed
    /// feature: against raw Apache Iggy it raises `UnsupportedError`.
    fn query_memory(&self, embedder: Py<PyAny>, topic: String) -> PyMemory {
        PyMemory {
            inner: Backend::Query {
                laser: self.inner.clone(),
                embedder: PyEmbedder {
                    callback: Arc::new(embedder),
                },
                topic,
            },
        }
    }

    /// Managed memory backed by the key-value store in `namespace`, one entry per
    /// item, optionally expiring `ttl_secs` after each write. `recall` returns the
    /// most recent (`semantic` ignored), `forget` truly deletes. A managed
    /// feature: against raw Apache Iggy it raises `UnsupportedError`.
    #[pyo3(signature = (namespace, *, ttl_secs=None))]
    fn kv_memory(&self, namespace: String, ttl_secs: Option<f64>) -> PyMemory {
        PyMemory {
            inner: Backend::Kv {
                laser: self.inner.clone(),
                namespace,
                ttl: ttl_secs.map(Duration::from_secs_f64),
            },
        }
    }
}

use crate::agent::PyAgentMessage;
use crate::agent_runtime::static_topic;
use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{json_to_py, payload_bytes};
use crate::errors::to_pyerr;
use crate::memory::{
    Backend, PyConsolidationReport, PyMemory, PyMemoryItem, RememberOptions, build_full_scope,
    map_kind, parse_agent, recall_query,
};
use crate::session::PyCheckpoint;
use crate::snapshot::SnapshotHandle;
use laser_sdk::agent::{ReplayBound, resume_offsets};
use laser_sdk::context::{Chain, ContextMessage, ContextPolicy, LastN, RoleFilter, TokenBudget};
use laser_sdk::laser::Laser;
use laser_sdk::memory::{Feedback, MemoryId, MemoryScope};
use laser_sdk::provenance::AgentTopic;
use laser_sdk::types::{AgentId, ConversationId};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::{BTreeMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

// The default number of turns a bounded read keeps.
const DEFAULT_LAST_N: usize = 50;

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

impl PyContextScope {
    pub(crate) fn new(scope: laser_sdk::context_scope::ContextScope) -> Self {
        Self {
            laser: scope.laser().clone(),
            conversation: scope.conversation(),
        }
    }
}

// The topics a read covers: the named ones, or the assembler's default pair
// (commands and responses) when none were named.
fn read_topics(topics: Option<Vec<String>>) -> PyResult<Vec<AgentTopic<'static>>> {
    match topics {
        Some(names) => names.into_iter().map(static_topic).collect(),
        None => Ok(vec![AgentTopic::Commands, AgentTopic::Responses]),
    }
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
        let topic = static_topic(topic)?;
        let payload = payload_bytes(payload)?;
        future_into_py(py, async move {
            laser
                .context(conversation)
                .append(topic, payload)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Read this conversation's history from `topics` (default `agent.commands`
    /// and `agent.responses`), bounded to the last `last_n` messages (default
    /// 50). `token_budget` trims the selected messages to an estimated token
    /// count, applied after `last_n`, so the read is bounded by turns and by
    /// prompt size at once. Use `fetch_with` for an explicit policy.
    #[pyo3(signature = (*, topics=None, last_n=None, token_budget=None))]
    fn fetch<'py>(
        &self,
        py: Python<'py>,
        topics: Option<Vec<String>>,
        last_n: Option<usize>,
        token_budget: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let policy = bounded_policy(last_n.unwrap_or(DEFAULT_LAST_N), token_budget);
        self.read(py, read_topics(topics)?, policy, None)
    }

    /// Read this conversation's history from `topics` under an explicit policy:
    /// `LastN`, `TokenBudget`, `RoleFilter`, or a `Chain` of them.
    fn fetch_with<'py>(
        &self,
        py: Python<'py>,
        topics: Vec<String>,
        policy: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (policy, failure) = PolicySpec::extract(policy)?.build();
        let topics = read_topics(Some(topics))?;
        self.read(py, topics, policy, Some(failure))
    }

    /// The last `last_n` messages rendered as one newline-joined text block,
    /// the prompt-ready form. `topics` defaults to `agent.commands` and
    /// `agent.responses`. `token_budget` trims the block to an estimated token
    /// count, applied after `last_n`.
    #[pyo3(signature = (*, topics=None, last_n=None, token_budget=None))]
    fn block<'py>(
        &self,
        py: Python<'py>,
        topics: Option<Vec<String>>,
        last_n: Option<usize>,
        token_budget: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let conversation = self.conversation;
        let topics = read_topics(topics)?;
        let policy = bounded_policy(last_n.unwrap_or(DEFAULT_LAST_N), token_budget);
        future_into_py(py, async move {
            let messages = laser
                .context(conversation)
                .fetch_with(topics, policy)
                .await
                .map_err(to_pyerr)?;
            Ok(messages
                .iter()
                .map(|message| String::from_utf8_lossy(&message.payload))
                .collect::<Vec<_>>()
                .join("\n"))
        })
    }

    /// Rebuild state by folding this conversation's messages on `topics` with
    /// `fold(state, message) -> state`, starting from `initial`. Name exactly
    /// one bound: `last_n` messages, `from_offsets` (one per-partition map
    /// shared by every topic), `from_checkpoint` (resume after a checkpoint),
    /// `at` (stop at a checkpoint), or `full=True` (the whole partition).
    #[pyo3(signature = (topics, initial, fold, *, last_n=None, from_offsets=None, from_checkpoint=None, at=None, full=false))]
    #[allow(clippy::too_many_arguments)]
    fn state<'py>(
        &self,
        py: Python<'py>,
        topics: Vec<String>,
        initial: Py<PyAny>,
        fold: Py<PyAny>,
        last_n: Option<usize>,
        from_offsets: Option<BTreeMap<u32, u64>>,
        from_checkpoint: Option<PyRef<'_, PyCheckpoint>>,
        at: Option<PyRef<'_, PyCheckpoint>>,
        full: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bound = replay_bound(
            last_n,
            from_offsets,
            from_checkpoint.map(|checkpoint| checkpoint.inner().clone()),
            at.map(|checkpoint| checkpoint.inner().clone()),
            full,
        )?;
        let topics = read_topics(Some(topics))?;
        let laser = self.laser.clone();
        let conversation = self.conversation;
        future_into_py(py, async move {
            let messages = collect(&laser, conversation, topics, bound).await?;
            fold_messages(messages, initial, fold)
        })
    }

    /// The same fold seeded through a snapshot store: the newest snapshot's
    /// state (JSON-decoded) plus a replay of only the messages after it. A
    /// conversation with no snapshot folds fully from `initial`. Accepts a native `SnapshotStore` or an object with synchronous or async `latest(conversation)` and `save(snapshot)` methods.
    fn state_with<'py>(
        &self,
        py: Python<'py>,
        store: &Bound<'_, PyAny>,
        topics: Vec<String>,
        initial: Py<PyAny>,
        fold: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = SnapshotHandle::from_py(store)?;
        let topics = read_topics(Some(topics))?;
        let laser = self.laser.clone();
        let conversation = self.conversation;
        future_into_py(py, async move {
            let (seed, bound) = match store.latest(conversation.into()).await.map_err(to_pyerr)? {
                Some(snapshot) => {
                    let state: serde_json::Value = serde_json::from_slice(&snapshot.state)
                        .map_err(|error| {
                            crate::errors::CodecError::new_err(format!(
                                "decode snapshot state: {error}"
                            ))
                        })?;
                    let seed = Python::attach(|py| json_to_py(py, &state))?;
                    (seed, ReplayBound::FromOffsets(resume_offsets(&snapshot)))
                }
                None => (initial, ReplayBound::Full),
            };
            let messages = collect(&laser, conversation, topics, bound).await?;
            fold_messages(messages, seed, fold)
        })
    }

    /// The current tail of `topics` (default `agent.commands` and
    /// `agent.responses`) as a `Checkpoint`: fold up to it with `state(at=..)`
    /// or resume after it with `state(from_checkpoint=..)`. Offsets are per
    /// topic partition, not per conversation.
    #[pyo3(signature = (topics=None))]
    fn checkpoint<'py>(
        &self,
        py: Python<'py>,
        topics: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let topics = read_topics(topics)?;
        let laser = self.laser.clone();
        let conversation = self.conversation;
        future_into_py(py, async move {
            let checkpoint = laser
                .context(conversation)
                .checkpoint(&topics)
                .await
                .map_err(to_pyerr)?;
            Ok(PyCheckpoint::new(checkpoint))
        })
    }

    /// Scope memory to this conversation. Pass a namespace (`str`, the same
    /// handle `laser.memory(namespace)` returns) or any `Memory` handle from
    /// `laser.memory`, `memory_on_topic`, `memory_topic`, `memory_with`, or
    /// `vector_memory`. The returned view's `recall` and `remember` bake the
    /// conversation in. Read across conversations through the unscoped handle.
    fn memory(&self, memory: &Bound<'_, PyAny>) -> PyResult<PyScopedMemory> {
        let backend = if let Ok(namespace) = memory.extract::<String>() {
            Backend::new(self.laser.memory(namespace))
        } else {
            memory.cast::<PyMemory>()?.get().backend()
        };
        Ok(PyScopedMemory::new(backend, self.conversation))
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

impl PyContextScope {
    fn read<'py>(
        &self,
        py: Python<'py>,
        topics: Vec<AgentTopic<'static>>,
        policy: Box<dyn ContextPolicy>,
        failure: Option<EstimatorFailure>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let conversation = self.conversation;
        future_into_py(py, async move {
            let messages = laser
                .context(conversation)
                .fetch_with(topics, policy)
                .await
                .map_err(to_pyerr)?;
            if let Some(error) = failure.and_then(|slot| take_failure(&slot)) {
                return Err(error);
            }
            Ok(messages
                .into_iter()
                .map(PyAgentMessage::from_context)
                .collect::<Vec<_>>())
        })
    }
}

fn replay_bound(
    last_n: Option<usize>,
    from_offsets: Option<BTreeMap<u32, u64>>,
    from_checkpoint: Option<laser_sdk::context::Checkpoint>,
    at: Option<laser_sdk::context::Checkpoint>,
    full: bool,
) -> PyResult<ReplayBound> {
    let mut bounds = Vec::new();
    if let Some(n) = last_n {
        bounds.push(ReplayBound::Last(n));
    }
    if let Some(offsets) = from_offsets {
        bounds.push(ReplayBound::FromOffsets(offsets));
    }
    if let Some(checkpoint) = from_checkpoint {
        bounds.push(ReplayBound::FromCheckpoint(checkpoint));
    }
    if let Some(checkpoint) = at {
        bounds.push(ReplayBound::At(checkpoint));
    }
    if full {
        bounds.push(ReplayBound::Full);
    }
    match bounds.len() {
        1 => Ok(bounds.remove(0)),
        _ => Err(crate::errors::InvalidError::new_err(
            "name exactly one replay bound: last_n, from_offsets, from_checkpoint, at, or full=True",
        )),
    }
}

async fn collect(
    laser: &Laser,
    conversation: ConversationId,
    topics: Vec<AgentTopic<'static>>,
    bound: ReplayBound,
) -> PyResult<Vec<ContextMessage>> {
    laser
        .context(conversation)
        .state(
            topics,
            bound,
            Vec::new(),
            |mut acc: Vec<ContextMessage>, message| {
                acc.push(message.clone());
                acc
            },
        )
        .await
        .map_err(to_pyerr)
}

fn fold_messages(
    messages: Vec<ContextMessage>,
    initial: Py<PyAny>,
    fold: Py<PyAny>,
) -> PyResult<Py<PyAny>> {
    Python::attach(|py| {
        let mut state = initial;
        for message in messages {
            let message = Py::new(py, PyAgentMessage::from_context(message))?;
            state = fold.call1(py, (state, message))?;
        }
        Ok(state)
    })
}

// The policy behind `last_n` / `token_budget`: cap by turns, then trim the kept
// turns to the estimated token count when a budget was asked for. Chained in
// that order so the budget always sees the newest window.
pub(crate) fn bounded_policy(last_n: usize, token_budget: Option<usize>) -> Box<dyn ContextPolicy> {
    match token_budget {
        Some(budget) => Box::new(Chain(vec![
            Box::new(LastN(last_n)),
            Box::new(TokenBudget::new(budget)),
        ])),
        None => Box::new(LastN(last_n)),
    }
}

// The first exception a Python token estimator raised during a read. The
// policy runs synchronously inside the assembler and cannot return an error,
// so the estimator parks it here and the read raises it afterwards.
pub(crate) type EstimatorFailure = Arc<Mutex<Option<PyErr>>>;

pub(crate) fn take_failure(slot: &EstimatorFailure) -> Option<PyErr> {
    slot.lock().ok().and_then(|mut slot| slot.take())
}

// A `LastN`, `TokenBudget`, `RoleFilter`, or `Chain` value into the Rust
// policy, plus the slot a Python token estimator reports a failure through.
pub(crate) fn context_policy(
    policy: &Bound<'_, PyAny>,
) -> PyResult<(Box<dyn ContextPolicy>, EstimatorFailure)> {
    Ok(PolicySpec::extract(policy)?.build())
}

#[derive(Clone)]
enum PolicySpec {
    LastN(usize),
    TokenBudget {
        max_tokens: usize,
        estimator: Option<Arc<Py<PyAny>>>,
    },
    RoleFilter(HashSet<AgentId>),
    Chain(Vec<PolicySpec>),
    Custom(Arc<Py<PyAny>>),
}

impl PolicySpec {
    fn extract(policy: &Bound<'_, PyAny>) -> PyResult<Self> {
        if let Ok(policy) = policy.cast::<PyLastN>() {
            return Ok(policy.get().spec.clone());
        }
        if let Ok(policy) = policy.cast::<PyTokenBudget>() {
            return Ok(policy.get().spec.clone());
        }
        if let Ok(policy) = policy.cast::<PyRoleFilter>() {
            return Ok(policy.get().spec.clone());
        }
        if let Ok(policy) = policy.cast::<PyChain>() {
            return Ok(policy.get().spec.clone());
        }
        if policy.is_callable()
            || policy
                .getattr("select")
                .is_ok_and(|apply| apply.is_callable())
        {
            return Ok(Self::Custom(Arc::new(policy.clone().unbind())));
        }
        Err(crate::errors::InvalidError::new_err(
            "a context policy is LastN, TokenBudget, RoleFilter, Chain, or a synchronous callable or select method",
        ))
    }

    fn build(self) -> (Box<dyn ContextPolicy>, EstimatorFailure) {
        let failure: EstimatorFailure = Arc::new(Mutex::new(None));
        (self.build_with(&failure), failure)
    }

    fn build_with(self, failure: &EstimatorFailure) -> Box<dyn ContextPolicy> {
        match self {
            Self::LastN(n) => Box::new(LastN(n)),
            Self::Custom(callback) => Box::new(CustomPolicy {
                callback,
                failure: failure.clone(),
            }),
            Self::TokenBudget {
                max_tokens,
                estimator: None,
            } => Box::new(TokenBudget::new(max_tokens)),
            Self::TokenBudget {
                max_tokens,
                estimator: Some(estimator),
            } => {
                let failure = failure.clone();
                Box::new(TokenBudget::with_estimator(max_tokens, move |message| {
                    estimate(&estimator, &failure, message)
                }))
            }
            Self::RoleFilter(roles) => Box::new(RoleFilter(roles)),
            Self::Chain(policies) => Box::new(Chain(
                policies
                    .into_iter()
                    .map(|policy| policy.build_with(failure))
                    .collect(),
            )),
        }
    }
}

struct CustomPolicy {
    callback: Arc<Py<PyAny>>,
    failure: EstimatorFailure,
}

impl ContextPolicy for CustomPolicy {
    fn select(&self, messages: &[ContextMessage]) -> Vec<ContextMessage> {
        let result = Python::attach(|py| {
            let messages = messages
                .iter()
                .cloned()
                .map(|message| Py::new(py, PyAgentMessage::from_context(message)))
                .collect::<PyResult<Vec<_>>>()?;
            let callback = self.callback.bind(py);
            let value = if callback.hasattr("select")? {
                callback.call_method1("select", (messages,))?
            } else {
                callback.call1((messages,))?
            };
            if value.hasattr("__await__")? {
                if value.hasattr("close")? {
                    value.call_method0("close")?;
                }
                return Err(crate::errors::InvalidError::new_err(
                    "a context policy must return messages synchronously",
                ));
            }
            let messages: Vec<Py<PyAgentMessage>> = value.extract()?;
            Ok(messages
                .into_iter()
                .map(|message| {
                    let py_message = message.borrow(py);
                    let message = py_message.inner.clone();
                    ContextMessage {
                        provenance: message.provenance,
                        payload: message.payload,
                        id: message.id,
                        envelope: message.envelope,
                        topic: py_message.topic.clone().unwrap_or_default(),
                    }
                })
                .collect())
        });
        match result {
            Ok(messages) => messages,
            Err(error) => {
                if let Ok(mut failure) = self.failure.lock()
                    && failure.is_none()
                {
                    *failure = Some(error);
                }
                Vec::new()
            }
        }
    }
}

// Call a Python estimator. A failure is parked for the read to raise, and the
// message is costed as unbounded so the budget keeps nothing past it.
fn estimate(estimator: &Py<PyAny>, failure: &EstimatorFailure, message: &ContextMessage) -> usize {
    let result = Python::attach(|py| -> PyResult<usize> {
        let message = Py::new(py, PyAgentMessage::from_context(message.clone()))?;
        estimator.call1(py, (message,))?.extract::<usize>(py)
    });
    match result {
        Ok(tokens) => tokens,
        Err(error) => {
            if let Ok(mut slot) = failure.lock()
                && slot.is_none()
            {
                *slot = Some(error);
            }
            usize::MAX
        }
    }
}

/// Context policy: keep the most recent `n` messages.
#[gen_stub_pyclass]
#[pyclass(name = "LastN", frozen)]
pub struct PyLastN {
    spec: PolicySpec,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLastN {
    #[new]
    fn new(n: usize) -> Self {
        Self {
            spec: PolicySpec::LastN(n),
        }
    }
}

/// Context policy: keep the newest messages that fit in `max_tokens`
/// estimated tokens, and always at least one. The default estimate is one
/// token for each 4 bytes of payload. `estimator(message) -> int` replaces it,
/// for example with a real tokenizer.
#[gen_stub_pyclass]
#[pyclass(name = "TokenBudget", frozen)]
pub struct PyTokenBudget {
    spec: PolicySpec,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyTokenBudget {
    #[new]
    #[pyo3(signature = (max_tokens, estimator=None))]
    fn new(max_tokens: usize, estimator: Option<Py<PyAny>>) -> Self {
        Self {
            spec: PolicySpec::TokenBudget {
                max_tokens,
                estimator: estimator.map(Arc::new),
            },
        }
    }
}

/// Context policy: keep only messages from the given agents.
#[gen_stub_pyclass]
#[pyclass(name = "RoleFilter", frozen)]
pub struct PyRoleFilter {
    spec: PolicySpec,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRoleFilter {
    #[new]
    fn new(agents: Vec<String>) -> PyResult<Self> {
        let roles = agents
            .into_iter()
            .map(|agent| AgentId::new(agent).map_err(|e| to_pyerr(e.into())))
            .collect::<PyResult<HashSet<_>>>()?;
        Ok(Self {
            spec: PolicySpec::RoleFilter(roles),
        })
    }
}

/// Context policy: apply `policies` in sequence, each one over the output of
/// the one before, for example `Chain([LastN(20), TokenBudget(4000)])`.
#[gen_stub_pyclass]
#[pyclass(name = "Chain", frozen)]
pub struct PyChain {
    spec: PolicySpec,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyChain {
    #[new]
    fn new(policies: Vec<Bound<'_, PyAny>>) -> PyResult<Self> {
        let specs = policies
            .iter()
            .map(PolicySpec::extract)
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Self {
            spec: PolicySpec::Chain(specs),
        })
    }
}

/// One conversation's memory: recall and remember with the conversation already
/// applied. Build it with `ContextScope.memory` or `Session.memory`.
#[gen_stub_pyclass]
#[pyclass(name = "ScopedMemory")]
pub struct PyScopedMemory {
    backend: Backend,
    conversation: ConversationId,
}

impl PyScopedMemory {
    pub(crate) fn new(backend: Backend, conversation: ConversationId) -> Self {
        Self {
            backend,
            conversation,
        }
    }

    fn scope(
        &self,
        agent: Option<String>,
        user: Option<String>,
        application: Option<String>,
        stream: Option<String>,
        durable: bool,
    ) -> PyResult<MemoryScope> {
        build_full_scope(
            agent,
            Some(self.conversation.to_string()),
            user,
            application,
            stream,
            durable,
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyScopedMemory {
    /// Recall up to `limit` items within this conversation. Pass `semantic`
    /// text to rank by similarity (backends that embed), or a `strategy`
    /// (`auto`, `recent`, `semantic`, `keyword`, `graph`, `temporal`,
    /// `hybrid`) with the query text in `semantic`. Default recall reads the
    /// managed key-value view. `folded=True` rebuilds memory from the topic in
    /// process instead.
    #[pyo3(signature = (*, limit=50, semantic=None, strategy=None, agent=None, user=None, application=None, folded=false, stream=None, durable=false, token_budget=None))]
    #[allow(clippy::too_many_arguments)]
    fn recall<'py>(
        &self,
        py: Python<'py>,
        limit: usize,
        semantic: Option<String>,
        strategy: Option<String>,
        agent: Option<String>,
        user: Option<String>,
        application: Option<String>,
        folded: bool,
        stream: Option<String>,
        durable: bool,
        token_budget: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.backend.clone();
        let scope = self.scope(agent, user, application, stream, durable)?;
        let query = recall_query(&scope, limit, semantic, strategy, token_budget)?;
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

    /// Remember `payload` (str, bytes, or bytearray) in this conversation.
    /// Returns the new item's id. `kind`, `durable`, and `dedup` behave as on
    /// `Memory.remember`.
    #[pyo3(signature = (payload, *, agent=None, stream=None, user=None, application=None, kind="fact", durable=false, dedup=false))]
    #[allow(clippy::too_many_arguments)]
    fn remember<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'_, PyAny>,
        agent: Option<String>,
        stream: Option<String>,
        user: Option<String>,
        application: Option<String>,
        kind: &str,
        durable: bool,
        dedup: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.backend.clone();
        let options = RememberOptions {
            agent: parse_agent(agent)?,
            conversation: Some(self.conversation),
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

    /// Keyword recall for `query` within this conversation, up to `limit`
    /// items. It needs no embedder. Default recall reads the managed key-value
    /// view, and `folded=True` folds the topic in process.
    #[pyo3(signature = (query, *, limit=50, folded=false))]
    fn search<'py>(
        &self,
        py: Python<'py>,
        query: String,
        limit: usize,
        folded: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.backend.clone();
        let scope = self.scope(None, None, None, None, false)?;
        let query = recall_query(&scope, limit, Some(query), Some("keyword".to_owned()), None)?;
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

    /// The one-call context altitude: recall this conversation's most relevant
    /// items rendered as one prompt-ready block under an optional
    /// `token_budget`.
    #[pyo3(signature = (token_budget=None))]
    fn block<'py>(
        &self,
        py: Python<'py>,
        token_budget: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.backend.clone();
        let conversation = self.conversation;
        future_into_py(py, async move {
            backend
                .context(conversation, token_budget)
                .await
                .map_err(to_pyerr)
        })
    }

    /// One consolidation pass over this conversation, keeping the newest
    /// `max_items` and pruning the rest. `summarizer` and `prune_summarized`
    /// work as on `Memory.consolidate`.
    #[pyo3(signature = (max_items, *, summarizer=None, prune_summarized=false))]
    fn consolidate<'py>(
        &self,
        py: Python<'py>,
        max_items: usize,
        summarizer: Option<Py<PyAny>>,
        prune_summarized: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.backend.clone();
        let summarizer = summarizer.map(|callback| crate::memory::PySummarizer { callback });
        let scope = MemoryScope::builder()
            .conversation(self.conversation)
            .build();
        future_into_py(py, async move {
            let report = backend
                .consolidate(scope, max_items, summarizer, prune_summarized)
                .await
                .map_err(to_pyerr)?;
            Ok(PyConsolidationReport::from(report))
        })
    }

    /// Record feedback on a recalled item in this conversation. `weight` is
    /// positive to promote, negative to demote. Returns the feedback record's id.
    fn improve<'py>(
        &self,
        py: Python<'py>,
        memory_id: String,
        weight: f32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.backend.clone();
        let scope = self.scope(None, None, None, None, false)?;
        let target = MemoryId::from_str(&memory_id).map_err(|e| to_pyerr(e.into()))?;
        future_into_py(py, async move {
            let id = backend
                .improve(scope, Feedback::new(target, weight))
                .await
                .map_err(to_pyerr)?;
            Ok(id.to_string())
        })
    }

    /// Forget the item `memory_id` in this conversation.
    fn forget<'py>(&self, py: Python<'py>, memory_id: String) -> PyResult<Bound<'py, PyAny>> {
        let backend = self.backend.clone();
        let scope = self.scope(None, None, None, None, false)?;
        let id = MemoryId::from_str(&memory_id).map_err(|e| to_pyerr(e.into()))?;
        future_into_py(py, async move {
            backend.forget(scope, id).await.map_err(to_pyerr)
        })
    }

    /// The underlying handle, for the cross-conversation verbs this scoped
    /// view does not narrow.
    #[getter]
    fn handle(&self) -> PyMemory {
        PyMemory::from_backend(self.backend.clone())
    }

    /// This scoped memory's conversation id.
    #[getter]
    fn conversation(&self) -> String {
        self.conversation.to_string()
    }
}

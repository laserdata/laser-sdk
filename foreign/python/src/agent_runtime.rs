use crate::agent::{PyAgentMessage, PyProvenance};
use crate::async_bridge::{HookLoop, PyHook, call_hook, future_into_py};
use crate::client::PyLaser;
use crate::convert::{duration_ms, payload_bytes, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use crate::sign::{PyKeyRegistry, PySigningKey};
use async_trait::async_trait;
use iggy::prelude::Identifier;
use laser_sdk::LaserError;
use laser_sdk::agent::{
    AgentCtx, AgentHandler, AgentMessage, AgentMiddleware, CapabilitySelector, ConcurrencyPolicy,
    Contract, DeadLetterSink, Deduplicator, Gather, GatherPolicy, InboxRoute, ReliableConsumer,
    RetryPolicy, RouteCandidate, RoutePolicy, RouteScorer, Router, ScatterReport, SlidingWindow,
};
use laser_sdk::context::{Chain, ContextAssembler, ContextPolicy, LastN, RoleFilter, TokenBudget};
use laser_sdk::laser::Laser;
use laser_sdk::provenance::{AgentTopic, Provenance};
use laser_sdk::types::{AgentId, ConversationId, PrincipalId};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{get_current_locals, get_runtime, scope};
use pyo3_stub_gen::derive::{
    gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pyfunction, gen_stub_pymethods,
};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

fn topic_id(name: &str) -> PyResult<Identifier> {
    Identifier::named(name).map_err(|e| InvalidError::new_err(e.to_string()))
}

/// Parse an advertised-health string to a [`Health`]. Unknown text collapses to a
/// single unrecognized sentinel, which routing treats as available (the permissive
/// default), so a typo never silently removes an agent.
fn parse_health(health: &str) -> laser_sdk::wire::agent::Health {
    use laser_sdk::wire::agent::Health;
    match health.to_ascii_lowercase().as_str() {
        "healthy" => Health::Healthy,
        "degraded" => Health::Degraded,
        "unavailable" => Health::Unavailable,
        _ => Health::Unrecognized(0),
    }
}

/// A fixed inbox route to `topic` when given, else the default advertised route.
pub(crate) fn inbox_route(fixed_inbox: Option<String>) -> PyResult<InboxRoute> {
    match fixed_inbox {
        Some(topic) => Ok(InboxRoute::Fixed(static_topic(topic)?)),
        None => Ok(InboxRoute::default()),
    }
}

/// A `respond_on` topic needs a `'static` `AgentTopic`. Well-known names map to
/// their static variant. A custom name is interned: the leaked `Identifier` is
/// memoized per distinct name, so a name reused on a per-message path costs one
/// allocation for the process rather than one per call.
///
/// An unusable name raises rather than falling back to a well-known topic:
/// silently rerouting a caller's messages onto the shared command topic is a
/// misroute, not a recovery.
pub(crate) fn static_topic(name: String) -> PyResult<AgentTopic<'static>> {
    static INTERNED: OnceLock<Mutex<HashMap<String, &'static Identifier>>> = OnceLock::new();
    match name.as_str() {
        "agent.sessions" => return Ok(AgentTopic::Sessions),
        "agent.streams" => return Ok(AgentTopic::Streams),
        "agent.heartbeats" => return Ok(AgentTopic::Heartbeats),
        "agent.control" => return Ok(AgentTopic::Control),
        "agent.memory" => return Ok(AgentTopic::Memory),
        "agent.audit" => return Ok(AgentTopic::Audit),
        "agent.registry" => return Ok(AgentTopic::Registry),
        "agent.workflow_journal" => return Ok(AgentTopic::WorkflowJournal),
        "agent.dlq" => return Ok(AgentTopic::Dlq),
        _ => {}
    }
    let mut interned = INTERNED
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(id) = interned.get(&name) {
        return Ok(AgentTopic::Custom(id));
    }
    let id = Identifier::named(&name)
        .map_err(|error| InvalidError::new_err(format!("invalid topic `{name}`: {error}")))?;
    let id: &'static Identifier = Box::leak(Box::new(id));
    interned.insert(name, id);
    Ok(AgentTopic::Custom(id))
}

// The Python spelling of a `RoutePolicy`: the variant in snake_case, with the
// pinned agent after a colon for `sticky`.
pub(crate) struct RouteWord(pub(crate) RoutePolicy);

impl FromStr for RouteWord {
    type Err = PyErr;

    fn from_str(word: &str) -> PyResult<Self> {
        let policy = match word {
            "any" => RoutePolicy::Any,
            "cheapest" => RoutePolicy::Cheapest,
            "fastest" => RoutePolicy::Fastest,
            "least_loaded" => RoutePolicy::LeastLoaded,
            other => match other.strip_prefix("sticky:") {
                Some(agent) => RoutePolicy::Sticky(
                    AgentId::new(agent.to_owned()).map_err(|e| to_pyerr(e.into()))?,
                ),
                None => {
                    return Err(InvalidError::new_err(format!(
                        "unknown route policy '{other}', expected any, cheapest, fastest, least_loaded, or sticky:<agent>"
                    )));
                }
            },
        };
        Ok(Self(policy))
    }
}

/// Parse a fan-out policy. Only `quorum` takes a quorum count.
pub(crate) type RouteFailure = Arc<Mutex<Option<PyErr>>>;

pub(crate) struct ParsedRoutePolicy {
    pub(crate) policy: RoutePolicy,
    pub(crate) failure: RouteFailure,
}

pub(crate) fn take_route_failure(failure: &RouteFailure) -> Option<PyErr> {
    failure.lock().ok().and_then(|mut failure| failure.take())
}

// A route scorer is synchronous, matching the native selection seam.
pub(crate) fn route_policy(policy: Option<&Bound<'_, PyAny>>) -> PyResult<ParsedRoutePolicy> {
    let failure = Arc::new(Mutex::new(None));
    let policy = match policy {
        None => RoutePolicy::Any,
        Some(policy) if policy.is_none() => RoutePolicy::Any,
        Some(policy) => {
            if let Ok(word) = policy.extract::<String>() {
                let RouteWord(policy) = word.parse()?;
                policy
            } else if policy.is_callable()
                || policy
                    .getattr("select")
                    .is_ok_and(|select| select.is_callable())
            {
                RoutePolicy::Custom(Arc::new(PyRouteScorer {
                    callback: policy.clone().unbind(),
                    failure: failure.clone(),
                }))
            } else {
                return Err(InvalidError::new_err(
                    "a route policy must be a built-in word, callable, or object with a synchronous select method",
                ));
            }
        }
    };
    Ok(ParsedRoutePolicy { policy, failure })
}

/// Rebuilds in-memory state by folding a conversation's logged events. The
/// same fold `Laser.context(conversation).state` runs, addressed by laser and
/// conversation.
#[gen_stub_pyclass]
#[pyclass(name = "ConversationState", frozen)]
pub struct PyConversationState;

#[gen_stub_pymethods]
#[pymethods]
impl PyConversationState {
    /// Replay `topics` for `conversation` under exactly one explicit bound
    /// (`last_n`, `from_offsets`, `from_checkpoint`, `at`, or `full=True`) and
    /// fold every message through `fold(state, message) -> state`, starting
    /// from `init`.
    #[staticmethod]
    #[pyo3(signature = (laser, conversation, topics, init, fold, *, last_n=None, from_offsets=None, from_checkpoint=None, at=None, full=false))]
    #[allow(clippy::too_many_arguments)]
    fn load<'py>(
        py: Python<'py>,
        laser: &Bound<'py, PyLaser>,
        conversation: String,
        topics: Vec<String>,
        init: Py<PyAny>,
        fold: Py<PyAny>,
        last_n: Option<usize>,
        from_offsets: Option<BTreeMap<String, BTreeMap<u32, u64>>>,
        from_checkpoint: Option<Py<PyAny>>,
        at: Option<Py<PyAny>>,
        full: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let bound = pyo3::types::PyDict::new(py);
        if let Some(last_n) = last_n {
            bound.set_item("last_n", last_n)?;
        }
        if let Some(from_offsets) = from_offsets {
            bound.set_item("from_offsets", from_offsets)?;
        }
        if let Some(from_checkpoint) = from_checkpoint {
            bound.set_item("from_checkpoint", from_checkpoint)?;
        }
        if let Some(at) = at {
            bound.set_item("at", at)?;
        }
        if full {
            bound.set_item("full", full)?;
        }
        laser.call_method1("context", (conversation,))?.call_method(
            "state",
            (topics, init, fold),
            Some(&bound),
        )
    }

    /// The same fold seeded through a snapshot `store`: the newest snapshot's
    /// state plus a replay of only the messages after it. A conversation with
    /// no snapshot folds fully from `init`.
    #[staticmethod]
    fn load_with<'py>(
        laser: &Bound<'py, PyLaser>,
        store: &Bound<'py, PyAny>,
        conversation: String,
        topics: Vec<String>,
        init: Py<PyAny>,
        fold: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        laser
            .call_method1("context", (conversation,))?
            .call_method1("state_with", (store, topics, init, fold))
    }
}

/// One capability-route candidate as a route scorer sees it: the agent, its
/// registered card, and the descriptor it advertises for the routed skill.
#[gen_stub_pyclass]
#[pyclass(name = "RouteCandidate", frozen)]
pub struct PyRouteCandidate {
    agent: String,
    card: Py<crate::registry::PyRegisteredCard>,
    capability: Option<Py<PyAny>>,
}

impl PyRouteCandidate {
    fn from_rust(py: Python<'_>, candidate: &RouteCandidate<'_>) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                agent: candidate.agent.as_str().to_owned(),
                card: Py::new(py, crate::registry::PyRegisteredCard::from(candidate.card))?,
                capability: candidate
                    .capability
                    .as_ref()
                    .map(|value| ser_to_py(py, value))
                    .transpose()?,
            },
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRouteCandidate {
    /// The candidate agent.
    #[getter]
    fn agent(&self) -> String {
        self.agent.clone()
    }

    /// The agent's registered card.
    #[getter]
    fn card(&self, py: Python<'_>) -> Py<crate::registry::PyRegisteredCard> {
        self.card.clone_ref(py)
    }

    /// The candidate's descriptor for the routed skill, as a dict mirroring
    /// `CapabilityDescriptor`, when its card names the skill.
    #[getter]
    fn capability(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.capability.as_ref().map(|value| value.clone_ref(py))
    }

    fn __repr__(&self) -> String {
        format!("RouteCandidate(agent={})", self.agent)
    }
}

struct PyRouteScorer {
    callback: Py<PyAny>,
    failure: RouteFailure,
}

impl RouteScorer for PyRouteScorer {
    fn select(&self, skill_id: &str, candidates: &[RouteCandidate<'_>]) -> Option<usize> {
        let result = Python::attach(|py| {
            let view = pyo3::types::PyList::empty(py);
            for candidate in candidates {
                view.append(PyRouteCandidate::from_rust(py, candidate)?)?;
            }
            let callback = self.callback.bind(py);
            let selected = if callback.hasattr("select")? {
                callback.call_method1("select", (skill_id, view))?
            } else {
                callback.call1((skill_id, view))?
            };
            if selected.hasattr("__await__")? {
                if selected.hasattr("close")? {
                    selected.call_method0("close")?;
                }
                return Err(InvalidError::new_err(
                    "a route scorer must return an index or None synchronously",
                ));
            }
            selected.extract::<Option<usize>>().map_err(|_| {
                InvalidError::new_err("a route scorer must return a non-negative index or None")
            })
        });
        match result {
            Ok(selected) => selected,
            Err(error) => {
                if let Ok(mut failure) = self.failure.lock()
                    && failure.is_none()
                {
                    *failure = Some(error);
                }
                None
            }
        }
    }
}

pub(crate) fn route_result<T>(
    result: Result<T, LaserError>,
    failure: &RouteFailure,
) -> PyResult<T> {
    if result
        .as_ref()
        .err()
        .is_some_and(LaserError::is_no_capable_agent)
        && let Some(error) = take_route_failure(failure)
    {
        return Err(error);
    }
    let _ = take_route_failure(failure);
    result.map_err(to_pyerr)
}

fn parse_gather_policy(policy: &str, quorum: Option<usize>) -> PyResult<GatherPolicy> {
    match policy {
        "require_all" => Ok(GatherPolicy::RequireAll),
        "best_effort" => Ok(GatherPolicy::BestEffort),
        "quorum" => quorum
            .map(GatherPolicy::Quorum)
            .ok_or_else(|| InvalidError::new_err("policy=\"quorum\" requires quorum=<n>")),
        other => Err(InvalidError::new_err(format!(
            "unknown fan_out policy `{other}`, expected require_all, quorum, or best_effort"
        ))),
    }
}

/// Reply provenance chained off the handled message (mirrors the Rust AgentCtx):
/// same conversation and root, causal parent is the handled message, the reply is
/// stamped with this agent's id, and the request's correlation id is echoed back
/// so the caller's request/reply dispatcher matches this reply unambiguously. The
/// business idempotency key is deliberately NOT echoed: the reply is its own
/// operation, and echoing it would cross-match replies when a caller sets a real
/// dedup key and retries.
fn reply_provenance(message: &AgentMessage, agent: &Option<AgentId>) -> Provenance {
    let mut provenance = Provenance::builder()
        .conversation_id(message.provenance.conversation_id)
        .causal_parent(message.id)
        .build();
    provenance.agent = agent.clone();
    provenance.root_conversation_id = message.provenance.root_conversation_id;
    provenance.correlation_id = message.provenance.correlation_id.clone();
    provenance
}

// The Rust handler that drives a Python handler: a callable or an object with
// `handle(ctx, message)`, sync or async. A per-partition lane runs outside the
// scoped consumer task, so the hook falls back to the loop captured at spawn.
struct PyHandler {
    callback: PyHook,
    agent: Option<AgentId>,
    respond_on: Option<String>,
    signing_key: Option<Arc<laser_sdk::sign::SigningKey>>,
    inbox_route: InboxRoute,
}

impl AgentHandler for PyHandler {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let laser = ctx.laser().clone();
        let py_message = PyAgentMessage::from_inner(message.clone());
        let py_ctx = PyAgentCtx {
            laser,
            session: ctx.session(),
            request_at: ctx.request_at(),
            agent: self.agent.clone(),
            respond_on: self.respond_on.clone(),
            message: message.clone(),
            signing_key: self.signing_key.clone(),
            inbox_route: self.inbox_route.clone(),
        };
        self.callback
            .call(|py| (py_ctx, py_message).into_pyobject(py))
            .await
            .map_err(crate::errors::from_callback_error)?;
        Ok(())
    }
}

// A `Deduplicator` backed by a Python callable or an object with
// `observe(key) -> bool`, sync or async. A callback that raises or returns a
// non-bool is treated as "new" (return true), so dedup never silently drops a
// message on a callback fault. The fault is logged (via the pyo3-log bridge)
// rather than swallowed, so a persistently broken deduplicator is observable.
struct PyDeduplicator {
    callback: PyHook,
}

#[async_trait]
impl Deduplicator for PyDeduplicator {
    async fn observe(&self, key: &str) -> bool {
        let key = key.to_owned();
        match self.callback.call(|py| (key,).into_pyobject(py)).await {
            Ok(value) => Python::attach(|py| match value.bind(py).extract::<bool>() {
                Ok(seen) => seen,
                Err(error) => {
                    log::warn!("dedup callback returned a non-bool, treating as new: {error}");
                    true
                }
            }),
            Err(error) => {
                log::warn!("dedup callback raised, treating as new: {error}");
                true
            }
        }
    }
}

// The sink, a callable or an object with `on_dead_letter`, receives the
// complete capsule and the typed publication failure.
struct PyDeadLetterSink {
    callback: PyHook,
}

#[async_trait]
impl laser_sdk::agent::DeadLetterSink for PyDeadLetterSink {
    async fn on_dead_letter(
        &self,
        message: Option<&AgentMessage>,
        capsule: &laser_sdk::wire::agent::AgentDeadLetter,
        publish_result: &Result<(), LaserError>,
    ) {
        let py_message = message.cloned().map(PyAgentMessage::from_inner);
        let result = self
            .callback
            .call(|py| {
                let capsule = ser_to_py(py, capsule)?;
                let publish_error = publish_result
                    .as_ref()
                    .err()
                    .map(crate::errors::to_pyerr_ref)
                    .map(|error| error.value(py).clone());
                (py_message, capsule, publish_error).into_pyobject(py)
            })
            .await;
        if let Err(error) = result {
            log::warn!("dead-letter sink raised: {error}");
        }
    }
}

// Optional middleware hooks receive the full attempt result.
struct PyMiddleware {
    hooks: Py<PyAny>,
    fallback: HookLoop,
}

#[async_trait]
impl AgentMiddleware for PyMiddleware {
    async fn before_handle(&self, message: &AgentMessage) -> Result<(), LaserError> {
        let py_message = PyAgentMessage::from_inner(message.clone());
        call_hook(&self.fallback, |call| {
            let bound = self.hooks.bind(call.py());
            if !bound.hasattr("before_handle")? {
                return Ok(call.py().None());
            }
            call.call_method(bound, "before_handle", (py_message,))
        })
        .await
        .map(|_| ())
        .map_err(crate::errors::from_callback_error)
    }

    async fn after_handle(
        &self,
        message: &AgentMessage,
        result: &Result<(), LaserError>,
        attempt: u32,
    ) {
        let py_message = PyAgentMessage::from_inner(message.clone());
        let result = call_hook(&self.fallback, |call| {
            let py = call.py();
            let bound = self.hooks.bind(py);
            if !bound.hasattr("after_handle")? {
                return Ok(py.None());
            }
            let outcome = pyo3::types::PyDict::new(py);
            outcome.set_item("ok", result.is_ok())?;
            let error = result.as_ref().err().map(crate::errors::to_pyerr_ref);
            outcome.set_item("error", error.as_ref().map(|error| error.value(py)))?;
            call.call_method(bound, "after_handle", (py_message, outcome, attempt))
        })
        .await;
        if let Err(error) = result {
            log::warn!("middleware after_handle raised: {error}");
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Spawn an agent: join `consumer_group` (default `agent_id`) over
    /// `listen_on` and drive `handler` for each message, with at-least-once
    /// delivery, dedup, retry, and DLQ. `handler`, `dedup`, `dead_letter`, and
    /// `consolidator` are each a callable or an object with the named method:
    /// `handle(ctx, message)`, `observe(key) -> bool` for a custom, e.g.
    /// durable, deduplicator, `on_dead_letter(message, capsule, publish_error)`,
    /// and `consolidate(scope)`. Every hook can return directly or through an
    /// awaitable. An asynchronous hook runs on the event loop that spawned the
    /// agent. A synchronous hook runs on an SDK worker thread, so it must not
    /// block or use the event loop. Both see the context variables of the
    /// spawning task. `dedup_window` sizes the default in-memory window in
    /// keys. `max_partitions` runs one ordered worker lane per partition up
    /// to that many concurrent lanes (omit for strict serial). `shutdown_grace_ms`
    /// bounds how long a graceful stop waits for the in-flight message.
    /// The dead-letter hook receives the complete capsule dictionary and a typed SDK exception when publication fails.
    /// `middleware` has optional `before_handle(message)` and `after_handle(message, result, attempt)` hooks.
    /// The result dictionary contains `ok` and `error`.
    /// `governor` (an object with `async def decide(action) ->
    /// ActionDecision`) governs everything the handler publishes, applied under
    /// `governor_mode` (`"enforce"` | `"observe"`), replacing any
    /// connection-level governor for this agent. Returns a handle to await
    /// readiness and stop it. Requires a default stream.
    /// `fixed_inbox` sets the handler's default fan-out route. `governor_retention=(capacity, idle_ttl_ms)` bounds evidence heads.
    /// Capabilities accept skill names or descriptor dicts. Consolidation requires both `consolidate_every_ms` and `consolidator`.
    /// Each pass receives a scope dict that names this agent when it has an id and leaves every other field `None`.
    /// Shutdown cancels the active asynchronous consolidation callback and stops new passes.
    /// `agent_id=None` opens an unscoped reliable consumer and requires `consumer_group`. It cannot advertise capabilities.
    /// `operations` lists the command operations the handler serves. Omit it to serve every operation.
    /// `sessions` is a `Sessions` factory whose config the runtime applies: the lens `ctx.session()` opens and whether a dead letter fails its session.
    #[pyo3(signature = (agent_id, listen_on, handler, *, consumer_group=None, respond_on=None, fixed_inbox=None, poll_interval_ms=None, warm_dedup=false, dedup=None, dedup_window=None, consolidate_every_ms=None, consolidator=None, capabilities=None, ack_on_pickup=false, health=None, max_partitions=None, max_queued_records=None, max_queued_bytes=None, understood_features=0, shutdown_grace_ms=None, dead_letter=None, middleware=None, retry_max_attempts=None, retry_base_delay_ms=None, governor=None, governor_mode="enforce", governor_retention=None, signing_key=None, verifier=None, operations=None, sessions=None))]
    #[allow(clippy::too_many_arguments)]
    fn spawn_agent(
        &self,
        py: Python<'_>,
        agent_id: Option<String>,
        listen_on: String,
        handler: &Bound<'_, PyAny>,
        consumer_group: Option<String>,
        respond_on: Option<String>,
        fixed_inbox: Option<String>,
        poll_interval_ms: Option<u64>,
        warm_dedup: bool,
        dedup: Option<&Bound<'_, PyAny>>,
        dedup_window: Option<usize>,
        consolidate_every_ms: Option<u64>,
        consolidator: Option<&Bound<'_, PyAny>>,
        capabilities: Option<Vec<Bound<'_, PyAny>>>,
        ack_on_pickup: bool,
        health: Option<String>,
        max_partitions: Option<usize>,
        max_queued_records: Option<usize>,
        max_queued_bytes: Option<usize>,
        understood_features: u64,
        shutdown_grace_ms: Option<u64>,
        dead_letter: Option<&Bound<'_, PyAny>>,
        middleware: Option<Vec<Py<PyAny>>>,
        retry_max_attempts: Option<u32>,
        retry_base_delay_ms: Option<u64>,
        governor: Option<Py<PyAny>>,
        governor_mode: &str,
        governor_retention: Option<(usize, f64)>,
        signing_key: Option<&PySigningKey>,
        verifier: Option<&PyKeyRegistry>,
        operations: Option<Vec<String>>,
        sessions: Option<&crate::session::PySessions>,
    ) -> PyResult<PyAgentHandle> {
        let sessions = sessions.map(|sessions| sessions.inner.config().clone());
        let agent = agent_id
            .map(AgentId::new)
            .transpose()
            .map_err(|e| to_pyerr(e.into()))?;
        // A per-agent governor re-scopes the agent's `Laser`, so everything the
        // handler publishes through its ctx is governed (mirrors the Rust
        // `Agent::builder().governor(..)`).
        let retention = governor_retention
            .map(|(capacity, idle_ttl_ms)| {
                Ok::<_, PyErr>(laser_sdk::govern::GovernorRetention {
                    capacity,
                    idle_ttl: duration_ms(idle_ttl_ms, "governor idle_ttl_ms")?,
                })
            })
            .transpose()?;
        let laser = match governor {
            Some(hooks) => {
                let governor = Arc::new(crate::govern::PyActionGovernor::new(hooks.bind(py))?);
                let mode = crate::govern::parse_mode(governor_mode)?;
                match retention {
                    Some(retention) => self
                        .inner
                        .with_governor_retention(governor, mode, retention),
                    None => self.inner.with_governor(governor, mode),
                }
            }
            None => self.inner.clone(),
        };
        let route = inbox_route(fixed_inbox)?;
        let group = match consumer_group {
            Some(group) => laser_sdk::types::ConsumerGroupName::new(group)
                .map_err(|error| to_pyerr(error.into()))?,
            None => agent
                .as_ref()
                .map(laser_sdk::types::ConsumerGroupName::for_agent)
                .ok_or_else(|| {
                    InvalidError::new_err("an unscoped reliable consumer requires consumer_group")
                })?,
        };
        let signing_key = signing_key.map(|key| key.inner.clone());
        let py_handler = PyHandler {
            callback: PyHook::new(handler, "handle", "an agent handler")?,
            agent: agent.clone(),
            respond_on: respond_on.clone(),
            signing_key: signing_key.clone(),
            inbox_route: route,
        };
        let deduplicator: Option<Box<dyn Deduplicator>> = match (dedup, dedup_window) {
            (Some(_), Some(_)) => {
                return Err(InvalidError::new_err(
                    "pass dedup or dedup_window, not both",
                ));
            }
            (Some(callback), None) => Some(Box::new(PyDeduplicator {
                callback: PyHook::new(callback, "observe", "a deduplicator")?,
            })),
            (None, Some(capacity)) => Some(Box::new(SlidingWindow::new(capacity))),
            (None, None) => None,
        };
        // One worker lane per partition when `max_partitions` is set (concurrent
        // across partitions, strictly ordered within one), else strict serial.
        let concurrency = match max_partitions {
            Some(max_partitions) => ConcurrencyPolicy::SerialPerPartition { max_partitions },
            None => ConcurrencyPolicy::Serial,
        };
        let dead_letter_sink = dead_letter
            .map(|callback| {
                Ok::<_, PyErr>(Arc::new(PyDeadLetterSink {
                    callback: PyHook::new(callback, "on_dead_letter", "a dead-letter sink")?,
                }) as Arc<dyn DeadLetterSink>)
            })
            .transpose()?;
        let middleware: Vec<Arc<dyn AgentMiddleware>> = middleware
            .unwrap_or_default()
            .into_iter()
            .map(|hooks| {
                Arc::new(PyMiddleware {
                    hooks,
                    fallback: HookLoop::capture(py),
                }) as Arc<dyn AgentMiddleware>
            })
            .collect();
        // Capped exponential-backoff retry when either knob is set, else the
        // consumer's default policy (5 attempts from 200ms).
        let retry = match (retry_max_attempts, retry_base_delay_ms) {
            (None, None) => None,
            (attempts, delay) => Some(RetryPolicy::backoff(
                attempts.unwrap_or(5),
                Duration::from_millis(delay.unwrap_or(200)),
            )),
        };
        let respond_topic = respond_on.map(static_topic).transpose()?;
        let poll = poll_interval_ms.map(Duration::from_millis);
        let shutdown_grace = shutdown_grace_ms.map(Duration::from_millis);
        let verifier = verifier.map(PyKeyRegistry::snapshot);
        // Skills the agent advertises: a capability card published on start, the
        // same auto-advertise the Rust `Agent` builder does when capabilities are
        // set. An optional `health` ("healthy"/"degraded"/"unavailable") applies to
        // every advertised skill.
        let advertised_health = health.as_deref().map(parse_health);
        let capabilities = crate::registry::capabilities(capabilities.unwrap_or_default())?;
        if agent.is_none() && !capabilities.is_empty() {
            return Err(InvalidError::new_err(
                "capability advertising requires an agent identity",
            ));
        }
        let card = (!capabilities.is_empty()).then(|| laser_sdk::wire::agent::AgentCard {
            name: None,
            version: None,
            capabilities: capabilities
                .into_iter()
                .map(|mut capability| {
                    if advertised_health.is_some() {
                        capability.health = advertised_health;
                    }
                    capability
                })
                .collect(),
            ttl_micros: None,
        });
        if consolidate_every_ms == Some(0) {
            return Err(InvalidError::new_err(
                "consolidate_every_ms must be greater than zero",
            ));
        }
        let locals = get_current_locals(py)?;
        let consolidation = match (consolidate_every_ms, consolidator) {
            (Some(every), Some(callback)) => {
                let callback = PyHook::new(callback, "consolidate", "a consolidator")?;
                // The agent consolidates its own memory: its id when it has
                // one, every other scope field open.
                let own_scope = laser_sdk::memory::MemoryScope {
                    agent: agent.clone(),
                    ..laser_sdk::memory::MemoryScope::default()
                };
                let task = get_runtime().spawn(scope(locals.clone(), async move {
                    let mut tick = tokio::time::interval(Duration::from_millis(every));
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tick.tick().await;
                        let result = callback
                            .call(|py| {
                                (crate::memory::scope_dict(py, &own_scope)?,).into_pyobject(py)
                            })
                            .await;
                        if let Err(error) = result {
                            log::warn!("background consolidation pass failed: {error}");
                        }
                    }
                }));
                Some(task.abort_handle())
            }
            _ => None,
        };
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let advertise_id = agent.clone();
        // The inbox the agent advertises is the topic it consumes on.
        let presence_inbox = listen_on.clone();
        let join = get_runtime().spawn(scope(locals, async move {
            if let (Some(card), Some(advertise_id)) = (card, advertise_id) {
                if let Err(error) = laser.publish_card(advertise_id.clone(), &card).await {
                    log::warn!("failed to publish the agent capability card: {error}");
                }
                // Advertise the live inbox too, mirroring the Rust builder, so a
                // capability route resolves this agent without a fixed inbox. Best
                // effort: a server without the presence command is fine.
                let presence = laser_sdk::wire::agent::AgentPresence::new(advertise_id.wire_id())
                    .with_inbox(presence_inbox);
                if let Err(error) = laser.advertise_presence(&presence).await {
                    if matches!(error, LaserError::PresenceConflict { .. }) {
                        return Err(error);
                    }
                    log::warn!("inbox presence not advertised: {error}");
                }
            }
            ReliableConsumer::builder()
                .group(group)
                .maybe_agent(agent)
                .topic(listen_on)
                .maybe_respond_on(respond_topic)
                .maybe_poll_interval(poll)
                .maybe_shutdown_grace(shutdown_grace)
                .concurrency(concurrency)
                .maybe_max_queued_records(max_queued_records)
                .maybe_max_queued_bytes(max_queued_bytes)
                .understood_features(understood_features)
                .maybe_retry(retry)
                .warm_dedup(warm_dedup)
                .maybe_deduplicator(deduplicator)
                .maybe_on_dead_letter(dead_letter_sink)
                .middleware(middleware)
                .ack_on_pickup(ack_on_pickup)
                .maybe_signing_key(signing_key)
                .maybe_verifier(verifier)
                .maybe_operations(operations)
                .maybe_sessions(sessions)
                .build()
                .run(&laser, py_handler, ready_tx, shutdown_rx)
                .await
        }));
        Ok(PyAgentHandle {
            shutdown: Mutex::new(Some(shutdown_tx)),
            join: Mutex::new(Some(join)),
            ready: Mutex::new(Some(ready_rx)),
            consolidation,
        })
    }

    /// Send a directed task and await its reply up to `deadline_ms` (default
    /// 30000, the same as Rust and TypeScript). Route by capability with
    /// `skill`, or to one named agent with `agent=` (pass `skill=None`).
    /// `principal` requires the target's live connection to authenticate as
    /// that principal. `fixed_inbox` routes to a fixed topic (a server with no
    /// presence command). Omit it to resolve each agent's advertised inbox.
    /// `expire_if_not_consumed_ms` lets an unpicked task expire, so a caller can
    /// tell `not_consumed` from `timed_out` (the agent must emit pickup status
    /// with `ack_on_pickup`). `reply_on`, `conversation`, and `fence` mirror
    /// the Rust contract builder. `parent` runs the contract as a child
    /// session of that session id, in the tree rooted at `root` (the parent
    /// when omitted). Returns the terminal
    /// `Contract`, whose `Completed` and `Failed` variants carry the reply.
    #[pyo3(signature = (skill, payload, *, source, agent=None, deadline_ms=30_000, fixed_inbox=None, principal=None, expire_if_not_consumed_ms=None, reply_on=None, conversation=None, fence=None, parent=None, root=None, policy=None))]
    #[allow(clippy::too_many_arguments)]
    fn contract<'py>(
        &self,
        py: Python<'py>,
        skill: Option<String>,
        payload: Vec<u8>,
        source: String,
        agent: Option<String>,
        deadline_ms: u64,
        fixed_inbox: Option<String>,
        principal: Option<u32>,
        expire_if_not_consumed_ms: Option<u64>,
        reply_on: Option<String>,
        conversation: Option<String>,
        fence: Option<u64>,
        parent: Option<String>,
        root: Option<String>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = ContractRequest::new(
            skill,
            agent,
            payload,
            source,
            deadline_ms,
            fixed_inbox,
            principal,
            expire_if_not_consumed_ms,
            reply_on,
            conversation,
            fence,
            parent_pair(parent, root)?,
            policy,
        )?;
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let outcome = request.send(&laser).await?;
            Python::attach(|py| PyContract::from_rust(py, outcome))
        })
    }

    /// Scatter a directed task to every agent advertising `skill`, concurrently,
    /// and return the reply body of each that completed (a verifier or diagnostic
    /// panel). Unavailable and quarantined agents are excluded. `deadline_ms` is
    /// required and bounds the whole scatter.
    #[pyo3(signature = (skill, payload, *, source, deadline_ms, fixed_inbox=None, principal=None, policy=None))]
    #[allow(clippy::too_many_arguments)]
    fn scatter<'py>(
        &self,
        py: Python<'py>,
        skill: String,
        payload: Vec<u8>,
        source: String,
        deadline_ms: u64,
        fixed_inbox: Option<String>,
        principal: Option<u32>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = AgentId::new(source).map_err(|e| to_pyerr(e.into()))?;
        let ParsedRoutePolicy {
            policy,
            failure: route_failure,
        } = route_policy(policy)?;
        let mut selector = CapabilitySelector::new(skill, policy);
        if let Some(principal) = principal {
            selector = selector.principal(PrincipalId::new(principal));
        }
        let route = inbox_route(fixed_inbox)?;
        future_into_py(py, async move {
            let result = laser
                .scatter(
                    source,
                    &selector,
                    &payload,
                    &route,
                    Duration::from_millis(deadline_ms),
                )
                .await;
            route_result(result, &route_failure)
        })
    }

    /// Scatter like [`scatter`](Self::scatter), but return every contracted
    /// agent's terminal outcome, not only the completed replies, so an all-failed
    /// scatter is a report of failures rather than an empty list.
    #[pyo3(signature = (skill, payload, *, source, deadline_ms, fixed_inbox=None, principal=None, policy=None))]
    #[allow(clippy::too_many_arguments)]
    fn scatter_report<'py>(
        &self,
        py: Python<'py>,
        skill: String,
        payload: Vec<u8>,
        source: String,
        deadline_ms: u64,
        fixed_inbox: Option<String>,
        principal: Option<u32>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = AgentId::new(source).map_err(|e| to_pyerr(e.into()))?;
        let ParsedRoutePolicy {
            policy,
            failure: route_failure,
        } = route_policy(policy)?;
        let mut selector = CapabilitySelector::new(skill, policy);
        if let Some(principal) = principal {
            selector = selector.principal(PrincipalId::new(principal));
        }
        let route = inbox_route(fixed_inbox)?;
        future_into_py(py, async move {
            let report = laser
                .scatter_report(
                    source,
                    &selector,
                    &payload,
                    &route,
                    Duration::from_millis(deadline_ms),
                )
                .await;
            let report = route_result(report, &route_failure)?;
            Python::attach(|py| PyScatterReport::from_rust(py, report))
        })
    }

    /// Quarantine `agent` as `operator`: append the fact to the registry so every
    /// fused registry folds it and excludes the agent from routing.
    fn quarantine<'py>(
        &self,
        py: Python<'py>,
        operator: String,
        agent: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let operator = AgentId::new(operator).map_err(|e| to_pyerr(e.into()))?;
        let agent = AgentId::new(agent).map_err(|e| to_pyerr(e.into()))?;
        future_into_py(py, async move {
            laser.quarantine(operator, &agent).await.map_err(to_pyerr)
        })
    }

    /// Lift a prior quarantine on `agent` as `operator`: append the counterpart
    /// fact so every fused registry folds it and returns the agent to routing.
    fn unquarantine<'py>(
        &self,
        py: Python<'py>,
        operator: String,
        agent: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let operator = AgentId::new(operator).map_err(|e| to_pyerr(e.into()))?;
        let agent = AgentId::new(agent).map_err(|e| to_pyerr(e.into()))?;
        future_into_py(py, async move {
            laser.unquarantine(operator, &agent).await.map_err(to_pyerr)
        })
    }

    /// Open a [`Workflow`](crate::workflow::PyWorkflow): dependency-ordered steps
    /// over the coordination primitives, with budgets, verifier panels, fenced
    /// exclusivity, on-timeout reassignment, and saga compensation. Declare steps,
    /// then `await wf.run()`. The name is the orchestrator identity the run
    /// dispatches as. `fixed_inbox` routes every step to a fixed topic (a
    /// server with no presence command). Omit it to resolve advertised inboxes.
    #[pyo3(signature = (name, *, fixed_inbox=None))]
    fn workflow(&self, name: String, fixed_inbox: Option<String>) -> crate::workflow::PyWorkflow {
        crate::workflow::PyWorkflow::new(self.inner.clone(), name, fixed_inbox)
    }

    /// Replay a conversation's history off the log: read `topics` (default
    /// `agent.sessions`), order by timestamp, and apply a policy.
    /// `roles` keeps only messages from those agents. Otherwise the last
    /// `last_n` messages are kept (default 50). `token_budget` then trims the
    /// selection to an estimated token count. Returns the selected messages.
    /// `policy` replaces the shorthand controls. Replay bounds accept per-partition offsets and per-topic checkpoints.
    /// `across_subconversations=True` includes child conversations.
    #[pyo3(signature = (conversation_id, *, topics=None, last_n=None, roles=None, token_budget=None, policy=None, across_subconversations=false, from_offsets=None, from_checkpoint=None, to_checkpoint=None))]
    #[allow(clippy::too_many_arguments)]
    fn assemble_context<'py>(
        &self,
        py: Python<'py>,
        conversation_id: String,
        topics: Option<Vec<String>>,
        last_n: Option<usize>,
        roles: Option<Vec<String>>,
        token_budget: Option<usize>,
        policy: Option<&Bound<'_, PyAny>>,
        across_subconversations: bool,
        from_offsets: Option<BTreeMap<u32, u64>>,
        from_checkpoint: Option<&crate::session::PyCheckpoint>,
        to_checkpoint: Option<&crate::session::PyCheckpoint>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let conversation =
            ConversationId::from_str(&conversation_id).map_err(|e| to_pyerr(e.into()))?;
        let topics: Option<Vec<AgentTopic<'static>>> = topics
            .map(|names| {
                names
                    .into_iter()
                    .map(static_topic)
                    .collect::<PyResult<Vec<_>>>()
            })
            .transpose()?;
        let (policy, estimator_failure) = if let Some(policy) = policy {
            if last_n.is_some() || roles.is_some() || token_budget.is_some() {
                return Err(InvalidError::new_err(
                    "policy cannot be combined with last_n, roles, or token_budget",
                ));
            }
            let (policy, failure) = crate::context::context_policy(policy)?;
            (policy, Some(failure))
        } else {
            let mut policies: Vec<Box<dyn ContextPolicy>> = Vec::new();
            let has_roles = roles.is_some();
            if let Some(roles) = roles {
                let mut set = HashSet::new();
                for role in roles {
                    set.insert(AgentId::new(role).map_err(|e| to_pyerr(e.into()))?);
                }
                policies.push(Box::new(RoleFilter(set)));
            }
            if let Some(count) = last_n.or_else(|| (!has_roles).then_some(50)) {
                policies.push(Box::new(LastN(count)));
            }
            if let Some(budget) = token_budget {
                policies.push(Box::new(TokenBudget::new(budget)));
            }
            (Box::new(Chain(policies)) as Box<dyn ContextPolicy>, None)
        };
        let from_checkpoint = from_checkpoint.map(|checkpoint| checkpoint.inner().clone());
        let to_checkpoint = to_checkpoint.map(|checkpoint| checkpoint.inner().clone());
        future_into_py(py, async move {
            let messages = ContextAssembler::builder()
                .conversation_id(conversation)
                .maybe_topics(topics)
                .policy(policy)
                .across_subconversations(across_subconversations)
                .from_offsets(from_offsets.unwrap_or_default())
                .maybe_from_checkpoint(from_checkpoint)
                .maybe_to_checkpoint(to_checkpoint)
                .build()
                .assemble(&laser)
                .await
                .map_err(to_pyerr)?;
            if let Some(error) =
                estimator_failure.and_then(|slot| crate::context::take_failure(&slot))
            {
                return Err(error);
            }
            Ok(messages
                .into_iter()
                .map(crate::context::PyContextMessage::new)
                .collect::<Vec<_>>())
        })
    }
}

/// The context handed to a Python handler: reply / send / request / spawn a
/// sub-conversation, or reach the full `Laser` for everything else.
#[gen_stub_pyclass]
#[pyclass(name = "AgentCtx")]
pub struct PyAgentCtx {
    laser: Laser,
    session: laser_sdk::agent::Session,
    request_at: Option<laser_sdk::wire::agent::LogPosition>,
    agent: Option<AgentId>,
    respond_on: Option<String>,
    message: AgentMessage,
    signing_key: Option<Arc<laser_sdk::sign::SigningKey>>,
    inbox_route: InboxRoute,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAgentCtx {
    /// The message currently being handled.
    #[getter]
    fn message(&self) -> PyAgentMessage {
        PyAgentMessage::from_inner(self.message.clone())
    }

    /// The session of the handled record, written as this agent and acting on
    /// the handled record: graph writes take it as source and remembered
    /// items as origin. A lens only: it holds no lease and starts no
    /// heartbeat.
    fn session(&self) -> crate::session::PySession {
        crate::session::PySession::new(self.session.clone())
    }

    /// The full client, for operations the ctx helpers do not cover (kv, query, ...).
    fn laser(&self) -> PyLaser {
        PyLaser::from_inner(self.laser.clone())
    }

    /// A child conversation of the handled message, linked by parent / root ids.
    fn spawn_subconversation(&self) -> PyResult<PyProvenance> {
        let author = self.agent.as_ref().ok_or_else(|| {
            to_pyerr(LaserError::HandlerConfig(
                "spawn_subconversation: the agent has no id".to_owned(),
            ))
        })?;
        Ok(PyProvenance {
            inner: self
                .laser
                .spawn_subconversation(&self.message.provenance, author),
        })
    }

    /// Resolve an AGDX request by publishing a correlated AGDX `response` on
    /// `reply_topic`, so the caller (a bridge `tasks/get` or tool result, or an
    /// `Agdx.request_input`) completes. An agent spawned with a signing key signs
    /// the response, so a verifying interrupt caller accepts this agent's decision
    /// and no one else's. Requires the handled message to be an AGDX envelope
    /// carrying a correlation, and the agent to have an id.
    fn respond_input<'py>(
        &self,
        py: Python<'py>,
        reply_topic: String,
        response: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let agent = self.agent.clone();
        let envelope = self.message.envelope.clone();
        let response = payload_bytes(response)?;
        let signing_key = self.signing_key.clone();
        future_into_py(py, async move {
            let envelope = envelope.ok_or_else(|| {
                to_pyerr(LaserError::HandlerConfig(
                    "respond_input: the handled message is not an AGDX envelope".to_owned(),
                ))
            })?;
            let correlation = envelope.correlation.ok_or_else(|| {
                to_pyerr(LaserError::HandlerConfig(
                    "respond_input: the request carries no correlation".to_owned(),
                ))
            })?;
            let source = agent
                .ok_or_else(|| {
                    to_pyerr(LaserError::HandlerConfig(
                        "respond_input: the agent has no id".to_owned(),
                    ))
                })?
                .wire_id();
            let producer = laser.agdx(static_topic(reply_topic)?, source, envelope.conversation);
            let send = producer.respond(correlation, response);
            let send = match &signing_key {
                Some(key) => send.signed_by(key),
                None => send,
            };
            send.send().await.map(|_record_id| ()).map_err(to_pyerr)
        })
    }

    /// Reply on the agent's configured respond_on topic, chaining causality and
    /// routing back to the sender. A typed AGDX command gets a typed response:
    /// correlated, addressed to the requester, caused by the request's log
    /// position, and signed when the agent was spawned with a signing key. A
    /// plain request gets a plain reply matched by its correlation. Raises
    /// ConfigError if no respond_on was set.
    fn respond<'py>(
        &self,
        py: Python<'py>,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let payload = payload_bytes(payload)?;
        let agent = self.agent.clone();
        let message = self.message.clone();
        let respond_on = self.respond_on.clone();
        let signing_key = self.signing_key.clone();
        let request_at = self.request_at;
        future_into_py(py, async move {
            let name = respond_on.ok_or_else(|| to_pyerr(LaserError::NoRespondTopic))?;
            if let Some(envelope) = &message.envelope
                && let Some(correlation) = envelope.correlation
            {
                let source = agent
                    .as_ref()
                    .ok_or_else(|| {
                        to_pyerr(LaserError::HandlerConfig(
                            "a responding agent must have an id".to_owned(),
                        ))
                    })?
                    .wire_id();
                let producer = laser.agdx(static_topic(name)?, source, envelope.conversation);
                let mut send = producer
                    .respond(correlation, payload)
                    .with_target(envelope.source.clone())
                    .with_ancestry(envelope.parent, envelope.root);
                if let Some(record) = envelope.record {
                    send = send.with_cause(record, request_at);
                }
                if let Some(key) = &signing_key {
                    send = send.signed_by(key);
                }
                return send.send().await.map(|_| ()).map_err(to_pyerr);
            }
            let mut provenance = reply_provenance(&message, &agent);
            if let Some(source) = &message.provenance.agent {
                Router::to(source.clone()).apply(&mut provenance);
            }
            let id = topic_id(&name)?;
            laser
                .send_agent(AgentTopic::Custom(&id), payload, &provenance)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Where the handled record sits on the log, as a `LogPosition`, when the
    /// runtime knows it.
    #[getter]
    fn request_at(&self) -> Option<crate::agdx::PyLogPosition> {
        self.request_at
            .map(|inner| crate::agdx::PyLogPosition { inner })
    }

    /// Reply on an explicit topic, chained off the handled message.
    fn reply_on<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let payload = payload_bytes(payload)?;
        let agent = self.agent.clone();
        let message = self.message.clone();
        future_into_py(py, async move {
            let provenance = reply_provenance(&message, &agent);
            let id = topic_id(&topic)?;
            laser
                .send_agent(AgentTopic::Custom(&id), payload, &provenance)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Send to `topic` with an explicit provenance (no automatic causality).
    fn send<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        payload: &Bound<'_, PyAny>,
        provenance: &PyProvenance,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let payload = payload_bytes(payload)?;
        let provenance = provenance.inner.clone();
        future_into_py(py, async move {
            let id = topic_id(&topic)?;
            laser
                .send_agent(AgentTopic::Custom(&id), payload, &provenance)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Send a request and await its correlated reply (see Laser.request).
    #[pyo3(signature = (request_topic, reply_topic, payload, provenance, *, timeout_ms))]
    fn request<'py>(
        &self,
        py: Python<'py>,
        request_topic: String,
        reply_topic: String,
        payload: &Bound<'_, PyAny>,
        provenance: &PyProvenance,
        timeout_ms: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let payload = payload_bytes(payload)?;
        let provenance = provenance.inner.clone();
        future_into_py(py, async move {
            let request_id = topic_id(&request_topic)?;
            let reply_id = topic_id(&reply_topic)?;
            let reply = laser
                .request(
                    AgentTopic::Custom(&request_id),
                    AgentTopic::Custom(&reply_id),
                    payload,
                    &provenance,
                    duration_ms(timeout_ms, "timeout_ms")?,
                )
                .await
                .map_err(to_pyerr)?;
            Ok(PyAgentMessage::from_inner(reply))
        })
    }

    /// Fan out a task to every agent advertising `skill`, gathering replies under
    /// `policy` within `deadline_ms`, which is required. `policy` is `"require_all"` (default, wait
    /// for every branch), `"quorum"` (stop once `quorum` branches succeed), or
    /// `"best_effort"` (take whatever landed by the deadline). Replies land on
    /// this handler's own `respond_on` topic, so the agent must have been spawned
    /// with one. Branches follow the agent's inbox route (`spawn_agent`
    /// `fixed_inbox=`), and `principal` and `route_policy` refine the
    /// capability selector, like Rust's `CapabilitySelector`. Returns a
    /// `Gather` of attributed replies and failures. A target that resolves no inbox is a `failures` entry,
    /// never silently rerouted.
    #[pyo3(signature = (skill, payload, *, deadline_ms, policy="require_all", quorum=None, principal=None, route_policy=None))]
    #[allow(clippy::too_many_arguments)]
    fn fan_out<'py>(
        &self,
        py: Python<'py>,
        skill: String,
        payload: &Bound<'_, PyAny>,
        deadline_ms: u64,
        policy: &str,
        quorum: Option<usize>,
        principal: Option<u32>,
        route_policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ParsedRoutePolicy {
            policy: route_policy,
            failure: route_failure,
        } = crate::agent_runtime::route_policy(route_policy)?;
        let laser = self.laser.clone();
        let message = self.message.clone();
        let agent = self.agent.clone();
        let respond_on = self.respond_on.clone().map(static_topic).transpose()?;
        let route = self.inbox_route.clone();
        let payload = payload_bytes(payload)?;
        let gather_policy = parse_gather_policy(policy, quorum)?;
        future_into_py(py, async move {
            let mut selector = CapabilitySelector::new(skill, route_policy);
            if let Some(principal) = principal {
                selector = selector.principal(PrincipalId::new(principal));
            }
            let ctx = laser_sdk::testing::agent_ctx(&laser, &message, agent, respond_on, route);
            let gather = ctx
                .fan_out(
                    selector,
                    payload,
                    gather_policy,
                    Duration::from_millis(deadline_ms),
                )
                .await;
            let gather = route_result(gather, &route_failure)?;
            Python::attach(|py| PyGather::from_rust(py, gather))
        })
    }

    /// Pause this handler on a human decision: publish `prompt` as an interrupt on
    /// the human-input topic and await the approver's correlated reply on
    /// `reply_topic`, up to `timeout_ms`, chained to the handled conversation.
    /// Returns the decision body on approval, or raises on rejection (the
    /// approver answers with `respond_input`). The agent must have an id.
    #[pyo3(signature = (reply_topic, prompt, *, timeout_ms))]
    fn approval_gate<'py>(
        &self,
        py: Python<'py>,
        reply_topic: String,
        prompt: &Bound<'_, PyAny>,
        timeout_ms: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let message = self.message.clone();
        let agent = self.agent.clone();
        let respond_on = self.respond_on.clone().map(static_topic).transpose()?;
        let prompt = payload_bytes(prompt)?;
        future_into_py(py, async move {
            let ctx = laser_sdk::testing::agent_ctx(
                &laser,
                &message,
                agent,
                respond_on,
                InboxRoute::default(),
            );
            let decision = ctx
                .approval_gate(
                    static_topic(reply_topic)?,
                    prompt,
                    duration_ms(timeout_ms, "timeout_ms")?,
                )
                .await
                .map_err(to_pyerr)?;
            Ok(decision)
        })
    }
}

/// Build a synthetic `AgentMessage` for a handler unit test: a plain
/// (non-envelope) message carrying `payload` and `provenance`, with no live
/// consumer or server involved. Feed it to your handler directly (`await
/// handle(ctx, message)`) to exercise it in isolation.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn agent_message(
    payload: &Bound<'_, PyAny>,
    provenance: &PyProvenance,
) -> PyResult<PyAgentMessage> {
    let payload = payload_bytes(payload)?;
    Ok(PyAgentMessage::from_inner(
        laser_sdk::testing::agent_message(payload, provenance.inner.clone()),
    ))
}

/// Build an `AgentCtx` for a handler unit test, over a caller-owned `laser` and
/// `message`, so a test can call `await handle(ctx, message)` directly without
/// spawning a live consumer. `laser` only needs to be live for whatever ctx
/// helpers the handler actually calls (`respond`/`fan_out`/...). A handler that
/// only reads its message needs no server at all.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (laser, message, *, agent=None, respond_on=None, fixed_inbox=None, signing_key=None))]
pub fn agent_ctx(
    laser: &PyLaser,
    message: &PyAgentMessage,
    agent: Option<String>,
    respond_on: Option<String>,
    fixed_inbox: Option<String>,
    signing_key: Option<&crate::sign::PySigningKey>,
) -> PyResult<PyAgentCtx> {
    let agent = agent
        .map(AgentId::new)
        .transpose()
        .map_err(|e| to_pyerr(e.into()))?;
    let mut session = laser
        .inner
        .sessions()
        .open(message.inner.provenance.conversation_id);
    if let Some(agent) = &agent {
        session = session.as_agent(agent.wire_id());
    }
    Ok(PyAgentCtx {
        laser: laser.inner.clone(),
        session,
        request_at: None,
        agent,
        respond_on,
        message: message.inner.clone(),
        signing_key: signing_key.map(|key| key.inner.clone()),
        inbox_route: inbox_route(fixed_inbox)?,
    })
}

/// Owns a spawned agent. Await `ready()` before publishing, `shutdown()` to stop
/// and surface any consumer error, or `abort()` to stop immediately.
#[gen_stub_pyclass]
#[pyclass(name = "AgentHandle")]
pub struct PyAgentHandle {
    shutdown: Mutex<Option<oneshot::Sender<()>>>,
    join: Mutex<Option<JoinHandle<Result<(), LaserError>>>>,
    ready: Mutex<Option<oneshot::Receiver<()>>>,
    consolidation: Option<tokio::task::AbortHandle>,
}

// Dropping the handle without `shutdown()` or `abort()` would otherwise strand
// the consumer task: it keeps polling, handling, and acking with its stop signal
// gone. Collection of the Python object is the last chance to stop it.
impl Drop for PyAgentHandle {
    fn drop(&mut self) {
        if let Some(task) = &self.consolidation {
            task.abort();
        }
        // Prefer the graceful signal so an in-flight handler still finishes.
        // Abort only when the signal is already spent and the task is somehow
        // still owned here, where there is nothing left to wind it down.
        let signalled = self
            .shutdown
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .map(|sender| sender.send(()))
            .is_some();
        if signalled {
            return;
        }
        if let Some(join) = self
            .join
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            join.abort();
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAgentHandle {
    /// Wait until the agent has joined its group and is polling.
    fn ready<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let receiver = self.ready.lock().expect("ready lock").take();
        future_into_py(py, async move {
            if let Some(receiver) = receiver {
                receiver.await.map_err(|_| {
                    to_pyerr(LaserError::HandlerConfig(
                        "agent stopped before ready".to_owned(),
                    ))
                })?;
            }
            Ok(())
        })
    }

    /// Signal the agent to stop, wait for it, and surface any consumer error.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if let Some(task) = &self.consolidation {
            task.abort();
        }
        if let Some(sender) = self.shutdown.lock().expect("shutdown lock").take() {
            let _ = sender.send(());
        }
        let join = self.join.lock().expect("join lock").take();
        future_into_py(py, async move {
            match join {
                Some(join) => match join.await {
                    Ok(result) => result.map_err(to_pyerr),
                    Err(error) => Err(to_pyerr(LaserError::HandlerConfig(error.to_string()))),
                },
                None => Ok(()),
            }
        })
    }

    /// Wait for the agent to finish (it runs until its consumer ends or errors).
    fn join<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let consolidation = self.consolidation.clone();
        let join = self.join.lock().expect("join lock").take();
        future_into_py(py, async move {
            let result = match join {
                Some(join) => match join.await {
                    Ok(result) => result.map_err(to_pyerr),
                    Err(error) => Err(to_pyerr(LaserError::HandlerConfig(error.to_string()))),
                },
                None => Ok(()),
            };
            if let Some(task) = consolidation {
                task.abort();
            }
            result
        })
    }

    /// Abort the agent's task immediately, without waiting.
    fn abort(&self) {
        if let Some(task) = &self.consolidation {
            task.abort();
        }
        if let Some(join) = self.join.lock().expect("join lock").as_ref() {
            join.abort();
        }
    }

    /// Enter `async with`: wait until the agent is ready, then yield the handle.
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let receiver = slf.borrow(py).ready.lock().expect("ready lock").take();
        let handle = slf.clone_ref(py);
        future_into_py(py, async move {
            if let Some(receiver) = receiver {
                receiver.await.map_err(|_| {
                    to_pyerr(LaserError::HandlerConfig(
                        "agent stopped before ready".to_owned(),
                    ))
                })?;
            }
            Ok(handle)
        })
    }

    /// Exit `async with`: stop the agent and surface any consumer error. Returns
    /// `False` so an exception in the body is not suppressed.
    #[pyo3(signature = (_exc_type, _exc_value, _traceback))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if let Some(task) = &self.consolidation {
            task.abort();
        }
        if let Some(sender) = self.shutdown.lock().expect("shutdown lock").take() {
            let _ = sender.send(());
        }
        let join = self.join.lock().expect("join lock").take();
        future_into_py(py, async move {
            if let Some(join) = join {
                match join.await {
                    Ok(result) => result.map_err(to_pyerr)?,
                    Err(error) => {
                        return Err(to_pyerr(LaserError::HandlerConfig(error.to_string())));
                    }
                }
            }
            Ok(false)
        })
    }
}

// One contract call's options, validated up front so a bad argument raises
// before any await.
pub(crate) struct ContractRequest {
    router: Router,
    payload: Vec<u8>,
    source: AgentId,
    deadline: Duration,
    route: InboxRoute,
    expire_if_not_consumed: Option<Duration>,
    reply_on: Option<AgentTopic<'static>>,
    conversation: Option<laser_sdk::types::ConversationId>,
    fence: Option<u64>,
    parent: Option<(
        laser_sdk::types::ConversationId,
        laser_sdk::types::ConversationId,
    )>,
    route_failure: RouteFailure,
}

// The `parent=`/`root=` keywords of a contract: the root defaults to the
// parent, and a root alone is refused.
pub(crate) fn parent_pair(
    parent: Option<String>,
    root: Option<String>,
) -> PyResult<
    Option<(
        laser_sdk::types::ConversationId,
        laser_sdk::types::ConversationId,
    )>,
> {
    let parse = |value: &str| {
        laser_sdk::types::ConversationId::from_str(value).map_err(|e| to_pyerr(e.into()))
    };
    match (parent, root) {
        (None, None) => Ok(None),
        (None, Some(_)) => Err(crate::errors::InvalidError::new_err("root= needs parent=")),
        (Some(parent), root) => {
            let parent = parse(&parent)?;
            let root = match root {
                Some(root) => parse(&root)?,
                None => parent,
            };
            Ok(Some((parent, root)))
        }
    }
}

impl ContractRequest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        skill: Option<String>,
        agent: Option<String>,
        payload: Vec<u8>,
        source: String,
        deadline_ms: u64,
        fixed_inbox: Option<String>,
        principal: Option<u32>,
        expire_if_not_consumed_ms: Option<u64>,
        reply_on: Option<String>,
        conversation: Option<String>,
        fence: Option<u64>,
        parent: Option<(
            laser_sdk::types::ConversationId,
            laser_sdk::types::ConversationId,
        )>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let ParsedRoutePolicy {
            policy,
            failure: route_failure,
        } = route_policy(policy)?;
        let router = match (skill, agent) {
            (Some(skill), None) => {
                let mut selector = CapabilitySelector::new(skill, policy);
                if let Some(principal) = principal {
                    selector = selector.principal(PrincipalId::new(principal));
                }
                Router::ToCapable(selector)
            }
            (None, Some(agent)) => {
                let agent = AgentId::new(agent).map_err(|e| to_pyerr(e.into()))?;
                match principal {
                    Some(principal) => Router::to_principal(agent, PrincipalId::new(principal)),
                    None => Router::to(agent),
                }
            }
            _ => {
                return Err(crate::errors::InvalidError::new_err(
                    "pass exactly one of skill or agent=",
                ));
            }
        };
        let conversation = conversation
            .map(|value| {
                laser_sdk::types::ConversationId::from_str(&value).map_err(|e| to_pyerr(e.into()))
            })
            .transpose()?;
        Ok(Self {
            router,
            payload,
            source: AgentId::new(source).map_err(|e| to_pyerr(e.into()))?,
            deadline: Duration::from_millis(deadline_ms),
            route: inbox_route(fixed_inbox)?,
            expire_if_not_consumed: expire_if_not_consumed_ms.map(Duration::from_millis),
            reply_on: reply_on.map(static_topic).transpose()?,
            conversation,
            fence,
            parent,
            route_failure,
        })
    }

    pub(crate) async fn send(self, laser: &Laser) -> PyResult<Contract> {
        let mut builder = laser
            .contract(self.router)
            .from(self.source)
            .payload(self.payload)
            .inbox_route(self.route)
            .deadline(self.deadline);
        if let Some(expiry) = self.expire_if_not_consumed {
            builder = builder.expire_if_not_consumed(expiry);
        }
        if let Some(topic) = self.reply_on {
            builder = builder.reply_on(topic);
        }
        if let Some(conversation) = self.conversation {
            builder = builder.conversation(conversation);
        }
        if let Some(fence) = self.fence {
            builder = builder.fence(fence);
        }
        if let Some((parent, root)) = self.parent {
            builder = builder.parent(parent, root);
        }
        route_result(builder.send().await, &self.route_failure)
    }
}

/// The outcome of a contract: a directed request to one agent resolved to one
/// terminal state. `Completed` and `Failed` carry the reply.
#[gen_stub_pyclass_complex_enum]
#[pyclass(name = "Contract", frozen)]
pub enum PyContract {
    /// The target replied within the deadline (a non-error reply).
    Completed(Py<PyAgentMessage>),
    /// The target replied with a terminal `error`.
    Failed(Py<PyAgentMessage>),
    /// No pickup acknowledgment landed within the consumption expiry.
    NotConsumed(),
    /// No terminal reply landed within the completion deadline.
    TimedOut(),
}

impl PyContract {
    pub(crate) fn from_rust(py: Python<'_>, outcome: Contract) -> PyResult<Py<Self>> {
        let contract = match outcome {
            Contract::Completed(reply) => {
                Self::Completed(Py::new(py, PyAgentMessage::from_inner(reply))?)
            }
            Contract::Failed(reply) => {
                Self::Failed(Py::new(py, PyAgentMessage::from_inner(reply))?)
            }
            Contract::NotConsumed => Self::NotConsumed(),
            Contract::TimedOut => Self::TimedOut(),
        };
        contract.into_pyobject(py).map(Bound::unbind)
    }
}

/// One agent's outcome in a `ScatterReport`.
#[gen_stub_pyclass]
#[pyclass(name = "ScatterOutcome", frozen)]
pub struct PyScatterOutcome {
    agent: String,
    result: Py<PyAny>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyScatterOutcome {
    /// The agent this branch was contracted to.
    #[getter]
    fn agent(&self) -> String {
        self.agent.clone()
    }

    /// Its terminal `Contract`, or the exception that failed the branch.
    #[getter]
    fn result(&self, py: Python<'_>) -> Py<PyAny> {
        self.result.clone_ref(py)
    }

    fn __repr__(&self) -> String {
        format!("ScatterOutcome(agent={})", self.agent)
    }
}

/// Every contracted agent's terminal state from `Laser.scatter_report`, so an
/// all-failed scatter is a report of failures rather than an empty success.
#[gen_stub_pyclass]
#[pyclass(name = "ScatterReport", frozen)]
pub struct PyScatterReport {
    outcomes: Vec<Py<PyScatterOutcome>>,
}

impl PyScatterReport {
    pub(crate) fn from_rust(py: Python<'_>, report: ScatterReport) -> PyResult<Self> {
        let outcomes = report
            .outcomes
            .into_iter()
            .map(|outcome| {
                let result = match outcome.result {
                    Ok(contract) => PyContract::from_rust(py, contract)?.into_any(),
                    Err(error) => to_pyerr(error).into_value(py).into_any(),
                };
                Py::new(
                    py,
                    PyScatterOutcome {
                        agent: outcome.agent.to_string(),
                        result,
                    },
                )
            })
            .collect::<PyResult<_>>()?;
        Ok(Self { outcomes })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyScatterReport {
    /// One entry per contracted agent, in completion order.
    #[getter]
    fn outcomes(&self, py: Python<'_>) -> Vec<Py<PyScatterOutcome>> {
        self.outcomes
            .iter()
            .map(|outcome| outcome.clone_ref(py))
            .collect()
    }

    /// The agents that completed, each with its reply.
    fn completed(&self, py: Python<'_>) -> Vec<(String, Py<PyAgentMessage>)> {
        self.outcomes
            .iter()
            .filter_map(|outcome| {
                let outcome = outcome.get();
                match outcome.result.bind(py).cast::<PyContract>().ok()?.get() {
                    PyContract::Completed(reply) => {
                        Some((outcome.agent.clone(), reply.clone_ref(py)))
                    }
                    _ => None,
                }
            })
            .collect()
    }

    /// The agents whose branch errored, each with its exception. A
    /// non-completing terminal contract is in `outcomes`, not here.
    fn failures(&self, py: Python<'_>) -> Vec<(String, Py<PyAny>)> {
        self.outcomes
            .iter()
            .filter_map(|outcome| {
                let outcome = outcome.get();
                let result = outcome.result.bind(py);
                (!result.is_instance_of::<PyContract>())
                    .then(|| (outcome.agent.clone(), result.clone().unbind()))
            })
            .collect()
    }

    fn __len__(&self) -> usize {
        self.outcomes.len()
    }
}

/// The outcome of `AgentCtx.fan_out`: the successful replies and the failed
/// branches, each attributed to its agent.
#[gen_stub_pyclass]
#[pyclass(name = "Gather", frozen)]
pub struct PyGather {
    ok: Vec<(String, Py<PyAgentMessage>)>,
    failures: Vec<(String, Py<PyAny>)>,
}

impl PyGather {
    pub(crate) fn from_rust(py: Python<'_>, gather: Gather) -> PyResult<Self> {
        let ok = gather
            .ok
            .into_iter()
            .map(|(agent, reply)| {
                Ok((
                    agent.to_string(),
                    Py::new(py, PyAgentMessage::from_inner(reply))?,
                ))
            })
            .collect::<PyResult<_>>()?;
        let failures = gather
            .failures
            .into_iter()
            .map(|(agent, error)| (agent.to_string(), to_pyerr(error).into_value(py).into_any()))
            .collect();
        Ok(Self { ok, failures })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyGather {
    /// The agents that replied, each with its reply.
    #[getter]
    fn ok(&self, py: Python<'_>) -> Vec<(String, Py<PyAgentMessage>)> {
        self.ok
            .iter()
            .map(|(agent, reply)| (agent.clone(), reply.clone_ref(py)))
            .collect()
    }

    /// The agents whose branch failed (no inbox, request error, timeout), each
    /// with its exception.
    #[getter]
    fn failures(&self, py: Python<'_>) -> Vec<(String, Py<PyAny>)> {
        self.failures
            .iter()
            .map(|(agent, error)| (agent.clone(), error.clone_ref(py)))
            .collect()
    }

    /// The reply messages alone, dropping agent attribution.
    fn replies(&self, py: Python<'_>) -> Vec<Py<PyAgentMessage>> {
        self.ok
            .iter()
            .map(|(_, reply)| reply.clone_ref(py))
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "Gather(ok={}, failures={})",
            self.ok.len(),
            self.failures.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{PyContract, PyGather, PyScatterReport};
    use laser_sdk::LaserError;
    use laser_sdk::agent::{Contract, Gather, ScatterOutcome, ScatterReport};
    use laser_sdk::provenance::Provenance;
    use laser_sdk::testing::agent_message;
    use laser_sdk::types::{AgentId, ConversationId};
    use pyo3::prelude::*;

    fn provenance() -> Provenance {
        Provenance::builder()
            .conversation_id(ConversationId::new())
            .build()
    }

    fn agent(name: &str) -> AgentId {
        AgentId::new(name).expect("valid agent id")
    }

    #[test]
    fn given_rust_contracts_when_converted_then_should_preserve_python_variant_types() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let outcomes = [
                (
                    "Completed",
                    Contract::Completed(agent_message(b"done".to_vec(), provenance())),
                ),
                (
                    "Failed",
                    Contract::Failed(agent_message(b"failed".to_vec(), provenance())),
                ),
                ("NotConsumed", Contract::NotConsumed),
                ("TimedOut", Contract::TimedOut),
            ];
            for (variant, outcome) in outcomes {
                let outcome = PyContract::from_rust(py, outcome)?;
                let variant_type = py.get_type::<PyContract>().getattr(variant)?;
                assert!(outcome.bind(py).as_any().is_instance(&variant_type)?);
            }
            Ok(())
        })
        .expect("contract variants convert");
    }

    #[test]
    fn given_a_mixed_scatter_when_reported_then_should_split_completed_from_failures() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let report = ScatterReport {
                outcomes: vec![
                    ScatterOutcome {
                        agent: agent("alpha"),
                        result: Ok(Contract::Completed(agent_message(
                            b"done".to_vec(),
                            provenance(),
                        ))),
                    },
                    ScatterOutcome {
                        agent: agent("beta"),
                        result: Ok(Contract::TimedOut),
                    },
                    ScatterOutcome {
                        agent: agent("gamma"),
                        result: Err(LaserError::Timeout("reply")),
                    },
                ],
            };
            let report = PyScatterReport::from_rust(py, report)?;
            assert_eq!(report.outcomes(py).len(), 3);
            let completed = report.completed(py);
            assert_eq!(completed.len(), 1);
            assert_eq!(completed[0].0, "alpha");
            assert_eq!(completed[0].1.get().inner.body(), b"done");
            let failures = report.failures(py);
            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].0, "gamma");
            assert!(
                failures[0]
                    .1
                    .bind(py)
                    .is_instance_of::<pyo3::exceptions::PyException>()
            );
            let timed_out = report.outcomes(py)[1].get().result.clone_ref(py);
            assert!(matches!(
                timed_out.bind(py).cast::<PyContract>()?.get(),
                PyContract::TimedOut()
            ));
            Ok(())
        })
        .expect("scatter report converts");
    }

    #[test]
    fn given_a_gather_when_converted_then_should_keep_attribution_and_replies() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let gather = Gather {
                ok: vec![(
                    agent("alpha"),
                    agent_message(b"scan".to_vec(), provenance()),
                )],
                failures: vec![(agent("beta"), LaserError::Timeout("reply"))],
            };
            let gather = PyGather::from_rust(py, gather)?;
            assert_eq!(gather.ok(py)[0].0, "alpha");
            assert_eq!(gather.replies(py)[0].get().inner.body(), b"scan");
            assert_eq!(gather.failures(py)[0].0, "beta");
            Ok(())
        })
        .expect("gather converts");
    }
}

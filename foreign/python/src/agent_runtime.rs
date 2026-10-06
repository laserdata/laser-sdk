use crate::agent::{PyAgentMessage, PyProvenance};
use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{duration_seconds, payload_bytes, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use crate::sign::{PyKeyRegistry, PySigningKey};
use async_trait::async_trait;
use iggy::prelude::Identifier;
use laser_sdk::LaserError;
use laser_sdk::agent::{
    AgentCtx, AgentHandler, AgentMessage, AgentMiddleware, CapabilitySelector, ConcurrencyPolicy,
    Contract, DeadLetterSink, Deduplicator, GatherPolicy, InboxRoute, ReliableConsumer,
    RetryPolicy, RouteCandidate, RoutePolicy, RouteScorer, Router, SlidingWindow,
};
use laser_sdk::context::{Chain, ContextAssembler, ContextPolicy, LastN, RoleFilter, TokenBudget};
use laser_sdk::laser::Laser;
use laser_sdk::provenance::{AgentTopic, Provenance};
use laser_sdk::types::{AgentId, ConversationId, PrincipalId};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{get_current_locals, get_runtime, into_future, scope};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
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
        "agent.commands" => return Ok(AgentTopic::Commands),
        "agent.responses" => return Ok(AgentTopic::Responses),
        "agent.tool_calls" => return Ok(AgentTopic::ToolCalls),
        "agent.tool_results" => return Ok(AgentTopic::ToolResults),
        "agent.llm_io" => return Ok(AgentTopic::LlmIo),
        "agent.human_input" => return Ok(AgentTopic::HumanInput),
        "agent.audit" => return Ok(AgentTopic::Audit),
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

struct PyRouteScorer {
    callback: Py<PyAny>,
    failure: RouteFailure,
}

impl RouteScorer for PyRouteScorer {
    fn select(&self, skill_id: &str, candidates: &[RouteCandidate<'_>]) -> Option<usize> {
        let result = Python::attach(|py| {
            let view = pyo3::types::PyList::empty(py);
            for candidate in candidates {
                let row = pyo3::types::PyDict::new(py);
                row.set_item("agent", candidate.agent.as_str())?;
                row.set_item("card", crate::registry::card_to_py(py, candidate.card)?)?;
                row.set_item(
                    "capability",
                    candidate
                        .capability
                        .as_ref()
                        .map(|value| ser_to_py(py, value))
                        .transpose()?,
                )?;
                view.append(row)?;
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

fn route_result<T>(result: Result<T, LaserError>, failure: &RouteFailure) -> PyResult<T> {
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

// The Rust handler that drives a Python `async def handle(ctx, message)`
// callback. Runs inside the scoped consumer task, so the captured event loop is
// in scope and `into_future` schedules the coroutine on it.
struct PyHandler {
    callback: Py<PyAny>,
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
            agent: self.agent.clone(),
            respond_on: self.respond_on.clone(),
            message: message.clone(),
            signing_key: self.signing_key.clone(),
            inbox_route: self.inbox_route.clone(),
        };
        crate::memory::call_hook_cancellable(|py| {
            self.callback
                .bind(py)
                .call1((py_ctx, py_message))
                .map(Bound::unbind)
        })
        .await
        .map_err(crate::errors::from_callback_error)?;
        Ok(())
    }
}

// A `Deduplicator` backed by a Python `async def observe(key) -> bool` callback.
// Runs inside the scoped consumer task, so the captured loop schedules the
// coroutine. A callback that raises or returns a non-bool is treated as "new"
// (return true), so dedup never silently drops a message on a callback fault -
// but the fault is logged (via the pyo3-log bridge) rather than swallowed, so a
// persistently broken deduplicator is observable, not a silent no-op.
struct PyDeduplicator {
    callback: Py<PyAny>,
}

#[async_trait]
impl Deduplicator for PyDeduplicator {
    async fn observe(&self, key: &str) -> bool {
        let key = key.to_owned();
        let future = Python::attach(|py| -> PyResult<_> {
            let coroutine = self.callback.bind(py).call1((key,))?;
            into_future(coroutine)
        });
        match future {
            Ok(future) => match future.await {
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
            },
            Err(error) => {
                log::warn!("dedup callback could not be scheduled, treating as new: {error}");
                true
            }
        }
    }
}

// The sink receives the complete capsule and the typed publication failure.
struct PyDeadLetterSink {
    callback: Py<PyAny>,
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
        let result = crate::memory::call_hook_cancellable(|py| {
            let capsule = ser_to_py(py, capsule)?;
            let publish_error = publish_result
                .as_ref()
                .err()
                .map(crate::errors::to_pyerr_ref);
            self.callback
                .bind(py)
                .call1((
                    py_message,
                    capsule,
                    publish_error.as_ref().map(|error| error.value(py)),
                ))
                .map(Bound::unbind)
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
}

#[async_trait]
impl AgentMiddleware for PyMiddleware {
    async fn before_handle(&self, message: &AgentMessage) -> Result<(), LaserError> {
        let py_message = PyAgentMessage::from_inner(message.clone());
        crate::memory::call_hook_cancellable(|py| {
            let bound = self.hooks.bind(py);
            if !bound.hasattr("before_handle")? {
                return Ok(py.None());
            }
            bound
                .call_method1("before_handle", (py_message,))
                .map(Bound::unbind)
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
        let result = crate::memory::call_hook_cancellable(|py| {
            let bound = self.hooks.bind(py);
            if !bound.hasattr("after_handle")? {
                return Ok(py.None());
            }
            let outcome = pyo3::types::PyDict::new(py);
            outcome.set_item("ok", result.is_ok())?;
            let error = result.as_ref().err().map(crate::errors::to_pyerr_ref);
            outcome.set_item("error", error.as_ref().map(|error| error.value(py)))?;
            bound
                .call_method1("after_handle", (py_message, outcome, attempt))
                .map(Bound::unbind)
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
    /// `listen_on` and drive `handler` (an `async def handle(ctx, message)`) for
    /// each message, with at-least-once delivery, dedup, retry, and DLQ. Pass
    /// `dedup` (an `async def observe(key) -> bool`) for a custom, e.g. durable,
    /// deduplicator, or `dedup_window` to size the default in-memory window in
    /// keys. `max_partitions` runs one ordered worker lane per partition up
    /// to that many concurrent lanes (omit for strict serial). `shutdown_grace_ms`
    /// bounds how long a graceful stop waits for the in-flight message.
    /// `dead_letter(message, capsule, publish_error)` receives the complete capsule dictionary and a typed SDK exception when publication fails.
    /// `middleware` has optional `before_handle(message)` and `after_handle(message, result, attempt)` hooks.
    /// The result dictionary contains `ok` and `error`. Hooks can return directly or through an awaitable.
    /// `governor` (an object with `async def decide(action) ->
    /// ActionDecision`) governs everything the handler publishes, applied under
    /// `governor_mode` (`"enforce"` | `"observe"`), replacing any
    /// connection-level governor for this agent. Returns a handle to await
    /// readiness and stop it. Requires a default stream.
    /// `fixed_inbox` sets the handler's default fan-out route. `governor_retention=(capacity, idle_ttl_secs)` bounds evidence heads.
    /// Capabilities accept skill names or descriptor dicts. Consolidation requires both `consolidate_every_ms` and `consolidator`.
    /// Shutdown cancels the active asynchronous consolidation callback and stops new passes.
    /// `agent_id=None` opens an unscoped reliable consumer and requires `consumer_group`. It cannot advertise capabilities.
    #[pyo3(signature = (agent_id, listen_on, handler, *, consumer_group=None, respond_on=None, fixed_inbox=None, poll_interval_ms=None, warm_dedup=false, dedup=None, dedup_window=None, consolidate_every_ms=None, consolidator=None, capabilities=None, ack_on_pickup=false, health=None, max_partitions=None, max_queued_records=None, max_queued_bytes=None, understood_features=0, shutdown_grace_ms=None, dead_letter=None, middleware=None, retry_max_attempts=None, retry_base_delay_ms=None, governor=None, governor_mode="enforce", governor_retention=None, signing_key=None, verifier=None))]
    #[allow(clippy::too_many_arguments)]
    fn spawn_agent(
        &self,
        py: Python<'_>,
        agent_id: Option<String>,
        listen_on: String,
        handler: Py<PyAny>,
        consumer_group: Option<String>,
        respond_on: Option<String>,
        fixed_inbox: Option<String>,
        poll_interval_ms: Option<u64>,
        warm_dedup: bool,
        dedup: Option<Py<PyAny>>,
        dedup_window: Option<usize>,
        consolidate_every_ms: Option<u64>,
        consolidator: Option<Py<PyAny>>,
        capabilities: Option<Vec<Bound<'_, PyAny>>>,
        ack_on_pickup: bool,
        health: Option<String>,
        max_partitions: Option<usize>,
        max_queued_records: Option<usize>,
        max_queued_bytes: Option<usize>,
        understood_features: u64,
        shutdown_grace_ms: Option<u64>,
        dead_letter: Option<Py<PyAny>>,
        middleware: Option<Vec<Py<PyAny>>>,
        retry_max_attempts: Option<u32>,
        retry_base_delay_ms: Option<u64>,
        governor: Option<Py<PyAny>>,
        governor_mode: &str,
        governor_retention: Option<(usize, f64)>,
        signing_key: Option<&PySigningKey>,
        verifier: Option<&PyKeyRegistry>,
    ) -> PyResult<PyAgentHandle> {
        let agent = agent_id
            .map(AgentId::new)
            .transpose()
            .map_err(|e| to_pyerr(e.into()))?;
        // A per-agent governor re-scopes the agent's `Laser`, so everything the
        // handler publishes through its ctx is governed (mirrors the Rust
        // `Agent::builder().governor(..)`).
        let retention = governor_retention
            .map(|(capacity, idle_ttl_secs)| {
                Ok::<_, PyErr>(laser_sdk::govern::GovernorRetention {
                    capacity,
                    idle_ttl: duration_seconds(idle_ttl_secs, "governor idle ttl")?,
                })
            })
            .transpose()?;
        let laser = match governor {
            Some(hooks) => {
                let governor = Arc::new(crate::govern::PyActionGovernor { hooks });
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
            callback: handler,
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
            (Some(callback), None) => Some(Box::new(PyDeduplicator { callback })),
            (None, Some(capacity)) => Some(Box::new(SlidingWindow::new(capacity))),
            (None, None) => None,
        };
        // One worker lane per partition when `max_partitions` is set (concurrent
        // across partitions, strictly ordered within one), else strict serial.
        let concurrency = match max_partitions {
            Some(max_partitions) => ConcurrencyPolicy::SerialPerPartition { max_partitions },
            None => ConcurrencyPolicy::Serial,
        };
        let dead_letter_sink: Option<std::sync::Arc<dyn DeadLetterSink>> =
            dead_letter.map(|callback| {
                std::sync::Arc::new(PyDeadLetterSink { callback })
                    as std::sync::Arc<dyn DeadLetterSink>
            });
        let middleware: Vec<std::sync::Arc<dyn AgentMiddleware>> = middleware
            .unwrap_or_default()
            .into_iter()
            .map(|hooks| {
                std::sync::Arc::new(PyMiddleware { hooks }) as std::sync::Arc<dyn AgentMiddleware>
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
                let task = get_runtime().spawn(scope(locals.clone(), async move {
                    let mut tick = tokio::time::interval(Duration::from_millis(every));
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tick.tick().await;
                        let result = crate::memory::call_hook_cancellable(|py| {
                            callback
                                .bind(py)
                                .call_method1("consolidate", (pyo3::types::PyDict::new(py),))
                                .map(Bound::unbind)
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
    /// with `ack_on_pickup`). `reply_on`, `conversation`, `fence`, and
    /// `registered` mirror the Rust contract builder. Returns the reply body,
    /// or `None` for any outcome other than completed. Use `contract_report`
    /// for the state.
    #[pyo3(signature = (skill, payload, *, source, agent=None, deadline_ms=30_000, fixed_inbox=None, principal=None, expire_if_not_consumed_ms=None, reply_on=None, conversation=None, fence=None, registered=false, policy=None))]
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
        registered: bool,
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
            registered,
            policy,
        )?;
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let outcome = request.send(&laser).await?;
            Ok(match outcome {
                Contract::Completed(reply) => Some(reply.body().to_vec()),
                _ => None,
            })
        })
    }

    /// Contract with the same routing and options as `contract`, returning a
    /// dict with `state` (`completed`, `failed`, `timed_out`, or
    /// `not_consumed`), `body`, and the authenticated `verified_principal`.
    #[pyo3(signature = (skill, payload, *, source, agent=None, deadline_ms=30_000, fixed_inbox=None, principal=None, expire_if_not_consumed_ms=None, reply_on=None, conversation=None, fence=None, registered=false, policy=None))]
    #[allow(clippy::too_many_arguments)]
    fn contract_report<'py>(
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
        registered: bool,
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
            registered,
            policy,
        )?;
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let outcome = request.send(&laser).await?;
            Python::attach(|py| contract_to_py(py, outcome))
        })
    }

    /// Scatter a directed task to every agent advertising `skill`, concurrently,
    /// and return the reply body of each that completed (a verifier or diagnostic
    /// panel). Unavailable and quarantined agents are excluded.
    #[pyo3(signature = (skill, payload, *, source, deadline_ms=30_000, fixed_inbox=None, principal=None, policy=None))]
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
    /// scatter is a report of failures rather than an empty list. Each entry is a
    /// dict: `agent` (str), `state` (`"completed"` / `"failed"` / `"timed_out"` /
    /// `"not_consumed"` / `"error"`), `body` (bytes when a reply landed, else
    /// `None`), `error` (the failure text for `"error"`, else `None`), and
    /// `verified_principal` (the authenticated signer when verification is on).
    #[pyo3(signature = (skill, payload, *, source, deadline_ms=30_000, fixed_inbox=None, principal=None, policy=None))]
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
            Python::attach(|py| {
                let entries = pyo3::types::PyList::empty(py);
                for outcome in &report.outcomes {
                    let dict = pyo3::types::PyDict::new(py);
                    dict.set_item("agent", outcome.agent.to_string())?;
                    let (state, body, error, verified_principal): (
                        &str,
                        Option<Vec<u8>>,
                        Option<String>,
                        Option<String>,
                    ) = match &outcome.result {
                        Ok(Contract::Completed(reply)) => (
                            "completed",
                            Some(reply.body().to_vec()),
                            None,
                            reply.verified_principal.clone(),
                        ),
                        Ok(Contract::Failed(reply)) => (
                            "failed",
                            Some(reply.body().to_vec()),
                            None,
                            reply.verified_principal.clone(),
                        ),
                        Ok(Contract::TimedOut) => ("timed_out", None, None, None),
                        Ok(Contract::NotConsumed) => ("not_consumed", None, None, None),
                        Err(cause) => ("error", None, Some(cause.to_string()), None),
                    };
                    dict.set_item("state", state)?;
                    dict.set_item("body", body)?;
                    dict.set_item("error", error)?;
                    dict.set_item("verified_principal", verified_principal)?;
                    entries.append(dict)?;
                }
                Ok(entries.into_any().unbind())
            })
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

    /// Replay a conversation's history off the log: read `topics` (default the
    /// command and response topics), order by timestamp, and apply a policy.
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
                .map(PyAgentMessage::from_context)
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

    /// The full client, for operations the ctx helpers do not cover (kv, query, ...).
    fn laser(&self) -> PyLaser {
        PyLaser::from_inner(self.laser.clone())
    }

    /// A child conversation of the handled message, linked by parent / root ids.
    fn spawn_subconversation(&self) -> PyProvenance {
        PyProvenance {
            inner: self.laser.spawn_subconversation(&self.message.provenance),
        }
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
    /// routing back to the sender. An agent spawned with a signing key answers a
    /// correlated AGDX command with a signed response instead, so a verifying
    /// caller accepts this agent's terminal and no one else's. Raises ConfigError
    /// if no respond_on was set.
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
        future_into_py(py, async move {
            let name = respond_on.ok_or_else(|| to_pyerr(LaserError::NoRespondTopic))?;
            if let Some(key) = &signing_key
                && let Some(envelope) = &message.envelope
                && let Some(correlation) = envelope.correlation
            {
                let source = agent
                    .as_ref()
                    .ok_or_else(|| {
                        to_pyerr(LaserError::HandlerConfig(
                            "a signing agent must have an id".to_owned(),
                        ))
                    })?
                    .wire_id();
                let producer = laser.agdx(
                    static_topic(name)?,
                    source,
                    message.provenance.conversation_id.into(),
                );
                let mut send = producer.respond(correlation, payload).signed_by(key);
                if let Some(target) = &message.provenance.agent {
                    send = send.with_target(target.wire_id());
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
                    duration_seconds(timeout_secs, "timeout_secs")?,
                )
                .await
                .map_err(to_pyerr)?;
            Ok(PyAgentMessage::from_inner(reply))
        })
    }

    /// Fan out a task to every agent advertising `skill`, gathering replies under
    /// `policy` within `deadline_ms`. `policy` is `"require_all"` (default, wait
    /// for every branch), `"quorum"` (stop once `quorum` branches succeed), or
    /// `"best_effort"` (take whatever landed by the deadline). Replies land on
    /// this handler's own `respond_on` topic, so the agent must have been spawned
    /// with one. `fixed_inbox` routes every branch to a fixed topic instead of
    /// each agent's advertised inbox. Returns `{"ok": [...], "failures": [...]}`:
    /// each `ok` entry is `{"agent": ..., "body": ...}`, each `failures` entry is
    /// `{"agent": ..., "error": ...}`. A target that resolves no inbox is a
    /// `failures` entry, never silently rerouted.
    #[pyo3(signature = (skill, payload, *, policy="require_all", quorum=None, deadline_ms=30_000, fixed_inbox=None, principal=None, route_policy=None))]
    #[allow(clippy::too_many_arguments)]
    fn fan_out<'py>(
        &self,
        py: Python<'py>,
        skill: String,
        payload: &Bound<'_, PyAny>,
        policy: &str,
        quorum: Option<usize>,
        deadline_ms: u64,
        fixed_inbox: Option<String>,
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
        let route = match fixed_inbox {
            Some(topic) => inbox_route(Some(topic))?,
            None => self.inbox_route.clone(),
        };
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
            Python::attach(|py| {
                let ok = pyo3::types::PyList::empty(py);
                for (agent, reply) in gather.ok {
                    let dict = pyo3::types::PyDict::new(py);
                    dict.set_item("agent", agent.to_string())?;
                    dict.set_item("body", reply.body().to_vec())?;
                    ok.append(dict)?;
                }
                let failures = pyo3::types::PyList::empty(py);
                for (agent, error) in gather.failures {
                    let dict = pyo3::types::PyDict::new(py);
                    dict.set_item("agent", agent.to_string())?;
                    dict.set_item("error", error.to_string())?;
                    failures.append(dict)?;
                }
                let result = pyo3::types::PyDict::new(py);
                result.set_item("ok", ok)?;
                result.set_item("failures", failures)?;
                Ok(result.into_any().unbind())
            })
        })
    }

    /// Pause this handler on a human decision: publish `prompt` as an interrupt on
    /// the human-input topic and await the approver's correlated reply on
    /// `reply_topic`, up to `timeout_secs`, chained to the handled conversation.
    /// Returns the decision body on approval, or raises on rejection (the
    /// approver answers with `respond_input`). The agent must have an id.
    #[pyo3(signature = (reply_topic, prompt, *, timeout_secs=30.0))]
    fn approval_gate<'py>(
        &self,
        py: Python<'py>,
        reply_topic: String,
        prompt: &Bound<'_, PyAny>,
        timeout_secs: f64,
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
                    duration_seconds(timeout_secs, "timeout_secs")?,
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
    Ok(PyAgentCtx {
        laser: laser.inner.clone(),
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
    registered: bool,
    route_failure: RouteFailure,
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
        registered: bool,
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
            registered,
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
        if self.registered {
            builder = builder.registered();
        }
        route_result(builder.send().await, &self.route_failure)
    }
}

// A contract outcome as the `contract_report` dict: `state`, `body`, and the
// authenticated `verified_principal`.
pub(crate) fn contract_to_py(py: Python<'_>, outcome: Contract) -> PyResult<Py<PyAny>> {
    let dict = pyo3::types::PyDict::new(py);
    let (state, body, verified_principal) = match outcome {
        Contract::Completed(reply) => (
            "completed",
            Some(reply.body().to_vec()),
            reply.verified_principal,
        ),
        Contract::Failed(reply) => (
            "failed",
            Some(reply.body().to_vec()),
            reply.verified_principal,
        ),
        Contract::TimedOut => ("timed_out", None, None),
        Contract::NotConsumed => ("not_consumed", None, None),
    };
    dict.set_item("state", state)?;
    dict.set_item("body", body)?;
    dict.set_item("verified_principal", verified_principal)?;
    Ok(dict.into_any().unbind())
}

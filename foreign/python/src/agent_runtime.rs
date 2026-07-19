use crate::agent::{PyAgentMessage, PyProvenance};
use crate::client::PyLaser;
use crate::convert::payload_bytes;
use crate::errors::{InvalidError, to_pyerr};
use crate::sign::{PyKeyRegistry, PySigningKey};
use async_trait::async_trait;
use iggy::prelude::Identifier;
use laser_sdk::LaserError;
use laser_sdk::agent::{
    AgentCtx, AgentHandler, AgentMessage, AgentMiddleware, CapabilitySelector, ConcurrencyPolicy,
    Contract, DeadLetterSink, Deduplicator, GatherPolicy, InboxRoute, ReliableConsumer,
    RetryPolicy, RoutePolicy, Router,
};
use laser_sdk::context::{ContextAssembler, ContextPolicy, LastN, RoleFilter};
use laser_sdk::laser::Laser;
use laser_sdk::provenance::{AgentTopic, Provenance};
use laser_sdk::types::{AgentId, ConversationId, PrincipalId};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{
    future_into_py, get_current_locals, get_runtime, into_future, scope,
};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Mutex;
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
fn inbox_route(fixed_inbox: Option<String>) -> InboxRoute {
    match fixed_inbox {
        Some(topic) => InboxRoute::Fixed(static_topic(topic)),
        None => InboxRoute::default(),
    }
}

/// A `respond_on` topic needs a `'static` `AgentTopic`. Well-known names map to
/// their static variant. A custom name leaks one `Identifier` (an agent is
/// long-lived and spawned rarely, so the one-time leak per agent is acceptable).
pub(crate) fn static_topic(name: String) -> AgentTopic<'static> {
    match name.as_str() {
        "agent.commands" => AgentTopic::Commands,
        "agent.responses" => AgentTopic::Responses,
        "agent.tool_calls" => AgentTopic::ToolCalls,
        "agent.tool_results" => AgentTopic::ToolResults,
        "agent.llm_io" => AgentTopic::LlmIo,
        "agent.human_input" => AgentTopic::HumanInput,
        "agent.audit" => AgentTopic::Audit,
        "agent.dlq" => AgentTopic::Dlq,
        _ => {
            let id: &'static Identifier =
                Box::leak(Box::new(Identifier::named(&name).unwrap_or_else(|_| {
                    Identifier::named("agent.commands").expect("static")
                })));
            AgentTopic::Custom(id)
        }
    }
}

/// Parse a `fan_out` policy name into a `GatherPolicy`. `quorum` is required
/// (and only meaningful) for `"quorum"`.
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
        };
        let future = Python::attach(|py| -> PyResult<_> {
            let callback = self.callback.bind(py);
            let coroutine = callback.call1((py_ctx, py_message))?;
            into_future(coroutine)
        })
        .map_err(|error| LaserError::Handler(format!("calling the handler: {error}")))?;
        future
            .await
            .map_err(|error| LaserError::Handler(format!("the handler raised: {error}")))?;
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

// A `DeadLetterSink` backed by a Python `async def on_dead_letter(message, reason,
// attempts, published) -> None` callback. `message` is the decoded poison message
// or `None` when the provenance itself would not decode. A callback that raises is
// logged and swallowed: the message is already dead-lettered, so a faulty sink
// must not crash the consumer.
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
        let reason = format!("{:?}", capsule.reason);
        let attempts = capsule.attempts;
        let published = publish_result.is_ok();
        let future = Python::attach(|py| -> PyResult<_> {
            let coroutine = self
                .callback
                .bind(py)
                .call1((py_message, reason, attempts, published))?;
            into_future(coroutine)
        });
        match future {
            Ok(future) => {
                if let Err(error) = future.await {
                    log::warn!("dead-letter sink raised: {error}");
                }
            }
            Err(error) => log::warn!("dead-letter sink could not be scheduled: {error}"),
        }
    }
}

// An `AgentMiddleware` backed by a Python object with optional `async def
// before_handle(message) -> None` and `async def after_handle(message, ok,
// attempt) -> None` methods. A raising `before_handle` rejects the message (it is
// dead-lettered, mirroring the Rust seam). A raising `after_handle` is logged and
// swallowed. Either method may be absent.
struct PyMiddleware {
    hooks: Py<PyAny>,
}

#[async_trait]
impl AgentMiddleware for PyMiddleware {
    async fn before_handle(&self, message: &AgentMessage) -> Result<(), LaserError> {
        let py_message = PyAgentMessage::from_inner(message.clone());
        let scheduled = Python::attach(|py| -> PyResult<_> {
            let bound = self.hooks.bind(py);
            if !bound.hasattr("before_handle")? {
                return Ok(None);
            }
            let coroutine = bound.getattr("before_handle")?.call1((py_message,))?;
            Ok(Some(into_future(coroutine)?))
        });
        match scheduled {
            Ok(Some(future)) => future
                .await
                .map(|_| ())
                .map_err(|error| LaserError::Handler(format!("middleware before_handle: {error}"))),
            Ok(None) => Ok(()),
            Err(error) => Err(LaserError::Handler(format!(
                "middleware before_handle: {error}"
            ))),
        }
    }

    async fn after_handle(
        &self,
        message: &AgentMessage,
        result: &Result<(), LaserError>,
        attempt: u32,
    ) {
        let py_message = PyAgentMessage::from_inner(message.clone());
        let ok = result.is_ok();
        let scheduled = Python::attach(|py| -> PyResult<_> {
            let bound = self.hooks.bind(py);
            if !bound.hasattr("after_handle")? {
                return Ok(None);
            }
            let coroutine = bound
                .getattr("after_handle")?
                .call1((py_message, ok, attempt))?;
            Ok(Some(into_future(coroutine)?))
        });
        match scheduled {
            Ok(Some(future)) => {
                if let Err(error) = future.await {
                    log::warn!("middleware after_handle raised: {error}");
                }
            }
            Ok(None) => {}
            Err(error) => log::warn!("middleware after_handle could not be scheduled: {error}"),
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
    /// deduplicator. `max_partitions` runs one ordered worker lane per partition up
    /// to that many concurrent lanes (omit for strict serial). `shutdown_grace_ms`
    /// bounds how long a graceful stop waits for the in-flight message.
    /// `dead_letter` (`async def on_dead_letter(message, reason, attempts,
    /// published)`) is notified for every poison message. `middleware` is a list of
    /// objects with optional `async def before_handle(message)` (a raise rejects
    /// and dead-letters the message) and `async def after_handle(message, ok,
    /// attempt)` hooks. `governor` (an object with `async def decide(action) ->
    /// ActionDecision`) governs everything the handler publishes, applied under
    /// `governor_mode` (`"enforce"` | `"observe"`), replacing any
    /// connection-level governor for this agent. Returns a handle to await
    /// readiness and stop it. Requires a default stream.
    #[pyo3(signature = (agent_id, listen_on, handler, *, consumer_group=None, respond_on=None, poll_interval_ms=None, warm_dedup=false, dedup=None, capabilities=None, ack_on_pickup=false, health=None, max_partitions=None, shutdown_grace_ms=None, dead_letter=None, middleware=None, retry_max_attempts=None, retry_base_delay_ms=None, governor=None, governor_mode="enforce", signing_key=None, verifier=None))]
    #[allow(clippy::too_many_arguments)]
    fn spawn_agent(
        &self,
        py: Python<'_>,
        agent_id: String,
        listen_on: String,
        handler: Py<PyAny>,
        consumer_group: Option<String>,
        respond_on: Option<String>,
        poll_interval_ms: Option<u64>,
        warm_dedup: bool,
        dedup: Option<Py<PyAny>>,
        capabilities: Option<Vec<String>>,
        ack_on_pickup: bool,
        health: Option<String>,
        max_partitions: Option<usize>,
        shutdown_grace_ms: Option<u64>,
        dead_letter: Option<Py<PyAny>>,
        middleware: Option<Vec<Py<PyAny>>>,
        retry_max_attempts: Option<u32>,
        retry_base_delay_ms: Option<u64>,
        governor: Option<Py<PyAny>>,
        governor_mode: &str,
        signing_key: Option<&PySigningKey>,
        verifier: Option<&PyKeyRegistry>,
    ) -> PyResult<PyAgentHandle> {
        let agent = AgentId::new(agent_id).map_err(|e| to_pyerr(e.into()))?;
        // A per-agent governor re-scopes the agent's `Laser`, so everything the
        // handler publishes through its ctx is governed (mirrors the Rust
        // `Agent::builder().governor(..)`).
        let laser = match governor {
            Some(hooks) => self.inner.with_governor(
                std::sync::Arc::new(crate::govern::PyActionGovernor { hooks }),
                crate::govern::parse_mode(governor_mode)?,
            ),
            None => self.inner.clone(),
        };
        let group = consumer_group
            .map(laser_sdk::types::ConsumerGroupName::new)
            .transpose()
            .map_err(|error| to_pyerr(error.into()))?
            .unwrap_or_else(|| laser_sdk::types::ConsumerGroupName::for_agent(&agent));
        let py_handler = PyHandler {
            callback: handler,
            agent: Some(agent.clone()),
            respond_on: respond_on.clone(),
        };
        let deduplicator: Option<Box<dyn Deduplicator>> =
            dedup.map(|callback| Box::new(PyDeduplicator { callback }) as Box<dyn Deduplicator>);
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
        let respond_topic = respond_on.map(static_topic);
        let poll = poll_interval_ms.map(Duration::from_millis);
        let shutdown_grace = shutdown_grace_ms.map(Duration::from_millis);
        let signing_key = signing_key.map(|key| key.inner.clone());
        let verifier = verifier.map(PyKeyRegistry::snapshot);
        // Skills the agent advertises: a capability card published on start, the
        // same auto-advertise the Rust `Agent` builder does when capabilities are
        // set. An optional `health` ("healthy"/"degraded"/"unavailable") applies to
        // every advertised skill.
        let advertised_health = health.as_deref().map(parse_health);
        let card = capabilities
            .filter(|skills| !skills.is_empty())
            .map(|skills| laser_sdk::wire::agent::AgentCard {
                name: None,
                version: None,
                capabilities: skills
                    .into_iter()
                    .map(|skill_id| laser_sdk::wire::agent::CapabilityDescriptor {
                        skill_id,
                        input: None,
                        output: None,
                        cost_class: None,
                        latency_class: None,
                        max_concurrency: None,
                        health: advertised_health,
                        load: None,
                    })
                    .collect(),
                ttl_micros: None,
            });
        let locals = get_current_locals(py)?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let advertise_id = agent.clone();
        // The inbox the agent advertises is the topic it consumes on.
        let presence_inbox = listen_on.clone();
        let join = get_runtime().spawn(scope(locals, async move {
            if let Some(card) = card {
                if let Err(error) = laser.publish_card(advertise_id.clone(), &card).await {
                    log::warn!("failed to publish the agent capability card: {error}");
                }
                // Advertise the live inbox too, mirroring the Rust builder, so a
                // capability route resolves this agent without a fixed inbox. Best
                // effort: a stock server without the presence command is fine.
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
                .agent(agent)
                .topic(listen_on)
                .maybe_respond_on(respond_topic)
                .maybe_poll_interval(poll)
                .maybe_shutdown_grace(shutdown_grace)
                .concurrency(concurrency)
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
        })
    }

    /// Send a directed task to one agent advertising `skill`, await its reply up to
    /// `deadline_ms`. Returns the reply body, or `None` if it did not complete in
    /// time. `fixed_inbox` routes to a fixed topic (a stock server with no presence
    /// command). Omit it to resolve each agent's advertised inbox.
    #[pyo3(signature = (skill, payload, *, source, deadline_ms=10_000, fixed_inbox=None, principal=None))]
    #[allow(clippy::too_many_arguments)]
    fn contract<'py>(
        &self,
        py: Python<'py>,
        skill: String,
        payload: Vec<u8>,
        source: String,
        deadline_ms: u64,
        fixed_inbox: Option<String>,
        principal: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = AgentId::new(source).map_err(|e| to_pyerr(e.into()))?;
        let route = inbox_route(fixed_inbox);
        future_into_py(py, async move {
            let mut selector = CapabilitySelector::new(skill, RoutePolicy::Any);
            if let Some(principal) = principal {
                selector = selector.principal(PrincipalId::new(principal));
            }
            let outcome = laser
                .contract(Router::ToCapable(selector))
                .from(source)
                .payload(payload)
                .inbox_route(route)
                .deadline(Duration::from_millis(deadline_ms))
                .send()
                .await
                .map_err(to_pyerr)?;
            Ok(match outcome {
                Contract::Completed(reply) => Some(reply.body().to_vec()),
                _ => None,
            })
        })
    }

    /// Contract with the same routing semantics as [`contract`](Self::contract),
    /// returning `state`, `body`, and the authenticated `verified_principal`.
    #[pyo3(signature = (skill, payload, *, source, deadline_ms=10_000, fixed_inbox=None, principal=None))]
    #[allow(clippy::too_many_arguments)]
    fn contract_report<'py>(
        &self,
        py: Python<'py>,
        skill: String,
        payload: Vec<u8>,
        source: String,
        deadline_ms: u64,
        fixed_inbox: Option<String>,
        principal: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = AgentId::new(source).map_err(|e| to_pyerr(e.into()))?;
        let route = inbox_route(fixed_inbox);
        future_into_py(py, async move {
            let mut selector = CapabilitySelector::new(skill, RoutePolicy::Any);
            if let Some(principal) = principal {
                selector = selector.principal(PrincipalId::new(principal));
            }
            let outcome = laser
                .contract(Router::ToCapable(selector))
                .from(source)
                .payload(payload)
                .inbox_route(route)
                .deadline(Duration::from_millis(deadline_ms))
                .send()
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| {
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
            })
        })
    }

    /// Scatter a directed task to every agent advertising `skill`, concurrently,
    /// and return the reply body of each that completed (a verifier or diagnostic
    /// panel). Unavailable and quarantined agents are excluded.
    #[pyo3(signature = (skill, payload, *, source, deadline_ms=30_000, fixed_inbox=None, principal=None))]
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = AgentId::new(source).map_err(|e| to_pyerr(e.into()))?;
        let mut selector = CapabilitySelector::new(skill, RoutePolicy::Any);
        if let Some(principal) = principal {
            selector = selector.principal(PrincipalId::new(principal));
        }
        let route = inbox_route(fixed_inbox);
        future_into_py(py, async move {
            laser
                .scatter(
                    source,
                    &selector,
                    &payload,
                    &route,
                    Duration::from_millis(deadline_ms),
                )
                .await
                .map_err(to_pyerr)
        })
    }

    /// Scatter like [`scatter`](Self::scatter), but return every contracted
    /// agent's terminal outcome, not only the completed replies, so an all-failed
    /// scatter is a report of failures rather than an empty list. Each entry is a
    /// dict: `agent` (str), `state` (`"completed"` / `"failed"` / `"timed_out"` /
    /// `"not_consumed"` / `"error"`), `body` (bytes when a reply landed, else
    /// `None`), `error` (the failure text for `"error"`, else `None`), and
    /// `verified_principal` (the authenticated signer when verification is on).
    #[pyo3(signature = (skill, payload, *, source, deadline_ms=30_000, fixed_inbox=None, principal=None))]
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = AgentId::new(source).map_err(|e| to_pyerr(e.into()))?;
        let mut selector = CapabilitySelector::new(skill, RoutePolicy::Any);
        if let Some(principal) = principal {
            selector = selector.principal(PrincipalId::new(principal));
        }
        let route = inbox_route(fixed_inbox);
        future_into_py(py, async move {
            let report = laser
                .scatter_report(
                    source,
                    &selector,
                    &payload,
                    &route,
                    Duration::from_millis(deadline_ms),
                )
                .await
                .map_err(to_pyerr)?;
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
    /// dispatches as. `fixed_inbox` routes every step to a fixed topic (a stock
    /// server with no presence command). Omit it to resolve advertised inboxes.
    #[pyo3(signature = (name, *, fixed_inbox=None))]
    fn workflow(&self, name: String, fixed_inbox: Option<String>) -> crate::workflow::PyWorkflow {
        crate::workflow::PyWorkflow::new(self.inner.clone(), name, fixed_inbox)
    }

    /// Replay a conversation's history off the log: read `topics` (default the
    /// command and response topics), order by timestamp, and apply a policy.
    /// `roles` keeps only messages from those agents. Otherwise the last
    /// `last_n` messages are kept (default 50). Returns the selected messages.
    #[pyo3(signature = (conversation_id, *, topics=None, last_n=None, roles=None))]
    fn assemble_context<'py>(
        &self,
        py: Python<'py>,
        conversation_id: String,
        topics: Option<Vec<String>>,
        last_n: Option<usize>,
        roles: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let conversation =
            ConversationId::from_str(&conversation_id).map_err(|e| to_pyerr(e.into()))?;
        let topics: Option<Vec<AgentTopic<'static>>> =
            topics.map(|names| names.into_iter().map(static_topic).collect());
        let policy: Box<dyn ContextPolicy> = match roles {
            Some(roles) => {
                let mut set = HashSet::new();
                for role in roles {
                    set.insert(AgentId::new(role).map_err(|e| to_pyerr(e.into()))?);
                }
                Box::new(RoleFilter(set))
            }
            None => Box::new(LastN(last_n.unwrap_or(50))),
        };
        future_into_py(py, async move {
            let messages = ContextAssembler::builder()
                .conversation_id(conversation)
                .maybe_topics(topics)
                .policy(policy)
                .build()
                .assemble(&laser)
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
                        // Context assembly does not thread the ct header. A
                        // python reader resolves claim-checked bodies through
                        // the Rust surface when it needs them.
                        content_type: None,
                        verified_principal: None,
                    })
                })
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
    /// `Agdx.request_input`) completes. Requires the handled message to be an AGDX
    /// envelope carrying a correlation, and the agent to have an id.
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
        future_into_py(py, async move {
            let envelope = envelope.ok_or_else(|| {
                to_pyerr(LaserError::Handler(
                    "respond_input: the handled message is not an AGDX envelope".to_owned(),
                ))
            })?;
            let correlation = envelope.correlation.ok_or_else(|| {
                to_pyerr(LaserError::Handler(
                    "respond_input: the request carries no correlation".to_owned(),
                ))
            })?;
            let source = agent
                .ok_or_else(|| {
                    to_pyerr(LaserError::Handler(
                        "respond_input: the agent has no id".to_owned(),
                    ))
                })?
                .wire_id();
            laser
                .agdx(static_topic(reply_topic), source, envelope.conversation)
                .respond(correlation, response)
                .send()
                .await
                .map(|_record_id| ())
                .map_err(to_pyerr)
        })
    }

    /// Reply on the agent's configured respond_on topic, chaining causality and
    /// routing back to the sender. Raises ConfigError if no respond_on was set.
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
        future_into_py(py, async move {
            let name = respond_on.ok_or_else(|| to_pyerr(LaserError::NoRespondTopic))?;
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
                    Duration::from_secs_f64(timeout_secs),
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
    #[pyo3(signature = (skill, payload, *, policy="require_all", quorum=None, deadline_ms=30_000, fixed_inbox=None, principal=None))]
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let message = self.message.clone();
        let agent = self.agent.clone();
        let respond_on = self.respond_on.clone().map(static_topic);
        let route = inbox_route(fixed_inbox);
        let payload = payload_bytes(payload)?;
        let gather_policy = parse_gather_policy(policy, quorum)?;
        future_into_py(py, async move {
            let mut selector = CapabilitySelector::new(skill, RoutePolicy::Any);
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
                .await
                .map_err(to_pyerr)?;
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
        let respond_on = self.respond_on.clone().map(static_topic);
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
                    static_topic(reply_topic),
                    prompt,
                    Duration::from_secs_f64(timeout_secs),
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
#[pyo3(signature = (laser, message, *, agent=None, respond_on=None))]
pub fn agent_ctx(
    laser: &PyLaser,
    message: &PyAgentMessage,
    agent: Option<String>,
    respond_on: Option<String>,
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
                    to_pyerr(LaserError::Handler("agent stopped before ready".to_owned()))
                })?;
            }
            Ok(())
        })
    }

    /// Signal the agent to stop, wait for it, and surface any consumer error.
    fn shutdown<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if let Some(sender) = self.shutdown.lock().expect("shutdown lock").take() {
            let _ = sender.send(());
        }
        let join = self.join.lock().expect("join lock").take();
        future_into_py(py, async move {
            match join {
                Some(join) => match join.await {
                    Ok(result) => result.map_err(to_pyerr),
                    Err(error) => Err(to_pyerr(LaserError::Handler(error.to_string()))),
                },
                None => Ok(()),
            }
        })
    }

    /// Wait for the agent to finish (it runs until its consumer ends or errors).
    fn join<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let join = self.join.lock().expect("join lock").take();
        future_into_py(py, async move {
            match join {
                Some(join) => match join.await {
                    Ok(result) => result.map_err(to_pyerr),
                    Err(error) => Err(to_pyerr(LaserError::Handler(error.to_string()))),
                },
                None => Ok(()),
            }
        })
    }

    /// Abort the agent's task immediately, without waiting.
    fn abort(&self) {
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
                    to_pyerr(LaserError::Handler("agent stopped before ready".to_owned()))
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
        if let Some(sender) = self.shutdown.lock().expect("shutdown lock").take() {
            let _ = sender.send(());
        }
        let join = self.join.lock().expect("join lock").take();
        future_into_py(py, async move {
            if let Some(join) = join {
                match join.await {
                    Ok(result) => result.map_err(to_pyerr)?,
                    Err(error) => return Err(to_pyerr(LaserError::Handler(error.to_string()))),
                }
            }
            Ok(false)
        })
    }
}

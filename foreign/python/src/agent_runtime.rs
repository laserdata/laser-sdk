use crate::agent::{PyAgentMessage, PyProvenance};
use crate::client::PyLaser;
use crate::convert::payload_bytes;
use crate::errors::{InvalidError, to_pyerr};
use async_trait::async_trait;
use iggy::prelude::Identifier;
use laser_sdk::LaserError;
use laser_sdk::agent::{
    AgentConsumer, AgentCtx, AgentHandler, AgentMessage, CapabilitySelector, Contract,
    Deduplicator, InboxRoute, RoutePolicy, Router,
};
use laser_sdk::context::{ContextAssembler, ContextPolicy, LastN, RoleFilter};
use laser_sdk::laser::Laser;
use laser_sdk::provenance::{AgentTopic, Provenance};
use laser_sdk::types::{AgentId, ConversationId};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::{
    future_into_py, get_current_locals, get_runtime, into_future, scope,
};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
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

/// Reply provenance chained off the handled message (mirrors the Rust AgentCtx):
/// same conversation and root, causal parent is the handled message, the reply is
/// stamped with this agent's id, and the request's idempotency key is echoed back
/// so the caller's request/reply correlator matches.
fn reply_provenance(message: &AgentMessage, agent: &Option<AgentId>) -> Provenance {
    let mut provenance = Provenance::builder()
        .conversation_id(message.provenance.conversation_id)
        .causal_parent(message.id)
        .build();
    provenance.agent = agent.clone();
    provenance.root_conversation_id = message.provenance.root_conversation_id;
    provenance.idempotency_key = message.provenance.idempotency_key.clone();
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
// (return true), so dedup never silently drops a message on a callback fault.
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
                Ok(value) => Python::attach(|py| value.bind(py).extract::<bool>().unwrap_or(true)),
                Err(_) => true,
            },
            Err(_) => true,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// Spawn an agent: join an Iggy consumer group named `agent_id` over
    /// `listen_on` and drive `handler` (an `async def handle(ctx, message)`) for
    /// each message, with at-least-once delivery, dedup, retry, and DLQ. Pass
    /// `dedup` (an `async def observe(key) -> bool`) for a custom, e.g. durable,
    /// deduplicator. Returns a handle to await readiness and stop it. Requires a
    /// default stream.
    #[pyo3(signature = (agent_id, listen_on, handler, *, respond_on=None, poll_interval_ms=None, warm_dedup=false, dedup=None, capabilities=None, ack_on_pickup=false, health=None))]
    #[allow(clippy::too_many_arguments)]
    fn spawn_agent(
        &self,
        py: Python<'_>,
        agent_id: String,
        listen_on: String,
        handler: Py<PyAny>,
        respond_on: Option<String>,
        poll_interval_ms: Option<u64>,
        warm_dedup: bool,
        dedup: Option<Py<PyAny>>,
        capabilities: Option<Vec<String>>,
        ack_on_pickup: bool,
        health: Option<String>,
    ) -> PyResult<PyAgentHandle> {
        let agent = AgentId::new(agent_id).map_err(|e| to_pyerr(e.into()))?;
        let laser = self.inner.clone();
        let group = agent.to_string();
        let py_handler = PyHandler {
            callback: handler,
            agent: Some(agent.clone()),
            respond_on: respond_on.clone(),
        };
        let deduplicator: Option<Box<dyn Deduplicator>> =
            dedup.map(|callback| Box::new(PyDeduplicator { callback }) as Box<dyn Deduplicator>);
        let respond_topic = respond_on.map(static_topic);
        let poll = poll_interval_ms.map(Duration::from_millis);
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
                    eprintln!("failed to publish the agent capability card: {error}");
                }
                // Advertise the live inbox too, mirroring the Rust builder, so a
                // capability route resolves this agent without a fixed inbox. Best
                // effort: a stock server without the presence command is fine.
                let presence = laser_sdk::wire::agent::AgentPresence::new(advertise_id.wire_id())
                    .with_inbox(presence_inbox);
                if let Err(error) = laser.advertise_presence(&presence).await {
                    eprintln!("inbox presence not advertised: {error}");
                }
            }
            AgentConsumer::builder()
                .group(group)
                .topic(listen_on)
                .maybe_respond_on(respond_topic)
                .maybe_poll_interval(poll)
                .warm_dedup(warm_dedup)
                .maybe_deduplicator(deduplicator)
                .ack_on_pickup(ack_on_pickup)
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
    #[pyo3(signature = (skill, payload, *, source, deadline_ms=10_000, fixed_inbox=None))]
    fn contract<'py>(
        &self,
        py: Python<'py>,
        skill: String,
        payload: Vec<u8>,
        source: String,
        deadline_ms: u64,
        fixed_inbox: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = AgentId::new(source).map_err(|e| to_pyerr(e.into()))?;
        let route = inbox_route(fixed_inbox);
        future_into_py(py, async move {
            let outcome = laser
                .contract(Router::to_capable(skill, RoutePolicy::Any))
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

    /// Scatter a directed task to every agent advertising `skill`, concurrently,
    /// and return the reply body of each that completed (a verifier or diagnostic
    /// panel). Unavailable and quarantined agents are excluded.
    #[pyo3(signature = (skill, payload, *, source, deadline_ms=30_000, fixed_inbox=None))]
    fn scatter<'py>(
        &self,
        py: Python<'py>,
        skill: String,
        payload: Vec<u8>,
        source: String,
        deadline_ms: u64,
        fixed_inbox: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = AgentId::new(source).map_err(|e| to_pyerr(e.into()))?;
        let selector = CapabilitySelector::new(skill, RoutePolicy::Any);
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
    /// server with no presence command); omit it to resolve advertised inboxes.
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

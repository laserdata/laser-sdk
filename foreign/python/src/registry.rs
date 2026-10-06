use crate::agent::{PyAgentMessage, PyProvenance};
use crate::agent_runtime::{
    ParsedRoutePolicy, inbox_route, route_policy, route_result, static_topic,
};
use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{payload_bytes, py_to_de, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use crate::sign::PySigningKey;
use laser_sdk::agent::{
    AgentScope, CapabilitySelector, ConsumerRef, ConsumptionStatus, RegisteredCard, Router,
};
use laser_sdk::laser::Laser;
use laser_sdk::query::QueryExecutionId;
use laser_sdk::types::{AgentId, ConsumerGroupName, ConversationId, PrincipalId};
use laser_sdk::wire::agent::{AgentCard, AgentPresence, CapabilityDescriptor, ChannelId};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{
    gen_stub_pyclass, gen_stub_pyclass_complex_enum, gen_stub_pyfunction, gen_stub_pymethods,
};
use std::str::FromStr;

fn agent_id(value: String) -> PyResult<AgentId> {
    AgentId::new(value).map_err(|e| to_pyerr(e.into()))
}

fn now_micros() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros();
    u64::try_from(now).unwrap_or(u64::MAX)
}

pub(crate) fn capabilities(values: Vec<Bound<'_, PyAny>>) -> PyResult<Vec<CapabilityDescriptor>> {
    values
        .iter()
        .map(|value| {
            if let Ok(skill_id) = value.extract::<String>() {
                return serde_json::from_value(serde_json::json!({ "skill_id": skill_id }))
                    .map_err(|e| crate::errors::CodecError::new_err(e.to_string()));
            }
            py_to_de(value)
        })
        .collect()
}

fn execution_id(value: &str) -> PyResult<QueryExecutionId> {
    QueryExecutionId::from_str(value).map_err(|error| InvalidError::new_err(error.to_string()))
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A read model over the agent card registry topic. It shares a per-stream
    /// cache, so `refresh` folds only what is new and every registry opened
    /// later on this connection sees the folded state.
    fn agent_registry(&self) -> PyAgentRegistry {
        PyAgentRegistry {
            laser: self.inner.clone(),
        }
    }

    /// Publish `source`'s capability card (a dict mirroring `AgentCard`:
    /// `name`, `version`, `capabilities`, `ttl_micros`, ...) to the registry
    /// topic, so capability routing can resolve `source`. Re-publish on an
    /// interval shorter than the card's `ttl_micros`.
    fn publish_card<'py>(
        &self,
        py: Python<'py>,
        source: String,
        card: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let source = agent_id(source)?;
        let card: AgentCard = py_to_de(card)?;
        future_into_py(py, async move {
            laser.publish_card(source, &card).await.map_err(to_pyerr)
        })
    }

    /// Quarantine `agent` with a fact signed by the operator's `key`, so a
    /// registry that enrolls the matching verifying key folds it.
    fn quarantine_signed<'py>(
        &self,
        py: Python<'py>,
        operator: String,
        agent: String,
        key: PyRef<'_, PySigningKey>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let operator = agent_id(operator)?;
        let agent = agent_id(agent)?;
        let key = key.inner.clone();
        future_into_py(py, async move {
            laser
                .quarantine_signed(operator, &agent, &key)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Lift a quarantine with a fact signed by the operator's `key`.
    fn unquarantine_signed<'py>(
        &self,
        py: Python<'py>,
        operator: String,
        agent: String,
        key: PyRef<'_, PySigningKey>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let operator = agent_id(operator)?;
        let agent = agent_id(agent)?;
        let key = key.inner.clone();
        future_into_py(py, async move {
            laser
                .unquarantine_signed(operator, &agent, &key)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Advertise this connection's live presence (a dict mirroring
    /// `AgentPresence`: `v`, `agent`, `inbox`, ...), so a registry can route
    /// work to its inbox. Served by the LaserData Iggy fork.
    fn advertise_presence<'py>(
        &self,
        py: Python<'py>,
        presence: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let presence: AgentPresence = py_to_de(presence)?;
        future_into_py(py, async move {
            laser.advertise_presence(&presence).await.map_err(to_pyerr)
        })
    }

    /// Withdraw this connection's advertised presence. Disconnecting clears it
    /// too.
    fn clear_presence<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(
            py,
            async move { laser.clear_presence().await.map_err(to_pyerr) },
        )
    }

    /// One page of live connections and their advertised metadata, as a
    /// `ClientMetadataPage`. Pass its `next_cursor` back
    /// as `after` for the next page. `metadata_only` keeps connections that
    /// advertised metadata, and `principal` narrows to one authenticated user.
    #[pyo3(signature = (*, metadata_only=false, principal=None, limit=None, after=None))]
    fn client_metadata<'py>(
        &self,
        py: Python<'py>,
        metadata_only: bool,
        principal: Option<u32>,
        limit: Option<u32>,
        after: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let page = client_metadata_request(&laser, metadata_only, principal, limit, after)
                .page()
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| {
                Ok(PyClientMetadataPage {
                    clients: ser_to_py(py, &page.clients)?,
                    next_cursor: page.next_cursor,
                })
            })
        })
    }

    /// Every live connection and its advertised metadata, walking the pages
    /// from `after` (or the start) until the cursor runs out. `limit` sizes
    /// each page. Prefer `client_metadata` when the set may be large.
    #[pyo3(signature = (*, metadata_only=false, principal=None, limit=None, after=None))]
    fn client_metadata_all<'py>(
        &self,
        py: Python<'py>,
        metadata_only: bool,
        principal: Option<u32>,
        limit: Option<u32>,
        after: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let clients = client_metadata_request(&laser, metadata_only, principal, limit, after)
                .all()
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &clients))
        })
    }

    /// The agent accessor: every verb on the returned scope acts as the agent
    /// `id` (sends carry it as the source, the card and presence advertise it).
    fn agent(&self, id: String) -> PyResult<PyAgentScope> {
        Ok(PyAgentScope {
            inner: self.inner.agent(agent_id(id)?),
            laser: self.inner.clone(),
        })
    }

    /// Reassemble a chunk stream from the log: read `conversation` on `topic`,
    /// take the chunk envelopes for `channel` in sequence order, and replay
    /// them into ordered `StreamEvent` dicts, the ones `ChunkAssembler.feed`
    /// returns.
    fn reassemble_channel<'py>(
        &self,
        py: Python<'py>,
        conversation: String,
        topic: String,
        channel: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let conversation =
            ConversationId::from_str(&conversation).map_err(|e| to_pyerr(e.into()))?;
        let topic = static_topic(topic)?;
        let channel = ChannelId::from_str(&channel)
            .map_err(|error| InvalidError::new_err(error.to_string()))?;
        future_into_py(py, async move {
            let events = laser
                .reassemble_channel(conversation, topic, channel)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Ok(crate::chunks::events_to_py(py, events)?.unbind()))
        })
    }

    /// Whether the `target` consumer has committed past the `LogPosition`
    /// `at`. Returns a `ConsumptionStatus`.
    fn consumed<'py>(
        &self,
        py: Python<'py>,
        target: &PyConsumerRef,
        at: &crate::agdx::PyLogPosition,
    ) -> PyResult<Bound<'py, PyAny>> {
        let target = target.to_rust()?;
        let at = at.inner;
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let status = laser.consumed(target, at).await.map_err(to_pyerr)?;
            Ok(PyConsumptionStatus::from(status))
        })
    }

    /// Execute a raw query (a dict mirroring the wire `Query`) and return one
    /// result page. `query(..)` builds the same request fluently.
    fn execute_query<'py>(
        &self,
        py: Python<'py>,
        query: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let query = py_to_de(query)?;
        future_into_py(py, async move {
            let result = laser.execute_query(query).await.map_err(to_pyerr)?;
            Ok(crate::query::PyQueryResult::from(result))
        })
    }

    /// Fetch the page after `cursor` for an execution already started.
    /// `deadline_micros` is the absolute deadline in epoch microseconds.
    fn query_page<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
        cursor: String,
        deadline_micros: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let execution_id = self::execution_id(&execution_id)?;
        future_into_py(py, async move {
            let result = laser
                .query_page(execution_id, cursor, deadline_micros)
                .await
                .map_err(to_pyerr)?;
            Ok(crate::query::PyQueryResult::from(result))
        })
    }

    /// Execute one raw checkpoint request (a dict mirroring the wire
    /// `CheckpointRequestEnvelope`) and return the mutation result as a dict.
    /// `Destinations.mutate` builds the common public mutation for you. Needs the
    /// destinations capability, otherwise raises `UnsupportedError`.
    fn execute_checkpoint<'py>(
        &self,
        py: Python<'py>,
        request: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let request = py_to_de(request)?;
        future_into_py(py, async move {
            let result = laser.execute_checkpoint(request).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &result))
        })
    }

    /// Request cancellation of a query execution and return its observed state.
    fn cancel_query<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let execution_id = self::execution_id(&execution_id)?;
        future_into_py(py, async move {
            let status = laser.cancel_query(execution_id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &status))
        })
    }

    /// Read the current state of a query execution.
    fn query_status<'py>(
        &self,
        py: Python<'py>,
        execution_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        let execution_id = self::execution_id(&execution_id)?;
        future_into_py(py, async move {
            let status = laser.query_status(execution_id).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &status))
        })
    }
}

/// One agent's latest card, with the time the registry folded it in. A card
/// older than its `ttl_micros` is treated as a dead agent.
#[gen_stub_pyclass]
#[pyclass(name = "RegisteredCard", frozen)]
pub struct PyRegisteredCard {
    pub(crate) inner: RegisteredCard,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyRegisteredCard {
    /// Build a registered card from `agent`, its `card` (a dict mirroring
    /// `AgentCard`), and the epoch micros the registry observed it.
    #[new]
    fn new(agent: String, card: &Bound<'_, PyAny>, observed_at_micros: u64) -> PyResult<Self> {
        Ok(Self {
            inner: RegisteredCard {
                agent: agent_id(agent)?,
                card: py_to_de(card)?,
                observed_at_micros,
            },
        })
    }

    #[getter]
    fn agent(&self) -> String {
        self.inner.agent.as_str().to_owned()
    }

    /// The card, as a dict mirroring `AgentCard`.
    #[getter]
    fn card(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner.card)
    }

    /// When the registry observed this card (epoch micros).
    #[getter]
    fn observed_at_micros(&self) -> u64 {
        self.inner.observed_at_micros
    }

    /// Whether the card is still fresh at `now_micros`. A card without a time
    /// to live never expires.
    fn is_fresh(&self, now_micros: u64) -> bool {
        self.inner.is_fresh(now_micros)
    }

    /// Whether the card advertises `skill_id`.
    fn serves(&self, skill_id: &str) -> bool {
        self.inner.serves(skill_id)
    }

    /// Whether the card advertises `skill_id` and does not report it
    /// unavailable.
    fn available_for(&self, skill_id: &str) -> bool {
        self.inner.available_for(skill_id)
    }

    fn __repr__(&self) -> String {
        format!(
            "RegisteredCard(agent={}, observed_at_micros={})",
            self.inner.agent, self.inner.observed_at_micros
        )
    }
}

impl From<&RegisteredCard> for PyRegisteredCard {
    fn from(card: &RegisteredCard) -> Self {
        Self {
            inner: card.clone(),
        }
    }
}

/// One page of live connections plus the cursor to fetch the next, returned
/// by `Laser.client_metadata`.
#[gen_stub_pyclass]
#[pyclass(name = "ClientMetadataPage", frozen)]
pub struct PyClientMetadataPage {
    clients: Py<PyAny>,
    next_cursor: Option<u32>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyClientMetadataPage {
    /// The connections on this page, each a dict mirroring `ClientMetadata`.
    #[getter]
    fn clients(&self, py: Python<'_>) -> Py<PyAny> {
        self.clients.clone_ref(py)
    }

    /// The `after` for the next page, or `None` on the last page.
    #[getter]
    fn next_cursor(&self) -> Option<u32> {
        self.next_cursor
    }
}

/// The agent card registry read model. Build it with `Laser.agent_registry`.
/// Cards, quarantine facts, and live presence live in a per-stream cache that
/// every registry on the connection shares.
#[gen_stub_pyclass]
#[pyclass(name = "AgentRegistry", frozen)]
pub struct PyAgentRegistry {
    laser: Laser,
}

impl PyAgentRegistry {
    fn read<T>(
        &self,
        f: impl FnOnce(&laser_sdk::agent::AgentRegistry<'_>) -> PyResult<T>,
    ) -> PyResult<T> {
        let registry = self.laser.agent_registry().map_err(to_pyerr)?;
        f(&registry)
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAgentRegistry {
    /// Fold new registry records. `now_micros` defaults to the current time.
    /// Returns the number of records folded.
    #[pyo3(signature = (now_micros=None))]
    fn refresh<'py>(
        &self,
        py: Python<'py>,
        now_micros: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let now = now_micros.unwrap_or_else(self::now_micros);
        future_into_py(py, async move {
            let mut registry = laser.agent_registry().map_err(to_pyerr)?;
            registry.refresh(now).await.map_err(to_pyerr)
        })
    }

    /// Page the live connection table for advertised presence, reusing a read
    /// younger than the presence ttl. Returns the number of presences folded.
    fn refresh_presence<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let mut registry = laser.agent_registry().map_err(to_pyerr)?;
            registry.refresh_presence().await.map_err(to_pyerr)
        })
    }

    /// Every folded card.
    fn agents(&self) -> PyResult<Vec<PyRegisteredCard>> {
        self.read(|registry| Ok(registry.agents().map(PyRegisteredCard::from).collect()))
    }

    /// The folded card for `agent`, or `None`.
    fn lookup(&self, agent: String) -> PyResult<Option<PyRegisteredCard>> {
        let agent = agent_id(agent)?;
        self.read(|registry| Ok(registry.lookup(&agent).map(PyRegisteredCard::from)))
    }

    /// The fresh, unquarantined cards advertising `skill_id` at `now_micros`
    /// (default now).
    #[pyo3(signature = (skill_id, now_micros=None))]
    fn resolve(
        &self,
        skill_id: String,
        now_micros: Option<u64>,
    ) -> PyResult<Vec<PyRegisteredCard>> {
        let now = now_micros.unwrap_or_else(self::now_micros);
        self.read(|registry| {
            Ok(registry
                .resolve(&skill_id, now)
                .into_iter()
                .map(PyRegisteredCard::from)
                .collect())
        })
    }

    /// Whether an operator quarantined `agent`.
    fn is_quarantined(&self, agent: String) -> PyResult<bool> {
        let agent = agent_id(agent)?;
        self.read(|registry| Ok(registry.is_quarantined(&agent)))
    }

    /// The inbox topic `agent` advertised through presence, or `None`.
    fn inbox_for(&self, agent: String) -> PyResult<Option<String>> {
        let agent = agent_id(agent)?;
        self.read(|registry| Ok(registry.inbox_for(&agent).map(str::to_owned)))
    }

    /// The inbox `agent` advertised, only when its connection authenticated as
    /// `principal`.
    fn inbox_for_principal(&self, agent: String, principal: u32) -> PyResult<Option<String>> {
        let agent = agent_id(agent)?;
        self.read(|registry| {
            Ok(registry
                .inbox_for_principal(&agent, PrincipalId::new(principal))
                .map(str::to_owned))
        })
    }

    /// Resolve a route to its concrete target agents at `now_micros` (default
    /// now). Name exactly one route: `to` is that one agent, `broadcast=True`
    /// is no target, `to_capable` is the one agent the route `policy` picks,
    /// and `all_capable` is every capable agent. `principal` pins the
    /// authenticated principal. Raises when a capability route matches no live
    /// agent.
    #[pyo3(signature = (*, to=None, to_capable=None, all_capable=None, broadcast=false, principal=None, policy=None, now_micros=None))]
    #[allow(clippy::too_many_arguments)]
    fn resolve_targets(
        &self,
        to: Option<String>,
        to_capable: Option<String>,
        all_capable: Option<String>,
        broadcast: bool,
        principal: Option<u32>,
        policy: Option<&Bound<'_, PyAny>>,
        now_micros: Option<u64>,
    ) -> PyResult<Vec<String>> {
        let ParsedRoutePolicy { policy, failure } = route_policy(policy)?;
        let selector = |skill: String| {
            let selector = CapabilitySelector::new(skill, policy.clone());
            match principal {
                Some(principal) => selector.principal(PrincipalId::new(principal)),
                None => selector,
            }
        };
        let router = match (to, to_capable, all_capable, broadcast) {
            (Some(agent), None, None, false) => match principal {
                Some(principal) => {
                    Router::to_principal(agent_id(agent)?, PrincipalId::new(principal))
                }
                None => Router::to(agent_id(agent)?),
            },
            (None, Some(skill), None, false) => Router::ToCapable(selector(skill)),
            (None, None, Some(skill), false) => Router::AllCapable(selector(skill)),
            (None, None, None, true) => Router::Broadcast,
            _ => {
                return Err(InvalidError::new_err(
                    "name exactly one of to / to_capable / all_capable / broadcast",
                ));
            }
        };
        let now = now_micros.unwrap_or_else(self::now_micros);
        self.read(|registry| {
            let targets = route_result(router.resolve_targets(registry, now), &failure)?;
            Ok(targets
                .into_iter()
                .map(|agent| agent.as_str().to_owned())
                .collect())
        })
    }

    /// The authenticated principal behind `agent`'s presence, or `None`.
    fn principal_for(&self, agent: String) -> PyResult<Option<u32>> {
        let agent = agent_id(agent)?;
        self.read(|registry| {
            Ok(registry
                .principal_for(&agent)
                .map(|principal| principal.get()))
        })
    }
}

/// The inbox topic `agent` resolves to under an inbox `route`: a fixed topic
/// name, or `None` for the agent's `advertised` live-presence inbox (from
/// `AgentRegistry.inbox_for`), the `fixed_inbox=` value the agent methods
/// take. Raises when the advertised route finds no inbox, rather than
/// inventing a destination.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (route, agent, advertised=None))]
pub fn inbox_route_resolve(
    route: Option<String>,
    agent: String,
    advertised: Option<String>,
) -> PyResult<String> {
    inbox_route(route)?
        .resolve(&agent_id(agent)?, advertised.as_deref())
        .map(|topic| topic.to_string())
        .map_err(to_pyerr)
}

/// A presence dict for `agent` at the current presence version, declaring the
/// `inbox` topic it consumes its work on when given. Pass it to
/// `Laser.advertise_presence`.
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (agent, *, inbox=None))]
pub fn agent_presence(py: Python<'_>, agent: String, inbox: Option<String>) -> PyResult<Py<PyAny>> {
    let mut presence = AgentPresence::new(agent_id(agent)?.wire_id());
    if let Some(inbox) = inbox {
        presence = presence.with_inbox(inbox);
    }
    ser_to_py(py, &presence)
}

/// Check a `presence` dict against the caps. Raises `ValidateError` on a
/// violation.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn validate_agent_presence(presence: &Bound<'_, PyAny>) -> PyResult<()> {
    let presence: AgentPresence = py_to_de(presence)?;
    presence
        .validate()
        .map_err(|error| crate::errors::validate_error(&error))
}

/// One agent identity over the fabric. Build it with `Laser.agent(id)`.
#[gen_stub_pyclass]
#[pyclass(name = "AgentScope", frozen)]
pub struct PyAgentScope {
    inner: AgentScope,
    laser: Laser,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAgentScope {
    /// This agent's id.
    #[getter]
    fn id(&self) -> String {
        self.inner.id().as_str().to_owned()
    }

    /// Append `payload` to an agent `topic` as this agent. The provenance gains
    /// this id as its agent, and the partition is keyed by conversation.
    fn send<'py>(
        &self,
        py: Python<'py>,
        topic: String,
        payload: &Bound<'_, PyAny>,
        provenance: &PyProvenance,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scope = self.inner.clone();
        let topic = static_topic(topic)?;
        let payload = payload_bytes(payload)?;
        let provenance = provenance.inner.clone();
        future_into_py(py, async move {
            scope
                .send(topic, payload, &provenance)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Request and reply as this agent: publish to `request_topic` and await
    /// the correlated reply on `reply_topic` up to `timeout_secs`.
    #[pyo3(signature = (request_topic, reply_topic, payload, provenance, *, timeout_secs=30.0))]
    fn ask<'py>(
        &self,
        py: Python<'py>,
        request_topic: String,
        reply_topic: String,
        payload: &Bound<'_, PyAny>,
        provenance: &PyProvenance,
        timeout_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scope = self.inner.clone();
        let request_topic = static_topic(request_topic)?;
        let reply_topic = static_topic(reply_topic)?;
        let payload = payload_bytes(payload)?;
        let provenance = provenance.inner.clone();
        let timeout = crate::convert::duration_seconds(timeout_secs, "timeout_secs")?;
        future_into_py(py, async move {
            let reply = scope
                .ask(request_topic, reply_topic, payload, &provenance, timeout)
                .await
                .map_err(to_pyerr)?;
            Ok(PyAgentMessage::from_inner(reply))
        })
    }

    /// Open a directed contract from this agent to one named `agent`, awaiting
    /// the reply up to `deadline_ms` (default 30000). Returns the terminal
    /// `Contract`, as `Laser.contract` does. Use
    /// `Laser.contract` for capability routing. Pass `agent=None` with `skill`
    /// to route by capability under the route `policy` word.
    #[pyo3(signature = (agent, payload, *, deadline_ms=30_000, skill=None, policy=None, fixed_inbox=None, principal=None, expire_if_not_consumed_ms=None, reply_on=None, conversation=None, fence=None, registered=false))]
    #[allow(clippy::too_many_arguments)]
    fn contract<'py>(
        &self,
        py: Python<'py>,
        agent: Option<String>,
        payload: Vec<u8>,
        deadline_ms: u64,
        skill: Option<String>,
        policy: Option<&Bound<'_, PyAny>>,
        fixed_inbox: Option<String>,
        principal: Option<u32>,
        expire_if_not_consumed_ms: Option<u64>,
        reply_on: Option<String>,
        conversation: Option<String>,
        fence: Option<u64>,
        registered: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let request = crate::agent_runtime::ContractRequest::new(
            skill,
            agent,
            payload,
            self.inner.id().as_str().to_owned(),
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
        let laser = self.laser.clone();
        future_into_py(py, async move {
            let outcome = request.send(&laser).await?;
            Python::attach(|py| crate::agent_runtime::PyContract::from_rust(py, outcome))
        })
    }

    /// Publish this agent's capability card (a dict mirroring `AgentCard`).
    fn publish_card<'py>(
        &self,
        py: Python<'py>,
        card: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scope = self.inner.clone();
        let card: AgentCard = py_to_de(card)?;
        future_into_py(py, async move {
            scope.publish_card(&card).await.map_err(to_pyerr)
        })
    }

    /// Advertise `capabilities` (skill ids or descriptor dicts) the way a
    /// spawning agent does: a durable card plus, where the server serves
    /// presence, this agent's live inbox at `listen_on`.
    fn advertise<'py>(
        &self,
        py: Python<'py>,
        listen_on: String,
        capabilities: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scope = self.inner.clone();
        let listen_on = static_topic(listen_on)?;
        let capabilities = self::capabilities(capabilities)?;
        future_into_py(py, async move {
            scope
                .advertise(listen_on, capabilities)
                .await
                .map_err(to_pyerr)
        })
    }
}

fn client_metadata_request(
    laser: &Laser,
    metadata_only: bool,
    principal: Option<u32>,
    limit: Option<u32>,
    after: Option<u32>,
) -> laser_sdk::agent::ClientMetadataRequest<'_> {
    let mut request = laser.client_metadata().with_metadata_only(metadata_only);
    if let Some(principal) = principal {
        request = request.principal(PrincipalId::new(principal));
    }
    if let Some(limit) = limit {
        request = request.limit(limit);
    }
    if let Some(after) = after {
        request = request.after(after);
    }
    request
}

/// Which consumer `Laser.consumed` probes: a deployment consumer group or a
/// named individual consumer.
#[gen_stub_pyclass_complex_enum]
#[pyclass(name = "ConsumerRef", frozen)]
pub enum PyConsumerRef {
    Group(String),
    Consumer(String),
}

impl PyConsumerRef {
    fn to_rust(&self) -> PyResult<ConsumerRef> {
        Ok(match self {
            Self::Group(name) => ConsumerRef::Group(
                ConsumerGroupName::new(name.clone()).map_err(|e| to_pyerr(e.into()))?,
            ),
            Self::Consumer(id) => ConsumerRef::Consumer(id.clone()),
        })
    }
}

/// Whether a target consumer has committed past a published message's position.
#[gen_stub_pyclass_complex_enum]
#[pyclass(name = "ConsumptionStatus", frozen, eq)]
#[derive(PartialEq)]
pub enum PyConsumptionStatus {
    /// The target's stored offset is still behind the message by `behind_by`.
    NotYetConsumed { behind_by: u64 },
    /// The target has committed past the message: `committed` is its stored
    /// offset, `head` the partition head at the time of the probe.
    Consumed { committed: u64, head: u64 },
}

impl From<ConsumptionStatus> for PyConsumptionStatus {
    fn from(status: ConsumptionStatus) -> Self {
        match status {
            ConsumptionStatus::NotYetConsumed { behind_by } => Self::NotYetConsumed { behind_by },
            ConsumptionStatus::Consumed { committed, head } => Self::Consumed { committed, head },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PyConsumerRef, PyConsumptionStatus};
    use laser_sdk::agent::{ConsumerRef, ConsumptionStatus};

    #[test]
    fn given_a_rust_status_when_converted_then_should_keep_its_offsets() {
        assert!(
            PyConsumptionStatus::from(ConsumptionStatus::Consumed {
                committed: 9,
                head: 12
            }) == PyConsumptionStatus::Consumed {
                committed: 9,
                head: 12
            }
        );
        assert!(
            PyConsumptionStatus::from(ConsumptionStatus::NotYetConsumed { behind_by: 3 })
                == PyConsumptionStatus::NotYetConsumed { behind_by: 3 }
        );
    }

    #[test]
    fn given_consumer_refs_when_converted_then_should_validate_group_names() {
        pyo3::Python::initialize();
        assert!(matches!(
            PyConsumerRef::Group("workers".to_owned()).to_rust(),
            Ok(ConsumerRef::Group(name)) if name.as_str() == "workers"
        ));
        assert!(matches!(
            PyConsumerRef::Consumer("probe".to_owned()).to_rust(),
            Ok(ConsumerRef::Consumer(id)) if id == "probe"
        ));
        assert!(PyConsumerRef::Group(String::new()).to_rust().is_err());
    }
}

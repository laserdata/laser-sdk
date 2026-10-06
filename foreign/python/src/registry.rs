use crate::agent::{PyAgentMessage, PyProvenance};
use crate::agent_runtime::static_topic;
use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{payload_bytes, py_to_de, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use crate::sign::PySigningKey;
use laser_sdk::agent::{AgentScope, ConsumerRef, ConsumptionStatus, RegisteredCard};
use laser_sdk::laser::Laser;
use laser_sdk::query::QueryExecutionId;
use laser_sdk::types::{AgentId, ConsumerGroupName, ConversationId, PrincipalId};
use laser_sdk::wire::agent::{
    AgentCard, AgentPresence, CapabilityDescriptor, ChannelId, LogPosition,
};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
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

    /// One page of live connections and their advertised metadata, as a dict
    /// `{"clients": [...], "next_cursor": int | None}`. Pass `next_cursor` back
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
            let page = request.page().await.map_err(to_pyerr)?;
            Python::attach(|py| {
                let dict = PyDict::new(py);
                dict.set_item("clients", ser_to_py(py, &page.clients)?)?;
                dict.set_item("next_cursor", page.next_cursor)?;
                Ok(dict.into_any().unbind())
            })
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
    /// them into ordered events (the same dicts `ChunkAssembler` returns).
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

    /// Whether a consumer has committed past a log position. Name the consumer
    /// with exactly one of `group` or `consumer`. `position` is
    /// `(stream_id, topic_id, partition_id, offset)`. Returns
    /// `{"consumed": True, "committed": int, "head": int}` or
    /// `{"consumed": False, "behind_by": int}`.
    #[pyo3(signature = (position, *, group=None, consumer=None))]
    fn consumed<'py>(
        &self,
        py: Python<'py>,
        position: (u32, u32, u32, u64),
        group: Option<String>,
        consumer: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let target = match (group, consumer) {
            (Some(group), None) => {
                ConsumerRef::Group(ConsumerGroupName::new(group).map_err(|e| to_pyerr(e.into()))?)
            }
            (None, Some(consumer)) => ConsumerRef::Consumer(consumer),
            _ => {
                return Err(InvalidError::new_err(
                    "pass exactly one of group= or consumer=",
                ));
            }
        };
        let (stream_id, topic_id, partition_id, offset) = position;
        let at = LogPosition {
            stream_id,
            topic_id,
            partition_id,
            offset,
        };
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let status = laser.consumed(target, at).await.map_err(to_pyerr)?;
            Python::attach(|py| {
                let dict = PyDict::new(py);
                match status {
                    ConsumptionStatus::Consumed { committed, head } => {
                        dict.set_item("consumed", true)?;
                        dict.set_item("committed", committed)?;
                        dict.set_item("head", head)?;
                    }
                    ConsumptionStatus::NotYetConsumed { behind_by } => {
                        dict.set_item("consumed", false)?;
                        dict.set_item("behind_by", behind_by)?;
                    }
                }
                Ok(dict.into_any().unbind())
            })
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

pub(crate) fn card_to_py(py: Python<'_>, card: &RegisteredCard) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("agent", card.agent.as_str())?;
    dict.set_item("card", ser_to_py(py, &card.card)?)?;
    dict.set_item("observed_at_micros", card.observed_at_micros)?;
    Ok(dict.into_any().unbind())
}

// The inverse of `card_to_py`, for the pure checks on a card dict.
fn card_from_py(card: &Bound<'_, PyDict>) -> PyResult<RegisteredCard> {
    let field = |name: &str| {
        card.get_item(name)?
            .ok_or_else(|| InvalidError::new_err(format!("registered card is missing '{name}'")))
    };
    Ok(RegisteredCard {
        agent: agent_id(field("agent")?.extract()?)?,
        card: py_to_de(&field("card")?)?,
        observed_at_micros: field("observed_at_micros")?.extract()?,
    })
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
    /// True when the registered `card` dict is within its time to live at
    /// `now_micros`. A card without a time to live is always fresh.
    #[staticmethod]
    fn card_is_fresh(card: &Bound<'_, PyDict>, now_micros: u64) -> PyResult<bool> {
        Ok(card_from_py(card)?.is_fresh(now_micros))
    }

    /// True when the registered `card` dict advertises `skill`.
    #[staticmethod]
    fn card_serves(card: &Bound<'_, PyDict>, skill: &str) -> PyResult<bool> {
        Ok(card_from_py(card)?.serves(skill))
    }

    /// True when the registered `card` dict advertises `skill` and reports it
    /// available.
    #[staticmethod]
    fn card_available_for(card: &Bound<'_, PyDict>, skill: &str) -> PyResult<bool> {
        Ok(card_from_py(card)?.available_for(skill))
    }

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

    /// Every folded card, as `{"agent", "card", "observed_at_micros"}` dicts.
    fn agents(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        self.read(|registry| registry.agents().map(|card| card_to_py(py, card)).collect())
    }

    /// The folded card for `agent`, or `None`.
    fn lookup(&self, py: Python<'_>, agent: String) -> PyResult<Option<Py<PyAny>>> {
        let agent = agent_id(agent)?;
        self.read(|registry| {
            registry
                .lookup(&agent)
                .map(|card| card_to_py(py, card))
                .transpose()
        })
    }

    /// The fresh, unquarantined cards advertising `skill_id` at `now_micros`
    /// (default now).
    #[pyo3(signature = (skill_id, now_micros=None))]
    fn resolve(
        &self,
        py: Python<'_>,
        skill_id: String,
        now_micros: Option<u64>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let now = now_micros.unwrap_or_else(self::now_micros);
        self.read(|registry| {
            registry
                .resolve(&skill_id, now)
                .into_iter()
                .map(|card| card_to_py(py, card))
                .collect()
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
    /// the reply up to `deadline_ms` (default 30000). Returns a dict with
    /// `state` and `body`, the shape `Laser.contract_report` returns. Use
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
            Python::attach(|py| crate::agent_runtime::contract_to_py(py, outcome))
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

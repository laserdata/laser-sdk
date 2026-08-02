use crate::agent::consumer::AgentMessage;
use crate::agent::router::Router;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::AgentId;
use laser_wire::agent::{AgentCard, CapabilityDescriptor};
use std::time::Duration;

impl Laser {
    /// The agent accessor: every verb on the returned scope acts as the agent
    /// `id` (sends carry it as the source, the card and presence advertise
    /// it). Free and synchronous, IO happens at the verbs. The coordination
    /// identity of the fabric, never an LLM wrapper.
    pub fn agent(&self, id: AgentId) -> AgentScope {
        AgentScope {
            laser: self.clone(),
            id,
        }
    }
}

/// One agent identity over the fabric: publish as it, request/reply as it,
/// advertise its card and live inbox, or open a contract from it. Build it
/// with [`Laser::agent`]. The handler runtime itself stays
/// [`Agent::builder`](crate::agent::Agent): this scope is the client-side
/// face of the same identity.
#[derive(Clone)]
pub struct AgentScope {
    laser: Laser,
    id: AgentId,
}

impl AgentScope {
    /// Append `payload` to an agent `topic` as this agent: the provenance
    /// gains this id as its `agent` and the partition is keyed by
    /// conversation.
    pub async fn send(
        &self,
        topic: AgentTopic<'_>,
        payload: impl Into<Vec<u8>>,
        provenance: &Provenance,
    ) -> Result<(), LaserError> {
        let mut provenance = provenance.clone();
        provenance.agent = Some(self.id.clone());
        self.laser.send_agent(topic, payload, &provenance).await
    }

    /// Request/reply as this agent: publish to `request_topic` and await the
    /// correlated reply on `reply_topic` up to `timeout`.
    pub async fn ask(
        &self,
        request_topic: AgentTopic<'_>,
        reply_topic: AgentTopic<'_>,
        payload: impl Into<Vec<u8>>,
        provenance: &Provenance,
        timeout: Duration,
    ) -> Result<AgentMessage, LaserError> {
        let mut provenance = provenance.clone();
        provenance.agent = Some(self.id.clone());
        self.laser
            .request(request_topic, reply_topic, payload, &provenance, timeout)
            .await
    }

    /// Open a directed [`contract`](Laser::contract) from this agent to
    /// `router`'s target, with `.from(..)` already set.
    pub fn contract(&self, router: Router) -> crate::agent::contract::ContractBuilder<'_> {
        self.laser.contract(router).from(self.id.clone())
    }

    /// Publish this agent's capability card to the registry, so capability
    /// routing can discover it.
    pub async fn publish_card(&self, card: &AgentCard) -> Result<(), LaserError> {
        self.laser.publish_card(self.id.clone(), card).await
    }

    /// Advertise `capabilities` the way a spawning agent does: a durable card
    /// on the registry plus, where the server serves presence, this agent's
    /// live inbox at `listen_on`. Fails when this connection already belongs to
    /// another advertised agent, so presence cannot be overwritten silently.
    pub async fn advertise(
        &self,
        listen_on: AgentTopic<'static>,
        capabilities: Vec<CapabilityDescriptor>,
    ) -> Result<(), LaserError> {
        crate::agent::builder::advertise(&self.laser, &self.id, &listen_on, capabilities).await
    }

    /// This agent's id.
    pub fn id(&self) -> &AgentId {
        &self.id
    }
}

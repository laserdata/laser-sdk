use crate::agent::consumer::AgentMessage;
use crate::agent::router::Router;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::AgentId;
use std::time::Duration;

/// Handed to an `AgentHandler::handle` so it can reply, send, request, or fan out
/// without holding `Laser` itself. Causality (conversation_id, causal_parent,
/// root) is wired automatically off the message being handled.
pub struct AgentCtx<'a> {
    laser: &'a Laser,
    message: &'a AgentMessage,
    agent: Option<AgentId>,
    respond_on: Option<AgentTopic<'static>>,
}

impl<'a> AgentCtx<'a> {
    pub(crate) fn new(
        laser: &'a Laser,
        message: &'a AgentMessage,
        agent: Option<AgentId>,
        respond_on: Option<AgentTopic<'static>>,
    ) -> Self {
        Self {
            laser,
            message,
            agent,
            respond_on,
        }
    }

    /// The `Laser` handle, for operations the ctx helpers do not cover (`kv`, `query`, ...).
    pub fn laser(&self) -> &Laser {
        self.laser
    }

    /// The message currently being handled.
    pub fn message(&self) -> &AgentMessage {
        self.message
    }

    /// Reply on the agent's configured `respond_on` topic, chaining causality
    /// (causal_parent = this message) and routing back to its sender. Errors with
    /// `NoRespondTopic` if the agent was built without `respond_on`.
    pub async fn respond(&self, payload: impl Into<Vec<u8>>) -> Result<(), LaserError> {
        let topic = self.respond_on.clone().ok_or(LaserError::NoRespondTopic)?;
        let mut provenance = self.reply_provenance();
        if let Some(source) = &self.message.provenance.agent {
            Router::to(source.clone()).apply(&mut provenance);
        }
        self.laser.send_agent(topic, payload, &provenance).await
    }

    /// A reply provenance for `topic`, chained off this message. The caller sets
    /// routing and usage as needed. Useful when replying somewhere other than `respond_on`.
    pub async fn reply_on(
        &self,
        topic: AgentTopic<'_>,
        payload: impl Into<Vec<u8>>,
    ) -> Result<(), LaserError> {
        let provenance = self.reply_provenance();
        self.laser.send_agent(topic, payload, &provenance).await
    }

    /// Send `payload` to `topic` with an explicit `provenance` (no automatic causality).
    pub async fn send(
        &self,
        topic: AgentTopic<'_>,
        payload: impl Into<Vec<u8>>,
        provenance: &Provenance,
    ) -> Result<(), LaserError> {
        self.laser.send_agent(topic, payload, provenance).await
    }

    /// Send a request and await its correlated reply (see `Laser::request`).
    pub async fn request(
        &self,
        request_topic: AgentTopic<'_>,
        reply_topic: AgentTopic<'_>,
        payload: impl Into<Vec<u8>>,
        provenance: &Provenance,
        timeout: Duration,
    ) -> Result<AgentMessage, LaserError> {
        self.laser
            .request(request_topic, reply_topic, payload, provenance, timeout)
            .await
    }

    /// Resolve the human-in-the-loop interrupt being handled: publish an AGDX
    /// `response` on `reply_topic` carrying the handled command's interrupt
    /// correlation, so the paused [`Agdx::request_input`](crate::agent::Agdx::request_input)
    /// caller resumes with `response`. The pairing is the correlation, so the
    /// reply reaches the right waiter even when several share `reply_topic`.
    /// Errors if the handled message is not an AGDX envelope carrying a
    /// correlation, or the agent was built without an id.
    pub async fn respond_input(
        &self,
        reply_topic: AgentTopic<'static>,
        response: impl Into<Vec<u8>>,
    ) -> Result<(), LaserError> {
        let envelope = self.message.envelope.as_ref().ok_or_else(|| {
            LaserError::Handler(
                "respond_input: the handled message is not an AGDX envelope".to_owned(),
            )
        })?;
        let correlation = envelope.correlation.ok_or_else(|| {
            LaserError::Handler("respond_input: the interrupt carries no correlation".to_owned())
        })?;
        let source = self
            .agent
            .as_ref()
            .ok_or_else(|| LaserError::Handler("respond_input: the agent has no id".to_owned()))?
            .wire_id();
        self.laser
            .agdx(reply_topic, source, envelope.conversation)
            .respond(correlation, response.into())
            .send()
            .await?;
        Ok(())
    }

    /// A child conversation of the handled message, linked by parent/root ids.
    pub fn spawn_subconversation(&self) -> Provenance {
        self.laser.spawn_subconversation(&self.message.provenance)
    }

    fn reply_provenance(&self) -> Provenance {
        let mut provenance = Provenance::builder()
            .conversation_id(self.message.provenance.conversation_id)
            .causal_parent(self.message.id)
            .build();
        provenance.agent = self.agent.clone();
        provenance.root_conversation_id = self.message.provenance.root_conversation_id;
        // Echo the request's idempotency_key back so the caller's request /
        // reply correlator can identify this reply unambiguously. Without this
        // a reply with only conversation_id matching would be hijackable when
        // multiple agents share a reply topic.
        provenance.idempotency_key = self.message.provenance.idempotency_key.clone();
        provenance
    }
}

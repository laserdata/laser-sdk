use crate::agent::consumer::{AgentConsumer, AgentHandler, Deduplicator};
use crate::agent::router::InboxRoute;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::AgentId;
use laser_wire::agent::{AgentCard, CapabilityDescriptor};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::warn;

/// One agent = one Iggy consumer group.
///
/// `spawn` uses `id.to_string()` as the consumer-group name. Horizontal
/// scaling (multiple replicas of the same agent) means multiple processes
/// joining that group, and Iggy load-balances messages across them.
///
/// **Do NOT put two different agent ids in the same group.** The
/// `ReliableConsumer` skips messages whose `agdx.to` does not
/// match its own `id`, which is the right behavior when a publisher
/// addresses a specific agent on a shared topic. If two agents shared
/// a group, Iggy would still deliver each message to exactly one member,
/// and if it landed on the wrong one the filter would drop it and the
/// intended agent would never see it. Each `Agent::builder().id(...).spawn(..)`
/// call is its own consumer group. Do not bypass that by reusing the
/// consumer group name across distinct agent ids.
#[derive(bon::Builder)]
pub struct Agent<H> {
    /// The agent's id, also used as its Iggy consumer-group name.
    pub id: AgentId,
    /// The topic this agent consumes.
    pub listen_on: AgentTopic<'static>,
    /// The handler invoked for each message.
    pub handler: H,
    /// Default reply topic for `AgentCtx::respond` (none = `respond` errors).
    pub respond_on: Option<AgentTopic<'static>>,
    /// Default route from a resolved target agent to the topic its work is sent
    /// on, used by [`AgentCtx::fan_out`](crate::agent::AgentCtx::fan_out) and the
    /// directed-send helpers. Sticks as this agent's routing convention. Defaults
    /// to [`InboxRoute::Advertised`] (resolve each target to its live-presence
    /// inbox). Set [`InboxRoute::Fixed`] for a workflow topic the caller owns.
    #[builder(default)]
    pub inbox_route: InboxRoute,
    /// Override the consumer poll interval (default: the runtime's reactive 10ms).
    pub poll_interval: Option<Duration>,
    #[builder(default)]
    pub warm_dedup: bool,
    /// Override the dedup backend (e.g. a durable `StateStore`-backed one). Defaults
    /// to the in-memory sliding window.
    pub deduplicator: Option<Box<dyn Deduplicator>>,
    /// What this agent serves. When non-empty, on spawn the agent publishes a
    /// capability card to the registry (so capability routing can discover it) and
    /// advertises its inbox (its `listen_on` topic) as live presence (so a fan-out
    /// resolves where to send it work). Empty means a private agent that does not
    /// advertise, the prior behavior.
    #[builder(default)]
    pub capabilities: Vec<CapabilityDescriptor>,
    /// Emit a `Working` status the moment an AGDX command is picked up, before the
    /// handler runs, so a [`Laser::contract`](crate::laser::Laser::contract) caller
    /// can tell the command was consumed. Off by default. Enable it on agents that
    /// take contracted work, with a `respond_on` set.
    #[builder(default)]
    pub ack_on_pickup: bool,
}

impl<H> Agent<H>
where
    H: AgentHandler + Sync + Send + 'static,
{
    /// Spawn the agent on `laser` as its own consumer group, returning a handle to stop it.
    pub fn spawn(self, laser: Laser) -> AgentHandle {
        let id = self.id.clone();
        let listen_on = self.listen_on.clone();
        let capabilities = self.capabilities;
        let group = self.id.to_string();
        let topic = self.listen_on.topic_string();
        let handler = self.handler;
        let respond_on = self.respond_on;
        let inbox_route = self.inbox_route;
        let ack_on_pickup = self.ack_on_pickup;
        let poll_interval = self.poll_interval;
        let warm_dedup = self.warm_dedup;
        let deduplicator = self.deduplicator;
        let (shutdown, shutdown_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            if !capabilities.is_empty() {
                advertise(&laser, &id, &listen_on, capabilities).await;
            }
            AgentConsumer::builder()
                .group(group)
                .topic(topic)
                .maybe_respond_on(respond_on)
                .inbox_route(inbox_route)
                .ack_on_pickup(ack_on_pickup)
                .maybe_poll_interval(poll_interval)
                .warm_dedup(warm_dedup)
                .maybe_deduplicator(deduplicator)
                .build()
                .run(&laser, handler, ready_tx, shutdown_rx)
                .await
        });
        AgentHandle {
            shutdown,
            task,
            ready: Some(ready_rx),
        }
    }
}

/// Advertise a capability-bearing agent before it starts consuming: publish a
/// non-expiring capability card to the registry (durable discovery), and, where
/// the server serves the presence command, advertise its inbox (its `listen_on`
/// topic) as live presence (so capability fan-out resolves where to send work).
/// Both are best-effort: a failure logs and the agent still runs, since a missing
/// advertisement degrades discovery but must not stop the agent.
async fn advertise(
    laser: &Laser,
    id: &AgentId,
    listen_on: &AgentTopic<'static>,
    capabilities: Vec<CapabilityDescriptor>,
) {
    let card = AgentCard {
        name: None,
        version: None,
        capabilities,
        ttl_micros: None,
    };
    if let Err(error) = laser.publish_card(id.clone(), &card).await {
        warn!(%error, agent = %id, "failed to publish the capability card");
    }
    #[cfg(feature = "query")]
    {
        let presence = laser_wire::agent::AgentPresence::new(id.wire_id())
            .with_inbox(listen_on.topic_string());
        if let Err(error) = laser.advertise_presence(&presence).await {
            // Expected against a streaming server without the presence command,
            // where routing falls back to a fixed inbox route. Not an error.
            tracing::debug!(%error, agent = %id, "inbox presence not advertised");
        }
    }
    #[cfg(not(feature = "query"))]
    let _ = listen_on;
}

/// Owns a spawned agent. Dropping it detaches the task and leaves it running, as
/// before. Call `shutdown` (or `join`) to stop it and observe a consumer error.
pub struct AgentHandle {
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), LaserError>>,
    ready: Option<oneshot::Receiver<()>>,
}

impl AgentHandle {
    /// Wait until the agent has joined its group and is polling, so a publish after
    /// this is delivered rather than racing the join. Idempotent.
    pub async fn ready(&mut self) -> Result<(), LaserError> {
        if let Some(ready) = self.ready.take() {
            ready.await.map_err(|_| {
                LaserError::Handler("agent stopped before it became ready".to_owned())
            })?;
        }
        Ok(())
    }

    /// Signal the agent to stop, wait for it, and surface any consumer error.
    pub async fn shutdown(self) -> Result<(), LaserError> {
        let _ = self.shutdown.send(());
        Self::join_task(self.task).await
    }

    /// Wait for the agent to finish (it runs until its consumer ends or errors).
    pub async fn join(self) -> Result<(), LaserError> {
        Self::join_task(self.task).await
    }

    /// Abort the agent's task immediately, without waiting.
    pub fn abort(&self) {
        self.task.abort();
    }

    async fn join_task(task: JoinHandle<Result<(), LaserError>>) -> Result<(), LaserError> {
        match task.await {
            Ok(result) => result,
            Err(join) => Err(LaserError::Handler(join.to_string())),
        }
    }
}

use crate::agent::consumer::{AgentConsumer, AgentHandler, Deduplicator};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::AgentId;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

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
    /// Override the consumer poll interval (default: the runtime's reactive 10ms).
    pub poll_interval: Option<Duration>,
    #[builder(default)]
    pub warm_dedup: bool,
    /// Override the dedup backend (e.g. a durable `StateStore`-backed one). Defaults
    /// to the in-memory sliding window.
    pub deduplicator: Option<Box<dyn Deduplicator>>,
}

impl<H> Agent<H>
where
    H: AgentHandler + Sync + Send + 'static,
{
    /// Spawn the agent on `laser` as its own consumer group, returning a handle to stop it.
    pub fn spawn(self, laser: Laser) -> AgentHandle {
        let group = self.id.to_string();
        let topic = self.listen_on.topic_string();
        let handler = self.handler;
        let respond_on = self.respond_on;
        let poll_interval = self.poll_interval;
        let warm_dedup = self.warm_dedup;
        let deduplicator = self.deduplicator;
        let (shutdown, shutdown_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            AgentConsumer::builder()
                .group(group)
                .topic(topic)
                .maybe_respond_on(respond_on)
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

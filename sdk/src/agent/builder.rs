use crate::agent::consumer::{
    AgentHandler, AgentMiddleware, ConcurrencyPolicy, DeadLetterSink, Deduplicator,
    ReliableConsumer, RetryPolicy,
};
use crate::agent::router::InboxRoute;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::{AgentId, ConsumerGroupName};
use laser_wire::agent::{AgentCard, CapabilityDescriptor};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::warn;

/// One logical agent defaults to one Iggy consumer group.
///
/// `spawn` derives the group name from `id` unless `consumer_group` is set. Horizontal
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
    /// The agent's stable logical identity.
    pub id: AgentId,
    /// Deployment group for load balancing replicas. Defaults to the agent id's
    /// spelling but remains a distinct type and may be overridden explicitly.
    pub consumer_group: Option<ConsumerGroupName>,
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
    /// How long [`AgentHandle::shutdown`] waits for the in-flight message to
    /// finish draining before the consumer is dropped. [`AgentHandle::abort`] is
    /// the unconditional hard stop. Defaults to the consumer's 30s grace.
    pub shutdown_grace: Option<Duration>,
    /// How message handling is scheduled across the partitions this agent is
    /// assigned. Defaults to [`ConcurrencyPolicy::Serial`]. Set
    /// [`ConcurrencyPolicy::SerialPerPartition`] so a slow or retrying message on
    /// one partition does not stall the others.
    pub concurrency: Option<ConcurrencyPolicy>,
    #[builder(default)]
    pub warm_dedup: bool,
    /// Cross-cutting hooks wrapped around each handler dispatch, in order, for
    /// auth, metrics, and tracing without touching the handler.
    #[builder(default)]
    pub middleware: Vec<Arc<dyn AgentMiddleware>>,
    /// Notified on every dead-letter with the result of publishing it, so a lost
    /// poison message is an observable event rather than only a log line.
    pub on_dead_letter: Option<Arc<dyn DeadLetterSink>>,
    /// Override the dedup window size (recent idempotency keys kept in memory).
    /// Defaults to the [`ReliableConsumer`] default when unset.
    pub dedup_window: Option<usize>,
    /// Override the handler retry policy (attempts + backoff for a retryable
    /// handler error). Defaults to the [`RetryPolicy`] default when unset.
    pub retry: Option<RetryPolicy>,
    /// Override the dedup backend (e.g. a durable `StateStore`-backed one). Defaults
    /// to the in-memory sliding window.
    pub deduplicator: Option<Box<dyn Deduplicator>>,
    /// The signature verifier for control and effect topics: when set, every
    /// message's envelope signature is verified against this registry before
    /// dispatch, and an unsigned or unverified record is dead-lettered. This is
    /// the enforcement gate for authorship and authorization, so it is reachable
    /// from the primary builder rather than only the lower-level consumer.
    #[cfg(feature = "sign")]
    pub verifier: Option<std::sync::Arc<crate::sign::KeyRegistry>>,
    /// This agent's signing identity: when set, `AgentCtx::respond` answers a
    /// correlated command with a signed AGDX response instead of a plain reply,
    /// so a caller holding a verifier (and the contract path's signer binding)
    /// can accept this agent's terminals and no one else's.
    #[cfg(feature = "sign")]
    pub signing_key: Option<std::sync::Arc<crate::sign::SigningKey>>,
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
    /// Run `consolidator` every `consolidate_every` off the handler loop (its
    /// own task, stopped with the agent). Both must be set for the tick to
    /// exist. Absent means no background consolidation, ever. The scope is the
    /// consolidator's own concern (a `DefaultConsolidator` holds its memory).
    pub consolidate_every: Option<Duration>,
    /// The consolidation pass the periodic tick runs (see
    /// [`consolidate_every`](Self::consolidate_every)).
    pub consolidator: Option<crate::memory::SharedConsolidator>,
    /// The pre-effect policy hook applied to everything this agent's handler
    /// publishes (`ctx.send`/`respond`/`request`/`fan_out`, AGDX verbs, memory
    /// writes), with the [`GovernorMode`](crate::govern::GovernorMode) it runs
    /// under. Spawn re-scopes the agent's `Laser` with it, so a governor set on
    /// the connection is replaced for this agent. See
    /// [`Laser::with_governor`](crate::laser::Laser::with_governor).
    pub governor: Option<(
        Arc<dyn crate::govern::ActionGovernor>,
        crate::govern::GovernorMode,
    )>,
}

impl<H> Agent<H>
where
    H: AgentHandler + Sync + Send + 'static,
{
    /// Spawn the agent on `laser` as its own consumer group, returning a handle to stop it.
    #[tracing::instrument(target = "laser", level = "info", skip_all, fields(agent = %self.id, operation = "spawn"))]
    pub fn spawn(self, laser: Laser) -> AgentHandle {
        let laser = match self.governor {
            Some((governor, mode)) => laser.with_governor(governor, mode),
            None => laser,
        };
        let id = self.id.clone();
        let listen_on = self.listen_on.clone();
        let capabilities = self.capabilities;
        let group = self
            .consumer_group
            .unwrap_or_else(|| ConsumerGroupName::for_agent(&id));
        let topic = self.listen_on.topic_string();
        let handler = self.handler;
        let respond_on = self.respond_on;
        let inbox_route = self.inbox_route;
        let ack_on_pickup = self.ack_on_pickup;
        let poll_interval = self.poll_interval;
        let warm_dedup = self.warm_dedup;
        let deduplicator = self.deduplicator;
        let shutdown_grace = self.shutdown_grace;
        let concurrency = self.concurrency;
        let dedup_window = self.dedup_window;
        let retry = self.retry;
        let middleware = self.middleware;
        let on_dead_letter = self.on_dead_letter;
        #[cfg(feature = "sign")]
        let verifier = self.verifier;
        #[cfg(feature = "sign")]
        let signing_key = self.signing_key;
        let (shutdown, shutdown_rx) = oneshot::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        // The consolidation tick, off the handler loop: best-effort, logged,
        // aborted with the agent. No background magic unless both knobs are set.
        let consolidation = match (self.consolidate_every, self.consolidator) {
            (Some(every), Some(consolidator)) => Some(tokio::spawn(async move {
                let mut tick = tokio::time::interval(every);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    let scope = crate::memory::MemoryScope::default();
                    if let Err(error) =
                        crate::memory::Consolidator::consolidate(&consolidator, &scope).await
                    {
                        warn!(%error, "background consolidation pass failed");
                    }
                }
            })),
            _ => None,
        };
        let task = tokio::spawn(async move {
            if !capabilities.is_empty() {
                advertise(&laser, &id, &listen_on, capabilities).await?;
            }
            let consumer = ReliableConsumer::builder()
                .group(group)
                .agent(id)
                .topic(topic)
                .maybe_respond_on(respond_on)
                .inbox_route(inbox_route)
                .ack_on_pickup(ack_on_pickup)
                .maybe_poll_interval(poll_interval)
                .maybe_shutdown_grace(shutdown_grace)
                .maybe_concurrency(concurrency)
                .warm_dedup(warm_dedup)
                .maybe_deduplicator(deduplicator)
                .maybe_dedup_window(dedup_window)
                .maybe_retry(retry)
                .middleware(middleware)
                .maybe_on_dead_letter(on_dead_letter);
            #[cfg(feature = "sign")]
            let consumer = consumer
                .maybe_verifier(verifier)
                .maybe_signing_key(signing_key);
            consumer
                .build()
                .run(&laser, handler, ready_tx, shutdown_rx)
                .await
        });
        AgentHandle {
            shutdown,
            task,
            ready: Some(ready_rx),
            consolidation,
        }
    }
}

/// Advertise a capability-bearing agent before it starts consuming: publish a
/// non-expiring capability card to the registry (durable discovery), and, where
/// the server serves the presence command, advertise its inbox (its `listen_on`
/// topic) as live presence (so capability fan-out resolves where to send work).
/// Both are best-effort: a failure logs and the agent still runs, since a missing
/// advertisement degrades discovery but must not stop the agent.
pub(crate) async fn advertise(
    laser: &Laser,
    id: &AgentId,
    listen_on: &AgentTopic<'static>,
    capabilities: Vec<CapabilityDescriptor>,
) -> Result<(), LaserError> {
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
            if matches!(error, LaserError::PresenceConflict { .. }) {
                return Err(error);
            }
            // Expected against a streaming server without the presence command,
            // where routing falls back to a fixed inbox route. Not an error.
            tracing::debug!(%error, agent = %id, "inbox presence not advertised");
        }
    }
    #[cfg(not(feature = "query"))]
    let _ = listen_on;
    Ok(())
}

/// Owns a spawned agent. Dropping it detaches the task and leaves it running, as
/// before. Call `shutdown` (or `join`) to stop it and observe a consumer error.
pub struct AgentHandle {
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), LaserError>>,
    ready: Option<oneshot::Receiver<()>>,
    // The background consolidation tick, aborted whenever the agent stops.
    consolidation: Option<JoinHandle<()>>,
}

impl AgentHandle {
    /// Wait until the agent has joined its group and is polling, so a publish after
    /// this is delivered rather than racing the join. Idempotent.
    pub async fn ready(&mut self) -> Result<(), LaserError> {
        if let Some(ready) = self.ready.take() {
            ready.await.map_err(|_| {
                LaserError::HandlerConfig("agent stopped before it became ready".to_owned())
            })?;
        }
        Ok(())
    }

    /// Signal the agent to stop, wait for it, and surface any consumer error.
    pub async fn shutdown(self) -> Result<(), LaserError> {
        let _ = self.shutdown.send(());
        if let Some(consolidation) = &self.consolidation {
            consolidation.abort();
        }
        Self::join_task(self.task).await
    }

    /// Wait for the agent to finish (it runs until its consumer ends or errors).
    pub async fn join(self) -> Result<(), LaserError> {
        let result = Self::join_task(self.task).await;
        if let Some(consolidation) = &self.consolidation {
            consolidation.abort();
        }
        result
    }

    /// Abort the agent's task immediately, without waiting.
    pub fn abort(&self) {
        self.task.abort();
        if let Some(consolidation) = &self.consolidation {
            consolidation.abort();
        }
    }

    async fn join_task(task: JoinHandle<Result<(), LaserError>>) -> Result<(), LaserError> {
        match task.await {
            Ok(result) => result,
            Err(join) => Err(LaserError::HandlerConfig(join.to_string())),
        }
    }
}

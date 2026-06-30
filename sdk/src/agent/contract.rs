use crate::agent::clock::{Clock, SystemClock};
use crate::agent::consumer::AgentMessage;
use crate::agent::laser::ContractTerminal;
use crate::agent::router::{CapabilitySelector, InboxRoute, Router};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::{AgentId, MintUlid};
use laser_wire::agent::{ConversationId, CorrelationId, IdempotencyKey};
use std::time::Duration;
use tokio::time::{Instant, sleep};

impl Laser {
    /// Scatter a directed contract to every agent advertising `selector`'s skill,
    /// concurrently, and collect the reply body of each that completes (excluding
    /// unavailable and quarantined agents, which capability resolution drops). The
    /// Laser-level gather the workflow's all-capable step and a verifier panel
    /// build on. Empty when no capable agent completes, [`LaserError::NoCapableAgent`]
    /// when none advertise the skill at all.
    pub async fn scatter(
        &self,
        source: AgentId,
        selector: &CapabilitySelector,
        payload: &[u8],
        inbox_route: &InboxRoute,
        deadline: Duration,
    ) -> Result<Vec<Vec<u8>>, LaserError> {
        let now = SystemClock.now_micros();
        let agents: Vec<AgentId> = {
            let mut registry = self.agent_registry()?;
            registry.refresh(now).await?;
            #[cfg(feature = "query")]
            if matches!(inbox_route, InboxRoute::Advertised) {
                registry.refresh_presence().await?;
            }
            registry
                .resolve(&selector.skill, now)
                .iter()
                .map(|card| card.agent.clone())
                .collect()
        };
        if agents.is_empty() {
            return Err(LaserError::NoCapableAgent {
                skill: selector.skill.clone(),
            });
        }

        let mut branches = tokio::task::JoinSet::new();
        for agent in agents {
            let laser = self.clone();
            let source = source.clone();
            let route = inbox_route.clone();
            let body = payload.to_vec();
            branches.spawn(async move {
                laser
                    .contract(Router::to(agent))
                    .from(source)
                    .payload(body)
                    .inbox_route(route)
                    .deadline(deadline)
                    .send()
                    .await
            });
        }

        let mut bodies = Vec::new();
        while let Some(joined) = branches.join_next().await {
            if let Ok(Ok(Contract::Completed(reply))) = joined {
                bodies.push(reply.body().to_vec());
            }
        }
        Ok(bodies)
    }
}

/// The outcome of a [`contract`](Laser::contract): a directed request to one
/// agent, with a deadline and an optional consumption expiry, resolved to one
/// terminal state.
#[derive(Debug)]
pub enum Contract {
    /// The target replied within the deadline (a non-error reply).
    Completed(AgentMessage),
    /// The target replied with a terminal `error`.
    Failed(AgentMessage),
    /// The command was not consumed within the expiry: no pickup acknowledgment
    /// landed, and the `deadline_micros` dropped it before any handler ran. Only
    /// reported when [`expire_if_not_consumed`](ContractBuilder::expire_if_not_consumed)
    /// is set and the target acknowledges pickup
    /// ([`Agent::ack_on_pickup`](crate::agent::Agent)).
    NotConsumed,
    /// Consumed (or no expiry was set) but no terminal reply landed within the
    /// completion deadline.
    TimedOut,
}

/// A directed request to one agent, bundling resolution, a deadline, and an
/// optional consumption expiry into one call. The "narrow and concrete"
/// orchestration request: hand a task to one agent (or one capability) and learn
/// the reply, whether it was picked up, or that it did not finish in time,
/// without hand-rolling correlation ids and timers.
///
/// Built by [`Laser::contract`], then `.send().await`. It is directed messaging
/// with confirmation over the log, never a bespoke consensus protocol: a thin
/// client-side state machine over the reply topic and two durations. The command
/// is an AGDX command, so a target built with
/// [`ack_on_pickup`](crate::agent::Agent) emits a `Working` status the contract
/// reads as the consumption signal.
pub struct ContractBuilder<'a> {
    laser: &'a Laser,
    router: Router,
    from: Option<AgentId>,
    payload: Vec<u8>,
    inbox_route: InboxRoute,
    reply_topic: AgentTopic<'static>,
    expiry: Option<Duration>,
    deadline: Duration,
    fence: Option<u64>,
    conversation: Option<ConversationId>,
}

impl Laser {
    /// Open a [`ContractBuilder`] addressed by `router` (one agent via
    /// [`Router::to`], or one capability via [`Router::to_capable`]). A broadcast
    /// or all-capable route is rejected at send time, since a contract is directed
    /// to exactly one target (use [`AgentCtx::fan_out`](crate::agent::AgentCtx::fan_out)
    /// to scatter).
    pub fn contract(&self, router: Router) -> ContractBuilder<'_> {
        ContractBuilder {
            laser: self,
            router,
            from: None,
            payload: Vec::new(),
            inbox_route: InboxRoute::default(),
            reply_topic: AgentTopic::Responses,
            expiry: None,
            deadline: Duration::from_secs(30),
            fence: None,
            conversation: None,
        }
    }
}

impl ContractBuilder<'_> {
    /// The agent id the contract sends as (the orchestrator). Required: the
    /// command is an AGDX command, which carries a source.
    pub fn from(mut self, source: AgentId) -> Self {
        self.from = Some(source);
        self
    }

    /// The task payload sent to the target.
    pub fn payload(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.payload = payload.into();
        self
    }

    /// How the target is resolved to the topic its work is sent on (default
    /// [`InboxRoute::Advertised`]).
    pub fn inbox_route(mut self, inbox_route: InboxRoute) -> Self {
        self.inbox_route = inbox_route;
        self
    }

    /// The topic the contract awaits the reply on (default
    /// [`AgentTopic::Responses`]). Must be where the target replies and acks.
    pub fn reply_on(mut self, reply_topic: AgentTopic<'static>) -> Self {
        self.reply_topic = reply_topic;
        self
    }

    /// Drop the command if it is not consumed within `expiry`: rides the command
    /// as its `deadline_micros`, so the reliable consumer drops an expired command
    /// before the handler runs. This is the consumption expiry, distinct from the
    /// completion [`deadline`](Self::deadline). Reported as
    /// [`Contract::NotConsumed`] only when the target acknowledges pickup.
    pub fn expire_if_not_consumed(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }

    /// How long to await a terminal reply before returning [`Contract::TimedOut`]
    /// (default 30s). Client-side, distinct from the consumption expiry.
    pub fn deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }

    /// Pin the conversation the command rides. The fence gate buckets its
    /// monotonic high-water mark by conversation, so a fenced task that may be
    /// re-dispatched (a reassignment, a workflow resume) must use the SAME
    /// conversation on every attempt, or each lands in its own bucket and the
    /// gate never compares the stale holder's token against the new one. Defaults
    /// to a fresh conversation per send, correct for a one-shot, non-fenced
    /// contract; [`fence`](Self::fence) is only meaningful with a pinned, stable
    /// conversation.
    pub fn conversation(mut self, conversation: crate::types::ConversationId) -> Self {
        self.conversation = Some(conversation.into());
        self
    }

    /// Stamp a fence token (`agdx.fence`) on the command. The token comes from a
    /// lease grant ([`Lease::token`](crate::kv::Lease)). It buys two distinct
    /// guarantees, and only together do they give at-most-once:
    ///
    /// - **Same-holder replay gate (local).** A consumer drops a command whose
    ///   fence is below the highest it has accepted for the task, so a stale
    ///   holder cannot act after a reassignment. This gate is per-consumer-process
    ///   and keyed by conversation, so it only bites when the task rides a stable
    ///   [`conversation`](Self::conversation) across re-dispatches; without that
    ///   pin (or across separate agent replicas, which do not share the gate) it
    ///   does nothing.
    /// - **Cross-holder effect gate (durable).** For an at-most-once EXTERNAL
    ///   effect across different holders or replicas, the handler must commit the
    ///   effect through a fenced compare-and-swap
    ///   ([`Kv::cas_fenced`](crate::kv::Kv::cas_fenced)) carrying this token, so
    ///   the plane rejects a lower-token writer at the sink. The engine provides
    ///   the token; the durable guarantee is the handler's to claim.
    pub fn fence(mut self, fence: u64) -> Self {
        self.fence = Some(fence);
        self
    }

    /// Resolve the target and its inbox, send the command, and run the contract
    /// state machine until a terminal state.
    pub async fn send(self) -> Result<Contract, LaserError> {
        let source = self.from.clone().ok_or_else(|| {
            LaserError::Invalid("a contract requires `.from(source agent id)`".to_owned())
        })?;
        let (target, inbox) = self.resolve().await?;

        let laser = self.laser;
        let expiry = self.expiry;
        let deadline = self.deadline;
        let conversation = self.conversation.unwrap_or_else(ConversationId::mint);
        let correlation = CorrelationId::mint();

        // Seed the reply reader at the topic tail BEFORE sending, so it reads only
        // the ack and the reply, never the topic's history.
        let mut reader = laser.agdx_reply_reader(self.reply_topic.clone()).await?;

        // Carry the correlation as the idempotency key too: a handler's
        // `ctx.respond` echoes the idempotency key (not the envelope correlation)
        // onto its `send_agent` reply, so this is what lets the contract match the
        // terminal. The `Working` ack matches on the envelope correlation directly.
        let idempotency = IdempotencyKey::try_from(correlation.to_string())
            .map_err(|error| LaserError::Invalid(format!("contract correlation: {error}")))?;
        let producer = laser.agdx(AgentTopic::Custom(&inbox), source.wire_id(), conversation);
        let mut command = producer
            .command(correlation, self.payload)
            .with_target(target.wire_id())
            .with_idempotency_key(idempotency);
        if let Some(fence) = self.fence {
            command = command.with_metadata(
                laser_wire::headers::FENCE,
                laser_wire::query::Value::Uint(fence),
            );
        }
        if let Some(expiry) = expiry {
            let at = SystemClock
                .now_micros()
                .saturating_add(expiry.as_micros().min(u128::from(u64::MAX)) as u64);
            command = command.with_deadline_micros(at);
        }
        command.send().await?;

        // Watch the reply topic for the pickup ack and the terminal, bounded by
        // the expiry and the completion deadline.
        let start = Instant::now();
        let expiry_at = expiry.map(|expiry| start + expiry);
        let deadline_at = start + deadline;
        let mut consumed = false;
        loop {
            let pass = reader.poll_contract(laser.client(), correlation).await?;
            match pass.terminal {
                Some(ContractTerminal::Completed(reply)) => return Ok(Contract::Completed(reply)),
                Some(ContractTerminal::Failed(reply)) => return Ok(Contract::Failed(reply)),
                None => {}
            }
            consumed |= pass.consumed;

            let now = Instant::now();
            if !consumed
                && let Some(expiry_at) = expiry_at
                && now >= expiry_at
            {
                return Ok(Contract::NotConsumed);
            }
            if now >= deadline_at {
                return Ok(Contract::TimedOut);
            }
            if !pass.read_any {
                // Never sleep past the deadline a tight contract set.
                let remaining = deadline_at.saturating_duration_since(Instant::now());
                sleep(Duration::from_millis(100).min(remaining)).await;
            }
        }
    }

    /// Resolve the route to exactly one target and its inbox topic in one registry
    /// read. Errors on a non-directed route or no match.
    async fn resolve(&self) -> Result<(AgentId, iggy::prelude::Identifier), LaserError> {
        if matches!(self.router, Router::Broadcast | Router::AllCapable(_)) {
            return Err(LaserError::Invalid(
                "a contract is directed to one agent, not a broadcast or all-capable route"
                    .to_owned(),
            ));
        }
        let mut registry = self.laser.agent_registry()?;
        let now = SystemClock.now_micros();
        registry.refresh(now).await?;
        #[cfg(feature = "query")]
        if matches!(self.inbox_route, InboxRoute::Advertised) {
            registry.refresh_presence().await?;
        }
        let target = self
            .router
            .resolve_targets(&registry, now)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                LaserError::Invalid("the contract route resolved no target".to_owned())
            })?;
        let inbox = self
            .inbox_route
            .resolve(&target, registry.inbox_for(&target))?;
        Ok((target, inbox))
    }
}

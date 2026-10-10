use crate::agent::clock::{Clock, SystemClock};
use crate::agent::consumer::AgentMessage;
use crate::agent::laser::ContractTerminal;
use crate::agent::router::{CapabilitySelector, InboxRoute, Router};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::{AgentId, MintUlid};
use laser_wire::agent::{ConversationId, CorrelationId};
use std::time::Duration;
use tokio::time::{Instant, sleep};

impl Laser {
    /// Scatter a directed contract to every agent advertising `selector`'s skill,
    /// concurrently, and collect the reply body of each that completes (excluding
    /// unavailable and quarantined agents, which capability resolution drops). The
    /// Laser-level gather the workflow's all-capable step and a verifier panel
    /// build on. Empty when no capable agent completes, [`LaserError::NoCapableAgent`]
    /// when none advertise the skill at all. A convenience over
    /// [`scatter_report`](Self::scatter_report), which keeps per-agent outcomes.
    pub async fn scatter(
        &self,
        source: AgentId,
        selector: &CapabilitySelector,
        payload: &[u8],
        inbox_route: &InboxRoute,
        deadline: Duration,
    ) -> Result<Vec<Vec<u8>>, LaserError> {
        let report = self
            .scatter_report(source, selector, payload, inbox_route, deadline)
            .await?;
        Ok(report
            .completed()
            .map(|(_, reply)| reply.body().to_vec())
            .collect())
    }

    /// Like [`scatter`](Self::scatter) but returns the outcome of every branch
    /// attributed to its agent, so a caller sees which agent completed, failed, or
    /// timed out rather than a bag of bodies. An all-failed scatter is a report of
    /// failures, distinguishable from an empty one (no completions).
    pub async fn scatter_report(
        &self,
        source: AgentId,
        selector: &CapabilitySelector,
        payload: &[u8],
        inbox_route: &InboxRoute,
        deadline: Duration,
    ) -> Result<ScatterReport, LaserError> {
        let now = SystemClock.now_micros();
        let agents: Vec<AgentId> = {
            let mut registry = self.agent_registry()?;
            registry.refresh(now).await?;
            #[cfg(feature = "query")]
            if matches!(inbox_route, InboxRoute::Advertised) || selector.principal.is_some() {
                registry.refresh_presence().await?;
            }
            Router::AllCapable(selector.clone()).resolve_targets(&registry, now)?
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
                let result = laser
                    .contract(Router::to(agent.clone()))
                    .from(source)
                    .payload(body)
                    .inbox_route(route)
                    .deadline(deadline)
                    .send()
                    .await;
                (agent, result)
            });
        }

        let mut outcomes = Vec::new();
        while let Some(joined) = branches.join_next().await {
            match joined {
                Ok((agent, result)) => outcomes.push(ScatterOutcome { agent, result }),
                Err(join_error) => {
                    tracing::warn!(%join_error, "scatter branch panicked");
                }
            }
        }
        Ok(ScatterReport { outcomes })
    }
}

/// One agent's outcome in a [`ScatterReport`].
#[derive(Debug)]
pub struct ScatterOutcome {
    /// The agent this branch was contracted to.
    pub agent: AgentId,
    /// Its terminal contract state, or the error that failed the branch.
    pub result: Result<Contract, LaserError>,
}

/// The per-agent outcome of a [`scatter_report`](Laser::scatter_report): every
/// contracted agent's terminal state, so an all-failed scatter is a report of
/// failures rather than an empty success.
#[derive(Debug)]
pub struct ScatterReport {
    /// One entry per contracted agent, in completion order.
    pub outcomes: Vec<ScatterOutcome>,
}

impl ScatterReport {
    /// The agents that completed, each with its reply.
    pub fn completed(&self) -> impl Iterator<Item = (&AgentId, &AgentMessage)> {
        self.outcomes
            .iter()
            .filter_map(|outcome| match &outcome.result {
                Ok(Contract::Completed(reply)) => Some((&outcome.agent, reply)),
                _ => None,
            })
    }

    /// The agents whose branch errored (an infrastructure or send failure), each
    /// with its cause. A non-completing terminal (`Failed`/`TimedOut`/`NotConsumed`)
    /// is an `Ok` outcome, read it from [`outcomes`](Self::outcomes).
    pub fn failures(&self) -> impl Iterator<Item = (&AgentId, &LaserError)> {
        self.outcomes
            .iter()
            .filter_map(|outcome| match &outcome.result {
                Err(error) => Some((&outcome.agent, error)),
                _ => None,
            })
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
    parent: Option<(ConversationId, ConversationId)>,
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
            reply_topic: AgentTopic::Sessions,
            expiry: None,
            deadline: Duration::from_secs(30),
            fence: None,
            conversation: None,
            parent: None,
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
    /// [`AgentTopic::Sessions`]). Must be where the target replies and acks.
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
    /// contract. [`fence`](Self::fence) is only meaningful with a pinned, stable
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
    ///   [`conversation`](Self::conversation) across re-dispatches. Without that
    ///   pin (or across separate agent replicas, which do not share the gate) it
    ///   does nothing.
    /// - **Cross-holder effect gate (durable).** For an at-most-once EXTERNAL
    ///   effect across different holders or replicas, the handler must commit the
    ///   effect through a fenced compare-and-swap
    ///   ([`Kv::cas_fenced`](crate::kv::Kv::cas_fenced)) carrying this token, so
    ///   the plane rejects a lower-token writer at the sink. The engine provides
    ///   the token. The durable guarantee is the handler's to claim.
    pub fn fence(mut self, fence: u64) -> Self {
        self.fence = Some(fence);
        self
    }

    /// Run the contract as a child session of `parent`, in the tree rooted at
    /// `root` (the parent itself when it has no parent). [`send`](Self::send)
    /// writes the child's submitted start on `agent.sessions` before the
    /// command, stamps the ancestry on the command, and ends the child by the
    /// outcome: completed on a reply, failed otherwise. A lifecycle record
    /// that fails to publish surfaces as the error unless the contract itself
    /// already erred.
    pub fn parent(
        mut self,
        parent: crate::types::ConversationId,
        root: crate::types::ConversationId,
    ) -> Self {
        self.parent = Some((parent.into(), root.into()));
        self
    }

    /// Resolve the target and its inbox, send the command, and run the contract
    /// state machine until a terminal state.
    #[tracing::instrument(target = "laser", level = "info", skip_all, fields(conversation = self.conversation.as_ref().map(tracing::field::display), operation = "contract"))]
    pub async fn send(self) -> Result<Contract, LaserError> {
        let source = self.from.clone().ok_or_else(|| {
            LaserError::Invalid("a contract requires `.from(source agent id)`".to_owned())
        })?;
        #[cfg(feature = "sign")]
        let expected_principal = self.router.required_principal();
        let (target, inbox) = self.resolve().await?;

        let laser = self.laser;
        let expiry = self.expiry;
        let deadline = self.deadline;
        let conversation = self.conversation.unwrap_or_else(ConversationId::mint);
        let correlation = CorrelationId::mint();

        // A child session's start lands before its command, so a reader never
        // sees work for a session that does not exist yet.
        let child = match self.parent {
            Some((parent, root)) => Some(
                start_child(
                    laser,
                    &source.wire_id(),
                    &target.wire_id(),
                    conversation,
                    parent,
                    root,
                )
                .await?,
            ),
            None => None,
        };

        // Seed the reply reader at the topic tail BEFORE sending, so it reads only
        // the ack and the reply, never the topic's history. Under a verifier the
        // reply signer must be the route's authenticated principal when one was
        // required, otherwise the resolved target's enrolled identity.
        let mut reader = laser
            .agdx_reply_reader(self.reply_topic.clone(), Some(&source.wire_id()))
            .await?;
        #[cfg(feature = "sign")]
        {
            reader.expected_signer = Some(
                expected_principal
                    .map(|principal| principal.to_string())
                    .unwrap_or_else(|| target.to_string()),
            );
        }

        // The command carries the AGDX envelope correlation. A worker handling it
        // sees that correlation as its message's `correlation_id` (synthesized from
        // the envelope), and `ctx.respond` echoes `correlation_id` onto its plain
        // reply, so the contract matches the terminal by correlation on both the
        // AGDX (`envelope.correlation`) and the plain (`agdx.corr`) shapes. The
        // `Working` ack matches on the envelope correlation directly.
        let producer = laser.agdx(AgentTopic::Custom(&inbox), source.wire_id(), conversation);
        let mut command = producer
            .command(correlation, self.payload)
            .with_target(target.wire_id());
        if let Some(fence) = self.fence {
            command = command.with_metadata(
                laser_wire::headers::FENCE,
                laser_wire::query::Value::Uint(fence),
            );
        }
        if let Some((parent, root)) = self.parent {
            command = command
                .with_ancestry(Some(parent), Some(root))
                .with_metadata(
                    laser_wire::agent::METADATA_SUBMITTED,
                    laser_wire::query::Value::Bool(true),
                );
        }
        if let Some(expiry) = expiry {
            let at = SystemClock
                .now_micros()
                .saturating_add(expiry.as_micros().min(u128::from(u64::MAX)) as u64);
            command = command.with_deadline_micros(at);
        }
        command.send().await?;

        let outcome = watch_terminal(laser, &mut reader, correlation, expiry, deadline).await;

        // End the child by the outcome before returning. A lifecycle record that
        // fails to publish surfaces as the error, except when the contract
        // itself already erred.
        if let Some(child) = child {
            let ended = match &outcome {
                Ok(Contract::Completed(_)) => child.end().await,
                Ok(other) => child.fail(contract_failure(failure_reason(other))).await,
                Err(error) => child.fail(contract_failure(error.to_string())).await,
            };
            if let (Ok(_), Err(error)) = (&outcome, ended) {
                return Err(error);
            }
        }
        outcome
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
        if matches!(self.inbox_route, InboxRoute::Advertised) || self.router.requires_presence() {
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

// Write a child session's submitted start as `source` and return its lens.
pub(crate) async fn start_child(
    laser: &Laser,
    source: &laser_wire::agent::AgentId,
    target: &laser_wire::agent::AgentId,
    conversation: ConversationId,
    parent: ConversationId,
    root: ConversationId,
) -> Result<crate::agent::Session, LaserError> {
    let sessions = laser.sessions();
    let start = laser_wire::agent::SessionStart {
        label: None,
        namespace: None,
        agent: target.clone(),
        sdk: sessions.config().sdk_info().clone(),
        parent: Some(parent),
        root: Some(root),
        idle_timeout_micros: Some(
            u64::try_from(sessions.config().idle_timeout_value().as_micros()).unwrap_or(u64::MAX),
        ),
        budget: None,
        tags: Vec::new(),
    };
    let session = sessions
        .open(conversation.into())
        .as_agent(source.clone())
        .with_ancestry(Some(parent.into()), Some(root.into()));
    session
        .lane()?
        .status(laser_wire::agent::OPERATION_SESSION)
        .with_task_state(laser_wire::agent::TaskState::Submitted)
        .body(laser_wire::framing::encode_named(&start)?)
        .content_type(laser_wire::content::ContentType::Cbor)
        .with_ancestry(Some(parent), Some(root))
        .send()
        .await?;
    Ok(session)
}

fn failure_reason(outcome: &Contract) -> String {
    match outcome {
        Contract::Completed(_) => "completed".to_owned(),
        Contract::Failed(_) => "the target replied with a terminal error".to_owned(),
        Contract::NotConsumed => "the command was not consumed within the expiry".to_owned(),
        Contract::TimedOut => "no terminal reply landed within the deadline".to_owned(),
    }
}

fn contract_failure(message: String) -> laser_wire::agent::AgentErrorBody {
    laser_wire::agent::AgentErrorBody {
        code: laser_wire::agent::AgentErrorCode::Internal,
        message: Some(message),
        retryable: false,
        detail: None,
    }
}

/// Watch the reply topic for the pickup ack and the terminal, bounded by the
/// consumption expiry and the completion deadline.
async fn watch_terminal(
    laser: &Laser,
    reader: &mut crate::agent::laser::AgentReplyReader,
    correlation: CorrelationId,
    expiry: Option<Duration>,
    deadline: Duration,
) -> Result<Contract, LaserError> {
    let start = Instant::now();
    let expiry_at = expiry.map(|expiry| start + expiry);
    let deadline_at = start + deadline;
    let mut consumed = false;
    loop {
        let pass = reader.poll_contract(&laser.client(), correlation).await?;
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

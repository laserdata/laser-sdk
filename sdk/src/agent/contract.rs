use crate::agent::clock::{Clock, SystemClock};
use crate::agent::consumer::{AgentMessage, DEAD_LETTER_REASON_DETAIL};
use crate::agent::ctx::{Branches, GatherPolicy, gather_branches};
use crate::agent::laser::ContractTerminal;
use crate::agent::replies::{ExpectedReply, ReplyTicket};
use crate::agent::router::{CapabilitySelector, InboxRoute, Router};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::{AgentId, MintUlid};
use laser_wire::agent::{AgentKind, ConversationId, CorrelationId, IdempotencyKey};
use std::time::Duration;
use tokio::time::Instant;

impl Laser {
    /// Scatter a directed contract to every agent advertising `selector`'s skill,
    /// concurrently, gather the branches under `policy` within `deadline`, and
    /// collect the reply body of each that completed (excluding unavailable and
    /// quarantined agents, which capability resolution drops). The Laser-level
    /// gather the workflow's all-capable step and a verifier panel build on.
    /// Empty when no capable agent completes, [`LaserError::NoCapableAgent`]
    /// when none advertise the skill at all. A convenience over
    /// [`scatter_report`](Self::scatter_report), which keeps per-agent outcomes,
    /// the cancelled branches, and whether the policy was met.
    pub async fn scatter(
        &self,
        source: AgentId,
        selector: &CapabilitySelector,
        payload: &[u8],
        inbox_route: &InboxRoute,
        policy: GatherPolicy,
        deadline: Duration,
    ) -> Result<Vec<Vec<u8>>, LaserError> {
        let report = self
            .scatter_report(source, selector, payload, inbox_route, policy, deadline)
            .await?;
        Ok(report
            .completed()
            .map(|(_, reply)| reply.body().to_vec())
            .collect())
    }

    /// Like [`scatter`](Self::scatter) but returns the outcome of every branch
    /// attributed to its agent, so a caller sees which agent completed, failed, or
    /// timed out rather than a bag of bodies. An all-failed scatter is a report of
    /// failures, distinguishable from an empty one (no completions). Under
    /// [`GatherPolicy::Quorum`] and [`GatherPolicy::BestEffort`] the branches
    /// still running when the gather stops are [`cancelled`](ScatterReport::cancelled).
    /// Errors on `Quorum(0)`.
    pub async fn scatter_report(
        &self,
        source: AgentId,
        selector: &CapabilitySelector,
        payload: &[u8],
        inbox_route: &InboxRoute,
        policy: GatherPolicy,
        deadline: Duration,
    ) -> Result<ScatterReport, LaserError> {
        policy.validate()?;
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

        let branch_count = agents.len();
        let mut branches = Branches::default();
        for agent in agents {
            let laser = self.clone();
            let source = source.clone();
            let route = inbox_route.clone();
            let body = payload.to_vec();
            let target = agent.clone();
            branches.spawn(agent, async move {
                laser
                    .contract(Router::to(target))
                    .from(source)
                    .payload(body)
                    .inbox_route(route)
                    .deadline(deadline)
                    .send()
                    .await
            });
        }

        let gathered = gather_branches(branches, policy, deadline, |contract| {
            matches!(contract, Contract::Completed(_))
        })
        .await;
        Ok(ScatterReport {
            outcomes: gathered
                .outcomes
                .into_iter()
                .map(|(agent, result)| ScatterOutcome { agent, result })
                .collect(),
            cancelled: gathered.cancelled,
            satisfied: policy.satisfied(gathered.successes, branch_count),
        })
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
/// finished branch's terminal state, so an all-failed scatter is a report of
/// failures rather than an empty success, plus the branches the gather policy
/// cancelled and whether the policy was met.
#[derive(Debug)]
pub struct ScatterReport {
    /// One entry per finished branch, in completion order.
    pub outcomes: Vec<ScatterOutcome>,
    /// The agents whose branch was still running when the policy stopped the
    /// gather.
    pub cancelled: Vec<AgentId>,
    /// Whether the gather met its [`GatherPolicy`].
    pub satisfied: bool,
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
    /// The command was not consumed: the target's consumer read it past its
    /// `deadline_micros` and dropped it before any handler ran, and answered
    /// so. Reported only on that answer, which a target built with a
    /// `respond_on` topic sends, never from a missing pickup acknowledgment.
    NotConsumed,
    /// Consumed (or no expiry was set) but no terminal reply landed within the
    /// completion deadline. The command carried that deadline, so a target that
    /// had not picked it up by then drops it.
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
/// client-side state machine over the shared reply dispatcher and two
/// durations. The command is an AGDX command, so a target built with
/// [`ack_on_pickup`](crate::agent::Agent) emits a `Working` status the contract
/// reads as the consumption signal.
pub struct ContractBuilder {
    laser: Laser,
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
    correlation: Option<CorrelationId>,
    idempotency_key: Option<IdempotencyKey>,
    resume_from: Option<Vec<u64>>,
    excluded: Vec<AgentId>,
}

impl Laser {
    /// Open a [`ContractBuilder`] addressed by `router` (one agent via
    /// [`Router::to`], or one capability via [`Router::to_capable`]). A broadcast
    /// or all-capable route is rejected at send time, since a contract is directed
    /// to exactly one target (use [`AgentCtx::fan_out`](crate::agent::AgentCtx::fan_out)
    /// or [`scatter`](Self::scatter) to reach many).
    pub fn contract(&self, router: Router) -> ContractBuilder {
        ContractBuilder {
            laser: self.clone(),
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
            correlation: None,
            idempotency_key: None,
            resume_from: None,
            excluded: Vec::new(),
        }
    }
}

impl ContractBuilder {
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
    /// as its `deadline_micros` when it is earlier than the completion
    /// [`deadline`](Self::deadline), so the reliable consumer drops an expired
    /// command before the handler runs. Reported as [`Contract::NotConsumed`]
    /// when the target answers that it dropped the command.
    pub fn expire_if_not_consumed(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }

    /// How long to await a terminal reply before returning [`Contract::TimedOut`]
    /// (default 30s). The command carries it as its `deadline_micros`, so a
    /// target that has not picked the command up by then drops it instead of
    /// working on a task its caller gave up on.
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
    ///   the managed backend rejects a lower-token writer at the sink. The engine provides
    ///   the token. The durable guarantee is the handler's to claim.
    pub fn fence(mut self, fence: u64) -> Self {
        self.fence = Some(fence);
        self
    }

    /// Run the contract as a child session of `parent`, in the tree rooted at
    /// `root` (the parent itself when it has no parent). [`send`](Self::send)
    /// writes the child's submitted start on `agent.sessions` before the
    /// command, stamps the ancestry on the command, and ends the child by the
    /// outcome: completed on a reply, failed otherwise, including a command
    /// that failed to publish. A lifecycle record that fails to publish
    /// surfaces as the error unless the contract itself already erred.
    pub fn parent(
        mut self,
        parent: crate::types::ConversationId,
        root: crate::types::ConversationId,
    ) -> Self {
        self.parent = Some((parent.into(), root.into()));
        self
    }

    // A correlation stable across sends of the same logical request, so a
    // reply to an earlier send still answers this one.
    pub(crate) fn correlation(mut self, correlation: CorrelationId) -> Self {
        self.correlation = Some(correlation);
        self
    }

    // The command's idempotency key, so the target's duplicate suppression
    // drops a repeated send it already handled.
    pub(crate) fn idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    // Look for a terminal from `positions` of the reply topic before sending,
    // and send only when none landed.
    pub(crate) fn resume_from(mut self, positions: Option<Vec<u64>>) -> Self {
        self.resume_from = positions;
        self
    }

    // Prefer a target outside `agents` when the route resolves another.
    #[cfg(feature = "kv")]
    pub(crate) fn excluding(mut self, agents: Vec<AgentId>) -> Self {
        self.excluded = agents;
        self
    }

    /// Resolve the target and its inbox, send the command, and run the contract
    /// state machine until a terminal state.
    #[tracing::instrument(target = "laser", level = "info", skip_all, fields(conversation = self.conversation.as_ref().map(tracing::field::display), operation = "contract"))]
    pub async fn send(self) -> Result<Contract, LaserError> {
        self.send_resolved().await.map(|(_, outcome)| outcome)
    }

    // `send`, also naming the agent the route resolved.
    pub(crate) async fn send_resolved(self) -> Result<(AgentId, Contract), LaserError> {
        let source = self.from.clone().ok_or_else(|| {
            LaserError::Invalid("a contract requires `.from(source agent id)`".to_owned())
        })?;
        #[cfg(feature = "sign")]
        let expected_principal = self.router.required_principal();
        let (target, inbox) = self.resolve().await?;

        let laser = &self.laser;
        let expiry = self.expiry;
        let deadline = self.deadline;
        let conversation = self.conversation.unwrap_or_else(ConversationId::mint);
        let correlation = self.correlation.unwrap_or_else(CorrelationId::mint);
        let expected = ExpectedReply {
            session: conversation.into(),
            requester: Some(source.as_str().to_owned()),
        };
        // Under a verifier the reply signer must be the route's authenticated
        // principal when one was required, otherwise the resolved target's
        // enrolled identity.
        #[cfg(feature = "sign")]
        let expected_signer = Some(
            expected_principal
                .map(|principal| principal.to_string())
                .unwrap_or_else(|| target.to_string()),
        );
        #[cfg(not(feature = "sign"))]
        let expected_signer = None;

        // Register with the shared reply dispatcher BEFORE sending, so the ack
        // and the reply, which cannot exist before the command, are never
        // missed.
        let reply_topic = laser.reply_topic_for(&self.reply_topic, Some(&source.wire_id()))?;
        let hub = laser.reply_hub(&AgentTopic::Custom(&reply_topic)).await?;
        let mut ticket = hub.subscribe(
            correlation.to_string(),
            ExpectedReply {
                session: expected.session,
                requester: expected.requester.clone(),
            },
            expected_signer.clone(),
        );
        ticket.arm(None);
        let landed = match self.resume_from {
            Some(positions) => {
                landed_terminal(
                    laser,
                    &self.reply_topic,
                    &source,
                    positions,
                    correlation,
                    &expected,
                    expected_signer,
                )
                .await?
            }
            None => None,
        };

        // A child session's start lands before its command, so a reader never
        // sees work for a session that does not exist yet.
        let child = match (self.parent, &landed) {
            (Some((parent, root)), None) => Some(
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
            (Some((parent, root)), Some(_)) => Some(
                laser
                    .sessions()
                    .open(conversation.into())
                    .as_agent(source.wire_id())
                    .with_ancestry(Some(parent.into()), Some(root.into())),
            ),
            (None, _) => None,
        };

        let outcome = match landed {
            Some(outcome) => Ok(outcome),
            None => {
                let sent = send_command(
                    laser,
                    &source,
                    &target,
                    &inbox,
                    conversation,
                    correlation,
                    CommandOptions {
                        payload: self.payload,
                        fence: self.fence,
                        parent: self.parent,
                        idempotency_key: self.idempotency_key,
                        lapse: expiry.map_or(deadline, |expiry| expiry.min(deadline)),
                    },
                )
                .await;
                match sent {
                    Ok(()) => watch_terminal(&mut ticket, expiry, deadline).await,
                    Err(error) => Err(error),
                }
            }
        };

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
        outcome.map(|outcome| (target, outcome))
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
        let targets =
            self.router
                .resolve_routable(&registry, &self.inbox_route, now, &self.excluded)?;
        let target = targets.first().cloned().ok_or_else(|| {
            LaserError::Invalid("the contract route resolved no target".to_owned())
        })?;
        let inbox = self
            .inbox_route
            .resolve(&target, registry.inbox_for(&target))?;
        Ok((target, inbox))
    }
}

// What the command carries besides its address.
struct CommandOptions {
    payload: Vec<u8>,
    fence: Option<u64>,
    parent: Option<(ConversationId, ConversationId)>,
    idempotency_key: Option<IdempotencyKey>,
    lapse: Duration,
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

// Publish the AGDX command. It carries the envelope correlation: a worker
// handling it sees that correlation as its message's `correlation_id`, and its
// reply echoes it, so the shared dispatcher completes this contract on both the
// AGDX and the plain reply shapes. Its `deadline_micros` is the earlier of the
// consumption expiry and the completion deadline, the point after which the
// target's consumer drops it instead of handling it.
async fn send_command(
    laser: &Laser,
    source: &AgentId,
    target: &AgentId,
    inbox: &iggy::prelude::Identifier,
    conversation: ConversationId,
    correlation: CorrelationId,
    options: CommandOptions,
) -> Result<(), LaserError> {
    let producer = laser.agdx(AgentTopic::Custom(inbox), source.wire_id(), conversation);
    let mut command = producer
        .command(correlation, options.payload)
        .with_target(target.wire_id())
        .with_deadline_at_micros(
            SystemClock
                .now_micros()
                .saturating_add(u64::try_from(options.lapse.as_micros()).unwrap_or(u64::MAX)),
        );
    if let Some(fence) = options.fence {
        command = command.with_metadata(
            laser_wire::headers::FENCE,
            laser_wire::query::Value::Uint(fence),
        );
    }
    if let Some((parent, root)) = options.parent {
        command = command
            .with_ancestry(Some(parent), Some(root))
            .with_metadata(
                laser_wire::agent::METADATA_SUBMITTED,
                laser_wire::query::Value::Bool(true),
            );
    }
    if let Some(key) = options.idempotency_key {
        command = command.with_idempotency_key(key);
    }
    command.send().await.map(|_| ())
}

// A terminal that already landed on the reply topic after `positions`, for a
// request sent before this process stopped.
async fn landed_terminal(
    laser: &Laser,
    reply_topic: &AgentTopic<'static>,
    source: &AgentId,
    positions: Vec<u64>,
    correlation: CorrelationId,
    expected: &ExpectedReply,
    expected_signer: Option<String>,
) -> Result<Option<Contract>, LaserError> {
    let mut reader =
        laser.agdx_reply_reader_from(reply_topic.clone(), &source.wire_id(), positions)?;
    #[cfg(feature = "sign")]
    {
        reader.expected_signer = expected_signer;
    }
    #[cfg(not(feature = "sign"))]
    let _ = expected_signer;
    loop {
        let pass = reader
            .poll_contract(&laser.client(), correlation, expected)
            .await?;
        match pass.terminal {
            Some(ContractTerminal::Completed(reply)) => {
                return Ok(Some(Contract::Completed(reply)));
            }
            Some(ContractTerminal::Failed(reply)) => return Ok(Some(Contract::Failed(reply))),
            None if !pass.read_any => return Ok(None),
            None => {}
        }
    }
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

// Await the terminal on the dispatcher ticket, bounded by the consumption
// expiry and the completion deadline.
async fn watch_terminal(
    ticket: &mut ReplyTicket,
    expiry: Option<Duration>,
    deadline: Duration,
) -> Result<Contract, LaserError> {
    let deadline_at = Instant::now() + deadline;
    if let Some(expiry) = expiry.filter(|expiry| *expiry <= deadline) {
        if let Some(reply) = ticket.next_within(expiry).await? {
            return Ok(terminal(reply));
        }
        // A missing pickup acknowledgment proves nothing: the target may lack
        // `ack_on_pickup` or be down. Only its answer that the expiry dropped
        // the command reads as not consumed.
        tracing::debug!(
            picked_up = ticket.consumed(),
            "The consumption expiry passed without a terminal reply"
        );
    }
    let remaining = deadline_at.saturating_duration_since(Instant::now());
    Ok(match ticket.next_within(remaining).await? {
        Some(reply) => terminal(reply),
        None => Contract::TimedOut,
    })
}

fn terminal(reply: AgentMessage) -> Contract {
    match reply.envelope.as_ref().map(|envelope| envelope.kind) {
        Some(AgentKind::Error) if dropped_unconsumed(&reply) => Contract::NotConsumed,
        Some(AgentKind::Error) => Contract::Failed(reply),
        _ => Contract::Completed(reply),
    }
}

// Whether the target answered that its consumer dropped the command past its
// deadline, before any handler ran.
fn dropped_unconsumed(reply: &AgentMessage) -> bool {
    let Some(envelope) = &reply.envelope else {
        return false;
    };
    laser_wire::framing::decode_named::<laser_wire::agent::AgentErrorBody>(&envelope.body)
        .ok()
        .and_then(|body| body.detail)
        .and_then(|detail| match detail.get(DEAD_LETTER_REASON_DETAIL) {
            Some(laser_wire::query::Value::Uint(code)) => Some(*code),
            Some(laser_wire::query::Value::Int(code)) => u64::try_from(*code).ok(),
            _ => None,
        })
        .is_some_and(|code| {
            code == u64::from(laser_wire::agent::DeadLetterReason::DeadlineExceeded.code())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn given_an_owned_contract_builder_when_spawned_then_should_send_on_its_own_task() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let contract = tokio::spawn(
            laser
                .contract(Router::to("worker".parse().expect("valid agent id")))
                .send(),
        );
        assert!(contract.await.expect("the contract task joins").is_err_and(
            |error| matches!(error, LaserError::Invalid(message) if message.contains("from"))
        ));
    }

    #[tokio::test]
    async fn given_a_zero_quorum_when_scattering_then_should_refuse_before_any_read() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let error = laser
            .scatter_report(
                "orchestrator".parse().expect("valid agent id"),
                &CapabilitySelector::new("review", crate::agent::RoutePolicy::Any),
                b"check",
                &InboxRoute::default(),
                GatherPolicy::Quorum(0),
                Duration::from_secs(1),
            )
            .await
            .expect_err("a zero quorum is refused");
        assert!(matches!(error, LaserError::Invalid(message) if message.contains("quorum")));
    }

    #[test]
    fn given_an_error_reply_when_classified_then_should_fail_the_contract() {
        let error = laser_wire::agent::AgentEnvelope::error(
            laser_wire::agent::RecordId::from_u128(3),
            ConversationId::from_u128(1),
            "worker".parse().expect("valid agent id"),
            CorrelationId::from_u128(2),
            Vec::new(),
        );
        let mut message = crate::testing::agent_message(
            Vec::new(),
            crate::provenance::Provenance::builder()
                .conversation_id(crate::types::ConversationId::new())
                .build(),
        );
        message.envelope = Some(error);
        assert!(matches!(terminal(message), Contract::Failed(_)));
        let plain = crate::testing::agent_message(
            b"done".to_vec(),
            crate::provenance::Provenance::builder()
                .conversation_id(crate::types::ConversationId::new())
                .build(),
        );
        assert!(matches!(terminal(plain), Contract::Completed(_)));
    }
}

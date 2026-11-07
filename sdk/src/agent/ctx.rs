use crate::agent::clock::{Clock, SystemClock};
use crate::agent::consumer::AgentMessage;
use crate::agent::router::{CapabilitySelector, InboxRoute, Router};
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::AgentId;
use iggy::prelude::IggyTimestamp;
use std::collections::HashMap;
use std::time::Duration;
use tokio::task::{self, JoinSet};

/// Handed to an `AgentHandler::handle` so it can reply, send, request, or fan out
/// without holding `Laser` itself. Causality (conversation_id, causal_parent,
/// root) is wired automatically off the message being handled.
pub struct AgentCtx<'a> {
    laser: &'a Laser,
    message: &'a AgentMessage,
    agent: Option<AgentId>,
    respond_on: Option<AgentTopic<'static>>,
    inbox_route: InboxRoute,
    request_at: Option<laser_wire::agent::LogPosition>,
    // The runtime's session factory and control state, when a reliable
    // consumer handed this context to its handler.
    sessions: Option<crate::agent::Sessions>,
    control: Option<std::sync::Arc<crate::agent::control::ControlBook>>,
    #[cfg(feature = "sign")]
    signing_key: Option<std::sync::Arc<crate::sign::SigningKey>>,
    approval: ApprovalSlot,
}

impl<'a> AgentCtx<'a> {
    pub(crate) fn new(
        laser: &'a Laser,
        message: &'a AgentMessage,
        agent: Option<AgentId>,
        respond_on: Option<AgentTopic<'static>>,
        inbox_route: InboxRoute,
        #[cfg(feature = "sign")] signing_key: Option<std::sync::Arc<crate::sign::SigningKey>>,
    ) -> Self {
        Self {
            laser,
            message,
            agent,
            respond_on,
            inbox_route,
            request_at: None,
            sessions: None,
            control: None,
            #[cfg(feature = "sign")]
            signing_key,
            approval: ApprovalSlot::default(),
        }
    }

    // The approval slot the runtime shares with this record's handler.
    pub(crate) fn with_approval(mut self, approval: ApprovalSlot) -> Self {
        self.approval = approval;
        self
    }

    /// The approval slot of the handled record, for a language binding that
    /// rebuilds this context per call.
    #[doc(hidden)]
    pub fn approval_slot(&self) -> ApprovalSlot {
        self.approval.clone()
    }

    // The session factory and control state of the runtime handling the
    // record, so `session` follows its configuration and control requests.
    pub(crate) fn with_sessions(
        mut self,
        sessions: crate::agent::Sessions,
        control: std::sync::Arc<crate::agent::control::ControlBook>,
    ) -> Self {
        self.sessions = Some(sessions);
        self.control = Some(control);
        self
    }

    /// Where the handled record sits on the log, when the runtime knows it.
    pub fn request_at(&self) -> Option<laser_wire::agent::LogPosition> {
        self.request_at
    }

    // Where the handled record sits on the log, stamped on typed replies as
    // their causal position.
    pub(crate) fn with_request_at(mut self, at: laser_wire::agent::LogPosition) -> Self {
        self.request_at = Some(at);
        self
    }

    /// Rebuild the runtime state of a handled record for a language binding:
    /// the signing key, the record's log position, and the session factory.
    #[doc(hidden)]
    pub fn with_runtime_state(
        mut self,
        #[cfg(feature = "sign")] signing_key: Option<std::sync::Arc<crate::sign::SigningKey>>,
        request_at: Option<laser_wire::agent::LogPosition>,
        sessions: Option<crate::agent::Sessions>,
        approval: Option<ApprovalSlot>,
    ) -> Self {
        #[cfg(feature = "sign")]
        {
            self.signing_key = signing_key;
        }
        self.request_at = request_at;
        self.sessions = sessions;
        if let Some(approval) = approval {
            self.approval = approval;
        }
        self
    }

    /// The `Laser` handle, for operations the ctx helpers do not cover (`kv`, `query`, ...).
    pub fn laser(&self) -> &Laser {
        self.laser
    }

    /// The message currently being handled.
    pub fn message(&self) -> &AgentMessage {
        self.message
    }

    // The agent handling the record, when the runtime has an id for it.
    pub(crate) fn agent(&self) -> Option<&AgentId> {
        self.agent.as_ref()
    }

    /// The session of the handled record, written as this agent and acting on
    /// the handled record: graph writes take it as source and remembered
    /// items as origin. A lens only: it holds no lease and starts no
    /// heartbeat. Its [`pending_control`](crate::agent::Session::pending_control)
    /// reports the pause and cancel requests the runtime observed.
    pub fn session(&self) -> crate::agent::Session {
        let conversation = self.message.provenance.conversation_id;
        let mut session = match &self.sessions {
            Some(sessions) => sessions.open(conversation),
            None => self.laser.sessions().open(conversation),
        };
        session.control.clone_from(&self.control);
        if let Some(agent) = &self.agent {
            session = session.as_agent(agent.wire_id());
        }
        if let Some(at) = self.request_at {
            session = session.acting_on(laser_wire::graph::SourceRef::Message {
                stream: at.stream_id,
                topic: at.topic_id,
                partition: at.partition_id,
                offset: at.offset,
                generation: None,
                conversation: Some(self.message.provenance.conversation_id.to_string()),
            });
        }
        session
    }

    /// Reply on the agent's configured `respond_on` topic, chaining causality
    /// (causal_parent = this message) and routing back to its sender. Errors with
    /// `NoRespondTopic` if the agent was built without `respond_on`.
    ///
    /// An agent built with a signing key answers a correlated command with a
    /// signed AGDX response instead, so a verifying caller (and the contract
    /// path's signer binding) accepts this agent's terminal and no one else's.
    pub async fn respond(&self, payload: impl Into<Vec<u8>>) -> Result<(), LaserError> {
        let topic = self.respond_on.clone().ok_or(LaserError::NoRespondTopic)?;
        let payload = payload.into();
        // A typed request gets a typed response: correlated, addressed to the
        // requester, and caused by the request's log position. A plain request
        // gets a plain reply matched by its string correlation.
        if let Some(envelope) = &self.message.envelope
            && let Some(correlation) = envelope.correlation
        {
            let source = self.agent.as_ref().ok_or_else(|| {
                LaserError::HandlerConfig("a responding agent must have an id".to_owned())
            })?;
            let producer = self
                .laser
                .agdx(topic, source.wire_id(), envelope.conversation);
            let mut send = producer
                .respond(correlation, payload)
                .with_target(envelope.source.clone())
                .with_ancestry(envelope.parent, envelope.root);
            if let Some(record) = envelope.record {
                send = send.with_cause(record, self.request_at);
            }
            #[cfg(feature = "sign")]
            if let Some(key) = &self.signing_key {
                send = send.signed_by(key);
            }
            return send.send().await.map(|_| ());
        }
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
        let mut provenance = self.reply_provenance();
        if let Some(source) = &self.message.provenance.agent {
            Router::to(source.clone()).apply(&mut provenance);
        }
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
    /// An agent built with a signing key signs the response, so a verifying
    /// interrupt caller accepts this agent's decision and no one else's.
    /// Errors if the handled message is not an AGDX envelope carrying a
    /// correlation, or the agent was built without an id.
    pub async fn respond_input(
        &self,
        reply_topic: AgentTopic<'static>,
        response: impl Into<Vec<u8>>,
    ) -> Result<(), LaserError> {
        let envelope = self.message.envelope.as_ref().ok_or_else(|| {
            LaserError::HandlerConfig(
                "respond_input: the handled message is not an AGDX envelope".to_owned(),
            )
        })?;
        let correlation = envelope.correlation.ok_or_else(|| {
            LaserError::HandlerConfig(
                "respond_input: the interrupt carries no correlation".to_owned(),
            )
        })?;
        let source = self
            .agent
            .as_ref()
            .ok_or_else(|| {
                LaserError::HandlerConfig("respond_input: the agent has no id".to_owned())
            })?
            .wire_id();
        let producer = self.laser.agdx(reply_topic, source, envelope.conversation);
        let mut send = producer
            .respond(correlation, response.into())
            .with_target(envelope.source.clone())
            .with_ancestry(envelope.parent, envelope.root);
        if let Some(record) = envelope.record {
            send = send.with_cause(record, self.request_at);
        }
        #[cfg(feature = "sign")]
        let send = match &self.signing_key {
            Some(key) => send.signed_by(key),
            None => send,
        };
        send.send().await?;
        Ok(())
    }

    /// Gate this handler on a decision by `approver`: publish `prompt` as an
    /// input request addressed to it in the handled conversation, with an
    /// `input-required` task status, and decide on its correlated reply on
    /// `reply_topic`. Only a reply authored by `approver`, and signed by it
    /// when a verifier is configured, decides. Returns the [`ApprovalGrant`]
    /// on approval, [`LaserError::Rejected`] when the approver rejects, and
    /// [`LaserError::Timeout`] once `timeout` passes without a decision. A
    /// governed effect re-sent with the grant does not step up again.
    ///
    /// Run by a reliable consumer whose agent reads `reply_topic` and whose
    /// stream has `agent.control` (the pause runtime), the gate parks instead
    /// of waiting: its first pass publishes the prompt and returns
    /// [`LaserError::PolicyDeferred`], the runtime parks the record and its
    /// session's later work and frees the consumer, and the decision replays
    /// the record through the handler, where the gate returns the decision.
    /// The handler code before the gate therefore runs again and must be
    /// idempotent. Without the pause runtime the gate waits in place. Errors
    /// if the agent was built without an id.
    pub async fn approval_gate(
        &self,
        approver: impl Into<AgentId>,
        reply_topic: AgentTopic<'_>,
        prompt: impl Into<Vec<u8>>,
        timeout: Duration,
    ) -> Result<ApprovalGrant, LaserError> {
        let source = self
            .agent
            .as_ref()
            .ok_or_else(|| {
                LaserError::HandlerConfig("approval_gate: the agent has no id".to_owned())
            })?
            .wire_id();
        let conversation = self.message.provenance.conversation_id;
        let approver = approver.into().wire_id();
        if let Some(at) = self
            .request_at
            .filter(|_| self.approval.parks_on(&reply_topic.topic_string()))
        {
            return self
                .parked_gate(source, approver, at, prompt.into(), timeout)
                .await;
        }
        let reply = self
            .laser
            .agdx(AgentTopic::Sessions, source, conversation.into())
            .input_reply(Some(approver), reply_topic, prompt.into(), timeout)
            .await?;
        Ok(ApprovalGrant {
            approver: reply.source,
            conversation,
            correlation: reply.correlation.ok_or_else(|| {
                LaserError::Protocol("an approval reply carries no correlation".to_owned())
            })?,
            decision: reply.body,
        })
    }

    // The parking gate: decide on the decision a replay carries, else publish
    // the prompt once and interrupt the handler.
    async fn parked_gate(
        &self,
        source: laser_wire::agent::AgentId,
        approver: laser_wire::agent::AgentId,
        at: laser_wire::agent::LogPosition,
        prompt: Vec<u8>,
        timeout: Duration,
    ) -> Result<ApprovalGrant, LaserError> {
        let conversation = self.message.provenance.conversation_id;
        let correlation = approval_correlation(at);
        let replayed = match self.approval.take_resume() {
            Some(ApprovalResume::Expired) => {
                return Err(LaserError::Timeout("the approval"));
            }
            Some(ApprovalResume::Decided(reply)) => {
                if let Some(decided) = self.decision_of(&reply, &approver, correlation) {
                    return decided.map(|decision| ApprovalGrant {
                        approver,
                        conversation,
                        correlation,
                        decision,
                    });
                }
                true
            }
            None => false,
        };
        let prompt_at = if replayed {
            // A reply from anyone but the approver does not decide, and the
            // prompt is already out.
            None
        } else {
            let producer = self
                .laser
                .agdx(AgentTopic::Sessions, source, conversation.into());
            producer
                .status(laser_wire::agent::OPERATION_TASK)
                .with_correlation(correlation)
                .with_task_state(laser_wire::agent::TaskState::InputRequired)
                .send()
                .await?;
            let deadline_micros = SystemClock
                .now_micros()
                .saturating_add(u64::try_from(timeout.as_micros()).unwrap_or(u64::MAX));
            let receipt = producer
                .command(correlation, prompt)
                .with_operation(laser_wire::dispatch::OPERATION_INPUT_REQUEST)
                .with_target(approver.clone())
                .with_deadline_at_micros(deadline_micros)
                .send_receipt()
                .await?;
            Some((
                laser_wire::agent::LogPosition::new(
                    at.stream_id,
                    at.topic_id,
                    receipt.partition_id.unwrap_or(at.partition_id),
                    receipt.offset.unwrap_or_default(),
                ),
                deadline_micros,
            ))
        };
        self.approval
            .interrupt(ApprovalInterrupt { prompt: prompt_at });
        Err(LaserError::PolicyDeferred(format!(
            "awaiting the approval of {approver}"
        )))
    }

    // The decision `reply` carries for the approval `correlation`: the body
    // on a response, the rejection on an error, `None` when the reply is not
    // the approver's.
    fn decision_of(
        &self,
        reply: &AgentMessage,
        approver: &laser_wire::agent::AgentId,
        correlation: laser_wire::agent::CorrelationId,
    ) -> Option<Result<Vec<u8>, LaserError>> {
        let envelope = reply.envelope.as_ref()?;
        if envelope.correlation != Some(correlation) || envelope.source != *approver {
            return None;
        }
        #[cfg(feature = "sign")]
        if self.laser.registry_verifier().is_some()
            && reply.verified_principal.as_deref() != Some(approver.as_str())
        {
            return None;
        }
        match envelope.kind {
            laser_wire::agent::AgentKind::Response => Some(Ok(envelope.body.clone())),
            laser_wire::agent::AgentKind::Error => {
                let body = laser_wire::framing::decode_named::<laser_wire::agent::AgentErrorBody>(
                    &envelope.body,
                )
                .ok();
                if body.as_ref().is_some_and(|body| {
                    body.code == laser_wire::agent::AgentErrorCode::DeadlineExceeded
                }) {
                    return Some(Err(LaserError::Timeout("the approval")));
                }
                Some(Err(LaserError::Rejected(
                    body.and_then(|body| body.message)
                        .unwrap_or_else(|| "the input request was rejected".to_owned()),
                )))
            }
            _ => None,
        }
    }

    /// A child conversation of the handled message, linked by parent/root ids.
    pub fn spawn_subconversation(&self) -> Result<Provenance, LaserError> {
        let author = self.agent.as_ref().ok_or_else(|| {
            LaserError::HandlerConfig("spawn_subconversation: the agent has no id".to_owned())
        })?;
        Ok(self
            .laser
            .spawn_subconversation(&self.message.provenance, author))
    }

    /// Fan out to every agent advertising `selector`'s skill, gathering replies
    /// under `policy` within `deadline`. Each branch is a sub-conversation routed
    /// to one agent's own inbox, resolved through the ctx's
    /// [`InboxRoute`](crate::agent::InboxRoute) (the per-agent live-presence inbox
    /// by default, or a fixed topic the caller set at build time), never a shared
    /// topic name baked into the SDK. Replies are collected on the orchestrator's
    /// own response topic (`respond_on`), so a deployment that scopes each user
    /// to its own stream and each workflow to its own topic routes correctly.
    /// Every branch carries the deadline as its `deadline_micros`, so a target
    /// that picks it up late drops it instead of working past the gather.
    ///
    /// A target that resolves no inbox (advertised none, or [`refresh_presence`](crate::agent::AgentRegistry::refresh_presence)
    /// surfaced none) is a per-branch [`Gather::failures`] entry, never silently
    /// rerouted. Branches still running when the policy stops the gather are
    /// [`Gather::cancelled`]. Errors on `Quorum(0)` and on an infrastructure
    /// failure (no capable agent, no response topic, registry read), not on a
    /// branch failure.
    pub async fn fan_out(
        &self,
        selector: CapabilitySelector,
        payload: impl Into<Vec<u8>>,
        policy: GatherPolicy,
        deadline: Duration,
    ) -> Result<Gather, LaserError> {
        policy.validate()?;
        // One buffer, refcount-cloned per branch: the fan-out never copies the body.
        let payload = payload.into();
        // The orchestrator's own response topic is where every branch's reply
        // lands. Required: without it there is nowhere to gather to.
        let reply_topic = self.respond_on.clone().ok_or(LaserError::NoRespondTopic)?;

        let mut registry = self.laser.agent_registry()?;
        let now = SystemClock.now_micros();
        registry.refresh(now).await?;
        // The advertised-inbox path needs live presence. The fixed path does not.
        #[cfg(feature = "query")]
        if matches!(self.inbox_route, InboxRoute::Advertised) || selector.principal.is_some() {
            registry.refresh_presence().await?;
        }
        let targets = Router::AllCapable(selector).resolve_targets(&registry, now)?;
        let author = self
            .agent
            .clone()
            .ok_or_else(|| LaserError::HandlerConfig("fan_out: the agent has no id".to_owned()))?;
        let deadline_at = IggyTimestamp::from(
            now.saturating_add(u64::try_from(deadline.as_micros()).unwrap_or(u64::MAX)),
        );

        // Resolve each target's inbox while the registry borrow is live, so the
        // owned topic identifier can move into the spawned branch. A target that
        // resolves no inbox fails its branch here rather than mis-routing.
        let mut unrouted = Vec::new();
        let mut branches = Branches::default();
        for agent in targets {
            let advertised = registry.inbox_for(&agent);
            let inbox = match self.inbox_route.resolve(&agent, advertised) {
                Ok(inbox) => inbox,
                Err(error) => {
                    unrouted.push((agent, error));
                    continue;
                }
            };
            let laser = self.laser.clone();
            let body = payload.clone();
            let parent = self.message.provenance.clone();
            let author = author.clone();
            let reply_topic = reply_topic.clone();
            let target = agent.clone();
            branches.spawn(agent, async move {
                let mut provenance = laser.spawn_subconversation(&parent, &author);
                provenance.target_agent_id = Some(target);
                provenance.deadline_at = Some(deadline_at);
                // A distinct correlation per branch so replies never cross.
                provenance.correlation_id = Some(ulid::Ulid::generate().to_string());
                laser
                    .request(
                        AgentTopic::Custom(&inbox),
                        reply_topic,
                        body,
                        &provenance,
                        deadline,
                    )
                    .await
            });
        }

        let branch_count = branches.len() + unrouted.len();
        let gathered = gather_branches(branches, policy, deadline, |_| true).await;
        let mut gather = Gather {
            satisfied: policy.satisfied(gathered.successes, branch_count),
            cancelled: gathered.cancelled,
            failures: unrouted,
            ..Gather::default()
        };
        for (agent, result) in gathered.outcomes {
            match result {
                Ok(reply) => gather.ok.push((agent, reply)),
                Err(error) => gather.failures.push((agent, error)),
            }
        }
        Ok(gather)
    }

    fn reply_provenance(&self) -> Provenance {
        let mut provenance = Provenance::builder()
            .conversation_id(self.message.provenance.conversation_id)
            .causal_parent(self.message.id)
            .build();
        provenance.agent = self.agent.clone();
        provenance.parent_conversation_id = self.message.provenance.parent_conversation_id;
        provenance.root_conversation_id = self.message.provenance.root_conversation_id;
        // Echo the request's correlation back so the caller's request/reply
        // correlator can identify this reply unambiguously. Without this a reply
        // with only conversation_id matching would be hijackable when multiple
        // agents share a reply topic. The request's business idempotency_key is
        // deliberately NOT echoed: the reply is its own operation. A request
        // without a correlation gets its own position as one, so the reply
        // names both its cause and a correlation and every reader classifies
        // it as a reply, never as new work.
        provenance.correlation_id = Some(
            self.message
                .provenance
                .correlation_id
                .clone()
                .unwrap_or_else(|| self.message.id.to_string()),
        );
        provenance
    }
}

/// An approver's decision from [`AgentCtx::approval_gate`], minted only by an
/// approving reply. A governed effect carries it with
/// [`AgdxSend::with_grant`](crate::agent::AgdxSend::with_grant), so the
/// governor's step-up in the same conversation is answered by this approval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalGrant {
    approver: laser_wire::agent::AgentId,
    conversation: crate::types::ConversationId,
    correlation: laser_wire::agent::CorrelationId,
    decision: Vec<u8>,
}

impl ApprovalGrant {
    /// The agent that approved.
    pub fn approver(&self) -> &laser_wire::agent::AgentId {
        &self.approver
    }

    /// The conversation the approval was asked in.
    pub fn conversation(&self) -> crate::types::ConversationId {
        self.conversation
    }

    /// The input request the approval answered.
    pub fn correlation(&self) -> laser_wire::agent::CorrelationId {
        self.correlation
    }

    /// The approver's decision body.
    pub fn decision(&self) -> &[u8] {
        &self.decision
    }
}

/// The approval state a reliable consumer shares with the handler of one
/// record: whether the runtime parks, the interrupt the gate raised, and the
/// decision a replay resumes the gate with.
#[doc(hidden)]
#[derive(Clone, Default)]
pub struct ApprovalSlot(std::sync::Arc<std::sync::Mutex<ApprovalState>>);

#[derive(Default)]
struct ApprovalState {
    // The consumer topic a decision must arrive on for the runtime to park.
    parking_topic: Option<String>,
    interrupt: Option<ApprovalInterrupt>,
    resume: Option<ApprovalResume>,
}

/// An approval gate's interrupt: on the pass that published the input
/// request, the prompt's position and deadline.
pub(crate) struct ApprovalInterrupt {
    pub(crate) prompt: Option<(laser_wire::agent::LogPosition, u64)>,
}

/// What a replayed approval gate resumes with.
pub(crate) enum ApprovalResume {
    /// The correlated reply addressed to the agent.
    Decided(Box<AgentMessage>),
    /// The approval deadline passed without a decision.
    Expired,
}

impl ApprovalSlot {
    // A slot whose gate parks when its decision arrives on `topic`, resumed
    // with `resume` on a replay.
    pub(crate) fn parking(topic: &str, resume: Option<ApprovalResume>) -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(ApprovalState {
            parking_topic: Some(topic.to_owned()),
            interrupt: None,
            resume,
        })))
    }

    fn state(&self) -> std::sync::MutexGuard<'_, ApprovalState> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn parks_on(&self, topic: &str) -> bool {
        self.state().parking_topic.as_deref() == Some(topic)
    }

    fn take_resume(&self) -> Option<ApprovalResume> {
        self.state().resume.take()
    }

    fn interrupt(&self, interrupt: ApprovalInterrupt) {
        self.state().interrupt = Some(interrupt);
    }

    pub(crate) fn take_interrupt(&self) -> Option<ApprovalInterrupt> {
        self.state().interrupt.take()
    }
}

/// When a [`fan_out`](AgentCtx::fan_out) or [`scatter`](Laser::scatter) gather is
/// complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatherPolicy {
    /// Wait for every branch (each bounded by the deadline).
    RequireAll,
    /// Stop once this many branches succeed, cancelling the rest. At least one.
    Quorum(usize),
    /// Take whatever has landed by the deadline.
    BestEffort,
}

impl GatherPolicy {
    // A quorum of zero would be met before any branch ran.
    pub(crate) fn validate(self) -> Result<(), LaserError> {
        match self {
            Self::Quorum(0) => Err(LaserError::Invalid(
                "a gather quorum must be at least one".to_owned(),
            )),
            _ => Ok(()),
        }
    }

    // Whether `successes` of `branches` meet the policy: every branch under
    // `RequireAll`, the quorum under `Quorum`, at least one under `BestEffort`.
    pub(crate) fn satisfied(self, successes: usize, branches: usize) -> bool {
        match self {
            Self::RequireAll => successes == branches,
            Self::Quorum(needed) => successes >= needed,
            Self::BestEffort => successes > 0,
        }
    }
}

/// The outcome of a [`fan_out`](AgentCtx::fan_out): the successful replies,
/// the failed branches, and the branches the policy cancelled. Failures are
/// surfaced here, never silently dropped, so a slow or dead agent is visible
/// rather than mistaken for a missing result.
#[derive(Debug, Default)]
pub struct Gather {
    /// The agents that replied, each with its reply. Attributed, so a caller can
    /// tell which agent produced which result rather than a bag of bodies.
    pub ok: Vec<(AgentId, AgentMessage)>,
    /// The agents whose branch failed (no inbox, request error, timeout), each
    /// with its cause. Surfaced, never silently dropped.
    pub failures: Vec<(AgentId, LaserError)>,
    /// The agents whose branch was still running when the policy stopped the
    /// gather (a met quorum, or the best-effort deadline).
    pub cancelled: Vec<AgentId>,
    /// Whether the gather met its [`GatherPolicy`].
    pub satisfied: bool,
}

impl Gather {
    /// The reply bodies alone, dropping agent attribution, for a caller that only
    /// wants the results.
    pub fn replies(&self) -> impl Iterator<Item = &AgentMessage> {
        self.ok.iter().map(|(_, message)| message)
    }
}

// Concurrent gather branches, each attributed to the agent it addresses.
pub(crate) struct Branches<T> {
    set: JoinSet<Result<T, LaserError>>,
    agents: HashMap<task::Id, AgentId>,
}

impl<T> Default for Branches<T> {
    fn default() -> Self {
        Self {
            set: JoinSet::new(),
            agents: HashMap::new(),
        }
    }
}

impl<T: Send + 'static> Branches<T> {
    pub(crate) fn spawn(
        &mut self,
        agent: AgentId,
        branch: impl Future<Output = Result<T, LaserError>> + Send + 'static,
    ) {
        let handle = self.set.spawn(branch);
        self.agents.insert(handle.id(), agent);
    }

    pub(crate) fn len(&self) -> usize {
        self.agents.len()
    }
}

// What a gather collected: every finished branch, the cancelled ones, and how
// many finished branches counted as a success.
pub(crate) struct Gathered<T> {
    pub(crate) outcomes: Vec<(AgentId, Result<T, LaserError>)>,
    pub(crate) cancelled: Vec<AgentId>,
    pub(crate) successes: usize,
}

/// Drain the branches under `policy`. `RequireAll` waits for every branch, each
/// already bounded by its own `deadline`. `Quorum` cancels the remaining
/// branches the moment enough have succeeded. `BestEffort` takes whatever has
/// landed when the wall-clock `deadline` elapses, cancelling the stragglers
/// rather than waiting out their own deadlines. A branch that panics surfaces
/// as a `Handler` failure of its agent, never a lost result.
pub(crate) async fn gather_branches<T: Send + 'static>(
    mut branches: Branches<T>,
    policy: GatherPolicy,
    deadline: Duration,
    succeeded: impl Fn(&T) -> bool,
) -> Gathered<T> {
    let mut gathered = Gathered {
        outcomes: Vec::new(),
        cancelled: Vec::new(),
        successes: 0,
    };
    let wall_clock = tokio::time::sleep(deadline);
    tokio::pin!(wall_clock);
    loop {
        tokio::select! {
            joined = branches.set.join_next_with_id() => {
                let Some(joined) = joined else { break };
                let (id, result) = match joined {
                    Ok((id, result)) => (id, result),
                    Err(join_error) => (
                        join_error.id(),
                        Err(LaserError::Handler(format!("gather branch panicked: {join_error}"))),
                    ),
                };
                let Some(agent) = branches.agents.remove(&id) else {
                    continue;
                };
                if result.as_ref().is_ok_and(&succeeded) {
                    gathered.successes += 1;
                }
                gathered.outcomes.push((agent, result));
                if matches!(policy, GatherPolicy::Quorum(needed) if gathered.successes >= needed) {
                    break;
                }
            }
            _ = &mut wall_clock, if matches!(policy, GatherPolicy::BestEffort) => break,
        }
    }
    branches.set.abort_all();
    gathered.cancelled = branches.agents.into_values().collect();
    gathered
        .cancelled
        .sort_by(|left, right| left.as_str().cmp(right.as_str()));
    gathered
}

// The high 16 bits that mark an approval correlation, so a runtime tells an
// approval decision from any other reply without a lookup. A minted
// correlation carries a timestamp there and never takes this value.
const APPROVAL_CORRELATION_TAG: u128 = 0xa6a6;

// The input request correlation of the approval gated on the record at `at`,
// stable across the record's replays.
pub(crate) fn approval_correlation(
    at: laser_wire::agent::LogPosition,
) -> laser_wire::agent::CorrelationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"laser.approval");
    hasher.update(&at.to_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    let hash = u128::from_be_bytes(bytes) & !(0xffff << 112);
    laser_wire::agent::CorrelationId::from_u128(hash | (APPROVAL_CORRELATION_TAG << 112))
}

// Whether `correlation` names an approval gate's input request.
pub(crate) fn is_approval_correlation(correlation: laser_wire::agent::CorrelationId) -> bool {
    correlation.as_u128() >> 112 == APPROVAL_CORRELATION_TAG
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_handled_record_when_deriving_its_approval_correlation_then_should_be_stable_and_tagged()
     {
        let at = laser_wire::agent::LogPosition::new(1, 2, 3, 40);
        let correlation = approval_correlation(at);
        assert_eq!(correlation, approval_correlation(at));
        assert!(is_approval_correlation(correlation));
        assert_ne!(
            correlation,
            approval_correlation(laser_wire::agent::LogPosition::new(1, 2, 3, 41))
        );
        assert!(!is_approval_correlation(
            <laser_wire::agent::CorrelationId as crate::types::MintUlid>::mint()
        ));
    }

    #[test]
    fn given_a_child_conversation_when_replying_then_should_keep_parent_and_root() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let root = crate::types::ConversationId::new();
        let parent = crate::types::ConversationId::new();
        let child = crate::types::ConversationId::new();
        let provenance = Provenance::builder()
            .conversation_id(child)
            .parent_conversation_id(parent)
            .root_conversation_id(root)
            .build();
        let message = crate::testing::agent_message(b"request".to_vec(), provenance);
        let ctx = AgentCtx::new(
            &laser,
            &message,
            Some("worker".parse().expect("agent id parses")),
            None,
            InboxRoute::default(),
            #[cfg(feature = "sign")]
            None,
        );

        let reply = ctx.reply_provenance();
        assert_eq!(reply.parent_conversation_id, Some(parent));
        assert_eq!(reply.root_conversation_id, Some(root));
    }

    #[test]
    fn given_each_policy_when_checking_satisfaction_then_should_apply_its_own_threshold() {
        assert!(GatherPolicy::RequireAll.satisfied(3, 3));
        assert!(!GatherPolicy::RequireAll.satisfied(2, 3));
        assert!(GatherPolicy::Quorum(2).satisfied(2, 3));
        assert!(!GatherPolicy::Quorum(2).satisfied(1, 3));
        assert!(!GatherPolicy::Quorum(5).satisfied(3, 3));
        assert!(GatherPolicy::BestEffort.satisfied(1, 3));
        assert!(!GatherPolicy::BestEffort.satisfied(0, 3));
    }

    #[test]
    fn given_a_zero_quorum_when_fanning_out_then_should_refuse_before_any_branch() {
        tokio_test_block(async {
            let laser = Laser::from_client(iggy::prelude::IggyClient::default());
            let message = crate::testing::agent_message(
                b"task".to_vec(),
                Provenance::builder()
                    .conversation_id(crate::types::ConversationId::new())
                    .build(),
            );
            let ctx = AgentCtx::new(
                &laser,
                &message,
                Some(agent("orchestrator")),
                Some(AgentTopic::Sessions),
                InboxRoute::default(),
                #[cfg(feature = "sign")]
                None,
            );
            let error = ctx
                .fan_out(
                    CapabilitySelector::new("review", crate::agent::RoutePolicy::Any),
                    b"check".to_vec(),
                    GatherPolicy::Quorum(0),
                    Duration::from_secs(1),
                )
                .await
                .expect_err("a zero quorum is refused");
            assert!(matches!(error, LaserError::Invalid(message) if message.contains("quorum")));
        });
    }

    #[test]
    fn given_a_met_quorum_when_gathering_then_should_cancel_and_report_the_rest() {
        tokio_test_block(async {
            let mut branches = Branches::default();
            branches.spawn(agent("fast"), async { Ok::<_, LaserError>(1) });
            branches.spawn(agent("slow"), async {
                std::future::pending::<()>().await;
                Ok(2)
            });
            let gathered = gather_branches(
                branches,
                GatherPolicy::Quorum(1),
                Duration::from_secs(5),
                |_| true,
            )
            .await;
            assert_eq!(gathered.successes, 1);
            assert_eq!(gathered.outcomes.len(), 1);
            assert_eq!(gathered.outcomes[0].0, agent("fast"));
            assert_eq!(gathered.cancelled, vec![agent("slow")]);
        });
    }

    #[test]
    fn given_a_best_effort_deadline_when_a_branch_outlives_it_then_should_cancel_it() {
        tokio_test_block(async {
            let mut branches = Branches::default();
            branches.spawn(agent("slow"), async {
                std::future::pending::<()>().await;
                Ok::<_, LaserError>(())
            });
            let gathered = gather_branches(
                branches,
                GatherPolicy::BestEffort,
                Duration::from_millis(20),
                |_| true,
            )
            .await;
            assert!(gathered.outcomes.is_empty());
            assert_eq!(gathered.cancelled, vec![agent("slow")]);
            assert!(!GatherPolicy::BestEffort.satisfied(gathered.successes, 1));
        });
    }

    #[test]
    fn given_require_all_when_gathering_then_should_keep_failures_and_attribute_a_panic() {
        tokio_test_block(async {
            let mut branches = Branches::<()>::default();
            branches.spawn(agent("late"), async { Err(LaserError::Timeout("reply")) });
            branches.spawn(agent("broken"), async { panic!("branch bug") });
            let gathered = gather_branches(
                branches,
                GatherPolicy::RequireAll,
                Duration::from_secs(5),
                |_| true,
            )
            .await;
            assert_eq!(gathered.successes, 0);
            assert!(gathered.cancelled.is_empty());
            let mut failed: Vec<(String, bool)> = gathered
                .outcomes
                .iter()
                .map(|(agent, result)| {
                    (
                        agent.to_string(),
                        matches!(result, Err(LaserError::Handler(_))),
                    )
                })
                .collect();
            failed.sort();
            assert_eq!(
                failed,
                vec![("broken".to_owned(), true), ("late".to_owned(), false)]
            );
        });
    }

    // A current-thread runtime with the timer enabled, so the BestEffort
    // wall-clock cut fires against a real (short) deadline.
    fn tokio_test_block<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime builds")
            .block_on(f)
    }

    fn agent(name: &str) -> AgentId {
        name.parse().expect("valid agent id")
    }
}

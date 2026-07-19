use crate::agent::clock::{Clock, SystemClock};
use crate::agent::consumer::AgentMessage;
use crate::agent::router::{CapabilitySelector, InboxRoute, Router};
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
    inbox_route: InboxRoute,
    #[cfg(feature = "sign")]
    signing_key: Option<std::sync::Arc<crate::sign::SigningKey>>,
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
            #[cfg(feature = "sign")]
            signing_key,
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
    ///
    /// An agent built with a signing key answers a correlated command with a
    /// signed AGDX response instead, so a verifying caller (and the contract
    /// path's signer binding) accepts this agent's terminal and no one else's.
    pub async fn respond(&self, payload: impl Into<Vec<u8>>) -> Result<(), LaserError> {
        let topic = self.respond_on.clone().ok_or(LaserError::NoRespondTopic)?;
        let payload = payload.into();
        // Sign only when answering an AGDX command (a directed contract or
        // workflow step): the caller awaits the envelope correlation and, under a
        // verifier, refuses an unsigned terminal. A plain request (fan-out branch,
        // `Laser::request`) carries no envelope and is matched by the string
        // correlation on the reply hub, so it keeps the plain reply.
        #[cfg(feature = "sign")]
        if let Some(key) = &self.signing_key
            && let Some(envelope) = &self.message.envelope
            && let Some(correlation) = envelope.correlation
        {
            let source = self.agent.as_ref().ok_or_else(|| {
                LaserError::HandlerConfig("a signing agent must have an id".to_owned())
            })?;
            let producer = self.laser.agdx(
                topic,
                source.wire_id(),
                self.message.provenance.conversation_id.into(),
            );
            let mut send = producer
                .respond(correlation, payload.to_vec())
                .signed_by(key);
            if let Some(target) = &self.message.provenance.agent {
                send = send.with_target(target.wire_id());
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
        self.laser
            .agdx(reply_topic, source, envelope.conversation)
            .respond(correlation, response.into())
            .send()
            .await?;
        Ok(())
    }

    /// Pause this handler on a human decision: publish `prompt` as an interrupt on
    /// the human-input topic and await the approver's correlated reply on
    /// `reply_topic`, up to `timeout`, chained to the handled conversation. Returns
    /// the decision body on approval, or [`LaserError::Rejected`] when the approver
    /// rejects (the approver answers with [`respond_input`](Self::respond_input)).
    /// A convenience over [`Agdx::request_input`](crate::agent::Agdx::request_input),
    /// so it adds nothing to the wire. Errors if the agent was built without an id.
    pub async fn approval_gate(
        &self,
        reply_topic: AgentTopic<'_>,
        prompt: impl Into<Vec<u8>>,
        timeout: Duration,
    ) -> Result<Vec<u8>, LaserError> {
        let source = self
            .agent
            .as_ref()
            .ok_or_else(|| {
                LaserError::HandlerConfig("approval_gate: the agent has no id".to_owned())
            })?
            .wire_id();
        self.laser
            .agdx(
                AgentTopic::HumanInput,
                source,
                self.message.provenance.conversation_id.into(),
            )
            .request_input(reply_topic, prompt, timeout)
            .await
    }

    /// A child conversation of the handled message, linked by parent/root ids.
    pub fn spawn_subconversation(&self) -> Provenance {
        self.laser.spawn_subconversation(&self.message.provenance)
    }

    /// Fan out to every agent advertising `selector`'s skill, gathering replies
    /// under `policy` within `deadline`. Each branch is a sub-conversation routed
    /// to one agent's own inbox, resolved through the ctx's
    /// [`InboxRoute`](crate::agent::InboxRoute) (the per-agent live-presence inbox
    /// by default, or a fixed topic the caller set at build time), never a shared
    /// topic name baked into the SDK. Replies are collected on the orchestrator's
    /// own response topic (`respond_on`), so a deployment that scopes each user
    /// to its own stream and each workflow to its own topic routes correctly.
    ///
    /// A target that resolves no inbox (advertised none, or [`refresh_presence`](crate::agent::AgentRegistry::refresh_presence)
    /// surfaced none) is a per-branch [`Gather::failures`] entry, never silently
    /// rerouted. Under [`GatherPolicy::Quorum`] the remaining branches are dropped
    /// once the quorum lands rather than waited on (signing a cancel to the losing
    /// branches on the control channel is a follow-on). Errors only on an
    /// infrastructure failure (no capable agent, no response topic, registry read),
    /// not on a branch failure.
    pub async fn fan_out(
        &self,
        selector: CapabilitySelector,
        payload: impl Into<Vec<u8>>,
        policy: GatherPolicy,
        deadline: Duration,
    ) -> Result<Gather, LaserError> {
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

        let mut gather = Gather::default();

        // Resolve each target's inbox while the registry borrow is live, so the
        // owned topic identifier can move into the spawned branch. A target that
        // resolves no inbox fails its branch here rather than mis-routing.
        let mut branches = tokio::task::JoinSet::new();
        for agent in targets {
            let advertised = registry.inbox_for(&agent);
            let inbox = match self.inbox_route.resolve(&agent, advertised) {
                Ok(inbox) => inbox,
                Err(error) => {
                    gather.failures.push((agent, error));
                    continue;
                }
            };
            let laser = self.laser.clone();
            let body = payload.clone();
            let parent = self.message.provenance.clone();
            let reply_topic = reply_topic.clone();
            branches.spawn(async move {
                let mut provenance = laser.spawn_subconversation(&parent);
                provenance.target_agent_id = Some(agent.clone());
                // A distinct correlation per branch so replies never cross.
                provenance.correlation_id = Some(ulid::Ulid::generate().to_string());
                let result = laser
                    .request(
                        AgentTopic::Custom(&inbox),
                        reply_topic,
                        body,
                        &provenance,
                        deadline,
                    )
                    .await;
                (agent, result)
            });
        }

        Ok(gather_branches(branches, gather, policy, deadline).await)
    }

    fn reply_provenance(&self) -> Provenance {
        let mut provenance = Provenance::builder()
            .conversation_id(self.message.provenance.conversation_id)
            .causal_parent(self.message.id)
            .build();
        provenance.agent = self.agent.clone();
        provenance.root_conversation_id = self.message.provenance.root_conversation_id;
        // Echo the request's correlation back so the caller's request/reply
        // correlator can identify this reply unambiguously. Without this a reply
        // with only conversation_id matching would be hijackable when multiple
        // agents share a reply topic. The request's business idempotency_key is
        // deliberately NOT echoed: the reply is its own operation.
        provenance.correlation_id = self.message.provenance.correlation_id.clone();
        provenance
    }
}

/// When a [`fan_out`](AgentCtx::fan_out) gather is complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatherPolicy {
    /// Wait for every branch (each bounded by the deadline).
    RequireAll,
    /// Stop once this many branches succeed, dropping the wait on the rest.
    Quorum(usize),
    /// Take whatever has landed by the deadline.
    BestEffort,
}

/// The outcome of a [`fan_out`](AgentCtx::fan_out): the successful replies and
/// the failed branches. Failures are surfaced here, never silently dropped, so
/// a slow or dead agent is visible rather than mistaken for a missing result.
#[derive(Debug, Default)]
pub struct Gather {
    /// The agents that replied, each with its reply. Attributed, so a caller can
    /// tell which agent produced which result rather than a bag of bodies.
    pub ok: Vec<(AgentId, AgentMessage)>,
    /// The agents whose branch failed (no inbox, request error, timeout), each
    /// with its cause. Surfaced, never silently dropped.
    pub failures: Vec<(AgentId, LaserError)>,
}

impl Gather {
    /// The reply bodies alone, dropping agent attribution, for a caller that only
    /// wants the results.
    pub fn replies(&self) -> impl Iterator<Item = &AgentMessage> {
        self.ok.iter().map(|(_, message)| message)
    }
}

/// Drain every fan-out branch into `gather` under `policy`. `RequireAll` waits
/// for every branch, each already bounded by its own request `deadline`. `Quorum`
/// aborts the remaining branches the moment enough have succeeded. `BestEffort`
/// takes whatever has landed when the wall-clock `deadline` elapses, aborting the
/// stragglers rather than waiting out their per-request deadlines. A branch that
/// panics surfaces as a `Handler` failure, never a lost result.
async fn gather_branches(
    mut branches: tokio::task::JoinSet<(AgentId, Result<AgentMessage, LaserError>)>,
    mut gather: Gather,
    policy: GatherPolicy,
    deadline: Duration,
) -> Gather {
    let wall_clock = tokio::time::sleep(deadline);
    tokio::pin!(wall_clock);
    loop {
        tokio::select! {
            joined = branches.join_next() => {
                let Some(joined) = joined else { break };
                match joined {
                    Ok((agent, Ok(reply))) => gather.ok.push((agent, reply)),
                    Ok((agent, Err(error))) => gather.failures.push((agent, error)),
                    // A JoinError is a panicked branch (a bug), not a normal
                    // outcome: the agent identity is unrecoverable here, so log it
                    // rather than attribute it to the wrong agent.
                    Err(join_error) => {
                        tracing::warn!(%join_error, "fan-out branch panicked");
                    }
                }
                if quorum_satisfied(policy, gather.ok.len()) {
                    branches.abort_all();
                    break;
                }
            }
            _ = &mut wall_clock, if matches!(policy, GatherPolicy::BestEffort) => {
                branches.abort_all();
                break;
            }
        }
    }
    gather
}

/// Whether `policy` is satisfied by `successes` replies so far, so the remaining
/// branches can be cancelled. Only `Quorum` short-circuits: `RequireAll` and
/// `BestEffort` drain every branch (the latter bounded by the wall-clock
/// deadline instead).
fn quorum_satisfied(policy: GatherPolicy, successes: usize) -> bool {
    matches!(policy, GatherPolicy::Quorum(needed) if successes >= needed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_require_all_when_checking_quorum_then_should_never_short_circuit() {
        // RequireAll drains every branch, so no count ever satisfies it early.
        assert!(!quorum_satisfied(GatherPolicy::RequireAll, 0));
        assert!(!quorum_satisfied(GatherPolicy::RequireAll, 1));
        assert!(!quorum_satisfied(GatherPolicy::RequireAll, 1000));
    }

    #[test]
    fn given_best_effort_when_checking_quorum_then_should_never_short_circuit() {
        // BestEffort is cut by the wall-clock deadline, not by a success count.
        assert!(!quorum_satisfied(GatherPolicy::BestEffort, 0));
        assert!(!quorum_satisfied(GatherPolicy::BestEffort, 1));
        assert!(!quorum_satisfied(GatherPolicy::BestEffort, 1000));
    }

    #[test]
    fn given_a_quorum_when_checking_then_should_short_circuit_at_or_above_the_threshold() {
        // Below the threshold keeps waiting, at or above it short-circuits.
        assert!(!quorum_satisfied(GatherPolicy::Quorum(2), 0));
        assert!(!quorum_satisfied(GatherPolicy::Quorum(2), 1));
        assert!(quorum_satisfied(GatherPolicy::Quorum(2), 2));
        assert!(quorum_satisfied(GatherPolicy::Quorum(2), 3));
    }

    #[test]
    fn given_a_zero_quorum_when_checking_then_should_be_satisfied_immediately() {
        // A degenerate Quorum(0) is satisfied before any branch replies, so the
        // fan-out aborts every branch and gathers nothing. The edge is defined,
        // not a panic.
        assert!(quorum_satisfied(GatherPolicy::Quorum(0), 0));
    }

    #[test]
    fn given_a_quorum_larger_than_the_target_set_when_never_reached_then_should_not_short_circuit()
    {
        // Quorum(5) against three responders never trips, so the loop drains all
        // three and the caller sees fewer successes than asked, surfaced as a
        // short gather rather than a hang.
        assert!(!quorum_satisfied(GatherPolicy::Quorum(5), 3));
    }

    #[test]
    fn given_branches_when_gathered_under_require_all_then_should_classify_replies_and_failures() {
        // Drive the real drain over a set of finished branches: two replies, one
        // failed request. RequireAll keeps both buckets, nothing is dropped.
        tokio_test_block(async {
            let mut branches = tokio::task::JoinSet::new();
            let agent: AgentId = "worker".parse().expect("valid agent id");
            branches.spawn(async move { (agent, Err(LaserError::Timeout("reply"))) });
            let gather = gather_branches(
                branches,
                Gather::default(),
                GatherPolicy::RequireAll,
                Duration::from_secs(1),
            )
            .await;
            assert_eq!(gather.ok.len(), 0);
            assert_eq!(gather.failures.len(), 1);
            assert!(matches!(gather.failures[0].1, LaserError::Timeout(_)));
        });
    }

    #[test]
    fn given_a_best_effort_deadline_when_a_branch_outlives_it_then_should_cut_and_return_what_landed()
     {
        // One branch never completes within the wall-clock deadline. BestEffort
        // aborts it at the deadline and returns the empty (but not hung) gather.
        tokio_test_block(async {
            let mut branches = tokio::task::JoinSet::new();
            let agent: AgentId = "worker".parse().expect("valid agent id");
            branches.spawn(async move {
                futures_pending().await;
                (agent, Ok::<_, LaserError>(unreachable_reply()))
            });
            let gather = gather_branches(
                branches,
                Gather::default(),
                GatherPolicy::BestEffort,
                Duration::from_millis(20),
            )
            .await;
            assert_eq!(gather.ok.len(), 0);
            assert_eq!(
                gather.failures.len(),
                0,
                "an aborted straggler is dropped, not a failure"
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

    async fn futures_pending() {
        std::future::pending::<()>().await
    }

    fn unreachable_reply() -> AgentMessage {
        unreachable!("the pending branch is aborted before it ever yields a reply")
    }
}

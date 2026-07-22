use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::{ConversationId, MintUlid};
use async_trait::async_trait;
use laser_wire::agent::{AgentEnvelope, AgentId, RecordId, validate};
use laser_wire::content::ContentType;
use laser_wire::framing::{decode_named, encode_named};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// The `operation` stamped on every policy-evidence event, so the audit topic
/// filters governance decisions without decoding bodies.
pub const POLICY_DECISION_OPERATION: &str = "policy_decision";

/// The pre-effect policy hook: [`decide`](Self::decide) runs before the SDK
/// publishes an agent send, a typed or raw topic publish, an AGDX verb, or a
/// memory write, and its
/// [`ActionDecision`] is applied under the configured [`GovernorMode`]. Enroll
/// one with [`Laser::with_governor`], `LaserBuilder::governor`, or
/// `Agent::builder().governor(..)`. Defense in depth at the effect boundary:
/// RBAC on the managed surfaces stays server-owned, this hook cannot widen it.
///
/// An `Err` fails the governed action (fail closed), so a broken governor
/// never fails open.
#[async_trait]
pub trait ActionGovernor: Send + Sync {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError>;
}

/// One side effect about to run, as the [`ActionGovernor`] sees it. Every
/// agent-written field here is a claim (the AGDX trusted-versus-advisory rule):
/// `purpose` and `data_classification` are advisory unless the envelope is
/// signed, and `signed` says only that this SDK will sign the record, not that
/// a peer verified it.
#[derive(Debug)]
pub struct GovernedAction<'a> {
    /// What kind of effect this is.
    pub kind: ActionKind,
    /// The Iggy stream the effect publishes to.
    pub stream: &'a str,
    /// The topic the effect publishes to.
    pub topic: &'a str,
    /// The acting agent, when the effect carries one.
    pub source: Option<&'a str>,
    /// The addressed agent, when the effect targets one.
    pub target: Option<&'a str>,
    /// The conversation the effect belongs to.
    pub conversation: Option<ConversationId>,
    /// The reply-correlation key, when the effect carries one.
    pub correlation: Option<&'a str>,
    /// The envelope operation name (AGDX path).
    pub operation: Option<&'a str>,
    /// The tool name (AGDX path).
    pub tool: Option<&'a str>,
    /// The delegation subject from the envelope metadata (`on_behalf_of`).
    pub on_behalf_of: Option<&'a str>,
    /// The declared purpose from the envelope metadata (advisory).
    pub purpose: Option<&'a str>,
    /// The declared data classification from the envelope metadata (advisory).
    pub data_classification: Option<&'a str>,
    /// The body about to be published.
    pub payload: &'a [u8],
    /// Whether this SDK will sign the record at send.
    pub signed: bool,
    /// Session counters at decision time, for rate and budget policies.
    pub counters: ActionCounters,
}

/// The kind of side effect a [`GovernedAction`] describes. Displays as its
/// snake_case evidence name (`send` | `publish` | `request` | `command` |
/// `response` | `event` | `status` | `error` | `memory_write`) and parses back
/// from it.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, strum::Display, strum::EnumString, strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum ActionKind {
    /// A plain agent-topic send (`send_agent`, `ctx.send`, `respond`).
    Send,
    /// A typed or raw publish to a data topic (`topic(...).publish()`).
    Publish,
    /// A request awaiting a correlated reply (`request`, `fan_out` branches).
    Request,
    /// An AGDX `command`.
    Command,
    /// An AGDX `response`.
    Response,
    /// An AGDX `event`.
    Event,
    /// An AGDX `status`.
    Status,
    /// An AGDX `error` terminal.
    Error,
    /// A memory write (`remember`, `improve`, `forget`).
    MemoryWrite,
}

impl ActionKind {
    /// The pinned evidence name of this kind.
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

/// Session counters at decision time. Shared by every clone of the governed
/// [`Laser`], so a policy can bound a whole session, not one handle.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActionCounters {
    /// Governed non-request effects so far.
    pub sends: u64,
    /// Governed requests so far.
    pub requests: u64,
    /// Payload bytes published through governed effects so far.
    pub bytes_sent: u64,
}

/// What the [`ActionGovernor`] decided, with the optional evidence detail
/// (reason, policy provenance, risk score) recorded in the [`PolicyEvidence`]
/// event. Build with the constructors and refine with the `with_*` methods.
#[derive(Debug, Clone, PartialEq)]
pub struct ActionDecision {
    /// The verdict to apply.
    pub verdict: Verdict,
    /// Why, recorded in evidence.
    pub reason: Option<String>,
    /// The policy pack and rules that decided, recorded in evidence.
    pub policy: Option<PolicyRef>,
    /// The governor's risk estimate, recorded in evidence.
    pub risk_score: Option<f64>,
}

impl ActionDecision {
    /// Run the effect, no evidence.
    pub fn allow() -> Self {
        Self::of(Verdict::Allow)
    }

    /// Run the effect and record evidence.
    pub fn observe() -> Self {
        Self::of(Verdict::Observe)
    }

    /// Reject before the effect ([`LaserError::PolicyBlocked`]).
    pub fn block(reason: impl Into<String>) -> Self {
        let mut decision = Self::of(Verdict::Block);
        decision.reason = Some(reason.into());
        decision
    }

    /// Reject with the scope an approval must grant
    /// ([`LaserError::StepUpRequired`]). The handler catches it, runs an
    /// [`approval_gate`](crate::agent::AgentCtx::approval_gate), and re-sends.
    pub fn step_up(scope: impl Into<String>) -> Self {
        Self::of(Verdict::StepUp {
            scope: scope.into(),
        })
    }

    /// Replace the body before the effect. Applied before claim-check and
    /// signing, so a signature always covers the body the log carries.
    pub fn modify(body: impl Into<Vec<u8>>) -> Self {
        Self::of(Verdict::Modify { body: body.into() })
    }

    /// Hold the work for later ([`LaserError::PolicyDeferred`], retryable).
    pub fn defer(reason: impl Into<String>) -> Self {
        let mut decision = Self::of(Verdict::Defer);
        decision.reason = Some(reason.into());
        decision
    }

    /// Record why, in evidence.
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Record the deciding policy pack and rules, in evidence.
    #[must_use]
    pub fn with_policy(mut self, policy: PolicyRef) -> Self {
        self.policy = Some(policy);
        self
    }

    /// Record the governor's risk estimate, in evidence.
    #[must_use]
    pub fn with_risk_score(mut self, risk_score: f64) -> Self {
        self.risk_score = Some(risk_score);
        self
    }

    fn of(verdict: Verdict) -> Self {
        Self {
            verdict,
            reason: None,
            policy: None,
            risk_score: None,
        }
    }
}

/// The decision vocabulary, broader than allow and deny. Displays as its
/// snake_case evidence name (`allow` | `observe` | `block` | `step_up` |
/// `modify` | `defer`).
#[derive(Debug, Clone, PartialEq, strum::Display, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum Verdict {
    /// Run the effect.
    Allow,
    /// Run the effect and emit evidence.
    Observe,
    /// Reject before the effect.
    Block,
    /// Pause on an approval granting `scope`.
    StepUp { scope: String },
    /// Replace the body, then run the effect.
    Modify { body: Vec<u8> },
    /// Record that the work is held for later.
    Defer,
}

impl Verdict {
    /// The pinned evidence name of this verdict.
    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

/// The versioned policy artifact a decision came from, recorded verbatim in
/// evidence. The SDK parses no policy language: a governor maps whatever
/// engine it fronts onto this.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyRef {
    /// The policy pack id.
    pub pack_id: String,
    /// The policy pack version.
    pub pack_version: String,
    /// The rule ids that matched.
    pub rule_ids: Vec<String>,
}

/// How a decision is applied. Configuration, never an envelope claim: an agent
/// must not self-authorize weaker enforcement. Displays as its snake_case
/// evidence name (`observe` | `enforce`) and parses back from it.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
pub enum GovernorMode {
    /// Shadow rollout: every decision is recorded with what enforcement would
    /// have done, the effect always runs unmodified, and an evidence-write
    /// failure only warns. Observing never impacts production.
    Observe,
    /// Apply the verdict: block, step up, modify, or defer before the effect.
    /// An evidence-write failure on a proceeding decision fails the call, so a
    /// governed effect is never unrecorded.
    #[default]
    Enforce,
}

impl GovernorMode {
    /// The pinned evidence name of this mode.
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

/// One governance decision, appended as an AGDX `event` (operation
/// [`POLICY_DECISION_OPERATION`], content-type cbor) on the audit topic.
/// Append-only evidence, never a grant. `receipt_digest` is the BLAKE3 hash of
/// this record's canonical encoding with the digest field empty, and
/// `previous_digest` chains it to the prior decision in the same conversation,
/// so reordering or dropping local evidence is detectable. Signed evidence
/// (a governed `Laser` whose sends are signed) additionally proves the
/// producer, and the log position proves order on the substrate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyEvidence {
    /// This decision's id (ULID).
    pub decision_id: String,
    /// The verdict name (`allow` | `observe` | `block` | `step_up` | `modify` | `defer`).
    pub decision: String,
    /// The enforcement mode the decision ran under.
    pub mode: String,
    /// The governed action's kind.
    pub kind: String,
    /// The stream the action targeted.
    pub stream: String,
    /// The topic the action targeted.
    pub topic: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub target: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub conversation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub correlation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub on_behalf_of: Option<String>,
    /// The governor's reason, when it gave one.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reason: Option<String>,
    /// The scope a step-up approval must grant.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub approved_scope: Option<String>,
    /// The deciding policy pack and rules.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub policy: Option<PolicyRef>,
    /// The governor's risk estimate.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub risk_score: Option<f64>,
    /// BLAKE3 (hex) of this record's canonical encoding, digest field empty.
    pub receipt_digest: String,
    /// The prior decision's `receipt_digest` in this conversation.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub previous_digest: Option<String>,
    /// What the decision did to the effect: `effected` (the decision let it
    /// proceed, the publish itself can still fail downstream) | `blocked` |
    /// `step_up` | `deferred`.
    pub outcome: String,
    /// Decision time, epoch micros.
    pub at_micros: u64,
}

impl PolicyEvidence {
    /// The canonical named-field CBOR encoding this evidence rides the log as.
    pub fn encode(&self) -> Result<Vec<u8>, LaserError> {
        Ok(encode_named(self)?)
    }

    /// Decode an evidence body read back off the audit topic.
    pub fn decode(payload: &[u8]) -> Result<Self, LaserError> {
        Ok(decode_named(payload)?)
    }

    /// Seal this record: compute [`receipt_digest`](Self::receipt_digest) over
    /// the canonical encoding with the digest field empty.
    fn seal(mut self) -> Result<Self, LaserError> {
        self.receipt_digest = String::new();
        let canonical = self.encode()?;
        self.receipt_digest = blake3::hash(&canonical).to_string();
        Ok(self)
    }
}

impl Laser {
    /// A clone of this `Laser` whose agent sends, typed or raw topic
    /// publishes, AGDX verbs, and memory writes run `governor` before the
    /// effect, applied under `mode`. The connection, producer cache, and
    /// everything else are shared with the original, while the governor's
    /// session counters and evidence chain are fresh and shared by every clone
    /// of the returned handle. Agents spawned from the governed handle inherit
    /// it, so a handler's `ctx` effects are governed too.
    #[must_use]
    pub fn with_governor(&self, governor: Arc<dyn ActionGovernor>, mode: GovernorMode) -> Self {
        let mut governed = self.clone();
        governed.governor = Some(Arc::new(GovernorState::new(governor, mode)));
        governed
    }

    // Run the governor over `action` when one is enrolled: decide, record
    // evidence, and apply the verdict under the mode. `Ok(None)` proceeds with
    // the original body, `Ok(Some(body))` proceeds with the replacement, `Err`
    // is the applied denial (or the fail-closed governor/evidence failure).
    pub(crate) async fn govern(
        &self,
        mut action: GovernedAction<'_>,
    ) -> Result<Option<Vec<u8>>, LaserError> {
        let Some(state) = self.governor.clone() else {
            return Ok(None);
        };
        action.counters = state.snapshot();
        let decision = state.governor.decide(&action).await?;
        match action.kind {
            ActionKind::Request => state.requests.fetch_add(1, Ordering::Relaxed),
            _ => state.sends.fetch_add(1, Ordering::Relaxed),
        };
        let applied = apply(state.mode, decision.verdict.clone());
        if applied.recorded {
            let emitted = state
                .emit_chained_evidence(self, &action, &decision, applied.outcome)
                .await;
            match state.mode {
                // Shadow never impacts production: a failed evidence write warns.
                GovernorMode::Observe => {
                    if let Err(error) = emitted {
                        tracing::warn!(%error, "policy evidence write failed (observe mode)");
                    }
                }
                GovernorMode::Enforce => match (&applied.denial, emitted) {
                    // A proceeding decision with no recorded evidence must not
                    // proceed: fail closed rather than run an unrecorded effect.
                    (None, Err(error)) => return Err(error),
                    // The denial stands whether or not its evidence landed.
                    (Some(_), Err(error)) => {
                        tracing::warn!(%error, "policy evidence write failed (denial stands)");
                    }
                    (_, Ok(())) => {}
                },
            }
        }
        if let Some(denial) = applied.denial {
            return Err(denial);
        }
        // Count what actually goes out: the replacement body when modified.
        let sent = applied.body.as_ref().map_or(action.payload.len(), Vec::len);
        state.bytes_sent.fetch_add(sent as u64, Ordering::Relaxed);
        Ok(applied.body)
    }

    // Publish `evidence` as an AGDX `event` on the audit topic of the action's
    // stream, keyed by the conversation. Goes straight to the batch path, never
    // back through the governed verbs, so evidence cannot recurse into
    // governance.
    async fn emit_evidence(
        &self,
        action: &GovernedAction<'_>,
        evidence: PolicyEvidence,
    ) -> Result<(), LaserError> {
        let source: AgentId = action
            .source
            .and_then(|source| source.parse().ok())
            .unwrap_or_else(|| "governor".parse().expect("static agent id is valid"));
        let conversation = action.conversation.unwrap_or_default();
        let envelope = AgentEnvelope::event(
            RecordId::mint(),
            conversation.into(),
            source,
            evidence.encode()?,
        )
        .with_operation(POLICY_DECISION_OPERATION);
        validate(&envelope)?;
        let payload = encode_named(&envelope)?;
        let headers = crate::agent::agdx_headers(&envelope, ContentType::Cbor)?;
        let message = iggy::prelude::IggyMessage::builder()
            .payload(bytes::Bytes::from(payload))
            .user_headers(headers)
            .build()?;
        let partition_key = conversation.to_string();
        self.send_batch_on(
            action.stream,
            &AgentTopic::Audit.topic_string(),
            vec![message],
            Some(&partition_key),
        )
        .await
    }
}

// The shared state of one governed `Laser` handle: the hook, the mode, the
// session counters, and the per-conversation evidence digest chain.
pub(crate) struct GovernorState {
    governor: Arc<dyn ActionGovernor>,
    mode: GovernorMode,
    sends: AtomicU64,
    requests: AtomicU64,
    bytes_sent: AtomicU64,
    chain: dashmap::DashMap<Option<u128>, Arc<tokio::sync::Mutex<Option<String>>>>,
}

impl GovernorState {
    pub(crate) fn new(governor: Arc<dyn ActionGovernor>, mode: GovernorMode) -> Self {
        Self {
            governor,
            mode,
            sends: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            chain: dashmap::DashMap::new(),
        }
    }

    fn snapshot(&self) -> ActionCounters {
        ActionCounters {
            sends: self.sends.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
        }
    }

    // Serialize audit publication per conversation. Advance the chain only
    // after the record lands, so a failed write cannot become a predecessor.
    async fn emit_chained_evidence(
        &self,
        laser: &Laser,
        action: &GovernedAction<'_>,
        decision: &ActionDecision,
        outcome: &'static str,
    ) -> Result<(), LaserError> {
        let chain_key = action.conversation.map(|id| id.as_u128());
        let slot = Arc::clone(
            self.chain
                .entry(chain_key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None)))
                .value(),
        );
        let mut previous = slot.lock().await;
        let evidence = self.seal_evidence(action, decision, outcome, previous.clone())?;
        let digest = evidence.receipt_digest.clone();
        laser.emit_evidence(action, evidence).await?;
        *previous = Some(digest);
        Ok(())
    }

    // Build and seal one evidence record against the supplied predecessor.
    fn seal_evidence(
        &self,
        action: &GovernedAction<'_>,
        decision: &ActionDecision,
        outcome: &'static str,
        previous_digest: Option<String>,
    ) -> Result<PolicyEvidence, LaserError> {
        let approved_scope = match &decision.verdict {
            Verdict::StepUp { scope } => Some(scope.clone()),
            _ => None,
        };
        PolicyEvidence {
            decision_id: ulid::Ulid::generate().to_string(),
            decision: decision.verdict.as_str().to_owned(),
            mode: self.mode.as_str().to_owned(),
            kind: action.kind.as_str().to_owned(),
            stream: action.stream.to_owned(),
            topic: action.topic.to_owned(),
            source: action.source.map(str::to_owned),
            target: action.target.map(str::to_owned),
            conversation: action.conversation.map(|id| id.to_string()),
            correlation: action.correlation.map(str::to_owned),
            operation: action.operation.map(str::to_owned),
            tool: action.tool.map(str::to_owned),
            on_behalf_of: action.on_behalf_of.map(str::to_owned),
            reason: decision.reason.clone(),
            approved_scope,
            policy: decision.policy.clone(),
            risk_score: decision.risk_score,
            receipt_digest: String::new(),
            previous_digest,
            outcome: outcome.to_owned(),
            at_micros: now_micros(),
        }
        .seal()
    }
}

// One verdict applied under one mode: whether evidence is recorded, whether
// and with what body the effect proceeds, and the denial when it does not.
struct AppliedVerdict {
    recorded: bool,
    outcome: &'static str,
    body: Option<Vec<u8>>,
    denial: Option<LaserError>,
}

// The verdict-by-mode table. Observe mode records what enforcement would have
// done and always proceeds unmodified.
fn apply(mode: GovernorMode, verdict: Verdict) -> AppliedVerdict {
    let enforced = matches!(mode, GovernorMode::Enforce);
    match verdict {
        Verdict::Allow => AppliedVerdict {
            recorded: false,
            outcome: "effected",
            body: None,
            denial: None,
        },
        Verdict::Observe => AppliedVerdict {
            recorded: true,
            outcome: "effected",
            body: None,
            denial: None,
        },
        Verdict::Modify { body } => AppliedVerdict {
            recorded: true,
            outcome: "effected",
            body: enforced.then_some(body),
            denial: None,
        },
        Verdict::Block => AppliedVerdict {
            recorded: true,
            outcome: if enforced { "blocked" } else { "effected" },
            body: None,
            denial: enforced
                .then(|| LaserError::PolicyBlocked("the governor blocked this action".to_owned())),
        },
        Verdict::StepUp { scope } => AppliedVerdict {
            recorded: true,
            outcome: if enforced { "step_up" } else { "effected" },
            body: None,
            denial: enforced.then(|| LaserError::StepUpRequired(scope)),
        },
        Verdict::Defer => AppliedVerdict {
            recorded: true,
            outcome: if enforced { "deferred" } else { "effected" },
            body: None,
            denial: enforced.then(|| {
                LaserError::PolicyDeferred("the governor deferred this action".to_owned())
            }),
        },
    }
}

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as u64)
        .unwrap_or(0)
}

/// How a [`QuorumGovernor`] combines its voters' verdicts into one decision.
/// Only [`Verdict::Allow`], [`Verdict::Observe`], and [`Verdict::Modify`] count
/// as affirmative (the action would proceed under that voter alone).
/// `Block`, `StepUp`, and `Defer` do not, regardless of quorum policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuorumPolicy {
    /// Every voter must be affirmative.
    All,
    /// At least one voter must be affirmative.
    Any,
    /// At least `n` distinct voters must be affirmative.
    AtLeast(usize),
}

impl QuorumPolicy {
    fn met(self, affirmative: usize, total: usize) -> bool {
        match self {
            QuorumPolicy::All => affirmative == total,
            QuorumPolicy::Any => affirmative >= 1,
            QuorumPolicy::AtLeast(n) => affirmative >= n,
        }
    }
}

// One named voter in a QuorumGovernor. Mandatory voters are required
// affirmatives, so an unavailable safety voter cannot be bypassed by `Any`.
#[derive(Clone)]
struct Voter {
    name: String,
    governor: Arc<dyn ActionGovernor>,
    mandatory: bool,
}

/// A governor that composes independent voters under a [`QuorumPolicy`],
/// itself an [`ActionGovernor`] so it enrolls with [`Laser::with_governor`]
/// exactly like a single governor. Every voter runs concurrently over the same
/// [`GovernedAction`]. Nothing here talks to a durable log: this is an
/// in-process combinator, not the durable propose/vote/commit lifecycle a
/// crash-safe, forgery-resistant version of this would need.
///
/// Affirmative verdicts ([`Verdict::Allow`], [`Verdict::Observe`],
/// [`Verdict::Modify`]) count toward the quorum. [`Verdict::Block`],
/// [`Verdict::StepUp`], and [`Verdict::Defer`] do not. Every mandatory voter
/// must be affirmative before the quorum can pass. When the quorum is met, the
/// composite verdict is the strongest affirmative found, in
/// order `Modify` (a body replacement must be preserved) then `Observe` (any
/// voter wanting evidence recorded wins) then `Allow`. When the quorum is not
/// met, the composite is the most actionable denial found, in order `Block`
/// then `StepUp` then `Defer`. Every voter's own verdict is recorded in the
/// composite decision's `reason`, so the audit trail names who said what.
#[derive(Clone)]
pub struct QuorumGovernor {
    voters: Vec<Voter>,
    policy: QuorumPolicy,
}

impl QuorumGovernor {
    /// A quorum under `policy`. Add voters with [`voter`](Self::voter). An
    /// empty or otherwise invalid configuration blocks when evaluated.
    pub fn new(policy: QuorumPolicy) -> Self {
        Self {
            voters: Vec::new(),
            policy,
        }
    }

    /// Enroll one named voter. A `mandatory` voter must return an affirmative
    /// verdict before the action can proceed, regardless of `policy`.
    #[must_use]
    pub fn voter(
        mut self,
        name: impl Into<String>,
        governor: Arc<dyn ActionGovernor>,
        mandatory: bool,
    ) -> Self {
        self.voters.push(Voter {
            name: name.into(),
            governor,
            mandatory,
        });
        self
    }
}

#[async_trait]
impl ActionGovernor for QuorumGovernor {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        if self.voters.is_empty() {
            return Ok(ActionDecision::block(
                "quorum governor has no configured voters",
            ));
        }
        if let QuorumPolicy::AtLeast(required) = self.policy
            && (required == 0 || required > self.voters.len())
        {
            return Ok(ActionDecision::block(format!(
                "quorum threshold {required} is invalid for {} voters",
                self.voters.len()
            )));
        }
        let mut names = std::collections::HashSet::with_capacity(self.voters.len());
        if let Some(duplicate) = self
            .voters
            .iter()
            .find(|voter| !names.insert(voter.name.as_str()))
        {
            return Ok(ActionDecision::block(format!(
                "quorum voter '{}' is configured more than once",
                duplicate.name
            )));
        }
        let votes = futures::future::join_all(
            self.voters
                .iter()
                .map(|voter| async move { (voter, voter.governor.decide(action).await) }),
        )
        .await;

        let mut ballot = Vec::with_capacity(votes.len());
        let mut affirmative = 0usize;
        let mut mandatory_denial: Option<ActionDecision> = None;
        let mut mandatory_error: Option<&str> = None;
        let mut best_affirmative: Option<ActionDecision> = None;
        let mut best_denial: Option<ActionDecision> = None;
        let mut replacement: Option<&[u8]> = None;
        let mut conflicting_replacements = false;
        for (voter, vote) in &votes {
            let decision = vote.as_ref().map_err(|error| error.to_string());
            ballot.push(format!(
                "{}={}",
                voter.name,
                decision
                    .as_ref()
                    .map(|decision| decision.verdict.as_str().to_owned())
                    .unwrap_or_else(|error| format!("error({error})"))
            ));
            let Ok(decision) = vote else {
                if voter.mandatory {
                    mandatory_error = mandatory_error.or(Some(voter.name.as_str()));
                }
                continue;
            };
            if voter.mandatory && !is_affirmative(&decision.verdict) {
                mandatory_denial = mandatory_denial.or(Some(decision.clone()));
            }
            if is_affirmative(&decision.verdict) {
                affirmative += 1;
                if let Verdict::Modify { body } = &decision.verdict {
                    match replacement {
                        None => replacement = Some(body),
                        Some(existing) if existing != body => conflicting_replacements = true,
                        Some(_) => {}
                    }
                }
                if affirmative_rank(&decision.verdict)
                    > best_affirmative
                        .as_ref()
                        .map_or(-1, |current| affirmative_rank(&current.verdict))
                {
                    best_affirmative = Some(decision.clone());
                }
            } else if denial_rank(&decision.verdict)
                > best_denial
                    .as_ref()
                    .map_or(-1, |current| denial_rank(&current.verdict))
            {
                best_denial = Some(decision.clone());
            }
        }

        // A non-mandatory voter error is an abstention. Mandatory errors block.
        let reason = format!("quorum({:?}): {}", self.policy, ballot.join(", "));

        if let Some(voter) = mandatory_error {
            return Ok(annotate(
                ActionDecision::block(format!("mandatory voter '{voter}' failed")),
                &reason,
            ));
        }
        if let Some(decision) = mandatory_denial {
            return Ok(annotate(decision, &reason));
        }
        if conflicting_replacements {
            return Ok(annotate(
                ActionDecision::block("quorum voters proposed conflicting body replacements"),
                &reason,
            ));
        }
        if self.policy.met(affirmative, self.voters.len()) {
            return Ok(annotate(
                best_affirmative.unwrap_or_else(ActionDecision::allow),
                &reason,
            ));
        }
        Ok(annotate(
            best_denial
                .unwrap_or_else(|| ActionDecision::block("no voter reached the required quorum")),
            &reason,
        ))
    }
}

fn annotate(mut decision: ActionDecision, ballot: &str) -> ActionDecision {
    decision.reason = Some(match decision.reason.take() {
        Some(detail) => format!("{detail}; {ballot}"),
        None => ballot.to_owned(),
    });
    decision
}

fn is_affirmative(verdict: &Verdict) -> bool {
    matches!(
        verdict,
        Verdict::Allow | Verdict::Observe | Verdict::Modify { .. }
    )
}

// Modify > Observe > Allow: a body replacement must survive, evidence-wanting
// is the next strongest signal, plain allow is the default.
fn affirmative_rank(verdict: &Verdict) -> i8 {
    match verdict {
        Verdict::Modify { .. } => 2,
        Verdict::Observe => 1,
        Verdict::Allow => 0,
        _ => -1,
    }
}

// Block > StepUp > Defer: an outright rejection is the most actionable denial
// to surface, a request for human approval is milder, holding for later is
// mildest.
fn denial_rank(verdict: &Verdict) -> i8 {
    match verdict {
        Verdict::Block => 2,
        Verdict::StepUp { .. } => 1,
        Verdict::Defer => 0,
        _ => -1,
    }
}

/// A governor whose active policy can be hot-swapped at runtime without
/// dropping clones already enrolled via [`Laser::with_governor`] or
/// restarting the process. [`swap`](Self::swap) can be driven by anything: an
/// operator call, a config reload, or a caller folding a policy-update topic
/// and swapping in the governor that matches the latest fact. A swap only
/// changes which policy the *next* [`decide`](Self::decide) call runs under:
/// it never reinterprets a [`PolicyEvidence`] record already on the log, the
/// same non-retroactive rule [`crate::intent::Intent::policy_version`] gives
/// durable intents.
pub struct SwappableGovernor {
    active: std::sync::RwLock<Arc<dyn ActionGovernor>>,
}

impl SwappableGovernor {
    /// A swappable governor starting from `initial`.
    pub fn new(initial: Arc<dyn ActionGovernor>) -> Self {
        Self {
            active: std::sync::RwLock::new(initial),
        }
    }

    /// Replace the active policy with `next`, returning the one just
    /// replaced. A `decide` already in flight finishes under whichever
    /// policy it read. This never cancels or reruns in-flight work.
    pub fn swap(&self, next: Arc<dyn ActionGovernor>) -> Arc<dyn ActionGovernor> {
        let mut active = self
            .active
            .write()
            .expect("governor lock is never poisoned");
        std::mem::replace(&mut active, next)
    }

    /// The currently active policy.
    pub fn current(&self) -> Arc<dyn ActionGovernor> {
        Arc::clone(&self.active.read().expect("governor lock is never poisoned"))
    }
}

#[async_trait]
impl ActionGovernor for SwappableGovernor {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        self.current().decide(action).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_block_decision_when_built_then_should_carry_the_reason_and_refinements() {
        let decision = ActionDecision::block("wire transfers need approval")
            .with_policy(PolicyRef {
                pack_id: "finance".to_owned(),
                pack_version: "3".to_owned(),
                rule_ids: vec!["no-wires".to_owned()],
            })
            .with_risk_score(0.9);
        assert_eq!(decision.verdict, Verdict::Block);
        assert_eq!(
            decision.reason.as_deref(),
            Some("wire transfers need approval")
        );
        assert_eq!(
            decision.policy.as_ref().map(|p| p.pack_id.as_str()),
            Some("finance")
        );
        assert_eq!(decision.risk_score, Some(0.9));
    }

    #[test]
    fn given_enforce_mode_when_verdicts_applied_then_should_map_the_full_table() {
        let allow = apply(GovernorMode::Enforce, Verdict::Allow);
        assert!(!allow.recorded && allow.denial.is_none() && allow.body.is_none());

        let observe = apply(GovernorMode::Enforce, Verdict::Observe);
        assert!(observe.recorded && observe.denial.is_none());
        assert_eq!(observe.outcome, "effected");

        let modify = apply(
            GovernorMode::Enforce,
            Verdict::Modify {
                body: b"x".to_vec(),
            },
        );
        assert_eq!(modify.body.as_deref(), Some(b"x".as_slice()));

        let block = apply(GovernorMode::Enforce, Verdict::Block);
        assert_eq!(block.outcome, "blocked");
        assert!(matches!(block.denial, Some(LaserError::PolicyBlocked(_))));

        let step_up = apply(
            GovernorMode::Enforce,
            Verdict::StepUp {
                scope: "payments:approve".to_owned(),
            },
        );
        assert_eq!(step_up.outcome, "step_up");
        assert!(
            matches!(step_up.denial, Some(LaserError::StepUpRequired(scope)) if scope == "payments:approve")
        );

        let defer = apply(GovernorMode::Enforce, Verdict::Defer);
        assert_eq!(defer.outcome, "deferred");
        assert!(matches!(defer.denial, Some(LaserError::PolicyDeferred(_))));
    }

    #[test]
    fn given_observe_mode_when_verdicts_applied_then_should_record_and_always_proceed() {
        for verdict in [
            Verdict::Observe,
            Verdict::Block,
            Verdict::StepUp {
                scope: "s".to_owned(),
            },
            Verdict::Modify {
                body: b"x".to_vec(),
            },
            Verdict::Defer,
        ] {
            let applied = apply(GovernorMode::Observe, verdict);
            assert!(applied.recorded);
            assert!(applied.denial.is_none());
            assert!(applied.body.is_none(), "shadow mode never modifies");
            assert_eq!(applied.outcome, "effected");
        }
    }

    #[test]
    fn given_sealed_evidence_when_re_encoded_without_its_digest_then_should_reproduce_the_digest() {
        let evidence = sample_evidence().seal().expect("seals");
        let mut unsealed = evidence.clone();
        unsealed.receipt_digest = String::new();
        let canonical = unsealed.encode().expect("encodes");
        assert_eq!(
            evidence.receipt_digest,
            blake3::hash(&canonical).to_string()
        );
        assert_eq!(evidence.receipt_digest.len(), 64);
    }

    #[test]
    fn given_evidence_when_round_tripped_through_cbor_then_should_decode_identically() {
        let evidence = sample_evidence().seal().expect("seals");
        let decoded =
            PolicyEvidence::decode(&evidence.encode().expect("encodes")).expect("decodes");
        assert_eq!(decoded, evidence);
    }

    #[test]
    fn given_consecutive_decisions_in_one_conversation_when_sealed_then_should_chain_digests() {
        let state = GovernorState::new(Arc::new(AllowAll), GovernorMode::Enforce);
        let conversation = ConversationId::new();
        let payload = b"body";
        let action = GovernedAction {
            kind: ActionKind::Send,
            stream: "laser",
            topic: "agent.commands",
            source: Some("planner"),
            target: None,
            conversation: Some(conversation),
            correlation: None,
            operation: None,
            tool: None,
            on_behalf_of: None,
            purpose: None,
            data_classification: None,
            payload,
            signed: false,
            counters: ActionCounters::default(),
        };
        let first = state
            .seal_evidence(&action, &ActionDecision::observe(), "effected", None)
            .expect("seals");
        assert_eq!(first.previous_digest, None);
        let second = state
            .seal_evidence(
                &action,
                &ActionDecision::block("no"),
                "blocked",
                Some(first.receipt_digest.clone()),
            )
            .expect("seals");
        assert_eq!(
            second.previous_digest.as_deref(),
            Some(first.receipt_digest.as_str())
        );
    }

    struct AllowAll;

    #[async_trait]
    impl ActionGovernor for AllowAll {
        async fn decide(&self, _: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
            Ok(ActionDecision::allow())
        }
    }

    fn sample_evidence() -> PolicyEvidence {
        PolicyEvidence {
            decision_id: "01J0000000000000000000000".to_owned(),
            decision: "block".to_owned(),
            mode: "enforce".to_owned(),
            kind: "command".to_owned(),
            stream: "laser".to_owned(),
            topic: "agent.commands".to_owned(),
            source: Some("planner".to_owned()),
            target: Some("worker".to_owned()),
            conversation: None,
            correlation: Some("corr-1".to_owned()),
            operation: Some("chat".to_owned()),
            tool: None,
            on_behalf_of: Some("user-1".to_owned()),
            reason: Some("blocked by policy".to_owned()),
            approved_scope: None,
            policy: Some(PolicyRef {
                pack_id: "base".to_owned(),
                pack_version: "1".to_owned(),
                rule_ids: vec!["r1".to_owned()],
            }),
            risk_score: Some(0.5),
            receipt_digest: String::new(),
            previous_digest: None,
            outcome: "blocked".to_owned(),
            at_micros: 1_717_171_777_000_000,
        }
    }

    struct FixedVerdict(Verdict);

    #[async_trait]
    impl ActionGovernor for FixedVerdict {
        async fn decide(&self, _: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
            let mut decision = ActionDecision::of(self.0.clone());
            decision.reason = Some("fixed".to_owned());
            Ok(decision)
        }
    }

    struct AlwaysErrors;

    #[async_trait]
    impl ActionGovernor for AlwaysErrors {
        async fn decide(&self, _: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
            Err(LaserError::Invalid("voter unavailable".to_owned()))
        }
    }

    fn voter(verdict: Verdict) -> Arc<dyn ActionGovernor> {
        Arc::new(FixedVerdict(verdict))
    }

    fn probe_action() -> GovernedAction<'static> {
        GovernedAction {
            kind: ActionKind::Send,
            stream: "laser",
            topic: "agent.commands",
            source: Some("planner"),
            target: None,
            conversation: None,
            correlation: None,
            operation: None,
            tool: None,
            on_behalf_of: None,
            purpose: None,
            data_classification: None,
            payload: b"body",
            signed: false,
            counters: ActionCounters::default(),
        }
    }

    #[tokio::test]
    async fn given_every_voter_allows_under_any_policy_when_decided_then_should_allow() {
        let quorum = QuorumGovernor::new(QuorumPolicy::Any)
            .voter("a", voter(Verdict::Allow), false)
            .voter("b", voter(Verdict::Allow), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Allow);
    }

    #[tokio::test]
    async fn given_a_mandatory_voter_blocks_when_decided_then_should_block_regardless_of_policy() {
        let quorum = QuorumGovernor::new(QuorumPolicy::Any)
            .voter("safety", voter(Verdict::Block), true)
            .voter("llm", voter(Verdict::Allow), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
        assert!(decision.reason.as_deref().unwrap().contains("safety"));
    }

    #[tokio::test]
    async fn given_all_policy_with_one_non_mandatory_block_when_decided_then_should_deny() {
        let quorum = QuorumGovernor::new(QuorumPolicy::All)
            .voter("a", voter(Verdict::Allow), false)
            .voter("b", voter(Verdict::Block), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
    }

    #[tokio::test]
    async fn given_any_policy_with_one_affirmative_among_blocks_when_decided_then_should_allow() {
        let quorum = QuorumGovernor::new(QuorumPolicy::Any)
            .voter("a", voter(Verdict::Block), false)
            .voter("b", voter(Verdict::Block), false)
            .voter("c", voter(Verdict::Allow), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Allow);
    }

    #[tokio::test]
    async fn given_at_least_two_policy_when_only_one_voter_affirms_then_should_deny() {
        let quorum = QuorumGovernor::new(QuorumPolicy::AtLeast(2))
            .voter("a", voter(Verdict::Allow), false)
            .voter("b", voter(Verdict::Block), false)
            .voter("c", voter(Verdict::Block), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
    }

    #[tokio::test]
    async fn given_at_least_two_policy_when_two_voters_affirm_then_should_allow() {
        let quorum = QuorumGovernor::new(QuorumPolicy::AtLeast(2))
            .voter("a", voter(Verdict::Allow), false)
            .voter("b", voter(Verdict::Observe), false)
            .voter("c", voter(Verdict::Block), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_ne!(decision.verdict, Verdict::Block);
    }

    #[tokio::test]
    async fn given_a_modify_voter_among_affirmatives_when_quorum_met_then_should_apply_the_body() {
        let quorum = QuorumGovernor::new(QuorumPolicy::All)
            .voter("a", voter(Verdict::Allow), false)
            .voter(
                "redactor",
                voter(Verdict::Modify {
                    body: b"redacted".to_vec(),
                }),
                false,
            );
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(
            decision.verdict,
            Verdict::Modify {
                body: b"redacted".to_vec()
            }
        );
    }

    #[tokio::test]
    async fn given_no_quorum_with_a_step_up_and_a_defer_when_decided_then_should_prefer_step_up() {
        let quorum = QuorumGovernor::new(QuorumPolicy::All)
            .voter(
                "reviewer",
                voter(Verdict::StepUp {
                    scope: "payments:approve".to_owned(),
                }),
                false,
            )
            .voter("scheduler", voter(Verdict::Defer), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert!(matches!(decision.verdict, Verdict::StepUp { .. }));
    }

    #[tokio::test]
    async fn given_no_voters_when_decided_then_should_block() {
        let quorum = QuorumGovernor::new(QuorumPolicy::All);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
    }

    #[tokio::test]
    async fn given_an_invalid_threshold_when_decided_then_should_block() {
        let quorum =
            QuorumGovernor::new(QuorumPolicy::AtLeast(0)).voter("a", voter(Verdict::Allow), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
    }

    #[tokio::test]
    async fn given_duplicate_voter_names_when_decided_then_should_block() {
        let quorum = QuorumGovernor::new(QuorumPolicy::Any)
            .voter("same", voter(Verdict::Allow), false)
            .voter("same", voter(Verdict::Allow), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
    }

    #[tokio::test]
    async fn given_a_mandatory_step_up_when_another_allows_then_should_step_up() {
        let quorum = QuorumGovernor::new(QuorumPolicy::Any)
            .voter(
                "safety",
                voter(Verdict::StepUp {
                    scope: "payments:approve".to_owned(),
                }),
                true,
            )
            .voter("llm", voter(Verdict::Allow), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert!(matches!(decision.verdict, Verdict::StepUp { .. }));
    }

    #[tokio::test]
    async fn given_a_mandatory_error_when_another_allows_then_should_block() {
        let quorum = QuorumGovernor::new(QuorumPolicy::Any)
            .voter("safety", Arc::new(AlwaysErrors), true)
            .voter("llm", voter(Verdict::Allow), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
    }

    #[tokio::test]
    async fn given_conflicting_modifications_when_quorum_met_then_should_block() {
        let quorum = QuorumGovernor::new(QuorumPolicy::All)
            .voter(
                "one",
                voter(Verdict::Modify {
                    body: b"one".to_vec(),
                }),
                false,
            )
            .voter(
                "two",
                voter(Verdict::Modify {
                    body: b"two".to_vec(),
                }),
                false,
            );
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
    }

    #[tokio::test]
    async fn given_an_erroring_voter_when_decided_then_should_not_veto_the_others() {
        let quorum = QuorumGovernor::new(QuorumPolicy::Any)
            .voter("flaky", Arc::new(AlwaysErrors), false)
            .voter("steady", voter(Verdict::Allow), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Allow);
        assert!(decision.reason.as_deref().unwrap().contains("error("));
    }

    #[tokio::test]
    async fn given_an_erroring_voter_alone_when_decided_then_should_deny_for_lack_of_quorum() {
        let quorum =
            QuorumGovernor::new(QuorumPolicy::Any).voter("flaky", Arc::new(AlwaysErrors), false);
        let decision = quorum.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
    }

    #[tokio::test]
    async fn given_a_swappable_governor_when_decided_then_should_run_the_active_policy() {
        let swappable = SwappableGovernor::new(voter(Verdict::Allow));
        let decision = swappable.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Allow);
    }

    #[tokio::test]
    async fn given_a_policy_swap_when_decided_again_then_should_run_the_new_policy() {
        let swappable = SwappableGovernor::new(voter(Verdict::Allow));
        swappable.swap(voter(Verdict::Block));
        let decision = swappable.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
    }

    #[tokio::test]
    async fn given_a_swap_when_applied_then_should_return_the_previous_policy() {
        let swappable = SwappableGovernor::new(voter(Verdict::Allow));
        let previous = swappable.swap(voter(Verdict::Block));
        let previous_decision = previous.decide(&probe_action()).await.expect("decides");
        assert_eq!(previous_decision.verdict, Verdict::Allow);
        let current_decision = swappable
            .current()
            .decide(&probe_action())
            .await
            .expect("decides");
        assert_eq!(current_decision.verdict, Verdict::Block);
    }

    #[tokio::test]
    async fn given_clones_sharing_one_swappable_governor_when_one_swaps_then_all_should_observe_it()
    {
        let swappable = Arc::new(SwappableGovernor::new(voter(Verdict::Allow)));
        let clone = Arc::clone(&swappable);
        clone.swap(voter(Verdict::Block));
        let decision = swappable.decide(&probe_action()).await.expect("decides");
        assert_eq!(decision.verdict, Verdict::Block);
    }
}

use crate::types::{AgentId, ConversationId, IntentId};
use laser_wire::framing::encode_named;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// A durable proposal for an effect. Publish it like any other typed record,
/// fold the [`Vote`] records that name it, and call [`decide`] for a replayable
/// outcome. This module defines record shapes and a pure fold, not a new wire
/// protocol or an implicit log integration.
///
/// `proposer` and `voter` are record claims. A deployment that needs trusted
/// authorship must enforce a signed-principal or topology-isolated profile.
/// This pure fold does not authenticate who appended a record.
///
/// This composes with, and does not replace, [`crate::govern::ActionGovernor`].
/// A governor can require a committed decision before releasing an effect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Intent {
    /// A fresh ULID stamped at build time.
    pub intent_id: IntentId,
    /// The conversation and usual partition key for this intent.
    pub conversation: ConversationId,
    /// The proposing agent.
    pub proposer: AgentId,
    /// The proposed effect, opaque to this module.
    pub body: Vec<u8>,
    /// BLAKE3 over the canonical named-field CBOR encoding of `body`.
    pub digest: String,
    /// The only principals whose votes count.
    pub eligible_voters: Vec<AgentId>,
    /// Eligible principals that must vote `Allow` before commit.
    pub mandatory_voters: Vec<AgentId>,
    /// How affirmative votes combine.
    pub policy: IntentPolicy,
    /// The policy version frozen when this intent was proposed.
    pub policy_version: u64,
    /// Epoch micros after which an incomplete intent aborts.
    pub deadline_micros: u64,
    /// Proposal time, in epoch micros.
    pub at_micros: u64,
}

#[bon::bon]
impl Intent {
    /// Build and validate an intent before it can be published.
    #[builder]
    pub fn new(
        conversation: ConversationId,
        proposer: AgentId,
        body: Vec<u8>,
        eligible_voters: Vec<AgentId>,
        #[builder(default)] mandatory_voters: Vec<AgentId>,
        policy: IntentPolicy,
        policy_version: u64,
        deadline_micros: u64,
    ) -> Result<Self, IntentError> {
        let intent = Self {
            intent_id: IntentId::new(),
            conversation,
            proposer,
            digest: digest_of(&body),
            body,
            eligible_voters,
            mandatory_voters,
            policy,
            policy_version,
            deadline_micros,
            at_micros: now_micros(),
        };
        intent.validate()?;
        Ok(intent)
    }

    /// Validate a constructed or deserialized intent.
    pub fn validate(&self) -> Result<(), IntentError> {
        if self.eligible_voters.is_empty() {
            return Err(IntentError::NoEligibleVoters);
        }

        let mut eligible = HashSet::with_capacity(self.eligible_voters.len());
        for voter in &self.eligible_voters {
            if !eligible.insert(voter.as_str()) {
                return Err(IntentError::DuplicateEligibleVoter(voter.to_string()));
            }
        }

        let mut mandatory = HashSet::with_capacity(self.mandatory_voters.len());
        for voter in &self.mandatory_voters {
            if !mandatory.insert(voter.as_str()) {
                return Err(IntentError::DuplicateMandatoryVoter(voter.to_string()));
            }
            if !eligible.contains(voter.as_str()) {
                return Err(IntentError::MandatoryVoterNotEligible(voter.to_string()));
            }
        }

        if let IntentPolicy::AtLeast(required) = self.policy
            && (required == 0 || required > self.eligible_voters.len())
        {
            return Err(IntentError::InvalidThreshold {
                required,
                eligible: self.eligible_voters.len(),
            });
        }
        if self.deadline_micros <= self.at_micros {
            return Err(IntentError::InvalidDeadline {
                proposed: self.at_micros,
                deadline: self.deadline_micros,
            });
        }
        if self.digest != digest_of(&self.body) {
            return Err(IntentError::DigestMismatch);
        }
        Ok(())
    }
}

/// An invalid durable-intent configuration or operation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IntentError {
    #[error("an intent requires at least one eligible voter")]
    NoEligibleVoters,
    #[error("eligible voter '{0}' appears more than once")]
    DuplicateEligibleVoter(String),
    #[error("mandatory voter '{0}' appears more than once")]
    DuplicateMandatoryVoter(String),
    #[error("mandatory voter '{0}' is not eligible")]
    MandatoryVoterNotEligible(String),
    #[error("threshold {required} is invalid for {eligible} eligible voters")]
    InvalidThreshold { required: usize, eligible: usize },
    #[error("deadline {deadline} must be after proposal time {proposed}")]
    InvalidDeadline { proposed: u64, deadline: u64 },
    #[error("intent digest does not match its body")]
    DigestMismatch,
    #[error("voter '{0}' is not eligible for this intent")]
    IneligibleVoter(String),
    #[error("decision is not bound to this intent body and policy version")]
    DecisionIntentMismatch,
}

/// How [`Vote`]s combine into a [`Decision`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentPolicy {
    /// Every eligible voter must allow.
    All,
    /// At least one eligible voter must allow.
    Any,
    /// At least `n` distinct eligible voters must allow.
    AtLeast(usize),
}

/// One principal's claimed ballot on one intent. Trusted authorship comes from
/// the deployment's signing or topic-write policy, not this record shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Vote {
    pub intent_id: IntentId,
    pub intent_digest: String,
    pub policy_version: u64,
    pub voter: AgentId,
    pub choice: VoteChoice,
    pub at_micros: u64,
}

impl Vote {
    /// Cast a ballot bound to the intent's digest and policy version.
    pub fn cast(intent: &Intent, voter: AgentId, choice: VoteChoice) -> Result<Self, IntentError> {
        intent.validate()?;
        if !intent.eligible_voters.contains(&voter) {
            return Err(IntentError::IneligibleVoter(voter.to_string()));
        }
        Ok(Self {
            intent_id: intent.intent_id,
            intent_digest: intent.digest.clone(),
            policy_version: intent.policy_version,
            voter,
            choice,
            at_micros: now_micros(),
        })
    }
}

/// A durable ballot choice.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum VoteChoice {
    Allow,
    Block,
    Abstain,
}

/// The terminal outcome of one intent.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum IntentOutcome {
    Committed,
    Aborted,
}

/// A decision bound to one exact intent body and policy version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub intent_id: IntentId,
    pub intent_digest: String,
    pub policy_version: u64,
    pub outcome: IntentOutcome,
    pub reason: String,
    /// Canonically ordered `(voter, choice)` pairs used by the fold.
    pub votes_considered: Vec<(AgentId, VoteChoice)>,
    /// The fact time that made the decision terminal, not replay wall time.
    pub at_micros: u64,
}

impl Decision {
    /// Verify that this decision is bound to `intent` before releasing an
    /// effect. An aborted decision is valid evidence but does not authorize.
    pub fn authorizes(&self, intent: &Intent) -> Result<bool, IntentError> {
        intent.validate()?;
        if self.intent_id != intent.intent_id
            || self.intent_digest != intent.digest
            || self.policy_version != intent.policy_version
        {
            return Err(IntentError::DecisionIntentMismatch);
        }
        Ok(self.outcome == IntentOutcome::Committed)
    }
}

/// Fold ballots into a decision, or return `None` while the outcome remains
/// reachable before the deadline.
///
/// Invalid ballots are ignored. Ballots outside the proposal-to-deadline
/// window are invalid. Identical repeats are idempotent. Conflicting choices
/// by one voter abort. Mandatory voters must allow before a quorum can commit.
/// The valid ballot set is sorted before folding, so input and replay order do
/// not affect the decision or its evidence.
pub fn decide(
    intent: &Intent,
    votes: &[Vote],
    now_micros: u64,
) -> Result<Option<Decision>, IntentError> {
    intent.validate()?;
    let mut valid: Vec<&Vote> = votes
        .iter()
        .filter(|vote| {
            vote.intent_id == intent.intent_id
                && vote.intent_digest == intent.digest
                && vote.policy_version == intent.policy_version
                && intent.eligible_voters.contains(&vote.voter)
                && vote.at_micros >= intent.at_micros
                && vote.at_micros <= intent.deadline_micros
                && vote.at_micros <= now_micros
        })
        .collect();
    valid.sort_by(|left, right| {
        left.voter
            .as_str()
            .cmp(right.voter.as_str())
            .then(left.at_micros.cmp(&right.at_micros))
            .then(choice_rank(left.choice).cmp(&choice_rank(right.choice)))
    });

    let mut ballots: HashMap<&AgentId, VoteChoice> = HashMap::new();
    let mut considered = Vec::new();
    let mut terminal_at = intent.at_micros;
    for vote in valid {
        terminal_at = terminal_at.max(vote.at_micros);
        match ballots.get(&vote.voter) {
            None => {
                ballots.insert(&vote.voter, vote.choice);
                considered.push((vote.voter.clone(), vote.choice));
            }
            Some(existing) if *existing == vote.choice => {}
            Some(_) => {
                considered.push((vote.voter.clone(), vote.choice));
                return Ok(Some(sealed(
                    intent,
                    IntentOutcome::Aborted,
                    format!("voter '{}' cast conflicting votes", vote.voter),
                    considered,
                    terminal_at,
                )));
            }
        }
    }

    for voter in &intent.mandatory_voters {
        if let Some(choice) = ballots.get(voter)
            && *choice != VoteChoice::Allow
        {
            return Ok(Some(sealed(
                intent,
                IntentOutcome::Aborted,
                format!("mandatory voter '{voter}' did not allow"),
                considered,
                terminal_at,
            )));
        }
    }

    let allow = ballots
        .values()
        .filter(|choice| **choice == VoteChoice::Allow)
        .count();
    let responded = ballots.len();
    let total = intent.eligible_voters.len();
    let mandatory_met = intent
        .mandatory_voters
        .iter()
        .all(|voter| ballots.get(voter) == Some(&VoteChoice::Allow));
    let quorum_met = match intent.policy {
        IntentPolicy::All => allow == total,
        IntentPolicy::Any => allow >= 1,
        IntentPolicy::AtLeast(required) => allow >= required,
    };
    if quorum_met && mandatory_met {
        return Ok(Some(sealed(
            intent,
            IntentOutcome::Committed,
            "quorum met".to_owned(),
            considered,
            terminal_at,
        )));
    }

    let impossible = match intent.policy {
        IntentPolicy::All => responded > allow,
        IntentPolicy::Any => responded == total && allow == 0,
        IntentPolicy::AtLeast(required) => allow + (total - responded) < required,
    };
    let deadline_passed = now_micros >= intent.deadline_micros;
    if impossible || deadline_passed {
        let reason = if deadline_passed && !mandatory_met {
            "mandatory approval not reached by deadline".to_owned()
        } else if impossible {
            "quorum became impossible".to_owned()
        } else {
            "quorum not reached by deadline".to_owned()
        };
        return Ok(Some(sealed(
            intent,
            IntentOutcome::Aborted,
            reason,
            considered,
            if deadline_passed {
                intent.deadline_micros
            } else {
                terminal_at
            },
        )));
    }
    Ok(None)
}

fn choice_rank(choice: VoteChoice) -> u8 {
    match choice {
        VoteChoice::Allow => 0,
        VoteChoice::Block => 1,
        VoteChoice::Abstain => 2,
    }
}

fn sealed(
    intent: &Intent,
    outcome: IntentOutcome,
    reason: String,
    votes_considered: Vec<(AgentId, VoteChoice)>,
    at_micros: u64,
) -> Decision {
    Decision {
        intent_id: intent.intent_id,
        intent_digest: intent.digest.clone(),
        policy_version: intent.policy_version,
        outcome,
        reason,
        votes_considered,
        at_micros,
    }
}

fn digest_of(body: &[u8]) -> String {
    #[derive(Serialize)]
    struct DigestInput<'a> {
        body: &'a [u8],
    }
    let canonical = encode_named(&DigestInput { body }).expect("bytes always encode");
    blake3::hash(&canonical).to_string()
}

fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(name: &str) -> AgentId {
        name.parse().expect("agent id parses")
    }

    fn intent(policy: IntentPolicy, voters: &[&str], mandatory: &[&str]) -> Intent {
        let mut intent = Intent::builder()
            .conversation(ConversationId::new())
            .proposer(agent("proposer"))
            .body(b"transfer $100".to_vec())
            .eligible_voters(voters.iter().map(|voter| agent(voter)).collect())
            .mandatory_voters(mandatory.iter().map(|voter| agent(voter)).collect())
            .policy(policy)
            .policy_version(1)
            .deadline_micros(u64::MAX)
            .build()
            .expect("intent is valid");
        intent.at_micros = 10;
        intent.deadline_micros = 1_000;
        intent
    }

    fn vote(intent: &Intent, voter: &str, choice: VoteChoice) -> Vote {
        let mut vote = Vote::cast(intent, agent(voter), choice).expect("voter is eligible");
        vote.at_micros = 100;
        vote
    }

    fn decided(intent: &Intent, votes: &[Vote], now: u64) -> Decision {
        decide(intent, votes, now)
            .expect("intent is valid")
            .expect("intent decides")
    }

    #[test]
    fn given_invalid_voter_sets_when_built_then_should_fail_closed() {
        let result = Intent::builder()
            .conversation(ConversationId::new())
            .proposer(agent("proposer"))
            .body(Vec::new())
            .eligible_voters(Vec::new())
            .policy(IntentPolicy::Any)
            .policy_version(1)
            .deadline_micros(u64::MAX)
            .build();
        assert_eq!(result, Err(IntentError::NoEligibleVoters));

        let mut duplicate = intent(IntentPolicy::Any, &["a"], &[]);
        duplicate.eligible_voters.push(agent("a"));
        assert_eq!(
            duplicate.validate(),
            Err(IntentError::DuplicateEligibleVoter("a".to_owned()))
        );
    }

    #[test]
    fn given_invalid_thresholds_when_built_then_should_fail_closed() {
        for required in [0, 2] {
            let result = Intent::builder()
                .conversation(ConversationId::new())
                .proposer(agent("proposer"))
                .body(Vec::new())
                .eligible_voters(vec![agent("a")])
                .policy(IntentPolicy::AtLeast(required))
                .policy_version(1)
                .deadline_micros(u64::MAX)
                .build();
            assert!(matches!(result, Err(IntentError::InvalidThreshold { .. })));
        }
    }

    #[test]
    fn given_a_mutated_body_when_decided_then_should_reject_the_intent() {
        let mut intent = intent(IntentPolicy::Any, &["a"], &[]);
        intent.body.push(1);
        assert_eq!(decide(&intent, &[], 500), Err(IntentError::DigestMismatch));
    }

    #[test]
    fn given_an_outsider_when_casting_then_should_reject_the_vote() {
        let intent = intent(IntentPolicy::Any, &["a"], &[]);
        assert_eq!(
            Vote::cast(&intent, agent("outsider"), VoteChoice::Allow),
            Err(IntentError::IneligibleVoter("outsider".to_owned()))
        );
    }

    #[test]
    fn given_all_policy_with_every_allow_when_decided_then_should_commit() {
        let intent = intent(IntentPolicy::All, &["a", "b"], &[]);
        let votes = [
            vote(&intent, "a", VoteChoice::Allow),
            vote(&intent, "b", VoteChoice::Allow),
        ];
        let decision = decided(&intent, &votes, 500);
        assert_eq!(decision.outcome, IntentOutcome::Committed);
        assert_eq!(decision.intent_digest, intent.digest);
        assert_eq!(decision.policy_version, 1);
        assert!(decision.authorizes(&intent).expect("decision is bound"));
    }

    #[test]
    fn given_any_policy_with_every_non_allow_when_decided_then_should_abort_early() {
        let intent = intent(IntentPolicy::Any, &["a", "b"], &[]);
        let votes = [
            vote(&intent, "a", VoteChoice::Block),
            vote(&intent, "b", VoteChoice::Abstain),
        ];
        assert_eq!(
            decided(&intent, &votes, 500).outcome,
            IntentOutcome::Aborted
        );
    }

    #[test]
    fn given_reachable_threshold_when_decided_then_should_wait() {
        let intent = intent(IntentPolicy::AtLeast(2), &["a", "b", "c"], &[]);
        let votes = [vote(&intent, "a", VoteChoice::Allow)];
        assert_eq!(decide(&intent, &votes, 500).expect("valid intent"), None);
    }

    #[test]
    fn given_impossible_threshold_when_decided_then_should_abort_early() {
        let intent = intent(IntentPolicy::AtLeast(2), &["a", "b", "c"], &[]);
        let votes = [
            vote(&intent, "a", VoteChoice::Block),
            vote(&intent, "b", VoteChoice::Block),
        ];
        assert_eq!(
            decided(&intent, &votes, 500).outcome,
            IntentOutcome::Aborted
        );
    }

    #[test]
    fn given_missing_mandatory_allow_when_quorum_met_then_should_wait_then_abort() {
        let intent = intent(IntentPolicy::Any, &["safety", "llm"], &["safety"]);
        let votes = [vote(&intent, "llm", VoteChoice::Allow)];
        assert_eq!(decide(&intent, &votes, 500).expect("valid intent"), None);
        let decision = decided(&intent, &votes, 1_000);
        assert_eq!(decision.outcome, IntentOutcome::Aborted);
        assert!(decision.reason.contains("mandatory"));
    }

    #[test]
    fn given_mandatory_allow_and_quorum_when_decided_then_should_commit() {
        let intent = intent(IntentPolicy::Any, &["safety", "llm"], &["safety"]);
        let votes = [vote(&intent, "safety", VoteChoice::Allow)];
        assert_eq!(
            decided(&intent, &votes, 500).outcome,
            IntentOutcome::Committed
        );
    }

    #[test]
    fn given_late_allow_when_decided_then_should_ignore_it_and_abort() {
        let intent = intent(IntentPolicy::Any, &["a"], &[]);
        let mut late = vote(&intent, "a", VoteChoice::Allow);
        late.at_micros = 1_001;
        assert_eq!(
            decided(&intent, &[late], 1_001).outcome,
            IntentOutcome::Aborted
        );
    }

    #[test]
    fn given_a_future_ballot_when_decided_then_should_ignore_it() {
        let intent = intent(IntentPolicy::Any, &["a"], &[]);
        let mut future = vote(&intent, "a", VoteChoice::Allow);
        future.at_micros = 600;
        assert_eq!(decide(&intent, &[future], 500).expect("valid intent"), None);
    }

    #[test]
    fn given_conflicting_votes_in_any_order_when_decided_then_should_match() {
        let intent = intent(IntentPolicy::All, &["a", "b"], &[]);
        let allow = vote(&intent, "a", VoteChoice::Allow);
        let mut block = vote(&intent, "a", VoteChoice::Block);
        block.at_micros = 200;
        let first = decided(&intent, &[allow.clone(), block.clone()], 500);
        let second = decided(&intent, &[block, allow], 500);
        assert_eq!(first, second);
        assert!(first.reason.contains("conflicting"));
    }

    #[test]
    fn given_identical_repeats_when_decided_then_should_be_idempotent() {
        let intent = intent(IntentPolicy::All, &["a", "b"], &[]);
        let allow = vote(&intent, "a", VoteChoice::Allow);
        assert_eq!(
            decide(&intent, &[allow.clone(), allow], 500).expect("valid intent"),
            None
        );
    }

    #[test]
    fn given_invalid_ballot_metadata_when_decided_then_should_ignore_it() {
        let intent = intent(IntentPolicy::Any, &["a"], &[]);
        let mut vote = vote(&intent, "a", VoteChoice::Allow);
        vote.intent_digest = "wrong".to_owned();
        assert_eq!(decide(&intent, &[vote], 500).expect("valid intent"), None);
    }

    #[test]
    fn given_an_intent_when_round_tripped_then_should_validate() {
        let intent = intent(IntentPolicy::Any, &["a"], &[]);
        let json = serde_json::to_vec(&intent).expect("encodes");
        let decoded: Intent = serde_json::from_slice(&json).expect("decodes");
        assert_eq!(decoded, intent);
        decoded.validate().expect("round trip remains valid");
    }

    #[test]
    fn given_durable_records_when_encoded_as_json_then_enum_words_should_be_snake_case() {
        assert_eq!(
            serde_json::to_value(IntentPolicy::AtLeast(1)).expect("policy encodes"),
            serde_json::json!({ "at_least": 1 })
        );
        assert_eq!(
            serde_json::to_value(VoteChoice::Abstain).expect("choice encodes"),
            serde_json::json!("abstain")
        );
        assert_eq!(
            serde_json::to_value(IntentOutcome::Aborted).expect("outcome encodes"),
            serde_json::json!("aborted")
        );
    }
}

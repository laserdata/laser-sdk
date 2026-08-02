use crate::govern::PolicyEvidence;
use std::collections::{HashMap, HashSet};

/// One agent's folded activity, so a supervisor answers "what has this agent
/// been doing" from evidence already read off the audit topic, without
/// re-deriving counts by hand every time. `by_verdict` counts every decision
/// this agent triggered, keyed by [`PolicyEvidence::decision`] (`allow`,
/// `observe`, `block`, `step_up`, `modify`, `defer`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AgentActivity {
    /// Every decision folded in for this agent, regardless of verdict.
    pub decisions: u64,
    by_verdict: HashMap<String, u64>,
    /// The most recent decision folded in (by `at_micros`), for a supervisor
    /// that wants the detail behind the counts.
    pub last_decision: Option<PolicyEvidence>,
}

impl AgentActivity {
    fn fold(&mut self, evidence: &PolicyEvidence) {
        self.decisions += 1;
        *self
            .by_verdict
            .entry(evidence.decision.clone())
            .or_insert(0) += 1;
        if self.last_decision.as_ref().is_none_or(|last| {
            (last.at_micros, last.decision_id.as_str())
                < (evidence.at_micros, evidence.decision_id.as_str())
        }) {
            self.last_decision = Some(evidence.clone());
        }
    }

    /// How many decisions this agent triggered with this exact verdict name
    /// (e.g. `"block"`). Zero for a verdict never folded in.
    pub fn count(&self, verdict: &str) -> u64 {
        self.by_verdict.get(verdict).copied().unwrap_or(0)
    }
}

/// A swarm-wide read model: every agent's folded [`AgentActivity`], built by
/// folding [`PolicyEvidence`] records already read off the audit topic (a
/// replay cursor, a projection, however the caller already reads it). A pure
/// in-process fold, not a topic reader of its own: it composes with whatever
/// offset-resume convention the caller uses, the same way
/// [`crate::intent::decide`] composes with whatever publishes the records it
/// folds. The role this answers is a Supervisor's: "what has the swarm been
/// doing," read-only over evidence, never a write path of its own.
#[derive(Debug, Clone, Default)]
pub struct SwarmActivity {
    by_agent: HashMap<String, AgentActivity>,
    seen_decisions: HashSet<String>,
}

impl SwarmActivity {
    /// An empty swarm view. Fold evidence in with [`observe`](Self::observe).
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one more evidence record in. A record with no `source` (a
    /// governed action that carried no acting agent) is not attributable to
    /// any agent and is dropped.
    pub fn observe(&mut self, evidence: &PolicyEvidence) {
        let Some(source) = evidence.source.as_deref() else {
            return;
        };
        if !self.seen_decisions.insert(evidence.decision_id.clone()) {
            return;
        }
        self.by_agent
            .entry(source.to_owned())
            .or_default()
            .fold(evidence);
    }

    /// This agent's folded activity, or `None` if this view has never folded
    /// a decision it triggered.
    pub fn agent(&self, agent: &str) -> Option<&AgentActivity> {
        self.by_agent.get(agent)
    }

    /// Every agent this view has folded activity for, busiest first (ties
    /// broken by name), so a supervisor's summary reads in the order that
    /// matters most.
    pub fn agents(&self) -> Vec<(&str, &AgentActivity)> {
        let mut agents: Vec<_> = self
            .by_agent
            .iter()
            .map(|(name, activity)| (name.as_str(), activity))
            .collect();
        agents.sort_by(|a, b| b.1.decisions.cmp(&a.1.decisions).then_with(|| a.0.cmp(b.0)));
        agents
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence(source: &str, decision: &str, at_micros: u64) -> PolicyEvidence {
        PolicyEvidence {
            decision_id: format!("{source}-{decision}-{at_micros}"),
            decision: decision.to_owned(),
            mode: "enforce".to_owned(),
            kind: "send".to_owned(),
            stream: "laser".to_owned(),
            topic: "agent.commands".to_owned(),
            source: Some(source.to_owned()),
            target: None,
            conversation: None,
            correlation: None,
            operation: None,
            tool: None,
            on_behalf_of: None,
            reason: None,
            approved_scope: None,
            policy: None,
            risk_score: None,
            receipt_digest: String::new(),
            previous_digest: None,
            outcome: "effected".to_owned(),
            at_micros,
        }
    }

    #[test]
    fn given_no_evidence_when_queried_then_should_have_no_agents() {
        let swarm = SwarmActivity::new();
        assert!(swarm.agent("planner").is_none());
        assert!(swarm.agents().is_empty());
    }

    #[test]
    fn given_evidence_with_no_source_when_observed_then_should_be_dropped() {
        let mut swarm = SwarmActivity::new();
        let mut anonymous = evidence("planner", "allow", 1);
        anonymous.source = None;
        swarm.observe(&anonymous);
        assert!(swarm.agents().is_empty());
    }

    #[test]
    fn given_several_decisions_for_one_agent_when_observed_then_should_count_by_verdict() {
        let mut swarm = SwarmActivity::new();
        swarm.observe(&evidence("planner", "allow", 1));
        swarm.observe(&evidence("planner", "block", 2));
        swarm.observe(&evidence("planner", "block", 3));

        let activity = swarm.agent("planner").expect("planner has activity");
        assert_eq!(activity.decisions, 3);
        assert_eq!(activity.count("allow"), 1);
        assert_eq!(activity.count("block"), 2);
        assert_eq!(activity.count("defer"), 0);
    }

    #[test]
    fn given_out_of_order_evidence_when_observed_then_last_decision_should_be_the_latest_by_time() {
        let mut swarm = SwarmActivity::new();
        swarm.observe(&evidence("planner", "allow", 100));
        swarm.observe(&evidence("planner", "block", 50));

        let activity = swarm.agent("planner").expect("planner has activity");
        assert_eq!(
            activity.last_decision.as_ref().map(|e| e.decision.as_str()),
            Some("allow")
        );
        assert_eq!(
            activity.last_decision.as_ref().map(|e| e.at_micros),
            Some(100)
        );
    }

    #[test]
    fn given_replayed_evidence_when_observed_then_should_count_it_once() {
        let mut swarm = SwarmActivity::new();
        let evidence = evidence("planner", "block", 1);
        swarm.observe(&evidence);
        swarm.observe(&evidence);

        let activity = swarm.agent("planner").expect("planner has activity");
        assert_eq!(activity.decisions, 1);
        assert_eq!(activity.count("block"), 1);
    }

    #[test]
    fn given_equal_timestamps_when_observed_then_last_decision_should_use_the_id_tie_break() {
        let mut swarm = SwarmActivity::new();
        let mut lower = evidence("planner", "allow", 1);
        lower.decision_id = "decision-a".to_owned();
        let mut higher = evidence("planner", "block", 1);
        higher.decision_id = "decision-b".to_owned();
        swarm.observe(&higher);
        swarm.observe(&lower);

        let activity = swarm.agent("planner").expect("planner has activity");
        assert_eq!(
            activity
                .last_decision
                .as_ref()
                .map(|evidence| evidence.decision_id.as_str()),
            Some("decision-b")
        );
    }

    #[test]
    fn given_several_agents_when_listed_then_should_order_busiest_first() {
        let mut swarm = SwarmActivity::new();
        swarm.observe(&evidence("quiet", "allow", 1));
        swarm.observe(&evidence("busy", "allow", 1));
        swarm.observe(&evidence("busy", "block", 2));

        let agents = swarm.agents();
        assert_eq!(
            agents.iter().map(|(name, _)| *name).collect::<Vec<_>>(),
            vec!["busy", "quiet"]
        );
    }
}

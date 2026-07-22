use crate::convert::{py_to_de, ser_to_py};
use crate::errors::InvalidError;
use laser_sdk::intent::{
    self, Decision, Intent, IntentError, IntentOutcome, IntentPolicy, Vote, VoteChoice,
};
use laser_sdk::types::{AgentId, ConversationId};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::str::FromStr;

fn parse_agent(name: &str) -> PyResult<AgentId> {
    AgentId::from_str(name)
        .map_err(|error| InvalidError::new_err(format!("invalid agent id: {error}")))
}

fn parse_agents(names: Vec<String>) -> PyResult<Vec<AgentId>> {
    names.iter().map(|name| parse_agent(name)).collect()
}

fn parse_conversation(text: &str) -> PyResult<ConversationId> {
    ConversationId::from_str(text)
        .map_err(|error| InvalidError::new_err(format!("invalid conversation id: {error}")))
}

fn parse_choice(text: &str) -> PyResult<VoteChoice> {
    text.parse().map_err(|_| {
        InvalidError::new_err(format!(
            "vote choice must be \"allow\", \"block\", or \"abstain\", got \"{text}\""
        ))
    })
}

fn intent_error(error: IntentError) -> PyErr {
    InvalidError::new_err(error.to_string())
}

/// How [`Vote`]s combine into a [`Decision`]. Mirrors `QuorumPolicy`, applied
/// durably instead of in-process.
#[gen_stub_pyclass]
#[pyclass(name = "IntentPolicy", from_py_object)]
#[derive(Clone, Copy)]
pub struct PyIntentPolicy {
    inner: IntentPolicy,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyIntentPolicy {
    /// Every eligible voter must vote `allow`.
    #[staticmethod]
    fn all() -> Self {
        Self {
            inner: IntentPolicy::All,
        }
    }

    /// At least one eligible voter must vote `allow`.
    #[staticmethod]
    fn any() -> Self {
        Self {
            inner: IntentPolicy::Any,
        }
    }

    /// At least `n` distinct eligible voters must vote `allow`.
    #[staticmethod]
    fn at_least(n: usize) -> Self {
        Self {
            inner: IntentPolicy::AtLeast(n),
        }
    }
}

/// A durable, log-appended proposal for an effect, so independent voters can
/// approve or block it before it runs and a decision replays deterministically
/// after a crash. Publish it on any typed topic like any other record, fold
/// the `Vote`s that name it, and call `decide` for the deterministic outcome.
/// `intent_id`, `digest`, and `at_micros` are computed at construction, never
/// supplied.
#[gen_stub_pyclass]
#[pyclass(name = "Intent", from_py_object)]
#[derive(Clone)]
pub struct PyIntent {
    pub(crate) inner: Intent,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyIntent {
    #[new]
    #[pyo3(signature = (
        conversation,
        proposer,
        body,
        eligible_voters,
        policy,
        policy_version,
        deadline_micros,
        mandatory_voters=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        conversation: String,
        proposer: String,
        body: Vec<u8>,
        eligible_voters: Vec<String>,
        policy: PyIntentPolicy,
        policy_version: u64,
        deadline_micros: u64,
        mandatory_voters: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let conversation = parse_conversation(&conversation)?;
        let proposer = parse_agent(&proposer)?;
        let eligible_voters = parse_agents(eligible_voters)?;
        let mandatory_voters = parse_agents(mandatory_voters.unwrap_or_default())?;
        let inner = Intent::builder()
            .conversation(conversation)
            .proposer(proposer)
            .body(body)
            .eligible_voters(eligible_voters)
            .mandatory_voters(mandatory_voters)
            .policy(policy.inner)
            .policy_version(policy_version)
            .deadline_micros(deadline_micros)
            .build()
            .map_err(intent_error)?;
        Ok(Self { inner })
    }

    /// This intent's id (ULID).
    #[getter]
    fn intent_id(&self) -> String {
        self.inner.intent_id.to_string()
    }

    /// The conversation this intent belongs to.
    #[getter]
    fn conversation(&self) -> String {
        self.inner.conversation.to_string()
    }

    /// The proposing agent.
    #[getter]
    fn proposer(&self) -> String {
        self.inner.proposer.to_string()
    }

    /// The proposed effect.
    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.body)
    }

    /// BLAKE3 (hex) over the canonical encoding of `body`.
    #[getter]
    fn digest(&self) -> &str {
        &self.inner.digest
    }

    /// The only principals whose votes count.
    #[getter]
    fn eligible_voters(&self) -> Vec<String> {
        self.inner
            .eligible_voters
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// The eligible voters that must allow before commit.
    #[getter]
    fn mandatory_voters(&self) -> Vec<String> {
        self.inner
            .mandatory_voters
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// How votes combine into a decision.
    #[getter]
    fn policy(&self) -> PyIntentPolicy {
        PyIntentPolicy {
            inner: self.inner.policy,
        }
    }

    /// The policy version this intent was proposed under.
    #[getter]
    fn policy_version(&self) -> u64 {
        self.inner.policy_version
    }

    /// The deadline (epoch micros) after which `decide` forces a decision.
    #[getter]
    fn deadline_micros(&self) -> u64 {
        self.inner.deadline_micros
    }

    /// Build time, epoch micros.
    #[getter]
    fn at_micros(&self) -> u64 {
        self.inner.at_micros
    }

    fn __laser_json__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner)
    }

    #[staticmethod]
    fn __laser_from_json__(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let inner: Intent = py_to_de(value)?;
        inner.validate().map_err(intent_error)?;
        Ok(Self { inner })
    }
}

/// One voter's vote on one intent. Checked against the intent's exact digest
/// and policy version before it counts: a vote for the wrong digest, the
/// wrong policy version, or from a non-eligible voter is invalid evidence,
/// never counted by `decide`.
#[gen_stub_pyclass]
#[pyclass(name = "Vote", from_py_object)]
#[derive(Clone)]
pub struct PyVote {
    inner: Vote,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyVote {
    /// Cast a vote naming `intent`'s exact digest and policy version
    /// (`choice` is `"allow"`, `"block"`, or `"abstain"`), so a vote for a
    /// stale or different intent is invalid by construction.
    #[staticmethod]
    fn cast(intent: &PyIntent, voter: String, choice: String) -> PyResult<Self> {
        let voter = parse_agent(&voter)?;
        let choice = parse_choice(&choice)?;
        let inner = Vote::cast(&intent.inner, voter, choice).map_err(intent_error)?;
        Ok(Self { inner })
    }

    #[getter]
    fn intent_id(&self) -> String {
        self.inner.intent_id.to_string()
    }

    #[getter]
    fn intent_digest(&self) -> &str {
        &self.inner.intent_digest
    }

    #[getter]
    fn policy_version(&self) -> u64 {
        self.inner.policy_version
    }

    #[getter]
    fn voter(&self) -> String {
        self.inner.voter.to_string()
    }

    /// `"allow"`, `"block"`, or `"abstain"`.
    #[getter]
    fn choice(&self) -> &'static str {
        self.inner.choice.into()
    }

    #[getter]
    fn at_micros(&self) -> u64 {
        self.inner.at_micros
    }

    fn __laser_json__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner)
    }

    #[staticmethod]
    fn __laser_from_json__(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: py_to_de(value)?,
        })
    }
}

/// A deterministic decision over one intent, replaying to the same outcome
/// from the same intent and vote set.
#[gen_stub_pyclass]
#[pyclass(name = "Decision", from_py_object)]
#[derive(Clone)]
pub struct PyDecision {
    inner: Decision,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyDecision {
    #[getter]
    fn intent_id(&self) -> String {
        self.inner.intent_id.to_string()
    }

    #[getter]
    fn intent_digest(&self) -> &str {
        &self.inner.intent_digest
    }

    #[getter]
    fn policy_version(&self) -> u64 {
        self.inner.policy_version
    }

    /// `"committed"` or `"aborted"`.
    #[getter]
    fn outcome(&self) -> &'static str {
        match self.inner.outcome {
            IntentOutcome::Committed => "committed",
            IntentOutcome::Aborted => "aborted",
        }
    }

    #[getter]
    fn reason(&self) -> &str {
        &self.inner.reason
    }

    /// Every valid vote considered, as `(voter, choice)` pairs.
    #[getter]
    fn votes_considered(&self) -> Vec<(String, &'static str)> {
        self.inner
            .votes_considered
            .iter()
            .map(|(voter, choice)| (voter.to_string(), (*choice).into()))
            .collect()
    }

    #[getter]
    fn at_micros(&self) -> u64 {
        self.inner.at_micros
    }

    /// Verify that this decision is bound to `intent` and return whether it
    /// authorizes the effect.
    fn authorizes(&self, intent: &PyIntent) -> PyResult<bool> {
        self.inner.authorizes(&intent.inner).map_err(intent_error)
    }

    fn __laser_json__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        ser_to_py(py, &self.inner)
    }

    #[staticmethod]
    fn __laser_from_json__(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: py_to_de(value)?,
        })
    }
}

/// Fold `votes` against `intent` and decide, or return `None` when the
/// outcome is not yet reachable and `now_micros` has not passed the intent's
/// deadline. Deterministic: the same intent, vote set, and `now_micros` (once
/// past the deadline) always reach the same `Decision`, so a crashed decider
/// replays to the identical outcome.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn decide(
    intent: &PyIntent,
    votes: Vec<PyVote>,
    now_micros: u64,
) -> PyResult<Option<PyDecision>> {
    let votes: Vec<Vote> = votes.into_iter().map(|vote| vote.inner).collect();
    intent::decide(&intent.inner, &votes, now_micros)
        .map(|decision| decision.map(|inner| PyDecision { inner }))
        .map_err(intent_error)
}

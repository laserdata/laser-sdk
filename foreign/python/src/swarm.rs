use crate::govern::PyPolicyEvidence;
use laser_sdk::swarm::{AgentActivity, SwarmActivity};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// One agent's folded activity, so a supervisor answers "what has this agent
/// been doing" from evidence already read off the audit topic, without
/// re-deriving counts by hand every time.
#[gen_stub_pyclass]
#[pyclass(name = "AgentActivity")]
pub struct PyAgentActivity {
    inner: AgentActivity,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAgentActivity {
    /// Every decision folded in for this agent, regardless of verdict.
    #[getter]
    fn decisions(&self) -> u64 {
        self.inner.decisions
    }

    /// How many decisions this agent triggered with this exact verdict name
    /// (`"allow"`, `"observe"`, `"block"`, `"step_up"`, `"modify"`, or
    /// `"defer"`). Zero for a verdict never folded in.
    fn count(&self, verdict: &str) -> u64 {
        self.inner.count(verdict)
    }

    /// The most recent decision folded in, for the detail behind the counts.
    #[getter]
    fn last_decision(&self) -> Option<PyPolicyEvidence> {
        self.inner
            .last_decision
            .clone()
            .map(|inner| PyPolicyEvidence { inner })
    }
}

/// A swarm-wide read model: every agent's folded `AgentActivity`, built by
/// folding `PolicyEvidence` records already read off the audit topic (a
/// replay cursor, a projection, however the caller already reads it). A pure
/// in-process fold, not a topic reader of its own.
#[gen_stub_pyclass]
#[pyclass(name = "SwarmActivity")]
pub struct PySwarmActivity {
    inner: SwarmActivity,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySwarmActivity {
    /// An empty swarm view. Fold evidence in with `observe`.
    #[new]
    fn new() -> Self {
        Self {
            inner: SwarmActivity::new(),
        }
    }

    /// Fold one more evidence record in. A record with no `source` (a
    /// governed action that carried no acting agent) is not attributable to
    /// any agent and is dropped.
    fn observe(&mut self, evidence: &PyPolicyEvidence) {
        self.inner.observe(&evidence.inner);
    }

    /// This agent's folded activity, or `None` if this view has never folded
    /// a decision it triggered.
    fn agent(&self, agent: &str) -> Option<PyAgentActivity> {
        self.inner.agent(agent).map(to_py_activity)
    }

    /// Every agent this view has folded activity for, as `(name, activity)`
    /// pairs, busiest first (ties broken by name).
    fn agents(&self) -> Vec<(String, PyAgentActivity)> {
        self.inner
            .agents()
            .into_iter()
            .map(|(name, activity)| (name.to_owned(), to_py_activity(activity)))
            .collect()
    }
}

fn to_py_activity(activity: &laser_sdk::swarm::AgentActivity) -> PyAgentActivity {
    PyAgentActivity {
        inner: activity.clone(),
    }
}

use crate::agent::PyAgentMessage;
use crate::convert::{py_to_de, ser_to_py};
use crate::govern::PyPolicyEvidence;
use laser_sdk::agent::AgentMessage;
use laser_sdk::context::ContextMessage;
use laser_sdk::crash_context::CrashContext;
use laser_sdk::wire::agent::AgentDeadLetter;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// A crash-recovery bundle for one conversation, so a recovery tool answers
/// "what was happening right before this crashed" from one call instead of
/// stitching three separate reads together by hand: the recent journal tail,
/// the dead-letter capsule for the crashed message (if any), and the most
/// recent governance decision (if any). Pure combination of already-read
/// pieces: no I/O of its own, and no model call, ever.
#[gen_stub_pyclass]
#[pyclass(name = "CrashContext")]
pub struct PyCrashContext {
    inner: CrashContext,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyCrashContext {
    /// Combine already-read pieces into one bundle. `journal` is a list of
    /// `AgentMessage` (from `ContextScope.fetch` or `Laser.assemble_context`),
    /// `dead_letter` is a capsule dict (the same shape `Laser.redrive_dead_letter`
    /// takes: `source` the 20 big-endian packed locator bytes, `reason` the
    /// dead-letter reason code, `attempts` an int, `detail` an optional string,
    /// `payload` the poison message's raw bytes), and `last_decision` is a
    /// decoded `PolicyEvidence`.
    #[new]
    #[pyo3(signature = (journal, dead_letter=None, last_decision=None))]
    fn new(
        journal: Vec<PyRef<'_, PyAgentMessage>>,
        dead_letter: Option<&Bound<'_, PyAny>>,
        last_decision: Option<&PyPolicyEvidence>,
    ) -> PyResult<Self> {
        let journal = journal
            .iter()
            .map(|message| ContextMessage {
                id: message.inner.id,
                provenance: message.inner.provenance.clone(),
                payload: message.inner.payload.clone(),
                envelope: message.inner.envelope.clone(),
            })
            .collect();
        let dead_letter = dead_letter.map(py_to_de::<AgentDeadLetter>).transpose()?;
        let last_decision = last_decision.map(|evidence| evidence.inner.clone());
        Ok(Self {
            inner: CrashContext::assemble(journal, dead_letter, last_decision),
        })
    }

    /// A deterministic, plain-text digest of this bundle: the journal (oldest
    /// first, each entry the acting agent and a truncated payload preview),
    /// the dead-letter detail, and the last decision, in that fixed order
    /// every time. For a recovery agent's own prompt assembly, or a log line:
    /// never produced by a model, and never fed to one inside the SDK.
    fn summarize(&self) -> String {
        self.inner.summarize()
    }

    /// The recent conversation history, oldest first.
    #[getter]
    fn journal(&self) -> Vec<PyAgentMessage> {
        self.inner
            .journal
            .iter()
            .map(|message| {
                PyAgentMessage::from_inner(AgentMessage {
                    provenance: message.provenance.clone(),
                    payload: message.payload.clone(),
                    id: message.id,
                    envelope: message.envelope.clone(),
                    content_type: None,
                    verified_principal: None,
                })
            })
            .collect()
    }

    /// The dead-letter capsule, or `None`.
    #[getter]
    fn dead_letter(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.inner.dead_letter {
            Some(capsule) => ser_to_py(py, capsule),
            None => Ok(py.None()),
        }
    }

    /// The most recent governance decision, or `None`.
    #[getter]
    fn last_decision(&self) -> Option<PyPolicyEvidence> {
        self.inner
            .last_decision
            .clone()
            .map(|inner| PyPolicyEvidence { inner })
    }
}

use crate::async_bridge::{PyHook, future_into_py};
use crate::client::PyLaser;
use crate::errors::{InvalidError, to_pyerr};
use async_trait::async_trait;
use laser_sdk::LaserError;
use laser_sdk::govern::{
    ActionCounters, ActionDecision, ActionGovernor, ActionKind, GovernedAction, GovernorMode,
    GovernorRetention, PolicyEvidence, PolicyRef, QuorumGovernor, QuorumPolicy, SwappableGovernor,
    Verdict,
};
use laser_sdk::types::ConversationId;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::sync::{Arc, RwLock};

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A clone of this `Laser` whose agent sends, typed or raw topic
    /// publishes, AGDX verbs, and memory writes run `governor` (an object with
    /// `async def decide(action) -> ActionDecision`) before the effect, applied
    /// under `mode`. `"enforce"` (the default) applies the verdict, and
    /// `"observe"` is the shadow rollout: everything runs, every decision is
    /// recorded. The connection is shared, the governor's session counters and
    /// evidence chain are fresh. Agents spawned from the governed handle
    /// inherit it.
    #[pyo3(signature = (governor, mode="enforce"))]
    fn with_governor(&self, governor: &Bound<'_, PyAny>, mode: &str) -> PyResult<PyLaser> {
        let mode = parse_mode(mode)?;
        Ok(PyLaser::from_inner(self.inner.with_governor(
            Arc::new(PyActionGovernor::new(governor)?),
            mode,
        )))
    }

    /// `with_governor` with an explicit `GovernorRetention` for the
    /// process-local evidence-chain heads. Eviction or a process restart
    /// starts a new local chain for that conversation.
    fn with_governor_retention(
        &self,
        governor: &Bound<'_, PyAny>,
        mode: &str,
        retention: PyGovernorRetention,
    ) -> PyResult<PyLaser> {
        let mode = parse_mode(mode)?;
        Ok(PyLaser::from_inner(self.inner.with_governor_retention(
            Arc::new(PyActionGovernor::new(governor)?),
            mode,
            retention.inner,
        )))
    }
}

/// Process-local retention for governance digest-chain heads: at most
/// `capacity` conversations, and a head idle for `idle_ttl_secs` may be
/// evicted. An omitted argument keeps the SDK default. Eviction or a process
/// restart starts a new local chain for that conversation.
#[gen_stub_pyclass]
#[pyclass(name = "GovernorRetention", frozen, from_py_object)]
#[derive(Clone, Copy)]
pub struct PyGovernorRetention {
    pub(crate) inner: GovernorRetention,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyGovernorRetention {
    #[new]
    #[pyo3(signature = (*, capacity=None, idle_ttl_secs=None))]
    fn new(capacity: Option<usize>, idle_ttl_secs: Option<f64>) -> PyResult<Self> {
        let mut inner = GovernorRetention::default();
        if let Some(capacity) = capacity {
            inner.capacity = capacity;
        }
        if let Some(idle_ttl_secs) = idle_ttl_secs {
            inner.idle_ttl = crate::convert::duration_seconds(idle_ttl_secs, "idle_ttl_secs")?;
        }
        Ok(Self { inner })
    }

    /// Maximum retained conversation heads.
    #[getter]
    fn capacity(&self) -> usize {
        self.inner.capacity
    }

    /// Inactivity, in seconds, after which an unlocked head may be evicted.
    #[getter]
    fn idle_ttl_secs(&self) -> f64 {
        self.inner.idle_ttl.as_secs_f64()
    }

    fn __repr__(&self) -> String {
        format!(
            "GovernorRetention(capacity={}, idle_ttl_secs={})",
            self.inner.capacity,
            self.inner.idle_ttl.as_secs_f64()
        )
    }
}

// An `ActionGovernor` backed by a Python object exposing `decide(action:
// GovernedAction) -> ActionDecision`, or a plain callable, sync or async. A
// raise or a non-decision return fails the governed action (fail closed),
// mirroring the Rust trait's `Err` contract, so a broken governor never fails
// open.
pub(crate) struct PyActionGovernor {
    hook: PyHook,
}

impl PyActionGovernor {
    pub(crate) fn new(governor: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            hook: PyHook::new(governor, "decide", "a governor")?,
        })
    }
}

#[async_trait]
impl ActionGovernor for PyActionGovernor {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        let snapshot = PyGovernedAction::snapshot(action);
        let value = self
            .hook
            .call(|py| (snapshot,).into_pyobject(py))
            .await
            .map_err(crate::errors::from_callback_error)?;
        Python::attach(|py| -> PyResult<ActionDecision> {
            let decision = value.bind(py).extract::<PyActionDecision>()?;
            Ok(decision.inner)
        })
        .map_err(|error| {
            LaserError::HandlerConfig(format!(
                "governor decide returned a non-ActionDecision: {error}"
            ))
        })
    }
}

/// One side effect about to run, as the governor's `decide` sees it. Advisory
/// fields (`purpose`, `data_classification`) are claims unless the envelope is
/// signed.
#[gen_stub_pyclass]
#[pyclass(name = "GovernedAction", skip_from_py_object)]
#[derive(Clone)]
pub struct PyGovernedAction {
    kind: String,
    stream: String,
    topic: String,
    source: Option<String>,
    target: Option<String>,
    conversation: Option<String>,
    correlation: Option<String>,
    operation: Option<String>,
    tool: Option<String>,
    on_behalf_of: Option<String>,
    purpose: Option<String>,
    data_classification: Option<String>,
    payload: Vec<u8>,
    signed: bool,
    counters: ActionCounters,
}

impl PyGovernedAction {
    fn snapshot(action: &GovernedAction<'_>) -> Self {
        Self {
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
            purpose: action.purpose.map(str::to_owned),
            data_classification: action.data_classification.map(str::to_owned),
            payload: action.payload.to_vec(),
            signed: action.signed,
            counters: action.counters,
        }
    }

    // The reverse of `snapshot`, borrowing from this owned snapshot. Lets a
    // native `QuorumGovernor` re-enter the real `ActionGovernor::decide` after
    // crossing into Python and back, instead of reimplementing the quorum
    // combinator in the binding layer.
    fn to_governed_action(&self) -> Result<GovernedAction<'_>, LaserError> {
        let kind = self.kind.parse::<ActionKind>().map_err(|_| {
            LaserError::HandlerConfig(format!("unknown action kind '{}'", self.kind))
        })?;
        let conversation = self
            .conversation
            .as_deref()
            .map(str::parse::<ConversationId>)
            .transpose()
            .map_err(|error| {
                LaserError::HandlerConfig(format!("invalid conversation id: {error}"))
            })?;
        Ok(GovernedAction {
            kind,
            stream: &self.stream,
            topic: &self.topic,
            source: self.source.as_deref(),
            target: self.target.as_deref(),
            conversation,
            correlation: self.correlation.as_deref(),
            operation: self.operation.as_deref(),
            tool: self.tool.as_deref(),
            on_behalf_of: self.on_behalf_of.as_deref(),
            purpose: self.purpose.as_deref(),
            data_classification: self.data_classification.as_deref(),
            payload: &self.payload,
            signed: self.signed,
            counters: self.counters,
        })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyGovernedAction {
    /// The effect kind (`send` | `publish` | `request` | `command` |
    /// `response` | `event` | `status` | `error` | `memory_write`).
    #[getter]
    fn kind(&self) -> &str {
        &self.kind
    }

    /// The Iggy stream the effect publishes to.
    #[getter]
    fn stream(&self) -> &str {
        &self.stream
    }

    /// The topic the effect publishes to.
    #[getter]
    fn topic(&self) -> &str {
        &self.topic
    }

    /// The acting agent, when the effect carries one.
    #[getter]
    fn source(&self) -> Option<&str> {
        self.source.as_deref()
    }

    /// The addressed agent, when the effect targets one.
    #[getter]
    fn target(&self) -> Option<&str> {
        self.target.as_deref()
    }

    /// The conversation the effect belongs to.
    #[getter]
    fn conversation(&self) -> Option<&str> {
        self.conversation.as_deref()
    }

    /// The reply-correlation key, when the effect carries one.
    #[getter]
    fn correlation(&self) -> Option<&str> {
        self.correlation.as_deref()
    }

    /// The envelope operation name (AGDX path).
    #[getter]
    fn operation(&self) -> Option<&str> {
        self.operation.as_deref()
    }

    /// The tool name (AGDX path).
    #[getter]
    fn tool(&self) -> Option<&str> {
        self.tool.as_deref()
    }

    /// The delegation subject from the envelope metadata.
    #[getter]
    fn on_behalf_of(&self) -> Option<&str> {
        self.on_behalf_of.as_deref()
    }

    /// The declared purpose from the envelope metadata (advisory).
    #[getter]
    fn purpose(&self) -> Option<&str> {
        self.purpose.as_deref()
    }

    /// The declared data classification from the envelope metadata (advisory).
    #[getter]
    fn data_classification(&self) -> Option<&str> {
        self.data_classification.as_deref()
    }

    /// The body about to be published.
    #[getter]
    fn payload<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.payload)
    }

    /// Whether this SDK will sign the record at send.
    #[getter]
    fn signed(&self) -> bool {
        self.signed
    }

    /// Session counters at decision time, for rate and budget policies.
    #[getter]
    fn counters(&self) -> PyActionCounters {
        PyActionCounters {
            inner: self.counters,
        }
    }
}

/// Session counters at decision time. Shared by every clone of the governed
/// `Laser`, so a policy can bound a whole session, not one handle.
#[gen_stub_pyclass]
#[pyclass(name = "ActionCounters", frozen, eq, skip_from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub struct PyActionCounters {
    inner: ActionCounters,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyActionCounters {
    /// Governed non-request effects so far.
    #[getter]
    fn sends(&self) -> u64 {
        self.inner.sends
    }

    /// Governed requests so far.
    #[getter]
    fn requests(&self) -> u64 {
        self.inner.requests
    }

    /// Payload bytes published through governed effects so far.
    #[getter]
    fn bytes_sent(&self) -> u64 {
        self.inner.bytes_sent
    }

    fn __repr__(&self) -> String {
        format!(
            "ActionCounters(sends={}, requests={}, bytes_sent={})",
            self.inner.sends, self.inner.requests, self.inner.bytes_sent
        )
    }
}

/// What a governor decided. Build with the static constructors (`allow`,
/// `observe`, `block`, `step_up`, `modify`, `defer`) and refine with
/// `with_reason` / `with_policy` / `with_risk_score` (each returns a new
/// decision), all recorded in the policy evidence.
#[gen_stub_pyclass]
#[pyclass(name = "ActionDecision", from_py_object)]
#[derive(Clone)]
pub struct PyActionDecision {
    inner: ActionDecision,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyActionDecision {
    /// Run the effect, no evidence.
    #[staticmethod]
    fn allow() -> Self {
        Self {
            inner: ActionDecision::allow(),
        }
    }

    /// Run the effect and record evidence.
    #[staticmethod]
    fn observe() -> Self {
        Self {
            inner: ActionDecision::observe(),
        }
    }

    /// Reject before the effect (`PolicyBlockedError`).
    #[staticmethod]
    fn block(reason: String) -> Self {
        Self {
            inner: ActionDecision::block(reason),
        }
    }

    /// Reject with the scope an approval must grant (`StepUpRequiredError`).
    #[staticmethod]
    fn step_up(scope: String) -> Self {
        Self {
            inner: ActionDecision::step_up(scope),
        }
    }

    /// Replace the body before the effect (applied before claim-check and
    /// signing).
    #[staticmethod]
    fn modify(body: Vec<u8>) -> Self {
        Self {
            inner: ActionDecision::modify(body),
        }
    }

    /// Hold the work for later (`PolicyDeferredError`, retryable).
    #[staticmethod]
    fn defer(reason: String) -> Self {
        Self {
            inner: ActionDecision::defer(reason),
        }
    }

    /// A copy of this decision with the reason recorded in evidence.
    fn with_reason(&self, reason: String) -> Self {
        Self {
            inner: self.inner.clone().with_reason(reason),
        }
    }

    /// A copy of this decision naming the deciding policy pack and rules.
    fn with_policy(&self, policy: PyPolicyRef) -> Self {
        Self {
            inner: self.inner.clone().with_policy(policy.inner),
        }
    }

    /// A copy of this decision carrying the governor's risk estimate.
    fn with_risk_score(&self, risk_score: f64) -> Self {
        Self {
            inner: self.inner.clone().with_risk_score(risk_score),
        }
    }

    /// The verdict to apply.
    #[getter]
    fn verdict(&self) -> PyVerdict {
        PyVerdict {
            inner: self.inner.verdict.clone(),
        }
    }

    /// Why, recorded in evidence.
    #[getter]
    fn reason(&self) -> Option<&str> {
        self.inner.reason.as_deref()
    }

    /// The policy pack and rules that decided, when named.
    #[getter]
    fn policy(&self) -> Option<PyPolicyRef> {
        self.inner.policy.clone().map(|inner| PyPolicyRef { inner })
    }

    /// The governor's risk estimate, recorded in evidence.
    #[getter]
    fn risk_score(&self) -> Option<f64> {
        self.inner.risk_score
    }

    fn __repr__(&self) -> String {
        format!("ActionDecision(verdict={})", self.inner.verdict)
    }
}

/// The decision vocabulary, broader than allow and deny. Build one with the
/// static constructors. `as_str()` is its evidence name (`allow` | `observe`
/// | `block` | `step_up` | `modify` | `defer`), and `scope` and `body` carry
/// the `step_up` and `modify` payloads.
#[gen_stub_pyclass]
#[pyclass(name = "Verdict", frozen, eq, from_py_object)]
#[derive(Clone, PartialEq)]
pub struct PyVerdict {
    inner: Verdict,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyVerdict {
    /// Run the effect.
    #[staticmethod]
    fn allow() -> Self {
        Self {
            inner: Verdict::Allow,
        }
    }

    /// Run the effect and emit evidence.
    #[staticmethod]
    fn observe() -> Self {
        Self {
            inner: Verdict::Observe,
        }
    }

    /// Reject before the effect.
    #[staticmethod]
    fn block() -> Self {
        Self {
            inner: Verdict::Block,
        }
    }

    /// Pause on an approval granting `scope`.
    #[staticmethod]
    fn step_up(scope: String) -> Self {
        Self {
            inner: Verdict::StepUp { scope },
        }
    }

    /// Replace the body, then run the effect.
    #[staticmethod]
    fn modify(body: Vec<u8>) -> Self {
        Self {
            inner: Verdict::Modify { body },
        }
    }

    /// Record that the work is held for later.
    #[staticmethod]
    fn defer() -> Self {
        Self {
            inner: Verdict::Defer,
        }
    }

    /// The pinned evidence name of this verdict.
    fn as_str(&self) -> &'static str {
        self.inner.as_str()
    }

    /// The scope a `step_up` verdict asks an approval to grant.
    #[getter]
    fn scope(&self) -> Option<&str> {
        match &self.inner {
            Verdict::StepUp { scope } => Some(scope),
            _ => None,
        }
    }

    /// The replacement body of a `modify` verdict.
    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        match &self.inner {
            Verdict::Modify { body } => Some(PyBytes::new(py, body)),
            _ => None,
        }
    }

    fn __str__(&self) -> &'static str {
        self.inner.as_str()
    }

    fn __repr__(&self) -> String {
        format!("Verdict({})", self.inner)
    }
}

/// The versioned policy artifact a decision came from, recorded verbatim in
/// evidence. The SDK parses no policy language: a governor maps whatever
/// engine it fronts onto this.
#[gen_stub_pyclass]
#[pyclass(name = "PolicyRef", frozen, eq, from_py_object)]
#[derive(Clone, PartialEq)]
pub struct PyPolicyRef {
    inner: PolicyRef,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyPolicyRef {
    #[new]
    fn new(pack_id: String, pack_version: String, rule_ids: Vec<String>) -> Self {
        Self {
            inner: PolicyRef {
                pack_id,
                pack_version,
                rule_ids,
            },
        }
    }

    /// The policy pack id.
    #[getter]
    fn pack_id(&self) -> &str {
        &self.inner.pack_id
    }

    /// The policy pack version.
    #[getter]
    fn pack_version(&self) -> &str {
        &self.inner.pack_version
    }

    /// The rule ids that matched.
    #[getter]
    fn rule_ids(&self) -> Vec<String> {
        self.inner.rule_ids.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "PolicyRef(pack_id={:?}, pack_version={:?}, rule_ids={:?})",
            self.inner.pack_id, self.inner.pack_version, self.inner.rule_ids
        )
    }
}

/// One governance decision read back off the audit topic: `PolicyEvidence.decode`
/// the body of an AGDX `event` whose operation is `policy_decision`.
#[gen_stub_pyclass]
#[pyclass(name = "PolicyEvidence", from_py_object)]
#[derive(Clone)]
pub struct PyPolicyEvidence {
    pub(crate) inner: PolicyEvidence,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyPolicyEvidence {
    /// Decode an evidence body (named-field CBOR).
    #[staticmethod]
    fn decode(payload: Vec<u8>) -> PyResult<Self> {
        Ok(Self {
            inner: PolicyEvidence::decode(&payload).map_err(to_pyerr)?,
        })
    }

    /// Encode this evidence body (named-field CBOR), the inverse of `decode`.
    fn encode<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let payload = self.inner.encode().map_err(to_pyerr)?;
        Ok(PyBytes::new(py, &payload))
    }

    /// This decision's id (ULID).
    #[getter]
    fn decision_id(&self) -> &str {
        &self.inner.decision_id
    }

    /// The verdict name (`allow` | `observe` | `block` | `step_up` | `modify` | `defer`).
    #[getter]
    fn decision(&self) -> &str {
        &self.inner.decision
    }

    /// The enforcement mode the decision ran under (`observe` | `enforce`).
    #[getter]
    fn mode(&self) -> &str {
        &self.inner.mode
    }

    /// The governed action's kind.
    #[getter]
    fn kind(&self) -> &str {
        &self.inner.kind
    }

    /// The stream the action targeted.
    #[getter]
    fn stream(&self) -> &str {
        &self.inner.stream
    }

    /// The topic the action targeted.
    #[getter]
    fn topic(&self) -> &str {
        &self.inner.topic
    }

    /// The acting agent.
    #[getter]
    fn source(&self) -> Option<&str> {
        self.inner.source.as_deref()
    }

    /// The addressed agent.
    #[getter]
    fn target(&self) -> Option<&str> {
        self.inner.target.as_deref()
    }

    /// The conversation the action belonged to.
    #[getter]
    fn conversation(&self) -> Option<&str> {
        self.inner.conversation.as_deref()
    }

    /// The reply-correlation key.
    #[getter]
    fn correlation(&self) -> Option<&str> {
        self.inner.correlation.as_deref()
    }

    /// The envelope operation name.
    #[getter]
    fn operation(&self) -> Option<&str> {
        self.inner.operation.as_deref()
    }

    /// The tool name.
    #[getter]
    fn tool(&self) -> Option<&str> {
        self.inner.tool.as_deref()
    }

    /// The delegation subject.
    #[getter]
    fn on_behalf_of(&self) -> Option<&str> {
        self.inner.on_behalf_of.as_deref()
    }

    /// The governor's reason.
    #[getter]
    fn reason(&self) -> Option<&str> {
        self.inner.reason.as_deref()
    }

    /// The scope a step-up approval must grant.
    #[getter]
    fn approved_scope(&self) -> Option<&str> {
        self.inner.approved_scope.as_deref()
    }

    /// The policy pack and rules that decided, when named.
    #[getter]
    fn policy(&self) -> Option<PyPolicyRef> {
        self.inner.policy.clone().map(|inner| PyPolicyRef { inner })
    }

    /// The governor's risk estimate.
    #[getter]
    fn risk_score(&self) -> Option<f64> {
        self.inner.risk_score
    }

    /// BLAKE3 (hex) of this record's canonical encoding, digest field empty.
    #[getter]
    fn receipt_digest(&self) -> &str {
        &self.inner.receipt_digest
    }

    /// The prior decision's `receipt_digest` in this conversation.
    #[getter]
    fn previous_digest(&self) -> Option<&str> {
        self.inner.previous_digest.as_deref()
    }

    /// What happened to the effect (`effected` | `blocked` | `step_up` | `deferred`).
    #[getter]
    fn outcome(&self) -> &str {
        &self.inner.outcome
    }

    /// Decision time, epoch micros.
    #[getter]
    fn at_micros(&self) -> u64 {
        self.inner.at_micros
    }
}

/// Whether `evidence`, in log order for one conversation, is an unbroken
/// chain: every record reproduces its own `receipt_digest` and names the
/// previous record's digest as its `previous_digest`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn verify_evidence_chain(evidence: Vec<PyPolicyEvidence>) -> bool {
    let evidence: Vec<PolicyEvidence> = evidence.into_iter().map(|item| item.inner).collect();
    laser_sdk::govern::verify_evidence_chain(&evidence)
}

/// How a `QuorumGovernor` combines its voters' verdicts into one decision.
/// Only `allow`, `observe`, and `modify` count as affirmative (the action
/// would proceed under that voter alone). `block`, `step_up`, and `defer` do
/// not, regardless of policy.
#[gen_stub_pyclass]
#[pyclass(name = "QuorumPolicy", from_py_object)]
#[derive(Clone, Copy)]
pub struct PyQuorumPolicy {
    inner: QuorumPolicy,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQuorumPolicy {
    /// Every voter must be affirmative.
    #[staticmethod]
    fn all() -> Self {
        Self {
            inner: QuorumPolicy::All,
        }
    }

    /// At least one voter must be affirmative.
    #[staticmethod]
    fn any() -> Self {
        Self {
            inner: QuorumPolicy::Any,
        }
    }

    /// At least `n` distinct voters must be affirmative.
    #[staticmethod]
    fn at_least(n: usize) -> Self {
        Self {
            inner: QuorumPolicy::AtLeast(n),
        }
    }
}

/// A governor that composes independent voters under a `QuorumPolicy`, itself
/// usable anywhere a governor is (`Laser.with_governor`, or nested as a voter
/// in another `QuorumGovernor`): it implements the same `async def
/// decide(action) -> ActionDecision` contract as a hand-written governor.
/// Every voter runs concurrently over the same action.
///
/// Every `mandatory` voter must be affirmative before the quorum can pass. A
/// denial or error cannot be bypassed by another voter. When the quorum is met,
/// the composite verdict is the strongest
/// affirmative found (`modify` over `observe` over `allow`). When it is not
/// met, the composite is the most actionable denial found (`block` over
/// `step_up` over `defer`). A non-mandatory error abstains. Empty, duplicate,
/// invalid-threshold, and conflicting-modification configurations block. Pure
/// in-process composition, no durable log or protocol of its own.
#[gen_stub_pyclass]
#[pyclass(name = "QuorumGovernor")]
pub struct PyQuorumGovernor {
    // `Option` only to move the inner value through the consuming Rust
    // builder API (`QuorumGovernor::voter`) across a `&mut self` Python call.
    // Always `Some` outside the brief window inside `voter` itself.
    inner: Option<QuorumGovernor>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyQuorumGovernor {
    #[new]
    fn new(policy: PyQuorumPolicy) -> Self {
        Self {
            inner: Some(QuorumGovernor::new(policy.inner)),
        }
    }

    /// Enroll one named voter (an object with `async def decide(action) ->
    /// ActionDecision`, the same contract `Laser.with_governor` takes). A
    /// `mandatory` voter must be affirmative, regardless of policy.
    fn voter(
        &mut self,
        name: String,
        governor: &Bound<'_, PyAny>,
        mandatory: bool,
    ) -> PyResult<()> {
        let governor = Arc::new(PyActionGovernor::new(governor)?);
        // The builder is consumed and put back. If a previous call unwound
        // between the two, the slot stays empty, so report that as a typed
        // error rather than panicking on every later call.
        let current = self.inner.take().ok_or_else(|| {
            InvalidError::new_err("this QuorumGovernor is unusable: a previous voter() call failed")
        })?;
        self.inner = Some(current.voter(name, governor, mandatory));
        Ok(())
    }

    /// Decide `action` by fanning out to every voter concurrently and folding
    /// their verdicts under this governor's policy. Reuses the real
    /// `ActionGovernor` combinator rather than reimplementing it here, so a
    /// `QuorumGovernor` built in Python and one built in Rust always agree.
    fn decide<'py>(
        &self,
        py: Python<'py>,
        action: &PyGovernedAction,
    ) -> PyResult<Bound<'py, PyAny>> {
        let governor = self
            .inner
            .clone()
            .expect("QuorumGovernor always holds a value between calls");
        let action = action.clone();
        future_into_py(py, async move {
            let governed = action.to_governed_action().map_err(to_pyerr)?;
            governor
                .decide(&governed)
                .await
                .map(|inner| PyActionDecision { inner })
                .map_err(to_pyerr)
        })
    }
}

/// A governor whose active policy can be hot-swapped at runtime without
/// dropping clones already enrolled via `Laser.with_governor` or restarting
/// the process. `swap` can be driven by anything: an operator call, a config
/// reload, or a caller folding a policy-update topic and swapping in the
/// governor that matches the latest fact. A swap only changes which policy
/// the *next* `decide` call runs under: it never reinterprets a
/// `PolicyEvidence` record already on the log.
#[gen_stub_pyclass]
#[pyclass(name = "SwappableGovernor")]
pub struct PySwappableGovernor {
    inner: Arc<SwappableGovernor>,
    active: RwLock<Py<PyAny>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySwappableGovernor {
    /// A swappable governor starting from `initial` (an object with
    /// `async def decide(action) -> ActionDecision`).
    #[new]
    fn new(initial: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            inner: Arc::new(SwappableGovernor::new(Arc::new(PyActionGovernor::new(
                initial,
            )?))),
            active: RwLock::new(initial.clone().unbind()),
        })
    }

    /// Replace the active policy with `next` and return the previous one.
    /// A `decide` already in flight finishes under whichever policy it read.
    fn swap(&self, next: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.inner.swap(Arc::new(PyActionGovernor::new(next)?));
        let mut active = self
            .active
            .write()
            .expect("Python governor lock is never poisoned");
        Ok(std::mem::replace(&mut *active, next.clone().unbind()))
    }

    /// The currently active Python policy object.
    fn current(&self, py: Python<'_>) -> Py<PyAny> {
        self.active
            .read()
            .expect("Python governor lock is never poisoned")
            .clone_ref(py)
    }

    /// Decide `action` under the currently active policy. Reuses the real
    /// Rust governor rather than reimplementing the swap in the binding
    /// layer, so a Python-driven swap and a Rust-driven one always agree.
    fn decide<'py>(
        &self,
        py: Python<'py>,
        action: &PyGovernedAction,
    ) -> PyResult<Bound<'py, PyAny>> {
        let governor = Arc::clone(&self.inner);
        let action = action.clone();
        future_into_py(py, async move {
            let governed = action.to_governed_action().map_err(to_pyerr)?;
            governor
                .decide(&governed)
                .await
                .map(|inner| PyActionDecision { inner })
                .map_err(to_pyerr)
        })
    }
}

pub(crate) fn parse_mode(mode: &str) -> PyResult<GovernorMode> {
    mode.parse().map_err(|_| {
        crate::errors::InvalidError::new_err(format!(
            "governor mode must be \"observe\" or \"enforce\", got \"{mode}\""
        ))
    })
}

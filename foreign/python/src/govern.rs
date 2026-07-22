use crate::client::PyLaser;
use crate::errors::to_pyerr;
use async_trait::async_trait;
use laser_sdk::LaserError;
use laser_sdk::govern::{
    ActionCounters, ActionDecision, ActionGovernor, ActionKind, GovernedAction, GovernorMode,
    PolicyEvidence, PolicyRef, QuorumGovernor, QuorumPolicy, SwappableGovernor,
};
use laser_sdk::types::ConversationId;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_async_runtimes::tokio::{future_into_py, into_future};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
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
    fn with_governor(&self, governor: Py<PyAny>, mode: &str) -> PyResult<PyLaser> {
        let mode = parse_mode(mode)?;
        Ok(PyLaser::from_inner(self.inner.with_governor(
            Arc::new(PyActionGovernor { hooks: governor }),
            mode,
        )))
    }
}

// An `ActionGovernor` backed by a Python object exposing `async def
// decide(action: GovernedAction) -> ActionDecision`. A raise or a non-decision
// return fails the governed action (fail closed), mirroring the Rust trait's
// `Err` contract, so a broken governor never fails open.
pub(crate) struct PyActionGovernor {
    pub(crate) hooks: Py<PyAny>,
}

#[async_trait]
impl ActionGovernor for PyActionGovernor {
    async fn decide(&self, action: &GovernedAction<'_>) -> Result<ActionDecision, LaserError> {
        let snapshot = PyGovernedAction::snapshot(action);
        let future = Python::attach(|py| -> PyResult<_> {
            let coroutine = self.hooks.bind(py).call_method1("decide", (snapshot,))?;
            into_future(coroutine)
        })
        .map_err(|error| LaserError::HandlerConfig(format!("governor decide: {error}")))?;
        let value = future.await.map_err(|error| {
            LaserError::HandlerConfig(format!("governor decide raised: {error}"))
        })?;
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
    sends: u64,
    requests: u64,
    bytes_sent: u64,
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
            sends: action.counters.sends,
            requests: action.counters.requests,
            bytes_sent: action.counters.bytes_sent,
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
            counters: ActionCounters {
                sends: self.sends,
                requests: self.requests,
                bytes_sent: self.bytes_sent,
            },
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

    /// Governed non-request effects so far this session.
    #[getter]
    fn sends(&self) -> u64 {
        self.sends
    }

    /// Governed requests so far this session.
    #[getter]
    fn requests(&self) -> u64 {
        self.requests
    }

    /// Payload bytes published through governed effects so far this session.
    #[getter]
    fn bytes_sent(&self) -> u64 {
        self.bytes_sent
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
    fn with_policy(&self, pack_id: String, pack_version: String, rule_ids: Vec<String>) -> Self {
        Self {
            inner: self.inner.clone().with_policy(PolicyRef {
                pack_id,
                pack_version,
                rule_ids,
            }),
        }
    }

    /// A copy of this decision carrying the governor's risk estimate.
    fn with_risk_score(&self, risk_score: f64) -> Self {
        Self {
            inner: self.inner.clone().with_risk_score(risk_score),
        }
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

    /// The deciding policy as `(pack_id, pack_version, rule_ids)`, when named.
    #[getter]
    fn policy(&self) -> Option<(String, String, Vec<String>)> {
        self.inner.policy.as_ref().map(|policy| {
            (
                policy.pack_id.clone(),
                policy.pack_version.clone(),
                policy.rule_ids.clone(),
            )
        })
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
    fn voter(&mut self, name: String, governor: Py<PyAny>, mandatory: bool) {
        let current = self
            .inner
            .take()
            .expect("QuorumGovernor always holds a value between calls");
        self.inner = Some(current.voter(
            name,
            Arc::new(PyActionGovernor { hooks: governor }),
            mandatory,
        ));
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
    /// A swappable governor starting from `governor` (an object with
    /// `async def decide(action) -> ActionDecision`).
    #[new]
    fn new(py: Python<'_>, governor: Py<PyAny>) -> Self {
        Self {
            inner: Arc::new(SwappableGovernor::new(Arc::new(PyActionGovernor {
                hooks: governor.clone_ref(py),
            }))),
            active: RwLock::new(governor),
        }
    }

    /// Replace the active policy with `governor` and return the previous one.
    /// A `decide` already in flight finishes under whichever policy it read.
    fn swap(&self, py: Python<'_>, governor: Py<PyAny>) -> Py<PyAny> {
        self.inner.swap(Arc::new(PyActionGovernor {
            hooks: governor.clone_ref(py),
        }));
        let mut active = self
            .active
            .write()
            .expect("Python governor lock is never poisoned");
        std::mem::replace(&mut *active, governor)
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

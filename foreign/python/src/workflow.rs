use crate::agent_runtime::{route_policy, static_topic, take_route_failure};
use crate::async_bridge::{Awaitable, HookLoop, awaitable, call_hook, hook_callable};
use crate::convert::payload_bytes;
use crate::errors::to_pyerr;
use laser_sdk::agent::{
    InboxRoute, OnTimeout, Router, StepContext, StepFn, Verifier, Workflow, WorkflowBudget,
};
use laser_sdk::laser::Laser;
use laser_sdk::types::{AgentId, ConversationId, PrincipalId};
use laser_sdk::wire::agent::SessionStatus;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

// Workflow callbacks are callables or objects with the named method, and can
// return directly or through an awaitable. Dropping a running workflow cancels
// any active Python callback task.
struct PyStepFn(Arc<Py<PyAny>>);

impl PyStepFn {
    async fn call(&self, ctx: &StepContext<'_>) -> PyResult<Vec<u8>> {
        let value = call_hook(&HookLoop::default(), |call| {
            let py = call.py();
            let build = hook_callable(self.0.bind(py), "build", "a workflow step builder")?;
            let outputs = PyDict::new(py);
            for (label, output) in ctx.outputs {
                outputs.set_item(label, PyBytes::new(py, output))?;
            }
            call.call(&build, (outputs,))
        })
        .await?;
        Python::attach(|py| payload_bytes(value.bind(py))).map_err(|error| {
            crate::errors::HandlerConfigError::new_err(format!(
                "a workflow step must return bytes or str: {error}"
            ))
        })
    }
}

#[async_trait::async_trait]
impl StepFn for PyStepFn {
    async fn build(&self, ctx: &StepContext<'_>) -> Result<Vec<u8>, laser_sdk::LaserError> {
        self.call(ctx)
            .await
            .map_err(crate::errors::from_callback_error)
    }
}

struct PyVerifier {
    callback: Arc<Py<PyAny>>,
    raised: Arc<RaisedError>,
}

#[async_trait::async_trait]
impl Verifier for PyVerifier {
    async fn verify(&self, output: &[u8]) -> Result<bool, laser_sdk::LaserError> {
        let value = call_hook(&HookLoop::default(), |call| {
            let py = call.py();
            let verify = hook_callable(self.callback.bind(py), "verify", "a workflow verifier")?;
            call.call(&verify, (PyBytes::new(py, output),))
        })
        .await
        .map_err(|error| {
            self.raised.keep(&error);
            crate::errors::from_callback_error(error)
        })?;
        Python::attach(|py| value.bind(py).extract::<bool>()).map_err(|error| {
            laser_sdk::LaserError::HandlerConfig(format!(
                "workflow verifier must return bool: {error}"
            ))
        })
    }
}

// Identify the forward step that failed routing. A scorer error during an
// ignored compensation must not replace that step's original error.
struct TrackedStepFn {
    callback: PyStepFn,
    index: usize,
    active: Arc<AtomicUsize>,
    raised: Arc<RaisedError>,
}

#[async_trait::async_trait]
impl StepFn for TrackedStepFn {
    async fn build(&self, context: &StepContext<'_>) -> Result<Vec<u8>, laser_sdk::LaserError> {
        self.active.store(self.index, Ordering::Release);
        self.callback.call(context).await.map_err(|error| {
            self.raised.keep(&error);
            crate::errors::from_callback_error(error)
        })
    }
}

// The first exception a forward step body or a verifier raises, so the run
// raises it unchanged rather than as a `HandlerError`.
#[derive(Default)]
struct RaisedError(std::sync::Mutex<Option<PyErr>>);

impl RaisedError {
    fn keep(&self, error: &PyErr) {
        let mut raised = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if raised.is_none() {
            *raised = Some(Python::attach(|py| error.clone_ref(py)));
        }
    }

    fn take(&self) -> Option<PyErr> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }
}

// One step's declaration, collected by `step` and replayed into the Rust builder
// at `run`. Cloneable so `run` can take a snapshot without consuming the builder.
#[derive(Clone)]
struct StepSpec {
    label: String,
    // `None` runs the step body in process.
    target: Option<Router>,
    policy: Option<Arc<Py<PyAny>>>,
    after: Vec<String>,
    exclusive: bool,
    fence_namespace: Option<String>,
    on_timeout: OnTimeout,
    deadline: Option<Duration>,
    build: Arc<Py<PyAny>>,
    verify: Option<Arc<Py<PyAny>>>,
    compensate: Option<Arc<Py<PyAny>>>,
}

/// A workflow's spend ceiling: tokens summed across step replies, the
/// wall-clock time of the whole run in milliseconds, and the number of step
/// dispatches. A dimension left unset is unbounded. Build it with
/// `WorkflowBudget.unlimited()` or `WorkflowBudget.tokens(n)`, then chain
/// `wall_clock(ms)` and `invocations(n)`, each returning a new budget.
#[gen_stub_pyclass]
#[pyclass(name = "WorkflowBudget", frozen, eq, from_py_object)]
#[derive(Clone, Copy, PartialEq, Default)]
pub struct PyWorkflowBudget {
    tokens: Option<u64>,
    wall_clock: Option<Duration>,
    invocations: Option<u32>,
}

impl PyWorkflowBudget {
    fn to_rust(self) -> WorkflowBudget {
        let mut budget = match self.tokens {
            Some(tokens) => WorkflowBudget::tokens(tokens),
            None => WorkflowBudget::unlimited(),
        };
        if let Some(wall_clock) = self.wall_clock {
            budget = budget.wall_clock(wall_clock);
        }
        if let Some(invocations) = self.invocations {
            budget = budget.invocations(invocations);
        }
        budget
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyWorkflowBudget {
    /// An unbounded budget (the default).
    #[staticmethod]
    fn unlimited() -> Self {
        Self::default()
    }

    /// A budget capping the summed input-plus-output tokens across step
    /// replies.
    #[staticmethod]
    #[pyo3(name = "tokens")]
    fn with_tokens(tokens: u64) -> Self {
        Self {
            tokens: Some(tokens),
            ..Self::default()
        }
    }

    /// This budget with the whole run capped at `wall_clock_ms` milliseconds.
    fn wall_clock(&self, wall_clock_ms: f64) -> PyResult<Self> {
        Ok(Self {
            wall_clock: Some(crate::convert::duration_ms(wall_clock_ms, "wall_clock_ms")?),
            ..*self
        })
    }

    /// This budget with at most `invocations` step dispatches.
    fn invocations(&self, invocations: u32) -> Self {
        Self {
            invocations: Some(invocations),
            ..*self
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkflowBudget(tokens={:?}, wall_clock_ms={:?}, invocations={:?})",
            self.tokens,
            self.wall_clock.map(|limit| limit.as_secs_f64() * 1000.0),
            self.invocations
        )
    }
}

/// A journalled directed-acyclic workflow over the coordination primitives, the
/// Python view of the Rust engine. Declare steps with [`step`](Self::step), set a
/// [`budget`](Self::budget), then `await wf.run()`. Each step is a
/// directed task to its target, ordered by its declared dependencies, with an
/// optional verifier panel, exclusivity (a fenced at-most-once effect), an
/// on-timeout policy, and a compensation (the saga rollback).
#[gen_stub_pyclass]
#[pyclass(name = "Workflow")]
pub struct PyWorkflow {
    laser: Laser,
    name: String,
    budget: WorkflowBudget,
    fixed_inbox: Option<String>,
    run_id: Option<ConversationId>,
    steps: Vec<StepSpec>,
}

impl PyWorkflow {
    pub(crate) fn new(laser: Laser, name: String, fixed_inbox: Option<String>) -> Self {
        Self {
            laser,
            name,
            budget: WorkflowBudget::unlimited(),
            fixed_inbox,
            run_id: None,
            steps: Vec::new(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyWorkflow {
    /// Run under `run_id`: the engine replays that run's whole journal, skips
    /// the steps recorded complete, and dispatches only the unfinished ones. A
    /// step dispatched before the run stopped is answered by its reply if one
    /// landed, and sent again under the same idempotency key otherwise. A
    /// journal that retention shortened past the run's first record raises
    /// `JournalTruncatedError`. The run session reopens, so a run that ended
    /// completed or failed runs again under the same id and a canceled one
    /// raises `SessionEndedError`. Omit it to start a fresh run with its own id.
    /// Returns the workflow, so calls chain.
    fn run_id(mut slf: PyRefMut<'_, Self>, run_id: String) -> PyResult<PyRefMut<'_, Self>> {
        let run_id = run_id
            .parse::<ConversationId>()
            .map_err(|error| to_pyerr(error.into()))?;
        slf.run_id = Some(run_id);
        Ok(slf)
    }

    /// Cap the workflow's spend with a `WorkflowBudget`. The token ceiling
    /// counts only the usage an AGDX reply carries, so it is advisory.
    /// Returns the workflow, so calls chain.
    fn budget(mut slf: PyRefMut<'_, Self>, budget: PyWorkflowBudget) -> PyRefMut<'_, Self> {
        slf.budget = budget.to_rust();
        slf
    }

    /// Add a step and return the workflow, so calls chain. A local step passes
    /// `run(outputs) -> bytes | str`, which runs in this process and whose
    /// result is the step output, so it needs no agent. A routed step passes
    /// `build` and exactly one target: `to` (a named agent),
    /// `to_capable` (one agent advertising a skill), or `all_capable` (scatter to
    /// every agent advertising a skill and fold the replies, a verifier panel).
    /// `build(outputs) -> bytes | str` forms the task from the prior outputs. Build, verify, and compensate callbacks can return directly or through an awaitable. Objects with `build` or `verify` methods also work. Callback exceptions retain their SDK error class.
    /// `after`
    /// declares the dependencies that order the step. `verify(output) -> bool`
    /// gates completion. `exclusive` claims a fenced lease (needs the managed
    /// backend). `fence_namespace` also makes the step exclusive and aligns the
    /// lease with a handler's fenced KV effect. `on_timeout` is `"fail"`
    /// (default) or `"reassign"` (re-acquire the
    /// lease, bumping the fence, and hand the task to a fresh holder, a capable
    /// agent other than the ones that timed out when one exists. Needs an
    /// exclusive step). `deadline_ms` bounds the step's build, dispatch,
    /// verification, and completion record (default 30000), never past the
    /// wall-clock budget left, and rides the command so a target that picks it
    /// up later drops it. `compensate(outputs) -> bytes | str` is the rollback
    /// for a later failure. A routed step sends it to the agent that ran the
    /// step. A local step runs it in process and ignores its result.
    #[pyo3(signature = (
        label, *, build=None, run=None, to=None, to_capable=None, all_capable=None, principal=None,
        after=None, verify=None, exclusive=false, fence_namespace=None, on_timeout="fail",
        compensate=None, policy=None, deadline_ms=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn step<'py>(
        mut slf: PyRefMut<'py, Self>,
        label: String,
        #[gen_stub(override_type(type_repr = "typing.Optional[typing.Callable[[dict[builtins.str, builtins.bytes]], builtins.bytes | builtins.str | typing.Awaitable[builtins.bytes | builtins.str]]]", imports = ("builtins", "typing")))]
        build: Option<Py<PyAny>>,
        #[gen_stub(override_type(type_repr = "typing.Optional[typing.Callable[[dict[builtins.str, builtins.bytes]], builtins.bytes | builtins.str | typing.Awaitable[builtins.bytes | builtins.str]]]", imports = ("builtins", "typing")))]
        run: Option<Py<PyAny>>,
        to: Option<String>,
        to_capable: Option<String>,
        all_capable: Option<String>,
        principal: Option<u32>,
        after: Option<Vec<String>>,
        #[gen_stub(override_type(type_repr = "typing.Optional[typing.Callable[[builtins.bytes], builtins.bool | typing.Awaitable[builtins.bool]]]", imports = ("builtins", "typing")))]
        verify: Option<Py<PyAny>>,
        exclusive: bool,
        fence_namespace: Option<String>,
        #[gen_stub(override_type(type_repr = "typing.Literal['fail', 'reassign']", imports = ("typing",)))]
        on_timeout: &str,
        #[gen_stub(override_type(type_repr = "typing.Optional[typing.Callable[[dict[builtins.str, builtins.bytes]], builtins.bytes | builtins.str | typing.Awaitable[builtins.bytes | builtins.str]]]", imports = ("builtins", "typing")))]
        compensate: Option<Py<PyAny>>,
        policy: Option<&Bound<'_, PyAny>>,
        deadline_ms: Option<f64>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        let on_timeout = match on_timeout {
            "fail" => OnTimeout::Fail,
            "reassign" => OnTimeout::Reassign,
            other => {
                return Err(crate::errors::InvalidError::new_err(format!(
                    "on_timeout must be `fail` or `reassign`, got `{other}`"
                )));
            }
        };
        let deadline = deadline_ms
            .map(|deadline| crate::convert::duration_ms(deadline, "deadline_ms"))
            .transpose()?;
        if let Some(run) = run {
            let routed = build.is_some()
                || to.is_some()
                || to_capable.is_some()
                || all_capable.is_some()
                || principal.is_some()
                || policy.is_some();
            if routed {
                return Err(crate::errors::InvalidError::new_err(
                    "a local step takes run= alone: build= and a target route a step to an agent",
                ));
            }
            if exclusive || fence_namespace.is_some() || on_timeout == OnTimeout::Reassign {
                return Err(crate::errors::InvalidError::new_err(
                    "a local step runs in process, so it cannot be exclusive",
                ));
            }
            slf.steps.push(StepSpec {
                label,
                target: None,
                policy: None,
                after: after.unwrap_or_default(),
                exclusive: false,
                fence_namespace: None,
                on_timeout,
                deadline,
                build: Arc::new(run),
                verify: verify.map(Arc::new),
                compensate: compensate.map(Arc::new),
            });
            return Ok(slf);
        }
        let Some(build) = build else {
            return Err(crate::errors::InvalidError::new_err(
                "a step needs run= for a local step, or build= with one of to / to_capable / all_capable",
            ));
        };
        let parsed = route_policy(policy)?;
        let saved_policy = policy.map(|policy| Arc::new(policy.clone().unbind()));
        let policy = parsed.policy;
        let target = match (to, to_capable, all_capable, principal) {
            (Some(agent), None, None, Some(principal)) => Router::to_principal(
                AgentId::new(agent).map_err(|e| to_pyerr(e.into()))?,
                PrincipalId::new(principal),
            ),
            (Some(agent), None, None, None) => {
                Router::to(AgentId::new(agent).map_err(|e| to_pyerr(e.into()))?)
            }
            (None, Some(skill), None, principal) => {
                let mut selector = laser_sdk::agent::CapabilitySelector::new(skill, policy);
                if let Some(principal) = principal {
                    selector = selector.principal(PrincipalId::new(principal));
                }
                Router::ToCapable(selector)
            }
            (None, None, Some(skill), principal) => {
                let mut selector = laser_sdk::agent::CapabilitySelector::new(skill, policy);
                if let Some(principal) = principal {
                    selector = selector.principal(PrincipalId::new(principal));
                }
                Router::AllCapable(selector)
            }
            _ => {
                return Err(crate::errors::InvalidError::new_err(
                    "a step needs exactly one of to / to_capable / all_capable",
                ));
            }
        };
        slf.steps.push(StepSpec {
            label,
            target: Some(target),
            policy: saved_policy,
            after: after.unwrap_or_default(),
            exclusive: exclusive || fence_namespace.is_some(),
            fence_namespace,
            on_timeout,
            deadline,
            build: Arc::new(build),
            verify: verify.map(Arc::new),
            compensate: compensate.map(Arc::new),
        });
        Ok(slf)
    }

    /// Run the workflow, returning a `WorkflowOutcome` with the completed steps'
    /// outputs keyed by label and the run id.
    /// The workflow name is the orchestrator identity it dispatches as, so it must
    /// be a valid agent id. A run on a stream without the agent topics creates
    /// them with the `bootstrap` defaults first. A failed step runs the
    /// compensations in reverse and raises `StepFailedError`, whose `reason`
    /// says how the step ended. An exception from a step body or a verifier is
    /// raised unchanged after the compensations. A misconfigured workflow (a
    /// duplicate label, a broadcast step, an exclusive all-capable step,
    /// reassignment without exclusivity) raises before anything is written.
    /// The run is a session whose id is the run id, and
    /// each step and compensation is a child session of it. A cancel request on
    /// `agent.control` stops the run at the next step boundary with
    /// `CancelledError` after the compensations. Cancelling the Python task
    /// stops the run at once without compensating and releases a held
    /// exclusive lease, so the run resumes by its id.
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyWorkflowOutcome>> {
        let laser = self.laser.clone();
        let name = self.name.clone();
        let budget = self.budget;
        let route = self
            .fixed_inbox
            .clone()
            .map(|topic| static_topic(topic).map(InboxRoute::Fixed))
            .transpose()?;
        let specs = self.steps.clone();
        let run_id = self.run_id;
        awaitable(py, async move {
            let mut workflow = laser.workflow(&name).budget(budget);
            if let Some(route) = route {
                workflow = workflow.inbox_route(route);
            }
            if let Some(run_id) = run_id {
                workflow = workflow.run_id(run_id);
            }
            // Thread the move-based Rust builder: the first step turns the workflow
            // into a step handle, each later step chains onto the handle.
            enum Builder {
                Fresh(Workflow),
                Step(laser_sdk::agent::StepHandle),
            }
            let mut builder = Builder::Fresh(workflow);
            let mut route_failures = Vec::new();
            let active_step = Arc::new(AtomicUsize::new(usize::MAX));
            let raised = Arc::new(RaisedError::default());
            for (index, mut spec) in specs.into_iter().enumerate() {
                let parsed = Python::attach(|py| {
                    route_policy(spec.policy.as_ref().map(|policy| policy.bind(py)))
                })?;
                route_failures.push(parsed.failure);
                if let Some(Router::ToCapable(selector) | Router::AllCapable(selector)) =
                    &mut spec.target
                {
                    selector.policy = parsed.policy;
                }
                let build = TrackedStepFn {
                    callback: PyStepFn(spec.build),
                    index,
                    active: Arc::clone(&active_step),
                    raised: Arc::clone(&raised),
                };
                let mut handle = match (builder, spec.target) {
                    (Builder::Fresh(workflow), Some(target)) => {
                        workflow.step(&spec.label, target, build)
                    }
                    (Builder::Fresh(workflow), None) => workflow.step_local(&spec.label, build),
                    (Builder::Step(handle), Some(target)) => {
                        handle.step(&spec.label, target, build)
                    }
                    (Builder::Step(handle), None) => handle.step_local(&spec.label, build),
                };
                for dependency in &spec.after {
                    handle = handle.after(dependency);
                }
                if let Some(verify) = spec.verify {
                    handle = handle.verify_with(PyVerifier {
                        callback: verify,
                        raised: Arc::clone(&raised),
                    });
                }
                if let Some(namespace) = spec.fence_namespace {
                    handle = handle.exclusive_in(namespace);
                } else if spec.exclusive {
                    handle = handle.exclusive();
                }
                handle = handle.on_timeout(spec.on_timeout);
                if let Some(deadline) = spec.deadline {
                    handle = handle.deadline(deadline);
                }
                if let Some(compensate) = spec.compensate {
                    handle = handle.compensate_with(PyStepFn(compensate));
                }
                builder = Builder::Step(handle);
            }
            let outcome = match builder {
                Builder::Fresh(workflow) => workflow.run().await,
                Builder::Step(handle) => handle.run().await,
            };
            if matches!(&outcome, Err(laser_sdk::LaserError::NoCapableAgent { .. }))
                && let Some(failure) = route_failures.get(active_step.load(Ordering::Acquire))
                && let Some(error) = take_route_failure(failure)
            {
                return Err(error);
            }
            if matches!(&outcome, Err(laser_sdk::LaserError::Handler(_)))
                && let Some(error) = raised.take()
            {
                return Err(error);
            }
            let outcome = outcome.map_err(to_pyerr)?;
            Ok(PyWorkflowOutcome {
                outputs: outcome.outputs,
                run_id: outcome.run_id.to_string(),
                completed_steps: outcome.completed_steps,
                status: outcome.status,
            })
        })
    }
}

/// The result of a completed `Workflow.run`: each step's output by label, the
/// run id, the completed step labels in completion order, and the run's end
/// status, `completed` for every returned outcome since a failed run raises.
/// Pass the run id to `Workflow.run_id` to resume the same run.
#[gen_stub_pyclass]
#[pyclass(name = "WorkflowOutcome", frozen)]
pub struct PyWorkflowOutcome {
    outputs: BTreeMap<String, Vec<u8>>,
    run_id: String,
    completed_steps: Vec<String>,
    status: SessionStatus,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyWorkflowOutcome {
    /// Each completed step's output, keyed by step label.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "dict[builtins.str, builtins.bytes]", imports = ("builtins",)))]
    fn outputs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let outputs = PyDict::new(py);
        for (label, output) in &self.outputs {
            outputs.set_item(label, PyBytes::new(py, output))?;
        }
        Ok(outputs)
    }

    /// The run id, which resumes the same run when passed to `Workflow.run_id`.
    #[getter]
    fn run_id(&self) -> String {
        self.run_id.clone()
    }

    /// The completed step labels, in completion order.
    #[getter]
    fn completed_steps(&self) -> Vec<String> {
        self.completed_steps.clone()
    }

    /// The run's end status word, `completed` for a returned outcome.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.str", imports = ("builtins",)))]
    fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(pythonize::pythonize(py, &self.status)?)
    }

    /// The output of step `label` as text (invalid UTF-8 replaced), or `None`
    /// when no completed step has that label.
    fn text(&self, label: &str) -> Option<String> {
        self.outputs
            .get(label)
            .map(|output| String::from_utf8_lossy(output).into_owned())
    }

    fn __repr__(&self) -> String {
        format!(
            "WorkflowOutcome(run_id={}, steps={})",
            self.run_id,
            self.outputs.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{PyStepFn, PyVerifier};
    use laser_sdk::LaserError;
    use laser_sdk::agent::{StepContext, StepFn, Verifier};
    use pyo3::prelude::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn callbacks<'py>(py: Python<'py>, name: &std::ffi::CStr) -> PyResult<Bound<'py, PyModule>> {
        let module = PyModule::from_code(
            py,
            c"import asyncio
async def async_build(outputs):
    await asyncio.sleep(0)
    return outputs['seed'] + b':built'
def sync_build(outputs):
    return outputs['seed'] + b':built'
async def async_verify(output):
    await asyncio.sleep(0)
    return output == b'seed:built'
def sync_verify(output):
    return output == b'seed:built'
async def refused(value):
    await asyncio.sleep(0)
    raise InvalidError('callback refused')
def invalid_build(outputs):
    return 7
def invalid_verify(output):
    return 'yes'
started = False
cancelled = False
async def pending_build(outputs):
    global started, cancelled
    started = True
    try:
        await asyncio.Future()
    finally:
        cancelled = True
",
            c"workflow_callbacks.py",
            name,
        )?;
        module
            .dict()
            .set_item("InvalidError", py.get_type::<crate::errors::InvalidError>())?;
        Ok(module)
    }

    #[test]
    fn given_sync_and_async_workflow_callbacks_when_called_then_should_resolve_both() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let module = callbacks(py, c"workflow_callbacks_sync_async")?;
            let builds = ["sync_build", "async_build"]
                .into_iter()
                .map(|name| {
                    module
                        .getattr(name)
                        .map(|callback| PyStepFn(Arc::new(callback.unbind())))
                })
                .collect::<PyResult<Vec<_>>>()?;
            let verifiers = ["sync_verify", "async_verify"]
                .into_iter()
                .map(|name| {
                    module.getattr(name).map(|callback| PyVerifier {
                        callback: Arc::new(callback.unbind()),
                        raised: Arc::default(),
                    })
                })
                .collect::<PyResult<Vec<_>>>()?;
            pyo3_async_runtimes::tokio::run(py, async move {
                let outputs = BTreeMap::from([("seed".to_owned(), b"seed".to_vec())]);
                for build in builds {
                    assert_eq!(
                        build
                            .build(&StepContext { outputs: &outputs })
                            .await
                            .expect("payload"),
                        b"seed:built"
                    );
                }
                for verifier in verifiers {
                    assert!(verifier.verify(b"seed:built").await.expect("verdict"));
                    assert!(!verifier.verify(b"other").await.expect("negative verdict"));
                }
                Ok(())
            })
        })
        .expect("workflow callbacks resolve");
    }

    #[test]
    fn given_async_sdk_callback_errors_when_called_then_should_keep_the_error_class() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let module = callbacks(py, c"workflow_callbacks_typed_error")?;
            let refused = module.getattr("refused")?.unbind();
            let builder = PyStepFn(Arc::new(refused.clone_ref(py)));
            let verifier = PyVerifier {
                callback: Arc::new(refused),
                raised: Arc::default(),
            };
            pyo3_async_runtimes::tokio::run(py, async move {
                let outputs = BTreeMap::new();
                let error = builder
                    .build(&StepContext { outputs: &outputs })
                    .await
                    .expect_err("build refused");
                assert!(matches!(error, LaserError::Invalid(_)));
                let error = verifier
                    .verify(b"reply")
                    .await
                    .expect_err("verification refused");
                assert!(matches!(error, LaserError::Invalid(_)));
                Ok(())
            })
        })
        .expect("callback errors retain their class");
    }

    #[test]
    fn given_invalid_workflow_callback_results_when_called_then_should_reject_configuration() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let module = callbacks(py, c"workflow_callbacks_invalid_value")?;
            let builder = PyStepFn(Arc::new(module.getattr("invalid_build")?.unbind()));
            let verifier = PyVerifier {
                callback: Arc::new(module.getattr("invalid_verify")?.unbind()),
                raised: Arc::default(),
            };
            pyo3_async_runtimes::tokio::run(py, async move {
                let outputs = BTreeMap::new();
                let error = builder
                    .build(&StepContext { outputs: &outputs })
                    .await
                    .expect_err("invalid build body");
                assert!(matches!(error, LaserError::HandlerConfig(_)));
                let error = verifier
                    .verify(b"reply")
                    .await
                    .expect_err("invalid verdict");
                assert!(matches!(error, LaserError::HandlerConfig(_)));
                Ok(())
            })
        })
        .expect("callback result types are checked");
    }

    #[test]
    fn given_a_pending_workflow_callback_when_dropped_then_should_cancel_the_python_task() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let module = callbacks(py, c"workflow_callbacks_cancellation")?;
            let builder = PyStepFn(Arc::new(module.getattr("pending_build")?.unbind()));
            let module = module.unbind();
            pyo3_async_runtimes::tokio::run(py, async move {
                let outputs = BTreeMap::new();
                let context = StepContext { outputs: &outputs };
                let mut pending = Box::pin(builder.build(&context));
                for _ in 0..100 {
                    tokio::select! {
                        result = &mut pending => panic!("callback finished before cancellation: {result:?}"),
                        () = tokio::time::sleep(std::time::Duration::from_millis(10)) => {},
                    }
                    let started = Python::attach(|py| module.bind(py).getattr("started")?.extract::<bool>())?;
                    if started { break; }
                }
                assert!(Python::attach(|py| module.bind(py).getattr("started")?.extract::<bool>())?);
                drop(pending);
                for _ in 0..100 {
                    let cancelled = Python::attach(|py| module.bind(py).getattr("cancelled")?.extract::<bool>())?;
                    if cancelled { return Ok(()); }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                panic!("Python callback was not cancelled");
            })
        })
        .expect("dropping a callback retires its Python task");
    }

    #[test]
    fn given_a_workflow_outcome_when_read_then_should_expose_outputs_steps_status_and_text() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let outcome = super::PyWorkflowOutcome {
                outputs: BTreeMap::from([("triage".to_owned(), b"sev2".to_vec())]),
                run_id: "run-7".to_owned(),
                completed_steps: vec!["triage".to_owned()],
                status: super::SessionStatus::Completed,
            };
            let outputs = outcome.outputs(py)?;
            let triage = outputs.get_item("triage")?.expect("triage output");
            assert_eq!(triage.extract::<Vec<u8>>()?, b"sev2");
            assert_eq!(outcome.run_id(), "run-7");
            assert_eq!(outcome.completed_steps(), ["triage"]);
            assert_eq!(outcome.status(py)?.extract::<String>()?, "completed");
            assert_eq!(outcome.text("triage").as_deref(), Some("sev2"));
            assert_eq!(outcome.text("missing"), None);
            Ok(())
        })
        .expect("workflow outcome reads");
    }
}

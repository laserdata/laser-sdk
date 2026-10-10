use crate::agent_runtime::{route_policy, static_topic, take_route_failure};
use crate::async_bridge::{HookLoop, call_hook, future_into_py, hook_callable};
use crate::errors::to_pyerr;
use laser_sdk::agent::{
    InboxRoute, OnTimeout, Router, StepContext, StepFn, Verifier, Workflow, WorkflowBudget,
};
use laser_sdk::laser::Laser;
use laser_sdk::types::{AgentId, ConversationId, PrincipalId};
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

#[async_trait::async_trait]
impl StepFn for PyStepFn {
    async fn build(&self, ctx: &StepContext<'_>) -> Result<Vec<u8>, laser_sdk::LaserError> {
        let value = call_hook(&HookLoop::default(), |call| {
            let py = call.py();
            let build = hook_callable(self.0.bind(py), "build", "a workflow step builder")?;
            let outputs = PyDict::new(py);
            for (label, output) in ctx.outputs {
                outputs.set_item(label, PyBytes::new(py, output))?;
            }
            call.call(&build, (outputs,))
        })
        .await
        .map_err(crate::errors::from_callback_error)?;
        Python::attach(|py| value.bind(py).extract::<Vec<u8>>()).map_err(|error| {
            laser_sdk::LaserError::HandlerConfig(format!(
                "workflow step builder must return bytes: {error}"
            ))
        })
    }
}

struct PyVerifier(Arc<Py<PyAny>>);

#[async_trait::async_trait]
impl Verifier for PyVerifier {
    async fn verify(&self, output: &[u8]) -> Result<bool, laser_sdk::LaserError> {
        let value = call_hook(&HookLoop::default(), |call| {
            let py = call.py();
            let verify = hook_callable(self.0.bind(py), "verify", "a workflow verifier")?;
            call.call(&verify, (PyBytes::new(py, output),))
        })
        .await
        .map_err(crate::errors::from_callback_error)?;
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
}

#[async_trait::async_trait]
impl StepFn for TrackedStepFn {
    async fn build(&self, context: &StepContext<'_>) -> Result<Vec<u8>, laser_sdk::LaserError> {
        self.active.store(self.index, Ordering::Release);
        self.callback.build(context).await
    }
}

// One step's declaration, collected by `step` and replayed into the Rust builder
// at `run`. Cloneable so `run` can take a snapshot without consuming the builder.
#[derive(Clone)]
struct StepSpec {
    label: String,
    target: Router,
    policy: Option<Arc<Py<PyAny>>>,
    after: Vec<String>,
    exclusive: bool,
    fence_namespace: Option<String>,
    on_timeout: OnTimeout,
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
    /// Resume an earlier run: the engine replays that run's journal and skips
    /// the steps already recorded complete, re-dispatching only the unfinished
    /// ones. Omit it to start a fresh run with its own id.
    fn run_id(&mut self, run_id: String) -> PyResult<()> {
        let run_id = run_id
            .parse::<ConversationId>()
            .map_err(|error| to_pyerr(error.into()))?;
        self.run_id = Some(run_id);
        Ok(())
    }

    /// Cap the workflow's spend with a `WorkflowBudget`. The token ceiling
    /// counts only the usage an AGDX reply carries, so it is advisory.
    fn budget(&mut self, budget: PyWorkflowBudget) {
        self.budget = budget.to_rust();
    }

    /// Add a step. Exactly one target is required: `to` (a named agent),
    /// `to_capable` (one agent advertising a skill), or `all_capable` (scatter to
    /// every agent advertising a skill and fold the replies, a verifier panel).
    /// `build(outputs) -> bytes` forms the task from the prior outputs. Build, verify, and compensate callbacks can return directly or through an awaitable. Objects with `build` or `verify` methods also work. Callback exceptions retain their SDK error class.
    /// `after`
    /// declares the dependencies that order the step. `verify(output) -> bool`
    /// gates completion. `exclusive` claims a fenced lease (needs the managed
    /// plane). `fence_namespace` also makes the step exclusive and aligns the
    /// lease with a handler's fenced KV effect. `on_timeout` is `"fail"`
    /// (default) or `"reassign"` (re-acquire the
    /// lease, bumping the fence, and hand the task to a fresh holder. Needs an
    /// exclusive step). `compensate(outputs) -> bytes` is the rollback run if a
    /// later step fails.
    #[pyo3(signature = (
        label, *, build, to=None, to_capable=None, all_capable=None, principal=None, after=None,
        verify=None, exclusive=false, fence_namespace=None, on_timeout="fail", compensate=None,
        policy=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn step(
        &mut self,
        label: String,
        build: Py<PyAny>,
        to: Option<String>,
        to_capable: Option<String>,
        all_capable: Option<String>,
        principal: Option<u32>,
        after: Option<Vec<String>>,
        verify: Option<Py<PyAny>>,
        exclusive: bool,
        fence_namespace: Option<String>,
        on_timeout: &str,
        compensate: Option<Py<PyAny>>,
        policy: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
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
        let on_timeout = match on_timeout {
            "fail" => OnTimeout::Fail,
            "reassign" => OnTimeout::Reassign,
            other => {
                return Err(crate::errors::InvalidError::new_err(format!(
                    "on_timeout must be `fail` or `reassign`, got `{other}`"
                )));
            }
        };
        self.steps.push(StepSpec {
            label,
            target,
            policy: saved_policy,
            after: after.unwrap_or_default(),
            exclusive: exclusive || fence_namespace.is_some(),
            fence_namespace,
            on_timeout,
            build: Arc::new(build),
            verify: verify.map(Arc::new),
            compensate: compensate.map(Arc::new),
        });
        Ok(())
    }

    /// Run the workflow, returning a `WorkflowOutcome` with the completed steps'
    /// outputs keyed by label and the run id.
    /// The workflow name is the orchestrator identity it dispatches as, so it must
    /// be a valid agent id. A failed step runs the compensations in reverse and
    /// raises. The run is a session whose id is the run id, and each step and
    /// compensation is a child session of it. A cancel request on
    /// `agent.control` stops the run at the next step boundary with
    /// `CancelledError` after the compensations.
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
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
        future_into_py(py, async move {
            let mut workflow = laser.workflow(&name).budget(budget);
            if let Some(route) = route {
                workflow = workflow.inbox_route(route);
            }
            if let Some(run_id) = run_id {
                workflow = workflow.run_id(run_id);
            }
            // Thread the move-based Rust builder: the first step turns the workflow
            // into a step handle, each later step chains onto the handle.
            enum Builder<'a> {
                Fresh(Workflow<'a>),
                Step(laser_sdk::agent::StepHandle<'a>),
            }
            let mut builder = Builder::Fresh(workflow);
            let mut route_failures = Vec::new();
            let active_step = Arc::new(AtomicUsize::new(usize::MAX));
            for (index, mut spec) in specs.into_iter().enumerate() {
                let parsed = Python::attach(|py| {
                    route_policy(spec.policy.as_ref().map(|policy| policy.bind(py)))
                })?;
                route_failures.push(parsed.failure);
                if let Router::ToCapable(selector) | Router::AllCapable(selector) = &mut spec.target
                {
                    selector.policy = parsed.policy;
                }
                let build = TrackedStepFn {
                    callback: PyStepFn(spec.build),
                    index,
                    active: Arc::clone(&active_step),
                };
                let mut handle = match builder {
                    Builder::Fresh(workflow) => workflow.step(&spec.label, spec.target, build),
                    Builder::Step(handle) => handle.step(&spec.label, spec.target, build),
                };
                for dependency in &spec.after {
                    handle = handle.after(dependency);
                }
                if let Some(verify) = spec.verify {
                    handle = handle.verify_with(PyVerifier(verify));
                }
                if let Some(namespace) = spec.fence_namespace {
                    handle = handle.exclusive_in(namespace);
                } else if spec.exclusive {
                    handle = handle.exclusive();
                }
                handle = handle.on_timeout(spec.on_timeout);
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
            let outcome = outcome.map_err(to_pyerr)?;
            Ok(PyWorkflowOutcome {
                outputs: outcome.outputs,
                run_id: outcome.run_id.to_string(),
            })
        })
    }
}

/// The result of a completed `Workflow.run`: each step's output by label, plus
/// the run id. Pass the run id to `Workflow.run_id` to resume the same run.
#[gen_stub_pyclass]
#[pyclass(name = "WorkflowOutcome", frozen)]
pub struct PyWorkflowOutcome {
    outputs: BTreeMap<String, Vec<u8>>,
    run_id: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyWorkflowOutcome {
    /// Each completed step's output, keyed by step label.
    #[getter]
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
                    module
                        .getattr(name)
                        .map(|callback| PyVerifier(Arc::new(callback.unbind())))
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
            let verifier = PyVerifier(Arc::new(refused));
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
            let verifier = PyVerifier(Arc::new(module.getattr("invalid_verify")?.unbind()));
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
    fn given_a_workflow_outcome_when_read_then_should_expose_outputs_and_run_id() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let outcome = super::PyWorkflowOutcome {
                outputs: BTreeMap::from([("triage".to_owned(), b"sev2".to_vec())]),
                run_id: "run-7".to_owned(),
            };
            let outputs = outcome.outputs(py)?;
            let triage = outputs.get_item("triage")?.expect("triage output");
            assert_eq!(triage.extract::<Vec<u8>>()?, b"sev2");
            assert_eq!(outcome.run_id(), "run-7");
            Ok(())
        })
        .expect("workflow outcome reads");
    }
}

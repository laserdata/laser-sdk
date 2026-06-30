use crate::agent_runtime::static_topic;
use crate::errors::to_pyerr;
use laser_sdk::agent::{
    Budget, InboxRoute, OnTimeout, RoutePolicy, Router, StepContext, StepFn, Verifier, Workflow,
};
use laser_sdk::laser::Laser;
use laser_sdk::types::AgentId;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::sync::Arc;
use std::time::Duration;

// A `StepFn` backed by a Python `build(outputs: dict[str, bytes]) -> bytes`
// callback: the step builds its task payload from the prior steps' outputs. Run
// synchronously inside the engine, so it holds the GIL only for the call. A
// callback that raises or returns a non-bytes value yields an empty payload (the
// step then carries nothing, rather than crashing the run).
struct PyStepFn(Arc<Py<PyAny>>);

impl StepFn for PyStepFn {
    fn build(&self, ctx: &StepContext<'_>) -> Vec<u8> {
        Python::attach(|py| {
            let outputs = PyDict::new(py);
            for (label, output) in ctx.outputs {
                if outputs.set_item(label, PyBytes::new(py, output)).is_err() {
                    return Vec::new();
                }
            }
            match self.0.bind(py).call1((outputs,)) {
                Ok(result) => result.extract::<Vec<u8>>().unwrap_or_default(),
                Err(error) => {
                    error.print(py);
                    Vec::new()
                }
            }
        })
    }
}

// A `Verifier` backed by a Python `verify(output: bytes) -> bool` callback. A
// callback that raises or returns a non-bool verdict is treated as a failed
// verification (the safe default, so a faulty verifier never passes a step).
struct PyVerifier(Arc<Py<PyAny>>);

impl Verifier for PyVerifier {
    fn verify(&self, output: &[u8]) -> bool {
        Python::attach(|py| {
            self.0
                .bind(py)
                .call1((PyBytes::new(py, output),))
                .and_then(|verdict| verdict.extract::<bool>())
                .unwrap_or(false)
        })
    }
}

// One step's declaration, collected by `step` and replayed into the Rust builder
// at `run`. Cloneable so `run` can take a snapshot without consuming the builder.
#[derive(Clone)]
struct StepSpec {
    label: String,
    target: Router,
    after: Vec<String>,
    exclusive: bool,
    on_timeout: OnTimeout,
    build: Arc<Py<PyAny>>,
    verify: Option<Arc<Py<PyAny>>>,
    compensate: Option<Arc<Py<PyAny>>>,
}

/// A journalled directed-acyclic workflow over the coordination primitives, the
/// Python view of the Rust engine. Declare steps with [`step`](Self::step), set a
/// [`budget`](Self::budget), then `await wf.run(source=...)`. Each step is a
/// directed task to its target, ordered by its declared dependencies, with an
/// optional verifier panel, exclusivity (a fenced at-most-once effect), an
/// on-timeout policy, and a compensation (the saga rollback).
#[gen_stub_pyclass]
#[pyclass(name = "Workflow")]
pub struct PyWorkflow {
    laser: Laser,
    name: String,
    budget: Budget,
    fixed_inbox: Option<String>,
    steps: Vec<StepSpec>,
}

impl PyWorkflow {
    pub(crate) fn new(laser: Laser, name: String, fixed_inbox: Option<String>) -> Self {
        Self {
            laser,
            name,
            budget: Budget::unlimited(),
            fixed_inbox,
            steps: Vec::new(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyWorkflow {
    /// Cap the workflow's spend. Any dimension left `None` is unbounded. The token
    /// ceiling counts only the usage an AGDX reply carries, so it is advisory.
    #[pyo3(signature = (*, tokens=None, wall_clock_ms=None, invocations=None))]
    fn budget(
        &mut self,
        tokens: Option<u64>,
        wall_clock_ms: Option<u64>,
        invocations: Option<u32>,
    ) {
        let mut budget = match tokens {
            Some(tokens) => Budget::tokens(tokens),
            None => Budget::unlimited(),
        };
        if let Some(ms) = wall_clock_ms {
            budget = budget.wall_clock(Duration::from_millis(ms));
        }
        if let Some(invocations) = invocations {
            budget = budget.invocations(invocations);
        }
        self.budget = budget;
    }

    /// Add a step. Exactly one target is required: `to` (a named agent),
    /// `to_capable` (one agent advertising a skill), or `all_capable` (scatter to
    /// every agent advertising a skill and fold the replies, a verifier panel).
    /// `build(outputs) -> bytes` forms the task from the prior outputs. `after`
    /// declares the dependencies that order the step. `verify(output) -> bool`
    /// gates completion. `exclusive` claims a fenced lease (needs the managed
    /// plane). `on_timeout` is `"fail"` (default) or `"reassign"` (re-acquire the
    /// lease, bumping the fence, and hand the task to a fresh holder; needs an
    /// exclusive step). `compensate(outputs) -> bytes` is the rollback run if a
    /// later step fails.
    #[pyo3(signature = (
        label, *, build, to=None, to_capable=None, all_capable=None, after=None,
        verify=None, exclusive=false, on_timeout="fail", compensate=None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn step(
        &mut self,
        label: String,
        build: Py<PyAny>,
        to: Option<String>,
        to_capable: Option<String>,
        all_capable: Option<String>,
        after: Option<Vec<String>>,
        verify: Option<Py<PyAny>>,
        exclusive: bool,
        on_timeout: &str,
        compensate: Option<Py<PyAny>>,
    ) -> PyResult<()> {
        let target = match (to, to_capable, all_capable) {
            (Some(agent), None, None) => {
                Router::to(AgentId::new(agent).map_err(|e| to_pyerr(e.into()))?)
            }
            (None, Some(skill), None) => Router::to_capable(skill, RoutePolicy::Any),
            (None, None, Some(skill)) => Router::all_capable(skill, RoutePolicy::Any),
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
            after: after.unwrap_or_default(),
            exclusive,
            on_timeout,
            build: Arc::new(build),
            verify: verify.map(Arc::new),
            compensate: compensate.map(Arc::new),
        });
        Ok(())
    }

    /// Run the workflow, returning the completed steps' outputs keyed by label.
    /// The workflow name is the orchestrator identity it dispatches as, so it must
    /// be a valid agent id. A failed step runs the compensations in reverse and
    /// raises.
    fn run<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let name = self.name.clone();
        let budget = self.budget;
        let route = self
            .fixed_inbox
            .clone()
            .map(|topic| InboxRoute::Fixed(static_topic(topic)));
        let specs = self.steps.clone();
        future_into_py(py, async move {
            let mut workflow = laser.workflow(&name).budget(budget);
            if let Some(route) = route {
                workflow = workflow.inbox_route(route);
            }
            // Thread the move-based Rust builder: the first step turns the workflow
            // into a step handle, each later step chains onto the handle.
            enum Builder<'a> {
                Fresh(Workflow<'a>),
                Step(laser_sdk::agent::StepHandle<'a>),
            }
            let mut builder = Builder::Fresh(workflow);
            for spec in specs {
                let build = PyStepFn(spec.build);
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
                if spec.exclusive {
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
            }
            .map_err(to_pyerr)?;
            Python::attach(|py| {
                let outputs = PyDict::new(py);
                for (label, output) in outcome.outputs {
                    outputs.set_item(label, PyBytes::new(py, &output))?;
                }
                Ok(outputs.into_any().unbind())
            })
        })
    }
}

use crate::async_bridge::{Awaitable, awaitable};
use crate::client::PyLaser;
use crate::errors::{InvalidError, from_callback_error, to_pyerr};
use crate::workflow::{PyWorkflowOutcome, decoded_step_output};
use laser_sdk::LaserError;
use laser_sdk::agent::{StepCall, WorkflowRun};
use laser_sdk::laser::Laser;
use laser_sdk::types::ConversationId;
use pyo3::exceptions::{PyException, PyRecursionError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBool, PyCFunction, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyfunction, gen_stub_pymethods};
use std::ffi::CStr;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::oneshot;

// How often a blocking run looks for a signal such as Ctrl-C.
const SIGNAL_CHECK_INTERVAL: Duration = Duration::from_millis(100);

// Arguments nested deeper than this, a cycle included, get no digest.
const CANONICAL_DEPTH: usize = 128;

// Async bodies stay in the caller's task, so its context, cancellation and
// traceback are the program's own.
const ASYNC_DRIVERS: &CStr = c"async def recorded(value):
    return value

async def run_step(checkpoint, function, args, kwargs, complete, abandon):
    try:
        await checkpoint()
        return await complete(await function(*args, **kwargs))
    except BaseException:
        abandon()
        raise

async def run_workflow(open_run, enter, leave, function, args, kwargs, finish, fail, release):
    run = await open_run()
    token = enter(run)
    try:
        value = await function(*args, **kwargs)
    except GeneratorExit:
        try:
            leave(token)
        except ValueError:
            pass
        release(run)
        raise
    except BaseException as error:
        leave(token)
        await fail(run, error)
        raise
    leave(token)
    return await finish(run, value)
";

static DRIVERS: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
static CURRENT_RUN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// A function decorated with `step`. Inside a workflow function a call journals
/// the step, and a resumed run returns the recorded value without calling it.
/// `fn` is the plain function.
#[gen_stub_pyclass]
#[pyclass(name = "Step", module = "laser_sdk", frozen, dict)]
pub struct PyStep {
    function: Py<PyAny>,
    label: String,
    asynchronous: bool,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyStep {
    /// The plain function, which runs without a workflow and journals nothing.
    #[getter]
    #[pyo3(name = "fn")]
    fn plain_function(&self, py: Python<'_>) -> Py<PyAny> {
        self.function.clone_ref(py)
    }

    #[pyo3(signature = (*args, **kwargs))]
    fn __call__<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let label = &self.label;
        let Some(active) = current_run(py)? else {
            let function = self
                .function
                .bind(py)
                .getattr("__name__")
                .and_then(|name| name.extract::<String>())
                .unwrap_or_else(|_| label.clone());
            return Err(InvalidError::new_err(format!(
                "step `{label}` was called outside a workflow: call it inside a function decorated with @laser_sdk.workflow, or call {function}.fn(...) for the plain function. A thread does not inherit its workflow: use asyncio.to_thread, or contextvars.copy_context().run"
            )));
        };
        let active = active.bind(py).get();
        if self.asynchronous && !active.asynchronous {
            return Err(InvalidError::new_err(format!(
                "step `{label}` is `async def` and its workflow is a plain function: make the workflow `async def` and await it"
            )));
        }
        let input = call_input(args, kwargs);
        let journal_label = match active
            .run
            .begin_step(label, input.as_deref())
            .map_err(to_pyerr)?
        {
            StepCall::Recorded(output) => {
                let value = decoded_step_output(py, label, &output)?;
                return if self.asynchronous {
                    drivers(py)?.call_method1("recorded", (value,))
                } else {
                    Ok(value)
                };
            }
            StepCall::Due(journal_label) => journal_label,
        };
        if self.asynchronous {
            return self.call_async(py, &active.run, journal_label, args, kwargs);
        }
        let called = self.call_blocking(py, &active.run, &journal_label, args, kwargs);
        if called.is_err() {
            active.run.abandon_step(&journal_label);
        }
        called
    }

    fn __get__<'py>(
        slf: &Bound<'py, Self>,
        instance: Option<&Bound<'py, PyAny>>,
        _owner: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let Some(instance) = instance else {
            return Ok(slf.clone().into_any());
        };
        let py = slf.py();
        let this = slf.get();
        let method = bound_method(this.function.bind(py), instance)?;
        let bound = Bound::new(
            py,
            Self {
                function: method.clone().unbind(),
                label: this.label.clone(),
                asynchronous: this.asynchronous,
            },
        )?;
        keep_function_identity(bound.as_any(), &method)?;
        Ok(bound.into_any())
    }
}

impl PyStep {
    fn call_blocking<'py>(
        &self,
        py: Python<'py>,
        run: &Arc<WorkflowRun>,
        journal_label: &str,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let checking = Arc::clone(run);
        block_on(py, async move { checking.checkpoint().await })?;
        let value = self.function.bind(py).call(args, kwargs)?;
        let output = step_output(&self.label, &value)?;
        let decoded = decoded_step_output(py, &self.label, &output)?;
        let recording = Arc::clone(run);
        let journal_label = journal_label.to_owned();
        block_on(py, async move {
            recording.complete_step(&journal_label, output).await
        })?;
        Ok(decoded)
    }

    fn call_async<'py>(
        &self,
        py: Python<'py>,
        run: &Arc<WorkflowRun>,
        journal_label: String,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let checkpoint_run = Arc::clone(run);
        let checkpoint = PyCFunction::new_closure(
            py,
            None,
            None,
            move |args: &Bound<'_, PyTuple>,
                  _: Option<&Bound<'_, PyDict>>|
                  -> PyResult<Py<PyAny>> {
                let run = Arc::clone(&checkpoint_run);
                let pending = awaitable(args.py(), async move {
                    run.checkpoint().await.map_err(to_pyerr)
                })?;
                Ok(pending_object(args.py(), pending))
            },
        )?;
        let settled = Arc::new(AtomicBool::new(false));
        let abandon = Py::new(
            py,
            DueStepGuard {
                run: Arc::clone(run),
                journal_label: journal_label.clone(),
                settled: Arc::clone(&settled),
            },
        )?;
        let complete_run = Arc::clone(run);
        let label = self.label.clone();
        let complete = PyCFunction::new_closure(
            py,
            None,
            None,
            move |args: &Bound<'_, PyTuple>,
                  _: Option<&Bound<'_, PyDict>>|
                  -> PyResult<Py<PyAny>> {
                let py = args.py();
                let output = step_output(&label, &args.get_item(0)?)?;
                let decoded = decoded_step_output(py, &label, &output)?.unbind();
                let run = Arc::clone(&complete_run);
                let journal_label = journal_label.clone();
                let settled = Arc::clone(&settled);
                let pending = awaitable(py, async move {
                    run.complete_step(&journal_label, output)
                        .await
                        .map_err(to_pyerr)?;
                    settled.store(true, Ordering::Release);
                    Ok(decoded)
                })?;
                Ok(pending_object(py, pending))
            },
        )?;
        let kwargs = match kwargs {
            Some(kwargs) => kwargs.clone(),
            None => PyDict::new(py),
        };
        drivers(py)?.call_method1(
            "run_step",
            (
                checkpoint,
                self.function.bind(py),
                args,
                kwargs,
                complete,
                abandon,
            ),
        )
    }
}

/// A function decorated with `workflow`. Calling it runs the workflow and
/// returns the function's value. `run(...)` returns the `WorkflowOutcome`,
/// `resume(run_id, ...)` continues a run, and `bind(laser)` runs on a client
/// the program already holds. An `async def` function makes each of them
/// awaitable.
#[gen_stub_pyclass]
#[pyclass(name = "WorkflowFunction", module = "laser_sdk", frozen, dict)]
pub struct PyWorkflowFunction {
    function: Py<PyAny>,
    name: String,
    stream: Option<String>,
    laser: Option<Laser>,
    asynchronous: bool,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyWorkflowFunction {
    #[pyo3(signature = (*args, **kwargs))]
    fn __call__<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.invoke(py, None, Answer::Value, args, kwargs)
    }

    /// Run the workflow and return its `WorkflowOutcome`: `run_id`, `status`,
    /// `completed_steps` in completion order, `value` (what the function
    /// returned), `json(label)` and `text(label)`.
    #[pyo3(signature = (*args, **kwargs))]
    #[gen_stub(override_return_type(type_repr = "typing.Any", imports = ("typing",)))]
    fn run<'py>(
        &self,
        py: Python<'py>,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.invoke(py, None, Answer::Outcome, args, kwargs)
    }

    /// Continue the run `run_id`: a step call the run recorded with the same
    /// arguments returns its value without being called. A call with other
    /// arguments is a new call and runs. Pass the function's arguments again,
    /// the journal does not hold them.
    #[pyo3(signature = (run_id, *args, **kwargs))]
    #[gen_stub(override_return_type(type_repr = "typing.Any", imports = ("typing",)))]
    fn resume<'py>(
        &self,
        py: Python<'py>,
        run_id: &str,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let run_id = run_id
            .parse::<ConversationId>()
            .map_err(|error| to_pyerr(error.into()))?;
        self.invoke(py, Some(run_id), Answer::Outcome, args, kwargs)
    }

    /// This workflow on `laser`, which stays open after the run. The run uses
    /// the client's default stream, else `LASER_STREAM`, else a stream named
    /// after the workflow.
    fn bind<'py>(slf: &Bound<'py, Self>, laser: PyRef<'_, PyLaser>) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let this = slf.get();
        let bound = Bound::new(
            py,
            Self {
                function: this.function.clone_ref(py),
                name: this.name.clone(),
                stream: this.stream.clone(),
                laser: Some(laser.inner.clone()),
                asynchronous: this.asynchronous,
            },
        )?;
        keep_function_identity(bound.as_any(), this.function.bind(py))?;
        Ok(bound)
    }

    fn __get__<'py>(
        slf: &Bound<'py, Self>,
        instance: Option<&Bound<'py, PyAny>>,
        _owner: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let Some(instance) = instance else {
            return Ok(slf.clone().into_any());
        };
        let py = slf.py();
        let this = slf.get();
        let method = bound_method(this.function.bind(py), instance)?;
        let bound = Bound::new(
            py,
            Self {
                function: method.clone().unbind(),
                name: this.name.clone(),
                stream: this.stream.clone(),
                laser: this.laser.clone(),
                asynchronous: this.asynchronous,
            },
        )?;
        keep_function_identity(bound.as_any(), &method)?;
        Ok(bound.into_any())
    }
}

impl PyWorkflowFunction {
    fn invoke<'py>(
        &self,
        py: Python<'py>,
        run_id: Option<ConversationId>,
        answer: Answer,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if self.asynchronous {
            self.invoke_async(py, run_id, answer, args, kwargs)
        } else {
            self.invoke_blocking(py, run_id, answer, args, kwargs)
        }
    }

    fn invoke_blocking<'py>(
        &self,
        py: Python<'py>,
        run_id: Option<ConversationId>,
        answer: Answer,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if py
            .import("asyncio")?
            .call_method0("get_running_loop")
            .is_ok()
        {
            return Err(InvalidError::new_err(format!(
                "workflow `{}` is a plain function and an event loop is running in this thread: make it `async def` and await it",
                self.name
            )));
        }
        let opened = self.config().open(run_id);
        let active = block_on(py, async move {
            opened
                .await
                .map_err(|error| LaserError::Handler(error.to_string()))?
        })?;
        let active = Py::new(py, active)?;
        let run = Arc::clone(&active.get().run);
        let owned = active.get().take_owned();
        let variable = current_run_variable(py)?;
        // Dropping the last handle releases the run, so one outlives its end.
        let token = variable.call_method1("set", (active.clone_ref(py),))?;
        let result = self.function.bind(py).call(args, kwargs);
        run.refuse_new_steps();
        variable.call_method1("reset", (token,))?;
        if let Err(error) = &result
            && !error.is_instance_of::<PyException>(py)
        {
            run.release();
            block_on(py, async move {
                close_owned(owned).await;
                Ok(())
            })?;
            return result;
        }
        let ended = match &result {
            Ok(_) => Ok(None),
            Err(error) => Err(from_callback_error(error.clone_ref(py))),
        };
        // Runs on when a signal ends the wait, so the connection still closes.
        let ending = pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            let finished = run.finish(ended).await;
            close_owned(owned).await;
            finished
        });
        let finished = match block_on(py, async move {
            ending
                .await
                .map_err(|error| LaserError::Handler(error.to_string()))?
        }) {
            // An interrupt while the run ends wins over the body's own result.
            Err(interrupt) if !interrupt.is_instance_of::<PyException>(py) => {
                return Err(interrupt);
            }
            finished => finished,
        };
        let value = result?;
        let outcome = finished?;
        match answer {
            Answer::Value => Ok(value),
            Answer::Outcome => Ok(Bound::new(
                py,
                PyWorkflowOutcome::completed(outcome, Some(value.unbind())),
            )?
            .into_any()),
        }
    }

    fn invoke_async<'py>(
        &self,
        py: Python<'py>,
        run_id: Option<ConversationId>,
        answer: Answer,
        args: &Bound<'py, PyTuple>,
        kwargs: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let config = self.config();
        let open_run = PyCFunction::new_closure(
            py,
            None,
            None,
            move |args: &Bound<'_, PyTuple>,
                  _: Option<&Bound<'_, PyDict>>|
                  -> PyResult<Py<PyAny>> {
                let opened = config.open(run_id);
                let pending = awaitable(args.py(), async move {
                    opened
                        .await
                        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
                        .map_err(to_pyerr)
                })?;
                Ok(pending_object(args.py(), pending))
            },
        )?;
        let finish = PyCFunction::new_closure(
            py,
            None,
            None,
            move |args: &Bound<'_, PyTuple>,
                  _: Option<&Bound<'_, PyDict>>|
                  -> PyResult<Py<PyAny>> {
                let py = args.py();
                let active = args.get_item(0)?.cast_into::<ActiveRun>()?;
                let run = Arc::clone(&active.get().run);
                run.refuse_new_steps();
                let owned = active.get().take_owned();
                let value = args.get_item(1)?.unbind();
                // Runs on even when the awaiting task is cancelled.
                let ending = pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
                    let finished = run.finish(Ok(None)).await;
                    close_owned(owned).await;
                    finished
                });
                let pending = awaitable(py, async move {
                    let outcome = ending
                        .await
                        .map_err(|error| PyRuntimeError::new_err(error.to_string()))?
                        .map_err(to_pyerr)?;
                    Python::attach(|py| match answer {
                        Answer::Value => Ok(value),
                        Answer::Outcome => Ok(Py::new(
                            py,
                            PyWorkflowOutcome::completed(outcome, Some(value)),
                        )?
                        .into_any()),
                    })
                })?;
                Ok(pending_object(py, pending))
            },
        )?;
        let fail = PyCFunction::new_closure(
            py,
            None,
            None,
            |args: &Bound<'_, PyTuple>, _: Option<&Bound<'_, PyDict>>| -> PyResult<Py<PyAny>> {
                let py = args.py();
                let active = args.get_item(0)?.cast_into::<ActiveRun>()?;
                let run = Arc::clone(&active.get().run);
                run.refuse_new_steps();
                let owned = active.get().take_owned();
                let error = args.get_item(1)?;
                // A cancelled or interrupted run stays resumable.
                let failure = error
                    .is_instance_of::<PyException>()
                    .then(|| from_callback_error(PyErr::from_value(error)));
                if failure.is_none() {
                    run.release();
                }
                let ending = pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
                    if let Some(failure) = failure
                        && let Err(error) = run.finish(Err(failure)).await
                    {
                        log::debug!("The failed workflow run ended with: {error}");
                    }
                    close_owned(owned).await;
                });
                let pending = awaitable(py, async move {
                    ending
                        .await
                        .map_err(|error| PyRuntimeError::new_err(error.to_string()))
                })?;
                Ok(pending_object(py, pending))
            },
        )?;
        let release = PyCFunction::new_closure(
            py,
            None,
            None,
            |args: &Bound<'_, PyTuple>, _: Option<&Bound<'_, PyDict>>| -> PyResult<()> {
                let active = args.get_item(0)?.cast_into::<ActiveRun>()?;
                active.get().run.release();
                let owned = active.get().take_owned();
                pyo3_async_runtimes::tokio::get_runtime().spawn(close_owned(owned));
                Ok(())
            },
        )?;
        let variable = current_run_variable(py)?;
        let kwargs = match kwargs {
            Some(kwargs) => kwargs.clone(),
            None => PyDict::new(py),
        };
        drivers(py)?.call_method1(
            "run_workflow",
            (
                open_run,
                variable.getattr("set")?,
                variable.getattr("reset")?,
                self.function.bind(py),
                args,
                kwargs,
                finish,
                fail,
                release,
            ),
        )
    }

    fn config(&self) -> WorkflowConfig {
        WorkflowConfig {
            name: self.name.clone(),
            stream: self.stream.clone(),
            laser: self.laser.clone(),
            asynchronous: self.asynchronous,
        }
    }
}

#[derive(Clone, Copy)]
enum Answer {
    Value,
    Outcome,
}

// What a run needs from its decorator, copied so a run owns it.
#[derive(Clone)]
struct WorkflowConfig {
    name: String,
    stream: Option<String>,
    laser: Option<Laser>,
    asynchronous: bool,
}

impl WorkflowConfig {
    // Opens the run on its own task, so a cancelled or interrupted caller strands no half opened connection.
    fn open(
        &self,
        run_id: Option<ConversationId>,
    ) -> oneshot::Receiver<Result<ActiveRun, LaserError>> {
        let opening = self.opening(run_id);
        let (opened, receive) = oneshot::channel();
        pyo3_async_runtimes::tokio::get_runtime().spawn(async move {
            drop(opened.send(opening.await));
        });
        receive
    }

    fn opening(
        &self,
        run_id: Option<ConversationId>,
    ) -> impl Future<Output = Result<ActiveRun, LaserError>> + Send + 'static {
        let config = self.clone();
        async move {
            let (laser, owned) = match (config.laser, config.stream) {
                (Some(laser), Some(stream)) => (laser.with_default_stream(stream), None),
                (Some(laser), None) => (laser, None),
                (None, Some(stream)) => {
                    let laser = Laser::connect_env_with_stream(&stream).await?;
                    (laser.clone(), Some(laser))
                }
                (None, None) => {
                    let laser = Laser::connect_env().await?;
                    (laser.clone(), Some(laser))
                }
            };
            let mut workflow = laser.workflow(&config.name);
            if let Some(run_id) = run_id {
                workflow = workflow.run_id(run_id);
            }
            match workflow.start().await {
                Ok(run) => Ok(ActiveRun {
                    run: Arc::new(run),
                    owned: Mutex::new(owned),
                    asynchronous: config.asynchronous,
                }),
                Err(error) => {
                    close_owned(owned).await;
                    Err(error)
                }
            }
        }
    }
}

// Gives a due step's label back. Held by the step's coroutine, so a coroutine
// that never starts gives the label back when it is collected.
#[pyclass(frozen)]
struct DueStepGuard {
    run: Arc<WorkflowRun>,
    journal_label: String,
    // Set once the label was given back or the step recorded. A traceback can keep the guard alive past both.
    settled: Arc<AtomicBool>,
}

#[pymethods]
impl DueStepGuard {
    fn __call__(&self) {
        self.give_back();
    }
}

impl DueStepGuard {
    fn give_back(&self) {
        if !self.settled.swap(true, Ordering::AcqRel) {
            self.run.abandon_step(&self.journal_label);
        }
    }
}

impl Drop for DueStepGuard {
    fn drop(&mut self) {
        self.give_back();
    }
}

// The run a workflow function is inside of, held in a context variable.
#[pyclass(frozen)]
struct ActiveRun {
    run: Arc<WorkflowRun>,
    // The connection this run opened, closed when the run ends.
    owned: Mutex<Option<Laser>>,
    asynchronous: bool,
}

impl ActiveRun {
    fn take_owned(&self) -> Option<Laser> {
        self.owned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

// A run nobody ended, because its caller was cancelled while it opened.
impl Drop for ActiveRun {
    fn drop(&mut self) {
        self.run.release();
        if let Some(laser) = self.take_owned() {
            pyo3_async_runtimes::tokio::get_runtime().spawn(close_owned(Some(laser)));
        }
    }
}

/// Make a function a workflow step. Inside a function decorated with `workflow`
/// each call of the step is journaled, so a resumed run returns the recorded
/// value and does not call the function again. A step takes and returns what
/// JSON holds: dict, list, str, int, float, bool or None. A call is known by the
/// step's name and its arguments, so a call with other arguments runs as a new
/// call. The step's name is the function's name, or `@step("name")`.
///
/// ```python
/// import laser_sdk as ls
///
/// @ls.step
/// def ingest():
///     return {"satellite": "sat-7", "frames": 12}
///
/// @ls.workflow
/// def pipeline():
///     return ingest()
///
/// print(pipeline())
/// ```
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (function=None, *, name=None))]
#[gen_stub(override_return_type(type_repr = "typing.Any", imports = ("typing",)))]
pub fn step<'py>(
    py: Python<'py>,
    function: Option<Bound<'py, PyAny>>,
    name: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let (function, name) = function_and_name(function, name)?;
    if let Some(function) = function {
        return new_step(&function, name);
    }
    let decorator = PyCFunction::new_closure(
        py,
        None,
        None,
        move |args: &Bound<'_, PyTuple>, _: Option<&Bound<'_, PyDict>>| -> PyResult<Py<PyAny>> {
            Ok(new_step(&args.get_item(0)?, name.clone())?.unbind())
        },
    )?;
    Ok(decorator.into_any())
}

/// Make a function a durable workflow. It connects with
/// `LASER_CONNECTION_STRING`, journals each step the function calls, and closes
/// the connection when the run ends. With that variable set, it needs no Docker
/// and no local server. Calling it returns the function's value. `run()`
/// returns a `WorkflowOutcome` with `run_id`, `status`, `completed_steps` and
/// `value`, and `resume(run_id)` continues a run. The run uses `stream=`, else
/// `LASER_STREAM`, else a stream named after the workflow, and creates it when
/// it is missing. An `async def` function is awaited, and its steps can be
/// `async def` too.
///
/// ```python
/// import json
/// import time
///
/// import laser_sdk as ls
///
///
/// @ls.step
/// def ingest():
///     return "frames from sat-7"
///
///
/// @ls.step
/// def analyze(frames):
///     time.sleep(1)
///     return f"analyzed {frames}"
///
///
/// @ls.step
/// def report(analysis):
///     return f"report on {analysis}"
///
///
/// @ls.workflow
/// def pipeline():
///     return report(analyze(ingest()))
///
///
/// run = pipeline.run()
/// print(json.dumps({"status": run.status, "steps": run.completed_steps, "report": run.value}))
/// ```
#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (function=None, *, name=None, stream=None))]
#[gen_stub(override_return_type(type_repr = "typing.Any", imports = ("typing",)))]
pub fn workflow<'py>(
    py: Python<'py>,
    function: Option<Bound<'py, PyAny>>,
    name: Option<String>,
    stream: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let (function, name) = function_and_name(function, name)?;
    if let Some(function) = function {
        return new_workflow(&function, name, stream);
    }
    let decorator = PyCFunction::new_closure(
        py,
        None,
        None,
        move |args: &Bound<'_, PyTuple>, _: Option<&Bound<'_, PyDict>>| -> PyResult<Py<PyAny>> {
            Ok(new_workflow(&args.get_item(0)?, name.clone(), stream.clone())?.unbind())
        },
    )?;
    Ok(decorator.into_any())
}

// `@step("name")` passes the name where `@step` passes the function.
fn function_and_name<'py>(
    function: Option<Bound<'py, PyAny>>,
    name: Option<String>,
) -> PyResult<(Option<Bound<'py, PyAny>>, Option<String>)> {
    match function {
        Some(given) if given.is_instance_of::<PyString>() => {
            Ok((None, Some(given.extract::<String>()?)))
        }
        function => Ok((function, name)),
    }
}

fn new_step<'py>(
    function: &Bound<'py, PyAny>,
    name: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let py = function.py();
    let step = Bound::new(
        py,
        PyStep {
            function: function.clone().unbind(),
            label: function_name(function, name)?,
            asynchronous: is_coroutine_function(function)?,
        },
    )?;
    keep_function_identity(step.as_any(), function)?;
    Ok(step.into_any())
}

fn new_workflow<'py>(
    function: &Bound<'py, PyAny>,
    name: Option<String>,
    stream: Option<String>,
) -> PyResult<Bound<'py, PyAny>> {
    let py = function.py();
    let workflow = Bound::new(
        py,
        PyWorkflowFunction {
            function: function.clone().unbind(),
            name: function_name(function, name)?,
            stream,
            laser: None,
            asynchronous: is_coroutine_function(function)?,
        },
    )?;
    keep_function_identity(workflow.as_any(), function)?;
    Ok(workflow.into_any())
}

fn function_name(function: &Bound<'_, PyAny>, name: Option<String>) -> PyResult<String> {
    if !function.is_callable() {
        return Err(PyTypeError::new_err(
            "a step or workflow decorates a function",
        ));
    }
    match name {
        Some(name) => Ok(name),
        None => function.getattr("__name__")?.extract(),
    }
}

fn is_coroutine_function(function: &Bound<'_, PyAny>) -> PyResult<bool> {
    function
        .py()
        .import("inspect")?
        .call_method1("iscoroutinefunction", (function,))?
        .extract()
}

fn keep_function_identity(wrapper: &Bound<'_, PyAny>, function: &Bound<'_, PyAny>) -> PyResult<()> {
    wrapper
        .py()
        .import("functools")?
        .call_method1("update_wrapper", (wrapper, function))?;
    Ok(())
}

fn drivers(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    DRIVERS
        .get_or_try_init(py, || {
            PyModule::from_code(
                py,
                ASYNC_DRIVERS,
                c"laser_sdk_workflow_function.py",
                c"laser_sdk_workflow_function",
            )
            .map(Bound::unbind)
        })
        .map(|module| module.bind(py).clone())
}

fn current_run_variable(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    CURRENT_RUN
        .get_or_try_init(py, || {
            py.import("contextvars")?
                .call_method1("ContextVar", ("laser_sdk_workflow_run",))
                .map(Bound::unbind)
        })
        .map(|variable| variable.bind(py).clone())
}

fn current_run(py: Python<'_>) -> PyResult<Option<Py<ActiveRun>>> {
    let value = current_run_variable(py)?.call_method1("get", (py.None(),))?;
    if value.is_none() {
        return Ok(None);
    }
    Ok(Some(value.cast_into::<ActiveRun>()?.unbind()))
}

fn bound_method<'py>(
    function: &Bound<'py, PyAny>,
    instance: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    function
        .py()
        .import("types")?
        .call_method1("MethodType", (function, instance))
}

// The canonical JSON of the arguments, the same bytes every client digests, so
// a resumed call finds the record of the same call. Arguments JSON cannot hold
// match their records by call order. Keyword arguments make the text an
// object, which no list of positional arguments can equal.
fn call_input(args: &Bound<'_, PyTuple>, kwargs: Option<&Bound<'_, PyDict>>) -> Option<Vec<u8>> {
    let mut text = String::new();
    match kwargs.filter(|kwargs| !kwargs.is_empty()) {
        Some(kwargs) => {
            text.push_str("{\"args\":");
            write_canonical(args.as_any(), 0, &mut text)?;
            text.push_str(",\"kwargs\":");
            write_canonical(kwargs.as_any(), 0, &mut text)?;
            text.push('}');
        }
        None => write_canonical(args.as_any(), 0, &mut text)?,
    }
    Some(text.into_bytes())
}

// Keys sort by code point and a number is written as `number_text` writes it.
fn write_canonical(value: &Bound<'_, PyAny>, depth: usize, text: &mut String) -> Option<()> {
    if depth > CANONICAL_DEPTH {
        return None;
    }
    if let Some(scalar) = scalar_text(value) {
        text.push_str(&scalar?);
    } else if let Ok(words) = value.cast::<PyString>() {
        text.push_str(&quoted(&words.extract::<String>().ok()?)?);
    } else if let Ok(entries) = value.cast::<PyDict>() {
        let mut members = Vec::with_capacity(entries.len());
        for (key, member) in entries.iter() {
            members.push((key_text(&key)?, member));
        }
        members.sort_unstable_by(|(first, _), (second, _)| first.cmp(second));
        // Two keys JSON writes alike leave the object without one meaning.
        if members
            .windows(2)
            .any(|pair| matches!(pair, [(first, _), (second, _)] if first == second))
        {
            return None;
        }
        text.push('{');
        for (index, (key, member)) in members.iter().enumerate() {
            if index > 0 {
                text.push(',');
            }
            text.push_str(&quoted(key)?);
            text.push(':');
            write_canonical(member, depth + 1, text)?;
        }
        text.push('}');
    } else if value.is_instance_of::<PyList>() || value.is_instance_of::<PyTuple>() {
        text.push('[');
        for (index, item) in value.try_iter().ok()?.enumerate() {
            if index > 0 {
                text.push(',');
            }
            write_canonical(&item.ok()?, depth + 1, text)?;
        }
        text.push(']');
    } else {
        return None;
    }
    Some(())
}

// A dict key as JSON writes it: text as it is, anything else a scalar's text.
fn key_text(key: &Bound<'_, PyAny>) -> Option<String> {
    match key.cast::<PyString>() {
        Ok(words) => words.extract().ok(),
        Err(_) => scalar_text(key)?,
    }
}

// `None`, a bool or a number as JSON text. The outer `None` says the value is
// none of those, the inner one that JSON cannot hold it.
fn scalar_text(value: &Bound<'_, PyAny>) -> Option<Option<String>> {
    if value.is_none() {
        Some(Some("null".to_owned()))
    } else if let Ok(flag) = value.cast::<PyBool>() {
        Some(Some(flag.is_true().to_string()))
    } else if value.is_instance_of::<PyInt>() || value.is_instance_of::<PyFloat>() {
        Some(number_text(value))
    } else {
        None
    }
}

// A whole number as its digits, also when it is a float, and a number with a
// fraction as ECMAScript writes it, so `1.0` and `1` are one argument in every
// client. A number that is not finite has no JSON.
fn number_text(value: &Bound<'_, PyAny>) -> Option<String> {
    let whole = if value.is_instance_of::<PyInt>() {
        value.clone()
    } else {
        let number = value.extract::<f64>().ok()?;
        if !number.is_finite() {
            return None;
        }
        if number.fract() != 0.0 {
            return fraction_text(number);
        }
        value.py().get_type::<PyInt>().call1((value,)).ok()?
    };
    value
        .py()
        .get_type::<PyInt>()
        .call_method1("__repr__", (whole,))
        .ok()?
        .extract()
        .ok()
}

// The shortest digits that read back as `number`, in plain notation from 1e-6
// up and with an exponent below it.
fn fraction_text(number: f64) -> Option<String> {
    let scientific = format!("{:e}", number.abs());
    let (mantissa, exponent) = scientific.split_once('e')?;
    let exponent = exponent.parse::<i32>().ok()?;
    let digits = mantissa.replace('.', "");
    let sign = if number < 0.0 { "-" } else { "" };
    if let Ok(whole_digits) = usize::try_from(exponent + 1)
        && whole_digits > 0
    {
        let (whole, fraction) = digits.split_at_checked(whole_digits)?;
        return Some(format!("{sign}{whole}.{fraction}"));
    }
    if exponent > -7 {
        let zeros = "0".repeat(usize::try_from(-exponent - 1).ok()?);
        return Some(format!("{sign}0.{zeros}{digits}"));
    }
    let (first, rest) = digits.split_at_checked(1)?;
    let point = if rest.is_empty() { "" } else { "." };
    Some(format!("{sign}{first}{point}{rest}e{exponent}"))
}

fn quoted(words: &str) -> Option<String> {
    serde_json::to_string(words).ok()
}

fn pending_object<T>(py: Python<'_>, pending: Awaitable<'_, T>) -> Py<PyAny> {
    match pending.into_pyobject(py) {
        Ok(future) => future.unbind(),
        Err(never) => match never {},
    }
}

// JSON a later run reads back, so a fresh run and a resumed one see one value.
fn step_output(label: &str, value: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let py = value.py();
    let options = PyDict::new(py);
    options.set_item("separators", (",", ":"))?;
    options.set_item("allow_nan", false)?;
    options.set_item("ensure_ascii", false)?;
    // Text that is not valid Unicode has no UTF-8, so it is refused as well.
    let encoded = py
        .import("json")?
        .call_method("dumps", (value,), Some(&options))
        .and_then(|text| text.extract::<String>());
    match encoded {
        Ok(text) => Ok(text.into_bytes()),
        Err(error)
            if error.is_instance_of::<PyTypeError>(py)
                || error.is_instance_of::<PyValueError>(py)
                || error.is_instance_of::<PyRecursionError>(py) =>
        {
            Err(InvalidError::new_err(format!(
                "step `{label}` returned a value JSON cannot hold ({}). Return a dict, list, str, int, float, bool or None",
                error.value(py)
            )))
        }
        Err(error) => Err(error),
    }
}

async fn close_owned(owned: Option<Laser>) {
    if let Some(laser) = owned
        && let Err(error) = laser.close().await
    {
        log::warn!("Closing the connection of a workflow run failed: {error}");
    }
}

// Wait on the runtime with the interpreter lock released, and let a signal
// such as Ctrl-C end the wait.
fn block_on<F, T>(py: Python<'_>, future: F) -> PyResult<T>
where
    F: Future<Output = Result<T, LaserError>> + Send + 'static,
    T: Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(InvalidError::new_err(
            "a plain workflow function cannot run inside a step callback: make it `async def` and await it",
        ));
    }
    let runtime = pyo3_async_runtimes::tokio::get_runtime();
    let mut task = runtime.spawn(future);
    loop {
        let waited = py.detach(|| {
            runtime.block_on(async { tokio::time::timeout(SIGNAL_CHECK_INTERVAL, &mut task).await })
        });
        match waited {
            Ok(Ok(result)) => return result.map_err(to_pyerr),
            Ok(Err(error)) => return Err(PyRuntimeError::new_err(error.to_string())),
            Err(_) => {
                if let Err(interrupted) = py.check_signals() {
                    task.abort();
                    return Err(interrupted);
                }
            }
        }
    }
}

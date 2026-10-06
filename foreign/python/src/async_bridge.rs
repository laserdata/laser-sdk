use crate::errors::InvalidError;
use pyo3::BoundObject;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3_async_runtimes::TaskLocals;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Takes back a successful result its cancelled Python future never received.
pub type Undelivered = Box<dyn FnOnce(Python<'_>, Py<PyAny>) + Send>;

/// The event loop and context variables a Python hook falls back to when it
/// runs outside any awaiting caller, such as on a worker lane the runtime
/// spawned. Captured where the hook was registered, empty when no loop ran.
#[derive(Clone, Default)]
pub(crate) struct HookLoop(Option<TaskLocals>);

impl HookLoop {
    pub(crate) fn capture(py: Python<'_>) -> Self {
        Self(pyo3_async_runtimes::tokio::get_current_locals(py).ok())
    }

    // The awaiting caller's loop and context win, so a hook sees the context
    // of the call that reached it.
    fn resolve(&self, py: Python<'_>) -> Option<TaskLocals> {
        pyo3_async_runtimes::tokio::get_current_locals(py)
            .ok()
            .or_else(|| self.0.clone())
    }
}

/// One synchronous step of a hook call. It runs inside a copy of the
/// caller's context, so context variables reach synchronous hooks too.
pub(crate) struct HookCall<'py> {
    py: Python<'py>,
    run: Option<Bound<'py, PyAny>>,
}

impl<'py> HookCall<'py> {
    pub(crate) fn py(&self) -> Python<'py> {
        self.py
    }

    pub(crate) fn call<A>(&self, callable: &Bound<'py, PyAny>, args: A) -> PyResult<Py<PyAny>>
    where
        A: IntoPyObject<'py, Target = PyTuple>,
        A::Error: Into<PyErr>,
    {
        let args = args
            .into_pyobject(self.py)
            .map_err(Into::into)?
            .into_bound();
        let value = match &self.run {
            Some(run) => {
                let mut items = Vec::with_capacity(args.len() + 1);
                items.push(callable.clone());
                items.extend(args.iter());
                run.call1(PyTuple::new(self.py, items)?)?
            }
            None => callable.call1(args)?,
        };
        Ok(value.unbind())
    }

    pub(crate) fn call_method<A>(
        &self,
        target: &Bound<'py, PyAny>,
        name: &str,
        args: A,
    ) -> PyResult<Py<PyAny>>
    where
        A: IntoPyObject<'py, Target = PyTuple>,
        A::Error: Into<PyErr>,
    {
        self.call(&target.getattr(name)?, args)
    }
}

/// A hook given as an object with a named method or as a plain callable,
/// the two shapes a Rust trait seam takes from Python. Either form can
/// return directly or through an awaitable.
#[derive(Clone)]
pub(crate) struct PyHook {
    callable: Arc<Py<PyAny>>,
    fallback: HookLoop,
}

impl PyHook {
    pub(crate) fn new(value: &Bound<'_, PyAny>, method: &str, role: &str) -> PyResult<Self> {
        Ok(Self {
            callable: Arc::new(hook_callable(value, method, role)?.unbind()),
            fallback: HookLoop::capture(value.py()),
        })
    }

    pub(crate) async fn call(
        &self,
        args: impl for<'py> FnOnce(Python<'py>) -> PyResult<Bound<'py, PyTuple>>,
    ) -> PyResult<Py<PyAny>> {
        call_hook(&self.fallback, |call| {
            let args = args(call.py())?;
            call.call(self.callable.bind(call.py()), args)
        })
        .await
    }
}

/// `value.method` when `value` has one, else `value` itself when it is
/// callable.
pub(crate) fn hook_callable<'py>(
    value: &Bound<'py, PyAny>,
    method: &str,
    role: &str,
) -> PyResult<Bound<'py, PyAny>> {
    match value.getattr(method) {
        Ok(bound) if bound.is_callable() => Ok(bound),
        _ if value.is_callable() => Ok(value.clone()),
        _ => Err(InvalidError::new_err(format!(
            "{role} must be callable or have a {method} method"
        ))),
    }
}

/// Call a Python hook and resolve its result: a plain value as is, an
/// awaitable as a task on its event loop with the caller's context. Dropping
/// the returned future cancels that task.
pub(crate) async fn call_hook(
    fallback: &HookLoop,
    call: impl FnOnce(&HookCall<'_>) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let scheduled = Python::attach(|py| -> PyResult<_> {
        let locals = fallback.resolve(py);
        let run = locals
            .as_ref()
            .map(|locals| locals.context(py))
            .filter(|context| !context.is_none())
            .map(|context| context.call_method0("copy")?.getattr("run"))
            .transpose()?;
        let value = call(&HookCall { py, run })?;
        let awaitable = value.bind(py);
        if !awaitable.hasattr("__await__")? {
            return Ok(Ok(value));
        }
        let Some(locals) = locals else {
            if awaitable.hasattr("close")? {
                awaitable.call_method0("close")?;
            }
            return Err(PyRuntimeError::new_err(
                "an asynchronous hook needs a running event loop",
            ));
        };
        let (sender, receiver) = oneshot::channel();
        let schedule = Py::new(
            py,
            ScheduleHook {
                awaitable: value,
                sender: Some(sender),
            },
        )?;
        let options = PyDict::new(py);
        options.set_item("context", locals.context(py))?;
        locals
            .event_loop(py)
            .call_method("call_soon_threadsafe", (schedule,), Some(&options))?;
        Ok(Err((locals, receiver)))
    })?;
    let (locals, receiver) = match scheduled {
        Ok(value) => return Ok(value),
        Err(pending) => pending,
    };
    let task = receiver
        .await
        .map_err(|_| PyRuntimeError::new_err("Python callback was not scheduled"))??;
    let mut cancellation = CancelHook {
        event_loop: Python::attach(|py| locals.event_loop(py).unbind()),
        task: Some(task),
    };
    let future = Python::attach(|py| {
        pyo3_async_runtimes::into_future_with_locals(
            &locals,
            cancellation
                .task
                .as_ref()
                .expect("pending callback")
                .bind(py)
                .clone(),
        )
    })?;
    let result = future.await;
    cancellation.task = None;
    result
}

// Wraps an awaitable in a task on its loop and hands the task back, or
// cancels it when the Rust caller stopped waiting before it started.
#[pyclass]
struct ScheduleHook {
    awaitable: Py<PyAny>,
    sender: Option<oneshot::Sender<PyResult<Py<PyAny>>>>,
}

#[pymethods]
impl ScheduleHook {
    fn __call__(&mut self, py: Python<'_>) -> PyResult<()> {
        let result = py
            .import("asyncio")?
            .call_method1("ensure_future", (self.awaitable.bind(py),))
            .map(Bound::unbind);
        if let Some(sender) = self.sender.take()
            && let Err(Ok(task)) = sender.send(result)
        {
            task.bind(py).call_method0("cancel")?;
        }
        Ok(())
    }
}

// Cancels a hook task still running when its Rust caller is dropped.
struct CancelHook {
    event_loop: Py<PyAny>,
    task: Option<Py<PyAny>>,
}

impl Drop for CancelHook {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            Python::attach(|py| {
                let result = task.bind(py).getattr("cancel").and_then(|cancel| {
                    self.event_loop
                        .bind(py)
                        .call_method1("call_soon_threadsafe", (cancel,))
                });
                if let Err(error) = result {
                    log::warn!("could not cancel Python callback: {error}");
                }
            });
        }
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use pyo3::BoundObject;
    use pyo3_async_runtimes::tokio::{get_current_locals, get_current_loop, scope};
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::sync::{Arc, Mutex};
    use tokio::task::AbortHandle;

    struct Completion {
        value: Py<PyAny>,
        failed: bool,
    }

    #[pyclass]
    struct ReadySignal {
        event_loop: Py<PyAny>,
        target: Py<PyAny>,
        reader: Mutex<UnixStream>,
        completion: Arc<Mutex<Option<Completion>>>,
        fd: i32,
    }

    #[pymethods]
    impl ReadySignal {
        fn __call__(&self, py: Python<'_>) -> PyResult<()> {
            let mut byte = [0_u8; 1];
            let _ = self
                .reader
                .lock()
                .expect("async signal reader lock")
                .read(&mut byte);
            self.event_loop
                .bind(py)
                .call_method1("remove_reader", (self.fd,))?;

            let Some(completion) = self
                .completion
                .lock()
                .expect("async completion lock")
                .take()
            else {
                return Ok(());
            };
            let target = self.target.bind(py);
            if target.call_method0("cancelled")?.is_truthy()? {
                return Ok(());
            }

            let method = if completion.failed {
                "set_exception"
            } else {
                "set_result"
            };
            target.call_method1(method, (completion.value.bind(py),))?;
            Ok(())
        }
    }

    #[pyclass]
    struct AbortOnCancel {
        event_loop: Py<PyAny>,
        abort: AbortHandle,
        fd: i32,
    }

    #[pymethods]
    impl AbortOnCancel {
        fn __call__(&self, future: &Bound<'_, PyAny>) -> PyResult<()> {
            if future.call_method0("cancelled")?.is_truthy()? {
                self.abort.abort();
                self.event_loop
                    .bind(future.py())
                    .call_method1("remove_reader", (self.fd,))?;
            }
            Ok(())
        }
    }

    pub fn future_into_py<'py, F, T>(py: Python<'py>, future: F) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'a> IntoPyObject<'a> + Send + 'static,
        for<'a> <T as IntoPyObject<'a>>::Error: Into<PyErr>,
    {
        spawn_into_py(py, future)
    }

    fn spawn_into_py<'py, F, T>(py: Python<'py>, future: F) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'a> IntoPyObject<'a> + Send + 'static,
        for<'a> <T as IntoPyObject<'a>>::Error: Into<PyErr>,
    {
        let event_loop = get_current_loop(py)?.unbind();
        let target = event_loop.bind(py).call_method0("create_future")?;
        let task_locals = get_current_locals(py)?;
        let (reader, mut writer) = UnixStream::pair()?;
        reader.set_nonblocking(true)?;
        let fd = reader.as_raw_fd();
        let completion = Arc::new(Mutex::new(None));
        let completion_for_task = Arc::clone(&completion);

        let ready = Py::new(
            py,
            ReadySignal {
                event_loop: event_loop.clone_ref(py),
                target: target.clone().unbind(),
                reader: Mutex::new(reader),
                completion,
                fd,
            },
        )?;
        event_loop
            .bind(py)
            .call_method1("add_reader", (fd, ready))?;

        let task =
            pyo3_async_runtimes::tokio::get_runtime().spawn(scope(task_locals, async move {
                let result = future.await;
                let completed = Python::attach(move |py| match result {
                    Ok(value) => match value.into_pyobject(py) {
                        Ok(value) => Completion {
                            value: value.into_any().unbind(),
                            failed: false,
                        },
                        Err(error) => Completion {
                            value: Into::<PyErr>::into(error).into_value(py).into_any(),
                            failed: true,
                        },
                    },
                    Err(error) => Completion {
                        value: error.into_value(py).into_any(),
                        failed: true,
                    },
                });
                *completion_for_task.lock().expect("async completion lock") = Some(completed);
                let _ = writer.write_all(&[1]);
            }));

        target.call_method1(
            "add_done_callback",
            (Py::new(
                py,
                AbortOnCancel {
                    event_loop,
                    abort: task.abort_handle(),
                    fd,
                },
            )?,),
        )?;

        Ok(target)
    }
}

#[cfg(unix)]
pub use unix::future_into_py;

#[cfg(not(unix))]
pub fn future_into_py<'py, F, T>(py: Python<'py>, future: F) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    pyo3_async_runtimes::tokio::future_into_py(py, future)
}

mod portable {
    use super::*;
    use pyo3::BoundObject;
    use pyo3::sync::PyOnceLock;
    use pyo3::types::PyModule;
    use pyo3_async_runtimes::tokio::{get_current_locals, get_current_loop, scope};
    use std::ffi::CStr;
    use std::sync::{Arc, Mutex};
    use tokio::task::AbortHandle;

    // The awaitable a caller gets. asyncio can cancel the awaiting task after
    // the result was set and before the task resumes, and the coroutine then
    // receives `CancelledError` with the result still on the future. Handing
    // the result back there closes the only window in which a completed read
    // could be lost.
    const RECEIVE: &CStr = c"async def receive(future, give_back, start):
    start()
    try:
        return await future
    except BaseException:
        if future.done() and not future.cancelled() and future.exception() is None:
            give_back(future.result())
        raise
";

    static RECEIVE_FN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

    type SharedUndelivered = Arc<Mutex<Option<Undelivered>>>;

    struct Completion {
        value: Py<PyAny>,
        failed: bool,
    }

    #[pyclass]
    struct Delivery {
        target: Py<PyAny>,
        completion: Arc<Mutex<Option<Completion>>>,
        undelivered: SharedUndelivered,
    }

    #[pymethods]
    impl Delivery {
        fn __call__(&self, py: Python<'_>) -> PyResult<()> {
            let Some(completion) = self.completion.lock().expect("completion lock").take() else {
                return Ok(());
            };
            let target = self.target.bind(py);
            if target.call_method0("cancelled")?.is_truthy()? {
                if !completion.failed {
                    return_value(py, &self.undelivered, completion.value);
                }
            } else {
                let method = if completion.failed {
                    "set_exception"
                } else {
                    "set_result"
                };
                target.call_method1(method, (completion.value.bind(py),))?;
            }
            Ok(())
        }
    }

    /// Returns a value its awaiting task never took, at most once.
    #[pyclass]
    struct GiveBack(SharedUndelivered);

    #[pymethods]
    impl GiveBack {
        fn __call__(&self, py: Python<'_>, value: Py<PyAny>) {
            return_value(py, &self.0, value);
        }
    }

    type StartCallback = Box<dyn for<'py> FnOnce(Python<'py>) -> PyResult<()> + Send>;

    // Work starts only when the Python coroutine starts. Cancelling a task
    // before its first poll must not pop a record on the Rust runtime.
    #[pyclass]
    struct StartRead(Mutex<Option<StartCallback>>);

    #[pymethods]
    impl StartRead {
        fn __call__(&self, py: Python<'_>) -> PyResult<()> {
            match self.0.lock().expect("start lock").take() {
                Some(start) => start(py),
                None => Ok(()),
            }
        }
    }

    #[pyclass]
    struct AbortOnCancel(AbortHandle);

    #[pymethods]
    impl AbortOnCancel {
        fn __call__(&self, future: &Bound<'_, PyAny>) -> PyResult<()> {
            if future.call_method0("cancelled")?.is_truthy()? {
                self.0.abort();
            }
            Ok(())
        }
    }

    pub fn future_into_py_returning<'py, F, T>(
        py: Python<'py>,
        future: F,
        undelivered: Undelivered,
    ) -> PyResult<Bound<'py, PyAny>>
    where
        F: Future<Output = PyResult<T>> + Send + 'static,
        T: for<'a> IntoPyObject<'a> + Send + 'static,
        for<'a> <T as IntoPyObject<'a>>::Error: Into<PyErr>,
    {
        let event_loop = get_current_loop(py)?.unbind();
        let target = event_loop.bind(py).call_method0("create_future")?;
        let locals = get_current_locals(py)?;
        let completion = Arc::new(Mutex::new(None));
        let undelivered: SharedUndelivered = Arc::new(Mutex::new(Some(undelivered)));
        let delivery = Py::new(
            py,
            Delivery {
                target: target.clone().unbind(),
                completion: completion.clone(),
                undelivered: undelivered.clone(),
            },
        )?;
        let callback_target = target.clone().unbind();
        let start = Py::new(
            py,
            StartRead(Mutex::new(Some(Box::new(move |py| {
                let task =
                    pyo3_async_runtimes::tokio::get_runtime().spawn(scope(locals, async move {
                        let result = future.await;
                        Python::attach(move |py| {
                            let (value, failed) = match result {
                                Ok(value) => match value.into_pyobject(py) {
                                    Ok(value) => (value.into_any().unbind(), false),
                                    Err(error) => {
                                        (Into::<PyErr>::into(error).into_value(py).into_any(), true)
                                    }
                                },
                                Err(error) => (error.into_value(py).into_any(), true),
                            };
                            *completion.lock().expect("completion lock") =
                                Some(Completion { value, failed });
                            // Cancellation aborts pending work but keeps this delivery
                            // callback, which returns a completed read to its queue.
                            let _ = event_loop
                                .bind(py)
                                .call_method1("call_soon_threadsafe", (delivery,));
                        });
                    }));
                callback_target.bind(py).call_method1(
                    "add_done_callback",
                    (Py::new(py, AbortOnCancel(task.abort_handle()))?,),
                )?;
                Ok(())
            })))),
        )?;
        let receive = RECEIVE_FN.get_or_try_init(py, || {
            PyModule::from_code(py, RECEIVE, c"laser_sdk_receive.py", c"laser_sdk_receive")?
                .getattr("receive")
                .map(Bound::unbind)
        })?;
        receive
            .bind(py)
            .call1((target, Py::new(py, GiveBack(undelivered))?, start))
    }

    fn return_value(py: Python<'_>, undelivered: &SharedUndelivered, value: Py<PyAny>) {
        let taken = undelivered.lock().expect("undelivered lock").take();
        if let Some(give_back) = taken {
            give_back(py, value);
        }
    }
}

pub use portable::future_into_py_returning;

#[cfg(test)]
mod tests {
    use super::{HookLoop, PyHook, call_hook};
    use pyo3::prelude::*;
    use pyo3::types::PyModule;

    fn hooks<'py>(py: Python<'py>, name: &std::ffi::CStr) -> PyResult<Bound<'py, PyModule>> {
        PyModule::from_code(
            py,
            c"import asyncio
import contextvars
request = contextvars.ContextVar('request')
async def read_async():
    await asyncio.sleep(0)
    return request.get('missing')
def read_sync():
    return request.get('missing')
class Observer:
    def observe(self, key):
        return key + ':method'
    def __call__(self, key):
        return key + ':call'
async def observe(key):
    await asyncio.sleep(0)
    return key + ':async'
",
            c"bridge_hooks.py",
            name,
        )
    }

    #[test]
    fn given_a_hook_on_an_unscoped_task_when_called_then_should_run_on_its_registration_loop() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let module = hooks(py, c"bridge_hooks_unscoped")?.unbind();
            pyo3_async_runtimes::tokio::run(py, async move {
                let hook = Python::attach(|py| {
                    PyHook::new(&module.bind(py).getattr("observe")?, "observe", "a hook")
                })?;
                let lane =
                    tokio::spawn(async move { hook.call(|py| ("key",).into_pyobject(py)).await });
                let value = lane.await.expect("lane task")?;
                let value = Python::attach(|py| value.extract::<String>(py))?;
                assert_eq!(value, "key:async");
                Ok(())
            })
        })
        .expect("an unscoped hook falls back to its registration loop");
    }

    #[test]
    fn given_a_caller_context_when_hooks_run_then_should_see_its_context_variables() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let module = hooks(py, c"bridge_hooks_context")?;
            module
                .getattr("request")?
                .call_method1("set", ("caller",))?;
            let module = module.unbind();
            pyo3_async_runtimes::tokio::run(py, async move {
                for name in ["read_async", "read_sync"] {
                    let value = call_hook(&HookLoop::default(), |call| {
                        call.call(&module.bind(call.py()).getattr(name)?, ())
                    })
                    .await?;
                    let value = Python::attach(|py| value.extract::<String>(py))?;
                    assert_eq!(value, "caller", "{name} lost the caller context");
                }
                Ok(())
            })
        })
        .expect("hooks see the caller context");
    }

    #[test]
    fn given_hook_shapes_when_wrapped_then_should_prefer_the_method_and_accept_callables() {
        Python::initialize();
        Python::attach(|py| -> PyResult<()> {
            let module = hooks(py, c"bridge_hooks_shapes")?;
            let observer = module.getattr("Observer")?.call0()?;
            let lambda = py.eval(c"lambda key: key + ':lambda'", None, None)?;
            let hooks = [
                PyHook::new(&observer, "observe", "a deduplicator")?,
                PyHook::new(&lambda, "observe", "a deduplicator")?,
            ];
            let refused = PyHook::new(
                &7_i32.into_pyobject(py)?.into_any(),
                "observe",
                "a deduplicator",
            );
            let error = refused
                .err()
                .expect("a non-callable without the method is refused");
            assert!(error.is_instance_of::<crate::errors::InvalidError>(py));
            pyo3_async_runtimes::tokio::run(py, async move {
                let mut values = Vec::new();
                for hook in hooks {
                    let value = hook.call(|py| ("key",).into_pyobject(py)).await?;
                    values.push(Python::attach(|py| value.extract::<String>(py))?);
                }
                assert_eq!(values, ["key:method", "key:lambda"]);
                Ok(())
            })
        })
        .expect("hook shapes resolve");
    }
}

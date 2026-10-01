use pyo3::prelude::*;
use std::future::Future;

/// Takes back a successful result its cancelled Python future never received.
pub type Undelivered = Box<dyn FnOnce(Python<'_>, Py<PyAny>) + Send>;

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

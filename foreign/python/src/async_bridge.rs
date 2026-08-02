use pyo3::prelude::*;
use std::future::Future;

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

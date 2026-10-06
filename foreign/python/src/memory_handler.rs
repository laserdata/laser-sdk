use crate::agent::PyAgentMessage;
use crate::async_bridge::future_into_py;
use crate::errors::to_pyerr;
use crate::memory::{Backend, PyMemory, RememberOptions, map_kind};
use laser_sdk::memory::MemoryKind;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::into_future;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

#[gen_stub_pyclass]
#[pyclass(name = "MemoryHandler")]
pub struct PyMemoryHandler {
    handler: Py<PyAny>,
    memory: Backend,
    kind: Option<MemoryKind>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMemoryHandler {
    #[new]
    fn new(handler: Py<PyAny>, memory: &PyMemory) -> Self {
        Self {
            handler,
            memory: memory.backend(),
            kind: None,
        }
    }

    /// Remember successful messages under their conversation. Memory failures do not repeat the handler.
    fn auto_remember<'py>(
        mut slf: PyRefMut<'py, Self>,
        kind: &str,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.kind = Some(map_kind(kind)?);
        Ok(slf)
    }

    fn __call__<'py>(
        &self,
        py: Python<'py>,
        context: Py<PyAny>,
        message: Py<PyAgentMessage>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handler = self.handler.clone_ref(py);
        let memory = self.memory.clone();
        let kind = self.kind;
        let inner = message.borrow(py).inner.clone();
        future_into_py(py, async move {
            let pending =
                Python::attach(|py| into_future(handler.bind(py).call1((context, message))?))?;
            pending.await?;
            if let Some(kind) = kind {
                let _ = memory
                    .remember(
                        RememberOptions {
                            agent: inner.provenance.agent,
                            conversation: Some(inner.provenance.conversation_id),
                            kind,
                            ..RememberOptions::default()
                        },
                        inner.payload,
                    )
                    .await
                    .map_err(to_pyerr);
            }
            Ok(())
        })
    }
}

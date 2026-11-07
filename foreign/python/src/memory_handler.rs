use crate::agent::PyAgentMessage;
use crate::agent_runtime::PyAgentCtx;
use crate::async_bridge::{Awaitable, PyHook, awaitable};
use crate::memory::{Backend, PyMemory, RememberOptions, map_kind};
use laser_sdk::memory::MemoryKind;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// Wraps an agent handler, a callable or an object with `handle(ctx,
/// message)` that returns directly or through an awaitable. Calling the
/// wrapper runs the handler, and cancelling that call cancels it.
#[gen_stub_pyclass]
#[pyclass(name = "MemoryHandler")]
pub struct PyMemoryHandler {
    handler: PyHook,
    memory: Backend,
    kind: Option<MemoryKind>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMemoryHandler {
    #[new]
    fn new(inner: &Bound<'_, PyAny>, memory: &PyMemory) -> PyResult<Self> {
        Ok(Self {
            handler: PyHook::new(inner, "handle", "a memory handler's handler")?,
            memory: memory.backend(),
            kind: None,
        })
    }

    /// Remember successful messages under their conversation, attributed to the handling agent. A memory failure is logged as a warning and does not repeat the handler.
    fn auto_remember<'py>(
        mut slf: PyRefMut<'py, Self>,
        #[gen_stub(override_type(
            type_repr = "typing.Literal['fact', 'message', 'summary', 'entity', 'feedback', 'procedure']",
            imports = ("typing",)
        ))]
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
    ) -> PyResult<Awaitable<'py, ()>> {
        let handler = self.handler.clone();
        let memory = self.memory.clone();
        let kind = self.kind;
        let inner = message.borrow(py).inner.clone();
        let agent = context
            .bind(py)
            .cast::<PyAgentCtx>()
            .ok()
            .and_then(|context| context.borrow().agent().cloned());
        awaitable(py, async move {
            handler
                .call(|py| (context, message).into_pyobject(py))
                .await?;
            if let Some(kind) = kind {
                let conversation = inner.provenance.conversation_id;
                if let Err(error) = memory
                    .remember(
                        RememberOptions {
                            agent,
                            conversation: Some(conversation),
                            kind,
                            ..RememberOptions::default()
                        },
                        inner.payload,
                    )
                    .await
                {
                    log::warn!(
                        "Memory handler could not remember the turn of conversation {conversation}: {error}"
                    );
                }
            }
            Ok(())
        })
    }
}

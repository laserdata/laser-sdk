use crate::async_bridge::{HookLoop, call_hook};
use async_trait::async_trait;
use laser_sdk::LaserError;
use laser_sdk::blob::BlobStore;
use pyo3::prelude::*;

// A `BlobStore` backed by a Python object exposing `get(reference: str) ->
// bytes` and, for the publish side, `put(data: bytes) -> str`, each sync or
// async. Every caller awaits it from a scoped task, so the caller's loop runs
// the coroutines, and dropping the caller cancels them. An exception keeps its
// SDK class. The digest verification of a resolved body stays in the SDK's
// canonical `resolve_body`, so a Python resolver cannot skip the integrity check.
pub(crate) struct PyBlobStore {
    pub(crate) hooks: Py<PyAny>,
}

#[async_trait]
impl BlobStore for PyBlobStore {
    async fn put(&self, payload: Vec<u8>) -> Result<String, LaserError> {
        let value = call_hook(&HookLoop::default(), |call| {
            let py = call.py();
            call.call_method(
                self.hooks.bind(py),
                "put",
                (pyo3::types::PyBytes::new(py, &payload),),
            )
        })
        .await
        .map_err(crate::errors::from_callback_error)?;
        Python::attach(|py| value.bind(py).extract::<String>()).map_err(|error| {
            LaserError::HandlerConfig(format!("blob store put must return a str: {error}"))
        })
    }

    async fn get(&self, reference: &str) -> Result<Vec<u8>, LaserError> {
        let reference = reference.to_owned();
        let value = call_hook(&HookLoop::default(), |call| {
            call.call_method(self.hooks.bind(call.py()), "get", (reference,))
        })
        .await
        .map_err(crate::errors::from_callback_error)?;
        Python::attach(|py| crate::convert::payload_bytes(value.bind(py))).map_err(|error| {
            LaserError::HandlerConfig(format!("blob store get must return bytes: {error}"))
        })
    }
}

use async_trait::async_trait;
use laser_sdk::LaserError;
use laser_sdk::blob::BlobStore;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::into_future;

// A `BlobStore` backed by a Python object exposing `async def get(reference: str)
// -> bytes` and (for the publish side) `async def put(data: bytes) -> str`.
// Runs inside the scoped resolve task, so the captured event loop schedules the
// coroutines. The digest verification of a resolved body stays in the SDK's
// canonical `resolve_body`, so a Python resolver cannot skip the integrity check.
pub(crate) struct PyBlobStore {
    pub(crate) hooks: Py<PyAny>,
}

#[async_trait]
impl BlobStore for PyBlobStore {
    async fn put(&self, payload: Vec<u8>) -> Result<String, LaserError> {
        let future = Python::attach(|py| -> PyResult<_> {
            let coroutine = self
                .hooks
                .bind(py)
                .call_method1("put", (pyo3::types::PyBytes::new(py, &payload),))?;
            into_future(coroutine)
        })
        .map_err(|error| LaserError::Codec(format!("blob store put: {error}")))?;
        let value = future
            .await
            .map_err(|error| LaserError::Codec(format!("blob store put raised: {error}")))?;
        Python::attach(|py| value.bind(py).extract::<String>())
            .map_err(|error| LaserError::Codec(format!("blob store put returned non-str: {error}")))
    }

    async fn get(&self, reference: &str) -> Result<Vec<u8>, LaserError> {
        let reference = reference.to_owned();
        let future = Python::attach(|py| -> PyResult<_> {
            let coroutine = self.hooks.bind(py).call_method1("get", (reference,))?;
            into_future(coroutine)
        })
        .map_err(|error| LaserError::Codec(format!("blob store get: {error}")))?;
        let value = future
            .await
            .map_err(|error| LaserError::Codec(format!("blob store get raised: {error}")))?;
        Python::attach(|py| value.bind(py).extract::<Vec<u8>>()).map_err(|error| {
            LaserError::Codec(format!("blob store get returned non-payload: {error}"))
        })
    }
}

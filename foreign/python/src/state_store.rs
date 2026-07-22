use crate::convert::payload_bytes;
use crate::errors::to_pyerr;
use laser_sdk::state_store::{FileStore, InMemoryStore, StateStore};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::sync::Arc;

/// An in-memory `StateStore`: `get` / `set` / `delete` over a process-local map.
/// Fast and self-contained, lost on restart. The same vocabulary as `Kv`, so a
/// handler written against this drops onto the managed store unchanged. Values
/// read back as `bytes`.
#[gen_stub_pyclass]
#[pyclass(name = "InMemoryStore", frozen)]
pub struct PyInMemoryStore {
    inner: Arc<InMemoryStore>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyInMemoryStore {
    #[new]
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemoryStore::new()),
        }
    }

    /// The value bytes at `key`, or `None` if absent.
    fn get<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { inner.get(&key).await.map_err(to_pyerr) })
    }

    /// Store `value` (`str`, `bytes`, or `bytearray`) at `key`.
    fn set<'py>(
        &self,
        py: Python<'py>,
        key: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let value = payload_bytes(value)?;
        future_into_py(
            py,
            async move { inner.set(&key, value).await.map_err(to_pyerr) },
        )
    }

    /// Remove `key`. A no-op if it was already absent.
    fn delete<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(
            py,
            async move { inner.delete(&key).await.map_err(to_pyerr) },
        )
    }
}

/// A file-backed `StateStore` rooted at one directory: each key is hex-encoded
/// into a file name, so any key is safe and no path traversal is possible.
/// Durable across restarts, suited to an on-box disk mounted onto a deployment.
/// Same `get` / `set` / `delete` vocabulary as `Kv` and `InMemoryStore`.
#[gen_stub_pyclass]
#[pyclass(name = "FileStore", frozen)]
pub struct PyFileStore {
    inner: Arc<FileStore>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFileStore {
    #[new]
    fn new(root: String) -> Self {
        Self {
            inner: Arc::new(FileStore::new(root)),
        }
    }

    /// The value bytes at `key`, or `None` if absent.
    fn get<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move { inner.get(&key).await.map_err(to_pyerr) })
    }

    /// Store `value` (`str`, `bytes`, or `bytearray`) at `key`.
    fn set<'py>(
        &self,
        py: Python<'py>,
        key: String,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let value = payload_bytes(value)?;
        future_into_py(
            py,
            async move { inner.set(&key, value).await.map_err(to_pyerr) },
        )
    }

    /// Remove `key`. A no-op if it was already absent.
    fn delete<'py>(&self, py: Python<'py>, key: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(
            py,
            async move { inner.delete(&key).await.map_err(to_pyerr) },
        )
    }
}

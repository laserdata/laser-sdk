use crate::async_bridge::future_into_py;
use crate::convert::payload_bytes;
use crate::errors::to_pyerr;
use laser_sdk::LaserError;
use laser_sdk::state_store::{FileStore, InMemoryStore, StateStore};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::sync::Arc;

/// The durable point-store seam for agent state: `get` / `set` / `delete`, the
/// same vocabulary as `Kv`. `InMemoryStore` and `FileStore` are the
/// self-contained stores. Values read back as `bytes`.
#[gen_stub_pyclass]
#[pyclass(name = "StateStore", subclass, frozen)]
pub struct PyStateStore {
    inner: Store,
}

/// An in-memory `StateStore` over a process-local map. Fast and
/// self-contained, lost on restart.
#[gen_stub_pyclass]
#[pyclass(name = "InMemoryStore", extends = PyStateStore, frozen)]
pub struct PyInMemoryStore;

/// A file-backed `StateStore` rooted at one directory: each key is hex-encoded
/// into a file name, so any key is safe and no path traversal is possible.
/// Durable across restarts, suited to an on-box disk mounted onto a deployment.
#[gen_stub_pyclass]
#[pyclass(name = "FileStore", extends = PyStateStore, frozen)]
pub struct PyFileStore;

#[gen_stub_pymethods]
#[pymethods]
impl PyStateStore {
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

#[gen_stub_pymethods]
#[pymethods]
impl PyInMemoryStore {
    #[new]
    fn new() -> PyClassInitializer<PyInMemoryStore> {
        PyClassInitializer::from(PyStateStore {
            inner: Store::Memory(Arc::new(InMemoryStore::new())),
        })
        .add_subclass(PyInMemoryStore)
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFileStore {
    #[new]
    fn new(root: String) -> PyClassInitializer<PyFileStore> {
        PyClassInitializer::from(PyStateStore {
            inner: Store::File(Arc::new(FileStore::new(root))),
        })
        .add_subclass(PyFileStore)
    }
}

// The concrete Rust store behind a Python `StateStore`, so the shared methods
// delegate to that store's own trait implementation.
#[derive(Clone)]
enum Store {
    Memory(Arc<InMemoryStore>),
    File(Arc<FileStore>),
}

impl Store {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, LaserError> {
        match self {
            Self::Memory(store) => store.get(key).await,
            Self::File(store) => store.get(key).await,
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), LaserError> {
        match self {
            Self::Memory(store) => store.set(key, value).await,
            Self::File(store) => store.set(key, value).await,
        }
    }

    async fn delete(&self, key: &str) -> Result<(), LaserError> {
        match self {
            Self::Memory(store) => store.delete(key).await,
            Self::File(store) => store.delete(key).await,
        }
    }
}

use crate::errors::InvalidError;
use laser_sdk::sign::{KeyRegistry, SigningKey};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::sync::{Arc, Mutex};

/// An Ed25519 signing key created from a 32-byte secret seed.
#[gen_stub_pyclass]
#[pyclass(name = "SigningKey", frozen)]
pub struct PySigningKey {
    pub(crate) inner: Arc<SigningKey>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PySigningKey {
    #[new]
    fn new(secret: Vec<u8>) -> PyResult<Self> {
        let secret: [u8; 32] = secret.try_into().map_err(|_| {
            InvalidError::new_err("an Ed25519 signing key requires exactly 32 secret bytes")
        })?;
        Ok(Self {
            inner: Arc::new(SigningKey::from_bytes(&secret)),
        })
    }

    /// The 8-byte identifier stamped on signatures produced by this key.
    #[getter]
    fn key_id(&self) -> Vec<u8> {
        self.inner.key_id().to_vec()
    }
}

/// Enrolled verifying keys bound to authenticated principal names.
#[gen_stub_pyclass]
#[pyclass(name = "KeyRegistry")]
pub struct PyKeyRegistry {
    inner: Mutex<KeyRegistry>,
}

impl PyKeyRegistry {
    pub(crate) fn snapshot(&self) -> Arc<KeyRegistry> {
        Arc::new(
            self.inner
                .lock()
                .expect("python key registry mutex is not poisoned")
                .clone(),
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKeyRegistry {
    #[new]
    fn new() -> Self {
        Self {
            inner: Mutex::new(KeyRegistry::new()),
        }
    }

    /// Enroll `key` as an agent signing key bound to `principal`.
    fn enroll(&self, principal: String, key: &PySigningKey) {
        self.inner
            .lock()
            .expect("python key registry mutex is not poisoned")
            .enroll(principal, key.inner.verifying_key());
    }

    /// Enroll `key` as an operator key for privileged control facts.
    fn enroll_operator(&self, principal: String, key: &PySigningKey) {
        self.inner
            .lock()
            .expect("python key registry mutex is not poisoned")
            .enroll_operator(principal, key.inner.verifying_key());
    }
}

use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::sign::{KeyKind, KeyRecord, KeyRegistry, KvKeyRegistry, SigningKey};
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

    /// The 32-byte public verifying key safe to share with a registry operator.
    #[getter]
    fn verifying_key(&self) -> Vec<u8> {
        self.inner.verifying_key().as_bytes().to_vec()
    }
}

/// Enrolled verifying keys bound to authenticated principal names.
#[gen_stub_pyclass]
#[pyclass(name = "KeyRegistry")]
pub struct PyKeyRegistry {
    inner: Mutex<KeyRegistry>,
}

impl PyKeyRegistry {
    fn from_inner(inner: KeyRegistry) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }

    pub(crate) fn snapshot(&self) -> Arc<KeyRegistry> {
        Arc::new(
            self.inner
                .lock()
                .expect("python key registry mutex is not poisoned")
                .clone(),
        )
    }
}

/// A lifecycle-aware verifying key enrolled in a managed key registry.
#[gen_stub_pyclass]
#[pyclass(name = "KeyRecord", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyKeyRecord {
    inner: KeyRecord,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKeyRecord {
    #[new]
    #[pyo3(signature = (principal, verifying_key, *, kind = "agent", valid_from_micros = 0, valid_to_micros = None, revoked = false))]
    fn new(
        principal: String,
        verifying_key: Vec<u8>,
        kind: &str,
        valid_from_micros: u64,
        valid_to_micros: Option<u64>,
        revoked: bool,
    ) -> PyResult<Self> {
        let kind = match kind {
            "agent" => KeyKind::Agent,
            "operator" => KeyKind::Operator,
            _ => {
                return Err(InvalidError::new_err(
                    "key kind must be 'agent' or 'operator'",
                ));
            }
        };
        if valid_to_micros.is_some_and(|end| end <= valid_from_micros) {
            return Err(InvalidError::new_err(
                "key validity end must be after its start",
            ));
        }
        let mut inner = KeyRecord::from_verifying_bytes(principal, &verifying_key, kind)
            .map_err(to_pyerr)?
            .valid_window(valid_from_micros, valid_to_micros);
        if revoked {
            inner = inner.revoked();
        }
        Ok(Self { inner })
    }

    #[getter]
    fn key_id(&self) -> Vec<u8> {
        self.inner.key_id()
    }

    #[getter]
    fn principal(&self) -> &str {
        &self.inner.principal
    }

    #[getter]
    fn verifying_key(&self) -> Vec<u8> {
        self.inner.verifying.as_bytes().to_vec()
    }

    #[getter]
    fn kind(&self) -> &'static str {
        match self.inner.kind {
            KeyKind::Agent => "agent",
            KeyKind::Operator => "operator",
        }
    }

    #[getter]
    fn valid_from_micros(&self) -> u64 {
        self.inner.valid_from_micros
    }

    #[getter]
    fn valid_to_micros(&self) -> Option<u64> {
        self.inner.valid_to_micros
    }

    #[getter]
    fn revoked(&self) -> bool {
        self.inner.revoked
    }

    fn revoke(&self) -> Self {
        Self {
            inner: self.inner.clone().revoked(),
        }
    }
}

/// The managed lifecycle-aware key registry stored in `agent.keys`.
#[gen_stub_pyclass]
#[pyclass(name = "KvKeyRegistry", frozen)]
pub struct PyKvKeyRegistry {
    inner: KvKeyRegistry,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvKeyRegistry {
    #[new]
    #[pyo3(signature = (laser, namespace = None))]
    fn new(laser: &PyLaser, namespace: Option<String>) -> Self {
        let inner = match namespace {
            Some(namespace) => KvKeyRegistry::in_namespace(laser.inner.clone(), namespace),
            None => KvKeyRegistry::new(laser.inner.clone()),
        };
        Self { inner }
    }

    fn enroll<'py>(&self, py: Python<'py>, record: &PyKeyRecord) -> PyResult<Bound<'py, PyAny>> {
        let registry = self.inner.clone();
        let record = record.inner.clone();
        future_into_py(py, async move {
            registry.enroll_record(&record).await.map_err(to_pyerr)
        })
    }

    fn revoke<'py>(&self, py: Python<'py>, key_id: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let registry = self.inner.clone();
        future_into_py(py, async move {
            registry.revoke(key_id).await.map_err(to_pyerr)
        })
    }

    fn registry<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let registry = self.inner.clone();
        future_into_py(py, async move {
            registry
                .registry()
                .await
                .map(PyKeyRegistry::from_inner)
                .map_err(to_pyerr)
        })
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

    /// Enroll a public agent verifying key bound to `principal`.
    fn enroll(&self, principal: String, verifying_key: Vec<u8>) -> PyResult<()> {
        let record = KeyRecord::from_verifying_bytes(principal, &verifying_key, KeyKind::Agent)
            .map_err(to_pyerr)?;
        self.inner
            .lock()
            .expect("python key registry mutex is not poisoned")
            .enroll_record(record);
        Ok(())
    }

    /// Enroll a public operator verifying key for privileged control facts.
    fn enroll_operator(&self, principal: String, verifying_key: Vec<u8>) -> PyResult<()> {
        let record = KeyRecord::from_verifying_bytes(principal, &verifying_key, KeyKind::Operator)
            .map_err(to_pyerr)?;
        self.inner
            .lock()
            .expect("python key registry mutex is not poisoned")
            .enroll_record(record);
        Ok(())
    }
}

use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::sign::{
    KeyKind, KeyRecord, KeyRegistry, KvKeyRegistry, SigningKey, VerifiedPrincipal,
};
use laser_sdk::wire::agent::{AgentEnvelope, SignatureContext};
use laser_sdk::wire::content::ContentType;
use pyo3::prelude::*;
use pyo3::types::{PyByteArray, PyBytes, PyMemoryView};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

// An envelope arrives as the CBOR bytes on the log (`AgentMessage.payload`)
// or as the dict `AgentMessage.envelope` returns.
pub(crate) fn envelope_of(value: &Bound<'_, PyAny>) -> PyResult<AgentEnvelope> {
    if value.is_instance_of::<PyBytes>()
        || value.is_instance_of::<PyByteArray>()
        || value.is_instance_of::<PyMemoryView>()
    {
        let bytes = crate::convert::payload_bytes(value)?;
        return laser_sdk::wire::framing::decode_named(&bytes)
            .map_err(|error| InvalidError::new_err(format!("not an AGDX envelope: {error}")));
    }
    crate::convert::py_to_de(value)
}

fn signature_context(
    content_type: Option<String>,
    agent_version: Option<u32>,
) -> PyResult<SignatureContext> {
    let content_type = content_type
        .map(|value| {
            ContentType::from_str(&value)
                .map(|content_type| content_type.code())
                .map_err(|_| InvalidError::new_err(format!("unknown content type '{value}'")))
        })
        .transpose()?;
    Ok(SignatureContext {
        content_type,
        agent_version,
    })
}

fn key_kind(kind: &str) -> PyResult<KeyKind> {
    match kind {
        "agent" => Ok(KeyKind::Agent),
        "operator" => Ok(KeyKind::Operator),
        _ => Err(InvalidError::new_err(
            "key kind must be 'agent' or 'operator'",
        )),
    }
}

fn key_kind_word(kind: KeyKind) -> &'static str {
    match kind {
        KeyKind::Agent => "agent",
        KeyKind::Operator => "operator",
    }
}

/// A verified signer: the enrolled principal and the kind of key it holds
/// (`agent` or `operator`).
#[gen_stub_pyclass]
#[pyclass(name = "VerifiedPrincipal", frozen, skip_from_py_object)]
pub struct PyVerifiedPrincipal {
    inner: VerifiedPrincipal,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyVerifiedPrincipal {
    /// The authenticated principal the key was enrolled under.
    #[getter]
    fn principal(&self) -> &str {
        &self.inner.principal
    }

    /// Whether the key is an `operator` or an `agent` key.
    #[getter]
    fn kind(&self) -> &'static str {
        key_kind_word(self.inner.kind)
    }

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    fn __repr__(&self) -> String {
        format!(
            "VerifiedPrincipal(principal={:?}, kind={:?})",
            self.inner.principal,
            key_kind_word(self.inner.kind)
        )
    }
}

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
        Self::from_bytes(secret)
    }

    /// A signing key from its 32-byte secret seed, the same as the constructor.
    #[staticmethod]
    fn from_bytes(secret: Vec<u8>) -> PyResult<Self> {
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

    /// Sign `envelope` (the envelope bytes or dict) and return the detached
    /// signature dict (`scheme`, `key_id`, `bytes`). The preimage covers the
    /// envelope alone.
    fn sign(&self, py: Python<'_>, envelope: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let signature = self.inner.sign(&envelope_of(envelope)?).map_err(to_pyerr)?;
        crate::convert::ser_to_py(py, &signature)
    }

    /// Sign `envelope` binding the interpretation context into the preimage:
    /// `content_type` (the `agdx.ct` codec, for example 'cbor') and
    /// `agent_version` (the `agdx.av` wire version). A verifier must observe the
    /// same headers, so an intermediary cannot flip the codec or version.
    #[pyo3(signature = (envelope, *, content_type=None, agent_version=None))]
    fn sign_with_context(
        &self,
        py: Python<'_>,
        envelope: &Bound<'_, PyAny>,
        content_type: Option<String>,
        agent_version: Option<u32>,
    ) -> PyResult<Py<PyAny>> {
        let context = signature_context(content_type, agent_version)?;
        let signature = self
            .inner
            .sign_with_context(&envelope_of(envelope)?, context)
            .map_err(to_pyerr)?;
        crate::convert::ser_to_py(py, &signature)
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
    #[pyo3(signature = (principal, verifying, *, kind = "agent", valid_from_micros = 0, valid_to_micros = None, revoked = false))]
    fn new(
        principal: String,
        verifying: Vec<u8>,
        kind: &str,
        valid_from_micros: u64,
        valid_to_micros: Option<u64>,
        revoked: bool,
    ) -> PyResult<Self> {
        let kind = key_kind(kind)?;
        if valid_to_micros.is_some_and(|end| end <= valid_from_micros) {
            return Err(InvalidError::new_err(
                "key validity end must be after its start",
            ));
        }
        let mut inner = KeyRecord::from_verifying_bytes(principal, &verifying, kind)
            .map_err(to_pyerr)?
            .valid_window(valid_from_micros, valid_to_micros);
        if revoked {
            inner = inner.revoked();
        }
        Ok(Self { inner })
    }

    /// An always-valid agent key from its 32-byte public verifying key.
    #[staticmethod]
    fn agent(principal: String, verifying: Vec<u8>) -> PyResult<Self> {
        Self::from_verifying_bytes(principal, verifying, "agent")
    }

    /// An always-valid record from a 32-byte public verifying key of `kind`
    /// (`agent` or `operator`).
    #[staticmethod]
    fn from_verifying_bytes(principal: String, verifying: Vec<u8>, kind: &str) -> PyResult<Self> {
        let inner = KeyRecord::from_verifying_bytes(principal, &verifying, key_kind(kind)?)
            .map_err(to_pyerr)?;
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

    /// The 32-byte public verifying key.
    #[getter]
    fn verifying(&self) -> Vec<u8> {
        self.inner.verifying.as_bytes().to_vec()
    }

    #[getter]
    fn kind(&self) -> &'static str {
        key_kind_word(self.inner.kind)
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

    fn enroll<'py>(
        &self,
        py: Python<'py>,
        principal: String,
        verifying: Vec<u8>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let registry = self.inner.clone();
        let record = KeyRecord::from_verifying_bytes(principal, &verifying, KeyKind::Agent)
            .map_err(to_pyerr)?;
        future_into_py(py, async move {
            registry
                .enroll(&record.principal, &record.verifying)
                .await
                .map_err(to_pyerr)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn enroll_record<'py>(
        &self,
        py: Python<'py>,
        record: &PyKeyRecord,
    ) -> PyResult<Bound<'py, PyAny>> {
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
    fn enroll(&self, principal: String, verifying: Vec<u8>) -> PyResult<()> {
        let record = KeyRecord::from_verifying_bytes(principal, &verifying, KeyKind::Agent)
            .map_err(to_pyerr)?;
        self.inner
            .lock()
            .expect("python key registry mutex is not poisoned")
            .enroll_record(record);
        Ok(())
    }

    /// Enroll a public operator verifying key for privileged control facts.
    fn enroll_operator(&self, principal: String, verifying: Vec<u8>) -> PyResult<()> {
        let record = KeyRecord::from_verifying_bytes(principal, &verifying, KeyKind::Operator)
            .map_err(to_pyerr)?;
        self.inner
            .lock()
            .expect("python key registry mutex is not poisoned")
            .enroll_record(record);
        Ok(())
    }

    /// Enroll a `KeyRecord` with its kind and lifecycle. A later enroll under
    /// the same key id replaces it.
    fn enroll_record(&self, record: &PyKeyRecord) {
        self.inner
            .lock()
            .expect("python key registry mutex is not poisoned")
            .enroll_record(record.inner.clone());
    }

    /// Verify the envelope's signature and return the enrolled principal.
    /// Raises on an unsigned envelope, an unknown or revoked key, or a failed
    /// check. Ignores the validity window. `verify_at` checks it.
    fn verify(&self, envelope: &Bound<'_, PyAny>) -> PyResult<String> {
        let envelope = envelope_of(envelope)?;
        self.inner
            .lock()
            .expect("python key registry mutex is not poisoned")
            .verify(&envelope)
            .map_err(to_pyerr)
    }

    /// Verify as of `at_micros` and return the `VerifiedPrincipal`. Raises
    /// also when the key is outside its validity window at that time.
    fn verify_at(
        &self,
        envelope: &Bound<'_, PyAny>,
        at_micros: u64,
    ) -> PyResult<PyVerifiedPrincipal> {
        let envelope = envelope_of(envelope)?;
        let inner = self
            .inner
            .lock()
            .expect("python key registry mutex is not poisoned")
            .verify_at(&envelope, at_micros)
            .map_err(to_pyerr)?;
        Ok(PyVerifiedPrincipal { inner })
    }

    /// Verify a log-resident envelope against the interpretation headers
    /// observed on its record (`content_type`, `agent_version`) and the record
    /// timestamp `at_micros`. A signed context that differs from the observed
    /// headers is rejected before the cryptographic check.
    #[pyo3(signature = (envelope, at_micros, *, content_type=None, agent_version=None))]
    fn verify_observed_at(
        &self,
        envelope: &Bound<'_, PyAny>,
        at_micros: u64,
        content_type: Option<String>,
        agent_version: Option<u32>,
    ) -> PyResult<PyVerifiedPrincipal> {
        let envelope = envelope_of(envelope)?;
        let observed = signature_context(content_type, agent_version)?;
        let inner = self
            .inner
            .lock()
            .expect("python key registry mutex is not poisoned")
            .verify_observed_at(&envelope, &observed, at_micros)
            .map_err(to_pyerr)?;
        Ok(PyVerifiedPrincipal { inner })
    }
}

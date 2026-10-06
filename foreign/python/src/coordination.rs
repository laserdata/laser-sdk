use crate::async_bridge::{HookLoop, call_hook, future_into_py};
use crate::convert::{duration_seconds, payload_bytes, py_to_de, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use crate::kv::PyLease;
use async_trait::async_trait;
use laser_sdk::LaserError;
use laser_sdk::kv::{
    AmbiguousMutationRecovery, DedicatedKvTransport, DynManagedKvTransport, FencedLeaseClient,
    KvCasFenced, KvGet, KvLease, KvLeaseRenew, KvRelease, ManagedKvTransport, PreparedMutation,
    SharedKvTransport,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::sync::Arc;

struct Transport(SharedKvTransport);

impl ManagedKvTransport for Transport {
    async fn ready(&self) -> Result<(), LaserError> {
        self.0.as_ref().ready().await
    }

    async fn send(&self, code: u32, frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
        self.0.as_ref().send(code, frame).await
    }

    async fn reset(&self) {
        self.0.as_ref().reset().await;
    }

    async fn close(&self) {
        self.0.as_ref().close().await;
    }
}

type Client = FencedLeaseClient<Transport>;

// A transport over a Python object. Its methods can be sync or async, and an
// abandoned call, such as a send past its attempt timeout, cancels its task.
struct HookTransport {
    hooks: Py<PyAny>,
    fallback: HookLoop,
}

impl HookTransport {
    async fn call(
        &self,
        name: &str,
        code: Option<u32>,
        frame: &[u8],
    ) -> Result<Py<PyAny>, LaserError> {
        call_hook(&self.fallback, |call| {
            let py = call.py();
            let hooks = self.hooks.bind(py);
            match code {
                Some(code) => call.call_method(hooks, name, (code, PyBytes::new(py, frame))),
                None if name == "ready" && !hooks.hasattr(name)? => Ok(py.None()),
                None => call.call_method(hooks, name, ()),
            }
        })
        .await
        .map_err(crate::errors::from_callback_error)
    }
}

#[async_trait]
impl DynManagedKvTransport for HookTransport {
    async fn ready(&self) -> Result<(), LaserError> {
        self.call("ready", None, &[]).await.map(|_| ())
    }

    async fn send(&self, code: u32, frame: Vec<u8>) -> Result<Vec<u8>, LaserError> {
        let value = self.call("send", Some(code), &frame).await?;
        Python::attach(|py| payload_bytes(value.bind(py)))
            .map_err(crate::errors::from_callback_error)
    }

    async fn reset(&self) {
        if let Err(error) = self.call("reset", None, &[]).await {
            log::warn!("coordination transport reset failed: {error}");
        }
    }

    async fn close(&self) {
        let close = Python::attach(|py| self.hooks.bind(py).hasattr("close")).unwrap_or(false);
        if close {
            if let Err(error) = self.call("close", None, &[]).await {
                log::warn!("coordination transport close failed: {error}");
            }
        } else {
            DynManagedKvTransport::reset(self).await;
        }
    }
}

#[gen_stub_pyclass]
#[pyclass(name = "DedicatedKvTransport", frozen)]
pub struct PyDedicatedKvTransport {
    inner: Arc<DedicatedKvTransport>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyDedicatedKvTransport {
    #[new]
    fn new(connection_string: String) -> Self {
        Self {
            inner: Arc::new(DedicatedKvTransport::new(connection_string)),
        }
    }

    fn ready<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let transport = self.inner.clone();
        future_into_py(py, async move {
            ManagedKvTransport::ready(transport.as_ref())
                .await
                .map_err(to_pyerr)
        })
    }

    fn send<'py>(
        &self,
        py: Python<'py>,
        code: u32,
        frame: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let transport = self.inner.clone();
        let frame = payload_bytes(frame)?;
        future_into_py(py, async move {
            let reply = ManagedKvTransport::send(transport.as_ref(), code, frame)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| Ok(PyBytes::new(py, &reply).unbind()))
        })
    }

    fn reset<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let transport = self.inner.clone();
        future_into_py(py, async move {
            ManagedKvTransport::reset(transport.as_ref()).await;
            Ok(())
        })
    }

    /// Close this transport permanently. `reset` permits later reuse.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let transport = self.inner.clone();
        future_into_py(py, async move {
            transport.close().await;
            Ok(())
        })
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(slf) })
    }

    #[pyo3(signature = (_exc_type, _exc_value, _traceback))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let transport = self.inner.clone();
        future_into_py(py, async move {
            transport.close().await;
            Ok(false)
        })
    }
}

#[gen_stub_pyclass]
#[pyclass(name = "PreparedMutation", frozen)]
pub struct PyPreparedMutation {
    inner: Arc<PreparedMutation>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyPreparedMutation {
    #[getter]
    fn operation_id(&self) -> u128 {
        self.inner.operation_id()
    }

    /// The recovery an ambiguous result of this mutation requires.
    #[getter]
    fn ambiguous_recovery(&self) -> PyAmbiguousMutationRecovery {
        PyAmbiguousMutationRecovery {
            inner: self.inner.ambiguous_recovery(),
        }
    }
}

/// The recovery a caller must use when a prepared mutation raises an
/// ambiguous-mutation error. `kind` is `wait_for_lease_expiry` (do not acquire
/// again until the requested lease TTL has elapsed), `repeat_prepared` (repeat
/// the same `PreparedMutation`), or `reconcile_target_precondition` (read the
/// target and reconcile through the fenced CAS precondition).
#[gen_stub_pyclass]
#[pyclass(name = "AmbiguousMutationRecovery", frozen, eq, skip_from_py_object)]
#[derive(Clone, PartialEq)]
pub struct PyAmbiguousMutationRecovery {
    inner: AmbiguousMutationRecovery,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyAmbiguousMutationRecovery {
    /// Wait `ttl_secs`, the requested lease TTL, before acquiring again.
    #[staticmethod]
    fn wait_for_lease_expiry(ttl_secs: f64) -> PyResult<Self> {
        Ok(Self {
            inner: AmbiguousMutationRecovery::WaitForLeaseExpiry(duration_seconds(
                ttl_secs, "ttl_secs",
            )?),
        })
    }

    /// Repeat the exact prepared mutation, keeping its operation id.
    #[staticmethod]
    fn repeat_prepared() -> Self {
        Self {
            inner: AmbiguousMutationRecovery::RepeatPrepared,
        }
    }

    /// Read the target and reconcile through the fenced CAS precondition.
    #[staticmethod]
    fn reconcile_target_precondition() -> Self {
        Self {
            inner: AmbiguousMutationRecovery::ReconcileTargetPrecondition,
        }
    }

    #[getter]
    fn kind(&self) -> &'static str {
        match self.inner {
            AmbiguousMutationRecovery::WaitForLeaseExpiry(_) => "wait_for_lease_expiry",
            AmbiguousMutationRecovery::RepeatPrepared => "repeat_prepared",
            AmbiguousMutationRecovery::ReconcileTargetPrecondition => {
                "reconcile_target_precondition"
            }
        }
    }

    fn __repr__(&self) -> String {
        format!("AmbiguousMutationRecovery({:?})", self.inner)
    }
}

#[gen_stub_pyclass]
#[pyclass(name = "FencedLeaseClient")]
pub struct PyFencedLeaseClient {
    inner: Option<Arc<Client>>,
}

impl PyFencedLeaseClient {
    fn client(&self) -> Arc<Client> {
        self.inner
            .as_ref()
            .expect("the coordination client is retained")
            .clone()
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyFencedLeaseClient {
    /// The transport supplies send(code, frame) and reset(), and optionally ready() and close(), each sync or async.
    /// A send still running when its attempt times out is cancelled before reset() is called. Reset must stop all other in-flight work before it returns.
    #[new]
    fn new(transport: &Bound<'_, PyAny>) -> PyResult<Self> {
        let transport: SharedKvTransport =
            if let Ok(dedicated) = transport.extract::<PyRef<'_, PyDedicatedKvTransport>>() {
                dedicated.inner.clone()
            } else {
                if !transport.hasattr("send")? || !transport.hasattr("reset")? {
                    return Err(InvalidError::new_err(
                        "coordination transport needs send and reset methods",
                    ));
                }
                Arc::new(HookTransport {
                    hooks: transport.clone().unbind(),
                    fallback: HookLoop::capture(transport.py()),
                })
            };
        Ok(Self {
            inner: Some(Arc::new(Client::new(Transport(transport)))),
        })
    }

    #[staticmethod]
    fn connect_dedicated(connection_string: String) -> Self {
        let transport: SharedKvTransport = Arc::new(DedicatedKvTransport::new(connection_string));
        Self {
            inner: Some(Arc::new(Client::new(Transport(transport)))),
        }
    }

    fn with_attempt_timeout(
        mut slf: PyRefMut<'_, Self>,
        seconds: f64,
    ) -> PyResult<PyRefMut<'_, Self>> {
        let timeout = duration_seconds(seconds, "coordination attempt timeout")?;
        let client = slf
            .inner
            .take()
            .expect("the coordination client is retained");
        match Arc::try_unwrap(client) {
            Ok(client) => slf.inner = Some(Arc::new(client.with_attempt_timeout(timeout))),
            Err(client) => {
                slf.inner = Some(client);
                return Err(InvalidError::new_err(
                    "cannot change the timeout during an attempt",
                ));
            }
        }
        Ok(slf)
    }

    fn prepare_acquire(&self, request: &Bound<'_, PyAny>) -> PyResult<PyPreparedMutation> {
        let request: KvLease = py_to_de(request)?;
        let inner = self.client().prepare_acquire(&request).map_err(to_pyerr)?;
        Ok(PyPreparedMutation {
            inner: Arc::new(inner),
        })
    }

    fn prepare_renew(&self, request: &Bound<'_, PyAny>) -> PyResult<PyPreparedMutation> {
        let request: KvLeaseRenew = py_to_de(request)?;
        let inner = self.client().prepare_renew(&request).map_err(to_pyerr)?;
        Ok(PyPreparedMutation {
            inner: Arc::new(inner),
        })
    }

    fn prepare_release(&self, request: &Bound<'_, PyAny>) -> PyResult<PyPreparedMutation> {
        let request: KvRelease = py_to_de(request)?;
        let inner = self.client().prepare_release(&request).map_err(to_pyerr)?;
        Ok(PyPreparedMutation {
            inner: Arc::new(inner),
        })
    }

    fn prepare_cas_fenced(&self, request: &Bound<'_, PyAny>) -> PyResult<PyPreparedMutation> {
        let request: KvCasFenced = py_to_de(request)?;
        let inner = self
            .client()
            .prepare_cas_fenced(&request)
            .map_err(to_pyerr)?;
        Ok(PyPreparedMutation {
            inner: Arc::new(inner),
        })
    }

    fn acquire<'py>(
        &self,
        py: Python<'py>,
        op: &PyPreparedMutation,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client();
        let operation = op.inner.clone();
        future_into_py(py, async move {
            client
                .acquire(&operation)
                .await
                .map(PyLease::from)
                .map_err(to_pyerr)
        })
    }

    fn renew<'py>(&self, py: Python<'py>, op: &PyPreparedMutation) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client();
        let operation = op.inner.clone();
        future_into_py(py, async move {
            client
                .renew(&operation)
                .await
                .map(PyLease::from)
                .map_err(to_pyerr)
        })
    }

    fn release<'py>(
        &self,
        py: Python<'py>,
        op: &PyPreparedMutation,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client();
        let operation = op.inner.clone();
        future_into_py(py, async move {
            client.release(&operation).await.map_err(to_pyerr)
        })
    }

    fn cas_fenced<'py>(
        &self,
        py: Python<'py>,
        op: &PyPreparedMutation,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client();
        let operation = op.inner.clone();
        future_into_py(py, async move {
            client.cas_fenced(&operation).await.map_err(to_pyerr)
        })
    }

    fn get<'py>(&self, py: Python<'py>, request: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client();
        let request: KvGet = py_to_de(request)?;
        future_into_py(py, async move {
            let entry = client.get(&request).await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &entry))
        })
    }

    /// Close this client and retire its transport. Later calls fail before sending.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client();
        future_into_py(py, async move {
            client.close().await;
            Ok(())
        })
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(slf) })
    }

    #[pyo3(signature = (_exc_type, _exc_value, _traceback))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.client();
        future_into_py(py, async move {
            client.close().await;
            Ok(false)
        })
    }
}

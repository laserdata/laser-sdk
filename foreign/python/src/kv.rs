use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{
    codec_decode, codec_encode, duration_ms, json_to_py, payload_bytes, py_to_json, ser_to_py,
};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::kv::{KvEntry, KvMetadata, KvPage, Lease, MutationPosition};
use laser_sdk::laser::Laser;
use laser_sdk::types::ConversationId;
use laser_sdk::wire::agent::SessionRef;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;

// Parse the optional conversation-lens filter: a Crockford conversation id, or
// `None` to scan every row.
fn parse_conversation(conversation: Option<String>) -> PyResult<Option<ConversationId>> {
    match conversation {
        Some(text) => ConversationId::from_str(&text)
            .map(Some)
            .map_err(|error| InvalidError::new_err(format!("invalid conversation id: {error}"))),
        None => Ok(None),
    }
}

impl PyKv {
    /// The Rust handle this Python handle stands for, linked to its session.
    pub(crate) fn rust_handle(&self) -> laser_sdk::kv::Kv {
        self.laser.kv(&self.namespace).linked(self.session.clone())
    }

    pub(crate) fn linked(laser: Laser, namespace: String, session: SessionRef) -> Self {
        Self {
            laser,
            namespace,
            session: Some(session),
        }
    }
}

// Link a key-value handle's writes to the session a `Session.kv` handle
// carries, or leave it unlinked.
trait Linked {
    fn linked(self, session: Option<SessionRef>) -> Self;
}

impl Linked for laser_sdk::kv::Kv {
    fn linked(self, session: Option<SessionRef>) -> Self {
        match session {
            Some(session) => self.in_session(session),
            None => self,
        }
    }
}

#[derive(Clone)]
enum Body {
    // No value supplied yet. Distinct from an explicit empty payload, so
    // forgetting `.payload()` / `.json()` raises instead of silently storing
    // an empty value.
    Unset,
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    Msgpack(serde_json::Value),
}

#[derive(Clone, Copy)]
enum Expect {
    Version(u64),
    Absent,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLaser {
    /// A handle to the managed key-value store scoped to `namespace`. Keys are
    /// `str` (UTF-8) or `bytes`. KV is a managed feature: against Apache Iggy
    /// every operation raises `UnsupportedError`.
    fn kv(&self, namespace: String) -> PyKv {
        PyKv {
            laser: self.inner.clone(),
            namespace,
            session: None,
        }
    }

    /// Every KV namespace holding at least one entry for this caller.
    fn kv_namespaces<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.inner.clone();
        future_into_py(py, async move {
            let list = laser.kv_namespaces().await.map_err(to_pyerr)?;
            Python::attach(|py| ser_to_py(py, &list))
        })
    }
}

/// A namespace-scoped view of the managed key-value store.
#[gen_stub_pyclass]
#[pyclass(name = "Kv", frozen)]
pub struct PyKv {
    laser: Laser,
    namespace: String,
    session: Option<SessionRef>,
}

/// The durable managed-mutation position used as a barrier for takeover reads.
#[gen_stub_pyclass]
#[pyclass(name = "MutationPosition", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyMutationPosition {
    #[pyo3(get)]
    pub topic_generation: u64,
    #[pyo3(get)]
    pub partition: u32,
    #[pyo3(get)]
    pub offset: u64,
}

impl From<MutationPosition> for PyMutationPosition {
    fn from(value: MutationPosition) -> Self {
        Self {
            topic_generation: value.topic_generation,
            partition: value.partition,
            offset: value.offset,
        }
    }
}

impl From<&PyMutationPosition> for MutationPosition {
    fn from(value: &PyMutationPosition) -> Self {
        Self {
            topic_generation: value.topic_generation,
            partition: value.partition,
            offset: value.offset,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyMutationPosition {
    #[new]
    fn new(topic_generation: u64, partition: u32, offset: u64) -> Self {
        Self {
            topic_generation,
            partition,
            offset,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "MutationPosition(topic_generation={}, partition={}, offset={})",
            self.topic_generation, self.partition, self.offset
        )
    }
}

/// A revocable lease grant and the mutation barrier it established.
#[gen_stub_pyclass]
#[pyclass(name = "Lease", frozen)]
pub struct PyLease {
    #[pyo3(get)]
    pub token: u64,
    #[pyo3(get)]
    pub granted_ttl_ms: f64,
    position: PyMutationPosition,
}

impl From<Lease> for PyLease {
    fn from(value: Lease) -> Self {
        Self {
            token: value.token,
            granted_ttl_ms: value.granted_ttl.as_secs_f64() * 1000.0,
            position: value.position.into(),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLease {
    #[getter]
    fn position(&self) -> PyMutationPosition {
        self.position.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "Lease(token={}, granted_ttl_ms={})",
            self.token, self.granted_ttl_ms
        )
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKv {
    /// The namespace this handle is bound to, as the caller named it.
    #[getter]
    fn namespace(&self) -> String {
        self.laser.kv(&self.namespace).namespace().to_owned()
    }

    /// The namespace as sent on the wire, scoped to the default stream unless
    /// the handle names resources bare.
    #[getter]
    fn resource_namespace(&self) -> String {
        self.laser
            .kv(self.namespace.clone())
            .resource_namespace()
            .to_owned()
    }

    /// This handle with every set, compare-and-swap, delete, and patch linked
    /// to `session`, a `{"stream", "session"}` dict as `Session.reference`
    /// returns, so the deployment records which session wrote each key.
    fn in_session(&self, session: &Bound<'_, PyAny>) -> PyResult<PyKv> {
        Ok(PyKv {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            session: Some(crate::convert::py_to_de(session)?),
        })
    }

    /// The session this handle links its writes to, as a dict, or None.
    #[getter]
    fn session<'py>(&self, py: Python<'py>) -> PyResult<Option<Py<PyAny>>> {
        self.session
            .as_ref()
            .map(|session| ser_to_py(py, session))
            .transpose()
    }

    /// Fetch the raw value bytes at `key`, or `None` if absent or expired.
    fn get<'py>(&self, py: Python<'py>, key: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let value = laser
                .kv(namespace)
                .linked(session)
                .get(key)
                .await
                .map_err(to_pyerr)?;
            Ok(value)
        })
    }

    /// Fetch the value at `key` decoded by a user `codec` (any object with
    /// `decode(data) -> value`), or `None` if absent or expired. A codec
    /// failure raises `CodecError` with the codec's exception as its cause.
    fn get_as<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        codec: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let value = laser
                .kv(namespace)
                .linked(session)
                .get(key)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| match value {
                Some(payload) => codec_decode(codec.bind(py), &payload),
                None => Ok(py.None()),
            })
        })
    }

    /// Fetch the full entry (key, value, version, expiry) at `key`, or `None`.
    fn get_entry<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let entry = laser
                .kv(namespace)
                .linked(session)
                .get_entry(key)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| match entry {
                Some(entry) => Ok(PyKvEntry::from(entry)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind()),
                None => Ok(py.None()),
            })
        })
    }

    /// Barriered read: wait until the answering fold has applied at least
    /// `min_position`, normally the position returned by `lease` or
    /// `renew_lease`. A fold that cannot catch up raises a stale `KvError`, never
    /// returns an absent value. Needs the `kv_fenced_leases` capability.
    fn get_entry_at_least<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        min_position: PyRef<'_, PyMutationPosition>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        let min_position = MutationPosition::from(&*min_position);
        future_into_py(py, async move {
            let entry = laser
                .kv(namespace)
                .linked(session)
                .get_entry_at_least(key, min_position)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| match entry {
                Some(entry) => Ok(PyKvEntry::from(entry)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind()),
                None => Ok(py.None()),
            })
        })
    }

    /// Fetch and JSON-decode the value at `key` into a Python value, or `None`.
    fn get_typed<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let value = laser
                .kv(namespace)
                .linked(session)
                .get(key)
                .await
                .map_err(to_pyerr)?;
            Python::attach(|py| match value {
                Some(payload) => {
                    let value: serde_json::Value = serde_json::from_slice(&payload)
                        .map_err(|e| crate::errors::CodecError::new_err(e.to_string()))?;
                    json_to_py(py, &value)
                }
                None => Ok(py.None()),
            })
        })
    }

    /// Start a set. Supply a value (`bytes` / `json` / `msgpack`), optional
    /// `ttl` / `expires_at` / `expect_*`, then `await .send()` (or `.commit()`
    /// for a compare-and-swap).
    fn set(&self, key: &Bound<'_, PyAny>) -> PyResult<PyKvSet> {
        Ok(PyKvSet {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            session: self.session.clone(),
            key: payload_bytes(key)?,
            body: Body::Unset,
            ttl_ms: None,
            expires_at_micros: None,
            expect: None,
        })
    }

    /// Fenced compare-and-swap: write a value to `key` in one backend
    /// transaction that requires a live lease at (`fence_namespace`,
    /// `fence_key`) with a fence sequence still equal to `fence_token` (both
    /// from a prior `lease`), plus a precondition. Returns a `KvCasFencedRequest`
    /// builder: chain `.bytes()`/`.json()`/`.msgpack()`, exactly one of
    /// `.expect_version(v)` or `.expect_absent()`, optionally `.ttl()`, then
    /// `await request.commit()`. The keyword form (`value=`, `expect_version=`,
    /// `expect_absent=`, `ttl_ms=`) fills the same builder, and awaiting the
    /// request commits it. Returns the new version. A stale fence, or a lease
    /// that expired or was released, raises `KvError` with the
    /// `version_conflict` attribute false. A precondition miss raises `KvError`
    /// with `version_conflict` true.
    #[pyo3(signature = (key, fence_namespace, fence_key, fence_token, value=None, *, expect_version=None, expect_absent=false, ttl_ms=None))]
    #[allow(clippy::too_many_arguments)]
    fn cas_fenced(
        &self,
        key: &Bound<'_, PyAny>,
        fence_namespace: String,
        fence_key: &Bound<'_, PyAny>,
        fence_token: u64,
        value: Option<&Bound<'_, PyAny>>,
        expect_version: Option<u64>,
        expect_absent: bool,
        ttl_ms: Option<f64>,
    ) -> PyResult<PyKvCasFenced> {
        let expect = match (expect_version, expect_absent) {
            (Some(_), true) => {
                return Err(InvalidError::new_err(
                    "pass expect_version or expect_absent, not both",
                ));
            }
            (Some(version), false) => Some(Expect::Version(version)),
            (None, true) => Some(Expect::Absent),
            (None, false) => None,
        };
        Ok(PyKvCasFenced {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            session: self.session.clone(),
            key: payload_bytes(key)?,
            fence_namespace,
            fence_key: payload_bytes(fence_key)?,
            fence_token,
            body: match value {
                Some(value) => Body::Bytes(payload_bytes(value)?),
                None => Body::Unset,
            },
            ttl_ms,
            expires_at_micros: None,
            expect,
        })
    }

    /// Delete `key`. Returns `True` when a live entry was removed.
    fn delete<'py>(&self, py: Python<'py>, key: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            laser
                .kv(namespace)
                .linked(session)
                .delete(key)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Test presence and read metadata without the value. Returns the
    /// `KvMetadata`, or `None` when absent.
    fn exists<'py>(&self, py: Python<'py>, key: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let meta = laser
                .kv(namespace)
                .linked(session)
                .exists(key)
                .await
                .map_err(to_pyerr)?;
            Ok(meta.map(PyKvMetadata::from))
        })
    }

    /// Set or refresh the entry's expiry in place. `ttl_ms` of `None` clears it.
    /// Returns the entry's (unchanged) version.
    #[pyo3(signature = (key, ttl_ms=None))]
    fn expire<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        ttl_ms: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        let ttl = ttl_ms
            .map(|ttl_ms| duration_ms(ttl_ms, "ttl_ms"))
            .transpose()?;
        future_into_py(py, async move {
            laser
                .kv(namespace)
                .linked(session)
                .expire(key, ttl)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Set the entry's absolute expiry (epoch microseconds) in place.
    /// `expires_at_micros` of `None` clears it. Returns the entry's (unchanged)
    /// version.
    #[pyo3(signature = (key, expires_at_micros=None))]
    fn expire_at<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        expires_at_micros: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            laser
                .kv(namespace)
                .linked(session)
                .expire_at(key, expires_at_micros)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Apply a merge `patch` (payload) to a structured value, returning the new
    /// version.
    fn patch<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        patch: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        let patch = payload_bytes(patch)?;
        future_into_py(py, async move {
            laser
                .kv(namespace)
                .linked(session)
                .patch(key, patch)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Acquire a revocable lease on `key` for `ttl_ms` as `holder` (a stable
    /// node or worker id). Returns a `Lease` with `token`, `granted_ttl_ms`,
    /// and the `position` to pass to `get_entry_at_least`. A live
    /// lease always conflicts: extend with `renew_lease`, never by
    /// re-acquiring. `ttl_ms` is a requested maximum between 1,000 and
    /// 300,000 milliseconds; the store may grant less and never more, so a value outside that
    /// range raises before the round trip and a holder needing longer renews.
    /// Needs the `kv_fenced_leases` capability. If acquisition is
    /// ambiguous, the SDK closes the dedicated coordination connection and waits
    /// `ttl_ms` before raising the non-retryable ambiguous-mutation error.
    fn lease<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        holder: String,
        ttl_ms: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let lease = laser
                .kv(namespace)
                .linked(session)
                .lease(key, holder, duration_ms(ttl_ms, "ttl_ms")?)
                .await
                .map_err(to_pyerr)?;
            Ok(PyLease::from(lease))
        })
    }

    /// Extend a held lease without changing its token, presenting the same
    /// `holder` and the `token` the grant returned. Returns
    /// a `Lease` with the unchanged token, fresh TTL, and renewal position.
    /// `ttl_ms` obeys the same range as `lease`. An expired, released, or
    /// re-acquired lease raises `KvError`: stop the protected work, a new epoch
    /// needs a fresh `lease`.
    fn renew_lease<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        holder: String,
        token: u64,
        ttl_ms: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let lease = laser
                .kv(namespace)
                .linked(session)
                .renew_lease(key, holder, token, duration_ms(ttl_ms, "ttl_ms")?)
                .await
                .map_err(to_pyerr)?;
            Ok(PyLease::from(lease))
        })
    }

    /// Release a held lease early, presenting the same `holder` and its
    /// `token`. Returns `True` when a held lease was released.
    fn release<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        holder: String,
        token: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            laser
                .kv(namespace)
                .linked(session)
                .release(key, holder, token)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Copy the value at `key` to `to_key` in one backend transaction. Returns
    /// a `KvCopyRequest`: chain `.into_namespace(ns)` to cross namespaces, then
    /// `await request.send()`, or await the request directly. `to_namespace=` is
    /// the keyword form of `into_namespace`. The result is the destination's
    /// new version. An absent or expired source raises the typed not-found
    /// error. The destination is overwritten, and the value moves with its
    /// remaining expiry.
    #[pyo3(signature = (key, to_key, *, to_namespace=None))]
    fn copy_to(
        &self,
        key: &Bound<'_, PyAny>,
        to_key: &Bound<'_, PyAny>,
        to_namespace: Option<String>,
    ) -> PyResult<PyKvCopy> {
        self.copy_request(key, to_key, to_namespace, false)
    }

    /// Move the value at `key` to `to_key`: copy plus the source delete, one
    /// backend transaction. The same builder as `copy_to`.
    #[pyo3(signature = (key, to_key, *, to_namespace=None))]
    fn move_to(
        &self,
        key: &Bound<'_, PyAny>,
        to_key: &Bound<'_, PyAny>,
        to_namespace: Option<String>,
    ) -> PyResult<PyKvCopy> {
        self.copy_request(key, to_key, to_namespace, true)
    }

    /// Point-read several keys in ONE round trip (the mixed-operation batch):
    /// one `bytes | None` per key, in key order.
    fn get_many<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let keys = keys
            .iter()
            .map(payload_bytes)
            .collect::<PyResult<Vec<_>>>()?;
        future_into_py(py, async move {
            laser
                .kv(namespace)
                .linked(session)
                .get_many(keys)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Start a filtered bulk delete over this namespace (chain `prefix` /
    /// `range` / `key_contains`, then `send`).
    fn delete_many(&self) -> PyKvDeleteMany {
        PyKvDeleteMany {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            session: self.session.clone(),
            prefix: None,
            range: None,
            key_contains: None,
            conversation: None,
        }
    }

    /// Start a scan over this namespace.
    fn scan(&self) -> PyKvScan {
        PyKvScan {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            session: self.session.clone(),
            prefix: None,
            range: None,
            key_contains: None,
            conversation: None,
            limit: None,
            cursor: None,
        }
    }
}

impl PyKv {
    fn copy_request(
        &self,
        key: &Bound<'_, PyAny>,
        to_key: &Bound<'_, PyAny>,
        to_namespace: Option<String>,
        delete_source: bool,
    ) -> PyResult<PyKvCopy> {
        Ok(PyKvCopy {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            session: self.session.clone(),
            key: payload_bytes(key)?,
            to_key: payload_bytes(to_key)?,
            to_namespace,
            delete_source,
        })
    }
}

/// A copy or move between keys, built by `Kv.copy_to` / `Kv.move_to`. Await
/// it, or call `send()`.
#[gen_stub_pyclass]
#[pyclass(name = "KvCopyRequest")]
pub struct PyKvCopy {
    laser: Laser,
    namespace: String,
    session: Option<SessionRef>,
    key: Vec<u8>,
    to_key: Vec<u8>,
    to_namespace: Option<String>,
    delete_source: bool,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvCopy {
    /// Write the destination in `namespace` instead of the source's namespace.
    fn into_namespace(mut slf: PyRefMut<'_, Self>, namespace: String) -> PyRefMut<'_, Self> {
        slf.to_namespace = Some(namespace);
        slf
    }

    /// Run the copy or move. Returns the destination's new version.
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = self.key.clone();
        let to_key = self.to_key.clone();
        let to_namespace = self.to_namespace.clone();
        let delete_source = self.delete_source;
        future_into_py(py, async move {
            let kv = laser.kv(namespace).linked(session);
            let mut request = if delete_source {
                kv.move_to(key, to_key)
            } else {
                kv.copy_to(key, to_key)
            };
            if let Some(to_namespace) = to_namespace {
                request = request.into_namespace(to_namespace);
            }
            request.send().await.map_err(to_pyerr)
        })
    }

    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.send(py)?.call_method0("__await__")
    }
}

/// A fenced compare-and-swap, built by `Kv.cas_fenced`. Await it, or call
/// `commit()`.
#[gen_stub_pyclass]
#[pyclass(name = "KvCasFencedRequest")]
pub struct PyKvCasFenced {
    laser: Laser,
    namespace: String,
    session: Option<SessionRef>,
    key: Vec<u8>,
    fence_namespace: String,
    fence_key: Vec<u8>,
    fence_token: u64,
    body: Body,
    ttl_ms: Option<f64>,
    expires_at_micros: Option<u64>,
    expect: Option<Expect>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvCasFenced {
    /// Store raw bytes (str, bytes, or bytearray).
    fn bytes<'py>(
        mut slf: PyRefMut<'py, Self>,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Bytes(payload_bytes(payload)?);
        Ok(slf)
    }

    /// JSON-encode and store the value.
    fn json<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Json(py_to_json(value)?);
        Ok(slf)
    }

    /// MessagePack-encode and store the value.
    fn msgpack<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Msgpack(py_to_json(value)?);
        Ok(slf)
    }

    /// Encode `value` with a user `codec` (any object with `encode(value) ->
    /// bytes`) and store the bytes. A codec failure raises `CodecError`.
    fn encode_with<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
        codec: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Bytes(codec_encode(codec, value)?);
        Ok(slf)
    }

    /// Expire the entry `ttl_ms` milliseconds from now.
    fn ttl(mut slf: PyRefMut<'_, Self>, ttl_ms: f64) -> PyRefMut<'_, Self> {
        slf.ttl_ms = Some(ttl_ms);
        slf
    }

    /// Expire the entry at an absolute epoch-microseconds timestamp.
    fn expires_at(mut slf: PyRefMut<'_, Self>, epoch_micros: u64) -> PyRefMut<'_, Self> {
        slf.expires_at_micros = Some(epoch_micros);
        slf
    }

    /// Precondition: apply only if the key holds `version`.
    fn expect_version(mut slf: PyRefMut<'_, Self>, version: u64) -> PyRefMut<'_, Self> {
        slf.expect = Some(Expect::Version(version));
        slf
    }

    /// Precondition: create only if the key does not exist.
    fn expect_absent(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.expect = Some(Expect::Absent);
        slf
    }

    /// Apply the fenced compare-and-swap. Returns the new version.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = self.key.clone();
        let fence_namespace = self.fence_namespace.clone();
        let fence_key = self.fence_key.clone();
        let fence_token = self.fence_token;
        let body = self.body.clone();
        let ttl_ms = self.ttl_ms;
        let expires_at_micros = self.expires_at_micros;
        let expect = self.expect;
        future_into_py(py, async move {
            let mut request = laser.kv(namespace).linked(session).cas_fenced(
                key,
                fence_namespace,
                fence_key,
                fence_token,
            );
            request = match body {
                Body::Unset => {
                    return Err(to_pyerr(laser_sdk::LaserError::Invalid(
                        "no value set: call .bytes(), .json(), or .msgpack() before committing"
                            .to_owned(),
                    )));
                }
                Body::Bytes(payload) => request.bytes(payload),
                Body::Json(value) => request.json(&value).map_err(to_pyerr)?,
                Body::Msgpack(value) => request.msgpack(&value).map_err(to_pyerr)?,
            };
            if let Some(ttl_ms) = ttl_ms {
                request = request.ttl(duration_ms(ttl_ms, "ttl_ms")?);
            }
            if let Some(epoch_micros) = expires_at_micros {
                request = request.expires_at(epoch_micros);
            }
            request = match expect {
                Some(Expect::Version(version)) => request.expect_version(version),
                Some(Expect::Absent) => request.expect_absent(),
                None => request,
            };
            request.commit().await.map_err(to_pyerr)
        })
    }

    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.commit(py)?.call_method0("__await__")
    }
}

/// One stored entry: key, value, version, and optional expiry.
#[gen_stub_pyclass]
#[pyclass(name = "KvEntry", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyKvEntry {
    #[pyo3(get)]
    pub key: Vec<u8>,
    #[pyo3(get)]
    pub value: Vec<u8>,
    #[pyo3(get)]
    pub version: u64,
    #[pyo3(get)]
    pub expires_at_micros: Option<u64>,
    scope: Option<laser_sdk::wire::kv::MemoryRowScope>,
    source: Option<laser_sdk::wire::graph::SourceRef>,
}

impl From<KvEntry> for PyKvEntry {
    fn from(entry: KvEntry) -> Self {
        Self {
            key: entry.key,
            value: entry.value,
            version: entry.version,
            expires_at_micros: entry.expires_at_micros,
            scope: entry.scope.map(|scope| *scope),
            source: entry.source.map(|source| *source),
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvEntry {
    /// The key decoded as UTF-8, or `None` for a binary key.
    fn key_str(&self) -> Option<String> {
        String::from_utf8(self.key.clone()).ok()
    }

    /// Decode the value as JSON into a Python value.
    fn decode_value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value: serde_json::Value =
            <laser_sdk::stream::Json as laser_sdk::stream::Decoder<_>>::decode(&self.value)
                .map_err(|error| to_pyerr(laser_sdk::LaserError::from(error)))?;
        json_to_py(py, &value)
    }

    /// Decode the value with a user `codec` (any object with
    /// `decode(data) -> value`, such as `Json` or `Cbor`). A codec failure
    /// raises `CodecError` with the codec's exception as its cause.
    fn decode_value_with(&self, codec: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        codec_decode(codec, &self.value)
    }

    /// The memory scope of a memory read-view row as a dict (`kind`, `agent`,
    /// `user`, `app`, `conversation`, `source`), or `None` for a generic entry.
    #[getter]
    fn scope(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.scope
            .as_ref()
            .map(|scope| ser_to_py(py, scope))
            .transpose()
    }

    /// The origin log record this entry was folded from, as a dict (the same
    /// shape a graph node's `source` uses), or `None` when the store did not
    /// stamp one. Every managed write is log-first, so a stamped entry points
    /// back to the record that wrote it.
    fn source(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.source
            .as_ref()
            .map(|source| crate::graph::source_to_py(py, source))
            .transpose()
    }
}

/// One key's metadata without its value: version, expiry, and value size.
#[gen_stub_pyclass]
#[pyclass(name = "KvMetadata", frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyKvMetadata {
    #[pyo3(get)]
    pub version: u64,
    #[pyo3(get)]
    pub expires_at_micros: Option<u64>,
    #[pyo3(get)]
    pub size_bytes: usize,
}

impl From<KvMetadata> for PyKvMetadata {
    fn from(meta: KvMetadata) -> Self {
        Self {
            version: meta.version,
            expires_at_micros: meta.expires_at_micros,
            size_bytes: meta.size_bytes,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvMetadata {
    fn __repr__(&self) -> String {
        format!(
            "KvMetadata(version={}, expires_at_micros={:?}, size_bytes={})",
            self.version, self.expires_at_micros, self.size_bytes
        )
    }
}

/// A page of scanned entries plus the cursor to resume after the last one.
#[gen_stub_pyclass]
#[pyclass(name = "KvPage", frozen)]
pub struct PyKvPage {
    #[pyo3(get)]
    pub entries: Vec<PyKvEntry>,
    #[pyo3(get)]
    pub cursor: Option<Vec<u8>>,
}

impl From<KvPage> for PyKvPage {
    fn from(page: KvPage) -> Self {
        Self {
            entries: page.entries.into_iter().map(PyKvEntry::from).collect(),
            cursor: page.cursor,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvPage {
    fn __len__(&self) -> usize {
        self.entries.len()
    }
}

/// Fluent builder for a KV set / compare-and-swap.
#[gen_stub_pyclass]
#[pyclass(name = "KvSetRequest")]
pub struct PyKvSet {
    laser: Laser,
    namespace: String,
    session: Option<SessionRef>,
    key: Vec<u8>,
    body: Body,
    ttl_ms: Option<f64>,
    expires_at_micros: Option<u64>,
    expect: Option<Expect>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvSet {
    /// Store raw bytes (str, bytes, or bytearray).
    fn bytes<'py>(
        mut slf: PyRefMut<'py, Self>,
        payload: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Bytes(payload_bytes(payload)?);
        Ok(slf)
    }

    /// JSON-encode and store the value.
    fn json<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Json(py_to_json(value)?);
        Ok(slf)
    }

    /// MessagePack-encode and store the value.
    fn msgpack<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Msgpack(py_to_json(value)?);
        Ok(slf)
    }

    /// Encode `value` with a user `codec` (any object with `encode(value) ->
    /// bytes`) and store the bytes. A codec failure raises `CodecError`.
    fn encode_with<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
        codec: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Bytes(codec_encode(codec, value)?);
        Ok(slf)
    }

    /// Expire the entry `ttl_ms` milliseconds from now.
    fn ttl(mut slf: PyRefMut<'_, Self>, ttl_ms: f64) -> PyRefMut<'_, Self> {
        slf.ttl_ms = Some(ttl_ms);
        slf
    }

    /// Expire the entry at an absolute epoch-microseconds timestamp.
    fn expires_at(mut slf: PyRefMut<'_, Self>, epoch_micros: u64) -> PyRefMut<'_, Self> {
        slf.expires_at_micros = Some(epoch_micros);
        slf
    }

    /// Compare-and-swap precondition: apply only if the key holds `version`.
    fn expect_version(mut slf: PyRefMut<'_, Self>, version: u64) -> PyRefMut<'_, Self> {
        slf.expect = Some(Expect::Version(version));
        slf
    }

    /// Compare-and-swap precondition: apply only if the key does not exist.
    fn expect_absent(mut slf: PyRefMut<'_, Self>) -> PyRefMut<'_, Self> {
        slf.expect = Some(Expect::Absent);
        slf
    }

    /// Apply an unconditional write. Raises `InvalidError` when a precondition
    /// was set: a conditional write goes through `commit()`.
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (laser, namespace, key, body, ttl_ms, expires_at_micros, expect) = self.snapshot();
        let session = self.session.clone();
        future_into_py(py, async move {
            let kv = laser.kv(namespace).linked(session);
            let mut request = kv.set(&key);
            request = apply_body(request, body).map_err(to_pyerr)?;
            if let Some(ttl_ms) = ttl_ms {
                request = request.ttl(duration_ms(ttl_ms, "ttl_ms")?);
            }
            if let Some(epoch_micros) = expires_at_micros {
                request = request.expires_at(epoch_micros);
            }
            request = match expect {
                Some(Expect::Version(version)) => request.expect_version(version),
                Some(Expect::Absent) => request.expect_absent(),
                None => request,
            };
            request.send().await.map_err(to_pyerr)
        })
    }

    /// Apply a compare-and-swap (needs `expect_version` / `expect_absent`).
    /// Returns the entry's new version.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (laser, namespace, key, body, ttl_ms, expires_at_micros, expect) = self.snapshot();
        let session = self.session.clone();
        future_into_py(py, async move {
            let kv = laser.kv(namespace).linked(session);
            let mut request = kv.set(&key);
            request = apply_body(request, body).map_err(to_pyerr)?;
            if let Some(ttl_ms) = ttl_ms {
                request = request.ttl(duration_ms(ttl_ms, "ttl_ms")?);
            }
            if let Some(epoch_micros) = expires_at_micros {
                request = request.expires_at(epoch_micros);
            }
            request = match expect {
                Some(Expect::Version(version)) => request.expect_version(version),
                Some(Expect::Absent) => request.expect_absent(),
                None => request,
            };
            request.commit().await.map_err(to_pyerr)
        })
    }
}

impl PyKvSet {
    #[allow(clippy::type_complexity)]
    fn snapshot(
        &self,
    ) -> (
        Laser,
        String,
        Vec<u8>,
        Body,
        Option<f64>,
        Option<u64>,
        Option<Expect>,
    ) {
        (
            self.laser.clone(),
            self.namespace.clone(),
            self.key.clone(),
            self.body.clone(),
            self.ttl_ms,
            self.expires_at_micros,
            self.expect,
        )
    }
}

fn apply_body(
    request: laser_sdk::kv::KvSetRequest,
    body: Body,
) -> Result<laser_sdk::kv::KvSetRequest, laser_sdk::LaserError> {
    match body {
        Body::Unset => Err(laser_sdk::LaserError::Invalid(
            "no value set: call .bytes(), .json(), or .msgpack() before sending".to_owned(),
        )),
        Body::Bytes(payload) => Ok(request.bytes(payload)),
        Body::Json(value) => request.json(&value),
        Body::Msgpack(value) => request.msgpack(&value),
    }
}

/// Fluent builder for a KV scan.
#[gen_stub_pyclass]
#[pyclass(name = "KvScanRequest")]
pub struct PyKvScan {
    laser: Laser,
    namespace: String,
    session: Option<SessionRef>,
    prefix: Option<Vec<u8>>,
    range: Option<(Vec<u8>, Vec<u8>)>,
    key_contains: Option<String>,
    conversation: Option<String>,
    limit: Option<usize>,
    cursor: Option<Vec<u8>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvScan {
    fn prefix<'py>(
        mut slf: PyRefMut<'py, Self>,
        prefix: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.prefix = Some(payload_bytes(prefix)?);
        Ok(slf)
    }

    fn range<'py>(
        mut slf: PyRefMut<'py, Self>,
        start: &Bound<'_, PyAny>,
        end: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.range = Some((payload_bytes(start)?, payload_bytes(end)?));
        Ok(slf)
    }

    fn key_contains<'py>(mut slf: PyRefMut<'py, Self>, substring: String) -> PyRefMut<'py, Self> {
        slf.key_contains = Some(substring);
        slf
    }

    /// The conversation lens: keep only the memory-view rows a given conversation
    /// wrote (a conversation id). Generic key-value rows carry no conversation.
    fn conversation<'py>(
        mut slf: PyRefMut<'py, Self>,
        conversation: String,
    ) -> PyRefMut<'py, Self> {
        slf.conversation = Some(conversation);
        slf
    }

    fn limit(mut slf: PyRefMut<'_, Self>, n: usize) -> PyRefMut<'_, Self> {
        slf.limit = Some(n);
        slf
    }

    fn cursor<'py>(
        mut slf: PyRefMut<'py, Self>,
        cursor: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.cursor = Some(payload_bytes(cursor)?);
        Ok(slf)
    }

    /// Fetch one page (entries plus the cursor to continue).
    fn fetch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let prefix = self.prefix.clone();
        let range = self.range.clone();
        let key_contains = self.key_contains.clone();
        let conversation = parse_conversation(self.conversation.clone())?;
        let limit = self.limit;
        let cursor = self.cursor.clone();
        future_into_py(py, async move {
            let kv = laser.kv(namespace).linked(session);
            let mut request = kv.scan();
            if let Some(prefix) = prefix {
                request = request.prefix(prefix);
            }
            if let Some((start, end)) = range {
                request = request.range(start, end);
            }
            if let Some(substring) = key_contains {
                request = request.key_contains(substring);
            }
            if let Some(conversation) = conversation {
                request = request.conversation(conversation);
            }
            if let Some(n) = limit {
                request = request.limit(n);
            }
            if let Some(cursor) = cursor {
                request = request.cursor(cursor);
            }
            let page = request.fetch().await.map_err(to_pyerr)?;
            Ok(PyKvPage::from(page))
        })
    }

    /// Walk every matching entry across pages.
    fn entries<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let prefix = self.prefix.clone();
        let range = self.range.clone();
        let key_contains = self.key_contains.clone();
        let conversation = parse_conversation(self.conversation.clone())?;
        let limit = self.limit;
        future_into_py(py, async move {
            let kv = laser.kv(namespace).linked(session);
            let mut request = kv.scan();
            if let Some(prefix) = prefix {
                request = request.prefix(prefix);
            }
            if let Some((start, end)) = range {
                request = request.range(start, end);
            }
            if let Some(substring) = key_contains {
                request = request.key_contains(substring);
            }
            if let Some(conversation) = conversation {
                request = request.conversation(conversation);
            }
            if let Some(n) = limit {
                request = request.limit(n);
            }
            let entries = request.entries().await.map_err(to_pyerr)?;
            Ok(entries.into_iter().map(PyKvEntry::from).collect::<Vec<_>>())
        })
    }
}

/// Fluent builder for a filtered bulk delete.
#[gen_stub_pyclass]
#[pyclass(name = "KvDeleteManyRequest")]
pub struct PyKvDeleteMany {
    laser: Laser,
    namespace: String,
    session: Option<SessionRef>,
    prefix: Option<Vec<u8>>,
    range: Option<(Vec<u8>, Vec<u8>)>,
    key_contains: Option<String>,
    conversation: Option<String>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvDeleteMany {
    fn prefix<'py>(
        mut slf: PyRefMut<'py, Self>,
        prefix: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.prefix = Some(payload_bytes(prefix)?);
        Ok(slf)
    }

    fn range<'py>(
        mut slf: PyRefMut<'py, Self>,
        start: &Bound<'_, PyAny>,
        end: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.range = Some((payload_bytes(start)?, payload_bytes(end)?));
        Ok(slf)
    }

    fn key_contains<'py>(mut slf: PyRefMut<'py, Self>, substring: String) -> PyRefMut<'py, Self> {
        slf.key_contains = Some(substring);
        slf
    }

    /// The conversation lens: clear only the memory-view rows a given
    /// conversation wrote (a conversation id).
    fn conversation<'py>(
        mut slf: PyRefMut<'py, Self>,
        conversation: String,
    ) -> PyRefMut<'py, Self> {
        slf.conversation = Some(conversation);
        slf
    }

    /// Apply the bulk delete. Returns the number of entries removed.
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let prefix = self.prefix.clone();
        let range = self.range.clone();
        let key_contains = self.key_contains.clone();
        let conversation = parse_conversation(self.conversation.clone())?;
        future_into_py(py, async move {
            let kv = laser.kv(namespace).linked(session);
            let mut request = kv.delete_many();
            if let Some(prefix) = prefix {
                request = request.prefix(prefix);
            }
            if let Some((start, end)) = range {
                request = request.range(start, end);
            }
            if let Some(substring) = key_contains {
                request = request.key_contains(substring);
            }
            if let Some(conversation) = conversation {
                request = request.conversation(conversation);
            }
            request.send().await.map_err(to_pyerr)
        })
    }
}

use crate::async_bridge::{Awaitable, awaitable};
use crate::client::PyLaser;
use crate::convert::{
    codec_decode, codec_encode, duration_ms, json_to_py, payload_bytes, py_to_json, ser_to_py,
};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::kv::{
    KvEntry, KvMetadata, KvPage, KvRevalidation, Lease, LeaseGuard, MutationPosition,
};
use laser_sdk::laser::Laser;
use laser_sdk::types::ConversationId;
use laser_sdk::wire::agent::SessionRef;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

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

    /// The highest mutation position this client has seen per managed
    /// surface. Every KV, fork and graph reply raises it, and every later read
    /// of that surface sends it, so whichever node answers has applied at
    /// least what this client wrote or read before.
    fn seen_positions(&self) -> PyAppliedFrontier {
        self.inner.seen_positions().into()
    }

    /// Every KV namespace holding at least one entry for this caller.
    fn kv_namespaces<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.inner.clone();
        awaitable(py, async move {
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
#[pyclass(name = "MutationPosition", frozen)]
#[derive(Clone)]
pub struct PyMutationPosition {
    #[pyo3(get)]
    pub topic_generation: u64,
    #[pyo3(get)]
    pub partition: u32,
    #[pyo3(get)]
    pub offset: u64,
}

/// The mutation positions seen per managed surface, `None` for a surface
/// with no position yet.
#[gen_stub_pyclass]
#[pyclass(name = "AppliedFrontier", frozen)]
#[derive(Clone)]
pub struct PyAppliedFrontier {
    #[pyo3(get)]
    pub kv: Option<PyMutationPosition>,
    #[pyo3(get)]
    pub fork: Option<PyMutationPosition>,
    #[pyo3(get)]
    pub graph: Option<PyMutationPosition>,
    /// The session positions seen per stream and source partition.
    #[pyo3(get)]
    pub sessions: Vec<PySessionPosition>,
}

impl From<laser_sdk::wire::mutation::AppliedFrontier> for PyAppliedFrontier {
    fn from(value: laser_sdk::wire::mutation::AppliedFrontier) -> Self {
        Self {
            kv: value.kv.map(Into::into),
            fork: value.fork.map(Into::into),
            graph: value.graph.map(Into::into),
            sessions: value.sessions.into_iter().map(Into::into).collect(),
        }
    }
}

/// How far a node's session fold has applied one partition of a session
/// source.
#[gen_stub_pyclass]
#[pyclass(name = "SessionPosition", frozen)]
#[derive(Clone)]
pub struct PySessionPosition {
    #[pyo3(get)]
    pub stream_id: u32,
    #[pyo3(get)]
    pub stream_generation: u64,
    #[pyo3(get)]
    pub topic: String,
    #[pyo3(get)]
    pub topic_generation: u64,
    #[pyo3(get)]
    pub partition_id: u32,
    #[pyo3(get)]
    pub offset: u64,
}

impl From<laser_sdk::wire::mutation::SessionPosition> for PySessionPosition {
    fn from(value: laser_sdk::wire::mutation::SessionPosition) -> Self {
        Self {
            stream_id: value.stream_id,
            stream_generation: value.stream_generation,
            topic: value.topic,
            topic_generation: value.topic_generation,
            partition_id: value.partition_id,
            offset: value.offset,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PySessionPosition {
    fn __repr__(&self) -> String {
        format!(
            "SessionPosition(stream_id={}, stream_generation={}, topic={:?}, topic_generation={}, partition_id={}, offset={})",
            self.stream_id,
            self.stream_generation,
            self.topic,
            self.topic_generation,
            self.partition_id,
            self.offset
        )
    }
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

#[gen_stub_pymethods]
#[pymethods]
impl PyAppliedFrontier {
    fn __repr__(&self) -> String {
        let show = |position: &Option<PyMutationPosition>| {
            position
                .as_ref()
                .map_or_else(|| "None".to_owned(), PyMutationPosition::__repr__)
        };
        format!(
            "AppliedFrontier(kv={}, fork={}, graph={}, sessions={})",
            show(&self.kv),
            show(&self.fork),
            show(&self.graph),
            self.sessions.len()
        )
    }
}

/// The scope guard `Kv.lease_guard` returns. Use it with `async with`, which
/// acquires on entry and releases on exit. `renew` extends the lease and
/// `release` ends it early.
#[gen_stub_pyclass]
#[pyclass(name = "LeaseGuard")]
pub struct PyLeaseGuard {
    laser: Laser,
    namespace: String,
    session: Option<SessionRef>,
    key: Vec<u8>,
    holder: String,
    ttl: std::time::Duration,
    guard: Arc<Mutex<Option<LeaseGuard>>>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyLeaseGuard {
    /// Acquire the lease and bind the guard.
    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Awaitable<'py, Py<Self>>> {
        let this = slf.borrow(py);
        let kv = this
            .laser
            .kv(this.namespace.clone())
            .linked(this.session.clone());
        let key = this.key.clone();
        let holder = this.holder.clone();
        let ttl = this.ttl;
        let guard = Arc::clone(&this.guard);
        drop(this);
        awaitable(py, async move {
            let acquired = kv.lease_guard(key, holder, ttl).await.map_err(to_pyerr)?;
            *guard.lock().await = Some(acquired);
            Ok(slf)
        })
    }

    /// Release the lease, then return `False` so an exception in the body is
    /// not suppressed.
    #[pyo3(signature = (_exc_type, _exc_value, _traceback))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<Awaitable<'py, bool>> {
        let guard = Arc::clone(&self.guard);
        awaitable(py, async move {
            if let Some(held) = guard.lock().await.take() {
                held.release().await.map_err(to_pyerr)?;
            }
            Ok(false)
        })
    }

    /// The current grant, `None` before entry or after release.
    #[getter]
    fn lease(&self) -> Option<PyLease> {
        self.guard
            .try_lock()
            .ok()
            .and_then(|held| held.as_ref().map(|held| PyLease::from(*held.lease())))
    }

    /// Renew the lease for `ttl_ms`, keeping its token.
    fn renew<'py>(&self, py: Python<'py>, ttl_ms: f64) -> PyResult<Awaitable<'py, PyLease>> {
        let ttl = duration_ms(ttl_ms, "ttl_ms")?;
        let guard = Arc::clone(&self.guard);
        awaitable(py, async move {
            let mut held = guard.lock().await;
            let Some(held) = held.as_mut() else {
                return Err(to_pyerr(laser_sdk::LaserError::Invalid(
                    "the lease guard holds no lease, enter it with async with".to_owned(),
                )));
            };
            let lease = *held.renew(ttl).await.map_err(to_pyerr)?;
            Ok(PyLease::from(lease))
        })
    }

    /// Release the lease now. `True` when it was still held.
    fn release<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, bool>> {
        let guard = Arc::clone(&self.guard);
        awaitable(py, async move {
            match guard.lock().await.take() {
                Some(held) => held.release().await.map_err(to_pyerr),
                None => Ok(false),
            }
        })
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
    valid_until: Instant,
}

impl From<Lease> for PyLease {
    fn from(value: Lease) -> Self {
        Self {
            token: value.token,
            granted_ttl_ms: value.granted_ttl.as_secs_f64() * 1000.0,
            position: value.position.into(),
            valid_until: value.valid_until,
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

    /// The `time.monotonic()` instant, in seconds, the lease stops being valid
    /// for this process: the first send of the grant or renewal plus the
    /// granted TTL. Work that stops before it never outlives the lease.
    #[getter]
    fn valid_until(&self, py: Python<'_>) -> PyResult<f64> {
        let monotonic: f64 = py.import("time")?.call_method0("monotonic")?.extract()?;
        let now = Instant::now();
        Ok(match self.valid_until.checked_duration_since(now) {
            Some(remaining) => monotonic + remaining.as_secs_f64(),
            None => monotonic - now.duration_since(self.valid_until).as_secs_f64(),
        })
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
    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[typing.Optional[builtins.bytes]]",
        imports = ("typing", "builtins")
    ))]
    fn get<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Awaitable<'py, Option<Vec<u8>>>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        codec: Py<PyAny>,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
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
    /// Conditional read: a `KvRevalidation` of kind `NOT_MODIFIED`, and no
    /// value sent, while `key` still holds `version` (the entry version of a
    /// prior read), else of kind `CHANGED` with the entry as it stands now.
    fn get_entry_if_changed<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        version: u64,
    ) -> PyResult<Awaitable<'py, PyKvRevalidation>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
            laser
                .kv(namespace)
                .linked(session)
                .get_entry_if_changed(key, version)
                .await
                .map(PyKvRevalidation::from)
                .map_err(to_pyerr)
        })
    }

    fn get_entry<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        min_position: PyRef<'_, PyMutationPosition>,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        let min_position = MutationPosition::from(&*min_position);
        awaitable(py, async move {
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Awaitable<'py, Py<PyAny>>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
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

    /// Start a set. Supply a value (`bytes` / `json` / `msgpack`), an optional
    /// `ttl` / `expires_at`, then `await .send()`. `expect_version` or
    /// `expect_absent` turn it into a `KvCasRequest` that only commits.
    fn set(
        &self,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
    ) -> PyResult<PyKvSet> {
        Ok(PyKvSet {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            session: self.session.clone(),
            key: payload_bytes(key)?,
            body: Body::Unset,
            ttl_ms: None,
            expires_at_micros: None,
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        fence_namespace: String,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        fence_key: &Bound<'_, PyAny>,
        fence_token: u64,
        #[gen_stub(override_type(type_repr = "typing.Optional[builtins.str | builtins.bytes | builtins.bytearray]", imports = ("typing", "builtins")))]
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
    fn delete<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Awaitable<'py, bool>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
            laser
                .kv(namespace)
                .linked(session)
                .delete(key)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Delete `key` only while it holds `version` (the entry version of a prior
    /// read), a conditional delete. A live entry at another version raises
    /// `KvError` with `version_conflict` set.
    fn delete_if_version<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        version: u64,
    ) -> PyResult<Awaitable<'py, bool>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
            laser
                .kv(namespace)
                .linked(session)
                .delete_if_version(key, version)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Test presence and read metadata without the value. Returns the
    /// `KvMetadata`, or `None` when absent.
    fn exists<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Awaitable<'py, Option<PyKvMetadata>>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        ttl_ms: Option<f64>,
    ) -> PyResult<Awaitable<'py, u64>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        let ttl = ttl_ms
            .map(|ttl_ms| duration_ms(ttl_ms, "ttl_ms"))
            .transpose()?;
        awaitable(py, async move {
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        expires_at_micros: Option<u64>,
    ) -> PyResult<Awaitable<'py, u64>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        patch: &Bound<'_, PyAny>,
    ) -> PyResult<Awaitable<'py, u64>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        let patch = payload_bytes(patch)?;
        awaitable(py, async move {
            laser
                .kv(namespace)
                .linked(session)
                .patch(key, patch)
                .await
                .map_err(to_pyerr)
        })
    }

    /// `patch` applied only while `key` holds `version`. A live entry at another
    /// version raises `KvError` with `version_conflict` set.
    fn patch_if_version<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        patch: &Bound<'_, PyAny>,
        version: u64,
    ) -> PyResult<Awaitable<'py, u64>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        let patch = payload_bytes(patch)?;
        awaitable(py, async move {
            laser
                .kv(namespace)
                .linked(session)
                .patch_if_version(key, patch, version)
                .await
                .map_err(to_pyerr)
        })
    }

    /// A lease scope for `async with kv.lease_guard(key, holder, ttl_ms) as
    /// guard:`. Entering acquires the lease, `guard.lease` is the grant, and
    /// leaving the block releases it however the block ends, unless a renewal
    /// reported it lost.
    fn lease_guard(
        &self,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        holder: String,
        ttl_ms: f64,
    ) -> PyResult<PyLeaseGuard> {
        Ok(PyLeaseGuard {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            session: self.session.clone(),
            key: payload_bytes(key)?,
            holder,
            ttl: duration_ms(ttl_ms, "ttl_ms")?,
            guard: Arc::new(Mutex::new(None)),
        })
    }

    /// `lease` that returns `None` instead of raising when a live lease holds
    /// `key`, the non-blocking acquire. Every other failure raises as `lease`
    /// raises it.
    fn try_lease<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        holder: String,
        ttl_ms: f64,
    ) -> PyResult<Awaitable<'py, Option<PyLease>>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
            let lease = laser
                .kv(namespace)
                .linked(session)
                .try_lease(key, holder, duration_ms(ttl_ms, "ttl_ms")?)
                .await
                .map_err(to_pyerr)?;
            Ok(lease.map(PyLease::from))
        })
    }

    /// Acquire a revocable lease on `key` for `ttl_ms` as `holder` (a stable
    /// node or worker id). Returns a `Lease` with `token`, `granted_ttl_ms`,
    /// the `position` to pass to `get_entry_at_least`, and `valid_until`. A live
    /// lease always conflicts with a `KvError` whose detail is the lease-held
    /// error naming the holder's expiry, and `try_lease` returns `None`
    /// instead: extend with `renew_lease`, never by re-acquiring. `ttl_ms` is a
    /// requested maximum between 1,000 and 300,000 milliseconds. The store may
    /// grant less and never more, so a value outside that range raises before
    /// the round trip and a holder needing longer renews. Needs the
    /// `kv_fenced_leases` capability. If acquisition is ambiguous, the SDK
    /// repeats the same operation identity until `ttl_ms` since the first send
    /// has passed before raising the non-retryable ambiguous-mutation error.
    fn lease<'py>(
        &self,
        py: Python<'py>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        holder: String,
        ttl_ms: f64,
    ) -> PyResult<Awaitable<'py, PyLease>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        holder: String,
        token: u64,
        ttl_ms: f64,
    ) -> PyResult<Awaitable<'py, PyLease>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        key: &Bound<'_, PyAny>,
        holder: String,
        token: u64,
    ) -> PyResult<Awaitable<'py, bool>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = payload_bytes(key)?;
        awaitable(py, async move {
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
    #[gen_stub(override_return_type(
        type_repr = "typing.Awaitable[builtins.list[typing.Optional[builtins.bytes]]]",
        imports = ("typing", "builtins")
    ))]
    fn get_many<'py>(
        &self,
        py: Python<'py>,
        keys: Vec<Bound<'_, PyAny>>,
    ) -> PyResult<Awaitable<'py, Vec<Option<Vec<u8>>>>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let keys = keys
            .iter()
            .map(payload_bytes)
            .collect::<PyResult<Vec<_>>>()?;
        awaitable(py, async move {
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
            page: 0,
            results: None,
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
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, u64>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let key = self.key.clone();
        let to_key = self.to_key.clone();
        let to_namespace = self.to_namespace.clone();
        let delete_source = self.delete_source;
        awaitable(py, async move {
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

    #[gen_stub(override_return_type(
        type_repr = "typing.Generator[typing.Any, None, builtins.int]",
        imports = ("typing", "builtins")
    ))]
    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.send(py)?.into_pyobject(py)?.call_method0("__await__")
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
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

    /// Expire the entry `ttl_ms` milliseconds after the server's time of the write.
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
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, u64>> {
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
        awaitable(py, async move {
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

    #[gen_stub(override_return_type(
        type_repr = "typing.Generator[typing.Any, None, builtins.int]",
        imports = ("typing", "builtins")
    ))]
    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.commit(py)?
            .into_pyobject(py)?
            .call_method0("__await__")
    }
}

/// One stored entry: key, value, version, and optional expiry.
#[gen_stub_pyclass]
#[pyclass(name = "KvEntry", frozen)]
#[derive(Clone)]
pub struct PyKvEntry {
    pub key: Vec<u8>,
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
    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.bytes", imports = ("builtins",)))]
    fn key(&self) -> Vec<u8> {
        self.key.clone()
    }

    #[getter]
    #[gen_stub(override_return_type(type_repr = "builtins.bytes", imports = ("builtins",)))]
    fn value(&self) -> Vec<u8> {
        self.value.clone()
    }

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
#[pyclass(name = "KvMetadata", frozen)]
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

/// The answer to a conditional read. `kind` is `NOT_MODIFIED` while the key
/// still held the version the caller sent, and then `entry` is `None`, or
/// `CHANGED` with `entry` as it stands now, `None` when absent or expired.
#[gen_stub_pyclass]
#[pyclass(name = "KvRevalidation", frozen)]
pub struct PyKvRevalidation {
    kind: &'static str,
    #[pyo3(get)]
    pub entry: Option<PyKvEntry>,
}

impl From<KvRevalidation> for PyKvRevalidation {
    fn from(value: KvRevalidation) -> Self {
        match value {
            KvRevalidation::NotModified => Self {
                kind: Self::NOT_MODIFIED,
                entry: None,
            },
            KvRevalidation::Changed(entry) => Self {
                kind: Self::CHANGED,
                entry: entry.map(PyKvEntry::from),
            },
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvRevalidation {
    /// The `kind` of an answer whose key still held the version sent.
    #[classattr]
    const NOT_MODIFIED: &'static str = "not_modified";
    /// The `kind` of an answer that carries the entry as it stands now.
    #[classattr]
    const CHANGED: &'static str = "changed";

    /// `KvRevalidation.NOT_MODIFIED` or `CHANGED`.
    #[getter]
    fn kind(&self) -> &'static str {
        self.kind
    }

    fn __repr__(&self) -> String {
        format!(
            "KvRevalidation(kind={}, entry={})",
            self.kind,
            self.entry.is_some()
        )
    }
}

/// One page of scanned entries in key order. `page` counts from 1, and
/// `total_results` counts every live match of the scan.
#[gen_stub_pyclass]
#[pyclass(name = "KvPage", frozen)]
pub struct PyKvPage {
    #[pyo3(get)]
    pub items: Vec<PyKvEntry>,
    #[pyo3(get)]
    pub page: u32,
    #[pyo3(get)]
    pub total_pages: u32,
    #[pyo3(get)]
    pub total_results: u32,
}

impl From<KvPage> for PyKvPage {
    fn from(page: KvPage) -> Self {
        Self {
            items: page.items.into_iter().map(PyKvEntry::from).collect(),
            page: page.page,
            total_pages: page.total_pages,
            total_results: page.total_results,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvPage {
    fn __len__(&self) -> usize {
        self.items.len()
    }
}

/// Fluent builder for a KV set. A precondition turns it into a `KvCasRequest`.
#[gen_stub_pyclass]
#[pyclass(name = "KvSetRequest")]
#[derive(Clone)]
pub struct PyKvSet {
    laser: Laser,
    namespace: String,
    session: Option<SessionRef>,
    key: Vec<u8>,
    body: Body,
    ttl_ms: Option<f64>,
    expires_at_micros: Option<u64>,
}

/// A compare-and-swap write, built by `KvSetRequest.expect_version` or
/// `expect_absent`. It only commits, so a precondition is never dropped by an
/// unconditional send. Await it, or call `commit()`.
#[gen_stub_pyclass]
#[pyclass(name = "KvCasRequest")]
pub struct PyKvCas {
    write: PyKvSet,
    expect: Expect,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvSet {
    /// Store raw bytes (str, bytes, or bytearray).
    fn bytes<'py>(
        mut slf: PyRefMut<'py, Self>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
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

    /// Expire the entry `ttl_ms` milliseconds after the server's time of the write.
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
    fn expect_version(&self, version: u64) -> PyKvCas {
        PyKvCas {
            write: self.clone(),
            expect: Expect::Version(version),
        }
    }

    /// Compare-and-swap precondition: apply only if the key does not exist.
    fn expect_absent(&self) -> PyKvCas {
        PyKvCas {
            write: self.clone(),
            expect: Expect::Absent,
        }
    }

    /// Apply the unconditional write.
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, ()>> {
        let write = self.clone();
        awaitable(py, async move {
            write.request()?.send().await.map_err(to_pyerr)
        })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvCas {
    /// Expire the entry `ttl_ms` milliseconds after the server's time of the write.
    fn ttl(mut slf: PyRefMut<'_, Self>, ttl_ms: f64) -> PyRefMut<'_, Self> {
        slf.write.ttl_ms = Some(ttl_ms);
        slf
    }

    /// Expire the entry at an absolute epoch-microseconds timestamp.
    fn expires_at(mut slf: PyRefMut<'_, Self>, epoch_micros: u64) -> PyRefMut<'_, Self> {
        slf.write.expires_at_micros = Some(epoch_micros);
        slf
    }

    /// Apply the compare-and-swap. Returns the entry's new version.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, u64>> {
        let write = self.write.clone();
        let expect = self.expect;
        awaitable(py, async move {
            let request = match expect {
                Expect::Version(version) => write.request()?.expect_version(version),
                Expect::Absent => write.request()?.expect_absent(),
            };
            request.commit().await.map_err(to_pyerr)
        })
    }

    #[gen_stub(override_return_type(
        type_repr = "typing.Generator[typing.Any, None, builtins.int]",
        imports = ("typing", "builtins")
    ))]
    fn __await__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.commit(py)?
            .into_pyobject(py)?
            .call_method0("__await__")
    }
}

impl PyKvSet {
    // The Rust request this builder describes, value and expiry applied.
    fn request(self) -> PyResult<laser_sdk::kv::KvSetRequest> {
        let kv = self.laser.kv(self.namespace).linked(self.session);
        let mut request = apply_body(kv.set(&self.key), self.body).map_err(to_pyerr)?;
        if let Some(ttl_ms) = self.ttl_ms {
            request = request.ttl(duration_ms(ttl_ms, "ttl_ms")?);
        }
        if let Some(epoch_micros) = self.expires_at_micros {
            request = request.expires_at(epoch_micros);
        }
        Ok(request)
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
    page: u32,
    results: Option<u32>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvScan {
    fn prefix<'py>(
        mut slf: PyRefMut<'py, Self>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        prefix: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.prefix = Some(payload_bytes(prefix)?);
        Ok(slf)
    }

    fn range<'py>(
        mut slf: PyRefMut<'py, Self>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        start: &Bound<'_, PyAny>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
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

    /// The 1-based page to read. Zero reads the first page.
    fn page(mut slf: PyRefMut<'_, Self>, page: u32) -> PyRefMut<'_, Self> {
        slf.page = page;
        slf
    }

    /// The entries per page, clamped to the scan cap. Zero reads the server's
    /// default page size.
    fn results(mut slf: PyRefMut<'_, Self>, results: u32) -> PyRefMut<'_, Self> {
        slf.results = Some(results);
        slf
    }

    /// Fetch the chosen page with the totals of the whole scan.
    fn fetch<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, PyKvPage>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let prefix = self.prefix.clone();
        let range = self.range.clone();
        let key_contains = self.key_contains.clone();
        let conversation = parse_conversation(self.conversation.clone())?;
        let page = self.page;
        let results = self.results;
        awaitable(py, async move {
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
            if let Some(results) = results {
                request = request.results(results);
            }
            let page = request.page(page).fetch().await.map_err(to_pyerr)?;
            Ok(PyKvPage::from(page))
        })
    }

    /// Walk every matching entry from the first page to the last.
    fn entries<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, Vec<PyKvEntry>>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let prefix = self.prefix.clone();
        let range = self.range.clone();
        let key_contains = self.key_contains.clone();
        let conversation = parse_conversation(self.conversation.clone())?;
        let results = self.results;
        awaitable(py, async move {
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
            if let Some(results) = results {
                request = request.results(results);
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
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        prefix: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.prefix = Some(payload_bytes(prefix)?);
        Ok(slf)
    }

    fn range<'py>(
        mut slf: PyRefMut<'py, Self>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
        start: &Bound<'_, PyAny>,
        #[gen_stub(override_type(type_repr = "builtins.str | builtins.bytes | builtins.bytearray", imports = ("builtins",)))]
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
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Awaitable<'py, usize>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let session = self.session.clone();
        let prefix = self.prefix.clone();
        let range = self.range.clone();
        let key_contains = self.key_contains.clone();
        let conversation = parse_conversation(self.conversation.clone())?;
        awaitable(py, async move {
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

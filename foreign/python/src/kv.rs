use crate::async_bridge::future_into_py;
use crate::client::PyLaser;
use crate::convert::{json_to_py, payload_bytes, py_to_json, ser_to_py};
use crate::errors::{InvalidError, to_pyerr};
use laser_sdk::kv::{KvEntry, KvPage};
use laser_sdk::laser::Laser;
use laser_sdk::types::ConversationId;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::str::FromStr;
use std::time::Duration;

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

#[derive(Clone)]
enum Body {
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
    /// `str` (UTF-8) or `bytes`. KV is a managed feature: against raw Apache Iggy
    /// every operation raises `UnsupportedError`.
    fn kv(&self, namespace: String) -> PyKv {
        PyKv {
            laser: self.inner.clone(),
            namespace,
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
    #[pyo3(get)]
    namespace: String,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKv {
    /// Fetch the raw value bytes at `key`, or `None` if absent or expired.
    fn get<'py>(&self, py: Python<'py>, key: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let value = laser.kv(namespace).get(key).await.map_err(to_pyerr)?;
            Ok(value)
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
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let entry = laser.kv(namespace).get_entry(key).await.map_err(to_pyerr)?;
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
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let value = laser.kv(namespace).get(key).await.map_err(to_pyerr)?;
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

    /// Start a set. Supply a value (`json` / `msgpack` / `payload`), optional
    /// `ttl` / `expires_at` / `expect_*`, then `await .send()` (or `.commit()`
    /// for a compare-and-swap).
    fn set(&self, key: &Bound<'_, PyAny>) -> PyResult<PyKvSet> {
        Ok(PyKvSet {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            key: payload_bytes(key)?,
            body: Body::Bytes(Vec::new()),
            ttl_secs: None,
            expires_at_micros: None,
            expect: None,
        })
    }

    /// Fenced compare-and-swap: write `value` (payload) to `key` only while the
    /// task's fence sequence still equals `fence_token` (from a prior `lease`),
    /// and the precondition holds. Give exactly one of `expect_version=` (apply
    /// only if the key holds that version) or `expect_absent=True` (create only if
    /// absent). Returns the new version. A stale fence raises `KvError`
    /// (`is_version_conflict()` is false, a newer holder bumped the sequence). A
    /// precondition miss raises `KvError` with `is_version_conflict()` true. The
    /// at-most-one-effective-writer gate for an exclusive external effect.
    #[pyo3(signature = (key, fence_key, fence_token, value, *, expect_version=None, expect_absent=false, ttl_secs=None))]
    #[allow(clippy::too_many_arguments)]
    fn cas_fenced<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        fence_key: &Bound<'_, PyAny>,
        fence_token: u64,
        value: &Bound<'_, PyAny>,
        expect_version: Option<u64>,
        expect_absent: bool,
        ttl_secs: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let key = payload_bytes(key)?;
        let fence_key = payload_bytes(fence_key)?;
        let value = payload_bytes(value)?;
        let ttl = ttl_secs.map(Duration::from_secs_f64);
        future_into_py(py, async move {
            let mut request = laser
                .kv(namespace)
                .cas_fenced(key, fence_key, fence_token)
                .bytes(value);
            if let Some(version) = expect_version {
                request = request.expect_version(version);
            } else if expect_absent {
                request = request.expect_absent();
            }
            if let Some(ttl) = ttl {
                request = request.ttl(ttl);
            }
            request.commit().await.map_err(to_pyerr)
        })
    }

    /// Delete `key`. Returns `True` when a live entry was removed.
    fn delete<'py>(&self, py: Python<'py>, key: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            laser.kv(namespace).delete(key).await.map_err(to_pyerr)
        })
    }

    /// Test presence and read metadata without the value. Returns
    /// `(version, expires_at_micros, size_bytes)` or `None` when absent.
    fn exists<'py>(&self, py: Python<'py>, key: &Bound<'_, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let meta = laser.kv(namespace).exists(key).await.map_err(to_pyerr)?;
            Ok(meta.map(|m| (m.version, m.expires_at_micros, m.size_bytes)))
        })
    }

    /// Set or refresh the entry's expiry in place. `ttl_secs` of `None` clears it.
    /// Returns the entry's (unchanged) version.
    #[pyo3(signature = (key, ttl_secs=None))]
    fn expire<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        ttl_secs: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let key = payload_bytes(key)?;
        let ttl = ttl_secs.map(Duration::from_secs_f64);
        future_into_py(py, async move {
            laser.kv(namespace).expire(key, ttl).await.map_err(to_pyerr)
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
        let key = payload_bytes(key)?;
        let patch = payload_bytes(patch)?;
        future_into_py(py, async move {
            laser
                .kv(namespace)
                .patch(key, patch)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Acquire an advisory lease on `key` for `ttl_secs`. Returns
    /// `(lease_token, granted_ttl_secs)`.
    fn lease<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        ttl_secs: f64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            let lease = laser
                .kv(namespace)
                .lease(key, Duration::from_secs_f64(ttl_secs))
                .await
                .map_err(to_pyerr)?;
            Ok((lease.token, lease.granted_ttl.as_secs_f64()))
        })
    }

    /// Release a held lease early, presenting its `token`. Returns `True` when a
    /// held lease was released.
    fn release<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        token: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let key = payload_bytes(key)?;
        future_into_py(py, async move {
            laser
                .kv(namespace)
                .release(key, token)
                .await
                .map_err(to_pyerr)
        })
    }

    /// Copy the value at `key` to `to_key` in one backend transaction
    /// (`to_namespace=` crosses namespaces). Returns the destination's new
    /// version. An absent or expired source raises the typed not-found error.
    /// The destination is overwritten, and the value moves with its remaining
    /// expiry.
    #[pyo3(signature = (key, to_key, *, to_namespace=None))]
    fn copy_to<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        to_key: &Bound<'_, PyAny>,
        to_namespace: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.copy_or_move(py, key, to_key, to_namespace, false)
    }

    /// Move the value at `key` to `to_key`: copy plus the source delete, one
    /// backend transaction.
    #[pyo3(signature = (key, to_key, *, to_namespace=None))]
    fn move_to<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        to_key: &Bound<'_, PyAny>,
        to_namespace: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.copy_or_move(py, key, to_key, to_namespace, true)
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
        let keys = keys
            .iter()
            .map(payload_bytes)
            .collect::<PyResult<Vec<_>>>()?;
        future_into_py(py, async move {
            laser.kv(namespace).get_many(keys).await.map_err(to_pyerr)
        })
    }

    /// Start a filtered bulk delete over this namespace (chain `prefix` /
    /// `range` / `key_contains`, then `send`).
    fn delete_many(&self) -> PyKvDeleteMany {
        PyKvDeleteMany {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
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
    fn copy_or_move<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
        to_key: &Bound<'_, PyAny>,
        to_namespace: Option<String>,
        delete_source: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let key = payload_bytes(key)?;
        let to_key = payload_bytes(to_key)?;
        future_into_py(py, async move {
            let kv = laser.kv(namespace);
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
    source: Option<laser_sdk::wire::graph::SourceRef>,
}

impl From<KvEntry> for PyKvEntry {
    fn from(entry: KvEntry) -> Self {
        Self {
            key: entry.key,
            value: entry.value,
            version: entry.version,
            expires_at_micros: entry.expires_at_micros,
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
    fn json(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let value: serde_json::Value = serde_json::from_slice(&self.value)
            .map_err(|e| crate::errors::CodecError::new_err(e.to_string()))?;
        json_to_py(py, &value)
    }

    /// The origin log record this entry was folded from, as a dict (the same
    /// shape `graph_node`'s `source` uses), or `None` when the store did not
    /// stamp one. Every managed write is log-first, so a stamped entry points
    /// back to the record that wrote it.
    fn source(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.source
            .as_ref()
            .map(|source| crate::graph::source_to_py(py, source))
            .transpose()
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
    key: Vec<u8>,
    body: Body,
    ttl_secs: Option<f64>,
    expires_at_micros: Option<u64>,
    expect: Option<Expect>,
}

#[gen_stub_pymethods]
#[pymethods]
impl PyKvSet {
    /// Store raw bytes (str, bytes, or bytearray).
    fn payload<'py>(
        mut slf: PyRefMut<'py, Self>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<PyRefMut<'py, Self>> {
        slf.body = Body::Bytes(payload_bytes(value)?);
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

    /// Expire the entry `seconds` from now.
    fn ttl(mut slf: PyRefMut<'_, Self>, seconds: f64) -> PyRefMut<'_, Self> {
        slf.ttl_secs = Some(seconds);
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

    /// Apply an unconditional write.
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (laser, namespace, key, body, ttl_secs, expires_at_micros, _) = self.snapshot();
        future_into_py(py, async move {
            let kv = laser.kv(namespace);
            let mut request = kv.set(&key);
            request = apply_body(request, body).map_err(to_pyerr)?;
            if let Some(seconds) = ttl_secs {
                request = request.ttl(Duration::from_secs_f64(seconds));
            }
            if let Some(epoch_micros) = expires_at_micros {
                request = request.expires_at(epoch_micros);
            }
            request.send().await.map_err(to_pyerr)
        })
    }

    /// Apply a compare-and-swap (needs `expect_version` / `expect_absent`).
    /// Returns the entry's new version.
    fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (laser, namespace, key, body, ttl_secs, expires_at_micros, expect) = self.snapshot();
        future_into_py(py, async move {
            let kv = laser.kv(namespace);
            let mut request = kv.set(&key);
            request = apply_body(request, body).map_err(to_pyerr)?;
            if let Some(seconds) = ttl_secs {
                request = request.ttl(Duration::from_secs_f64(seconds));
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
            self.ttl_secs,
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
        let prefix = self.prefix.clone();
        let range = self.range.clone();
        let key_contains = self.key_contains.clone();
        let conversation = parse_conversation(self.conversation.clone())?;
        let limit = self.limit;
        let cursor = self.cursor.clone();
        future_into_py(py, async move {
            let kv = laser.kv(namespace);
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
        let prefix = self.prefix.clone();
        let range = self.range.clone();
        let key_contains = self.key_contains.clone();
        let conversation = parse_conversation(self.conversation.clone())?;
        let limit = self.limit;
        future_into_py(py, async move {
            let kv = laser.kv(namespace);
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
        let prefix = self.prefix.clone();
        let range = self.range.clone();
        let key_contains = self.key_contains.clone();
        let conversation = parse_conversation(self.conversation.clone())?;
        future_into_py(py, async move {
            let kv = laser.kv(namespace);
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

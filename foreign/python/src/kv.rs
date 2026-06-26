use crate::client::PyLaser;
use crate::convert::{json_to_py, payload_bytes, py_to_json, ser_to_py};
use crate::errors::to_pyerr;
use laser_sdk::kv::{KvEntry, KvPage};
use laser_sdk::laser::Laser;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::time::Duration;

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
                Some(bytes) => {
                    let value: serde_json::Value = serde_json::from_slice(&bytes)
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

    /// Apply a merge `patch` (bytes) to a structured value, returning the new
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

    /// Start a filtered bulk delete over this namespace.
    fn delete_many(&self) -> PyKvDeleteMany {
        PyKvDeleteMany {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            prefix: None,
            range: None,
            key_contains: None,
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
            limit: None,
            cursor: None,
        }
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
}

impl From<KvEntry> for PyKvEntry {
    fn from(entry: KvEntry) -> Self {
        Self {
            key: entry.key,
            value: entry.value,
            version: entry.version,
            expires_at_micros: entry.expires_at_micros,
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

fn apply_body<'a>(
    request: laser_sdk::kv::KvSetRequest<'a>,
    body: Body,
) -> Result<laser_sdk::kv::KvSetRequest<'a>, laser_sdk::LaserError> {
    match body {
        Body::Bytes(bytes) => Ok(request.bytes(bytes)),
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

    /// Apply the bulk delete. Returns the number of entries removed.
    fn send<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let laser = self.laser.clone();
        let namespace = self.namespace.clone();
        let prefix = self.prefix.clone();
        let range = self.range.clone();
        let key_contains = self.key_contains.clone();
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
            request.send().await.map_err(to_pyerr)
        })
    }
}

use crate::error::LaserError;
use crate::kv::{
    AGDX_KV_CAS_CODE, AGDX_KV_DELETE_CODE, AGDX_KV_DELETE_MANY_CODE, AGDX_KV_GET_CODE,
    AGDX_KV_NAMESPACES_CODE, AGDX_KV_SCAN_CODE, AGDX_KV_SET_CODE, CasExpect, DEFAULT_SCAN_LIMIT,
    KV_OP_VERSION, KvCas, KvDelete, KvDeleteMany, KvEntry, KvError, KvGet, KvNamespaceInfo,
    KvNamespaces, KvOutcome, KvPage, KvReply, KvScan, KvSet, MAX_KEY_BYTES, MAX_VALUE_BYTES,
};
use crate::laser::Laser;
use crate::query::{Codec, Decoder};
use laser_wire::framing::encode_named;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

impl Laser {
    /// A handle to the managed key-value store, scoped to `namespace`. Cheap to
    /// create, it borrows the connection. Keys are unique within a namespace and
    /// scans are scoped to it.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use std::time::Duration;
    /// # async fn run(laser: &Laser) -> Result<(), LaserError> {
    /// let kv = laser.kv("sessions");
    /// kv.set("user:42").json(&"online")?.ttl(Duration::from_secs(300)).send().await?;
    /// let state: Option<String> = kv.get_typed("user:42").await?;
    /// kv.delete("user:42").await?;
    /// # Ok(()) }
    /// ```
    pub fn kv<'a>(&'a self, namespace: impl Into<String>) -> Kv<'a> {
        Kv {
            laser: self,
            namespace: namespace.into(),
        }
    }

    /// Every KV namespace that holds at least one entry for this caller,
    /// sorted. Namespace discovery for tooling and UIs: browse the store
    /// without knowing names upfront. Read-only, user-scoped.
    pub async fn kv_namespaces(&self) -> Result<Vec<KvNamespaceInfo>, LaserError> {
        let request = KvNamespaces { v: KV_OP_VERSION };
        match self.execute_kv(AGDX_KV_NAMESPACES_CODE, &request).await? {
            KvOutcome::Namespaces(namespaces) => Ok(namespaces),
            _ => Err(LaserError::Protocol(
                "kv namespaces: unexpected outcome".to_owned(),
            )),
        }
    }

    // Send one KV command over the binary connection and decode the reply. Gated
    // on `managed_kv` (set by the connect-time probe). Without it, `Unsupported`.
    pub(crate) async fn execute_kv(
        &self,
        code: u32,
        request: &impl Serialize,
    ) -> Result<KvOutcome, LaserError> {
        let capabilities = self.capabilities().await;
        if !capabilities.managed_kv {
            return Err(LaserError::Unsupported(
                "kv requires LaserData Cloud".to_owned(),
            ));
        }
        // Fail fast on advertised version skew (see `execute_query`): a server
        // that advertised its accepted KV op version spares the round-trip.
        if let Some(versions) = capabilities.versions
            && versions.kv != KV_OP_VERSION
        {
            return Err(KvError::Version {
                expected: versions.kv,
                got: KV_OP_VERSION,
            }
            .into());
        }
        let payload = bytes::Bytes::from(
            encode_named(request)
                .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?,
        );
        let bytes = self.send_raw_with_response(code, payload).await?;
        match crate::error::decode_managed_reply::<KvReply>(&bytes)? {
            KvReply::Ok(outcome) => Ok(outcome),
            KvReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol("kv: unknown reply variant".to_owned())),
        }
    }
}

/// A namespace-scoped view of the key-value store. Build it with
/// [`Laser::kv`](crate::laser::Laser::kv).
pub struct Kv<'a> {
    laser: &'a Laser,
    namespace: String,
}

impl<'a> Kv<'a> {
    /// The namespace this handle is bound to.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Fetch the raw value bytes stored at `key`, or `None` if absent or
    /// expired. The key is anything byte-like (`&str`, `&[u8]`, `Vec<u8>`, ...).
    pub async fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>, LaserError> {
        Ok(self.get_entry(key).await?.map(|entry| entry.value))
    }

    /// Fetch the full [`KvEntry`] at `key` (key, value, expiry), or `None`.
    pub async fn get_entry(&self, key: impl AsRef<[u8]>) -> Result<Option<KvEntry>, LaserError> {
        let key = validated_key(key.as_ref())?;
        let request = KvGet {
            v: KV_OP_VERSION,
            namespace: self.namespace.clone(),
            key,
        };
        match self.laser.execute_kv(AGDX_KV_GET_CODE, &request).await? {
            KvOutcome::Value(entry) => Ok(entry),
            other => Err(unexpected("get", &other)),
        }
    }

    /// Fetch and JSON-decode the value at `key` into `T`, or `None` if absent.
    /// Sugar for [`get_as`](Self::get_as) with the `Json` codec.
    pub async fn get_typed<T: DeserializeOwned>(
        &self,
        key: impl AsRef<[u8]>,
    ) -> Result<Option<T>, LaserError> {
        self.get_as::<crate::query::Json, T>(key).await
    }

    /// Fetch and decode the value at `key` with any [`Decoder`] (`Json`,
    /// `Msgpack`, or your own codec), or `None` if absent. The store keeps
    /// values as opaque bytes, so the codec is the caller's choice, and reads
    /// are no more JSON-locked than writes.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use laser_sdk::query::Msgpack;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)] struct Session { user: String }
    /// # async fn run(kv: &laser_sdk::kv::Kv<'_>) -> Result<(), LaserError> {
    /// let session: Option<Session> = kv.get_as::<Msgpack, _>("user:42").await?;
    /// # Ok(()) }
    /// ```
    pub async fn get_as<C, T>(&self, key: impl AsRef<[u8]>) -> Result<Option<T>, LaserError>
    where
        C: Decoder<T>,
    {
        match self.get(key).await? {
            Some(bytes) => C::decode(&bytes).map(Some).map_err(LaserError::from),
            None => Ok(None),
        }
    }

    /// Start a `set`. Finish with `.send().await` after supplying a value
    /// (`.bytes` / `.json` / `.msgpack` / `.encode_with`) and an optional
    /// `.ttl` / `.expires_at`. The key is anything byte-like.
    pub fn set(&self, key: impl AsRef<[u8]>) -> KvSetRequest<'a> {
        KvSetRequest {
            laser: self.laser,
            namespace: self.namespace.clone(),
            key: key.as_ref().to_vec(),
            value: Vec::new(),
            expires_at_micros: None,
            expect: None,
        }
    }

    /// Delete `key`. Returns `true` when a live entry was removed, `false` when
    /// none existed.
    pub async fn delete(&self, key: impl AsRef<[u8]>) -> Result<bool, LaserError> {
        let key = validated_key(key.as_ref())?;
        let request = KvDelete {
            v: KV_OP_VERSION,
            namespace: self.namespace.clone(),
            key,
        };
        match self.laser.execute_kv(AGDX_KV_DELETE_CODE, &request).await? {
            KvOutcome::Deleted(existed) => Ok(existed),
            other => Err(unexpected("delete", &other)),
        }
    }

    /// Start a filtered bulk delete over this namespace. Narrow with `.prefix` /
    /// `.range` / `.key_contains` (same bounds as `scan`), then `.send().await`
    /// for the count removed. With no bounds it clears the whole namespace.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # async fn run(kv: &laser_sdk::kv::Kv<'_>) -> Result<(), LaserError> {
    /// let removed = kv.delete_many().prefix("session:").send().await?;
    /// # let _ = removed; Ok(()) }
    /// ```
    pub fn delete_many(&self) -> KvDeleteManyRequest<'a> {
        KvDeleteManyRequest {
            laser: self.laser,
            namespace: self.namespace.clone(),
            prefix: None,
            start: None,
            end: None,
            key_contains: None,
        }
    }

    /// Start a `scan` over this namespace. Finish with `.fetch().await` for one
    /// page, or `.entries().await` to walk every match across pages.
    pub fn scan(&self) -> KvScanRequest<'a> {
        KvScanRequest {
            laser: self.laser,
            namespace: self.namespace.clone(),
            prefix: None,
            start: None,
            end: None,
            key_contains: None,
            limit: DEFAULT_SCAN_LIMIT,
            cursor: None,
        }
    }
}

// The managed KV store is the durable, managed `StateStore`: hand a `Kv` to any
// seam that wants a point store (a durable `Deduplicator`, a `Cursor` checkpoint,
// per-agent state) and it persists off the log via the fork. Richer ops (`scan`,
// `ttl`, `delete_many`) stay on the inherent `Kv` API. The trait is the narrow
// `get`/`set`/`delete` shape shared with `InMemoryStore` / `FileStore`, so a
// `StateStore`-routed `set` never expires (reach for the inherent `set(..).ttl(..)`
// when you want a TTL). The explicit `Kv::` paths avoid resolving back into these
// same trait methods.
#[cfg(feature = "agent")]
impl crate::state_store::StateStore for Kv<'_> {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, LaserError> {
        Kv::get(self, key).await
    }

    async fn set(&self, key: &str, value: Vec<u8>) -> Result<(), LaserError> {
        Kv::set(self, key).bytes(value).send().await
    }

    async fn delete(&self, key: &str) -> Result<(), LaserError> {
        Kv::delete(self, key).await.map(|_existed| ())
    }
}

/// Fluent builder for `Kv::set`. Supply a value with one of `.bytes` (raw),
/// `.json` / `.msgpack` (built-in codecs), or `.encode_with::<C>` (any codec -
/// Avro, Protobuf, CBOR, Arrow, your own), optionally an expiry, then
/// `.send().await`. Values are stored as opaque bytes, and the codec is the
/// caller's choice.
pub struct KvSetRequest<'a> {
    laser: &'a Laser,
    namespace: String,
    key: Vec<u8>,
    value: Vec<u8>,
    expires_at_micros: Option<u64>,
    expect: Option<CasExpect>,
}

impl<'a> KvSetRequest<'a> {
    /// Store raw `bytes` as the value (anything byte-like: `Vec<u8>`, `&[u8]`,
    /// `&str`, ...). Use this for an already-encoded body (Avro / Protobuf /
    /// Arrow / your own framing).
    pub fn bytes(mut self, bytes: impl AsRef<[u8]>) -> Self {
        self.value = bytes.as_ref().to_vec();
        self
    }

    /// Encode `value` with any [`Codec`] and store the bytes. The generic path:
    /// `.json` and `.msgpack` are one-line sugar over it.
    pub fn encode_with<C, T>(mut self, value: &T) -> Result<Self, LaserError>
    where
        C: Codec<T>,
        T: ?Sized,
    {
        self.value = C::encode(value)?;
        Ok(self)
    }

    /// JSON-encode `value` and store the bytes. Sugar for
    /// `.encode_with::<Json, _>`.
    pub fn json<T: Serialize + ?Sized>(self, value: &T) -> Result<Self, LaserError> {
        self.encode_with::<crate::query::Json, T>(value)
    }

    /// MessagePack-encode `value` and store the bytes. Sugar for
    /// `.encode_with::<Msgpack, _>` - more compact than JSON for binary state.
    pub fn msgpack<T: Serialize + ?Sized>(self, value: &T) -> Result<Self, LaserError> {
        self.encode_with::<crate::query::Msgpack, T>(value)
    }

    /// Expire the entry `ttl` from now. Overrides any prior `expires_at`.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.expires_at_micros = Some(now_micros().saturating_add(ttl.as_micros() as u64));
        self
    }

    /// Expire the entry at an absolute epoch-microseconds timestamp.
    pub fn expires_at(mut self, epoch_micros: u64) -> Self {
        self.expires_at_micros = Some(epoch_micros);
        self
    }

    /// Make this a compare-and-swap that applies only if the key currently holds
    /// `version` (the [`KvEntry::version`] from a prior read). Finish with
    /// [`commit`](Self::commit). Lock-free optimistic concurrency: read the
    /// version, compute the new value, `commit`, and retry on a conflict.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # async fn run(kv: &laser_sdk::kv::Kv<'_>) -> Result<(), LaserError> {
    /// let entry = kv.get_entry("counter").await?.expect("seeded");
    /// let prev: u64 = String::from_utf8_lossy(&entry.value).parse().unwrap_or(0);
    /// let next = kv.set("counter").json(&(prev + 1))?.expect_version(entry.version).commit().await?;
    /// # let _ = next; Ok(()) }
    /// ```
    pub fn expect_version(mut self, version: u64) -> Self {
        self.expect = Some(CasExpect::Match(version));
        self
    }

    /// Make this a compare-and-swap that applies only if the key does not yet
    /// exist (a create-if-absent). Finish with [`commit`](Self::commit). A racing
    /// writer that got there first surfaces as a conflict.
    pub fn expect_absent(mut self) -> Self {
        self.expect = Some(CasExpect::Absent);
        self
    }

    /// Apply a compare-and-swap write set up by [`expect_version`](Self::expect_version)
    /// or [`expect_absent`](Self::expect_absent), returning the entry's new
    /// version. The precondition miss is a typed
    /// [`KvError::VersionConflict`](laser_wire::kv::KvError::VersionConflict)
    /// carrying the current version, so the caller can re-read and retry.
    /// Returns `LaserError::Invalid` if no precondition was set (use `send` for
    /// an unconditional write), and `LaserError::Unsupported` when the
    /// deployment does not advertise the `kv_cas` capability.
    pub async fn commit(self) -> Result<u64, LaserError> {
        let Some(expect) = self.expect else {
            return Err(LaserError::Invalid(
                "commit() needs a precondition: call expect_version(..) or expect_absent()"
                    .to_owned(),
            ));
        };
        // Fail fast when the deployment does not advertise compare-and-swap.
        // Unlike a plain set, CAS rides its own command code an unaware server
        // rejects, but checking the advertised capability locally turns that
        // into the documented `Unsupported` before the round-trip.
        if !self.laser.capabilities().await.kv_cas {
            return Err(LaserError::Unsupported(
                "compare-and-swap requires a backend that serves it (kv_cas capability)".to_owned(),
            ));
        }
        let key = validated_key(&self.key)?;
        validated_value(&self.value)?;
        let request = KvCas {
            v: KV_OP_VERSION,
            namespace: self.namespace,
            key,
            value: self.value,
            expires_at_micros: self.expires_at_micros,
            expect,
        };
        match self.laser.execute_kv(AGDX_KV_CAS_CODE, &request).await? {
            KvOutcome::Committed { version } => Ok(version),
            other => Err(unexpected("cas", &other)),
        }
    }

    /// Apply the write.
    pub async fn send(self) -> Result<(), LaserError> {
        let key = validated_key(&self.key)?;
        validated_value(&self.value)?;
        let request = KvSet {
            v: KV_OP_VERSION,
            namespace: self.namespace,
            key,
            value: self.value,
            expires_at_micros: self.expires_at_micros,
        };
        match self.laser.execute_kv(AGDX_KV_SET_CODE, &request).await? {
            KvOutcome::Written => Ok(()),
            other => Err(unexpected("set", &other)),
        }
    }
}

/// Fluent builder for `Kv::scan`. Narrow with `.prefix` or `.range`, cap with
/// `.limit`, then `.fetch()` for a page or `.entries()` for everything.
pub struct KvScanRequest<'a> {
    laser: &'a Laser,
    namespace: String,
    prefix: Option<Vec<u8>>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    key_contains: Option<String>,
    limit: usize,
    cursor: Option<Vec<u8>>,
}

impl<'a> KvScanRequest<'a> {
    /// Only keys starting with `prefix` (byte order). The prefix is any
    /// byte-like value.
    pub fn prefix(mut self, prefix: impl AsRef<[u8]>) -> Self {
        self.prefix = Some(prefix.as_ref().to_vec());
        self
    }

    /// Only keys in `[start, end)` (inclusive start, exclusive end, byte order).
    pub fn range(mut self, start: impl AsRef<[u8]>, end: impl AsRef<[u8]>) -> Self {
        self.start = Some(start.as_ref().to_vec());
        self.end = Some(end.as_ref().to_vec());
        self
    }

    /// Keep only keys that are valid UTF-8 and contain `substring`. Binary keys
    /// are skipped. Composes with `prefix` / `range`.
    pub fn key_contains(mut self, substring: impl Into<String>) -> Self {
        self.key_contains = Some(substring.into());
        self
    }

    /// Cap the page at `n` entries (clamped to `MAX_SCAN_LIMIT`).
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = n;
        self
    }

    /// Resume after a previous page's cursor.
    pub fn cursor(mut self, cursor: impl AsRef<[u8]>) -> Self {
        self.cursor = Some(cursor.as_ref().to_vec());
        self
    }

    /// Fetch one page (entries plus the cursor to continue).
    pub async fn fetch(self) -> Result<KvPage, LaserError> {
        let request = self.request();
        match self.laser.execute_kv(AGDX_KV_SCAN_CODE, &request).await? {
            KvOutcome::Page(page) => Ok(page),
            other => Err(unexpected("scan", &other)),
        }
    }

    /// Walk every matching entry across pages, following the cursor until the
    /// scan is exhausted. Convenient when the working set fits in memory.
    pub async fn entries(self) -> Result<Vec<KvEntry>, LaserError> {
        let laser = self.laser;
        let mut request = self.request();
        let mut out = Vec::new();
        loop {
            let page = match laser.execute_kv(AGDX_KV_SCAN_CODE, &request).await? {
                KvOutcome::Page(page) => page,
                other => return Err(unexpected("scan", &other)),
            };
            out.extend(page.entries);
            match page.cursor {
                Some(cursor) => request.cursor = Some(cursor),
                None => return Ok(out),
            }
        }
    }

    fn request(&self) -> KvScan {
        KvScan {
            v: KV_OP_VERSION,
            namespace: self.namespace.clone(),
            prefix: self.prefix.clone(),
            start: self.start.clone(),
            end: self.end.clone(),
            key_contains: self.key_contains.clone(),
            limit: self.limit,
            cursor: self.cursor.clone(),
        }
    }
}

/// Fluent builder for `Kv::delete_many`. Narrow with `.prefix` / `.range` /
/// `.key_contains`, then `.send()` for the count removed.
pub struct KvDeleteManyRequest<'a> {
    laser: &'a Laser,
    namespace: String,
    prefix: Option<Vec<u8>>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    key_contains: Option<String>,
}

impl<'a> KvDeleteManyRequest<'a> {
    /// Only keys starting with `prefix` (byte order).
    pub fn prefix(mut self, prefix: impl AsRef<[u8]>) -> Self {
        self.prefix = Some(prefix.as_ref().to_vec());
        self
    }

    /// Only keys in `[start, end)` (inclusive start, exclusive end, byte order).
    pub fn range(mut self, start: impl AsRef<[u8]>, end: impl AsRef<[u8]>) -> Self {
        self.start = Some(start.as_ref().to_vec());
        self.end = Some(end.as_ref().to_vec());
        self
    }

    /// Keep only keys that are valid UTF-8 and contain `substring`. Composes with
    /// `prefix` / `range`.
    pub fn key_contains(mut self, substring: impl Into<String>) -> Self {
        self.key_contains = Some(substring.into());
        self
    }

    /// Apply the bulk delete. Returns the number of entries removed.
    pub async fn send(self) -> Result<usize, LaserError> {
        let request = KvDeleteMany {
            v: KV_OP_VERSION,
            namespace: self.namespace,
            prefix: self.prefix,
            start: self.start,
            end: self.end,
            key_contains: self.key_contains,
        };
        match self
            .laser
            .execute_kv(AGDX_KV_DELETE_MANY_CODE, &request)
            .await?
        {
            KvOutcome::DeletedMany(removed) => Ok(removed),
            other => Err(unexpected("delete_many", &other)),
        }
    }
}

// Validate a key and return it as an owned `Vec<u8>`. Keys are arbitrary bytes,
// non-empty, at most `MAX_KEY_BYTES`.
fn validated_key(key: &[u8]) -> Result<Vec<u8>, LaserError> {
    if key.is_empty() {
        return Err(LaserError::Invalid("key must not be empty".to_owned()));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(LaserError::Invalid(format!(
            "key is {}B, exceeds cap {MAX_KEY_BYTES}B",
            key.len()
        )));
    }
    Ok(key.to_vec())
}

// Reject an over-cap value before the round-trip, the single check shared by
// `set` and the compare-and-swap `commit` so the cap and message cannot drift
// between the two terminals.
fn validated_value(value: &[u8]) -> Result<(), LaserError> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(LaserError::Invalid(format!(
            "value is {}B, exceeds cap {MAX_VALUE_BYTES}B",
            value.len()
        )));
    }
    Ok(())
}

fn unexpected(op: &str, outcome: &KvOutcome) -> LaserError {
    LaserError::Protocol(format!("kv {op}: unexpected reply outcome {outcome:?}"))
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_an_empty_key_when_validated_then_should_error() {
        assert!(matches!(validated_key(b""), Err(LaserError::Invalid(_))));
    }

    #[test]
    fn given_an_oversized_key_when_validated_then_should_error() {
        let key = vec![b'x'; MAX_KEY_BYTES + 1];
        assert!(matches!(validated_key(&key), Err(LaserError::Invalid(_))));
    }

    #[test]
    fn given_a_binary_key_when_validated_then_should_accept_it() {
        assert!(validated_key(&[0xff, 0x00, 0xfe]).is_ok());
    }

    #[test]
    fn given_a_kv_error_when_converted_then_should_nest_the_typed_error() {
        let unsupported = LaserError::from(KvError::Unsupported("no managed backend".to_owned()));
        assert!(matches!(
            unsupported,
            LaserError::Kv(KvError::Unsupported(_))
        ));
        assert!(unsupported.is_unsupported());

        let backend = LaserError::from(KvError::Backend("boom".to_owned()));
        assert!(matches!(backend, LaserError::Kv(KvError::Backend(_))));
        assert!(backend.is_retryable());

        let skew = LaserError::from(KvError::Version {
            expected: 1,
            got: 2,
        });
        assert!(matches!(skew, LaserError::Kv(KvError::Version { .. })));
        assert!(skew.is_version_skew());
        assert!(!skew.is_retryable());
    }
}

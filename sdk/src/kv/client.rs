use crate::error::LaserError;
use crate::kv::{
    AGDX_KV_CAS_CODE, AGDX_KV_CAS_FENCED_CODE, AGDX_KV_COPY_CODE, AGDX_KV_DELETE_CODE,
    AGDX_KV_DELETE_MANY_CODE, AGDX_KV_EXISTS_CODE, AGDX_KV_EXPIRE_CODE, AGDX_KV_GET_CODE,
    AGDX_KV_LEASE_CODE, AGDX_KV_MOVE_CODE, AGDX_KV_NAMESPACES_CODE, AGDX_KV_PATCH_CODE,
    AGDX_KV_RELEASE_CODE, AGDX_KV_SCAN_CODE, AGDX_KV_SET_CODE, CasExpect, DEFAULT_SCAN_LIMIT,
    KV_OP_VERSION, KvCas, KvCasFenced, KvCopy, KvDelete, KvDeleteMany, KvEntry, KvError, KvExists,
    KvExpire, KvGet, KvLease, KvMetadata, KvNamespaceInfo, KvNamespaces, KvOutcome, KvPage,
    KvPatch, KvRelease, KvReply, KvScan, KvSet, MAX_KEY_BYTES, MAX_VALUE_BYTES,
};
use crate::laser::Laser;
use crate::stream::{Codec, Decoder};
use crate::types::ConversationId;
use laser_wire::framing::encode_named;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A granted advisory lease: the fencing token to present on protected mutations
/// and the TTL the store granted (which may be shorter than requested).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    pub token: u64,
    pub granted_ttl: Duration,
}

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
    pub fn kv(&self, namespace: impl Into<String>) -> Kv {
        Kv {
            laser: self.clone(),
            namespace: namespace.into(),
        }
    }

    /// Every KV namespace that holds at least one entry for this caller,
    /// sorted. Namespace discovery for tooling and UIs: browse the store
    /// without knowing names upfront. Read-only, user-scoped.
    pub async fn kv_namespaces(&self) -> Result<Vec<KvNamespaceInfo>, LaserError> {
        let request = KvNamespaces { v: KV_OP_VERSION };
        match self
            .execute_kv(None, AGDX_KV_NAMESPACES_CODE, &request)
            .await?
        {
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
        namespace: Option<&str>,
        code: u32,
        request: &impl Serialize,
    ) -> Result<KvOutcome, LaserError> {
        if let Some(namespace) = namespace {
            laser_wire::kv::validate_namespace(namespace)?;
        }
        let capabilities = self.capabilities().await;
        if !capabilities.kv.available {
            return Err(LaserError::unsupported(
                "kv",
                "the key-value surface is not served by this deployment",
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
        let payload = encode_named(request)
            .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?;
        let payload = self.send_raw_with_response(code, payload).await?;
        match crate::error::decode_managed_reply::<KvReply>(&payload)? {
            KvReply::Ok(outcome) => Ok(outcome),
            KvReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol("kv: unknown reply variant".to_owned())),
        }
    }
}

/// A namespace-scoped view of the key-value store. Build it with
/// [`Laser::kv`](crate::laser::Laser::kv).
pub struct Kv {
    laser: Laser,
    namespace: String,
}

impl Kv {
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
            if_none_match: None,
        };
        match self
            .laser
            .execute_kv(Some(&self.namespace), AGDX_KV_GET_CODE, &request)
            .await?
        {
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
        self.get_as::<crate::stream::Json, T>(key).await
    }

    /// Fetch and decode the value at `key` with any [`Decoder`] (`Json`,
    /// `Msgpack`, or your own codec), or `None` if absent. The store keeps
    /// values as opaque bytes, so the codec is the caller's choice, and reads
    /// are no more JSON-locked than writes.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use laser_sdk::stream::Msgpack;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)] struct Session { user: String }
    /// # async fn run(kv: &laser_sdk::kv::Kv) -> Result<(), LaserError> {
    /// let session: Option<Session> = kv.get_as::<Msgpack, _>("user:42").await?;
    /// # Ok(()) }
    /// ```
    pub async fn get_as<C, T>(&self, key: impl AsRef<[u8]>) -> Result<Option<T>, LaserError>
    where
        C: Decoder<T>,
    {
        match self.get(key).await? {
            Some(payload) => C::decode(&payload).map(Some).map_err(LaserError::from),
            None => Ok(None),
        }
    }

    /// Start a `set`. Finish with `.send().await` after supplying a value
    /// (`.bytes` / `.json` / `.msgpack` / `.encode_with`) and an optional
    /// `.ttl` / `.expires_at`. The key is anything byte-like.
    pub fn set(&self, key: impl AsRef<[u8]>) -> KvSetRequest {
        KvSetRequest {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            key: key.as_ref().to_vec(),
            value: Vec::new(),
            expires_at_micros: None,
            expect: None,
        }
    }

    /// Start a fenced compare-and-swap on `key`: the write applies only while the
    /// task's fence sequence still equals `fence_token` (the value a
    /// [`lease`](Self::lease) returned). Supply a value and a precondition
    /// (`.expect_absent()` or `.expect_version(..)`), then `.commit().await`. The
    /// at-most-one-effective-writer gate for an exclusive effect: a zombie holder
    /// at an older token is rejected even if its lease was presumed lost.
    pub fn cas_fenced(
        &self,
        key: impl AsRef<[u8]>,
        fence_key: impl AsRef<[u8]>,
        fence_token: u64,
    ) -> KvCasFencedRequest {
        KvCasFencedRequest {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            key: key.as_ref().to_vec(),
            value: Vec::new(),
            expires_at_micros: None,
            expect: None,
            fence_key: fence_key.as_ref().to_vec(),
            fence_token,
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
            if_match: None,
        };
        match self
            .laser
            .execute_kv(Some(&self.namespace), AGDX_KV_DELETE_CODE, &request)
            .await?
        {
            KvOutcome::Deleted(existed) => Ok(existed),
            other => Err(unexpected("delete", &other)),
        }
    }

    /// Test presence and read metadata (version, expiry, value size) without
    /// transferring the value. The cheap precondition check before a large fetch.
    pub async fn exists(&self, key: impl AsRef<[u8]>) -> Result<Option<KvMetadata>, LaserError> {
        let key = validated_key(key.as_ref())?;
        let request = KvExists {
            v: KV_OP_VERSION,
            namespace: self.namespace.clone(),
            key,
        };
        match self
            .laser
            .execute_kv(Some(&self.namespace), AGDX_KV_EXISTS_CODE, &request)
            .await?
        {
            KvOutcome::Metadata(metadata) => Ok(metadata),
            other => Err(unexpected("exists", &other)),
        }
    }

    /// Set or refresh the entry's expiry in place without rewriting its value.
    /// `ttl` of `None` clears the expiry. Returns the entry's (unchanged) version.
    pub async fn expire(
        &self,
        key: impl AsRef<[u8]>,
        ttl: Option<Duration>,
    ) -> Result<u64, LaserError> {
        let key = validated_key(key.as_ref())?;
        let expires_at_micros = ttl.map(|ttl| now_micros().saturating_add(ttl.as_micros() as u64));
        let request = KvExpire {
            v: KV_OP_VERSION,
            namespace: self.namespace.clone(),
            key,
            expires_at_micros,
        };
        match self
            .laser
            .execute_kv(Some(&self.namespace), AGDX_KV_EXPIRE_CODE, &request)
            .await?
        {
            KvOutcome::Versioned { version } => Ok(version),
            other => Err(unexpected("expire", &other)),
        }
    }

    /// Apply a merge `patch` to a structured value without transferring the whole
    /// object, returning the new version. The patch bytes are codec-specific (a
    /// JSON merge patch over a JSON value, for instance).
    pub async fn patch(
        &self,
        key: impl AsRef<[u8]>,
        patch: impl Into<Vec<u8>>,
    ) -> Result<u64, LaserError> {
        let key = validated_key(key.as_ref())?;
        let request = KvPatch {
            v: KV_OP_VERSION,
            namespace: self.namespace.clone(),
            key,
            patch: patch.into(),
            if_match: None,
        };
        match self
            .laser
            .execute_kv(Some(&self.namespace), AGDX_KV_PATCH_CODE, &request)
            .await?
        {
            KvOutcome::Versioned { version } => Ok(version),
            other => Err(unexpected("patch", &other)),
        }
    }

    /// Acquire an advisory lease (a bounded-TTL distributed lock) on `key`. The
    /// returned [`Lease`] carries the fencing token to present on protected
    /// mutations and the TTL the store granted.
    pub async fn lease(&self, key: impl AsRef<[u8]>, ttl: Duration) -> Result<Lease, LaserError> {
        let key = validated_key(key.as_ref())?;
        let request = KvLease {
            v: KV_OP_VERSION,
            namespace: self.namespace.clone(),
            key,
            lease_ttl_micros: ttl.as_micros() as u64,
        };
        match self
            .laser
            .execute_kv(Some(&self.namespace), AGDX_KV_LEASE_CODE, &request)
            .await?
        {
            KvOutcome::Leased {
                lease_token,
                granted_ttl_micros,
            } => Ok(Lease {
                token: lease_token,
                granted_ttl: Duration::from_micros(granted_ttl_micros),
            }),
            other => Err(unexpected("lease", &other)),
        }
    }

    /// Release a held lease early, presenting the `token` the grant returned.
    /// Returns `true` when a held lease was released.
    pub async fn release(&self, key: impl AsRef<[u8]>, token: u64) -> Result<bool, LaserError> {
        let key = validated_key(key.as_ref())?;
        let request = KvRelease {
            v: KV_OP_VERSION,
            namespace: self.namespace.clone(),
            key,
            lease_token: token,
        };
        match self
            .laser
            .execute_kv(Some(&request.namespace), AGDX_KV_RELEASE_CODE, &request)
            .await?
        {
            KvOutcome::Released(released) => Ok(released),
            other => Err(unexpected("release", &other)),
        }
    }

    /// Copy the value at `key` to `to_key` in one backend transaction: chain
    /// [`into_namespace`](KvCopyRequest::into_namespace) for a cross-namespace
    /// copy, then `.send().await` for the destination's new version. The
    /// destination is overwritten (compose `exists` + `cas` for a guarded
    /// copy), and the value moves with its remaining expiry.
    pub fn copy_to(&self, key: impl AsRef<[u8]>, to_key: impl AsRef<[u8]>) -> KvCopyRequest {
        KvCopyRequest {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            key: key.as_ref().to_vec(),
            to_namespace: None,
            to_key: to_key.as_ref().to_vec(),
            delete_source: false,
        }
    }

    /// Move the value at `key` to `to_key`: [`copy_to`](Self::copy_to) plus
    /// the source delete, one backend transaction.
    pub fn move_to(&self, key: impl AsRef<[u8]>, to_key: impl AsRef<[u8]>) -> KvCopyRequest {
        KvCopyRequest {
            delete_source: true,
            ..self.copy_to(key, to_key)
        }
    }

    /// Point-read several keys in ONE round trip, riding the mixed-operation
    /// batch ([`Laser::execute_batch`]): one `Option<value>` per key, in key
    /// order. The common multi-get amortization for agent hot loops.
    pub async fn get_many(
        &self,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
    ) -> Result<Vec<Option<Vec<u8>>>, LaserError> {
        let mut ops = Vec::new();
        for key in keys {
            let request = KvGet {
                v: KV_OP_VERSION,
                namespace: self.namespace.clone(),
                key: validated_key(key.as_ref())?,
                if_none_match: None,
            };
            ops.push(laser_wire::batch::BatchItem {
                code: AGDX_KV_GET_CODE,
                payload: encode_named(&request)
                    .map_err(|error| LaserError::Codec(format!("encode get: {error}")))?,
            });
        }
        let results = self.laser.execute_batch(ops).await?;
        results
            .iter()
            .map(
                |slot| match crate::error::decode_managed_reply::<KvReply>(slot)? {
                    KvReply::Ok(KvOutcome::Value(entry)) => Ok(entry.map(|entry| entry.value)),
                    KvReply::Ok(ref other) => Err(unexpected("get", other)),
                    KvReply::Err(error) => Err(error.into()),
                    // The reply enums are non_exhaustive: a newer server's
                    // unknown outcome is a protocol surprise, not a value.
                    _ => Err(LaserError::Protocol(
                        "unrecognized kv reply in a batch slot".to_owned(),
                    )),
                },
            )
            .collect()
    }

    /// Start a filtered bulk delete over this namespace. Narrow with `.prefix` /
    /// `.range` / `.key_contains` (same bounds as `scan`), then `.send().await`
    /// for the count removed. With no bounds it clears the whole namespace.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # async fn run(kv: &laser_sdk::kv::Kv) -> Result<(), LaserError> {
    /// let removed = kv.delete_many().prefix("session:").send().await?;
    /// # let _ = removed; Ok(()) }
    /// ```
    pub fn delete_many(&self) -> KvDeleteManyRequest {
        KvDeleteManyRequest {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            prefix: None,
            start: None,
            end: None,
            key_contains: None,
            conversation: None,
        }
    }

    /// Start a `scan` over this namespace. Finish with `.fetch().await` for one
    /// page, or `.entries().await` to walk every match across pages.
    pub fn scan(&self) -> KvScanRequest {
        KvScanRequest {
            laser: self.laser.clone(),
            namespace: self.namespace.clone(),
            prefix: None,
            start: None,
            end: None,
            key_contains: None,
            conversation: None,
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
impl crate::state_store::StateStore for Kv {
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
#[must_use = "call .send().await to write, or .commit().await for a compare-and-swap"]
pub struct KvSetRequest {
    laser: Laser,
    namespace: String,
    key: Vec<u8>,
    value: Vec<u8>,
    expires_at_micros: Option<u64>,
    expect: Option<CasExpect>,
}

impl KvSetRequest {
    /// Store raw `bytes` as the value (anything byte-like: `Vec<u8>`, `&[u8]`,
    /// `&str`, ...). Use this for an already-encoded body (Avro / Protobuf /
    /// Arrow / your own framing).
    pub fn bytes(mut self, payload: impl AsRef<[u8]>) -> Self {
        self.value = payload.as_ref().to_vec();
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
        self.encode_with::<crate::stream::Json, T>(value)
    }

    /// MessagePack-encode `value` and store the bytes. Sugar for
    /// `.encode_with::<Msgpack, _>`, more compact than JSON for binary state.
    pub fn msgpack<T: Serialize + ?Sized>(self, value: &T) -> Result<Self, LaserError> {
        self.encode_with::<crate::stream::Msgpack, T>(value)
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
    /// # async fn run(kv: &laser_sdk::kv::Kv) -> Result<(), LaserError> {
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
        if !self.laser.capabilities().await.kv.cas {
            return Err(LaserError::unsupported_feature(
                "kv",
                "cas",
                "compare-and-swap is not advertised by this deployment",
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
        match self
            .laser
            .execute_kv(Some(&request.namespace), AGDX_KV_CAS_CODE, &request)
            .await?
        {
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
        match self
            .laser
            .execute_kv(Some(&request.namespace), AGDX_KV_SET_CODE, &request)
            .await?
        {
            KvOutcome::Written => Ok(()),
            other => Err(unexpected("set", &other)),
        }
    }
}

/// Fluent builder for `Kv::cas_fenced`. Supply a value (`.bytes` / `.json` /
/// `.msgpack` / `.encode_with`), a precondition (`.expect_absent` or
/// `.expect_version`), an optional expiry, then `.commit().await`. The write
/// lands only while the task fence sequence still equals the held token.
#[must_use = "call .commit().await to apply the fenced compare-and-swap"]
pub struct KvCasFencedRequest {
    laser: Laser,
    namespace: String,
    key: Vec<u8>,
    value: Vec<u8>,
    expires_at_micros: Option<u64>,
    expect: Option<CasExpect>,
    fence_key: Vec<u8>,
    fence_token: u64,
}

impl KvCasFencedRequest {
    /// Store raw `bytes` as the value (anything byte-like).
    pub fn bytes(mut self, payload: impl AsRef<[u8]>) -> Self {
        self.value = payload.as_ref().to_vec();
        self
    }

    /// Encode `value` with any [`Codec`] and store the bytes.
    pub fn encode_with<C, T>(mut self, value: &T) -> Result<Self, LaserError>
    where
        C: Codec<T>,
        T: ?Sized,
    {
        self.value = C::encode(value)?;
        Ok(self)
    }

    /// JSON-encode `value` and store the bytes.
    pub fn json<T: Serialize + ?Sized>(self, value: &T) -> Result<Self, LaserError> {
        self.encode_with::<crate::stream::Json, T>(value)
    }

    /// MessagePack-encode `value` and store the bytes.
    pub fn msgpack<T: Serialize + ?Sized>(self, value: &T) -> Result<Self, LaserError> {
        self.encode_with::<crate::stream::Msgpack, T>(value)
    }

    /// Expire the entry `ttl` from now.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.expires_at_micros = Some(now_micros().saturating_add(ttl.as_micros() as u64));
        self
    }

    /// Expire the entry at an absolute epoch-microseconds timestamp.
    pub fn expires_at(mut self, epoch_micros: u64) -> Self {
        self.expires_at_micros = Some(epoch_micros);
        self
    }

    /// Apply only if the key currently holds `version`.
    pub fn expect_version(mut self, version: u64) -> Self {
        self.expect = Some(CasExpect::Match(version));
        self
    }

    /// Apply only if the key does not yet exist (a create-if-absent).
    pub fn expect_absent(mut self) -> Self {
        self.expect = Some(CasExpect::Absent);
        self
    }

    /// Apply the fenced compare-and-swap, returning the entry's new version. A
    /// stale fence surfaces as
    /// [`KvError::LeaseLost`](laser_wire::kv::KvError::LeaseLost) (a newer holder
    /// bumped the sequence), a precondition miss as
    /// [`KvError::VersionConflict`](laser_wire::kv::KvError::VersionConflict).
    /// Returns `LaserError::Invalid` if no precondition was set, and
    /// `LaserError::Unsupported` when the deployment does not advertise the
    /// `kv_cas_fenced` capability.
    pub async fn commit(self) -> Result<u64, LaserError> {
        let Some(expect) = self.expect else {
            return Err(LaserError::Invalid(
                "commit() needs a precondition: call expect_version(..) or expect_absent()"
                    .to_owned(),
            ));
        };
        // Fail fast when the deployment does not advertise fenced compare-and-swap,
        // turning the unaware-server code rejection into the documented
        // `Unsupported` before the round-trip.
        if !self.laser.capabilities().await.kv.cas_fenced {
            return Err(LaserError::unsupported_feature(
                "kv",
                "cas_fenced",
                "fenced compare-and-swap is not advertised by this deployment",
            ));
        }
        let key = validated_key(&self.key)?;
        let fence_key = validated_key(&self.fence_key)?;
        validated_value(&self.value)?;
        let request = KvCasFenced {
            v: KV_OP_VERSION,
            namespace: self.namespace,
            key,
            value: self.value,
            expires_at_micros: self.expires_at_micros,
            expect,
            fence_key,
            fence_token: self.fence_token,
        };
        match self
            .laser
            .execute_kv(Some(&request.namespace), AGDX_KV_CAS_FENCED_CODE, &request)
            .await?
        {
            KvOutcome::Committed { version } => Ok(version),
            other => Err(unexpected("cas_fenced", &other)),
        }
    }
}

/// Fluent builder for `Kv::scan`. Narrow with `.prefix` or `.range`, cap with
/// `.limit`, then `.fetch()` for a page or `.entries()` for everything.
#[must_use = "call .fetch().await or .entries().await to read the page"]
pub struct KvScanRequest {
    laser: Laser,
    namespace: String,
    prefix: Option<Vec<u8>>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    key_contains: Option<String>,
    conversation: Option<String>,
    limit: usize,
    cursor: Option<Vec<u8>>,
}

impl KvScanRequest {
    /// The conversation lens: keep only entries the given conversation wrote.
    /// The memory read view stamps this on every record it materializes, so a
    /// scan of a memory namespace narrows to one conversation's memory. Generic
    /// key-value entries carry no conversation, so this filters them out.
    pub fn conversation(mut self, conversation: ConversationId) -> Self {
        self.conversation = Some(conversation.to_string());
        self
    }

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
        match self
            .laser
            .execute_kv(Some(&self.namespace), AGDX_KV_SCAN_CODE, &request)
            .await?
        {
            KvOutcome::Page(page) => Ok(page),
            other => Err(unexpected("scan", &other)),
        }
    }

    /// Walk every matching entry across pages, following the cursor until the
    /// scan is exhausted. Convenient when the working set fits in memory.
    pub async fn entries(self) -> Result<Vec<KvEntry>, LaserError> {
        let laser = self.laser.clone();
        let mut request = self.request();
        let mut out = Vec::new();
        loop {
            let page = match laser
                .execute_kv(Some(&request.namespace), AGDX_KV_SCAN_CODE, &request)
                .await?
            {
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
            conversation: self.conversation.clone(),
            limit: self.limit,
            cursor: self.cursor.clone(),
        }
    }
}

/// Fluent builder for `Kv::delete_many`. Narrow with `.prefix` / `.range` /
/// `.key_contains`, then `.send()` for the count removed.
#[must_use = "call .send().await to delete the matched entries"]
pub struct KvDeleteManyRequest {
    laser: Laser,
    namespace: String,
    prefix: Option<Vec<u8>>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    key_contains: Option<String>,
    conversation: Option<String>,
}

impl KvDeleteManyRequest {
    /// The conversation lens over a bulk delete: clear only the entries the
    /// given conversation wrote (a memory namespace's rows for one conversation).
    pub fn conversation(mut self, conversation: ConversationId) -> Self {
        self.conversation = Some(conversation.to_string());
        self
    }

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
            conversation: self.conversation,
        };
        match self
            .laser
            .execute_kv(Some(&request.namespace), AGDX_KV_DELETE_MANY_CODE, &request)
            .await?
        {
            KvOutcome::DeletedMany(removed) => Ok(removed),
            other => Err(unexpected("delete_many", &other)),
        }
    }
}

/// One copy or move, built by [`Kv::copy_to`] / [`Kv::move_to`], finished with
/// `.send().await`. `Committed`'s new destination version comes back.
/// An absent or expired source is the typed `KvError::NotFound`.
#[must_use = "call .send().await to copy or move the value"]
pub struct KvCopyRequest {
    laser: Laser,
    namespace: String,
    key: Vec<u8>,
    to_namespace: Option<String>,
    to_key: Vec<u8>,
    delete_source: bool,
}

impl KvCopyRequest {
    /// Send the copy into another namespace instead of the source's.
    pub fn into_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.to_namespace = Some(namespace.into());
        self
    }

    /// Apply the copy (or move) and return the destination's new version.
    pub async fn send(self) -> Result<u64, LaserError> {
        let key = validated_key(&self.key)?;
        let to_key = validated_key(&self.to_key)?;
        let code = if self.delete_source {
            AGDX_KV_MOVE_CODE
        } else {
            AGDX_KV_COPY_CODE
        };
        if let Some(to_namespace) = &self.to_namespace {
            laser_wire::kv::validate_namespace(to_namespace)?;
        }
        // KvMove mirrors KvCopy field-for-field, so the copy shape encodes both.
        let request = KvCopy {
            v: KV_OP_VERSION,
            namespace: self.namespace,
            key,
            to_namespace: self.to_namespace,
            to_key,
        };
        match self
            .laser
            .execute_kv(Some(&request.namespace), code, &request)
            .await?
        {
            KvOutcome::Committed { version } => Ok(version),
            other => Err(unexpected("copy", &other)),
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

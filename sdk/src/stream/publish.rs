use crate::error::LaserError;
use crate::laser::Laser;
use crate::stream::{Codec, ContentType, Json, Msgpack, Record};
use iggy::prelude::{HeaderKey, HeaderValue};
use serde::Serialize;
use std::collections::BTreeMap;

impl Laser {
    pub(crate) fn publish<'a>(&'a self, topic: &'a str) -> PublishRequest<'a> {
        PublishRequest::new(self, None, topic)
    }

    pub(crate) fn publish_on<'a>(&'a self, stream: &'a str, topic: &'a str) -> PublishRequest<'a> {
        PublishRequest::new(self, Some(stream), topic)
    }

    pub(crate) fn publish_batch<'a>(&'a self, topic: &'a str) -> BatchPublishRequest<'a> {
        BatchPublishRequest::new(self, None, topic)
    }

    pub(crate) fn publish_batch_on<'a>(
        &'a self,
        stream: &'a str,
        topic: &'a str,
    ) -> BatchPublishRequest<'a> {
        BatchPublishRequest::new(self, Some(stream), topic)
    }
}

/// Fluent builder for `Laser::publish`. Accumulates a `Record` + payload, then
/// `.send().await` performs the publish.
#[must_use = "call .send().await to publish the record"]
pub struct PublishRequest<'a> {
    laser: &'a Laser,
    stream: Option<&'a str>,
    topic: &'a str,
    record: Record,
    payload: Vec<u8>,
    partition_key: Option<String>,
    #[cfg(feature = "provenance")]
    provenance: Option<crate::provenance::Provenance>,
    #[cfg(feature = "agent")]
    claim_check: Option<(&'a dyn crate::blob::BlobStore, usize)>,
}

impl<'a> PublishRequest<'a> {
    fn new(laser: &'a Laser, stream: Option<&'a str>, topic: &'a str) -> Self {
        Self {
            laser,
            stream,
            topic,
            record: Record::builder().build(),
            payload: Vec::new(),
            partition_key: None,
            #[cfg(feature = "provenance")]
            provenance: None,
            #[cfg(feature = "agent")]
            claim_check: None,
        }
    }

    /// Stamp AGDX provenance (conversation id, causal parent, idempotency key,
    /// agent, deadline, ...) onto this record's headers, so a typed data-topic
    /// publish carries the same spine the agent path does. The partition key
    /// defaults to the conversation id (keeping one conversation ordered) unless
    /// an explicit [`partition_key`](Self::partition_key) was already set. Feature
    /// `provenance`.
    #[cfg(feature = "provenance")]
    pub fn provenance(mut self, provenance: &crate::provenance::Provenance) -> Self {
        if self.partition_key.is_none() {
            self.partition_key = Some(provenance.partition_key());
        }
        self.provenance = Some(provenance.clone());
        self
    }

    /// Pin this record to a partition by `key`: keyed partitioning sends every
    /// record sharing a key to the same partition (preserving per-key order),
    /// and spreads distinct keys across partitions. Without it the producer
    /// balances. Pick a key with the cardinality you want spread across the
    /// topic's partitions (a user id, a conversation id, an entity id).
    pub fn partition_key(mut self, key: impl Into<String>) -> Self {
        self.partition_key = Some(key.into());
        self
    }

    /// Stamp `agdx.ct` on the record. If never called (and no codec helper is
    /// used), the header is omitted and consumers treat the payload as opaque
    /// bytes. Codec helpers (`.json`, `.msgpack`, `.encode_with`, `.raw_bytes`)
    /// set this automatically. This method exists for the case where you
    /// already have bytes and want to tag them explicitly.
    pub fn content_type(mut self, value: ContentType) -> Self {
        self.record.content_type = Some(value);
        self
    }

    /// Stamp a projection ref on `agdx.ref`. Routes the record through
    /// the matching `Projection` in the worker's catalog when the topic has a
    /// binding that allows that ref. Use `"<name>.v<version>"` shape, e.g.
    /// `"order.v1"`, so producer and projector evolve together.
    pub fn projection_ref(mut self, value: impl Into<String>) -> Self {
        self.record.projection_ref = Some(value.into());
        self
    }

    /// Stamp a schema id on `agdx.sid`. **Reserved** for future
    /// infrastructure-native validation and codec dispatch. The projector does
    /// not use this for materialization routing, which is `projection_ref`'s job.
    pub fn schema_id(mut self, value: u32) -> Self {
        self.record.schema_id = Some(value);
        self
    }

    /// Index `value` under `agdx.idx.<key>` so queries can `where_eq`, `filter_*`,
    /// or `order_*` on it. Only index what you actually query, since the full
    /// object lives in the payload. Call it multiple times to add more indexed
    /// scalars to the record. String-valued so the query DSL's comparisons
    /// stay uniform over any caller-declared key, see [`header`](Self::header)
    /// for the exact-typed alternative one layer down.
    ///
    /// **A record with zero `.index(...)` calls is dropped by the projector.**
    /// Indexing is the explicit opt-in to materializing a queryable row.
    pub fn index(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.record.index.push((key.into(), value.into()));
        self
    }

    /// Attach a non-indexed metadata header (trace id, user tag, source).
    /// Rides through to the projector as a `metadata` map on each `Row`, free
    /// to inspect without `with_payload()`. The `agdx.idx.` prefix is reserved,
    /// so use `.index(...)` for queryable scalars.
    ///
    /// String-valued by design: this and `.index(...)` cross into the managed
    /// query plane, where every caller-declared key must compare and filter
    /// uniformly regardless of what a producer happened to send. For an exact
    /// Apache Iggy typed header (`bool`/`u8`../`u64`/raw bytes) on the wire
    /// itself, reach for the streaming layer below the query plane:
    /// `topic.producer()` (`ProducerMessage::header`), `topic.send(..)`, or
    /// the raw `topic.iggy_producer()` escape hatch.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.record.metadata.push((key.into(), value.into()));
        self
    }

    /// Opt in to inlining the payload bytes alongside the indexed row in the
    /// query DB. Off unless this is called. Iggy still persists the original
    /// message in the log either way. This controls whether the materialized
    /// index row also carries the body so consumers can fetch it back via
    /// `with_payload()`, `fetch_typed`, or `fetch_one`. Skip it for raw or
    /// oversized payloads the index has no business duplicating.
    pub fn inline_payload(mut self) -> Self {
        self.record.inline_payload = true;
        self
    }

    /// Use raw payload bytes (any format). Accepts anything byte-like
    /// (`Vec<u8>`, `&[u8]`, ...). For typed bodies prefer `.json(&body)`,
    /// `.msgpack(&body)`, or `.encode_with::<C>(&body)` for a custom codec.
    /// Chain `.inline_payload()` if readers need to fetch the bytes back
    /// through the query layer.
    pub fn payload(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.payload = payload.into();
        self
    }

    /// Encode `body` with a [`Codec`] (`Json`, `Msgpack`, or your own marker
    /// type). Stamps the codec's `ContentType` on `agdx.ct`. The generic path:
    /// `.json(&body)` and `.msgpack(&body)` are one-line sugar.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use laser_sdk::stream::{Json, Msgpack};
    /// # use serde::Serialize;
    /// # #[derive(Serialize)] struct Order { id: String }
    /// # async fn run(laser: &Laser, order: Order) -> Result<(), LaserError> {
    /// laser.topic("orders").publish()
    ///     .encode_with::<Json, _>(&order)?
    ///     .send().await?;
    /// laser.topic("orders").publish()
    ///     .encode_with::<Msgpack, _>(&order)?
    ///     .send().await?;
    /// # Ok(()) }
    /// ```
    pub fn encode_with<C, T>(mut self, body: &T) -> Result<Self, LaserError>
    where
        C: Codec<T>,
        T: ?Sized,
    {
        self.record.content_type = Some(C::content_type());
        self.payload = C::encode(body)?;
        Ok(self)
    }

    /// Convenience wrapper: `.encode_with::<Json, _>(body)`.
    pub fn json<T: Serialize>(self, body: &T) -> Result<Self, LaserError> {
        self.encode_with::<Json, T>(body)
    }

    /// Convenience wrapper: `.encode_with::<Msgpack, _>(body)`.
    pub fn msgpack<T: Serialize>(self, body: &T) -> Result<Self, LaserError> {
        self.encode_with::<Msgpack, T>(body)
    }

    /// One-line raw bytes plus content type. Equivalent to
    /// `.content_type(ct).payload(payload)`. Use this for Avro, Protobuf, CBOR,
    /// BSON, Arrow, or your own framing: encode the body with whichever crate
    /// you prefer, hand the bytes and the codec tag in one call.
    pub fn raw_bytes(mut self, payload: impl Into<Vec<u8>>, content_type: ContentType) -> Self {
        self.record.content_type = Some(content_type);
        self.payload = payload.into();
        self
    }

    /// Claim-check the body against `store` when it is at or over
    /// `threshold_bytes` at send time: the bytes are stored, hashed, and
    /// replaced on the log by the [`BodyRef`](laser_wire::agent::BodyRef)
    /// capsule with content-type `ref`. Under the threshold, byte-identical
    /// to no claim check. Readers resolve with
    /// [`blob::resolve_body`](crate::blob::resolve_body) (or
    /// `AgentMessage::resolve_body`), which digest-verifies before returning
    /// the bytes. Feature `agent`.
    #[cfg(feature = "agent")]
    pub fn claim_check(
        mut self,
        store: &'a dyn crate::blob::BlobStore,
        threshold_bytes: usize,
    ) -> Self {
        self.claim_check = Some((store, threshold_bytes));
        self
    }

    /// Encode `body` as a raw Avro datum under a registered writer schema and
    /// stamp `agdx.ct=avro` + `agdx.sid`. Compile the schema once
    /// with [`CompiledSchema::compile`](crate::schema_codecs::CompiledSchema::compile)
    /// and reuse it across publishes. Encoding fails client-side
    /// (`LaserError::Codec`) when the body does not match the schema, instead
    /// of a managed-side warning the producer cannot see.
    #[cfg(feature = "schema-codecs")]
    pub fn avro<T: Serialize>(
        mut self,
        schema: &crate::schema_codecs::CompiledSchema,
        schema_id: u32,
        body: &T,
    ) -> Result<Self, LaserError> {
        self.payload = schema.encode_avro(body)?;
        self.record.content_type = Some(ContentType::Avro);
        self.record.schema_id = Some(schema_id);
        Ok(self)
    }

    /// Publish the message.
    pub async fn send(self) -> Result<(), LaserError> {
        #[cfg(feature = "agent")]
        let mut this = self;
        #[cfg(not(feature = "agent"))]
        let this = self;
        #[cfg(feature = "agent")]
        {
            let stream = match this.stream {
                Some(stream) => stream,
                None => this.laser.stream_required()?,
            };
            #[cfg(feature = "provenance")]
            let provenance = this.provenance.as_ref();
            #[cfg(not(feature = "provenance"))]
            let provenance: Option<&crate::provenance::Provenance> = None;
            let action = crate::govern::GovernedAction {
                kind: crate::govern::ActionKind::Publish,
                stream,
                topic: this.topic,
                source: provenance
                    .and_then(|provenance| provenance.agent.as_ref())
                    .map(|agent| agent.as_str()),
                target: provenance
                    .and_then(|provenance| provenance.target_agent_id.as_ref())
                    .map(|agent| agent.as_str()),
                conversation: provenance.map(|provenance| provenance.conversation_id),
                correlation: provenance.and_then(|provenance| provenance.correlation_id.as_deref()),
                operation: None,
                tool: None,
                on_behalf_of: None,
                purpose: None,
                data_classification: None,
                payload: this.payload.as_ref(),
                signed: false,
                counters: crate::govern::ActionCounters::default(),
            };
            if let Some(modified) = this.laser.govern(action).await? {
                this.payload = modified;
            }
        }
        #[cfg(feature = "agent")]
        if let Some((store, threshold)) = this.claim_check {
            let (payload, replaced) = crate::blob::check_in(store, threshold, this.payload).await?;
            this.payload = payload;
            if let Some(content_type) = replaced {
                this.record.content_type = Some(content_type);
            }
        }
        let self_ = this;
        #[cfg(feature = "provenance")]
        let mut headers: BTreeMap<HeaderKey, HeaderValue> = (&self_.record).try_into()?;
        #[cfg(not(feature = "provenance"))]
        let headers: BTreeMap<HeaderKey, HeaderValue> = (&self_.record).try_into()?;
        #[cfg(feature = "provenance")]
        if let Some(provenance) = &self_.provenance {
            headers.extend(BTreeMap::<HeaderKey, HeaderValue>::try_from(provenance)?);
        }
        match self_.stream {
            Some(stream) => {
                self_
                    .laser
                    .send_with_headers_on(
                        stream,
                        self_.topic,
                        self_.payload,
                        headers,
                        self_.partition_key.as_deref(),
                    )
                    .await
            }
            None => {
                self_
                    .laser
                    .send_with_headers(
                        self_.topic,
                        self_.payload,
                        headers,
                        self_.partition_key.as_deref(),
                    )
                    .await
            }
        }
    }
}

/// Fluent builder for `Laser::publish_batch`. Accumulates 1..N records that all
/// share the topic, content_type, and inline-payload directive, then flushes
/// them in a single Iggy `send_messages` call. Schema-driven projectors do not
/// need per-record `.index(...)` headers, so a batch just encodes each body and
/// ships them together, the throughput-friendly default.
///
/// The per-record `add_*` methods append, and the terminal `.send().await`
/// flushes everything. Headers and indexed fields stamped on the batch apply to
/// every record (the typical case: schema-driven topics where producers ship
/// raw bodies). For per-record metadata, push a manually-built `Record` via
/// [`add_record`](Self::add_record).
#[must_use = "call .send().await to publish the batch"]
pub struct BatchPublishRequest<'a> {
    laser: &'a Laser,
    stream: Option<&'a str>,
    topic: &'a str,
    inline_payload: bool,
    content_type: Option<ContentType>,
    projection_ref: Option<String>,
    schema_id: Option<u32>,
    shared_index: Vec<(String, String)>,
    shared_metadata: Vec<(String, String)>,
    records: Vec<(Vec<u8>, Record)>,
    partition_key: Option<String>,
}

impl<'a> BatchPublishRequest<'a> {
    fn new(laser: &'a Laser, stream: Option<&'a str>, topic: &'a str) -> Self {
        Self {
            laser,
            stream,
            topic,
            inline_payload: false,
            content_type: None,
            projection_ref: None,
            schema_id: None,
            shared_index: Vec::new(),
            shared_metadata: Vec::new(),
            records: Vec::new(),
            partition_key: None,
        }
    }

    /// Set `inline_payload = true` for every record in the batch. Off by
    /// default: the projector keeps only indexed scalars + metadata unless
    /// this is called.
    pub fn inline_payload(mut self) -> Self {
        self.inline_payload = true;
        self
    }

    /// Stamp `agdx.ct` on every record in the batch that does not
    /// already carry its own. Three modes the batch supports together:
    ///   - set on the batch, and every record uses the default
    ///   - set on the batch, and some records override via codec helpers,
    ///     `add_raw_bytes(_with_projection)`, or `add_record`
    ///   - omitted on both, so records ride with no `agdx.ct` header
    ///     and consumers treat the payload as opaque bytes
    pub fn content_type(mut self, value: ContentType) -> Self {
        self.content_type = Some(value);
        self
    }

    /// Stamp the projection ref on every record in the batch. The projector
    /// looks the ref up in its catalog and applies the matching `Projection`.
    /// Per-record `add_*_with_projection` overrides win over this default,
    /// the right shape for heterogeneous batches.
    pub fn projection_ref(mut self, value: impl Into<String>) -> Self {
        self.projection_ref = Some(value.into());
        self
    }

    /// Stamp `schema_id` on every record. **Reserved** for future
    /// infrastructure-native validation and codec dispatch. The projector does
    /// not route on this. Use `projection_ref` for materialization.
    pub fn schema_id(mut self, value: u32) -> Self {
        self.schema_id = Some(value);
        self
    }

    /// Pin every record in this batch to one partition via `partition_key`.
    /// Without this, Iggy uses its balanced partitioner to choose one
    /// partition for the whole `send_messages` call. With it, every message
    /// lands on the same keyed partition, preserving per-key ordering.
    pub fn partition_key(mut self, value: impl Into<String>) -> Self {
        self.partition_key = Some(value.into());
        self
    }

    /// Add an `agdx.idx.<key>=value` header to every record in the batch.
    /// Typically callers do not need this when a schema is declared on the
    /// projector. It is useful for batches where every record shares a constant
    /// (e.g. a user id, a static message_type).
    pub fn index(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.shared_index.push((key.into(), value.into()));
        self
    }

    /// Add a metadata header to every record in the batch. String-valued for
    /// the same reason as [`PublishRequest::header`]: exact typed headers
    /// live one layer down, on `topic.producer()` / `topic.send(..)`.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.shared_metadata.push((key.into(), value.into()));
        self
    }

    /// Append a record encoded with a [`Codec`]. The generic path: `add_json`
    /// and `add_msgpack` are one-line sugar that dispatch through this. Plug in
    /// your own codec for Avro, Protobuf, CBOR, BSON, Arrow, or custom framing.
    pub fn add_encoded<C, T>(mut self, body: &T) -> Result<Self, LaserError>
    where
        C: Codec<T>,
        T: ?Sized,
    {
        let payload = C::encode(body)?;
        let record = Record::builder().content_type(C::content_type()).build();
        self.records.push((payload, record));
        Ok(self)
    }

    /// Append a record encoded with a [`Codec`] AND stamped with a
    /// `projection_ref`. Per-record routing override, the "different shapes
    /// on one topic" pattern.
    pub fn add_encoded_with_projection<C, T>(
        mut self,
        projection_ref: impl Into<String>,
        body: &T,
    ) -> Result<Self, LaserError>
    where
        C: Codec<T>,
        T: ?Sized,
    {
        let payload = C::encode(body)?;
        let record = Record::builder()
            .content_type(C::content_type())
            .projection_ref(projection_ref.into())
            .build();
        self.records.push((payload, record));
        Ok(self)
    }

    /// Convenience: `.add_encoded::<Json, _>(body)`.
    pub fn add_json<T: Serialize>(self, body: &T) -> Result<Self, LaserError> {
        self.add_encoded::<Json, T>(body)
    }

    /// Convenience: `.add_encoded::<Msgpack, _>(body)`.
    pub fn add_msgpack<T: Serialize>(self, body: &T) -> Result<Self, LaserError> {
        self.add_encoded::<Msgpack, T>(body)
    }

    /// Convenience: `.add_encoded_with_projection::<Json, _>(ref, body)`.
    pub fn add_json_with_projection<T: Serialize>(
        self,
        projection_ref: impl Into<String>,
        body: &T,
    ) -> Result<Self, LaserError> {
        self.add_encoded_with_projection::<Json, T>(projection_ref, body)
    }

    /// Convenience: `.add_encoded_with_projection::<Msgpack, _>(ref, body)`.
    pub fn add_msgpack_with_projection<T: Serialize>(
        self,
        projection_ref: impl Into<String>,
        body: &T,
    ) -> Result<Self, LaserError> {
        self.add_encoded_with_projection::<Msgpack, T>(projection_ref, body)
    }

    /// Append a record with raw payload bytes (any format). Pair with
    /// `.content_type(ContentType::*)` so consumers know how to decode.
    pub fn add_payload(mut self, payload: impl Into<Vec<u8>>) -> Self {
        self.records
            .push((payload.into(), Record::builder().build()));
        self
    }

    /// Append a raw-payload record plus content type in one call. Use it for
    /// Avro, Protobuf, or your own framing where the body is already encoded.
    pub fn add_raw_bytes(mut self, payload: impl Into<Vec<u8>>, content_type: ContentType) -> Self {
        let record = Record::builder().content_type(content_type).build();
        self.records.push((payload.into(), record));
        self
    }

    /// Append a record encoded as a raw Avro datum under a registered writer
    /// schema, stamped with `agdx.ct=avro` + `agdx.sid`. The batch
    /// counterpart of [`PublishRequest::avro`].
    #[cfg(feature = "schema-codecs")]
    pub fn add_avro<T: Serialize>(
        mut self,
        schema: &crate::schema_codecs::CompiledSchema,
        schema_id: u32,
        body: &T,
    ) -> Result<Self, LaserError> {
        let payload = schema.encode_avro(body)?;
        let record = Record::builder()
            .content_type(ContentType::Avro)
            .schema_id(schema_id)
            .build();
        self.records.push((payload, record));
        Ok(self)
    }

    /// Append a raw-payload record plus content type, stamped with a
    /// `projection_ref`. The body bytes stay opaque, and the projector routes
    /// via `agdx.ref`.
    pub fn add_raw_bytes_with_projection(
        mut self,
        projection_ref: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        content_type: ContentType,
    ) -> Self {
        let record = Record::builder()
            .content_type(content_type)
            .projection_ref(projection_ref.into())
            .build();
        self.records.push((payload.into(), record));
        self
    }

    /// Append a raw-payload record stamped with `projection_ref`. The body
    /// bytes stay opaque, and the projector routes via `agdx.ref` so different
    /// kinds of binary records (Avro, Protobuf, your own framing) can share a
    /// topic and project differently.
    pub fn add_payload_with_projection(
        mut self,
        projection_ref: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        let record = Record::builder()
            .projection_ref(projection_ref.into())
            .build();
        self.records.push((payload.into(), record));
        self
    }

    /// Append a record with explicit `Record` metadata (per-record `.index()`,
    /// custom headers). For the common case prefer the `add_json`,
    /// `add_msgpack`, or `add_encoded` plus batch-level `.index()` form. This
    /// is the escape hatch.
    pub fn add_record(mut self, payload: impl Into<Vec<u8>>, record: Record) -> Self {
        self.records.push((payload.into(), record));
        self
    }

    /// Append every `body` in `iter` encoded with `C`. Errors short-circuit
    /// on the first encoding failure.
    pub fn extend_encoded<C, I, T>(mut self, iter: I) -> Result<Self, LaserError>
    where
        I: IntoIterator<Item = T>,
        C: Codec<T>,
    {
        for body in iter {
            self = self.add_encoded::<C, T>(&body)?;
        }
        Ok(self)
    }

    /// Convenience: `.extend_encoded::<Json, _, _>(iter)`.
    pub fn extend_json<I, T>(self, iter: I) -> Result<Self, LaserError>
    where
        I: IntoIterator<Item = T>,
        T: Serialize,
    {
        self.extend_encoded::<Json, I, T>(iter)
    }

    /// Convenience: `.extend_encoded::<Msgpack, _, _>(iter)`.
    pub fn extend_msgpack<I, T>(self, iter: I) -> Result<Self, LaserError>
    where
        I: IntoIterator<Item = T>,
        T: Serialize,
    {
        self.extend_encoded::<Msgpack, I, T>(iter)
    }

    /// Number of records currently queued. Useful for callers that want to
    /// flush every N records.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the batch has no queued records yet.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Flush every queued record in a single Iggy `send_messages` call. An
    /// empty batch is a no-op. Returns the number of records sent.
    pub async fn send(self) -> Result<usize, LaserError> {
        if self.records.is_empty() {
            return Ok(0);
        }
        let count = self.records.len();
        let mut iggy_messages = Vec::with_capacity(count);
        let batch_content_type = self.content_type;
        let batch_projection_ref = self.projection_ref;
        let batch_schema_id = self.schema_id;
        let batch_inline_payload = self.inline_payload;
        let mut shared_index = self.shared_index;
        let mut shared_metadata = self.shared_metadata;
        let shared_present = !shared_index.is_empty() || !shared_metadata.is_empty();
        for (idx, (payload, mut record)) in self.records.into_iter().enumerate() {
            // Per-record metadata wins over batch-level defaults: a user that
            // wants to override content_type / projection_ref / schema_id /
            // inline_payload for one row in the batch can stamp it on the
            // Record directly via `add_record`. Batch-level values fill in
            // only when the record itself does not carry one. Three modes
            // all work: set on batch + all records inherit, set on batch +
            // some records override, omitted entirely on both -> no
            // `agdx.ct` header stamped on the wire.
            if record.content_type.is_none() {
                record.content_type = batch_content_type;
            }
            if record.projection_ref.is_none() {
                record.projection_ref = batch_projection_ref.clone();
            }
            if record.schema_id.is_none() {
                record.schema_id = batch_schema_id;
            }
            if !record.inline_payload {
                record.inline_payload = batch_inline_payload;
            }
            // Prepend shared values so per-record entries land LAST in the
            // vector, and the `TryFrom<&Record>` lowering inserts into a BTreeMap
            // in iteration order, so the last write wins for any key
            // collision. That gives per-record overrides priority over the
            // batch-wide default, the semantics callers expect. We move the
            // shared vectors into the FIRST record and clone them for the
            // rest, so a batch of N records with shared headers does N-1
            // clones instead of N (and zero clones when N == 1).
            let last = idx + 1 == count;
            if shared_present {
                let mut combined_index = if last {
                    std::mem::take(&mut shared_index)
                } else {
                    shared_index.clone()
                };
                let mut combined_metadata = if last {
                    std::mem::take(&mut shared_metadata)
                } else {
                    shared_metadata.clone()
                };
                combined_index.append(&mut record.index);
                combined_metadata.append(&mut record.metadata);
                record.index = combined_index;
                record.metadata = combined_metadata;
            }
            let headers: std::collections::BTreeMap<HeaderKey, HeaderValue> = (&record)
                .try_into()
                .map_err(|err: LaserError| LaserError::Invalid(format!("record #{idx}: {err}")))?;
            iggy_messages.push(
                iggy::prelude::IggyMessage::builder()
                    .payload(payload.into())
                    .user_headers(headers)
                    .build()?,
            );
        }
        match self.stream {
            Some(stream) => {
                self.laser
                    .send_batch_on(
                        stream,
                        self.topic,
                        iggy_messages,
                        self.partition_key.as_deref(),
                    )
                    .await?
            }
            None => {
                self.laser
                    .send_batch(self.topic, iggy_messages, self.partition_key.as_deref())
                    .await?
            }
        }
        Ok(count)
    }
}

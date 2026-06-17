use crate::error::LaserError;
use crate::laser::Laser;
use crate::query::{
    AGDX_GET_PROJECTION_CODE, AGDX_GET_SCHEMA_CODE, AGDX_LIST_PROJECTIONS_CODE,
    AGDX_LIST_SCHEMAS_CODE, AGDX_QUERY_CODE, AGDX_REGISTER_SCHEMA_CODE, AggCall, AggFunc,
    Aggregate, BrowseOutcome, BrowseReply, CONTROL_OP_VERSION, CmpOp, Codec, Consistency,
    ContentType, ControlCommand, ControlEnvelope, Decoder, Dir, Filter, GetProjection, GetSchema,
    Json, KeyMatch, ListProjections, ListSchemas, MAX_PAGE_SIZE, Msgpack, Projection,
    ProjectionBinding, ProjectionInfo, QUERY_OP_VERSION, Query, QueryEnvelope, QueryError,
    QueryReply, QueryResult, RawSql, Record, RegisterSchema, Row, SchemaInfo, SchemaSource, Select,
    Sort, SourceSelector, VECTOR_FIELD, Value, VectorQuery, Window,
};
use bytes::Bytes;
use iggy::prelude::{HeaderKey, HeaderValue};
use laser_wire::framing::encode_named;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

// Co-locate every control command on one partition so a `RegisterProjection` is
// applied before the `ApplyBinding` that references it.
const CONTROL_PARTITION_KEY: &str = "control";

impl Laser {
    /// Start publishing a single record to `topic`. Returns a fluent builder,
    /// finished with `.send().await`. For one-message-at-a-time producers. For
    /// throughput-sensitive paths see [`publish_batch`](Self::publish_batch).
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use serde::Serialize;
    /// # #[derive(Serialize)] struct Order { id: String, customer: String, amount: i64 }
    /// # async fn run(laser: &Laser, order: Order) -> Result<(), LaserError> {
    /// laser.publish("orders")
    ///     .index("order_id", &order.id)
    ///     .index("customer_id", &order.customer)
    ///     .index("total", order.amount.to_string())
    ///     .inline_payload()
    ///     .json(&order)?
    ///     .send().await?;
    /// # Ok(()) }
    /// ```
    pub fn publish<'a>(&'a self, topic: &'a str) -> PublishRequest<'a> {
        PublishRequest::new(self, None, topic)
    }

    /// Like [`publish`](Self::publish) but on an explicit `stream`, for
    /// connection-only handles or publishing across several streams from a
    /// single connection.
    pub fn publish_on<'a>(&'a self, stream: &'a str, topic: &'a str) -> PublishRequest<'a> {
        PublishRequest::new(self, Some(stream), topic)
    }

    /// Start publishing a batch of records to `topic`. Returns a fluent builder
    /// that accumulates 1..N records and flushes them in a single Iggy
    /// `send_messages` call, the path Iggy is built around for high
    /// throughput. Schema-driven projectors do not need per-record
    /// `.index(...)` headers, so the batch is just `[T; N]` plus the
    /// `inline_payload` directive (set once on the batch).
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use serde::Serialize;
    /// # #[derive(Serialize)] struct Order { id: String }
    /// # async fn run(laser: &Laser, orders: &[Order]) -> Result<(), LaserError> {
    /// laser.publish_batch("orders")
    ///     .inline_payload()
    ///     .extend_json(orders)?
    ///     .send().await?;
    /// # Ok(()) }
    /// ```
    pub fn publish_batch<'a>(&'a self, topic: &'a str) -> BatchPublishRequest<'a> {
        BatchPublishRequest::new(self, None, topic)
    }

    /// Like [`publish_batch`](Self::publish_batch) but on an explicit `stream`,
    /// for connection-only handles or batching across several streams from a
    /// single connection.
    pub fn publish_batch_on<'a>(
        &'a self,
        stream: &'a str,
        topic: &'a str,
    ) -> BatchPublishRequest<'a> {
        BatchPublishRequest::new(self, Some(stream), topic)
    }

    /// Start a query over `index`. Returns a fluent builder, finished with
    /// `.fetch().await` (paged `QueryResult`), `.fetch_typed::<T>().await`
    /// (typed `Vec<T>`), or `.fetch_one::<T>().await` (typed `Option<T>`).
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)] struct Order { customer: String, amount: i64 }
    /// # async fn run(laser: &Laser) -> Result<(), LaserError> {
    /// let alice: Vec<Order> = laser.query("orders")
    ///     .where_eq("customer_id", "alice")
    ///     .filter_gte("total", 100)
    ///     .order_desc("total")
    ///     .limit(10)
    ///     .with_payload()
    ///     .fetch_typed().await?;
    /// # Ok(()) }
    /// ```
    pub fn query<'a>(&'a self, index: &'a str) -> QueryRequest<'a> {
        QueryRequest::new(self, index)
    }

    /// Lower-level: send a fully-built `Record` to `topic`. Most callers want
    /// `publish(topic).index(...).json(...).send()` instead.
    pub async fn publish_record(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
        record: Record,
    ) -> Result<(), LaserError> {
        let headers: BTreeMap<HeaderKey, HeaderValue> = (&record).try_into()?;
        self.send_with_headers(topic, payload, headers, None).await
    }

    /// Lower-level: execute a pre-built `Query` and return the raw paged result.
    /// Most callers want `query(index).where_eq(...).fetch().await` instead.
    ///
    /// Query is a LaserData Cloud feature. Against raw Apache Iggy (`managed_query`
    /// false) this returns `LaserError::Unsupported`.
    pub async fn execute_query(&self, query: Query) -> Result<QueryResult, LaserError> {
        let capabilities = self.capabilities().await;
        if !capabilities.managed_query {
            return Err(LaserError::Unsupported(
                "query is a LaserData Cloud feature".to_owned(),
            ));
        }
        // Fail fast on a consistency level the server has not advertised.
        // The additive `consistency` field is silently ignored by a backend
        // that does not implement it, which then serves an eventual read,
        // so refusing locally is the only way to honor fail-not-downgrade
        // and never serve a silently stale read that looks successful.
        // The Eventual level is always served, so the default path holds.
        if !capabilities.serves_consistency(query.consistency) {
            return Err(LaserError::Unsupported(format!(
                "consistency level {:?} is not served by this deployment",
                query.consistency
            )));
        }
        // Fail fast on advertised version skew: the server told us at connect
        // which envelope version it accepts, so spend the typed error locally
        // instead of a decode failure (or a server-side Version error) after a
        // round-trip. Servers that advertise nothing skip this check.
        if let Some(versions) = capabilities.versions
            && versions.query != QUERY_OP_VERSION
        {
            return Err(QueryError::Version {
                expected: versions.query,
                got: QUERY_OP_VERSION,
            }
            .into());
        }
        // Fail fast on an over-cap page before the round trip: LaserData Cloud would
        // reject it with the same `TooLarge`, so spend the error locally. `0`
        // means "a full page" (LaserData Cloud defaults it to `MAX_PAGE_SIZE`), so only
        // an explicit over-cap value is rejected here. `top_k` on a vector query
        // is the same page bound under a different name.
        if query.limit > MAX_PAGE_SIZE {
            return Err(QueryError::TooLarge {
                what: "limit".to_owned(),
                size: query.limit,
                cap: MAX_PAGE_SIZE,
            }
            .into());
        }
        if let Some(vector) = &query.vector
            && vector.top_k > MAX_PAGE_SIZE
        {
            return Err(QueryError::TooLarge {
                what: "top_k".to_owned(),
                size: vector.top_k,
                cap: MAX_PAGE_SIZE,
            }
            .into());
        }
        let request = QueryEnvelope::new(query);
        let payload = Bytes::from(
            encode_named(&request)
                .map_err(|error| LaserError::Codec(format!("encode request: {error}")))?,
        );
        // The encoded query envelope rides the `AGDX_QUERY` managed command: the
        // server forwards it to LaserData Cloud over its local socket and returns
        // the `QueryReply` bytes, off the log, no reply topic, no correlation poll.
        let bytes = self
            .send_raw_with_response(AGDX_QUERY_CODE, payload)
            .await?;
        match crate::error::decode_managed_reply::<QueryReply>(&bytes)? {
            QueryReply::Ok(result) => Ok(result),
            QueryReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "query: unknown reply variant".to_owned(),
            )),
        }
    }

    /// Handle to the projection registry: `.register(projection)`,
    /// `.drop(id)`, `.get(id).await`, and the filterable `.list()` browse.
    /// Cheap to create, it borrows the connection.
    pub fn projections(&self) -> Projections<'_> {
        Projections { laser: self }
    }

    /// Handle to the binding surface: `.apply(binding)` routes a `(stream,
    /// topic)` source into registered projections, `.remove(source, ..)` stops
    /// it. Cheap to create, it borrows the connection.
    pub fn bindings(&self) -> Bindings<'_> {
        Bindings { laser: self }
    }

    /// Handle to the writer-schema registry (Avro, Protobuf):
    /// `.register(def)`, `.drop(id)`, `.get(id).await`, `.list().await`.
    /// Cheap to create, it borrows the connection.
    pub fn schemas(&self) -> Schemas<'_> {
        Schemas { laser: self }
    }

    // Read-only registry browse over the server's managed bridge, off the log. Gated
    // on `managed_host`: browse has no topic transport, so anything but the fork
    // (including raw Apache Iggy) returns `Unsupported`.
    async fn execute_browse(
        &self,
        code: u32,
        request: &impl Serialize,
    ) -> Result<BrowseOutcome, LaserError> {
        let capabilities = self.capabilities().await;
        if !capabilities.managed_host {
            return Err(LaserError::Unsupported(
                "projection browse requires LaserData Cloud".to_owned(),
            ));
        }
        // Browse rides the query envelope version. Fail fast on advertised skew.
        if let Some(versions) = capabilities.versions
            && versions.query != QUERY_OP_VERSION
        {
            return Err(QueryError::Version {
                expected: versions.query,
                got: QUERY_OP_VERSION,
            }
            .into());
        }
        let payload = Bytes::from(
            encode_named(request)
                .map_err(|error| LaserError::Codec(format!("encode browse request: {error}")))?,
        );
        let bytes = self.send_raw_with_response(code, payload).await?;
        match crate::error::decode_managed_reply::<BrowseReply>(&bytes)? {
            BrowseReply::Ok(outcome) => Ok(outcome),
            BrowseReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "browse: unknown reply variant".to_owned(),
            )),
        }
    }

    async fn publish_control(&self, command: ControlCommand) -> Result<(), LaserError> {
        let timestamp_micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_micros() as u64)
            .unwrap_or(0);
        let envelope = ControlEnvelope {
            v: CONTROL_OP_VERSION,
            timestamp_micros,
            command,
        };
        let payload = Bytes::from(
            encode_named(&envelope)
                .map_err(|error| LaserError::Codec(format!("encode control command: {error}")))?,
        );
        self.send_with_headers_on(
            self.ops_stream(),
            self.control_topic(),
            payload,
            BTreeMap::new(),
            Some(CONTROL_PARTITION_KEY),
        )
        .await
    }
}

/// Handle to the projection registry, built with
/// [`Laser::projections`](crate::laser::Laser::projections). Writes
/// (`register` / `drop`) publish control commands to `<ops>/control.commands`
/// and are applied asynchronously by LaserData Cloud (202-accepted semantics: poll
/// `.get(id)` to observe the apply). Reads (`get` / `list`) are managed-command
/// browses served off the registry snapshot.
pub struct Projections<'a> {
    laser: &'a Laser,
}

impl<'a> Projections<'a> {
    /// Register a [`Projection`] on LaserData Cloud by publishing a
    /// `RegisterProjection` control command. LaserData Cloud creates the backing
    /// table and starts applying the projection.
    pub async fn register(&self, projection: Projection) -> Result<(), LaserError> {
        self.laser
            .publish_control(ControlCommand::RegisterProjection(projection))
            .await
    }

    /// Drop a projection by publishing a `DropProjection` control command.
    /// LaserData Cloud stops applying it, and existing materialized rows are
    /// left untouched. `id` is the projection ref (e.g. `"order.v1"`).
    pub async fn drop(&self, id: impl Into<String>) -> Result<(), LaserError> {
        self.laser
            .publish_control(ControlCommand::DropProjection(id.into()))
            .await
    }

    /// Read one projection's details by `id` (schema, content type, indexed
    /// fields, bindings), or `None` when no projection has that id.
    pub async fn get(&self, id: impl Into<String>) -> Result<Option<ProjectionInfo>, LaserError> {
        let request = GetProjection {
            v: QUERY_OP_VERSION,
            id: id.into(),
        };
        match self
            .laser
            .execute_browse(AGDX_GET_PROJECTION_CODE, &request)
            .await?
        {
            BrowseOutcome::Projection(info) => Ok(info),
            _ => Err(LaserError::Protocol(
                "get: unexpected browse outcome".to_owned(),
            )),
        }
    }

    /// Browse the registry. Returns a builder: narrow with `.for_topic` /
    /// `.name_contains` / `.id_prefix`, then `.fetch().await` for the matching
    /// projections (each with its extraction schema, content type, indexed
    /// fields, and bindings). No filter lists every projection.
    pub fn list(&self) -> ProjectionsRequest<'a> {
        ProjectionsRequest {
            laser: self.laser,
            topics: Vec::new(),
            name_contains: None,
            id_prefix: None,
            search: None,
        }
    }
}

/// Handle to the binding surface, built with
/// [`Laser::bindings`](crate::laser::Laser::bindings). Writes publish control
/// commands to `<ops>/control.commands` (202-accepted, applied asynchronously
/// by LaserData Cloud). Bindings are browsed through the projections they route to:
/// `laser.projections().get(id)` returns each projection with its bindings.
pub struct Bindings<'a> {
    laser: &'a Laser,
}

impl Bindings<'_> {
    /// Apply a [`ProjectionBinding`] by publishing an `ApplyBinding` control
    /// command. LaserData Cloud starts a projector for the binding's `(stream,
    /// topic)` source. Register the referenced projection first, or LaserData Cloud
    /// rejects the binding.
    pub async fn apply(&self, binding: ProjectionBinding) -> Result<(), LaserError> {
        self.laser
            .publish_control(ControlCommand::ApplyBinding(binding))
            .await
    }

    /// Remove a binding by publishing a `RemoveBinding` control command. The
    /// LaserData Cloud stops the projector for `source`, so new records on that
    /// `(stream, topic)` no longer materialize. Rows already written stay.
    /// `projection_ref` scopes the removal to a single allowed projection when
    /// set, else removes the whole binding.
    pub async fn remove(
        &self,
        source: SourceSelector,
        projection_ref: Option<String>,
    ) -> Result<(), LaserError> {
        self.laser
            .publish_control(ControlCommand::RemoveBinding {
                source,
                projection_ref,
            })
            .await
    }
}

/// Handle to the writer-schema registry (Avro, Protobuf), built with
/// [`Laser::schemas`](crate::laser::Laser::schemas). Writes (`register` /
/// `drop`) publish control commands and are applied asynchronously by the
/// LaserData Cloud (202-accepted semantics: poll `.get(id)` until `Some` to observe the
/// apply). Reads are managed-command browses.
pub struct Schemas<'a> {
    laser: &'a Laser,
}

impl Schemas<'_> {
    /// Register a writer schema. Synchronous: LaserData Cloud validates that the
    /// definition compiles, allocates the next free id (ids are
    /// LaserData-Cloud-allocated and permanent, concurrent callers never collide),
    /// durably appends the control event, and `.send()` returns the id.
    /// Producers then stamp it on `agdx.sid` via the publish builders'
    /// `.schema_id(..)`. Browse visibility follows within the apply latency.
    pub fn register(&self, source: SchemaSource) -> RegisterSchemaRequest<'_> {
        RegisterSchemaRequest {
            laser: self.laser,
            source,
            name: None,
            version: None,
        }
    }

    /// Drop the writer schema registered under `id` by publishing a
    /// `DropSchema` control command. **Drop tombstones, it does not delete**:
    /// the id leaves the active set but records stamped with it keep
    /// decoding, and the id stays reserved against re-registration with a
    /// different definition. Dropping an unknown id is a no-op.
    pub async fn drop(&self, id: u32) -> Result<(), LaserError> {
        self.laser
            .publish_control(ControlCommand::DropSchema(id))
            .await
    }

    /// Read the writer schema occupying `id` (active or tombstoned, see
    /// [`SchemaInfo::dropped`]), or `None` when the id is free. Read-only browse.
    pub async fn get(&self, id: u32) -> Result<Option<SchemaInfo>, LaserError> {
        let request = GetSchema {
            v: QUERY_OP_VERSION,
            id,
        };
        match self
            .laser
            .execute_browse(AGDX_GET_SCHEMA_CODE, &request)
            .await?
        {
            BrowseOutcome::Schema(info) => Ok(info),
            _ => Err(LaserError::Protocol(
                "schema: unexpected browse outcome".to_owned(),
            )),
        }
    }

    /// List every known writer schema, active and tombstoned (see
    /// [`SchemaInfo::dropped`]). Read-only browse, listing every schema.
    pub async fn list(&self) -> Result<Vec<SchemaInfo>, LaserError> {
        let request = ListSchemas {
            v: QUERY_OP_VERSION,
            name_contains: None,
        };
        match self
            .laser
            .execute_browse(AGDX_LIST_SCHEMAS_CODE, &request)
            .await?
        {
            BrowseOutcome::Schemas(list) => Ok(list),
            _ => Err(LaserError::Protocol(
                "schemas: unexpected browse outcome".to_owned(),
            )),
        }
    }
}

/// Builder for the synchronous schema register. Terminal `.send().await`
/// returns LaserData Cloud-allocated id.
pub struct RegisterSchemaRequest<'a> {
    laser: &'a Laser,
    source: SchemaSource,
    name: Option<String>,
    version: Option<u32>,
}

impl RegisterSchemaRequest<'_> {
    /// Optional human label, pure metadata: stored and returned by the
    /// LaserData Cloud, never dispatched on and not unique.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Optional caller-tracked schema version, pure metadata: stored and
    /// returned by LaserData Cloud, never dispatched on.
    pub fn version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    /// Execute the register and return LaserData Cloud-allocated id. Typed errors:
    /// an uncompilable definition answers `Unsupported` (nothing allocated),
    /// transport/server trouble answers `Backend`.
    pub async fn send(self) -> Result<u32, LaserError> {
        let request = RegisterSchema {
            v: QUERY_OP_VERSION,
            source: self.source,
            name: self.name,
            version: self.version,
        };
        match self
            .laser
            .execute_browse(AGDX_REGISTER_SCHEMA_CODE, &request)
            .await?
        {
            BrowseOutcome::SchemaRegistered(id) => Ok(id),
            _ => Err(LaserError::Protocol(
                "register schema: unexpected outcome".to_owned(),
            )),
        }
    }
}

/// Fluent builder for `Laser::publish`. Accumulates a `Record` + payload, then
/// `.send().await` performs the publish.
pub struct PublishRequest<'a> {
    laser: &'a Laser,
    stream: Option<&'a str>,
    topic: &'a str,
    record: Record,
    payload: Vec<u8>,
    partition_key: Option<String>,
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
        }
    }

    /// Pin this record to a partition by `key`: keyed partitioning sends every
    /// record sharing a key to the same partition (preserving per-key order),
    /// and spreads distinct keys across partitions. Without it the producer
    /// balances. Pick a key with the cardinality you want spread across the
    /// topic's partitions (a user id, a user id, an entity id).
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
    /// scalars to the record.
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
    pub fn payload(mut self, bytes: impl Into<Vec<u8>>) -> Self {
        self.payload = bytes.into();
        self
    }

    /// Encode `body` with a [`Codec`] (`Json`, `Msgpack`, or your own marker
    /// type). Stamps the codec's `ContentType` on `agdx.ct`. The generic path:
    /// `.json(&body)` and `.msgpack(&body)` are one-line sugar.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use laser_sdk::query::{Json, Msgpack};
    /// # use serde::Serialize;
    /// # #[derive(Serialize)] struct Order { id: String }
    /// # async fn run(laser: &Laser, order: Order) -> Result<(), LaserError> {
    /// laser.publish("orders")
    ///     .encode_with::<Json, _>(&order)?
    ///     .send().await?;
    /// laser.publish("orders")
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
    /// `.content_type(ct).payload(bytes)`. Use this for Avro, Protobuf, CBOR,
    /// BSON, Arrow, or your own framing: encode the body with whichever crate
    /// you prefer, hand the bytes and the codec tag in one call.
    pub fn raw_bytes(mut self, bytes: impl Into<Vec<u8>>, content_type: ContentType) -> Self {
        self.record.content_type = Some(content_type);
        self.payload = bytes.into();
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
        let headers: BTreeMap<HeaderKey, HeaderValue> = (&self.record).try_into()?;
        match self.stream {
            Some(stream) => {
                self.laser
                    .send_with_headers_on(
                        stream,
                        self.topic,
                        self.payload,
                        headers,
                        self.partition_key.as_deref(),
                    )
                    .await
            }
            None => {
                self.laser
                    .send_with_headers(
                        self.topic,
                        self.payload,
                        headers,
                        self.partition_key.as_deref(),
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

    /// Add a metadata header to every record in the batch.
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
        let bytes = C::encode(body)?;
        let record = Record::builder().content_type(C::content_type()).build();
        self.records.push((bytes, record));
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
        let bytes = C::encode(body)?;
        let record = Record::builder()
            .content_type(C::content_type())
            .projection_ref(projection_ref.into())
            .build();
        self.records.push((bytes, record));
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
    pub fn add_payload(mut self, bytes: impl Into<Vec<u8>>) -> Self {
        self.records.push((bytes.into(), Record::builder().build()));
        self
    }

    /// Append a raw-payload record plus content type in one call. Use it for
    /// Avro, Protobuf, or your own framing where the body is already encoded.
    pub fn add_raw_bytes(mut self, bytes: impl Into<Vec<u8>>, content_type: ContentType) -> Self {
        let record = Record::builder().content_type(content_type).build();
        self.records.push((bytes.into(), record));
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
        let bytes = schema.encode_avro(body)?;
        let record = Record::builder()
            .content_type(ContentType::Avro)
            .schema_id(schema_id)
            .build();
        self.records.push((bytes, record));
        Ok(self)
    }

    /// Append a raw-payload record plus content type, stamped with a
    /// `projection_ref`. The body bytes stay opaque, and the projector routes
    /// via `agdx.ref`.
    pub fn add_raw_bytes_with_projection(
        mut self,
        projection_ref: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
        content_type: ContentType,
    ) -> Self {
        let record = Record::builder()
            .content_type(content_type)
            .projection_ref(projection_ref.into())
            .build();
        self.records.push((bytes.into(), record));
        self
    }

    /// Append a raw-payload record stamped with `projection_ref`. The body
    /// bytes stay opaque, and the projector routes via `agdx.ref` so different
    /// kinds of binary records (Avro, Protobuf, your own framing) can share a
    /// topic and project differently.
    pub fn add_payload_with_projection(
        mut self,
        projection_ref: impl Into<String>,
        bytes: impl Into<Vec<u8>>,
    ) -> Self {
        let record = Record::builder()
            .projection_ref(projection_ref.into())
            .build();
        self.records.push((bytes.into(), record));
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
            // batch-wide default - the semantics callers expect. We move the
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
                    .payload(Bytes::from(payload))
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

/// Fluent builder for `Laser::query`. Accumulates a `Query`, then `.fetch()`
/// returns the paged result, `.fetch_typed::<T>()` deserializes every row's
/// payload into `T`, `.fetch_one::<T>()` is the same but for at most one row.
pub struct QueryRequest<'a> {
    laser: &'a Laser,
    query: Query,
}

impl<'a> QueryRequest<'a> {
    fn new(laser: &'a Laser, index: &'a str) -> Self {
        Self {
            laser,
            query: Query::builder().index(index).build(),
        }
    }

    /// Exact-match on an indexed field (point lookup).
    pub fn where_eq(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.by_key.push(KeyMatch::new(field, value));
        self
    }

    /// Resolve this query against a fork's copy-on-write view (the trunk overlaid
    /// with the fork's speculative rows) instead of the trunk. Open the fork with
    /// [`Laser::fork`](crate::laser::Laser::fork).
    pub fn fork(mut self, fork_id: impl Into<String>) -> Self {
        self.query.fork = Some(fork_id.into());
        self
    }

    /// Filter rows where `field == value`.
    pub fn filter_eq(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Eq, value)
    }

    /// Filter rows where `field != value`.
    pub fn filter_ne(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Ne, value)
    }

    /// Filter rows where `field > value` (numeric if both parse as numbers, else lexical).
    pub fn filter_gt(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Gt, value)
    }

    /// Filter rows where `field >= value`.
    pub fn filter_gte(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Gte, value)
    }

    /// Filter rows where `field < value`.
    pub fn filter_lt(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Lt, value)
    }

    /// Filter rows where `field <= value`.
    pub fn filter_lte(self, field: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(field, CmpOp::Lte, value)
    }

    /// Filter rows where `field` is one of the given values.
    pub fn filter_in<T: Into<Value>>(
        self,
        field: impl Into<String>,
        values: impl IntoIterator<Item = T>,
    ) -> Self {
        let list = Value::List(values.into_iter().map(Into::into).collect());
        self.predicate(field, CmpOp::In, list)
    }

    /// Filter rows where `field` contains `value` (substring).
    pub fn filter_contains(self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.predicate(field, CmpOp::Contains, Value::Str(value.into()))
    }

    /// Filter rows where `field` starts with `value`.
    pub fn filter_prefix(self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.predicate(field, CmpOp::Prefix, Value::Str(value.into()))
    }

    /// Filter on `agdx.idx.message_type`.
    pub fn message_type(mut self, value: impl Into<String>) -> Self {
        self.query.message_type = Some(value.into());
        self
    }

    /// Filter rows whose `agdx.idx.ts` (epoch micros) falls in `[start, end]`.
    pub fn time_range(mut self, start: u64, end: u64) -> Self {
        self.query.time_range = Some((start, end));
        self
    }

    /// Sort ascending by `field` (numeric-then-lexical).
    pub fn order_asc(mut self, field: impl Into<String>) -> Self {
        self.query.order.push(Sort {
            field: field.into(),
            dir: Dir::Asc,
        });
        self
    }

    /// Sort descending by `field` (numeric-then-lexical).
    pub fn order_desc(mut self, field: impl Into<String>) -> Self {
        self.query.order.push(Sort {
            field: field.into(),
            dir: Dir::Desc,
        });
        self
    }

    /// Limit the page to `n` rows. Above `MAX_PAGE_SIZE` is rejected with
    /// `QueryError::TooLarge`. `0` means a full page.
    pub fn limit(mut self, n: usize) -> Self {
        self.query.limit = n;
        self
    }

    /// Skip the first `n` matching rows.
    pub fn offset(mut self, n: usize) -> Self {
        self.query.offset = n;
        self
    }

    /// Return the opaque payload bytes on each row (so you can decode them).
    pub fn with_payload(mut self) -> Self {
        self.query.select.payload = true;
        self
    }

    /// Require read-your-writes: wait for the projector to apply the source log
    /// up to its current head before serving, so this query sees writes that
    /// completed before it. Bounded - if the projector cannot catch up in time
    /// the query returns `QueryError::Stale` (`LaserError::is_stale()`) instead
    /// of silently serving older data. Backend-gated: a deployment that cannot
    /// honor it returns `Unsupported`.
    pub fn read_your_writes(mut self) -> Self {
        self.query.consistency = Consistency::ReadYourWrites;
        self
    }

    /// Set the read-consistency level explicitly (`Eventual` is the default,
    /// `ReadYourWrites`, or `Strong`). `Strong` is gated by `strong_consistency`.
    pub fn consistency(mut self, level: Consistency) -> Self {
        self.query.consistency = level;
        self
    }

    /// Project only the named indexed fields into each row's headers.
    pub fn select_fields<S: Into<String>>(mut self, fields: impl IntoIterator<Item = S>) -> Self {
        self.query.select.fields = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Aggregate: row count, output under the header `count`.
    pub fn count(self) -> Self {
        self.push_agg(agg_call(AggFunc::Count, None, None, "count"))
    }

    /// Aggregate: sum of `field`, output under the header `sum`.
    pub fn sum(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Sum, Some(field.into()), None, "sum"))
    }

    /// Aggregate: arithmetic mean of `field`, output under the header `avg`.
    pub fn avg(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Avg, Some(field.into()), None, "avg"))
    }

    /// Aggregate: minimum of `field`, output under the header `min`.
    pub fn min(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Min, Some(field.into()), None, "min"))
    }

    /// Aggregate: maximum of `field`, output under the header `max`.
    pub fn max(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(AggFunc::Max, Some(field.into()), None, "max"))
    }

    /// Aggregate: distinct count of `field`, output under the header
    /// `count_distinct`.
    pub fn count_distinct(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(
            AggFunc::CountDistinct,
            Some(field.into()),
            None,
            "count_distinct",
        ))
    }

    /// Aggregate: population standard deviation of `field`, output under the
    /// header `stddev`. Backend-gated (columnar backends only).
    pub fn stddev(self, field: impl Into<String>) -> Self {
        self.push_agg(agg_call(
            AggFunc::StdDev,
            Some(field.into()),
            None,
            "stddev",
        ))
    }

    /// Aggregate: the `fraction` quantile of `field` (e.g. 0.95 for p95), output
    /// under the header `percentile`. Backend-gated (columnar backends only).
    pub fn percentile(self, field: impl Into<String>, fraction: f64) -> Self {
        self.push_agg(agg_call(
            AggFunc::Percentile,
            Some(field.into()),
            Some(fraction),
            "percentile",
        ))
    }

    /// Add an aggregate with an explicit output alias. Use to return several
    /// aggregates of the same kind in one query, or to name the output column.
    pub fn agg_as(
        self,
        func: AggFunc,
        field: Option<String>,
        fraction: Option<f64>,
        alias: impl Into<String>,
    ) -> Self {
        let alias = alias.into();
        self.push_agg(AggCall {
            func,
            field,
            arg: fraction,
            alias,
        })
    }

    /// Group the aggregate by the named fields.
    pub fn group_by<S: Into<String>>(mut self, fields: impl IntoIterator<Item = S>) -> Self {
        let group_by: Vec<String> = fields.into_iter().map(Into::into).collect();
        match self.query.aggregate.as_mut() {
            Some(aggregate) => aggregate.group_by = group_by,
            None => {
                self.query.aggregate = Some(Aggregate {
                    group_by,
                    funcs: Vec::new(),
                    window: None,
                });
            }
        }
        self
    }

    /// Bucket the aggregate into tumbling windows of `every_micros` over the
    /// timestamp `field`. Each result row carries a `window_start` header (the
    /// bucket's lower edge in epoch micros).
    pub fn window(mut self, field: impl Into<String>, every_micros: u64) -> Self {
        let window = Some(Window {
            field: field.into(),
            every_micros,
        });
        match self.query.aggregate.as_mut() {
            Some(aggregate) => aggregate.window = window,
            None => {
                self.query.aggregate = Some(Aggregate {
                    group_by: Vec::new(),
                    funcs: Vec::new(),
                    window,
                });
            }
        }
        self
    }

    /// Keep only aggregate groups matching `filter`. Predicate fields reference
    /// an aggregate alias (e.g. `count`) or a group key, not raw row fields.
    pub fn having(mut self, filter: Filter) -> Self {
        self.query.having = Some(filter);
        self
    }

    /// Return only distinct rows over the projected fields. Requires
    /// [`select_fields`](Self::select_fields).
    pub fn distinct(mut self) -> Self {
        self.query.distinct = true;
        self
    }

    /// Opt-in raw-SQL escape hatch: run `sql` (a single read-only SELECT) on the
    /// index's backend. SQL backends only, not portable. Result columns come
    /// back as row headers keyed by column name.
    pub fn raw_sql(mut self, sql: impl Into<String>) -> Self {
        self.query.raw_sql = Some(RawSql {
            sql: sql.into(),
            params: Vec::new(),
        });
        self
    }

    /// Like [`raw_sql`](Self::raw_sql) but with positional bind params.
    pub fn raw_sql_with<V: Into<Value>>(
        mut self,
        sql: impl Into<String>,
        params: impl IntoIterator<Item = V>,
    ) -> Self {
        self.query.raw_sql = Some(RawSql {
            sql: sql.into(),
            params: params.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Approximate nearest-neighbour search against the default vector field
    /// (`VECTOR_FIELD` = `"embedding"`). Use [`nearest_in`](Self::nearest_in) to
    /// point at a different field name.
    pub fn nearest(self, embedding: Vec<f32>, top_k: usize) -> Self {
        self.nearest_in(VECTOR_FIELD, embedding, top_k)
    }

    /// Approximate nearest-neighbour search on an explicit `field`. Use when
    /// your projector stores the vector under a custom payload key.
    pub fn nearest_in(
        mut self,
        field: impl Into<String>,
        embedding: Vec<f32>,
        top_k: usize,
    ) -> Self {
        self.query.vector = Some(VectorQuery {
            field: field.into(),
            embedding,
            top_k,
        });
        self
    }

    /// Run the query and return the paged result + metadata.
    pub async fn fetch(self) -> Result<QueryResult, LaserError> {
        self.laser.execute_query(self.query).await
    }

    /// Run the query, then deserialize every row's payload into `T` (JSON).
    /// `with_payload()` is implied. Single-page only: use `.stream_typed()` or
    /// `.fetch_all_typed()` if there may be more than `MAX_PAGE_SIZE` matches.
    pub async fn fetch_typed<T: DeserializeOwned>(mut self) -> Result<Vec<T>, LaserError> {
        self.query.select.payload = true;
        let result = self.laser.execute_query(self.query).await?;
        result
            .rows
            .iter()
            .map(|row| row.decode_json::<T>().map_err(LaserError::from))
            .collect()
    }

    /// Like `fetch_typed` but caps the result at one row.
    pub async fn fetch_one<T: DeserializeOwned>(mut self) -> Result<Option<T>, LaserError> {
        self.query.select.payload = true;
        self.query.limit = 1;
        let result = self.laser.execute_query(self.query).await?;
        match result.rows.first() {
            Some(row) => row.decode_json::<T>().map(Some).map_err(LaserError::from),
            None => Ok(None),
        }
    }

    /// Like [`fetch_typed`](Self::fetch_typed) but decode each row's payload
    /// with any [`Decoder`] (`Json`, `Msgpack`, or your own codec) instead of
    /// being locked to JSON. `with_payload()` is implied.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use laser_sdk::query::Msgpack;
    /// # use serde::Deserialize;
    /// # #[derive(Deserialize)] struct Order { id: String }
    /// # async fn run(laser: &Laser) -> Result<(), LaserError> {
    /// let orders: Vec<Order> = laser.query("orders").fetch_typed_with::<Msgpack, _>().await?;
    /// # Ok(()) }
    /// ```
    pub async fn fetch_typed_with<C, T>(mut self) -> Result<Vec<T>, LaserError>
    where
        C: Decoder<T>,
    {
        self.query.select.payload = true;
        let result = self.laser.execute_query(self.query).await?;
        result
            .rows
            .iter()
            .map(|row| row.decode_with::<C, T>().map_err(LaserError::from))
            .collect()
    }

    /// Like [`fetch_one`](Self::fetch_one) but decode the row with any
    /// [`Decoder`] instead of JSON.
    pub async fn fetch_one_with<C, T>(mut self) -> Result<Option<T>, LaserError>
    where
        C: Decoder<T>,
    {
        self.query.select.payload = true;
        self.query.limit = 1;
        let result = self.laser.execute_query(self.query).await?;
        match result.rows.first() {
            Some(row) => row
                .decode_with::<C, T>()
                .map(Some)
                .map_err(LaserError::from),
            None => Ok(None),
        }
    }

    /// Walk every matching row across pages. Each `.next().await` yields the
    /// next row. The stream auto-paginates with `limit` (or 100 if unset) and
    /// stops when the worker reports `has_more = false` (or an empty page,
    /// whichever comes first). Aggregate and vector queries are single-page by
    /// construction: the stream yields exactly one page and then finishes,
    /// because `offset` is not a meaningful cursor for either shape.
    ///
    /// Pagination is offset-based, so concurrent writes against the underlying
    /// index can produce duplicate or skipped rows. Use this for snapshot-style
    /// reads. For live tailing, prefer a topic consumer.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # async fn run(laser: &Laser) -> Result<(), LaserError> {
    /// let mut rows = laser.query("orders").where_eq("status", "paid").stream();
    /// while let Some(row) = rows.next().await? {
    ///     // process row
    ///     let _ = row;
    /// }
    /// # Ok(()) }
    /// ```
    pub fn stream(mut self) -> QueryStream<'a> {
        if self.query.limit == 0 {
            self.query.limit = crate::query::DEFAULT_STREAM_PAGE_SIZE;
        }
        let single_page = self.query.aggregate.is_some() || self.query.vector.is_some();
        QueryStream::new(self.laser, self.query, single_page)
    }

    /// Like [`stream`](Self::stream) but each yield is `T` decoded from the
    /// row's payload. `with_payload()` is implied, and the publisher must have
    /// chained `.inline_payload()` for the bytes to be there.
    pub fn stream_typed<T: DeserializeOwned>(mut self) -> TypedQueryStream<'a, T> {
        self.query.select.payload = true;
        if self.query.limit == 0 {
            self.query.limit = crate::query::DEFAULT_STREAM_PAGE_SIZE;
        }
        let single_page = self.query.aggregate.is_some() || self.query.vector.is_some();
        TypedQueryStream::new(self.laser, self.query, single_page)
    }

    /// Materialize EVERY matching row by walking pages internally. Convenient
    /// when you need them all and the working set fits comfortably in memory.
    pub async fn fetch_all(self) -> Result<Vec<Row>, LaserError> {
        let mut stream = self.stream();
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// Materialize EVERY matching row, decoded into `T`.
    pub async fn fetch_all_typed<T: DeserializeOwned>(self) -> Result<Vec<T>, LaserError> {
        let mut stream = self.stream_typed::<T>();
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// Inspect the raw `Query` the builder produced (debugging).
    pub fn into_query(self) -> Query {
        self.query
    }

    fn predicate(self, field: impl Into<String>, op: CmpOp, value: impl Into<Value>) -> Self {
        self.and_filter(Filter::pred(field, op, value))
    }

    /// AND `filter` into the query's predicate tree. The fluent `filter_*`
    /// helpers route through here, so chained filters compose as a conjunction.
    /// Build `Any`/`Not` subtrees with [`Filter::any`]/[`Filter::not`] and pass
    /// them here.
    pub fn filter(self, filter: Filter) -> Self {
        self.and_filter(filter)
    }

    fn and_filter(mut self, filter: Filter) -> Self {
        self.query.filter = Some(match self.query.filter.take() {
            None => filter,
            Some(Filter::All(mut existing)) => {
                existing.push(filter);
                Filter::All(existing)
            }
            Some(other) => Filter::All(vec![other, filter]),
        });
        self
    }

    fn push_agg(mut self, call: AggCall) -> Self {
        match self.query.aggregate.as_mut() {
            Some(aggregate) => aggregate.funcs.push(call),
            None => {
                self.query.aggregate = Some(Aggregate {
                    group_by: Vec::new(),
                    funcs: vec![call],
                    window: None,
                });
            }
        }
        self
    }
}

/// Build an [`AggCall`] with the given output alias.
fn agg_call(func: AggFunc, field: Option<String>, arg: Option<f64>, alias: &str) -> AggCall {
    AggCall {
        func,
        field,
        arg,
        alias: alias.to_owned(),
    }
}

impl<'a> From<QueryRequest<'a>> for Query {
    fn from(request: QueryRequest<'a>) -> Self {
        request.query
    }
}

/// Auto-paginating row stream returned by `QueryRequest::stream`. Holds the
/// `Query` and refills its buffer by re-issuing it with an advanced `offset`
/// each time the local page drains, until the worker reports `has_more = false`
/// (or returns an empty page, whichever comes first). The empty-page guard
/// rules out an infinite loop if the worker ever skews on the `has_more` flag.
pub struct QueryStream<'a> {
    laser: &'a Laser,
    query: Query,
    finished: bool,
    // Aggregate / vector queries do not have a meaningful offset cursor, so the
    // stream fetches once and stops regardless of `has_more`.
    single_page: bool,
    buffer: std::vec::IntoIter<Row>,
}

impl<'a> QueryStream<'a> {
    fn new(laser: &'a Laser, query: Query, single_page: bool) -> Self {
        Self {
            laser,
            query,
            finished: false,
            single_page,
            buffer: Vec::new().into_iter(),
        }
    }

    /// Yield the next row, fetching the next page if the local buffer is empty.
    /// Returns `Ok(None)` after the final row of the last page.
    pub async fn next(&mut self) -> Result<Option<Row>, LaserError> {
        if let Some(row) = self.buffer.next() {
            return Ok(Some(row));
        }
        if self.finished {
            return Ok(None);
        }
        let page = self.laser.execute_query(self.query.clone()).await?;
        let fetched = page.rows.len();
        // Empty page always terminates - belt-and-braces against a worker that
        // mis-reports `has_more` and would otherwise wedge the loop.
        self.finished = self.single_page || fetched == 0 || !page.page.has_more;
        self.query.offset = self.query.offset.saturating_add(fetched);
        self.buffer = page.rows.into_iter();
        Ok(self.buffer.next())
    }
}

/// Auto-paginating typed stream returned by `QueryRequest::stream_typed`. Each
/// `.next().await` decodes the next row's payload into `T`.
pub struct TypedQueryStream<'a, T> {
    inner: QueryStream<'a>,
    _marker: std::marker::PhantomData<fn() -> T>,
}

impl<'a, T: DeserializeOwned> TypedQueryStream<'a, T> {
    fn new(laser: &'a Laser, query: Query, single_page: bool) -> Self {
        Self {
            inner: QueryStream::new(laser, query, single_page),
            _marker: std::marker::PhantomData,
        }
    }

    /// Yield the next decoded value, or `Ok(None)` after the last page.
    pub async fn next(&mut self) -> Result<Option<T>, LaserError> {
        match self.inner.next().await? {
            Some(row) => row.decode_json::<T>().map(Some).map_err(LaserError::from),
            None => Ok(None),
        }
    }
}

/// Fluent builder for [`Projections::list`]. Narrow the registry browse with
/// `.for_topic` / `.for_topics` / `.name_contains` / `.id_prefix`, then
/// `.fetch().await`. No filter lists every projection.
pub struct ProjectionsRequest<'a> {
    laser: &'a Laser,
    topics: Vec<String>,
    name_contains: Option<String>,
    id_prefix: Option<String>,
    search: Option<String>,
}

impl<'a> ProjectionsRequest<'a> {
    /// Keep only projections bound to `topic`. Repeatable.
    pub fn for_topic(mut self, topic: impl Into<String>) -> Self {
        self.topics.push(topic.into());
        self
    }

    /// Keep only projections bound to any of `topics`.
    pub fn for_topics<S: Into<String>>(mut self, topics: impl IntoIterator<Item = S>) -> Self {
        self.topics.extend(topics.into_iter().map(Into::into));
        self
    }

    /// Keep only projections whose name contains `substring`.
    pub fn name_contains(mut self, substring: impl Into<String>) -> Self {
        self.name_contains = Some(substring.into());
        self
    }

    /// Keep only projections whose id starts with `prefix`.
    pub fn id_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.id_prefix = Some(prefix.into());
        self
    }

    /// Keep only projections whose name or id matches `substring`, the
    /// single-box convenience filter. Composes with the narrower filters.
    pub fn search(mut self, substring: impl Into<String>) -> Self {
        self.search = Some(substring.into());
        self
    }

    /// Run the browse and return the matching projections.
    pub async fn fetch(self) -> Result<Vec<ProjectionInfo>, LaserError> {
        let request = ListProjections {
            v: QUERY_OP_VERSION,
            topics: self.topics,
            name_contains: self.name_contains,
            id_prefix: self.id_prefix,
            search: self.search,
        };
        match self
            .laser
            .execute_browse(AGDX_LIST_PROJECTIONS_CODE, &request)
            .await?
        {
            BrowseOutcome::Projections(list) => Ok(list),
            _ => Err(LaserError::Protocol(
                "list: unexpected browse outcome".to_owned(),
            )),
        }
    }
}

// `Select` field needs to be public for the builder access above.
#[allow(dead_code)]
fn _select_field_is_pub(s: Select) -> Select {
    s
}

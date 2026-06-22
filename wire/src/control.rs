use crate::content::ContentType;
use crate::error::InvalidError;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// One indexed scalar: the index key `name` and the RFC-6901 JSON `pointer` into
/// the payload it is extracted from.
///
/// **Why declared-once extraction exists.** Stamping `agdx.idx.customer_id=alice`
/// on every Iggy message duplicates the field name on the wire and couples the
/// producer to projection details. With a schema declared once on the
/// projector, producers just push the payload (single or batched) and the
/// worker derives the index from the body. The `agdx.idx.*` header path remains
/// as the escape hatch for raw payloads and for schema-first bodies whose
/// writer schema is not registered. Explicit headers always win over
/// schema-extracted values for the same field name.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexField {
    pub name: String,
    pub pointer: String,
    /// Optional storage-type hint. The embedded engine ignores it and keeps
    /// native JSON types. A wide-column backend uses it to create a real typed
    /// column instead of a text fallback. Absent on the wire when unset, so
    /// pre-hint registries decode unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field_type: Option<FieldType>,
}

impl IndexField {
    /// An indexed field `name` extracted from the payload at JSON `pointer`.
    pub fn new(name: impl Into<String>, pointer: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            pointer: pointer.into(),
            field_type: None,
        }
    }

    /// An indexed field with an explicit storage-type hint for columnar backends.
    pub fn typed(
        name: impl Into<String>,
        pointer: impl Into<String>,
        field_type: FieldType,
    ) -> Self {
        Self {
            name: name.into(),
            pointer: pointer.into(),
            field_type: Some(field_type),
        }
    }
}

/// Storage-type hint for an indexed field. A hint, not a constraint: the
/// projector stores whatever scalar the payload carries either way. Columnar
/// backends use the hint for real column DDL.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
#[non_exhaustive]
pub enum FieldType {
    Text,
    Int,
    Float,
    Bool,
}

/// The indexed fields (and optional vector field) a projection extracts.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct IndexSchema {
    pub fields: Vec<IndexField>,
    // Optional JSON pointer to a vector field (`[f32]`) inside the payload.
    // Default is `/embedding` at the root, which matches what producers
    // writing JSON bodies produce today.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector_field: Option<String>,
    // Whether the projector inlines the payload bytes alongside the row by
    // default - the producer can still override per record, but a sensible
    // default means batch-publishers do not need to stamp the directive on
    // every message.
    #[serde(default)]
    pub inline_payload: bool,
}

impl IndexSchema {
    /// Start building an index schema.
    pub fn builder() -> IndexSchemaBuilder {
        IndexSchemaBuilder::default()
    }
}

/// Fluent builder for an `IndexSchema`.
#[derive(Default)]
pub struct IndexSchemaBuilder {
    schema: IndexSchema,
}

impl IndexSchemaBuilder {
    /// Index the JSON value at the root-level field `name`. The index key and
    /// the JSON path are the same in the common case, so `field("customer")`
    /// expands to "extract `/customer` from the payload and store it under
    /// the index key `customer`". For nested or renamed extraction use
    /// [`field_at`](Self::field_at).
    pub fn field(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        let pointer = format!("/{name}");
        self.schema.fields.push(IndexField::new(name, pointer));
        self
    }

    /// Index the JSON value at `pointer` (RFC-6901) under the index key
    /// `name`. Use when the index column name differs from the payload field,
    /// or when the value lives in a nested structure -
    /// `field_at("amount_cents", "/amount/value")` indexes `amount.value` as
    /// `amount_cents`.
    pub fn field_at(mut self, name: impl Into<String>, pointer: impl Into<String>) -> Self {
        self.schema.fields.push(IndexField::new(name, pointer));
        self
    }

    /// Point the vector extractor at `pointer` instead of the default
    /// (`/embedding` at the payload root). Pointer is RFC-6901.
    pub fn vector_field(mut self, pointer: impl Into<String>) -> Self {
        self.schema.vector_field = Some(pointer.into());
        self
    }

    /// Default `inline_payload = true` for every record projected through this
    /// schema. Per-record overrides stay valid.
    pub fn inline_payload(mut self) -> Self {
        self.schema.inline_payload = true;
        self
    }

    /// Finish the index schema.
    pub fn build(self) -> IndexSchema {
        self.schema
    }
}

/// Opaque projection identifier. Stable string the producer stamps on the
/// wire via the `agdx.ref` header and the worker keys its catalog by.
/// Recommended shape: `"<name>.v<version>"`, e.g. `"order.v1"`. Distinct from
/// `schema_id`, which selects a codec's writer schema rather than a
/// materialization rule.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProjectionId(String);

impl ProjectionId {
    /// A projection id from a string.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ProjectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ProjectionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::borrow::Borrow<str> for ProjectionId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl FromStr for ProjectionId {
    type Err = InvalidError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(InvalidError::new("projection id must not be empty"));
        }
        Ok(Self(s.to_owned()))
    }
}

impl From<&str> for ProjectionId {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for ProjectionId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Global reusable projection definition. Names the extraction rules that turn
/// a payload into a queryable row. Not attached to a topic on its own:
/// bindings ([`ProjectionBinding`]) declare where projections may apply.
///
/// # Storage model
///
/// A published record lives in up to three places, controlled by the projection:
///
/// 1. **Iggy log** (always): the original wire bytes, partitioned, replayable
///    from offset 0. The source of truth.
/// 2. **Indexed columns** (always): the scalar fields declared via
///    `field` / `field_at`, extracted from the payload at materialize time
///    and stored in the row. These drive filters, ordering, and aggregates.
/// 3. **Inline body** (opt-in via `inline_payload`, on by default through
///    `Projection::builder`): a copy of the full original payload alongside
///    the row, so typed fetches can decode it without going back to the log.
///
/// The body may carry fields that are not indexed. Only the declared fields
/// are queryable. Everything else rides through as part of the inlined body
/// (when on) or is reachable only by Iggy replay (when off).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Projection {
    /// Stable id used on the wire as `agdx.ref`.
    pub id: ProjectionId,
    /// Human-readable name (e.g. `"order"`). Distinct from `id` so the same
    /// logical projection can rename without breaking the wire ref.
    pub name: String,
    /// Schema-evolution version. Bump on incompatible changes. The wire id
    /// usually encodes this (`order.v2`).
    pub version: u32,
    /// Expected payload codec. `Any` means best-effort decode: the extraction
    /// plan tolerates an opaque payload.
    pub content_type: ContentType,
    /// Field extraction plan.
    pub extraction: IndexSchema,
    /// Default `inline_payload` for records routed through this projection,
    /// overridable per record.
    #[serde(default)]
    pub inline_payload_default: bool,
}

impl Projection {
    /// Start building a projection with this id.
    pub fn builder(id: impl Into<ProjectionId>) -> ProjectionBuilder {
        ProjectionBuilder {
            projection: Self {
                id: id.into(),
                name: String::new(),
                version: 1,
                content_type: ContentType::Any,
                // Default: inline the body alongside the indexed row so typed
                // fetches can decode it without going back to the Iggy log.
                // Opt out with `.index_only()` for high-volume projections
                // where duplication is not worth it.
                extraction: IndexSchema {
                    fields: Vec::new(),
                    vector_field: None,
                    inline_payload: true,
                },
                inline_payload_default: true,
            },
        }
    }
}

/// Fluent builder for a `Projection`.
pub struct ProjectionBuilder {
    projection: Projection,
}

impl ProjectionBuilder {
    /// Set the human-readable projection name.
    pub fn name(mut self, value: impl Into<String>) -> Self {
        self.projection.name = value.into();
        self
    }

    /// Set the projection version.
    pub fn version(mut self, value: u32) -> Self {
        self.projection.version = value;
        self
    }

    /// Set the wire codec the projector decodes records with.
    pub fn content_type(mut self, value: ContentType) -> Self {
        self.projection.content_type = value;
        self
    }

    /// Set the full index schema (instead of `field`/`vector_field`).
    pub fn extraction(mut self, value: IndexSchema) -> Self {
        self.projection.extraction = value;
        self
    }

    /// Index a field by name (extracted at its matching JSON pointer).
    pub fn field(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        let pointer = format!("/{name}");
        self.projection
            .extraction
            .fields
            .push(IndexField::new(name, pointer));
        self
    }

    /// Bulk version of [`field`](Self::field): declare many top-level indexed
    /// fields in one call. Each name `n` lands at JSON pointer `/n`. For nested
    /// pointers use [`field_at`](Self::field_at) instead.
    pub fn fields<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        for name in names {
            let name = name.into();
            let pointer = format!("/{name}");
            self.projection
                .extraction
                .fields
                .push(IndexField::new(name, pointer));
        }
        self
    }

    /// Index a field `name` from an explicit JSON `pointer`.
    pub fn field_at(mut self, name: impl Into<String>, pointer: impl Into<String>) -> Self {
        self.projection
            .extraction
            .fields
            .push(IndexField::new(name, pointer));
        self
    }

    /// Like [`field`](Self::field) but with a storage-type hint for columnar
    /// backends (`FieldType::Int`, `Float`, `Bool`, `Text`). The embedded
    /// engine ignores the hint.
    pub fn field_typed(mut self, name: impl Into<String>, field_type: FieldType) -> Self {
        let name = name.into();
        let pointer = format!("/{name}");
        self.projection
            .extraction
            .fields
            .push(IndexField::typed(name, pointer, field_type));
        self
    }

    /// Like [`field_at`](Self::field_at) but with a storage-type hint.
    pub fn field_at_typed(
        mut self,
        name: impl Into<String>,
        pointer: impl Into<String>,
        field_type: FieldType,
    ) -> Self {
        self.projection
            .extraction
            .fields
            .push(IndexField::typed(name, pointer, field_type));
        self
    }

    /// Extract the embedding vector from this JSON pointer.
    pub fn vector_field(mut self, pointer: impl Into<String>) -> Self {
        self.projection.extraction.vector_field = Some(pointer.into());
        self
    }

    /// Inline the original payload alongside the materialized row. This is
    /// the default for projections built through `Projection::builder`, so
    /// calling it is redundant. It stays in the API for callers who want to be
    /// explicit. To opt out, use [`index_only`](Self::index_only).
    pub fn inline_payload(mut self) -> Self {
        self.projection.inline_payload_default = true;
        self.projection.extraction.inline_payload = true;
        self
    }

    /// Opt out of inlining the body. The materialized row keeps only the
    /// indexed scalars declared via `field` / `field_at` (and any vector
    /// extracted via `vector_field`). The full payload is not duplicated into
    /// the index. Use this when:
    ///
    /// - the body is large and you already store it elsewhere (object store,
    ///   OLAP table) and only need a fast queryable secondary index, or
    /// - you are pushing extreme throughput and the index DB cost of a body
    ///   copy is not worth it.
    ///
    /// The Iggy log retains the original bytes either way, so a future
    /// projector rebuild can re-inline. With `index_only`, typed fetches
    /// return rows whose `payload` is `None`. Callers either decode from the
    /// indexed columns or replay from the log.
    pub fn index_only(mut self) -> Self {
        self.projection.inline_payload_default = false;
        self.projection.extraction.inline_payload = false;
        self
    }

    /// Finish the projection.
    pub fn build(self) -> Projection {
        self.projection
    }
}

/// How long a binding's materialized rows live, **decoupled from the source
/// topic's Iggy `message_expiry`**. Lets a projection outlive (or undershoot)
/// the log it was built from.
///
/// The canonical case: a topic with aggressive expiry (short-lived partitions
/// for cheap storage and fast replay) whose derived index must survive forever.
/// Set the binding to [`RetentionPolicy::Keep`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum RetentionPolicy {
    /// Follow the log: rows are pruned once Iggy drops the messages that
    /// produced them. The default, and the only policy that also deletes the
    /// projection when the source topic is deleted.
    #[default]
    MirrorLog,
    /// Keep rows forever, regardless of the source log *or its deletion*.
    Keep,
    /// Keep rows forever while the source topic exists, but drop the whole
    /// projection when the source topic is **deleted**. Ignores message expiry
    /// like `Keep`, but is tied to the topic's existence like `MirrorLog`.
    KeepUntilSourceDeleted,
    /// Keep rows for `ttl_micros` after they were materialized, independent of
    /// the log. Older rows are swept, and survivors outlive an expired source.
    TimeToLive { ttl_micros: u64 },
    /// Keep the newest `rows` rows for the target table, independent of the log.
    MaxRows { rows: u64 },
    /// A policy `kind` a newer server named that this build does not know. The
    /// decode degrades to this rather than failing the whole reply, so an older
    /// client keeps reading bindings it cannot fully interpret (it must not
    /// re-apply one: re-serializing loses the original `kind` and its fields).
    /// The same forward-compat shape the `ResultCode` and u8 dictionaries use.
    #[serde(other)]
    Unknown,
}

/// Declares where a `Projection` is allowed to materialize. Bindings, not
/// projections, tell the worker what to consume. A binding pairs a source
/// selector (stream and topic) with a set of allowed projection refs, an
/// optional default, and the materialization target (which DB table to
/// write rows into).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectionBinding {
    pub source: SourceSelector,
    /// Projections allowed to fire on messages from this source. A record
    /// stamped with an `agdx.ref` outside this set is either DLQ'd or skipped,
    /// per worker policy.
    #[serde(default)]
    pub allowed_projections: Vec<ProjectionId>,
    /// Default projection applied to records that do not carry an `agdx.ref`.
    /// `None` means such records are skipped.
    #[serde(default)]
    pub default_projection: Option<ProjectionId>,
    /// Where rows land: one or more targets. Exactly one is `read_write`, the
    /// query-serving home. The rest are write-only mirrors. A single
    /// `read_write` target is the common case (see [`target_table`]).
    ///
    /// [`target_table`]: ProjectionBindingBuilder::target_table
    pub targets: Vec<Target>,
    /// Row lifetime for this binding, independent of the source topic's Iggy
    /// retention. `None` inherits LaserData Cloud's fleet-wide default
    /// (`MirrorLog` unless an operator changed it). Set it via
    /// [`retention`](ProjectionBindingBuilder::retention).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<RetentionPolicy>,
}

impl ProjectionBinding {
    /// Start building a binding (which topic feeds which projection).
    pub fn builder() -> ProjectionBindingBuilder {
        ProjectionBindingBuilder::default()
    }
}

/// Fluent builder for a `ProjectionBinding`.
#[derive(Default)]
pub struct ProjectionBindingBuilder {
    source: Option<SourceSelector>,
    allowed: Vec<ProjectionId>,
    default_projection: Option<ProjectionId>,
    targets: Vec<Target>,
    retention: Option<RetentionPolicy>,
}

impl ProjectionBindingBuilder {
    /// Set the binding source. A source is always a `(stream, topic)` pair,
    /// and a topic only exists within a stream, so both are required.
    pub fn source(mut self, stream: impl Into<String>, topic: impl Into<String>) -> Self {
        self.source = Some(SourceSelector::new(stream, topic));
        self
    }

    /// Set the binding source from a pre-built [`SourceSelector`].
    pub fn selector(mut self, source: SourceSelector) -> Self {
        self.source = Some(source);
        self
    }

    /// Allow records to route to this projection.
    pub fn allow(mut self, projection: impl Into<ProjectionId>) -> Self {
        self.allowed.push(projection.into());
        self
    }

    /// Projection used when a record carries no `agdx.ref`.
    pub fn default_projection(mut self, projection: impl Into<ProjectionId>) -> Self {
        self.default_projection = Some(projection.into());
        self
    }

    /// Add a materialization target. The first `read_write` target serves
    /// queries. Further targets are write-only mirrors.
    pub fn add_target(mut self, target: Target) -> Self {
        self.targets.push(target);
        self
    }

    /// Set how long the materialized rows live, independent of the source
    /// topic's Iggy retention. Omit to inherit the managed backend's default.
    ///
    /// ```no_run
    /// # use laser_wire::control::{ProjectionBinding, RetentionPolicy};
    /// // Short-lived topic, permanent index:
    /// let binding = ProjectionBinding::builder()
    ///     .source("_agdx", "telemetry")
    ///     .allow("telemetry.v1")
    ///     .target_table("telemetry_rows")
    ///     .retention(RetentionPolicy::Keep)
    ///     .build();
    /// ```
    pub fn retention(mut self, retention: RetentionPolicy) -> Self {
        self.retention = Some(retention);
        self
    }

    /// Materialize into a table of this name on the embedded backend: sugar for
    /// a single `read_write`, `effectively_once` target. The common case.
    pub fn target_table(self, table: impl Into<String>) -> Self {
        self.target_on("embedded", table)
    }

    /// Materialize into `table` on a named backend: the `read_write`,
    /// `effectively_once` query-serving home. This is how a binding routes one
    /// index to a specific configured backend (e.g. an external warehouse
    /// declared as `warehouse`) while other bindings stay on `embedded`, so
    /// different topics materialize to and are served from different stores at
    /// once.
    ///
    /// ```no_run
    /// # use laser_wire::control::ProjectionBinding;
    /// // orders -> external warehouse, events stay on the embedded engine.
    /// let binding = ProjectionBinding::builder()
    ///     .source("shop", "orders")
    ///     .allow("orders.v1")
    ///     .target_on("warehouse", "orders_rows")
    ///     .build();
    /// ```
    pub fn target_on(self, backend: impl Into<String>, table: impl Into<String>) -> Self {
        self.add_target(Target {
            backend: backend.into(),
            table: table.into(),
            role: TargetRole::ReadWrite,
            delivery: Delivery::EffectivelyOnce,
            required: true,
        })
    }

    /// Add a write-only mirror of this binding's rows into `table` on a named
    /// backend. The mirror does not serve queries (exactly one `read_write`
    /// target does) and is non-blocking, so one projection can fan the same rows
    /// to several backends at once (e.g. the embedded engine for low-latency
    /// reads plus an external warehouse for analytics).
    pub fn mirror_to(self, backend: impl Into<String>, table: impl Into<String>) -> Self {
        self.add_target(Target {
            backend: backend.into(),
            table: table.into(),
            role: TargetRole::WriteOnly,
            delivery: Delivery::EffectivelyOnce,
            required: false,
        })
    }

    /// Build the binding. Panics if `.source(..)` / `.selector(..)` was not set
    /// (programmer error, not user input). Prefer [`try_build`](Self::try_build)
    /// for code paths that handle config-load failures gracefully.
    pub fn build(self) -> ProjectionBinding {
        self.try_build()
            .expect("ProjectionBinding requires a source - call .source(stream, topic)")
    }

    /// Build the binding, returning an error if required fields are missing
    /// instead of panicking.
    pub fn try_build(self) -> Result<ProjectionBinding, InvalidError> {
        let source = self
            .source
            .ok_or_else(|| InvalidError::new("ProjectionBinding requires a source"))?;
        let targets = if self.targets.is_empty() {
            // Default: one read_write target on the embedded backend, named
            // after the topic. Keeps the no-target builder path usable.
            vec![Target {
                backend: "embedded".to_owned(),
                table: source.topic.clone(),
                role: TargetRole::ReadWrite,
                delivery: Delivery::EffectivelyOnce,
                required: true,
            }]
        } else {
            self.targets
        };
        Ok(ProjectionBinding {
            source,
            allowed_projections: self.allowed,
            default_projection: self.default_projection,
            targets,
            retention: self.retention,
        })
    }
}

/// Selects a single source `(stream, topic)` for v1. Both are required, since
/// a topic only exists within a stream. Future versions can extend this to
/// prefix, glob, or multi-stream selectors. Today the model is
/// one-topic-per-binding to keep routing simple.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceSelector {
    pub stream: String,
    pub topic: String,
}

impl SourceSelector {
    /// A `(stream, topic)` source for a binding.
    pub fn new(stream: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            topic: topic.into(),
        }
    }
}

/// One materialization sink for a binding: a named backend and table, the role
/// it plays, and its delivery guarantee. `backend` is a logical id LaserData
/// Cloud resolves against its configured backend set (the embedded engine is
/// `embedded`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Target {
    pub backend: String,
    pub table: String,
    #[serde(default)]
    pub role: TargetRole,
    #[serde(default)]
    pub delivery: Delivery,
    #[serde(default)]
    pub required: bool,
}

/// A target's role. Exactly one `read_write` target per binding serves
/// queries. The rest are write-only mirrors.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TargetRole {
    #[default]
    ReadWrite,
    WriteOnly,
}

/// A target's delivery guarantee. The `read_write` target is never
/// `at_most_once`.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
    strum::VariantArray,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum Delivery {
    #[default]
    EffectivelyOnce,
    AtMostOnce,
}

/// A registered writer schema, keyed by the `id` a producer stamps on
/// `agdx.sid`. Avro and Protobuf schemas decode their schema-first bodies. A
/// JSON Schema validates the decoded payload of a self-describing codec
/// (JSON, MessagePack, CBOR, BSON), which otherwise needs no entry here. Ids
/// are permanent: registering an occupied id with a different definition is
/// rejected managed-side, and dropping tombstones the definition.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SchemaDef {
    pub id: u32,
    pub source: SchemaSource,
    /// Optional human label, pure metadata. LaserData Cloud stores and returns
    /// it but never dispatches on it (`agdx.sid` carries the id). Uniqueness is
    /// not enforced. Absent on the wire when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Optional caller-tracked schema version, pure metadata. LaserData Cloud
    /// stores and returns it but never dispatches on it (the `id` alone selects
    /// the decoder). Absent on the wire when unset, so pre-version registries
    /// decode unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
}

impl SchemaDef {
    /// The codec this schema applies to. A JSON Schema validates any
    /// self-describing codec and reports as `Json`.
    pub fn content_type(&self) -> ContentType {
        match self.source {
            SchemaSource::Avro { .. } => ContentType::Avro,
            SchemaSource::Protobuf { .. } => ContentType::Protobuf,
            SchemaSource::JsonSchema { .. } => ContentType::Json,
            // An unknown source kind from a newer server: best-effort.
            SchemaSource::Unknown => ContentType::Any,
        }
    }
}

/// The schema payload for a schema-first codec. Internally tagged on `kind`
/// (`{"kind":"avro","schema":...}`) so an older client tolerates a newer
/// server's unknown source kind: an unrecognized `kind` decodes to
/// [`SchemaSource::Unknown`] instead of failing the whole reply.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum SchemaSource {
    /// An Avro writer schema as its canonical JSON text.
    Avro { schema: String },
    /// A Protobuf `FileDescriptorSet` (one or more compiled `.proto` files)
    /// plus the fully-qualified message type to decode, e.g. `"shop.Order"`.
    Protobuf {
        #[serde(with = "crate::encoding::bin_bytes")]
        descriptor_set: Vec<u8>,
        message_type: String,
    },
    /// A JSON Schema (draft 2020-12) as its JSON text. Validation-only: a
    /// self-describing record stamping this schema's id on `agdx.sid` has its
    /// decoded payload validated by LaserData Cloud. Mismatches are counted,
    /// optionally dead-lettered, and never materialize body fields.
    JsonSchema { schema: String },
    /// A schema `kind` a newer server named that this build does not know. The
    /// decode degrades to this rather than failing, so an older client can still
    /// read a schema registry that holds a source kind it cannot decode against
    /// (it must not re-register one: the original `kind` and its fields are
    /// lost). Same forward-compat shape as [`RetentionPolicy::Unknown`].
    #[serde(other)]
    Unknown,
}

/// One control command on the control topic. The customer SDK (or any tool
/// driving LaserData Cloud) publishes these to register projections,
/// bindings, and schemas.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControlCommand {
    RegisterProjection(Projection),
    DropProjection(String),
    ApplyBinding(ProjectionBinding),
    RemoveBinding {
        source: SourceSelector,
        projection_ref: Option<String>,
    },
    /// Register (or replace by `id`) a writer schema for a schema-first codec.
    RegisterSchema(SchemaDef),
    /// Drop the schema registered under this id.
    DropSchema(u32),
}

/// Versioned wrapper around a [`ControlCommand`], CBOR-named on the wire.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ControlEnvelope {
    pub v: u32,
    pub timestamp_micros: u64,
    pub command: ControlCommand,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_an_unknown_schema_source_kind_when_decoded_then_should_degrade_not_fail() {
        // A newer server names a `kind` this build does not know. The decode
        // must degrade to `Unknown` (the forward-compat catch-all) rather than
        // failing the whole reply, and any extra fields are ignored.
        let source: SchemaSource =
            serde_json::from_str(r#"{"kind":"parquet_descriptor","blob":[1,2,3]}"#)
                .expect("an unknown schema kind decodes to Unknown, not an error");
        assert_eq!(source, SchemaSource::Unknown);
        // A `SchemaDef` wrapping it reports the best-effort content type.
        let def = SchemaDef {
            id: 1,
            source,
            name: None,
            version: None,
        };
        assert_eq!(def.content_type(), ContentType::Any);
    }

    #[test]
    fn given_an_unknown_retention_kind_when_decoded_then_should_degrade_not_fail() {
        let policy: RetentionPolicy = serde_json::from_str(r#"{"kind":"keep_for_eras","eras":3}"#)
            .expect("an unknown retention kind decodes to Unknown, not an error");
        assert_eq!(policy, RetentionPolicy::Unknown);
    }

    #[test]
    fn given_target_table_sugar_when_built_then_should_be_single_read_write_target() {
        let binding = ProjectionBinding::builder()
            .source("shop", "orders")
            .allow("order.v1")
            .target_table("orders_rows")
            .build();
        assert_eq!(binding.targets.len(), 1);
        assert_eq!(binding.targets[0].backend, "embedded");
        assert_eq!(binding.targets[0].table, "orders_rows");
        assert_eq!(binding.targets[0].role, TargetRole::ReadWrite);
        assert_eq!(binding.targets[0].delivery, Delivery::EffectivelyOnce);
        assert!(binding.targets[0].required);
    }

    #[test]
    fn given_target_on_named_backend_when_built_then_should_route_read_write_to_it() {
        let binding = ProjectionBinding::builder()
            .source("shop", "orders")
            .allow("order.v1")
            .target_on("warehouse", "orders_rows")
            .build();
        assert_eq!(binding.targets.len(), 1);
        assert_eq!(binding.targets[0].backend, "warehouse");
        assert_eq!(binding.targets[0].table, "orders_rows");
        assert_eq!(binding.targets[0].role, TargetRole::ReadWrite);
        assert!(binding.targets[0].required);
    }

    #[test]
    fn given_target_on_and_mirror_to_when_built_then_should_fan_one_projection_to_two_backends() {
        // Read-serve from the embedded engine, mirror the same rows into an
        // external warehouse: one projection, two backends at once.
        let binding = ProjectionBinding::builder()
            .source("shop", "orders")
            .allow("order.v1")
            .target_on("embedded", "orders_rows")
            .mirror_to("warehouse", "orders_warehouse")
            .build();
        assert_eq!(binding.targets.len(), 2);
        assert_eq!(binding.targets[0].role, TargetRole::ReadWrite);
        assert_eq!(binding.targets[0].backend, "embedded");
        assert_eq!(binding.targets[1].role, TargetRole::WriteOnly);
        assert_eq!(binding.targets[1].backend, "warehouse");
        assert_eq!(binding.targets[1].table, "orders_warehouse");
        assert!(!binding.targets[1].required, "a mirror is non-blocking");
    }

    #[test]
    fn given_a_read_write_target_and_a_mirror_when_added_then_should_keep_both_in_order() {
        let binding = ProjectionBinding::builder()
            .source("shop", "orders")
            .allow("order.v1")
            .target_table("orders_rows")
            .add_target(Target {
                backend: "warehouse".to_owned(),
                table: "orders_mirror".to_owned(),
                role: TargetRole::WriteOnly,
                delivery: Delivery::AtMostOnce,
                required: false,
            })
            .build();
        assert_eq!(binding.targets.len(), 2);
        assert_eq!(binding.targets[0].role, TargetRole::ReadWrite);
        assert_eq!(binding.targets[1].role, TargetRole::WriteOnly);
        assert_eq!(binding.targets[1].backend, "warehouse");
    }

    #[test]
    fn given_no_retention_when_built_then_should_default_to_none() {
        let binding = ProjectionBinding::builder()
            .source("shop", "orders")
            .target_table("orders_rows")
            .build();
        assert_eq!(binding.retention, None);
    }

    #[test]
    fn given_no_source_when_try_built_then_should_error() {
        assert!(ProjectionBinding::builder().try_build().is_err());
    }

    #[test]
    fn given_a_projection_built_with_the_default_when_inspected_then_should_inline_payload() {
        let projection = Projection::builder("api.call.v1")
            .name("api.call")
            .version(1)
            .fields(["endpoint", "status"])
            .build();
        assert!(
            projection.inline_payload_default,
            "Projection::builder default should inline payload"
        );
        assert!(
            projection.extraction.inline_payload,
            "Projection::builder default should mark extraction.inline_payload too"
        );
    }

    #[test]
    fn given_a_projection_with_index_only_when_inspected_then_should_skip_inlining() {
        let projection = Projection::builder("api.call.v1")
            .name("api.call")
            .version(1)
            .fields(["endpoint"])
            .index_only()
            .build();
        assert!(
            !projection.inline_payload_default,
            "index_only must clear inline_payload_default"
        );
        assert!(
            !projection.extraction.inline_payload,
            "index_only must clear extraction.inline_payload"
        );
    }

    #[test]
    fn given_an_empty_projection_id_when_parsed_then_should_error() {
        assert!("".parse::<ProjectionId>().is_err());
        assert_eq!(
            "order.v1"
                .parse::<ProjectionId>()
                .expect("non-empty id parses")
                .as_str(),
            "order.v1"
        );
    }
}

#[cfg(all(test, feature = "cbor"))]
mod wire_tests {
    use super::*;
    use crate::codes::CONTROL_OP_VERSION;
    use crate::content::ContentType;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_an_apply_binding_when_round_tripped_then_should_preserve_targets_and_version() {
        let binding = ProjectionBinding::builder()
            .source("shop", "orders")
            .allow("order.v1")
            .default_projection("order.v1")
            .target_table("orders_rows")
            .add_target(Target {
                backend: "warehouse".to_owned(),
                table: "orders_mirror".to_owned(),
                role: TargetRole::WriteOnly,
                delivery: Delivery::AtMostOnce,
                required: false,
            })
            .build();
        let envelope = ControlEnvelope {
            v: CONTROL_OP_VERSION,
            timestamp_micros: 42,
            command: ControlCommand::ApplyBinding(binding),
        };
        let bytes = encode_named(&envelope).expect("envelope serializes");
        let back: ControlEnvelope = decode_named(&bytes).expect("envelope deserializes");
        assert_eq!(back.v, CONTROL_OP_VERSION);
        let ControlCommand::ApplyBinding(decoded) = back.command else {
            panic!("expected ApplyBinding");
        };
        assert_eq!(decoded.targets.len(), 2);
        assert_eq!(decoded.targets[0].table, "orders_rows");
        assert_eq!(decoded.targets[0].role, TargetRole::ReadWrite);
        assert_eq!(decoded.targets[1].table, "orders_mirror");
        assert_eq!(decoded.targets[1].delivery, Delivery::AtMostOnce);
        // Retention left unset stays unset across the wire (inherits LaserData Cloud
        // default), and is omitted from the encoding entirely.
        assert_eq!(decoded.retention, None);
    }

    #[test]
    fn given_a_register_schema_when_round_tripped_then_should_preserve_source() {
        let envelope = ControlEnvelope {
            v: CONTROL_OP_VERSION,
            timestamp_micros: 7,
            command: ControlCommand::RegisterSchema(SchemaDef {
                id: 11,
                source: SchemaSource::Avro {
                    schema: r#"{"type":"record","name":"Order","fields":[]}"#.to_owned(),
                },
                name: None,
                version: None,
            }),
        };
        let bytes = encode_named(&envelope).expect("envelope serializes");
        let back: ControlEnvelope = decode_named(&bytes).expect("envelope deserializes");
        let ControlCommand::RegisterSchema(decoded) = back.command else {
            panic!("expected RegisterSchema");
        };
        assert_eq!(decoded.id, 11);
        assert_eq!(decoded.content_type(), ContentType::Avro);
    }

    #[test]
    fn given_a_protobuf_schema_source_when_round_tripped_then_should_preserve_bytes() {
        let source = SchemaSource::Protobuf {
            descriptor_set: vec![10, 20, 30],
            message_type: "shop.Order".to_owned(),
        };
        let bytes = encode_named(&source).expect("serializes");
        let back: SchemaSource = decode_named(&bytes).expect("deserializes");
        assert_eq!(back, source);
    }

    #[test]
    fn given_retention_set_when_round_tripped_then_should_preserve_policy() {
        for policy in [
            // Explicit `MirrorLog` is the default *value* but not the default
            // *state* (`None`). Set explicitly it must survive as `Some(MirrorLog)`,
            // distinct from the unset binding that omits the field entirely.
            RetentionPolicy::MirrorLog,
            RetentionPolicy::Keep,
            RetentionPolicy::KeepUntilSourceDeleted,
            RetentionPolicy::TimeToLive {
                ttl_micros: 3_600_000_000,
            },
            RetentionPolicy::MaxRows { rows: 10_000 },
        ] {
            let binding = ProjectionBinding::builder()
                .source("shop", "telemetry")
                .allow("telemetry.v1")
                .target_table("telemetry_rows")
                .retention(policy)
                .build();
            assert_eq!(binding.retention, Some(policy));
            let bytes = encode_named(&binding).expect("binding serializes");
            let back: ProjectionBinding = decode_named(&bytes).expect("binding deserializes");
            assert_eq!(back.retention, Some(policy));
        }
    }
}

#[cfg(all(test, feature = "codecs"))]
mod schema_tests {
    use super::*;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_a_schema_def_with_version_when_round_tripped_then_should_preserve_it() {
        let def = SchemaDef {
            id: 7,
            source: SchemaSource::Avro {
                schema: "{}".to_owned(),
            },
            name: Some("orders".to_owned()),
            version: Some(2),
        };
        let bytes = encode_named(&def).expect("serializes");
        let back: SchemaDef = decode_named(&bytes).expect("deserializes");
        assert_eq!(back.version, Some(2));
        // Unset version is omitted from the wire entirely (back-compat with
        // pre-version registries).
        let unversioned = SchemaDef {
            name: None,
            version: None,
            ..def
        };
        let json = serde_json::to_string(&unversioned).expect("serializes");
        assert!(
            !json.contains("version"),
            "unset version must be omitted: {json}"
        );
    }
}

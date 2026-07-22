use crate::error::LaserError;
use crate::laser::Laser;
use laser_wire::browse::{
    BrowseOutcome, BrowseReply, GetProjection, GetSchema, ListProjections, ListSchemas,
    ProjectionInfo, RegisterSchema, SchemaInfo,
};
use laser_wire::codes::{
    AGDX_GET_PROJECTION_CODE, AGDX_GET_SCHEMA_CODE, AGDX_LIST_PROJECTIONS_CODE,
    AGDX_LIST_SCHEMAS_CODE, AGDX_REGISTER_SCHEMA_CODE, QUERY_OP_VERSION,
};
use laser_wire::control::{
    ControlCommand, Projection, ProjectionBinding, ProjectionKind, SchemaSource, SourceSelector,
};
use laser_wire::framing::encode_named;
use laser_wire::query::QueryError;
use serde::Serialize;

impl Laser {
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
        if !capabilities.managed {
            return Err(LaserError::unsupported(
                "projections",
                "projection browse is not served by this deployment",
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
        let payload = encode_named(request)
            .map_err(|error| LaserError::Codec(format!("encode browse request: {error}")))?;
        let payload = self.send_raw_with_response(code, payload).await?;
        match crate::error::decode_managed_reply::<BrowseReply>(&payload)? {
            BrowseReply::Ok(outcome) => Ok(outcome),
            BrowseReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "browse: unknown reply variant".to_owned(),
            )),
        }
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
    ///
    /// Rejects a graph projection (one built with
    /// [`ProjectionBuilder::graph`](crate::query::ProjectionBuilder::graph)) with
    /// [`LaserError::Invalid`]: a graph materializes nodes and edges, not a
    /// queryable row table, so it registers through
    /// [`register_graph`](Self::register_graph) instead.
    pub async fn register(&self, projection: Projection) -> Result<(), LaserError> {
        if projection.kind == ProjectionKind::Graph {
            return Err(LaserError::Invalid(format!(
                "projection `{}` is a graph projection. Register it with `register_graph`",
                projection.id
            )));
        }
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

    /// Register a graph projection: a [`Projection`] with `kind = Graph` and an
    /// entity schema (build it with [`Projection::builder`]`.graph(schema)`). It
    /// records the named knowledge graph so it is discoverable, and declares the
    /// node and edge extraction plan. A distinct command from
    /// [`register`](Self::register) so a backend can gate graph registration on
    /// the `graph` capability.
    ///
    /// Graph data is written content-addressed through the `graph` feature's
    /// `GraphHandle::upsert`, or by the managed projector when this projection
    /// is bound to a source topic via [`Bindings::apply`] (it applies the
    /// entity schema to each record and upserts the extracted nodes and edges).
    /// Registering a graph projection creates no row table. Rejects a non-graph
    /// projection with [`LaserError::Invalid`].
    pub async fn register_graph(&self, projection: Projection) -> Result<(), LaserError> {
        if projection.kind != ProjectionKind::Graph || projection.entity_schema.is_none() {
            return Err(LaserError::Invalid(format!(
                "projection `{}` is not a graph projection. Build it with `Projection::builder(..).graph(schema)` or register it with `register`",
                projection.id
            )));
        }
        self.laser
            .publish_control(ControlCommand::RegisterGraph(projection))
            .await
    }

    /// Drop the graph projection registered under `id` by publishing a
    /// `DropGraph` control command. The materialized nodes and edges are left
    /// untouched, the same as [`drop`](Self::drop) for a row projection.
    pub async fn drop_graph(&self, id: impl Into<String>) -> Result<(), LaserError> {
        self.laser
            .publish_control(ControlCommand::DropGraph(id.into()))
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

    /// Remove a binding by publishing a `RemoveBinding` control command.
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
#[must_use = "call .send().await to register the schema"]
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

/// Fluent builder for [`Projections::list`]. Narrow the registry browse with
/// `.for_topic` / `.for_topics` / `.name_contains` / `.id_prefix`, then
/// `.fetch().await`. No filter lists every projection.
#[must_use = "call .fetch().await to list the projections"]
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

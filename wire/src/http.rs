//! The `/agdx/*` HTTP surface: route constants, path builders for the
//! parameterized routes, the typed query-parameter structs, and the JSON view
//! and error types the server's router serves. This module is the first-class,
//! executable definition of the HTTP binding (the prose lives in the AGDX spec,
//! Part B4): both the server's router and every browser/native client share
//! these exact paths, shapes, and the bare-`Ok`-or-[`ErrorBody`] reply contract,
//! so a renamed route or a drifted shape is a compile or doc-test failure rather
//! than a 404 in production.
//!
//! Path builders take PRE-ENCODED segments: a caller embedding a user-supplied
//! namespace, key, or fork id must percent-encode it first. The crate stays
//! dependency-free by not doing it here. The [`http_client`](crate::http_client)
//! feature owns the base64url and query-string composition.
//!
//! The contract, exercised (this doc-test is the CI drift gate):
//!
//! ```
//! use laser_wire::http::{kv_entry_path, ErrorBody, CAPABILITIES_PATH};
//! use laser_wire::result::ResultCode;
//!
//! // Routes are owned as code, never hand-typed at a call site.
//! assert_eq!(CAPABILITIES_PATH, "/agdx/capabilities");
//! assert_eq!(kv_entry_path("sessions", "dXNlcjox"), "/agdx/kv/sessions/dXNlcjox");
//!
//! // A failure carries a machine-dispatchable code plus a human message.
//! // the status line is derived from the code, so a client matches on `code`.
//! let body = ErrorBody::new(ResultCode::NotFound, "no such fork");
//! assert_eq!(body.http_status(), 404);
//! let json = serde_json::to_string(&body).unwrap();
//! assert_eq!(serde_json::from_str::<ErrorBody>(&json).unwrap(), body);
//! ```

use crate::fork::{ForkError, ForkKind};
use crate::hello::{BackendDescriptor, OpVersions};
use crate::kv::KvError;
use crate::query::{Consistency, QueryError};
use crate::result::ResultCode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// `GET /agdx/capabilities`: the feature-detection probe.
pub const CAPABILITIES_PATH: &str = "/agdx/capabilities";
/// `POST /agdx/query` (and `GET` with the query as a parameter).
pub const QUERY_PATH: &str = "/agdx/query";
/// `GET /agdx/projections` to list, `POST` to register.
pub const PROJECTIONS_PATH: &str = "/agdx/projections";
/// `POST /agdx/bindings` to apply, `DELETE` to remove.
pub const BINDINGS_PATH: &str = "/agdx/bindings";
/// `GET /agdx/schemas` to list, `POST` to register.
pub const SCHEMAS_PATH: &str = "/agdx/schemas";
/// `GET /agdx/kv` to list the caller's namespaces.
pub const KV_PATH: &str = "/agdx/kv";
/// `GET /agdx/forks` to list, `POST` to create.
pub const FORKS_PATH: &str = "/agdx/forks";
/// `GET /agdx/graphs` to list graph projections, `POST` to register.
pub const GRAPHS_PATH: &str = "/agdx/graphs";

/// `DELETE`/`GET /agdx/graphs/{id}`: drop or read a graph projection.
pub fn graph_path(id: &str) -> String {
    format!("{GRAPHS_PATH}/{id}")
}

/// `POST /agdx/graph/{name}/query`: run a traversal (a `GraphQuery` body).
pub fn graph_query_path(name: &str) -> String {
    format!("/agdx/graph/{name}/query")
}

/// `GET /agdx/graph/{name}/neighbors/{node}`: one-hop neighbor read.
pub fn graph_neighbors_path(name: &str, node: &str) -> String {
    format!("/agdx/graph/{name}/neighbors/{node}")
}

/// `GET`/`DELETE /agdx/projections/{id}`.
pub fn projection_path(id: &str) -> String {
    format!("{PROJECTIONS_PATH}/{id}")
}

/// `GET`/`DELETE /agdx/schemas/{id}`.
pub fn schema_path(id: u32) -> String {
    format!("{SCHEMAS_PATH}/{id}")
}

/// `POST /agdx/schemas/{id}/decode`.
pub fn schema_decode_path(id: u32) -> String {
    format!("{SCHEMAS_PATH}/{id}/decode")
}

/// `GET /agdx/kv/{namespace}` to scan, `DELETE` to bulk-delete.
pub fn kv_namespace_path(namespace: &str) -> String {
    format!("{KV_PATH}/{namespace}")
}

/// `GET`/`PUT`/`DELETE /agdx/kv/{namespace}/{key}`. `key` is the URL-safe
/// unpadded base64 form of the key bytes, the encoding this surface uses for
/// every binary body.
pub fn kv_entry_path(namespace: &str, key_b64: &str) -> String {
    format!("{KV_PATH}/{namespace}/{key_b64}")
}

/// `PUT /agdx/kv/{namespace}/{key}/cas`: a conditional write (compare-and-swap).
/// The precondition rides the query string (`expect_version` or `expect_absent`)
/// and the value rides the raw body, like the plain `PUT`. A success replies
/// `CasCommittedView` with the new version, a precondition miss replies `409`
/// with an `ErrorBody` of code `conflict` whose `detail` carries the current
/// version.
pub fn kv_cas_path(namespace: &str, key_b64: &str) -> String {
    format!("{KV_PATH}/{namespace}/{key_b64}/cas")
}

/// `DELETE /agdx/forks/{id}`.
pub fn fork_path(id: &str) -> String {
    format!("{FORKS_PATH}/{id}")
}

/// `POST /agdx/forks/{id}/promote`.
pub fn fork_promote_path(id: &str) -> String {
    format!("{FORKS_PATH}/{id}/promote")
}

/// `PUT /agdx/forks/{id}/rows`.
pub fn fork_rows_path(id: &str) -> String {
    format!("{FORKS_PATH}/{id}/rows")
}

/// `GET /agdx/capabilities` reply: what the `/agdx/*` surface offers on this
/// server, so a browser client can feature-detect before showing the
/// projections / query / KV / fork views. Richer than the binary `AGDX_HELLO`
/// probe (per-surface flags plus the wire op versions the JSON bodies must
/// match), and it answers truthfully even when the managed backend is disabled (200
/// with `managed: false`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Capabilities {
    /// Connected to a managed plane at all (the root: with no plane every managed
    /// surface below is off, and the reply still answers `200` with `managed:
    /// false`).
    pub managed: bool,
    /// The managed query surface, its registry browse views, and the strongest
    /// read-consistency it serves.
    pub query: QueryCapsView,
    /// The managed key-value surface and its conditional-write support.
    pub kv: KvCapsView,
    /// Whether the knowledge-graph ops (traversal, neighbors) are served. The
    /// agentic-memory API composes the query and graph surfaces, so it has no
    /// flag of its own: a client reads `query` and `graph`.
    #[serde(default)]
    pub graph: bool,
    /// Whether copy-on-write forks are served.
    pub fork: bool,
    pub versions: OpVersions,
    /// Materialization backends the server currently exposes, so a client can
    /// show what it may route to. Identity only (id + engine kind), no settings
    /// or secrets. Empty (the default) is skipped on encode, so a pre-backends
    /// capabilities reply stays byte-identical.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backends: Vec<BackendDescriptor>,
}

/// The managed query surface on the HTTP capabilities reply: whether it is
/// served, its registry browse views, and the consistency it honors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryCapsView {
    /// Whether `POST /agdx/query` is served.
    pub available: bool,
    /// Whether the projection registry browse routes are served.
    pub projections: bool,
    /// Whether the schema registry browse routes are served.
    pub schemas: bool,
    /// The strongest read-consistency the surface serves (the ladder
    /// `eventual < read_your_writes < strong`, so a level implies the weaker
    /// ones). Defaults to `eventual`, which every query surface serves.
    #[serde(default)]
    pub consistency: Consistency,
}

/// The managed key-value surface on the HTTP capabilities reply.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvCapsView {
    /// Whether the get/set/scan routes are served.
    pub available: bool,
    /// Whether compare-and-swap (`AGDX_KV_CAS`) is served. Independent of plain
    /// get/set: a backend that cannot do a conditional write leaves it off and a
    /// `cas` returns a clean unsupported error.
    #[serde(default)]
    pub cas: bool,
}

impl Capabilities {
    /// Constructor for the non-exhaustive wire struct. The core surfaces track
    /// `enabled`, the way the binary `AGDX_HELLO` probe answers. The per-surface
    /// sub-features (`kv.cas`, `query.consistency` above `eventual`, `graph`) all
    /// start off: a server must opt into each, never over-advertising (which would
    /// turn a clean unsupported error into a silent wrong answer). Build them with
    /// [`from_versions`](Self::from_versions) or the setters.
    pub fn new(enabled: bool, versions: OpVersions) -> Self {
        Self {
            managed: enabled,
            query: QueryCapsView {
                available: enabled,
                projections: enabled,
                schemas: enabled,
                consistency: Consistency::Eventual,
            },
            kv: KvCapsView {
                available: enabled,
                cas: false,
            },
            graph: false,
            fork: enabled,
            versions,
            backends: Vec::new(),
        }
    }

    /// Advertise that the knowledge-graph ops are served. Off by default: a
    /// server sets it only when a backend implements the graph surface.
    #[must_use]
    pub fn with_graph(mut self, value: bool) -> Self {
        self.graph = value;
        self
    }

    /// Advertise the materialization backends the server exposes. The wire pins
    /// no engine, so a server lists whatever it has open by id and kind.
    #[must_use]
    pub fn with_backends(mut self, backends: Vec<BackendDescriptor>) -> Self {
        self.backends = backends;
        self
    }

    /// Advertise compare-and-swap on the KV surface (`AGDX_KV_CAS`). Only a
    /// backend that does a genuine conditional write may set it.
    #[must_use]
    pub fn with_kv_cas(mut self, on: bool) -> Self {
        self.kv.cas = on;
        self
    }

    /// Advertise the strongest read-consistency the query surface serves.
    #[must_use]
    pub fn with_query_consistency(mut self, level: Consistency) -> Self {
        self.query.consistency = level;
        self
    }

    /// Build the HTTP capabilities from the same `OpVersions` the binary
    /// `AGDX_HELLO` probe answers with, reading the per-surface sub-features
    /// straight off its `features` bitset and `graph` op version. A server SHOULD
    /// use this so its two capability carriages (the binary `features` bits and
    /// these HTTP fields) cannot disagree (A12): the one source drives both.
    pub fn from_versions(enabled: bool, versions: OpVersions) -> Self {
        use crate::hello::feature;
        let consistency = if versions.has_feature(feature::STRONG_CONSISTENCY) {
            Consistency::Strong
        } else if versions.has_feature(feature::READ_YOUR_WRITES) {
            Consistency::ReadYourWrites
        } else {
            Consistency::Eventual
        };
        Self::new(enabled, versions)
            .with_kv_cas(versions.has_feature(feature::KV_CAS))
            .with_query_consistency(consistency)
            // The graph surface needs a backend that serves it, advertised as a
            // non-zero graph op version, so it is gated on that rather than implied.
            .with_graph(enabled && versions.graph > 0)
    }
}

/// One KV entry on the HTTP surface. `key` and `value` are URL-safe unpadded
/// base64, because keys and values are arbitrary bytes that JSON strings
/// cannot carry raw.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvEntryView {
    pub key: String,
    pub value: String,
    pub expires_at_micros: Option<u64>,
}

/// One KV scan page on the HTTP surface.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvPageView {
    pub entries: Vec<KvEntryView>,
    pub cursor: Option<String>,
}

/// `DELETE /agdx/kv/{namespace}` reply: the number of entries removed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedManyView {
    pub deleted: usize,
}

/// `POST /agdx/forks/{id}/promote` reply: the number of rows applied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromotedView {
    pub rows: usize,
}

/// One graph node on the HTTP surface: id as a string, its labels, and its
/// attributes rendered as strings (e.g. the entity `value`) for a browser or
/// wasm client that has no access to the typed `Value`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNodeView {
    pub id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attrs: Vec<(String, String)>,
}

/// One graph edge on the HTTP surface: endpoint ids as strings, the type, and the
/// weight.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GraphEdgeView {
    pub id: String,
    pub from: String,
    pub to: String,
    pub edge_type: String,
    pub weight: f32,
    /// Valid-time window (epoch micros) for a bitemporal edge, omitted when open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_to: Option<u64>,
}

/// `POST /agdx/graph/{name}/query` reply: the reachable nodes, traversed edges,
/// and (for a `paths` return) the reconstructed paths as id sequences.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GraphResultView {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<GraphNodeView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edges: Vec<GraphEdgeView>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<PathView>,
}

/// One path in a `GraphResultView`: parallel node and edge id sequences, ids as
/// Crockford-base32 strings (the JSON view of [`crate::graph::Path`]).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PathView {
    pub nodes: Vec<String>,
    pub edges: Vec<String>,
}

/// `POST /agdx/schemas` body: the register request without an id. The managed
/// backend allocates it and the reply carries it back as
/// `{"SchemaRegistered":id}`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisterSchemaBody {
    pub source: crate::control::SchemaSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
}

/// `POST /agdx/schemas/{id}/decode` body: the record payload as URL-safe
/// unpadded base64.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DecodeRecordBody {
    pub payload: String,
}

/// `POST /agdx/forks` body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkCreateBody {
    pub fork_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(default)]
    pub kind: ForkKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<String>,
}

/// `PUT /agdx/forks/{id}/rows` body. `payload_b64` is URL-safe unpadded base64,
/// like every binary body on this surface.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkPutBody {
    pub table: String,
    pub partition_id: u32,
    pub offset: u64,
    #[serde(default)]
    pub projection_id: String,
    #[serde(default)]
    pub projection_version: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub fields: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_b64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<String>,
    #[serde(default)]
    pub tombstone: bool,
}

/// `DELETE /agdx/bindings` body: which binding to remove, by its source stream
/// and topic. `projection_ref` absent removes the whole binding for that source.
/// `projection_ref` present removes only that one projection from the binding,
/// leaving the rest. Mirrors `ControlCommand::RemoveBinding`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveBindingBody {
    pub stream: String,
    pub topic: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_ref: Option<String>,
}

/// The canonical error body every `/agdx/*` route returns on a non-2xx status.
/// The HTTP binding's rule is "a 2xx carries the bare `Ok` payload, a failure
/// carries this": the status line gives the coarse class (from
/// [`ResultCode::http_status`]) and this body gives the machine-dispatchable
/// [`ResultCode`] plus a human `message`, so a client matches on `code` instead
/// of grepping the message text (which is for humans and may change). `detail`
/// carries optional structured context (e.g. the conflicting version on a CAS
/// miss) as free-form JSON.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    pub code: ResultCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<serde_json::Value>,
}

impl ErrorBody {
    /// An error body from a classified code and a human message.
    pub fn new(code: ResultCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            detail: None,
        }
    }

    /// Attach structured context.
    #[must_use]
    pub fn with_detail(mut self, detail: serde_json::Value) -> Self {
        self.detail = Some(detail);
        self
    }

    /// The HTTP status this body's `code` maps to, so a server sets the status
    /// line and the body from one value.
    pub fn http_status(&self) -> u16 {
        self.code.http_status()
    }
}

impl From<&QueryError> for ErrorBody {
    fn from(error: &QueryError) -> Self {
        Self::new(ResultCode::from(error), error.to_string())
    }
}

impl From<&KvError> for ErrorBody {
    fn from(error: &KvError) -> Self {
        Self::new(ResultCode::from(error), error.to_string())
    }
}

impl From<&ForkError> for ErrorBody {
    fn from(error: &ForkError) -> Self {
        Self::new(ResultCode::from(error), error.to_string())
    }
}

/// `?topic=` on `GET /agdx/projections`: filter to bindings off this source topic.
pub const PARAM_TOPIC: &str = "topic";
/// `?name_contains=` on a projection or schema list: substring over the name.
pub const PARAM_NAME_CONTAINS: &str = "name_contains";
/// `?id_prefix=` on `GET /agdx/projections`: keep ids starting with the prefix.
pub const PARAM_ID_PREFIX: &str = "id_prefix";
/// `?search=` on `GET /agdx/projections`: one substring matched against the
/// projection name OR id. A console with a single filter box maps to it. A
/// server matches it as `name_contains(name) OR id contains search`. Composes
/// (AND) with the narrower `name_contains` / `id_prefix` when several are set.
pub const PARAM_SEARCH: &str = "search";
/// `?prefix=` on a KV scan: base64url key prefix.
pub const PARAM_PREFIX: &str = "prefix";
/// `?start=` on a KV scan: base64url inclusive lower bound.
pub const PARAM_START: &str = "start";
/// `?end=` on a KV scan: base64url exclusive upper bound.
pub const PARAM_END: &str = "end";
/// `?key_contains=`: base64url substring the key must contain.
pub const PARAM_KEY_CONTAINS: &str = "key_contains";
/// `?limit=`: page size.
pub const PARAM_LIMIT: &str = "limit";
/// `?cursor=`: opaque continuation token from the prior page.
pub const PARAM_CURSOR: &str = "cursor";
/// `?expires_at_micros=` on a KV `PUT`: absolute expiry, epoch microseconds.
pub const PARAM_EXPIRES_AT_MICROS: &str = "expires_at_micros";
/// `?expect_version=` on a KV compare-and-swap: apply only if the key holds
/// this exact version.
pub const PARAM_EXPECT_VERSION: &str = "expect_version";
/// `?expect_absent=` on a KV compare-and-swap: apply only if the key is absent
/// (create-if-absent).
pub const PARAM_EXPECT_ABSENT: &str = "expect_absent";

/// Response header on `GET /agdx/kv/{namespace}/{key}` carrying the entry's
/// absolute expiry (epoch microseconds) as a decimal string. The value itself
/// rides the raw response body, so this header carries the one piece of
/// out-of-band metadata a single-key read needs. Owned here so the name is a
/// wire constant rather than an unscoped string. Absent means no expiry.
pub const KV_EXPIRES_AT_MICROS_HEADER: &str = "agdx-expires-at-micros";

/// `GET /agdx/projections` filters. Every field is optional, and an absent field is
/// omitted from the query string (no empty `topic=`). Field names are the
/// `PARAM_*` consts verbatim, so the client serializer and the server parser
/// share one spelling.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionListQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id_prefix: Option<String>,
    /// One substring matched against the projection name OR id, for a console
    /// with a single filter box. Composes (AND) with `name_contains`/`id_prefix`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search: Option<String>,
}

/// `GET /agdx/schemas` filters. `name_contains` is the substring filter on a
/// schema's optional name, the same spelling as the projection list, so the two
/// list surfaces share one filter vocabulary.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaListQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_contains: Option<String>,
}

/// `GET /agdx/kv/{namespace}` scan filters. The byte-valued bounds
/// (`prefix`/`start`/`end`/`key_contains`) are base64url, like every binary
/// value on this surface.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvScanQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// `PUT /agdx/kv/{namespace}/{key}` query: an optional absolute expiry.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvPutQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_micros: Option<u64>,
}

/// `GET /agdx/graph/{name}/neighbors/{node}` query: the traversal direction
/// (`out`, `in`, or `both`, omitted for the default `out`), an optional edge-type
/// filter, the hop depth (omitted for the default one hop), and a result limit
/// (omitted for the backend ceiling). One struct shared by the typed client and
/// the server route, so the two cannot drift.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphNeighborsQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Valid-time "as of" read (epoch micros): only edges valid at this instant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub as_of: Option<u64>,
}

/// `PUT /agdx/kv/{namespace}/{key}/cas` query: the compare-and-swap precondition
/// plus an optional expiry. Exactly one of `expect_version` (match the held
/// version) or `expect_absent` (create-if-absent) is set, mirroring the binary
/// `CasExpect`. The value rides the raw request body.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvCasQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_absent: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_micros: Option<u64>,
}

/// `PUT /agdx/kv/{namespace}/{key}/cas` reply on success: the new version the
/// committed write took.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasCommittedView {
    pub version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_path_builders_when_rendered_then_should_match_the_router() {
        assert_eq!(projection_path("order.v1"), "/agdx/projections/order.v1");
        assert_eq!(schema_path(7), "/agdx/schemas/7");
        assert_eq!(schema_decode_path(7), "/agdx/schemas/7/decode");
        assert_eq!(kv_namespace_path("sessions"), "/agdx/kv/sessions");
        assert_eq!(
            kv_entry_path("sessions", "dXNlcjo0Mg"),
            "/agdx/kv/sessions/dXNlcjo0Mg"
        );
        assert_eq!(
            kv_cas_path("sessions", "dXNlcjo0Mg"),
            "/agdx/kv/sessions/dXNlcjo0Mg/cas"
        );
        assert_eq!(fork_path("f1"), "/agdx/forks/f1");
        assert_eq!(fork_promote_path("f1"), "/agdx/forks/f1/promote");
        assert_eq!(fork_rows_path("f1"), "/agdx/forks/f1/rows");
    }

    #[test]
    fn given_capabilities_when_constructed_then_extended_features_default_off() {
        let caps = Capabilities::new(true, OpVersions::new(1, 1, 1, 1));
        assert!(
            caps.query.available && caps.kv.available && caps.fork,
            "core surfaces track enabled"
        );
        assert!(
            !caps.kv.cas && caps.query.consistency == Consistency::Eventual,
            "sub-features must be opt-in, never on by default"
        );
        let opted = caps
            .with_kv_cas(true)
            .with_query_consistency(Consistency::ReadYourWrites);
        assert!(opted.kv.cas && opted.query.consistency == Consistency::ReadYourWrites);
    }

    #[test]
    fn given_capabilities_backends_when_json_round_tripped_then_should_preserve_and_omit_empty() {
        use crate::hello::BackendDescriptor;
        let caps = Capabilities::new(true, OpVersions::new(1, 1, 1, 1)).with_backends(vec![
            BackendDescriptor::new("embedded", "embedded"),
            BackendDescriptor::new("warehouse", "columnar"),
        ]);
        let json = serde_json::to_string(&caps).expect("serializes");
        let back: Capabilities = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back.backends.len(), 2);
        assert_eq!(back.backends[1].id, "warehouse");
        assert_eq!(back.backends[1].kind, "columnar");

        // No advertised backends is omitted on the wire, so a pre-backends
        // capabilities reply stays byte-identical.
        let plain = Capabilities::new(true, OpVersions::new(1, 1, 1, 1));
        let json = serde_json::to_string(&plain).expect("json");
        assert!(!json.contains("backends"), "empty backends omitted: {json}");
    }

    #[test]
    fn given_a_typed_error_when_made_into_a_body_then_should_carry_code_and_message() {
        let body = ErrorBody::from(&QueryError::IndexNotFound("orders".to_owned()));
        assert_eq!(body.code, ResultCode::NotFound);
        assert_eq!(body.http_status(), 404);
        assert!(body.message.contains("orders"));
        // Round-trips as JSON, the form the HTTP surface serves it in.
        let json = serde_json::to_string(&body).expect("serializes");
        let back: ErrorBody = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, body);
    }

    #[test]
    #[cfg(feature = "http-client")]
    fn given_scan_filters_when_url_encoded_then_should_omit_absent_fields() {
        let query = KvScanQuery {
            prefix: Some("dXNlcjo".to_owned()),
            limit: Some(50),
            ..Default::default()
        };
        let encoded = serde_urlencoded::to_string(&query).expect("encodes");
        assert_eq!(encoded, "prefix=dXNlcjo&limit=50");
        // Field names are the PARAM_* consts verbatim.
        assert!(encoded.contains(&format!("{PARAM_PREFIX}=")));
        assert!(encoded.contains(&format!("{PARAM_LIMIT}=")));
    }

    #[test]
    #[cfg(feature = "http-client")]
    fn given_list_filters_when_url_encoded_then_field_names_match_the_param_consts() {
        let projections = ProjectionListQuery {
            name_contains: Some("order".to_owned()),
            id_prefix: Some("order.".to_owned()),
            ..Default::default()
        };
        let encoded = serde_urlencoded::to_string(&projections).expect("encodes");
        assert_eq!(encoded, "name_contains=order&id_prefix=order.");
        assert!(encoded.contains(&format!("{PARAM_NAME_CONTAINS}=")));
        assert!(encoded.contains(&format!("{PARAM_ID_PREFIX}=")));

        let schemas = SchemaListQuery {
            name_contains: Some("Order".to_owned()),
        };
        assert_eq!(
            serde_urlencoded::to_string(&schemas).expect("encodes"),
            "name_contains=Order"
        );
    }
}

// Managed command codes LaserData Cloud reserves. Upstream Apache Iggy uses low
// codes (1..=605), so LaserData reserves everything from one million up and the
// two never collide. Within that, each feature owns a 100-wide block:
//
//   1_000_000..=1_000_099  internal commands (capability probe, future)
//   1_000_100..=1_000_199  query family (query, projection/schema browse)
//   1_000_200..=1_000_299  key-value store
//   1_000_300..=1_000_399  forks
//   1_000_400..=1_000_499  agentic memory (remember/recall/improve/forget)
//   1_000_500..=1_000_599  knowledge graph (traverse, neighbors, upsert)
//   1_000_600..=1_000_699  agent and workflow control (submit/cancel/status/list)
//
// A query is a non-replicated read, so it is served off the log via these
// managed commands instead of a topic round-trip. Raw Apache Iggy rejects them
// with `InvalidCommand`. The values are a pinned wire contract, enforced by
// the constants test.

/// Base of LaserData's reserved managed-command range.
pub const AGDX_COMMAND_BASE: u32 = 1_000_000;
// Capability probe (internal block): LaserData Cloud answers it,
// raw Apache Iggy rejects it.
/// Managed command code: capability probe.
pub const AGDX_HELLO_CODE: u32 = AGDX_COMMAND_BASE;
/// Internal command code: the managed backend announces its served capabilities
/// (an `OpVersions`) to the streaming server over their private socket on connect. The
/// streaming server caches it and answers the client `AGDX_HELLO` with it, so the binary
/// feature bits and the HTTP capability flags cannot drift from what the backend
/// actually serves (the backend is the single source of its own truth). Not a
/// client-facing code, a client never sends it.
pub const AGDX_BACKEND_HELLO_CODE: u32 = AGDX_COMMAND_BASE + 1;
/// Fork-native command code: set this connection's advertised metadata (the
/// transport-level discovery primitive). Handled by the streaming server itself,
/// not forwarded to the plane, because it touches per-connection session state the
/// plane never sees. Connection-scoped and cleared on disconnect.
pub const AGDX_SET_CLIENT_METADATA_CODE: u32 = AGDX_COMMAND_BASE + 2;
/// Fork-native command code: list every connection with its advertised metadata.
/// The discovery read, answered by the streaming server from its connection table.
/// A LaserData-owned reply ([`crate::clients::ClientMetadataList`]), never the
/// upstream Apache Iggy `get_clients` shape, so a stock Iggy SDK against LaserData
/// Cloud and the metadata read stay byte-independent.
pub const AGDX_GET_CLIENTS_METADATA_CODE: u32 = AGDX_COMMAND_BASE + 3;
// Execute a `Query` (query block): request body is a CBOR `QueryEnvelope`,
// reply a CBOR `QueryReply`, off the log over the managed command channel.
// Direct query ops reserve 1_000_100..=1_000_109, registry browse 1_000_110+.
/// Managed command code: execute a query.
pub const AGDX_QUERY_CODE: u32 = AGDX_COMMAND_BASE + 100;
// Browse the projection registry (read-only). Get one projection by id (request
// `GetProjection`) or list them all (request `ListProjections`), reply a CBOR
// `BrowseReply`.
/// Managed command code: browse one projection.
pub const AGDX_GET_PROJECTION_CODE: u32 = AGDX_COMMAND_BASE + 110;
/// Managed command code: list projections.
pub const AGDX_LIST_PROJECTIONS_CODE: u32 = AGDX_COMMAND_BASE + 111;
/// Managed command code: browse one registered schema by id.
pub const AGDX_GET_SCHEMA_CODE: u32 = AGDX_COMMAND_BASE + 120;
/// Managed command code: list registered schemas.
pub const AGDX_LIST_SCHEMAS_CODE: u32 = AGDX_COMMAND_BASE + 121;
/// Managed command code: advisory next free schema id.
pub const AGDX_REGISTER_SCHEMA_CODE: u32 = AGDX_COMMAND_BASE + 122;
/// Managed command code: decode one payload under a registered schema id.
pub const AGDX_DECODE_RECORD_CODE: u32 = AGDX_COMMAND_BASE + 123;

// Key-value command block (1_000_200..=1_000_299). Each op is its own managed
// command, forwarded to LaserData Cloud over the same local channel the query path
// uses, with the authenticated identity stamped in.
/// Base of the KV managed-command block.
pub const AGDX_KV_BASE: u32 = AGDX_COMMAND_BASE + 200;
/// Managed command code: KV get.
pub const AGDX_KV_GET_CODE: u32 = AGDX_KV_BASE;
/// Managed command code: KV set.
pub const AGDX_KV_SET_CODE: u32 = AGDX_KV_BASE + 1;
/// Managed command code: KV scan.
pub const AGDX_KV_SCAN_CODE: u32 = AGDX_KV_BASE + 2;
/// Managed command code: KV delete one.
pub const AGDX_KV_DELETE_CODE: u32 = AGDX_KV_BASE + 3;
/// Managed command code: KV bulk delete by filter.
pub const AGDX_KV_DELETE_MANY_CODE: u32 = AGDX_KV_BASE + 4;
/// Managed command code: list the caller's namespaces.
pub const AGDX_KV_NAMESPACES_CODE: u32 = AGDX_KV_BASE + 5;
/// Managed command code: compare-and-swap a key (optimistic concurrency).
/// Additive over [`KV_OP_VERSION`] 1: a backend or server that does not serve it
/// rejects the code, which the client surfaces as an unsupported error. Whether
/// it is served is advertised by the `kv_cas` capability flag.
pub const AGDX_KV_CAS_CODE: u32 = AGDX_KV_BASE + 6;
/// Managed command code: test presence and read metadata without the value
/// (the formal `EXISTS` object primitive).
pub const AGDX_KV_EXISTS_CODE: u32 = AGDX_KV_BASE + 7;
/// Managed command code: set, refresh, or clear a key's expiry in place without
/// rewriting its value (the formal `EXPIRE` primitive).
pub const AGDX_KV_EXPIRE_CODE: u32 = AGDX_KV_BASE + 8;
/// Managed command code: apply a merge patch to a structured value (the formal
/// `PATCH` primitive).
pub const AGDX_KV_PATCH_CODE: u32 = AGDX_KV_BASE + 9;
/// Managed command code: acquire an advisory lease on a key (the formal `LEASE`
/// primitive). A backend that cannot serve it returns a clean unsupported error.
pub const AGDX_KV_LEASE_CODE: u32 = AGDX_KV_BASE + 10;
/// Managed command code: release an advisory lease early (the formal `RELEASE`
/// primitive).
pub const AGDX_KV_RELEASE_CODE: u32 = AGDX_KV_BASE + 11;
/// Managed command code: fenced compare-and-swap. Applies the CAS only while the
/// task's fence sequence still equals the presented token (the at-most-one
/// effective-writer gate). Additive over [`KV_OP_VERSION`] 1: a backend or server
/// that does not serve it rejects the code, which the client surfaces as an
/// unsupported error. Whether it is served is advertised by the `kv_cas_fenced`
/// capability flag.
pub const AGDX_KV_CAS_FENCED_CODE: u32 = AGDX_KV_BASE + 12;

// Fork block (1_000_300..): agentic copy-on-write branches of the materialized
// read model. Each op is its own managed command, forwarded over the same bridge.
/// Base of the fork managed-command block.
pub const AGDX_FORK_BASE: u32 = AGDX_COMMAND_BASE + 300;
/// Managed command code: open a fork.
pub const AGDX_FORK_CREATE_CODE: u32 = AGDX_FORK_BASE;
/// Managed command code: squash a fork.
pub const AGDX_FORK_DELETE_CODE: u32 = AGDX_FORK_BASE + 1;
/// Managed command code: promote a fork onto the trunk.
pub const AGDX_FORK_PROMOTE_CODE: u32 = AGDX_FORK_BASE + 2;
/// Managed command code: list forks.
pub const AGDX_FORK_LIST_CODE: u32 = AGDX_FORK_BASE + 3;
/// Managed command code: write a speculative fork row.
pub const AGDX_FORK_PUT_CODE: u32 = AGDX_FORK_BASE + 4;

// Knowledge graph block (1_000_500..=1_000_599). Traversal reads and the
// projector's node/edge upsert. Whether they are served is advertised by the
// `managed_graph` capability flag. Agentic memory is not a wire band of its own:
// the four-verb memory API is an SDK facade that composes `publish`, the query
// block, and this graph block, so there is one managed read/write model, not a
// parallel one.
/// Base of the graph managed-command block.
pub const AGDX_GRAPH_BASE: u32 = AGDX_COMMAND_BASE + 500;
/// Managed command code: run a graph traversal.
pub const AGDX_GRAPH_QUERY_CODE: u32 = AGDX_GRAPH_BASE;
/// Managed command code: write nodes and edges (the projector path).
pub const AGDX_GRAPH_UPSERT_CODE: u32 = AGDX_GRAPH_BASE + 1;
/// Managed command code: one-hop neighbor read.
pub const AGDX_GRAPH_NEIGHBORS_CODE: u32 = AGDX_GRAPH_BASE + 2;

// Agent and workflow control band (1_000_600..=1_000_699). Plane-served control
// operations over the agent and workflow surfaces, forwarded over the same
// bridge. Distinct from the agent ENVELOPE (the on-the-log message form, carried
// by `agdx.av`, not a command code): this band is the request-reply control
// surface a coordinator drives. Whether it is served is advertised by the
// `agent_workflow` feature bit.
/// Base of the agent and workflow control band.
pub const AGDX_AGENT_BASE: u32 = AGDX_COMMAND_BASE + 600;
/// Managed command code: submit a task to an agent or workflow.
pub const AGDX_AGENT_SUBMIT_CODE: u32 = AGDX_AGENT_BASE;
/// Managed command code: cancel a submitted task.
pub const AGDX_AGENT_CANCEL_CODE: u32 = AGDX_AGENT_BASE + 1;
/// Managed command code: read a task's status.
pub const AGDX_AGENT_STATUS_CODE: u32 = AGDX_AGENT_BASE + 2;
/// Managed command code: list tasks.
pub const AGDX_AGENT_LIST_CODE: u32 = AGDX_AGENT_BASE + 3;

// Per-surface op-schema versions, stamped on every request envelope (or, for
// the agent surface, carried as the `agdx.av` header). A peer rejects a payload
// it cannot decode rather than mis-reading a skewed schema.
/// Wire version of the query envelope.
pub const QUERY_OP_VERSION: u32 = 1;
/// Wire version of the control envelope.
pub const CONTROL_OP_VERSION: u32 = 1;
/// Wire version of the KV op envelopes.
pub const KV_OP_VERSION: u32 = 1;
/// Wire version of the fork op envelopes.
pub const FORK_OP_VERSION: u32 = 1;
/// Wire version of the graph op envelopes.
pub const GRAPH_OP_VERSION: u32 = 1;
/// Wire version of the agent and workflow control-band envelopes. Distinct from
/// [`AGENT_OP_VERSION`] (the on-the-log envelope), this versions the request and
/// reply types of the control band.
pub const AGENT_WORKFLOW_OP_VERSION: u32 = 1;
/// Wire version of the client-metadata discovery request and reply
/// ([`crate::clients`]).
pub const CLIENT_METADATA_OP_VERSION: u32 = 1;
/// Wire version of the agent presence body ([`crate::agent::AgentPresence`]) an
/// agent advertises in its connection metadata. Carried in the body's own `v`
/// field, not out-of-band, because presence rides the opaque connection-metadata
/// bytes with no envelope header to select a decoder.
pub const PRESENCE_OP_VERSION: u32 = 1;
/// Wire version of the agent envelope (the Agent Data Exchange Protocol). Carried
/// out-of-band as the typed `agdx.av` header, never inside the body: a durable
/// log record must select its decoder before any body byte is read.
pub const AGENT_OP_VERSION: u32 = 1;

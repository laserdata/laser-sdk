// Wire caps, enforced client-side so an oversized op fails fast instead of
// round-tripping, and server-side so a hostile client cannot inflate state.

/// Hard ceiling on rows in a single query reply. A `limit` above it is
/// rejected with `QueryError::TooLarge`, and a `0` limit defaults to a full
/// page. Callers page through larger result sets with `offset`.
pub const MAX_PAGE_SIZE: usize = 1000;
/// Page size a streaming reader pulls when the caller has not set an explicit
/// limit. Large enough to amortize round-trips, small enough that an
/// unbounded scan does not spike memory.
pub const DEFAULT_STREAM_PAGE_SIZE: usize = 100;
/// Hard cap on the number of `agdx.idx.*` headers a single record may carry.
/// Total header byte size is already capped, but a buggy producer could stamp
/// dozens of tiny indexed scalars under the byte budget and slow the
/// projector. 32 covers every legitimate analytics row with head-room.
pub const MAX_INDEX_ENTRIES_PER_RECORD: usize = 32;
/// Cap on the payload bytes the projector **inlines into a materialized row**
/// (when `inline_payload` is set). This bounds only the copy kept alongside the
/// indexed row in the read-model backend, never the original message: the Iggy
/// log always retains the full bytes and a fetch can replay them. Held at
/// [`MAX_VALUE_BYTES`] (8 MiB) so a single inlined body and a single KV value
/// share one "max opaque value" ceiling. A body above it still indexes and
/// still lives in the log. It is just not duplicated into the row, so a typed
/// fetch decodes from the log (or a claim-check [`ContentType::Ref`] body). The
/// cap exists because multi-MB BLOBs per row bloat the embedded query DB and
/// slow scans.
///
/// [`ContentType::Ref`]: crate::content::ContentType::Ref
pub const MAX_PROJECTOR_PAYLOAD_BYTES: usize = MAX_VALUE_BYTES;

// KV caps. Keys are arbitrary bytes (text, binary, anything), at
// most 512 bytes. Values are arbitrary opaque bytes, capped at 8 MiB:
// generous for session, flag, counter, cached-JSON, and chunked working state,
// yet well under the frame ceiling so a set request and a get reply each ride
// one socket frame with envelope head-room.
/// Maximum KV key length, in bytes.
pub const MAX_KEY_BYTES: usize = 512;
/// Maximum KV value size, in bytes.
pub const MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;
/// Hard ceiling on a KV scan page (LaserData Cloud clamps to it).
pub const MAX_SCAN_LIMIT: usize = 1000;
/// KV scan page size when the caller sets none.
pub const DEFAULT_SCAN_LIMIT: usize = 100;
/// The namespace a KV call without an explicit one binds to. A namespace is a
/// logical bucket: keys are unique within it, scans are scoped to it, and one
/// user's namespaces stay isolated from another's.
pub const DEFAULT_NAMESPACE: &str = "default";

// Fork caps. A fork id is a caller-chosen name (e.g. `"experiment-2026-q2"`),
// so its length is a validatable input cap like a KV key: the client rejects an
// over-long id before the round-trip. Per-deployment resource ceilings (how
// many forks may exist) are NOT here: those are a managed-side policy a client
// cannot validate against, surfaced only as a `ForkError`.
/// Maximum fork id length, in bytes.
pub const MAX_FORK_ID_BYTES: usize = 128;

/// Ceiling on one `[len: u32 LE][bytes]` frame on the managed-command sockets,
/// enforced by both the server and LaserData Cloud. A reply above it is replaced by
/// a structured too-large error rather than truncated. The `u32` length prefix
/// addresses far more (4 GiB), so this is a deliberate per-frame memory bound on
/// the whole-frame buffer, not a transport limit. Every consumer of the managed
/// sockets (the server-side dispatch, the streaming server sidecar, the reply-byte budget)
/// MUST source this one constant rather than redefining its own frame cap, so
/// the layers cannot disagree and a reply admitted by one is not rejected by the
/// next. Changing it moves all of them in lockstep.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
/// Hard ceiling on a single query reply's serialized bytes. A reply rides the
/// managed-command socket as one `[len: u32 LE][bytes]` frame, buffered whole,
/// so it is bounded by [`MAX_FRAME_BYTES`] by construction (the two are held
/// equal on purpose so anything a backend admits to a reply, the socket can
/// frame). Larger result sets are not returned as one oversized reply: they
/// page via [`MAX_PAGE_SIZE`] rows plus `offset`. Raising this means raising
/// `MAX_FRAME_BYTES` in lockstep across the server, LaserData Cloud, and the
/// socket buffer, since it is the same frame.
pub const MAX_QUERY_REPLY_BYTES: usize = MAX_FRAME_BYTES;

// Agent Data Exchange Protocol (AGDX) envelope caps, sized to sit inside the existing cap
// family. The metadata caps are the load-bearing ones: that field is
// bridge-injected, so a hostile or buggy edge gets a publish-time rejection
// instead of inflating every record on a topic.
/// Cap on the envelope's vocabulary strings (`operation`, `tool`,
/// `finish_reason`), each.
pub const MAX_AGENT_STRING_BYTES: usize = 256;
/// Cap on a producer-supplied idempotency key.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 64;
/// Max entries in an envelope's `metadata` map.
pub const MAX_METADATA_ENTRIES: usize = 32;
/// Max bytes in one `metadata` key.
pub const MAX_METADATA_KEY_BYTES: usize = 256;
/// Max bytes in one `metadata` value (scalar/text size).
pub const MAX_METADATA_VALUE_BYTES: usize = 1024;
/// Max total bytes across the whole `metadata` map.
pub const MAX_METADATA_TOTAL_BYTES: usize = 8192;
/// Cap on a [`BodyRef`](crate::agent::BodyRef) `reference` string (a URI,
/// object key, or KV key naming where the externalized body lives).
pub const MAX_BODY_REFERENCE_BYTES: usize = 1024;
/// Max capability entries on an [`AgentCard`](crate::agent::AgentCard).
pub const MAX_CARD_CAPABILITIES: usize = 64;

// Memory and graph caps, sized inside the existing cap family. A memory body
// shares the opaque-value ceiling. A recall page shares the query page cap.
/// Max bytes in one memory item's body (shares the opaque-value ceiling).
pub const MAX_MEMORY_BODY_BYTES: usize = MAX_VALUE_BYTES;
/// Max items a single recall returns (shares the query page cap).
pub const MAX_RECALL_LIMIT: usize = MAX_PAGE_SIZE;
/// Recall page size when the caller sets none.
pub const DEFAULT_RECALL_LIMIT: usize = DEFAULT_STREAM_PAGE_SIZE;
/// Max tags on one memory item.
pub const MAX_MEMORY_TAGS: usize = 16;
/// Max bytes in one memory tag.
pub const MAX_MEMORY_TAG_BYTES: usize = 64;
/// Hard ceiling on the hop depth a single graph traversal may request.
pub const MAX_GRAPH_TRAVERSE_DEPTH: u32 = 8;
/// Hard ceiling on nodes plus edges in one graph reply.
pub const MAX_GRAPH_RESULT_ELEMENTS: usize = 10_000;
/// Max labels on one graph node.
pub const MAX_GRAPH_NODE_LABELS: usize = 16;

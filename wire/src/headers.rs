// Header conventions. Format framing rides `agdx.*`, and index values ride the
// `agdx.idx.` namespace as small scalars. The reserved keys are deliberately SHORT
// and their values TYPED (u8 content-type code, u32 schema id, bool inline
// flag): these headers ride every message, so at millions of messages the
// key+value bytes are real bandwidth. The byte layout also matches the
// planned native Iggy reserved-block carve (`schema_id: u32, content_type:
// u8`), so the eventual migration is a dispatch-key swap.

/// Header key: the wire codec tag (`u8` code, see
/// [`ContentType::code`](crate::content::ContentType::code)).
pub const CONTENT_TYPE: &str = "agdx.ct";
/// Header key carrying the `u32` writer-schema id (typed header value) for a
/// schema-first codec (Avro, Protobuf). The LaserData Cloud resolves it against
/// its schema registry to decode the body. A bridge that lives in LaserData Cloud
/// until Iggy gains native schema dispatch.
pub const SCHEMA_ID: &str = "agdx.sid";
/// Prefix marking an indexed-field header (`agdx.idx.<name>`).
pub const IDX_PREFIX: &str = "agdx.idx.";
/// Header key: per-record inline-payload override (`bool` typed value).
/// Stamped so the projector keeps a copy of the opaque payload bytes inline
/// with the materialized row. Absent means the index row carries only indexed
/// scalars and metadata while Iggy keeps the original body in the log.
pub const INLINE_PAYLOAD: &str = "agdx.inline";
/// Header key: which projection a record routes to. Opaque string by design
/// (a name + version like `"order.v1"`), deliberately distinct from `agdx.sid`,
/// which selects a codec's writer schema rather than a materialization rule.
pub const PROJECTION_REF: &str = "agdx.ref";
/// Header key for the generic request/reply correlation id. Short on purpose
/// (it rides every request/reply pair) and independent of any agentic header
/// so the generic substrate works without provenance.
pub const CORRELATION_ID: &str = "agdx.corr";

// Well-known projected field names (the `agdx.idx.*` value after the prefix).
/// Reserved indexed-field name for the event type.
pub const FIELD_MESSAGE_TYPE: &str = "message_type";
/// Reserved indexed-field name for the timestamp (epoch micros).
pub const FIELD_TS: &str = "ts";
/// Default field/pointer for the embedding vector.
pub const VECTOR_FIELD: &str = "embedding";
/// Result-row header key carrying a tumbling window's lower edge (epoch
/// micros) when an aggregate query sets a window. Byte-identical to the
/// LaserData Cloud's `window_start` column.
pub const WINDOW_START: &str = "window_start";

// On-wire header caps (Iggy-level, not provenance-specific).
/// Soft cap on total header bytes per record.
pub const HEADER_SOFT_CAP: usize = 1024;
/// Per-header wire overhead counted toward the soft cap (key length, value
/// kind, value length), so the cap reflects on-wire size.
pub const HEADER_FRAMING_BYTES: usize = 9;
/// Maximum bytes in a single header value (Iggy serializes the length as u8).
pub const HEADER_VALUE_MAX: usize = 255;

// The provenance dictionary. OTel `gen_ai.*` keys stay spec-exact (verified
// against the gen-ai semconv registry) so traces correlate across tooling.
// Custom keys are deliberately short because they ride every agentic message.
/// Header key: conversation id (OTel `gen_ai.conversation.id`).
pub const CONVERSATION_ID: &str = "gen_ai.conversation.id";
/// Header key: the producing agent's id.
pub const AGENT_ID: &str = "gen_ai.agent.id";
/// Header key: LLM input/prompt tokens.
pub const USAGE_INPUT_TOKENS: &str = "gen_ai.usage.input_tokens";
/// Header key: LLM output/completion tokens.
pub const USAGE_OUTPUT_TOKENS: &str = "gen_ai.usage.output_tokens";
/// Header key: the message this one is a reply to.
pub const CAUSAL_PARENT: &str = "agdx.cause";
/// Header key: the conversation this was spawned from.
pub const PARENT_CONVERSATION_ID: &str = "agdx.parent_conv";
/// Header key: the root of the conversation tree.
pub const ROOT_CONVERSATION_ID: &str = "agdx.root_conv";
/// Header key: the agent this message is addressed to.
pub const TARGET_AGENT_ID: &str = "agdx.to";
/// Header key: dedup / reply-correlation key.
pub const IDEMPOTENCY_KEY: &str = "agdx.idem";
/// Header key: drop-dead time (epoch micros).
pub const DEADLINE: &str = "agdx.deadline";
/// Header key: LLM call cost in USD.
pub const COST_USD: &str = "agdx.cost";
/// Header key: the agent envelope's wire version (`u32` typed value).
/// Rides outside the CBOR body so projections, a viewer, and rolling-upgrade
/// consumers pick the decoder (and filter per version) without decoding the body.
pub const AGENT_VERSION: &str = "agdx.av";

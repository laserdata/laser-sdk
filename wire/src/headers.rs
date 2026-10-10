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
/// Header key carrying the 32-byte logical schema fingerprint for an Arrow IPC stream.
pub const LOGICAL_SCHEMA_FINGERPRINT: &str = "agdx.sfp";
/// Prefix marking an indexed-field header (`agdx.idx.<name>`).
pub const IDX_PREFIX: &str = "agdx.idx.";
/// Header key: per-record inline-payload override (`bool` typed value).
/// Stamped so the projector keeps a copy of the opaque payload bytes inline
/// with the materialized row. Absent means the index row carries only indexed
/// scalars and metadata while Iggy keeps the original body in the log.
pub const INLINE_PAYLOAD: &str = "agdx.inline";
/// Header key: which projection a record routes to. Opaque string by design
/// (a name + version like `"reading.v1"`), deliberately distinct from `agdx.sid`,
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
/// The projected-column name a deployment materializes the
/// [`CONVERSATION_ID`] header under, so every projection row is filterable by
/// the conversation that produced it without a producer stamping an index
/// header. The stable short field name the query surface's conversation lens
/// and the graph/KV conversation columns all agree on.
pub const CONVERSATION_FIELD: &str = "conversation_id";
/// Header key: the producing agent's id.
pub const AGENT_ID: &str = "gen_ai.agent.id";
/// Header key: model requested for this call.
pub const REQUEST_MODEL: &str = "gen_ai.request.model";
/// Header key: model that answered this call.
pub const RESPONSE_MODEL: &str = "gen_ai.response.model";
/// Header key: provider that served this call.
pub const PROVIDER_NAME: &str = "gen_ai.provider.name";
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
/// Header key: reserved alias for an on-behalf-of subject. Signed delegation
/// uses the envelope metadata key `on_behalf_of`.
pub const DELEGATED_BY: &str = "agdx.on_behalf_of";
/// Header key: dedup / reply-correlation key.
pub const IDEMPOTENCY_KEY: &str = "agdx.idem";
/// Header key: drop-dead time (epoch micros).
pub const DEADLINE: &str = "agdx.deadline";
/// Header key: LLM call cost in USD.
pub const COST_USD: &str = "agdx.cost";
/// Header key: the strictly-monotonic per-task fence the producer held (`u64`).
/// A consumer rejects a log-resident effect whose fence is below the highest it
/// has accepted for the task, so a stale-holder replay cannot re-fire.
pub const FENCE: &str = "agdx.fence";
/// Header key: the agent envelope's wire version (`u32` typed value).
/// Rides outside the CBOR body so projections, a viewer, and rolling-upgrade
/// consumers pick the decoder (and filter per version) without decoding the body.
pub const AGENT_VERSION: &str = "agdx.av";

// Memory scope. Carried as headers so the fold keys the read view on the scope,
// not the topic, and one shared topic still resolves every context.
/// Header key: the logical memory namespace the read view materializes under.
pub const MEMORY_NAMESPACE: &str = "agdx.mem.ns";
/// Header key: the user scope layer. Unscoped recall widens across it.
pub const MEMORY_USER: &str = "agdx.mem.user";
/// Header key: the app scope layer. Unscoped recall widens across it.
pub const MEMORY_APP: &str = "agdx.mem.app";

/// One typed user-header value as a record carries it.
#[derive(Clone, Debug, PartialEq)]
pub enum HeaderField {
    Text(String),
    Uint8(u8),
    Uint32(u32),
    Uint64(u64),
    Float64(f64),
}

/// Who a record is addressed to. Broadcast rides `agdx.to` as the literal `*`
/// and is never read as an agent id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Addressee {
    Agent(crate::agent::AgentId),
    Broadcast,
}

impl Addressee {
    /// The `agdx.to` text.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Agent(agent) => agent.as_str(),
            Self::Broadcast => BROADCAST,
        }
    }

    /// Read an `agdx.to` value. `*` is broadcast, anything else must be an
    /// agent id.
    pub fn parse(text: &str) -> Result<Self, crate::error::InvalidError> {
        if text == BROADCAST {
            return Ok(Self::Broadcast);
        }
        text.parse()
            .map(Self::Agent)
            .map_err(|_| crate::error::InvalidError::new("agdx.to is not an agent id or `*`"))
    }
}

/// The `agdx.to` value that addresses every agent.
pub const BROADCAST: &str = "*";

/// The routing and provenance headers of one record. Envelope records carry
/// the envelope version and content type. Generic records do not. Ids ride as
/// canonical strings and numbers ride typed.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordHeaders {
    pub envelope: Option<(u32, crate::content::ContentType)>,
    pub conversation: crate::agent::ConversationId,
    pub parent: Option<crate::agent::ConversationId>,
    pub root: Option<crate::agent::ConversationId>,
    pub agent: Option<crate::agent::AgentId>,
    pub addressee: Option<Addressee>,
    pub causal_parent: Option<String>,
    pub idempotency_key: Option<String>,
    pub correlation: Option<String>,
    pub fence: Option<u64>,
    pub deadline_micros: Option<u64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cost_usd: Option<f64>,
}

impl RecordHeaders {
    /// Headers of a generic record in `conversation`.
    pub fn new(conversation: crate::agent::ConversationId) -> Self {
        Self {
            envelope: None,
            conversation,
            parent: None,
            root: None,
            agent: None,
            addressee: None,
            causal_parent: None,
            idempotency_key: None,
            correlation: None,
            fence: None,
            deadline_micros: None,
            input_tokens: None,
            output_tokens: None,
            cost_usd: None,
        }
    }

    /// Headers of an envelope record: version, content type, conversation,
    /// ancestry, author, and addressee. A record without a target is
    /// addressed to every agent when `broadcast` is set, as every record on a
    /// shared session topic must be.
    pub fn for_envelope(
        envelope: &crate::agent::AgentEnvelope,
        content_type: crate::content::ContentType,
        broadcast: bool,
    ) -> Self {
        let mut headers = Self::new(envelope.conversation);
        headers.envelope = Some((crate::codes::AGENT_OP_VERSION, content_type));
        headers.parent = envelope.parent;
        headers.root = envelope.root;
        headers.agent = Some(envelope.source.clone());
        headers.addressee = match &envelope.target {
            Some(target) => Some(Addressee::Agent(target.clone())),
            None if broadcast => Some(Addressee::Broadcast),
            None => None,
        };
        headers
    }

    /// The header block, sorted by key.
    pub fn encode(&self) -> Vec<(&'static str, HeaderField)> {
        let text = |value: &dyn std::fmt::Display| HeaderField::Text(value.to_string());
        let mut block = Vec::new();
        if let Some((version, content_type)) = self.envelope {
            block.push((AGENT_VERSION, HeaderField::Uint32(version)));
            block.push((CONTENT_TYPE, HeaderField::Uint8(content_type.code())));
        }
        block.push((CONVERSATION_ID, text(&self.conversation)));
        let optional: [(&'static str, Option<HeaderField>); 12] = [
            (PARENT_CONVERSATION_ID, self.parent.map(|id| text(&id))),
            (ROOT_CONVERSATION_ID, self.root.map(|id| text(&id))),
            (
                AGENT_ID,
                self.agent
                    .as_ref()
                    .map(|agent| HeaderField::Text(agent.as_str().to_owned())),
            ),
            (
                TARGET_AGENT_ID,
                self.addressee
                    .as_ref()
                    .map(|to| HeaderField::Text(to.as_str().to_owned())),
            ),
            (
                CAUSAL_PARENT,
                self.causal_parent.clone().map(HeaderField::Text),
            ),
            (
                IDEMPOTENCY_KEY,
                self.idempotency_key.clone().map(HeaderField::Text),
            ),
            (
                CORRELATION_ID,
                self.correlation.clone().map(HeaderField::Text),
            ),
            (FENCE, self.fence.map(HeaderField::Uint64)),
            (DEADLINE, self.deadline_micros.map(HeaderField::Uint64)),
            (
                USAGE_INPUT_TOKENS,
                self.input_tokens.map(HeaderField::Uint64),
            ),
            (
                USAGE_OUTPUT_TOKENS,
                self.output_tokens.map(HeaderField::Uint64),
            ),
            (COST_USD, self.cost_usd.map(HeaderField::Float64)),
        ];
        block.extend(
            optional
                .into_iter()
                .filter_map(|(key, value)| value.map(|value| (key, value))),
        );
        block.sort_by(|left, right| left.0.cmp(right.0));
        block
    }
}

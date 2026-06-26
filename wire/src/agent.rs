// The Agent Data Exchange Protocol (AGDX) wire surface: the versioned on-log envelope that
// agent traffic rides, its id types, the task-state and error-code
// dictionaries, the dead-letter capsule, and the per-kind validity matrix.
// Specified and fixtured like every other surface. The server stays a thin
// router (no managed commands, no fork delta), so AGDX works on raw Apache Iggy
// too.
//
// Versioning is out of band. A log record is durable and read back for years,
// so the `agdx.av` header (u32, `AGENT_OP_VERSION`) selects the decoder before
// any byte of the body is read. The envelope carries no `v` field by design.
//
// Identity is a claim, by deliberate and permanent design. `source` is not
// infrastructure-stamped identity. Per-record authorship comes from topology
// (write-exclusive topics per principal) and, in a future envelope version,
// from signatures. `usage` is advisory analytics input, never
// enforcement-grade accounting.

use crate::error::InvalidError;
use crate::query::Value;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

/// Why parsing a Crockford base32 id failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum IdParseError {
    #[error("id must be 26 characters, got {got}")]
    Length { got: usize },
    #[error("id contains invalid character `{0}`")]
    Char(char),
    #[error("id overflows 128 bits")]
    Overflow,
}

// Every u128 id rides the CBOR payload as one atomic 16-byte byte string
// (big-endian), a fixed-width form with no bignum tag. Duplicated routing
// HEADERS use Iggy's typed Uint128 representation instead (little-endian on the
// server wire). Fixtures pin both so the two encodings cannot drift silently.
macro_rules! wire_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(u128);

        impl $name {
            /// Wrap a raw 128-bit id.
            pub const fn from_u128(value: u128) -> Self {
                Self(value)
            }

            /// The raw 128-bit value.
            pub const fn as_u128(self) -> u128 {
                self.0
            }

            /// The big-endian 16 bytes (the payload wire form).
            pub const fn to_bytes(self) -> [u8; 16] {
                self.0.to_be_bytes()
            }

            /// An id from its big-endian 16 bytes.
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(u128::from_be_bytes(bytes))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let encoded = crockford_encode(self.0);
                // The alphabet is ASCII, so the buffer is always valid UTF-8.
                f.write_str(std::str::from_utf8(&encoded).expect("crockford output is ASCII"))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self)
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                crockford_decode(s).map(Self)
            }
        }

        impl From<u128> for $name {
            fn from(value: u128) -> Self {
                Self(value)
            }
        }

        impl From<$name> for u128 {
            fn from(value: $name) -> u128 {
                value.0
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_bytes(&self.to_bytes())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                struct BytesVisitor;

                impl<'de> Visitor<'de> for BytesVisitor {
                    type Value = $name;

                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("16 big-endian id bytes")
                    }

                    fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                        let bytes: [u8; 16] = v
                            .try_into()
                            .map_err(|_| E::invalid_length(v.len(), &self))?;
                        Ok($name::from_bytes(bytes))
                    }
                }

                deserializer.deserialize_bytes(BytesVisitor)
            }
        }
    };
}

// Reused by the memory and graph modules for their id newtypes (MemoryId,
// NodeId, EdgeId), so every wire id shares one display, codec, and parse form.
pub(crate) use wire_id;

wire_id!(
    /// A record's producer-assigned identity, a ULID minted before publish.
    ///
    /// Portable in a way a log position is not. An offset is meaningful only
    /// within the partition that assigned it, so a copy elsewhere (a re-publish
    /// into another stream, partition, or DR cluster) gets a fresh position.
    /// This id rides in the payload and stays the same everywhere.
    RecordId
);
wire_id!(
    /// The conversation a message belongs to. The unit of ordering and the
    /// partition key, and the trace id of the causal trace.
    ConversationId
);
wire_id!(
    /// Request/reply pairing id. A2A task identity and MCP tool-call ids map
    /// onto it at the bridges.
    CorrelationId
);
wire_id!(
    /// A chunk stream's grouping id when several streams run under one
    /// correlation. Named `channel` because `stream` is an iggy topology term.
    ChannelId
);

/// The Iggy binding's packing of the causal locator (`cause_at`, and the
/// dead-letter `source`).
///
/// The locator rides the wire as one **opaque byte string**, the substrate-
/// neutral slot every binding packs its own form into, so the envelope itself
/// names no server. A consumer that cannot interpret the bytes ignores them and
/// falls back to the portable `cause` / `record` id. The Iggy packing is the
/// four-level address as fixed-width big-endian bytes: `stream_id`, `topic_id`,
/// `partition_id` (each u32) then `offset` (u64), 20 bytes. Another binding
/// packs its own locator (a topic name or id, a partition, an offset) into the
/// same opaque slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LogPosition {
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub offset: u64,
}

const LOG_POSITION_BYTES: usize = 20;

impl LogPosition {
    /// A locator at `(stream, topic, partition, offset)`.
    pub const fn new(stream_id: u32, topic_id: u32, partition_id: u32, offset: u64) -> Self {
        Self {
            stream_id,
            topic_id,
            partition_id,
            offset,
        }
    }

    /// The Iggy locator packed as 20 big-endian bytes (the opaque wire form).
    pub fn to_bytes(self) -> [u8; LOG_POSITION_BYTES] {
        let mut out = [0u8; LOG_POSITION_BYTES];
        out[0..4].copy_from_slice(&self.stream_id.to_be_bytes());
        out[4..8].copy_from_slice(&self.topic_id.to_be_bytes());
        out[8..12].copy_from_slice(&self.partition_id.to_be_bytes());
        out[12..20].copy_from_slice(&self.offset.to_be_bytes());
        out
    }

    /// Unpack the Iggy locator from its 20 big-endian bytes.
    pub fn from_bytes(bytes: [u8; LOG_POSITION_BYTES]) -> Self {
        let u32_at = |start: usize| {
            u32::from_be_bytes(bytes[start..start + 4].try_into().expect("4-byte slice"))
        };
        Self {
            stream_id: u32_at(0),
            topic_id: u32_at(4),
            partition_id: u32_at(8),
            offset: u64::from_be_bytes(bytes[12..20].try_into().expect("8-byte slice")),
        }
    }
}

// The locator rides the payload as one opaque CBOR byte string, not a named-field
// map, so the slot is binding-neutral (an Iggy packing here, another binding's
// packing elsewhere) and pins smaller than the four labelled fields would.
impl Serialize for LogPosition {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.to_bytes())
    }
}

impl<'de> Deserialize<'de> for LogPosition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LocatorVisitor;

        impl<'de> Visitor<'de> for LocatorVisitor {
            type Value = LogPosition;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("20 packed locator bytes")
            }

            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                let bytes: [u8; LOG_POSITION_BYTES] = v
                    .try_into()
                    .map_err(|_| E::invalid_length(v.len(), &self))?;
                Ok(LogPosition::from_bytes(bytes))
            }
        }

        deserializer.deserialize_bytes(LocatorVisitor)
    }
}

/// A producer-supplied business idempotency key: non-empty, at most 64 bytes.
///
/// A readable string by design, often a natural business key like
/// `order-123-attempt-2`, so consoles and dead-letter capsules stay debuggable.
/// The reliable consumer's dedup store hashes it internally.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// The key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for IdempotencyKey {
    type Err = InvalidError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.to_owned().try_into()
    }
}

impl TryFrom<String> for IdempotencyKey {
    type Error = InvalidError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(InvalidError::new("idempotency key must not be empty"));
        }
        if value.len() > crate::limits::MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(InvalidError::new(format!(
                "idempotency key is {}B, exceeds cap {}B",
                value.len(),
                crate::limits::MAX_IDEMPOTENCY_KEY_BYTES
            )));
        }
        Ok(Self(value))
    }
}

impl From<IdempotencyKey> for String {
    fn from(value: IdempotencyKey) -> Self {
        value.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// An agent's identity: a bounded, human-readable name string.
///
/// A string rather than an opaque numeric id, because an agent is a named
/// principal every edge protocol spells out as text (an A2A agent name or URL,
/// an MCP server name, OTel `gen_ai.agent.id`). The SDK's named agents map
/// straight onto it with no lossy hash. It is an authorship claim on shared
/// topics (see the module docs). Non-empty, at most
/// [`MAX_AGENT_STRING_BYTES`](crate::limits::MAX_AGENT_STRING_BYTES), and free
/// of ASCII control characters. Every other character is allowed.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AgentId(String);

impl AgentId {
    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for AgentId {
    type Err = InvalidError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.to_owned().try_into()
    }
}

impl TryFrom<String> for AgentId {
    type Error = InvalidError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty() {
            return Err(InvalidError::new("agent id must not be empty"));
        }
        if value.len() > crate::limits::MAX_AGENT_STRING_BYTES {
            return Err(InvalidError::new(format!(
                "agent id is {}B, exceeds cap {}B",
                value.len(),
                crate::limits::MAX_AGENT_STRING_BYTES
            )));
        }
        if let Some(c) = value.chars().find(|c| c.is_control()) {
            return Err(InvalidError::new(format!(
                "agent id must not contain control characters (found {c:?})"
            )));
        }
        Ok(Self(value))
    }
}

impl TryFrom<&str> for AgentId {
    type Error = InvalidError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.to_owned().try_into()
    }
}

impl From<AgentId> for String {
    fn from(value: AgentId) -> Self {
        value.0
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// What a message is. A closed vocabulary by design: adding a kind requires an
/// `AGENT_OP_VERSION` bump and a hello advertisement, because an unknown kind
/// must fail decode rather than flow misinterpreted.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    strum::Display,
    strum::EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum AgentKind {
    /// Expects a reply or effect. Requires `correlation`. Fire-and-forget
    /// commands do not exist by definition. Those are events.
    Command,
    /// The paired answer to a command.
    Response,
    /// Expects nothing.
    Event,
    /// One piece of a stream, ordered by `sequence` within a `channel`.
    Chunk,
    /// Lifecycle signal, discriminated by `operation`: task updates (`task`),
    /// liveness cards (`card`), and progress ticks (`progress`).
    Status,
    /// A terminal failure. The body is a structured [`AgentErrorBody`].
    Error,
}

/// A2A's task lifecycle, adopted verbatim, riding the wire as a u8 code (the
/// `agdx.ct` dictionary pattern) so a future A2A state takes the next free code
/// and flows through old consumers as an opaque non-terminal value instead of
/// forcing a version bump on someone else's release schedule. Codes are
/// permanent and never renumbered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "u8", into = "u8")]
pub enum TaskState {
    Submitted,
    Working,
    InputRequired,
    Completed,
    Canceled,
    Failed,
    Rejected,
    AuthRequired,
    Unknown,
    /// A code this build does not know: passed through, treated as non-terminal.
    Unrecognized(u8),
}

impl TaskState {
    /// The pinned wire code.
    pub const fn code(self) -> u8 {
        match self {
            TaskState::Submitted => 1,
            TaskState::Working => 2,
            TaskState::InputRequired => 3,
            TaskState::Completed => 4,
            TaskState::Canceled => 5,
            TaskState::Failed => 6,
            TaskState::Rejected => 7,
            TaskState::AuthRequired => 8,
            TaskState::Unknown => 9,
            TaskState::Unrecognized(code) => code,
        }
    }

    /// The state for a wire code (total: unknown codes become
    /// [`Unrecognized`](Self::Unrecognized)).
    pub const fn from_code(code: u8) -> Self {
        match code {
            1 => TaskState::Submitted,
            2 => TaskState::Working,
            3 => TaskState::InputRequired,
            4 => TaskState::Completed,
            5 => TaskState::Canceled,
            6 => TaskState::Failed,
            7 => TaskState::Rejected,
            8 => TaskState::AuthRequired,
            9 => TaskState::Unknown,
            other => TaskState::Unrecognized(other),
        }
    }

    /// Whether this state ends the task (A2A's terminal set). Unrecognized
    /// codes are non-terminal by rule.
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskState::Completed | TaskState::Canceled | TaskState::Failed | TaskState::Rejected
        )
    }
}

impl From<u8> for TaskState {
    fn from(code: u8) -> Self {
        Self::from_code(code)
    }
}

impl From<TaskState> for u8 {
    fn from(state: TaskState) -> u8 {
        state.code()
    }
}

impl fmt::Display for TaskState {
    // The A2A kebab-case names, for bridges and consoles.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskState::Submitted => f.write_str("submitted"),
            TaskState::Working => f.write_str("working"),
            TaskState::InputRequired => f.write_str("input-required"),
            TaskState::Completed => f.write_str("completed"),
            TaskState::Canceled => f.write_str("canceled"),
            TaskState::Failed => f.write_str("failed"),
            TaskState::Rejected => f.write_str("rejected"),
            TaskState::AuthRequired => f.write_str("auth-required"),
            TaskState::Unknown => f.write_str("unknown"),
            TaskState::Unrecognized(code) => write!(f, "unrecognized-{code}"),
        }
    }
}

impl FromStr for TaskState {
    type Err = InvalidError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "submitted" => TaskState::Submitted,
            "working" => TaskState::Working,
            "input-required" => TaskState::InputRequired,
            "completed" => TaskState::Completed,
            "canceled" => TaskState::Canceled,
            "failed" => TaskState::Failed,
            "rejected" => TaskState::Rejected,
            "auth-required" => TaskState::AuthRequired,
            "unknown" => TaskState::Unknown,
            other => return Err(InvalidError::new(format!("unknown task state `{other}`"))),
        })
    }
}

/// Typed token accounting, OTel-aligned (`gen_ai.usage.*`, the current
/// `input_tokens` and `output_tokens` names, never the deprecated
/// prompt/completion pair).
///
/// Advisory analytics input. It is agent-written, so budgets enforce where the
/// LLM call executes, never on this field.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_read_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_creation_input_tokens: Option<u64>,
}

/// Why an agent operation failed, as a pinned u8 dictionary (the
/// [`TaskState`] pattern: unknown codes decode and pass through).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "u8", into = "u8")]
pub enum AgentErrorCode {
    InvalidRequest,
    Unauthorized,
    Unsupported,
    DeadlineExceeded,
    Cancelled,
    ToolFailure,
    Internal,
    /// A code this build does not know: pass it through.
    Unrecognized(u8),
}

impl AgentErrorCode {
    /// The pinned wire code.
    pub const fn code(self) -> u8 {
        match self {
            AgentErrorCode::InvalidRequest => 1,
            AgentErrorCode::Unauthorized => 2,
            AgentErrorCode::Unsupported => 3,
            AgentErrorCode::DeadlineExceeded => 4,
            AgentErrorCode::Cancelled => 5,
            AgentErrorCode::ToolFailure => 6,
            AgentErrorCode::Internal => 7,
            AgentErrorCode::Unrecognized(code) => code,
        }
    }

    /// The error code for a wire code (total).
    pub const fn from_code(code: u8) -> Self {
        match code {
            1 => AgentErrorCode::InvalidRequest,
            2 => AgentErrorCode::Unauthorized,
            3 => AgentErrorCode::Unsupported,
            4 => AgentErrorCode::DeadlineExceeded,
            5 => AgentErrorCode::Cancelled,
            6 => AgentErrorCode::ToolFailure,
            7 => AgentErrorCode::Internal,
            other => AgentErrorCode::Unrecognized(other),
        }
    }
}

impl From<u8> for AgentErrorCode {
    fn from(code: u8) -> Self {
        Self::from_code(code)
    }
}

impl From<AgentErrorCode> for u8 {
    fn from(code: AgentErrorCode) -> u8 {
        code.code()
    }
}

/// The structured body of a `kind = error` envelope, mirroring the wire error
/// enums of the other surfaces. The `code` is the machine discriminator, the
/// optional `message` is human detail.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentErrorBody {
    pub code: AgentErrorCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default)]
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<BTreeMap<String, Value>>,
}

/// Why a message was dead-lettered, as a pinned u8 dictionary.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(from = "u8", into = "u8")]
pub enum DeadLetterReason {
    RetryExhausted,
    Rejected,
    DecodeFailed,
    DeadlineExceeded,
    /// A code this build does not know: pass it through.
    Unrecognized(u8),
}

impl DeadLetterReason {
    /// The pinned wire code.
    pub const fn code(self) -> u8 {
        match self {
            DeadLetterReason::RetryExhausted => 1,
            DeadLetterReason::Rejected => 2,
            DeadLetterReason::DecodeFailed => 3,
            DeadLetterReason::DeadlineExceeded => 4,
            DeadLetterReason::Unrecognized(code) => code,
        }
    }

    /// The reason for a wire code (total).
    pub const fn from_code(code: u8) -> Self {
        match code {
            1 => DeadLetterReason::RetryExhausted,
            2 => DeadLetterReason::Rejected,
            3 => DeadLetterReason::DecodeFailed,
            4 => DeadLetterReason::DeadlineExceeded,
            other => DeadLetterReason::Unrecognized(other),
        }
    }
}

impl From<u8> for DeadLetterReason {
    fn from(code: u8) -> Self {
        Self::from_code(code)
    }
}

impl From<DeadLetterReason> for u8 {
    fn from(reason: DeadLetterReason) -> u8 {
        reason.code()
    }
}

/// The agent-level dead-letter capsule: the poison message's log position, the
/// reason, and the original payload verbatim.
///
/// The payload is the encoded [`AgentEnvelope`], byte-identical, so redrive is
/// trivially correct. Republish it to the source topic, or inspect it by
/// decoding the inner envelope. The capsule rides a dedicated dead-letter topic
/// in the agent stream, CBOR like everything else in AGDX. LaserData Cloud's own
/// dead-letter capsules stay operator-facing JSON on a different topic, for a
/// different audience.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentDeadLetter {
    pub source: LogPosition,
    pub reason: DeadLetterReason,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
}

/// The pinned minimal body of a liveness or capability card (`status` with
/// `operation = card`).
///
/// Without a pinned shape every bridge and viewer invents its own card variant.
/// The publishing agent rides the envelope's `source`, so the card carries only
/// what discovery needs. Anything richer is application data in `metadata` or a
/// follow-up body, never new card fields by convention.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AgentCard {
    /// Human-readable display label (a viewer shows the base32 id without
    /// it). Capped like every vocabulary string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// The agent's own version label, opaque to the protocol.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// What the agent serves: operation and tool names, capped in both count
    /// and entry size.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
    /// How long this card stays fresh. A card older than its ttl means a dead
    /// agent, the convention that makes cards liveness and not just discovery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_micros: Option<u64>,
}

impl AgentCard {
    /// Check a decoded card against the caps.
    pub fn validate(&self) -> Result<(), ValidateError> {
        cap_str(self.name.as_deref(), "name")?;
        cap_str(self.version.as_deref(), "version")?;
        if self.capabilities.len() > crate::limits::MAX_CARD_CAPABILITIES {
            return Err(ValidateError::TooLarge {
                field: "capabilities",
                size: self.capabilities.len(),
                cap: crate::limits::MAX_CARD_CAPABILITIES,
            });
        }
        for capability in &self.capabilities {
            cap_str(Some(capability), "capability")?;
        }
        Ok(())
    }
}

/// The claim-check capsule a `agdx.ct = ref` body carries.
///
/// The content lives elsewhere (object storage, the KV store, another topic).
/// The record carries where it lives, how big it is, and a digest, so any
/// consumer verifies the fetched bytes against the log without trusting the
/// store. The envelope and the validity matrix are untouched: a referenced
/// body is still a `body`, just one whose bytes are a `BodyRef` instead of the
/// content itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BodyRef {
    /// Where the bytes live: a URI, object key, or KV key. Bounded by
    /// [`MAX_BODY_REFERENCE_BYTES`](crate::limits::MAX_BODY_REFERENCE_BYTES).
    pub reference: String,
    /// The externalized content's size in bytes.
    pub size_bytes: u64,
    /// SHA-256 of the externalized content, exactly 32 bytes as a CBOR byte string.
    #[serde(with = "crate::encoding::bin_bytes")]
    pub sha256: Vec<u8>,
    /// Encryption scheme code, dormant like [`Signature::scheme`]. Absent means
    /// plaintext. Codes are assigned when the key registry (the same registry
    /// signatures verify against) lands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<u8>,
}

const SHA256_BYTES: usize = 32;

impl BodyRef {
    /// A plaintext reference to externalized content.
    pub fn new(reference: impl Into<String>, size_bytes: u64, sha256: [u8; 32]) -> Self {
        Self {
            reference: reference.into(),
            size_bytes,
            sha256: sha256.to_vec(),
            encryption: None,
        }
    }

    /// Check a decoded capsule: non-empty bounded `reference`, 32-byte digest.
    ///
    /// Receivers run it after decode. [`new`](Self::new) is valid by construction.
    pub fn validate(&self) -> Result<(), ValidateError> {
        if self.reference.is_empty() {
            return Err(ValidateError::Invalid {
                field: "reference",
                reason: "reference must not be empty".to_owned(),
            });
        }
        if self.reference.len() > crate::limits::MAX_BODY_REFERENCE_BYTES {
            return Err(ValidateError::TooLarge {
                field: "reference",
                size: self.reference.len(),
                cap: crate::limits::MAX_BODY_REFERENCE_BYTES,
            });
        }
        if self.sha256.len() != SHA256_BYTES {
            return Err(ValidateError::Invalid {
                field: "sha256",
                reason: format!(
                    "digest must be {SHA256_BYTES} bytes, got {}",
                    self.sha256.len()
                ),
            });
        }
        Ok(())
    }
}

/// A detached envelope signature: designed but dormant.
///
/// The type exists so the future opt-in (per-agent keys, consumer-side
/// verification against a key registry) is an additive
/// `signature: Option<Signature>` envelope field, not a redesign. No crypto
/// dependency enters this crate. Signing and verification live SDK-side.
///
/// Field sizes are scheme-discriminated, so the `scheme` byte exists precisely
/// to keep the type from welding to one algorithm. The wire fields are bounded
/// bytes, [`validate`](Self::validate) enforces the registered per-scheme
/// lengths, and unknown schemes pass through.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature {
    /// The signing scheme code ([`SIGNATURE_SCHEME_ED25519`] = 1). Codes are
    /// permanent, and unknown codes pass through like every open dictionary.
    pub scheme: u8,
    /// Which of the agent's registered keys signed (8 bytes for Ed25519).
    #[serde(with = "crate::encoding::bin_bytes")]
    pub key_id: Vec<u8>,
    /// The signature over the canonical envelope encoding (signature field
    /// absent), domain-separated (64 bytes for Ed25519).
    #[serde(with = "crate::encoding::bin_bytes")]
    pub bytes: Vec<u8>,
}

/// The Ed25519 signing scheme code.
pub const SIGNATURE_SCHEME_ED25519: u8 = 1;

/// The domain separator prefixed to the canonical envelope encoding before
/// signing, so an AGDX signature can never be replayed into another protocol.
/// The canonical encoding is this crate's own: named-field CBOR, fields in
/// declaration order, absent optionals skipped, the signature field absent.
pub const SIGNATURE_DOMAIN: &[u8] = b"agdx.signature.v1";

const ED25519_KEY_ID_BYTES: usize = 8;
const ED25519_SIGNATURE_BYTES: usize = 64;

impl Signature {
    /// Check a signature capsule against its scheme's registered lengths.
    ///
    /// Unknown scheme codes validate, so future schemes flow through old
    /// consumers. Verification itself is SDK-side and scheme-aware.
    pub fn validate(&self) -> Result<(), ValidateError> {
        if self.scheme != SIGNATURE_SCHEME_ED25519 {
            return Ok(());
        }
        if self.key_id.len() != ED25519_KEY_ID_BYTES {
            return Err(ValidateError::Invalid {
                field: "key_id",
                reason: format!(
                    "Ed25519 key id must be {ED25519_KEY_ID_BYTES} bytes, got {}",
                    self.key_id.len()
                ),
            });
        }
        if self.bytes.len() != ED25519_SIGNATURE_BYTES {
            return Err(ValidateError::Invalid {
                field: "bytes",
                reason: format!(
                    "Ed25519 signature must be {ED25519_SIGNATURE_BYTES} bytes, got {}",
                    self.bytes.len()
                ),
            });
        }
        Ok(())
    }
}

/// The AGDX envelope: one CBOR named-field decode unit per agent message.
///
/// Field semantics, the per-kind validity matrix, and the caps are enforced by
/// [`validate`], and the per-kind constructors stamp the required shape. Routing
/// fields (`conversation`, `target`, content type) are also stamped as typed
/// headers so projections and plain Iggy consumers work without decoding
/// bodies. The envelope is the typed, versioned form of what the headers say.
///
/// `metadata` is the AGDX-native extension slot, distinct from headers (the
/// substrate and observability dictionary) and `body` (the content). Foreign
/// metadata (A2A `metadata`, MCP `_meta`) never maps into it. It tunnels whole
/// inside `body`, keeping bridge round trips byte-identical.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentEnvelope {
    pub kind: AgentKind,
    /// Producer-assigned record identity, required on every kind except
    /// `chunk` (chunks are identified by `channel` + `sequence`, saving the
    /// id bytes and a clock-and-entropy call per token).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<RecordId>,
    /// Also the partition key.
    pub conversation: ConversationId,
    /// Agent-authorship claim (see the module docs).
    pub source: AgentId,
    /// Routing refinement within a shared topic. The topic itself is the
    /// primary address. Consumer-side filtering by `target` is a convenience,
    /// not a confidentiality control. The topic is the boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<AgentId>,
    /// The causal parent's record id: the identity half of the causal pointer,
    /// stable across replication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<RecordId>,
    /// The causal parent's log position: the locator half, an O(1) dereference
    /// for raw log walkers, deployment-local.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause_at: Option<LogPosition>,
    /// Request/reply pairing. A2A task identity maps onto it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation: Option<CorrelationId>,
    /// Chunk grouping when many streams run under one correlation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<ChannelId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<IdempotencyKey>,
    /// Drop-dead time, epoch micros. On a stream-opening message it is also
    /// the reader-local abandonment bound: the producer knows its own model
    /// timeout, the consumer would be guessing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_micros: Option<u64>,
    /// Chunk ordering within a channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    /// Terminal flag: the final chunk of a stream / the final task update.
    /// `false` is equivalent to absence and is skipped on encode, so only `true`
    /// has protocol meaning. Maps one-to-one onto A2A's `final`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub last: bool,
    /// Why a stream or response ended (OTel finish-reason vocabulary: stop,
    /// length, content_filter, tool_call, ...). A string deliberately: that
    /// vocabulary belongs to OTel and the providers, not to us.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_state: Option<TaskState>,
    /// OTel `gen_ai.operation.name` value (`chat`, `execute_tool`, ...). On
    /// `status` it is the required discriminator (`task`, `card`, `progress`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    /// OTel `gen_ai.tool.name`, for tool commands and results.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
    /// AGDX-native scalar extension context. Never foreign metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<BTreeMap<String, Value>>,
    /// Must-understand marker: a bitset of feature bits a receiver MUST
    /// understand to process this message correctly (see [`features`]). A
    /// receiver that sees a set bit it does not implement MUST reject or
    /// dead-letter the message rather than mis-handle it ([`unmet_requirements`]).
    /// `0` (the default, skipped on the wire so pre-marker records stay
    /// byte-identical) means "ignore anything you don't understand", the
    /// open-world default. This lets one message demand strict handling of a new
    /// feature without a whole-envelope version bump (the spec's `must_understand`, A9.1).
    ///
    /// [`features`]: crate::agent::features
    /// [`unmet_requirements`]: AgentEnvelope::unmet_requirements
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub must_understand: u64,
    /// The content, codec per the `agdx.ct` header. Default-empty and skipped
    /// when empty. The validity matrix says which kinds require it.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "crate::encoding::bin_bytes"
    )]
    pub body: Vec<u8>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

/// Must-understand feature bits for [`AgentEnvelope::must_understand`]. Each
/// constant names one capability a message may demand a receiver implement.
/// Defined additively as features land: a newer producer sets a bit an older
/// receiver does not know, and that receiver rejects rather than mis-handling
/// the message. No bits are defined yet, so today the marker is the mechanism
/// in place for the first feature that needs strict handling.
pub mod features {
    /// A receiver's full understood set is the OR of the bits it implements.
    /// With no feature bits defined yet, a current build understands none and
    /// only ever sees `must_understand == 0`.
    pub const NONE: u64 = 0;
}

impl AgentEnvelope {
    fn base(kind: AgentKind, conversation: ConversationId, source: AgentId) -> Self {
        Self {
            kind,
            record: None,
            conversation,
            source,
            target: None,
            cause: None,
            cause_at: None,
            correlation: None,
            channel: None,
            idempotency_key: None,
            deadline_micros: None,
            sequence: None,
            last: false,
            finish_reason: None,
            task_state: None,
            operation: None,
            tool: None,
            usage: None,
            metadata: None,
            must_understand: 0,
            body: Vec::new(),
        }
    }

    /// Declare that a receiver MUST understand the feature `bits` to process this
    /// message. Bits a receiver lacks make it reject the message
    /// ([`unmet_requirements`](Self::unmet_requirements)). Absent bits are
    /// ignore-if-unknown. Additive builder method.
    #[must_use]
    pub fn requiring(mut self, bits: u64) -> Self {
        self.must_understand = bits;
        self
    }

    /// The subset of this message's [`must_understand`](Self::must_understand)
    /// bits NOT present in the receiver's `understood` set. Non-zero means the
    /// receiver cannot safely process the message and must reject or
    /// dead-letter it rather than mis-handle a feature it does not implement.
    pub fn unmet_requirements(&self, understood: u64) -> u64 {
        self.must_understand & !understood
    }

    /// A `command`: expects a reply or effect, so `correlation` is required.
    pub fn command(
        record: RecordId,
        conversation: ConversationId,
        source: AgentId,
        correlation: CorrelationId,
        body: Vec<u8>,
    ) -> Self {
        let mut envelope = Self::base(AgentKind::Command, conversation, source);
        envelope.record = Some(record);
        envelope.correlation = Some(correlation);
        envelope.body = body;
        envelope
    }

    /// A `response`: the paired answer to a command (same `correlation`).
    pub fn response(
        record: RecordId,
        conversation: ConversationId,
        source: AgentId,
        correlation: CorrelationId,
        body: Vec<u8>,
    ) -> Self {
        let mut envelope = Self::base(AgentKind::Response, conversation, source);
        envelope.record = Some(record);
        envelope.correlation = Some(correlation);
        envelope.body = body;
        envelope
    }

    /// An `event`: expects nothing.
    pub fn event(
        record: RecordId,
        conversation: ConversationId,
        source: AgentId,
        body: Vec<u8>,
    ) -> Self {
        let mut envelope = Self::base(AgentKind::Event, conversation, source);
        envelope.record = Some(record);
        envelope.body = body;
        envelope
    }

    /// A `chunk` of the stream `channel`, ordered by `sequence`. Mark the
    /// final one with [`terminal`](Self::terminal).
    pub fn chunk(
        conversation: ConversationId,
        source: AgentId,
        correlation: CorrelationId,
        channel: ChannelId,
        sequence: u64,
        body: Vec<u8>,
    ) -> Self {
        let mut envelope = Self::base(AgentKind::Chunk, conversation, source);
        envelope.correlation = Some(correlation);
        envelope.channel = Some(channel);
        envelope.sequence = Some(sequence);
        envelope.body = body;
        envelope
    }

    /// A `status` signal discriminated by `operation` (`task`, `card`,
    /// `progress`). Task updates additionally require `correlation` and
    /// `task_state` (use the with-setters).
    pub fn status(
        record: RecordId,
        conversation: ConversationId,
        source: AgentId,
        operation: impl Into<String>,
    ) -> Self {
        let mut envelope = Self::base(AgentKind::Status, conversation, source);
        envelope.record = Some(record);
        envelope.operation = Some(operation.into());
        envelope
    }

    /// An `error` terminal for `correlation`. `body` is the encoded
    /// [`AgentErrorBody`].
    pub fn error(
        record: RecordId,
        conversation: ConversationId,
        source: AgentId,
        correlation: CorrelationId,
        body: Vec<u8>,
    ) -> Self {
        let mut envelope = Self::base(AgentKind::Error, conversation, source);
        envelope.record = Some(record);
        envelope.correlation = Some(correlation);
        envelope.body = body;
        envelope
    }

    /// Narrow delivery to one agent within a shared topic.
    pub fn with_target(mut self, target: AgentId) -> Self {
        self.target = Some(target);
        self
    }

    /// Stamp the causal parent: its record id (identity) and, when known, its
    /// log position (locator). A handler has both for free from the message it
    /// is replying to.
    pub fn with_cause(mut self, cause: RecordId, cause_at: Option<LogPosition>) -> Self {
        self.cause = Some(cause);
        self.cause_at = cause_at;
        self
    }

    /// Pair this message with a correlation id. Required on `command`,
    /// `response`, `error`, and `chunk`. Optional on `event` and non-task
    /// `status`.
    pub fn with_correlation(mut self, correlation: CorrelationId) -> Self {
        self.correlation = Some(correlation);
        self
    }

    /// Attach a business idempotency key (commands, responses, events only).
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    /// Declare the drop-dead time (and, on a stream-opening message, the
    /// abandonment bound).
    pub fn with_deadline_micros(mut self, deadline_micros: u64) -> Self {
        self.deadline_micros = Some(deadline_micros);
        self
    }

    /// Mark this message terminal (`last = true`), with the reason the stream
    /// or response ended.
    pub fn terminal(mut self, finish_reason: impl Into<String>) -> Self {
        self.last = true;
        self.finish_reason = Some(finish_reason.into());
        self
    }

    /// Attach a task state. Status task updates require it. Responses and
    /// errors may carry it as the one-message terminal convenience.
    pub fn with_task_state(mut self, state: TaskState) -> Self {
        self.task_state = Some(state);
        self
    }

    /// Set the OTel operation name.
    pub fn with_operation(mut self, operation: impl Into<String>) -> Self {
        self.operation = Some(operation.into());
        self
    }

    /// Set the OTel tool name.
    pub fn with_tool(mut self, tool: impl Into<String>) -> Self {
        self.tool = Some(tool.into());
        self
    }

    /// Attach token accounting (advisory).
    pub fn with_usage(mut self, usage: TokenUsage) -> Self {
        self.usage = Some(usage);
        self
    }

    /// Add one AGDX-native metadata entry.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.metadata
            .get_or_insert_with(BTreeMap::new)
            .insert(key.into(), value.into());
        self
    }
}

/// A validity-matrix or cap violation. Receivers treat these as protocol
/// errors rather than guessing, and the SDK rejects them at publish time.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ValidateError {
    #[error("{kind} requires `{field}`")]
    Missing {
        kind: AgentKind,
        field: &'static str,
    },
    #[error("`{field}` is invalid on {kind}")]
    Forbidden {
        kind: AgentKind,
        field: &'static str,
    },
    #[error("`{field}` is {size}B, exceeds cap {cap}B")]
    TooLarge {
        field: &'static str,
        size: usize,
        cap: usize,
    },
    #[error("`{field}`: {reason}")]
    Invalid { field: &'static str, reason: String },
}

// The per-kind validity matrix, mechanical. R = required, O = optional,
// X = invalid:
//
// | field            | command | response | event | chunk | status | error |
// |------------------|---------|----------|-------|-------|--------|-------|
// | record           | R       | R        | R     | O     | R      | R     |
// | conversation     | R       | R        | R     | R     | R      | R     |
// | source           | R       | R        | R     | R     | R      | R     |
// | target           | O       | O        | O     | O     | O      | O     |
// | cause / cause_at | O       | O        | O     | O     | O      | O     |
// | correlation      | R       | R        | O     | R     | O (R task) | R |
// | channel          | X       | X        | X     | R     | X      | O     |
// | sequence         | X       | X        | X     | R     | X      | O (with channel) |
// | last             | X       | X        | X     | O     | O      | X (always terminal) |
// | finish_reason    | X       | O        | X     | O (with last) | X | X  |
// | idempotency_key  | O       | O        | O     | X     | X      | X     |
// | deadline_micros  | O       | X        | X     | O     | X      | X     |
// | task_state       | X       | O        | X     | X     | R (task) | O   |
// | operation        | O       | O        | O     | R seq 0 (chat|reasoning|tool_args), X after | R (task|card|progress) | O |
// | tool             | O       | O        | O     | O     | X      | O     |
// | usage            | X       | O        | O     | O (terminal) | O | O  |
// | metadata         | O       | O        | O     | O     | O      | O     |
// | body             | R       | R        | R     | R (may be empty with last) | O | R |
/// Check an envelope against the per-kind validity matrix and the caps.
pub fn validate(envelope: &AgentEnvelope) -> Result<(), ValidateError> {
    use AgentKind::*;
    let kind = envelope.kind;

    let require = |present: bool, field: &'static str| {
        if present {
            Ok(())
        } else {
            Err(ValidateError::Missing { kind, field })
        }
    };
    let forbid = |absent: bool, field: &'static str| {
        if absent {
            Ok(())
        } else {
            Err(ValidateError::Forbidden { kind, field })
        }
    };

    // record: required everywhere except chunk (where it is optional).
    if kind != Chunk {
        require(envelope.record.is_some(), "record")?;
    }

    // correlation.
    match kind {
        Command | Response | Chunk | Error => {
            require(envelope.correlation.is_some(), "correlation")?
        }
        Status => {
            if envelope.operation.as_deref() == Some(OPERATION_TASK) {
                require(envelope.correlation.is_some(), "correlation")?;
            }
        }
        Event => {}
    }

    // channel + sequence: the chunk identity, allowed on error as a stream
    // terminal, sequence only alongside channel.
    match kind {
        Chunk => {
            require(envelope.channel.is_some(), "channel")?;
            require(envelope.sequence.is_some(), "sequence")?;
        }
        Error => {
            if envelope.sequence.is_some() && envelope.channel.is_none() {
                return Err(ValidateError::Invalid {
                    field: "sequence",
                    reason: "sequence requires channel".to_owned(),
                });
            }
        }
        _ => {
            forbid(envelope.channel.is_none(), "channel")?;
            forbid(envelope.sequence.is_none(), "sequence")?;
        }
    }

    // last: chunk and status only (error is always terminal, so the flag
    // would be redundant noise there).
    if envelope.last && !matches!(kind, Chunk | Status) {
        return Err(ValidateError::Forbidden {
            kind,
            field: "last",
        });
    }

    // finish_reason: responses, and terminal chunks.
    match kind {
        Response => {}
        Chunk => {
            if envelope.finish_reason.is_some() && !envelope.last {
                return Err(ValidateError::Invalid {
                    field: "finish_reason",
                    reason: "finish_reason rides only the terminal chunk".to_owned(),
                });
            }
        }
        _ => forbid(envelope.finish_reason.is_none(), "finish_reason")?,
    }

    // idempotency_key: business idempotency for command/response/event.
    // Chunks and signals carry no dedup semantics.
    if matches!(kind, Chunk | Status | Error) {
        forbid(envelope.idempotency_key.is_none(), "idempotency_key")?;
    }

    // deadline_micros: commands and stream-opening chunks (the abandonment
    // bound rides sequence 0, and a mid-stream deadline would be ambiguous).
    if matches!(kind, Response | Event | Status | Error) {
        forbid(envelope.deadline_micros.is_none(), "deadline_micros")?;
    }
    if kind == Chunk && envelope.deadline_micros.is_some() && envelope.sequence != Some(0) {
        return Err(ValidateError::Invalid {
            field: "deadline_micros",
            reason: "the stream bound rides the opening chunk (sequence 0)".to_owned(),
        });
    }

    // task_state.
    match kind {
        Status => {
            if envelope.operation.as_deref() == Some(OPERATION_TASK) {
                require(envelope.task_state.is_some(), "task_state")?;
            }
        }
        Response | Error => {}
        _ => forbid(envelope.task_state.is_none(), "task_state")?,
    }

    // operation: two CLOSED vocabularies (protocol machinery, version-gated
    // like AgentKind), open OTel values everywhere else. The status
    // discriminator must be task | card | progress, and the chunk-stream purpose
    // must be chat | reasoning | tool_args and rides ONLY the opening chunk
    // (sequence 0), where it is required.
    match kind {
        Status => {
            require(envelope.operation.is_some(), "operation")?;
            if let Some(operation) = envelope.operation.as_deref()
                && !matches!(
                    operation,
                    OPERATION_TASK | OPERATION_CARD | OPERATION_PROGRESS
                )
            {
                return Err(ValidateError::Invalid {
                    field: "operation",
                    reason: format!(
                        "status operation must be `{OPERATION_TASK}`, `{OPERATION_CARD}`, \
                         or `{OPERATION_PROGRESS}`, got `{operation}`"
                    ),
                });
            }
        }
        Chunk => {
            if envelope.sequence == Some(0) {
                require(envelope.operation.is_some(), "operation")?;
            }
            if let Some(operation) = envelope.operation.as_deref() {
                if envelope.sequence != Some(0) {
                    return Err(ValidateError::Invalid {
                        field: "operation",
                        reason: "the stream purpose rides the opening chunk (sequence 0)"
                            .to_owned(),
                    });
                }
                if !matches!(
                    operation,
                    OPERATION_CHAT | OPERATION_REASONING | OPERATION_TOOL_ARGS
                ) {
                    return Err(ValidateError::Invalid {
                        field: "operation",
                        reason: format!(
                            "chunk-stream purpose must be `{OPERATION_CHAT}`, \
                             `{OPERATION_REASONING}`, or `{OPERATION_TOOL_ARGS}`, \
                             got `{operation}`"
                        ),
                    });
                }
            }
        }
        Command | Response | Event | Error => {}
    }

    // tool: meaningless on status signals.
    if kind == Status {
        forbid(envelope.tool.is_none(), "tool")?;
    }

    // usage: terminal-chunk accounting, never on commands.
    match kind {
        Command => forbid(envelope.usage.is_none(), "usage")?,
        Chunk if envelope.usage.is_some() && !envelope.last => {
            return Err(ValidateError::Invalid {
                field: "usage",
                reason: "whole-stream accounting rides the terminal chunk".to_owned(),
            });
        }
        _ => {}
    }

    // body.
    match kind {
        Status => {}
        Chunk => {
            if envelope.body.is_empty() && !envelope.last {
                return Err(ValidateError::Missing {
                    kind,
                    field: "body",
                });
            }
        }
        _ => require(!envelope.body.is_empty(), "body")?,
    }

    // Caps. The metadata caps are the load-bearing ones: that field is
    // bridge-injected and foreign-influenced, so a hostile edge gets a
    // publish-time rejection instead of inflating every record on a topic.
    cap_str(envelope.operation.as_deref(), "operation")?;
    cap_str(envelope.tool.as_deref(), "tool")?;
    cap_str(envelope.finish_reason.as_deref(), "finish_reason")?;
    if let Some(metadata) = &envelope.metadata {
        if metadata.len() > crate::limits::MAX_METADATA_ENTRIES {
            return Err(ValidateError::TooLarge {
                field: "metadata",
                size: metadata.len(),
                cap: crate::limits::MAX_METADATA_ENTRIES,
            });
        }
        let mut total = 0usize;
        for (key, value) in metadata {
            if key.len() > crate::limits::MAX_METADATA_KEY_BYTES {
                return Err(ValidateError::TooLarge {
                    field: "metadata key",
                    size: key.len(),
                    cap: crate::limits::MAX_METADATA_KEY_BYTES,
                });
            }
            let value_size = value_size(value);
            if value_size > crate::limits::MAX_METADATA_VALUE_BYTES {
                return Err(ValidateError::TooLarge {
                    field: "metadata value",
                    size: value_size,
                    cap: crate::limits::MAX_METADATA_VALUE_BYTES,
                });
            }
            total += key.len() + value_size;
        }
        if total > crate::limits::MAX_METADATA_TOTAL_BYTES {
            return Err(ValidateError::TooLarge {
                field: "metadata",
                size: total,
                cap: crate::limits::MAX_METADATA_TOTAL_BYTES,
            });
        }
    }
    Ok(())
}

/// The `status` operation value for task lifecycle updates.
pub const OPERATION_TASK: &str = "task";
/// The `status` operation value for liveness/capability cards.
pub const OPERATION_CARD: &str = "card";
/// The `status` operation value for progress ticks.
pub const OPERATION_PROGRESS: &str = "progress";

// The chunk-stream purpose vocabulary: `operation` on the stream-opening
// chunk says what the channel IS, so a consumer reassembling several channels
// under one correlation (answer text next to reasoning next to streamed tool
// arguments) tells them apart without decoding bodies. Pinned here because
// every bridge and viewer would otherwise invent its own spelling.
/// The chunk-stream `operation` value for answer/content text (OTel's `chat`).
pub const OPERATION_CHAT: &str = "chat";
/// The chunk-stream `operation` value for a model's reasoning stream.
pub const OPERATION_REASONING: &str = "reasoning";
/// The chunk-stream `operation` value for streamed tool-call arguments.
pub const OPERATION_TOOL_ARGS: &str = "tool_args";

// The state-sync convention: UI/shared state rides `event` envelopes
// discriminated by `operation`, never a new kind. Replaying snapshot + deltas
// reconstructs the state at any historical offset.
/// The `event` operation value for a full state snapshot (the body is the
/// state, codec per `agdx.ct`).
pub const OPERATION_STATE_SNAPSHOT: &str = "state_snapshot";
/// The `event` operation value for a state delta (the body is an RFC 6902
/// JSON Patch document).
pub const OPERATION_STATE_DELTA: &str = "state_delta";

// Pinned AGDX-native metadata keys. Values stay strings or scalars per the
// metadata rules. The keys are pinned so transcripts, bridges, and projections
// agree without per-app conventions.
/// Metadata key: the message's chat role. Recommended values: `user`,
/// `assistant`, `system`, `tool`. A string because that vocabulary belongs to
/// the model providers and the edge protocols, not to us.
pub const METADATA_ROLE: &str = "role";
/// Metadata key: the bridge hop list, a `Value::List` of bridge id strings.
/// A bridge republishing a message appends its own id, and drops a message
/// whose hop list already contains it: the loop guard for multi-bridge
/// deployments (A2A in, AG-UI out, A2A out again). Bounded by the metadata
/// caps like every other entry.
pub const METADATA_BRIDGE_HOPS: &str = "bridge_hops";

// Crockford base32, the canonical display form of every u128 id (26
// characters, the same rendering ULIDs use). Hand-rolled so the crate stays
// dependency-free. Generation (entropy and clock) lives SDK-side.
const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

pub(crate) fn crockford_encode(value: u128) -> [u8; 26] {
    let mut out = [0u8; 26];
    let mut v = value;
    for slot in out.iter_mut().rev() {
        *slot = CROCKFORD[(v & 0x1f) as usize];
        v >>= 5;
    }
    out
}

pub(crate) fn crockford_decode(s: &str) -> Result<u128, IdParseError> {
    let bytes = s.as_bytes();
    if bytes.len() != 26 {
        return Err(IdParseError::Length { got: bytes.len() });
    }
    let mut value: u128 = 0;
    for (i, byte) in bytes.iter().enumerate() {
        let digit = CROCKFORD
            .iter()
            .position(|c| *c == byte.to_ascii_uppercase())
            .ok_or(IdParseError::Char(*byte as char))?;
        // 26 chars carry 130 bits. The top character may only use 3 of its 5
        // (the ULID overflow rule), so a first digit past 7 cannot fit u128.
        if i == 0 && digit > 7 {
            return Err(IdParseError::Overflow);
        }
        value = (value << 5) | digit as u128;
    }
    Ok(value)
}

fn cap_str(value: Option<&str>, field: &'static str) -> Result<(), ValidateError> {
    if let Some(value) = value
        && value.len() > crate::limits::MAX_AGENT_STRING_BYTES
    {
        return Err(ValidateError::TooLarge {
            field,
            size: value.len(),
            cap: crate::limits::MAX_AGENT_STRING_BYTES,
        });
    }
    Ok(())
}

// Approximate scalar size in bytes for the metadata caps: text by length,
// scalars by their widest encoding, lists by the sum of their elements.
fn value_size(value: &Value) -> usize {
    match value {
        Value::Str(s) => s.len(),
        Value::List(items) => items.iter().map(|item| 1 + value_size(item)).sum(),
        _ => 9,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_an_id_when_displayed_then_should_round_trip_through_crockford_base32() {
        let id = RecordId::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);
        let text = id.to_string();
        assert_eq!(text.len(), 26);
        assert_eq!(text.parse::<RecordId>().expect("parses"), id);
        // Lowercase parses too (Crockford is case-insensitive).
        assert_eq!(text.to_lowercase().parse::<RecordId>().expect("parses"), id);
        assert_eq!(
            RecordId::from_u128(0).to_string(),
            "00000000000000000000000000"
        );
        assert_eq!(
            RecordId::from_u128(u128::MAX).to_string(),
            "7ZZZZZZZZZZZZZZZZZZZZZZZZZ"
        );
    }

    #[test]
    fn given_invalid_id_strings_when_parsed_then_should_reject_with_the_right_error() {
        assert_eq!(
            "short".parse::<RecordId>(),
            Err(IdParseError::Length { got: 5 })
        );
        assert_eq!(
            "8ZZZZZZZZZZZZZZZZZZZZZZZZZ".parse::<RecordId>(),
            Err(IdParseError::Overflow)
        );
        assert!(matches!(
            "UUUUUUUUUUUUUUUUUUUUUUUUUU".parse::<RecordId>(),
            Err(IdParseError::Char('U'))
        ));
    }

    #[test]
    fn given_agent_id_strings_when_parsed_then_should_accept_printable_and_reject_control() {
        for s in ["planner", "planner@acme.example", "team/planner", "a:b"] {
            assert_eq!(
                s.parse::<AgentId>()
                    .expect("a printable agent id is valid")
                    .as_str(),
                s
            );
        }
        assert!("".parse::<AgentId>().is_err());
        assert!("bad\nid".parse::<AgentId>().is_err());
    }

    #[test]
    fn given_task_state_codes_when_mapped_then_should_match_the_pinned_dictionary() {
        let expected = [
            (TaskState::Submitted, 1u8),
            (TaskState::Working, 2),
            (TaskState::InputRequired, 3),
            (TaskState::Completed, 4),
            (TaskState::Canceled, 5),
            (TaskState::Failed, 6),
            (TaskState::Rejected, 7),
            (TaskState::AuthRequired, 8),
            (TaskState::Unknown, 9),
        ];
        for (state, code) in expected {
            assert_eq!(state.code(), code);
            assert_eq!(TaskState::from_code(code), state);
        }
        // An unknown code decodes and passes through as non-terminal.
        let future = TaskState::from_code(42);
        assert_eq!(future, TaskState::Unrecognized(42));
        assert_eq!(future.code(), 42);
        assert!(!future.is_terminal());
        assert!(TaskState::Completed.is_terminal());
        assert!(!TaskState::Working.is_terminal());
    }

    #[test]
    fn given_task_state_names_when_round_tripped_then_should_match_the_a2a_vocabulary() {
        assert_eq!(TaskState::InputRequired.to_string(), "input-required");
        assert_eq!(
            "auth-required".parse::<TaskState>().expect("parses"),
            TaskState::AuthRequired
        );
        assert!("nope".parse::<TaskState>().is_err());
    }

    #[test]
    fn given_error_and_dead_letter_codes_when_mapped_then_should_match_the_dictionaries() {
        assert_eq!(AgentErrorCode::InvalidRequest.code(), 1);
        assert_eq!(AgentErrorCode::Internal.code(), 7);
        assert_eq!(
            AgentErrorCode::from_code(99),
            AgentErrorCode::Unrecognized(99)
        );
        assert_eq!(DeadLetterReason::RetryExhausted.code(), 1);
        assert_eq!(DeadLetterReason::DeadlineExceeded.code(), 4);
        assert_eq!(
            DeadLetterReason::from_code(77),
            DeadLetterReason::Unrecognized(77)
        );
    }

    #[test]
    fn given_every_u8_when_mapped_through_the_dictionaries_then_the_code_should_round_trip() {
        // Totality: from_code is defined on the whole u8 space (unknown codes
        // become Unrecognized), and code() is its left inverse, so no byte can
        // panic or be lost on decode.
        for code in 0u8..=u8::MAX {
            assert_eq!(TaskState::from_code(code).code(), code);
            assert_eq!(AgentErrorCode::from_code(code).code(), code);
            assert_eq!(DeadLetterReason::from_code(code).code(), code);
        }
    }

    #[test]
    fn given_an_idempotency_key_when_validated_then_should_enforce_the_cap() {
        assert!("order-123-attempt-2".parse::<IdempotencyKey>().is_ok());
        assert!("".parse::<IdempotencyKey>().is_err());
        assert!("x".repeat(65).parse::<IdempotencyKey>().is_err());
    }

    #[test]
    fn given_a_command_when_validated_then_should_pass_and_enforce_the_matrix() {
        let (record, conversation, source, correlation) = ids();
        let command =
            AgentEnvelope::command(record, conversation, source, correlation, b"do".to_vec());
        validate(&command).expect("a well-formed command validates");

        // A command without correlation is an event wearing the wrong kind.
        let mut missing = command.clone();
        missing.correlation = None;
        assert_eq!(
            validate(&missing),
            Err(ValidateError::Missing {
                kind: AgentKind::Command,
                field: "correlation"
            })
        );

        // usage is X on commands (accounting rides replies and terminals).
        let with_usage = command.clone().with_usage(TokenUsage::default());
        assert!(matches!(
            validate(&with_usage),
            Err(ValidateError::Forbidden { field: "usage", .. })
        ));

        // channel is chunk identity, invalid elsewhere.
        let mut with_channel = command;
        with_channel.channel = Some(ChannelId::from_u128(1));
        assert!(matches!(
            validate(&with_channel),
            Err(ValidateError::Forbidden {
                field: "channel",
                ..
            })
        ));
    }

    #[test]
    fn given_chunks_when_validated_then_should_enforce_stream_semantics() {
        let (_, conversation, source, correlation) = ids();
        let channel = ChannelId::from_u128(23);
        let chunk = AgentEnvelope::chunk(
            conversation,
            source.clone(),
            correlation,
            channel,
            0,
            b"tok".to_vec(),
        )
        .with_operation(OPERATION_CHAT);
        validate(&chunk).expect("a stream chunk validates");

        // The opening chunk REQUIRES its purpose, from the pinned vocabulary.
        let mut undeclared = chunk.clone();
        undeclared.operation = None;
        assert!(matches!(
            validate(&undeclared),
            Err(ValidateError::Missing {
                field: "operation",
                ..
            })
        ));
        let mut off_vocabulary = chunk.clone();
        off_vocabulary.operation = Some("telemetry".to_owned());
        assert!(matches!(
            validate(&off_vocabulary),
            Err(ValidateError::Invalid {
                field: "operation",
                ..
            })
        ));

        // The purpose rides ONLY the opening chunk.
        let redeclared = AgentEnvelope::chunk(
            conversation,
            source.clone(),
            correlation,
            channel,
            3,
            b"tok".to_vec(),
        )
        .with_operation(OPERATION_REASONING);
        assert!(matches!(
            validate(&redeclared),
            Err(ValidateError::Invalid {
                field: "operation",
                ..
            })
        ));

        // The terminal chunk may be empty and carries finish_reason + usage.
        let terminal = AgentEnvelope::chunk(
            conversation,
            source.clone(),
            correlation,
            channel,
            41,
            Vec::new(),
        )
        .terminal("stop")
        .with_usage(TokenUsage {
            input_tokens: 100,
            output_tokens: 42,
            ..Default::default()
        });
        validate(&terminal).expect("a terminal chunk validates");

        // A non-terminal chunk cannot carry finish_reason or usage.
        let mut early_finish = chunk.clone();
        early_finish.finish_reason = Some("stop".to_owned());
        assert!(matches!(
            validate(&early_finish),
            Err(ValidateError::Invalid {
                field: "finish_reason",
                ..
            })
        ));

        // An empty non-terminal chunk carries nothing.
        let empty = AgentEnvelope::chunk(
            conversation,
            source.clone(),
            correlation,
            channel,
            1,
            Vec::new(),
        );
        assert!(matches!(
            validate(&empty),
            Err(ValidateError::Missing { field: "body", .. })
        ));

        // Chunks carry no dedup semantics.
        let mut keyed = chunk.clone();
        keyed.idempotency_key = Some("k".parse().expect("valid key"));
        assert!(matches!(
            validate(&keyed),
            Err(ValidateError::Forbidden {
                field: "idempotency_key",
                ..
            })
        ));

        // The abandonment bound rides only the opening chunk (sequence 0).
        let opening = chunk.with_deadline_micros(1);
        validate(&opening).expect("an opening chunk may declare the bound");
        let late = AgentEnvelope::chunk(
            conversation,
            source.clone(),
            correlation,
            channel,
            5,
            b"tok".to_vec(),
        )
        .with_deadline_micros(1);
        assert!(matches!(
            validate(&late),
            Err(ValidateError::Invalid {
                field: "deadline_micros",
                ..
            })
        ));
    }

    #[test]
    fn given_status_signals_when_validated_then_task_updates_should_require_state() {
        let (record, conversation, source, correlation) = ids();
        // A liveness card: no correlation, no task_state.
        let card = AgentEnvelope::status(record, conversation, source.clone(), OPERATION_CARD);
        validate(&card).expect("a card validates");

        // A task update requires correlation + task_state.
        let bare_task = AgentEnvelope::status(record, conversation, source.clone(), OPERATION_TASK);
        assert!(matches!(
            validate(&bare_task),
            Err(ValidateError::Missing {
                field: "correlation",
                ..
            })
        ));
        let task = AgentEnvelope::status(record, conversation, source.clone(), OPERATION_TASK)
            .with_correlation(correlation)
            .with_task_state(TaskState::Working);
        validate(&task).expect("a task update validates");

        // The discriminator is a CLOSED vocabulary: task | card | progress.
        let off_vocabulary =
            AgentEnvelope::status(record, conversation, source.clone(), "telemetry");
        assert!(matches!(
            validate(&off_vocabulary),
            Err(ValidateError::Invalid {
                field: "operation",
                ..
            })
        ));
    }

    #[test]
    fn given_an_error_when_validated_then_last_should_be_forbidden() {
        let (record, conversation, source, correlation) = ids();
        let error =
            AgentEnvelope::error(record, conversation, source, correlation, b"boom".to_vec());
        validate(&error).expect("an error validates");

        // error is ALWAYS terminal, so the flag would be redundant.
        let mut flagged = error.clone();
        flagged.last = true;
        assert!(matches!(
            validate(&flagged),
            Err(ValidateError::Forbidden { field: "last", .. })
        ));

        // sequence without channel is incoherent.
        let mut dangling = error;
        dangling.sequence = Some(3);
        assert!(matches!(
            validate(&dangling),
            Err(ValidateError::Invalid {
                field: "sequence",
                ..
            })
        ));
    }

    #[test]
    fn given_an_agent_card_when_validated_then_should_enforce_the_caps() {
        let card = AgentCard {
            name: Some("trip-planner".to_owned()),
            version: Some("1.4.2".to_owned()),
            capabilities: vec!["chat".to_owned(), "search_flights".to_owned()],
            ttl_micros: Some(30_000_000),
        };
        card.validate().expect("a well-formed card validates");

        let mut crowded = card.clone();
        crowded.capabilities = vec!["x".to_owned(); crate::limits::MAX_CARD_CAPABILITIES + 1];
        assert!(matches!(
            crowded.validate(),
            Err(ValidateError::TooLarge {
                field: "capabilities",
                ..
            })
        ));

        let mut oversized = card;
        oversized.name = Some("n".repeat(crate::limits::MAX_AGENT_STRING_BYTES + 1));
        assert!(matches!(
            oversized.validate(),
            Err(ValidateError::TooLarge { field: "name", .. })
        ));
    }

    #[test]
    fn given_a_signature_when_validated_then_should_enforce_per_scheme_lengths() {
        let valid = Signature {
            scheme: SIGNATURE_SCHEME_ED25519,
            key_id: vec![1u8; 8],
            bytes: vec![2u8; 64],
        };
        valid
            .validate()
            .expect("a well-formed Ed25519 signature validates");

        let mut short_key = valid.clone();
        short_key.key_id = vec![1u8; 4];
        assert!(matches!(
            short_key.validate(),
            Err(ValidateError::Invalid {
                field: "key_id",
                ..
            })
        ));

        let mut short_signature = valid.clone();
        short_signature.bytes = vec![2u8; 32];
        assert!(matches!(
            short_signature.validate(),
            Err(ValidateError::Invalid { field: "bytes", .. })
        ));

        // Unknown scheme codes pass through (the open-dictionary rule).
        // Verification is SDK-side and scheme-aware.
        let future = Signature {
            scheme: 42,
            key_id: vec![1u8; 3],
            bytes: vec![2u8; 99],
        };
        future.validate().expect("an unknown scheme flows through");
    }

    #[test]
    fn given_a_body_ref_when_validated_then_should_enforce_reference_and_digest() {
        let valid = BodyRef::new("s3://transcripts/conv-1/msg-9", 4_194_304, [7u8; 32]);
        valid.validate().expect("a well-formed reference validates");

        let mut empty = valid.clone();
        empty.reference = String::new();
        assert!(matches!(
            empty.validate(),
            Err(ValidateError::Invalid {
                field: "reference",
                ..
            })
        ));

        let mut oversized = valid.clone();
        oversized.reference = "x".repeat(crate::limits::MAX_BODY_REFERENCE_BYTES + 1);
        assert!(matches!(
            oversized.validate(),
            Err(ValidateError::TooLarge {
                field: "reference",
                ..
            })
        ));

        let mut truncated = valid;
        truncated.sha256 = vec![7u8; 16];
        assert!(matches!(
            truncated.validate(),
            Err(ValidateError::Invalid {
                field: "sha256",
                ..
            })
        ));
    }

    #[test]
    fn given_oversized_metadata_when_validated_then_should_reject() {
        let (record, conversation, source, correlation) = ids();
        let mut command =
            AgentEnvelope::command(record, conversation, source, correlation, b"x".to_vec());
        for i in 0..crate::limits::MAX_METADATA_ENTRIES + 1 {
            command = command.with_metadata(format!("k{i}"), i as i64);
        }
        assert!(matches!(
            validate(&command),
            Err(ValidateError::TooLarge {
                field: "metadata",
                ..
            })
        ));
    }

    fn ids() -> (RecordId, ConversationId, AgentId, CorrelationId) {
        (
            RecordId::from_u128(7),
            ConversationId::from_u128(11),
            "test-agent".parse().expect("valid agent id"),
            CorrelationId::from_u128(17),
        )
    }
}

#[cfg(all(test, feature = "cbor"))]
mod wire_tests {
    use super::*;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_a_wrong_length_locator_when_decoded_then_should_error_not_panic() {
        // CBOR byte string: 0x40 | len for lengths below 24. Exactly 20 bytes
        // is the only valid locator. Anything else is a clean decode error.
        let mut valid = vec![0x40 | 20];
        valid.extend_from_slice(&[0u8; 20]);
        decode_named::<LogPosition>(&valid).expect("20 packed bytes decode");

        for bad_len in [0u8, 19, 21, 23] {
            let mut bytes = vec![0x40 | bad_len];
            bytes.extend_from_slice(&vec![0u8; bad_len as usize]);
            assert!(
                decode_named::<LogPosition>(&bytes).is_err(),
                "a {bad_len}-byte locator must error, not panic"
            );
        }
    }

    #[test]
    fn given_a_locator_when_round_tripped_then_should_preserve_every_field() {
        let pos = LogPosition::new(0x0102_0304, 0x0506_0708, 0x090A_0B0C, 0x0D0E_0F10_1112_1314);
        let bytes = encode_named(&pos).expect("encodes");
        let back: LogPosition = decode_named(&bytes).expect("decodes");
        assert_eq!(back, pos);
    }

    #[test]
    fn given_an_envelope_when_round_tripped_then_should_preserve_every_field() {
        let envelope = AgentEnvelope::command(
            RecordId::from_u128(1),
            ConversationId::from_u128(2),
            "source-agent".parse().expect("valid agent id"),
            CorrelationId::from_u128(4),
            b"payload".to_vec(),
        )
        .with_target("target-agent".parse().expect("valid agent id"))
        .with_cause(RecordId::from_u128(6), Some(LogPosition::new(1, 2, 3, 44)))
        .with_idempotency_key("order-1".parse().expect("valid key"))
        .with_deadline_micros(1_700_000_000_000_000)
        .with_operation("chat")
        .with_tool("search")
        .with_metadata("customer_tier", "gold");
        let bytes = encode_named(&envelope).expect("encodes");
        let back: AgentEnvelope = decode_named(&bytes).expect("decodes");
        assert_eq!(back, envelope);
    }

    #[test]
    fn given_a_must_understand_marker_when_round_tripped_then_should_preserve_bits_and_skip_zero() {
        // A receiver lacking a required bit must see it as unmet, and the
        // default-zero marker is omitted on the wire so pre-marker records stay
        // byte-identical.
        let envelope = AgentEnvelope::event(
            RecordId::from_u128(1),
            ConversationId::from_u128(2),
            "source-agent".parse().expect("valid agent id"),
            b"e".to_vec(),
        )
        .requiring(0b101);
        let bytes = encode_named(&envelope).expect("encodes");
        let back: AgentEnvelope = decode_named(&bytes).expect("decodes");
        assert_eq!(back.must_understand, 0b101);
        // A receiver that understands bit 0 but not bit 2 has bit 2 unmet.
        assert_eq!(back.unmet_requirements(0b001), 0b100);
        // A receiver that understands both has nothing unmet.
        assert_eq!(back.unmet_requirements(0b111), 0);
        // The zero marker (open-world default) is unmet by nobody and omitted.
        let plain = AgentEnvelope::event(
            RecordId::from_u128(1),
            ConversationId::from_u128(2),
            "source-agent".parse().expect("valid agent id"),
            b"e".to_vec(),
        );
        assert_eq!(plain.unmet_requirements(features::NONE), 0);
        let json = serde_json::to_string(&plain).expect("json");
        assert!(
            !json.contains("must_understand"),
            "zero marker must be omitted: {json}"
        );
    }

    #[test]
    fn given_absent_options_when_encoded_then_should_cost_zero_bytes() {
        // The skip-serializing discipline: a minimal event encodes only its
        // five present fields (kind, record, conversation, source, body).
        let envelope = AgentEnvelope::event(
            RecordId::from_u128(1),
            ConversationId::from_u128(2),
            "source-agent".parse().expect("valid agent id"),
            b"e".to_vec(),
        );
        let bytes = encode_named(&envelope).expect("encodes");
        // A CBOR map with at most 15 fields encodes its count in the head byte's
        // low nibble (major type 5, `0xa0 | count`).
        assert_eq!(bytes[0] & 0x0f, 5, "absent optionals must not be encoded");
        let back: AgentEnvelope = decode_named(&bytes).expect("decodes");
        assert!(!back.last);
        assert!(back.metadata.is_none());
    }

    #[test]
    fn given_an_id_when_encoded_then_should_ride_as_one_fixed_width_byte_string() {
        let bytes = encode_named(&RecordId::from_u128(0x0102)).expect("encodes");
        // CBOR byte-string head `0x50` (major type 2, length 16), then the 16
        // big-endian bytes - fixed width, no bignum tag. 17 bytes total.
        assert_eq!(bytes.len(), 17);
        assert_eq!(bytes[0], 0x50);
        assert_eq!(bytes[16], 0x02);
        assert_eq!(bytes[15], 0x01);
    }

    #[test]
    fn given_a_task_state_when_encoded_then_should_ride_as_a_bare_u8() {
        let bytes = encode_named(&TaskState::Completed).expect("encodes");
        assert_eq!(bytes, vec![4]);
        let unknown = encode_named(&42u8).expect("encodes a raw code");
        let back: TaskState = decode_named(&unknown).expect("unknown code decodes");
        assert_eq!(back, TaskState::Unrecognized(42));
    }

    #[test]
    fn given_an_error_body_when_round_tripped_then_should_preserve_the_dictionary_code() {
        let body = AgentErrorBody {
            code: AgentErrorCode::ToolFailure,
            message: Some("search timed out".to_owned()),
            retryable: true,
            detail: Some(BTreeMap::from([("attempt".to_owned(), Value::Int(3))])),
        };
        let bytes = encode_named(&body).expect("encodes");
        let back: AgentErrorBody = decode_named(&bytes).expect("decodes");
        assert_eq!(back, body);
    }

    #[test]
    fn given_a_body_ref_when_round_tripped_then_should_preserve_the_digest_as_a_byte_string() {
        let capsule = BodyRef::new("kv://bodies/abc", 1024, [9u8; 32]);
        let bytes = encode_named(&capsule).expect("encodes");
        let back: BodyRef = decode_named(&bytes).expect("decodes");
        assert_eq!(back, capsule);
        assert!(back.encryption.is_none(), "absent encryption costs nothing");
        back.validate().expect("decoded capsule validates");
    }

    #[test]
    fn given_a_dead_letter_when_round_tripped_then_payload_should_stay_byte_identical() {
        let inner = AgentEnvelope::command(
            RecordId::from_u128(9),
            ConversationId::from_u128(8),
            "source-agent".parse().expect("valid agent id"),
            CorrelationId::from_u128(6),
            b"poison".to_vec(),
        );
        let payload = encode_named(&inner).expect("inner encodes");
        let capsule = AgentDeadLetter {
            source: LogPosition::new(1, 2, 3, 99),
            reason: DeadLetterReason::RetryExhausted,
            attempts: 5,
            detail: Some("handler kept failing".to_owned()),
            payload: payload.clone(),
        };
        let bytes = encode_named(&capsule).expect("encodes");
        let back: AgentDeadLetter = decode_named(&bytes).expect("decodes");
        assert_eq!(back.payload, payload, "redrive needs the original bytes");
        let redrive: AgentEnvelope = decode_named(&back.payload).expect("inner decodes");
        assert_eq!(redrive, inner);
    }
}

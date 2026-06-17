use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use ulid::Ulid;

const MAX_ID_LEN: usize = 255;

// Versioned FNV-1a so derived ids stay stable across compiler/std versions.
// Bumping DERIVE_VERSION deliberately remaps every derived conversation id.
const DERIVE_VERSION: u8 = 1;
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Why parsing or validating an id failed (`ConversationId`, `AgentId`, `MessageId`).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    #[error("identifier must not be empty")]
    Empty,
    #[error("identifier length {got}B exceeds max {max}B")]
    TooLong { got: usize, max: usize },
    #[error("identifier contains invalid character `{0}`")]
    InvalidChar(char),
    #[error("invalid ULID `{0}`")]
    InvalidUlid(String),
    #[error("invalid message id `{0}`, expected `<partition_id>:<offset>`")]
    InvalidMessageId(String),
}

/// A conversation: the unit of ordering and causality. One conversation maps to
/// one Iggy partition, so all its messages share a total order. Created fresh with
/// [`new`](Self::new) or derived deterministically from a seed with
/// [`derive`](Self::derive).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConversationId(Ulid);

impl ConversationId {
    /// A fresh, random conversation id (a time-ordered ULID).
    pub fn new() -> Self {
        Self(Ulid::new())
    }

    /// Derives a stable conversation id from a seed (e.g. a user identity).
    /// The same seed always yields the same id, giving per-seed ordering and
    /// isolation without coordination. Used by `SessionPolicy::PerUser`.
    pub fn derive(seed: &str) -> Self {
        let high = hash_with(0x1d, seed);
        let low = hash_with(0x9e, seed);
        Self(Ulid((u128::from(high) << 64) | u128::from(low)))
    }
}

fn hash_with(salt: u8, seed: &str) -> u64 {
    let mut hash = FNV_OFFSET;
    for byte in [DERIVE_VERSION, salt].into_iter().chain(seed.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

impl Default for ConversationId {
    fn default() -> Self {
        Self::new()
    }
}

impl Display for ConversationId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl FromStr for ConversationId {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ulid::from_string(s)
            .map(Self)
            .map_err(|_| IdError::InvalidUlid(s.to_owned()))
    }
}

/// An agent's stable name. Almost any string: the only rules are that it is
/// non-empty, at most 255 bytes (the Iggy consumer-group-name limit, since one
/// agent is one consumer group), and free of ASCII control characters (it also
/// rides as a message header value). So plain labels (`planner`), email-like
/// federated identities (`planner@acme.example`), URLs, and namespaced names
/// (`team/planner`) are all valid. Construct with [`new`](Self::new), or by
/// parsing (`"planner".parse()?` / `TryFrom`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AgentId(String);

impl AgentId {
    /// Build an agent id from any string that satisfies the rules (non-empty,
    /// ≤ 255 bytes, no ASCII control characters), or an [`IdError`] saying which
    /// rule it broke.
    pub fn new(name: impl Into<String>) -> Result<Self, IdError> {
        let name = name.into();
        validate_id(&name)?;
        Ok(Self(name))
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for AgentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for AgentId {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<&str> for AgentId {
    type Error = IdError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

impl TryFrom<String> for AgentId {
    type Error = IdError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::new(s)
    }
}

/// A message's position on the log: its partition and offset. Stamped on a reply
/// as the `causal_parent` so a flow's causality is walkable. Displays as
/// `<partition_id>:<offset>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MessageId {
    /// The Iggy partition the message lives on.
    pub partition_id: u32,
    /// The message's offset within that partition.
    pub offset: u64,
}

impl MessageId {
    /// A message id at `(partition_id, offset)`.
    pub fn new(partition_id: u32, offset: u64) -> Self {
        Self {
            partition_id,
            offset,
        }
    }
}

impl Display for MessageId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.partition_id, self.offset)
    }
}

impl FromStr for MessageId {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (partition, offset) = s
            .split_once(':')
            .ok_or_else(|| IdError::InvalidMessageId(s.to_owned()))?;
        // Reject signs, whitespace, leading zeros, anything that survives
        // FromStr<u32>/<u64> but breaks the round-trip with `Display`. We
        // do the check at byte level (no allocation) so a high-traffic
        // request/reply loop does not pay a `String` per parse.
        if !is_canonical_digits(partition) || !is_canonical_digits(offset) {
            return Err(IdError::InvalidMessageId(s.to_owned()));
        }
        Ok(Self {
            partition_id: partition
                .parse()
                .map_err(|_| IdError::InvalidMessageId(s.to_owned()))?,
            offset: offset
                .parse()
                .map_err(|_| IdError::InvalidMessageId(s.to_owned()))?,
        })
    }
}

// Canonical: non-empty, all ASCII digits, no leading zero except for "0"
// itself. This is exactly what `format!("{}", u32_or_u64)` produces, so the
// invariant equals `display(parse(s)) == s` without allocating to check.
fn is_canonical_digits(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    if bytes.len() > 1 && bytes[0] == b'0' {
        return false;
    }
    bytes.iter().all(u8::is_ascii_digit)
}

fn validate_id(s: &str) -> Result<(), IdError> {
    if s.is_empty() {
        return Err(IdError::Empty);
    }
    if s.len() > MAX_ID_LEN {
        return Err(IdError::TooLong {
            got: s.len(),
            max: MAX_ID_LEN,
        });
    }
    // The only character rule is no ASCII control characters: an agent id is an
    // Iggy consumer-group name (which permits any non-empty byte string up to
    // 255 bytes) and a message header value (which rejects control chars). Every
    // printable character is allowed, including `:`, `@`, `+`, `/`, and spaces.
    if let Some(c) = s.chars().find(|c| c.is_control()) {
        return Err(IdError::InvalidChar(c));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_conversation_id_when_round_tripped_through_a_string_then_should_be_equal() {
        let id = ConversationId::new();
        let parsed = id
            .to_string()
            .parse::<ConversationId>()
            .expect("a formatted conversation id should parse");
        assert_eq!(parsed, id);
    }

    #[test]
    fn given_seeds_when_deriving_conversation_ids_then_should_be_stable_and_distinct() {
        assert_eq!(
            ConversationId::derive("user-1"),
            ConversationId::derive("user-1")
        );
        assert_ne!(
            ConversationId::derive("user-1"),
            ConversationId::derive("user-2")
        );
    }

    // Pins the derivation to a golden value. A change here means existing
    // `SessionPolicy::PerUser` conversations would be remapped: bump
    // `DERIVE_VERSION` deliberately rather than silently editing this constant.
    #[test]
    fn given_a_known_seed_when_derived_then_should_match_the_pinned_id() {
        assert_eq!(
            ConversationId::derive("user-1").to_string(),
            "6X4VM88293CP9BFK3H58TFMMS7"
        );
    }

    #[test]
    fn given_an_invalid_string_when_parsing_a_conversation_id_then_should_error() {
        assert!(matches!(
            "not-a-ulid".parse::<ConversationId>(),
            Err(IdError::InvalidUlid(_))
        ));
    }

    #[test]
    fn given_agent_id_strings_when_parsing_then_should_accept_valid_and_reject_invalid() {
        let valid = "executor-v1"
            .parse::<AgentId>()
            .expect("a valid agent id should parse");
        assert_eq!(valid.as_str(), "executor-v1");
        assert_eq!("".parse::<AgentId>(), Err(IdError::Empty));
        // Almost any printable string is a valid agent id: spaces, `:`, `@`, `+`,
        // `/`, email-like federated identities, and namespaced names all parse.
        for s in [
            "bad id",
            "planner+eu@acme.example",
            "team/planner",
            "a:b",
            "https://acme.example/agents/planner",
        ] {
            assert_eq!(
                s.parse::<AgentId>()
                    .expect("a printable agent id is valid")
                    .as_str(),
                s
            );
        }
        // Only ASCII control characters are rejected.
        assert_eq!(
            "bad\nid".parse::<AgentId>(),
            Err(IdError::InvalidChar('\n'))
        );
    }

    #[test]
    fn given_a_message_id_when_round_tripped_then_should_be_equal() {
        let id = MessageId::new(3, 42);
        assert_eq!(id.to_string(), "3:42");
        assert_eq!(
            "3:42"
                .parse::<MessageId>()
                .expect("a formatted message id should parse"),
            id
        );
        assert!(matches!(
            "nope".parse::<MessageId>(),
            Err(IdError::InvalidMessageId(_))
        ));
    }

    #[test]
    fn given_non_canonical_message_id_strings_when_parsed_then_should_reject() {
        for bad in [
            "+3:42", " 3:42", "3:42 ", "03:42", "3:042", "3:", ":42", "::", "3", "",
        ] {
            assert!(
                bad.parse::<MessageId>().is_err(),
                "expected `{bad}` to be rejected by MessageId::from_str",
            );
        }
    }

    #[test]
    fn given_a_message_id_shaped_string_when_used_as_agent_id_then_should_accept() {
        let id = AgentId::new("3:42").expect("`3:42` is a valid agent id");
        assert_eq!(id.as_str(), "3:42");
    }

    #[test]
    fn given_a_control_character_when_constructing_agent_id_then_should_reject() {
        assert_eq!(AgentId::new("a\tb"), Err(IdError::InvalidChar('\t')));
        assert_eq!(AgentId::new(""), Err(IdError::Empty));
    }
}

// The Agent Data Exchange Protocol (AGDX) id bridge. The wire crate's agent ids are plain
// u128 newtypes with no clock or entropy (it must stay runtime-free and
// wasm-portable). GENERATION lives here, where the ulid crate already is.

/// Mint a fresh ULID-valued wire id (time-ordered, human-readable). The
/// trait exists because the wire crate deliberately cannot generate ids: it
/// has no clock and no randomness.
pub trait MintUlid: Sized + From<u128> {
    /// A fresh ULID as this id type.
    fn mint() -> Self {
        Self::from(Ulid::new().0)
    }
}

impl MintUlid for laser_wire::agent::RecordId {}
impl MintUlid for laser_wire::agent::ConversationId {}
impl MintUlid for laser_wire::agent::CorrelationId {}
impl MintUlid for laser_wire::agent::ChannelId {}

impl ConversationId {
    /// The raw 128-bit ULID value.
    pub fn as_u128(&self) -> u128 {
        self.0.0
    }
}

impl From<ConversationId> for laser_wire::agent::ConversationId {
    fn from(id: ConversationId) -> Self {
        Self::from_u128(id.as_u128())
    }
}

impl From<laser_wire::agent::ConversationId> for ConversationId {
    fn from(id: laser_wire::agent::ConversationId) -> Self {
        Self(Ulid(id.as_u128()))
    }
}

impl AgentId {
    /// This agent's identity on the wire: the same name string. Wire and SDK
    /// agent ids are both the name now, so this only re-checks the wire cap
    /// (the SDK cap is the tighter of the two, so a valid SDK id always fits).
    pub fn wire_id(&self) -> laser_wire::agent::AgentId {
        laser_wire::agent::AgentId::from_str(&self.0)
            .expect("a valid SDK agent id is a valid wire agent id")
    }
}

#[cfg(test)]
mod agdx_bridge_tests {
    use super::*;

    #[test]
    fn given_a_conversation_id_when_bridged_to_the_wire_id_then_should_round_trip() {
        let id = ConversationId::new();
        let wire: laser_wire::agent::ConversationId = id.into();
        let back: ConversationId = wire.into();
        assert_eq!(back, id);
        // Same canonical Crockford rendering on both sides.
        assert_eq!(wire.to_string(), id.to_string());
    }

    #[test]
    fn given_an_agent_name_when_derived_then_wire_id_should_be_stable_and_distinct() {
        let planner: AgentId = "planner".parse().expect("valid agent id");
        let executor: AgentId = "executor".parse().expect("valid agent id");
        assert_eq!(planner.wire_id(), planner.wire_id());
        assert_ne!(planner.wire_id(), executor.wire_id());
    }

    // The wire id is the name verbatim now (no derivation), so it round-trips
    // as the readable string rather than an opaque code.
    #[test]
    fn given_an_agent_name_when_taken_to_the_wire_then_should_be_the_same_string() {
        let planner: AgentId = "planner".parse().expect("valid agent id");
        assert_eq!(planner.wire_id().to_string(), "planner");
    }

    #[test]
    fn given_minted_wire_ids_when_compared_then_should_be_distinct() {
        use laser_wire::agent::RecordId;
        assert_ne!(RecordId::mint(), RecordId::mint());
    }
}

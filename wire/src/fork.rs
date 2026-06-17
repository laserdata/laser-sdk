use crate::error::InvalidError;
use crate::limits::MAX_FORK_ID_BYTES;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// How a fork relates to the trunk it branched from.
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
#[non_exhaustive]
pub enum ForkKind {
    /// Frozen snapshot: sees trunk rows only up to the offsets captured at
    /// creation, plus the fork's own rows. Later trunk appends are invisible.
    Severed,
    /// Live branch: sees the trunk as it grows, plus the fork's own rows overlaid.
    #[default]
    Continuous,
}

/// Lifecycle of a fork.
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
#[non_exhaustive]
pub enum ForkStatus {
    #[default]
    Open,
    Promoted,
    Squashed,
}

/// A fork's metadata, returned by `create` and `list`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForkInfo {
    pub fork_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub kind: ForkKind,
    pub user_id: u32,
    pub status: ForkStatus,
    pub created_at_micros: u64,
    pub row_count: usize,
}

/// Wire form of the `AGDX_FORK_CREATE` request. Wire-stability-bound, built
/// through the SDK's fork handle in application code.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkCreate {
    pub v: u32,
    pub fork_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(default)]
    pub kind: ForkKind,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<String>,
}

/// Wire form of the `AGDX_FORK_DELETE` (squash) request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkDelete {
    pub v: u32,
    pub fork_id: String,
}

/// Wire form of the `AGDX_FORK_PROMOTE` request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkPromote {
    pub v: u32,
    pub fork_id: String,
}

/// Wire form of the `AGDX_FORK_LIST` request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkList {
    pub v: u32,
}

/// Wire form of the `AGDX_FORK_PUT` request. Wire-stability-bound, built through
/// the SDK's fork handle in application code.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ForkPut {
    pub v: u32,
    pub fork_id: String,
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
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub payload: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding: Option<String>,
    #[serde(default)]
    pub tombstone: bool,
}

/// The result of a fork op: `Ok` with the outcome, or `Err` with a failure.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ForkReply {
    Ok(ForkOutcome),
    Err(ForkError),
}

/// The successful outcome of a fork command, shaped per op.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ForkOutcome {
    Created(ForkInfo),
    Deleted(bool),
    Promoted { rows: usize },
    List(Vec<ForkInfo>),
    Written,
}

/// Why a fork operation failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[non_exhaustive]
pub enum ForkError {
    #[error("forks not supported: {0}")]
    Unsupported(String),
    #[error("fork not found: {0}")]
    NotFound(String),
    #[error("invalid fork: {0}")]
    InvalidFork(String),
    #[error("fork conflict: {0}")]
    Conflict(String),
    #[error("fork backend error: {0}")]
    Backend(String),
    #[error("unsupported fork op version (expected {expected}, got {got})")]
    Version { expected: u32, got: u32 },
}

/// The canonical fork-id safelist, shared by every fork-serving backend. A
/// fork id is caller-chosen and a backend that overlays a fork inlines it into
/// a copy-on-write query as a quoted identifier, so the charset must be a
/// strict safelist, not just a length bound: this is the one anti-injection
/// rule. A valid id is non-empty, at most [`MAX_FORK_ID_BYTES`] bytes, and made
/// only of ASCII letters, digits, `-`, `_`, and `.`. A backend that binds the
/// id as a parameter may skip the call, but one that inlines it gets the defense for
/// free by calling this before use.
pub fn validate_fork_id(fork_id: &str) -> Result<(), InvalidError> {
    if fork_id.is_empty() {
        return Err(InvalidError::new("fork id must not be empty"));
    }
    if fork_id.len() > MAX_FORK_ID_BYTES {
        return Err(InvalidError::new(format!(
            "fork id is {}B, exceeds cap {MAX_FORK_ID_BYTES}B",
            fork_id.len()
        )));
    }
    if let Some(bad) = fork_id
        .bytes()
        .find(|byte| !matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.'))
    {
        return Err(InvalidError::new(format!(
            "fork id has a disallowed byte {bad:#04x}: allowed are ASCII letters, digits, '-', '_', '.'"
        )));
    }
    Ok(())
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::codes::FORK_OP_VERSION;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_fork_ids_when_validated_then_should_enforce_charset_and_length() {
        assert!(validate_fork_id("experiment-2026-q2").is_ok());
        assert!(validate_fork_id("run_7.v2").is_ok());
        assert!(validate_fork_id("").is_err(), "empty");
        assert!(validate_fork_id("bad id").is_err(), "space");
        assert!(
            validate_fork_id("o'brien; drop table").is_err(),
            "sql metachars rejected"
        );
        assert!(
            validate_fork_id("name/../etc").is_err(),
            "slash and dot-dot"
        );
        assert!(validate_fork_id(&"f".repeat(MAX_FORK_ID_BYTES)).is_ok());
        assert!(validate_fork_id(&"f".repeat(MAX_FORK_ID_BYTES + 1)).is_err());
    }

    #[test]
    fn given_fork_create_when_round_tripped_then_should_preserve_kind() {
        let request = ForkCreate {
            v: FORK_OP_VERSION,
            fork_id: "agent-run-7".to_owned(),
            parent: None,
            kind: ForkKind::Severed,
            tables: vec!["orders".to_owned()],
        };
        let bytes = encode_named(&request).expect("serializes");
        let back: ForkCreate = decode_named(&bytes).expect("deserializes");
        assert_eq!(back.fork_id, "agent-run-7");
        assert_eq!(back.kind, ForkKind::Severed);
    }

    #[test]
    fn given_fork_reply_created_when_round_tripped_then_should_preserve_info() {
        let reply = ForkReply::Ok(ForkOutcome::Created(ForkInfo {
            fork_id: "f1".to_owned(),
            parent: None,
            kind: ForkKind::Continuous,
            user_id: 5,
            status: ForkStatus::Open,
            created_at_micros: 1,
            row_count: 0,
        }));
        let bytes = encode_named(&reply).expect("serializes");
        let back: ForkReply = decode_named(&bytes).expect("deserializes");
        let ForkReply::Ok(ForkOutcome::Created(info)) = back else {
            panic!("expected Created");
        };
        assert_eq!(info.user_id, 5);
        assert_eq!(info.kind, ForkKind::Continuous);
    }

    #[test]
    fn given_fork_kind_when_serialized_then_should_be_snake_case() {
        // Must match LaserData Cloud's `rename_all = "snake_case"` on the wire.
        assert_eq!(
            serde_json::to_string(&ForkKind::Severed).expect("serializes"),
            "\"severed\""
        );
        assert_eq!(
            serde_json::to_string(&ForkKind::Continuous).expect("serializes"),
            "\"continuous\""
        );
    }
}

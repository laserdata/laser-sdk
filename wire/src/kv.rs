use serde::{Deserialize, Serialize};

/// One stored entry: an arbitrary-bytes key and value plus optional expiry. The
/// key and value are owned `Vec<u8>`, so the public API never leaks the `bytes`
/// crate, and they ride the wire as CBOR byte strings, byte-exact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvEntry {
    #[serde(with = "crate::encoding::bin_bytes")]
    pub key: Vec<u8>,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub value: Vec<u8>,
    /// Absolute expiry in epoch microseconds, or `None` for no expiry. Expired
    /// entries are hidden on read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_micros: Option<u64>,
    /// Optimistic-concurrency version, assigned by the store and bumped on every
    /// successful mutation of this key. The token a [`KvCas`] precondition
    /// matches against. A versioned managed backend reports `>= 1` for a live
    /// entry and reserves `0` solely for "unversioned": a store that does not
    /// track versions, or an entry written before versioning. A caller must
    /// therefore never treat `0` as a valid compare token. The field is skipped
    /// on the wire when `0` so those entries stay byte-identical.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub version: u64,
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

impl KvEntry {
    /// The key decoded as UTF-8, or `None` when it is not valid UTF-8. Keys are
    /// arbitrary bytes, so a binary key has no string form.
    pub fn key_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.key).ok()
    }
}

/// A page of scanned entries plus the cursor to resume after the last one.
/// `cursor` is `None` when the scan reached the end.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct KvPage {
    pub entries: Vec<KvEntry>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub cursor: Option<Vec<u8>>,
}

/// Request to read the value at `key` in `namespace`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KvGet {
    pub v: u32,
    pub namespace: String,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub key: Vec<u8>,
}

/// Request to write `value` at `key` in `namespace`, with an optional expiry.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KvSet {
    pub v: u32,
    pub namespace: String,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub key: Vec<u8>,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub value: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_micros: Option<u64>,
}

/// The precondition a [`KvCas`] write must satisfy to apply. The compare half
/// of compare-and-swap: lock-free optimistic concurrency for callers contending
/// on one key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CasExpect {
    /// Apply only if the key currently holds this exact [`KvEntry::version`].
    Match(u64),
    /// Apply only if the key does not currently exist (a create-if-absent).
    Absent,
}

/// Compare-and-swap write at `key` in `namespace`: apply `value` (with optional
/// expiry) only if `expect` holds, else fail with
/// [`KvError::VersionConflict`]. The swap half of optimistic concurrency, paired
/// with [`KvEntry::version`] as the compare token.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KvCas {
    pub v: u32,
    pub namespace: String,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub key: Vec<u8>,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub value: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_micros: Option<u64>,
    pub expect: CasExpect,
}

/// Request to remove `key` from `namespace`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KvDelete {
    pub v: u32,
    pub namespace: String,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub key: Vec<u8>,
}

/// Request to list every namespace that holds at least one entry for the caller.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KvNamespaces {
    pub v: u32,
}

/// One namespace summary in a `Namespaces` reply.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KvNamespaceInfo {
    pub namespace: String,
    pub entries: usize,
}

/// Request to list entries in `namespace`. With no bounds it lists the whole
/// namespace. `prefix` matches keys that start with it in byte order. `start`
/// and `end` bound an inclusive-start, exclusive-end key range. `key_contains`
/// keeps only keys that are valid UTF-8 and contain the substring (binary keys
/// are skipped). All bounds compose.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KvScan {
    pub v: u32,
    pub namespace: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub prefix: Option<Vec<u8>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub start: Option<Vec<u8>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub end: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_contains: Option<String>,
    pub limit: usize,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub cursor: Option<Vec<u8>>,
}

/// Request to bulk-delete entries in `namespace` matching the same composed
/// bounds as a scan (`prefix`/`start`/`end`/`key_contains`). With no bounds it
/// clears the whole namespace. No `limit`/`cursor`, and expiry is ignored (a
/// matching expired entry is removed too).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KvDeleteMany {
    pub v: u32,
    pub namespace: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub prefix: Option<Vec<u8>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub start: Option<Vec<u8>>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::encoding::opt_bin_bytes"
    )]
    pub end: Option<Vec<u8>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_contains: Option<String>,
}

/// The result of a key-value operation: `Ok` with the operation's outcome, or
/// `Err` with a structured failure.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum KvReply {
    Ok(KvOutcome),
    Err(KvError),
}

/// The successful outcome of a KV command, shaped per op.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum KvOutcome {
    /// `get`: the live entry, or `None` when absent or expired.
    Value(Option<KvEntry>),
    /// `set`: the write was applied.
    Written,
    /// `cas`: the compare-and-swap applied. Carries the entry's new version, so
    /// the caller can chain a further conditional write without a re-read.
    Committed { version: u64 },
    /// `delete`: `true` when a live entry was removed, `false` when none existed.
    Deleted(bool),
    /// `delete_many`: the number of entries removed by a filtered bulk delete.
    DeletedMany(usize),
    /// `scan`: one page of entries.
    Page(KvPage),
    /// `namespaces`: every namespace holding at least one entry for the
    /// caller with its entry count, sorted by name.
    Namespaces(Vec<KvNamespaceInfo>),
}

/// Why a key-value operation failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[non_exhaustive]
pub enum KvError {
    #[error("kv not supported: {0}")]
    Unsupported(String),
    #[error("invalid key: {0}")]
    InvalidKey(String),
    #[error("{what} is {size}B, exceeds cap {cap}B")]
    TooLarge {
        what: String,
        size: usize,
        cap: usize,
    },
    #[error("kv backend error: {0}")]
    Backend(String),
    #[error("unsupported kv op version (expected {expected}, got {got})")]
    Version { expected: u32, got: u32 },
    /// A [`KvCas`] precondition was not met. `current` is the key's present
    /// version (`Some`) or `None` when the key does not exist, so the caller can
    /// re-read and retry, or learn that an `Absent` precondition lost a race.
    #[error("kv version conflict (current: {current:?})")]
    VersionConflict { current: Option<u64> },
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::codes::KV_OP_VERSION;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_kv_delete_many_when_round_tripped_then_should_preserve_bounds() {
        let request = KvDeleteMany {
            v: KV_OP_VERSION,
            namespace: "sessions".to_owned(),
            prefix: Some(b"user:".to_vec()),
            start: None,
            end: None,
            key_contains: Some("stale".to_owned()),
        };
        let bytes = encode_named(&request).expect("serializes");
        let back: KvDeleteMany = decode_named(&bytes).expect("deserializes");
        assert_eq!(back.prefix, Some(b"user:".to_vec()));
        assert_eq!(back.key_contains.as_deref(), Some("stale"));
    }

    #[test]
    fn given_kv_deleted_many_reply_when_round_tripped_then_should_preserve_count() {
        let reply = KvReply::Ok(KvOutcome::DeletedMany(7));
        let bytes = encode_named(&reply).expect("serializes");
        let back: KvReply = decode_named(&bytes).expect("deserializes");
        match back {
            KvReply::Ok(KvOutcome::DeletedMany(n)) => assert_eq!(n, 7),
            other => panic!("expected DeletedMany, got {other:?}"),
        }
    }

    #[test]
    fn given_a_set_request_when_round_tripped_then_should_preserve_value_and_expiry() {
        let request = KvSet {
            v: KV_OP_VERSION,
            namespace: "sessions".to_owned(),
            key: b"user:42".to_vec(),
            value: b"online".to_vec(),
            expires_at_micros: Some(1_700_000_000_000_000),
        };
        let bytes = encode_named(&request).expect("the request serializes");
        let back: KvSet = decode_named(&bytes).expect("the request deserializes");
        assert_eq!(back.key, b"user:42");
        assert_eq!(back.value, b"online");
        assert_eq!(back.expires_at_micros, Some(1_700_000_000_000_000));
    }

    #[test]
    fn given_a_binary_key_entry_when_round_tripped_then_should_preserve_raw_bytes() {
        let reply = KvReply::Ok(KvOutcome::Value(Some(KvEntry {
            key: vec![0xff, 0x00, 0xfe],
            value: vec![0x00, 0x01, 0x02],
            expires_at_micros: None,
            version: 0,
        })));
        let bytes = encode_named(&reply).expect("the reply serializes");
        let back: KvReply = decode_named(&bytes).expect("the reply deserializes");
        let KvReply::Ok(KvOutcome::Value(Some(entry))) = back else {
            panic!("expected an Ok(Value(Some)) reply");
        };
        assert_eq!(entry.key, vec![0xff, 0x00, 0xfe]);
        assert_eq!(entry.key_str(), None, "non-UTF-8 key has no string form");
        assert_eq!(entry.value, vec![0x00, 0x01, 0x02]);
    }

    #[test]
    fn given_a_scan_page_when_round_tripped_then_should_preserve_cursor() {
        let reply = KvReply::Ok(KvOutcome::Page(KvPage {
            entries: vec![KvEntry {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
                expires_at_micros: None,
                version: 0,
            }],
            cursor: Some(b"a".to_vec()),
        }));
        let bytes = encode_named(&reply).expect("serializes");
        let back: KvReply = decode_named(&bytes).expect("deserializes");
        let KvReply::Ok(KvOutcome::Page(page)) = back else {
            panic!("expected an Ok(Page) reply");
        };
        assert_eq!(page.entries.len(), 1);
        assert_eq!(page.entries[0].key_str(), Some("a"));
        assert_eq!(page.cursor.as_deref(), Some(b"a".as_ref()));
    }

    #[test]
    fn given_a_cas_request_when_round_tripped_then_should_preserve_the_precondition() {
        for expect in [CasExpect::Match(7), CasExpect::Absent] {
            let request = KvCas {
                v: KV_OP_VERSION,
                namespace: "counters".to_owned(),
                key: b"hits".to_vec(),
                value: b"42".to_vec(),
                expires_at_micros: None,
                expect,
            };
            let bytes = encode_named(&request).expect("serializes");
            let back: KvCas = decode_named(&bytes).expect("deserializes");
            assert_eq!(back.expect, expect);
            assert_eq!(back.key, b"hits");
        }
    }

    #[test]
    fn given_a_committed_reply_when_round_tripped_then_should_preserve_the_version() {
        let reply = KvReply::Ok(KvOutcome::Committed { version: 9 });
        let bytes = encode_named(&reply).expect("serializes");
        let back: KvReply = decode_named(&bytes).expect("deserializes");
        match back {
            KvReply::Ok(KvOutcome::Committed { version }) => assert_eq!(version, 9),
            other => panic!("expected Committed, got {other:?}"),
        }
    }

    #[test]
    fn given_a_version_conflict_when_round_tripped_then_should_preserve_the_current_version() {
        for current in [Some(3u64), None] {
            let reply = KvReply::Err(KvError::VersionConflict { current });
            let bytes = encode_named(&reply).expect("serializes");
            let back: KvReply = decode_named(&bytes).expect("deserializes");
            match back {
                KvReply::Err(KvError::VersionConflict { current: got }) => assert_eq!(got, current),
                other => panic!("expected VersionConflict, got {other:?}"),
            }
        }
    }

    #[test]
    fn given_a_versioned_entry_when_round_tripped_then_should_preserve_version_and_skip_zero() {
        let entry = KvEntry {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            expires_at_micros: None,
            version: 5,
        };
        let bytes = encode_named(&entry).expect("serializes");
        let back: KvEntry = decode_named(&bytes).expect("deserializes");
        assert_eq!(back.version, 5);
        // An unversioned entry (version 0) omits the field on the wire, so a
        // pre-versioning store stays byte-identical.
        let unversioned = KvEntry {
            version: 0,
            ..entry
        };
        let json = serde_json::to_string(&unversioned).expect("json");
        assert!(
            !json.contains("version"),
            "version 0 must be omitted: {json}"
        );
    }

    #[test]
    fn given_a_scan_with_bounds_when_round_tripped_then_should_preserve_filters() {
        let scan = KvScan {
            v: KV_OP_VERSION,
            namespace: "sessions".to_owned(),
            prefix: Some(b"user:".to_vec()),
            start: None,
            end: None,
            key_contains: Some("admin".to_owned()),
            limit: 50,
            cursor: Some(b"user:9".to_vec()),
        };
        let bytes = encode_named(&scan).expect("serializes");
        let back: KvScan = decode_named(&bytes).expect("deserializes");
        assert_eq!(back.prefix.as_deref(), Some(b"user:".as_ref()));
        assert_eq!(back.key_contains.as_deref(), Some("admin"));
        assert_eq!(back.cursor.as_deref(), Some(b"user:9".as_ref()));
    }
}

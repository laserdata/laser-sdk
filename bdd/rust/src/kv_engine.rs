// The reference key-value store, the counterpart to `query_engine` for the KV
// and compare-and-swap surface. It is the executable specification of the CAS
// race semantics every backend (and every SDK port) must reproduce: match,
// absent, conflict, version monotonicity, and expiry read as absence. Pure and
// transport-free. Time is an explicit `now_micros` argument so expiry is
// deterministic, with no wall clock.

use laser_sdk::kv::{CasExpect, KvEntry, KvError};
use std::collections::HashMap;

#[derive(Clone)]
struct Stored {
    value: Vec<u8>,
    version: u64,
    expires_at_micros: Option<u64>,
}

impl Stored {
    fn is_expired(&self, now_micros: u64) -> bool {
        self.expires_at_micros
            .is_some_and(|expiry| now_micros >= expiry)
    }
}

/// One namespace of the reference store. Keys are arbitrary bytes. Each key
/// carries a version that increases by one on every successful mutation in its
/// current lifetime. Expiry or deletion ends that lifetime, so a later create
/// starts again at version `1`.
#[derive(Default)]
pub struct KvEngine {
    entries: HashMap<Vec<u8>, Stored>,
}

impl KvEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the live entry at `key`, or `None` when it is absent or has expired.
    /// Expiry reads as absence: an expired entry is invisible to a reader.
    pub fn get(&self, key: &[u8], now_micros: u64) -> Option<KvEntry> {
        let stored = self.entries.get(key)?;
        if stored.is_expired(now_micros) {
            return None;
        }
        Some(KvEntry {
            key: key.to_vec(),
            value: stored.value.clone(),
            expires_at_micros: stored.expires_at_micros,
            version: stored.version,
            scope: None,
            source: None,
        })
    }

    /// Unconditional write. Bumps the version monotonically (the live version
    /// plus one, or `1` for a fresh or expired key). Returns the new version.
    pub fn set(
        &mut self,
        key: &[u8],
        value: Vec<u8>,
        expires_at_micros: Option<u64>,
        now_micros: u64,
    ) -> u64 {
        let version = self.next_version(key, now_micros);
        self.entries.insert(
            key.to_vec(),
            Stored {
                value,
                version,
                expires_at_micros,
            },
        );
        version
    }

    /// Compare-and-swap. Applies `value` only if `expect` holds against the live
    /// state, returning the new version on commit. On a precondition miss it
    /// returns [`KvError::VersionConflict`] carrying the live version (`Some`) or
    /// `None` when the key is absent or expired. The contract:
    ///
    /// - `Absent` commits only over an absent or expired key. An expired entry
    ///   is indistinguishable from an absent one, so the create-if-absent
    ///   succeeds over it and starts a fresh life at version `1`.
    /// - `Match(v)` commits only when the live version equals `v`. An expired
    ///   entry has no live version, so any `Match` against it conflicts with
    ///   `current: None`.
    pub fn cas(
        &mut self,
        key: &[u8],
        value: Vec<u8>,
        expect: CasExpect,
        expires_at_micros: Option<u64>,
        now_micros: u64,
    ) -> Result<u64, KvError> {
        let live = self
            .entries
            .get(key)
            .filter(|stored| !stored.is_expired(now_micros))
            .map(|stored| stored.version);
        match expect {
            CasExpect::Absent if live.is_none() => {}
            CasExpect::Match(expected) if live == Some(expected) => {}
            _ => return Err(KvError::VersionConflict { current: live }),
        }
        Ok(self.set(key, value, expires_at_micros, now_micros))
    }

    /// Delete the entry at `key`. Returns whether a live entry existed (an
    /// expired entry counts as already gone). The version counter for the key is
    /// dropped with it, so a later write starts again at `1`.
    pub fn delete(&mut self, key: &[u8], now_micros: u64) -> bool {
        let existed = self
            .entries
            .get(key)
            .is_some_and(|stored| !stored.is_expired(now_micros));
        self.entries.remove(key);
        existed
    }

    // The version a write to `key` should take: one past the live version, or
    // `1` when the key is absent or expired (an expired key starts a fresh life).
    fn next_version(&self, key: &[u8], now_micros: u64) -> u64 {
        self.entries
            .get(key)
            .filter(|stored| !stored.is_expired(now_micros))
            .map_or(1, |stored| stored.version + 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T0: u64 = 1_000;

    #[test]
    fn given_an_absent_key_when_cas_absent_then_should_commit_version_one() {
        let mut kv = KvEngine::new();
        let version = kv
            .cas(b"k", b"v1".to_vec(), CasExpect::Absent, None, T0)
            .expect("absent precondition holds on an empty key");
        assert_eq!(version, 1);
        assert_eq!(kv.get(b"k", T0).expect("present").value, b"v1");
    }

    #[test]
    fn given_a_present_key_when_cas_absent_then_should_conflict_with_current_version() {
        let mut kv = KvEngine::new();
        kv.cas(b"k", b"v1".to_vec(), CasExpect::Absent, None, T0)
            .expect("first create");
        let error = kv
            .cas(b"k", b"v2".to_vec(), CasExpect::Absent, None, T0)
            .expect_err("create-if-absent must fail on a present key");
        assert_eq!(error, KvError::VersionConflict { current: Some(1) });
    }

    #[test]
    fn given_a_matching_version_when_cas_then_should_commit_and_bump() {
        let mut kv = KvEngine::new();
        kv.cas(b"k", b"v1".to_vec(), CasExpect::Absent, None, T0)
            .expect("create");
        let version = kv
            .cas(b"k", b"v2".to_vec(), CasExpect::Match(1), None, T0)
            .expect("match on the live version commits");
        assert_eq!(version, 2);
        // The lost token can never match again.
        let error = kv
            .cas(b"k", b"v3".to_vec(), CasExpect::Match(1), None, T0)
            .expect_err("a stale token must conflict");
        assert_eq!(error, KvError::VersionConflict { current: Some(2) });
    }

    #[test]
    fn given_unconditional_sets_when_repeated_then_versions_increase_monotonically() {
        let mut kv = KvEngine::new();
        assert_eq!(kv.set(b"k", b"a".to_vec(), None, T0), 1);
        assert_eq!(kv.set(b"k", b"b".to_vec(), None, T0), 2);
        assert_eq!(kv.set(b"k", b"c".to_vec(), None, T0), 3);
    }

    #[test]
    fn given_an_expired_entry_when_read_then_should_be_absent() {
        let mut kv = KvEngine::new();
        kv.set(b"k", b"v".to_vec(), Some(T0 + 10), T0);
        assert!(kv.get(b"k", T0 + 5).is_some(), "live before expiry");
        assert!(kv.get(b"k", T0 + 10).is_none(), "expiry reads as absence");
    }

    #[test]
    fn given_an_expired_entry_when_cas_absent_then_should_commit_a_fresh_life() {
        let mut kv = KvEngine::new();
        kv.set(b"k", b"v1".to_vec(), Some(T0 + 10), T0); // version 1, expires at T0+10
        // After expiry the create-if-absent succeeds, and the key starts a fresh
        // life at version 1, since an expired key is indistinguishable from an
        // absent one.
        let version = kv
            .cas(b"k", b"v2".to_vec(), CasExpect::Absent, None, T0 + 20)
            .expect("expired key counts as absent");
        assert_eq!(version, 1);
    }

    #[test]
    fn given_an_expired_entry_when_cas_match_then_should_conflict_with_none() {
        let mut kv = KvEngine::new();
        kv.set(b"k", b"v1".to_vec(), Some(T0 + 10), T0); // version 1
        let error = kv
            .cas(b"k", b"v2".to_vec(), CasExpect::Match(1), None, T0 + 20)
            .expect_err("no live version to match after expiry");
        assert_eq!(error, KvError::VersionConflict { current: None });
    }

    #[test]
    fn given_a_deleted_key_when_rewritten_then_should_restart_versioning() {
        let mut kv = KvEngine::new();
        kv.set(b"k", b"v".to_vec(), None, T0);
        assert!(kv.delete(b"k", T0));
        assert!(
            !kv.delete(b"k", T0),
            "a second delete reports nothing existed"
        );
        assert_eq!(kv.set(b"k", b"again".to_vec(), None, T0), 1);
    }
}

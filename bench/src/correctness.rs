use std::collections::{BTreeSet, HashMap};

use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OraclePolicy {
    pub allow_duplicates: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ObservedRecord<'a> {
    pub id: u64,
    pub partition: u32,
    pub partition_sequence: u64,
    pub payload: &'a [u8],
    pub checksum: [u8; 32],
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CorrectnessSummary {
    pub observed: u64,
    pub missing: Vec<u64>,
    pub unexpected: Vec<u64>,
    pub duplicates: Vec<u64>,
    pub checksum_failures: Vec<u64>,
    pub ordering_violations: Vec<u64>,
    pub late_arrivals: Vec<u64>,
}

impl CorrectnessSummary {
    #[must_use]
    pub fn valid(&self, policy: OraclePolicy) -> bool {
        self.missing.is_empty()
            && self.unexpected.is_empty()
            && (policy.allow_duplicates || self.duplicates.is_empty())
            && self.checksum_failures.is_empty()
            && self.ordering_violations.is_empty()
    }
}

pub struct CorrectnessOracle {
    expected: BTreeSet<u64>,
    explained: BTreeSet<u64>,
    seen: BTreeSet<u64>,
    last_partition_sequence: HashMap<u32, u64>,
    policy: OraclePolicy,
    summary: CorrectnessSummary,
}

impl CorrectnessOracle {
    #[must_use]
    pub fn new(expected: impl IntoIterator<Item = u64>, policy: OraclePolicy) -> Self {
        Self {
            expected: expected.into_iter().collect(),
            explained: BTreeSet::new(),
            seen: BTreeSet::new(),
            last_partition_sequence: HashMap::new(),
            policy,
            summary: CorrectnessSummary::default(),
        }
    }

    /// Declare identifiers whose records may legally appear on the log even
    /// though their operations did not complete successfully: a client-side
    /// timeout or a transient-retry failure can still commit records
    /// server-side. Such records are counted as explained late arrivals
    /// instead of unexpected records, so overload evidence stays valid.
    #[must_use]
    pub fn with_explained(mut self, explained: impl IntoIterator<Item = u64>) -> Self {
        self.explained = explained.into_iter().collect();
        self
    }

    pub fn observe(&mut self, record: ObservedRecord<'_>) {
        self.summary.observed += 1;
        let explained = self.explained.contains(&record.id);
        if explained {
            self.summary.late_arrivals.push(record.id);
            if checksum(record.payload) != record.checksum {
                self.summary.checksum_failures.push(record.id);
            }
            return;
        }
        if !self.seen.insert(record.id) {
            self.summary.duplicates.push(record.id);
        }
        if !self.expected.contains(&record.id) {
            self.summary.unexpected.push(record.id);
        }
        if checksum(record.payload) != record.checksum {
            self.summary.checksum_failures.push(record.id);
        }
        if self
            .last_partition_sequence
            .get(&record.partition)
            .is_some_and(|last| record.partition_sequence <= *last)
        {
            self.summary.ordering_violations.push(record.id);
        }
        self.last_partition_sequence
            .insert(record.partition, record.partition_sequence);
    }

    #[must_use]
    pub fn finish(mut self) -> CorrectnessSummary {
        self.summary.missing = self.expected.difference(&self.seen).copied().collect();
        debug_assert!(self.summary.valid(self.policy) || !self.expected.is_empty());
        self.summary
    }
}

#[must_use]
pub fn checksum(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: u64, partition_sequence: u64, payload: &[u8]) -> ObservedRecord<'_> {
        ObservedRecord {
            id,
            partition: 0,
            partition_sequence,
            payload,
            checksum: checksum(payload),
        }
    }

    #[test]
    fn given_complete_ordered_records_when_finished_then_should_be_valid() {
        let policy = OraclePolicy {
            allow_duplicates: false,
        };
        let mut oracle = CorrectnessOracle::new(0..2, policy);
        oracle.observe(record(0, 0, b"zero"));
        oracle.observe(record(1, 1, b"one"));
        assert!(oracle.finish().valid(policy));
    }

    #[test]
    fn given_missing_duplicate_corrupt_and_reordered_records_then_should_report_each_defect() {
        let policy = OraclePolicy {
            allow_duplicates: false,
        };
        let mut oracle = CorrectnessOracle::new(0..3, policy);
        oracle.observe(record(0, 1, b"zero"));
        oracle.observe(record(0, 0, b"zero"));
        let mut corrupt = record(1, 2, b"one");
        corrupt.checksum = [0; 32];
        oracle.observe(corrupt);
        let summary = oracle.finish();
        assert_eq!(summary.missing, [2]);
        assert!(summary.unexpected.is_empty());
        assert_eq!(summary.duplicates, [0]);
        assert_eq!(summary.checksum_failures, [1]);
        assert_eq!(summary.ordering_violations, [0]);
        assert!(!summary.valid(policy));
    }

    #[test]
    fn given_unknown_record_when_finished_then_should_report_it() {
        let policy = OraclePolicy {
            allow_duplicates: false,
        };
        let mut oracle = CorrectnessOracle::new([0], policy);
        oracle.observe(record(7, 0, b"unknown"));
        let summary = oracle.finish();
        assert_eq!(summary.missing, [0]);
        assert_eq!(summary.unexpected, [7]);
        assert!(!summary.valid(policy));
    }

    #[test]
    fn given_corrupt_retried_late_arrival_when_observed_then_should_validate_integrity_only() {
        let policy = OraclePolicy {
            allow_duplicates: false,
        };
        let mut oracle = CorrectnessOracle::new([0], policy).with_explained([7]);
        let mut corrupt = record(7, 1, b"late");
        corrupt.checksum = [0; 32];
        oracle.observe(corrupt);
        oracle.observe(record(7, 0, b"late"));
        oracle.observe(record(0, 2, b"expected"));

        let summary = oracle.finish();
        assert_eq!(summary.late_arrivals, [7, 7]);
        assert!(summary.duplicates.is_empty());
        assert_eq!(summary.checksum_failures, [7]);
        assert!(summary.ordering_violations.is_empty());
        assert!(!summary.valid(policy));
    }
}

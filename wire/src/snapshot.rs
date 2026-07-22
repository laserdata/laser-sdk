use crate::agent::ConversationId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A fold snapshot: the folded `state` plus the per-partition offsets it folded
/// through. `as_of` maps a partition id to the last offset folded into `state`
/// (inclusive), matching the cursor's per-partition offsets, so a multi-partition
/// fold resumes correctly. Resume seeds the cursor at `offset + 1` per partition,
/// because the cursor takes the next offset to read (exclusive) while the
/// snapshot records the last offset folded (inclusive).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FoldSnapshot {
    /// The fold this snapshots (the conversation or journal partition key).
    pub conversation: ConversationId,
    /// Per-partition last folded offset, inclusive. A windowless fold over one
    /// partition is a single entry.
    pub as_of: BTreeMap<u32, u64>,
    /// The opaque folded state. The codec is the producer's choice, the wire
    /// crate never inspects it.
    #[serde(with = "crate::encoding::bin_bytes")]
    pub state: Vec<u8>,
}

impl FoldSnapshot {
    /// The offset a partition's resume should start reading from: one past the
    /// last folded offset, or `0` for a partition the snapshot did not cover.
    pub fn resume_offset(&self, partition: u32) -> u64 {
        self.as_of
            .get(&partition)
            .map_or(0, |offset| offset.saturating_add(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_snapshot_when_asked_for_resume_then_should_return_one_past_the_folded_offset() {
        let snapshot = FoldSnapshot {
            conversation: ConversationId::from_u128(1),
            as_of: BTreeMap::from([(0, 41), (1, 9)]),
            state: vec![1, 2, 3],
        };
        assert_eq!(snapshot.resume_offset(0), 42);
        assert_eq!(snapshot.resume_offset(1), 10);
        // A partition the snapshot never folded resumes from zero.
        assert_eq!(snapshot.resume_offset(2), 0);
    }
}

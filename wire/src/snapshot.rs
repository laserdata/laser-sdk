use crate::agent::ConversationId;
use crate::error::InvalidError;
use crate::validate::Validate;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// One source partition and its last folded offset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotOffset {
    pub topic_id: u32,
    pub topic_created_at_micros: u64,
    pub partition_id: u32,
    pub offset: u64,
}

impl SnapshotOffset {
    pub const fn new(
        topic_id: u32,
        topic_created_at_micros: u64,
        partition_id: u32,
        offset: u64,
    ) -> Self {
        Self {
            topic_id,
            topic_created_at_micros,
            partition_id,
            offset,
        }
    }

    fn key(self) -> (u32, u64, u32) {
        (
            self.topic_id,
            self.topic_created_at_micros,
            self.partition_id,
        )
    }
}

impl Serialize for SnapshotOffset {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (
            self.topic_id,
            self.topic_created_at_micros,
            self.partition_id,
            self.offset,
        )
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SnapshotOffset {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (topic_id, topic_created_at_micros, partition_id, offset) =
            <(u32, u64, u32, u64)>::deserialize(deserializer)?;
        Ok(Self::new(
            topic_id,
            topic_created_at_micros,
            partition_id,
            offset,
        ))
    }
}

/// A fold state and the exact source positions included in it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FoldSnapshot {
    pub stream: String,
    pub stream_id: u32,
    pub stream_created_at_micros: u64,
    pub conversation: ConversationId,
    pub fold: String,
    /// Sorted by topic ID, topic creation time, and partition ID.
    pub as_of: Vec<SnapshotOffset>,
    /// The application chooses the codec for these bytes.
    #[serde(with = "crate::encoding::bin_bytes")]
    pub state: Vec<u8>,
}

impl FoldSnapshot {
    /// Return the next offset for one exact source partition.
    pub fn resume_offset(
        &self,
        topic_id: u32,
        topic_created_at_micros: u64,
        partition_id: u32,
    ) -> u64 {
        let key = (topic_id, topic_created_at_micros, partition_id);
        self.as_of
            .binary_search_by_key(&key, |entry| entry.key())
            .ok()
            .map_or(0, |index| self.as_of[index].offset.saturating_add(1))
    }
}

impl Validate for FoldSnapshot {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.stream.is_empty() || self.fold.is_empty() {
            return Err(InvalidError::new(
                "snapshot stream and fold must be non-empty",
            ));
        }
        if self
            .as_of
            .windows(2)
            .any(|pair| pair[0].key() >= pair[1].key())
        {
            return Err(InvalidError::new(
                "snapshot offsets must be sorted with no duplicate source",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> FoldSnapshot {
        FoldSnapshot {
            stream: "agents".to_owned(),
            stream_id: 0,
            stream_created_at_micros: 100,
            conversation: ConversationId::from_u128(1),
            fold: "planner".to_owned(),
            as_of: vec![
                SnapshotOffset::new(2, 20, 0, 41),
                SnapshotOffset::new(2, 20, 1, 9),
            ],
            state: vec![1, 2, 3],
        }
    }

    #[test]
    fn given_a_snapshot_when_resumed_then_should_match_the_exact_topic_generation() {
        let snapshot = snapshot();
        snapshot.validate().expect("valid snapshot");
        assert_eq!(snapshot.resume_offset(2, 20, 0), 42);
        assert_eq!(snapshot.resume_offset(2, 20, 1), 10);
        assert_eq!(snapshot.resume_offset(2, 21, 0), 0);
        assert_eq!(snapshot.resume_offset(3, 20, 0), 0);
    }

    #[test]
    fn given_duplicate_or_unsorted_sources_when_validated_then_should_reject_them() {
        let mut snapshot = snapshot();
        snapshot.as_of.push(SnapshotOffset::new(2, 20, 1, 10));
        assert!(snapshot.validate().is_err());
        snapshot.as_of[2] = SnapshotOffset::new(1, 20, 0, 10);
        assert!(snapshot.validate().is_err());
    }
}

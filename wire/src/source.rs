use crate::agent::wire_id;
use crate::error::InvalidError;
use crate::validate::Validate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_SOURCE_PARTITIONS: usize = 65_536;

wire_id!(
    /// Durable identity of one physical Iggy cluster across replica replacement and restore.
    PhysicalClusterIncarnation
);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SourceIncarnation {
    pub cluster: PhysicalClusterIncarnation,
    pub stream_id: u32,
    pub topic_id: u32,
    pub partition_id: u32,
    pub partition_created_revision: u64,
}

impl Validate for SourceIncarnation {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.cluster.as_u128() == 0 {
            return Err(InvalidError::new(
                "physical cluster incarnation must be nonzero",
            ));
        }
        if self.partition_created_revision == 0 {
            return Err(InvalidError::new(
                "partition created revision must be nonzero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceScope {
    pub stream: String,
    pub topic: String,
}

impl SourceScope {
    pub fn new(stream: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            topic: topic.into(),
        }
    }
}

impl Validate for SourceScope {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_source_name("stream", &self.stream)?;
        validate_source_name("topic", &self.topic)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourcePartitionCut {
    pub incarnation: SourceIncarnation,
    pub retained_start: u64,
    pub end_exclusive: u64,
}

impl Validate for SourcePartitionCut {
    fn validate(&self) -> Result<(), InvalidError> {
        self.incarnation.validate()?;
        if self.retained_start > self.end_exclusive {
            return Err(InvalidError::new(format!(
                "retained start {} is above end-exclusive head {}",
                self.retained_start, self.end_exclusive
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCut {
    pub metadata_revision: u64,
    pub partitions: Vec<SourcePartitionCut>,
}

impl Validate for SourceCut {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.metadata_revision == 0 {
            return Err(InvalidError::new(
                "source metadata revision must be nonzero",
            ));
        }
        if self.partitions.is_empty() || self.partitions.len() > MAX_SOURCE_PARTITIONS {
            return Err(InvalidError::new(format!(
                "source cut must contain 1..={MAX_SOURCE_PARTITIONS} partitions"
            )));
        }
        let mut partitions = BTreeSet::new();
        let mut previous_partition = None;
        let mut namespace = None;
        for partition in &self.partitions {
            partition.validate()?;
            let current = (
                partition.incarnation.cluster,
                partition.incarnation.stream_id,
                partition.incarnation.topic_id,
            );
            if namespace.is_some_and(|expected| expected != current) {
                return Err(InvalidError::new(
                    "source cut partitions must share one cluster, stream, and topic",
                ));
            }
            namespace = Some(current);
            if !partitions.insert(partition.incarnation.partition_id) {
                return Err(InvalidError::new(format!(
                    "source cut repeats partition {}",
                    partition.incarnation.partition_id
                )));
            }
            if previous_partition
                .is_some_and(|previous| previous >= partition.incarnation.partition_id)
            {
                return Err(InvalidError::new(
                    "source cut partitions must be ordered by ascending partition id",
                ));
            }
            previous_partition = Some(partition.incarnation.partition_id);
        }
        Ok(())
    }
}

fn validate_source_name(label: &str, value: &str) -> Result<(), InvalidError> {
    if value.is_empty() {
        return Err(InvalidError::new(format!("{label} must not be empty")));
    }
    if value.len() > 255 {
        return Err(InvalidError::new(format!(
            "{label} is {} bytes, exceeds cap 255",
            value.len()
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(InvalidError::new(format!(
            "{label} contains a control character"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn incarnation(partition_id: u32, created_revision: u64) -> SourceIncarnation {
        SourceIncarnation {
            cluster: PhysicalClusterIncarnation::from_u128(1),
            stream_id: 10,
            topic_id: 20,
            partition_id,
            partition_created_revision: created_revision,
        }
    }

    #[test]
    fn given_a_recreated_partition_when_incarnations_are_compared_then_should_differ() {
        assert_ne!(incarnation(1, 7), incarnation(1, 8));
        incarnation(1, 7).validate().expect("incarnation is valid");
    }

    #[test]
    fn given_duplicate_partitions_or_inverted_heads_when_a_source_cut_is_validated_then_should_reject()
     {
        let duplicate = SourceCut {
            metadata_revision: 9,
            partitions: vec![
                SourcePartitionCut {
                    incarnation: incarnation(1, 7),
                    retained_start: 0,
                    end_exclusive: 10,
                },
                SourcePartitionCut {
                    incarnation: incarnation(1, 7),
                    retained_start: 2,
                    end_exclusive: 10,
                },
            ],
        };
        assert!(duplicate.validate().is_err());

        let inverted = SourcePartitionCut {
            incarnation: incarnation(1, 7),
            retained_start: 11,
            end_exclusive: 10,
        };
        assert!(inverted.validate().is_err());
    }
}

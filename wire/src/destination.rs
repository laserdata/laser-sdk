use crate::agent::wire_id;
use crate::control::ProjectionId;
use crate::error::InvalidError;
use crate::schema::{LogicalSchemaRef, UuidValue};
use crate::source::{SourceIncarnation, SourceScope};
use crate::validate::Validate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_DESTINATION_NAME_BYTES: usize = 255;
pub const MAX_NAMESPACE_PARTS: usize = 16;
pub const MAX_TABLE_NAME_BYTES: usize = 255;
pub const MAX_EXPLICIT_PARTITION_STARTS: usize = 65_536;

wire_id!(
    /// Immutable identity of one materialization destination across generations.
    DestinationId
);

wire_id!(
    /// Stable identity of one supervisor-owned backend resource.
    BackendResourceId
);

wire_id!(
    /// Immutable identity of one logical query route across generations.
    QueryRouteId
);

wire_id!(
    /// Identity of one accepted asynchronous destination operation.
    DestinationOperationId
);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionRef {
    pub id: ProjectionId,
    pub version: u32,
}

impl Validate for ProjectionRef {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.id.as_str().is_empty() {
            return Err(InvalidError::new("projection id must not be empty"));
        }
        if self.version == 0 {
            return Err(InvalidError::new("projection version must be nonzero"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendBinding {
    pub resource_id: BackendResourceId,
    pub generation: u64,
}

impl Validate for BackendBinding {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.resource_id.as_u128() == 0 {
            return Err(InvalidError::new("backend resource id must be nonzero"));
        }
        if self.generation == 0 {
            return Err(InvalidError::new("backend generation must be nonzero"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalTable {
    pub namespace: Vec<String>,
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_table_uuid: Option<UuidValue>,
}

impl Validate for PhysicalTable {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.namespace.is_empty() || self.namespace.len() > MAX_NAMESPACE_PARTS {
            return Err(InvalidError::new(format!(
                "table namespace must contain 1..={MAX_NAMESPACE_PARTS} parts"
            )));
        }
        for part in &self.namespace {
            validate_table_name("namespace part", part)?;
        }
        validate_table_name("table", &self.table)?;
        if let Some(table_uuid) = &self.expected_table_uuid
            && table_uuid.as_bytes().len() != UuidValue::BYTES
        {
            return Err(InvalidError::new(format!(
                "table UUID must be {} bytes",
                UuidValue::BYTES
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum FileFormat {
    Parquet,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TableFormat {
    IcebergV2,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum StartPolicy {
    Beginning,
    CapturedLatest,
    Explicit { partitions: Vec<PartitionStart> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionStart {
    pub incarnation: SourceIncarnation,
    pub next_offset: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NewPartitionPolicy {
    Beginning,
    CapturedLatest,
    Reject,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RecreatedPartitionPolicy {
    Reject,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DestinationErrorPolicy {
    Block,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DestinationDesiredState {
    Disabled,
    Enabled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationDestination {
    pub id: DestinationId,
    pub generation: u64,
    pub definition_revision: u64,
    pub name: String,
    pub source: SourceScope,
    pub recreated_partition_policy: RecreatedPartitionPolicy,
    pub projection: ProjectionRef,
    pub schema: LogicalSchemaRef,
    pub backend: BackendBinding,
    pub table: PhysicalTable,
    pub file_format: FileFormat,
    pub table_format: TableFormat,
    pub start_policy: StartPolicy,
    pub new_partition_policy: NewPartitionPolicy,
    pub error_policy: DestinationErrorPolicy,
    pub desired_state: DestinationDesiredState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRoute {
    pub id: QueryRouteId,
    pub generation: u64,
    pub definition_revision: u64,
    pub name: String,
    pub target: QueryRouteTarget,
    pub desired_state: DestinationDesiredState,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum QueryRouteTarget {
    Operational {
        index: String,
    },
    Lakehouse {
        destination_id: DestinationId,
        destination_generation: u64,
    },
}

impl Validate for QueryRoute {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.id.as_u128() == 0 || self.generation == 0 || self.definition_revision == 0 {
            return Err(InvalidError::new(
                "query route identity, generation, and definition revision must be nonzero",
            ));
        }
        validate_destination_name(&self.name)?;
        match &self.target {
            QueryRouteTarget::Operational { index } => validate_table_name("index", index),
            QueryRouteTarget::Lakehouse {
                destination_id,
                destination_generation,
            } => {
                if destination_id.as_u128() == 0 || *destination_generation == 0 {
                    return Err(InvalidError::new(
                        "query route destination identity and generation must be nonzero",
                    ));
                }
                Ok(())
            }
        }
    }
}

impl Validate for MaterializationDestination {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.id.as_u128() == 0 {
            return Err(InvalidError::new("destination id must be nonzero"));
        }
        if self.generation == 0 {
            return Err(InvalidError::new("destination generation must be nonzero"));
        }
        if self.definition_revision == 0 {
            return Err(InvalidError::new(
                "destination definition revision must be nonzero",
            ));
        }
        validate_destination_name(&self.name)?;
        self.source.validate()?;
        self.projection.validate()?;
        self.schema.validate()?;
        self.backend.validate()?;
        self.table.validate()?;
        validate_start_policy(&self.start_policy)
    }
}

fn validate_start_policy(policy: &StartPolicy) -> Result<(), InvalidError> {
    let StartPolicy::Explicit { partitions } = policy else {
        return Ok(());
    };
    if partitions.is_empty() || partitions.len() > MAX_EXPLICIT_PARTITION_STARTS {
        return Err(InvalidError::new(format!(
            "explicit start must contain 1..={MAX_EXPLICIT_PARTITION_STARTS} partitions"
        )));
    }
    let mut partition_ids = BTreeSet::new();
    let mut previous_partition = None;
    let mut namespace = None;
    for partition in partitions {
        partition.incarnation.validate()?;
        let current = (
            partition.incarnation.cluster,
            partition.incarnation.stream_id,
            partition.incarnation.topic_id,
        );
        if namespace.is_some_and(|expected| expected != current) {
            return Err(InvalidError::new(
                "explicit start partitions must share one cluster, stream, and topic",
            ));
        }
        namespace = Some(current);
        if !partition_ids.insert(partition.incarnation.partition_id) {
            return Err(InvalidError::new(format!(
                "explicit start repeats partition {}",
                partition.incarnation.partition_id
            )));
        }
        if previous_partition.is_some_and(|previous| previous >= partition.incarnation.partition_id)
        {
            return Err(InvalidError::new(
                "explicit start partitions must be ordered by ascending partition id",
            ));
        }
        previous_partition = Some(partition.incarnation.partition_id);
    }
    Ok(())
}

fn validate_destination_name(value: &str) -> Result<(), InvalidError> {
    if value.is_empty() || value.len() > MAX_DESTINATION_NAME_BYTES {
        return Err(InvalidError::new(format!(
            "destination name must contain 1..={MAX_DESTINATION_NAME_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(InvalidError::new(
            "destination name contains a control character",
        ));
    }
    Ok(())
}

fn validate_table_name(label: &str, value: &str) -> Result<(), InvalidError> {
    if value.is_empty() || value.len() > MAX_TABLE_NAME_BYTES {
        return Err(InvalidError::new(format!(
            "{label} must contain 1..={MAX_TABLE_NAME_BYTES} bytes"
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
    use crate::schema::{LogicalSchemaId, SchemaFingerprint};
    use crate::source::PhysicalClusterIncarnation;

    fn incarnation(partition_id: u32) -> SourceIncarnation {
        SourceIncarnation {
            cluster: PhysicalClusterIncarnation::from_u128(1),
            stream_id: 10,
            topic_id: 20,
            partition_id,
            partition_created_revision: 7,
        }
    }

    fn destination() -> MaterializationDestination {
        MaterializationDestination {
            id: DestinationId::from_u128(1),
            generation: 1,
            definition_revision: 1,
            name: "orders-lakehouse".to_owned(),
            source: SourceScope::new("shop", "orders"),
            recreated_partition_policy: RecreatedPartitionPolicy::Reject,
            projection: ProjectionRef {
                id: ProjectionId::new("order.v1"),
                version: 1,
            },
            schema: LogicalSchemaRef {
                id: LogicalSchemaId::from_u128(2),
                version: 1,
                fingerprint: SchemaFingerprint::new([3; 32]),
            },
            backend: BackendBinding {
                resource_id: BackendResourceId::from_u128(4),
                generation: 2,
            },
            table: PhysicalTable {
                namespace: vec!["shop".to_owned()],
                table: "orders".to_owned(),
                expected_table_uuid: None,
            },
            file_format: FileFormat::Parquet,
            table_format: TableFormat::IcebergV2,
            start_policy: StartPolicy::CapturedLatest,
            new_partition_policy: NewPartitionPolicy::CapturedLatest,
            error_policy: DestinationErrorPolicy::Block,
            desired_state: DestinationDesiredState::Disabled,
        }
    }

    #[test]
    fn given_a_destination_when_validated_then_should_bind_schema_projection_backend_and_table() {
        destination().validate().expect("destination is valid");
        let mut invalid = destination();
        invalid.backend.generation = 0;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn given_an_explicit_start_when_a_partition_repeats_then_should_reject() {
        let mut invalid = destination();
        invalid.start_policy = StartPolicy::Explicit {
            partitions: vec![
                PartitionStart {
                    incarnation: incarnation(1),
                    next_offset: 0,
                },
                PartitionStart {
                    incarnation: incarnation(1),
                    next_offset: 1,
                },
            ],
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn given_a_lakehouse_route_when_validated_then_should_require_a_specific_destination_generation()
     {
        let route = QueryRoute {
            id: QueryRouteId::from_u128(5),
            generation: 1,
            definition_revision: 1,
            name: "orders".to_owned(),
            target: QueryRouteTarget::Lakehouse {
                destination_id: destination().id,
                destination_generation: 1,
            },
            desired_state: DestinationDesiredState::Enabled,
        };
        route.validate().expect("route is valid");
    }
}

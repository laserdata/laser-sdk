use crate::agent::wire_id;
use crate::authz::{
    Action, Feature, SUPERVISOR_ASSERTION_KEY_ID_BYTES, SupervisorActorAssertion,
    SupervisorAssertionAction,
};
use crate::destination::{
    BackendBinding, DestinationDesiredState, DestinationId, MaterializationDestination,
    ProjectionRef, QueryRoute, QueryRouteId,
};
use crate::error::InvalidError;
use crate::schema::{Digest32, LogicalSchemaRef, SchemaFingerprint, TypedValue, UuidValue};
use crate::source::SourceIncarnation;
use crate::validate::Validate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_CHECKPOINT_PARTITIONS: usize = 65_536;
pub const MAX_ATTEMPT_OBJECTS: usize = 100_000;
pub const MAX_CREDENTIAL_GENERATIONS: usize = 32;
pub const MAX_MANIFEST_IDENTITY_BYTES: usize = 4_096;
pub const MAX_CHECKPOINT_ERROR_BYTES: usize = 4_096;
pub const MAX_REPAIR_DETAIL_BYTES: usize = 4_096;
pub const MAX_CHECKPOINT_LEASE_DURATION_MICROS: u64 = 300_000_000;

wire_id!(
    /// Stable identity of one Plane process incarnation that may own a destination.
    CheckpointOwnerId
);

wire_id!(
    /// Immutable identity of one prepared materialization attempt.
    PreparedAttemptId
);

wire_id!(
    /// Stable request identity used to deduplicate a checkpoint mutation retry.
    CheckpointRequestId
);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCheckpoint {
    pub incarnation: SourceIncarnation,
    pub started_at_offset: u64,
    pub next_offset: u64,
    pub lifecycle: PartitionLifecycleState,
}

impl Validate for PartitionCheckpoint {
    fn validate(&self) -> Result<(), InvalidError> {
        self.incarnation.validate()?;
        if self.started_at_offset > self.next_offset {
            return Err(InvalidError::new(
                "partition start offset must not exceed its next offset",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PartitionLifecycleState {
    Active,
    Removed,
    Recreated,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceOffsetRange {
    pub incarnation: SourceIncarnation,
    pub start: u64,
    pub end_exclusive: u64,
}

impl Validate for SourceOffsetRange {
    fn validate(&self) -> Result<(), InvalidError> {
        self.incarnation.validate()?;
        if self.start >= self.end_exclusive {
            return Err(InvalidError::new(format!(
                "source range [{}, {}) is empty or inverted",
                self.start, self.end_exclusive
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointOwnerLease {
    pub owner: CheckpointOwnerId,
    pub epoch: u64,
    pub sequence: u64,
    pub deadline_micros: u64,
}

impl Validate for CheckpointOwnerLease {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.owner.as_u128() == 0 {
            return Err(InvalidError::new("checkpoint owner id must be nonzero"));
        }
        if self.epoch == 0 || self.sequence == 0 || self.deadline_micros == 0 {
            return Err(InvalidError::new(
                "checkpoint lease epoch, sequence, and deadline must be nonzero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialGeneration {
    pub role: String,
    pub generation: u64,
}

impl Validate for CredentialGeneration {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.role.is_empty() || self.role.len() > 64 {
            return Err(InvalidError::new(
                "credential role must contain 1..=64 bytes",
            ));
        }
        if self
            .role
            .bytes()
            .any(|byte| !matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_'))
        {
            return Err(InvalidError::new(
                "credential role contains a disallowed byte",
            ));
        }
        if self.generation == 0 {
            return Err(InvalidError::new("credential generation must be nonzero"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AttemptObject {
    pub identity: String,
    pub size_bytes: u64,
    pub row_count: u64,
    pub sha256: Digest32,
    pub columns: Vec<AttemptColumnMetrics>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AttemptColumnMetrics {
    pub field_id: u32,
    pub value_count: u64,
    pub null_count: u64,
    pub nan_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<TypedValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<TypedValue>,
}

impl Validate for AttemptObject {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_object_identity(&self.identity)?;
        if self.size_bytes == 0 {
            return Err(InvalidError::new("attempt object size must be nonzero"));
        }
        if self.row_count == 0 {
            return Err(InvalidError::new(
                "attempt object row count must be nonzero",
            ));
        }
        self.sha256.validate()?;
        if self.columns.len() > crate::schema::MAX_LOGICAL_SCHEMA_FIELDS {
            return Err(InvalidError::new(
                "attempt object column metric count exceeds the schema field cap",
            ));
        }
        let mut fields = BTreeSet::new();
        for column in &self.columns {
            column.validate()?;
            if !fields.insert(column.field_id) {
                return Err(InvalidError::new(format!(
                    "attempt object repeats column metric {}",
                    column.field_id
                )));
            }
        }
        Ok(())
    }
}

impl Validate for AttemptColumnMetrics {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.field_id == 0
            || self.null_count > self.value_count
            || self.nan_count > self.value_count.saturating_sub(self.null_count)
        {
            return Err(InvalidError::new("attempt column metrics are invalid"));
        }
        if let Some(value) = &self.lower_bound {
            value.validate()?;
        }
        if let Some(value) = &self.upper_bound {
            value.validate()?;
        }
        if self.lower_bound.is_some() != self.upper_bound.is_some() {
            return Err(InvalidError::new(
                "attempt column bounds must be both present or both absent",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedTableRequirements {
    pub table_uuid: UuidValue,
    pub base_metadata_identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_snapshot_id: Option<i64>,
    pub schema_id: i32,
    pub partition_spec_id: i32,
    pub commit_requirements: Vec<IcebergCommitRequirement>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum IcebergCommitRequirement {
    AssertTableUuid { table_uuid: UuidValue },
    AssertMetadataIdentity { identity: String },
    AssertCurrentSnapshot { snapshot_id: Option<i64> },
    AssertCurrentSchema { schema_id: i32 },
    AssertDefaultPartitionSpec { partition_spec_id: i32 },
}

impl Validate for PreparedTableRequirements {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_uuid(&self.table_uuid)?;
        validate_object_identity(&self.base_metadata_identity)?;
        if self
            .base_snapshot_id
            .is_some_and(|snapshot_id| snapshot_id <= 0)
            || self.schema_id < 0
            || self.partition_spec_id < 0
        {
            return Err(InvalidError::new(
                "prepared table snapshot, schema, or partition spec is invalid",
            ));
        }
        let expected = [
            IcebergCommitRequirement::AssertTableUuid {
                table_uuid: self.table_uuid.clone(),
            },
            IcebergCommitRequirement::AssertMetadataIdentity {
                identity: self.base_metadata_identity.clone(),
            },
            IcebergCommitRequirement::AssertCurrentSnapshot {
                snapshot_id: self.base_snapshot_id,
            },
            IcebergCommitRequirement::AssertCurrentSchema {
                schema_id: self.schema_id,
            },
            IcebergCommitRequirement::AssertDefaultPartitionSpec {
                partition_spec_id: self.partition_spec_id,
            },
        ];
        if self.commit_requirements.as_slice() != expected.as_slice() {
            return Err(InvalidError::new(
                "Iceberg commit requirements must contain the exact frozen table preconditions in canonical order",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PreparedAttempt {
    pub id: PreparedAttemptId,
    pub destination_id: DestinationId,
    pub destination_generation: u64,
    pub backend: BackendBinding,
    pub owner: CheckpointOwnerId,
    pub epoch: u64,
    pub created_at_checkpoint_revision: u64,
    pub table: PreparedTableRequirements,
    pub schema_fingerprint: SchemaFingerprint,
    pub projection: ProjectionRef,
    pub ranges: Vec<SourceOffsetRange>,
    pub resulting_boundary: Vec<PartitionCheckpoint>,
    pub resulting_boundary_digest: Digest32,
    pub manifest_identity: String,
    pub manifest_digest: Digest32,
    pub objects: Vec<AttemptObject>,
    pub credential_generations: Vec<CredentialGeneration>,
}

impl Validate for PreparedAttempt {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_attempt_identity(self.id, self.destination_id, self.destination_generation)?;
        self.backend.validate()?;
        if self.owner.as_u128() == 0 || self.epoch == 0 {
            return Err(InvalidError::new(
                "prepared attempt owner and epoch must be nonzero",
            ));
        }
        if self.created_at_checkpoint_revision == 0 {
            return Err(InvalidError::new(
                "prepared attempt creation revision must be nonzero",
            ));
        }
        self.table.validate()?;
        self.schema_fingerprint.validate()?;
        self.projection.validate()?;
        self.resulting_boundary_digest.validate()?;
        self.manifest_digest.validate()?;
        validate_ranges(&self.ranges)?;
        validate_resulting_boundary(&self.resulting_boundary, &self.ranges)?;
        validate_object_identity(&self.manifest_identity)?;
        if self.objects.is_empty() || self.objects.len() > MAX_ATTEMPT_OBJECTS {
            return Err(InvalidError::new(format!(
                "prepared attempt must contain 1..={MAX_ATTEMPT_OBJECTS} objects"
            )));
        }
        let mut object_identities = BTreeSet::new();
        for object in &self.objects {
            object.validate()?;
            if !object_identities.insert(&object.identity) {
                return Err(InvalidError::new(format!(
                    "prepared attempt repeats object `{}`",
                    object.identity
                )));
            }
        }
        validate_credential_generations(&self.credential_generations)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedAttemptSummary {
    pub id: PreparedAttemptId,
    pub owner: CheckpointOwnerId,
    pub epoch: u64,
    pub table: PreparedTableRequirements,
    pub schema_fingerprint: SchemaFingerprint,
    pub projection: ProjectionRef,
    pub manifest_identity: String,
    pub manifest_digest: Digest32,
    pub resulting_boundary_digest: Digest32,
    pub ranges: Vec<SourceOffsetRange>,
    pub object_count: u32,
    pub credential_generations: Vec<CredentialGeneration>,
}

impl Validate for PreparedAttemptSummary {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.id.as_u128() == 0 || self.owner.as_u128() == 0 || self.epoch == 0 {
            return Err(InvalidError::new(
                "prepared attempt summary identity must be nonzero",
            ));
        }
        self.table.validate()?;
        self.schema_fingerprint.validate()?;
        self.projection.validate()?;
        validate_object_identity(&self.manifest_identity)?;
        self.manifest_digest.validate()?;
        self.resulting_boundary_digest.validate()?;
        validate_ranges(&self.ranges)?;
        if self.object_count == 0 || self.object_count as usize > MAX_ATTEMPT_OBJECTS {
            return Err(InvalidError::new(
                "prepared attempt summary object count is invalid",
            ));
        }
        validate_credential_generations(&self.credential_generations)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletedAttempt {
    pub id: PreparedAttemptId,
    pub table_uuid: UuidValue,
    pub snapshot_id: i64,
    pub manifest_digest: Digest32,
    pub resulting_boundary_digest: Digest32,
    pub ranges: Vec<SourceOffsetRange>,
    pub completion_revision: u64,
}

impl Validate for CompletedAttempt {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.id.as_u128() == 0 {
            return Err(InvalidError::new("completed attempt id must be nonzero"));
        }
        if self.table_uuid.as_bytes().len() != UuidValue::BYTES {
            return Err(InvalidError::new("completed attempt table UUID is invalid"));
        }
        if self.snapshot_id <= 0 {
            return Err(InvalidError::new(
                "completed attempt snapshot id must be positive",
            ));
        }
        self.manifest_digest.validate()?;
        self.resulting_boundary_digest.validate()?;
        if self.completion_revision == 0 {
            return Err(InvalidError::new(
                "completed attempt revision must be nonzero",
            ));
        }
        validate_ranges(&self.ranges)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionGap {
    pub incarnation: SourceIncarnation,
    pub required_next_offset: u64,
    pub retained_start: u64,
}

impl Validate for RetentionGap {
    fn validate(&self) -> Result<(), InvalidError> {
        self.incarnation.validate()?;
        if self.required_next_offset >= self.retained_start {
            return Err(InvalidError::new(
                "retention gap requires next offset below retained start",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DestinationBlockCode {
    Decode,
    Schema,
    Projection,
    Value,
    Size,
    RetentionGap,
    PreparedAttempt,
    BackendGeneration,
    BackendUnavailable,
    TableIdentity,
    CatalogOutcomeUnknown,
    SourceIncarnation,
    Authorization,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationBlock {
    pub code: DestinationBlockCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incarnation: Option<SourceIncarnation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_ordinal: Option<u32>,
}

impl Validate for DestinationBlock {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.message.is_empty() || self.message.len() > MAX_CHECKPOINT_ERROR_BYTES {
            return Err(InvalidError::new(format!(
                "checkpoint error message must contain 1..={MAX_CHECKPOINT_ERROR_BYTES} bytes"
            )));
        }
        if let Some(incarnation) = &self.incarnation {
            incarnation.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CheckpointReadConsistency {
    Linearizable,
    #[default]
    PotentiallyStale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DestinationEffectiveState {
    Disabled,
    WaitingForBackend,
    Ready,
    Running,
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationCheckpointStatus {
    pub destination_id: DestinationId,
    pub destination_generation: u64,
    pub backend: BackendBinding,
    pub schema: LogicalSchemaRef,
    pub projection: ProjectionRef,
    pub global_state_revision: u64,
    pub definition_revision: u64,
    pub checkpoint_revision: u64,
    pub desired_state: DestinationDesiredState,
    pub effective_state: DestinationEffectiveState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table_uuid: Option<UuidValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<CheckpointOwnerLease>,
    pub partitions: Vec<PartitionCheckpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prepared_attempt: Option<PreparedAttemptSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_completion: Option<CompletedAttempt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_gap: Option<RetentionGap>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block: Option<DestinationBlock>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_repair: Option<RepairRecord>,
    pub consistency: CheckpointReadConsistency,
}

impl Validate for DestinationCheckpointStatus {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.destination_id.as_u128() == 0 || self.destination_generation == 0 {
            return Err(InvalidError::new(
                "checkpoint status destination identity must be nonzero",
            ));
        }
        self.backend.validate()?;
        self.schema.validate()?;
        self.projection.validate()?;
        if self.global_state_revision == 0 || self.definition_revision == 0 {
            return Err(InvalidError::new(
                "checkpoint status global and definition revisions must be nonzero",
            ));
        }
        if self.partitions.len() > MAX_CHECKPOINT_PARTITIONS {
            return Err(InvalidError::new(format!(
                "checkpoint status exceeds {MAX_CHECKPOINT_PARTITIONS} partitions"
            )));
        }
        let mut partitions = BTreeSet::new();
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
                    "checkpoint partitions must share one cluster, stream, and topic",
                ));
            }
            namespace = Some(current);
            if !partitions.insert(partition.incarnation.partition_id) {
                return Err(InvalidError::new(format!(
                    "checkpoint status repeats partition {}",
                    partition.incarnation.partition_id
                )));
            }
        }
        if let Some(owner) = &self.owner {
            owner.validate()?;
        }
        if let Some(table_uuid) = &self.table_uuid {
            validate_uuid(table_uuid)?;
        }
        if let Some(prepared) = &self.prepared_attempt {
            prepared.validate()?;
        }
        if let Some(completion) = &self.last_completion {
            completion.validate()?;
        }
        if let Some(gap) = &self.retention_gap {
            gap.validate()?;
        }
        if let Some(block) = &self.block {
            block.validate()?;
        }
        if let Some(repair) = &self.last_repair {
            repair.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CheckpointRequestEnvelope {
    pub v: u32,
    pub request_id: CheckpointRequestId,
    pub expected_global_state_revision: u64,
    pub mutation: PublicCheckpointMutation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervisor_assertion: Option<SupervisorActorAssertion>,
}

impl CheckpointRequestEnvelope {
    pub const fn new(
        request_id: CheckpointRequestId,
        expected_global_state_revision: u64,
        mutation: PublicCheckpointMutation,
    ) -> Self {
        Self {
            v: crate::codes::CHECKPOINT_OP_VERSION,
            request_id,
            expected_global_state_revision,
            mutation,
            supervisor_assertion: None,
        }
    }

    #[must_use]
    pub fn with_supervisor_assertion(mut self, assertion: SupervisorActorAssertion) -> Self {
        self.supervisor_assertion = Some(assertion);
        self
    }
}

impl Validate for CheckpointRequestEnvelope {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.v != crate::codes::CHECKPOINT_OP_VERSION {
            return Err(InvalidError::new(format!(
                "checkpoint version must be {}, got {}",
                crate::codes::CHECKPOINT_OP_VERSION,
                self.v
            )));
        }
        if self.request_id.as_u128() == 0 {
            return Err(InvalidError::new("checkpoint request id must be nonzero"));
        }
        self.mutation.validate()?;
        validate_supervisor_assertion(self)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum PublicCheckpointMutation {
    RegisterDestination {
        destination: MaterializationDestination,
    },
    RegisterQueryRoute {
        route: QueryRoute,
    },
    RemoveQueryRoute {
        route_id: QueryRouteId,
        route_generation: u64,
        expected_definition_revision: u64,
    },
    BindTable {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_definition_revision: u64,
        table_uuid: UuidValue,
    },
    SetDesiredState {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_definition_revision: u64,
        desired_state: DestinationDesiredState,
    },
    AddPartition {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        partition_id: u32,
    },
    ObservePartitionLifecycle {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        partition_id: u32,
    },
    AcquireLease {
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        expected_lease_sequence: u64,
        lease_duration_micros: u64,
    },
    RenewLease {
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        epoch: u64,
        expected_lease_sequence: u64,
        lease_duration_micros: u64,
    },
    TakeoverLease {
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        expected_lease_sequence: u64,
        lease_duration_micros: u64,
    },
    Prepare {
        expected_checkpoint_revision: u64,
        attempt: PreparedAttempt,
    },
    Complete {
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        epoch: u64,
        expected_checkpoint_revision: u64,
        completion: CompletedAttempt,
    },
    RecordBlock {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        block: DestinationBlock,
    },
    ClearBlock {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        expected_code: DestinationBlockCode,
    },
    RecordRetentionGap {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        gap: RetentionGap,
    },
    AcceptRetentionGap {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        next_offset: u64,
    },
    SupersedeGeneration {
        expected_definition_revision: u64,
        replacement: MaterializationDestination,
    },
    RecordRepair {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        repair: RepairRecord,
    },
}

impl Validate for PublicCheckpointMutation {
    fn validate(&self) -> Result<(), InvalidError> {
        match self {
            Self::RegisterDestination { destination } => destination.validate(),
            Self::RegisterQueryRoute { route } => route.validate(),
            Self::RemoveQueryRoute {
                route_id,
                route_generation,
                expected_definition_revision,
            } => {
                if route_id.as_u128() == 0
                    || *route_generation == 0
                    || *expected_definition_revision == 0
                {
                    return Err(InvalidError::new(
                        "query route identity, generation, and revision must be nonzero",
                    ));
                }
                Ok(())
            }
            Self::BindTable {
                destination_id,
                destination_generation,
                expected_definition_revision,
                table_uuid,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_definition_revision,
                    "definition",
                )?;
                validate_uuid(table_uuid)
            }
            Self::SetDesiredState {
                destination_id,
                destination_generation,
                expected_definition_revision,
                ..
            } => validate_destination_revision(
                *destination_id,
                *destination_generation,
                *expected_definition_revision,
                "definition",
            ),
            Self::AddPartition {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                ..
            }
            | Self::ObservePartitionLifecycle {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                ..
            }
            | Self::ClearBlock {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                ..
            }
            | Self::AcceptRetentionGap {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                ..
            } => validate_destination_revision(
                *destination_id,
                *destination_generation,
                *expected_checkpoint_revision,
                "checkpoint",
            ),
            Self::AcquireLease {
                destination_id,
                destination_generation,
                owner,
                lease_duration_micros,
                ..
            }
            | Self::TakeoverLease {
                destination_id,
                destination_generation,
                owner,
                lease_duration_micros,
                ..
            } => validate_lease_request(
                *destination_id,
                *destination_generation,
                *owner,
                *lease_duration_micros,
            ),
            Self::RenewLease {
                destination_id,
                destination_generation,
                owner,
                epoch,
                lease_duration_micros,
                ..
            } => {
                validate_lease_request(
                    *destination_id,
                    *destination_generation,
                    *owner,
                    *lease_duration_micros,
                )?;
                if *epoch == 0 {
                    return Err(InvalidError::new("lease epoch must be nonzero"));
                }
                Ok(())
            }
            Self::Prepare {
                expected_checkpoint_revision,
                attempt,
            } => {
                if *expected_checkpoint_revision == 0 {
                    return Err(InvalidError::new(
                        "expected checkpoint revision must be nonzero",
                    ));
                }
                attempt.validate()
            }
            Self::Complete {
                destination_id,
                destination_generation,
                owner,
                epoch,
                expected_checkpoint_revision,
                completion,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_checkpoint_revision,
                    "checkpoint",
                )?;
                if owner.as_u128() == 0 || *epoch == 0 {
                    return Err(InvalidError::new(
                        "completion owner and epoch must be nonzero",
                    ));
                }
                completion.validate()
            }
            Self::RecordBlock {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                block,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_checkpoint_revision,
                    "checkpoint",
                )?;
                block.validate()
            }
            Self::RecordRetentionGap {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                gap,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_checkpoint_revision,
                    "checkpoint",
                )?;
                gap.validate()
            }
            Self::SupersedeGeneration {
                expected_definition_revision,
                replacement,
            } => {
                if *expected_definition_revision == 0 {
                    return Err(InvalidError::new(
                        "expected definition revision must be nonzero",
                    ));
                }
                replacement.validate()
            }
            Self::RecordRepair {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                repair,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_checkpoint_revision,
                    "checkpoint",
                )?;
                repair.validate()
            }
        }
    }
}

impl PublicCheckpointMutation {
    pub const fn required_capability(&self) -> (Feature, Action) {
        match self {
            Self::RegisterDestination { .. }
            | Self::RegisterQueryRoute { .. }
            | Self::RemoveQueryRoute { .. }
            | Self::BindTable { .. }
            | Self::SetDesiredState { .. }
            | Self::AddPartition { .. }
            | Self::ObservePartitionLifecycle { .. } => (Feature::Destination, Action::Write),
            Self::AcceptRetentionGap { .. }
            | Self::SupersedeGeneration { .. }
            | Self::RecordRepair { .. } => (Feature::Checkpoint, Action::Admin),
            Self::AcquireLease { .. }
            | Self::RenewLease { .. }
            | Self::TakeoverLease { .. }
            | Self::Prepare { .. }
            | Self::Complete { .. }
            | Self::RecordBlock { .. }
            | Self::ClearBlock { .. }
            | Self::RecordRetentionGap { .. } => (Feature::Checkpoint, Action::Write),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PartitionLifecycleChange {
    Removed,
    Recreated,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairRecord {
    pub action: RepairAction,
    pub detail: String,
}

impl Validate for RepairRecord {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.detail.is_empty() || self.detail.len() > MAX_REPAIR_DETAIL_BYTES {
            return Err(InvalidError::new(format!(
                "repair detail must contain 1..={MAX_REPAIR_DETAIL_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum RepairAction {
    ReconciledPreparedAttempt,
    AcceptedRetentionGap,
    ClearedRetryableBlock,
    SupersededGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointMutationStamp {
    pub committed_at_micros: u64,
    pub iggy_actor_id: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supervisor_actor: Option<VerifiedSupervisorActor>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifiedSupervisorActor {
    pub request_id: CheckpointRequestId,
    pub deployment_id: u32,
    pub cloud_user_id: u32,
    pub action: SupervisorAssertionAction,
    pub destination_id: DestinationId,
    pub destination_generation: u64,
    pub expected_revision: u64,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub key_id: Vec<u8>,
    pub issued_at_micros: u64,
}

impl Validate for CheckpointMutationStamp {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.committed_at_micros == 0 {
            return Err(InvalidError::new(
                "checkpoint committed timestamp must be nonzero",
            ));
        }
        if let Some(actor) = &self.supervisor_actor
            && (actor.request_id.as_u128() == 0
                || actor.deployment_id == 0
                || actor.cloud_user_id == 0
                || actor.destination_id.as_u128() == 0
                || actor.destination_generation == 0
                || actor.expected_revision == 0
                || actor.issued_at_micros == 0
                || actor.key_id.len() != SUPERVISOR_ASSERTION_KEY_ID_BYTES)
        {
            return Err(InvalidError::new(
                "verified supervisor actor evidence is invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplicatedCheckpointMutation {
    pub request_id: CheckpointRequestId,
    pub expected_global_state_revision: u64,
    pub stamp: CheckpointMutationStamp,
    pub mutation: ReplicatedCheckpointMutationBody,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum ReplicatedCheckpointMutationBody {
    RegisterDestination {
        destination: MaterializationDestination,
    },
    RegisterQueryRoute {
        route: QueryRoute,
    },
    RemoveQueryRoute {
        route_id: QueryRouteId,
        route_generation: u64,
        expected_definition_revision: u64,
    },
    BindTable {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_definition_revision: u64,
        table_uuid: UuidValue,
    },
    Activate {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_definition_revision: u64,
        source_cut: crate::source::SourceCut,
    },
    Disable {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_definition_revision: u64,
    },
    AddPartition {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        partition: crate::source::SourcePartitionCut,
    },
    ObservePartitionLifecycle {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        incarnation: SourceIncarnation,
        change: PartitionLifecycleChange,
    },
    AcquireLease {
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        expected_lease_sequence: u64,
        deadline_micros: u64,
    },
    RenewLease {
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        epoch: u64,
        expected_lease_sequence: u64,
        deadline_micros: u64,
    },
    TakeoverLease {
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        expected_lease_sequence: u64,
        deadline_micros: u64,
    },
    Prepare {
        expected_checkpoint_revision: u64,
        attempt: PreparedAttempt,
    },
    Complete {
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        epoch: u64,
        expected_checkpoint_revision: u64,
        completion: CompletedAttempt,
    },
    RecordBlock {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        block: DestinationBlock,
    },
    ClearBlock {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        expected_code: DestinationBlockCode,
    },
    RecordRetentionGap {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        gap: RetentionGap,
    },
    AcceptRetentionGap {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        incarnation: SourceIncarnation,
        next_offset: u64,
        repair: RepairRecord,
    },
    SupersedeGeneration {
        expected_definition_revision: u64,
        replacement: MaterializationDestination,
        repair: RepairRecord,
    },
    RecordRepair {
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        repair: RepairRecord,
    },
}

impl Validate for ReplicatedCheckpointMutation {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.request_id.as_u128() == 0 {
            return Err(InvalidError::new(
                "replicated checkpoint request id must be nonzero",
            ));
        }
        self.stamp.validate()?;
        self.mutation.validate()
    }
}

impl Validate for ReplicatedCheckpointMutationBody {
    fn validate(&self) -> Result<(), InvalidError> {
        match self {
            Self::RegisterDestination { destination } => destination.validate(),
            Self::RegisterQueryRoute { route } => route.validate(),
            Self::RemoveQueryRoute {
                route_id,
                route_generation,
                expected_definition_revision,
            } => {
                if route_id.as_u128() == 0
                    || *route_generation == 0
                    || *expected_definition_revision == 0
                {
                    return Err(InvalidError::new(
                        "replicated query route identity and revision must be nonzero",
                    ));
                }
                Ok(())
            }
            Self::BindTable {
                destination_id,
                destination_generation,
                expected_definition_revision,
                table_uuid,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_definition_revision,
                    "definition",
                )?;
                validate_uuid(table_uuid)
            }
            Self::Activate {
                destination_id,
                destination_generation,
                expected_definition_revision,
                source_cut,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_definition_revision,
                    "definition",
                )?;
                source_cut.validate()
            }
            Self::Disable {
                destination_id,
                destination_generation,
                expected_definition_revision,
            } => validate_destination_revision(
                *destination_id,
                *destination_generation,
                *expected_definition_revision,
                "definition",
            ),
            Self::AddPartition {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                partition,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_checkpoint_revision,
                    "checkpoint",
                )?;
                partition.validate()
            }
            Self::ObservePartitionLifecycle {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                incarnation,
                ..
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_checkpoint_revision,
                    "checkpoint",
                )?;
                incarnation.validate()
            }
            Self::AcquireLease {
                destination_id,
                destination_generation,
                owner,
                deadline_micros,
                ..
            }
            | Self::TakeoverLease {
                destination_id,
                destination_generation,
                owner,
                deadline_micros,
                ..
            } => validate_lease_request(
                *destination_id,
                *destination_generation,
                *owner,
                *deadline_micros,
            ),
            Self::RenewLease {
                destination_id,
                destination_generation,
                owner,
                epoch,
                deadline_micros,
                ..
            } => {
                validate_lease_request(
                    *destination_id,
                    *destination_generation,
                    *owner,
                    *deadline_micros,
                )?;
                if *epoch == 0 {
                    return Err(InvalidError::new("replicated lease epoch must be nonzero"));
                }
                Ok(())
            }
            Self::Prepare {
                expected_checkpoint_revision,
                attempt,
            } => {
                if *expected_checkpoint_revision == 0 {
                    return Err(InvalidError::new(
                        "expected checkpoint revision must be nonzero",
                    ));
                }
                attempt.validate()
            }
            Self::Complete {
                destination_id,
                destination_generation,
                owner,
                epoch,
                expected_checkpoint_revision,
                completion,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_checkpoint_revision,
                    "checkpoint",
                )?;
                if owner.as_u128() == 0 || *epoch == 0 {
                    return Err(InvalidError::new(
                        "replicated completion owner and epoch must be nonzero",
                    ));
                }
                completion.validate()
            }
            Self::RecordBlock {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                block,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_checkpoint_revision,
                    "checkpoint",
                )?;
                block.validate()
            }
            Self::ClearBlock {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                ..
            } => validate_destination_revision(
                *destination_id,
                *destination_generation,
                *expected_checkpoint_revision,
                "checkpoint",
            ),
            Self::RecordRetentionGap {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                gap,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_checkpoint_revision,
                    "checkpoint",
                )?;
                gap.validate()
            }
            Self::AcceptRetentionGap {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                incarnation,
                repair,
                ..
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_checkpoint_revision,
                    "checkpoint",
                )?;
                incarnation.validate()?;
                repair.validate()
            }
            Self::SupersedeGeneration {
                expected_definition_revision,
                replacement,
                repair,
            } => {
                if *expected_definition_revision == 0 {
                    return Err(InvalidError::new(
                        "expected definition revision must be nonzero",
                    ));
                }
                replacement.validate()?;
                repair.validate()
            }
            Self::RecordRepair {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                repair,
            } => {
                validate_destination_revision(
                    *destination_id,
                    *destination_generation,
                    *expected_checkpoint_revision,
                    "checkpoint",
                )?;
                repair.validate()
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum CheckpointMutationResult {
    Destination {
        request_id: CheckpointRequestId,
        destination_id: DestinationId,
        destination_generation: u64,
        global_state_revision: u64,
        definition_revision: u64,
        checkpoint_revision: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lease: Option<CheckpointOwnerLease>,
    },
    QueryRoute {
        request_id: CheckpointRequestId,
        route_id: QueryRouteId,
        route_generation: u64,
        global_state_revision: u64,
        definition_revision: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[non_exhaustive]
pub enum CheckpointError {
    #[error("checkpoint request is invalid: {0}")]
    Invalid(String),
    #[error("checkpoint destination was not found")]
    NotFound,
    #[error("checkpoint mutation conflicts with revision {observed_revision}")]
    Conflict { observed_revision: u64 },
    #[error("checkpoint owner lease was lost")]
    LeaseLost,
    #[error("checkpoint mutation is unauthorized")]
    Unauthorized,
    #[error("checkpoint service is unavailable: {0}")]
    Unavailable(String),
    #[error("checkpoint version mismatch: expected {expected}, got {got}")]
    Version { expected: u32, got: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CheckpointReply {
    Ok(CheckpointMutationResult),
    Err(CheckpointError),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationGetRequest {
    pub v: u32,
    pub destination_id: DestinationId,
    pub consistency: CheckpointReadConsistency,
}

impl DestinationGetRequest {
    pub const fn new(
        destination_id: DestinationId,
        consistency: CheckpointReadConsistency,
    ) -> Self {
        Self {
            v: crate::codes::CHECKPOINT_OP_VERSION,
            destination_id,
            consistency,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationListFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_stream: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_topic: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_contains: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationListRequest {
    pub v: u32,
    #[serde(default)]
    pub filter: DestinationListFilter,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<DestinationId>,
    pub limit: u32,
    pub consistency: CheckpointReadConsistency,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRouteListRequest {
    pub v: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<QueryRouteId>,
    pub limit: u32,
    pub consistency: CheckpointReadConsistency,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationCheckpointView {
    pub destination: MaterializationDestination,
    pub status: DestinationCheckpointStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationCheckpointPage {
    pub destinations: Vec<DestinationCheckpointView>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after: Option<DestinationId>,
    pub global_state_revision: u64,
    pub consistency: CheckpointReadConsistency,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRoutePage {
    pub routes: Vec<QueryRoute>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_after: Option<QueryRouteId>,
    pub global_state_revision: u64,
    pub consistency: CheckpointReadConsistency,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CheckpointReadReply {
    Destination(Option<Box<DestinationCheckpointView>>),
    Destinations(DestinationCheckpointPage),
    QueryRoutes(QueryRoutePage),
    Err(CheckpointError),
}

impl Validate for DestinationGetRequest {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_checkpoint_read(self.v)?;
        if self.destination_id.as_u128() == 0 {
            return Err(InvalidError::new("destination id must be nonzero"));
        }
        Ok(())
    }
}

impl Validate for DestinationListRequest {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_checkpoint_read(self.v)?;
        validate_read_limit(self.limit)?;
        validate_optional_filter("source stream", self.filter.source_stream.as_deref())?;
        validate_optional_filter("source topic", self.filter.source_topic.as_deref())?;
        validate_optional_filter("destination name", self.filter.name_contains.as_deref())
    }
}

impl Validate for QueryRouteListRequest {
    fn validate(&self) -> Result<(), InvalidError> {
        validate_checkpoint_read(self.v)?;
        validate_read_limit(self.limit)?;
        validate_optional_filter("query route name", self.name_contains.as_deref())
    }
}

impl Validate for CheckpointMutationResult {
    fn validate(&self) -> Result<(), InvalidError> {
        match self {
            Self::Destination {
                request_id,
                destination_id,
                destination_generation,
                global_state_revision,
                definition_revision,
                lease,
                ..
            } => {
                if request_id.as_u128() == 0
                    || destination_id.as_u128() == 0
                    || *destination_generation == 0
                    || *global_state_revision == 0
                    || *definition_revision == 0
                {
                    return Err(InvalidError::new(
                        "checkpoint destination result identity, generation, global revision, and definition revision must be nonzero",
                    ));
                }
                if let Some(lease) = lease {
                    lease.validate()?;
                }
            }
            Self::QueryRoute {
                request_id,
                route_id,
                route_generation,
                global_state_revision,
                definition_revision,
            } => {
                if request_id.as_u128() == 0
                    || route_id.as_u128() == 0
                    || *route_generation == 0
                    || *global_state_revision == 0
                    || *definition_revision == 0
                {
                    return Err(InvalidError::new(
                        "checkpoint query route result identity, generation, global revision, and definition revision must be nonzero",
                    ));
                }
            }
        }
        Ok(())
    }
}

impl Validate for DestinationCheckpointView {
    fn validate(&self) -> Result<(), InvalidError> {
        self.destination.validate()?;
        self.status.validate()?;
        if self.destination.id != self.status.destination_id
            || self.destination.generation != self.status.destination_generation
            || self.destination.definition_revision != self.status.definition_revision
            || self.destination.backend != self.status.backend
            || self.destination.schema != self.status.schema
            || self.destination.projection != self.status.projection
        {
            return Err(InvalidError::new(
                "destination declaration and checkpoint status do not describe the same generation",
            ));
        }
        Ok(())
    }
}

impl Validate for DestinationCheckpointPage {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.global_state_revision == 0 || self.destinations.len() > crate::limits::MAX_PAGE_SIZE
        {
            return Err(InvalidError::new("destination page metadata is invalid"));
        }
        let mut ids = BTreeSet::new();
        for destination in &self.destinations {
            destination.validate()?;
            if destination.status.global_state_revision > self.global_state_revision
                || !ids.insert(destination.destination.id)
            {
                return Err(InvalidError::new(
                    "destination page contains duplicate or future state",
                ));
            }
        }
        Ok(())
    }
}

impl Validate for QueryRoutePage {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.global_state_revision == 0 || self.routes.len() > crate::limits::MAX_PAGE_SIZE {
            return Err(InvalidError::new("query route page metadata is invalid"));
        }
        let mut ids = BTreeSet::new();
        for route in &self.routes {
            route.validate()?;
            if !ids.insert(route.id) {
                return Err(InvalidError::new("query route page repeats a route id"));
            }
        }
        Ok(())
    }
}

impl Validate for CheckpointReadReply {
    fn validate(&self) -> Result<(), InvalidError> {
        match self {
            Self::Destination(Some(destination)) => destination.validate(),
            Self::Destination(None) | Self::Err(_) => Ok(()),
            Self::Destinations(page) => page.validate(),
            Self::QueryRoutes(page) => page.validate(),
        }
    }
}

fn validate_checkpoint_read(version: u32) -> Result<(), InvalidError> {
    if version != crate::codes::CHECKPOINT_OP_VERSION {
        return Err(InvalidError::new(format!(
            "checkpoint read version must be {}, got {version}",
            crate::codes::CHECKPOINT_OP_VERSION
        )));
    }
    Ok(())
}

fn validate_read_limit(limit: u32) -> Result<(), InvalidError> {
    if limit == 0 || limit > crate::limits::MAX_PAGE_SIZE as u32 {
        return Err(InvalidError::new(format!(
            "checkpoint read limit must be in 1..={}",
            crate::limits::MAX_PAGE_SIZE
        )));
    }
    Ok(())
}

fn validate_optional_filter(label: &str, value: Option<&str>) -> Result<(), InvalidError> {
    if let Some(value) = value
        && (value.is_empty() || value.len() > 255 || value.chars().any(char::is_control))
    {
        return Err(InvalidError::new(format!(
            "{label} filter must contain 1..=255 bytes without control characters"
        )));
    }
    Ok(())
}

fn validate_supervisor_assertion(envelope: &CheckpointRequestEnvelope) -> Result<(), InvalidError> {
    let required = match &envelope.mutation {
        PublicCheckpointMutation::AcceptRetentionGap {
            destination_id,
            destination_generation,
            expected_checkpoint_revision,
            ..
        } => Some((
            SupervisorAssertionAction::AcceptRetentionGap,
            *destination_id,
            *destination_generation,
            *expected_checkpoint_revision,
        )),
        PublicCheckpointMutation::SupersedeGeneration {
            expected_definition_revision,
            replacement,
        } => Some((
            SupervisorAssertionAction::SupersedeGeneration,
            replacement.id,
            replacement.generation,
            *expected_definition_revision,
        )),
        PublicCheckpointMutation::RecordRepair {
            destination_id,
            destination_generation,
            expected_checkpoint_revision,
            ..
        } => Some((
            SupervisorAssertionAction::RecordRepair,
            *destination_id,
            *destination_generation,
            *expected_checkpoint_revision,
        )),
        _ => None,
    };
    match (required, &envelope.supervisor_assertion) {
        (None, None) => Ok(()),
        (None, Some(_)) => Err(InvalidError::new(
            "supervisor assertion is not accepted for this checkpoint mutation",
        )),
        (Some(_), None) => Err(InvalidError::new(
            "high-risk checkpoint mutation requires a supervisor assertion",
        )),
        (
            Some((action, destination_id, destination_generation, expected_revision)),
            Some(assertion),
        ) => {
            assertion.validate()?;
            if assertion.claims.request_id != envelope.request_id
                || assertion.claims.action != action
                || assertion.claims.destination_id != destination_id
                || assertion.claims.destination_generation != destination_generation
                || assertion.claims.expected_revision != Some(expected_revision)
            {
                return Err(InvalidError::new(
                    "supervisor assertion is not bound to this request, action, and destination",
                ));
            }
            Ok(())
        }
    }
}

fn validate_destination_revision(
    destination_id: DestinationId,
    destination_generation: u64,
    revision: u64,
    revision_kind: &str,
) -> Result<(), InvalidError> {
    if destination_id.as_u128() == 0 || destination_generation == 0 {
        return Err(InvalidError::new(
            "checkpoint destination identity and generation must be nonzero",
        ));
    }
    if revision == 0 {
        return Err(InvalidError::new(format!(
            "expected {revision_kind} revision must be nonzero"
        )));
    }
    Ok(())
}

fn validate_uuid(value: &UuidValue) -> Result<(), InvalidError> {
    if value.as_bytes().len() != UuidValue::BYTES {
        return Err(InvalidError::new(format!(
            "table UUID must be {} bytes",
            UuidValue::BYTES
        )));
    }
    Ok(())
}

fn validate_lease_request(
    destination_id: DestinationId,
    destination_generation: u64,
    owner: CheckpointOwnerId,
    lease_duration_micros: u64,
) -> Result<(), InvalidError> {
    if destination_id.as_u128() == 0
        || destination_generation == 0
        || owner.as_u128() == 0
        || lease_duration_micros == 0
        || lease_duration_micros > MAX_CHECKPOINT_LEASE_DURATION_MICROS
    {
        return Err(InvalidError::new(
            "lease destination, owner, and generation must be nonzero, and duration must be within the configured cap",
        ));
    }
    Ok(())
}

fn validate_attempt_identity(
    attempt_id: PreparedAttemptId,
    destination_id: DestinationId,
    destination_generation: u64,
) -> Result<(), InvalidError> {
    if attempt_id.as_u128() == 0 {
        return Err(InvalidError::new("prepared attempt id must be nonzero"));
    }
    if destination_id.as_u128() == 0 || destination_generation == 0 {
        return Err(InvalidError::new(
            "prepared attempt destination identity must be nonzero",
        ));
    }
    Ok(())
}

fn validate_ranges(ranges: &[SourceOffsetRange]) -> Result<(), InvalidError> {
    if ranges.is_empty() || ranges.len() > MAX_CHECKPOINT_PARTITIONS {
        return Err(InvalidError::new(format!(
            "source ranges must contain 1..={MAX_CHECKPOINT_PARTITIONS} partitions"
        )));
    }
    let mut partitions = BTreeSet::new();
    let mut previous_partition = None;
    let mut namespace = None;
    for range in ranges {
        range.validate()?;
        let current = (
            range.incarnation.cluster,
            range.incarnation.stream_id,
            range.incarnation.topic_id,
        );
        if namespace.is_some_and(|expected| expected != current) {
            return Err(InvalidError::new(
                "source ranges must share one cluster, stream, and topic",
            ));
        }
        namespace = Some(current);
        if !partitions.insert(range.incarnation.partition_id) {
            return Err(InvalidError::new(format!(
                "source ranges repeat partition {}",
                range.incarnation.partition_id
            )));
        }
        if previous_partition.is_some_and(|previous| previous >= range.incarnation.partition_id) {
            return Err(InvalidError::new(
                "source ranges must be ordered by ascending partition id",
            ));
        }
        previous_partition = Some(range.incarnation.partition_id);
    }
    Ok(())
}

fn validate_resulting_boundary(
    boundary: &[PartitionCheckpoint],
    ranges: &[SourceOffsetRange],
) -> Result<(), InvalidError> {
    if boundary.len() != ranges.len() {
        return Err(InvalidError::new(
            "prepared attempt boundary must cover every source range exactly once",
        ));
    }
    for (range, checkpoint) in ranges.iter().zip(boundary) {
        checkpoint.validate()?;
        if checkpoint.incarnation != range.incarnation
            || checkpoint.next_offset != range.end_exclusive
            || checkpoint.lifecycle != PartitionLifecycleState::Active
        {
            return Err(InvalidError::new(
                "prepared attempt boundary does not match its source ranges",
            ));
        }
    }
    Ok(())
}

fn validate_credential_generations(
    generations: &[CredentialGeneration],
) -> Result<(), InvalidError> {
    if generations.is_empty() || generations.len() > MAX_CREDENTIAL_GENERATIONS {
        return Err(InvalidError::new(format!(
            "credential generations must contain 1..={MAX_CREDENTIAL_GENERATIONS} roles"
        )));
    }
    let mut roles = BTreeSet::new();
    for generation in generations {
        generation.validate()?;
        if !roles.insert(&generation.role) {
            return Err(InvalidError::new(format!(
                "credential role `{}` appears more than once",
                generation.role
            )));
        }
    }
    Ok(())
}

fn validate_object_identity(identity: &str) -> Result<(), InvalidError> {
    if identity.is_empty() || identity.len() > MAX_MANIFEST_IDENTITY_BYTES {
        return Err(InvalidError::new(format!(
            "object identity must contain 1..={MAX_MANIFEST_IDENTITY_BYTES} bytes"
        )));
    }
    if identity.contains('?')
        || identity.contains('#')
        || identity.contains('@')
        || identity.contains("//")
    {
        return Err(InvalidError::new(
            "object identity must be canonical, provider-relative, and secret-free",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "cbor")]
    use crate::destination::{
        BackendResourceId, DestinationErrorPolicy, FileFormat, NewPartitionPolicy, PhysicalTable,
        RecreatedPartitionPolicy, StartPolicy, TableFormat,
    };
    #[cfg(feature = "cbor")]
    use crate::framing::{decode_named, encode_named};
    #[cfg(feature = "cbor")]
    use crate::schema::{LogicalSchemaId, SchemaFingerprint};
    use crate::source::PhysicalClusterIncarnation;
    #[cfg(feature = "cbor")]
    use crate::source::SourceScope;

    fn incarnation(partition_id: u32) -> SourceIncarnation {
        SourceIncarnation {
            cluster: PhysicalClusterIncarnation::from_u128(1),
            stream_id: 2,
            topic_id: 3,
            partition_id,
            partition_created_revision: 4,
        }
    }

    #[cfg(feature = "cbor")]
    fn destination() -> MaterializationDestination {
        MaterializationDestination {
            id: DestinationId::from_u128(10),
            generation: 1,
            definition_revision: 1,
            name: "orders-lakehouse".to_owned(),
            source: SourceScope::new("shop", "orders"),
            recreated_partition_policy: RecreatedPartitionPolicy::Reject,
            projection: ProjectionRef {
                id: crate::control::ProjectionId::new("order.v1"),
                version: 1,
            },
            schema: LogicalSchemaRef {
                id: LogicalSchemaId::from_u128(11),
                version: 1,
                fingerprint: SchemaFingerprint::new([12; 32]),
            },
            backend: BackendBinding {
                resource_id: BackendResourceId::from_u128(13),
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

    fn table_requirements() -> PreparedTableRequirements {
        let table_uuid = UuidValue::new([14; 16]);
        let base_metadata_identity = "warehouse/shop/orders/metadata/00001.json".to_owned();
        PreparedTableRequirements {
            table_uuid: table_uuid.clone(),
            base_metadata_identity: base_metadata_identity.clone(),
            base_snapshot_id: Some(15),
            schema_id: 1,
            partition_spec_id: 0,
            commit_requirements: vec![
                IcebergCommitRequirement::AssertTableUuid { table_uuid },
                IcebergCommitRequirement::AssertMetadataIdentity {
                    identity: base_metadata_identity,
                },
                IcebergCommitRequirement::AssertCurrentSnapshot {
                    snapshot_id: Some(15),
                },
                IcebergCommitRequirement::AssertCurrentSchema { schema_id: 1 },
                IcebergCommitRequirement::AssertDefaultPartitionSpec {
                    partition_spec_id: 0,
                },
            ],
        }
    }

    #[cfg(feature = "cbor")]
    #[test]
    fn given_a_public_request_when_decoded_as_a_replicated_mutation_then_should_fail() {
        let request = CheckpointRequestEnvelope::new(
            CheckpointRequestId::from_u128(20),
            0,
            PublicCheckpointMutation::RegisterDestination {
                destination: destination(),
            },
        );
        request.validate().expect("public request is valid");
        let bytes = encode_named(&request).expect("public request encodes");
        assert!(decode_named::<ReplicatedCheckpointMutation>(&bytes).is_err());
    }

    #[cfg(feature = "cbor")]
    #[test]
    fn given_a_replicated_mutation_when_decoded_as_a_public_request_then_should_fail() {
        let mutation = ReplicatedCheckpointMutation {
            request_id: CheckpointRequestId::from_u128(21),
            expected_global_state_revision: 0,
            stamp: CheckpointMutationStamp {
                committed_at_micros: 22,
                iggy_actor_id: 23,
                supervisor_actor: None,
            },
            mutation: ReplicatedCheckpointMutationBody::RegisterDestination {
                destination: destination(),
            },
        };
        mutation.validate().expect("replicated mutation is valid");
        let bytes = encode_named(&mutation).expect("replicated mutation encodes");
        assert!(decode_named::<CheckpointRequestEnvelope>(&bytes).is_err());
    }

    #[test]
    fn given_prepared_table_requirements_when_validated_then_should_require_every_frozen_iceberg_precondition_in_order()
     {
        let valid = table_requirements();
        valid.validate().expect("requirements are complete");

        let mut missing = valid.clone();
        missing.commit_requirements.pop();
        assert!(missing.validate().is_err());

        let mut reordered = valid;
        reordered.commit_requirements.swap(0, 1);
        assert!(reordered.validate().is_err());
    }

    #[test]
    fn given_a_lease_duration_when_validated_then_should_be_bounded_before_managed_log_submission()
    {
        let mutation = PublicCheckpointMutation::AcquireLease {
            destination_id: DestinationId::from_u128(10),
            destination_generation: 1,
            owner: CheckpointOwnerId::from_u128(30),
            expected_lease_sequence: 0,
            lease_duration_micros: MAX_CHECKPOINT_LEASE_DURATION_MICROS + 1,
        };
        assert!(mutation.validate().is_err());
    }

    #[test]
    fn given_a_root_authored_stamp_when_validated_then_should_preserve_actor_zero() {
        let stamp = CheckpointMutationStamp {
            committed_at_micros: 1,
            iggy_actor_id: 0,
            supervisor_actor: None,
        };

        stamp
            .validate()
            .expect("root actor zero is a valid Iggy identity");
    }

    #[test]
    fn given_an_unserved_checkpoint_request_version_when_validated_then_should_reject_it() {
        let mut request = CheckpointRequestEnvelope::new(
            CheckpointRequestId::from_u128(1),
            0,
            PublicCheckpointMutation::RegisterDestination {
                destination: destination(),
            },
        );
        request.v = crate::codes::CHECKPOINT_OP_VERSION + 1;

        assert!(request.validate().is_err());
    }

    #[test]
    fn given_destination_and_query_route_results_when_validated_then_should_require_variant_specific_identity()
     {
        CheckpointMutationResult::Destination {
            request_id: CheckpointRequestId::from_u128(1),
            destination_id: DestinationId::from_u128(2),
            destination_generation: 3,
            global_state_revision: 4,
            definition_revision: 5,
            checkpoint_revision: 0,
            lease: None,
        }
        .validate()
        .expect("destination result is valid");
        CheckpointMutationResult::QueryRoute {
            request_id: CheckpointRequestId::from_u128(6),
            route_id: QueryRouteId::from_u128(7),
            route_generation: 8,
            global_state_revision: 9,
            definition_revision: 10,
        }
        .validate()
        .expect("query route result is valid");
    }

    #[test]
    fn given_source_ranges_when_out_of_canonical_partition_order_then_should_reject() {
        let ranges = vec![
            SourceOffsetRange {
                incarnation: incarnation(2),
                start: 0,
                end_exclusive: 1,
            },
            SourceOffsetRange {
                incarnation: incarnation(1),
                start: 0,
                end_exclusive: 1,
            },
        ];
        assert!(validate_ranges(&ranges).is_err());
    }
}

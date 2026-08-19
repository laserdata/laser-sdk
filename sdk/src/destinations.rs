use crate::error::LaserError;
use crate::laser::Laser;
use crate::types::MintUlid;
use laser_wire::authz::SupervisorActorAssertion;
use laser_wire::checkpoint::{
    CheckpointMutationResult, CheckpointOwnerId, CheckpointReadConsistency, CheckpointReadReply,
    CheckpointReply, CheckpointRequestEnvelope, CompletedAttempt, DestinationBlock,
    DestinationBlockCode, DestinationCheckpointPage, DestinationCheckpointView,
    DestinationGetRequest, DestinationListFilter, DestinationListRequest, PreparedAttempt,
    PublicCheckpointMutation, QueryRouteListRequest, QueryRoutePage, RepairRecord, RetentionGap,
};
use laser_wire::codes::{
    AGDX_CHECKPOINT_CODE, AGDX_DESTINATION_GET_CODE, AGDX_DESTINATION_LIST_CODE,
    AGDX_QUERY_ROUTE_LIST_CODE, CHECKPOINT_OP_VERSION,
};
use laser_wire::destination::{
    DestinationDesiredState, DestinationId, MaterializationDestination, QueryRoute, QueryRouteId,
};
use laser_wire::framing::encode_named;
use laser_wire::schema::UuidValue;
use laser_wire::validate::Validate;

impl Laser {
    pub fn destinations(&self) -> Destinations<'_> {
        Destinations { laser: self }
    }

    pub async fn execute_checkpoint(
        &self,
        request: CheckpointRequestEnvelope,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.require_checkpoint_capability().await?;
        request.validate()?;
        let payload = encode_named(&request)
            .map_err(|error| LaserError::Codec(format!("encode checkpoint request: {error}")))?;
        let payload = self
            .send_raw_with_response(AGDX_CHECKPOINT_CODE, payload)
            .await?;
        match crate::error::decode_managed_reply::<CheckpointReply>(&payload)? {
            CheckpointReply::Ok(result) => {
                result.validate()?;
                Ok(result)
            }
            CheckpointReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "checkpoint: unknown reply variant".to_owned(),
            )),
        }
    }

    async fn require_checkpoint_capability(&self) -> Result<(), LaserError> {
        let capabilities = self.capabilities().await;
        if !capabilities.destinations.available {
            return Err(LaserError::unsupported(
                "destinations",
                "destination and checkpoint operations are not served by this deployment",
            ));
        }
        if let Some(versions) = capabilities.versions
            && versions.checkpoint != CHECKPOINT_OP_VERSION
        {
            return Err(laser_wire::checkpoint::CheckpointError::Version {
                expected: versions.checkpoint,
                got: CHECKPOINT_OP_VERSION,
            }
            .into());
        }
        Ok(())
    }

    async fn checkpoint_read<R: serde::Serialize>(
        &self,
        code: u32,
        request: &R,
    ) -> Result<CheckpointReadReply, LaserError> {
        self.require_checkpoint_capability().await?;
        let payload = encode_named(request)
            .map_err(|error| LaserError::Codec(format!("encode checkpoint read: {error}")))?;
        let payload = self.send_raw_with_response(code, payload).await?;
        let reply: CheckpointReadReply = crate::error::decode_managed_reply(&payload)?;
        reply.validate()?;
        Ok(reply)
    }
}

pub struct Destinations<'a> {
    laser: &'a Laser,
}

impl Destinations<'_> {
    pub async fn mutate(
        &self,
        expected_global_state_revision: u64,
        mutation: PublicCheckpointMutation,
    ) -> Result<CheckpointMutationResult, LaserError> {
        let request = CheckpointRequestEnvelope::new(
            laser_wire::checkpoint::CheckpointRequestId::mint(),
            expected_global_state_revision,
            mutation,
        );
        self.laser.execute_checkpoint(request).await
    }

    pub async fn mutate_with_supervisor_assertion(
        &self,
        expected_global_state_revision: u64,
        mutation: PublicCheckpointMutation,
        supervisor_assertion: SupervisorActorAssertion,
    ) -> Result<CheckpointMutationResult, LaserError> {
        let request_id = supervisor_assertion.claims.request_id;
        let request =
            CheckpointRequestEnvelope::new(request_id, expected_global_state_revision, mutation)
                .with_supervisor_assertion(supervisor_assertion);
        self.laser.execute_checkpoint(request).await
    }

    pub async fn register(
        &self,
        expected_global_state_revision: u64,
        destination: MaterializationDestination,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::RegisterDestination { destination },
        )
        .await
    }

    pub async fn set_desired_state(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        expected_definition_revision: u64,
        desired_state: DestinationDesiredState,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::SetDesiredState {
                destination_id,
                destination_generation,
                expected_definition_revision,
                desired_state,
            },
        )
        .await
    }

    pub async fn bind_table(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        expected_definition_revision: u64,
        table_uuid: UuidValue,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::BindTable {
                destination_id,
                destination_generation,
                expected_definition_revision,
                table_uuid,
            },
        )
        .await
    }

    pub async fn add_partition(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        partition_id: u32,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::AddPartition {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                partition_id,
            },
        )
        .await
    }

    pub async fn observe_partition_lifecycle(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        partition_id: u32,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::ObservePartitionLifecycle {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                partition_id,
            },
        )
        .await
    }

    pub async fn acquire_lease(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        expected_lease_sequence: u64,
        lease_duration_micros: u64,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::AcquireLease {
                destination_id,
                destination_generation,
                owner,
                expected_lease_sequence,
                lease_duration_micros,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn renew_lease(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        epoch: u64,
        expected_lease_sequence: u64,
        lease_duration_micros: u64,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::RenewLease {
                destination_id,
                destination_generation,
                owner,
                epoch,
                expected_lease_sequence,
                lease_duration_micros,
            },
        )
        .await
    }

    pub async fn take_over_lease(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        expected_lease_sequence: u64,
        lease_duration_micros: u64,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::TakeoverLease {
                destination_id,
                destination_generation,
                owner,
                expected_lease_sequence,
                lease_duration_micros,
            },
        )
        .await
    }

    pub async fn prepare(
        &self,
        expected_global_state_revision: u64,
        expected_checkpoint_revision: u64,
        attempt: PreparedAttempt,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::Prepare {
                expected_checkpoint_revision,
                attempt,
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn complete(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        owner: CheckpointOwnerId,
        epoch: u64,
        expected_checkpoint_revision: u64,
        completion: CompletedAttempt,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::Complete {
                destination_id,
                destination_generation,
                owner,
                epoch,
                expected_checkpoint_revision,
                completion,
            },
        )
        .await
    }

    pub async fn record_block(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        block: DestinationBlock,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::RecordBlock {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                block,
            },
        )
        .await
    }

    pub async fn clear_block(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        expected_code: DestinationBlockCode,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::ClearBlock {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                expected_code,
            },
        )
        .await
    }

    pub async fn record_retention_gap(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        gap: RetentionGap,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::RecordRetentionGap {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                gap,
            },
        )
        .await
    }

    pub async fn accept_retention_gap(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        next_offset: u64,
        supervisor_assertion: SupervisorActorAssertion,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate_with_supervisor_assertion(
            expected_global_state_revision,
            PublicCheckpointMutation::AcceptRetentionGap {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                next_offset,
            },
            supervisor_assertion,
        )
        .await
    }

    pub async fn supersede_generation(
        &self,
        expected_global_state_revision: u64,
        expected_definition_revision: u64,
        replacement: MaterializationDestination,
        supervisor_assertion: SupervisorActorAssertion,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate_with_supervisor_assertion(
            expected_global_state_revision,
            PublicCheckpointMutation::SupersedeGeneration {
                expected_definition_revision,
                replacement,
            },
            supervisor_assertion,
        )
        .await
    }

    pub async fn record_repair(
        &self,
        expected_global_state_revision: u64,
        destination_id: DestinationId,
        destination_generation: u64,
        expected_checkpoint_revision: u64,
        repair: RepairRecord,
        supervisor_assertion: SupervisorActorAssertion,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate_with_supervisor_assertion(
            expected_global_state_revision,
            PublicCheckpointMutation::RecordRepair {
                destination_id,
                destination_generation,
                expected_checkpoint_revision,
                repair,
            },
            supervisor_assertion,
        )
        .await
    }

    pub async fn register_query_route(
        &self,
        expected_global_state_revision: u64,
        route: QueryRoute,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::RegisterQueryRoute { route },
        )
        .await
    }

    pub async fn remove_query_route(
        &self,
        expected_global_state_revision: u64,
        route_id: QueryRouteId,
        route_generation: u64,
        expected_definition_revision: u64,
    ) -> Result<CheckpointMutationResult, LaserError> {
        self.mutate(
            expected_global_state_revision,
            PublicCheckpointMutation::RemoveQueryRoute {
                route_id,
                route_generation,
                expected_definition_revision,
            },
        )
        .await
    }

    pub async fn get(
        &self,
        destination_id: DestinationId,
        consistency: CheckpointReadConsistency,
    ) -> Result<Option<DestinationCheckpointView>, LaserError> {
        let request = DestinationGetRequest::new(destination_id, consistency);
        request.validate()?;
        match self
            .laser
            .checkpoint_read(AGDX_DESTINATION_GET_CODE, &request)
            .await?
        {
            CheckpointReadReply::Destination(destination) => Ok(destination.map(|value| *value)),
            CheckpointReadReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "destination get: unexpected checkpoint reply".to_owned(),
            )),
        }
    }

    pub async fn list(
        &self,
        filter: DestinationListFilter,
        after: Option<DestinationId>,
        limit: usize,
        consistency: CheckpointReadConsistency,
    ) -> Result<DestinationCheckpointPage, LaserError> {
        let request = DestinationListRequest {
            v: CHECKPOINT_OP_VERSION,
            filter,
            after,
            limit: u32::try_from(limit).unwrap_or(u32::MAX),
            consistency,
        };
        request.validate()?;
        match self
            .laser
            .checkpoint_read(AGDX_DESTINATION_LIST_CODE, &request)
            .await?
        {
            CheckpointReadReply::Destinations(page) => Ok(page),
            CheckpointReadReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "destination list: unexpected checkpoint reply".to_owned(),
            )),
        }
    }

    pub async fn query_routes(
        &self,
        name_contains: Option<String>,
        after: Option<QueryRouteId>,
        limit: usize,
        consistency: CheckpointReadConsistency,
    ) -> Result<QueryRoutePage, LaserError> {
        let request = QueryRouteListRequest {
            v: CHECKPOINT_OP_VERSION,
            name_contains,
            after,
            limit: u32::try_from(limit).unwrap_or(u32::MAX),
            consistency,
        };
        request.validate()?;
        match self
            .laser
            .checkpoint_read(AGDX_QUERY_ROUTE_LIST_CODE, &request)
            .await?
        {
            CheckpointReadReply::QueryRoutes(page) => Ok(page),
            CheckpointReadReply::Err(error) => Err(error.into()),
            _ => Err(LaserError::Protocol(
                "query route list: unexpected checkpoint reply".to_owned(),
            )),
        }
    }
}

use crate::error::LaserError;
use crate::laser::Laser;
use crate::types::MintUlid;
use laser_wire::checkpoint::{
    CheckpointMutationResult, CheckpointReadConsistency, CheckpointReadReply, CheckpointReply,
    CheckpointRequestEnvelope, DestinationCheckpointPage, DestinationCheckpointView,
    DestinationGetRequest, DestinationListFilter, DestinationListRequest, PublicCheckpointMutation,
    QueryRouteListRequest, QueryRoutePage,
};
use laser_wire::codes::{
    AGDX_CHECKPOINT_CODE, AGDX_DESTINATION_GET_CODE, AGDX_DESTINATION_LIST_CODE,
    AGDX_QUERY_ROUTE_LIST_CODE, CHECKPOINT_OP_VERSION,
};
use laser_wire::destination::{
    DestinationDesiredState, DestinationId, MaterializationDestination, QueryRoute, QueryRouteId,
};
use laser_wire::framing::encode_named;
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

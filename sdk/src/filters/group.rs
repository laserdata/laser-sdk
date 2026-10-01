use super::reader::SourceIncarnation;
use crate::error::LaserError;
use crate::iggy::prelude::{ConsumerGroupClient, IggyClient};
use iggy_binary_protocol::codes::SYNC_CONSUMER_GROUP_CODE;
use iggy_binary_protocol::requests::consumer_groups::SyncConsumerGroupRequest;
use iggy_binary_protocol::responses::consumer_groups::SyncConsumerGroupResponse;
use iggy_binary_protocol::{WireDecode, WireEncode};
use iggy_common::Identifier;
use iggy_common::wire_conversions::identifier_to_wire;
use laser_wire::filter::{FilterConsumer, FilterError, FilterErrorReason, FilterSource};
use std::time::Duration;
use tokio::time::Instant;

/// How often a member re-reads its assignment even when every read succeeds,
/// so a member holding no partitions still discovers a new one.
pub(crate) const ASSIGNMENT_REFRESH: Duration = Duration::from_secs(2);

/// This reader's membership of a consumer group, held by the reader's own
/// coordinator connection, so two readers are two members and a dropped reader
/// leaves with its connection. The coordinator assigns partitions. A filtered
/// read of a partition the member does not own is fenced by the server.
pub(crate) struct Membership {
    stream: Identifier,
    topic: Identifier,
    group: Identifier,
    generation: u64,
    partitions: Vec<u32>,
    synced_at: Option<Instant>,
    rejoined: bool,
    source: FilterSource,
    pinned_source: Option<SourceIncarnation>,
    source_replaced: bool,
}

impl Membership {
    /// Join an existing consumer group and read the first assignment.
    pub(crate) async fn join(
        coordinator: &IggyClient,
        source: &FilterSource,
        consumer: &FilterConsumer,
        pinned_source: Option<SourceIncarnation>,
    ) -> Result<Self, LaserError> {
        let mut membership = Self {
            stream: Identifier::named(&source.stream)?,
            topic: Identifier::named(&source.topic)?,
            group: match consumer {
                FilterConsumer::Group(name) => Identifier::named(name)?,
                FilterConsumer::GroupId(id) => Identifier::numeric(
                    u32::try_from(*id)
                        .map_err(|_| LaserError::Config("consumer group id exceeds 32 bits"))?,
                )?,
                FilterConsumer::Consumer(_) => {
                    return Err(LaserError::Config("membership requires a group"));
                }
            },
            generation: 0,
            partitions: Vec::new(),
            synced_at: None,
            rejoined: false,
            source: source.clone(),
            pinned_source,
            source_replaced: false,
        };
        coordinator
            .join_consumer_group(&membership.stream, &membership.topic, &membership.group)
            .await?;
        membership.sync(coordinator).await?;
        Ok(membership)
    }

    pub(crate) fn partitions(&self) -> &[u32] {
        &self.partitions
    }

    pub(crate) fn is_due(&self) -> bool {
        self.synced_at
            .is_none_or(|synced_at| synced_at.elapsed() >= ASSIGNMENT_REFRESH)
    }

    /// Re-read the assignment before the next read, because the server fenced
    /// one.
    pub(crate) const fn expire(&mut self) {
        self.synced_at = None;
    }

    /// Read the current assignment. `true` when it changed.
    pub(crate) async fn sync(&mut self, coordinator: &IggyClient) -> Result<bool, LaserError> {
        let request = SyncConsumerGroupRequest {
            stream_id: identifier_to_wire(&self.stream)?,
            topic_id: identifier_to_wire(&self.topic)?,
            group_id: identifier_to_wire(&self.group)?,
        };
        let mut reply = coordinator
            .send_binary_request(SYNC_CONSUMER_GROUP_CODE, request.to_bytes())
            .await?;
        if reply.is_empty() {
            if let Some(pinned) = self.pinned_source
                && SourceIncarnation::read(coordinator, &self.source).await? != pinned
            {
                let _ = self.leave(coordinator).await;
                self.source_replaced = true;
                return Err(FilterError::new(
                    FilterErrorReason::NotFound,
                    "the numeric consumer group belongs to a replaced stream or topic",
                )
                .into());
            }
            coordinator
                .join_consumer_group(&self.stream, &self.topic, &self.group)
                .await?;
            self.rejoined = true;
            reply = coordinator
                .send_binary_request(SYNC_CONSUMER_GROUP_CODE, request.to_bytes())
                .await?;
            if reply.is_empty() {
                return Err(FilterError::new(
                    FilterErrorReason::MembershipStale,
                    "the consumer group has not accepted this reader's rejoin",
                )
                .into());
            }
        }
        self.synced_at = Some(Instant::now());
        let assignment = SyncConsumerGroupResponse::decode_from(&reply).map_err(|error| {
            LaserError::Protocol(format!("decode the group assignment: {error}"))
        })?;
        let changed = self.rejoined
            || assignment.generation != self.generation
            || assignment.partitions != self.partitions;
        self.generation = assignment.generation;
        self.partitions = assignment.partitions;
        Ok(changed)
    }

    pub(crate) fn take_rejoined(&mut self) -> bool {
        std::mem::take(&mut self.rejoined)
    }

    pub(crate) async fn leave(&self, coordinator: &IggyClient) -> Result<(), LaserError> {
        if self.source_replaced {
            return Ok(());
        }
        coordinator
            .leave_consumer_group(&self.stream, &self.topic, &self.group)
            .await
            .map_err(LaserError::from)
    }
}

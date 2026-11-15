use crate::error::LaserError;
use crate::laser::{Laser, delete_topic, forget_stream, forget_topic, is_missing_resource};
use crate::stream::{Stream, Topic};
use iggy::prelude::{
    ConsumerGroup, ConsumerGroupClient, Identifier, IggyError, IggyExpiry, MaxTopicSize, Partition,
    PartitionClient, ResourceOptions, SegmentClient, StreamClient, StreamDetails,
    StreamUpdateOptions, TopicClient, TopicCreateOptions, TopicResolvedOptions, TopicUpdateOptions,
};
use iggy_common::DEFAULT_MAX_TOPIC_SIZE;
use std::time::Duration;

/// One stream as the server reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamInfo {
    /// The native numeric stream id.
    pub id: u32,
    pub name: String,
    pub topics_count: u32,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub created_at_micros: u64,
}

/// One topic as the server reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicInfo {
    /// The native numeric topic id.
    pub id: u32,
    pub name: String,
    pub partitions_count: u32,
    /// How many nodes store each partition, `None` when the server reports no factor.
    pub replication_factor: Option<u32>,
    /// How long a message is kept, `None` when messages never expire.
    pub message_expiry: Option<Duration>,
    /// The retained size cap, `None` when the topic is unlimited.
    pub max_size_bytes: Option<u64>,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub created_at_micros: u64,
}

/// One partition of a topic as the server reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionInfo {
    pub id: u32,
    pub segments_count: u32,
    /// The offset of the newest message, 0 for an empty partition.
    pub current_offset: u64,
    pub size_bytes: u64,
    pub messages_count: u64,
    pub created_at_micros: u64,
}

/// One consumer group of a topic as the topic listing reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerGroupSummary {
    /// The native numeric group id.
    pub id: u32,
    pub name: String,
    pub partitions_count: u32,
    pub members_count: u32,
}

/// The changes [`Topic::update`] applies. A field left unset keeps its value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UpdateTopicOptions {
    pub name: Option<String>,
    /// Keep each message this long.
    pub expire_after: Option<Duration>,
    /// Keep messages until the size cap removes them.
    pub never_expire: bool,
    /// Cap the retained size of the topic in bytes.
    pub max_topic_bytes: Option<u64>,
    /// Remove the size cap.
    pub unlimited_topic_size: bool,
}

impl TryFrom<UpdateTopicOptions> for TopicUpdateOptions {
    type Error = LaserError;

    fn try_from(options: UpdateTopicOptions) -> Result<Self, Self::Error> {
        let message_expiry = match (options.expire_after, options.never_expire) {
            (Some(_), true) => {
                return Err(LaserError::Invalid(
                    "set expire_after or never_expire, not both".to_owned(),
                ));
            }
            (Some(expiry), false) => Some(IggyExpiry::ExpireDuration(expiry.into())),
            (None, true) => Some(IggyExpiry::NeverExpire),
            (None, false) => None,
        };
        let max_topic_size = match (options.max_topic_bytes, options.unlimited_topic_size) {
            (Some(_), true) => {
                return Err(LaserError::Invalid(
                    "set max_topic_bytes or unlimited_topic_size, not both".to_owned(),
                ));
            }
            (Some(bytes), false) => Some(MaxTopicSize::from(bytes)),
            (None, true) => Some(MaxTopicSize::Unlimited),
            (None, false) => None,
        };
        Ok(Self {
            name: options.name,
            message_expiry,
            max_topic_size,
            ..Self::default()
        })
    }
}

impl From<iggy::prelude::Stream> for StreamInfo {
    fn from(stream: iggy::prelude::Stream) -> Self {
        Self {
            id: stream.id,
            name: stream.name,
            topics_count: stream.topics_count,
            size_bytes: stream.size.as_bytes_u64(),
            messages_count: stream.messages_count,
            created_at_micros: stream.created_at.as_micros(),
        }
    }
}

impl From<StreamDetails> for StreamInfo {
    fn from(stream: StreamDetails) -> Self {
        Self {
            id: stream.id,
            name: stream.name,
            topics_count: stream.topics_count,
            size_bytes: stream.size.as_bytes_u64(),
            messages_count: stream.messages_count,
            created_at_micros: stream.created_at.as_micros(),
        }
    }
}

impl From<iggy::prelude::Topic> for TopicInfo {
    fn from(topic: iggy::prelude::Topic) -> Self {
        Self {
            id: topic.id,
            name: topic.name,
            partitions_count: topic.partitions_count,
            replication_factor: None,
            message_expiry: None,
            max_size_bytes: None,
            size_bytes: topic.size.as_bytes_u64(),
            messages_count: topic.messages_count,
            created_at_micros: topic.created_at.as_micros(),
        }
        .with_options(&topic.options)
    }
}

impl From<iggy::prelude::TopicDetails> for TopicInfo {
    fn from(topic: iggy::prelude::TopicDetails) -> Self {
        Self {
            id: topic.id,
            name: topic.name,
            partitions_count: topic.partitions_count,
            replication_factor: None,
            message_expiry: None,
            max_size_bytes: None,
            size_bytes: topic.size.as_bytes_u64(),
            messages_count: topic.messages_count,
            created_at_micros: topic.created_at.as_micros(),
        }
        .with_options(&topic.options)
    }
}

impl TopicInfo {
    fn with_options(self, options: &ResourceOptions) -> Self {
        let resolved = TopicResolvedOptions::from(options);
        Self {
            replication_factor: TopicCreateOptions::from_resource_options(options)
                .replication_factor,
            message_expiry: match resolved.message_expiry {
                IggyExpiry::ExpireDuration(age) => Some(age.get_duration()),
                IggyExpiry::NeverExpire | IggyExpiry::ServerDefault => None,
            },
            max_size_bytes: resolved.max_topic_size.resolve(DEFAULT_MAX_TOPIC_SIZE),
            ..self
        }
    }
}

impl From<Partition> for PartitionInfo {
    fn from(partition: Partition) -> Self {
        Self {
            id: partition.id,
            segments_count: partition.segments_count,
            current_offset: partition.current_offset,
            size_bytes: partition.size.as_bytes_u64(),
            messages_count: partition.messages_count,
            created_at_micros: partition.created_at.as_micros(),
        }
    }
}

impl From<ConsumerGroup> for ConsumerGroupSummary {
    fn from(group: ConsumerGroup) -> Self {
        Self {
            id: group.id,
            name: group.name,
            partitions_count: group.partitions_count,
            members_count: group.members_count,
        }
    }
}

impl Laser {
    /// Every stream this connection's user can read.
    pub async fn streams(&self) -> Result<Vec<StreamInfo>, LaserError> {
        let streams = self.client().get_streams().await?;
        Ok(streams.into_iter().map(StreamInfo::from).collect())
    }
}

impl Stream {
    /// This stream as the server reports it, `None` when it does not exist.
    pub async fn get(&self) -> Result<Option<StreamInfo>, LaserError> {
        let identifier = Identifier::named(&self.name)?;
        match self.laser.client().get_stream(&identifier).await {
            Ok(found) => Ok(found.map(StreamInfo::from)),
            Err(error) if is_missing_resource(&error) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Whether this stream exists.
    pub async fn exists(&self) -> Result<bool, LaserError> {
        Ok(self.get().await?.is_some())
    }

    /// Rename this stream. Cached producers for the old name are dropped.
    /// After the rename, address the stream by its new name.
    pub async fn rename(&self, name: impl Into<String>) -> Result<(), LaserError> {
        let update = StreamUpdateOptions {
            name: Some(name.into()),
            ..StreamUpdateOptions::default()
        };
        self.laser
            .client()
            .update_stream(&Identifier::named(&self.name)?, &update)
            .await?;
        forget_stream(&self.laser, &self.name);
        Ok(())
    }

    /// Every topic of this stream. A missing stream fails with an error
    /// [`LaserError::is_stream_or_topic_not_found`] recognizes.
    pub async fn topics(&self) -> Result<Vec<TopicInfo>, LaserError> {
        let identifier = Identifier::named(&self.name)?;
        let topics = self.laser.client().get_topics(&identifier).await?;
        // The server lists a missing stream as empty.
        if topics.is_empty() && self.get().await?.is_none() {
            return Err(IggyError::StreamNameNotFound(self.name.clone()).into());
        }
        Ok(topics.into_iter().map(TopicInfo::from).collect())
    }
}

impl Topic {
    /// This topic as the server reports it, `None` when it or its stream does
    /// not exist.
    pub async fn get(&self) -> Result<Option<TopicInfo>, LaserError> {
        Ok(self.details().await?.map(TopicInfo::from))
    }

    /// Whether this topic exists.
    pub async fn exists(&self) -> Result<bool, LaserError> {
        Ok(self.details().await?.is_some())
    }

    /// Delete this topic with every message in it. Returns `false` when the
    /// topic or its stream did not exist. Cached producers for the topic are
    /// dropped.
    pub async fn delete(&self) -> Result<bool, LaserError> {
        delete_topic(&self.laser, self.stream()?, &self.name).await
    }

    /// Change the name, retention, or size cap of this topic. A field left
    /// unset keeps its value. Cached producers for the topic are dropped.
    /// After a rename, address the topic by its new name.
    pub async fn update(&self, options: UpdateTopicOptions) -> Result<(), LaserError> {
        let update = TopicUpdateOptions::try_from(options)?;
        let stream = Identifier::named(self.stream()?)?;
        let topic = Identifier::named(&self.name)?;
        self.laser
            .client()
            .update_topic(&stream, &topic, &update)
            .await?;
        forget_topic(&self.laser, self.stream()?, &self.name);
        Ok(())
    }

    /// Delete the oldest `count` segments of one partition with their
    /// messages. The open segment stays.
    pub async fn delete_segments(&self, partition_id: u32, count: u32) -> Result<(), LaserError> {
        let (stream, topic) = self.identifiers(count)?;
        self.laser
            .client()
            .delete_segments(&stream, &topic, partition_id, count)
            .await?;
        Ok(())
    }

    /// Every partition of this topic. A missing topic fails with an error
    /// [`LaserError::is_stream_or_topic_not_found`] recognizes.
    pub async fn partitions(&self) -> Result<Vec<PartitionInfo>, LaserError> {
        let details = self.details().await?.ok_or_else(|| self.missing())?;
        Ok(details
            .partitions
            .into_iter()
            .map(PartitionInfo::from)
            .collect())
    }

    /// Add `count` partitions after the existing ones. A topic created with a
    /// fixed partition layout refuses the change.
    pub async fn add_partitions(&self, count: u32) -> Result<(), LaserError> {
        let (stream, topic) = self.identifiers(count)?;
        self.laser
            .client()
            .create_partitions(&stream, &topic, count)
            .await?;
        forget_topic(&self.laser, self.stream()?, &self.name);
        Ok(())
    }

    /// Delete the last `count` partitions with their messages. A topic created
    /// with a fixed partition layout refuses the change.
    pub async fn delete_partitions(&self, count: u32) -> Result<(), LaserError> {
        let (stream, topic) = self.identifiers(count)?;
        self.laser
            .client()
            .delete_partitions(&stream, &topic, count)
            .await?;
        forget_topic(&self.laser, self.stream()?, &self.name);
        Ok(())
    }

    /// Every consumer group of this topic. A missing topic fails with an
    /// error [`LaserError::is_stream_or_topic_not_found`] recognizes.
    pub async fn consumer_groups(&self) -> Result<Vec<ConsumerGroupSummary>, LaserError> {
        let stream = Identifier::named(self.stream()?)?;
        let topic = Identifier::named(&self.name)?;
        let groups = self
            .laser
            .client()
            .get_consumer_groups(&stream, &topic)
            .await?;
        // The server lists a missing topic as empty.
        if groups.is_empty() && self.details().await?.is_none() {
            return Err(self.missing());
        }
        Ok(groups.into_iter().map(ConsumerGroupSummary::from).collect())
    }

    async fn details(&self) -> Result<Option<iggy::prelude::TopicDetails>, LaserError> {
        let stream = Identifier::named(self.stream()?)?;
        let topic = Identifier::named(&self.name)?;
        match self.laser.client().get_topic(&stream, &topic).await {
            Ok(found) => Ok(found),
            Err(error) if is_missing_resource(&error) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn identifiers(&self, count: u32) -> Result<(Identifier, Identifier), LaserError> {
        if count == 0 {
            return Err(LaserError::Invalid(
                "count must be greater than zero".to_owned(),
            ));
        }
        Ok((
            Identifier::named(self.stream()?)?,
            Identifier::named(&self.name)?,
        ))
    }

    fn missing(&self) -> LaserError {
        IggyError::TopicNameNotFound(self.name.clone(), self.stream.clone().unwrap_or_default())
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::{IggyByteSize, IggyTimestamp};

    #[test]
    fn given_a_listed_topic_without_options_when_converted_then_should_report_no_expiry_or_cap() {
        let info = TopicInfo::from(iggy::prelude::Topic {
            id: 7,
            created_at: IggyTimestamp::from(1_700_000_000_123_456),
            name: "metrics".to_owned(),
            size: IggyByteSize::from(2048),
            messages_count: 12,
            partitions_count: 3,
            options: ResourceOptions::default(),
        });
        assert_eq!(
            info,
            TopicInfo {
                id: 7,
                name: "metrics".to_owned(),
                partitions_count: 3,
                replication_factor: None,
                message_expiry: None,
                max_size_bytes: None,
                size_bytes: 2048,
                messages_count: 12,
                created_at_micros: 1_700_000_000_123_456,
            }
        );
    }

    #[test]
    fn given_a_stream_when_converted_then_should_keep_microsecond_creation_time() {
        let info = StreamInfo::from(iggy::prelude::Stream {
            id: 3,
            created_at: IggyTimestamp::from(1_700_000_000_000_001),
            name: "hosts".to_owned(),
            size: IggyByteSize::from(10),
            messages_count: 1,
            topics_count: 2,
            options: ResourceOptions::default(),
        });
        assert_eq!(info.created_at_micros, 1_700_000_000_000_001);
        assert_eq!(info.topics_count, 2);
        assert_eq!(info.size_bytes, 10);
    }

    #[tokio::test]
    async fn given_a_zero_partition_count_when_partitions_added_then_should_refuse_before_sending()
    {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let error = laser
            .stream("hosts")
            .topic("metrics")
            .add_partitions(0)
            .await
            .expect_err("a zero count is refused");
        assert!(matches!(error, LaserError::Invalid(_)));
    }

    #[test]
    fn given_an_expiry_and_never_expire_when_a_topic_update_converts_then_should_refuse() {
        let options = UpdateTopicOptions {
            expire_after: Some(Duration::from_secs(60)),
            never_expire: true,
            ..UpdateTopicOptions::default()
        };
        assert!(matches!(
            TopicUpdateOptions::try_from(options),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_an_empty_topic_update_when_converted_then_should_keep_every_setting() {
        let update = TopicUpdateOptions::try_from(UpdateTopicOptions::default())
            .expect("an empty update converts");
        assert!(update.name.is_none());
        assert!(update.message_expiry.is_none());
        assert!(update.max_topic_size.is_none());
    }

    #[tokio::test]
    async fn given_no_default_stream_when_topic_inspected_then_should_fail_with_no_stream() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let error = laser
            .topic("metrics")
            .get()
            .await
            .expect_err("a topic without a stream is refused");
        assert!(matches!(error, LaserError::NoStream));
    }
}

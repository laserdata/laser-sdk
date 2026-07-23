use crate::error::LaserError;
use crate::stream::{HeaderKey, HeaderValue, Topic};
use crate::types::MessageId;
use bytes::Bytes;
use futures::Stream;
use iggy::prelude::{
    AutoCommit, AutoCommitWhen, BackgroundConfig, ConsumerGroupClient, DirectConfig, Identifier,
    IggyConsumer, IggyConsumerBuilder, IggyDuration, IggyExpiry, IggyMessage, IggyProducer,
    IggyTimestamp, MaxTopicSize, Partitioning, PollingStrategy, ReceivedMessage,
};
use serde::de::DeserializeOwned;
use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

const DEFAULT_BATCH_LENGTH: u32 = 1000;
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Exact Apache Iggy user headers, preserving each value's wire type.
pub type Headers = BTreeMap<HeaderKey, HeaderValue>;

/// Per-send partition selection.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Routing {
    /// Let Apache Iggy balance records across the topic partitions.
    #[default]
    Balanced,
    /// Hash a non-empty key, preserving order for records with the same key.
    Key(Vec<u8>),
    /// Send directly to one partition.
    Partition(u32),
}

impl Routing {
    /// Route records by a stable, non-empty key.
    pub fn key(value: impl Into<Vec<u8>>) -> Self {
        Self::Key(value.into())
    }

    fn into_partitioning(self) -> Result<Partitioning, LaserError> {
        match self {
            Self::Balanced => Ok(Partitioning::balanced()),
            Self::Key(key) => Ok(Partitioning::messages_key(&key)?),
            Self::Partition(partition) => Ok(Partitioning::partition_id(partition)),
        }
    }
}

/// Polling position for a consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConsumerStart {
    /// Begin at the first available record.
    First,
    /// Begin at the last available record.
    Last,
    /// Resume after the server-stored consumer offset.
    #[default]
    Next,
    /// Begin at an absolute log offset.
    Offset(u64),
    /// Begin at a microsecond Unix timestamp.
    TimestampMicros(u64),
}

impl ConsumerStart {
    fn into_polling(self) -> PollingStrategy {
        match self {
            Self::First => PollingStrategy::first(),
            Self::Last => PollingStrategy::last(),
            Self::Next => PollingStrategy::next(),
            Self::Offset(offset) => PollingStrategy::offset(offset),
            Self::TimestampMicros(timestamp) => {
                PollingStrategy::timestamp(IggyTimestamp::from(timestamp))
            }
        }
    }
}

/// Server offset storage policy for a live consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CommitPolicy {
    /// Never store offsets automatically. Call [`Consumer::commit`] after a
    /// record has been handled successfully.
    Disabled,
    /// Store offsets on a fixed interval.
    Interval(Duration),
    /// Store the previous batch offset before polling again.
    #[default]
    Polling,
    /// Store on an interval or before polling, whichever happens first.
    IntervalOrPolling(Duration),
    /// Store after consuming all records returned by a poll.
    All,
    /// Store on an interval or after consuming a full poll result.
    IntervalOrAll(Duration),
    /// Store after every yielded record.
    Each,
    /// Store on an interval or after every yielded record.
    IntervalOrEach(Duration),
    /// Store after every `n` yielded records.
    Every(u32),
    /// Store on an interval or after every `n` yielded records.
    IntervalOrEvery(Duration, u32),
}

impl CommitPolicy {
    fn into_auto_commit(self) -> Result<AutoCommit, LaserError> {
        let interval = |duration: Duration| {
            if duration.is_zero() {
                Err(LaserError::Invalid(
                    "commit interval must be greater than zero".to_owned(),
                ))
            } else {
                Ok(IggyDuration::from(duration))
            }
        };
        let every = |messages: u32| {
            if messages == 0 {
                Err(LaserError::Invalid(
                    "commit frequency must be greater than zero".to_owned(),
                ))
            } else {
                Ok(AutoCommitWhen::ConsumingEveryNthMessage(messages))
            }
        };
        Ok(match self {
            Self::Disabled => AutoCommit::Disabled,
            Self::Interval(duration) => AutoCommit::Interval(interval(duration)?),
            Self::Polling => AutoCommit::When(AutoCommitWhen::PollingMessages),
            Self::IntervalOrPolling(duration) => {
                AutoCommit::IntervalOrWhen(interval(duration)?, AutoCommitWhen::PollingMessages)
            }
            Self::All => AutoCommit::When(AutoCommitWhen::ConsumingAllMessages),
            Self::IntervalOrAll(duration) => AutoCommit::IntervalOrWhen(
                interval(duration)?,
                AutoCommitWhen::ConsumingAllMessages,
            ),
            Self::Each => AutoCommit::When(AutoCommitWhen::ConsumingEachMessage),
            Self::IntervalOrEach(duration) => AutoCommit::IntervalOrWhen(
                interval(duration)?,
                AutoCommitWhen::ConsumingEachMessage,
            ),
            Self::Every(messages) => AutoCommit::When(every(messages)?),
            Self::IntervalOrEvery(duration, messages) => {
                AutoCommit::IntervalOrWhen(interval(duration)?, every(messages)?)
            }
        })
    }
}

/// A raw streaming record with optional exact-width user headers.
#[derive(Debug, bon::Builder)]
pub struct ProducerMessage {
    #[builder(into)]
    payload: Bytes,
    #[builder(default)]
    headers: Headers,
}

impl ProducerMessage {
    /// Create a record without user headers.
    pub fn new(payload: impl Into<Bytes>) -> Self {
        Self {
            payload: payload.into(),
            headers: Headers::new(),
        }
    }

    /// Replace all user headers.
    #[must_use]
    pub fn with_headers(mut self, headers: Headers) -> Self {
        self.headers = headers;
        self
    }

    /// Add or replace one user header.
    #[must_use]
    pub fn header(mut self, key: HeaderKey, value: HeaderValue) -> Self {
        self.headers.insert(key, value);
        self
    }

    fn into_iggy(self) -> Result<IggyMessage, LaserError> {
        Ok(IggyMessage::builder()
            .payload(self.payload)
            .user_headers(self.headers)
            .build()?)
    }
}

/// Configures a live [`Producer`] for one topic.
pub struct ProducerBuilder {
    topic: Topic,
    batch_length: u32,
    linger: Duration,
    retries: Option<u32>,
    retry_interval: Option<Duration>,
    routing: Routing,
    create_stream: bool,
    create_topic: bool,
    partitions: u32,
    replication_factor: Option<u8>,
    expiry: IggyExpiry,
    max_topic_size: MaxTopicSize,
    background: Option<BackgroundConfig>,
}

impl ProducerBuilder {
    pub(crate) fn new(topic: Topic) -> Self {
        Self {
            topic,
            batch_length: DEFAULT_BATCH_LENGTH,
            linger: Duration::ZERO,
            retries: Some(3),
            retry_interval: Some(DEFAULT_RETRY_INTERVAL),
            routing: Routing::Balanced,
            create_stream: true,
            create_topic: true,
            partitions: 1,
            replication_factor: None,
            expiry: IggyExpiry::ServerDefault,
            max_topic_size: MaxTopicSize::ServerDefault,
            background: None,
        }
    }

    #[must_use]
    pub fn batch_length(mut self, batch_length: u32) -> Self {
        self.batch_length = batch_length;
        self
    }

    #[must_use]
    pub fn linger(mut self, linger: Duration) -> Self {
        self.linger = linger;
        self
    }

    #[must_use]
    pub fn retries(mut self, retries: Option<u32>, interval: Option<Duration>) -> Self {
        self.retries = retries;
        self.retry_interval = interval;
        self
    }

    #[must_use]
    pub fn routing(mut self, routing: Routing) -> Self {
        self.routing = routing;
        self
    }

    #[must_use]
    pub fn create_stream(mut self, create: bool) -> Self {
        self.create_stream = create;
        self
    }

    #[must_use]
    pub fn create_topic(mut self, create: bool) -> Self {
        self.create_topic = create;
        self
    }

    #[must_use]
    pub fn partitions(mut self, partitions: u32) -> Self {
        self.partitions = partitions;
        self
    }

    #[must_use]
    pub fn replication_factor(mut self, replication_factor: Option<u8>) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    #[must_use]
    pub fn expire_after(mut self, expiry: Duration) -> Self {
        self.expiry = IggyExpiry::ExpireDuration(expiry.into());
        self
    }

    #[must_use]
    pub fn never_expire(mut self) -> Self {
        self.expiry = IggyExpiry::NeverExpire;
        self
    }

    #[must_use]
    pub fn max_topic_bytes(mut self, payload: u64) -> Self {
        self.max_topic_size = MaxTopicSize::from(payload);
        self
    }

    #[must_use]
    pub fn unlimited_topic_size(mut self) -> Self {
        self.max_topic_size = MaxTopicSize::Unlimited;
        self
    }

    /// Switches to Apache Iggy's buffered, sharded `background` send mode
    /// instead of the default synchronous `direct` mode: `batch_length`/
    /// `linger` are ignored once this is set, `config` carries their
    /// equivalents plus sharding and backpressure. Call
    /// [`Producer::shutdown`] before dropping the built producer, or
    /// buffered-but-unsent messages are lost.
    #[must_use]
    pub fn background(mut self, config: BackgroundConfig) -> Self {
        self.background = Some(config);
        self
    }

    pub async fn build(self) -> Result<Producer, LaserError> {
        if self.background.is_none() && self.batch_length == 0 {
            return Err(LaserError::Invalid(
                "producer batch length must be greater than zero".to_owned(),
            ));
        }
        if self.create_topic && self.partitions == 0 {
            return Err(LaserError::Invalid(
                "topic partition count must be greater than zero".to_owned(),
            ));
        }
        let mut builder = self.topic.iggy_producer()?;
        builder = match self.background {
            Some(config) => builder.background(config),
            None => builder.direct(
                DirectConfig::builder()
                    .batch_length(self.batch_length)
                    .linger_time(self.linger.into())
                    .build(),
            ),
        };
        builder = builder
            .partitioning(self.routing.into_partitioning()?)
            .send_retries(self.retries, self.retry_interval.map(IggyDuration::from));
        builder = if self.create_stream {
            builder.create_stream_if_not_exists()
        } else {
            builder.do_not_create_stream_if_not_exists()
        };
        builder = if self.create_topic {
            builder.create_topic_if_not_exists(
                self.partitions,
                self.replication_factor,
                self.expiry,
                self.max_topic_size,
            )
        } else {
            builder.do_not_create_topic_if_not_exists()
        };
        let producer = builder.build();
        producer.init().await?;
        Ok(Producer {
            inner: Arc::new(producer),
        })
    }
}

#[derive(Clone)]
/// A cloneable, initialized streaming producer.
pub struct Producer {
    inner: Arc<IggyProducer>,
}

impl Producer {
    /// Send one record without user headers using the configured routing.
    pub async fn send(&self, payload: impl Into<Bytes>) -> Result<(), LaserError> {
        self.send_message(ProducerMessage::new(payload)).await
    }

    /// Send one record using the configured routing.
    pub async fn send_message(&self, message: ProducerMessage) -> Result<(), LaserError> {
        self.inner.send(vec![message.into_iggy()?]).await?;
        Ok(())
    }

    /// Send one record with a per-call routing override.
    pub async fn send_with_routing(
        &self,
        message: ProducerMessage,
        routing: Routing,
    ) -> Result<(), LaserError> {
        self.inner
            .send_with_partitioning(
                vec![message.into_iggy()?],
                Some(Arc::new(routing.into_partitioning()?)),
            )
            .await?;
        Ok(())
    }

    /// Send one record with a per-call partition key.
    pub async fn send_keyed(
        &self,
        message: ProducerMessage,
        key: impl Into<Vec<u8>>,
    ) -> Result<(), LaserError> {
        self.send_with_routing(message, Routing::key(key)).await
    }

    /// Send one record to a specific partition.
    pub async fn send_to_partition(
        &self,
        message: ProducerMessage,
        partition: u32,
    ) -> Result<(), LaserError> {
        self.send_with_routing(message, Routing::Partition(partition))
            .await
    }

    /// Send a batch using the configured routing. Returns the record count.
    pub async fn send_batch(
        &self,
        messages: impl IntoIterator<Item = ProducerMessage>,
    ) -> Result<usize, LaserError> {
        self.send_batch_with_routing(messages, None).await
    }

    /// Send a batch with an optional per-call routing override.
    pub async fn send_batch_with_routing(
        &self,
        messages: impl IntoIterator<Item = ProducerMessage>,
        routing: Option<Routing>,
    ) -> Result<usize, LaserError> {
        let messages = messages
            .into_iter()
            .map(ProducerMessage::into_iggy)
            .collect::<Result<Vec<_>, _>>()?;
        let count = messages.len();
        if count == 0 {
            return Ok(0);
        }
        let routing = routing
            .map(Routing::into_partitioning)
            .transpose()?
            .map(Arc::new);
        self.inner.send_with_partitioning(messages, routing).await?;
        Ok(count)
    }

    /// Flushes buffered `background`-mode messages and stops the worker. A
    /// `direct`-mode producer has nothing to flush, so this is a cheap
    /// no-op for it. Requires this to be the last live handle to the
    /// producer, since Apache Iggy's shutdown takes ownership; fails if
    /// other clones of this `Producer` are still alive.
    pub async fn shutdown(self) -> Result<(), LaserError> {
        match Arc::try_unwrap(self.inner) {
            Ok(producer) => {
                producer.shutdown().await;
                Ok(())
            }
            Err(_) => Err(LaserError::Invalid(
                "producer still has other live handles, drop them before shutdown".to_owned(),
            )),
        }
    }
}

enum ConsumerTarget {
    Partition { name: String, partition: u32 },
    Group { name: String },
}

/// Configures a live partition or consumer-group reader.
pub struct ConsumerBuilder {
    topic: Topic,
    target: ConsumerTarget,
    batch_length: u32,
    poll_interval: Option<Duration>,
    start: ConsumerStart,
    commit: CommitPolicy,
    auto_join_group: bool,
    create_group: bool,
    polling_retry_interval: Duration,
    init_retries: Option<(u32, Duration)>,
    allow_replay: bool,
}

impl ConsumerBuilder {
    pub(crate) fn partition(topic: Topic, name: impl Into<String>, partition: u32) -> Self {
        Self::new(
            topic,
            ConsumerTarget::Partition {
                name: name.into(),
                partition,
            },
        )
    }

    pub(crate) fn group(topic: Topic, name: impl Into<String>) -> Self {
        Self::new(topic, ConsumerTarget::Group { name: name.into() })
    }

    fn new(topic: Topic, target: ConsumerTarget) -> Self {
        Self {
            topic,
            target,
            batch_length: DEFAULT_BATCH_LENGTH,
            poll_interval: None,
            start: ConsumerStart::Next,
            commit: CommitPolicy::Polling,
            auto_join_group: true,
            create_group: true,
            polling_retry_interval: DEFAULT_RETRY_INTERVAL,
            init_retries: None,
            allow_replay: false,
        }
    }

    #[must_use]
    pub fn batch_length(mut self, batch_length: u32) -> Self {
        self.batch_length = batch_length;
        self
    }

    #[must_use]
    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = Some(poll_interval);
        self
    }

    #[must_use]
    pub fn without_poll_interval(mut self) -> Self {
        self.poll_interval = None;
        self
    }

    #[must_use]
    pub fn start_at(mut self, start: ConsumerStart) -> Self {
        self.start = start;
        self
    }

    #[must_use]
    pub fn commit_policy(mut self, commit: CommitPolicy) -> Self {
        self.commit = commit;
        self
    }

    #[must_use]
    pub fn auto_join_group(mut self, auto_join: bool) -> Self {
        self.auto_join_group = auto_join;
        self
    }

    #[must_use]
    pub fn create_group(mut self, create: bool) -> Self {
        self.create_group = create;
        self
    }

    #[must_use]
    pub fn polling_retry_interval(mut self, interval: Duration) -> Self {
        self.polling_retry_interval = interval;
        self
    }

    #[must_use]
    pub fn init_retries(mut self, retries: u32, interval: Duration) -> Self {
        self.init_retries = Some((retries, interval));
        self
    }

    #[must_use]
    pub fn allow_replay(mut self) -> Self {
        self.allow_replay = true;
        self
    }

    pub async fn build(self) -> Result<Consumer, LaserError> {
        if self.batch_length == 0 {
            return Err(LaserError::Invalid(
                "consumer batch length must be greater than zero".to_owned(),
            ));
        }
        let group = matches!(self.target, ConsumerTarget::Group { .. });
        let manual_commit = self.commit == CommitPolicy::Disabled;
        let shutdown_target = match &self.target {
            ConsumerTarget::Group { name } if manual_commit && self.auto_join_group => {
                Some(ConsumerGroupTarget {
                    laser: self.topic.laser.clone(),
                    stream: self.topic.stream()?.to_owned(),
                    topic: self.topic.name.clone(),
                    group: name.clone(),
                })
            }
            _ => None,
        };
        let mut builder: IggyConsumerBuilder = match &self.target {
            ConsumerTarget::Partition { name, partition } => {
                self.topic.iggy_consumer(name, *partition)?
            }
            ConsumerTarget::Group { name } => self.topic.iggy_consumer_group(name)?,
        };
        builder = builder
            .batch_length(self.batch_length)
            .polling_strategy(self.start.into_polling())
            .auto_commit(self.commit.into_auto_commit()?)
            .polling_retry_interval(self.polling_retry_interval.into());
        builder = match self.poll_interval {
            Some(interval) => builder.poll_interval(interval.into()),
            None => builder.without_poll_interval(),
        };
        if group {
            builder = if self.auto_join_group {
                builder.auto_join_consumer_group()
            } else {
                builder.do_not_auto_join_consumer_group()
            };
            builder = if self.create_group {
                builder.create_consumer_group_if_not_exists()
            } else {
                builder.do_not_create_consumer_group_if_not_exists()
            };
        }
        if let Some((retries, interval)) = self.init_retries {
            builder = builder.init_retries(retries, interval.into());
        }
        if self.allow_replay {
            builder = builder.allow_replay();
        }
        let mut consumer = builder.build();
        consumer.init().await?;
        Ok(Consumer {
            inner: Some(consumer),
            manual_commit,
            shutdown_target,
        })
    }
}

#[derive(Debug, Clone)]
/// One live-consumer record with its exact payload, headers, and log position.
pub struct ConsumerMessage {
    pub payload: Bytes,
    pub headers: Headers,
    pub message_id: u128,
    pub checksum: u64,
    pub position: MessageId,
    pub current_offset: u64,
    pub partition_id: u32,
    pub timestamp_micros: u64,
    pub origin_timestamp_micros: u64,
}

impl ConsumerMessage {
    /// Decode the payload as JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, LaserError> {
        serde_json::from_slice(&self.payload).map_err(|error| LaserError::Codec(error.to_string()))
    }
}

impl TryFrom<ReceivedMessage> for ConsumerMessage {
    type Error = LaserError;

    fn try_from(received: ReceivedMessage) -> Result<Self, Self::Error> {
        let headers = received.message.user_headers_map()?.unwrap_or_default();
        let header = received.message.header;
        Ok(Self {
            payload: received.message.payload,
            headers,
            message_id: header.id,
            checksum: header.checksum,
            position: MessageId::new(received.partition_id, header.offset),
            current_offset: received.current_offset,
            partition_id: received.partition_id,
            timestamp_micros: header.timestamp,
            origin_timestamp_micros: header.origin_timestamp,
        })
    }
}

/// An initialized live reader with server-managed offsets.
pub struct Consumer {
    inner: Option<IggyConsumer>,
    manual_commit: bool,
    shutdown_target: Option<ConsumerGroupTarget>,
}

struct ConsumerGroupTarget {
    laser: crate::laser::Laser,
    stream: String,
    topic: String,
    group: String,
}

impl Consumer {
    /// Wait for the next record.
    pub async fn next(&mut self) -> Option<Result<ConsumerMessage, LaserError>> {
        futures::StreamExt::next(self).await
    }

    /// Wait for the next record, bounding how long a caller sits idle: a typed
    /// [`LaserError::Timeout`] past `wait`, a typed [`LaserError::Invalid`] if
    /// the stream ended, or the record itself. One call replaces threading a
    /// timeout, a stream-end check, and the per-record decode result through
    /// every call site by hand.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # use std::time::Duration;
    /// # async fn run(mut consumer: Consumer) -> Result<(), LaserError> {
    /// let message = consumer.next_within(Duration::from_secs(10)).await?;
    /// # let _ = message; Ok(()) }
    /// ```
    pub async fn next_within(&mut self, wait: Duration) -> Result<ConsumerMessage, LaserError> {
        tokio::time::timeout(wait, self.next())
            .await
            .map_err(|_| LaserError::Timeout("the live consumer to yield a record"))?
            .ok_or_else(|| LaserError::Invalid("the live consumer stream ended".to_owned()))?
    }

    /// Store a handled record's offset on the server.
    pub async fn commit(&self, message: &ConsumerMessage) -> Result<(), LaserError> {
        self.store_offset(message.position.offset, Some(message.partition_id))
            .await
    }

    /// Store an explicit server offset.
    pub async fn store_offset(
        &self,
        offset: u64,
        partition: Option<u32>,
    ) -> Result<(), LaserError> {
        self.inner()?.store_offset(offset, partition).await?;
        Ok(())
    }

    /// Delete the server offset for a partition or the current partition.
    pub async fn delete_offset(&self, partition: Option<u32>) -> Result<(), LaserError> {
        self.inner()?.delete_offset(partition).await?;
        Ok(())
    }

    /// Return the last locally yielded offset for a partition.
    pub fn last_consumed_offset(&self, partition: u32) -> Option<u64> {
        self.inner
            .as_ref()
            .and_then(|consumer| consumer.get_last_consumed_offset(partition))
    }

    /// Return the last offset this reader stored for a partition.
    pub fn last_stored_offset(&self, partition: u32) -> Option<u64> {
        self.inner
            .as_ref()
            .and_then(|consumer| consumer.get_last_stored_offset(partition))
    }

    /// Stop polling and leave the group. Automatic policies flush final offset
    /// state. [`CommitPolicy::Disabled`] preserves the last explicit commit.
    pub async fn shutdown(&mut self) -> Result<(), LaserError> {
        if self.manual_commit {
            drop(self.inner.take());
            if let Some(target) = self.shutdown_target.take() {
                target
                    .laser
                    .client()
                    .leave_consumer_group(
                        &Identifier::try_from(target.stream)?,
                        &Identifier::try_from(target.topic)?,
                        &Identifier::try_from(target.group)?,
                    )
                    .await?;
            }
            return Ok(());
        }
        if let Some(consumer) = self.inner.as_mut() {
            consumer.shutdown().await?;
        }
        self.inner.take();
        self.shutdown_target.take();
        Ok(())
    }

    fn inner(&self) -> Result<&IggyConsumer, LaserError> {
        self.inner
            .as_ref()
            .ok_or_else(|| LaserError::Invalid("consumer has been shut down".to_owned()))
    }
}

impl Stream for Consumer {
    type Item = Result<ConsumerMessage, LaserError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match Pin::new(inner).poll_next(context) {
            Poll::Ready(Some(Ok(message))) => Poll::Ready(Some(message.try_into())),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_zero_frequency_when_building_commit_policy_then_should_reject_it() {
        let error = CommitPolicy::Every(0)
            .into_auto_commit()
            .expect_err("zero cannot be a commit frequency");
        assert!(matches!(error, LaserError::Invalid(_)));
    }

    #[test]
    fn given_zero_interval_when_building_commit_policy_then_should_reject_it() {
        let error = CommitPolicy::Interval(Duration::ZERO)
            .into_auto_commit()
            .expect_err("zero cannot be a commit interval");
        assert!(matches!(error, LaserError::Invalid(_)));
    }

    #[test]
    fn given_empty_key_when_building_routing_then_should_reject_it() {
        let error = Routing::key(Vec::new())
            .into_partitioning()
            .expect_err("an empty partition key should be rejected");
        assert!(matches!(error, LaserError::Iggy(_)));
    }

    #[test]
    fn given_typed_header_when_building_message_then_should_preserve_width() {
        let key = HeaderKey::try_from("type").expect("the header key should be valid");
        let message = ProducerMessage::new(b"event".to_vec())
            .header(key.clone(), HeaderValue::from(7_u16))
            .into_iggy()
            .expect("the producer message should encode");
        let headers = message
            .user_headers_map()
            .expect("the headers should decode")
            .expect("the message should carry headers");
        assert_eq!(
            headers
                .get(&key)
                .expect("the type header should exist")
                .as_uint16()
                .expect("the type header should remain uint16"),
            7
        );
    }
}

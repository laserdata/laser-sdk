use crate::capabilities::{Capabilities, HelloOutcome};
use crate::error::LaserError;
use crate::filters::group::settled_assignment;
use crate::filters::reader::{DEFAULT_IDLE_INTERVAL, Delivery, FilteredReader, MatchedRecord};
use crate::laser::{ensure_stream, ensure_topic_retained, validate_replication_factor};
use crate::poll::current_tail_offset;
use crate::stream::consumer_group::ConsumerGroup;
use crate::stream::{HeaderKey, HeaderValue, Topic};
use crate::types::MessageId;
use bytes::Bytes;
use futures::Stream;
use futures::future::{Either, ready};
use iggy::prelude::{
    AutoCommit, BackgroundConfig, Client, CommitAfter, CommitEvery, ConsumerGroupClient,
    ConsumerOffsetClient, DEFAULT_POLL_MAX_BYTES, DirectConfig, Identifier, IggyClient,
    IggyConsumer, IggyConsumerBuilder, IggyError, IggyExpiry, IggyMessage, IggyProducer,
    IggyTimestamp, MAX_DEFERRED_POLL_WAIT_US, MIN_DEFERRED_POLL_MAX_BYTES, MaxTopicSize,
    MessageClient, Partitioning, PollOptions, PollingStrategy, ReadError, ReceivedMessage,
    SendMessagesResponse,
};
use laser_wire::filter::FilterRef;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::{Instant, sleep};

const DEFAULT_BATCH_LENGTH: u32 = 1000;
const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(5);
const DEFAULT_MIN_COUNT: u32 = 1;

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
    // Whether a group that never stored a partition must store this start
    // before its `Next` read begins where the start names.
    const fn seeds(self) -> bool {
        !matches!(self, Self::First | Self::Next | Self::Offset(0))
    }

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
    /// Commit the current polled batch on the server before delivery. A crash
    /// can skip records the application has not processed. A policy-aware
    /// group consumer stores the delivered prefix when it polls again and at
    /// shutdown instead, so a crash redelivers the current batch and never
    /// skips it.
    #[default]
    Polling,
    /// Store on an interval or commit the current batch during polling.
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

impl TryFrom<CommitPolicy> for iggy::prelude::CommitPolicy {
    type Error = LaserError;

    fn try_from(commit: CommitPolicy) -> Result<Self, Self::Error> {
        let interval = |duration: Duration| {
            positive_duration(duration, "commit interval must be greater than zero")
        };
        let every = |messages: u32| {
            std::num::NonZeroU32::new(messages)
                .map(CommitEvery::Messages)
                .ok_or_else(|| {
                    LaserError::Invalid("commit frequency must be greater than zero".to_owned())
                })
        };
        let auto = |interval, every| {
            Self::Auto(AutoCommit {
                interval,
                every,
                after: CommitAfter::Processing,
            })
        };
        Ok(match commit {
            CommitPolicy::Disabled => Self::Manual,
            CommitPolicy::Polling | CommitPolicy::IntervalOrPolling(_) => Self::OnRead,
            CommitPolicy::Interval(duration) => auto(Some(interval(duration)?), None),
            CommitPolicy::All => auto(None, Some(CommitEvery::Batch)),
            CommitPolicy::IntervalOrAll(duration) => {
                auto(Some(interval(duration)?), Some(CommitEvery::Batch))
            }
            CommitPolicy::Each => auto(None, Some(CommitEvery::Message)),
            CommitPolicy::IntervalOrEach(duration) => {
                auto(Some(interval(duration)?), Some(CommitEvery::Message))
            }
            CommitPolicy::Every(messages) => auto(None, Some(every(messages)?)),
            CommitPolicy::IntervalOrEvery(duration, messages) => {
                auto(Some(interval(duration)?), Some(every(messages)?))
            }
        })
    }
}

fn positive_duration<T>(duration: Duration, message: &'static str) -> Result<T, LaserError>
where
    T: TryFrom<Duration>,
{
    if duration.is_zero() {
        return Err(LaserError::Invalid(message.to_owned()));
    }
    duration
        .try_into()
        .map_err(|_| LaserError::Invalid(message.to_owned()))
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
    replication_factor: Option<u32>,
    expire_after: Option<Duration>,
    never_expire: bool,
    max_topic_bytes: Option<u64>,
    unlimited_topic_size: bool,
    background: Option<BackgroundConfig>,
}

impl ProducerBuilder {
    pub(crate) fn new(topic: Topic) -> Self {
        let publish = topic.laser.publish_options();
        Self {
            topic,
            batch_length: DEFAULT_BATCH_LENGTH,
            linger: Duration::ZERO,
            retries: Some(publish.max_retries),
            retry_interval: Some(publish.retry_backoff),
            routing: Routing::Balanced,
            // A producer never creates a stream unless asked. A governed
            // identity has no stream-wide create right, and a stream is
            // provisioned with its tenant.
            create_stream: false,
            create_topic: true,
            partitions: 1,
            replication_factor: None,
            expire_after: None,
            never_expire: false,
            max_topic_bytes: None,
            unlimited_topic_size: false,
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
    pub fn retry_backoff(mut self, interval: Duration) -> Self {
        self.retry_interval = Some(interval);
        self
    }

    #[must_use]
    pub fn routing(mut self, routing: Routing) -> Self {
        self.routing = routing;
        self
    }

    /// Create the stream on init when it does not exist. Off by default: a
    /// stream is provisioned with its tenant, and a scoped identity may not
    /// create one. A refused create on an existing stream still proceeds.
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

    /// Creates the topic with each partition stored on `factor` nodes,
    /// 1..=[`MAX_REPLICATION_FACTOR`](crate::stream::MAX_REPLICATION_FACTOR) and
    /// at most the cluster's node count. Without it the server's default
    /// applies. An existing topic keeps its
    /// own.
    #[must_use]
    pub fn replication_factor(mut self, factor: u32) -> Self {
        self.replication_factor = Some(factor);
        self
    }

    /// Creates the topic with messages expiring after `expiry`, which must be
    /// greater than zero. Set this or [`never_expire`](Self::never_expire), not
    /// both, or [`build`](Self::build) fails with [`LaserError::Invalid`].
    /// Without either the server default applies.
    #[must_use]
    pub fn expire_after(mut self, expiry: Duration) -> Self {
        self.expire_after = Some(expiry);
        self
    }

    /// Creates the topic with messages that never expire. Set this or
    /// [`expire_after`](Self::expire_after), not both.
    #[must_use]
    pub fn never_expire(mut self) -> Self {
        self.never_expire = true;
        self
    }

    /// Creates the topic capped at `payload` bytes, which must be greater than
    /// zero. Set this or [`unlimited_topic_size`](Self::unlimited_topic_size),
    /// not both, or [`build`](Self::build) fails with [`LaserError::Invalid`].
    /// Without either the server default applies.
    #[must_use]
    pub fn max_topic_bytes(mut self, payload: u64) -> Self {
        self.max_topic_bytes = Some(payload);
        self
    }

    /// Creates the topic without a size limit. Set this or
    /// [`max_topic_bytes`](Self::max_topic_bytes), not both.
    #[must_use]
    pub fn unlimited_topic_size(mut self) -> Self {
        self.unlimited_topic_size = true;
        self
    }

    // The topic expiry and size the builder settings ask for. Both forms of
    // one setting is a conflict, as in Python and TypeScript.
    fn topic_limits(&self) -> Result<(IggyExpiry, MaxTopicSize), LaserError> {
        let expiry = match (self.expire_after, self.never_expire) {
            (Some(_), true) => {
                return Err(LaserError::Invalid(
                    "producer takes expire_after or never_expire, not both".to_owned(),
                ));
            }
            (Some(age), false) if age.is_zero() => {
                return Err(LaserError::Invalid(
                    "producer expire_after must be greater than zero".to_owned(),
                ));
            }
            (Some(age), false) => IggyExpiry::ExpireDuration(age.into()),
            (None, true) => IggyExpiry::NeverExpire,
            (None, false) => IggyExpiry::ServerDefault,
        };
        let max_topic_size = match (self.max_topic_bytes, self.unlimited_topic_size) {
            (Some(_), true) => {
                return Err(LaserError::Invalid(
                    "producer takes max_topic_bytes or unlimited_topic_size, not both".to_owned(),
                ));
            }
            (Some(0), false) => {
                return Err(LaserError::Invalid(
                    "producer max_topic_bytes must be greater than zero".to_owned(),
                ));
            }
            (Some(bytes), false) => MaxTopicSize::from(bytes),
            (None, true) => MaxTopicSize::Unlimited,
            (None, false) => MaxTopicSize::ServerDefault,
        };
        Ok((expiry, max_topic_size))
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
        let (expiry, max_topic_size) = self.topic_limits()?;
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
        if let Some(factor) = self.replication_factor {
            validate_replication_factor(factor)?;
        }
        // The upstream producer creates a topic without a replication factor,
        // so a topic that needs one is created here first.
        let replicated_topic = self.replication_factor.filter(|_| self.create_topic);
        let background = self.background.is_some();
        let mut publish_options = self.topic.laser.publish_options();
        publish_options.max_retries = self.retries.unwrap_or(0);
        if let Some(interval) = self.retry_interval {
            publish_options.retry_backoff = interval;
        }
        publish_options.validate()?;
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
        builder = builder.partitioning(self.routing.into_partitioning()?);
        builder = if self.create_stream {
            builder.create_stream_if_not_exists()
        } else {
            builder.do_not_create_stream_if_not_exists()
        };
        builder = if self.create_topic && replicated_topic.is_none() {
            builder.create_topic_if_not_exists(self.partitions, expiry, max_topic_size)
        } else {
            builder.do_not_create_topic_if_not_exists()
        };
        let producer = builder.build();
        let observed = std::sync::atomic::AtomicU64::new(0);
        publish_options
            .run(
                || async {
                    observed.store(
                        self.topic.laser.publish_generation(),
                        std::sync::atomic::Ordering::Release,
                    );
                    if let Some(factor) = replicated_topic {
                        let client = self.topic.laser.client();
                        let stream = self.topic.stream()?;
                        if self.create_stream {
                            ensure_stream(&client, stream).await?;
                        }
                        ensure_topic_retained(
                            &client,
                            stream,
                            &self.topic.name,
                            self.partitions,
                            expiry,
                            max_topic_size,
                            Some(factor),
                        )
                        .await?;
                    }
                    Ok(crate::laser::init_producer(&producer).await?)
                },
                || {
                    self.topic
                        .laser
                        .reconnect_for_publish(observed.load(std::sync::atomic::Ordering::Acquire))
                },
            )
            .await?;
        let statistics = super::producer_statistics::ProducerRecorder::new(
            &self.topic.laser,
            producer.stream().to_string(),
            producer.topic().to_string(),
            !background,
        );
        Ok(Producer {
            statistics,
            inner: Arc::new(producer),
            laser: self.topic.laser.clone(),
            publish_options,
            batch_length: if self.batch_length == 0 {
                DEFAULT_BATCH_LENGTH
            } else {
                self.batch_length
            } as usize,
            background,
        })
    }
}

#[derive(Clone)]
/// A cloneable, initialized streaming producer.
pub struct Producer {
    statistics: super::producer_statistics::ProducerRecorder,
    inner: Arc<IggyProducer>,
    laser: crate::laser::Laser,
    publish_options: crate::laser::PublishOptions,
    batch_length: usize,
    background: bool,
}

impl Producer {
    /// Send one record without user headers using the configured routing.
    pub fn send(
        &self,
        payload: impl Into<Bytes>,
    ) -> impl Future<Output = Result<SendMessagesResponse, LaserError>> + Send + '_ {
        self.send_message(ProducerMessage::new(payload))
    }

    /// Send one record using the configured routing.
    pub fn send_message(
        &self,
        message: ProducerMessage,
    ) -> impl Future<Output = Result<SendMessagesResponse, LaserError>> + Send + '_ {
        self.publish_converted(message.into_iggy().map(|message| vec![message]), Ok(None))
    }

    /// Send one record with a per-call routing override.
    pub fn send_with_routing(
        &self,
        message: ProducerMessage,
        routing: Routing,
    ) -> impl Future<Output = Result<SendMessagesResponse, LaserError>> + Send + '_ {
        self.publish_converted(
            message.into_iggy().map(|message| vec![message]),
            routing
                .into_partitioning()
                .map(|partitioning| Some(Arc::new(partitioning))),
        )
    }

    /// Send one record with a per-call partition key.
    pub fn send_keyed(
        &self,
        message: ProducerMessage,
        key: impl Into<Vec<u8>>,
    ) -> impl Future<Output = Result<SendMessagesResponse, LaserError>> + Send + '_ {
        self.send_with_routing(message, Routing::key(key))
    }

    /// Send one record to a specific partition.
    pub fn send_to_partition(
        &self,
        message: ProducerMessage,
        partition: u32,
    ) -> impl Future<Output = Result<SendMessagesResponse, LaserError>> + Send + '_ {
        self.send_with_routing(message, Routing::Partition(partition))
    }

    /// Send a batch using the configured routing.
    pub fn send_batch(
        &self,
        messages: impl IntoIterator<Item = ProducerMessage>,
    ) -> impl Future<Output = Result<SendMessagesResponse, LaserError>> + Send + '_ {
        self.send_batch_with_routing(messages, None)
    }

    /// Send a batch with an optional per-call routing override.
    pub fn send_batch_with_routing(
        &self,
        messages: impl IntoIterator<Item = ProducerMessage>,
        routing: Option<Routing>,
    ) -> impl Future<Output = Result<SendMessagesResponse, LaserError>> + Send + '_ {
        self.publish_converted(
            messages
                .into_iter()
                .map(ProducerMessage::into_iggy)
                .collect::<Result<Vec<_>, _>>(),
            routing
                .map(Routing::into_partitioning)
                .transpose()
                .map(|partitioning| partitioning.map(Arc::new)),
        )
    }

    // Every send converts its arguments before the publish future starts, so
    // no send keeps them across the publish's awaits.
    fn publish_converted(
        &self,
        messages: Result<Vec<IggyMessage>, LaserError>,
        routing: Result<Option<Arc<Partitioning>>, LaserError>,
    ) -> impl Future<Output = Result<SendMessagesResponse, LaserError>> + Send + '_ {
        match (messages, routing) {
            (Err(error), _) => Either::Right(ready(Err(error))),
            (Ok(messages), _) if messages.is_empty() => {
                Either::Right(ready(Ok(SendMessagesResponse {
                    confirmations: Vec::new(),
                })))
            }
            (Ok(_), Err(error)) => Either::Right(ready(Err(error))),
            (Ok(messages), Ok(routing)) => Either::Left(self.send_iggy(messages, routing)),
        }
    }

    async fn send_iggy(
        &self,
        messages: Vec<IggyMessage>,
        routing: Option<Arc<Partitioning>>,
    ) -> Result<SendMessagesResponse, LaserError> {
        let observation = self.statistics.begin(
            messages.len() as u64,
            messages
                .iter()
                .map(|message| message.payload.len() as u64)
                .sum(),
        );
        let result = self.send_iggy_observed(messages, routing).await;
        observation.finish(result.is_ok());
        result
    }

    async fn send_iggy_observed(
        &self,
        mut messages: Vec<IggyMessage>,
        routing: Option<Arc<Partitioning>>,
    ) -> Result<SendMessagesResponse, LaserError> {
        if self.background {
            return Ok(self.inner.send_with_partitioning(messages, routing).await?);
        }
        crate::laser::prepare_publish_messages(&mut messages);
        let mut confirmations = Vec::new();
        let observed = std::sync::atomic::AtomicU64::new(0);
        for (index, chunk) in messages.chunks(self.batch_length).enumerate() {
            let response = self
                .publish_options
                .run(
                    || async {
                        observed.store(
                            self.laser.publish_generation(),
                            std::sync::atomic::Ordering::Release,
                        );
                        Ok(self
                            .inner
                            .send_with_partitioning(
                                chunk.iter().map(crate::laser::clone_iggy_message).collect(),
                                routing.clone(),
                            )
                            .await?)
                    },
                    || {
                        self.laser.reconnect_for_publish(
                            observed.load(std::sync::atomic::Ordering::Acquire),
                        )
                    },
                )
                .await
                .map_err(|error| {
                    crate::laser::publish_failure(
                        error,
                        &messages[index * self.batch_length..],
                        std::mem::take(&mut confirmations),
                        chunk.len(),
                        (
                            &self.inner.stream().to_string(),
                            &self.inner.topic().to_string(),
                        ),
                    )
                })?;
            confirmations.extend(response.confirmations);
        }
        Ok(SendMessagesResponse { confirmations })
    }

    /// Flushes buffered `background`-mode messages and stops the worker. A
    /// `direct`-mode producer has nothing to flush, so this is a cheap
    /// no-op for it. Requires this to be the last live handle to the
    /// producer, since Apache Iggy's shutdown takes ownership. Fails if
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

#[derive(Clone)]
enum ConsumerTarget {
    Partition { name: String, partition: u32 },
    Group(Box<ConsumerGroup>),
}

/// Configures a live partition or consumer-group reader.
///
/// A group consumer on a server that resolves group policies reads through
/// the group-aware engine: the server runs the group's policy, filtered or
/// unfiltered, and the consumer acknowledges through the fenced group
/// contract. On Apache Iggy, or a managed server whose probe positively
/// announced no filters, it is the native Iggy group consumer. Every option
/// below applies to both, except where noted.
#[derive(Clone)]
pub struct ConsumerBuilder {
    topic: Topic,
    target: ConsumerTarget,
    batch_length: u32,
    max_wait: Duration,
    min_count: u32,
    max_bytes: u32,
    start: ConsumerStart,
    commit: CommitPolicy,
    create_group: bool,
    polling_retry_interval: Duration,
    init_retries: Option<(u32, Duration)>,
    allow_replay: bool,
    native: bool,
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

    pub(crate) fn group(group: ConsumerGroup) -> Self {
        Self::new(
            group.topic().clone(),
            ConsumerTarget::Group(Box::new(group)),
        )
    }

    fn new(topic: Topic, target: ConsumerTarget) -> Self {
        Self {
            topic,
            target,
            batch_length: DEFAULT_BATCH_LENGTH,
            max_wait: DEFAULT_MAX_WAIT,
            min_count: DEFAULT_MIN_COUNT,
            max_bytes: DEFAULT_POLL_MAX_BYTES,
            start: ConsumerStart::Next,
            commit: CommitPolicy::Polling,
            create_group: true,
            polling_retry_interval: DEFAULT_RETRY_INTERVAL,
            init_retries: None,
            allow_replay: false,
            native: false,
        }
    }

    // Read a group natively even on a server that resolves group policies.
    // Only for a caller that knows no policy can bind the group, because a
    // bound group read natively would skip its policy.
    #[cfg_attr(not(feature = "agent"), allow(dead_code))]
    pub(crate) fn native(mut self) -> Self {
        self.native = true;
        self
    }

    /// Most records one poll returns. A policy-aware group consumer also
    /// examines at most this many source records per partition poll, so a
    /// selective policy returns fewer records, down to none, while progress
    /// still advances.
    #[must_use]
    pub fn batch_length(mut self, batch_length: u32) -> Self {
        self.batch_length = batch_length;
        self
    }

    /// How long the server holds a poll that finds fewer than `min_count`
    /// new records before it answers with what it has. Default 5 s, at most
    /// 600 s. Zero answers at once. A policy-aware group consumer holds its
    /// filtered wait at most this long and at most what the server announces.
    #[must_use]
    pub const fn max_wait(mut self, max_wait: Duration) -> Self {
        self.max_wait = max_wait;
        self
    }

    /// How many new records end a held poll early. Default 1, at most the
    /// batch length. Native only.
    #[must_use]
    pub const fn min_count(mut self, min_count: u32) -> Self {
        self.min_count = min_count;
        self
    }

    /// Most record bytes one poll returns. Default 1 MiB. A record larger
    /// than the cap comes alone, so a poll always makes progress. Native only.
    #[must_use]
    pub const fn max_bytes(mut self, max_bytes: u32) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Where reading begins. A partition consumer starts there. A group
    /// consumer always resumes after the group's stored offset: the start
    /// only seeds a partition the group never stored, so its first read
    /// begins at the record the start names. A policy-aware consumer seeds
    /// every such partition it gains before reading it. A native consumer
    /// seeds only those it holds when it joins, so one it gains later with
    /// nothing stored begins at its first record. A seed the server refuses,
    /// such as an offset past a partition's end, fails `build` and leaves the
    /// group, or fails the first read of a partition gained later.
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
    pub fn create_group(mut self, create: bool) -> Self {
        self.create_group = create;
        self
    }

    /// How long a failed poll waits before it is retried.
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

    /// Native only. A policy-aware group consumer honors its start position
    /// as given.
    #[must_use]
    pub fn allow_replay(mut self) -> Self {
        self.allow_replay = true;
        self
    }

    // A group reads `Next` everywhere, because upstream applies any other
    // start again to every partition a rebalance hands the member.
    fn reader_start(&self) -> ConsumerStart {
        match self.target {
            ConsumerTarget::Group(_) => ConsumerStart::Next,
            ConsumerTarget::Partition { .. } => self.start,
        }
    }

    pub async fn build(self) -> Result<Consumer, LaserError> {
        if self.batch_length == 0 {
            return Err(LaserError::Invalid(
                "consumer batch length must be greater than zero".to_owned(),
            ));
        }
        if self.min_count == 0 || self.min_count > self.batch_length {
            return Err(LaserError::Invalid(
                "consumer min_count must be 1..=batch_length".to_owned(),
            ));
        }
        if self.max_wait.as_micros() > u128::from(MAX_DEFERRED_POLL_WAIT_US) {
            return Err(LaserError::Invalid(
                "consumer max_wait must be at most 600 s".to_owned(),
            ));
        }
        if (self.max_bytes as usize) < MIN_DEFERRED_POLL_MAX_BYTES {
            return Err(LaserError::Invalid(format!(
                "consumer max_bytes must be at least {MIN_DEFERRED_POLL_MAX_BYTES}"
            )));
        }
        if let ConsumerTarget::Group(group) = &self.target
            && !self.native
        {
            let mut capabilities = self.topic.laser().capabilities().await;
            if capabilities.hello == HelloOutcome::Unknown {
                capabilities = self.topic.laser().refresh_capabilities().await;
            }
            if policy_aware(&capabilities)? {
                let group = group.as_ref().clone();
                return self.build_group(group).await;
            }
        }
        self.build_native().await
    }

    async fn build_group(self, group: ConsumerGroup) -> Result<Consumer, LaserError> {
        iggy::prelude::CommitPolicy::try_from(self.commit)?;
        if self.create_group && group.name().is_some() {
            group.ensure_native().await?;
        }
        let polling_retry_interval = positive_duration(
            self.polling_retry_interval,
            "consumer polling retry interval must be greater than zero",
        )?;
        let mut retries_left = self.init_retries;
        let reader = loop {
            let built = group
                .reader_with(FilterRef::Group)?
                .seeded(self.start)
                .count(self.batch_length)
                .max_examined(self.batch_length)
                .idle_interval(DEFAULT_IDLE_INTERVAL)
                .max_wait(self.max_wait)
                .retry_interval(polling_retry_interval)
                .build()
                .await;
            match built {
                Ok(reader) => break reader,
                Err(error)
                    if error.is_retryable()
                        && retries_left.is_some_and(|(retries, _)| retries > 0) =>
                {
                    let (retries, interval) = retries_left.unwrap_or_default();
                    retries_left = Some((retries - 1, interval));
                    sleep(positive_duration(
                        interval,
                        "consumer initialization retry interval must be greater than zero",
                    )?)
                    .await;
                }
                Err(error) => return Err(error),
            }
        };
        let offset_owner = OffsetOwner {
            laser: self.topic.laser.clone(),
            stream: self.topic.stream()?.to_owned(),
            topic: self.topic.name.clone(),
            consumer: group_offset_consumer(&group)?,
        };
        let offsets = Arc::new(std::sync::Mutex::new(GroupOffsets::default()));
        let deferred = Arc::new(std::sync::Mutex::new(Vec::new()));
        Ok(Consumer {
            yielded_zero: BTreeSet::new(),
            inner: Some(ConsumerInner::Group(Arc::new(tokio::sync::Mutex::new(
                GroupEngine {
                    reader,
                    commit: self.commit,
                    buffered: VecDeque::new(),
                    pending_delivery: None,
                    pending_flush: None,
                    returned: VecDeque::new(),
                    yielded_since_flush: 0,
                    last_flush: Instant::now(),
                    offsets: Arc::clone(&offsets),
                    deferred: Arc::clone(&deferred),
                },
            )))),
            manual_commit: self.commit == CommitPolicy::Disabled,
            allow_replay: self.allow_replay,
            shutdown_target: None,
            group_reads: std::sync::Mutex::new(GroupReads::default()),
            offsets: Some(offsets),
            deferred: Some(deferred),
            returned_native: std::sync::Mutex::new(VecDeque::new()),
            offset_owner,
        })
    }

    async fn build_native(self) -> Result<Consumer, LaserError> {
        let manual_commit = self.commit == CommitPolicy::Disabled;
        let native_group = match &self.target {
            ConsumerTarget::Group(group) => Some(group.native_name().await?),
            ConsumerTarget::Partition { .. } => None,
        };
        let shutdown_target = match &self.target {
            ConsumerTarget::Group(_) if manual_commit => Some(ConsumerGroupTarget {
                laser: self.topic.laser.clone(),
                stream: self.topic.stream()?.to_owned(),
                topic: self.topic.name.clone(),
                group: native_group
                    .clone()
                    .ok_or(LaserError::Config("consumer group name is absent"))?,
            }),
            _ => None,
        };
        let offset_owner = OffsetOwner {
            laser: self.topic.laser.clone(),
            stream: self.topic.stream()?.to_owned(),
            topic: self.topic.name.clone(),
            consumer: match (&self.target, &native_group) {
                (ConsumerTarget::Partition { name, .. }, _) => {
                    iggy::prelude::Consumer::new(Identifier::named(name)?)
                }
                (ConsumerTarget::Group(_), Some(group)) => {
                    iggy::prelude::Consumer::group(Identifier::named(group)?)
                }
                (ConsumerTarget::Group(_), None) => {
                    return Err(LaserError::Config("consumer group name is absent"));
                }
            },
        };
        let group = matches!(self.target, ConsumerTarget::Group(_));
        let mut builder: IggyConsumerBuilder = match &self.target {
            ConsumerTarget::Partition { name, partition } => {
                self.topic.iggy_consumer(name, *partition)?
            }
            ConsumerTarget::Group(_) => self.topic.iggy_consumer_group(
                native_group
                    .as_deref()
                    .ok_or(LaserError::Config("consumer group name is absent"))?,
            )?,
        };
        let poll_options = PollOptions {
            count: self.batch_length,
            max_wait: self.max_wait.into(),
            min_count: self.min_count,
            max_bytes: self.max_bytes,
            commit: iggy::prelude::CommitPolicy::try_from(self.commit)?,
        };
        let reader_start = self.reader_start();
        builder = builder
            .poll_options(poll_options)
            .polling_strategy(reader_start.into_polling())
            .polling_retry_interval(positive_duration(
                self.polling_retry_interval,
                "consumer polling retry interval must be greater than zero",
            )?);
        if group {
            builder = if self.create_group {
                builder.create_consumer_group_if_not_exists()
            } else {
                builder.do_not_create_consumer_group_if_not_exists()
            };
        }
        if let Some((retries, interval)) = self.init_retries {
            builder = builder.init_retries(
                retries,
                positive_duration(
                    interval,
                    "consumer initialization retry interval must be greater than zero",
                )?,
            );
        }
        if self.allow_replay {
            builder = builder.allow_replay();
        }
        let seeds = group && self.start.seeds();
        if seeds {
            if let ConsumerTarget::Group(handle) = &self.target
                && self.create_group
                && handle.name().is_some()
            {
                handle.ensure_native().await?;
            }
            seed_group_start(&offset_owner, self.start).await?;
        }
        let mut consumer = builder.build();
        if let Err(error) = consumer.init().await {
            if seeds {
                leave_seeded_group(&offset_owner).await;
            }
            return Err(error.into());
        }
        Ok(Consumer {
            yielded_zero: BTreeSet::new(),
            inner: Some(ConsumerInner::Native(Box::new(consumer))),
            manual_commit,
            allow_replay: self.allow_replay,
            shutdown_target,
            group_reads: std::sync::Mutex::new(GroupReads::default()),
            offsets: None,
            deferred: None,
            returned_native: std::sync::Mutex::new(VecDeque::new()),
            offset_owner,
        })
    }
}

/// How the native consumer resumes a partition that a read error stopped,
/// as the upstream connector sink does.
#[derive(Clone, Copy, Debug, PartialEq)]
enum ReadRecovery {
    /// A read with an unknown outcome blocks the partition. Its operation is
    /// abandoned and the partition resumes after its stored offset.
    Abandon { partition_id: u32, operation: u64 },
    /// A record does not decode. It is reported once and read past.
    Skip {
        partition_id: u32,
        start: PollingStrategy,
    },
    /// The partition has a new history. It resumes after its stored offset.
    Resume { partition_id: u32 },
    /// The reader stopped the partition after lasting refusals (permission,
    /// ownership, a missing stream, topic or partition). The refusal is
    /// reported and the partition resumes after its stored offset.
    Stopped { partition_id: u32 },
    /// The reader retries by itself, the error is only reported.
    Report,
}

impl From<&ReadError> for ReadRecovery {
    fn from(error: &ReadError) -> Self {
        match (error.partition_id, &error.error, error.skip) {
            (
                Some(partition_id),
                IggyError::FetchProgressUncertain(operation)
                | IggyError::PartitionLaneBlocked(_, operation),
                _,
            ) => Self::Abandon {
                partition_id,
                operation: *operation,
            },
            (Some(partition_id), _, Some(start)) => Self::Skip {
                partition_id,
                start,
            },
            (Some(partition_id), IggyError::HistoryUnavailable, None) => {
                Self::Resume { partition_id }
            }
            (Some(partition_id), _, None) if error.is_lasting_refusal() => {
                Self::Stopped { partition_id }
            }
            _ => Self::Report,
        }
    }
}

// Seed the partitions this member holds that the group never stored. Only a
// holder may store a group offset, so the member joins first, and leaves
// again when a seed fails.
async fn seed_group_start(owner: &OffsetOwner, start: ConsumerStart) -> Result<(), LaserError> {
    let client = owner.laser.client();
    let stream = Identifier::named(&owner.stream)?;
    let topic = Identifier::named(&owner.topic)?;
    let group = &owner.consumer.id;
    client.join_consumer_group(&stream, &topic, group).await?;
    let seeded = async {
        let held = settled_assignment(&client, &stream, &topic, group)
            .await?
            .ok_or_else(|| {
                IggyError::ConsumerGroupMemberNotFound(0, group.clone(), topic.clone())
            })?;
        for partition in held.partitions {
            seed_group_partition(
                &client,
                &owner.consumer,
                (&stream, &topic, partition),
                start,
            )
            .await?;
        }
        Ok::<(), LaserError>(())
    }
    .await;
    if seeded.is_err() {
        leave_seeded_group(owner).await;
    }
    seeded
}

/// Store `start` on a partition the group never stored, so the group's
/// `Next` read begins at the record the start names.
pub(crate) async fn seed_group_partition(
    client: &IggyClient,
    group: &iggy::prelude::Consumer,
    (stream, topic, partition): (&Identifier, &Identifier, u32),
    start: ConsumerStart,
) -> Result<(), LaserError> {
    if !start.seeds()
        || client
            .get_consumer_offset(group, stream, topic, Some(partition))
            .await?
            .is_some()
    {
        return Ok(());
    }
    let probe = iggy::prelude::Consumer::new(Identifier::named("laser-group-start")?);
    let found = match start {
        ConsumerStart::TimestampMicros(timestamp) => client
            .poll_messages(
                stream,
                topic,
                partition,
                &probe,
                &PollingStrategy::timestamp(IggyTimestamp::from(timestamp)),
                PollOptions {
                    count: 1,
                    ..PollOptions::default()
                },
            )
            .await?
            .messages
            .first()
            .map(|message| message.header.offset),
        _ => None,
    };
    let tail = match (start, found) {
        (ConsumerStart::Last, _) | (ConsumerStart::TimestampMicros(_), None) => {
            current_tail_offset(client, stream, topic, &probe, partition).await?
        }
        _ => 0,
    };
    if let Some(offset) = group_seed(start, tail, found) {
        client
            .store_consumer_offset(group, stream, topic, Some(partition), offset)
            .await?;
    }
    Ok(())
}

// The offset a group stores on a partition it never stored, so `Next` begins
// at the record `start` names. `tail` is the offset the partition writes next
// and `found` the first one at or after a timestamp start.
fn group_seed(start: ConsumerStart, tail: u64, found: Option<u64>) -> Option<u64> {
    let first = match start {
        ConsumerStart::First | ConsumerStart::Next => 0,
        ConsumerStart::Offset(offset) => offset,
        ConsumerStart::Last => tail.saturating_sub(1),
        ConsumerStart::TimestampMicros(_) => found.unwrap_or(tail),
    };
    first.checked_sub(1)
}

// A joined session without a reader would hold its partitions until the
// session ends.
async fn leave_seeded_group(owner: &OffsetOwner) {
    let group = &owner.consumer.id;
    let left = match (
        Identifier::named(&owner.stream),
        Identifier::named(&owner.topic),
    ) {
        (Ok(stream), Ok(topic)) => owner
            .laser
            .client()
            .leave_consumer_group(&stream, &topic, group)
            .await
            .map_err(LaserError::from),
        (Err(error), _) | (_, Err(error)) => Err(LaserError::from(error)),
    };
    if let Err(error) = left {
        tracing::debug!(target: "laser", %error, "Leaving consumer group {group} after a failed start failed.");
    }
}

// The two calls a recovery makes on the reader and its client.
#[trait_variant::make(Send)]
trait PartitionRecovery {
    async fn abandon_operation(&mut self, operation: u64) -> Result<(), IggyError>;
    async fn seek(&mut self, partition_id: u32, start: PollingStrategy) -> Result<(), IggyError>;
}

struct NativeRecoveryTarget<'a> {
    client: &'a IggyClient,
    consumer: &'a mut IggyConsumer,
}

impl PartitionRecovery for NativeRecoveryTarget<'_> {
    async fn abandon_operation(&mut self, operation: u64) -> Result<(), IggyError> {
        self.client.abandon_operation(operation).await
    }

    async fn seek(&mut self, partition_id: u32, start: PollingStrategy) -> Result<(), IggyError> {
        self.consumer.seek(partition_id, start).await
    }
}

fn group_reads(engine: Arc<tokio::sync::Mutex<GroupEngine>>) -> GroupReadStream {
    Box::pin(futures::stream::unfold(engine, |engine| async move {
        let item = engine.lock().await.next().await;
        item.map(|item| (item, engine))
    }))
}

async fn recover_native(
    client: Arc<IggyClient>,
    mut consumer: Box<IggyConsumer>,
    error: ReadError,
) -> NativeRecovered {
    let mut target = NativeRecoveryTarget {
        client: &client,
        consumer: &mut consumer,
    };
    let reported = recover_read(&mut target, error).await;
    NativeRecovered { consumer, reported }
}

// Resume the partition a read error stopped after its stored offset and return
// the error the caller still sees: an undecodable record, a failed seek, or an
// error the reader retries by itself.
async fn recover_read(target: &mut impl PartitionRecovery, error: ReadError) -> Option<LaserError> {
    let (partition_id, start, reported) = match ReadRecovery::from(&error) {
        ReadRecovery::Abandon {
            partition_id,
            operation,
        } => {
            tracing::warn!(target: "laser", partition_id, operation, error = %error.error, "Abandoning operation {operation} with an unknown outcome, which blocks partition {partition_id}. The partition resumes after its stored offset.");
            match target.abandon_operation(operation).await {
                // The reader reports a lost fetch for each of its partitions,
                // and the first abandon frees the operation for all of them.
                Ok(()) | Err(IggyError::OperationNotRetained(_)) => {}
                Err(abandon_error) => {
                    tracing::warn!(target: "laser", partition_id, operation, error = %abandon_error, "Abandoning operation {operation} failed.");
                }
            }
            (partition_id, PollingStrategy::next(), None)
        }
        ReadRecovery::Skip {
            partition_id,
            start,
        } => {
            tracing::warn!(target: "laser", partition_id, error = %error.error, "Skipping a record that does not decode in partition {partition_id}.");
            (partition_id, start, Some(LaserError::from(error.error)))
        }
        ReadRecovery::Resume { partition_id } => {
            tracing::warn!(target: "laser", partition_id, error = %error.error, "Partition {partition_id} has a new history. It resumes after its stored offset.");
            (partition_id, PollingStrategy::next(), None)
        }
        ReadRecovery::Stopped { partition_id } => {
            tracing::warn!(target: "laser", partition_id, error = %error.error, "The reader stopped partition {partition_id} after lasting refusals. It resumes after its stored offset.");
            (
                partition_id,
                PollingStrategy::next(),
                Some(LaserError::from(error.error)),
            )
        }
        ReadRecovery::Report => return Some(LaserError::from(error.error)),
    };
    match target.seek(partition_id, start).await {
        Ok(()) => reported,
        Err(seek_error) => {
            tracing::warn!(target: "laser", partition_id, error = %seek_error, "Resuming partition {partition_id} failed.");
            Some(reported.unwrap_or_else(|| LaserError::from(seek_error)))
        }
    }
}

/// Whether a group consumer reads through the group-aware engine. A server
/// that advertises group-aware reads does. A server that serves consumer
/// filters without them must be upgraded, because its groups may be bound
/// and a native poll would read past their policies. A probe that
/// established nothing is not a server without filters. Only a server whose
/// announcement, or refusal to announce, positively lacks filters reads
/// natively.
pub(crate) fn policy_aware(capabilities: &Capabilities) -> Result<bool, LaserError> {
    if capabilities.filters.group_policy_reads {
        return Ok(true);
    }
    if capabilities.filters.native {
        return Err(LaserError::unsupported_feature(
            "filters",
            "group_policy_reads",
            "this server serves consumer filters but not group-aware reads, upgrade it before consuming groups through the Laser SDK",
        ));
    }
    match capabilities.hello {
        HelloOutcome::Unknown | HelloOutcome::Failed => Err(LaserError::Timeout(
            "the managed probe that decides whether this server resolves consumer group policies, reconnect and build the consumer again",
        )),
        HelloOutcome::Answered | HelloOutcome::Rejected => Ok(false),
    }
}

// The server identity a group's offsets are stored under, by name or by its
// native 32-bit id.
fn group_offset_consumer(group: &ConsumerGroup) -> Result<iggy::prelude::Consumer, LaserError> {
    let id =
        match (group.name(), group.id()) {
            (Some(name), _) => Identifier::named(name)?,
            (None, Some(id)) => Identifier::numeric(u32::try_from(id).map_err(|_| {
                LaserError::Invalid("consumer group id exceeds 32 bits".to_owned())
            })?)?,
            (None, None) => return Err(LaserError::Config("consumer group name is absent")),
        };
    Ok(iggy::prelude::Consumer::group(id))
}

/// When the group engine asks whether to store the handled prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitPoint {
    /// A record was just delivered, `last_of_page` when it ended its poll.
    Delivered { last_of_page: bool },
    /// The next poll is about to start.
    Poll,
}

/// The interval an automatic commit policy stores on, if it has one.
const fn commit_interval(commit: CommitPolicy) -> Option<Duration> {
    match commit {
        CommitPolicy::Interval(interval)
        | CommitPolicy::IntervalOrPolling(interval)
        | CommitPolicy::IntervalOrAll(interval)
        | CommitPolicy::IntervalOrEach(interval)
        | CommitPolicy::IntervalOrEvery(interval, _) => Some(interval),
        CommitPolicy::Disabled
        | CommitPolicy::Polling
        | CommitPolicy::All
        | CommitPolicy::Each
        | CommitPolicy::Every(_) => None,
    }
}

/// Whether an automatic commit policy stores the handled prefix now.
/// `yielded` counts records delivered since the last store and `elapsed` the
/// time since it.
const fn commit_due(
    commit: CommitPolicy,
    yielded: u32,
    elapsed: Duration,
    point: CommitPoint,
) -> bool {
    let polling = matches!(point, CommitPoint::Poll) && yielded > 0;
    let last_of_page = matches!(point, CommitPoint::Delivered { last_of_page: true });
    let delivered = matches!(point, CommitPoint::Delivered { .. });
    match commit {
        CommitPolicy::Disabled => false,
        CommitPolicy::Polling => polling,
        CommitPolicy::Interval(interval) => elapsed.as_nanos() >= interval.as_nanos(),
        CommitPolicy::IntervalOrPolling(interval) => {
            polling || elapsed.as_nanos() >= interval.as_nanos()
        }
        CommitPolicy::All => last_of_page,
        CommitPolicy::IntervalOrAll(interval) => {
            last_of_page || elapsed.as_nanos() >= interval.as_nanos()
        }
        CommitPolicy::Each | CommitPolicy::IntervalOrEach(_) => delivered && yielded > 0,
        CommitPolicy::Every(every) => delivered && yielded >= every,
        CommitPolicy::IntervalOrEvery(interval, every) => {
            (delivered && yielded >= every) || elapsed.as_nanos() >= interval.as_nanos()
        }
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
    /// The header block exactly as stored, for a caller that decodes its
    /// entries itself.
    pub user_headers: Option<Bytes>,
    /// True when the header block does not decode. `headers` is then empty
    /// and the payload stays readable.
    pub headers_malformed: bool,
    // The delivery handle a policy-aware group consumer commits by. Absent
    // on a native record.
    pub(crate) delivery: Option<Delivery>,
}

impl ConsumerMessage {
    /// Decode the payload as JSON.
    pub fn json<T: DeserializeOwned>(&self) -> Result<T, LaserError> {
        serde_json::from_slice(&self.payload).map_err(|error| LaserError::Codec(error.to_string()))
    }

    // A record whose header block does not decode is still delivered: its
    // payload and position are intact, and dropping it would lose a record a
    // commit before delivery already covered.
    pub(crate) fn of(
        message: IggyMessage,
        partition_id: u32,
        current_offset: u64,
        delivery: Option<Delivery>,
    ) -> Self {
        let (headers, headers_malformed) = match message.user_headers_map() {
            Ok(headers) => (headers.unwrap_or_default(), false),
            Err(_) => (Headers::new(), true),
        };
        let header = message.header;
        Self {
            payload: message.payload,
            headers,
            message_id: header.id,
            checksum: header.checksum,
            position: MessageId::new(partition_id, header.offset),
            current_offset,
            partition_id,
            timestamp_micros: header.timestamp,
            origin_timestamp_micros: header.origin_timestamp,
            user_headers: message.user_headers,
            headers_malformed,
            delivery,
        }
    }
}

impl From<ReceivedMessage> for ConsumerMessage {
    fn from(received: ReceivedMessage) -> Self {
        Self::of(
            received.message,
            received.partition_id,
            received.current_offset,
            None,
        )
    }
}

/// An initialized live reader with server-managed offsets.
pub struct Consumer {
    yielded_zero: BTreeSet<u32>,
    inner: Option<ConsumerInner>,
    manual_commit: bool,
    // Native only: whether a stored offset may move back.
    allow_replay: bool,
    shutdown_target: Option<ConsumerGroupTarget>,
    // The group engine's reads. Behind a lock only so the consumer stays
    // `Sync`: they are reached through `&mut self` alone.
    group_reads: std::sync::Mutex<GroupReads>,
    offsets: Option<Arc<std::sync::Mutex<GroupOffsets>>>,
    // Handled deliveries the group engine acknowledges before its next read.
    #[cfg_attr(not(feature = "agent"), allow(dead_code))]
    deferred: Option<DeferredAcks>,
    returned_native: std::sync::Mutex<VecDeque<ConsumerMessage>>,
    offset_owner: OffsetOwner,
}

type DeferredAcks = Arc<std::sync::Mutex<Vec<Delivery>>>;

/// A consumer's offset as the server stores it, read by
/// [`Consumer::stored_offset`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredOffset {
    /// The offset the server stored for the consumer.
    pub stored_offset: u64,
    /// The partition's current head offset.
    pub current_offset: u64,
}

// One stream for the consumer's life, so a group read allocates nothing per
// record.
type GroupReadStream = Pin<Box<dyn Stream<Item = Result<ConsumerMessage, LaserError>> + Send>>;

#[derive(Default)]
struct GroupReads {
    stream: Option<GroupReadStream>,
    // The last poll answered `Pending`, so the read may hold the engine lock.
    in_flight: bool,
}

impl GroupReads {
    // Ends a read in flight so the caller can take the engine lock.
    fn interrupt(&mut self) {
        if self.in_flight {
            *self = Self::default();
        }
    }
}

// A native consumer resuming a partition that a read error stopped, and the
// error to report once it is back.
type NativeRecovery = Pin<Box<dyn Future<Output = NativeRecovered> + Send>>;

// A finished native recovery: the consumer and the error the caller still sees.
struct NativeRecovered {
    consumer: Box<IggyConsumer>,
    reported: Option<LaserError>,
}

enum ConsumerInner {
    Native(Box<IggyConsumer>),
    // Behind a lock only so the consumer stays `Sync`, like the group read.
    Recovering(std::sync::Mutex<NativeRecovery>),
    Group(Arc<tokio::sync::Mutex<GroupEngine>>),
}

struct ConsumerGroupTarget {
    laser: crate::laser::Laser,
    stream: String,
    topic: String,
    group: String,
}

// The identity the server stores a consumer's offsets under: its consumer
// name, or its group.
struct OffsetOwner {
    laser: crate::laser::Laser,
    stream: String,
    topic: String,
    consumer: iggy::prelude::Consumer,
}

/// The offsets a policy-aware group consumer reports: the last record it
/// yielded and the last one it stored, per partition.
#[derive(Default)]
struct GroupOffsets {
    consumed: BTreeMap<u32, u64>,
    stored: BTreeMap<u32, u64>,
    // The partitions the reader reads now, `None` before its first read.
    assigned: Option<BTreeSet<u32>>,
}

/// The group-aware delivery engine behind a policy-aware group consumer: the
/// group reader with the commit policy applied on top of its fenced
/// acknowledgments. A new read or graceful shutdown marks the previous
/// delivery handled, and the policy decides when that prefix is stored.
struct GroupEngine {
    reader: FilteredReader,
    commit: CommitPolicy,
    buffered: VecDeque<MatchedRecord>,
    pending_delivery: Option<(Delivery, bool)>,
    pending_flush: Option<CommitPoint>,
    returned: VecDeque<(ConsumerMessage, bool)>,
    yielded_since_flush: u32,
    last_flush: Instant,
    offsets: Arc<std::sync::Mutex<GroupOffsets>>,
    deferred: DeferredAcks,
}

impl GroupEngine {
    async fn next(&mut self) -> Option<Result<ConsumerMessage, LaserError>> {
        if let Err(error) = self.finish_delivery().await {
            return Some(Err(error));
        }
        if let Err(error) = self.apply_deferred().await {
            return Some(Err(error));
        }
        loop {
            if let Some((message, last_of_page)) = self.returned.pop_front() {
                if let Some(delivery) = &message.delivery {
                    self.pending_delivery = Some((delivery.clone(), last_of_page));
                }
                return Some(Ok(message));
            }
            if let Some(record) = self.buffered.pop_front() {
                let delivery = record.delivery();
                self.pending_delivery = Some((delivery.clone(), self.buffered.is_empty()));
                self.offsets
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .consumed
                    .insert(delivery.partition_id, delivery.offset);
                return Some(Ok(ConsumerMessage::of(
                    record.message,
                    record.partition_id,
                    record.frontier,
                    Some(delivery),
                )));
            }
            // One bounded round at a time, so an interval policy stores the
            // handled prefix while the partitions stay idle instead of
            // waiting for the next page to arrive.
            let page = loop {
                if let Err(error) = self.apply_deferred().await {
                    return Some(Err(error));
                }
                if let Err(error) = self.flush_if_due(CommitPoint::Poll).await {
                    return Some(Err(error));
                }
                match self.reader.read_round().await {
                    Ok((Some(page), _)) => break page,
                    Ok((None, true)) => {}
                    Ok((None, false)) => {
                        let limit = self.commit_due_in();
                        self.reader.wait_for_records_within(limit).await;
                    }
                    Err(error) => {
                        self.sync_stored();
                        return Some(Err(error));
                    }
                }
            };
            self.sync_stored();
            self.buffered.extend(page.records);
        }
    }

    // How long an idle wait may hold before an interval policy has to store
    // the handled prefix. `None` with nothing handled to store.
    fn commit_due_in(&self) -> Option<Duration> {
        if self.yielded_since_flush == 0 {
            return None;
        }
        commit_interval(self.commit)
            .map(|interval| interval.saturating_sub(self.last_flush.elapsed()))
    }

    async fn finish_delivery(&mut self) -> Result<(), LaserError> {
        if let Some((delivery, last_of_page)) = self.pending_delivery.take()
            && self.commit != CommitPolicy::Disabled
        {
            self.reader.handled(&delivery);
            self.yielded_since_flush = self.yielded_since_flush.saturating_add(1);
            self.pending_flush = Some(CommitPoint::Delivered { last_of_page });
        }
        if let Some(point) = self.pending_flush {
            self.flush_if_due(point).await?;
            self.pending_flush = None;
        }
        Ok(())
    }

    async fn flush_if_due(&mut self, point: CommitPoint) -> Result<(), LaserError> {
        if !commit_due(
            self.commit,
            self.yielded_since_flush,
            self.last_flush.elapsed(),
            point,
        ) {
            return Ok(());
        }
        if let Err(error) = self.reader.flush_completed().await {
            let reason = error.filter_reason();
            if matches!(
                reason,
                Some(
                    laser_wire::filter::FilterErrorReason::Conflict
                        | laser_wire::filter::FilterErrorReason::SourceChanged
                )
            ) {
                self.buffered
                    .retain(|record| self.reader.owns_record(record));
                self.pending_flush = None;
                self.yielded_since_flush = 0;
            }
            self.sync_stored();
            // A policy change fences the prefix read under the old policy.
            // Those records are delivered again from the stored offset, so
            // nothing is lost and the consumer goes on without an error.
            if reason == Some(laser_wire::filter::FilterErrorReason::Conflict) {
                tracing::warn!(target: "laser", %error, "the group's policy changed, progress read under the old policy is delivered again");
                self.last_flush = Instant::now();
                return Ok(());
            }
            return Err(error);
        }
        self.yielded_since_flush = 0;
        self.last_flush = Instant::now();
        self.sync_stored();
        Ok(())
    }

    async fn commit(&mut self, delivery: &Delivery) -> Result<(), LaserError> {
        self.reader.ack_through_delivery(delivery).await?;
        self.sync_stored();
        Ok(())
    }

    // Acknowledge the deliveries handled since the last read, in the order
    // they were handled. A failed acknowledgment stays queued with the ones
    // after it, so the next attempt resends it. A partition this member lost
    // refuses its acknowledgment, and the new owner reads those records
    // again, so that one is dropped.
    async fn apply_deferred(&mut self) -> Result<(), LaserError> {
        loop {
            let next = self
                .deferred
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .first()
                .cloned();
            let Some(delivery) = next else {
                return Ok(());
            };
            match self.commit(&delivery).await {
                Ok(()) => {}
                Err(error)
                    if error.filter_reason()
                        == Some(laser_wire::filter::FilterErrorReason::MembershipStale) =>
                {
                    tracing::debug!(target: "laser", %error, partition_id = delivery.partition_id, "a handled record of a lost partition is read again by its new owner");
                }
                Err(error) => return Err(error),
            }
            self.deferred
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(0);
        }
    }

    fn sync_stored(&self) {
        let mut offsets = self
            .offsets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        offsets.stored = self.reader.stored_offsets();
        offsets.assigned = Some(self.reader.partitions().into_iter().collect());
    }
}

impl Consumer {
    /// Wait for the next record.
    pub async fn next(&mut self) -> Option<Result<ConsumerMessage, LaserError>> {
        futures::StreamExt::next(self).await
    }

    /// Return a delivery that a language binding could not hand to its caller.
    #[doc(hidden)]
    pub async fn return_delivery(&self, message: ConsumerMessage) -> Result<(), LaserError> {
        match self.inner.as_ref() {
            Some(ConsumerInner::Group(engine)) => {
                self.group_reads
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .interrupt();
                let mut engine = engine.lock().await;
                let pending = engine.pending_delivery.as_ref().ok_or_else(|| {
                    LaserError::Invalid(
                        "only the latest unhandled delivery can be returned".to_owned(),
                    )
                })?;
                if !message
                    .delivery
                    .as_ref()
                    .is_some_and(|delivery| pending.0.same(delivery))
                {
                    return Err(LaserError::Invalid(
                        "the delivery is not the latest one from this consumer".to_owned(),
                    ));
                }
                let last_of_page = pending.1;
                engine.pending_delivery = None;
                engine.returned.push_front((message, last_of_page));
                Ok(())
            }
            Some(ConsumerInner::Native(_) | ConsumerInner::Recovering(_)) => {
                self.returned_native
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push_front(message);
                Ok(())
            }
            None => Err(LaserError::Invalid(
                "consumer has been shut down".to_owned(),
            )),
        }
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

    /// Store a handled record's offset on the server. A policy-aware group
    /// consumer stores the contiguous prefix of its partition through this
    /// record, so earlier records yielded out of order cannot be skipped.
    pub async fn commit(&self, message: &ConsumerMessage) -> Result<(), LaserError> {
        match self.inner.as_ref() {
            Some(ConsumerInner::Group(engine)) => {
                let delivery = message.delivery.as_ref().ok_or_else(|| {
                    LaserError::Invalid(
                        "the record was not delivered by this group consumer".to_owned(),
                    )
                })?;
                self.group_reads
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .interrupt();
                engine.lock().await.commit(delivery).await
            }
            _ => {
                self.store_offset(message.position.offset, Some(message.partition_id))
                    .await
            }
        }
    }

    // Commit a handled record without waiting for a read in flight. The
    // group engine queues the acknowledgment and sends it before its next
    // read, so the read is never cancelled. A native consumer stores the
    // offset at once.
    #[cfg_attr(not(feature = "agent"), allow(dead_code))]
    pub(crate) async fn commit_handled(&self, message: &ConsumerMessage) -> Result<(), LaserError> {
        match (self.inner.as_ref(), &self.deferred) {
            (Some(ConsumerInner::Group(_)), Some(deferred)) => {
                let delivery = message.delivery.clone().ok_or_else(|| {
                    LaserError::Invalid(
                        "the record was not delivered by this group consumer".to_owned(),
                    )
                })?;
                deferred
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(delivery);
                Ok(())
            }
            _ => self.commit(message).await,
        }
    }

    // The partitions a policy-aware group consumer reads now. `None` for a
    // native consumer, whose assignment only the server knows, and before
    // the first read.
    #[cfg_attr(not(feature = "agent"), allow(dead_code))]
    pub(crate) fn assigned_partitions(&self) -> Option<BTreeSet<u32>> {
        self.offsets.as_ref().and_then(|offsets| {
            offsets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .assigned
                .clone()
        })
    }

    /// Store an explicit server offset. Native only: a policy-aware group
    /// consumer refuses it, because an arbitrary offset bypasses the group's
    /// acknowledgment contract. Commit a delivered record instead. Without
    /// [`allow_replay`](ConsumerBuilder::allow_replay) an offset at or below
    /// the one this consumer stored last is not sent, so the stored offset
    /// never moves back.
    pub async fn store_offset(
        &self,
        offset: u64,
        partition: Option<u32>,
    ) -> Result<(), LaserError> {
        let consumer = self.native()?;
        if !self.allow_replay {
            let partition_id = partition.unwrap_or_else(|| consumer.partition_id());
            if consumer
                .get_last_stored_offset(partition_id)
                .is_some_and(|stored| stored >= offset)
            {
                return Ok(());
            }
        }
        consumer.store_offset(offset, partition).await?;
        Ok(())
    }

    /// Delete the server offset for a partition or the current partition.
    /// Native only, like [`store_offset`](Self::store_offset).
    pub async fn delete_offset(&self, partition: Option<u32>) -> Result<(), LaserError> {
        self.native()?.delete_offset(partition).await?;
        Ok(())
    }

    /// Return the last locally yielded offset for a partition.
    pub fn last_consumed_offset(&self, partition: u32) -> Option<u64> {
        match self.inner.as_ref() {
            Some(ConsumerInner::Native(consumer)) => consumer.get_last_consumed_offset(partition),
            Some(ConsumerInner::Recovering(_)) => None,
            Some(ConsumerInner::Group(_)) => self.offsets.as_ref().and_then(|offsets| {
                offsets
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .consumed
                    .get(&partition)
                    .copied()
            }),
            None => None,
        }
    }

    /// Return local offset bookkeeping: the native SDK's stored offset, whose
    /// initial zero does not prove a durable checkpoint exists, or the last
    /// offset a policy-aware group consumer acknowledged. Use `Next` for
    /// server-side resume.
    pub fn last_stored_offset(&self, partition: u32) -> Option<u64> {
        match self.inner.as_ref() {
            Some(ConsumerInner::Native(consumer)) => consumer.get_last_stored_offset(partition),
            Some(ConsumerInner::Recovering(_)) => None,
            Some(ConsumerInner::Group(_)) => self.offsets.as_ref().and_then(|offsets| {
                offsets
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .stored
                    .get(&partition)
                    .copied()
            }),
            None => None,
        }
    }

    /// Read the offset the server stores for this consumer, or for its group,
    /// on `partition`, with the partition's current head. `None` when the
    /// server stores nothing for it yet. A server read, unlike the local
    /// bookkeeping of [`last_stored_offset`](Self::last_stored_offset).
    pub async fn stored_offset(&self, partition: u32) -> Result<Option<StoredOffset>, LaserError> {
        let owner = &self.offset_owner;
        let info = owner
            .laser
            .client()
            .get_consumer_offset(
                &owner.consumer,
                &Identifier::named(&owner.stream)?,
                &Identifier::named(&owner.topic)?,
                Some(partition),
            )
            .await?;
        Ok(info.map(|info| StoredOffset {
            stored_offset: info.stored_offset,
            current_offset: info.current_offset,
        }))
    }

    /// Stop polling and leave the group. Automatic policies store the handled
    /// prefix first. For group consumers, a clean stop marks the last
    /// delivery handled. Native polling commits before delivery, so its
    /// shutdown is not a processing checkpoint. [`CommitPolicy::Disabled`]
    /// preserves the last explicit commit.
    pub async fn shutdown(&mut self) -> Result<(), LaserError> {
        *self
            .group_reads
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = GroupReads::default();
        match self.inner.take() {
            None => Ok(()),
            Some(ConsumerInner::Group(engine)) => {
                self.shutdown_target.take();
                let engine = Arc::try_unwrap(engine).map_err(|_| {
                    LaserError::Invalid("the consumer is still being polled".to_owned())
                })?;
                let mut engine = engine.into_inner();
                let completed = engine.finish_delivery().await;
                let completed = match completed {
                    Ok(()) => engine.apply_deferred().await,
                    Err(error) => Err(error),
                };
                let closed = engine.reader.close().await;
                completed.and(closed)
            }
            Some(ConsumerInner::Native(consumer)) => self.shutdown_native(consumer).await,
            Some(ConsumerInner::Recovering(recovery)) => {
                let recovery = recovery
                    .into_inner()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let recovered = recovery.await;
                if let Some(error) = recovered.reported {
                    tracing::debug!(target: "laser", %error, "a read error arrived while the consumer shut down");
                }
                self.shutdown_native(recovered.consumer).await
            }
        }
    }

    async fn shutdown_native(&mut self, mut consumer: Box<IggyConsumer>) -> Result<(), LaserError> {
        if self.manual_commit {
            drop(consumer);
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
        let mut stored = Ok(());
        for partition in &self.yielded_zero {
            if consumer
                .get_last_stored_offset(*partition)
                .is_none_or(|offset| offset == 0)
            {
                // The native shutdown treats an unset stored offset as zero.
                // Explicitly store a delivered zero for automatic policies.
                stored = stored.and(consumer.store_offset(0, Some(*partition)).await);
            }
        }
        let stopped = consumer.shutdown().await;
        self.shutdown_target.take();
        stored.and(stopped).map_err(Into::into)
    }

    fn native(&self) -> Result<&IggyConsumer, LaserError> {
        match self.inner.as_ref() {
            Some(ConsumerInner::Native(consumer)) => Ok(consumer),
            Some(ConsumerInner::Recovering(_)) => Err(LaserError::Invalid(
                "the consumer is resuming a partition a read error stopped, read its next record first"
                    .to_owned(),
            )),
            Some(ConsumerInner::Group(_)) => Err(LaserError::Invalid(
                "explicit offsets bypass the group's acknowledgment contract, commit a delivered record instead"
                    .to_owned(),
            )),
            None => Err(LaserError::Invalid(
                "consumer has been shut down".to_owned(),
            )),
        }
    }
}

impl Stream for Consumer {
    type Item = Result<ConsumerMessage, LaserError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if matches!(
            self.inner,
            Some(ConsumerInner::Native(_) | ConsumerInner::Recovering(_))
        ) && let Some(message) = self
            .returned_native
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pop_front()
        {
            return Poll::Ready(Some(Ok(message)));
        }
        let engine = loop {
            match self.inner.as_mut() {
                None => return Poll::Ready(None),
                Some(ConsumerInner::Recovering(recovery)) => match recovery
                    .get_mut()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_mut()
                    .poll(context)
                {
                    Poll::Ready(recovered) => {
                        self.inner = Some(ConsumerInner::Native(recovered.consumer));
                        if let Some(error) = recovered.reported {
                            return Poll::Ready(Some(Err(error)));
                        }
                    }
                    Poll::Pending => return Poll::Pending,
                },
                Some(ConsumerInner::Native(inner)) => match Pin::new(inner).poll_next(context) {
                    Poll::Ready(Some(Ok(message))) => {
                        let message = ConsumerMessage::from(message);
                        if message.position.offset == 0 {
                            self.yielded_zero.insert(message.partition_id);
                        } else {
                            self.yielded_zero.remove(&message.partition_id);
                        }
                        return Poll::Ready(Some(Ok(message)));
                    }
                    Poll::Ready(Some(Err(error))) => {
                        let recovery = ReadRecovery::from(&error);
                        if recovery == ReadRecovery::Report {
                            return Poll::Ready(Some(Err(error.error.into())));
                        }
                        match self.inner.take() {
                            Some(ConsumerInner::Native(consumer)) => {
                                let client = self.offset_owner.laser.client();
                                self.inner =
                                    Some(ConsumerInner::Recovering(std::sync::Mutex::new(
                                        Box::pin(recover_native(client, consumer, error)),
                                    )));
                            }
                            other => {
                                self.inner = other;
                                return Poll::Ready(Some(Err(error.error.into())));
                            }
                        }
                    }
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                },
                Some(ConsumerInner::Group(engine)) => break Arc::clone(engine),
            }
        };
        let reads = self
            .group_reads
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let stream = reads.stream.get_or_insert_with(|| group_reads(engine));
        match stream.as_mut().poll_next(context) {
            Poll::Pending => {
                reads.in_flight = true;
                Poll::Pending
            }
            Poll::Ready(Some(item)) => {
                reads.in_flight = false;
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                *reads = GroupReads::default();
                Poll::Ready(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::MAX_REPLICATION_FACTOR;
    use iggy::prelude::NonZeroIggyDuration;
    use std::num::NonZeroU32;

    fn producer_builder() -> ProducerBuilder {
        let laser = crate::laser::Laser::from_client(iggy::prelude::IggyClient::default());
        laser.stream("fleet").topic("readings").producer()
    }

    #[tokio::test]
    async fn given_both_expiry_settings_when_building_a_producer_then_should_refuse_it() {
        let result = producer_builder()
            .expire_after(Duration::from_secs(60))
            .never_expire()
            .build()
            .await;
        assert!(
            matches!(result, Err(LaserError::Invalid(message)) if message.contains("not both"))
        );
        let result = producer_builder()
            .never_expire()
            .expire_after(Duration::from_secs(60))
            .build()
            .await;
        assert!(
            matches!(result, Err(LaserError::Invalid(message)) if message.contains("not both"))
        );
    }

    #[tokio::test]
    async fn given_both_size_settings_when_building_a_producer_then_should_refuse_it() {
        let result = producer_builder()
            .max_topic_bytes(1_048_576)
            .unlimited_topic_size()
            .build()
            .await;
        assert!(
            matches!(result, Err(LaserError::Invalid(message)) if message.contains("not both"))
        );
    }

    #[tokio::test]
    async fn given_a_zero_expiry_or_size_when_building_a_producer_then_should_refuse_it() {
        let result = producer_builder()
            .expire_after(Duration::ZERO)
            .build()
            .await;
        assert!(matches!(result, Err(LaserError::Invalid(_))));
        let result = producer_builder().max_topic_bytes(0).build().await;
        assert!(matches!(result, Err(LaserError::Invalid(_))));
    }

    #[tokio::test]
    async fn given_a_replication_factor_outside_one_to_thirty_two_when_building_a_producer_then_should_refuse_it_before_sending()
     {
        for factor in [0, MAX_REPLICATION_FACTOR + 1] {
            let result = producer_builder().replication_factor(factor).build().await;
            assert!(
                matches!(&result, Err(LaserError::Invalid(message)) if message == "replication factor must be 1..=32"),
                "factor {factor}: {:?}",
                result.err()
            );
        }
    }

    #[tokio::test]
    async fn given_a_replication_factor_outside_one_to_thirty_two_when_ensuring_a_topic_then_should_refuse_it_before_sending()
     {
        let laser = crate::laser::Laser::from_client(iggy::prelude::IggyClient::default());
        let topic = laser.stream("fleet").topic("readings");
        for factor in [0, MAX_REPLICATION_FACTOR + 1] {
            let result = topic.ensure(1, Some(factor)).await;
            assert!(
                matches!(&result, Err(LaserError::Invalid(message)) if message == "replication factor must be 1..=32"),
                "factor {factor}: {result:?}"
            );
        }
    }

    #[test]
    fn given_one_form_of_each_setting_when_resolved_then_should_map_to_the_topic_limits() {
        let (expiry, size) = producer_builder()
            .never_expire()
            .unlimited_topic_size()
            .topic_limits()
            .expect("one form each");
        assert_eq!(expiry, IggyExpiry::NeverExpire);
        assert_eq!(size, MaxTopicSize::Unlimited);
        let (expiry, size) = producer_builder().topic_limits().expect("defaults");
        assert_eq!(expiry, IggyExpiry::ServerDefault);
        assert_eq!(size, MaxTopicSize::ServerDefault);
    }

    #[test]
    fn given_a_consumer_when_shared_across_tasks_then_should_be_send_and_sync() {
        fn shared<T: Send + Sync>() {}
        shared::<Consumer>();
        shared::<ConsumerBuilder>();
        shared::<ConsumerMessage>();
    }

    #[test]
    fn given_a_group_handle_when_its_stored_offset_is_addressed_then_should_name_the_group() {
        let laser = crate::laser::Laser::from_client(iggy::prelude::IggyClient::default());
        let topic = laser.stream("fleet").topic("readings");
        let named = group_offset_consumer(&topic.consumer_group("workers")).expect("named group");
        assert_eq!(named.kind, iggy::prelude::ConsumerKind::ConsumerGroup);
        assert_eq!(named.id, Identifier::named("workers").expect("valid name"));
        let numeric = group_offset_consumer(&topic.consumer_group_id(7)).expect("numeric group");
        assert_eq!(numeric.id, Identifier::numeric(7).expect("valid id"));
        assert!(matches!(
            group_offset_consumer(&topic.consumer_group_id(u64::from(u32::MAX) + 1)),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_a_server_with_group_reads_when_classified_then_should_use_the_group_engine() {
        let capabilities = Capabilities::OPEN.with_filters(true, true);
        assert!(policy_aware(&capabilities).expect("classified"));
    }

    #[test]
    fn given_group_reads_without_an_evaluator_when_classified_then_should_preserve_group_policy() {
        let mut capabilities = Capabilities::OPEN;
        capabilities.filters.group_policy_reads = true;
        assert!(policy_aware(&capabilities).expect("group policy resolves without evaluation"));
    }

    #[test]
    fn given_a_server_serving_filters_without_group_reads_when_classified_then_should_require_an_upgrade()
     {
        let mut capabilities = Capabilities::OPEN.with_filters(true, true);
        capabilities.filters.group_policy_reads = false;
        let error = policy_aware(&capabilities).expect_err("an old server is refused");
        assert!(matches!(
            error,
            LaserError::Unsupported {
                feature: Some("group_policy_reads"),
                ..
            }
        ));
    }

    #[test]
    fn given_a_probe_that_established_nothing_when_classified_then_should_not_read_natively() {
        let mut capabilities = Capabilities::OPEN;
        for hello in [HelloOutcome::Unknown, HelloOutcome::Failed] {
            capabilities.hello = hello;
            assert!(matches!(
                policy_aware(&capabilities),
                Err(LaserError::Timeout(_))
            ));
        }
        for hello in [HelloOutcome::Rejected, HelloOutcome::Answered] {
            capabilities.hello = hello;
            assert!(
                !policy_aware(&capabilities).expect("positively absent"),
                "{hello:?} without filters reads natively"
            );
        }
    }

    #[test]
    fn given_each_commit_policy_when_a_record_is_yielded_then_should_store_on_its_own_cadence() {
        let second = Duration::from_secs(1);
        let middle = CommitPoint::Delivered {
            last_of_page: false,
        };
        let last = CommitPoint::Delivered { last_of_page: true };
        assert!(!commit_due(CommitPolicy::Disabled, 5, second, last));
        assert!(!commit_due(
            CommitPolicy::Disabled,
            5,
            second,
            CommitPoint::Poll
        ));
        assert!(commit_due(CommitPolicy::Each, 1, Duration::ZERO, middle));
        assert!(!commit_due(
            CommitPolicy::Each,
            1,
            Duration::ZERO,
            CommitPoint::Poll
        ));
        assert!(commit_due(CommitPolicy::All, 1, Duration::ZERO, last));
        assert!(!commit_due(CommitPolicy::All, 1, Duration::ZERO, middle));
        assert!(commit_due(
            CommitPolicy::Every(3),
            3,
            Duration::ZERO,
            middle
        ));
        assert!(!commit_due(
            CommitPolicy::Every(3),
            2,
            Duration::ZERO,
            middle
        ));
        assert!(commit_due(
            CommitPolicy::Interval(second),
            0,
            second,
            CommitPoint::Poll
        ));
        assert!(!commit_due(
            CommitPolicy::Interval(second),
            9,
            Duration::from_millis(999),
            last
        ));
        assert!(commit_due(
            CommitPolicy::IntervalOrEvery(second, 10),
            10,
            Duration::ZERO,
            middle
        ));
        assert!(commit_due(
            CommitPolicy::IntervalOrAll(second),
            1,
            Duration::ZERO,
            last
        ));
    }

    #[test]
    fn given_the_polling_policy_when_the_next_poll_starts_then_should_store_the_delivered_prefix() {
        let last = CommitPoint::Delivered { last_of_page: true };
        assert!(
            !commit_due(CommitPolicy::Polling, 4, Duration::ZERO, last),
            "a delivered record alone stores nothing"
        );
        assert!(commit_due(
            CommitPolicy::Polling,
            4,
            Duration::ZERO,
            CommitPoint::Poll
        ));
        assert!(
            !commit_due(CommitPolicy::Polling, 0, Duration::ZERO, CommitPoint::Poll),
            "an idle poll has nothing to store"
        );
        assert!(commit_due(
            CommitPolicy::IntervalOrPolling(Duration::from_secs(1)),
            0,
            Duration::from_secs(1),
            last
        ));
    }

    #[test]
    fn given_zero_frequency_when_building_commit_policy_then_should_reject_it() {
        let error = iggy::prelude::CommitPolicy::try_from(CommitPolicy::Every(0))
            .expect_err("zero cannot be a commit frequency");
        assert!(matches!(error, LaserError::Invalid(_)));
    }

    #[test]
    fn given_zero_interval_when_building_commit_policy_then_should_reject_it() {
        let error = iggy::prelude::CommitPolicy::try_from(CommitPolicy::Interval(Duration::ZERO))
            .expect_err("zero cannot be a commit interval");
        assert!(matches!(error, LaserError::Invalid(_)));
    }

    #[test]
    fn given_each_commit_policy_when_mapped_to_the_native_reader_then_should_keep_its_cadence() {
        let second = Duration::from_secs(1);
        let interval = Some(NonZeroIggyDuration::new(second).expect("one second is non-zero"));
        let auto = |interval, every| {
            iggy::prelude::CommitPolicy::Auto(AutoCommit {
                interval,
                every,
                after: CommitAfter::Processing,
            })
        };
        let three = CommitEvery::Messages(NonZeroU32::new(3).expect("three is non-zero"));
        let cases = [
            (CommitPolicy::Disabled, iggy::prelude::CommitPolicy::Manual),
            (CommitPolicy::Polling, iggy::prelude::CommitPolicy::OnRead),
            (
                CommitPolicy::IntervalOrPolling(second),
                iggy::prelude::CommitPolicy::OnRead,
            ),
            (CommitPolicy::Interval(second), auto(interval, None)),
            (CommitPolicy::All, auto(None, Some(CommitEvery::Batch))),
            (
                CommitPolicy::IntervalOrAll(second),
                auto(interval, Some(CommitEvery::Batch)),
            ),
            (CommitPolicy::Each, auto(None, Some(CommitEvery::Message))),
            (
                CommitPolicy::IntervalOrEach(second),
                auto(interval, Some(CommitEvery::Message)),
            ),
            (CommitPolicy::Every(3), auto(None, Some(three))),
            (
                CommitPolicy::IntervalOrEvery(second, 3),
                auto(interval, Some(three)),
            ),
        ];
        for (policy, expected) in cases {
            assert_eq!(
                iggy::prelude::CommitPolicy::try_from(policy).expect("a valid policy maps"),
                expected,
                "{policy:?}"
            );
        }
    }

    #[test]
    fn given_each_commit_policy_when_an_idle_group_read_waits_then_should_bound_it_by_its_interval()
    {
        let second = Duration::from_secs(1);
        for policy in [
            CommitPolicy::Interval(second),
            CommitPolicy::IntervalOrPolling(second),
            CommitPolicy::IntervalOrAll(second),
            CommitPolicy::IntervalOrEach(second),
            CommitPolicy::IntervalOrEvery(second, 3),
        ] {
            assert_eq!(commit_interval(policy), Some(second), "{policy:?}");
        }
        for policy in [
            CommitPolicy::Disabled,
            CommitPolicy::Polling,
            CommitPolicy::All,
            CommitPolicy::Each,
            CommitPolicy::Every(3),
        ] {
            assert_eq!(commit_interval(policy), None, "{policy:?}");
        }
    }

    #[test]
    fn given_each_read_error_when_classified_then_should_pick_its_recovery() {
        let skip = PollingStrategy::offset(8);
        let cases = [
            (
                read_error(Some(2), IggyError::FetchProgressUncertain(7), None),
                ReadRecovery::Abandon {
                    partition_id: 2,
                    operation: 7,
                },
            ),
            (
                read_error(Some(2), IggyError::PartitionLaneBlocked(2, 7), Some(skip)),
                ReadRecovery::Abandon {
                    partition_id: 2,
                    operation: 7,
                },
            ),
            (
                read_error(Some(2), IggyError::InvalidMessagePayloadLength, Some(skip)),
                ReadRecovery::Skip {
                    partition_id: 2,
                    start: skip,
                },
            ),
            (
                read_error(Some(2), IggyError::HistoryUnavailable, None),
                ReadRecovery::Resume { partition_id: 2 },
            ),
            (
                read_error(Some(2), IggyError::Unauthorized, None),
                ReadRecovery::Stopped { partition_id: 2 },
            ),
            (
                read_error(
                    Some(2),
                    IggyError::TopicNameNotFound("orbit".to_owned(), "fleet".to_owned()),
                    None,
                ),
                ReadRecovery::Stopped { partition_id: 2 },
            ),
            (
                read_error(None, IggyError::Unauthorized, None),
                ReadRecovery::Report,
            ),
            (
                read_error(Some(2), IggyError::TransientNotAccepted, None),
                ReadRecovery::Report,
            ),
            (
                read_error(None, IggyError::FetchProgressUncertain(7), None),
                ReadRecovery::Report,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(ReadRecovery::from(&error), expected, "{error:?}");
        }
    }

    #[tokio::test]
    async fn given_an_uncertain_fetch_when_recovered_then_should_abandon_it_and_resume_after_the_stored_offset()
     {
        for abandoned in [Ok(()), Err(IggyError::OperationNotRetained(7))] {
            let mut target = RecordingRecovery::answering(abandoned, Ok(()));
            let error = read_error(Some(2), IggyError::FetchProgressUncertain(7), None);

            let reported = recover_read(&mut target, error).await;

            assert!(reported.is_none(), "{reported:?}");
            assert_eq!(target.abandoned, vec![7]);
            assert_eq!(target.seeks, vec![(2, PollingStrategy::next())]);
        }
    }

    #[test]
    fn given_each_start_when_configuring_the_native_reader_then_should_read_next_for_a_group_and_the_start_for_a_partition()
     {
        let laser = crate::laser::Laser::from_client(iggy::prelude::IggyClient::default());
        let topic = laser.stream("fleet").topic("readings");
        for start in [
            ConsumerStart::First,
            ConsumerStart::Last,
            ConsumerStart::Next,
            ConsumerStart::Offset(7),
            ConsumerStart::TimestampMicros(9),
        ] {
            let group = topic.consumer_group("workers").consumer().start_at(start);
            let partition = topic.consumer("audit", 0).start_at(start);

            assert_eq!(group.reader_start(), ConsumerStart::Next, "{start:?}");
            assert_eq!(partition.reader_start(), start, "{start:?}");
        }
    }

    #[test]
    fn given_each_start_when_a_group_partition_has_nothing_stored_then_should_seed_the_offset_before_the_record_it_names()
     {
        let cases = [
            (ConsumerStart::First, 10, None, None),
            (ConsumerStart::Next, 10, None, None),
            (ConsumerStart::Offset(0), 10, None, None),
            (ConsumerStart::Offset(7), 10, None, Some(6)),
            (ConsumerStart::Offset(7), 0, None, Some(6)),
            (ConsumerStart::Last, 10, None, Some(8)),
            (ConsumerStart::Last, 1, None, None),
            (ConsumerStart::Last, 0, None, None),
            (ConsumerStart::TimestampMicros(9), 10, Some(5), Some(4)),
            (ConsumerStart::TimestampMicros(9), 10, Some(0), None),
            (ConsumerStart::TimestampMicros(9), 10, None, Some(9)),
            (ConsumerStart::TimestampMicros(9), 0, None, None),
        ];
        for (start, tail, found, expected) in cases {
            assert_eq!(
                group_seed(start, tail, found),
                expected,
                "{start:?} tail {tail} found {found:?}"
            );
        }
    }

    #[tokio::test]
    async fn given_a_partition_the_reader_stopped_after_lasting_refusals_when_recovered_then_should_report_the_refusal_and_resume_after_the_stored_offset()
     {
        let mut target = RecordingRecovery::answering(Ok(()), Ok(()));
        let error = read_error(Some(2), IggyError::Unauthorized, None);

        let reported = recover_read(&mut target, error).await;

        assert!(
            matches!(reported, Some(LaserError::Iggy(IggyError::Unauthorized))),
            "{reported:?}"
        );
        assert!(target.abandoned.is_empty());
        assert_eq!(target.seeks, vec![(2, PollingStrategy::next())]);
    }

    #[tokio::test]
    async fn given_a_failed_abandon_when_recovered_then_should_still_resume_the_partition() {
        let mut target = RecordingRecovery::answering(Err(IggyError::Disconnected), Ok(()));
        let error = read_error(Some(2), IggyError::PartitionLaneBlocked(2, 7), None);

        let reported = recover_read(&mut target, error).await;

        assert!(reported.is_none(), "{reported:?}");
        assert_eq!(target.seeks, vec![(2, PollingStrategy::next())]);
    }

    #[tokio::test]
    async fn given_a_new_partition_history_when_recovered_then_should_resume_after_the_stored_offset()
     {
        let mut target = RecordingRecovery::answering(Ok(()), Ok(()));
        let error = read_error(Some(3), IggyError::HistoryUnavailable, None);

        let reported = recover_read(&mut target, error).await;

        assert!(reported.is_none(), "{reported:?}");
        assert!(target.abandoned.is_empty());
        assert_eq!(target.seeks, vec![(3, PollingStrategy::next())]);
    }

    #[tokio::test]
    async fn given_an_undecodable_record_when_recovered_then_should_report_it_once_and_seek_past_it()
     {
        let skip = PollingStrategy::offset(9);
        let mut target = RecordingRecovery::answering(Ok(()), Ok(()));
        let error = read_error(Some(1), IggyError::InvalidMessagePayloadLength, Some(skip));

        let reported = recover_read(&mut target, error).await;

        assert!(
            matches!(
                reported,
                Some(LaserError::Iggy(IggyError::InvalidMessagePayloadLength))
            ),
            "{reported:?}"
        );
        assert_eq!(target.seeks, vec![(1, skip)]);
    }

    #[tokio::test]
    async fn given_a_failed_seek_when_resuming_a_partition_then_should_report_the_seek_error() {
        let mut target = RecordingRecovery::answering(Ok(()), Err(IggyError::InvalidCommand));
        let error = read_error(Some(2), IggyError::HistoryUnavailable, None);

        let reported = recover_read(&mut target, error).await;

        assert!(
            matches!(reported, Some(LaserError::Iggy(IggyError::InvalidCommand))),
            "{reported:?}"
        );
    }

    #[tokio::test]
    async fn given_any_other_read_error_when_recovered_then_should_report_it_without_a_seek() {
        let mut target = RecordingRecovery::answering(Ok(()), Ok(()));
        let error = read_error(Some(2), IggyError::TransientNotAccepted, None);

        let reported = recover_read(&mut target, error).await;

        assert!(
            matches!(
                reported,
                Some(LaserError::Iggy(IggyError::TransientNotAccepted))
            ),
            "{reported:?}"
        );
        assert!(target.abandoned.is_empty());
        assert!(target.seeks.is_empty());
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

    #[test]
    fn given_bytes_payload_when_lowered_to_iggy_then_should_preserve_pointer_identity() {
        let payload = Bytes::from_static(&[0x42; 4096]);
        let message = ProducerMessage::new(payload.clone())
            .into_iggy()
            .expect("the producer message should lower to Iggy");

        assert_eq!(message.payload.as_ptr(), payload.as_ptr());
    }

    fn read_error(
        partition_id: Option<u32>,
        error: IggyError,
        skip: Option<PollingStrategy>,
    ) -> ReadError {
        ReadError {
            partition_id,
            error,
            skip,
            offset: None,
            record: None,
        }
    }

    struct RecordingRecovery {
        abandon_answer: Result<(), IggyError>,
        seek_answer: Result<(), IggyError>,
        abandoned: Vec<u64>,
        seeks: Vec<(u32, PollingStrategy)>,
    }

    impl RecordingRecovery {
        fn answering(
            abandon_answer: Result<(), IggyError>,
            seek_answer: Result<(), IggyError>,
        ) -> Self {
            Self {
                abandon_answer,
                seek_answer,
                abandoned: Vec::new(),
                seeks: Vec::new(),
            }
        }
    }

    impl PartitionRecovery for RecordingRecovery {
        async fn abandon_operation(&mut self, operation: u64) -> Result<(), IggyError> {
            self.abandoned.push(operation);
            self.abandon_answer.clone()
        }

        async fn seek(
            &mut self,
            partition_id: u32,
            start: PollingStrategy,
        ) -> Result<(), IggyError> {
            self.seeks.push((partition_id, start));
            self.seek_answer.clone()
        }
    }
}

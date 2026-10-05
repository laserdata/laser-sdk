use crate::capabilities::{Capabilities, HelloOutcome};
use crate::error::LaserError;
use crate::filters::reader::{DEFAULT_IDLE_INTERVAL, Delivery, FilteredReader, MatchedRecord};
use crate::stream::consumer_group::ConsumerGroup;
use crate::stream::{HeaderKey, HeaderValue, Topic};
use crate::types::MessageId;
use bytes::Bytes;
use futures::Stream;
use iggy::prelude::{
    AutoCommit, AutoCommitWhen, BackgroundConfig, ConsumerGroupClient, DirectConfig, Identifier,
    IggyConsumer, IggyConsumerBuilder, IggyExpiry, IggyMessage, IggyProducer, IggyTimestamp,
    MaxTopicSize, Partitioning, PollingStrategy, ReceivedMessage, SendMessagesResponse,
};
use laser_wire::filter::{FilterRef, FilteredStart};
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
    const fn into_filtered(self) -> FilteredStart {
        match self {
            Self::First => FilteredStart::First,
            Self::Last => FilteredStart::Last,
            Self::Next => FilteredStart::Next,
            Self::Offset(offset) => FilteredStart::Offset(offset),
            Self::TimestampMicros(timestamp) => FilteredStart::Timestamp(timestamp),
        }
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

impl CommitPolicy {
    fn into_auto_commit(self) -> Result<AutoCommit, LaserError> {
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
            Self::Interval(duration) => AutoCommit::Interval(positive_duration(
                duration,
                "commit interval must be greater than zero",
            )?),
            Self::Polling => AutoCommit::When(AutoCommitWhen::PollingMessages),
            Self::IntervalOrPolling(duration) => AutoCommit::IntervalOrWhen(
                positive_duration(duration, "commit interval must be greater than zero")?,
                AutoCommitWhen::PollingMessages,
            ),
            Self::All => AutoCommit::When(AutoCommitWhen::ConsumingAllMessages),
            Self::IntervalOrAll(duration) => AutoCommit::IntervalOrWhen(
                positive_duration(duration, "commit interval must be greater than zero")?,
                AutoCommitWhen::ConsumingAllMessages,
            ),
            Self::Each => AutoCommit::When(AutoCommitWhen::ConsumingEachMessage),
            Self::IntervalOrEach(duration) => AutoCommit::IntervalOrWhen(
                positive_duration(duration, "commit interval must be greater than zero")?,
                AutoCommitWhen::ConsumingEachMessage,
            ),
            Self::Every(messages) => AutoCommit::When(every(messages)?),
            Self::IntervalOrEvery(duration, messages) => AutoCommit::IntervalOrWhen(
                positive_duration(duration, "commit interval must be greater than zero")?,
                every(messages)?,
            ),
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
    expiry: IggyExpiry,
    max_topic_size: MaxTopicSize,
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
            create_stream: true,
            create_topic: true,
            partitions: 1,
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
    pub fn retry_backoff(mut self, interval: Duration) -> Self {
        self.retry_interval = Some(interval);
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
        let retry_interval = self
            .retry_interval
            .map(|duration| {
                positive_duration(
                    duration,
                    "producer retry interval must be greater than zero",
                )
            })
            .transpose()?;
        builder = builder
            .partitioning(self.routing.into_partitioning()?)
            .send_retries(
                if background { self.retries } else { Some(0) },
                retry_interval,
            );
        builder = if self.create_stream {
            builder.create_stream_if_not_exists()
        } else {
            builder.do_not_create_stream_if_not_exists()
        };
        builder = if self.create_topic {
            builder.create_topic_if_not_exists(self.partitions, self.expiry, self.max_topic_size)
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
                    Ok(producer.init().await?)
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
    pub async fn send(
        &self,
        payload: impl Into<Bytes>,
    ) -> Result<SendMessagesResponse, LaserError> {
        self.send_message(ProducerMessage::new(payload)).await
    }

    /// Send one record using the configured routing.
    pub async fn send_message(
        &self,
        message: ProducerMessage,
    ) -> Result<SendMessagesResponse, LaserError> {
        self.send_iggy(vec![message.into_iggy()?], None).await
    }

    /// Send one record with a per-call routing override.
    pub async fn send_with_routing(
        &self,
        message: ProducerMessage,
        routing: Routing,
    ) -> Result<SendMessagesResponse, LaserError> {
        self.send_iggy(
            vec![message.into_iggy()?],
            Some(Arc::new(routing.into_partitioning()?)),
        )
        .await
    }

    /// Send one record with a per-call partition key.
    pub async fn send_keyed(
        &self,
        message: ProducerMessage,
        key: impl Into<Vec<u8>>,
    ) -> Result<SendMessagesResponse, LaserError> {
        self.send_with_routing(message, Routing::key(key)).await
    }

    /// Send one record to a specific partition.
    pub async fn send_to_partition(
        &self,
        message: ProducerMessage,
        partition: u32,
    ) -> Result<SendMessagesResponse, LaserError> {
        self.send_with_routing(message, Routing::Partition(partition))
            .await
    }

    /// Send a batch using the configured routing.
    pub async fn send_batch(
        &self,
        messages: impl IntoIterator<Item = ProducerMessage>,
    ) -> Result<SendMessagesResponse, LaserError> {
        self.send_batch_with_routing(messages, None).await
    }

    /// Send a batch with an optional per-call routing override.
    pub async fn send_batch_with_routing(
        &self,
        messages: impl IntoIterator<Item = ProducerMessage>,
        routing: Option<Routing>,
    ) -> Result<SendMessagesResponse, LaserError> {
        let messages = messages
            .into_iter()
            .map(ProducerMessage::into_iggy)
            .collect::<Result<Vec<_>, _>>()?;
        if messages.is_empty() {
            return Ok(SendMessagesResponse {
                confirmations: Vec::new(),
            });
        }
        let routing = routing
            .map(Routing::into_partitioning)
            .transpose()?
            .map(Arc::new);
        self.send_iggy(messages, routing).await
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

    /// Most records one poll returns. A policy-aware group consumer also
    /// examines at most this many source records per partition poll, so a
    /// selective policy returns fewer records, down to none, while progress
    /// still advances.
    #[must_use]
    pub fn batch_length(mut self, batch_length: u32) -> Self {
        self.batch_length = batch_length;
        self
    }

    /// How long the consumer waits after a poll that found nothing new.
    #[must_use]
    pub fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = Some(poll_interval);
        self
    }

    /// Poll again at once after an empty poll. A policy-aware group consumer
    /// keeps a short wait so an idle partition is not spun on.
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

    /// Native only. A policy-aware group consumer always joins its group.
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

    pub async fn build(self) -> Result<Consumer, LaserError> {
        if self.batch_length == 0 {
            return Err(LaserError::Invalid(
                "consumer batch length must be greater than zero".to_owned(),
            ));
        }
        if let ConsumerTarget::Group(group) = &self.target {
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
        self.commit.into_auto_commit()?;
        if !self.auto_join_group {
            return Err(LaserError::Invalid(
                "a policy-aware group consumer always joins its group, remove auto_join_group(false)"
                    .to_owned(),
            ));
        }
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
                .start(self.start.into_filtered())
                .count(self.batch_length)
                .max_examined(self.batch_length)
                .idle_interval(self.poll_interval.unwrap_or(DEFAULT_IDLE_INTERVAL))
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
        let offsets = Arc::new(std::sync::Mutex::new(GroupOffsets::default()));
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
                },
            )))),
            manual_commit: self.commit == CommitPolicy::Disabled,
            shutdown_target: None,
            next_future: std::sync::Mutex::new(None),
            offsets: Some(offsets),
            returned_native: std::sync::Mutex::new(VecDeque::new()),
        })
    }

    async fn build_native(self) -> Result<Consumer, LaserError> {
        let manual_commit = self.commit == CommitPolicy::Disabled;
        let native_group = match &self.target {
            ConsumerTarget::Group(group) => Some(group.native_name().await?),
            ConsumerTarget::Partition { .. } => None,
        };
        let shutdown_target = match &self.target {
            ConsumerTarget::Group(_) if manual_commit && self.auto_join_group => {
                Some(ConsumerGroupTarget {
                    laser: self.topic.laser.clone(),
                    stream: self.topic.stream()?.to_owned(),
                    topic: self.topic.name.clone(),
                    group: native_group
                        .clone()
                        .ok_or(LaserError::Config("consumer group name is absent"))?,
                })
            }
            _ => None,
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
        builder = builder
            .batch_length(self.batch_length)
            .polling_strategy(self.start.into_polling())
            .auto_commit(self.commit.into_auto_commit()?)
            .polling_retry_interval(positive_duration(
                self.polling_retry_interval,
                "consumer polling retry interval must be greater than zero",
            )?);
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
        let mut consumer = builder.build();
        consumer.init().await?;
        Ok(Consumer {
            yielded_zero: BTreeSet::new(),
            inner: Some(ConsumerInner::Native(Box::new(consumer))),
            manual_commit,
            shutdown_target,
            next_future: std::sync::Mutex::new(None),
            offsets: None,
            returned_native: std::sync::Mutex::new(VecDeque::new()),
        })
    }
}

/// Whether a group consumer reads through the group-aware engine. A server
/// that advertises group-aware reads does. A server that serves consumer
/// filters without them must be upgraded, because its groups may be bound
/// and a native poll would read past their policies. A probe that
/// established nothing is not a server without filters. Only a server whose
/// announcement, or refusal to announce, positively lacks filters reads
/// natively.
fn policy_aware(capabilities: &Capabilities) -> Result<bool, LaserError> {
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

/// When the group engine asks whether to store the handled prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CommitPoint {
    /// A record was just delivered, `last_of_page` when it ended its poll.
    Delivered { last_of_page: bool },
    /// The next poll is about to start.
    Poll,
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
    fn of(
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

/// An initialized live reader with server-managed offsets. A purge restarts
/// the partition at offset 0 without telling an open reader, which can keep
/// its old position and skip the replacement records, so rebuild it after a
/// purge.
pub struct Consumer {
    yielded_zero: BTreeSet<u32>,
    inner: Option<ConsumerInner>,
    manual_commit: bool,
    shutdown_target: Option<ConsumerGroupTarget>,
    // The group engine's read in flight. Behind a lock only so the consumer
    // stays `Sync`: it is reached through `&mut self` alone.
    next_future: std::sync::Mutex<Option<NextFuture>>,
    offsets: Option<Arc<std::sync::Mutex<GroupOffsets>>>,
    returned_native: std::sync::Mutex<VecDeque<ConsumerMessage>>,
}

type NextFuture = Pin<Box<dyn Future<Output = Option<Result<ConsumerMessage, LaserError>>> + Send>>;

enum ConsumerInner {
    Native(Box<IggyConsumer>),
    Group(Arc<tokio::sync::Mutex<GroupEngine>>),
}

struct ConsumerGroupTarget {
    laser: crate::laser::Laser,
    stream: String,
    topic: String,
    group: String,
}

/// The offsets a policy-aware group consumer reports: the last record it
/// yielded and the last one it stored, per partition.
#[derive(Default)]
struct GroupOffsets {
    consumed: BTreeMap<u32, u64>,
    stored: BTreeMap<u32, u64>,
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
}

impl GroupEngine {
    async fn next(&mut self) -> Option<Result<ConsumerMessage, LaserError>> {
        if let Err(error) = self.finish_delivery().await {
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
                if let Err(error) = self.flush_if_due(CommitPoint::Poll).await {
                    return Some(Err(error));
                }
                match self.reader.read_round().await {
                    Ok((Some(page), _)) => break page,
                    Ok((None, true)) => {}
                    Ok((None, false)) => sleep(self.reader.idle_interval()).await,
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

    fn sync_stored(&self) {
        self.offsets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .stored = self.reader.stored_offsets();
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
                *self
                    .next_future
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
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
            Some(ConsumerInner::Native(_)) => {
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
                *self
                    .next_future
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                engine.lock().await.commit(delivery).await
            }
            _ => {
                self.store_offset(message.position.offset, Some(message.partition_id))
                    .await
            }
        }
    }

    /// Store an explicit server offset. Native only: a policy-aware group
    /// consumer refuses it, because an arbitrary offset bypasses the group's
    /// acknowledgment contract. Commit a delivered record instead.
    pub async fn store_offset(
        &self,
        offset: u64,
        partition: Option<u32>,
    ) -> Result<(), LaserError> {
        self.native()?.store_offset(offset, partition).await?;
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

    /// Stop polling and leave the group. Automatic policies store the handled
    /// prefix first. For group consumers, a clean stop marks the last
    /// delivery handled. Native polling commits before delivery, so its
    /// shutdown is not a processing checkpoint. [`CommitPolicy::Disabled`]
    /// preserves the last explicit commit.
    pub async fn shutdown(&mut self) -> Result<(), LaserError> {
        *self
            .next_future
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        match self.inner.take() {
            None => Ok(()),
            Some(ConsumerInner::Group(engine)) => {
                self.shutdown_target.take();
                let engine = Arc::try_unwrap(engine).map_err(|_| {
                    LaserError::Invalid("the consumer is still being polled".to_owned())
                })?;
                let mut engine = engine.into_inner();
                let completed = engine.finish_delivery().await;
                let closed = engine.reader.close().await;
                completed.and(closed)
            }
            Some(ConsumerInner::Native(mut consumer)) => {
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
        }
    }

    fn native(&self) -> Result<&IggyConsumer, LaserError> {
        match self.inner.as_ref() {
            Some(ConsumerInner::Native(consumer)) => Ok(consumer),
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
        if matches!(self.inner, Some(ConsumerInner::Native(_)))
            && let Some(message) = self
                .returned_native
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop_front()
        {
            return Poll::Ready(Some(Ok(message)));
        }
        let engine = match self.inner.as_mut() {
            None => return Poll::Ready(None),
            Some(ConsumerInner::Native(inner)) => {
                return match Pin::new(inner).poll_next(context) {
                    Poll::Ready(Some(Ok(message))) => {
                        let message = ConsumerMessage::from(message);
                        if message.position.offset == 0 {
                            self.yielded_zero.insert(message.partition_id);
                        } else {
                            self.yielded_zero.remove(&message.partition_id);
                        }
                        Poll::Ready(Some(Ok(message)))
                    }
                    Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error.into()))),
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                };
            }
            Some(ConsumerInner::Group(engine)) => Arc::clone(engine),
        };
        let in_flight = self
            .next_future
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next = in_flight
            .get_or_insert_with(|| Box::pin(async move { engine.lock().await.next().await }));
        match next.as_mut().poll(context) {
            Poll::Ready(item) => {
                *in_flight = None;
                Poll::Ready(item)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_consumer_when_shared_across_tasks_then_should_be_send_and_sync() {
        fn shared<T: Send + Sync>() {}
        shared::<Consumer>();
        shared::<ConsumerBuilder>();
        shared::<ConsumerMessage>();
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
    fn given_every_start_when_mapped_to_a_group_read_then_should_keep_its_meaning() {
        assert_eq!(ConsumerStart::First.into_filtered(), FilteredStart::First);
        assert_eq!(ConsumerStart::Last.into_filtered(), FilteredStart::Last);
        assert_eq!(ConsumerStart::Next.into_filtered(), FilteredStart::Next);
        assert_eq!(
            ConsumerStart::Offset(7).into_filtered(),
            FilteredStart::Offset(7)
        );
        assert_eq!(
            ConsumerStart::TimestampMicros(9).into_filtered(),
            FilteredStart::Timestamp(9)
        );
    }

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

    #[test]
    fn given_bytes_payload_when_lowered_to_iggy_then_should_preserve_pointer_identity() {
        let payload = Bytes::from_static(&[0x42; 4096]);
        let message = ProducerMessage::new(payload.clone())
            .into_iggy()
            .expect("the producer message should lower to Iggy");

        assert_eq!(message.payload.as_ptr(), payload.as_ptr());
    }
}

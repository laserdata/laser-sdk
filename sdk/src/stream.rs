use crate::error::LaserError;
use crate::laser::Laser;
use iggy::prelude::IggyMessage;
use std::collections::BTreeMap;

pub use iggy::prelude::{HeaderKey, HeaderValue};

/// The raw Apache Iggy producer/consumer escape hatch: same types
/// [`Topic::iggy_producer`]/[`Topic::iggy_consumer`]/[`Topic::iggy_consumer_group`]
/// return, re-exported so reaching for them doesn't need an `iggy`
/// dependency of your own.
pub use iggy::prelude::{
    BackgroundConfig, BalancedSharding, DirectConfig, IggyConsumer, IggyConsumerBuilder,
    IggyProducer, IggyProducerBuilder, OrderedSharding, Sharding,
};

pub use laser_wire::codecs::{Cbor, Codec, Decoder, Json, Msgpack};
pub use laser_wire::content::ContentType;
pub use laser_wire::headers::{
    CONTENT_TYPE, IDX_PREFIX, INLINE_PAYLOAD, PROJECTION_REF, SCHEMA_ID,
};
pub use laser_wire::limits::MAX_INDEX_ENTRIES_PER_RECORD;

mod publish;
mod record;
mod transport;

pub use publish::{BatchPublishRequest, PublishRequest};
pub use record::{Record, RecordBuilder};
pub use transport::{
    CommitPolicy, Consumer, ConsumerBuilder, ConsumerMessage, ConsumerStart, Headers, Producer,
    ProducerBuilder, ProducerMessage, Routing,
};

impl Laser {
    /// The stream accessor: `name` is a real Apache Iggy stream, the topology
    /// layer grouping topics. Free and synchronous, IO happens at the verbs.
    /// One connection drives any number of streams:
    /// `laser.stream("users").topic("events").publish(..)`.
    pub fn stream(&self, name: impl Into<String>) -> Stream {
        Stream {
            laser: self.clone(),
            name: name.into(),
        }
    }

    /// The topic accessor against this client's default stream (set at connect
    /// or with [`with_default_stream`](Self::with_default_stream)), the
    /// one-word shortcut for the single-stream app. With no default configured
    /// every verb on the handle returns the typed
    /// [`NoStream`](LaserError::NoStream) error naming the fix. Cross-stream
    /// work spells the stream it means: `laser.stream(name).topic(name)`.
    pub fn topic(&self, name: impl Into<String>) -> Topic {
        Topic {
            laser: self.clone(),
            stream: self.default_stream().map(str::to_owned),
            name: name.into(),
        }
    }
}

/// One Apache Iggy stream: the topology handle. Build it with
/// [`Laser::stream`], reach the data through [`topic`](Self::topic).
#[derive(Clone)]
pub struct Stream {
    laser: Laser,
    name: String,
}

impl Stream {
    /// The topic accessor within this stream.
    pub fn topic(&self, name: impl Into<String>) -> Topic {
        Topic {
            laser: self.laser.clone(),
            stream: Some(self.name.clone()),
            name: name.into(),
        }
    }

    /// Idempotently create this stream.
    pub async fn ensure(&self) -> Result<(), LaserError> {
        crate::laser::ensure_stream(self.laser.client(), &self.name).await
    }

    /// This stream's name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// One topic within a stream: the Log primitive's data handle. Build it with
/// [`Stream::topic`] or, against the default stream, [`Laser::topic`]. Verbs
/// resolve the producer through the client's shared cache, so handles are free
/// to construct and clone.
#[derive(Clone)]
pub struct Topic {
    laser: Laser,
    stream: Option<String>,
    name: String,
}

impl Topic {
    /// Open the fluent publish builder (payload via `.payload()` / `.json()` /
    /// `.record()`, then `.send()`). The raw payload path compiles to the same
    /// bytes as the low-level send, nothing is linked unless a typed form is
    /// used. Feature `streaming`.
    pub fn publish(&self) -> PublishRequest<'_> {
        match &self.stream {
            Some(stream) => self.laser.publish_on(stream, &self.name),
            None => self.laser.publish(&self.name),
        }
    }

    /// Open the fluent record-batch builder (add records, then `.send()`), the
    /// typed sibling of [`batch`](Self::batch). Feature `streaming`.
    pub fn publish_batch(&self) -> BatchPublishRequest<'_> {
        match &self.stream {
            Some(stream) => self.laser.publish_batch_on(stream, &self.name),
            None => self.laser.publish_batch(&self.name),
        }
    }

    /// One raw message with explicit user headers, the zero-overhead path.
    /// Keyed partitioning preserves per-key ordering, `None` lets the producer
    /// balance across partitions.
    pub async fn send(
        &self,
        payload: impl Into<Vec<u8>>,
        headers: BTreeMap<HeaderKey, HeaderValue>,
        partition_key: Option<&str>,
    ) -> Result<(), LaserError> {
        let stream = self.stream()?.to_owned();
        self.laser
            .send_with_headers_on(&stream, &self.name, payload.into(), headers, partition_key)
            .await
    }

    /// One Iggy `send_messages` call covering many pre-built messages, all
    /// sharing the same partitioning. An empty batch is a cheap no-op.
    pub async fn batch(
        &self,
        messages: Vec<IggyMessage>,
        partition_key: Option<&str>,
    ) -> Result<(), LaserError> {
        let stream = self.stream()?.to_owned();
        self.laser
            .send_batch_on(&stream, &self.name, messages, partition_key)
            .await
    }

    /// A size-and-time batching publisher over this topic: `.batching()`,
    /// tune `.max_records`/`.max_bytes`/`.linger`/`.partition_key`, then
    /// `.build()`. Opt-in: the unbatched verbs compile to exactly the same
    /// calls as before. Feature `agent` (the linger timer rides tokio).
    #[cfg(feature = "agent")]
    pub fn batching(&self) -> Result<crate::batching::BatchingProducerBuilder, LaserError> {
        let stream = self.stream()?.to_owned();
        Ok(crate::batching::BatchingProducerBuilder::new(
            self.laser.clone(),
            stream,
            self.name.clone(),
        ))
    }

    /// The positional replay handle over this topic: a resumable [`Cursor`]
    /// reading from explicit offsets. The bounded-read rung below the reliable
    /// consumer.
    ///
    /// [`Cursor`]: crate::cursor::Cursor
    pub fn replay(&self) -> Result<crate::cursor::Cursor, LaserError> {
        match &self.stream {
            Some(stream) => self.laser.reader_on(stream, &self.name),
            None => Err(LaserError::NoStream),
        }
    }

    /// Idempotently create this topic with `partitions`, creating the stream
    /// first if needed.
    pub async fn ensure(&self, partitions: u32) -> Result<(), LaserError> {
        let stream = self.stream()?.to_owned();
        self.laser
            .ensure_topic_on(&stream, &self.name, partitions)
            .await
    }

    /// The raw Iggy producer builder for this topic, the substrate front door:
    /// Iggy's own options (batching, partitioning, retries, encryption) with
    /// nothing wrapped. Independent of the fluent path's cached producer.
    pub fn iggy_producer(&self) -> Result<IggyProducerBuilder, LaserError> {
        self.laser.iggy_producer(self.stream()?, &self.name)
    }

    /// The raw Iggy consumer builder named `name` over one `partition` of this
    /// topic. The built `IggyConsumer` implements `futures::Stream`.
    pub fn iggy_consumer(
        &self,
        name: &str,
        partition: u32,
    ) -> Result<IggyConsumerBuilder, LaserError> {
        self.laser
            .iggy_consumer(name, self.stream()?, &self.name, partition)
    }

    /// The raw Iggy consumer-group builder over this topic: the balanced group,
    /// offsets committed under `group`.
    pub fn iggy_consumer_group(&self, group: &str) -> Result<IggyConsumerBuilder, LaserError> {
        self.laser
            .iggy_consumer_group(group, self.stream()?, &self.name)
    }

    /// Build a Laser streaming producer with direct batching, linger, retries,
    /// routing, and optional stream/topic creation. `build().await` initializes
    /// it before returning.
    pub fn producer(&self) -> ProducerBuilder {
        ProducerBuilder::new(self.clone())
    }

    /// Build a live Laser consumer for one partition. The built consumer
    /// implements `futures::Stream` and exposes server-backed offset control.
    pub fn consumer(&self, name: impl Into<String>, partition: u32) -> ConsumerBuilder {
        ConsumerBuilder::partition(self.clone(), name, partition)
    }

    /// Build a live, load-balanced Laser consumer group with server-backed
    /// offsets. The built consumer implements `futures::Stream`.
    pub fn consumer_group(&self, group: impl Into<String>) -> ConsumerBuilder {
        ConsumerBuilder::group(self.clone(), group)
    }

    /// This topic's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    #[cfg(feature = "schema-codecs")]
    pub(crate) fn laser(&self) -> &Laser {
        &self.laser
    }

    fn stream(&self) -> Result<&str, LaserError> {
        self.stream.as_deref().ok_or(LaserError::NoStream)
    }
}

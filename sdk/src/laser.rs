use crate::capabilities::Capabilities;
use crate::error::LaserError;
#[cfg(any(
    feature = "fork",
    feature = "graph",
    feature = "kv",
    feature = "projections",
    feature = "query",
    feature = "rbac",
    feature = "runs"
))]
use bytes::Bytes;
use dashmap::DashMap;
#[cfg(any(
    feature = "fork",
    feature = "graph",
    feature = "kv",
    feature = "projections",
    feature = "query",
    feature = "rbac",
    feature = "runs"
))]
use iggy::binary::BinaryTransport;
#[cfg(any(
    feature = "fork",
    feature = "graph",
    feature = "kv",
    feature = "projections",
    feature = "query",
    feature = "rbac",
    feature = "runs"
))]
use iggy::prelude::locking::IggyRwLockFn;
use iggy::prelude::*;
#[cfg(any(
    feature = "fork",
    feature = "graph",
    feature = "kv",
    feature = "projections",
    feature = "query",
    feature = "rbac",
    feature = "runs"
))]
use laser_wire::framing::decode_named;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tokio::time::{Duration, sleep};

// The generic correlation header and the header caps moved to laser-wire (they
// are wire contract). Re-exported here so the historical paths keep resolving.
pub use laser_wire::headers::{
    CORRELATION_ID, HEADER_FRAMING_BYTES, HEADER_SOFT_CAP, HEADER_VALUE_MAX,
};

// Default ops stream for the control surface (`control.commands`, `dlq`). One
// LaserData Cloud per deployment owns it, so in production it is fixed.
// Overridable via `LaserBuilder::ops_stream` / `Laser::with_ops_stream`. Tests
// isolate it per case the same way they isolate the data stream. Mirrors
// `query::OPS_STREAM`.
/// Default ops stream name (`_agdx`).
pub const OPS_STREAM_DEFAULT: &str = "_agdx";

// Producers are cached per (stream, topic): the ops query path publishes to the
// `_agdx` stream while data rides the customer stream, so the cache key must carry
// the stream too or the two would collide on a shared topic name.
type ProducerKey = (String, String);
type ProducerCell = Arc<OnceCell<Arc<IggyProducer>>>;
const TRANSIENT_SEND_ATTEMPTS: usize = 10;

/// The Laser client. Cheap to `clone`, since the connection and producer cache
/// are shared via an internal `Arc`, so one connection is reused across tasks.
/// Build it through [`Laser::connect`] or [`Laser::builder`]. Never wrap it in
/// your own `Arc`.
#[derive(Clone)]
pub struct Laser {
    inner: Arc<LaserInner>,
    // Read in-crate by the memory and graph facades for synchronous capability
    // checks (the public async `capabilities()` getter hands out a reference).
    pub(crate) capabilities: Capabilities,
    ops_stream: String,
    // The control-command topic on the ops stream. Defaults to
    // `laser_wire::topics::CONTROL_TOPIC` (`control.commands`). Overridable so a
    // deployment that names its ops topics differently still drives projections.
    control_topic: String,
    // The dead-letter topic on the ops stream. Defaults to
    // `laser_wire::topics::DLQ_TOPIC` (`dlq`). Overridable alongside the other
    // ops-stream topic names.
    dlq_topic: String,
    // The change-feed topic on the ops stream. Defaults to
    // `laser_wire::topics::CHANGES_TOPIC` (`changes`). Overridable alongside the
    // other ops-stream topic names.
    changes_topic: String,
    // Optional default data stream. Set via `connect_with_stream` / the builder /
    // `with_default_stream`, it serves the one-word `topic(name)` accessor and
    // the agentic helpers. It lives on `Laser` (not the shared `inner`) so
    // `with_default_stream` re-scopes cheaply, sharing the one connection across
    // any number of streams. `stream(name).topic(name)` ignores it.
    stream: Option<String>,
    // Optional pre-effect policy hook. Per-handle (like `stream`) so
    // `with_governor` re-scopes cheaply, while the state inside is shared by
    // every clone of the governed handle (one session's counters and evidence
    // chain).
    #[cfg(feature = "agent")]
    pub(crate) governor: Option<Arc<crate::govern::GovernorState>>,
}

struct LaserInner {
    // `Arc` so a background reply dispatcher can hold the client without a
    // reference cycle back through `LaserInner` (which would leak the task).
    client: Arc<IggyClient>,
    producers: DashMap<ProducerKey, ProducerCell>,
    // The agent registry read model's per-stream cache, so a fresh `AgentRegistry`
    // resumes the card fold instead of re-reading the registry topic from offset 0.
    // Keyed by data stream (the isolation boundary the registry topic lives on).
    #[cfg(feature = "agent")]
    registry_caches: DashMap<String, Arc<std::sync::Mutex<crate::agent::registry::RegistryCache>>>,
    // Connection metadata has one slot. Reserve it for one logical agent across
    // every clone so a second advertisement cannot overwrite the first route.
    // Presence rides the managed metadata command, so the slot exists only
    // where `advertise_presence` compiles (plus its unit test).
    #[cfg(all(feature = "agent", any(feature = "query", test)))]
    advertised_agent: std::sync::Mutex<Option<crate::types::AgentId>>,
    // One shared reply dispatcher per (data stream, reply topic), so concurrent
    // request/reply waiters read the reply topic once between them instead of each
    // scanning it. Created lazily, driven by a background task that stops when this
    // `Laser` (the last clone) drops.
    #[cfg(feature = "agent")]
    reply_hubs:
        DashMap<(String, String), Arc<tokio::sync::OnceCell<crate::agent::replies::ReplyHub>>>,
    // Optional enrolled-key verifier. When set, the agent registry rejects a
    // quarantine fact that is not validly signed by an enrolled operator key
    // (defense in depth over the registry topic's write access control).
    #[cfg(feature = "sign")]
    verifier: Option<Arc<crate::sign::KeyRegistry>>,
}

impl Laser {
    #[cfg(all(feature = "agent", feature = "query"))]
    pub(crate) fn claim_presence(
        &self,
        requested: crate::types::AgentId,
    ) -> Result<(), LaserError> {
        claim_presence_slot(&self.inner.advertised_agent, requested)
    }

    #[cfg(all(feature = "agent", feature = "query"))]
    pub(crate) fn release_presence(&self) {
        *self
            .inner
            .advertised_agent
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    /// Connect using an Iggy connection string. The connection string is the
    /// only thing required. For a `*.laserdata.cloud` or `*.laserdata.com`
    /// host with no `tls_ca_file=` already set, TLS is auto-attached with
    /// LaserData's public root CA, bundled in the SDK itself. Point
    /// Set `LASER_TLS_CERT=<path>` to override the CA, or disable automatic TLS with `LASER_NO_TLS=1`. Other hosts keep their Apache Iggy TLS settings. Connection strings use the bare `user:password@host:port` form because `Laser::connect` supplies the TCP scheme.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # async fn run() -> Result<(), LaserError> {
    /// Laser::connect("iggy:iggy@127.0.0.1:8090").await?;
    /// # Ok(()) }
    /// ```
    ///
    /// The returned handle has no default stream, so operations name the stream
    /// explicitly: `laser.stream(name).topic(name)`. One connection drives any
    /// number of Iggy streams. To set a default stream so the one-word
    /// `laser.topic(name)` shortcut and the agentic helpers work, use
    /// [`connect_with_stream`](Self::connect_with_stream) or
    /// [`with_default_stream`](Self::with_default_stream).
    #[tracing::instrument(
        target = "laser",
        level = "info",
        skip_all,
        fields(operation = "connect")
    )]
    pub async fn connect(connection_string: &str) -> Result<Self, LaserError> {
        LaserBuilder::default()
            .connection_string(connection_string)
            .build()
            .await
    }

    /// Connect from the environment: `LASER_CONNECTION_STRING` (the whole
    /// iggy connection string, exactly what [`connect`](Self::connect) takes)
    /// plus the optional `LASER_STREAM` pinning the default stream. The same
    /// two variables every deployment guide and the example crate already
    /// use, so a program moves between local, staging, and LaserData Cloud
    /// with no code change. Missing `LASER_CONNECTION_STRING` is a typed
    /// [`Config`](LaserError::Config) error naming the variable.
    pub async fn connect_env() -> Result<Self, LaserError> {
        let connection = std::env::var("LASER_CONNECTION_STRING")
            .map_err(|_| LaserError::Config("LASER_CONNECTION_STRING is not set"))?;
        match std::env::var("LASER_STREAM") {
            Ok(stream) => Self::connect_with_stream(&connection, &stream).await,
            Err(_) => Self::connect(&connection).await,
        }
    }

    /// Connect to the stock local Apache Iggy container at `iggy:iggy@127.0.0.1:8090`.
    pub async fn local() -> Result<Self, LaserError> {
        Self::connect("iggy:iggy@127.0.0.1:8090").await
    }

    /// Connect and pin a default Iggy `stream`, so the one-word
    /// `laser.topic(name)` shortcut and the agentic helpers (`bootstrap` /
    /// `send_agent` / `request`) take just a topic. Any other stream stays one
    /// accessor away (`laser.stream(name).topic(name)`), or re-scope with
    /// [`with_default_stream`](Self::with_default_stream). The default is
    /// purely ergonomic.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # async fn run() -> Result<(), LaserError> {
    /// let laser = Laser::connect_with_stream("iggy:iggy@127.0.0.1:8090", "agent-telemetry").await?;
    /// // publishes to the "agent-telemetry" stream
    /// laser.topic("inferences").ensure(4).await?;
    /// laser.topic("inferences").publish().payload(b"...".to_vec()).send().await?;
    /// # Ok(()) }
    /// ```
    pub async fn connect_with_stream(
        connection_string: &str,
        stream: &str,
    ) -> Result<Self, LaserError> {
        LaserBuilder::default()
            .connection_string(connection_string)
            .stream(stream)
            .build()
            .await
    }

    /// Begin building a `Laser` with non-default options (BYO `IggyClient`,
    /// explicit `Capabilities`, host/credentials instead of a connection string,
    /// an optional default stream).
    pub fn builder() -> LaserBuilder {
        LaserBuilder::default()
    }

    /// Wrap a pre-connected, already logged-in `IggyClient`, with no default
    /// stream. Power-user and test helpers reach for this. Apps use
    /// [`Laser::connect`] or [`Laser::builder`]. Chain
    /// [`with_default_stream`](Self::with_default_stream) to pin a default
    /// stream.
    pub fn from_client(client: IggyClient) -> Self {
        Self {
            inner: Arc::new(LaserInner {
                client: Arc::new(client),
                producers: DashMap::new(),
                #[cfg(feature = "agent")]
                registry_caches: DashMap::new(),
                #[cfg(feature = "agent")]
                #[cfg(all(feature = "agent", any(feature = "query", test)))]
                advertised_agent: std::sync::Mutex::new(None),
                #[cfg(feature = "agent")]
                reply_hubs: DashMap::new(),
                #[cfg(feature = "sign")]
                verifier: None,
            }),
            capabilities: Capabilities::default(),
            ops_stream: OPS_STREAM_DEFAULT.to_owned(),
            control_topic: laser_wire::topics::CONTROL_TOPIC.to_owned(),
            dlq_topic: laser_wire::topics::DLQ_TOPIC.to_owned(),
            changes_topic: laser_wire::topics::CHANGES_TOPIC.to_owned(),
            stream: None,
            #[cfg(feature = "agent")]
            governor: None,
        }
    }

    // The shared reply dispatcher for `reply_topic` on the default data stream,
    // created once per (stream, topic) and cached on the connection. The lock on
    // the map shard is released before the create await (mirroring the producer
    // cache), so one slow first-create never serializes unrelated reply topics.
    #[cfg(feature = "agent")]
    pub(crate) async fn reply_hub(
        &self,
        reply_topic: &crate::provenance::AgentTopic<'_>,
    ) -> Result<crate::agent::replies::ReplyHub, LaserError> {
        let stream = self.stream_required()?.to_owned();
        let topic = reply_topic.topic_string();
        let cell = {
            self.inner
                .reply_hubs
                .entry((stream.clone(), topic))
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };
        let hub = cell
            .get_or_try_init(|| {
                crate::agent::replies::ReplyHub::create(
                    self.inner.client.clone(),
                    stream,
                    reply_topic.as_identifier(),
                )
            })
            .await?;
        Ok(hub.clone())
    }

    /// Returns a clone of this `Laser` with the given capability set. The
    /// underlying connection + producer cache are shared with the original.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    /// Returns a clone of this `Laser` whose query/control surface rides
    /// `ops_stream` instead of the default [`OPS_STREAM_DEFAULT`] (`_agdx`). The
    /// underlying connection and producer cache are shared with the original.
    /// Production keeps the default, since one LaserData Cloud per deployment
    /// owns `_agdx`. Tests override it for per-case isolation.
    #[must_use]
    pub fn with_ops_stream(mut self, ops_stream: impl Into<String>) -> Self {
        self.ops_stream = ops_stream.into();
        self
    }

    /// Returns a clone of this `Laser` whose control commands publish to
    /// `control_topic` on the ops stream instead of the default
    /// (`control.commands`). The underlying connection and producer cache are
    /// shared. Production keeps the default, a deployment with its own ops-topic
    /// naming overrides it.
    #[must_use]
    pub fn with_control_topic(mut self, control_topic: impl Into<String>) -> Self {
        self.control_topic = control_topic.into();
        self
    }

    /// Returns a clone of this `Laser` whose dead-letter capsules publish to
    /// `dlq_topic` on the ops stream instead of the default (`dlq`). The
    /// underlying connection and producer cache are shared. Production keeps the
    /// default, a deployment with its own ops-topic naming overrides it.
    #[must_use]
    pub fn with_dlq_topic(mut self, dlq_topic: impl Into<String>) -> Self {
        self.dlq_topic = dlq_topic.into();
        self
    }

    /// Returns a clone of this `Laser` whose change-feed records publish to
    /// `changes_topic` on the ops stream instead of the default (`changes`). The
    /// underlying connection and producer cache are shared. Production keeps the
    /// default, a deployment with its own ops-topic naming overrides it.
    #[must_use]
    pub fn with_changes_topic(mut self, changes_topic: impl Into<String>) -> Self {
        self.changes_topic = changes_topic.into();
        self
    }

    /// A clone of this `Laser` pinned to a default data `stream`, sharing the one
    /// connection + producer cache. The default exists to serve the one-word
    /// `laser.topic(name)` shortcut and the agentic helpers. Cross-stream work
    /// spells its address with `laser.stream(name).topic(name)`. Takes `&self`,
    /// so you can re-scope the same long-lived connection to as many streams as
    /// you like.
    #[must_use]
    pub fn with_default_stream(&self, stream: impl Into<String>) -> Self {
        let mut scoped = self.clone();
        scoped.stream = Some(stream.into());
        scoped
    }

    /// The raw `IggyClient` this laser holds. Most callers should not need it.
    pub fn client(&self) -> &IggyClient {
        &self.inner.client
    }

    /// This laser's default data stream, if one was set (via
    /// [`connect_with_stream`](Self::connect_with_stream),
    /// [`with_default_stream`](Self::with_default_stream), or the builder).
    /// `None` for a connection-only handle that names the stream per operation
    /// (`laser.stream(name).topic(name)`).
    pub fn default_stream(&self) -> Option<&str> {
        self.stream.as_deref().filter(|value| !value.is_empty())
    }

    // The default stream, or `NoStream` if none is set. Used by the convenience
    // methods that take just a topic.
    pub(crate) fn stream_required(&self) -> Result<&str, LaserError> {
        self.default_stream().ok_or(LaserError::NoStream)
    }

    /// The shared agent-registry cache for the default stream, created on first
    /// use. Per-stream because the registry topic is scoped to the data stream
    /// (the isolation boundary).
    #[cfg(feature = "agent")]
    pub(crate) fn registry_cache(
        &self,
    ) -> Result<Arc<std::sync::Mutex<crate::agent::registry::RegistryCache>>, LaserError> {
        let stream = self.stream_required()?;
        Ok(self
            .inner
            .registry_caches
            .entry(stream.to_owned())
            .or_default()
            .clone())
    }

    /// The enrolled-key verifier the agent registry checks privileged facts
    /// against, if one was set on the builder.
    #[cfg(feature = "sign")]
    pub(crate) fn registry_verifier(&self) -> Option<Arc<crate::sign::KeyRegistry>> {
        self.inner.verifier.clone()
    }

    /// The Iggy stream carrying this laser's query/control ops surface
    /// (default [`OPS_STREAM_DEFAULT`]).
    pub fn ops_stream(&self) -> &str {
        &self.ops_stream
    }

    /// The control-command topic on the ops stream (default `control.commands`).
    pub fn control_topic(&self) -> &str {
        &self.control_topic
    }

    /// The dead-letter topic on the ops stream (default `dlq`).
    pub fn dlq_topic(&self) -> &str {
        &self.dlq_topic
    }

    /// The change-feed topic on the ops stream (default `changes`).
    pub fn changes_topic(&self) -> &str {
        &self.changes_topic
    }

    /// The capability set this laser was built with (default
    /// [`Capabilities::OPEN`]). Async to reserve a future capability negotiation
    /// round-trip. Open features work regardless of the result.
    pub async fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// Idempotently creates `topic` on `stream` with `partitions`, creating the
    /// stream first if needed. Used for the `_agdx` ops stream, which is separate
    /// from this laser's data stream.
    pub(crate) async fn ensure_topic_on(
        &self,
        stream: &str,
        topic: &str,
        partitions: u32,
    ) -> Result<(), LaserError> {
        ensure_stream(&self.inner.client, stream).await?;
        ensure_topic(&self.inner.client, stream, topic, partitions).await
    }

    /// Like [`ensure_topic_on`](Self::ensure_topic_on) with an explicit
    /// message-expiry, for the configurable memory topic.
    #[cfg(feature = "agent")]
    pub(crate) async fn ensure_topic_on_with(
        &self,
        stream: &str,
        topic: &str,
        partitions: u32,
        expiry: IggyExpiry,
    ) -> Result<(), LaserError> {
        ensure_stream(&self.inner.client, stream).await?;
        ensure_topic_with(&self.inner.client, stream, topic, partitions, expiry).await
    }

    /// Low-level send: one message with explicit user-headers, on the default
    /// stream. Keyed partitioning preserves per-key ordering, and `None` lets
    /// the producer balance across partitions. Most callers should use `publish`
    /// or `send_agent`. Requires a default stream. Use
    /// [`send_with_headers_on`](Self::send_with_headers_on) to target an explicit one.
    pub(crate) async fn send_with_headers(
        &self,
        topic: &str,
        payload: impl Into<Vec<u8>>,
        headers: BTreeMap<HeaderKey, HeaderValue>,
        partition_key: Option<&str>,
    ) -> Result<(), LaserError> {
        self.send_with_headers_on(
            self.stream_required()?,
            topic,
            payload,
            headers,
            partition_key,
        )
        .await
    }

    /// Like [`send_with_headers`](Self::send_with_headers) but targets `stream`
    /// instead of this laser's data stream. Used for the `_agdx` ops stream.
    pub(crate) async fn send_with_headers_on(
        &self,
        stream: &str,
        topic: &str,
        payload: impl Into<Vec<u8>>,
        headers: BTreeMap<HeaderKey, HeaderValue>,
        partition_key: Option<&str>,
    ) -> Result<(), LaserError> {
        let payload: Vec<u8> = payload.into();
        let message = IggyMessage::builder()
            .payload(payload.into())
            .user_headers(headers)
            .build()?;
        self.send_batch_on(stream, topic, vec![message], partition_key)
            .await
    }

    /// Low-level batch send: one Iggy `send_messages` call covering many
    /// pre-built `IggyMessage`s. All messages in the batch share the same
    /// partitioning. Without a `partition_key`, Iggy chooses one partition for the
    /// whole call using its balanced partitioner. An empty batch is a cheap no-op.
    pub(crate) async fn send_batch(
        &self,
        topic: &str,
        messages: Vec<IggyMessage>,
        partition_key: Option<&str>,
    ) -> Result<(), LaserError> {
        let stream = self.stream_required()?.to_owned();
        self.send_batch_on(&stream, topic, messages, partition_key)
            .await
    }

    /// Like [`send_batch`](Self::send_batch) but targets `stream` instead of this
    /// laser's data stream. Used for the `_agdx` ops stream.
    #[tracing::instrument(target = "laser", level = "debug", skip_all, fields(topic = %topic, operation = "publish"))]
    pub(crate) async fn send_batch_on(
        &self,
        stream: &str,
        topic: &str,
        messages: Vec<IggyMessage>,
        partition_key: Option<&str>,
    ) -> Result<(), LaserError> {
        if messages.is_empty() {
            return Ok(());
        }
        let partitioning = Arc::new(match partition_key {
            Some(key) => Partitioning::messages_key_str(key)?,
            None => Partitioning::balanced(),
        });
        let key = (stream.to_owned(), topic.to_owned());
        let mut pending = messages;
        for attempt in 0..TRANSIENT_SEND_ATTEMPTS {
            let producer = self.producer_on(stream, topic).await?;
            match producer
                .send_with_partitioning(pending, Some(partitioning.clone()))
                .await
            {
                Ok(()) => return Ok(()),
                Err(IggyError::ProducerSendFailed { cause, failed, .. })
                    if is_transient_iggy_io_error(&cause)
                        && attempt + 1 < TRANSIENT_SEND_ATTEMPTS =>
                {
                    pending = reclaim_failed_messages(failed);
                    self.inner.producers.remove(&key);
                    sleep(Duration::from_millis(50 * (attempt + 1) as u64)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
        unreachable!("retry loop either sends or returns the last publish error")
    }

    /// Send a managed command `code` with `payload` over the existing binary
    /// connection and return the raw reply bytes. The query path uses it for
    /// `AGDX_QUERY` on the server, and the connect-time probe uses it for
    /// `AGDX_HELLO`. `IggyClient` does not surface `BinaryTransport`, so we
    /// dispatch on the underlying transport variant. Each binary transport
    /// implements it. The HTTP and wrapping `Iggy` variants do not, so those
    /// report `InvalidCommand`.
    #[cfg(any(
        feature = "fork",
        feature = "graph",
        feature = "kv",
        feature = "projections",
        feature = "query",
        feature = "rbac",
        feature = "runs"
    ))]
    #[tracing::instrument(target = "laser", level = "debug", skip_all, fields(code = code, operation = "managed"))]
    pub(crate) async fn send_raw_with_response(
        &self,
        code: u32,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, IggyError> {
        let payload = bytes::Bytes::from(payload);
        let wrapper = self.inner.client.client();
        let guard = wrapper.read().await;
        let reply = match &*guard {
            ClientWrapper::Tcp(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::Quic(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::WebSocket(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::Http(_) | ClientWrapper::Iggy(_) => Err(IggyError::InvalidCommand),
        }?;
        Ok(reply.to_vec())
    }

    /// A raw Iggy `IggyProducerBuilder` for `(stream, topic)`. laser-sdk builds on
    /// the Iggy SDK and does not hide it: reach for this when you want Iggy's own
    /// producer options (batching, partitioning, send retries, encryption) instead
    /// of the fluent [`publish`](Self::publish). Call `.build()` then
    /// `.init().await` on the result. The fluent `publish` path keeps its own
    /// cached producer, so a producer you build here is independent.
    pub(crate) fn iggy_producer(
        &self,
        stream: &str,
        topic: &str,
    ) -> Result<IggyProducerBuilder, LaserError> {
        Ok(self.inner.client.producer(stream, topic)?)
    }

    /// A raw Iggy `IggyConsumerBuilder` for a standalone consumer over one
    /// `partition` of `(stream, topic)`. The built `IggyConsumer` implements
    /// `futures::Stream`, so with `futures::StreamExt` you can
    /// `while let Some(msg) = consumer.next().await { .. }`, or drive it with
    /// `consume_messages`. Iggy's full consumer options (polling strategy,
    /// auto-commit, batch length, retries) live on the builder.
    ///
    /// # Replaying history (important)
    ///
    /// The high-level `IggyConsumer` tracks its position **in memory** and polls
    /// forward (`PollingStrategy::next()` from the last consumed offset). By
    /// default it will **not** re-read messages it has already seen, and a fresh
    /// instance resumes from the server-stored offset for its consumer id. To
    /// replay a partition's full history from the beginning (e.g. rebuilding an
    /// agent's conversation/context after a crash), you must BOTH:
    ///
    /// 1. set `.polling_strategy(PollingStrategy::first())` (or `offset(0)`), and
    /// 2. call `.allow_replay()` on the builder **when that consumer id already
    ///    has a stored offset**, which it does in the crash-recovery case.
    ///    Without `allow_replay`, a consumer that has previously committed an
    ///    offset filters out every message at/under that mark and yields
    ///    nothing. A brand-new consumer id with no stored offset replays from
    ///    `first()` regardless. When in doubt set it: it is a no-op for a fresh
    ///    id.
    ///
    /// You usually do **not** need this: the SDK's own history-rebuild paths
    /// ([`ContextAssembler`](crate::context::ContextAssembler),
    /// [`ConversationState`](crate::agent::ConversationState),
    /// [`LogMemory`](crate::memory::LogMemory), [`Cursor`](crate::cursor::Cursor))
    /// replay correctly by reading from offset 0 with the low-level offset poll,
    /// independent of any consumer state, reach for those first. This raw builder
    /// is for bespoke streaming where you opt into the replay semantics yourself.
    pub(crate) fn iggy_consumer(
        &self,
        name: &str,
        stream: &str,
        topic: &str,
        partition: u32,
    ) -> Result<IggyConsumerBuilder, LaserError> {
        Ok(self.inner.client.consumer(name, stream, topic, partition)?)
    }

    /// A raw Iggy `IggyConsumerBuilder` for a consumer-group consumer over
    /// `(stream, topic)`: Iggy load-balances partitions across the group's
    /// members. The built `IggyConsumer` is a `futures::Stream` (async-iterate it
    /// with `StreamExt::next`) and carries the full set of Iggy consumer options.
    /// The agent runtime uses this builder internally. It is exposed here for
    /// generic streaming.
    ///
    /// A consumer group is for **forward, load-balanced** consumption with
    /// committed offsets: on restart it resumes from the committed offset, it is
    /// NOT the tool for replaying a conversation's full history (offsets are
    /// shared across the group and `.allow_replay()` would re-deliver to the
    /// whole group). To rebuild an agent's history after a crash, read the
    /// partition from offset 0 via the SDK's [`ContextAssembler`](crate::context::ContextAssembler)
    /// / [`ConversationState`](crate::agent::ConversationState) (which use the
    /// low-level offset poll), or an individual [`iggy_consumer`](Self::iggy_consumer)
    /// with `.polling_strategy(PollingStrategy::first()).allow_replay()`.
    pub(crate) fn iggy_consumer_group(
        &self,
        group: &str,
        stream: &str,
        topic: &str,
    ) -> Result<IggyConsumerBuilder, LaserError> {
        Ok(self.inner.client.consumer_group(group, stream, topic)?)
    }

    pub(crate) async fn producer_on(
        &self,
        stream: &str,
        topic: &str,
    ) -> Result<Arc<IggyProducer>, LaserError> {
        // DashMap entry holds only a shard lock for the insert/get, and the closure
        // has no awaits. We clone the `Arc<OnceCell>` out and release the lock
        // before awaiting init, so a slow connection init blocks only callers racing
        // for the same (stream, topic), never sends on other topics.
        let cell = self
            .inner
            .producers
            .entry((stream.to_owned(), topic.to_owned()))
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();
        let producer = cell
            .get_or_try_init(|| async {
                let producer = self.inner.client.producer(stream, topic)?.build();
                producer.init().await?;
                Ok::<_, LaserError>(Arc::new(producer))
            })
            .await?;
        Ok(producer.clone())
    }
}

fn reclaim_failed_messages(failed: Arc<Vec<IggyMessage>>) -> Vec<IggyMessage> {
    match Arc::try_unwrap(failed) {
        Ok(messages) => messages,
        // IggyMessage is intentionally not Clone, but its bodies are Bytes. A
        // transport-held Arc must not collapse a transient retry into failure.
        Err(shared) => shared.iter().map(clone_iggy_message).collect(),
    }
}

#[cfg(all(feature = "agent", any(feature = "query", test)))]
fn claim_presence_slot(
    slot: &std::sync::Mutex<Option<crate::types::AgentId>>,
    requested: crate::types::AgentId,
) -> Result<(), LaserError> {
    let mut advertised = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    match advertised.as_ref() {
        Some(current) if current != &requested => Err(LaserError::PresenceConflict {
            advertised: current.to_string(),
            requested: requested.to_string(),
        }),
        Some(_) => Ok(()),
        None => {
            *advertised = Some(requested);
            Ok(())
        }
    }
}

fn clone_iggy_message(message: &IggyMessage) -> IggyMessage {
    IggyMessage {
        header: IggyMessageHeader {
            checksum: message.header.checksum,
            id: message.header.id,
            offset: message.header.offset,
            timestamp: message.header.timestamp,
            origin_timestamp: message.header.origin_timestamp,
            user_headers_length: message.header.user_headers_length,
            payload_length: message.header.payload_length,
            reserved: message.header.reserved,
        },
        payload: message.payload.clone(),
        user_headers: message.user_headers.clone(),
    }
}

pub(crate) fn is_transient_iggy_io_error(error: &IggyError) -> bool {
    match error {
        IggyError::CannotReadFile
        | IggyError::CannotReadPartitions
        | IggyError::PartitionNotFound(..) => true,
        IggyError::ProducerSendFailed { cause, .. } => is_transient_iggy_io_error(cause),
        _ => false,
    }
}

/// Builds a connected [`Laser`]. Three connection shapes are supported:
///
/// - `Laser::builder().connection_string("iggy+tcp://user:pass@host:8090").stream("agents").build().await?`
/// - `Laser::builder().address("127.0.0.1:8090").credentials("user", "pass").stream("agents").build().await?`
/// - `Laser::builder().client(my_iggy_client).stream("agents").build().await?` (bring-your-own client)
#[derive(Default)]
pub struct LaserBuilder {
    connection: ConnectionConfig,
    // Set when a connection setter from one mode (connection string, address +
    // credentials, or a bring-your-own client) overwrites a different mode already
    // configured, so `build` fails loudly instead of silently dropping the first.
    connection_conflict: Option<&'static str>,
    stream: Option<String>,
    ops_stream: Option<String>,
    control_topic: Option<String>,
    dlq_topic: Option<String>,
    changes_topic: Option<String>,
    capabilities: Capabilities,
    #[cfg(feature = "sign")]
    verifier: Option<Arc<crate::sign::KeyRegistry>>,
    #[cfg(feature = "agent")]
    governor: Option<Arc<crate::govern::GovernorState>>,
}

#[derive(Default)]
enum ConnectionConfig {
    #[default]
    Unset,
    ConnectionString(String),
    Tcp {
        address: String,
        username: String,
        password: String,
    },
    Client(IggyClient),
}

impl LaserBuilder {
    /// Connect using an Iggy connection string
    /// (`iggy+tcp://user:pass@host:port`, `iggy+quic://...`, `iggy+http://...`,
    /// `iggy+ws://...`). The most ergonomic option.
    pub fn connection_string(mut self, value: impl Into<String>) -> Self {
        if matches!(
            self.connection,
            ConnectionConfig::Tcp { .. } | ConnectionConfig::Client(_)
        ) {
            self.connection_conflict = Some(
                "connection_string() conflicts with an address/credentials or client already set",
            );
        }
        self.connection = ConnectionConfig::ConnectionString(value.into());
        self
    }

    /// Connect over TCP to `address` (`host:port`). Requires `credentials`.
    pub fn address(mut self, value: impl Into<String>) -> Self {
        if matches!(
            self.connection,
            ConnectionConfig::ConnectionString(_) | ConnectionConfig::Client(_)
        ) {
            self.connection_conflict =
                Some("address() conflicts with a connection_string or client already set");
        }
        match self.connection {
            ConnectionConfig::Tcp {
                username, password, ..
            } => {
                self.connection = ConnectionConfig::Tcp {
                    address: value.into(),
                    username,
                    password,
                };
            }
            _ => {
                self.connection = ConnectionConfig::Tcp {
                    address: value.into(),
                    username: String::new(),
                    password: String::new(),
                };
            }
        }
        self
    }

    /// Username and password for the TCP connection. Pair with `address`.
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        if matches!(
            self.connection,
            ConnectionConfig::ConnectionString(_) | ConnectionConfig::Client(_)
        ) {
            self.connection_conflict =
                Some("credentials() conflicts with a connection_string or client already set");
        }
        match self.connection {
            ConnectionConfig::Tcp { address, .. } => {
                self.connection = ConnectionConfig::Tcp {
                    address,
                    username: username.into(),
                    password: password.into(),
                };
            }
            _ => {
                self.connection = ConnectionConfig::Tcp {
                    address: String::new(),
                    username: username.into(),
                    password: password.into(),
                };
            }
        }
        self
    }

    /// Use a pre-configured `IggyClient`. The builder will not call `connect`
    /// or `login_user`. Do that yourself before passing the client in.
    pub fn client(mut self, client: IggyClient) -> Self {
        if matches!(
            self.connection,
            ConnectionConfig::ConnectionString(_) | ConnectionConfig::Tcp { .. }
        ) {
            self.connection_conflict = Some(
                "client() conflicts with a connection_string or address/credentials already set",
            );
        }
        self.connection = ConnectionConfig::Client(client);
        self
    }

    /// Optional default Iggy stream for the convenience methods (`publish(topic)`,
    /// agentic `bootstrap` / `send_agent`, ...). Omit it for a connection-only
    /// handle that names the stream per operation (`publish_on(stream, topic)`).
    pub fn stream(mut self, value: impl Into<String>) -> Self {
        self.stream = Some(value.into());
        self
    }

    /// Premium capability set, normally negotiated with LaserData Cloud. The
    /// default is [`Capabilities::OPEN`]: everything off, raw Apache Iggy.
    pub fn capabilities(mut self, value: Capabilities) -> Self {
        self.capabilities = value;
        self
    }

    /// Enroll the operator-key verifier the agent registry checks privileged
    /// facts against. With it set, a quarantine or un-quarantine record is folded
    /// only when it carries a signature that verifies against an enrolled key, so
    /// the registry topic's write access control is no longer the sole gate on who
    /// can evict an agent from routing. Omit it to fold on the write-ACL alone.
    #[cfg(feature = "sign")]
    pub fn verifier(mut self, verifier: Arc<crate::sign::KeyRegistry>) -> Self {
        self.verifier = Some(verifier);
        self
    }

    /// Enroll a pre-effect policy hook: `governor` decides before every agent
    /// send, AGDX verb, and memory write this `Laser` performs, applied under
    /// `mode` (see [`GovernorMode`](crate::govern::GovernorMode)). Defense in
    /// depth at the effect boundary, orthogonal to the server-owned capability
    /// layer. Same as [`Laser::with_governor`] after connect.
    #[cfg(feature = "agent")]
    pub fn governor(
        mut self,
        governor: Arc<dyn crate::govern::ActionGovernor>,
        mode: crate::govern::GovernorMode,
    ) -> Self {
        self.governor = Some(Arc::new(crate::govern::GovernorState::new(governor, mode)));
        self
    }

    /// Override the query/control ops stream (default [`OPS_STREAM_DEFAULT`],
    /// `_agdx`). Production keeps the default. Tests isolate it per case.
    pub fn ops_stream(mut self, value: impl Into<String>) -> Self {
        self.ops_stream = Some(value.into());
        self
    }

    /// Override the control-command topic on the ops stream (default
    /// `control.commands`). Production keeps the default.
    pub fn control_topic(mut self, value: impl Into<String>) -> Self {
        self.control_topic = Some(value.into());
        self
    }

    /// Override the dead-letter topic on the ops stream (default `dlq`).
    /// Production keeps the default.
    pub fn dlq_topic(mut self, value: impl Into<String>) -> Self {
        self.dlq_topic = Some(value.into());
        self
    }

    /// Override the change-feed topic on the ops stream (default `changes`).
    /// Production keeps the default.
    pub fn changes_topic(mut self, value: impl Into<String>) -> Self {
        self.changes_topic = Some(value.into());
        self
    }

    /// Connect and return a ready [`Laser`]. The stream is optional: omit it for
    /// a connection-only handle and name the stream per operation.
    // `self` is mutated only to adopt announced topology, which is behind the
    // managed-surface features, so a build with none of them never mutates it.
    #[allow(unused_mut)]
    pub async fn build(mut self) -> Result<Laser, LaserError> {
        if let Some(conflict) = self.connection_conflict {
            return Err(LaserError::Config(conflict));
        }
        let stream = self.stream.filter(|value| !value.is_empty());
        let client = match self.connection {
            ConnectionConfig::Unset => {
                return Err(LaserError::Config(
                    "connection_string, address+credentials, or client is required",
                ));
            }
            ConnectionConfig::ConnectionString(value) => {
                let normalized = normalize_connection_string(&value)?;
                let client = IggyClientBuilder::from_connection_string(&normalized)?.build()?;
                client.connect().await?;
                client
            }
            ConnectionConfig::Tcp {
                address,
                username,
                password,
            } => {
                if address.is_empty() {
                    return Err(LaserError::Config("address is required"));
                }
                if username.is_empty() {
                    return Err(LaserError::Config("credentials are required"));
                }
                // Build through a connection string so the client carries
                // auto-login credentials: iggy-rs re-authenticates on every
                // reconnect, so a dropped connection resumes transparently. A
                // plain `with_tcp` + manual `login_user` reconnects the socket
                // but leaves it unauthenticated after a server restart.
                let with_tls = resolve_tls(format!("iggy+tcp://{username}:{password}@{address}"))?;
                let client = IggyClientBuilder::from_connection_string(&with_tls)?.build()?;
                client.connect().await?;
                client
            }
            ConnectionConfig::Client(client) => client,
        };
        // Probe the server's `AGDX_HELLO` managed command once. LaserData Cloud answers
        // it and the query path moves to `AGDX_QUERY` (off the log), and raw Apache Iggy
        // rejects it, leaving `managed_host` false (query then returns `Unsupported`).
        // Non-fatal: any error just leaves `managed_host` false.
        //
        // A server that answers `AGDX_HELLO` exposes the whole managed bridge, so the
        // probe implies the managed surfaces: a plain `Laser::connect(..)` against the
        // fork can query out of the box without the caller hand-setting capabilities.
        // A surface explicitly set by a BYO client is kept, so the `|=` only ever adds
        // the bridge-implied one.
        #[allow(unused_mut)]
        let mut capabilities = self.capabilities;
        #[cfg(any(
            feature = "fork",
            feature = "graph",
            feature = "kv",
            feature = "projections",
            feature = "query",
            feature = "rbac",
            feature = "runs"
        ))]
        {
            let (managed_host, versions, backends, topology) = probe_managed_host(&client).await;
            capabilities.managed |= managed_host;
            adopt_announced_topology(
                topology,
                &mut self.ops_stream,
                &mut self.control_topic,
                &mut self.dlq_topic,
                &mut self.changes_topic,
            );
            // Advertised wire op versions, when the server's hello reply
            // carried a body (older servers answer with an empty body).
            capabilities.versions = versions;
            // Materialization backends the server exposes, advertised in the
            // same hello reply. Empty against raw Apache Iggy and pre-backends
            // servers, so a caller routes only to an advertised id.
            capabilities.backends = backends;
            // The bridge serves query, the KV store, and forks, so the probe implies
            // all three. An older plane that lacks one answers its ops with
            // `Unsupported`, which the SDK surfaces as `LaserError::Unsupported`.
            capabilities.query.available |= managed_host;
            capabilities.kv.available |= managed_host;
            capabilities.forks |= managed_host;
            // The per-surface sub-features (compare-and-swap, the consistency level)
            // are advertised as bits, NOT implied by the host. The graph surface
            // needs a backend that implements it, advertised by a non-zero graph op
            // version. Agentic memory composes query + graph, so it has no flag.
            if let Some(versions) = capabilities.versions {
                capabilities.merge_features(&versions);
                capabilities.graph |= versions.graph > 0;
            }
        }
        Ok(Laser {
            inner: Arc::new(LaserInner {
                client: Arc::new(client),
                producers: DashMap::new(),
                #[cfg(feature = "agent")]
                registry_caches: DashMap::new(),
                #[cfg(feature = "agent")]
                #[cfg(all(feature = "agent", any(feature = "query", test)))]
                advertised_agent: std::sync::Mutex::new(None),
                #[cfg(feature = "agent")]
                reply_hubs: DashMap::new(),
                #[cfg(feature = "sign")]
                verifier: self.verifier,
            }),
            capabilities,
            ops_stream: self
                .ops_stream
                .unwrap_or_else(|| OPS_STREAM_DEFAULT.to_owned()),
            control_topic: self
                .control_topic
                .unwrap_or_else(|| laser_wire::topics::CONTROL_TOPIC.to_owned()),
            dlq_topic: self
                .dlq_topic
                .unwrap_or_else(|| laser_wire::topics::DLQ_TOPIC.to_owned()),
            changes_topic: self
                .changes_topic
                .unwrap_or_else(|| laser_wire::topics::CHANGES_TOPIC.to_owned()),
            stream,
            #[cfg(feature = "agent")]
            governor: self.governor,
        })
    }
}

// Cheap, non-fatal capability probe: send the server's `AGDX_HELLO` managed command
// over the binary connection. `Ok` means the connected infrastructure is the fork
// and exposes the managed bridge (query/KV/browse/fork off the log). Any error (raw
// Apache Iggy answers `InvalidCommand`) leaves `managed_host` false. A reply body,
// when present, is the CBOR `BackendAnnounce` advertising the wire op versions
// the server accepts plus the materialization backends it exposes. It decodes
// byte-identically from a pre-backends `HelloReply` (the `backends` list is
// skip-when-empty), so an older server's versions-only reply still parses.
// Older servers answer with an empty body, which leaves the versions
// unadvertised (`None`), the backends empty, and the SDK skips fail-fast version
// checks.
#[cfg(any(
    feature = "fork",
    feature = "graph",
    feature = "kv",
    feature = "projections",
    feature = "query",
    feature = "rbac",
    feature = "runs"
))]
async fn probe_managed_host(
    client: &IggyClient,
) -> (
    bool,
    Option<crate::capabilities::OpVersions>,
    Vec<crate::capabilities::BackendDescriptor>,
    Option<laser_wire::topology::WireTopology>,
) {
    let wrapper = client.client();
    let guard = wrapper.read().await;
    let result = match &*guard {
        ClientWrapper::Tcp(client) => {
            client.send_raw_with_response(laser_wire::codes::AGDX_HELLO_CODE, Bytes::new())
        }
        ClientWrapper::Quic(client) => {
            client.send_raw_with_response(laser_wire::codes::AGDX_HELLO_CODE, Bytes::new())
        }
        ClientWrapper::WebSocket(client) => {
            client.send_raw_with_response(laser_wire::codes::AGDX_HELLO_CODE, Bytes::new())
        }
        ClientWrapper::Http(_) | ClientWrapper::Iggy(_) => {
            return (false, None, Vec::new(), None);
        }
    };
    match result.await {
        Ok(reply) => match decode_named::<laser_wire::hello::BackendAnnounce>(&reply) {
            Ok(announce) => (
                true,
                Some(announce.versions),
                announce.backends,
                announce.topology,
            ),
            Err(_) => (true, None, Vec::new(), None),
        },
        Err(_) => (false, None, Vec::new(), None),
    }
}

#[cfg(any(
    feature = "fork",
    feature = "graph",
    feature = "kv",
    feature = "projections",
    feature = "query",
    feature = "rbac",
    feature = "runs"
))]
fn adopt_announced_topology(
    announced: Option<laser_wire::topology::WireTopology>,
    ops_stream: &mut Option<String>,
    control_topic: &mut Option<String>,
    dlq_topic: &mut Option<String>,
    changes_topic: &mut Option<String>,
) {
    let Some(announced) = announced else {
        return;
    };
    ops_stream.get_or_insert(announced.ops_stream);
    control_topic.get_or_insert(announced.control_topic);
    dlq_topic.get_or_insert(announced.dlq_topic);
    changes_topic.get_or_insert(announced.changes_topic);
}

#[cfg(all(
    test,
    any(
        feature = "fork",
        feature = "graph",
        feature = "kv",
        feature = "projections",
        feature = "query",
        feature = "rbac",
        feature = "runs"
    )
))]
mod topology_tests {
    use super::adopt_announced_topology;
    use laser_wire::topology::WireTopology;

    #[test]
    fn given_announced_topology_when_adopted_then_should_fill_unset_names() {
        let announced = WireTopology {
            ops_stream: "ops.custom".to_owned(),
            control_topic: "control.custom".to_owned(),
            dlq_topic: "dlq.custom".to_owned(),
            changes_topic: "changes.custom".to_owned(),
            ..WireTopology::default()
        };
        let mut ops_stream = None;
        let mut control_topic = None;
        let mut dlq_topic = None;
        let mut changes_topic = None;

        adopt_announced_topology(
            Some(announced),
            &mut ops_stream,
            &mut control_topic,
            &mut dlq_topic,
            &mut changes_topic,
        );

        assert_eq!(ops_stream.as_deref(), Some("ops.custom"));
        assert_eq!(control_topic.as_deref(), Some("control.custom"));
        assert_eq!(dlq_topic.as_deref(), Some("dlq.custom"));
        assert_eq!(changes_topic.as_deref(), Some("changes.custom"));
    }

    #[test]
    fn given_explicit_names_when_topology_is_adopted_then_should_keep_overrides() {
        let announced = WireTopology {
            ops_stream: "ops.announced".to_owned(),
            control_topic: "control.announced".to_owned(),
            dlq_topic: "dlq.announced".to_owned(),
            changes_topic: "changes.announced".to_owned(),
            ..WireTopology::default()
        };
        let mut ops_stream = Some("ops.explicit".to_owned());
        let mut control_topic = Some("control.explicit".to_owned());
        let mut dlq_topic = Some("dlq.explicit".to_owned());
        let mut changes_topic = Some("changes.explicit".to_owned());

        adopt_announced_topology(
            Some(announced),
            &mut ops_stream,
            &mut control_topic,
            &mut dlq_topic,
            &mut changes_topic,
        );

        assert_eq!(ops_stream.as_deref(), Some("ops.explicit"));
        assert_eq!(control_topic.as_deref(), Some("control.explicit"));
        assert_eq!(dlq_topic.as_deref(), Some("dlq.explicit"));
        assert_eq!(changes_topic.as_deref(), Some("changes.explicit"));
    }
}

/// Normalize a connection string: if no `iggy` scheme is present (`iggy://` or
/// `iggy+<protocol>://`), prepend `iggy://` so a raw `user:pass@host:port` from
/// e.g. a LaserData Cloud bootstrap endpoint works as-is. Then, for a
/// LaserData Cloud host that does not already name a `tls_ca_file=`, attach
/// `tls=true` plus LaserData's bundled public CA so a bare connection string
/// is enough. `LASER_NO_TLS=1` disables this, and `LASER_TLS_CERT=<path>`
/// overrides the bundled CA with any CA file (the same knob as the connection
/// string's own `tls_ca_file=`).
fn normalize_connection_string(value: &str) -> Result<String, LaserError> {
    let trimmed = value.trim();
    let scheme_applied = if trimmed.starts_with("iggy://") || trimmed.starts_with("iggy+") {
        trimmed.to_owned()
    } else {
        format!("iggy://{trimmed}")
    };
    resolve_tls(scheme_applied)
}

/// True for a LaserData-operated host (`*.laserdata.cloud` or
/// `*.laserdata.com`). The trailing-dot match rejects look-alikes like
/// `laserdata.cloud.attacker.com`.
fn is_laserdata_host(host: &str) -> bool {
    let host = host.to_ascii_lowercase();
    host == "laserdata.cloud"
        || host.ends_with(".laserdata.cloud")
        || host == "laserdata.com"
        || host.ends_with(".laserdata.com")
}

// Strip scheme, userinfo, and port from a connection string's authority.
fn host_of(connection_string: &str) -> &str {
    let after_scheme = connection_string
        .split_once("://")
        .map_or(connection_string, |(_, rest)| rest);
    let authority = after_scheme
        .split(['/', '?'])
        .next()
        .unwrap_or(after_scheme);
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host_and_port)| host_and_port);
    if let Some(bracketed) = authority.strip_prefix('[')
        && let Some(closing) = bracketed.find(']')
    {
        return &bracketed[..closing];
    }
    authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host)
}

fn resolve_tls(connection_string: String) -> Result<String, LaserError> {
    if std::env::var("LASER_NO_TLS").is_ok() || connection_string.contains("tls_ca_file=") {
        return Ok(connection_string);
    }
    if !is_laserdata_host(host_of(&connection_string)) {
        return Ok(connection_string);
    }
    let cert_path = resolve_cert_path()?;
    let mut with_tls = connection_string;
    if !with_tls.contains("tls=") {
        let separator = if with_tls.contains('?') { '&' } else { '?' };
        with_tls = format!("{with_tls}{separator}tls=true");
    }
    Ok(format!("{with_tls}&tls_ca_file={}", cert_path.display()))
}

// LaserData Cloud's public root CA, bundled so `Laser::connect` works against
// a LaserData Cloud host with no extra setup. Public certificate, no secret
// material. `LASER_TLS_CERT=<path>` overrides it with any CA file, and a
// rotated CA is always reachable through that same override.
static PROD_CERT: &[u8] = include_bytes!("../certs/laserdata.crt");

fn resolve_cert_path() -> Result<std::path::PathBuf, LaserError> {
    if let Ok(custom) = std::env::var("LASER_TLS_CERT")
        && !custom.is_empty()
    {
        return Ok(std::path::PathBuf::from(custom));
    }
    let path = std::env::temp_dir().join("laserdata.crt");
    if !path.exists() {
        std::fs::write(&path, PROD_CERT)
            .map_err(|error| LaserError::Invalid(format!("write CA cert: {error}")))?;
    }
    Ok(path)
}

pub(crate) async fn ensure_stream(client: &IggyClient, stream: &str) -> Result<(), LaserError> {
    if client
        .get_stream(&Identifier::named(stream)?)
        .await?
        .is_some()
    {
        return Ok(());
    }
    if let Err(error) = client.create_stream(stream).await
        && client
            .get_stream(&Identifier::named(stream)?)
            .await?
            .is_none()
    {
        return Err(error.into());
    }
    Ok(())
}

pub(crate) async fn ensure_topic(
    client: &IggyClient,
    stream: &str,
    topic: &str,
    partitions: u32,
) -> Result<(), LaserError> {
    ensure_topic_with(client, stream, topic, partitions, IggyExpiry::NeverExpire).await
}

/// Idempotently create `topic` with an explicit message-expiry, so a caller
/// (the memory topic) can bound how long records live.
pub(crate) async fn ensure_topic_with(
    client: &IggyClient,
    stream: &str,
    topic: &str,
    partitions: u32,
    expiry: IggyExpiry,
) -> Result<(), LaserError> {
    let stream_id = Identifier::named(stream)?;
    let topic_id = Identifier::named(topic)?;
    if client.get_topic(&stream_id, &topic_id).await?.is_some() {
        return Ok(());
    }
    let result = client
        .create_topic(
            &stream_id,
            topic,
            partitions,
            CompressionAlgorithm::default(),
            None,
            expiry,
            MaxTopicSize::ServerDefault,
        )
        .await;
    if let Err(error) = result
        && client.get_topic(&stream_id, &topic_id).await?.is_none()
    {
        return Err(error.into());
    }
    Ok(())
}

#[cfg(test)]
mod builder_conflict_tests {
    #[cfg(feature = "agent")]
    use super::claim_presence_slot;
    use super::{Laser, reclaim_failed_messages};
    use crate::error::LaserError;
    use bytes::Bytes;
    use iggy::prelude::IggyMessage;
    use std::sync::Arc;
    #[cfg(feature = "agent")]
    use std::sync::Mutex;

    #[cfg(feature = "agent")]
    #[test]
    fn given_one_connection_when_two_agents_claim_presence_then_should_reject_the_second() {
        let slot = Mutex::new(None);
        claim_presence_slot(&slot, "risk".parse().expect("risk is a valid agent id"))
            .expect("the first agent claims the connection");
        claim_presence_slot(&slot, "risk".parse().expect("risk is a valid agent id"))
            .expect("the same agent may refresh its presence");

        let error = claim_presence_slot(
            &slot,
            "support".parse().expect("support is a valid agent id"),
        )
        .expect_err("a second agent must not overwrite connection presence");

        assert!(matches!(
            error,
            LaserError::PresenceConflict { advertised, requested }
                if advertised == "risk" && requested == "support"
        ));
    }

    #[test]
    fn given_a_shared_failed_batch_when_reclaimed_then_should_preserve_it_for_retry() {
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"retry-body"))
            .build()
            .expect("the retry fixture message builds");
        let failed = Arc::new(vec![message]);
        let held_by_transport = failed.clone();

        let reclaimed = reclaim_failed_messages(failed);

        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].payload, Bytes::from_static(b"retry-body"));
        assert_eq!(held_by_transport.len(), 1);
    }

    #[tokio::test]
    async fn given_two_connection_modes_when_built_then_should_error_before_connecting() {
        // The conflict is caught at the top of `build`, before any IO, so this
        // needs no server: mixing a connection string with address/credentials
        // fails loudly instead of silently dropping the string.
        let result = Laser::builder()
            .connection_string("iggy:iggy@127.0.0.1:8090")
            .address("127.0.0.1:8090")
            .credentials("iggy", "iggy")
            .build()
            .await;
        assert!(matches!(result, Err(LaserError::Config(_))));
    }
}

#[cfg(test)]
mod connection_string_tests {
    use super::{host_of, is_laserdata_host, normalize_connection_string, resolve_tls};

    #[test]
    fn given_a_full_tcp_connection_string_when_normalized_then_should_be_unchanged() {
        assert_eq!(
            normalize_connection_string("iggy+tcp://iggy:iggy@127.0.0.1:8090")
                .expect("a local connection string normalizes"),
            "iggy+tcp://iggy:iggy@127.0.0.1:8090",
        );
    }

    #[test]
    fn given_a_default_scheme_when_normalized_then_should_be_unchanged() {
        assert_eq!(
            normalize_connection_string("iggy://user:password@host:8090")
                .expect("a local connection string normalizes"),
            "iggy://user:password@host:8090",
        );
    }

    #[test]
    fn given_a_bare_endpoint_when_normalized_then_should_prepend_default_scheme() {
        assert_eq!(
            normalize_connection_string("user:password@host:8090")
                .expect("a bare endpoint normalizes"),
            "iggy://user:password@host:8090",
        );
    }

    #[test]
    fn given_whitespace_around_the_value_when_normalized_then_should_be_trimmed() {
        assert_eq!(
            normalize_connection_string("  iggy:iggy@host:8090  ")
                .expect("a padded endpoint normalizes"),
            "iggy://iggy:iggy@host:8090",
        );
    }

    #[test]
    fn given_laserdata_hosts_when_checked_then_should_match_both_domains() {
        assert!(is_laserdata_host("laserdata.cloud"));
        assert!(is_laserdata_host("starter-123.aws.laserdata.cloud"));
        assert!(is_laserdata_host("LASERDATA.CLOUD"));
        assert!(is_laserdata_host("laserdata.com"));
        assert!(is_laserdata_host("api.laserdata.com"));
        assert!(
            !is_laserdata_host("laserdata.cloud.attacker.com"),
            "a look-alike suffix must not match"
        );
        assert!(
            !is_laserdata_host("laserdata.com.attacker.com"),
            "a look-alike suffix must not match"
        );
    }

    #[test]
    fn given_a_connection_string_when_the_host_is_extracted_then_should_strip_scheme_userinfo_and_port()
     {
        assert_eq!(
            host_of("iggy+tcp://user:pwd@starter-123.aws.laserdata.cloud:8090"),
            "starter-123.aws.laserdata.cloud",
        );
        assert_eq!(
            host_of("user:pwd@host.laserdata.cloud:8090"),
            "host.laserdata.cloud"
        );
    }

    #[test]
    fn given_a_laserdata_cloud_host_when_resolving_tls_then_should_attach_tls_and_the_bundled_ca() {
        let resolved = resolve_tls("iggy+tcp://u:p@h.laserdata.cloud:8090".to_owned())
            .expect("tls resolution should succeed");
        assert!(resolved.contains("tls=true"), "{resolved}");
        assert!(resolved.contains("tls_ca_file="), "{resolved}");
    }

    #[test]
    fn given_a_non_laserdata_host_when_resolving_tls_then_should_leave_it_untouched() {
        assert_eq!(
            resolve_tls("iggy+tcp://u:p@127.0.0.1:8090".to_owned())
                .expect("tls resolution should succeed"),
            "iggy+tcp://u:p@127.0.0.1:8090",
        );
    }

    #[test]
    fn given_an_explicit_tls_ca_file_when_resolving_tls_then_should_leave_it_untouched() {
        let connection_string =
            "iggy+tcp://u:p@h.laserdata.cloud:8090?tls_ca_file=/tmp/my-ca.crt".to_owned();
        assert_eq!(
            resolve_tls(connection_string.clone()).expect("tls resolution should succeed"),
            connection_string,
        );
    }
}

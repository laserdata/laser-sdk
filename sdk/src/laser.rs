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
#[cfg(feature = "streaming")]
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
    capability_override: Option<Capabilities>,
    ops_stream_override: Option<String>,
    // The control-command topic on the ops stream. Defaults to
    // `laser_wire::topics::CONTROL_TOPIC` (`control.commands`). Overridable so a
    // deployment that names its ops topics differently still drives projections.
    control_topic_override: Option<String>,
    // The dead-letter topic on the ops stream. Defaults to
    // `laser_wire::topics::DLQ_TOPIC` (`dlq`). Overridable alongside the other
    // ops-stream topic names.
    dlq_topic_override: Option<String>,
    // The change-feed topic on the ops stream. Defaults to
    // `laser_wire::topics::CHANGES_TOPIC` (`changes`). Overridable alongside the
    // other ops-stream topic names.
    changes_topic_override: Option<String>,
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
    negotiated: std::sync::RwLock<NegotiatedState>,
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

struct NegotiatedState {
    configured_capabilities: Capabilities,
    capabilities: Capabilities,
    topology: laser_wire::topology::WireTopology,
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

    /// Connect to Apache Iggy container at `iggy:iggy@127.0.0.1:8090`.
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
                negotiated: std::sync::RwLock::new(NegotiatedState {
                    configured_capabilities: Capabilities::OPEN,
                    capabilities: Capabilities::OPEN,
                    topology: laser_wire::topology::WireTopology::default(),
                }),
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
            capability_override: None,
            ops_stream_override: None,
            control_topic_override: None,
            dlq_topic_override: None,
            changes_topic_override: None,
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
                    #[cfg(feature = "sign")]
                    self.inner.verifier.clone(),
                )
            })
            .await?;
        Ok(hub.clone())
    }

    /// Returns a clone of this `Laser` with the given capability set. The
    /// underlying connection + producer cache are shared with the original.
    #[must_use]
    pub fn with_capabilities(mut self, capabilities: Capabilities) -> Self {
        self.capability_override = Some(capabilities);
        self
    }

    /// Returns a clone of this `Laser` whose query/control surface rides
    /// `ops_stream` instead of the default [`OPS_STREAM_DEFAULT`] (`_agdx`). The
    /// underlying connection and producer cache are shared with the original.
    /// Production keeps the default, since one LaserData Cloud per deployment
    /// owns `_agdx`. Tests override it for per-case isolation.
    #[must_use]
    pub fn with_ops_stream(mut self, ops_stream: impl Into<String>) -> Self {
        self.ops_stream_override = Some(ops_stream.into());
        self
    }

    /// Returns a clone of this `Laser` whose control commands publish to
    /// `control_topic` on the ops stream instead of the default
    /// (`control.commands`). The underlying connection and producer cache are
    /// shared. Production keeps the default, a deployment with its own ops-topic
    /// naming overrides it.
    #[must_use]
    pub fn with_control_topic(mut self, control_topic: impl Into<String>) -> Self {
        self.control_topic_override = Some(control_topic.into());
        self
    }

    /// Returns a clone of this `Laser` whose dead-letter capsules publish to
    /// `dlq_topic` on the ops stream instead of the default (`dlq`). The
    /// underlying connection and producer cache are shared. Production keeps the
    /// default, a deployment with its own ops-topic naming overrides it.
    #[must_use]
    pub fn with_dlq_topic(mut self, dlq_topic: impl Into<String>) -> Self {
        self.dlq_topic_override = Some(dlq_topic.into());
        self
    }

    /// Returns a clone of this `Laser` whose change-feed records publish to
    /// `changes_topic` on the ops stream instead of the default (`changes`). The
    /// underlying connection and producer cache are shared. Production keeps the
    /// default, a deployment with its own ops-topic naming overrides it.
    #[must_use]
    pub fn with_changes_topic(mut self, changes_topic: impl Into<String>) -> Self {
        self.changes_topic_override = Some(changes_topic.into());
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
    pub fn ops_stream(&self) -> String {
        self.ops_stream_override.clone().unwrap_or_else(|| {
            self.inner
                .negotiated
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .topology
                .ops_stream
                .clone()
        })
    }

    /// The control-command topic on the ops stream (default `control.commands`).
    pub fn control_topic(&self) -> String {
        self.control_topic_override.clone().unwrap_or_else(|| {
            self.inner
                .negotiated
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .topology
                .control_topic
                .clone()
        })
    }

    /// The dead-letter topic on the ops stream (default `dlq`).
    pub fn dlq_topic(&self) -> String {
        self.dlq_topic_override.clone().unwrap_or_else(|| {
            self.inner
                .negotiated
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .topology
                .dlq_topic
                .clone()
        })
    }

    /// The change-feed topic on the ops stream (default `changes`).
    pub fn changes_topic(&self) -> String {
        self.changes_topic_override.clone().unwrap_or_else(|| {
            self.inner
                .negotiated
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .topology
                .changes_topic
                .clone()
        })
    }

    /// The capability set this laser was built with (default
    /// [`Capabilities::OPEN`]). Async to reserve a future capability negotiation
    /// round-trip. Open features work regardless of the result.
    pub async fn capabilities(&self) -> Capabilities {
        self.current_capabilities()
    }

    pub(crate) fn current_capabilities(&self) -> Capabilities {
        self.capability_override.clone().unwrap_or_else(|| {
            self.inner
                .negotiated
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .capabilities
                .clone()
        })
    }

    /// Re-probe managed readiness, operation versions, backends, and topology.
    /// Explicit capability and topology overrides remain authoritative.
    pub async fn refresh_capabilities(&self) -> Capabilities {
        #[allow(unused_mut)]
        let mut capabilities = self
            .inner
            .negotiated
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .configured_capabilities
            .clone();
        #[allow(unused_mut)]
        let mut topology = None;
        #[cfg(any(
            feature = "fork",
            feature = "graph",
            feature = "kv",
            feature = "projections",
            feature = "query",
            feature = "rbac",
            feature = "runs"
        ))]
        if let Some(announce) = probe_managed_host(&self.inner.client).await {
            merge_announcement(&mut capabilities, &announce);
            topology = announce.topology;
        }
        let mut negotiated = self
            .inner
            .negotiated
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        negotiated.capabilities = capabilities;
        if let Some(topology) = topology {
            negotiated.topology = topology;
        }
        self.capability_override
            .clone()
            .unwrap_or_else(|| negotiated.capabilities.clone())
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
    /// `AGDX_HELLO`. Managed authorization mutations use their dedicated
    /// replicated operations under VSR. Every other AGDX code is explicitly
    /// non-replicated.
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
        let payload = if laser_wire::codes::is_idempotent_managed_request(code) {
            laser_wire::framing::encode_named(&laser_wire::mutation::ManagedRequestEnvelope {
                v: laser_wire::mutation::MANAGED_REQUEST_VERSION,
                operation_id: u128::from(ulid::Ulid::generate()),
                payload,
            })
            .map_err(|_| IggyError::InvalidFormat)?
        } else {
            payload
        };
        let reply = self
            .inner
            .client
            .send_binary_request(code, bytes::Bytes::from(payload))
            .await?;
        Ok(reply.to_vec())
    }

    /// A Iggy `IggyProducerBuilder` for `(stream, topic)`. laser-sdk builds on
    /// Iggy SDK and does not hide it: reach for this when you want Iggy's own
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

    /// A Iggy `IggyConsumerBuilder` for a standalone consumer over one
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

    /// A Iggy `IggyConsumerBuilder` for a consumer-group consumer over
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
                for attempt in 0..TRANSIENT_SEND_ATTEMPTS {
                    match producer.init().await {
                        Ok(()) => return Ok::<_, LaserError>(Arc::new(producer)),
                        Err(error)
                            if is_idempotent_create_race(&error)
                                && attempt + 1 < TRANSIENT_SEND_ATTEMPTS =>
                        {
                            sleep(Duration::from_millis(50 * (attempt + 1) as u64)).await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                unreachable!("producer init retry returns success or the last error")
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

fn is_idempotent_create_race(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::StreamNameAlreadyExists(_) | IggyError::TopicNameAlreadyExists(_, _)
    )
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
    capabilities: Option<Capabilities>,
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
    /// default is [`Capabilities::OPEN`]: everything off, Apache Iggy.
    pub fn capabilities(mut self, value: Capabilities) -> Self {
        self.capabilities = Some(value);
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

    /// Enroll a pre-effect policy hook with explicit process-local evidence
    /// chain retention. A restart or retention eviction begins a new local
    /// chain for the affected conversation.
    #[cfg(feature = "agent")]
    pub fn governor_with_retention(
        mut self,
        governor: Arc<dyn crate::govern::ActionGovernor>,
        mode: crate::govern::GovernorMode,
        retention: crate::govern::GovernorRetention,
    ) -> Self {
        self.governor = Some(Arc::new(crate::govern::GovernorState::with_retention(
            governor, mode, retention,
        )));
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
        // Probe the server's `AGDX_HELLO` managed command once. A ready managed
        // backend lights up only the surfaces and feature bits it announces.
        // Apache Iggy rejects the probe, and a configured backend that has not
        // answered returns an unavailable announcement, so both remain fail-closed.
        // Non-fatal: any error leaves the negotiated set open-only.
        //
        // A surface explicitly set by a BYO client is kept, so a ready
        // announcement only adds what the deployment serves.
        // The caller's set is the starting point, never a ceiling: the probe's
        // surfaces are added on top, so a builder that passes `Capabilities::OPEN`
        // still lights up the managed surfaces the connected deployment serves.
        #[allow(unused_mut)]
        let configured_capabilities = self.capabilities.clone().unwrap_or(Capabilities::OPEN);
        let mut capabilities = configured_capabilities.clone();
        let mut topology = laser_wire::topology::WireTopology::default();
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
            if let Some(announce) = probe_managed_host(&client).await {
                if let Some(announced) = announce.topology.clone() {
                    topology = announced;
                }
                merge_announcement(&mut capabilities, &announce);
            }
        }
        Ok(Laser {
            inner: Arc::new(LaserInner {
                client: Arc::new(client),
                producers: DashMap::new(),
                negotiated: std::sync::RwLock::new(NegotiatedState {
                    configured_capabilities,
                    capabilities,
                    topology,
                }),
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
            // The builder's set seeded the negotiated state above, so it is not an
            // override: a later refresh keeps adding the deployment's surfaces.
            capability_override: None,
            ops_stream_override: self.ops_stream,
            control_topic_override: self.control_topic,
            dlq_topic_override: self.dlq_topic,
            changes_topic_override: self.changes_topic,
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
async fn probe_managed_host(client: &IggyClient) -> Option<laser_wire::hello::BackendAnnounce> {
    match client
        .send_binary_request(laser_wire::codes::AGDX_HELLO_CODE, Bytes::new())
        .await
    {
        Ok(reply) if !reply.is_empty() => {
            decode_named::<laser_wire::hello::BackendAnnounce>(&reply).ok()
        }
        Ok(_) | Err(_) => None,
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
fn merge_announcement(
    capabilities: &mut Capabilities,
    announce: &laser_wire::hello::BackendAnnounce,
) {
    let versions = announce.versions;
    capabilities.versions = Some(versions);
    capabilities.authz |= versions.has_feature(laser_wire::hello::feature::AUTHZ);
    if announce.ready {
        capabilities.managed = true;
        capabilities.query.available |= versions.query > 0;
        capabilities.kv.available |= versions.kv > 0;
        capabilities.forks |= versions.fork > 0;
        capabilities.graph |= versions.graph > 0;
        capabilities.backends.clone_from(&announce.backends);
        capabilities.merge_features(&versions);
    }
}

#[cfg(test)]
mod announcement_tests {
    use super::*;
    use laser_wire::hello::{BackendAnnounce, BackendDescriptor, OpVersions, feature};

    #[test]
    fn given_an_unavailable_backend_when_merged_then_should_not_enable_plane_surfaces() {
        let announce = BackendAnnounce::new(
            OpVersions::new(1, 1, 1, 1)
                .with_agent(1)
                .with_graph(1)
                .with_features(
                    feature::KV_CAS
                        | feature::KV_CAS_FENCED
                        | feature::STRONG_CONSISTENCY
                        | feature::AGENT_WORKFLOW
                        | feature::KEYWORD_SEARCH
                        | feature::WATCH
                        | feature::AUTHZ,
                ),
        )
        .with_backends(vec![BackendDescriptor::new("stale", "embedded")])
        .unavailable();
        let mut capabilities = Capabilities::OPEN;

        merge_announcement(&mut capabilities, &announce);

        assert!(!capabilities.managed);
        assert!(!capabilities.query.available);
        assert!(!capabilities.query.keyword);
        assert!(!capabilities.kv.available);
        assert!(!capabilities.kv.cas);
        assert!(!capabilities.kv.cas_fenced);
        assert!(!capabilities.graph);
        assert!(!capabilities.forks);
        assert!(!capabilities.agent_workflow);
        assert!(!capabilities.watch);
        assert!(capabilities.authz, "server-native authz remains available");
        assert!(capabilities.backends.is_empty());
        assert_eq!(capabilities.versions, Some(announce.versions));
    }

    #[test]
    fn given_a_ready_backend_when_merged_then_should_enable_advertised_plane_surfaces() {
        let announce = BackendAnnounce::new(
            OpVersions::new(1, 1, 1, 1)
                .with_graph(1)
                .with_features(feature::KV_CAS | feature::AGENT_WORKFLOW),
        )
        .with_backends(vec![BackendDescriptor::new("embedded", "embedded")]);
        let mut capabilities = Capabilities::OPEN;

        merge_announcement(&mut capabilities, &announce);

        assert!(capabilities.managed);
        assert!(capabilities.query.available);
        assert!(capabilities.kv.available);
        assert!(capabilities.kv.cas);
        assert!(capabilities.graph);
        assert!(capabilities.forks);
        assert!(capabilities.agent_workflow);
        assert_eq!(capabilities.backends, announce.backends);
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

// Everything after the scheme, where the authority begins.
fn after_scheme(connection_string: &str) -> &str {
    connection_string
        .split_once("://")
        .map_or(connection_string, |(_, rest)| rest)
}

// Byte offset where the authority begins: everything before it is userinfo.
// The userinfo terminator is located before the `/` and `?` delimiters are
// applied, because a generated password routinely contains `/` and splitting
// first would truncate the authority and hide the real host. The search is
// bounded to the pre-query region so an `@` inside a query value cannot be
// mistaken for the terminator. A literal `?` in a password must be
// percent-encoded.
fn authority_start(after_scheme: &str) -> usize {
    let before_query = after_scheme
        .find('?')
        .map_or(after_scheme, |query| &after_scheme[..query]);
    before_query.rfind('@').map_or(0, |at| at + 1)
}

// Strip scheme, userinfo, and port from a connection string's authority.
fn host_of(connection_string: &str) -> &str {
    let after_scheme = after_scheme(connection_string);
    let host_and_port = &after_scheme[authority_start(after_scheme)..];
    let authority = host_and_port
        .split(['/', '?', '#'])
        .next()
        .unwrap_or(host_and_port);
    if let Some(bracketed) = authority.strip_prefix('[')
        && let Some(closing) = bracketed.find(']')
    {
        return &bracketed[..closing];
    }
    authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host)
}

// The query string, located after the authority so a `/` inside userinfo does
// not shift the split. `None` when the connection string carries no `?`.
fn query_of(connection_string: &str) -> Option<&str> {
    let after_scheme = after_scheme(connection_string);
    let authority = &after_scheme[authority_start(after_scheme)..];
    authority.split_once('?').map(|(_, query)| query)
}

// True when the connection string already carries this query parameter. Matched
// key by key rather than by substring, so credential content cannot suppress
// TLS by containing a parameter name.
fn has_query_param(connection_string: &str, key: &str) -> bool {
    query_of(connection_string).is_some_and(|query| {
        query.split('&').any(|pair| {
            let name = pair.split_once('=').map_or(pair, |(name, _)| name);
            name.eq_ignore_ascii_case(key)
        })
    })
}

// An opt-out flag is read by value. Bare presence is not enough:
// `LASER_NO_TLS=0` and `LASER_NO_TLS=false` must not disable TLS.
fn flag_value_enabled(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| flag_value_enabled(&value))
}

fn resolve_tls(connection_string: String) -> Result<String, LaserError> {
    if env_flag_enabled("LASER_NO_TLS") || has_query_param(&connection_string, "tls_ca_file") {
        return Ok(connection_string);
    }
    if !is_laserdata_host(host_of(&connection_string)) {
        return Ok(connection_string);
    }
    let cert_path = resolve_cert_path()?;
    let mut with_tls = connection_string;
    if !has_query_param(&with_tls, "tls") {
        let separator = if query_of(&with_tls).is_some() {
            '&'
        } else {
            '?'
        };
        with_tls = format!("{with_tls}{separator}tls=true");
    }
    Ok(format!("{with_tls}&tls_ca_file={}", cert_path.display()))
}

// LaserData Cloud's public root CA, bundled so `Laser::connect` works against
// a LaserData Cloud host with no extra setup. Public certificate, no secret
// material. `LASER_TLS_CERT=<path>` overrides it with any CA file, and a
// rotated CA is always reachable through that same override.
static PROD_CERT: &[u8] = include_bytes!("../certs/laserdata.crt");

// A private, user-owned directory to cache the bundled CA in. A world-writable
// shared directory is never used with a fixed name: another local user could
// pre-create the file and become the trust anchor for every Cloud connection.
fn cert_cache_dir() -> Result<std::path::PathBuf, LaserError> {
    #[cfg(windows)]
    let base = std::env::var_os("LOCALAPPDATA").map(std::path::PathBuf::from);
    #[cfg(target_os = "macos")]
    let base =
        std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join("Library/Caches"));
    #[cfg(all(unix, not(target_os = "macos")))]
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".cache"))
        });
    let base = base.ok_or(LaserError::Config(
        "no per-user cache directory to store the LaserData CA in: set LASER_TLS_CERT to a CA file path",
    ))?;
    Ok(base.join("laser-sdk"))
}

// Restrict a directory to its owner. A cache directory that cannot be made
// private is refused rather than used, so the CA is never read from a path
// another local user can write.
#[cfg(unix)]
fn restrict_to_owner(dir: &std::path::Path) -> Result<(), LaserError> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
        .map_err(|error| LaserError::Invalid(format!("restrict CA cache directory: {error}")))
}

#[cfg(not(unix))]
fn restrict_to_owner(_dir: &std::path::Path) -> Result<(), LaserError> {
    Ok(())
}

// Write the bundled CA to a fresh owner-only file, then rename it over the
// target. A reader never observes a partial certificate, and a pre-planted file
// or symlink at the target is replaced rather than followed.
fn write_cert(dir: &std::path::Path, path: &std::path::Path) -> Result<(), LaserError> {
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    // The scratch name is unique per process and per attempt, so concurrent
    // installs never collide on it.
    static ATTEMPT: AtomicU64 = AtomicU64::new(0);
    let attempt = ATTEMPT.fetch_add(1, Ordering::Relaxed);
    let temp = dir.join(format!(
        "laserdata.crt.{}.{attempt}.tmp",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&temp);
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&temp)
        .map_err(|error| LaserError::Invalid(format!("create CA cert: {error}")))?;
    file.write_all(PROD_CERT)
        .and_then(|()| file.sync_all())
        .map_err(|error| LaserError::Invalid(format!("write CA cert: {error}")))?;
    drop(file);
    std::fs::rename(&temp, path)
        .map_err(|error| LaserError::Invalid(format!("install CA cert: {error}")))
}

// Cache the bundled CA in `dir` and return its path. The cached file is reused
// only when its bytes are exactly the bundled CA, so a rotated certificate is
// rewritten instead of going stale and a planted trust anchor is replaced
// instead of being trusted.
fn install_cert(dir: &std::path::Path) -> Result<std::path::PathBuf, LaserError> {
    std::fs::create_dir_all(dir)
        .map_err(|error| LaserError::Invalid(format!("create CA cache directory: {error}")))?;
    restrict_to_owner(dir)?;
    let path = dir.join("laserdata.crt");
    if std::fs::read(&path).is_ok_and(|cached| cached == PROD_CERT) {
        return Ok(path);
    }
    write_cert(dir, &path)?;
    Ok(path)
}

fn resolve_cert_path() -> Result<std::path::PathBuf, LaserError> {
    if let Ok(custom) = std::env::var("LASER_TLS_CERT")
        && !custom.is_empty()
    {
        return Ok(std::path::PathBuf::from(custom));
    }
    install_cert(&cert_cache_dir()?)
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
    use super::{
        PROD_CERT, flag_value_enabled, has_query_param, host_of, install_cert, is_laserdata_host,
        normalize_connection_string, resolve_tls,
    };

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

    #[test]
    fn given_a_password_containing_a_slash_when_the_host_is_extracted_then_should_find_the_real_host()
     {
        assert_eq!(
            host_of("iggy+tcp://user:pa/ss@host.laserdata.cloud:8090"),
            "host.laserdata.cloud",
        );
        assert_eq!(
            host_of("iggy+tcp://user:a/b/c@host.laserdata.cloud"),
            "host.laserdata.cloud",
        );
    }

    #[test]
    fn given_a_password_containing_a_slash_when_resolving_tls_then_should_still_attach_tls() {
        let resolved = resolve_tls("iggy+tcp://user:pa/ss@h.laserdata.cloud:8090".to_owned())
            .expect("tls resolution should succeed");
        assert!(
            resolved.contains("tls=true"),
            "a slash in the password must not silently disable TLS: {resolved}"
        );
    }

    #[test]
    fn given_an_at_sign_inside_the_query_when_the_host_is_extracted_then_should_ignore_it() {
        assert_eq!(
            host_of("iggy+tcp://u:p@h.laserdata.cloud:8090?tls_ca_file=/x@y.crt"),
            "h.laserdata.cloud",
        );
        assert_eq!(
            host_of("iggy+tcp://h.laserdata.cloud:8090?note=a@b"),
            "h.laserdata.cloud",
        );
    }

    #[test]
    fn given_a_bracketed_ipv6_authority_when_the_host_is_extracted_then_should_drop_the_brackets() {
        assert_eq!(host_of("iggy+tcp://u:p@[::1]:8090"), "::1");
    }

    #[test]
    fn given_a_parameter_name_inside_the_password_when_checked_then_should_not_count_as_a_query_param()
     {
        assert!(!has_query_param(
            "iggy+tcp://u:tls_ca_file=x@h.laserdata.cloud:8090",
            "tls_ca_file"
        ));
        assert!(has_query_param(
            "iggy+tcp://u:p@h.laserdata.cloud:8090?tls_ca_file=/ca.crt",
            "tls_ca_file"
        ));
        assert!(has_query_param(
            "iggy+tcp://u:p@h.laserdata.cloud:8090?tls=true&tls_ca_file=/ca.crt",
            "tls"
        ));
    }

    #[test]
    fn given_a_password_containing_a_parameter_name_when_resolving_tls_then_should_still_attach_the_ca()
     {
        let resolved = resolve_tls("iggy+tcp://u:tls_ca_file=x@h.laserdata.cloud:8090".to_owned())
            .expect("tls resolution should succeed");
        assert!(
            resolved.contains("tls=true"),
            "credential content must not suppress TLS: {resolved}"
        );
        assert!(resolved.ends_with(".crt"), "{resolved}");
    }

    #[test]
    fn given_an_opt_out_flag_value_when_read_then_should_only_accept_an_affirmative() {
        assert!(flag_value_enabled("1"));
        assert!(flag_value_enabled("true"));
        assert!(flag_value_enabled("TRUE"));
        assert!(flag_value_enabled(" yes "));
        assert!(flag_value_enabled("on"));
        assert!(!flag_value_enabled("0"), "`0` must not disable TLS");
        assert!(!flag_value_enabled("false"), "`false` must not disable TLS");
        assert!(
            !flag_value_enabled(""),
            "an empty value must not disable TLS"
        );
        assert!(!flag_value_enabled("no"));
    }

    #[test]
    fn given_no_cached_certificate_when_installed_then_should_write_the_bundled_ca() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = install_cert(dir.path()).expect("the certificate installs");
        assert_eq!(
            std::fs::read(&path).expect("the installed certificate reads"),
            PROD_CERT,
        );
    }

    #[test]
    fn given_a_tampered_cached_certificate_when_installed_then_should_replace_it() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let planted = dir.path().join("laserdata.crt");
        std::fs::write(&planted, b"-----BEGIN CERTIFICATE-----\nattacker\n")
            .expect("the planted certificate writes");
        let path = install_cert(dir.path()).expect("the certificate installs");
        assert_eq!(path, planted);
        assert_eq!(
            std::fs::read(&path).expect("the installed certificate reads"),
            PROD_CERT,
            "a pre-planted trust anchor must be replaced, never trusted"
        );
    }

    #[test]
    fn given_an_already_current_certificate_when_installed_then_should_reuse_it() {
        let dir = tempfile::tempdir().expect("a temp dir");
        let first = install_cert(dir.path()).expect("the certificate installs");
        let second = install_cert(dir.path()).expect("the certificate installs again");
        assert_eq!(first, second);
        assert_eq!(
            std::fs::read(&second).expect("the installed certificate reads"),
            PROD_CERT,
        );
    }

    #[cfg(unix)]
    #[test]
    fn given_an_installed_certificate_when_inspected_then_should_be_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("a temp dir");
        let path = install_cert(dir.path()).expect("the certificate installs");
        let file_mode = std::fs::metadata(&path)
            .expect("the certificate has metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600, "the cached CA must be owner-only");
        let dir_mode = std::fs::metadata(dir.path())
            .expect("the cache directory has metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            dir_mode, 0o700,
            "the cache directory must not be group or world writable"
        );
    }
}

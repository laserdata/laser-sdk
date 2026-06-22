use crate::capabilities::Capabilities;
use crate::error::LaserError;
#[cfg(feature = "query")]
use crate::query::AGDX_HELLO_CODE;
use bytes::Bytes;
use dashmap::DashMap;
#[cfg(feature = "query")]
use iggy::binary::BinaryTransport;
#[cfg(feature = "query")]
use iggy::prelude::locking::IggyRwLockFn;
use iggy::prelude::*;
#[cfg(feature = "query")]
use laser_wire::framing::decode_named;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::OnceCell;

// The generic correlation header and the header caps moved to laser-wire (they
// are wire contract). Re-exported here so the historical paths keep resolving.
pub use laser_wire::headers::{
    CORRELATION_ID, HEADER_FRAMING_BYTES, HEADER_SOFT_CAP, HEADER_VALUE_MAX,
};

// Default ops stream for the control surface (`control.commands`, `dlq`). One
// LaserData Cloud per deployment owns it, so in production it is fixed.
// Overridable via `LaserBuilder::ops_stream` / `Laser::with_ops_stream` - tests
// isolate it per case the same way they isolate the data stream. Mirrors
// `query::OPS_STREAM`.
/// Default ops stream name (`_agdx`).
pub const OPS_STREAM_DEFAULT: &str = "_agdx";

// Producers are cached per (stream, topic): the ops query path publishes to the
// `_agdx` stream while data rides the customer stream, so the cache key must carry
// the stream too or the two would collide on a shared topic name.
type ProducerKey = (String, String);
type ProducerCell = Arc<OnceCell<Arc<IggyProducer>>>;

/// The Laser client. Cheap to `clone`, since the connection and producer cache
/// are shared via an internal `Arc`, so one connection is reused across tasks.
/// Build it through [`Laser::connect`] or [`Laser::builder`]. Never wrap it in
/// your own `Arc`.
#[derive(Clone)]
pub struct Laser {
    inner: Arc<LaserInner>,
    capabilities: Capabilities,
    ops_stream: String,
    // The control-command topic on the ops stream. Defaults to
    // `laser_wire::topics::CONTROL_TOPIC` (`control.commands`). Overridable so a
    // deployment that names its ops topics differently still drives projections.
    control_topic: String,
    // Optional default data stream. Set via `connect_with_stream` / the builder /
    // `with_stream`, it lets the convenience methods (`publish`, `bootstrap`, ...)
    // take just a topic. It lives on `Laser` (not the shared `inner`) so
    // `with_stream` re-scopes cheaply, sharing the one connection across any
    // number of streams. The explicit `*_on(stream, ...)` methods ignore it.
    stream: Option<String>,
}

struct LaserInner {
    client: IggyClient,
    producers: DashMap<ProducerKey, ProducerCell>,
}

impl Laser {
    /// Connect using an Iggy connection string. The connection string is the
    /// only thing required. The SDK targets Apache Iggy over TCP by default and
    /// auto-attaches TLS against LaserData Cloud. The scheme is optional
    /// (`Laser::connect` prepends `iggy://` if missing), so all of these work:
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # async fn run() -> Result<(), LaserError> {
    /// // full TCP form
    /// Laser::connect("iggy+tcp://iggy:iggy@127.0.0.1:8090").await?;
    /// // default TCP (iggy:// = TCP)
    /// Laser::connect("iggy://iggy:iggy@127.0.0.1:8090").await?;
    /// // scheme omitted, paste a raw bootstrap endpoint from LaserData Cloud
    /// Laser::connect("iggy:iggy@127.0.0.1:8090").await?;
    /// # Ok(()) }
    /// ```
    ///
    /// The returned handle has no default stream, so operations name the stream
    /// explicitly: `laser.publish_on(stream, topic)`, `laser.ensure_topic_on(..)`,
    /// `laser.reader_on(stream, topic)`. One connection drives any number of Iggy
    /// streams. To set a default stream so the shorter `publish(topic)` / agentic
    /// methods work, use [`connect_with_stream`](Self::connect_with_stream) or
    /// [`with_stream`](Self::with_stream).
    pub async fn connect(connection_string: &str) -> Result<Self, LaserError> {
        LaserBuilder::default()
            .connection_string(connection_string)
            .build()
            .await
    }

    /// Connect and pin a default Iggy `stream`, so the convenience methods
    /// (`publish(topic)`, `ensure_topic(topic, n)`, `reader(topic)`, and the
    /// agentic `bootstrap` / `send_agent` / `request`) take just a topic. You can
    /// still target any other stream with the `*_on(stream, ..)` methods or
    /// [`with_stream`](Self::with_stream). The default is purely ergonomic.
    ///
    /// ```no_run
    /// # use laser_sdk::prelude::*;
    /// # async fn run() -> Result<(), LaserError> {
    /// let laser = Laser::connect_with_stream("iggy:iggy@127.0.0.1:8090", "agent-telemetry").await?;
    /// // publishes to the "agent-telemetry" stream
    /// # #[cfg(feature = "query")]
    /// laser.publish("inferences").payload(b"...".to_vec()).send().await?;
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
    /// [`Laser::connect`] or [`Laser::builder`]. Chain [`with_stream`](Self::with_stream)
    /// to pin a default stream.
    pub fn from_client(client: IggyClient) -> Self {
        Self {
            inner: Arc::new(LaserInner {
                client,
                producers: DashMap::new(),
            }),
            capabilities: Capabilities::default(),
            ops_stream: OPS_STREAM_DEFAULT.to_owned(),
            control_topic: laser_wire::topics::CONTROL_TOPIC.to_owned(),
            stream: None,
        }
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

    /// A clone of this `Laser` pinned to a default data `stream`, sharing the one
    /// connection + producer cache. This is how one connection serves any number
    /// of Iggy streams: `laser.with_stream("orders").publish("events")`,
    /// `laser.with_stream("telemetry")...`. Takes `&self`, so you can re-scope the
    /// same long-lived connection to as many streams as you like. The convenience
    /// methods (`publish`, `ensure_topic`, `reader`, and the agentic `bootstrap` /
    /// `send_agent` / `request`) then operate on this stream.
    #[must_use]
    pub fn with_stream(&self, stream: impl Into<String>) -> Self {
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
    /// [`with_stream`](Self::with_stream), or the builder). `None` for a
    /// connection-only handle that names the stream per operation.
    pub fn stream(&self) -> Option<&str> {
        self.stream.as_deref().filter(|value| !value.is_empty())
    }

    // The default stream, or `NoStream` if none is set. Used by the convenience
    // methods that take just a topic.
    pub(crate) fn stream_required(&self) -> Result<&str, LaserError> {
        self.stream().ok_or(LaserError::NoStream)
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

    /// The capability set this laser was built with (default
    /// [`Capabilities::OPEN`]). Async to reserve a future capability negotiation
    /// round-trip. Open features work regardless of the result.
    pub async fn capabilities(&self) -> Capabilities {
        self.capabilities.clone()
    }

    /// Idempotently creates `topic` on the default stream with `partitions`,
    /// creating the stream first if needed. Requires a default stream (see
    /// [`with_stream`](Self::with_stream)). Use
    /// [`ensure_topic_on`](Self::ensure_topic_on) to target an explicit stream.
    pub async fn ensure_topic(&self, topic: &str, partitions: u32) -> Result<(), LaserError> {
        self.ensure_topic_on(self.stream_required()?, topic, partitions)
            .await
    }

    /// Idempotently creates `topic` on `stream` with `partitions`, creating the
    /// stream first if needed. Used for the `_agdx` ops stream, which is separate
    /// from this laser's data stream.
    pub async fn ensure_topic_on(
        &self,
        stream: &str,
        topic: &str,
        partitions: u32,
    ) -> Result<(), LaserError> {
        ensure_stream(&self.inner.client, stream).await?;
        ensure_topic(&self.inner.client, stream, topic, partitions).await
    }

    /// Low-level send: one message with explicit user-headers, on the default
    /// stream. Keyed partitioning preserves per-key ordering, and `None` lets
    /// the producer balance across partitions. Most callers should use `publish`
    /// or `send_agent`. Requires a default stream. Use
    /// [`send_with_headers_on`](Self::send_with_headers_on) to target an explicit one.
    pub async fn send_with_headers(
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
    pub async fn send_with_headers_on(
        &self,
        stream: &str,
        topic: &str,
        payload: impl Into<Vec<u8>>,
        headers: BTreeMap<HeaderKey, HeaderValue>,
        partition_key: Option<&str>,
    ) -> Result<(), LaserError> {
        let message = IggyMessage::builder()
            .payload(Bytes::from(payload.into()))
            .user_headers(headers)
            .build()?;
        self.send_batch_on(stream, topic, vec![message], partition_key)
            .await
    }

    /// Low-level batch send: one Iggy `send_messages` call covering many
    /// pre-built `IggyMessage`s. All messages in the batch share the same
    /// partitioning. Without a `partition_key`, Iggy chooses one partition for the
    /// whole call using its balanced partitioner. An empty batch is a cheap no-op.
    pub async fn send_batch(
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
    pub async fn send_batch_on(
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
        self.producer_on(stream, topic)
            .await?
            .send_with_partitioning(messages, Some(partitioning))
            .await?;
        Ok(())
    }

    /// Send a managed command `code` with `payload` over the existing binary
    /// connection and return the raw reply bytes. The query path uses it for
    /// `AGDX_QUERY` on the server, and the connect-time probe uses it for
    /// `AGDX_HELLO`. `IggyClient` does not surface `BinaryTransport`, so we
    /// dispatch on the underlying transport variant. Each binary transport
    /// implements it. The HTTP and wrapping `Iggy` variants do not, so those
    /// report `InvalidCommand`.
    #[cfg(feature = "query")]
    pub(crate) async fn send_raw_with_response(
        &self,
        code: u32,
        payload: Bytes,
    ) -> Result<Bytes, IggyError> {
        let wrapper = self.inner.client.client();
        let guard = wrapper.read().await;
        match &*guard {
            ClientWrapper::Tcp(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::Quic(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::WebSocket(client) => client.send_raw_with_response(code, payload).await,
            ClientWrapper::Http(_) | ClientWrapper::Iggy(_) => Err(IggyError::InvalidCommand),
        }
    }

    /// A raw Iggy `IggyProducerBuilder` for `(stream, topic)`. laser-sdk builds on
    /// the Iggy SDK and does not hide it: reach for this when you want Iggy's own
    /// producer options (batching, partitioning, send retries, encryption) instead
    /// of the fluent [`publish`](Self::publish). Call `.build()` then
    /// `.init().await` on the result. The fluent `publish` path keeps its own
    /// cached producer, so a producer you build here is independent.
    pub fn iggy_producer(
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
    pub fn iggy_consumer(
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
    pub fn iggy_consumer_group(
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

/// Builds a connected [`Laser`]. Three connection shapes are supported:
///
/// - `Laser::builder().connection_string("iggy+tcp://user:pass@host:8090").stream("agents").build().await?`
/// - `Laser::builder().address("127.0.0.1:8090").credentials("user", "pass").stream("agents").build().await?`
/// - `Laser::builder().client(my_iggy_client).stream("agents").build().await?` (bring-your-own client)
#[derive(Default)]
pub struct LaserBuilder {
    connection: ConnectionConfig,
    stream: Option<String>,
    ops_stream: Option<String>,
    control_topic: Option<String>,
    capabilities: Capabilities,
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
        self.connection = ConnectionConfig::ConnectionString(value.into());
        self
    }

    /// Connect over TCP to `address` (`host:port`). Requires `credentials`.
    pub fn address(mut self, value: impl Into<String>) -> Self {
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

    /// Connect and return a ready [`Laser`]. The stream is optional: omit it for
    /// a connection-only handle and name the stream per operation.
    pub async fn build(self) -> Result<Laser, LaserError> {
        let stream = self.stream.filter(|value| !value.is_empty());
        let client = match self.connection {
            ConnectionConfig::Unset => {
                return Err(LaserError::Config(
                    "connection_string, address+credentials, or client is required",
                ));
            }
            ConnectionConfig::ConnectionString(value) => {
                let normalized = normalize_connection_string(&value);
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
                let client = IggyClient::builder()
                    .with_tcp()
                    .with_server_address(address)
                    .build()?;
                client.connect().await?;
                client.login_user(&username, &password).await?;
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
        // probe also implies `managed_query`: a plain `Laser::connect(..)` against
        // the fork can query out of the box without the caller hand-setting
        // capabilities. An explicit `managed_query` (BYO client + `with_capabilities`)
        // is kept as-is, so the `|=` only ever adds the fork-implied capability.
        #[allow(unused_mut)]
        let mut capabilities = self.capabilities;
        #[cfg(feature = "query")]
        {
            let (managed_host, versions, backends) = probe_managed_host(&client).await;
            capabilities.managed_host = managed_host;
            // Advertised wire op versions, when the server's hello reply
            // carried a body (older servers answer with an empty body).
            capabilities.versions = versions;
            // Materialization backends the server exposes, advertised in the
            // same hello reply. Empty against raw Apache Iggy and pre-backends
            // servers, so a caller routes only to an advertised id.
            capabilities.backends = backends;
            capabilities.managed_query |= managed_host;
            // The same bridge serves the `AGDX_KV` store, so the probe implies `managed_kv`
            // too. An older LaserData Cloud without KV answers `AGDX_KV` with `Unsupported`, which the
            // SDK surfaces as `LaserError::Unsupported`.
            capabilities.managed_kv |= managed_host;
            // Forks (agentic copy-on-write branches of the read model) ride the same
            // managed bridge, so the probe implies `forks` too. An older LaserData Cloud without
            // forks answers `AGDX_FORK_*` with `Unsupported`.
            capabilities.forks |= managed_host;
            // Per-feature managed sub-capabilities (compare-and-swap,
            // read-your-writes, strong consistency) are advertised as bits in
            // the hello reply, NOT implied by `managed_host`: a managed host may
            // serve plain KV without CAS, or eventual queries without a
            // read-your-writes wait. A server that does not set a bit leaves the
            // capability off, and the matching call returns `Unsupported`.
            if let Some(versions) = capabilities.versions {
                capabilities.merge_features(&versions);
            }
        }
        Ok(Laser {
            inner: Arc::new(LaserInner {
                client,
                producers: DashMap::new(),
            }),
            capabilities,
            ops_stream: self
                .ops_stream
                .unwrap_or_else(|| OPS_STREAM_DEFAULT.to_owned()),
            control_topic: self
                .control_topic
                .unwrap_or_else(|| laser_wire::topics::CONTROL_TOPIC.to_owned()),
            stream,
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
#[cfg(feature = "query")]
async fn probe_managed_host(
    client: &IggyClient,
) -> (
    bool,
    Option<crate::capabilities::OpVersions>,
    Vec<crate::capabilities::BackendDescriptor>,
) {
    let wrapper = client.client();
    let guard = wrapper.read().await;
    let result = match &*guard {
        ClientWrapper::Tcp(client) => client.send_raw_with_response(AGDX_HELLO_CODE, Bytes::new()),
        ClientWrapper::Quic(client) => client.send_raw_with_response(AGDX_HELLO_CODE, Bytes::new()),
        ClientWrapper::WebSocket(client) => {
            client.send_raw_with_response(AGDX_HELLO_CODE, Bytes::new())
        }
        ClientWrapper::Http(_) | ClientWrapper::Iggy(_) => return (false, None, Vec::new()),
    };
    match result.await {
        Ok(reply) => match decode_named::<laser_wire::hello::BackendAnnounce>(&reply) {
            Ok(announce) => (true, Some(announce.versions), announce.backends),
            Err(_) => (true, None, Vec::new()),
        },
        Err(_) => (false, None, Vec::new()),
    }
}

/// Normalize a connection string: if no `iggy` scheme is present (`iggy://` or
/// `iggy+<protocol>://`), prepend `iggy://` so a raw `user:pass@host:port` from
/// e.g. a LaserData Cloud bootstrap endpoint works as-is.
fn normalize_connection_string(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.starts_with("iggy://") || trimmed.starts_with("iggy+") {
        trimmed.to_owned()
    } else {
        format!("iggy://{trimmed}")
    }
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
            IggyExpiry::NeverExpire,
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
mod connection_string_tests {
    use super::normalize_connection_string;

    #[test]
    fn given_a_full_tcp_connection_string_when_normalized_then_should_be_unchanged() {
        assert_eq!(
            normalize_connection_string("iggy+tcp://iggy:iggy@127.0.0.1:8090"),
            "iggy+tcp://iggy:iggy@127.0.0.1:8090",
        );
    }

    #[test]
    fn given_a_default_scheme_when_normalized_then_should_be_unchanged() {
        assert_eq!(
            normalize_connection_string("iggy://iggy:iggy@127.0.0.1:8090"),
            "iggy://iggy:iggy@127.0.0.1:8090",
        );
    }

    #[test]
    fn given_a_bare_endpoint_when_normalized_then_should_prepend_default_scheme() {
        assert_eq!(
            normalize_connection_string("iggy:iggy@127.0.0.1:8090"),
            "iggy://iggy:iggy@127.0.0.1:8090",
        );
    }

    #[test]
    fn given_whitespace_around_the_value_when_normalized_then_should_be_trimmed() {
        assert_eq!(
            normalize_connection_string("  iggy:iggy@host:8090  "),
            "iggy://iggy:iggy@host:8090",
        );
    }
}

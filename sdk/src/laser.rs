use crate::capabilities::{Capabilities, HelloOutcome};
use crate::connect_options::{ConnectOptions, connect_before};
use crate::error::{LaserError, TransportError};
use crate::nodes::{Answered, Nodes, Target};
pub use crate::publish_options::PublishOptions;
use bytes::Bytes;
use dashmap::DashMap;
#[cfg(feature = "streaming")]
use iggy::prelude::*;
use iggy_common::{ConnectionString, MAX_REPLICATION_FACTOR, TcpConnectionStringOptions};
use laser_wire::batch::BatchReply;
use laser_wire::codes::AGDX_BATCH_CODE;
use laser_wire::framing::decode_named;
use laser_wire::validate::Validate;
use std::collections::BTreeMap;
use std::net::Ipv6Addr;
use std::sync::Arc;
#[cfg(feature = "kv")]
use std::sync::atomic::AtomicBool;
use tokio::sync::OnceCell;
use tokio::time::{Duration, sleep};
use tracing::Instrument;

// Each publish chunk boxes its send, because inline the driver's deep future
// would push every caller past the layout and `Send` query limits.
type SendFuture<'a> =
    std::pin::Pin<Box<dyn Future<Output = Result<SendMessagesResponse, IggyError>> + Send + 'a>>;

/// How long a managed mutation keeps repeating its frame while its outcome is
/// unknown, the shared client contract deadline.
pub(crate) use crate::retry::{MANAGED_ATTEMPT_FIRST, MANAGED_MUTATION_DEADLINE};

/// What the attempts of one managed mutation know about its outcome. An
/// attempt whose outcome is unknown makes every later refusal ambiguous too,
/// because the earlier attempt may still apply.
#[derive(Debug, Default)]
struct MutationAttempts {
    uncertain: Option<String>,
    /// The unresolved items of a batch frame, by slot, empty for one mutation.
    slots: Vec<Option<String>>,
    batch: bool,
}

/// What one attempt of a managed mutation came back with.
enum Attempt {
    Answered(Vec<u8>),
    /// The request may have left, but no outcome came back.
    Lost(String),
    Refused(LaserError),
}

impl MutationAttempts {
    /// The attempts of one managed request. A batch frame settles item by item.
    fn for_code(code: u32) -> Self {
        Self {
            batch: code == AGDX_BATCH_CODE,
            ..Self::default()
        }
    }

    /// The reply that settles the mutation, `None` to send it again, or the
    /// error the caller gets.
    fn settle(&mut self, attempt: Attempt) -> Result<Option<Vec<u8>>, LaserError> {
        match attempt {
            Attempt::Answered(reply) => {
                if self.batch
                    && let Ok(batch) = decode_named::<BatchReply>(&reply)
                {
                    return self.settle_batch(reply, batch);
                }
                if let Some(error) = unknown_outcome(&reply) {
                    self.uncertain = Some(error.message);
                    return Ok(None);
                }
                match (&self.uncertain, admission_refusal(&reply)) {
                    (Some(earlier), Some(refusal)) => Err(self.ambiguous(earlier, &refusal)),
                    _ => Ok(Some(reply)),
                }
            }
            Attempt::Lost(error) => {
                self.uncertain = Some(error);
                Ok(None)
            }
            Attempt::Refused(error) => match &self.uncertain {
                Some(earlier) => Err(self.ambiguous(earlier, &error.to_string())),
                None => Err(error),
            },
        }
    }

    /// Settle a batch frame item by item. An item whose outcome is unknown
    /// leaves the whole frame unresolved, so the same frame is sent again and
    /// every item answers under its own derived identity: a mutation with its
    /// recorded outcome, a read with a fresh result. An item that was unknown
    /// and is then refused by admission may still apply, like a mutation.
    fn settle_batch(
        &mut self,
        reply: Vec<u8>,
        batch: BatchReply,
    ) -> Result<Option<Vec<u8>>, LaserError> {
        let frame_uncertain = self.uncertain.take();
        self.slots.resize(batch.results.len(), None);
        let mut unresolved = Vec::new();
        for (index, slot) in batch.results.iter().enumerate() {
            let earlier = self.slots[index].take().or_else(|| frame_uncertain.clone());
            if let Some(error) = unknown_outcome(slot) {
                self.slots[index] = Some(error.message);
                unresolved.push(index);
                continue;
            }
            if let (Some(earlier), Some(refusal)) = (earlier, admission_refusal(slot)) {
                return Err(self.ambiguous(&format!("item {index}: {earlier}"), &refusal));
            }
        }
        let Some(first) = unresolved.first() else {
            return Ok(Some(reply));
        };
        self.uncertain = Some(format!(
            "items {unresolved:?} are unresolved: {}",
            self.slots[*first].clone().unwrap_or_default()
        ));
        Ok(None)
    }

    fn ambiguous(&self, earlier: &str, refusal: &str) -> LaserError {
        LaserError::AmbiguousMutation {
            operation_id: None,
            message: format!(
                "an earlier attempt's outcome is unknown ({earlier}), then the request was refused ({refusal})"
            ),
        }
    }
}

/// A refusal that names no outcome of the mutation, only why this attempt was
/// not admitted.
fn admission_refusal(reply: &[u8]) -> Option<String> {
    let error = decode_named::<laser_wire::result::CommandError>(reply).ok()?;
    matches!(
        error.code,
        laser_wire::result::ResultCode::Forbidden
            | laser_wire::result::ResultCode::Unauthenticated
            | laser_wire::result::ResultCode::StepUpRequired
    )
    .then(|| format!("{:?}: {}", error.code, error.message))
}

/// The error of a managed reply that leaves a mutation's outcome unknown, or
/// `None` for a final reply. `Pending` means the managed backend accepted the mutation
/// and has not folded it yet. `BackendUnavailable` means the node's own backend did
/// not answer, so the request may still apply. `Unavailable` means the node was
/// too busy to forward it. Each is repeated under the same operation identity.
pub(crate) fn unknown_outcome(reply: &[u8]) -> Option<laser_wire::result::CommandError> {
    let error = decode_named::<laser_wire::result::CommandError>(reply).ok()?;
    (error.code.is_pending()
        || error.code.is_backend_unavailable()
        || error.code == laser_wire::result::ResultCode::Unavailable)
        .then_some(error)
}

/// Whether `position` is past `other` on the same surface. A later topic
/// generation is past every position of an earlier one, because the earlier
/// topic is gone and its offsets are never read again.
fn is_past(
    position: &laser_wire::mutation::MutationPosition,
    other: &laser_wire::mutation::MutationPosition,
) -> bool {
    (position.topic_generation, position.offset) > (other.topic_generation, other.offset)
}

/// Raise each surface of `seen` to the later of its own position and the one
/// in `answered`.
fn merge_frontier(
    seen: &mut laser_wire::mutation::AppliedFrontier,
    answered: &laser_wire::mutation::AppliedFrontier,
) {
    for (slot, position) in [
        (&mut seen.kv, answered.kv),
        (&mut seen.fork, answered.fork),
        (&mut seen.graph, answered.graph),
    ] {
        if let Some(position) = position
            && slot.is_none_or(|current| is_past(&position, &current))
        {
            *slot = Some(position);
        }
    }
    for position in &answered.sessions {
        // A newer incarnation of the stream replaces every position of the
        // one before it.
        seen.sessions.retain(|kept| {
            kept.stream_id != position.stream_id
                || kept.stream_generation >= position.stream_generation
        });
        let same_unit = |kept: &laser_wire::mutation::SessionPosition| {
            kept.stream_id == position.stream_id
                && kept.topic == position.topic
                && kept.partition_id == position.partition_id
        };
        let order = |unit: &laser_wire::mutation::SessionPosition| {
            (unit.stream_generation, unit.topic_generation, unit.offset)
        };
        match seen.sessions.iter_mut().find(|kept| same_unit(kept)) {
            Some(kept) if order(position) > order(kept) => *kept = position.clone(),
            Some(_) => {}
            None => seen.sessions.push(position.clone()),
        }
    }
}

pub(crate) use crate::retry::jittered;

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
// `_agdx` stream while data rides the data stream, so the cache key must carry
// the stream too or the two would collide on a shared topic name.
type ProducerKey = (String, String);
type ProducerCell = Arc<OnceCell<Arc<IggyProducer>>>;
const PUBLISH_BATCH_LENGTH: usize = 1000;

/// How a [`Laser`] names the managed resources it sends: KV, memory, lease,
/// and fence namespaces, the key registry, graph names, projection ids and
/// index names, and fork ids.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ResourceNaming {
    /// Scope every managed resource name to the default stream,
    /// `stream:<stream>/<name>`. A name already scoped to that stream is sent
    /// as is, a `stream:` name for another stream is scoped like a local name,
    /// and a default stream containing `/` refuses managed calls. A handle
    /// without a default stream sends bare names.
    #[default]
    Stream,
    /// Send every name exactly as the caller wrote it, deployment wide.
    Bare,
}

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
    resource_naming: ResourceNaming,
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
    client: std::sync::RwLock<Arc<IggyClient>>,
    reconnect_gate: tokio::sync::Mutex<()>,
    // Bumped by every publish reconnect so a publish that failed on an older
    // connection cannot tear down the one another publish already replaced it
    // with.
    publish_generation: std::sync::atomic::AtomicU64,
    publish_options: PublishOptions,
    // The normalized connection string, when this handle built its own client.
    // Dedicated connections (fenced-lease coordination, group readers) reuse
    // its credentials and TLS settings.
    connection_string: Option<String>,
    #[cfg(feature = "kv")]
    coordination: OnceCell<Arc<crate::kv::FencedLeaseClient<crate::kv::DedicatedKvTransport>>>,
    producers: DashMap<ProducerKey, ProducerCell>,
    // The highest mutation position seen per surface, from mutation and read
    // replies. Every KV, fork and graph read sends it back, so whichever node
    // answers has applied at least what this client wrote or read before.
    seen_frontier: std::sync::Mutex<laser_wire::mutation::AppliedFrontier>,
    // The node each managed request goes to.
    nodes: Nodes,
    pub(crate) producer_statistics: Arc<crate::stream::producer_statistics::ProducerRegistry>,
    negotiated: std::sync::RwLock<NegotiatedState>,
    // Lets one caller at a time re-probe an unmanaged capability set.
    reprobe_gate: tokio::sync::Mutex<()>,
    // The agent registry read model's per-stream cache, so a fresh `AgentRegistry`
    // resumes the card fold instead of re-reading the registry topic from offset 0.
    // Keyed by data stream (the isolation boundary the registry topic lives on).
    #[cfg(feature = "agent")]
    registry_caches: DashMap<String, Arc<std::sync::Mutex<crate::agent::registry::RegistryCache>>>,
    // The session leases this connection holds, read by its heartbeat task.
    #[cfg(feature = "agent")]
    leases: Arc<crate::agent::lease::LeaseRegistry>,
    // The session layout declared per data stream, read at the agent send
    // boundary to pick each record's partition.
    #[cfg(feature = "agent")]
    layouts: DashMap<String, crate::agent::SessionLayout>,
    // Connection metadata has one slot. Reserve it for one logical agent across
    // every clone so a second advertisement cannot overwrite the first route.
    // Presence rides the managed metadata command, so the slot exists only
    // where `advertise_presence` compiles (plus its unit test).
    #[cfg(all(feature = "agent", any(feature = "query", test)))]
    advertised_agent: std::sync::Mutex<Option<crate::types::AgentId>>,
    #[cfg(all(feature = "agent", feature = "query"))]
    presence_replica: tokio::sync::Mutex<Option<u8>>,
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
    // When the server was last probed. `None` until the first probe.
    probed_at: Option<tokio::time::Instant>,
}

// How long a set without a managed backend, or with an enabled backend that
// is not ready, is trusted before `capabilities()` probes again, so a backend
// that becomes ready after connect is picked up.
const REPROBE_INTERVAL: Duration = Duration::from_secs(1);

impl Laser {
    #[cfg(all(feature = "agent", feature = "query"))]
    pub(crate) fn claim_presence(
        &self,
        requested: crate::types::AgentId,
    ) -> Result<(), LaserError> {
        claim_presence_slot(&self.inner.advertised_agent, requested)
    }

    /// The replica that holds this connection's advertised presence, pinned
    /// by the first advertise and released by a clear that reached it. The
    /// lock serializes advertise and clear, so one cannot overtake the other.
    #[cfg(all(feature = "agent", feature = "query"))]
    pub(crate) fn presence_pin(&self) -> &tokio::sync::Mutex<Option<u8>> {
        &self.inner.presence_replica
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
    /// LaserData's public root CA, bundled in the SDK itself. Set `LASER_TLS_CERT=<path>` to enable TLS with an explicit CA for any host or to override the bundled CA. Disable automatic TLS with `LASER_NO_TLS=1`. Other hosts keep their Apache Iggy TLS settings when neither variable is set. Connection strings use the bare `user:password@host[:port]` form because `Laser::connect` supplies the TCP scheme, and the port defaults to 8090.
    ///
    /// Connecting gives up after 30 seconds, or `LASER_CONNECT_TIMEOUT_MS`, with a [`LaserError::Timeout`] that says whether the server never accepted the connection or never answered the login. Set another budget with [`LaserBuilder::connect_timeout`].
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
    /// laser.topic("inferences").ensure(4, None).await?;
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
    /// stream. The fenced-lease convenience methods need a separately owned
    /// coordination connection, so a bring-your-own client uses an explicit
    /// `FencedLeaseClient` for those mutations.
    pub fn from_client(client: IggyClient) -> Self {
        Self {
            inner: Arc::new(LaserInner {
                client: std::sync::RwLock::new(Arc::new(client)),
                publish_options: PublishOptions::default(),
                reconnect_gate: tokio::sync::Mutex::new(()),
                seen_frontier: std::sync::Mutex::default(),
                nodes: Nodes::new(caller_key()),
                publish_generation: std::sync::atomic::AtomicU64::new(0),
                connection_string: None,
                #[cfg(feature = "kv")]
                coordination: OnceCell::new(),
                producers: DashMap::new(),
                producer_statistics: Default::default(),
                negotiated: std::sync::RwLock::new(NegotiatedState {
                    configured_capabilities: Capabilities::OPEN,
                    capabilities: Capabilities::OPEN,
                    topology: laser_wire::topology::WireTopology::default(),
                    probed_at: None,
                }),
                reprobe_gate: tokio::sync::Mutex::new(()),
                #[cfg(feature = "agent")]
                registry_caches: DashMap::new(),
                #[cfg(feature = "agent")]
                leases: Arc::default(),
                #[cfg(feature = "agent")]
                layouts: DashMap::new(),
                #[cfg(feature = "agent")]
                #[cfg(all(feature = "agent", any(feature = "query", test)))]
                advertised_agent: std::sync::Mutex::new(None),
                #[cfg(all(feature = "agent", feature = "query"))]
                presence_replica: tokio::sync::Mutex::new(None),
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
            resource_naming: ResourceNaming::default(),
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
                    self.client(),
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
    pub fn with_capabilities(&self, capabilities: Capabilities) -> Self {
        let mut scoped = self.clone();
        scoped.capability_override = Some(capabilities);
        scoped
    }

    /// Returns a clone of this `Laser` whose query/control surface rides
    /// `ops_stream` instead of the default [`OPS_STREAM_DEFAULT`] (`_agdx`). The
    /// underlying connection and producer cache are shared with the original.
    /// Production keeps the default, since one LaserData Cloud per deployment
    /// owns `_agdx`. Tests override it for per-case isolation.
    #[must_use]
    pub fn with_ops_stream(&self, ops_stream: impl Into<String>) -> Self {
        let mut scoped = self.clone();
        scoped.ops_stream_override = Some(ops_stream.into());
        scoped
    }

    /// Returns a clone of this `Laser` whose control commands publish to
    /// `control_topic` on the ops stream instead of the default
    /// (`control.commands`). The underlying connection and producer cache are
    /// shared. Production keeps the default, a deployment with its own ops-topic
    /// naming overrides it.
    #[must_use]
    pub fn with_control_topic(&self, control_topic: impl Into<String>) -> Self {
        let mut scoped = self.clone();
        scoped.control_topic_override = Some(control_topic.into());
        scoped
    }

    /// Returns a clone of this `Laser` whose dead-letter capsules publish to
    /// `dlq_topic` on the ops stream instead of the default (`dlq`). The
    /// underlying connection and producer cache are shared. Production keeps the
    /// default, a deployment with its own ops-topic naming overrides it.
    #[must_use]
    pub fn with_dlq_topic(&self, dlq_topic: impl Into<String>) -> Self {
        let mut scoped = self.clone();
        scoped.dlq_topic_override = Some(dlq_topic.into());
        scoped
    }

    /// Returns a clone of this `Laser` whose change-feed records publish to
    /// `changes_topic` on the ops stream instead of the default (`changes`). The
    /// underlying connection and producer cache are shared. Production keeps the
    /// default, a deployment with its own ops-topic naming overrides it.
    #[must_use]
    pub fn with_changes_topic(&self, changes_topic: impl Into<String>) -> Self {
        let mut scoped = self.clone();
        scoped.changes_topic_override = Some(changes_topic.into());
        scoped
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

    /// A clone of this `Laser` that names managed resources under `naming`,
    /// sharing the one connection. [`ResourceNaming::Bare`] opts out of the
    /// default stream scoping.
    #[must_use]
    pub fn with_resource_naming(&self, naming: ResourceNaming) -> Self {
        let mut renamed = self.clone();
        renamed.resource_naming = naming;
        renamed
    }

    /// How this handle names the managed resources it sends.
    pub fn resource_naming(&self) -> ResourceNaming {
        self.resource_naming
    }

    /// The name this handle sends for the managed resource `name`:
    /// `stream:<default stream>/<name>` under [`ResourceNaming::Stream`] with a
    /// default stream, else `name` unchanged. A name already scoped to the
    /// default stream is returned unchanged, and a `stream:` name for any other
    /// stream is scoped like a local name. Re-scope with
    /// [`with_default_stream`](Self::with_default_stream) or use
    /// [`ResourceNaming::Bare`] to address another stream.
    pub fn resource_name(&self, name: &str) -> String {
        self.resource_name_in(self.default_stream(), name)
    }

    // The default stream when this handle scopes its resources to it.
    pub(crate) fn resource_stream(&self) -> Option<&str> {
        self.resource_scope(self.default_stream())
    }

    // `stream` when this handle scopes its resources, the gate every scoped name
    // and every lens `stream` field passes through.
    pub(crate) fn resource_scope<'a>(&self, stream: Option<&'a str>) -> Option<&'a str> {
        match self.resource_naming {
            ResourceNaming::Stream => stream.filter(|stream| !stream.is_empty()),
            ResourceNaming::Bare => None,
        }
    }

    // `name` scoped to `stream` under this handle's naming. An empty name stays
    // empty so the caller's validation still rejects it. A name already scoped
    // to `stream` is kept, and any other `stream:` name is scoped like a local
    // name, so caller input never selects another stream's resources.
    pub(crate) fn resource_name_in(&self, stream: Option<&str>, name: &str) -> String {
        match self.resource_scope(stream) {
            Some(stream) if !name.is_empty() => {
                match laser_wire::authz::split_scoped_resource(name) {
                    Some((owner, _)) if owner == stream => name.to_owned(),
                    _ => format!("{}/{name}", laser_wire::authz::stream_resource(stream)),
                }
            }
            _ => name.to_owned(),
        }
    }

    // A handle that scopes its names to a default stream containing `/` would
    // send names every server reads as belonging to the stream before the
    // first slash, so its managed calls are refused.
    fn ensure_scopable(&self, code: u32) -> Result<(), LaserError> {
        match self.resource_stream() {
            Some(stream) if stream.contains('/') && code != laser_wire::codes::AGDX_HELLO_CODE => {
                Err(LaserError::Invalid(format!(
                    "default stream `{stream}` contains `/`, so managed names cannot be scoped to it, use ResourceNaming::Bare"
                )))
            }
            _ => Ok(()),
        }
    }

    // The caller's name for `name` returned by a listing: the local part when it
    // sits under this handle's own prefix, `None` when it belongs to another
    // stream or, while scoping is active, to no stream. A handle that does not
    // scope keeps every name unchanged.
    #[cfg(any(feature = "kv", feature = "fork", feature = "projections", test))]
    pub(crate) fn local_resource_name<'a>(&self, name: &'a str) -> Option<&'a str> {
        let Some(stream) = self.resource_stream() else {
            return Some(name);
        };
        match laser_wire::authz::split_scoped_resource(name) {
            Some((owner, local)) if owner == stream => Some(local),
            _ => None,
        }
    }

    pub(crate) fn publish_options(&self) -> PublishOptions {
        self.inner.publish_options
    }

    /// The raw `IggyClient` this laser holds. Most callers should not need it.
    pub fn client(&self) -> Arc<IggyClient> {
        self.inner
            .client
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// Where this client's managed requests go.
    pub(crate) fn nodes(&self) -> &Nodes {
        &self.inner.nodes
    }

    #[doc(hidden)]
    pub async fn caller_replica(&self) -> Result<u8, LaserError> {
        self.nodes()
            .caller_node(&self.client())
            .await
            .map(|(replica, _)| replica)
            .map_err(LaserError::from)
    }

    /// Close the shared connection. Every clone of this `Laser`, and every consumer or reply reader riding it, loses the connection too. A fenced-lease coordination connection this handle opened closes with it. Safe to call more than once.
    pub async fn close(&self) -> Result<(), LaserError> {
        self.inner.producer_statistics.close();
        #[cfg(feature = "kv")]
        if self.inner.connection_string.is_some() {
            self.coordination_client().await?.close().await;
        }
        #[cfg(feature = "agent")]
        self.inner.reply_hubs.clear();
        self.client().shutdown().await.map_err(LaserError::from)
    }

    pub(crate) fn publish_generation(&self) -> u64 {
        self.inner
            .publish_generation
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Reconnect the shared client for a publish that failed on generation
    /// `observed`. A concurrent publish that already reconnected has moved the
    /// generation on, and tearing its fresh connection down again would starve
    /// every publisher during a rolling restart, so that call returns instead.
    pub(crate) async fn reconnect_for_publish(&self, observed: u64) -> Result<(), LaserError> {
        let _gate = self.inner.reconnect_gate.lock().await;
        if self.publish_generation() != observed {
            return Ok(());
        }
        let client = self.client();
        // Keep consumers and reply readers on the same client and its diagnostic events.
        client.disconnect().await?;
        client.connect().await?;
        self.inner.producers.clear();
        self.inner
            .publish_generation
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// The connection string this handle connected with, or `None` for a
    /// bring-your-own client.
    pub(crate) fn producer_statistics_registry(
        &self,
    ) -> &Arc<crate::stream::producer_statistics::ProducerRegistry> {
        &self.inner.producer_statistics
    }

    pub(crate) fn connection_string(&self) -> Option<&str> {
        self.inner.connection_string.as_deref()
    }

    #[cfg(feature = "kv")]
    pub(crate) async fn coordination_client(
        &self,
    ) -> Result<Arc<crate::kv::FencedLeaseClient<crate::kv::DedicatedKvTransport>>, LaserError>
    {
        let connection = self
            .inner
            .connection_string
            .as_ref()
            .ok_or(LaserError::Config(
                "fenced lease convenience methods need a connection-backed Laser; use FencedLeaseClient with a dedicated transport for a bring-your-own IggyClient",
            ))?
            .clone();
        Ok(self
            .inner
            .coordination
            .get_or_init(|| async move {
                Arc::new(crate::kv::FencedLeaseClient::connect_dedicated(connection))
            })
            .await
            .clone())
    }

    #[cfg(feature = "kv")]
    pub(crate) async fn acquire_lease(
        &self,
        request: laser_wire::kv::KvLease,
    ) -> Result<crate::kv::Lease, LaserError> {
        self.ensure_scopable(laser_wire::codes::AGDX_KV_LEASE_CODE)?;
        let laser = self.clone();
        // The owned task keeps an attempt and its transport retirement running
        // if the caller cancels while the outcome is still unknown.
        tokio::spawn(async move {
            let coordination = laser.coordination_client().await?;
            let prepared = coordination.prepare_acquire(&request)?;
            let lease = coordination.acquire_resolving(&prepared).await?;
            laser.observe_kv_position(lease.position);
            Ok(lease)
        })
        .await
        .map_err(|error| {
            LaserError::HandlerConfig(format!("coordination acquisition task failed: {error}"))
        })?
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

    #[cfg(feature = "agent")]
    pub(crate) fn declare_layout(&self, stream: &str, layout: crate::agent::SessionLayout) {
        self.inner.layouts.insert(stream.to_owned(), layout);
    }

    #[cfg(feature = "agent")]
    pub(crate) fn layout(&self, stream: &str) -> Option<crate::agent::SessionLayout> {
        self.inner.layouts.get(stream).map(|layout| layout.clone())
    }

    // The topic a record sent on `topic` lands on under the default stream's
    // declared per-agent topic layout, or `None` to keep `topic`.
    #[cfg(feature = "agent")]
    pub(crate) fn agent_destination(
        &self,
        topic: &str,
        kind: Option<laser_wire::agent::AgentKind>,
        target: Option<&laser_wire::agent::AgentId>,
    ) -> Option<String> {
        let layout = self.default_stream().and_then(|stream| self.layout(stream));
        crate::agent::partitioning::destination_topic(layout.as_ref(), topic, kind, target)
    }

    /// The topic `agent` reads its addressed work and replies on under the
    /// default stream's declared per-agent topic layout, or `None` when the
    /// agent is not declared. A language binding spawns its agents on it.
    #[cfg(feature = "agent")]
    #[doc(hidden)]
    pub fn declared_topic(&self, agent: &laser_wire::agent::AgentId) -> Option<String> {
        let layout = self.default_stream().and_then(|stream| self.layout(stream));
        crate::agent::partitioning::declared_topic(layout.as_ref(), agent)
    }

    /// The session leases this connection holds.
    #[cfg(feature = "agent")]
    pub(crate) fn lease_registry(&self) -> &Arc<crate::agent::lease::LeaseRegistry> {
        &self.inner.leases
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

    /// The negotiated capability set (default [`Capabilities::OPEN`]). A set
    /// without a managed backend, or with an enabled backend that is not ready,
    /// is probed again when it is at least one second old, so a backend that
    /// was not ready at connect is picked up without an explicit
    /// [`refresh_capabilities`](Self::refresh_capabilities). A set whose
    /// backends are all ready and an explicit override are returned as they
    /// are. Open features work regardless of the result.
    pub async fn capabilities(&self) -> Capabilities {
        if let Some(capabilities) = &self.capability_override {
            return capabilities.clone();
        }
        if !self.reprobe_due() {
            return self.current_capabilities();
        }
        let _gate = self.inner.reprobe_gate.lock().await;
        // Another caller may have probed while this one waited.
        if !self.reprobe_due() {
            return self.current_capabilities();
        }
        self.refresh_capabilities().await
    }

    fn reprobe_due(&self) -> bool {
        let negotiated = self
            .inner
            .negotiated
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let unsettled = !negotiated.capabilities.managed
            || negotiated.capabilities.unready_backends().next().is_some();
        unsettled
            && negotiated
                .probed_at
                .is_none_or(|at| at.elapsed() >= REPROBE_INTERVAL)
    }

    // The set held now, injected or last negotiated, without a reprobe. Public
    // for the Python binding's synchronous `with_capabilities`.
    #[doc(hidden)]
    pub fn current_capabilities(&self) -> Capabilities {
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
        match probe_managed_host(&self.client(), &self.inner.nodes).await {
            Ok(Some(announce)) => {
                merge_announcement(&mut capabilities, &announce);
                topology = announce.topology;
                capabilities.hello = HelloOutcome::Answered;
            }
            Ok(None) => capabilities.hello = HelloOutcome::Rejected,
            Err(()) => capabilities.hello = HelloOutcome::Failed,
        }
        let mut negotiated = self
            .inner
            .negotiated
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        negotiated.capabilities = capabilities;
        negotiated.probed_at = Some(tokio::time::Instant::now());
        if let Some(topology) = topology {
            negotiated.topology = topology;
        }
        self.capability_override
            .clone()
            .unwrap_or_else(|| negotiated.capabilities.clone())
    }

    pub async fn wait_until_ready(&self, timeout: Duration) -> Result<Capabilities, LaserError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let capabilities = self.refresh_capabilities().await;
            if capabilities.is_ready() {
                return Ok(capabilities);
            }
            if capabilities.backends.is_empty() {
                return Err(LaserError::unsupported(
                    "readiness",
                    "server has no managed backend descriptors",
                ));
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(LaserError::Timeout("managed backend readiness"));
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Idempotently creates `topic` on `stream` with `partitions`, an explicit
    /// message expiry and an optional replication factor (the server default
    /// when `None`), creating the stream first if needed.
    pub(crate) async fn ensure_topic_on_with(
        &self,
        stream: &str,
        topic: &str,
        partitions: u32,
        expiry: IggyExpiry,
        replication_factor: Option<u32>,
    ) -> Result<(), LaserError> {
        if let Some(factor) = replication_factor {
            validate_replication_factor(factor)?;
        }
        let client = self.client();
        ensure_stream(&client, stream).await?;
        ensure_topic_with(
            &client,
            stream,
            topic,
            partitions,
            expiry,
            replication_factor,
        )
        .await
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
    ) -> Result<SendMessagesResponse, LaserError> {
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
    ) -> Result<SendMessagesResponse, LaserError> {
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
    ) -> Result<SendMessagesResponse, LaserError> {
        let stream = self.stream_required()?.to_owned();
        self.send_batch_on(&stream, topic, messages, partition_key)
            .await
    }

    /// Like [`send_batch`](Self::send_batch) but targets `stream` instead of this
    /// laser's data stream. Used for the `_agdx` ops stream.
    // A plain function over the batch send, since every async layer here
    // deepens each publish future that callers have to lay out.
    pub(crate) fn send_batch_on<'a>(
        &'a self,
        stream: &'a str,
        topic: &'a str,
        messages: Vec<IggyMessage>,
        partition_key: Option<&str>,
    ) -> impl Future<Output = Result<SendMessagesResponse, LaserError>> + 'a {
        let span = tracing::debug_span!(target: "laser", "send_batch_on", topic = %topic, operation = "publish");
        let partitioning = partition_key.map_or_else(
            || Ok(Partitioning::balanced()),
            |key| Partitioning::messages_key_str(key).map_err(LaserError::from),
        );
        self.send_batch_partitioned_on_checked(stream, topic, messages, partitioning, || async {
            Ok(None)
        })
        .instrument(span)
    }

    /// Like [`send_batch_on`](Self::send_batch_on) with an explicit
    /// `partitioning`, such as one declared partition.
    #[cfg(feature = "agent")]
    pub(crate) fn send_batch_partitioned_on<'a>(
        &'a self,
        stream: &'a str,
        topic: &'a str,
        messages: Vec<IggyMessage>,
        partitioning: Partitioning,
    ) -> impl Future<Output = Result<SendMessagesResponse, LaserError>> + 'a {
        self.send_batch_partitioned_on_checked(
            stream,
            topic,
            messages,
            Ok(partitioning),
            || async { Ok(None) },
        )
    }

    // An empty batch is sent as nothing, before `partitioning` is checked.
    pub(crate) async fn send_batch_partitioned_on_checked<F, Fut>(
        &self,
        stream: &str,
        topic: &str,
        mut messages: Vec<IggyMessage>,
        partitioning: Result<Partitioning, LaserError>,
        check: F,
    ) -> Result<SendMessagesResponse, LaserError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<Option<(u32, u32, Partitioning)>, LaserError>>,
    {
        if messages.is_empty() {
            return Ok(SendMessagesResponse {
                confirmations: Vec::new(),
            });
        }
        let partitioning = Arc::new(partitioning?);
        let first = std::sync::Mutex::new(Some(check().await?));
        prepare_publish_messages(&mut messages);
        let mut confirmations = Vec::new();
        let observed = std::sync::atomic::AtomicU64::new(0);
        for (index, chunk) in messages.chunks(PUBLISH_BATCH_LENGTH).enumerate() {
            let response = self
                .inner
                .publish_options
                .run(
                    || async {
                        observed.store(
                            self.publish_generation(),
                            std::sync::atomic::Ordering::Release,
                        );
                        let cached = first.lock().expect("first publish source").take();
                        let source = match cached {
                            Some(source) => source,
                            None => check().await?,
                        };
                        if let Some((stream_id, topic_id, route)) = source {
                            let mut batch: Vec<_> = chunk.iter().map(clone_iggy_message).collect();
                            let client = self.client();
                            let stream_id = Identifier::numeric(stream_id)?;
                            let topic_id = Identifier::numeric(topic_id)?;
                            let sent: SendFuture<'_> = Box::pin(
                                client.send_messages(&stream_id, &topic_id, &route, &mut batch),
                            );
                            return Ok(sent.await?);
                        }
                        let producer = self.producer_on(stream, topic).await?;
                        let sent: SendFuture<'_> = Box::pin(producer.send_with_partitioning(
                            chunk.iter().map(clone_iggy_message).collect(),
                            Some(partitioning.clone()),
                        ));
                        let sent = sent.await;
                        match sent {
                            // A cached producer can outlive a deleted and
                            // recreated stream or topic. Rebuild it once.
                            Err(error) if is_missing_resource(&error) => {
                                self.reconnect_for_publish(self.publish_generation())
                                    .await?;
                                let producer = self.producer_on(stream, topic).await?;
                                let resent: SendFuture<'_> =
                                    Box::pin(producer.send_with_partitioning(
                                        chunk.iter().map(clone_iggy_message).collect(),
                                        Some(partitioning.clone()),
                                    ));
                                Ok(resent.await?)
                            }
                            other => Ok(other?),
                        }
                    },
                    || {
                        self.reconnect_for_publish(
                            observed.load(std::sync::atomic::Ordering::Acquire),
                        )
                    },
                )
                .await
                .map_err(|error| {
                    publish_failure(
                        error,
                        &messages[index * PUBLISH_BATCH_LENGTH..],
                        std::mem::take(&mut confirmations),
                        chunk.len(),
                        (stream, topic),
                    )
                })?;
            confirmations.extend(response.confirmations);
        }
        Ok(SendMessagesResponse { confirmations })
    }

    /// Send a managed command `code` with `payload` over the existing binary
    /// connection and return the raw reply bytes. The query path uses it for
    /// `AGDX_QUERY` on the server, and the connect-time probe uses it for
    /// `AGDX_HELLO`. Every managed command is non-replicated from Iggy's
    /// perspective. Idempotent writes carry a stable operation identity so
    /// the managed backend can append them to its mutation topics, see
    /// [`send_managed_mutation`](Self::send_managed_mutation).
    #[tracing::instrument(target = "laser", level = "debug", skip_all, fields(code = code, operation = "managed"))]
    pub(crate) async fn send_raw_with_response(
        &self,
        code: u32,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, LaserError> {
        Ok(self
            .send_managed_to(code, payload, Target::of_code(code))
            .await?
            .0)
    }

    /// Send managed command `code` to `target` like
    /// [`send_raw_with_response`](Self::send_raw_with_response), and name the
    /// replica that answered, `None` for the metadata primary.
    pub(crate) async fn send_managed_to(
        &self,
        code: u32,
        payload: Vec<u8>,
        target: Target,
    ) -> Result<(Vec<u8>, Option<u8>), LaserError> {
        self.ensure_scopable(code)?;
        if !laser_wire::codes::is_idempotent_managed_request(code) {
            let frame = bytes::Bytes::from(self.read_frame(code, &payload)?);
            let answered = self
                .inner
                .nodes
                .send(&self.client(), code, frame, target)
                .await?;
            return Ok((self.take_positioned(&answered), answered.replica));
        }
        self.send_managed_with_operation(code, payload, u128::from(ulid::Ulid::generate()), target)
            .await
    }

    pub(crate) async fn send_managed_with_operation(
        &self,
        code: u32,
        payload: Vec<u8>,
        operation_id: u128,
        target: Target,
    ) -> Result<(Vec<u8>, Option<u8>), LaserError> {
        self.ensure_scopable(code)?;
        if operation_id == 0 {
            return Err(LaserError::Invalid(
                "operation id must be nonzero".to_owned(),
            ));
        }
        let frame =
            laser_wire::framing::encode_named(&laser_wire::mutation::ManagedRequestEnvelope {
                v: laser_wire::mutation::MANAGED_REQUEST_VERSION,
                operation_id,
                payload,
            })
            .map_err(|error| LaserError::Codec(format!("encode managed request: {error}")))?;
        self.send_managed_mutation(code, operation_id, frame, target)
            .await
    }

    /// Send managed read or control command `code` to `target` with the
    /// reply awaited until `deadline`, naming the replica that answered.
    #[cfg(all(feature = "agent", feature = "query"))]
    pub(crate) async fn send_managed_within(
        &self,
        code: u32,
        payload: Vec<u8>,
        target: Target,
        deadline: tokio::time::Instant,
    ) -> Result<(Vec<u8>, Option<u8>), LaserError> {
        self.ensure_scopable(code)?;
        let frame = bytes::Bytes::from(self.read_frame(code, &payload)?);
        let answered = self
            .inner
            .nodes
            .send_held(&self.client(), code, frame, target, deadline.into_std())
            .await?;
        Ok((self.take_positioned(&answered), answered.replica))
    }

    /// Send one managed mutation until its outcome is known. The frame, and
    /// with it the operation identity, never changes between attempts, so the
    /// server applies the mutation at most once and returns the recorded
    /// outcome to a repeat. A `Pending` reply or a transport failure after the
    /// request may have left leaves the outcome unknown, so the same frame is
    /// sent again with jittered backoff, and a refusal after that stays
    /// ambiguous. Each attempt is bounded from [`MANAGED_ATTEMPT_FIRST`] up,
    /// within the one [`MANAGED_MUTATION_DEADLINE`] that roster reads,
    /// reconnects, attempts and backoff share. When the deadline passes first,
    /// the caller gets [`LaserError::AmbiguousMutation`] naming the identity:
    /// the mutation may still apply.
    async fn send_managed_mutation(
        &self,
        code: u32,
        operation_id: u128,
        frame: Vec<u8>,
        target: Target,
    ) -> Result<(Vec<u8>, Option<u8>), LaserError> {
        let deadline = tokio::time::Instant::now() + MANAGED_MUTATION_DEADLINE;
        let mut delay = crate::retry::MANAGED_MUTATION.base;
        let mut bound = MANAGED_ATTEMPT_FIRST;
        let mut attempts = MutationAttempts::for_code(code);
        loop {
            let attempt_deadline = (tokio::time::Instant::now() + bound).min(deadline);
            let (attempt, replica) = match self
                .inner
                .nodes
                .send_held(
                    &self.client(),
                    code,
                    bytes::Bytes::from(frame.clone()),
                    target,
                    attempt_deadline.into_std(),
                )
                .await
            {
                Ok(answered) => (
                    Attempt::Answered(self.take_positioned(&answered)),
                    answered.replica,
                ),
                Err(error) if crate::error::is_transient_iggy_io_error(&error) => {
                    (Attempt::Lost(error.to_string()), None)
                }
                Err(error) => (Attempt::Refused(error.into()), None),
            };
            if let Some(reply) = attempts.settle(attempt).map_err(|error| match error {
                LaserError::AmbiguousMutation { message, .. } => LaserError::AmbiguousMutation {
                    operation_id: Some(operation_id),
                    message: format!("operation {operation_id:032x}: {message}"),
                },
                error => error,
            })? {
                return Ok((reply, replica));
            }
            let unknown = attempts.uncertain.clone().unwrap_or_default();
            let wait = jittered(delay);
            if tokio::time::Instant::now() + wait >= deadline {
                return Err(LaserError::AmbiguousMutation {
                    operation_id: Some(operation_id),
                    message: format!(
                        "operation {operation_id:032x} outcome is still unknown after {}s: {unknown}",
                        MANAGED_MUTATION_DEADLINE.as_secs()
                    ),
                });
            }
            tracing::debug!(
                target: "laser",
                code,
                operation = %format!("{operation_id:032x}"),
                %unknown,
                "managed mutation outcome unknown, repeating it under the same identity"
            );
            sleep(wait).await;
            delay = delay
                .saturating_mul(2)
                .min(crate::retry::MANAGED_MUTATION.max);
            bound = bound.saturating_mul(2);
        }
    }

    /// The frame for managed read `code`. A KV, fork, graph or session read
    /// always goes in a
    /// [`ManagedReadEnvelope`](laser_wire::mutation::ManagedReadEnvelope)
    /// carrying the highest positions this client has seen for the surface, so
    /// the reply brings the answering node's frontier back.
    fn read_frame(&self, code: u32, read: &[u8]) -> Result<Vec<u8>, LaserError> {
        if !laser_wire::codes::is_barrier_read(code) {
            return Ok(read.to_vec());
        }
        let (min_position, min_sessions) = {
            let seen = self
                .inner
                .seen_frontier
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match laser_wire::mutation::PositionSurface::of_code(code) {
                Some(laser_wire::mutation::PositionSurface::Kv) => (seen.kv, Vec::new()),
                Some(laser_wire::mutation::PositionSurface::Fork) => (seen.fork, Vec::new()),
                Some(laser_wire::mutation::PositionSurface::Graph) => (seen.graph, Vec::new()),
                // A session read: the managed backend keeps only the positions of the
                // read's own stream.
                None => (None, seen.sessions.clone()),
            }
        };
        laser_wire::framing::encode_named(&laser_wire::mutation::ManagedReadEnvelope {
            v: laser_wire::mutation::MANAGED_REQUEST_VERSION,
            read: read.to_vec(),
            min_position,
            log_head: false,
            min_sessions,
        })
        .map_err(|error| LaserError::Codec(format!("encode managed read: {error}")))
    }

    /// The opened reply, after raising the seen frontier to the one its
    /// positioned envelope carried.
    fn take_positioned(&self, answered: &Answered) -> Vec<u8> {
        if let Some(frontier) = &answered.frontier {
            merge_frontier(
                &mut self
                    .inner
                    .seen_frontier
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                frontier,
            );
        }
        answered.reply.to_vec()
    }

    /// The highest mutation position this client has seen per managed
    /// surface. Every KV, fork and graph reply raises it, and every later read
    /// of that surface sends it, so whichever node answers has applied at
    /// least what this client wrote or read before. Clones of this handle
    /// share it.
    pub fn seen_positions(&self) -> laser_wire::mutation::AppliedFrontier {
        self.inner
            .seen_frontier
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    // A grant or renewal answered over the dedicated coordination connection
    // raises this handle's KV frontier, so a later read sees the acquisition.
    #[cfg(feature = "kv")]
    pub(crate) fn observe_kv_position(&self, position: laser_wire::mutation::MutationPosition) {
        merge_frontier(
            &mut self
                .inner
                .seen_frontier
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            &laser_wire::mutation::AppliedFrontier {
                kv: Some(position),
                ..Default::default()
            },
        );
    }

    /// Send an already-framed managed command: the caller built (and holds)
    /// the `ManagedRequestEnvelope`, so nothing is wrapped here and a retry
    /// can reuse the exact bytes under the same operation id. The KV
    /// coordination client is the caller. Every other path uses
    /// [`send_raw_with_response`](Self::send_raw_with_response), which mints a
    /// fresh operation id per call. `written` is marked once an attempt wrote
    /// the frame to a connection.
    #[cfg(feature = "kv")]
    #[tracing::instrument(target = "laser", level = "debug", skip_all, fields(code = code, operation = "managed_preframed"))]
    pub(crate) async fn send_raw_preframed(
        &self,
        code: u32,
        payload: Vec<u8>,
        written: &AtomicBool,
    ) -> Result<Vec<u8>, IggyError> {
        let answered = self
            .inner
            .nodes
            .send_to_caller_marking(&self.client(), code, bytes::Bytes::from(payload), written)
            .await?;
        Ok(self.take_positioned(&answered))
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
        Ok(self.client().producer(stream, topic)?)
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
        Ok(self
            .client()
            .consumer(name, stream, topic)?
            .partitions([partition]))
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
        Ok(self.client().consumer_group(group, stream, topic)?)
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
                let producer = self
                    .client()
                    .producer(stream, topic)?
                    .direct(
                        DirectConfig::builder()
                            .batch_length(PUBLISH_BATCH_LENGTH as u32)
                            .build(),
                    )
                    .build();
                init_producer(&producer).await?;
                Ok::<_, LaserError>(Arc::new(producer))
            })
            .await?;
        Ok(producer.clone())
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

// `pending` is the failed chunk followed by every record after it, and
// `attempted` is that chunk's length. Apache Iggy reports which records of the
// chunk it did not confirm, so only those join the unsent tail. A timeout
// leaves the whole chunk unconfirmed.
pub(crate) fn publish_failure(
    error: LaserError,
    pending: &[IggyMessage],
    mut confirmations: Vec<SendMessagesConfirmationResponse>,
    attempted: usize,
    target: (&str, &str),
) -> LaserError {
    let unsent = pending.get(attempted..).unwrap_or_default();
    let (source, stream, topic, unconfirmed) = match error {
        LaserError::Transport(TransportError(IggyError::ProducerSendFailed {
            cause,
            failed,
            uncertain: _,
            committed,
            stream_name,
            topic_name,
        })) => {
            confirmations.extend(committed.iter().cloned());
            let unconfirmed = failed
                .iter()
                .chain(unsent)
                .map(clone_iggy_message)
                .collect();
            (
                LaserError::from(*cause),
                stream_name.into(),
                topic_name.into(),
                unconfirmed,
            )
        }
        error => (
            error,
            target.0.to_owned(),
            target.1.to_owned(),
            pending.iter().map(clone_iggy_message).collect(),
        ),
    };
    LaserError::PublishFailed(Box::new(crate::error::PublishFailure {
        source,
        stream,
        topic,
        committed: confirmations,
        unconfirmed,
    }))
}

pub(crate) fn prepare_publish_messages(messages: &mut [IggyMessage]) {
    for message in messages {
        if message.header.id == 0 {
            message.header.id = u128::from(ulid::Ulid::generate());
        }
    }
}

pub(crate) fn clone_iggy_message(message: &IggyMessage) -> IggyMessage {
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

/// Initialize a producer, treating a concurrent creation of its stream or topic
/// as success. Apache Iggy's init reads, then creates, so two producers racing
/// on a new topic see `AlreadyExists` on the loser. The retry reads the stream
/// and topic the winner created.
pub(crate) async fn init_producer(producer: &IggyProducer) -> Result<(), IggyError> {
    for attempt in 0..crate::retry::CREATE_RACE_ATTEMPTS {
        match producer.init().await {
            Err(error)
                if is_idempotent_create_race(&error)
                    && attempt + 1 < crate::retry::CREATE_RACE_ATTEMPTS =>
            {
                sleep(Duration::from_millis(50 * (attempt + 1) as u64)).await;
            }
            result => return result,
        }
    }
    unreachable!("producer init retry returns success or the last error")
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
    connect_timeout: Option<Duration>,
    publish_timeout: Option<Duration>,
    publish_max_retries: Option<u32>,
    publish_retry_backoff: Option<Duration>,
    stream: Option<String>,
    resource_naming: ResourceNaming,
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

impl ConnectionConfig {
    // The normalized connection string a dialed connection carries. The
    // client and every second connection dial through it, so each of them
    // re-authenticates with the same credentials after a reconnect.
    fn connection_string(&self) -> Result<String, LaserError> {
        match self {
            Self::Unset | Self::Client(_) => Err(LaserError::Config(
                "connection_string, address+credentials, or client is required",
            )),
            Self::ConnectionString(value) => normalize_connection_string(value),
            Self::Tcp {
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
                // Encoded, so a `:`, `@` or `?` in either part stays inside it.
                normalize_connection_string(&format!(
                    "iggy+tcp://{}:{}@{address}",
                    percent_encode(username),
                    percent_encode(password)
                ))
            }
        }
    }
}

impl LaserBuilder {
    /// Budget for the initial connect: TCP dial, TLS handshake, login, and the managed capability probe. An expired budget returns [`LaserError::Timeout`] naming the stage that stalled. Default: 30 seconds, or `LASER_CONNECT_TIMEOUT_MS`.
    pub fn connect_timeout(mut self, value: Duration) -> Self {
        self.connect_timeout = Some(value);
        self
    }

    /// Per-attempt publish budget, including producer setup and reconnect. Default: 60 seconds.
    pub fn publish_timeout(mut self, value: Duration) -> Self {
        self.publish_timeout = Some(value);
        self
    }

    /// Additional at-least-once publish attempts. Default: 3. Zero disables retries.
    pub fn publish_max_retries(mut self, value: u32) -> Self {
        self.publish_max_retries = Some(value);
        self
    }

    /// Initial exponential retry delay, capped at 30 seconds. Default: 250 milliseconds.
    pub fn publish_retry_backoff(mut self, value: Duration) -> Self {
        self.publish_retry_backoff = Some(value);
        self
    }

    /// Connect using an Iggy TCP connection string: `user:password@host[:port]`,
    /// `token@host[:port]`, optionally prefixed with `iggy://` or `iggy+tcp://`.
    /// The port defaults to 8090. Other transports are refused. The most
    /// ergonomic option.
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

    /// Connect over TCP to `address` (`host` or `host:port`, the port defaults to 8090). Requires `credentials`.
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

    /// How the client names the managed resources it sends. Default
    /// [`ResourceNaming::Stream`] scopes them to the default stream.
    /// [`ResourceNaming::Bare`] sends names exactly as written.
    pub fn resource_naming(mut self, value: ResourceNaming) -> Self {
        self.resource_naming = value;
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
        let publish_options = PublishOptions::from_env(
            self.publish_timeout,
            self.publish_max_retries,
            self.publish_retry_backoff,
        )?;
        let connect_deadline =
            tokio::time::Instant::now() + ConnectOptions::from_env(self.connect_timeout)?.timeout;
        let stream = self.stream.filter(|value| !value.is_empty());
        let (client, connection_string) = match self.connection {
            ConnectionConfig::Client(client) => (client, None),
            connection => {
                let normalized = connection.connection_string()?;
                let client = tcp_client(&normalized)?;
                connect_before(&client, connect_deadline).await?;
                (client, Some(normalized))
            }
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
        let configured_capabilities = self.capabilities.clone().unwrap_or(Capabilities::OPEN);
        let mut capabilities = configured_capabilities.clone();
        let mut topology = laser_wire::topology::WireTopology::default();
        // A probe still unanswered at the connect deadline leaves the set
        // open-only, like any other probe failure, and the outcome records
        // that nothing was established, so a group consumer does not read a
        // deployment it could not classify as unfiltered.
        let nodes = Nodes::new(caller_key());
        match tokio::time::timeout_at(connect_deadline, probe_managed_host(&client, &nodes)).await {
            Ok(Ok(Some(announce))) => {
                if let Some(announced) = announce.topology.clone() {
                    topology = announced;
                }
                merge_announcement(&mut capabilities, &announce);
                capabilities.hello = HelloOutcome::Answered;
            }
            Ok(Ok(None)) => capabilities.hello = HelloOutcome::Rejected,
            Ok(Err(())) | Err(_) => capabilities.hello = HelloOutcome::Failed,
        }
        Ok(Laser {
            inner: Arc::new(LaserInner {
                client: std::sync::RwLock::new(Arc::new(client)),
                publish_options,
                reconnect_gate: tokio::sync::Mutex::new(()),
                seen_frontier: std::sync::Mutex::default(),
                nodes,
                publish_generation: std::sync::atomic::AtomicU64::new(0),
                connection_string,
                #[cfg(feature = "kv")]
                coordination: OnceCell::new(),
                producers: DashMap::new(),
                producer_statistics: Default::default(),
                negotiated: std::sync::RwLock::new(NegotiatedState {
                    configured_capabilities,
                    capabilities,
                    topology,
                    probed_at: Some(tokio::time::Instant::now()),
                }),
                reprobe_gate: tokio::sync::Mutex::new(()),
                #[cfg(feature = "agent")]
                registry_caches: DashMap::new(),
                #[cfg(feature = "agent")]
                leases: Arc::default(),
                #[cfg(feature = "agent")]
                layouts: DashMap::new(),
                #[cfg(feature = "agent")]
                #[cfg(all(feature = "agent", any(feature = "query", test)))]
                advertised_agent: std::sync::Mutex::new(None),
                #[cfg(all(feature = "agent", feature = "query"))]
                presence_replica: tokio::sync::Mutex::new(None),
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
            resource_naming: self.resource_naming,
            #[cfg(feature = "agent")]
            governor: self.governor,
        })
    }
}

// Cheap, non-fatal capability probe: send the server's `AGDX_HELLO` managed command
// over the binary connection. `Ok` means the connected infrastructure is Laser Streams
// and exposes the managed bridge (query/KV/browse/fork off the log). Any error (raw
// Apache Iggy answers `InvalidCommand`) leaves `managed_host` false. A reply body,
// when present, is the CBOR `BackendAnnounce` advertising the wire op versions
// the server accepts plus the materialization backends it exposes. It decodes
// byte-identically from a pre-backends `HelloReply` (the `backends` list is
// skip-when-empty), so an older server's versions-only reply still parses.
// Older servers answer with an empty body, which leaves the versions
// unadvertised (`None`), the backends empty, and the SDK skips fail-fast version
// checks.
// `Ok(None)` is a server that answered without a managed announcement, Apache
// Iggy refusing the command or an older server's empty body, so the managed
// surfaces are positively absent. `Err` is a probe that established nothing:
// the transport failed or the reply did not decode.
async fn probe_managed_host(
    client: &IggyClient,
    nodes: &Nodes,
) -> Result<Option<laser_wire::hello::BackendAnnounce>, ()> {
    match nodes
        .send(
            client,
            laser_wire::codes::AGDX_HELLO_CODE,
            Bytes::new(),
            Target::Caller,
        )
        .await
        .map(|answered| answered.reply)
    {
        Ok(reply) if reply.is_empty() => Ok(None),
        Ok(reply) => decode_named::<laser_wire::hello::BackendAnnounce>(&reply)
            .ok()
            .filter(|announce| announce.validate().is_ok())
            .map(Some)
            .ok_or(()),
        Err(IggyError::InvalidCommand) => Ok(None),
        Err(_) => Err(()),
    }
}

// The key that picks this client's node: random, so clients spread over the
// cluster, and fixed for the handle's life.
fn caller_key() -> u64 {
    u64::try_from(ulid::Ulid::generate().random() & u128::from(u64::MAX)).unwrap_or_default()
}

fn merge_announcement(
    capabilities: &mut Capabilities,
    announce: &laser_wire::hello::BackendAnnounce,
) {
    let versions = announce.versions;
    capabilities.versions = Some(versions);
    capabilities.backends.clone_from(&announce.backends);
    capabilities.authz |= versions.has_feature(laser_wire::hello::feature::AUTHZ);
    capabilities.filters.native |=
        versions.has_feature(laser_wire::hello::feature::CONSUMER_FILTERS);
    // Served by the streaming server itself, like the native reads: a server
    // without a managed backend resolves every group as unbound.
    capabilities.filters.group_policy_reads |=
        versions.has_feature(laser_wire::hello::feature::GROUP_POLICY_READS);
    if capabilities.filters.native {
        capabilities
            .filters
            .evaluation
            .clone_from(&announce.filters);
        capabilities.filters.max_wait =
            announce.filters.as_ref().map_or(Duration::ZERO, |filters| {
                Duration::from_millis(u64::from(filters.max_wait_ms))
            });
    }
    if announce.ready {
        capabilities.managed = true;
        capabilities.filters.catalog |= (capabilities.filters.native
            || capabilities.filters.group_policy_reads)
            && versions.filter > 0;
        capabilities.kv.available |= versions.kv > 0;
        capabilities.forks |= versions.fork > 0;
        capabilities.graph |= versions.graph > 0;
        capabilities.merge_features(&versions);
        for backend in announce
            .backends
            .iter()
            .filter(|backend| backend.readiness.ready)
        {
            if let Some(query) = &backend.query {
                capabilities.query.available |= versions.query > 0;
                if query
                    .consistency
                    .contains(&laser_wire::query::Consistency::ReadYourWrites)
                {
                    capabilities.query.consistency = capabilities
                        .query
                        .consistency
                        .max(laser_wire::query::Consistency::ReadYourWrites);
                }
            }
        }
    }
}

#[cfg(all(
    test,
    any(
        feature = "filters",
        feature = "fork",
        feature = "graph",
        feature = "kv",
        feature = "projections",
        feature = "query",
        feature = "rbac"
    )
))]
mod announcement_tests {
    use super::*;
    use laser_wire::destination::BackendResourceId;
    use laser_wire::hello::{
        BackendAnnounce, BackendDescriptor, BackendDesiredState, BackendImplementation,
        BackendMode, BackendObservedState, BackendReadiness, OpVersions, QueryCapabilities,
        feature,
    };

    fn backend(id: u128, ready: bool) -> BackendDescriptor {
        let backend = BackendDescriptor::new(
            BackendResourceId::from_u128(id),
            BackendMode::Operational,
            "Embedded",
            BackendImplementation {
                kind: "embedded".to_owned(),
                version: "1.0.0".to_owned(),
            },
            1,
            1,
        );
        if !ready {
            return backend;
        }
        backend
            .with_state(
                BackendDesiredState::Enabled,
                BackendObservedState::Ready,
                BackendReadiness::ready(1),
            )
            .with_query(QueryCapabilities {
                dialects: vec![laser_wire::query::SqlDialect::Sqlite],
                consistency: vec![laser_wire::query::Consistency::Eventual],
                logical_types: vec![laser_wire::schema::LogicalTypeKind::String],
                raw_sql: true,
                aggregates: Vec::new(),
                vector_search: false,
                text_search: false,
            })
            .with_limits(laser_wire::hello::BackendLimits {
                max_query_rows: 1_000,
                max_query_bytes: 1_000_000,
                max_scan_bytes: 10_000_000,
                max_query_micros: 30_000_000,
                max_concurrent_queries: 8,
                max_schema_fields: 0,
                max_payload_bytes: 0,
            })
    }

    #[test]
    fn given_consumer_filters_without_a_managed_backend_when_merged_then_should_serve_native_filters_only()
     {
        let announce = BackendAnnounce::new(
            OpVersions::new(0, 0, 0, 0).with_features(feature::CONSUMER_FILTERS | feature::AUTHZ),
        )
        .unavailable();
        let mut capabilities = Capabilities::OPEN;

        merge_announcement(&mut capabilities, &announce);

        assert!(capabilities.filters.native);
        assert!(!capabilities.filters.catalog);
        assert!(!capabilities.managed);
    }

    #[test]
    fn given_an_announced_filtered_wait_when_merged_then_should_expose_its_limit() {
        let announce = BackendAnnounce::new(
            OpVersions::new(0, 0, 0, 0)
                .with_features(feature::CONSUMER_FILTERS | feature::GROUP_POLICY_READS),
        )
        .with_filters(laser_wire::hello::FilterAnnounce {
            max_wait_ms: 20_000,
            ..laser_wire::hello::FilterAnnounce::served()
        })
        .unavailable();
        let mut capabilities = Capabilities::OPEN;

        merge_announcement(&mut capabilities, &announce);

        assert_eq!(capabilities.filters.max_wait, Duration::from_secs(20));
    }

    #[test]
    fn given_filters_announced_without_a_wait_when_merged_then_should_serve_no_filtered_wait() {
        let announce = BackendAnnounce::new(
            OpVersions::new(0, 0, 0, 0).with_features(feature::CONSUMER_FILTERS),
        )
        .unavailable();
        let mut capabilities = Capabilities::OPEN;

        merge_announcement(&mut capabilities, &announce);

        assert_eq!(capabilities.filters.max_wait, Duration::ZERO);
    }

    #[test]
    fn given_consumer_filters_and_a_ready_catalog_when_merged_then_should_serve_both() {
        let announce = BackendAnnounce::new(
            OpVersions::new(1, 1, 1, 1)
                .with_filter(laser_wire::codes::FILTER_OP_VERSION)
                .with_features(feature::CONSUMER_FILTERS),
        )
        .with_backends(vec![backend(1, true)]);
        let mut capabilities = Capabilities::OPEN;

        merge_announcement(&mut capabilities, &announce);

        assert!(capabilities.filters.native);
        assert!(capabilities.filters.catalog);
    }

    #[test]
    fn given_an_unavailable_backend_when_merged_then_should_not_enable_managed_surfaces() {
        let announce = BackendAnnounce::new(
            OpVersions::new(1, 1, 1, 1)
                .with_agent(1)
                .with_graph(1)
                .with_features(
                    feature::KV_CAS
                        | feature::KV_CAS_FENCED
                        | feature::READ_YOUR_WRITES
                        | feature::SESSIONS
                        | feature::KEYWORD_SEARCH
                        | feature::WATCH
                        | feature::AUTHZ,
                ),
        )
        .with_backends(vec![backend(1, false)])
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
        assert!(!capabilities.sessions);
        assert!(!capabilities.watch);
        assert!(capabilities.authz, "server-native authz remains available");
        assert_eq!(capabilities.backends, announce.backends);
        assert_eq!(capabilities.versions, Some(announce.versions));
    }

    // Gated exactly like `merge_announcement` itself: with none of the managed
    // surface features enabled the function does not exist, so neither can its
    // tests.
    #[test]
    fn given_a_ready_backend_when_merged_then_should_enable_advertised_managed_surfaces() {
        let announce = BackendAnnounce::new(
            OpVersions::new(1, 1, 1, 1)
                .with_graph(1)
                .with_features(feature::KV_CAS | feature::SESSIONS),
        )
        .with_backends(vec![backend(1, true)]);
        let mut capabilities = Capabilities::OPEN;

        merge_announcement(&mut capabilities, &announce);

        assert!(capabilities.managed);
        assert!(capabilities.query.available);
        assert!(capabilities.kv.available);
        assert!(capabilities.kv.cas);
        assert!(capabilities.graph);
        assert!(capabilities.forks);
        assert!(capabilities.sessions);
        assert_eq!(capabilities.backends, announce.backends);
    }
}

// Apache Iggy's TCP port. A connection string or address may omit it.
const DEFAULT_TCP_PORT: u16 = 8090;

// The query parameters Apache Iggy's TCP transport reads. Any other key is
// refused, so a misspelled option fails at connect instead of being ignored.
const CONNECTION_OPTIONS: [&str; 8] = [
    "tls",
    "tls_domain",
    "tls_ca_file",
    "reconnection_retries",
    "reconnection_interval",
    "reestablish_after",
    "heartbeat_interval",
    "nodelay",
];

// The login a connection string's userinfo names, percent-decoded.
enum Userinfo {
    Password { username: String, password: String },
    Token(String),
}

impl From<Userinfo> for Credentials {
    fn from(userinfo: Userinfo) -> Self {
        match userinfo {
            Userinfo::Password { username, password } => {
                Self::UsernamePassword(username, password.into())
            }
            Userinfo::Token(token) => Self::PersonalAccessToken(token.into()),
        }
    }
}

/// Validate and normalize a connection string. The scheme is optional and
/// must be `iggy://` or `iggy+tcp://`, so a raw `user:pass@host:port` from a
/// LaserData Cloud bootstrap endpoint works as-is. Credentials are required,
/// either `user:password@` or `token@`, and are percent-decoded, so a `:`,
/// `@`, `?` or `%` inside one is written `%3A`, `%40`, `%3F` or `%25`. A missing port
/// defaults to 8090, the Apache Iggy TCP port. Only Apache Iggy's TCP options
/// are accepted, each at most once. `tls_ca_file=` without `tls=` turns TLS on,
/// and `tls=false` next to `tls_ca_file=` is refused. Then, for a LaserData
/// Cloud host that does not already name a `tls_ca_file=`, attach `tls=true`
/// plus LaserData's bundled public CA so a bare connection string is enough.
/// `LASER_TLS_CERT=<path>` enables TLS with that CA for any host or overrides
/// the bundled CA. `LASER_NO_TLS=1` and an explicit `tls=false` disable
/// automatic TLS setup. The connection string's own `tls_ca_file=` remains
/// authoritative.
fn normalize_connection_string(value: &str) -> Result<String, LaserError> {
    let trimmed = value.trim();
    let (scheme, rest) = split_scheme(trimmed)?;
    let authority = authority_start(rest);
    if authority == 0 {
        return Err(LaserError::Config(
            "connection string missing credentials: use user:password@host or token@host",
        ));
    }
    let userinfo = &rest[..authority - 1];
    decode_userinfo(userinfo)?;
    let (address, query) = match rest[authority..].split_once('?') {
        Some((address, query)) => (address, Some(query)),
        None => (&rest[authority..], None),
    };
    let address = with_default_port(address)?;
    let mut normalized = format!("{scheme}{userinfo}@{address}");
    if let Some(query) = query {
        let options = validate_options(query)?;
        normalized = format!("{normalized}?{query}");
        if options.contains(&"tls_ca_file") && !options.contains(&"tls") {
            normalized.push_str("&tls=true");
        }
    }
    resolve_tls(normalized).map(with_tls_domain)
}

/// A TCP client for a normalized connection string, logging in by itself
/// after every reconnect.
pub(crate) fn tcp_client(normalized: &str) -> Result<IggyClient, LaserError> {
    let client = TcpClient::create(Arc::new(tcp_client_config(normalized)?))?;
    Ok(IggyClientBuilder::new()
        .with_client(ClientWrapper::Tcp(client))
        .build()?)
}

// Apache Iggy's parser splits userinfo on `:` and `@` without decoding it, so
// it reads the address and options next to a placeholder login, and the
// decoded credentials replace that login.
fn tcp_client_config(normalized: &str) -> Result<TcpClientConfig, LaserError> {
    let (scheme, rest) = split_scheme(normalized)?;
    let authority = authority_start(rest);
    let Some(userinfo) = authority.checked_sub(1).map(|end| &rest[..end]) else {
        return Err(LaserError::Config(
            "connection string missing credentials: use user:password@host or token@host",
        ));
    };
    let credentials = decode_userinfo(userinfo)?;
    let mut config: TcpClientConfig = ConnectionString::<TcpConnectionStringOptions>::new(
        &format!("{scheme}laser@{}", &rest[authority..]),
    )?
    .into();
    config.auto_login = AutoLogin::Enabled(credentials.into());
    Ok(config)
}

// Split off the scheme. A string with no scheme gets `iggy://`. A `://` after
// the userinfo has begun belongs to a password, not to a scheme.
fn split_scheme(value: &str) -> Result<(&'static str, &str), LaserError> {
    if let Some(rest) = value.strip_prefix("iggy://") {
        return Ok(("iggy://", rest));
    }
    if let Some(rest) = value.strip_prefix("iggy+tcp://") {
        return Ok(("iggy+tcp://", rest));
    }
    match value.split_once("://") {
        Some((scheme, _)) if !scheme.contains([':', '@']) => Err(LaserError::Config(
            "unsupported connection string scheme: use iggy:// or iggy+tcp://",
        )),
        _ => Ok(("iggy://", value)),
    }
}

// Credentials are `user:password` or a personal access token, both non-empty
// and percent-decoded (RFC 3986). A raw `@` ends the userinfo and a raw `:`
// ends the username, so either character inside a credential is encoded.
fn decode_userinfo(userinfo: &str) -> Result<Userinfo, LaserError> {
    if userinfo.contains('@') {
        return Err(LaserError::Config(
            "connection string credentials cannot contain a raw `@`, write it as %40",
        ));
    }
    let mut parts = userinfo.split(':');
    let first = parts.next().unwrap_or_default();
    match (parts.next(), parts.next()) {
        (None, _) if first.is_empty() => Err(LaserError::Config(
            "connection string has an empty access token",
        )),
        (None, _) => Ok(Userinfo::Token(percent_decode(first)?)),
        (Some(password), None) if first.is_empty() || password.is_empty() => Err(
            LaserError::Config("connection string has an empty username or password"),
        ),
        (Some(password), None) => Ok(Userinfo::Password {
            username: percent_decode(first)?,
            password: percent_decode(password)?,
        }),
        (Some(_), Some(_)) => Err(LaserError::Config(
            "connection string credentials cannot contain more than one raw `:`, write it as %3A",
        )),
    }
}

fn percent_decode(value: &str) -> Result<String, LaserError> {
    let malformed =
        || LaserError::Config("connection string credentials hold a malformed % escape");
    let mut decoded = Vec::with_capacity(value.len());
    let mut bytes = value.bytes();
    while let Some(byte) = bytes.next() {
        if byte != b'%' {
            decoded.push(byte);
            continue;
        }
        let high = bytes
            .next()
            .and_then(|digit| char::from(digit).to_digit(16));
        let low = bytes
            .next()
            .and_then(|digit| char::from(digit).to_digit(16));
        let (Some(high), Some(low)) = (high, low) else {
            return Err(malformed());
        };
        decoded.push(u8::try_from(high << 4 | low).map_err(|_| malformed())?);
    }
    String::from_utf8(decoded).map_err(|_| {
        LaserError::Config("connection string credentials are not UTF-8 once percent-decoded")
    })
}

// Every byte outside RFC 3986's unreserved set as `%XX`.
fn percent_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(byte).to_string()
            }
            other => format!("%{other:02X}"),
        })
        .collect()
}

// `host`, `host:port`, `[ipv6]` or `[ipv6]:port`. A missing port becomes 8090.
fn with_default_port(address: &str) -> Result<String, LaserError> {
    let malformed = || {
        LaserError::Config(
            "connection string address must be host, host:port, [ipv6] or [ipv6]:port, without a path",
        )
    };
    if address.contains(['/', '#']) {
        return Err(malformed());
    }
    let (host, port) = match address.strip_prefix('[') {
        Some(bracketed) => {
            let (literal, rest) = bracketed.split_once(']').ok_or_else(malformed)?;
            if literal.parse::<Ipv6Addr>().is_err() {
                return Err(LaserError::Config(
                    "connection string brackets must hold an IPv6 address",
                ));
            }
            let port = match rest {
                "" => None,
                rest => Some(rest.strip_prefix(':').ok_or_else(malformed)?),
            };
            (&address[..literal.len() + 2], port)
        }
        None if address.contains(['[', ']']) => return Err(malformed()),
        None => match address.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (address, None),
        },
    };
    if host.is_empty() {
        return Err(LaserError::Config("connection string missing host"));
    }
    match port {
        None => Ok(format!("{host}:{DEFAULT_TCP_PORT}")),
        Some(port) => match port.parse::<u16>() {
            Ok(number) if number > 0 && !port.starts_with('+') => Ok(format!("{host}:{number}")),
            _ => Err(LaserError::Config(
                "connection string port must be a number from 1 to 65535",
            )),
        },
    }
}

// Every option is `key=value`, names one of Apache Iggy's TCP options, and
// appears once. Boolean options take `true` or `false` only. Returns the keys.
fn validate_options(query: &str) -> Result<Vec<&str>, LaserError> {
    let mut keys = Vec::new();
    let mut tls_off = false;
    for pair in query.split('&') {
        let mut parts = pair.split('=');
        let (Some(key), Some(value), None) = (parts.next(), parts.next(), parts.next()) else {
            return Err(LaserError::Config(
                "connection string option must be key=value",
            ));
        };
        if !CONNECTION_OPTIONS.contains(&key) {
            return Err(LaserError::Config(
                "connection string has an unknown option: supported are tls, tls_domain, tls_ca_file, reconnection_retries, reconnection_interval, reestablish_after, heartbeat_interval, nodelay",
            ));
        }
        if keys.contains(&key) {
            return Err(LaserError::Config("connection string repeats an option"));
        }
        if matches!(key, "tls" | "nodelay") && !matches!(value, "true" | "false") {
            return Err(LaserError::Config(
                "connection string tls and nodelay take true or false",
            ));
        }
        tls_off |= key == "tls" && value == "false";
        keys.push(key);
    }
    if tls_off && keys.contains(&"tls_ca_file") {
        return Err(LaserError::Config(
            "connection string sets tls=false together with tls_ca_file",
        ));
    }
    Ok(keys)
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
pub(crate) fn host_of(connection_string: &str) -> &str {
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

// The client dials every node at the address the cluster roster advertises.
// Node certificates carry the deployment host name, so a TLS connection keeps
// verifying the host it was given instead of each node's address.
fn with_tls_domain(connection_string: String) -> String {
    let tls = query_of(&connection_string).is_some_and(|query| {
        query
            .split('&')
            .any(|pair| pair.eq_ignore_ascii_case("tls=true"))
    });
    if !tls || has_query_param(&connection_string, "tls_domain") {
        return connection_string;
    }
    let domain = host_of(&connection_string).to_owned();
    format!("{connection_string}&tls_domain={domain}")
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
    let custom_cert = std::env::var("LASER_TLS_CERT")
        .ok()
        .filter(|path| !path.is_empty())
        .map(std::path::PathBuf::from);
    resolve_tls_with(
        connection_string,
        env_flag_enabled("LASER_NO_TLS"),
        custom_cert,
    )
}

fn resolve_tls_with(
    connection_string: String,
    no_tls: bool,
    custom_cert: Option<std::path::PathBuf>,
) -> Result<String, LaserError> {
    if no_tls
        || has_query_param(&connection_string, "tls_ca_file")
        || query_of(&connection_string)
            .is_some_and(|query| query.split('&').any(|pair| pair == "tls=false"))
    {
        return Ok(connection_string);
    }
    if custom_cert.is_none() && !is_laserdata_host(host_of(&connection_string)) {
        return Ok(connection_string);
    }
    let cert_path = custom_cert.map_or_else(resolve_cert_path, Ok)?;
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
    if let Err(error) = client
        .create_stream(stream, &StreamCreateOptions::default())
        .await
        && client
            .get_stream(&Identifier::named(stream)?)
            .await?
            .is_none()
    {
        return Err(error.into());
    }
    Ok(())
}

fn is_missing_resource(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::ResourceNotFound(_)
            | IggyError::StreamIdNotFound(_)
            | IggyError::StreamNameNotFound(_)
            | IggyError::TopicIdNotFound(_, _)
            | IggyError::TopicNameNotFound(_, _)
    )
}

pub(crate) async fn delete_stream(laser: &Laser, stream: &str) -> Result<bool, LaserError> {
    let client = laser.client();
    let identifier = Identifier::named(stream)?;
    let existed = client.get_stream(&identifier).await?.is_some();
    if existed
        && let Err(error) = client.delete_stream(&identifier).await
        && client.get_stream(&identifier).await?.is_some()
    {
        return Err(error.into());
    }
    laser
        .inner
        .producers
        .retain(|(producer_stream, _), _| producer_stream != stream);
    #[cfg(feature = "agent")]
    {
        laser.inner.registry_caches.remove(stream);
        laser
            .inner
            .reply_hubs
            .retain(|(hub_stream, _), _| hub_stream != stream);
    }
    Ok(existed)
}

#[cfg(feature = "agent")]
pub(crate) async fn ensure_topic(
    client: &IggyClient,
    stream: &str,
    topic: &str,
    partitions: u32,
) -> Result<(), LaserError> {
    ensure_topic_with(
        client,
        stream,
        topic,
        partitions,
        IggyExpiry::NeverExpire,
        None,
    )
    .await
}

/// Idempotently create `topic` with an explicit message-expiry, so a caller
/// (the memory topic) can bound how long records live, and an optional
/// replication factor.
pub(crate) async fn ensure_topic_with(
    client: &IggyClient,
    stream: &str,
    topic: &str,
    partitions: u32,
    expiry: IggyExpiry,
    replication_factor: Option<u32>,
) -> Result<(), LaserError> {
    ensure_topic_retained(
        client,
        stream,
        topic,
        partitions,
        expiry,
        MaxTopicSize::ServerDefault,
        replication_factor,
    )
    .await
}

/// Idempotently create `topic` with an explicit expiry, size bound and
/// replication factor. `None` leaves the factor to the server's default.
pub(crate) async fn ensure_topic_retained(
    client: &IggyClient,
    stream: &str,
    topic: &str,
    partitions: u32,
    expiry: IggyExpiry,
    max_size: MaxTopicSize,
    replication_factor: Option<u32>,
) -> Result<(), LaserError> {
    if let Some(factor) = replication_factor {
        validate_replication_factor(factor)?;
    }
    let stream_id = Identifier::named(stream)?;
    let topic_id = Identifier::named(topic)?;
    if client.get_topic(&stream_id, &topic_id).await?.is_some() {
        return Ok(());
    }
    let options = TopicCreateOptions {
        partitions_count: Some(partitions),
        compression_algorithm: Some(CompressionAlgorithm::default()),
        message_expiry: Some(expiry),
        max_topic_size: Some(max_size),
        replication_factor,
        ..TopicCreateOptions::default()
    };
    let result = client.create_topic(&stream_id, topic, &options).await;
    if let Err(error) = result
        && client.get_topic(&stream_id, &topic_id).await?.is_none()
    {
        return Err(error.into());
    }
    Ok(())
}

/// Refuse a replication factor the server never accepts, before anything is
/// sent. The server also caps it at the cluster's node count.
pub(crate) fn validate_replication_factor(factor: u32) -> Result<(), LaserError> {
    if (1..=MAX_REPLICATION_FACTOR).contains(&factor) {
        return Ok(());
    }
    Err(LaserError::Invalid(format!(
        "replication factor must be 1..={MAX_REPLICATION_FACTOR}"
    )))
}

#[cfg(test)]
mod builder_conflict_tests {
    #[cfg(feature = "agent")]
    use super::claim_presence_slot;
    use super::{Laser, clone_iggy_message, prepare_publish_messages};
    use crate::error::{LaserError, TransportError};
    use bytes::Bytes;
    use iggy::prelude::IggyMessage;
    use laser_wire::destination::BackendResourceId;
    use laser_wire::hello::{
        BackendDescriptor, BackendDesiredState, BackendImplementation, BackendMode,
        BackendObservedState, BackendReadiness, BackendReadinessCode,
    };
    #[cfg(feature = "agent")]
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

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
    fn given_a_publish_batch_when_cloned_for_retry_then_should_preserve_message_identity() {
        let message = IggyMessage::builder()
            .payload(Bytes::from_static(b"retry-body"))
            .build()
            .expect("the retry fixture message builds");
        let mut messages = vec![message];
        prepare_publish_messages(&mut messages);
        let retry = clone_iggy_message(&messages[0]);
        assert_ne!(retry.header.id, 0);
        assert_eq!(retry.header.id, messages[0].header.id);
        assert_eq!(retry.payload, Bytes::from_static(b"retry-body"));
        assert_eq!(retry.user_headers, messages[0].user_headers);
    }

    #[test]
    fn given_a_failed_chunk_when_reporting_progress_then_should_keep_confirmations_and_unsent_tail()
    {
        use iggy::prelude::{IggyError, SendMessagesConfirmationResponse};
        use std::sync::Arc;

        let message = |id| {
            IggyMessage::builder()
                .id(id)
                .payload(Bytes::from_static(b"body"))
                .build()
                .expect("message builds")
        };
        let confirmed = SendMessagesConfirmationResponse {
            stream_id: 1,
            topic_id: 2,
            partition_id: 0,
            base_offset: 9,
        };
        let error = super::publish_failure(
            LaserError::from(IggyError::ProducerSendFailed {
                cause: Box::new(IggyError::Disconnected),
                failed: Arc::new(vec![message(2)]),
                uncertain: 0,
                committed: Arc::new(vec![]),
                stream_name: "stream".into(),
                topic_name: "topic".into(),
            }),
            &[message(1), message(2), message(3)],
            vec![confirmed],
            2,
            ("other", "other"),
        );
        assert!(error.is_retryable());
        assert_eq!(
            error.to_string(),
            "publish failed to stream/topic: Disconnected"
        );
        let LaserError::PublishFailed(failure) = error else {
            panic!("preserve the structured publish error");
        };
        assert!(matches!(
            failure.source,
            LaserError::Transport(TransportError(IggyError::Disconnected))
        ));
        assert_eq!(
            failure
                .unconfirmed
                .iter()
                .map(|message| message.header.id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(failure.committed.len(), 1);
        assert_eq!(failure.committed[0].base_offset, 9);
    }

    #[test]
    fn given_a_partially_confirmed_batch_when_a_chunk_times_out_then_should_preserve_progress() {
        let message = |id| {
            IggyMessage::builder()
                .id(id)
                .payload(Bytes::from_static(b"body"))
                .build()
                .expect("message builds")
        };
        let error = super::publish_failure(
            LaserError::Timeout("Iggy publish response"),
            &[message(4), message(5), message(6)],
            vec![iggy::prelude::SendMessagesConfirmationResponse {
                stream_id: 1,
                topic_id: 2,
                partition_id: 0,
                base_offset: 9,
            }],
            2,
            ("stream", "topic"),
        );
        assert!(error.is_retryable());
        assert!(matches!(error.publish_cause(), LaserError::Timeout(_)));
        let LaserError::PublishFailed(failure) = error else {
            panic!("a timeout must retain batch progress");
        };
        assert_eq!(failure.committed.len(), 1);
        assert_eq!(failure.committed[0].base_offset, 9);
        assert_eq!(
            failure
                .unconfirmed
                .iter()
                .map(|message| message.header.id)
                .collect::<Vec<_>>(),
            vec![4, 5, 6]
        );
        assert_eq!(failure.stream, "stream");
        assert_eq!(failure.topic, "topic");
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

    #[tokio::test]
    async fn given_a_server_that_never_answers_login_when_building_then_should_time_out_at_the_budget()
     {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a local listener");
        let address = listener.local_addr().expect("read the bound address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept the client");
            let mut buffer = [0_u8; 1024];
            while socket.read(&mut buffer).await.is_ok_and(|read| read > 0) {}
        });
        let started = tokio::time::Instant::now();
        let result = Laser::builder()
            .connection_string(format!("iggy:iggy@{address}"))
            .connect_timeout(Duration::from_millis(300))
            .build()
            .await;
        assert!(matches!(
            result,
            Err(LaserError::Timeout("the Iggy login reply"))
        ));
        assert!(started.elapsed() < Duration::from_secs(5));
        server.abort();
    }

    #[tokio::test]
    async fn given_a_zero_connect_timeout_when_building_then_should_reject_the_configuration() {
        let result = Laser::builder()
            .connection_string("iggy:iggy@127.0.0.1:8090")
            .connect_timeout(Duration::ZERO)
            .build()
            .await;
        assert!(matches!(result, Err(LaserError::Config(_))));
    }

    #[test]
    fn given_a_capability_set_when_it_ages_then_should_reprobe_only_while_a_backend_is_unready() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        assert!(laser.reprobe_due(), "a set never probed is due");
        let backend = |ready: bool| {
            let (observed, readiness) = if ready {
                (BackendObservedState::Ready, BackendReadiness::ready(1))
            } else {
                (
                    BackendObservedState::Starting,
                    BackendReadiness::not_ready(BackendReadinessCode::ConfigurationPending),
                )
            };
            BackendDescriptor::new(
                BackendResourceId::from_u128(7),
                BackendMode::Operational,
                "Doris",
                BackendImplementation {
                    kind: "velodb".to_owned(),
                    version: "1.0.0".to_owned(),
                },
                1,
                1,
            )
            .with_state(BackendDesiredState::Enabled, observed, readiness)
        };
        let set_probe =
            |at: tokio::time::Instant, managed: bool, backends: Vec<BackendDescriptor>| {
                let mut negotiated = laser.inner.negotiated.write().expect("not poisoned");
                negotiated.probed_at = Some(at);
                negotiated.capabilities.managed = managed;
                negotiated.capabilities.backends = backends;
            };
        let now = tokio::time::Instant::now();
        set_probe(now, false, Vec::new());
        assert!(!laser.reprobe_due(), "a fresh unmanaged set is trusted");
        let aged = now
            .checked_sub(super::REPROBE_INTERVAL)
            .expect("the clock is past one second");
        set_probe(aged, false, Vec::new());
        assert!(laser.reprobe_due(), "an aged unmanaged set is probed again");
        set_probe(now, true, vec![backend(false)]);
        assert!(
            !laser.reprobe_due(),
            "a fresh set with a starting backend is trusted"
        );
        set_probe(aged, true, vec![backend(false)]);
        assert!(
            laser.reprobe_due(),
            "an aged set with a starting backend is probed again"
        );
        set_probe(aged, true, vec![backend(true)]);
        assert!(
            !laser.reprobe_due(),
            "a set whose backends are ready is never probed again"
        );
        set_probe(aged, true, Vec::new());
        assert!(
            !laser.reprobe_due(),
            "a managed set without backends has nothing to wait for"
        );
    }

    #[tokio::test]
    async fn given_a_capability_override_when_read_then_should_not_probe() {
        let mut capabilities = crate::capabilities::Capabilities::OPEN;
        capabilities.graph = true;
        let laser = Laser::from_client(iggy::prelude::IggyClient::default())
            .with_capabilities(capabilities.clone());
        assert_eq!(laser.capabilities().await, capabilities);
    }

    #[cfg(feature = "kv")]
    #[tokio::test]
    async fn given_a_closed_laser_when_acquiring_its_first_lease_then_should_not_open_a_connection()
    {
        let mut laser = Laser::from_client(iggy::prelude::IggyClient::default());
        std::sync::Arc::get_mut(&mut laser.inner)
            .expect("the Laser is not shared yet")
            .connection_string = Some("not a connection string".to_owned());
        let mut capabilities = crate::capabilities::Capabilities::OPEN;
        capabilities.kv.fenced_leases = true;
        let laser = laser.with_capabilities(capabilities);
        laser.close().await.expect("close the unconnected client");
        let result = laser
            .kv("leases")
            .lease(b"key", "holder", Duration::from_secs(1))
            .await;
        assert!(matches!(
            result,
            Err(LaserError::Transport(TransportError(
                iggy::prelude::IggyError::ClientShutdown
            )))
        ));
    }
}

#[cfg(test)]
mod connection_string_tests {
    use super::{
        ConnectionConfig, LaserError, PROD_CERT, flag_value_enabled, has_query_param, host_of,
        install_cert, is_laserdata_host, normalize_connection_string, resolve_tls,
        resolve_tls_with, tcp_client_config,
    };
    use iggy::prelude::{AutoLogin, Credentials, TcpClientConfig};
    use iggy_common::login_user::LoginUser;
    use iggy_common::login_with_personal_access_token::LoginWithPersonalAccessToken;

    #[test]
    fn given_a_password_with_a_colon_and_an_at_sign_when_given_as_credentials_then_should_log_in_with_it_exactly()
     {
        let connection = ConnectionConfig::Tcp {
            address: "127.0.0.1".to_owned(),
            username: "agent:one".to_owned(),
            password: "pa:ss@w?rd%/#".to_owned(),
        };
        let normalized = connection
            .connection_string()
            .expect("any credential characters are accepted");
        let config = tcp_client_config(&normalized).expect("a TCP client config");
        assert_eq!(config.server_address, "127.0.0.1:8090");
        assert_eq!(
            login_of(&config),
            Login::Password("agent:one".to_owned(), "pa:ss@w?rd%/#".to_owned())
        );
        let redialed = tcp_client_config(
            &normalize_connection_string(&normalized).expect("the stored string normalizes again"),
        )
        .expect("a second connection's config");
        assert_eq!(login_of(&redialed), login_of(&config));
    }

    #[test]
    fn given_percent_encoded_credentials_when_connecting_by_string_then_should_log_in_with_the_decoded_secret()
     {
        let normalized =
            normalize_connection_string("iggy+tcp://ag%40ent:pa%3Ass%40w%3Frd%25@host:8090")
                .expect("percent-encoded credentials normalize");
        let config = tcp_client_config(&normalized).expect("a TCP client config");
        assert_eq!(
            login_of(&config),
            Login::Password("ag@ent".to_owned(), "pa:ss@w?rd%".to_owned())
        );
        let token = normalize_connection_string("tok%3Aen%40one@host")
            .and_then(|normalized| tcp_client_config(&normalized))
            .expect("a percent-encoded token normalizes");
        assert_eq!(login_of(&token), Login::Token("tok:en@one".to_owned()));
    }

    #[test]
    fn given_a_malformed_percent_escape_when_normalized_then_should_fail_as_config() {
        for value in [
            "user:pa%zz@host",
            "user:pa%4@host",
            "user:pa%@host",
            "user:%FF%FE@host",
        ] {
            let result = normalize_connection_string(value);
            assert!(
                matches!(result, Err(LaserError::Config(_))),
                "{value} must be refused, got {result:?}"
            );
        }
    }

    #[test]
    fn given_a_tls_connection_string_when_normalized_then_should_verify_the_given_host_on_every_node()
     {
        assert_eq!(
            normalize_connection_string(
                "iggy+tcp://user:pass@edge.example.com:8090?tls=true&tls_ca_file=/ca.pem"
            )
            .expect("a TLS connection string normalizes"),
            "iggy+tcp://user:pass@edge.example.com:8090?tls=true&tls_ca_file=/ca.pem&tls_domain=edge.example.com",
        );
        assert_eq!(
            normalize_connection_string(
                "iggy+tcp://user:pass@10.0.0.7:8090?tls=true&tls_ca_file=/ca.pem&tls_domain=edge.example.com"
            )
            .expect("an explicit TLS domain normalizes"),
            "iggy+tcp://user:pass@10.0.0.7:8090?tls=true&tls_ca_file=/ca.pem&tls_domain=edge.example.com",
            "an explicit domain is kept"
        );
    }

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
    fn given_an_address_without_a_port_when_normalized_then_should_default_to_8090() {
        assert_eq!(
            normalize_connection_string("iggy:iggy@localhost").expect("a portless address"),
            "iggy://iggy:iggy@localhost:8090",
        );
        assert_eq!(
            normalize_connection_string("iggy+tcp://token@127.0.0.1?nodelay=true")
                .expect("a portless token address"),
            "iggy+tcp://token@127.0.0.1:8090?nodelay=true",
        );
    }

    #[test]
    fn given_an_invalid_port_when_normalized_then_should_fail_as_config() {
        for value in [
            "iggy:iggy@host:0",
            "iggy:iggy@host:65536",
            "iggy:iggy@host:",
            "iggy:iggy@host:+1",
            "iggy:iggy@host:80:81",
            "iggy:iggy@host:8090/path",
            "iggy:iggy@:8090",
        ] {
            assert!(
                matches!(
                    normalize_connection_string(value),
                    Err(LaserError::Config(_))
                ),
                "{value} must be refused"
            );
        }
    }

    #[test]
    fn given_a_bracketed_ipv6_address_when_normalized_then_should_keep_it_and_default_its_port() {
        assert_eq!(
            normalize_connection_string("iggy:iggy@[::1]:8090").expect("a bracketed IPv6 address"),
            "iggy://iggy:iggy@[::1]:8090",
        );
        assert_eq!(
            normalize_connection_string("iggy+tcp://token@[2001:db8::1]?nodelay=true")
                .expect("a portless IPv6 address"),
            "iggy+tcp://token@[2001:db8::1]:8090?nodelay=true",
        );
        let config = tcp_client_config(
            &normalize_connection_string("iggy:iggy@[::ffff:192.0.2.1]:9090")
                .expect("a mapped IPv6 address"),
        )
        .expect("a TCP client config");
        assert_eq!(config.server_address, "[::ffff:192.0.2.1]:9090");
    }

    #[test]
    fn given_a_malformed_ipv6_address_when_normalized_then_should_fail_as_config() {
        for value in [
            "iggy:iggy@[::1",
            "iggy:iggy@[::1]x:8090",
            "iggy:iggy@[::1]:",
            "iggy:iggy@[::1]:0",
            "iggy:iggy@[::1]:65536",
            "iggy:iggy@[]:8090",
            "iggy:iggy@[localhost]:8090",
            "iggy:iggy@[gggg::1]:8090",
            "iggy:iggy@::1:8090",
            "iggy:iggy@2001:db8::1:8090",
            "iggy:iggy@[::1]:8090/path",
        ] {
            assert!(
                matches!(
                    normalize_connection_string(value),
                    Err(LaserError::Config(_))
                ),
                "{value} must be refused"
            );
        }
    }

    #[test]
    fn given_another_transport_scheme_when_normalized_then_should_fail_as_config() {
        for value in [
            "iggy+quic://iggy:iggy@host:8080",
            "iggy+ws://iggy:iggy@host:8092",
            "iggy+http://iggy:iggy@host:3000",
            "tcp://iggy:iggy@host:8090",
        ] {
            assert!(
                matches!(
                    normalize_connection_string(value),
                    Err(LaserError::Config(_))
                ),
                "{value} must be refused"
            );
        }
    }

    #[test]
    fn given_missing_or_malformed_credentials_when_normalized_then_should_fail_as_config() {
        for value in [
            "127.0.0.1:8090",
            "iggy://127.0.0.1:8090",
            "@host:8090",
            ":pass@host:8090",
            "user:@host:8090",
            "a:b:c@host:8090",
            "user:p@ss@host:8090",
        ] {
            assert!(
                matches!(
                    normalize_connection_string(value),
                    Err(LaserError::Config(_))
                ),
                "{value} must be refused"
            );
        }
    }

    #[test]
    fn given_unknown_repeated_or_malformed_options_when_normalized_then_should_fail_as_config() {
        for value in [
            "iggy:iggy@host:8090?foo=bar",
            "iggy:iggy@host:8090?TLS=true",
            "iggy:iggy@host:8090?tls=true&tls=false",
            "iggy:iggy@host:8090?tls",
            "iggy:iggy@host:8090?",
            "iggy:iggy@host:8090?tls=yes",
            "iggy:iggy@host:8090?nodelay=1",
            "iggy:iggy@host:8090?tls=false&tls_ca_file=/ca.crt",
        ] {
            assert!(
                matches!(
                    normalize_connection_string(value),
                    Err(LaserError::Config(_))
                ),
                "{value} must be refused"
            );
        }
    }

    #[test]
    fn given_a_ca_file_without_tls_when_normalized_then_should_turn_tls_on() {
        assert_eq!(
            normalize_connection_string("iggy:iggy@host:8090?tls_ca_file=/ca.crt")
                .expect("a CA file alone"),
            "iggy://iggy:iggy@host:8090?tls_ca_file=/ca.crt&tls=true&tls_domain=host",
        );
        assert_eq!(
            normalize_connection_string("iggy:iggy@host?tls=true&tls_ca_file=/ca.crt")
                .expect("an explicit TLS flag"),
            "iggy://iggy:iggy@host:8090?tls=true&tls_ca_file=/ca.crt&tls_domain=host",
        );
    }

    #[test]
    fn given_an_explicit_tls_false_when_resolving_tls_then_should_skip_automatic_tls() {
        assert_eq!(
            resolve_tls_with(
                "iggy://u:p@h.laserdata.cloud:8090?tls=false".to_owned(),
                false,
                Some("/tmp/local-ca.crt".into()),
            )
            .expect("an explicit opt-out"),
            "iggy://u:p@h.laserdata.cloud:8090?tls=false",
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
    fn given_a_custom_ca_when_resolving_a_local_host_then_should_enable_tls() {
        let resolved = resolve_tls_with(
            "iggy+tcp://u:p@demo.localhost:8090".to_owned(),
            false,
            Some("/tmp/local-ca.crt".into()),
        )
        .expect("local TLS resolution should succeed");
        assert_eq!(
            resolved,
            "iggy+tcp://u:p@demo.localhost:8090?tls=true&tls_ca_file=/tmp/local-ca.crt"
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

    #[derive(Debug, PartialEq)]
    enum Login {
        Password(String, String),
        Token(String),
    }

    // A secret has no accessor outside `secrecy`, so it is read the way the
    // HTTP login serializes it.
    fn login_of(config: &TcpClientConfig) -> Login {
        let secret = |value: serde_json::Value, field: &str| {
            value[field]
                .as_str()
                .expect("the login serializes its secret")
                .to_owned()
        };
        match &config.auto_login {
            AutoLogin::Enabled(Credentials::UsernamePassword(username, password)) => {
                let login = LoginUser {
                    username: username.clone(),
                    password: password.clone(),
                    version: None,
                    context: None,
                };
                Login::Password(
                    username.clone(),
                    secret(
                        serde_json::to_value(login).expect("the login serializes"),
                        "password",
                    ),
                )
            }
            AutoLogin::Enabled(Credentials::PersonalAccessToken(token)) => {
                let login = LoginWithPersonalAccessToken {
                    token: token.clone(),
                };
                Login::Token(secret(
                    serde_json::to_value(login).expect("the login serializes"),
                    "token",
                ))
            }
            AutoLogin::Disabled => panic!("the client must log in by itself"),
        }
    }
}

#[cfg(test)]
mod resource_naming_tests {
    use super::{Laser, ResourceNaming};

    fn laser() -> Laser {
        Laser::from_client(iggy::prelude::IggyClient::default())
    }

    #[test]
    fn given_a_borrowed_laser_when_scoping_with_every_with_method_then_should_leave_it_untouched() {
        let original = laser();
        let borrowed = &original;
        let scoped = borrowed
            .with_capabilities(crate::capabilities::Capabilities::OPEN.with_managed(true))
            .with_ops_stream("acme-ops")
            .with_control_topic("acme-control")
            .with_dlq_topic("acme-dead")
            .with_changes_topic("acme-changes")
            .with_default_stream("acme")
            .with_resource_naming(ResourceNaming::Bare);
        assert_eq!(
            [
                scoped.ops_stream(),
                scoped.control_topic(),
                scoped.dlq_topic(),
                scoped.changes_topic()
            ],
            ["acme-ops", "acme-control", "acme-dead", "acme-changes"]
        );
        assert!(scoped.current_capabilities().managed);
        assert_eq!(original.ops_stream(), laser().ops_stream());
        assert!(!original.current_capabilities().managed);
    }

    #[test]
    fn given_a_default_stream_when_naming_a_resource_then_should_scope_it_to_the_stream() {
        let laser = laser().with_default_stream("acme");
        assert_eq!(laser.resource_naming(), ResourceNaming::Stream);
        assert_eq!(laser.resource_name("agent.keys"), "stream:acme/agent.keys");
        assert_eq!(laser.resource_stream(), Some("acme"));
    }

    #[test]
    fn given_a_name_already_scoped_to_the_stream_when_naming_it_then_should_send_it_as_is() {
        let laser = laser().with_default_stream("acme");
        assert_eq!(
            laser.resource_name("stream:acme/sessions"),
            "stream:acme/sessions"
        );
    }

    #[test]
    fn given_a_name_scoped_to_another_stream_when_naming_it_then_should_stay_inside_the_handle_stream()
     {
        let laser = laser().with_default_stream("acme");
        assert_eq!(
            laser.resource_name("stream:other/sessions"),
            "stream:acme/stream:other/sessions"
        );
        assert_eq!(
            laser.local_resource_name(&laser.resource_name("stream:other/sessions")),
            Some("stream:other/sessions")
        );
        assert_eq!(
            laser
                .with_resource_naming(ResourceNaming::Bare)
                .resource_name("stream:other/sessions"),
            "stream:other/sessions"
        );
        assert_eq!(
            laser
                .with_default_stream("other")
                .resource_name("stream:other/sessions"),
            "stream:other/sessions"
        );
    }

    #[cfg(feature = "kv")]
    #[tokio::test]
    async fn given_a_default_stream_with_a_slash_when_a_managed_call_runs_then_should_refuse_it() {
        let mut capabilities = crate::capabilities::Capabilities::OPEN;
        capabilities.managed = true;
        capabilities.kv.available = true;
        let laser = laser()
            .with_capabilities(capabilities)
            .with_default_stream("tenants/acme");
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            laser.kv("profiles").get("user-1"),
        )
        .await
        .expect("the refusal happens before any connection attempt");
        assert!(
            matches!(&read, Err(crate::error::LaserError::Invalid(message)) if message.contains('/')),
            "{read:?}"
        );
        let bare = laser.with_resource_naming(ResourceNaming::Bare);
        assert_eq!(bare.resource_name("profiles"), "profiles");
    }

    #[cfg(feature = "kv")]
    #[tokio::test]
    async fn given_a_default_stream_with_a_slash_when_a_lease_is_acquired_then_should_refuse_it() {
        let mut capabilities = crate::capabilities::Capabilities::OPEN;
        capabilities.managed = true;
        capabilities.kv.available = true;
        capabilities.kv.fenced_leases = true;
        let laser = laser()
            .with_capabilities(capabilities)
            .with_default_stream("tenants/acme");
        let lease = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            laser
                .kv("locks")
                .lease("job-1", "worker-1", std::time::Duration::from_secs(5)),
        )
        .await
        .expect("the refusal happens before any connection attempt");
        assert!(
            matches!(&lease, Err(crate::error::LaserError::Invalid(message)) if message.contains('/')),
            "{lease:?}"
        );
    }

    #[test]
    fn given_bare_naming_when_naming_a_resource_then_should_send_the_name_unchanged() {
        let laser = laser()
            .with_default_stream("acme")
            .with_resource_naming(ResourceNaming::Bare);
        assert_eq!(laser.resource_name("agent.keys"), "agent.keys");
        assert_eq!(laser.resource_stream(), None);
        assert_eq!(
            laser.local_resource_name("stream:acme/x"),
            Some("stream:acme/x")
        );
    }

    #[test]
    fn given_no_default_stream_when_naming_a_resource_then_should_send_the_name_unchanged() {
        let laser = laser();
        assert_eq!(laser.resource_name("agent.keys"), "agent.keys");
        assert_eq!(laser.local_resource_name("agent.keys"), Some("agent.keys"));
    }

    #[test]
    fn given_an_empty_name_when_naming_it_then_should_stay_empty_for_validation() {
        assert_eq!(laser().with_default_stream("acme").resource_name(""), "");
    }

    #[test]
    fn given_listed_names_when_localized_then_should_strip_the_own_prefix_and_drop_the_rest() {
        let laser = laser().with_default_stream("acme");
        assert_eq!(
            laser.local_resource_name("stream:acme/sessions"),
            Some("sessions")
        );
        assert_eq!(laser.local_resource_name("stream:other/sessions"), None);
        assert_eq!(laser.local_resource_name("sessions"), None);
    }

    #[test]
    fn given_an_explicit_stream_when_naming_in_it_then_should_scope_to_that_stream() {
        let laser = laser().with_default_stream("acme");
        assert_eq!(
            laser.resource_name_in(Some("fleet"), "memory"),
            "stream:fleet/memory"
        );
        assert_eq!(laser.resource_name_in(None, "memory"), "memory");
    }
}

#[cfg(test)]
mod managed_mutation_tests {
    use super::unknown_outcome;
    use laser_wire::framing::encode_named;
    use laser_wire::result::{CommandError, ResultCode};

    fn reply(code: ResultCode, message: &str) -> Vec<u8> {
        encode_named(&CommandError::new(code, message)).expect("encodes")
    }

    #[test]
    fn given_pending_or_unavailable_replies_when_classified_then_should_repeat_the_same_identity() {
        for (code, message) in [
            (ResultCode::Pending, "not folded yet"),
            (ResultCode::BackendUnavailable, "backend down"),
            (ResultCode::Unavailable, "busy"),
        ] {
            let unknown = unknown_outcome(&reply(code, message)).expect("outcome unknown");
            assert_eq!(unknown.code, code);
            assert_eq!(unknown.message, message);
        }
    }

    #[test]
    fn given_final_replies_when_classified_then_should_return_them() {
        assert_eq!(unknown_outcome(&reply(ResultCode::Conflict, "taken")), None);
        assert_eq!(unknown_outcome(&reply(ResultCode::Forbidden, "no")), None);
        assert_eq!(unknown_outcome(&[0xa1, 0x62, 0x6f, 0x6b, 0xf6]), None);
    }
}

#[cfg(test)]
mod positioned_reply_tests {
    use super::Laser;
    use crate::nodes::Answered;
    use bytes::Bytes;
    use laser_wire::codes::{AGDX_KV_GET_CODE, AGDX_SESSION_GET_CODE};
    use laser_wire::framing::{decode_named, encode_named};
    use laser_wire::mutation::{
        AppliedFrontier, MANAGED_REQUEST_VERSION, ManagedReadEnvelope, ManagedReplyEnvelope,
        MutationPosition, SessionPosition,
    };
    use laser_wire::result::{CommandError, ResultCode};

    fn at(offset: u64) -> MutationPosition {
        MutationPosition {
            topic_generation: 1,
            partition: 0,
            offset,
        }
    }

    fn positioned(kv: u64, reply: &[u8]) -> Vec<u8> {
        encode_named(&ManagedReplyEnvelope {
            v: MANAGED_REQUEST_VERSION,
            frontier: AppliedFrontier {
                kv: Some(at(kv)),
                ..AppliedFrontier::default()
            },
            reply: reply.to_vec(),
        })
        .expect("encodes")
    }

    #[test]
    fn given_positioned_replies_when_taken_then_reads_should_carry_the_highest_position_seen() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let first: ManagedReadEnvelope =
            decode_named(&laser.read_frame(AGDX_KV_GET_CODE, b"get").unwrap()).unwrap();
        assert_eq!(first.read, b"get");
        assert_eq!(first.min_position, None, "nothing seen yet");
        assert!(!first.log_head);

        assert_eq!(
            laser.take_positioned(&Answered::open(
                Bytes::from(positioned(9, b"written")),
                None
            )),
            b"written"
        );
        assert_eq!(
            laser.take_positioned(&Answered::open(Bytes::from(positioned(4, b"older")), None)),
            b"older"
        );
        assert_eq!(laser.seen_positions().kv, Some(at(9)));
        let next: ManagedReadEnvelope =
            decode_named(&laser.read_frame(AGDX_KV_GET_CODE, b"get").unwrap()).unwrap();
        assert_eq!(next.min_position, Some(at(9)));
        assert!(!next.log_head);
    }

    #[cfg(feature = "kv")]
    #[test]
    fn given_a_grant_position_from_the_coordination_connection_when_observed_then_should_raise_the_kv_frontier()
     {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let granted = laser_wire::mutation::MutationPosition {
            topic_generation: 1,
            partition: 0,
            offset: 9,
        };
        laser.observe_kv_position(granted);
        assert_eq!(laser.seen_positions().kv, Some(granted));
        laser.observe_kv_position(laser_wire::mutation::MutationPosition {
            offset: 3,
            ..granted
        });
        assert_eq!(
            laser.seen_positions().kv,
            Some(granted),
            "an older position never lowers the frontier"
        );
    }

    #[test]
    fn given_frontiers_when_merged_then_should_keep_the_later_position_per_surface() {
        let at = |topic_generation, offset| MutationPosition {
            topic_generation,
            partition: 0,
            offset,
        };
        let mut seen = AppliedFrontier {
            kv: Some(at(1, 9)),
            fork: Some(at(1, 4)),
            graph: None,
            sessions: Vec::new(),
        };
        super::merge_frontier(
            &mut seen,
            &AppliedFrontier {
                kv: Some(at(1, 3)),
                fork: Some(at(2, 0)),
                graph: Some(at(1, 1)),
                sessions: Vec::new(),
            },
        );
        assert_eq!(seen.kv, Some(at(1, 9)), "an older answer never lowers it");
        assert_eq!(seen.fork, Some(at(2, 0)), "a recreated topic is later");
        assert_eq!(seen.graph, Some(at(1, 1)));
    }

    #[test]
    fn given_session_positions_when_merged_then_session_reads_should_carry_them() {
        let session = |stream_generation, topic: &str, offset| SessionPosition {
            stream_id: 4,
            stream_generation,
            topic: topic.to_owned(),
            topic_generation: 1,
            partition_id: 0,
            offset,
        };
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        let opened = |sessions| {
            Answered::open(
                Bytes::from(
                    encode_named(&ManagedReplyEnvelope {
                        v: MANAGED_REQUEST_VERSION,
                        frontier: AppliedFrontier {
                            sessions,
                            ..AppliedFrontier::default()
                        },
                        reply: b"page".to_vec(),
                    })
                    .expect("encodes"),
                ),
                None,
            )
        };
        laser.take_positioned(&opened(vec![session(1, "lane", 5), session(1, "inbox", 2)]));
        laser.take_positioned(&opened(vec![session(1, "lane", 3)]));
        assert_eq!(
            laser.seen_positions().sessions,
            vec![session(1, "lane", 5), session(1, "inbox", 2)],
            "an older answer never lowers a unit"
        );
        let read: ManagedReadEnvelope =
            decode_named(&laser.read_frame(AGDX_SESSION_GET_CODE, b"session").unwrap()).unwrap();
        assert_eq!(read.read, b"session");
        assert_eq!(read.min_position, None);
        assert_eq!(read.min_sessions.len(), 2);

        laser.take_positioned(&opened(vec![session(2, "lane", 0)]));
        assert_eq!(
            laser.seen_positions().sessions,
            vec![session(2, "lane", 0)],
            "a recreated stream drops the positions of the one before it"
        );
    }

    #[test]
    fn given_unpositioned_traffic_when_framed_then_should_pass_it_unwrapped() {
        let laser = Laser::from_client(iggy::prelude::IggyClient::default());
        assert_eq!(
            laser
                .read_frame(laser_wire::codes::AGDX_QUERY_CODE, b"query")
                .unwrap(),
            b"query",
            "a read outside the folded surfaces is sent as it is"
        );
        let refusal = encode_named(&CommandError::new(ResultCode::BackendUnavailable, "down"))
            .expect("encodes");
        assert_eq!(
            laser.take_positioned(&Answered::open(Bytes::from(refusal.clone()), None)),
            refusal
        );
        assert_eq!(laser.seen_positions(), AppliedFrontier::default());
    }
}

#[cfg(all(test, feature = "agent", feature = "query"))]
mod presence_pin_tests {
    use super::Laser;
    use crate::error::LaserError;
    use laser_wire::agent::AgentPresence;
    use laser_wire::result::ResultCode;

    fn presence() -> AgentPresence {
        AgentPresence {
            v: 1,
            agent: "risk".parse().expect("risk is a valid agent id"),
            inbox: Some("risk.inbox".to_owned()),
        }
    }

    #[tokio::test]
    async fn given_a_pinned_presence_replica_that_does_not_answer_when_clearing_or_advertising_then_should_report_unavailable_and_keep_the_pin()
     {
        let laser = Laser::from_client(crate::iggy::clients::client::IggyClient::default());
        *laser.presence_pin().lock().await = Some(1);
        laser
            .claim_presence("risk".parse().expect("risk is a valid agent id"))
            .expect("the slot is free");

        let cleared = laser.clear_presence().await;
        let LaserError::Command(error) = cleared.expect_err("the pinned replica does not answer")
        else {
            panic!("an unreachable pinned replica is reported as a command error");
        };
        assert_eq!(error.code, ResultCode::Unavailable);
        assert!(
            error
                .message
                .contains("replica 1 holds this connection's presence")
        );
        assert_eq!(
            *laser.presence_pin().lock().await,
            Some(1),
            "the pin is kept until the replica answers"
        );
        assert!(
            laser
                .claim_presence("audit".parse().expect("audit is a valid agent id"))
                .is_err(),
            "the presence is still advertised"
        );

        let advertised = laser.advertise_presence(&presence()).await;
        assert!(
            matches!(advertised, Err(LaserError::Command(ref error)) if error.code == ResultCode::Unavailable),
            "a new advertise never goes to another replica: {advertised:?}"
        );
        assert_eq!(*laser.presence_pin().lock().await, Some(1));
    }
}

#[cfg(test)]
mod mutation_attempt_tests {
    use super::{Attempt, MutationAttempts};
    use crate::error::LaserError;
    use laser_wire::batch::BatchReply;
    use laser_wire::codes::{AGDX_BATCH_CODE, AGDX_KV_SET_CODE};
    use laser_wire::framing::encode_named;
    use laser_wire::result::{CommandError, ResultCode};

    fn reply(code: ResultCode) -> Vec<u8> {
        encode_named(&CommandError::new(code, "reply")).expect("encoded")
    }

    fn batch(slots: Vec<Vec<u8>>) -> Vec<u8> {
        encode_named(&BatchReply { results: slots }).expect("encoded batch")
    }

    #[test]
    fn given_a_batch_with_an_unresolved_item_when_settled_then_should_repeat_the_frame_until_every_item_is_final()
     {
        let mut attempts = MutationAttempts::for_code(AGDX_BATCH_CODE);
        let first = batch(vec![reply(ResultCode::Ok), reply(ResultCode::Pending)]);
        assert!(
            matches!(attempts.settle(Attempt::Answered(first)), Ok(None)),
            "a pending item leaves the frame unresolved"
        );
        let unavailable = batch(vec![
            reply(ResultCode::Ok),
            reply(ResultCode::BackendUnavailable),
        ]);
        assert!(matches!(
            attempts.settle(Attempt::Answered(unavailable)),
            Ok(None)
        ));
        let settled = batch(vec![reply(ResultCode::Ok), reply(ResultCode::TooLarge)]);
        assert_eq!(
            attempts
                .settle(Attempt::Answered(settled.clone()))
                .expect("every item final"),
            Some(settled),
            "a frame whose items are all final settles, an item-level refusal included"
        );
    }

    #[test]
    fn given_a_batch_item_left_unresolved_when_a_later_attempt_refuses_it_by_admission_then_should_stay_ambiguous()
     {
        let mut attempts = MutationAttempts::for_code(AGDX_BATCH_CODE);
        attempts
            .settle(Attempt::Answered(batch(vec![
                reply(ResultCode::Ok),
                reply(ResultCode::Pending),
            ])))
            .expect("unresolved");
        let settled = attempts.settle(Attempt::Answered(batch(vec![
            reply(ResultCode::Ok),
            reply(ResultCode::Forbidden),
        ])));
        assert!(
            matches!(settled, Err(LaserError::AmbiguousMutation { ref message, .. }) if message.contains("item 1")),
            "the refused item may still apply: {settled:?}"
        );
    }

    #[test]
    fn given_a_lost_batch_attempt_when_the_repeat_refuses_an_item_by_admission_then_should_stay_ambiguous()
     {
        let mut attempts = MutationAttempts::for_code(AGDX_BATCH_CODE);
        attempts
            .settle(Attempt::Lost("connection reset".to_owned()))
            .expect("lost");
        let settled = attempts.settle(Attempt::Answered(batch(vec![
            reply(ResultCode::Unauthenticated),
            reply(ResultCode::Ok),
        ])));
        assert!(matches!(settled, Err(LaserError::AmbiguousMutation { .. })));
    }

    #[test]
    fn given_a_batch_whose_items_are_final_on_the_first_attempt_when_settled_then_should_settle_with_it()
     {
        let mut attempts = MutationAttempts::for_code(AGDX_BATCH_CODE);
        let settled = batch(vec![reply(ResultCode::Forbidden), reply(ResultCode::Ok)]);
        assert_eq!(
            attempts
                .settle(Attempt::Answered(settled.clone()))
                .expect("a definite refusal of one item is final"),
            Some(settled)
        );
        let mut single = MutationAttempts::for_code(AGDX_KV_SET_CODE);
        let pending_batch = batch(vec![reply(ResultCode::Pending)]);
        assert_eq!(
            single
                .settle(Attempt::Answered(pending_batch.clone()))
                .expect("not a batch code"),
            Some(pending_batch),
            "only the batch code settles item by item"
        );
    }

    #[test]
    fn given_a_pending_attempt_when_a_later_one_is_forbidden_then_should_stay_ambiguous() {
        let mut attempts = MutationAttempts::default();
        assert!(matches!(
            attempts.settle(Attempt::Answered(reply(ResultCode::Pending))),
            Ok(None)
        ));

        let settled = attempts.settle(Attempt::Answered(reply(ResultCode::Forbidden)));

        assert!(matches!(settled, Err(LaserError::AmbiguousMutation { .. })));
    }

    #[test]
    fn given_a_lost_attempt_when_a_later_one_is_refused_by_the_transport_then_should_stay_ambiguous()
     {
        let mut attempts = MutationAttempts::default();
        assert!(matches!(
            attempts.settle(Attempt::Lost("connection reset".to_owned())),
            Ok(None)
        ));

        let settled = attempts.settle(Attempt::Refused(LaserError::Invalid("refused".to_owned())));

        assert!(matches!(settled, Err(LaserError::AmbiguousMutation { .. })));
    }

    #[test]
    fn given_a_first_attempt_when_it_is_forbidden_or_answered_then_should_settle_with_it() {
        let forbidden = reply(ResultCode::Forbidden);
        assert_eq!(
            MutationAttempts::default()
                .settle(Attempt::Answered(forbidden.clone()))
                .expect("a definite refusal"),
            Some(forbidden)
        );
        let mut attempts = MutationAttempts::default();
        attempts
            .settle(Attempt::Answered(reply(ResultCode::Pending)))
            .expect("pending");
        let applied = reply(ResultCode::Ok);
        assert_eq!(
            attempts
                .settle(Attempt::Answered(applied.clone()))
                .expect("a durable outcome resolves it"),
            Some(applied)
        );
    }
}

use crate::agent::clock::{Clock, SystemClock};
use crate::agent::ctx::AgentCtx;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::{AgentTopic, Provenance};
use crate::types::{AgentId, ConsumerGroupName, ConversationId, MessageId};
use async_trait::async_trait;
use iggy::consumer_ext::MessageConsumer;
use iggy::prelude::*;
use laser_wire::agent::{
    AgentDeadLetter, AgentEnvelope, AgentKind, DeadLetterReason, LogPosition, OPERATION_TASK,
    SignatureContext, TaskState, features, validate,
};
use laser_wire::codes::AGENT_OP_VERSION;
use laser_wire::content::ContentType;
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::headers::{AGENT_VERSION, CONTENT_TYPE, FENCE};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tracing::{debug, error, warn};

// Capped exponential backoff between consecutive poll failures: 50ms, 100ms,
// 200ms, up to one second.
fn backoff_for(attempt: u32) -> Duration {
    const BASE_MILLIS: u64 = 50;
    const CEILING_MILLIS: u64 = 1000;
    let scaled = BASE_MILLIS.saturating_mul(2u64.saturating_pow(attempt.saturating_sub(1).min(16)));
    Duration::from_millis(scaled.min(CEILING_MILLIS))
}

/// The composed-dedup and fence-map tuning constants, grouped near the top so
/// the consume path below reads without stopping at a definition.
/// Separator between the principal and the idempotency key in the composed dedup
/// key (ASCII unit separator, which cannot appear in an agent id).
const DEDUP_SCOPE_SEP: char = '\u{1f}';

/// The most fence high-water entries kept before an idle-eviction sweep is
/// considered, so a long-lived consumer's per-task fence map stays bounded by the
/// recently-active working set rather than every task ever seen.
const FENCE_MAP_SOFT_CAP: usize = 16_384;

/// How many verified record ids a consumer remembers, to refuse a replay of the
/// exact signed bytes. Sized like the fence map: bounded by the recently-active
/// working set, not by every record ever seen.
#[cfg(feature = "sign")]
const VERIFIED_RECORD_WINDOW: usize = 16_384;

/// A fence entry untouched for this long is swept once the map is over its soft
/// cap. The gate is kept for any task active within the window. Only tasks long
/// idle (where a stale-holder replay is no longer plausible, and dedup is the
/// backstop) are dropped.
const FENCE_ENTRY_TTL_MICROS: u64 = 600_000_000;

/// The least time between idle-eviction sweeps, so the O(n) `retain` runs at most
/// this often under load instead of on every accepted fence.
const FENCE_SWEEP_INTERVAL_MICROS: u64 = 1_000_000;

// Iggy defaults to a one-second poll, tuned for throughput. An agent runtime is
// latency-bound, so each hop would wait up to a second. Override per agent with
// `Agent::builder().poll_interval(..)`.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

// How long a graceful shutdown waits for the in-flight message (its handler and
// any retry backoff) to finish before dropping the consumer. `abort` is the
// unconditional hard stop, this bounds the polite one.
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
const DEFAULT_MAX_QUEUED_RECORDS: usize = 4_096;
const DEFAULT_MAX_QUEUED_BYTES: usize = 64 * 1024 * 1024;

/// What you implement: one async `handle` per message. (`AgentHandler` is the `Send` variant the runtime drives.)
#[trait_variant::make(AgentHandler: Send)]
pub trait LocalAgentHandler {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError>;
}

/// A cross-cutting hook wrapped around every handler dispatch, for auth, metrics,
/// and tracing without a handler rewrite. `before_handle` runs once before the
/// retry loop and may reject the message (a rejection dead-letters it without
/// running the handler). `after_handle` runs after every attempt with its result
/// and one-based attempt number, so a metrics sink counts retries and outcomes.
/// Boxed-future (`#[async_trait]`) so it composes as `Arc<dyn AgentMiddleware>`,
/// the same seam shape as [`Deduplicator`].
#[async_trait]
pub trait AgentMiddleware: Send + Sync {
    async fn before_handle(&self, message: &AgentMessage) -> Result<(), LaserError> {
        let _ = message;
        Ok(())
    }

    async fn after_handle(
        &self,
        message: &AgentMessage,
        result: &Result<(), LaserError>,
        attempt: u32,
    ) {
        let _ = (message, result, attempt);
    }
}

/// Notified whenever the consumer produces a dead-letter capsule, with the
/// capsule and the result of publishing it to the DLQ topic. A publish failure
/// means the poison message is lost as its offset commits, so this is the seam an
/// operator wires to alert on a lost message rather than grep logs. The `message`
/// is present for a decoded poison message, absent when the provenance itself
/// would not decode. Boxed-future (`#[async_trait]`) so it composes as
/// `Arc<dyn DeadLetterSink>`.
#[async_trait]
pub trait DeadLetterSink: Send + Sync {
    async fn on_dead_letter(
        &self,
        message: Option<&AgentMessage>,
        capsule: &AgentDeadLetter,
        publish_result: &Result<(), LaserError>,
    );
}

/// A message delivered to a handler: decoded provenance, raw payload, and log position.
#[derive(Debug, Clone)]
pub struct AgentMessage {
    /// Provenance headers decoded off the message. For an AGDX message it is
    /// synthesized from the decoded [`envelope`](Self::envelope), so routing,
    /// dedup, and deadline work uniformly for both message shapes.
    pub provenance: Provenance,
    /// The raw message body. Owned `Vec<u8>` so the public API never leaks the
    /// `bytes` crate. Decode it with whatever codec the producer used.
    pub payload: Vec<u8>,
    /// Where the message sits on the log (partition and offset).
    pub id: MessageId,
    /// The decoded AGDX envelope when the message carries one (the `agdx.av`
    /// header is present). `None` for a plain `send_agent` message.
    pub envelope: Option<AgentEnvelope>,
    /// The `agdx.ct` content-type header when stamped (what the
    /// [`body`](Self::body) bytes are), `None` when the producer stamped none.
    /// `ContentType::Ref` marks a claim-checked body, resolved with
    /// [`resolve_body`](Self::resolve_body).
    pub content_type: Option<ContentType>,
    /// The principal returned by enrolled signature verification. Set on
    /// contract replies accepted through a verifier, otherwise `None`.
    pub verified_principal: Option<String>,
}

impl AgentMessage {
    /// The task body, regardless of message shape: the AGDX envelope's `body` when
    /// the message is an AGDX command/response (its [`payload`](Self::payload) is
    /// the encoded envelope, not the body), otherwise the raw `payload`. A handler
    /// uses this so it does not have to know whether it was reached by a `contract`
    /// or workflow (AGDX) or a plain `send_agent`.
    pub fn body(&self) -> &[u8] {
        match &self.envelope {
            Some(envelope) => &envelope.body,
            None => &self.payload,
        }
    }

    // Decode a received message into an `AgentMessage`, materializing the payload
    // exactly once. On a decode failure the owned payload rides back in the error
    // so the caller can dead-letter it verbatim without a second copy (the old
    // path cloned the payload up front on every message just for that case).
    fn from_received(
        received: ReceivedMessage,
        understood_features: u64,
    ) -> Result<DecodedAgentMessage, (Box<LaserError>, Vec<u8>)> {
        // The message's own offset, not `received.current_offset` (the partition
        // high-water, shared across a polled batch).
        let id = MessageId::new(received.partition_id, received.message.header.offset);
        let payload = received.message.payload.to_vec();
        let decoded = match decode_agent_record(&received.message, understood_features) {
            Ok(decoded) => decoded,
            Err(error) => return Err((Box::new(error), payload)),
        };
        Ok(DecodedAgentMessage {
            message: Self {
                provenance: decoded.provenance,
                payload,
                id,
                envelope: decoded.envelope,
                content_type: decoded.content_type,
                verified_principal: None,
            },
            #[cfg(feature = "sign")]
            signature_context: decoded.signature_context,
            #[cfg(feature = "sign")]
            observed_at_micros: decoded.observed_at_micros,
        })
    }
}

struct DecodedAgentMessage {
    message: AgentMessage,
    #[cfg(feature = "sign")]
    signature_context: Option<SignatureContext>,
    #[cfg(feature = "sign")]
    observed_at_micros: u64,
}

pub(crate) struct DecodedAgentRecord {
    pub(crate) provenance: Provenance,
    pub(crate) envelope: Option<AgentEnvelope>,
    pub(crate) content_type: Option<ContentType>,
    pub(crate) signature_context: Option<SignatureContext>,
    pub(crate) observed_at_micros: u64,
}

pub(crate) fn decode_agent_record(
    message: &IggyMessage,
    understood_features: u64,
) -> Result<DecodedAgentRecord, LaserError> {
    let headers = message.user_headers_map()?.unwrap_or_default();
    let content_type_key = HeaderKey::from_str(CONTENT_TYPE)?;
    let content_type_code = headers
        .get(&content_type_key)
        .map(HeaderValue::as_uint8)
        .transpose()?;
    let content_type = content_type_code.and_then(ContentType::from_code);
    let version_key = HeaderKey::from_str(AGENT_VERSION)?;
    let version = headers
        .get(&version_key)
        .map(HeaderValue::as_uint32)
        .transpose()?;
    let (provenance, envelope, signature_context) = match version {
        Some(version) => {
            if version != AGENT_OP_VERSION {
                return Err(LaserError::Invalid(format!(
                    "unsupported agent envelope version {version}"
                )));
            }
            let envelope: AgentEnvelope = decode_named(&message.payload)?;
            validate(&envelope)?;
            let unmet = envelope.unmet_requirements(understood_features);
            if unmet != features::NONE {
                return Err(LaserError::Invalid(format!(
                    "agent envelope requires unsupported features 0x{unmet:016x}"
                )));
            }
            let provenance = provenance_from_envelope(&envelope);
            let context = SignatureContext {
                content_type: content_type_code,
                agent_version: Some(version),
            };
            (provenance, Some(envelope), Some(context))
        }
        None => (
            crate::provenance::provenance_from_headers(&headers)?,
            None,
            None,
        ),
    };
    Ok(DecodedAgentRecord {
        provenance,
        envelope,
        content_type,
        signature_context,
        observed_at_micros: message.header.timestamp,
    })
}

// Decode a log message into its runtime provenance and, when it is an AGDX
// message (the `agdx.av` header is present), its envelope. An AGDX message routes
// off the decoded envelope, whose typed fields the string-header provenance
// decoder cannot read. Everything else routes off the provenance headers. The
// read paths (the reliable consumer, context assembly, the stream reader) share
// this so AGDX and `send_agent` messages decode identically everywhere.
pub(crate) fn provenance_and_envelope(
    message: &IggyMessage,
) -> Result<(Provenance, Option<AgentEnvelope>), LaserError> {
    let decoded = decode_agent_record(message, features::NONE)?;
    Ok((decoded.provenance, decoded.envelope))
}

// Synthesize the runtime provenance from an AGDX envelope, so the consumer's
// target filter, dedup, and deadline checks read one shape for both message
// kinds. Agent ids are name strings on both sides, so `source`/`target` map
// straight across, and a name the SDK validator rejects simply drops out.
fn provenance_from_envelope(envelope: &AgentEnvelope) -> Provenance {
    Provenance::builder()
        .conversation_id(envelope.conversation.into())
        .maybe_agent(AgentId::try_from(envelope.source.as_str()).ok())
        .maybe_target_agent_id(
            envelope
                .target
                .as_ref()
                .and_then(|target| AgentId::try_from(target.as_str()).ok()),
        )
        .maybe_idempotency_key(
            envelope
                .idempotency_key
                .as_ref()
                .map(|key| key.as_str().to_owned()),
        )
        .maybe_correlation_id(
            envelope
                .correlation
                .map(|correlation| correlation.to_string()),
        )
        .maybe_deadline(envelope.deadline_micros.map(IggyTimestamp::from))
        // An enveloped fenced effect carries the fence as the `agdx.fence`
        // metadata key, so the consumer gate reads it the same way it reads the
        // header on a generic-provenance message.
        .maybe_fence_token(envelope.metadata.as_ref().and_then(
            |metadata| match metadata.get(FENCE) {
                Some(laser_wire::query::Value::Uint(fence)) => Some(*fence),
                Some(laser_wire::query::Value::Int(fence)) => u64::try_from(*fence).ok(),
                _ => None,
            },
        ))
        .build()
}

/// How the reliable consumer retries a transient handler error: capped attempts with exponential backoff.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Total attempts before dead-lettering.
    pub max_attempts: u32,
    /// First backoff delay, doubled each attempt.
    pub base_delay: Duration,
}

impl RetryPolicy {
    /// A policy of `max_attempts` with exponential backoff from `base_delay`.
    pub fn backoff(max_attempts: u32, base_delay: Duration) -> Self {
        Self {
            max_attempts,
            base_delay,
        }
    }

    fn delay_for(&self, attempt: u32) -> Duration {
        self.base_delay
            .saturating_mul(2u32.saturating_pow(attempt.min(16)))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(200),
        }
    }
}

/// How the reliable consumer schedules message handling across partitions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConcurrencyPolicy {
    /// One message at a time across every partition (the safe, ordered default).
    /// A slow or retrying message holds the member until it finishes.
    #[default]
    Serial,
    /// One worker lane per partition: messages from different partitions run
    /// concurrently, but a single partition is still handled strictly in order,
    /// one at a time. Retry backoff is lane-local, so a poison message on one
    /// partition never stalls another. `max_partitions` bounds the number of
    /// concurrent lanes (a message for a partition beyond the cap is handled
    /// inline rather than dropped).
    SerialPerPartition {
        /// The most concurrent lanes to run.
        max_partitions: usize,
    },
}

/// The reliable consumer (consumer-group delivery + dedup + retry + DLQ). Most callers use `Agent::builder`, not this directly.
#[derive(bon::Builder)]
pub struct ReliableConsumer {
    pub group: ConsumerGroupName,
    /// Logical identity used for target filtering and replies. It is never
    /// inferred from the deployment group name.
    pub agent: Option<AgentId>,
    #[builder(into)]
    pub topic: String,
    #[builder(default = 10_000)]
    pub dedup_window: usize,
    #[builder(default)]
    pub retry: RetryPolicy,
    /// Must-understand AGDX feature bits implemented by this receiver. Messages
    /// requiring any other bit are dead-lettered before dispatch.
    #[builder(default = features::NONE)]
    pub understood_features: u64,
    /// Poll interval, default a reactive 10ms. Raise for throughput-bound work.
    #[builder(default = POLL_INTERVAL)]
    pub poll_interval: Duration,
    /// How long a graceful shutdown waits for the in-flight message to finish
    /// before dropping the consumer. `run` returns [`LaserError::Timeout`] if the
    /// grace elapses with a message still in flight. Default 30s.
    #[builder(default = DEFAULT_SHUTDOWN_GRACE)]
    pub shutdown_grace: Duration,
    /// How message handling is scheduled across partitions. Defaults to
    /// [`ConcurrencyPolicy::Serial`] (strict one-at-a-time).
    #[builder(default)]
    pub concurrency: ConcurrencyPolicy,
    /// Maximum records buffered across partition lanes.
    #[builder(default = DEFAULT_MAX_QUEUED_RECORDS)]
    pub max_queued_records: usize,
    /// Maximum payload and header bytes buffered across partition lanes.
    #[builder(default = DEFAULT_MAX_QUEUED_BYTES)]
    pub max_queued_bytes: usize,
    pub respond_on: Option<AgentTopic<'static>>,
    /// Default inbox route for the ctx's directed-send and fan-out helpers.
    #[builder(default)]
    pub inbox_route: crate::agent::router::InboxRoute,
    /// Emit a `Working` task status on `respond_on` the moment an AGDX command is
    /// picked up, before the handler runs, so a [`contract`](crate::laser::Laser::contract)
    /// caller can tell the command was consumed (versus expired unconsumed). Off by
    /// default. Requires `respond_on` and a valid agent id.
    #[builder(default)]
    pub ack_on_pickup: bool,
    // Override the dedup backend. Defaults to an in-memory `SlidingWindow` of
    // `dedup_window` keys, and a durable backend is a drop-in via this seam.
    pub deduplicator: Option<Box<dyn Deduplicator>>,
    // Replay the partition tail into the dedup window on startup so a restart does
    // not reprocess duplicates that are still inside the window. Off by default
    // (the at-least-once + idempotent-handler default tolerates the replay).
    #[builder(default)]
    pub warm_dedup: bool,
    /// Cross-cutting hooks wrapped around each handler dispatch, in order, for
    /// auth, metrics, and tracing without touching the handler.
    #[builder(default)]
    pub middleware: Vec<std::sync::Arc<dyn AgentMiddleware>>,
    /// Notified on every dead-letter with the result of publishing it, so a lost
    /// poison message (a DLQ publish failure) is an observable event, not a log line.
    pub on_dead_letter: Option<std::sync::Arc<dyn DeadLetterSink>>,
    /// When set, every message's envelope signature is verified against this
    /// registry before dispatch, and an unsigned or unverified record is
    /// dead-lettered. Set it on control and effect topics, where verification is
    /// mandatory (the enforcement chokepoint for authorship and authorization).
    #[cfg(feature = "sign")]
    pub verifier: Option<std::sync::Arc<crate::sign::KeyRegistry>>,
    /// This consumer's signing identity, threaded into `AgentCtx` so `respond`
    /// answers correlated commands with signed AGDX responses.
    #[cfg(feature = "sign")]
    pub signing_key: Option<std::sync::Arc<crate::sign::SigningKey>>,
}

impl ReliableConsumer {
    /// Consume until `shutdown` fires, dispatching each message to `handler`.
    /// `ready` fires once the consumer has joined its group and is polling.
    pub async fn run<H>(
        self,
        laser: &Laser,
        handler: H,
        ready: oneshot::Sender<()>,
        shutdown: oneshot::Receiver<()>,
    ) -> Result<(), LaserError>
    where
        H: AgentHandler + Sync + Send + 'static,
    {
        let shutdown_grace = self.shutdown_grace;
        let concurrency = self.concurrency;
        // Both schedulers commit explicitly after successful handling. Iggy's
        // auto-commit runs while yielding a record, before the handler result is
        // known, so it cannot provide commit-after-success semantics.
        let auto_commit = AutoCommit::Disabled;
        let mut consumer = laser
            .client()
            .consumer_group(self.group.as_str(), laser.stream_required()?, &self.topic)?
            .auto_commit(auto_commit)
            .create_consumer_group_if_not_exists()
            .auto_join_consumer_group()
            .poll_interval(IggyDuration::new(self.poll_interval))
            .build();
        consumer.init().await?;

        let deduplicator = self
            .deduplicator
            .unwrap_or_else(|| Box::new(SlidingWindow::new(self.dedup_window)));
        if self.warm_dedup {
            warm_dedup_window(
                laser,
                self.group.as_str(),
                &self.topic,
                deduplicator.as_ref(),
                self.dedup_window,
            )
            .await?;
        }
        // Joined and dedup-warmed: signal readiness. A dropped receiver is fine.
        let _ = ready.send(());
        let agent = self.agent;
        // Resolve the subscribed stream and topic to their numeric ids once, so
        // every dead-letter capsule can carry a complete `LogPosition` for the
        // poison message without a server round-trip per failure. The consumer has
        // already joined this stream/topic, so a missing id is a should-never
        // happen: warn loudly rather than silently stamping a wrong locator -
        // the partition and offset (the locate-within-topic half) stay correct.
        let stream_ident = Identifier::named(laser.stream_required()?)?;
        let topic_ident = Identifier::named(&self.topic)?;
        let stream_id = laser
            .client()
            .get_stream(&stream_ident)
            .await?
            .map(|details| details.id);
        let topic_id = laser
            .client()
            .get_topic(&stream_ident, &topic_ident)
            .await?
            .map(|details| details.id);
        if stream_id.is_none() || topic_id.is_none() {
            warn!(
                topic = %self.topic,
                "could not resolve the numeric stream/topic id, dead-letter capsules \
                 carry 0 for the unresolved locator half (partition and offset stay correct)"
            );
        }
        let (stream_id, topic_id) = (stream_id.unwrap_or_default(), topic_id.unwrap_or_default());
        let group_consumer = Consumer::group(Identifier::named(self.group.as_str())?);
        let reliable = ReliableWorker {
            handler,
            laser: laser.clone(),
            retry: self.retry,
            understood_features: self.understood_features,
            dedup: deduplicator,
            agent,
            respond_on: self.respond_on,
            inbox_route: self.inbox_route,
            ack_on_pickup: self.ack_on_pickup,
            stream_id,
            topic_id,
            middleware: self.middleware,
            on_dead_letter: self.on_dead_letter,
            high_water_fence: dashmap::DashMap::new(),
            fence_last_sweep: std::sync::atomic::AtomicU64::new(0),
            #[cfg(feature = "sign")]
            verifier: self.verifier,
            #[cfg(feature = "sign")]
            signing_key: self.signing_key,
            #[cfg(feature = "sign")]
            verified_records: Mutex::new(DedupWindow::new(VERIFIED_RECORD_WINDOW)),
        };
        match concurrency {
            ConcurrencyPolicy::Serial => {
                run_serial(
                    consumer,
                    reliable,
                    group_consumer,
                    stream_ident,
                    topic_ident,
                    shutdown,
                    shutdown_grace,
                )
                .await
            }
            ConcurrencyPolicy::SerialPerPartition { max_partitions } => {
                run_per_partition(
                    consumer,
                    reliable,
                    max_partitions.max(1),
                    self.max_queued_records.max(1),
                    self.max_queued_bytes.max(1),
                    shutdown,
                    shutdown_grace,
                )
                .await
            }
        }
    }
}

struct SerialCommitter<'a, H> {
    worker: &'a ReliableWorker<H>,
    client: &'a IggyClient,
    group: &'a Consumer,
    stream: &'a Identifier,
    topic: &'a Identifier,
    failure: tokio::sync::mpsc::Sender<IggyError>,
}

impl<H> SerialCommitter<'_, H> {
    async fn stop(&self, error: IggyError) -> Result<(), IggyError> {
        let _ = self.failure.send(error).await;
        std::future::pending().await
    }
}

impl<H> MessageConsumer for SerialCommitter<'_, H>
where
    H: AgentHandler + Sync,
{
    async fn consume(&self, received: ReceivedMessage) -> Result<(), IggyError> {
        let partition = received.partition_id;
        let offset = received.message.header.offset;
        if let Err(error) = self.worker.consume(received).await {
            return self.stop(error).await;
        }
        if let Err(error) = self
            .client
            .store_consumer_offset(self.group, self.stream, self.topic, Some(partition), offset)
            .await
        {
            return self.stop(error).await;
        }
        Ok(())
    }
}

async fn run_serial<H>(
    mut consumer: IggyConsumer,
    reliable: ReliableWorker<H>,
    group: Consumer,
    stream: Identifier,
    topic: Identifier,
    shutdown: oneshot::Receiver<()>,
    shutdown_grace: Duration,
) -> Result<(), LaserError>
where
    H: AgentHandler + Sync + Send + 'static,
{
    let (drain_tx, drain_rx) = oneshot::channel();
    let (failure_tx, mut failure_rx) = tokio::sync::mpsc::channel(1);
    let committer = SerialCommitter {
        worker: &reliable,
        client: reliable.laser.client(),
        group: &group,
        stream: &stream,
        topic: &topic,
        failure: failure_tx,
    };
    let mut drained = true;
    let result = {
        let consume = consumer.consume_messages(&committer, drain_rx);
        tokio::pin!(consume);
        tokio::select! {
            result = &mut consume => result.map_err(LaserError::from),
            Some(error) = failure_rx.recv() => Err(LaserError::from(error)),
            signal = shutdown => match signal {
                Ok(()) => {
                    let _ = drain_tx.send(());
                    let finish = async {
                        tokio::select! {
                            result = &mut consume => result.map_err(LaserError::from),
                            Some(error) = failure_rx.recv() => Err(LaserError::from(error)),
                        }
                    };
                    match tokio::time::timeout(shutdown_grace, finish).await {
                        Ok(result) => result,
                        Err(_) => {
                            drained = false;
                            Err(LaserError::Timeout("agent shutdown drain"))
                        }
                    }
                }
                Err(_) => tokio::select! {
                    result = &mut consume => result.map_err(LaserError::from),
                    Some(error) = failure_rx.recv() => Err(LaserError::from(error)),
                },
            },
        }
    };
    if drained && let Err(error) = consumer.shutdown().await {
        warn!(%error, "failed to leave the consumer group on shutdown");
    }
    result
}

// The per-partition scheduler: poll the consumer, route each message to a lane
// keyed by partition, and store each offset only after its lane has handled the
// message. Lanes run concurrently across partitions. Within one partition the
// lane is a serial mpsc queue, so ordering and per-partition offset monotonicity
// hold. A retrying handler blocks only its own lane. Offsets are stored by this
// task (the sole owner of the consumer), driven by lane completions, so there is
// no shared mutable consumer. On shutdown the lane senders are dropped, so each
// lane finishes its queued messages and exits, bounded by the grace.
async fn run_per_partition<H>(
    mut consumer: IggyConsumer,
    reliable: ReliableWorker<H>,
    max_partitions: usize,
    max_queued_records: usize,
    max_queued_bytes: usize,
    shutdown: oneshot::Receiver<()>,
    shutdown_grace: Duration,
) -> Result<(), LaserError>
where
    H: AgentHandler + Sync + Send + 'static,
{
    use futures::StreamExt;

    let max_queued_records = max_queued_records.min(u32::MAX as usize);
    let max_queued_bytes = max_queued_bytes.min(u32::MAX as usize);
    let worker = std::sync::Arc::new(reliable);
    let mut lanes: std::collections::HashMap<u32, tokio::sync::mpsc::Sender<QueuedMessage>> =
        std::collections::HashMap::new();
    let mut lane_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    let (commit_tx, mut commit_rx) =
        tokio::sync::mpsc::channel::<Result<(u32, u64), IggyError>>(max_partitions * 2);
    let notify = std::sync::Arc::new(tokio::sync::Notify::new());
    let record_capacity = std::sync::Arc::new(tokio::sync::Semaphore::new(max_queued_records));
    let byte_capacity = std::sync::Arc::new(tokio::sync::Semaphore::new(max_queued_bytes));
    tokio::pin!(shutdown);

    // A non-transport poll error (an authorization failure on the topic, say)
    // is recoverable in principle, so it does not end the consumer. It is also
    // usually persistent, so retrying it immediately would spin a core. Back
    // off between consecutive failures and give up once they stop looking
    // transient.
    const MAX_CONSECUTIVE_POLL_ERRORS: u32 = 10;
    let mut consecutive_poll_errors: u32 = 0;

    let stopped = 'polling: loop {
        // Store every completed offset before polling again. The consumer is not
        // borrowed by any live future here, so this is the one place offsets are
        // committed, keeping the borrow simple.
        while let Ok(completion) = commit_rx.try_recv() {
            match completion {
                Ok((partition, offset)) => {
                    if let Err(error) = consumer.store_offset(offset, Some(partition)).await {
                        break 'polling Err(LaserError::from(error));
                    }
                }
                Err(error) => break 'polling Err(LaserError::from(error)),
            }
        }
        tokio::select! {
            _ = &mut shutdown => break Ok(()),
            // Wake to drain a completed offset at the top of the loop. The value is
            // left in the channel (a spurious wake is harmless).
            _ = notify.notified() => {}
            message = consumer.next() => match message {
                Some(Ok(received)) => {
                    consecutive_poll_errors = 0;
                    let partition = received.partition_id;
                    let known = lanes.contains_key(&partition);
                    if !known && lanes.len() >= max_partitions {
                        // Over the lane cap: handle inline rather than drop the
                        // message or spawn an unbounded number of lanes.
                        let offset = received.message.header.offset;
                        if let Err(error) = worker.consume(received).await {
                            break 'polling Err(LaserError::from(error));
                        }
                        if let Err(error) = consumer.store_offset(offset, Some(partition)).await {
                            break 'polling Err(LaserError::from(error));
                        }
                        continue;
                    }
                    let lane = lanes.entry(partition).or_insert_with(|| {
                        let (tx, mut rx) = tokio::sync::mpsc::channel::<QueuedMessage>(max_queued_records);
                        let worker = worker.clone();
                        let commit_tx = commit_tx.clone();
                        let notify = notify.clone();
                        lane_handles.push(tokio::spawn(async move {
                            while let Some(queued) = rx.recv().await {
                                let received = queued.received;
                                let partition = received.partition_id;
                                let offset = received.message.header.offset;
                                let result = worker.consume(received).await;
                                drop(queued.record_permit);
                                drop(queued.byte_permit);
                                let failed = result.is_err();
                                let completion = result.map(|()| (partition, offset));
                                let _ = commit_tx.send(completion).await;
                                notify.notify_one();
                                if failed {
                                    break;
                                }
                            }
                        }));
                        tx
                    });
                    let buffered_bytes = received.message.payload.len()
                        + received.message.header.user_headers_length as usize;
                    let record_permit = record_capacity
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|_| LaserError::HandlerConfig("record queue closed".to_owned()))?;
                    let byte_permits = buffered_bytes.clamp(1, max_queued_bytes) as u32;
                    let byte_permit = byte_capacity
                        .clone()
                        .acquire_many_owned(byte_permits)
                        .await
                        .map_err(|_| LaserError::HandlerConfig("byte queue closed".to_owned()))?;
                    let queued = QueuedMessage {
                        received,
                        record_permit,
                        byte_permit,
                    };
                    if lane.send(queued).await.is_err() {
                        break 'polling Err(LaserError::HandlerConfig(
                            "partition lane stopped before accepting a message".to_owned(),
                        ));
                    }
                }
                Some(Err(error)) => match error {
                    IggyError::Disconnected
                    | IggyError::CannotEstablishConnection
                    | IggyError::StaleClient
                    | IggyError::InvalidServerAddress
                    | IggyError::InvalidClientAddress
                    | IggyError::NotConnected
                    | IggyError::ClientShutdown => break Err(LaserError::from(error)),
                    other => {
                        consecutive_poll_errors += 1;
                        if consecutive_poll_errors >= MAX_CONSECUTIVE_POLL_ERRORS {
                            break 'polling Err(LaserError::from(other));
                        }
                        warn!(
                            error = %other,
                            attempt = consecutive_poll_errors,
                            "agent consumer poll failed, backing off"
                        );
                        sleep(backoff_for(consecutive_poll_errors)).await;
                    }
                },
                None => break Ok(()),
            },
        }
    };

    // Drain: dropping the lane senders lets each lane finish its queued messages
    // and exit. Await the lanes and flush their final offsets, bounded by grace.
    drop(lanes);
    drop(commit_tx);
    let drain = async {
        for handle in lane_handles {
            let _ = handle.await;
        }
        while let Some(completion) = commit_rx.recv().await {
            match completion {
                Ok((partition, offset)) => {
                    if let Err(error) = consumer.store_offset(offset, Some(partition)).await {
                        return Err(LaserError::from(error));
                    }
                }
                Err(error) => return Err(LaserError::from(error)),
            }
        }
        Ok::<(), LaserError>(())
    };
    match tokio::time::timeout(shutdown_grace, drain).await {
        Ok(Ok(())) => {
            // Every lane finished and stored its offsets, so the member can leave
            // its group and a restarted consumer owns every partition. On a drain
            // timeout the lanes may hold unhandled messages, so leaving (which
            // flushes the polled high-water offsets) would mark them consumed.
            // That membership is left to the connection close instead.
            if let Err(error) = consumer.shutdown().await {
                warn!(%error, "failed to leave the consumer group on shutdown");
            }
            stopped
        }
        Ok(Err(error)) => Err(error),
        Err(_) => Err(LaserError::Timeout("agent shutdown drain")),
    }
}

struct QueuedMessage {
    received: ReceivedMessage,
    record_permit: tokio::sync::OwnedSemaphorePermit,
    byte_permit: tokio::sync::OwnedSemaphorePermit,
}

/// The dedup key, principal-scoped so one producer cannot suppress or replay
/// another's idempotency key. Composed as `{agent}{SEP}{key}`. The agent is
/// publisher-asserted, so this is a namespace against accidental reuse, not a
/// security boundary (the fence is the real at-most-once gate). The live and
/// warm-up paths both go through this, or dedup breaks after a restart.
fn dedup_key(provenance: &Provenance) -> Option<String> {
    let key = provenance.idempotency_key.as_ref()?;
    Some(match &provenance.agent {
        Some(agent) => format!("{}{DEDUP_SCOPE_SEP}{key}", agent.as_str()),
        None => key.clone(),
    })
}

/// One task's fence high-water mark and when it was last advanced, so an idle
/// entry can be swept without losing the gate for an active task.
#[derive(Clone, Copy)]
struct FenceEntry {
    fence: u64,
    touched_micros: u64,
}

/// The monotonic high-water-mark fence gate. Returns `true` to accept the fence
/// (advancing the task's high water) or `false` to drop a stale-holder replay
/// whose fence is below the highest already accepted. An equal fence is accepted,
/// the same holder's legitimate retry, which dedup then handles. When the map is
/// over its soft cap, an idle-entry sweep runs at most once per sweep interval,
/// bounding memory without reopening the gate for any recently-active task.
fn accept_fence(
    high_water: &dashmap::DashMap<ConversationId, FenceEntry>,
    last_sweep_micros: &std::sync::atomic::AtomicU64,
    task: ConversationId,
    fence: u64,
    now_micros: u64,
) -> bool {
    if high_water.len() > FENCE_MAP_SOFT_CAP {
        use std::sync::atomic::Ordering;
        let previous = last_sweep_micros.load(Ordering::Relaxed);
        if now_micros.saturating_sub(previous) > FENCE_SWEEP_INTERVAL_MICROS
            && last_sweep_micros
                .compare_exchange(previous, now_micros, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            high_water.retain(|_, entry| {
                now_micros.saturating_sub(entry.touched_micros) < FENCE_ENTRY_TTL_MICROS
            });
        }
    }
    let mut entry = high_water.entry(task).or_insert(FenceEntry {
        fence: 0,
        touched_micros: now_micros,
    });
    if fence < entry.fence {
        return false;
    }
    entry.fence = fence;
    entry.touched_micros = now_micros;
    true
}

struct ReliableWorker<H> {
    handler: H,
    laser: Laser,
    retry: RetryPolicy,
    understood_features: u64,
    dedup: Box<dyn Deduplicator>,
    agent: Option<AgentId>,
    respond_on: Option<AgentTopic<'static>>,
    inbox_route: crate::agent::router::InboxRoute,
    ack_on_pickup: bool,
    stream_id: u32,
    topic_id: u32,
    middleware: Vec<std::sync::Arc<dyn AgentMiddleware>>,
    on_dead_letter: Option<std::sync::Arc<dyn DeadLetterSink>>,
    #[cfg(feature = "sign")]
    verifier: Option<std::sync::Arc<crate::sign::KeyRegistry>>,
    #[cfg(feature = "sign")]
    signing_key: Option<std::sync::Arc<crate::sign::SigningKey>>,
    /// Highest fence token accepted per task (the conversation is the task scope).
    /// A log-resident effect with a lower fence is a stale-holder replay and is
    /// dropped before dedup, so it never consumes the legitimate retry's slot.
    /// Idle entries are swept past a ttl once over a soft cap, so the map stays
    /// bounded by the active working set.
    high_water_fence: dashmap::DashMap<ConversationId, FenceEntry>,
    /// When the fence map was last swept of idle entries (epoch micros), so the
    /// sweep runs at most once per interval under load.
    fence_last_sweep: std::sync::atomic::AtomicU64,
    /// Record ids already accepted through signature verification. The signed
    /// preimage covers the envelope but binds it to no log position, so the
    /// exact signed bytes stay valid wherever they are replayed. This bounded
    /// window refuses the second delivery of a record id, the same guard the
    /// registry fold keeps over applied facts.
    #[cfg(feature = "sign")]
    verified_records: Mutex<DedupWindow>,
}

impl<H> ReliableWorker<H> {
    fn log_position(&self, id: MessageId) -> LogPosition {
        LogPosition::new(self.stream_id, self.topic_id, id.partition_id, id.offset)
    }

    // Dead-letters a decoded message: the capsule carries the poison message's
    // log position, the reason code, the attempt count, a human detail, and the
    // original payload VERBATIM, so redrive is republishing those bytes.
    async fn dead_letter(
        &self,
        message: &AgentMessage,
        reason: DeadLetterReason,
        attempts: u32,
        detail: &str,
    ) -> Result<(), LaserError> {
        let capsule = AgentDeadLetter {
            source: self.log_position(message.id),
            reason,
            attempts,
            detail: Some(detail.to_owned()),
            payload: message.payload.clone(),
        };
        // Carry the original provenance for inspection, repointed at the poison
        // message. Clear the deadline so a deadline-bound DLQ consumer does not
        // re-drop the capsule for the very deadline that killed the original.
        let mut provenance = message.provenance.clone();
        provenance.causal_parent = Some(message.id);
        provenance.deadline = None;
        self.publish_dead_letter(provenance, message.id, capsule, Some(message))
            .await
    }

    // Dead-letters a message whose provenance could not be decoded. The original
    // payload rides verbatim so nothing is lost, and the synthetic provenance carries
    // only the source offset as the causal parent (there are no original headers
    // to keep, failing to decode them is why this path ran).
    async fn dead_letter_undecodable(
        &self,
        source: MessageId,
        payload: Vec<u8>,
    ) -> Result<(), LaserError> {
        let capsule = AgentDeadLetter {
            source: self.log_position(source),
            reason: DeadLetterReason::DecodeFailed,
            attempts: 0,
            detail: None,
            payload,
        };
        let provenance = Provenance::builder()
            .conversation_id(ConversationId::new())
            .causal_parent(source)
            .build();
        self.publish_dead_letter(provenance, source, capsule, None)
            .await
    }

    async fn publish_dead_letter(
        &self,
        provenance: Provenance,
        source: MessageId,
        capsule: AgentDeadLetter,
        message: Option<&AgentMessage>,
    ) -> Result<(), LaserError> {
        let reason = capsule.reason;
        let result = self.send_dead_letter(&provenance, &capsule).await;
        if let Err(error) = &result {
            error!(%error, source = %source, ?reason, "failed to publish the dead-letter capsule, leaving the source offset uncommitted");
        }
        if let Some(sink) = &self.on_dead_letter {
            sink.on_dead_letter(message, &capsule, &result).await;
        }
        result
    }

    // Encode and publish one dead-letter capsule, returning the outcome so the
    // caller can log it and notify the sink. Any encode or header failure is a
    // lost message just like a publish failure, so it surfaces as an `Err`.
    async fn send_dead_letter(
        &self,
        provenance: &Provenance,
        capsule: &AgentDeadLetter,
    ) -> Result<(), LaserError> {
        let payload = encode_named(capsule)
            .map_err(|error| LaserError::Codec(format!("dead-letter capsule: {error}")))?;
        let mut headers = BTreeMap::<HeaderKey, HeaderValue>::try_from(provenance)
            .map_err(|error| LaserError::Codec(format!("dead-letter headers: {error}")))?;
        // Mark the capsule body as cbor so a DLQ consumer is self-describing.
        let key = HeaderKey::from_str(CONTENT_TYPE)?;
        headers.insert(key, HeaderValue::from(ContentType::Cbor.code()));
        let topic = AgentTopic::Dlq.topic_string();
        let partition_key = provenance.partition_key();
        self.laser
            .send_with_headers(&topic, payload, headers, Some(&partition_key))
            .await
            .map(|_| ())
    }
}

impl<H> MessageConsumer for ReliableWorker<H>
where
    H: AgentHandler + Sync,
{
    #[tracing::instrument(target = "laser", level = "debug", skip_all, fields(conversation = tracing::field::Empty, operation = "handle"))]
    async fn consume(&self, received: ReceivedMessage) -> Result<(), IggyError> {
        let source = MessageId::new(received.partition_id, received.message.header.offset);
        let decoded = match AgentMessage::from_received(received, self.understood_features) {
            Ok(decoded) => decoded,
            Err((error, payload)) => {
                warn!(%error, source = %source, "undecodable provenance, dead-lettering raw payload");
                self.dead_letter_undecodable(source, payload)
                    .await
                    .map_err(|_| IggyError::Error)?;
                return Ok(());
            }
        };
        #[cfg(feature = "sign")]
        let DecodedAgentMessage {
            mut message,
            signature_context,
            observed_at_micros,
        } = decoded;
        #[cfg(not(feature = "sign"))]
        let DecodedAgentMessage { message, .. } = decoded;
        tracing::Span::current().record(
            "conversation",
            tracing::field::display(&message.provenance.conversation_id),
        );

        // Target-agent routing filter (defensive). Iggy's consumer-group
        // semantics already guarantee one delivery per group, so the
        // canonical one-agent-one-group setup (see `Agent` docstring) makes
        // this check a no-op in steady state. Bites in two cases:
        //   1. a publisher mis-addresses `target_agent_id` to the wrong
        //      agent that happens to subscribe to the same topic, drop
        //      cleanly instead of corrupting state with a misrouted handler
        //      invocation.
        //   2. operator error: two distinct agent ids accidentally joined
        //      the same consumer group, in which case Iggy delivers each
        //      message to ONE member and we want the other member's
        //      messages skipped, not handled.
        // Tolerating one-message-loss in case (2) is by design: the operator
        // is supposed to fix the group-per-agent setup, not have the SDK
        // paper over it by handling unrelated agents' work.
        if let (Some(target), Some(agent)) = (&message.provenance.target_agent_id, &self.agent)
            && target != agent
        {
            debug!(target = %target, agent = %agent, source = %message.id, "skipping message targeted at another agent");
            return Ok(());
        }

        // Mandatory signature verification on a verified (control or effect) topic.
        // An unsigned or unverified record is dead-lettered before any gate or
        // handler runs: the field is optional on the wire, so the only enforcement
        // is the consumer refusing to act on an unverified record here.
        #[cfg(feature = "sign")]
        if let Some(registry) = &self.verifier {
            let verified = message.envelope.as_ref().and_then(|envelope| {
                signature_context.as_ref().and_then(|context| {
                    registry
                        .verify_observed_at(envelope, context, observed_at_micros)
                        .ok()
                })
            });
            let Some(verified) = verified else {
                warn!(source = %message.id, "unsigned or unverified message on a verified topic, dead-lettering");
                self.dead_letter(
                    &message,
                    DeadLetterReason::Rejected,
                    0,
                    "signature verification failed",
                )
                .await
                .map_err(|_| IggyError::Error)?;
                return Ok(());
            };
            // A verified signature proves who authored the envelope, not that
            // this is its first delivery. Any writer on the topic can replay the
            // captured bytes, and they verify again, so a repeat record id is
            // dropped here before the handler acts on it.
            if let Some(record) = message
                .envelope
                .as_ref()
                .and_then(|envelope| envelope.record)
            {
                let first_delivery = self
                    .verified_records
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .insert(&record.to_string());
                if !first_delivery {
                    warn!(source = %message.id, %record, "replayed signed record, dropping");
                    return Ok(());
                }
            }
            message.verified_principal = Some(verified.principal);
        }

        // Fence gate, ordered BEFORE dedup. A log-resident effect carrying a fence
        // below the highest this task has accepted is a stale-holder replay: drop
        // it (the offset still commits, it is not a dead-letter). Running this
        // before `dedup.observe` matters, a fenced-out record must not consume the
        // idempotency slot the legitimate holder's retry needs. A malformed fence
        // already failed to decode (an error, never `.ok()`-ed to absent), so a
        // present token here is trustworthy.
        if let Some(fence) = message.provenance.fence_token
            && !accept_fence(
                &self.high_water_fence,
                &self.fence_last_sweep,
                message.provenance.conversation_id,
                fence,
                SystemClock.now_micros(),
            )
        {
            debug!(source = %message.id, fence, "stale-holder fence, dropping replay");
            return Ok(());
        }

        if let Some(key) = dedup_key(&message.provenance) {
            // Dedup marks the key seen before the handler runs: a duplicate arriving
            // while the original is still in the window is skipped even if the
            // original later dead-letters. This is the at-least-once + idempotent
            // model, and a durable `Deduplicator` is the drop-in upgrade. The key is
            // principal-scoped (see `dedup_key`).
            if !self.dedup.observe(&key).await {
                debug!(dedup_key = %key, source = %message.id, "skipping duplicate message");
                return Ok(());
            }
        }

        if let Some(deadline) = message.provenance.deadline
            && IggyTimestamp::now().as_micros() > deadline.as_micros()
        {
            warn!(source = %message.id, "message past its deadline, dead-lettering");
            self.dead_letter(
                &message,
                DeadLetterReason::DeadlineExceeded,
                0,
                "message past its deadline",
            )
            .await
            .map_err(|_| IggyError::Error)?;
            return Ok(());
        }

        // Ack-on-pickup: a `Working` status the instant an AGDX command is taken,
        // before the handler runs, so a contract caller distinguishes consumed
        // from expired-unconsumed. The command survived the deadline check above,
        // so it was consumed in time.
        if self.ack_on_pickup
            && let (Some(agent), Some(respond_on), Some(envelope)) =
                (&self.agent, &self.respond_on, &message.envelope)
            && envelope.kind == AgentKind::Command
            && let Some(correlation) = envelope.correlation
        {
            let producer =
                self.laser
                    .agdx(respond_on.clone(), agent.wire_id(), envelope.conversation);
            let ack = producer
                .status(OPERATION_TASK)
                .with_correlation(correlation)
                .with_task_state(TaskState::Working);
            #[cfg(feature = "sign")]
            let ack = match &self.signing_key {
                Some(signing_key) => ack.signed_by(signing_key),
                None => ack,
            };
            if let Err(error) = ack.send().await {
                warn!(source = %message.id, %error, "failed to emit ack-on-pickup status");
            }
        }

        let ctx = AgentCtx::new(
            &self.laser,
            &message,
            self.agent.clone(),
            self.respond_on.clone(),
            self.inbox_route.clone(),
            #[cfg(feature = "sign")]
            self.signing_key.clone(),
        );
        // Middleware `before_handle` runs once, in order, before the retry loop.
        // A rejection there dead-letters the message without ever running the
        // handler (the auth/gatekeeping use), so it is a non-retryable stop.
        for middleware in &self.middleware {
            if let Err(error) = middleware.before_handle(&message).await {
                warn!(%error, source = %message.id, "middleware rejected message before handling, dead-lettering");
                self.dead_letter(&message, DeadLetterReason::Rejected, 0, &error.to_string())
                    .await
                    .map_err(|_| IggyError::Error)?;
                return Ok(());
            }
        }
        let mut attempt = 0;
        loop {
            let result = self.handler.handle(&message, &ctx).await;
            // `after_handle` sees every attempt's outcome and one-based number, so a
            // metrics middleware counts retries, successes, and terminal failures.
            for middleware in &self.middleware {
                middleware
                    .after_handle(&message, &result, attempt + 1)
                    .await;
            }
            match result {
                Ok(()) => {
                    debug!(source = %message.id, "message handled");
                    return Ok(());
                }
                Err(error) => {
                    if !error.is_retryable() {
                        warn!(%error, source = %message.id, "handler rejected message, dead-lettering without retry");
                        self.dead_letter(
                            &message,
                            DeadLetterReason::Rejected,
                            attempt + 1,
                            &error.to_string(),
                        )
                        .await
                        .map_err(|_| IggyError::Error)?;
                        return Ok(());
                    }
                    if attempt + 1 >= self.retry.max_attempts {
                        error!(%error, source = %message.id, attempts = attempt + 1, "handler exhausted retries, dead-lettering");
                        self.dead_letter(
                            &message,
                            DeadLetterReason::RetryExhausted,
                            attempt + 1,
                            &error.to_string(),
                        )
                        .await
                        .map_err(|_| IggyError::Error)?;
                        return Ok(());
                    }
                    warn!(%error, source = %message.id, attempt = attempt + 1, "handler failed, retrying");
                    sleep(self.retry.delay_for(attempt)).await;
                    attempt += 1;
                }
            }
        }
    }
}

// Pre-fills the dedup window from each partition so a freshly started consumer
// recognizes duplicates of messages it processed before the restart. Reads only
// up to the group's stored (already-consumed) offset and at most `depth` per
// partition: reading past the stored offset would pre-mark un-consumed messages
// and cause them to be skipped (data loss).
async fn warm_dedup_window(
    laser: &Laser,
    group: &str,
    topic: &str,
    dedup: &dyn Deduplicator,
    depth: usize,
) -> Result<(), LaserError> {
    let stream = Identifier::named(laser.stream_required()?)?;
    let topic_id = Identifier::named(topic)?;
    let Some(details) = laser.client().get_topic(&stream, &topic_id).await? else {
        return Ok(());
    };
    let group_consumer = Consumer::group(Identifier::named(group)?);
    let reader = Consumer::new(Identifier::named("laser-dedup-warmer")?);
    let depth = u64::try_from(depth).unwrap_or(u64::MAX);
    for partition in 0..crate::poll::bounded_partitions(details.partitions_count) {
        let Some(offset) = laser
            .client()
            .get_consumer_offset(&group_consumer, &stream, &topic_id, Some(partition))
            .await?
        else {
            continue;
        };
        let stored = offset.stored_offset;
        let start = stored.saturating_sub(depth.saturating_sub(1));
        let count =
            u32::try_from(stored.saturating_sub(start).saturating_add(1)).unwrap_or(u32::MAX);
        let polled = laser
            .client()
            .poll_messages(
                &stream,
                &topic_id,
                Some(partition),
                &reader,
                &PollingStrategy::offset(start),
                count,
                false,
            )
            .await?;
        for message in polled.messages {
            if message.header.offset > stored {
                continue;
            }
            if let Ok(provenance) = Provenance::try_from(&message)
                && let Some(key) = dedup_key(&provenance)
            {
                dedup.observe(&key).await;
            }
        }
    }
    Ok(())
}

/// The dedup seam: decides whether an idempotency key has been seen before. The
/// default `SlidingWindow` is an in-memory bounded set. A durable backend (a
/// `StateStore`, or infrastructure-side dedup) is a drop-in. `observe` is async
/// and the trait is `dyn`-safe so a premium backend can do I/O behind it.
#[async_trait]
pub trait Deduplicator: Send + Sync {
    // Records the key and returns true if it is new, false if already seen.
    async fn observe(&self, key: &str) -> bool;
}

/// The default `Deduplicator`: an in-memory bounded set of recent keys.
pub struct SlidingWindow {
    inner: Mutex<DedupWindow>,
}

impl SlidingWindow {
    /// A window that remembers the most recent `capacity` keys.
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(DedupWindow::new(capacity)),
        }
    }
}

#[async_trait]
impl Deduplicator for SlidingWindow {
    async fn observe(&self, key: &str) -> bool {
        self.inner
            .lock()
            .expect("the dedup mutex is not poisoned")
            .insert(key)
    }
}

struct DedupWindow {
    capacity: usize,
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl DedupWindow {
    fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            seen: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    fn insert(&mut self, key: &str) -> bool {
        if self.seen.contains(key) {
            return false;
        }
        if self.order.len() >= self.capacity
            && let Some(evicted) = self.order.pop_front()
        {
            self.seen.remove(&evicted);
        }
        self.seen.insert(key.to_owned());
        self.order.push_back(key.to_owned());
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_small_positive_fence_when_decoded_then_should_preserve_the_token() {
        let envelope = AgentEnvelope::command(
            laser_wire::agent::RecordId::from_u128(3),
            laser_wire::agent::ConversationId::from_u128(1),
            "orchestrator".parse().expect("agent id parses"),
            laser_wire::agent::CorrelationId::from_u128(2),
            Vec::new(),
        )
        .with_metadata(FENCE, laser_wire::query::Value::Int(7));

        assert_eq!(provenance_from_envelope(&envelope).fence_token, Some(7));
    }

    #[test]
    fn given_a_seen_key_when_inserting_again_then_should_report_a_duplicate() {
        let mut window = DedupWindow::new(8);
        assert!(window.insert("a"));
        assert!(!window.insert("a"));
        assert!(window.insert("b"));
    }

    #[test]
    fn given_a_full_window_when_inserting_then_should_evict_the_oldest_key() {
        let mut window = DedupWindow::new(2);
        assert!(window.insert("a"));
        assert!(window.insert("b"));
        assert!(window.insert("c"));
        assert!(window.insert("a"));
    }

    #[test]
    fn given_increasing_attempts_when_computing_backoff_then_should_grow_and_stay_bounded() {
        let policy = RetryPolicy::backoff(5, Duration::from_millis(100));
        assert_eq!(policy.delay_for(0), Duration::from_millis(100));
        assert_eq!(policy.delay_for(1), Duration::from_millis(200));
        assert_eq!(policy.delay_for(2), Duration::from_millis(400));
        assert!(policy.delay_for(60) >= policy.delay_for(2));
    }

    #[test]
    fn given_two_agents_with_the_same_idempotency_key_when_scoped_then_should_differ() {
        let conversation = ConversationId::new();
        let with_agent = |agent: &str| {
            Provenance::builder()
                .conversation_id(conversation)
                .agent(agent.parse().expect("valid agent id"))
                .idempotency_key("attempt-1".to_owned())
                .build()
        };
        // Same idempotency key, different producers, so the scoped keys differ and
        // one cannot suppress the other.
        assert_ne!(
            dedup_key(&with_agent("planner")),
            dedup_key(&with_agent("worker"))
        );
        // No agent falls back to the bare key.
        let anon = Provenance::builder()
            .conversation_id(conversation)
            .idempotency_key("attempt-1".to_owned())
            .build();
        assert_eq!(dedup_key(&anon).as_deref(), Some("attempt-1"));
    }

    #[test]
    fn given_a_fence_gate_when_a_stale_token_arrives_then_should_drop_it_and_keep_per_task_scope() {
        let high_water = dashmap::DashMap::new();
        let sweep = std::sync::atomic::AtomicU64::new(0);
        let task_a = ConversationId::new();
        let task_b = ConversationId::new();

        // First grant accepted, advances the high water.
        assert!(accept_fence(&high_water, &sweep, task_a, 1, 100));
        // A fresh holder at a higher fence accepted.
        assert!(accept_fence(&high_water, &sweep, task_a, 2, 101));
        // The original holder's stale replay (below the high water) is dropped.
        assert!(!accept_fence(&high_water, &sweep, task_a, 1, 102));
        // An equal fence (the same holder's legitimate retry) is accepted, dedup
        // handles the duplicate downstream.
        assert!(accept_fence(&high_water, &sweep, task_a, 2, 103));
        // A different task keeps its own high water, so a low fence there is fine.
        assert!(accept_fence(&high_water, &sweep, task_b, 1, 104));
    }
}

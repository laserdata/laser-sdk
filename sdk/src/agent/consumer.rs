use crate::agent::budget::BudgetGate;
use crate::agent::clock::{Clock, SystemClock};
use crate::agent::control::ControlBook;
use crate::agent::ctx::AgentCtx;
use crate::agent::pause::{Hold, PauseRuntime, Replay, SourceTopic};
use crate::agent::session::{SessionConfig, Sessions};
use crate::capabilities::HelloOutcome;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::{AgentTopic, LlmUsage, Provenance};
use crate::stream::{CommitPolicy, Consumer, ConsumerMessage, Headers};
use crate::types::{AgentId, ConsumerGroupName, ConversationId, MessageId};
use async_trait::async_trait;
use futures::FutureExt;
use iggy::prelude::*;
use laser_wire::agent::{
    AgentDeadLetter, AgentEnvelope, AgentErrorBody, AgentErrorCode, AgentKind, DeadLetterReason,
    LogPosition, OPERATION_TASK, SignatureContext, TaskState, features, validate,
};
use laser_wire::codes::AGENT_OP_VERSION;
use laser_wire::content::ContentType;
use laser_wire::dispatch::{
    Dispatch, HandledOperations, addressee_filter, classify, classify_generic,
};
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::headers::{AGENT_VERSION, CONTENT_TYPE, CONVERSATION_ID, FENCE};
use laser_wire::topics::{AGENT_CONTROL, AGENT_SESSIONS};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tracing::{debug, error, warn};

// Capped exponential backoff between consecutive poll failures: 50ms, 100ms,
// 200ms, up to one second.
pub(crate) fn backoff_for(attempt: u32) -> Duration {
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

    // Decode a delivered record into an `AgentMessage`. On a decode failure
    // the payload rides back in the error so the caller can dead-letter it
    // verbatim.
    fn from_consumer(
        received: &ConsumerMessage,
        understood_features: u64,
    ) -> Result<DecodedAgentMessage, (Box<LaserError>, Vec<u8>)> {
        let id = received.position;
        let payload = received.payload.to_vec();
        if received.headers_malformed {
            return Err((
                Box::new(LaserError::Invalid(
                    "the record's header block does not decode".to_owned(),
                )),
                payload,
            ));
        }
        let decoded = match decode_record_parts(
            &received.headers,
            &received.payload,
            received.timestamp_micros,
            understood_features,
        ) {
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
    decode_record_parts(
        &headers,
        &message.payload,
        message.header.timestamp,
        understood_features,
    )
}

// Decode one record from its header map, payload, and broker timestamp.
fn decode_record_parts(
    headers: &Headers,
    payload: &[u8],
    observed_at_micros: u64,
    understood_features: u64,
) -> Result<DecodedAgentRecord, LaserError> {
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
            let envelope: AgentEnvelope = decode_named(payload)?;
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
            crate::provenance::provenance_from_headers(headers)?,
            None,
            None,
        ),
    };
    Ok(DecodedAgentRecord {
        provenance,
        envelope,
        content_type,
        signature_context,
        observed_at_micros,
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

// The conversation a record's header names, in either encoding the wire
// allows, when the rest of the record does not decode.
fn original_conversation(headers: &Headers) -> Option<ConversationId> {
    let value = headers.get(&HeaderKey::from_str(CONVERSATION_ID).ok()?)?;
    match value.kind() {
        HeaderKind::Uint128 => {
            Some(laser_wire::agent::ConversationId::from_u128(value.as_uint128().ok()?).into())
        }
        _ => value.as_str().ok()?.parse().ok(),
    }
}

// Synthesize the runtime provenance from an AGDX envelope, so the consumer's
// target filter, dedup, and deadline checks read one shape for both message
// kinds. Agent ids are name strings on both sides, so `source`/`target` map
// straight across, and a name the SDK validator rejects simply drops out.
fn provenance_from_envelope(envelope: &AgentEnvelope) -> Provenance {
    Provenance::builder()
        .conversation_id(envelope.conversation.into())
        .maybe_parent_conversation_id(envelope.parent.map(Into::into))
        .maybe_root_conversation_id(envelope.root.map(Into::into))
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
        .maybe_usage(envelope.usage.map(|usage| LlmUsage {
            input_tokens: Some(usage.input_tokens),
            output_tokens: Some(usage.output_tokens),
            cost_usd: None,
        }))
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
    /// The command operations the handler serves. `None` serves every
    /// operation. A command for another operation is reported as skipped,
    /// so a model or tool record an agent writes for itself never becomes its
    /// own work.
    pub operations: Option<Vec<String>>,
    /// The session configuration the runtime applies: the session lens
    /// handlers read through `AgentCtx::session`, and
    /// [`SessionConfig::fail_on_dead_letter`]. Defaults to
    /// [`SessionConfig::default`].
    pub sessions: Option<SessionConfig>,
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
    ///
    /// Delivery runs on [`ConsumerGroup::consumer`](crate::stream::ConsumerGroup::consumer),
    /// so a server that resolves group policies reads through the group-aware
    /// engine. On `agent.sessions` and `agent.control`, when the server serves
    /// filtered reads and the filter catalog, the group is bound to the
    /// addressee filter `agdx.to In [<agent>, "*"]` before it reads. A group
    /// bound to another filter is refused. On open Apache Iggy the group
    /// stays unbound and records are classified on the client. A capability
    /// probe that established nothing is an error.
    pub async fn run<H>(
        mut self,
        laser: &Laser,
        handler: H,
        ready: oneshot::Sender<()>,
        shutdown: oneshot::Receiver<()>,
    ) -> Result<(), LaserError>
    where
        H: AgentHandler + Sync + Send + 'static,
    {
        let stream = laser.stream_required()?.to_owned();
        let engine = DeliveryEngine::resolve(laser).await?;
        let me = self.agent.as_ref().map(AgentId::wire_id);
        if let Some(me) = &me {
            engine
                .bind_addressee(laser, &stream, &self.topic, self.group.as_str(), me)
                .await?;
        }
        let opener = Opener {
            laser: laser.clone(),
            stream: stream.clone(),
            topic: self.topic.clone(),
            group: self.group.as_str().to_owned(),
            poll_interval: self.poll_interval,
            native: engine.native,
        };
        let consumer = opener.open().await?;

        let deduplicator = self
            .deduplicator
            .take()
            .unwrap_or_else(|| Box::new(SlidingWindow::new(self.dedup_window)));
        if self.warm_dedup {
            warm_dedup_window(laser, &self, deduplicator.as_ref()).await?;
        }
        // Resolve the subscribed stream and topic to their numeric ids once, so
        // every dead-letter capsule can carry a complete `LogPosition` for the
        // poison message without a server round-trip per failure. The consumer has
        // already joined this stream/topic, so a missing id is a should-never
        // happen: warn loudly rather than silently stamping a wrong locator -
        // the partition and offset (the locate-within-topic half) stay correct.
        let stream_ident = Identifier::named(&stream)?;
        let topic_ident = Identifier::named(&self.topic)?;
        let stream_id = laser
            .client()
            .get_stream(&stream_ident)
            .await?
            .map(|details| details.id);
        let topic_details = laser
            .client()
            .get_topic(&stream_ident, &topic_ident)
            .await?;
        let topic_id = topic_details.as_ref().map(|details| details.id);
        let topic_generation = topic_details
            .as_ref()
            .map_or(0, |details| details.created_at.as_micros());
        if stream_id.is_none() || topic_id.is_none() {
            warn!(
                topic = %self.topic,
                "could not resolve the numeric stream/topic id, dead-letter capsules \
                 carry 0 for the unresolved locator half (partition and offset stay correct)"
            );
        }
        let source = match (stream_id, topic_id) {
            (Some(stream_id), Some(topic_id)) => Some(SourceTopic {
                stream_id,
                topic_id,
                generation: topic_generation,
            }),
            _ => None,
        };
        let (stream_id, topic_id) = (stream_id.unwrap_or_default(), topic_id.unwrap_or_default());
        let sessions = laser.sessions_with(self.sessions.clone().unwrap_or_default());

        // The control subscription: a bounded read of `agent.control` loads
        // the requests already on the log, and the follower reads every
        // partition from where it ended, so each instance of the role sees
        // every pause, resume, and cancel request whatever partitions its
        // group assigns it. Handlers read the requests through their session
        // lens. An agent with an id also runs the pause runtime. A stream
        // without the topic has no operator control.
        let control = Arc::new(ControlBook::default());
        let (reconcile, reconcile_requests) = tokio::sync::mpsc::unbounded_channel();
        let mut pause = None;
        let control_follower = if self.topic == AGENT_CONTROL {
            None
        } else {
            match crate::agent::pause::load_control(laser, me.as_ref(), &control).await? {
                Some(feed) => {
                    match (&me, source) {
                        (Some(me), Some(source)) => {
                            pause = Some(Arc::new(PauseRuntime::new(
                                sessions.clone(),
                                me.clone(),
                                Arc::clone(&control),
                                &feed,
                                source,
                                reconcile.clone(),
                            )));
                        }
                        (Some(_), None) => {
                            warn!(topic = %self.topic, "the source topic ids did not resolve, sessions of this agent cannot be paused");
                        }
                        (None, _) => {}
                    }
                    control.set_live(true);
                    let (stop, stopped) = oneshot::channel();
                    let task = tokio::spawn(crate::agent::pause::follow_control(
                        laser.clone(),
                        feed,
                        me.clone(),
                        Arc::clone(&control),
                        pause.is_some().then(|| reconcile.clone()),
                        self.poll_interval,
                        stopped,
                    ));
                    Some((stop, task))
                }
                None => None,
            }
        };
        drop(reconcile);

        let probe_interval = sessions.config().heartbeat_value();
        let reliable = Arc::new(ReliableWorker {
            handler,
            laser: laser.clone(),
            retry: self.retry,
            understood_features: self.understood_features,
            dedup: deduplicator,
            agent: self.agent,
            respond_on: self.respond_on,
            inbox_route: self.inbox_route,
            ack_on_pickup: self.ack_on_pickup,
            stream_id,
            topic_id,
            middleware: self.middleware,
            on_dead_letter: self.on_dead_letter,
            topic: self.topic.clone(),
            operations: self.operations,
            sessions,
            control,
            pause: pause.clone(),
            budget: BudgetGate::default(),
            high_water_fence: dashmap::DashMap::new(),
            fence_last_sweep: std::sync::atomic::AtomicU64::new(0),
            #[cfg(feature = "sign")]
            verifier: self.verifier,
            #[cfg(feature = "sign")]
            signing_key: self.signing_key,
            #[cfg(feature = "sign")]
            verified_records: Mutex::new(DedupWindow::new(VERIFIED_RECORD_WINDOW)),
        });

        // Rebuild the pause state from the log before the first dispatch,
        // then let the pause driver bring each session a request names up
        // to date.
        let pause_driver = match &pause {
            Some(runtime) => {
                if let Err(error) = runtime.recover().await {
                    stop_follower(control_follower).await;
                    return Err(error);
                }
                let (stop, stopped) = oneshot::channel();
                let worker: Arc<dyn Replay> = reliable.clone();
                let task = tokio::spawn(crate::agent::pause::drive(
                    Arc::clone(runtime),
                    worker,
                    reconcile_requests,
                    stopped,
                ));
                Some((stop, task))
            }
            None => None,
        };

        // An aborted or dropped runtime stops its background tasks with it.
        let _background = AbortOnDrop(
            control_follower
                .iter()
                .map(|(_, task)| task.abort_handle())
                .chain(pause_driver.iter().map(|(_, task)| task.abort_handle()))
                .collect(),
        );

        // Joined, dedup-warmed, and recovered: signal readiness. A dropped
        // receiver is fine.
        let _ = ready.send(());
        // A dropped shutdown sender never stops the consumer, only a sent
        // signal does.
        let stop = async {
            if shutdown.await.is_err() {
                std::future::pending::<()>().await;
            }
        }
        .fuse();
        let result = match self.concurrency {
            ConcurrencyPolicy::Serial => {
                run_serial(&opener, consumer, &reliable, stop, self.shutdown_grace).await
            }
            ConcurrencyPolicy::SerialPerPartition { max_partitions } => {
                let limits = LaneLimits {
                    max_partitions: max_partitions.max(1),
                    max_queued_records: self.max_queued_records.clamp(1, u32::MAX as usize),
                    max_queued_bytes: self.max_queued_bytes.clamp(1, u32::MAX as usize),
                    probe_interval,
                };
                run_per_partition(
                    &opener,
                    consumer,
                    Arc::clone(&reliable),
                    limits,
                    stop,
                    self.shutdown_grace,
                )
                .await
            }
        };
        if let Some((stop, mut task)) = pause_driver {
            let _ = stop.send(());
            if tokio::time::timeout(self.shutdown_grace, &mut task)
                .await
                .is_err()
            {
                warn!("the pause driver did not finish within the shutdown grace, aborting it");
                task.abort();
            }
        }
        stop_follower(control_follower).await;
        result
    }
}

// Aborts the runtime's background tasks when the runtime future is dropped,
// so an aborted agent leaves no control follower or pause driver behind.
struct AbortOnDrop(Vec<tokio::task::AbortHandle>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

// Stop the control follower and wait for it to leave.
async fn stop_follower(follower: Option<(oneshot::Sender<()>, tokio::task::JoinHandle<()>)>) {
    if let Some((stop, task)) = follower {
        let _ = stop.send(());
        let _ = task.await;
    }
}

/// How the runtime reads its groups, decided once at spawn from the
/// server's capabilities.
#[derive(Clone, Copy)]
struct DeliveryEngine {
    /// Read groups natively. Only when no group policy can apply.
    native: bool,
    /// The server serves filtered reads and the catalog that binds a group
    /// to its filter.
    filters: bool,
}

impl DeliveryEngine {
    async fn resolve(laser: &Laser) -> Result<Self, LaserError> {
        let mut capabilities = laser.capabilities().await;
        if capabilities.hello == HelloOutcome::Unknown {
            capabilities = laser.refresh_capabilities().await;
        }
        // An uncertain probe, or a server that serves filters without
        // group-aware reads, is an error here, never a native fallback.
        if !crate::stream::transport::policy_aware(&capabilities)? {
            return Ok(Self {
                native: true,
                filters: false,
            });
        }
        // The group-aware engine opens its own connections, which a client
        // brought by the caller cannot. Such a runtime reads natively and
        // binds no filter: delivery stays correct because the runtime still
        // classifies every record by its addressee, it only examines more.
        if laser.connection_string().is_none() {
            if capabilities.managed {
                tracing::warn!(
                    "the agent reads natively because its client was not built from a connection string, so no group filter is bound"
                );
            }
            return Ok(Self {
                native: true,
                filters: false,
            });
        }
        Ok(Self {
            native: false,
            filters: capabilities.filters.native && capabilities.filters.catalog,
        })
    }

    // Bind the role group of `topic` to the addressee filter. Only the
    // session topics carry `agdx.to` on every record, so a filter on any
    // other topic would drop untargeted records.
    async fn bind_addressee(
        &self,
        laser: &Laser,
        stream: &str,
        topic: &str,
        group: &str,
        me: &laser_wire::agent::AgentId,
    ) -> Result<(), LaserError> {
        if !self.filters || (topic != AGENT_SESSIONS && topic != AGENT_CONTROL) {
            return Ok(());
        }
        laser
            .stream(stream)
            .topic(topic)
            .consumer_group(group)
            .create()
            .filter(addressee_filter(me))
            .build()
            .await
            .map(|_| ())
    }
}

/// Opens the runtime's consumer of one topic, again after a recoverable
/// failure.
#[derive(Clone)]
struct Opener {
    laser: Laser,
    stream: String,
    topic: String,
    group: String,
    poll_interval: Duration,
    native: bool,
}

impl Opener {
    async fn open(&self) -> Result<Consumer, LaserError> {
        let builder = self
            .laser
            .stream(&self.stream)
            .topic(&self.topic)
            .consumer_group(&self.group)
            .consumer()
            .commit_policy(CommitPolicy::Disabled)
            .poll_interval(self.poll_interval)
            .create_group(true);
        let builder = if self.native {
            builder.native()
        } else {
            builder
        };
        builder.build().await
    }

    // Leave the failed consumer and open another, after the backoff the
    // failure count asks for.
    async fn reopen(&self, consumer: &mut Consumer, failures: u32) -> Result<(), LaserError> {
        if let Err(error) = consumer.shutdown().await {
            debug!(%error, topic = %self.topic, "the failed consumer did not leave its group cleanly");
        }
        sleep(backoff_for(failures)).await;
        *consumer = self.open().await?;
        Ok(())
    }
}

// The consecutive consumer failures after which a retryable failure is no
// longer treated as transient.
pub(crate) const MAX_CONSECUTIVE_POLL_ERRORS: u32 = 10;

// Whether a consumer failure is worth reopening the consumer for. The
// verdict is `LaserError::is_retryable`, the one TypeScript shares: a stale
// membership, an unavailable catalog, and transient transport failures are,
// a policy conflict, a changed source, and a filter fault are not. A member
// the group no longer knows rejoins.
fn recoverable(error: &LaserError, failures: u32) -> bool {
    let rejoin = matches!(
        error,
        LaserError::Iggy(IggyError::ConsumerGroupMemberNotFound(..))
    );
    (rejoin || error.is_retryable()) && failures < MAX_CONSECUTIVE_POLL_ERRORS
}

// The serial scheduler: one record at a time, committed after it is handled,
// by the task that owns the consumer. A failed dead-letter publish stops the
// consumer with the record uncommitted.
async fn run_serial<H, S>(
    opener: &Opener,
    mut consumer: Consumer,
    worker: &ReliableWorker<H>,
    stop: S,
    shutdown_grace: Duration,
) -> Result<(), LaserError>
where
    H: AgentHandler + Sync + Send + 'static,
    S: std::future::Future<Output = ()> + futures::future::FusedFuture,
{
    tokio::pin!(stop);
    let mut failures = 0u32;
    let mut stopping = false;
    let result = loop {
        if stopping {
            break Ok(());
        }
        let next = tokio::select! {
            () = &mut stop => break Ok(()),
            next = consumer.next() => next,
        };
        let message = match next {
            None => break Ok(()),
            Some(Ok(message)) => message,
            Some(Err(error)) => {
                failures += 1;
                if !recoverable(&error, failures) {
                    break Err(error);
                }
                warn!(%error, attempt = failures, topic = %opener.topic, "agent consumer failed, reopening it");
                if let Err(error) = opener.reopen(&mut consumer, failures).await {
                    break Err(error);
                }
                worker.invalidate_pause();
                continue;
            }
        };
        failures = 0;
        worker.observe_assignment(&consumer);
        let handled = worker.consume(&message);
        tokio::pin!(handled);
        let outcome = tokio::select! {
            outcome = &mut handled => outcome,
            () = &mut stop => {
                stopping = true;
                match tokio::time::timeout(shutdown_grace, &mut handled).await {
                    Ok(outcome) => outcome,
                    // The record is still in flight, so the consumer is not
                    // shut down: leaving would store nothing more, but the
                    // connection close ends the membership instead.
                    Err(_) => return Err(LaserError::Timeout("agent shutdown drain")),
                }
            }
        };
        if let Err(error) = outcome {
            break Err(error);
        }
        if let Err(error) = consumer.commit(&message).await {
            failures += 1;
            if !recoverable(&error, failures) {
                break Err(error);
            }
            warn!(%error, source = %message.position, "committing a handled record failed, reopening the consumer");
            if let Err(error) = opener.reopen(&mut consumer, failures).await {
                break Err(error);
            }
            worker.invalidate_pause();
        }
    };
    if let Err(error) = consumer.shutdown().await {
        warn!(%error, "failed to leave the consumer group on shutdown");
    }
    result
}

/// The per-partition scheduler's bounds.
struct LaneLimits {
    max_partitions: usize,
    max_queued_records: usize,
    max_queued_bytes: usize,
    /// How often the assignment probe drops lanes of partitions this member
    /// no longer reads.
    probe_interval: Duration,
}

// The per-partition scheduler: route each record to a lane keyed by
// partition and commit it once its lane has handled it. Lanes run
// concurrently across partitions, and within one partition a lane is a
// serial queue, so ordering and the committed prefix hold. Commits are issued
// by this task, the sole owner of the consumer, from lane completions. A lane
// whose dead-letter publish fails stops, so its queued successors are never
// committed. A recoverable consumer failure drains every lane, then reopens
// the consumer.
async fn run_per_partition<H, S>(
    opener: &Opener,
    mut consumer: Consumer,
    worker: Arc<ReliableWorker<H>>,
    limits: LaneLimits,
    stop: S,
    shutdown_grace: Duration,
) -> Result<(), LaserError>
where
    H: AgentHandler + Sync + Send + 'static,
    S: std::future::Future<Output = ()> + futures::future::FusedFuture,
{
    tokio::pin!(stop);
    let mut failures = 0u32;
    loop {
        let (outcome, drain) =
            drive_lanes(opener, &mut consumer, &worker, &limits, &mut stop).await;
        if drain.delivered {
            failures = 0;
        }
        // After a consumer failure a commit may be refused for a partition
        // the member lost. Its records are read again by the new owner.
        let tolerate_commits = matches!(outcome, LaneOutcome::Failed(_));
        let drained =
            tokio::time::timeout(shutdown_grace, drain.finish(&consumer, tolerate_commits)).await;
        match outcome {
            LaneOutcome::Stopped => {
                return match drained {
                    Ok(Ok(())) => {
                        // Every lane finished and committed, so the member can
                        // leave its group. On a drain timeout the lanes may
                        // hold unhandled records, and the membership is left
                        // to the connection close instead.
                        if let Err(error) = consumer.shutdown().await {
                            warn!(%error, "failed to leave the consumer group on shutdown");
                        }
                        Ok(())
                    }
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(LaserError::Timeout("agent shutdown drain")),
                };
            }
            LaneOutcome::Fatal(error) => {
                // The failed lane committed nothing after its record, so
                // leaving stores no more than what was committed.
                if matches!(drained, Ok(Ok(())))
                    && let Err(error) = consumer.shutdown().await
                {
                    warn!(%error, "failed to leave the consumer group on shutdown");
                }
                return Err(error);
            }
            LaneOutcome::Failed(error) => {
                if let Ok(Err(drain_error)) | Err(drain_error) =
                    drained.map_err(|_| LaserError::Timeout("agent lane drain"))
                {
                    return Err(drain_error);
                }
                failures += 1;
                if !recoverable(&error, failures) {
                    return Err(error);
                }
                warn!(%error, attempt = failures, topic = %opener.topic, "agent consumer failed, reopening it");
                opener.reopen(&mut consumer, failures).await?;
                worker.invalidate_pause();
            }
        }
    }
}

enum LaneOutcome {
    /// Shutdown was signalled or the stream ended.
    Stopped,
    /// The consumer failed, which may be recoverable.
    Failed(LaserError),
    /// A lane could not dead-letter a record. Nothing after it is committed.
    Fatal(LaserError),
}

struct Lane {
    sender: tokio::sync::mpsc::Sender<QueuedMessage>,
    task: tokio::task::JoinHandle<()>,
}

// The lanes still running after the polling loop ends, and their pending
// completions.
struct LaneDrain {
    lanes: Vec<tokio::task::JoinHandle<()>>,
    completions: tokio::sync::mpsc::Receiver<Result<ConsumerMessage, LaserError>>,
    /// Whether the pass delivered any record, which resets the failure count.
    delivered: bool,
}

impl LaneDrain {
    // Wait for every lane to finish its queue and commit what it handled.
    // The first lane failure stops further commits.
    async fn finish(
        mut self,
        consumer: &Consumer,
        tolerate_commits: bool,
    ) -> Result<(), LaserError> {
        for lane in self.lanes {
            let _ = lane.await;
        }
        while let Some(completion) = self.completions.recv().await {
            let message = completion?;
            match consumer.commit(&message).await {
                Ok(()) => {}
                Err(error) if tolerate_commits => {
                    debug!(%error, source = %message.position, "a handled record could not be committed, it is read again");
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

async fn drive_lanes<H, S>(
    opener: &Opener,
    consumer: &mut Consumer,
    worker: &Arc<ReliableWorker<H>>,
    limits: &LaneLimits,
    stop: &mut std::pin::Pin<&mut S>,
) -> (LaneOutcome, LaneDrain)
where
    H: AgentHandler + Sync + Send + 'static,
    S: std::future::Future<Output = ()> + futures::future::FusedFuture,
{
    let mut lanes: std::collections::HashMap<u32, Lane> = std::collections::HashMap::new();
    // Lanes of revoked partitions finishing their queues. A lane opened for
    // the same partition waits for its predecessor, so a partition is never
    // handled by two lanes at once.
    let mut retiring: std::collections::HashMap<u32, tokio::task::JoinHandle<()>> =
        std::collections::HashMap::new();
    let (commit_tx, mut commit_rx) = tokio::sync::mpsc::channel::<
        Result<ConsumerMessage, LaserError>,
    >(limits.max_partitions * 2);
    let notify = Arc::new(tokio::sync::Notify::new());
    let record_capacity = Arc::new(tokio::sync::Semaphore::new(limits.max_queued_records));
    let byte_capacity = Arc::new(tokio::sync::Semaphore::new(limits.max_queued_bytes));
    let mut probe = tokio::time::interval(limits.probe_interval);
    probe.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    probe.reset();
    let mut delivered = false;

    let outcome = 'polling: loop {
        // Commit every completed record before polling again. The group
        // engine applies these between reads, so no read is cancelled.
        while let Ok(completion) = commit_rx.try_recv() {
            match completion {
                Ok(message) => {
                    if let Err(error) = consumer.commit_handled(&message).await {
                        break 'polling LaneOutcome::Failed(error);
                    }
                }
                Err(error) => break 'polling LaneOutcome::Fatal(error),
            }
        }
        tokio::select! {
            () = &mut *stop => break LaneOutcome::Stopped,
            // Wake to commit a completion at the top of the loop. The value
            // stays in the channel, so a spurious wake is harmless.
            () = notify.notified() => {}
            _ = probe.tick() => {
                drop_revoked(opener, consumer, &mut lanes, &mut retiring).await;
            }
            next = consumer.next() => match next {
                Some(Ok(message)) => {
                    delivered = true;
                    worker.observe_assignment(consumer);
                    let partition = message.partition_id;
                    if !lanes.contains_key(&partition) && lanes.len() >= limits.max_partitions {
                        // Over the lane cap: handle inline rather than drop
                        // the record or spawn an unbounded number of lanes.
                        // No lane holds this partition, so nothing queued
                        // precedes the record.
                        if let Err(error) = worker.consume(&message).await {
                            break 'polling LaneOutcome::Fatal(error);
                        }
                        if let Err(error) = consumer.commit_handled(&message).await {
                            break 'polling LaneOutcome::Failed(error);
                        }
                        continue;
                    }
                    let lane = lanes.entry(partition).or_insert_with(|| {
                        spawn_lane(
                            worker,
                            retiring.remove(&partition),
                            limits.max_queued_records,
                            commit_tx.clone(),
                            Arc::clone(&notify),
                        )
                    });
                    let buffered_bytes = message.payload.len()
                        + message.user_headers.as_ref().map_or(0, bytes::Bytes::len);
                    let Ok(record_permit) = Arc::clone(&record_capacity).acquire_owned().await
                    else {
                        break 'polling LaneOutcome::Fatal(LaserError::HandlerConfig(
                            "record queue closed".to_owned(),
                        ));
                    };
                    let byte_permits = buffered_bytes.clamp(1, limits.max_queued_bytes) as u32;
                    let Ok(byte_permit) = Arc::clone(&byte_capacity)
                        .acquire_many_owned(byte_permits)
                        .await
                    else {
                        break 'polling LaneOutcome::Fatal(LaserError::HandlerConfig(
                            "byte queue closed".to_owned(),
                        ));
                    };
                    let queued = QueuedMessage {
                        message,
                        record_permit,
                        byte_permit,
                    };
                    if lane.sender.send(queued).await.is_err() {
                        // The lane stopped after a failed dead-letter publish.
                        // Its failure is waiting in the completion channel.
                        continue;
                    }
                }
                Some(Err(error)) => {
                    if matches!(error, LaserError::Iggy(IggyError::ConsumerGroupMemberNotFound(..))) {
                        drop_revoked(opener, consumer, &mut lanes, &mut retiring).await;
                    }
                    break LaneOutcome::Failed(error);
                }
                None => break LaneOutcome::Stopped,
            },
        }
    };
    // Dropping the senders lets each lane finish its queue and exit.
    let mut tasks: Vec<_> = lanes.into_values().map(|lane| lane.task).collect();
    tasks.extend(retiring.into_values());
    drop(commit_tx);
    (
        outcome,
        LaneDrain {
            lanes: tasks,
            completions: commit_rx,
            delivered,
        },
    )
}

fn spawn_lane<H>(
    worker: &Arc<ReliableWorker<H>>,
    predecessor: Option<tokio::task::JoinHandle<()>>,
    capacity: usize,
    commits: tokio::sync::mpsc::Sender<Result<ConsumerMessage, LaserError>>,
    notify: Arc<tokio::sync::Notify>,
) -> Lane
where
    H: AgentHandler + Sync + Send + 'static,
{
    let (sender, mut queue) = tokio::sync::mpsc::channel::<QueuedMessage>(capacity);
    let worker = Arc::clone(worker);
    let task = tokio::spawn(async move {
        if let Some(predecessor) = predecessor {
            let _ = predecessor.await;
        }
        while let Some(queued) = queue.recv().await {
            let result = worker.consume(&queued.message).await;
            drop(queued.record_permit);
            drop(queued.byte_permit);
            let failed = result.is_err();
            let _ = commits.send(result.map(|()| queued.message)).await;
            notify.notify_one();
            if failed {
                break;
            }
        }
    });
    Lane { sender, task }
}

// The assignment probe. A lane of a partition this member no longer reads is
// closed: it finishes the records it holds and exits, instead of living until
// shutdown. The group-aware engine knows its own assignment. A native group
// asks the server.
async fn drop_revoked(
    opener: &Opener,
    consumer: &Consumer,
    lanes: &mut std::collections::HashMap<u32, Lane>,
    retiring: &mut std::collections::HashMap<u32, tokio::task::JoinHandle<()>>,
) {
    let assigned = match consumer.assigned_partitions() {
        Some(assigned) => Some(assigned),
        None => opener.assigned_partitions().await,
    };
    let Some(assigned) = assigned else {
        return;
    };
    let revoked: Vec<u32> = lanes
        .keys()
        .filter(|partition| !assigned.contains(partition))
        .copied()
        .collect();
    for partition in revoked {
        if let Some(lane) = lanes.remove(&partition) {
            debug!(
                partition,
                "dropping the lane of a partition no longer assigned"
            );
            drop(lane.sender);
            retiring.insert(partition, lane.task);
        }
    }
    retiring.retain(|_, task| !task.is_finished());
}

impl Opener {
    // The partitions the server assigns this connection in the group, `None`
    // when the probe fails or the connection is not a member.
    async fn assigned_partitions(&self) -> Option<std::collections::BTreeSet<u32>> {
        let client = self.laser.client();
        let me = match client.get_me().await {
            Ok(me) => me.client_id,
            Err(error) => {
                debug!(%error, "the assignment probe could not identify this client");
                return None;
            }
        };
        let group = client
            .get_consumer_group(
                &Identifier::named(&self.stream).ok()?,
                &Identifier::named(&self.topic).ok()?,
                &Identifier::named(&self.group).ok()?,
            )
            .await;
        match group {
            Ok(Some(details)) => details
                .members
                .iter()
                .find(|member| member.id == me)
                .map(|member| member.partitions.iter().copied().collect()),
            Ok(None) => None,
            Err(error) => {
                debug!(%error, "the assignment probe could not read the group");
                None
            }
        }
    }
}

struct QueuedMessage {
    message: ConsumerMessage,
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

// The identity an agentless consumer classifies untargeted records as.
const ANONYMOUS_AGENT: &str = "anonymous";

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
    topic: String,
    operations: Option<Vec<String>>,
    /// The session factory handlers and pickups open their lens from, under
    /// the consumer's session configuration.
    sessions: Sessions,
    /// The control requests the control subscription observed.
    control: Arc<ControlBook>,
    /// The pause runtime, for an agent with an id on a stream with
    /// `agent.control`.
    pause: Option<Arc<PauseRuntime>>,
    /// The budget check before a session's work reaches the handler.
    budget: BudgetGate,
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
    // Tell the pause runtime which partitions the consumer reads, when it
    // knows.
    fn observe_assignment(&self, consumer: &Consumer) {
        if let Some(pause) = &self.pause {
            pause.observe_assignment(consumer.assigned_partitions());
        }
    }

    // The consumer was reopened: the pause runtime reads the lane again
    // before the next dispatch.
    fn invalidate_pause(&self) {
        if let Some(pause) = &self.pause {
            pause.invalidate();
        }
    }

    fn dispatch(&self, message: &AgentMessage) -> Dispatch {
        // A consumer without an agent id accepts records for any addressee, so
        // it classifies each record as its own addressee would.
        let me = match (&self.agent, &message.provenance.target_agent_id) {
            (Some(agent), _) | (None, Some(agent)) => agent.wire_id(),
            (None, None) => ANONYMOUS_AGENT.parse().expect("a valid agent id"),
        };
        match &message.envelope {
            Some(envelope) => {
                let operations: Vec<&str> = self
                    .operations
                    .iter()
                    .flatten()
                    .map(String::as_str)
                    .collect();
                let handled = match self.operations {
                    Some(_) => HandledOperations::Only(&operations),
                    None => HandledOperations::Any,
                };
                classify(envelope, &self.topic, &me, handled)
            }
            None => classify_generic(
                message
                    .provenance
                    .target_agent_id
                    .as_ref()
                    .map(AgentId::as_str),
                message.provenance.causal_parent.is_some(),
                message.provenance.correlation_id.is_some(),
                &self.topic,
                &me,
            ),
        }
    }

    async fn pick_up_submitted(
        &self,
        message: &AgentMessage,
    ) -> Option<crate::agent::SessionLease> {
        let envelope = message.envelope.as_ref()?;
        let submitted = envelope
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get(laser_wire::agent::METADATA_SUBMITTED))
            .is_some_and(|value| matches!(value, laser_wire::query::Value::Bool(true)));
        let agent = self.agent.as_ref()?;
        if !submitted || envelope.kind != AgentKind::Command {
            return None;
        }
        let session = self
            .sessions
            .open(message.provenance.conversation_id)
            .as_agent(agent.wire_id());
        match session.pick_up().await {
            Ok(lease) => Some(lease),
            Err(error) => {
                warn!(%error, source = %message.id, "failed to mark the submitted session working");
                None
            }
        }
    }

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
        let session = Some(message.provenance.conversation_id);
        self.publish_dead_letter(provenance, message.id, capsule, Some(message), session)
            .await
    }

    // Dead-letters a message whose provenance could not be decoded. The original
    // payload rides verbatim so nothing is lost. The capsule keeps the
    // record's own conversation when its header still reads, so the dead
    // letter stays on its session's timeline. A record without one gets a
    // conversation derived from its log position, stable across redeliveries.
    async fn dead_letter_undecodable(
        &self,
        source: MessageId,
        conversation: Option<ConversationId>,
        payload: Vec<u8>,
    ) -> Result<(), LaserError> {
        let capsule = AgentDeadLetter {
            source: self.log_position(source),
            reason: DeadLetterReason::DecodeFailed,
            attempts: 0,
            detail: None,
            payload,
        };
        let position = capsule.source;
        let provenance = Provenance::builder()
            .conversation_id(conversation.unwrap_or_else(|| {
                ConversationId::derive(&format!(
                    "dead-letter\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
                    position.stream_id, position.topic_id, position.partition_id, position.offset
                ))
            }))
            .causal_parent(source)
            .build();
        self.publish_dead_letter(provenance, source, capsule, None, conversation)
            .await
    }

    async fn publish_dead_letter(
        &self,
        provenance: Provenance,
        source: MessageId,
        capsule: AgentDeadLetter,
        message: Option<&AgentMessage>,
        session: Option<ConversationId>,
    ) -> Result<(), LaserError> {
        let reason = capsule.reason;
        let result = self.send_dead_letter(&provenance, &capsule).await;
        if let Err(error) = &result {
            error!(%error, source = %source, ?reason, "failed to publish the dead-letter capsule, leaving the source offset uncommitted");
        }
        if let Some(sink) = &self.on_dead_letter {
            sink.on_dead_letter(message, &capsule, &result).await;
        }
        if result.is_ok()
            && let Some(session) = session
        {
            self.fail_session_on_dead_letter(session, &capsule).await;
        }
        result
    }

    // Under `SessionConfig::fail_on_dead_letter`, a dead-lettered record
    // fails its session. Best effort: the dead letter is already published,
    // so a failed status write is logged and the record still commits.
    async fn fail_session_on_dead_letter(
        &self,
        session: ConversationId,
        capsule: &AgentDeadLetter,
    ) {
        if !self.sessions.config().fails_on_dead_letter() {
            return;
        }
        let Some(agent) = &self.agent else {
            warn!(%session, "a consumer without an agent id cannot fail the session of a dead letter");
            return;
        };
        let source = capsule.source;
        let body = AgentErrorBody {
            code: AgentErrorCode::Internal,
            message: Some(match capsule.detail.as_deref() {
                Some(detail) => format!(
                    "a record of this session was dead-lettered ({:?}): {detail}",
                    capsule.reason
                ),
                None => format!(
                    "a record of this session was dead-lettered ({:?})",
                    capsule.reason
                ),
            }),
            retryable: false,
            detail: Some(BTreeMap::from([
                (
                    "dead_letter_reason".to_owned(),
                    laser_wire::query::Value::Uint(u64::from(capsule.reason.code())),
                ),
                (
                    "source".to_owned(),
                    laser_wire::query::Value::Str(format!(
                        "{}/{}/{}/{}",
                        source.stream_id, source.topic_id, source.partition_id, source.offset
                    )),
                ),
            ])),
        };
        if let Err(error) = self
            .sessions
            .open(session)
            .as_agent(agent.wire_id())
            .fail(body)
            .await
        {
            warn!(%error, %session, "failed to fail the session of a dead-lettered record");
        }
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

impl<H> ReliableWorker<H>
where
    H: AgentHandler + Sync,
{
    // Handle one delivered record. `Err` only when a dead letter, a parking
    // record, or a pause acknowledgment could not be published, so the
    // record must stay uncommitted.
    #[tracing::instrument(target = "laser", level = "debug", skip_all, fields(conversation = tracing::field::Empty, operation = "handle"))]
    async fn consume(&self, received: &ConsumerMessage) -> Result<(), LaserError> {
        self.deliver(received, true).await
    }

    // Handle one record. A live record passes the pause check, a held record
    // replayed after the resume does not.
    async fn deliver(&self, received: &ConsumerMessage, live: bool) -> Result<(), LaserError> {
        let source = received.position;
        let decoded = match AgentMessage::from_consumer(received, self.understood_features) {
            Ok(decoded) => decoded,
            Err((error, payload)) => {
                warn!(%error, source = %source, "undecodable provenance, dead-lettering raw payload");
                let conversation = (!received.headers_malformed)
                    .then(|| original_conversation(&received.headers))
                    .flatten();
                self.dead_letter_undecodable(source, conversation, payload)
                    .await?;
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

        // Dispatch classification, shared with every SDK and the session fold.
        // Only work for an operation this handler serves reaches it. Replies,
        // status, events, control, and records for another agent are skipped
        // and still committed. The author is never a discriminator.
        let dispatch = self.dispatch(&message);
        if dispatch != Dispatch::Work {
            debug!(source = %message.id, dispatch = dispatch.as_str(), "skipping a record that is not work for this agent");
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
                .await?;
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

        // The pause check (D7a), before the fence and dedup gates so a held
        // record keeps its slot for the resume. Work for a paused session is
        // parked on the session lane and committed. A resumed session handles
        // its held records first. The session gate stays held through the
        // handler, so a pause acknowledgment follows the record in flight.
        let _gate = match (&self.pause, live) {
            (Some(pause), true) => {
                let gate = pause.gate(message.provenance.conversation_id).await;
                if pause.hold(self, &message).await? == Hold::Commit {
                    return Ok(());
                }
                Some(gate)
            }
            _ => None,
        };

        // The budget check (D9), on a deployment that indexes sessions: work
        // for a session over its budget ends the session failed with reason
        // `budget`, once, and is committed without reaching the handler.
        if let Some(agent) = &self.agent {
            let session = message.provenance.conversation_id;
            let admitted = self
                .budget
                .admit(
                    &self.sessions,
                    session,
                    received.partition_id,
                    received.current_offset,
                    || {
                        let mut lens = self.sessions.open(session).as_agent(agent.wire_id());
                        lens.control = Some(Arc::clone(&self.control));
                        lens
                    },
                )
                .await;
            if !admitted {
                debug!(source = %message.id, "skipping work for a session over its budget");
                return Ok(());
            }
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
            .await?;
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

        // Picking up the first command of a submitted session marks the
        // session working and keeps it listed in this process's heartbeat
        // while the handler runs.
        let _pickup = self.pick_up_submitted(&message).await;
        let ctx = AgentCtx::new(
            &self.laser,
            &message,
            self.agent.clone(),
            self.respond_on.clone(),
            self.inbox_route.clone(),
            #[cfg(feature = "sign")]
            self.signing_key.clone(),
        )
        .with_request_at(self.log_position(message.id))
        .with_sessions(self.sessions.clone(), Arc::clone(&self.control));
        // Middleware `before_handle` runs once, in order, before the retry loop.
        // A rejection there dead-letters the message without ever running the
        // handler (the auth/gatekeeping use), so it is a non-retryable stop.
        for middleware in &self.middleware {
            if let Err(error) = middleware.before_handle(&message).await {
                warn!(%error, source = %message.id, "middleware rejected message before handling, dead-lettering");
                self.dead_letter(&message, DeadLetterReason::Rejected, 0, &error.to_string())
                    .await?;
                return Ok(());
            }
        }
        let mut attempt = 0;
        loop {
            // A panicking handler becomes a non-retryable error, so the record
            // dead-letters as rejected and the consumer keeps running.
            let result = std::panic::AssertUnwindSafe(self.handler.handle(&message, &ctx))
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| {
                    let reason = panic
                        .downcast_ref::<&str>()
                        .map(|message| (*message).to_owned())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_owned());
                    Err(LaserError::HandlerConfig(format!(
                        "handler panicked: {reason}"
                    )))
                });
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
                        .await?;
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
                        .await?;
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

impl<H> Replay for ReliableWorker<H>
where
    H: AgentHandler + Sync,
{
    fn replay<'a>(
        &'a self,
        received: &'a ConsumerMessage,
    ) -> futures::future::BoxFuture<'a, Result<(), LaserError>> {
        Box::pin(self.deliver(received, false))
    }
}

// Pre-fills the dedup window from each partition so a freshly started consumer
// recognizes duplicates of messages it processed before the restart. Reads only
// up to the group's stored (already-consumed) offset and at most `depth` per
// partition: reading past the stored offset would pre-mark un-consumed messages
// and cause them to be skipped (data loss).
async fn warm_dedup_window(
    laser: &Laser,
    settings: &ReliableConsumer,
    dedup: &dyn Deduplicator,
) -> Result<(), LaserError> {
    let stream = Identifier::named(laser.stream_required()?)?;
    let topic_id = Identifier::named(&settings.topic)?;
    let Some(details) = laser.client().get_topic(&stream, &topic_id).await? else {
        return Ok(());
    };
    let group_consumer =
        iggy::prelude::Consumer::group(Identifier::named(settings.group.as_str())?);
    let reader = iggy::prelude::Consumer::new(Identifier::named("laser-dedup-warmer")?);
    let depth = u64::try_from(settings.dedup_window).unwrap_or(u64::MAX);
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
            if let Some(key) = warm_dedup_key(
                &message,
                settings.agent.as_ref(),
                settings.understood_features,
                #[cfg(feature = "sign")]
                settings.verifier.as_deref(),
            ) {
                dedup.observe(&key).await;
            }
        }
    }
    Ok(())
}

fn warm_dedup_key(
    message: &IggyMessage,
    agent: Option<&AgentId>,
    understood_features: u64,
    #[cfg(feature = "sign")] verifier: Option<&crate::sign::KeyRegistry>,
) -> Option<String> {
    let decoded = decode_agent_record(message, understood_features).ok()?;
    if let (Some(target), Some(agent)) = (&decoded.provenance.target_agent_id, agent)
        && target != agent
    {
        return None;
    }
    #[cfg(feature = "sign")]
    if let Some(registry) = verifier {
        registry
            .verify_observed_at(
                decoded.envelope.as_ref()?,
                decoded.signature_context.as_ref()?,
                decoded.observed_at_micros,
            )
            .ok()?;
    }
    dedup_key(&decoded.provenance)
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
    fn given_an_agdx_command_when_warming_dedup_then_should_restore_the_scoped_key() {
        let envelope = AgentEnvelope::command(
            laser_wire::agent::RecordId::from_u128(3),
            laser_wire::agent::ConversationId::from_u128(1),
            "author".parse().expect("source parses"),
            laser_wire::agent::CorrelationId::from_u128(2),
            b"work".to_vec(),
        )
        .with_operation(laser_wire::agent::OPERATION_CHAT)
        .with_target("worker".parse().expect("target parses"))
        .with_idempotency_key("attempt-1".parse().expect("key parses"));
        let mut headers = BTreeMap::new();
        headers.insert(
            HeaderKey::from_str(AGENT_VERSION).expect("version key"),
            HeaderValue::from(AGENT_OP_VERSION),
        );
        let message = IggyMessage::builder()
            .payload(bytes::Bytes::from(
                encode_named(&envelope).expect("envelope encodes"),
            ))
            .user_headers(headers)
            .build()
            .expect("message builds");
        let worker: AgentId = "worker".parse().expect("worker parses");
        let other: AgentId = "other".parse().expect("other parses");

        assert_eq!(
            warm_dedup_key(
                &message,
                Some(&worker),
                features::NONE,
                #[cfg(feature = "sign")]
                None,
            ),
            Some(format!("author{DEDUP_SCOPE_SEP}attempt-1"))
        );
        assert!(
            warm_dedup_key(
                &message,
                Some(&other),
                features::NONE,
                #[cfg(feature = "sign")]
                None,
            )
            .is_none()
        );
    }

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
    fn given_an_envelope_with_usage_when_decoded_then_should_keep_token_counts() {
        let envelope = AgentEnvelope::command(
            laser_wire::agent::RecordId::from_u128(3),
            laser_wire::agent::ConversationId::from_u128(1),
            "author".parse().expect("agent id parses"),
            laser_wire::agent::CorrelationId::from_u128(2),
            b"work".to_vec(),
        )
        .with_operation(laser_wire::agent::OPERATION_CHAT)
        .with_usage(laser_wire::agent::TokenUsage {
            input_tokens: 13,
            output_tokens: 21,
            ..Default::default()
        });

        let usage = provenance_from_envelope(&envelope)
            .usage
            .expect("usage should be preserved");
        assert_eq!(usage.input_tokens, Some(13));
        assert_eq!(usage.output_tokens, Some(21));
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

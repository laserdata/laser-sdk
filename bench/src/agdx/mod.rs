use std::collections::{BTreeMap, HashMap};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use hdrhistogram::Histogram;
use laser_sdk::agent::{
    Agent, AgentCtx, AgentHandler, AgentMessage, ChunkAssembler, ConcurrencyPolicy, StreamEvent,
};
use laser_sdk::error::LaserError;
use laser_sdk::iggy::prelude::{HeaderKey, HeaderValue, Identifier};
use laser_sdk::laser::Laser;
use laser_sdk::provenance::{AgentTopic, Provenance};
use laser_sdk::stream::{CommitPolicy, Consumer, ConsumerStart, Topic};
use laser_sdk::types::{AgentId as SdkAgentId, ConversationId as SdkConversationId};
use laser_wire::agent::{
    AgentEnvelope, AgentId, AgentKind, ChannelId, ConversationId, CorrelationId, OPERATION_CHAT,
    RecordId, validate,
};
use laser_wire::codes::AGENT_OP_VERSION;
use laser_wire::content::ContentType;
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::headers::{AGENT_VERSION, CONTENT_TYPE, CONVERSATION_ID, HEADER_FRAMING_BYTES};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::BenchError;
use crate::correctness::{CorrectnessOracle, ObservedRecord, OraclePolicy, checksum};
use crate::engine::{
    Dispatch, LoadResult, LoadTimeSeriesPoint, Operation, run_closed_loop_for, run_open_loop_for,
};
use crate::metrics::{ProcessDelta, ProcessSnapshot};
use crate::network::{NetworkByteMeasurement, NetworkByteProbe};
use crate::report::OutcomeCounts;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgdxCase {
    pub payload_bytes: usize,
    pub chunks_per_stream: usize,
    pub operations: u64,
    pub duration_seconds: u64,
    pub concurrency: usize,
    pub partitions: u32,
    pub warmup_seconds: u64,
    pub timeout_millis: u64,
    pub offered_rate: Option<u64>,
    pub spin_dispatch: bool,
    pub max_in_flight: Option<usize>,
}

/// Measured record identifiers start here. Warmup identifiers count up from
/// zero, so replay validation can separate the populations without knowing
/// how many operations a timed warmup issued.
pub(crate) const MEASUREMENT_RECORD_OFFSET: u64 = 1_u64 << 63;

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_agdx_driver
)]
pub enum AgdxDriver {
    AgdxPublish,
    AgdxStream,
    ContextFetch,
    FanOut,
    ReliableConsume,
    RequestReply,
    Scatter,
}

fn invalid_agdx_driver(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported AGDX driver `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AgdxArmSummary {
    pub arm: String,
    pub order: u8,
    pub elapsed_ns: u64,
    pub operations_per_second: f64,
    pub payload_bytes_per_second: f64,
    pub scheduled_p50_ns: u64,
    pub scheduled_p90_ns: u64,
    pub scheduled_p99_ns: u64,
    pub service_p50_ns: u64,
    pub service_p90_ns: u64,
    pub service_p99_ns: u64,
    pub service_p999_ns: Option<u64>,
    pub scheduler_lateness_p99_ns: u64,
    pub primary_p99_ns: u64,
    pub p99_supported: bool,
    pub time_series: Vec<LoadTimeSeriesPoint>,
    pub outcomes: OutcomeCounts,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AgdxByteCount {
    pub arm: String,
    pub body_bytes: usize,
    pub stored_payload_bytes: usize,
    pub user_header_bytes: usize,
    pub record_bytes_before_transport_framing: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AgdxPublishSummary {
    pub bare: AgdxArmSummary,
    pub provenance: AgdxArmSummary,
    pub typed: AgdxArmSummary,
    pub byte_counts: Vec<AgdxByteCount>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AgdxProcessMeasurement {
    pub name: String,
    pub phase: String,
    pub delta: ProcessDelta,
}

pub struct AgdxArmEvidence {
    pub summary: AgdxArmSummary,
    pub load: LoadResult,
    pub processes: Vec<AgdxProcessMeasurement>,
    pub network: Option<NetworkByteMeasurement>,
}

pub struct AgdxPublishEvidence {
    pub bare: AgdxArmEvidence,
    pub provenance: AgdxArmEvidence,
    pub typed: AgdxArmEvidence,
    pub byte_counts: Vec<AgdxByteCount>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AgdxRequestReplySummary {
    pub request_reply: AgdxArmSummary,
    pub handler_entry_samples: u64,
    pub handler_entry_p50_ns: u64,
    pub handler_entry_p99_ns: u64,
    pub configuration: serde_json::Value,
}

pub struct AgdxRequestReplyEvidence {
    pub request_reply: AgdxArmEvidence,
    pub handler_entry: Histogram<u64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct AgdxStreamSummary {
    pub stream: AgdxArmSummary,
    pub time_to_first_chunk_samples: u64,
    pub time_to_first_chunk_p50_ns: u64,
    pub time_to_first_chunk_p99_ns: u64,
    pub inter_chunk_gap_samples: u64,
    pub inter_chunk_gap_p50_ns: u64,
    pub inter_chunk_gap_p99_ns: u64,
    pub completion_samples: u64,
    pub completion_p50_ns: u64,
    pub completion_p99_ns: u64,
    pub configuration: serde_json::Value,
}

pub struct AgdxStreamEvidence {
    pub stream: AgdxArmEvidence,
    pub time_to_first_chunk: Histogram<u64>,
    pub inter_chunk_gap: Histogram<u64>,
    pub completion: Histogram<u64>,
}

impl AgdxStreamEvidence {
    #[must_use]
    pub fn summary(&self) -> AgdxStreamSummary {
        AgdxStreamSummary {
            stream: self.stream.summary.clone(),
            time_to_first_chunk_samples: self.time_to_first_chunk.len(),
            time_to_first_chunk_p50_ns: self.time_to_first_chunk.value_at_quantile(0.5),
            time_to_first_chunk_p99_ns: self.time_to_first_chunk.value_at_quantile(0.99),
            inter_chunk_gap_samples: self.inter_chunk_gap.len(),
            inter_chunk_gap_p50_ns: self.inter_chunk_gap.value_at_quantile(0.5),
            inter_chunk_gap_p99_ns: self.inter_chunk_gap.value_at_quantile(0.99),
            completion_samples: self.completion.len(),
            completion_p50_ns: self.completion.value_at_quantile(0.5),
            completion_p99_ns: self.completion.value_at_quantile(0.99),
            configuration: serde_json::json!({
                "producer": "agdx-stream-unbuffered",
                "consumer": "partition-readers",
                "reassembly": "chunk-assembler",
                "purpose": OPERATION_CHAT,
                "latency_clock": "producer-side-monotonic",
                "connections": 1,
            }),
        }
    }
}

impl AgdxRequestReplyEvidence {
    #[must_use]
    pub fn summary(&self) -> AgdxRequestReplySummary {
        AgdxRequestReplySummary {
            request_reply: self.request_reply.summary.clone(),
            handler_entry_samples: self.handler_entry.len(),
            handler_entry_p50_ns: self.handler_entry.value_at_quantile(0.5),
            handler_entry_p99_ns: self.handler_entry.value_at_quantile(0.99),
            configuration: serde_json::json!({
                "request": "typed-agdx-command",
                "response": "typed-agdx-response",
                "consumer": "reliable-consumer",
                "consumer_poll": "tight",
                "response_readers": "one-per-partition",
                "latency_clock": "producer-side-monotonic",
                "connections": 2,
                "connection_topology": "requester and its response readers share one connection, the worker agent runs on its own dedicated connection",
            }),
        }
    }
}

impl AgdxPublishEvidence {
    #[must_use]
    pub fn summary(&self) -> AgdxPublishSummary {
        AgdxPublishSummary {
            bare: self.bare.summary.clone(),
            provenance: self.provenance.summary.clone(),
            typed: self.typed.summary.clone(),
            byte_counts: self.byte_counts.clone(),
        }
    }
}

#[derive(Clone, Copy)]
enum PublishArm {
    Bare,
    Provenance,
    Typed,
}

struct PublishOperations {
    bare: Operation,
    provenance: Operation,
    typed: Operation,
}

struct PublishTopics<'a> {
    bare: &'a Topic,
    provenance: &'a Topic,
    typed: &'a Topic,
}

const TRACKER_SHARDS: usize = 16;

type PendingRequestShard = tokio::sync::Mutex<HashMap<u64, PendingRequest>>;

struct PendingRequest {
    started: Instant,
    response: tokio::sync::oneshot::Sender<()>,
}

#[derive(Clone)]
struct ObservedResponse {
    id: u64,
    body: Vec<u8>,
    partition: u32,
    offset: u64,
}

#[derive(Clone)]
struct RequestTracker {
    pending: Arc<Vec<PendingRequestShard>>,
    handler_entry: Arc<tokio::sync::Mutex<Histogram<u64>>>,
    responses: Arc<tokio::sync::Mutex<Vec<ObservedResponse>>>,
}

impl Default for RequestTracker {
    fn default() -> Self {
        Self {
            pending: Arc::new(
                (0..TRACKER_SHARDS)
                    .map(|_| PendingRequestShard::default())
                    .collect(),
            ),
            handler_entry: Arc::new(tokio::sync::Mutex::new(
                Histogram::new(3).expect("three significant figures are a valid histogram"),
            )),
            responses: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }
}

fn tracker_shard(id: u64) -> usize {
    usize::try_from(id % TRACKER_SHARDS as u64).expect("shard index fits usize")
}

struct ResponsePump {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), BenchError>>,
}

type PendingStreams = Arc<tokio::sync::Mutex<HashMap<ChannelId, PendingStream>>>;

struct PendingStream {
    id: u64,
    started: Instant,
    last_chunk: Option<Instant>,
    assembler: ChunkAssembler,
    bodies: Vec<Vec<u8>>,
    completion: tokio::sync::oneshot::Sender<()>,
}

#[derive(Clone)]
struct ObservedStream {
    id: u64,
    bodies: Vec<Vec<u8>>,
    partition: u32,
    offset: u64,
    valid_terminal: bool,
    duplicates: u64,
    late: u64,
}

#[derive(Clone, Default)]
struct StreamTracker {
    pending: PendingStreams,
    time_to_first_chunk: Arc<tokio::sync::Mutex<Vec<Duration>>>,
    inter_chunk_gap: Arc<tokio::sync::Mutex<Vec<Duration>>>,
    completion: Arc<tokio::sync::Mutex<Vec<Duration>>>,
    observed: Arc<tokio::sync::Mutex<Vec<ObservedStream>>>,
}

struct StreamPump {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), BenchError>>,
}

struct EchoHandler {
    tracker: RequestTracker,
    source: AgentId,
}

impl AgentHandler for EchoHandler {
    async fn handle(&self, message: &AgentMessage, ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let envelope = message
            .envelope
            .as_ref()
            .ok_or_else(|| LaserError::HandlerConfig("request has no AGDX envelope".to_owned()))?;
        let correlation = envelope.correlation.ok_or_else(|| {
            LaserError::HandlerConfig("request has no AGDX correlation".to_owned())
        })?;
        let id = decode_record_id(&envelope.body)
            .map_err(|error| LaserError::HandlerConfig(error.to_string()))?;
        self.tracker.handler_entry(id).await;
        ctx.laser()
            .agdx(
                AgentTopic::Responses,
                self.source.clone(),
                envelope.conversation,
            )
            .respond(correlation, envelope.body.clone())
            .send()
            .await?;
        Ok(())
    }
}

impl PublishArm {
    fn name(self) -> &'static str {
        match self {
            Self::Bare => "bare-laser-publish",
            Self::Provenance => "provenance-publish",
            Self::Typed => "typed-agdx-command",
        }
    }
}

impl RequestTracker {
    async fn register(&self, id: u64) -> tokio::sync::oneshot::Receiver<()> {
        let (response, receiver) = tokio::sync::oneshot::channel();
        self.pending[tracker_shard(id)].lock().await.insert(
            id,
            PendingRequest {
                started: Instant::now(),
                response,
            },
        );
        receiver
    }

    async fn cancel(&self, id: u64) {
        self.pending[tracker_shard(id)].lock().await.remove(&id);
    }

    async fn handler_entry(&self, id: u64) {
        let entered = Instant::now();
        let started = self.pending[tracker_shard(id)]
            .lock()
            .await
            .get(&id)
            .map(|request| request.started);
        if let Some(started) = started {
            let elapsed = entered.saturating_duration_since(started);
            let _ = self
                .handler_entry
                .lock()
                .await
                .record(duration_ns(elapsed).max(1));
        }
    }

    async fn resolve(&self, response: ObservedResponse) {
        let pending = self.pending[tracker_shard(response.id)]
            .lock()
            .await
            .remove(&response.id);
        if let Some(pending) = pending {
            let _ = pending.response.send(());
        }
        self.responses.lock().await.push(response);
    }

    async fn clear_handler_entries(&self) {
        self.handler_entry.lock().await.reset();
    }

    async fn handler_entry_histogram(&self) -> Result<Histogram<u64>, BenchError> {
        Ok(self.handler_entry.lock().await.clone())
    }
}

impl StreamTracker {
    async fn register(&self, channel: ChannelId, id: u64) -> tokio::sync::oneshot::Receiver<()> {
        let (completion, receiver) = tokio::sync::oneshot::channel();
        self.pending.lock().await.insert(
            channel,
            PendingStream {
                id,
                started: Instant::now(),
                last_chunk: None,
                assembler: ChunkAssembler::new(),
                bodies: Vec::new(),
                completion,
            },
        );
        receiver
    }

    async fn cancel(&self, channel: ChannelId) {
        self.pending.lock().await.remove(&channel);
    }

    async fn observe(
        &self,
        envelope: &AgentEnvelope,
        partition: u32,
        offset: u64,
    ) -> Result<(), BenchError> {
        let channel = envelope
            .channel
            .ok_or_else(|| BenchError::Invalid("AGDX stream envelope has no channel".to_owned()))?;
        let now = Instant::now();
        let mut pending = self.pending.lock().await;
        let state = pending.get_mut(&channel).ok_or_else(|| {
            BenchError::Invalid(format!(
                "AGDX stream channel `{channel}` was not registered"
            ))
        })?;
        let is_body_chunk = envelope.kind == AgentKind::Chunk && !envelope.body.is_empty();
        let (time_to_first_chunk, inter_chunk_gap) = if is_body_chunk {
            if let Some(previous) = state.last_chunk.replace(now) {
                (None, Some(now.duration_since(previous)))
            } else {
                (Some(now.duration_since(state.started)), None)
            }
        } else {
            (None, None)
        };
        let events = state.assembler.feed(envelope);
        let mut valid_terminal = false;
        for event in events {
            match event {
                StreamEvent::Body { payload, .. } => state.bodies.push(payload),
                StreamEvent::Finished { synthetic, .. } => valid_terminal = !synthetic,
                StreamEvent::Failed { .. } => valid_terminal = false,
            }
        }
        let finished = state.assembler.is_finished();
        let completed = finished.then(|| pending.remove(&channel)).flatten();
        drop(pending);
        if let Some(sample) = time_to_first_chunk {
            self.time_to_first_chunk.lock().await.push(sample);
        }
        if let Some(sample) = inter_chunk_gap {
            self.inter_chunk_gap.lock().await.push(sample);
        }
        let Some(completed) = completed else {
            return Ok(());
        };
        self.completion
            .lock()
            .await
            .push(now.duration_since(completed.started));
        self.observed.lock().await.push(ObservedStream {
            id: completed.id,
            bodies: completed.bodies,
            partition,
            offset,
            valid_terminal,
            duplicates: completed.assembler.duplicates_dropped(),
            late: completed.assembler.late_dropped(),
        });
        let _ = completed.completion.send(());
        Ok(())
    }

    async fn clear_measurements(&self) {
        self.time_to_first_chunk.lock().await.clear();
        self.inter_chunk_gap.lock().await.clear();
        self.completion.lock().await.clear();
    }

    async fn histograms(
        &self,
    ) -> Result<(Histogram<u64>, Histogram<u64>, Histogram<u64>), BenchError> {
        Ok((
            durations_histogram(&self.time_to_first_chunk.lock().await)?,
            durations_histogram(&self.inter_chunk_gap.lock().await)?,
            durations_histogram(&self.completion.lock().await)?,
        ))
    }
}

/// Run bare publish, provenance-only publish, and typed AGDX command arms.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, workload failure, or replay failure.
pub async fn run_publish_evidence(
    laser: &Laser,
    case: &AgdxCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<AgdxPublishEvidence, BenchError> {
    validate_case(case)?;
    let stream = format!("bench-agdx-publish-{seed:016x}");
    let bare_topic = laser.stream(&stream).topic("bare");
    let provenance_topic = laser.stream(&stream).topic("provenance");
    let typed_topic = laser.stream(&stream).topic("typed");
    for topic in [&bare_topic, &provenance_topic, &typed_topic] {
        topic
            .ensure(case.partitions)
            .await
            .map_err(|error| sdk_error(&error))?;
    }
    let scoped = laser.with_default_stream(&stream);

    let payload = seeded_payload(case.payload_bytes, seed);
    let source: SdkAgentId = "laser-bench-source"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid benchmark agent id: {error}")))?;
    let bare = bare_operation(
        bare_topic.clone(),
        payload.clone(),
        seed,
        MEASUREMENT_RECORD_OFFSET,
    );
    let provenance = provenance_operation(
        scoped.clone(),
        payload.clone(),
        source.clone(),
        seed,
        MEASUREMENT_RECORD_OFFSET,
    );
    let typed = typed_operation(
        scoped.clone(),
        payload.clone(),
        source.wire_id(),
        seed,
        MEASUREMENT_RECORD_OFFSET,
    );
    let bare_warmup = bare_operation(bare_topic.clone(), payload.clone(), seed, 0);
    let provenance_warmup =
        provenance_operation(scoped.clone(), payload.clone(), source.clone(), seed, 0);
    let typed_warmup = typed_operation(scoped, payload.clone(), source.wire_id(), seed, 0);
    let timeout = Duration::from_millis(case.timeout_millis);

    warmup(case, timeout, bare_warmup).await?;
    warmup(case, timeout, provenance_warmup).await?;
    warmup(case, timeout, typed_warmup).await?;

    let (bare, provenance, typed) = measure_publish_arms(
        case,
        seed,
        timeout,
        PublishOperations {
            bare,
            provenance,
            typed,
        },
        monitored_processes,
    )
    .await?;
    let mut evidence = AgdxPublishEvidence {
        bare,
        provenance,
        typed,
        byte_counts: byte_counts(&payload, &source)?,
    };
    validate_publish_evidence(
        &mut evidence,
        PublishTopics {
            bare: &bare_topic,
            provenance: &provenance_topic,
            typed: &typed_topic,
        },
        &payload,
    )
    .await?;
    Ok(evidence)
}

/// Run typed AGDX command-to-handler and command-to-correlated-response timing.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, workload failure, consumer failure, or response correctness failure.
pub async fn run_request_reply_evidence(
    laser: &Laser,
    connection_string: &str,
    case: &AgdxCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<AgdxRequestReplyEvidence, BenchError> {
    validate_case(case)?;
    let stream = format!("bench-request-reply-{seed:016x}");
    let command_topic = laser
        .stream(&stream)
        .topic(AgentTopic::Commands.topic_string());
    let response_topic = laser
        .stream(&stream)
        .topic(AgentTopic::Responses.topic_string());
    for topic in [&command_topic, &response_topic] {
        topic
            .ensure(case.partitions)
            .await
            .map_err(|error| sdk_error(&error))?;
    }
    let scoped = laser.with_default_stream(&stream);
    let tracker = RequestTracker::default();
    let pumps =
        start_response_pumps(&response_topic, case.partitions, seed, tracker.clone()).await?;
    let worker: SdkAgentId = "laser-bench-worker"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid benchmark worker id: {error}")))?;
    let max_partitions = usize::try_from(case.partitions)
        .map_err(|_| BenchError::Invalid("AGDX partition count exceeds usize".to_owned()))?;
    let worker_laser = Laser::connect(connection_string)
        .await
        .map_err(|error| sdk_error(&error))?
        .with_default_stream(&stream);
    let mut agent = Agent::builder()
        .id(worker.clone())
        .listen_on(AgentTopic::Commands)
        .handler(EchoHandler {
            tracker: tracker.clone(),
            source: worker.wire_id(),
        })
        .poll_interval(Duration::ZERO)
        .concurrency(ConcurrencyPolicy::SerialPerPartition { max_partitions })
        .build()
        .spawn(worker_laser);
    let run = async {
        agent.ready().await.map_err(|error| sdk_error(&error))?;
        let payload = seeded_payload(case.payload_bytes, seed);
        let source: AgentId = "laser-bench-client".parse().map_err(|error| {
            BenchError::Invalid(format!("invalid benchmark source id: {error}"))
        })?;
        let timeout = Duration::from_millis(case.timeout_millis);
        let warmup_operation = request_reply_operation(
            scoped.clone(),
            payload.clone(),
            source.clone(),
            tracker.clone(),
            seed,
            0,
        );
        warmup(case, timeout, warmup_operation).await?;
        tracker.clear_handler_entries().await;
        let operation = request_reply_operation(
            scoped,
            payload.clone(),
            source,
            tracker.clone(),
            seed,
            MEASUREMENT_RECORD_OFFSET,
        );
        let mut evidence = measured_arm(
            "typed-agdx-request-reply",
            1,
            case,
            timeout,
            operation,
            monitored_processes,
        )
        .await?;
        let expected = expected_ids(&evidence.load);
        let explained = explained_ids(&evidence.load);
        let correctness = validate_responses(&tracker, &payload, &expected, &explained).await;
        apply_correctness(&mut evidence.summary.outcomes, &correctness);
        Ok::<_, BenchError>((evidence, tracker.handler_entry_histogram().await?))
    }
    .await;
    let agent_shutdown = agent.shutdown().await.map_err(|error| sdk_error(&error));
    let pump_shutdown = stop_response_pumps(pumps).await;
    let (request_reply, handler_entry) = run?;
    agent_shutdown?;
    pump_shutdown?;
    Ok(AgdxRequestReplyEvidence {
        request_reply,
        handler_entry,
    })
}

/// Run typed AGDX chunk streams through live partition readers and `ChunkAssembler`.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, workload failure, consumer failure, or stream correctness failure.
pub async fn run_stream_evidence(
    laser: &Laser,
    case: &AgdxCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<AgdxStreamEvidence, BenchError> {
    validate_case(case)?;
    if case.payload_bytes < 2 * size_of::<u64>() {
        return Err(BenchError::Invalid(
            "AGDX stream payload must fit record and chunk IDs".to_owned(),
        ));
    }
    let stream_name = format!("bench-agdx-stream-{seed:016x}");
    let topic = laser
        .stream(&stream_name)
        .topic(AgentTopic::LlmIo.topic_string());
    topic
        .ensure(case.partitions)
        .await
        .map_err(|error| sdk_error(&error))?;
    let scoped = laser.with_default_stream(&stream_name);
    let tracker = StreamTracker::default();
    let pumps = start_stream_pumps(&topic, case.partitions, tracker.clone()).await?;
    let run = async {
        let payload = seeded_payload(case.payload_bytes, seed);
        let source: AgentId = "laser-bench-streamer"
            .parse()
            .map_err(|error| BenchError::Invalid(format!("invalid stream source id: {error}")))?;
        let timeout = Duration::from_millis(case.timeout_millis);
        let warmup_operation = stream_operation(
            scoped.clone(),
            payload.clone(),
            source.clone(),
            tracker.clone(),
            case.chunks_per_stream,
            seed,
            0,
        );
        warmup(case, timeout, warmup_operation).await?;
        tracker.clear_measurements().await;
        let operation = stream_operation(
            scoped,
            payload.clone(),
            source,
            tracker.clone(),
            case.chunks_per_stream,
            seed,
            MEASUREMENT_RECORD_OFFSET,
        );
        let mut evidence = measured_arm(
            "typed-agdx-stream",
            1,
            case,
            timeout,
            operation,
            monitored_processes,
        )
        .await?;
        let successful_bytes = evidence
            .load
            .outcomes
            .successful
            .saturating_mul(u64::try_from(case.chunks_per_stream).unwrap_or(u64::MAX))
            .saturating_mul(u64::try_from(case.payload_bytes).unwrap_or(u64::MAX));
        evidence.summary.payload_bytes_per_second =
            per_second(successful_bytes, evidence.load.elapsed);
        let expected = expected_ids(&evidence.load);
        let explained = explained_ids(&evidence.load);
        let correctness = validate_streams(
            &tracker,
            &payload,
            case.chunks_per_stream,
            &expected,
            &explained,
        )
        .await?;
        apply_correctness(&mut evidence.summary.outcomes, &correctness);
        let (time_to_first_chunk, inter_chunk_gap, completion) = tracker.histograms().await?;
        Ok::<_, BenchError>(AgdxStreamEvidence {
            stream: evidence,
            time_to_first_chunk,
            inter_chunk_gap,
            completion,
        })
    }
    .await;
    let shutdown = stop_stream_pumps(pumps).await;
    let evidence = run?;
    shutdown?;
    Ok(evidence)
}

fn stream_operation(
    laser: Laser,
    payload: Bytes,
    source: AgentId,
    tracker: StreamTracker,
    chunks: usize,
    seed: u64,
    id_offset: u64,
) -> Operation {
    Arc::new(move |sequence| {
        let laser = laser.clone();
        let payload = payload.clone();
        let source = source.clone();
        let tracker = tracker.clone();
        Box::pin(async move {
            let id = id_offset
                .checked_add(sequence)
                .ok_or_else(|| "AGDX stream ID exceeds u64".to_owned())?;
            let mut stream = laser
                .agdx(AgentTopic::LlmIo, source, conversation(seed, id))
                .stream(correlation(seed, id), OPERATION_CHAT);
            let channel = stream.channel();
            let completion = tracker.register(channel, id).await;
            for chunk in 0..chunks {
                if let Err(error) = stream.write(chunk_payload(&payload, id, chunk)?).await {
                    tracker.cancel(channel).await;
                    return Err(error.to_string());
                }
            }
            if let Err(error) = stream.finish("stop", None).await {
                tracker.cancel(channel).await;
                return Err(error.to_string());
            }
            completion
                .await
                .map_err(|_| "AGDX stream tracker stopped".to_owned())
        })
    })
}

async fn start_stream_pumps(
    topic: &Topic,
    partitions: u32,
    tracker: StreamTracker,
) -> Result<Vec<StreamPump>, BenchError> {
    let mut pumps = Vec::with_capacity(partitions as usize);
    for partition in 0..partitions {
        let consumer = topic
            .consumer(format!("laser-bench-stream-{partition}"), partition)
            .batch_length(128)
            .without_poll_interval()
            .start_at(ConsumerStart::Offset(0))
            .commit_policy(CommitPolicy::Disabled)
            .allow_replay()
            .build()
            .await
            .map_err(|error| sdk_error(&error))?;
        pumps.push(spawn_stream_pump(consumer, tracker.clone()));
    }
    Ok(pumps)
}

fn spawn_stream_pump(mut consumer: Consumer, tracker: StreamTracker) -> StreamPump {
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                message = consumer.next() => {
                    let message = message
                        .ok_or_else(|| BenchError::Invalid("AGDX stream consumer ended".to_owned()))?
                        .map_err(|error| sdk_error(&error))?;
                    let envelope: AgentEnvelope = decode_named(&message.payload).map_err(|error| {
                        BenchError::Invalid(format!("AGDX stream decode failed: {error}"))
                    })?;
                    validate(&envelope).map_err(|error| {
                        BenchError::Invalid(format!("AGDX stream validation failed: {error}"))
                    })?;
                    if !matches!(envelope.kind, AgentKind::Chunk | AgentKind::Error) {
                        return Err(BenchError::Invalid(
                            "AGDX stream topic contained a non-stream envelope".to_owned(),
                        ));
                    }
                    tracker
                        .observe(&envelope, message.partition_id, message.position.offset)
                        .await?;
                }
            }
        }
        Ok(())
    });
    StreamPump { stop, task }
}

async fn stop_stream_pumps(pumps: Vec<StreamPump>) -> Result<(), BenchError> {
    for (index, pump) in pumps.into_iter().enumerate() {
        let StreamPump { stop, mut task } = pump;
        let _ = stop.send(());
        if let Ok(result) = tokio::time::timeout(Duration::from_secs(1), &mut task).await {
            result.map_err(|error| {
                BenchError::Invalid(format!("AGDX stream pump {index} failed: {error}"))
            })??;
            continue;
        }
        task.abort();
        match task.await {
            Ok(result) => result?,
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                return Err(BenchError::Invalid(format!(
                    "AGDX stream pump {index} failed during abort: {error}"
                )));
            }
        }
    }
    Ok(())
}

async fn validate_streams(
    tracker: &StreamTracker,
    payload: &Bytes,
    chunks: usize,
    expected_ids: &[u64],
    explained_ids: &[u64],
) -> Result<crate::correctness::CorrectnessSummary, BenchError> {
    let policy = OraclePolicy {
        allow_duplicates: false,
    };
    let explained = explained_ids
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let mut oracle = CorrectnessOracle::new(expected_ids.iter().copied(), policy)
        .with_explained(explained_ids.iter().copied());
    let observed = tracker.observed.lock().await;
    let mut stream_defects = Vec::new();
    for stream in observed.iter() {
        if stream.id < MEASUREMENT_RECORD_OFFSET || explained.contains(&stream.id) {
            continue;
        }
        let expected = (0..chunks)
            .map(|chunk| chunk_payload(payload, stream.id, chunk))
            .collect::<Result<Vec<_>, _>>()
            .map_err(BenchError::Invalid)?
            .concat();
        let actual = stream.bodies.concat();
        oracle.observe(ObservedRecord {
            id: stream.id,
            partition: stream.partition,
            partition_sequence: stream.offset,
            payload: &actual,
            checksum: checksum(&expected),
        });
        if !stream.valid_terminal || stream.late != 0 {
            stream_defects.push(stream.id);
        }
    }
    let mut summary = oracle.finish();
    summary.checksum_failures.extend(stream_defects);
    for stream in observed.iter() {
        if stream.id < MEASUREMENT_RECORD_OFFSET || explained.contains(&stream.id) {
            continue;
        }
        summary.duplicates.extend(std::iter::repeat_n(
            stream.id,
            usize::try_from(stream.duplicates).unwrap_or(usize::MAX),
        ));
    }
    Ok(summary)
}

fn request_reply_operation(
    laser: Laser,
    payload: Bytes,
    source: AgentId,
    tracker: RequestTracker,
    seed: u64,
    id_offset: u64,
) -> Operation {
    Arc::new(move |sequence| {
        let laser = laser.clone();
        let payload = payload.clone();
        let source = source.clone();
        let tracker = tracker.clone();
        Box::pin(async move {
            let id = id_offset
                .checked_add(sequence)
                .ok_or_else(|| "AGDX request ID exceeds u64".to_owned())?;
            let receiver = tracker.register(id).await;
            let publish_result = laser
                .agdx(AgentTopic::Commands, source, conversation(seed, id))
                .command(correlation(seed, id), record_payload(&payload, id)?)
                .send()
                .await;
            if let Err(error) = publish_result {
                tracker.cancel(id).await;
                return Err(error.to_string());
            }
            receiver
                .await
                .map_err(|_| "AGDX response tracker stopped".to_owned())
        })
    })
}

async fn start_response_pumps(
    topic: &Topic,
    partitions: u32,
    seed: u64,
    tracker: RequestTracker,
) -> Result<Vec<ResponsePump>, BenchError> {
    let mut pumps = Vec::with_capacity(partitions as usize);
    for partition in 0..partitions {
        let consumer = topic
            .consumer(format!("laser-bench-response-{partition}"), partition)
            .batch_length(128)
            .without_poll_interval()
            .start_at(ConsumerStart::Offset(0))
            .commit_policy(CommitPolicy::Disabled)
            .allow_replay()
            .build()
            .await
            .map_err(|error| sdk_error(&error))?;
        pumps.push(spawn_response_pump(consumer, seed, tracker.clone()));
    }
    Ok(pumps)
}

fn spawn_response_pump(mut consumer: Consumer, seed: u64, tracker: RequestTracker) -> ResponsePump {
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                message = consumer.next() => {
                    let message = message
                        .ok_or_else(|| BenchError::Invalid("AGDX response consumer ended".to_owned()))?
                        .map_err(|error| sdk_error(&error))?;
                    let envelope: AgentEnvelope = decode_named(&message.payload).map_err(|error| {
                        BenchError::Invalid(format!("AGDX response decode failed: {error}"))
                    })?;
                    validate(&envelope).map_err(|error| {
                        BenchError::Invalid(format!("AGDX response validation failed: {error}"))
                    })?;
                    if envelope.kind != AgentKind::Response {
                        return Err(BenchError::Invalid(
                            "AGDX response topic contained a non-response envelope".to_owned(),
                        ));
                    }
                    let id = decode_record_id(&envelope.body)?;
                    if envelope.correlation != Some(correlation(seed, id)) {
                        return Err(BenchError::Invalid(
                            "AGDX response correlation did not match its request".to_owned(),
                        ));
                    }
                    tracker
                        .resolve(ObservedResponse {
                            id,
                            body: envelope.body,
                            partition: message.partition_id,
                            offset: message.position.offset,
                        })
                        .await;
                }
            }
        }
        Ok(())
    });
    ResponsePump { stop, task }
}

async fn stop_response_pumps(pumps: Vec<ResponsePump>) -> Result<(), BenchError> {
    for (index, pump) in pumps.into_iter().enumerate() {
        let ResponsePump { stop, mut task } = pump;
        let _ = stop.send(());
        if let Ok(result) = tokio::time::timeout(Duration::from_secs(1), &mut task).await {
            result.map_err(|error| {
                BenchError::Invalid(format!("AGDX response pump {index} failed: {error}"))
            })??;
            continue;
        }
        task.abort();
        match task.await {
            Ok(result) => result?,
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                return Err(BenchError::Invalid(format!(
                    "AGDX response pump {index} failed during abort: {error}"
                )));
            }
        }
    }
    Ok(())
}

async fn validate_responses(
    tracker: &RequestTracker,
    payload: &Bytes,
    expected_ids: &[u64],
    explained_ids: &[u64],
) -> crate::correctness::CorrectnessSummary {
    let policy = OraclePolicy {
        allow_duplicates: false,
    };
    let mut oracle = CorrectnessOracle::new(expected_ids.iter().copied(), policy)
        .with_explained(explained_ids.iter().copied());
    for response in tracker.responses.lock().await.iter() {
        if response.id < MEASUREMENT_RECORD_OFFSET {
            continue;
        }
        let expected = record_payload(payload, response.id).unwrap_or_default();
        oracle.observe(ObservedRecord {
            id: response.id,
            partition: response.partition,
            partition_sequence: response.offset,
            payload: &response.body,
            checksum: checksum(&expected),
        });
    }
    oracle.finish()
}

async fn measure_publish_arms(
    case: &AgdxCase,
    seed: u64,
    timeout: Duration,
    operations: PublishOperations,
    monitored_processes: &[(String, u32)],
) -> Result<(AgdxArmEvidence, AgdxArmEvidence, AgdxArmEvidence), BenchError> {
    let mut bare = None;
    let mut provenance = None;
    let mut typed = None;
    for (index, arm) in publish_order(seed).into_iter().enumerate() {
        let operation = match arm {
            PublishArm::Bare => Arc::clone(&operations.bare),
            PublishArm::Provenance => Arc::clone(&operations.provenance),
            PublishArm::Typed => Arc::clone(&operations.typed),
        };
        let evidence = measured_arm(
            arm.name(),
            u8::try_from(index + 1)
                .map_err(|_| BenchError::Invalid("AGDX arm order exceeds u8".to_owned()))?,
            case,
            timeout,
            operation,
            monitored_processes,
        )
        .await?;
        match arm {
            PublishArm::Bare => bare = Some(evidence),
            PublishArm::Provenance => provenance = Some(evidence),
            PublishArm::Typed => typed = Some(evidence),
        }
    }
    Ok((
        bare.ok_or_else(|| BenchError::Invalid("bare AGDX publish arm did not run".to_owned()))?,
        provenance
            .ok_or_else(|| BenchError::Invalid("provenance publish arm did not run".to_owned()))?,
        typed
            .ok_or_else(|| BenchError::Invalid("typed AGDX publish arm did not run".to_owned()))?,
    ))
}

async fn validate_publish_evidence(
    evidence: &mut AgdxPublishEvidence,
    topics: PublishTopics<'_>,
    payload: &Bytes,
) -> Result<(), BenchError> {
    for (arm, topic, result) in [
        (PublishArm::Bare, topics.bare, &mut evidence.bare),
        (
            PublishArm::Provenance,
            topics.provenance,
            &mut evidence.provenance,
        ),
        (PublishArm::Typed, topics.typed, &mut evidence.typed),
    ] {
        let expected = expected_ids(&result.load);
        let explained = explained_ids(&result.load);
        let correctness = validate_topic(topic, payload, &expected, &explained, arm).await?;
        apply_correctness(&mut result.summary.outcomes, &correctness);
    }
    Ok(())
}

fn bare_operation(topic: Topic, payload: Bytes, seed: u64, id_offset: u64) -> Operation {
    Arc::new(move |sequence| {
        let topic = topic.clone();
        let payload = payload.clone();
        Box::pin(async move {
            let id = id_offset
                .checked_add(sequence)
                .ok_or_else(|| "AGDX record ID exceeds u64".to_owned())?;
            let conversation = conversation(seed, id);
            topic
                .send(
                    record_payload(&payload, id)?,
                    BTreeMap::new(),
                    Some(&conversation.to_string()),
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    })
}

fn provenance_operation(
    laser: Laser,
    payload: Bytes,
    source: SdkAgentId,
    seed: u64,
    id_offset: u64,
) -> Operation {
    Arc::new(move |sequence| {
        let laser = laser.clone();
        let payload = payload.clone();
        let source = source.clone();
        Box::pin(async move {
            let id = id_offset
                .checked_add(sequence)
                .ok_or_else(|| "AGDX record ID exceeds u64".to_owned())?;
            let conversation: SdkConversationId = conversation(seed, id).into();
            let provenance = Provenance::builder()
                .conversation_id(conversation)
                .agent(source)
                .correlation_id(correlation(seed, id).to_string())
                .build();
            let topic = Identifier::named("provenance").map_err(|error| error.to_string())?;
            laser
                .send_agent(
                    AgentTopic::Custom(&topic),
                    record_payload(&payload, id)?,
                    &provenance,
                )
                .await
                .map_err(|error| error.to_string())
        })
    })
}

fn typed_operation(
    laser: Laser,
    payload: Bytes,
    source: AgentId,
    seed: u64,
    id_offset: u64,
) -> Operation {
    Arc::new(move |sequence| {
        let laser = laser.clone();
        let payload = payload.clone();
        let source = source.clone();
        Box::pin(async move {
            let id = id_offset
                .checked_add(sequence)
                .ok_or_else(|| "AGDX record ID exceeds u64".to_owned())?;
            let topic = Identifier::named("typed").map_err(|error| error.to_string())?;
            laser
                .agdx(AgentTopic::Custom(&topic), source, conversation(seed, id))
                .command(correlation(seed, id), record_payload(&payload, id)?)
                .send()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    })
}

pub(crate) async fn warmup(
    case: &AgdxCase,
    timeout: Duration,
    operation: Operation,
) -> Result<(), BenchError> {
    let result = run_closed_loop_for(
        Duration::from_secs(case.warmup_seconds.max(1)),
        case.concurrency,
        timeout,
        operation,
    )
    .await?;
    if result.outcomes.successful == 0
        || result.outcomes.failed != 0
        || result.outcomes.timed_out != 0
    {
        let first_error = result
            .samples
            .iter()
            .find_map(|sample| sample.error.as_deref())
            .unwrap_or("no explicit error");
        return Err(BenchError::Invalid(format!(
            "AGDX publish warmup did not complete successfully: failed={}, timed_out={}, first_error={first_error}",
            result.outcomes.failed, result.outcomes.timed_out
        )));
    }
    Ok(())
}

pub(crate) async fn measured_arm(
    name: &str,
    order: u8,
    case: &AgdxCase,
    timeout: Duration,
    operation: Operation,
    monitored_processes: &[(String, u32)],
) -> Result<AgdxArmEvidence, BenchError> {
    let before = capture_processes(monitored_processes)?;
    let load = match case.offered_rate {
        Some(rate) => {
            run_open_loop_for(
                Duration::from_secs(case.duration_seconds),
                rate,
                case.max_in_flight.unwrap_or(case.concurrency),
                timeout,
                agdx_dispatch(case),
                operation,
            )
            .await?
        }
        None => {
            run_closed_loop_for(
                Duration::from_secs(case.duration_seconds),
                case.concurrency,
                timeout,
                operation,
            )
            .await?
        }
    };
    let processes = finish_processes(before, "measurement")?;
    Ok(AgdxArmEvidence {
        summary: summarize(name, order, &load, case),
        load,
        processes,
        network: None,
    })
}

pub(crate) async fn measured_arm_with_network(
    name: &str,
    order: u8,
    case: &AgdxCase,
    timeout: Duration,
    operation: Operation,
    monitored_processes: &[(String, u32)],
    server_port: u16,
) -> Result<AgdxArmEvidence, BenchError> {
    let before = capture_processes(monitored_processes)?;
    let network = NetworkByteProbe::start(server_port);
    let load = match case.offered_rate {
        Some(rate) => {
            run_open_loop_for(
                Duration::from_secs(case.duration_seconds),
                rate,
                case.max_in_flight.unwrap_or(case.concurrency),
                timeout,
                agdx_dispatch(case),
                operation,
            )
            .await?
        }
        None => {
            run_closed_loop_for(
                Duration::from_secs(case.duration_seconds),
                case.concurrency,
                timeout,
                operation,
            )
            .await?
        }
    };
    let network = network.finish();
    let processes = finish_processes(before, "measurement")?;
    Ok(AgdxArmEvidence {
        summary: summarize(name, order, &load, case),
        load,
        processes,
        network: Some(network),
    })
}

fn summarize(name: &str, order: u8, load: &LoadResult, case: &AgdxCase) -> AgdxArmSummary {
    let successful = load.outcomes.successful;
    let operations_per_second = per_second(successful, load.elapsed);
    let successful_bytes =
        successful.saturating_mul(u64::try_from(case.payload_bytes).unwrap_or(u64::MAX));
    AgdxArmSummary {
        arm: name.to_owned(),
        order,
        elapsed_ns: duration_ns(load.elapsed),
        operations_per_second,
        payload_bytes_per_second: per_second(successful_bytes, load.elapsed),
        scheduled_p50_ns: load.scheduled_response.value_at_quantile(0.5),
        scheduled_p90_ns: load.scheduled_response.value_at_quantile(0.9),
        scheduled_p99_ns: load.scheduled_response.value_at_quantile(0.99),
        service_p50_ns: load.service.value_at_quantile(0.5),
        service_p90_ns: load.service.value_at_quantile(0.9),
        service_p99_ns: load.service.value_at_quantile(0.99),
        service_p999_ns: (load.outcomes.successful >= 100_000)
            .then(|| load.service.value_at_quantile(0.999)),
        scheduler_lateness_p99_ns: load.scheduler_lateness.value_at_quantile(0.99),
        primary_p99_ns: if case.offered_rate.is_some() {
            load.scheduled_response.value_at_quantile(0.99)
        } else {
            load.service.value_at_quantile(0.99)
        },
        p99_supported: load.outcomes.successful >= 10_000,
        time_series: load.time_series.clone(),
        outcomes: load.outcomes.clone(),
    }
}

async fn validate_topic(
    topic: &Topic,
    payload: &Bytes,
    expected_ids: &[u64],
    explained_ids: &[u64],
    arm: PublishArm,
) -> Result<crate::correctness::CorrectnessSummary, BenchError> {
    let policy = OraclePolicy {
        allow_duplicates: false,
    };
    let mut oracle = CorrectnessOracle::new(expected_ids.iter().copied(), policy)
        .with_explained(explained_ids.iter().copied());
    let mut cursor = topic
        .replay()
        .map_err(|error| sdk_error(&error))?
        .batch(1_000);
    loop {
        let records = cursor.poll().await.map_err(|error| sdk_error(&error))?;
        if records.is_empty() {
            break;
        }
        for record in records {
            let body = match arm {
                PublishArm::Bare | PublishArm::Provenance => record.payload,
                PublishArm::Typed => {
                    let envelope: AgentEnvelope =
                        decode_named(&record.payload).map_err(|error| {
                            BenchError::Invalid(format!("typed AGDX replay decode failed: {error}"))
                        })?;
                    validate(&envelope).map_err(|error| {
                        BenchError::Invalid(format!("typed AGDX replay validation failed: {error}"))
                    })?;
                    if envelope.kind != AgentKind::Command {
                        return Err(BenchError::Invalid(
                            "typed AGDX replay contained a non-command envelope".to_owned(),
                        ));
                    }
                    envelope.body
                }
            };
            let id = decode_record_id(&body)?;
            if id < MEASUREMENT_RECORD_OFFSET {
                continue;
            }
            let expected = record_payload(payload, id).map_err(BenchError::Invalid)?;
            oracle.observe(ObservedRecord {
                id,
                partition: record.id.partition_id,
                partition_sequence: record.id.offset,
                payload: &body,
                checksum: checksum(&expected),
            });
        }
    }
    Ok(oracle.finish())
}

fn expected_ids(load: &LoadResult) -> Vec<u64> {
    load.successful_sequences
        .iter()
        .filter_map(|sequence| MEASUREMENT_RECORD_OFFSET.checked_add(*sequence))
        .collect()
}

fn explained_ids(load: &LoadResult) -> Vec<u64> {
    load.samples
        .iter()
        .filter_map(|sample| MEASUREMENT_RECORD_OFFSET.checked_add(sample.sequence))
        .collect()
}

fn byte_counts(payload: &Bytes, source: &SdkAgentId) -> Result<Vec<AgdxByteCount>, BenchError> {
    let conversation = conversation(1, 1);
    let correlation = correlation(1, 1);
    let sdk_conversation: SdkConversationId = conversation.into();
    let provenance = Provenance::builder()
        .conversation_id(sdk_conversation)
        .agent(source.clone())
        .correlation_id(correlation.to_string())
        .build();
    let provenance_headers = BTreeMap::<HeaderKey, HeaderValue>::try_from(&provenance)
        .map_err(|error| BenchError::Invalid(format!("provenance byte count failed: {error}")))?;
    let envelope = AgentEnvelope::command(
        RecordId::from_u128(1),
        conversation,
        source.wire_id(),
        correlation,
        payload.to_vec(),
    );
    validate(&envelope).map_err(|error| {
        BenchError::Invalid(format!("AGDX byte-count envelope is invalid: {error}"))
    })?;
    let encoded = encode_named(&envelope)
        .map_err(|error| BenchError::Invalid(format!("AGDX byte count encode failed: {error}")))?;
    let typed_headers = agdx_headers(&envelope, ContentType::Raw)?;
    Ok(vec![
        byte_count("bare-laser-publish", payload.len(), payload.len(), 0),
        byte_count(
            "provenance-publish",
            payload.len(),
            payload.len(),
            header_bytes(&provenance_headers),
        ),
        byte_count(
            "typed-agdx-command",
            payload.len(),
            encoded.len(),
            header_bytes(&typed_headers),
        ),
    ])
}

fn byte_count(
    arm: &str,
    body_bytes: usize,
    stored_payload_bytes: usize,
    user_header_bytes: usize,
) -> AgdxByteCount {
    AgdxByteCount {
        arm: arm.to_owned(),
        body_bytes,
        stored_payload_bytes,
        user_header_bytes,
        record_bytes_before_transport_framing: stored_payload_bytes
            .saturating_add(user_header_bytes),
    }
}

fn agdx_headers(
    envelope: &AgentEnvelope,
    content_type: ContentType,
) -> Result<BTreeMap<HeaderKey, HeaderValue>, BenchError> {
    let mut headers = BTreeMap::new();
    headers.insert(
        HeaderKey::from_str(AGENT_VERSION)
            .map_err(|error| BenchError::Invalid(error.to_string()))?,
        HeaderValue::from(AGENT_OP_VERSION),
    );
    headers.insert(
        HeaderKey::from_str(CONTENT_TYPE)
            .map_err(|error| BenchError::Invalid(error.to_string()))?,
        HeaderValue::from(content_type.code()),
    );
    headers.insert(
        HeaderKey::from_str(CONVERSATION_ID)
            .map_err(|error| BenchError::Invalid(error.to_string()))?,
        HeaderValue::from(envelope.conversation.as_u128()),
    );
    Ok(headers)
}

fn header_bytes(headers: &BTreeMap<HeaderKey, HeaderValue>) -> usize {
    headers
        .iter()
        .map(|(key, value)| key.as_bytes().len() + value.as_bytes().len() + HEADER_FRAMING_BYTES)
        .sum()
}

fn capture_processes(
    monitored_processes: &[(String, u32)],
) -> Result<Vec<(String, ProcessSnapshot)>, BenchError> {
    monitored_processes
        .iter()
        .map(|(name, pid)| Ok((name.clone(), ProcessSnapshot::capture(*pid)?)))
        .collect()
}

fn finish_processes(
    before: Vec<(String, ProcessSnapshot)>,
    phase: &str,
) -> Result<Vec<AgdxProcessMeasurement>, BenchError> {
    before
        .into_iter()
        .map(|(name, snapshot)| {
            let later = ProcessSnapshot::capture(snapshot.pid)?;
            Ok(AgdxProcessMeasurement {
                name,
                phase: phase.to_owned(),
                delta: snapshot.delta(later)?,
            })
        })
        .collect()
}

fn apply_correctness(
    outcomes: &mut OutcomeCounts,
    summary: &crate::correctness::CorrectnessSummary,
) {
    outcomes.duplicates = u64::try_from(summary.duplicates.len()).unwrap_or(u64::MAX);
    outcomes.gaps = u64::try_from(
        summary
            .missing
            .len()
            .saturating_add(summary.unexpected.len()),
    )
    .unwrap_or(u64::MAX);
    outcomes.ordering_violations =
        u64::try_from(summary.ordering_violations.len()).unwrap_or(u64::MAX);
    outcomes.checksum_failures = u64::try_from(summary.checksum_failures.len()).unwrap_or(u64::MAX);
    outcomes.late_arrivals = u64::try_from(summary.late_arrivals.len()).unwrap_or(u64::MAX);
}

pub(crate) fn agdx_dispatch(case: &AgdxCase) -> Dispatch {
    if case.spin_dispatch {
        Dispatch::SpinWindow
    } else {
        Dispatch::Sleep
    }
}

pub(crate) fn validate_case(case: &AgdxCase) -> Result<(), BenchError> {
    if case.payload_bytes < size_of::<u64>()
        || case.operations == 0
        || case.duration_seconds == 0
        || case.chunks_per_stream == 0
        || case.concurrency == 0
        || case.partitions == 0
        || case.warmup_seconds == 0
        || case.timeout_millis == 0
    {
        return Err(BenchError::Invalid(
            "AGDX publish dimensions must be nonzero and payloads must fit a record ID".to_owned(),
        ));
    }
    Ok(())
}

fn publish_order(seed: u64) -> [PublishArm; 3] {
    const ORDERS: [[PublishArm; 3]; 6] = [
        [PublishArm::Bare, PublishArm::Provenance, PublishArm::Typed],
        [PublishArm::Bare, PublishArm::Typed, PublishArm::Provenance],
        [PublishArm::Provenance, PublishArm::Bare, PublishArm::Typed],
        [PublishArm::Provenance, PublishArm::Typed, PublishArm::Bare],
        [PublishArm::Typed, PublishArm::Bare, PublishArm::Provenance],
        [PublishArm::Typed, PublishArm::Provenance, PublishArm::Bare],
    ];
    ORDERS[usize::try_from(seed % 6).expect("seed modulo six fits usize")]
}

fn conversation(seed: u64, id: u64) -> ConversationId {
    ConversationId::from_u128((u128::from(seed) << 64) | u128::from(id))
}

fn correlation(seed: u64, id: u64) -> CorrelationId {
    CorrelationId::from_u128((u128::from(seed ^ u64::MAX) << 64) | u128::from(id))
}

pub(crate) fn record_payload(payload: &Bytes, id: u64) -> Result<Vec<u8>, String> {
    if payload.len() < size_of::<u64>() {
        return Err("AGDX payload must fit a record ID".to_owned());
    }
    let mut record = payload.to_vec();
    record[..size_of::<u64>()].copy_from_slice(&id.to_le_bytes());
    Ok(record)
}

fn chunk_payload(payload: &Bytes, id: u64, chunk: usize) -> Result<Vec<u8>, String> {
    if payload.len() < 2 * size_of::<u64>() {
        return Err("AGDX chunk payload must fit record and chunk IDs".to_owned());
    }
    let chunk = u64::try_from(chunk).map_err(|_| "AGDX chunk index exceeds u64".to_owned())?;
    let mut record = payload.to_vec();
    record[..size_of::<u64>()].copy_from_slice(&id.to_le_bytes());
    record[size_of::<u64>()..2 * size_of::<u64>()].copy_from_slice(&chunk.to_le_bytes());
    Ok(record)
}

fn decode_record_id(payload: &[u8]) -> Result<u64, BenchError> {
    let encoded = payload
        .get(..size_of::<u64>())
        .ok_or_else(|| BenchError::Invalid("observed AGDX body has no record ID".to_owned()))?;
    Ok(u64::from_le_bytes(encoded.try_into().map_err(|_| {
        BenchError::Invalid("observed AGDX record ID is invalid".to_owned())
    })?))
}

pub(crate) fn seeded_payload(size: usize, seed: u64) -> Bytes {
    let mut state = seed.max(1);
    let mut payload = Vec::with_capacity(size);
    while payload.len() < size {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let bytes = state.to_le_bytes();
        let take = bytes.len().min(size - payload.len());
        payload.extend_from_slice(&bytes[..take]);
    }
    Bytes::from(payload)
}

#[allow(clippy::cast_precision_loss)]
fn per_second(count: u64, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64()
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

pub(crate) fn durations_histogram(samples: &[Duration]) -> Result<Histogram<u64>, BenchError> {
    let mut histogram = Histogram::new_with_bounds(1, 3_600_000_000_000, 3)
        .map_err(|error| BenchError::Invalid(format!("invalid AGDX histogram: {error}")))?;
    for sample in samples {
        histogram
            .record(duration_ns(*sample).max(1))
            .map_err(|error| BenchError::Invalid(format!("AGDX latency overflow: {error}")))?;
    }
    Ok(histogram)
}

fn sdk_error(error: &laser_sdk::LaserError) -> BenchError {
    BenchError::Invalid(format!("Laser AGDX operation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_agdx_publish_driver_when_parsed_then_should_select_publish_path() {
        assert_eq!(
            "agdx_publish".parse::<AgdxDriver>().expect("driver parses"),
            AgdxDriver::AgdxPublish
        );
    }

    #[test]
    fn given_request_reply_driver_when_parsed_then_should_select_request_path() {
        assert_eq!(
            "request_reply"
                .parse::<AgdxDriver>()
                .expect("driver parses"),
            AgdxDriver::RequestReply
        );
    }

    #[test]
    fn given_agdx_stream_driver_when_parsed_then_should_select_stream_path() {
        assert_eq!(
            "agdx_stream".parse::<AgdxDriver>().expect("driver parses"),
            AgdxDriver::AgdxStream
        );
    }

    #[test]
    fn given_six_repetitions_when_ordered_then_each_arm_should_occupy_each_position_twice() {
        let mut positions = [[0_u8; 3]; 3];
        for seed in 0..6 {
            for (position, arm) in publish_order(seed).into_iter().enumerate() {
                let arm = match arm {
                    PublishArm::Bare => 0,
                    PublishArm::Provenance => 1,
                    PublishArm::Typed => 2,
                };
                positions[arm][position] += 1;
            }
        }
        assert_eq!(positions, [[2; 3]; 3]);
    }

    #[test]
    fn given_agdx_body_when_record_id_is_stamped_then_should_preserve_width_and_id() {
        let payload = seeded_payload(64, 7);
        let record = record_payload(&payload, 42).expect("record should build");
        assert_eq!(record.len(), payload.len());
        assert_eq!(decode_record_id(&record).expect("record should decode"), 42);
    }

    #[test]
    fn given_chunk_body_when_ids_are_stamped_then_should_preserve_record_and_chunk_ids() {
        let payload = seeded_payload(64, 7);
        let record = chunk_payload(&payload, 42, 3).expect("chunk should build");
        assert_eq!(record.len(), payload.len());
        assert_eq!(decode_record_id(&record).expect("record should decode"), 42);
        assert_eq!(
            u64::from_le_bytes(
                record[8..16]
                    .try_into()
                    .expect("chunk index has eight bytes")
            ),
            3
        );
    }

    #[test]
    fn given_agdx_shapes_when_counted_then_typed_record_should_expose_envelope_cost() {
        let payload = seeded_payload(64, 7);
        let source = "laser-bench-source"
            .parse()
            .expect("benchmark source should be valid");
        let counts = byte_counts(&payload, &source).expect("byte counts should build");
        assert_eq!(counts.len(), 3);
        assert_eq!(counts[0].stored_payload_bytes, 64);
        assert!(counts[1].user_header_bytes > 0);
        assert!(counts[2].stored_payload_bytes > counts[0].stored_payload_bytes);
    }
}

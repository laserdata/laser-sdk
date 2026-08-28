use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::StreamExt as _;
use laser_sdk::batching::BatchingProducer;
use laser_sdk::cursor::Cursor;
use laser_sdk::iggy::prelude::{
    AutoCommit, BackgroundConfig, Consumer as IggyConsumerIdentity, DirectConfig, Identifier,
    IggyByteSize, IggyConsumer, IggyDuration, IggyMessage, MessageClient, NonZeroIggyDuration,
    Partitioning, PollingStrategy,
};
use laser_sdk::laser::Laser;
use laser_sdk::stream::{
    CommitPolicy, Consumer, ConsumerStart, Producer, ProducerMessage, Routing, Topic,
};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::BenchError;
use crate::correctness::{CorrectnessOracle, ObservedRecord, OraclePolicy, checksum};
use crate::engine::{
    Dispatch, LoadResult, LoadTimeSeriesPoint, Operation, run_closed_loop, run_closed_loop_for,
    run_open_loop_for,
};
use crate::metrics::{ProcessDelta, ProcessSnapshot};
use crate::report::OutcomeCounts;

const MEASUREMENT_RECORD_OFFSET: u64 = 1_u64 << 63;
const PAIRED_MEASUREMENT_EPOCHS: u32 = 10;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DirectStreamingCase {
    pub payload_bytes: usize,
    pub batch_size: usize,
    pub batches: u64,
    pub duration_seconds: u64,
    pub concurrency: usize,
    pub partitions: u32,
    pub warmup_seconds: u64,
    pub timeout_millis: u64,
    pub offered_rate: Option<u64>,
    pub spin_dispatch: bool,
    pub max_in_flight: Option<usize>,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_streaming_producer_path
)]
pub enum StreamingProducerPath {
    StreamDirect,
    StreamDirectAa,
    StreamFluent,
    StreamBackground,
    StreamBatchingRecord,
    StreamBatchingByte,
    StreamBatchingLinger,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_streaming_consumer_path
)]
pub enum StreamingConsumerPath {
    StreamConsumerPartition,
    StreamConsumerGroup,
    StreamCursor,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_streaming_pipeline_path
)]
pub enum StreamingPipelinePath {
    StreamEndToEnd,
}

fn invalid_streaming_pipeline_path(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported streaming pipeline driver `{value}`"))
}

#[must_use]
pub fn is_c2_driver(driver: &str) -> bool {
    matches!(
        driver.parse::<StreamingProducerPath>(),
        Ok(StreamingProducerPath::StreamDirect)
    ) || matches!(
        driver.parse::<StreamingConsumerPath>(),
        Ok(StreamingConsumerPath::StreamConsumerPartition
            | StreamingConsumerPath::StreamConsumerGroup)
    )
}

impl StreamingConsumerPath {
    fn label(self) -> &'static str {
        match self {
            Self::StreamConsumerPartition => "laser-consumer",
            Self::StreamConsumerGroup => "laser-consumer-group",
            Self::StreamCursor => "laser-cursor",
        }
    }

    fn stream_label(self) -> &'static str {
        match self {
            Self::StreamConsumerPartition => "consumer",
            Self::StreamConsumerGroup => "consumer-group",
            Self::StreamCursor => "cursor",
        }
    }
}

fn invalid_streaming_consumer_path(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported streaming consumer driver `{value}`"))
}

impl StreamingProducerPath {
    fn label(self) -> &'static str {
        match self {
            Self::StreamDirect => "laser-direct",
            Self::StreamDirectAa => "raw-iggy-b",
            Self::StreamFluent => "laser-fluent",
            Self::StreamBackground => "laser-background",
            Self::StreamBatchingRecord => "laser-batching-record",
            Self::StreamBatchingByte => "laser-batching-byte",
            Self::StreamBatchingLinger => "laser-batching-linger",
        }
    }

    fn stream_label(self) -> &'static str {
        match self {
            Self::StreamDirect => "direct",
            Self::StreamDirectAa => "direct-aa",
            Self::StreamFluent => "fluent",
            Self::StreamBackground => "background",
            Self::StreamBatchingRecord => "batching-record",
            Self::StreamBatchingByte => "batching-byte",
            Self::StreamBatchingLinger => "batching-linger",
        }
    }
}

fn invalid_streaming_producer_path(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported streaming producer driver `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct StreamingArmSummary {
    pub arm: String,
    pub order: u8,
    pub elapsed_ns: u64,
    pub batches_per_second: f64,
    pub records_per_second: f64,
    pub payload_bytes_per_second: f64,
    pub scheduled_p50_ns: u64,
    pub scheduled_p90_ns: u64,
    pub scheduled_p99_ns: u64,
    pub service_p50_ns: u64,
    pub service_p90_ns: u64,
    pub service_p99_ns: u64,
    pub scheduler_lateness_p99_ns: u64,
    pub primary_p99_ns: u64,
    pub p99_supported: bool,
    pub service_p999_ns: Option<u64>,
    pub time_series: Vec<LoadTimeSeriesPoint>,
    pub configuration: serde_json::Value,
    pub outcomes: OutcomeCounts,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct DirectPairSummary {
    pub raw: StreamingArmSummary,
    pub laser: StreamingArmSummary,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ProcessMeasurement {
    pub name: String,
    pub phase: String,
    pub delta: ProcessDelta,
}

pub struct StreamingArmEvidence {
    pub summary: StreamingArmSummary,
    pub load: LoadResult,
    pub processes: Vec<ProcessMeasurement>,
}

pub struct DirectPairEvidence {
    pub raw: StreamingArmEvidence,
    pub laser: StreamingArmEvidence,
}

#[derive(Clone)]
struct ObservedMessage {
    payload: Bytes,
    partition: u32,
    offset: u64,
}

const TRACKER_SHARDS: usize = 16;

type PendingShard =
    tokio::sync::Mutex<HashMap<u64, (Instant, tokio::sync::oneshot::Sender<Duration>)>>;

#[derive(Clone)]
struct LatencyTracker {
    pending: Arc<Vec<PendingShard>>,
    delivery: Arc<Vec<tokio::sync::Mutex<hdrhistogram::Histogram<u64>>>>,
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self {
            pending: Arc::new(
                (0..TRACKER_SHARDS)
                    .map(|_| PendingShard::default())
                    .collect(),
            ),
            delivery: Arc::new(
                (0..TRACKER_SHARDS)
                    .map(|_| {
                        tokio::sync::Mutex::new(
                            hdrhistogram::Histogram::new(3)
                                .expect("three significant figures are a valid histogram"),
                        )
                    })
                    .collect(),
            ),
        }
    }
}

fn tracker_shard(id: u64) -> usize {
    usize::try_from(id % TRACKER_SHARDS as u64).expect("shard index fits usize")
}

struct ConsumerPump {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), BenchError>>,
}

struct PartitionConsumers {
    raw: Vec<Arc<tokio::sync::Mutex<IggyConsumer>>>,
    laser: Vec<Arc<tokio::sync::Mutex<Consumer>>>,
}

struct PairedOperations {
    raw_warmup: Operation,
    laser_warmup: Operation,
    raw: Operation,
    laser: Operation,
    shutdown: Option<ProducerShutdown>,
    lane_connections: Vec<Laser>,
}

enum ProducerShutdown {
    Background(BackgroundShutdown),
    Batching(Arc<BatchingProducer>),
}

struct BackgroundShutdown {
    raw: Arc<laser_sdk::iggy::prelude::IggyProducer>,
    laser: Producer,
}

struct ProducerSetup<'a> {
    laser: &'a Laser,
    connection_string: &'a str,
    case: &'a DirectStreamingCase,
    path: StreamingProducerPath,
    stream: &'a str,
    raw_topic: &'a Topic,
    laser_topic: &'a Topic,
    payload: &'a Bytes,
    warmup_records: u64,
}

struct EndToEndResources {
    raw_pumps: Vec<ConsumerPump>,
    laser_pumps: Vec<ConsumerPump>,
    consumers: PartitionConsumers,
    raw_warmup: Operation,
    laser_warmup: Operation,
    raw_operation: Operation,
    laser_operation: Operation,
    payload: Bytes,
    lane_connections: Vec<Laser>,
}

struct EndToEndInputs<'a> {
    connection_string: &'a str,
    stream: &'a str,
    raw_topic: &'a Topic,
    laser_topic: &'a Topic,
    case: &'a DirectStreamingCase,
    seed: u64,
    raw_tracker: &'a LatencyTracker,
    laser_tracker: &'a LatencyTracker,
    raw_observed: &'a Arc<tokio::sync::Mutex<Vec<ObservedMessage>>>,
    laser_observed: &'a Arc<tokio::sync::Mutex<Vec<ObservedMessage>>>,
}

#[derive(Default)]
struct EpochAccumulator {
    next_sequence: u64,
    loads: Vec<LoadResult>,
    processes: Vec<ProcessMeasurement>,
}

impl LatencyTracker {
    async fn register(&self, id: u64) -> tokio::sync::oneshot::Receiver<Duration> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pending[tracker_shard(id)]
            .lock()
            .await
            .insert(id, (Instant::now(), sender));
        receiver
    }

    async fn cancel(&self, id: u64) {
        self.pending[tracker_shard(id)].lock().await.remove(&id);
    }

    async fn resolve(&self, id: u64) {
        let received = Instant::now();
        let shard = tracker_shard(id);
        let Some((started, sender)) = self.pending[shard].lock().await.remove(&id) else {
            return;
        };
        let elapsed = received.saturating_duration_since(started);
        let _ = sender.send(elapsed);
        let _ = self.delivery[shard]
            .lock()
            .await
            .record(duration_sample_ns(elapsed));
    }

    async fn clear_samples(&self) {
        for shard in self.delivery.iter() {
            shard.lock().await.reset();
        }
    }

    async fn histogram(&self) -> Result<hdrhistogram::Histogram<u64>, BenchError> {
        let mut histogram = hdrhistogram::Histogram::new(3)
            .map_err(|error| BenchError::Invalid(format!("invalid histogram: {error}")))?;
        for shard in self.delivery.iter() {
            histogram.add(&*shard.lock().await).map_err(|error| {
                BenchError::Invalid(format!("end-to-end latency merge failed: {error}"))
            })?;
        }
        Ok(histogram)
    }
}

/// Open one dedicated TCP VSR connection per lane, matching the
/// connection-per-actor topology of `iggy-bench`.
async fn connect_lanes(connection_string: &str, count: usize) -> Result<Vec<Laser>, BenchError> {
    let mut lasers = Vec::with_capacity(count);
    for _ in 0..count {
        lasers.push(
            Laser::connect(connection_string)
                .await
                .map_err(|error| sdk_error(&error))?,
        );
    }
    Ok(lasers)
}

fn duration_sample_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos())
        .unwrap_or(u64::MAX)
        .max(1)
}

impl DirectPairEvidence {
    #[must_use]
    pub fn summary(&self) -> DirectPairSummary {
        DirectPairSummary {
            raw: self.raw.summary.clone(),
            laser: self.laser.summary.clone(),
        }
    }
}

/// Run counterbalanced raw Iggy and Laser direct producers, one dedicated
/// connection per lane in both arms.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, producer failure, or workload failure.
pub async fn run_direct_pair(
    laser: &Laser,
    connection_string: &str,
    case: &DirectStreamingCase,
    seed: u64,
) -> Result<DirectPairSummary, BenchError> {
    run_direct_pair_evidence(laser, connection_string, case, seed, &[])
        .await
        .map(|evidence| evidence.summary())
}

/// Run a paired direct-producer comparison and retain histograms and per-process deltas.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, process counter failure, producer failure, or workload failure.
pub async fn run_direct_pair_evidence(
    laser: &Laser,
    connection_string: &str,
    case: &DirectStreamingCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<DirectPairEvidence, BenchError> {
    run_producer_pair_evidence(
        laser,
        connection_string,
        case,
        seed,
        monitored_processes,
        StreamingProducerPath::StreamDirect,
    )
    .await
}

/// Run a paired raw and Laser producer comparison for the selected public path.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, process counter failure, producer failure, or workload failure.
pub async fn run_producer_pair_evidence(
    laser: &Laser,
    connection_string: &str,
    case: &DirectStreamingCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
    path: StreamingProducerPath,
) -> Result<DirectPairEvidence, BenchError> {
    validate_path_case(case, path)?;
    let stream = format!("bench-{}-{seed:016x}", path.stream_label());
    let raw_topic = laser.stream(&stream).topic("raw");
    let laser_topic = laser.stream(&stream).topic("sdk");
    raw_topic
        .ensure(case.partitions)
        .await
        .map_err(|error| sdk_error(&error))?;
    laser_topic
        .ensure(case.partitions)
        .await
        .map_err(|error| sdk_error(&error))?;
    let payload = seeded_payload(case.payload_bytes, seed);
    checked_records(case.batches, case.batch_size)?;
    let PairedOperations {
        raw_warmup,
        laser_warmup,
        raw,
        laser: laser_operation,
        shutdown,
        lane_connections,
    } = producer_operations(ProducerSetup {
        laser,
        connection_string,
        case,
        path,
        stream: &stream,
        raw_topic: &raw_topic,
        laser_topic: &laser_topic,
        payload: &payload,
        warmup_records: MEASUREMENT_RECORD_OFFSET,
    })
    .await?;
    let timeout = Duration::from_millis(case.timeout_millis);

    warmup(case, timeout, &raw_warmup).await?;
    warmup(case, timeout, &laser_warmup).await?;
    drop(raw_warmup);
    drop(laser_warmup);
    let (raw_result, laser_result) = measure_producer_pair(
        case,
        seed,
        path,
        timeout,
        raw,
        laser_operation,
        monitored_processes,
    )
    .await?;
    let mut raw_result = raw_result;
    let mut laser_result = laser_result;
    if let Some(shutdown) = shutdown {
        let (raw_shutdown, laser_shutdown) =
            shutdown_producers(shutdown, monitored_processes).await?;
        raw_result.processes.extend(raw_shutdown);
        laser_result.processes.extend(laser_shutdown);
    }
    drop(lane_connections);
    apply_correctness(
        &mut raw_result.summary.outcomes,
        &validate_topic(
            &raw_topic,
            &payload,
            &expected_ids(case, &raw_result.load)?,
            &explained_ids(case, &raw_result.load)?,
        )
        .await?,
    );
    apply_correctness(
        &mut laser_result.summary.outcomes,
        &validate_topic(
            &laser_topic,
            &payload,
            &expected_ids(case, &laser_result.load)?,
            &explained_ids(case, &laser_result.load)?,
        )
        .await?,
    );
    Ok(DirectPairEvidence {
        raw: raw_result,
        laser: laser_result,
    })
}

/// Run matched raw and Laser live-consumer comparisons over preloaded topics.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, process counter failure, consumer failure, shutdown failure, or correctness failure.
pub async fn run_consumer_pair_evidence(
    laser: &Laser,
    connection_string: &str,
    case: &DirectStreamingCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
    path: StreamingConsumerPath,
) -> Result<DirectPairEvidence, BenchError> {
    validate_case(case)?;
    if path == StreamingConsumerPath::StreamCursor {
        return run_cursor_pair_evidence(laser, connection_string, case, seed, monitored_processes)
            .await;
    }
    if path == StreamingConsumerPath::StreamConsumerPartition
        && case.concurrency != case.partitions as usize
    {
        return Err(BenchError::Invalid(
            "partition consumer arms require one consumer per partition".to_owned(),
        ));
    }
    let stream = format!("bench-{}-{seed:016x}", path.stream_label());
    let raw_topic = laser.stream(&stream).topic("raw");
    let laser_topic = laser.stream(&stream).topic("sdk");
    raw_topic
        .ensure(case.partitions)
        .await
        .map_err(|error| sdk_error(&error))?;
    laser_topic
        .ensure(case.partitions)
        .await
        .map_err(|error| sdk_error(&error))?;
    let payload = seeded_payload(case.payload_bytes, seed);
    preload_topic(&raw_topic, case, path, &payload).await?;
    preload_topic(&laser_topic, case, path, &payload).await?;
    let lane_connections = connect_lanes(connection_string, case.concurrency * 2).await?;
    let consumers = build_consumers(&lane_connections, &stream, case, path).await?;
    let raw_observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let laser_observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let raw_consumers = Arc::new(consumers.raw.clone());
    let laser_consumers = Arc::new(consumers.laser.clone());
    let raw_operation = raw_consumer_operation(raw_consumers, Arc::clone(&raw_observed));
    let laser_operation = laser_consumer_operation(laser_consumers, Arc::clone(&laser_observed));
    let timeout = Duration::from_millis(case.timeout_millis);
    let (mut raw_result, mut laser_result) = measure_consumer_pair(
        case,
        seed,
        path,
        timeout,
        raw_operation,
        laser_operation,
        monitored_processes,
    )
    .await?;
    shutdown_partition_consumers(consumers).await?;
    drop(lane_connections);
    apply_correctness(
        &mut raw_result.summary.outcomes,
        &validate_observed(&payload, case.batches, &raw_observed).await?,
    );
    apply_correctness(
        &mut laser_result.summary.outcomes,
        &validate_observed(&payload, case.batches, &laser_observed).await?,
    );
    Ok(DirectPairEvidence {
        raw: raw_result,
        laser: laser_result,
    })
}

async fn run_cursor_pair_evidence(
    laser: &Laser,
    connection_string: &str,
    case: &DirectStreamingCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<DirectPairEvidence, BenchError> {
    if case.concurrency != 1 {
        return Err(BenchError::Invalid(
            "cursor replay arms require concurrency = 1".to_owned(),
        ));
    }
    let stream = format!("bench-cursor-{seed:016x}");
    let raw_topic = laser.stream(&stream).topic("raw");
    let laser_topic = laser.stream(&stream).topic("sdk");
    raw_topic
        .ensure(case.partitions)
        .await
        .map_err(|error| sdk_error(&error))?;
    laser_topic
        .ensure(case.partitions)
        .await
        .map_err(|error| sdk_error(&error))?;
    let payload = seeded_payload(case.payload_bytes, seed);
    preload_topic(
        &raw_topic,
        case,
        StreamingConsumerPath::StreamCursor,
        &payload,
    )
    .await?;
    preload_topic(
        &laser_topic,
        case,
        StreamingConsumerPath::StreamCursor,
        &payload,
    )
    .await?;

    let raw_observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let laser_observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let arm_connections = connect_lanes(connection_string, 2).await?;
    let raw_operation = raw_cursor_operation(
        arm_connections[0].clone(),
        &stream,
        "raw",
        case,
        Arc::clone(&raw_observed),
    )?;
    let cursor = arm_connections[1]
        .stream(&stream)
        .topic("sdk")
        .replay()
        .map_err(|error| sdk_error(&error))?
        .batch(
            u32::try_from(case.batch_size)
                .map_err(|_| BenchError::Invalid("cursor batch size exceeds u32".to_owned()))?,
        );
    let laser_operation = laser_cursor_operation(
        Arc::new(tokio::sync::Mutex::new(cursor)),
        Arc::clone(&laser_observed),
    );
    let (mut raw_result, mut laser_result) = measure_cursor_pair(
        case,
        seed,
        raw_operation,
        laser_operation,
        monitored_processes,
    )
    .await?;
    apply_correctness(
        &mut raw_result.summary.outcomes,
        &validate_observed(&payload, case.batches, &raw_observed).await?,
    );
    apply_correctness(
        &mut laser_result.summary.outcomes,
        &validate_observed(&payload, case.batches, &laser_observed).await?,
    );
    Ok(DirectPairEvidence {
        raw: raw_result,
        laser: laser_result,
    })
}

/// Run a paired producer-to-consumer latency comparison using sequence-keyed monotonic timestamps.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, workload failure, consumer shutdown failure, or correctness failure.
pub async fn run_pipeline_pair_evidence(
    laser: &Laser,
    connection_string: &str,
    case: &DirectStreamingCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
    path: StreamingPipelinePath,
) -> Result<DirectPairEvidence, BenchError> {
    validate_case(case)?;
    if path != StreamingPipelinePath::StreamEndToEnd {
        return Err(BenchError::Invalid(
            "unsupported streaming pipeline path".to_owned(),
        ));
    }
    let stream = format!("bench-end-to-end-{seed:016x}");
    let raw_topic = laser.stream(&stream).topic("raw");
    let laser_topic = laser.stream(&stream).topic("sdk");
    raw_topic
        .ensure(case.partitions)
        .await
        .map_err(|error| sdk_error(&error))?;
    laser_topic
        .ensure(case.partitions)
        .await
        .map_err(|error| sdk_error(&error))?;
    let raw_tracker = LatencyTracker::default();
    let laser_tracker = LatencyTracker::default();
    let raw_observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let laser_observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let EndToEndResources {
        raw_pumps,
        laser_pumps,
        consumers,
        raw_warmup,
        laser_warmup,
        raw_operation,
        laser_operation,
        payload,
        lane_connections,
    } = prepare_end_to_end_resources(EndToEndInputs {
        connection_string,
        stream: &stream,
        raw_topic: &raw_topic,
        laser_topic: &laser_topic,
        case,
        seed,
        raw_tracker: &raw_tracker,
        laser_tracker: &laser_tracker,
        raw_observed: &raw_observed,
        laser_observed: &laser_observed,
    })
    .await?;
    let workload = async {
        let timeout = Duration::from_millis(case.timeout_millis);
        warmup(case, timeout, &raw_warmup).await?;
        warmup(case, timeout, &laser_warmup).await?;
        raw_tracker.clear_samples().await;
        laser_tracker.clear_samples().await;
        measure_end_to_end_pair(
            case,
            seed,
            raw_operation,
            laser_operation,
            &raw_tracker,
            &laser_tracker,
            monitored_processes,
        )
        .await
    }
    .await;
    stop_consumer_pumps(raw_pumps).await?;
    stop_consumer_pumps(laser_pumps).await?;
    shutdown_partition_consumers(consumers).await?;
    drop(lane_connections);
    let (mut raw_result, mut laser_result) = workload?;
    apply_expected_correctness(&mut raw_result, case, &payload, &raw_observed).await?;
    apply_expected_correctness(&mut laser_result, case, &payload, &laser_observed).await?;
    Ok(DirectPairEvidence {
        raw: raw_result,
        laser: laser_result,
    })
}

async fn prepare_end_to_end_resources(
    inputs: EndToEndInputs<'_>,
) -> Result<EndToEndResources, BenchError> {
    let EndToEndInputs {
        connection_string,
        stream,
        raw_topic: _raw_topic,
        laser_topic: _laser_topic,
        case,
        seed,
        raw_tracker,
        laser_tracker,
        raw_observed,
        laser_observed,
    } = inputs;
    let producer_lanes = case.concurrency * 2;
    let reader_lanes = case.partitions as usize * 2;
    let lane_connections = connect_lanes(connection_string, producer_lanes + reader_lanes).await?;
    let (raw_producer_lanes, rest) = lane_connections.split_at(case.concurrency);
    let (laser_producer_lanes, reader_connections) = rest.split_at(case.concurrency);
    let raw_producers = raw_producer_pool(raw_producer_lanes, stream, "raw", case).await?;
    let laser_producers = laser_producer_pool(laser_producer_lanes, stream, "sdk", case).await?;
    let consumers = build_partition_consumers(reader_connections, stream, case).await?;
    let raw_pumps = consumers
        .raw
        .iter()
        .map(|consumer| {
            spawn_raw_consumer_pump(
                Arc::clone(consumer),
                raw_tracker.clone(),
                Arc::clone(raw_observed),
            )
        })
        .collect::<Vec<_>>();
    let laser_pumps = consumers
        .laser
        .iter()
        .map(|consumer| {
            spawn_laser_consumer_pump(
                Arc::clone(consumer),
                laser_tracker.clone(),
                Arc::clone(laser_observed),
            )
        })
        .collect::<Vec<_>>();
    let payload = seeded_payload(case.payload_bytes, seed);
    let raw_warmup = raw_end_to_end_operation(
        Arc::clone(&raw_producers),
        payload.clone(),
        0,
        case.batch_size,
        raw_tracker.clone(),
    );
    let laser_warmup = laser_end_to_end_operation(
        Arc::clone(&laser_producers),
        payload.clone(),
        0,
        case.batch_size,
        laser_tracker.clone(),
    );
    let raw_operation = raw_end_to_end_operation(
        raw_producers,
        payload.clone(),
        MEASUREMENT_RECORD_OFFSET,
        case.batch_size,
        raw_tracker.clone(),
    );
    let laser_operation = laser_end_to_end_operation(
        laser_producers,
        payload.clone(),
        MEASUREMENT_RECORD_OFFSET,
        case.batch_size,
        laser_tracker.clone(),
    );
    Ok(EndToEndResources {
        raw_pumps,
        laser_pumps,
        consumers,
        raw_warmup,
        laser_warmup,
        raw_operation,
        laser_operation,
        payload,
        lane_connections,
    })
}

async fn apply_expected_correctness(
    result: &mut StreamingArmEvidence,
    case: &DirectStreamingCase,
    payload: &Bytes,
    observed: &tokio::sync::Mutex<Vec<ObservedMessage>>,
) -> Result<(), BenchError> {
    let expected = expected_ids(case, &result.load)?;
    let explained = explained_ids(case, &result.load)?;
    let correctness = validate_observed_ids(payload, &expected, &explained, observed).await?;
    apply_correctness(&mut result.summary.outcomes, &correctness);
    Ok(())
}

async fn measure_cursor_pair(
    case: &DirectStreamingCase,
    seed: u64,
    raw_operation: Operation,
    laser_operation: Operation,
    monitored_processes: &[(String, u32)],
) -> Result<(StreamingArmEvidence, StreamingArmEvidence), BenchError> {
    let timeout = Duration::from_millis(case.timeout_millis);
    if seed.is_multiple_of(2) {
        Ok((
            measured_cursor_arm(
                "raw-iggy-cursor",
                1,
                case,
                timeout,
                raw_operation,
                monitored_processes,
            )
            .await?,
            measured_cursor_arm(
                "laser-cursor",
                2,
                case,
                timeout,
                laser_operation,
                monitored_processes,
            )
            .await?,
        ))
    } else {
        let laser_result = measured_cursor_arm(
            "laser-cursor",
            1,
            case,
            timeout,
            laser_operation,
            monitored_processes,
        )
        .await?;
        let raw_result = measured_cursor_arm(
            "raw-iggy-cursor",
            2,
            case,
            timeout,
            raw_operation,
            monitored_processes,
        )
        .await?;
        Ok((raw_result, laser_result))
    }
}

async fn measure_end_to_end_pair(
    case: &DirectStreamingCase,
    seed: u64,
    raw_operation: Operation,
    laser_operation: Operation,
    raw_tracker: &LatencyTracker,
    laser_tracker: &LatencyTracker,
    monitored_processes: &[(String, u32)],
) -> Result<(StreamingArmEvidence, StreamingArmEvidence), BenchError> {
    let timeout = Duration::from_millis(case.timeout_millis);
    let duration = Duration::from_secs(case.duration_seconds) / PAIRED_MEASUREMENT_EPOCHS;
    let raw_starts = seed.is_multiple_of(2);
    let mut raw = EpochAccumulator::default();
    let mut laser = EpochAccumulator::default();
    for epoch in 0..PAIRED_MEASUREMENT_EPOCHS {
        let raw_first = raw_starts == epoch.is_multiple_of(2);
        if raw_first {
            measure_epoch_into(
                case,
                duration,
                timeout,
                &raw_operation,
                monitored_processes,
                &mut raw,
            )
            .await?;
            measure_epoch_into(
                case,
                duration,
                timeout,
                &laser_operation,
                monitored_processes,
                &mut laser,
            )
            .await?;
        } else {
            measure_epoch_into(
                case,
                duration,
                timeout,
                &laser_operation,
                monitored_processes,
                &mut laser,
            )
            .await?;
            measure_epoch_into(
                case,
                duration,
                timeout,
                &raw_operation,
                monitored_processes,
                &mut raw,
            )
            .await?;
        }
    }
    let mut raw_load = merge_load_results(raw.loads)?;
    let mut laser_load = merge_load_results(laser.loads)?;
    raw_load.service = raw_tracker.histogram().await?;
    laser_load.service = laser_tracker.histogram().await?;
    let mut raw_summary = summarize_end_to_end(
        "raw-iggy-end-to-end",
        u8::from(!raw_starts) + 1,
        &raw_load,
        case,
    );
    let mut laser_summary = summarize_end_to_end(
        "laser-end-to-end",
        u8::from(raw_starts) + 1,
        &laser_load,
        case,
    );
    annotate_pairing(&mut raw_summary);
    annotate_pairing(&mut laser_summary);
    Ok((
        StreamingArmEvidence {
            summary: raw_summary,
            load: raw_load,
            processes: merge_process_measurements(raw.processes),
        },
        StreamingArmEvidence {
            summary: laser_summary,
            load: laser_load,
            processes: merge_process_measurements(laser.processes),
        },
    ))
}

fn raw_cursor_operation(
    laser: Laser,
    stream: &str,
    topic: &str,
    case: &DirectStreamingCase,
    observed: Arc<tokio::sync::Mutex<Vec<ObservedMessage>>>,
) -> Result<Operation, BenchError> {
    let stream = Identifier::named(stream).map_err(|error| iggy_error(&error))?;
    let topic = Identifier::named(topic).map_err(|error| iggy_error(&error))?;
    let consumer = IggyConsumerIdentity::new(
        Identifier::named("laser-bench-raw-cursor").map_err(|error| iggy_error(&error))?,
    );
    let partitions = case.partitions;
    let batch = u32::try_from(case.batch_size)
        .map_err(|_| BenchError::Invalid("cursor batch size exceeds u32".to_owned()))?;
    Ok(Arc::new(move |_| {
        let laser = laser.clone();
        let stream = stream.clone();
        let topic = topic.clone();
        let consumer = consumer.clone();
        let observed = Arc::clone(&observed);
        Box::pin(async move {
            let mut offsets = vec![0_u64; partitions as usize];
            for partition in 0..partitions {
                loop {
                    let polled = laser
                        .client()
                        .poll_messages(
                            &stream,
                            &topic,
                            Some(partition),
                            &consumer,
                            &PollingStrategy::offset(offsets[partition as usize]),
                            batch,
                            false,
                        )
                        .await
                        .map_err(|error| error.to_string())?;
                    let count = polled.messages.len();
                    let Some(last) = polled.messages.last() else {
                        break;
                    };
                    offsets[partition as usize] = last.header.offset.saturating_add(1);
                    let mut sink = observed.lock().await;
                    sink.extend(polled.messages.into_iter().map(|message| ObservedMessage {
                        payload: message.payload,
                        partition,
                        offset: message.header.offset,
                    }));
                    drop(sink);
                    if count < batch as usize {
                        break;
                    }
                }
            }
            Ok(())
        })
    }))
}

fn laser_cursor_operation(
    cursor: Arc<tokio::sync::Mutex<Cursor>>,
    observed: Arc<tokio::sync::Mutex<Vec<ObservedMessage>>>,
) -> Operation {
    Arc::new(move |_| {
        let cursor = Arc::clone(&cursor);
        let observed = Arc::clone(&observed);
        Box::pin(async move {
            loop {
                let records = cursor
                    .lock()
                    .await
                    .poll()
                    .await
                    .map_err(|error| error.to_string())?;
                if records.is_empty() {
                    break;
                }
                observed
                    .lock()
                    .await
                    .extend(records.into_iter().map(|message| ObservedMessage {
                        payload: Bytes::from(message.payload),
                        partition: message.id.partition_id,
                        offset: message.id.offset,
                    }));
            }
            Ok(())
        })
    })
}

async fn measured_cursor_arm(
    name: &str,
    order: u8,
    case: &DirectStreamingCase,
    timeout: Duration,
    operation: Operation,
    monitored_processes: &[(String, u32)],
) -> Result<StreamingArmEvidence, BenchError> {
    let before = capture_processes(monitored_processes)?;
    let load = run_closed_loop(1, 1, timeout, operation).await?;
    let processes = finish_processes(before, "measurement")?;
    let summary = summarize_cursor(name, order, &load, case);
    Ok(StreamingArmEvidence {
        summary,
        load,
        processes,
    })
}

async fn preload_topic(
    topic: &Topic,
    case: &DirectStreamingCase,
    path: StreamingConsumerPath,
    payload: &Bytes,
) -> Result<(), BenchError> {
    let batch_length = u32::try_from(case.batch_size)
        .map_err(|_| BenchError::Invalid("batch size exceeds u32".to_owned()))?;
    let producer_count = if path == StreamingConsumerPath::StreamConsumerPartition {
        case.partitions
    } else {
        1
    };
    let mut producers = Vec::with_capacity(producer_count as usize);
    for partition in 0..producer_count {
        let mut builder = topic
            .producer()
            .batch_length(batch_length)
            .create_stream(false)
            .create_topic(false);
        if path == StreamingConsumerPath::StreamConsumerPartition {
            builder = builder.routing(Routing::Partition(partition));
        }
        producers.push(builder.build().await.map_err(|error| sdk_error(&error))?);
    }
    let total = usize::try_from(case.batches)
        .map_err(|_| BenchError::Invalid("record count exceeds usize".to_owned()))?;
    for start in (0..total).step_by(case.batch_size) {
        let end = start.saturating_add(case.batch_size).min(total);
        let messages = (start..end)
            .map(|id| {
                let id = u64::try_from(id)
                    .map_err(|_| BenchError::Invalid("record ID exceeds u64".to_owned()))?;
                record_payload(payload, id)
                    .map(ProducerMessage::new)
                    .map_err(BenchError::Invalid)
            })
            .collect::<Result<Vec<_>, BenchError>>()?;
        let sequence = u64::try_from(start / case.batch_size)
            .map_err(|_| BenchError::Invalid("preload sequence exceeds u64".to_owned()))?;
        producer_for_sequence(&producers, sequence)
            .map_err(BenchError::Invalid)?
            .send_batch(messages)
            .await
            .map_err(|error| sdk_error(&error))?;
    }
    for producer in producers {
        producer
            .shutdown()
            .await
            .map_err(|error| sdk_error(&error))?;
    }
    Ok(())
}

async fn build_consumers(
    lane_connections: &[Laser],
    stream: &str,
    case: &DirectStreamingCase,
    path: StreamingConsumerPath,
) -> Result<PartitionConsumers, BenchError> {
    let batch_length = u32::try_from(case.batch_size)
        .map_err(|_| BenchError::Invalid("consumer batch size exceeds u32".to_owned()))?;
    let mut raw = Vec::with_capacity(case.concurrency);
    let mut laser = Vec::with_capacity(case.concurrency);
    for lane in 0..case.concurrency {
        let partition = u32::try_from(lane)
            .map_err(|_| BenchError::Invalid("consumer lane exceeds u32".to_owned()))?;
        let raw_topic = lane_connections[lane].stream(stream).topic("raw");
        let laser_topic = lane_connections[case.concurrency + lane]
            .stream(stream)
            .topic("sdk");
        let raw_builder = match path {
            StreamingConsumerPath::StreamConsumerPartition => raw_topic
                .iggy_consumer(&format!("laser-bench-raw-{lane}"), partition)
                .map_err(|error| sdk_error(&error))?,
            StreamingConsumerPath::StreamConsumerGroup => raw_topic
                .iggy_consumer_group("laser-bench-raw-group")
                .map_err(|error| sdk_error(&error))?
                .auto_join_consumer_group()
                .create_consumer_group_if_not_exists(),
            StreamingConsumerPath::StreamCursor => {
                return Err(BenchError::Invalid(
                    "cursor replay does not use a live consumer".to_owned(),
                ));
            }
        };
        let mut raw_consumer = raw_builder
            .batch_length(batch_length)
            .polling_strategy(PollingStrategy::first())
            .auto_commit(AutoCommit::Disabled)
            .without_poll_interval()
            .allow_replay()
            .build();
        raw_consumer
            .init()
            .await
            .map_err(|error| iggy_error(&error))?;
        raw.push(Arc::new(tokio::sync::Mutex::new(raw_consumer)));

        let laser_builder = match path {
            StreamingConsumerPath::StreamConsumerPartition => {
                laser_topic.consumer(format!("laser-bench-sdk-{lane}"), partition)
            }
            StreamingConsumerPath::StreamConsumerGroup => {
                laser_topic.consumer_group("laser-bench-sdk-group")
            }
            StreamingConsumerPath::StreamCursor => {
                return Err(BenchError::Invalid(
                    "cursor replay does not use a live consumer".to_owned(),
                ));
            }
        };
        let laser_consumer = laser_builder
            .batch_length(batch_length)
            .without_poll_interval()
            .start_at(ConsumerStart::First)
            .commit_policy(CommitPolicy::Disabled)
            .allow_replay()
            .build()
            .await
            .map_err(|error| sdk_error(&error))?;
        laser.push(Arc::new(tokio::sync::Mutex::new(laser_consumer)));
    }
    Ok(PartitionConsumers { raw, laser })
}

async fn build_partition_consumers(
    reader_connections: &[Laser],
    stream: &str,
    case: &DirectStreamingCase,
) -> Result<PartitionConsumers, BenchError> {
    let batch_length = consumer_batch_length(case)?;
    let partitions = case.partitions as usize;
    let mut raw = Vec::with_capacity(partitions);
    let mut laser = Vec::with_capacity(partitions);
    for partition in 0..case.partitions {
        let lane = partition as usize;
        let raw_topic = reader_connections[lane].stream(stream).topic("raw");
        let laser_topic = reader_connections[partitions + lane]
            .stream(stream)
            .topic("sdk");
        let mut raw_consumer = raw_topic
            .iggy_consumer(&format!("laser-bench-raw-e2e-{partition}"), partition)
            .map_err(|error| sdk_error(&error))?
            .batch_length(batch_length)
            .polling_strategy(PollingStrategy::offset(0))
            .auto_commit(AutoCommit::Disabled)
            .without_poll_interval()
            .allow_replay()
            .build();
        raw_consumer
            .init()
            .await
            .map_err(|error| iggy_error(&error))?;
        raw.push(Arc::new(tokio::sync::Mutex::new(raw_consumer)));

        let laser_consumer = laser_topic
            .consumer(format!("laser-bench-sdk-e2e-{partition}"), partition)
            .batch_length(batch_length)
            .without_poll_interval()
            .start_at(ConsumerStart::Offset(0))
            .commit_policy(CommitPolicy::Disabled)
            .allow_replay()
            .build()
            .await
            .map_err(|error| sdk_error(&error))?;
        laser.push(Arc::new(tokio::sync::Mutex::new(laser_consumer)));
    }
    Ok(PartitionConsumers { raw, laser })
}

fn consumer_batch_length(case: &DirectStreamingCase) -> Result<u32, BenchError> {
    u32::try_from(case.batch_size)
        .map_err(|_| BenchError::Invalid("consumer batch length exceeds u32".to_owned()))
}

fn raw_consumer_operation(
    consumers: Arc<Vec<Arc<tokio::sync::Mutex<IggyConsumer>>>>,
    observed: Arc<tokio::sync::Mutex<Vec<ObservedMessage>>>,
) -> Operation {
    Arc::new(move |sequence| {
        let consumers = Arc::clone(&consumers);
        let observed = Arc::clone(&observed);
        Box::pin(async move {
            let consumer = producer_for_sequence(&consumers, sequence)?;
            let mut consumer = consumer.lock().await;
            let message = consumer
                .next()
                .await
                .ok_or_else(|| "raw consumer ended".to_owned())?
                .map_err(|error| error.to_string())?;
            // The lane lock stays held across the push so the observation
            // order cannot invert the consumption order when two engine
            // slots land on the same lane.
            observed.lock().await.push(ObservedMessage {
                payload: message.message.payload,
                partition: message.partition_id,
                offset: message.message.header.offset,
            });
            drop(consumer);
            Ok(())
        })
    })
}

fn laser_consumer_operation(
    consumers: Arc<Vec<Arc<tokio::sync::Mutex<Consumer>>>>,
    observed: Arc<tokio::sync::Mutex<Vec<ObservedMessage>>>,
) -> Operation {
    Arc::new(move |sequence| {
        let consumers = Arc::clone(&consumers);
        let observed = Arc::clone(&observed);
        Box::pin(async move {
            let consumer = producer_for_sequence(&consumers, sequence)?;
            let mut consumer = consumer.lock().await;
            let message = consumer
                .next()
                .await
                .ok_or_else(|| "Laser consumer ended".to_owned())?
                .map_err(|error| error.to_string())?;
            // The lane lock stays held across the push so the observation
            // order cannot invert the consumption order when two engine
            // slots land on the same lane.
            observed.lock().await.push(ObservedMessage {
                payload: message.payload,
                partition: message.partition_id,
                offset: message.position.offset,
            });
            drop(consumer);
            Ok(())
        })
    })
}

fn raw_end_to_end_operation(
    producers: Arc<Vec<Arc<laser_sdk::iggy::prelude::IggyProducer>>>,
    payload: Bytes,
    id_offset: u64,
    batch_size: usize,
    tracker: LatencyTracker,
) -> Operation {
    Arc::new(move |sequence| {
        let producers = Arc::clone(&producers);
        let payload = payload.clone();
        let tracker = tracker.clone();
        Box::pin(async move {
            let ids = batch_record_ids(id_offset, sequence, batch_size)?;
            let producer = producer_for_sequence(&producers, sequence)?;
            let mut receivers = Vec::with_capacity(ids.len());
            let mut messages = Vec::with_capacity(ids.len());
            for id in &ids {
                receivers.push(tracker.register(*id).await);
                messages.push(
                    IggyMessage::builder()
                        .payload(record_payload(&payload, *id)?)
                        .build()
                        .map_err(|error| error.to_string())?,
                );
            }
            if let Err(error) = producer.send(messages).await {
                cancel_tracked(&tracker, &ids).await;
                return Err(error.to_string());
            }
            await_deliveries(receivers, "raw").await
        })
    })
}

fn laser_end_to_end_operation(
    producers: Arc<Vec<Producer>>,
    payload: Bytes,
    id_offset: u64,
    batch_size: usize,
    tracker: LatencyTracker,
) -> Operation {
    Arc::new(move |sequence| {
        let producers = Arc::clone(&producers);
        let payload = payload.clone();
        let tracker = tracker.clone();
        Box::pin(async move {
            let ids = batch_record_ids(id_offset, sequence, batch_size)?;
            let producer = producer_for_sequence(&producers, sequence)?.clone();
            let mut receivers = Vec::with_capacity(ids.len());
            let mut messages = Vec::with_capacity(ids.len());
            for id in &ids {
                receivers.push(tracker.register(*id).await);
                messages.push(ProducerMessage::new(record_payload(&payload, *id)?));
            }
            if let Err(error) = producer.send_batch(messages).await {
                cancel_tracked(&tracker, &ids).await;
                return Err(error.to_string());
            }
            await_deliveries(receivers, "Laser").await
        })
    })
}

fn batch_record_ids(id_offset: u64, sequence: u64, batch_size: usize) -> Result<Vec<u64>, String> {
    (0..batch_size)
        .map(|index| record_id(id_offset, sequence, batch_size, index))
        .collect()
}

async fn cancel_tracked(tracker: &LatencyTracker, ids: &[u64]) {
    for id in ids {
        tracker.cancel(*id).await;
    }
}

async fn await_deliveries(
    receivers: Vec<tokio::sync::oneshot::Receiver<Duration>>,
    arm: &str,
) -> Result<(), String> {
    for receiver in receivers {
        receiver
            .await
            .map_err(|_| format!("{arm} end-to-end delivery waiter closed"))?;
    }
    Ok(())
}

fn spawn_raw_consumer_pump(
    consumer: Arc<tokio::sync::Mutex<IggyConsumer>>,
    tracker: LatencyTracker,
    observed: Arc<tokio::sync::Mutex<Vec<ObservedMessage>>>,
) -> ConsumerPump {
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                result = async {
                    let mut consumer = consumer.lock().await;
                    consumer
                        .next()
                        .await
                        .ok_or_else(|| BenchError::Invalid("raw end-to-end consumer ended".to_owned()))?
                        .map_err(|error| iggy_error(&error))
                } => {
                    let message = result?;
                    let id = decode_record_id(&message.message.payload)?;
                    tracker.resolve(id).await;
                    observed.lock().await.push(ObservedMessage {
                        payload: message.message.payload,
                        partition: message.partition_id,
                        offset: message.message.header.offset,
                    });
                }
            }
        }
        Ok(())
    });
    ConsumerPump { stop, task }
}

fn spawn_laser_consumer_pump(
    consumer: Arc<tokio::sync::Mutex<Consumer>>,
    tracker: LatencyTracker,
    observed: Arc<tokio::sync::Mutex<Vec<ObservedMessage>>>,
) -> ConsumerPump {
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                result = async {
                    let mut consumer = consumer.lock().await;
                    consumer
                        .next()
                        .await
                        .ok_or_else(|| BenchError::Invalid("Laser end-to-end consumer ended".to_owned()))?
                        .map_err(|error| sdk_error(&error))
                } => {
                    let message = result?;
                    let id = decode_record_id(&message.payload)?;
                    tracker.resolve(id).await;
                    observed.lock().await.push(ObservedMessage {
                        payload: message.payload,
                        partition: message.partition_id,
                        offset: message.position.offset,
                    });
                }
            }
        }
        Ok(())
    });
    ConsumerPump { stop, task }
}

async fn stop_consumer_pumps(pumps: Vec<ConsumerPump>) -> Result<(), BenchError> {
    for (index, pump) in pumps.into_iter().enumerate() {
        stop_consumer_pump(&format!("consumer-pump-{index}"), pump).await?;
    }
    Ok(())
}

async fn stop_consumer_pump(name: &str, pump: ConsumerPump) -> Result<(), BenchError> {
    let ConsumerPump { stop, mut task } = pump;
    let _ = stop.send(());
    if let Ok(result) = tokio::time::timeout(Duration::from_secs(1), &mut task).await {
        result.map_err(|error| {
            BenchError::Invalid(format!("{name} consumer task failed: {error}"))
        })??;
    } else {
        task.abort();
        match task.await {
            Ok(result) => result.map_err(|error| {
                BenchError::Invalid(format!("{name} consumer task failed after abort: {error}"))
            })?,
            Err(error) if error.is_cancelled() => {}
            Err(error) => {
                return Err(BenchError::Invalid(format!(
                    "{name} consumer task failed during abort: {error}"
                )));
            }
        }
    }
    Ok(())
}

async fn measure_consumer_pair(
    case: &DirectStreamingCase,
    seed: u64,
    path: StreamingConsumerPath,
    timeout: Duration,
    raw_operation: Operation,
    laser_operation: Operation,
    monitored_processes: &[(String, u32)],
) -> Result<(StreamingArmEvidence, StreamingArmEvidence), BenchError> {
    let raw_starts = seed.is_multiple_of(2);
    let mut raw = EpochAccumulator::default();
    let mut laser = EpochAccumulator::default();
    let epochs = u64::from(PAIRED_MEASUREMENT_EPOCHS).min(case.batches);
    let base = case.batches / epochs;
    let remainder = case.batches % epochs;
    for epoch in 0..PAIRED_MEASUREMENT_EPOCHS {
        let epoch_index = u64::from(epoch);
        if epoch_index >= epochs {
            break;
        }
        let operations = base + u64::from(epoch_index < remainder);
        if operations == 0 {
            continue;
        }
        let raw_first = raw_starts == epoch.is_multiple_of(2);
        if raw_first {
            consume_epoch_into(
                case,
                operations,
                timeout,
                &raw_operation,
                monitored_processes,
                &mut raw,
            )
            .await?;
            consume_epoch_into(
                case,
                operations,
                timeout,
                &laser_operation,
                monitored_processes,
                &mut laser,
            )
            .await?;
        } else {
            consume_epoch_into(
                case,
                operations,
                timeout,
                &laser_operation,
                monitored_processes,
                &mut laser,
            )
            .await?;
            consume_epoch_into(
                case,
                operations,
                timeout,
                &raw_operation,
                monitored_processes,
                &mut raw,
            )
            .await?;
        }
    }
    let raw_load = merge_load_results(raw.loads)?;
    let laser_load = merge_load_results(laser.loads)?;
    let mut raw_summary = summarize_consumer(
        "raw-iggy-consumer",
        u8::from(!raw_starts) + 1,
        &raw_load,
        case,
        path,
    );
    let mut laser_summary = summarize_consumer(
        path.label(),
        u8::from(raw_starts) + 1,
        &laser_load,
        case,
        path,
    );
    annotate_pairing(&mut raw_summary);
    annotate_pairing(&mut laser_summary);
    Ok((
        StreamingArmEvidence {
            summary: raw_summary,
            load: raw_load,
            processes: merge_process_measurements(raw.processes),
        },
        StreamingArmEvidence {
            summary: laser_summary,
            load: laser_load,
            processes: merge_process_measurements(laser.processes),
        },
    ))
}

async fn consume_epoch_into(
    case: &DirectStreamingCase,
    operations: u64,
    timeout: Duration,
    operation: &Operation,
    monitored_processes: &[(String, u32)],
    accumulator: &mut EpochAccumulator,
) -> Result<(), BenchError> {
    let before = capture_processes(monitored_processes)?;
    let mut load = run_closed_loop(
        operations,
        case.concurrency,
        timeout,
        offset_operation(operation, accumulator.next_sequence),
    )
    .await?;
    shift_load_sequences(&mut load, accumulator.next_sequence)?;
    accumulator.next_sequence = accumulator
        .next_sequence
        .checked_add(load.outcomes.offered)
        .ok_or_else(|| BenchError::Invalid("consumer epoch sequence overflowed".to_owned()))?;
    accumulator
        .processes
        .extend(finish_processes(before, "measurement")?);
    accumulator.loads.push(load);
    Ok(())
}

async fn shutdown_partition_consumers(consumers: PartitionConsumers) -> Result<(), BenchError> {
    for raw in consumers.raw {
        let mut raw = Arc::try_unwrap(raw)
            .map_err(|_| BenchError::Invalid("raw partition consumer is still shared".to_owned()))?
            .into_inner();
        raw.shutdown().await.map_err(|error| iggy_error(&error))?;
    }
    for laser in consumers.laser {
        let mut laser = Arc::try_unwrap(laser)
            .map_err(|_| {
                BenchError::Invalid("Laser partition consumer is still shared".to_owned())
            })?
            .into_inner();
        laser.shutdown().await.map_err(|error| sdk_error(&error))?;
    }
    Ok(())
}

async fn validate_observed(
    payload: &Bytes,
    expected_records: u64,
    observed: &tokio::sync::Mutex<Vec<ObservedMessage>>,
) -> Result<crate::correctness::CorrectnessSummary, BenchError> {
    let policy = OraclePolicy {
        allow_duplicates: false,
    };
    let mut oracle = CorrectnessOracle::new(0..expected_records, policy);
    for message in observed.lock().await.iter() {
        let id = decode_record_id(&message.payload)?;
        let expected = record_payload(payload, id).map_err(BenchError::Invalid)?;
        oracle.observe(ObservedRecord {
            id,
            partition: message.partition,
            partition_sequence: message.offset,
            payload: &message.payload,
            checksum: checksum(&expected),
        });
    }
    Ok(oracle.finish())
}

async fn validate_observed_ids(
    payload: &Bytes,
    expected_ids: &[u64],
    explained_ids: &[u64],
    observed: &tokio::sync::Mutex<Vec<ObservedMessage>>,
) -> Result<crate::correctness::CorrectnessSummary, BenchError> {
    let policy = OraclePolicy {
        allow_duplicates: false,
    };
    let mut oracle = CorrectnessOracle::new(expected_ids.iter().copied(), policy)
        .with_explained(explained_ids.iter().copied());
    for message in observed.lock().await.iter() {
        let id = decode_record_id(&message.payload)?;
        if id < MEASUREMENT_RECORD_OFFSET {
            continue;
        }
        let expected = record_payload(payload, id).map_err(BenchError::Invalid)?;
        oracle.observe(ObservedRecord {
            id,
            partition: message.partition,
            partition_sequence: message.offset,
            payload: &message.payload,
            checksum: checksum(&expected),
        });
    }
    Ok(oracle.finish())
}

async fn producer_operations(setup: ProducerSetup<'_>) -> Result<PairedOperations, BenchError> {
    let ProducerSetup {
        laser,
        connection_string,
        case,
        path,
        stream,
        raw_topic,
        laser_topic,
        payload,
        warmup_records,
    } = setup;
    match path {
        StreamingProducerPath::StreamDirect | StreamingProducerPath::StreamDirectAa => {
            direct_producer_operations(
                connection_string,
                case,
                path,
                stream,
                payload,
                warmup_records,
            )
            .await
        }
        StreamingProducerPath::StreamFluent => Ok(PairedOperations {
            raw_warmup: raw_one_shot_operation(
                laser.clone(),
                stream,
                raw_topic.name(),
                payload.clone(),
                0,
            )?,
            laser_warmup: fluent_operation(laser_topic.clone(), payload.clone(), 0),
            raw: raw_one_shot_operation(
                laser.clone(),
                stream,
                raw_topic.name(),
                payload.clone(),
                warmup_records,
            )?,
            laser: fluent_operation(laser_topic.clone(), payload.clone(), warmup_records),
            shutdown: None,
            lane_connections: Vec::new(),
        }),
        StreamingProducerPath::StreamBackground => {
            let raw = raw_background_producer(raw_topic, case).await?;
            let laser_producer = laser_background_producer(laser_topic, case).await?;
            Ok(PairedOperations {
                raw_warmup: raw_operation(Arc::clone(&raw), payload.clone(), case.batch_size, 0),
                laser_warmup: laser_operation(
                    laser_producer.clone(),
                    payload.clone(),
                    case.batch_size,
                    0,
                ),
                raw: raw_operation(
                    Arc::clone(&raw),
                    payload.clone(),
                    case.batch_size,
                    warmup_records,
                ),
                laser: laser_operation(
                    laser_producer.clone(),
                    payload.clone(),
                    case.batch_size,
                    warmup_records,
                ),
                shutdown: Some(ProducerShutdown::Background(BackgroundShutdown {
                    raw,
                    laser: laser_producer,
                })),
                lane_connections: Vec::new(),
            })
        }
        StreamingProducerPath::StreamBatchingRecord
        | StreamingProducerPath::StreamBatchingByte
        | StreamingProducerPath::StreamBatchingLinger => {
            batching_operations(ProducerSetup {
                laser,
                connection_string,
                case,
                path,
                stream,
                raw_topic,
                laser_topic,
                payload,
                warmup_records,
            })
            .await
        }
    }
}

async fn direct_producer_operations(
    connection_string: &str,
    case: &DirectStreamingCase,
    path: StreamingProducerPath,
    stream: &str,
    payload: &Bytes,
    warmup_records: u64,
) -> Result<PairedOperations, BenchError> {
    let lane_connections = connect_lanes(connection_string, case.concurrency * 2).await?;
    let (raw_lanes, second_lanes) = lane_connections.split_at(case.concurrency);
    let raw = raw_producer_pool(raw_lanes, stream, "raw", case).await?;
    if path == StreamingProducerPath::StreamDirectAa {
        let raw_b = raw_producer_pool(second_lanes, stream, "sdk", case).await?;
        return Ok(PairedOperations {
            raw_warmup: raw_pool_operation(Arc::clone(&raw), payload.clone(), case.batch_size, 0),
            laser_warmup: raw_pool_operation(
                Arc::clone(&raw_b),
                payload.clone(),
                case.batch_size,
                0,
            ),
            raw: raw_pool_operation(raw, payload.clone(), case.batch_size, warmup_records),
            laser: raw_pool_operation(raw_b, payload.clone(), case.batch_size, warmup_records),
            shutdown: None,
            lane_connections,
        });
    }
    let direct = laser_producer_pool(second_lanes, stream, "sdk", case).await?;
    Ok(PairedOperations {
        raw_warmup: raw_pool_operation(Arc::clone(&raw), payload.clone(), case.batch_size, 0),
        laser_warmup: laser_pool_operation(
            Arc::clone(&direct),
            payload.clone(),
            case.batch_size,
            0,
        ),
        raw: raw_pool_operation(raw, payload.clone(), case.batch_size, warmup_records),
        laser: laser_pool_operation(direct, payload.clone(), case.batch_size, warmup_records),
        shutdown: None,
        lane_connections,
    })
}

async fn batching_operations(setup: ProducerSetup<'_>) -> Result<PairedOperations, BenchError> {
    let raw = raw_producer(setup.raw_topic, setup.case).await?;
    let batching = Arc::new(batching_producer(
        setup.laser_topic,
        setup.case,
        setup.path,
    )?);
    Ok(PairedOperations {
        raw_warmup: raw_operation(
            Arc::clone(&raw),
            setup.payload.clone(),
            setup.case.batch_size,
            0,
        ),
        laser_warmup: batching_operation(
            Arc::clone(&batching),
            setup.payload.clone(),
            setup.case.batch_size,
            0,
        ),
        raw: raw_operation(
            raw,
            setup.payload.clone(),
            setup.case.batch_size,
            setup.warmup_records,
        ),
        laser: batching_operation(
            Arc::clone(&batching),
            setup.payload.clone(),
            setup.case.batch_size,
            setup.warmup_records,
        ),
        shutdown: Some(ProducerShutdown::Batching(batching)),
        lane_connections: Vec::new(),
    })
}

async fn measure_producer_pair(
    case: &DirectStreamingCase,
    seed: u64,
    path: StreamingProducerPath,
    timeout: Duration,
    raw_operation: Operation,
    laser_operation: Operation,
    monitored_processes: &[(String, u32)],
) -> Result<(StreamingArmEvidence, StreamingArmEvidence), BenchError> {
    let duration = Duration::from_secs(case.duration_seconds) / PAIRED_MEASUREMENT_EPOCHS;
    let raw_starts = seed.is_multiple_of(2);
    let mut raw = EpochAccumulator::default();
    let mut laser = EpochAccumulator::default();
    for epoch in 0..PAIRED_MEASUREMENT_EPOCHS {
        let raw_first = raw_starts == epoch.is_multiple_of(2);
        if raw_first {
            measure_epoch_into(
                case,
                duration,
                timeout,
                &raw_operation,
                monitored_processes,
                &mut raw,
            )
            .await?;
            measure_epoch_into(
                case,
                duration,
                timeout,
                &laser_operation,
                monitored_processes,
                &mut laser,
            )
            .await?;
        } else {
            measure_epoch_into(
                case,
                duration,
                timeout,
                &laser_operation,
                monitored_processes,
                &mut laser,
            )
            .await?;
            measure_epoch_into(
                case,
                duration,
                timeout,
                &raw_operation,
                monitored_processes,
                &mut raw,
            )
            .await?;
        }
    }
    let raw_load = merge_load_results(raw.loads)?;
    let laser_load = merge_load_results(laser.loads)?;
    let mut raw_summary = summarize("raw-iggy", u8::from(!raw_starts) + 1, &raw_load, case, path);
    let mut laser_summary = summarize(
        path.label(),
        u8::from(raw_starts) + 1,
        &laser_load,
        case,
        path,
    );
    annotate_pairing(&mut raw_summary);
    annotate_pairing(&mut laser_summary);
    Ok((
        StreamingArmEvidence {
            summary: raw_summary,
            load: raw_load,
            processes: merge_process_measurements(raw.processes),
        },
        StreamingArmEvidence {
            summary: laser_summary,
            load: laser_load,
            processes: merge_process_measurements(laser.processes),
        },
    ))
}

async fn measure_epoch_into(
    case: &DirectStreamingCase,
    duration: Duration,
    timeout: Duration,
    operation: &Operation,
    monitored_processes: &[(String, u32)],
    accumulator: &mut EpochAccumulator,
) -> Result<(), BenchError> {
    let before = capture_processes(monitored_processes)?;
    let mut load = match case.offered_rate {
        Some(rate) => {
            run_open_loop_for(
                duration,
                rate,
                case.max_in_flight.unwrap_or(case.concurrency),
                timeout,
                dispatch(case),
                offset_operation(operation, accumulator.next_sequence),
            )
            .await?
        }
        None => {
            run_closed_loop_for(
                duration,
                case.concurrency,
                timeout,
                offset_operation(operation, accumulator.next_sequence),
            )
            .await?
        }
    };
    shift_load_sequences(&mut load, accumulator.next_sequence)?;
    accumulator.next_sequence = accumulator
        .next_sequence
        .checked_add(load.outcomes.offered)
        .ok_or_else(|| BenchError::Invalid("paired operation sequence overflowed".to_owned()))?;
    accumulator
        .processes
        .extend(finish_processes(before, "measurement")?);
    accumulator.loads.push(load);
    Ok(())
}

fn offset_operation(operation: &Operation, offset: u64) -> Operation {
    let operation = Arc::clone(operation);
    Arc::new(move |sequence| {
        let operation = Arc::clone(&operation);
        Box::pin(async move {
            let sequence = sequence
                .checked_add(offset)
                .ok_or_else(|| "paired operation sequence overflowed".to_owned())?;
            operation(sequence).await
        })
    })
}

fn shift_load_sequences(load: &mut LoadResult, offset: u64) -> Result<(), BenchError> {
    for sequence in &mut load.successful_sequences {
        *sequence = sequence
            .checked_add(offset)
            .ok_or_else(|| BenchError::Invalid("successful sequence overflowed".to_owned()))?;
    }
    for sample in &mut load.samples {
        sample.sequence = sample
            .sequence
            .checked_add(offset)
            .ok_or_else(|| BenchError::Invalid("failed sequence overflowed".to_owned()))?;
    }
    Ok(())
}

fn merge_load_results(loads: Vec<LoadResult>) -> Result<LoadResult, BenchError> {
    let mut loads = loads.into_iter();
    let mut merged = loads
        .next()
        .ok_or_else(|| BenchError::Invalid("paired measurement produced no epochs".to_owned()))?;
    for mut load in loads {
        let time_offset = elapsed_seconds_ceil(merged.elapsed);
        for point in &mut load.time_series {
            point.second = point.second.saturating_add(time_offset);
        }
        merged.time_series.append(&mut load.time_series);
        merged.elapsed = merged.elapsed.saturating_add(load.elapsed);
        add_outcomes(&mut merged.outcomes, &load.outcomes);
        merged
            .successful_sequences
            .append(&mut load.successful_sequences);
        merged.samples.append(&mut load.samples);
        merged
            .scheduled_response
            .add(&load.scheduled_response)
            .map_err(|error| {
                BenchError::Invalid(format!("scheduled histogram merge failed: {error}"))
            })?;
        merged.service.add(&load.service).map_err(|error| {
            BenchError::Invalid(format!("service histogram merge failed: {error}"))
        })?;
        merged
            .scheduler_lateness
            .add(&load.scheduler_lateness)
            .map_err(|error| {
                BenchError::Invalid(format!("scheduler histogram merge failed: {error}"))
            })?;
        merged
            .failed_service
            .add(&load.failed_service)
            .map_err(|error| {
                BenchError::Invalid(format!("failed-service histogram merge failed: {error}"))
            })?;
    }
    merged.successful_sequences.sort_unstable();
    merged.samples.sort_by_key(|sample| sample.sequence);
    Ok(merged)
}

fn elapsed_seconds_ceil(elapsed: Duration) -> u64 {
    elapsed
        .as_secs()
        .saturating_add(u64::from(elapsed.subsec_nanos() != 0))
}

fn add_outcomes(total: &mut OutcomeCounts, next: &OutcomeCounts) {
    total.offered = total.offered.saturating_add(next.offered);
    total.dispatched = total.dispatched.saturating_add(next.dispatched);
    total.completed = total.completed.saturating_add(next.completed);
    total.successful = total.successful.saturating_add(next.successful);
    total.failed = total.failed.saturating_add(next.failed);
    total.timed_out = total.timed_out.saturating_add(next.timed_out);
    total.missed = total.missed.saturating_add(next.missed);
    total.duplicates = total.duplicates.saturating_add(next.duplicates);
    total.gaps = total.gaps.saturating_add(next.gaps);
    total.ordering_violations = total
        .ordering_violations
        .saturating_add(next.ordering_violations);
    total.checksum_failures = total
        .checksum_failures
        .saturating_add(next.checksum_failures);
    total.late_arrivals = total.late_arrivals.saturating_add(next.late_arrivals);
}

fn merge_process_measurements(measurements: Vec<ProcessMeasurement>) -> Vec<ProcessMeasurement> {
    let mut merged = BTreeMap::<(String, String, u32), ProcessMeasurement>::new();
    for measurement in measurements {
        let key = (
            measurement.name.clone(),
            measurement.phase.clone(),
            measurement.delta.pid,
        );
        merged
            .entry(key)
            .and_modify(|total| {
                total.delta.cpu_seconds += measurement.delta.cpu_seconds;
                total.delta.final_rss_kib = measurement.delta.final_rss_kib;
                total.delta.voluntary_context_switches = total
                    .delta
                    .voluntary_context_switches
                    .saturating_add(measurement.delta.voluntary_context_switches);
                total.delta.involuntary_context_switches = total
                    .delta
                    .involuntary_context_switches
                    .saturating_add(measurement.delta.involuntary_context_switches);
                total.delta.read_bytes = total
                    .delta
                    .read_bytes
                    .saturating_add(measurement.delta.read_bytes);
                total.delta.write_bytes = total
                    .delta
                    .write_bytes
                    .saturating_add(measurement.delta.write_bytes);
            })
            .or_insert(measurement);
    }
    merged.into_values().collect()
}

fn annotate_pairing(summary: &mut StreamingArmSummary) {
    if let Some(configuration) = summary.configuration.as_object_mut() {
        configuration.insert(
            "pairing".to_owned(),
            serde_json::json!({
                "design": "counterbalanced_interleaved_epochs",
                "epochs": PAIRED_MEASUREMENT_EPOCHS,
            }),
        );
    }
}

async fn shutdown_producers(
    shutdown: ProducerShutdown,
    monitored_processes: &[(String, u32)],
) -> Result<(Vec<ProcessMeasurement>, Vec<ProcessMeasurement>), BenchError> {
    match shutdown {
        ProducerShutdown::Background(shutdown) => {
            shutdown_background(shutdown, monitored_processes).await
        }
        ProducerShutdown::Batching(producer) => {
            let before = capture_processes(monitored_processes)?;
            let producer = Arc::try_unwrap(producer)
                .map_err(|_| BenchError::Invalid("batching producer is still shared".to_owned()))?;
            producer.close().await.map_err(|error| sdk_error(&error))?;
            Ok((Vec::new(), finish_processes(before, "shutdown")?))
        }
    }
}

async fn shutdown_background(
    shutdown: BackgroundShutdown,
    monitored_processes: &[(String, u32)],
) -> Result<(Vec<ProcessMeasurement>, Vec<ProcessMeasurement>), BenchError> {
    let raw_before = capture_processes(monitored_processes)?;
    let raw = Arc::try_unwrap(shutdown.raw)
        .map_err(|_| BenchError::Invalid("raw background producer is still shared".to_owned()))?;
    raw.shutdown().await;
    let raw_processes = finish_processes(raw_before, "shutdown")?;

    let laser_before = capture_processes(monitored_processes)?;
    shutdown
        .laser
        .shutdown()
        .await
        .map_err(|error| sdk_error(&error))?;
    let laser_processes = finish_processes(laser_before, "shutdown")?;
    Ok((raw_processes, laser_processes))
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
) -> Result<Vec<ProcessMeasurement>, BenchError> {
    before
        .into_iter()
        .map(|(name, snapshot)| {
            let later = ProcessSnapshot::capture(snapshot.pid)?;
            Ok(ProcessMeasurement {
                name,
                phase: phase.to_owned(),
                delta: snapshot.delta(later)?,
            })
        })
        .collect()
}

async fn raw_producer(
    topic: &Topic,
    case: &DirectStreamingCase,
) -> Result<Arc<laser_sdk::iggy::prelude::IggyProducer>, BenchError> {
    raw_producer_with_partition(topic, case, None).await
}

async fn raw_producer_pool(
    lane_connections: &[Laser],
    stream: &str,
    topic: &str,
    case: &DirectStreamingCase,
) -> Result<Arc<Vec<Arc<laser_sdk::iggy::prelude::IggyProducer>>>, BenchError> {
    let pinned = pinned_producer_lanes(case);
    let mut producers = Vec::with_capacity(case.concurrency);
    for (lane, connection) in lane_connections.iter().enumerate().take(case.concurrency) {
        let partition = pinned
            .then(|| u32::try_from(lane))
            .transpose()
            .map_err(|_| BenchError::Invalid("producer lane exceeds u32".to_owned()))?;
        let lane_topic = connection.stream(stream).topic(topic);
        producers.push(raw_producer_with_partition(&lane_topic, case, partition).await?);
    }
    Ok(Arc::new(producers))
}

async fn raw_producer_with_partition(
    topic: &Topic,
    case: &DirectStreamingCase,
    partition: Option<u32>,
) -> Result<Arc<laser_sdk::iggy::prelude::IggyProducer>, BenchError> {
    let batch_length = u32::try_from(case.batch_size)
        .map_err(|_| BenchError::Invalid("batch size exceeds u32".to_owned()))?;
    let partitioning = partition.map_or_else(Partitioning::balanced, Partitioning::partition_id);
    let producer = topic
        .iggy_producer()
        .map_err(|error| sdk_error(&error))?
        .direct(
            DirectConfig::builder()
                .batch_length(batch_length)
                .linger_time(IggyDuration::from(Duration::ZERO))
                .build(),
        )
        .partitioning(partitioning)
        .send_retries(
            Some(3),
            Some(
                NonZeroIggyDuration::try_from(Duration::from_secs(1))
                    .expect("retry interval should be non-zero"),
            ),
        )
        .do_not_create_stream_if_not_exists()
        .do_not_create_topic_if_not_exists()
        .build();
    producer.init().await.map_err(|error| iggy_error(&error))?;
    Ok(Arc::new(producer))
}

async fn laser_producer_pool(
    lane_connections: &[Laser],
    stream: &str,
    topic: &str,
    case: &DirectStreamingCase,
) -> Result<Arc<Vec<Producer>>, BenchError> {
    let pinned = pinned_producer_lanes(case);
    let mut producers = Vec::with_capacity(case.concurrency);
    for (lane, connection) in lane_connections.iter().enumerate().take(case.concurrency) {
        let partition = pinned
            .then(|| u32::try_from(lane))
            .transpose()
            .map_err(|_| BenchError::Invalid("producer lane exceeds u32".to_owned()))?;
        let lane_topic = connection.stream(stream).topic(topic);
        producers.push(laser_producer_with_partition(&lane_topic, case, partition).await?);
    }
    Ok(Arc::new(producers))
}

async fn laser_producer_with_partition(
    topic: &Topic,
    case: &DirectStreamingCase,
    partition: Option<u32>,
) -> Result<Producer, BenchError> {
    let batch_length = u32::try_from(case.batch_size)
        .map_err(|_| BenchError::Invalid("batch size exceeds u32".to_owned()))?;
    let mut builder = topic
        .producer()
        .batch_length(batch_length)
        .linger(Duration::ZERO)
        .retries(Some(3), Some(Duration::from_secs(1)))
        .create_stream(false)
        .create_topic(false);
    if let Some(partition) = partition {
        builder = builder.routing(Routing::Partition(partition));
    }
    builder.build().await.map_err(|error| sdk_error(&error))
}

fn dispatch(case: &DirectStreamingCase) -> Dispatch {
    if case.spin_dispatch {
        Dispatch::SpinWindow
    } else {
        Dispatch::Sleep
    }
}

fn pinned_producer_lanes(case: &DirectStreamingCase) -> bool {
    case.concurrency > 1 && usize::try_from(case.partitions) == Ok(case.concurrency)
}

async fn raw_background_producer(
    topic: &Topic,
    case: &DirectStreamingCase,
) -> Result<Arc<laser_sdk::iggy::prelude::IggyProducer>, BenchError> {
    let producer = topic
        .iggy_producer()
        .map_err(|error| sdk_error(&error))?
        .background(background_config(case))
        .partitioning(Partitioning::balanced())
        .send_retries(
            Some(3),
            Some(
                NonZeroIggyDuration::try_from(Duration::from_secs(1))
                    .expect("retry interval should be non-zero"),
            ),
        )
        .do_not_create_stream_if_not_exists()
        .do_not_create_topic_if_not_exists()
        .build();
    producer.init().await.map_err(|error| iggy_error(&error))?;
    Ok(Arc::new(producer))
}

async fn laser_background_producer(
    topic: &Topic,
    case: &DirectStreamingCase,
) -> Result<Producer, BenchError> {
    topic
        .producer()
        .background(background_config(case))
        .retries(Some(3), Some(Duration::from_secs(1)))
        .create_stream(false)
        .create_topic(false)
        .build()
        .await
        .map_err(|error| sdk_error(&error))
}

fn background_config(case: &DirectStreamingCase) -> BackgroundConfig {
    BackgroundConfig::builder()
        .num_shards(1)
        .batch_size(0)
        .batch_length(case.batch_size)
        .linger_time(IggyDuration::from(Duration::from_millis(1)))
        .max_buffer_size(IggyByteSize::from(32 * 1_024 * 1_024))
        .max_in_flight(1)
        .build()
}

fn batching_producer(
    topic: &Topic,
    case: &DirectStreamingCase,
    path: StreamingProducerPath,
) -> Result<BatchingProducer, BenchError> {
    let builder = topic.batching().map_err(|error| sdk_error(&error))?;
    let builder =
        match path {
            StreamingProducerPath::StreamBatchingRecord => builder
                .max_records(case.batch_size)
                .max_bytes(usize::MAX)
                .linger(Duration::from_hours(1)),
            StreamingProducerPath::StreamBatchingByte => {
                builder
                    .max_records(usize::MAX)
                    .max_bytes(case.payload_bytes.checked_mul(case.batch_size).ok_or_else(
                        || BenchError::Invalid("batching byte threshold exceeds usize".to_owned()),
                    )?)
                    .linger(Duration::from_hours(1))
            }
            StreamingProducerPath::StreamBatchingLinger => builder
                .max_records(usize::MAX)
                .max_bytes(usize::MAX)
                .linger(Duration::from_millis(1)),
            _ => {
                return Err(BenchError::Invalid(
                    "non-batching path cannot build a batching producer".to_owned(),
                ));
            }
        };
    Ok(builder.build())
}

fn batching_operation(
    producer: Arc<BatchingProducer>,
    payload: Bytes,
    batch_size: usize,
    id_offset: u64,
) -> Operation {
    Arc::new(move |sequence| {
        let producer = Arc::clone(&producer);
        let payload = payload.clone();
        Box::pin(async move {
            for index in 0..batch_size {
                producer
                    .send(
                        record_payload_vec(
                            &payload,
                            record_id(id_offset, sequence, batch_size, index)?,
                        )?,
                        BTreeMap::new(),
                    )
                    .await
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        })
    })
}

fn raw_operation(
    producer: Arc<laser_sdk::iggy::prelude::IggyProducer>,
    payload: Bytes,
    batch_size: usize,
    id_offset: u64,
) -> Operation {
    Arc::new(move |sequence| {
        let producer = Arc::clone(&producer);
        let payload = payload.clone();
        Box::pin(async move {
            let messages = (0..batch_size)
                .map(|index| {
                    IggyMessage::builder()
                        .payload(record_payload(
                            &payload,
                            record_id(id_offset, sequence, batch_size, index)?,
                        )?)
                        .build()
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            producer
                .send(messages)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    })
}

fn raw_pool_operation(
    producers: Arc<Vec<Arc<laser_sdk::iggy::prelude::IggyProducer>>>,
    payload: Bytes,
    batch_size: usize,
    id_offset: u64,
) -> Operation {
    Arc::new(move |sequence| {
        let producers = Arc::clone(&producers);
        let payload = payload.clone();
        Box::pin(async move {
            let producer = producer_for_sequence(&producers, sequence)?;
            let messages = (0..batch_size)
                .map(|index| {
                    IggyMessage::builder()
                        .payload(record_payload(
                            &payload,
                            record_id(id_offset, sequence, batch_size, index)?,
                        )?)
                        .build()
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?;
            producer
                .send(messages)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    })
}

fn laser_operation(
    producer: Producer,
    payload: Bytes,
    batch_size: usize,
    id_offset: u64,
) -> Operation {
    Arc::new(move |sequence| {
        let producer = producer.clone();
        let payload = payload.clone();
        Box::pin(async move {
            let messages = (0..batch_size)
                .map(|index| {
                    record_payload(&payload, record_id(id_offset, sequence, batch_size, index)?)
                        .map(ProducerMessage::new)
                })
                .collect::<Result<Vec<_>, String>>()?;
            producer
                .send_batch(messages)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    })
}

fn laser_pool_operation(
    producers: Arc<Vec<Producer>>,
    payload: Bytes,
    batch_size: usize,
    id_offset: u64,
) -> Operation {
    Arc::new(move |sequence| {
        let producers = Arc::clone(&producers);
        let payload = payload.clone();
        Box::pin(async move {
            let producer = producer_for_sequence(&producers, sequence)?.clone();
            let messages = (0..batch_size)
                .map(|index| {
                    record_payload(&payload, record_id(id_offset, sequence, batch_size, index)?)
                        .map(ProducerMessage::new)
                })
                .collect::<Result<Vec<_>, String>>()?;
            producer
                .send_batch(messages)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    })
}

fn producer_for_sequence<T>(producers: &[T], sequence: u64) -> Result<&T, String> {
    let count =
        u64::try_from(producers.len()).map_err(|_| "producer count exceeds u64".to_owned())?;
    if count == 0 {
        return Err("producer pool is empty".to_owned());
    }
    let index =
        usize::try_from(sequence % count).map_err(|_| "producer index exceeds usize".to_owned())?;
    producers
        .get(index)
        .ok_or_else(|| "producer index is unavailable".to_owned())
}

fn raw_one_shot_operation(
    laser: Laser,
    stream: &str,
    topic: &str,
    payload: Bytes,
    id_offset: u64,
) -> Result<Operation, BenchError> {
    let stream = Identifier::named(stream).map_err(|error| iggy_error(&error))?;
    let topic = Identifier::named(topic).map_err(|error| iggy_error(&error))?;
    let partitioning = Arc::new(Partitioning::balanced());
    Ok(Arc::new(move |sequence| {
        let laser = laser.clone();
        let stream = stream.clone();
        let topic = topic.clone();
        let partitioning = Arc::clone(&partitioning);
        let payload = payload.clone();
        Box::pin(async move {
            let mut messages = vec![
                IggyMessage::builder()
                    .payload(record_payload(
                        &payload,
                        record_id(id_offset, sequence, 1, 0)?,
                    )?)
                    .build()
                    .map_err(|error| error.to_string())?,
            ];
            laser
                .client()
                .send_messages(&stream, &topic, &partitioning, &mut messages)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    }))
}

fn fluent_operation(topic: Topic, payload: Bytes, id_offset: u64) -> Operation {
    Arc::new(move |sequence| {
        let topic = topic.clone();
        let payload = payload.clone();
        Box::pin(async move {
            topic
                .publish()
                .payload(record_payload_vec(
                    &payload,
                    record_id(id_offset, sequence, 1, 0)?,
                )?)
                .send()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })
    })
}

async fn warmup(
    case: &DirectStreamingCase,
    timeout: Duration,
    operation: &Operation,
) -> Result<(), BenchError> {
    if case.warmup_seconds == 0 {
        return Ok(());
    }
    let result = run_closed_loop_for(
        Duration::from_secs(case.warmup_seconds),
        case.concurrency,
        timeout,
        Arc::clone(operation),
    )
    .await?;
    if result.outcomes.successful == 0
        || result.outcomes.failed != 0
        || result.outcomes.timed_out != 0
    {
        return Err(BenchError::Invalid(
            "streaming warmup did not complete successfully".to_owned(),
        ));
    }
    Ok(())
}

fn summarize(
    arm: &str,
    order: u8,
    result: &LoadResult,
    case: &DirectStreamingCase,
    path: StreamingProducerPath,
) -> StreamingArmSummary {
    let successful_records = result
        .outcomes
        .successful
        .saturating_mul(u64::try_from(case.batch_size).unwrap_or(u64::MAX));
    let successful_bytes =
        successful_records.saturating_mul(u64::try_from(case.payload_bytes).unwrap_or(u64::MAX));
    let service_p999 =
        (result.outcomes.successful >= 100_000).then(|| result.service.value_at_quantile(0.999));
    let scheduled_p99 = result.scheduled_response.value_at_quantile(0.99);
    let service_p99 = result.service.value_at_quantile(0.99);
    StreamingArmSummary {
        arm: arm.to_owned(),
        order,
        elapsed_ns: u64::try_from(result.elapsed.as_nanos()).unwrap_or(u64::MAX),
        batches_per_second: per_second(result.outcomes.successful, result.elapsed),
        records_per_second: per_second(successful_records, result.elapsed),
        payload_bytes_per_second: per_second(successful_bytes, result.elapsed),
        scheduled_p50_ns: result.scheduled_response.value_at_quantile(0.5),
        scheduled_p90_ns: result.scheduled_response.value_at_quantile(0.9),
        scheduled_p99_ns: scheduled_p99,
        service_p50_ns: result.service.value_at_quantile(0.5),
        service_p90_ns: result.service.value_at_quantile(0.9),
        service_p99_ns: service_p99,
        scheduler_lateness_p99_ns: result.scheduler_lateness.value_at_quantile(0.99),
        primary_p99_ns: case.offered_rate.map_or(service_p99, |_| scheduled_p99),
        p99_supported: result.outcomes.successful >= 10_000,
        service_p999_ns: service_p999,
        time_series: result.time_series.clone(),
        configuration: producer_configuration(path, arm, case),
        outcomes: result.outcomes.clone(),
    }
}

fn summarize_consumer(
    arm: &str,
    order: u8,
    result: &LoadResult,
    case: &DirectStreamingCase,
    path: StreamingConsumerPath,
) -> StreamingArmSummary {
    let successful_bytes = result
        .outcomes
        .successful
        .saturating_mul(u64::try_from(case.payload_bytes).unwrap_or(u64::MAX));
    let service_p99 = result.service.value_at_quantile(0.99);
    StreamingArmSummary {
        arm: arm.to_owned(),
        order,
        elapsed_ns: u64::try_from(result.elapsed.as_nanos()).unwrap_or(u64::MAX),
        batches_per_second: per_second(result.outcomes.successful, result.elapsed),
        records_per_second: per_second(result.outcomes.successful, result.elapsed),
        payload_bytes_per_second: per_second(successful_bytes, result.elapsed),
        scheduled_p50_ns: result.scheduled_response.value_at_quantile(0.5),
        scheduled_p90_ns: result.scheduled_response.value_at_quantile(0.9),
        scheduled_p99_ns: result.scheduled_response.value_at_quantile(0.99),
        service_p50_ns: result.service.value_at_quantile(0.5),
        service_p90_ns: result.service.value_at_quantile(0.9),
        service_p99_ns: service_p99,
        scheduler_lateness_p99_ns: result.scheduler_lateness.value_at_quantile(0.99),
        primary_p99_ns: service_p99,
        p99_supported: result.outcomes.successful >= 10_000,
        service_p999_ns: (result.outcomes.successful >= 100_000)
            .then(|| result.service.value_at_quantile(0.999)),
        time_series: result.time_series.clone(),
        configuration: serde_json::json!({
            "mode": match path {
                StreamingConsumerPath::StreamConsumerPartition => "partition",
                StreamingConsumerPath::StreamConsumerGroup => "consumer-group",
                StreamingConsumerPath::StreamCursor => "cursor",
            },
            "latency_boundary": "poll_dispatch_to_record_delivery",
            "batch_length": case.batch_size,
            "poll_interval": null,
            "start": "first",
            "auto_commit": "disabled",
            "allow_replay": true,
            "consumer_lanes": case.concurrency,
            "throughput_scope": "aggregate_all_lanes",
            "connections": case.concurrency,
            "connection_topology": "one_dedicated_connection_per_consumer_lane",
        }),
        outcomes: result.outcomes.clone(),
    }
}

fn summarize_cursor(
    arm: &str,
    order: u8,
    result: &LoadResult,
    case: &DirectStreamingCase,
) -> StreamingArmSummary {
    let successful_records = if result.outcomes.successful == 1 {
        case.batches
    } else {
        0
    };
    let successful_bytes =
        successful_records.saturating_mul(u64::try_from(case.payload_bytes).unwrap_or(u64::MAX));
    let service_p99 = result.service.value_at_quantile(0.99);
    StreamingArmSummary {
        arm: arm.to_owned(),
        order,
        elapsed_ns: u64::try_from(result.elapsed.as_nanos()).unwrap_or(u64::MAX),
        batches_per_second: per_second(result.outcomes.successful, result.elapsed),
        records_per_second: per_second(successful_records, result.elapsed),
        payload_bytes_per_second: per_second(successful_bytes, result.elapsed),
        scheduled_p50_ns: result.scheduled_response.value_at_quantile(0.5),
        scheduled_p90_ns: result.scheduled_response.value_at_quantile(0.9),
        scheduled_p99_ns: result.scheduled_response.value_at_quantile(0.99),
        service_p50_ns: result.service.value_at_quantile(0.5),
        service_p90_ns: result.service.value_at_quantile(0.9),
        service_p99_ns: service_p99,
        scheduler_lateness_p99_ns: result.scheduler_lateness.value_at_quantile(0.99),
        primary_p99_ns: service_p99,
        p99_supported: result.outcomes.successful >= 10_000,
        service_p999_ns: None,
        time_series: result.time_series.clone(),
        configuration: serde_json::json!({
            "mode": "full-drain",
            "latency_boundary": "full_cursor_drain",
            "poll_batch_length": case.batch_size,
            "partitions": case.partitions,
            "records": case.batches,
            "offset_owner": "client",
            "auto_commit": false,
            "connections": 1,
        }),
        outcomes: result.outcomes.clone(),
    }
}

fn summarize_end_to_end(
    arm: &str,
    order: u8,
    result: &LoadResult,
    case: &DirectStreamingCase,
) -> StreamingArmSummary {
    let successful_records = result
        .outcomes
        .successful
        .saturating_mul(u64::try_from(case.batch_size).unwrap_or(u64::MAX));
    let successful_bytes =
        successful_records.saturating_mul(u64::try_from(case.payload_bytes).unwrap_or(u64::MAX));
    let scheduled_p99 = result.scheduled_response.value_at_quantile(0.99);
    let service_p99 = result.service.value_at_quantile(0.99);
    StreamingArmSummary {
        arm: arm.to_owned(),
        order,
        elapsed_ns: u64::try_from(result.elapsed.as_nanos()).unwrap_or(u64::MAX),
        batches_per_second: per_second(result.outcomes.successful, result.elapsed),
        records_per_second: per_second(successful_records, result.elapsed),
        payload_bytes_per_second: per_second(successful_bytes, result.elapsed),
        scheduled_p50_ns: result.scheduled_response.value_at_quantile(0.5),
        scheduled_p90_ns: result.scheduled_response.value_at_quantile(0.9),
        scheduled_p99_ns: scheduled_p99,
        service_p50_ns: result.service.value_at_quantile(0.5),
        service_p90_ns: result.service.value_at_quantile(0.9),
        service_p99_ns: service_p99,
        scheduler_lateness_p99_ns: result.scheduler_lateness.value_at_quantile(0.99),
        primary_p99_ns: case.offered_rate.map_or(service_p99, |_| scheduled_p99),
        p99_supported: if case.offered_rate.is_some() {
            result.outcomes.successful >= 10_000
        } else {
            result.service.len() >= 10_000
        },
        service_p999_ns: (result.service.len() >= 100_000)
            .then(|| result.service.value_at_quantile(0.999)),
        time_series: result.time_series.clone(),
        configuration: serde_json::json!({
            "mode": "producer-to-consumer",
            "latency_boundary": "producer_dispatch_to_consumer_receive",
            "producer": "direct",
            "consumer": "partition-readers",
            "producer_lanes": case.concurrency,
            "consumer_lanes": case.partitions,
            "batch_length": case.batch_size,
            "consumer_batch_length": case.batch_size,
            "poll_interval": null,
            "auto_commit": "none",
            "producer_connections": case.concurrency,
            "consumer_connections": case.partitions,
            "connection_topology": "one_dedicated_connection_per_lane_and_per_partition_reader",
            "latency_clock": "producer-side-monotonic",
            "correlation": "record-sequence-id",
            "routing": producer_routing(case),
        }),
        outcomes: result.outcomes.clone(),
    }
}

fn producer_configuration(
    path: StreamingProducerPath,
    arm: &str,
    case: &DirectStreamingCase,
) -> serde_json::Value {
    if arm == "raw-iggy"
        && matches!(
            path,
            StreamingProducerPath::StreamBatchingRecord
                | StreamingProducerPath::StreamBatchingByte
                | StreamingProducerPath::StreamBatchingLinger
        )
    {
        return serde_json::json!({
            "mode": "direct",
            "latency_boundary": "publish_request_to_acknowledgement",
            "batch_length": case.batch_size,
            "linger_millis": 0,
            "routing": "balanced",
        });
    }
    match path {
        StreamingProducerPath::StreamDirect => serde_json::json!({
            "mode": "direct",
            "latency_boundary": "publish_request_to_acknowledgement",
            "producer_lanes": case.concurrency,
            "batch_length": case.batch_size,
            "linger_millis": 0,
            "retries": 3,
            "routing": producer_routing(case),
            "connections": case.concurrency,
            "connection_topology": "one_dedicated_connection_per_producer_lane",
        }),
        StreamingProducerPath::StreamDirectAa => serde_json::json!({
            "mode": "direct-aa-calibration",
            "latency_boundary": "publish_request_to_acknowledgement",
            "producer_lanes": case.concurrency,
            "batch_length": case.batch_size,
            "linger_millis": 0,
            "retries": 3,
            "routing": producer_routing(case),
            "implementation": "raw-iggy",
            "connections": case.concurrency,
            "connection_topology": "one_dedicated_connection_per_producer_lane",
        }),
        StreamingProducerPath::StreamFluent => serde_json::json!({
            "mode": if arm == "raw-iggy" { "raw-one-shot" } else { "fluent-one-shot" },
            "latency_boundary": "publish_request_to_acknowledgement",
            "batch_length": 1,
            "routing": "balanced",
        }),
        StreamingProducerPath::StreamBackground => serde_json::json!({
            "mode": "background",
            "latency_boundary": "enqueue_to_background_acknowledgement",
            "batch_length": case.batch_size,
            "batch_bytes": 0,
            "linger_millis": 1,
            "shards": 1,
            "max_in_flight": 1,
            "max_buffer_bytes": 32 * 1_024 * 1_024,
            "backpressure": "block",
            "routing": "balanced",
        }),
        StreamingProducerPath::StreamBatchingRecord => serde_json::json!({
            "mode": "sdk-batching",
            "latency_boundary": "enqueue_to_batch_acknowledgement",
            "trigger": "records",
            "max_records": case.batch_size,
            "max_bytes": usize::MAX,
            "linger_millis": 3_600_000,
            "routing": "balanced",
        }),
        StreamingProducerPath::StreamBatchingByte => serde_json::json!({
            "mode": "sdk-batching",
            "latency_boundary": "enqueue_to_batch_acknowledgement",
            "trigger": "bytes",
            "max_records": usize::MAX,
            "max_bytes": case.payload_bytes.saturating_mul(case.batch_size),
            "linger_millis": 3_600_000,
            "routing": "balanced",
        }),
        StreamingProducerPath::StreamBatchingLinger => serde_json::json!({
            "mode": "sdk-batching",
            "latency_boundary": "enqueue_to_batch_acknowledgement",
            "trigger": "linger",
            "max_records": usize::MAX,
            "max_bytes": usize::MAX,
            "linger_millis": 1,
            "routing": "balanced",
        }),
    }
}

fn producer_routing(case: &DirectStreamingCase) -> &'static str {
    if pinned_producer_lanes(case) {
        "pinned_partition_per_producer"
    } else {
        "balanced"
    }
}

fn validate_case(case: &DirectStreamingCase) -> Result<(), BenchError> {
    if case.payload_bytes < size_of::<u64>()
        || case.batch_size == 0
        || case.batches == 0
        || case.duration_seconds == 0
        || case.concurrency == 0
        || case.partitions == 0
        || case.timeout_millis == 0
    {
        return Err(BenchError::Invalid(
            "direct streaming dimensions must be nonzero".to_owned(),
        ));
    }
    Ok(())
}

fn validate_path_case(
    case: &DirectStreamingCase,
    path: StreamingProducerPath,
) -> Result<(), BenchError> {
    validate_case(case)?;
    if path == StreamingProducerPath::StreamFluent && case.batch_size != 1 {
        return Err(BenchError::Invalid(
            "stream-fluent requires batch_size = 1".to_owned(),
        ));
    }
    if path == StreamingProducerPath::StreamBatchingLinger && case.batch_size != 1 {
        return Err(BenchError::Invalid(
            "stream-batching-linger requires batch_size = 1".to_owned(),
        ));
    }
    Ok(())
}

async fn validate_topic(
    topic: &Topic,
    payload: &Bytes,
    expected_ids: &[u64],
    explained_ids: &[u64],
) -> Result<crate::correctness::CorrectnessSummary, BenchError> {
    let policy = OraclePolicy {
        allow_duplicates: false,
    };
    let mut oracle = CorrectnessOracle::new(expected_ids.iter().copied(), policy)
        .with_explained(explained_ids.iter().copied());
    let mut cursor = topic
        .replay()
        .map_err(|error| sdk_error(&error))?
        .batch(10_000);
    loop {
        let records = cursor.poll().await.map_err(|error| sdk_error(&error))?;
        if records.is_empty() {
            break;
        }
        for record in records {
            let id = decode_record_id(&record.payload)?;
            if id < MEASUREMENT_RECORD_OFFSET {
                continue;
            }
            let expected = record_payload(payload, id).map_err(BenchError::Invalid)?;
            oracle.observe(ObservedRecord {
                id,
                partition: record.id.partition_id,
                partition_sequence: record.id.offset,
                payload: &record.payload,
                checksum: checksum(&expected),
            });
        }
    }
    Ok(oracle.finish())
}

fn explained_ids(case: &DirectStreamingCase, load: &LoadResult) -> Result<Vec<u64>, BenchError> {
    let mut ids = Vec::with_capacity(load.samples.len().saturating_mul(case.batch_size));
    for sample in &load.samples {
        for index in 0..case.batch_size {
            ids.push(
                record_id(
                    MEASUREMENT_RECORD_OFFSET,
                    sample.sequence,
                    case.batch_size,
                    index,
                )
                .map_err(BenchError::Invalid)?,
            );
        }
    }
    Ok(ids)
}

fn expected_ids(case: &DirectStreamingCase, load: &LoadResult) -> Result<Vec<u64>, BenchError> {
    let measured_records = checked_records(case.batches, case.batch_size)?;
    let capacity = usize::try_from(measured_records)
        .map_err(|_| BenchError::Invalid("expected record count exceeds usize".to_owned()))?;
    let mut ids = Vec::with_capacity(capacity);
    for sequence in &load.successful_sequences {
        for index in 0..case.batch_size {
            ids.push(
                record_id(MEASUREMENT_RECORD_OFFSET, *sequence, case.batch_size, index)
                    .map_err(BenchError::Invalid)?,
            );
        }
    }
    Ok(ids)
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

fn checked_records(batches: u64, batch_size: usize) -> Result<u64, BenchError> {
    batches
        .checked_mul(
            u64::try_from(batch_size)
                .map_err(|_| BenchError::Invalid("batch size exceeds u64".to_owned()))?,
        )
        .ok_or_else(|| BenchError::Invalid("streaming record count exceeds u64".to_owned()))
}

fn record_id(offset: u64, sequence: u64, batch_size: usize, index: usize) -> Result<u64, String> {
    let batch_size = u64::try_from(batch_size).map_err(|_| "batch size exceeds u64".to_owned())?;
    let index = u64::try_from(index).map_err(|_| "record index exceeds u64".to_owned())?;
    offset
        .checked_add(
            sequence
                .checked_mul(batch_size)
                .and_then(|base| base.checked_add(index))
                .ok_or_else(|| "record ID exceeds u64".to_owned())?,
        )
        .ok_or_else(|| "record ID exceeds u64".to_owned())
}

fn record_payload(payload: &Bytes, id: u64) -> Result<Bytes, String> {
    record_payload_vec(payload, id).map(Bytes::from)
}

fn record_payload_vec(payload: &Bytes, id: u64) -> Result<Vec<u8>, String> {
    if payload.len() < size_of::<u64>() {
        return Err("streaming payload must fit a record ID".to_owned());
    }
    let mut record = payload.to_vec();
    record[..size_of::<u64>()].copy_from_slice(&id.to_le_bytes());
    Ok(record)
}

fn decode_record_id(payload: &[u8]) -> Result<u64, BenchError> {
    let encoded = payload.get(..size_of::<u64>()).ok_or_else(|| {
        BenchError::Invalid("observed streaming payload has no record ID".to_owned())
    })?;
    Ok(u64::from_le_bytes(encoded.try_into().map_err(|_| {
        BenchError::Invalid("observed streaming record ID is invalid".to_owned())
    })?))
}

fn seeded_payload(size: usize, seed: u64) -> Bytes {
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

fn sdk_error(error: &laser_sdk::LaserError) -> BenchError {
    BenchError::Invalid(format!("Laser streaming operation failed: {error}"))
}

fn iggy_error(error: &laser_sdk::iggy::prelude::IggyError) -> BenchError {
    BenchError::Invalid(format!("Iggy streaming operation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_same_seed_when_payloads_are_generated_then_should_be_identical() {
        assert_eq!(seeded_payload(1_024, 42), seeded_payload(1_024, 42));
        assert_ne!(seeded_payload(1_024, 42), seeded_payload(1_024, 43));
    }

    #[test]
    fn given_short_run_when_summarized_then_should_omit_unsupported_p999() {
        let mut service = hdrhistogram::Histogram::new(3).expect("histogram should be valid");
        service.record(10).expect("sample should fit");
        let result = LoadResult {
            elapsed: Duration::from_secs(1),
            outcomes: OutcomeCounts {
                successful: 1,
                ..OutcomeCounts::default()
            },
            successful_sequences: vec![0],
            samples: Vec::new(),
            time_series: Vec::new(),
            scheduled_response: service.clone(),
            service,
            scheduler_lateness: hdrhistogram::Histogram::new(3).expect("histogram should be valid"),
            failed_service: hdrhistogram::Histogram::new(3).expect("histogram should be valid"),
        };
        let summary = summarize(
            "raw",
            1,
            &result,
            &DirectStreamingCase {
                payload_bytes: 10,
                batch_size: 100,
                batches: 1,
                duration_seconds: 1,
                concurrency: 1,
                partitions: 1,
                warmup_seconds: 0,
                timeout_millis: 1_000,
                offered_rate: None,
                spin_dispatch: false,
                max_in_flight: None,
            },
            StreamingProducerPath::StreamDirect,
        );
        assert!((summary.records_per_second - 100.0).abs() < f64::EPSILON);
        assert_eq!(summary.service_p999_ns, None);
    }

    #[test]
    fn given_cursor_driver_when_parsed_then_should_select_cursor_path() {
        assert_eq!(
            "stream_cursor"
                .parse::<StreamingConsumerPath>()
                .expect("cursor driver should parse"),
            StreamingConsumerPath::StreamCursor
        );
    }

    #[test]
    fn given_end_to_end_driver_when_parsed_then_should_select_pipeline_path() {
        assert_eq!(
            "stream_end_to_end"
                .parse::<StreamingPipelinePath>()
                .expect("pipeline driver should parse"),
            StreamingPipelinePath::StreamEndToEnd
        );
    }

    #[test]
    fn given_direct_streaming_drivers_when_classified_then_should_require_c2_equivalence() {
        assert!(is_c2_driver("stream_direct"));
        assert!(is_c2_driver("stream_consumer_partition"));
        assert!(is_c2_driver("stream_consumer_group"));
        assert!(!is_c2_driver("stream_end_to_end"));
        assert!(!is_c2_driver("stream_fluent"));
        assert!(!is_c2_driver("stream_background"));
    }

    #[test]
    fn given_aa_driver_when_parsed_and_formatted_then_should_keep_snake_case() {
        let path = "stream_direct_aa"
            .parse::<StreamingProducerPath>()
            .expect("A/A driver should parse");

        assert_eq!(path, StreamingProducerPath::StreamDirectAa);
        assert_eq!(path.to_string(), "stream_direct_aa");
        assert_eq!(path.label(), "raw-iggy-b");
    }

    #[test]
    fn given_successful_cursor_drain_when_summarized_then_should_count_records() {
        let mut service = hdrhistogram::Histogram::new(3).expect("histogram should be valid");
        service.record(1_000_000).expect("drain sample should fit");
        let result = LoadResult {
            elapsed: Duration::from_secs(1),
            outcomes: OutcomeCounts {
                offered: 1,
                dispatched: 1,
                completed: 1,
                successful: 1,
                ..OutcomeCounts::default()
            },
            successful_sequences: vec![0],
            samples: Vec::new(),
            time_series: Vec::new(),
            scheduled_response: service.clone(),
            service,
            scheduler_lateness: hdrhistogram::Histogram::new(3).expect("histogram should be valid"),
            failed_service: hdrhistogram::Histogram::new(3).expect("histogram should be valid"),
        };
        let summary = summarize_cursor(
            "laser-cursor",
            1,
            &result,
            &DirectStreamingCase {
                payload_bytes: 1_024,
                batch_size: 37,
                batches: 251,
                duration_seconds: 1,
                concurrency: 1,
                partitions: 4,
                warmup_seconds: 0,
                timeout_millis: 1_000,
                offered_rate: None,
                spin_dispatch: false,
                max_in_flight: None,
            },
        );
        assert!((summary.records_per_second - 251.0).abs() < f64::EPSILON);
        assert_eq!(summary.outcomes.successful, 1);
        assert_eq!(summary.configuration["mode"], "full-drain");
    }

    #[test]
    fn given_successful_end_to_end_run_when_summarized_then_should_report_partition_reader_path() {
        let mut scheduled =
            hdrhistogram::Histogram::new(3).expect("scheduled histogram should be valid");
        scheduled
            .record(1_200_000)
            .expect("scheduled sample should fit");
        let mut service =
            hdrhistogram::Histogram::new(3).expect("service histogram should be valid");
        service.record(900_000).expect("service sample should fit");
        let result = LoadResult {
            elapsed: Duration::from_secs(1),
            outcomes: OutcomeCounts {
                offered: 8,
                dispatched: 8,
                completed: 8,
                successful: 8,
                ..OutcomeCounts::default()
            },
            successful_sequences: (0..8).collect(),
            samples: Vec::new(),
            time_series: Vec::new(),
            scheduled_response: scheduled,
            service,
            scheduler_lateness: hdrhistogram::Histogram::new(3)
                .expect("lateness histogram should be valid"),
            failed_service: hdrhistogram::Histogram::new(3)
                .expect("failed histogram should be valid"),
        };
        let summary = summarize_end_to_end(
            "laser-end-to-end",
            1,
            &result,
            &DirectStreamingCase {
                payload_bytes: 256,
                batch_size: 1,
                batches: 8,
                duration_seconds: 1,
                concurrency: 4,
                partitions: 4,
                warmup_seconds: 1,
                timeout_millis: 1_000,
                offered_rate: None,
                spin_dispatch: false,
                max_in_flight: None,
            },
        );
        assert!((summary.records_per_second - 8.0).abs() < f64::EPSILON);
        assert_eq!(summary.configuration["mode"], "producer-to-consumer");
        assert_eq!(summary.configuration["consumer"], "partition-readers");
        assert_eq!(
            summary.configuration["latency_clock"],
            "producer-side-monotonic"
        );
        assert_eq!(
            summary.configuration["routing"],
            "pinned_partition_per_producer"
        );
        assert_eq!(summary.configuration["producer_lanes"], 4);
        assert_eq!(summary.configuration["consumer_lanes"], 4);
    }

    #[test]
    fn given_different_producer_and_partition_counts_when_routing_then_should_use_balancing() {
        let case = DirectStreamingCase {
            payload_bytes: 256,
            batch_size: 1,
            batches: 8,
            duration_seconds: 1,
            concurrency: 2,
            partitions: 4,
            warmup_seconds: 1,
            timeout_millis: 1_000,
            offered_rate: None,
            spin_dispatch: false,
            max_in_flight: None,
        };

        assert_eq!(producer_routing(&case), "balanced");
    }

    #[test]
    fn given_epoch_results_when_merged_then_should_preserve_unique_sequences_and_samples() {
        let mut first = load_result(0, 10);
        let mut second = load_result(0, 20);
        shift_load_sequences(&mut first, 0).expect("first epoch should shift");
        shift_load_sequences(&mut second, 1).expect("second epoch should shift");

        let merged = merge_load_results(vec![first, second]).expect("epochs should merge");

        assert_eq!(merged.elapsed, Duration::from_secs(2));
        assert_eq!(merged.outcomes.successful, 2);
        assert_eq!(merged.successful_sequences, vec![0, 1]);
        assert_eq!(merged.service.len(), 2);
    }

    fn load_result(sequence: u64, latency: u64) -> LoadResult {
        let mut histogram = hdrhistogram::Histogram::new(3).expect("histogram should be valid");
        histogram.record(latency).expect("sample should fit");
        LoadResult {
            elapsed: Duration::from_secs(1),
            outcomes: OutcomeCounts {
                offered: 1,
                dispatched: 1,
                completed: 1,
                successful: 1,
                ..OutcomeCounts::default()
            },
            successful_sequences: vec![sequence],
            samples: Vec::new(),
            time_series: Vec::new(),
            scheduled_response: histogram.clone(),
            service: histogram,
            scheduler_lateness: hdrhistogram::Histogram::new(3).expect("histogram should be valid"),
            failed_service: hdrhistogram::Histogram::new(3).expect("histogram should be valid"),
        }
    }
}

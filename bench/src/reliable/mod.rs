use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use laser_sdk::agent::{
    Agent, AgentCtx, AgentHandler, AgentMessage, AgentMiddleware, ConcurrencyPolicy,
    DeadLetterSink, Deduplicator, RetryPolicy,
};
use laser_sdk::error::LaserError;
use laser_sdk::laser::Laser;
use laser_sdk::provenance::AgentTopic;
use laser_sdk::stream::{CommitPolicy, Consumer, Topic};
use laser_sdk::types::AgentId as SdkAgentId;
use laser_wire::agent::{
    AgentDeadLetter, AgentEnvelope, AgentId, AgentKind, ConversationId, CorrelationId,
    IdempotencyKey, validate,
};
use laser_wire::framing::decode_named;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::BenchError;
use crate::agdx::{
    AgdxArmEvidence, AgdxArmSummary, AgdxProcessMeasurement, MEASUREMENT_RECORD_OFFSET,
};
use crate::correctness::{CorrectnessOracle, ObservedRecord, OraclePolicy, checksum};
use crate::engine::{Dispatch, LoadResult, Operation, run_closed_loop_for, run_open_loop_for};
use crate::metrics::ProcessSnapshot;
use crate::report::OutcomeCounts;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ReliableCase {
    pub payload_bytes: usize,
    pub operations: u64,
    pub duration_seconds: u64,
    pub concurrency: usize,
    pub partitions: u32,
    pub warmup_seconds: u64,
    pub timeout_millis: u64,
    pub offered_rate: Option<u64>,
    pub spin_dispatch: bool,
    pub max_in_flight: Option<usize>,
    pub variant: ReliableVariant,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_reliable_variant
)]
pub enum ReliableVariant {
    PlainGroup,
    CommitAfterSuccess,
    DedupMiss,
    DedupHit,
    Middleware,
    RetryReady,
    RetryOnce,
    DlqTerminal,
}

fn invalid_reliable_variant(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported reliable-consume arm `{value}`"))
}

impl ReliableVariant {
    #[must_use]
    pub fn label(self) -> &'static str {
        self.into()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ReliableSummary {
    pub consume: AgdxArmSummary,
    pub handler_attempts: u64,
    pub middleware_before_calls: u64,
    pub middleware_after_calls: u64,
    pub dead_letters: u64,
    pub configuration: serde_json::Value,
}

pub struct ReliableEvidence {
    pub consume: AgdxArmEvidence,
    pub handler_attempts: u64,
    pub middleware_before_calls: u64,
    pub middleware_after_calls: u64,
    pub dead_letters: u64,
    pub configuration: serde_json::Value,
}

impl ReliableEvidence {
    #[must_use]
    pub fn summary(&self) -> ReliableSummary {
        ReliableSummary {
            consume: self.consume.summary.clone(),
            handler_attempts: self.handler_attempts,
            middleware_before_calls: self.middleware_before_calls,
            middleware_after_calls: self.middleware_after_calls,
            dead_letters: self.dead_letters,
            configuration: self.configuration.clone(),
        }
    }
}

#[derive(Clone, Default)]
struct DeliveryTracker {
    pending: Arc<tokio::sync::Mutex<HashMap<u64, tokio::sync::oneshot::Sender<()>>>>,
    observed: Arc<tokio::sync::Mutex<Vec<u64>>>,
    keys: Arc<tokio::sync::Mutex<HashMap<String, u64>>>,
}

impl DeliveryTracker {
    async fn register(&self, id: u64, key: Option<String>) -> tokio::sync::oneshot::Receiver<()> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.pending.lock().await.insert(id, sender);
        if let Some(key) = key {
            self.keys.lock().await.insert(key, id);
        }
        receiver
    }

    async fn resolve(&self, id: u64) {
        let pending = self.pending.lock().await.remove(&id);
        self.observed.lock().await.push(id);
        if let Some(pending) = pending {
            let _ = pending.send(());
        }
    }

    async fn resolve_key(&self, scoped_key: &str) {
        let id = self
            .keys
            .lock()
            .await
            .iter()
            .find_map(|(key, id)| scoped_key.ends_with(key).then_some(*id));
        if let Some(id) = id {
            self.resolve(id).await;
        }
    }

    async fn cancel(&self, id: u64) {
        self.pending.lock().await.remove(&id);
    }
}

struct RecordingHandler {
    tracker: DeliveryTracker,
    mode: HandlerMode,
    attempts: Arc<AtomicU64>,
    attempted: tokio::sync::Mutex<BTreeSet<u64>>,
}

#[derive(Clone, Copy)]
enum HandlerMode {
    Complete,
    RetryOnce,
    Reject,
}

impl AgentHandler for RecordingHandler {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        let id = message_id(message)?;
        self.attempts.fetch_add(1, Ordering::Relaxed);
        match self.mode {
            HandlerMode::Complete => self.tracker.resolve(id).await,
            HandlerMode::RetryOnce => {
                if self.attempted.lock().await.insert(id) {
                    return Err(LaserError::Handler("injected retry".to_owned()));
                }
                self.tracker.resolve(id).await;
            }
            HandlerMode::Reject => return Err(LaserError::rejected("injected rejection")),
        }
        Ok(())
    }
}

struct TrackingDeduplicator {
    tracker: DeliveryTracker,
    hit: bool,
}

#[async_trait]
impl Deduplicator for TrackingDeduplicator {
    async fn observe(&self, key: &str) -> bool {
        if self.hit {
            self.tracker.resolve_key(key).await;
            false
        } else {
            true
        }
    }
}

struct CountingMiddleware {
    before: Arc<AtomicU64>,
    after: Arc<AtomicU64>,
}

#[async_trait]
impl AgentMiddleware for CountingMiddleware {
    async fn before_handle(&self, _message: &AgentMessage) -> Result<(), LaserError> {
        self.before.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn after_handle(
        &self,
        _message: &AgentMessage,
        _result: &Result<(), LaserError>,
        _attempt: u32,
    ) {
        self.after.fetch_add(1, Ordering::Relaxed);
    }
}

struct RecordingDeadLetterSink {
    tracker: DeliveryTracker,
    count: Arc<AtomicU64>,
}

#[async_trait]
impl DeadLetterSink for RecordingDeadLetterSink {
    async fn on_dead_letter(
        &self,
        message: Option<&AgentMessage>,
        _capsule: &AgentDeadLetter,
        publish_result: &Result<(), LaserError>,
    ) {
        if publish_result.is_err() {
            return;
        }
        self.count.fetch_add(1, Ordering::Relaxed);
        if let Some(message) = message
            && let Ok(id) = message_id(message)
        {
            self.tracker.resolve(id).await;
        }
    }
}

struct PlainPump {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Result<(), BenchError>>,
}

struct AgentCounters {
    attempts: Arc<AtomicU64>,
    middleware_before: Arc<AtomicU64>,
    middleware_after: Arc<AtomicU64>,
    dead_letters: Arc<AtomicU64>,
}

/// Run one explicit reliable-consumer configuration.
///
/// # Errors
///
/// Returns an error for invalid dimensions, setup failure, workload failure, consumer failure, or replay failure.
pub async fn run_reliable_evidence(
    laser: &Laser,
    case: &ReliableCase,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<ReliableEvidence, BenchError> {
    validate_case(case)?;
    let stream = format!("bench-reliable-{}-{seed:016x}", case.variant.label());
    let topic = laser
        .stream(&stream)
        .topic(AgentTopic::Commands.topic_string());
    topic
        .ensure(case.partitions)
        .await
        .map_err(|error| sdk_error(&error))?;
    let scoped = laser.with_default_stream(&stream);
    let tracker = DeliveryTracker::default();
    let payload = seeded_payload(case.payload_bytes, seed);
    let counters = AgentCounters {
        attempts: Arc::new(AtomicU64::new(0)),
        middleware_before: Arc::new(AtomicU64::new(0)),
        middleware_after: Arc::new(AtomicU64::new(0)),
        dead_letters: Arc::new(AtomicU64::new(0)),
    };
    let (mut agent, plain) =
        start_consumer(&topic, scoped.clone(), case, tracker.clone(), &counters).await?;
    let run = async {
        if let Some(agent) = &mut agent {
            agent.ready().await.map_err(|error| sdk_error(&error))?;
        }
        let timeout = Duration::from_millis(case.timeout_millis);
        let warmup = reliable_operation(
            scoped.clone(),
            payload.clone(),
            tracker.clone(),
            case.variant,
            seed,
            0,
        )?;
        warmup_load(case, timeout, warmup).await?;
        reset_counters(&counters);
        let operation = reliable_operation(
            scoped,
            payload.clone(),
            tracker.clone(),
            case.variant,
            seed,
            MEASUREMENT_RECORD_OFFSET,
        )?;
        let mut consume = measured_arm(case, timeout, operation, monitored_processes).await?;
        let expected = expected_ids(&consume.load);
        let explained = explained_ids(&consume.load);
        let correctness = validate_topic(&topic, &payload, &expected, &explained).await?;
        apply_correctness(&mut consume.summary.outcomes, &correctness);
        validate_deliveries(
            &tracker,
            &expected,
            &explained,
            &mut consume.summary.outcomes,
        )
        .await;
        Ok::<_, BenchError>(consume)
    }
    .await;
    let agent_shutdown = match agent {
        Some(agent) => agent.shutdown().await.map_err(|error| sdk_error(&error)),
        None => Ok(()),
    };
    let plain_shutdown = match plain {
        Some(pump) => stop_plain_pump(pump).await,
        None => Ok(()),
    };
    let consume = run?;
    agent_shutdown?;
    plain_shutdown?;
    Ok(ReliableEvidence {
        consume,
        handler_attempts: counters.attempts.load(Ordering::Relaxed),
        middleware_before_calls: counters.middleware_before.load(Ordering::Relaxed),
        middleware_after_calls: counters.middleware_after.load(Ordering::Relaxed),
        dead_letters: counters.dead_letters.load(Ordering::Relaxed),
        configuration: configuration(case.variant),
    })
}

async fn start_consumer(
    topic: &Topic,
    laser: Laser,
    case: &ReliableCase,
    tracker: DeliveryTracker,
    counters: &AgentCounters,
) -> Result<(Option<laser_sdk::agent::AgentHandle>, Option<PlainPump>), BenchError> {
    if case.variant == ReliableVariant::PlainGroup {
        let consumer = topic
            .consumer_group("laser-bench-plain")
            .batch_length(128)
            .without_poll_interval()
            .commit_policy(CommitPolicy::Disabled)
            .build()
            .await
            .map_err(|error| sdk_error(&error))?;
        return Ok((None, Some(spawn_plain_pump(consumer, tracker))));
    }
    let handler_mode = match case.variant {
        ReliableVariant::RetryOnce => HandlerMode::RetryOnce,
        ReliableVariant::DlqTerminal => HandlerMode::Reject,
        _ => HandlerMode::Complete,
    };
    let worker: SdkAgentId = "laser-bench-reliable"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid reliable worker id: {error}")))?;
    let deduplicator: Option<Box<dyn Deduplicator>> = matches!(
        case.variant,
        ReliableVariant::DedupMiss | ReliableVariant::DedupHit
    )
    .then(|| {
        Box::new(TrackingDeduplicator {
            tracker: tracker.clone(),
            hit: case.variant == ReliableVariant::DedupHit,
        }) as Box<dyn Deduplicator>
    });
    let middleware: Vec<Arc<dyn AgentMiddleware>> = if case.variant == ReliableVariant::Middleware {
        vec![Arc::new(CountingMiddleware {
            before: Arc::clone(&counters.middleware_before),
            after: Arc::clone(&counters.middleware_after),
        }) as Arc<dyn AgentMiddleware>]
    } else {
        Vec::new()
    };
    let retry = matches!(
        case.variant,
        ReliableVariant::RetryReady | ReliableVariant::RetryOnce
    )
    .then(|| RetryPolicy::backoff(2, Duration::ZERO));
    let dead_letter_sink: Option<Arc<dyn DeadLetterSink>> =
        (case.variant == ReliableVariant::DlqTerminal).then(|| {
            Arc::new(RecordingDeadLetterSink {
                tracker: tracker.clone(),
                count: Arc::clone(&counters.dead_letters),
            }) as Arc<dyn DeadLetterSink>
        });
    let builder = Agent::builder()
        .id(worker)
        .listen_on(AgentTopic::Commands)
        .handler(RecordingHandler {
            tracker: tracker.clone(),
            mode: handler_mode,
            attempts: Arc::clone(&counters.attempts),
            attempted: tokio::sync::Mutex::new(BTreeSet::new()),
        })
        .poll_interval(Duration::ZERO)
        .concurrency(ConcurrencyPolicy::SerialPerPartition {
            max_partitions: usize::try_from(case.partitions).map_err(|_| {
                BenchError::Invalid("reliable partition count exceeds usize".to_owned())
            })?,
        })
        .maybe_deduplicator(deduplicator)
        .middleware(middleware)
        .maybe_retry(retry)
        .maybe_on_dead_letter(dead_letter_sink);
    Ok((Some(builder.build().spawn(laser)), None))
}

fn spawn_plain_pump(mut consumer: Consumer, tracker: DeliveryTracker) -> PlainPump {
    let (stop, mut stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                message = consumer.next() => {
                    let message = message
                        .ok_or_else(|| BenchError::Invalid("plain consumer ended".to_owned()))?
                        .map_err(|error| sdk_error(&error))?;
                    let envelope: AgentEnvelope = decode_named(&message.payload).map_err(|error| {
                        BenchError::Invalid(format!("plain consumer decode failed: {error}"))
                    })?;
                    validate(&envelope).map_err(|error| {
                        BenchError::Invalid(format!("plain consumer validation failed: {error}"))
                    })?;
                    let id = body_id(&envelope.body)?;
                    consumer.commit(&message).await.map_err(|error| sdk_error(&error))?;
                    tracker.resolve(id).await;
                }
            }
        }
        Ok(())
    });
    PlainPump { stop, task }
}

async fn stop_plain_pump(pump: PlainPump) -> Result<(), BenchError> {
    let PlainPump { stop, mut task } = pump;
    let _ = stop.send(());
    if let Ok(result) = tokio::time::timeout(Duration::from_secs(1), &mut task).await {
        return result
            .map_err(|error| BenchError::Invalid(format!("plain consumer failed: {error}")))?;
    }
    task.abort();
    match task.await {
        Ok(result) => result,
        Err(error) if error.is_cancelled() => Ok(()),
        Err(error) => Err(BenchError::Invalid(format!(
            "plain consumer failed during abort: {error}"
        ))),
    }
}

fn reliable_operation(
    laser: Laser,
    payload: Bytes,
    tracker: DeliveryTracker,
    variant: ReliableVariant,
    seed: u64,
    id_offset: u64,
) -> Result<Operation, BenchError> {
    let source: AgentId = "laser-bench-client"
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid reliable source id: {error}")))?;
    Ok(Arc::new(move |sequence| {
        let laser = laser.clone();
        let payload = payload.clone();
        let tracker = tracker.clone();
        let source = source.clone();
        Box::pin(async move {
            let id = id_offset
                .checked_add(sequence)
                .ok_or_else(|| "reliable-consume ID exceeds u64".to_owned())?;
            let idempotency = matches!(
                variant,
                ReliableVariant::DedupMiss | ReliableVariant::DedupHit
            )
            .then(|| idempotency(seed, id))
            .transpose()?;
            let receiver = tracker
                .register(id, idempotency.as_ref().map(ToString::to_string))
                .await;
            let producer = laser.agdx(AgentTopic::Commands, source, conversation(seed, id));
            let mut request =
                producer.command(correlation(seed, id), record_payload(&payload, id)?);
            if let Some(key) = idempotency {
                request = request.with_idempotency_key(key);
            }
            if let Err(error) = request.send().await {
                tracker.cancel(id).await;
                return Err(error.to_string());
            }
            receiver
                .await
                .map_err(|_| "reliable-consume tracker stopped".to_owned())
        })
    }))
}

async fn warmup_load(
    case: &ReliableCase,
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
        return Err(BenchError::Invalid(
            "reliable-consume warmup did not complete successfully".to_owned(),
        ));
    }
    Ok(())
}

async fn measured_arm(
    case: &ReliableCase,
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
                if case.spin_dispatch {
                    Dispatch::SpinWindow
                } else {
                    Dispatch::Sleep
                },
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
    let summary = summarize(case, &load);
    Ok(AgdxArmEvidence {
        summary,
        load,
        processes,
        network: None,
    })
}

fn summarize(case: &ReliableCase, load: &LoadResult) -> AgdxArmSummary {
    let successful_bytes = load
        .outcomes
        .successful
        .saturating_mul(u64::try_from(case.payload_bytes).unwrap_or(u64::MAX));
    let scheduled_p99 = load.scheduled_response.value_at_quantile(0.99);
    let service_p99 = load.service.value_at_quantile(0.99);
    AgdxArmSummary {
        arm: format!("reliable-consume-{}", case.variant.label()),
        order: 1,
        elapsed_ns: duration_ns(load.elapsed),
        operations_per_second: per_second(load.outcomes.successful, load.elapsed),
        payload_bytes_per_second: per_second(successful_bytes, load.elapsed),
        scheduled_p50_ns: load.scheduled_response.value_at_quantile(0.5),
        scheduled_p90_ns: load.scheduled_response.value_at_quantile(0.9),
        scheduled_p99_ns: scheduled_p99,
        service_p50_ns: load.service.value_at_quantile(0.5),
        service_p90_ns: load.service.value_at_quantile(0.9),
        service_p99_ns: service_p99,
        service_p999_ns: (load.outcomes.successful >= 100_000)
            .then(|| load.service.value_at_quantile(0.999)),
        scheduler_lateness_p99_ns: load.scheduler_lateness.value_at_quantile(0.99),
        primary_p99_ns: case.offered_rate.map_or(service_p99, |_| scheduled_p99),
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
            let envelope: AgentEnvelope = decode_named(&record.payload).map_err(|error| {
                BenchError::Invalid(format!("reliable replay decode failed: {error}"))
            })?;
            validate(&envelope).map_err(|error| {
                BenchError::Invalid(format!("reliable replay validation failed: {error}"))
            })?;
            if envelope.kind != AgentKind::Command {
                return Err(BenchError::Invalid(
                    "reliable source topic contained a non-command envelope".to_owned(),
                ));
            }
            let id = body_id(&envelope.body)?;
            if id < MEASUREMENT_RECORD_OFFSET {
                continue;
            }
            let expected = record_payload(payload, id).map_err(BenchError::Invalid)?;
            oracle.observe(ObservedRecord {
                id,
                partition: record.id.partition_id,
                partition_sequence: record.id.offset,
                payload: &envelope.body,
                checksum: checksum(&expected),
            });
        }
    }
    Ok(oracle.finish())
}

async fn validate_deliveries(
    tracker: &DeliveryTracker,
    expected: &[u64],
    explained: &[u64],
    outcomes: &mut OutcomeCounts,
) {
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    let explained = explained.iter().copied().collect::<BTreeSet<_>>();
    let observed = tracker.observed.lock().await;
    let mut seen = BTreeSet::new();
    let mut duplicates = 0_u64;
    let mut unexpected = 0_u64;
    for id in observed.iter().copied() {
        if id < MEASUREMENT_RECORD_OFFSET || explained.contains(&id) {
            continue;
        }
        if !seen.insert(id) {
            duplicates = duplicates.saturating_add(1);
        }
        if !expected.contains(&id) {
            unexpected = unexpected.saturating_add(1);
        }
    }
    let missing = u64::try_from(expected.difference(&seen).count()).unwrap_or(u64::MAX);
    outcomes.duplicates = outcomes.duplicates.saturating_add(duplicates);
    outcomes.gaps = outcomes
        .gaps
        .saturating_add(missing)
        .saturating_add(unexpected);
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

fn configuration(variant: ReliableVariant) -> serde_json::Value {
    let boundary = match variant {
        ReliableVariant::PlainGroup => "manual-commit-complete",
        ReliableVariant::DedupHit => "dedup-decision",
        ReliableVariant::DlqTerminal => "dead-letter-published",
        _ => "handler-complete",
    };
    serde_json::json!({
        "variant": variant.label(),
        "boundary": boundary,
        "delivery": "at-least-once",
        "consumer_poll": "tight",
        "retry_backoff": if variant == ReliableVariant::RetryOnce { "zero" } else { "not-injected" },
    })
}

fn reset_counters(counters: &AgentCounters) {
    counters.attempts.store(0, Ordering::Relaxed);
    counters.middleware_before.store(0, Ordering::Relaxed);
    counters.middleware_after.store(0, Ordering::Relaxed);
    counters.dead_letters.store(0, Ordering::Relaxed);
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

fn validate_case(case: &ReliableCase) -> Result<(), BenchError> {
    if case.payload_bytes < size_of::<u64>()
        || case.operations == 0
        || case.duration_seconds == 0
        || case.concurrency == 0
        || case.partitions == 0
        || case.warmup_seconds == 0
        || case.timeout_millis == 0
    {
        return Err(BenchError::Invalid(
            "reliable-consume dimensions must be nonzero and payloads must fit a record ID"
                .to_owned(),
        ));
    }
    Ok(())
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

fn message_id(message: &AgentMessage) -> Result<u64, LaserError> {
    body_id(message.body()).map_err(|error| LaserError::HandlerConfig(error.to_string()))
}

fn body_id(payload: &[u8]) -> Result<u64, BenchError> {
    let bytes = payload.get(..size_of::<u64>()).ok_or_else(|| {
        BenchError::Invalid("reliable-consume payload has no record ID".to_owned())
    })?;
    Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
        BenchError::Invalid("reliable-consume record ID is invalid".to_owned())
    })?))
}

fn record_payload(payload: &Bytes, id: u64) -> Result<Vec<u8>, String> {
    if payload.len() < size_of::<u64>() {
        return Err("reliable-consume payload must fit a record ID".to_owned());
    }
    let mut record = payload.to_vec();
    record[..size_of::<u64>()].copy_from_slice(&id.to_le_bytes());
    Ok(record)
}

fn conversation(seed: u64, id: u64) -> ConversationId {
    ConversationId::from_u128((u128::from(seed) << 64) | u128::from(id))
}

fn correlation(seed: u64, id: u64) -> CorrelationId {
    CorrelationId::from_u128((u128::from(seed ^ u64::MAX) << 64) | u128::from(id))
}

fn idempotency(seed: u64, id: u64) -> Result<IdempotencyKey, String> {
    format!("bench-{seed:016x}-{id:016x}")
        .parse()
        .map_err(|error| format!("invalid reliable idempotency key: {error}"))
}

fn seeded_payload(size: usize, seed: u64) -> Bytes {
    let mut state = seed.max(1);
    let mut payload = Vec::with_capacity(size);
    for _ in 0..size {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        payload.push(state.to_le_bytes()[0]);
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

fn sdk_error(error: &LaserError) -> BenchError {
    BenchError::Invalid(format!("Laser reliable-consume operation failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_reliable_arm_names_when_parsed_then_should_cover_the_declared_matrix() {
        for name in [
            "plain_group",
            "commit_after_success",
            "dedup_miss",
            "dedup_hit",
            "middleware",
            "retry_ready",
            "retry_once",
            "dlq_terminal",
        ] {
            assert_eq!(
                name.parse::<ReliableVariant>()
                    .expect("reliable arm should parse")
                    .label(),
                name
            );
        }
    }

    #[test]
    fn given_reliable_payload_when_record_id_is_stamped_then_should_round_trip() {
        let payload = seeded_payload(64, 3);
        let record = record_payload(&payload, 42).expect("record should build");
        assert_eq!(body_id(&record).expect("record should decode"), 42);
    }
}

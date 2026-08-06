use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use bytes::Bytes;
use laser_sdk::laser::Laser;
use laser_sdk::memory::{MemoryHandle, MemoryItem};
use laser_sdk::stream::{
    CommitPolicy, Consumer, ConsumerStart, ContentType, Producer, ProducerMessage, Record, Routing,
    Topic,
};
use laser_sdk::types::ConversationId;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use strum::{Display, EnumString, IntoStaticStr};

use crate::BenchError;
use crate::agdx::{record_payload, seeded_payload};
use crate::binary::BinaryManifest;
use crate::managed::{
    ManagedCase, ProjectionNames, prepare_projection, query_payload, wait_for_row_count,
};
use crate::manifest::Environment;
use crate::process::{NativeIggy, NativePlane, PlaneProfile, StoppedPlane};
use crate::report::OutcomeCounts;
use crate::telemetry::{TelemetrySampler, TelemetrySeries};

const RESTART_INDEX: u32 = 1;
const TELEMETRY_INTERVAL: Duration = Duration::from_secs(1);

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_recovery_driver
)]
pub enum RecoveryDriver {
    ConsumerRestart,
    IggyRestart,
    PlaneRestartMemory,
    PlaneRestartProjection,
}

impl RecoveryDriver {
    #[must_use]
    pub fn requires_plane(self) -> bool {
        matches!(
            self,
            Self::PlaneRestartMemory | Self::PlaneRestartProjection
        )
    }
}

fn invalid_recovery_driver(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported recovery driver `{value}`"))
}

#[derive(Clone, Debug)]
pub struct RecoveryCase {
    pub payload_bytes: usize,
    pub backlog_records: usize,
    pub partitions: u32,
    pub timeout: Duration,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RecoveryTimeline {
    pub fault_injected_ns: u64,
    pub plane_stopped_ns: u64,
    pub managed_unavailable_ns: u64,
    pub backlog_published_ns: u64,
    pub restart_started_ns: u64,
    pub plane_available_ns: u64,
    pub plane_ready_ns: u64,
    pub converged_ns: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct RecoverySummary {
    pub driver: RecoveryDriver,
    pub backend_profile: PlaneProfile,
    pub baseline_records: usize,
    pub backlog_records: usize,
    pub recovered_records: usize,
    pub expected_managed_errors: u64,
    pub unexpected_managed_errors: u64,
    pub duplicates: u64,
    pub timeline: RecoveryTimeline,
    pub outcomes: OutcomeCounts,
    pub configuration: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ConsumerRecoveryTimeline {
    pub fault_injected_ns: u64,
    pub consumer_stopped_ns: u64,
    pub backlog_published_ns: u64,
    pub restart_started_ns: u64,
    pub first_recovered_record_ns: Option<u64>,
    pub converged_ns: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ConsumerRecoverySummary {
    pub driver: RecoveryDriver,
    pub baseline_records: usize,
    pub backlog_records: usize,
    pub recovered_records: usize,
    pub duplicates: u64,
    pub gaps: u64,
    pub ordering_violations: u64,
    pub catch_up_records_per_second: f64,
    pub timeline: ConsumerRecoveryTimeline,
    pub outcomes: OutcomeCounts,
    pub configuration: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct IggyRecoveryTimeline {
    pub fault_injected_ns: u64,
    pub server_stopped_ns: u64,
    pub unavailable_confirmed_ns: u64,
    pub restart_started_ns: u64,
    pub server_ready_ns: u64,
    pub persisted_records_verified_ns: u64,
    pub converged_ns: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct IggyRecoverySummary {
    pub driver: RecoveryDriver,
    pub baseline_records: usize,
    pub appended_after_restart: usize,
    pub recovered_records: usize,
    pub duplicates: u64,
    pub gaps: u64,
    pub ordering_violations: u64,
    pub timeline: IggyRecoveryTimeline,
    pub outcomes: OutcomeCounts,
    pub configuration: serde_json::Value,
}

pub struct IggyRecoveryEvidence {
    pub summary: IggyRecoverySummary,
    pub server: NativeIggy,
    pub telemetry_before: TelemetrySeries,
    pub telemetry_after: TelemetrySeries,
}

pub struct RecoveryEvidence {
    pub summary: RecoverySummary,
    pub plane: NativePlane,
    pub telemetry_before: TelemetrySeries,
    pub telemetry_after: TelemetrySeries,
}

pub struct RecoveryRun<'a> {
    pub laser: &'a Laser,
    pub server: &'a NativeIggy,
    pub plane: NativePlane,
    pub plane_manifest: &'a BinaryManifest,
    pub environment: &'a Environment,
    pub case: &'a RecoveryCase,
    pub driver: RecoveryDriver,
    pub scenario: &'a str,
    pub seed: u64,
}

#[derive(Clone, Copy)]
struct RecoveryContext<'a> {
    laser: &'a Laser,
    server: &'a NativeIggy,
    plane_manifest: &'a BinaryManifest,
    environment: &'a Environment,
    case: &'a RecoveryCase,
    scenario: &'a str,
    seed: u64,
}

struct RestartedPlane {
    plane: NativePlane,
    telemetry: TelemetrySampler,
    plane_available_ns: u64,
    plane_ready_ns: u64,
}

struct RecoveryMetrics {
    telemetry_before: TelemetrySeries,
    telemetry_after: TelemetrySeries,
    recovered_records: usize,
    duplicates: u64,
    timeline: RecoveryTimeline,
}

/// Stop plane while Iggy remains live, append a deterministic backlog, restart plane, and verify convergence.
///
/// # Errors
///
/// Returns an error when setup, fault injection, expected unavailability, restart, telemetry, or convergence validation fails.
pub async fn run_recovery_evidence(run: RecoveryRun<'_>) -> Result<RecoveryEvidence, BenchError> {
    validate_case(run.case)?;
    let context = RecoveryContext {
        laser: run.laser,
        server: run.server,
        plane_manifest: run.plane_manifest,
        environment: run.environment,
        case: run.case,
        scenario: run.scenario,
        seed: run.seed,
    };
    match run.driver {
        RecoveryDriver::ConsumerRestart => Err(BenchError::Invalid(
            "consumer restart uses run_consumer_recovery".to_owned(),
        )),
        RecoveryDriver::IggyRestart => Err(BenchError::Invalid(
            "Iggy restart uses run_iggy_recovery".to_owned(),
        )),
        RecoveryDriver::PlaneRestartMemory => run_memory_recovery(context, run.plane).await,
        RecoveryDriver::PlaneRestartProjection => run_projection_recovery(context, run.plane).await,
    }
}

/// Restart Iggy on its persisted system path and verify retained plus newly appended records.
///
/// # Errors
///
/// Returns an error when setup, shutdown, unavailability probing, restart, telemetry, replay, or append validation fails.
pub async fn run_iggy_recovery(
    laser: Laser,
    server: NativeIggy,
    server_manifest: &BinaryManifest,
    environment: &Environment,
    case: &RecoveryCase,
    scenario: &str,
    seed: u64,
) -> Result<IggyRecoveryEvidence, BenchError> {
    validate_case(case)?;
    if case.partitions != 1 {
        return Err(BenchError::Invalid(
            "Iggy restart currently requires one partition".to_owned(),
        ));
    }
    let stream = format!("bench-iggy-recovery-{seed:016x}");
    let topic = laser.stream(&stream).topic(scenario);
    topic
        .ensure(1)
        .await
        .map_err(|error| recovery_error("create Iggy recovery topic", &error))?;
    let payload = seeded_payload(case.payload_bytes, seed);
    let producer = recovery_producer(&topic).await?;
    publish_recovery_records(&producer, &payload, 0, case.backlog_records).await?;
    producer
        .shutdown()
        .await
        .map_err(|error| recovery_error("stop Iggy recovery producer", &error))?;
    let telemetry_before = start_iggy_telemetry(&server, environment)
        .await?
        .stop()
        .await?;
    let connection_string = server.connection_string.clone();
    drop(laser);
    let started = Instant::now();
    let fault_injected_ns = elapsed_ns(started);
    let stopped = server.stop_for_restart().await?;
    let server_stopped_ns = elapsed_ns(started);
    if matches!(
        tokio::time::timeout(
            Duration::from_millis(250),
            Laser::connect(&connection_string)
        )
        .await,
        Ok(Ok(_))
    ) {
        return Err(BenchError::Invalid(
            "Iggy connection unexpectedly succeeded while the server was stopped".to_owned(),
        ));
    }
    let unavailable_confirmed_ns = elapsed_ns(started);
    let restart_started_ns = elapsed_ns(started);
    let server = stopped.restart(server_manifest, environment).await?;
    let laser = server.probe_vsr().await?;
    let server_ready_ns = elapsed_ns(started);
    let topic = laser.stream(&stream).topic(scenario);
    let mut consumer =
        build_recovery_consumer(&topic, "iggy-restart-validator", ConsumerStart::First).await?;
    consume_exact(&mut consumer, 0, case.backlog_records, case.timeout).await?;
    let persisted_records_verified_ns = elapsed_ns(started);
    let producer = recovery_producer(&topic).await?;
    publish_recovery_records(
        &producer,
        &payload,
        case.backlog_records as u64,
        case.backlog_records,
    )
    .await?;
    let recovered = consume_recovery_backlog(
        &mut consumer,
        case.backlog_records as u64,
        case.backlog_records,
        case.timeout,
        started,
    )
    .await;
    let converged_ns = elapsed_ns(started);
    consumer
        .shutdown()
        .await
        .map_err(|error| recovery_error("stop Iggy recovery consumer", &error))?;
    producer
        .shutdown()
        .await
        .map_err(|error| recovery_error("stop post-restart producer", &error))?;
    let telemetry_after = start_iggy_telemetry(&server, environment)
        .await?
        .stop()
        .await?;
    Ok(IggyRecoveryEvidence {
        summary: iggy_recovery_summary(
            case,
            &recovered,
            IggyRecoveryTimeline {
                fault_injected_ns,
                server_stopped_ns,
                unavailable_confirmed_ns,
                restart_started_ns,
                server_ready_ns,
                persisted_records_verified_ns,
                converged_ns,
            },
        ),
        server,
        telemetry_before,
        telemetry_after,
    })
}

fn iggy_recovery_summary(
    case: &RecoveryCase,
    recovered: &RecoveredBacklog,
    timeline: IggyRecoveryTimeline,
) -> IggyRecoverySummary {
    let recovered_records = recovered.ids.len();
    let backlog_u64 = u64::try_from(case.backlog_records).unwrap_or(u64::MAX);
    let recovered_u64 = u64::try_from(recovered_records).unwrap_or(u64::MAX);
    let gaps = backlog_u64.saturating_sub(recovered_u64);
    IggyRecoverySummary {
        driver: RecoveryDriver::IggyRestart,
        baseline_records: case.backlog_records,
        appended_after_restart: case.backlog_records,
        recovered_records,
        duplicates: recovered.duplicates,
        gaps,
        ordering_violations: recovered.ordering_violations,
        timeline,
        outcomes: OutcomeCounts {
            offered: backlog_u64,
            dispatched: backlog_u64,
            completed: backlog_u64,
            successful: recovered_u64.saturating_sub(recovered.duplicates),
            failed: gaps,
            duplicates: recovered.duplicates,
            gaps,
            ordering_violations: recovered.ordering_violations,
            ..OutcomeCounts::default()
        },
        configuration: serde_json::json!({
            "fault": "iggy_process_stop",
            "server": "server-ng",
            "transport": "tcp_vsr",
            "system_path_reused": true,
            "same_listen_address": true,
            "latency_boundary": "server_stop_to_persisted_and_new_record_convergence",
        }),
    }
}

/// Stop a committed SDK consumer, publish a backlog, resume from its server offset, and verify convergence.
///
/// # Errors
///
/// Returns an error when topology setup, baseline delivery, publishing, consumer shutdown, or resumed-consumer construction fails.
pub async fn run_consumer_recovery(
    laser: &Laser,
    case: &RecoveryCase,
    scenario: &str,
    seed: u64,
) -> Result<ConsumerRecoverySummary, BenchError> {
    validate_case(case)?;
    if case.partitions != 1 {
        return Err(BenchError::Invalid(
            "consumer restart currently requires one partition".to_owned(),
        ));
    }
    let stream = format!("bench-recovery-{seed:016x}");
    let topic = laser.stream(&stream).topic(scenario);
    topic
        .ensure(1)
        .await
        .map_err(|error| recovery_error("create recovery topic", &error))?;
    let producer = recovery_producer(&topic).await?;
    let payload = seeded_payload(case.payload_bytes, seed);
    publish_recovery_records(&producer, &payload, 0, case.backlog_records).await?;
    let consumer_id = format!("laser-bench-recovery-{seed:016x}");
    let mut consumer = build_recovery_consumer(&topic, &consumer_id, ConsumerStart::First).await?;
    consume_exact(&mut consumer, 0, case.backlog_records, case.timeout).await?;
    let started = Instant::now();
    let fault_injected_ns = elapsed_ns(started);
    consumer
        .shutdown()
        .await
        .map_err(|error| recovery_error("stop recovery consumer", &error))?;
    let consumer_stopped_ns = elapsed_ns(started);
    publish_recovery_records(
        &producer,
        &payload,
        case.backlog_records as u64,
        case.backlog_records,
    )
    .await?;
    let backlog_published_ns = elapsed_ns(started);
    let restart_started_ns = elapsed_ns(started);
    let mut resumed = build_recovery_consumer(&topic, &consumer_id, ConsumerStart::Next).await?;
    let recovered = consume_recovery_backlog(
        &mut resumed,
        case.backlog_records as u64,
        case.backlog_records,
        case.timeout,
        started,
    )
    .await;
    let converged_ns = elapsed_ns(started);
    resumed
        .shutdown()
        .await
        .map_err(|error| recovery_error("stop resumed consumer", &error))?;
    producer
        .shutdown()
        .await
        .map_err(|error| recovery_error("stop recovery producer", &error))?;
    let catch_up_elapsed = Duration::from_nanos(
        converged_ns.saturating_sub(
            recovered
                .first_recovered_record_ns
                .unwrap_or(restart_started_ns),
        ),
    );
    let first_recovered_record_ns = recovered.first_recovered_record_ns;
    Ok(consumer_recovery_summary(
        case,
        ConsumerRecoveryMetrics {
            recovered,
            catch_up_elapsed,
            timeline: ConsumerRecoveryTimeline {
                fault_injected_ns,
                consumer_stopped_ns,
                backlog_published_ns,
                restart_started_ns,
                first_recovered_record_ns,
                converged_ns,
            },
        },
    ))
}

fn consumer_recovery_summary(
    case: &RecoveryCase,
    metrics: ConsumerRecoveryMetrics,
) -> ConsumerRecoverySummary {
    let recovered_records = metrics.recovered.ids.len();
    let recovered_u64 = u64::try_from(recovered_records).unwrap_or(u64::MAX);
    let backlog_u64 = u64::try_from(case.backlog_records).unwrap_or(u64::MAX);
    let gaps = backlog_u64.saturating_sub(recovered_u64);
    let catch_up_records_per_second = if metrics.catch_up_elapsed.is_zero() {
        0.0
    } else {
        as_f64(recovered_u64) / metrics.catch_up_elapsed.as_secs_f64()
    };
    ConsumerRecoverySummary {
        driver: RecoveryDriver::ConsumerRestart,
        baseline_records: case.backlog_records,
        backlog_records: case.backlog_records,
        recovered_records,
        duplicates: metrics.recovered.duplicates,
        gaps,
        ordering_violations: metrics.recovered.ordering_violations,
        catch_up_records_per_second,
        timeline: metrics.timeline,
        outcomes: OutcomeCounts {
            offered: backlog_u64,
            dispatched: backlog_u64,
            completed: backlog_u64,
            successful: recovered_u64.saturating_sub(metrics.recovered.duplicates),
            failed: gaps,
            duplicates: metrics.recovered.duplicates,
            gaps,
            ordering_violations: metrics.recovered.ordering_violations,
            ..OutcomeCounts::default()
        },
        configuration: serde_json::json!({
            "fault": "sdk_consumer_shutdown",
            "iggy_remained_available": true,
            "resume_position": "server_stored_consumer_offset",
            "commit_policy": "explicit_after_each_record",
            "backlog_published_while_consumer_stopped": true,
            "latency_boundary": "consumer_shutdown_to_full_backlog_convergence",
        }),
    }
}

struct RecoveredBacklog {
    ids: BTreeSet<u64>,
    duplicates: u64,
    ordering_violations: u64,
    first_recovered_record_ns: Option<u64>,
}

struct ConsumerRecoveryMetrics {
    recovered: RecoveredBacklog,
    catch_up_elapsed: Duration,
    timeline: ConsumerRecoveryTimeline,
}

async fn build_recovery_consumer(
    topic: &Topic,
    consumer_id: &str,
    start: ConsumerStart,
) -> Result<Consumer, BenchError> {
    topic
        .consumer(consumer_id, 0)
        .batch_length(100)
        .without_poll_interval()
        .start_at(start)
        .commit_policy(CommitPolicy::Disabled)
        .allow_replay()
        .build()
        .await
        .map_err(|error| recovery_error("build recovery consumer", &error))
}

async fn recovery_producer(topic: &Topic) -> Result<Producer, BenchError> {
    topic
        .producer()
        .batch_length(100)
        .routing(Routing::Partition(0))
        .create_stream(false)
        .create_topic(false)
        .build()
        .await
        .map_err(|error| recovery_error("build recovery producer", &error))
}

async fn publish_recovery_records(
    producer: &Producer,
    payload: &Bytes,
    start: u64,
    count: usize,
) -> Result<(), BenchError> {
    for offset in (0..count).step_by(100) {
        let end = offset.saturating_add(100).min(count);
        let messages = (offset..end)
            .map(|index| {
                let id = start.saturating_add(index as u64);
                record_payload(payload, id)
                    .map(Bytes::from)
                    .map(ProducerMessage::new)
                    .map_err(BenchError::Invalid)
            })
            .collect::<Result<Vec<_>, BenchError>>()?;
        producer
            .send_batch(messages)
            .await
            .map_err(|error| recovery_error("publish recovery backlog", &error))?;
    }
    Ok(())
}

async fn consume_exact(
    consumer: &mut Consumer,
    start: u64,
    count: usize,
    timeout: Duration,
) -> Result<(), BenchError> {
    for index in 0..count {
        let message = consumer
            .next_within(timeout)
            .await
            .map_err(|error| recovery_error("consume recovery baseline", &error))?;
        let id = decode_recovery_id(&message.payload)?;
        if id != start.saturating_add(index as u64) {
            return Err(BenchError::Invalid(format!(
                "recovery baseline expected record {}, found {id}",
                start.saturating_add(index as u64)
            )));
        }
        consumer
            .commit(&message)
            .await
            .map_err(|error| recovery_error("commit recovery record", &error))?;
    }
    Ok(())
}

async fn consume_recovery_backlog(
    consumer: &mut Consumer,
    start: u64,
    count: usize,
    timeout: Duration,
    started: Instant,
) -> RecoveredBacklog {
    let mut ids = BTreeSet::new();
    let mut duplicates = 0_u64;
    let mut ordering_violations = 0_u64;
    let mut previous = None;
    let mut first_recovered_record_ns = None;
    for _ in 0..count {
        let Ok(message) = consumer.next_within(timeout).await else {
            break;
        };
        let Ok(id) = decode_recovery_id(&message.payload) else {
            break;
        };
        first_recovered_record_ns.get_or_insert_with(|| elapsed_ns(started));
        if !ids.insert(id) {
            duplicates = duplicates.saturating_add(1);
        }
        if previous.is_some_and(|previous| id <= previous)
            || id < start
            || id >= start.saturating_add(count as u64)
        {
            ordering_violations = ordering_violations.saturating_add(1);
        }
        previous = Some(id);
        if consumer.commit(&message).await.is_err() {
            break;
        }
    }
    RecoveredBacklog {
        ids,
        duplicates,
        ordering_violations,
        first_recovered_record_ns,
    }
}

fn decode_recovery_id(payload: &[u8]) -> Result<u64, BenchError> {
    let bytes = payload
        .get(..size_of::<u64>())
        .ok_or_else(|| BenchError::Invalid("recovery record has no sequence ID".to_owned()))?;
    Ok(u64::from_le_bytes(bytes.try_into().map_err(|_| {
        BenchError::Invalid("recovery record sequence ID is invalid".to_owned())
    })?))
}

async fn run_projection_recovery(
    context: RecoveryContext<'_>,
    plane: NativePlane,
) -> Result<RecoveryEvidence, BenchError> {
    let names = ProjectionNames::new(context.scenario, context.seed);
    let managed_case = managed_case(context.case);
    let (laser, topic) =
        prepare_projection(context.laser, &managed_case, &names, plane.profile).await?;
    publish_projection(
        &topic,
        &names,
        &managed_case,
        0,
        context.case.backlog_records,
    )
    .await?;
    wait_for_row_count(
        &laser,
        &names.index,
        context.case.backlog_records as u64,
        context.case.timeout,
    )
    .await?;
    let telemetry_before = start_telemetry(context.server, &plane, context.environment).await?;
    let telemetry_before = telemetry_before.stop().await?;
    let started = Instant::now();
    let fault_injected_ns = elapsed_ns(started);
    let stopped = plane.stop_for_restart().await?;
    let plane_stopped_ns = elapsed_ns(started);
    require_query_unavailable(&laser, &names.index).await?;
    let managed_unavailable_ns = elapsed_ns(started);
    publish_projection(
        &topic,
        &names,
        &managed_case,
        context.case.backlog_records as u64,
        context.case.backlog_records,
    )
    .await?;
    let backlog_published_ns = elapsed_ns(started);
    let restart_started_ns = elapsed_ns(started);
    let restarted = restart_plane(context, stopped, started).await?;
    let expected = context.case.backlog_records.saturating_mul(2);
    if let Err(error) =
        wait_for_row_count(&laser, &names.index, expected as u64, context.case.timeout).await
    {
        return restarted.fail(error).await;
    }
    let (recovered, duplicates) = match validate_projection(&laser, &names.index, expected).await {
        Ok(values) => values,
        Err(error) => return restarted.fail(error).await,
    };
    let converged_ns = elapsed_ns(started);
    let plane_available_ns = restarted.plane_available_ns;
    let plane_ready_ns = restarted.plane_ready_ns;
    let (plane, telemetry_after) = restarted.finish().await?;
    let metrics = RecoveryMetrics {
        telemetry_before,
        telemetry_after,
        recovered_records: recovered,
        duplicates,
        timeline: RecoveryTimeline {
            fault_injected_ns,
            plane_stopped_ns,
            managed_unavailable_ns,
            backlog_published_ns,
            restart_started_ns,
            plane_available_ns,
            plane_ready_ns,
            converged_ns,
        },
    };
    Ok(evidence(
        &context,
        RecoveryDriver::PlaneRestartProjection,
        plane,
        metrics,
    ))
}

async fn run_memory_recovery(
    context: RecoveryContext<'_>,
    plane: NativePlane,
) -> Result<RecoveryEvidence, BenchError> {
    let names = RecoveryNames::new(context.scenario, context.seed);
    let stream = context.laser.stream(&names.stream);
    stream
        .ensure()
        .await
        .map_err(|error| recovery_error("create memory stream", &error))?;
    let memory = context
        .laser
        .with_default_stream(&names.stream)
        .memory_topic(&names.topic)
        .partitions(context.case.partitions)
        .no_expiry()
        .build()
        .await
        .map_err(|error| recovery_error("create memory topic", &error))?;
    let conversation =
        ConversationId::derive(&format!("{}:{}:recovery", context.scenario, context.seed));
    let payload = seeded_payload(context.case.payload_bytes, context.seed);
    publish_memory(
        &memory,
        conversation,
        &payload,
        0,
        context.case.backlog_records,
    )
    .await?;
    wait_for_memory(
        &memory,
        conversation,
        &payload,
        context.case.backlog_records,
        context.case.timeout,
    )
    .await?;
    let telemetry_before = start_telemetry(context.server, &plane, context.environment).await?;
    let telemetry_before = telemetry_before.stop().await?;
    let started = Instant::now();
    let fault_injected_ns = elapsed_ns(started);
    let stopped = plane.stop_for_restart().await?;
    let plane_stopped_ns = elapsed_ns(started);
    require_memory_unavailable(&memory, conversation).await?;
    let managed_unavailable_ns = elapsed_ns(started);
    publish_memory(
        &memory,
        conversation,
        &payload,
        context.case.backlog_records as u64,
        context.case.backlog_records,
    )
    .await?;
    let backlog_published_ns = elapsed_ns(started);
    let restart_started_ns = elapsed_ns(started);
    let restarted = restart_plane(context, stopped, started).await?;
    let expected = context.case.backlog_records.saturating_mul(2);
    let items = match wait_for_memory(
        &memory,
        conversation,
        &payload,
        expected,
        context.case.timeout,
    )
    .await
    {
        Ok(items) => items,
        Err(error) => return restarted.fail(error).await,
    };
    let duplicates = duplicate_payloads(&items);
    let converged_ns = elapsed_ns(started);
    let plane_available_ns = restarted.plane_available_ns;
    let plane_ready_ns = restarted.plane_ready_ns;
    let (plane, telemetry_after) = restarted.finish().await?;
    Ok(evidence(
        &context,
        RecoveryDriver::PlaneRestartMemory,
        plane,
        RecoveryMetrics {
            telemetry_before,
            telemetry_after,
            recovered_records: items.len(),
            duplicates,
            timeline: RecoveryTimeline {
                fault_injected_ns,
                plane_stopped_ns,
                managed_unavailable_ns,
                backlog_published_ns,
                restart_started_ns,
                plane_available_ns,
                plane_ready_ns,
                converged_ns,
            },
        },
    ))
}

fn evidence(
    context: &RecoveryContext<'_>,
    driver: RecoveryDriver,
    plane: NativePlane,
    metrics: RecoveryMetrics,
) -> RecoveryEvidence {
    let expected = context.case.backlog_records.saturating_mul(2);
    let valid = metrics.recovered_records == expected && metrics.duplicates == 0;
    RecoveryEvidence {
        summary: RecoverySummary {
            driver,
            backend_profile: plane.profile,
            baseline_records: context.case.backlog_records,
            backlog_records: context.case.backlog_records,
            recovered_records: metrics.recovered_records,
            expected_managed_errors: 1,
            unexpected_managed_errors: 0,
            duplicates: metrics.duplicates,
            timeline: metrics.timeline,
            outcomes: OutcomeCounts {
                offered: 1,
                dispatched: 1,
                completed: 1,
                successful: u64::from(valid),
                failed: u64::from(!valid),
                duplicates: metrics.duplicates,
                gaps: expected.saturating_sub(metrics.recovered_records) as u64,
                ..OutcomeCounts::default()
            },
            configuration: serde_json::json!({
                "fault": "plane_process_stop",
                "iggy_remained_available": true,
                "database_reused": true,
                "backlog_published_while_plane_stopped": true,
                "latency_boundary": "fault_injection_to_full_result_convergence",
                "observation_resolution_ns": 1_000_000,
            }),
        },
        plane,
        telemetry_before: metrics.telemetry_before,
        telemetry_after: metrics.telemetry_after,
    }
}

impl RestartedPlane {
    async fn fail(self, error: BenchError) -> Result<RecoveryEvidence, BenchError> {
        let _ = self.plane.shutdown().await;
        Err(error)
    }

    async fn finish(self) -> Result<(NativePlane, TelemetrySeries), BenchError> {
        let RestartedPlane {
            plane,
            telemetry,
            plane_available_ns: _,
            plane_ready_ns: _,
        } = self;
        let telemetry_after = telemetry.stop().await?;
        Ok((plane, telemetry_after))
    }
}

async fn restart_plane(
    context: RecoveryContext<'_>,
    stopped: StoppedPlane,
    started: Instant,
) -> Result<RestartedPlane, BenchError> {
    let mut plane = stopped
        .start(
            context.plane_manifest,
            context.server,
            context.environment,
            RESTART_INDEX,
        )
        .await?;
    let plane_available_ns = elapsed_ns(started);
    let telemetry = match start_telemetry(context.server, &plane, context.environment).await {
        Ok(telemetry) => telemetry,
        Err(error) => {
            let _ = plane.shutdown().await;
            return Err(error);
        }
    };
    if let Err(error) = plane.wait_ready(context.case.timeout).await {
        let _ = plane.shutdown().await;
        return Err(error);
    }
    Ok(RestartedPlane {
        plane,
        telemetry,
        plane_available_ns,
        plane_ready_ns: elapsed_ns(started),
    })
}

async fn publish_projection(
    topic: &Topic,
    names: &ProjectionNames,
    case: &ManagedCase,
    first: u64,
    count: usize,
) -> Result<(), BenchError> {
    let total = count.saturating_mul(2) as u64;
    let mut batch = topic.publish_batch();
    for id in first..first.saturating_add(count as u64) {
        let payload = query_payload(id, case, total)?;
        let record = Record::builder()
            .content_type(ContentType::Json)
            .projection_ref(names.projection.clone())
            .inline_payload()
            .build();
        batch = batch.add_record(payload, record);
    }
    batch
        .send()
        .await
        .map_err(|error| recovery_error("publish projection recovery batch", &error))?;
    Ok(())
}

async fn validate_projection(
    laser: &Laser,
    index: &str,
    expected: usize,
) -> Result<(usize, u64), BenchError> {
    let result = laser
        .query(index)
        .limit(expected)
        .fetch()
        .await
        .map_err(|error| recovery_error("validate projection recovery", &error))?;
    let ids = result
        .rows
        .iter()
        .filter_map(|row| row.headers.get("id"))
        .map(|value| value.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let duplicates = result.rows.len().saturating_sub(ids.len()) as u64;
    if ids.len() != expected {
        return Err(BenchError::Invalid(format!(
            "projection recovery returned {} unique rows, expected {expected}",
            ids.len()
        )));
    }
    Ok((ids.len(), duplicates))
}

async fn require_query_unavailable(laser: &Laser, index: &str) -> Result<(), BenchError> {
    if laser.query(index).limit(1).fetch().await.is_ok() {
        return Err(BenchError::Invalid(
            "managed query remained available after plane stopped".to_owned(),
        ));
    }
    Ok(())
}

async fn publish_memory(
    memory: &MemoryHandle,
    conversation: ConversationId,
    payload: &Bytes,
    first: u64,
    count: usize,
) -> Result<(), BenchError> {
    for id in first..first.saturating_add(count as u64) {
        memory
            .remember(record_payload(payload, id).map_err(BenchError::Invalid)?)
            .scope(conversation)
            .send()
            .await
            .map_err(|error| recovery_error("publish memory recovery record", &error))?;
    }
    Ok(())
}

async fn require_memory_unavailable(
    memory: &MemoryHandle,
    conversation: ConversationId,
) -> Result<(), BenchError> {
    if memory.recall(conversation).limit(1).fetch().await.is_ok() {
        return Err(BenchError::Invalid(
            "managed memory remained available after plane stopped".to_owned(),
        ));
    }
    Ok(())
}

async fn wait_for_memory(
    memory: &MemoryHandle,
    conversation: ConversationId,
    payload: &Bytes,
    expected: usize,
    timeout: Duration,
) -> Result<Vec<MemoryItem>, BenchError> {
    let expected_payloads = (0..expected as u64)
        .map(|id| record_payload(payload, id).map_err(BenchError::Invalid))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(items) = memory.recall(conversation).limit(expected).fetch().await {
            let actual = items
                .iter()
                .map(|item| item.payload.clone())
                .collect::<BTreeSet<_>>();
            if actual == expected_payloads {
                return Ok(items);
            }
        }
        if Instant::now() >= deadline {
            return Err(BenchError::Invalid(format!(
                "memory recovery did not converge to {expected} records within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

fn duplicate_payloads(items: &[MemoryItem]) -> u64 {
    let unique = items
        .iter()
        .map(|item| item.payload.as_slice())
        .collect::<BTreeSet<_>>();
    items.len().saturating_sub(unique.len()) as u64
}

async fn start_telemetry(
    server: &NativeIggy,
    plane: &NativePlane,
    environment: &Environment,
) -> Result<TelemetrySampler, BenchError> {
    TelemetrySampler::start(
        &server.connection_string,
        vec![
            ("laser-bench".to_owned(), std::process::id()),
            (
                "iggy-server-ng".to_owned(),
                server
                    .pid()
                    .ok_or_else(|| BenchError::Invalid("Iggy PID unavailable".to_owned()))?,
            ),
            (
                "plane".to_owned(),
                plane
                    .pid()
                    .ok_or_else(|| BenchError::Invalid("plane PID unavailable".to_owned()))?,
            ),
        ],
        plane.health_address,
        TELEMETRY_INTERVAL,
        environment
            .host
            .as_ref()
            .is_some_and(|host| host.perf_counters),
    )
    .await
}

async fn start_iggy_telemetry(
    server: &NativeIggy,
    environment: &Environment,
) -> Result<TelemetrySampler, BenchError> {
    TelemetrySampler::start(
        &server.connection_string,
        vec![
            ("laser-bench".to_owned(), std::process::id()),
            (
                "iggy-server-ng".to_owned(),
                server
                    .pid()
                    .ok_or_else(|| BenchError::Invalid("Iggy PID unavailable".to_owned()))?,
            ),
        ],
        None,
        TELEMETRY_INTERVAL,
        environment
            .host
            .as_ref()
            .is_some_and(|host| host.perf_counters),
    )
    .await
}

fn managed_case(case: &RecoveryCase) -> ManagedCase {
    ManagedCase {
        payload_bytes: case.payload_bytes,
        operations: 1,
        duration_seconds: 1,
        concurrency: 1,
        batch_size: 1,
        partitions: case.partitions,
        corpus_entries: None,
        warmup_seconds: 1,
        timeout_millis: case.timeout.as_millis().try_into().unwrap_or(u64::MAX),
        offered_rate: None,
        spin_dispatch: false,
        max_in_flight: None,
    }
}

#[derive(Clone)]
struct RecoveryNames {
    stream: String,
    topic: String,
}

impl RecoveryNames {
    fn new(scenario: &str, seed: u64) -> Self {
        let digest = Sha256::digest(scenario.as_bytes());
        let scenario = u64::from_be_bytes(
            digest[..size_of::<u64>()]
                .try_into()
                .expect("SHA-256 prefix has a fixed length"),
        );
        let suffix = format!("{scenario:016x}_{seed:016x}");
        Self {
            stream: format!("bench_recovery_stream_{suffix}"),
            topic: format!("bench_recovery_topic_{suffix}"),
        }
    }
}

fn validate_case(case: &RecoveryCase) -> Result<(), BenchError> {
    if case.payload_bytes < size_of::<u64>()
        || case.backlog_records == 0
        || case.backlog_records.saturating_mul(2) > 1_000
        || case.partitions == 0
        || case.timeout.is_zero()
    {
        return Err(BenchError::Invalid(
            "recovery requires a nonzero timeout, partitions and backlog, a payload that fits an ID, and at most 1,000 total records"
                .to_owned(),
        ));
    }
    Ok(())
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[allow(clippy::cast_precision_loss)]
fn as_f64(value: u64) -> f64 {
    value as f64
}

fn recovery_error(operation: &str, error: &laser_sdk::LaserError) -> BenchError {
    BenchError::Invalid(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_recovery_driver_names_when_parsed_then_should_use_snake_case() {
        for name in [
            "consumer_restart",
            "plane_restart_memory",
            "plane_restart_projection",
        ] {
            let driver = name
                .parse::<RecoveryDriver>()
                .expect("recovery driver should parse");
            let rendered: &'static str = driver.into();
            assert_eq!(rendered, name);
        }
        assert!("plane-restart-memory".parse::<RecoveryDriver>().is_err());
    }

    #[test]
    fn given_oversized_recovery_corpus_when_validated_then_should_reject_it() {
        let case = RecoveryCase {
            payload_bytes: 128,
            backlog_records: 501,
            partitions: 1,
            timeout: Duration::from_secs(1),
        };
        assert!(validate_case(&case).is_err());
    }
}

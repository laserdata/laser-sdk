use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use laser_sdk::kv::Kv;
use laser_sdk::laser::Laser;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use strum::{Display, EnumString, IntoStaticStr};

use crate::BenchError;
use crate::agdx::{record_payload, seeded_payload};
use crate::engine::{
    Dispatch, LoadResult, LoadTimeSeriesPoint, Operation, run_closed_loop, run_closed_loop_for,
    run_open_loop_for,
};
use crate::metrics::{ProcessDelta, ProcessSnapshot};
use crate::process::PlaneProfile;
use crate::report::OutcomeCounts;

mod batch;
mod fork;
mod graph;
mod memory;
mod projection;
mod query;
mod uds;

pub use batch::{
    ManagedBatchArm, ManagedBatchEvidence, ManagedBatchSummary, run_managed_batch_evidence,
};
pub use fork::{ForkArm, ForkEvidence, ForkSummary, run_fork_evidence};
pub use graph::{GraphArm, GraphEvidence, GraphSummary, run_graph_evidence};
pub use memory::{MemoryArm, MemoryEvidence, MemorySummary, run_memory_evidence};
pub use projection::{
    ProjectionArm, ProjectionEvidence, ProjectionSummary, run_projection_evidence,
};
pub(crate) use query::{ProjectionNames, prepare_projection, query_payload, wait_for_row_count};
pub use query::{QueryArm, QueryEvidence, QuerySummary, run_query_evidence};
pub use uds::{UdsArm, UdsEvidence, UdsSummary, run_uds_evidence};

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_managed_driver
)]
pub enum ManagedDriver {
    Batch,
    Fork,
    Graph,
    Kv,
    Memory,
    Projection,
    Query,
    Uds,
}

fn invalid_managed_driver(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported managed driver `{value}`"))
}

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_kv_arm
)]
pub enum KvArm {
    BatchGet,
    BatchIndividualGet,
    CasHotKey,
    CasUncontended,
    GetHit,
    GetMiss,
    #[serde(rename = "mixed_read_10")]
    #[strum(serialize = "mixed_read_10")]
    MixedRead10,
    #[serde(rename = "mixed_read_50")]
    #[strum(serialize = "mixed_read_50")]
    MixedRead50,
    #[serde(rename = "mixed_read_90")]
    #[strum(serialize = "mixed_read_90")]
    MixedRead90,
    ScanPage,
    SetInsert,
    SetOverwrite,
}

fn invalid_kv_arm(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported KV arm `{value}`"))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManagedCase {
    pub payload_bytes: usize,
    pub operations: u64,
    pub duration_seconds: u64,
    pub concurrency: usize,
    pub batch_size: usize,
    pub partitions: u32,
    pub corpus_entries: Option<u64>,
    pub warmup_seconds: u64,
    pub timeout_millis: u64,
    pub offered_rate: Option<u64>,
    pub spin_dispatch: bool,
    pub max_in_flight: Option<usize>,
}

/// Measured record identifiers start here. Warmup identifiers count up from
/// zero, so replay and final-state validation can separate the two
/// populations without knowing how many operations a timed warmup issued.
pub(crate) const MEASUREMENT_ID_OFFSET: u64 = 1_u64 << 63;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ManagedArmSummary {
    pub arm: String,
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

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ManagedProcessMeasurement {
    pub name: String,
    pub phase: String,
    pub delta: ProcessDelta,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct KvSummary {
    pub operation: ManagedArmSummary,
    pub backend_profile: PlaneProfile,
    pub namespace: String,
    pub cas_conflicts: u64,
    pub configuration: serde_json::Value,
}

pub struct KvEvidence {
    pub summary: KvSummary,
    pub load: LoadResult,
    pub processes: Vec<ManagedProcessMeasurement>,
}

#[derive(Clone)]
struct KvOperationContext {
    kv: Arc<Kv>,
    arm: KvArm,
    payload: Bytes,
    seed: u64,
    cas_conflicts: Arc<AtomicU64>,
    batch_size: usize,
    keyspace: u64,
}

/// Run one managed KV operation through the public SDK path.
///
/// # Errors
///
/// Returns an error when capabilities do not converge, setup fails, or the workload cannot run.
pub async fn run_kv_evidence(
    laser: &Laser,
    case: &ManagedCase,
    arm: KvArm,
    profile: PlaneProfile,
    scenario: &str,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<KvEvidence, BenchError> {
    validate_case(case)?;
    validate_arm(case, arm)?;
    wait_for_kv(laser, Duration::from_secs(30)).await?;
    let namespace = namespace(scenario, seed);
    let payload = seeded_payload(case.payload_bytes, seed);
    prepare_kv(laser, &namespace, case, arm, &payload, seed).await?;
    let cas_conflicts = Arc::new(AtomicU64::new(0));
    let operation_context = KvOperationContext {
        kv: Arc::new(laser.kv(&namespace)),
        arm,
        payload: payload.clone(),
        seed,
        cas_conflicts: Arc::clone(&cas_conflicts),
        batch_size: case.batch_size,
        keyspace: case.operations,
    };
    let timeout = Duration::from_millis(case.timeout_millis);
    warmup(case, timeout, operation_context.clone().operation(0)).await?;
    if arm == KvArm::CasHotKey {
        operation_context
            .kv
            .set(hot_key(seed))
            .bytes(counter_value(&payload, 0))
            .send()
            .await
            .map_err(|error| managed_error("KV hot-key warmup reset", &error))?;
    }
    cas_conflicts.store(0, Ordering::Relaxed);
    let operation = operation_context.operation(MEASUREMENT_ID_OFFSET);
    let before = capture_processes(monitored_processes)?;
    let mut load = run_load(case, timeout, operation).await?;
    let processes = finish_processes(before, "measurement")?;
    validate_final_state(
        &operation_context,
        &load.successful_sequences,
        &mut load.outcomes,
    )
    .await?;
    let operation = summarize(arm.into(), &load, case, values_per_kv_operation(arm, case));
    Ok(KvEvidence {
        summary: KvSummary {
            operation,
            backend_profile: profile,
            namespace,
            cas_conflicts: cas_conflicts.load(Ordering::Relaxed),
            configuration: serde_json::json!({
                "path": "laser_kv_through_iggy_and_plane",
                "backend_profile": profile,
                "capability": "kv",
                "setup_timed": false,
                "validation_timed": false,
            }),
        },
        load,
        processes,
    })
}

async fn wait_for_kv(laser: &Laser, timeout: Duration) -> Result<(), BenchError> {
    let deadline = Instant::now() + timeout;
    loop {
        if laser.refresh_capabilities().await.kv.available {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(BenchError::Invalid(format!(
                "plane did not advertise the KV capability within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn prepare_kv(
    laser: &Laser,
    namespace: &str,
    case: &ManagedCase,
    arm: KvArm,
    payload: &Bytes,
    seed: u64,
) -> Result<(), BenchError> {
    match arm {
        KvArm::GetMiss | KvArm::SetInsert => Ok(()),
        KvArm::CasHotKey => laser
            .kv(namespace)
            .set(hot_key(seed))
            .bytes(counter_value(payload, 0))
            .send()
            .await
            .map_err(|error| managed_error("KV hot-key setup", &error)),
        KvArm::ScanPage => {
            let entries = case.corpus_entries.ok_or_else(|| {
                BenchError::Invalid("scan_page requires corpus_entries".to_owned())
            })?;
            seed_entries(laser, namespace, payload, seed, entries, false).await
        }
        KvArm::BatchGet | KvArm::BatchIndividualGet => {
            let entries = case
                .operations
                .checked_mul(u64::try_from(case.batch_size).unwrap_or(u64::MAX))
                .ok_or_else(|| BenchError::Invalid("KV batch corpus overflowed".to_owned()))?;
            seed_entries(laser, namespace, payload, seed, entries, false).await
        }
        KvArm::CasUncontended | KvArm::SetOverwrite => {
            seed_entries(laser, namespace, payload, seed, case.operations, true).await
        }
        KvArm::GetHit | KvArm::MixedRead10 | KvArm::MixedRead50 | KvArm::MixedRead90 => {
            seed_entries(laser, namespace, payload, seed, case.operations, false).await
        }
    }
}

async fn seed_entries(
    laser: &Laser,
    namespace: &str,
    payload: &Bytes,
    seed: u64,
    entries: u64,
    zeroed: bool,
) -> Result<(), BenchError> {
    let kv = laser.kv(namespace);
    for id in 0..entries {
        let value = if zeroed {
            vec![0; payload.len()]
        } else {
            record_payload(payload, id).map_err(BenchError::Invalid)?
        };
        kv.set(key(seed, id))
            .bytes(value)
            .send()
            .await
            .map_err(|error| managed_error("KV setup", &error))?;
    }
    Ok(())
}

impl KvOperationContext {
    fn operation(&self, id_offset: u64) -> Operation {
        let context = self.clone();
        Arc::new(move |sequence| {
            let context = context.clone();
            Box::pin(async move {
                let id = id_offset
                    .checked_add(sequence)
                    .ok_or_else(|| "managed operation ID overflowed".to_owned())?;
                let id = context.prepared_id(id);
                match context.arm {
                    KvArm::BatchGet | KvArm::BatchIndividualGet => {
                        batch_get(
                            &context.kv,
                            &context.payload,
                            context.seed,
                            id,
                            context.batch_size,
                            context.arm,
                        )
                        .await
                    }
                    KvArm::CasHotKey => {
                        cas_hot_key(
                            &context.kv,
                            &context.payload,
                            context.seed,
                            &context.cas_conflicts,
                        )
                        .await
                    }
                    KvArm::CasUncontended => {
                        cas_uncontended(&context.kv, &context.payload, context.seed, id).await
                    }
                    KvArm::GetHit => get_hit(&context.kv, &context.payload, context.seed, id).await,
                    KvArm::GetMiss => get_miss(&context.kv, context.seed, id).await,
                    KvArm::MixedRead10 | KvArm::MixedRead50 | KvArm::MixedRead90 => {
                        mixed_operation(
                            &context.kv,
                            &context.payload,
                            context.seed,
                            id,
                            context.arm,
                        )
                        .await
                    }
                    KvArm::ScanPage => {
                        scan_page(&context.kv, &context.payload, context.batch_size).await
                    }
                    KvArm::SetInsert | KvArm::SetOverwrite => {
                        set_value(&context.kv, &context.payload, context.seed, id).await
                    }
                }
            })
        })
    }

    fn prepared_id(&self, id: u64) -> u64 {
        prepared_id(self.arm, self.keyspace, id)
    }
}

fn prepared_id(arm: KvArm, keyspace: u64, id: u64) -> u64 {
    if matches!(
        arm,
        KvArm::BatchGet
            | KvArm::BatchIndividualGet
            | KvArm::CasUncontended
            | KvArm::GetHit
            | KvArm::MixedRead10
            | KvArm::MixedRead50
            | KvArm::MixedRead90
            | KvArm::SetOverwrite
    ) {
        id % keyspace
    } else {
        id
    }
}

fn is_expected_record(payload: &Bytes, id: u64, value: &[u8]) -> bool {
    value.len() == payload.len()
        && value[..size_of::<u64>()] == id.to_le_bytes()
        && value[size_of::<u64>()..] == payload[size_of::<u64>()..]
}

async fn get_hit(kv: &Kv, payload: &Bytes, seed: u64, id: u64) -> Result<(), String> {
    match kv
        .get(key(seed, id))
        .await
        .map_err(|error| error.to_string())?
    {
        Some(value) if is_expected_record(payload, id, &value) => Ok(()),
        Some(_) => Err("KV get returned the wrong value".to_owned()),
        None => Err("KV get missed a seeded key".to_owned()),
    }
}

async fn get_miss(kv: &Kv, seed: u64, id: u64) -> Result<(), String> {
    match kv
        .get(key(seed, id))
        .await
        .map_err(|error| error.to_string())?
    {
        None => Ok(()),
        Some(_) => Err("KV get unexpectedly found a value".to_owned()),
    }
}

async fn set_value(kv: &Kv, payload: &Bytes, seed: u64, id: u64) -> Result<(), String> {
    kv.set(key(seed, id))
        .bytes(record_payload(payload, id)?)
        .send()
        .await
        .map_err(|error| error.to_string())
}

async fn mixed_operation(
    kv: &Kv,
    payload: &Bytes,
    seed: u64,
    id: u64,
    arm: KvArm,
) -> Result<(), String> {
    let read_percent = match arm {
        KvArm::MixedRead10 => 10,
        KvArm::MixedRead50 => 50,
        KvArm::MixedRead90 => 90,
        _ => return Err("mixed KV operation received a non-mixed arm".to_owned()),
    };
    if id % 100 < read_percent {
        get_hit(kv, payload, seed, id).await
    } else {
        set_value(kv, payload, seed, id).await
    }
}

async fn cas_uncontended(kv: &Kv, payload: &Bytes, seed: u64, id: u64) -> Result<(), String> {
    let entry = kv
        .get_entry(key(seed, id))
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "uncontended CAS key is missing".to_owned())?;
    kv.set(key(seed, id))
        .bytes(record_payload(payload, id)?)
        .expect_version(entry.version)
        .commit()
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

async fn cas_hot_key(
    kv: &Kv,
    payload: &Bytes,
    seed: u64,
    conflicts: &AtomicU64,
) -> Result<(), String> {
    for _ in 0..1_000 {
        let entry = kv
            .get_entry(hot_key(seed))
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "hot CAS key is missing".to_owned())?;
        let current = counter(&entry.value)?;
        let result = kv
            .set(hot_key(seed))
            .bytes(counter_value(payload, current.saturating_add(1)))
            .expect_version(entry.version)
            .commit()
            .await;
        match result {
            Ok(_) => return Ok(()),
            Err(error) if error.is_version_conflict() => {
                conflicts.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("hot-key CAS exceeded the retry bound".to_owned())
}

async fn scan_page(kv: &Kv, payload: &Bytes, page_size: usize) -> Result<(), String> {
    let page = kv
        .scan()
        .limit(page_size)
        .fetch()
        .await
        .map_err(|error| error.to_string())?;
    if page.entries.len() != page_size {
        return Err(format!(
            "KV scan returned {} entries, expected {page_size}",
            page.entries.len()
        ));
    }
    for entry in page.entries {
        let id = key_id(&entry.key)?;
        if !is_expected_record(payload, id, &entry.value) {
            return Err("KV scan returned the wrong value".to_owned());
        }
    }
    Ok(())
}

async fn batch_get(
    kv: &Kv,
    payload: &Bytes,
    seed: u64,
    logical_id: u64,
    batch_size: usize,
    arm: KvArm,
) -> Result<(), String> {
    let batch_size = u64::try_from(batch_size).map_err(|_| "batch size exceeds u64".to_owned())?;
    let first = logical_id
        .checked_mul(batch_size)
        .ok_or_else(|| "KV batch key range overflowed".to_owned())?;
    let ids = (first..first.saturating_add(batch_size)).collect::<Vec<_>>();
    match arm {
        KvArm::BatchGet => {
            let keys = ids.iter().map(|id| key(seed, *id)).collect::<Vec<_>>();
            let values = kv
                .get_many(&keys)
                .await
                .map_err(|error| error.to_string())?;
            validate_batch_values(payload, &ids, values)
        }
        KvArm::BatchIndividualGet => {
            let mut values = Vec::with_capacity(ids.len());
            for id in &ids {
                values.push(
                    kv.get(key(seed, *id))
                        .await
                        .map_err(|error| error.to_string())?,
                );
            }
            validate_batch_values(payload, &ids, values)
        }
        _ => Err("batch KV operation received a non-batch arm".to_owned()),
    }
}

fn validate_batch_values(
    payload: &Bytes,
    ids: &[u64],
    values: Vec<Option<Vec<u8>>>,
) -> Result<(), String> {
    if ids.len() != values.len() {
        return Err("KV batch result length did not match the request".to_owned());
    }
    for (id, value) in ids.iter().zip(values) {
        let value = value.ok_or_else(|| "KV batch missed a seeded key".to_owned())?;
        if !is_expected_record(payload, *id, &value) {
            return Err("KV batch returned the wrong value".to_owned());
        }
    }
    Ok(())
}

pub(crate) async fn warmup(
    case: &ManagedCase,
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
    if result.outcomes.successful == 0 {
        return Err(BenchError::Invalid(
            "managed warmup completed no operations".to_owned(),
        ));
    }
    check_warmup(&result)
}

/// Warm up with an exact operation count for the drivers whose setup corpus
/// and identifier arithmetic are coupled to the number of warmup operations
/// (fork, graph, memory, projection).
pub(crate) async fn warmup_count(
    count: u64,
    case: &ManagedCase,
    timeout: Duration,
    operation: Operation,
) -> Result<(), BenchError> {
    let result = run_closed_loop(count, case.concurrency, timeout, operation).await?;
    check_warmup(&result)
}

fn check_warmup(result: &LoadResult) -> Result<(), BenchError> {
    if result.outcomes.failed == 0 && result.outcomes.timed_out == 0 {
        return Ok(());
    }
    let first_error = result
        .samples
        .iter()
        .find_map(|sample| sample.error.as_deref())
        .unwrap_or("no explicit error");
    Err(BenchError::Invalid(format!(
        "managed warmup failed: failed={}, timed_out={}, first_error={first_error}",
        result.outcomes.failed, result.outcomes.timed_out
    )))
}

pub(crate) async fn run_load(
    case: &ManagedCase,
    timeout: Duration,
    operation: Operation,
) -> Result<LoadResult, BenchError> {
    match case.offered_rate {
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
            .await
        }
        None => {
            run_closed_loop_for(
                Duration::from_secs(case.duration_seconds),
                case.concurrency,
                timeout,
                operation,
            )
            .await
        }
    }
}

async fn validate_final_state(
    context: &KvOperationContext,
    successful_sequences: &[u64],
    outcomes: &mut OutcomeCounts,
) -> Result<(), BenchError> {
    let kv = &context.kv;
    let arm = context.arm;
    if matches!(
        arm,
        KvArm::BatchGet
            | KvArm::BatchIndividualGet
            | KvArm::GetHit
            | KvArm::GetMiss
            | KvArm::ScanPage
    ) {
        return Ok(());
    }
    if arm == KvArm::CasHotKey {
        let actual = kv
            .get(hot_key(context.seed))
            .await
            .map_err(|error| managed_error("KV hot-key validation", &error))?;
        let expected = outcomes.successful;
        match actual {
            None => outcomes.gaps = outcomes.gaps.saturating_add(1),
            Some(value) if counter(&value).map_err(BenchError::Invalid)? != expected => {
                outcomes.checksum_failures = outcomes.checksum_failures.saturating_add(1);
            }
            Some(_) => {}
        }
        return Ok(());
    }
    let ids = successful_sequences
        .iter()
        .map(|sequence| {
            MEASUREMENT_ID_OFFSET
                .checked_add(*sequence)
                .map(|id| context.prepared_id(id))
                .ok_or_else(|| BenchError::Invalid("managed validation ID overflowed".to_owned()))
        })
        .collect::<Result<std::collections::BTreeSet<_>, _>>()?;
    for id in ids {
        let actual = kv
            .get(key(context.seed, id))
            .await
            .map_err(|error| managed_error("KV validation", &error))?;
        let expected = record_payload(&context.payload, id).map_err(BenchError::Invalid)?;
        match actual {
            None => outcomes.gaps = outcomes.gaps.saturating_add(1),
            Some(value) if value != expected => {
                outcomes.checksum_failures = outcomes.checksum_failures.saturating_add(1);
            }
            Some(_) => {}
        }
    }
    Ok(())
}

fn values_per_kv_operation(arm: KvArm, case: &ManagedCase) -> u64 {
    if matches!(
        arm,
        KvArm::BatchGet | KvArm::BatchIndividualGet | KvArm::ScanPage
    ) {
        u64::try_from(case.batch_size).unwrap_or(u64::MAX)
    } else {
        1
    }
}

pub(crate) fn summarize(
    arm: &str,
    load: &LoadResult,
    case: &ManagedCase,
    values_per_operation: u64,
) -> ManagedArmSummary {
    let successful = load.outcomes.successful;
    let successful_bytes = successful
        .saturating_mul(values_per_operation)
        .saturating_mul(u64::try_from(case.payload_bytes).unwrap_or(u64::MAX));
    ManagedArmSummary {
        arm: arm.to_owned(),
        elapsed_ns: duration_ns(load.elapsed),
        operations_per_second: per_second(successful, load.elapsed),
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

pub(crate) fn capture_processes(
    monitored_processes: &[(String, u32)],
) -> Result<Vec<(String, ProcessSnapshot)>, BenchError> {
    monitored_processes
        .iter()
        .map(|(name, pid)| Ok((name.clone(), ProcessSnapshot::capture(*pid)?)))
        .collect()
}

pub(crate) fn finish_processes(
    before: Vec<(String, ProcessSnapshot)>,
    phase: &str,
) -> Result<Vec<ManagedProcessMeasurement>, BenchError> {
    before
        .into_iter()
        .map(|(name, snapshot)| {
            let later = ProcessSnapshot::capture(snapshot.pid)?;
            Ok(ManagedProcessMeasurement {
                name,
                phase: phase.to_owned(),
                delta: snapshot.delta(later)?,
            })
        })
        .collect()
}

pub(crate) fn validate_case(case: &ManagedCase) -> Result<(), BenchError> {
    if case.payload_bytes < size_of::<u64>()
        || case.operations == 0
        || case.duration_seconds == 0
        || case.concurrency == 0
        || case.batch_size == 0
        || case.partitions == 0
        || case.warmup_seconds == 0
        || case.timeout_millis == 0
    {
        return Err(BenchError::Invalid(
            "managed dimensions must be nonzero and payloads must fit an operation ID".to_owned(),
        ));
    }
    Ok(())
}

fn validate_arm(case: &ManagedCase, arm: KvArm) -> Result<(), BenchError> {
    if matches!(arm, KvArm::BatchGet | KvArm::BatchIndividualGet)
        && case.batch_size > laser_wire::limits::MAX_BATCH_OPS
    {
        return Err(BenchError::Invalid(format!(
            "managed batch size exceeds {}",
            laser_wire::limits::MAX_BATCH_OPS
        )));
    }
    if arm == KvArm::ScanPage {
        let corpus_entries = case
            .corpus_entries
            .ok_or_else(|| BenchError::Invalid("scan_page requires corpus_entries".to_owned()))?;
        let page_size = u64::try_from(case.batch_size)
            .map_err(|_| BenchError::Invalid("scan page size exceeds u64".to_owned()))?;
        if corpus_entries < page_size || case.batch_size > laser_wire::limits::MAX_SCAN_LIMIT {
            return Err(BenchError::Invalid(format!(
                "scan_page requires corpus_entries >= batch_size and batch_size <= {}",
                laser_wire::limits::MAX_SCAN_LIMIT
            )));
        }
    }
    Ok(())
}

fn key(seed: u64, id: u64) -> String {
    format!("item_{seed:016x}_{id:016x}")
}

fn namespace(scenario: &str, seed: u64) -> String {
    let digest = Sha256::digest(scenario.as_bytes());
    let scenario = u64::from_be_bytes(
        digest[..size_of::<u64>()]
            .try_into()
            .expect("SHA-256 prefix has a fixed length"),
    );
    format!("laser_bench_kv_{scenario:016x}_{seed:016x}")
}

fn hot_key(seed: u64) -> String {
    format!("hot_{seed:016x}")
}

fn key_id(key: &[u8]) -> Result<u64, String> {
    let key = std::str::from_utf8(key).map_err(|error| format!("KV key is not UTF-8: {error}"))?;
    let id = key
        .rsplit_once('_')
        .map(|(_, id)| id)
        .ok_or_else(|| "KV key has no operation ID".to_owned())?;
    u64::from_str_radix(id, 16).map_err(|error| format!("KV key has an invalid ID: {error}"))
}

fn counter_value(payload: &Bytes, value: u64) -> Vec<u8> {
    let mut body = payload.to_vec();
    body[..size_of::<u64>()].copy_from_slice(&value.to_le_bytes());
    body
}

fn counter(payload: &[u8]) -> Result<u64, String> {
    let value = payload
        .get(..size_of::<u64>())
        .ok_or_else(|| "KV counter payload is too short".to_owned())?;
    Ok(u64::from_le_bytes(
        value
            .try_into()
            .map_err(|_| "KV counter payload is invalid".to_owned())?,
    ))
}

#[allow(clippy::cast_precision_loss)]
fn per_second(count: u64, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64()
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn managed_error(context: &str, error: &laser_sdk::error::LaserError) -> BenchError {
    BenchError::Invalid(format!("{context} failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_managed_driver_names_when_parsed_then_should_use_snake_case() {
        assert_eq!(
            "kv".parse::<ManagedDriver>().expect("driver parses"),
            ManagedDriver::Kv
        );
        assert_eq!(
            "query".parse::<ManagedDriver>().expect("driver parses"),
            ManagedDriver::Query
        );
        assert_eq!(
            "projection"
                .parse::<ManagedDriver>()
                .expect("driver parses"),
            ManagedDriver::Projection
        );
        assert_eq!(
            "memory".parse::<ManagedDriver>().expect("driver parses"),
            ManagedDriver::Memory
        );
        assert!("managedKv".parse::<ManagedDriver>().is_err());
    }

    #[test]
    fn given_kv_arm_names_when_parsed_then_should_use_snake_case() {
        for name in [
            "batch_get",
            "batch_individual_get",
            "cas_hot_key",
            "cas_uncontended",
            "get_hit",
            "get_miss",
            "mixed_read_10",
            "mixed_read_50",
            "mixed_read_90",
            "scan_page",
            "set_insert",
            "set_overwrite",
        ] {
            let arm = name.parse::<KvArm>().expect("KV arm should parse");
            let rendered: &'static str = arm.into();
            assert_eq!(rendered, name);
        }
    }

    #[test]
    fn given_duration_exceeds_prepared_keyspace_when_selecting_key_then_should_cycle_only_prepared_arms()
     {
        assert_eq!(prepared_id(KvArm::GetHit, 1_000, 2_007), 7);
        assert_eq!(prepared_id(KvArm::SetOverwrite, 1_000, 2_007), 7);
        assert_eq!(prepared_id(KvArm::GetMiss, 1_000, 2_007), 2_007);
        assert_eq!(prepared_id(KvArm::SetInsert, 1_000, 2_007), 2_007);
    }

    #[test]
    fn given_kv_key_when_decoded_then_should_recover_the_operation_id() {
        let key = key(7, 42);
        assert_eq!(key_id(key.as_bytes()).expect("key should decode"), 42);
    }

    #[test]
    fn given_counter_payload_when_decoded_then_should_recover_the_counter() {
        let payload = Bytes::from(vec![0; 64]);
        let value = counter_value(&payload, 42);
        assert_eq!(counter(&value).expect("counter should decode"), 42);
    }
}

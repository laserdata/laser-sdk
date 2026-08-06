use std::sync::Arc;
use std::time::{Duration, Instant};

use laser_sdk::laser::Laser;
use laser_sdk::query::{Projection, ProjectionBinding, QueryResult};
use laser_sdk::stream::{ContentType, Record};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use strum::{Display, EnumString, IntoStaticStr};

use super::{
    ManagedArmSummary, ManagedCase, ManagedProcessMeasurement, capture_processes, finish_processes,
    run_load, summarize, validate_case, warmup,
};
use crate::BenchError;
use crate::engine::{LoadResult, Operation};
use crate::process::PlaneProfile;

const CORPUS_BATCH_SIZE: usize = 256;
const ID_FIELD: &str = "id";
const BUCKET_FIELD: &str = "bucket";
const SELECTED_FIELD: &str = "selected";
const SELECTED_VALUE: &str = "yes";
const COUNT_FIELD: &str = "count";

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_query_arm
)]
pub enum QueryArm {
    AggregateGroupBy,
    PageScan,
    PayloadOff,
    PayloadOn,
    PointPredicate,
    SelectiveFilter,
}

fn invalid_query_arm(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported query arm `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct QuerySummary {
    pub operation: ManagedArmSummary,
    pub backend_profile: PlaneProfile,
    pub stream: String,
    pub topic: String,
    pub index: String,
    pub corpus_entries: u64,
    pub configuration: serde_json::Value,
}

pub struct QueryEvidence {
    pub summary: QuerySummary,
    pub load: LoadResult,
    pub processes: Vec<ManagedProcessMeasurement>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct QueryDocument {
    id: String,
    bucket: String,
    selected: String,
    padding: String,
}

#[derive(Clone)]
struct QueryOperationContext {
    laser: Laser,
    index: String,
    arm: QueryArm,
    result_size: usize,
    corpus_entries: u64,
}

/// Run one managed query operation through the public projection and query APIs.
///
/// # Errors
///
/// Returns an error when capability discovery, corpus setup, materialization, or measurement fails.
pub async fn run_query_evidence(
    laser: &Laser,
    case: &ManagedCase,
    arm: QueryArm,
    profile: PlaneProfile,
    scenario: &str,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<QueryEvidence, BenchError> {
    validate_case(case)?;
    validate_query_case(case, arm)?;
    wait_for_query(laser, Duration::from_secs(30)).await?;
    let names = ProjectionNames::new(scenario, seed);
    let scoped = prepare_corpus(laser, case, &names, profile).await?;
    let corpus_entries = case
        .corpus_entries
        .ok_or_else(|| BenchError::Invalid("query requires corpus_entries".to_owned()))?;
    let operation_context = QueryOperationContext {
        laser: scoped,
        index: names.index.clone(),
        arm,
        result_size: case.batch_size,
        corpus_entries,
    };
    let timeout = Duration::from_millis(case.timeout_millis);
    warmup(case, timeout, operation_context.clone().operation()).await?;
    let before = capture_processes(monitored_processes)?;
    let load = run_load(case, timeout, operation_context.clone().operation()).await?;
    let processes = finish_processes(before, "measurement")?;
    operation_context.deep_validate().await?;
    let operation = summarize(arm.into(), &load, case, query_rows_per_operation(arm, case));
    Ok(QueryEvidence {
        summary: QuerySummary {
            operation,
            backend_profile: profile,
            stream: names.stream,
            topic: names.topic,
            index: names.index,
            corpus_entries,
            configuration: serde_json::json!({
                "path": "laser_query_through_iggy_and_plane",
                "backend_profile": profile,
                "capabilities": ["projections", "query"],
                "setup_timed": false,
                "materialization_wait_timed": false,
            }),
        },
        load,
        processes,
    })
}

#[derive(Clone)]
pub(crate) struct ProjectionNames {
    pub(crate) stream: String,
    pub(crate) topic: String,
    pub(crate) projection: String,
    pub(crate) index: String,
}

impl ProjectionNames {
    pub(crate) fn new(scenario: &str, seed: u64) -> Self {
        let digest = Sha256::digest(scenario.as_bytes());
        let scenario = u64::from_be_bytes(
            digest[..size_of::<u64>()]
                .try_into()
                .expect("SHA-256 prefix has a fixed length"),
        );
        let suffix = format!("{scenario:016x}_{seed:016x}");
        Self {
            stream: format!("bench_query_stream_{suffix}"),
            topic: format!("bench_query_topic_{suffix}"),
            projection: format!("bench_query_projection_{suffix}.v1"),
            index: format!("bench_query_index_{suffix}"),
        }
    }
}

async fn wait_for_query(laser: &Laser, timeout: Duration) -> Result<(), BenchError> {
    let deadline = Instant::now() + timeout;
    loop {
        let capabilities = laser.refresh_capabilities().await;
        if capabilities.managed && capabilities.query.available {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(BenchError::Invalid(format!(
                "plane did not advertise query and projections within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub(super) async fn prepare_corpus(
    laser: &Laser,
    case: &ManagedCase,
    names: &ProjectionNames,
    profile: PlaneProfile,
) -> Result<Laser, BenchError> {
    let (scoped, topic) = prepare_projection(laser, case, names, profile).await?;
    publish_corpus(&topic, case, names).await?;
    wait_for_materialization(&scoped, &names.index, case, Duration::from_mins(1)).await?;
    Ok(scoped)
}

pub(crate) async fn prepare_projection(
    laser: &Laser,
    case: &ManagedCase,
    names: &ProjectionNames,
    profile: PlaneProfile,
) -> Result<(Laser, laser_sdk::stream::Topic), BenchError> {
    let stream = laser.stream(&names.stream);
    stream
        .ensure()
        .await
        .map_err(|error| setup_error("create query stream", &error))?;
    let topic = stream.topic(&names.topic);
    topic
        .ensure(case.partitions)
        .await
        .map_err(|error| setup_error("create query topic", &error))?;
    let scoped = laser.with_default_stream(&names.stream);
    let projection = Projection::builder(names.projection.clone())
        .name(&names.index)
        .version(1)
        .content_type(ContentType::Json)
        .fields([ID_FIELD, BUCKET_FIELD, SELECTED_FIELD])
        .build();
    scoped
        .projections()
        .register(projection)
        .await
        .map_err(|error| setup_error("register query projection", &error))?;
    let binding = ProjectionBinding::builder()
        .source(&names.stream, &names.topic)
        .allow(names.projection.clone())
        .default_projection(names.projection.clone())
        .target_on(profile.projection_backend(), &names.index)
        .notify()
        .build();
    scoped
        .bindings()
        .apply(binding)
        .await
        .map_err(|error| setup_error("apply query binding", &error))?;
    wait_for_index(&scoped, &names.index, Duration::from_secs(30)).await?;
    Ok((scoped, topic))
}

async fn wait_for_index(laser: &Laser, index: &str, timeout: Duration) -> Result<(), BenchError> {
    let deadline = Instant::now() + timeout;
    loop {
        if laser.query(index).limit(1).fetch().await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(BenchError::Invalid(format!(
                "plane did not create query index `{index}` within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn publish_corpus(
    topic: &laser_sdk::stream::Topic,
    case: &ManagedCase,
    names: &ProjectionNames,
) -> Result<(), BenchError> {
    let entries = case
        .corpus_entries
        .ok_or_else(|| BenchError::Invalid("query requires corpus_entries".to_owned()))?;
    let mut first = 0;
    while first < entries {
        let last = first
            .saturating_add(u64::try_from(CORPUS_BATCH_SIZE).unwrap_or(u64::MAX))
            .min(entries);
        let mut batch = topic.publish_batch();
        for id in first..last {
            let payload = query_payload(id, case, entries)?;
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
            .map_err(|error| setup_error("publish query corpus", &error))?;
        first = last;
    }
    Ok(())
}

pub(crate) fn query_payload(
    id: u64,
    case: &ManagedCase,
    corpus_entries: u64,
) -> Result<Vec<u8>, BenchError> {
    let groups = u64::try_from(case.batch_size)
        .map_err(|_| BenchError::Invalid("query result size exceeds u64".to_owned()))?;
    let mut document = QueryDocument {
        id: format!("item_{id:016x}"),
        bucket: format!("group_{:04}", id % groups),
        selected: if id < groups {
            SELECTED_VALUE.to_owned()
        } else {
            "no".to_owned()
        },
        padding: String::new(),
    };
    let base = serde_json::to_vec(&document)?;
    if base.len() > case.payload_bytes {
        return Err(BenchError::Invalid(format!(
            "query payload size {} is too small for the generated document, minimum is {}",
            case.payload_bytes,
            base.len()
        )));
    }
    document.padding = "x".repeat(case.payload_bytes - base.len());
    let payload = serde_json::to_vec(&document)?;
    if payload.len() != case.payload_bytes || id >= corpus_entries {
        return Err(BenchError::Invalid(
            "query corpus generator violated its size or range contract".to_owned(),
        ));
    }
    Ok(payload)
}

pub(super) async fn wait_for_materialization(
    laser: &Laser,
    index: &str,
    case: &ManagedCase,
    timeout: Duration,
) -> Result<(), BenchError> {
    let expected = case
        .corpus_entries
        .ok_or_else(|| BenchError::Invalid("query requires corpus_entries".to_owned()))?;
    wait_for_row_count(laser, index, expected, timeout).await
}

pub(crate) async fn wait_for_row_count(
    laser: &Laser,
    index: &str,
    expected: u64,
    timeout: Duration,
) -> Result<(), BenchError> {
    let deadline = Instant::now() + timeout;
    loop {
        let total = laser
            .query(index)
            .limit(1)
            .with_total()
            .fetch()
            .await
            .ok()
            .and_then(|result| result.page.total)
            .unwrap_or_default();
        if total == expected {
            return Ok(());
        }
        if total > expected {
            return Err(BenchError::Invalid(format!(
                "query index `{index}` contains {total} rows, expected {expected}"
            )));
        }
        if Instant::now() >= deadline {
            return Err(BenchError::Invalid(format!(
                "query index `{index}` materialized {total}/{expected} rows within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

impl QueryOperationContext {
    fn operation(self) -> Operation {
        Arc::new(move |_| {
            let context = self.clone();
            Box::pin(async move { context.execute().await })
        })
    }

    async fn execute(&self) -> Result<(), String> {
        let result = match self.arm {
            QueryArm::AggregateGroupBy => {
                self.laser
                    .query(&self.index)
                    .count()
                    .group_by([BUCKET_FIELD])
                    .limit(self.result_size)
                    .fetch()
                    .await
            }
            QueryArm::PointPredicate => {
                self.laser
                    .query(&self.index)
                    .where_eq(ID_FIELD, "item_0000000000000000")
                    .limit(1)
                    .fetch()
                    .await
            }
            QueryArm::SelectiveFilter => {
                self.laser
                    .query(&self.index)
                    .filter_eq(SELECTED_FIELD, SELECTED_VALUE)
                    .limit(self.result_size)
                    .fetch()
                    .await
            }
            QueryArm::PageScan | QueryArm::PayloadOff => {
                self.laser
                    .query(&self.index)
                    .limit(self.result_size)
                    .fetch()
                    .await
            }
            QueryArm::PayloadOn => {
                self.laser
                    .query(&self.index)
                    .limit(self.result_size)
                    .with_payload()
                    .fetch()
                    .await
            }
        }
        .map_err(|error| error.to_string())?;
        self.validate_result(&result)
    }

    /// Decode every row body of one untimed representative query after the
    /// measured window. The timed path checks only row counts and payload
    /// presence, so the structural JSON check cannot distort latency samples.
    async fn deep_validate(&self) -> Result<(), BenchError> {
        if self.arm != QueryArm::PayloadOn {
            return Ok(());
        }
        let result = self
            .laser
            .query(&self.index)
            .limit(self.result_size)
            .with_payload()
            .fetch()
            .await
            .map_err(|error| setup_error("deep-validate query", &error))?;
        for row in &result.rows {
            let payload = row.payload.as_ref().ok_or_else(|| {
                BenchError::Invalid("payload_on deep validation omitted payload bytes".to_owned())
            })?;
            serde_json::from_slice::<QueryDocument>(payload).map_err(|error| {
                BenchError::Invalid(format!("query payload is invalid: {error}"))
            })?;
        }
        Ok(())
    }

    fn validate_result(&self, result: &QueryResult) -> Result<(), String> {
        let expected_rows = if self.arm == QueryArm::PointPredicate {
            1
        } else {
            self.result_size
        };
        if result.rows.len() != expected_rows {
            return Err(format!(
                "query returned {} rows, expected {expected_rows}",
                result.rows.len()
            ));
        }
        match self.arm {
            QueryArm::AggregateGroupBy => validate_aggregate(result, self.corpus_entries),
            QueryArm::PayloadOn => validate_payload_presence(result, true),
            QueryArm::PayloadOff => validate_payload_presence(result, false),
            QueryArm::PointPredicate => result
                .rows
                .first()
                .and_then(|row| row.headers.get(ID_FIELD))
                .filter(|id| id.as_str() == "item_0000000000000000")
                .map(|_| ())
                .ok_or_else(|| "point query returned the wrong row".to_owned()),
            QueryArm::SelectiveFilter => result
                .rows
                .iter()
                .all(|row| {
                    row.headers
                        .get(SELECTED_FIELD)
                        .is_some_and(|value| value == SELECTED_VALUE)
                })
                .then_some(())
                .ok_or_else(|| "selective query returned a non-matching row".to_owned()),
            QueryArm::PageScan => Ok(()),
        }
    }
}

fn validate_aggregate(result: &QueryResult, corpus_entries: u64) -> Result<(), String> {
    let mut total = 0_u64;
    for row in &result.rows {
        if !row.headers.contains_key(BUCKET_FIELD) {
            return Err("aggregate query omitted its group key".to_owned());
        }
        total = total.saturating_add(
            row.headers
                .get(COUNT_FIELD)
                .ok_or_else(|| "aggregate query omitted count".to_owned())?
                .parse::<u64>()
                .map_err(|error| format!("aggregate count is invalid: {error}"))?,
        );
    }
    if total == corpus_entries {
        Ok(())
    } else {
        Err(format!(
            "aggregate query counted {total} rows, expected {corpus_entries}"
        ))
    }
}

fn validate_payload_presence(result: &QueryResult, expected: bool) -> Result<(), String> {
    for row in &result.rows {
        match (&row.payload, expected) {
            (Some(_), true) | (None, false) => {}
            (Some(_), false) => return Err("payload_off query returned payload bytes".to_owned()),
            (None, true) => return Err("payload_on query omitted payload bytes".to_owned()),
        }
    }
    Ok(())
}

fn query_rows_per_operation(arm: QueryArm, case: &ManagedCase) -> u64 {
    if arm == QueryArm::PointPredicate {
        1
    } else {
        u64::try_from(case.batch_size).unwrap_or(u64::MAX)
    }
}

fn validate_query_case(case: &ManagedCase, arm: QueryArm) -> Result<(), BenchError> {
    let corpus_entries = case
        .corpus_entries
        .ok_or_else(|| BenchError::Invalid("query requires corpus_entries".to_owned()))?;
    let result_size = u64::try_from(case.batch_size)
        .map_err(|_| BenchError::Invalid("query result size exceeds u64".to_owned()))?;
    if case.batch_size > laser_wire::limits::MAX_PAGE_SIZE || corpus_entries < result_size {
        return Err(BenchError::Invalid(format!(
            "query requires corpus_entries >= batch_size and batch_size <= {}",
            laser_wire::limits::MAX_PAGE_SIZE
        )));
    }
    if arm == QueryArm::PointPredicate && case.batch_size != 1 {
        return Err(BenchError::Invalid(
            "point_predicate requires batch_size = 1".to_owned(),
        ));
    }
    Ok(())
}

fn setup_error(operation: &str, error: &laser_sdk::LaserError) -> BenchError {
    BenchError::Invalid(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_case(payload_bytes: usize, batch_size: usize) -> ManagedCase {
        ManagedCase {
            payload_bytes,
            operations: 1,
            duration_seconds: 1,
            concurrency: 1,
            batch_size,
            partitions: 1,
            corpus_entries: Some(1_000),
            warmup_seconds: 1,
            timeout_millis: 1_000,
            offered_rate: None,
            spin_dispatch: false,
            max_in_flight: None,
        }
    }

    #[test]
    fn given_query_arm_names_when_parsed_then_should_use_snake_case() {
        for name in [
            "aggregate_group_by",
            "page_scan",
            "payload_off",
            "payload_on",
            "point_predicate",
            "selective_filter",
        ] {
            let arm = name.parse::<QueryArm>().expect("query arm should parse");
            let rendered: &'static str = arm.into();
            assert_eq!(rendered, name);
        }
        assert!("point-predicate".parse::<QueryArm>().is_err());
    }

    #[test]
    fn given_query_payload_size_when_generated_then_should_match_exactly() {
        let case = query_case(256, 100);
        let payload = query_payload(42, &case, 1_000).expect("query payload should encode");
        assert_eq!(payload.len(), 256);
        let document: QueryDocument =
            serde_json::from_slice(&payload).expect("query payload should decode");
        assert_eq!(document.id, "item_000000000000002a");
        assert_eq!(document.bucket, "group_0042");
    }

    #[test]
    fn given_point_query_with_many_results_when_validated_then_should_reject_it() {
        let case = query_case(256, 100);
        assert!(validate_query_case(&case, QueryArm::PointPredicate).is_err());
    }
}

use std::sync::Arc;
use std::time::{Duration, Instant};

use laser_sdk::laser::Laser;
use laser_sdk::stream::{ContentType, Record, Topic};
use laser_sdk::watch::WatchReader;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};
use tokio::sync::Mutex;

use super::query::{ProjectionNames, prepare_projection, query_payload, wait_for_row_count};
use super::{
    ManagedArmSummary, ManagedCase, ManagedProcessMeasurement, capture_processes, finish_processes,
    run_load, summarize, validate_case, warmup_count,
};
use crate::BenchError;
use crate::engine::{LoadResult, Operation};
use crate::process::PlaneProfile;

const VISIBILITY_POLL_INTERVAL: Duration = Duration::from_millis(1);

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_projection_arm
)]
pub enum ProjectionArm {
    BacklogDrain,
    BurstIngest,
    ChangeRecordLag,
    QueryVisibleLag,
    SustainedIngest,
}

fn invalid_projection_arm(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported projection arm `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ProjectionSummary {
    pub operation: ManagedArmSummary,
    pub backend_profile: PlaneProfile,
    pub stream: String,
    pub topic: String,
    pub index: String,
    pub rows_per_operation: usize,
    pub materialization_wait_ns: u64,
    pub observation_resolution_ns: Option<u64>,
    pub configuration: serde_json::Value,
}

pub struct ProjectionEvidence {
    pub summary: ProjectionSummary,
    pub load: LoadResult,
    pub processes: Vec<ManagedProcessMeasurement>,
}

#[derive(Clone)]
struct ProjectionOperationContext {
    laser: Laser,
    topic: Topic,
    names: ProjectionNames,
    case: ManagedCase,
    arm: ProjectionArm,
    total_rows: u64,
    watch: Option<Arc<Mutex<WatchReader>>>,
}

/// Run projection ingest or visibility work through public SDK operations.
///
/// # Errors
///
/// Returns an error when the projection cannot be created, required capabilities are absent, or the load cannot execute.
pub async fn run_projection_evidence(
    laser: &Laser,
    case: &ManagedCase,
    arm: ProjectionArm,
    profile: PlaneProfile,
    scenario: &str,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<ProjectionEvidence, BenchError> {
    validate_case(case)?;
    validate_projection_case(case, arm)?;
    let names = ProjectionNames::new(scenario, seed);
    let (scoped, topic) = prepare_projection(laser, case, &names, profile).await?;
    let rows_per_operation = rows_per_operation(arm, case);
    let total_rows = u64::MAX;
    let watch = prepare_watch(&scoped, &names.index, arm).await?;
    let context = ProjectionOperationContext {
        laser: scoped,
        topic,
        names: names.clone(),
        case: case.clone(),
        arm,
        total_rows,
        watch,
    };
    let timeout = Duration::from_millis(case.timeout_millis);
    let warmup_operations = case.warmup_seconds.max(1);
    warmup_count(
        warmup_operations,
        case,
        timeout,
        context.clone().operation(0),
    )
    .await?;
    let warmup_rows = warmup_operations.saturating_mul(rows_per_operation);
    wait_for_row_count(&context.laser, &names.index, warmup_rows, timeout).await?;
    let before = capture_processes(monitored_processes)?;
    let mut load = run_load(case, timeout, context.clone().operation(warmup_operations)).await?;
    let processes = finish_processes(before, "measurement")?;
    let expected_rows =
        warmup_rows.saturating_add(load.outcomes.successful.saturating_mul(rows_per_operation));
    let materialization_started = Instant::now();
    if wait_for_row_count(&context.laser, &names.index, expected_rows, timeout)
        .await
        .is_err()
    {
        load.outcomes.gaps = load.outcomes.gaps.saturating_add(1);
    }
    let materialization_wait = materialization_started.elapsed();
    let operation = summarize(arm.into(), &load, case, rows_per_operation);
    Ok(ProjectionEvidence {
        summary: ProjectionSummary {
            operation,
            backend_profile: profile,
            stream: names.stream,
            topic: names.topic,
            index: names.index,
            rows_per_operation: usize::try_from(rows_per_operation).unwrap_or(usize::MAX),
            materialization_wait_ns: duration_ns(materialization_wait),
            observation_resolution_ns: visibility_arm(arm)
                .then_some(duration_ns(VISIBILITY_POLL_INTERVAL)),
            configuration: serde_json::json!({
                "path": "laser_projection_through_iggy_and_plane",
                "backend_profile": profile,
                "setup_timed": false,
                "warmup_materialization_timed": false,
                "final_validation_timed": false,
            }),
        },
        load,
        processes,
    })
}

async fn prepare_watch(
    laser: &Laser,
    index: &str,
    arm: ProjectionArm,
) -> Result<Option<Arc<Mutex<WatchReader>>>, BenchError> {
    if arm != ProjectionArm::ChangeRecordLag {
        return Ok(None);
    }
    if !laser.refresh_capabilities().await.watch {
        return Err(BenchError::Invalid(
            "change_record_lag requires the watch capability".to_owned(),
        ));
    }
    laser
        .watch()
        .index(index)
        .records()
        .map(|reader| Some(Arc::new(Mutex::new(reader))))
        .map_err(|error| BenchError::Invalid(format!("open projection change feed: {error}")))
}

impl ProjectionOperationContext {
    fn operation(self, operation_offset: u64) -> Operation {
        Arc::new(move |sequence| {
            let context = self.clone();
            Box::pin(async move {
                let operation = operation_offset
                    .checked_add(sequence)
                    .ok_or_else(|| "projection operation ID overflowed".to_owned())?;
                context.execute(operation).await
            })
        })
    }

    async fn execute(&self, operation: u64) -> Result<(), String> {
        match self.arm {
            ProjectionArm::BurstIngest | ProjectionArm::BacklogDrain => {
                self.publish_batch(operation).await?;
            }
            ProjectionArm::ChangeRecordLag
            | ProjectionArm::QueryVisibleLag
            | ProjectionArm::SustainedIngest => {
                self.publish_one(operation).await?;
            }
        }
        match self.arm {
            ProjectionArm::BacklogDrain => self.await_batch_visibility(operation).await,
            ProjectionArm::ChangeRecordLag => self.await_change_record().await,
            ProjectionArm::QueryVisibleLag => self.await_query_visibility(operation).await,
            ProjectionArm::BurstIngest | ProjectionArm::SustainedIngest => Ok(()),
        }
    }

    async fn publish_one(&self, operation: u64) -> Result<(), String> {
        let id = operation;
        let payload =
            query_payload(id, &self.case, self.total_rows).map_err(|error| error.to_string())?;
        self.topic
            .publish()
            .raw_bytes(payload, ContentType::Json)
            .projection_ref(self.names.projection.clone())
            .inline_payload()
            .send()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn publish_batch(&self, operation: u64) -> Result<(), String> {
        let rows = rows_per_operation(self.arm, &self.case);
        let first = operation
            .checked_mul(rows)
            .ok_or_else(|| "projection batch range overflowed".to_owned())?;
        let mut batch = self.topic.publish_batch();
        for id in first..first.saturating_add(rows) {
            let payload = query_payload(id, &self.case, self.total_rows)
                .map_err(|error| error.to_string())?;
            let record = Record::builder()
                .content_type(ContentType::Json)
                .projection_ref(self.names.projection.clone())
                .inline_payload()
                .build();
            batch = batch.add_record(payload, record);
        }
        batch
            .send()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    async fn await_batch_visibility(&self, operation: u64) -> Result<(), String> {
        let expected = operation
            .saturating_add(1)
            .saturating_mul(rows_per_operation(self.arm, &self.case));
        wait_for_row_count(
            &self.laser,
            &self.names.index,
            expected,
            Duration::from_millis(self.case.timeout_millis),
        )
        .await
        .map_err(|error| error.to_string())
    }

    async fn await_change_record(&self) -> Result<(), String> {
        let watch = self
            .watch
            .as_ref()
            .ok_or_else(|| "projection change feed is unavailable".to_owned())?;
        let deadline = Instant::now() + Duration::from_millis(self.case.timeout_millis);
        let mut watch = watch.lock().await;
        loop {
            if watch
                .poll()
                .await
                .map_err(|error| error.to_string())?
                .iter()
                .any(|change| change.rows > 0)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("projection change record timed out".to_owned());
            }
            tokio::time::sleep(VISIBILITY_POLL_INTERVAL).await;
        }
    }

    async fn await_query_visibility(&self, operation: u64) -> Result<(), String> {
        let expected = format!("item_{operation:016x}");
        let deadline = Instant::now() + Duration::from_millis(self.case.timeout_millis);
        loop {
            let visible = self
                .laser
                .query(&self.names.index)
                .where_eq("id", &expected)
                .limit(1)
                .fetch()
                .await
                .map_err(|error| error.to_string())?
                .rows
                .first()
                .and_then(|row| row.headers.get("id"))
                == Some(&expected);
            if visible {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err("projection query visibility timed out".to_owned());
            }
            tokio::time::sleep(VISIBILITY_POLL_INTERVAL).await;
        }
    }
}

fn rows_per_operation(arm: ProjectionArm, case: &ManagedCase) -> u64 {
    if matches!(
        arm,
        ProjectionArm::BurstIngest | ProjectionArm::BacklogDrain
    ) {
        u64::try_from(case.batch_size).unwrap_or(u64::MAX)
    } else {
        1
    }
}

fn visibility_arm(arm: ProjectionArm) -> bool {
    matches!(
        arm,
        ProjectionArm::BacklogDrain
            | ProjectionArm::ChangeRecordLag
            | ProjectionArm::QueryVisibleLag
    )
}

fn validate_projection_case(case: &ManagedCase, arm: ProjectionArm) -> Result<(), BenchError> {
    if !matches!(
        arm,
        ProjectionArm::BurstIngest | ProjectionArm::BacklogDrain
    ) && case.batch_size != 1
    {
        return Err(BenchError::Invalid(format!(
            "{arm} requires batch_size = 1"
        )));
    }
    if visibility_arm(arm) && case.concurrency != 1 {
        return Err(BenchError::Invalid(format!(
            "{arm} requires producers = 1 so visibility attribution is unambiguous"
        )));
    }
    Ok(())
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn projection_case(batch_size: usize, concurrency: usize) -> ManagedCase {
        ManagedCase {
            payload_bytes: 256,
            operations: 1,
            duration_seconds: 1,
            concurrency,
            batch_size,
            partitions: 1,
            corpus_entries: None,
            warmup_seconds: 1,
            timeout_millis: 1_000,
            offered_rate: None,
            spin_dispatch: false,
            max_in_flight: None,
        }
    }

    #[test]
    fn given_projection_arm_names_when_parsed_then_should_use_snake_case() {
        for name in [
            "backlog_drain",
            "burst_ingest",
            "change_record_lag",
            "query_visible_lag",
            "sustained_ingest",
        ] {
            let arm = name
                .parse::<ProjectionArm>()
                .expect("projection arm should parse");
            let rendered: &'static str = arm.into();
            assert_eq!(rendered, name);
        }
        assert!("query-visible-lag".parse::<ProjectionArm>().is_err());
    }

    #[test]
    fn given_visibility_arm_with_concurrency_when_validated_then_should_reject_it() {
        let case = projection_case(1, 2);
        assert!(validate_projection_case(&case, ProjectionArm::QueryVisibleLag).is_err());
    }

    #[test]
    fn given_burst_arm_when_counting_rows_then_should_use_batch_size() {
        let case = projection_case(100, 2);
        assert_eq!(rows_per_operation(ProjectionArm::BurstIngest, &case), 100);
    }
}

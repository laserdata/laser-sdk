use std::sync::Arc;
use std::time::{Duration, Instant};

use laser_sdk::laser::Laser;
use laser_sdk::query::QueryResult;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use strum::{Display, EnumString, IntoStaticStr};

use super::query::{ProjectionNames, prepare_corpus, prepare_projection};
use super::{
    ManagedArmSummary, ManagedCase, ManagedProcessMeasurement, capture_processes, finish_processes,
    run_load, summarize, validate_case, warmup_count,
};
use crate::BenchError;
use crate::engine::{LoadResult, Operation};
use crate::process::PlaneProfile;

const ID_FIELD: &str = "id";

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_fork_arm
)]
pub enum ForkArm {
    BaseSize,
    CreateContinuous,
    CreateSevered,
    Delete,
    OverlayPut,
    OverlayQuery,
    Promote,
    Squash,
}

fn invalid_fork_arm(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported fork arm `{value}`"))
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct ForkSummary {
    pub operation: ManagedArmSummary,
    pub backend_profile: PlaneProfile,
    pub fork_prefix: String,
    pub table: String,
    pub base_rows: u64,
    pub rows_per_operation: usize,
    pub configuration: serde_json::Value,
}

pub struct ForkEvidence {
    pub summary: ForkSummary,
    pub load: LoadResult,
    pub processes: Vec<ManagedProcessMeasurement>,
}

#[derive(Clone)]
struct ForkOperationContext {
    laser: Laser,
    arm: ForkArm,
    case: ManagedCase,
    prefix: String,
    table: String,
    projection: String,
}

/// Run one fork lifecycle operation through the public managed SDK surface.
///
/// # Errors
///
/// Returns an error when capability discovery, setup, execution, final-state validation, or cleanup fails.
pub async fn run_fork_evidence(
    laser: &Laser,
    case: &ManagedCase,
    arm: ForkArm,
    profile: PlaneProfile,
    scenario: &str,
    seed: u64,
    monitored_processes: &[(String, u32)],
) -> Result<ForkEvidence, BenchError> {
    validate_case(case)?;
    validate_fork_case(case, arm)?;
    wait_for_forks(laser, Duration::from_secs(30)).await?;
    let names = ProjectionNames::new(scenario, seed);
    let needs_projection = matches!(
        arm,
        ForkArm::BaseSize
            | ForkArm::OverlayPut
            | ForkArm::OverlayQuery
            | ForkArm::Promote
            | ForkArm::Squash
    );
    let scoped = if arm == ForkArm::BaseSize {
        prepare_corpus(laser, case, &names, profile).await?
    } else if needs_projection {
        prepare_projection(laser, case, &names, profile).await?.0
    } else {
        laser.clone()
    };
    let context = ForkOperationContext {
        laser: scoped,
        arm,
        case: case.clone(),
        prefix: fork_prefix(scenario, seed),
        table: names.index,
        projection: names.projection,
    };
    let timeout = Duration::from_millis(case.timeout_millis);
    prepare_forks(&context).await?;
    let warmup_operations = case.warmup_seconds.max(1);
    warmup_count(
        warmup_operations,
        case,
        timeout,
        context.clone().operation(0),
    )
    .await?;
    let before = capture_processes(monitored_processes)?;
    let load = run_load(case, timeout, context.clone().operation(warmup_operations)).await?;
    let processes = finish_processes(before, "measurement")?;
    validate_and_cleanup(&context, &load).await?;
    let rows_per_operation = rows_per_operation(arm, case);
    let operation = summarize(arm.into(), &load, case, rows_per_operation);
    Ok(ForkEvidence {
        summary: ForkSummary {
            operation,
            backend_profile: profile,
            fork_prefix: context.prefix,
            table: context.table,
            base_rows: if arm == ForkArm::BaseSize {
                case.corpus_entries.unwrap_or_default()
            } else {
                0
            },
            rows_per_operation: usize::try_from(rows_per_operation).unwrap_or(usize::MAX),
            configuration: serde_json::json!({
                "path": "laser_fork_through_iggy_and_plane",
                "backend_profile": profile,
                "setup_timed": false,
                "validation_timed": false,
            }),
        },
        load,
        processes,
    })
}

async fn wait_for_forks(laser: &Laser, timeout: Duration) -> Result<(), BenchError> {
    let deadline = Instant::now() + timeout;
    loop {
        if laser.refresh_capabilities().await.forks {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(BenchError::Invalid(format!(
                "plane did not advertise forks within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn prepare_forks(context: &ForkOperationContext) -> Result<(), BenchError> {
    if matches!(
        context.arm,
        ForkArm::BaseSize | ForkArm::CreateContinuous | ForkArm::CreateSevered
    ) {
        return Ok(());
    }
    let total = context
        .case
        .warmup_seconds
        .max(1)
        .checked_add(context.case.operations)
        .ok_or_else(|| BenchError::Invalid("fork operation count overflowed".to_owned()))?;
    for operation in 0..total {
        let id = context.id(operation);
        context
            .laser
            .fork(&id)
            .create()
            .severed()
            .send()
            .await
            .map_err(|error| fork_error("create prepared fork", &error))?;
        let rows = match context.arm {
            ForkArm::Promote | ForkArm::Squash => rows_per_operation(context.arm, &context.case),
            ForkArm::OverlayPut | ForkArm::OverlayQuery => 1,
            ForkArm::BaseSize
            | ForkArm::CreateContinuous
            | ForkArm::CreateSevered
            | ForkArm::Delete => 0,
        };
        for row in 0..rows {
            context.put(operation, row).await?;
        }
    }
    Ok(())
}

impl ForkOperationContext {
    fn operation(self, offset: u64) -> Operation {
        Arc::new(move |sequence| {
            let context = self.clone();
            Box::pin(async move {
                let operation = offset
                    .checked_add(sequence)
                    .ok_or_else(|| "fork operation ID overflowed".to_owned())?;
                context.execute(operation).await
            })
        })
    }

    async fn execute(&self, operation: u64) -> Result<(), String> {
        let id = self.id(operation);
        match self.arm {
            ForkArm::BaseSize => self
                .laser
                .fork(id)
                .create()
                .severed()
                .tables([self.table.clone()])
                .send()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            ForkArm::CreateSevered => self
                .laser
                .fork(id)
                .create()
                .severed()
                .send()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            ForkArm::CreateContinuous => self
                .laser
                .fork(id)
                .create()
                .continuous()
                .send()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            ForkArm::Delete | ForkArm::Squash => self
                .laser
                .fork(id)
                .squash()
                .await
                .and_then(|removed| {
                    removed.then_some(()).ok_or_else(|| {
                        laser_sdk::LaserError::Invalid("fork was not removed".to_owned())
                    })
                })
                .map_err(|error| error.to_string()),
            ForkArm::OverlayPut => self
                .put(operation, 0)
                .await
                .map_err(|error| error.to_string()),
            ForkArm::OverlayQuery => self.query(operation).await,
            ForkArm::Promote => self
                .laser
                .fork(id)
                .promote()
                .await
                .and_then(|rows| {
                    let expected = usize::try_from(rows_per_operation(self.arm, &self.case))
                        .unwrap_or(usize::MAX);
                    (rows == expected).then_some(()).ok_or_else(|| {
                        laser_sdk::LaserError::Invalid(format!(
                            "fork promoted {rows} rows, expected {expected}"
                        ))
                    })
                })
                .map_err(|error| error.to_string()),
        }
    }

    async fn put(&self, operation: u64, row: u64) -> Result<(), BenchError> {
        self.laser
            .fork(self.id(operation))
            .put_row(
                &self.table,
                0,
                operation.saturating_mul(1_000).saturating_add(row),
            )
            .projection(&self.projection, 1)
            .field(ID_FIELD, Self::row_id(operation, row))
            .field("bucket", "fork")
            .field("selected", "yes")
            .send()
            .await
            .map_err(|error| fork_error("put fork row", &error))
    }

    async fn query(&self, operation: u64) -> Result<(), String> {
        let result = self
            .laser
            .query(&self.table)
            .fork(self.id(operation))
            .where_eq(ID_FIELD, Self::row_id(operation, 0))
            .limit(1)
            .fetch()
            .await
            .map_err(|error| error.to_string())?;
        validate_overlay(&result, &Self::row_id(operation, 0))
    }

    fn id(&self, operation: u64) -> String {
        format!("{}_{operation:016x}", self.prefix)
    }

    fn row_id(operation: u64, row: u64) -> String {
        format!("fork_{operation:016x}_{row:016x}")
    }
}

async fn validate_and_cleanup(
    context: &ForkOperationContext,
    load: &LoadResult,
) -> Result<(), BenchError> {
    let first = context.case.warmup_seconds.max(1);
    let last = first.saturating_add(load.outcomes.offered);
    let open = context
        .laser
        .forks()
        .await
        .map_err(|error| fork_error("list forks", &error))?;
    for operation in first..last {
        let id = context.id(operation);
        let should_exist = matches!(
            context.arm,
            ForkArm::BaseSize
                | ForkArm::CreateContinuous
                | ForkArm::CreateSevered
                | ForkArm::OverlayPut
                | ForkArm::OverlayQuery
        );
        if open.iter().any(|fork| fork.fork_id == id) != should_exist {
            return Err(BenchError::Invalid(format!(
                "fork `{id}` final existence did not match {should_exist}"
            )));
        }
        if context.arm == ForkArm::OverlayPut {
            context
                .query(operation)
                .await
                .map_err(BenchError::Invalid)?;
        }
        if should_exist {
            context
                .laser
                .fork(id)
                .squash()
                .await
                .map_err(|error| fork_error("clean up fork", &error))?;
        }
    }
    Ok(())
}

fn validate_overlay(result: &QueryResult, expected: &str) -> Result<(), String> {
    result
        .rows
        .first()
        .and_then(|row| result.value_text(row, ID_FIELD))
        .filter(|value| value == expected)
        .map(|_| ())
        .ok_or_else(|| "fork overlay query returned the wrong row".to_owned())
}

fn fork_prefix(scenario: &str, seed: u64) -> String {
    let digest = Sha256::digest(scenario.as_bytes());
    let scenario = u64::from_be_bytes(
        digest[..size_of::<u64>()]
            .try_into()
            .expect("SHA-256 prefix has a fixed length"),
    );
    format!("bench_{scenario:016x}_{seed:08x}")
}

fn rows_per_operation(arm: ForkArm, case: &ManagedCase) -> u64 {
    if matches!(arm, ForkArm::Promote | ForkArm::Squash) {
        u64::try_from(case.batch_size).unwrap_or(u64::MAX)
    } else {
        1
    }
}

fn validate_fork_case(case: &ManagedCase, arm: ForkArm) -> Result<(), BenchError> {
    if arm == ForkArm::BaseSize && case.corpus_entries.is_none_or(|entries| entries == 0) {
        return Err(BenchError::Invalid(format!(
            "{arm} requires nonzero corpus_entries"
        )));
    }
    Ok(())
}

fn fork_error(operation: &str, error: &laser_sdk::LaserError) -> BenchError {
    BenchError::Invalid(format!("{operation}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_scenario_and_seed_when_prefix_is_built_then_should_be_stable_and_bounded() {
        let prefix = fork_prefix("fork-smoke", 7);
        assert_eq!(prefix, fork_prefix("fork-smoke", 7));
        assert!(format!("{prefix}_{:016x}", u64::MAX).len() <= 64);
    }

    #[test]
    fn given_base_size_without_corpus_when_validated_then_should_reject() {
        let case = ManagedCase {
            payload_bytes: 128,
            operations: 1,
            duration_seconds: 1,
            concurrency: 1,
            batch_size: 1,
            partitions: 1,
            corpus_entries: None,
            warmup_seconds: 1,
            timeout_millis: 1_000,
            offered_rate: None,
            spin_dispatch: false,
            max_in_flight: None,
        };
        assert!(validate_fork_case(&case, ForkArm::BaseSize).is_err());
    }
}

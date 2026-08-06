use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::BenchError;
use crate::engine::{Dispatch, Operation, run_open_loop_for};

const HEADROOM_NUMERATOR: u64 = 5;
const HEADROOM_DENOMINATOR: u64 = 4;
const MAX_P99_LATENESS_SLEEP_NS: u64 = 1_000_000;
const MAX_P99_LATENESS_SPIN_NS: u64 = 250_000;
const CALIBRATION_IN_FLIGHT: usize = 1_024;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SchedulerCalibration {
    pub highest_offered_rate: u64,
    pub calibration_rate: u64,
    pub duration_seconds: u64,
    pub scheduled_operations: u64,
    pub completed_operations: u64,
    pub achieved_operations_per_second: f64,
    pub dispatch: String,
    pub p99_lateness_bound_ns: u64,
    pub p50_lateness_ns: u64,
    pub p99_lateness_ns: u64,
    pub max_lateness_ns: u64,
    pub passed: bool,
}

/// Exercise the full open-loop dispatch path (timer wait, completed-task
/// drain, permit acquisition, and task spawn) with a no-op operation and no
/// server, above the campaign's highest offered rate.
///
/// # Errors
///
/// Returns an error when the rate or duration overflows the bounded calibration workload or the engine cannot run it.
pub async fn run_scheduler_calibration(
    highest_offered_rate: u64,
    duration_seconds: u64,
    dispatch: Dispatch,
) -> Result<SchedulerCalibration, BenchError> {
    let calibration_rate = highest_offered_rate
        .checked_mul(HEADROOM_NUMERATOR)
        .and_then(|rate| rate.checked_div(HEADROOM_DENOMINATOR))
        .filter(|rate| *rate > highest_offered_rate)
        .ok_or_else(|| BenchError::Invalid("scheduler calibration rate overflowed".to_owned()))?;
    let operations = calibration_rate
        .checked_mul(duration_seconds)
        .ok_or_else(|| BenchError::Invalid("scheduler calibration size overflowed".to_owned()))?;
    if operations == 0 || operations > 20_000_000 {
        return Err(BenchError::Invalid(format!(
            "scheduler calibration requires 1 through 20000000 operations, found {operations}"
        )));
    }
    let operation: Operation = Arc::new(|_| Box::pin(async { Ok(()) }));
    let load = run_open_loop_for(
        Duration::from_secs(duration_seconds),
        calibration_rate,
        CALIBRATION_IN_FLIGHT,
        Duration::from_secs(1),
        dispatch,
        operation,
    )
    .await?;
    let bound = match dispatch {
        Dispatch::Sleep => MAX_P99_LATENESS_SLEEP_NS,
        Dispatch::SpinWindow => MAX_P99_LATENESS_SPIN_NS,
    };
    let achieved_operations_per_second =
        as_f64(load.outcomes.completed) / load.elapsed.as_secs_f64();
    let p99_lateness_ns = load.scheduler_lateness.value_at_quantile(0.99);
    Ok(SchedulerCalibration {
        highest_offered_rate,
        calibration_rate,
        duration_seconds,
        scheduled_operations: load.outcomes.offered,
        completed_operations: load.outcomes.completed,
        achieved_operations_per_second,
        dispatch: format!("{dispatch:?}"),
        p99_lateness_bound_ns: bound,
        p50_lateness_ns: load.scheduler_lateness.value_at_quantile(0.5),
        p99_lateness_ns,
        max_lateness_ns: load.scheduler_lateness.max(),
        passed: load.outcomes.missed == 0
            && achieved_operations_per_second >= as_f64(calibration_rate) * 0.99
            && p99_lateness_ns <= bound,
    })
}

#[allow(clippy::cast_precision_loss)]
fn as_f64(value: u64) -> f64 {
    value as f64
}

#[cfg(test)]
mod tests {
    use super::run_scheduler_calibration;
    use crate::engine::Dispatch;

    #[tokio::test]
    async fn given_open_loop_rate_when_calibrated_then_should_record_real_timer_lateness() {
        let calibration = run_scheduler_calibration(100, 1, Dispatch::Sleep)
            .await
            .expect("small calibration should run");

        assert_eq!(calibration.calibration_rate, 125);
        assert_eq!(calibration.completed_operations, 125);
        assert!(calibration.achieved_operations_per_second > 0.0);
        assert_eq!(calibration.dispatch, "Sleep");
    }
}

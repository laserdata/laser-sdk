use serde::{Deserialize, Serialize};
use std::time::Instant;

use crate::BenchError;
use crate::metrics::ProcessSnapshot;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
pub struct ObserverCost {
    pub operations: u64,
    pub minimal_elapsed_ns: u64,
    pub instrumented_elapsed_ns: u64,
    pub elapsed_ratio: f64,
}

impl ObserverCost {
    /// Compare minimally instrumented and instrumented pilot runs.
    ///
    /// # Errors
    ///
    /// Returns an error when either run has no operations or elapsed time.
    pub fn compare(
        operations: u64,
        minimal_elapsed_ns: u64,
        instrumented_elapsed_ns: u64,
    ) -> Result<Self, BenchError> {
        if operations == 0 || minimal_elapsed_ns == 0 || instrumented_elapsed_ns == 0 {
            return Err(BenchError::Invalid(
                "observer-cost inputs must be nonzero".to_owned(),
            ));
        }
        let minimal = elapsed_as_f64(minimal_elapsed_ns);
        let instrumented = elapsed_as_f64(instrumented_elapsed_ns);
        Ok(Self {
            operations,
            minimal_elapsed_ns,
            instrumented_elapsed_ns,
            elapsed_ratio: instrumented / minimal,
        })
    }

    /// Run the same pilot operation count with minimal and full instrumentation.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation count or either measured duration is zero.
    pub fn measure<Minimal, Instrumented>(
        operations: u64,
        mut minimal: Minimal,
        mut instrumented: Instrumented,
    ) -> Result<Self, BenchError>
    where
        Minimal: FnMut(u64),
        Instrumented: FnMut(u64),
    {
        if operations == 0 {
            return Err(BenchError::Invalid(
                "observer-cost operation count must be nonzero".to_owned(),
            ));
        }
        let minimal_started = Instant::now();
        for operation in 0..operations {
            minimal(operation);
        }
        let minimal_elapsed_ns = duration_ns(minimal_started.elapsed());

        let instrumented_started = Instant::now();
        for operation in 0..operations {
            instrumented(operation);
        }
        let instrumented_elapsed_ns = duration_ns(instrumented_started.elapsed());
        Self::compare(operations, minimal_elapsed_ns, instrumented_elapsed_ns)
    }

    /// Measure periodic process-counter sampling against the same deterministic CPU pilot without sampling.
    ///
    /// # Errors
    ///
    /// Returns an error when process counters cannot be read or a measured duration is zero.
    pub fn process_sampling_pilot() -> Result<Self, BenchError> {
        const OPERATIONS_PER_ARM: u64 = 1_000_000;
        const SAMPLE_EVERY: u64 = 100_000;

        let pid = std::process::id();
        let minimal_a = run_process_pilot(OPERATIONS_PER_ARM, SAMPLE_EVERY, pid, false)?;
        let instrumented_a = run_process_pilot(OPERATIONS_PER_ARM, SAMPLE_EVERY, pid, true)?;
        let instrumented_b = run_process_pilot(OPERATIONS_PER_ARM, SAMPLE_EVERY, pid, true)?;
        let minimal_b = run_process_pilot(OPERATIONS_PER_ARM, SAMPLE_EVERY, pid, false)?;
        Self::compare(
            OPERATIONS_PER_ARM.saturating_mul(2),
            minimal_a.saturating_add(minimal_b),
            instrumented_a.saturating_add(instrumented_b),
        )
    }
}

fn run_process_pilot(
    operations: u64,
    sample_every: u64,
    pid: u32,
    instrumented: bool,
) -> Result<u64, BenchError> {
    let started = Instant::now();
    let mut state = 0xcbf2_9ce4_8422_2325_u64;
    for operation in 0..operations {
        state ^= operation;
        state = state.wrapping_mul(0x0000_0100_0000_01b3);
        if instrumented && operation % sample_every == 0 {
            std::hint::black_box(ProcessSnapshot::capture(pid)?);
        }
    }
    std::hint::black_box(state);
    Ok(duration_ns(started.elapsed()))
}

#[allow(clippy::cast_precision_loss)]
fn elapsed_as_f64(value: u64) -> f64 {
    value as f64
}

fn duration_ns(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_pilot_measurements_when_compared_then_should_report_observer_ratio() {
        let cost = ObserverCost::compare(1_000, 100_000, 125_000)
            .expect("pilot measurements should be valid");
        assert!((cost.elapsed_ratio - 1.25).abs() < f64::EPSILON);
    }

    #[test]
    fn given_empty_pilot_when_compared_then_should_reject_it() {
        assert!(ObserverCost::compare(0, 100, 100).is_err());
    }

    #[test]
    fn given_two_pilot_arms_when_measured_then_should_report_each_elapsed_time() {
        let cost = ObserverCost::measure(
            100,
            |operation| {
                std::hint::black_box(operation);
            },
            |operation| {
                std::hint::black_box(format!("operation-{operation}"));
            },
        )
        .expect("pilot should be measurable");
        assert_eq!(cost.operations, 100);
        assert!(cost.minimal_elapsed_ns > 0);
        assert!(cost.instrumented_elapsed_ns > 0);
    }

    #[test]
    fn given_linux_process_when_sampling_pilot_runs_then_should_record_counterbalanced_cost() {
        let cost = ObserverCost::process_sampling_pilot()
            .expect("process counter sampling pilot should complete");
        assert_eq!(cost.operations, 2_000_000);
        assert!(cost.elapsed_ratio.is_finite());
        assert!(cost.elapsed_ratio > 0.0);
    }
}

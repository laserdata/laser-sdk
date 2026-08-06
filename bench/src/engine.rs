use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::{Instant as TokioInstant, sleep_until};

use crate::BenchError;
use crate::report::OutcomeCounts;

pub type OperationFuture = Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
pub type Operation = Arc<dyn Fn(u64) -> OperationFuture + Send + Sync>;

/// How the open-loop dispatcher waits for the next scheduled arrival.
///
/// `Sleep` uses the Tokio timer, whose wheel granularity is about one
/// millisecond. `SpinWindow` sleeps until shortly before the arrival and then
/// spins on the monotonic clock, trading one busy client core for
/// sub-millisecond dispatch precision on latency-critical offered-rate cells.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Dispatch {
    #[default]
    Sleep,
    SpinWindow,
}

const SPIN_WINDOW: Duration = Duration::from_micros(1_500);

async fn wait_until_arrival(tokio_started: TokioInstant, arrival: Duration, dispatch: Dispatch) {
    match dispatch {
        Dispatch::Sleep => sleep_until(tokio_started + arrival).await,
        Dispatch::SpinWindow => {
            let target = tokio_started + arrival;
            if let Some(coarse) = target.checked_sub(SPIN_WINDOW)
                && TokioInstant::now() < coarse
            {
                sleep_until(coarse).await;
            }
            while TokioInstant::now() < target {
                std::hint::spin_loop();
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationOutcome {
    Successful,
    Failed,
    TimedOut,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationSample {
    pub sequence: u64,
    pub completed_elapsed_ns: u64,
    pub scheduled_response_ns: u64,
    pub service_ns: u64,
    pub scheduler_lateness_ns: u64,
    pub outcome: OperationOutcome,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct LoadTimeSeriesPoint {
    pub second: u64,
    pub outcomes: OutcomeCounts,
    pub max_in_flight: usize,
}

pub struct LoadResult {
    pub elapsed: Duration,
    pub outcomes: OutcomeCounts,
    pub successful_sequences: Vec<u64>,
    pub samples: Vec<OperationSample>,
    pub time_series: Vec<LoadTimeSeriesPoint>,
    pub scheduled_response: Histogram<u64>,
    pub service: Histogram<u64>,
    pub scheduler_lateness: Histogram<u64>,
    pub failed_service: Histogram<u64>,
}

/// Run a bounded open-loop schedule without moving arrivals when the client saturates.
///
/// # Errors
///
/// Returns an error for an empty or non-monotonic schedule, zero in-flight bound, histogram overflow, or task failure.
pub async fn run_open_loop(
    arrivals: &[Duration],
    max_in_flight: usize,
    timeout: Duration,
    dispatch: Dispatch,
    operation: Operation,
) -> Result<LoadResult, BenchError> {
    validate_arrivals(arrivals, max_in_flight)?;
    let started = Instant::now();
    let tokio_started = TokioInstant::now();
    let permits = Arc::new(Semaphore::new(max_in_flight));
    let mut tasks = JoinSet::new();
    let mut accumulator = LoadAccumulator::new(OutcomeCounts::default())?;

    for (index, arrival) in arrivals.iter().copied().enumerate() {
        wait_until_arrival(tokio_started, arrival, dispatch).await;
        accumulator.record_ready(&mut tasks)?;
        let observed = started.elapsed();
        let lateness = observed.saturating_sub(arrival);
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            accumulator.record_arrival(arrival, false, true, tasks.len());
            continue;
        };
        accumulator.record_arrival(arrival, true, false, tasks.len() + 1);
        let operation = Arc::clone(&operation);
        let sequence = u64::try_from(index)
            .map_err(|_| BenchError::Invalid("operation index exceeds u64".to_owned()))?;
        tasks.spawn(async move {
            let dispatched = Instant::now();
            let result = tokio::time::timeout(timeout, operation(sequence)).await;
            let completed = Instant::now();
            drop(permit);
            let (outcome, error) = match result {
                Ok(Ok(())) => (OperationOutcome::Successful, None),
                Ok(Err(error)) => (OperationOutcome::Failed, Some(error)),
                Err(_) => (OperationOutcome::TimedOut, None),
            };
            OperationSample {
                sequence,
                completed_elapsed_ns: duration_ns(completed.duration_since(started)),
                scheduled_response_ns: duration_ns(
                    completed.saturating_duration_since(started + arrival),
                ),
                service_ns: duration_ns(completed.duration_since(dispatched)),
                scheduler_lateness_ns: duration_ns(lateness),
                outcome,
                error,
            }
        });
    }
    accumulator.collect(tasks).await?;
    Ok(accumulator.finish(started))
}

/// Run a fixed-rate open-loop workload for the declared duration without moving arrivals when the client saturates.
///
/// # Errors
///
/// Returns an error for a zero duration, rate, or in-flight bound, histogram overflow, or task failure.
pub async fn run_open_loop_for(
    duration: Duration,
    rate_per_second: u64,
    max_in_flight: usize,
    timeout: Duration,
    dispatch: Dispatch,
    operation: Operation,
) -> Result<LoadResult, BenchError> {
    if duration.is_zero() || rate_per_second == 0 || max_in_flight == 0 {
        return Err(BenchError::Invalid(
            "open-loop duration, rate, and maximum in-flight must be nonzero".to_owned(),
        ));
    }
    u32::try_from(rate_per_second).map_err(|_| {
        BenchError::Invalid("open-loop rate exceeds the supported u32 range".to_owned())
    })?;
    let started = Instant::now();
    let tokio_started = TokioInstant::now();
    let permits = Arc::new(Semaphore::new(max_in_flight));
    let mut tasks = JoinSet::new();
    let mut accumulator = LoadAccumulator::new(OutcomeCounts::default())?;
    let mut sequence = 0_u64;

    loop {
        let arrival = fixed_arrival(sequence, rate_per_second)?;
        if arrival >= duration {
            break;
        }
        wait_until_arrival(tokio_started, arrival, dispatch).await;
        accumulator.record_ready(&mut tasks)?;
        let observed = started.elapsed();
        let lateness = observed.saturating_sub(arrival);
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            accumulator.record_arrival(arrival, false, true, tasks.len());
            sequence = sequence.checked_add(1).ok_or_else(|| {
                BenchError::Invalid("open-loop operation sequence overflowed".to_owned())
            })?;
            continue;
        };
        accumulator.record_arrival(arrival, true, false, tasks.len() + 1);
        let operation = Arc::clone(&operation);
        tasks.spawn(async move {
            let dispatched = Instant::now();
            let result = tokio::time::timeout(timeout, operation(sequence)).await;
            let completed = Instant::now();
            drop(permit);
            let (outcome, error) = match result {
                Ok(Ok(())) => (OperationOutcome::Successful, None),
                Ok(Err(error)) => (OperationOutcome::Failed, Some(error)),
                Err(_) => (OperationOutcome::TimedOut, None),
            };
            OperationSample {
                sequence,
                completed_elapsed_ns: duration_ns(completed.duration_since(started)),
                scheduled_response_ns: duration_ns(
                    completed.saturating_duration_since(started + arrival),
                ),
                service_ns: duration_ns(completed.duration_since(dispatched)),
                scheduler_lateness_ns: duration_ns(lateness),
                outcome,
                error,
            }
        });
        sequence = sequence.checked_add(1).ok_or_else(|| {
            BenchError::Invalid("open-loop operation sequence overflowed".to_owned())
        })?;
    }

    accumulator.collect(tasks).await?;
    Ok(accumulator.finish(started))
}

/// Run a fixed-operation closed-loop workload at a bounded concurrency.
///
/// # Errors
///
/// Returns an error for zero operations or concurrency, histogram overflow, or task failure.
pub async fn run_closed_loop(
    operations: u64,
    concurrency: usize,
    timeout: Duration,
    operation: Operation,
) -> Result<LoadResult, BenchError> {
    if operations == 0 || concurrency == 0 {
        return Err(BenchError::Invalid(
            "closed-loop operations and concurrency must be nonzero".to_owned(),
        ));
    }
    let started = Instant::now();
    let mut tasks = JoinSet::new();
    let mut accumulator = LoadAccumulator::new(OutcomeCounts::default())?;
    for sequence in 0..operations {
        if tasks.len() == concurrency {
            accumulator.record_next(&mut tasks).await?;
        }
        accumulator.record_arrival(started.elapsed(), true, false, tasks.len() + 1);
        spawn_closed_loop_operation(
            &mut tasks,
            sequence,
            timeout,
            Arc::clone(&operation),
            started,
        );
    }
    accumulator.collect(tasks).await?;
    Ok(accumulator.finish(started))
}

/// Run a duration-controlled closed-loop workload at a bounded concurrency.
///
/// No operation starts after the measurement deadline. Operations already in flight are allowed to complete or reach their timeout before the result is finalized.
///
/// # Errors
///
/// Returns an error for a zero duration or concurrency, histogram overflow, or task failure.
pub async fn run_closed_loop_for(
    duration: Duration,
    concurrency: usize,
    timeout: Duration,
    operation: Operation,
) -> Result<LoadResult, BenchError> {
    if duration.is_zero() || concurrency == 0 {
        return Err(BenchError::Invalid(
            "closed-loop duration and concurrency must be nonzero".to_owned(),
        ));
    }
    let started = Instant::now();
    let deadline = TokioInstant::now() + duration;
    let mut tasks = JoinSet::new();
    let mut accumulator = LoadAccumulator::new(OutcomeCounts::default())?;
    let mut sequence = 0_u64;

    loop {
        while tasks.len() < concurrency && TokioInstant::now() < deadline {
            accumulator.record_arrival(started.elapsed(), true, false, tasks.len() + 1);
            spawn_closed_loop_operation(
                &mut tasks,
                sequence,
                timeout,
                Arc::clone(&operation),
                started,
            );
            sequence = sequence.checked_add(1).ok_or_else(|| {
                BenchError::Invalid("closed-loop operation sequence overflowed".to_owned())
            })?;
        }
        if tasks.is_empty() {
            break;
        }
        if TokioInstant::now() >= deadline {
            break;
        }
        tokio::select! {
            result = tasks.join_next() => accumulator.record_join(result)?,
            () = sleep_until(deadline) => break,
        }
    }

    accumulator.collect(tasks).await?;
    Ok(accumulator.finish(started))
}

fn spawn_closed_loop_operation(
    tasks: &mut JoinSet<OperationSample>,
    sequence: u64,
    timeout: Duration,
    operation: Operation,
    started: Instant,
) {
    tasks.spawn(async move {
        let dispatched = Instant::now();
        let result = tokio::time::timeout(timeout, operation(sequence)).await;
        let completed = Instant::now();
        let (outcome, error) = match result {
            Ok(Ok(())) => (OperationOutcome::Successful, None),
            Ok(Err(error)) => (OperationOutcome::Failed, Some(error)),
            Err(_) => (OperationOutcome::TimedOut, None),
        };
        let service_ns = duration_ns(completed.duration_since(dispatched));
        OperationSample {
            sequence,
            completed_elapsed_ns: duration_ns(completed.duration_since(started)),
            scheduled_response_ns: service_ns,
            service_ns,
            scheduler_lateness_ns: 0,
            outcome,
            error,
        }
    });
}

struct LoadAccumulator {
    outcomes: OutcomeCounts,
    successful_sequences: Vec<u64>,
    samples: Vec<OperationSample>,
    time_series: BTreeMap<u64, LoadTimeSeriesPoint>,
    scheduled_response: Histogram<u64>,
    service: Histogram<u64>,
    scheduler_lateness: Histogram<u64>,
    failed_service: Histogram<u64>,
}

impl LoadAccumulator {
    fn new(outcomes: OutcomeCounts) -> Result<Self, BenchError> {
        Ok(Self {
            outcomes,
            successful_sequences: Vec::new(),
            samples: Vec::new(),
            time_series: BTreeMap::new(),
            scheduled_response: latency_histogram()?,
            service: latency_histogram()?,
            scheduler_lateness: latency_histogram()?,
            failed_service: latency_histogram()?,
        })
    }

    fn record_arrival(
        &mut self,
        elapsed: Duration,
        dispatched: bool,
        missed: bool,
        in_flight: usize,
    ) {
        self.outcomes.offered += 1;
        if dispatched {
            self.outcomes.dispatched += 1;
        }
        if missed {
            self.outcomes.missed += 1;
        }
        let point = self.time_series_point(elapsed);
        point.outcomes.offered += 1;
        if dispatched {
            point.outcomes.dispatched += 1;
        }
        if missed {
            point.outcomes.missed += 1;
        }
        point.max_in_flight = point.max_in_flight.max(in_flight);
    }

    fn record_ready(&mut self, tasks: &mut JoinSet<OperationSample>) -> Result<(), BenchError> {
        while let Some(result) = tasks.try_join_next() {
            self.record_join(Some(result))?;
        }
        Ok(())
    }

    async fn record_next(
        &mut self,
        tasks: &mut JoinSet<OperationSample>,
    ) -> Result<(), BenchError> {
        let result = tasks.join_next().await;
        self.record_join(result)
    }

    async fn collect(&mut self, mut tasks: JoinSet<OperationSample>) -> Result<(), BenchError> {
        while !tasks.is_empty() {
            self.record_next(&mut tasks).await?;
        }
        Ok(())
    }

    fn record_join(
        &mut self,
        result: Option<Result<OperationSample, tokio::task::JoinError>>,
    ) -> Result<(), BenchError> {
        let sample = result
            .ok_or_else(|| BenchError::Invalid("load task set ended unexpectedly".to_owned()))?
            .map_err(|error| BenchError::Invalid(format!("load task failed: {error}")))?;
        self.outcomes.completed += 1;
        match sample.outcome {
            OperationOutcome::Successful => {
                self.outcomes.successful += 1;
                self.successful_sequences.push(sample.sequence);
            }
            OperationOutcome::Failed => self.outcomes.failed += 1,
            OperationOutcome::TimedOut => self.outcomes.timed_out += 1,
        }
        let point = self.time_series_point(Duration::from_nanos(sample.completed_elapsed_ns));
        point.outcomes.completed += 1;
        match sample.outcome {
            OperationOutcome::Successful => point.outcomes.successful += 1,
            OperationOutcome::Failed => point.outcomes.failed += 1,
            OperationOutcome::TimedOut => point.outcomes.timed_out += 1,
        }
        self.scheduler_lateness
            .record(sample.scheduler_lateness_ns.max(1))
            .map_err(|error| BenchError::Invalid(format!("scheduler latency overflow: {error}")))?;
        if sample.outcome == OperationOutcome::Successful {
            self.scheduled_response
                .record(sample.scheduled_response_ns.max(1))
                .map_err(|error| {
                    BenchError::Invalid(format!("scheduled latency overflow: {error}"))
                })?;
            self.service
                .record(sample.service_ns.max(1))
                .map_err(|error| {
                    BenchError::Invalid(format!("service latency overflow: {error}"))
                })?;
        } else {
            self.failed_service
                .record(sample.service_ns.max(1))
                .map_err(|error| {
                    BenchError::Invalid(format!("failed latency overflow: {error}"))
                })?;
            self.samples.push(sample);
        }
        Ok(())
    }

    fn time_series_point(&mut self, elapsed: Duration) -> &mut LoadTimeSeriesPoint {
        let second = elapsed.as_secs();
        self.time_series
            .entry(second)
            .or_insert_with(|| LoadTimeSeriesPoint {
                second,
                outcomes: OutcomeCounts::default(),
                max_in_flight: 0,
            })
    }

    fn finish(mut self, started: Instant) -> LoadResult {
        self.samples.sort_by_key(|sample| sample.sequence);
        self.successful_sequences.sort_unstable();
        LoadResult {
            elapsed: started.elapsed(),
            outcomes: self.outcomes,
            successful_sequences: self.successful_sequences,
            samples: self.samples,
            time_series: self.time_series.into_values().collect(),
            scheduled_response: self.scheduled_response,
            service: self.service,
            scheduler_lateness: self.scheduler_lateness,
            failed_service: self.failed_service,
        }
    }
}

fn validate_arrivals(arrivals: &[Duration], max_in_flight: usize) -> Result<(), BenchError> {
    if arrivals.is_empty() || max_in_flight == 0 {
        return Err(BenchError::Invalid(
            "open-loop arrivals and maximum in-flight must be nonzero".to_owned(),
        ));
    }
    if arrivals.windows(2).any(|pair| pair[0] > pair[1]) {
        return Err(BenchError::Invalid(
            "open-loop arrivals must be monotonic".to_owned(),
        ));
    }
    Ok(())
}

fn fixed_arrival(sequence: u64, rate_per_second: u64) -> Result<Duration, BenchError> {
    let seconds = sequence / rate_per_second;
    let remainder = sequence % rate_per_second;
    let nanos = remainder
        .checked_mul(1_000_000_000)
        .ok_or_else(|| BenchError::Invalid("open-loop arrival overflowed".to_owned()))?
        / rate_per_second;
    Ok(Duration::from_secs(seconds) + Duration::from_nanos(nanos))
}

fn latency_histogram() -> Result<Histogram<u64>, BenchError> {
    Histogram::new_with_bounds(1, 3_600_000_000_000, 3)
        .map_err(|error| BenchError::Invalid(format!("invalid latency histogram: {error}")))
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn given_saturated_open_loop_when_run_then_should_record_misses_without_shifting() {
        let operation: Operation = Arc::new(|_| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            })
        });
        let result = run_open_loop(
            &[Duration::ZERO, Duration::from_millis(1)],
            1,
            Duration::from_secs(1),
            Dispatch::Sleep,
            operation,
        )
        .await
        .expect("open-loop run should complete");
        assert_eq!(result.outcomes.offered, 2);
        assert_eq!(result.outcomes.dispatched, 1);
        assert_eq!(result.outcomes.missed, 1);
    }

    #[tokio::test]
    async fn given_failure_and_timeout_when_run_then_should_keep_both_in_outcomes() {
        let operation: Operation = Arc::new(|sequence| {
            Box::pin(async move {
                if sequence == 0 {
                    Err("injected".to_owned())
                } else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok(())
                }
            })
        });
        let result = run_closed_loop(2, 2, Duration::from_millis(1), operation)
            .await
            .expect("closed-loop run should complete");
        assert_eq!(result.outcomes.failed, 1);
        assert_eq!(result.outcomes.timed_out, 1);
        assert_eq!(result.samples.len(), 2);
    }

    #[tokio::test]
    async fn given_duration_when_closed_loop_runs_then_should_measure_until_deadline() {
        let operation: Operation = Arc::new(|_| Box::pin(async { Ok(()) }));
        let duration = Duration::from_millis(20);
        let result = run_closed_loop_for(duration, 1, Duration::from_secs(1), operation)
            .await
            .expect("duration-controlled closed loop should complete");
        assert!(result.elapsed >= duration);
        assert!(result.outcomes.successful > 1);
        assert_eq!(result.outcomes.offered, result.outcomes.completed);
    }

    #[tokio::test]
    async fn given_fixed_rate_when_open_loop_runs_then_should_offer_for_full_duration() {
        let operation: Operation = Arc::new(|_| Box::pin(async { Ok(()) }));
        let result = run_open_loop_for(
            Duration::from_millis(25),
            100,
            1,
            Duration::from_secs(1),
            Dispatch::Sleep,
            operation,
        )
        .await
        .expect("duration-controlled open loop should complete");
        assert_eq!(result.outcomes.offered, 3);
        assert_eq!(result.outcomes.successful, 3);
        assert_eq!(
            result
                .time_series
                .iter()
                .map(|point| point.outcomes.offered)
                .sum::<u64>(),
            result.outcomes.offered
        );
        assert_eq!(
            result
                .time_series
                .iter()
                .map(|point| point.outcomes.successful)
                .sum::<u64>(),
            result.outcomes.successful
        );
    }
}

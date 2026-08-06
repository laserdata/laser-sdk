use std::time::Duration;

use crate::BenchError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArrivalModel {
    Fixed,
    Poisson,
}

#[derive(Clone, Copy, Debug)]
pub struct ScheduleSpec {
    pub model: ArrivalModel,
    pub rate_per_second: u64,
    pub operations: usize,
    pub seed: u64,
}

impl ScheduleSpec {
    /// Generate monotonic arrival offsets from the declared model.
    ///
    /// # Errors
    ///
    /// Returns an error when the rate or operation count is zero, or the rate cannot be represented safely.
    pub fn generate(self) -> Result<Vec<Duration>, BenchError> {
        if self.rate_per_second == 0 || self.operations == 0 {
            return Err(BenchError::Invalid(
                "schedule rate and operation count must be nonzero".to_owned(),
            ));
        }
        let rate = u32::try_from(self.rate_per_second).map_err(|_| {
            BenchError::Invalid("schedule rate exceeds the supported u32 range".to_owned())
        })?;
        let rate = f64::from(rate);
        let mut arrivals = Vec::with_capacity(self.operations);
        let mut elapsed = 0.0;
        let mut random = XorShift64::new(self.seed);
        for index in 0..self.operations {
            let interval = match self.model {
                ArrivalModel::Fixed => 1.0 / rate,
                ArrivalModel::Poisson => {
                    let sample = random.unit_interval();
                    -sample.ln() / rate
                }
            };
            if index > 0 {
                elapsed += interval;
            }
            arrivals.push(Duration::from_secs_f64(elapsed));
        }
        Ok(arrivals)
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct VirtualClock {
    now: Duration,
}

impl VirtualClock {
    #[must_use]
    pub fn now(&self) -> Duration {
        self.now
    }

    /// Move the virtual clock to a later instant.
    ///
    /// # Errors
    ///
    /// Returns an error when `target` is earlier than the current instant.
    pub fn advance_to(&mut self, target: Duration) -> Result<(), BenchError> {
        if target < self.now {
            return Err(BenchError::Invalid(
                "virtual clock cannot move backwards".to_owned(),
            ));
        }
        self.now = target;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchDecision {
    Dispatched {
        scheduled_at: Duration,
        dispatched_at: Duration,
    },
    Missed {
        scheduled_at: Duration,
        observed_at: Duration,
    },
    Pending,
    Complete,
}

pub struct OpenLoopScheduler {
    arrivals: Vec<Duration>,
    next: usize,
    in_flight: usize,
    max_in_flight: usize,
}

impl OpenLoopScheduler {
    /// Construct a bounded scheduler over monotonic arrival offsets.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero in-flight bound or non-monotonic arrivals.
    pub fn new(arrivals: Vec<Duration>, max_in_flight: usize) -> Result<Self, BenchError> {
        if max_in_flight == 0 {
            return Err(BenchError::Invalid(
                "maximum in-flight operations must be nonzero".to_owned(),
            ));
        }
        if arrivals.windows(2).any(|window| window[0] > window[1]) {
            return Err(BenchError::Invalid(
                "scheduled arrivals must be monotonic".to_owned(),
            ));
        }
        Ok(Self {
            arrivals,
            next: 0,
            in_flight: 0,
            max_in_flight,
        })
    }

    #[must_use]
    pub fn poll(&mut self, now: Duration) -> DispatchDecision {
        let Some(&scheduled_at) = self.arrivals.get(self.next) else {
            return DispatchDecision::Complete;
        };
        if scheduled_at > now {
            return DispatchDecision::Pending;
        }
        self.next += 1;
        if self.in_flight == self.max_in_flight {
            return DispatchDecision::Missed {
                scheduled_at,
                observed_at: now,
            };
        }
        self.in_flight += 1;
        DispatchDecision::Dispatched {
            scheduled_at,
            dispatched_at: now,
        }
    }

    /// Release one in-flight operation.
    ///
    /// # Errors
    ///
    /// Returns an error when no operation is currently in flight.
    pub fn complete_one(&mut self) -> Result<(), BenchError> {
        if self.in_flight == 0 {
            return Err(BenchError::Invalid(
                "cannot complete an operation when none are in flight".to_owned(),
            ));
        }
        self.in_flight -= 1;
        Ok(())
    }
}

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    #[allow(clippy::cast_precision_loss)]
    fn unit_interval(&mut self) -> f64 {
        let mut value = self.state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.state = value;
        let numerator = (value >> 11) as f64 + 1.0;
        numerator / ((1_u64 << 53) as f64 + 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_fixed_rate_when_generated_then_should_preserve_exact_schedule() {
        let arrivals = ScheduleSpec {
            model: ArrivalModel::Fixed,
            rate_per_second: 10,
            operations: 3,
            seed: 1,
        }
        .generate()
        .expect("fixed schedule should be valid");
        assert_eq!(
            arrivals,
            [
                Duration::ZERO,
                Duration::from_millis(100),
                Duration::from_millis(200)
            ]
        );
    }

    #[test]
    fn given_same_seed_when_generating_poisson_then_should_be_deterministic() {
        let spec = ScheduleSpec {
            model: ArrivalModel::Poisson,
            rate_per_second: 100,
            operations: 20,
            seed: 42,
        };
        assert_eq!(
            spec.generate().expect("first schedule should be valid"),
            spec.generate().expect("second schedule should be valid")
        );
    }

    #[test]
    fn given_saturated_scheduler_when_arrival_is_due_then_should_record_miss_without_shifting() {
        let mut scheduler =
            OpenLoopScheduler::new(vec![Duration::ZERO, Duration::from_millis(10)], 1)
                .expect("scheduler should be valid");
        assert!(matches!(
            scheduler.poll(Duration::ZERO),
            DispatchDecision::Dispatched { .. }
        ));
        assert_eq!(
            scheduler.poll(Duration::from_millis(20)),
            DispatchDecision::Missed {
                scheduled_at: Duration::from_millis(10),
                observed_at: Duration::from_millis(20)
            }
        );
    }

    #[test]
    fn given_virtual_clock_when_moved_backwards_then_should_reject_it() {
        let mut clock = VirtualClock::default();
        clock
            .advance_to(Duration::from_secs(1))
            .expect("forward advance should work");
        assert!(clock.advance_to(Duration::ZERO).is_err());
    }
}

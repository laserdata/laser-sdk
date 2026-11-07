use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// A source of the current time in epoch microseconds. The seam the SLA timer
/// and any deadline check read, so a test can drive time deterministically
/// instead of sleeping. Mirrors the `Deduplicator` and `StateStore` seams: one
/// trait, a real implementation, and a test double.
pub trait Clock: Send + Sync {
    /// The current time, epoch microseconds.
    fn now_micros(&self) -> u64;
}

/// The real clock, reading the same epoch-microsecond time the substrate stamps.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_micros(&self) -> u64 {
        iggy::prelude::IggyTimestamp::now().as_micros()
    }
}

/// A test clock whose time is set and advanced explicitly, so a deadline or SLA
/// test fires on demand without sleeping. Cheap to share (`&TestClock`).
#[derive(Debug, Default)]
pub struct TestClock {
    now_micros: AtomicU64,
}

impl TestClock {
    /// A test clock starting at `start_micros`.
    pub fn new(start_micros: u64) -> Self {
        Self {
            now_micros: AtomicU64::new(start_micros),
        }
    }

    /// Move time forward by `by`, kept to the microsecond.
    pub fn advance(&self, by: Duration) {
        let by_micros = u64::try_from(by.as_micros()).unwrap_or(u64::MAX);
        self.now_micros.fetch_add(by_micros, Ordering::Relaxed);
    }

    /// Set the absolute time.
    pub fn set(&self, now_micros: u64) {
        self.now_micros.store(now_micros, Ordering::Relaxed);
    }
}

impl Clock for TestClock {
    fn now_micros(&self) -> u64 {
        self.now_micros.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_test_clock_when_advanced_then_should_report_the_set_time() {
        let clock = TestClock::new(1_000);
        assert_eq!(clock.now_micros(), 1_000);
        clock.advance(Duration::from_micros(500));
        assert_eq!(clock.now_micros(), 1_500);
        clock.advance(Duration::from_millis(2));
        assert_eq!(clock.now_micros(), 3_500);
        clock.set(42);
        assert_eq!(clock.now_micros(), 42);
    }
}

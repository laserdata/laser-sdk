// Shared with `src/retry.ts`. Both are tested against `sdk/tests/fixtures/retry-table.json`.

// Builds without the agent runtime leave the consumer entries unused.
#![cfg_attr(not(feature = "agent"), allow(dead_code))]

use std::time::Duration;

/// A delay that doubles per consecutive failure from `base` up to `max`.
/// [`delay`](Self::delay) spreads it over its upper half.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Backoff {
    pub base: Duration,
    pub max: Duration,
}

impl Backoff {
    const fn from_millis(base: u64, max: u64) -> Self {
        Self {
            base: Duration::from_millis(base),
            max: Duration::from_millis(max),
        }
    }

    /// The delay before retry `attempt` (1 for the first retry), before jitter.
    pub fn ceiling(&self, attempt: u32) -> Duration {
        self.base
            .saturating_mul(2u32.saturating_pow(attempt.saturating_sub(1).min(16)))
            .min(self.max)
    }

    /// The jittered delay before retry `attempt`.
    pub fn delay(&self, attempt: u32) -> Duration {
        jittered(self.ceiling(attempt))
    }
}

/// Spread `delay` over its upper half, so clients that failed together do not
/// retry together.
pub(crate) fn jittered(delay: Duration) -> Duration {
    let random = (u128::from(ulid::Ulid::generate()) & 0xffff) as f64 / 65_535.0;
    delay.mul_f64(JITTER_LOW + random * (JITTER_HIGH - JITTER_LOW))
}

const JITTER_LOW: f64 = 0.5;
const JITTER_HIGH: f64 = 1.0;

/// A managed mutation or control append whose outcome is unknown repeats with
/// the same identity on this backoff until the deadline.
pub(crate) const MANAGED_MUTATION: Backoff = Backoff::from_millis(100, 2_000);
pub(crate) const MANAGED_MUTATION_DEADLINE: Duration = Duration::from_secs(30);

/// A reliable consumer or pause runtime reopens on this backoff, and gives up
/// after this many consecutive failures.
pub(crate) const CONSUMER: Backoff = Backoff::from_millis(50, 1_000);
pub(crate) const CONSUMER_FAILURE_BUDGET: u32 = 10;

/// The longest wait between looks for a control topic that does not exist yet.
pub(crate) const CONTROL_DISCOVERY_MAX: Duration = Duration::from_secs(30);

/// A consumer-group filter change is tried this many times, this far apart,
/// and its outcome is awaited this long.
pub(crate) const FILTER_MUTATION_ATTEMPTS: u32 = 3;
pub(crate) const FILTER_MUTATION_BACKOFF: Duration = Duration::from_millis(200);
pub(crate) const FILTER_OUTCOME_WAIT: Duration = Duration::from_secs(30);

/// A query without its own deadline gets this one, and no query may ask for
/// a later one. The server forwards a query reply for at most this long, so
/// the reply comes back inside the 30 second reply wait of the transport.
#[cfg_attr(not(feature = "query"), allow(dead_code))]
pub(crate) const QUERY_DEADLINE: Duration = Duration::from_secs(25);
/// How long past its deadline the client still waits for a query reply.
#[cfg_attr(not(feature = "query"), allow(dead_code))]
pub(crate) const QUERY_REPLY_GRACE: Duration = Duration::from_secs(2);

/// Producers racing to create the same stream or topic retry this often.
pub(crate) const CREATE_RACE_ATTEMPTS: usize = 10;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn millis(table: &Value, section: &str, field: &str) -> Duration {
        Duration::from_millis(table[section][field].as_u64().expect("a millisecond value"))
    }

    fn count(table: &Value, section: &str, field: &str) -> u64 {
        table[section][field].as_u64().expect("a count")
    }

    #[test]
    fn given_the_shared_retry_table_when_compared_then_should_match_every_rust_value() {
        let table: Value = serde_json::from_str(include_str!("../tests/fixtures/retry-table.json"))
            .expect("the table parses");
        assert_eq!(table["jitter"]["low"].as_f64(), Some(JITTER_LOW));
        assert_eq!(table["jitter"]["high"].as_f64(), Some(JITTER_HIGH));
        assert_eq!(
            MANAGED_MUTATION.base,
            millis(&table, "managed_mutation", "base_ms")
        );
        assert_eq!(
            MANAGED_MUTATION.max,
            millis(&table, "managed_mutation", "max_ms")
        );
        assert_eq!(
            MANAGED_MUTATION_DEADLINE,
            millis(&table, "managed_mutation", "deadline_ms")
        );
        assert_eq!(CONSUMER.base, millis(&table, "consumer", "base_ms"));
        assert_eq!(CONSUMER.max, millis(&table, "consumer", "max_ms"));
        assert_eq!(
            u64::from(CONSUMER_FAILURE_BUDGET),
            count(&table, "consumer", "failure_budget")
        );
        assert_eq!(
            CONTROL_DISCOVERY_MAX,
            millis(&table, "control_discovery", "max_ms")
        );
        assert_eq!(
            u64::from(FILTER_MUTATION_ATTEMPTS),
            count(&table, "filter_mutation", "attempts")
        );
        assert_eq!(
            FILTER_MUTATION_BACKOFF,
            millis(&table, "filter_mutation", "backoff_ms")
        );
        assert_eq!(
            FILTER_OUTCOME_WAIT,
            millis(&table, "filter_mutation", "outcome_wait_ms")
        );
        assert_eq!(QUERY_DEADLINE, millis(&table, "query", "deadline_ms"));
        assert_eq!(QUERY_REPLY_GRACE, millis(&table, "query", "reply_grace_ms"));
        assert_eq!(
            CREATE_RACE_ATTEMPTS as u64,
            count(&table, "create_race", "attempts")
        );
    }

    #[test]
    fn given_consecutive_failures_when_backing_off_then_should_double_up_to_the_cap() {
        assert_eq!(CONSUMER.ceiling(1), Duration::from_millis(50));
        assert_eq!(CONSUMER.ceiling(2), Duration::from_millis(100));
        assert_eq!(CONSUMER.ceiling(40), Duration::from_secs(1));
        assert_eq!(MANAGED_MUTATION.ceiling(0), Duration::from_millis(100));
        assert_eq!(MANAGED_MUTATION.ceiling(6), Duration::from_secs(2));
        for _ in 0..100 {
            let delay = CONSUMER.delay(3);
            assert!(delay >= Duration::from_millis(100) && delay <= Duration::from_millis(200));
        }
    }
}

/** The one retry and deadline table every TypeScript surface reads. The Rust
 * and Python clients read the same values from the Rust `retry` module. Both
 * tables are tested against `sdk/tests/fixtures/retry-table.json`, so the
 * clients cannot drift apart. */

/** A delay that doubles per consecutive failure from `baseMs` up to `maxMs`. */
export interface Backoff {
  readonly baseMs: number
  readonly maxMs: number
}

export const JITTER_LOW = 0.5
export const JITTER_HIGH = 1

/** The delay before retry `attempt` (1 for the first retry), before jitter. */
export function backoffCeiling(backoff: Backoff, attempt: number): number {
  return Math.min(backoff.baseMs * 2 ** Math.min(Math.max(attempt - 1, 0), 16), backoff.maxMs)
}

/** Spread `ms` over its upper half, so clients that failed together do not
 * retry together. */
export function jittered(ms: number): number {
  return ms * (JITTER_LOW + Math.random() * (JITTER_HIGH - JITTER_LOW))
}

/** The jittered delay before retry `attempt`. */
export function backoffDelay(backoff: Backoff, attempt: number): number {
  return jittered(backoffCeiling(backoff, attempt))
}

/** A managed mutation or control append whose outcome is unknown repeats with
 * the same identity on this backoff until the deadline. */
export const MANAGED_MUTATION: Backoff = { baseMs: 100, maxMs: 2_000 }
export const MANAGED_MUTATION_DEADLINE_MS = 30_000

/** A reliable consumer or pause runtime reopens on this backoff, and gives up
 * after this many consecutive failures. */
export const CONSUMER: Backoff = { baseMs: 50, maxMs: 1_000 }
export const CONSUMER_FAILURE_BUDGET = 10

/** The longest wait between looks for a control topic that does not exist yet. */
export const CONTROL_DISCOVERY_MAX_MS = 30_000

/** A consumer-group filter change is tried this many times, this far apart,
 * and its outcome is awaited this long. */
export const FILTER_MUTATION_ATTEMPTS = 3
export const FILTER_MUTATION_BACKOFF_MS = 200
export const FILTER_OUTCOME_WAIT_MS = 30_000

/** A query without its own deadline gets this one, and no query may ask for a
 * later one. The server forwards a query reply for at most this long, so the
 * reply comes back inside the 30 second reply wait of the transport. */
export const QUERY_DEADLINE_MS = 25_000
/** How long past its deadline the client still waits for a query reply. */
export const QUERY_REPLY_GRACE_MS = 2_000

/** Producers racing to create the same stream or topic retry this often. */
export const CREATE_RACE_ATTEMPTS = 10

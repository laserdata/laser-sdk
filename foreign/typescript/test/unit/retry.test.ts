import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import path from "node:path"
import { test } from "node:test"
import {
  CONSUMER,
  CONSUMER_FAILURE_BUDGET,
  CONTROL_DISCOVERY_MAX_MS,
  CREATE_RACE_ATTEMPTS,
  FILTER_MUTATION_ATTEMPTS,
  FILTER_MUTATION_BACKOFF_MS,
  FILTER_OUTCOME_WAIT_MS,
  JITTER_HIGH,
  JITTER_LOW,
  MANAGED_MUTATION,
  MANAGED_MUTATION_DEADLINE_MS,
  QUERY_DEADLINE_MS,
  QUERY_REPLY_GRACE_MS,
  backoffCeiling,
  backoffDelay
} from "../../src/retry.js"

interface RetryTable {
  readonly jitter: { readonly low: number; readonly high: number }
  readonly managed_mutation: {
    readonly base_ms: number
    readonly max_ms: number
    readonly deadline_ms: number
  }
  readonly consumer: {
    readonly base_ms: number
    readonly max_ms: number
    readonly failure_budget: number
  }
  readonly control_discovery: { readonly max_ms: number }
  readonly filter_mutation: {
    readonly attempts: number
    readonly backoff_ms: number
    readonly outcome_wait_ms: number
  }
  readonly query: { readonly deadline_ms: number; readonly reply_grace_ms: number }
  readonly create_race: { readonly attempts: number }
}

const TABLE = JSON.parse(
  readFileSync(path.resolve(process.cwd(), "../../sdk/tests/fixtures/retry-table.json"), "utf8")
) as RetryTable

void test("given_the_shared_retry_table_when_compared_then_should_match_every_typescript_value", () => {
  assert.equal(JITTER_LOW, TABLE.jitter.low)
  assert.equal(JITTER_HIGH, TABLE.jitter.high)
  assert.deepEqual(MANAGED_MUTATION, {
    baseMs: TABLE.managed_mutation.base_ms,
    maxMs: TABLE.managed_mutation.max_ms
  })
  assert.equal(MANAGED_MUTATION_DEADLINE_MS, TABLE.managed_mutation.deadline_ms)
  assert.deepEqual(CONSUMER, { baseMs: TABLE.consumer.base_ms, maxMs: TABLE.consumer.max_ms })
  assert.equal(CONSUMER_FAILURE_BUDGET, TABLE.consumer.failure_budget)
  assert.equal(CONTROL_DISCOVERY_MAX_MS, TABLE.control_discovery.max_ms)
  assert.equal(FILTER_MUTATION_ATTEMPTS, TABLE.filter_mutation.attempts)
  assert.equal(FILTER_MUTATION_BACKOFF_MS, TABLE.filter_mutation.backoff_ms)
  assert.equal(FILTER_OUTCOME_WAIT_MS, TABLE.filter_mutation.outcome_wait_ms)
  assert.equal(QUERY_DEADLINE_MS, TABLE.query.deadline_ms)
  assert.equal(QUERY_REPLY_GRACE_MS, TABLE.query.reply_grace_ms)
  assert.equal(CREATE_RACE_ATTEMPTS, TABLE.create_race.attempts)
})

void test("given_consecutive_failures_when_backing_off_then_should_double_up_to_the_cap", () => {
  assert.equal(backoffCeiling(CONSUMER, 1), 50)
  assert.equal(backoffCeiling(CONSUMER, 2), 100)
  assert.equal(backoffCeiling(CONSUMER, 40), 1_000)
  assert.equal(backoffCeiling(MANAGED_MUTATION, 0), 100)
  assert.equal(backoffCeiling(MANAGED_MUTATION, 6), 2_000)
  for (let index = 0; index < 100; index += 1) {
    const delay = backoffDelay(CONSUMER, 3)
    assert.ok(delay >= 100 && delay <= 200, String(delay))
  }
})

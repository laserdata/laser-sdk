import assert from "node:assert/strict"
import { test } from "node:test"
import { TestClock } from "../../src/runtime/clock.js"

void test("given_a_test_clock_when_advanced_then_should_report_the_set_time", () => {
  const clock = new TestClock(1_000n)
  assert.equal(clock.nowMicros(), 1_000n)
  clock.advance(500n)
  assert.equal(clock.nowMicros(), 1_500n)
  clock.set(42n)
  assert.equal(clock.nowMicros(), 42n)
})

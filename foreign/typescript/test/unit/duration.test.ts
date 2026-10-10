import assert from "node:assert/strict"
import { test } from "node:test"
import { millis, millisToMicros } from "../../src/client/duration.js"
import { InvalidError } from "../../src/client/errors.js"

void test("given_fractional_milliseconds_when_converted_then_should_match_the_python_microsecond_rule", () => {
  assert.equal(millisToMicros(0), 0n)
  assert.equal(millisToMicros(1.5), 1_500n)
  assert.equal(millisToMicros(1.1), 1_100n)
  assert.equal(millisToMicros(4.35), 4_350n)
  assert.equal(millisToMicros(0.0015), 1n)
  assert.equal(millisToMicros(0.0009999), 1n)
  assert.equal(millisToMicros(0.0004), 0n)
  assert.equal(millisToMicros(86_400_000), 86_400_000_000n)
})

void test("given_a_duration_when_checked_then_should_keep_fractions_and_refuse_negative_or_non_finite", () => {
  assert.equal(millis(0.25, "wait"), 0.25)
  assert.throws(() => millis(-0.5, "wait"), InvalidError)
  assert.throws(() => millis(Number.POSITIVE_INFINITY, "wait"), InvalidError)
  assert.throws(() => millis(Number.NaN, "wait"), InvalidError)
})

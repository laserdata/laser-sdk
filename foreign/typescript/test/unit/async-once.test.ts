import assert from "node:assert/strict"
import { test } from "node:test"
import { AsyncOnce } from "../../src/runtime/async-once.js"

void test("given_concurrent_callers_when_get_is_called_then_should_compute_only_once", async () => {
  const once = new AsyncOnce<number>()
  let calls = 0
  const compute = () =>
    new Promise<number>((resolve) => {
      calls += 1
      setTimeout(() => {
        resolve(42)
      }, 10)
    })

  const [a, b, c] = await Promise.all([once.get(compute), once.get(compute), once.get(compute)])
  assert.equal(calls, 1)
  assert.equal(a, 42)
  assert.equal(b, 42)
  assert.equal(c, 42)
})

void test("given_a_failed_computation_when_get_is_called_again_then_should_retry", async () => {
  const once = new AsyncOnce<number>()
  let attempt = 0
  const compute = () => {
    attempt += 1
    return attempt === 1 ? Promise.reject(new Error("boom")) : Promise.resolve(7)
  }

  await assert.rejects(once.get(compute), /boom/)
  const value = await once.get(compute)
  assert.equal(value, 7)
  assert.equal(attempt, 2)
})

void test("given_a_resolved_computation_when_get_is_called_again_then_should_return_the_cached_value", async () => {
  const once = new AsyncOnce<object>()
  const value = { id: 1 }
  const first = await once.get(() => Promise.resolve(value))
  const second = await once.get(() => Promise.resolve({ id: 2 }))
  assert.equal(first, value)
  assert.equal(second, value)
})

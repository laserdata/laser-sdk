import assert from "node:assert/strict"
import { test } from "node:test"
import { Mutex } from "../../src/runtime/mutex.js"

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

void test("given_concurrent_callers_when_run_exclusive_is_called_then_should_serialize_in_call_order", async () => {
  const mutex = new Mutex()
  const order: number[] = []

  const first = mutex.runExclusive(async () => {
    await delay(20)
    order.push(1)
  })
  const second = mutex.runExclusive(() => {
    order.push(2)
    return Promise.resolve()
  })
  const third = mutex.runExclusive(() => {
    order.push(3)
    return Promise.resolve()
  })

  await Promise.all([first, second, third])
  assert.deepEqual(order, [1, 2, 3])
})

void test("given_a_body_that_throws_when_run_exclusive_is_called_then_should_release_the_lock", async () => {
  const mutex = new Mutex()

  await assert.rejects(
    mutex.runExclusive(() => Promise.reject(new Error("boom"))),
    /boom/
  )

  const order: number[] = []
  await mutex.runExclusive(() => {
    order.push(1)
    return Promise.resolve()
  })
  assert.deepEqual(order, [1])
})

void test("given_run_exclusive_when_the_body_resolves_then_should_return_its_value", async () => {
  const mutex = new Mutex()
  const value = await mutex.runExclusive(() => Promise.resolve("done"))
  assert.equal(value, "done")
})

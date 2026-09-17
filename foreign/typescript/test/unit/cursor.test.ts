import assert from "node:assert/strict"
import { test } from "node:test"
import { CancelledError, InvalidError } from "../../src/client/errors.js"
import type { LaserTransport, PolledMessage } from "../../src/iggy/apache-iggy.js"
import { Cursor } from "../../src/stream/cursor.js"

void test("given_later_partition_failure_when_polling_then_should_preserve_all_offsets", async () => {
  let fail = true
  const transport = {
    pollMessages(_stream, _topic, target, strategy) {
      assert.equal(target.kind, "single")
      if (target.partitionId === 1 && fail) {
        fail = false
        return Promise.reject(new Error("partition unavailable"))
      }
      assert.equal(strategy.kind, "offset")
      assert.equal(strategy.value, 0n)
      return Promise.resolve([message(target.partitionId, 0n)])
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const cursor = new Cursor(transport as unknown as LaserTransport, "test", "events", [0, 1])

  await assert.rejects(cursor.poll(), /partition unavailable/u)
  assert.deepEqual(
    cursor.offsets,
    new Map([
      [0, 0n],
      [1, 0n]
    ])
  )
  assert.equal((await cursor.poll()).length, 2)
  assert.deepEqual(
    cursor.offsets,
    new Map([
      [0, 1n],
      [1, 1n]
    ])
  )
})

void test("given_abort_during_a_partition_read_when_polling_then_should_preserve_all_offsets", async () => {
  const controller = new AbortController()
  const transport = {
    pollMessages(_stream, _topic, target) {
      assert.equal(target.kind, "single")
      if (target.partitionId === 1) controller.abort()
      return Promise.resolve([message(target.partitionId, 0n)])
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const cursor = new Cursor(transport as unknown as LaserTransport, "test", "events", [0, 1])

  await assert.rejects(cursor.poll({ signal: controller.signal }), CancelledError)
  assert.deepEqual(
    cursor.offsets,
    new Map([
      [0, 0n],
      [1, 0n]
    ])
  )
  assert.equal((await cursor.poll()).length, 2)
})

void test("given_large_batches_when_polling_then_should_bound_reads_and_resume_without_gaps", async () => {
  const transport = {
    pollMessages(_stream, _topic, _target, strategy, count) {
      assert.ok(count <= 10_000)
      assert.equal(strategy.kind, "offset")
      const offset = Number(strategy.value)
      return Promise.resolve(
        Array.from({ length: Math.min(count, 10_001 - offset) }, (_, index) =>
          message(0, BigInt(offset + index))
        )
      )
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const cursor = new Cursor(transport as unknown as LaserTransport, "test", "events", [0], {
    batchSize: 20_000
  })

  assert.equal((await cursor.poll()).length, 10_000)
  assert.deepEqual(cursor.offsets, new Map([[0, 10_000n]]))
  const remaining = await cursor.poll()
  assert.equal(remaining.length, 1)
  assert.equal(remaining[0]?.offset, 10_000n)
})

void test("given_invalid_batch_sizes_when_configuring_a_cursor_then_should_reject_before_polling", () => {
  const transport = {} as LaserTransport
  const cursor = new Cursor(transport, "test", "events", [0])
  for (const size of [-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, 0x1_0000_0000]) {
    assert.throws(() => cursor.batch(size), InvalidError)
    assert.throws(
      () => new Cursor(transport, "test", "events", [0], { batchSize: size }),
      InvalidError
    )
  }
})

void test("given_a_checkpoint_boundary_and_later_failure_when_polling_then_should_preserve_offsets_until_success", async () => {
  let fail = true
  const transport = {
    pollMessages(_stream, _topic, target, strategy) {
      assert.equal(target.kind, "single")
      assert.equal(strategy.kind, "offset")
      assert.equal(strategy.value, 0n)
      if (target.partitionId === 1 && fail) {
        fail = false
        return Promise.reject(new Error("partition unavailable"))
      }
      return Promise.resolve([message(target.partitionId, 2n)])
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const cursor = new Cursor(transport as unknown as LaserTransport, "test", "events", [0, 1]).until(
    new Map([
      [0, 1n],
      [1, 1n]
    ])
  )

  await assert.rejects(cursor.poll(), /partition unavailable/u)
  assert.deepEqual(
    cursor.offsets,
    new Map([
      [0, 0n],
      [1, 0n]
    ])
  )
  assert.deepEqual(await cursor.poll(), [])
  assert.deepEqual(
    cursor.offsets,
    new Map([
      [0, 1n],
      [1, 1n]
    ])
  )
  assert.deepEqual(await cursor.poll(), [])
})

function message(partitionId: number, offset: bigint): PolledMessage {
  return { partitionId, offset, payload: new Uint8Array([1]), headers: new Map() }
}

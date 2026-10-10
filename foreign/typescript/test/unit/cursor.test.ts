import assert from "node:assert/strict"
import { test } from "node:test"
import { CancelledError, InvalidError } from "../../src/client/errors.js"
import type { LaserTransport, PolledMessage } from "../../src/iggy/apache-iggy.js"
import { Json } from "../../src/stream/codecs.js"
import { Cursor } from "../../src/stream/cursor.js"
import { TypedRecords } from "../../src/stream/typed-topic.js"

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
  const cursor = Cursor.create(transport as unknown as LaserTransport, "test", "events", [0, 1])

  await assert.rejects(cursor.poll(), /partition unavailable/u)
  assert.deepEqual(cursor.offsets, new Map())
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
  const cursor = Cursor.create(transport as unknown as LaserTransport, "test", "events", [0, 1])

  await assert.rejects(cursor.poll({ signal: controller.signal }), CancelledError)
  assert.deepEqual(cursor.offsets, new Map())
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
  const cursor = Cursor.create(transport as unknown as LaserTransport, "test", "events", [0]).batch(
    20_000
  )

  assert.equal((await cursor.poll()).length, 10_000)
  assert.deepEqual(cursor.offsets, new Map([[0, 10_000n]]))
  const remaining = await cursor.poll()
  assert.equal(remaining.length, 1)
  assert.equal(remaining[0]?.id.offset, 10_000n)
})

void test("given_invalid_batch_sizes_when_configuring_a_cursor_then_should_reject_before_polling", () => {
  const transport = {} as LaserTransport
  const cursor = Cursor.create(transport, "test", "events", [0])
  for (const size of [-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, 0x1_0000_0000]) {
    assert.throws(() => cursor.batch(size), InvalidError)
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
  const cursor = Cursor.create(
    transport as unknown as LaserTransport,
    "test",
    "events",
    [0, 1]
  ).until(
    new Map([
      [0, 1n],
      [1, 1n]
    ])
  )

  await assert.rejects(cursor.poll(), /partition unavailable/u)
  assert.deepEqual(cursor.offsets, new Map())
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

void test("given_a_fresh_cursor_when_polled_then_should_report_no_offsets_until_the_first_poll_and_every_partition_after", async () => {
  const transport = {
    pollMessages(_stream, _topic, target) {
      assert.equal(target.kind, "single")
      return Promise.resolve(target.partitionId === 0 ? [message(0, 0n)] : [])
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const cursor = Cursor.create(transport as unknown as LaserTransport, "test", "events", [0, 1])

  assert.deepEqual(cursor.offsets, new Map())
  assert.equal((await cursor.poll()).length, 1)
  assert.deepEqual(
    cursor.offsets,
    new Map([
      [0, 1n],
      [1, 0n]
    ])
  )
})

void test("given_persisted_offsets_when_resuming_then_should_replace_the_offsets_and_read_unnamed_partitions_from_zero", async () => {
  const starts: [number, bigint][] = []
  const transport = {
    pollMessages(_stream, _topic, target, strategy) {
      assert.equal(target.kind, "single")
      assert.equal(strategy.kind, "offset")
      starts.push([target.partitionId, strategy.value])
      return Promise.resolve([])
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const cursor = Cursor.create(transport as unknown as LaserTransport, "test", "events", [0, 1])

  cursor.fromOffsets(new Map([[1, 7n]]))
  assert.deepEqual(cursor.offsets, new Map([[1, 7n]]))
  await cursor.poll()
  assert.deepEqual(starts, [
    [0, 0n],
    [1, 7n]
  ])
})

void test("given_the_default_batch_when_a_partition_holds_more_than_one_request_then_should_drain_it_in_one_poll", async () => {
  const counts: number[] = []
  const transport = {
    pollMessages(_stream, _topic, _target, strategy, count) {
      assert.equal(strategy.kind, "offset")
      counts.push(count)
      const offset = Number(strategy.value)
      return Promise.resolve(
        Array.from({ length: Math.max(0, Math.min(count, 2_500 - offset)) }, (_, index) =>
          message(0, BigInt(offset + index))
        )
      )
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const cursor = Cursor.create(transport as unknown as LaserTransport, "test", "events", [0])

  assert.equal((await cursor.poll()).length, 2_500)
  assert.deepEqual(counts, [1_000, 1_000, 1_000])
  assert.deepEqual(cursor.offsets, new Map([[0, 2_500n]]))
})

void test("given_records_on_several_partitions_when_polled_then_should_order_them_by_log_timestamp", async () => {
  const transport = {
    pollMessages(_stream, _topic, target) {
      assert.equal(target.kind, "single")
      return Promise.resolve(
        target.partitionId === 0
          ? [stamped(0, 0n, 20n), stamped(0, 1n, 40n)]
          : [stamped(1, 0n, 10n), stamped(1, 1n, 30n)]
      )
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const cursor = Cursor.create(transport as unknown as LaserTransport, "test", "events", [0, 1])

  const read = (await cursor.poll()).map((record) => [record.id.partitionId, record.id.offset])
  assert.deepEqual(read, [
    [1, 0n],
    [0, 0n],
    [1, 1n],
    [0, 1n]
  ])
})

void test("given_a_cursor_stream_when_the_reader_catches_up_then_should_end", async () => {
  let served = false
  const transport = {
    pollMessages() {
      if (served) return Promise.resolve([])
      served = true
      return Promise.resolve([message(0, 0n), message(0, 1n)])
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const cursor = Cursor.create(transport as unknown as LaserTransport, "test", "events", [0])

  const read: bigint[] = []
  for await (const record of cursor.stream()) read.push(record.id.offset)
  assert.deepEqual(read, [0n, 1n])
})

void test("given_a_typed_reader_when_calling_next_then_should_yield_one_record_at_a_time_and_undefined_when_caught_up", async () => {
  let served = false
  const transport = {
    pollMessages() {
      if (served) return Promise.resolve([])
      served = true
      return Promise.resolve([
        json(0n, { cpu: 1 }),
        { ...message(0, 1n), payload: new TextEncoder().encode("not json") },
        json(2n, { cpu: 3 })
      ])
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const records = TypedRecords.create(
    Cursor.create(transport as unknown as LaserTransport, "test", "events", [0]),
    new Json<{ readonly cpu: number }>()
  )

  const first = await records.next()
  assert.ok(first?.kind === "record")
  assert.equal(first.record.value.cpu, 1)
  const second = await records.next()
  assert.ok(second?.kind === "error")
  assert.deepEqual(second.error.position, { partitionId: 0, offset: 1n })
  const third = await records.next()
  assert.ok(third?.kind === "record")
  assert.equal(third.record.value.cpu, 3)
  assert.equal(await records.next(), undefined)
  assert.deepEqual(records.offsets, new Map([[0, 3n]]))
})

void test("given_a_failed_poll_when_calling_next_then_should_yield_an_unpositioned_error_and_poll_again", async () => {
  let fail = true
  const transport = {
    pollMessages() {
      if (fail) {
        fail = false
        return Promise.reject(new Error("partition unavailable"))
      }
      return Promise.resolve([json(0n, { cpu: 1 })])
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const records = TypedRecords.create(
    Cursor.create(transport as unknown as LaserTransport, "test", "events", [0]),
    new Json<{ readonly cpu: number }>()
  )

  const failed = await records.next()
  assert.ok(failed?.kind === "error")
  assert.equal(failed.error.position, undefined)
  const recovered = await records.next()
  assert.ok(recovered?.kind === "record")
  assert.equal(recovered.record.value.cpu, 1)
})

void test("given_records_buffered_by_next_when_polling_then_should_return_them_before_reading_more", async () => {
  let polls = 0
  const transport = {
    pollMessages() {
      polls += 1
      return Promise.resolve(polls === 1 ? [json(0n, { cpu: 1 }), json(1n, { cpu: 2 })] : [])
    }
  } satisfies Pick<LaserTransport, "pollMessages">
  const records = TypedRecords.create(
    Cursor.create(transport as unknown as LaserTransport, "test", "events", [0]),
    new Json<{ readonly cpu: number }>()
  )

  await records.next()
  const rest = await records.poll()
  assert.equal(rest.length, 1)
  assert.ok(rest[0]?.kind === "record")
  assert.equal(rest[0].record.value.cpu, 2)
  assert.equal(polls, 1)
})

function message(partitionId: number, offset: bigint): PolledMessage {
  return { partitionId, offset, payload: new Uint8Array([1]), headers: new Map() }
}

function stamped(partitionId: number, offset: bigint, timestampMicros: bigint): PolledMessage {
  return { ...message(partitionId, offset), timestampMicros }
}

function json(offset: bigint, value: unknown): PolledMessage {
  return { ...message(0, offset), payload: new TextEncoder().encode(JSON.stringify(value)) }
}

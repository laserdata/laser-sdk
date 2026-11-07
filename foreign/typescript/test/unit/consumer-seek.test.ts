import assert from "node:assert/strict"
import { test } from "node:test"

import { InvalidError } from "../../src/client/errors.js"
import type { LaserTransport, PolledMessage } from "../../src/iggy/apache-iggy.js"
import type { FilteredReader } from "../../src/managed/filters.js"
import type { ConsumerStart } from "../../src/stream/consumer-start.js"
import { Consumer } from "../../src/stream/consumer.js"

void test("given_a_partition_consumer_holding_a_poll_when_it_seeks_then_should_drop_the_rest_and_read_from_the_sought_start", async () => {
  const starts: ConsumerStart[] = []
  const transport = {
    pollMessages: (_stream: string, _topic: string, _target: unknown, start: ConsumerStart) => {
      starts.push(start)
      return Promise.resolve(
        starts.length === 1 ? [polled(0n), polled(1n), polled(2n)] : [polled(1n), polled(2n)]
      )
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(
    transport,
    "stream",
    "topic",
    { kind: "single", name: "reader", partitionId: 0 },
    { commitPolicy: { kind: "disabled" }, startAt: { kind: "first" }, maxWaitMs: 0 }
  )
  for (const expected of [0n, 1n, 2n]) {
    assert.equal((await consumer.nextWithin(100)).position.offset, expected)
  }
  await consumer.seek(0, { kind: "offset", value: 1n })
  assert.equal(
    (await consumer.nextWithin(100)).position.offset,
    1n,
    "a record already consumed is delivered again after the seek"
  )
  assert.deepEqual(starts, [{ kind: "first" }, { kind: "offset", value: 1n }])
  await consumer.shutdown()
})

void test("given_a_partition_consumer_with_buffered_records_when_it_seeks_then_should_drop_them_and_count_nothing_before_the_seek", async () => {
  const stored: bigint[] = []
  const starts: ConsumerStart[] = []
  const transport = {
    pollMessages: (_stream: string, _topic: string, _target: unknown, start: ConsumerStart) => {
      starts.push(start)
      return Promise.resolve(starts.length === 1 ? [polled(0n), polled(1n), polled(2n)] : [])
    },
    storeOffset: (
      _stream: string,
      _topic: string,
      _target: unknown,
      _partition: number,
      offset: bigint
    ) => {
      stored.push(offset)
      return Promise.resolve()
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(
    transport,
    "stream",
    "topic",
    { kind: "single", name: "reader", partitionId: 0 },
    { commitPolicy: { kind: "all" }, maxWaitMs: 0 }
  )
  await consumer.nextWithin(100)
  await consumer.seek(0, { kind: "last" })
  await assert.rejects(consumer.nextWithin(5))
  assert.deepEqual(starts.at(-1), { kind: "last" })
  await consumer.shutdown()
  assert.deepEqual(stored, [], "the record held before the seek is not stored")
})

void test("given_a_native_group_consumer_when_one_partition_is_sought_then_should_keep_the_buffered_records_of_another", async () => {
  const reads: { partitionId: number | undefined; start: ConsumerStart }[] = []
  const transport = {
    syncConsumerGroup: () => Promise.resolve({ generation: 1n, partitions: [0, 1] }),
    leaveConsumerGroup: () => Promise.resolve(),
    pollMessages: (
      _stream: string,
      _topic: string,
      target: { readonly partitionId?: number },
      start: ConsumerStart
    ) => {
      reads.push({ partitionId: target.partitionId, start })
      return Promise.resolve(
        target.partitionId === 0 ? [polled(0n, 0), polled(1n, 0)] : [polled(7n, 1)]
      )
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(
    transport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    { commitPolicy: { kind: "disabled" }, maxWaitMs: 0 }
  )
  assert.equal((await consumer.nextWithin(100)).position.offset, 0n)
  await consumer.seek(1, { kind: "offset", value: 3n })
  assert.equal(
    (await consumer.nextWithin(100)).position.offset,
    1n,
    "the buffered record of the other partition stays"
  )
  await consumer.nextWithin(100)
  assert.deepEqual(reads.at(-1), { partitionId: 1, start: { kind: "offset", value: 3n } })
  await consumer.shutdown()
})

void test("given_a_poll_in_flight_when_its_partition_is_sought_then_should_discard_its_records", async () => {
  const starts: ConsumerStart[] = []
  let release: (records: readonly PolledMessage[]) => void = () => undefined
  const transport = {
    pollMessages: (_stream: string, _topic: string, _target: unknown, start: ConsumerStart) => {
      starts.push(start)
      if (starts.length === 1) {
        return new Promise<readonly PolledMessage[]>((resolve) => {
          release = resolve
        })
      }
      return Promise.resolve([polled(5n)])
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(
    transport,
    "stream",
    "topic",
    { kind: "single", name: "reader", partitionId: 0 },
    { commitPolicy: { kind: "disabled" }, maxWaitMs: 0 }
  )
  const next = consumer.nextWithin(1_000)
  await new Promise((resolve) => setImmediate(resolve))
  await consumer.seek(0, { kind: "offset", value: 5n })
  release([polled(0n), polled(1n)])
  assert.equal((await next).position.offset, 5n)
  assert.deepEqual(starts, [{ kind: "next" }, { kind: "offset", value: 5n }])
  await consumer.shutdown()
})

void test("given_the_polling_policy_when_seeking_past_offset_zero_then_should_store_the_offset_before_it_first", async () => {
  const stored: bigint[] = []
  const transport = {
    storeOffset: (
      _stream: string,
      _topic: string,
      _target: unknown,
      _partition: number,
      offset: bigint
    ) => {
      stored.push(offset)
      return Promise.resolve()
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(
    transport,
    "stream",
    "topic",
    { kind: "single", name: "reader", partitionId: 0 },
    { commitPolicy: { kind: "polling" } }
  )
  await consumer.seek(0, { kind: "offset", value: 5n })
  await consumer.seek(0, { kind: "offset", value: 0n })
  await consumer.seek(0, { kind: "first" })
  assert.deepEqual(stored, [4n])
})

void test("given_a_consumer_that_cannot_seek_when_it_seeks_then_should_refuse", async () => {
  const reader = {
    wake: () => undefined,
    close: () => Promise.resolve()
  } as unknown as FilteredReader
  const grouped = Consumer.create(
    {} as LaserTransport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    { commitPolicy: { kind: "disabled" } },
    reader
  )
  await assert.rejects(grouped.seek(0, { kind: "first" }), InvalidError)
  const single = Consumer.create(
    {} as LaserTransport,
    "stream",
    "topic",
    { kind: "single", name: "reader", partitionId: 0 },
    { commitPolicy: { kind: "disabled" } }
  )
  await assert.rejects(single.seek(1, { kind: "first" }), InvalidError, "another partition")
  await assert.rejects(single.seek(0, { kind: "offset", value: -1n }), InvalidError)
  await single.shutdown()
  await assert.rejects(single.seek(0, { kind: "first" }), InvalidError, "shut down")
})

function polled(offset: bigint, partitionId = 0): PolledMessage {
  return { payload: new Uint8Array(), partitionId, offset, headers: new Map() }
}

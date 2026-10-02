import assert from "node:assert/strict"
import { test } from "node:test"

import { CancelledError } from "../../src/client/errors.js"
import type { LaserTransport } from "../../src/iggy/apache-iggy.js"
import type { FilteredReader, MatchedPage, MatchedRecord } from "../../src/managed/filters.js"
import { Consumer } from "../../src/stream/consumer.js"

function matched(offset: bigint): MatchedRecord {
  return {
    partitionId: 0,
    offset,
    frontier: 10n,
    evaluated: false,
    payload: new Uint8Array([Number(offset)]),
    headers: new Map(),
    json: () => Number(offset)
  }
}

function matchedPage(records: readonly MatchedRecord[]): MatchedPage {
  return {
    partitionId: 0,
    records,
    policy: { groupId: 7n, mode: "unfiltered", policyGeneration: 0n },
    generation: {
      streamId: 1,
      streamCreatedAtMicros: 1n,
      topicId: 1,
      topicCreatedAtMicros: 1n,
      partitionId: 0,
      partitionCreatedRevision: 1n,
      purgeGeneration: 0n
    },
    stop: "end_of_visible",
    examined: records.length,
    frontier: 10n
  }
}

void test("given_a_group_backlog_without_matches_when_next_times_out_then_should_yield_between_bounded_rounds", async () => {
  let rounds = 0
  let yielded = false
  const timer = setTimeout(() => {
    yielded = true
  }, 1)
  const reader = {
    owns: () => true,
    readRound: () => {
      rounds += 1
      return Promise.resolve([undefined, true] as const)
    }
  } as unknown as FilteredReader
  const consumer = new Consumer(
    {} as LaserTransport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    { autoCommit: false, pollIntervalMs: 60_000 },
    reader
  )
  try {
    assert.equal(await consumer.nextWithin(20), null)
    assert.equal(yielded, true, "busy scans let timers and cancellation run")
    assert.ok(rounds > 1, "busy scans do not use the idle wait")
  } finally {
    clearTimeout(timer)
  }
})

void test("given_a_group_poll_when_aborted_before_delivery_then_should_keep_the_record_for_the_next_call", async () => {
  const controller = new AbortController()
  const handled: bigint[] = []
  const reader = {
    owns: () => true,
    readRound: () => {
      controller.abort()
      return Promise.resolve([matchedPage([matched(0n)]), false] as const)
    },
    handled: (record: MatchedRecord) => handled.push(record.offset),
    close: () => Promise.resolve()
  } as unknown as FilteredReader
  const consumer = new Consumer(
    {} as LaserTransport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    {},
    reader
  )
  await assert.rejects(consumer.nextWithin(100, { signal: controller.signal }), CancelledError)
  assert.deepEqual(handled, [])
  assert.equal((await consumer.nextWithin(100))?.offset, 0n)
  assert.deepEqual(handled, [], "delivery stays pending until the next call or shutdown")
  await consumer.shutdown()
  assert.deepEqual(handled, [0n])
})

void test("given_an_automatic_group_consumer_when_shutdown_with_buffered_records_then_should_store_only_delivered_records", async () => {
  const handled: bigint[] = []
  const reader = {
    owns: () => true,
    readRound: () =>
      Promise.resolve([matchedPage([matched(0n), matched(1n), matched(2n)]), false] as const),
    handled: (record: MatchedRecord) => handled.push(record.offset),
    close: () => Promise.resolve()
  } as unknown as FilteredReader
  const consumer = new Consumer(
    {} as LaserTransport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    {},
    reader
  )
  assert.equal((await consumer.nextWithin(100))?.offset, 0n)
  assert.deepEqual(handled, [])
  assert.equal((await consumer.nextWithin(100))?.offset, 1n)
  assert.deepEqual(handled, [0n])
  await consumer.shutdown()
  assert.deepEqual(handled, [0n, 1n], "offset two was never delivered")
  assert.equal(await consumer.nextWithin(100), null)
})

void test("given_a_group_consumer_when_asynchronously_disposed_then_should_leave_once", async () => {
  let leaves = 0
  const transport = {
    leaveConsumerGroup(): Promise<void> {
      leaves += 1
      return Promise.resolve()
    }
  } as unknown as LaserTransport
  const consumer = new Consumer(
    transport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    {}
  )

  await consumer[Symbol.asyncDispose]()
  await consumer[Symbol.asyncDispose]()

  assert.equal(leaves, 1)
})

void test("given_an_explicit_start_when_polling_multiple_batches_then_should_advance_local_offsets", async () => {
  const starts: unknown[] = []
  let offset = 4n
  const transport = {
    pollMessages(_stream: string, _topic: string, _target: unknown, start: unknown) {
      starts.push(start)
      return Promise.resolve([
        { payload: new Uint8Array(), partitionId: 0, offset: offset++, headers: new Map() }
      ])
    }
  } as unknown as LaserTransport
  const consumer = new Consumer(
    transport,
    "stream",
    "topic",
    { kind: "single", name: "reader", partitionId: 0 },
    { startFrom: { kind: "offset", value: 4n }, autoCommit: false }
  )
  assert.equal((await consumer.nextWithin(100))?.offset, 4n)
  assert.equal((await consumer.nextWithin(100))?.offset, 5n)
  assert.deepEqual(starts, [
    { kind: "offset", value: 4n },
    { kind: "offset", value: 5n }
  ])
})

void test("given_anonymous_consumers_when_polling_then_should_use_distinct_uncommitted_identities", async () => {
  const seen: { target: unknown; autoCommit: boolean }[] = []
  const transport = {
    pollMessages(
      _stream: string,
      _topic: string,
      target: unknown,
      _start: unknown,
      _count: number,
      autoCommit: boolean
    ) {
      seen.push({ target, autoCommit })
      return Promise.resolve([
        { payload: new Uint8Array(), partitionId: 0, offset: 0n, headers: new Map() }
      ])
    }
  } as unknown as LaserTransport
  for (let index = 0; index < 2; index += 1) {
    const consumer = new Consumer(transport, "stream", "topic", { kind: "single", partitionId: 0 })
    assert.equal((await consumer.nextWithin(100))?.offset, 0n)
  }
  assert.notDeepEqual(seen[0]?.target, seen[1]?.target)
  assert.equal(
    seen.every((item) => !item.autoCommit),
    true
  )
})

void test("given_group_replay_when_reading_several_partitions_then_should_keep_independent_offsets", async () => {
  const starts: unknown[] = []
  const transport = {
    syncConsumerGroup: () => Promise.resolve({ generation: 1n, partitions: [0, 1] }),
    pollMessages(
      _stream: string,
      _topic: string,
      target: { partitionId: number },
      start: { kind: string; value?: bigint }
    ) {
      starts.push([target.partitionId, start])
      return Promise.resolve([
        {
          payload: new Uint8Array(),
          partitionId: target.partitionId,
          offset: start.value ?? 0n,
          headers: new Map()
        }
      ])
    }
  } as unknown as LaserTransport
  const consumer = new Consumer(
    transport,
    "stream",
    "topic",
    { kind: "group", name: "replay" },
    { startFrom: { kind: "offset", value: 4n }, autoCommit: false }
  )
  for (let index = 0; index < 4; index += 1) await consumer.nextWithin(100)
  assert.deepEqual(starts, [
    [0, { kind: "offset", value: 4n }],
    [1, { kind: "offset", value: 4n }],
    [0, { kind: "offset", value: 5n }],
    [1, { kind: "offset", value: 5n }]
  ])
})

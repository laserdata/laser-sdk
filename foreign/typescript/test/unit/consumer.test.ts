import assert from "node:assert/strict"
import { test } from "node:test"

import { CancelledError, InvalidError, TimeoutError } from "../../src/client/errors.js"
import type { LaserTransport } from "../../src/iggy/apache-iggy.js"
import type { FilteredReader, MatchedPage, MatchedRecord } from "../../src/managed/filters.js"
import { Consumer } from "../../src/stream/consumer.js"
import { withMessageJson } from "../../src/stream/message.js"

function matched(offset: bigint): MatchedRecord {
  return {
    partitionId: 0,
    offset,
    frontier: 10n,
    evaluated: false,
    message: withMessageJson({
      payload: new Uint8Array([Number(offset)]),
      id: { partitionId: 0, offset },
      partitionId: 0,
      headers: new Map(),
      position: { partitionId: 0, offset },
      messageId: 0n,
      checksum: 0n,
      currentOffset: 10n,
      timestampMicros: 0n,
      originTimestampMicros: 0n,
      headersMalformed: false
    }),
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

void test("given_a_group_backlog_without_matches_when_next_times_out_then_should_yield_between_bounded_rounds", async (context) => {
  let rounds = 0
  context.mock.method(Date, "now", () => (rounds < 3 ? 0 : 20))
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
  const consumer = Consumer.create(
    {} as LaserTransport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    { commitPolicy: { kind: "disabled" }, pollIntervalMs: 60_000 },
    reader
  )
  try {
    await assert.rejects(consumer.nextWithin(20), TimeoutError)
    assert.equal(yielded, true, "busy scans let timers and cancellation run")
    assert.ok(rounds > 1, "busy scans do not use the idle wait")
  } finally {
    clearTimeout(timer)
  }
})

void test("given_a_stalled_group_poll_when_next_times_out_then_should_keep_one_poll_and_deliver_its_late_record", async () => {
  let finish: ((value: readonly [MatchedPage | undefined, boolean]) => void) | undefined
  let rounds = 0
  const pending = new Promise<readonly [MatchedPage | undefined, boolean]>((resolve) => {
    finish = resolve
  })
  const reader = {
    owns: () => true,
    readRound: () => {
      rounds += 1
      return pending
    },
    close: () => Promise.resolve()
  } as unknown as FilteredReader
  const consumer = Consumer.create(
    {} as LaserTransport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    { commitPolicy: { kind: "disabled" } },
    reader
  )
  await assert.rejects(consumer.nextWithin(5), TimeoutError)
  finish?.([matchedPage([matched(0n)]), false])
  assert.equal((await consumer.nextWithin(100)).position.offset, 0n)
  assert.equal(rounds, 1, "a timed-out read resumes the same poll")
  await consumer.shutdown()
})

void test(
  "given_a_large_poll_timeout_when_cancelled_then_should_not_expire_at_the_node_timer_ceiling",
  { timeout: 1000 },
  async () => {
    let finish: ((value: readonly [MatchedPage | undefined, boolean]) => void) | undefined
    const pending = new Promise<readonly [MatchedPage | undefined, boolean]>((resolve) => {
      finish = resolve
    })
    const reader = {
      owns: () => true,
      readRound: () => pending,
      close: () => Promise.resolve()
    } as unknown as FilteredReader
    const consumer = Consumer.create(
      {} as LaserTransport,
      "stream",
      "topic",
      { kind: "group", name: "workers" },
      { commitPolicy: { kind: "disabled" } },
      reader
    )
    const controller = new AbortController()
    const waiting = consumer.nextWithin(2_147_483_648, { signal: controller.signal })
    const cancelled = setTimeout(() => {
      controller.abort()
    }, 10)
    try {
      await assert.rejects(waiting, CancelledError)
    } finally {
      clearTimeout(cancelled)
      finish?.([undefined, false])
      await consumer.shutdown()
    }
  }
)

void test("given_a_stalled_group_poll_when_aborted_then_should_resume_its_late_record", async () => {
  let finish: ((value: readonly [MatchedPage | undefined, boolean]) => void) | undefined
  const pending = new Promise<readonly [MatchedPage | undefined, boolean]>((resolve) => {
    finish = resolve
  })
  const reader = {
    owns: () => true,
    readRound: () => pending,
    close: () => Promise.resolve()
  } as unknown as FilteredReader
  const consumer = Consumer.create(
    {} as LaserTransport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    { commitPolicy: { kind: "disabled" } },
    reader
  )
  const controller = new AbortController()
  const waiting = consumer.nextWithin(100, { signal: controller.signal })
  controller.abort()
  await assert.rejects(waiting, CancelledError)
  finish?.([matchedPage([matched(0n)]), false])
  assert.equal((await consumer.nextWithin(100)).position.offset, 0n)
  await consumer.shutdown()
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
  const consumer = Consumer.create(
    {} as LaserTransport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    {},
    reader
  )
  await assert.rejects(consumer.nextWithin(100, { signal: controller.signal }), CancelledError)
  assert.deepEqual(handled, [])
  assert.equal((await consumer.nextWithin(100)).position.offset, 0n)
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
  const consumer = Consumer.create(
    {} as LaserTransport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    {},
    reader
  )
  assert.equal((await consumer.nextWithin(100)).position.offset, 0n)
  assert.deepEqual(handled, [])
  assert.equal((await consumer.nextWithin(100)).position.offset, 1n)
  assert.deepEqual(handled, [0n])
  await consumer.shutdown()
  assert.deepEqual(handled, [0n, 1n], "offset two was never delivered")
  await assert.rejects(consumer.nextWithin(100), InvalidError)
})

void test("given_a_group_consumer_when_asynchronously_disposed_then_should_leave_once", async () => {
  let leaves = 0
  const transport = {
    leaveConsumerGroup(): Promise<void> {
      leaves += 1
      return Promise.resolve()
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(
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
  const consumer = Consumer.create(
    transport,
    "stream",
    "topic",
    { kind: "single", name: "reader", partitionId: 0 },
    { startAt: { kind: "offset", value: 4n }, commitPolicy: { kind: "disabled" } }
  )
  assert.equal((await consumer.nextWithin(100)).position.offset, 4n)
  assert.equal((await consumer.nextWithin(100)).position.offset, 5n)
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
    const consumer = Consumer.create(transport, "stream", "topic", {
      kind: "single",
      partitionId: 0
    })
    assert.equal((await consumer.nextWithin(100)).position.offset, 0n)
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
  const consumer = Consumer.create(
    transport,
    "stream",
    "topic",
    { kind: "group", name: "replay" },
    { startAt: { kind: "offset", value: 4n }, commitPolicy: { kind: "disabled" } }
  )
  for (let index = 0; index < 4; index += 1) await consumer.nextWithin(100)
  assert.deepEqual(starts, [
    [0, { kind: "offset", value: 4n }],
    [1, { kind: "offset", value: 4n }],
    [0, { kind: "offset", value: 5n }],
    [1, { kind: "offset", value: 5n }]
  ])
})

void test("given_group_commit_policies_when_records_are_delivered_then_should_store_the_prefix_at_their_points", async () => {
  const cases = [
    { policy: { kind: "each" }, flushes: [1, 2, 3] },
    { policy: { kind: "every", count: 2 }, flushes: [0, 1, 1] },
    { policy: { kind: "all" }, flushes: [0, 0, 1] },
    { policy: { kind: "polling" }, flushes: [0, 0, 1] }
  ] as const
  for (const { policy, flushes } of cases) {
    let stored = 0
    let served = false
    const reader = {
      owns: () => true,
      handled: () => undefined,
      flushCompleted: () => {
        stored += 1
        return Promise.resolve()
      },
      readRound: () => {
        if (served) return Promise.resolve([undefined, false] as const)
        served = true
        return Promise.resolve([
          matchedPage([matched(0n), matched(1n), matched(2n)]),
          false
        ] as const)
      },
      close: () => Promise.resolve()
    } as unknown as FilteredReader
    const consumer = Consumer.create(
      {} as LaserTransport,
      "stream",
      "topic",
      { kind: "group", name: "workers" },
      { commitPolicy: policy },
      reader
    )
    const seen: number[] = []
    await consumer.nextWithin(100)
    for (let read = 0; read < 3; read += 1) {
      if (read < 2) await consumer.nextWithin(100)
      else await assert.rejects(consumer.nextWithin(5), TimeoutError)
      seen.push(stored)
    }
    assert.deepEqual(seen, flushes, policy.kind)
    await consumer.shutdown()
  }
})

void test("given_a_native_group_consumer_that_did_not_join_when_shut_down_then_should_not_leave_the_group", async () => {
  let left = 0
  const transport = {
    leaveConsumerGroup: () => {
      left += 1
      return Promise.resolve()
    }
  } as unknown as LaserTransport
  const joined = Consumer.create(transport, "s", "t", { kind: "group", name: "workers" })
  const detached = Consumer.create(
    transport,
    "s",
    "t",
    { kind: "group", name: "workers" },
    { autoJoinGroup: false }
  )
  await detached.shutdown()
  assert.equal(left, 0)
  await joined.shutdown()
  assert.equal(left, 1)
})

void test("given_no_poll_interval_when_a_native_poll_is_empty_then_should_poll_again_without_waiting", async () => {
  let polls = 0
  const transport = {
    pollMessages: () => {
      polls += 1
      return Promise.resolve([])
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(transport, "s", "t", {
    kind: "single",
    partitionId: 0,
    name: "metrics"
  })
  await assert.rejects(consumer.nextWithin(50), TimeoutError)
  assert.ok(polls > 5, `polled ${String(polls)} times in 50 ms`)
  await consumer.shutdown()
})

void test("given_a_zero_polling_retry_interval_when_built_then_should_reject_it", () => {
  assert.throws(
    () =>
      Consumer.create(
        {} as LaserTransport,
        "s",
        "t",
        { kind: "single", partitionId: 0 },
        {
          pollingRetryIntervalMs: 0
        }
      ),
    InvalidError
  )
})

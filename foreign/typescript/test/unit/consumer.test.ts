import assert from "node:assert/strict"
import { test } from "node:test"

import {
  CancelledError,
  InvalidError,
  TimeoutError,
  TransportError
} from "../../src/client/errors.js"
import type { LaserTransport, PollWait } from "../../src/iggy/apache-iggy.js"
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
      partitionIncarnation: 1n
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
    wake: () => undefined,
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
    { commitPolicy: { kind: "disabled" } },
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
    wake: () => undefined,
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
      wake: () => undefined,
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
    wake: () => undefined,
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
    wake: () => undefined,
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
    wake: () => undefined,
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

void test("given_a_group_started_at_first_when_partitions_are_assigned_at_join_and_on_a_rebalance_then_should_read_each_after_the_stored_offset", async () => {
  const starts: unknown[] = []
  let syncs = 0
  const transport = {
    syncConsumerGroup: () => {
      syncs += 1
      return Promise.resolve({ generation: 1n, partitions: syncs <= 2 ? [0, 1] : [0, 1, 2] })
    },
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
          offset: start.value ?? 4n,
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
    { startAt: { kind: "first" }, commitPolicy: { kind: "disabled" } }
  )
  for (let index = 0; index < 4; index += 1) await consumer.nextWithin(100)
  assert.deepEqual(starts, [
    [0, { kind: "next" }],
    [1, { kind: "next" }],
    [2, { kind: "next" }],
    [0, { kind: "offset", value: 5n }]
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
      wake: () => undefined,
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
      waitForRecordsWithin: () => new Promise((resolve) => setTimeout(resolve, 1)),
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

void test("given_a_native_group_consumer_when_shut_down_then_should_leave_the_group_it_joined", async () => {
  let left = 0
  const transport = {
    leaveConsumerGroup: () => {
      left += 1
      return Promise.resolve()
    }
  } as unknown as LaserTransport
  await Consumer.create(transport, "s", "t", { kind: "group", name: "workers" }).shutdown()
  assert.equal(left, 1)
})

void test("given_an_empty_native_poll_when_polled_again_then_should_leave_the_wait_to_the_server", async () => {
  let polls = 0
  const transport = {
    pollMessages: () => {
      polls += 1
      return Promise.resolve([])
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(
    transport,
    "s",
    "t",
    { kind: "single", partitionId: 0, name: "metrics" },
    { maxWaitMs: 0 }
  )
  await assert.rejects(consumer.nextWithin(50), TimeoutError)
  assert.ok(polls > 5, `polled ${String(polls)} times in 50 ms`)
  await consumer.shutdown()
})

void test("given_out_of_range_wait_options_when_a_consumer_is_built_then_should_refuse_them", () => {
  const cases: readonly [Parameters<typeof Consumer.create>[4], string][] = [
    [{ maxWaitMs: 600_001 }, "consumer maxWaitMs must be 0..=600000"],
    [{ maxWaitMs: -1 }, "consumer maxWaitMs must be 0..=600000"],
    [{ minCount: 0 }, "consumer minCount must be 1..=batchLength"],
    [{ batchLength: 10, minCount: 11 }, "consumer minCount must be 1..=batchLength"],
    [{ maxBytes: 343 }, "consumer maxBytes must be at least 344"]
  ]
  for (const [options, message] of cases) {
    assert.throws(
      () =>
        Consumer.create(
          {} as LaserTransport,
          "s",
          "t",
          { kind: "single", partitionId: 0 },
          options
        ),
      new InvalidError(message)
    )
  }
  Consumer.create(
    {} as LaserTransport,
    "s",
    "t",
    { kind: "single", partitionId: 0 },
    { maxWaitMs: 600_000, batchLength: 10, minCount: 10, maxBytes: 344 }
  )
})

void test("given_a_single_partition_consumer_when_polling_then_should_hold_each_read_for_the_whole_wait", async () => {
  const waits: (PollWait | undefined)[] = []
  const transport = {
    pollMessages(
      _stream: string,
      _topic: string,
      _target: unknown,
      _start: unknown,
      _count: number,
      _commit: boolean,
      wait?: PollWait
    ) {
      waits.push(wait)
      return Promise.resolve([])
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(transport, "s", "t", {
    kind: "single",
    partitionId: 0,
    name: "r"
  })
  await assert.rejects(consumer.nextWithin(5), TimeoutError)
  await consumer.shutdown()
  assert.deepEqual(waits[0], { maxWaitMs: 5_000, minCount: 1, maxBytes: 1_048_576 })
})

void test("given_a_group_consumer_over_three_partitions_when_a_pass_is_idle_then_should_hold_each_read_for_a_third_of_the_wait", async () => {
  const waits: number[] = []
  let offset = 0n
  let deliverAt = 6
  const transport = {
    syncConsumerGroup: () => Promise.resolve({ generation: 1n, partitions: [0, 1, 2] }),
    leaveConsumerGroup: () => Promise.resolve(),
    storeOffset: () => Promise.resolve(),
    pollMessages(
      _stream: string,
      _topic: string,
      target: { readonly partitionId?: number },
      _start: unknown,
      _count: number,
      _commit: boolean,
      wait?: PollWait
    ) {
      waits.push(wait?.maxWaitMs ?? -1)
      deliverAt -= 1
      return Promise.resolve(
        deliverAt === 0
          ? [
              {
                payload: new Uint8Array(),
                partitionId: target.partitionId ?? 0,
                offset: offset++,
                headers: new Map()
              }
            ]
          : []
      )
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(
    transport,
    "s",
    "t",
    { kind: "group", name: "workers" },
    { startAt: { kind: "first" }, maxWaitMs: 3_000 }
  )
  await consumer.nextWithin(1_000)
  deliverAt = Number.MAX_SAFE_INTEGER
  await assert.rejects(consumer.nextWithin(5), TimeoutError)
  await consumer.shutdown()
  assert.deepEqual(waits.slice(0, 6), [0, 0, 0, 1_000, 1_000, 1_000])
  assert.equal(waits[6], 0, "a delivered record starts a pass that answers at once")
})

void test("given_a_policy_aware_group_consumer_when_a_round_is_idle_then_should_wait_through_the_reader", async () => {
  let idles = 0
  let rounds = 0
  const reader = {
    owns: () => true,
    wake: () => undefined,
    readRound: () => {
      rounds += 1
      return Promise.resolve(
        rounds === 3
          ? ([matchedPage([matched(0n)]), false] as const)
          : ([undefined, false] as const)
      )
    },
    waitForRecordsWithin: () => {
      idles += 1
      return Promise.resolve()
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
  assert.equal((await consumer.nextWithin(1_000)).position.offset, 0n)
  assert.equal(idles, 2, "each idle round waits once through the reader")
  await consumer.shutdown()
})

void test("given_a_held_native_poll_when_the_consumer_shuts_down_then_should_cancel_it", async () => {
  let cancelled = false
  const transport = {
    pollMessages(
      _stream: string,
      _topic: string,
      _target: unknown,
      _start: unknown,
      _count: number,
      _commit: boolean,
      _wait?: PollWait,
      signal?: AbortSignal
    ) {
      return new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => {
          cancelled = true
          reject(new Error("cancelled"))
        })
      })
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(transport, "s", "t", {
    kind: "single",
    partitionId: 0,
    name: "r"
  })
  await assert.rejects(consumer.nextWithin(5), TimeoutError)
  await consumer.shutdown()
  assert.equal(cancelled, true, "shutdown cancels the read the server holds")
})

void test("given_a_lasting_refusal_when_polling_then_should_report_it_once_per_six_refused_polls_in_a_row", async () => {
  let polls = 0
  let refusing = true
  const transport = {
    pollMessages() {
      polls += 1
      if (!refusing)
        return Promise.resolve([
          { payload: new Uint8Array(), partitionId: 0, offset: 0n, headers: new Map() }
        ])
      return Promise.reject(
        new TransportError("poll refused", false, {
          cause: Object.assign(new Error("topic not found"), { errorCode: 2010 })
        })
      )
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(
    transport,
    "s",
    "t",
    { kind: "single", partitionId: 0, name: "r" },
    { commitPolicy: { kind: "disabled" }, pollingRetryIntervalMs: 5, allowReplay: true }
  )
  const pollsAtReports: number[] = []
  const readUntilReported = async (reports: number): Promise<void> => {
    while (pollsAtReports.length < reports && polls < 40) {
      try {
        await consumer.nextWithin(50)
      } catch (error) {
        if (!(error instanceof TimeoutError)) pollsAtReports.push(polls)
      }
    }
  }
  await readUntilReported(2)
  assert.deepEqual(pollsAtReports, [6, 12], "the sixth refusal in a row is reported, then six more")
  refusing = false
  assert.equal((await consumer.nextWithin(500)).position.offset, 0n)
  refusing = true
  const recovered = polls
  await readUntilReported(3)
  assert.equal(pollsAtReports[2], recovered + 6, "a delivered poll starts the count again")
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

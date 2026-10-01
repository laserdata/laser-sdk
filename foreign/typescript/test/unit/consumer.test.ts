import assert from "node:assert/strict"
import { test } from "node:test"

import type { LaserTransport } from "../../src/iggy/apache-iggy.js"
import { Consumer } from "../../src/stream/consumer.js"

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

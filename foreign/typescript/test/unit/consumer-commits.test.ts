import assert from "node:assert/strict"
import { test } from "node:test"

import { TimeoutError } from "../../src/client/errors.js"
import type { LaserTransport, PolledMessage } from "../../src/iggy/apache-iggy.js"
import { Consumer, resolveConsumerOptions } from "../../src/stream/consumer.js"

void test("given_a_named_consumer_without_a_commit_policy_when_resolved_then_should_store_once_the_last_record_of_a_poll_is_handled", async () => {
  assert.deepEqual(resolveConsumerOptions({}, false).commitPolicy, { kind: "all" })
  assert.deepEqual(resolveConsumerOptions({}, true).commitPolicy, { kind: "disabled" })
  const committingReads: boolean[] = []
  const stored: bigint[] = []
  let served = false
  const transport = {
    pollMessages: (
      _stream: string,
      _topic: string,
      _target: unknown,
      _start: unknown,
      _count: number,
      autoCommit: boolean
    ) => {
      committingReads.push(autoCommit)
      if (served) return Promise.resolve([])
      served = true
      return Promise.resolve([polled(0n), polled(1n)])
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
    { maxWaitMs: 0 }
  )
  await consumer.nextWithin(100)
  await consumer.nextWithin(100)
  assert.deepEqual(stored, [], "the record in hand and the poll it came from are not stored yet")
  await assert.rejects(consumer.nextWithin(5), TimeoutError)
  assert.deepEqual(stored, [1n], "the poll is stored once its last record is handled")
  assert.equal(
    committingReads.some((committing) => committing),
    false,
    "no read stores on the server before delivery"
  )
  await consumer.shutdown()
})

void test("given_a_native_delivery_when_committed_or_stored_automatically_then_should_pass_its_partition_context", async () => {
  for (const policy of [{ kind: "disabled" }, { kind: "each" }] as const) {
    const stores: { offset: bigint; context: unknown }[] = []
    const consumer = Consumer.create(
      contextTransport(stores),
      "stream",
      "topic",
      { kind: "single", name: "reader", partitionId: 0 },
      { commitPolicy: policy, maxWaitMs: 0 }
    )
    const zero = await consumer.nextWithin(100)
    if (policy.kind === "disabled") await consumer.commit(zero)
    await consumer.nextWithin(100)
    await consumer.shutdown()
    assert.deepEqual(
      stores,
      policy.kind === "disabled"
        ? [{ offset: 0n, context: CONTEXT }]
        : [
            { offset: 0n, context: CONTEXT },
            { offset: 1n, context: CONTEXT }
          ],
      policy.kind
    )
  }
})

void test("given_an_automatic_store_refused_for_an_older_partition_history_when_stored_then_should_drop_it_and_read_on", async () => {
  for (const code of [87, 5009]) {
    let attempts = 0
    const transport = {
      pollMessages: (() => {
        let served = false
        return () => {
          if (served) return Promise.resolve([])
          served = true
          return Promise.resolve([polled(0n), polled(1n)])
        }
      })(),
      storeOffset: () => {
        attempts += 1
        return Promise.reject(Object.assign(new Error("refused"), { errorCode: code }))
      }
    } as unknown as LaserTransport
    const consumer = Consumer.create(
      transport,
      "stream",
      "topic",
      { kind: "single", name: "reader", partitionId: 0 },
      { commitPolicy: { kind: "each" }, maxWaitMs: 0 }
    )
    await consumer.nextWithin(100)
    await consumer.nextWithin(100)
    assert.equal(attempts, 1, `${String(code)}: the handled record was stored once`)
    await consumer.shutdown()
    assert.equal(attempts, 2, `${String(code)}: shutdown stores only the record in hand`)
  }
})

void test("given_an_explicit_commit_refused_for_an_older_partition_history_when_committed_then_should_reject", async () => {
  const transport = {
    pollMessages: () => Promise.resolve([polled(0n)]),
    storeOffset: () => Promise.reject(Object.assign(new Error("refused"), { errorCode: 87 }))
  } as unknown as LaserTransport
  const consumer = Consumer.create(
    transport,
    "stream",
    "topic",
    { kind: "single", name: "reader", partitionId: 0 },
    { commitPolicy: { kind: "disabled" }, maxWaitMs: 0 }
  )
  const message = await consumer.nextWithin(100)
  await assert.rejects(consumer.commit(message), /refused/)
  await consumer.shutdown()
})

const CONTEXT = { incarnation: 3n, ownerGeneration: 5n, metadataOp: 8n }

function polled(offset: bigint, partitionId = 0): PolledMessage {
  return { payload: new Uint8Array(), partitionId, offset, headers: new Map() }
}

function contextTransport(stores: { offset: bigint; context: unknown }[]): LaserTransport {
  let served = false
  return {
    pollMessages: () => {
      if (served) return Promise.resolve([])
      served = true
      return Promise.resolve(
        [0n, 1n].map((offset) =>
          Object.defineProperty(polled(offset), "context", { value: CONTEXT })
        )
      )
    },
    storeOffset: (
      _stream: string,
      _topic: string,
      _target: unknown,
      _partition: number,
      offset: bigint,
      context?: unknown
    ) => {
      stores.push({ offset, context })
      return Promise.resolve()
    }
  } as unknown as LaserTransport
}

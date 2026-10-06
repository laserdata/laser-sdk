import { INTERNAL_RETURN_DELIVERY } from "../../src/client/internals.js"
import assert from "node:assert/strict"
import { test } from "node:test"

import { decodeUtf8, utf8 } from "../../src/client/bytes.js"
import {
  code,
  filterReason,
  iggyErrorCode,
  isAmbiguousMutation,
  isBudgetExceeded,
  isFenceViolation,
  isLeaseLost,
  isNoCapableAgent,
  isNotFound,
  isNotLeader,
  isPermissionDenied,
  isQuarantined,
  isStale,
  isStreamOrTopicNotFound,
  isUnavailable,
  isUnsupported,
  isVersionConflict,
  isVersionSkew
} from "../../src/client/error-classify.js"
import {
  AmbiguousMutationError,
  BudgetExceededError,
  CheckpointExecutionError,
  FenceViolationError,
  FilterExecutionError,
  ForkExecutionError,
  InvalidError,
  KvExecutionError,
  NoCapableAgentError,
  NoRespondTopicError,
  PublishFailedError,
  QuarantinedError,
  QueryExecutionError,
  RoutePrincipalMismatchError,
  TimeoutError,
  TransportError,
  UnsupportedError,
  publishCause
} from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { isRetryable } from "../../src/agent/reliable-consumer.js"
import { CONTEXT_READ_WINDOW, Checkpoint, ContextAssembler, LastN } from "../../src/context.js"
import { ScopedMemory } from "../../src/context-scope.js"
import type { LaserTransport, PolledMessage } from "../../src/iggy/apache-iggy.js"
import { Destinations } from "../../src/managed/destinations.js"
import { encodeCheckpointReplyFrame } from "../../src/wire/checkpoint.js"
import { DestinationId } from "../../src/wire/ids.js"
import { MemoryHandle } from "../../src/memory/handle.js"
import { type Memory, MemoryId, type MemoryItem, type MemoryScope } from "../../src/memory/types.js"
import { encodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import { BatchingProducerBuilder } from "../../src/stream/batching.js"
import { Consumer } from "../../src/stream/consumer.js"
import { Cursor } from "../../src/stream/cursor.js"
import { Topic } from "../../src/stream/topic.js"
import { ConversationId } from "../../src/types/ids.js"
import { managedCapabilitiesFrom } from "../../src/client/capabilities.js"

function iggyFailure(errorCode: number): TransportError {
  return new TransportError("rejected", false, { cause: { errorCode } })
}

void test("given_each_rust_classifier_when_applied_to_its_failure_then_should_answer_true", () => {
  assert.equal(isPermissionDenied(iggyFailure(41)), true)
  assert.equal(isPermissionDenied(iggyFailure(40)), true)
  assert.equal(isPermissionDenied(new RoutePrincipalMismatchError("metrics", 1)), true)
  assert.equal(
    isPermissionDenied(new CheckpointExecutionError("denied", { kind: "unauthorized" })),
    true
  )
  assert.equal(isStreamOrTopicNotFound(iggyFailure(1009)), true)
  assert.equal(isNotFound(iggyFailure(2011)), true)
  assert.equal(isNotFound(iggyFailure(20)), true)
  assert.equal(isNotFound(new QueryExecutionError("gone", { kind: "index_not_found" })), true)
  assert.equal(isUnsupported(new UnsupportedError("no plane")), true)
  assert.equal(isUnsupported(new ForkExecutionError("no", { kind: "unsupported" })), true)
  assert.equal(isVersionSkew(new KvExecutionError("skew", { kind: "version" })), true)
  assert.equal(isVersionConflict(new KvExecutionError("race", { kind: "versionConflict" })), true)
  assert.equal(isStale(new QueryExecutionError("behind", { kind: "stale" })), true)
  assert.equal(isNotLeader(new KvExecutionError("moved", { kind: "notLeader" })), true)
  assert.equal(isLeaseLost(new KvExecutionError("lost", { kind: "leaseLost" })), true)
  assert.equal(isAmbiguousMutation(new AmbiguousMutationError("unknown")), true)
  assert.equal(isNoCapableAgent(new NoCapableAgentError("rollback")), true)
  assert.equal(isFenceViolation(new FenceViolationError(3n, 4n)), true)
  assert.equal(isBudgetExceeded(new BudgetExceededError(10n, 11n)), true)
  assert.equal(isQuarantined(new QuarantinedError("metrics")), true)
  assert.equal(isUnavailable(new TransportError("socket closed", true)), true)
  assert.equal(iggyErrorCode(iggyFailure(41)), 41)
})

void test("given_a_failed_publish_when_classified_then_should_answer_for_its_cause", () => {
  const failed = new PublishFailedError("fleet", "readings", [], [], iggyFailure(41))
  assert.equal(publishCause(failed) instanceof TransportError, true)
  assert.equal(failed.publishCause() instanceof TransportError, true)
  assert.equal(isPermissionDenied(failed), true)
  assert.deepEqual(code(failed), { kind: "known", name: "Forbidden" })
  assert.equal(
    isRetryable(new PublishFailedError("fleet", "readings", [], [], new TransportError("x", true))),
    true
  )
})

void test("given_failures_when_coded_then_should_match_the_rust_result_codes", () => {
  assert.deepEqual(code(new UnsupportedError("x")), { kind: "known", name: "Unsupported" })
  assert.deepEqual(code(new NoRespondTopicError()), { kind: "known", name: "Unsupported" })
  assert.deepEqual(code(new InvalidError("x")), { kind: "known", name: "InvalidArgument" })
  assert.deepEqual(code(new FenceViolationError(1n, 2n)), { kind: "known", name: "Conflict" })
  assert.deepEqual(code(new QuarantinedError("metrics")), { kind: "known", name: "Forbidden" })
  assert.deepEqual(code(new KvExecutionError("x", { kind: "notLeader" })), {
    kind: "known",
    name: "Unavailable"
  })
  assert.deepEqual(code(new CheckpointExecutionError("x", { kind: "lease_lost" })), {
    kind: "known",
    name: "Conflict"
  })
  const filter = new FilterExecutionError("busy", {
    code: { kind: "known", name: "Unavailable" },
    reason: "forbidden",
    message: "no grant"
  })
  assert.equal(filterReason(filter), "forbidden")
  assert.deepEqual(code(filter), { kind: "known", name: "Unavailable" })
})

void test("given_text_when_converted_then_should_round_trip_through_utf8", () => {
  assert.deepEqual(utf8("drain node-7"), new TextEncoder().encode("drain node-7"))
  assert.equal(decodeUtf8(utf8("drained, 0 connections left")), "drained, 0 connections left")
})

void test("given_a_topic_send_failure_when_published_then_should_report_every_record_unconfirmed", async () => {
  const transport = {
    sendMessages: () => Promise.reject(new TransportError("down", true))
  } as unknown as LaserTransport
  const topic = Topic.create(transport, "fleet", "readings")
  await assert.rejects(topic.batch([utf8("a"), utf8("b")]), (error: unknown) => {
    assert.ok(error instanceof PublishFailedError)
    assert.equal(error.stream, "fleet")
    assert.equal(error.topic, "readings")
    assert.equal(error.committed.length, 0)
    assert.equal(error.unconfirmed.length, 2)
    return true
  })
})

class ListMemory implements Memory {
  readonly forgotten: MemoryId[] = []
  constructor(private readonly items: MemoryItem[]) {}
  remember(): Promise<MemoryId> {
    return Promise.resolve(MemoryId.new())
  }
  recall(): Promise<readonly MemoryItem[]> {
    return Promise.resolve(this.items)
  }
  improve(): Promise<MemoryId> {
    return Promise.resolve(MemoryId.new())
  }
  forget(_scope: MemoryScope, id: MemoryId): Promise<void> {
    this.forgotten.push(id)
    return Promise.resolve()
  }
}

function item(body: string): MemoryItem {
  return {
    id: MemoryId.new(),
    body: utf8(body),
    provenance: { conversationId: ConversationId.new() }
  } as unknown as MemoryItem
}

void test("given_scoped_memory_when_consolidating_then_should_keep_the_newest_items", async () => {
  const backend = new ListMemory([item("a"), item("b"), item("c")])
  const scoped = ScopedMemory.create(MemoryHandle.custom(backend), ConversationId.new())
  const report = await scoped.consolidate(1)
  assert.equal(report.pruned, 2)
  assert.equal(report.reweighted, 0)
  assert.equal(report.derived, 0)
  assert.equal(backend.forgotten.length, 2)
})

void test("given_a_vector_handle_when_named_verbs_are_called_then_should_refuse_unsupported", () => {
  const handle = MemoryHandle.vector({ embed: () => Promise.resolve([1]) })
  assert.throws(() => handle.set("plan", utf8("{}")), UnsupportedError)
  assert.throws(() => handle.fetch("plan"), UnsupportedError)
  assert.throws(() => handle.remove("plan"), UnsupportedError)
})

void test("given_a_checkpoint_error_when_mutating_then_should_raise_the_checkpoint_class", async () => {
  const transport = {
    sendManaged: () =>
      Promise.resolve(encodeCheckpointReplyFrame({ kind: "err", error: { kind: "unauthorized" } }))
  }
  const caps = managedCapabilitiesFrom({
    versions: {
      query: 1,
      control: 1,
      kv: 1,
      fork: 1,
      agent: 1,
      graph: 1,
      checkpoint: 1,
      features: 0n
    },
    backends: []
  })
  const destinations = Destinations.create(transport, () =>
    Promise.resolve({ ...caps, destinations: { available: true, consistency: "linearizable" } })
  )
  await assert.rejects(
    destinations.setDesiredState(0n, DestinationId.fromU128(1n), 1n, 1n, "enabled"),
    (error: unknown) => error instanceof CheckpointExecutionError && isPermissionDenied(error)
  )
})

void test("given_a_size_bound_when_batching_then_should_flush_one_append_per_full_batch", async () => {
  const batches: number[] = []
  const producer = BatchingProducerBuilder.create(
    (records) => {
      batches.push(records.length)
      return Promise.resolve()
    },
    "fleet",
    "readings"
  )
    .maxRecords(2)
    .linger(60_000)
    .build()
  await producer.send(utf8("a"))
  await producer.send(utf8("b"))
  await producer.send(utf8("c"))
  assert.deepEqual(batches, [2])
  await producer.close()
  assert.deepEqual(batches, [2, 1])
  await assert.rejects(producer.send(utf8("d")), InvalidError)
})

function nativeTransport(records: PolledMessage[]): {
  readonly transport: LaserTransport
  readonly stored: [number, bigint][]
  readonly deleted: number[]
} {
  const stored: [number, bigint][] = []
  const deleted: number[] = []
  let served = false
  const transport = {
    pollMessages: () => {
      if (served) return Promise.resolve([])
      served = true
      return Promise.resolve(records)
    },
    storeOffset: (_s: string, _t: string, _target: unknown, partition: number, offset: bigint) => {
      stored.push([partition, offset])
      return Promise.resolve()
    },
    deleteOffset: (_s: string, _t: string, _target: unknown, partition: number) => {
      deleted.push(partition)
      return Promise.resolve()
    }
  } as unknown as LaserTransport
  return { transport, stored, deleted }
}

function polled(offset: bigint, body: unknown): PolledMessage {
  return { payload: utf8(JSON.stringify(body)), partitionId: 0, offset, headers: new Map() }
}

void test("given_an_each_policy_when_records_are_yielded_then_should_store_each_offset", async () => {
  const { transport, stored } = nativeTransport([
    polled(0n, { host: "node-1" }),
    polled(1n, { host: "node-2" })
  ])
  const consumer = Consumer.create(
    transport,
    "fleet",
    "readings",
    { kind: "single", partitionId: 0, name: "metrics" },
    { commitPolicy: { kind: "each" }, pollIntervalMs: 0 }
  )
  const first = await consumer.nextWithin(100)
  assert.deepEqual(first.json(), { host: "node-1" })
  await consumer.nextWithin(100)
  assert.deepEqual(stored, [
    [0, 0n],
    [0, 1n]
  ])
  assert.equal(consumer.lastStoredOffset(0), 1n)
  await consumer.shutdown()
})

void test("given_a_native_consumer_when_offsets_are_stored_and_deleted_then_should_track_them", async () => {
  const { transport, stored, deleted } = nativeTransport([polled(0n, {})])
  const consumer = Consumer.create(
    transport,
    "fleet",
    "readings",
    { kind: "single", partitionId: 0, name: "metrics" },
    { commitPolicy: { kind: "disabled" }, pollIntervalMs: 0 }
  )
  await consumer.storeOffset(7n)
  assert.deepEqual(stored, [[0, 7n]])
  assert.equal(consumer.lastStoredOffset(0), 7n)
  await consumer.deleteOffset()
  assert.deepEqual(deleted, [0])
  assert.equal(consumer.lastStoredOffset(0), undefined)
  await consumer.shutdown()
})

void test("given_a_returned_delivery_when_reading_again_then_should_yield_it_first", async () => {
  const { transport } = nativeTransport([polled(0n, { host: "node-1" }), polled(1n, {})])
  const consumer = Consumer.create(
    transport,
    "fleet",
    "readings",
    { kind: "single", partitionId: 0, name: "metrics" },
    { commitPolicy: { kind: "disabled" }, pollIntervalMs: 0 }
  )
  const first = await consumer.nextWithin(100)
  consumer[INTERNAL_RETURN_DELIVERY](first)
  assert.equal(await consumer.nextWithin(100), first)
  assert.equal((await consumer.nextWithin(100)).position.offset, 1n)
  assert.throws(() => {
    consumer[INTERNAL_RETURN_DELIVERY](first)
  }, InvalidError)
  await consumer.shutdown()
})

void test("given_records_at_or_below_the_consumed_offset_when_polled_again_then_should_skip_them_unless_replay_is_allowed", async () => {
  for (const [allowReplay, expected] of [
    [false, [0n, 1n, 2n]],
    [true, [0n, 1n, 0n, 1n, 2n]]
  ] as const) {
    const polls = [
      [polled(0n, {}), polled(1n, {})],
      [polled(0n, {}), polled(1n, {}), polled(2n, {})]
    ]
    const transport = {
      pollMessages: () => Promise.resolve(polls.shift() ?? [])
    } as unknown as LaserTransport
    const consumer = Consumer.create(
      transport,
      "fleet",
      "readings",
      { kind: "single", partitionId: 0, name: "metrics" },
      { commitPolicy: { kind: "disabled" }, pollIntervalMs: 0, allowReplay }
    )
    const offsets: bigint[] = []
    while (offsets.length < expected.length)
      offsets.push((await consumer.nextWithin(100)).position.offset)
    assert.deepEqual(offsets, [...expected])
    await consumer.shutdown()
  }
})

// A partition holding more raw records than the context read window: one turn
// of the conversation sits before the window, one inside it.
function contextLaser(conversation: ConversationId, total: number): Laser {
  const other = ConversationId.new()
  const records: PolledMessage[] = Array.from({ length: total }, (_, index) => ({
    payload: utf8(`turn-${String(index)}`),
    partitionId: 0,
    offset: BigInt(index),
    headers: encodeProvenanceHeaders({
      conversationId: index === 2 || index === total - 1 ? conversation : other
    })
  }))
  const transport = {
    pollMessages: (
      _stream: string,
      _topic: string,
      _target: unknown,
      strategy: { readonly kind: string; readonly value?: bigint },
      count: number
    ) => {
      const from = Number(strategy.value ?? 0n)
      return Promise.resolve(records.slice(from, from + count))
    }
  } as unknown as LaserTransport
  return {
    topic: (name: string) => ({
      partitionCount: () => Promise.resolve(1),
      tailOffsets: () => Promise.resolve(new Map([[0, BigInt(total)]])),
      replay: () => Promise.resolve(Cursor.create(transport, "fleet", name, [0]))
    })
  } as unknown as Laser
}

void test("given_turns_before_the_read_window_when_assembling_then_should_read_only_the_window", async () => {
  const conversation = ConversationId.new()
  const total = CONTEXT_READ_WINDOW + 5
  const laser = contextLaser(conversation, total)
  const open = await ContextAssembler.builder()
    .conversationId(conversation)
    .topics(["agent.commands"])
    .policy(new LastN(100))
    .build()
    .assemble(laser)
  assert.deepEqual(
    open.map((message) => decodeUtf8(message.payload)),
    [`turn-${String(total - 1)}`]
  )
  const atStart = Checkpoint.fromJSON({ per_topic: { "agent.commands": { "0": 5 } } })
  const before = await ContextAssembler.builder()
    .conversationId(conversation)
    .topics(["agent.commands"])
    .policy(new LastN(100))
    .toCheckpoint(atStart)
    .build()
    .assemble(laser)
  assert.deepEqual(
    before.map((message) => decodeUtf8(message.payload)),
    ["turn-2"]
  )
})

void test("given_a_failed_timer_flush_when_closing_the_batcher_then_should_report_the_failure", async () => {
  const failure = new Error("batch failed")
  let attempted!: () => void
  const attempt = new Promise<void>((resolve) => {
    attempted = resolve
  })
  const producer = BatchingProducerBuilder.create(
    () => {
      attempted()
      return Promise.reject(failure)
    },
    "fleet",
    "readings"
  )
    .linger(1)
    .build()
  const keepAlive = setTimeout(attempted, 1_000)
  try {
    await producer.send(utf8("reading"))
    await attempt
    await assert.rejects(producer.close(), (error: unknown) => error === failure)
  } finally {
    clearTimeout(keepAlive)
    await producer.close()
  }
})

void test("given_a_kept_failure_without_records_when_sending_then_should_list_the_refused_record", async () => {
  const failure = new Error("append refused")
  let failed!: () => void
  const firstFailed = new Promise<void>((resolve) => {
    failed = resolve
  })
  let calls = 0
  const producer = BatchingProducerBuilder.create(
    () => {
      calls += 1
      failed()
      return Promise.reject(failure)
    },
    "fleet",
    "readings"
  )
    .linger(1)
    .build()
  const keepAlive = setTimeout(failed, 1_000)
  try {
    await producer.send(utf8("a"))
    await firstFailed
    await assert.rejects(producer.send(utf8("b")), (error: unknown) => {
      assert.ok(error instanceof PublishFailedError)
      assert.equal(error.stream, "fleet")
      assert.equal(error.topic, "readings")
      assert.equal(error.cause, failure)
      assert.deepEqual(
        error.unconfirmed.map((record) => decodeUtf8(record.payload)),
        ["b"]
      )
      return true
    })
    await producer.close()
    assert.equal(calls, 1)
  } finally {
    clearTimeout(keepAlive)
  }
})

void test("given_a_kept_timer_failure_when_flushing_then_should_report_it_once", async () => {
  let failed!: () => void
  const firstFailed = new Promise<void>((resolve) => {
    failed = resolve
  })
  const producer = BatchingProducerBuilder.create(
    (records) => {
      failed()
      return Promise.reject(
        new PublishFailedError("fleet", "readings", [], records, new Error("refused"))
      )
    },
    "fleet",
    "readings"
  )
    .linger(1)
    .build()
  const keepAlive = setTimeout(failed, 1_000)
  try {
    await producer.send(utf8("a"))
    await firstFailed
    await assert.rejects(producer.flush(), (error: unknown) => {
      assert.ok(error instanceof PublishFailedError)
      assert.deepEqual(
        error.unconfirmed.map((record) => decodeUtf8(record.payload)),
        ["a"]
      )
      return true
    })
    await producer.flush()
    await producer.close()
  } finally {
    clearTimeout(keepAlive)
  }
})

void test("given_a_kept_timer_failure_when_sending_then_should_report_it_with_the_refused_record_unqueued", async () => {
  let failed!: () => void
  const firstFailed = new Promise<void>((resolve) => {
    failed = resolve
  })
  let calls = 0
  const producer = BatchingProducerBuilder.create(
    (records) => {
      calls += 1
      if (calls === 1) {
        failed()
        return Promise.reject(
          new PublishFailedError("fleet", "readings", [], records, new Error("append refused"))
        )
      }
      return Promise.resolve()
    },
    "fleet",
    "readings"
  )
    .maxRecords(2)
    .linger(1)
    .build()
  const keepAlive = setTimeout(failed, 1_000)
  try {
    await producer.send(utf8("a"))
    await firstFailed
    await new Promise((resolve) => setImmediate(resolve))
    await assert.rejects(producer.send(utf8("b")), (error: unknown) => {
      assert.ok(error instanceof PublishFailedError)
      assert.deepEqual(
        error.unconfirmed.map((record) => decodeUtf8(record.payload)),
        ["a", "b"]
      )
      return true
    })
    await producer.close()
    assert.equal(calls, 1)
  } finally {
    clearTimeout(keepAlive)
  }
})

void test("given_a_failed_offset_store_when_a_record_is_yielded_then_should_deliver_it_and_store_later", async () => {
  const { transport, stored } = nativeTransport([polled(0n, {}), polled(1n, {})])
  const storeOffset = transport.storeOffset.bind(transport)
  let attempts = 0
  transport.storeOffset = (...args: Parameters<typeof storeOffset>) => {
    attempts += 1
    return attempts === 1
      ? Promise.reject(new TransportError("socket closed", true))
      : storeOffset(...args)
  }
  const consumer = Consumer.create(
    transport,
    "fleet",
    "readings",
    { kind: "single", partitionId: 0, name: "metrics" },
    { commitPolicy: { kind: "each" }, pollIntervalMs: 0 }
  )
  assert.equal((await consumer.nextWithin(100)).position.offset, 0n)
  assert.equal(consumer.lastStoredOffset(0), undefined)
  assert.equal((await consumer.nextWithin(100)).position.offset, 1n)
  assert.deepEqual(stored, [[0, 1n]])
  await consumer.shutdown()
})

void test("given_a_deleted_offset_when_shutting_down_then_should_not_store_it_again", async () => {
  const { transport, stored, deleted } = nativeTransport([polled(0n, {})])
  const consumer = Consumer.create(
    transport,
    "fleet",
    "readings",
    { kind: "single", partitionId: 0, name: "metrics" },
    { commitPolicy: { kind: "each" }, pollIntervalMs: 0 }
  )
  await consumer.nextWithin(100)
  await consumer.deleteOffset()
  await consumer.shutdown()
  assert.deepEqual(stored, [[0, 0n]])
  assert.deepEqual(deleted, [0])
})

void test("given_a_failing_poll_when_the_deadline_is_short_then_should_time_out_before_the_retry_interval", async () => {
  const transport = {
    pollMessages: () => Promise.reject(new TransportError("socket closed", true))
  } as unknown as LaserTransport
  const consumer = Consumer.create(
    transport,
    "fleet",
    "readings",
    { kind: "single", partitionId: 0, name: "metrics" },
    { commitPolicy: { kind: "disabled" }, pollIntervalMs: 0 }
  )
  const started = Date.now()
  await assert.rejects(consumer.nextWithin(30), TimeoutError)
  assert.ok(Date.now() - started < 500)
})

void test("given_a_typed_error_that_wraps_an_iggy_refusal_when_classified_then_should_keep_its_own_class", () => {
  const wrapped = new InvalidError("the handler failed", { cause: iggyFailure(41) })
  assert.equal(iggyErrorCode(wrapped), undefined)
  assert.equal(isPermissionDenied(wrapped), false)
  assert.equal(
    isPermissionDenied(new PublishFailedError("fleet", "readings", [], [], iggyFailure(41))),
    true
  )
})

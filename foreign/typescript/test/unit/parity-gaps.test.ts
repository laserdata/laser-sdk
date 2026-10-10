import assert from "node:assert/strict"
import { test } from "node:test"

import { createAgdx } from "../../src/agent/agdx.js"
import type { AgentCtx } from "../../src/agent/context.js"
import { ContractBuilder } from "../../src/agent/contract.js"
import { MemoryHandler } from "../../src/agent/memory-handler.js"
import {
  cardAvailableFor,
  cardIsFresh,
  cardServes,
  type RegisteredCard
} from "../../src/agent/registry.js"
import type { AgentHandler, AgentMessage } from "../../src/agent/reliable-consumer.js"
import type { Router } from "../../src/agent/router.js"
import { AgentScope } from "../../src/agent/scope.js"
import { type BlobStore, resolveBody } from "../../src/blob.js"
import {
  backend,
  enabledBackends,
  filterCapsEvaluates,
  isOpenOnly,
  isReady,
  OPEN_CAPABILITIES,
  readinessReasons,
  unreadyBackends,
  type Capabilities
} from "../../src/client/capabilities.js"
import { InvalidError, PublishFailedError, TransportError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { ScopedMemory } from "../../src/context-scope.js"
import { type ActionDecision, type ActionGovernor, SwappableGovernor } from "../../src/govern.js"
import {
  type LaserTransport,
  type MessageWithHeaders,
  type TopicCreateSettings,
  UNLIMITED_TOPIC_SIZE
} from "../../src/iggy/apache-iggy.js"
import { type QueryExecutor, QueryRequest } from "../../src/managed/query.js"
import { MemoryHandle } from "../../src/memory/handle.js"
import {
  type Memory,
  MemoryId,
  type MemoryItem,
  MemoryKind,
  type MemoryScope,
  type Summarizer
} from "../../src/memory/types.js"
import { VectorMemory } from "../../src/memory/vector-memory.js"
import { KeyRecord, KeyRegistry, SigningKey } from "../../src/signing.js"
import { Producer } from "../../src/stream/producer.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { decodeAgentEnvelope } from "../../src/wire/agent.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import { ContentType, contentTypeCode } from "../../src/wire/content.js"
import { CONTENT_TYPE } from "../../src/wire/headers.js"
import type { BackendDescriptor } from "../../src/wire/hello.js"
import { BackendResourceId, CorrelationId, DestinationId } from "../../src/wire/ids.js"
import { lakehouseTarget } from "../../src/wire/query.js"

const confirmation = (baseOffset: bigint) => ({
  streamId: 1,
  topicId: 2,
  partitionId: 0,
  baseOffset
})

function recordingTransport(fail: (call: number) => boolean = () => false): {
  transport: LaserTransport
  calls: (readonly MessageWithHeaders[])[]
} {
  const calls: (readonly MessageWithHeaders[])[] = []
  const transport = {
    sendMessagesWithHeaders: (
      _stream: string,
      _topic: string,
      messages: readonly MessageWithHeaders[]
    ) => {
      calls.push(messages)
      return fail(calls.length)
        ? Promise.reject(new TransportError("permanent", false))
        : Promise.resolve({ confirmations: [confirmation(BigInt(calls.length))] })
    }
  } as unknown as LaserTransport
  return { transport, calls }
}

const payloads = (count: number) =>
  Array.from({ length: count }, (_, index) => new Uint8Array([index]))

void test("given_a_batch_longer_than_batch_length_when_sent_directly_then_should_split_it_into_sequential_requests", async () => {
  const { transport, calls } = recordingTransport()
  const producer = Producer.create(transport, "fleet", "readings", {
    batchLength: 2,
    createStream: false,
    createTopic: false
  })
  const response = await producer.sendBatch(payloads(5))
  assert.deepEqual(
    calls.map((call) => call.length),
    [2, 2, 1]
  )
  assert.equal(response.confirmations.length, 3)
})

void test("given_a_failure_mid_batch_when_sent_directly_then_should_report_the_committed_prefix_and_the_unconfirmed_tail", async () => {
  const { transport } = recordingTransport((call) => call === 2)
  const producer = Producer.create(transport, "fleet", "readings", {
    batchLength: 2,
    retries: 0,
    createStream: false,
    createTopic: false
  })
  await assert.rejects(
    producer.sendBatch(payloads(5)),
    (error: unknown) =>
      error instanceof PublishFailedError &&
      error.committed.length === 1 &&
      error.unconfirmed.length === 3
  )
})

void test("given_a_zero_batch_length_when_building_a_producer_then_should_refuse_it", () => {
  assert.throws(
    () => Producer.create(recordingTransport().transport, "fleet", "readings", { batchLength: 0 }),
    InvalidError
  )
})

void test("given_a_linger_gap_when_sending_twice_then_should_wait_out_the_rest_of_it", async () => {
  const { transport } = recordingTransport()
  const producer = Producer.create(transport, "fleet", "readings", {
    lingerMs: 40,
    createStream: false,
    createTopic: false
  })
  await producer.send(new Uint8Array([1]))
  const started = Date.now()
  await producer.send(new Uint8Array([2]))
  assert.ok(Date.now() - started >= 30)
})

void test("given_background_mode_when_sending_then_should_queue_without_confirmations_and_write_on_flush", async () => {
  const { transport, calls } = recordingTransport()
  const producer = Producer.create(transport, "fleet", "readings", {
    background: { lingerMs: 10_000 },
    createStream: false,
    createTopic: false
  })
  assert.equal(producer.isBackground, true)
  const response = await producer.send(new Uint8Array([1]))
  await producer.send(new Uint8Array([2]))
  assert.equal(response.confirmations.length, 0)
  assert.equal(calls.length, 0)
  await producer.flush()
  assert.deepEqual(
    calls.map((call) => call.length),
    [2]
  )
  await producer.shutdown()
})

void test("given_background_mode_when_the_batch_length_is_reached_then_should_flush_without_waiting_for_linger", async () => {
  const { transport, calls } = recordingTransport()
  const producer = Producer.create(transport, "fleet", "readings", {
    background: { batchLength: 2, lingerMs: 10_000 },
    createStream: false,
    createTopic: false
  })
  await producer.send(new Uint8Array([1]))
  assert.equal(calls.length, 0)
  await producer.send(new Uint8Array([2]))
  await new Promise((resolve) => setImmediate(resolve))
  assert.deepEqual(
    calls.map((call) => call.length),
    [2]
  )
  await producer.shutdown()
})

void test("given_a_failed_background_write_when_flushing_then_should_reject_with_the_failure_once", async () => {
  const { transport } = recordingTransport(() => true)
  const producer = Producer.create(transport, "fleet", "readings", {
    retries: 0,
    background: { lingerMs: 0 },
    createStream: false,
    createTopic: false
  })
  await producer.send(new Uint8Array([1]))
  await assert.rejects(producer.flush(), PublishFailedError)
  await producer.flush()
})

void test("given_a_background_error_handler_when_a_write_fails_then_should_receive_the_failure", async () => {
  const { transport } = recordingTransport(() => true)
  const failures: PublishFailedError[] = []
  const producer = Producer.create(transport, "fleet", "readings", {
    retries: 0,
    background: {
      lingerMs: 0,
      onError: (error) => {
        failures.push(error)
      }
    },
    createStream: false,
    createTopic: false
  })
  await producer.send(new Uint8Array([1]))
  await producer.shutdown()
  assert.equal(failures.length, 1)
  assert.equal(failures[0]?.unconfirmed.length, 1)
})

void test("given_topic_size_limits_when_provisioning_then_should_create_the_topic_with_them", async () => {
  const created: TopicCreateSettings[] = []
  const transport = {
    ...recordingTransport().transport,
    sendMessagesWithHeaders: () => Promise.resolve({ confirmations: [] }),
    ensureStream: () => Promise.resolve(),
    ensureTopic: () => Promise.resolve(),
    createTopicIfAbsent: (
      _stream: string,
      _topic: string,
      _partitions: number,
      settings: TopicCreateSettings
    ) => {
      created.push(settings)
      return Promise.resolve()
    }
  } as unknown as LaserTransport
  await Producer.create(transport, "fleet", "readings", { maxTopicBytes: 1024n }).send(
    new Uint8Array([1])
  )
  await Producer.create(transport, "fleet", "readings", { unlimitedTopicSize: true }).send(
    new Uint8Array([1])
  )
  assert.deepEqual(created, [{ maxTopicSize: 1024n }, { maxTopicSize: UNLIMITED_TOPIC_SIZE }])
})

const noopExecutor: QueryExecutor = () => Promise.reject(new Error("not executed"))

void test("given_a_lakehouse_target_when_selecting_a_snapshot_then_should_set_the_selector", () => {
  const target = lakehouseTarget(DestinationId.fromU128(7n), 1n)
  const bySnapshot = QueryRequest.create(target, noopExecutor).atSnapshot(42n).intoQuery()
  const byTime = QueryRequest.create(target, noopExecutor).atTimestampMicros(1_000n).intoQuery()
  assert.deepEqual(bySnapshot.target.kind === "lakehouse" && bySnapshot.target.snapshot, {
    kind: "snapshot_id",
    value: 42n
  })
  assert.deepEqual(byTime.target.kind === "lakehouse" && byTime.target.snapshot, {
    kind: "timestamp_micros",
    value: 1_000n
  })
})

void test("given_expiry_only_when_a_producer_provisions_then_should_use_create_if_absent", async () => {
  const created: TopicCreateSettings[] = []
  let updates = 0
  const transport = {
    ...recordingTransport().transport,
    ensureStream: () => Promise.resolve(),
    createTopicIfAbsent: (
      _stream: string,
      _topic: string,
      _partitions: number,
      settings: TopicCreateSettings
    ) => {
      created.push(settings)
      return Promise.resolve()
    },
    ensureTopicWithExpiry: () => {
      updates += 1
      return Promise.resolve()
    }
  } as unknown as LaserTransport
  await Producer.create(transport, "fleet", "readings", { expireAfterMs: 1_000 }).send(
    new Uint8Array([1])
  )
  assert.deepEqual(created, [{ messageExpiryMicros: 1_000_000n }])
  assert.equal(updates, 0)
})

void test("given_an_operational_target_when_selecting_a_snapshot_then_should_refuse_it", () => {
  assert.throws(() => QueryRequest.create("readings", noopExecutor).atSnapshot(1n), InvalidError)
  assert.throws(
    () => QueryRequest.create("readings", noopExecutor).atTimestampMicros(1n),
    InvalidError
  )
})

void test("given_no_row_ceiling_when_streaming_typed_rows_then_should_refuse_it", () => {
  assert.throws(
    () =>
      QueryRequest.create("readings", noopExecutor).rowsTyped({
        contentType: "raw",
        encode: (value: string) => new TextEncoder().encode(value),
        decode: (bytes: Uint8Array) => new TextDecoder().decode(bytes)
      }),
    InvalidError
  )
})

void test("given_memory_handles_when_reading_the_backend_then_should_name_the_resolved_backend", () => {
  const custom: Memory = {
    remember: () => Promise.reject(new Error("unused")),
    recall: () => Promise.resolve([]),
    improve: () => Promise.reject(new Error("unused")),
    forget: () => Promise.resolve()
  }
  const vector = MemoryHandle.create(new VectorMemory({ embed: () => Promise.resolve([1]) }))
  assert.equal(vector.backend, "vector")
  assert.equal(MemoryHandle.create(custom).backend, "custom")
  assert.equal(
    vector.reranker({ rerank: (_query, items) => Promise.resolve(items) }).backend,
    "vector"
  )
})

void test("given_an_agent_scope_when_opening_a_contract_then_should_send_as_that_agent", () => {
  const router = { kind: "broadcast" } as unknown as Router
  const laser = {
    contract: (route: Router) => ContractBuilder.create(laser as unknown as Laser, route)
  }
  const scope = AgentScope.create(laser as unknown as Laser, AgentId.new("rotator"))
  const builder = scope.contract(router)
  assert.ok(builder instanceof ContractBuilder)
})

function descriptor(id: bigint, ready: boolean, enabled = true): BackendDescriptor {
  return {
    resourceId: BackendResourceId.fromU128(id),
    desiredState: enabled ? "enabled" : "disabled",
    observedState: ready ? "ready" : "degraded",
    readiness: { ready, reasons: ready ? [] : ["catalog_unavailable"] }
  } as unknown as BackendDescriptor
}

void test("given_backend_observations_when_reading_readiness_then_should_mirror_the_rust_helpers", () => {
  const capabilities: Capabilities = {
    ...OPEN_CAPABILITIES,
    managed: true,
    backends: [descriptor(1n, true), descriptor(2n, false), descriptor(3n, false, false)]
  }
  assert.equal(enabledBackends(capabilities).length, 2)
  assert.equal(unreadyBackends(capabilities).length, 1)
  assert.equal(backend(capabilities, BackendResourceId.fromU128(2n))?.readiness.ready, false)
  assert.deepEqual(readinessReasons(capabilities, BackendResourceId.fromU128(2n)), [
    "catalog_unavailable"
  ])
  assert.equal(isReady(capabilities), false)
  assert.equal(
    isReady({ ...capabilities, backends: [descriptor(1n, true), descriptor(3n, false, false)] }),
    true
  )
})

void test("given_capabilities_when_checking_open_only_then_should_match_only_the_open_set", () => {
  assert.equal(isOpenOnly(OPEN_CAPABILITIES), true)
  assert.equal(isOpenOnly({ ...OPEN_CAPABILITIES, watch: true }), false)
  assert.equal(isOpenOnly({ ...OPEN_CAPABILITIES, hello: "answered" }), true)
})

void test("given_a_swappable_governor_when_swapping_then_should_return_the_previous_and_expose_the_current", () => {
  const first: ActionGovernor = {
    decide: () => Promise.resolve(undefined as unknown as ActionDecision)
  }
  const second: ActionGovernor = {
    decide: () => Promise.resolve(undefined as unknown as ActionDecision)
  }
  const governor = new SwappableGovernor(first)
  assert.equal(governor.current(), first)
  assert.equal(governor.swap(second), first)
  assert.equal(governor.current(), second)
})

void test("given_an_empty_flush_when_background_records_arrive_then_should_still_drain_them", async () => {
  const { transport, calls } = recordingTransport()
  const producer = Producer.create(transport, "fleet", "readings", {
    background: { lingerMs: 10_000 },
    createStream: false,
    createTopic: false
  })
  await producer.flush()
  await producer.send(new Uint8Array([1]))
  await producer.flush()
  await producer.flush()
  await producer.send(new Uint8Array([2]))
  await producer.shutdown()
  assert.deepEqual(
    calls.map((call) => call[0]?.payload[0]),
    [1, 2]
  )
})

void test("given_a_throwing_background_callback_when_a_write_fails_then_should_preserve_the_publish_failure", async () => {
  const { transport } = recordingTransport(() => true)
  const producer = Producer.create(transport, "fleet", "readings", {
    retries: 0,
    background: {
      lingerMs: 0,
      onError: () => {
        throw new Error("callback failed")
      }
    },
    createStream: false,
    createTopic: false
  })
  await producer.send(new Uint8Array([1]))
  await assert.rejects(
    producer.shutdown(),
    (error: unknown) => error instanceof PublishFailedError && error.unconfirmed.length === 1
  )
})

void test("given_a_rejected_background_callback_when_a_write_fails_then_should_preserve_the_publish_failure", async () => {
  const { transport } = recordingTransport(() => true)
  const producer = Producer.create(transport, "fleet", "readings", {
    retries: 0,
    background: {
      lingerMs: 0,
      onError: () => Promise.reject(new Error("notification failed"))
    },
    createStream: false,
    createTopic: false
  })
  await producer.send(new Uint8Array([1]))
  await assert.rejects(
    producer.shutdown(),
    (error: unknown) => error instanceof PublishFailedError && error.unconfirmed.length === 1
  )
})

void test("given_a_partially_committed_transport_chunk_when_sending_then_should_preserve_its_confirmations_and_tail", async () => {
  const { transport } = recordingTransport()
  transport.sendMessagesWithHeaders = (_stream, _topic, messages) =>
    Promise.reject(
      new PublishFailedError(
        "fleet",
        "readings",
        [confirmation(7n)],
        messages.slice(1),
        new TransportError("permanent", false)
      )
    )
  const producer = Producer.create(transport, "fleet", "readings", {
    batchLength: 2,
    retries: 0,
    createStream: false,
    createTopic: false
  })
  await assert.rejects(producer.sendBatch(payloads(4)), (error: unknown) => {
    assert.ok(error instanceof PublishFailedError)
    assert.deepEqual(error.committed, [confirmation(7n)])
    assert.deepEqual(
      error.unconfirmed.map((message) => message.payload[0]),
      [1, 2, 3]
    )
    return true
  })
})

void test("given_a_send_waiting_for_provisioning_when_shutdown_finishes_then_should_refuse_the_late_enqueue", async () => {
  let provisioned!: () => void
  const { transport, calls } = recordingTransport()
  transport.ensureStream = () =>
    new Promise<void>((resolve) => {
      provisioned = resolve
    })
  const producer = Producer.create(transport, "fleet", "readings", {
    background: { lingerMs: 10_000 },
    createTopic: false
  })
  const sending = producer.send(new Uint8Array([1]))
  const refused = assert.rejects(sending, InvalidError)
  await producer.shutdown()
  provisioned()
  await refused
  assert.equal(calls.length, 0)
})

void test("given_two_failed_background_writes_when_flushing_then_should_report_the_records_of_both", async () => {
  const { transport } = recordingTransport(() => true)
  const producer = Producer.create(transport, "fleet", "readings", {
    retries: 0,
    background: { lingerMs: 0 },
    createStream: false,
    createTopic: false
  })
  await producer.send(new Uint8Array([1]), { partition: 0 })
  await producer.send(new Uint8Array([2]), { partition: 1 })
  await assert.rejects(producer.flush(), (error: unknown) => {
    assert.ok(error instanceof PublishFailedError)
    assert.deepEqual(
      error.unconfirmed.map((record) => [...record.payload]),
      [[1], [2]]
    )
    return true
  })
  await producer.flush()
})

void test("given_a_shutdown_in_flight_when_shutdown_is_called_again_then_should_wait_for_the_drain", async () => {
  let release!: () => void
  const written = new Promise<void>((resolve) => {
    release = resolve
  })
  let finished = false
  const transport = {
    sendMessagesWithHeaders: async () => {
      await written
      finished = true
      return { confirmations: [] }
    }
  } as unknown as LaserTransport
  const producer = Producer.create(transport, "fleet", "readings", {
    background: { lingerMs: 60_000 },
    createStream: false,
    createTopic: false
  })
  await producer.send(new Uint8Array([1]))
  const first = producer.shutdown()
  const second = producer.shutdown().then(() => finished)
  release()
  await first
  assert.equal(await second, true)
})

void test("given_a_message_expiry_when_the_transport_cannot_set_it_then_should_refuse_to_provision", async () => {
  const transport = {
    ...recordingTransport().transport,
    ensureStream: () => Promise.resolve(),
    ensureTopic: () => Promise.resolve()
  } as unknown as LaserTransport
  const producer = Producer.create(transport, "fleet", "readings", {
    expireAfterMs: 1_000
  })
  await assert.rejects(producer.send(new Uint8Array([1])), (error: unknown) => {
    assert.ok(error instanceof PublishFailedError)
    return error.publishCause() instanceof InvalidError
  })
})

class MapBlobStore implements BlobStore {
  readonly blobs = new Map<string, Uint8Array>()

  put(payload: Uint8Array): Promise<string> {
    const reference = `blob-${String(this.blobs.size)}`
    this.blobs.set(reference, payload.slice())
    return Promise.resolve(reference)
  }

  get(reference: string): Promise<Uint8Array> {
    const payload = this.blobs.get(reference)
    return payload === undefined
      ? Promise.reject(new InvalidError(`unknown blob ${reference}`))
      : Promise.resolve(payload.slice())
  }
}

function agdxOver(transport: LaserTransport) {
  return createAgdx(
    transport,
    "agents",
    "agent.sessions",
    AgentId.new("source-agent"),
    ConversationId.derive("claim-check")
  )
}

function sentEnvelope(message: MessageWithHeaders | undefined) {
  assert.ok(message !== undefined, "one message was sent")
  const context = "sent envelope"
  return decodeAgentEnvelope(expectMap(decodeOne(message.payload, context), context), context)
}

void test("given_a_body_over_the_threshold_when_an_agdx_send_claim_checks_then_should_sign_and_send_the_capsule", async () => {
  const { transport, calls } = recordingTransport()
  const store = new MapBlobStore()
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(7))
  const body = new Uint8Array(4096).fill(9)
  await agdxOver(transport)
    .command(CorrelationId.fromU128(5n), body)
    .contentType(ContentType.Json)
    .signedBy(key)
    .claimCheck(store, 1024)
    .send()
  const sent = calls[0]?.[0]
  const envelope = sentEnvelope(sent)
  assert.equal(store.blobs.size, 1)
  assert.deepEqual(sent?.headers.get(CONTENT_TYPE), {
    kind: "uint8",
    value: contentTypeCode(ContentType.Ref)
  })
  assert.deepEqual(await resolveBody(store, envelope.body), body)
  assert.equal(envelope.signature?.context?.contentType, contentTypeCode(ContentType.Ref))
  const registry = new KeyRegistry()
  registry.enroll("source-principal", key.verifyingKey())
  assert.equal(registry.verify(envelope), "source-principal")
})

void test("given_a_body_under_the_threshold_when_an_agdx_send_claim_checks_then_should_send_it_inline", async () => {
  const { transport, calls } = recordingTransport()
  const store = new MapBlobStore()
  const body = new TextEncoder().encode("small")
  await agdxOver(transport)
    .command(CorrelationId.fromU128(5n), body)
    .contentType(ContentType.Json)
    .claimCheck(store, 1024)
    .send()
  const sent = calls[0]?.[0]
  assert.equal(store.blobs.size, 0)
  assert.deepEqual(sentEnvelope(sent).body, body)
  assert.deepEqual(sent?.headers.get(CONTENT_TYPE), {
    kind: "uint8",
    value: contentTypeCode(ContentType.Json)
  })
})

void test("given_a_negative_threshold_when_an_agdx_send_claim_checks_then_should_refuse_it", () => {
  const { transport } = recordingTransport()
  const send = agdxOver(transport).command(CorrelationId.fromU128(5n), new Uint8Array(1))
  assert.throws(() => send.claimCheck(new MapBlobStore(), -1), InvalidError)
})

void test("given_a_conversation_id_when_reading_as_u128_then_should_return_the_raw_ulid_value", () => {
  assert.equal(ConversationId.parse("0000000000000000000000000A").asU128(), 10n)
  assert.equal(ConversationId.parse("7ZZZZZZZZZZZZZZZZZZZZZZZZZ").asU128(), (1n << 128n) - 1n)
  assert.equal(ConversationId.derive("node-7").asU128(), ConversationId.derive("node-7").asU128())
  assert.notEqual(
    ConversationId.derive("node-7").asU128(),
    ConversationId.derive("node-8").asU128()
  )
})

class KindMemory implements Memory {
  readonly remembered: Uint8Array[] = []
  readonly forgotten: MemoryId[] = []
  refuseForget: MemoryId | undefined

  constructor(private readonly items: MemoryItem[]) {}

  remember(_scope: MemoryScope, payload: Uint8Array): Promise<MemoryId> {
    this.remembered.push(payload)
    return Promise.resolve(MemoryId.new())
  }

  recall(): Promise<readonly MemoryItem[]> {
    return Promise.resolve([...this.items].reverse())
  }

  improve(): Promise<MemoryId> {
    return Promise.resolve(MemoryId.new())
  }

  forget(_scope: MemoryScope, id: MemoryId): Promise<void> {
    if (this.refuseForget?.equals(id) === true) {
      return Promise.reject(new TransportError("forget refused", false))
    }
    this.forgotten.push(id)
    return Promise.resolve()
  }
}

let nextKindId = 1n

function kindItem(kind: MemoryKind, body: string): MemoryItem {
  return {
    id: MemoryId.fromU128(nextKindId++),
    payload: new TextEncoder().encode(body),
    provenance: { conversationId: ConversationId.derive("consolidate") },
    kind,
    signals: []
  }
}

const joiningSummarizer: Summarizer = {
  summarize: (bodies) =>
    Promise.resolve(
      new TextEncoder().encode(bodies.map((body) => new TextDecoder().decode(body)).join("+"))
    )
}

void test("given_a_summarizer_when_consolidated_then_should_fold_only_the_messages_and_keep_them", async () => {
  const store = new KindMemory([
    kindItem(MemoryKind.Message, "cpu 82"),
    kindItem(MemoryKind.Fact, "node-7 is a storage host"),
    kindItem(MemoryKind.Message, "cpu 91")
  ])
  const report = await MemoryHandle.custom(store).consolidate({}, 100, {
    summarizer: joiningSummarizer
  })
  assert.deepEqual(report, { summarized: 2, reweighted: 0, pruned: 0, derived: 0 })
  assert.deepEqual(
    store.remembered.map((body) => new TextDecoder().decode(body)),
    ["cpu 91+cpu 82"]
  )
  assert.equal(store.forgotten.length, 0)
})

void test("given_prune_summarized_when_consolidated_then_should_forget_the_folded_messages", async () => {
  const items = [
    kindItem(MemoryKind.Message, "cpu 82"),
    kindItem(MemoryKind.Fact, "node-7 is a storage host"),
    kindItem(MemoryKind.Message, "cpu 91"),
    kindItem(MemoryKind.Fact, "node-8 is a metrics host")
  ]
  const store = new KindMemory(items)
  const scoped = ScopedMemory.create(
    MemoryHandle.custom(store),
    ConversationId.derive("consolidate")
  )
  const report = await scoped.consolidate(1, {
    summarizer: joiningSummarizer,
    pruneSummarized: true
  })
  assert.deepEqual(report, { summarized: 2, reweighted: 0, pruned: 3, derived: 0 })
  assert.deepEqual(
    store.forgotten.map((id) => id.asU128()),
    [items[2], items[0], items[1]].map((entry) => entry?.id.asU128())
  )
})

void test("given_a_refused_forget_when_pruning_summarized_then_should_count_only_the_forgotten_messages", async () => {
  const items = [kindItem(MemoryKind.Message, "cpu 82"), kindItem(MemoryKind.Message, "cpu 91")]
  const store = new KindMemory(items)
  store.refuseForget = items[0]?.id
  const report = await MemoryHandle.custom(store).consolidate({}, 100, {
    summarizer: joiningSummarizer,
    pruneSummarized: true
  })
  assert.deepEqual(report, { summarized: 2, reweighted: 0, pruned: 1, derived: 0 })
})

void test("given_a_refused_forget_when_pruning_past_max_items_then_should_keep_the_item_and_not_count_it", async () => {
  const items = [
    kindItem(MemoryKind.Fact, "node-7 is a storage host"),
    kindItem(MemoryKind.Fact, "node-8 is a metrics host"),
    kindItem(MemoryKind.Fact, "node-9 is a config host")
  ]
  const store = new KindMemory(items)
  store.refuseForget = items[1]?.id
  const report = await MemoryHandle.custom(store).consolidate({}, 1)
  assert.deepEqual(report, { summarized: 0, reweighted: 0, pruned: 1, derived: 0 })
})

void test("given_no_messages_when_consolidated_with_a_summarizer_then_should_not_call_it", async () => {
  const store = new KindMemory([kindItem(MemoryKind.Fact, "node-7 is a storage host")])
  let calls = 0
  const report = await MemoryHandle.custom(store).consolidate({}, 100, {
    summarizer: {
      summarize: () => {
        calls += 1
        return Promise.resolve(new Uint8Array())
      }
    },
    pruneSummarized: true
  })
  assert.equal(calls, 0)
  assert.deepEqual(report, { summarized: 0, reweighted: 0, pruned: 0, derived: 0 })
  assert.equal(store.remembered.length, 0)
})

void test("given_filter_capabilities_when_checking_evaluation_then_should_match_the_announced_contract", () => {
  const announced = {
    ...OPEN_CAPABILITIES.filters,
    evaluation: { evaluatorVersion: 2, codecs: ["json", "cbor"] as const }
  }
  assert.equal(filterCapsEvaluates(announced, 2, "json"), true)
  assert.equal(filterCapsEvaluates(announced, 2, "avro"), false)
  assert.equal(filterCapsEvaluates(announced, 1, "json"), false)
  assert.equal(filterCapsEvaluates(OPEN_CAPABILITIES.filters, 9, "protobuf"), true)
})

void test("given_a_key_record_when_reading_its_key_id_then_should_match_the_signing_key_id", () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(7))
  const record = KeyRecord.agent("source-principal", key.verifyingKey())
  assert.equal(record.keyId().byteLength, 8)
  assert.deepEqual(record.keyId(), key.keyId())
  const other = SigningKey.fromBytes(new Uint8Array(32).fill(8))
  assert.notDeepEqual(KeyRecord.agent("other", other.verifyingKey()).keyId(), key.keyId())
})

function handledMessage(agent?: AgentId): AgentMessage {
  return {
    provenance: {
      conversationId: ConversationId.derive("incident-42"),
      ...(agent !== undefined ? { agent } : {})
    },
    payload: new TextEncoder().encode("disk pressure on node-7")
  } as unknown as AgentMessage
}

const noContext = {} as unknown as AgentCtx

void test("given_auto_remember_when_a_message_is_handled_then_should_remember_it_after_the_handler", async () => {
  const memory = MemoryHandle.vector({ embed: () => Promise.resolve([1]) })
  const conversation = ConversationId.derive("incident-42")
  let seenBeforeRemember = -1
  const inner: AgentHandler = {
    handle: async (_message, context) => {
      assert.equal(context, noContext)
      seenBeforeRemember = (await memory.recall(conversation).fetch()).length
    }
  }
  const handler = new MemoryHandler(inner, memory).autoRemember(MemoryKind.Message)
  await handler.handle(handledMessage(AgentId.new("triage")), noContext)
  const items = await memory.recall(conversation).agent(AgentId.new("triage")).fetch()
  assert.equal(seenBeforeRemember, 0)
  const [remembered] = items
  assert.ok(remembered !== undefined, "the handled message was remembered")
  assert.equal(items.length, 1)
  assert.equal(remembered.kind, MemoryKind.Message)
  assert.equal(new TextDecoder().decode(remembered.payload), "disk pressure on node-7")
})

void test("given_no_auto_remember_when_a_message_is_handled_then_should_remember_nothing", async () => {
  const store = new KindMemory([])
  let handled = 0
  const inner: AgentHandler = {
    handle: () => {
      handled += 1
      return Promise.resolve()
    }
  }
  await new MemoryHandler(inner, MemoryHandle.custom(store)).handle(handledMessage(), noContext)
  assert.equal(handled, 1)
  assert.equal(store.remembered.length, 0)
})

void test("given_a_failing_handler_when_auto_remembering_then_should_rethrow_and_remember_nothing", async () => {
  const store = new KindMemory([])
  const inner: AgentHandler = {
    handle: () => Promise.reject(new InvalidError("handler refused"))
  }
  const handler = new MemoryHandler(inner, MemoryHandle.custom(store)).autoRemember(
    MemoryKind.Message
  )
  await assert.rejects(handler.handle(handledMessage(), noContext), InvalidError)
  assert.equal(store.remembered.length, 0)
})

void test("given_a_failing_memory_write_when_auto_remembering_then_should_still_resolve_the_turn", async () => {
  let attempts = 0
  const failing = {
    remember: () => {
      attempts += 1
      return Promise.reject(new TransportError("memory down", false))
    }
  } as unknown as Memory
  const inner: AgentHandler = { handle: () => Promise.resolve() }
  const handler = new MemoryHandler(inner, MemoryHandle.custom(failing)).autoRemember(
    MemoryKind.Message
  )
  await handler.handle(handledMessage(), noContext)
  assert.equal(attempts, 1)
})

function registered(card: RegisteredCard["card"], observedAtMicros = 100n): RegisteredCard {
  return { agent: AgentId.new("triage"), card, observedAtMicros }
}

void test("given_registered_cards_when_checking_freshness_then_should_expire_only_past_the_ttl", () => {
  assert.equal(cardIsFresh(registered({ capabilities: [] }), 10_000n), true)
  const expiring = registered({ capabilities: [], ttlMicros: 50n })
  assert.equal(cardIsFresh(expiring, 150n), true)
  assert.equal(cardIsFresh(expiring, 151n), false)
})

void test("given_a_registered_card_when_checking_skills_then_should_gate_on_advertised_health", () => {
  const card = registered({
    capabilities: [
      { skillId: "diagnose" },
      { skillId: "drain", health: { kind: "known", name: "Unavailable" } },
      { skillId: "restart", health: { kind: "known", name: "Degraded" } }
    ]
  })
  assert.equal(cardServes(card, "diagnose"), true)
  assert.equal(cardServes(card, "drain"), true)
  assert.equal(cardServes(card, "reboot"), false)
  assert.equal(cardAvailableFor(card, "diagnose"), true)
  assert.equal(cardAvailableFor(card, "drain"), false)
  assert.equal(cardAvailableFor(card, "restart"), true)
  assert.equal(cardAvailableFor(card, "reboot"), false)
})

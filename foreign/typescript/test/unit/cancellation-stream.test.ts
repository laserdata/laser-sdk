import assert from "node:assert/strict"
import { test } from "node:test"

import { OPEN_CAPABILITIES, type Capabilities } from "../../src/client/capabilities.js"
import { CancelledError, PublishFailedError, TransportError } from "../../src/client/errors.js"
import {
  ApacheIggyTransport,
  type IggyClient,
  type LaserTransport
} from "../../src/iggy/apache-iggy.js"
import { BatchingProducerBuilder } from "../../src/stream/batching.js"
import { Json } from "../../src/stream/codecs.js"
import { ConsumerGroup, type GroupContext } from "../../src/stream/consumer-group.js"
import { Consumer, type ConsumerMessage } from "../../src/stream/consumer.js"
import { Cursor } from "../../src/stream/cursor.js"
import { Producer, ProducerMessage } from "../../src/stream/producer.js"
import { Stream } from "../../src/stream/stream.js"
import { Topic } from "../../src/stream/topic.js"
import { TypedRecords } from "../../src/stream/typed-topic.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import {
  AGDX_FILTER_MUTATE_CODE,
  AGDX_GET_FILTER_BINDING_CODE,
  FILTER_OP_VERSION
} from "../../src/wire/codes.js"
import {
  ConsumerFilter,
  FilterExpr,
  consumerFilterDigest,
  encodeFilterCatalogReply,
  type FilterBinding,
  type FilterGroupIdentity
} from "../../src/wire/filter.js"

const identity: FilterGroupIdentity = {
  streamId: 1,
  streamCreatedAtMicros: 1n,
  topicId: 2,
  topicCreatedAtMicros: 2n,
  groupId: 7n
}
const definition = ConsumerFilter.json(FilterExpr.present("mode"))
const binding: FilterBinding = {
  group: { stream: "orbit", topic: "fleet", group: "workers" },
  identity,
  filterId: 1,
  revision: 1,
  digest: consumerFilterDigest(definition),
  boundAtMicros: 3n,
  policyGeneration: 1n
}
const capabilities: Capabilities = {
  ...OPEN_CAPABILITIES,
  managed: true,
  hello: "answered",
  versions: {
    query: 1,
    control: 1,
    kv: 1,
    fork: 1,
    graph: 1,
    agent: 1,
    filter: 1,
    features: 0n
  },
  filters: { native: false, catalog: true, groupPolicyReads: true, maxWaitMs: 0 }
}
const nativeCapabilities: Capabilities = { ...OPEN_CAPABILITIES, hello: "rejected" }
// Transport members a handle reads without sending anything.
const LOCAL_MEMBERS = new Set([
  "then",
  "kind",
  "iggyClient",
  "nodes",
  "connectsNodes",
  "openCoordinator",
  "publishRetriesManaged",
  "publishOptions",
  "publishTimeoutMs",
  "seenPositions",
  "observePositions"
])

void test("given_an_aborted_signal_when_each_streaming_call_runs_then_should_reject_with_cancelled_before_any_io", async () => {
  const calls: string[] = []
  const transport = recordingTransport(calls)
  const groups: GroupContext = {
    transport,
    capabilities: () => {
      calls.push("capabilities")
      return Promise.resolve(capabilities)
    }
  }
  const resolveSchema = (): Promise<undefined> => {
    calls.push("resolveSchema")
    return Promise.resolve(undefined)
  }
  const topic = Topic.create(
    transport,
    "orbit",
    "fleet",
    undefined,
    resolveSchema,
    undefined,
    groups
  )
  const group = topic.consumerGroup("workers")
  const filter = group.filter()
  const consumer = topic.consumer("reader", 0)
  const producer = topic.producer()
  const batching = topic.batching().build()
  const typed = topic.json(new Json())
  const cursor = Cursor.create(transport, "orbit", "fleet", [0])
  const records = TypedRecords.create(Cursor.create(transport, "orbit", "fleet", [0]), new Json())
  const stream = Stream.create(transport, "orbit")
  const message = new ProducerMessage(Uint8Array.of(1))
  const spec = { kind: "definition" as const, name: "safe-mode", filter: definition }
  const options = { signal: AbortSignal.abort() }
  const calling: readonly (readonly [string, () => Promise<unknown>])[] = [
    ["Consumer.commit", () => consumer.commit({} as ConsumerMessage, options)],
    ["Consumer.storeOffset", () => consumer.storeOffset(1n, 0, options)],
    ["Consumer.deleteOffset", () => consumer.deleteOffset(0, options)],
    ["Consumer.storedOffset", () => consumer.storedOffset(0, options)],
    ["Consumer.seek", () => consumer.seek(0, { kind: "next" }, options)],
    ["Consumer.skip", () => consumer.skip(0, 1n, options)],
    ["Consumer.nextWithin", () => consumer.nextWithin(10, options)],
    ["Consumer.stream", () => consumer.stream(options)[Symbol.asyncIterator]().next()],
    ["ConsumerGroup.create", () => group.create(options)],
    ["ConsumerGroup.info", () => group.info(options)],
    ["ConsumerGroup.consumer", () => group.consumer(options)],
    ["GroupFilter.configure", () => filter.configure("safe-mode", definition, options)],
    ["GroupFilter.configureWith", () => filter.configureWith(spec, options)],
    ["GroupFilter.configureAs", () => filter.configureAs(11n, spec, options)],
    ["GroupFilter.get", () => filter.get(options)],
    ["GroupFilter.summary", () => filter.summary(options)],
    ["GroupFilter.revisions", () => filter.revisions(undefined, undefined, options)],
    ["GroupFilter.setRevisionEnabled", () => filter.setRevisionEnabled(1, false, options)],
    ["GroupFilter.delete", () => filter.delete(options)],
    ["GroupFilter.release", () => filter.release(options)],
    ["GroupFilter.preview", () => filter.preview(0, options)],
    ["GroupFilter.test", () => filter.test("{}", [], options)],
    ["Producer.send", () => producer.send(Uint8Array.of(1), options)],
    ["Producer.sendMessage", () => producer.sendMessage(message, undefined, options)],
    [
      "Producer.sendWithRouting",
      () => producer.sendWithRouting(message, { kind: "balanced" }, options)
    ],
    ["Producer.sendKeyed", () => producer.sendKeyed(message, Uint8Array.of(9), options)],
    ["Producer.sendToPartition", () => producer.sendToPartition(message, 0, options)],
    ["Producer.sendBatch", () => producer.sendBatch([Uint8Array.of(1)], options)],
    [
      "Producer.sendBatchWithRouting",
      () => producer.sendBatchWithRouting([message], undefined, options)
    ],
    ["Producer.flush", () => producer.flush(options)],
    ["BatchingProducer.send", () => batching.send(Uint8Array.of(1), undefined, options)],
    ["BatchingProducer.flush", () => batching.flush(options)],
    ["Topic.send", () => topic.send(Uint8Array.of(1), options)],
    ["Topic.batch", () => topic.batch([Uint8Array.of(1)], options)],
    ["Topic.ensure", () => topic.ensure(1, undefined, options)],
    ["Topic.ensureConsumerGroup", () => topic.ensureConsumerGroup("workers", options)],
    ["Topic.replay", () => topic.replay(options)],
    ["Topic.schema", () => topic.schema(1, new Json(), options)],
    ["PublishRequest.send", () => topic.publish().payload(Uint8Array.of(1)).send(options)],
    [
      "BatchPublishRequest.send",
      () => topic.publishBatch().addPayload(Uint8Array.of(1)).send(options)
    ],
    ["TypedTopic.publish", () => typed.publish({ mode: "safe" }, options)],
    ["TypedTopic.publishBatch", () => typed.publishBatch([{ mode: "safe" }], options)],
    ["TypedTopic.records", () => typed.records("reader", options)],
    ["TypedRecords.next", () => records.next(options)],
    ["TypedRecords.poll", () => records.poll(options)],
    ["Cursor.poll", () => cursor.poll(options)],
    ["Cursor.stream", () => cursor.stream(options)[Symbol.asyncIterator]().next()],
    ["Stream.ensure", () => stream.ensure(options)],
    ["Stream.delete", () => stream.delete(options)]
  ]
  const failures: string[] = []
  try {
    for (const [name, call] of calling) {
      try {
        await call()
        failures.push(`${name} resolved`)
      } catch (error) {
        if (!(error instanceof CancelledError)) failures.push(`${name} threw ${String(error)}`)
      }
    }
    assert.deepEqual(failures, [])
    assert.deepEqual(calls, [])
  } finally {
    await batching.close().catch(() => undefined)
    await producer.shutdown().catch(() => undefined)
    await consumer.shutdown().catch(() => undefined)
  }
})

void test("given_a_producer_send_in_flight_when_the_signal_aborts_then_should_reject_with_cancelled", async () => {
  const controller = new AbortController()
  let sends = 0
  const producer = Producer.create(
    transportWithSend(() => {
      sends += 1
      return answeredAfterAbort(controller.signal, { confirmations: [] })
    }),
    "orbit",
    "fleet",
    { createTopic: false }
  )
  const sending = producer.send(Uint8Array.of(1), { signal: controller.signal })
  await settled()
  assert.equal(sends, 1)
  controller.abort()
  await assert.rejects(sending, CancelledError)
})

void test("given_a_failed_send_waiting_out_its_retry_backoff_when_the_signal_aborts_then_should_reject_and_send_no_retry", async () => {
  const controller = new AbortController()
  let attempts = 0
  const producer = Producer.create(
    transportWithSend(() => {
      attempts += 1
      return Promise.reject(new TransportError("transient", true))
    }),
    "orbit",
    "fleet",
    { retries: 1, retryBackoffMs: 1_000, createTopic: false }
  )
  const sending = producer.send(Uint8Array.of(1), { signal: controller.signal })
  await settled()
  assert.equal(attempts, 1)
  controller.abort()
  await assert.rejects(sending, CancelledError)
  await pause(20)
  assert.equal(attempts, 1, "no attempt starts after the abort")
})

void test("given_a_send_waiting_out_the_linger_gap_when_the_signal_aborts_then_should_reject_and_send_nothing", async () => {
  const controller = new AbortController()
  let sends = 0
  const producer = Producer.create(
    transportWithSend(() => {
      sends += 1
      return Promise.resolve({ confirmations: [] })
    }),
    "orbit",
    "fleet",
    { lingerMs: 1_000, createTopic: false }
  )
  await producer.send(Uint8Array.of(1))
  const sending = producer.send(Uint8Array.of(2), { signal: controller.signal })
  await settled()
  controller.abort()
  await assert.rejects(sending, CancelledError)
  assert.equal(sends, 1)
})

void test("given_a_background_send_waiting_for_buffer_room_when_the_signal_aborts_then_should_reject_and_queue_nothing", async () => {
  const controller = new AbortController()
  const written: number[] = []
  const producer = Producer.create(
    transportWithSend((_stream, _topic, messages) => {
      written.push(messages.length)
      return Promise.resolve({ confirmations: [] })
    }),
    "orbit",
    "fleet",
    { createTopic: false, background: { maxBufferBytes: 1, lingerMs: 60_000 } }
  )
  await producer.send(Uint8Array.of(1))
  const sending = producer.send(Uint8Array.of(2), { signal: controller.signal })
  await settled()
  controller.abort()
  const closing = producer.shutdown()
  await assert.rejects(sending, CancelledError)
  await closing
  assert.deepEqual(written, [1], "only the record queued before the abort is written")
})

void test("given_a_group_join_waiting_out_its_init_retry_interval_when_the_signal_aborts_then_should_reject_and_join_no_more", async () => {
  const controller = new AbortController()
  let joins = 0
  const transport = {
    joinConsumerGroup: () => {
      joins += 1
      return Promise.reject(
        new TransportError("stream missing", false, { cause: { errorCode: 1009 } })
      )
    }
  } as unknown as LaserTransport
  const consuming = nativeGroup(transport).consumer({
    initRetries: { retries: 1, intervalMs: 1_000 },
    signal: controller.signal
  })
  await settled()
  assert.equal(joins, 1)
  controller.abort()
  await assert.rejects(consuming, CancelledError)
  await pause(20)
  assert.equal(joins, 1, "no join starts after the abort")
})

void test("given_a_group_join_in_flight_when_the_signal_aborts_then_should_reject_and_leave_the_group_once_joined", async () => {
  const controller = new AbortController()
  let joined: (() => void) | undefined
  let leaves = 0
  const transport = {
    joinConsumerGroup: () =>
      new Promise<void>((resolve) => {
        joined = resolve
      }),
    leaveConsumerGroup: () => {
      leaves += 1
      return Promise.resolve()
    },
    findTopicPartitionCount: () => Promise.resolve(0)
  } as unknown as LaserTransport
  const consuming = nativeGroup(transport).consumer({ signal: controller.signal })
  await settled()
  controller.abort()
  joined?.()
  await assert.rejects(consuming, CancelledError)
  await settled()
  assert.equal(leaves, 1, "a membership the abandoned join made is left")
})

void test("given_a_group_policy_configuring_when_the_signal_aborts_then_should_reject_with_cancelled_not_a_setup_error", async () => {
  const controller = new AbortController()
  const fixture = managedGroup((code) =>
    code === AGDX_FILTER_MUTATE_CODE
      ? answeredAfterAbort(controller.signal, applied(11n))
      : undefined
  )
  const creating = fixture.group.create({
    filter: definition,
    filterName: "safe-mode",
    operationId: 11n,
    signal: controller.signal
  })
  await settled()
  assert.equal(fixture.codes.at(-1), AGDX_FILTER_MUTATE_CODE)
  controller.abort()
  await assert.rejects(creating, CancelledError)
})

void test("given_a_group_filter_read_waiting_for_its_reply_when_the_signal_aborts_then_should_reject_with_cancelled", async () => {
  const controller = new AbortController()
  const fixture = managedGroup((code) =>
    code === AGDX_GET_FILTER_BINDING_CODE
      ? answeredAfterAbort(
          controller.signal,
          encodeNamed(
            encodeFilterCatalogReply({ kind: "ok", outcome: { kind: "binding", binding } })
          )
        )
      : undefined
  )
  const reading = fixture.group.filter().get({ signal: controller.signal })
  await settled()
  assert.equal(fixture.codes.at(-1), AGDX_GET_FILTER_BINDING_CODE)
  controller.abort()
  await assert.rejects(reading, CancelledError)
})

void test("given_a_topic_send_in_flight_when_the_signal_aborts_then_should_reject_with_cancelled_not_a_publish_failure", async () => {
  const controller = new AbortController()
  let sends = 0
  const transport = {
    sendMessages: () => {
      sends += 1
      return answeredAfterAbort(controller.signal, { confirmations: [] })
    }
  } as unknown as LaserTransport
  const sending = Topic.create(transport, "orbit", "fleet").send(Uint8Array.of(1), {
    signal: controller.signal
  })
  await settled()
  assert.equal(sends, 1)
  controller.abort()
  await assert.rejects(sending, CancelledError)
})

void test("given_an_offset_store_in_flight_when_the_signal_aborts_then_should_reject_with_cancelled", async () => {
  const controller = new AbortController()
  let stores = 0
  const transport = {
    storeOffset: () => {
      stores += 1
      return answeredAfterAbort(controller.signal, undefined)
    }
  } as unknown as LaserTransport
  const consumer = Consumer.create(transport, "orbit", "fleet", {
    kind: "single",
    name: "reader",
    partitionId: 0
  })
  const storing = consumer.storeOffset(4n, 0, { signal: controller.signal })
  await settled()
  assert.equal(stores, 1)
  controller.abort()
  try {
    await assert.rejects(storing, CancelledError)
  } finally {
    await consumer.shutdown()
  }
})

void test("given_a_batching_flush_in_flight_when_the_signal_aborts_then_should_reject_with_cancelled", async () => {
  const controller = new AbortController()
  const batching = BatchingProducerBuilder.create(
    () => answeredAfterAbort(controller.signal, undefined),
    "orbit",
    "fleet"
  ).build()
  await batching.send(Uint8Array.of(1))
  const flushing = batching.flush({ signal: controller.signal })
  await settled()
  controller.abort()
  try {
    await assert.rejects(flushing, CancelledError)
  } finally {
    await batching.close()
  }
})

void test("given_an_aborted_batching_flush_when_its_batch_fails_then_should_keep_the_failure_for_the_next_flush", async () => {
  const controller = new AbortController()
  const failure = new PublishFailedError(
    "orbit",
    "fleet",
    [],
    [{ payload: Uint8Array.of(1), headers: new Map() }],
    new TransportError("refused", false)
  )
  const batching = BatchingProducerBuilder.create(
    () => failedAfterAbort(controller.signal, failure),
    "orbit",
    "fleet"
  )
    .linger(60_000)
    .build()
  await batching.send(Uint8Array.of(1))
  const flushing = batching.flush({ signal: controller.signal })
  await settled()
  controller.abort()
  await assert.rejects(flushing, CancelledError)
  await assert.rejects(batching.flush(), (error: unknown) => error === failure)
  await batching.close()
})

void test("given_a_publish_waiting_out_its_retry_delay_when_the_signal_aborts_then_should_reject_and_send_no_retry", async () => {
  const header = new Map([["origin", { kind: "string" as const, value: "test" }]])
  const publishing: readonly (readonly [
    string,
    (topic: Topic, signal: AbortSignal) => Promise<unknown>
  ])[] = [
    ["Topic.send", (topic, signal) => topic.send(Uint8Array.of(1), { partition: 0, signal })],
    [
      "Topic.send with headers",
      (topic, signal) => topic.send(Uint8Array.of(1), { partition: 0, headers: header, signal })
    ],
    ["Topic.batch", (topic, signal) => topic.batch([Uint8Array.of(1)], { partition: 0, signal })],
    [
      "PublishRequest.send",
      (topic, signal) => topic.publish().partition(0).payload(Uint8Array.of(1)).send({ signal })
    ],
    [
      "BatchPublishRequest.send",
      (topic, signal) => topic.publishBatch().addPayload(Uint8Array.of(1)).send({ signal })
    ],
    [
      "Producer.send",
      (topic, signal) =>
        topic.producer({ createTopic: false }).send(Uint8Array.of(1), { partition: 0, signal })
    ],
    [
      "BatchingProducer.send",
      (topic, signal) =>
        topic.batching().maxRecords(1).linger(60_000).build().send(Uint8Array.of(1), header, {
          signal
        })
    ],
    [
      "BatchingProducer.flush",
      async (topic, signal) => {
        const batching = topic.batching().linger(60_000).build()
        await batching.send(Uint8Array.of(1))
        return batching.flush({ signal })
      }
    ]
  ]
  for (const [name, publish] of publishing) {
    const controller = new AbortController()
    let attempts = 0
    const transport = await ApacheIggyTransport.fromClient(
      refusingClient(() => {
        attempts += 1
      }),
      "owned",
      { timeoutMs: 1_000, maxRetries: 3, retryBackoffMs: 40 }
    )
    try {
      const sending = publish(Topic.create(transport, "orbit", "fleet"), controller.signal)
      for (let turn = 0; attempts === 0 && turn < 100; turn += 1) await settled()
      await settled()
      assert.equal(attempts, 1, `${name} sends its first attempt`)
      controller.abort()
      await assert.rejects(sending, CancelledError, name)
      await pause(100)
      assert.equal(attempts, 1, `${name} sends no attempt after the abort`)
    } finally {
      await transport.close()
    }
  }
})

// A client whose every send is refused with a transient server code.
function refusingClient(sent: () => void): IggyClient {
  return {
    clientProvider: () => Promise.resolve({}),
    topic: { get: () => Promise.resolve({ partitionsCount: 1 }) },
    message: {
      send: () => {
        sent()
        return Promise.reject(Object.assign(new Error("not accepted"), { errorCode: 58 }))
      }
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
}

// A transport that records the name of every call it is sent.
function recordingTransport(calls: string[]): LaserTransport {
  return new Proxy(
    {},
    {
      get: (_target, name) =>
        typeof name !== "string" || LOCAL_MEMBERS.has(name)
          ? undefined
          : () => {
              calls.push(name)
              return Promise.resolve(undefined)
            }
    }
  ) as LaserTransport
}

function transportWithSend(send: LaserTransport["sendMessagesWithHeaders"]): LaserTransport {
  return { sendMessagesWithHeaders: send } as unknown as LaserTransport
}

function nativeGroup(transport: LaserTransport): ConsumerGroup {
  return ConsumerGroup.create(
    transport,
    Topic.create(transport, "orbit", "fleet"),
    { kind: "name", name: "workers" },
    { transport, capabilities: () => Promise.resolve(nativeCapabilities) }
  )
}

// A managed group whose stream, topic and group metadata answer at once, and
// whose other commands `reply` answers.
function managedGroup(reply: (code: number) => Promise<Uint8Array> | undefined): {
  readonly group: ConsumerGroup
  readonly codes: readonly number[]
} {
  const codes: number[] = []
  const transport = {
    ensureConsumerGroup: () => Promise.resolve(),
    sendManaged: (code: number): Promise<Uint8Array> => {
      codes.push(code)
      if (code === 200) return Promise.resolve(metadata(1, 1n))
      if (code === 300) return Promise.resolve(metadata(2, 2n))
      if (code === 600) return Promise.resolve(groupMetadata(7))
      return reply(code) ?? Promise.reject(new Error(`unexpected command ${code.toString()}`))
    }
  } as unknown as LaserTransport
  const group = ConsumerGroup.create(
    transport,
    Topic.create(transport, "orbit", "fleet"),
    { kind: "name", name: "workers" },
    { transport, capabilities: () => Promise.resolve(capabilities) }
  )
  return { group, codes }
}

function metadata(id: number, created: bigint): Uint8Array {
  const value = new Uint8Array(12)
  const view = new DataView(value.buffer)
  view.setUint32(0, id, true)
  view.setBigUint64(4, created, true)
  return value
}

function groupMetadata(id: number): Uint8Array {
  const name = new TextEncoder().encode("workers")
  const value = new Uint8Array(13 + name.byteLength)
  const view = new DataView(value.buffer)
  view.setUint32(0, id, true)
  view.setUint8(12, name.byteLength)
  value.set(name, 13)
  return value
}

function applied(operationId: bigint): Uint8Array {
  return encodeNamed(
    encodeFilterCatalogReply({
      kind: "ok",
      outcome: {
        kind: "mutation",
        outcome: {
          v: FILTER_OP_VERSION,
          operationId,
          status: { kind: "applied", result: { kind: "bound", binding } },
          catalogPosition: { partitionId: 0, offset: 41n, operationId }
        }
      }
    })
  )
}

// A reply that lands shortly after `signal` aborts, as one already sent does.
function answeredAfterAbort<T>(signal: AbortSignal, value: T): Promise<T> {
  return new Promise((resolve) => {
    signal.addEventListener(
      "abort",
      () => {
        setTimeout(() => {
          resolve(value)
        }, 20)
      },
      { once: true }
    )
  })
}

// A refusal that lands shortly after `signal` aborts.
function failedAfterAbort(signal: AbortSignal, error: Error): Promise<never> {
  return new Promise((_resolve, reject) => {
    signal.addEventListener(
      "abort",
      () => {
        setTimeout(() => {
          reject(error)
        }, 20)
      },
      { once: true }
    )
  })
}

function settled(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve))
}

function pause(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

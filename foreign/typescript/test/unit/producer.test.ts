import assert from "node:assert/strict"
import { test } from "node:test"

import {
  InvalidError,
  PublishFailedError,
  TimeoutError,
  TransportError
} from "../../src/client/errors.js"
import type { LaserTransport } from "../../src/iggy/apache-iggy.js"
import { Producer, ProducerMessage } from "../../src/stream/producer.js"

function transportWithSend(send: LaserTransport["sendMessagesWithHeaders"]): LaserTransport {
  return { sendMessagesWithHeaders: send } as unknown as LaserTransport
}

void test("given_retryable_transport_failures_when_sending_then_should_retry_to_the_configured_limit", async () => {
  let attempts = 0
  const transport = transportWithSend(() => {
    attempts += 1
    return attempts < 3
      ? Promise.reject(new TransportError("transient", true))
      : Promise.resolve({ confirmations: [] })
  })
  const producer = Producer.create(transport, "stream", "topic", {
    retries: 2,
    retryBackoffMs: 1,
    createTopic: false,
    createStream: false
  })
  await producer.send(new Uint8Array([1]))
  assert.equal(attempts, 3)
})

void test("given_a_non_retryable_transport_failure_when_sending_then_should_not_retry", async () => {
  let attempts = 0
  const transport = transportWithSend(() => {
    attempts += 1
    return Promise.reject(new TransportError("permanent", false))
  })
  const producer = Producer.create(transport, "stream", "topic", {
    retries: 3,
    retryBackoffMs: 1,
    createTopic: false,
    createStream: false
  })
  await assert.rejects(
    producer.send(new Uint8Array([1])),
    (error: unknown) =>
      error instanceof PublishFailedError &&
      error.publishCause() instanceof TransportError &&
      error.committed.length === 0 &&
      error.unconfirmed.length === 1
  )
  assert.equal(attempts, 1)
})

void test("given_a_producer_when_asynchronously_disposed_then_should_reject_further_sends", async () => {
  const producer = Producer.create(
    transportWithSend(() => Promise.resolve({ confirmations: [] })),
    "stream",
    "topic"
  )

  await producer[Symbol.asyncDispose]()

  await assert.rejects(producer.send(new Uint8Array([1])), /called after shutdown/)
})

void test("given_a_committed_send_when_publishing_then_should_return_the_confirmation", async () => {
  const confirmation = {
    streamId: 1,
    topicId: 2,
    partitionId: 3,
    baseOffset: 4n
  }
  const producer = Producer.create(
    transportWithSend(() => Promise.resolve({ confirmations: [confirmation] })),
    "stream",
    "topic",
    { createTopic: false, createStream: false }
  )

  const response = await producer.send(new Uint8Array([1]))

  assert.deepEqual(response.confirmations, [confirmation])
})

void test("given_transport_managed_retries_when_producer_overrides_them_then_should_not_multiply_attempts", async () => {
  let sends = 0
  const transport = {
    publishRetriesManaged: true,
    sendMessagesWithHeaders: (...args: Parameters<LaserTransport["sendMessagesWithHeaders"]>) => {
      sends += 1
      assert.deepEqual(args[5], { batchLength: 1000, maxRetries: 2, retryBackoffMs: 7 })
      return Promise.reject(new TransportError("exhausted", true))
    }
  } as unknown as LaserTransport
  const producer = Producer.create(transport, "stream", "topic", {
    retries: 2,
    retryBackoffMs: 7,
    createTopic: false,
    createStream: false
  })
  await assert.rejects(producer.send(new Uint8Array([1])), PublishFailedError)
  assert.equal(sends, 1)
})

void test("given_a_producer_with_defaults_when_sending_first_then_should_create_the_stream_and_topic_once", async () => {
  const ensured: string[] = []
  const transport = {
    ensureStream: (stream: string) => {
      ensured.push(`stream:${stream}`)
      return Promise.resolve()
    },
    ensureTopic: (stream: string, topic: string, partitions: number) => {
      ensured.push(`topic:${stream}/${topic}/${String(partitions)}`)
      return Promise.resolve()
    },
    sendMessagesWithHeaders: () => Promise.resolve({ confirmations: [] })
  } as unknown as LaserTransport
  const producer = Producer.create(transport, "fleet", "readings", { partitions: 3 })
  await producer.send(new Uint8Array([1]))
  await producer.send(new Uint8Array([2]))
  assert.deepEqual(ensured, ["stream:fleet", "topic:fleet/readings/3"])
})

void test("given_invalid_routing_keys_or_retry_interval_when_configured_then_should_reject_before_io", async () => {
  const transport = transportWithSend(() => Promise.reject(new Error("must not send")))
  assert.throws(
    () => Producer.create(transport, "s", "t", { routing: { kind: "key", key: new Uint8Array() } }),
    InvalidError
  )
  assert.throws(() => Producer.create(transport, "s", "t", { retryBackoffMs: 0 }), InvalidError)
  const producer = Producer.create(transport, "s", "t", { createStream: false, createTopic: false })
  await assert.rejects(
    producer.send(new Uint8Array([1]), { key: new Uint8Array(256) }),
    InvalidError
  )
  await assert.rejects(
    producer.sendKeyed(new ProducerMessage(new Uint8Array([1])), new Uint8Array()),
    InvalidError
  )
})

void test("given_a_failed_chunk_when_sending_then_should_report_the_ids_every_record_was_sent_with", async () => {
  const sent: bigint[] = []
  const transport = transportWithSend((_stream, _topic, messages) => {
    for (const message of messages) sent.push(message.id ?? 0n)
    return sent.length > 1
      ? Promise.reject(new TransportError("down", false))
      : Promise.resolve({ confirmations: [] })
  })
  const producer = Producer.create(transport, "s", "t", {
    batchLength: 1,
    createStream: false,
    createTopic: false
  })
  await assert.rejects(
    producer.sendBatch([new Uint8Array([1]), new Uint8Array([2]), new Uint8Array([3])]),
    (error: unknown) => {
      assert.ok(error instanceof PublishFailedError)
      assert.equal(error.unconfirmed.length, 2)
      assert.equal(error.unconfirmed[0]?.id, sent[1])
      assert.ok(error.unconfirmed.every((record) => (record.id ?? 0n) > 0n))
      return true
    }
  )
  assert.equal(sent.length, 2)
})

void test("given_a_full_background_buffer_when_sending_then_should_follow_the_failure_mode", async () => {
  const release: (() => void)[] = []
  const transport = transportWithSend(
    () =>
      new Promise((resolve) => {
        release.push(() => {
          resolve({ confirmations: [] })
        })
      })
  )
  const options = (
    failureMode:
      | { readonly kind: "failImmediately" }
      | { readonly kind: "blockWithTimeout"; readonly timeoutMs: number }
  ) => ({
    createStream: false,
    createTopic: false,
    background: { lingerMs: 0, maxBufferBytes: 4, failureMode }
  })
  const immediate = Producer.create(transport, "s", "t", options({ kind: "failImmediately" }))
  await immediate.send(new Uint8Array(4))
  await assert.rejects(
    immediate.send(new Uint8Array(1)),
    (error: unknown) =>
      error instanceof PublishFailedError &&
      error.publishCause() instanceof TransportError &&
      error.unconfirmed.length === 1
  )
  const bounded = Producer.create(
    transport,
    "s",
    "t",
    options({ kind: "blockWithTimeout", timeoutMs: 5 })
  )
  await bounded.send(new Uint8Array(4))
  await assert.rejects(
    bounded.send(new Uint8Array(1)),
    (error: unknown) =>
      error instanceof PublishFailedError && error.publishCause() instanceof TimeoutError
  )
  for (const resolve of release.splice(0)) resolve()
  await immediate.shutdown()
  await bounded.shutdown()
})

void test("given_balanced_background_shards_when_sending_then_should_write_up_to_max_in_flight_at_once", async () => {
  let writing = 0
  let peak = 0
  const release: (() => void)[] = []
  const transport = transportWithSend(() => {
    writing += 1
    peak = Math.max(peak, writing)
    return new Promise((resolve) => {
      release.push(() => {
        writing -= 1
        resolve({ confirmations: [] })
      })
    })
  })
  const producer = Producer.create(transport, "s", "t", {
    createStream: false,
    createTopic: false,
    background: { shards: 3, sharding: "balanced", maxInFlight: 2, lingerMs: 0 }
  })
  for (let index = 0; index < 3; index += 1) await producer.send(new Uint8Array([index]))
  await new Promise((resolve) => setImmediate(resolve))
  assert.equal(peak, 2)
  const flushed = producer.flush()
  while (writing > 0 || release.length > 0) {
    for (const resolve of release.splice(0)) resolve()
    await new Promise((resolve) => setImmediate(resolve))
  }
  await flushed
  assert.equal(peak, 2)
  await producer.shutdown()
})

function serverError(code: number): TransportError {
  return new TransportError("server refused", false, { cause: { errorCode: code } })
}

void test("given_a_transient_setup_failure_when_sending_then_should_retry_setup_within_the_publish_budget", async () => {
  let ensures = 0
  let sends = 0
  const transport = {
    ensureStream: () => {
      ensures += 1
      return ensures === 1
        ? Promise.reject(new TransportError("transient", true))
        : Promise.resolve()
    },
    ensureTopic: () => Promise.resolve(),
    sendMessagesWithHeaders: () => {
      sends += 1
      return Promise.resolve({ confirmations: [] })
    }
  } as unknown as LaserTransport
  await Producer.create(transport, "stream", "topic", { retries: 1, retryBackoffMs: 1 }).send(
    new Uint8Array([1])
  )
  assert.equal(ensures, 2)
  assert.equal(sends, 1)

  ensures = 0
  await assert.rejects(
    Producer.create(transport, "stream", "topic", { retries: 0, retryBackoffMs: 1 }).send(
      new Uint8Array([1])
    ),
    (error: unknown) => error instanceof PublishFailedError
  )
  assert.equal(ensures, 1)
})

void test("given_a_lost_topic_creation_race_when_sending_then_should_retry_setup_and_succeed", async () => {
  let creates = 0
  const transport = {
    ensureStream: () => Promise.resolve(),
    createTopicIfAbsent: () => {
      creates += 1
      return creates < 3 ? Promise.reject(serverError(2013)) : Promise.resolve()
    },
    sendMessagesWithHeaders: () => Promise.resolve({ confirmations: [] })
  } as unknown as LaserTransport
  await Producer.create(transport, "stream", "topic", { retries: 0 }).send(new Uint8Array([1]))
  assert.equal(creates, 3)
})

void test("given_a_background_producer_when_writes_fail_then_should_resend_at_once_then_at_the_fixed_interval", async () => {
  const at: number[] = []
  const transport = transportWithSend(() => {
    at.push(Date.now())
    return at.length < 4
      ? Promise.reject(serverError(2010))
      : Promise.resolve({ confirmations: [] })
  })
  const producer = Producer.create(transport, "stream", "topic", {
    retries: 3,
    retryBackoffMs: 60,
    createTopic: false,
    createStream: false,
    background: { lingerMs: 0 }
  })
  await producer.send(new Uint8Array([1]))
  await producer.shutdown()
  assert.equal(at.length, 4)
  const gaps = at.slice(1).map((time, index) => time - (at[index] ?? time))
  assert.ok((gaps[0] ?? 0) < 30, `first resend waited ${String(gaps[0])} ms`)
  for (const gap of gaps.slice(1)) assert.ok(gap >= 55 && gap < 110, `resend gap ${String(gap)} ms`)
})

void test("given_a_background_producer_when_the_confirmation_is_lost_then_should_not_resend", async () => {
  let attempts = 0
  const failures: PublishFailedError[] = []
  const transport = transportWithSend(() => {
    attempts += 1
    return Promise.reject(serverError(303))
  })
  const producer = Producer.create(transport, "stream", "topic", {
    retries: 3,
    retryBackoffMs: 1,
    createTopic: false,
    createStream: false,
    background: {
      lingerMs: 0,
      onError: (error) => {
        failures.push(error)
      }
    }
  })
  await producer.send(new Uint8Array([1]))
  await producer.shutdown()
  assert.equal(attempts, 1)
  assert.equal(failures.length, 1)
})

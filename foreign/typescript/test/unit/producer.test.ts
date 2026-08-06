import assert from "node:assert/strict"
import { test } from "node:test"

import { TransportError } from "../../src/client/errors.js"
import type { LaserTransport } from "../../src/iggy/apache-iggy.js"
import { Producer } from "../../src/stream/producer.js"

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
  const producer = new Producer(transport, "stream", "topic", {
    retries: 2,
    retryIntervalMs: 0
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
  const producer = new Producer(transport, "stream", "topic", {
    retries: 3,
    retryIntervalMs: 0
  })
  await assert.rejects(producer.send(new Uint8Array([1])), TransportError)
  assert.equal(attempts, 1)
})

void test("given_a_producer_when_asynchronously_disposed_then_should_reject_further_sends", async () => {
  const producer = new Producer(
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
  const producer = new Producer(
    transportWithSend(() => Promise.resolve({ confirmations: [confirmation] })),
    "stream",
    "topic"
  )

  const response = await producer.send(new Uint8Array([1]))

  assert.deepEqual(response.confirmations, [confirmation])
})

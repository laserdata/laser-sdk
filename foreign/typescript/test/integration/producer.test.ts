import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"

import { Laser } from "../../src/client/laser.js"
import { HeaderValue } from "../../src/stream/header-value.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

const utf8 = (text: string) => new TextEncoder().encode(text)
const decodeUtf8 = (bytes: Uint8Array) => new TextDecoder().decode(bytes)

async function freshTopic(laser: Laser, partitions = 1) {
  const streamName = `laser-ts-test-${randomUUID()}`
  const topic = laser.stream(streamName).topic("events")
  await laser.stream(streamName).ensure()
  await topic.ensure(partitions)
  return topic
}

void test("given_a_direct_producer_when_send_resolves_then_should_make_the_record_visible", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const producer = topic.producer()
    await producer.send(utf8("direct"))

    const message = await topic.consumer(0, { startFrom: { kind: "first" } }).nextWithin(1_000)
    assert.ok(message !== null)
    assert.equal(decodeUtf8(message.payload), "direct")
    await producer.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_direct_producer_when_a_batch_is_sent_then_should_use_one_ordered_send", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const producer = topic.producer()
    const count = await producer.sendBatch([utf8("x"), utf8("y")])
    assert.equal(count, 2)

    const consumer = topic.consumer(0, { startFrom: { kind: "first" }, batchLength: 2 })
    const first = await consumer.nextWithin(1_000)
    const second = await consumer.nextWithin(1_000)
    assert.ok(first !== null)
    assert.ok(second !== null)
    assert.deepEqual([decodeUtf8(first.payload), decodeUtf8(second.payload)], ["x", "y"])
    await producer.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_structured_keyed_message_when_sent_then_should_preserve_binary_key_and_header", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const producer = topic.producer({ retries: 1, retryIntervalMs: 1 })
    await producer.sendKeyed(
      { payload: utf8("typed"), headers: { type: HeaderValue.uint16(7) } },
      new Uint8Array([0xff, 0x00, 0x61])
    )

    const message = await topic.consumer(0, { startFrom: { kind: "first" } }).nextWithin(1_000)
    assert.ok(message !== null)
    assert.equal(decodeUtf8(message.payload), "typed")
    assert.deepEqual(message.headers.get("type"), { kind: "uint16", value: 7 })
    await producer.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_shutdown_direct_producer_when_reused_then_should_reject", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const producer = (await freshTopic(laser)).producer()
    await producer.flush()
    await producer.shutdown()
    await assert.rejects(producer.send(utf8("after-shutdown")), /after shutdown/)
    await assert.rejects(producer.flush(), /after shutdown/)
  } finally {
    await laser.close()
  }
})

void test("given_invalid_direct_producer_controls_when_created_then_should_reject_before_io", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    assert.throws(() => topic.producer({ retries: -1 }), /retries/)
    assert.throws(() => topic.producer({ retryIntervalMs: Number.NaN }), /retryIntervalMs/)
    assert.throws(
      () => topic.producer({ routing: { kind: "partition", partition: -1 } }),
      /partition/
    )
  } finally {
    await laser.close()
  }
})

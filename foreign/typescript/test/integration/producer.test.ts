import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"

import { Laser } from "../../src/client/laser.js"
import { HeaderValue } from "../../src/stream/header-value.js"
import { ProducerMessage } from "../../src/stream/producer.js"

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

void test("given_concurrent_first_publishes_when_provisioning_one_topic_then_should_deliver_every_record", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  const streamName = `laser-ts-test-${randomUUID()}`
  await laser.stream(streamName).ensure()
  const topic = laser.stream(streamName).topic("events")
  const producers = Array.from({ length: 8 }, () => topic.producer({ createStream: false }))
  try {
    await Promise.all(producers.map((producer, index) => producer.send(utf8(String(index)))))
    const consumer = topic.consumer(0, {
      startAt: { kind: "first" },
      commitPolicy: { kind: "disabled" }
    })
    const seen = new Set<string>()
    for (let remaining = producers.length; remaining > 0; remaining -= 1) {
      seen.add(decodeUtf8((await consumer.nextWithin(1_000)).payload))
    }
    assert.equal(seen.size, producers.length)
    await consumer.shutdown()
  } finally {
    await Promise.all(producers.map((producer) => producer.shutdown()))
  }
})

void test("given_a_direct_producer_when_send_resolves_then_should_make_the_record_visible", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const producer = topic.producer()
    await producer.send(utf8("direct"))

    const message = await topic.consumer(0, { startAt: { kind: "first" } }).nextWithin(1_000)
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
    const committed = await producer.sendBatch([utf8("x"), utf8("y")])
    assert.equal(committed.confirmations.length, 1)
    assert.equal(committed.confirmations[0]?.partitionId, 0)

    const consumer = topic.consumer(0, { startAt: { kind: "first" }, batchLength: 2 })
    const first = await consumer.nextWithin(1_000)
    const second = await consumer.nextWithin(1_000)
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
    const producer = topic.producer({ retries: 1, retryBackoffMs: 1 })
    await producer.sendKeyed(
      new ProducerMessage(utf8("typed")).header("type", HeaderValue.uint16(7)),
      new Uint8Array([0xff, 0x00, 0x61])
    )

    const message = await topic.consumer(0, { startAt: { kind: "first" } }).nextWithin(1_000)
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
    assert.throws(() => topic.producer({ retryBackoffMs: Number.NaN }), /retryBackoffMs/)
    assert.throws(
      () => topic.producer({ routing: { kind: "partition", partition: -1 } }),
      /partition/
    )
  } finally {
    await laser.close()
  }
})

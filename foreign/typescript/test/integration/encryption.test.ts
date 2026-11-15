import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"

import { iggyErrorCode } from "../../src/client/error-classify.js"
import { TransportError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { Encryption } from "../../src/stream/encryption.js"
import { ProducerMessage, type ShardRequest } from "../../src/stream/producer.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const KEY = new TextEncoder().encode("laser-encryption-test-key-32byte")
const PLAINTEXT = new TextEncoder().encode('{"host":"node-7","cpu":82}')

async function freshTopic(laser: Laser) {
  const streamName = `laser-ts-test-${randomUUID()}`
  const topic = laser.stream(streamName).topic("events")
  await laser.stream(streamName).ensure()
  await topic.ensure(1)
  return { streamName, topic }
}

void test("given_an_encrypted_producer_when_read_with_and_without_the_key_then_should_store_only_ciphertext", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  const { topic } = await freshTopic(laser)
  const encryption = Encryption.aes256Gcm(KEY)
  const producer = topic.producer({ encryption })
  await producer.sendMessage(
    new ProducerMessage(PLAINTEXT).header("source", { kind: "string", value: "node-7" })
  )
  await producer.shutdown()

  const plain = topic.consumer("without-key", 0, { startAt: { kind: "first" } })
  const stored = await plain.nextWithin(5_000)
  await plain.shutdown()
  assert.notDeepEqual(stored.payload, PLAINTEXT)
  assert.deepEqual(encryption.decrypt(stored.payload), PLAINTEXT)
  assert.equal(stored.headers.size, 0)

  const opened = topic.consumer("with-key", 0, { startAt: { kind: "first" }, encryption })
  const record = await opened.nextWithin(5_000)
  await opened.shutdown()
  assert.deepEqual(record.payload, PLAINTEXT)
  assert.deepEqual(record.headers.get("source"), { kind: "string", value: "node-7" })
})

void test("given_an_encrypted_group_consumer_when_reading_then_should_see_the_plaintext", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  const { topic } = await freshTopic(laser)
  const encryption = Encryption.aes256Gcm(KEY)
  const producer = topic.producer({ encryption })
  await producer.send(PLAINTEXT)
  await producer.shutdown()
  const consumer = await topic
    .consumerGroup(`g-${randomUUID()}`)
    .consumer({ startAt: { kind: "first" }, encryption })
  try {
    assert.deepEqual((await consumer.nextWithin(5_000)).payload, PLAINTEXT)
  } finally {
    await consumer.shutdown()
  }
})

void test("given_a_record_that_does_not_decrypt_when_read_then_should_report_it_once_and_read_past", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  const { topic } = await freshTopic(laser)
  const encryption = Encryption.aes256Gcm(KEY)
  const plain = topic.producer()
  await plain.send(new TextEncoder().encode("not sealed"), { partition: 0 })
  await plain.shutdown()
  const sealed = topic.producer({ encryption })
  await sealed.send(PLAINTEXT, { partition: 0 })
  await sealed.shutdown()
  const consumer = topic.consumer("mixed", 0, { startAt: { kind: "first" }, encryption })
  try {
    await assert.rejects(
      consumer.nextWithin(5_000),
      (error: unknown) => error instanceof TransportError && iggyErrorCode(error) === 72
    )
    const record = await consumer.nextWithin(5_000)
    assert.deepEqual(record.payload, PLAINTEXT)
    assert.equal(record.position.offset, 1n)
  } finally {
    await consumer.shutdown()
  }
})

void test("given_a_sharding_function_when_sending_in_background_then_should_route_each_send_through_it", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  const { streamName, topic } = await freshTopic(laser)
  const seen: ShardRequest[] = []
  const producer = topic.producer({
    routing: { kind: "partition", partition: 0 },
    background: { shards: 4 },
    sharding: (request) => {
      seen.push(request)
      return request.records % request.shards
    }
  })
  const encoder = new TextEncoder()
  await producer.sendBatch([encoder.encode("one"), encoder.encode("two"), encoder.encode("three")])
  await producer.shutdown()
  assert.deepEqual(seen, [{ shards: 4, stream: streamName, topic: "events", records: 3 }])

  const consumer = topic.consumer("sharded-reader", 0, { startAt: { kind: "first" } })
  try {
    const payloads: string[] = []
    for (let read = 0; read < 3; read += 1) {
      payloads.push(new TextDecoder().decode((await consumer.nextWithin(5_000)).payload))
    }
    assert.deepEqual(payloads, ["one", "two", "three"])
  } finally {
    await consumer.shutdown()
  }
})

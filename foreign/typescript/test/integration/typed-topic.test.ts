import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { jsonCodec } from "../../src/stream/codecs.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

interface Order {
  readonly id: string
  readonly total: number
}

function decodeOrder(value: unknown): Order {
  if (
    value === null ||
    typeof value !== "object" ||
    !("id" in value) ||
    typeof value.id !== "string" ||
    !("total" in value) ||
    typeof value.total !== "number"
  ) {
    throw new TypeError("order requires string id and numeric total")
  }
  return { id: value.id, total: value.total }
}

async function freshTopic(laser: Laser) {
  const streamName = `laser-ts-test-${randomUUID()}`
  const topic = laser.stream(streamName).topic("events")
  await laser.stream(streamName).ensure()
  await topic.ensure(1)
  return topic
}

void test("given_a_typed_topic_when_publishing_and_reading_records_then_should_decode_the_value_and_position", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const orders = topic.json(jsonCodec(decodeOrder))
    await orders.publish({ id: "o-1", total: 42 })

    const records = await orders.records("orders-one")
    const results = await records.poll()
    assert.equal(results.length, 1)
    const [result] = results
    assert.ok(result?.kind === "record")
    assert.deepEqual(result.record.value, { id: "o-1", total: 42 })
    assert.equal(result.record.partitionId, 0)
    assert.deepEqual(result.record.position, { partitionId: 0, offset: 0n })
    assert.deepEqual(result.record.headers.get("agdx.ct"), { kind: "uint8", value: 1 })
  } finally {
    await laser.close()
  }
})

void test("given_a_typed_topic_when_publishing_a_batch_then_should_decode_every_value_in_order", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const orders = topic.json(jsonCodec(decodeOrder))
    const count = await orders.publishBatch([
      { id: "o-1", total: 1 },
      { id: "o-2", total: 2 }
    ])
    assert.equal(count, 2)

    const records = await orders.records("orders-batch")
    const results = await records.poll()
    const values = results.map((result) => (result.kind === "record" ? result.record.value : null))
    assert.deepEqual(values, [
      { id: "o-1", total: 1 },
      { id: "o-2", total: 2 }
    ])
    for (const result of results) {
      assert.ok(result.kind === "record")
      assert.deepEqual(result.record.headers.get("agdx.ct"), { kind: "uint8", value: 1 })
    }
  } finally {
    await laser.close()
  }
})

void test("given_a_poison_record_among_good_ones_when_polled_then_should_report_its_position_and_keep_reading", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const orders = topic.json(jsonCodec(decodeOrder))

    await topic.send(new TextEncoder().encode(JSON.stringify({ id: "o-1", total: 1 })))
    await topic.send(new TextEncoder().encode("not valid json"))
    await topic.send(new TextEncoder().encode(JSON.stringify({ id: "o-3", total: 3 })))

    const records = await orders.records("orders-poison")
    const results = await records.poll()
    assert.equal(results.length, 3)

    const [first, second, third] = results
    assert.ok(first?.kind === "record")
    assert.equal(first.record.value.id, "o-1")

    assert.ok(second?.kind === "error")
    assert.ok(second.error.position !== undefined)
    assert.equal(second.error.position.partitionId, 0)
    assert.equal(second.error.position.offset, 1n)

    assert.ok(third?.kind === "record")
    assert.equal(third.record.value.id, "o-3")

    const caughtUp = await records.poll()
    assert.deepEqual(caughtUp, [])
  } finally {
    await laser.close()
  }
})

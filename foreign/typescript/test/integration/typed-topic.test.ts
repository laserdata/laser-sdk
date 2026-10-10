import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { Json } from "../../src/stream/codecs.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

interface Reading {
  readonly host: string
  readonly cpu: number
}

function decodeReading(value: unknown): Reading {
  if (
    value === null ||
    typeof value !== "object" ||
    !("host" in value) ||
    typeof value.host !== "string" ||
    !("cpu" in value) ||
    typeof value.cpu !== "number"
  ) {
    throw new TypeError("a reading requires a string host and a numeric cpu")
  }
  return { host: value.host, cpu: value.cpu }
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
    const readings = topic.json(new Json(decodeReading))
    await readings.publish({ host: "node-1", cpu: 42 })

    const records = await readings.records("readings-one")
    const results = await records.poll()
    assert.equal(results.length, 1)
    const [result] = results
    assert.ok(result?.kind === "record")
    assert.deepEqual(result.record.value, { host: "node-1", cpu: 42 })
    assert.equal(result.record.position.partitionId, 0)
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
    const readings = topic.json(new Json(decodeReading))
    const committed = await readings.publishBatch([
      { host: "node-1", cpu: 1 },
      { host: "node-2", cpu: 2 }
    ])
    assert.equal(committed.confirmations.length, 1)
    assert.equal(committed.confirmations[0]?.partitionId, 0)

    const records = await readings.records("readings-batch")
    const results = await records.poll()
    const values = results.map((result) => (result.kind === "record" ? result.record.value : null))
    assert.deepEqual(values, [
      { host: "node-1", cpu: 1 },
      { host: "node-2", cpu: 2 }
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
    const readings = topic.json(new Json(decodeReading))

    await topic.send(new TextEncoder().encode(JSON.stringify({ host: "node-1", cpu: 1 })))
    await topic.send(new TextEncoder().encode("not valid json"))
    await topic.send(new TextEncoder().encode(JSON.stringify({ host: "node-3", cpu: 3 })))

    const records = await readings.records("readings-poison")
    const results = await records.poll()
    assert.equal(results.length, 3)

    const [first, second, third] = results
    assert.ok(first?.kind === "record")
    assert.equal(first.record.value.host, "node-1")

    assert.ok(second?.kind === "error")
    assert.ok(second.error.position !== undefined)
    assert.equal(second.error.position.partitionId, 0)
    assert.equal(second.error.position.offset, 1n)

    assert.ok(third?.kind === "record")
    assert.equal(third.record.value.host, "node-3")

    const caughtUp = await records.poll()
    assert.deepEqual(caughtUp, [])
  } finally {
    await laser.close()
  }
})

void test("given_a_typed_reader_when_calling_next_then_should_yield_each_record_and_undefined_when_caught_up", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const readings = topic.json(new Json(decodeReading))
    await readings.publishBatch([
      { host: "node-1", cpu: 1 },
      { host: "node-2", cpu: 2 }
    ])

    const records = await readings.records("readings-next")
    assert.deepEqual(records.offsets, new Map())
    const first = await records.next()
    assert.ok(first?.kind === "record")
    assert.equal(first.record.value.host, "node-1")
    const second = await records.next()
    assert.ok(second?.kind === "record")
    assert.equal(second.record.value.host, "node-2")
    assert.equal(await records.next(), undefined)
    assert.deepEqual(records.offsets, new Map([[0, 2n]]))
  } finally {
    await laser.close()
  }
})

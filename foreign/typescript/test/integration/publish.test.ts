import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { Consumer, PollingStrategy } from "apache-iggy"
import { test } from "node:test"
import { PolicyBlockedError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { ActionDecision, GovernorMode, type ActionGovernor } from "../../src/govern.js"
import { jsonCodec } from "../../src/stream/codecs.js"
import { Record } from "../../src/stream/record.js"
import { ContentType } from "../../src/wire/content.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

interface Order {
  readonly id: string
}

function decodeOrder(value: unknown): Order {
  if (
    value === null ||
    typeof value !== "object" ||
    !("id" in value) ||
    typeof value.id !== "string"
  ) {
    throw new TypeError("order requires a string id")
  }
  return { id: value.id }
}

async function freshTopic(laser: Laser, partitions = 1) {
  const streamName = `laser-ts-test-${randomUUID()}`
  const topic = laser.stream(streamName).topic("events")
  await laser.stream(streamName).ensure()
  await topic.ensure(partitions)
  return topic
}

async function pollFirst(
  laser: Laser,
  streamName: string,
  partitionId: number,
  topicName = "events"
) {
  return laser.iggyClient.message.poll({
    streamId: streamName,
    topicId: topicName,
    consumer: Consumer.Single,
    partitionId,
    pollingStrategy: PollingStrategy.First,
    count: 10,
    autocommit: false
  })
}

void test("given_a_publish_request_with_a_raw_payload_when_sent_then_should_deliver_it", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.publish().payload(new TextEncoder().encode("raw-body")).send()

    const reply = await pollFirst(laser, topic.streamName, 0)
    assert.equal(reply.count, 1)
    assert.equal(reply.messages[0]?.payload.toString("utf8"), "raw-body")
  } finally {
    await laser.close()
  }
})

void test("given_a_publish_request_with_a_json_body_when_sent_then_should_deliver_the_encoded_value", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.publish().json({ id: "o-1" }, jsonCodec(decodeOrder)).send()

    const reply = await pollFirst(laser, topic.streamName, 0)
    assert.equal(reply.count, 1)
    assert.deepEqual(JSON.parse(reply.messages[0]?.payload.toString("utf8") ?? "null"), {
      id: "o-1"
    })
  } finally {
    await laser.close()
  }
})

void test("given_a_publish_request_when_the_last_body_setter_wins_then_should_send_only_that_body", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic
      .publish()
      .payload(new TextEncoder().encode("first-body"))
      .json({ id: "second-body" }, jsonCodec(decodeOrder))
      .send()

    const reply = await pollFirst(laser, topic.streamName, 0)
    assert.equal(reply.count, 1)
    assert.deepEqual(JSON.parse(reply.messages[0]?.payload.toString("utf8") ?? "null"), {
      id: "second-body"
    })
  } finally {
    await laser.close()
  }
})

void test("given_a_publish_request_with_an_explicit_partition_when_sent_then_should_land_on_that_partition", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser, 2)
    await topic.publish().partition(1).payload(new TextEncoder().encode("on-partition-one")).send()

    const reply = await pollFirst(laser, topic.streamName, 1)
    assert.equal(reply.count, 1)
    assert.equal(reply.messages[0]?.payload.toString("utf8"), "on-partition-one")
  } finally {
    await laser.close()
  }
})

void test("given_a_publish_request_with_no_body_when_sent_then_should_reject_locally", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await assert.rejects(topic.publish().send(), /requires a body/)
  } finally {
    await laser.close()
  }
})

void test("given_a_complete_record_when_published_then_should_preserve_every_contract_header", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic
      .publish()
      .rawBytes(new TextEncoder().encode('{"id":"o-7"}'), ContentType.Json)
      .projectionRef("orders.v1")
      .schemaId(7)
      .index("id", "o-7")
      .header("trace", "abc")
      .inlinePayload()
      .send()

    const message = await topic.consumer(0, { startFrom: { kind: "first" } }).nextWithin(1_000)
    assert.ok(message !== null)
    assert.deepEqual(message.headers.get("agdx.ct"), { kind: "uint8", value: 1 })
    assert.deepEqual(message.headers.get("agdx.sid"), { kind: "uint32", value: 7 })
    assert.deepEqual(message.headers.get("agdx.inline"), { kind: "bool", value: true })
    assert.deepEqual(message.headers.get("agdx.ref"), { kind: "string", value: "orders.v1" })
    assert.deepEqual(message.headers.get("agdx.idx.id"), { kind: "string", value: "o-7" })
    assert.deepEqual(message.headers.get("trace"), { kind: "string", value: "abc" })
  } finally {
    await laser.close()
  }
})

void test("given_a_heterogeneous_publish_batch_when_sent_then_should_apply_defaults_and_record_overrides", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const empty = await topic.publishBatch().send()
    assert.equal(empty, 0)
    const count = await topic
      .publishBatch()
      .contentType(ContentType.Json)
      .schemaId(7)
      .index("tenant", "shared")
      .addPayload(new TextEncoder().encode("one"))
      .addRecord(
        new TextEncoder().encode("two"),
        new Record().contentType(ContentType.Cbor).schemaId(9).index("tenant", "override")
      )
      .send()
    assert.equal(count, 2)

    const consumer = topic.consumer(0, { startFrom: { kind: "first" }, batchLength: 2 })
    const first = await consumer.nextWithin(1_000)
    const second = await consumer.nextWithin(1_000)
    assert.ok(first !== null)
    assert.ok(second !== null)
    assert.deepEqual(first.headers.get("agdx.ct"), { kind: "uint8", value: 1 })
    assert.deepEqual(first.headers.get("agdx.sid"), { kind: "uint32", value: 7 })
    assert.deepEqual(second.headers.get("agdx.ct"), { kind: "uint8", value: 3 })
    assert.deepEqual(second.headers.get("agdx.sid"), { kind: "uint32", value: 9 })
    assert.deepEqual(second.headers.get("agdx.idx.tenant"), {
      kind: "string",
      value: "override"
    })
  } finally {
    await laser.close()
  }
})

void test("given_a_blocking_governor_when_a_publish_batch_is_sent_then_should_write_no_records", async () => {
  const streamName = `laser-ts-test-${randomUUID()}`
  const connected = await Laser.connectWithStream(CONNECTION_STRING, streamName)
  const governor: ActionGovernor = {
    decide: () => Promise.resolve(ActionDecision.block("batch writes are blocked"))
  }
  const laser = connected.withGovernor(governor, GovernorMode.Enforce)
  try {
    await laser.bootstrap(1)
    const topic = laser.topic("business.audit")
    await topic.ensure(1)

    await assert.rejects(
      topic
        .publishBatch()
        .addPayload(new TextEncoder().encode("first"))
        .addPayload(new TextEncoder().encode("second"))
        .send(),
      PolicyBlockedError
    )

    const reply = await pollFirst(laser, streamName, 0, "business.audit")
    assert.equal(reply.count, 0)
  } finally {
    await connected.close()
  }
})

import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { Consumer, PollingStrategy } from "apache-iggy"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

void test("given_a_sent_message_when_polled_back_through_the_raw_client_then_should_match_the_payload", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const streamName = `laser-ts-test-${randomUUID()}`
    const topic = laser.stream(streamName).topic("events")
    await laser.stream(streamName).ensure()
    await topic.ensure(1)

    await topic.send(new TextEncoder().encode("hello from laser-sdk"))

    const reply = await laser.iggyClient.message.poll({
      streamId: streamName,
      topicId: "events",
      consumer: Consumer.Single,
      partitionId: 0,
      pollingStrategy: PollingStrategy.First,
      count: 10,
      autocommit: false
    })

    assert.equal(reply.count, 1)
    assert.equal(reply.messages[0]?.payload.toString("utf8"), "hello from laser-sdk")
  } finally {
    await laser.close()
  }
})

void test("given_a_topic_batch_call_when_sent_then_should_deliver_every_message_and_return_the_count", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const streamName = `laser-ts-test-${randomUUID()}`
    const topic = laser.stream(streamName).topic("events")
    await laser.stream(streamName).ensure()
    await topic.ensure(1)

    const count = await topic.batch([
      new TextEncoder().encode("one"),
      new TextEncoder().encode("two"),
      new TextEncoder().encode("three")
    ])
    assert.equal(count, 3)

    const reply = await laser.iggyClient.message.poll({
      streamId: streamName,
      topicId: "events",
      consumer: Consumer.Single,
      partitionId: 0,
      pollingStrategy: PollingStrategy.First,
      count: 10,
      autocommit: false
    })
    assert.equal(reply.count, 3)
    assert.deepEqual(
      reply.messages.map((message) => message.payload.toString("utf8")),
      ["one", "two", "three"]
    )
  } finally {
    await laser.close()
  }
})

void test("given_a_send_with_conflicting_routing_options_when_called_then_should_reject_locally", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const streamName = `laser-ts-test-${randomUUID()}`
    const topic = laser.stream(streamName).topic("events")
    await laser.stream(streamName).ensure()
    await topic.ensure(1)

    await assert.rejects(
      topic.send(new Uint8Array([1]), { key: new Uint8Array([1]), partition: 1 }),
      /routing key or an explicit partition, not both/
    )
  } finally {
    await laser.close()
  }
})

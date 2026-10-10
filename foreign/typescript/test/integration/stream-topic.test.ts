import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { NoStreamError } from "../../src/client/errors.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { TopicRetention } from "../../src/session.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

void test("given_a_new_stream_and_topic_when_ensured_then_should_create_and_be_idempotent", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const streamName = `laser-ts-test-${randomUUID()}`
    const stream = laser.stream(streamName)
    await stream.ensure()
    await stream.ensure()

    const topic = stream.topic("events")
    await topic.ensure(1)
    await topic.ensure(1)

    assert.equal(stream.name, streamName)
    assert.equal(topic.streamName, streamName)
    assert.equal(topic.name, "events")
  } finally {
    await laser.close()
  }
})

void test("given_no_default_stream_when_topic_is_called_then_should_throw_no_stream_error", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    assert.throws(() => laser.topic("events"), NoStreamError)
  } finally {
    await laser.close()
  }
})

void test("given_a_default_stream_when_topic_is_called_then_should_use_it", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const streamName = `laser-ts-test-${randomUUID()}`
    const scoped = laser.withDefaultStream(streamName)
    await scoped.stream(streamName).ensure()
    const topic = scoped.topic("events")
    assert.equal(topic.streamName, streamName)
  } finally {
    await laser.close()
  }
})

void test("given_an_ensured_stream_when_deleted_then_should_report_absence_on_repeat", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const stream = laser.stream(`laser-ts-test-${randomUUID()}`)
    await stream.ensure()
    await stream.topic("events").ensure(1)
    assert.equal(await stream.delete(), true)
    assert.equal(await stream.delete(), false)
  } finally {
    await laser.close()
  }
})

void test("given_agent_bootstrap_when_called_then_should_create_seven_topics_per_partition_count", async () => {
  const streamName = `laser-ts-test-${randomUUID()}`
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, streamName)
  try {
    await laser.bootstrap(2, TopicRetention.expireAfter(86_400_000))
    const topics = await laser.client.topic.list({ streamId: streamName })
    assert.deepEqual(topics.map((topic) => topic.name).sort(), [
      AgentTopic.Audit,
      AgentTopic.Dlq,
      AgentTopic.Heartbeats,
      AgentTopic.Memory,
      AgentTopic.Sessions,
      AgentTopic.Streams,
      AgentTopic.WorkflowJournal
    ])
    assert.equal(
      topics.every((topic) => topic.partitionsCount === 2),
      true
    )
    const expiry = async (name: string) =>
      (await laser.client.topic.get({ streamId: streamName, topicId: name }))?.messageExpiry
    assert.equal(await expiry(AgentTopic.Sessions), 86_400_000_000n)
    assert.equal(await expiry(AgentTopic.Heartbeats), 3_600_000_000n)
  } finally {
    await laser.stream(streamName).delete()
  }
})

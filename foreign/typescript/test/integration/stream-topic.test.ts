import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { InvalidError, NoStreamError } from "../../src/client/errors.js"
import { isStreamOrTopicNotFound } from "../../src/client/error-classify.js"
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

void test("given_a_unique_stream_when_listed_and_deleted_then_should_report_it_until_gone", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  const name = `laser-ts-test-${randomUUID()}`
  const stream = laser.stream(name)
  try {
    assert.equal(await stream.get(), undefined)
    assert.equal(await stream.exists(), false)
    await stream.topic("metrics").ensure(2)

    const listed = (await laser.streams()).filter((info) => info.name === name)
    assert.equal(listed.length, 1)
    const info = listed.find((candidate) => candidate.name === name)
    assert.ok(info !== undefined)
    assert.equal(info.topicsCount, 1)
    assert.ok(info.createdAtMicros > 0n)
    assert.deepEqual(await stream.get(), info)
    assert.equal(await stream.exists(), true)

    const topics = await stream.topics()
    assert.deepEqual(
      topics.map((topic) => [topic.name, topic.partitionsCount, topic.messageExpiryMs]),
      [["metrics", 2, undefined]]
    )

    assert.equal(await stream.delete(), true)
    assert.equal(await stream.exists(), false)
    assert.ok((await laser.streams()).every((info) => info.name !== name))
  } finally {
    await stream.delete()
    await laser.close()
  }
})

void test("given_a_missing_stream_when_topics_or_groups_listed_then_should_fail_as_not_found", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const stream = laser.stream(`laser-ts-test-${randomUUID()}`)
    await assert.rejects(stream.topics(), (error: unknown) => isStreamOrTopicNotFound(error))
    await assert.rejects(stream.topic("jobs").consumerGroups(), (error: unknown) =>
      isStreamOrTopicNotFound(error)
    )
    assert.equal(await stream.delete(), false)
  } finally {
    await laser.close()
  }
})

void test("given_a_topic_when_partitions_changed_and_deleted_then_should_report_each_state", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  const stream = laser.stream(`laser-ts-test-${randomUUID()}`)
  try {
    const topic = stream.topic("hosts")
    assert.equal(await topic.get(), undefined)
    assert.equal(await topic.delete(), false)
    await topic.ensure(1)
    await topic.addPartitions(2)
    assert.equal((await topic.get())?.partitionsCount, 3)
    assert.equal((await topic.partitions()).length, 3)
    await topic.deletePartitions(1)
    assert.equal((await topic.partitions()).length, 2)
    for (const change of [
      () => topic.addPartitions(0),
      () => topic.deletePartitions(0),
      () => topic.deleteSegments(1, 0)
    ])
      await assert.rejects(
        change(),
        (error: unknown) =>
          error instanceof InvalidError && error.message === "count must be greater than zero"
      )
    assert.equal(await topic.delete(), true)
    assert.equal(await topic.exists(), false)
    await assert.rejects(topic.partitions(), (error: unknown) => isStreamOrTopicNotFound(error))
  } finally {
    await stream.delete()
    await laser.close()
  }
})

void test("given_a_consumer_group_when_listed_and_deleted_then_should_report_it_until_gone", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  const stream = laser.stream(`laser-ts-test-${randomUUID()}`)
  try {
    const topic = stream.topic("jobs")
    await topic.ensure(1)
    await topic.ensureConsumerGroup("workers")
    const groups = await topic.consumerGroups()
    assert.deepEqual(
      groups.map((group) => [group.name, group.membersCount]),
      [["workers", 0]]
    )
    const group = topic.consumerGroup("workers")
    assert.equal(await group.delete(), true)
    assert.equal(await group.delete(), false)
    assert.deepEqual(await topic.consumerGroups(), [])
  } finally {
    await stream.delete()
    await laser.close()
  }
})

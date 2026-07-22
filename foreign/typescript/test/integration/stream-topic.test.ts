import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { NoStreamError } from "../../src/client/errors.js"

const CONNECTION_STRING =
  process.env["LASER_CONNECTION_STRING"] ?? "iggy://iggy:iggy@127.0.0.1:8090"

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

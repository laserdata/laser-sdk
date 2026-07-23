import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

const utf8 = (text: string) => new TextEncoder().encode(text)
const decodeUtf8 = (bytes: Uint8Array) => new TextDecoder().decode(bytes)

async function freshTopic(laser: Laser) {
  const streamName = `laser-ts-test-${randomUUID()}`
  const topic = laser.stream(streamName).topic("events")
  await laser.stream(streamName).ensure()
  await topic.ensure(1)
  return topic
}

void test("given_sent_messages_when_a_cursor_polls_then_should_return_them_from_the_start", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.batch([utf8("one"), utf8("two"), utf8("three")])

    const cursor = await topic.replay()
    const batch = await cursor.poll()
    assert.deepEqual(
      batch.map((message) => decodeUtf8(message.payload)),
      ["one", "two", "three"]
    )
  } finally {
    await laser.close()
  }
})

void test("given_a_caught_up_cursor_when_polled_again_then_should_return_an_empty_batch_not_an_error", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(utf8("only"))

    const cursor = await topic.replay()
    const first = await cursor.poll()
    assert.equal(first.length, 1)
    const second = await cursor.poll()
    assert.deepEqual(second, [])
  } finally {
    await laser.close()
  }
})

void test("given_a_snapshot_of_offsets_when_a_new_cursor_resumes_then_should_continue_after_it", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(utf8("alpha"))

    const first = await topic.replay()
    const firstBatch = await first.poll()
    assert.equal(firstBatch.length, 1)
    const snapshot = first.offsets

    await topic.send(utf8("beta"))

    const resumed = await topic.replay()
    resumed.fromOffsets(snapshot)
    const resumedBatch = await resumed.poll()
    assert.deepEqual(
      resumedBatch.map((message) => decodeUtf8(message.payload)),
      ["beta"]
    )
  } finally {
    await laser.close()
  }
})

void test("given_a_cursor_stream_when_a_message_is_sent_later_then_should_yield_it_without_terminating", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(utf8("first"))

    const cursor = await topic.replay({ batchSize: 10 })
    const iterator = cursor.stream({ pollIntervalMs: 50 })[Symbol.asyncIterator]()

    const first = await iterator.next()
    assert.equal(first.done, false)
    assert.equal(decodeUtf8(first.value.payload), "first")

    await topic.send(utf8("second"))
    const second = await iterator.next()
    assert.equal(second.done, false)
    assert.equal(decodeUtf8(second.value.payload), "second")
  } finally {
    await laser.close()
  }
})

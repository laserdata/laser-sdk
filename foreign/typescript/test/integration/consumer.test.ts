import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { CancelledError } from "../../src/client/errors.js"

const CONNECTION_STRING =
  process.env["LASER_CONNECTION_STRING"] ?? "iggy://iggy:iggy@127.0.0.1:8090"

const utf8 = (text: string) => new TextEncoder().encode(text)
const decodeUtf8 = (bytes: Uint8Array) => new TextDecoder().decode(bytes)

async function freshTopic(laser: Laser) {
  const streamName = `laser-ts-test-${randomUUID()}`
  const topic = laser.stream(streamName).topic("events")
  await laser.stream(streamName).ensure()
  await topic.ensure(1)
  return topic
}

void test("given_several_sent_messages_when_consumed_with_next_within_then_should_return_them_in_order", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(utf8("first"))
    await topic.send(utf8("second"))
    await topic.send(utf8("third"))

    const consumer = topic.consumer(0, { startFrom: { kind: "first" } })
    const first = await consumer.nextWithin(2_000)
    const second = await consumer.nextWithin(2_000)
    const third = await consumer.nextWithin(2_000)

    assert.ok(first !== null)
    assert.ok(second !== null)
    assert.ok(third !== null)
    assert.equal(decodeUtf8(first.payload), "first")
    assert.equal(decodeUtf8(second.payload), "second")
    assert.equal(decodeUtf8(third.payload), "third")
    assert.ok(second.offset > first.offset)
    assert.ok(third.offset > second.offset)
  } finally {
    await laser.close()
  }
})

void test("given_no_messages_when_polling_with_next_within_then_should_return_null_on_timeout", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const consumer = topic.consumer(0, { startFrom: { kind: "first" }, pollIntervalMs: 50 })
    const message = await consumer.nextWithin(200)
    assert.equal(message, null)
  } finally {
    await laser.close()
  }
})

void test("given_manual_commit_when_the_consumer_is_recreated_then_should_resume_after_the_committed_offset", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(utf8("one"))
    await topic.send(utf8("two"))

    const first = topic.consumer(0, { startFrom: { kind: "first" }, autoCommit: false })
    const one = await first.nextWithin(2_000)
    assert.ok(one !== null)
    assert.equal(decodeUtf8(one.payload), "one")
    await first.commit(one)

    const resumed = topic.consumer(0, { startFrom: { kind: "next" }, autoCommit: false })
    const two = await resumed.nextWithin(2_000)
    assert.ok(two !== null)
    assert.equal(decodeUtf8(two.payload), "two")
  } finally {
    await laser.close()
  }
})

void test("given_a_live_consumer_when_iterated_with_for_await_then_should_yield_every_message", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(utf8("alpha"))
    await topic.send(utf8("beta"))

    const consumer = topic.consumer(0, { startFrom: { kind: "first" }, pollIntervalMs: 50 })
    const seen: string[] = []
    for await (const message of consumer) {
      seen.push(decodeUtf8(message.payload))
      if (seen.length === 2) {
        await consumer.shutdown()
      }
    }
    assert.deepEqual(seen, ["alpha", "beta"])
  } finally {
    await laser.close()
  }
})

void test("given_a_named_consumer_when_committed_then_should_report_local_and_server_offsets", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(utf8("tracked"))
    const consumer = topic.consumer("tracked-reader", 0, {
      startFrom: { kind: "first" },
      autoCommit: false
    })
    const message = await consumer.nextWithin(1_000)
    assert.ok(message !== null)
    assert.equal(consumer.lastConsumedOffset(0), message.offset)
    await consumer.commit(message)
    const offset = await consumer.storedOffset(0)
    assert.equal(offset?.storedOffset, message.offset)
  } finally {
    await laser.close()
  }
})

void test("given_an_aborted_consumer_stream_when_waiting_then_should_fail_as_cancelled", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const consumer = topic.consumer("cancelled-reader", 0, { pollIntervalMs: 10 })
    const controller = new AbortController()
    controller.abort("stop")
    const iterator = consumer.stream({ signal: controller.signal })[Symbol.asyncIterator]()
    await assert.rejects(iterator.next(), CancelledError)
  } finally {
    await laser.close()
  }
})

void test("given_invalid_consumer_controls_when_created_then_should_reject_before_io", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    assert.throws(() => topic.consumer(-1), /partition/)
    assert.throws(() => topic.consumer(0, { batchLength: 0 }), /batchLength/)
    assert.throws(() => topic.consumer(0, { pollIntervalMs: -1 }), /pollIntervalMs/)
    assert.throws(
      () => topic.consumer(0, { startFrom: { kind: "timestamp", value: -1n } }),
      /start/
    )
    const consumer = topic.consumer(0)
    await assert.rejects(consumer.nextWithin(Number.NaN), /timeout/)
  } finally {
    await laser.close()
  }
})

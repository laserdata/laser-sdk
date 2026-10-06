import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { setTimeout as delay } from "node:timers/promises"
import { Consumer, PollingStrategy } from "apache-iggy"
import { Laser } from "../../src/client/laser.js"
import { CancelledError, TimeoutError } from "../../src/client/errors.js"

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

void test("given_several_sent_messages_when_consumed_with_next_within_then_should_return_them_in_order", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(utf8("first"))
    await topic.send(utf8("second"))
    await topic.send(utf8("third"))

    const consumer = topic.consumer(0, { startAt: { kind: "first" } })
    const first = await consumer.nextWithin(2_000)
    const second = await consumer.nextWithin(2_000)
    const third = await consumer.nextWithin(2_000)

    assert.equal(decodeUtf8(first.payload), "first")
    assert.equal(decodeUtf8(second.payload), "second")
    assert.equal(decodeUtf8(third.payload), "third")
    assert.ok(second.position.offset > first.position.offset)
    assert.ok(third.position.offset > second.position.offset)
  } finally {
    await laser.close()
  }
})

void test("given_no_messages_when_polling_with_next_within_then_should_fail_with_a_timeout", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const consumer = topic.consumer(0, { startAt: { kind: "first" }, pollIntervalMs: 50 })
    await assert.rejects(consumer.nextWithin(200), TimeoutError)
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

    const first = topic.consumer("resume-reader", 0, {
      startAt: { kind: "first" },
      commitPolicy: { kind: "disabled" }
    })
    const one = await first.nextWithin(2_000)
    assert.equal(decodeUtf8(one.payload), "one")
    await first.commit(one)
    await first.shutdown()

    const resumed = topic.consumer("resume-reader", 0, {
      startAt: { kind: "next" },
      commitPolicy: { kind: "disabled" }
    })
    const two = await resumed.nextWithin(2_000)
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

    const consumer = topic.consumer(0, { startAt: { kind: "first" }, pollIntervalMs: 50 })
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
      startAt: { kind: "first" },
      commitPolicy: { kind: "disabled" }
    })
    const message = await consumer.nextWithin(1_000)
    assert.equal(consumer.lastConsumedOffset(0), message.position.offset)
    await consumer.commit(message)
    const offset = await consumer.storedOffset(0)
    assert.equal(offset?.storedOffset, message.position.offset)
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
      () => topic.consumer(0, { startAt: { kind: "timestampMicros", value: -1n } }),
      /start/
    )
    const consumer = topic.consumer(0)
    await assert.rejects(consumer.nextWithin(Number.NaN), /timeout/)
  } finally {
    await laser.close()
  }
})

void test("given_a_fresh_consumer_when_reading_next_then_should_start_at_zero", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(utf8("zero"))
    await topic.send(utf8("one"))
    const options = { commitPolicy: { kind: "disabled" as const }, batchLength: 1 }
    const first = topic.consumer("fresh", 0, options)
    assert.equal((await first.nextWithin(2_000)).position.offset, 0n)
    await first.shutdown()
    const retried = topic.consumer("fresh", 0, options)
    const zero = await retried.nextWithin(2_000)
    assert.equal(zero.position.offset, 0n)
    await retried.commit(zero)
    await retried.shutdown()
    const resumed = topic.consumer("fresh", 0, options)
    assert.equal((await resumed.nextWithin(2_000)).position.offset, 1n)
    await resumed.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_default_polling_when_shutdown_after_offset_zero_then_should_resume_at_one", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    for (const value of ["zero", "one", "two", "three"]) await topic.send(utf8(value))
    const first = topic.consumer("partial", 0, { batchLength: 4 })
    assert.equal((await first.nextWithin(2_000)).position.offset, 0n)
    assert.equal(first.lastConsumedOffset(0), 0n)
    await first.shutdown()
    const resumed = topic.consumer("partial", 0, { batchLength: 4 })
    assert.equal((await resumed.nextWithin(2_000)).position.offset, 1n)
    await resumed.shutdown()
  } finally {
    await laser.close()
  }
})

// A purge restarts the partition at offset zero and clears every stored
// offset. A consumer rebuilt after the purge reads the replacement history
// from its first record, whatever its commit mode or start position.
void test("given_a_purged_topic_when_the_consumer_is_rebuilt_then_should_start_at_the_new_offset_zero", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const variants = [
      { group: false, options: {} },
      { group: false, options: { commitPolicy: { kind: "disabled" as const } } },
      { group: false, options: { startAt: { kind: "first" as const } } },
      { group: true, options: {} },
      { group: true, options: { commitPolicy: { kind: "disabled" as const } } }
    ]
    for (const { group, options } of variants) {
      const streamName = `laser-ts-test-${randomUUID()}`
      const topic = laser.stream(streamName).topic("telemetry")
      await laser.stream(streamName).ensure()
      await topic.ensure(1)
      for (const value of ["telemetry-0", "telemetry-1", "telemetry-2"])
        await topic.send(utf8(value))
      const open = () =>
        group
          ? topic.consumerGroup("ground-station").consumer({ ...options, batchLength: 3 })
          : Promise.resolve(topic.consumer("ground-station", 0, { ...options, batchLength: 3 }))
      const before = await open()
      for (let expected = 0n; expected < 3n; expected++) {
        const record = await before.nextWithin(2_000)
        assert.equal(record.position.offset, expected)
        if ("commitPolicy" in options) await before.commit(record)
      }
      await before.shutdown()

      await laser.client.topic.purge({ streamId: streamName, topicId: "telemetry" })
      // Metadata commits before the owner applies the purge. Wait for the empty source before publishing its replacement.
      const deadline = performance.now() + 5_000
      for (;;) {
        const polled = await laser.client.message.poll({
          streamId: streamName,
          topicId: "telemetry",
          partitionId: 0,
          consumer: Consumer.Single,
          pollingStrategy: PollingStrategy.First,
          count: 3,
          autocommit: false
        })
        if (polled.count === 0) break
        assert(performance.now() < deadline, "the owner did not apply the purge")
        await delay(10)
      }
      for (const value of ["safe-mode-0", "safe-mode-1"]) await topic.send(utf8(value))

      const after = await open()
      try {
        const record = await after.nextWithin(2_000)
        assert.equal(record.position.offset, 0n)
        assert.equal(decodeUtf8(record.payload), "safe-mode-0")
      } finally {
        await after.shutdown()
      }
    }
  } finally {
    await laser.close()
  }
})

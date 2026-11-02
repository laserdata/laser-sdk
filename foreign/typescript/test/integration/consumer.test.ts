import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { UserStatus } from "apache-iggy"
import { Laser } from "../../src/client/laser.js"
import { CancelledError, TimeoutError } from "../../src/client/errors.js"
import type { Consumer, ConsumerMessage } from "../../src/stream/consumer.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

const utf8 = (text: string) => new TextEncoder().encode(text)
const decodeUtf8 = (bytes: Uint8Array) => new TextDecoder().decode(bytes)

type UserPermissions = Parameters<Laser["client"]["user"]["updatePermissions"]>[0]["permissions"]

async function freshTopic(laser: Laser) {
  const streamName = `laser-ts-test-${randomUUID()}`
  const topic = laser.stream(streamName).topic("events")
  await laser.stream(streamName).ensure()
  await topic.ensure(1)
  return topic
}

void test("given_native_offset_history_when_rewound_and_deleted_then_should_follow_the_replay_option", async () => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  const topic = await freshTopic(laser)
  for (const payload of ["zero", "one", "two"]) await topic.send(utf8(payload))
  for (const allowReplay of [false, true]) {
    await using consumer = topic.consumer(`native-offset-history-${String(allowReplay)}`, 0, {
      startAt: { kind: "first" },
      commitPolicy: { kind: "disabled" },
      allowReplay
    })
    const records = []
    for (let index = 0; index < 3; index += 1) records.push(await consumer.nextWithin(3_000))
    const [zero, one, two] = records
    assert.ok(zero && one && two)
    await consumer.commit(two)
    await consumer.commit(one)
    const expected = allowReplay ? 1n : 2n
    assert.equal(consumer.lastStoredOffset(0), expected)
    await consumer.storeOffset(2n, 0)
    await consumer.storeOffset(2n, 0)
    await consumer.storeOffset(1n, 0)
    assert.equal((await consumer.storedOffset(0))?.storedOffset, expected)
    await consumer.deleteOffset(0)
    assert.equal(await consumer.storedOffset(0), undefined)
    assert.equal(
      consumer.lastStoredOffset(0),
      undefined,
      "a deletion forgets the local record, so the next store reaches the server"
    )
    await consumer.storeOffset(1n, 0)
    assert.equal((await consumer.storedOffset(0))?.storedOffset, 1n)
    await consumer.storeOffset(0n, 0)
    const rewound = allowReplay ? 0n : 1n
    assert.equal(
      consumer.lastStoredOffset(0),
      rewound,
      "zero moves the offset back only with replay allowed"
    )
    assert.equal((await consumer.storedOffset(0))?.storedOffset, rewound)
  }
})

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
    const consumer = topic.consumer(0, { startAt: { kind: "first" } })
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

    const consumer = topic.consumer(0, { startAt: { kind: "first" } })
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
    const consumer = topic.consumer("cancelled-reader", 0)
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
    assert.throws(() => topic.consumer(0, { maxWaitMs: -1 }), /maxWaitMs/)
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

void test("given_a_deleted_topic_when_it_returns_then_should_report_the_refusal_and_read_on", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  const streamName = `laser-ts-test-${randomUUID()}`
  try {
    await laser.stream(streamName).ensure()
    const topic = laser.stream(streamName).topic("events")
    await topic.ensure(1)
    await topic.send(utf8("before"))
    const consumer = topic.consumer("outlasting", 0, {
      startAt: { kind: "first" },
      maxWaitMs: 200,
      pollingRetryIntervalMs: 200
    })
    try {
      assert.equal(decodeUtf8((await consumer.nextWithin(3_000)).payload), "before")
      await laser.client.topic.delete({
        streamId: streamName,
        topicId: "events",
        partitionsCount: 1
      })
      const outage = await readFor(consumer, 3_000)
      assert.ok(outage.refusals > 0, "the refusal reaches the application")
      assert.ok(outage.reads <= 12, `the application read ${String(outage.reads)} times in 3 s`)
      await topic.ensure(1)
      await topic.send(utf8("after"))
      assert.equal(decodeUtf8((await nextRecord(consumer, 15_000)).payload), "after")
    } finally {
      await consumer.shutdown().catch(() => undefined)
    }
  } finally {
    await laser
      .stream(streamName)
      .delete()
      .catch(() => false)
    await laser.close()
  }
})

void test("given_revoked_poll_permission_when_restored_then_should_report_the_refusal_and_read_on", async () => {
  const admin = await Laser.connect(CONNECTION_STRING)
  const streamName = `laser-ts-test-${randomUUID()}`
  const username = `reader-${randomUUID().slice(0, 8)}`
  const password = `secret-${randomUUID()}`
  let reader: Laser | undefined
  try {
    await admin.stream(streamName).ensure()
    const topic = admin.stream(streamName).topic("events")
    await topic.ensure(1)
    await topic.send(utf8("before"))
    await admin.client.user.create({
      username,
      password,
      status: UserStatus.Active,
      permissions: readerPermissions(true)
    })
    reader = await Laser.connect(CONNECTION_STRING.replace(/^[^@]*@/u, `${username}:${password}@`))
    const consumer = reader
      .stream(streamName)
      .topic("events")
      .consumer("outlasting", 0, {
        startAt: { kind: "first" },
        maxWaitMs: 200,
        pollingRetryIntervalMs: 200
      })
    try {
      assert.equal(decodeUtf8((await consumer.nextWithin(3_000)).payload), "before")
      await admin.client.user.updatePermissions({
        userId: username,
        permissions: readerPermissions(false)
      })
      const outage = await readFor(consumer, 3_000)
      assert.ok(outage.refusals > 0, "the refusal reaches the application")
      assert.ok(outage.reads <= 12, `the application read ${String(outage.reads)} times in 3 s`)
      await admin.client.user.updatePermissions({
        userId: username,
        permissions: readerPermissions(true)
      })
      await topic.send(utf8("after"))
      assert.equal(decodeUtf8((await nextRecord(consumer, 15_000)).payload), "after")
    } finally {
      await consumer.shutdown().catch(() => undefined)
    }
  } finally {
    await reader?.close().catch(() => undefined)
    await admin.client.user.delete({ userId: username }).catch(() => false)
    await admin
      .stream(streamName)
      .delete()
      .catch(() => false)
    await admin.close()
  }
})

// An application that reads on through failures for `durationMs`, without a
// pause of its own between reads.
async function readFor(
  consumer: Consumer,
  durationMs: number
): Promise<{ readonly reads: number; readonly refusals: number }> {
  const deadline = Date.now() + durationMs
  let reads = 0
  let refusals = 0
  while (Date.now() < deadline) {
    reads += 1
    try {
      await consumer.nextWithin(500)
      assert.fail("nothing is readable while the cause lasts")
    } catch (error) {
      if (error instanceof assert.AssertionError) throw error
      if (!(error instanceof TimeoutError)) refusals += 1
    }
  }
  return { reads, refusals }
}

async function nextRecord(consumer: Consumer, timeoutMs: number): Promise<ConsumerMessage> {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      return await consumer.nextWithin(500)
    } catch (error) {
      if (Date.now() >= deadline) throw error
    }
  }
}

function readerPermissions(poll: boolean): UserPermissions {
  return {
    global: {
      ManageServers: false,
      ReadServers: false,
      ManageUsers: false,
      ReadUsers: false,
      ManageStreams: false,
      ReadStreams: poll,
      ManageTopics: false,
      ReadTopics: poll,
      PollMessages: poll,
      SendMessages: false
    },
    streams: []
  }
}

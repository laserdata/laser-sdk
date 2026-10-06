import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

const utf8 = (text: string) => new TextEncoder().encode(text)
const decodeUtf8 = (bytes: Uint8Array) => new TextDecoder().decode(bytes)

async function freshTopic(laser: Laser, partitions = 1) {
  const streamName = `laser-ts-test-${randomUUID()}`
  const topic = laser.stream(streamName).topic("events")
  await laser.stream(streamName).ensure()
  await topic.ensure(partitions)
  return topic
}

void test("given_a_consumer_group_when_messages_are_sent_then_should_receive_them_in_order", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(utf8("one"))
    await topic.send(utf8("two"))

    const groupName = `g-${randomUUID()}`
    const consumer = await topic.consumerGroup(groupName).consumer({ startFrom: { kind: "first" } })
    try {
      const one = await consumer.nextWithin(2_000)
      const two = await consumer.nextWithin(2_000)
      assert.equal(decodeUtf8(one.payload), "one")
      assert.equal(decodeUtf8(two.payload), "two")
    } finally {
      await consumer.shutdown()
    }
  } finally {
    await laser.close()
  }
})

void test("given_a_group_consumer_with_manual_commit_when_rejoined_then_should_resume_after_the_committed_offset", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(utf8("alpha"))
    await topic.send(utf8("beta"))

    const groupName = `g-${randomUUID()}`
    const first = await topic.consumerGroup(groupName).consumer({
      startFrom: { kind: "first" },
      autoCommit: false
    })
    const alpha = await first.nextWithin(2_000)
    assert.equal(decodeUtf8(alpha.payload), "alpha")
    await first.commit(alpha)
    await first.shutdown()

    const rejoined = await topic.consumerGroup(groupName).consumer({
      startFrom: { kind: "next" },
      autoCommit: false
    })
    try {
      const beta = await rejoined.nextWithin(2_000)
      assert.equal(decodeUtf8(beta.payload), "beta")
    } finally {
      await rejoined.shutdown()
    }
  } finally {
    await laser.close()
  }
})

void test("given_manual_group_replay_when_reading_multiple_batches_then_should_not_repeat_offset_zero", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    await topic.send(new TextEncoder().encode("first"))
    await topic.send(new TextEncoder().encode("second"))
    const consumer = await topic.consumerGroup(`replay-${randomUUID()}`).consumer({
      startFrom: { kind: "first" },
      autoCommit: false,
      batchLength: 1
    })
    try {
      assert.equal((await consumer.nextWithin(3_000)).offset, 0n)
      assert.equal((await consumer.nextWithin(3_000)).offset, 1n)
    } finally {
      await consumer.shutdown()
    }
  } finally {
    await laser.close()
  }
})

void test("given_an_unbound_group_id_when_consumed_then_should_preserve_payloads_and_resume", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const topic = await freshTopic(laser)
    const payloads = [new Uint8Array([0xff, 0, 0x80]), utf8("second")]
    for (const payload of payloads) await topic.send(payload)
    const info = await topic.consumerGroup(`numeric-${randomUUID()}`).create()
    const byId = topic.consumerGroupId(info.id)
    const first = await byId.consumer({
      batchLength: 1,
      autoCommit: false,
      startFrom: { kind: "first" }
    })
    try {
      const record = await first.nextWithin(3_000)
      assert.equal(record.offset, 0n)
      assert.deepEqual(record.payload, payloads[0])
      await first.commit(record)
    } finally {
      await first.shutdown()
    }
    const resumed = await byId.consumer({ batchLength: 1, autoCommit: false })
    try {
      const record = await resumed.nextWithin(3_000)
      assert.equal(record.offset, 1n)
      assert.deepEqual(record.payload, payloads[1])
    } finally {
      await resumed.shutdown()
    }
    if ((await laser.capabilities()).filters.groupPolicyReads) {
      const advanced = await byId.reader().start({ kind: "first" }).count(2).build()
      try {
        const page = await advanced.nextPage({ timeoutMs: 3_000 })
        assert.equal(page.policy.mode, "unfiltered")
        assert.deepEqual(
          page.records.map((record) => record.payload),
          payloads
        )
        assert.ok(page.records.every((record) => !record.evaluated))
      } finally {
        await advanced.close()
      }
    }
  } finally {
    await laser.close()
  }
})

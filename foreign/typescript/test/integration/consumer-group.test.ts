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
    const consumer = await topic.consumerGroup(groupName, { startFrom: { kind: "first" } })
    try {
      const one = await consumer.nextWithin(2_000)
      const two = await consumer.nextWithin(2_000)
      assert.ok(one !== null)
      assert.ok(two !== null)
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
    const first = await topic.consumerGroup(groupName, {
      startFrom: { kind: "first" },
      autoCommit: false
    })
    const alpha = await first.nextWithin(2_000)
    assert.ok(alpha !== null)
    assert.equal(decodeUtf8(alpha.payload), "alpha")
    await first.commit(alpha)
    await first.shutdown()

    const rejoined = await topic.consumerGroup(groupName, {
      startFrom: { kind: "next" },
      autoCommit: false
    })
    try {
      const beta = await rejoined.nextWithin(2_000)
      assert.ok(beta !== null)
      assert.equal(decodeUtf8(beta.payload), "beta")
    } finally {
      await rejoined.shutdown()
    }
  } finally {
    await laser.close()
  }
})

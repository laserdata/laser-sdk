import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { TimeoutError } from "../../src/client/errors.js"
import { INTERNAL_NATIVE_CONSUMER } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import { serverErrorCode } from "../../src/iggy/apache-iggy.js"
import type { Consumer } from "../../src/stream/consumer.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

const INVALID_OFFSET = 4100

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
    const consumer = await topic.consumerGroup(groupName).consumer({ startAt: { kind: "first" } })
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
      startAt: { kind: "first" },
      commitPolicy: { kind: "disabled" }
    })
    const alpha = await first.nextWithin(2_000)
    assert.equal(decodeUtf8(alpha.payload), "alpha")
    await first.commit(alpha)
    await first.shutdown()

    const rejoined = await topic.consumerGroup(groupName).consumer({
      startAt: { kind: "next" },
      commitPolicy: { kind: "disabled" }
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
      startAt: { kind: "first" },
      commitPolicy: { kind: "disabled" },
      batchLength: 1
    })
    try {
      assert.equal((await consumer.nextWithin(3_000)).position.offset, 0n)
      assert.equal((await consumer.nextWithin(3_000)).position.offset, 1n)
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
      commitPolicy: { kind: "disabled" },
      startAt: { kind: "first" }
    })
    try {
      const record = await first.nextWithin(3_000)
      assert.equal(record.position.offset, 0n)
      assert.deepEqual(record.payload, payloads[0])
      await first.commit(record)
    } finally {
      await first.shutdown()
    }
    const resumed = await byId.consumer({ batchLength: 1, commitPolicy: { kind: "disabled" } })
    try {
      const record = await resumed.nextWithin(3_000)
      assert.equal(record.position.offset, 1n)
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
          page.records.map((record) => record.message.payload),
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

void test("given_a_group_started_at_first_or_last_when_a_new_member_takes_over_then_should_resume_after_the_stored_offset_without_replay_or_skip", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const paths = (await laser.capabilities()).filters.groupPolicyReads
      ? (["native", "policy"] as const)
      : (["native"] as const)
    for (const path of paths) {
      for (const startAt of [{ kind: "first" }, { kind: "last" }] as const) {
        const label = `${path} consumer started at ${startAt.kind}`
        const topic = await freshTopic(laser, 2)
        const send = async (from: number, to: number) => {
          for (let offset = from; offset < to; offset += 1)
            for (const partition of [0, 1]) await topic.send(utf8(String(offset)), { partition })
        }
        const group = topic.consumerGroup(`takeover-${randomUUID()}`)
        const join = () =>
          path === "native"
            ? group[INTERNAL_NATIVE_CONSUMER]({ startAt, commitPolicy: { kind: "disabled" } })
            : group.consumer({ startAt, commitPolicy: { kind: "disabled" } })
        await send(0, 3)
        const member = await join()
        try {
          // A fresh group started at last reads the last record first.
          if (startAt.kind === "last")
            assert.deepEqual(await take(member, 2, label), ["0-2", "1-2"], label)
          if (startAt.kind === "first")
            assert.deepEqual(
              await take(member, 6, label),
              ["0-0", "0-1", "0-2", "1-0", "1-1", "1-2"],
              label
            )
          await idle(member, label)
          await send(3, 5)
          assert.deepEqual(await take(member, 4, label), ["0-3", "0-4", "1-3", "1-4"], label)
        } finally {
          await member.shutdown()
        }
        await send(5, 7)
        const successor = await join()
        try {
          assert.deepEqual(await take(successor, 4, label), ["0-5", "0-6", "1-5", "1-6"], label)
          await idle(successor, label)
        } finally {
          await successor.shutdown()
        }
      }
    }
  } finally {
    await laser.close()
  }
})

void test("given_a_fresh_group_started_at_last_when_the_consumer_joins_then_should_store_the_offset_before_the_last_record_and_deliver_it_first", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const paths = (await laser.capabilities()).filters.groupPolicyReads
      ? (["native", "policy"] as const)
      : (["native"] as const)
    for (const path of paths) {
      const topic = await freshTopic(laser)
      for (let offset = 0; offset < 5; offset += 1) await topic.send(utf8(String(offset)))
      const group = topic.consumerGroup(`last-${randomUUID()}`)
      const options = { startAt: { kind: "last" }, commitPolicy: { kind: "disabled" } } as const
      const consumer = await (path === "native"
        ? group[INTERNAL_NATIVE_CONSUMER](options)
        : group.consumer(options))
      try {
        const record = await consumer.nextWithin(10_000)
        assert.equal(record.position.offset, 4n, path)
        assert.equal((await consumer.storedOffset(0))?.storedOffset, 3n, path)
      } finally {
        await consumer.shutdown()
      }
    }
  } finally {
    await laser.close()
  }
})

void test("given_a_group_start_past_the_end_of_its_partition_when_the_consumer_reads_then_should_fail_with_invalid_offset_and_leave_no_member", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const paths = (await laser.capabilities()).filters.groupPolicyReads
      ? (["native", "policy"] as const)
      : (["native"] as const)
    for (const path of paths) {
      const topic = await freshTopic(laser)
      for (let offset = 0; offset < 10; offset += 1) await topic.send(utf8(String(offset)))
      const name = `past-end-${randomUUID()}`
      const group = topic.consumerGroup(name)
      const options = { startAt: { kind: "offset", value: 100n } } as const
      // The seed is stored at the start when the join already holds the
      // partition, otherwise before its first read.
      const failure = await (async () => {
        let consumer: Consumer
        try {
          consumer = await (path === "native"
            ? group[INTERNAL_NATIVE_CONSUMER](options)
            : group.consumer(options))
        } catch (error) {
          return error
        }
        try {
          return await consumer.nextWithin(10_000)
        } catch (error) {
          return error
        } finally {
          await consumer.shutdown().catch(() => undefined)
        }
      })()
      assert.equal(serverErrorCode(failure), INVALID_OFFSET, `${path}: ${String(failure)}`)
      const described = await laser.client.group.get({
        streamId: topic.streamName,
        topicId: topic.name,
        groupId: name
      })
      assert.equal(described?.membersCount, 0, path)
    }
  } finally {
    await laser.close()
  }
})

// The next `count` records as `partition-offset`, sorted, each committed.
async function take(consumer: Consumer, count: number, label: string): Promise<string[]> {
  const seen: string[] = []
  for (let index = 0; index < count; index += 1) {
    const record = await consumer.nextWithin(10_000).catch((error: unknown) => {
      throw new Error(`${label} delivered ${JSON.stringify(seen)}, then nothing`, {
        cause: error
      })
    })
    seen.push(`${String(record.position.partitionId)}-${record.position.offset.toString()}`)
    await consumer.commit(record)
  }
  return seen.sort()
}

// Records already in the topic arrive at once, so a quiet second shows none is due.
async function idle(consumer: Consumer, label: string): Promise<void> {
  await assert.rejects(consumer.nextWithin(1_000), TimeoutError, `${label} delivered a record`)
}

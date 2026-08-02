import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { INTERNAL_TRANSPORT, Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { decodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import { ConversationId } from "../../src/types/ids.js"
import type { AgentDeadLetter } from "../../src/wire/agent.js"
import { IDEMPOTENCY_KEY } from "../../src/wire/headers.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

void test("given_a_committed_group_offset_when_consumption_is_probed_then_should_report_the_acknowledgment", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const topic = laser.topic(AgentTopic.Commands)
    const cursor = await topic.replay()
    await laser.sendAgent(AgentTopic.Commands, new TextEncoder().encode("work"), {
      conversationId: ConversationId.new()
    })
    const [published] = await cursor.poll()
    assert.ok(published !== undefined)
    const groupName = `probe-${randomUUID()}`
    const consumer = await topic.consumerGroup(groupName, {
      autoCommit: false,
      startFrom: { kind: "first" }
    })
    try {
      const received = await consumer.nextWithin(2_000)
      assert.ok(received !== null)
      await consumer.commit(received)
      const ids = await laser[INTERNAL_TRANSPORT]().resolveStreamTopicIds?.(
        stream,
        AgentTopic.Commands
      )
      assert.ok(ids !== undefined)
      const status = await laser.consumed(
        { kind: "group", name: groupName },
        {
          streamId: ids.streamId,
          topicId: ids.topicId,
          partitionId: published.partitionId,
          offset: published.offset
        }
      )
      assert.equal(status.kind, "consumed")
      assert.ok(status.committed >= published.offset)
      assert.ok(status.head >= published.offset)
    } finally {
      await consumer.shutdown()
    }
  } finally {
    await laser.close()
  }
})

void test("given_a_dead_letter_source_when_redriven_then_should_preserve_the_record_and_rekey_deduplication", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const cursor = await laser.topic(AgentTopic.Commands).replay()
    const conversationId = ConversationId.new()
    const payload = new TextEncoder().encode("retry-this")
    await laser.sendAgent(AgentTopic.Commands, payload, {
      conversationId,
      idempotencyKey: "original-key"
    })
    const [original] = await cursor.poll()
    assert.ok(original !== undefined)
    const ids = await laser[INTERNAL_TRANSPORT]().resolveStreamTopicIds?.(
      stream,
      AgentTopic.Commands
    )
    assert.ok(ids !== undefined)
    const capsule: AgentDeadLetter = {
      source: {
        streamId: ids.streamId,
        topicId: ids.topicId,
        partitionId: original.partitionId,
        offset: original.offset
      },
      reason: { kind: "known", name: "Rejected" },
      attempts: 1,
      payload
    }

    await laser.redriveDeadLetter(capsule)
    const [redriven] = await cursor.poll()
    assert.ok(redriven !== undefined)
    assert.deepEqual(redriven.payload, original.payload)
    const provenance = decodeProvenanceHeaders(redriven.headers)
    assert.equal(provenance.conversationId.toString(), conversationId.toString())
    assert.deepEqual(redriven.headers.get(IDEMPOTENCY_KEY), {
      kind: "string",
      value: `original-key/redrive/${String(original.partitionId)}-${original.offset.toString()}`
    })
  } finally {
    await laser.close()
  }
})

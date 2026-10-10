import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { INTERNAL_TRANSPORT, Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { decodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import type { AgentDeadLetter } from "../../src/wire/agent.js"
import { IDEMPOTENCY_KEY } from "../../src/wire/headers.js"
import { TopicRetention } from "../../src/session.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

void test("given_a_committed_group_offset_when_consumption_is_probed_then_should_report_the_acknowledgment", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    const topic = laser.topic(AgentTopic.Sessions)
    const cursor = await topic.replay()
    await laser.sendAgent(AgentTopic.Sessions, new TextEncoder().encode("work"), {
      conversationId: ConversationId.new()
    })
    const [published] = await cursor.poll()
    assert.ok(published !== undefined)
    const groupName = `probe-${randomUUID()}`
    const consumer = await topic.consumerGroup(groupName).consumer({
      commitPolicy: { kind: "disabled" },
      startAt: { kind: "first" }
    })
    try {
      const received = await consumer.nextWithin(2_000)
      await consumer.commit(received)
      const ids = await laser[INTERNAL_TRANSPORT]().resolveStreamTopicIds?.(
        stream,
        AgentTopic.Sessions
      )
      assert.ok(ids !== undefined)
      const status = await laser.consumed(
        { kind: "group", name: groupName },
        {
          streamId: ids.streamId,
          topicId: ids.topicId,
          partitionId: published.id.partitionId,
          offset: published.id.offset
        }
      )
      assert.equal(status.kind, "consumed")
      assert.ok(status.committed >= published.id.offset)
      assert.ok(status.head >= published.id.offset)
    } finally {
      await consumer.shutdown()
    }
  } finally {
    await laser.close()
  }
})

void test("given_a_record_for_another_agent_when_its_group_commits_past_it_then_should_report_skipped", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    const topic = laser.topic(AgentTopic.Sessions)
    const cursor = await topic.replay()
    await laser.sendAgent(AgentTopic.Sessions, new TextEncoder().encode("not mine"), {
      conversationId: ConversationId.new(),
      targetAgentId: AgentId.new("someone-else")
    })
    const [published] = await cursor.poll()
    assert.ok(published !== undefined)
    const groupName = `skipper-${randomUUID().slice(0, 8)}`
    const consumer = await topic.consumerGroup(groupName).consumer({
      commitPolicy: { kind: "disabled" },
      startAt: { kind: "first" }
    })
    try {
      await consumer.commit(await consumer.nextWithin(2_000))
      const ids = await laser[INTERNAL_TRANSPORT]().resolveStreamTopicIds?.(
        stream,
        AgentTopic.Sessions
      )
      assert.ok(ids !== undefined)
      const status = await laser.consumed(
        { kind: "group", name: groupName },
        {
          streamId: ids.streamId,
          topicId: ids.topicId,
          partitionId: published.id.partitionId,
          offset: published.id.offset
        }
      )
      assert.ok(status.kind === "skipped")
      assert.equal(status.dispatch, "foreign")
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
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    const cursor = await laser.topic(AgentTopic.Sessions).replay()
    const conversationId = ConversationId.new()
    const payload = new TextEncoder().encode("retry-this")
    await laser.sendAgent(AgentTopic.Sessions, payload, {
      conversationId,
      idempotencyKey: "original-key"
    })
    const [original] = await cursor.poll()
    assert.ok(original !== undefined)
    const ids = await laser[INTERNAL_TRANSPORT]().resolveStreamTopicIds?.(
      stream,
      AgentTopic.Sessions
    )
    assert.ok(ids !== undefined)
    const capsule: AgentDeadLetter = {
      source: {
        streamId: ids.streamId,
        topicId: ids.topicId,
        partitionId: original.id.partitionId,
        offset: original.id.offset
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
      value: `original-key/redrive/${String(original.id.partitionId)}-${original.id.offset.toString()}`
    })
  } finally {
    await laser.close()
  }
})

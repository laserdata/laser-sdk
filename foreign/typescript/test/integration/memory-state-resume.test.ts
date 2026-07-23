import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { TopicSnapshotStore } from "../../src/snapshot.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { ConversationId as WireConversationId } from "../../src/wire/ids.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const encoder = new TextEncoder()
const decoder = new TextDecoder()

async function eventually<Value>(read: () => Promise<Value | undefined>): Promise<Value> {
  const deadline = performance.now() + 2_000
  let value = await read()
  while (value === undefined && performance.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 10))
    value = await read()
  }
  assert.ok(value !== undefined)
  return value
}

void test("given_a_configured_memory_topic_when_recalled_through_a_scope_then_should_preserve_topic_and_source", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  const memory = await laser.memoryTopic("incidents").partitions(2).ttl(86_400_000).build()
  const conversation = ConversationId.new()
  await memory
    .remember(encoder.encode("checkout uses the read replica"))
    .conversation(conversation)
    .send()

  const items = await eventually(async () => {
    const found = await laser.context(conversation).memory(memory).recall().limit(1).fetch()
    return found.length === 1 ? found : undefined
  })
  assert.equal(decoder.decode(items[0]?.payload), "checkout uses the read replica")
  assert.equal(items[0]?.source?.kind, "message")

  const iggy = laser.iggyClient as unknown as {
    readonly topic: {
      get(input: { readonly streamId: string; readonly topicId: string }): Promise<{
        readonly partitionsCount: number
        readonly messageExpiry: bigint
      } | null>
    }
  }
  const topic = await iggy.topic.get({ streamId: stream, topicId: "incidents" })
  assert.ok(topic !== null)
  assert.equal(topic.partitionsCount, 2)
  await laser.memoryTopic("incidents").partitions(2).ttl(86_400_000).build()
})

void test("given_durable_memory_when_a_fresh_handle_folds_then_should_rebuild_feedback_tombstones_and_named_state", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(2)
    const namespace = `support-${randomUUID()}`
    const conversation = ConversationId.new()
    const agent = AgentId.new("memory-agent")
    const memory = laser.memory(namespace)
    const keep = await memory
      .remember(encoder.encode("keep"))
      .conversation(conversation)
      .agent(agent)
      .dedup()
      .send()
    await memory
      .remember(encoder.encode("keep"))
      .conversation(conversation)
      .agent(agent)
      .dedup()
      .send()
    const drop = await memory
      .remember(encoder.encode("drop"))
      .conversation(conversation)
      .agent(agent)
      .send()
    await memory.improve({ conversation, agent }, { target: keep, weight: 4 })
    await memory.forget({ conversation, agent }, drop)
    const log = memory.logBackend()
    assert.ok(log !== undefined)
    await log.set("plan", encoder.encode('{"step":1,"old":true}'))
    await log.update("plan", encoder.encode('{"step":2,"old":null}'))

    const rebuilt = laser.memory(namespace)
    const items = await eventually(async () => {
      const found = await rebuilt.recall().conversation(conversation).agent(agent).fetch()
      return found.length === 1 ? found : undefined
    })
    assert.equal(decoder.decode(items[0]?.payload), "keep")
    assert.equal(items[0]?.score, 4)
    assert.deepEqual(JSON.parse(decoder.decode(await rebuilt.logBackend()?.fetchFolded("plan"))), {
      step: 2
    })
  } finally {
    await laser.close()
  }
})

void test("given_a_topic_snapshot_when_state_is_loaded_then_should_resume_after_saved_offsets", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    await laser.topic("agent.snapshots").ensure(1)
    const conversation = ConversationId.new()
    const scope = laser.context(conversation)
    await scope.append(AgentTopic.Commands, encoder.encode("1"))
    await scope.append(AgentTopic.Commands, encoder.encode("2"))
    const history = await eventually(async () => {
      const messages = await scope.fetch([AgentTopic.Commands], 10)
      return messages.length === 2 ? messages : undefined
    })
    const asOf = new Map<number, bigint>()
    for (const message of history) asOf.set(message.id.partitionId, message.id.offset)
    const snapshots = new TopicSnapshotStore(laser)
    await snapshots.save({
      conversation: WireConversationId.parse(conversation.toString()),
      asOf,
      state: encoder.encode("3")
    })
    await scope.append(AgentTopic.Commands, encoder.encode("3"))

    const resumed = await eventually(async () => {
      const total = await scope.stateWith(
        new TopicSnapshotStore(laser),
        [AgentTopic.Commands],
        0,
        (bytes) => Number(decoder.decode(bytes)),
        (sum, message) => sum + Number(decoder.decode(message.payload))
      )
      return total === 6 ? total : undefined
    })
    assert.equal(resumed, 6)
  } finally {
    await laser.close()
  }
})

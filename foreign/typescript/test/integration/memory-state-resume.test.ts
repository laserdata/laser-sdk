import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { TopicSnapshotStore } from "../../src/snapshot.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { ConversationId as WireConversationId } from "../../src/wire/ids.js"
import { TopicRetention } from "../../src/session.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const encoder = new TextEncoder()
const decoder = new TextDecoder()

async function eventually<Value>(read: () => Promise<Value | undefined>): Promise<Value> {
  const deadline = performance.now() + 10_000
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
  await memory.remember(encoder.encode("auth uses the read replica")).scope(conversation).send()

  const items = await eventually(async () => {
    const found = await laser
      .context(conversation)
      .memory(memory)
      .recall()
      .limit(1)
      .folded()
      .fetch()
    return found.length === 1 ? found : undefined
  })
  assert.equal(decoder.decode(items[0]?.payload), "auth uses the read replica")
  assert.equal(items[0]?.source?.kind, "message")

  const iggy = laser.client as unknown as {
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
    await laser.bootstrap(2, TopicRetention.expireAfter(86_400_000))
    const namespace = `support-${randomUUID()}`
    const conversation = ConversationId.new()
    const agent = AgentId.new("memory-agent")
    const memory = laser.memory(namespace)
    const keep = await memory
      .remember(encoder.encode("keep"))
      .scope(conversation)
      .agent(agent)
      .dedup()
      .send()
    await memory.remember(encoder.encode("keep")).scope(conversation).agent(agent).dedup().send()
    const drop = await memory
      .remember(encoder.encode("drop"))
      .scope(conversation)
      .agent(agent)
      .send()
    await memory.improve({ conversation, agent }, { target: keep, weight: 4 })
    await memory.forget({ conversation, agent }, drop)
    const log = memory.logBackend()
    assert.ok(log !== undefined)
    await log.setNamed("plan", encoder.encode('{"step":1,"old":true}'))
    await log.updateNamed("plan", encoder.encode('{"step":2,"old":null}'))

    const rebuilt = laser.memory(namespace)
    const items = await eventually(async () => {
      const found = await rebuilt.recall(conversation).agent(agent).folded().fetch()
      return found.length === 1 ? found : undefined
    })
    assert.equal(decoder.decode(items[0]?.payload), "keep")
    assert.equal(items[0]?.score, 4)
    assert.deepEqual(
      JSON.parse(decoder.decode(await rebuilt.logBackend()?.fetchNamedFolded("plan"))),
      {
        step: 2
      }
    )
  } finally {
    await laser.close()
  }
})

void test("given_a_topic_snapshot_when_state_is_loaded_then_should_resume_after_saved_offsets", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    await laser.topic("agent.snapshots").ensure(1)
    const conversation = ConversationId.new()
    const scope = laser.context(conversation)
    await scope.append(AgentTopic.Sessions, encoder.encode("1"))
    await scope.append(AgentTopic.Sessions, encoder.encode("2"))
    const history = await eventually(async () => {
      const messages = await scope.fetch([AgentTopic.Sessions], 10)
      return messages.length === 2 ? messages : undefined
    })
    const offsets = new Map<number, bigint>()
    for (const message of history) offsets.set(message.id.partitionId, message.id.offset)
    const transport = laser[INTERNAL_TRANSPORT]()
    const source = await transport.findSnapshotStream?.(stream)
    const topic = await transport.findSnapshotTopic?.(stream, AgentTopic.Sessions)
    assert.ok(source !== undefined && topic !== undefined)
    assert.ok(source.createdAtMicros % 1000n !== 0n || topic.createdAtMicros % 1000n !== 0n)
    const asOf = [...offsets]
      .sort(([left], [right]) => left - right)
      .map(([partitionId, offset]) => ({
        topicId: topic.id,
        topicCreatedAtMicros: topic.createdAtMicros,
        partitionId,
        offset
      }))
    const snapshots = new TopicSnapshotStore(laser, "planner")
    await snapshots.save({
      stream,
      streamId: source.id,
      streamCreatedAtMicros: source.createdAtMicros,
      conversation: WireConversationId.parse(conversation.toString()),
      fold: "planner",
      asOf,
      state: encoder.encode("3")
    })
    await scope.append(AgentTopic.Sessions, encoder.encode("3"))

    const resumed = await eventually(async () => {
      const total = await scope.stateWith(
        new TopicSnapshotStore(laser, "planner"),
        [AgentTopic.Sessions],
        0,
        (sum, message) => sum + Number(decoder.decode(message.payload)),
        (bytes) => Number(decoder.decode(bytes))
      )
      return total === 6 ? total : undefined
    })
    assert.equal(resumed, 6)
  } finally {
    await laser.close()
  }
})
void test("given_managed_memory_with_user_and_application_scopes_when_recalled_then_should_isolate_the_newest_matching_items", async (context) => {
  const stream = `laser-ts-test-${randomUUID()}`
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  if (!(await laser.capabilities()).kv.available) {
    context.skip("this deployment has no managed memory view")
    return
  }
  const memory = await laser.memoryTopic(`notes-${randomUUID()}`).build()
  const first = ConversationId.new()
  const second = ConversationId.new()
  const matching = []
  matching.push(
    await memory
      .remember(encoder.encode("first matching"))
      .scope(first)
      .user("reader")
      .application("diagnostics")
      .send()
  )
  await memory
    .remember(encoder.encode("other user"))
    .scope(first)
    .user("other")
    .application("diagnostics")
    .send()
  await memory
    .remember(encoder.encode("other application"))
    .scope(second)
    .user("reader")
    .application("other")
    .send()
  matching.push(
    await memory
      .remember(encoder.encode("second matching"))
      .scope(second)
      .user("reader")
      .application("diagnostics")
      .send()
  )
  const items = await eventually(async () => {
    const selected = await memory
      .recall()
      .user("reader")
      .application("diagnostics")
      .limit(2)
      .fetch()
    return selected.length === 2 ? selected : undefined
  })
  const expected = matching
    .map((id) => id.asU128())
    .sort((left, right) => (left > right ? -1 : left < right ? 1 : 0))
  assert.deepEqual(
    items.map((item) => item.id.asU128()),
    expected
  )
  const conversations = items.map((item) => item.provenance.conversationId.toString()).sort()
  assert.deepEqual(conversations, [first.toString(), second.toString()].sort())
  await laser.stream(stream).delete()
})

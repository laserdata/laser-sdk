import assert from "node:assert/strict"
import { test } from "node:test"
import { ConfigError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import {
  Chain,
  ContextAssembler,
  LastN,
  RoleFilter,
  TokenBudget,
  contextCheckpoint,
  type ContextMessage
} from "../../src/context.js"
import { ContextScope, ScopedMemory } from "../../src/context-scope.js"
import { type MemoryHandle, RememberBuilder } from "../../src/memory/handle.js"
import type { MemoryId, MemoryScope } from "../../src/memory/types.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

function message(agent: string, offset: bigint, bytes = 4): ContextMessage {
  return {
    id: { partitionId: 0, offset },
    provenance: { conversationId: ConversationId.new(), agent: AgentId.new(agent) },
    payload: new Uint8Array(bytes),
    topic: "agent.sessions",
    timestampMicros: offset,
    streamId: 1,
    topicId: 1
  }
}

void test("given_a_history_when_selecting_last_n_then_should_keep_the_recent_tail", () => {
  const history = [message("a", 0n), message("b", 1n), message("c", 2n)]
  assert.deepEqual(
    new LastN(2).select(history).map((entry) => entry.id.offset),
    [1n, 2n]
  )
})

void test("given_role_and_tail_policies_when_chained_then_should_narrow_in_order", () => {
  const history = [message("planner", 0n), message("writer", 1n), message("planner", 2n)]
  const selected = new Chain([new RoleFilter([AgentId.new("planner")]), new LastN(1)]).select(
    history
  )
  assert.equal(selected.length, 1)
  assert.equal(selected[0]?.id.offset, 2n)
})

void test("given_a_token_budget_when_the_newest_message_exceeds_it_then_should_keep_one", () => {
  const history = [message("a", 0n, 400), message("a", 1n, 1_200)]
  assert.deepEqual(
    new TokenBudget(10).select(history).map((entry) => entry.id.offset),
    [1n]
  )
})

void test("given_no_conversation_when_building_an_assembler_then_should_reject_the_config", () => {
  assert.throws(() => ContextAssembler.builder().topics(["agent.sessions"]).build(), ConfigError)
  assert.doesNotThrow(() => ContextAssembler.builder().conversationId(ConversationId.new()).build())
})

void test("given_topics_when_capturing_a_context_checkpoint_then_should_record_each_tail", async () => {
  const tails = new Map<string, ReadonlyMap<number, bigint>>([
    ["agent.sessions", new Map([[0, 7n]])],
    ["agent.streams", new Map()]
  ])
  const laser = {
    topic: (name: string) => ({ tailOffsets: () => Promise.resolve(tails.get(name)) })
  } as unknown as Laser
  const checkpoint = await contextCheckpoint(laser, ["agent.sessions", "agent.streams"])
  assert.deepEqual(checkpoint.topicOffsets("agent.sessions"), new Map([[0, 7n]]))
  assert.deepEqual(checkpoint.topicOffsets("agent.streams"), new Map())
  const scoped = await ContextScope.create(laser, ConversationId.new()).checkpoint([
    "agent.sessions"
  ])
  assert.deepEqual(scoped.toJSON(), { per_topic: { "agent.sessions": { "0": 7 } } })
})

void test("given_a_context_scope_when_reading_its_laser_then_should_be_the_client_it_was_opened_on", () => {
  const laser = {} as Laser
  assert.equal(ContextScope.create(laser, ConversationId.new()).laser, laser)
})

void test("given_the_built_in_policies_when_named_then_should_match_the_rust_manifest_names", () => {
  const history = [message("a", 0n, 4), message("b", 1n, 4), message("a", 2n, 4)]
  assert.equal(new LastN(2).name(), "last_n(2)")
  assert.equal(new TokenBudget(4000).name(), "token_budget(4000)")
  assert.equal(new RoleFilter([AgentId.new("a")]).name(), "role_filter")
  assert.equal(
    new Chain([new LastN(50), new TokenBudget(4000)]).name(),
    "chain(last_n(50),token_budget(4000))"
  )
  assert.equal(new LastN(2).version(), "1")
  const selection = new LastN(2).selection(history)
  assert.deepEqual(
    selection.kept.map((entry) => entry.id.offset),
    [1n, 2n]
  )
  assert.deepEqual(
    selection.dropped.map((entry) => entry.id.offset),
    [0n]
  )
  assert.equal(selection.reason, "last_n(2)")
})

void test("given_a_scoped_memory_with_lineage_when_remembering_then_should_stamp_origin_and_producer", async () => {
  const scopes: MemoryScope[] = []
  const handle = {
    remember: (payload: Uint8Array) => RememberBuilder.create(handle, payload),
    append: (scope: MemoryScope, id: MemoryId) => {
      scopes.push(scope)
      return Promise.resolve(id)
    }
  } as unknown as MemoryHandle
  const conversation = ConversationId.new()
  const origin = { kind: "memory" as const, id: "m-1" }
  const producer = { name: "sdk:planner", version: "0.7.0" }
  const plain = ScopedMemory.create(handle, conversation)
  assert.equal(plain.origin(), undefined)
  const linked = plain.withLineage(origin, producer)
  assert.equal(linked.origin(), origin)
  assert.equal(linked.producer(), producer)
  await linked.remember(new Uint8Array([1])).send()
  assert.deepEqual(scopes, [{ conversation, origin, producer }])
})

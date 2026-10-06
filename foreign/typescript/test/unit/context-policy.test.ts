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
import { ContextScope } from "../../src/context-scope.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

function message(agent: string, offset: bigint, bytes = 4): ContextMessage {
  return {
    id: { partitionId: 0, offset },
    provenance: { conversationId: ConversationId.new(), agent: AgentId.new(agent) },
    payload: new Uint8Array(bytes),
    topic: "agent.commands"
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
  assert.throws(() => ContextAssembler.builder().topics(["agent.commands"]).build(), ConfigError)
  assert.doesNotThrow(() => ContextAssembler.builder().conversationId(ConversationId.new()).build())
})

void test("given_topics_when_capturing_a_context_checkpoint_then_should_record_each_tail", async () => {
  const tails = new Map<string, ReadonlyMap<number, bigint>>([
    ["agent.commands", new Map([[0, 7n]])],
    ["agent.responses", new Map()]
  ])
  const laser = {
    topic: (name: string) => ({ tailOffsets: () => Promise.resolve(tails.get(name)) })
  } as unknown as Laser
  const checkpoint = await contextCheckpoint(laser, ["agent.commands", "agent.responses"])
  assert.deepEqual(checkpoint.topicOffsets("agent.commands"), new Map([[0, 7n]]))
  assert.deepEqual(checkpoint.topicOffsets("agent.responses"), new Map())
  const scoped = await ContextScope.create(laser, ConversationId.new()).checkpoint([
    "agent.commands"
  ])
  assert.deepEqual(scoped.toJSON(), { per_topic: { "agent.commands": { "0": 7 } } })
})

void test("given_a_context_scope_when_reading_its_laser_then_should_be_the_client_it_was_opened_on", () => {
  const laser = {} as Laser
  assert.equal(ContextScope.create(laser, ConversationId.new()).laser, laser)
})

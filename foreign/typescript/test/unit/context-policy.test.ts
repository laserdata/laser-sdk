import assert from "node:assert/strict"
import { test } from "node:test"
import {
  ContextChain,
  LastN,
  RoleFilter,
  TokenBudget,
  type ContextMessage
} from "../../src/context.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

function message(agent: string, offset: bigint, bytes = 4): ContextMessage {
  return {
    id: { partitionId: 0, offset },
    provenance: { conversationId: ConversationId.new(), agent: AgentId.new(agent) },
    payload: new Uint8Array(bytes),
    timestampMicros: offset
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
  const selected = new ContextChain([
    new RoleFilter([AgentId.new("planner")]),
    new LastN(1)
  ]).select(history)
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

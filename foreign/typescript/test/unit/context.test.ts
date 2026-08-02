import assert from "node:assert/strict"
import { test } from "node:test"
import {
  BEST_EFFORT,
  REQUIRE_ALL,
  emptyGather,
  gatherReplies,
  quorumOf,
  quorumSatisfied,
  type Gather
} from "../../src/agent/context.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

void test("given_require_all_when_any_successes_then_should_never_be_satisfied_early", () => {
  assert.equal(quorumSatisfied(REQUIRE_ALL, 0), false)
  assert.equal(quorumSatisfied(REQUIRE_ALL, 5), false)
})

void test("given_best_effort_when_any_successes_then_should_never_be_satisfied_early", () => {
  assert.equal(quorumSatisfied(BEST_EFFORT, 0), false)
  assert.equal(quorumSatisfied(BEST_EFFORT, 5), false)
})

void test("given_quorum_when_successes_below_needed_then_should_not_be_satisfied", () => {
  assert.equal(quorumSatisfied(quorumOf(3), 2), false)
})

void test("given_quorum_when_successes_meet_needed_then_should_be_satisfied", () => {
  assert.equal(quorumSatisfied(quorumOf(3), 3), true)
})

void test("given_quorum_when_successes_exceed_needed_then_should_still_be_satisfied", () => {
  assert.equal(quorumSatisfied(quorumOf(3), 4), true)
})

void test("given_an_empty_gather_when_reading_replies_then_should_be_empty", () => {
  assert.deepEqual(gatherReplies(emptyGather()), [])
})

void test("given_a_gather_with_oks_when_reading_replies_then_should_drop_agent_attribution", () => {
  const message = {
    provenance: { conversationId: ConversationId.new() },
    payload: new Uint8Array(),
    id: { partitionId: 0, offset: 0n }
  }
  const gather: Gather = {
    ok: [[AgentId.new("worker"), message]],
    failures: []
  }
  assert.equal(gatherReplies(gather).length, 1)
  assert.equal(gatherReplies(gather)[0], message)
})

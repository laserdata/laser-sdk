import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError, TimeoutError } from "../../src/client/errors.js"
import {
  BEST_EFFORT,
  REQUIRE_ALL,
  gatherBranches,
  gatherReplies,
  gatherSatisfied,
  quorumOf,
  validateGatherPolicy,
  type Branch,
  type BranchResult,
  type Gather
} from "../../src/agent/context.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

void test("given_each_policy_when_checking_satisfaction_then_should_apply_its_own_threshold", () => {
  assert.equal(gatherSatisfied(REQUIRE_ALL, 3, 3), true)
  assert.equal(gatherSatisfied(REQUIRE_ALL, 2, 3), false)
  assert.equal(gatherSatisfied(quorumOf(2), 2, 3), true)
  assert.equal(gatherSatisfied(quorumOf(2), 1, 3), false)
  assert.equal(gatherSatisfied(quorumOf(5), 3, 3), false)
  assert.equal(gatherSatisfied(BEST_EFFORT, 1, 3), true)
  assert.equal(gatherSatisfied(BEST_EFFORT, 0, 3), false)
})

void test("given_a_zero_quorum_when_validated_then_should_refuse_it", () => {
  assert.throws(() => quorumOf(0), InvalidError)
  assert.throws(() => {
    validateGatherPolicy({ kind: "quorum", needed: 0 })
  }, InvalidError)
  validateGatherPolicy(quorumOf(1))
})

void test("given_a_met_quorum_when_gathering_then_should_cancel_and_report_the_rest", async () => {
  const slow = pending("slow")
  const gathered = await gatherBranches(
    [settled("fast", { kind: "ok", value: 1 }), slow],
    quorumOf(1),
    5_000,
    () => true
  )
  assert.equal(gathered.successes, 1)
  assert.deepEqual(
    gathered.outcomes.map(([agent]) => agent.asStr()),
    ["fast"]
  )
  assert.deepEqual(
    gathered.cancelled.map((agent) => agent.asStr()),
    ["slow"]
  )
  assert.equal(slow.controller.signal.aborted, true)
})

void test("given_a_best_effort_deadline_when_a_branch_outlives_it_then_should_cancel_it", async () => {
  const slow = pending("slow")
  const gathered = await gatherBranches([slow], BEST_EFFORT, 20, () => true)
  assert.deepEqual(gathered.outcomes, [])
  assert.deepEqual(
    gathered.cancelled.map((agent) => agent.asStr()),
    ["slow"]
  )
  assert.equal(gatherSatisfied(BEST_EFFORT, gathered.successes, 1), false)
})

void test("given_require_all_when_gathering_then_should_keep_every_failure", async () => {
  const gathered = await gatherBranches(
    [
      settled<number>("late", { kind: "error", error: new TimeoutError("reply") }),
      settled("ok", { kind: "ok", value: 2 })
    ],
    REQUIRE_ALL,
    5_000,
    () => true
  )
  assert.equal(gathered.successes, 1)
  assert.deepEqual(gathered.cancelled, [])
  assert.equal(gathered.outcomes.length, 2)
})

void test("given_a_gather_with_oks_when_reading_replies_then_should_drop_agent_attribution", () => {
  const message = {
    provenance: { conversationId: ConversationId.new() },
    payload: new Uint8Array(),
    id: { partitionId: 0, offset: 0n }
  }
  const gather: Gather = {
    ok: [[AgentId.new("worker"), message]],
    failures: [],
    cancelled: [],
    satisfied: true
  }
  assert.equal(gatherReplies(gather).length, 1)
  assert.equal(gatherReplies(gather)[0], message)
})

function settled<T>(agent: string, result: BranchResult<T>): Branch<T> {
  return {
    agent: AgentId.new(agent),
    controller: new AbortController(),
    result: Promise.resolve(result)
  }
}

function pending(agent: string): Branch<number> {
  const controller = new AbortController()
  const result = new Promise<BranchResult<number>>((resolve) => {
    controller.signal.addEventListener("abort", () => {
      resolve({ kind: "error", error: new TimeoutError("cancelled") })
    })
  })
  return { agent: AgentId.new(agent), controller, result }
}

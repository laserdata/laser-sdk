import assert from "node:assert/strict"
import { test } from "node:test"
import {
  AgentId,
  ConversationId,
  CrashContext,
  GovernorMode,
  SwarmActivity,
  type ContextMessage,
  type PolicyEvidence
} from "../../src/index.js"

function evidence(
  id: string,
  source: string | undefined,
  decision: "allow" | "block",
  at: bigint
): PolicyEvidence {
  return {
    decisionId: id,
    decision,
    mode: GovernorMode.Enforce,
    kind: "send",
    stream: "laser",
    topic: "agent.commands",
    ...(source !== undefined ? { source } : {}),
    receiptDigest: "",
    outcome: decision === "block" ? "blocked" : "effected",
    atMicros: at
  }
}

void test("given_replayed_swarm_evidence_when_folded_then_should_deduplicate_and_order_agents", () => {
  const swarm = new SwarmActivity()
  const repeated = evidence("d1", "busy", "allow", 1n)
  swarm.observe(repeated)
  swarm.observe(repeated)
  swarm.observe(evidence("d2", "busy", "block", 2n))
  swarm.observe(evidence("d3", "quiet", "allow", 3n))
  swarm.observe(evidence("anonymous", undefined, "allow", 4n))
  assert.deepEqual(
    swarm.agents().map(([name]) => name),
    ["busy", "quiet"]
  )
  assert.equal(swarm.agent("busy")?.decisions, 2n)
  assert.equal(swarm.agent("busy")?.count("block"), 1n)
  assert.equal(swarm.agent("busy")?.lastDecision?.decisionId, "d2")
})

void test("given_untrusted_crash_fields_when_summarized_then_should_escape_and_truncate_them", () => {
  const message: ContextMessage = {
    id: { partitionId: 0, offset: 0n },
    provenance: {
      conversationId: ConversationId.new(),
      agent: AgentId.new("planner")
    },
    payload: new TextEncoder().encode(`${"x".repeat(201)}\nforged`),
    timestampMicros: 1n
  }
  const context = new CrashContext(
    [message],
    {
      source: { streamId: 0, topicId: 0, partitionId: 0, offset: 0n },
      reason: { kind: "known", name: "RetryExhausted" },
      attempts: 3,
      detail: "failure\r\nforged",
      payload: new Uint8Array()
    },
    { ...evidence("d1", "planner", "block", 2n), reason: "denied\nforged" }
  )
  const summary = context.summarize()
  assert.equal(summary.split("\n").length - 1, 4)
  assert.match(summary, /x{200}\.\.\./)
  assert.match(summary, /failure\\r\\nforged/)
  assert.match(summary, /denied\\nforged/)
})

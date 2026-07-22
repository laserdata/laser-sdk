import assert from "node:assert/strict"
import { test } from "node:test"
import {
  ActionDecision,
  ActionKind,
  ConversationId,
  GovernorMode,
  GovernorState,
  PolicyBlockedError,
  QuorumGovernor,
  SwappableGovernor,
  decodePolicyEvidence,
  encodePolicyEvidence,
  verifyEvidenceChain,
  type ActionGovernor,
  type PolicyEvidence
} from "../../src/index.js"

const encoder = new TextEncoder()

const allow: ActionGovernor = { decide: () => Promise.resolve(ActionDecision.allow()) }
const block: ActionGovernor = {
  decide: () => Promise.resolve(ActionDecision.block("refused"))
}

function action(conversation = ConversationId.derive("govern")) {
  return {
    kind: ActionKind.Send,
    stream: "laser",
    topic: "agent.commands",
    source: "planner",
    conversation,
    payload: encoder.encode("wire-funds"),
    signed: false
  } as const
}

void test("given_enforce_mode_when_blocked_then_should_emit_evidence_before_rejecting", async () => {
  const state = new GovernorState(block, GovernorMode.Enforce)
  const evidence: PolicyEvidence[] = []
  await assert.rejects(
    state.govern(action(), (item) => {
      evidence.push(item)
      return Promise.resolve()
    }),
    PolicyBlockedError
  )
  const first = evidence[0]
  assert.ok(first !== undefined)
  assert.equal(first.decision, "block")
  assert.equal(first.outcome, "blocked")
  assert.ok(verifyEvidenceChain(evidence))
})

void test("given_observe_mode_when_blocked_then_should_emit_and_preserve_the_original_body", async () => {
  const state = new GovernorState(block, GovernorMode.Observe)
  const evidence: PolicyEvidence[] = []
  const payload = await state.govern(action(), (item) => {
    evidence.push(item)
    return Promise.resolve()
  })
  assert.deepEqual(payload, encoder.encode("wire-funds"))
  assert.equal(evidence[0]?.outcome, "effected")
})

void test("given_two_decisions_when_emitted_then_should_form_a_verifiable_digest_chain", async () => {
  const observe: ActionGovernor = { decide: () => Promise.resolve(ActionDecision.observe()) }
  const state = new GovernorState(observe, GovernorMode.Enforce)
  const evidence: PolicyEvidence[] = []
  for (let index = 0; index < 2; index += 1) {
    await state.govern(action(), (item) => {
      evidence.push(item)
      return Promise.resolve()
    })
  }
  assert.equal(evidence[1]?.previousDigest, evidence[0]?.receiptDigest)
  assert.ok(verifyEvidenceChain(evidence))
  const first = evidence[0]
  assert.ok(first !== undefined)
  assert.deepEqual(decodePolicyEvidence(encodePolicyEvidence(first)), first)
})

void test("given_quorum_voters_when_combined_then_should_require_the_configured_threshold", async () => {
  const quorum = new QuorumGovernor(2).voter("allow", allow).voter("block", block)
  assert.equal(
    (await quorum.decide({ ...action(), counters: { sends: 0n, requests: 0n, bytesSent: 0n } }))
      .verdict.kind,
    "block"
  )
  quorum.voter("allow-2", allow)
  assert.equal(
    (await quorum.decide({ ...action(), counters: { sends: 0n, requests: 0n, bytesSent: 0n } }))
      .verdict.kind,
    "allow"
  )
})

void test("given_a_swappable_governor_when_replaced_then_should_use_the_new_policy", async () => {
  const swappable = new SwappableGovernor(allow)
  const governed = { ...action(), counters: { sends: 0n, requests: 0n, bytesSent: 0n } }
  assert.equal((await swappable.decide(governed)).verdict.kind, "allow")
  swappable.swap(block)
  assert.equal((await swappable.decide(governed)).verdict.kind, "block")
})

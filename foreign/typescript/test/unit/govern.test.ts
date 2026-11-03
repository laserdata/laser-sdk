import assert from "node:assert/strict"
import { test } from "node:test"
import {
  ActionDecision,
  ActionKind,
  ConversationId,
  CodecError,
  InvalidError,
  GovernorMode,
  PolicyBlockedError,
  QuorumGovernor,
  SwappableGovernor,
  decodePolicyEvidence,
  encodePolicyEvidence,
  verdictAsStr,
  verifyEvidenceChain,
  type ActionGovernor,
  type PolicyEvidence
} from "../../src/index.js"
import { GovernorState } from "../../src/govern.js"
import { decodeOne, encodeNamed } from "../../src/wire/cbor.js"

const encoder = new TextEncoder()

const allow: ActionGovernor = { decide: () => Promise.resolve(ActionDecision.allow()) }
const block: ActionGovernor = {
  decide: () => Promise.resolve(ActionDecision.block("refused"))
}

function action(conversation = ConversationId.derive("govern")) {
  return {
    kind: ActionKind.Send,
    stream: "laser",
    topic: "agent.sessions",
    source: "planner",
    conversation,
    payload: encoder.encode("wire-funds"),
    signed: false
  } as const
}

void test("given_enforce_mode_when_blocked_then_should_emit_evidence_before_rejecting", async () => {
  const state = new GovernorState(block, GovernorMode.Enforce, () => 42n)
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
  assert.equal(first.atMicros, 42n)
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

void test("given_an_expired_chain_head_when_reused_then_should_start_a_new_local_chain", async () => {
  let now = 1n
  const observe: ActionGovernor = { decide: () => Promise.resolve(ActionDecision.observe()) }
  const state = new GovernorState(observe, GovernorMode.Enforce, () => now, {
    capacity: 1,
    idleTtlMs: 0
  })
  const evidence: PolicyEvidence[] = []

  await state.govern(action(), (item) => {
    evidence.push(item)
    return Promise.resolve()
  })
  now = 2n
  await state.govern(action(), (item) => {
    evidence.push(item)
    return Promise.resolve()
  })

  assert.equal(evidence[0]?.previousDigest, undefined)
  assert.equal(evidence[1]?.previousDigest, undefined)
})

void test("given_a_fractional_idle_ttl_when_constructed_then_should_accept_it_and_refuse_a_non_finite_one", () => {
  const observe: ActionGovernor = { decide: () => Promise.resolve(ActionDecision.observe()) }
  assert.doesNotThrow(
    () => new GovernorState(observe, GovernorMode.Enforce, undefined, { idleTtlMs: 0.5 })
  )
  assert.throws(
    () => new GovernorState(observe, GovernorMode.Enforce, undefined, { idleTtlMs: Number.NaN }),
    InvalidError
  )
  assert.throws(
    () => new GovernorState(observe, GovernorMode.Enforce, undefined, { idleTtlMs: -1 }),
    InvalidError
  )
})

void test("given_a_full_chain_table_when_a_slot_releases_then_should_admit_without_growing", async () => {
  const observe: ActionGovernor = { decide: () => Promise.resolve(ActionDecision.observe()) }
  const state = new GovernorState(observe, GovernorMode.Enforce, undefined, {
    capacity: 1,
    idleTtlMs: 60_000
  })
  let releaseFirst: (() => void) | undefined
  let secondEmitted = false
  const first = state.govern(action(ConversationId.derive("first")), () => {
    return new Promise<void>((resolve) => {
      releaseFirst = resolve
    })
  })
  while (releaseFirst === undefined) await Promise.resolve()
  const second = state.govern(action(ConversationId.derive("second")), () => {
    secondEmitted = true
    return Promise.resolve()
  })

  await Promise.resolve()
  assert.equal(secondEmitted, false)
  releaseFirst()
  await first
  await second
  assert.equal(secondEmitted, true)
})

void test("given_an_unknown_evidence_vocabulary_when_decoded_then_should_reject_it", async () => {
  const evidence: PolicyEvidence[] = []
  await new GovernorState(block, GovernorMode.Observe).govern(action(), (item) => {
    evidence.push(item)
    return Promise.resolve()
  })
  const first = evidence[0]
  assert.ok(first !== undefined)
  const map = decodeOne(encodePolicyEvidence(first), "PolicyEvidence")
  assert.ok(map instanceof Map)
  map.set("decision", "unknown")
  assert.throws(() => decodePolicyEvidence(encodeNamed(map)), CodecError)
})

void test("given_quorum_voters_when_combined_then_should_require_the_configured_threshold", async () => {
  const quorum = new QuorumGovernor({ kind: "at-least", required: 2 })
    .voter("allow", allow, false)
    .voter("block", block, false)
  assert.equal(
    (await quorum.decide({ ...action(), counters: { sends: 0n, requests: 0n, bytesSent: 0n } }))
      .verdict.kind,
    "block"
  )
  quorum.voter("allow-2", allow, false)
  assert.equal(
    (await quorum.decide({ ...action(), counters: { sends: 0n, requests: 0n, bytesSent: 0n } }))
      .verdict.kind,
    "allow"
  )
})

function governedProbe() {
  return { ...action(), counters: { sends: 0n, requests: 0n, bytesSent: 0n } }
}

function fixed(decision: ActionDecision): ActionGovernor {
  return { decide: () => Promise.resolve(decision) }
}

const failing: ActionGovernor = { decide: () => Promise.reject(new Error("voter offline")) }

void test("given_duplicate_voter_names_when_decided_then_should_block", async () => {
  const quorum = new QuorumGovernor({ kind: "any" })
    .voter("same", allow, false)
    .voter("same", allow, false)
  const decision = await quorum.decide(governedProbe())
  assert.equal(decision.verdict.kind, "block")
  assert.match(decision.reason ?? "", /configured more than once/)
})

void test("given_a_non_mandatory_voter_error_when_another_allows_then_should_abstain_and_allow", async () => {
  const quorum = new QuorumGovernor({ kind: "any" })
    .voter("flaky", failing, false)
    .voter("llm", allow, false)
  const decision = await quorum.decide(governedProbe())
  assert.equal(decision.verdict.kind, "allow")
  assert.equal(decision.reason, "quorum(Any): flaky=error(voter offline), llm=allow")
})

void test("given_a_voter_that_throws_synchronously_when_another_allows_then_should_abstain_and_allow", async () => {
  const throwing: ActionGovernor = {
    decide: () => {
      throw new Error("voter crashed")
    }
  }
  const quorum = new QuorumGovernor({ kind: "any" })
    .voter("broken", throwing, false)
    .voter("llm", allow, false)
  const decision = await quorum.decide(governedProbe())
  assert.equal(decision.verdict.kind, "allow")
  assert.equal(decision.reason, "quorum(Any): broken=error(voter crashed), llm=allow")
})

void test("given_a_mandatory_voter_error_when_another_allows_then_should_block", async () => {
  const quorum = new QuorumGovernor({ kind: "any" })
    .voter("safety", failing, true)
    .voter("llm", allow, false)
  const decision = await quorum.decide(governedProbe())
  assert.equal(decision.verdict.kind, "block")
  assert.match(decision.reason ?? "", /^mandatory voter 'safety' failed; quorum\(Any\)/)
})

void test("given_conflicting_modifications_when_quorum_met_then_should_block", async () => {
  const quorum = new QuorumGovernor({ kind: "all" })
    .voter("one", fixed(ActionDecision.modify(encoder.encode("one"))), false)
    .voter("two", fixed(ActionDecision.modify(encoder.encode("two"))), false)
  assert.equal((await quorum.decide(governedProbe())).verdict.kind, "block")
})

void test("given_matching_modifications_when_quorum_met_then_should_apply_the_body", async () => {
  const quorum = new QuorumGovernor({ kind: "all" })
    .voter("one", fixed(ActionDecision.modify(encoder.encode("same"))), false)
    .voter("two", fixed(ActionDecision.modify(encoder.encode("same"))), false)
    .voter("three", allow, false)
  const decision = await quorum.decide(governedProbe())
  assert.deepEqual(decision.verdict, { kind: "modify", body: encoder.encode("same") })
})

void test("given_an_unmet_quorum_without_denials_when_decided_then_should_block", async () => {
  const quorum = new QuorumGovernor({ kind: "at-least", required: 2 })
    .voter("flaky", failing, false)
    .voter("llm", allow, false)
  const decision = await quorum.decide(governedProbe())
  assert.equal(decision.verdict.kind, "block")
  assert.match(
    decision.reason ?? "",
    /^no voter reached the required quorum; quorum\(AtLeast\(2\)\)/
  )
})

void test("given_a_voter_reason_when_annotated_then_should_keep_it_before_the_ballot", async () => {
  const quorum = new QuorumGovernor({ kind: "all" })
    .voter("a", allow, false)
    .voter("b", block, false)
  const decision = await quorum.decide(governedProbe())
  assert.equal(decision.verdict.kind, "block")
  assert.equal(decision.reason, "refused; quorum(All): a=allow, b=block")
})

void test("given_a_step_up_and_a_defer_without_quorum_when_decided_then_should_prefer_step_up", async () => {
  const quorum = new QuorumGovernor({ kind: "all" })
    .voter("reviewer", fixed(ActionDecision.stepUp("storage:rotate")), false)
    .voter("scheduler", fixed(ActionDecision.defer("later")), false)
  assert.equal((await quorum.decide(governedProbe())).verdict.kind, "step_up")
})

void test("given_a_fractional_threshold_when_decided_then_should_block", async () => {
  const quorum = new QuorumGovernor({ kind: "at-least", required: 1.5 })
    .voter("a", allow, false)
    .voter("b", allow, false)
  assert.equal((await quorum.decide(governedProbe())).verdict.kind, "block")
})

void test("given_a_swappable_governor_when_replaced_then_should_use_the_new_policy", async () => {
  const swappable = new SwappableGovernor(allow)
  const governed = { ...action(), counters: { sends: 0n, requests: 0n, bytesSent: 0n } }
  assert.equal((await swappable.decide(governed)).verdict.kind, "allow")
  swappable.swap(block)
  assert.equal((await swappable.decide(governed)).verdict.kind, "block")
})

void test("given_each_verdict_when_named_then_should_use_the_pinned_evidence_name", () => {
  assert.deepEqual(
    [
      ActionDecision.allow(),
      ActionDecision.observe(),
      ActionDecision.block("no"),
      ActionDecision.stepUp("deploys"),
      ActionDecision.modify(new Uint8Array([1])),
      ActionDecision.defer("later")
    ].map((decision) => verdictAsStr(decision.verdict)),
    ["allow", "observe", "block", "step_up", "modify", "defer"]
  )
})

void test("given_observe_mode_when_the_evidence_write_fails_then_should_warn_and_proceed", async (t) => {
  const warnings: string[] = []
  t.mock.method(process, "emitWarning", (message: string) => {
    warnings.push(message)
  })
  const state = new GovernorState(block, GovernorMode.Observe)
  const payload = await state.govern(action(), () => Promise.reject(new Error("audit topic down")))
  assert.deepEqual(payload, encoder.encode("wire-funds"))
  assert.equal(warnings.length, 1)
  assert.match(warnings[0] ?? "", /observe mode.*audit topic down/u)
})

void test("given_a_zero_retention_capacity_when_constructed_then_should_keep_one_chain", async () => {
  const state = new GovernorState(allow, GovernorMode.Observe, undefined, { capacity: 0 })
  await state.govern(action(), () => Promise.resolve())
  assert.throws(
    () => new GovernorState(allow, GovernorMode.Observe, undefined, { capacity: -1 }),
    InvalidError
  )
})

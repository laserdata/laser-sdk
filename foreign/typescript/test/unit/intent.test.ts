import assert from "node:assert/strict"
import { test } from "node:test"
import {
  AgentId,
  ConversationId,
  Intent,
  IntentError,
  IntentOutcome,
  Vote,
  VoteChoice,
  decide
} from "../../src/index.js"

function build(policy: ConstructorParameters<typeof Intent>[0]["policy"]): Intent {
  return new Intent({
    conversation: ConversationId.new(),
    proposer: AgentId.new("proposer"),
    body: new TextEncoder().encode("transfer $100"),
    eligibleVoters: [AgentId.new("a"), AgentId.new("b")],
    policy,
    policyVersion: 1n,
    atMicros: 10n,
    deadlineMicros: 1_000n
  })
}

void test("given_invalid_voters_and_thresholds_when_constructed_then_should_fail_closed", () => {
  assert.throws(
    () =>
      new Intent({
        conversation: ConversationId.new(),
        proposer: AgentId.new("proposer"),
        body: new Uint8Array(),
        eligibleVoters: [],
        policy: { kind: "any" },
        policyVersion: 1n,
        atMicros: 1n,
        deadlineMicros: 2n
      }),
    IntentError
  )
  assert.throws(() => build({ kind: "at-least", required: 3 }), IntentError)
})

void test("given_quorum_and_mandatory_votes_when_folded_then_should_commit_deterministically", () => {
  const base = build({ kind: "at-least", required: 1 })
  const intent = new Intent({
    intentId: base.intentId,
    conversation: base.conversation,
    proposer: base.proposer,
    body: base.body,
    eligibleVoters: base.eligibleVoters,
    mandatoryVoters: [AgentId.new("b")],
    policy: base.policy,
    policyVersion: base.policyVersion,
    atMicros: base.atMicros,
    deadlineMicros: base.deadlineMicros,
    digest: base.digest
  })
  const votes = [
    Vote.cast(intent, AgentId.new("b"), VoteChoice.Allow, 20n),
    Vote.cast(intent, AgentId.new("a"), VoteChoice.Allow, 30n)
  ]
  const forward = decide(intent, votes, 40n)
  const reverse = decide(intent, votes.toReversed(), 40n)
  assert.deepEqual(reverse, forward)
  assert.ok(forward !== undefined)
  assert.equal(forward.outcome, IntentOutcome.Committed)
  assert.equal(forward.authorizes(intent), true)
})

void test("given_conflicting_or_expired_votes_when_folded_then_should_abort", () => {
  const intent = build({ kind: "any" })
  const conflicting = [
    Vote.cast(intent, AgentId.new("a"), VoteChoice.Allow, 20n),
    Vote.cast(intent, AgentId.new("a"), VoteChoice.Block, 30n)
  ]
  assert.equal(decide(intent, conflicting, 40n)?.outcome, IntentOutcome.Aborted)
  assert.equal(decide(intent, [], 1_000n)?.reason, "quorum not reached by deadline")
})

void test("given_a_mutated_body_or_foreign_voter_when_used_then_should_reject", () => {
  const intent = build({ kind: "any" })
  intent.body[0] = 0
  assert.throws(() => decide(intent, [], 20n), IntentError)
  const valid = build({ kind: "any" })
  assert.throws(() => Vote.cast(valid, AgentId.new("outsider"), VoteChoice.Allow, 20n), IntentError)
})

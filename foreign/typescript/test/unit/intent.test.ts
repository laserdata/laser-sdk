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
    deadlineMicros: nowMicros() + MINUTE_MICROS
  })
}

const MINUTE_MICROS = 60_000_000n

function nowMicros(): bigint {
  return BigInt(Date.now()) * 1_000n
}

// A moment after every vote cast so far and before any deadline.
function soon(): bigint {
  return nowMicros() + 1_000_000n
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
        deadlineMicros: nowMicros() + MINUTE_MICROS
      }),
    IntentError
  )
  assert.throws(() => build({ kind: "at-least", required: 3 }), IntentError)
})

void test("given_quorum_and_mandatory_votes_when_folded_then_should_commit_deterministically", () => {
  const intent = new Intent({
    conversation: ConversationId.new(),
    proposer: AgentId.new("proposer"),
    body: new TextEncoder().encode("transfer $100"),
    eligibleVoters: [AgentId.new("a"), AgentId.new("b")],
    mandatoryVoters: [AgentId.new("b")],
    policy: { kind: "at-least", required: 1 },
    policyVersion: 1n,
    deadlineMicros: nowMicros() + MINUTE_MICROS
  })
  const votes = [
    Vote.cast(intent, AgentId.new("b"), VoteChoice.Allow),
    Vote.cast(intent, AgentId.new("a"), VoteChoice.Allow)
  ]
  const at = soon()
  const forward = decide(intent, votes, at)
  const reverse = decide(intent, votes.toReversed(), at)
  assert.deepEqual(reverse, forward)
  assert.ok(forward !== undefined)
  assert.equal(forward.outcome, IntentOutcome.Committed)
  assert.equal(forward.authorizes(intent), true)
})

void test("given_conflicting_or_expired_votes_when_folded_then_should_abort", () => {
  const intent = build({ kind: "any" })
  const conflicting = [
    Vote.cast(intent, AgentId.new("a"), VoteChoice.Allow),
    Vote.cast(intent, AgentId.new("a"), VoteChoice.Block)
  ]
  assert.equal(decide(intent, conflicting, soon())?.outcome, IntentOutcome.Aborted)
  assert.equal(decide(intent, [], intent.deadlineMicros)?.reason, "quorum not reached by deadline")
})

void test("given_a_mutated_body_or_foreign_voter_when_used_then_should_reject", () => {
  const intent = build({ kind: "any" })
  intent.body[0] = 0
  assert.throws(() => decide(intent, [], soon()), IntentError)
  const valid = build({ kind: "any" })
  assert.throws(() => Vote.cast(valid, AgentId.new("outsider"), VoteChoice.Allow), IntentError)
})

void test("given_each_invalid_configuration_when_constructed_then_should_raise_its_rust_variant", () => {
  const a = AgentId.new("a")
  const options = {
    conversation: ConversationId.new(),
    proposer: AgentId.new("proposer"),
    body: new Uint8Array(),
    eligibleVoters: [a],
    policy: { kind: "any" } as const,
    policyVersion: 1n,
    deadlineMicros: nowMicros() + MINUTE_MICROS
  }
  const cases: readonly [() => unknown, IntentError][] = [
    [() => new Intent({ ...options, eligibleVoters: [] }), IntentError.noEligibleVoters()],
    [
      () => new Intent({ ...options, eligibleVoters: [a, a] }),
      IntentError.duplicateEligibleVoter("a")
    ],
    [
      () => new Intent({ ...options, mandatoryVoters: [a, a] }),
      IntentError.duplicateMandatoryVoter("a")
    ],
    [
      () => new Intent({ ...options, mandatoryVoters: [AgentId.new("b")] }),
      IntentError.mandatoryVoterNotEligible("b")
    ],
    [
      () => new Intent({ ...options, policy: { kind: "at-least", required: 2 } }),
      IntentError.invalidThreshold(2, 1)
    ],
    [() => new Intent({ ...options, deadlineMicros: 10n }), IntentError.invalidDeadline(0n, 10n)]
  ]
  for (const [construct, expected] of cases) {
    assert.throws(construct, (error: unknown) => {
      assert.ok(error instanceof IntentError)
      if (expected.message.startsWith("deadline")) {
        assert.match(error.message, /^deadline 10 must be after proposal time \d+$/u)
      } else {
        assert.equal(error.message, expected.message)
        assert.deepEqual(error.context, expected.context)
      }
      return true
    })
  }
  assert.equal(
    IntentError.invalidThreshold(2, 1).message,
    "threshold 2 is invalid for 1 eligible voters"
  )
  assert.deepEqual(IntentError.invalidDeadline(10n, 5n).context, { proposed: 10n, deadline: 5n })
  const intent = new Intent(options)
  assert.throws(
    () => Vote.cast(intent, AgentId.new("outsider"), VoteChoice.Allow),
    (error: unknown) =>
      error instanceof IntentError &&
      error.message === IntentError.ineligibleVoter("outsider").message
  )
  const other = new Intent(options)
  const decision = decide(other, [Vote.cast(other, a, VoteChoice.Allow)], soon())
  assert.throws(
    () => decision?.authorizes(intent),
    (error: unknown) =>
      error instanceof IntentError && error.message === IntentError.decisionIntentMismatch().message
  )
})

void test("given_a_new_intent_and_vote_when_created_then_should_stamp_the_current_time", () => {
  const before = nowMicros()
  const intent = build({ kind: "any" })
  const vote = Vote.cast(intent, AgentId.new("a"), VoteChoice.Allow)
  const after = nowMicros()
  assert.ok(intent.atMicros >= before && intent.atMicros <= after)
  assert.ok(vote.atMicros >= intent.atMicros && vote.atMicros <= after)
})

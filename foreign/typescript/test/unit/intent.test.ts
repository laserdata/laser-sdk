import assert from "node:assert/strict"
import { test } from "node:test"
import {
  AgentId,
  ConversationId,
  Decision,
  Intent,
  IntentError,
  IntentOutcome,
  Vote,
  VoteChoice,
  decide,
  type ObservedVote
} from "../../src/index.js"

function build(policy: ConstructorParameters<typeof Intent>[0]["policy"]): Intent {
  return new Intent({
    conversation: ConversationId.new(),
    proposer: AgentId.new("proposer"),
    body: new TextEncoder().encode("transfer $100"),
    eligibleVoters: [AgentId.new("a"), AgentId.new("b")],
    policy,
    policyVersion: 1n,
    deadlineAtMicros: nowMicros() + MINUTE_MICROS
  })
}

const MINUTE_MICROS = 60_000_000n

function nowMicros(): bigint {
  return BigInt(Date.now()) * 1_000n
}

// Offline ballots are observed when they were cast.
function observed(votes: readonly Vote[]): ObservedVote[] {
  return votes.map((vote) => ({ vote, observedAtMicros: vote.atMicros }))
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
        deadlineAtMicros: nowMicros() + MINUTE_MICROS
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
    deadlineAtMicros: nowMicros() + MINUTE_MICROS
  })
  const votes = [
    Vote.cast(intent, AgentId.new("b"), VoteChoice.Allow),
    Vote.cast(intent, AgentId.new("a"), VoteChoice.Allow)
  ]
  const at = soon()
  const forward = decide(intent, observed(votes), at)
  const reverse = decide(intent, observed(votes.toReversed()), at)
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
  assert.equal(decide(intent, observed(conflicting), soon())?.outcome, IntentOutcome.Aborted)
  assert.equal(
    decide(intent, [], intent.deadlineAtMicros)?.reason,
    "quorum not reached by deadline"
  )
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
    deadlineAtMicros: nowMicros() + MINUTE_MICROS
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
    [() => new Intent({ ...options, deadlineAtMicros: 10n }), IntentError.invalidDeadline(0n, 10n)]
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
  const decision = decide(other, observed([Vote.cast(other, a, VoteChoice.Allow)]), soon())
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

// The JSON Rust and Python write for one intent, pinned so every client
// rebuilds the same record and digest.
const RUST_INTENT_JSON = {
  intent_id: "01JC0000000000000000000000",
  conversation: "01JC0000000000000000000001",
  proposer: "proposer",
  body: [116, 114, 97, 110, 115, 102, 101, 114, 32, 36, 49, 48, 48],
  digest: "93daa878cad59dae1a8b72cba78631d5e752940cd3225723b8e5e1b00f51726a",
  eligible_voters: ["a", "b"],
  mandatory_voters: ["b"],
  policy: { at_least: 1 },
  policy_version: 1,
  deadline_at_micros: 2_000_000_000_000_000,
  at_micros: 1_700_000_000_000_000
}

void test("given_a_known_body_when_digested_then_should_match_the_pinned_digest", () => {
  assert.equal(build({ kind: "any" }).digest, RUST_INTENT_JSON.digest)
})

void test("given_the_rust_json_of_an_intent_when_rebuilt_then_should_validate_and_round_trip", () => {
  const intent = Intent.fromJSON(JSON.stringify(RUST_INTENT_JSON))
  assert.equal(intent.intentId.toString(), RUST_INTENT_JSON.intent_id)
  assert.equal(intent.proposer.asStr(), "proposer")
  assert.deepEqual(intent.policy, { kind: "at-least", required: 1 })
  assert.equal(intent.atMicros, 1_700_000_000_000_000n)
  assert.deepEqual(JSON.parse(JSON.stringify(intent)), RUST_INTENT_JSON)
  const tampered = { ...RUST_INTENT_JSON, body: [0, ...RUST_INTENT_JSON.body.slice(1)] }
  assert.throws(() => Intent.fromJSON(tampered), IntentError)
  assert.throws(() => Intent.fromJSON({ ...RUST_INTENT_JSON, policy: "most" }), IntentError)
})

void test("given_a_committed_decision_without_votes_when_authorizing_then_should_refuse_it", () => {
  const intent = build({ kind: "any" })
  const forged = new Decision(
    intent.intentId,
    intent.digest,
    intent.policyVersion,
    IntentOutcome.Committed,
    "quorum met",
    [],
    soon()
  )
  assert.throws(
    () => forged.authorizes(intent),
    (error: unknown) =>
      error instanceof IntentError && error.message === IntentError.decisionQuorumNotMet().message
  )
  const valid = decide(
    intent,
    observed([Vote.cast(intent, AgentId.new("a"), VoteChoice.Allow)]),
    soon()
  )
  assert.equal(valid?.authorizes(intent), true)
})

void test("given_a_ballot_claiming_an_in_window_time_but_observed_after_the_deadline_when_decided_then_should_not_count", () => {
  const intent = build({ kind: "any" })
  const backdated = new Vote(
    intent.intentId,
    intent.digest,
    intent.policyVersion,
    AgentId.new("a"),
    VoteChoice.Allow,
    intent.atMicros + 1n
  )
  const late = intent.deadlineAtMicros + 1n
  const decision = decide(intent, [{ vote: backdated, observedAtMicros: late }], late)
  assert.equal(decision?.outcome, IntentOutcome.Aborted)
  assert.deepEqual(decision.votesConsidered, [])
})

import { blake3 } from "@noble/hashes/blake3.js"
import { bytesToHex } from "@noble/hashes/utils.js"
import { InvalidError } from "./client/errors.js"
import { compareCodePoints } from "./runtime/compare.js"
import { IntentId, type AgentId, type ConversationId } from "./types/ids.js"
import { encodeNamed } from "./wire/cbor.js"

export const VoteChoice = { Allow: "allow", Block: "block", Abstain: "abstain" } as const
export type VoteChoice = (typeof VoteChoice)[keyof typeof VoteChoice]

export const IntentOutcome = { Committed: "committed", Aborted: "aborted" } as const
export type IntentOutcome = (typeof IntentOutcome)[keyof typeof IntentOutcome]

export type IntentPolicy =
  | { readonly kind: "all" }
  | { readonly kind: "any" }
  | { readonly kind: "at-least"; readonly required: number }

export interface IntentOptions {
  readonly conversation: ConversationId
  readonly proposer: AgentId
  readonly body: Uint8Array
  readonly eligibleVoters: readonly AgentId[]
  readonly mandatoryVoters?: readonly AgentId[]
  readonly policy: IntentPolicy
  readonly policyVersion: bigint
  readonly deadlineMicros: bigint
}

/** An invalid durable-intent configuration or operation. Each static
 * constructor is one Rust `IntentError` variant, with the same message and its
 * payload in `context`. */
export class IntentError extends InvalidError {
  static noEligibleVoters(): IntentError {
    return new IntentError("an intent requires at least one eligible voter")
  }

  static duplicateEligibleVoter(voter: string): IntentError {
    return new IntentError(`eligible voter '${voter}' appears more than once`, { voter })
  }

  static duplicateMandatoryVoter(voter: string): IntentError {
    return new IntentError(`mandatory voter '${voter}' appears more than once`, { voter })
  }

  static mandatoryVoterNotEligible(voter: string): IntentError {
    return new IntentError(`mandatory voter '${voter}' is not eligible`, { voter })
  }

  static invalidThreshold(required: number, eligible: number): IntentError {
    return new IntentError(
      `threshold ${String(required)} is invalid for ${String(eligible)} eligible voters`,
      { required, eligible }
    )
  }

  static invalidDeadline(proposed: bigint, deadline: bigint): IntentError {
    return new IntentError(
      `deadline ${String(deadline)} must be after proposal time ${String(proposed)}`,
      { proposed, deadline }
    )
  }

  static digestMismatch(): IntentError {
    return new IntentError("intent digest does not match its body")
  }

  static ineligibleVoter(voter: string): IntentError {
    return new IntentError(`voter '${voter}' is not eligible for this intent`, { voter })
  }

  static decisionIntentMismatch(): IntentError {
    return new IntentError("decision is not bound to this intent body and policy version")
  }
}

export class Intent {
  readonly intentId: IntentId
  readonly conversation: ConversationId
  readonly proposer: AgentId
  readonly body: Uint8Array
  readonly digest: string
  readonly eligibleVoters: readonly AgentId[]
  readonly mandatoryVoters: readonly AgentId[]
  readonly policy: IntentPolicy
  readonly policyVersion: bigint
  readonly deadlineMicros: bigint
  readonly atMicros: bigint

  /** Mints the intent id, hashes the body, and stamps the proposal time now,
   * like Rust `Intent::new`. */
  constructor(options: IntentOptions) {
    this.intentId = IntentId.new()
    this.conversation = options.conversation
    this.proposer = options.proposer
    this.body = options.body.slice()
    this.digest = digestOf(this.body)
    this.eligibleVoters = [...options.eligibleVoters]
    this.mandatoryVoters = [...(options.mandatoryVoters ?? [])]
    this.policy = options.policy
    this.policyVersion = options.policyVersion
    this.deadlineMicros = options.deadlineMicros
    this.atMicros = BigInt(Date.now()) * 1_000n
    this.validate()
  }

  validate(): void {
    if (this.eligibleVoters.length === 0) throw IntentError.noEligibleVoters()
    const eligible = uniqueAgents(this.eligibleVoters, (voter) =>
      IntentError.duplicateEligibleVoter(voter)
    )
    const mandatory = uniqueAgents(this.mandatoryVoters, (voter) =>
      IntentError.duplicateMandatoryVoter(voter)
    )
    for (const voter of mandatory) {
      if (!eligible.has(voter)) throw IntentError.mandatoryVoterNotEligible(voter)
    }
    if (this.policy.kind === "at-least") {
      if (
        !Number.isSafeInteger(this.policy.required) ||
        this.policy.required < 1 ||
        this.policy.required > this.eligibleVoters.length
      ) {
        throw IntentError.invalidThreshold(this.policy.required, this.eligibleVoters.length)
      }
    }
    if (this.deadlineMicros <= this.atMicros) {
      throw IntentError.invalidDeadline(this.atMicros, this.deadlineMicros)
    }
    if (this.digest !== digestOf(this.body)) throw IntentError.digestMismatch()
  }
}

export class Vote {
  constructor(
    readonly intentId: IntentId,
    readonly intentDigest: string,
    readonly policyVersion: bigint,
    readonly voter: AgentId,
    readonly choice: VoteChoice,
    readonly atMicros: bigint = BigInt(Date.now()) * 1_000n
  ) {}

  /** A ballot for `intent` from an eligible `voter`, stamped now like Rust `Vote::cast`. */
  static cast(intent: Intent, voter: AgentId, choice: VoteChoice): Vote {
    intent.validate()
    if (!intent.eligibleVoters.some((eligible) => eligible.equals(voter))) {
      throw IntentError.ineligibleVoter(voter.toString())
    }
    return new Vote(intent.intentId, intent.digest, intent.policyVersion, voter, choice)
  }
}

export class Decision {
  constructor(
    readonly intentId: IntentId,
    readonly intentDigest: string,
    readonly policyVersion: bigint,
    readonly outcome: IntentOutcome,
    readonly reason: string,
    readonly votesConsidered: readonly (readonly [AgentId, VoteChoice])[],
    readonly atMicros: bigint
  ) {}

  authorizes(intent: Intent): boolean {
    intent.validate()
    if (
      !this.intentId.equals(intent.intentId) ||
      this.intentDigest !== intent.digest ||
      this.policyVersion !== intent.policyVersion
    ) {
      throw IntentError.decisionIntentMismatch()
    }
    return this.outcome === IntentOutcome.Committed
  }
}

export function decide(
  intent: Intent,
  votes: readonly Vote[],
  nowMicros: bigint
): Decision | undefined {
  intent.validate()
  const eligible = new Set(intent.eligibleVoters.map((voter) => voter.asStr()))
  const valid = votes
    .filter(
      (vote) =>
        vote.intentId.equals(intent.intentId) &&
        vote.intentDigest === intent.digest &&
        vote.policyVersion === intent.policyVersion &&
        eligible.has(vote.voter.asStr()) &&
        vote.atMicros >= intent.atMicros &&
        vote.atMicros <= intent.deadlineMicros &&
        vote.atMicros <= nowMicros
    )
    .toSorted(
      (left, right) =>
        compareCodePoints(left.voter.asStr(), right.voter.asStr()) ||
        compareBigInt(left.atMicros, right.atMicros) ||
        choiceRank(left.choice) - choiceRank(right.choice)
    )
  const ballots = new Map<string, VoteChoice>()
  const considered: (readonly [AgentId, VoteChoice])[] = []
  let terminalAt = intent.atMicros
  for (const vote of valid) {
    if (vote.atMicros > terminalAt) terminalAt = vote.atMicros
    const voter = vote.voter.asStr()
    const previous = ballots.get(voter)
    if (previous === undefined) {
      ballots.set(voter, vote.choice)
      considered.push([vote.voter, vote.choice])
    } else if (previous !== vote.choice) {
      considered.push([vote.voter, vote.choice])
      return sealed(
        intent,
        IntentOutcome.Aborted,
        `voter '${voter}' cast conflicting votes`,
        considered,
        terminalAt
      )
    }
  }
  for (const voter of intent.mandatoryVoters) {
    const choice = ballots.get(voter.asStr())
    if (choice !== undefined && choice !== VoteChoice.Allow) {
      return sealed(
        intent,
        IntentOutcome.Aborted,
        `mandatory voter '${voter.toString()}' did not allow`,
        considered,
        terminalAt
      )
    }
  }
  const allow = [...ballots.values()].filter((choice) => choice === VoteChoice.Allow).length
  const responded = ballots.size
  const total = intent.eligibleVoters.length
  const mandatoryMet = intent.mandatoryVoters.every(
    (voter) => ballots.get(voter.asStr()) === VoteChoice.Allow
  )
  const quorumMet =
    intent.policy.kind === "all"
      ? allow === total
      : intent.policy.kind === "any"
        ? allow >= 1
        : allow >= intent.policy.required
  if (quorumMet && mandatoryMet) {
    return sealed(intent, IntentOutcome.Committed, "quorum met", considered, terminalAt)
  }
  const impossible =
    intent.policy.kind === "all"
      ? responded > allow
      : intent.policy.kind === "any"
        ? responded === total && allow === 0
        : allow + (total - responded) < intent.policy.required
  const deadlinePassed = nowMicros >= intent.deadlineMicros
  if (!impossible && !deadlinePassed) return undefined
  const reason =
    deadlinePassed && !mandatoryMet
      ? "mandatory approval not reached by deadline"
      : impossible
        ? "quorum became impossible"
        : "quorum not reached by deadline"
  return sealed(
    intent,
    IntentOutcome.Aborted,
    reason,
    considered,
    deadlinePassed ? intent.deadlineMicros : terminalAt
  )
}

function digestOf(body: Uint8Array): string {
  return bytesToHex(blake3(encodeNamed(new Map<string, unknown>([["body", body]]))))
}

function uniqueAgents(
  agents: readonly AgentId[],
  duplicate: (voter: string) => IntentError
): Set<string> {
  const unique = new Set<string>()
  for (const agent of agents) {
    const value = agent.asStr()
    if (unique.has(value)) throw duplicate(value)
    unique.add(value)
  }
  return unique
}

function compareBigInt(left: bigint, right: bigint): number {
  return left < right ? -1 : left > right ? 1 : 0
}

function choiceRank(choice: VoteChoice): number {
  return choice === VoteChoice.Allow ? 0 : choice === VoteChoice.Block ? 1 : 2
}

function sealed(
  intent: Intent,
  outcome: IntentOutcome,
  reason: string,
  votes: readonly (readonly [AgentId, VoteChoice])[],
  atMicros: bigint
): Decision {
  return new Decision(
    intent.intentId,
    intent.digest,
    intent.policyVersion,
    outcome,
    reason,
    [...votes],
    atMicros
  )
}

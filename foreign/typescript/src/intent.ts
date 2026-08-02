import { blake3 } from "@noble/hashes/blake3.js"
import { bytesToHex } from "@noble/hashes/utils.js"
import { InvalidError } from "./client/errors.js"
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
  readonly intentId?: IntentId
  readonly atMicros?: bigint
}

export class IntentError extends InvalidError {}

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

  constructor(options: IntentOptions & { readonly digest?: string }) {
    this.intentId = options.intentId ?? IntentId.new()
    this.conversation = options.conversation
    this.proposer = options.proposer
    this.body = options.body.slice()
    this.digest = options.digest ?? digestOf(this.body)
    this.eligibleVoters = [...options.eligibleVoters]
    this.mandatoryVoters = [...(options.mandatoryVoters ?? [])]
    this.policy = options.policy
    this.policyVersion = options.policyVersion
    this.deadlineMicros = options.deadlineMicros
    this.atMicros = options.atMicros ?? BigInt(Date.now()) * 1_000n
    this.validate()
  }

  validate(): void {
    if (this.eligibleVoters.length === 0)
      throw new IntentError("an intent requires eligible voters")
    const eligible = uniqueAgents(this.eligibleVoters, "eligible")
    const mandatory = uniqueAgents(this.mandatoryVoters, "mandatory")
    for (const voter of mandatory) {
      if (!eligible.has(voter)) throw new IntentError(`mandatory voter '${voter}' is not eligible`)
    }
    if (this.policy.kind === "at-least") {
      if (
        !Number.isSafeInteger(this.policy.required) ||
        this.policy.required < 1 ||
        this.policy.required > this.eligibleVoters.length
      ) {
        throw new IntentError(
          `threshold ${String(this.policy.required)} is invalid for ${String(this.eligibleVoters.length)} eligible voters`
        )
      }
    }
    if (this.deadlineMicros <= this.atMicros) {
      throw new IntentError("intent deadline must be after proposal time")
    }
    if (this.digest !== digestOf(this.body))
      throw new IntentError("intent digest does not match its body")
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

  static cast(intent: Intent, voter: AgentId, choice: VoteChoice, atMicros?: bigint): Vote {
    intent.validate()
    if (!intent.eligibleVoters.some((eligible) => eligible.equals(voter))) {
      throw new IntentError(`voter '${voter.toString()}' is not eligible for this intent`)
    }
    return new Vote(intent.intentId, intent.digest, intent.policyVersion, voter, choice, atMicros)
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
      throw new IntentError("decision is not bound to this intent body and policy version")
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
  const eligible = new Set(intent.eligibleVoters.map((voter) => voter.asString()))
  const valid = votes
    .filter(
      (vote) =>
        vote.intentId.equals(intent.intentId) &&
        vote.intentDigest === intent.digest &&
        vote.policyVersion === intent.policyVersion &&
        eligible.has(vote.voter.asString()) &&
        vote.atMicros >= intent.atMicros &&
        vote.atMicros <= intent.deadlineMicros &&
        vote.atMicros <= nowMicros
    )
    .toSorted(
      (left, right) =>
        left.voter.asString().localeCompare(right.voter.asString()) ||
        compareBigInt(left.atMicros, right.atMicros) ||
        choiceRank(left.choice) - choiceRank(right.choice)
    )
  const ballots = new Map<string, VoteChoice>()
  const considered: (readonly [AgentId, VoteChoice])[] = []
  let terminalAt = intent.atMicros
  for (const vote of valid) {
    if (vote.atMicros > terminalAt) terminalAt = vote.atMicros
    const voter = vote.voter.asString()
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
    const choice = ballots.get(voter.asString())
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
    (voter) => ballots.get(voter.asString()) === VoteChoice.Allow
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

function uniqueAgents(agents: readonly AgentId[], label: string): Set<string> {
  const unique = new Set<string>()
  for (const agent of agents) {
    const value = agent.asString()
    if (unique.has(value)) throw new IntentError(`${label} voter '${value}' appears more than once`)
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

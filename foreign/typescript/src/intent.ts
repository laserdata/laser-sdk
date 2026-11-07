import { blake3 } from "@noble/hashes/blake3.js"
import { bytesToHex } from "@noble/hashes/utils.js"
import { InvalidError } from "./client/errors.js"
import { compareCodePoints } from "./runtime/compare.js"
import { AgentId, ConversationId, IntentId } from "./types/ids.js"
import { encodeNamed } from "./wire/cbor.js"

const U64_MAX = 0xffff_ffff_ffff_ffffn

// Node 22 writes and reads JSON integers beyond 2^53 exactly through these.
const EXACT_JSON = JSON as unknown as { rawJSON(text: string): unknown }

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
  readonly deadlineAtMicros: bigint
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

  static digestEncoding(reason: string): IntentError {
    return new IntentError(`intent body does not encode for its digest: ${reason}`, { reason })
  }

  static ineligibleVoter(voter: string): IntentError {
    return new IntentError(`voter '${voter}' is not eligible for this intent`, { voter })
  }

  static decisionIntentMismatch(): IntentError {
    return new IntentError("decision is not bound to this intent body and policy version")
  }

  static decisionQuorumNotMet(): IntentError {
    return new IntentError("decision votes do not meet this intent's quorum")
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
  readonly deadlineAtMicros: bigint
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
    this.deadlineAtMicros = options.deadlineAtMicros
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
    if (this.deadlineAtMicros <= this.atMicros) {
      throw IntentError.invalidDeadline(this.atMicros, this.deadlineAtMicros)
    }
    if (this.digest !== digestOf(this.body)) throw IntentError.digestMismatch()
  }

  /** The Rust and Python JSON shape of this intent, snake_case fields, the
   * body as an array of bytes, and every integer exact. */
  toJSON(): Readonly<Record<string, unknown>> {
    return {
      intent_id: this.intentId.toString(),
      conversation: this.conversation.toString(),
      proposer: this.proposer.asStr(),
      body: [...this.body],
      digest: this.digest,
      eligible_voters: this.eligibleVoters.map((voter) => voter.asStr()),
      mandatory_voters: this.mandatoryVoters.map((voter) => voter.asStr()),
      policy:
        this.policy.kind === "at-least" ? { at_least: this.policy.required } : this.policy.kind,
      policy_version: jsonInteger(this.policyVersion),
      deadline_at_micros: jsonInteger(this.deadlineAtMicros),
      at_micros: jsonInteger(this.atMicros)
    }
  }

  /** Rebuilds an intent another client published, from `toJSON` output or
   * the Rust and Python JSON, as a string or an already parsed value. The id,
   * digest, and proposal time are kept as written, then validated. */
  static fromJSON(value: unknown): Intent {
    let parsed: unknown = value
    if (typeof value === "string") {
      try {
        parsed = JSON.parse(value, exactIntegers)
      } catch (cause) {
        throw new IntentError("an intent must be JSON", undefined, { cause })
      }
    }
    const fields = jsonObject(parsed)
    const intent = Object.assign(Object.create(Intent.prototype) as Intent, {
      intentId: IntentId.parse(jsonString(fields, "intent_id")),
      conversation: ConversationId.parse(jsonString(fields, "conversation")),
      proposer: AgentId.new(jsonString(fields, "proposer")),
      body: jsonBytes(fields["body"]),
      digest: jsonString(fields, "digest"),
      eligibleVoters: jsonAgents(fields, "eligible_voters"),
      mandatoryVoters: jsonAgents(fields, "mandatory_voters"),
      policy: jsonPolicy(fields["policy"]),
      policyVersion: jsonU64(fields, "policy_version"),
      deadlineAtMicros: jsonU64(fields, "deadline_at_micros"),
      atMicros: jsonU64(fields, "at_micros")
    })
    intent.validate()
    return intent
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

/** One ballot as a reader saw it: the `Vote` and the time the log assigned
 * it, the record's server timestamp in epoch micros. `decide` windows and
 * orders by `observedAtMicros` and keeps the voter's `atMicros` only as
 * recorded evidence. */
export interface ObservedVote {
  readonly vote: Vote
  readonly observedAtMicros: bigint
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
    if (this.outcome !== IntentOutcome.Committed) return false
    if (!commitSupported(intent, this.votesConsidered)) throw IntentError.decisionQuorumNotMet()
    return true
  }
}

/** Folds `votes` into a decision, or `undefined` while the outcome remains
 * reachable before the deadline. Each ballot is windowed and ordered by its
 * `observedAtMicros`, never by the voter's own claim, so a late ballot cannot
 * claim an earlier time. */
export function decide(
  intent: Intent,
  votes: readonly ObservedVote[],
  nowMicros: bigint
): Decision | undefined {
  intent.validate()
  const eligible = new Set(intent.eligibleVoters.map((voter) => voter.asStr()))
  const valid = votes
    .filter(
      ({ vote, observedAtMicros }) =>
        vote.intentId.equals(intent.intentId) &&
        vote.intentDigest === intent.digest &&
        vote.policyVersion === intent.policyVersion &&
        eligible.has(vote.voter.asStr()) &&
        observedAtMicros >= intent.atMicros &&
        observedAtMicros <= intent.deadlineAtMicros &&
        observedAtMicros <= nowMicros
    )
    .toSorted(
      (left, right) =>
        compareCodePoints(left.vote.voter.asStr(), right.vote.voter.asStr()) ||
        compareBigInt(left.observedAtMicros, right.observedAtMicros) ||
        choiceRank(left.vote.choice) - choiceRank(right.vote.choice)
    )
  const ballots = new Map<string, VoteChoice>()
  const considered: (readonly [AgentId, VoteChoice])[] = []
  let terminalAt = intent.atMicros
  for (const { vote, observedAtMicros } of valid) {
    if (observedAtMicros > terminalAt) terminalAt = observedAtMicros
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
  if (quorumMet(intent.policy, allow, total) && mandatoryMet) {
    return sealed(intent, IntentOutcome.Committed, "quorum met", considered, terminalAt)
  }
  const impossible =
    intent.policy.kind === "all"
      ? responded > allow
      : intent.policy.kind === "any"
        ? responded === total && allow === 0
        : allow + (total - responded) < intent.policy.required
  const deadlinePassed = nowMicros >= intent.deadlineAtMicros
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
    deadlinePassed ? intent.deadlineAtMicros : terminalAt
  )
}

function quorumMet(policy: IntentPolicy, allow: number, total: number): boolean {
  if (policy.kind === "all") return allow === total
  return allow >= (policy.kind === "any" ? 1 : policy.required)
}

// Whether `votes` alone justify committing `intent`: eligible voters, one
// choice each, every mandatory voter allowing, and the quorum met.
function commitSupported(
  intent: Intent,
  votes: readonly (readonly [AgentId, VoteChoice])[]
): boolean {
  const eligible = new Set(intent.eligibleVoters.map((voter) => voter.asStr()))
  const ballots = new Map<string, VoteChoice>()
  for (const [voter, choice] of votes) {
    const name = voter.asStr()
    if (!eligible.has(name)) return false
    const previous = ballots.get(name)
    if (previous !== undefined && previous !== choice) return false
    ballots.set(name, choice)
  }
  const allow = [...ballots.values()].filter((choice) => choice === VoteChoice.Allow).length
  return (
    intent.mandatoryVoters.every((voter) => ballots.get(voter.asStr()) === VoteChoice.Allow) &&
    quorumMet(intent.policy, allow, intent.eligibleVoters.length)
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

function jsonObject(value: unknown): Readonly<Record<string, unknown>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new IntentError("an intent must be a JSON object")
  }
  return value as Readonly<Record<string, unknown>>
}

function jsonString(fields: Readonly<Record<string, unknown>>, key: string): string {
  const value = fields[key]
  if (typeof value !== "string") throw new IntentError(`intent ${key} must be a string`)
  return value
}

function jsonAgents(fields: Readonly<Record<string, unknown>>, key: string): readonly AgentId[] {
  const value = fields[key]
  if (!Array.isArray(value) || !value.every((item) => typeof item === "string")) {
    throw new IntentError(`intent ${key} must be an array of agent ids`)
  }
  return value.map((name) => AgentId.new(name))
}

function jsonBytes(value: unknown): Uint8Array {
  if (
    !Array.isArray(value) ||
    !value.every(
      (byte) => typeof byte === "number" && Number.isInteger(byte) && byte >= 0 && byte <= 255
    )
  ) {
    throw new IntentError("intent body must be an array of bytes")
  }
  return Uint8Array.from(value as readonly number[])
}

function jsonU64(fields: Readonly<Record<string, unknown>>, key: string): bigint {
  const value = fields[key]
  const integer =
    typeof value === "bigint"
      ? value
      : typeof value === "number" && Number.isSafeInteger(value)
        ? BigInt(value)
        : undefined
  if (integer === undefined || integer < 0n || integer > U64_MAX) {
    throw new IntentError(`intent ${key} must be a u64`)
  }
  return integer
}

function jsonPolicy(value: unknown): IntentPolicy {
  if (value === "all" || value === "any") return { kind: value }
  if (typeof value === "object" && value !== null && !Array.isArray(value)) {
    const required: unknown = (value as Readonly<Record<string, unknown>>)["at_least"]
    if (typeof required === "number" && Number.isSafeInteger(required)) {
      return { kind: "at-least", required }
    }
    if (typeof required === "bigint" && required <= BigInt(Number.MAX_SAFE_INTEGER)) {
      return { kind: "at-least", required: Number(required) }
    }
  }
  throw new IntentError('intent policy must be "all", "any", or {"at_least": n}')
}

function jsonInteger(value: bigint): unknown {
  return value <= BigInt(Number.MAX_SAFE_INTEGER)
    ? Number(value)
    : EXACT_JSON.rawJSON(String(value))
}

function exactIntegers(
  _key: string,
  value: unknown,
  context?: { readonly source?: string }
): unknown {
  return typeof value === "number" &&
    context?.source !== undefined &&
    /^-?\d+$/.test(context.source) &&
    !Number.isSafeInteger(value)
    ? BigInt(context.source)
    : value
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

import { blake3 } from "@noble/hashes/blake3.js"
import { bytesToHex } from "@noble/hashes/utils.js"
import {
  PolicyBlockedError,
  PolicyDeferredError,
  StepUpRequiredError,
  type LaserError
} from "./client/errors.js"
import { Mutex } from "./runtime/mutex.js"
import { IntentId, type ConversationId } from "./types/ids.js"
import { decodeOne, encodeNamed, expectMap, field, type CborMap } from "./wire/cbor.js"

export const POLICY_DECISION_OPERATION = "policy_decision"

export const ActionKind = {
  Send: "send",
  Publish: "publish",
  Request: "request",
  Command: "command",
  Response: "response",
  Event: "event",
  Status: "status",
  Error: "error",
  MemoryWrite: "memory_write"
} as const
export type ActionKind = (typeof ActionKind)[keyof typeof ActionKind]

export const GovernorMode = { Observe: "observe", Enforce: "enforce" } as const
export type GovernorMode = (typeof GovernorMode)[keyof typeof GovernorMode]

export interface ActionCounters {
  readonly sends: bigint
  readonly requests: bigint
  readonly bytesSent: bigint
}

export interface GovernedAction {
  readonly kind: ActionKind
  readonly stream: string
  readonly topic: string
  readonly source?: string
  readonly target?: string
  readonly conversation?: ConversationId
  readonly correlation?: string
  readonly operation?: string
  readonly tool?: string
  readonly onBehalfOf?: string
  readonly purpose?: string
  readonly dataClassification?: string
  readonly payload: Uint8Array
  readonly signed: boolean
  readonly counters: ActionCounters
}

export type Verdict =
  | { readonly kind: "allow" }
  | { readonly kind: "observe" }
  | { readonly kind: "block" }
  | { readonly kind: "step_up"; readonly scope: string }
  | { readonly kind: "modify"; readonly body: Uint8Array }
  | { readonly kind: "defer" }

export interface PolicyRef {
  readonly packId: string
  readonly packVersion: string
  readonly ruleIds: readonly string[]
}

export class ActionDecision {
  private constructor(
    readonly verdict: Verdict,
    readonly reason?: string,
    readonly policy?: PolicyRef,
    readonly riskScore?: number
  ) {}

  static allow(): ActionDecision {
    return new ActionDecision({ kind: "allow" })
  }

  static observe(): ActionDecision {
    return new ActionDecision({ kind: "observe" })
  }

  static block(reason: string): ActionDecision {
    return new ActionDecision({ kind: "block" }, reason)
  }

  static stepUp(scope: string): ActionDecision {
    return new ActionDecision({ kind: "step_up", scope })
  }

  static modify(body: Uint8Array): ActionDecision {
    return new ActionDecision({ kind: "modify", body: body.slice() })
  }

  static defer(reason: string): ActionDecision {
    return new ActionDecision({ kind: "defer" }, reason)
  }

  withReason(reason: string): ActionDecision {
    return new ActionDecision(this.verdict, reason, this.policy, this.riskScore)
  }

  withPolicy(policy: PolicyRef): ActionDecision {
    return new ActionDecision(this.verdict, this.reason, policy, this.riskScore)
  }

  withRiskScore(riskScore: number): ActionDecision {
    return new ActionDecision(this.verdict, this.reason, this.policy, riskScore)
  }
}

export interface ActionGovernor {
  decide(action: GovernedAction): Promise<ActionDecision>
}

export interface PolicyEvidence {
  readonly decisionId: string
  readonly decision: Verdict["kind"]
  readonly mode: GovernorMode
  readonly kind: ActionKind
  readonly stream: string
  readonly topic: string
  readonly source?: string
  readonly target?: string
  readonly conversation?: string
  readonly correlation?: string
  readonly operation?: string
  readonly tool?: string
  readonly onBehalfOf?: string
  readonly reason?: string
  readonly approvedScope?: string
  readonly policy?: PolicyRef
  readonly riskScore?: number
  readonly receiptDigest: string
  readonly previousDigest?: string
  readonly outcome: "effected" | "blocked" | "step_up" | "deferred"
  readonly atMicros: bigint
}

interface Applied {
  readonly recorded: boolean
  readonly outcome: PolicyEvidence["outcome"]
  readonly body?: Uint8Array
  readonly denial?: LaserError
}

export class GovernorState {
  private sends = 0n
  private requests = 0n
  private bytesSent = 0n
  private readonly locks = new Map<string, Mutex>()
  private readonly digests = new Map<string, string>()

  constructor(
    private readonly governor: ActionGovernor,
    readonly mode: GovernorMode
  ) {}

  async govern(
    action: Omit<GovernedAction, "counters">,
    emit: (evidence: PolicyEvidence) => Promise<void>
  ): Promise<Uint8Array> {
    const complete: GovernedAction = { ...action, counters: this.counters() }
    const decision = await this.governor.decide(complete)
    if (action.kind === ActionKind.Request) this.requests += 1n
    else this.sends += 1n
    const applied = apply(this.mode, decision.verdict)
    if (applied.recorded) {
      const key = action.conversation?.toString() ?? ""
      const lock = this.locks.get(key) ?? new Mutex()
      this.locks.set(key, lock)
      const emission = lock.runExclusive(async () => {
        const evidence = sealEvidence(
          action,
          decision,
          this.mode,
          applied.outcome,
          this.digests.get(key)
        )
        await emit(evidence)
        this.digests.set(key, evidence.receiptDigest)
      })
      if (this.mode === GovernorMode.Observe) await emission.catch(() => undefined)
      else if (applied.denial === undefined) await emission
      else await emission.catch(() => undefined)
    }
    if (applied.denial !== undefined) throw applied.denial
    const payload = applied.body ?? action.payload
    this.bytesSent += BigInt(payload.byteLength)
    return payload.slice()
  }

  counters(): ActionCounters {
    return { sends: this.sends, requests: this.requests, bytesSent: this.bytesSent }
  }
}

export function encodePolicyEvidence(evidence: PolicyEvidence): Uint8Array {
  return encodeNamed(policyEvidenceMap(evidence), { forceFloatNumbers: true })
}

export function decodePolicyEvidence(payload: Uint8Array): PolicyEvidence {
  const context = "policy evidence"
  const map = expectMap(decodeOne(payload, context), context)
  const policyMap = map.get("policy")
  const riskScore = field.optionalF64(map, "risk_score", context)
  return {
    decisionId: field.requiredString(map, "decision_id", context),
    decision: field.requiredString(map, "decision", context) as Verdict["kind"],
    mode: field.requiredString(map, "mode", context) as GovernorMode,
    kind: field.requiredString(map, "kind", context) as ActionKind,
    stream: field.requiredString(map, "stream", context),
    topic: field.requiredString(map, "topic", context),
    ...optionalString(map, "source", context),
    ...optionalString(map, "target", context),
    ...optionalString(map, "conversation", context),
    ...optionalString(map, "correlation", context),
    ...optionalString(map, "operation", context),
    ...optionalString(map, "tool", context),
    ...optionalString(map, "on_behalf_of", context, "onBehalfOf"),
    ...optionalString(map, "reason", context),
    ...optionalString(map, "approved_scope", context, "approvedScope"),
    ...(policyMap instanceof Map ? { policy: decodePolicy(policyMap, context) } : {}),
    ...(riskScore !== undefined ? { riskScore } : {}),
    receiptDigest: field.requiredString(map, "receipt_digest", context),
    ...optionalString(map, "previous_digest", context, "previousDigest"),
    outcome: field.requiredString(map, "outcome", context) as PolicyEvidence["outcome"],
    atMicros: field.requiredU64(map, "at_micros", context)
  }
}

export function verifyEvidenceChain(evidence: readonly PolicyEvidence[]): boolean {
  let previous: string | undefined
  for (const item of evidence) {
    if (item.previousDigest !== previous) return false
    if (seal({ ...item, receiptDigest: "" }) !== item.receiptDigest) return false
    previous = item.receiptDigest
  }
  return true
}

export class QuorumGovernor implements ActionGovernor {
  private readonly voters: {
    readonly name: string
    readonly governor: ActionGovernor
    readonly mandatory: boolean
  }[] = []

  constructor(private readonly required: "all" | "any" | number) {}

  voter(name: string, governor: ActionGovernor, mandatory = false): this {
    this.voters.push({ name, governor, mandatory })
    return this
  }

  async decide(action: GovernedAction): Promise<ActionDecision> {
    if (this.voters.length === 0)
      return ActionDecision.block("quorum governor has no configured voters")
    if (
      typeof this.required === "number" &&
      (this.required < 1 || this.required > this.voters.length)
    ) {
      return ActionDecision.block(
        `quorum threshold ${String(this.required)} is invalid for ${String(this.voters.length)} voters`
      )
    }
    const decisions = await Promise.all(
      this.voters.map(async (voter) => ({ voter, decision: await voter.governor.decide(action) }))
    )
    const affirmative = decisions.filter(({ decision }) =>
      ["allow", "observe", "modify"].includes(decision.verdict.kind)
    )
    const required =
      this.required === "all" ? decisions.length : this.required === "any" ? 1 : this.required
    const mandatoryPassed = decisions.every(
      ({ voter, decision }) =>
        !voter.mandatory || ["allow", "observe", "modify"].includes(decision.verdict.kind)
    )
    const reason = decisions
      .map(({ voter, decision }) => `${voter.name}:${decision.verdict.kind}`)
      .join(",")
    if (mandatoryPassed && affirmative.length >= required) {
      const modify = affirmative.find(
        ({ decision }) => decision.verdict.kind === "modify"
      )?.decision
      return (
        modify ??
        (affirmative.some(({ decision }) => decision.verdict.kind === "observe")
          ? ActionDecision.observe()
          : ActionDecision.allow())
      ).withReason(reason)
    }
    const denied =
      decisions.find(({ decision }) => decision.verdict.kind === "block")?.decision ??
      decisions.find(({ decision }) => decision.verdict.kind === "step_up")?.decision ??
      ActionDecision.defer("quorum not met")
    return denied.withReason(reason)
  }
}

export class SwappableGovernor implements ActionGovernor {
  constructor(private current: ActionGovernor) {}

  swap(governor: ActionGovernor): void {
    this.current = governor
  }

  decide(action: GovernedAction): Promise<ActionDecision> {
    return this.current.decide(action)
  }
}

function apply(mode: GovernorMode, verdict: Verdict): Applied {
  const enforced = mode === GovernorMode.Enforce
  switch (verdict.kind) {
    case "allow":
      return { recorded: false, outcome: "effected" }
    case "observe":
      return { recorded: true, outcome: "effected" }
    case "modify":
      return { recorded: true, outcome: "effected", ...(enforced ? { body: verdict.body } : {}) }
    case "block":
      return {
        recorded: true,
        outcome: enforced ? "blocked" : "effected",
        ...(enforced ? { denial: new PolicyBlockedError("the governor blocked this action") } : {})
      }
    case "step_up":
      return {
        recorded: true,
        outcome: enforced ? "step_up" : "effected",
        ...(enforced ? { denial: new StepUpRequiredError(verdict.scope) } : {})
      }
    case "defer":
      return {
        recorded: true,
        outcome: enforced ? "deferred" : "effected",
        ...(enforced
          ? { denial: new PolicyDeferredError("the governor deferred this action") }
          : {})
      }
  }
}

function sealEvidence(
  action: Omit<GovernedAction, "counters">,
  decision: ActionDecision,
  mode: GovernorMode,
  outcome: PolicyEvidence["outcome"],
  previousDigest?: string
): PolicyEvidence {
  const evidence: PolicyEvidence = {
    decisionId: IntentId.new().toString(),
    decision: decision.verdict.kind,
    mode,
    kind: action.kind,
    stream: action.stream,
    topic: action.topic,
    ...(action.source !== undefined ? { source: action.source } : {}),
    ...(action.target !== undefined ? { target: action.target } : {}),
    ...(action.conversation !== undefined ? { conversation: action.conversation.toString() } : {}),
    ...(action.correlation !== undefined ? { correlation: action.correlation } : {}),
    ...(action.operation !== undefined ? { operation: action.operation } : {}),
    ...(action.tool !== undefined ? { tool: action.tool } : {}),
    ...(action.onBehalfOf !== undefined ? { onBehalfOf: action.onBehalfOf } : {}),
    ...(decision.reason !== undefined ? { reason: decision.reason } : {}),
    ...(decision.verdict.kind === "step_up" ? { approvedScope: decision.verdict.scope } : {}),
    ...(decision.policy !== undefined ? { policy: decision.policy } : {}),
    ...(decision.riskScore !== undefined ? { riskScore: decision.riskScore } : {}),
    receiptDigest: "",
    ...(previousDigest !== undefined ? { previousDigest } : {}),
    outcome,
    atMicros: BigInt(Date.now()) * 1000n
  }
  return { ...evidence, receiptDigest: seal(evidence) }
}

function seal(evidence: PolicyEvidence): string {
  return bytesToHex(
    blake3(
      encodeNamed(policyEvidenceMap({ ...evidence, receiptDigest: "" }), {
        forceFloatNumbers: true
      })
    )
  )
}

function policyEvidenceMap(evidence: PolicyEvidence): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["decision_id", evidence.decisionId],
    ["decision", evidence.decision],
    ["mode", evidence.mode],
    ["kind", evidence.kind],
    ["stream", evidence.stream],
    ["topic", evidence.topic]
  ])
  optionalMap(map, "source", evidence.source)
  optionalMap(map, "target", evidence.target)
  optionalMap(map, "conversation", evidence.conversation)
  optionalMap(map, "correlation", evidence.correlation)
  optionalMap(map, "operation", evidence.operation)
  optionalMap(map, "tool", evidence.tool)
  optionalMap(map, "on_behalf_of", evidence.onBehalfOf)
  optionalMap(map, "reason", evidence.reason)
  optionalMap(map, "approved_scope", evidence.approvedScope)
  if (evidence.policy !== undefined)
    map.set(
      "policy",
      new Map<string, unknown>([
        ["pack_id", evidence.policy.packId],
        ["pack_version", evidence.policy.packVersion],
        ["rule_ids", [...evidence.policy.ruleIds]]
      ])
    )
  optionalMap(map, "risk_score", evidence.riskScore)
  map.set("receipt_digest", evidence.receiptDigest)
  optionalMap(map, "previous_digest", evidence.previousDigest)
  map.set("outcome", evidence.outcome)
  map.set("at_micros", evidence.atMicros)
  return map
}

function optionalMap(map: Map<string, unknown>, key: string, value: unknown): void {
  if (value !== undefined) map.set(key, value)
}

function optionalString(
  map: CborMap,
  key: string,
  context: string,
  property = key
): Record<string, string> {
  const value = field.optionalString(map, key, context)
  return value === undefined ? {} : { [property]: value }
}

function decodePolicy(map: CborMap, context: string): PolicyRef {
  return {
    packId: field.requiredString(map, "pack_id", context),
    packVersion: field.requiredString(map, "pack_version", context),
    ruleIds: field.requiredArray(map, "rule_ids", context, (value) => String(value))
  }
}

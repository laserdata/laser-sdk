import {
  HandlerConfigError,
  HandlerError,
  InvalidError,
  type LaserError
} from "../client/errors.js"
import type { BytesLike } from "../client/bytes.js"
import type { Laser } from "../client/laser.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import type { Provenance } from "../provenance/provenance.js"
import type { AgentId } from "../types/ids.js"
import type { SigningKey } from "../signing.js"
import type { AgentMessage } from "./reliable-consumer.js"
import {
  ADVERTISED_INBOX_ROUTE,
  resolveInboxRoute,
  resolveTargets,
  type CapabilitySelector,
  type InboxRoute
} from "./router.js"

// When a `fan_out` gather is complete.
export type GatherPolicy =
  // Wait for every branch (each bounded by the deadline).
  | { readonly kind: "requireAll" }
  // Stop once this many branches succeed, dropping the wait on the rest.
  | { readonly kind: "quorum"; readonly needed: number }
  // Take whatever has landed by the deadline.
  | { readonly kind: "bestEffort" }

export const REQUIRE_ALL: GatherPolicy = { kind: "requireAll" }
export const BEST_EFFORT: GatherPolicy = { kind: "bestEffort" }

export function quorumOf(needed: number): GatherPolicy {
  if (!Number.isSafeInteger(needed) || needed < 0) {
    throw new InvalidError("quorum must be a non-negative safe integer")
  }
  return { kind: "quorum", needed }
}

// The outcome of a fan-out: the successful replies and the failed
// branches. Failures are surfaced here, never silently dropped, so a
// slow or dead agent is visible rather than mistaken for a missing
// result.
export interface Gather {
  // The agents that replied, each with its reply. Attributed, so a
  // caller can tell which agent produced which result rather than a bag
  // of bodies.
  readonly ok: readonly (readonly [AgentId, AgentMessage])[]
  // The agents whose branch failed (no inbox, request error, timeout),
  // each with its cause. Surfaced, never silently dropped.
  readonly failures: readonly (readonly [AgentId, LaserError])[]
}

export function emptyGather(): Gather {
  return { ok: [], failures: [] }
}

// The reply bodies alone, dropping agent attribution, for a caller that
// only wants the results.
export function gatherReplies(gather: Gather): readonly AgentMessage[] {
  return gather.ok.map(([, message]) => message)
}

// Whether `policy` is satisfied by `successes` replies so far, so the
// remaining branches can be cancelled. Only `quorum` short-circuits:
// `requireAll` and `bestEffort` drain every branch (the latter bounded by
// the wall-clock deadline instead).
export function quorumSatisfied(policy: GatherPolicy, successes: number): boolean {
  return policy.kind === "quorum" && successes >= policy.needed
}

export interface AgentContextOptions {
  readonly agent?: AgentId
  readonly respondOn?: string
  readonly inboxRoute?: InboxRoute
  readonly signingKey?: SigningKey
}

interface Branch {
  readonly controller: AbortController
  readonly result: Promise<BranchResult>
}

type BranchResult =
  | { readonly kind: "ok"; readonly agent: AgentId; readonly message: AgentMessage }
  | { readonly kind: "error"; readonly agent: AgentId; readonly error: LaserError }

function asLaserError(error: unknown): LaserError {
  if (error instanceof Error && "kind" in error) return error as LaserError
  return new HandlerError("fan-out branch failed", { cause: error })
}

function deadlineTimer(ms: number): { readonly promise: Promise<"deadline">; cancel(): void } {
  let timer: ReturnType<typeof setTimeout>
  const promise = new Promise<"deadline">((resolve) => {
    timer = setTimeout(() => {
      resolve("deadline")
    }, ms)
  })
  return {
    promise,
    cancel: () => {
      clearTimeout(timer)
    }
  }
}

async function gatherBranches(
  branches: readonly Branch[],
  seed: Gather,
  policy: GatherPolicy,
  deadlineMs: number
): Promise<Gather> {
  const ok = [...seed.ok]
  const failures = [...seed.failures]
  const pending = new Map(branches.map((branch, index) => [index, branch]))
  if (quorumSatisfied(policy, 0)) {
    for (const branch of pending.values()) branch.controller.abort()
    return { ok, failures }
  }
  const deadline = policy.kind === "bestEffort" ? deadlineTimer(deadlineMs) : undefined
  while (pending.size > 0) {
    const settled = [...pending].map(([index, branch]) =>
      branch.result.then((result) => ({ kind: "branch" as const, index, result }))
    )
    const next =
      deadline !== undefined
        ? await Promise.race([...settled, deadline.promise])
        : await Promise.race(settled)
    if (next === "deadline") {
      for (const branch of pending.values()) branch.controller.abort()
      break
    }
    pending.delete(next.index)
    if (next.result.kind === "ok") {
      ok.push([next.result.agent, next.result.message])
    } else {
      failures.push([next.result.agent, next.result.error])
    }
    if (quorumSatisfied(policy, ok.length)) {
      for (const branch of pending.values()) branch.controller.abort()
      break
    }
  }
  deadline?.cancel()
  return { ok, failures }
}

export class AgentContext {
  readonly agent: AgentId | undefined
  readonly respondOn: string | undefined
  readonly inboxRoute: InboxRoute
  private readonly signingKey: SigningKey | undefined

  constructor(
    readonly laser: Laser,
    readonly message: AgentMessage,
    options: AgentContextOptions = {}
  ) {
    this.agent = options.agent
    this.respondOn = options.respondOn
    this.inboxRoute = options.inboxRoute ?? ADVERTISED_INBOX_ROUTE
    this.signingKey = options.signingKey
  }

  async respond(payload: BytesLike): Promise<void> {
    const topic = this.respondOn
    if (topic === undefined) {
      throw new HandlerConfigError("respond() requires the agent to configure respondOn")
    }
    const sender = this.message.provenance.agent
    const envelope = this.message.envelope
    if (
      this.signingKey !== undefined &&
      envelope?.correlation !== undefined &&
      this.agent !== undefined
    ) {
      let response = this.laser
        .agdx(topic, this.agent, this.message.provenance.conversationId)
        .respond(envelope.correlation, payload)
        .signedBy(this.signingKey)
      if (sender !== undefined) response = response.withTarget(sender)
      await response.send()
      return
    }
    const provenance = {
      ...this.replyProvenance(),
      ...(sender !== undefined ? { targetAgentId: sender } : {})
    }
    await this.laser.sendAgent(topic, payload, provenance)
  }

  async replyOn(topic: string, payload: BytesLike): Promise<void> {
    await this.laser.sendAgent(topic, payload, this.replyProvenance())
  }

  async send(topic: string, payload: BytesLike, provenance: Provenance): Promise<void> {
    await this.laser.sendAgent(topic, payload, provenance)
  }

  request(
    requestTopic: string,
    replyTopic: string,
    payload: BytesLike,
    provenance: Provenance,
    timeoutMs: number,
    signal?: AbortSignal
  ): Promise<AgentMessage> {
    return this.laser.request(requestTopic, replyTopic, payload, provenance, timeoutMs, signal)
  }

  async respondInput(replyTopic: string, response: BytesLike): Promise<void> {
    const envelope = this.message.envelope
    if (envelope === undefined) {
      throw new HandlerConfigError("respondInput(): the handled message is not an AGDX envelope")
    }
    if (envelope.correlation === undefined) {
      throw new HandlerConfigError("respondInput(): the interrupt carries no correlation")
    }
    if (this.agent === undefined) {
      throw new HandlerConfigError("respondInput(): the agent has no id")
    }
    await this.laser
      .agdx(replyTopic, this.agent, this.message.provenance.conversationId)
      .respond(envelope.correlation, response)
      .send()
  }

  approvalGate(
    replyTopic: string,
    prompt: BytesLike,
    timeoutMs: number,
    options?: { readonly signal?: AbortSignal }
  ): Promise<Uint8Array> {
    if (this.agent === undefined) {
      throw new HandlerConfigError("approvalGate(): the agent has no id")
    }
    return this.laser
      .agdx(AgentTopic.HumanInput, this.agent, this.message.provenance.conversationId)
      .requestInput(replyTopic, prompt, timeoutMs, options)
  }

  spawnSubconversation(): Provenance {
    return this.laser.spawnSubconversation(this.message.provenance)
  }

  async fanOut(
    selector: CapabilitySelector,
    payload: BytesLike,
    policy: GatherPolicy,
    deadlineMs: number
  ): Promise<Gather> {
    if (!Number.isFinite(deadlineMs) || deadlineMs < 0) {
      throw new InvalidError("fanOut() deadline must be a non-negative finite number")
    }
    const replyTopic = this.respondOn
    if (replyTopic === undefined) {
      throw new HandlerConfigError("fanOut() requires the agent to configure respondOn")
    }
    const registry = await this.laser.agentRegistry()
    const nowMicros = BigInt(Date.now()) * 1000n
    await registry.refresh(nowMicros)
    if (this.inboxRoute.kind === "advertised" || selector.principal !== undefined) {
      await registry.refreshPresence()
    }
    const targets = resolveTargets({ kind: "allCapable", selector }, registry, nowMicros)
    const failures: (readonly [AgentId, LaserError])[] = []
    const branches: Branch[] = []
    for (const target of targets) {
      let inbox: string
      try {
        inbox = resolveInboxRoute(this.inboxRoute, target, registry.inboxFor(target))
      } catch (error) {
        failures.push([target, asLaserError(error)])
        continue
      }
      const controller = new AbortController()
      const provenance = {
        ...this.laser.spawnSubconversation(this.message.provenance),
        targetAgentId: target
      }
      const result = this.laser
        .request(inbox, replyTopic, payload, provenance, deadlineMs, controller.signal)
        .then((message): BranchResult => ({ kind: "ok", agent: target, message }))
        .catch((error: unknown): BranchResult => ({
          kind: "error",
          agent: target,
          error: asLaserError(error)
        }))
      branches.push({ controller, result })
    }
    return gatherBranches(branches, { ok: [], failures }, policy, deadlineMs)
  }

  private replyProvenance(): Provenance {
    const provenance = this.message.provenance
    return {
      conversationId: provenance.conversationId,
      causalParent: this.message.id,
      ...(this.agent !== undefined ? { agent: this.agent } : {}),
      ...(provenance.rootConversationId !== undefined
        ? { rootConversationId: provenance.rootConversationId }
        : {}),
      ...(provenance.correlationId !== undefined ? { correlationId: provenance.correlationId } : {})
    }
  }
}

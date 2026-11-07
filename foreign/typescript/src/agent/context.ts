import { createHash } from "node:crypto"
import {
  HandlerConfigError,
  HandlerError,
  InvalidError,
  type LaserError,
  NoRespondTopicError,
  PolicyDeferredError,
  ProtocolError,
  RejectedError,
  TimeoutError
} from "../client/errors.js"
import { INTERNAL_VERIFIER } from "../client/internals.js"
import type { BytesLike } from "../client/bytes.js"
import { type CallOptions, abortable, cancellable, throwIfAborted } from "../client/call-options.js"
import type { Laser } from "../client/laser.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import type { Provenance } from "../provenance/provenance.js"
import { AgentId, ConversationId, messageIdToString } from "../types/ids.js"
import {
  AgentKind,
  OPERATION_TASK,
  TaskStateName,
  decodeAgentErrorBody,
  taskStateFromCode,
  type AgentErrorBody,
  type AgentId as WireAgentId
} from "../wire/agent.js"
import { decodeOne, expectMap } from "../wire/cbor.js"
import { OPERATION_INPUT_REQUEST } from "../wire/dispatch.js"
import { CorrelationId, logPositionToBytes, type LogPosition } from "../wire/ids.js"
import type { Session, Sessions } from "../session.js"
import type { ControlBook } from "./control.js"
import type { SigningKey } from "../signing.js"
import type { AgentMessage } from "./reliable-consumer.js"
import { longTimeout } from "./timer.js"
import {
  ADVERTISED_INBOX_ROUTE,
  resolveInboxRoute,
  resolveTargets,
  type CapabilitySelector,
  type InboxRoute
} from "./router.js"

/** An approver's decision from `AgentCtx.approvalGate`, minted only by an
 * approving reply. A governed effect carries it with `AgdxSend.withGrant`, so
 * the governor's step-up in the same conversation is answered by this
 * approval. */
export class ApprovalGrant {
  private constructor(
    readonly approver: WireAgentId,
    readonly conversation: ConversationId,
    readonly correlation: CorrelationId,
    readonly decision: Uint8Array
  ) {}

  /** @internal */
  static mint(
    approver: WireAgentId,
    conversation: ConversationId,
    correlation: CorrelationId,
    decision: Uint8Array
  ): ApprovalGrant {
    return new ApprovalGrant(approver, conversation, correlation, decision)
  }
}

/** The approval state a reliable consumer shares with the handler of one
 * record: the consumer topic a decision must arrive on for the runtime to
 * park, the interrupt the gate raised, and the decision a replay resumes the
 * gate with.
 * @internal */
export class ApprovalSlot {
  interrupt: ApprovalInterrupt | undefined
  constructor(
    readonly parkingTopic: string,
    public resume: ApprovalResume | undefined
  ) {}
}

/** An approval gate's interrupt: on the pass that published the input
 * request, the prompt's position and deadline.
 * @internal */
export interface ApprovalInterrupt {
  readonly prompt?: { readonly at: LogPosition; readonly deadlineMicros: bigint }
}

/** What a replayed approval gate resumes with.
 * @internal */
export type ApprovalResume =
  { readonly kind: "decided"; readonly reply: AgentMessage } | { readonly kind: "expired" }

// The high 16 bits that mark an approval correlation, so a runtime tells an
// approval decision from any other reply without a lookup. A minted
// correlation carries a timestamp there and never takes this value.
const APPROVAL_CORRELATION_TAG = 0xa6a6n

/** The input request correlation of the approval gated on the record at
 * `at`, stable across the record's replays.
 * @internal */
export function approvalCorrelation(at: LogPosition): CorrelationId {
  const digest = createHash("sha256")
    .update("laser.approval")
    .update(logPositionToBytes(at))
    .digest()
  let hash = 0n
  for (const byte of digest.subarray(0, 16)) hash = (hash << 8n) | BigInt(byte)
  const mask = (1n << 112n) - 1n
  return CorrelationId.fromU128((hash & mask) | (APPROVAL_CORRELATION_TAG << 112n))
}

/** Whether `correlation` names an approval gate's input request.
 * @internal */
export function isApprovalCorrelation(correlation: CorrelationId): boolean {
  return correlation.asU128() >> 112n === APPROVAL_CORRELATION_TAG
}

/** Determines when a fan-out or scatter gather completes. */
export type GatherPolicy =
  /** Wait for every branch, bounded by the deadline. */
  | { readonly kind: "requireAll" }
  /** Stop after the requested number of branches succeed, at least one. */
  | { readonly kind: "quorum"; readonly needed: number }
  /** Collect the branches completed before the deadline. */
  | { readonly kind: "bestEffort" }

export const REQUIRE_ALL: GatherPolicy = { kind: "requireAll" }
export const BEST_EFFORT: GatherPolicy = { kind: "bestEffort" }

export function quorumOf(needed: number): GatherPolicy {
  const policy: GatherPolicy = { kind: "quorum", needed }
  validateGatherPolicy(policy)
  return policy
}

/** Refuses a quorum below one, which would be met before any branch ran.
 * @internal */
export function validateGatherPolicy(policy: GatherPolicy): void {
  if (policy.kind === "quorum" && (!Number.isSafeInteger(policy.needed) || policy.needed < 1)) {
    throw new InvalidError("a gather quorum must be at least one")
  }
}

/** Whether `successes` of `branches` meet the policy: every branch under
 * `requireAll`, the quorum under `quorum`, at least one under `bestEffort`.
 * @internal */
export function gatherSatisfied(
  policy: GatherPolicy,
  successes: number,
  branches: number
): boolean {
  switch (policy.kind) {
    case "requireAll":
      return successes === branches
    case "quorum":
      return successes >= policy.needed
    case "bestEffort":
      return successes > 0
  }
}

/** Attributed replies, failures, and cancelled branches from a fan-out
 * gather. */
export interface Gather {
  /** Successful replies paired with their agents. */
  readonly ok: readonly (readonly [AgentId, AgentMessage])[]
  /** Failed branches paired with their agents. */
  readonly failures: readonly (readonly [AgentId, LaserError])[]
  /** The agents whose branch was still running when the policy stopped the
   * gather (a met quorum, or the best-effort deadline). */
  readonly cancelled: readonly AgentId[]
  /** Whether the gather met its policy. */
  readonly satisfied: boolean
}

/** Returns successful replies without agent attribution. */
export function gatherReplies(gather: Gather): readonly AgentMessage[] {
  return gather.ok.map(([, message]) => message)
}

export interface AgentCtxOptions {
  readonly agent?: AgentId
  readonly respondOn?: string
  readonly inboxRoute?: InboxRoute
  readonly signingKey?: SigningKey
  /** Where the handled record sits on the log, stamped on typed replies as
   * their causal position. */
  readonly requestAt?: LogPosition
  /** The runtime's session factory, so `session` follows its configuration. */
  readonly sessions?: Sessions
  /** The runtime's control state, so `session` sees its control requests.
   * @internal */
  readonly control?: ControlBook
  /** Time source for deterministic presence checks. */
  readonly nowMicros?: () => bigint
  /** The approval slot the runtime parks an interrupted record through.
   * @internal */
  readonly approval?: ApprovalSlot
}

/** One gather branch, cancelled through its controller.
 * @internal */
export interface Branch<T> {
  readonly agent: AgentId
  readonly controller: AbortController
  readonly result: Promise<BranchResult<T>>
}

/** @internal */
export type BranchResult<T> =
  | { readonly kind: "ok"; readonly value: T }
  | { readonly kind: "error"; readonly error: LaserError }

/** What a gather collected: every finished branch, the cancelled ones, and
 * how many finished branches counted as a success.
 * @internal */
export interface Gathered<T> {
  readonly outcomes: readonly (readonly [AgentId, BranchResult<T>])[]
  readonly cancelled: readonly AgentId[]
  readonly successes: number
}

/** @internal */
export function asLaserError(error: unknown): LaserError {
  if (error instanceof Error && "kind" in error) return error as LaserError
  return new HandlerError("gather branch failed", { cause: error })
}

function deadlineTimer(ms: number): { readonly promise: Promise<"deadline">; cancel(): void } {
  let timer: { cancel(): void } | undefined
  const promise = new Promise<"deadline">((resolve) => {
    timer = longTimeout(() => {
      resolve("deadline")
    }, ms)
  })
  return {
    promise,
    cancel: () => {
      timer?.cancel()
    }
  }
}

/** Drain the branches under `policy`. `requireAll` waits for every branch,
 * each bounded by its own deadline. `quorum` cancels the remaining branches
 * once enough have succeeded. `bestEffort` takes whatever landed when the
 * deadline elapses and cancels the stragglers.
 * @internal */
export async function gatherBranches<T>(
  branches: readonly Branch<T>[],
  policy: GatherPolicy,
  deadlineMs: number,
  succeeded: (value: T) => boolean
): Promise<Gathered<T>> {
  const outcomes: (readonly [AgentId, BranchResult<T>])[] = []
  let successes = 0
  const pending = new Map(branches.map((branch, index) => [index, branch]))
  const deadline = policy.kind === "bestEffort" ? deadlineTimer(deadlineMs) : undefined
  while (pending.size > 0) {
    const settled = [...pending].map(([index, branch]) =>
      branch.result.then((result) => ({ kind: "branch" as const, index, result }))
    )
    const next =
      deadline !== undefined
        ? await Promise.race([...settled, deadline.promise])
        : await Promise.race(settled)
    if (next === "deadline") break
    const branch = pending.get(next.index)
    pending.delete(next.index)
    if (branch === undefined) continue
    if (next.result.kind === "ok" && succeeded(next.result.value)) successes += 1
    outcomes.push([branch.agent, next.result])
    if (policy.kind === "quorum" && successes >= policy.needed) break
  }
  deadline?.cancel()
  for (const branch of pending.values()) branch.controller.abort()
  const cancelled = [...pending.values()]
    .map((branch) => branch.agent)
    .sort((left, right) =>
      left.asStr() < right.asStr() ? -1 : left.asStr() > right.asStr() ? 1 : 0
    )
  return { outcomes, cancelled, successes }
}

export class AgentCtx {
  /** @internal */
  readonly agent: AgentId | undefined
  /** @internal */
  readonly respondOn: string | undefined
  /** @internal */
  readonly inboxRoute: InboxRoute
  private readonly signingKey: SigningKey | undefined
  private readonly requestPosition: LogPosition | undefined
  private readonly sessions: Sessions | undefined
  private readonly control: ControlBook | undefined
  private readonly nowMicros: () => bigint
  private readonly approval: ApprovalSlot | undefined

  private constructor(
    readonly laser: Laser,
    readonly message: AgentMessage,
    options: AgentCtxOptions = {}
  ) {
    this.agent = options.agent
    this.respondOn = options.respondOn
    this.inboxRoute = options.inboxRoute ?? ADVERTISED_INBOX_ROUTE
    this.signingKey = options.signingKey
    this.requestPosition = options.requestAt
    this.sessions = options.sessions
    this.control = options.control
    this.nowMicros = options.nowMicros ?? (() => BigInt(Date.now()) * 1000n)
    this.approval = options.approval
  }

  /** @internal */
  static create(laser: Laser, message: AgentMessage, options: AgentCtxOptions = {}): AgentCtx {
    return new AgentCtx(laser, message, options)
  }

  /** Where the handled record sits on the log, when the runtime knows it. */
  requestAt(): LogPosition | undefined {
    return this.requestPosition
  }

  /** The handled record's session, written as this agent and acting on the
   * handled record: graph writes take it as source and remembered items as
   * origin. A lens only: it holds no lease and starts no heartbeat. */
  session(): Session {
    const conversation = this.message.provenance.conversationId
    let session = (this.sessions ?? this.laser.sessions()).open(conversation)
    if (this.control !== undefined) session = session.withControl(this.control)
    if (this.agent !== undefined) session = session.asAgent(this.agent)
    const at = this.requestPosition
    if (at !== undefined) {
      session = session.actingOn({
        kind: "message",
        stream: at.streamId,
        topic: at.topicId,
        partition: at.partitionId,
        offset: at.offset,
        conversation: this.message.provenance.conversationId.toString()
      })
    }
    return session
  }

  /** Replies on the agent's `respondOn` topic. A typed request gets a typed
   * response: correlated, addressed to the requester, and caused by the
   * request's log position. A plain request gets a plain reply, matched by its
   * string correlation and routed back to its sender. */
  async respond(payload: BytesLike, options: CallOptions = {}): Promise<void> {
    throwIfAborted(options.signal, "respond")
    const topic = this.respondOn
    if (topic === undefined) {
      throw new NoRespondTopicError("respond() requires the agent to configure respondOn")
    }
    const envelope = this.message.envelope
    if (envelope?.correlation !== undefined) {
      if (this.agent === undefined) {
        throw new HandlerConfigError("a responding agent must have an id")
      }
      let response = this.laser
        .agdx(topic, this.agent, ConversationId.parse(envelope.conversation.toString()))
        .respond(envelope.correlation, payload)
        .withTarget(AgentId.new(envelope.source))
        .withAncestry(envelope.parent, envelope.root)
      if (envelope.record !== undefined)
        response = response.withCause(envelope.record, this.requestPosition)
      if (this.signingKey !== undefined) response = response.signedBy(this.signingKey)
      await response.send(options)
      return
    }
    await abortable(
      this.laser.sendAgent(topic, payload, this.routedReplyProvenance(), options),
      options.signal,
      "respond"
    )
  }

  /** Replies on `topic` instead of `respondOn`, chained off this message and
   * routed back to its sender. */
  async replyOn(topic: string, payload: BytesLike, options: CallOptions = {}): Promise<void> {
    await cancellable(options.signal, "reply", () =>
      this.laser.sendAgent(topic, payload, this.routedReplyProvenance(), options)
    )
  }

  async send(
    topic: string,
    payload: BytesLike,
    provenance: Provenance,
    options: CallOptions = {}
  ): Promise<void> {
    await cancellable(options.signal, "agent send", () =>
      this.laser.sendAgent(topic, payload, provenance, options)
    )
  }

  request(
    requestTopic: string,
    replyTopic: string,
    payload: BytesLike,
    provenance: Provenance,
    timeoutMs: number,
    options: CallOptions = {}
  ): Promise<AgentMessage> {
    return this.laser.request(requestTopic, replyTopic, payload, provenance, timeoutMs, options)
  }

  async respondInput(
    replyTopic: string,
    response: BytesLike,
    options: CallOptions = {}
  ): Promise<void> {
    throwIfAborted(options.signal, "input response")
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
    let reply = this.laser
      .agdx(replyTopic, this.agent, ConversationId.parse(envelope.conversation.toString()))
      .respond(envelope.correlation, response)
      .withTarget(AgentId.new(envelope.source))
      .withAncestry(envelope.parent, envelope.root)
    if (envelope.record !== undefined)
      reply = reply.withCause(envelope.record, this.requestPosition)
    if (this.signingKey !== undefined) reply = reply.signedBy(this.signingKey)
    await reply.send(options)
  }

  /** Pause this handler on a decision by `approver`: publish `prompt` as an
   * input request addressed to it in the handled conversation and await its
   * correlated reply on `replyTopic`. Only a reply authored by `approver`, and
   * signed by it when a verifier is configured, decides. Resolves to the
   * `ApprovalGrant` on approval and rejects with `RejectedError` on a
   * rejection. A governed effect re-sent with the grant does not step up
   * again. */
  async approvalGate(
    approver: AgentId,
    replyTopic: string,
    prompt: BytesLike,
    timeoutMs: number,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<ApprovalGrant> {
    throwIfAborted(options.signal, "approval gate")
    if (this.agent === undefined) {
      throw new HandlerConfigError("approvalGate(): the agent has no id")
    }
    const conversation = this.message.provenance.conversationId
    const at = this.requestPosition
    if (at !== undefined && this.approval?.parkingTopic === replyTopic) {
      return this.parkedGate(this.approval, this.agent, approver, at, prompt, timeoutMs, options)
    }
    const reply = await this.laser
      .agdx(AgentTopic.Sessions, this.agent, conversation)
      .inputReply(replyTopic, prompt, timeoutMs, { ...options, target: approver })
    if (reply.correlation === undefined) {
      throw new ProtocolError("an approval reply carries no correlation")
    }
    return ApprovalGrant.mint(reply.source, conversation, reply.correlation, reply.body)
  }

  // The parking gate: decide on the decision a replay carries, else publish
  // the prompt once and interrupt the handler.
  private async parkedGate(
    approval: ApprovalSlot,
    agent: AgentId,
    approver: AgentId,
    at: LogPosition,
    prompt: BytesLike,
    timeoutMs: number,
    options: CallOptions
  ): Promise<ApprovalGrant> {
    const conversation = this.message.provenance.conversationId
    const correlation = approvalCorrelation(at)
    const resume = approval.resume
    approval.resume = undefined
    if (resume?.kind === "expired") throw new TimeoutError("the approval")
    if (resume?.kind === "decided") {
      const decision = this.decisionOf(resume.reply, approver, correlation)
      if (decision !== undefined) {
        return ApprovalGrant.mint(approver.wireId(), conversation, correlation, decision)
      }
      // A reply from anyone but the approver does not decide, and the prompt
      // is already out.
      approval.interrupt = {}
    } else {
      const producer = this.laser.agdx(AgentTopic.Sessions, agent, conversation)
      await producer
        .status(OPERATION_TASK)
        .withCorrelation(correlation)
        .withTaskState(taskStateFromCode(TaskStateName.InputRequired))
        .send(options)
      const deadlineMicros = this.nowMicros() + BigInt(Math.round(timeoutMs * 1_000))
      const receipt = await producer
        .command(correlation, prompt)
        .withOperation(OPERATION_INPUT_REQUEST)
        .withTarget(approver)
        .withDeadlineAtMicros(deadlineMicros)
        .sendReceipt(options)
      approval.interrupt = {
        prompt: {
          at: {
            streamId: at.streamId,
            topicId: at.topicId,
            partitionId: receipt.partitionId ?? at.partitionId,
            offset: receipt.offset ?? 0n
          },
          deadlineMicros
        }
      }
    }
    throw new PolicyDeferredError(`awaiting the approval of ${approver.asStr()}`)
  }

  // The decision `reply` carries for the approval `correlation`: the body on
  // a response, else throws the rejection. Undefined when the reply is not
  // the approver's.
  private decisionOf(
    reply: AgentMessage,
    approver: AgentId,
    correlation: CorrelationId
  ): Uint8Array | undefined {
    const envelope = reply.envelope
    if (
      envelope?.correlation?.equals(correlation) !== true ||
      envelope.source !== approver.wireId() ||
      (this.laser[INTERNAL_VERIFIER]() !== undefined &&
        reply.verifiedPrincipal !== approver.asStr())
    ) {
      return undefined
    }
    if (envelope.kind === AgentKind.Response) return envelope.body
    if (envelope.kind !== AgentKind.Error) return undefined
    let body: AgentErrorBody | undefined
    try {
      const context = "AGDX error body"
      body = decodeAgentErrorBody(expectMap(decodeOne(envelope.body, context), context), context)
    } catch {
      body = undefined
    }
    if (body?.code.kind === "known" && body.code.name === "DeadlineExceeded") {
      throw new TimeoutError("the approval")
    }
    throw new RejectedError(body?.message ?? "the input request was rejected")
  }

  spawnSubconversation(): Provenance {
    if (this.agent === undefined) {
      throw new HandlerConfigError("spawnSubconversation(): the agent has no id")
    }
    return this.laser.spawnSubconversation(this.message.provenance, this.agent)
  }

  /** Fan out to every agent advertising the selector's skill and gather the
   * replies under `policy` within `deadlineMs`. Every branch carries the
   * deadline, so a target that picks it up late drops it. Branches still
   * running when the policy stops the gather are `cancelled`. Refuses a
   * quorum below one. */
  async fanOut(
    selector: CapabilitySelector,
    payload: BytesLike,
    policy: GatherPolicy,
    deadlineMs: number,
    options: CallOptions = {}
  ): Promise<Gather> {
    if (!Number.isFinite(deadlineMs) || deadlineMs < 0) {
      throw new InvalidError("fanOut() deadline must be a non-negative finite number")
    }
    validateGatherPolicy(policy)
    const replyTopic = this.respondOn
    if (replyTopic === undefined) {
      throw new NoRespondTopicError("fanOut() requires the agent to configure respondOn")
    }
    throwIfAborted(options.signal, "fan-out")
    const registry = await abortable(this.laser.agentRegistry(), options.signal, "fan-out")
    const nowMicros = this.nowMicros()
    await registry.refresh(nowMicros, options)
    if (this.inboxRoute.kind === "advertised" || selector.principal !== undefined) {
      await registry.refreshPresence(options)
    }
    const targets = resolveTargets({ kind: "allCapable", selector }, registry, nowMicros)
    if (this.agent === undefined) {
      throw new HandlerConfigError("fanOut(): the agent has no id")
    }
    const deadlineMicros = BigInt(Date.now()) * 1000n + BigInt(Math.ceil(deadlineMs * 1000))
    const failures: (readonly [AgentId, LaserError])[] = []
    const branches: Branch<AgentMessage>[] = []
    for (const target of targets) {
      let inbox: string
      try {
        inbox = resolveInboxRoute(this.inboxRoute, target, registry.inboxFor(target))
      } catch (error) {
        failures.push([target, asLaserError(error)])
        continue
      }
      const controller = new AbortController()
      const signal =
        options.signal === undefined
          ? controller.signal
          : AbortSignal.any([controller.signal, options.signal])
      const provenance = {
        ...this.laser.spawnSubconversation(this.message.provenance, this.agent),
        targetAgentId: target,
        deadlineAtMicros: deadlineMicros
      }
      const result = this.laser
        .request(inbox, replyTopic, payload, provenance, deadlineMs, { signal })
        .then((value): BranchResult<AgentMessage> => ({ kind: "ok", value }))
        .catch((error: unknown): BranchResult<AgentMessage> => ({
          kind: "error",
          error: asLaserError(error)
        }))
      branches.push({ agent: target, controller, result })
    }
    const branchCount = branches.length + failures.length
    const gathered = await abortable(
      gatherBranches(branches, policy, deadlineMs, () => true),
      options.signal,
      "fan-out"
    )
    const ok: (readonly [AgentId, AgentMessage])[] = []
    for (const [agent, result] of gathered.outcomes) {
      if (result.kind === "ok") ok.push([agent, result.value])
      else failures.push([agent, result.error])
    }
    return {
      ok,
      failures,
      cancelled: gathered.cancelled,
      satisfied: gatherSatisfied(policy, gathered.successes, branchCount)
    }
  }

  private routedReplyProvenance(): Provenance {
    const sender = this.message.provenance.agent
    return {
      ...this.replyProvenance(),
      ...(sender !== undefined ? { targetAgentId: sender } : {})
    }
  }

  // The request's correlation is echoed so a shared reply topic cannot hand
  // one caller another's reply. A request without a correlation gets its own
  // position as one, so the reply names both its cause and a correlation and
  // every reader classifies it as a reply, never as new work.
  private replyProvenance(): Provenance {
    const provenance = this.message.provenance
    return {
      conversationId: provenance.conversationId,
      causalParent: this.message.id,
      ...(this.agent !== undefined ? { agent: this.agent } : {}),
      ...(provenance.parentConversationId !== undefined
        ? { parentConversationId: provenance.parentConversationId }
        : {}),
      ...(provenance.rootConversationId !== undefined
        ? { rootConversationId: provenance.rootConversationId }
        : {}),
      correlationId: provenance.correlationId ?? messageIdToString(this.message.id)
    }
  }
}

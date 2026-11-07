import { millisToMicros } from "../client/duration.js"
import { ownedBytes, type BytesLike } from "../client/bytes.js"
import { type CallOptions, abortable, cancellable, throwIfAborted } from "../client/call-options.js"
import { InvalidError, TimeoutError, type LaserError } from "../client/errors.js"
import { INTERNAL_REPLY_HUB, INTERNAL_VERIFIER } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import type { AgdxSend } from "./agdx.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { ConversationId, type AgentId } from "../types/ids.js"
import {
  type IdempotencyKey,
  AgentKind,
  type AgentEnvelope,
  type AgentErrorBody,
  METADATA_SUBMITTED,
  OPERATION_SESSION,
  TaskStateName,
  deadLetterReasonCode,
  decodeAgentErrorBody,
  encodeSessionStart,
  taskStateFromCode
} from "../wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../wire/cbor.js"
import { ContentType } from "../wire/content.js"
import type { Session } from "../session.js"
import { FENCE } from "../wire/headers.js"
import { ConversationId as WireConversationId, CorrelationId } from "../wire/ids.js"
import {
  DEAD_LETTER_REASON_DETAIL,
  agentMessageBody,
  type AgentMessage
} from "./reliable-consumer.js"
import {
  type Branch,
  type BranchResult,
  type GatherPolicy,
  asLaserError,
  gatherBranches,
  gatherSatisfied,
  validateGatherPolicy
} from "./context.js"
import type { ExpectedReply, ReplyStreamTicket } from "./replies.js"
import {
  ADVERTISED_INBOX_ROUTE,
  requiredPrincipal,
  resolveInboxRoute,
  resolveRoutable,
  resolveTargets,
  routeRequiresPresence,
  routeTo,
  type CapabilitySelector,
  type InboxRoute,
  type Router
} from "./router.js"

const DEFAULT_DEADLINE_MS = 30_000

export type Contract =
  | { readonly kind: "completed"; readonly reply: AgentMessage }
  | { readonly kind: "failed"; readonly reply: AgentMessage }
  | { readonly kind: "notConsumed" }
  | { readonly kind: "timedOut" }

export interface ScatterOutcome {
  readonly agent: AgentId
  readonly result:
    | { readonly kind: "ok"; readonly contract: Contract }
    | { readonly kind: "err"; readonly error: LaserError }
}

/** Every finished scatter branch, the branches the gather policy cancelled,
 * and whether the policy was met. */
export class ScatterReport {
  constructor(
    readonly outcomes: readonly ScatterOutcome[],
    readonly cancelled: readonly AgentId[] = [],
    readonly satisfied: boolean = outcomes.length > 0
  ) {}

  completed(): readonly { readonly agent: AgentId; readonly reply: AgentMessage }[] {
    return this.outcomes.flatMap((outcome) =>
      outcome.result.kind === "ok" && outcome.result.contract.kind === "completed"
        ? [{ agent: outcome.agent, reply: outcome.result.contract.reply }]
        : []
    )
  }

  failures(): readonly { readonly agent: AgentId; readonly error: LaserError }[] {
    return this.outcomes.flatMap((outcome) =>
      outcome.result.kind === "err" ? [{ agent: outcome.agent, error: outcome.result.error }] : []
    )
  }
}

interface ResolvedContract {
  readonly target: AgentId
  readonly inbox: string
  readonly expectedSigner: string
}

function duration(name: string, value: number): number {
  if (!Number.isFinite(value) || value < 0) {
    throw new InvalidError(`${name} must be a non-negative finite number`)
  }
  return value
}

function durationMicros(name: string, value: number): bigint {
  duration(name, value)
  const micros = millisToMicros(value)
  if (micros > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new InvalidError(`${name} must fit in a safe whole number of microseconds`)
  }
  return micros
}

async function resolveContract(
  laser: Laser,
  router: Router,
  inboxRoute: InboxRoute,
  nowMicros: bigint,
  excluded: readonly AgentId[] = [],
  options: CallOptions = {}
): Promise<ResolvedContract> {
  if (router.kind === "broadcast" || router.kind === "allCapable") {
    throw new InvalidError(
      "a contract is directed to one agent, not a broadcast or all-capable route"
    )
  }
  const registry = await abortable(laser.agentRegistry(), options.signal, "contract")
  await registry.refresh(nowMicros, options)
  if (inboxRoute.kind === "advertised" || routeRequiresPresence(router)) {
    await registry.refreshPresence(options)
  }
  const target = resolveRoutable(router, registry, inboxRoute, nowMicros, excluded)[0]
  if (target === undefined) throw new InvalidError("the contract route resolved no target")
  const inbox = resolveInboxRoute(inboxRoute, target, registry.inboxFor(target))
  return {
    target,
    inbox,
    expectedSigner: requiredPrincipal(router)?.toString() ?? target.asStr()
  }
}

function acceptsReply(
  laser: Laser,
  message: AgentMessage,
  expectedSigner: string
): AgentMessage | undefined {
  const verifier = laser[INTERNAL_VERIFIER]()
  if (verifier === undefined) return message
  return message.verifiedPrincipal === expectedSigner ? message : undefined
}

function isWorking(envelope: AgentEnvelope | undefined): boolean {
  return (
    envelope?.kind === AgentKind.Status &&
    envelope.taskState?.kind === "known" &&
    envelope.taskState.name === "Working"
  )
}

// Write a child session's submitted start as `source` and return its lens,
// which carries the child's parent and root on every record it writes.
/** @internal */
export async function startChild(
  laser: Laser,
  source: AgentId,
  target: AgentId,
  conversation: ConversationId,
  parent: ConversationId,
  root: ConversationId
): Promise<Session> {
  const sessions = laser.sessions()
  const wireParent = WireConversationId.parse(parent.toString())
  const wireRoot = WireConversationId.parse(root.toString())
  const start = encodeNamed(
    encodeSessionStart({
      agent: target.wireId(),
      sdk: sessions.config.sdkInfo,
      parent: wireParent,
      root: wireRoot,
      idleTimeoutMicros: millisToMicros(sessions.config.idleTimeoutValue),
      tags: []
    })
  )
  const session = sessions.open(conversation).asAgent(source).withAncestry(parent, root)
  await session
    .lane()
    .status(OPERATION_SESSION)
    .withTaskState(taskStateFromCode(TaskStateName.Submitted))
    .body(start)
    .contentType(ContentType.Cbor)
    .withAncestry(wireParent, wireRoot)
    .send()
  return session
}

function failureReason(outcome: Contract): string {
  switch (outcome.kind) {
    case "completed":
      return "completed"
    case "failed":
      return "the target replied with a terminal error"
    case "notConsumed":
      return "the command was not consumed within the expiry"
    case "timedOut":
      return "no terminal reply landed within the deadline"
  }
}

function contractFailure(message: string): AgentErrorBody {
  return { code: { kind: "known", name: "Internal" }, message, retryable: false }
}

export class ContractBuilder {
  private source: AgentId | undefined
  private body: Uint8Array = new Uint8Array()
  private route: InboxRoute = ADVERTISED_INBOX_ROUTE
  private replyTopic: string = AgentTopic.Sessions
  private expiryMicros: bigint | undefined
  private deadlineMs = DEFAULT_DEADLINE_MS
  private fenceToken: bigint | undefined
  private conversationId: ConversationId | undefined
  private parentIds: { readonly parent: ConversationId; readonly root: ConversationId } | undefined
  private correlationId: CorrelationId | undefined
  private idempotency: IdempotencyKey | undefined
  private resumePositions: readonly bigint[] | undefined
  private excluded: readonly AgentId[] = []

  private constructor(
    private readonly laser: Laser,
    private readonly router: Router,
    private readonly nowMicros: () => bigint = () => BigInt(Date.now()) * 1000n
  ) {}

  /** @internal */
  static create(laser: Laser, router: Router, nowMicros?: () => bigint): ContractBuilder {
    return new ContractBuilder(laser, router, nowMicros)
  }

  from(source: AgentId): this {
    this.source = source
    return this
  }

  payload(payload: BytesLike): this {
    this.body = ownedBytes(payload)
    return this
  }

  inboxRoute(route: InboxRoute): this {
    this.route = route
    return this
  }

  replyOn(topic: string): this {
    this.replyTopic = topic
    return this
  }

  expireIfNotConsumed(expiryMs: number): this {
    this.expiryMicros = durationMicros("contract consumption expiry", expiryMs)
    return this
  }

  deadline(deadlineMs: number): this {
    this.deadlineMs = duration("contract completion deadline", deadlineMs)
    return this
  }

  conversation(conversation: ConversationId): this {
    this.conversationId = conversation
    return this
  }

  fence(fence: bigint): this {
    if (fence < 0n || fence > 0xffff_ffff_ffff_ffffn) {
      throw new InvalidError("contract fence must be an unsigned 64-bit integer")
    }
    this.fenceToken = fence
    return this
  }

  /** Run the contract as a child session of `parent`, in the tree rooted at
   * `root` (the parent itself when it has no parent). `send` writes the
   * child's submitted start on `agent.sessions` before the command, stamps the
   * ancestry on the command, and ends the child by the outcome: completed on a
   * reply, failed otherwise, including a command that failed to publish. A
   * lifecycle record that fails to publish surfaces as the error unless the
   * contract itself already erred. */
  parent(parent: ConversationId, root: ConversationId): this {
    this.parentIds = { parent, root }
    return this
  }

  /** A correlation stable across sends of the same logical request.
   * @internal */
  correlation(correlation: CorrelationId): this {
    this.correlationId = correlation
    return this
  }

  /** The command's idempotency key, so the target drops a repeated send.
   * @internal */
  idempotencyKey(key: IdempotencyKey): this {
    this.idempotency = key
    return this
  }

  /** Look for a terminal from `positions` of the reply topic before sending,
   * and send only when none landed.
   * @internal */
  resumeFrom(positions: readonly bigint[] | undefined): this {
    this.resumePositions = positions
    return this
  }

  /** Prefer a capable target outside `agents`, reusing one of them only when
   * no other agent is capable.
   * @internal */
  excluding(agents: readonly AgentId[]): this {
    this.excluded = agents
    return this
  }

  /** Resolve the target, send the command, and await its terminal state. The
   * command carries the completion deadline, or the earlier consumption
   * expiry, as its `deadline_micros`, so a target that picks it up later drops
   * it instead of working on a task its caller gave up on. */
  async send(options: CallOptions = {}): Promise<Contract> {
    return (await this.sendResolved(options)).contract
  }

  /** `send`, also naming the agent the route resolved.
   * @internal */
  sendResolved(
    options: CallOptions = {}
  ): Promise<{ readonly target: AgentId; readonly contract: Contract }> {
    return cancellable(options.signal, "contract", () => this.resolveAndSend(options))
  }

  // An abort releases the caller at once while a started child still ends failed.
  private async resolveAndSend(
    options: CallOptions
  ): Promise<{ readonly target: AgentId; readonly contract: Contract }> {
    const source = this.source
    if (source === undefined) {
      throw new InvalidError("a contract requires `.from(source agent id)`")
    }
    const resolved = await resolveContract(
      this.laser,
      this.router,
      this.route,
      this.nowMicros(),
      this.excluded,
      options
    )
    const conversation = this.conversationId ?? ConversationId.new()
    const correlation = this.correlationId ?? CorrelationId.parse(ConversationId.new().toString())
    const expected: ExpectedReply = { session: conversation, requester: source.asStr() }
    const hub = await this.laser[INTERNAL_REPLY_HUB](this.replyTopic, source)
    const ticket = hub.subscribeStream(correlation.toString(), resolved.expectedSigner, expected)
    try {
      throwIfAborted(options.signal, "contract")
      const landed =
        this.resumePositions === undefined
          ? undefined
          : await hub.landed(
              this.resumePositions,
              correlation.toString(),
              expected,
              resolved.expectedSigner
            )
      // A child session's start lands before its command, so a reader never
      // sees work for a session that does not exist yet.
      const ancestry = this.parentIds
      let child: Session | undefined
      if (ancestry !== undefined && landed === undefined) {
        throwIfAborted(options.signal, "contract")
        child = await startChild(
          this.laser,
          source,
          resolved.target,
          conversation,
          ancestry.parent,
          ancestry.root
        )
      } else if (ancestry !== undefined) {
        child = this.laser
          .sessions()
          .open(conversation)
          .asAgent(source)
          .withAncestry(ancestry.parent, ancestry.root)
      }
      let outcome: Contract
      try {
        if (landed !== undefined) {
          outcome = terminal(landed)
        } else {
          await this.command(source, resolved, conversation, correlation).send(options)
          outcome = await this.watch(ticket, resolved.expectedSigner, options.signal)
        }
      } catch (error) {
        // The contract error wins over a lifecycle error.
        await child
          ?.fail(contractFailure(error instanceof Error ? error.message : String(error)))
          .catch(() => undefined)
        throw error
      }
      if (child !== undefined) {
        if (outcome.kind === "completed") await child.end()
        else await child.fail(contractFailure(failureReason(outcome)))
      }
      return { target: resolved.target, contract: outcome }
    } finally {
      ticket.cancel()
    }
  }

  // The AGDX command. Its `deadline_micros` is the earlier of the consumption
  // expiry and the completion deadline, the point after which the target's
  // consumer drops it instead of handling it.
  private command(
    source: AgentId,
    resolved: ResolvedContract,
    conversation: ConversationId,
    correlation: CorrelationId
  ): AgdxSend {
    const deadlineMicros = millisToMicros(this.deadlineMs)
    const lapse =
      this.expiryMicros !== undefined && this.expiryMicros < deadlineMicros
        ? this.expiryMicros
        : deadlineMicros
    let command = this.laser
      .agdx(resolved.inbox, source, conversation)
      .command(correlation, this.body)
      .withTarget(resolved.target)
      .withDeadlineAtMicros(this.nowMicros() + lapse)
    if (this.fenceToken !== undefined) {
      command = command.withMetadata(FENCE, { kind: "uint", value: this.fenceToken })
    }
    const ancestry = this.parentIds
    if (ancestry !== undefined) {
      command = command
        .withAncestry(
          WireConversationId.parse(ancestry.parent.toString()),
          WireConversationId.parse(ancestry.root.toString())
        )
        .withMetadata(METADATA_SUBMITTED, { kind: "bool", value: true })
    }
    if (this.idempotency !== undefined) command = command.withIdempotencyKey(this.idempotency)
    return command
  }

  // A missing pickup acknowledgment proves nothing: the target may lack
  // `ackOnPickup` or be down. Only its answer that the expiry dropped the
  // command reads as not consumed.
  private async watch(
    ticket: ReplyStreamTicket,
    expectedSigner: string,
    signal: AbortSignal | undefined
  ): Promise<Contract> {
    const started = performance.now()
    for (;;) {
      const elapsed = performance.now() - started
      if (elapsed >= this.deadlineMs) return { kind: "timedOut" }
      try {
        const candidate = await ticket.next(Math.max(0, this.deadlineMs - elapsed), signal)
        const reply = acceptsReply(this.laser, candidate, expectedSigner)
        if (reply === undefined) continue
        if (isWorking(reply.envelope)) continue
        return terminal(reply)
      } catch (error) {
        if (!(error instanceof TimeoutError)) throw error
      }
    }
  }
}

function terminal(reply: AgentMessage): Contract {
  const envelope = reply.envelope
  if (envelope?.kind !== AgentKind.Error) return { kind: "completed", reply }
  return droppedUnconsumed(envelope.body) ? { kind: "notConsumed" } : { kind: "failed", reply }
}

// Whether the target answered that its consumer dropped the command past its
// deadline, before any handler ran.
function droppedUnconsumed(body: Uint8Array): boolean {
  try {
    const context = "AGDX error body"
    const reason = decodeAgentErrorBody(
      expectMap(decodeOne(body, context), context),
      context
    ).detail?.get(DEAD_LETTER_REASON_DETAIL)
    return (
      reason !== undefined &&
      (reason.kind === "int" || reason.kind === "uint") &&
      reason.value === BigInt(deadLetterReasonCode({ kind: "known", name: "DeadlineExceeded" }))
    )
  } catch {
    return false
  }
}

/** Scatter a contract to every agent advertising the selector's skill and
 * gather the branches under `policy` within `deadlineMs`. Branches still
 * running when the policy stops the gather are `cancelled`: the gather no
 * longer waits on them, and their command carries the deadline, so a target
 * that picks one up later drops it. Refuses a quorum below one. */
export async function scatterReport(
  laser: Laser,
  source: AgentId,
  selector: CapabilitySelector,
  payload: BytesLike,
  inboxRoute: InboxRoute,
  policy: GatherPolicy,
  deadlineMs: number,
  nowMicros: bigint = BigInt(Date.now()) * 1000n,
  options: CallOptions = {}
): Promise<ScatterReport> {
  duration("scatter deadline", deadlineMs)
  validateGatherPolicy(policy)
  throwIfAborted(options.signal, "scatter")
  const registry = await abortable(laser.agentRegistry(), options.signal, "scatter")
  await registry.refresh(nowMicros, options)
  if (inboxRoute.kind === "advertised" || selector.principal !== undefined) {
    await registry.refreshPresence(options)
  }
  const agents = resolveTargets({ kind: "allCapable", selector }, registry, nowMicros)
  const branches: Branch<Contract>[] = agents.map((agent) => {
    const controller = new AbortController()
    const signal =
      options.signal === undefined
        ? controller.signal
        : AbortSignal.any([controller.signal, options.signal])
    return {
      agent,
      controller,
      result: laser
        .contract(routeTo(agent))
        .from(source)
        .payload(payload)
        .inboxRoute(inboxRoute)
        .deadline(deadlineMs)
        .send({ signal })
        .then(
          (value): BranchResult<Contract> => ({ kind: "ok", value }),
          (error: unknown): BranchResult<Contract> => ({
            kind: "error",
            error: asLaserError(error)
          })
        )
    }
  })
  const gathered = await abortable(
    gatherBranches(branches, policy, deadlineMs, (contract) => contract.kind === "completed"),
    options.signal,
    "scatter"
  )
  return new ScatterReport(
    gathered.outcomes.map(([agent, result]): ScatterOutcome => ({
      agent,
      result:
        result.kind === "ok"
          ? { kind: "ok", contract: result.value }
          : { kind: "err", error: result.error }
    })),
    gathered.cancelled,
    gatherSatisfied(policy, gathered.successes, branches.length)
  )
}

/** The reply body of every scatter branch that completed, a convenience over
 * `scatterReport`. */
export async function scatter(
  laser: Laser,
  source: AgentId,
  selector: CapabilitySelector,
  payload: BytesLike,
  inboxRoute: InboxRoute,
  policy: GatherPolicy,
  deadlineMs: number,
  nowMicros: bigint = BigInt(Date.now()) * 1000n,
  options: CallOptions = {}
): Promise<readonly Uint8Array[]> {
  const report = await scatterReport(
    laser,
    source,
    selector,
    payload,
    inboxRoute,
    policy,
    deadlineMs,
    nowMicros,
    options
  )
  return report.completed().map(({ reply }) => agentMessageBody(reply))
}

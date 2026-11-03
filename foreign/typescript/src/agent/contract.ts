import { millisToMicros } from "../client/duration.js"
import { ownedBytes, type BytesLike } from "../client/bytes.js"
import { InvalidError, TimeoutError, type LaserError } from "../client/errors.js"
import { INTERNAL_REPLY_HUB, INTERNAL_VERIFIER } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { ConversationId, type AgentId } from "../types/ids.js"
import {
  AgentKind,
  type AgentEnvelope,
  type AgentErrorBody,
  METADATA_SUBMITTED,
  OPERATION_SESSION,
  TaskStateName,
  encodeSessionStart,
  taskStateFromCode
} from "../wire/agent.js"
import { encodeNamed } from "../wire/cbor.js"
import { ContentType } from "../wire/content.js"
import type { Session } from "../session.js"
import { FENCE } from "../wire/headers.js"
import { ConversationId as WireConversationId, CorrelationId } from "../wire/ids.js"
import { agentMessageBody, type AgentMessage } from "./reliable-consumer.js"
import type { ReplyStreamTicket } from "./replies.js"
import {
  ADVERTISED_INBOX_ROUTE,
  requiredPrincipal,
  resolveInboxRoute,
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

export class ScatterReport {
  constructor(readonly outcomes: readonly ScatterOutcome[]) {}

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
  nowMicros: bigint
): Promise<ResolvedContract> {
  if (router.kind === "broadcast" || router.kind === "allCapable") {
    throw new InvalidError(
      "a contract is directed to one agent, not a broadcast or all-capable route"
    )
  }
  const registry = await laser.agentRegistry()
  await registry.refresh(nowMicros)
  if (inboxRoute.kind === "advertised" || routeRequiresPresence(router)) {
    await registry.refreshPresence()
  }
  const target = resolveTargets(router, registry, nowMicros)[0]
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
   * reply, failed otherwise. A lifecycle record that fails to publish surfaces
   * as the error unless the contract itself already erred. */
  parent(parent: ConversationId, root: ConversationId): this {
    this.parentIds = { parent, root }
    return this
  }

  async send(): Promise<Contract> {
    const source = this.source
    if (source === undefined) {
      throw new InvalidError("a contract requires `.from(source agent id)`")
    }
    const resolved = await resolveContract(this.laser, this.router, this.route, this.nowMicros())
    const conversation = this.conversationId ?? ConversationId.new()
    const actualCorrelation = CorrelationId.parse(ConversationId.new().toString())
    // A child session's start lands before its command, so a reader never sees
    // work for a session that does not exist yet.
    const ancestry = this.parentIds
    const child =
      ancestry === undefined
        ? undefined
        : await startChild(
            this.laser,
            source,
            resolved.target,
            conversation,
            ancestry.parent,
            ancestry.root
          )
    const hub = await this.laser[INTERNAL_REPLY_HUB](this.replyTopic, source)
    const ticket = hub.subscribeStream(actualCorrelation.toString(), resolved.expectedSigner)
    try {
      let command = this.laser
        .agdx(resolved.inbox, source, conversation)
        .command(actualCorrelation, this.body)
        .withTarget(resolved.target)
      if (this.fenceToken !== undefined) {
        command = command.withMetadata(FENCE, { kind: "uint", value: this.fenceToken })
      }
      if (ancestry !== undefined) {
        command = command
          .withAncestry(
            WireConversationId.parse(ancestry.parent.toString()),
            WireConversationId.parse(ancestry.root.toString())
          )
          .withMetadata(METADATA_SUBMITTED, { kind: "bool", value: true })
      }
      if (this.expiryMicros !== undefined) {
        command = command.withDeadlineMicros(this.nowMicros() + this.expiryMicros)
      }
      await command.send()
      let outcome: Contract
      try {
        outcome = await this.watch(ticket, resolved.expectedSigner)
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
      return outcome
    } finally {
      ticket.cancel()
    }
  }

  private async watch(ticket: ReplyStreamTicket, expectedSigner: string): Promise<Contract> {
    const started = performance.now()
    const expiryMs = this.expiryMicros === undefined ? undefined : Number(this.expiryMicros) / 1_000
    let consumed = false
    for (;;) {
      const elapsed = performance.now() - started
      if (!consumed && expiryMs !== undefined && elapsed >= expiryMs) {
        return { kind: "notConsumed" }
      }
      if (elapsed >= this.deadlineMs) return { kind: "timedOut" }
      const nextBoundary = Math.min(
        this.deadlineMs - elapsed,
        !consumed && expiryMs !== undefined ? expiryMs - elapsed : Number.POSITIVE_INFINITY
      )
      try {
        const candidate = await ticket.next(Math.max(0, nextBoundary))
        const reply = acceptsReply(this.laser, candidate, expectedSigner)
        if (reply === undefined) continue
        const envelope = reply.envelope
        if (isWorking(envelope)) {
          consumed = true
          continue
        }
        if (envelope === undefined || envelope.kind === AgentKind.Response) {
          return { kind: "completed", reply }
        }
        if (envelope.kind === AgentKind.Error) return { kind: "failed", reply }
      } catch (error) {
        if (!(error instanceof TimeoutError)) throw error
      }
    }
  }
}

export async function scatterReport(
  laser: Laser,
  source: AgentId,
  selector: CapabilitySelector,
  payload: BytesLike,
  inboxRoute: InboxRoute,
  deadlineMs: number,
  nowMicros: bigint = BigInt(Date.now()) * 1000n
): Promise<ScatterReport> {
  duration("scatter deadline", deadlineMs)
  const registry = await laser.agentRegistry()
  await registry.refresh(nowMicros)
  if (inboxRoute.kind === "advertised" || selector.principal !== undefined) {
    await registry.refreshPresence()
  }
  const agents = resolveTargets({ kind: "allCapable", selector }, registry, nowMicros)
  const outcomes: ScatterOutcome[] = []
  await Promise.all(
    agents.map(async (agent) => {
      try {
        const contract = await laser
          .contract(routeTo(agent))
          .from(source)
          .payload(payload)
          .inboxRoute(inboxRoute)
          .deadline(deadlineMs)
          .send()
        outcomes.push({ agent, result: { kind: "ok", contract } })
      } catch (error) {
        outcomes.push({
          agent,
          result: {
            kind: "err",
            error:
              error instanceof Error && "kind" in error
                ? (error as LaserError)
                : new InvalidError(String(error))
          }
        })
      }
    })
  )
  return new ScatterReport(outcomes)
}

export async function scatter(
  laser: Laser,
  source: AgentId,
  selector: CapabilitySelector,
  payload: BytesLike,
  inboxRoute: InboxRoute,
  deadlineMs: number,
  nowMicros: bigint = BigInt(Date.now()) * 1000n
): Promise<readonly Uint8Array[]> {
  const report = await scatterReport(
    laser,
    source,
    selector,
    payload,
    inboxRoute,
    deadlineMs,
    nowMicros
  )
  return report.completed().map(({ reply }) => agentMessageBody(reply))
}

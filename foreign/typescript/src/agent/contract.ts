import { ownedBytes, type BytesLike } from "../client/bytes.js"
import { InvalidError, TimeoutError, UnsupportedError, type LaserError } from "../client/errors.js"
import { INTERNAL_REPLY_HUB, INTERNAL_VERIFIER } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { ConversationId, type AgentId } from "../types/ids.js"
import {
  AgentKind,
  METADATA_RUN,
  OPERATION_TASK,
  type TaskState,
  type AgentEnvelope
} from "../wire/agent.js"
import { FENCE } from "../wire/headers.js"
import { CorrelationId } from "../wire/ids.js"
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
    expectedSigner: requiredPrincipal(router)?.toString() ?? target.asString()
  }
}

function acceptsReply(
  laser: Laser,
  message: AgentMessage,
  expectedSigner: string
): AgentMessage | undefined {
  const verifier = laser[INTERNAL_VERIFIER]()
  if (verifier === undefined) return message
  const envelope = message.envelope
  if (envelope === undefined) return undefined
  try {
    const principal = verifier.verify(envelope)
    return principal === expectedSigner ? { ...message, verifiedPrincipal: principal } : undefined
  } catch {
    return undefined
  }
}

function isWorking(envelope: AgentEnvelope | undefined): boolean {
  return (
    envelope?.kind === AgentKind.Status &&
    envelope.taskState?.kind === "known" &&
    envelope.taskState.name === "Working"
  )
}

export async function markRun(
  laser: Laser,
  source: AgentId,
  conversation: ConversationId,
  run: string,
  state: TaskState,
  detail?: string
): Promise<void> {
  const correlation = CorrelationId.parse(conversation.toString())
  let status = laser
    .agdx(AgentTopic.Responses, source, conversation)
    .status(OPERATION_TASK)
    .withCorrelation(correlation)
    .withTaskState(state)
    .withMetadata(METADATA_RUN, { kind: "string", value: run })
  if (detail !== undefined) {
    status = status.withMetadata("detail", { kind: "string", value: detail })
  }
  await status.send()
}

export class ContractBuilder {
  private source: AgentId | undefined
  private body: Uint8Array = new Uint8Array()
  private route: InboxRoute = ADVERTISED_INBOX_ROUTE
  private replyTopic: string = AgentTopic.Responses
  private expiryMs: number | undefined
  private deadlineMs = DEFAULT_DEADLINE_MS
  private fenceToken: bigint | undefined
  private conversationId: ConversationId | undefined
  private registerRun = false

  constructor(
    private readonly laser: Laser,
    private readonly router: Router,
    private readonly nowMicros: () => bigint = () => BigInt(Date.now()) * 1000n
  ) {}

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
    this.expiryMs = duration("contract consumption expiry", expiryMs)
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

  registered(): this {
    this.registerRun = true
    return this
  }

  async send(): Promise<Contract> {
    const source = this.source
    if (source === undefined) {
      throw new InvalidError("a contract requires `.from(source agent id)`")
    }
    if (this.registerRun && !(await this.laser.capabilities()).agentWorkflow) {
      throw new UnsupportedError(
        "a registered contract requires a plane that serves the run registry"
      )
    }
    const resolved = await resolveContract(this.laser, this.router, this.route, this.nowMicros())
    const conversation = this.conversationId ?? ConversationId.new()
    const actualCorrelation = CorrelationId.parse(ConversationId.new().toString())
    const hub = await this.laser[INTERNAL_REPLY_HUB](this.replyTopic)
    const ticket = hub.subscribeStream(actualCorrelation.toString())
    let run: string | undefined
    try {
      if (this.registerRun) {
        run = (await this.laser.runs().submitWith(resolved.target.asString(), { input: this.body }))
          .runId
      }
      let command = this.laser
        .agdx(resolved.inbox, source, conversation)
        .command(actualCorrelation, this.body)
        .withTarget(resolved.target)
      if (this.fenceToken !== undefined) {
        command = command.withMetadata(FENCE, { kind: "int", value: this.fenceToken })
      }
      if (run !== undefined)
        command = command.withMetadata(METADATA_RUN, { kind: "string", value: run })
      if (this.expiryMs !== undefined) {
        command = command.withDeadlineMicros(this.nowMicros() + BigInt(this.expiryMs) * 1000n)
      }
      await command.send()
      if (run !== undefined) {
        await markRun(this.laser, source, conversation, run, { kind: "known", name: "Working" })
      }
      const outcome = await this.watch(ticket, resolved.expectedSigner)
      if (run !== undefined) {
        const state = outcome.kind === "completed" ? "Completed" : "Failed"
        const detail =
          outcome.kind === "failed"
            ? "the target replied with a terminal error"
            : outcome.kind === "notConsumed"
              ? "the command was not consumed within the expiry"
              : outcome.kind === "timedOut"
                ? "no terminal reply landed within the deadline"
                : undefined
        await markRun(this.laser, source, conversation, run, { kind: "known", name: state }, detail)
      }
      return outcome
    } finally {
      ticket.cancel()
    }
  }

  private async watch(ticket: ReplyStreamTicket, expectedSigner: string): Promise<Contract> {
    const started = performance.now()
    let consumed = false
    for (;;) {
      const elapsed = performance.now() - started
      if (!consumed && this.expiryMs !== undefined && elapsed >= this.expiryMs) {
        return { kind: "notConsumed" }
      }
      if (elapsed >= this.deadlineMs) return { kind: "timedOut" }
      const nextBoundary = Math.min(
        this.deadlineMs - elapsed,
        !consumed && this.expiryMs !== undefined
          ? this.expiryMs - elapsed
          : Number.POSITIVE_INFINITY
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

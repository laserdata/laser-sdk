import { code } from "../client/error-classify.js"
import {
  CancelledError,
  ConsumerGroupSetupError,
  FilterExecutionError,
  FilterFaultError,
  FilterOversizedRecordError,
  HandlerError,
  InvalidError,
  LaserError,
  NoStreamError,
  TimeoutError,
  TransportError,
  publishCause
} from "../client/errors.js"
import {
  INTERNAL_ASSIGNED_PARTITIONS,
  INTERNAL_COMMIT_HANDLED,
  INTERNAL_DIALED,
  INTERNAL_NATIVE_CONSUMER,
  INTERNAL_TRANSPORT
} from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import type { LaserTransport } from "../iggy/apache-iggy.js"
import type { HeaderValue } from "../stream/header-value.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import type { Provenance } from "../provenance/provenance.js"
import { SystemClock, type Clock } from "../runtime/clock.js"
import { warn } from "../runtime/warn.js"
import type { KeyRegistry, SigningKey } from "../signing.js"
import type { SessionConfig, Sessions } from "../session.js"
import {
  type AgentId,
  ConversationId,
  type ConsumerGroupName,
  type MessageId
} from "../types/ids.js"
import {
  AgentKind,
  OPERATION_TASK,
  TaskStateName,
  encodeAgentDeadLetter,
  METADATA_SUBMITTED,
  deadLetterReasonCode,
  parseAgentId,
  taskStateFromCode,
  type AgentDeadLetter,
  type AgentId as WireAgentId,
  type DeadLetterReasonName
} from "../wire/agent.js"
import { resultCodeIsRetryable } from "../wire/result.js"
import { encodeNamed } from "../wire/cbor.js"
import { type Dispatch, addresseeFilter, classify, classifyGeneric } from "../wire/dispatch.js"
import { CONVERSATION_ID } from "../wire/headers.js"
import { type LogPosition, crockfordEncode } from "../wire/ids.js"
import { AGENT_CONTROL, AGENT_SESSIONS } from "../wire/topics.js"
import type { Consumer, ConsumerMessage } from "../stream/consumer.js"
import { policyAware } from "../stream/consumer-group.js"
import { AgentCtx } from "./context.js"
import {
  type AgentMessage,
  type ReceivedAgentMessage,
  decodeAgentMessage,
  headersMalformed
} from "./decode.js"
import { BudgetGate } from "./budget.js"
import { ControlBook } from "./control.js"
import {
  ControlFollower,
  PauseDriver,
  PauseRequests,
  PauseRuntime,
  type SourceTopic,
  loadControl
} from "./pause.js"
import type { SessionLease } from "./lease.js"
import { ADVERTISED_INBOX_ROUTE, type InboxRoute } from "./router.js"

const DEDUP_SCOPE_SEP = "\u001f"
const TEXT_ENCODER = new TextEncoder()

const FENCE_MAP_SOFT_CAP = 16_384

const FENCE_ENTRY_TTL_MICROS = 600_000_000n

const FENCE_SWEEP_INTERVAL_MICROS = 1_000_000n
// How many verified record ids a consumer remembers to refuse a replay of the
// exact signed bytes, sized like the fence map.
const VERIFIED_RECORD_WINDOW = 16_384
// The addressee an agentless consumer classifies an untargeted record as.
const ANONYMOUS_AGENT = "anonymous"

export {
  agentMessageBody,
  contentTypeOf,
  decodeAgentMessage,
  provenanceAndEnvelope,
  provenanceFromEnvelope
} from "./decode.js"
export type {
  AgentMessage,
  DecodedAgentMessage,
  ProvenanceAndEnvelope,
  ReceivedAgentMessage
} from "./decode.js"

export interface RetryPolicy {
  readonly maxAttempts: number
  readonly baseDelayMs: number
}

export function retryBackoff(maxAttempts: number, baseDelayMs: number): RetryPolicy {
  return { maxAttempts, baseDelayMs }
}

export const DEFAULT_RETRY_POLICY: RetryPolicy = { maxAttempts: 5, baseDelayMs: 200 }

export function retryDelayMs(policy: RetryPolicy, attempt: number): number {
  return policy.baseDelayMs * 2 ** Math.min(attempt, 16)
}

export type ConcurrencyPolicy =
  | { readonly kind: "serial" }
  | { readonly kind: "serial-per-partition"; readonly maxPartitions: number }

export const SERIAL_CONCURRENCY: ConcurrencyPolicy = { kind: "serial" }

export interface Deduplicator {
  observe(key: string): Promise<boolean>
}

export class SlidingWindow implements Deduplicator {
  private readonly capacity: number
  private readonly seen = new Set<string>()
  private readonly order: string[] = []

  constructor(capacity: number) {
    this.capacity = Math.max(capacity, 1)
  }

  observe(key: string): Promise<boolean> {
    if (this.seen.has(key)) return Promise.resolve(false)
    if (this.order.length >= this.capacity) {
      const evicted = this.order.shift()
      if (evicted !== undefined) this.seen.delete(evicted)
    }
    this.seen.add(key)
    this.order.push(key)
    return Promise.resolve(true)
  }
}

export function dedupKey(provenance: Provenance): string | undefined {
  if (provenance.idempotencyKey === undefined) return undefined
  return provenance.agent !== undefined
    ? `${provenance.agent.asStr()}${DEDUP_SCOPE_SEP}${provenance.idempotencyKey}`
    : provenance.idempotencyKey
}

export interface FenceEntry {
  readonly fence: bigint
  readonly touchedMicros: bigint
}

export interface FenceSweepState {
  lastSweepMicros: bigint
}

export function acceptFence(
  highWater: Map<string, FenceEntry>,
  sweepState: FenceSweepState,
  taskKey: string,
  fence: bigint,
  nowMicros: bigint
): boolean {
  if (
    highWater.size > FENCE_MAP_SOFT_CAP &&
    nowMicros - sweepState.lastSweepMicros > FENCE_SWEEP_INTERVAL_MICROS
  ) {
    sweepState.lastSweepMicros = nowMicros
    for (const [key, entry] of highWater) {
      if (nowMicros - entry.touchedMicros >= FENCE_ENTRY_TTL_MICROS) {
        highWater.delete(key)
      }
    }
  }
  const existing = highWater.get(taskKey)
  if (existing !== undefined && fence < existing.fence) return false
  highWater.set(taskKey, { fence, touchedMicros: nowMicros })
  return true
}

export interface AgentHandler {
  handle(message: AgentMessage, context: AgentCtx): Promise<void>
}

export type HandlerResult =
  { readonly kind: "ok" } | { readonly kind: "error"; readonly error: LaserError }

export interface AgentMiddleware {
  beforeHandle?(message: AgentMessage): Promise<void>
  afterHandle?(
    message: AgentMessage,
    result: { readonly kind: "ok" } | { readonly kind: "error"; readonly error: LaserError },
    attempt: number
  ): Promise<void>
}

export interface DeadLetterSink {
  onDeadLetter(
    message: AgentMessage | undefined,
    capsule: AgentDeadLetter,
    publishError: LaserError | undefined
  ): Promise<void>
}

export interface ReliableConsumerOptions {
  readonly group: ConsumerGroupName
  readonly topic: string
  readonly agent?: AgentId
  readonly dedupWindow?: number
  readonly retry?: RetryPolicy
  readonly pollIntervalMs?: number
  /** Bounds active work after shutdown is requested. Defaults to 30 seconds. */
  readonly shutdownGraceMs?: number
  readonly concurrency?: ConcurrencyPolicy
  readonly maxQueuedRecords?: number
  readonly maxQueuedBytes?: number
  readonly understoodFeatures?: bigint
  readonly respondOn?: string
  readonly inboxRoute?: InboxRoute
  readonly ackOnPickup?: boolean
  readonly deduplicator?: Deduplicator
  readonly warmDedup?: boolean
  readonly middleware?: readonly AgentMiddleware[]
  readonly onDeadLetter?: DeadLetterSink
  /** The command operations the handler serves. Unset serves every operation.
   * A command for another operation is skipped, so a model or tool record an
   * agent writes for itself never becomes its own work. */
  readonly operations?: readonly string[]
  /** The session configuration the runtime applies: the session lens handlers
   * read through `AgentCtx.session`, and `SessionConfig.failOnDeadLetter`.
   * Defaults to `new SessionConfig()`. */
  readonly sessions?: SessionConfig
  /** Deadline and fence checks read this clock, the system clock by default.
   * @internal */
  readonly clock?: Clock
  readonly verifier?: KeyRegistry
  readonly signingKey?: SigningKey
}

export interface ReliableConsumerControl {
  readonly signal?: AbortSignal
  readonly hardSignal?: AbortSignal
  readonly ready?: () => void
  readonly hardAborted?: () => boolean
}

function handlerError(error: unknown): LaserError {
  return error instanceof LaserError
    ? error
    : new HandlerError(error instanceof Error ? error.message : String(error), { cause: error })
}

/** Whether retrying the same call can succeed, the classifier the Rust
 * `LaserError::is_retryable` defines. A failed publish answers for its cause.
 * Managed failures follow the canonical result-code classifier, while
 * transport, handler, routing, timeout, and deferred-policy failures keep
 * their local semantics. */
export function isRetryable(error: LaserError): boolean {
  const cause = publishCause(error)
  if (!(cause instanceof LaserError)) return false
  switch (cause.kind) {
    case "config":
    case "no-stream":
    case "no-respond-topic":
    case "ambiguous-mutation":
    case "unsupported":
    case "invalid":
    case "id":
    case "provenance":
    case "codec":
    case "protocol":
    case "handler-config":
    case "state-store":
    case "integrity":
    case "rejected":
    case "presence-conflict":
    case "policy-blocked":
    case "step-up-required":
    case "cancelled":
    case "typed-decode":
    case "authz":
    case "signature":
    case "budget-exceeded":
    case "fence-violation":
    case "quarantined":
      return false
    case "publish-failed":
      return false
    case "transport":
      return cause instanceof TransportError ? cause.retryable : true
    case "routing":
      return !(
        "reason" in cause &&
        (cause as { readonly reason?: { readonly kind?: string } }).reason?.kind ===
          "principalMismatch"
      )
    case "query":
    case "kv":
    case "fork":
    case "graph":
    case "checkpoint":
    case "session":
      return resultCodeIsRetryable(code(cause))
    case "filter":
      if (cause instanceof ConsumerGroupSetupError) {
        return cause.cause instanceof LaserError && isRetryable(cause.cause)
      }
      if (cause instanceof FilterFaultError || cause instanceof FilterOversizedRecordError) {
        return false
      }
      return cause instanceof FilterExecutionError && resultCodeIsRetryable(cause.detail.code)
    case "timeout":
    case "handler":
    case "policy-deferred":
      return true
  }
}

// The next record, or undefined when none arrived within `waitMs`.
async function nextOrIdle(
  consumer: Consumer,
  waitMs: number,
  signal?: AbortSignal
): Promise<ConsumerMessage | undefined> {
  try {
    return await consumer.nextWithin(waitMs, signal === undefined ? {} : { signal })
  } catch (error) {
    if (
      error instanceof TimeoutError ||
      (error instanceof CancelledError && signal?.aborted === true)
    )
      return undefined
    throw error
  }
}

// The worker reads the record's log offset, which a delivery carries in its position.
function received(message: ConsumerMessage): ReceivedAgentMessage {
  return {
    payload: message.payload,
    partitionId: message.partitionId,
    offset: message.position.offset,
    timestampMicros: message.timestampMicros,
    headers: message.headers,
    headersMalformed: message.headersMalformed,
    currentOffset: message.currentOffset
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted === true) return Promise.resolve()
  return new Promise((resolve) => {
    let timer: ReturnType<typeof setTimeout>
    const deadline = Date.now() + ms
    const finish = (): void => {
      clearTimeout(timer)
      signal?.removeEventListener("abort", finish)
      resolve()
    }
    const tick = (): void => {
      const remaining = deadline - Date.now()
      if (remaining <= 0) finish()
      else timer = setTimeout(tick, Math.min(remaining, 2_147_483_647))
    }
    signal?.addEventListener("abort", finish, { once: true })
    timer = setTimeout(tick, Math.min(Math.max(0, ms), 2_147_483_647))
  })
}

async function consumeUntilDone(
  work: Promise<void>,
  hardSignal: AbortSignal | undefined
): Promise<boolean> {
  if (hardSignal === undefined) {
    await work
    return true
  }
  if (hardSignal.aborted) {
    void work.catch(() => undefined)
    return false
  }
  let removeAbort = (): void => undefined
  const aborted = new Promise<false>((resolve) => {
    const onAbort = (): void => {
      resolve(false)
    }
    hardSignal.addEventListener("abort", onAbort, { once: true })
    removeAbort = () => {
      hardSignal.removeEventListener("abort", onAbort)
    }
  })
  const completed = work.then(() => true)
  try {
    return await Promise.race([completed, aborted])
  } finally {
    removeAbort()
  }
}

function shutdownControl(control: ReliableConsumerControl, graceMs: number) {
  const forced = new AbortController()
  const hardSignal =
    control.hardSignal === undefined
      ? forced.signal
      : AbortSignal.any([forced.signal, control.hardSignal])
  const signal =
    control.signal === undefined ? hardSignal : AbortSignal.any([control.signal, hardSignal])
  let expired = false
  let timer: ReturnType<typeof setTimeout> | undefined
  const onShutdown = (): void => {
    const deadline = Date.now() + graceMs
    const tick = (): void => {
      const remaining = deadline - Date.now()
      if (remaining <= 0) {
        expired = true
        forced.abort("agent shutdown drain")
      } else {
        timer = setTimeout(tick, Math.min(remaining, 2_147_483_647))
      }
    }
    timer = setTimeout(tick, Math.min(graceMs, 2_147_483_647))
  }
  control.signal?.addEventListener("abort", onShutdown, { once: true })
  if (control.signal?.aborted === true) onShutdown()
  return {
    runtime: {
      ...control,
      signal,
      hardSignal,
      hardAborted: () => hardSignal.aborted || control.hardAborted?.() === true
    },
    expired: () => expired,
    stopped: () => signal.aborted,
    dispose: (): void => {
      clearTimeout(timer)
      control.signal?.removeEventListener("abort", onShutdown)
    }
  }
}

class ReliableWorker {
  private readonly highWaterFence = new Map<string, FenceEntry>()
  private readonly fenceSweep: FenceSweepState = { lastSweepMicros: 0n }
  // A signature binds the envelope to no log position, so the exact signed
  // bytes verify again wherever a writer replays them.
  private readonly verifiedRecords = new SlidingWindow(VERIFIED_RECORD_WINDOW)
  // The budget check before a session's work reaches the handler.
  private readonly budget = new BudgetGate()

  constructor(
    private readonly laser: Laser,
    private readonly handler: AgentHandler,
    private readonly options: Required<
      Pick<
        ReliableConsumerOptions,
        | "ackOnPickup"
        | "clock"
        | "deduplicator"
        | "inboxRoute"
        | "middleware"
        | "retry"
        | "understoodFeatures"
      >
    > &
      Pick<
        ReliableConsumerOptions,
        "agent" | "onDeadLetter" | "operations" | "respondOn" | "signingKey" | "verifier"
      > & {
        readonly hardSignal: AbortSignal
        readonly topic: string
        readonly sessions: Sessions
        readonly control: ControlBook
        readonly pause?: PauseRuntime
      },
    private readonly streamId: number,
    private readonly topicId: number
  ) {}

  private cancelled(): boolean {
    return this.options.hardSignal.aborted
  }

  // A consumer without an agent id accepts records for any addressee, so it
  // classifies each record as its own addressee would.
  private dispatch(message: AgentMessage): Dispatch {
    const me =
      this.options.agent?.wireId() ??
      message.provenance.targetAgentId?.wireId() ??
      parseAgentId(ANONYMOUS_AGENT)
    const envelope = message.envelope
    if (envelope !== undefined) {
      return classify(envelope, this.options.topic, me, this.options.operations ?? "any")
    }
    return classifyGeneric(
      message.provenance.targetAgentId?.asStr(),
      message.provenance.causalParent !== undefined,
      message.provenance.correlationId !== undefined,
      this.options.topic,
      me
    )
  }

  async consume(received: ReceivedAgentMessage): Promise<void> {
    await this.deliver(received, true)
  }

  /** Handle a held record again after the resume, without the pause check. */
  async replay(received: ReceivedAgentMessage): Promise<void> {
    await this.deliver(received, false)
  }

  // Tell the pause runtime which partitions the consumer reads, when it
  // knows.
  observeAssignment(consumer: Consumer): void {
    this.options.pause?.observeAssignment(consumer[INTERNAL_ASSIGNED_PARTITIONS]())
  }

  // The consumer was reopened: the pause runtime reads the lane again before
  // the next dispatch.
  invalidatePause(): void {
    this.options.pause?.invalidate()
  }

  // Handle one record. A live record passes the pause check, a held record
  // replayed after the resume does not.
  private async deliver(received: ReceivedAgentMessage, live: boolean): Promise<void> {
    if (this.cancelled()) return
    const decoded = decodeAgentMessage(received, this.options.understoodFeatures)
    if (decoded.kind === "error") {
      await this.deadLetterUndecodable(received, decoded.payload)
      return
    }
    let message = decoded.message
    const target = message.provenance.targetAgentId
    if (
      target !== undefined &&
      this.options.agent !== undefined &&
      !target.equals(this.options.agent)
    ) {
      return
    }
    // Only work for an operation this handler serves reaches it. Replies,
    // status, events, control, and records for another agent are skipped and
    // still committed. The author is never a discriminator.
    if (this.dispatch(message) !== "work") return
    if (this.options.verifier !== undefined) {
      try {
        const envelope = message.envelope
        if (envelope === undefined) throw new InvalidError("verified topic requires an envelope")
        if (decoded.signatureContext === undefined || decoded.observedAtMicros === undefined) {
          throw new InvalidError("verified topic requires observed record headers and timestamp")
        }
        message = {
          ...message,
          verifiedPrincipal: this.options.verifier.verifyObservedAt(
            envelope,
            decoded.signatureContext,
            decoded.observedAtMicros
          ).principal
        }
      } catch {
        await this.deadLetter(message, "Rejected", 0, "signature verification failed")
        return
      }
      const record = message.envelope?.record
      if (record !== undefined && !(await this.verifiedRecords.observe(record.toString()))) return
    }
    // The pause check, before the fence and dedup gates so a held record
    // keeps its slot for the resume. Work for a paused session is parked on
    // the session lane and committed. A resumed session handles its held
    // records first. The session gate stays held through the handler, so a
    // pause acknowledgment follows the record in flight.
    const pause = this.options.pause
    if (!live || pause === undefined) {
      await this.gated(received, message)
      return
    }
    const release = await pause.gate(message.provenance.conversationId.toString())
    try {
      if ((await pause.hold(this, message)) === "commit") return
      await this.gated(received, message)
    } finally {
      release()
    }
  }

  // The budget check, the fence and dedup gates, the deadline, and the
  // handler.
  private async gated(received: ReceivedAgentMessage, message: AgentMessage): Promise<void> {
    // The budget check, on a deployment that indexes sessions: work for a
    // session over its budget ends the session failed with reason `budget`,
    // once, and is committed without reaching the handler.
    const agent = this.options.agent
    if (agent !== undefined) {
      const session = message.provenance.conversationId
      const admitted = await this.budget.admit(
        this.options.sessions,
        session.toString(),
        received.partitionId,
        received.currentOffset ?? received.offset,
        () => this.options.sessions.open(session).asAgent(agent).withControl(this.options.control)
      )
      if (!admitted) return
    }
    const fence = message.provenance.fenceToken
    if (
      fence !== undefined &&
      !acceptFence(
        this.highWaterFence,
        this.fenceSweep,
        message.provenance.conversationId.toString(),
        fence,
        this.options.clock.nowMicros()
      )
    ) {
      return
    }
    const key = dedupKey(message.provenance)
    if (key !== undefined && !(await this.options.deduplicator.observe(key))) return
    if (
      message.provenance.deadlineMicros !== undefined &&
      this.options.clock.nowMicros() > message.provenance.deadlineMicros
    ) {
      await this.deadLetter(message, "DeadlineExceeded", 0, "message past its deadline")
      return
    }
    if (this.cancelled()) return
    await this.ackOnPickup(message)
    // Picking up the first command of a submitted session marks the session
    // working and keeps it listed in this process's heartbeat while the
    // handler runs.
    const pickup = await this.pickUpSubmitted(message)
    try {
      await this.handle(message)
    } finally {
      pickup?.release()
    }
  }

  private async handle(message: AgentMessage): Promise<void> {
    const context = AgentCtx.create(this.laser, message, {
      ...(this.options.agent !== undefined ? { agent: this.options.agent } : {}),
      ...(this.options.respondOn !== undefined ? { respondOn: this.options.respondOn } : {}),
      ...(this.options.signingKey !== undefined ? { signingKey: this.options.signingKey } : {}),
      inboxRoute: this.options.inboxRoute,
      requestAt: this.position(message.id),
      sessions: this.options.sessions,
      control: this.options.control
    })
    for (const middleware of this.options.middleware) {
      if (this.cancelled()) return
      try {
        await middleware.beforeHandle?.(message)
      } catch (error) {
        const rejected = handlerError(error)
        await this.deadLetter(message, "Rejected", 0, rejected.message)
        return
      }
    }
    for (let attempt = 0; ; attempt += 1) {
      if (this.cancelled()) return
      let result: HandlerResult
      try {
        await this.handler.handle(message, context)
        result = { kind: "ok" }
      } catch (error) {
        result = { kind: "error", error: handlerError(error) }
      }
      if (this.cancelled()) return
      for (const middleware of this.options.middleware) {
        if (this.cancelled()) return
        try {
          await middleware.afterHandle?.(message, result, attempt + 1)
        } catch {
          // Middleware observation cannot change the handler result.
        }
      }
      if (result.kind === "ok") return
      if (!isRetryable(result.error)) {
        await this.deadLetter(message, "Rejected", attempt + 1, result.error.message)
        return
      }
      if (attempt + 1 >= this.options.retry.maxAttempts) {
        await this.deadLetter(message, "RetryExhausted", attempt + 1, result.error.message)
        return
      }
      await sleep(retryDelayMs(this.options.retry, attempt), this.options.hardSignal)
    }
  }

  private async pickUpSubmitted(message: AgentMessage): Promise<SessionLease | undefined> {
    const envelope = message.envelope
    const agent = this.options.agent
    const submitted = envelope?.metadata?.get(METADATA_SUBMITTED)
    if (
      envelope?.kind !== AgentKind.Command ||
      agent === undefined ||
      submitted?.kind !== "bool" ||
      !submitted.value
    ) {
      return undefined
    }
    try {
      return await this.options.sessions
        .open(message.provenance.conversationId)
        .asAgent(agent)
        .pickUp()
    } catch {
      // A failed pickup mark never blocks the handler.
      return undefined
    }
  }

  private async ackOnPickup(message: AgentMessage): Promise<void> {
    const envelope = message.envelope
    if (
      !this.options.ackOnPickup ||
      this.options.agent === undefined ||
      this.options.respondOn === undefined ||
      envelope?.kind !== AgentKind.Command ||
      envelope.correlation === undefined
    ) {
      return
    }
    try {
      let acknowledgment = this.laser
        .agdx(this.options.respondOn, this.options.agent, message.provenance.conversationId)
        .status(OPERATION_TASK)
        .withCorrelation(envelope.correlation)
        .withTaskState(taskStateFromCode(TaskStateName.Working))
      if (this.options.signingKey !== undefined) {
        acknowledgment = acknowledgment.signedBy(this.options.signingKey)
      }
      await acknowledgment.send()
    } catch {
      // Pickup acknowledgement is advisory.
    }
  }

  private position(message: MessageId): LogPosition {
    return {
      streamId: this.streamId,
      topicId: this.topicId,
      partitionId: message.partitionId,
      offset: message.offset
    }
  }

  private async deadLetter(
    message: AgentMessage,
    reason: keyof typeof DeadLetterReasonName,
    attempts: number,
    detail: string
  ): Promise<void> {
    const { deadlineMicros, ...provenance } = message.provenance
    await this.publishDeadLetter(
      {
        ...provenance,
        causalParent: message.id
      },
      {
        source: this.position(message.id),
        reason: { kind: "known", name: reason },
        attempts,
        detail,
        payload: message.payload
      },
      message,
      message.provenance.conversationId
    )
  }

  // The capsule keeps the record's own conversation when its header still
  // reads, so the dead letter stays on its session's timeline. A record
  // without one gets a conversation derived from its log position, stable
  // across redeliveries.
  private async deadLetterUndecodable(
    received: ReceivedAgentMessage,
    payload: Uint8Array
  ): Promise<void> {
    const id = { partitionId: received.partitionId, offset: received.offset }
    const source = this.position(id)
    const conversation = headersMalformed(received)
      ? undefined
      : originalConversation(received.headers)
    await this.publishDeadLetter(
      {
        conversationId:
          conversation ??
          ConversationId.derive(
            [
              "dead-letter",
              String(source.streamId),
              String(source.topicId),
              String(source.partitionId),
              source.offset.toString()
            ].join(DEDUP_SCOPE_SEP)
          ),
        causalParent: id
      },
      {
        source,
        reason: { kind: "known", name: "DecodeFailed" },
        attempts: 0,
        payload
      },
      undefined,
      conversation
    )
  }

  private async publishDeadLetter(
    provenance: Provenance,
    capsule: AgentDeadLetter,
    message: AgentMessage | undefined,
    session: ConversationId | undefined
  ): Promise<void> {
    let publishError: LaserError | undefined
    try {
      await this.laser.sendAgent(
        AgentTopic.Dlq,
        encodeNamed(encodeAgentDeadLetter(capsule)),
        provenance,
        { contentType: "cbor" }
      )
    } catch (error) {
      publishError = handlerError(error)
    }
    try {
      await this.options.onDeadLetter?.onDeadLetter(message, capsule, publishError)
    } catch {
      // Dead-letter sinks observe the terminal delivery decision.
    }
    if (publishError !== undefined) throw publishError
    if (session !== undefined) await this.failSessionOnDeadLetter(session, capsule)
  }

  // Under `SessionConfig.failOnDeadLetter`, a dead-lettered record fails its
  // session. Best effort: the dead letter is already published, so a failed
  // status write leaves the record to commit.
  private async failSessionOnDeadLetter(
    session: ConversationId,
    capsule: AgentDeadLetter
  ): Promise<void> {
    const agent = this.options.agent
    if (!this.options.sessions.config.failsOnDeadLetter || agent === undefined) return
    const reason =
      capsule.reason.kind === "known" ? capsule.reason.name : String(capsule.reason.code)
    const { source } = capsule
    try {
      await this.options.sessions
        .open(session)
        .asAgent(agent)
        .fail({
          code: { kind: "known", name: "Internal" },
          message:
            capsule.detail === undefined
              ? `a record of this session was dead-lettered (${reason})`
              : `a record of this session was dead-lettered (${reason}): ${capsule.detail}`,
          retryable: false,
          detail: new Map([
            [
              "dead_letter_reason",
              { kind: "int", value: BigInt(deadLetterReasonCode(capsule.reason)) }
            ],
            [
              "source",
              {
                kind: "str",
                value: `${String(source.streamId)}/${String(source.topicId)}/${String(source.partitionId)}/${source.offset.toString()}`
              }
            ]
          ])
        })
    } catch {
      // The dead letter is published, so the record still commits.
    }
  }
}

// The conversation a record's header names, in either encoding the wire
// allows, when the rest of the record does not decode.
function originalConversation(
  headers: ReadonlyMap<string, HeaderValue>
): ConversationId | undefined {
  const value = headers.get(CONVERSATION_ID)
  try {
    if (value?.kind === "string") return ConversationId.parse(value.value)
    if (value?.kind === "uint128") {
      let id = 0n
      for (let index = value.value.byteLength - 1; index >= 0; index -= 1) {
        id = (id << 8n) | BigInt(value.value[index] ?? 0)
      }
      return ConversationId.parse(crockfordEncode(id))
    }
  } catch {
    // An unreadable header leaves the record without a conversation.
  }
  return undefined
}

export class ReliableConsumer {
  private readonly options: ReliableConsumerOptions

  constructor(options: ReliableConsumerOptions) {
    if (
      !Number.isSafeInteger(options.dedupWindow ?? 10_000) ||
      (options.dedupWindow ?? 10_000) < 1
    ) {
      throw new InvalidError("dedupWindow must be a positive safe integer")
    }
    for (const [name, value] of [
      ["maxQueuedRecords", options.maxQueuedRecords ?? 4_096],
      ["maxQueuedBytes", options.maxQueuedBytes ?? 64 * 1024 * 1024]
    ] as const) {
      if (!Number.isSafeInteger(value) || value < 1) {
        throw new InvalidError(`${name} must be a positive safe integer`)
      }
    }
    const grace = options.shutdownGraceMs ?? 30_000
    if (!Number.isFinite(grace) || grace < 0)
      throw new InvalidError("shutdownGraceMs must be a non-negative finite number")
    this.options = options
  }

  /**
   * Consume until shutdown, dispatching each message to `handler`. Delivery
   * runs on the group consumer, so a server that resolves group policies
   * reads through the group-aware engine. On `agent.sessions` and
   * `agent.control`, when the server serves filtered reads and the filter
   * catalog, the group is bound to the addressee filter
   * `agdx.to In [<agent>, "*"]` before it reads. A group bound to another
   * filter is refused. On Apache Iggy the group stays unbound and records are
   * classified on the client. A capability probe that established nothing is
   * an error.
   */
  async run(
    laser: Laser,
    handler: AgentHandler,
    control: ReliableConsumerControl = {}
  ): Promise<void> {
    const stream = laser.defaultStream
    if (stream === undefined) throw new NoStreamError("ReliableConsumer.run() requires a stream")
    const pollIntervalMs = this.options.pollIntervalMs ?? 10
    const deduplicator =
      this.options.deduplicator ?? new SlidingWindow(this.options.dedupWindow ?? 10_000)
    const engine = await resolveEngine(laser)
    const group = this.options.group.asStr()
    const me = this.options.agent?.wireId()
    if (me !== undefined) await bindAddressee(laser, engine, this.options.topic, group, me)
    const opener = new Opener(laser, this.options.topic, group, pollIntervalMs, engine.native)
    const consumer = await opener.open()
    let follower: ControlFollower | undefined
    let started = false
    try {
      if (this.options.warmDedup === true) {
        await this.warmDedup(laser, deduplicator, this.options.dedupWindow ?? 10_000)
      }
      const ids = await laserTransportIds(laser, stream, this.options.topic)
      const sessions = laser.sessions(this.options.sessions)
      // The control subscription: a bounded read of `agent.control` loads the
      // requests already on the log, and the follower reads every partition
      // from where it ended, so each instance of the role sees every pause,
      // resume, and cancel request whatever partitions its group assigns it.
      // Handlers read the requests through their session lens. An agent with
      // an id also runs the pause runtime. A stream without the topic has no
      // operator control.
      const book = new ControlBook()
      const requests = new PauseRequests()
      let pause: PauseRuntime | undefined
      const feed =
        this.options.topic === AGENT_CONTROL ? undefined : await loadControl(laser, me, book)
      if (feed !== undefined) {
        const source = await sourceTopic(laser, stream, this.options.topic)
        const agent = this.options.agent
        if (agent !== undefined && source !== undefined) {
          pause = new PauseRuntime(
            laser,
            sessions,
            agent,
            book,
            [feed.streamId, feed.topicId],
            feed.truncated,
            source,
            (session) => {
              requests.request(session)
            }
          )
        }
        follower = new ControlFollower(
          laserTransport(laser),
          stream,
          feed,
          me,
          book,
          pause === undefined
            ? undefined
            : (session) => {
                requests.request(session)
              },
          pollIntervalMs
        )
      }
      started = true
      await this.consume(laser, handler, control, opener, consumer, deduplicator, ids, {
        sessions,
        book,
        requests,
        ...(pause !== undefined ? { pause } : {})
      })
    } catch (error) {
      if (!started) {
        try {
          await consumer.shutdown()
        } catch {
          // Preserve the setup failure.
        }
      }
      throw error
    } finally {
      if (control.hardAborted?.() === true || control.hardSignal?.aborted === true) {
        follower?.abort()
      } else {
        await follower?.stop()
      }
    }
  }

  private async consume(
    laser: Laser,
    handler: AgentHandler,
    control: ReliableConsumerControl,
    opener: Opener,
    first: Consumer,
    deduplicator: Deduplicator,
    ids: { readonly streamId: number; readonly topicId: number },
    sessionRuntime: {
      readonly sessions: Sessions
      readonly book: ControlBook
      readonly requests: PauseRequests
      readonly pause?: PauseRuntime
    }
  ): Promise<void> {
    let consumer = first
    const pollIntervalMs = opener.pollIntervalMs
    const { sessions, book, requests, pause } = sessionRuntime
    const shutdown = shutdownControl(control, this.options.shutdownGraceMs ?? 30_000)
    const runtime = shutdown.runtime
    const worker = new ReliableWorker(
      laser,
      handler,
      {
        retry: this.options.retry ?? DEFAULT_RETRY_POLICY,
        understoodFeatures: this.options.understoodFeatures ?? 0n,
        clock: this.options.clock ?? new SystemClock(),
        hardSignal: runtime.hardSignal,
        inboxRoute: this.options.inboxRoute ?? ADVERTISED_INBOX_ROUTE,
        middleware: this.options.middleware ?? [],
        ackOnPickup: this.options.ackOnPickup ?? false,
        deduplicator,
        topic: this.options.topic,
        sessions,
        control: book,
        ...(pause !== undefined ? { pause } : {}),
        ...(this.options.operations !== undefined ? { operations: this.options.operations } : {}),
        ...(this.options.agent !== undefined ? { agent: this.options.agent } : {}),
        ...(this.options.respondOn !== undefined ? { respondOn: this.options.respondOn } : {}),
        ...(this.options.onDeadLetter !== undefined
          ? { onDeadLetter: this.options.onDeadLetter }
          : {}),
        ...(this.options.verifier !== undefined ? { verifier: this.options.verifier } : {}),
        ...(this.options.signingKey !== undefined ? { signingKey: this.options.signingKey } : {})
      },
      ids.streamId,
      ids.topicId
    )
    let driver: PauseDriver | undefined
    try {
      // Rebuild the pause state from the log before the first dispatch, then
      // let the pause driver bring each session a request names up to date.
      if (pause !== undefined) {
        await pause.recover()
        driver = new PauseDriver(pause, worker)
        requests.attach(driver)
      }
      control.ready?.()
      for (;;) {
        try {
          const concurrency = this.options.concurrency ?? SERIAL_CONCURRENCY
          const running =
            concurrency.kind === "serial"
              ? this.runSerial(consumer, worker, runtime, pollIntervalMs)
              : this.runPerPartition(
                  consumer,
                  worker,
                  {
                    maxPartitions: Math.max(1, concurrency.maxPartitions),
                    maxQueuedRecords: Math.max(1, this.options.maxQueuedRecords ?? 4_096),
                    maxQueuedBytes: Math.max(1, this.options.maxQueuedBytes ?? 64 * 1024 * 1024),
                    probeIntervalMs: sessions.config.heartbeatValue
                  },
                  runtime,
                  pollIntervalMs
                )
          await consumeUntilDone(running, runtime.hardSignal)
          if (shutdown.expired()) throw new TimeoutError("agent shutdown drain")
          return
        } catch (error) {
          const failure = handlerError(error)
          if (runtime.signal.aborted || runtime.hardAborted() || !isRetryable(failure))
            throw failure
          try {
            await consumer.shutdown()
          } catch {
            // Reconnection continues when the failed consumer cannot leave cleanly.
          }
          await sleep(pollIntervalMs, runtime.signal)
          if (shutdown.stopped()) return
          consumer = await opener.open()
          worker.invalidatePause()
        }
      }
    } finally {
      shutdown.dispose()
      if (!runtime.hardAborted()) await driver?.stop()
      if (runtime.hardAborted()) {
        // The record in flight stays uncommitted. The consumer leaves in the
        // background, so a group-aware member hands its partitions over
        // without the caller waiting for work it abandoned.
        void consumer.shutdown().catch(() => undefined)
      } else {
        try {
          await consumer.shutdown()
        } catch {
          // Preserve the primary consumer failure.
        }
      }
    }
  }

  private async runSerial(
    consumer: Consumer,
    worker: ReliableWorker,
    control: ReliableConsumerControl,
    pollIntervalMs: number
  ): Promise<void> {
    while (control.signal?.aborted !== true) {
      const message = await nextOrIdle(consumer, pollIntervalMs, control.signal)
      if (message === undefined) continue
      worker.observeAssignment(consumer)
      if (!(await consumeUntilDone(worker.consume(received(message)), control.hardSignal))) return
      if (control.hardAborted?.() !== true) await consumer.commit(message)
    }
  }

  // One lane per partition, each a serial chain of records committed once
  // handled. A lane whose dead-letter publish fails stops its chain, so its
  // queued successors are never committed. The assignment probe closes the
  // lane of a partition this member no longer reads, and a later lane of the
  // same partition waits for it, so a partition is never handled by two lanes
  // at once.
  private async runPerPartition(
    consumer: Consumer,
    worker: ReliableWorker,
    limits: LaneLimits,
    control: ReliableConsumerControl,
    pollIntervalMs: number
  ): Promise<void> {
    const lanes = new Map<number, Promise<void>>()
    const retiring = new Map<number, Promise<void>>()
    const scheduled = new Set<string>()
    const pending = (): Promise<void>[] => [...lanes.values(), ...retiring.values()]
    let queuedRecords = 0
    let queuedBytes = 0
    let failure: LaserError | undefined
    let probedAt = Date.now()
    const currentFailure = (): LaserError | undefined => failure
    try {
      while (control.signal?.aborted !== true && failure === undefined) {
        if (Date.now() - probedAt >= limits.probeIntervalMs) {
          probedAt = Date.now()
          dropRevoked(consumer, lanes, retiring)
        }
        const message = await nextOrIdle(consumer, pollIntervalMs, control.signal)
        if (message === undefined) continue
        worker.observeAssignment(consumer)
        const position = `${String(message.partitionId)}:${message.position.offset.toString()}`
        if (scheduled.has(position)) continue
        const messageBytes = message.payload.byteLength + headerBytes(message.headers)
        while (
          pending().length > 0 &&
          (queuedRecords >= limits.maxQueuedRecords ||
            queuedBytes + messageBytes > limits.maxQueuedBytes)
        ) {
          await Promise.race(pending())
        }
        let existing = lanes.get(message.partitionId)
        while (
          existing === undefined &&
          lanes.size >= limits.maxPartitions &&
          currentFailure() === undefined
        ) {
          await Promise.race(lanes.values())
          existing = lanes.get(message.partitionId)
        }
        if (currentFailure() !== undefined) break
        const predecessor = existing ?? retiring.get(message.partitionId) ?? Promise.resolve()
        retiring.delete(message.partitionId)
        scheduled.add(position)
        queuedRecords += 1
        queuedBytes += messageBytes
        const lane: Promise<void> = predecessor
          .then(async () => {
            if (currentFailure() !== undefined || control.hardAborted?.() === true) return
            await worker.consume(received(message))
            if (control.hardAborted?.() !== true) await consumer[INTERNAL_COMMIT_HANDLED](message)
          })
          .catch((error: unknown) => {
            failure ??= handlerError(error)
          })
          .finally(() => {
            scheduled.delete(position)
            queuedRecords -= 1
            queuedBytes -= messageBytes
            if (lanes.get(message.partitionId) === lane) lanes.delete(message.partitionId)
            if (retiring.get(message.partitionId) === lane) retiring.delete(message.partitionId)
          })
        lanes.set(message.partitionId, lane)
      }
    } finally {
      if (control.hardSignal?.aborted !== true) await Promise.all(pending())
    }
    if (failure !== undefined) throw failure
  }

  private async warmDedup(laser: Laser, deduplicator: Deduplicator, depth: number): Promise<void> {
    const transport = laserTransport(laser)
    if (transport.getConsumerOffset === undefined) {
      throw new InvalidError("warm dedup requires consumer-offset reads")
    }
    const stream = laser.defaultStream
    if (stream === undefined) throw new NoStreamError("warm dedup requires a stream")
    const partitions = await transport.getTopicPartitionCount(stream, this.options.topic)
    for (let partitionId = 0; partitionId < partitions; partitionId += 1) {
      const offset = await transport.getConsumerOffset(
        stream,
        this.options.topic,
        { kind: "group", name: this.options.group.asStr() },
        partitionId
      )
      if (offset === undefined) continue
      const span = BigInt(Math.max(depth - 1, 0))
      const start = offset.storedOffset > span ? offset.storedOffset - span : 0n
      const messages = await transport.pollMessages(
        stream,
        this.options.topic,
        { kind: "single", partitionId },
        { kind: "offset", value: start },
        Number(offset.storedOffset - start + 1n),
        false
      )
      for (const received of messages) {
        if (received.offset > offset.storedOffset) continue
        const decoded = decodeAgentMessage(received)
        if (decoded.kind !== "message") continue
        const key = dedupKey(decoded.message.provenance)
        if (key !== undefined) await deduplicator.observe(key)
      }
    }
  }
}

// The per-partition scheduler's bounds.
interface LaneLimits {
  readonly maxPartitions: number
  readonly maxQueuedRecords: number
  readonly maxQueuedBytes: number
  // How often the assignment probe drops lanes of partitions this member no
  // longer reads.
  readonly probeIntervalMs: number
}

// The assignment probe. The lane of a partition this member no longer reads
// retires: it finishes the records it holds and no longer counts against the
// lane cap.
function dropRevoked(
  consumer: Consumer,
  lanes: Map<number, Promise<void>>,
  retiring: Map<number, Promise<void>>
): void {
  const assigned = consumer[INTERNAL_ASSIGNED_PARTITIONS]()
  if (assigned === undefined) return
  for (const [partition, lane] of lanes) {
    if (assigned.has(partition)) continue
    lanes.delete(partition)
    retiring.set(partition, lane)
  }
}

// How the runtime reads its groups, decided once at start from the server's
// capabilities.
interface DeliveryEngine {
  // Read groups natively. Only when no group policy can apply.
  readonly native: boolean
  // The server serves filtered reads and the catalog that binds a group to
  // its filter.
  readonly filters: boolean
}

async function resolveEngine(laser: Laser): Promise<DeliveryEngine> {
  let capabilities = await laser.capabilities()
  if (capabilities.hello === "unknown") capabilities = await laser.refreshCapabilities()
  // An uncertain probe, or a server that serves filters without group-aware
  // reads, is an error here, never a native fallback.
  if (!policyAware(capabilities)) return { native: true, filters: false }
  // The group-aware engine opens its own connections, which a client brought
  // by the caller cannot. Such a runtime reads natively and binds no filter:
  // delivery stays correct because the runtime still classifies every record
  // by its addressee, it only examines more.
  if (!laser[INTERNAL_DIALED]()) {
    if (capabilities.managed) {
      warn(
        "the agent reads natively because its client was not built from a connection string, so no group filter is bound"
      )
    }
    return { native: true, filters: false }
  }
  return {
    native: false,
    filters: capabilities.filters.native && capabilities.filters.catalog
  }
}

// Bind the role group of `topic` to the addressee filter. Only the session
// topics carry `agdx.to` on every record, so a filter on any other topic
// would drop untargeted records.
async function bindAddressee(
  laser: Laser,
  engine: DeliveryEngine,
  topic: string,
  group: string,
  me: WireAgentId
): Promise<void> {
  if (!engine.filters || (topic !== AGENT_SESSIONS && topic !== AGENT_CONTROL)) return
  await laser
    .topic(topic)
    .consumerGroup(group)
    .create({ filter: addresseeFilter(me) })
}

// Opens the runtime's consumer of one topic, again after a recoverable failure.
class Opener {
  constructor(
    private readonly laser: Laser,
    readonly topic: string,
    private readonly group: string,
    readonly pollIntervalMs: number,
    private readonly native: boolean
  ) {}

  open(): Promise<Consumer> {
    const group = this.laser.topic(this.topic).consumerGroup(this.group)
    const options = {
      commitPolicy: { kind: "disabled" },
      pollIntervalMs: this.pollIntervalMs
    } as const
    return this.native
      ? group[INTERNAL_NATIVE_CONSUMER](options, "propagate")
      : group.consumer({ ...options, createGroup: true })
  }
}

// The source topic of a runtime's work records, with the creation time that
// proves its numeric id. `undefined` when the ids do not resolve.
async function sourceTopic(
  laser: Laser,
  stream: string,
  topic: string
): Promise<SourceTopic | undefined> {
  const transport = laserTransport(laser)
  const ids = await transport.resolveStreamTopicIds?.(stream, topic)
  const details = await transport.findSnapshotTopic?.(stream, topic)
  if (ids === undefined || details === undefined) return undefined
  return {
    stream,
    topic,
    streamId: ids.streamId,
    topicId: ids.topicId,
    generation: details.createdAtMicros
  }
}

function headerBytes(headers: ReadonlyMap<string, HeaderValue>): number {
  let size = 0
  for (const [key, value] of headers) {
    size += TEXT_ENCODER.encode(key).byteLength
    if (value.kind === "raw" || value.kind === "int128" || value.kind === "uint128") {
      size += value.value.byteLength
    } else if (value.kind === "string") size += TEXT_ENCODER.encode(value.value).byteLength
    else size += 16
  }
  return size
}

function laserTransport(laser: Laser): LaserTransport {
  return laser[INTERNAL_TRANSPORT]()
}

async function laserTransportIds(
  laser: Laser,
  stream: string,
  topic: string
): Promise<{ readonly streamId: number; readonly topicId: number }> {
  return (
    (await laserTransport(laser).resolveStreamTopicIds?.(stream, topic)) ?? {
      streamId: 0,
      topicId: 0
    }
  )
}

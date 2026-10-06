import { code } from "../client/error-classify.js"
import {
  CancelledError,
  CodecError,
  ConsumerGroupSetupError,
  FilterExecutionError,
  FilterStopError,
  HandlerError,
  InvalidError,
  LaserError,
  NoStreamError,
  TimeoutError,
  TransportError,
  publishCause
} from "../client/errors.js"
import { INTERNAL_NATIVE_CONSUMER, INTERNAL_TRANSPORT } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import type { IggyHeaderValue, LaserTransport } from "../iggy/apache-iggy.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { decodeProvenanceHeaders, type Provenance } from "../provenance/provenance.js"
import { SystemClock, type Clock } from "../runtime/clock.js"
import type { KeyRegistry, SigningKey } from "../signing.js"
import { AgentId, ConversationId, type ConsumerGroupName, type MessageId } from "../types/ids.js"
import {
  AgentKind,
  OPERATION_TASK,
  TaskStateName,
  decodeAgentEnvelope,
  encodeAgentDeadLetter,
  taskStateFromCode,
  unmetRequirements,
  validateAgentEnvelope,
  type AgentDeadLetter,
  type AgentEnvelope,
  type DeadLetterReasonName,
  type SignatureContext
} from "../wire/agent.js"
import { AGENT_OP_VERSION } from "../wire/codes.js"
import { resultCodeIsRetryable } from "../wire/result.js"
import { decodeOne, encodeNamed, expectMap } from "../wire/cbor.js"
import { type ContentType, contentTypeFromCode } from "../wire/content.js"
import { AGENT_VERSION, CONTENT_TYPE, FENCE } from "../wire/headers.js"
import type { LogPosition } from "../wire/ids.js"
import type { Consumer, ConsumerMessage } from "../stream/consumer.js"
import { AgentContext } from "./context.js"
import { ADVERTISED_INBOX_ROUTE, type InboxRoute } from "./router.js"

const DEDUP_SCOPE_SEP = "\u001f"
const TEXT_ENCODER = new TextEncoder()

const FENCE_MAP_SOFT_CAP = 16_384

const FENCE_ENTRY_TTL_MICROS = 600_000_000n

const FENCE_SWEEP_INTERVAL_MICROS = 1_000_000n

function tryAgentId(value: string): AgentId | undefined {
  try {
    return AgentId.new(value)
  } catch {
    return undefined
  }
}

function fenceFromMetadata(
  metadata: ReadonlyMap<string, { readonly kind: string; readonly value?: unknown }> | undefined
): bigint | undefined {
  const entry = metadata?.get(FENCE)
  if (entry?.kind !== "int") return undefined
  const value = entry.value
  return typeof value === "bigint" && value >= 0n ? value : undefined
}

export function provenanceFromEnvelope(envelope: AgentEnvelope): Provenance {
  const agent = tryAgentId(envelope.source)
  const targetAgentId = envelope.target !== undefined ? tryAgentId(envelope.target) : undefined
  const fenceToken = fenceFromMetadata(envelope.metadata)
  return {
    conversationId: ConversationId.parse(envelope.conversation.toString()),
    ...(agent !== undefined ? { agent } : {}),
    ...(targetAgentId !== undefined ? { targetAgentId } : {}),
    ...(envelope.idempotencyKey !== undefined ? { idempotencyKey: envelope.idempotencyKey } : {}),
    ...(envelope.correlation !== undefined
      ? { correlationId: envelope.correlation.toString() }
      : {}),
    ...(envelope.deadlineMicros !== undefined ? { deadlineMicros: envelope.deadlineMicros } : {}),
    ...(fenceToken !== undefined ? { fenceToken } : {})
  }
}

export interface ReceivedAgentMessage {
  readonly payload: Uint8Array
  readonly partitionId: number
  readonly offset: bigint
  readonly timestampMicros?: bigint
  readonly headers: ReadonlyMap<string, IggyHeaderValue>
}

export interface ProvenanceAndEnvelope {
  readonly provenance: Provenance
  readonly envelope?: AgentEnvelope
  readonly signatureContext?: SignatureContext
}

export function provenanceAndEnvelope(
  message: ReceivedAgentMessage,
  understoodFeatures = 0n
): ProvenanceAndEnvelope {
  const version = message.headers.get(AGENT_VERSION)
  if (version !== undefined) {
    if (version.kind !== "uint32" || version.value !== AGENT_OP_VERSION) {
      throw new CodecError("unsupported agent envelope version", "agent", AGENT_VERSION)
    }
    const context = "agent envelope"
    const envelope = decodeAgentEnvelope(
      expectMap(decodeOne(message.payload, context), context),
      context
    )
    validateAgentEnvelope(envelope)
    const unmet = unmetRequirements(envelope, understoodFeatures)
    if (unmet !== 0n) {
      throw new CodecError(
        `agent envelope requires unsupported features 0x${unmet.toString(16).padStart(16, "0")}`,
        "agent",
        "must_understand"
      )
    }
    const contentType = message.headers.get(CONTENT_TYPE)
    if (contentType !== undefined && contentType.kind !== "uint8") {
      throw new CodecError("invalid content-type header", "agent", CONTENT_TYPE)
    }
    return {
      provenance: provenanceFromEnvelope(envelope),
      envelope,
      signatureContext: {
        ...(contentType?.kind === "uint8" ? { contentType: contentType.value } : {}),
        agentVersion: version.value
      }
    }
  }
  return { provenance: decodeProvenanceHeaders(message.headers) }
}

export function contentTypeOf(message: ReceivedAgentMessage): ContentType | undefined {
  const header = message.headers.get(CONTENT_TYPE)
  return header?.kind === "uint8" ? contentTypeFromCode(header.value) : undefined
}

export interface AgentMessage {
  readonly provenance: Provenance
  readonly payload: Uint8Array
  readonly id: MessageId
  readonly envelope?: AgentEnvelope
  readonly contentType?: ContentType
  readonly verifiedPrincipal?: string
}

export function agentMessageBody(message: AgentMessage): Uint8Array {
  return message.envelope !== undefined ? message.envelope.body : message.payload
}

export type DecodedAgentMessage =
  | {
      readonly kind: "message"
      readonly message: AgentMessage
      readonly signatureContext?: SignatureContext
      readonly observedAtMicros?: bigint
    }
  | { readonly kind: "error"; readonly error: CodecError; readonly payload: Uint8Array }

export function decodeAgentMessage(
  received: ReceivedAgentMessage,
  understoodFeatures = 0n
): DecodedAgentMessage {
  try {
    const { provenance, envelope, signatureContext } = provenanceAndEnvelope(
      received,
      understoodFeatures
    )
    const contentType = contentTypeOf(received)
    return {
      kind: "message",
      message: {
        provenance,
        payload: received.payload,
        id: { partitionId: received.partitionId, offset: received.offset },
        ...(envelope !== undefined ? { envelope } : {}),
        ...(contentType !== undefined ? { contentType } : {})
      },
      ...(signatureContext !== undefined ? { signatureContext } : {}),
      ...(received.timestampMicros !== undefined
        ? { observedAtMicros: received.timestampMicros }
        : {})
    }
  } catch (cause) {
    return {
      kind: "error",
      error: new CodecError("failed to decode agent message", "agent", "decode", { cause }),
      payload: received.payload
    }
  }
}

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
    ? `${provenance.agent.asString()}${DEDUP_SCOPE_SEP}${provenance.idempotencyKey}`
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
  handle(message: AgentMessage, context: AgentContext): Promise<void>
}

export type HandlerResult =
  { readonly kind: "ok" } | { readonly kind: "error"; readonly error: LaserError }

export interface AgentMiddleware {
  beforeHandle?(message: AgentMessage): Promise<void>
  afterHandle?(message: AgentMessage, result: HandlerResult, attempt: number): Promise<void>
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
  readonly deadLetterSink?: DeadLetterSink
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
    case "agent-workflow":
    case "checkpoint":
      return resultCodeIsRetryable(code(cause))
    case "filter":
      if (cause instanceof ConsumerGroupSetupError) {
        return cause.cause instanceof LaserError && isRetryable(cause.cause)
      }
      if (cause instanceof FilterStopError) return false
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
        "agent" | "deadLetterSink" | "respondOn" | "signingKey" | "verifier"
      > & { readonly hardSignal: AbortSignal },
    private readonly streamId: number,
    private readonly topicId: number
  ) {}

  private cancelled(): boolean {
    return this.options.hardSignal.aborted
  }

  async consume(received: ReceivedAgentMessage): Promise<void> {
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
    const context = new AgentContext(this.laser, message, {
      ...(this.options.agent !== undefined ? { agent: this.options.agent } : {}),
      ...(this.options.respondOn !== undefined ? { respondOn: this.options.respondOn } : {}),
      ...(this.options.signingKey !== undefined ? { signingKey: this.options.signingKey } : {}),
      inboxRoute: this.options.inboxRoute
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
      message
    )
  }

  private async deadLetterUndecodable(
    received: ReceivedAgentMessage,
    payload: Uint8Array
  ): Promise<void> {
    const id = { partitionId: received.partitionId, offset: received.offset }
    await this.publishDeadLetter(
      { conversationId: ConversationId.new(), causalParent: id },
      {
        source: this.position(id),
        reason: { kind: "known", name: "DecodeFailed" },
        attempts: 0,
        payload
      },
      undefined
    )
  }

  private async publishDeadLetter(
    provenance: Provenance,
    capsule: AgentDeadLetter,
    message: AgentMessage | undefined
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
      await this.options.deadLetterSink?.onDeadLetter(message, capsule, publishError)
    } catch {
      // Dead-letter sinks observe the terminal delivery decision.
    }
    if (publishError !== undefined) throw publishError
  }
}

export class ReliableConsumer {
  readonly options: ReliableConsumerOptions

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
    // The runtime owns its delivery contract over the native group consumer.
    const group = laser.topic(this.options.topic).consumerGroup(this.options.group.asString())
    const openConsumer = (): Promise<Consumer> =>
      group[INTERNAL_NATIVE_CONSUMER]({ autoCommit: false, pollIntervalMs }, "propagate")
    let consumer = await openConsumer()
    if (this.options.warmDedup === true) {
      await this.warmDedup(laser, deduplicator, this.options.dedupWindow ?? 10_000)
    }
    const ids = await laserTransportIds(laser, stream, this.options.topic)
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
        ...(this.options.agent !== undefined ? { agent: this.options.agent } : {}),
        ...(this.options.respondOn !== undefined ? { respondOn: this.options.respondOn } : {}),
        ...(this.options.deadLetterSink !== undefined
          ? { deadLetterSink: this.options.deadLetterSink }
          : {}),
        ...(this.options.verifier !== undefined ? { verifier: this.options.verifier } : {}),
        ...(this.options.signingKey !== undefined ? { signingKey: this.options.signingKey } : {})
      },
      ids.streamId,
      ids.topicId
    )
    try {
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
                  concurrency.maxPartitions,
                  this.options.maxQueuedRecords ?? 4_096,
                  this.options.maxQueuedBytes ?? 64 * 1024 * 1024,
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
          consumer = await openConsumer()
        }
      }
    } finally {
      shutdown.dispose()
      if (!runtime.hardAborted()) {
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
      if (!(await consumeUntilDone(worker.consume(message), control.hardSignal))) return
      if (control.hardAborted?.() !== true) await consumer.commit(message)
    }
  }

  private async runPerPartition(
    consumer: Consumer,
    worker: ReliableWorker,
    maxPartitions: number,
    maxQueuedRecords: number,
    maxQueuedBytes: number,
    control: ReliableConsumerControl,
    pollIntervalMs: number
  ): Promise<void> {
    const limit = Math.max(1, maxPartitions)
    const lanes = new Map<number, Promise<void>>()
    const scheduled = new Set<string>()
    let queuedRecords = 0
    let queuedBytes = 0
    let failure: LaserError | undefined
    const currentFailure = (): LaserError | undefined => failure
    while (control.signal?.aborted !== true && failure === undefined) {
      const message = await nextOrIdle(consumer, pollIntervalMs, control.signal)
      if (message === undefined) continue
      const position = `${String(message.partitionId)}:${message.offset.toString()}`
      if (scheduled.has(position)) continue
      const messageBytes = message.payload.byteLength + headerBytes(message.headers)
      while (
        lanes.size > 0 &&
        (queuedRecords >= Math.max(1, maxQueuedRecords) ||
          queuedBytes + messageBytes > Math.max(1, maxQueuedBytes))
      ) {
        await Promise.race(lanes.values())
      }
      let existing = lanes.get(message.partitionId)
      while (existing === undefined && lanes.size >= limit && currentFailure() === undefined) {
        await Promise.race(lanes.values())
        existing = lanes.get(message.partitionId)
      }
      if (currentFailure() !== undefined) break
      scheduled.add(position)
      queuedRecords += 1
      queuedBytes += messageBytes
      const lane = (existing ?? Promise.resolve())
        .then(async () => {
          if (currentFailure() !== undefined || control.hardAborted?.() === true) return
          await worker.consume(message)
          if (control.hardAborted?.() !== true) await consumer.commit(message)
        })
        .catch((error: unknown) => {
          failure ??= handlerError(error)
        })
        .finally(() => {
          scheduled.delete(position)
          queuedRecords -= 1
          queuedBytes -= messageBytes
          if (lanes.get(message.partitionId) === lane) lanes.delete(message.partitionId)
        })
      lanes.set(message.partitionId, lane)
    }
    if (control.hardSignal?.aborted !== true) await Promise.all(lanes.values())
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
        { kind: "group", name: this.options.group.asString() },
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

function headerBytes(headers: ReadonlyMap<string, IggyHeaderValue>): number {
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

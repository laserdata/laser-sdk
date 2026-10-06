import { HandlerConfigError, TimeoutError } from "../client/errors.js"
import type { Laser } from "../client/laser.js"
import { ConsumerGroupName, type AgentId } from "../types/ids.js"
import type { ActionGovernor, GovernorMode, GovernorRetention } from "../govern.js"
import type { KeyRegistry, SigningKey } from "../signing.js"
import type { CapabilityDescriptor } from "../wire/agent.js"
import type { Consolidator } from "../memory/types.js"
import {
  ReliableConsumer,
  type AgentHandler,
  type AgentMiddleware,
  type ConcurrencyPolicy,
  type DeadLetterSink,
  type Deduplicator,
  type RetryPolicy
} from "./reliable-consumer.js"
import type { InboxRoute } from "./router.js"
import { longTimeout } from "./timer.js"

export interface AgentDefinition {
  readonly id: AgentId
  readonly consumerGroup?: ConsumerGroupName
  readonly listenOn: string
  readonly handler: AgentHandler
  readonly respondOn?: string
  readonly inboxRoute?: InboxRoute
  readonly pollIntervalMs?: number
  readonly shutdownGraceMs: number
  readonly concurrency?: ConcurrencyPolicy
  readonly maxQueuedRecords?: number
  readonly maxQueuedBytes?: number
  readonly understoodFeatures: bigint
  readonly warmDedup: boolean
  readonly middleware: readonly AgentMiddleware[]
  readonly onDeadLetter?: DeadLetterSink
  readonly dedupWindow?: number
  readonly retry?: RetryPolicy
  readonly deduplicator?: Deduplicator
  readonly capabilities: readonly CapabilityDescriptor[]
  readonly ackOnPickup: boolean
  readonly verifier?: KeyRegistry
  readonly signingKey?: SigningKey
  readonly governor?: ActionGovernor
  readonly governorMode?: GovernorMode
  readonly governorRetention?: Partial<GovernorRetention>
  readonly consolidateEveryMs?: number
  readonly consolidator?: Consolidator
}

/**
 * One configured agent. `spawn` runs it as its own consumer group, named after
 * its id unless `consumerGroup` is set. Never put two agent ids in one group:
 * each member skips work addressed to another id, so the intended agent would
 * never see it.
 */
export class Agent {
  constructor(private readonly definition: AgentDefinition) {
    validateDefinition(definition)
  }

  static builder(): AgentBuilder {
    return new AgentBuilder()
  }

  /** Starts the agent on `laser` and returns the handle that stops it. */
  spawn(laser: Laser): AgentHandle {
    return AgentHandle.create(this.definition, laser)
  }
}

type RunOutcome = { readonly kind: "ok" } | { readonly kind: "error"; readonly error: unknown }

/** Controls one running agent and drains it on async disposal. */
export class AgentHandle implements AsyncDisposable {
  private readonly shutdownController = new AbortController()
  private readonly hardStopController = new AbortController()
  private hardStopped = false
  private readySettled = false
  private readonly readyPromise: Promise<void>
  private readonly resolveReady: () => void
  private readonly rejectReady: (error: unknown) => void
  private readonly task: Promise<RunOutcome>
  private readonly shutdownGraceMs: number
  private readonly consolidationController = new AbortController()
  private readonly consolidationTask: Promise<void> | undefined

  private constructor(definition: AgentDefinition, laser: Laser) {
    this.shutdownGraceMs = definition.shutdownGraceMs
    let resolveReady: (() => void) | undefined
    let rejectReady: ((error: unknown) => void) | undefined
    this.readyPromise = new Promise<void>((resolve, reject) => {
      resolveReady = resolve
      rejectReady = reject
    })
    this.resolveReady = resolveReady as () => void
    this.rejectReady = rejectReady as (error: unknown) => void
    void this.readyPromise.catch(() => undefined)
    this.task = this.run(definition, laser).finally(() => {
      this.consolidationController.abort("agent stopped")
    })
    this.consolidationTask =
      definition.consolidateEveryMs !== undefined && definition.consolidator !== undefined
        ? this.runConsolidation(
            definition.id,
            definition.consolidateEveryMs,
            definition.consolidator,
            this.consolidationController.signal
          )
        : undefined
  }

  /** @internal */
  static create(definition: AgentDefinition, laser: Laser): AgentHandle {
    return new AgentHandle(definition, laser)
  }

  ready(): Promise<void> {
    return this.readyPromise
  }

  /** Stops intake, drains active handlers, and joins background work. */
  async shutdown(): Promise<void> {
    this.shutdownController.abort("agent shutdown")
    this.consolidationController.abort("agent shutdown")
    let timer: { cancel(): void } | undefined
    const timeout = new Promise<"timeout">((resolve) => {
      timer = longTimeout(() => {
        resolve("timeout")
      }, this.shutdownGraceMs)
    })
    const outcome = await Promise.race([this.task, timeout])
    timer?.cancel()
    if (outcome === "timeout") {
      this.hardStopped = true
      this.hardStopController.abort("agent shutdown drain")
      throw new TimeoutError("agent shutdown drain")
    }
    if (outcome.kind === "error") throw outcome.error
    await this.consolidationTask
  }

  /** Delegates async disposal to `shutdown()`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.shutdown()
  }

  async join(): Promise<void> {
    const outcome = await this.task
    this.consolidationController.abort("agent stopped")
    await this.consolidationTask
    if (outcome.kind === "error") throw outcome.error
  }

  abort(): void {
    this.hardStopped = true
    this.hardStopController.abort("agent abort")
    this.shutdownController.abort("agent abort")
    this.consolidationController.abort("agent abort")
  }

  // Each pass covers the agent's own memory, never the whole namespace.
  private async runConsolidation(
    agent: AgentId,
    everyMs: number,
    consolidator: Consolidator,
    signal: AbortSignal
  ): Promise<void> {
    while (!signal.aborted) {
      let onAbort: (() => void) | undefined
      const stopped = new Promise<void>((resolve) => {
        onAbort = () => {
          resolve()
        }
        signal.addEventListener("abort", onAbort, { once: true })
      })
      try {
        await Promise.race([consolidator.consolidate({ agent }, signal), stopped])
      } catch {
        // Consolidation failures do not stop message handling.
      } finally {
        if (onAbort !== undefined) signal.removeEventListener("abort", onAbort)
      }
      await this.waitForConsolidationInterval(everyMs, signal)
    }
  }

  private waitForConsolidationInterval(everyMs: number, signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.resolve()
    return new Promise<void>((resolve) => {
      const onAbort = (): void => {
        timer.cancel()
        resolve()
      }
      const timer = longTimeout(() => {
        signal.removeEventListener("abort", onAbort)
        resolve()
      }, everyMs)
      signal.addEventListener("abort", onAbort, { once: true })
    })
  }

  private async run(definition: AgentDefinition, laser: Laser): Promise<RunOutcome> {
    try {
      const scoped =
        definition.governor !== undefined && definition.governorMode !== undefined
          ? laser.withGovernor(
              definition.governor,
              definition.governorMode,
              definition.governorRetention
            )
          : laser
      if (definition.capabilities.length > 0) {
        await scoped.agent(definition.id).advertise(definition.listenOn, definition.capabilities)
      }
      const consumer = new ReliableConsumer({
        group: definition.consumerGroup ?? ConsumerGroupName.forAgent(definition.id),
        topic: definition.listenOn,
        agent: definition.id,
        shutdownGraceMs: definition.shutdownGraceMs,
        ...(definition.respondOn !== undefined ? { respondOn: definition.respondOn } : {}),
        ...(definition.inboxRoute !== undefined ? { inboxRoute: definition.inboxRoute } : {}),
        ...(definition.pollIntervalMs !== undefined
          ? { pollIntervalMs: definition.pollIntervalMs }
          : {}),
        ...(definition.concurrency !== undefined ? { concurrency: definition.concurrency } : {}),
        ...(definition.maxQueuedRecords !== undefined
          ? { maxQueuedRecords: definition.maxQueuedRecords }
          : {}),
        ...(definition.maxQueuedBytes !== undefined
          ? { maxQueuedBytes: definition.maxQueuedBytes }
          : {}),
        understoodFeatures: definition.understoodFeatures,
        ...(definition.onDeadLetter !== undefined ? { onDeadLetter: definition.onDeadLetter } : {}),
        ...(definition.dedupWindow !== undefined ? { dedupWindow: definition.dedupWindow } : {}),
        ...(definition.retry !== undefined ? { retry: definition.retry } : {}),
        ...(definition.deduplicator !== undefined ? { deduplicator: definition.deduplicator } : {}),
        warmDedup: definition.warmDedup,
        middleware: definition.middleware,
        ackOnPickup: definition.ackOnPickup,
        ...(definition.verifier !== undefined ? { verifier: definition.verifier } : {}),
        ...(definition.signingKey !== undefined ? { signingKey: definition.signingKey } : {})
      })
      await consumer.run(scoped, definition.handler, {
        signal: this.shutdownController.signal,
        hardSignal: this.hardStopController.signal,
        hardAborted: () => this.hardStopped,
        ready: () => {
          this.readySettled = true
          this.resolveReady()
        }
      })
      if (!this.readySettled) this.resolveReady()
      return { kind: "ok" }
    } catch (error) {
      if (!this.readySettled) {
        this.readySettled = true
        this.rejectReady(error)
      }
      return { kind: "error", error }
    }
  }
}

export class AgentBuilder {
  private agentId: AgentId | undefined
  private group: ConsumerGroupName | undefined
  private listenTopic: string | undefined
  private replyTopic: string | undefined
  private agentHandler: AgentHandler | undefined
  private route: InboxRoute | undefined
  private pollMs: number | undefined
  private graceMs = 30_000
  private concurrencyPolicy: ConcurrencyPolicy | undefined
  private queuedRecords: number | undefined
  private queuedBytes: number | undefined
  private understood = 0n
  private shouldWarmDedup = false
  private readonly middlewareList: AgentMiddleware[] = []
  private sink: DeadLetterSink | undefined
  private window: number | undefined
  private retryPolicy: RetryPolicy | undefined
  private dedup: Deduplicator | undefined
  private advertisedCapabilities: readonly CapabilityDescriptor[] = []
  private pickupAck = false
  private signatureVerifier: KeyRegistry | undefined
  private signer: SigningKey | undefined
  private actionGovernor: ActionGovernor | undefined
  private mode: GovernorMode | undefined
  private retention: Partial<GovernorRetention> | undefined
  private consolidationIntervalMs: number | undefined
  private memoryConsolidator: Consolidator | undefined

  id(id: AgentId): this {
    this.agentId = id
    return this
  }

  consumerGroup(group: ConsumerGroupName): this {
    this.group = group
    return this
  }

  listenOn(topic: string): this {
    this.listenTopic = topic
    return this
  }

  respondOn(topic: string): this {
    this.replyTopic = topic
    return this
  }

  handler(handler: AgentHandler): this {
    this.agentHandler = handler
    return this
  }

  inboxRoute(route: InboxRoute): this {
    this.route = route
    return this
  }

  pollInterval(ms: number): this {
    this.pollMs = ms
    return this
  }

  shutdownGrace(ms: number): this {
    this.graceMs = ms
    return this
  }

  concurrency(policy: ConcurrencyPolicy): this {
    this.concurrencyPolicy = policy
    return this
  }

  maxQueuedRecords(value: number): this {
    this.queuedRecords = value
    return this
  }

  maxQueuedBytes(value: number): this {
    this.queuedBytes = value
    return this
  }

  understoodFeatures(bits: bigint): this {
    this.understood = bits
    return this
  }

  warmDedup(value = true): this {
    this.shouldWarmDedup = value
    return this
  }

  middleware(middleware: AgentMiddleware): this {
    this.middlewareList.push(middleware)
    return this
  }

  onDeadLetter(sink: DeadLetterSink): this {
    this.sink = sink
    return this
  }

  dedupWindow(size: number): this {
    this.window = size
    return this
  }

  retry(retry: RetryPolicy): this {
    this.retryPolicy = retry
    return this
  }

  deduplicator(deduplicator: Deduplicator): this {
    this.dedup = deduplicator
    return this
  }

  capabilities(capabilities: readonly CapabilityDescriptor[]): this {
    this.advertisedCapabilities = capabilities
    return this
  }

  ackOnPickup(value = true): this {
    this.pickupAck = value
    return this
  }

  verifier(verifier: KeyRegistry): this {
    this.signatureVerifier = verifier
    return this
  }

  signingKey(signingKey: SigningKey): this {
    this.signer = signingKey
    return this
  }

  /** The policy and mode applied to everything this agent's handler publishes. */
  governor(governor: readonly [ActionGovernor, GovernorMode]): this {
    this.actionGovernor = governor[0]
    this.mode = governor[1]
    return this
  }

  /** Process-local evidence-chain retention for this agent's governor. */
  governorRetention(retention: Partial<GovernorRetention>): this {
    this.retention = retention
    return this
  }

  consolidateEvery(milliseconds: number): this {
    this.consolidationIntervalMs = milliseconds
    return this
  }

  consolidator(consolidator: Consolidator): this {
    this.memoryConsolidator = consolidator
    return this
  }

  /** The configured agent. Throws `HandlerConfigError` when the identity, input topic, or handler is missing. */
  build(): Agent {
    if (this.agentId === undefined) throw new HandlerConfigError("Agent.builder().id() is required")
    if (this.listenTopic === undefined) {
      throw new HandlerConfigError("Agent.builder().listenOn() is required")
    }
    if (this.agentHandler === undefined) {
      throw new HandlerConfigError("Agent.builder().handler() is required")
    }
    return new Agent({
      id: this.agentId,
      ...(this.group !== undefined ? { consumerGroup: this.group } : {}),
      listenOn: this.listenTopic,
      handler: this.agentHandler,
      ...(this.replyTopic !== undefined ? { respondOn: this.replyTopic } : {}),
      ...(this.route !== undefined ? { inboxRoute: this.route } : {}),
      ...(this.pollMs !== undefined ? { pollIntervalMs: this.pollMs } : {}),
      shutdownGraceMs: this.graceMs,
      ...(this.concurrencyPolicy !== undefined ? { concurrency: this.concurrencyPolicy } : {}),
      ...(this.queuedRecords !== undefined ? { maxQueuedRecords: this.queuedRecords } : {}),
      ...(this.queuedBytes !== undefined ? { maxQueuedBytes: this.queuedBytes } : {}),
      understoodFeatures: this.understood,
      warmDedup: this.shouldWarmDedup,
      middleware: [...this.middlewareList],
      ...(this.sink !== undefined ? { onDeadLetter: this.sink } : {}),
      ...(this.window !== undefined ? { dedupWindow: this.window } : {}),
      ...(this.retryPolicy !== undefined ? { retry: this.retryPolicy } : {}),
      ...(this.dedup !== undefined ? { deduplicator: this.dedup } : {}),
      capabilities: this.advertisedCapabilities,
      ackOnPickup: this.pickupAck,
      ...(this.signatureVerifier !== undefined ? { verifier: this.signatureVerifier } : {}),
      ...(this.signer !== undefined ? { signingKey: this.signer } : {}),
      ...(this.actionGovernor !== undefined ? { governor: this.actionGovernor } : {}),
      ...(this.mode !== undefined ? { governorMode: this.mode } : {}),
      ...(this.retention !== undefined ? { governorRetention: this.retention } : {}),
      ...(this.consolidationIntervalMs !== undefined
        ? { consolidateEveryMs: this.consolidationIntervalMs }
        : {}),
      ...(this.memoryConsolidator !== undefined ? { consolidator: this.memoryConsolidator } : {})
    })
  }
}

function validateDefinition(definition: AgentDefinition): void {
  if (!Number.isFinite(definition.shutdownGraceMs) || definition.shutdownGraceMs < 0) {
    throw new HandlerConfigError("shutdownGrace must be a non-negative finite number")
  }
  if (
    definition.pollIntervalMs !== undefined &&
    (!Number.isFinite(definition.pollIntervalMs) || definition.pollIntervalMs < 0)
  ) {
    throw new HandlerConfigError("pollInterval must be a non-negative finite number")
  }
  if (
    definition.dedupWindow !== undefined &&
    (!Number.isSafeInteger(definition.dedupWindow) || definition.dedupWindow < 1)
  ) {
    throw new HandlerConfigError("dedupWindow must be a positive safe integer")
  }
  if (
    definition.retry !== undefined &&
    (!Number.isSafeInteger(definition.retry.maxAttempts) || definition.retry.maxAttempts < 1)
  ) {
    throw new HandlerConfigError("retry maxAttempts must be a positive safe integer")
  }
  if (
    definition.consolidateEveryMs !== undefined &&
    (!Number.isFinite(definition.consolidateEveryMs) || definition.consolidateEveryMs <= 0)
  ) {
    throw new HandlerConfigError("consolidateEvery must be a positive finite number")
  }
  if (
    definition.retry !== undefined &&
    (!Number.isFinite(definition.retry.baseDelayMs) || definition.retry.baseDelayMs < 0)
  ) {
    throw new HandlerConfigError("retry baseDelayMs must be a non-negative finite number")
  }
  if (
    definition.concurrency?.kind === "serial-per-partition" &&
    (!Number.isSafeInteger(definition.concurrency.maxPartitions) ||
      definition.concurrency.maxPartitions < 1)
  ) {
    throw new HandlerConfigError("maxPartitions must be a positive safe integer")
  }
  for (const [name, value] of [
    ["maxQueuedRecords", definition.maxQueuedRecords],
    ["maxQueuedBytes", definition.maxQueuedBytes]
  ] as const) {
    if (value !== undefined && (!Number.isSafeInteger(value) || value < 1)) {
      throw new HandlerConfigError(`${name} must be a positive safe integer`)
    }
  }
}

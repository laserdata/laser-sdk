import { mintUlidValue } from "../runtime/ulid.js"
import type {
  ConsumerTarget,
  IggyHeaderValue,
  LaserTransport,
  PolledMessage
} from "../iggy/apache-iggy.js"
import {
  CancelledError,
  FilterExecutionError,
  InvalidError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "../client/errors.js"
import type { FilteredReader, MatchedRecord } from "../managed/filters.js"
import { jsonCodec, type ValueDecoder } from "./codecs.js"
import type { PollingStrategy } from "./polling-strategy.js"

export interface ConsumedMessage {
  readonly payload: Uint8Array
  readonly partitionId: number
  readonly offset: bigint
  readonly timestampMicros?: bigint
  readonly headers: ReadonlyMap<string, IggyHeaderValue>
}

/** A message a live consumer delivered. `json()` decodes its payload. */
export interface ConsumerMessage extends ConsumedMessage {
  /** Decodes the payload as JSON, through `decodeValue` when given. A payload
   * that is not JSON fails with `CodecError`. */
  json<T = unknown>(decodeValue?: ValueDecoder<T>): T
}

/**
 * When a live consumer stores offsets on the server, the same ten policies the
 * Rust `CommitPolicy` defines. `disabled` never stores on its own: call
 * `commit` after a record is handled. `polling` commits the polled batch on
 * the server before delivery, so a crash can skip records not yet processed.
 * `all` stores after every record of a poll was yielded, `each` after every
 * yielded record, `every` after every `count` yielded records, and the
 * `interval*` forms also store on a timer.
 */
export type CommitPolicy =
  | { readonly kind: "disabled" }
  | { readonly kind: "interval"; readonly intervalMs: number }
  | { readonly kind: "polling" }
  | { readonly kind: "intervalOrPolling"; readonly intervalMs: number }
  | { readonly kind: "all" }
  | { readonly kind: "intervalOrAll"; readonly intervalMs: number }
  | { readonly kind: "each" }
  | { readonly kind: "intervalOrEach"; readonly intervalMs: number }
  | { readonly kind: "every"; readonly count: number }
  | { readonly kind: "intervalOrEvery"; readonly intervalMs: number; readonly count: number }

export interface ConsumerOptions {
  /** Most records one poll returns. Defaults to 1000, like Rust and Python. */
  readonly batchLength?: number
  /** Shorthand for `commitPolicy`: `true` is `polling`, `false` is
   * `disabled`. Set one of the two, not both. */
  readonly autoCommit?: boolean
  readonly commitPolicy?: CommitPolicy
  readonly startFrom?: PollingStrategy
  readonly pollIntervalMs?: number
  /** Native group consumers only: join the group when the consumer is built.
   * Defaults to `true`. A policy-aware group consumer always joins. */
  readonly autoJoinGroup?: boolean
  /** Create the consumer group when it does not exist. Defaults to `true`. */
  readonly createGroup?: boolean
  /** How long a failed poll waits before it is retried. Defaults to 1 second. */
  readonly pollingRetryIntervalMs?: number
  /** Retries for building the consumer (joining or creating its group). */
  readonly initRetries?: { readonly retries: number; readonly intervalMs: number }
  /** Native only: deliver records at or below an offset already consumed,
   * for example after moving the start back. Without it, such records are
   * skipped. A policy-aware group consumer honors its start as given. */
  readonly allowReplay?: boolean
}

const DEFAULT_BATCH_LENGTH = 1000
const DEFAULT_POLL_INTERVAL_MS = 250
const DEFAULT_POLLING_RETRY_INTERVAL_MS = 1_000
const DEFAULT_START_FROM: PollingStrategy = { kind: "next" }

function throwIfAborted(signal: AbortSignal | undefined, message: string): void {
  if (signal?.aborted === true) throw new CancelledError(message, { cause: signal.reason })
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new CancelledError("wait aborted", { cause: signal.reason }))
      return
    }
    const onAbort = (): void => {
      clearTimeout(timer)
      reject(new CancelledError("wait aborted", { cause: signal?.reason }))
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort)
      resolve()
    }, ms)
    signal?.addEventListener("abort", onAbort, { once: true })
  })
}

/**
 * What a live consumer does with a retryable poll failure. `retry` waits the
 * polling retry interval and polls again, like the Rust consumer. `propagate`
 * hands it to a runtime that reopens the consumer itself.
 *
 * @internal
 */
export type PollFailures = "retry" | "propagate"

/** A consumer's options with every default applied and validated. */
export interface ResolvedConsumerOptions {
  readonly batchLength: number
  readonly autoCommit: boolean
  readonly commitPolicy: CommitPolicy
  readonly startFrom: PollingStrategy
  readonly pollIntervalMs: number
  readonly autoJoinGroup: boolean
  readonly createGroup: boolean
  readonly pollingRetryIntervalMs: number
  readonly initRetries: { readonly retries: number; readonly intervalMs: number }
  readonly allowReplay: boolean
}

function commitInterval(policy: CommitPolicy): number | undefined {
  return "intervalMs" in policy ? policy.intervalMs : undefined
}

function validateCommitPolicy(policy: CommitPolicy): void {
  const interval = commitInterval(policy)
  if (interval !== undefined && (!Number.isFinite(interval) || interval <= 0)) {
    throw new InvalidError("commit policy intervalMs must be a positive finite number")
  }
  if ("count" in policy && (!Number.isSafeInteger(policy.count) || policy.count < 1)) {
    throw new InvalidError("commit policy count must be a positive safe integer")
  }
}

/** Apply the defaults and reject values no consumer can run with. */
export function resolveConsumerOptions(
  options: ConsumerOptions,
  anonymous: boolean
): ResolvedConsumerOptions {
  if (options.autoCommit !== undefined && options.commitPolicy !== undefined) {
    throw new InvalidError("set autoCommit or commitPolicy, not both")
  }
  const commitPolicy: CommitPolicy =
    options.commitPolicy ??
    ((options.autoCommit ?? !anonymous) ? { kind: "polling" } : { kind: "disabled" })
  validateCommitPolicy(commitPolicy)
  const initRetries = options.initRetries ?? { retries: 0, intervalMs: 0 }
  const resolved = {
    batchLength: options.batchLength ?? DEFAULT_BATCH_LENGTH,
    autoCommit: commitPolicy.kind !== "disabled",
    commitPolicy,
    startFrom: options.startFrom ?? DEFAULT_START_FROM,
    pollIntervalMs: options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS,
    autoJoinGroup: options.autoJoinGroup ?? true,
    createGroup: options.createGroup ?? true,
    pollingRetryIntervalMs: options.pollingRetryIntervalMs ?? DEFAULT_POLLING_RETRY_INTERVAL_MS,
    initRetries,
    allowReplay: options.allowReplay ?? false
  }
  if (!Number.isFinite(resolved.pollingRetryIntervalMs) || resolved.pollingRetryIntervalMs < 0) {
    throw new InvalidError("consumer pollingRetryIntervalMs must be a non-negative finite number")
  }
  if (
    !Number.isSafeInteger(initRetries.retries) ||
    initRetries.retries < 0 ||
    !Number.isFinite(initRetries.intervalMs) ||
    initRetries.intervalMs < 0
  ) {
    throw new InvalidError("consumer initRetries needs non-negative retries and intervalMs")
  }
  if (!Number.isSafeInteger(resolved.batchLength) || resolved.batchLength < 1) {
    throw new InvalidError("consumer batchLength must be a positive safe integer")
  }
  if (!Number.isFinite(resolved.pollIntervalMs) || resolved.pollIntervalMs < 0) {
    throw new InvalidError("consumer pollIntervalMs must be a non-negative finite number")
  }
  if (
    (resolved.startFrom.kind === "offset" || resolved.startFrom.kind === "timestamp") &&
    resolved.startFrom.value < 0n
  ) {
    throw new InvalidError("consumer start value must be non-negative")
  }
  return resolved
}

/**
 * Polls one partition or consumer group until shutdown. A group consumer on a
 * server that resolves group policies reads through the group reader: the
 * server runs the group's filter, or none, and commits go through the group's
 * fenced acknowledgments. `batchLength` then also bounds the source records
 * one partition poll examines. A purge restarts the partition at offset 0
 * without telling an open reader, which can keep its old position and skip
 * the replacement records, so rebuild it after a purge.
 */
export class Consumer implements AsyncIterable<ConsumerMessage>, AsyncDisposable {
  private readonly batchLength: number
  private readonly autoCommit: boolean
  private readonly commitPolicy: CommitPolicy
  private readonly startFrom: PollingStrategy
  private readonly pollIntervalMs: number
  private readonly pollingRetryIntervalMs: number
  private readonly allowReplay: boolean
  private buffer: PolledMessage[] = []
  private readonly storedOffsets = new Map<number, bigint>()
  private yieldedSinceStore = 0
  private commitTimer: ReturnType<typeof setInterval> | undefined
  private latestDelivered: ConsumerMessage | undefined
  // Group reads: the records of the current page, the record each delivered
  // message came from, and whether a delivery waits to be stored.
  private readonly records: MatchedRecord[] = []
  private readonly delivered = new WeakMap<ConsumedMessage, MatchedRecord>()
  private handledSincePoll = false
  private lastDelivery: MatchedRecord | undefined
  private moreScanned = false
  private readonly consumedOffsets = new Map<number, bigint>()
  private readonly explicitOffsets = new Map<number, bigint>()
  // The consumed offset a partition had when its server offset was deleted.
  // No commit policy stores that offset again, so the delete stands until a
  // newer record is delivered.
  private readonly deletedAt = new Map<number, bigint>()
  private readonly nextOffsets = new Map<number, bigint>()
  private assignedPartitions = new Set<number>()
  private assignmentSeen = false
  private partitionCursor = 0
  private readonly gainedPartitions = new Set<number>()
  private started = false
  private shuttingDown = false
  private polling: Promise<void> | undefined

  constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    private readonly topicName: string,
    private readonly target: ConsumerTarget,
    options: ConsumerOptions = {},
    private readonly reader?: FilteredReader,
    private readonly pollFailures: "retry" | "propagate" = "retry"
  ) {
    const anonymous = target.kind === "single" && target.name === undefined
    if (anonymous) this.target = { ...target, name: `anonymous-${mintUlidValue().toString(16)}` }
    if (
      this.target.kind === "single" &&
      (!Number.isSafeInteger(this.target.partitionId) || this.target.partitionId < 0)
    ) {
      throw new InvalidError("consumer partition must be a non-negative safe integer")
    }
    const resolved = resolveConsumerOptions(options, anonymous)
    this.batchLength = resolved.batchLength
    this.autoCommit = resolved.autoCommit
    this.commitPolicy = resolved.commitPolicy
    this.startFrom = resolved.startFrom
    this.pollIntervalMs = resolved.pollIntervalMs
    this.pollingRetryIntervalMs = resolved.pollingRetryIntervalMs
    this.allowReplay = resolved.allowReplay
  }

  /**
   * Waits for the next record, bounding how long the caller sits idle. Fails
   * with `TimeoutError` past `timeoutMs` and with `InvalidError` once the
   * consumer was shut down, like the Rust `next_within`.
   */
  async nextWithin(
    timeoutMs: number,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<ConsumerMessage> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new InvalidError("nextWithin timeout must be a non-negative finite number")
    }
    const deadline = Date.now() + timeoutMs
    for (;;) {
      if (this.stopped()) throw new InvalidError("the live consumer stream ended")
      throwIfAborted(options.signal, "nextWithin aborted")
      this.handledPrevious()
      const message = this.take()
      if (message !== undefined) {
        await this.afterYield(message)
        return message
      }
      await this.pollOnce(options.signal, deadline)
      throwIfAborted(options.signal, "nextWithin aborted")
      if (this.stopped()) throw new InvalidError("the live consumer stream ended")
      if (this.pending()) continue
      const remaining = deadline - Date.now()
      if (remaining <= 0) throw new TimeoutError("the live consumer to yield a record")
      await delay(this.moreScanned ? 0 : Math.min(this.pollIntervalMs, remaining), options.signal)
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ConsumerMessage> {
    yield* this.stream()
  }

  async *stream(options: { readonly signal?: AbortSignal } = {}): AsyncIterable<ConsumerMessage> {
    while (!this.stopped()) {
      throwIfAborted(options.signal, "consumer stream aborted")
      this.handledPrevious()
      const message = this.take()
      if (message !== undefined) {
        await this.afterYield(message)
        yield message
        continue
      }
      await this.pollOnce(options.signal)
      throwIfAborted(options.signal, "consumer stream aborted")
      if (this.stopped()) return
      if (!this.pending()) {
        await delay(this.moreScanned ? 0 : this.pollIntervalMs, options.signal)
      }
    }
  }

  /**
   * Hands back the latest delivery that the caller could not process, so the
   * next read yields it again. Only the most recent delivery can be returned.
   */
  returnDelivery(message: ConsumerMessage): void {
    if (this.stopped()) throw new InvalidError("consumer has been shut down")
    if (message !== this.latestDelivered) {
      throw new InvalidError("the delivery is not the latest one from this consumer")
    }
    this.latestDelivered = undefined
    if (this.reader !== undefined) {
      const record = this.delivered.get(message)
      if (record === undefined) {
        throw new InvalidError("the record was not delivered by this group consumer")
      }
      if (this.lastDelivery === record) this.lastDelivery = undefined
      this.records.unshift(record)
      return
    }
    this.buffer.unshift(message)
  }

  /**
   * Store a handled message's offset on the server. A group consumer on a
   * server that resolves group policies stores the contiguous prefix of the
   * partition through this message, and needs the message object it delivered.
   */
  async commit(message: ConsumedMessage): Promise<void> {
    if (this.reader !== undefined) {
      const record = this.delivered.get(message)
      if (record === undefined) {
        throw new InvalidError("commit the message object this group consumer delivered")
      }
      await this.reader.ackThrough(record)
      this.explicitOffsets.set(message.partitionId, message.offset)
      this.storedOffsets.set(message.partitionId, message.offset)
      return
    }
    await this.transport.storeOffset(
      this.streamName,
      this.topicName,
      this.target,
      message.partitionId,
      message.offset
    )
    this.explicitOffsets.set(message.partitionId, message.offset)
    this.storedOffsets.set(message.partitionId, message.offset)
  }

  lastConsumedOffset(partitionId: number): bigint | undefined {
    return this.consumedOffsets.get(partitionId)
  }

  /** The last offset this consumer stored for `partitionId`, by `commit`,
   * `storeOffset`, or its commit policy. Local bookkeeping: for server-side
   * resume, start from `next`. */
  lastStoredOffset(partitionId: number): bigint | undefined {
    return this.storedOffsets.get(partitionId)
  }

  /**
   * Stores an explicit server offset for `partitionId`, or for the partition
   * of the latest delivery when omitted. Native only: a policy-aware group
   * consumer refuses it, because an arbitrary offset bypasses the group's
   * acknowledgment contract. Commit a delivered record instead.
   */
  async storeOffset(offset: bigint, partitionId?: number): Promise<void> {
    this.requireNative("storeOffset")
    if (offset < 0n) throw new InvalidError("offset must be non-negative")
    const partition = this.currentPartition(partitionId)
    await this.transport.storeOffset(
      this.streamName,
      this.topicName,
      this.target,
      partition,
      offset
    )
    this.explicitOffsets.set(partition, offset)
    this.storedOffsets.set(partition, offset)
  }

  /** Deletes the server offset for `partitionId`, or for the partition of the
   * latest delivery when omitted. Native only, like `storeOffset`. */
  async deleteOffset(partitionId?: number): Promise<void> {
    this.requireNative("deleteOffset")
    if (this.transport.deleteOffset === undefined) {
      throw new UnsupportedError("the Apache Iggy client does not expose offset deletion")
    }
    const partition = this.currentPartition(partitionId)
    await this.transport.deleteOffset(this.streamName, this.topicName, this.target, partition)
    this.explicitOffsets.delete(partition)
    this.storedOffsets.delete(partition)
    const consumed = this.consumedOffsets.get(partition)
    if (consumed === undefined) this.deletedAt.delete(partition)
    else this.deletedAt.set(partition, consumed)
  }

  async storedOffset(
    partitionId: number
  ): Promise<{ readonly storedOffset: bigint; readonly currentOffset: bigint } | undefined> {
    if (this.transport.getConsumerOffset === undefined) {
      throw new UnsupportedError("the Apache Iggy client does not expose consumer offsets")
    }
    const offsetTarget =
      this.target.kind === "group"
        ? { kind: "group" as const, name: this.target.name }
        : this.target.name === undefined
          ? undefined
          : { kind: "consumer" as const, name: this.target.name }
    if (offsetTarget === undefined) {
      throw new InvalidError("storedOffset() requires a named standalone consumer")
    }
    return this.transport.getConsumerOffset(
      this.streamName,
      this.topicName,
      offsetTarget,
      partitionId
    )
  }

  /**
   * Ends iteration and leaves the consumer group when present. An
   * auto-committing consumer stores the delivered prefix first.
   */
  async shutdown(): Promise<void> {
    if (this.shuttingDown) return
    this.shuttingDown = true
    if (this.commitTimer !== undefined) {
      clearInterval(this.commitTimer)
      this.commitTimer = undefined
    }
    if (this.reader !== undefined) {
      this.handledPrevious()
      this.records.length = 0
      await this.reader.close()
      return
    }
    try {
      if (this.autoCommit) {
        for (const [partition, offset] of this.consumedOffsets) {
          const explicit = this.explicitOffsets.get(partition)
          if (this.deletedAt.get(partition) === offset) continue
          if (explicit === undefined || explicit < offset) {
            await this.transport.storeOffset(
              this.streamName,
              this.topicName,
              this.target,
              partition,
              offset
            )
            this.storedOffsets.set(partition, offset)
          }
        }
      }
    } finally {
      if (this.target.kind === "group") {
        await this.transport.leaveConsumerGroup(this.streamName, this.topicName, this.target.name)
      }
    }
  }

  /** Delegates async disposal to `shutdown()`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.shutdown()
  }

  // The next buffered record as the message to deliver. A group read marks
  // the prefix through it handled when the consumer commits on its own.
  private take(): ConsumerMessage | undefined {
    if (this.reader === undefined) {
      const polled = this.buffer.shift()
      if (polled === undefined) return undefined
      this.consumedOffsets.set(polled.partitionId, polled.offset)
      const message = withJson(polled)
      this.latestDelivered = message
      return message
    }
    let record = this.records.shift()
    while (record !== undefined && !this.reader.owns(record)) record = this.records.shift()
    if (record === undefined) return undefined
    if (this.autoCommit) this.lastDelivery = record
    this.consumedOffsets.set(record.partitionId, record.offset)
    const message = withJson({
      payload: record.payload,
      partitionId: record.partitionId,
      offset: record.offset,
      ...(record.timestampMicros !== undefined ? { timestampMicros: record.timestampMicros } : {}),
      headers: record.headers
    })
    this.delivered.set(message, record)
    this.latestDelivered = message
    return message
  }

  // The native commit policies that store after yielding. `polling` stores on
  // the server during the poll itself, and a group reader stores its handled
  // prefix on its own. A failed store never costs the caller the record: the
  // offset stays pending, and the next yield, timer tick, or shutdown stores
  // it, like the Rust consumer.
  private async afterYield(message: ConsumerMessage): Promise<void> {
    if (this.reader !== undefined) return
    const policy = this.commitPolicy
    this.yieldedSinceStore += 1
    const store =
      policy.kind === "each" ||
      policy.kind === "intervalOrEach" ||
      ((policy.kind === "all" || policy.kind === "intervalOrAll") && this.buffer.length === 0) ||
      ((policy.kind === "every" || policy.kind === "intervalOrEvery") &&
        this.yieldedSinceStore >= policy.count)
    if (!store) return
    try {
      await this.storeConsumed(message.partitionId)
      this.yieldedSinceStore = 0
    } catch {
      // Stored on a later attempt.
    }
  }

  private async storeConsumed(only?: number): Promise<void> {
    for (const [partition, offset] of this.consumedOffsets) {
      if (only !== undefined && partition !== only) continue
      if ((this.storedOffsets.get(partition) ?? -1n) >= offset) continue
      if (this.deletedAt.get(partition) === offset) continue
      await this.transport.storeOffset(
        this.streamName,
        this.topicName,
        this.target,
        partition,
        offset
      )
      this.storedOffsets.set(partition, offset)
    }
  }

  private startCommitTimer(): void {
    const interval = commitInterval(this.commitPolicy)
    if (interval === undefined || this.commitTimer !== undefined || this.reader !== undefined) {
      return
    }
    this.commitTimer = setInterval(() => {
      this.storeConsumed().catch(() => undefined)
    }, interval)
    this.commitTimer.unref()
  }

  private requireNative(operation: string): void {
    if (this.reader !== undefined) {
      throw new InvalidError(
        `${operation}() is native only: a policy-aware group consumer commits delivered records`
      )
    }
  }

  private currentPartition(partitionId: number | undefined): number {
    const partition =
      partitionId ??
      (this.target.kind === "single" ? this.target.partitionId : undefined) ??
      this.latestDelivered?.partitionId
    if (partition === undefined || !Number.isSafeInteger(partition) || partition < 0) {
      throw new InvalidError("name a partition: this consumer has no current partition yet")
    }
    return partition
  }

  // One poll. A retryable transport failure waits the polling retry interval
  // and reports an empty poll, so the read loop keeps going after a server
  // restart, like the Rust consumer. The wait never runs past `deadline`.
  private async pollOnce(signal?: AbortSignal, deadline?: number): Promise<void> {
    this.startCommitTimer()
    try {
      this.polling ??= this.fillBuffer()
      await this.waitForPoll(this.polling, signal, deadline)
    } catch (error) {
      if (
        this.pollFailures === "propagate" ||
        !(error instanceof TransportError) ||
        !error.retryable ||
        this.stopped()
      ) {
        throw error
      }
      const wait =
        deadline === undefined
          ? this.pollingRetryIntervalMs
          : Math.min(this.pollingRetryIntervalMs, Math.max(0, deadline - Date.now()))
      await delay(wait, signal)
    }
  }

  private waitForPoll(poll: Promise<void>, signal?: AbortSignal, deadline?: number): Promise<void> {
    return new Promise((resolve, reject) => {
      let waiting = true
      let timer: ReturnType<typeof setTimeout> | undefined
      const finish = (complete: () => void, observed: boolean): void => {
        if (!waiting) return
        waiting = false
        clearTimeout(timer)
        signal?.removeEventListener("abort", onAbort)
        if (observed && this.polling === poll) this.polling = undefined
        complete()
      }
      const onAbort = (): void => {
        finish(() => {
          reject(new CancelledError("consumer poll aborted", { cause: signal?.reason }))
        }, false)
      }
      poll.then(
        () => {
          if (deadline !== undefined && Date.now() > deadline) {
            finish(() => {
              reject(new TimeoutError("the live consumer to yield a record"))
            }, true)
          } else {
            finish(resolve, true)
          }
        },
        (error: unknown) => {
          finish(() => {
            reject(
              error instanceof Error
                ? error
                : new TransportError("consumer poll failed", true, { cause: error })
            )
          }, true)
        }
      )
      signal?.addEventListener("abort", onAbort, { once: true })
      if (signal?.aborted === true) {
        onAbort()
      } else if (deadline !== undefined) {
        const tick = (): void => {
          if (!waiting) return
          const remaining = deadline - Date.now()
          if (remaining <= 0) {
            finish(() => {
              reject(new TimeoutError("the live consumer to yield a record"))
            }, false)
          } else {
            timer = setTimeout(tick, Math.min(remaining, 2_147_483_647))
          }
        }
        tick()
      }
    })
  }

  private pending(): boolean {
    return this.reader === undefined ? this.buffer.length > 0 : this.records.length > 0
  }

  private stopped(): boolean {
    return this.shuttingDown
  }

  // Asking for the next record or shutting down confirms that the previous
  // delivery reached the caller. A cancelled poll cannot complete a record
  // that the caller never received.
  private handledPrevious(): void {
    if (this.lastDelivery === undefined || this.reader === undefined) return
    this.reader.handled(this.lastDelivery)
    this.lastDelivery = undefined
    this.handledSincePoll = true
  }

  // One group poll. The delivered prefix is stored first, so a crash
  // redelivers the current batch instead of skipping it.
  private async fillFromGroup(reader: FilteredReader): Promise<void> {
    if (this.handledSincePoll) {
      try {
        await reader.flushCompleted()
      } catch (error) {
        // A policy change fences the prefix read under the old policy. Those
        // records are delivered again from the stored offset, so nothing is
        // lost and the consumer goes on without an error.
        if (!(error instanceof FilterExecutionError) || error.reason !== "conflict") throw error
        this.records.length = 0
      }
      this.handledSincePoll = false
    }
    const [page, more] = await reader.readRound()
    this.moreScanned = more
    if (page !== undefined) this.records.push(...page.records)
  }

  private async fillBuffer(signal?: AbortSignal): Promise<void> {
    this.moreScanned = false
    if (signal?.aborted === true) {
      throw new CancelledError("consumer poll aborted", { cause: signal.reason })
    }
    if (this.reader !== undefined) {
      await this.fillFromGroup(this.reader)
      return
    }
    let target = this.target
    if (
      target.kind === "group" &&
      (this.startFrom.kind !== "next" || !this.autoCommit) &&
      this.transport.syncConsumerGroup !== undefined
    ) {
      const assignment = await this.transport.syncConsumerGroup(
        this.streamName,
        this.topicName,
        target.name
      )
      if (assignment === undefined)
        throw new TransportError("the consumer group has no active assignment", true)
      if (assignment.rejoined) {
        this.nextOffsets.clear()
        this.assignedPartitions.clear()
      }
      const assigned = new Set(assignment.partitions)
      for (const partition of this.assignedPartitions) {
        if (!assigned.has(partition)) {
          this.nextOffsets.delete(partition)
          this.gainedPartitions.delete(partition)
        }
      }
      if (this.assignmentSeen) {
        for (const partition of assigned) {
          if (!this.assignedPartitions.has(partition)) this.gainedPartitions.add(partition)
        }
      }
      this.assignedPartitions = assigned
      this.assignmentSeen = true
      if (assignment.partitions.length === 0) return
      const partitionId =
        assignment.partitions[this.partitionCursor++ % assignment.partitions.length]
      if (partitionId === undefined) return
      target = { ...target, partitionId }
    }
    const partition = target.partitionId
    const next = partition === undefined ? undefined : this.nextOffsets.get(partition)
    const strategy =
      next !== undefined
        ? { kind: "offset" as const, value: next }
        : partition !== undefined
          ? this.gainedPartitions.has(partition)
            ? { kind: "next" as const }
            : this.startFrom
          : this.started
            ? { kind: "next" as const }
            : this.startFrom
    const commitOnPoll =
      this.commitPolicy.kind === "polling" || this.commitPolicy.kind === "intervalOrPolling"
    const polled = await this.transport.pollMessages(
      this.streamName,
      this.topicName,
      target,
      strategy,
      this.batchLength,
      commitOnPoll
    )
    this.started = true
    for (const message of polled) this.nextOffsets.set(message.partitionId, message.offset + 1n)
    const fresh = this.allowReplay
      ? polled
      : polled.filter((message) => {
          const consumed = this.consumedOffsets.get(message.partitionId)
          return consumed === undefined || message.offset > consumed
        })
    if (commitOnPoll) {
      for (const message of fresh) this.storedOffsets.set(message.partitionId, message.offset)
    }
    this.buffer.push(...fresh)
  }
}

function withJson(message: ConsumedMessage): ConsumerMessage {
  const json = <T = unknown>(decodeValue?: ValueDecoder<T>): T =>
    jsonCodec<T>(decodeValue ?? ((value) => value as T)).decode(message.payload)
  // Not enumerable, so a delivered message still compares equal to its plain
  // fields.
  return Object.defineProperty({ ...message }, "json", { value: json }) as ConsumerMessage
}

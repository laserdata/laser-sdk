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
  TransportError,
  UnsupportedError
} from "../client/errors.js"
import type { FilteredReader, MatchedRecord } from "../managed/filters.js"
import type { PollingStrategy } from "./polling-strategy.js"

export interface ConsumedMessage {
  readonly payload: Uint8Array
  readonly partitionId: number
  readonly offset: bigint
  readonly timestampMicros?: bigint
  readonly headers: ReadonlyMap<string, IggyHeaderValue>
}

export interface ConsumerOptions {
  readonly batchLength?: number
  readonly autoCommit?: boolean
  readonly startFrom?: PollingStrategy
  readonly pollIntervalMs?: number
}

const DEFAULT_BATCH_LENGTH = 100
const DEFAULT_POLL_INTERVAL_MS = 250
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

/** A consumer's options with every default applied and validated. */
export interface ResolvedConsumerOptions {
  readonly batchLength: number
  readonly autoCommit: boolean
  readonly startFrom: PollingStrategy
  readonly pollIntervalMs: number
}

/** Apply the defaults and reject values no consumer can run with. */
export function resolveConsumerOptions(
  options: ConsumerOptions,
  anonymous: boolean
): ResolvedConsumerOptions {
  const resolved = {
    batchLength: options.batchLength ?? DEFAULT_BATCH_LENGTH,
    autoCommit: options.autoCommit ?? !anonymous,
    startFrom: options.startFrom ?? DEFAULT_START_FROM,
    pollIntervalMs: options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS
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
export class Consumer implements AsyncIterable<ConsumedMessage>, AsyncDisposable {
  private readonly batchLength: number
  private readonly autoCommit: boolean
  private readonly startFrom: PollingStrategy
  private readonly pollIntervalMs: number
  private buffer: PolledMessage[] = []
  // Group reads: the records of the current page, the record each delivered
  // message came from, and whether a delivery waits to be stored.
  private readonly records: MatchedRecord[] = []
  private readonly delivered = new WeakMap<ConsumedMessage, MatchedRecord>()
  private handledSincePoll = false
  private lastDelivery: MatchedRecord | undefined
  private moreScanned = false
  private readonly consumedOffsets = new Map<number, bigint>()
  private readonly explicitOffsets = new Map<number, bigint>()
  private readonly nextOffsets = new Map<number, bigint>()
  private assignedPartitions = new Set<number>()
  private assignmentSeen = false
  private partitionCursor = 0
  private readonly gainedPartitions = new Set<number>()
  private started = false
  private shuttingDown = false

  constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    private readonly topicName: string,
    private readonly target: ConsumerTarget,
    options: ConsumerOptions = {},
    private readonly reader?: FilteredReader
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
    this.startFrom = resolved.startFrom
    this.pollIntervalMs = resolved.pollIntervalMs
  }

  async nextWithin(
    timeoutMs: number,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<ConsumedMessage | null> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new InvalidError("nextWithin timeout must be a non-negative finite number")
    }
    const deadline = Date.now() + timeoutMs
    for (;;) {
      if (this.stopped()) return null
      throwIfAborted(options.signal, "nextWithin aborted")
      this.handledPrevious()
      const message = this.take()
      if (message !== undefined) return message
      await this.fillBuffer(options.signal)
      throwIfAborted(options.signal, "nextWithin aborted")
      if (this.stopped()) return null
      if (this.pending()) continue
      const remaining = deadline - Date.now()
      if (remaining <= 0) return null
      await delay(this.moreScanned ? 0 : Math.min(this.pollIntervalMs, remaining), options.signal)
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ConsumedMessage> {
    yield* this.stream()
  }

  async *stream(options: { readonly signal?: AbortSignal } = {}): AsyncIterable<ConsumedMessage> {
    while (!this.stopped()) {
      throwIfAborted(options.signal, "consumer stream aborted")
      this.handledPrevious()
      const message = this.take()
      if (message !== undefined) {
        yield message
        continue
      }
      await this.fillBuffer(options.signal)
      throwIfAborted(options.signal, "consumer stream aborted")
      if (this.stopped()) return
      if (!this.pending()) {
        await delay(this.moreScanned ? 0 : this.pollIntervalMs, options.signal)
      }
    }
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
  }

  lastConsumedOffset(partitionId: number): bigint | undefined {
    return this.consumedOffsets.get(partitionId)
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
          if (explicit === undefined || explicit < offset) {
            await this.transport.storeOffset(
              this.streamName,
              this.topicName,
              this.target,
              partition,
              offset
            )
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
  private take(): ConsumedMessage | undefined {
    if (this.reader === undefined) {
      const message = this.buffer.shift()
      if (message !== undefined) this.consumedOffsets.set(message.partitionId, message.offset)
      return message
    }
    let record = this.records.shift()
    while (record !== undefined && !this.reader.owns(record)) record = this.records.shift()
    if (record === undefined) return undefined
    if (this.autoCommit) this.lastDelivery = record
    this.consumedOffsets.set(record.partitionId, record.offset)
    const message: ConsumedMessage = {
      payload: record.payload,
      partitionId: record.partitionId,
      offset: record.offset,
      ...(record.timestampMicros !== undefined ? { timestampMicros: record.timestampMicros } : {}),
      headers: record.headers
    }
    this.delivered.set(message, record)
    return message
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
    const polled = await this.transport.pollMessages(
      this.streamName,
      this.topicName,
      target,
      strategy,
      this.batchLength,
      this.autoCommit
    )
    this.started = true
    for (const message of polled) this.nextOffsets.set(message.partitionId, message.offset + 1n)
    this.buffer.push(...polled)
  }
}

import { mintUlidValue } from "../runtime/ulid.js"
import type {
  ConsumerTarget,
  IggyHeaderValue,
  LaserTransport,
  PolledMessage
} from "../iggy/apache-iggy.js"
import { CancelledError, InvalidError, TransportError, UnsupportedError } from "../client/errors.js"
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
 * Polls one partition or consumer group until shutdown. A purge restarts the
 * partition at offset 0 without telling an open reader, which can keep its old
 * position and skip the replacement records, so rebuild it after a purge.
 */
export class Consumer implements AsyncIterable<ConsumedMessage>, AsyncDisposable {
  private readonly batchLength: number
  private readonly autoCommit: boolean
  private readonly startFrom: PollingStrategy
  private readonly pollIntervalMs: number
  private buffer: PolledMessage[] = []
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
    options: ConsumerOptions = {}
  ) {
    const anonymous = target.kind === "single" && target.name === undefined
    if (anonymous) this.target = { ...target, name: `anonymous-${mintUlidValue().toString(16)}` }
    this.batchLength = options.batchLength ?? DEFAULT_BATCH_LENGTH
    this.autoCommit = options.autoCommit ?? !anonymous
    this.startFrom = options.startFrom ?? DEFAULT_START_FROM
    this.pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS
    if (
      this.target.kind === "single" &&
      (!Number.isSafeInteger(this.target.partitionId) || this.target.partitionId < 0)
    ) {
      throw new InvalidError("consumer partition must be a non-negative safe integer")
    }
    if (!Number.isSafeInteger(this.batchLength) || this.batchLength < 1) {
      throw new InvalidError("consumer batchLength must be a positive safe integer")
    }
    if (!Number.isFinite(this.pollIntervalMs) || this.pollIntervalMs < 0) {
      throw new InvalidError("consumer pollIntervalMs must be a non-negative finite number")
    }
    if (
      (this.startFrom.kind === "offset" || this.startFrom.kind === "timestamp") &&
      this.startFrom.value < 0n
    ) {
      throw new InvalidError("consumer start value must be non-negative")
    }
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
      if (options.signal?.aborted === true) {
        throw new CancelledError("nextWithin aborted", { cause: options.signal.reason })
      }
      const message = this.buffer.shift()
      if (message !== undefined) {
        this.consumedOffsets.set(message.partitionId, message.offset)
        return message
      }
      await this.fillBuffer(options.signal)
      if (this.buffer.length > 0) continue
      const remaining = deadline - Date.now()
      if (remaining <= 0) return null
      await delay(Math.min(this.pollIntervalMs, remaining), options.signal)
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ConsumedMessage> {
    yield* this.stream()
  }

  async *stream(options: { readonly signal?: AbortSignal } = {}): AsyncIterable<ConsumedMessage> {
    while (!this.shuttingDown) {
      if (options.signal?.aborted === true) {
        throw new CancelledError("consumer stream aborted", { cause: options.signal.reason })
      }
      const message = this.buffer.shift()
      if (message !== undefined) {
        this.consumedOffsets.set(message.partitionId, message.offset)
        yield message
        continue
      }
      await this.fillBuffer(options.signal)
      if (this.buffer.length === 0) {
        await delay(this.pollIntervalMs, options.signal)
      }
    }
  }

  async commit(message: ConsumedMessage): Promise<void> {
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

  /** Ends iteration and leaves the consumer group when present. */
  async shutdown(): Promise<void> {
    if (this.shuttingDown) return
    this.shuttingDown = true
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

  private async fillBuffer(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted === true) {
      throw new CancelledError("consumer poll aborted", { cause: signal.reason })
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

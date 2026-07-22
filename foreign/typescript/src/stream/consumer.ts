import type {
  ConsumerTarget,
  IggyHeaderValue,
  LaserTransport,
  PolledMessage
} from "../iggy/apache-iggy.js"
import { CancelledError, InvalidError, UnsupportedError } from "../client/errors.js"
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

// A consumer over either one partition (`Topic.consumer`) or a consumer
// group (`Topic.consumerGroup`), the same class either way since polling,
// buffering, and commit all take the same shape once the target is
// resolved. `done: true` from the async iterator means shutdown, never
// merely caught up: a poll that returns no messages waits `pollIntervalMs`
// and polls again rather than terminating.
export class Consumer implements AsyncIterable<ConsumedMessage> {
  private readonly batchLength: number
  private readonly autoCommit: boolean
  private readonly startFrom: PollingStrategy
  private readonly pollIntervalMs: number
  private buffer: PolledMessage[] = []
  private readonly consumedOffsets = new Map<number, bigint>()
  private started = false
  private shuttingDown = false

  constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    private readonly topicName: string,
    private readonly target: ConsumerTarget,
    options: ConsumerOptions = {}
  ) {
    this.batchLength = options.batchLength ?? DEFAULT_BATCH_LENGTH
    this.autoCommit = options.autoCommit ?? true
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
      if (message !== undefined) return message
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

  async shutdown(): Promise<void> {
    this.shuttingDown = true
    if (this.target.kind === "group") {
      await this.transport.leaveConsumerGroup(this.streamName, this.topicName, this.target.name)
    }
  }

  private async fillBuffer(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted === true) {
      throw new CancelledError("consumer poll aborted", { cause: signal.reason })
    }
    const strategy = this.started ? ({ kind: "next" } as const) : this.startFrom
    this.started = true
    const polled = await this.transport.pollMessages(
      this.streamName,
      this.topicName,
      this.target,
      strategy,
      this.batchLength,
      this.autoCommit
    )
    for (const message of polled) this.consumedOffsets.set(message.partitionId, message.offset)
    this.buffer.push(...polled)
  }
}

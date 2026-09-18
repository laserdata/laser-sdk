import type { LaserTransport } from "../iggy/apache-iggy.js"
import { CancelledError, InvalidError } from "../client/errors.js"
import type { ConsumedMessage } from "./consumer.js"
import type { PollingStrategy } from "./polling-strategy.js"

export interface CursorOptions {
  readonly batchSize?: number
  readonly readerName?: string
}

const DEFAULT_BATCH_SIZE = 100
const MAX_BATCH_SIZE = 10_000
const DEFAULT_POLL_INTERVAL_MS = 250

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

export class Cursor {
  private batchSize: number
  private readonly partitionOffsets: Map<number, bigint>
  private readonly partitionEnds = new Map<number, bigint>()
  private readonly readerName: string | undefined

  constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    private readonly topicName: string,
    partitionIds: readonly number[],
    options: CursorOptions = {}
  ) {
    this.batchSize = batchSize(options.batchSize ?? DEFAULT_BATCH_SIZE)
    this.readerName = options.readerName
    this.partitionOffsets = new Map(partitionIds.map((id) => [id, 0n]))
  }

  get offsets(): ReadonlyMap<number, bigint> {
    return new Map(this.partitionOffsets)
  }

  fromOffsets(offsets: ReadonlyMap<number, bigint>): this {
    for (const [partitionId, offset] of offsets) {
      if (this.partitionOffsets.has(partitionId)) {
        this.partitionOffsets.set(partitionId, offset)
      }
    }
    return this
  }

  /** Stops each partition at its exclusive `ends` offset instead of the tail. */
  until(ends: ReadonlyMap<number, bigint>): this {
    for (const [partitionId, end] of ends) {
      if (this.partitionOffsets.has(partitionId)) this.partitionEnds.set(partitionId, end)
    }
    return this
  }

  batch(size: number): this {
    this.batchSize = batchSize(size)
    return this
  }

  async poll(options: { readonly signal?: AbortSignal } = {}): Promise<readonly ConsumedMessage[]> {
    if (options.signal?.aborted === true) {
      throw new CancelledError("poll aborted", { cause: options.signal.reason })
    }
    const results: ConsumedMessage[] = []
    const nextOffsets = new Map(this.partitionOffsets)
    for (const [partitionId, offset] of this.partitionOffsets) {
      const end = this.partitionEnds.get(partitionId)
      if (end !== undefined && offset >= end) continue
      const strategy: PollingStrategy = { kind: "offset", value: offset }
      const polled = await this.transport.pollMessages(
        this.streamName,
        this.topicName,
        {
          kind: "single",
          partitionId,
          ...(this.readerName !== undefined ? { name: this.readerName } : {})
        },
        strategy,
        this.batchSize,
        false
      )
      checkCancellation(options.signal)
      for (const message of polled) {
        if (end !== undefined && message.offset >= end) {
          nextOffsets.set(partitionId, end)
          break
        }
        results.push(message)
        nextOffsets.set(partitionId, message.offset + 1n)
      }
    }
    for (const [partitionId, offset] of nextOffsets) {
      this.partitionOffsets.set(partitionId, offset)
    }
    return results
  }

  async *stream(
    options: { readonly signal?: AbortSignal; readonly pollIntervalMs?: number } = {}
  ): AsyncIterable<ConsumedMessage> {
    const pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS
    for (;;) {
      const batch = await this.poll(options.signal === undefined ? {} : { signal: options.signal })
      if (batch.length === 0) {
        await delay(pollIntervalMs, options.signal)
        continue
      }
      for (const message of batch) yield message
    }
  }
}

function batchSize(size: number): number {
  if (!Number.isInteger(size) || size < 0 || size > 0xffff_ffff) {
    throw new InvalidError("cursor batch size must be an unsigned 32-bit integer")
  }
  return Math.min(MAX_BATCH_SIZE, Math.max(1, size))
}

function checkCancellation(signal?: AbortSignal): void {
  if (signal?.aborted === true) {
    throw new CancelledError("poll aborted", { cause: signal.reason })
  }
}

import type { LaserTransport } from "../iggy/apache-iggy.js"
import { CancelledError } from "../client/errors.js"
import type { ConsumedMessage } from "./consumer.js"
import type { PollingStrategy } from "./polling-strategy.js"

export interface CursorOptions {
  readonly batchSize?: number
  readonly readerName?: string
}

const DEFAULT_BATCH_SIZE = 100
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
  private readonly readerName: string | undefined

  constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    private readonly topicName: string,
    partitionIds: readonly number[],
    options: CursorOptions = {}
  ) {
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE
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

  batch(size: number): this {
    this.batchSize = size
    return this
  }

  async poll(options: { readonly signal?: AbortSignal } = {}): Promise<readonly ConsumedMessage[]> {
    if (options.signal?.aborted === true) {
      throw new CancelledError("poll aborted", { cause: options.signal.reason })
    }
    const results: ConsumedMessage[] = []
    for (const [partitionId, offset] of this.partitionOffsets) {
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
      for (const message of polled) {
        results.push(message)
        this.partitionOffsets.set(partitionId, message.offset + 1n)
      }
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

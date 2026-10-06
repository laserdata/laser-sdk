import type { LaserTransport } from "../iggy/apache-iggy.js"
import { CancelledError, InvalidError } from "../client/errors.js"
import type { ConsumerStart } from "./consumer-start.js"
import { type Message, withMessageJson } from "./message.js"

// A cursor read with the log fields the SDK's own readers order and resume by.
interface CursorRecord extends Message {
  readonly partitionId: number
  readonly offset: bigint
  readonly timestampMicros?: bigint
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
  private batchSize = DEFAULT_BATCH_SIZE
  private readonly partitionOffsets: Map<number, bigint>
  private readonly partitionEnds = new Map<number, bigint>()
  private readerName: string | undefined

  private constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    private readonly topicName: string,
    partitionIds: readonly number[]
  ) {
    this.partitionOffsets = new Map(partitionIds.map((id) => [id, 0n]))
  }

  /** @internal */
  static create(
    transport: LaserTransport,
    streamName: string,
    topicName: string,
    partitionIds: readonly number[]
  ): Cursor {
    return new Cursor(transport, streamName, topicName, partitionIds)
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

  /**
   * Stops each partition at its exclusive `ends` offset instead of the tail.
   * @internal
   */
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

  /**
   * Attributes the reads to a named reader. Offsets stay client-owned, the
   * name only identifies the reader on the server.
   * @internal
   */
  named(readerName: string): this {
    this.readerName = readerName
    return this
  }

  async poll(options: { readonly signal?: AbortSignal } = {}): Promise<readonly Message[]> {
    return this.pollRecords(options)
  }

  /** @internal */
  async pollRecords(
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<readonly CursorRecord[]> {
    if (options.signal?.aborted === true) {
      throw new CancelledError("poll aborted", { cause: options.signal.reason })
    }
    const results: CursorRecord[] = []
    const nextOffsets = new Map(this.partitionOffsets)
    for (const [partitionId, offset] of this.partitionOffsets) {
      const end = this.partitionEnds.get(partitionId)
      if (end !== undefined && offset >= end) continue
      const strategy: ConsumerStart = { kind: "offset", value: offset }
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
        results.push(
          withMessageJson({
            ...message,
            id: { partitionId: message.partitionId, offset: message.offset }
          })
        )
        nextOffsets.set(partitionId, message.offset + 1n)
      }
    }
    for (const [partitionId, offset] of nextOffsets) {
      this.partitionOffsets.set(partitionId, offset)
    }
    return results
  }

  stream(
    options: { readonly signal?: AbortSignal; readonly pollIntervalMs?: number } = {}
  ): AsyncIterable<Message> {
    return this.streamRecords(options)
  }

  /** @internal */
  async *streamRecords(
    options: { readonly signal?: AbortSignal; readonly pollIntervalMs?: number } = {}
  ): AsyncIterable<CursorRecord> {
    const pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS
    for (;;) {
      const batch = await this.pollRecords(
        options.signal === undefined ? {} : { signal: options.signal }
      )
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

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

const DEFAULT_BATCH_SIZE = 1_000
// Ceiling on the records one poll accumulates per partition, like Rust's
// MAX_DRAIN_MESSAGES. A caller that has not caught up polls again.
const MAX_DRAIN_RECORDS = 10_000

export class Cursor {
  private batchSize = DEFAULT_BATCH_SIZE
  // Empty until `fromOffsets` or the first successful poll, like Rust.
  private partitionOffsets = new Map<number, bigint>()
  private readonly partitionEnds = new Map<number, bigint>()
  private readerName: string | undefined

  private constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    private readonly topicName: string,
    private readonly partitionIds: readonly number[]
  ) {}

  /** @internal */
  static create(
    transport: LaserTransport,
    streamName: string,
    topicName: string,
    partitionIds: readonly number[]
  ): Cursor {
    return new Cursor(transport, streamName, topicName, partitionIds)
  }

  /**
   * The next offset to read on each partition. Empty before the first poll
   * unless `fromOffsets` seeded it. Persist this to resume later with
   * `fromOffsets`.
   */
  get offsets(): ReadonlyMap<number, bigint> {
    return new Map(this.partitionOffsets)
  }

  /** @internal The partitions this cursor reads. */
  get partitions(): readonly number[] {
    return this.partitionIds
  }

  /**
   * Resumes from previously persisted offsets, exactly what an earlier
   * `offsets` returned. A partition the map does not name reads from 0.
   */
  fromOffsets(offsets: ReadonlyMap<number, bigint>): this {
    this.partitionOffsets = new Map(offsets)
    return this
  }

  /**
   * Stops each partition at its exclusive `ends` offset instead of the tail.
   * @internal
   */
  until(ends: ReadonlyMap<number, bigint>): this {
    for (const [partitionId, end] of ends) {
      if (this.partitionIds.includes(partitionId)) this.partitionEnds.set(partitionId, end)
    }
    return this
  }

  /**
   * Sizes each server request at `size` records per partition (default
   * 1000). One poll keeps requesting until the partition's tail or 10,000
   * records, whichever comes first.
   */
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

  /**
   * Drains everything appended since the last poll, ordered by log timestamp,
   * then partition and offset. An empty array means caught up.
   */
  async poll(options: { readonly signal?: AbortSignal } = {}): Promise<readonly Message[]> {
    return this.pollRecords(options)
  }

  /** @internal */
  async pollRecords(
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<readonly CursorRecord[]> {
    checkCancellation(options.signal)
    const results: CursorRecord[] = []
    const nextOffsets = new Map(this.partitionOffsets)
    for (const partitionId of this.partitionIds) {
      const start = this.partitionOffsets.get(partitionId) ?? 0n
      nextOffsets.set(partitionId, await this.drain(partitionId, start, results, options.signal))
    }
    this.partitionOffsets = nextOffsets
    return results.sort(byLogOrder)
  }

  /**
   * This cursor as an async iterable that drains everything currently
   * appended and ends once caught up, like Rust `Cursor::stream`. A fresh
   * cursor seeded from the persisted `offsets` resumes from there.
   */
  stream(options: { readonly signal?: AbortSignal } = {}): AsyncIterable<Message> {
    return this.streamRecords(options)
  }

  /** @internal */
  async *streamRecords(
    options: { readonly signal?: AbortSignal } = {}
  ): AsyncIterable<CursorRecord> {
    for (;;) {
      const batch = await this.pollRecords(options)
      if (batch.length === 0) return
      yield* batch
    }
  }

  private async drain(
    partitionId: number,
    start: bigint,
    into: CursorRecord[],
    signal: AbortSignal | undefined
  ): Promise<bigint> {
    const end = this.partitionEnds.get(partitionId)
    let offset = start
    let drained = 0
    while (end === undefined || offset < end) {
      const count = Math.min(this.batchSize, MAX_DRAIN_RECORDS - drained)
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
        count,
        false
      )
      checkCancellation(signal)
      for (const message of polled) {
        if (end !== undefined && message.offset >= end) return end
        into.push(
          withMessageJson({
            ...message,
            id: { partitionId: message.partitionId, offset: message.offset }
          })
        )
        offset = message.offset + 1n
        drained += 1
      }
      if (polled.length < count || drained >= MAX_DRAIN_RECORDS) break
    }
    return offset
  }
}

function byLogOrder(left: CursorRecord, right: CursorRecord): number {
  const leftTime = left.timestampMicros ?? 0n
  const rightTime = right.timestampMicros ?? 0n
  if (leftTime !== rightTime) return leftTime < rightTime ? -1 : 1
  if (left.partitionId !== right.partitionId) return left.partitionId - right.partitionId
  return left.offset < right.offset ? -1 : left.offset > right.offset ? 1 : 0
}

function batchSize(size: number): number {
  if (!Number.isInteger(size) || size < 0 || size > 0xffff_ffff) {
    throw new InvalidError("cursor batch size must be an unsigned 32-bit integer")
  }
  return Math.max(1, size)
}

function checkCancellation(signal?: AbortSignal): void {
  if (signal?.aborted === true) {
    throw new CancelledError("poll aborted", { cause: signal.reason })
  }
}

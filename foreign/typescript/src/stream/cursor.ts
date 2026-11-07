import type {
  ConsumerTarget,
  HeldPartition,
  LaserTransport,
  PollWait,
  PolledMessage
} from "../iggy/apache-iggy.js"
import { CancelledError, InvalidError, TransportError } from "../client/errors.js"
import type { ConsumerStart } from "./consumer-start.js"
import { type Message, withMessageJson } from "./message.js"

// A cursor read with the log fields the SDK's own readers order and resume by.
interface CursorRecord extends Message {
  readonly partitionId: number
  readonly offset: bigint
  readonly timestampMicros?: bigint
  /** The id the record was sent with. */
  readonly messageId?: bigint
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
    private partitionIds: readonly number[],
    private readonly partitionCount?: () => Promise<number | undefined>
  ) {}

  /** @internal A cursor over `partitionIds`, or over the topic's partitions
   * as `partitionCount` reports them at each poll, like Rust. */
  static create(
    transport: LaserTransport,
    streamName: string,
    topicName: string,
    partitionIds: readonly number[],
    partitionCount?: () => Promise<number | undefined>
  ): Cursor {
    return new Cursor(transport, streamName, topicName, partitionIds, partitionCount)
  }

  /**
   * The next offset to read on each partition, past every record handed out
   * so far. Empty before the first poll unless `fromOffsets` seeded it.
   * Persist this after handling a record to resume right after it with
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
    const [records, nextOffsets] = await this.readBatch(options.signal)
    this.partitionOffsets = nextOffsets
    return records
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
      const [batch, nextOffsets] = await this.readBatch(options.signal)
      if (batch.length === 0) {
        this.partitionOffsets = nextOffsets
        return
      }
      for (const [index, record] of batch.entries()) {
        if (index === batch.length - 1) this.partitionOffsets = nextOffsets
        else this.partitionOffsets.set(record.partitionId, record.offset + 1n)
        yield record
      }
    }
  }

  // One drain of every partition and the offsets after it, leaving the
  // cursor's own offsets as they were.
  private async readBatch(
    signal: AbortSignal | undefined
  ): Promise<readonly [readonly CursorRecord[], Map<number, bigint>]> {
    checkCancellation(signal)
    if (this.partitionCount !== undefined) {
      const count = await this.partitionCount()
      if (count === undefined) return [[], new Map(this.partitionOffsets)]
      this.partitionIds = Array.from({ length: count }, (_, index) => index)
    }
    const results: CursorRecord[] = []
    const nextOffsets = new Map(this.partitionOffsets)
    for (const partitionId of this.partitionIds) {
      const start = this.partitionOffsets.get(partitionId) ?? 0n
      nextOffsets.set(partitionId, await this.drain(partitionId, start, results, signal))
    }
    return [results.sort(byLogOrder), nextOffsets]
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
      // A reply under the byte cap can hold fewer records than asked, so only
      // an empty reply or the head it reported ends the partition.
      const head = polled.at(-1)?.currentOffset
      if (
        polled.length === 0 ||
        drained >= MAX_DRAIN_RECORDS ||
        (head !== undefined && offset > head)
      )
        break
    }
    return offset
  }
}

/** Read one partition from `from` through `through`, stopping early at an
 * empty reply. A reply under the byte cap can hold fewer records than asked.
 * @internal */
export async function readThrough(
  transport: LaserTransport,
  stream: string,
  topic: string,
  target: ConsumerTarget,
  from: bigint,
  through: bigint
): Promise<readonly PolledMessage[]> {
  const messages: PolledMessage[] = []
  let next = from
  while (next <= through) {
    const batch = await transport.pollMessages(
      stream,
      topic,
      target,
      { kind: "offset", value: next },
      Number(through - next + 1n),
      false
    )
    const fresh = batch.filter((message) => message.offset >= next && message.offset <= through)
    if (fresh.length === 0) break
    messages.push(...fresh)
    next = (fresh.at(-1)?.offset ?? next) + 1n
  }
  return messages
}

/**
 * The records of the first partition of `offsets` that holds one at or after
 * its offset, with one read that the server holds up to `wait` per partition
 * primary, or per partition when the client cannot route a fetch. One client
 * parks at most 64 such reads, so a fetch per primary keeps a topic of any
 * partition count from queueing behind them. The other reads are cancelled
 * once one answers with records. They store no offset, so nothing is lost.
 * `undefined` when every read ended empty.
 * @internal
 */
export async function heldRead(
  transport: LaserTransport,
  stream: string,
  topic: string,
  offsets: ReadonlyMap<number, bigint>,
  count: number,
  wait: PollWait,
  signal?: AbortSignal,
  reader?: string
): Promise<HeldPartition | undefined> {
  const cancel = new AbortController()
  const stop = (): void => {
    cancel.abort()
  }
  signal?.addEventListener("abort", stop, { once: true })
  try {
    const fetchHeld = transport.fetchHeld?.bind(transport)
    const groups =
      fetchHeld === undefined || offsets.size === 0
        ? undefined
        : await transport.primaryGroups?.(stream, topic, [...offsets.keys()])
    const reads: (() => Promise<readonly HeldPartition[]>)[] =
      fetchHeld === undefined || groups === undefined
        ? [...offsets].map(([partitionId, offset]) => async () => {
            const target: ConsumerTarget = {
              kind: "single",
              partitionId,
              ...(reader !== undefined ? { name: reader } : {})
            }
            const messages = await transport.pollMessages(
              stream,
              topic,
              target,
              { kind: "offset", value: offset },
              count,
              false,
              wait,
              cancel.signal
            )
            return [{ partitionId, messages }]
          })
        : groups.map(
            (group) => () =>
              fetchHeld(
                stream,
                topic,
                new Map(group.map((partitionId) => [partitionId, offsets.get(partitionId) ?? 0n])),
                count,
                wait,
                cancel.signal,
                reader
              )
          )
    return await new Promise((resolve, reject) => {
      let open = reads.length
      let failure: unknown
      const settle = (): void => {
        open -= 1
        if (open > 0) return
        if (failure === undefined) resolve(undefined)
        else
          reject(
            failure instanceof Error
              ? failure
              : new TransportError("a held read failed", true, { cause: failure })
          )
      }
      if (open === 0) resolve(undefined)
      for (const read of reads) {
        read().then(
          (answered) => {
            for (const { partitionId, messages } of answered) {
              const from = offsets.get(partitionId) ?? 0n
              const fresh = messages.filter((message) => message.offset >= from)
              if (fresh.length > 0) {
                resolve({ partitionId, messages: fresh })
                break
              }
            }
            settle()
          },
          (error: unknown) => {
            failure ??= error
            settle()
          }
        )
      }
    })
  } finally {
    signal?.removeEventListener("abort", stop)
    cancel.abort()
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

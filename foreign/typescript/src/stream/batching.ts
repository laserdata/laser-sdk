import { type BytesLike, ownedBytes } from "../client/bytes.js"
import { InvalidError, PublishFailedError } from "../client/errors.js"
import type { IggyHeaderValue, MessageWithHeaders } from "../iggy/apache-iggy.js"

/** Flush at this many queued records unless overridden. */
export const DEFAULT_MAX_RECORDS = 512
/** Flush at this many queued payload bytes unless overridden (1 MiB). */
export const DEFAULT_MAX_BYTES = 1024 * 1024
/** Flush a non-empty queue after at most this many milliseconds unless overridden. */
export const DEFAULT_LINGER_MS = 5
/** The smallest linger the timer runs at. */
export const MIN_LINGER_MS = 1

/** Appends one flushed batch under the handle's partitioning. */
export type BatchSink = (
  records: readonly MessageWithHeaders[],
  partitionKey: Uint8Array | undefined
) => Promise<unknown>

/**
 * Builds a `BatchingProducer`, opened with `topic.batching()`. The batch
 * flushes on whichever of `maxRecords`, `maxBytes`, or `lingerMs` trips first.
 */
export class BatchingProducerBuilder {
  private maxRecordsValue = DEFAULT_MAX_RECORDS
  private maxBytesValue = DEFAULT_MAX_BYTES
  private lingerValue = DEFAULT_LINGER_MS
  private partitionKeyValue: Uint8Array | undefined

  constructor(private readonly sink: BatchSink) {}

  /** Flush once this many records are queued. */
  maxRecords(count: number): this {
    this.maxRecordsValue = Math.max(1, positive(count, "maxRecords"))
    return this
  }

  /** Flush once the queued payload bytes reach this bound. */
  maxBytes(bytes: number): this {
    this.maxBytesValue = Math.max(1, positive(bytes, "maxBytes"))
    return this
  }

  /** Flush a non-empty queue after at most this long, so a trickle of records
   * never waits for a full batch. */
  linger(milliseconds: number): this {
    if (!Number.isFinite(milliseconds) || milliseconds < 0) {
      throw new InvalidError("linger must be a non-negative finite number")
    }
    this.lingerValue = milliseconds
    return this
  }

  /** Pins every record of this handle to one partition key, so ordering within
   * the key is never interleaved. Without a key, each flushed batch is
   * spread by the balanced partitioner. */
  partitionKey(key: string | BytesLike): this {
    this.partitionKeyValue =
      typeof key === "string" ? new TextEncoder().encode(key) : ownedBytes(key)
    return this
  }

  /** Builds the handle and starts its linger timer. */
  build(): BatchingProducer {
    return new BatchingProducer(
      this.sink,
      this.partitionKeyValue,
      this.maxRecordsValue,
      this.maxBytesValue,
      Math.max(MIN_LINGER_MS, this.lingerValue)
    )
  }
}

/**
 * A size-and-time batching publisher over one topic: `send` enqueues, and the
 * queue flushes as one append when `maxRecords`, `maxBytes`, or the linger
 * trips. `flush()` is the guaranteed path, and `close()` flushes and stops the
 * timer.
 *
 * A failed linger flush never stops the timer. Its failure is kept and thrown
 * by the next `flush()` or `close()`, after that call has drained the queue.
 * When several batches fail before the caller asks, the report is one
 * `PublishFailedError` that lists the records of all of them.
 */
export class BatchingProducer implements AsyncDisposable {
  private queue: MessageWithHeaders[] = []
  private payloadBytes = 0
  private flushing: Promise<void> = Promise.resolve()
  private readonly timer: ReturnType<typeof setInterval>
  private closed = false
  private failure: { readonly error: unknown } | undefined

  constructor(
    private readonly sink: BatchSink,
    private readonly partitionKey: Uint8Array | undefined,
    private readonly maxRecords: number,
    private readonly maxBytes: number,
    lingerMs: number
  ) {
    this.timer = setInterval(() => {
      void this.drain("timer")
    }, lingerMs)
    this.timer.unref()
  }

  /** Enqueues one payload with optional headers. Flushes inline when a size
   * bound trips, so backpressure lands on the sender. An error is the failure
   * of that inline flush and lists this record as unconfirmed. A failed linger
   * flush is reported by `flush()` or `close()`, never here. */
  async send(
    payload: BytesLike,
    headers: ReadonlyMap<string, IggyHeaderValue> = new Map()
  ): Promise<void> {
    if (this.closed) throw new InvalidError("send() called after close()")
    const bytes = ownedBytes(payload)
    this.queue.push({ payload: bytes, headers: new Map(headers) })
    this.payloadBytes += bytes.byteLength
    if (this.queue.length >= this.maxRecords || this.payloadBytes >= this.maxBytes) {
      await this.drain("send")
    }
  }

  /** Flushes everything queued as one batch append, then reports the failure
   * an earlier linger flush left, if any. A no-op on an empty queue. */
  flush(): Promise<void> {
    return this.drain("caller")
  }

  // Drains run one at a time, so batches reach the log in queue order. A
  // sender hears its own inline flush. The timer keeps a failure. The caller
  // hears everything kept so far together with its own flush.
  private drain(trigger: "send" | "timer" | "caller"): Promise<void> {
    const next = this.flushing.then(async () => {
      const batch = this.queue
      this.queue = []
      this.payloadBytes = 0
      let sent: { readonly error: unknown } | undefined
      if (batch.length > 0) {
        try {
          await this.sink(batch, this.partitionKey)
        } catch (error) {
          sent = { error }
        }
      }
      if (trigger === "send") {
        if (sent !== undefined) throw sent.error
        return
      }
      if (trigger === "timer") {
        if (sent !== undefined) {
          this.failure =
            this.failure === undefined
              ? sent
              : { error: mergeFailures(this.failure.error, sent.error) }
        }
        return
      }
      const kept = this.failure
      this.failure = undefined
      if (kept !== undefined) {
        throw sent === undefined ? kept.error : mergeFailures(kept.error, sent.error)
      }
      if (sent !== undefined) throw sent.error
    })
    this.flushing = next.catch(() => undefined)
    return next
  }

  /** Flushes, stops the linger timer, and reports a kept linger failure. */
  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    clearInterval(this.timer)
    await this.flush()
  }

  /** Delegates async disposal to `close()`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.close()
  }
}

// One report for every batch that failed: the first cause, with the records
// of the later batch added. A later failure that carries no records gives way
// to the earlier one.
function mergeFailures(earlier: unknown, later: unknown): unknown {
  if (!(earlier instanceof PublishFailedError) || !(later instanceof PublishFailedError)) {
    return earlier
  }
  return new PublishFailedError(
    earlier.stream,
    earlier.topic,
    [...earlier.committed, ...later.committed],
    [...earlier.unconfirmed, ...later.unconfirmed],
    earlier.cause
  )
}

function positive(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new InvalidError(`${name} must be a non-negative safe integer`)
  }
  return value
}

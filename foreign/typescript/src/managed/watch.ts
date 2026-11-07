import { BARE_SCOPE, type ResourceScope } from "../client/resource-scope.js"
import { type CallOptions, cancellable } from "../client/call-options.js"
import type { Capabilities } from "../client/capabilities.js"
import { millis } from "../client/duration.js"
import { CancelledError, CommandExecutionError, UnsupportedError } from "../client/errors.js"
import type { MessageId } from "../types/ids.js"
import { decodeOne, expectMap, field } from "../wire/cbor.js"
import { type ChangeRecord, decodeChangeRecord } from "../wire/change.js"
import { CHANGE_OP_VERSION } from "../wire/codes.js"
import type { Cursor } from "../stream/cursor.js"

// How often a wait reads the feed again while nothing new landed.
const WAIT_INTERVAL_MS = 100

/** One change-feed record with its place on the feed. */
export interface WatchedChange {
  readonly change: ChangeRecord
  /** The feed record's message id. A retried batch publishes a change again
   * under the same id, so a reader drops a copy it has seen. */
  readonly messageId: bigint
  /** Where the record sits on the change feed. */
  readonly position: MessageId
}

/** The change feed a watch reads.
 * @internal */
export interface ChangeFeed {
  replay(): Promise<Cursor>
  tailOffsets(): Promise<ReadonlyMap<number, bigint>>
}

export class Watch {
  private filterIndex: string | undefined
  private startAtEnd = false

  private constructor(
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly openFeed: () => Promise<ChangeFeed>,
    private readonly scope: ResourceScope = BARE_SCOPE
  ) {}

  /** @internal */
  static create(
    getCapabilities: () => Promise<Capabilities>,
    openFeed: () => Promise<ChangeFeed>,
    scope: ResourceScope = BARE_SCOPE
  ): Watch {
    return new Watch(getCapabilities, openFeed, scope)
  }

  /** Keep only advancements of this materialized index, named as the
   * connection's `resourceName` names it. */
  index(index: string): this {
    this.filterIndex = this.scope.name(index)
    return this
  }

  /** Start at the feed's current end, so the reader receives only changes
   * published after it opens. Without it a reader starts at the first
   * retained change. */
  fromEnd(): this {
    this.startAtEnd = true
    return this
  }

  /** Open the reader. Refused with `UnsupportedError` when the deployment
   * does not publish the change feed. */
  async records(options: CallOptions = {}): Promise<WatchReader> {
    const signal = options.signal
    const capabilities = await cancellable(signal, "watch records", this.getCapabilities)
    if (!capabilities.watch) {
      throw new UnsupportedError("the change feed is not published by this deployment", {
        surface: "watch",
        feature: "watch"
      })
    }
    const feed = await cancellable(signal, "watch records", this.openFeed)
    const cursor = await cancellable(signal, "watch records", () => feed.replay())
    const unanchored = new Set<number>()
    if (this.startAtEnd) {
      const tails = await cancellable(signal, "watch records", () => feed.tailOffsets())
      cursor.fromOffsets(tails)
      for (const [partitionId, tail] of tails) if (tail === 0n) unanchored.add(partitionId)
    }
    return WatchReader.create(cursor, this.filterIndex, unanchored)
  }
}

/**
 * A resumable change-feed reader. Each `poll` drains the changes that landed
 * since the last one. Persist `offsets` and seed a fresh reader with
 * `fromOffsets` to resume across restarts.
 */
export class WatchReader {
  private missedRecords = 0n
  private skippedRecords = 0n

  private constructor(
    private cursor: Cursor,
    private readonly filterIndex: string | undefined,
    // Partitions the reader started at the end of while they held nothing,
    // so the first record they get says nothing about records it missed.
    private readonly unanchored: Set<number>
  ) {}

  /** @internal */
  static create(
    cursor: Cursor,
    filterIndex: string | undefined,
    unanchored: ReadonlySet<number> = new Set()
  ): WatchReader {
    return new WatchReader(cursor, filterIndex, new Set(unanchored))
  }

  /** Resume from previously persisted offsets. */
  fromOffsets(offsets: ReadonlyMap<number, bigint>): this {
    this.cursor = this.cursor.fromOffsets(offsets)
    this.unanchored.clear()
    return this
  }

  /** The next offset to read on each partition, the resume state. */
  get offsets(): ReadonlyMap<number, bigint> {
    return this.cursor.offsets
  }

  /** Feed records retention removed before this reader reached them, counted
   * from the offsets it started or resumed at. Non-zero means changes were
   * missed: read the view again instead of trusting the feed. */
  get missed(): bigint {
    return this.missedRecords
  }

  /** Feed records that were not change records and were skipped, for
   * example other traffic on an overridden changes topic. */
  get skipped(): bigint {
    return this.skippedRecords
  }

  /**
   * Drain the changes that landed since the last poll, filtered to the
   * watched index when one was set. A change record of another version
   * stops its partition there: the changes before it are returned, and a
   * poll with nothing else to return fails with a `VersionSkew` command
   * error, so this build never misreads a newer record.
   */
  async poll(options: { readonly signal?: AbortSignal } = {}): Promise<readonly WatchedChange[]> {
    const resumed = this.cursor.offsets
    const records = await this.cursor.pollRecords(options)
    const offsets = new Map(this.cursor.offsets)
    const seen = new Set<number>()
    const stopped = new Map<number, number>()
    const changes: WatchedChange[] = []
    for (const record of records) {
      if (stopped.has(record.partitionId)) continue
      if (!seen.has(record.partitionId)) {
        seen.add(record.partitionId)
        if (!this.unanchored.delete(record.partitionId)) {
          const expected = resumed.get(record.partitionId) ?? 0n
          if (record.offset > expected) this.missedRecords += record.offset - expected
        }
      }
      const version = changeVersion(record.payload)
      if (version === undefined) {
        this.skippedRecords += 1n
        continue
      }
      if (version !== CHANGE_OP_VERSION) {
        stopped.set(record.partitionId, version)
        offsets.set(record.partitionId, record.offset)
        continue
      }
      const change = tryDecodeChangeRecord(record.payload)
      if (change === undefined) {
        this.skippedRecords += 1n
        continue
      }
      if (this.filterIndex !== undefined && change.index !== this.filterIndex) continue
      changes.push({
        change,
        messageId: record.messageId ?? 0n,
        position: { partitionId: record.partitionId, offset: record.offset }
      })
    }
    this.cursor.fromOffsets(offsets)
    const skew = stopped.values().next()
    if (changes.length === 0 && skew.done !== true) {
      const message = `change record version ${String(skew.value)} is not the version ${String(CHANGE_OP_VERSION)} this build reads`
      throw new CommandExecutionError(message, {
        code: { kind: "known", name: "VersionSkew" },
        message
      })
    }
    return changes
  }

  /**
   * `poll`, waiting up to `timeoutMs` for the first change to land. Returns
   * an empty array when none landed in time.
   */
  async pollWithin(
    timeoutMs: number,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<readonly WatchedChange[]> {
    const deadline = Date.now() + millis(timeoutMs, "timeoutMs")
    for (;;) {
      const changes = await this.poll(options)
      const remaining = deadline - Date.now()
      if (changes.length > 0 || remaining <= 0) return changes
      await pause(Math.min(WAIT_INTERVAL_MS, remaining), options.signal)
    }
  }

  /** Every change that landed since the last read, ending once caught up. */
  async *stream(options: { readonly signal?: AbortSignal } = {}): AsyncGenerator<WatchedChange> {
    for (;;) {
      const batch = await this.poll(options)
      if (batch.length === 0) return
      for (const change of batch) yield change
    }
  }
}

// The `v` of a change-feed record, undefined when the payload is no named
// record at all.
function changeVersion(payload: Uint8Array): number | undefined {
  try {
    const map = expectMap(decodeOne(payload, "change record"), "change record")
    return field.requiredU32(map, "v", "change record")
  } catch {
    return undefined
  }
}

function tryDecodeChangeRecord(payload: Uint8Array): ChangeRecord | undefined {
  try {
    const map = expectMap(decodeOne(payload, "change record"), "change record")
    return decodeChangeRecord(map, "change record")
  } catch {
    return undefined
  }
}

function pause(ms: number, signal: AbortSignal | undefined): Promise<void> {
  if (signal?.aborted === true) {
    return Promise.reject(new CancelledError("change feed wait aborted", { cause: signal.reason }))
  }
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", abort)
      resolve()
    }, ms)
    const abort = (): void => {
      clearTimeout(timer)
      reject(new CancelledError("change feed wait aborted", { cause: signal?.reason }))
    }
    signal?.addEventListener("abort", abort, { once: true })
  })
}

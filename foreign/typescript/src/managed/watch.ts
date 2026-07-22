import type { Capabilities } from "../client/capabilities.js"
import { UnsupportedError } from "../client/errors.js"
import { decodeOne, expectMap } from "../wire/cbor.js"
import { type ChangeRecord, decodeChangeRecord } from "../wire/change.js"
import type { Cursor, CursorOptions } from "../stream/cursor.js"

function tryDecodeChangeRecord(payload: Uint8Array): ChangeRecord | undefined {
  try {
    const map = expectMap(decodeOne(payload, "change record"), "change record")
    return decodeChangeRecord(map, "change record")
  } catch {
    return undefined
  }
}

// The change-feed accessor: `ChangeRecord`s the deployment publishes after
// each committed projector batch for a binding that opted into `notify`.
// "Query after my data landed" becomes await-then-query instead of
// sleep-and-retry, and it composes with read-your-writes rather than
// replacing it. Build it with `Laser.watch()`. Gated on the `watch`
// capability: refused typed when the deployment does not publish the feed.
export class Watch {
  private filterIndex: string | undefined

  constructor(
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly openCursor: (options?: CursorOptions) => Promise<Cursor>
  ) {}

  // Keep only advancements of this materialized index.
  index(index: string): this {
    this.filterIndex = index
    return this
  }

  // Open the feed reader: the existing `Cursor` over the changes topic on
  // the ops stream, decoding each record and applying the index filter
  // client-side. No new consumption machinery, the feed is ordinary
  // records consumed by offset.
  async records(): Promise<WatchReader> {
    const capabilities = await this.getCapabilities()
    if (!capabilities.watch) {
      throw new UnsupportedError("the change feed is not published by this deployment", {
        cause: { surface: "watch", feature: "watch" }
      })
    }
    const cursor = await this.openCursor()
    return new WatchReader(cursor, this.filterIndex)
  }
}

// A resumable change-feed reader. Each `.poll()` drains the records that
// landed since the last one. Persist `.offsets` and seed a fresh reader
// (`.fromOffsets`) to resume across restarts, exactly like any `Cursor`.
export class WatchReader {
  constructor(
    private cursor: Cursor,
    private readonly filterIndex: string | undefined
  ) {}

  // Resume from a previous reader's offsets.
  fromOffsets(offsets: ReadonlyMap<number, bigint>): this {
    this.cursor = this.cursor.fromOffsets(offsets)
    return this
  }

  // The per-partition offsets consumed so far, the resume state.
  get offsets(): ReadonlyMap<number, bigint> {
    return this.cursor.offsets
  }

  // Drain the change records that landed since the last poll, filtered to
  // the watched index when one was set. A record that does not decode as a
  // `ChangeRecord` is skipped: the changes topic defaults to
  // `CHANGES_TOPIC` but is overridable, and a misbehaving deployment
  // publishing other traffic on it must not wedge the reader either way.
  async poll(): Promise<readonly ChangeRecord[]> {
    const messages = await this.cursor.poll()
    const records: ChangeRecord[] = []
    for (const message of messages) {
      const record = tryDecodeChangeRecord(message.payload)
      if (record === undefined) continue
      if (this.filterIndex !== undefined && record.index !== this.filterIndex) continue
      records.push(record)
    }
    return records
  }

  // This reader as an async iterable of change records, one at a time,
  // draining what landed since the last read and ending once caught up
  // (unlike `Cursor.stream()`, which polls forever on an interval).
  async *stream(): AsyncGenerator<ChangeRecord> {
    for (;;) {
      const batch = await this.poll()
      if (batch.length === 0) return
      for (const record of batch) yield record
    }
  }
}

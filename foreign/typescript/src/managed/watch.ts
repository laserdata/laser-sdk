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

export class Watch {
  private filterIndex: string | undefined

  constructor(
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly openCursor: (options?: CursorOptions) => Promise<Cursor>
  ) {}

  index(index: string): this {
    this.filterIndex = index
    return this
  }

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

export class WatchReader {
  constructor(
    private cursor: Cursor,
    private readonly filterIndex: string | undefined
  ) {}

  fromOffsets(offsets: ReadonlyMap<number, bigint>): this {
    this.cursor = this.cursor.fromOffsets(offsets)
    return this
  }

  get offsets(): ReadonlyMap<number, bigint> {
    return this.cursor.offsets
  }

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

  async *stream(): AsyncGenerator<ChangeRecord> {
    for (;;) {
      const batch = await this.poll()
      if (batch.length === 0) return
      for (const record of batch) yield record
    }
  }
}

const DOUBLE = 0x01
const STRING = 0x02
const DOCUMENT = 0x03
const ARRAY = 0x04
const BINARY = 0x05
const BOOLEAN = 0x08
const NULL = 0x0a
const INT32 = 0x10
const INT64 = 0x12
const BINARY_OLD = 0x02
const MAX_DEPTH = 100
const I64_MIN = -(1n << 63n)
const I64_MAX = (1n << 63n) - 1n

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder("utf-8", { fatal: true })

// Encodes the way the Rust and Python clients do through serde: every integer
// is an Int64, every other number a Double, bytes a generic Binary.
export function encodeBsonDocument(value: unknown): Uint8Array {
  const entries = documentEntries(value)
  if (entries === undefined) throw new TypeError("a BSON body must be a document")
  const out: number[] = []
  writeDocument(out, entries, 0)
  return Uint8Array.from(out)
}

export function decodeBsonDocument(payload: Uint8Array): Record<string, unknown> {
  const reader = new Reader(payload)
  const document = reader.document(0, false) as Record<string, unknown>
  if (reader.offset !== payload.length) throw new RangeError("BSON payload has trailing bytes")
  return document
}

function documentEntries(value: unknown): [string, unknown][] | undefined {
  if (value instanceof Map) {
    const entries = [...value.entries()] as [unknown, unknown][]
    if (!entries.every(([key]) => typeof key === "string"))
      throw new TypeError("BSON document keys must be strings")
    return entries as [string, unknown][]
  }
  if (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    !ArrayBuffer.isView(value) &&
    !(value instanceof Date)
  ) {
    return Object.entries(value).filter(([, nested]) => nested !== undefined)
  }
  return undefined
}

function writeDocument(out: number[], entries: [string, unknown][], depth: number): void {
  if (depth > MAX_DEPTH) throw new TypeError("BSON value exceeds the maximum depth")
  const start = out.length
  writeI32(out, 0)
  for (const [key, value] of entries) writeElement(out, key, value, depth)
  out.push(0)
  patchI32(out, start, out.length - start)
}

function writeElement(out: number[], key: string, value: unknown, depth: number): void {
  const tag = out.length
  out.push(0)
  writeCString(out, key)
  out[tag] = writeValue(out, value, depth)
}

function writeValue(out: number[], value: unknown, depth: number): number {
  if (value === null) return NULL
  if (typeof value === "boolean") {
    out.push(value ? 1 : 0)
    return BOOLEAN
  }
  if (typeof value === "number") {
    if (Number.isSafeInteger(value)) {
      writeI64(out, BigInt(value))
      return INT64
    }
    writeF64(out, value)
    return DOUBLE
  }
  if (typeof value === "bigint") {
    if (value < I64_MIN || value > I64_MAX)
      throw new RangeError("BSON integers must fit in signed 64 bits")
    writeI64(out, value)
    return INT64
  }
  if (typeof value === "string") {
    const bytes = textEncoder.encode(value)
    writeI32(out, bytes.length + 1)
    for (const byte of bytes) out.push(byte)
    out.push(0)
    return STRING
  }
  if (value instanceof Uint8Array) {
    writeI32(out, value.length)
    out.push(0)
    for (const byte of value) out.push(byte)
    return BINARY
  }
  if (Array.isArray(value)) {
    writeDocument(
      out,
      value.map((item, index): [string, unknown] => [String(index), item ?? null]),
      depth + 1
    )
    return ARRAY
  }
  const entries = documentEntries(value)
  if (entries === undefined) throw new TypeError(`${typeof value} has no BSON form`)
  writeDocument(out, entries, depth + 1)
  return DOCUMENT
}

function writeCString(out: number[], text: string): void {
  const bytes = textEncoder.encode(text)
  if (bytes.includes(0)) throw new TypeError("BSON keys must not contain a NUL byte")
  for (const byte of bytes) out.push(byte)
  out.push(0)
}

function writeI32(out: number[], value: number): void {
  out.push(value & 0xff, (value >>> 8) & 0xff, (value >>> 16) & 0xff, (value >>> 24) & 0xff)
}

function patchI32(out: number[], at: number, value: number): void {
  out[at] = value & 0xff
  out[at + 1] = (value >>> 8) & 0xff
  out[at + 2] = (value >>> 16) & 0xff
  out[at + 3] = (value >>> 24) & 0xff
}

function writeI64(out: number[], value: bigint): void {
  const view = new DataView(new ArrayBuffer(8))
  view.setBigInt64(0, value, true)
  for (let index = 0; index < 8; index += 1) out.push(view.getUint8(index))
}

function writeF64(out: number[], value: number): void {
  const view = new DataView(new ArrayBuffer(8))
  view.setFloat64(0, value, true)
  for (let index = 0; index < 8; index += 1) out.push(view.getUint8(index))
}

class Reader {
  offset = 0
  private readonly view: DataView

  constructor(private readonly bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  }

  document(depth: number, array: boolean): Record<string, unknown> | unknown[] {
    if (depth > MAX_DEPTH) throw new RangeError("BSON value exceeds the maximum depth")
    const start = this.offset
    const length = this.i32()
    const end = start + length
    if (length < 5 || end > this.bytes.length)
      throw new RangeError("BSON document length is invalid")
    const object: Record<string, unknown> = {}
    const items: unknown[] = []
    while (this.offset < end - 1) {
      const tag = this.u8()
      const key = this.cstring()
      const value = this.value(tag, depth)
      if (array) items.push(value)
      else object[key] = value
    }
    if (this.offset !== end - 1 || this.u8() !== 0)
      throw new RangeError("BSON document is not terminated")
    return array ? items : object
  }

  private value(tag: number, depth: number): unknown {
    switch (tag) {
      case DOUBLE:
        return this.view.getFloat64(this.take(8), true)
      case STRING: {
        const length = this.i32()
        if (length < 1) throw new RangeError("BSON string length is invalid")
        const start = this.take(length)
        if (this.bytes[start + length - 1] !== 0)
          throw new RangeError("BSON string is not terminated")
        return textDecoder.decode(this.bytes.subarray(start, start + length - 1))
      }
      case DOCUMENT:
        return this.document(depth + 1, false)
      case ARRAY:
        return this.document(depth + 1, true)
      case BINARY: {
        const length = this.i32()
        if (length < 0) throw new RangeError("BSON binary length is invalid")
        const subtype = this.u8()
        if (subtype === BINARY_OLD) {
          const inner = this.i32()
          if (inner !== length - 4) throw new RangeError("BSON binary length is invalid")
          const start = this.take(inner)
          return this.bytes.slice(start, start + inner)
        }
        const start = this.take(length)
        return this.bytes.slice(start, start + length)
      }
      case BOOLEAN: {
        const byte = this.u8()
        if (byte > 1) throw new RangeError("BSON boolean must be 0 or 1")
        return byte === 1
      }
      case NULL:
        return null
      case INT32:
        return this.view.getInt32(this.take(4), true)
      case INT64: {
        const value = this.view.getBigInt64(this.take(8), true)
        return value >= BigInt(Number.MIN_SAFE_INTEGER) && value <= BigInt(Number.MAX_SAFE_INTEGER)
          ? Number(value)
          : value
      }
      default:
        throw new RangeError(`BSON element type 0x${tag.toString(16)} has no plain value form`)
    }
  }

  private cstring(): string {
    const end = this.bytes.indexOf(0, this.offset)
    if (end < 0) throw new RangeError("BSON key is not terminated")
    const text = textDecoder.decode(this.bytes.subarray(this.offset, end))
    this.offset = end + 1
    return text
  }

  private i32(): number {
    return this.view.getInt32(this.take(4), true)
  }

  private u8(): number {
    return this.view.getUint8(this.take(1))
  }

  private take(length: number): number {
    const start = this.offset
    if (start + length > this.bytes.length) throw new RangeError("BSON payload is truncated")
    this.offset += length
    return start
  }
}

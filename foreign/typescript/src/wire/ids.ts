import { InvalidError } from "../client/errors.js"

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
const U128_MAX = (1n << 128n) - 1n

export function crockfordEncode(value: bigint): string {
  if (value < 0n || value > U128_MAX) {
    throw new InvalidError("id value must fit in 128 bits", { value: value.toString() })
  }
  const out = new Array<string>(26)
  let remaining = value
  for (let i = 25; i >= 0; i -= 1) {
    out[i] = CROCKFORD.charAt(Number(remaining & 0x1fn))
    remaining >>= 5n
  }
  return out.join("")
}

export function crockfordDecode(text: string): bigint {
  if (text.length !== 26) {
    throw new InvalidError(`id must be 26 characters, got ${String(text.length)}`, {
      got: text.length
    })
  }
  let value = 0n
  for (let i = 0; i < 26; i += 1) {
    const char = text[i]?.toUpperCase()
    const digit = char === undefined ? -1 : CROCKFORD.indexOf(char)
    if (digit === -1) {
      throw new InvalidError(`id contains invalid character \`${text[i] ?? ""}\``, {
        char: text[i]
      })
    }
    if (i === 0 && digit > 7) {
      throw new InvalidError("id overflows 128 bits", { text })
    }
    value = (value << 5n) | BigInt(digit)
  }
  return value
}

export function bigIntToBytes16(value: bigint): Uint8Array {
  if (value < 0n || value > U128_MAX) {
    throw new InvalidError("id value must fit in 128 bits", { value: value.toString() })
  }
  const bytes = new Uint8Array(16)
  let remaining = value
  for (let i = 15; i >= 0; i -= 1) {
    bytes[i] = Number(remaining & 0xffn)
    remaining >>= 8n
  }
  return bytes
}

export function bytes16ToBigInt(bytes: Uint8Array): bigint {
  if (bytes.length !== 16) {
    throw new InvalidError(`id bytes must be 16 bytes, got ${String(bytes.length)}`, {
      got: bytes.length
    })
  }
  let value = 0n
  for (const byte of bytes) {
    value = (value << 8n) | BigInt(byte)
  }
  return value
}

const LOG_POSITION_BYTES = 20

export interface LogPosition {
  readonly streamId: number
  readonly topicId: number
  readonly partitionId: number
  readonly offset: bigint
}

export function logPositionToBytes(position: LogPosition): Uint8Array {
  const bytes = new Uint8Array(LOG_POSITION_BYTES)
  const view = new DataView(bytes.buffer)
  view.setUint32(0, position.streamId, false)
  view.setUint32(4, position.topicId, false)
  view.setUint32(8, position.partitionId, false)
  view.setBigUint64(12, position.offset, false)
  return bytes
}

export function logPositionFromBytes(bytes: Uint8Array): LogPosition {
  if (bytes.length !== LOG_POSITION_BYTES) {
    throw new InvalidError(`log position must be 20 bytes, got ${String(bytes.length)}`, {
      got: bytes.length
    })
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  return {
    streamId: view.getUint32(0, false),
    topicId: view.getUint32(4, false),
    partitionId: view.getUint32(8, false),
    offset: view.getBigUint64(12, false)
  }
}

export abstract class WireId<Brand extends string> {
  declare protected readonly brand: Brand

  protected constructor(private readonly value: bigint) {}

  asU128(): bigint {
    return this.value
  }

  toBytes(): Uint8Array {
    return bigIntToBytes16(this.value)
  }

  toString(): string {
    return crockfordEncode(this.value)
  }

  equals(other: WireId<Brand>): boolean {
    return other.value === this.value
  }
}

function checkedU128(name: string, value: bigint): bigint {
  if (value < 0n || value > U128_MAX) {
    throw new InvalidError(`${name} value must fit in 128 bits`, { value: value.toString() })
  }
  return value
}

export class RecordId extends WireId<"RecordId"> {
  private constructor(value: bigint) {
    super(value)
  }

  static fromU128(value: bigint): RecordId {
    return new RecordId(checkedU128("RecordId", value))
  }

  static fromBytes(bytes: Uint8Array): RecordId {
    return RecordId.fromU128(bytes16ToBigInt(bytes))
  }

  static parse(text: string): RecordId {
    return RecordId.fromU128(crockfordDecode(text))
  }

  static tryParse(text: string): RecordId | undefined {
    try {
      return RecordId.parse(text)
    } catch {
      return undefined
    }
  }
}

export class ConversationId extends WireId<"ConversationId"> {
  private constructor(value: bigint) {
    super(value)
  }

  static fromU128(value: bigint): ConversationId {
    return new ConversationId(checkedU128("ConversationId", value))
  }

  static fromBytes(bytes: Uint8Array): ConversationId {
    return ConversationId.fromU128(bytes16ToBigInt(bytes))
  }

  static parse(text: string): ConversationId {
    return ConversationId.fromU128(crockfordDecode(text))
  }

  static tryParse(text: string): ConversationId | undefined {
    try {
      return ConversationId.parse(text)
    } catch {
      return undefined
    }
  }
}

export class CorrelationId extends WireId<"CorrelationId"> {
  private constructor(value: bigint) {
    super(value)
  }

  static fromU128(value: bigint): CorrelationId {
    return new CorrelationId(checkedU128("CorrelationId", value))
  }

  static fromBytes(bytes: Uint8Array): CorrelationId {
    return CorrelationId.fromU128(bytes16ToBigInt(bytes))
  }

  static parse(text: string): CorrelationId {
    return CorrelationId.fromU128(crockfordDecode(text))
  }

  static tryParse(text: string): CorrelationId | undefined {
    try {
      return CorrelationId.parse(text)
    } catch {
      return undefined
    }
  }
}

export class ChannelId extends WireId<"ChannelId"> {
  private constructor(value: bigint) {
    super(value)
  }

  static fromU128(value: bigint): ChannelId {
    return new ChannelId(checkedU128("ChannelId", value))
  }

  static fromBytes(bytes: Uint8Array): ChannelId {
    return ChannelId.fromU128(bytes16ToBigInt(bytes))
  }

  static parse(text: string): ChannelId {
    return ChannelId.fromU128(crockfordDecode(text))
  }

  static tryParse(text: string): ChannelId | undefined {
    try {
      return ChannelId.parse(text)
    } catch {
      return undefined
    }
  }
}

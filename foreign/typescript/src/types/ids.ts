import { InvalidError } from "../client/errors.js"
import { mintUlidValue, type UlidSource } from "../runtime/ulid.js"
import { crockfordDecode, crockfordEncode } from "../wire/ids.js"

const MAX_ID_LEN = 255

const DERIVE_VERSION = 1
const FNV_OFFSET = 0xcbf2_9ce4_8422_2325n
const FNV_PRIME = 0x0000_0100_0000_01b3n
const MASK_64 = (1n << 64n) - 1n

function hashWith(salt: number, seed: string): bigint {
  let hash = FNV_OFFSET
  for (const byte of [DERIVE_VERSION, salt, ...new TextEncoder().encode(seed)]) {
    hash ^= BigInt(byte)
    hash = (hash * FNV_PRIME) & MASK_64
  }
  return hash
}

export class ConversationId {
  private constructor(private readonly value: bigint) {}

  static new(source?: UlidSource): ConversationId {
    return new ConversationId(mintUlidValue(source))
  }

  static derive(seed: string): ConversationId {
    const high = hashWith(0x1d, seed)
    const low = hashWith(0x9e, seed)
    return new ConversationId((high << 64n) | low)
  }

  static parse(text: string): ConversationId {
    try {
      return new ConversationId(crockfordDecode(text))
    } catch (cause) {
      throw new InvalidError(`invalid ULID \`${text}\``, { text }, { cause })
    }
  }

  toString(): string {
    return crockfordEncode(this.value)
  }

  equals(other: ConversationId): boolean {
    return other.value === this.value
  }
}

export class IntentId {
  private constructor(private readonly value: bigint) {}

  static new(source?: UlidSource): IntentId {
    return new IntentId(mintUlidValue(source))
  }

  static parse(text: string): IntentId {
    try {
      return new IntentId(crockfordDecode(text))
    } catch (cause) {
      throw new InvalidError(`invalid ULID \`${text}\``, { text }, { cause })
    }
  }

  toString(): string {
    return crockfordEncode(this.value)
  }

  equals(other: IntentId): boolean {
    return other.value === this.value
  }
}

function validateId(name: string): void {
  if (name.length === 0) {
    throw new InvalidError("identifier must not be empty")
  }
  const bytes = new TextEncoder().encode(name)
  if (bytes.length > MAX_ID_LEN) {
    throw new InvalidError(
      `identifier length ${String(bytes.length)}B exceeds max ${String(MAX_ID_LEN)}B`
    )
  }
  for (const char of name) {
    const codePoint = char.codePointAt(0) ?? 0
    if (codePoint <= 0x1f || (codePoint >= 0x7f && codePoint <= 0x9f)) {
      throw new InvalidError(`identifier contains invalid character \`${char}\``)
    }
  }
}

export class AgentId {
  private constructor(private readonly value: string) {}

  static new(name: string): AgentId {
    validateId(name)
    return new AgentId(name)
  }

  asString(): string {
    return this.value
  }

  toString(): string {
    return this.value
  }

  equals(other: AgentId): boolean {
    return other.value === this.value
  }
}

export class ConsumerGroupName {
  private constructor(private readonly value: string) {}

  static new(name: string): ConsumerGroupName {
    validateId(name)
    return new ConsumerGroupName(name)
  }

  static forAgent(agent: AgentId): ConsumerGroupName {
    return new ConsumerGroupName(agent.asString())
  }

  asString(): string {
    return this.value
  }

  toString(): string {
    return this.value
  }
}

export class PrincipalId {
  private constructor(private readonly value: number) {}

  static new(value: number): PrincipalId {
    if (!Number.isSafeInteger(value) || value < 0 || value > 0xffff_ffff) {
      throw new InvalidError("principal id must be an unsigned 32-bit integer", { value })
    }
    return new PrincipalId(value)
  }

  get(): number {
    return this.value
  }

  toString(): string {
    return String(this.value)
  }
}

export interface MessageId {
  readonly partitionId: number
  readonly offset: bigint
}

export function messageIdToString(id: MessageId): string {
  return `${String(id.partitionId)}:${String(id.offset)}`
}

function isCanonicalDigits(s: string): boolean {
  if (s.length === 0) return false
  if (s.length > 1 && s.startsWith("0")) return false
  return /^[0-9]+$/.test(s)
}

export function parseMessageId(text: string): MessageId {
  const separator = text.indexOf(":")
  if (separator === -1) {
    throw new InvalidError(`invalid message id \`${text}\`, expected \`<partition_id>:<offset>\``)
  }
  const partition = text.slice(0, separator)
  const offset = text.slice(separator + 1)
  if (!isCanonicalDigits(partition) || !isCanonicalDigits(offset)) {
    throw new InvalidError(`invalid message id \`${text}\`, expected \`<partition_id>:<offset>\``)
  }
  return { partitionId: Number(partition), offset: BigInt(offset) }
}

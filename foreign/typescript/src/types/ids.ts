import { InvalidError } from "../client/errors.js"
import { mintUlidValue, type UlidSource } from "../runtime/ulid.js"
import { crockfordDecode, crockfordEncode } from "../wire/ids.js"

const MAX_ID_LEN = 255

// Versioned FNV-1a so derived ids stay stable across releases. Bumping
// DERIVE_VERSION deliberately remaps every derived conversation id.
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

// A conversation: the unit of ordering and causality. One conversation maps
// to one Iggy partition, so all its messages share a total order. Created
// fresh (time-ordered ULID) with `.new()`, or derived deterministically
// from a seed with `.derive()`.
//
// Distinct from, and unrelated to, `wire/ids.ts`'s `ConversationId` (a
// 16-byte content-addressed wire id used inside the AGDX envelope). Rust
// keeps them apart by crate (`laser_sdk::types::ConversationId` here vs.
// `laser_wire::agent::ConversationId` there); this is the public,
// ULID-based one every session/provenance API actually uses, so it owns
// the unqualified name in this package. The wire crate's envelope id is
// not currently exported from the root; if it ever needs to be, it must
// take a distinguishing name to avoid the collision.
export class ConversationId {
  private constructor(private readonly value: bigint) {}

  // A fresh, random conversation id (a time-ordered ULID).
  static new(source?: UlidSource): ConversationId {
    return new ConversationId(mintUlidValue(source))
  }

  // Derives a stable conversation id from a seed (e.g. a user identity).
  // The same seed always yields the same id, giving per-seed ordering and
  // isolation without coordination. Used by a per-user session policy.
  static derive(seed: string): ConversationId {
    const high = hashWith(0x1d, seed)
    const low = hashWith(0x9e, seed)
    return new ConversationId((high << 64n) | low)
  }

  // Parses a conversation id from its canonical ULID text, or throws
  // `InvalidError`.
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

// A durable intent's id (a fresh, time-ordered ULID), naming one proposed
// effect across its intent/vote/decision records.
export class IntentId {
  private constructor(private readonly value: bigint) {}

  // A fresh, random intent id (a time-ordered ULID).
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

// Non-empty, at most `MAX_ID_LEN` UTF-8 bytes, and free of ASCII control
// characters (0x00-0x1F, 0x7F) and the C1 control range (0x80-0x9F).
// Rust's `char::is_control` covers both. Shared by `AgentId` and
// `ConsumerGroupName`.
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

// An agent's stable logical name. Almost any string: the only rules are
// that it is non-empty, at most 255 bytes, and free of control characters
// because it rides as a message header value. So plain labels
// (`planner`), email-like federated identities (`planner@acme.example`),
// URLs, and namespaced names (`team/planner`) are all valid.
export class AgentId {
  private constructor(private readonly value: string) {}

  // Build an agent id from any string that satisfies the rules, or throws
  // `InvalidError` saying which rule it broke.
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

// An Apache Iggy consumer-group name. This is deployment topology,
// distinct from the logical `AgentId`: replicas of one agent commonly
// share a group, while non-agent workers also use groups.
export class ConsumerGroupName {
  private constructor(private readonly value: string) {}

  // Validate a non-empty Iggy group name that fits the 255-byte name
  // limit.
  static new(name: string): ConsumerGroupName {
    validateId(name)
    return new ConsumerGroupName(name)
  }

  // Use the logical agent spelling as its default deployment group.
  // Infallible by a deliberate invariant: a group name today has exactly
  // an agent id's rules, so every valid id is a valid group.
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

// The numeric identity of an authenticated Apache Iggy principal. A
// semantic tag, not a proof: anyone can construct one client-side, and the
// trust anchor is always the server-stamped user id a connection
// authenticated as. The type exists so principal-scoped APIs (presence,
// bindings, RBAC) cannot silently accept an arbitrary integer that was
// never meant as a principal.
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

// A message's position on the log: its partition and offset. Stamped on a
// reply as the causal parent so a flow's causality is walkable. Displays
// as `<partitionId>:<offset>`.
export interface MessageId {
  readonly partitionId: number
  readonly offset: bigint
}

export function messageIdToString(id: MessageId): string {
  return `${String(id.partitionId)}:${String(id.offset)}`
}

// Canonical: non-empty, all ASCII digits, no leading zero except for "0"
// itself. This is exactly what a formatted u32/u64 produces, so the
// invariant equals `format(parse(s)) === s` without a full round trip.
function isCanonicalDigits(s: string): boolean {
  if (s.length === 0) return false
  if (s.length > 1 && s.startsWith("0")) return false
  return /^[0-9]+$/.test(s)
}

// Parses `<partitionId>:<offset>`, or throws `InvalidError`.
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

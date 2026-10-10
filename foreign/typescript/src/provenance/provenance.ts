import { IdError, ProvenanceError } from "../client/errors.js"
import type { HeaderValue } from "../stream/header-value.js"
import {
  AgentId,
  ConversationId,
  type MessageId,
  messageIdToString,
  parseMessageId
} from "../types/ids.js"
import {
  AGENT_ID,
  BROADCAST,
  CAUSAL_PARENT,
  CONVERSATION_ID,
  CORRELATION_ID,
  COST_USD,
  DEADLINE,
  FENCE,
  HEADER_FRAMING_BYTES,
  HEADER_SOFT_CAP,
  HEADER_VALUE_MAX,
  IDEMPOTENCY_KEY,
  PARENT_CONVERSATION_ID,
  ROOT_CONVERSATION_ID,
  TARGET_AGENT_ID,
  USAGE_INPUT_TOKENS,
  USAGE_OUTPUT_TOKENS,
  encodeRecordHeaders,
  type HeaderField
} from "../wire/headers.js"

export interface LlmUsage {
  readonly inputTokens?: bigint
  readonly outputTokens?: bigint
  readonly costUsd?: number
}

export interface Provenance {
  readonly conversationId: ConversationId
  readonly causalParent?: MessageId
  readonly parentConversationId?: ConversationId
  readonly rootConversationId?: ConversationId
  readonly agent?: AgentId
  readonly targetAgentId?: AgentId
  readonly usage?: LlmUsage
  readonly deadlineMicros?: bigint
  readonly idempotencyKey?: string
  readonly correlationId?: string
  readonly fenceToken?: bigint
}

export function provenancePartitionKey(provenance: Provenance): string {
  return provenance.conversationId.toString()
}

function putHeader(map: Map<string, HeaderValue>, key: string, value: string): void {
  if (value.length === 0) {
    throw ProvenanceError.emptyValue(key)
  }
  const bytes = new TextEncoder().encode(value)
  if (bytes.length > HEADER_VALUE_MAX) {
    throw ProvenanceError.valueTooLong(key, bytes.length, HEADER_VALUE_MAX)
  }
  for (const byte of bytes) {
    if (byte < 0x20 || byte === 0x7f) {
      throw ProvenanceError.invalidValueBytes(key)
    }
  }
  map.set(key, { kind: "string", value })
}

export function encodeProvenanceHeaders(provenance: Provenance): ReadonlyMap<string, HeaderValue> {
  const block = encodeRecordHeaders({
    conversation: provenance.conversationId.toString(),
    ...(provenance.parentConversationId !== undefined
      ? { parent: provenance.parentConversationId.toString() }
      : {}),
    ...(provenance.rootConversationId !== undefined
      ? { root: provenance.rootConversationId.toString() }
      : {}),
    ...(provenance.causalParent !== undefined
      ? { causalParent: messageIdToString(provenance.causalParent) }
      : {}),
    ...(provenance.agent !== undefined ? { agent: provenance.agent.asStr() } : {}),
    ...(provenance.targetAgentId !== undefined
      ? { addressee: { kind: "agent" as const, agent: provenance.targetAgentId.asStr() } }
      : {}),
    ...(provenance.idempotencyKey !== undefined
      ? { idempotencyKey: provenance.idempotencyKey }
      : {}),
    ...(provenance.correlationId !== undefined ? { correlation: provenance.correlationId } : {}),
    ...(provenance.fenceToken !== undefined ? { fence: provenance.fenceToken } : {}),
    ...(provenance.deadlineMicros !== undefined
      ? { deadlineMicros: provenance.deadlineMicros }
      : {}),
    ...(provenance.usage?.inputTokens !== undefined
      ? { inputTokens: provenance.usage.inputTokens }
      : {}),
    ...(provenance.usage?.outputTokens !== undefined
      ? { outputTokens: provenance.usage.outputTokens }
      : {}),
    ...(provenance.usage?.costUsd !== undefined ? { costUsd: provenance.usage.costUsd } : {})
  })
  return headerMap(block)
}

/** The Iggy header map of one record's header block, checked against the
 * value and soft size caps. */
export function headerMap(
  block: readonly (readonly [string, HeaderField])[]
): ReadonlyMap<string, HeaderValue> {
  const map = new Map<string, HeaderValue>()
  let size = 0
  for (const [key, field] of block) {
    let bytes = 0
    switch (field.kind) {
      case "text":
        putHeader(map, key, field.value)
        bytes = new TextEncoder().encode(field.value).length
        break
      case "uint8":
        map.set(key, { kind: "uint8", value: field.value })
        bytes = 1
        break
      case "uint32":
        map.set(key, { kind: "uint32", value: field.value })
        bytes = 4
        break
      case "uint64":
        map.set(key, { kind: "uint64", value: field.value })
        bytes = 8
        break
      case "float64":
        if (!Number.isFinite(field.value)) throw ProvenanceError.nonFinite(key)
        map.set(key, { kind: "double", value: field.value })
        bytes = 8
        break
    }
    size += new TextEncoder().encode(key).length + bytes + HEADER_FRAMING_BYTES
  }
  if (size > HEADER_SOFT_CAP) {
    throw ProvenanceError.tooLarge(size, HEADER_SOFT_CAP)
  }
  return map
}

// An id header that does not parse fails provenance decode with the id
// error as its cause, like the Rust `ProvenanceError::Id` conversion.
function parseId<T>(parse: () => T): T {
  try {
    return parse()
  } catch (cause) {
    if (cause instanceof IdError) throw ProvenanceError.id(cause)
    throw cause
  }
}

function strValue(value: HeaderValue, key: string): string {
  if (value.kind !== "string") {
    throw ProvenanceError.invalidValue(key)
  }
  return value.value
}

// A malformed number is a decode error, never dropped: a silently missing fence
// would skip the gate and let a tampered record through as unfenced.
function uint64Value(value: HeaderValue, key: string): bigint {
  if (value.kind !== "uint64") throw ProvenanceError.invalidValue(key)
  return value.value
}

export function decodeProvenanceHeaders(headers: ReadonlyMap<string, HeaderValue>): Provenance {
  let conversationId: ConversationId | undefined
  let causalParent: MessageId | undefined
  let parentConversationId: ConversationId | undefined
  let rootConversationId: ConversationId | undefined
  let agent: AgentId | undefined
  let targetAgentId: AgentId | undefined
  let idempotencyKey: string | undefined
  let correlationId: string | undefined
  let fenceToken: bigint | undefined
  let deadlineMicros: bigint | undefined
  let inputTokens: bigint | undefined
  let outputTokens: bigint | undefined
  let costUsd: number | undefined
  let hasUsage = false

  for (const [key, value] of headers) {
    switch (key) {
      case CONVERSATION_ID:
        conversationId = parseId(() => ConversationId.parse(strValue(value, key)))
        break
      case CAUSAL_PARENT:
        causalParent = parseId(() => parseMessageId(strValue(value, key)))
        break
      case PARENT_CONVERSATION_ID:
        parentConversationId = parseId(() => ConversationId.parse(strValue(value, key)))
        break
      case ROOT_CONVERSATION_ID:
        rootConversationId = parseId(() => ConversationId.parse(strValue(value, key)))
        break
      case AGENT_ID:
        agent = parseId(() => AgentId.new(strValue(value, key)))
        break
      case TARGET_AGENT_ID: {
        // Broadcast `*` addresses every agent and is never an agent id.
        const target = strValue(value, key)
        if (target !== BROADCAST) targetAgentId = parseId(() => AgentId.new(target))
        break
      }
      case IDEMPOTENCY_KEY:
        idempotencyKey = strValue(value, key)
        break
      case CORRELATION_ID:
        correlationId = strValue(value, key)
        break
      case FENCE:
        fenceToken = uint64Value(value, key)
        break
      case DEADLINE:
        deadlineMicros = uint64Value(value, key)
        break
      case USAGE_INPUT_TOKENS:
        inputTokens = uint64Value(value, key)
        hasUsage = true
        break
      case USAGE_OUTPUT_TOKENS:
        outputTokens = uint64Value(value, key)
        hasUsage = true
        break
      case COST_USD:
        if (value.kind !== "double") throw ProvenanceError.invalidValue(key)
        if (!Number.isFinite(value.value)) throw ProvenanceError.nonFinite(key)
        costUsd = value.value
        hasUsage = true
        break
      default:
        break
    }
  }

  if (conversationId === undefined) {
    throw ProvenanceError.missingRequired(CONVERSATION_ID)
  }

  return {
    conversationId,
    ...(causalParent !== undefined ? { causalParent } : {}),
    ...(parentConversationId !== undefined ? { parentConversationId } : {}),
    ...(rootConversationId !== undefined ? { rootConversationId } : {}),
    ...(agent !== undefined ? { agent } : {}),
    ...(targetAgentId !== undefined ? { targetAgentId } : {}),
    ...(hasUsage
      ? {
          usage: {
            ...(inputTokens !== undefined ? { inputTokens } : {}),
            ...(outputTokens !== undefined ? { outputTokens } : {}),
            ...(costUsd !== undefined ? { costUsd } : {})
          }
        }
      : {}),
    ...(deadlineMicros !== undefined ? { deadlineMicros } : {}),
    ...(idempotencyKey !== undefined ? { idempotencyKey } : {}),
    ...(correlationId !== undefined ? { correlationId } : {}),
    ...(fenceToken !== undefined ? { fenceToken } : {})
  }
}

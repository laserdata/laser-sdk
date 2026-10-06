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
  USAGE_OUTPUT_TOKENS
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

function putFinite(map: Map<string, HeaderValue>, key: string, value: number): void {
  if (!Number.isFinite(value)) {
    throw ProvenanceError.nonFinite(key)
  }
  putHeader(map, key, String(value))
}

export function encodeProvenanceHeaders(provenance: Provenance): ReadonlyMap<string, HeaderValue> {
  const map = new Map<string, HeaderValue>()
  putHeader(map, CONVERSATION_ID, provenance.conversationId.toString())
  if (provenance.parentConversationId !== undefined) {
    putHeader(map, PARENT_CONVERSATION_ID, provenance.parentConversationId.toString())
  }
  if (provenance.rootConversationId !== undefined) {
    putHeader(map, ROOT_CONVERSATION_ID, provenance.rootConversationId.toString())
  }
  if (provenance.causalParent !== undefined) {
    putHeader(map, CAUSAL_PARENT, messageIdToString(provenance.causalParent))
  }
  if (provenance.agent !== undefined) {
    putHeader(map, AGENT_ID, provenance.agent.asStr())
  }
  if (provenance.targetAgentId !== undefined) {
    putHeader(map, TARGET_AGENT_ID, provenance.targetAgentId.asStr())
  }
  if (provenance.idempotencyKey !== undefined) {
    putHeader(map, IDEMPOTENCY_KEY, provenance.idempotencyKey)
  }
  if (provenance.correlationId !== undefined) {
    putHeader(map, CORRELATION_ID, provenance.correlationId)
  }
  if (provenance.fenceToken !== undefined) {
    putHeader(map, FENCE, provenance.fenceToken.toString())
  }
  if (provenance.deadlineMicros !== undefined) {
    putHeader(map, DEADLINE, provenance.deadlineMicros.toString())
  }
  if (provenance.usage !== undefined) {
    if (provenance.usage.inputTokens !== undefined) {
      putHeader(map, USAGE_INPUT_TOKENS, provenance.usage.inputTokens.toString())
    }
    if (provenance.usage.outputTokens !== undefined) {
      putHeader(map, USAGE_OUTPUT_TOKENS, provenance.usage.outputTokens.toString())
    }
    if (provenance.usage.costUsd !== undefined) {
      putFinite(map, COST_USD, provenance.usage.costUsd)
    }
  }

  let size = 0
  for (const [key, value] of map) {
    const valueBytes = value.kind === "string" ? new TextEncoder().encode(value.value).length : 0
    size += new TextEncoder().encode(key).length + valueBytes + HEADER_FRAMING_BYTES
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

function parseUnsignedBigInt(text: string, key: string): bigint {
  // Bounded before parsing: decimal-to-BigInt conversion is superlinear, so a
  // megabyte of digits on a peer-supplied header would burn CPU per message.
  // Encode enforces the same cap, so anything longer was never produced here.
  if (text.length > HEADER_VALUE_MAX || !/^[0-9]+$/.test(text)) {
    throw ProvenanceError.invalidValue(key)
  }
  return BigInt(text)
}

function parseFloatValue(text: string, key: string): number {
  const parsed = Number(text)
  if (Number.isNaN(parsed) && text.trim().toLowerCase() !== "nan") {
    throw ProvenanceError.invalidValue(key)
  }
  return parsed
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
      case TARGET_AGENT_ID:
        targetAgentId = parseId(() => AgentId.new(strValue(value, key)))
        break
      case IDEMPOTENCY_KEY:
        idempotencyKey = strValue(value, key)
        break
      case CORRELATION_ID:
        correlationId = strValue(value, key)
        break
      case FENCE:
        fenceToken = parseUnsignedBigInt(strValue(value, key), key)
        break
      case DEADLINE:
        deadlineMicros = parseUnsignedBigInt(strValue(value, key), key)
        break
      case USAGE_INPUT_TOKENS:
        inputTokens = parseUnsignedBigInt(strValue(value, key), key)
        hasUsage = true
        break
      case USAGE_OUTPUT_TOKENS:
        outputTokens = parseUnsignedBigInt(strValue(value, key), key)
        hasUsage = true
        break
      case COST_USD:
        costUsd = parseFloatValue(strValue(value, key), key)
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

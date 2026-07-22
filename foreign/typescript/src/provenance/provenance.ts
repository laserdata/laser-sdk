import { CodecError, InvalidError } from "../client/errors.js"
import type { IggyHeaderValue } from "../iggy/apache-iggy.js"
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

// Token / cost usage for an LLM call, carried on provenance for rollup.
export interface LlmUsage {
  readonly inputTokens?: bigint
  readonly outputTokens?: bigint
  readonly costUsd?: number
}

// The agentic message spine: conversation, causality, routing, usage.
// Encoded to/from message headers with `encodeProvenanceHeaders`/
// `decodeProvenanceHeaders`.
export interface Provenance {
  // The conversation this message belongs to (the partition key).
  readonly conversationId: ConversationId
  // The message this one is a reply to, if any.
  readonly causalParent?: MessageId
  // The conversation this was spawned from (sub-conversations).
  readonly parentConversationId?: ConversationId
  // The root of the conversation tree (sub-conversations).
  readonly rootConversationId?: ConversationId
  // The agent that produced this message.
  readonly agent?: AgentId
  // The agent this message is addressed to (set by a router).
  readonly targetAgentId?: AgentId
  // LLM token / cost usage for this step.
  readonly usage?: LlmUsage
  // Drop-dead time (epoch micros). A consumer past it dead-letters the
  // message.
  readonly deadlineMicros?: bigint
  // Business dedup key: a producer-supplied key that suppresses a
  // duplicate of the same logical operation. Distinct from
  // `correlationId`: setting a real dedup key and retrying no longer
  // cross-matches replies.
  readonly idempotencyKey?: string
  // Reply-correlation key: matches a reply to the request that minted it,
  // carried in the `agdx.corr` header. A request helper mints one when
  // unset. Kept separate from `idempotencyKey` so a business dedup key and
  // a reply correlation never overload one slot.
  readonly correlationId?: string
  // The strictly-monotonic per-task fence the producer held, when this
  // message carries a fenced effect. The reliable consumer drops a
  // message whose fence is below the highest it has accepted for the task
  // (a stale-holder replay).
  readonly fenceToken?: bigint
}

// The Iggy partition key (the conversation id), so one conversation stays
// ordered.
export function provenancePartitionKey(provenance: Provenance): string {
  return provenance.conversationId.toString()
}

function putHeader(map: Map<string, IggyHeaderValue>, key: string, value: string): void {
  if (value.length === 0) {
    throw new InvalidError(`header \`${key}\` value must not be empty`)
  }
  const bytes = new TextEncoder().encode(value)
  if (bytes.length > HEADER_VALUE_MAX) {
    throw new InvalidError(
      `header \`${key}\` value is ${String(bytes.length)}B, exceeds max ${String(HEADER_VALUE_MAX)}B`
    )
  }
  for (const byte of bytes) {
    if (byte < 0x20 || byte === 0x7f) {
      throw new InvalidError(`header \`${key}\` value must not contain control characters or NUL`)
    }
  }
  map.set(key, { kind: "string", value })
}

function putFinite(map: Map<string, IggyHeaderValue>, key: string, value: number): void {
  if (!Number.isFinite(value)) {
    throw new InvalidError(`non-finite floating-point value for header \`${key}\``)
  }
  putHeader(map, key, String(value))
}

// Encode `provenance` into the Iggy user-header map it rides on the wire.
// Throws `InvalidError` for an out-of-range field (empty/oversized/
// control-byte value, a non-finite cost, or a headers total over the soft
// cap).
export function encodeProvenanceHeaders(
  provenance: Provenance
): ReadonlyMap<string, IggyHeaderValue> {
  const map = new Map<string, IggyHeaderValue>()
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
    putHeader(map, AGENT_ID, provenance.agent.asString())
  }
  if (provenance.targetAgentId !== undefined) {
    putHeader(map, TARGET_AGENT_ID, provenance.targetAgentId.asString())
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
    throw new InvalidError(
      `provenance headers ${String(size)}B exceed soft cap ${String(HEADER_SOFT_CAP)}B`
    )
  }
  return map
}

function strValue(value: IggyHeaderValue, key: string): string {
  if (value.kind !== "string") {
    throw new CodecError(`invalid value for header \`${key}\``, "provenance", "decode")
  }
  return value.value
}

function parseUnsignedBigInt(text: string, key: string): bigint {
  if (!/^[0-9]+$/.test(text)) {
    throw new CodecError(`invalid value for header \`${key}\``, "provenance", "decode")
  }
  return BigInt(text)
}

function parseFloatValue(text: string, key: string): number {
  const parsed = Number(text)
  if (Number.isNaN(parsed) && text.trim().toLowerCase() !== "nan") {
    throw new CodecError(`invalid value for header \`${key}\``, "provenance", "decode")
  }
  return parsed
}

// Decode provenance from an Iggy user-header map. Throws `CodecError` when
// the required conversation id is missing, or a known key's value is
// either the wrong type or does not parse. A header outside the
// provenance dictionary (a typed AGDX field, a routing duplicate, any
// app-custom key) is foreign and ignored, not an error.
export function decodeProvenanceHeaders(headers: ReadonlyMap<string, IggyHeaderValue>): Provenance {
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
        conversationId = ConversationId.parse(strValue(value, key))
        break
      case CAUSAL_PARENT:
        causalParent = parseMessageId(strValue(value, key))
        break
      case PARENT_CONVERSATION_ID:
        parentConversationId = ConversationId.parse(strValue(value, key))
        break
      case ROOT_CONVERSATION_ID:
        rootConversationId = ConversationId.parse(strValue(value, key))
        break
      case AGENT_ID:
        agent = AgentId.new(strValue(value, key))
        break
      case TARGET_AGENT_ID:
        targetAgentId = AgentId.new(strValue(value, key))
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
    throw new CodecError(`missing required header \`${CONVERSATION_ID}\``, "provenance", "decode")
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

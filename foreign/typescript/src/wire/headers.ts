export const CONTENT_TYPE = "agdx.ct"
export const SCHEMA_ID = "agdx.sid"
export const LOGICAL_SCHEMA_FINGERPRINT = "agdx.sfp"
export const IDX_PREFIX = "agdx.idx."
export const INLINE_PAYLOAD = "agdx.inline"
export const PROJECTION_REF = "agdx.ref"
export const CORRELATION_ID = "agdx.corr"

export const FIELD_MESSAGE_TYPE = "message_type"
export const FIELD_TS = "ts"
export const VECTOR_FIELD = "embedding"
export const WINDOW_START = "window_start"

export const HEADER_SOFT_CAP = 1024
export const HEADER_FRAMING_BYTES = 9
export const HEADER_VALUE_MAX = 255

export const CONVERSATION_ID = "gen_ai.conversation.id"
export const CONVERSATION_FIELD = "conversation_id"
export const AGENT_ID = "gen_ai.agent.id"
export const REQUEST_MODEL = "gen_ai.request.model"
export const RESPONSE_MODEL = "gen_ai.response.model"
export const PROVIDER_NAME = "gen_ai.provider.name"
export const USAGE_INPUT_TOKENS = "gen_ai.usage.input_tokens"
export const USAGE_OUTPUT_TOKENS = "gen_ai.usage.output_tokens"
export const CAUSAL_PARENT = "agdx.cause"
export const PARENT_CONVERSATION_ID = "agdx.parent_conv"
export const ROOT_CONVERSATION_ID = "agdx.root_conv"
export const TARGET_AGENT_ID = "agdx.to"
export const DELEGATED_BY = "agdx.on_behalf_of"
export const IDEMPOTENCY_KEY = "agdx.idem"
export const DEADLINE = "agdx.deadline"
export const COST_USD = "agdx.cost"
export const FENCE = "agdx.fence"
export const AGENT_VERSION = "agdx.av"

export const MEMORY_NAMESPACE = "agdx.mem.ns"
export const MEMORY_USER = "agdx.mem.user"
export const MEMORY_APP = "agdx.mem.app"

/** One typed user-header value as a record carries it. */
export type HeaderField =
  | { readonly kind: "text"; readonly value: string }
  | { readonly kind: "uint8"; readonly value: number }
  | { readonly kind: "uint32"; readonly value: number }
  | { readonly kind: "uint64"; readonly value: bigint }
  | { readonly kind: "float64"; readonly value: number }

/** The `agdx.to` value that addresses every agent. */
export const BROADCAST = "*"

/** Who a record is addressed to: one agent by name, or every agent. */
export type Addressee =
  { readonly kind: "agent"; readonly agent: string } | { readonly kind: "broadcast" }

/** The `agdx.to` text of `addressee`. */
export function addresseeText(addressee: Addressee): string {
  return addressee.kind === "agent" ? addressee.agent : BROADCAST
}

/**
 * The routing and provenance headers of one record. Envelope records carry the
 * envelope version and content type. Generic records do not. Ids ride as
 * canonical strings and numbers ride typed.
 */
export interface RecordHeaders {
  readonly envelope?: { readonly version: number; readonly contentType: number }
  readonly conversation: string
  readonly parent?: string
  readonly root?: string
  readonly agent?: string
  readonly addressee?: Addressee
  readonly causalParent?: string
  readonly idempotencyKey?: string
  readonly correlation?: string
  readonly fence?: bigint
  readonly deadlineMicros?: bigint
  readonly inputTokens?: bigint
  readonly outputTokens?: bigint
  readonly costUsd?: number
}

/** The header block of `headers`, sorted by key. */
export function encodeRecordHeaders(
  headers: RecordHeaders
): readonly (readonly [string, HeaderField])[] {
  const block: (readonly [string, HeaderField])[] = []
  const text = (key: string, value: string | undefined): void => {
    if (value !== undefined) block.push([key, { kind: "text", value }])
  }
  const uint64 = (key: string, value: bigint | undefined): void => {
    if (value !== undefined) block.push([key, { kind: "uint64", value }])
  }
  if (headers.envelope !== undefined) {
    block.push([AGENT_VERSION, { kind: "uint32", value: headers.envelope.version }])
    block.push([CONTENT_TYPE, { kind: "uint8", value: headers.envelope.contentType }])
  }
  text(CONVERSATION_ID, headers.conversation)
  text(PARENT_CONVERSATION_ID, headers.parent)
  text(ROOT_CONVERSATION_ID, headers.root)
  text(AGENT_ID, headers.agent)
  text(
    TARGET_AGENT_ID,
    headers.addressee === undefined ? undefined : addresseeText(headers.addressee)
  )
  text(CAUSAL_PARENT, headers.causalParent)
  text(IDEMPOTENCY_KEY, headers.idempotencyKey)
  text(CORRELATION_ID, headers.correlation)
  uint64(FENCE, headers.fence)
  uint64(DEADLINE, headers.deadlineMicros)
  uint64(USAGE_INPUT_TOKENS, headers.inputTokens)
  uint64(USAGE_OUTPUT_TOKENS, headers.outputTokens)
  if (headers.costUsd !== undefined)
    block.push([COST_USD, { kind: "float64", value: headers.costUsd }])
  return block.sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
}

import { CodecError } from "../client/errors.js"
import type { HeaderFault } from "../iggy/apache-iggy.js"
import { decodeProvenanceHeaders, type Provenance } from "../provenance/provenance.js"
import type { HeaderValue } from "../stream/header-value.js"
import { AgentId, ConversationId, type MessageId } from "../types/ids.js"
import {
  type AgentEnvelope,
  decodeAgentEnvelope,
  unmetRequirements,
  validateAgentEnvelope,
  type SignatureContext
} from "../wire/agent.js"
import { decodeOne, expectMap } from "../wire/cbor.js"
import { AGENT_OP_VERSION } from "../wire/codes.js"
import { type ContentType, contentTypeFromCode } from "../wire/content.js"
import { AGENT_VERSION, CONTENT_TYPE, FENCE } from "../wire/headers.js"

function tryAgentId(value: string): AgentId | undefined {
  try {
    return AgentId.new(value)
  } catch {
    return undefined
  }
}

function fenceFromMetadata(
  metadata: ReadonlyMap<string, { readonly kind: string; readonly value?: unknown }> | undefined
): bigint | undefined {
  const entry = metadata?.get(FENCE)
  if (entry?.kind !== "int" && entry?.kind !== "uint") return undefined
  const value = entry.value
  return typeof value === "bigint" && value >= 0n ? value : undefined
}

export function provenanceFromEnvelope(envelope: AgentEnvelope): Provenance {
  const agent = tryAgentId(envelope.source)
  const targetAgentId = envelope.target !== undefined ? tryAgentId(envelope.target) : undefined
  const fenceToken = fenceFromMetadata(envelope.metadata)
  return {
    conversationId: ConversationId.parse(envelope.conversation.toString()),
    ...(envelope.parent !== undefined
      ? { parentConversationId: ConversationId.parse(envelope.parent.toString()) }
      : {}),
    ...(envelope.root !== undefined
      ? { rootConversationId: ConversationId.parse(envelope.root.toString()) }
      : {}),
    ...(agent !== undefined ? { agent } : {}),
    ...(targetAgentId !== undefined ? { targetAgentId } : {}),
    ...(envelope.idempotencyKey !== undefined ? { idempotencyKey: envelope.idempotencyKey } : {}),
    ...(envelope.correlation !== undefined
      ? { correlationId: envelope.correlation.toString() }
      : {}),
    ...(envelope.deadlineMicros !== undefined ? { deadlineMicros: envelope.deadlineMicros } : {}),
    ...(fenceToken !== undefined ? { fenceToken } : {})
  }
}

export interface ReceivedAgentMessage {
  readonly payload: Uint8Array
  readonly partitionId: number
  readonly offset: bigint
  readonly timestampMicros?: bigint
  readonly headers: ReadonlyMap<string, HeaderValue>
  /** Set when the record's header block did not decode, so `headers` is empty. */
  readonly headersMalformed?: boolean | HeaderFault
  /** The partition head offset when the record was polled. */
  readonly currentOffset?: bigint
}

export interface ProvenanceAndEnvelope {
  readonly provenance: Provenance
  readonly envelope?: AgentEnvelope
  readonly signatureContext?: SignatureContext
}

export function provenanceAndEnvelope(
  message: ReceivedAgentMessage,
  understoodFeatures = 0n
): ProvenanceAndEnvelope {
  const version = message.headers.get(AGENT_VERSION)
  if (version !== undefined) {
    if (version.kind !== "uint32" || version.value !== AGENT_OP_VERSION) {
      throw new CodecError("unsupported agent envelope version", "agent", AGENT_VERSION)
    }
    const context = "agent envelope"
    const envelope = decodeAgentEnvelope(
      expectMap(decodeOne(message.payload, context), context),
      context
    )
    validateAgentEnvelope(envelope)
    const unmet = unmetRequirements(envelope, understoodFeatures)
    if (unmet !== 0n) {
      throw new CodecError(
        `agent envelope requires unsupported features 0x${unmet.toString(16).padStart(16, "0")}`,
        "agent",
        "must_understand"
      )
    }
    const contentType = message.headers.get(CONTENT_TYPE)
    if (contentType !== undefined && contentType.kind !== "uint8") {
      throw new CodecError("invalid content-type header", "agent", CONTENT_TYPE)
    }
    return {
      provenance: provenanceFromEnvelope(envelope),
      envelope,
      signatureContext: {
        ...(contentType?.kind === "uint8" ? { contentType: contentType.value } : {}),
        agentVersion: version.value
      }
    }
  }
  return { provenance: decodeProvenanceHeaders(message.headers) }
}

/** Whether the record's header block did not decode.
 * @internal */
export function headersMalformed(received: ReceivedAgentMessage): boolean {
  return received.headersMalformed !== undefined && received.headersMalformed !== false
}

export function contentTypeOf(message: ReceivedAgentMessage): ContentType | undefined {
  const header = message.headers.get(CONTENT_TYPE)
  return header?.kind === "uint8" ? contentTypeFromCode(header.value) : undefined
}

export interface AgentMessage {
  readonly provenance: Provenance
  readonly payload: Uint8Array
  readonly id: MessageId
  readonly envelope?: AgentEnvelope
  readonly contentType?: ContentType
  readonly verifiedPrincipal?: string
}

export function agentMessageBody(message: AgentMessage): Uint8Array {
  return message.envelope !== undefined ? message.envelope.body : message.payload
}

export type DecodedAgentMessage =
  | {
      readonly kind: "message"
      readonly message: AgentMessage
      readonly signatureContext?: SignatureContext
      readonly observedAtMicros?: bigint
    }
  | { readonly kind: "error"; readonly error: CodecError; readonly payload: Uint8Array }

export function decodeAgentMessage(
  received: ReceivedAgentMessage,
  understoodFeatures = 0n
): DecodedAgentMessage {
  try {
    if (headersMalformed(received)) {
      throw new CodecError("the record's header block does not decode", "agent", "headers")
    }
    const { provenance, envelope, signatureContext } = provenanceAndEnvelope(
      received,
      understoodFeatures
    )
    const contentType = contentTypeOf(received)
    return {
      kind: "message",
      message: {
        provenance,
        payload: received.payload,
        id: { partitionId: received.partitionId, offset: received.offset },
        ...(envelope !== undefined ? { envelope } : {}),
        ...(contentType !== undefined ? { contentType } : {})
      },
      ...(signatureContext !== undefined ? { signatureContext } : {}),
      ...(received.timestampMicros !== undefined
        ? { observedAtMicros: received.timestampMicros }
        : {})
    }
  } catch (cause) {
    return {
      kind: "error",
      error: new CodecError("failed to decode agent message", "agent", "decode", { cause }),
      payload: received.payload
    }
  }
}

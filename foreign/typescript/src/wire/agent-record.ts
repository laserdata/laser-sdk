import { CodecError } from "../client/errors.js"
import { type AgentEnvelope, decodeAgentEnvelope, encodeAgentEnvelope } from "./agent.js"
import { type CborMap, decodeOne, encodeNamed, expectMap, field } from "./cbor.js"
import { AGENT_OP_VERSION } from "./codes.js"
import { type ContentType, contentTypeCode } from "./content.js"
import { type RecordHeaders, encodeRecordHeaders } from "./headers.js"

export const CanonicalHeaderKind = {
  U32: "u32",
  U8: "u8",
  Uint128: "uint128",
  String: "string"
} as const

export type CanonicalHeaderKind = (typeof CanonicalHeaderKind)[keyof typeof CanonicalHeaderKind]

export interface CanonicalHeader {
  readonly kind: CanonicalHeaderKind
  readonly bytes: Uint8Array
}

export interface CanonicalAgentRecord {
  readonly partitionKey: string
  readonly headers: ReadonlyMap<string, CanonicalHeader>
  readonly payload: Uint8Array
}

const textEncoder = new TextEncoder()

function u32LittleEndian(value: number): Uint8Array {
  const bytes = new Uint8Array(4)
  new DataView(bytes.buffer).setUint32(0, value, true)
  return bytes
}

/**
 * The header block of an envelope record: version, content type, conversation,
 * ancestry, author, and addressee. A record without a target is addressed to
 * every agent when `broadcast` is set, as every record on a shared session
 * topic must be.
 */
export function recordHeadersForEnvelope(
  envelope: AgentEnvelope,
  contentType: ContentType,
  broadcast = false
): RecordHeaders {
  return {
    envelope: { version: AGENT_OP_VERSION, contentType: contentTypeCode(contentType) },
    conversation: envelope.conversation.toString(),
    ...(envelope.parent !== undefined ? { parent: envelope.parent.toString() } : {}),
    ...(envelope.root !== undefined ? { root: envelope.root.toString() } : {}),
    agent: envelope.source,
    ...(envelope.target !== undefined
      ? { addressee: { kind: "agent" as const, agent: envelope.target } }
      : broadcast
        ? { addressee: { kind: "broadcast" as const } }
        : {})
  }
}

/** The canonical record of `envelope`. `broadcast` addresses an untargeted
 * envelope to every agent, as on a shared session topic. */
export function canonicalAgentRecord(
  envelope: AgentEnvelope,
  contentType: ContentType,
  broadcast = false
): CanonicalAgentRecord {
  const headers = new Map<string, CanonicalHeader>()
  for (const [key, field] of encodeRecordHeaders(
    recordHeadersForEnvelope(envelope, contentType, broadcast)
  )) {
    switch (field.kind) {
      case "text":
        headers.set(key, {
          kind: CanonicalHeaderKind.String,
          bytes: textEncoder.encode(field.value)
        })
        break
      case "uint32":
        headers.set(key, { kind: CanonicalHeaderKind.U32, bytes: u32LittleEndian(field.value) })
        break
      case "uint8":
        headers.set(key, { kind: CanonicalHeaderKind.U8, bytes: Uint8Array.of(field.value) })
        break
      case "uint64":
      case "float64":
        throw new CodecError(`unexpected envelope header ${key}`, "agent record", key)
    }
  }
  return {
    partitionKey: envelope.conversation.toString(),
    headers,
    payload: encodeNamed(encodeAgentEnvelope(envelope))
  }
}

export function encodeCanonicalAgentRecord(record: CanonicalAgentRecord): Map<string, unknown> {
  return new Map<string, unknown>([
    ["partition_key", record.partitionKey],
    [
      "headers",
      new Map(
        Array.from(record.headers, ([key, header]) => [
          key,
          new Map<string, unknown>([
            ["kind", header.kind],
            ["bytes", header.bytes]
          ])
        ])
      )
    ],
    ["payload", record.payload]
  ])
}

function decodeCanonicalHeader(value: unknown, context: string): CanonicalHeader {
  const map = expectMap(value, context)
  const kind = field.requiredString(map, "kind", context)
  if (!Object.values(CanonicalHeaderKind).includes(kind as CanonicalHeaderKind)) {
    throw new CodecError(`unrecognized canonical header kind \`${kind}\``, context, "kind")
  }
  return {
    kind: kind as CanonicalHeaderKind,
    bytes: field.requiredBytes(map, "bytes", context)
  }
}

export function decodeCanonicalAgentRecord(map: CborMap, context: string): CanonicalAgentRecord {
  const headerMap = field.requiredMap(map, "headers", context)
  const headers = new Map<string, CanonicalHeader>()
  for (const [key, value] of headerMap) {
    if (typeof key !== "string") {
      throw new CodecError("canonical header key must be a string", context, "headers")
    }
    headers.set(key, decodeCanonicalHeader(value, `${context}.headers.${key}`))
  }
  return {
    partitionKey: field.requiredString(map, "partition_key", context),
    headers,
    payload: field.requiredBytes(map, "payload", context)
  }
}

export function decodeCanonicalAgentEnvelope(
  record: CanonicalAgentRecord,
  context: string
): AgentEnvelope {
  return decodeAgentEnvelope(
    expectMap(decodeOne(record.payload, `${context}.payload`), `${context}.payload`),
    context
  )
}

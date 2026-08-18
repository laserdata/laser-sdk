import { CodecError } from "../client/errors.js"
import { type CborMap, field } from "./cbor.js"

export const MANAGED_REQUEST_VERSION = 1

export interface ManagedRequestEnvelope {
  readonly v: number
  readonly operationId: bigint
  readonly payload: Uint8Array
}

export function encodeManagedRequestEnvelope(
  envelope: ManagedRequestEnvelope
): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", BigInt(envelope.v)],
    ["operation_id", envelope.operationId],
    ["payload", envelope.payload]
  ])
}

export function decodeManagedRequestEnvelope(
  map: CborMap,
  context: string
): ManagedRequestEnvelope {
  const version = field.requiredU32(map, "v", context)
  if (version !== MANAGED_REQUEST_VERSION) {
    throw new CodecError("unsupported managed request version", "managed", "v")
  }
  const operationId = field.requiredU128(map, "operation_id", context)
  if (operationId === 0n) {
    throw new CodecError("managed request operation id must not be zero", "managed", "operation_id")
  }
  return {
    v: version,
    operationId,
    payload: field.requiredBytes(map, "payload", context)
  }
}

/** Position of one applied mutation on the KV mutation topic: the fold
 * coordinate a barriered read waits for. Positions compare only within one
 * `topicGeneration`. A generation mismatch is fail-closed. */
export interface MutationPosition {
  readonly topicGeneration: bigint
  readonly partition: number
  readonly offset: bigint
}

export function encodeMutationPosition(position: MutationPosition): Map<string, unknown> {
  return new Map<string, unknown>([
    ["topic_generation", position.topicGeneration],
    ["partition", BigInt(position.partition)],
    ["offset", position.offset]
  ])
}

export function decodeMutationPosition(map: CborMap, context: string): MutationPosition {
  return {
    topicGeneration: field.requiredU64(map, "topic_generation", context),
    partition: field.requiredU32(map, "partition", context),
    offset: field.requiredU64(map, "offset", context)
  }
}

export interface MutationCommandEnvelope {
  readonly v: number
  readonly operationId: bigint
  readonly timestampMicros: bigint
  readonly commandCode: number
  readonly payload: Uint8Array
}

export function encodeMutationCommandEnvelope(
  envelope: MutationCommandEnvelope
): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", BigInt(envelope.v)],
    ["operation_id", envelope.operationId],
    ["timestamp_micros", envelope.timestampMicros],
    ["command_code", BigInt(envelope.commandCode)],
    ["payload", envelope.payload]
  ])
}

export function decodeMutationCommandEnvelope(
  map: CborMap,
  context: string
): MutationCommandEnvelope {
  const version = field.requiredU32(map, "v", context)
  if (version === 0) {
    throw new CodecError("mutation command version must not be zero", "mutation", "v")
  }
  const operationId = field.requiredU128(map, "operation_id", context)
  if (operationId === 0n) {
    throw new CodecError(
      "mutation command operation id must not be zero",
      "mutation",
      "operation_id"
    )
  }
  return {
    v: version,
    operationId,
    timestampMicros: field.requiredU64(map, "timestamp_micros", context),
    commandCode: field.requiredU32(map, "command_code", context),
    payload: field.requiredBytes(map, "payload", context)
  }
}

import { type CborMap, field } from "./cbor.js"

export interface MutationCommandEnvelope {
  readonly v: number
  readonly timestampMicros: bigint
  readonly commandCode: number
  readonly payload: Uint8Array
}

export function encodeMutationCommandEnvelope(
  envelope: MutationCommandEnvelope
): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", BigInt(envelope.v)],
    ["timestamp_micros", envelope.timestampMicros],
    ["command_code", BigInt(envelope.commandCode)],
    ["payload", envelope.payload]
  ])
}

export function decodeMutationCommandEnvelope(
  map: CborMap,
  context: string
): MutationCommandEnvelope {
  return {
    v: field.requiredU32(map, "v", context),
    timestampMicros: field.requiredU64(map, "timestamp_micros", context),
    commandCode: field.requiredU32(map, "command_code", context),
    payload: field.requiredBytes(map, "payload", context)
  }
}

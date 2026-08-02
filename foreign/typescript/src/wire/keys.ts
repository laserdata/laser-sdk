import { SignatureError } from "../client/errors.js"
import { type CborMap, field } from "./cbor.js"

export const KEY_RECORD_VERSION = 1
export const KEY_ID_BYTES = 8
export const VERIFYING_KEY_BYTES = 32

export const KeyKind = { Agent: "agent", Operator: "operator" } as const
export type KeyKind = (typeof KeyKind)[keyof typeof KeyKind]

export interface StoredKeyRecord {
  readonly v: number
  readonly principal: string
  readonly keyId: Uint8Array
  readonly verifyingKey: Uint8Array
  readonly kind: KeyKind
  readonly validFromMicros: bigint
  readonly validToMicros?: bigint
  readonly revoked: boolean
}

export function encodeStoredKeyRecord(record: StoredKeyRecord): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["v", BigInt(record.v)],
    ["principal", record.principal],
    ["key_id", record.keyId],
    ["verifying_key", record.verifyingKey],
    ["kind", record.kind],
    ["valid_from_micros", record.validFromMicros]
  ])
  if (record.validToMicros !== undefined) map.set("valid_to_micros", record.validToMicros)
  map.set("revoked", record.revoked)
  return map
}

export function decodeStoredKeyRecord(map: CborMap, context: string): StoredKeyRecord {
  const kind = field.requiredString(map, "kind", context)
  if (kind !== KeyKind.Agent && kind !== KeyKind.Operator) {
    throw new SignatureError("invalid key kind")
  }
  const v = field.requiredU32(map, "v", context)
  const principal = field.requiredString(map, "principal", context)
  const keyId = field.requiredBytes(map, "key_id", context)
  const verifyingKey = field.requiredBytes(map, "verifying_key", context)
  const validFromMicros = field.requiredU64(map, "valid_from_micros", context)
  const validToMicros = field.optionalU64(map, "valid_to_micros", context)
  if (v !== KEY_RECORD_VERSION) throw new SignatureError("unsupported key record version")
  if (principal.length === 0) throw new SignatureError("key principal must not be empty")
  if (keyId.byteLength !== KEY_ID_BYTES) {
    throw new SignatureError("Ed25519 key id must be 8 bytes")
  }
  if (verifyingKey.byteLength !== VERIFYING_KEY_BYTES) {
    throw new SignatureError("Ed25519 public key must be 32 bytes")
  }
  if (validToMicros !== undefined && validToMicros <= validFromMicros) {
    throw new SignatureError("key validity end must be after its start")
  }
  return {
    v,
    principal,
    keyId,
    verifyingKey,
    kind,
    validFromMicros,
    ...(validToMicros !== undefined ? { validToMicros } : {}),
    revoked: field.requiredBoolean(map, "revoked", context)
  }
}

import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, field } from "./cbor.js"

export const ARROW_IPC_CONTRACT_VERSION = 1
export const ARROW_IPC_MEDIA_TYPE = "application/vnd.apache.arrow.stream"
export const MAX_ARROW_IPC_MESSAGE_BYTES = 8 * 1024 * 1024
export const MAX_ARROW_IPC_FIELDS = 4096
export const MAX_ARROW_IPC_BATCHES = 64
export const MAX_ARROW_IPC_ROWS = 1_000_000n
export const MAX_ARROW_IPC_DICTIONARIES = 4096
export const MAX_ARROW_DECIMAL_BITS = 128

export interface ArrowIpcMessageMetadata {
  readonly contractVersion: number
  readonly schemaFingerprint: Uint8Array
  readonly encodedBytes: bigint
  readonly fieldCount: number
  readonly recordBatchCount: number
  readonly rowCount: bigint
  readonly dictionaryCount: number
}

export interface ArrowIpcPolicy {
  readonly contractVersion: number
  readonly streamFormatOnly: boolean
  readonly selfContained: boolean
  readonly dictionaryDeltas: boolean
  readonly replacementDictionaries: boolean
  readonly timestampUnit: ArrowTimestampUnit
  readonly maxDecimalBits: number
  readonly unions: boolean
  readonly extensionTypes: boolean
}

export type ArrowTimestampUnit = "microsecond"

export type ArrowIpcRejectionCode =
  | "file_format"
  | "missing_schema"
  | "missing_dictionary"
  | "dictionary_delta"
  | "dictionary_replacement"
  | "union"
  | "extension_type"
  | "timestamp_unit"
  | "decimal_width"
  | "schema_fingerprint"
  | "field_limit"
  | "batch_limit"
  | "row_limit"
  | "byte_limit"
  | "malformed_stream"

export const DEFAULT_ARROW_IPC_POLICY: ArrowIpcPolicy = {
  contractVersion: ARROW_IPC_CONTRACT_VERSION,
  streamFormatOnly: true,
  selfContained: true,
  dictionaryDeltas: false,
  replacementDictionaries: false,
  timestampUnit: "microsecond",
  maxDecimalBits: MAX_ARROW_DECIMAL_BITS,
  unions: false,
  extensionTypes: false
}

export function encodeArrowIpcMessageMetadata(
  metadata: ArrowIpcMessageMetadata
): Map<string, unknown> {
  return new Map<string, unknown>([
    ["contract_version", metadata.contractVersion],
    ["schema_fingerprint", metadata.schemaFingerprint],
    ["encoded_bytes", metadata.encodedBytes],
    ["field_count", metadata.fieldCount],
    ["record_batch_count", metadata.recordBatchCount],
    ["row_count", metadata.rowCount],
    ["dictionary_count", metadata.dictionaryCount]
  ])
}

export function decodeArrowIpcMessageMetadata(
  map: CborMap,
  context: string
): ArrowIpcMessageMetadata {
  return {
    contractVersion: field.requiredU32(map, "contract_version", context),
    schemaFingerprint: fixedBytes(
      field.requiredBytes(map, "schema_fingerprint", context),
      32,
      `${context}.schema_fingerprint`
    ),
    encodedBytes: field.requiredU64(map, "encoded_bytes", context),
    fieldCount: field.requiredU32(map, "field_count", context),
    recordBatchCount: field.requiredU32(map, "record_batch_count", context),
    rowCount: field.requiredU64(map, "row_count", context),
    dictionaryCount: field.requiredU32(map, "dictionary_count", context)
  }
}

export function encodeArrowIpcPolicy(policy: ArrowIpcPolicy): Map<string, unknown> {
  return new Map<string, unknown>([
    ["contract_version", policy.contractVersion],
    ["stream_format_only", policy.streamFormatOnly],
    ["self_contained", policy.selfContained],
    ["dictionary_deltas", policy.dictionaryDeltas],
    ["replacement_dictionaries", policy.replacementDictionaries],
    ["timestamp_unit", policy.timestampUnit],
    ["max_decimal_bits", policy.maxDecimalBits],
    ["unions", policy.unions],
    ["extension_types", policy.extensionTypes]
  ])
}

export function decodeArrowIpcPolicy(map: CborMap, context: string): ArrowIpcPolicy {
  const timestampUnit = field.requiredString(map, "timestamp_unit", context)
  if (timestampUnit !== "microsecond") {
    throw new CodecError(
      `unsupported Arrow timestamp unit \`${timestampUnit}\``,
      context,
      "timestamp_unit"
    )
  }
  const maxDecimalBits = field.requiredU32(map, "max_decimal_bits", context)
  if (maxDecimalBits !== MAX_ARROW_DECIMAL_BITS) {
    throw new CodecError(
      `unsupported Arrow decimal width ${String(maxDecimalBits)}`,
      context,
      "max_decimal_bits"
    )
  }
  return {
    contractVersion: field.requiredU32(map, "contract_version", context),
    streamFormatOnly: requireTrue(map, "stream_format_only", context),
    selfContained: requireTrue(map, "self_contained", context),
    dictionaryDeltas: requireFalse(map, "dictionary_deltas", context),
    replacementDictionaries: requireFalse(map, "replacement_dictionaries", context),
    timestampUnit,
    maxDecimalBits,
    unions: requireFalse(map, "unions", context),
    extensionTypes: requireFalse(map, "extension_types", context)
  }
}

export function validateArrowIpcMetadata(metadata: ArrowIpcMessageMetadata): void {
  if (metadata.contractVersion !== ARROW_IPC_CONTRACT_VERSION)
    throw new InvalidError("Arrow IPC contract version is unsupported")
  if (metadata.schemaFingerprint.length !== 32)
    throw new InvalidError("Arrow IPC schema fingerprint must contain 32 bytes")
  if (metadata.encodedBytes < 1n || metadata.encodedBytes > BigInt(MAX_ARROW_IPC_MESSAGE_BYTES))
    throw new InvalidError("Arrow IPC encoded byte count is outside its bound")
  if (metadata.fieldCount < 1 || metadata.fieldCount > MAX_ARROW_IPC_FIELDS)
    throw new InvalidError("Arrow IPC field count is outside its bound")
  if (metadata.recordBatchCount < 1 || metadata.recordBatchCount > MAX_ARROW_IPC_BATCHES)
    throw new InvalidError("Arrow IPC record batch count is outside its bound")
  if (metadata.rowCount < 0n || metadata.rowCount > MAX_ARROW_IPC_ROWS)
    throw new InvalidError("Arrow IPC row count is outside its bound")
  if (metadata.dictionaryCount < 0 || metadata.dictionaryCount > MAX_ARROW_IPC_DICTIONARIES)
    throw new InvalidError("Arrow IPC dictionary count is outside its bound")
}

export function validateArrowIpcPolicy(policy: ArrowIpcPolicy): void {
  if (policy.contractVersion !== ARROW_IPC_CONTRACT_VERSION) {
    throw new InvalidError("Arrow IPC contract version is unsupported")
  }
  if (!policy.streamFormatOnly || !policy.selfContained) {
    throw new InvalidError("Arrow IPC input must be one self-contained stream per message")
  }
  if (policy.dictionaryDeltas || policy.replacementDictionaries) {
    throw new InvalidError("Arrow IPC dictionary deltas and replacements are unsupported")
  }
  if (
    !isArrowTimestampUnit(policy.timestampUnit) ||
    policy.maxDecimalBits !== MAX_ARROW_DECIMAL_BITS
  ) {
    throw new InvalidError("Arrow IPC timestamp unit or decimal width is unsupported")
  }
  if (policy.unions || policy.extensionTypes) {
    throw new InvalidError("Arrow IPC unions and extension types are unsupported")
  }
}

function isArrowTimestampUnit(value: string): value is ArrowTimestampUnit {
  return value === "microsecond"
}

function requireTrue(map: CborMap, key: string, context: string): true {
  if (!field.requiredBoolean(map, key, context)) {
    throw new CodecError(`field \`${key}\` must be true`, context, key)
  }
  return true
}

function requireFalse(map: CborMap, key: string, context: string): false {
  if (field.requiredBoolean(map, key, context)) {
    throw new CodecError(`field \`${key}\` must be false`, context, key)
  }
  return false
}

function fixedBytes(value: Uint8Array, length: number, context: string): Uint8Array {
  if (value.length !== length) {
    throw new CodecError(`${context} must contain ${String(length)} bytes`, context, "bytes")
  }
  return value
}

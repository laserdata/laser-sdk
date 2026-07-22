import { type CborMap, field } from "./cbor.js"

export interface ChangeRecord {
  readonly v: number
  readonly index: string
  readonly partitionId: number
  readonly fromOffset: bigint
  readonly toOffset: bigint
  readonly rows: number
}

export function encodeChangeRecord(record: ChangeRecord): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", BigInt(record.v)],
    ["index", record.index],
    ["partition_id", BigInt(record.partitionId)],
    ["from_offset", record.fromOffset],
    ["to_offset", record.toOffset],
    ["rows", BigInt(record.rows)]
  ])
}

export function decodeChangeRecord(map: CborMap, context: string): ChangeRecord {
  return {
    v: field.requiredU32(map, "v", context),
    index: field.requiredString(map, "index", context),
    partitionId: field.requiredU32(map, "partition_id", context),
    fromOffset: field.requiredU64(map, "from_offset", context),
    toOffset: field.requiredU64(map, "to_offset", context),
    rows: field.requiredU32(map, "rows", context)
  }
}

import { type CborMap, field } from "./cbor.js"

export interface ChangeRecord {
  readonly v: number
  readonly index: string
  readonly partitionId: number
  readonly fromOffset: bigint
  readonly toOffset: bigint
  readonly rows: number
  /** The stream whose source the batch read, when the deployment publishes
   * change records per stream. */
  readonly stream?: string
}

export function encodeChangeRecord(record: ChangeRecord): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["v", BigInt(record.v)],
    ["index", record.index],
    ["partition_id", BigInt(record.partitionId)],
    ["from_offset", record.fromOffset],
    ["to_offset", record.toOffset],
    ["rows", BigInt(record.rows)]
  ])
  if (record.stream !== undefined) map.set("stream", record.stream)
  return map
}

export function decodeChangeRecord(map: CborMap, context: string): ChangeRecord {
  return {
    v: field.requiredU32(map, "v", context),
    index: field.requiredString(map, "index", context),
    partitionId: field.requiredU32(map, "partition_id", context),
    fromOffset: field.requiredU64(map, "from_offset", context),
    toOffset: field.requiredU64(map, "to_offset", context),
    rows: field.requiredU32(map, "rows", context),
    ...streamOf(map, context)
  }
}

function streamOf(map: CborMap, context: string): { readonly stream?: string } {
  const stream = field.optionalString(map, "stream", context)
  return stream === undefined ? {} : { stream }
}

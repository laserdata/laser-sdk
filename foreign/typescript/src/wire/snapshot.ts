import { CodecError } from "../client/errors.js"
import { type CborMap, expectMap, field } from "./cbor.js"
import { ConversationId } from "./ids.js"

export interface FoldSnapshot {
  readonly conversation: ConversationId
  readonly asOf: ReadonlyMap<number, bigint>
  readonly state: Uint8Array
}

export function encodeFoldSnapshot(snapshot: FoldSnapshot): Map<string, unknown> {
  const asOf = new Map<bigint, bigint>()
  for (const [partition, offset] of [...snapshot.asOf].sort(([left], [right]) => left - right)) {
    asOf.set(BigInt(partition), offset)
  }
  return new Map<string, unknown>([
    ["conversation", snapshot.conversation.toBytes()],
    ["as_of", asOf],
    ["state", snapshot.state]
  ])
}

export function decodeFoldSnapshot(map: CborMap, context: string): FoldSnapshot {
  const rawAsOf = expectMap(map.get("as_of"), `${context}.as_of`)
  const asOf = new Map<number, bigint>()
  for (const [rawPartition, rawOffset] of rawAsOf) {
    const partition = coercePartition(rawPartition, context)
    if (asOf.has(partition)) {
      throw new CodecError(
        `duplicate partition ${String(partition)} in ${context}.as_of`,
        context,
        "as_of"
      )
    }
    asOf.set(partition, coerceOffset(rawOffset, context))
  }
  return {
    conversation: ConversationId.fromBytes(field.requiredBytes(map, "conversation", context)),
    asOf,
    state: field.requiredBytes(map, "state", context)
  }
}

export function foldSnapshotResumeOffset(snapshot: FoldSnapshot, partition: number): bigint {
  const offset = snapshot.asOf.get(partition)
  if (offset === undefined) return 0n
  return offset === (1n << 64n) - 1n ? offset : offset + 1n
}

function coercePartition(value: unknown, context: string): number {
  if (typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 0xffff_ffff) {
    return value
  }
  throw new CodecError(`partition key in ${context}.as_of must fit u32`, context, "as_of")
}

function coerceOffset(value: unknown, context: string): bigint {
  if (typeof value === "bigint" && value >= 0n) return value
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return BigInt(value)
  throw new CodecError(`offset in ${context}.as_of must fit u64`, context, "as_of")
}

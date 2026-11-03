import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, expectU32, expectU64, field } from "./cbor.js"
import { ConversationId } from "./ids.js"

export interface SnapshotOffset {
  readonly topicId: number
  readonly topicCreatedAtMicros: bigint
  readonly partitionId: number
  readonly offset: bigint
}

export interface FoldSnapshot {
  readonly stream: string
  readonly streamId: number
  readonly streamCreatedAtMicros: bigint
  readonly conversation: ConversationId
  readonly fold: string
  readonly asOf: readonly SnapshotOffset[]
  readonly state: Uint8Array
}

export function encodeFoldSnapshot(snapshot: FoldSnapshot): Map<string, unknown> {
  validateFoldSnapshot(snapshot)
  return new Map<string, unknown>([
    ["stream", snapshot.stream],
    ["stream_id", BigInt(snapshot.streamId)],
    ["stream_created_at_micros", snapshot.streamCreatedAtMicros],
    ["conversation", snapshot.conversation.toBytes()],
    ["fold", snapshot.fold],
    [
      "as_of",
      snapshot.asOf.map((entry) => [
        BigInt(entry.topicId),
        entry.topicCreatedAtMicros,
        BigInt(entry.partitionId),
        entry.offset
      ])
    ],
    ["state", snapshot.state]
  ])
}

export function decodeFoldSnapshot(map: CborMap, context: string): FoldSnapshot {
  const asOf = field.requiredArray(map, "as_of", context, (raw, index) => {
    if (!Array.isArray(raw) || raw.length !== 4)
      throw new CodecError(
        `source offset ${String(index)} must have four integers`,
        context,
        "as_of"
      )
    return {
      topicId: expectU32(raw[0], `${context}.as_of[${String(index)}].topic_id`),
      topicCreatedAtMicros: expectU64(
        raw[1],
        `${context}.as_of[${String(index)}].topic_created_at_micros`
      ),
      partitionId: expectU32(raw[2], `${context}.as_of[${String(index)}].partition_id`),
      offset: expectU64(raw[3], `${context}.as_of[${String(index)}].offset`)
    }
  })
  const snapshot: FoldSnapshot = {
    stream: field.requiredString(map, "stream", context),
    streamId: field.requiredU32(map, "stream_id", context),
    streamCreatedAtMicros: field.requiredU64(map, "stream_created_at_micros", context),
    conversation: ConversationId.fromBytes(field.requiredBytes(map, "conversation", context)),
    fold: field.requiredString(map, "fold", context),
    asOf,
    state: field.requiredBytes(map, "state", context)
  }
  validateFoldSnapshot(snapshot)
  return snapshot
}

export function foldSnapshotResumeOffset(
  snapshot: FoldSnapshot,
  topicId: number,
  topicCreatedAtMicros: bigint,
  partitionId: number
): bigint {
  let low = 0
  let high = snapshot.asOf.length
  const wanted = { topicId, topicCreatedAtMicros, partitionId, offset: 0n }
  while (low < high) {
    const middle = Math.floor((low + high) / 2)
    const entry = snapshot.asOf[middle]
    if (entry === undefined) break
    const order = compareSource(entry, wanted)
    if (order < 0) low = middle + 1
    else high = middle
  }
  const entry = snapshot.asOf[low]
  if (entry === undefined || compareSource(entry, wanted) !== 0) return 0n
  return entry.offset === (1n << 64n) - 1n ? entry.offset : entry.offset + 1n
}

export function validateFoldSnapshot(snapshot: FoldSnapshot): void {
  if (snapshot.stream.length === 0 || snapshot.fold.length === 0)
    throw new InvalidError("snapshot stream and fold must be non-empty")
  for (let index = 1; index < snapshot.asOf.length; index += 1) {
    const previous = snapshot.asOf[index - 1]
    const current = snapshot.asOf[index]
    if (previous !== undefined && current !== undefined && compareSource(previous, current) >= 0)
      throw new InvalidError("snapshot offsets must be sorted with no duplicate source")
  }
}

function compareSource(left: SnapshotOffset, right: SnapshotOffset): number {
  if (left.topicId !== right.topicId) return left.topicId - right.topicId
  if (left.topicCreatedAtMicros !== right.topicCreatedAtMicros)
    return left.topicCreatedAtMicros < right.topicCreatedAtMicros ? -1 : 1
  return left.partitionId - right.partitionId
}

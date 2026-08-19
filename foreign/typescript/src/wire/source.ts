import { InvalidError } from "../client/errors.js"
import { type CborMap, expectMap, field } from "./cbor.js"
import { PhysicalClusterIncarnation } from "./ids.js"

export const MAX_SOURCE_PARTITIONS = 65_536
export interface SourceIncarnation {
  readonly cluster: PhysicalClusterIncarnation
  readonly streamId: number
  readonly topicId: number
  readonly partitionId: number
  readonly partitionCreatedRevision: bigint
}
export interface SourceScope {
  readonly stream: string
  readonly topic: string
}
export interface SourcePartitionCut {
  readonly incarnation: SourceIncarnation
  readonly retainedStart: bigint
  readonly endExclusive: bigint
}
export interface SourceCut {
  readonly partitions: readonly SourcePartitionCut[]
}

export function encodeSourceIncarnation(value: SourceIncarnation): Map<string, unknown> {
  return new Map<string, unknown>([
    ["cluster", value.cluster.toBytes()],
    ["stream_id", value.streamId],
    ["topic_id", value.topicId],
    ["partition_id", value.partitionId],
    ["partition_created_revision", value.partitionCreatedRevision]
  ])
}
export function decodeSourceIncarnation(map: CborMap, context: string): SourceIncarnation {
  return {
    cluster: PhysicalClusterIncarnation.fromBytes(field.requiredBytes(map, "cluster", context)),
    streamId: field.requiredU32(map, "stream_id", context),
    topicId: field.requiredU32(map, "topic_id", context),
    partitionId: field.requiredU32(map, "partition_id", context),
    partitionCreatedRevision: field.requiredU64(map, "partition_created_revision", context)
  }
}
export function encodeSourceScope(value: SourceScope): Map<string, unknown> {
  return new Map([
    ["stream", value.stream],
    ["topic", value.topic]
  ])
}
export function decodeSourceScope(map: CborMap, context: string): SourceScope {
  return {
    stream: field.requiredString(map, "stream", context),
    topic: field.requiredString(map, "topic", context)
  }
}
export function encodeSourcePartitionCut(value: SourcePartitionCut): Map<string, unknown> {
  return new Map<string, unknown>([
    ["incarnation", encodeSourceIncarnation(value.incarnation)],
    ["retained_start", value.retainedStart],
    ["end_exclusive", value.endExclusive]
  ])
}
export function decodeSourcePartitionCut(map: CborMap, context: string): SourcePartitionCut {
  return {
    incarnation: decodeSourceIncarnation(
      field.requiredMap(map, "incarnation", context),
      `${context}.incarnation`
    ),
    retainedStart: field.requiredU64(map, "retained_start", context),
    endExclusive: field.requiredU64(map, "end_exclusive", context)
  }
}
export function encodeSourceCut(value: SourceCut): Map<string, unknown> {
  return new Map<string, unknown>([["partitions", value.partitions.map(encodeSourcePartitionCut)]])
}
export function decodeSourceCut(map: CborMap, context: string): SourceCut {
  return {
    partitions: field.requiredArray(map, "partitions", context, (item, index) =>
      decodeSourcePartitionCut(
        expectMap(item, `${context}.partitions[${String(index)}]`),
        `${context}.partitions[${String(index)}]`
      )
    )
  }
}
export function validateSourceIncarnation(value: SourceIncarnation): void {
  if (value.cluster.asU128() === 0n || value.partitionCreatedRevision === 0n) {
    throw new InvalidError("source incarnation identity or creation revision is invalid")
  }
}
export function validateSourceScope(value: SourceScope): void {
  validateSourceName("stream", value.stream)
  validateSourceName("topic", value.topic)
}
export function validateSourceCut(value: SourceCut): void {
  if (value.partitions.length < 1 || value.partitions.length > MAX_SOURCE_PARTITIONS)
    throw new InvalidError("source cut revision or partition count is invalid")
  let previous = -1
  let namespace: string | undefined
  for (const partition of value.partitions) {
    validateSourceIncarnation(partition.incarnation)
    if (
      partition.incarnation.cluster.asU128() === 0n ||
      partition.incarnation.partitionCreatedRevision === 0n ||
      partition.retainedStart > partition.endExclusive ||
      partition.incarnation.partitionId <= previous
    )
      throw new InvalidError("source cut partition is invalid or not canonically ordered")
    const current = `${partition.incarnation.cluster.asU128().toString()}:${String(partition.incarnation.streamId)}:${String(partition.incarnation.topicId)}`
    if (namespace !== undefined && namespace !== current)
      throw new InvalidError("source cut partitions must share one source namespace")
    namespace = current
    previous = partition.incarnation.partitionId
  }
}

function validateSourceName(label: string, value: string): void {
  const bytes = new TextEncoder().encode(value).length
  if (bytes < 1 || bytes > 255 || hasControlCharacter(value)) {
    throw new InvalidError(`${label} is empty, oversized, or contains a control character`)
  }
}

function hasControlCharacter(value: string): boolean {
  for (const character of value) {
    const code = character.codePointAt(0) ?? 0
    if (code < 32 || code === 127) return true
  }
  return false
}

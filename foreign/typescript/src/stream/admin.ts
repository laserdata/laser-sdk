import { UnsupportedError } from "../client/errors.js"

/** One stream as the server reports it. `Laser.streams` and `Stream.get` return it. */
export interface StreamInfo {
  /** The native numeric stream id. */
  readonly id: number
  readonly name: string
  readonly topicsCount: number
  readonly sizeBytes: bigint
  readonly messagesCount: bigint
  readonly createdAtMicros: bigint
}

/** One topic as the server reports it. `Stream.topics` and `Topic.get` return it. */
export interface TopicInfo {
  /** The native numeric topic id. */
  readonly id: number
  readonly name: string
  readonly partitionsCount: number
  /** How many nodes store each partition. Absent when the server reports no factor. */
  readonly replicationFactor?: number
  /** How long a message is kept, in milliseconds. Absent when messages never expire. */
  readonly messageExpiryMs?: number
  /** The retained size cap in bytes. Absent when the topic is unlimited. */
  readonly maxSizeBytes?: bigint
  readonly sizeBytes: bigint
  readonly messagesCount: bigint
  readonly createdAtMicros: bigint
}

/** One partition of a topic as the server reports it. `Topic.partitions` returns it. */
export interface PartitionInfo {
  readonly id: number
  readonly segmentsCount: number
  /** The offset of the newest message, 0 for an empty partition. */
  readonly currentOffset: bigint
  readonly sizeBytes: bigint
  readonly messagesCount: bigint
  readonly createdAtMicros: bigint
}

/** One consumer group of a topic as the topic listing reports it. `Topic.consumerGroups` returns it. */
export interface ConsumerGroupSummary {
  /** The native numeric group id. */
  readonly id: number
  readonly name: string
  readonly partitionsCount: number
  readonly membersCount: number
}

/** The changes `Topic.update` applies. A field left out keeps its value. */
export interface UpdateTopicOptions {
  readonly name?: string
  /** Keep each message this many milliseconds. */
  readonly expireAfterMs?: number
  /** Keep messages until the size cap removes them. */
  readonly neverExpire?: boolean
  /** Cap the retained size of the topic in bytes. */
  readonly maxTopicBytes?: bigint
  /** Remove the size cap. */
  readonly unlimitedTopicSize?: boolean
}

/** @internal The transport method behind an inspection or administration call. */
export function adminOperation<Method>(method: Method | undefined, operation: string): Method {
  if (method === undefined)
    throw new UnsupportedError(`${operation} needs the Apache Iggy transport`, {
      surface: "streaming"
    })
  return method
}

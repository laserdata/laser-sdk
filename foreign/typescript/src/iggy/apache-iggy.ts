import {
  MANAGED_ATTEMPT_FIRST_MS,
  MANAGED_MUTATION,
  MANAGED_MUTATION_DEADLINE_MS,
  jittered
} from "../retry.js"
import { JOIN_GROUP } from "apache-iggy/dist/wire/consumer-group/join-group.command.js"
import { SYNC_GROUP } from "apache-iggy/dist/wire/consumer-group/sync-group.command.js"
import { GET_STREAM } from "apache-iggy/dist/wire/stream/get-stream.command.js"
import { GET_TOPIC } from "apache-iggy/dist/wire/topic/get-topic.command.js"
import {
  FETCH_MESSAGES,
  fetchSectionError
} from "apache-iggy/dist/wire/message/fetch-messages.command.js"
import { type CallOptions, delay, throwIfAborted } from "../client/call-options.js"
import { connectOptions } from "../client/connect-options.js"
import {
  publishOptions,
  publishWithin,
  retryDelayMs,
  type PublishOptions
} from "../client/publish-options.js"
import {
  Consumer,
  DeserializeError,
  FetchProgressUncertainError,
  HeaderKeyFactory,
  HeaderValue as IggyHeaderValueFactory,
  LaneBlockedError,
  NotConnectedError,
  OperationNotRetainedError,
  OperationUncertainError,
  Partitioning,
  PollingStrategy as IggyPollingStrategy,
  SimpleClient,
  VsrResponseTimeoutError,
  getRawClient
} from "apache-iggy"
import type {
  ClientConfig,
  ClientCredentials,
  PartitionContext,
  RawClient,
  SendCommandOptions,
  SendMessagesConfirmation,
  SendMessagesResponse
} from "apache-iggy"
import { readFileSync } from "node:fs"
import { Socket, isIP } from "node:net"
import { setTimeout as sleep } from "node:timers/promises"
import { TLSSocket } from "node:tls"
import {
  AmbiguousMutationError,
  CancelledError,
  ConfigError,
  SessionError,
  InvalidError,
  ProtocolError,
  PublishFailedError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "../client/errors.js"
import { LASERDATA_ROOT_CA } from "../client/laserdata-ca.js"
import type { ConsumerStart } from "../stream/consumer-start.js"
import type { HeaderValue } from "../stream/header-value.js"
import type { Routing } from "../stream/routing.js"
import { Mutex } from "../runtime/mutex.js"
import { mintUlidValue } from "../runtime/ulid.js"
import { decodeOne, encodeNamed, expectMap } from "../wire/cbor.js"
import {
  AGDX_AUTHZ_BIND_ROLES_CODE,
  AGDX_AUTHZ_DEFINE_ROLE_CODE,
  AGDX_AUTHZ_DELETE_ROLE_CODE,
  AGDX_BATCH_CODE,
  AGDX_COMMAND_BASE,
  isBarrierRead,
  isIdempotentManagedRequest
} from "../wire/codes.js"
import { type BatchReply, decodeBatchReply } from "../wire/batch.js"
import {
  type AppliedFrontier,
  MANAGED_REQUEST_VERSION,
  decodeManagedReplyEnvelope,
  encodeManagedReadEnvelope,
  encodeManagedRequestEnvelope,
  mergeFrontier,
  positionSurfaceOf
} from "../wire/mutation.js"

export interface PolledMessage {
  readonly payload: Uint8Array
  readonly partitionId: number
  readonly offset: bigint
  readonly timestampMicros?: bigint
  readonly headers: ReadonlyMap<string, HeaderValue>
  /**
   * Why the header block did not decode, when it did not: its structure, the
   * `agdx.ct` entry, or another entry. A valid content type can remain available.
   */
  readonly headersMalformed?: HeaderFault
  /** The id the record was sent with. */
  readonly messageId?: bigint
  /** The record checksum as stored. */
  readonly checksum?: bigint
  /** The producer timestamp in microseconds. */
  readonly originTimestampMicros?: bigint
  /** The partition head when the record was read. */
  readonly currentOffset?: bigint
  /** The user-header block exactly as stored, absent when the record has none. */
  readonly userHeaders?: Uint8Array
  /** The partition incarnation and owner the record was read under, which a
   * store of its offset is fenced to. Not enumerable, so a copy of the record
   * leaves it behind. */
  readonly context?: PartitionContext
}

/** The part of a user-header block that did not decode. */
export type HeaderFault = "structure" | "content_type" | "entry"

export type IggyClient = SimpleClient
export type ClientOwnership = "owned" | "borrowed"
export type { PartitionContext, SendMessagesConfirmation, SendMessagesResponse }

/** Settings applied only when a topic is created. `maxTopicSize` is in bytes,
 * `UNLIMITED_TOPIC_SIZE` lifts the limit, and leaving it out keeps the server
 * default. `replicationFactor` is how many nodes store each partition,
 * 1..=`MAX_REPLICATION_FACTOR` and at most the cluster's node count, the
 * server's default when left out. */
export interface TopicCreateSettings {
  readonly messageExpiryMicros?: bigint
  readonly maxTopicSize?: bigint
  readonly replicationFactor?: number
}

/** The most nodes one partition can be stored on, Apache Iggy's cap. */
export const MAX_REPLICATION_FACTOR = 32

/** The `maxTopicSize` value that lifts the topic size limit (u64 max). */
export const UNLIMITED_TOPIC_SIZE = 18_446_744_073_709_551_615n
/** The message expiry that keeps records until retention removes them (u64 max). */
export const NEVER_EXPIRE = 18_446_744_073_709_551_615n

const TOPIC_NAME_ALREADY_EXISTS = 2013
const NOT_FOUND_METADATA_CODES = new Set([1009, 1010, 2010, 2011])
const STREAM_NAME_NOT_FOUND = 1010
const TOPIC_NAME_NOT_FOUND = 2011
// A missing resource, stream, or topic on publish: the cached routing may
// predate a deleted and recreated stream or topic.
const MISSING_RESOURCE_CODES = new Set([20, 1009, 1010, 2010, 2011])
const UNAUTHENTICATED = 40
// `InvalidJsonResponse` and `InvalidBytesResponse`: the batch committed and only
// its confirmation was lost.
const LOST_CONFIRMATION_CODES: ReadonlySet<number> = new Set([302, 303])
const POLL_MESSAGES_DEFERRED_CODE = 106
const PUBLISH_BATCH_LENGTH = 1_000
const NUMERIC_IDENTIFIER = 1
const STRING_IDENTIFIER = 2
// Server replies a later attempt can clear: a write the cluster did not
// commit or admit, and a partition the node could not read yet.
const TRANSIENT_SERVER_CODES: ReadonlySet<number> = new Set([57, 58, 3004, 10002])
const PARTITION_NOT_FOUND = 3007
// A topic's cached partition count is read again once this old, so keyed and
// balanced publishes reach partitions added since.
const PARTITION_COUNT_MAX_AGE_MS = 5_000
const DEFAULT_RECONNECT_INTERVAL_MS = 1_000
// Apache Iggy's spelling of `reconnection_retries=unlimited`, u32::MAX.
const UNLIMITED_RECONNECT_RETRIES = 4_294_967_295
// The Apache Iggy TCP default port, as in Rust.
const DEFAULT_TCP_PORT = 8090
const ACCEPT_STAGE = "Iggy server to accept the connection"
const LOGIN_STAGE = "Iggy login reply"
const VSR_HEARTBEAT_INTERVAL_MS = 5_000
// partition_id u32, current_offset u64, messages_count u32, then the
// partition context: incarnation, owner generation, and metadata op, u64 each.
export const POLLED_HEAD_BYTES = 40
const BATCH_HEADER_BYTES = 256
const FRAME_HEADER_BYTES = 48
/** The smallest poll reply byte cap that can hold one record: the polled head,
 * one batch header and one frame header, as Apache Iggy
 * `MIN_DEFERRED_POLL_MAX_BYTES`. */
export const MIN_DEFERRED_POLL_MAX_BYTES =
  POLLED_HEAD_BYTES + BATCH_HEADER_BYTES + FRAME_HEADER_BYTES
/** The longest server wait of a deferred poll, 600 s. */
export const MAX_DEFERRED_POLL_WAIT_MS = 600_000
/** The poll reply byte cap Apache Iggy uses by default. */
export const DEFAULT_POLL_MAX_BYTES = 1_048_576
// The client gives a deferred poll its wait plus this long for the final read
// and the reply, as the Apache Iggy clients do.
const DEFERRED_POLL_REPLY_MARGIN_MS = 10_000
const US_PER_MS = 1_000n
const STRING_HEADER_KIND = 2
const CONTENT_TYPE_HEADER = "agdx.ct"
const STRICT_UTF8 = new TextDecoder("utf-8", { fatal: true })

export function toNodeBuffer(bytes: Uint8Array): Buffer {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength)
}

export interface MessageWithHeaders {
  readonly payload: Uint8Array
  readonly headers: ReadonlyMap<string, HeaderValue>
  /** The message id. A record without one, or with `0n`, gets a fresh id
   * before its first attempt, and every retry and a resend from a failure
   * report keep it, so the server deduplicates them. */
  readonly id?: bigint
}

/** The records with an id each: a record without one gets a fresh id. */
export function withMessageIds(records: readonly MessageWithHeaders[]): MessageWithHeaders[] {
  return records.map((record) =>
    record.id === undefined || record.id === 0n ? { ...record, id: mintUlidValue() } : record
  )
}

const SYNC_CONSUMER_GROUP_CODE = 606
const GROUP_MEMBER_NOT_FOUND = 5006
const GROUP_PARTITION_NOT_OWNED = 5009
const RESYNC_REQUIRED_PARTITION = 0xffff_ffff
const GROUP_ASSIGNMENT_REFRESH_MS = 5_000
const GROUP_POLL_MAX_ATTEMPTS = 2
const TRANSIENT_NOT_COMMITTED = 57
const INVALID_IDENTIFIER = 6
const NOT_PRIMARY = 89
const GET_PARTITION_ROUTE_CODE = 105
// incarnation u64, metadata_op u64, view u32, partitions_count u32, primary_replica u8
const PARTITION_ROUTE_BYTES = 25
const U64_MASK = (1n << 64n) - 1n

interface GroupCursor {
  generation: bigint
  partitions: readonly number[]
  position: number
  synchronizedAt: number
  // Consecutive reads that found nothing, so a whole idle pass holds the next reads.
  idle: number
}
interface GroupPollState {
  readonly cursors: Map<string, GroupCursor>
  sessionGeneration: number
}
// A topic's partition count and when it was read.
interface PartitionCount {
  readonly count: number
  readonly readAt: number
}
const GROUP_POLL_STATES = new WeakMap<RawClient, GroupPollState>()

/**
 * How the server holds a poll: up to `maxWaitMs` for `minCount` new records,
 * answering with at most `maxBytes` of records. A record larger than the cap
 * comes alone.
 */
export interface PollWait {
  readonly maxWaitMs: number
  readonly minCount: number
  readonly maxBytes: number
}

/** The records one partition of a held fetch answered with. */
export interface HeldPartition {
  readonly partitionId: number
  readonly messages: readonly PolledMessage[]
}

/** A read that answers at once, under the default reply byte cap. */
export const ONE_SHOT_POLL: PollWait = Object.freeze({
  maxWaitMs: 0,
  minCount: 1,
  maxBytes: DEFAULT_POLL_MAX_BYTES
})

export type ConsumerTarget =
  | { readonly kind: "single"; readonly partitionId: number; readonly name?: string }
  | { readonly kind: "group"; readonly name: string; readonly partitionId?: number }

export type ConsumerOffsetTarget =
  | { readonly kind: "group"; readonly name: string }
  | { readonly kind: "consumer"; readonly name: string }

/** Where a managed request goes, as Rust `nodes::Target`. */
export type ManagedTarget =
  /** The metadata primary, the only node that submits the replicated authorization writes. */
  | { readonly kind: "metadata_primary" }
  /** The node this client spreads its requests to. */
  | { readonly kind: "caller" }
  /** One replica, by the id cluster metadata names it with. */
  | { readonly kind: "replica"; readonly replica: number }

/** A managed reply and the replica that sent it, absent when the metadata primary answered. */
export interface ManagedAnswer {
  /** The reply, opened from the positioned envelope a managed node may wrap it in. */
  readonly reply: Uint8Array
  /** The applied frontier the positioned envelope carried. */
  readonly frontier?: AppliedFrontier
  readonly replica?: number
}

export interface ManagedSendOptions {
  readonly operationId?: bigint
  readonly retryAfterReconnect?: boolean
  /** The node the request goes to, when not the one its code picks. */
  readonly target?: ManagedTarget
  /** When the call gives up, as an epoch time in milliseconds, for a request
   * the server may hold. Defaults to the client's 30 second call bound. */
  readonly deadline?: number
  /** Overrides the mutation deadline, for tests.
   * @internal */
  readonly mutationDeadlineMs?: number
  /** Marked once an attempt wrote the frame, so a later attempt's failure
   * cannot hide that its outcome is unknown.
   * @internal */
  readonly written?: FrameWritten
}

/** Whether any attempt of one managed send wrote its frame to a connection. */
export interface FrameWritten {
  written: boolean
}

/** A consumer offset stored under the partition owner a filtered page was read under. */
export interface FencedPosition {
  readonly streamId: number
  readonly topicId: number
  /** A group by the numeric id its page named, or an independent consumer by name. */
  readonly consumer:
    | { readonly kind: "group"; readonly id: number }
    | { readonly kind: "consumer"; readonly name: string }
  readonly partitionId: number
  readonly offset: bigint
  readonly context: PartitionContext
}

export interface LaserTransport {
  readonly publishRetriesManaged?: boolean
  readonly kind: "apache-iggy"
  readonly iggyClient: SimpleClient
  sendManaged(code: number, payload: Uint8Array, options?: ManagedSendOptions): Promise<Uint8Array>
  /** Sends managed command `code` to `target`, and names the replica that answered. */
  sendManagedTo?(
    code: number,
    payload: Uint8Array,
    target: ManagedTarget,
    options?: ManagedSendOptions
  ): Promise<ManagedAnswer>
  /** The node every managed request of this connection goes to, shared by its clones. */
  replicaIds?(): Promise<readonly number[]>
  readonly nodes?: Nodes
  /** The replica id of one partition's primary. */
  partitionPrimary?(streamId: string, topicId: string, partitionId: number): Promise<number>
  /** Stores a consumer group offset fenced to the partition incarnation and owner it was read under. */
  storePosition?(position: FencedPosition): Promise<void>
  /** The highest mutation position seen per managed surface. */
  seenPositions?(): AppliedFrontier
  /** Raises the seen positions to a frontier another connection observed. */
  observePositions?(frontier: AppliedFrontier): void
  ensureStream(name: string): Promise<void>
  deleteStream(name: string): Promise<boolean>
  ensureTopic(
    streamId: string,
    topicId: string,
    partitions: number,
    replicationFactor?: number
  ): Promise<void>
  ensureConsumerGroup(streamId: string, topicId: string, name: string): Promise<void>
  ensureTopicWithExpiry?(
    streamId: string,
    topicId: string,
    partitions: number,
    messageExpiryMicros: bigint,
    replicationFactor?: number
  ): Promise<void>
  /** Creates the topic with these settings when it is absent. An existing
   * topic is left as it is, like Rust `create_topic_if_not_exists`. */
  createTopicIfAbsent?(
    streamId: string,
    topicId: string,
    partitions: number,
    settings: TopicCreateSettings
  ): Promise<void>
  findTopicPartitionCount(streamId: string, topicId: string): Promise<number | undefined>
  findSnapshotStream?(
    stream: string
  ): Promise<{ readonly id: number; readonly createdAtMicros: bigint } | undefined>
  findSnapshotTopic?(
    stream: string,
    topic: string
  ): Promise<
    | { readonly id: number; readonly createdAtMicros: bigint; readonly partitions: number }
    | undefined
  >
  getTopicPartitionCount(streamId: string, topicId: string): Promise<number>
  resolveStreamTopicIds?(
    streamId: string,
    topicId: string
  ): Promise<{ readonly streamId: number; readonly topicId: number }>
  resolveStreamTopicNames?(
    streamId: number,
    topicId: number
  ): Promise<{ readonly stream: string; readonly topic: string } | undefined>
  sendMessages(
    streamId: string,
    topicId: string,
    payloads: readonly Uint8Array[],
    routing: Routing,
    options?: CallOptions
  ): Promise<SendMessagesResponse>
  /** Sends one message with exact headers and optional key or partition routing. */
  sendMessageWithHeaders(
    streamId: string,
    topicId: string,
    payload: Uint8Array,
    headers: ReadonlyMap<string, HeaderValue>,
    partitionKey?: string | Uint8Array,
    partitionId?: number,
    options?: CallOptions
  ): Promise<SendMessagesResponse>
  /** Sends the records in requests of at most `batchLength` (1000 unless
   * set). A failure throws `PublishFailedError` with the confirmed requests
   * and the records from the failed request on. An aborted `signal` starts
   * no new attempt and no retry wait. */
  sendMessagesWithHeaders(
    streamId: string,
    topicId: string,
    messages: readonly MessageWithHeaders[],
    partitionKey?: string | Uint8Array,
    partitionId?: number,
    options?: Partial<PublishOptions> &
      CallOptions & {
        readonly batchLength?: number
        /** Background mode: resend at once, then every this many milliseconds, any failure but a lost confirmation. */
        readonly fixedRetryIntervalMs?: number
        readonly beforeSend?: () => Promise<{
          readonly streamId: number
          readonly topicId: number
          readonly partitions: number
        }>
      }
  ): Promise<SendMessagesResponse>
  /**
   * One poll, held on the server as `wait` says. A one-shot read when absent.
   * A group poll without a partition reads the member's partitions in turn
   * and holds a read only after a whole pass found nothing, each for at most
   * `wait.maxWaitMs / partitions`.
   */
  pollMessages(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    strategy: ConsumerStart,
    count: number,
    autoCommit: boolean,
    wait?: PollWait,
    signal?: AbortSignal
  ): Promise<readonly PolledMessage[]>
  /** The partitions grouped by their primary, each group small enough for
   * one fetch. `undefined` when the client cannot route a fetch. */
  primaryGroups?(
    streamId: string,
    topicId: string,
    partitions: readonly number[]
  ): Promise<readonly (readonly number[])[] | undefined>
  /**
   * One read the server holds on partitions that share a primary, answered
   * once any of them has a record at or after its offset or `wait` elapses.
   * It takes one of the client's 64 deferred request slots, however many
   * partitions it names.
   */
  fetchHeld?(
    streamId: string,
    topicId: string,
    offsets: ReadonlyMap<number, bigint>,
    count: number,
    wait: PollWait,
    signal?: AbortSignal,
    reader?: string
  ): Promise<readonly HeldPartition[]>
  /** Stores a consumer offset. With the `context` a record was read under,
   * the store is refused once the partition was recreated (87) or moved to
   * another group member (5009), instead of landing on the newer history. */
  storeOffset(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    partitionId: number,
    offset: bigint,
    context?: PartitionContext
  ): Promise<void>
  getConsumerOffset?(
    streamId: string,
    topicId: string,
    target: ConsumerOffsetTarget,
    partitionId: number
  ): Promise<{ readonly storedOffset: bigint; readonly currentOffset: bigint } | undefined>
  /** Deletes the stored server offset of a consumer for one partition. */
  deleteOffset?(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    partitionId: number
  ): Promise<void>
  joinConsumerGroup(streamId: string, topicId: string, name: string): Promise<void>
  syncConsumerGroup?(
    streamId: string,
    topicId: string,
    name: string
  ): Promise<
    | {
        readonly generation: bigint
        readonly partitions: readonly number[]
        readonly rejoined?: boolean
      }
    | undefined
  >
  leaveConsumerGroup(streamId: string, topicId: string, name: string | number): Promise<void>
  /** Join an existing consumer group without creating it. */
  joinExistingConsumerGroup?(
    streamId: string,
    topicId: string,
    name: string | number
  ): Promise<void>
  /**
   * A second authenticated connection to the node this transport reaches,
   * which holds its own consumer-group memberships and never rejoins them on
   * its own. A group reader joins through it, so every reader is its own
   * member and a dropped reader leaves with its connection.
   */
  openCoordinator?(): Promise<CoordinatorConnection>
  /** Whether this transport can open node and coordinator connections of its own. */
  readonly connectsNodes?: boolean
  /** How long one publish may take, in milliseconds. */
  publishTimeoutMs?(): number
  /** The publish timeout, retry count, and backoff this connection uses. */
  publishOptions?(): PublishOptions
  close(): Promise<void>
}

/** A connection owned by its opener. A managed command goes to its node like any managed request. */
export interface NodeConnection {
  send(code: number, payload: Uint8Array): Promise<Uint8Array>
  close(): Promise<void>
}

/**
 * A dedicated coordinator connection, owned by its opener. It holds a group
 * reader's membership, and its targeted requests pick their node with the
 * opener's node choice, as a Rust reader sends over its own client with the
 * `Laser`'s nodes.
 */
export interface CoordinatorConnection extends NodeConnection {
  sendManagedTo(
    code: number,
    payload: Uint8Array,
    target: ManagedTarget,
    options?: { readonly deadline?: number }
  ): Promise<ManagedAnswer>
  partitionPrimary(streamId: string, topicId: string, partitionId: number): Promise<number>
  storePosition(position: FencedPosition): Promise<void>
  /** Stores a group offset as this connection's member, which owns the partition. */
  storeOffset?(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    partitionId: number,
    offset: bigint
  ): Promise<void>
  joinConsumerGroup(streamId: string, topicId: string, name: string | number): Promise<void>
  leaveConsumerGroup(streamId: string, topicId: string, name: string | number): Promise<void>
}

function toIggyConsumer(target: ConsumerTarget) {
  return target.kind === "single"
    ? target.name === undefined
      ? Consumer.Single
      : { kind: 1 as const, id: target.name }
    : Consumer.Group(target.name)
}

// The deferred poll body: the plain poll's consumer, stream, topic,
// partition, strategy, count and auto-commit flag, then the wait, the minimum
// count, the reply byte cap and the request timeout, laid out as the Apache
// Iggy client lays them.
function encodePollRequest(
  streamId: string,
  topicId: string,
  target: ConsumerTarget,
  partitionId: number,
  strategy: ConsumerStart,
  count: number,
  autoCommit: boolean,
  wait: PollWait
): Uint8Array {
  const consumer =
    target.kind === "group" ? encodeIdentifier(target.name) : encodeIdentifier(target.name ?? 0)
  const parts = [
    Uint8Array.of(target.kind === "group" ? 2 : 1),
    consumer,
    encodeIdentifier(streamId),
    encodeIdentifier(topicId)
  ]
  const tail = new Uint8Array(19)
  const view = new DataView(tail.buffer)
  view.setUint8(0, 1)
  view.setUint32(1, partitionId, true)
  const polling = toIggyPollingStrategy(strategy)
  view.setUint8(5, polling.kind)
  view.setBigUint64(6, polling.value, true)
  view.setUint32(14, count, true)
  view.setUint8(18, autoCommit ? 1 : 0)
  parts.push(tail)
  const deferred = new Uint8Array(24)
  const options = new DataView(deferred.buffer)
  options.setBigUint64(0, BigInt(wait.maxWaitMs) * US_PER_MS, true)
  options.setUint32(8, wait.minCount, true)
  options.setUint32(12, wait.maxBytes, true)
  options.setBigUint64(16, BigInt(wait.maxWaitMs + DEFERRED_POLL_REPLY_MARGIN_MS) * US_PER_MS, true)
  parts.push(deferred)
  const request = new Uint8Array(parts.reduce((size, part) => size + part.byteLength, 0))
  let offset = 0
  for (const part of parts) {
    request.set(part, offset)
    offset += part.byteLength
  }
  return request
}

function encodeIdentifier(value: string | number): Uint8Array {
  if (typeof value === "number") {
    const numeric = new Uint8Array(6)
    const view = new DataView(numeric.buffer)
    view.setUint8(0, NUMERIC_IDENTIFIER)
    view.setUint8(1, 4)
    view.setUint32(2, value, true)
    return numeric
  }
  const name = new TextEncoder().encode(value)
  if (name.byteLength === 0 || name.byteLength > 255)
    throw new ConfigError("an Iggy identifier must contain 1 to 255 UTF-8 bytes")
  const encoded = new Uint8Array(2 + name.byteLength)
  encoded[0] = STRING_IDENTIFIER
  encoded[1] = name.byteLength
  encoded.set(name, 2)
  return encoded
}

function toIggyOffsetConsumer(target: ConsumerOffsetTarget) {
  return target.kind === "group"
    ? Consumer.Group(target.name)
    : { kind: 1 as const, id: target.name }
}

function rotateLeft(value: number, bits: number): number {
  return ((value << bits) | (value >>> (32 - bits))) >>> 0
}

function xxHashRound(accumulator: number, lane: number): number {
  const added = (accumulator + Math.imul(lane, 0x85ebca77)) >>> 0
  return Math.imul(rotateLeft(added, 13), 0x9e3779b1) >>> 0
}

export function xxHash32(bytes: Uint8Array): number {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  let offset = 0
  let hash: number
  if (bytes.byteLength >= 16) {
    let lane1 = 0x24234428
    let lane2 = 0x85ebca77
    let lane3 = 0
    let lane4 = 0x61c8864f
    const limit = bytes.byteLength - 16
    do {
      lane1 = xxHashRound(lane1, view.getUint32(offset, true))
      lane2 = xxHashRound(lane2, view.getUint32(offset + 4, true))
      lane3 = xxHashRound(lane3, view.getUint32(offset + 8, true))
      lane4 = xxHashRound(lane4, view.getUint32(offset + 12, true))
      offset += 16
    } while (offset <= limit)
    hash =
      (rotateLeft(lane1, 1) +
        rotateLeft(lane2, 7) +
        rotateLeft(lane3, 12) +
        rotateLeft(lane4, 18)) >>>
      0
  } else {
    hash = 0x165667b1
  }
  hash = (hash + bytes.byteLength) >>> 0
  while (offset + 4 <= bytes.byteLength) {
    hash = (hash + Math.imul(view.getUint32(offset, true), 0xc2b2ae3d)) >>> 0
    hash = Math.imul(rotateLeft(hash, 17), 0x27d4eb2f) >>> 0
    offset += 4
  }
  while (offset < bytes.byteLength) {
    hash = (hash + Math.imul(bytes[offset] ?? 0, 0x165667b1)) >>> 0
    hash = Math.imul(rotateLeft(hash, 11), 0x9e3779b1) >>> 0
    offset += 1
  }
  hash ^= hash >>> 15
  hash = Math.imul(hash, 0x85ebca77) >>> 0
  hash ^= hash >>> 13
  hash = Math.imul(hash, 0xc2b2ae3d) >>> 0
  hash ^= hash >>> 16
  return hash >>> 0
}

function toIggyPollingStrategy(strategy: ConsumerStart) {
  switch (strategy.kind) {
    case "first":
      return IggyPollingStrategy.First
    case "last":
      return IggyPollingStrategy.Last
    case "next":
      return IggyPollingStrategy.Next
    case "offset":
      return IggyPollingStrategy.Offset(strategy.value)
    case "timestampMicros":
      return IggyPollingStrategy.Timestamp(strategy.value)
  }
}

function toIggyHeaderValue(value: HeaderValue) {
  switch (value.kind) {
    case "raw":
      return IggyHeaderValueFactory.Raw(toNodeBuffer(value.value))
    case "string":
      return IggyHeaderValueFactory.String(value.value)
    case "bool":
      return IggyHeaderValueFactory.Bool(value.value)
    case "int8":
      return IggyHeaderValueFactory.Int8(value.value)
    case "int16":
      return IggyHeaderValueFactory.Int16(value.value)
    case "int32":
      return IggyHeaderValueFactory.Int32(value.value)
    case "int64":
      return IggyHeaderValueFactory.Int64(value.value)
    case "int128":
      return IggyHeaderValueFactory.Int128(toNodeBuffer(value.value))
    case "uint8":
      return IggyHeaderValueFactory.Uint8(value.value)
    case "uint16":
      return IggyHeaderValueFactory.Uint16(value.value)
    case "uint32":
      return IggyHeaderValueFactory.Uint32(value.value)
    case "uint64":
      return IggyHeaderValueFactory.Uint64(value.value)
    case "uint128":
      return IggyHeaderValueFactory.Uint128(toNodeBuffer(value.value))
    case "float":
      return IggyHeaderValueFactory.Float(value.value)
    case "double":
      return IggyHeaderValueFactory.Double(value.value)
  }
}

/** The Iggy error code a server reply carried, through any transport wrapping. */
/** Refuses a replication factor the server never accepts, before anything is
 * sent. The server also caps it at the cluster's node count. */
export function validateReplicationFactor(factor: number): void {
  if (!Number.isInteger(factor) || factor < 1 || factor > MAX_REPLICATION_FACTOR)
    throw new InvalidError(`replication factor must be 1..=${String(MAX_REPLICATION_FACTOR)}`)
}

export function serverErrorCode(error: unknown): number | undefined {
  let current = error
  for (let depth = 0; depth < 8; depth += 1) {
    if (typeof current !== "object" || current === null) return undefined
    const code = (current as { readonly errorCode?: unknown }).errorCode
    if (typeof code === "number") return code
    current = "cause" in current ? (current as { readonly cause?: unknown }).cause : undefined
  }
  return undefined
}

/** The head of a standard polled-messages body. */
export interface PolledHead {
  readonly partitionId: number
  readonly currentOffset: bigint
  readonly count: number
  /** The partition incarnation, owner, and metadata op the poll was served under. */
  readonly context: PartitionContext
}

export function decodePolledHead(body: Uint8Array): PolledHead {
  if (body.byteLength < POLLED_HEAD_BYTES)
    throw new ProtocolError("the polled record body has a malformed head")
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength)
  return {
    partitionId: view.getUint32(0, true),
    currentOffset: view.getBigUint64(4, true),
    count: view.getUint32(12, true),
    context: {
      incarnation: view.getBigUint64(16, true),
      ownerGeneration: view.getBigUint64(24, true),
      metadataOp: view.getBigUint64(32, true)
    }
  }
}

/**
 * The records of a standard polled-messages body, decoded exactly as stored:
 * original offsets, per-record microsecond timestamps, payloads, and typed
 * user headers.
 */
export function decodePolledBody(body: Uint8Array): readonly PolledMessage[] {
  const malformed = (what: string): ProtocolError =>
    new ProtocolError(`the polled record body has a malformed ${what}`)
  const { partitionId, currentOffset, context } = decodePolledHead(body)
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength)
  const messages: PolledMessage[] = []
  let position = POLLED_HEAD_BYTES
  while (position < body.byteLength) {
    if (position + BATCH_HEADER_BYTES > body.byteLength) throw malformed("batch header")
    const baseOffset = view.getBigUint64(position + 8, true)
    const baseTimestamp = view.getBigUint64(position + 16, true)
    const originTimestamp = view.getBigUint64(position + 24, true)
    const batchLength = view.getBigUint64(position + 32, true)
    if (
      batchLength < BigInt(BATCH_HEADER_BYTES) ||
      batchLength > BigInt(body.byteLength - position)
    )
      throw malformed("batch length")
    const batchEnd = position + Number(batchLength)
    position += BATCH_HEADER_BYTES
    while (position < batchEnd) {
      if (position + FRAME_HEADER_BYTES > batchEnd) throw malformed("message frame")
      const offsetDelta = view.getUint32(position + 24, true)
      const headersLength = view.getUint32(position + 32, true)
      const payloadLength = view.getUint32(position + 36, true)
      const payloadStart = position + FRAME_HEADER_BYTES
      const headersStart = payloadStart + payloadLength
      const frameEnd = headersStart + headersLength
      if (frameEnd > batchEnd) throw malformed("message frame")
      const block = body.subarray(headersStart, frameEnd)
      const headers = decodeUserHeaders(block)
      const contentType = headers === "entry" ? decodeUserHeaders(block, true) : headers
      const message: PolledMessage = {
        payload: body.subarray(payloadStart, headersStart),
        partitionId,
        offset: baseOffset + BigInt(offsetDelta),
        timestampMicros: baseTimestamp,
        messageId:
          view.getBigUint64(position + 8, true) | (view.getBigUint64(position + 16, true) << 64n),
        checksum: view.getBigUint64(position, true),
        originTimestampMicros: originTimestamp + BigInt(view.getUint32(position + 28, true)),
        currentOffset,
        ...(block.byteLength > 0 ? { userHeaders: block } : {}),
        ...(typeof headers === "string"
          ? {
              headers: typeof contentType === "string" ? new Map() : contentType,
              headersMalformed: typeof contentType === "string" ? contentType : headers
            }
          : { headers })
      }
      messages.push(Object.defineProperty(message, "context", { value: context }))
      position = frameEnd
    }
  }
  return messages
}

// The typed user headers as the server reads them for a filter: string keys
// only, a kind this build does not know kept raw, a value that does not fit
// its kind faulting the record, and the last of a repeated key kept. A structurally broken
// block names what did not decode, which the server treats as a malformed
// record when its filter needs that part.
function decodeUserHeaders(
  block: Uint8Array,
  contentTypeOnly = false
): ReadonlyMap<string, HeaderValue> | HeaderFault {
  const headers = new Map<string, HeaderValue>()
  const view = new DataView(block.buffer, block.byteOffset, block.byteLength)
  let position = 0
  while (position < block.byteLength) {
    if (position + 5 > block.byteLength) return "structure"
    const keyKind = view.getUint8(position)
    const keyStart = position + 5
    const keyEnd = keyStart + view.getUint32(position + 1, true)
    if (
      keyKind === 0 ||
      keyEnd === keyStart ||
      keyEnd - keyStart > 255 ||
      keyEnd + 5 > block.byteLength
    )
      return "structure"
    const valueKind = view.getUint8(keyEnd)
    const valueStart = keyEnd + 5
    const valueEnd = valueStart + view.getUint32(keyEnd + 1, true)
    if (
      valueKind === 0 ||
      valueEnd === valueStart ||
      valueEnd - valueStart > 255 ||
      valueEnd > block.byteLength
    )
      return "structure"
    position = valueEnd
    if (keyKind !== STRING_HEADER_KIND) continue
    if (
      contentTypeOnly &&
      (keyEnd - keyStart !== CONTENT_TYPE_HEADER.length ||
        CONTENT_TYPE_HEADER.split("").some(
          (character, index) => block[keyStart + index] !== character.charCodeAt(0)
        ))
    )
      continue
    let key: string
    try {
      key = STRICT_UTF8.decode(block.subarray(keyStart, keyEnd))
    } catch {
      return "entry"
    }
    const value = headerValueOf(valueKind, block.subarray(valueStart, valueEnd))
    if (value === undefined) return key === CONTENT_TYPE_HEADER ? "content_type" : "entry"
    headers.set(key, value)
  }
  return headers
}

function headerValueOf(kind: number, bytes: Uint8Array): HeaderValue | undefined {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
  const sized = (size: number): boolean => bytes.byteLength === size
  switch (kind) {
    case STRING_HEADER_KIND:
      try {
        return { kind: "string", value: STRICT_UTF8.decode(bytes) }
      } catch {
        return undefined
      }
    case 3:
      return sized(1) && (bytes[0] === 0 || bytes[0] === 1)
        ? { kind: "bool", value: bytes[0] === 1 }
        : undefined
    case 4:
      return sized(1) ? { kind: "int8", value: view.getInt8(0) } : undefined
    case 5:
      return sized(2) ? { kind: "int16", value: view.getInt16(0, true) } : undefined
    case 6:
      return sized(4) ? { kind: "int32", value: view.getInt32(0, true) } : undefined
    case 7:
      return sized(8) ? { kind: "int64", value: view.getBigInt64(0, true) } : undefined
    case 8:
      return sized(16) ? { kind: "int128", value: bytes.slice() } : undefined
    case 9:
      return sized(1) ? { kind: "uint8", value: view.getUint8(0) } : undefined
    case 10:
      return sized(2) ? { kind: "uint16", value: view.getUint16(0, true) } : undefined
    case 11:
      return sized(4) ? { kind: "uint32", value: view.getUint32(0, true) } : undefined
    case 12:
      return sized(8) ? { kind: "uint64", value: view.getBigUint64(0, true) } : undefined
    case 13:
      return sized(16) ? { kind: "uint128", value: bytes.slice() } : undefined
    case 14:
      return sized(4) ? { kind: "float", value: view.getFloat32(0, true) } : undefined
    case 15:
      return sized(8) ? { kind: "double", value: view.getFloat64(0, true) } : undefined
    default:
      return { kind: "raw", value: bytes.slice() }
  }
}

interface ParsedConnectionString {
  readonly host: string
  readonly port: number
  /** The TLS name to verify, when it differs from `host`. */
  readonly servername?: string
  readonly credentials: ClientCredentials
  readonly tls: boolean
  readonly ca?: string
  readonly reconnection: {
    readonly intervalMs: number
    readonly maxRetries: number | undefined
  }
  readonly heartbeatIntervalMs?: number
  readonly noDelay?: boolean
  /** Validated like Rust. The Node transport reconnects on its own schedule. */
  readonly reestablishAfterMs?: number
}

function laserDataHost(host: string): boolean {
  const normalized = host.toLowerCase()
  return (
    normalized === "laserdata.cloud" ||
    normalized.endsWith(".laserdata.cloud") ||
    normalized === "laserdata.com" ||
    normalized.endsWith(".laserdata.com")
  )
}

// Apache Iggy's TCP connection string options, the only ones Rust and Python
// accept.
const CONNECTION_OPTIONS: ReadonlySet<string> = new Set([
  "tls",
  "tls_domain",
  "tls_ca_file",
  "reconnection_retries",
  "reconnection_interval",
  "reestablish_after",
  "heartbeat_interval",
  "nodelay"
])

function parseDuration(name: string, value: string): number {
  const match = /^(\d+)(ms|s|m)$/.exec(value)
  if (match === null) {
    throw new ConfigError(`${name} must use ms, s, or m`)
  }
  const amount = Number(match[1])
  const unit = match[2]
  const multiplier = unit === "ms" ? 1 : unit === "s" ? 1_000 : 60_000
  const durationMs = amount * multiplier
  if (!Number.isSafeInteger(durationMs) || durationMs < 0) {
    throw new ConfigError(`${name} is outside the supported range`)
  }
  return durationMs
}

function parseReconnectRetries(value: string | undefined): number | undefined {
  if (value === "unlimited") return undefined
  if (value === undefined) return undefined
  const retries = Number(value)
  if (!/^\d+$/.test(value) || !Number.isSafeInteger(retries)) {
    throw new ConfigError("reconnection_retries must be a non-negative integer or unlimited")
  }
  return retries
}

// The scheme is optional. A `://` after the userinfo has begun belongs to a
// password, not to a scheme. `apache-iggy` dials TCP only, so QUIC and
// WebSocket stay with the Rust and Python clients.
function stripScheme(value: string): string {
  for (const scheme of ["iggy://", "iggy+tcp://"]) {
    if (value.startsWith(scheme)) return value.slice(scheme.length)
  }
  const separator = value.indexOf("://")
  if (separator >= 0 && !/[:@]/.test(value.slice(0, separator))) {
    const scheme = value.slice(0, separator)
    if (scheme === "iggy+quic" || scheme === "iggy+ws") {
      throw new ConfigError(
        "the TypeScript client connects over TCP, plain or TLS: use iggy:// or iggy+tcp://, QUIC and WebSocket are served by the Rust and Python clients"
      )
    }
    if (scheme === "iggy+http") {
      throw new ConfigError(
        "iggy+http:// is not supported: HTTP has no binary request path for Laser features, use iggy:// or iggy+tcp://"
      )
    }
    throw new ConfigError("unsupported connection string scheme: use iggy:// or iggy+tcp://")
  }
  return value
}

// `user:password` or a personal access token, both non-empty and
// percent-decoded, so a secret carries `:`, `@`, `?` and `%` as `%3A`, `%40`,
// `%3F` and `%25`. A raw `@` or a second raw `:` would split them again.
function parseCredentials(userinfo: string): ClientCredentials {
  if (userinfo.includes("@")) {
    throw new ConfigError("connection string credentials cannot contain a raw `@`, write it as %40")
  }
  const parts = userinfo.split(":")
  if (parts.length > 2) {
    throw new ConfigError(
      "connection string credentials cannot contain more than one raw `:`, write it as %3A"
    )
  }
  const [first = "", password] = parts
  if (password === undefined) {
    if (first.length === 0) throw new ConfigError("connection string has an empty access token")
    return { token: decodeCredential(first) }
  }
  if (first.length === 0 || password.length === 0) {
    throw new ConfigError("connection string has an empty username or password")
  }
  return { username: decodeCredential(first), password: decodeCredential(password) }
}

// The error never echoes the credential.
function decodeCredential(encoded: string): string {
  try {
    return decodeURIComponent(encoded)
  } catch {
    throw new ConfigError("connection string credentials have a malformed percent escape")
  }
}

// `host`, `host:port`, `[ipv6]` or `[ipv6]:port`. A missing port becomes 8090.
// The host of an IPv6 address is dialed without its brackets.
function parseAddress(address: string): { readonly host: string; readonly port: number } {
  const malformed = (): ConfigError =>
    new ConfigError(
      "connection string address must be host, host:port, [ipv6] or [ipv6]:port, without a path"
    )
  if (/[/#]/.test(address)) throw malformed()
  let host: string
  let portText: string | undefined
  if (address.startsWith("[")) {
    const closing = address.indexOf("]")
    if (closing < 0) throw malformed()
    host = address.slice(1, closing)
    if (isIP(host) !== 6) {
      throw new ConfigError("connection string brackets must hold an IPv6 address")
    }
    const rest = address.slice(closing + 1)
    if (rest.length > 0 && !rest.startsWith(":")) throw malformed()
    portText = rest.length === 0 ? undefined : rest.slice(1)
  } else {
    if (/[[\]]/.test(address)) throw malformed()
    const separator = address.indexOf(":")
    host = separator < 0 ? address : address.slice(0, separator)
    portText = separator < 0 ? undefined : address.slice(separator + 1)
  }
  if (host.length === 0) throw new ConfigError("connection string missing host")
  if (portText === undefined) return { host, port: DEFAULT_TCP_PORT }
  const port = Number(portText)
  if (!/^\d+$/.test(portText) || port < 1 || port > 65_535) {
    throw new ConfigError("connection string port must be a number from 1 to 65535")
  }
  return { host, port }
}

// Every option is `key=value`, names one of Apache Iggy's TCP options, and
// appears once. Boolean options take `true` or `false` only.
function parseOptions(query: string): ReadonlyMap<string, string> {
  const options = new Map<string, string>()
  for (const pair of query.split("&")) {
    const parts = pair.split("=")
    const [key, value] = parts
    if (parts.length !== 2 || key === undefined || value === undefined) {
      throw new ConfigError("connection string option must be key=value")
    }
    if (!CONNECTION_OPTIONS.has(key)) {
      throw new ConfigError(
        "connection string has an unknown option: supported are tls, tls_domain, tls_ca_file, reconnection_retries, reconnection_interval, reestablish_after, heartbeat_interval, nodelay"
      )
    }
    if (options.has(key)) throw new ConfigError("connection string repeats an option")
    if ((key === "tls" || key === "nodelay") && value !== "true" && value !== "false") {
      throw new ConfigError("connection string tls and nodelay take true or false")
    }
    options.set(key, value)
  }
  if (options.get("tls") === "false" && options.has("tls_ca_file")) {
    throw new ConfigError("connection string sets tls=false together with tls_ca_file")
  }
  return options
}

/**
 * Parses a connection string with the Rust grammar: an optional `iggy://` or
 * `iggy+tcp://` scheme, required percent-decoded credentials, `host[:port]`,
 * and only Apache Iggy's TCP options. The string carries a password, so errors
 * never echo it.
 */
export function parseConnectionString(
  connectionString: string,
  env: Readonly<Record<string, string | undefined>> = process.env
): ParsedConnectionString {
  const rest = stripScheme(connectionString.trim())
  const queryStart = rest.indexOf("?")
  const beforeQuery = queryStart < 0 ? rest : rest.slice(0, queryStart)
  const at = beforeQuery.lastIndexOf("@")
  if (at < 0) {
    throw new ConfigError(
      "connection string missing credentials: use user:password@host or token@host"
    )
  }
  const credentials = parseCredentials(rest.slice(0, at))
  const { host, port } = parseAddress(beforeQuery.slice(at + 1))
  const options =
    queryStart < 0 ? new Map<string, string>() : parseOptions(rest.slice(queryStart + 1))

  // Read by value, not by presence: `LASER_NO_TLS=0` and `=false` must not
  // silently downgrade a managed host to plaintext.
  const noTls = ["1", "true", "yes", "on"].includes(
    (env["LASER_NO_TLS"] ?? "").trim().toLowerCase()
  )
  const envCert = env["LASER_TLS_CERT"]
  const explicitTls = options.get("tls")
  const stringCa = options.get("tls_ca_file")
  let tls = explicitTls === "true"
  let caPath: string | undefined
  let ca: string | undefined
  // A CA file in the string states TLS intent, so it turns TLS on even under
  // `LASER_NO_TLS`. An explicit `tls=false` or `LASER_NO_TLS` stops automatic
  // TLS, as in Rust `resolve_tls`.
  if (stringCa !== undefined) {
    tls = true
    caPath = stringCa
  } else if (explicitTls !== "false" && !noTls) {
    if (envCert !== undefined && envCert.length > 0) {
      tls = true
      caPath = envCert
    } else if (laserDataHost(host)) {
      tls = true
      ca = LASERDATA_ROOT_CA
    }
  }
  if (caPath !== undefined) {
    try {
      ca = readFileSync(caPath, "utf8")
    } catch (cause) {
      throw new ConfigError(`failed to read TLS CA file: ${caPath}`, { cause })
    }
  }

  const heartbeat = options.get("heartbeat_interval")
  const nodelay = options.get("nodelay")
  const servername = options.get("tls_domain")
  const interval = options.get("reconnection_interval")
  return {
    host,
    port,
    ...(servername !== undefined ? { servername } : {}),
    credentials,
    tls,
    ...(ca !== undefined ? { ca } : {}),
    reconnection: {
      intervalMs:
        interval === undefined
          ? DEFAULT_RECONNECT_INTERVAL_MS
          : parseDuration("reconnection_interval", interval),
      maxRetries: parseReconnectRetries(options.get("reconnection_retries"))
    },
    ...(heartbeat !== undefined
      ? { heartbeatIntervalMs: parseDuration("heartbeat_interval", heartbeat) }
      : {}),
    ...(nodelay !== undefined ? { noDelay: nodelay === "true" } : {}),
    ...(options.has("reestablish_after")
      ? {
          reestablishAfterMs: parseDuration(
            "reestablish_after",
            options.get("reestablish_after") ?? ""
          )
        }
      : {})
  }
}

interface ConnectedClient {
  readonly client: SimpleClient
  readonly raw: RawClient
}

async function connectSimpleClient(
  parsed: ParsedConnectionString,
  deadline?: number,
  signal?: AbortSignal
): Promise<ConnectedClient> {
  if (signal?.aborted === true) throw new TransportError("transport is closed", false)
  // The client redials a dropped replica link and resumes the session on it,
  // with the policy of the connection string, as Rust hands it to its client.
  const reconnect = {
    enabled: true,
    interval: Math.max(1, parsed.reconnection.intervalMs),
    maxRetries: Math.min(
      parsed.reconnection.maxRetries ?? UNLIMITED_RECONNECT_RETRIES,
      UNLIMITED_RECONNECT_RETRIES
    )
  }
  const config: ClientConfig = parsed.tls
    ? {
        heartbeatInterval: parsed.heartbeatIntervalMs ?? VSR_HEARTBEAT_INTERVAL_MS,
        transport: "TLS",
        options: {
          port: parsed.port,
          host: parsed.host,
          ...(parsed.noDelay !== undefined ? { noDelay: parsed.noDelay } : {}),
          ...(parsed.servername !== undefined
            ? { servername: parsed.servername }
            : isIP(parsed.host) === 0
              ? { servername: parsed.host }
              : {}),
          ...(parsed.ca !== undefined ? { ca: parsed.ca } : {})
        },
        credentials: parsed.credentials,
        reconnect
      }
    : {
        heartbeatInterval: parsed.heartbeatIntervalMs ?? VSR_HEARTBEAT_INTERVAL_MS,
        transport: "TCP",
        options: {
          port: parsed.port,
          host: parsed.host,
          ...(parsed.noDelay !== undefined ? { noDelay: parsed.noDelay } : {})
        },
        credentials: parsed.credentials,
        reconnect
      }

  let raw: RawClient | undefined
  try {
    raw = getRawClient(config)
  } catch (cause) {
    throw new ConfigError("invalid Apache Iggy client configuration", { cause })
  }
  try {
    const client = new SimpleClient(raw)
    const ready = client.client.getMe()
    void ready.catch(() => undefined)
    let timer: ReturnType<typeof setTimeout> | undefined
    let closing: (() => void) | undefined
    try {
      await Promise.race([
        ready,
        ...(deadline === undefined
          ? []
          : [
              new Promise<never>((_, reject) => {
                timer = setTimeout(
                  () => {
                    reject(new TimeoutError(transportUp(raw) ? LOGIN_STAGE : ACCEPT_STAGE))
                  },
                  Math.max(0, deadline - Date.now())
                )
              })
            ]),
        ...(signal === undefined
          ? []
          : [
              new Promise<never>((_, reject) => {
                closing = () => {
                  reject(new TransportError("transport is closed", false))
                }
                signal.addEventListener("abort", closing, { once: true })
              })
            ])
      ])
    } finally {
      clearTimeout(timer)
      if (closing !== undefined) signal?.removeEventListener("abort", closing)
    }
    return { client, raw }
  } catch (cause) {
    raw.destroy()
    if (
      cause instanceof TimeoutError ||
      cause instanceof UnsupportedError ||
      (cause instanceof TransportError && !cause.retryable)
    )
      throw cause
    throw new TransportError(`failed to connect to ${parsed.host}:${String(parsed.port)}`, true, {
      cause
    })
  }
}

// Whether the socket the client dialed last is up, its TLS handshake included,
// which separates a server that never accepts the connection from one that
// never answers the login.
function transportUp(raw: RawClient | undefined): boolean {
  let socket: unknown
  try {
    socket = raw?.getReadStream()
  } catch {
    return false
  }
  if (!(socket instanceof Socket) || socket.readyState !== "open") return false
  // The peer's certificate is known once the handshake finished.
  return !(socket instanceof TLSSocket) || Object.keys(socket.getPeerCertificate()).length > 0
}

// A stream or topic a lookup found missing, carrying Apache Iggy's code for it
// so it classifies like the server's own refusal.
function missingResource(message: string, errorCode: number): TransportError {
  return new TransportError(message, false, {
    cause: Object.assign(new Error(message), { errorCode })
  })
}

// A failure a later attempt can clear: no server reply, or a server reply the
// cluster marks transient.
function retryableFailure(error: unknown): boolean {
  if (serverResponseError(error) === undefined) return true
  const code = serverErrorCode(error)
  return code !== undefined && TRANSIENT_SERVER_CODES.has(code)
}

function serverResponseError(error: unknown): Error | undefined {
  let current = error
  for (let depth = 0; depth < 8; depth += 1) {
    if (typeof current !== "object" || current === null) return undefined
    if (
      "errorCode" in current &&
      typeof (current as { readonly errorCode?: unknown }).errorCode === "number"
    ) {
      return current instanceof Error ? current : new Error("server rejected connection")
    }
    current = "cause" in current ? (current as { readonly cause?: unknown }).cause : undefined
  }
  return undefined
}

async function connectWithRetry(
  parsed: ParsedConnectionString,
  deadline: number,
  signal?: AbortSignal
): Promise<ConnectedClient> {
  let lastError: unknown
  for (;;) {
    try {
      return await connectSimpleClient(parsed, deadline, signal)
    } catch (error) {
      if (error instanceof ConfigError || error instanceof TimeoutError || signal?.aborted === true)
        throw error
      lastError = error
      // The client spends a bounded retry budget on its own dials. An unlimited
      // one outlasts its 30-second call, so only the connect budget ends it.
      if (
        serverResponseError(error) !== undefined ||
        parsed.reconnection.maxRetries !== undefined
      ) {
        break
      }
      const remaining = deadline - Date.now()
      if (remaining <= 0) throw new TimeoutError(ACCEPT_STAGE, { cause: error })
      await sleep(
        Math.min(parsed.reconnection.intervalMs, remaining),
        undefined,
        signal === undefined ? {} : { signal }
      ).catch(() => undefined)
    }
  }
  const responseError = serverResponseError(lastError)
  const message =
    responseError === undefined
      ? `failed to connect to ${parsed.host}:${String(parsed.port)}`
      : `server rejected connection: ${responseError.message}`
  throw new TransportError(message, responseError === undefined, { cause: lastError })
}

/** How long a managed mutation repeats its frame while its outcome is unknown,
 * as Rust `MANAGED_MUTATION_DEADLINE`. */
export { MANAGED_MUTATION_DEADLINE_MS }

/** Where `code` goes when nothing more specific names its node, as Rust
 * `Target::of_code`. A standard Iggy command keeps the client's own routing,
 * the way Rust sends it as a typed call. */
export function managedTargetOf(code: number): ManagedTarget {
  if (
    code < AGDX_COMMAND_BASE ||
    code === AGDX_AUTHZ_DEFINE_ROLE_CODE ||
    code === AGDX_AUTHZ_DELETE_ROLE_CODE ||
    code === AGDX_AUTHZ_BIND_ROLES_CODE
  )
    return { kind: "metadata_primary" }
  return { kind: "caller" }
}

/**
 * The caller's node: the roster entry the caller key picks, moved one entry on
 * per failed link, as Rust `nodes::pick`.
 * @internal
 */
export function pickNode(
  roster: readonly number[],
  callerKey: bigint,
  moved: bigint
): number | undefined {
  if (roster.length === 0) return undefined
  return roster[Number(((callerKey + moved) & U64_MASK) % BigInt(roster.length))]
}

/** Whether the node a partition request went to no longer leads the partition
 * or cannot be reached, so its route is looked up again, as Rust
 * `is_route_stale`. */
export function isRouteStale(error: unknown): boolean {
  const code = serverErrorCode(error)
  return code === NOT_PRIMARY || code === INVALID_IDENTIFIER || isTransientFailure(error)
}

// A failure Rust `is_transient_iggy_io_error` names: a lost or refused link
// before a reply, or a reply the cluster marks transient.
function isTransientFailure(error: unknown): boolean {
  return (
    error instanceof AmbiguousMutationError || (error instanceof TransportError && error.retryable)
  )
}

type Runner = <Value>(
  operation: (client: SimpleClient) => Promise<Value>,
  message: string
) => Promise<Value>

/**
 * Picks the node of every managed request, as Rust `nodes::Nodes`. Every node
 * answers every managed request from shared state, so a client keeps to one
 * node chosen by a key of its own, which spreads clients over the cluster, and
 * moves to the next node after a failed link.
 */
export class Nodes {
  // Ascending replica ids, empty until the first request reads them.
  private roster: readonly number[] = []
  // Failed links so far, each one moves the caller to the next node.
  private moved = 0n
  // The node each recent query ran on, newest last.

  constructor(private readonly callerKey: bigint = callerKeyOf()) {}

  /**
   * Sends `code` to `target`. A request to the caller's node moves on to the
   * next node after a failed link, at most once per node, and reads the roster
   * again once when a replica id is no longer in it. A request to a named
   * replica is sent there only.
   */
  async send(
    run: Runner,
    code: number,
    payload: Uint8Array,
    target: ManagedTarget,
    deadline?: number,
    written: FrameWritten = { written: false }
  ): Promise<ManagedAnswer> {
    const buffer = toNodeBuffer(payload)
    switch (target.kind) {
      case "metadata_primary":
        return openAnswer(
          await run(
            (client) =>
              deadline === undefined
                ? client.sendBinaryRequest(code, buffer)
                : sendWithin(client, code, buffer, { deadline }),
            `managed command ${String(code)} failed`
          )
        )
      case "replica":
        return openAnswer(
          await run(
            (client) =>
              deadline === undefined
                ? client.sendBinaryRequestToReplica(code, buffer, target.replica)
                : sendWithin(client, code, buffer, { deadline, replica: target.replica }),
            `managed command ${String(code)} to replica ${String(target.replica)} failed`
          ),
          target.replica
        )
      case "caller":
        return this.sendToCaller(run, code, buffer, deadline, written)
    }
  }

  /** The caller's node and the roster size, reading the roster when none is known. */
  async callerNode(run: Runner): Promise<{ readonly replica: number; readonly nodes: number }> {
    const picked = this.pick()
    if (picked !== undefined) return picked
    const metadata = await run(
      (client) => client.cluster.getClusterMetadata(),
      "failed to read the cluster metadata"
    )
    this.roster = [...new Set(metadata.nodes.map((node) => node.replicaId))].sort(
      (left, right) => left - right
    )
    const read = this.pick()
    if (read === undefined) {
      throw new TransportError("the cluster metadata names no replica", false, {
        cause: Object.assign(new Error("invalid identifier"), { errorCode: INVALID_IDENTIFIER })
      })
    }
    return read
  }

  /** Records a failed link to `replica`. The caller moves on only when
   * `replica` is still its node, so concurrent failures move it once. */
  failed(replica: number): void {
    if (pickNode(this.roster, this.callerKey, this.moved) === replica) this.moved += 1n
  }

  forgetRoster(): void {
    this.roster = []
  }

  private async sendToCaller(
    run: Runner,
    code: number,
    buffer: Buffer,
    deadline: number | undefined,
    written: FrameWritten
  ): Promise<ManagedAnswer> {
    let rosterRead = false
    let failedLinks = 0
    for (;;) {
      const { replica, nodes } = await this.callerNode(run)
      try {
        const reply = await run(
          (client) =>
            deadline === undefined
              ? client.sendBinaryRequestToReplica(code, buffer, replica)
              : sendWithin(client, code, buffer, { deadline, replica }),
          `managed command ${String(code)} to replica ${String(replica)} failed`
        )
        written.written = true
        const answer = openAnswer(reply, replica)
        if (backendUnavailable(answer.reply)) {
          this.failed(replica)
          failedLinks += 1
          if (walksOn(failedLinks, nodes, deadline)) continue
        }
        return answer
      } catch (error) {
        if (frameWritten(error)) written.written = true
        if (serverErrorCode(error) === INVALID_IDENTIFIER && !rosterRead) {
          rosterRead = true
          this.forgetRoster()
          continue
        }
        if (!isTransientFailure(error)) throw error
        this.failed(replica)
        failedLinks += 1
        if (!walksOn(failedLinks, nodes, deadline)) throw error
      }
    }
  }

  private pick(): { readonly replica: number; readonly nodes: number } | undefined {
    const replica = pickNode(this.roster, this.callerKey, this.moved)
    return replica === undefined ? undefined : { replica, nodes: this.roster.length }
  }
}

// Whether a caller request whose link failed on `failedLinks` nodes moves to
// the next one. An expired attempt deadline ends the walk, so the next attempt
// starts on the next node with a fresh deadline, as Rust `walks_on`.
function walksOn(failedLinks: number, nodes: number, deadline: number | undefined): boolean {
  return failedLinks < nodes && (deadline === undefined || Date.now() < deadline)
}

// A raw command bounded by the caller's deadline instead of the client's
// default call bound, for a request the server may hold.
async function sendWithin(
  client: SimpleClient,
  code: number,
  payload: Buffer,
  options: SendCommandOptions
): Promise<Buffer> {
  const response = await (
    await client.clientProvider()
  ).sendCommand(code, Buffer.from(payload), options)
  return response.length <= 1 ? Buffer.alloc(0) : response.data
}

/**
 * Whether an opened reply refuses as `BackendUnavailable`, as Rust
 * `reply_error`. A recovering node's positioned refusal is opened first.
 * @internal
 */
export function backendUnavailable(reply: Uint8Array): boolean {
  try {
    const value: unknown = decodeOne(reply, "CommandError")
    return (
      value instanceof Map &&
      value.get("code") === "backend_unavailable" &&
      typeof value.get("message") === "string"
    )
  } catch {
    return false
  }
}

// A node's reply with its positioned envelope opened once, the frontier kept
// beside the reply.
function openAnswer(reply: Buffer, replica?: number): ManagedAnswer {
  const opened = openPositioned(fromNodeBuffer(reply))
  return {
    reply: opened.reply,
    ...(opened.frontier !== undefined ? { frontier: opened.frontier } : {}),
    ...(replica !== undefined ? { replica } : {})
  }
}

// The key that picks a client's node: random, so clients spread over the
// cluster, and fixed for the connection's life.
function callerKeyOf(): bigint {
  return mintUlidValue() & U64_MASK
}

function fromNodeBuffer(reply: Buffer): Uint8Array {
  return new Uint8Array(reply.buffer, reply.byteOffset, reply.byteLength)
}

function encodePartitionRoute(streamId: string, topicId: string, partitionId: number): Uint8Array {
  const stream = encodeIdentifier(streamId)
  const topic = encodeIdentifier(topicId)
  const request = new Uint8Array(stream.byteLength + topic.byteLength + 4)
  request.set(stream, 0)
  request.set(topic, stream.byteLength)
  new DataView(request.buffer).setUint32(stream.byteLength + topic.byteLength, partitionId, true)
  return request
}

// The primary replica of a route reply. A reply of any other length comes from
// a mismatched build.
function decodePartitionPrimary(reply: Uint8Array): number {
  if (reply.byteLength !== PARTITION_ROUTE_BYTES)
    throw new ProtocolError("the partition route reply has an unexpected length")
  return reply[PARTITION_ROUTE_BYTES - 1] ?? 0
}

/**
 * The surface reply inside a positioned reply, and the frontier it carries, as
 * Rust `open_positioned`. A reply the streaming server itself refused with
 * comes unwrapped.
 * @internal
 */
export function openPositioned(reply: Uint8Array): {
  readonly reply: Uint8Array
  readonly frontier?: AppliedFrontier
} {
  try {
    const envelope = decodeManagedReplyEnvelope(
      expectMap(decodeOne(reply, "ManagedReplyEnvelope"), "ManagedReplyEnvelope"),
      "ManagedReplyEnvelope"
    )
    return { reply: envelope.reply, frontier: envelope.frontier }
  } catch {
    return { reply }
  }
}

/** The message of a surface-neutral `Pending` reply, `undefined` for any other
 * reply. A typed surface reply never decodes as this shape. */
export function pendingReplyMessage(reply: Uint8Array): string | undefined {
  let value: unknown
  try {
    value = decodeOne(reply, "CommandError")
  } catch {
    return undefined
  }
  // Pending: accepted, outcome unknown. BackendUnavailable: the node's own
  // managed backend did not answer, and the request may still apply.
  // Unavailable: the node was too busy to forward it. Each repeats the same
  // identity.
  if (!(value instanceof Map)) return undefined
  const code: unknown = value.get("code")
  if (code !== "pending" && code !== "backend_unavailable" && code !== "unavailable")
    return undefined
  const message: unknown = value.get("message")
  return typeof message === "string" ? message : code
}

/** What one attempt of a managed mutation came back with. @internal */
export type MutationAttempt =
  | { readonly kind: "answered"; readonly reply: Uint8Array }
  | { readonly kind: "lost"; readonly message: string }
  | { readonly kind: "refused"; readonly cause: unknown }

/**
 * What the attempts of one managed mutation know about its outcome, as Rust
 * `MutationAttempts`. An attempt whose outcome is unknown makes every later
 * refusal ambiguous too, because the earlier attempt may still apply.
 * @internal
 */
export class MutationAttempts {
  uncertain: string | undefined
  // The unresolved items of a batch frame, by slot, empty for one mutation.
  private readonly slots: (string | undefined)[] = []

  /** `batch` when the request is the mixed-operation batch, which settles
   * item by item. */
  constructor(private readonly batch = false) {}

  /** The reply that settles the mutation, `undefined` to send it again. Throws
   * the error the caller gets. */
  settle(attempt: MutationAttempt): Uint8Array | undefined {
    switch (attempt.kind) {
      case "answered": {
        if (this.batch) {
          const batch = batchReply(attempt.reply)
          if (batch !== undefined) return this.settleBatch(attempt.reply, batch)
        }
        const pending = pendingReplyMessage(attempt.reply)
        if (pending !== undefined) {
          this.uncertain = pending
          return undefined
        }
        const refusal = admissionRefusal(attempt.reply)
        if (this.uncertain !== undefined && refusal !== undefined) {
          throw this.ambiguous(this.uncertain, refusal)
        }
        return attempt.reply
      }
      case "lost":
        this.uncertain = attempt.message
        return undefined
      case "refused":
        if (this.uncertain !== undefined) {
          const message =
            attempt.cause instanceof Error ? attempt.cause.message : String(attempt.cause)
          throw this.ambiguous(this.uncertain, message)
        }
        throw attempt.cause
    }
  }

  // Settle a batch frame item by item, as Rust `settle_batch`. An item whose
  // outcome is unknown leaves the whole frame unresolved, so the same frame is
  // sent again and every item answers under its own derived identity: a
  // mutation with its recorded outcome, a read with a fresh result. An item
  // that was unknown and is then refused by admission may still apply.
  private settleBatch(reply: Uint8Array, batch: BatchReply): Uint8Array | undefined {
    const frameUncertain = this.uncertain
    this.uncertain = undefined
    this.slots.length = batch.results.length
    const unresolved: number[] = []
    for (const [index, slot] of batch.results.entries()) {
      const earlier = this.slots[index] ?? frameUncertain
      this.slots[index] = undefined
      const pending = pendingReplyMessage(slot)
      if (pending !== undefined) {
        this.slots[index] = pending
        unresolved.push(index)
        continue
      }
      const refusal = admissionRefusal(slot)
      if (earlier !== undefined && refusal !== undefined) {
        throw this.ambiguous(`item ${String(index)}: ${earlier}`, refusal)
      }
    }
    const first = unresolved[0]
    if (first === undefined) return reply
    this.uncertain = `items [${unresolved.join(", ")}] are unresolved: ${this.slots[first] ?? ""}`
    return undefined
  }

  private ambiguous(earlier: string, refusal: string): AmbiguousMutationError {
    return new AmbiguousMutationError(
      `an earlier attempt's outcome is unknown (${earlier}), then the request was refused (${refusal})`
    )
  }
}

// The reply decoded as a batch frame, `undefined` for any other reply.
function batchReply(reply: Uint8Array): BatchReply | undefined {
  try {
    return decodeBatchReply(expectMap(decodeOne(reply, "BatchReply"), "BatchReply"), "BatchReply")
  } catch {
    return undefined
  }
}

// A refusal that names no outcome of the mutation, only why this attempt was
// not admitted, as Rust `admission_refusal`.
function admissionRefusal(reply: Uint8Array): string | undefined {
  let value: unknown
  try {
    value = decodeOne(reply, "CommandError")
  } catch {
    return undefined
  }
  if (!(value instanceof Map)) return undefined
  const code: unknown = value.get("code")
  if (code !== "forbidden" && code !== "unauthenticated" && code !== "step_up_required")
    return undefined
  const message: unknown = value.get("message")
  return `${code}: ${typeof message === "string" ? message : ""}`
}

export class ApacheIggyTransport implements LaserTransport {
  readonly kind = "apache-iggy" as const
  readonly publishRetriesManaged = true
  private readonly reconnectLock = new Mutex()
  private readonly publishLane = new Mutex()
  private readonly disconnected = new WeakSet<SimpleClient>()
  // The highest mutation position seen per surface, from mutation and read
  // replies. Every KV, fork and graph read sends it back, so whichever node
  // answers has applied at least what this client wrote or read before.
  private seenFrontier: AppliedFrontier = {}
  // Groups this transport rejoins after a reconnect. Setup helpers allow
  // recreation, while joining an existing group keeps `create: false`.
  private readonly consumerGroups = new Map<
    string,
    {
      readonly streamId: string
      readonly topicId: string
      readonly name: string | number
      readonly create: boolean
    }
  >()
  private readonly partitionCounts = new Map<string, PartitionCount>()
  private readonly balancedCursors = new Map<string, number>()
  // Sends of the current client whose outcome is unknown. Each one blocks its
  // partition lane until a later send resolves it.
  private readonly uncertainSends = new Set<bigint>()
  // Ends a redial that `close` overtakes, so a closed transport keeps no client.
  private readonly lifetime = new AbortController()
  private closed = false

  private constructor(
    private client: SimpleClient,
    private readonly connection: ParsedConnectionString | undefined,
    private readonly ownership: ClientOwnership,
    private readonly publishConfig: PublishOptions,
    readonly nodes: Nodes = new Nodes()
  ) {}

  async replicaIds(): Promise<readonly number[]> {
    const metadata = await this.runner()(
      (client) => client.cluster.getClusterMetadata(),
      "failed to read the cluster metadata"
    )
    return [...new Set(metadata.nodes.map((node) => node.replicaId))].sort(
      (left, right) => left - right
    )
  }

  publishTimeoutMs(): number {
    return this.publishConfig.timeoutMs
  }

  publishOptions(): PublishOptions {
    return this.publishConfig
  }

  get iggyClient(): SimpleClient {
    return this.client
  }

  get connectsNodes(): boolean {
    return this.connection !== undefined
  }

  static async connect(
    connectionString: string,
    options?: Partial<PublishOptions>,
    deadline = Date.now() + connectOptions().timeoutMs,
    signal?: AbortSignal
  ): Promise<ApacheIggyTransport> {
    const config = publishOptions(options)
    const parsed = parseConnectionString(connectionString)
    const connected = await connectWithRetry(parsed, deadline, signal)
    return new ApacheIggyTransport(connected.client, parsed, "owned", config)
  }

  static async fromClient(
    client: SimpleClient,
    ownership: ClientOwnership = "borrowed",
    options?: Partial<PublishOptions>,
    deadline = Date.now() + connectOptions().timeoutMs
  ): Promise<ApacheIggyTransport> {
    const config = publishOptions(options)
    try {
      await publishWithin(client.clientProvider(), Math.max(0, deadline - Date.now()))
    } catch (cause) {
      if (ownership === "owned") void client.destroy().catch(() => undefined)
      if (cause instanceof TimeoutError) throw new TimeoutError("Iggy client readiness", { cause })
      throw cause
    }
    return new ApacheIggyTransport(client, undefined, ownership, config)
  }

  private async execute<Value>(
    operation: (client: SimpleClient) => Promise<Value>,
    message: string,
    retryAfterReconnect = true,
    signal?: AbortSignal,
    deadline?: number
  ): Promise<Value> {
    if (this.closed) throw new TransportError("transport is closed", false)
    if (deadline !== undefined && Date.now() >= deadline) {
      throw new TransportError(`${message}: attempt deadline elapsed`, true)
    }
    const stale = this.client
    if (this.disconnected.has(stale)) {
      await this.reconnect(stale, deadline)
      if (deadline !== undefined && Date.now() >= deadline) {
        throw new TransportError(`${message}: attempt deadline elapsed`, true)
      }
      try {
        return await operation(this.client)
      } catch (cause) {
        throw new TransportError(message, retryableFailure(cause), { cause })
      }
    }
    try {
      return await operation(stale)
    } catch (firstCause) {
      // A call its caller cancelled, such as the losing reads of a raced held
      // read, says nothing about the link.
      if (signal?.aborted === true) {
        throw new CancelledError(`${message}: cancelled`, { cause: firstCause })
      }
      if (deadline !== undefined && Date.now() >= deadline) {
        throw new TransportError(`${message}: attempt deadline elapsed`, true, {
          cause: firstCause
        })
      }
      // The client could not prove the outcome, or no node took the command.
      // Neither is cleared by another connection, and a resend could apply it
      // twice, so the caller decides.
      if (unprovenOutcome(firstCause) !== undefined) {
        if (!retryAfterReconnect) {
          throw new AmbiguousMutationError(`${message}: outcome is unknown`, { cause: firstCause })
        }
        throw new TransportError(message, true, { cause: firstCause })
      }
      if (cause(firstCause, NotConnectedError) !== undefined) {
        throw new TransportError(message, true, { cause: firstCause })
      }
      if (!this.disconnected.has(stale) && serverResponseError(firstCause) === undefined) {
        this.disconnected.add(stale)
      }
      if (!this.disconnected.has(stale)) {
        if (!retryAfterReconnect && serverResponseError(firstCause) === undefined) {
          throw new AmbiguousMutationError(`${message}: outcome is unknown`, { cause: firstCause })
        }
        throw new TransportError(message, retryableFailure(firstCause), { cause: firstCause })
      }
      if (!retryAfterReconnect) {
        throw new AmbiguousMutationError(`${message}: outcome is unknown`, { cause: firstCause })
      }
      try {
        await this.reconnect(stale, deadline)
        if (deadline !== undefined && Date.now() >= deadline) {
          throw new TransportError(`${message}: attempt deadline elapsed`, true)
        }
        return await operation(this.client)
      } catch (cause) {
        const actual = cause ?? firstCause
        throw new TransportError(message, retryableFailure(actual), { cause: actual })
      }
    }
  }

  private reconnect(stale: SimpleClient, deadline?: number): Promise<void> {
    return this.reconnectLock.runExclusive(async () => {
      if (deadline !== undefined && Date.now() >= deadline)
        throw new TimeoutError("Iggy publish reconnect")
      if (this.closed) throw new TransportError("transport is closed", false)
      if (this.client !== stale) return
      if (this.connection === undefined) {
        throw new TransportError("an injected client cannot be reconnected by Laser", false)
      }
      await stale.destroy().catch(() => undefined)
      let lastError: unknown
      for (;;) {
        try {
          const remaining = deadline === undefined ? undefined : deadline - Date.now()
          if (remaining !== undefined && remaining <= 0)
            throw new TimeoutError("Iggy publish reconnect")
          const connected = await connectSimpleClient(
            this.connection,
            deadline,
            this.lifetime.signal
          )
          try {
            for (const [key, group] of [...this.consumerGroups]) {
              const join: Promise<unknown> =
                group.create && typeof group.name === "string"
                  ? connected.client.group.ensureAndJoin(group.streamId, group.topicId, group.name)
                  : connected.client.group
                      .join({
                        streamId: group.streamId,
                        topicId: group.topicId,
                        groupId: group.name
                      })
                      .then(
                        () => undefined,
                        // A deleted group is not recreated: its member finds out
                        // on its next assignment read.
                        () => {
                          this.consumerGroups.delete(key)
                        }
                      )
              await (deadline === undefined
                ? join
                : publishWithin(join, Math.max(1, deadline - Date.now())))
            }
            if (this.lifetime.signal.aborted) throw new TransportError("transport is closed", false)
          } catch (cause) {
            await connected.client.destroy().catch(() => undefined)
            throw cause
          }
          this.client = connected.client
          this.uncertainSends.clear()
          return
        } catch (error) {
          lastError = error
          if (this.lifetime.signal.aborted)
            throw new TransportError("transport is closed", false, { cause: error })
          if (deadline !== undefined && Date.now() >= deadline)
            throw new TimeoutError("Iggy publish reconnect", { cause: error })
          // A bounded retry budget went to the client's own dials.
          if (
            serverResponseError(error) !== undefined ||
            this.connection.reconnection.maxRetries !== undefined
          ) {
            break
          }
          await sleep(
            Math.min(
              this.connection.reconnection.intervalMs,
              deadline === undefined ? Infinity : Math.max(0, deadline - Date.now())
            ),
            undefined,
            { signal: this.lifetime.signal }
          ).catch(() => undefined)
        }
      }
      const responseError = serverResponseError(lastError)
      const message =
        responseError === undefined
          ? "reconnect attempts exhausted"
          : `server rejected connection: ${responseError.message}`
      throw new TransportError(message, responseError === undefined, { cause: lastError })
    })
  }

  async sendManaged(
    code: number,
    payload: Uint8Array,
    options?: ManagedSendOptions
  ): Promise<Uint8Array> {
    return (
      await this.sendManagedTo(code, payload, options?.target ?? managedTargetOf(code), options)
    ).reply
  }

  /**
   * Sends managed command `code` to `target` and names the replica that
   * answered, as Rust `send_managed_to`. A read goes in its read frame, a
   * mutation repeats its frame until its outcome is known.
   */
  async sendManagedTo(
    code: number,
    payload: Uint8Array,
    target: ManagedTarget,
    options?: ManagedSendOptions
  ): Promise<ManagedAnswer> {
    if (!isIdempotentManagedRequest(code)) {
      const answer = await this.nodes.send(
        this.runner(options),
        code,
        this.readFrame(code, payload),
        target,
        options?.deadline
      )
      return { ...answer, reply: this.takePositioned(answer) }
    }
    const operationId = options?.operationId ?? mintUlidValue()
    if (operationId <= 0n || operationId >= 1n << 128n)
      throw new InvalidError("operation id must be a nonzero u128")
    const frame = encodeNamed(
      encodeManagedRequestEnvelope({
        v: MANAGED_REQUEST_VERSION,
        operationId,
        payload
      })
    )
    return this.sendManagedMutation(code, operationId, frame, target, options)
  }

  /**
   * Sends one managed mutation until its outcome is known, as Rust
   * `send_managed_mutation`. The frame and its operation identity never change
   * between attempts, so the server applies the mutation at most once. A
   * `Pending` reply or a retryable transport failure repeats the same frame with
   * jittered backoff. Past the deadline the caller gets
   * `AmbiguousMutationError` naming the identity, because the mutation may
   * still apply.
   */
  private async sendManagedMutation(
    code: number,
    operationId: bigint,
    frame: Uint8Array,
    target: ManagedTarget,
    options?: ManagedSendOptions
  ): Promise<ManagedAnswer> {
    const deadlineMs = options?.mutationDeadlineMs ?? MANAGED_MUTATION_DEADLINE_MS
    const deadline = Date.now() + deadlineMs
    const identity = operationId.toString(16).padStart(32, "0")
    let delay = MANAGED_MUTATION.baseMs
    let bound = MANAGED_ATTEMPT_FIRST_MS
    const attempts = new MutationAttempts(code === AGDX_BATCH_CODE)
    for (;;) {
      let attempt: MutationAttempt
      let answer: ManagedAnswer | undefined
      const attemptDeadline = Math.min(Date.now() + bound, deadline)
      try {
        answer = await this.nodes.send(
          this.runner({ ...options, deadline: attemptDeadline }),
          code,
          frame,
          target,
          attemptDeadline
        )
        attempt = { kind: "answered", reply: this.takePositioned(answer) }
      } catch (cause) {
        if (cause instanceof AmbiguousMutationError) {
          throw new AmbiguousMutationError(`operation ${identity}: ${cause.message}`, {
            cause,
            operationId
          })
        }
        attempt = isTransientFailure(cause)
          ? { kind: "lost", message: cause instanceof Error ? cause.message : String(cause) }
          : { kind: "refused", cause }
      }
      let settled: Uint8Array | undefined
      try {
        settled = attempts.settle(attempt)
      } catch (cause) {
        if (cause instanceof AmbiguousMutationError) {
          throw new AmbiguousMutationError(`operation ${identity}: ${cause.message}`, {
            cause,
            operationId
          })
        }
        throw cause
      }
      if (settled !== undefined && answer !== undefined) return { ...answer, reply: settled }
      const unknown = attempts.uncertain ?? ""
      const wait = jittered(delay)
      if (Date.now() + wait >= deadline) {
        throw new AmbiguousMutationError(
          `operation ${identity} outcome is still unknown after ${String(deadlineMs / 1000)}s: ${unknown}`,
          { operationId }
        )
      }
      await new Promise((resolve) => setTimeout(resolve, wait))
      delay = Math.min(delay * 2, MANAGED_MUTATION.maxMs)
      bound *= 2
    }
  }

  // Runs one operation on this connection, reconnecting it as `execute` does.
  private runner(options?: ManagedSendOptions): Runner {
    return async (operation, message) => {
      const pending = this.execute(
        operation,
        message,
        options?.retryAfterReconnect ?? true,
        undefined,
        options?.deadline
      )
      if (options?.deadline === undefined) return pending
      try {
        return await publishWithin(pending, Math.max(0, options.deadline - Date.now()))
      } catch (cause) {
        if (cause instanceof TimeoutError) {
          throw new TransportError(`${message}: attempt deadline elapsed`, true, { cause })
        }
        throw cause
      }
    }
  }

  /**
   * The frame for managed read `code`, as Rust `read_frame`. A KV, fork,
   * graph or session read always goes in a read envelope carrying the highest
   * positions this client has seen for the surface, so the reply brings the
   * answering node's frontier back.
   */
  private readFrame(code: number, read: Uint8Array): Uint8Array {
    if (!isBarrierRead(code)) return read
    const surface = positionSurfaceOf(code)
    const minPosition = surface === undefined ? undefined : this.seenFrontier[surface]
    // A session read: the backend keeps only the positions of the read's own
    // stream.
    const minSessions = surface === undefined ? (this.seenFrontier.sessions ?? []) : []
    return encodeNamed(
      encodeManagedReadEnvelope({
        v: MANAGED_REQUEST_VERSION,
        read,
        ...(minPosition !== undefined ? { minPosition } : {}),
        ...(minSessions.length > 0 ? { minSessions } : {})
      })
    )
  }

  /** The opened reply, after raising the seen frontier to its own. */
  private takePositioned(answer: ManagedAnswer): Uint8Array {
    if (answer.frontier !== undefined) {
      this.seenFrontier = mergeFrontier(this.seenFrontier, answer.frontier)
    }
    return answer.reply
  }

  /** The highest mutation position this client has seen per surface. */
  seenPositions(): AppliedFrontier {
    return this.seenFrontier
  }

  /** Raises the seen positions to a frontier another connection observed, as
   * a lease granted over the dedicated coordination connection. */
  observePositions(frontier: AppliedFrontier): void {
    this.seenFrontier = mergeFrontier(this.seenFrontier, frontier)
  }

  /** Sends an already framed managed command to the caller's node, as Rust
   * `send_raw_preframed`. */
  async sendManagedPreframed(
    code: number,
    payload: Uint8Array,
    options?: ManagedSendOptions
  ): Promise<Uint8Array> {
    const answer = await this.nodes.send(
      this.runner(options),
      code,
      payload,
      { kind: "caller" },
      undefined,
      options?.written
    )
    return this.takePositioned(answer)
  }

  async ensureStream(name: string): Promise<void> {
    await this.execute(
      (client) => client.stream.ensure(name),
      `failed to ensure stream \`${name}\``
    )
  }

  async deleteStream(name: string): Promise<boolean> {
    const deleted = await this.execute(async (client) => {
      if ((await client.stream.get({ streamId: name })) === null) return false
      try {
        await client.stream.delete({ streamId: name })
      } catch (cause) {
        if ((await client.stream.get({ streamId: name })) !== null) throw cause
      }
      return true
    }, `failed to delete stream \`${name}\``)
    const prefix = `${name}\0`
    for (const cache of [this.partitionCounts, this.balancedCursors, this.consumerGroups]) {
      for (const key of cache.keys()) if (key.startsWith(prefix)) cache.delete(key)
    }
    return deleted
  }

  async ensureConsumerGroup(streamId: string, topicId: string, name: string): Promise<void> {
    await this.execute(
      (client) => client.group.ensure(streamId, topicId, name),
      `failed to ensure consumer group \`${name}\``
    )
  }

  /** Creates the topic when it is absent, keeping records until retention
   * removes them like Rust `ensure_topic`. An existing topic is left as it is. */
  async ensureTopic(
    streamId: string,
    topicId: string,
    partitions: number,
    replicationFactor?: number
  ): Promise<void> {
    await this.createTopicIfAbsent(streamId, topicId, partitions, {
      messageExpiryMicros: NEVER_EXPIRE,
      ...(replicationFactor === undefined ? {} : { replicationFactor })
    })
  }

  /** `ensureTopic` with a message expiry for a topic it creates. An existing
   * topic keeps its own expiry. */
  async ensureTopicWithExpiry(
    streamId: string,
    topicId: string,
    partitions: number,
    messageExpiryMicros: bigint,
    replicationFactor?: number
  ): Promise<void> {
    await this.createTopicIfAbsent(streamId, topicId, partitions, {
      messageExpiryMicros,
      ...(replicationFactor === undefined ? {} : { replicationFactor })
    })
  }

  async createTopicIfAbsent(
    streamId: string,
    topicId: string,
    partitions: number,
    settings: TopicCreateSettings
  ): Promise<void> {
    if (settings.replicationFactor !== undefined)
      validateReplicationFactor(settings.replicationFactor)
    const partitionCount = await this.execute(async (client) => {
      const existing = await client.topic.get({ streamId, topicId })
      if (existing !== null) return existing.partitionsCount
      try {
        const created = await client.topic.create({
          streamId,
          name: topicId,
          partitionCount: partitions,
          compressionAlgorithm: 1,
          ...(settings.messageExpiryMicros === undefined
            ? {}
            : { messageExpiry: settings.messageExpiryMicros }),
          ...(settings.maxTopicSize === undefined ? {} : { maxTopicSize: settings.maxTopicSize }),
          ...(settings.replicationFactor === undefined
            ? {}
            : { replicationFactor: settings.replicationFactor })
        })
        return created.partitionsCount
      } catch (error) {
        if (serverErrorCode(error) !== TOPIC_NAME_ALREADY_EXISTS) throw error
        const raced = await client.topic.get({ streamId, topicId })
        if (raced === null) throw error
        return raced.partitionsCount
      }
    }, `failed to create topic \`${topicId}\` on stream \`${streamId}\``)
    this.partitionCounts.set(this.topicKey(streamId, topicId), {
      count: partitionCount,
      readAt: Date.now()
    })
  }

  async findTopicPartitionCount(streamId: string, topicId: string): Promise<number | undefined> {
    const topic = await this.execute(
      (client) => client.topic.get({ streamId, topicId }),
      `failed to read topic \`${topicId}\``
    )
    if (topic === null) return undefined
    this.partitionCounts.set(this.topicKey(streamId, topicId), {
      count: topic.partitionsCount,
      readAt: Date.now()
    })
    return topic.partitionsCount
  }

  // The standard client turns creation stamps into millisecond dates. A
  // snapshot checks the exact microsecond stamp, so these two lookups read the
  // raw standard reply: id at byte 0, creation micros at byte 4, and for a
  // topic the partition count at byte 12.
  async findSnapshotStream(
    stream: string
  ): Promise<{ readonly id: number; readonly createdAtMicros: bigint } | undefined> {
    const data = await this.rawMetadata(
      GET_STREAM.code,
      GET_STREAM.serialize({ streamId: stream }),
      `failed to read stream \`${stream}\``
    )
    if (data === undefined) return undefined
    return { id: data.readUInt32LE(0), createdAtMicros: data.readBigUInt64LE(4) }
  }

  async findSnapshotTopic(
    stream: string,
    topic: string
  ): Promise<
    | { readonly id: number; readonly createdAtMicros: bigint; readonly partitions: number }
    | undefined
  > {
    const data = await this.rawMetadata(
      GET_TOPIC.code,
      GET_TOPIC.serialize({ streamId: stream, topicId: topic }),
      `failed to read topic \`${topic}\``
    )
    if (data === undefined) return undefined
    return {
      id: data.readUInt32LE(0),
      createdAtMicros: data.readBigUInt64LE(4),
      partitions: data.readUInt32LE(12)
    }
  }

  private async rawMetadata(
    code: number,
    payload: Buffer,
    message: string
  ): Promise<Buffer | undefined> {
    return this.execute(async (client) => {
      const raw = await client.clientProvider()
      try {
        const response = await raw.sendCommand(code, payload)
        if (response.length === 0) return undefined
        if (response.data.length < 16) throw new ProtocolError("metadata reply is too short")
        return response.data
      } catch (error) {
        if (NOT_FOUND_METADATA_CODES.has(serverErrorCode(error) ?? -1)) return undefined
        throw error
      }
    }, message)
  }

  async getTopicPartitionCount(streamId: string, topicId: string): Promise<number> {
    const partitions = await this.findTopicPartitionCount(streamId, topicId)
    if (partitions === undefined) {
      throw missingResource(
        `topic \`${topicId}\` on stream \`${streamId}\` does not exist`,
        TOPIC_NAME_NOT_FOUND
      )
    }
    return partitions
  }

  async resolveStreamTopicIds(
    streamId: string,
    topicId: string
  ): Promise<{ readonly streamId: number; readonly topicId: number }> {
    const [stream, topic] = await this.execute(
      (client) =>
        Promise.all([client.stream.get({ streamId }), client.topic.get({ streamId, topicId })]),
      `failed to resolve topic \`${topicId}\``
    )
    if (stream === null) {
      throw missingResource(`stream \`${streamId}\` does not exist`, STREAM_NAME_NOT_FOUND)
    }
    if (topic === null) {
      throw missingResource(
        `topic \`${topicId}\` on stream \`${streamId}\` does not exist`,
        TOPIC_NAME_NOT_FOUND
      )
    }
    return { streamId: stream.id, topicId: topic.id }
  }

  async resolveStreamTopicNames(
    streamId: number,
    topicId: number
  ): Promise<{ readonly stream: string; readonly topic: string } | undefined> {
    const stream = await this.execute(
      (client) => client.stream.get({ streamId }),
      `failed to resolve stream ${String(streamId)}`
    )
    if (stream === null) return undefined
    const topic = await this.execute(
      (client) => client.topic.get({ streamId, topicId }),
      `failed to resolve topic ${String(topicId)}`
    )
    return topic === null ? undefined : { stream: stream.name, topic: topic.name }
  }

  private async publish(
    operation: (client: SimpleClient) => Promise<SendMessagesResponse>,
    options?: Partial<PublishOptions> & { readonly fixedRetryIntervalMs?: number } & CallOptions
  ): Promise<SendMessagesResponse> {
    const config = { ...this.publishConfig, ...options }
    const fixed = options?.fixedRetryIntervalMs
    const signal = options?.signal
    for (let attempt = 0; attempt <= config.maxRetries; attempt += 1) {
      let used = this.client
      try {
        // One attempt holds the connection at a time. The Apache Iggy client
        // follows a leader move by re-issuing every queued command, and two
        // queued sends make it authenticate twice per hop, with the second
        // login settling back on the metadata leader. A lone command walks the
        // roster cleanly and stays on the partition primary that admits it.
        return await this.publishLane.runExclusive(async () => {
          const deadline = Date.now() + config.timeoutMs
          used = this.client
          if (this.closed) throw new TransportError("transport is closed", false)
          throwIfAborted(signal, "publish")
          if (this.disconnected.has(used)) {
            // Half the attempt budget goes to recovery so the send that follows
            // always gets a real window. Spending it all here would leave a
            // one-millisecond send that times out on a healthy connection and
            // retires it, and the next attempt would repeat that forever.
            const recovery = Math.max(1, Math.floor(config.timeoutMs / 2))
            await publishWithin(this.reconnect(used, Date.now() + recovery), recovery)
            used = this.client
          }
          return await publishWithin(
            this.sendPastBlocker(operation, used, deadline),
            Math.max(1, deadline - Date.now())
          )
        })
      } catch (cause) {
        if (cause instanceof SessionError || cause instanceof CancelledError) throw cause
        // A send that may have committed is never resent and keeps the
        // connection, as the Apache Iggy producer leaves it to its caller.
        const unproven = unprovenOutcome(cause)
        if (
          unproven instanceof OperationUncertainError ||
          unproven instanceof OperationNotRetainedError
        ) {
          if (unproven instanceof OperationUncertainError && unproven.operationId !== undefined)
            this.uncertainSends.add(unproven.operationId)
          throw new TransportError("Iggy publish failed", false, { cause })
        }
        const response = serverResponseError(cause)
        const code = serverErrorCode(cause)
        const transient = code !== undefined && TRANSIENT_SERVER_CODES.has(code)
        // An expired session on a connection that logged in once means the
        // socket was re-dialed underneath it. A reconnect logs in again.
        const reauthenticate = code === UNAUTHENTICATED && this.connection !== undefined
        const unusable =
          cause instanceof DeserializeError ||
          cause instanceof ConfigError ||
          (cause instanceof TransportError && !cause.retryable)
        // Background mode resends every failure except a lost confirmation,
        // like Apache Iggy's dispatcher in Rust and Python.
        const retryable =
          fixed === undefined
            ? transient || reauthenticate || (response === undefined && !unusable)
            : !unusable && !LOST_CONFIRMATION_CODES.has(code ?? -1)
        // Retire the connection this attempt ran on, and only while it is still
        // the current one. A concurrent publish may already have replaced it,
        // and destroying that fresh connection would starve every publisher for
        // as long as failures keep arriving.
        if ((response === undefined || reauthenticate) && retryable && this.client === used) {
          this.disconnected.add(used)
          // Destroy before retry so a timed-out queued write cannot run on the old socket.
          await used.destroy().catch(() => undefined)
        }
        if (
          !retryable ||
          attempt === config.maxRetries ||
          (this.connection === undefined &&
            !transient &&
            (fixed === undefined || response === undefined))
        ) {
          if (cause instanceof TimeoutError) throw cause
          throw new TransportError("Iggy publish failed", retryable, { cause })
        }
        const wait = fixed === undefined ? retryDelayMs(config, attempt) : attempt === 0 ? 0 : fixed
        // An abort ends the wait and starts no new attempt. One already sent is not undone.
        await delay(wait, signal, "publish")
      }
    }
    throw new TransportError("publish attempts exhausted", true)
  }

  // A send blocked by an earlier uncertain send of this transport resolves
  // that send first, then goes out once within the same deadline, as the
  // Apache Iggy producer does. The earlier send is never submitted again.
  private async sendPastBlocker(
    operation: (client: SimpleClient) => Promise<SendMessagesResponse>,
    client: SimpleClient,
    deadline: number
  ): Promise<SendMessagesResponse> {
    try {
      return await operation(client)
    } catch (error) {
      const blocker = cause(error, LaneBlockedError)?.operationId
      if (blocker === undefined || !this.uncertainSends.has(blocker)) throw error
      try {
        await client.resolveOperation(blocker, { deadline })
      } catch (resolution) {
        if (cause(resolution, OperationUncertainError) !== undefined) throw error
      }
      this.uncertainSends.delete(blocker)
      return await operation(client)
    }
  }

  async sendMessages(
    streamId: string,
    topicId: string,
    payloads: readonly Uint8Array[],
    routing: Routing,
    options: CallOptions = {}
  ): Promise<SendMessagesResponse> {
    return this.sendRecords(
      streamId,
      topicId,
      payloads.map((payload) => ({ payload, headers: new Map() })),
      routing,
      options
    )
  }

  async sendMessageWithHeaders(
    streamId: string,
    topicId: string,
    payload: Uint8Array,
    headers: ReadonlyMap<string, HeaderValue>,
    partitionKey?: string | Uint8Array,
    partitionId?: number,
    options: CallOptions = {}
  ): Promise<SendMessagesResponse> {
    return this.sendMessagesWithHeaders(
      streamId,
      topicId,
      [{ payload, headers }],
      partitionKey,
      partitionId,
      options
    )
  }

  async sendMessagesWithHeaders(
    streamId: string,
    topicId: string,
    messages: readonly MessageWithHeaders[],
    partitionKey?: string | Uint8Array,
    partitionId?: number,
    options?: Partial<PublishOptions> &
      CallOptions & {
        readonly batchLength?: number
        /** Background mode: resend at once, then every this many milliseconds, any failure but a lost confirmation. */
        readonly fixedRetryIntervalMs?: number
        readonly beforeSend?: () => Promise<{
          readonly streamId: number
          readonly topicId: number
          readonly partitions: number
        }>
      }
  ): Promise<SendMessagesResponse> {
    const routing: Routing =
      partitionId !== undefined
        ? { kind: "partition", partition: partitionId }
        : partitionKey !== undefined
          ? {
              kind: "key",
              key:
                typeof partitionKey === "string"
                  ? new TextEncoder().encode(partitionKey)
                  : partitionKey
            }
          : { kind: "balanced" }
    return this.sendRecords(streamId, topicId, messages, routing, options)
  }

  // Sends the records in consecutive requests of at most `batchLength`, each
  // with bounded retries, like the Rust `send_batch_on`. A failure reports the
  // confirmed requests and every record from the failed request on, with the
  // ids its attempts used. An empty batch sends nothing.
  private async sendRecords(
    streamId: string,
    topicId: string,
    messages: readonly MessageWithHeaders[],
    routing: Routing,
    options?: Partial<PublishOptions> &
      CallOptions & {
        readonly batchLength?: number
        /** Background mode: resend at once, then every this many milliseconds, any failure but a lost confirmation. */
        readonly fixedRetryIntervalMs?: number
        readonly beforeSend?: () => Promise<{
          readonly streamId: number
          readonly topicId: number
          readonly partitions: number
        }>
      }
  ): Promise<SendMessagesResponse> {
    if (messages.length === 0) return { confirmations: [] }
    if (routing.kind === "key" && (routing.key.byteLength === 0 || routing.key.byteLength > 255))
      throw new InvalidError("a partition key must hold 1 to 255 bytes")
    const records = withMessageIds(messages)
    const batchLength = options?.batchLength ?? PUBLISH_BATCH_LENGTH
    const confirmations: SendMessagesConfirmation[] = []
    let resolvedPartition: number | undefined
    for (let start = 0; start < records.length; start += batchLength) {
      const prepared = records.slice(start, start + batchLength).map((record) => ({
        id: record.id ?? mintUlidValue(),
        payload: toNodeBuffer(record.payload),
        headers: [...record.headers].map(([key, value]) => ({
          key: HeaderKeyFactory.String(key),
          value: toIggyHeaderValue(value)
        }))
      }))
      const sendChunk = (): Promise<SendMessagesResponse> =>
        this.publish(async (client) => {
          const source = await options?.beforeSend?.()
          const partition =
            source === undefined
              ? (resolvedPartition ??= await this.resolvePartition(
                  streamId,
                  topicId,
                  routing,
                  client
                ))
              : routing.kind === "partition"
                ? routing.partition
                : routing.kind === "key"
                  ? xxHash32(routing.key) % source.partitions
                  : await this.resolvePartition(streamId, topicId, routing, client)
          if (this.disconnected.has(client))
            throw new TransportError("publish connection was retired", true)
          throwIfAborted(options?.signal, "publish")
          return client.message.send({
            streamId: source?.streamId ?? streamId,
            topicId: source?.topicId ?? topicId,
            messages: prepared,
            partition: Partitioning.PartitionId(partition)
          })
        }, options)
      try {
        let response: SendMessagesResponse
        try {
          response = await sendChunk()
        } catch (cause) {
          const code = serverErrorCode(cause) ?? -1
          const missedRoute = code === PARTITION_NOT_FOUND && routing.kind !== "partition"
          if (!MISSING_RESOURCE_CODES.has(code) && !missedRoute) throw cause
          if (MISSING_RESOURCE_CODES.has(code) && options?.beforeSend === undefined) {
            await this.ensureStream(streamId)
            await this.createTopicIfAbsent(streamId, topicId, 1, {})
          }
          // Rebuild the routing once, as a recreated stream or topic or a
          // topic with fewer partitions needs.
          this.partitionCounts.delete(this.topicKey(streamId, topicId))
          this.balancedCursors.delete(this.topicKey(streamId, topicId))
          if (routing.kind !== "partition") resolvedPartition = undefined
          response = await sendChunk()
        }
        confirmations.push(...response.confirmations)
      } catch (cause) {
        throw new PublishFailedError(streamId, topicId, confirmations, records.slice(start), cause)
      }
    }
    return { confirmations }
  }

  async pollMessages(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    strategy: ConsumerStart,
    count: number,
    autoCommit: boolean,
    wait: PollWait = ONE_SHOT_POLL,
    signal?: AbortSignal
  ): Promise<readonly PolledMessage[]> {
    try {
      return await this.poll(streamId, topicId, target, strategy, count, autoCommit, wait, signal)
    } catch (error) {
      await this.abandonBlocked(error)
      throw error
    }
  }

  private async poll(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    strategy: ConsumerStart,
    count: number,
    autoCommit: boolean,
    wait: PollWait,
    signal: AbortSignal | undefined
  ): Promise<readonly PolledMessage[]> {
    const partitionId = target.partitionId
    if (partitionId === undefined) {
      if (target.kind !== "group") throw new InvalidError("a single consumer needs a partition")
      return this.execute(
        async (client) => {
          const raw = await client.clientProvider()
          const release = raw.hold?.()
          try {
            const state = groupPollState(raw)
            for (;;) {
              const generation = state.sessionGeneration
              try {
                return await pollGroupRecords(
                  raw,
                  state,
                  streamId,
                  topicId,
                  target,
                  strategy,
                  count,
                  autoCommit,
                  wait,
                  signal
                )
              } catch (error) {
                if (
                  state.sessionGeneration === generation ||
                  serverErrorCode(error) === TRANSIENT_NOT_COMMITTED ||
                  unprovenOutcome(error) !== undefined
                )
                  throw error
              }
            }
          } finally {
            release?.()
          }
        },
        `failed to poll topic \`${topicId}\``,
        true,
        signal
      )
    }
    const request = toNodeBuffer(
      encodePollRequest(streamId, topicId, target, partitionId, strategy, count, autoCommit, wait)
    )
    const body = await this.execute(
      async (client) => {
        const response = await (
          await client.clientProvider()
        ).sendCommand(POLL_MESSAGES_DEFERRED_CODE, request, signal === undefined ? {} : { signal })
        return response.length <= 1 ? Buffer.alloc(0) : response.data
      },
      `failed to poll topic \`${topicId}\``,
      true,
      signal
    )
    // The stored records exactly: microsecond times, message ids, and a
    // header block that does not decode marked rather than failing the poll.
    return body.byteLength === 0
      ? []
      : decodePolledBody(new Uint8Array(body.buffer, body.byteOffset, body.byteLength))
  }

  // A committing read whose outcome the client could not prove keeps its lane
  // blocked until it is abandoned. The read is never replayed: the reader
  // resumes after the stored offset, whatever the lost read stored.
  private async abandonBlocked(error: unknown): Promise<void> {
    const blocked =
      cause(error, LaneBlockedError)?.operationId ??
      cause(error, FetchProgressUncertainError)?.operationId
    if (blocked === undefined) return
    try {
      await this.client.abandonOperation(blocked)
    } catch {
      // Not retained means already released. Anything else the next read reports.
    }
  }

  async storeOffset(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    partitionId: number,
    offset: bigint,
    context?: PartitionContext
  ): Promise<void> {
    await this.execute(
      (client) =>
        client.offset.store({
          streamId,
          topicId,
          consumer: toIggyConsumer(target),
          partitionId,
          offset,
          ...(context !== undefined ? { context } : {})
        }),
      `failed to store offset for topic \`${topicId}\``
    )
  }

  async primaryGroups(
    streamId: string,
    topicId: string,
    partitions: readonly number[]
  ): Promise<readonly (readonly number[])[] | undefined> {
    return this.execute(async (client) => {
      const raw = await client.clientProvider()
      return raw.readinessGroups?.(streamId, topicId, [...partitions])
    }, `failed to route the partitions of topic \`${topicId}\``)
  }

  async fetchHeld(
    streamId: string,
    topicId: string,
    offsets: ReadonlyMap<number, bigint>,
    count: number,
    wait: PollWait,
    signal?: AbortSignal,
    reader?: string
  ): Promise<readonly HeldPartition[]> {
    const request = FETCH_MESSAGES.serialize({
      streamId,
      topicId,
      consumer: reader === undefined ? Consumer.Single : { ...Consumer.Single, id: reader },
      targets: [...offsets].map(([partitionId, offset]) => ({
        partitionId,
        pollingStrategy: IggyPollingStrategy.Offset(offset)
      })),
      count,
      autocommit: false,
      options: {
        maxWait: wait.maxWaitMs,
        minCount: wait.minCount,
        maxBytes: wait.maxBytes,
        requestTimeout: wait.maxWaitMs + DEFERRED_POLL_REPLY_MARGIN_MS
      }
    })
    const sections = await this.execute(
      async (client) =>
        FETCH_MESSAGES.deserialize(
          await (
            await client.clientProvider()
          ).sendCommand(FETCH_MESSAGES.code, request, signal === undefined ? {} : { signal })
        ),
      `failed to read topic \`${topicId}\``,
      true,
      signal
    )
    const held: HeldPartition[] = []
    let refused: Error | undefined
    for (const section of sections) {
      if (section.status !== 0) {
        refused ??= fetchSectionError(section)
        continue
      }
      if (section.messagesCount === 0) continue
      // A section carries its records as a poll reply does, after the head.
      const body = new Uint8Array(POLLED_HEAD_BYTES + section.records.byteLength)
      const head = new DataView(body.buffer)
      head.setUint32(0, section.partitionId, true)
      head.setBigUint64(4, section.currentOffset, true)
      head.setUint32(12, section.messagesCount, true)
      head.setBigUint64(16, section.context.incarnation, true)
      head.setBigUint64(24, section.context.ownerGeneration, true)
      head.setBigUint64(32, section.context.metadataOp, true)
      body.set(section.records, POLLED_HEAD_BYTES)
      held.push({ partitionId: section.partitionId, messages: decodePolledBody(body) })
    }
    if (held.length === 0 && refused !== undefined) {
      throw new TransportError(`failed to read topic \`${topicId}\``, true, { cause: refused })
    }
    return held
  }

  /**
   * The replica id of one partition's primary, as any bound replica answers it
   * from its applied metadata, as Rust `partition_primary`.
   */
  async partitionPrimary(streamId: string, topicId: string, partitionId: number): Promise<number> {
    const request = toNodeBuffer(encodePartitionRoute(streamId, topicId, partitionId))
    const reply = await this.execute(
      (client) => client.sendBinaryRequest(GET_PARTITION_ROUTE_CODE, request),
      `failed to read the route of partition ${String(partitionId)}`
    )
    return decodePartitionPrimary(fromNodeBuffer(reply))
  }

  /**
   * Stores a consumer group offset fenced to the partition incarnation and
   * group owner it was read under, as Rust `store_consumer_position`. A changed
   * history answers `HistoryUnavailable`, a moved assignment
   * `ConsumerGroupPartitionNotOwned`.
   */
  async storePosition(position: FencedPosition): Promise<void> {
    await this.execute(
      (client) =>
        client.offset.store({
          streamId: position.streamId,
          topicId: position.topicId,
          consumer:
            position.consumer.kind === "group"
              ? Consumer.Group(position.consumer.id)
              : { kind: 1 as const, id: position.consumer.name },
          partitionId: position.partitionId,
          offset: position.offset,
          context: position.context
        }),
      `failed to store the position of partition ${String(position.partitionId)}`
    )
  }

  async deleteOffset(
    streamId: string,
    topicId: string,
    target: ConsumerTarget,
    partitionId: number
  ): Promise<void> {
    await this.execute(
      (client) =>
        client.offset.delete({
          streamId,
          topicId,
          consumer: toIggyConsumer(target),
          partitionId
        }),
      `failed to delete offset for topic \`${topicId}\``
    )
  }

  async getConsumerOffset(
    streamId: string,
    topicId: string,
    target: ConsumerOffsetTarget,
    partitionId: number
  ): Promise<{ readonly storedOffset: bigint; readonly currentOffset: bigint } | undefined> {
    const offset = await this.execute(
      (client) =>
        client.offset.get({
          streamId,
          topicId,
          consumer: toIggyOffsetConsumer(target),
          partitionId
        }),
      `failed to read offset for topic \`${topicId}\``
    )
    return offset === null
      ? undefined
      : { storedOffset: offset.storedOffset, currentOffset: offset.currentOffset }
  }

  async joinConsumerGroup(streamId: string, topicId: string, name: string): Promise<void> {
    await this.execute(
      (client) => client.group.ensureAndJoin(streamId, topicId, name),
      `failed to join consumer group \`${name}\``
    )
    this.consumerGroups.set(`${streamId}\0${topicId}\0${name}`, {
      streamId,
      topicId,
      name,
      create: true
    })
  }

  async syncConsumerGroup(
    streamId: string,
    topicId: string,
    name: string
  ): Promise<
    | {
        readonly generation: bigint
        readonly partitions: readonly number[]
        readonly rejoined?: boolean
      }
    | undefined
  > {
    const identifiers = [streamId, topicId, name].map((value) => new TextEncoder().encode(value))
    if (identifiers.some((value) => value.byteLength === 0 || value.byteLength > 255))
      throw new ConfigError("consumer group identifiers must contain 1 to 255 UTF-8 bytes")
    const payload = new Uint8Array(
      identifiers.reduce((size, value) => size + 2 + value.byteLength, 0)
    )
    let offset = 0
    for (const value of identifiers) {
      payload.set([2, value.byteLength], offset)
      payload.set(value, offset + 2)
      offset += value.byteLength + 2
    }
    let reply = await this.sendManaged(SYNC_CONSUMER_GROUP_CODE, payload, {
      retryAfterReconnect: true
    })
    let rejoined = false
    if (reply.byteLength === 0) {
      await this.joinConsumerGroup(streamId, topicId, name)
      rejoined = true
      reply = await this.sendManaged(SYNC_CONSUMER_GROUP_CODE, payload, {
        retryAfterReconnect: true
      })
      if (reply.byteLength === 0) return undefined
    }
    if (reply.byteLength < 12) throw new ProtocolError("the consumer group assignment is truncated")
    const view = new DataView(reply.buffer, reply.byteOffset, reply.byteLength)
    const count = view.getUint32(8, true)
    if (reply.byteLength !== 12 + count * 4)
      throw new ProtocolError("the consumer group assignment length is invalid")
    return {
      generation: view.getBigUint64(0, true),
      rejoined,
      partitions: Array.from({ length: count }, (_, index) => view.getUint32(12 + index * 4, true))
    }
  }

  async leaveConsumerGroup(
    streamId: string,
    topicId: string,
    name: string | number
  ): Promise<void> {
    await this.execute(
      (client) => client.group.leave({ streamId, topicId, groupId: name }),
      `failed to leave consumer group \`${String(name)}\``
    )
    this.consumerGroups.delete(`${streamId}\0${topicId}\0${String(name)}`)
  }

  async joinExistingConsumerGroup(
    streamId: string,
    topicId: string,
    name: string | number
  ): Promise<void> {
    await this.execute(
      (client) => client.group.join({ streamId, topicId, groupId: name }),
      `failed to join consumer group \`${String(name)}\``
    )
    this.consumerGroups.set(`${streamId}\0${topicId}\0${String(name)}`, {
      streamId,
      topicId,
      name,
      create: false
    })
  }

  async openCoordinator(): Promise<CoordinatorConnection> {
    if (this.connection === undefined) {
      throw new ConfigError(
        "a coordinator connection needs a Laser connected from a connection string, not an injected client"
      )
    }
    const connected = await connectSimpleClient(
      this.connection,
      Date.now() + connectOptions().timeoutMs
    )
    const coordinator = new ApacheIggyTransport(
      connected.client,
      this.connection,
      "owned",
      this.publishConfig
    )
    const nodes = this.nodes
    return {
      // A connection of its own picks its own node, as a Rust `Laser` connected
      // for it would.
      async send(code: number, payload: Uint8Array): Promise<Uint8Array> {
        const answer = await coordinator.nodes.send(
          coordinator.runner(),
          code,
          payload,
          managedTargetOf(code)
        )
        return answer.reply
      },
      sendManagedTo: (code, payload, target, options) =>
        nodes.send(coordinator.runner(), code, payload, target, options?.deadline),
      partitionPrimary: (streamId, topicId, partitionId) =>
        coordinator.partitionPrimary(streamId, topicId, partitionId),
      storePosition: (position) => coordinator.storePosition(position),
      storeOffset: (streamId, topicId, target, partitionId, offset) =>
        coordinator.storeOffset(streamId, topicId, target, partitionId, offset),
      async joinConsumerGroup(
        streamId: string,
        topicId: string,
        name: string | number
      ): Promise<void> {
        await coordinator.execute(
          (client) => client.group.join({ streamId, topicId, groupId: name }),
          `failed to join consumer group \`${String(name)}\``
        )
      },
      async leaveConsumerGroup(
        streamId: string,
        topicId: string,
        name: string | number
      ): Promise<void> {
        await coordinator.execute(
          (client) => client.group.leave({ streamId, topicId, groupId: name }),
          `failed to leave consumer group \`${String(name)}\``
        )
      },
      close: () => coordinator.close()
    }
  }

  async close(): Promise<void> {
    this.closed = true
    this.lifetime.abort()
    this.consumerGroups.clear()
    if (this.ownership === "owned") await this.client.destroy()
  }

  private topicKey(streamId: string, topicId: string): string {
    return `${streamId}\0${topicId}`
  }

  private async resolvePartition(
    streamId: string,
    topicId: string,
    routing: Routing,
    client: SimpleClient
  ): Promise<number> {
    if (routing.kind === "partition") return routing.partition
    const key = this.topicKey(streamId, topicId)
    let cached = this.partitionCounts.get(key)
    if (cached === undefined || Date.now() - cached.readAt >= PARTITION_COUNT_MAX_AGE_MS) {
      const topic = await client.topic.get({ streamId, topicId })
      if (topic === null) {
        throw missingResource(
          `topic \`${topicId}\` on stream \`${streamId}\` does not exist`,
          TOPIC_NAME_NOT_FOUND
        )
      }
      cached = { count: topic.partitionsCount, readAt: Date.now() }
      this.partitionCounts.set(key, cached)
    }
    const partitionCount = cached.count
    if (partitionCount <= 0) {
      throw new TransportError(
        `topic \`${topicId}\` on stream \`${streamId}\` has no partitions`,
        false
      )
    }
    if (routing.kind === "key") return xxHash32(routing.key) % partitionCount
    const cursor = this.balancedCursors.get(key) ?? 0
    this.balancedCursors.set(key, (cursor + 1) >>> 0)
    return cursor % partitionCount
  }
}

function groupPollState(client: RawClient): GroupPollState {
  const current = GROUP_POLL_STATES.get(client)
  if (current !== undefined) return current
  const state: GroupPollState = { cursors: new Map(), sessionGeneration: 0 }
  GROUP_POLL_STATES.set(client, state)
  client.on("sessionReset", () => {
    state.cursors.clear()
    state.sessionGeneration += 1
  })
  client.on("heartbeat", () => {
    for (const cursor of state.cursors.values()) cursor.synchronizedAt = 0
  })
  return state
}

async function pollGroupRecords(
  client: RawClient,
  state: GroupPollState,
  streamId: string,
  topicId: string,
  target: Extract<ConsumerTarget, { readonly kind: "group" }>,
  strategy: ConsumerStart,
  count: number,
  autoCommit: boolean,
  wait: PollWait,
  signal: AbortSignal | undefined
): Promise<readonly PolledMessage[]> {
  const key = `${streamId}\0${topicId}\0${target.name}`
  for (let attempt = 0; attempt < GROUP_POLL_MAX_ATTEMPTS; attempt += 1) {
    let cursor = state.cursors.get(key)
    const age = cursor === undefined ? -1 : Date.now() - cursor.synchronizedAt
    if (cursor === undefined || age < 0 || age >= GROUP_ASSIGNMENT_REFRESH_MS) {
      const group = { streamId, topicId, groupId: target.name }
      let assignment
      try {
        assignment = SYNC_GROUP.deserialize(
          await client.sendCommand(SYNC_GROUP.code, SYNC_GROUP.serialize(group))
        )
      } catch (error) {
        if (serverErrorCode(error) !== GROUP_MEMBER_NOT_FOUND) throw error
        assignment = null
      }
      if (assignment === null) {
        await client.sendCommand(JOIN_GROUP.code, JOIN_GROUP.serialize(group))
        assignment = SYNC_GROUP.deserialize(
          await client.sendCommand(SYNC_GROUP.code, SYNC_GROUP.serialize(group))
        )
      }
      if (assignment === null) throw new ProtocolError("the consumer group assignment is absent")
      cursor = {
        generation: assignment.generation,
        partitions: assignment.partitions,
        position:
          cursor?.generation === assignment.generation &&
          cursor.position < assignment.partitions.length
            ? cursor.position
            : 0,
        synchronizedAt: Date.now(),
        idle: cursor?.generation === assignment.generation ? cursor.idle : 0
      }
      state.cursors.set(key, cursor)
    }
    const partition = cursor.partitions[cursor.position]
    if (partition === undefined) return []
    cursor.position = (cursor.position + 1) % cursor.partitions.length
    let body: Uint8Array
    try {
      const response = await client.sendCommand(
        POLL_MESSAGES_DEFERRED_CODE,
        toNodeBuffer(
          encodePollRequest(
            streamId,
            topicId,
            target,
            partition,
            strategy,
            count,
            autoCommit,
            passWait(wait, cursor.partitions.length, cursor.idle)
          )
        ),
        signal === undefined ? {} : { signal }
      )
      body = new Uint8Array(
        response.data.buffer,
        response.data.byteOffset,
        response.data.byteLength
      )
    } catch (error) {
      const code = serverErrorCode(error)
      if (code !== GROUP_MEMBER_NOT_FOUND && code !== GROUP_PARTITION_NOT_OWNED) throw error
      state.cursors.delete(key)
      continue
    }
    if (body.byteLength === 0) return []
    const view = new DataView(body.buffer, body.byteOffset, body.byteLength)
    if (
      body.byteLength >= POLLED_HEAD_BYTES &&
      view.getUint32(0, true) === RESYNC_REQUIRED_PARTITION &&
      view.getUint32(12, true) === 0
    ) {
      state.cursors.delete(key)
      continue
    }
    const messages = decodePolledBody(body)
    cursor.idle = messages.length === 0 ? cursor.idle + 1 : 0
    return messages
  }
  return []
}

/**
 * The server wait of one read in a pass over `partitions`: none until a whole
 * pass found nothing, then at most `wait.maxWaitMs / partitions` each, so a
 * record on another partition waits at most one pass. A single partition
 * always waits.
 * @internal
 */
export function passWait(wait: PollWait, partitions: number, idleReads: number): PollWait {
  if (partitions <= 1) return wait
  if (idleReads < partitions || wait.maxWaitMs === 0) return { ...wait, maxWaitMs: 0 }
  return { ...wait, maxWaitMs: Math.max(1, Math.floor(wait.maxWaitMs / partitions)) }
}

/** The first error of type `type` in the cause chain of `error`. */
function cause<Type>(
  error: unknown,
  type: abstract new (...args: never[]) => Type
): Type | undefined {
  let current = error
  for (let depth = 0; depth < 8; depth += 1) {
    if (current instanceof type) return current
    if (typeof current !== "object" || current === null || !("cause" in current)) return undefined
    current = (current as { readonly cause?: unknown }).cause
  }
  return undefined
}

// An outcome the client could not prove: a reply lost after the command may
// have applied, an abandoned operation, a call past its deadline, or a lane an
// uncertain read blocks.
function unprovenOutcome(error: unknown): Error | undefined {
  return (
    cause(error, OperationUncertainError) ??
    cause(error, OperationNotRetainedError) ??
    cause(error, VsrResponseTimeoutError) ??
    cause(error, LaneBlockedError)
  )
}

/**
 * Whether a failed send wrote its frame and lost the reply: the client proved
 * no outcome for it, as Rust `TransientNotCommitted` or `OperationUncertain`.
 * @internal
 */
export function frameWritten(error: unknown): boolean {
  return error instanceof AmbiguousMutationError || unprovenOutcome(error) !== undefined
}

/**
 * Whether a failed send never wrote its frame: no link carried it.
 * @internal
 */
export function frameNotWritten(error: unknown): boolean {
  return cause(error, NotConnectedError) !== undefined
}

/**
 * A refusal before any frame was written, as `frameNotWritten` reads it.
 * @internal
 */
export function notWrittenError(message: string): TransportError {
  return new TransportError(message, true, { cause: new NotConnectedError() })
}

/**
 * Whether a poll failed on a partition lane an uncertain read blocked. The
 * transport abandoned that read, so the consumer resumes the partition after
 * its stored offset, as Rust seeks it to `PollingStrategy::next()`.
 * @internal
 */
export function resumesAfterStoredOffset(error: unknown): boolean {
  return (
    cause(error, LaneBlockedError) !== undefined ||
    cause(error, FetchProgressUncertainError) !== undefined
  )
}

import { type CallOptions, abortable } from "../client/call-options.js"
import {
  INTERNAL_ASSIGNED_PARTITIONS,
  INTERNAL_COMMIT_HANDLED,
  INTERNAL_RETURN_DELIVERY
} from "../client/internals.js"
import { mintUlidValue } from "../runtime/ulid.js"
import {
  type ConsumerTarget,
  DEFAULT_POLL_MAX_BYTES,
  type LaserTransport,
  MAX_DEFERRED_POLL_WAIT_MS,
  MIN_DEFERRED_POLL_MAX_BYTES,
  type PartitionContext,
  type PollWait,
  type PolledMessage,
  passWait,
  resumesAfterStoredOffset,
  serverErrorCode
} from "../iggy/apache-iggy.js"
import {
  CancelledError,
  FilterExecutionError,
  InvalidError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "../client/errors.js"
import type { FilteredReader, MatchedRecord } from "../managed/filters.js"
import type { MessageId } from "../types/ids.js"
import { Json } from "./codecs.js"
import { type ConsumerStart, storeSeeds } from "./consumer-start.js"
import type { Message } from "./message.js"

/** A message a live consumer delivered, with its stored header like the Rust
 * `ConsumerMessage`. A value the transport did not report reads `0n`.
 * `json()` decodes its payload. */
export interface ConsumerMessage extends Message {
  readonly partitionId: number
  /** The record's log position. */
  readonly position: MessageId
  /** The id the record was sent with. */
  readonly messageId: bigint
  /** The record checksum as stored. */
  readonly checksum: bigint
  /** The partition head when the record was read. */
  readonly currentOffset: bigint
  /** The server timestamp in microseconds. */
  readonly timestampMicros: bigint
  /** The producer timestamp in microseconds. */
  readonly originTimestampMicros: bigint
  /** The user-header block exactly as stored, for a caller that decodes its
   * entries itself. Absent when the record has none. */
  readonly userHeaders?: Uint8Array
  /** True when the header block does not decode. `headers` then holds what
   * did decode, and the payload stays readable. */
  readonly headersMalformed: boolean
  /** Decodes the payload as JSON, through `decodeValue` when given. A payload
   * that is not JSON fails with `CodecError`. */
  json<T = unknown>(decodeValue?: (value: unknown) => T): T
}

/** A consumer's server-stored offset on one partition and the partition head. */
export interface StoredOffset {
  readonly storedOffset: bigint
  readonly currentOffset: bigint
}

/**
 * When a live consumer stores offsets on the server, the same ten policies the
 * Rust `CommitPolicy` defines. `disabled` never stores on its own: call
 * `commit` after a record is handled. `polling` commits the polled batch on
 * the server before delivery, so a crash can skip records not yet processed.
 * The others store only handled records: a record counts as handled once the
 * caller asks for the next one or shuts the consumer down, so a crash
 * delivers the record in hand again. `all` stores once the last record of a
 * poll is handled, `each` every handled record, `every` after `count` handled
 * records, and the `interval*` forms also store the handled records on a
 * timer.
 */
export type CommitPolicy =
  | { readonly kind: "disabled" }
  | { readonly kind: "interval"; readonly intervalMs: number }
  | { readonly kind: "polling" }
  | { readonly kind: "intervalOrPolling"; readonly intervalMs: number }
  | { readonly kind: "all" }
  | { readonly kind: "intervalOrAll"; readonly intervalMs: number }
  | { readonly kind: "each" }
  | { readonly kind: "intervalOrEach"; readonly intervalMs: number }
  | { readonly kind: "every"; readonly count: number }
  | { readonly kind: "intervalOrEvery"; readonly intervalMs: number; readonly count: number }

export interface ConsumerOptions {
  /** Most records one poll returns. Defaults to 1000, like Rust and Python. */
  readonly batchLength?: number
  /** Defaults to `all`, and to `disabled` for a consumer without a name. */
  readonly commitPolicy?: CommitPolicy
  /** Where reading starts, `next` by default. A group consumer always resumes
   * after the group's stored offset: its start only seeds the partitions the
   * group never stored, taken when it joins. */
  readonly startAt?: ConsumerStart
  /** How long the server holds a poll that finds fewer than `minCount` new
   * records before it answers with what it has. Defaults to 5 seconds, at most
   * 600 seconds. Zero answers at once. A policy-aware group consumer holds its
   * idle rounds as long, within the server's filtered wait. */
  readonly maxWaitMs?: number
  /** How many new records end a held poll early. Defaults to 1, at most
   * `batchLength`. Native only: a policy-aware group consumer holds its idle
   * rounds through the group reader. */
  readonly minCount?: number
  /** Most record bytes one poll returns. Defaults to 1 MiB. A record larger
   * than the cap comes alone, so a poll always makes progress. A policy-aware
   * group consumer caps its filtered pages with it, up to 8 MiB. */
  readonly maxBytes?: number
  /** Create the consumer group when it does not exist. Defaults to `true`. */
  readonly createGroup?: boolean
  /** How long a failed poll waits before it is retried. Must be positive.
   * Defaults to 1 second. */
  readonly pollingRetryIntervalMs?: number
  /** Retries for building the consumer, `intervalMs` (positive) apart. A
   * native consumer retries while its stream or topic is missing. A
   * policy-aware group consumer retries a failure a later attempt can clear. */
  readonly initRetries?: { readonly retries: number; readonly intervalMs: number }
  /** Native only: deliver records at or below an offset already consumed,
   * for example after moving the start back. Without it, such records are
   * skipped. */
  readonly allowReplay?: boolean
}

const DEFAULT_BATCH_LENGTH = 1000
const DEFAULT_MAX_WAIT_MS = 5_000
const DEFAULT_MIN_COUNT = 1
const DEFAULT_POLLING_RETRY_INTERVAL_MS = 1_000
const DEFAULT_START_AT: ConsumerStart = { kind: "next" }
// Refusals that last until an operator acts, on which the Apache Iggy reader
// stops a partition: invalid command, feature unavailable, unauthorized, a
// missing stream, topic or partition, and a partition the member no longer owns.
const LASTING_REFUSALS = new Set([3, 5, 41, 1009, 1010, 2010, 2011, 3007, 5009])
// The refusals that mean the partition's history is gone.
const HISTORY_GONE = new Set([1009, 1010, 2010, 2011, 3007])
// A store fenced to a delivery refused because the partition was recreated
// since (87) or moved to another group member (5009).
const STALE_DELIVERY = new Set([87, 5009])
// Refused polls in a row on one partition before the refusal is reported,
// the count after which the Apache Iggy reader stops a partition.
const REFUSALS_PER_REPORT = 6

function throwIfAborted(signal: AbortSignal | undefined, message: string): void {
  if (signal?.aborted === true) throw new CancelledError(message, { cause: signal.reason })
}

function delay(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new CancelledError("wait aborted", { cause: signal.reason }))
      return
    }
    const onAbort = (): void => {
      clearTimeout(timer)
      reject(new CancelledError("wait aborted", { cause: signal?.reason }))
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort)
      resolve()
    }, ms)
    signal?.addEventListener("abort", onAbort, { once: true })
  })
}

/**
 * What a live consumer does with a retryable poll failure. `retry` waits the
 * polling retry interval and polls again, like the Rust consumer. `propagate`
 * hands it to a runtime that reopens the consumer itself.
 *
 * @internal
 */
export type PollFailures = "retry" | "propagate"

/** A consumer's options with every default applied and validated. */
export interface ResolvedConsumerOptions {
  readonly batchLength: number
  readonly autoCommit: boolean
  readonly commitPolicy: CommitPolicy
  readonly startAt: ConsumerStart
  readonly maxWaitMs: number
  readonly minCount: number
  readonly maxBytes: number
  readonly createGroup: boolean
  readonly pollingRetryIntervalMs: number
  readonly initRetries: { readonly retries: number; readonly intervalMs: number }
  readonly allowReplay: boolean
}

function commitInterval(policy: CommitPolicy): number | undefined {
  return "intervalMs" in policy ? policy.intervalMs : undefined
}

function validateCommitPolicy(policy: CommitPolicy): void {
  const interval = commitInterval(policy)
  if (interval !== undefined && (!Number.isFinite(interval) || interval <= 0)) {
    throw new InvalidError("commit policy intervalMs must be a positive finite number")
  }
  if ("count" in policy && (!Number.isSafeInteger(policy.count) || policy.count < 1)) {
    throw new InvalidError("commit policy count must be a positive safe integer")
  }
}

/** Apply the defaults and reject values no consumer can run with. */
export function resolveConsumerOptions(
  options: ConsumerOptions,
  anonymous: boolean
): ResolvedConsumerOptions {
  const commitPolicy: CommitPolicy =
    options.commitPolicy ?? (anonymous ? { kind: "disabled" } : { kind: "all" })
  validateCommitPolicy(commitPolicy)
  const initRetries = options.initRetries ?? { retries: 0, intervalMs: 0 }
  const resolved = {
    batchLength: options.batchLength ?? DEFAULT_BATCH_LENGTH,
    autoCommit: commitPolicy.kind !== "disabled",
    commitPolicy,
    startAt: options.startAt ?? DEFAULT_START_AT,
    maxWaitMs: options.maxWaitMs ?? DEFAULT_MAX_WAIT_MS,
    minCount: options.minCount ?? DEFAULT_MIN_COUNT,
    maxBytes: options.maxBytes ?? DEFAULT_POLL_MAX_BYTES,
    createGroup: options.createGroup ?? true,
    pollingRetryIntervalMs: options.pollingRetryIntervalMs ?? DEFAULT_POLLING_RETRY_INTERVAL_MS,
    initRetries,
    allowReplay: options.allowReplay ?? false
  }
  if (!Number.isFinite(resolved.pollingRetryIntervalMs) || resolved.pollingRetryIntervalMs <= 0) {
    throw new InvalidError("consumer pollingRetryIntervalMs must be a positive finite number")
  }
  if (
    !Number.isSafeInteger(initRetries.retries) ||
    initRetries.retries < 0 ||
    !Number.isFinite(initRetries.intervalMs) ||
    (options.initRetries === undefined ? initRetries.intervalMs < 0 : initRetries.intervalMs <= 0)
  ) {
    throw new InvalidError(
      "consumer initRetries needs non-negative retries and a positive intervalMs"
    )
  }
  if (!Number.isSafeInteger(resolved.batchLength) || resolved.batchLength < 1) {
    throw new InvalidError("consumer batchLength must be a positive safe integer")
  }
  if (
    !Number.isSafeInteger(resolved.maxWaitMs) ||
    resolved.maxWaitMs < 0 ||
    resolved.maxWaitMs > MAX_DEFERRED_POLL_WAIT_MS
  ) {
    throw new InvalidError(`consumer maxWaitMs must be 0..=${String(MAX_DEFERRED_POLL_WAIT_MS)}`)
  }
  if (
    !Number.isSafeInteger(resolved.minCount) ||
    resolved.minCount < 1 ||
    resolved.minCount > resolved.batchLength
  ) {
    throw new InvalidError("consumer minCount must be 1..=batchLength")
  }
  if (
    !Number.isSafeInteger(resolved.maxBytes) ||
    resolved.maxBytes < MIN_DEFERRED_POLL_MAX_BYTES ||
    resolved.maxBytes > 0xffff_ffff
  ) {
    throw new InvalidError(
      `consumer maxBytes must be at least ${String(MIN_DEFERRED_POLL_MAX_BYTES)}`
    )
  }
  if (
    (resolved.startAt.kind === "offset" || resolved.startAt.kind === "timestampMicros") &&
    resolved.startAt.value < 0n
  ) {
    throw new InvalidError("consumer start value must be non-negative")
  }
  return resolved
}

/**
 * Polls one partition or consumer group until shutdown. A group consumer on a
 * server that resolves group policies reads through the group reader: the
 * server runs the group's filter, or none, and commits go through the group's
 * fenced acknowledgments. `batchLength` then also bounds the source records
 * one partition poll examines.
 */
export class Consumer implements AsyncIterable<ConsumerMessage>, AsyncDisposable {
  private readonly batchLength: number
  private readonly autoCommit: boolean
  private readonly commitPolicy: CommitPolicy
  private readonly startAt: ConsumerStart
  private readonly wait: PollWait
  private readonly pollingRetryIntervalMs: number
  private readonly allowReplay: boolean
  private buffer: PolledMessage[] = []
  // Native deliveries handed back, yielded again before the buffer.
  private readonly returned: ConsumerMessage[] = []
  private readonly storedOffsets = new Map<number, bigint>()
  // Native: the delivery the caller holds, handled once it asks for the next
  // record or shuts down, and the handled offsets the commit policy stores.
  private uncounted: NativeDelivery | undefined
  private readonly countedOffsets = new Map<number, Counted>()
  // The partition history each native delivery was read under, which a
  // store of its offset is fenced to.
  private readonly deliveries = new WeakMap<ConsumerMessage, DeliveredUnder>()
  // A sought start per partition, read by its next poll, and the seek count
  // when each partition was last sought, so a poll in flight across a seek
  // drops its records.
  private readonly soughtStarts = new Map<number, ConsumerStart>()
  private readonly soughtAt = new Map<number, number>()
  private seeks = 0
  // A native group consumer that sought holds each partition's position
  // itself from then on, reading the partitions in turn.
  private selectsPartitions = false
  private yieldedSinceStore = 0
  private commitTimer: ReturnType<typeof setInterval> | undefined
  private latestDelivered: ConsumerMessage | undefined
  // Group reads: the records of the current page, the record each delivered
  // message came from, and whether a delivery waits to be stored.
  private readonly records: MatchedRecord[] = []
  private readonly delivered = new WeakMap<ConsumerMessage, MatchedRecord>()
  private lastDelivery: { readonly record: MatchedRecord; readonly lastOfPage: boolean } | undefined
  // When a group consumer's commit policy last stored the handled prefix, the
  // records yielded since, and the store decision a failed store left due.
  private yieldedSinceFlush = 0
  private lastFlushAt = Date.now()
  private pendingCommit: CommitPoint | undefined
  private moreScanned = false
  // Records handled out of band, acknowledged in order before the next read.
  private readonly deferred: MatchedRecord[] = []
  private groupRead = false
  private readonly consumedOffsets = new Map<number, bigint>()
  private readonly explicitOffsets = new Map<number, bigint>()
  // The consumed offset a partition had when its server offset was deleted.
  // No commit policy stores that offset again, so the delete stands until a
  // newer record is delivered.
  private readonly deletedAt = new Map<number, bigint>()
  private readonly nextOffsets = new Map<number, bigint>()
  private assignedPartitions = new Set<number>()
  private assignmentSeen = false
  private partitionCursor = 0
  // The offsets a group's start seeds, taken at the join, each stored when
  // this member first owns its partition. Every group read resumes after the
  // stored offset.
  private readonly seeds: Map<number, bigint>
  // Partitions whose uncertain committing read was abandoned. They resume
  // after the stored offset, whatever the lost read stored.
  private readonly resumeAfterStored = new Set<number>()
  // A lasting refusal holds the next native poll until then, so a caller
  // that reads on through it never spins.
  private refusedUntil = 0
  // Refused polls in a row by partition, the server-picked one under -1.
  private readonly refusals = new Map<number, number>()
  // Consecutive native polls that found nothing, so a whole idle pass over a
  // group's partitions holds its next reads on the server.
  private idleReads = 0
  // Cancels the native poll the server holds when the consumer shuts down.
  private readonly pollAbort = new AbortController()
  private shuttingDown = false
  private polling: Promise<void> | undefined
  // A native group consumer leaves at shutdown only the group it joined.
  private readonly leavesGroup: boolean

  private constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    private readonly topicName: string,
    private readonly target: ConsumerTarget,
    options: ConsumerOptions = {},
    private readonly reader?: FilteredReader,
    private readonly pollFailures: "retry" | "propagate" = "retry",
    seeds: ReadonlyMap<number, bigint> = new Map()
  ) {
    const anonymous = target.kind === "single" && target.name === undefined
    if (anonymous) this.target = { ...target, name: `anonymous-${mintUlidValue().toString(16)}` }
    if (
      this.target.kind === "single" &&
      (!Number.isSafeInteger(this.target.partitionId) || this.target.partitionId < 0)
    ) {
      throw new InvalidError("consumer partition must be a non-negative safe integer")
    }
    const resolved = resolveConsumerOptions(options, anonymous)
    this.batchLength = resolved.batchLength
    this.autoCommit = resolved.autoCommit
    this.commitPolicy = resolved.commitPolicy
    this.startAt = resolved.startAt
    this.seeds = new Map(seeds)
    this.wait = {
      maxWaitMs: resolved.maxWaitMs,
      minCount: resolved.minCount,
      maxBytes: resolved.maxBytes
    }
    this.pollingRetryIntervalMs = resolved.pollingRetryIntervalMs
    this.allowReplay = resolved.allowReplay
    this.leavesGroup = this.target.kind === "group"
  }

  /** @internal */
  static create(
    transport: LaserTransport,
    streamName: string,
    topicName: string,
    target: ConsumerTarget,
    options: ConsumerOptions = {},
    reader?: FilteredReader,
    pollFailures: "retry" | "propagate" = "retry",
    seeds: ReadonlyMap<number, bigint> = new Map()
  ): Consumer {
    return new Consumer(
      transport,
      streamName,
      topicName,
      target,
      options,
      reader,
      pollFailures,
      seeds
    )
  }

  /**
   * Waits for the next record, bounding how long the caller sits idle. Fails
   * with `TimeoutError` past `timeoutMs` and with `InvalidError` once the
   * consumer was shut down, like the Rust `next_within`.
   */
  async nextWithin(
    timeoutMs: number,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<ConsumerMessage> {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0) {
      throw new InvalidError("nextWithin timeout must be a non-negative finite number")
    }
    const deadline = Date.now() + timeoutMs
    for (;;) {
      if (this.stopped()) throw new InvalidError("the live consumer stream ended")
      throwIfAborted(options.signal, "nextWithin aborted")
      await this.finishDelivery()
      const message = this.take()
      if (message !== undefined) return message
      await this.pollOnce(options.signal, deadline)
      throwIfAborted(options.signal, "nextWithin aborted")
      if (this.stopped()) throw new InvalidError("the live consumer stream ended")
      if (this.pending()) continue
      if (deadline - Date.now() <= 0) throw new TimeoutError("the live consumer to yield a record")
      // A busy scan yields to timers and cancellation between bounded rounds.
      if (this.moreScanned) await delay(0, options.signal)
    }
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ConsumerMessage> {
    yield* this.stream()
  }

  async *stream(options: { readonly signal?: AbortSignal } = {}): AsyncIterable<ConsumerMessage> {
    while (!this.stopped()) {
      throwIfAborted(options.signal, "consumer stream aborted")
      await this.finishDelivery()
      const message = this.take()
      if (message !== undefined) {
        yield message
        continue
      }
      await this.pollOnce(options.signal)
      throwIfAborted(options.signal, "consumer stream aborted")
      if (this.stopped()) return
      if (!this.pending() && this.moreScanned) await delay(0, options.signal)
    }
  }

  /**
   * Hands back the latest delivery that the caller could not process, so the
   * next read yields it again. Only the most recent delivery can be returned.
   */
  [INTERNAL_RETURN_DELIVERY](message: ConsumerMessage): void {
    if (this.stopped()) throw new InvalidError("consumer has been shut down")
    if (message !== this.latestDelivered) {
      throw new InvalidError("the delivery is not the latest one from this consumer")
    }
    this.latestDelivered = undefined
    if (this.reader !== undefined) {
      const record = this.delivered.get(message)
      if (record === undefined) {
        throw new InvalidError("the record was not delivered by this group consumer")
      }
      if (this.lastDelivery?.record === record) this.lastDelivery = undefined
      this.records.unshift(record)
      return
    }
    if (this.uncounted?.message === message) this.uncounted = undefined
    this.returned.unshift(message)
  }

  /**
   * Store a handled message's offset on the server. It needs the message
   * object this consumer delivered. A native consumer stores it fenced to the
   * partition history the message was read under, so a commit after the
   * partition was recreated or moved to another group member is refused
   * instead of landing on the newer history. Under `polling` the server
   * already stored the poll, and the commit stores the offset unfenced. A
   * group consumer on a server that resolves group policies stores the
   * contiguous prefix of the partition through this message.
   */
  async commit(message: ConsumerMessage, options: CallOptions = {}): Promise<void> {
    throwIfAborted(options.signal, "consumer commit aborted")
    if (this.reader !== undefined) {
      const record = this.delivered.get(message)
      if (record === undefined) {
        throw new InvalidError("commit the message object this group consumer delivered")
      }
      await this.reader.ackThrough(record, options)
      this.explicitOffsets.set(message.partitionId, message.position.offset)
      this.storedOffsets.set(message.partitionId, message.position.offset)
      return
    }
    const delivery = this.deliveries.get(message)
    if (delivery === undefined) {
      throw new InvalidError("commit the message object this consumer delivered")
    }
    const polling =
      this.commitPolicy.kind === "polling" || this.commitPolicy.kind === "intervalOrPolling"
    await abortable(
      this.storeAt(
        message.position.offset,
        message.partitionId,
        polling ? undefined : delivery.context
      ),
      options.signal,
      "consumer commit"
    )
  }

  /**
   * Pass the record a `FilterFaultError` or a `FilterOversizedRecordError`
   * stopped `partitionId` at, and read on after it, as `FilteredReader.skip`
   * does. Policy-aware group consumers only: a native consumer runs no filter,
   * so nothing stops it there, and `storeOffset` moves it.
   */
  async skip(partitionId: number, offset: bigint, options: CallOptions = {}): Promise<void> {
    throwIfAborted(options.signal, "consumer skip aborted")
    if (this.reader === undefined) {
      throw new InvalidError(
        "a native consumer runs no filter, so no fault stops it. Store an offset instead"
      )
    }
    await this.reader.skip(partitionId, offset, options)
  }

  /**
   * Commit a handled message without waiting for a read in flight. A group
   * consumer queues the acknowledgment and sends it before its next read, so
   * the read is never cancelled. A native consumer stores the offset at once.
   *
   * @internal
   */
  async [INTERNAL_COMMIT_HANDLED](message: ConsumerMessage): Promise<void> {
    if (this.reader === undefined) {
      await this.commit(message)
      return
    }
    const record = this.delivered.get(message)
    if (record === undefined) {
      throw new InvalidError("the record was not delivered by this group consumer")
    }
    this.deferred.push(record)
  }

  /**
   * The partitions this group consumer reads now. A native consumer knows the
   * assignment its last group sync returned. `undefined` before the first
   * read, and for a native consumer that never syncs.
   *
   * @internal
   */
  [INTERNAL_ASSIGNED_PARTITIONS](): ReadonlySet<number> | undefined {
    if (this.reader === undefined) {
      return this.assignmentSeen ? new Set(this.assignedPartitions) : undefined
    }
    return this.groupRead ? new Set(this.reader.partitions()) : undefined
  }

  lastConsumedOffset(partitionId: number): bigint | undefined {
    return this.consumedOffsets.get(partitionId)
  }

  /** The last offset this consumer stored for `partitionId`, by `commit`,
   * `storeOffset`, or its commit policy. Local bookkeeping: for server-side
   * resume, start from `next`. */
  lastStoredOffset(partitionId: number): bigint | undefined {
    return this.storedOffsets.get(partitionId)
  }

  /**
   * Stores an explicit server offset for `partitionId`, or for the partition
   * of the latest delivery when omitted. Native only: a policy-aware group
   * consumer refuses it, because an arbitrary offset bypasses the group's
   * acknowledgment contract. Commit a delivered record instead.
   */
  async storeOffset(
    offset: bigint,
    partitionId?: number,
    options: CallOptions = {}
  ): Promise<void> {
    throwIfAborted(options.signal, "consumer offset store aborted")
    this.requireNative("storeOffset")
    if (offset < 0n) throw new InvalidError("offset must be non-negative")
    await abortable(
      this.storeAt(offset, this.currentPartition(partitionId)),
      options.signal,
      "consumer offset store"
    )
  }

  /** Deletes the server offset for `partitionId`, or for the partition of the
   * latest delivery when omitted. Native only, like `storeOffset`. */
  async deleteOffset(partitionId?: number, options: CallOptions = {}): Promise<void> {
    throwIfAborted(options.signal, "consumer offset delete aborted")
    this.requireNative("deleteOffset")
    if (this.transport.deleteOffset === undefined) {
      throw new UnsupportedError("the Apache Iggy client does not expose offset deletion", {
        surface: "stream"
      })
    }
    const partition = this.currentPartition(partitionId)
    // Cancellation can hide a committed delete, so a later store must reach the server.
    this.storedOffsets.delete(partition)
    await abortable(
      this.transport.deleteOffset(this.streamName, this.topicName, this.target, partition),
      options.signal,
      "consumer offset delete"
    )
    this.explicitOffsets.delete(partition)
    const consumed = this.consumedOffsets.get(partition)
    if (consumed === undefined) this.deletedAt.delete(partition)
    else this.deletedAt.set(partition, consumed)
  }

  /**
   * Moves the next read of `partitionId` to `start`, as the Rust
   * `Consumer::seek` does: `next` reads on after the stored offset. The
   * records of the partition this consumer holds but has not handed out are
   * dropped, a poll in flight for it is discarded, and the records delivered
   * before the seek count no more, so no commit policy stores them. The
   * sought records are delivered again, also without `allowReplay`. Under
   * `polling` a seek to an offset above 0 first stores the offset before it,
   * so a restart resumes at the new position. Every other seek stores
   * nothing. A native group consumer then holds each partition's position
   * itself, reading its partitions in turn. Native only: a policy-aware group
   * consumer refuses it, because a position outside a delivery bypasses the
   * group's acknowledgments.
   */
  async seek(partitionId: number, start: ConsumerStart, options: CallOptions = {}): Promise<void> {
    throwIfAborted(options.signal, "consumer seek aborted")
    this.requireNative("seek")
    if (this.stopped()) throw new InvalidError("consumer has been shut down")
    if (!Number.isSafeInteger(partitionId) || partitionId < 0) {
      throw new InvalidError("seek partition must be a non-negative safe integer")
    }
    if (this.target.kind === "single" && this.target.partitionId !== partitionId) {
      throw new InvalidError(
        `this consumer reads partition ${String(this.target.partitionId)}, not ${String(partitionId)}`
      )
    }
    if ((start.kind === "offset" || start.kind === "timestampMicros") && start.value < 0n) {
      throw new InvalidError("seek start value must be non-negative")
    }
    if (this.target.kind === "group" && this.transport.syncConsumerGroup === undefined) {
      throw new UnsupportedError("this transport cannot hold a group partition's position", {
        surface: "stream"
      })
    }
    const polling =
      this.commitPolicy.kind === "polling" || this.commitPolicy.kind === "intervalOrPolling"
    const storedBefore = polling && start.kind === "offset" && start.value > 0n
    if (storedBefore) {
      await this.transport.storeOffset(
        this.streamName,
        this.topicName,
        this.target,
        partitionId,
        start.value - 1n
      )
    }
    this.seeks += 1
    this.soughtAt.set(partitionId, this.seeks)
    this.soughtStarts.set(partitionId, start)
    this.buffer = this.buffer.filter((message) => message.partitionId !== partitionId)
    const handedBack = this.returned.filter((message) => message.partitionId !== partitionId)
    this.returned.splice(0, this.returned.length, ...handedBack)
    if (this.uncounted?.message.partitionId === partitionId) this.uncounted = undefined
    if (this.latestDelivered?.partitionId === partitionId) this.latestDelivered = undefined
    this.countedOffsets.delete(partitionId)
    this.consumedOffsets.delete(partitionId)
    this.nextOffsets.delete(partitionId)
    this.resumeAfterStored.delete(partitionId)
    this.explicitOffsets.delete(partitionId)
    this.deletedAt.delete(partitionId)
    // A store after the seek may move the offset back.
    if (storedBefore) this.storedOffsets.set(partitionId, start.value - 1n)
    else this.storedOffsets.delete(partitionId)
    if (this.target.kind === "group") this.selectsPartitions = true
  }

  /** The offset the server stores for this consumer, its group or its
   * name, on `partitionId`, with the partition head. `undefined` when nothing
   * is stored. */
  async storedOffset(
    partitionId: number,
    options: CallOptions = {}
  ): Promise<StoredOffset | undefined> {
    throwIfAborted(options.signal, "consumer stored offset aborted")
    if (this.transport.getConsumerOffset === undefined) {
      throw new UnsupportedError("the Apache Iggy client does not expose consumer offsets", {
        surface: "stream"
      })
    }
    const offsetTarget =
      this.target.kind === "group"
        ? { kind: "group" as const, name: this.target.name }
        : this.target.name === undefined
          ? undefined
          : { kind: "consumer" as const, name: this.target.name }
    if (offsetTarget === undefined) {
      throw new InvalidError("storedOffset() requires a named standalone consumer")
    }
    return abortable(
      this.transport.getConsumerOffset(this.streamName, this.topicName, offsetTarget, partitionId),
      options.signal,
      "consumer stored offset"
    )
  }

  /**
   * Ends iteration and leaves the consumer group when present. An
   * auto-committing consumer stores the delivered prefix first.
   */
  async shutdown(): Promise<void> {
    if (this.shuttingDown) return
    this.shuttingDown = true
    if (this.commitTimer !== undefined) {
      clearInterval(this.commitTimer)
      this.commitTimer = undefined
    }
    if (this.reader === undefined) {
      this.pollAbort.abort()
      await this.polling?.catch(() => undefined)
      this.polling = undefined
    }
    if (this.reader !== undefined) {
      try {
        this.reader.wake()
        if (this.deferred.length > 0) await this.polling?.catch(() => undefined)
        this.polling = undefined
        await this.finishDelivery()
      } finally {
        this.records.length = 0
        await this.reader.close()
      }
      return
    }
    try {
      if (this.autoCommit) {
        this.countDelivery()
        for (const [partition, counted] of this.countedOffsets) {
          const { offset } = counted
          const explicit = this.explicitOffsets.get(partition)
          if (this.deletedAt.get(partition) === offset) continue
          if (this.storedOffsets.get(partition) === offset) continue
          if (explicit === undefined || explicit < offset) {
            if (await this.storeCountedOffset(partition, counted)) {
              this.storedOffsets.set(partition, offset)
            }
          }
        }
      }
    } finally {
      if (this.leavesGroup && this.target.kind === "group") {
        await this.transport.leaveConsumerGroup(this.streamName, this.topicName, this.target.name)
      }
    }
  }

  /** Delegates async disposal to `shutdown()`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.shutdown()
  }

  // The next buffered record as the message to deliver. A group read marks
  // the prefix through it handled when the consumer commits on its own.
  private take(): ConsumerMessage | undefined {
    if (this.reader === undefined) {
      let message = this.returned.shift()
      if (message === undefined) {
        const polled = this.buffer.shift()
        if (polled === undefined) return undefined
        message = withJson(polled, polled.headersMalformed !== undefined)
        this.deliveries.set(
          message,
          polled.context === undefined ? {} : { context: polled.context }
        )
      }
      this.consumedOffsets.set(message.partitionId, message.position.offset)
      this.uncounted = {
        message,
        endsBatch: this.returned.length === 0 && this.buffer.length === 0
      }
      this.latestDelivered = message
      return message
    }
    let record = this.records.shift()
    while (record !== undefined && !this.reader.owns(record)) record = this.records.shift()
    if (record === undefined) return undefined
    if (this.autoCommit) this.lastDelivery = { record, lastOfPage: this.records.length === 0 }
    this.consumedOffsets.set(record.partitionId, record.offset)
    const message = record.message
    this.delivered.set(message, record)
    this.latestDelivered = message
    return message
  }

  // The delivery the caller held is handled now, like the Rust consumer under
  // `CommitAfter::Processing`. Reports whether its commit policy stores here.
  private countDelivery(): NativeDelivery | undefined {
    const delivery = this.uncounted
    this.uncounted = undefined
    if (delivery === undefined) return undefined
    const offset = delivery.message.position.offset
    const context = this.deliveries.get(delivery.message)?.context
    this.countedOffsets.set(
      delivery.message.partitionId,
      context === undefined ? { offset } : { offset, context }
    )
    this.yieldedSinceStore += 1
    return delivery
  }

  // The native commit policies that store handled records. `polling` stores
  // on the server during the poll itself, and a group reader stores its
  // handled prefix on its own. A failed store never costs the caller the
  // record: the offset stays pending, and the next handled record, timer
  // tick, or shutdown stores it.
  private async storeHandled(): Promise<void> {
    const delivery = this.countDelivery()
    if (delivery === undefined) return
    const policy = this.commitPolicy
    const store =
      policy.kind === "each" ||
      policy.kind === "intervalOrEach" ||
      ((policy.kind === "all" || policy.kind === "intervalOrAll") && delivery.endsBatch) ||
      ((policy.kind === "every" || policy.kind === "intervalOrEvery") &&
        this.yieldedSinceStore >= policy.count)
    if (!store) return
    try {
      await this.storeCounted(delivery.message.partitionId)
      this.yieldedSinceStore = 0
    } catch {
      // Stored on a later attempt.
    }
  }

  private async storeCounted(only?: number): Promise<void> {
    for (const [partition, counted] of this.countedOffsets) {
      if (only !== undefined && partition !== only) continue
      if ((this.storedOffsets.get(partition) ?? -1n) >= counted.offset) continue
      if (this.deletedAt.get(partition) === counted.offset) continue
      if (await this.storeCountedOffset(partition, counted)) {
        this.storedOffsets.set(partition, counted.offset)
      }
    }
  }

  // Stores a handled offset fenced to the history its record was read under.
  // A refusal for an older history or owner drops it: the partition's current
  // reader reads the record again. Reports whether it was stored.
  private async storeCountedOffset(partition: number, counted: Counted): Promise<boolean> {
    try {
      await this.transport.storeOffset(
        this.streamName,
        this.topicName,
        this.target,
        partition,
        counted.offset,
        counted.context
      )
      return true
    } catch (error) {
      const code = serverErrorCode(error)
      if (code === undefined || !STALE_DELIVERY.has(code)) throw error
      if (this.countedOffsets.get(partition) === counted) this.countedOffsets.delete(partition)
      return false
    }
  }

  // Stores an offset unless one at or above it is stored already, which only
  // `allowReplay` permits moving back.
  private async storeAt(
    offset: bigint,
    partition: number,
    context?: PartitionContext
  ): Promise<void> {
    const stored = this.storedOffsets.get(partition)
    if (!this.allowReplay && stored !== undefined && offset <= stored) return
    await this.transport.storeOffset(
      this.streamName,
      this.topicName,
      this.target,
      partition,
      offset,
      context
    )
    this.explicitOffsets.set(partition, offset)
    this.storedOffsets.set(partition, offset)
  }

  private startCommitTimer(): void {
    const interval = commitInterval(this.commitPolicy)
    if (interval === undefined || this.commitTimer !== undefined || this.reader !== undefined) {
      return
    }
    this.commitTimer = setInterval(() => {
      this.storeCounted().catch(() => undefined)
    }, interval)
    this.commitTimer.unref()
  }

  private requireNative(operation: string): void {
    if (this.reader !== undefined) {
      throw new InvalidError(
        `${operation}() is native only: a policy-aware group consumer commits delivered records`
      )
    }
  }

  private currentPartition(partitionId: number | undefined): number {
    const partition =
      partitionId ??
      (this.target.kind === "single" ? this.target.partitionId : undefined) ??
      this.latestDelivered?.partitionId
    if (partition === undefined || !Number.isSafeInteger(partition) || partition < 0) {
      throw new InvalidError("name a partition: this consumer has no current partition yet")
    }
    return partition
  }

  // One poll. A retryable transport failure waits the polling retry interval
  // and reports an empty poll, so the read loop keeps going after a server
  // restart, like the Rust consumer. The wait never runs past `deadline`.
  private async pollOnce(signal?: AbortSignal, deadline?: number): Promise<void> {
    this.startCommitTimer()
    try {
      if (this.polling === undefined) {
        const held = this.refusedUntil - Date.now()
        if (held > 0) {
          await delay(deadline === undefined ? held : Math.min(held, deadline - Date.now()), signal)
          if (Date.now() < this.refusedUntil) return
        }
      }
      this.polling ??= this.fillBuffer()
      await this.waitForPoll(this.polling, signal, deadline)
    } catch (error) {
      // A lasting refusal reaches the caller, also when the client reported
      // it as a read of unknown progress, and paces the reads after it.
      if (lastingRefusal(error) !== undefined && !this.stopped()) {
        this.refusedUntil = Date.now() + this.pollingRetryIntervalMs
        throw error
      }
      if (
        this.pollFailures === "propagate" ||
        !(error instanceof TransportError) ||
        !error.retryable ||
        this.stopped()
      ) {
        throw error
      }
      const wait =
        deadline === undefined
          ? this.pollingRetryIntervalMs
          : Math.min(this.pollingRetryIntervalMs, Math.max(0, deadline - Date.now()))
      await delay(wait, signal)
    }
  }

  private waitForPoll(poll: Promise<void>, signal?: AbortSignal, deadline?: number): Promise<void> {
    return new Promise((resolve, reject) => {
      let waiting = true
      let timer: ReturnType<typeof setTimeout> | undefined
      const finish = (complete: () => void, observed: boolean): void => {
        if (!waiting) return
        waiting = false
        clearTimeout(timer)
        signal?.removeEventListener("abort", onAbort)
        if (observed && this.polling === poll) this.polling = undefined
        complete()
      }
      const onAbort = (): void => {
        finish(() => {
          reject(new CancelledError("consumer poll aborted", { cause: signal?.reason }))
        }, false)
      }
      poll.then(
        () => {
          if (deadline !== undefined && Date.now() > deadline) {
            finish(() => {
              reject(new TimeoutError("the live consumer to yield a record"))
            }, true)
          } else {
            finish(resolve, true)
          }
        },
        (error: unknown) => {
          finish(() => {
            reject(
              error instanceof Error
                ? error
                : new TransportError("consumer poll failed", true, { cause: error })
            )
          }, true)
        }
      )
      signal?.addEventListener("abort", onAbort, { once: true })
      if (signal?.aborted === true) {
        onAbort()
      } else if (deadline !== undefined) {
        const tick = (): void => {
          if (!waiting) return
          const remaining = deadline - Date.now()
          if (remaining <= 0) {
            finish(() => {
              reject(new TimeoutError("the live consumer to yield a record"))
            }, false)
          } else {
            timer = setTimeout(tick, Math.min(remaining, 2_147_483_647))
          }
        }
        tick()
      }
    })
  }

  private pending(): boolean {
    return this.reader === undefined
      ? this.buffer.length > 0 || this.returned.length > 0
      : this.records.length > 0
  }

  private stopped(): boolean {
    return this.shuttingDown
  }

  // Asking for the next record or shutting down confirms that the previous
  // delivery reached the caller, and the commit policy decides whether the
  // handled prefix is stored now. A cancelled poll cannot complete a record
  // that the caller never received.
  private async finishDelivery(): Promise<void> {
    if (this.reader === undefined) {
      await this.storeHandled()
      return
    }
    const delivery = this.lastDelivery
    this.lastDelivery = undefined
    // A disabled policy completes only what the caller commits.
    if (delivery !== undefined && this.commitPolicy.kind !== "disabled") {
      this.reader.handled(delivery.record)
      this.yieldedSinceFlush += 1
      this.pendingCommit = { kind: "delivered", lastOfPage: delivery.lastOfPage }
    }
    const point = this.pendingCommit
    if (point !== undefined) {
      await this.flushIfDue(this.reader, point)
      this.pendingCommit = undefined
    }
    // A read in flight sends the queued acknowledgments before its next read.
    if (this.polling === undefined) await this.applyDeferred(this.reader)
  }

  // Acknowledge the records handled out of band, in the order they were
  // handled. A failed acknowledgment stays queued with the ones after it, so
  // the next attempt sends it again. A partition this member lost refuses its
  // acknowledgment, and the new owner reads those records again, so that one
  // is dropped.
  private async applyDeferred(reader: FilteredReader): Promise<void> {
    for (let record = this.deferred[0]; record !== undefined; record = this.deferred[0]) {
      try {
        await reader.ackThrough(record)
        this.explicitOffsets.set(record.partitionId, record.offset)
        this.storedOffsets.set(record.partitionId, record.offset)
      } catch (error) {
        if (!(
          error instanceof FilterExecutionError && error.detail?.reason === "membership_stale"
        )) {
          throw error
        }
      }
      this.deferred.shift()
    }
  }

  // Stores the handled prefix when the commit policy says so at this point.
  private async flushIfDue(reader: FilteredReader, point: CommitPoint): Promise<void> {
    if (
      !commitDue(this.commitPolicy, this.yieldedSinceFlush, Date.now() - this.lastFlushAt, point)
    ) {
      return
    }
    try {
      await reader.flushCompleted()
    } catch (error) {
      const reason = error instanceof FilterExecutionError ? error.detail?.reason : undefined
      if (reason !== "conflict" && reason !== "source_changed") throw error
      const owned = this.records.filter((record) => reader.owns(record))
      this.records.splice(0, this.records.length, ...owned)
      this.pendingCommit = undefined
      this.yieldedSinceFlush = 0
      // A policy change fences the prefix read under the old policy. Those
      // records are delivered again from the stored offset, so nothing is
      // lost and the consumer goes on without an error.
      if (reason !== "conflict") throw error
      this.lastFlushAt = Date.now()
      return
    }
    this.yieldedSinceFlush = 0
    this.lastFlushAt = Date.now()
  }

  // Store the seed of each owned partition the group still stored nothing on,
  // so its first read begins at the record the start named at the join.
  private async storeSeeds(group: string, partitions: readonly number[]): Promise<boolean> {
    const read = this.transport.getConsumerOffset?.bind(this.transport)
    if (read === undefined)
      throw new UnsupportedError("this transport cannot read a group's stored offsets", {
        surface: "stream"
      })
    const target = { kind: "group" as const, name: group }
    return storeSeeds(
      this.seeds,
      partitions,
      (partitionId) => read(this.streamName, this.topicName, target, partitionId),
      (partitionId, seed) =>
        this.transport.storeOffset(this.streamName, this.topicName, target, partitionId, seed)
    )
  }

  // One group poll. A policy that stores at the poll stores the delivered
  // prefix first, so a crash redelivers the current batch instead of
  // skipping it.
  private async fillFromGroup(reader: FilteredReader): Promise<void> {
    await this.applyDeferred(reader)
    await this.flushIfDue(reader, { kind: "poll" })
    const [page, more] = await reader.readRound()
    this.groupRead = true
    this.moreScanned = more
    if (page !== undefined) this.records.push(...page.records)
    else if (!more) {
      // An interval policy stores the handled prefix on time while the
      // partitions stay idle.
      const interval = commitInterval(this.commitPolicy)
      await reader.waitForRecordsWithin(
        interval === undefined ? undefined : Math.max(0, interval - (Date.now() - this.lastFlushAt))
      )
    }
  }

  private async fillBuffer(signal?: AbortSignal): Promise<void> {
    this.moreScanned = false
    if (signal?.aborted === true) {
      throw new CancelledError("consumer poll aborted", { cause: signal.reason })
    }
    if (this.reader !== undefined) {
      await this.fillFromGroup(this.reader)
      return
    }
    let target = this.target
    if (
      target.kind === "group" &&
      (this.startAt.kind !== "next" || !this.autoCommit || this.selectsPartitions) &&
      this.transport.syncConsumerGroup !== undefined
    ) {
      const assignment = await this.transport.syncConsumerGroup(
        this.streamName,
        this.topicName,
        target.name
      )
      if (assignment === undefined)
        throw new TransportError("the consumer group has no active assignment", true)
      if (assignment.rejoined) {
        this.nextOffsets.clear()
        this.soughtStarts.clear()
        this.assignedPartitions.clear()
      }
      const assigned = new Set(assignment.partitions)
      // A partition not yet installed for this member keeps its seed, and the
      // round reads nothing until a later sync stores it.
      if (this.seeds.size > 0 && !(await this.storeSeeds(target.name, assignment.partitions)))
        return
      for (const partition of this.assignedPartitions) {
        if (assigned.has(partition)) continue
        this.nextOffsets.delete(partition)
        this.soughtStarts.delete(partition)
      }
      this.assignedPartitions = assigned
      this.assignmentSeen = true
      if (assignment.partitions.length === 0) return
      const partitionId =
        assignment.partitions[this.partitionCursor++ % assignment.partitions.length]
      if (partitionId === undefined) return
      target = { ...target, partitionId }
    }
    const partition = target.partitionId
    const next = partition === undefined ? undefined : this.nextOffsets.get(partition)
    const sought = partition === undefined ? undefined : this.soughtStarts.get(partition)
    const strategy =
      sought ??
      (next !== undefined
        ? { kind: "offset" as const, value: next }
        : target.kind === "group" ||
            (partition !== undefined && this.resumeAfterStored.has(partition))
          ? { kind: "next" as const }
          : this.startAt)
    const seeksBefore = this.seeks
    const commitOnPoll =
      this.commitPolicy.kind === "polling" || this.commitPolicy.kind === "intervalOrPolling"
    // The consumer picks a group partition itself only on the synced path,
    // and then holds its reads after a whole idle pass. The transport does the
    // same when it picks the partition.
    const passes =
      target.kind === "group" && partition !== undefined ? this.assignedPartitions.size : 1
    let read: readonly PolledMessage[]
    try {
      read = await this.transport.pollMessages(
        this.streamName,
        this.topicName,
        target,
        strategy,
        this.batchLength,
        commitOnPoll,
        passWait(this.wait, passes, this.idleReads),
        this.pollAbort.signal
      )
    } catch (error) {
      const refusal = lastingRefusal(error)
      if (partition !== undefined && (resumesAfterStoredOffset(error) || refusal !== undefined)) {
        this.nextOffsets.delete(partition)
        this.resumeAfterStored.add(partition)
        // The records of a recreated stream or topic are a new history.
        if (refusal !== undefined && HISTORY_GONE.has(refusal)) {
          this.consumedOffsets.delete(partition)
          this.countedOffsets.delete(partition)
          if (this.uncounted?.message.partitionId === partition) this.uncounted = undefined
        }
      }
      if (refusal !== undefined) {
        const refused = (this.refusals.get(partition ?? -1) ?? 0) + 1
        if (refused < REFUSALS_PER_REPORT) {
          this.refusals.set(partition ?? -1, refused)
          this.refusedUntil = Date.now() + this.pollingRetryIntervalMs
          return
        }
        this.refusals.delete(partition ?? -1)
      }
      throw error
    }
    this.refusals.delete(partition ?? -1)
    // A partition sought while the poll was in flight reads from its sought
    // start next, so the records this poll brought for it are dropped.
    const polled =
      this.seeks === seeksBefore
        ? read
        : read.filter((message) => (this.soughtAt.get(message.partitionId) ?? 0) <= seeksBefore)
    this.idleReads = polled.length === 0 ? this.idleReads + 1 : 0
    for (const message of polled) {
      this.nextOffsets.set(message.partitionId, message.offset + 1n)
      this.resumeAfterStored.delete(message.partitionId)
      this.soughtStarts.delete(message.partitionId)
    }
    const fresh = this.allowReplay
      ? polled
      : polled.filter((message) => {
          const consumed = this.consumedOffsets.get(message.partitionId)
          return consumed === undefined || message.offset > consumed
        })
    if (commitOnPoll) {
      for (const message of fresh) this.storedOffsets.set(message.partitionId, message.offset)
    }
    this.buffer.push(...fresh)
  }
}

// The stored header fields a record can carry beyond its position.
interface StoredHeader {
  readonly offset: bigint
  readonly timestampMicros?: bigint
  readonly messageId?: bigint
  readonly checksum?: bigint
  readonly originTimestampMicros?: bigint
  readonly currentOffset?: bigint
  readonly userHeaders?: Uint8Array
}

function withJson(
  message: Omit<Message, "id" | "json"> & { readonly partitionId: number } & StoredHeader,
  headersMalformed: boolean
): ConsumerMessage {
  const json = <T = unknown>(decodeValue?: (value: unknown) => T): T =>
    new Json<T>(decodeValue).decode(message.payload)
  const position = { partitionId: message.partitionId, offset: message.offset }
  const delivered = {
    payload: message.payload,
    id: position,
    partitionId: message.partitionId,
    headers: message.headers,
    position,
    messageId: message.messageId ?? 0n,
    checksum: message.checksum ?? 0n,
    currentOffset: message.currentOffset ?? 0n,
    timestampMicros: message.timestampMicros ?? 0n,
    originTimestampMicros: message.originTimestampMicros ?? 0n,
    ...(message.userHeaders !== undefined ? { userHeaders: message.userHeaders } : {}),
    headersMalformed
  }
  // Not enumerable, so a delivered message still compares equal to its plain
  // fields.
  return Object.defineProperty(delivered, "json", { value: json }) as ConsumerMessage
}

// A native delivery the caller holds, and whether it ended its poll.
interface NativeDelivery {
  readonly message: ConsumerMessage
  readonly endsBatch: boolean
}

// The partition history a native delivery was read under, absent when the
// transport reported none.
interface DeliveredUnder {
  readonly context?: PartitionContext
}

// A handled offset a commit policy stores, with the history of its record.
interface Counted extends DeliveredUnder {
  readonly offset: bigint
}

/** When the group engine asks whether to store the handled prefix: a record
 * was delivered, `lastOfPage` when it ended its poll, or the next poll is
 * about to start. */
type CommitPoint =
  { readonly kind: "delivered"; readonly lastOfPage: boolean } | { readonly kind: "poll" }

// Whether an automatic commit policy stores the handled prefix now, like the
// Rust `commit_due`. `yielded` counts records delivered since the last store
// and `elapsedMs` the time since it.
function commitDue(
  policy: CommitPolicy,
  yielded: number,
  elapsedMs: number,
  point: CommitPoint
): boolean {
  const polling = point.kind === "poll" && yielded > 0
  const lastOfPage = point.kind === "delivered" && point.lastOfPage
  const delivered = point.kind === "delivered"
  switch (policy.kind) {
    case "disabled":
      return false
    case "polling":
      return polling
    case "interval":
      return elapsedMs >= policy.intervalMs
    case "intervalOrPolling":
      return polling || elapsedMs >= policy.intervalMs
    case "all":
      return lastOfPage
    case "intervalOrAll":
      return lastOfPage || elapsedMs >= policy.intervalMs
    case "each":
    case "intervalOrEach":
      return delivered && yielded > 0
    case "every":
      return delivered && yielded >= policy.count
    case "intervalOrEvery":
      return (delivered && yielded >= policy.count) || elapsedMs >= policy.intervalMs
  }
}

// The lasting refusal code anywhere in the cause chain of a failed poll.
function lastingRefusal(error: unknown): number | undefined {
  let current = error
  for (let depth = 0; depth < 8; depth += 1) {
    if (typeof current !== "object" || current === null) return undefined
    const code = (current as { readonly errorCode?: unknown }).errorCode
    if (typeof code === "number" && LASTING_REFUSALS.has(code)) return code
    current = "cause" in current ? (current as { readonly cause?: unknown }).cause : undefined
  }
  return undefined
}

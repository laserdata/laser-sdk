import { type BytesLike, ownedBytes } from "../client/bytes.js"
import { InvalidError, PublishFailedError, TimeoutError, TransportError } from "../client/errors.js"
import {
  type LaserTransport,
  type MessageWithHeaders,
  NEVER_EXPIRE,
  type SendMessagesConfirmation,
  type SendMessagesResponse,
  UNLIMITED_TOPIC_SIZE,
  withMessageIds,
  xxHash32
} from "../iggy/apache-iggy.js"
import type { HeaderValue } from "./header-value.js"
import type { Headers } from "./message.js"
import { ProducerRecorder } from "./producer-statistics.js"
import type { Routing } from "./routing.js"

export interface ProducerOptions {
  readonly routing?: Routing
  /** Resend attempts after a failed publish. Defaults to the connection's
   * publish retries. */
  readonly retries?: number
  /** First retry delay, doubled on each later attempt up to 30 seconds. Must
   * be positive. Defaults to the connection's publish retry backoff. */
  readonly retryBackoffMs?: number
  /** Create the stream before the first send when it does not exist.
   * Defaults to `true`, like Rust and Python. */
  readonly createStream?: boolean
  /** Create the topic before the first send when it does not exist.
   * Defaults to `true`, like Rust and Python. */
  readonly createTopic?: boolean
  /** Partition count of a topic this producer creates. Defaults to 1. */
  readonly partitions?: number
  /** Message expiry of a topic this producer creates, in microseconds. Leave
   * it out for the server default. */
  readonly expireAfterMicros?: bigint
  /** Create the topic with messages that never expire. Set this or
   * `expireAfterMicros`, not both. */
  readonly neverExpire?: boolean
  /** Most messages one direct request carries. A larger batch is split into
   * consecutive requests of this size, each awaited before the next one.
   * Defaults to 1000, like Rust and Python. Ignored in background mode. */
  readonly batchLength?: number
  /** Minimum gap between sequential direct sends, in milliseconds. A send
   * first waits out what is left of it since the previous send. Defaults to
   * 0. Ignored in background mode. */
  readonly lingerMs?: number
  /** Maximum size of a topic this producer creates, in bytes. Leave it out
   * for the server default. */
  readonly maxTopicBytes?: bigint
  /** Create the topic without a size limit. Overrides `maxTopicBytes`. */
  readonly unlimitedTopicSize?: boolean
  /** Buffered background mode instead of the default direct mode. A send
   * returns once its records are queued, with no confirmations. Call
   * `shutdown()` to wait for the writes. */
  readonly background?: ProducerBackgroundOptions
}

/** Background mode, mirroring Apache Iggy's `BackgroundConfig` defaults.
 * Each worker queues the sends routed to it and flushes when any of its
 * limits is reached. */
export interface ProducerBackgroundOptions {
  /** Worker queues. Defaults to 1, and `0` means one. */
  readonly shards?: number
  /** How a send picks its worker. `ordered`, the default, keeps every send
   * of this producer on one worker in send order. `balanced` deals sends out
   * round-robin across the workers and gives up that order. */
  readonly sharding?: "ordered" | "balanced"
  /** Sends queued on one worker that trigger its flush. Defaults to 1000.
   * `0` disables it. */
  readonly batchLength?: number
  /** Payload bytes queued on one worker that trigger its flush. Defaults to
   * 1 MiB. `0` disables it. */
  readonly batchBytes?: number
  /** Longest time a non-empty worker queue waits before a flush, in
   * milliseconds. Defaults to 1. */
  readonly lingerMs?: number
  /** Payload bytes queued or in flight across all workers. Defaults to
   * 32 MiB. `0` means unlimited. */
  readonly maxBufferBytes?: number
  /** What a send does once `maxBufferBytes` is exhausted. Defaults to
   * `block`, which waits for room. `blockWithTimeout` waits at most
   * `timeoutMs`, then throws `PublishFailedError` caused by `TimeoutError`.
   * `failImmediately` throws `PublishFailedError` at once. A send that throws
   * is never queued. */
  readonly failureMode?:
    | { readonly kind: "block" }
    | { readonly kind: "blockWithTimeout"; readonly timeoutMs: number }
    | { readonly kind: "failImmediately" }
  /** Writes in flight at once across all workers. Each worker still writes
   * in order. Defaults to 1, and `0` lifts the limit. */
  readonly maxInFlight?: number
  /** Receives each failure after retries. A returned promise is awaited. Without a callback, failed writes reject `shutdown()`. */
  readonly onError?: (error: PublishFailedError) => void | Promise<void>
}

type BackgroundFailureMode = NonNullable<ProducerBackgroundOptions["failureMode"]>

export interface ProducerSendOptions {
  readonly key?: Uint8Array
  readonly partition?: number
  readonly headers?: ReadonlyMap<string, HeaderValue> | Readonly<Record<string, HeaderValue>>
}

/** A raw streaming record with optional exact-width user headers. */
export class ProducerMessage {
  readonly payload: Uint8Array
  private headerValues = new Map<string, HeaderValue>()

  /** A record without user headers. */
  constructor(payload: BytesLike) {
    this.payload = ownedBytes(payload)
  }

  get headers(): Headers {
    return new Map(this.headerValues)
  }

  /** Replace all user headers. */
  withHeaders(headers: Headers): this {
    this.headerValues = new Map(headers)
    return this
  }

  /** Add or replace one user header. */
  header(key: string, value: HeaderValue): this {
    this.headerValues.set(key, value)
    return this
  }
}

const DEFAULT_RETRIES = 3
const DEFAULT_RETRY_BACKOFF_MS = 250
const MAX_RETRY_DELAY_MS = 30_000
const MAX_TIMER_MS = 0x7fff_ffff
const DEFAULT_BATCH_LENGTH = 1_000
const MAX_KEY_BYTES = 255
const MIB = 1024 * 1024

interface QueuedSend {
  readonly messages: readonly MessageWithHeaders[]
  readonly routing: Routing
  readonly bytes: number
}

interface BackgroundSettings {
  readonly shards: number
  readonly sharding: "ordered" | "balanced"
  readonly batchLength: number
  readonly batchBytes: number
  readonly lingerMs: number
  readonly maxBufferBytes: number
  readonly failureMode: BackgroundFailureMode
  readonly maxInFlight: number
}

interface Shard {
  readonly queue: QueuedSend[]
  queuedBytes: number
  timer: ReturnType<typeof setTimeout> | undefined
  draining: Promise<void> | undefined
}

function payloadBytes(messages: readonly MessageWithHeaders[]): number {
  return messages.reduce((total, message) => total + message.payload.byteLength, 0)
}

function sameRouting(left: Routing, right: Routing): boolean {
  if (left.kind !== right.kind) return false
  if (left.kind === "partition" && right.kind === "partition")
    return left.partition === right.partition
  if (left.kind === "key" && right.kind === "key")
    return (
      left.key.length === right.key.length && left.key.every((byte, i) => byte === right.key[i])
    )
  return true
}

function nonNegative(value: number, name: string): number {
  if (!Number.isFinite(value) || value < 0)
    throw new InvalidError(`${name} must be a non-negative finite number`)
  return value
}

function headersMap(headers: ProducerSendOptions["headers"]): ReadonlyMap<string, HeaderValue> {
  if (headers === undefined) return new Map()
  return headers instanceof Map ? new Map(headers) : new Map(Object.entries(headers))
}

function optionRouting(options: ProducerSendOptions, fallback: Routing): Routing {
  if (options.key !== undefined && options.partition !== undefined) {
    throw new InvalidError("send() accepts a routing key or an explicit partition, not both")
  }
  if (options.partition !== undefined) return { kind: "partition", partition: options.partition }
  if (options.key !== undefined) return { kind: "key", key: routingKey(options.key) }
  return fallback
}

// A routing key holds 1 to 255 bytes, the range Apache Iggy accepts.
function routingKey(key: Uint8Array): Uint8Array {
  if (key.byteLength === 0 || key.byteLength > MAX_KEY_BYTES) {
    throw new InvalidError("a routing key must hold 1 to 255 bytes")
  }
  return key.slice()
}

function positiveDelay(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 1 || value > MAX_TIMER_MS) {
    throw new InvalidError(`${name} must be an integer between 1 and 2147483647 milliseconds`)
  }
  return value
}

function backgroundSettings(background: ProducerBackgroundOptions): BackgroundSettings {
  const count = (value: number | undefined, fallback: number, name: string): number => {
    const resolved = value ?? fallback
    if (!Number.isSafeInteger(resolved) || resolved < 0) {
      throw new InvalidError(`background ${name} must be a non-negative safe integer`)
    }
    return resolved
  }
  const failureMode = background.failureMode ?? { kind: "block" }
  if (failureMode.kind === "blockWithTimeout") {
    positiveDelay(failureMode.timeoutMs, "background failure timeoutMs")
  }
  return {
    shards: Math.max(1, count(background.shards, 1, "shards")),
    sharding: background.sharding ?? "ordered",
    batchLength: count(background.batchLength, 1_000, "batchLength"),
    batchBytes: count(background.batchBytes, MIB, "batchBytes"),
    lingerMs: nonNegative(background.lingerMs ?? 1, "background lingerMs"),
    maxBufferBytes: count(background.maxBufferBytes, 32 * MIB, "maxBufferBytes"),
    failureMode,
    maxInFlight: count(background.maxInFlight, 1, "maxInFlight") || Number.POSITIVE_INFINITY
  }
}

function lowerMessage(message: ProducerMessage): MessageWithHeaders {
  return { payload: message.payload, headers: message.headers }
}

/** Publishes directly with bounded retries and explicit routing. Unless told
 * otherwise, it creates its stream and topic before the first send. */
export class Producer implements AsyncDisposable {
  private readonly routing: Routing
  private readonly retries: number
  private readonly retryBackoffMs: number
  private closed = false
  private closing: Promise<void> | undefined
  private readonly statistics: ProducerRecorder
  private provisioned: Promise<void> | undefined
  private readonly batchLength: number
  private readonly lingerMs: number
  private lastSentAt = 0
  private readonly background: BackgroundSettings | undefined
  private readonly onError: ((error: PublishFailedError) => void | Promise<void>) | undefined
  private readonly shards: Shard[] = []
  private nextShard = 0
  private bufferedBytes = 0
  private writing = 0
  private readonly writeWaiters: (() => void)[] = []
  private backgroundFailure: PublishFailedError | undefined
  private readonly roomWaiters: (() => void)[] = []

  private constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    private readonly topicName: string,
    private readonly options: ProducerOptions = {}
  ) {
    this.statistics = new ProducerRecorder(streamName, topicName, transport)
    this.routing =
      options.routing?.kind === "key"
        ? { kind: "key", key: routingKey(options.routing.key) }
        : (options.routing ?? { kind: "balanced" })
    this.retries = options.retries ?? DEFAULT_RETRIES
    this.retryBackoffMs =
      options.retryBackoffMs === undefined
        ? DEFAULT_RETRY_BACKOFF_MS
        : positiveDelay(options.retryBackoffMs, "producer retryBackoffMs")
    if (!Number.isSafeInteger(this.retries) || this.retries < 0) {
      throw new InvalidError("producer retries must be a non-negative safe integer")
    }
    if (this.routing.kind === "partition" && this.routing.partition < 0) {
      throw new InvalidError("producer partition must be non-negative")
    }
    const partitions = options.partitions ?? 1
    if (!Number.isSafeInteger(partitions) || partitions < 1) {
      throw new InvalidError("producer partitions must be a positive safe integer")
    }
    this.batchLength = options.batchLength ?? DEFAULT_BATCH_LENGTH
    if (!Number.isSafeInteger(this.batchLength) || this.batchLength < 1) {
      throw new InvalidError("producer batch length must be greater than zero")
    }
    this.lingerMs = nonNegative(options.lingerMs ?? 0, "producer lingerMs")
    if (options.neverExpire === true && options.expireAfterMicros !== undefined) {
      throw new InvalidError("producer takes neverExpire or expireAfterMicros, not both")
    }
    if (options.expireAfterMicros !== undefined && options.expireAfterMicros <= 0n) {
      throw new InvalidError("producer expireAfterMicros must be greater than zero")
    }
    if (options.maxTopicBytes !== undefined && options.maxTopicBytes <= 0n) {
      throw new InvalidError("producer maxTopicBytes must be greater than zero")
    }
    const background = options.background
    this.onError = background?.onError
    this.background = background === undefined ? undefined : backgroundSettings(background)
    for (let shard = 0; shard < (this.background?.shards ?? 0); shard += 1) {
      this.shards.push({ queue: [], queuedBytes: 0, timer: undefined, draining: undefined })
    }
  }

  /** @internal */
  static create(
    transport: LaserTransport,
    streamName: string,
    topicName: string,
    options: ProducerOptions = {}
  ): Producer {
    return new Producer(transport, streamName, topicName, options)
  }

  /** @internal True when this producer queues sends in background mode. */
  get isBackground(): boolean {
    return this.background !== undefined
  }

  // Creates the stream and topic once, before the first send, when the
  // options ask for it.
  private provision(): Promise<void> {
    this.provisioned ??= (async () => {
      const createStream = this.options.createStream ?? true
      const createTopic = this.options.createTopic ?? true
      if (createStream) await this.transport.ensureStream(this.streamName)
      if (!createTopic) return
      const partitions = this.options.partitions ?? 1
      const expiry =
        this.options.neverExpire === true ? NEVER_EXPIRE : this.options.expireAfterMicros
      const maxTopicSize =
        this.options.unlimitedTopicSize === true ? UNLIMITED_TOPIC_SIZE : this.options.maxTopicBytes
      if (this.transport.createTopicIfAbsent !== undefined) {
        await this.transport.createTopicIfAbsent(this.streamName, this.topicName, partitions, {
          ...(maxTopicSize === undefined ? {} : { maxTopicSize }),
          ...(expiry === undefined ? {} : { messageExpiryMicros: expiry })
        })
        return
      }
      if (maxTopicSize !== undefined || expiry !== undefined)
        throw new InvalidError("this transport cannot set topic provisioning settings")
      await this.transport.ensureTopic(this.streamName, this.topicName, partitions)
    })().catch((error: unknown) => {
      this.provisioned = undefined
      throw error
    })
    return this.provisioned
  }

  async send(payload: BytesLike, options: ProducerSendOptions = {}): Promise<SendMessagesResponse> {
    return this.sendMessage(
      new ProducerMessage(payload).withHeaders(headersMap(options.headers)),
      optionRouting(options, this.routing)
    )
  }

  async sendMessage(
    message: ProducerMessage,
    routing: Routing = this.routing
  ): Promise<SendMessagesResponse> {
    this.throwIfClosed("sendMessage")
    return this.sendWithRetry([lowerMessage(message)], routing)
  }

  async sendWithRouting(message: ProducerMessage, routing: Routing): Promise<SendMessagesResponse> {
    return this.sendMessage(
      message,
      routing.kind === "key" ? { kind: "key", key: routingKey(routing.key) } : routing
    )
  }

  async sendKeyed(message: ProducerMessage, key: BytesLike): Promise<SendMessagesResponse> {
    return this.sendMessage(message, { kind: "key", key: routingKey(ownedBytes(key)) })
  }

  async sendToPartition(
    message: ProducerMessage,
    partition: number
  ): Promise<SendMessagesResponse> {
    if (!Number.isSafeInteger(partition) || partition < 0) {
      throw new InvalidError("partition must be a non-negative safe integer")
    }
    return this.sendMessage(message, { kind: "partition", partition })
  }

  async sendBatch(
    messages: readonly (BytesLike | ProducerMessage)[],
    options: ProducerSendOptions = {}
  ): Promise<SendMessagesResponse> {
    const lowered = messages.map((message) =>
      message instanceof ProducerMessage
        ? lowerMessage(message)
        : { payload: ownedBytes(message), headers: headersMap(options.headers) }
    )
    return this.sendLoweredBatch(lowered, optionRouting(options, this.routing))
  }

  async sendBatchWithRouting(
    messages: readonly ProducerMessage[],
    routing: Routing = this.routing
  ): Promise<SendMessagesResponse> {
    return this.sendLoweredBatch(
      messages.map(lowerMessage),
      routing.kind === "key" ? { kind: "key", key: routingKey(routing.key) } : routing
    )
  }

  private async sendLoweredBatch(
    messages: readonly MessageWithHeaders[],
    routing: Routing
  ): Promise<SendMessagesResponse> {
    this.throwIfClosed("sendBatch")
    if (messages.length === 0) return { confirmations: [] }
    return this.sendWithRetry(messages, routing)
  }

  /** @internal Waits until every queued background send is written. A
   * direct producer has nothing to flush. Rejects with the unreported
   * background failures as one `PublishFailedError` that lists the records
   * of every failed batch. */
  async flush(): Promise<void> {
    if (this.closed) throw new InvalidError("flush() called after shutdown()")
    await this.drainQueue()
    this.throwBackgroundFailure()
  }

  /** Flushes queued background sends, then rejects future sends. Safe to
   * call more than once: a later call waits for the first one to finish, and
   * the first call reports an unreported background failure. */
  async shutdown(): Promise<void> {
    if (this.closing !== undefined) {
      await this.closing.catch(() => undefined)
      return
    }
    this.closed = true
    this.closing = (async () => {
      for (const wake of this.roomWaiters.splice(0)) wake()
      try {
        await this.drainQueue()
      } finally {
        this.statistics.closed = true
      }
      this.throwBackgroundFailure()
    })()
    return this.closing
  }

  /** Delegates async disposal to `shutdown()`. */
  [Symbol.asyncDispose](): Promise<void> {
    return this.shutdown()
  }

  private throwIfClosed(operation: string): void {
    if (this.closed) throw new InvalidError(`${operation}() called after shutdown()`)
  }

  private async sendWithRetry(
    messages: readonly MessageWithHeaders[],
    routing: Routing
  ): Promise<SendMessagesResponse> {
    if (this.background !== undefined) return this.enqueue(messages, routing, this.background)
    await this.waitForLinger()
    return this.writeChunks(messages, routing)
  }

  // Waits out what is left of the linger gap since the previous direct send.
  private async waitForLinger(): Promise<void> {
    const now = Date.now()
    const wait = this.lingerMs - (now - this.lastSentAt)
    if (this.lingerMs > 0 && this.lastSentAt > 0 && wait > 0) {
      await new Promise((resolve) => setTimeout(resolve, wait))
    }
    this.lastSentAt = Date.now()
  }

  // Splits the batch into `batchLength` requests awaited one after another.
  // Every record gets its id first, so a failure reports the confirmed prefix
  // and every record from the failed request on with the ids its attempts used.
  private async writeChunks(
    batch: readonly MessageWithHeaders[],
    routing: Routing
  ): Promise<SendMessagesResponse> {
    const messages = withMessageIds(batch)
    const finish = this.statistics.begin(messages.length, payloadBytes(messages))
    const confirmations: SendMessagesConfirmation[] = []
    let start = 0
    try {
      await this.provision()
      for (; start < messages.length; start += this.batchLength) {
        const chunk = messages.slice(start, start + this.batchLength)
        const response = await this.sendAttempts(chunk, routing)
        confirmations.push(...response.confirmations)
      }
      finish(true)
      return { confirmations }
    } catch (error) {
      finish(false)
      throw new PublishFailedError(
        this.streamName,
        this.topicName,
        error instanceof PublishFailedError
          ? [...confirmations, ...error.committed]
          : confirmations,
        error instanceof PublishFailedError
          ? [...error.unconfirmed, ...messages.slice(start + this.batchLength)]
          : messages.slice(start),
        error instanceof PublishFailedError ? error.publishCause() : error
      )
    }
  }

  private async enqueue(
    messages: readonly MessageWithHeaders[],
    routing: Routing,
    background: BackgroundSettings
  ): Promise<SendMessagesResponse> {
    await this.provision()
    this.throwIfClosed("send")
    const bytes = payloadBytes(messages)
    await this.waitForRoom(messages, bytes, background)
    const shard = this.pickShard(background)
    shard.queue.push({ messages, routing, bytes })
    shard.queuedBytes += bytes
    this.bufferedBytes += bytes
    const full =
      (background.batchLength > 0 && shard.queue.length >= background.batchLength) ||
      (background.batchBytes > 0 && shard.queuedBytes >= background.batchBytes)
    if (full || background.lingerMs === 0) {
      void this.drainShard(shard)
    } else {
      shard.timer ??= setTimeout(() => {
        shard.timer = undefined
        void this.drainShard(shard)
      }, background.lingerMs)
      shard.timer.unref()
    }
    return { confirmations: [] }
  }

  // Holds a send until the buffer has room for it, as the failure mode says.
  // Bytes stay charged until their write completes. A send larger than the
  // whole buffer waits for it to empty.
  private async waitForRoom(
    messages: readonly MessageWithHeaders[],
    bytes: number,
    background: BackgroundSettings
  ): Promise<void> {
    const mode = background.failureMode
    const deadline = mode.kind === "blockWithTimeout" ? Date.now() + mode.timeoutMs : undefined
    while (
      background.maxBufferBytes > 0 &&
      this.bufferedBytes > 0 &&
      this.bufferedBytes + bytes > background.maxBufferBytes
    ) {
      if (mode.kind === "failImmediately") {
        throw new PublishFailedError(
          this.streamName,
          this.topicName,
          [],
          messages,
          new TransportError("the background send buffer is full", false)
        )
      }
      const woken = await this.roomOrDeadline(deadline)
      this.throwIfClosed("send")
      if (!woken) {
        throw new PublishFailedError(
          this.streamName,
          this.topicName,
          [],
          messages,
          new TimeoutError("room in the background send buffer")
        )
      }
    }
  }

  private roomOrDeadline(deadline: number | undefined): Promise<boolean> {
    return new Promise((resolve) => {
      let timer: ReturnType<typeof setTimeout> | undefined
      const wake = (): void => {
        clearTimeout(timer)
        resolve(true)
      }
      this.roomWaiters.push(wake)
      if (deadline !== undefined) {
        timer = setTimeout(
          () => {
            const index = this.roomWaiters.indexOf(wake)
            if (index !== -1) this.roomWaiters.splice(index, 1)
            resolve(false)
          },
          Math.max(0, deadline - Date.now())
        )
      }
    })
  }

  // Ordered sharding keeps this producer's one destination on one worker, as
  // Apache Iggy's ordered sharding hashes the stream and topic. Balanced
  // sharding deals sends out round-robin.
  private pickShard(background: BackgroundSettings): Shard {
    const index =
      background.sharding === "balanced"
        ? this.nextShard++ % background.shards
        : xxHash32(new TextEncoder().encode(`${this.streamName}\0${this.topicName}`)) %
          background.shards
    const shard = this.shards[index]
    if (shard === undefined) throw new InvalidError("background shard is out of range")
    return shard
  }

  // Writes until every worker queue is empty and no write is in flight.
  private async drainQueue(): Promise<void> {
    while (this.shards.some((shard) => shard.queue.length > 0 || shard.draining !== undefined)) {
      await Promise.all(this.shards.map((shard) => this.drainShard(shard)))
    }
  }

  // Writes one worker's queue in order, merging adjacent sends that share
  // routing. Writes across workers are bounded by `maxInFlight`.
  private drainShard(shard: Shard): Promise<void> {
    if (shard.timer !== undefined) {
      clearTimeout(shard.timer)
      shard.timer = undefined
    }
    shard.draining ??= Promise.resolve().then(async () => {
      try {
        while (shard.queue.length > 0) {
          const first = shard.queue.shift()
          if (first === undefined) break
          const merged = [...first.messages]
          let bytes = first.bytes
          while (
            shard.queue[0] !== undefined &&
            sameRouting(shard.queue[0].routing, first.routing)
          ) {
            const next = shard.queue.shift()
            if (next === undefined) break
            merged.push(...next.messages)
            bytes += next.bytes
          }
          shard.queuedBytes -= bytes
          await this.acquireWrite()
          try {
            await this.writeChunks(merged, first.routing)
          } catch (error) {
            await this.reportBackgroundFailure(error)
          } finally {
            this.releaseWrite()
            this.bufferedBytes -= bytes
            for (const wake of this.roomWaiters.splice(0)) wake()
          }
        }
      } finally {
        shard.draining = undefined
      }
    })
    return shard.draining
  }

  private async acquireWrite(): Promise<void> {
    const limit = this.background?.maxInFlight ?? 1
    while (this.writing >= limit) {
      await new Promise<void>((resolve) => this.writeWaiters.push(resolve))
    }
    this.writing += 1
  }

  private releaseWrite(): void {
    this.writing -= 1
    this.writeWaiters.shift()?.()
  }

  private async reportBackgroundFailure(error: unknown): Promise<void> {
    const failure =
      error instanceof PublishFailedError
        ? error
        : new PublishFailedError(this.streamName, this.topicName, [], [], error)
    if (this.onError !== undefined) {
      try {
        await this.onError(failure)
        return
      } catch {
        // Preserve the failed records if the notification callback fails too.
      }
    }
    const earlier = this.backgroundFailure
    this.backgroundFailure =
      earlier === undefined
        ? failure
        : new PublishFailedError(
            this.streamName,
            this.topicName,
            [...earlier.committed, ...failure.committed],
            [...earlier.unconfirmed, ...failure.unconfirmed],
            earlier.cause
          )
  }

  private throwBackgroundFailure(): void {
    const failure = this.backgroundFailure
    if (failure === undefined) return
    this.backgroundFailure = undefined
    throw failure
  }

  private async sendAttempts(
    messages: readonly MessageWithHeaders[],
    routing: Routing
  ): Promise<SendMessagesResponse> {
    if (this.transport.publishRetriesManaged === true) {
      return this.transport.sendMessagesWithHeaders(
        this.streamName,
        this.topicName,
        messages,
        routing.kind === "key" ? routing.key : undefined,
        routing.kind === "partition" ? routing.partition : undefined,
        {
          batchLength: this.batchLength,
          ...(this.options.retries === undefined ? {} : { maxRetries: this.retries }),
          ...(this.options.retryBackoffMs === undefined
            ? {}
            : { retryBackoffMs: this.retryBackoffMs })
        }
      )
    }
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await this.transport.sendMessagesWithHeaders(
          this.streamName,
          this.topicName,
          messages,
          routing.kind === "key" ? routing.key : undefined,
          routing.kind === "partition" ? routing.partition : undefined
        )
      } catch (error) {
        if (!(error instanceof TransportError) || !error.retryable || attempt >= this.retries) {
          throw error
        }
        await new Promise((resolve) =>
          setTimeout(
            resolve,
            Math.min(this.retryBackoffMs * 2 ** Math.min(attempt, 16), MAX_RETRY_DELAY_MS)
          )
        )
      }
    }
  }
}

import { type BytesLike, ownedBytes } from "../client/bytes.js"
import { InvalidError, PublishFailedError, TransportError } from "../client/errors.js"
import {
  type IggyHeaderValue,
  type LaserTransport,
  type MessageWithHeaders,
  type SendMessagesConfirmation,
  type SendMessagesResponse,
  UNLIMITED_TOPIC_SIZE
} from "../iggy/apache-iggy.js"
import { ProducerRecorder } from "./producer-statistics.js"
import type { Routing } from "./routing.js"

export interface ProducerOptions {
  readonly routing?: Routing
  /** Resend attempts after a failed publish. Defaults to the connection's
   * publish retries. */
  readonly retries?: number
  /** First retry delay. Defaults to the connection's publish retry backoff. */
  readonly retryIntervalMs?: number
  /** Create the stream before the first send when it does not exist.
   * Defaults to `true`, like Rust and Python. */
  readonly createStream?: boolean
  /** Create the topic before the first send when it does not exist.
   * Defaults to `true`, like Rust and Python. */
  readonly createTopic?: boolean
  /** Partition count of a topic this producer creates. Defaults to 1. */
  readonly partitions?: number
  /** Message expiry of a topic this producer creates, in microseconds. */
  readonly messageExpiryMicros?: bigint
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
   * `flush()` or `shutdown()` to wait for the writes. */
  readonly background?: ProducerBackgroundOptions
}

/** Background mode, mirroring Apache Iggy's `BackgroundConfig` defaults. One
 * ordered worker per producer flushes when any limit is reached. */
export interface ProducerBackgroundOptions {
  /** Queued sends that trigger a flush. Defaults to 1000. `0` disables it. */
  readonly batchLength?: number
  /** Queued payload bytes that trigger a flush. Defaults to 1 MiB. `0`
   * disables it. */
  readonly batchBytes?: number
  /** Longest time a non-empty queue waits before a flush, in milliseconds.
   * Defaults to 1. */
  readonly lingerMs?: number
  /** Payload bytes the queue may hold. A send waits for room once it is
   * full. Defaults to 32 MiB. `0` means unlimited. */
  readonly maxBufferBytes?: number
  /** Receives each failure after retries. A returned promise is awaited. Without a callback, failed writes reject the next `flush()` or `shutdown()`. */
  readonly onError?: (error: PublishFailedError) => void | Promise<void>
}

export interface ProducerSendOptions {
  readonly key?: Uint8Array
  readonly partition?: number
  readonly headers?:
    ReadonlyMap<string, IggyHeaderValue> | Readonly<Record<string, IggyHeaderValue>>
}

export interface ProducerMessage {
  readonly payload: BytesLike
  readonly headers?:
    ReadonlyMap<string, IggyHeaderValue> | Readonly<Record<string, IggyHeaderValue>>
}

const DEFAULT_RETRIES = 3
const DEFAULT_RETRY_INTERVAL_MS = 1_000
const DEFAULT_BATCH_LENGTH = 1_000
const MIB = 1024 * 1024

interface QueuedSend {
  readonly messages: readonly MessageWithHeaders[]
  readonly routing: Routing
  readonly bytes: number
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

function headersMap(headers: ProducerMessage["headers"]): ReadonlyMap<string, IggyHeaderValue> {
  if (headers === undefined) return new Map()
  return headers instanceof Map ? new Map(headers) : new Map(Object.entries(headers))
}

function optionRouting(options: ProducerSendOptions, fallback: Routing): Routing {
  if (options.key !== undefined && options.partition !== undefined) {
    throw new InvalidError("send() accepts a routing key or an explicit partition, not both")
  }
  if (options.partition !== undefined) return { kind: "partition", partition: options.partition }
  if (options.key !== undefined) return { kind: "key", key: options.key.slice() }
  return fallback
}

function lowerMessage(message: ProducerMessage): MessageWithHeaders {
  return { payload: ownedBytes(message.payload), headers: headersMap(message.headers) }
}

function isProducerMessage(value: BytesLike | ProducerMessage): value is ProducerMessage {
  return "payload" in value
}

/** Publishes directly with bounded retries and explicit routing. Unless told
 * otherwise, it creates its stream and topic before the first send. */
export class Producer implements AsyncDisposable {
  private readonly routing: Routing
  private readonly retries: number
  private readonly retryIntervalMs: number
  private closed = false
  private closing: Promise<void> | undefined
  private readonly statistics: ProducerRecorder
  private provisioned: Promise<void> | undefined
  private readonly batchLength: number
  private readonly lingerMs: number
  private lastSentAt = 0
  private readonly background: Required<Omit<ProducerBackgroundOptions, "onError">> | undefined
  private readonly onError: ((error: PublishFailedError) => void | Promise<void>) | undefined
  private readonly queue: QueuedSend[] = []
  private queuedBytes = 0
  private flushTimer: ReturnType<typeof setTimeout> | undefined
  private draining: Promise<void> | undefined
  private backgroundFailure: PublishFailedError | undefined
  private readonly roomWaiters: (() => void)[] = []

  constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    private readonly topicName: string,
    private readonly options: ProducerOptions = {}
  ) {
    this.statistics = new ProducerRecorder(streamName, topicName, transport)
    this.routing =
      options.routing?.kind === "key"
        ? { kind: "key", key: options.routing.key.slice() }
        : (options.routing ?? { kind: "balanced" })
    this.retries = options.retries ?? DEFAULT_RETRIES
    this.retryIntervalMs = options.retryIntervalMs ?? DEFAULT_RETRY_INTERVAL_MS
    if (!Number.isSafeInteger(this.retries) || this.retries < 0) {
      throw new InvalidError("producer retries must be a non-negative safe integer")
    }
    if (!Number.isFinite(this.retryIntervalMs) || this.retryIntervalMs < 0) {
      throw new InvalidError("producer retryIntervalMs must be a non-negative finite number")
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
    if (options.maxTopicBytes !== undefined && options.maxTopicBytes <= 0n) {
      throw new InvalidError("producer maxTopicBytes must be greater than zero")
    }
    const background = options.background
    this.onError = background?.onError
    this.background =
      background === undefined
        ? undefined
        : {
            batchLength: nonNegative(background.batchLength ?? 1_000, "background batchLength"),
            batchBytes: nonNegative(background.batchBytes ?? MIB, "background batchBytes"),
            lingerMs: nonNegative(background.lingerMs ?? 1, "background lingerMs"),
            maxBufferBytes: nonNegative(
              background.maxBufferBytes ?? 32 * MIB,
              "background maxBufferBytes"
            )
          }
  }

  /** True when this producer queues sends in background mode. */
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
      const expiry = this.options.messageExpiryMicros
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
      { payload, ...(options.headers !== undefined ? { headers: options.headers } : {}) },
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
    return this.sendMessage(message, routing)
  }

  async sendKeyed(message: ProducerMessage, key: BytesLike): Promise<SendMessagesResponse> {
    return this.sendMessage(message, { kind: "key", key: ownedBytes(key) })
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
      isProducerMessage(message)
        ? lowerMessage(message)
        : { payload: ownedBytes(message), headers: headersMap(options.headers) }
    )
    return this.sendLoweredBatch(lowered, optionRouting(options, this.routing))
  }

  async sendBatchWithRouting(
    messages: readonly ProducerMessage[],
    routing: Routing = this.routing
  ): Promise<SendMessagesResponse> {
    return this.sendLoweredBatch(messages.map(lowerMessage), routing)
  }

  private async sendLoweredBatch(
    messages: readonly MessageWithHeaders[],
    routing: Routing
  ): Promise<SendMessagesResponse> {
    this.throwIfClosed("sendBatch")
    if (messages.length === 0) return { confirmations: [] }
    return this.sendWithRetry(messages, routing)
  }

  /** Waits until every queued background send is written. A direct
   * producer has nothing to flush. Rejects with the unreported background
   * failures as one `PublishFailedError` that lists the records of every
   * failed batch. */
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
  // A failure reports the confirmed prefix and every record from the failed
  // request on.
  private async writeChunks(
    messages: readonly MessageWithHeaders[],
    routing: Routing
  ): Promise<SendMessagesResponse> {
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
    background: Required<Omit<ProducerBackgroundOptions, "onError">>
  ): Promise<SendMessagesResponse> {
    await this.provision()
    this.throwIfClosed("send")
    const bytes = payloadBytes(messages)
    while (
      background.maxBufferBytes > 0 &&
      this.queuedBytes > 0 &&
      this.queuedBytes + bytes > background.maxBufferBytes
    ) {
      await new Promise<void>((resolve) => this.roomWaiters.push(resolve))
      this.throwIfClosed("send")
    }
    this.queue.push({ messages, routing, bytes })
    this.queuedBytes += bytes
    const full =
      (background.batchLength > 0 && this.queue.length >= background.batchLength) ||
      (background.batchBytes > 0 && this.queuedBytes >= background.batchBytes)
    if (full || background.lingerMs === 0) {
      void this.drainQueue()
    } else {
      this.flushTimer ??= setTimeout(() => {
        this.flushTimer = undefined
        void this.drainQueue()
      }, background.lingerMs)
      this.flushTimer.unref()
    }
    return { confirmations: [] }
  }

  // Writes queued sends in order, merging adjacent sends that share routing.
  private drainQueue(): Promise<void> {
    if (this.flushTimer !== undefined) {
      clearTimeout(this.flushTimer)
      this.flushTimer = undefined
    }
    this.draining ??= Promise.resolve().then(async () => {
      try {
        while (this.queue.length > 0) {
          const first = this.queue.shift()
          if (first === undefined) break
          const merged = [...first.messages]
          let bytes = first.bytes
          while (this.queue[0] !== undefined && sameRouting(this.queue[0].routing, first.routing)) {
            const next = this.queue.shift()
            if (next === undefined) break
            merged.push(...next.messages)
            bytes += next.bytes
          }
          try {
            await this.writeChunks(merged, first.routing)
          } catch (error) {
            await this.reportBackgroundFailure(error)
          } finally {
            this.queuedBytes -= bytes
            for (const wake of this.roomWaiters.splice(0)) wake()
          }
        }
      } finally {
        this.draining = undefined
      }
    })
    return this.draining
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
          ...(this.options.retries === undefined ? {} : { maxRetries: this.retries }),
          ...(this.options.retryIntervalMs === undefined
            ? {}
            : { retryBackoffMs: this.retryIntervalMs })
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
        if (this.retryIntervalMs > 0) {
          await new Promise((resolve) => setTimeout(resolve, this.retryIntervalMs))
        }
      }
    }
  }
}

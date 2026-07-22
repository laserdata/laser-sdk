import { type BytesLike, ownedBytes } from "../client/bytes.js"
import { InvalidError, TransportError } from "../client/errors.js"
import type { IggyHeaderValue, LaserTransport, MessageWithHeaders } from "../iggy/apache-iggy.js"
import type { Routing } from "./routing.js"

export interface ProducerOptions {
  readonly routing?: Routing
  readonly retries?: number
  readonly retryIntervalMs?: number
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

export class Producer {
  private readonly routing: Routing
  private readonly retries: number
  private readonly retryIntervalMs: number
  private closed = false

  constructor(
    private readonly transport: LaserTransport,
    private readonly streamName: string,
    private readonly topicName: string,
    options: ProducerOptions = {}
  ) {
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
  }

  async send(payload: BytesLike, options: ProducerSendOptions = {}): Promise<void> {
    await this.sendMessage(
      { payload, ...(options.headers !== undefined ? { headers: options.headers } : {}) },
      optionRouting(options, this.routing)
    )
  }

  async sendMessage(message: ProducerMessage, routing: Routing = this.routing): Promise<void> {
    this.throwIfClosed("sendMessage")
    await this.sendWithRetry([lowerMessage(message)], routing)
  }

  async sendWithRouting(message: ProducerMessage, routing: Routing): Promise<void> {
    await this.sendMessage(message, routing)
  }

  async sendKeyed(message: ProducerMessage, key: BytesLike): Promise<void> {
    await this.sendMessage(message, { kind: "key", key: ownedBytes(key) })
  }

  async sendToPartition(message: ProducerMessage, partition: number): Promise<void> {
    if (!Number.isSafeInteger(partition) || partition < 0) {
      throw new InvalidError("partition must be a non-negative safe integer")
    }
    await this.sendMessage(message, { kind: "partition", partition })
  }

  async sendBatch(
    messages: readonly (BytesLike | ProducerMessage)[],
    options: ProducerSendOptions = {}
  ): Promise<number> {
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
  ): Promise<number> {
    return this.sendLoweredBatch(messages.map(lowerMessage), routing)
  }

  private async sendLoweredBatch(
    messages: readonly MessageWithHeaders[],
    routing: Routing
  ): Promise<number> {
    this.throwIfClosed("sendBatch")
    if (messages.length === 0) return 0
    await this.sendWithRetry(messages, routing)
    return messages.length
  }

  flush(): Promise<void> {
    return this.closed
      ? Promise.reject(new InvalidError("flush() called after shutdown()"))
      : Promise.resolve()
  }

  shutdown(): Promise<void> {
    this.closed = true
    return Promise.resolve()
  }

  private throwIfClosed(operation: string): void {
    if (this.closed) throw new InvalidError(`${operation}() called after shutdown()`)
  }

  private async sendWithRetry(
    messages: readonly MessageWithHeaders[],
    routing: Routing
  ): Promise<void> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        await this.transport.sendMessagesWithHeaders(
          this.streamName,
          this.topicName,
          messages,
          routing.kind === "key" ? routing.key : undefined,
          routing.kind === "partition" ? routing.partition : undefined
        )
        return
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

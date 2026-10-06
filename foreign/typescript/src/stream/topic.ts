import type {
  LaserTransport,
  MessageWithHeaders,
  SendMessagesResponse
} from "../iggy/apache-iggy.js"
import type { HeaderValue } from "./header-value.js"
import { type BytesLike, ownedBytes } from "../client/bytes.js"
import { InvalidError, PublishFailedError, UnsupportedError } from "../client/errors.js"
import { CompiledSchema } from "../schema-codecs.js"
import type { SchemaDef } from "../wire/control.js"
import { ContentType } from "../wire/content.js"
import {
  encodeProvenanceHeaders,
  provenancePartitionKey,
  type Provenance
} from "../provenance/provenance.js"
import type { Codec } from "./codecs.js"
import { ConsumerGroup, type GroupContext } from "./consumer-group.js"
import { Consumer, type ConsumerOptions } from "./consumer.js"
import { Cursor } from "./cursor.js"
import { Producer, type ProducerOptions } from "./producer.js"
import { BatchingProducerBuilder } from "./batching.js"
import { BatchPublishRequest, PublishRequest } from "./publish.js"
import type { Routing } from "./routing.js"
import { TypedTopic } from "./typed-topic.js"

const DEFAULT_PARTITIONS = 1

export type GovernPublish = (
  stream: string,
  topic: string,
  payload: Uint8Array,
  provenance?: Provenance
) => Promise<Uint8Array>

export type ResolveSchema = (id: number) => Promise<SchemaDef | undefined>

export type ObserveEffect = <T>(
  operation: string,
  attributes: Readonly<Record<string, unknown>>,
  effect: () => Promise<T>
) => Promise<T>

export interface RawSendOptions {
  readonly key?: Uint8Array
  readonly partition?: number
  readonly provenance?: Provenance
  readonly headers?: ReadonlyMap<string, HeaderValue>
}

export class Topic {
  /** @internal */
  readonly streamName: string

  private constructor(
    private readonly transport: LaserTransport,
    streamName: string,
    readonly name: string,
    private readonly govern?: GovernPublish,
    private readonly resolveSchema?: ResolveSchema,
    private readonly observe?: ObserveEffect,
    private readonly groups?: GroupContext
  ) {
    this.streamName = streamName
  }

  /** @internal */
  static create(
    transport: LaserTransport,
    streamName: string,
    name: string,
    govern?: GovernPublish,
    resolveSchema?: ResolveSchema,
    observe?: ObserveEffect,
    groups?: GroupContext
  ): Topic {
    return new Topic(transport, streamName, name, govern, resolveSchema, observe, groups)
  }

  /** Idempotently creates this topic with `partitions`, creating its stream
   * first if needed, so one call is enough to address a fresh server. */
  async ensure(partitions: number = DEFAULT_PARTITIONS): Promise<void> {
    await this.ensureWithExpiry(partitions)
  }

  /** @internal `ensure` that also sets the message expiry of the topic. */
  async ensureWithExpiry(
    partitions: number = DEFAULT_PARTITIONS,
    messageExpiryMicros?: bigint
  ): Promise<void> {
    await this.observed("ensure", { partitions }, async () => {
      await this.transport.ensureStream(this.streamName)
      if (messageExpiryMicros !== undefined && this.transport.ensureTopicWithExpiry !== undefined) {
        await this.transport.ensureTopicWithExpiry(
          this.streamName,
          this.name,
          partitions,
          messageExpiryMicros
        )
        return
      }
      await this.transport.ensureTopic(this.streamName, this.name, partitions)
    })
  }

  /**
   * Idempotently creates the consumer group `name` on this topic without
   * joining it or giving it a filter policy. `consumerGroup(name).create()`
   * configures a policy in the same step.
   */
  async ensureConsumerGroup(name: string): Promise<void> {
    await this.observed("ensure", { group: name }, async () => {
      await this.consumerGroup(name).create()
    })
  }

  async send(payload: BytesLike, options: RawSendOptions = {}): Promise<SendMessagesResponse> {
    if (options.key !== undefined && options.partition !== undefined) {
      throw new InvalidError("send() accepts a routing key or an explicit partition, not both")
    }
    const input = ownedBytes(payload)
    const bytes =
      this.govern === undefined
        ? input
        : await this.govern(this.streamName, this.name, input, options.provenance)
    const routing: Routing =
      options.partition !== undefined
        ? { kind: "partition", partition: options.partition }
        : options.key !== undefined
          ? { kind: "key", key: options.key }
          : { kind: "balanced" }

    const headers = new Map(options.headers)
    if (options.provenance !== undefined) {
      for (const [key, value] of encodeProvenanceHeaders(options.provenance))
        headers.set(key, value)
    }
    if (headers.size > 0) {
      return this.observed("publish", { records: 1 }, () =>
        this.published([{ payload: bytes, headers }], () =>
          this.transport.sendMessageWithHeaders(
            this.streamName,
            this.name,
            bytes,
            headers,
            options.key ??
              (options.provenance !== undefined && options.partition === undefined
                ? provenancePartitionKey(options.provenance)
                : undefined),
            options.partition
          )
        )
      )
    } else {
      return this.observed("publish", { records: 1 }, () =>
        this.published([{ payload: bytes, headers }], () =>
          this.transport.sendMessages(this.streamName, this.name, [bytes], routing)
        )
      )
    }
  }

  /** Appends the messages in order, in requests of at most 1000 records. A
   * message is a payload or a record with its own headers and id, so the
   * unconfirmed records of a `PublishFailedError` resend deduplicated. An
   * empty batch sends nothing. A failure throws `PublishFailedError` with the
   * confirmed requests and the records from the failed request on. */
  async batch(
    messages: readonly (BytesLike | MessageWithHeaders)[],
    options: RawSendOptions = {}
  ): Promise<SendMessagesResponse> {
    if (options.key !== undefined && options.partition !== undefined) {
      throw new InvalidError("batch() accepts a routing key or an explicit partition, not both")
    }
    if (messages.length === 0) return { confirmations: [] }
    if (messages.some(isRecord)) return this.batchRecords(messages, options)
    const payloads = messages as readonly BytesLike[]
    const bytesList: Uint8Array[] = []
    for (const payload of payloads) {
      const input = ownedBytes(payload)
      bytesList.push(
        this.govern === undefined
          ? input
          : await this.govern(this.streamName, this.name, input, options.provenance)
      )
    }
    const routing: Routing =
      options.partition !== undefined
        ? { kind: "partition", partition: options.partition }
        : options.key !== undefined
          ? { kind: "key", key: options.key }
          : { kind: "balanced" }

    const headers = new Map(options.headers)
    if (options.provenance !== undefined) {
      for (const [key, value] of encodeProvenanceHeaders(options.provenance))
        headers.set(key, value)
    }
    if (headers.size > 0) {
      const records = bytesList.map((payload) => ({ payload, headers }))
      return this.observed("publish_batch", { records: bytesList.length }, () =>
        this.published(records, () =>
          this.transport.sendMessagesWithHeaders(
            this.streamName,
            this.name,
            records,
            options.key ??
              (options.provenance !== undefined && options.partition === undefined
                ? provenancePartitionKey(options.provenance)
                : undefined),
            options.partition
          )
        )
      )
    } else {
      return this.observed("publish_batch", { records: bytesList.length }, () =>
        this.published(
          bytesList.map((payload) => ({ payload, headers })),
          () => this.transport.sendMessages(this.streamName, this.name, bytesList, routing)
        )
      )
    }
  }

  /** @internal Appends records with their own headers and ids. */
  async sendRecords(
    records: readonly MessageWithHeaders[],
    options: { readonly key?: string | Uint8Array; readonly partition?: number } = {}
  ): Promise<SendMessagesResponse> {
    if (options.key !== undefined && options.partition !== undefined) {
      throw new InvalidError(
        "sendRecords() accepts a routing key or an explicit partition, not both"
      )
    }
    if (records.length === 0) return { confirmations: [] }
    const governed: MessageWithHeaders[] = []
    for (const record of records) {
      const input = ownedBytes(record.payload)
      governed.push({
        payload:
          this.govern === undefined ? input : await this.govern(this.streamName, this.name, input),
        headers: record.headers,
        ...(record.id !== undefined ? { id: record.id } : {})
      })
    }
    return this.observed("publish_batch", { records: governed.length }, () =>
      this.published(governed, () =>
        this.transport.sendMessagesWithHeaders(
          this.streamName,
          this.name,
          governed,
          options.key,
          options.partition
        )
      )
    )
  }

  // Payload entries take the shared headers and provenance, record entries
  // keep their own headers and id.
  private batchRecords(
    messages: readonly (BytesLike | MessageWithHeaders)[],
    options: RawSendOptions
  ): Promise<SendMessagesResponse> {
    const headers = new Map(options.headers)
    if (options.provenance !== undefined) {
      for (const [key, value] of encodeProvenanceHeaders(options.provenance))
        headers.set(key, value)
    }
    const records = messages.map((message) =>
      isRecord(message) ? message : { payload: ownedBytes(message), headers }
    )
    const key =
      options.key ??
      (options.provenance !== undefined && options.partition === undefined
        ? provenancePartitionKey(options.provenance)
        : undefined)
    return this.sendRecords(records, {
      ...(key === undefined ? {} : { key }),
      ...(options.partition === undefined ? {} : { partition: options.partition })
    })
  }

  // Preserve confirmations and message IDs from a transport failure.
  private published(
    records: readonly MessageWithHeaders[],
    effect: () => Promise<SendMessagesResponse>
  ): Promise<SendMessagesResponse> {
    return effect().catch((error: unknown) => {
      if (error instanceof PublishFailedError) throw error
      throw new PublishFailedError(this.streamName, this.name, [], records, error)
    })
  }

  private observed<T>(
    operation: string,
    attributes: Readonly<Record<string, unknown>>,
    effect: () => Promise<T>
  ): Promise<T> {
    if (this.observe === undefined) return effect()
    return this.observe(
      `laser.topic.${operation}`,
      {
        operation,
        stream: this.streamName,
        topic: this.name,
        ...attributes
      },
      effect
    )
  }

  producer(options?: ProducerOptions): Producer {
    return Producer.create(this.transport, this.streamName, this.name, options)
  }

  /** A size-and-time batching publisher over this topic. Each flushed batch is
   * one append under the handle's partition key, or balanced without one. */
  batching(): BatchingProducerBuilder {
    return BatchingProducerBuilder.create(
      (records, partitionKey) =>
        this.sendRecords(records, partitionKey === undefined ? {} : { key: partitionKey }),
      this.streamName,
      this.name
    )
  }

  consumer(partitionId: number, options?: ConsumerOptions): Consumer
  consumer(name: string, partitionId: number, options?: ConsumerOptions): Consumer
  consumer(
    nameOrPartition: string | number,
    partitionOrOptions?: number | ConsumerOptions,
    namedOptions?: ConsumerOptions
  ): Consumer {
    const partitionId =
      typeof nameOrPartition === "number" ? nameOrPartition : (partitionOrOptions as number)
    const options =
      typeof nameOrPartition === "number"
        ? (partitionOrOptions as ConsumerOptions | undefined)
        : namedOptions
    return Consumer.create(
      this.transport,
      this.streamName,
      this.name,
      {
        kind: "single",
        partitionId,
        ...(typeof nameOrPartition === "string" ? { name: nameOrPartition } : {})
      },
      options
    )
  }

  /**
   * The consumer group `name` of this topic: the handle that owns the group's
   * filter policy and builds its consumers and readers. Free and synchronous,
   * IO happens at the verbs.
   */
  consumerGroup(name: string): ConsumerGroup {
    return ConsumerGroup.create(this.transport, this, { kind: "name", name }, this.groups)
  }

  /**
   * `consumerGroup` by the group's native numeric id. The id names a group
   * inside this topic incarnation only.
   */
  consumerGroupId(id: bigint | number): ConsumerGroup {
    if (typeof id === "number" && !Number.isSafeInteger(id))
      throw new InvalidError("consumer group id must be an integer")
    const value = BigInt(id)
    if (value < 0n || value > 0xffff_ffffn)
      throw new InvalidError("consumer group id exceeds 32 bits")
    return ConsumerGroup.create(this.transport, this, { kind: "id", id: value }, this.groups)
  }

  async replay(): Promise<Cursor> {
    const partitionCount = await this.transport.getTopicPartitionCount(this.streamName, this.name)
    const partitionIds = Array.from({ length: partitionCount }, (_, index) => index)
    return Cursor.create(this.transport, this.streamName, this.name, partitionIds)
  }

  /** @internal The partition count, or `undefined` when the topic does not exist yet. */
  partitionCount(): Promise<number | undefined> {
    return this.transport.findTopicPartitionCount(this.streamName, this.name)
  }

  /** @internal The next offset each partition will write, `0n` for an empty partition.
   * Empty when the topic does not exist yet. */
  async tailOffsets(): Promise<ReadonlyMap<number, bigint>> {
    const partitionCount = (await this.partitionCount()) ?? 0
    const tails = await Promise.all(
      Array.from({ length: partitionCount }, async (_, partitionId) => {
        const polled = await this.transport.pollMessages(
          this.streamName,
          this.name,
          { kind: "single", partitionId, name: "laser-checkpoint" },
          { kind: "last" },
          1,
          false
        )
        const last = polled.at(-1)
        return [partitionId, last === undefined ? 0n : last.offset + 1n] as const
      })
    )
    return new Map(tails)
  }

  publish(): PublishRequest {
    return PublishRequest.create(this)
  }

  publishBatch(): BatchPublishRequest {
    return BatchPublishRequest.create(this)
  }

  json<T>(codec: Codec<T>): TypedTopic<T> {
    return TypedTopic.create(this, codec, "json")
  }

  cbor<T>(codec: Codec<T>): TypedTopic<T> {
    return TypedTopic.create(this, codec, "cbor")
  }

  async schema<T>(
    schemaId: number,
    codecOrDecoder: Codec<T> | ((value: unknown) => T)
  ): Promise<TypedTopic<T>> {
    if (this.resolveSchema === undefined) {
      throw new UnsupportedError("registered schema topics require a Laser-managed topic", {
        surface: "schemas"
      })
    }
    const schema = await this.resolveSchema(schemaId)
    if (schema === undefined) {
      throw new InvalidError(`schema ${String(schemaId)} is not registered`)
    }
    const compiled = CompiledSchema.compile(schema)
    const contentType =
      compiled.kind === "avro"
        ? ContentType.Avro
        : compiled.kind === "protobuf"
          ? ContentType.Protobuf
          : ContentType.Json
    const codec =
      typeof codecOrDecoder === "function" ? compiled.codec(codecOrDecoder) : codecOrDecoder
    return TypedTopic.create(this, codec, "schema", {
      contentType,
      schemaId,
      compiled
    })
  }
}

function isRecord(message: BytesLike | MessageWithHeaders): message is MessageWithHeaders {
  return !(message instanceof ArrayBuffer) && !ArrayBuffer.isView(message)
}

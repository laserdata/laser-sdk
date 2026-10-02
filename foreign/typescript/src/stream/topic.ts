import type {
  IggyHeaderValue,
  LaserTransport,
  MessageWithHeaders,
  SendMessagesResponse
} from "../iggy/apache-iggy.js"
import { type BytesLike, ownedBytes } from "../client/bytes.js"
import { InvalidError, UnsupportedError } from "../client/errors.js"
import { CompiledSchema } from "../schema-codecs.js"
import type { SchemaDef } from "../wire/control.js"
import { ContentType } from "../wire/content.js"
import {
  encodeProvenanceHeaders,
  provenancePartitionKey,
  type Provenance
} from "../provenance/provenance.js"
import type { Codec, ValueDecoder } from "./codecs.js"
import { ConsumerGroup, type GroupContext } from "./consumer-group.js"
import { Consumer, type ConsumerOptions } from "./consumer.js"
import { Cursor, type CursorOptions } from "./cursor.js"
import { Producer, type ProducerOptions } from "./producer.js"
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
  readonly headers?: ReadonlyMap<string, IggyHeaderValue>
}

export interface TopicEnsureOptions {
  readonly messageExpiryMicros?: bigint
}

export class Topic {
  constructor(
    private readonly transport: LaserTransport,
    readonly streamName: string,
    readonly name: string,
    private readonly govern?: GovernPublish,
    private readonly resolveSchema?: ResolveSchema,
    private readonly observe?: ObserveEffect,
    private readonly groups?: GroupContext
  ) {}

  /** Idempotently creates this topic with `partitions`, creating its stream
   * first if needed, so one call is enough to address a fresh server. */
  async ensure(
    partitions: number = DEFAULT_PARTITIONS,
    options: TopicEnsureOptions = {}
  ): Promise<void> {
    await this.observed("ensure", { partitions }, async () => {
      await this.transport.ensureStream(this.streamName)
      if (
        options.messageExpiryMicros !== undefined &&
        this.transport.ensureTopicWithExpiry !== undefined
      ) {
        await this.transport.ensureTopicWithExpiry(
          this.streamName,
          this.name,
          partitions,
          options.messageExpiryMicros
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
    } else {
      return this.observed("publish", { records: 1 }, () =>
        this.transport.sendMessages(this.streamName, this.name, [bytes], routing)
      )
    }
  }

  async batch(
    payloads: readonly BytesLike[],
    options: RawSendOptions = {}
  ): Promise<SendMessagesResponse> {
    if (options.key !== undefined && options.partition !== undefined) {
      throw new InvalidError("batch() accepts a routing key or an explicit partition, not both")
    }
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
      return this.observed("publish_batch", { records: bytesList.length }, () =>
        this.transport.sendMessagesWithHeaders(
          this.streamName,
          this.name,
          bytesList.map((payload) => ({ payload, headers })),
          options.key ??
            (options.provenance !== undefined && options.partition === undefined
              ? provenancePartitionKey(options.provenance)
              : undefined),
          options.partition
        )
      )
    } else {
      return this.observed("publish_batch", { records: bytesList.length }, () =>
        this.transport.sendMessages(this.streamName, this.name, bytesList, routing)
      )
    }
  }

  async sendRecords(
    records: readonly MessageWithHeaders[],
    options: { readonly key?: Uint8Array; readonly partition?: number } = {}
  ): Promise<SendMessagesResponse> {
    if (options.key !== undefined && options.partition !== undefined) {
      throw new InvalidError(
        "sendRecords() accepts a routing key or an explicit partition, not both"
      )
    }
    const governed: MessageWithHeaders[] = []
    for (const record of records) {
      const input = ownedBytes(record.payload)
      governed.push({
        payload:
          this.govern === undefined ? input : await this.govern(this.streamName, this.name, input),
        headers: record.headers
      })
    }
    return this.observed("publish_batch", { records: governed.length }, () =>
      this.transport.sendMessagesWithHeaders(
        this.streamName,
        this.name,
        governed,
        options.key,
        options.partition
      )
    )
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
    return new Producer(this.transport, this.streamName, this.name, options)
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
    return new Consumer(
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
    return new ConsumerGroup(
      this.transport,
      this.streamName,
      this.name,
      { kind: "name", name },
      this.groups
    )
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
    return new ConsumerGroup(
      this.transport,
      this.streamName,
      this.name,
      { kind: "id", id: value },
      this.groups
    )
  }

  async replay(options?: CursorOptions): Promise<Cursor> {
    const partitionCount = await this.transport.getTopicPartitionCount(this.streamName, this.name)
    const partitionIds = Array.from({ length: partitionCount }, (_, index) => index)
    return new Cursor(this.transport, this.streamName, this.name, partitionIds, options)
  }

  /** The partition count, or `undefined` when the topic does not exist yet. */
  partitionCount(): Promise<number | undefined> {
    return this.transport.findTopicPartitionCount(this.streamName, this.name)
  }

  /** The next offset each partition will write, `0n` for an empty partition.
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
    return new PublishRequest(this)
  }

  publishBatch(): BatchPublishRequest {
    return new BatchPublishRequest(this)
  }

  json<T>(codec: Codec<T>): TypedTopic<T> {
    return new TypedTopic(this, codec, "json")
  }

  cbor<T>(codec: Codec<T>): TypedTopic<T> {
    return new TypedTopic(this, codec, "cbor")
  }

  async schema<T>(
    schemaId: number,
    codecOrDecoder: Codec<T> | ValueDecoder<T>
  ): Promise<TypedTopic<T>> {
    if (this.resolveSchema === undefined) {
      throw new UnsupportedError("registered schema topics require a Laser-managed topic")
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
    return new TypedTopic(this, codec, "schema", {
      contentType,
      schemaId,
      compiled
    })
  }
}

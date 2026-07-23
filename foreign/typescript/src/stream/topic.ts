import type { IggyHeaderValue, LaserTransport, MessageWithHeaders } from "../iggy/apache-iggy.js"
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
    private readonly observe?: ObserveEffect
  ) {}

  async ensure(
    partitions: number = DEFAULT_PARTITIONS,
    options: TopicEnsureOptions = {}
  ): Promise<void> {
    await this.observed("ensure", { partitions }, async () => {
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

  async send(payload: BytesLike, options: RawSendOptions = {}): Promise<void> {
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
      await this.observed("publish", { records: 1 }, () =>
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
      await this.observed("publish", { records: 1 }, () =>
        this.transport.sendMessages(this.streamName, this.name, [bytes], routing)
      )
    }
  }

  async batch(payloads: readonly BytesLike[], options: RawSendOptions = {}): Promise<number> {
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
      await this.observed("publish_batch", { records: bytesList.length }, () =>
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
      await this.observed("publish_batch", { records: bytesList.length }, () =>
        this.transport.sendMessages(this.streamName, this.name, bytesList, routing)
      )
    }
    return bytesList.length
  }

  async sendRecords(
    records: readonly MessageWithHeaders[],
    options: { readonly key?: Uint8Array; readonly partition?: number } = {}
  ): Promise<void> {
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
    await this.observed("publish_batch", { records: governed.length }, () =>
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

  async consumerGroup(name: string, options?: ConsumerOptions): Promise<Consumer> {
    await this.transport.joinConsumerGroup(this.streamName, this.name, name)
    return new Consumer(
      this.transport,
      this.streamName,
      this.name,
      { kind: "group", name },
      options
    )
  }

  async replay(options?: CursorOptions): Promise<Cursor> {
    const partitionCount = await this.transport.getTopicPartitionCount(this.streamName, this.name)
    const partitionIds = Array.from({ length: partitionCount }, (_, index) => index)
    return new Cursor(this.transport, this.streamName, this.name, partitionIds, options)
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

import {
  type LaserTransport,
  type MessageWithHeaders,
  type SendMessagesResponse,
  missingTopic,
  validateCount
} from "../iggy/apache-iggy.js"
import type { Materialization, MaterializeRequest } from "../managed/materialize.js"
import type { HeaderValue } from "./header-value.js"
import { type BytesLike, ownedBytes } from "../client/bytes.js"
import { type CallOptions, abortable, cancellable, throwIfAborted } from "../client/call-options.js"
import {
  CancelledError,
  InvalidError,
  PublishFailedError,
  UnsupportedError
} from "../client/errors.js"
import { CompiledSchema } from "../schema-codecs.js"
import type { SchemaDef } from "../wire/control.js"
import { ContentType } from "../wire/content.js"
import {
  encodeProvenanceHeaders,
  provenancePartitionKey,
  type Provenance
} from "../provenance/provenance.js"
import type { Codec } from "./codecs.js"
import {
  type ConsumerGroupSummary,
  type PartitionInfo,
  type TopicInfo,
  type UpdateTopicOptions,
  adminOperation
} from "./admin.js"
import { administer } from "../client/server.js"
import { millis, millisToMicros } from "../client/duration.js"
import { deleteSegments, updateTopic } from "../iggy/server-admin.js"
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

export type ResolveSchema = (id: number, options?: CallOptions) => Promise<SchemaDef | undefined>

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
    private readonly groups?: GroupContext,
    private readonly materializeRequest?: () => MaterializeRequest,
    private readonly forget?: () => void
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
    groups?: GroupContext,
    materializeRequest?: () => MaterializeRequest,
    forget?: () => void
  ): Topic {
    return new Topic(
      transport,
      streamName,
      name,
      govern,
      resolveSchema,
      observe,
      groups,
      materializeRequest,
      forget
    )
  }

  materialize(): MaterializeRequest {
    if (this.materializeRequest === undefined)
      throw new UnsupportedError("materializations require a managed client", {
        surface: "materializations"
      })
    return this.materializeRequest()
  }

  /** The handle `materialize().apply()` returned for the setup whose
   * projection is `projectionId`, read under this topic's stream. A setup
   * without an explicit projection id or typed projection uses the index
   * name, which defaults to the topic name. */
  materialization(projectionId: string): Materialization {
    return this.materialize().attach(projectionId)
  }

  /** Idempotently creates this topic with `partitions`, creating its stream
   * first if needed, so one call is enough to address a fresh server.
   * `replicationFactor` is how many nodes store each partition,
   * 1..=`MAX_REPLICATION_FACTOR` and at most the cluster's node count, the
   * server's default when left out. An existing topic is left as it is. */
  async ensure(
    partitions: number = DEFAULT_PARTITIONS,
    replicationFactor?: number,
    options: CallOptions = {}
  ): Promise<void> {
    await this.ensureWithExpiry(partitions, undefined, replicationFactor, options)
  }

  /** @internal `ensure` that also sets the message expiry of the topic. */
  async ensureWithExpiry(
    partitions: number = DEFAULT_PARTITIONS,
    messageExpiryMicros?: bigint,
    replicationFactor?: number,
    options: CallOptions = {}
  ): Promise<void> {
    const signal = options.signal
    throwIfAborted(signal, "topic ensure")
    await this.observed("ensure", { partitions }, async () => {
      await abortable(this.transport.ensureStream(this.streamName), signal, "topic ensure")
      if (messageExpiryMicros !== undefined && this.transport.ensureTopicWithExpiry !== undefined) {
        const ensureTopicWithExpiry = this.transport.ensureTopicWithExpiry.bind(this.transport)
        await cancellable(signal, "topic ensure", () =>
          ensureTopicWithExpiry(
            this.streamName,
            this.name,
            partitions,
            messageExpiryMicros,
            replicationFactor
          )
        )
        return
      }
      await cancellable(signal, "topic ensure", () =>
        this.transport.ensureTopic(this.streamName, this.name, partitions, replicationFactor)
      )
    })
  }

  /**
   * Idempotently creates the consumer group `name` on this topic without
   * joining it or giving it a filter policy. `consumerGroup(name).create()`
   * configures a policy in the same step.
   */
  async ensureConsumerGroup(name: string, options: CallOptions = {}): Promise<void> {
    await this.observed("ensure", { group: name }, async () => {
      await this.consumerGroup(name).create(options)
    })
  }

  /** This topic as the server reports it. Resolves `undefined` when the topic or its stream does not exist. */
  async get(options: CallOptions = {}): Promise<TopicInfo | undefined> {
    return (await this.details(options))?.topic
  }

  /** Whether this topic exists. */
  async exists(options: CallOptions = {}): Promise<boolean> {
    return (await this.details(options)) !== undefined
  }

  /** Deletes this topic with every message in it. Resolves `false` when the topic or its stream did not exist. */
  async delete(options: CallOptions = {}): Promise<boolean> {
    const remove = adminOperation(this.transport.deleteTopic?.bind(this.transport), "topic delete")
    // An abandoned delete that lands still clears what the client cached for the topic.
    return cancellable(options.signal, "topic delete", async () => {
      const deleted = await this.observed("delete", {}, () => remove(this.streamName, this.name))
      this.forget?.()
      return deleted
    })
  }

  /** Every partition of this topic. A missing topic fails with an error `isStreamOrTopicNotFound` recognizes. */
  async partitions(options: CallOptions = {}): Promise<readonly PartitionInfo[]> {
    const details = await this.details(options)
    if (details === undefined) throw missingTopic(this.streamName, this.name)
    return details.partitions
  }

  /** Adds `count` partitions after the existing ones. A topic created with a fixed partition layout refuses the change. */
  async addPartitions(count: number, options: CallOptions = {}): Promise<void> {
    const add = adminOperation(this.transport.addPartitions?.bind(this.transport), "partition add")
    throwIfAborted(options.signal, "partition add")
    await this.observed("partitions", { add: count }, () =>
      abortable(
        add(this.streamName, this.name, count).then(() => this.forget?.()),
        options.signal,
        "partition add"
      )
    )
  }

  /** Deletes the last `count` partitions with their messages. A topic created with a fixed partition layout refuses the change. */
  async deletePartitions(count: number, options: CallOptions = {}): Promise<void> {
    const remove = adminOperation(
      this.transport.deletePartitions?.bind(this.transport),
      "partition delete"
    )
    throwIfAborted(options.signal, "partition delete")
    await this.observed("partitions", { delete: count }, () =>
      abortable(
        remove(this.streamName, this.name, count).then(() => this.forget?.()),
        options.signal,
        "partition delete"
      )
    )
  }

  /** Changes the name, retention, or size cap of this topic. A field left out keeps its value. Pass `expireAfterMs` or `neverExpire`, and `maxTopicBytes` or `unlimitedTopicSize`. After a rename, address the topic by its new name. */
  async update(options: UpdateTopicOptions & CallOptions = {}): Promise<void> {
    if (options.expireAfterMs !== undefined && options.neverExpire === true)
      throw new InvalidError("set expireAfterMs or neverExpire, not both")
    if (options.maxTopicBytes !== undefined && options.unlimitedTopicSize === true)
      throw new InvalidError("set maxTopicBytes or unlimitedTopicSize, not both")
    const settings = {
      ...(options.name === undefined ? {} : { name: options.name }),
      ...(options.expireAfterMs === undefined
        ? {}
        : { expiryMicros: millisToMicros(millis(options.expireAfterMs, "expireAfterMs")) }),
      ...(options.neverExpire === undefined ? {} : { neverExpire: options.neverExpire }),
      ...(options.maxTopicBytes === undefined ? {} : { maxTopicBytes: options.maxTopicBytes }),
      ...(options.unlimitedTopicSize === undefined
        ? {}
        : { unlimitedTopicSize: options.unlimitedTopicSize })
    }
    // An abandoned update that lands still clears what the client cached for the topic.
    return administer(this.transport, "topic update", false, options, async (client) => {
      await updateTopic(client, this.streamName, this.name, settings)
      this.transport.forgetTopic?.(this.streamName, this.name)
      this.forget?.()
    })
  }

  /** Deletes the oldest `count` segments of one partition with their messages. The open segment stays. */
  async deleteSegments(
    partitionId: number,
    count: number,
    options: CallOptions = {}
  ): Promise<void> {
    validateCount(count)
    return administer(this.transport, "segment delete", false, options, (client) =>
      deleteSegments(client, this.streamName, this.name, partitionId, count)
    )
  }

  /** Every consumer group of this topic. A missing topic fails with an error `isStreamOrTopicNotFound` recognizes. */
  async consumerGroups(options: CallOptions = {}): Promise<readonly ConsumerGroupSummary[]> {
    const list = adminOperation(
      this.transport.listConsumerGroups?.bind(this.transport),
      "consumer group listing"
    )
    throwIfAborted(options.signal, "consumer group listing")
    return abortable(list(this.streamName, this.name), options.signal, "consumer group listing")
  }

  private async details(
    options: CallOptions
  ): Promise<
    { readonly topic: TopicInfo; readonly partitions: readonly PartitionInfo[] } | undefined
  > {
    const find = adminOperation(this.transport.findTopic?.bind(this.transport), "topic lookup")
    throwIfAborted(options.signal, "topic lookup")
    return abortable(find(this.streamName, this.name), options.signal, "topic lookup")
  }

  async send(
    payload: BytesLike,
    options: RawSendOptions & CallOptions = {}
  ): Promise<SendMessagesResponse> {
    if (options.key !== undefined && options.partition !== undefined) {
      throw new InvalidError("send() accepts a routing key or an explicit partition, not both")
    }
    throwIfAborted(options.signal, "topic publish")
    return this.sendGoverned(
      await abortable(
        this.governPayload(ownedBytes(payload), options.provenance),
        options.signal,
        "topic publish"
      ),
      options
    )
  }

  /** @internal The body this topic's governor lets through for `payload`. */
  governPayload(payload: Uint8Array, provenance?: Provenance): Promise<Uint8Array> {
    return this.govern === undefined
      ? Promise.resolve(payload)
      : this.govern(this.streamName, this.name, payload, provenance)
  }

  /** @internal `send` for a body the governor already decided. */
  async sendGoverned(
    bytes: Uint8Array,
    options: RawSendOptions & CallOptions = {}
  ): Promise<SendMessagesResponse> {
    if (options.key !== undefined && options.partition !== undefined) {
      throw new InvalidError("send() accepts a routing key or an explicit partition, not both")
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
      return this.observed("publish", { records: 1 }, () =>
        this.published(
          [{ payload: bytes, headers }],
          () =>
            this.transport.sendMessageWithHeaders(
              this.streamName,
              this.name,
              bytes,
              headers,
              options.key ??
                (options.provenance !== undefined && options.partition === undefined
                  ? provenancePartitionKey(options.provenance)
                  : undefined),
              options.partition,
              { signal: options.signal }
            ),
          options.signal
        )
      )
    } else {
      return this.observed("publish", { records: 1 }, () =>
        this.published(
          [{ payload: bytes, headers }],
          () =>
            this.transport.sendMessages(this.streamName, this.name, [bytes], routing, {
              signal: options.signal
            }),
          options.signal
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
    options: RawSendOptions & CallOptions = {}
  ): Promise<SendMessagesResponse> {
    if (options.key !== undefined && options.partition !== undefined) {
      throw new InvalidError("batch() accepts a routing key or an explicit partition, not both")
    }
    throwIfAborted(options.signal, "topic publish")
    if (messages.length === 0) return { confirmations: [] }
    if (messages.some(isRecord)) return this.batchRecords(messages, options)
    const payloads = messages as readonly BytesLike[]
    const bytesList: Uint8Array[] = []
    for (const payload of payloads) {
      const input = ownedBytes(payload)
      bytesList.push(
        this.govern === undefined
          ? input
          : await abortable(
              this.govern(this.streamName, this.name, input, options.provenance),
              options.signal,
              "topic publish"
            )
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
        this.published(
          records,
          () =>
            this.transport.sendMessagesWithHeaders(
              this.streamName,
              this.name,
              records,
              options.key ??
                (options.provenance !== undefined && options.partition === undefined
                  ? provenancePartitionKey(options.provenance)
                  : undefined),
              options.partition,
              { signal: options.signal }
            ),
          options.signal
        )
      )
    } else {
      return this.observed("publish_batch", { records: bytesList.length }, () =>
        this.published(
          bytesList.map((payload) => ({ payload, headers })),
          () =>
            this.transport.sendMessages(this.streamName, this.name, bytesList, routing, {
              signal: options.signal
            }),
          options.signal
        )
      )
    }
  }

  /** @internal Appends records with their own headers and ids. */
  async sendRecords(
    records: readonly MessageWithHeaders[],
    options: { readonly key?: string | Uint8Array; readonly partition?: number } & CallOptions = {}
  ): Promise<SendMessagesResponse> {
    if (options.key !== undefined && options.partition !== undefined) {
      throw new InvalidError(
        "sendRecords() accepts a routing key or an explicit partition, not both"
      )
    }
    throwIfAborted(options.signal, "topic publish")
    if (records.length === 0) return { confirmations: [] }
    const governed: MessageWithHeaders[] = []
    for (const record of records) {
      const input = ownedBytes(record.payload)
      governed.push({
        payload:
          this.govern === undefined
            ? input
            : await abortable(
                this.govern(this.streamName, this.name, input),
                options.signal,
                "topic publish"
              ),
        headers: record.headers,
        ...(record.id !== undefined ? { id: record.id } : {})
      })
    }
    return this.observed("publish_batch", { records: governed.length }, () =>
      this.published(
        governed,
        () =>
          this.transport.sendMessagesWithHeaders(
            this.streamName,
            this.name,
            governed,
            options.key,
            options.partition,
            { signal: options.signal }
          ),
        options.signal
      )
    )
  }

  // Payload entries take the shared headers and provenance, record entries
  // keep their own headers and id.
  private batchRecords(
    messages: readonly (BytesLike | MessageWithHeaders)[],
    options: RawSendOptions & CallOptions
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
      ...(options.partition === undefined ? {} : { partition: options.partition }),
      ...(options.signal === undefined ? {} : { signal: options.signal })
    })
  }

  // Preserve confirmations and message IDs from a transport failure. An
  // aborted publish is no failure report, its outcome is unknown.
  private published(
    records: readonly MessageWithHeaders[],
    effect: () => Promise<SendMessagesResponse>,
    signal: AbortSignal | undefined
  ): Promise<SendMessagesResponse> {
    return cancellable(signal, "topic publish", effect).catch((error: unknown) => {
      if (error instanceof PublishFailedError || error instanceof CancelledError) throw error
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
    return Producer.create(
      this.transport,
      this.streamName,
      this.name,
      options,
      this.govern === undefined
        ? undefined
        : (payload, provenance) => this.governPayload(payload, provenance)
    )
  }

  /** A size-and-time batching publisher over this topic. Each flushed batch is
   * one append under the handle's partition key, or balanced without one. */
  batching(): BatchingProducerBuilder {
    return BatchingProducerBuilder.create(
      (records, partitionKey, options) =>
        this.sendRecords(records, {
          ...(partitionKey === undefined ? {} : { key: partitionKey }),
          signal: options.signal
        }),
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

  /** A resumable reader over this topic. Each poll reads the partitions the
   * topic has at that moment, so partitions added later are read too. */
  async replay(options: CallOptions = {}): Promise<Cursor> {
    throwIfAborted(options.signal, "topic replay")
    const partitionCount = await abortable(
      this.transport.getTopicPartitionCount(this.streamName, this.name),
      options.signal,
      "topic replay"
    )
    const partitionIds = Array.from({ length: partitionCount }, (_, index) => index)
    return Cursor.create(this.transport, this.streamName, this.name, partitionIds, () =>
      this.transport.findTopicPartitionCount(this.streamName, this.name)
    )
  }

  /** @internal The partition count, or `undefined` when the topic does not exist yet. */
  partitionCount(): Promise<number | undefined> {
    return this.transport.findTopicPartitionCount(this.streamName, this.name)
  }

  /** @internal The next offset each partition will write, `0n` for an empty partition.
   * Empty when the topic does not exist yet. */
  async tailOffsets(): Promise<ReadonlyMap<number, bigint>> {
    const partitionCount = (await this.partitionCount()) ?? 0
    const settled = await Promise.allSettled(
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
    const failure = settled.find((result) => result.status === "rejected")
    if (failure?.status === "rejected") throw failure.reason
    const tails = settled.flatMap((result) => (result.status === "fulfilled" ? [result.value] : []))
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
    codecOrDecoder: Codec<T> | ((value: unknown) => T),
    options: CallOptions = {}
  ): Promise<TypedTopic<T>> {
    throwIfAborted(options.signal, "topic schema")
    if (this.resolveSchema === undefined) {
      throw new UnsupportedError("registered schema topics require a Laser-managed topic", {
        surface: "schemas"
      })
    }
    const schema = await abortable(
      this.resolveSchema(schemaId, options),
      options.signal,
      "topic schema"
    )
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

import { CodecError, LaserError, TypedDecodeError } from "../client/errors.js"
import type { HeaderValue } from "./header-value.js"
import type { SendMessagesResponse } from "../iggy/apache-iggy.js"
import type { CompiledSchema } from "../schema-codecs.js"
import type { MessageId } from "../types/ids.js"
import {
  ContentType,
  contentTypeCode,
  type ContentType as ContentTypeValue
} from "../wire/content.js"
import type { Codec } from "./codecs.js"
import type { Cursor } from "./cursor.js"
import type { RawSendOptions } from "./topic.js"
import type { Topic } from "./topic.js"

export type TypedTopicKind = "json" | "cbor" | "schema"

export interface TypedRecord<T> {
  readonly value: T
  readonly position: MessageId
  readonly headers: ReadonlyMap<string, HeaderValue>
}

export type TypedPollResult<T> =
  | { readonly kind: "record"; readonly record: TypedRecord<T> }
  | { readonly kind: "error"; readonly error: TypedDecodeError }

export interface TypedContract {
  readonly contentType: ContentTypeValue
  readonly schemaId?: number
  readonly compiled?: CompiledSchema
}

export class TypedTopic<T> {
  private constructor(
    /** The untyped handle underneath, for the verbs the typed form does not wrap. */
    readonly topic: Topic,
    private readonly codec: Codec<T>,
    private readonly kind: TypedTopicKind,
    private readonly contract: TypedContract = {
      contentType: kind === "json" ? ContentType.Json : ContentType.Cbor
    }
  ) {}

  /** @internal */
  static create<T>(
    topic: Topic,
    codec: Codec<T>,
    kind: TypedTopicKind,
    contract: TypedContract = {
      contentType: kind === "json" ? ContentType.Json : ContentType.Cbor
    }
  ): TypedTopic<T> {
    return new TypedTopic(topic, codec, kind, contract)
  }

  async publish(value: T, options?: RawSendOptions): Promise<SendMessagesResponse> {
    if (options?.key !== undefined && options.partition !== undefined) {
      throw new CodecError(
        "typed publish accepts a routing key or an explicit partition, not both",
        "typed-topic",
        "publish"
      )
    }
    const payload = this.encode(value)
    const request = this.topic.publish().rawBytes(payload, this.contract.contentType)
    if (this.contract.schemaId !== undefined) request.schemaId(this.contract.schemaId)
    if (options?.key !== undefined) request.partitionKey(options.key)
    if (options?.partition !== undefined) request.partition(options.partition)
    if (options?.provenance !== undefined) request.provenance(options.provenance)
    if (options?.headers !== undefined) {
      return this.topic.send(payload, {
        ...options,
        headers: this.contractHeaders(options.headers)
      })
    }
    return request.send()
  }

  async publishBatch(
    values: readonly T[],
    options?: RawSendOptions
  ): Promise<SendMessagesResponse> {
    const payloads = values.map((value) => this.encode(value))
    const headers = this.contractHeaders(options?.headers)
    return this.topic.batch(payloads, { ...options, headers })
  }

  async records(readerName: string): Promise<TypedRecords<T>> {
    const cursor = (await this.topic.replay()).named(readerName)
    return TypedRecords.create(cursor, this.codec, this.contract.compiled)
  }

  private encode(value: T): Uint8Array {
    const payload = this.codec.encode(value)
    if (this.contract.compiled !== undefined && !this.contract.compiled.validate(payload)) {
      throw new CodecError("encoded body does not match the registered schema", "schema", "encode")
    }
    return payload
  }

  private contractHeaders(
    headers: ReadonlyMap<string, HeaderValue> | undefined
  ): ReadonlyMap<string, HeaderValue> {
    const contract = new Map(headers)
    contract.set("agdx.ct", {
      kind: "uint8",
      value: contentTypeCode(this.contract.contentType)
    })
    if (this.contract.schemaId !== undefined) {
      contract.set("agdx.sid", { kind: "uint32", value: this.contract.schemaId })
    }
    return contract
  }
}

function decodeRecord<T>(
  codec: Codec<T>,
  message: {
    readonly payload: Uint8Array
    readonly partitionId: number
    readonly offset: bigint
    readonly headers: ReadonlyMap<string, HeaderValue>
  },
  compiled?: CompiledSchema
): TypedPollResult<T> {
  try {
    if (compiled !== undefined) compiled.decode(message.payload)
    return {
      kind: "record",
      record: {
        value: codec.decode(message.payload),
        position: { partitionId: message.partitionId, offset: message.offset },
        headers: message.headers
      }
    }
  } catch (cause) {
    const source =
      cause instanceof LaserError
        ? cause
        : new CodecError("typed record payload does not decode", "typed-topic", "decode", {
            cause
          })
    return {
      kind: "error",
      error: new TypedDecodeError(
        "failed to decode typed record",
        { partitionId: message.partitionId, offset: message.offset },
        source
      )
    }
  }
}

export class TypedRecords<T> {
  private constructor(
    private readonly cursor: Cursor,
    private readonly codec: Codec<T>,
    private readonly compiled?: CompiledSchema
  ) {}

  /** @internal */
  static create<T>(cursor: Cursor, codec: Codec<T>, compiled?: CompiledSchema): TypedRecords<T> {
    return new TypedRecords(cursor, codec, compiled)
  }

  get offsets(): ReadonlyMap<number, bigint> {
    return this.cursor.offsets
  }

  fromOffsets(offsets: ReadonlyMap<number, bigint>): this {
    this.cursor.fromOffsets(offsets)
    return this
  }

  batch(size: number): this {
    this.cursor.batch(size)
    return this
  }

  async poll(options?: { readonly signal?: AbortSignal }): Promise<readonly TypedPollResult<T>[]> {
    const batch = await this.cursor.pollRecords(options)
    return batch.map((message) => decodeRecord(this.codec, message, this.compiled))
  }

  async *stream(options?: {
    readonly signal?: AbortSignal
    readonly pollIntervalMs?: number
  }): AsyncIterable<TypedPollResult<T>> {
    for await (const message of this.cursor.streamRecords(options)) {
      yield decodeRecord(this.codec, message, this.compiled)
    }
  }
}

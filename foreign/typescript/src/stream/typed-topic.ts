import {
  CancelledError,
  CodecError,
  LaserError,
  TransportError,
  TypedDecodeError
} from "../client/errors.js"
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
  private buffered: TypedPollResult<T>[] = []

  private constructor(
    private readonly cursor: Cursor,
    private readonly codec: Codec<T>,
    private readonly compiled?: CompiledSchema
  ) {}

  /** @internal */
  static create<T>(cursor: Cursor, codec: Codec<T>, compiled?: CompiledSchema): TypedRecords<T> {
    return new TypedRecords(cursor, codec, compiled)
  }

  /**
   * The next offset to read on each partition. Empty before the first poll
   * unless `fromOffsets` seeded it. Persist this to resume with `fromOffsets`.
   */
  get offsets(): ReadonlyMap<number, bigint> {
    return this.cursor.offsets
  }

  /** Resumes from offsets an earlier `offsets` returned. */
  fromOffsets(offsets: ReadonlyMap<number, bigint>): this {
    this.cursor.fromOffsets(offsets)
    return this
  }

  /** Reads at most `size` records per partition per server request (default 1000). */
  batch(size: number): this {
    this.cursor.batch(size)
    return this
  }

  /**
   * The next record decoded, or `undefined` when the reader is caught up. A
   * record that does not decode yields its positioned error and the reader
   * moves past it. A failed poll yields an error with no position, and the
   * next call polls again. Drive one reader from one task.
   */
  async next(options?: { readonly signal?: AbortSignal }): Promise<TypedPollResult<T> | undefined> {
    if (this.buffered.length === 0) {
      try {
        this.buffered = [...(await this.pollCursor(options))]
      } catch (cause) {
        if (cause instanceof CancelledError) throw cause
        return { kind: "error", error: pollFailure(cause) }
      }
    }
    return this.buffered.shift()
  }

  /**
   * One bounded poll, decoded: everything appended since the last read, each
   * a record or its positioned decode error. An empty array means caught up.
   * Records `next` already buffered come first. A failed poll throws.
   */
  async poll(options?: { readonly signal?: AbortSignal }): Promise<readonly TypedPollResult<T>[]> {
    if (this.buffered.length > 0) return this.buffered.splice(0)
    return this.pollCursor(options)
  }

  /**
   * The records one at a time, ending once caught up like Rust
   * `TypedRecords::stream`. A failed poll yields its error and ends the
   * iteration.
   */
  async *stream(options?: { readonly signal?: AbortSignal }): AsyncIterable<TypedPollResult<T>> {
    for (;;) {
      const item = await this.next(options)
      if (item === undefined) return
      yield item
      if (item.kind === "error" && item.error.position === undefined) return
    }
  }

  private async pollCursor(options?: {
    readonly signal?: AbortSignal
  }): Promise<readonly TypedPollResult<T>[]> {
    const batch = await this.cursor.pollRecords(options)
    return batch.map((message) => decodeRecord(this.codec, message, this.compiled))
  }
}

function pollFailure(cause: unknown): TypedDecodeError {
  const source =
    cause instanceof LaserError
      ? cause
      : new TransportError("typed read failed to poll", false, { cause })
  return new TypedDecodeError("typed read failed to poll", undefined, source)
}

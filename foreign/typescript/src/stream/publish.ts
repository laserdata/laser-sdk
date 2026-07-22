import type { BlobStore } from "../blob.js"
import { checkIn } from "../blob.js"
import { type BytesLike, ownedBytes } from "../client/bytes.js"
import { InvalidError } from "../client/errors.js"
import type { Provenance } from "../provenance/provenance.js"
import type { CompiledSchema } from "../schema-codecs.js"
import { ContentType, type ContentType as ContentTypeValue } from "../wire/content.js"
import type { Codec } from "./codecs.js"
import { mergeRecord, Record, recordHeaders } from "./record.js"
import type { Topic } from "./topic.js"

interface ClaimCheck {
  readonly store: BlobStore
  readonly thresholdBytes: number
}

export class PublishRequest {
  private body: Uint8Array | undefined
  private key: Uint8Array | undefined
  private partitionId: number | undefined
  private provenanceValue: Provenance | undefined
  private readonly record = new Record()
  private claimCheckValue: ClaimCheck | undefined

  constructor(private readonly topic: Topic) {}

  partitionKey(key: BytesLike): this {
    this.key = ownedBytes(key)
    this.partitionId = undefined
    return this
  }

  partition(id: number): this {
    this.partitionId = id
    this.key = undefined
    return this
  }

  provenance(provenance: Provenance): this {
    this.provenanceValue = provenance
    return this
  }

  contentType(value: ContentTypeValue): this {
    this.record.contentType(value)
    return this
  }

  projectionRef(value: string): this {
    this.record.projectionRef(value)
    return this
  }

  schemaId(value: number): this {
    this.record.schemaId(value)
    return this
  }

  index(key: string, value: string): this {
    this.record.index(key, value)
    return this
  }

  header(key: string, value: string): this {
    this.record.header(key, value)
    return this
  }

  metadata(key: string, value: string): this {
    return this.header(key, value)
  }

  inlinePayload(): this {
    this.record.inlinePayload()
    return this
  }

  payload(bytes: BytesLike): this {
    this.body = ownedBytes(bytes)
    return this
  }

  rawBytes(bytes: BytesLike, contentType: ContentTypeValue): this {
    this.body = ownedBytes(bytes)
    this.record.contentType(contentType)
    return this
  }

  encodeWith<T>(value: T, codec: Codec<T>, contentType?: ContentTypeValue): this {
    this.body = codec.encode(value)
    if (contentType !== undefined) this.record.contentType(contentType)
    return this
  }

  encode<T>(value: T, codec: Codec<T>, contentType?: ContentTypeValue): this {
    return this.encodeWith(value, codec, contentType)
  }

  json<T>(value: T, codec: Codec<T>): this {
    return this.encodeWith(value, codec, ContentType.Json)
  }

  msgpack<T>(value: T, codec: Codec<T>): this {
    return this.encodeWith(value, codec, ContentType.Msgpack)
  }

  messagePack<T>(value: T, codec: Codec<T>): this {
    return this.msgpack(value, codec)
  }

  avro(schema: CompiledSchema, schemaId: number, value: unknown): this {
    if (schema.kind !== "avro") {
      throw new InvalidError("avro() requires a compiled Avro schema")
    }
    this.body = schema.encode(value)
    this.record.contentType(ContentType.Avro).schemaId(schemaId)
    return this
  }

  claimCheck(store: BlobStore, thresholdBytes: number): this {
    if (!Number.isSafeInteger(thresholdBytes) || thresholdBytes < 0) {
      throw new InvalidError("claim-check threshold must be a non-negative safe integer")
    }
    this.claimCheckValue = { store, thresholdBytes }
    return this
  }

  async send(): Promise<void> {
    if (this.body === undefined) {
      throw new InvalidError(
        "publish() requires a body, call payload()/json()/msgpack()/rawBytes() before send()"
      )
    }
    let payload = this.body
    if (this.claimCheckValue !== undefined) {
      const checked = await checkIn(
        this.claimCheckValue.store,
        this.claimCheckValue.thresholdBytes,
        payload
      )
      payload = checked.payload
      if (checked.contentType !== undefined) this.record.contentType(checked.contentType)
    }
    await this.topic.send(payload, {
      ...(this.key !== undefined ? { key: this.key } : {}),
      ...(this.partitionId !== undefined ? { partition: this.partitionId } : {}),
      ...(this.provenanceValue !== undefined ? { provenance: this.provenanceValue } : {}),
      headers: recordHeaders(this.record)
    })
  }
}

interface BatchEntry {
  readonly payload: Uint8Array
  readonly record: Record
}

export class BatchPublishRequest {
  private readonly defaults = new Record()
  private readonly entries: BatchEntry[] = []
  private key: Uint8Array | undefined

  constructor(private readonly topic: Topic) {}

  get length(): number {
    return this.entries.length
  }

  get isEmpty(): boolean {
    return this.entries.length === 0
  }

  contentType(value: ContentTypeValue): this {
    this.defaults.contentType(value)
    return this
  }

  projectionRef(value: string): this {
    this.defaults.projectionRef(value)
    return this
  }

  schemaId(value: number): this {
    this.defaults.schemaId(value)
    return this
  }

  inlinePayload(): this {
    this.defaults.inlinePayload()
    return this
  }

  partitionKey(value: BytesLike): this {
    this.key = ownedBytes(value)
    return this
  }

  index(key: string, value: string): this {
    this.defaults.index(key, value)
    return this
  }

  header(key: string, value: string): this {
    this.defaults.header(key, value)
    return this
  }

  metadata(key: string, value: string): this {
    return this.header(key, value)
  }

  addPayload(payload: BytesLike): this {
    this.entries.push({ payload: ownedBytes(payload), record: new Record() })
    return this
  }

  addRawBytes(payload: BytesLike, contentType: ContentTypeValue): this {
    this.entries.push({
      payload: ownedBytes(payload),
      record: new Record().contentType(contentType)
    })
    return this
  }

  addEncoded<T>(value: T, codec: Codec<T>, contentType?: ContentTypeValue): this {
    const record = new Record()
    if (contentType !== undefined) record.contentType(contentType)
    this.entries.push({ payload: codec.encode(value), record })
    return this
  }

  addEncodedWithProjection<T>(
    projectionRef: string,
    value: T,
    codec: Codec<T>,
    contentType?: ContentTypeValue
  ): this {
    const record = new Record().projectionRef(projectionRef)
    if (contentType !== undefined) record.contentType(contentType)
    this.entries.push({ payload: codec.encode(value), record })
    return this
  }

  addJson<T>(value: T, codec: Codec<T>): this {
    return this.addEncoded(value, codec, ContentType.Json)
  }

  addMsgpack<T>(value: T, codec: Codec<T>): this {
    return this.addEncoded(value, codec, ContentType.Msgpack)
  }

  addMessagePack<T>(value: T, codec: Codec<T>): this {
    return this.addMsgpack(value, codec)
  }

  addJsonWithProjection<T>(projectionRef: string, value: T, codec: Codec<T>): this {
    return this.addEncodedWithProjection(projectionRef, value, codec, ContentType.Json)
  }

  addMessagePackWithProjection<T>(projectionRef: string, value: T, codec: Codec<T>): this {
    return this.addEncodedWithProjection(projectionRef, value, codec, ContentType.Msgpack)
  }

  addAvro(schema: CompiledSchema, schemaId: number, value: unknown): this {
    if (schema.kind !== "avro") {
      throw new InvalidError("addAvro() requires a compiled Avro schema")
    }
    this.entries.push({
      payload: schema.encode(value),
      record: new Record().contentType(ContentType.Avro).schemaId(schemaId)
    })
    return this
  }

  addRawBytesWithProjection(
    projectionRef: string,
    payload: BytesLike,
    contentType: ContentTypeValue
  ): this {
    this.entries.push({
      payload: ownedBytes(payload),
      record: new Record().contentType(contentType).projectionRef(projectionRef)
    })
    return this
  }

  addPayloadWithProjection(projectionRef: string, payload: BytesLike): this {
    this.entries.push({
      payload: ownedBytes(payload),
      record: new Record().projectionRef(projectionRef)
    })
    return this
  }

  extendEncoded<T>(values: Iterable<T>, codec: Codec<T>, contentType?: ContentTypeValue): this {
    for (const value of values) this.addEncoded(value, codec, contentType)
    return this
  }

  extendJson<T>(values: Iterable<T>, codec: Codec<T>): this {
    return this.extendEncoded(values, codec, ContentType.Json)
  }

  extendMessagePack<T>(values: Iterable<T>, codec: Codec<T>): this {
    return this.extendEncoded(values, codec, ContentType.Msgpack)
  }

  addRecord(payload: BytesLike, record: Record): this {
    this.entries.push({ payload: ownedBytes(payload), record })
    return this
  }

  async send(): Promise<number> {
    if (this.entries.length === 0) return 0
    const entries = this.entries.map(({ payload, record }, index) => {
      try {
        return { payload, headers: recordHeaders(mergeRecord(this.defaults, record)) }
      } catch (cause) {
        throw new InvalidError(`record #${String(index)} is invalid`, { cause })
      }
    })
    await this.topic.sendRecords(entries, this.key === undefined ? {} : { key: this.key })
    return entries.length
  }
}

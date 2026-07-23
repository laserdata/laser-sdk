import { InvalidError } from "../client/errors.js"
import type { IggyHeaderValue } from "../iggy/apache-iggy.js"
import { contentTypeCode, type ContentType } from "../wire/content.js"
import {
  CONTENT_TYPE,
  HEADER_FRAMING_BYTES,
  HEADER_SOFT_CAP,
  HEADER_VALUE_MAX,
  IDX_PREFIX,
  INLINE_PAYLOAD,
  PROJECTION_REF,
  SCHEMA_ID
} from "../wire/headers.js"
import { MAX_INDEX_ENTRIES_PER_RECORD } from "../wire/limits.js"

const encoder = new TextEncoder()

interface RecordSnapshot {
  readonly contentType?: ContentType
  readonly projectionRef?: string
  readonly schemaId?: number
  readonly inlinePayload: boolean
  readonly index: readonly (readonly [string, string])[]
  readonly metadata: readonly (readonly [string, string])[]
}

export class Record {
  private contentTypeValue: ContentType | undefined
  private projectionRefValue: string | undefined
  private schemaIdValue: number | undefined
  private shouldInlinePayload = false
  private readonly indexedValues: [string, string][] = []
  private readonly metadataValues: [string, string][] = []

  contentType(value: ContentType): this {
    this.contentTypeValue = value
    return this
  }

  projectionRef(value: string): this {
    this.projectionRefValue = value
    return this
  }

  schemaId(value: number): this {
    if (!Number.isSafeInteger(value) || value < 0 || value > 0xffff_ffff) {
      throw new InvalidError("schema id must be an unsigned 32-bit integer")
    }
    this.schemaIdValue = value
    return this
  }

  inlinePayload(): this {
    this.shouldInlinePayload = true
    return this
  }

  index(key: string, value: string): this {
    this.indexedValues.push([key, value])
    return this
  }

  header(key: string, value: string): this {
    this.metadataValues.push([key, value])
    return this
  }

  snapshot(): RecordSnapshot {
    return {
      ...(this.contentTypeValue !== undefined ? { contentType: this.contentTypeValue } : {}),
      ...(this.projectionRefValue !== undefined ? { projectionRef: this.projectionRefValue } : {}),
      ...(this.schemaIdValue !== undefined ? { schemaId: this.schemaIdValue } : {}),
      inlinePayload: this.shouldInlinePayload,
      index: this.indexedValues.map(([key, value]) => [key, value]),
      metadata: this.metadataValues.map(([key, value]) => [key, value])
    }
  }
}

function stringHeader(headers: Map<string, IggyHeaderValue>, key: string, value: string): void {
  const size = encoder.encode(value).byteLength
  if (value.length === 0) throw new InvalidError(`header \`${key}\` value must not be empty`)
  if (size > HEADER_VALUE_MAX) {
    throw new InvalidError(
      `header \`${key}\` value is ${String(size)}B, exceeds max ${String(HEADER_VALUE_MAX)}B`
    )
  }
  headers.set(key, { kind: "string", value })
}

function headerValueBytes(value: IggyHeaderValue): number {
  switch (value.kind) {
    case "raw":
    case "int128":
    case "uint128":
      return value.value.byteLength
    case "string":
      return encoder.encode(value.value).byteLength
    case "bool":
    case "int8":
    case "uint8":
      return 1
    case "int16":
    case "uint16":
      return 2
    case "int32":
    case "uint32":
    case "float":
      return 4
    case "int64":
    case "uint64":
    case "double":
      return 8
  }
}

export function recordHeaders(record: Record): ReadonlyMap<string, IggyHeaderValue> {
  const value = record.snapshot()
  const headers = new Map<string, IggyHeaderValue>()
  if (value.contentType !== undefined) {
    headers.set(CONTENT_TYPE, { kind: "uint8", value: contentTypeCode(value.contentType) })
  }
  if (value.projectionRef !== undefined) stringHeader(headers, PROJECTION_REF, value.projectionRef)
  if (value.schemaId !== undefined) {
    headers.set(SCHEMA_ID, { kind: "uint32", value: value.schemaId })
  }
  if (value.inlinePayload) headers.set(INLINE_PAYLOAD, { kind: "bool", value: true })
  if (value.index.length > MAX_INDEX_ENTRIES_PER_RECORD) {
    throw new InvalidError(
      `record has ${String(value.index.length)} indexed scalars, exceeds cap of ${String(MAX_INDEX_ENTRIES_PER_RECORD)}`
    )
  }
  for (const [key, indexed] of value.index) {
    if (key.length === 0) throw new InvalidError("index key must not be empty")
    if (key.startsWith(IDX_PREFIX)) {
      throw new InvalidError(
        `index key \`${key}\` must not start with the reserved \`${IDX_PREFIX}\` prefix`
      )
    }
    stringHeader(headers, `${IDX_PREFIX}${key}`, indexed)
  }
  for (const [key, metadata] of value.metadata) {
    if (key.startsWith(IDX_PREFIX)) {
      throw new InvalidError(
        `metadata header \`${key}\` collides with the \`${IDX_PREFIX}\` namespace, use index() instead`
      )
    }
    if ([CONTENT_TYPE, SCHEMA_ID, PROJECTION_REF, INLINE_PAYLOAD].includes(key)) {
      throw new InvalidError(
        `metadata header \`${key}\` is reserved, use the dedicated builder method`
      )
    }
    stringHeader(headers, key, metadata)
  }
  let size = 0
  for (const [key, header] of headers) {
    size += encoder.encode(key).byteLength + headerValueBytes(header) + HEADER_FRAMING_BYTES
  }
  if (size > HEADER_SOFT_CAP) {
    throw new InvalidError(
      `record headers ${String(size)}B exceed soft cap ${String(HEADER_SOFT_CAP)}B`
    )
  }
  return headers
}

export function mergeRecord(defaults: Record, override: Record): Record {
  const base = defaults.snapshot()
  const own = override.snapshot()
  const merged = new Record()
  const contentType = own.contentType ?? base.contentType
  const projectionRef = own.projectionRef ?? base.projectionRef
  const schemaId = own.schemaId ?? base.schemaId
  if (contentType !== undefined) merged.contentType(contentType)
  if (projectionRef !== undefined) merged.projectionRef(projectionRef)
  if (schemaId !== undefined) merged.schemaId(schemaId)
  if (base.inlinePayload || own.inlinePayload) merged.inlinePayload()
  for (const [key, value] of base.index) merged.index(key, value)
  for (const [key, value] of own.index) merged.index(key, value)
  for (const [key, value] of base.metadata) merged.header(key, value)
  for (const [key, value] of own.metadata) merged.header(key, value)
  return merged
}

import { assertNever } from "../client/errors.js"

// The wire codec tag stamped on `agdx.ct`. Ported from `wire/src/content.rs`.
// The codes are a growable dictionary: an unknown code decodes to `undefined`
// rather than an error, and the caller treats that body as opaque rather than
// rejecting the record.
export const ContentType = {
  Any: "any",
  Raw: "raw",
  Json: "json",
  Avro: "avro",
  Protobuf: "protobuf",
  Msgpack: "msgpack",
  Cbor: "cbor",
  Bson: "bson",
  Arrow: "arrow",
  Ref: "ref"
} as const

export type ContentType = (typeof ContentType)[keyof typeof ContentType]

export function isRawContentType(value: ContentType): boolean {
  return value === ContentType.Raw
}

export function contentTypeCode(value: ContentType): number {
  switch (value) {
    case ContentType.Raw:
      return 0
    case ContentType.Json:
      return 1
    case ContentType.Msgpack:
      return 2
    case ContentType.Cbor:
      return 3
    case ContentType.Bson:
      return 4
    case ContentType.Avro:
      return 5
    case ContentType.Protobuf:
      return 6
    case ContentType.Arrow:
      return 7
    case ContentType.Ref:
      return 8
    case ContentType.Any:
      return 255
    default:
      return assertNever(value)
  }
}

const CONTENT_TYPE_BY_CODE: ReadonlyMap<number, ContentType> = new Map(
  Object.values(ContentType).map((contentType) => [contentTypeCode(contentType), contentType])
)

export function contentTypeFromCode(code: number): ContentType | undefined {
  return CONTENT_TYPE_BY_CODE.get(code)
}

import { decode as decodeMessagePack, encode as encodeMessagePack } from "@msgpack/msgpack"

import { CodecError } from "../client/errors.js"
import { decodeOne, encodeOne } from "../wire/cbor.js"
import { ContentType } from "../wire/content.js"
import type { KvEntry } from "../wire/kv.js"
import { decodeBsonDocument, encodeBsonDocument } from "./bson.js"

/** The decode half of a codec. */
export interface Decoder<T> {
  decode(payload: Uint8Array): T
}

/**
 * Encoding strategy for a typed body. `contentType` is the tag stamped on
 * `agdx.ct` so consumers can decode downstream.
 */
export interface Codec<T> extends Decoder<T> {
  readonly contentType: ContentType
  encode(value: T): Uint8Array
}

/**
 * The built-in JSON codec. `decodeValue` checks a decoded value and narrows it
 * to `T`, and the codec passes the decoded value through without it.
 */
export class Json<T = unknown> implements Codec<T> {
  readonly contentType: ContentType = ContentType.Json

  constructor(private readonly decodeValue: (value: unknown) => T = (value) => value as T) {}

  encode(value: T): Uint8Array {
    try {
      if (value === undefined || typeof value === "function" || typeof value === "symbol") {
        throw new TypeError("value has no JSON representation")
      }
      return new TextEncoder().encode(JSON.stringify(value))
    } catch (cause) {
      throw new CodecError("value does not encode as JSON", "json", "encode", { cause })
    }
  }

  decode(payload: Uint8Array): T {
    let value: unknown
    try {
      value = JSON.parse(new TextDecoder().decode(payload)) as unknown
    } catch (cause) {
      throw new CodecError("payload does not decode as JSON", "json", "decode", { cause })
    }
    return this.decodeValue(value)
  }
}

/** The built-in CBOR codec. `decodeValue` narrows a decoded value to `T`. */
export class Cbor<T = unknown> implements Codec<T> {
  readonly contentType: ContentType = ContentType.Cbor

  constructor(private readonly decodeValue: (value: unknown) => T = (value) => value as T) {}

  encode(value: T): Uint8Array {
    try {
      return encodeOne(value)
    } catch (cause) {
      throw new CodecError("value does not encode as CBOR", "cbor", "encode", { cause })
    }
  }

  decode(payload: Uint8Array): T {
    return this.decodeValue(lowerCborValue(decodeOne(payload, "typed CBOR payload")))
  }
}

/**
 * The built-in MessagePack codec, with named-map encoding so field names
 * round-trip with JSON-shaped consumers. `decodeValue` narrows a decoded value
 * to `T`.
 */
export class Msgpack<T = unknown> implements Codec<T> {
  readonly contentType: ContentType = ContentType.Msgpack

  constructor(private readonly decodeValue: (value: unknown) => T = (value) => value as T) {}

  encode(value: T): Uint8Array {
    try {
      return encodeMessagePack(messagePackValue(value), { useBigInt64: true })
    } catch (cause) {
      throw new CodecError("value does not encode as MessagePack", "msgpack", "encode", {
        cause
      })
    }
  }

  decode(payload: Uint8Array): T {
    let value: unknown
    try {
      value = decodeMessagePack(payload, { useBigInt64: true })
    } catch (cause) {
      throw new CodecError("payload does not decode as MessagePack", "msgpack", "decode", {
        cause
      })
    }
    return this.decodeValue(value)
  }
}

/**
 * The built-in BSON codec. The top-level body must be a document (an object or
 * a string-keyed map). `decodeValue` narrows a decoded value to `T`.
 */
export class Bson<T = unknown> implements Codec<T> {
  readonly contentType: ContentType = ContentType.Bson

  constructor(private readonly decodeValue: (value: unknown) => T = (value) => value as T) {}

  encode(value: T): Uint8Array {
    try {
      return encodeBsonDocument(value)
    } catch (cause) {
      throw new CodecError("value does not encode as BSON", "bson", "encode", { cause })
    }
  }

  decode(payload: Uint8Array): T {
    let value: unknown
    try {
      value = decodeBsonDocument(payload)
    } catch (cause) {
      throw new CodecError("payload does not decode as BSON", "bson", "decode", { cause })
    }
    return this.decodeValue(value)
  }
}

/** Decode a key-value entry's value as JSON, narrowed by `decodeValue` when given. */
export function kvEntryDecodeValue<T = unknown>(
  entry: KvEntry,
  decodeValue?: (value: unknown) => T
): T {
  return new Json(decodeValue).decode(entry.value)
}

/** Decode a key-value entry's value with any decoder. */
export function kvEntryDecodeValueWith<T>(entry: KvEntry, decoder: Decoder<T>): T {
  return decoder.decode(entry.value)
}

function lowerCborValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(lowerCborValue)
  if (value instanceof Map) {
    const entries = Array.from(value.entries())
    if (entries.every(([key]) => typeof key === "string")) {
      return Object.fromEntries(entries.map(([key, nested]) => [key, lowerCborValue(nested)]))
    }
    return new Map(entries.map(([key, nested]) => [key, lowerCborValue(nested)]))
  }
  return value
}

function messagePackValue(value: unknown, depth = 0): unknown {
  if (depth > 100) throw new TypeError("MessagePack value exceeds the maximum depth")
  if (typeof value === "bigint") {
    if (value < -(1n << 63n) || value > (1n << 64n) - 1n)
      throw new RangeError("MessagePack integers must fit in signed or unsigned 64 bits")
    return value >= -0x8000_0000n && value <= 0xffff_ffffn ? Number(value) : value
  }
  if (typeof value === "number" && Number.isSafeInteger(value)) {
    return value < -0x8000_0000 || value > 0xffff_ffff ? BigInt(value) : value
  }
  if (Array.isArray(value)) return value.map((nested) => messagePackValue(nested, depth + 1))
  if (value instanceof Map) {
    const entries = [...value.entries()] as readonly [unknown, unknown][]
    if (!entries.every(([key]) => typeof key === "string"))
      throw new TypeError("MessagePack object keys must be strings")
    return Object.fromEntries(
      entries.map(([key, nested]) => [key, messagePackValue(nested, depth + 1)])
    )
  }
  if (
    typeof value === "object" &&
    value !== null &&
    !ArrayBuffer.isView(value) &&
    !(value instanceof Date)
  ) {
    return Object.fromEntries(
      Object.entries(value).map(([key, nested]) => [key, messagePackValue(nested, depth + 1)])
    )
  }
  return value
}

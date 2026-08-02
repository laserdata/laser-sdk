import { decode as decodeMessagePack, encode as encodeMessagePack } from "@msgpack/msgpack"

import { CodecError } from "../client/errors.js"
import { decodeOne, encodeOne } from "../wire/cbor.js"

export interface Codec<T> {
  encode(value: T): Uint8Array
  decode(bytes: Uint8Array): T
}

export type ValueDecoder<T> = (value: unknown) => T

function encodeJson(value: unknown): Uint8Array {
  try {
    if (value === undefined || typeof value === "function" || typeof value === "symbol") {
      throw new TypeError("value has no JSON representation")
    }
    const json = JSON.stringify(value)
    return new TextEncoder().encode(json)
  } catch (cause) {
    throw new CodecError("value does not encode as JSON", "json", "encode", { cause })
  }
}

function decodeJson(bytes: Uint8Array): unknown {
  try {
    return JSON.parse(new TextDecoder().decode(bytes)) as unknown
  } catch (cause) {
    throw new CodecError("payload does not decode as JSON", "json", "decode", { cause })
  }
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

export function jsonCodec<T>(decodeValue: ValueDecoder<T>): Codec<T> {
  return {
    encode: encodeJson,
    decode: (bytes) => decodeValue(decodeJson(bytes))
  }
}

export function cborCodec<T>(decodeValue: ValueDecoder<T>): Codec<T> {
  return {
    encode(value): Uint8Array {
      try {
        return encodeOne(value)
      } catch (cause) {
        throw new CodecError("value does not encode as CBOR", "cbor", "encode", { cause })
      }
    },
    decode(bytes): T {
      return decodeValue(lowerCborValue(decodeOne(bytes, "typed CBOR payload")))
    }
  }
}

export function messagePackCodec<T>(decodeValue: ValueDecoder<T>): Codec<T> {
  return {
    encode(value): Uint8Array {
      try {
        return encodeMessagePack(value, { useBigInt64: true })
      } catch (cause) {
        throw new CodecError("value does not encode as MessagePack", "msgpack", "encode", {
          cause
        })
      }
    },
    decode(bytes): T {
      try {
        return decodeValue(decodeMessagePack(bytes, { useBigInt64: true }))
      } catch (cause) {
        if (cause instanceof CodecError) throw cause
        throw new CodecError("payload does not decode as MessagePack", "msgpack", "decode", {
          cause
        })
      }
    }
  }
}

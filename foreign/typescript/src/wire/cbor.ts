import { encode as cborgEncode, decode as cborgDecode, Token, Type } from "cborg"
import { CodecError } from "../client/errors.js"

const DECODE_OPTIONS = {
  useMaps: true,
  allowBigInt: true,
  rejectDuplicateMapKeys: true,
  strict: true
} as const

const ENCODE_OPTIONS = {
  mapSorter: () => 0
} as const

// Forces every plain JS `number` in the encode call to the CBOR float major
// type, matching ciborium's behavior for a Rust `f32`/`f64` field. cborg's
// own default only float-encodes a number that "has a fractional part"
// (`1.0` and `1` are indistinguishable in JS), so a whole-number float
// value like a default edge `weight` of `1.0` would otherwise silently
// encode as an integer. Only usable when every OTHER numeric value in the
// same encode call is a `bigint`, never a plain `number` (this option
// applies to every number in the whole nested tree, not just one field).
// See `graph.ts`'s float-bearing encoders for the one place this is used.
const FORCE_FLOAT_ENCODE_OPTIONS = {
  mapSorter: () => 0,
  typeEncoders: {
    number: (value: number) => [new Token(Type.float, value)]
  }
} as const

export type CborMap = ReadonlyMap<unknown, unknown>

export function encodeNamed(
  entries: ReadonlyMap<string, unknown>,
  options?: { readonly forceFloatNumbers?: boolean }
): Uint8Array {
  return cborgEncode(
    entries,
    options?.forceFloatNumbers === true ? FORCE_FLOAT_ENCODE_OPTIONS : ENCODE_OPTIONS
  )
}

// The encode counterpart to `decodeOne`: any single CBOR value at the top
// level, not just a named map. Most wire replies are map-shaped (handled by
// `encodeNamed`), but a bare unit-variant reply (e.g. an `Ok` tagged as a
// plain string) encodes directly with no wrapping map.
export function encodeOne(value: unknown): Uint8Array {
  return cborgEncode(value, ENCODE_OPTIONS)
}

export function decodeOne(bytes: Uint8Array, context: string): unknown {
  try {
    return cborgDecode(bytes, DECODE_OPTIONS)
  } catch (cause) {
    throw new CodecError(`failed to decode ${context}`, context, "decode", { cause })
  }
}

export function expectMap(value: unknown, context: string): CborMap {
  if (!(value instanceof Map)) {
    throw new CodecError(`expected a CBOR map for ${context}`, context, "expect-map")
  }
  return value
}

function requireField(map: CborMap, key: string, context: string): unknown {
  if (!map.has(key)) {
    throw new CodecError(`missing required field \`${key}\` in ${context}`, context, key)
  }
  return map.get(key)
}

export const field = {
  requiredString(map: CborMap, key: string, context: string): string {
    const value = requireField(map, key, context)
    if (typeof value !== "string") {
      throw new CodecError(`field \`${key}\` in ${context} must be a string`, context, key)
    }
    return value
  },

  optionalString(map: CborMap, key: string, context: string): string | undefined {
    if (!map.has(key)) return undefined
    const value = map.get(key)
    if (typeof value !== "string") {
      throw new CodecError(`field \`${key}\` in ${context} must be a string`, context, key)
    }
    return value
  },

  requiredBytes(map: CborMap, key: string, context: string): Uint8Array {
    const value = requireField(map, key, context)
    if (!(value instanceof Uint8Array)) {
      throw new CodecError(`field \`${key}\` in ${context} must be bytes`, context, key)
    }
    return value
  },

  optionalBytes(map: CborMap, key: string, context: string): Uint8Array | undefined {
    if (!map.has(key)) return undefined
    const value = map.get(key)
    if (!(value instanceof Uint8Array)) {
      throw new CodecError(`field \`${key}\` in ${context} must be bytes`, context, key)
    }
    return value
  },

  requiredMap(map: CborMap, key: string, context: string): CborMap {
    return expectMap(requireField(map, key, context), `${context}.${key}`)
  },

  optionalMap(map: CborMap, key: string, context: string): CborMap | undefined {
    if (!map.has(key)) return undefined
    return expectMap(map.get(key), `${context}.${key}`)
  },

  requiredU64(map: CborMap, key: string, context: string): bigint {
    const value = requireField(map, key, context)
    return coerceU64(value, key, context)
  },

  requiredU128(map: CborMap, key: string, context: string): bigint {
    const value = requireField(map, key, context)
    return coerceU128(value, key, context)
  },

  optionalU64(map: CborMap, key: string, context: string): bigint | undefined {
    if (!map.has(key)) return undefined
    return coerceU64(map.get(key), key, context)
  },

  requiredU32(map: CborMap, key: string, context: string): number {
    const value = requireField(map, key, context)
    return coerceU32(value, key, context)
  },

  optionalU32(map: CborMap, key: string, context: string): number | undefined {
    if (!map.has(key)) return undefined
    return coerceU32(map.get(key), key, context)
  },

  requiredU8(map: CborMap, key: string, context: string): number {
    const value = requireField(map, key, context)
    return coerceUint(value, key, context, 0xff)
  },

  optionalU8(map: CborMap, key: string, context: string): number | undefined {
    if (!map.has(key)) return undefined
    return coerceUint(map.get(key), key, context, 0xff)
  },

  optionalU16(map: CborMap, key: string, context: string): number | undefined {
    if (!map.has(key)) return undefined
    return coerceUint(map.get(key), key, context, 0xffff)
  },

  optionalF64(map: CborMap, key: string, context: string): number | undefined {
    if (!map.has(key)) return undefined
    const value = map.get(key)
    if (typeof value !== "number") {
      throw new CodecError(`field \`${key}\` in ${context} must be a number`, context, key)
    }
    return value
  },

  requiredBoolean(map: CborMap, key: string, context: string): boolean {
    const value = requireField(map, key, context)
    if (typeof value !== "boolean") {
      throw new CodecError(`field \`${key}\` in ${context} must be a boolean`, context, key)
    }
    return value
  },

  optionalBoolean(map: CborMap, key: string, context: string): boolean | undefined {
    if (!map.has(key)) return undefined
    const value = map.get(key)
    if (typeof value !== "boolean") {
      throw new CodecError(`field \`${key}\` in ${context} must be a boolean`, context, key)
    }
    return value
  },

  requiredArray<T>(
    map: CborMap,
    key: string,
    context: string,
    decodeItem: (item: unknown, index: number) => T
  ): T[] {
    const value = requireField(map, key, context)
    if (!Array.isArray(value)) {
      throw new CodecError(`field \`${key}\` in ${context} must be an array`, context, key)
    }
    return value.map((item, index) => decodeItem(item, index))
  },

  optionalArray<T>(
    map: CborMap,
    key: string,
    context: string,
    decodeItem: (item: unknown, index: number) => T
  ): T[] {
    if (!map.has(key)) return []
    const value = map.get(key)
    if (!Array.isArray(value)) {
      throw new CodecError(`field \`${key}\` in ${context} must be an array`, context, key)
    }
    return value.map((item, index) => decodeItem(item, index))
  }
}

// Rust's default externally-tagged enum representation: a map with exactly
// one entry, keyed by the variant name. Shared by every additive
// request/reply enum ported so far (`agent-workflow.ts`, `authz.ts`, and
// beyond) rather than re-implemented per module.
export function singleVariantTag(value: unknown, context: string): readonly [string, unknown] {
  const map = expectMap(value, context)
  if (map.size !== 1) {
    throw new CodecError(`expected exactly one variant tag in ${context}`, context, "variant")
  }
  const [first] = Array.from(map.entries())
  if (first === undefined) {
    throw new CodecError(`expected exactly one variant tag in ${context}`, context, "variant")
  }
  const [tag, inner] = first
  if (typeof tag !== "string") {
    throw new CodecError(`variant tag in ${context} must be a string`, context, "variant")
  }
  return [tag, inner]
}

export function expectString(value: unknown, context: string): string {
  if (typeof value !== "string") {
    throw new CodecError(`expected a string in ${context}`, context, "value")
  }
  return value
}

export function expectBoolean(value: unknown, context: string): boolean {
  if (typeof value !== "boolean") {
    throw new CodecError(`expected a boolean in ${context}`, context, "value")
  }
  return value
}

export function expectArray(value: unknown, context: string): readonly unknown[] {
  if (!Array.isArray(value)) {
    throw new CodecError(`expected an array in ${context}`, context, "value")
  }
  return value
}

// A bare unsigned integer value, not read from a map field: the tuple-
// variant payload of an externally-tagged enum (e.g. `CasExpect::Match(u64)`).
export function expectU64(value: unknown, context: string): bigint {
  return coerceU64(value, "value", context)
}

export function expectU32(value: unknown, context: string): number {
  return coerceU32(value, "value", context)
}

const U32_MAX = 2 ** 32 - 1

function coerceU32(value: unknown, key: string, context: string): number {
  return coerceUint(value, key, context, U32_MAX)
}

function coerceUint(value: unknown, key: string, context: string, max: number): number {
  if (typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= max) {
    return value
  }
  throw new CodecError(
    `field \`${key}\` in ${context} must be an unsigned integer at most ${String(max)}`,
    context,
    key
  )
}

function coerceU64(value: unknown, key: string, context: string): bigint {
  if (typeof value === "bigint") {
    if (value < 0n || value > (1n << 64n) - 1n) {
      throw new CodecError(`field \`${key}\` in ${context} must fit u64`, context, key)
    }
    return value
  }
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) {
    return BigInt(value)
  }
  throw new CodecError(`field \`${key}\` in ${context} must be an unsigned integer`, context, key)
}

function coerceU128(value: unknown, key: string, context: string): bigint {
  if (typeof value === "bigint" && value >= 0n && value <= (1n << 128n) - 1n) return value
  if (typeof value === "number" && Number.isSafeInteger(value) && value >= 0) return BigInt(value)
  throw new CodecError(`field \`${key}\` in ${context} must fit u128`, context, key)
}

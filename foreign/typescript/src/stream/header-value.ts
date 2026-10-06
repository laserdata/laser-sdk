import { InvalidError } from "../client/errors.js"

function integer(name: string, value: number, minimum: number, maximum: number): number {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new InvalidError(`${name} header value is outside ${String(minimum)}..${String(maximum)}`)
  }
  return value
}

function bigint(name: string, value: bigint, minimum: bigint, maximum: bigint): bigint {
  if (value < minimum || value > maximum) {
    throw new InvalidError(`${name} header value is outside its integer range`)
  }
  return value
}

function integer128(name: string, value: Uint8Array): Uint8Array {
  if (value.byteLength !== 16) throw new InvalidError(`${name} header value must be 16 bytes`)
  return value.slice()
}

function floating(name: string, value: number): number {
  if (!Number.isFinite(value)) throw new InvalidError(`${name} header value must be finite`)
  return value
}

/** Represents every Apache Iggy user-header kind. */
export type HeaderValue =
  | { readonly kind: "raw"; readonly value: Uint8Array }
  | { readonly kind: "string"; readonly value: string }
  | { readonly kind: "bool"; readonly value: boolean }
  | { readonly kind: "int8"; readonly value: number }
  | { readonly kind: "int16"; readonly value: number }
  | { readonly kind: "int32"; readonly value: number }
  | { readonly kind: "int64"; readonly value: bigint }
  | { readonly kind: "int128"; readonly value: Uint8Array }
  | { readonly kind: "uint8"; readonly value: number }
  | { readonly kind: "uint16"; readonly value: number }
  | { readonly kind: "uint32"; readonly value: number }
  | { readonly kind: "uint64"; readonly value: bigint }
  | { readonly kind: "uint128"; readonly value: Uint8Array }
  | { readonly kind: "float"; readonly value: number }
  | { readonly kind: "double"; readonly value: number }

export const HeaderValue = {
  string: (value: string): HeaderValue => ({ kind: "string", value }),
  bool: (value: boolean): HeaderValue => ({ kind: "bool", value }),
  int8: (value: number): HeaderValue => ({
    kind: "int8",
    value: integer("int8", value, -128, 127)
  }),
  int16: (value: number): HeaderValue => ({
    kind: "int16",
    value: integer("int16", value, -32_768, 32_767)
  }),
  int32: (value: number): HeaderValue => ({
    kind: "int32",
    value: integer("int32", value, -2_147_483_648, 2_147_483_647)
  }),
  int64: (value: bigint): HeaderValue => ({
    kind: "int64",
    value: bigint("int64", value, -(1n << 63n), (1n << 63n) - 1n)
  }),
  int128: (value: Uint8Array): HeaderValue => ({
    kind: "int128",
    value: integer128("int128", value)
  }),
  uint8: (value: number): HeaderValue => ({
    kind: "uint8",
    value: integer("uint8", value, 0, 255)
  }),
  uint16: (value: number): HeaderValue => ({
    kind: "uint16",
    value: integer("uint16", value, 0, 65_535)
  }),
  uint32: (value: number): HeaderValue => ({
    kind: "uint32",
    value: integer("uint32", value, 0, 4_294_967_295)
  }),
  uint64: (value: bigint): HeaderValue => ({
    kind: "uint64",
    value: bigint("uint64", value, 0n, (1n << 64n) - 1n)
  }),
  uint128: (value: Uint8Array): HeaderValue => ({
    kind: "uint128",
    value: integer128("uint128", value)
  }),
  float: (value: number): HeaderValue => ({ kind: "float", value: floating("float", value) }),
  double: (value: number): HeaderValue => ({ kind: "double", value: floating("double", value) })
} as const

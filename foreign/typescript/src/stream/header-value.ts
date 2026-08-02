import type { IggyHeaderValue } from "../iggy/apache-iggy.js"
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

export const HeaderValue = {
  string: (value: string): IggyHeaderValue => ({ kind: "string", value }),
  bool: (value: boolean): IggyHeaderValue => ({ kind: "bool", value }),
  int8: (value: number): IggyHeaderValue => ({
    kind: "int8",
    value: integer("int8", value, -128, 127)
  }),
  int16: (value: number): IggyHeaderValue => ({
    kind: "int16",
    value: integer("int16", value, -32_768, 32_767)
  }),
  int32: (value: number): IggyHeaderValue => ({
    kind: "int32",
    value: integer("int32", value, -2_147_483_648, 2_147_483_647)
  }),
  int64: (value: bigint): IggyHeaderValue => ({
    kind: "int64",
    value: bigint("int64", value, -(1n << 63n), (1n << 63n) - 1n)
  }),
  int128: (value: Uint8Array): IggyHeaderValue => ({
    kind: "int128",
    value: integer128("int128", value)
  }),
  uint8: (value: number): IggyHeaderValue => ({
    kind: "uint8",
    value: integer("uint8", value, 0, 255)
  }),
  uint16: (value: number): IggyHeaderValue => ({
    kind: "uint16",
    value: integer("uint16", value, 0, 65_535)
  }),
  uint32: (value: number): IggyHeaderValue => ({
    kind: "uint32",
    value: integer("uint32", value, 0, 4_294_967_295)
  }),
  uint64: (value: bigint): IggyHeaderValue => ({
    kind: "uint64",
    value: bigint("uint64", value, 0n, (1n << 64n) - 1n)
  }),
  uint128: (value: Uint8Array): IggyHeaderValue => ({
    kind: "uint128",
    value: integer128("uint128", value)
  }),
  float: (value: number): IggyHeaderValue => ({ kind: "float", value: floating("float", value) }),
  double: (value: number): IggyHeaderValue => ({ kind: "double", value: floating("double", value) })
} as const

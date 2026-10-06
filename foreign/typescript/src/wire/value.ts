import { CodecError } from "../client/errors.js"

export type Value =
  | { readonly kind: "str"; readonly value: string }
  /** A signed integer within the i64 range. */
  | { readonly kind: "int"; readonly value: bigint }
  /** An unsigned integer past the i64 range. */
  | { readonly kind: "uint"; readonly value: bigint }
  | { readonly kind: "float"; readonly value: number }
  | { readonly kind: "bool"; readonly value: boolean }
  | { readonly kind: "null" }
  | { readonly kind: "list"; readonly value: readonly Value[] }

const INTEGER_INPUT = /^[+-]?\d+$/
const DECIMAL_INPUT = /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/
const I64_MIN = -(1n << 63n)
const I64_MAX = (1n << 63n) - 1n
const U64_MAX = (1n << 64n) - 1n

export function valueFromInput(input: string): Value {
  if (input === "null") return { kind: "null" }
  if (input === "true") return { kind: "bool", value: true }
  if (input === "false") return { kind: "bool", value: false }
  if (INTEGER_INPUT.test(input)) {
    const negative = input.startsWith("-")
    const magnitude = BigInt(input.replace(/^[+-]/, ""))
    const integer = negative ? -magnitude : magnitude
    if (integer >= I64_MIN && integer <= I64_MAX) return { kind: "int", value: integer }
    if (integer > I64_MAX && integer <= U64_MAX) return { kind: "uint", value: integer }
  }
  if (DECIMAL_INPUT.test(input)) return { kind: "float", value: Number(input) }
  return { kind: "str", value: input }
}

export function encodeValue(value: Value): unknown {
  switch (value.kind) {
    case "str":
      return value.value
    case "int":
    case "uint":
      return value.value
    case "float":
      return value.value
    case "bool":
      return value.value
    case "null":
      return null
    case "list":
      return value.value.map(encodeValue)
  }
}

export function decodeValue(raw: unknown, context: string): Value {
  if (typeof raw === "string") return { kind: "str", value: raw }
  if (typeof raw === "boolean") return { kind: "bool", value: raw }
  if (raw === null) return { kind: "null" }
  if (typeof raw === "bigint")
    return raw > I64_MAX ? { kind: "uint", value: raw } : { kind: "int", value: raw }
  if (typeof raw === "number") {
    return Number.isInteger(raw)
      ? { kind: "int", value: BigInt(raw) }
      : { kind: "float", value: raw }
  }
  if (Array.isArray(raw)) {
    return {
      kind: "list",
      value: raw.map((item, index) => decodeValue(item, `${context}[${String(index)}]`))
    }
  }
  throw new CodecError(`cannot decode value in ${context}`, context, "value")
}

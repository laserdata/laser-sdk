import { CodecError } from "../client/errors.js"

export type Value =
  | { readonly kind: "string"; readonly value: string }
  | { readonly kind: "int"; readonly value: bigint }
  | { readonly kind: "float"; readonly value: number }
  | { readonly kind: "bool"; readonly value: boolean }
  | { readonly kind: "null" }
  | { readonly kind: "list"; readonly value: readonly Value[] }

export function encodeValue(value: Value): unknown {
  switch (value.kind) {
    case "string":
      return value.value
    case "int":
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
  if (typeof raw === "string") return { kind: "string", value: raw }
  if (typeof raw === "boolean") return { kind: "bool", value: raw }
  if (raw === null) return { kind: "null" }
  if (typeof raw === "bigint") return { kind: "int", value: raw }
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

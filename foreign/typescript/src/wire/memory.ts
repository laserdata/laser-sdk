import { CodecError } from "../client/errors.js"
import { encodeNamed, expectMap, field, singleVariantTag, type CborMap } from "./cbor.js"

export type MemoryRecord =
  | {
      readonly kind: "item"
      readonly id: string
      readonly memoryKind: string
      readonly body: Uint8Array
    }
  | { readonly kind: "forget"; readonly target: string }
  | { readonly kind: "feedback"; readonly target: string; readonly weight: number }

export function encodeMemoryRecord(record: MemoryRecord): Map<string, unknown> {
  switch (record.kind) {
    case "item":
      return new Map([
        [
          "Item",
          new Map<string, unknown>([
            ["id", record.id],
            ["kind", record.memoryKind],
            ["body", Array.from(record.body, (byte) => BigInt(byte))]
          ])
        ]
      ])
    case "forget":
      return new Map([["Forget", new Map([["target", record.target]])]])
    case "feedback":
      return new Map([
        [
          "Feedback",
          new Map<string, unknown>([
            ["target", record.target],
            ["weight", record.weight]
          ])
        ]
      ])
  }
}

export function decodeMemoryRecord(value: unknown, context: string): MemoryRecord {
  const [tag, inner] = singleVariantTag(value, context)
  const map = expectMap(inner, context)
  switch (tag) {
    case "Item":
      return {
        kind: "item",
        id: field.requiredString(map, "id", context),
        memoryKind: field.requiredString(map, "kind", context),
        body: decodeByteArray(map, "body", context)
      }
    case "Forget":
      return { kind: "forget", target: field.requiredString(map, "target", context) }
    case "Feedback": {
      const weight = map.get("weight")
      if (typeof weight !== "number") {
        throw new CodecError(`field \`weight\` in ${context} must be a number`, context, "weight")
      }
      return { kind: "feedback", target: field.requiredString(map, "target", context), weight }
    }
    default:
      throw new CodecError(
        `\`${tag}\` is not a recognized memory record variant`,
        context,
        "record"
      )
  }
}

export function encodeMemoryRecordFrame(record: MemoryRecord): Uint8Array {
  return encodeNamed(encodeMemoryRecord(record), { forceFloatNumbers: true })
}

function decodeByteArray(map: CborMap, key: string, context: string): Uint8Array {
  return Uint8Array.from(
    field.requiredArray(map, key, context, (item, index) => {
      if (typeof item !== "number" || !Number.isInteger(item) || item < 0 || item > 0xff) {
        throw new CodecError(`${context}.${key}[${String(index)}] must fit u8`, context, key)
      }
      return item
    })
  )
}

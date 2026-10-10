import { CodecError } from "../client/errors.js"
import { encodeNamed, expectMap, field, singleVariantTag, type CborMap } from "./cbor.js"
import {
  decodeProducer,
  decodeSourceRef,
  encodeProducer,
  encodeSourceRef,
  type ProducerInfo,
  type SourceRef
} from "./graph.js"

export type MemoryRecord =
  | {
      readonly kind: "item"
      readonly id: string
      readonly memoryKind: string
      readonly body: Uint8Array
      readonly origin?: SourceRef
      readonly producer?: ProducerInfo
    }
  | { readonly kind: "forget"; readonly target: string; readonly conversation?: string }
  | {
      readonly kind: "feedback"
      readonly target: string
      readonly weight: number
      readonly conversation?: string
    }

export function encodeMemoryRecord(record: MemoryRecord): Map<string, unknown> {
  switch (record.kind) {
    case "item": {
      const item = new Map<string, unknown>([
        ["id", record.id],
        ["kind", record.memoryKind],
        ["body", Array.from(record.body, (byte) => BigInt(byte))]
      ])
      if (record.origin !== undefined) item.set("origin", encodeSourceRef(record.origin))
      if (record.producer !== undefined) item.set("producer", encodeProducer(record.producer))
      return new Map([["Item", item]])
    }
    case "forget": {
      const forget = new Map<string, unknown>([["target", record.target]])
      if (record.conversation !== undefined) forget.set("conversation", record.conversation)
      return new Map([["Forget", forget]])
    }
    case "feedback": {
      const feedback = new Map<string, unknown>([
        ["target", record.target],
        ["weight", record.weight]
      ])
      if (record.conversation !== undefined) feedback.set("conversation", record.conversation)
      return new Map([["Feedback", feedback]])
    }
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
        body: decodeByteArray(map, "body", context),
        ...(map.has("origin")
          ? { origin: decodeSourceRef(map.get("origin"), `${context}.origin`) }
          : {}),
        ...(map.has("producer")
          ? {
              producer: decodeProducer(
                field.requiredMap(map, "producer", context),
                `${context}.producer`
              )
            }
          : {})
      }
    case "Forget": {
      const conversation = field.optionalString(map, "conversation", context)
      return {
        kind: "forget",
        target: field.requiredString(map, "target", context),
        ...(conversation !== undefined ? { conversation } : {})
      }
    }
    case "Feedback": {
      const weight = map.get("weight")
      if (typeof weight !== "number") {
        throw new CodecError(`field \`weight\` in ${context} must be a number`, context, "weight")
      }
      const conversation = field.optionalString(map, "conversation", context)
      return {
        kind: "feedback",
        target: field.requiredString(map, "target", context),
        weight,
        ...(conversation !== undefined ? { conversation } : {})
      }
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

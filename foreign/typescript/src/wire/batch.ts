import { CodecError } from "../client/errors.js"
import { type CborMap, expectMap, field } from "./cbor.js"

export interface BatchItem {
  readonly code: number
  readonly payload: Uint8Array
}

export interface BatchRequest {
  readonly v: number
  readonly ops: readonly BatchItem[]
}

export interface BatchReply {
  readonly results: readonly Uint8Array[]
}

export function encodeBatchRequest(request: BatchRequest): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", BigInt(request.v)],
    [
      "ops",
      request.ops.map(
        (item) =>
          new Map<string, unknown>([
            ["code", BigInt(item.code)],
            ["payload", item.payload]
          ])
      )
    ]
  ])
}

export function decodeBatchRequest(map: CborMap, context: string): BatchRequest {
  return {
    v: field.requiredU32(map, "v", context),
    ops: field.requiredArray(map, "ops", context, (item, index) => {
      const itemContext = `${context}.ops[${String(index)}]`
      const itemMap = expectMap(item, itemContext)
      return {
        code: field.requiredU32(itemMap, "code", itemContext),
        payload: field.requiredBytes(itemMap, "payload", itemContext)
      }
    })
  }
}

export function encodeBatchReply(reply: BatchReply): Map<string, unknown> {
  return new Map([["results", [...reply.results]]])
}

export function decodeBatchReply(map: CborMap, context: string): BatchReply {
  return {
    results: field.requiredArray(map, "results", context, (item, index) => {
      if (!(item instanceof Uint8Array)) {
        throw new CodecError(
          `${context}.results[${String(index)}] must be bytes`,
          context,
          "results"
        )
      }
      return item
    })
  }
}

import { type CborMap, field } from "./cbor.js"
import { BackendResourceId } from "./ids.js"

export interface BackendBinding {
  readonly resourceId: BackendResourceId
  readonly generation: bigint
}

function mapOf(entries: readonly (readonly [string, unknown])[]): Map<string, unknown> {
  return new Map(entries)
}
export function encodeBackendBinding(value: BackendBinding): Map<string, unknown> {
  return mapOf([
    ["resource_id", value.resourceId.toBytes()],
    ["generation", value.generation]
  ])
}
export function decodeBackendBinding(map: CborMap, context: string): BackendBinding {
  return {
    resourceId: BackendResourceId.fromBytes(field.requiredBytes(map, "resource_id", context)),
    generation: field.requiredU64(map, "generation", context)
  }
}

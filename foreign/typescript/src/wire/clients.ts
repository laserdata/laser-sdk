import { type CborMap, expectMap, field } from "./cbor.js"

export interface ClientMetadataQuery {
  readonly v: number
  readonly withMetadataOnly: boolean
  readonly userId?: number
  readonly afterClientId?: number
  readonly limit: number
}

export interface ClientMetadata {
  readonly clientId: number
  readonly userId?: number
  readonly transport: number
  readonly address: string
  readonly consumerGroupsCount: number
  readonly metadata?: Uint8Array
}

export interface ClientMetadataList {
  readonly clients: readonly ClientMetadata[]
  readonly nextCursor?: number
}

export function encodeClientMetadataQuery(query: ClientMetadataQuery): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", BigInt(query.v))
  if (query.withMetadataOnly) map.set("with_metadata_only", true)
  if (query.userId !== undefined) map.set("user_id", BigInt(query.userId))
  if (query.afterClientId !== undefined) map.set("after_client_id", BigInt(query.afterClientId))
  map.set("limit", BigInt(query.limit))
  return map
}

export function decodeClientMetadataQuery(map: CborMap, context: string): ClientMetadataQuery {
  const userId = field.optionalU32(map, "user_id", context)
  const afterClientId = field.optionalU32(map, "after_client_id", context)
  return {
    v: field.requiredU32(map, "v", context),
    withMetadataOnly: field.optionalBoolean(map, "with_metadata_only", context) ?? false,
    ...(userId !== undefined ? { userId } : {}),
    ...(afterClientId !== undefined ? { afterClientId } : {}),
    limit: field.requiredU32(map, "limit", context)
  }
}

export function encodeClientMetadata(metadata: ClientMetadata): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("client_id", BigInt(metadata.clientId))
  if (metadata.userId !== undefined) map.set("user_id", BigInt(metadata.userId))
  map.set("transport", BigInt(metadata.transport))
  map.set("address", metadata.address)
  map.set("consumer_groups_count", BigInt(metadata.consumerGroupsCount))
  if (metadata.metadata !== undefined) map.set("metadata", metadata.metadata)
  return map
}

export function decodeClientMetadata(map: CborMap, context: string): ClientMetadata {
  const userId = field.optionalU32(map, "user_id", context)
  const metadata = field.optionalBytes(map, "metadata", context)
  return {
    clientId: field.requiredU32(map, "client_id", context),
    ...(userId !== undefined ? { userId } : {}),
    transport: field.requiredU8(map, "transport", context),
    address: field.requiredString(map, "address", context),
    consumerGroupsCount: field.requiredU32(map, "consumer_groups_count", context),
    ...(metadata !== undefined ? { metadata } : {})
  }
}

export function encodeClientMetadataList(list: ClientMetadataList): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["clients", list.clients.map((metadata) => encodeClientMetadata(metadata))]
  ])
  if (list.nextCursor !== undefined) map.set("next_cursor", BigInt(list.nextCursor))
  return map
}

export function decodeClientMetadataList(map: CborMap, context: string): ClientMetadataList {
  const nextCursor = field.optionalU32(map, "next_cursor", context)
  return {
    clients: field.requiredArray(map, "clients", context, (item, index) =>
      decodeClientMetadata(
        expectMap(item, `${context}.clients[${String(index)}]`),
        `${context}.clients[${String(index)}]`
      )
    ),
    ...(nextCursor !== undefined ? { nextCursor } : {})
  }
}

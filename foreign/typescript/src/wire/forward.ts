import { CodecError } from "../client/errors.js"
import { decodeGrant, encodeGrant, type Grant } from "./authz.js"
import { type CborMap, expectMap, field } from "./cbor.js"

export interface ForwardedQuery {
  readonly userId: number
  readonly clientId: bigint
  readonly correlation?: string
  readonly queryEnvelope: Uint8Array
  readonly grants: readonly Grant[]
}

export interface ForwardedCommand {
  readonly userId: number
  readonly clientId: bigint
  readonly correlation?: string
  readonly readAll: boolean
  readonly commandCode: number
  readonly payload: Uint8Array
  readonly grants: readonly Grant[]
}

function requireU128(value: bigint, context: string): bigint {
  if (value > (1n << 128n) - 1n) {
    throw new CodecError(`${context} must fit in u128`, context, "client_id")
  }
  return value
}

function encodeGrants(grants: readonly Grant[]): readonly Map<string, unknown>[] {
  return grants.map((grant) => encodeGrant(grant))
}

function decodeGrants(map: CborMap, context: string): Grant[] {
  return field.optionalArray(map, "grants", context, (item, index) =>
    decodeGrant(
      expectMap(item, `${context}.grants[${String(index)}]`),
      `${context}.grants[${String(index)}]`
    )
  )
}

export function encodeForwardedQuery(query: ForwardedQuery): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["user_id", BigInt(query.userId)],
    ["client_id", requireU128(query.clientId, "ForwardedQuery.clientId")],
    ["correlation", query.correlation ?? null]
  ])
  map.set("query_envelope", query.queryEnvelope)
  if (query.grants.length > 0) map.set("grants", encodeGrants(query.grants))
  return map
}

export function decodeForwardedQuery(map: CborMap, context: string): ForwardedQuery {
  const correlation = decodeCorrelation(map, context)
  return {
    userId: field.requiredU32(map, "user_id", context),
    clientId: field.requiredU128(map, "client_id", context),
    ...(correlation !== undefined ? { correlation } : {}),
    queryEnvelope: field.requiredBytes(map, "query_envelope", context),
    grants: decodeGrants(map, context)
  }
}

export function encodeForwardedCommand(command: ForwardedCommand): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["user_id", BigInt(command.userId)],
    ["client_id", requireU128(command.clientId, "ForwardedCommand.clientId")],
    ["correlation", command.correlation ?? null]
  ])
  map.set("read_all", command.readAll)
  map.set("command_code", BigInt(command.commandCode))
  map.set("payload", command.payload)
  if (command.grants.length > 0) map.set("grants", encodeGrants(command.grants))
  return map
}

export function decodeForwardedCommand(map: CborMap, context: string): ForwardedCommand {
  const correlation = decodeCorrelation(map, context)
  return {
    userId: field.requiredU32(map, "user_id", context),
    clientId: field.requiredU128(map, "client_id", context),
    ...(correlation !== undefined ? { correlation } : {}),
    readAll: field.optionalBoolean(map, "read_all", context) ?? false,
    commandCode: field.requiredU32(map, "command_code", context),
    payload: field.requiredBytes(map, "payload", context),
    grants: decodeGrants(map, context)
  }
}

function decodeCorrelation(map: CborMap, context: string): string | undefined {
  if (!map.has("correlation")) return undefined
  const value = map.get("correlation")
  if (value === null) return undefined
  if (typeof value === "string") return value
  throw new CodecError(
    `field \`correlation\` in ${context} must be a string or null`,
    context,
    "correlation"
  )
}

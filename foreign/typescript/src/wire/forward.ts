import { CodecError } from "../client/errors.js"
import { decodeGrant, encodeGrant, type Grant, type Tenancy } from "./authz.js"
import { type CborMap, expectMap, field } from "./cbor.js"
import {
  type ForwardedScope,
  type MutationPosition,
  type SessionPosition,
  decodeForwardedScope,
  decodeMutationPosition,
  decodeSessionPosition,
  encodeForwardedScope,
  encodeMutationPosition,
  encodeSessionPosition
} from "./mutation.js"

export { type ForwardedScope, decodeForwardedScope, encodeForwardedScope } from "./mutation.js"

export interface ForwardedQuery {
  readonly userId: number
  readonly clientId: bigint
  readonly correlation?: string
  readonly queryEnvelope: Uint8Array
  readonly grants: readonly Grant[]
  /** Absent for deployment tenancy, as Rust skips the default. */
  readonly tenancy?: Tenancy
  /** The stream of the queried index, stamped after a native read check. */
  readonly scope?: ForwardedScope
}

export interface ForwardedCommand {
  readonly userId: number
  readonly clientId: bigint
  readonly correlation?: string
  readonly operationId?: bigint
  readonly readAll: boolean
  readonly scope?: ForwardedScope
  readonly commandCode: number
  readonly payload: Uint8Array
  readonly grants: readonly Grant[]
  readonly readBarrier: boolean
  readonly minPosition?: MutationPosition
  readonly positionedReply: boolean
  readonly minSessions: readonly SessionPosition[]
  /** Absent for deployment tenancy, as Rust skips the default. */
  readonly tenancy?: Tenancy
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
  if (query.tenancy === "stream") map.set("tenancy", query.tenancy)
  if (query.scope !== undefined) map.set("scope", encodeForwardedScope(query.scope))
  return map
}

export function decodeForwardedQuery(map: CborMap, context: string): ForwardedQuery {
  const correlation = decodeCorrelation(map, context)
  const tenancy = decodeTenancy(map, context)
  const scopeMap = field.optionalMap(map, "scope", context)
  return {
    userId: field.requiredU32(map, "user_id", context),
    clientId: field.requiredU128(map, "client_id", context),
    ...(correlation !== undefined ? { correlation } : {}),
    queryEnvelope: field.requiredBytes(map, "query_envelope", context),
    grants: decodeGrants(map, context),
    ...(tenancy !== undefined ? { tenancy } : {}),
    ...(scopeMap !== undefined ? { scope: decodeForwardedScope(scopeMap, `${context}.scope`) } : {})
  }
}

export function encodeForwardedCommand(command: ForwardedCommand): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["user_id", BigInt(command.userId)],
    ["client_id", requireU128(command.clientId, "ForwardedCommand.clientId")],
    ["correlation", command.correlation ?? null]
  ])
  if (command.operationId !== undefined) {
    map.set("operation_id", requireU128(command.operationId, "ForwardedCommand.operationId"))
  }
  map.set("read_all", command.readAll)
  if (command.scope !== undefined) map.set("scope", encodeForwardedScope(command.scope))
  map.set("command_code", BigInt(command.commandCode))
  map.set("payload", command.payload)
  if (command.grants.length > 0) map.set("grants", encodeGrants(command.grants))
  if (command.readBarrier) map.set("read_barrier", true)
  if (command.minPosition !== undefined) {
    map.set("min_position", encodeMutationPosition(command.minPosition))
  }
  if (command.positionedReply) map.set("positioned_reply", true)
  if (command.minSessions.length > 0) {
    map.set("min_sessions", command.minSessions.map(encodeSessionPosition))
  }
  if (command.tenancy === "stream") map.set("tenancy", command.tenancy)
  return map
}

export function decodeForwardedCommand(map: CborMap, context: string): ForwardedCommand {
  const correlation = decodeCorrelation(map, context)
  const operationId = field.optionalU128(map, "operation_id", context)
  const scopeMap = field.optionalMap(map, "scope", context)
  const tenancy = decodeTenancy(map, context)
  const minPosition = field.optionalMap(map, "min_position", context)
  return {
    userId: field.requiredU32(map, "user_id", context),
    clientId: field.requiredU128(map, "client_id", context),
    ...(correlation !== undefined ? { correlation } : {}),
    ...(operationId !== undefined ? { operationId } : {}),
    readAll: field.optionalBoolean(map, "read_all", context) ?? false,
    ...(scopeMap !== undefined
      ? { scope: decodeForwardedScope(scopeMap, `${context}.scope`) }
      : {}),
    commandCode: field.requiredU32(map, "command_code", context),
    payload: field.requiredBytes(map, "payload", context),
    grants: decodeGrants(map, context),
    readBarrier: field.optionalBoolean(map, "read_barrier", context) ?? false,
    ...(minPosition !== undefined
      ? { minPosition: decodeMutationPosition(minPosition, `${context}.min_position`) }
      : {}),
    positionedReply: field.optionalBoolean(map, "positioned_reply", context) ?? false,
    minSessions: field.optionalArray(map, "min_sessions", context, (item, index) => {
      const itemContext = `${context}.min_sessions[${String(index)}]`
      return decodeSessionPosition(expectMap(item, itemContext), itemContext)
    }),
    ...(tenancy !== undefined ? { tenancy } : {})
  }
}

function decodeTenancy(map: CborMap, context: string): Tenancy | undefined {
  const tenancy = field.optionalString(map, "tenancy", context)
  if (tenancy === undefined || tenancy === "deployment") return undefined
  if (tenancy === "stream") return tenancy
  throw new CodecError(`unknown tenancy \`${tenancy}\``, context, "tenancy")
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

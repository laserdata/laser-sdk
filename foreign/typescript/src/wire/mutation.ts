import { CodecError } from "../client/errors.js"
import { type CborMap, expectMap, field } from "./cbor.js"
import {
  AGDX_FORK_CREATE_CODE,
  AGDX_FORK_DELETE_CODE,
  AGDX_FORK_LIST_CODE,
  AGDX_FORK_PROMOTE_CODE,
  AGDX_FORK_PUT_CODE,
  AGDX_GRAPH_NEIGHBORS_CODE,
  AGDX_GRAPH_QUERY_CODE,
  AGDX_GRAPH_UPSERT_CODE,
  AGDX_KV_CAS_CODE,
  AGDX_KV_CAS_FENCED_CODE,
  AGDX_KV_COPY_CODE,
  AGDX_KV_DELETE_CODE,
  AGDX_KV_DELETE_MANY_CODE,
  AGDX_KV_EXISTS_CODE,
  AGDX_KV_EXPIRE_CODE,
  AGDX_KV_GET_CODE,
  AGDX_KV_LEASE_CODE,
  AGDX_KV_LEASE_RENEW_CODE,
  AGDX_KV_MOVE_CODE,
  AGDX_KV_NAMESPACES_CODE,
  AGDX_KV_PATCH_CODE,
  AGDX_KV_RELEASE_CODE,
  AGDX_KV_SCAN_CODE,
  AGDX_KV_SET_CODE
} from "./codes.js"

export interface ForwardedScope {
  readonly streamId: number
  readonly stream: string
  readonly streamCreatedAtMicros: bigint
}

export function encodeForwardedScope(scope: ForwardedScope): Map<string, unknown> {
  return new Map<string, unknown>([
    ["stream_id", BigInt(scope.streamId)],
    ["stream", scope.stream],
    ["stream_created_at_micros", scope.streamCreatedAtMicros]
  ])
}

export function decodeForwardedScope(map: CborMap, context: string): ForwardedScope {
  return {
    streamId: field.requiredU32(map, "stream_id", context),
    stream: field.requiredString(map, "stream", context),
    streamCreatedAtMicros: field.requiredU64(map, "stream_created_at_micros", context)
  }
}

export const MANAGED_REQUEST_VERSION = 1

export interface ManagedRequestEnvelope {
  readonly v: number
  readonly operationId: bigint
  readonly payload: Uint8Array
}

export function encodeManagedRequestEnvelope(
  envelope: ManagedRequestEnvelope
): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", BigInt(envelope.v)],
    ["operation_id", envelope.operationId],
    ["payload", envelope.payload]
  ])
}

export function decodeManagedRequestEnvelope(
  map: CborMap,
  context: string
): ManagedRequestEnvelope {
  const version = field.requiredU32(map, "v", context)
  if (version !== MANAGED_REQUEST_VERSION) {
    throw new CodecError("unsupported managed request version", "managed", "v")
  }
  const operationId = field.requiredU128(map, "operation_id", context)
  if (operationId === 0n) {
    throw new CodecError("managed request operation id must not be zero", "managed", "operation_id")
  }
  return {
    v: version,
    operationId,
    payload: field.requiredBytes(map, "payload", context)
  }
}

/**
 * A managed read sent with a consistency requirement, as Rust
 * `ManagedReadEnvelope`. With `minPosition` the answering node serves the read
 * only once its fold for the read's surface has applied that position. With
 * `logHead` it first waits until its fold has applied the surface's log head,
 * which a client asks for after its managed requests moved to another node.
 */
export interface ManagedReadEnvelope {
  readonly v: number
  readonly read: Uint8Array
  readonly minPosition?: MutationPosition
  readonly logHead?: boolean
  /** For a session read: positions the session fold must have applied. */
  readonly minSessions?: readonly SessionPosition[]
}

/**
 * How far a node's session fold has applied one partition of a session
 * source, as Rust `SessionPosition`.
 */
export interface SessionPosition {
  readonly streamId: number
  readonly streamGeneration: bigint
  readonly topic: string
  readonly topicGeneration: bigint
  readonly partitionId: number
  readonly offset: bigint
}

export function encodeSessionPosition(position: SessionPosition): Map<string, unknown> {
  return new Map<string, unknown>([
    ["stream_id", BigInt(position.streamId)],
    ["stream_generation", position.streamGeneration],
    ["topic", position.topic],
    ["topic_generation", position.topicGeneration],
    ["partition_id", BigInt(position.partitionId)],
    ["offset", position.offset]
  ])
}

export function decodeSessionPosition(map: CborMap, context: string): SessionPosition {
  refuseUnknownFields(
    map,
    ["stream_id", "stream_generation", "topic", "topic_generation", "partition_id", "offset"],
    context
  )
  return {
    streamId: field.requiredU32(map, "stream_id", context),
    streamGeneration: field.requiredU64(map, "stream_generation", context),
    topic: field.requiredString(map, "topic", context),
    topicGeneration: field.requiredU64(map, "topic_generation", context),
    partitionId: field.requiredU32(map, "partition_id", context),
    offset: field.requiredU64(map, "offset", context)
  }
}

function decodeSessionPositions(map: CborMap, key: string, context: string): SessionPosition[] {
  return field.optionalArray(map, key, context, (item, index) =>
    decodeSessionPosition(expectMap(item, `${context}.${key}[${String(index)}]`), context)
  )
}

export function encodeManagedReadEnvelope(envelope: ManagedReadEnvelope): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["v", BigInt(envelope.v)],
    ["read", envelope.read]
  ])
  if (envelope.minPosition !== undefined) {
    map.set("min_position", encodeMutationPosition(envelope.minPosition))
  }
  if (envelope.logHead === true) map.set("log_head", true)
  if (envelope.minSessions !== undefined && envelope.minSessions.length > 0) {
    map.set("min_sessions", envelope.minSessions.map(encodeSessionPosition))
  }
  return map
}

export function decodeManagedReadEnvelope(map: CborMap, context: string): ManagedReadEnvelope {
  refuseUnknownFields(map, ["v", "read", "min_position", "log_head", "min_sessions"], context)
  const minPosition = field.optionalMap(map, "min_position", context)
  const logHead = field.optionalBoolean(map, "log_head", context)
  const minSessions = decodeSessionPositions(map, "min_sessions", context)
  return {
    v: field.requiredU32(map, "v", context),
    read: field.requiredBytes(map, "read", context),
    ...(minPosition !== undefined
      ? { minPosition: decodeMutationPosition(minPosition, `${context}.min_position`) }
      : {}),
    ...(logHead === true ? { logHead } : {}),
    ...(minSessions.length > 0 ? { minSessions } : {})
  }
}

/** A managed surface with its own mutation log and fold. */
export type PositionSurface = "kv" | "fork" | "graph"

/**
 * The surface a managed read or mutation acts on, as Rust
 * `PositionSurface::of_code`, or `undefined` outside the three mutation logs.
 */
export function positionSurfaceOf(code: number): PositionSurface | undefined {
  switch (code) {
    case AGDX_KV_GET_CODE:
    case AGDX_KV_SCAN_CODE:
    case AGDX_KV_NAMESPACES_CODE:
    case AGDX_KV_EXISTS_CODE:
    case AGDX_KV_SET_CODE:
    case AGDX_KV_CAS_CODE:
    case AGDX_KV_DELETE_CODE:
    case AGDX_KV_DELETE_MANY_CODE:
    case AGDX_KV_EXPIRE_CODE:
    case AGDX_KV_PATCH_CODE:
    case AGDX_KV_COPY_CODE:
    case AGDX_KV_MOVE_CODE:
    case AGDX_KV_CAS_FENCED_CODE:
    case AGDX_KV_LEASE_CODE:
    case AGDX_KV_LEASE_RENEW_CODE:
    case AGDX_KV_RELEASE_CODE:
      return "kv"
    case AGDX_FORK_LIST_CODE:
    case AGDX_FORK_CREATE_CODE:
    case AGDX_FORK_DELETE_CODE:
    case AGDX_FORK_PROMOTE_CODE:
    case AGDX_FORK_PUT_CODE:
      return "fork"
    case AGDX_GRAPH_QUERY_CODE:
    case AGDX_GRAPH_NEIGHBORS_CODE:
    case AGDX_GRAPH_UPSERT_CODE:
      return "graph"
    default:
      return undefined
  }
}

/**
 * The positions a node's folds had applied when it answered, per surface.
 * `sessions` covers the stream of a session read.
 */
export interface AppliedFrontier {
  readonly kv?: MutationPosition
  readonly fork?: MutationPosition
  readonly graph?: MutationPosition
  readonly sessions?: readonly SessionPosition[]
}

/**
 * Whether `position` is past `other` on the same surface. A later topic
 * generation is past every position of an earlier one.
 */
export function isAfter(position: MutationPosition, other: MutationPosition): boolean {
  if (position.topicGeneration !== other.topicGeneration) {
    return position.topicGeneration > other.topicGeneration
  }
  return position.offset > other.offset
}

/** Each surface at the later of its position in `seen` and in `other`. */
export function mergeFrontier(seen: AppliedFrontier, other: AppliedFrontier): AppliedFrontier {
  const later = (
    current: MutationPosition | undefined,
    next: MutationPosition | undefined
  ): MutationPosition | undefined =>
    next !== undefined && (current === undefined || isAfter(next, current)) ? next : current
  const kv = later(seen.kv, other.kv)
  const fork = later(seen.fork, other.fork)
  const graph = later(seen.graph, other.graph)
  const sessions = mergeSessionPositions(seen.sessions ?? [], other.sessions ?? [])
  return {
    ...(kv !== undefined ? { kv } : {}),
    ...(fork !== undefined ? { fork } : {}),
    ...(graph !== undefined ? { graph } : {}),
    ...(sessions.length > 0 ? { sessions } : {})
  }
}

/**
 * Each session source partition at its later position, as Rust
 * `merge_frontier`. A newer incarnation of a stream replaces every position of
 * the one before it.
 */
function mergeSessionPositions(
  seen: readonly SessionPosition[],
  answered: readonly SessionPosition[]
): SessionPosition[] {
  let merged = [...seen]
  const order = (position: SessionPosition): readonly bigint[] => [
    position.streamGeneration,
    position.topicGeneration,
    position.offset
  ]
  const later = (left: readonly bigint[], right: readonly bigint[]): boolean => {
    for (let index = 0; index < left.length; index += 1) {
      const a = left[index] ?? 0n
      const b = right[index] ?? 0n
      if (a !== b) return a > b
    }
    return false
  }
  for (const position of answered) {
    merged = merged.filter(
      (kept) =>
        kept.streamId !== position.streamId || kept.streamGeneration >= position.streamGeneration
    )
    const index = merged.findIndex(
      (kept) =>
        kept.streamId === position.streamId &&
        kept.topic === position.topic &&
        kept.partitionId === position.partitionId
    )
    const kept = index >= 0 ? merged[index] : undefined
    if (kept === undefined) merged.push(position)
    else if (later(order(position), order(kept))) merged[index] = position
  }
  return merged
}

export function encodeAppliedFrontier(frontier: AppliedFrontier): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (frontier.kv !== undefined) map.set("kv", encodeMutationPosition(frontier.kv))
  if (frontier.fork !== undefined) map.set("fork", encodeMutationPosition(frontier.fork))
  if (frontier.graph !== undefined) map.set("graph", encodeMutationPosition(frontier.graph))
  if (frontier.sessions !== undefined && frontier.sessions.length > 0) {
    map.set("sessions", frontier.sessions.map(encodeSessionPosition))
  }
  return map
}

export function decodeAppliedFrontier(map: CborMap, context: string): AppliedFrontier {
  refuseUnknownFields(map, ["kv", "fork", "graph", "sessions"], context)
  const at = (key: PositionSurface): MutationPosition | undefined => {
    const position = field.optionalMap(map, key, context)
    return position === undefined
      ? undefined
      : decodeMutationPosition(position, `${context}.${key}`)
  }
  const kv = at("kv")
  const fork = at("fork")
  const graph = at("graph")
  const sessions = decodeSessionPositions(map, "sessions", context)
  return {
    ...(kv !== undefined ? { kv } : {}),
    ...(fork !== undefined ? { fork } : {}),
    ...(graph !== undefined ? { graph } : {}),
    ...(sessions.length > 0 ? { sessions } : {})
  }
}

/**
 * The reply to a managed request an SDK client sent, as Rust
 * `ManagedReplyEnvelope`: the surface reply plus the frontier the answering
 * node had applied once it computed it.
 */
export interface ManagedReplyEnvelope {
  readonly v: number
  readonly frontier: AppliedFrontier
  readonly reply: Uint8Array
}

export function encodeManagedReplyEnvelope(envelope: ManagedReplyEnvelope): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", BigInt(envelope.v)],
    ["frontier", encodeAppliedFrontier(envelope.frontier)],
    ["reply", envelope.reply]
  ])
}

/** Decodes a positioned reply. Unknown fields are refused, so no surface
 * reply is ever taken for one. */
export function decodeManagedReplyEnvelope(map: CborMap, context: string): ManagedReplyEnvelope {
  refuseUnknownFields(map, ["v", "frontier", "reply"], context)
  const frontier = field.optionalMap(map, "frontier", context)
  return {
    v: field.requiredU32(map, "v", context),
    frontier: frontier === undefined ? {} : decodeAppliedFrontier(frontier, `${context}.frontier`),
    reply: field.requiredBytes(map, "reply", context)
  }
}

function refuseUnknownFields(map: CborMap, known: readonly string[], context: string): void {
  for (const key of map.keys()) {
    if (typeof key !== "string" || !known.includes(key)) {
      throw new CodecError(`unknown field ${String(key)}`, context, String(key))
    }
  }
}

/** Position of one applied mutation on the KV mutation topic: the fold
 * coordinate a barriered read waits for. Positions compare only within one
 * `topicGeneration`. A generation mismatch is fail-closed. */
export interface MutationPosition {
  readonly topicGeneration: bigint
  readonly partition: number
  readonly offset: bigint
}

export function encodeMutationPosition(position: MutationPosition): Map<string, unknown> {
  return new Map<string, unknown>([
    ["topic_generation", position.topicGeneration],
    ["partition", BigInt(position.partition)],
    ["offset", position.offset]
  ])
}

export function decodeMutationPosition(map: CborMap, context: string): MutationPosition {
  return {
    topicGeneration: field.requiredU64(map, "topic_generation", context),
    partition: field.requiredU32(map, "partition", context),
    offset: field.requiredU64(map, "offset", context)
  }
}

export interface MutationCommandEnvelope {
  readonly v: number
  readonly operationId: bigint
  readonly actorUserId: number
  readonly deploymentId: bigint
  readonly timestampMicros: bigint
  readonly commandCode: number
  readonly scope?: ForwardedScope
  readonly payload: Uint8Array
}

export function encodeMutationCommandEnvelope(
  envelope: MutationCommandEnvelope
): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["v", BigInt(envelope.v)],
    ["operation_id", envelope.operationId],
    ["actor_user_id", BigInt(envelope.actorUserId)],
    ["deployment_id", envelope.deploymentId],
    ["timestamp_micros", envelope.timestampMicros],
    ["command_code", BigInt(envelope.commandCode)]
  ])
  if (envelope.scope !== undefined) map.set("scope", encodeForwardedScope(envelope.scope))
  map.set("payload", envelope.payload)
  return map
}

export function decodeMutationCommandEnvelope(
  map: CborMap,
  context: string
): MutationCommandEnvelope {
  const version = field.requiredU32(map, "v", context)
  if (version === 0) {
    throw new CodecError("mutation command version must not be zero", "mutation", "v")
  }
  const operationId = field.requiredU128(map, "operation_id", context)
  const scopeMap = field.optionalMap(map, "scope", context)
  if (operationId === 0n) {
    throw new CodecError(
      "mutation command operation id must not be zero",
      "mutation",
      "operation_id"
    )
  }
  return {
    v: version,
    actorUserId: field.requiredU32(map, "actor_user_id", context),
    deploymentId: field.requiredU64(map, "deployment_id", context),
    operationId,
    timestampMicros: field.requiredU64(map, "timestamp_micros", context),
    commandCode: field.requiredU32(map, "command_code", context),
    ...(scopeMap !== undefined
      ? { scope: decodeForwardedScope(scopeMap, `${context}.scope`) }
      : {}),
    payload: field.requiredBytes(map, "payload", context)
  }
}

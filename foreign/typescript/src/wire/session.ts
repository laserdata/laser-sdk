import { CodecError, InvalidError } from "../client/errors.js"
import {
  type AgentId,
  type Budget,
  type SdkInfo,
  type SessionStatus,
  type TokenUsage,
  cborToJson,
  decodeBudget,
  decodeSdkInfo,
  decodeTokenUsage,
  encodeBudget,
  encodeSdkInfo,
  encodeTokenUsage,
  jsonToCbor,
  parseAgentId,
  sessionStatusFromWire
} from "./agent.js"
import {
  type CborMap,
  expectMap,
  expectArray,
  expectU32,
  expectU64,
  field,
  singleVariantTag
} from "./cbor.js"
import { type SourceRef, decodeSourceRef, encodeSourceRef } from "./graph.js"
import { MAX_HEARTBEAT_SESSIONS } from "./limits.js"
import type { ResultCode, ResultCodeName } from "./result.js"
import {
  ConversationId,
  CorrelationId,
  type LogPosition,
  logPositionFromBytes,
  logPositionToBytes
} from "./ids.js"

export type LinkSurface = "memory" | "kv" | "graph_node" | "graph_edge" | "projection" | "child"

export type LinkRelation = "wrote" | "recalled" | "touched"

function encodeU32(value: number, name: string): bigint {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff)
    throw new InvalidError(`${name} must fit an unsigned 32-bit integer`)
  return BigInt(value)
}

function encodeU64(value: bigint, name: string): bigint {
  if (typeof value !== "bigint" || value < 0n || value > 0xffff_ffff_ffff_ffffn)
    throw new InvalidError(`${name} must fit an unsigned 64-bit integer`)
  return value
}

function decodeSurface(value: string, context: string): LinkSurface {
  if (
    value === "memory" ||
    value === "kv" ||
    value === "graph_node" ||
    value === "graph_edge" ||
    value === "projection" ||
    value === "child"
  )
    return value
  throw new CodecError(`unknown session link surface ${value}`, context, "surface")
}

function encodeIdentity(stream: string, id: ConversationId): Map<string, unknown> {
  return new Map<string, unknown>([
    ["stream", stream],
    ["id", id.toBytes()]
  ])
}

function decodeIdentity(map: CborMap, context: string): { stream: string; id: ConversationId } {
  return {
    stream: field.requiredString(map, "stream", context),
    id: ConversationId.fromBytes(field.requiredBytes(map, "id", context))
  }
}

export interface SessionGet {
  readonly stream: string
  readonly id: ConversationId
}

export const encodeSessionGet = (request: SessionGet): Map<string, unknown> =>
  encodeIdentity(request.stream, request.id)
export const decodeSessionGet = (map: CborMap, context: string): SessionGet =>
  decodeIdentity(map, context)

export interface SessionList {
  readonly stream: string
  readonly status?: SessionStatus
  /** Only sessions in the tree rooted at this session. */
  readonly root?: ConversationId
  /** Only sessions whose label starts with this prefix. */
  readonly labelPrefix?: string
  readonly agent?: AgentId
  readonly text?: string
  readonly cursor?: string
  /** The page size. Zero leaves it to the server. */
  readonly limit: number
  readonly wantTotal: boolean
}

export function encodeSessionList(request: SessionList): Map<string, unknown> {
  const map = new Map<string, unknown>([["stream", request.stream]])
  if (request.status !== undefined) map.set("status", sessionStatusFromWire(request.status))
  if (request.root !== undefined) map.set("root", request.root.toBytes())
  if (request.labelPrefix !== undefined) map.set("label_prefix", request.labelPrefix)
  if (request.agent !== undefined) map.set("agent", parseAgentId(request.agent))
  if (request.text !== undefined) map.set("text", request.text)
  if (request.cursor !== undefined) map.set("cursor", request.cursor)
  map.set("limit", encodeU32(request.limit, "session list limit"))
  map.set("want_total", request.wantTotal)
  return map
}

export function decodeSessionList(map: CborMap, context: string): SessionList {
  const status = field.optionalString(map, "status", context)
  const root = field.optionalBytes(map, "root", context)
  const labelPrefix = field.optionalString(map, "label_prefix", context)
  const agent = field.optionalString(map, "agent", context)
  const text = field.optionalString(map, "text", context)
  const cursor = field.optionalString(map, "cursor", context)
  return {
    stream: field.requiredString(map, "stream", context),
    ...(status !== undefined ? { status: sessionStatusFromWire(status) } : {}),
    ...(root !== undefined ? { root: ConversationId.fromBytes(root) } : {}),
    ...(labelPrefix !== undefined ? { labelPrefix } : {}),
    ...(agent !== undefined ? { agent: parseAgentId(agent) } : {}),
    ...(text !== undefined ? { text } : {}),
    ...(cursor !== undefined ? { cursor } : {}),
    limit: field.requiredU32(map, "limit", context),
    wantTotal: field.requiredBoolean(map, "want_total", context)
  }
}

export interface SessionEvents {
  readonly stream: string
  readonly id: ConversationId
  readonly cursor?: string
  /** The page size. Zero leaves it to the server. */
  readonly limit: number
  readonly fixedFrontier: boolean
}

export function encodeSessionEvents(request: SessionEvents): Map<string, unknown> {
  const map = encodeIdentity(request.stream, request.id)
  if (request.cursor !== undefined) map.set("cursor", request.cursor)
  map.set("limit", encodeU32(request.limit, "session events limit"))
  map.set("fixed_frontier", request.fixedFrontier)
  return map
}

export function decodeSessionEvents(map: CborMap, context: string): SessionEvents {
  const identity = decodeIdentity(map, context)
  const cursor = field.optionalString(map, "cursor", context)
  return {
    ...identity,
    ...(cursor !== undefined ? { cursor } : {}),
    limit: field.requiredU32(map, "limit", context),
    fixedFrontier: field.requiredBoolean(map, "fixed_frontier", context)
  }
}

export interface SessionState {
  readonly stream: string
  readonly id: ConversationId
  /** The most history rows. Zero leaves it to the server. */
  readonly historyLimit: number
}

export function encodeSessionState(request: SessionState): Map<string, unknown> {
  const map = encodeIdentity(request.stream, request.id)
  map.set("history_limit", encodeU32(request.historyLimit, "session state history limit"))
  return map
}

export function decodeSessionState(map: CborMap, context: string): SessionState {
  return {
    ...decodeIdentity(map, context),
    historyLimit: field.requiredU32(map, "history_limit", context)
  }
}

export interface SessionLinks {
  readonly stream: string
  readonly id: ConversationId
  readonly surface?: LinkSurface
}

export function encodeSessionLinks(request: SessionLinks): Map<string, unknown> {
  const map = encodeIdentity(request.stream, request.id)
  if (request.surface !== undefined)
    map.set("surface", decodeSurface(request.surface, "session links"))
  return map
}

export function decodeSessionLinks(map: CborMap, context: string): SessionLinks {
  const surface = field.optionalString(map, "surface", context)
  return {
    ...decodeIdentity(map, context),
    ...(surface !== undefined ? { surface: decodeSurface(surface, context) } : {})
  }
}

export interface SessionSources {
  readonly stream: string
  readonly id: ConversationId
  readonly laneOnly?: boolean
}

export function encodeSessionSources(request: SessionSources): Map<string, unknown> {
  const map = encodeIdentity(request.stream, request.id)
  if (request.laneOnly === true) map.set("lane_only", true)
  return map
}
export function decodeSessionSources(map: CborMap, context: string): SessionSources {
  const laneOnly = field.optionalBoolean(map, "lane_only", context)
  return { ...decodeIdentity(map, context), ...(laneOnly === true ? { laneOnly } : {}) }
}

export interface SessionChanges {
  readonly stream: string
  readonly after: bigint
  /** The page size. Zero leaves it to the server. */
  readonly limit: number
}

export function encodeSessionChanges(request: SessionChanges): Map<string, unknown> {
  return new Map<string, unknown>([
    ["stream", request.stream],
    ["after", encodeU64(request.after, "session changes after")],
    ["limit", encodeU32(request.limit, "session changes limit")]
  ])
}

export function decodeSessionChanges(map: CborMap, context: string): SessionChanges {
  return {
    stream: field.requiredString(map, "stream", context),
    after: field.requiredU64(map, "after", context),
    limit: field.requiredU32(map, "limit", context)
  }
}

/** The liveness record a process publishes on `agent.heartbeats`, listing the
 * sessions in one stream that the process holds leases on. */
export interface SessionHeartbeat {
  readonly process: string
  readonly stream: string
  readonly sessions: readonly ConversationId[]
}

function validateSessionHeartbeat(heartbeat: SessionHeartbeat): void {
  if (heartbeat.sessions.length > MAX_HEARTBEAT_SESSIONS)
    throw new InvalidError("heartbeat lists too many sessions")
}

export function encodeSessionHeartbeat(heartbeat: SessionHeartbeat): Map<string, unknown> {
  validateSessionHeartbeat(heartbeat)
  return new Map<string, unknown>([
    ["process", heartbeat.process],
    ["stream", heartbeat.stream],
    ["sessions", heartbeat.sessions.map((id) => id.toBytes())]
  ])
}

export function decodeSessionHeartbeat(map: CborMap, context: string): SessionHeartbeat {
  const heartbeat = {
    process: field.requiredString(map, "process", context),
    stream: field.requiredString(map, "stream", context),
    sessions: field.requiredArray(map, "sessions", context, (item, index) => {
      if (!(item instanceof Uint8Array))
        throw new CodecError(`sessions ${String(index)} must be bytes`, context, "sessions")
      return ConversationId.fromBytes(item)
    })
  }
  validateSessionHeartbeat(heartbeat)
  return heartbeat
}

export interface SourceFrontier {
  readonly topicId: number
  readonly topicGeneration: bigint
  readonly partitionId: number
  readonly folded?: bigint
  readonly head?: bigint
  readonly retainedFrom?: bigint
}

export type GapReason =
  "expired_before_fold" | "pruned" | "truncated" | "rebuilding" | "unrecognized"

export interface SourceGap {
  readonly topicId: number
  readonly topicGeneration: bigint
  readonly partitionId: number
  readonly from: bigint
  readonly to: bigint
  readonly reason: GapReason
}

export interface PayloadRange {
  readonly topicId: number
  readonly topicGeneration: bigint
  readonly partitionId: number
  readonly first: bigint
  readonly last: bigint
}

export type StateOutcome = "applied" | "rejected" | "stale" | "duplicate" | "unrecognized"

export interface StateChange {
  readonly revision: bigint
  readonly opId?: string
  readonly outcome: StateOutcome
  readonly at: LogPosition
  readonly brokerTs: bigint
  readonly oldDigest?: Uint8Array
  readonly newDigest?: Uint8Array
  readonly reason?: string
}

export interface SessionFlags {
  readonly labelTruncated: boolean
  readonly overflow: boolean
  readonly eventsTruncated: boolean
  readonly laneConflict: boolean
  readonly rebuilding: boolean
  /** Liveness is not known yet because the heartbeat tail has not caught up,
   * so `idle` is not meaningful. */
  readonly livenessUnknown: boolean
}

export interface SessionInfo {
  readonly stream: string
  readonly id: ConversationId
  readonly label?: string
  readonly namespace?: string
  readonly agent?: AgentId
  readonly parent?: ConversationId
  readonly root?: ConversationId
  readonly status: SessionStatus
  readonly idle: boolean
  readonly overBudget: boolean
  readonly pauseRequested: boolean
  readonly cancelRequested: boolean
  readonly startedAt?: bigint
  readonly endedAt?: bigint
  readonly firstEventAt?: bigint
  readonly lastEventAt?: bigint
  readonly lastHeartbeatAt?: bigint
  readonly events: bigint
  readonly modelCalls: bigint
  readonly toolCalls: bigint
  readonly tokensIn: bigint
  readonly tokensOut: bigint
  readonly costMicros: bigint
  readonly errors: bigint
  readonly budget?: Budget
  readonly sdk?: SdkInfo
  readonly flags: SessionFlags
  /** Work records held while the session was paused and not yet handled. */
  readonly held: bigint
  readonly frontier: readonly SourceFrontier[]
}

export interface SessionPage {
  readonly items: readonly SessionInfo[]
  readonly cursor?: string
  readonly total?: bigint
  readonly searched?: bigint
  readonly truncated: boolean
}

export interface SessionEvent {
  readonly at: SourceRef
  readonly session: ConversationId
  readonly brokerTs: bigint
  readonly kind: string
  readonly operation?: string
  readonly display: string
  readonly agent?: AgentId
  readonly addressee?: string
  readonly correlation?: CorrelationId
  readonly cause?: LogPosition
  readonly tool?: string
  readonly usage?: TokenUsage
  readonly afterEnd: boolean
  /** The principal whose enrolled key verified this record's signature. */
  readonly verifiedActor?: string
  readonly summary: ReadonlyMap<string, unknown>
}

export interface SessionEventsPage {
  readonly items: readonly SessionEvent[]
  readonly cursor?: string
  readonly ranges: readonly PayloadRange[]
  readonly frontier: readonly SourceFrontier[]
  readonly fixedFrontier: boolean
  readonly gaps: readonly SourceGap[]
}

export interface SessionStateView {
  readonly revision: bigint
  readonly document: unknown
  readonly history: readonly StateChange[]
  readonly frontier?: SourceFrontier
  readonly complete: boolean
}

export interface SessionLink {
  readonly surface: LinkSurface
  readonly resource: string
  readonly item: string
  readonly relation: LinkRelation
  readonly first: LogPosition
  readonly last: LogPosition
}

export interface SessionLinksView {
  readonly links: readonly SessionLink[]
  readonly frontier: readonly SourceFrontier[]
  readonly truncated: boolean
}

export interface SessionSourcesView {
  readonly sources: readonly SourceFrontier[]
  readonly lane?: readonly [number, bigint, number]
}

export interface SessionChangeRow {
  readonly seq: bigint
  readonly sessions: readonly ConversationId[]
  readonly positions: readonly LogPosition[]
  readonly truncated: boolean
}

export interface SessionChangesView {
  readonly rows: readonly SessionChangeRow[]
  readonly floor: bigint
  readonly resync: boolean
}

export type SessionOutcome =
  | { readonly kind: "info"; readonly info: SessionInfo }
  | { readonly kind: "page"; readonly page: SessionPage }
  | { readonly kind: "events"; readonly page: SessionEventsPage }
  | { readonly kind: "state"; readonly state: SessionStateView }
  | { readonly kind: "links"; readonly links: SessionLinksView }
  | { readonly kind: "sources"; readonly sources: SessionSourcesView }
  | { readonly kind: "changes"; readonly changes: SessionChangesView }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export type SessionErrorKind =
  | "unsupported"
  | "notFound"
  | "notRegistered"
  | "invalid"
  | "unauthorized"
  | "stale"
  | "backend"
  | "unavailable"

export type SessionError =
  | { readonly kind: SessionErrorKind; readonly message: string }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export type SessionReply =
  | { readonly kind: "ok"; readonly outcome: SessionOutcome }
  | { readonly kind: "err"; readonly error: SessionError }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

const SESSION_ERROR_TEXT: Readonly<
  Record<SessionErrorKind, readonly [keyof typeof ResultCodeName, string]>
> = {
  unsupported: ["Unsupported", "sessions not supported"],
  notFound: ["NotFound", "session not found"],
  notRegistered: ["NotFound", "stream is not registered for sessions"],
  invalid: ["InvalidArgument", "invalid session request"],
  unauthorized: ["Forbidden", "session read not authorized"],
  stale: ["StaleGeneration", "stale session generation"],
  backend: ["Backend", "session backend error"],
  unavailable: ["Unavailable", "temporarily unavailable"]
}

/** The result code of a session read failure. */
export function sessionErrorResultCode(error: SessionError): ResultCode {
  return {
    kind: "known",
    name: error.kind === "unrecognized" ? "Backend" : SESSION_ERROR_TEXT[error.kind][0]
  }
}

/** The text of a session read failure. */
export function sessionErrorMessage(error: SessionError): string {
  return error.kind === "unrecognized"
    ? `session backend error: unrecognized error ${error.tag}`
    : `${SESSION_ERROR_TEXT[error.kind][1]}: ${error.message}`
}

const SESSION_ERROR_TAGS: readonly (readonly [SessionErrorKind, string])[] = [
  ["unsupported", "Unsupported"],
  ["notFound", "NotFound"],
  ["notRegistered", "NotRegistered"],
  ["invalid", "Invalid"],
  ["unauthorized", "Unauthorized"],
  ["stale", "Stale"],
  ["backend", "Backend"],
  ["unavailable", "Unavailable"]
]

function encodeU64Option(map: Map<string, unknown>, key: string, value: bigint | undefined): void {
  if (value !== undefined) map.set(key, encodeU64(value, key))
}

function decodeRelation(value: string, context: string): LinkRelation {
  if (value === "wrote" || value === "recalled" || value === "touched") return value
  throw new CodecError(`unknown session link relation ${value}`, context, "relation")
}

function decodeGapReason(value: string): GapReason {
  switch (value) {
    case "expired_before_fold":
    case "pruned":
    case "truncated":
    case "rebuilding":
      return value
    default:
      return "unrecognized"
  }
}

function decodeStateOutcome(value: string): StateOutcome {
  switch (value) {
    case "applied":
    case "rejected":
    case "stale":
    case "duplicate":
      return value
    default:
      return "unrecognized"
  }
}

function encodeSourceAddress(
  map: Map<string, unknown>,
  value: { topicId: number; topicGeneration: bigint; partitionId: number }
): void {
  map.set("topic_id", encodeU32(value.topicId, "topic id"))
  map.set("topic_generation", encodeU64(value.topicGeneration, "topic generation"))
  map.set("partition_id", encodeU32(value.partitionId, "partition id"))
}

function decodeSourceAddress(
  map: CborMap,
  context: string
): { topicId: number; topicGeneration: bigint; partitionId: number } {
  return {
    topicId: field.requiredU32(map, "topic_id", context),
    topicGeneration: field.requiredU64(map, "topic_generation", context),
    partitionId: field.requiredU32(map, "partition_id", context)
  }
}

export function encodeSourceFrontier(frontier: SourceFrontier): Map<string, unknown> {
  const map = new Map<string, unknown>()
  encodeSourceAddress(map, frontier)
  encodeU64Option(map, "folded", frontier.folded)
  encodeU64Option(map, "head", frontier.head)
  encodeU64Option(map, "retained_from", frontier.retainedFrom)
  return map
}

export function decodeSourceFrontier(map: CborMap, context: string): SourceFrontier {
  const folded = field.optionalU64(map, "folded", context)
  const head = field.optionalU64(map, "head", context)
  const retainedFrom = field.optionalU64(map, "retained_from", context)
  return {
    ...decodeSourceAddress(map, context),
    ...(folded !== undefined ? { folded } : {}),
    ...(head !== undefined ? { head } : {}),
    ...(retainedFrom !== undefined ? { retainedFrom } : {})
  }
}

function encodeSourceGap(gap: SourceGap): Map<string, unknown> {
  const map = new Map<string, unknown>()
  encodeSourceAddress(map, gap)
  map.set("from", encodeU64(gap.from, "gap start"))
  map.set("to", encodeU64(gap.to, "gap end"))
  if (gap.reason === "unrecognized") throw new InvalidError("gap reason must be known")
  map.set("reason", gap.reason)
  return map
}

function decodeSourceGap(map: CborMap, context: string): SourceGap {
  return {
    ...decodeSourceAddress(map, context),
    from: field.requiredU64(map, "from", context),
    to: field.requiredU64(map, "to", context),
    reason: decodeGapReason(field.requiredString(map, "reason", context))
  }
}

function encodePayloadRange(range: PayloadRange): Map<string, unknown> {
  const map = new Map<string, unknown>()
  encodeSourceAddress(map, range)
  map.set("first", encodeU64(range.first, "range start"))
  map.set("last", encodeU64(range.last, "range end"))
  return map
}

function decodePayloadRange(map: CborMap, context: string): PayloadRange {
  return {
    ...decodeSourceAddress(map, context),
    first: field.requiredU64(map, "first", context),
    last: field.requiredU64(map, "last", context)
  }
}

function setList<T>(
  map: Map<string, unknown>,
  key: string,
  items: readonly T[],
  encode: (item: T) => unknown
): void {
  if (items.length > 0) map.set(key, items.map(encode))
}

function readList<T>(
  map: CborMap,
  key: string,
  context: string,
  decode: (item: CborMap, context: string) => T
): T[] {
  return field.optionalArray(map, key, context, (item, index) => {
    const itemContext = `${context}.${key}[${String(index)}]`
    return decode(expectMap(item, itemContext), itemContext)
  })
}

function readFrontier(map: CborMap, context: string): SourceFrontier[] {
  return readList(map, "frontier", context, decodeSourceFrontier)
}

function setTrue(map: Map<string, unknown>, key: string, value: boolean): void {
  if (value) map.set(key, true)
}

function encodeStateChange(change: StateChange): Map<string, unknown> {
  const map = new Map<string, unknown>([["revision", encodeU64(change.revision, "revision")]])
  if (change.opId !== undefined) map.set("op_id", change.opId)
  if (change.outcome === "unrecognized") throw new InvalidError("state outcome must be known")
  map.set("outcome", change.outcome)
  map.set("at", logPositionToBytes(change.at))
  map.set("broker_ts", encodeU64(change.brokerTs, "broker timestamp"))
  if (change.oldDigest !== undefined) map.set("old_digest", change.oldDigest)
  if (change.newDigest !== undefined) map.set("new_digest", change.newDigest)
  if (change.reason !== undefined) map.set("reason", change.reason)
  return map
}

function decodeStateChange(map: CborMap, context: string): StateChange {
  const opId = field.optionalString(map, "op_id", context)
  const oldDigest = field.optionalBytes(map, "old_digest", context)
  const newDigest = field.optionalBytes(map, "new_digest", context)
  const reason = field.optionalString(map, "reason", context)
  return {
    revision: field.requiredU64(map, "revision", context),
    ...(opId !== undefined ? { opId } : {}),
    outcome: decodeStateOutcome(field.requiredString(map, "outcome", context)),
    at: logPositionFromBytes(field.requiredBytes(map, "at", context)),
    brokerTs: field.requiredU64(map, "broker_ts", context),
    ...(oldDigest !== undefined ? { oldDigest } : {}),
    ...(newDigest !== undefined ? { newDigest } : {}),
    ...(reason !== undefined ? { reason } : {})
  }
}

function encodeFlags(flags: SessionFlags): Map<string, unknown> {
  const map = new Map<string, unknown>()
  setTrue(map, "label_truncated", flags.labelTruncated)
  setTrue(map, "overflow", flags.overflow)
  setTrue(map, "events_truncated", flags.eventsTruncated)
  setTrue(map, "lane_conflict", flags.laneConflict)
  setTrue(map, "rebuilding", flags.rebuilding)
  setTrue(map, "liveness_unknown", flags.livenessUnknown)
  return map
}

function decodeFlags(map: CborMap | undefined, context: string): SessionFlags {
  const read = (key: string): boolean =>
    map === undefined ? false : (field.optionalBoolean(map, key, context) ?? false)
  return {
    labelTruncated: read("label_truncated"),
    overflow: read("overflow"),
    eventsTruncated: read("events_truncated"),
    laneConflict: read("lane_conflict"),
    rebuilding: read("rebuilding"),
    livenessUnknown: read("liveness_unknown")
  }
}

export function encodeSessionInfo(info: SessionInfo): Map<string, unknown> {
  const map = encodeIdentity(info.stream, info.id)
  if (info.label !== undefined) map.set("label", info.label)
  if (info.namespace !== undefined) map.set("namespace", info.namespace)
  if (info.agent !== undefined) map.set("agent", parseAgentId(info.agent))
  if (info.parent !== undefined) map.set("parent", info.parent.toBytes())
  if (info.root !== undefined) map.set("root", info.root.toBytes())
  if (info.status === "unrecognized") throw new InvalidError("session status must be known")
  map.set("status", info.status)
  map.set("idle", info.idle)
  map.set("over_budget", info.overBudget)
  map.set("pause_requested", info.pauseRequested)
  map.set("cancel_requested", info.cancelRequested)
  encodeU64Option(map, "started_at", info.startedAt)
  encodeU64Option(map, "ended_at", info.endedAt)
  encodeU64Option(map, "first_event_at", info.firstEventAt)
  encodeU64Option(map, "last_event_at", info.lastEventAt)
  encodeU64Option(map, "last_heartbeat_at", info.lastHeartbeatAt)
  map.set("events", encodeU64(info.events, "events"))
  map.set("model_calls", encodeU64(info.modelCalls, "model calls"))
  map.set("tool_calls", encodeU64(info.toolCalls, "tool calls"))
  map.set("tokens_in", encodeU64(info.tokensIn, "tokens in"))
  map.set("tokens_out", encodeU64(info.tokensOut, "tokens out"))
  map.set("cost_micros", encodeU64(info.costMicros, "cost"))
  map.set("errors", encodeU64(info.errors, "errors"))
  if (info.budget !== undefined) map.set("budget", encodeBudget(info.budget))
  if (info.sdk !== undefined) map.set("sdk", encodeSdkInfo(info.sdk))
  map.set("flags", encodeFlags(info.flags))
  if (info.held > 0n) map.set("held", info.held)
  setList(map, "frontier", info.frontier, encodeSourceFrontier)
  return map
}

export function decodeSessionInfo(map: CborMap, context: string): SessionInfo {
  const label = field.optionalString(map, "label", context)
  const namespace = field.optionalString(map, "namespace", context)
  const agent = field.optionalString(map, "agent", context)
  const parent = field.optionalBytes(map, "parent", context)
  const root = field.optionalBytes(map, "root", context)
  const startedAt = field.optionalU64(map, "started_at", context)
  const endedAt = field.optionalU64(map, "ended_at", context)
  const firstEventAt = field.optionalU64(map, "first_event_at", context)
  const lastEventAt = field.optionalU64(map, "last_event_at", context)
  const lastHeartbeatAt = field.optionalU64(map, "last_heartbeat_at", context)
  const budget = field.optionalMap(map, "budget", context)
  const sdk = field.optionalMap(map, "sdk", context)
  return {
    ...decodeIdentity(map, context),
    ...(label !== undefined ? { label } : {}),
    ...(namespace !== undefined ? { namespace } : {}),
    ...(agent !== undefined ? { agent: parseAgentId(agent) } : {}),
    ...(parent !== undefined ? { parent: ConversationId.fromBytes(parent) } : {}),
    ...(root !== undefined ? { root: ConversationId.fromBytes(root) } : {}),
    status: sessionStatusFromWire(field.requiredString(map, "status", context)),
    idle: field.requiredBoolean(map, "idle", context),
    overBudget: field.requiredBoolean(map, "over_budget", context),
    pauseRequested: field.requiredBoolean(map, "pause_requested", context),
    cancelRequested: field.requiredBoolean(map, "cancel_requested", context),
    ...(startedAt !== undefined ? { startedAt } : {}),
    ...(endedAt !== undefined ? { endedAt } : {}),
    ...(firstEventAt !== undefined ? { firstEventAt } : {}),
    ...(lastEventAt !== undefined ? { lastEventAt } : {}),
    ...(lastHeartbeatAt !== undefined ? { lastHeartbeatAt } : {}),
    events: field.requiredU64(map, "events", context),
    modelCalls: field.requiredU64(map, "model_calls", context),
    toolCalls: field.requiredU64(map, "tool_calls", context),
    tokensIn: field.requiredU64(map, "tokens_in", context),
    tokensOut: field.requiredU64(map, "tokens_out", context),
    costMicros: field.requiredU64(map, "cost_micros", context),
    errors: field.requiredU64(map, "errors", context),
    ...(budget !== undefined ? { budget: decodeBudget(budget, `${context}.budget`) } : {}),
    ...(sdk !== undefined ? { sdk: decodeSdkInfo(sdk, `${context}.sdk`) } : {}),
    flags: decodeFlags(field.optionalMap(map, "flags", context), `${context}.flags`),
    held: field.optionalU64(map, "held", context) ?? 0n,
    frontier: readFrontier(map, context)
  }
}

function encodeSessionPage(page: SessionPage): Map<string, unknown> {
  const map = new Map<string, unknown>([["items", page.items.map(encodeSessionInfo)]])
  if (page.cursor !== undefined) map.set("cursor", page.cursor)
  encodeU64Option(map, "total", page.total)
  encodeU64Option(map, "searched", page.searched)
  map.set("truncated", page.truncated)
  return map
}

function decodeSessionPage(map: CborMap, context: string): SessionPage {
  const cursor = field.optionalString(map, "cursor", context)
  const total = field.optionalU64(map, "total", context)
  const searched = field.optionalU64(map, "searched", context)
  return {
    items: readList(map, "items", context, decodeSessionInfo),
    ...(cursor !== undefined ? { cursor } : {}),
    ...(total !== undefined ? { total } : {}),
    ...(searched !== undefined ? { searched } : {}),
    truncated: field.requiredBoolean(map, "truncated", context)
  }
}

export function encodeSessionEvent(event: SessionEvent): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["at", encodeSourceRef(event.at)],
    ["session", event.session.toBytes()],
    ["broker_ts", encodeU64(event.brokerTs, "broker timestamp")],
    ["kind", event.kind]
  ])
  if (event.operation !== undefined) map.set("operation", event.operation)
  map.set("display", event.display)
  if (event.agent !== undefined) map.set("agent", parseAgentId(event.agent))
  if (event.addressee !== undefined) map.set("addressee", event.addressee)
  if (event.correlation !== undefined) map.set("correlation", event.correlation.toBytes())
  if (event.cause !== undefined) map.set("cause", logPositionToBytes(event.cause))
  if (event.tool !== undefined) map.set("tool", event.tool)
  if (event.usage !== undefined) map.set("usage", encodeTokenUsage(event.usage))
  setTrue(map, "after_end", event.afterEnd)
  if (event.verifiedActor !== undefined) map.set("verified_actor", event.verifiedActor)
  if (event.summary.size > 0) map.set("summary", jsonToCbor(Object.fromEntries(event.summary)))
  return map
}

export function decodeSessionEvent(map: CborMap, context: string): SessionEvent {
  const operation = field.optionalString(map, "operation", context)
  const agent = field.optionalString(map, "agent", context)
  const addressee = field.optionalString(map, "addressee", context)
  const correlation = field.optionalBytes(map, "correlation", context)
  const cause = field.optionalBytes(map, "cause", context)
  const tool = field.optionalString(map, "tool", context)
  const usage = field.optionalMap(map, "usage", context)
  const summary = field.optionalMap(map, "summary", context)
  const verifiedActor = field.optionalString(map, "verified_actor", context)
  return {
    at: decodeSourceRef(map.get("at"), `${context}.at`),
    session: ConversationId.fromBytes(field.requiredBytes(map, "session", context)),
    brokerTs: field.requiredU64(map, "broker_ts", context),
    kind: field.requiredString(map, "kind", context),
    ...(operation !== undefined ? { operation } : {}),
    display: field.requiredString(map, "display", context),
    ...(agent !== undefined ? { agent: parseAgentId(agent) } : {}),
    ...(addressee !== undefined ? { addressee } : {}),
    ...(correlation !== undefined ? { correlation: CorrelationId.fromBytes(correlation) } : {}),
    ...(cause !== undefined ? { cause: logPositionFromBytes(cause) } : {}),
    ...(tool !== undefined ? { tool } : {}),
    ...(usage !== undefined ? { usage: decodeTokenUsage(usage, `${context}.usage`) } : {}),
    afterEnd: field.optionalBoolean(map, "after_end", context) ?? false,
    ...(verifiedActor !== undefined ? { verifiedActor } : {}),
    summary: decodeSummary(summary, `${context}.summary`)
  }
}

function decodeSummary(map: CborMap | undefined, context: string): Map<string, unknown> {
  const out = new Map<string, unknown>()
  for (const [key, value] of map ?? []) {
    if (typeof key !== "string")
      throw new CodecError("summary key must be text", context, "summary")
    out.set(key, cborToJson(value, context))
  }
  return out
}

function encodeEventsPage(page: SessionEventsPage): Map<string, unknown> {
  const map = new Map<string, unknown>([["items", page.items.map(encodeSessionEvent)]])
  if (page.cursor !== undefined) map.set("cursor", page.cursor)
  setList(map, "ranges", page.ranges, encodePayloadRange)
  setList(map, "frontier", page.frontier, encodeSourceFrontier)
  map.set("fixed_frontier", page.fixedFrontier)
  setList(map, "gaps", page.gaps, encodeSourceGap)
  return map
}

function decodeEventsPage(map: CborMap, context: string): SessionEventsPage {
  const cursor = field.optionalString(map, "cursor", context)
  return {
    items: readList(map, "items", context, decodeSessionEvent),
    ...(cursor !== undefined ? { cursor } : {}),
    ranges: readList(map, "ranges", context, decodePayloadRange),
    frontier: readFrontier(map, context),
    fixedFrontier: field.requiredBoolean(map, "fixed_frontier", context),
    gaps: readList(map, "gaps", context, decodeSourceGap)
  }
}

function encodeStateView(state: SessionStateView): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["revision", encodeU64(state.revision, "revision")],
    ["document", jsonToCbor(state.document)]
  ])
  setList(map, "history", state.history, encodeStateChange)
  if (state.frontier !== undefined) map.set("frontier", encodeSourceFrontier(state.frontier))
  map.set("complete", state.complete)
  return map
}

function decodeStateView(map: CborMap, context: string): SessionStateView {
  const frontier = field.optionalMap(map, "frontier", context)
  return {
    revision: field.requiredU64(map, "revision", context),
    document: cborToJson(map.get("document"), `${context}.document`),
    history: readList(map, "history", context, decodeStateChange),
    ...(frontier !== undefined
      ? { frontier: decodeSourceFrontier(frontier, `${context}.frontier`) }
      : {}),
    complete: field.requiredBoolean(map, "complete", context)
  }
}

function encodeLink(link: SessionLink): Map<string, unknown> {
  return new Map<string, unknown>([
    ["surface", decodeSurface(link.surface, "session link")],
    ["resource", link.resource],
    ["item", link.item],
    ["relation", decodeRelation(link.relation, "session link")],
    ["first", logPositionToBytes(link.first)],
    ["last", logPositionToBytes(link.last)]
  ])
}

function decodeLink(map: CborMap, context: string): SessionLink {
  return {
    surface: decodeSurface(field.requiredString(map, "surface", context), context),
    resource: field.requiredString(map, "resource", context),
    item: field.requiredString(map, "item", context),
    relation: decodeRelation(field.requiredString(map, "relation", context), context),
    first: logPositionFromBytes(field.requiredBytes(map, "first", context)),
    last: logPositionFromBytes(field.requiredBytes(map, "last", context))
  }
}

function encodeLinksView(view: SessionLinksView): Map<string, unknown> {
  const map = new Map<string, unknown>([["links", view.links.map(encodeLink)]])
  setList(map, "frontier", view.frontier, encodeSourceFrontier)
  map.set("truncated", view.truncated)
  return map
}

function decodeLinksView(map: CborMap, context: string): SessionLinksView {
  return {
    links: readList(map, "links", context, decodeLink),
    frontier: readFrontier(map, context),
    truncated: field.requiredBoolean(map, "truncated", context)
  }
}

function encodeChangeRow(row: SessionChangeRow): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["seq", encodeU64(row.seq, "change sequence")],
    ["sessions", row.sessions.map((id) => id.toBytes())]
  ])
  setList(map, "positions", row.positions, logPositionToBytes)
  map.set("truncated", row.truncated)
  return map
}

function decodeChangeRow(map: CborMap, context: string): SessionChangeRow {
  const bytes = (key: string): Uint8Array[] =>
    field.optionalArray(map, key, context, (item, index) => {
      if (!(item instanceof Uint8Array))
        throw new CodecError(`${key} ${String(index)} must be bytes`, context, key)
      return item
    })
  return {
    seq: field.requiredU64(map, "seq", context),
    sessions: bytes("sessions").map((item) => ConversationId.fromBytes(item)),
    positions: bytes("positions").map(logPositionFromBytes),
    truncated: field.requiredBoolean(map, "truncated", context)
  }
}

function encodeChanges(changes: SessionChangesView): Map<string, unknown> {
  return new Map<string, unknown>([
    ["rows", changes.rows.map(encodeChangeRow)],
    ["floor", encodeU64(changes.floor, "change floor")],
    ["resync", changes.resync]
  ])
}

function decodeChanges(map: CborMap, context: string): SessionChangesView {
  return {
    rows: readList(map, "rows", context, decodeChangeRow),
    floor: field.requiredU64(map, "floor", context),
    resync: field.requiredBoolean(map, "resync", context)
  }
}

export function encodeSessionOutcome(outcome: SessionOutcome): Map<string, unknown> {
  switch (outcome.kind) {
    case "info":
      return new Map([["Info", encodeSessionInfo(outcome.info)]])
    case "page":
      return new Map([["Page", encodeSessionPage(outcome.page)]])
    case "events":
      return new Map([["Events", encodeEventsPage(outcome.page)]])
    case "state":
      return new Map([["State", encodeStateView(outcome.state)]])
    case "links":
      return new Map([["Links", encodeLinksView(outcome.links)]])
    case "sources":
      return new Map([
        [
          "Sources",
          new Map<string, unknown>([
            ["sources", outcome.sources.sources.map(encodeSourceFrontier)],
            ...(outcome.sources.lane === undefined
              ? []
              : [
                  [
                    "lane",
                    [
                      encodeU32(outcome.sources.lane[0], "lane topic"),
                      encodeU64(outcome.sources.lane[1], "lane generation"),
                      encodeU32(outcome.sources.lane[2], "lane partitions")
                    ]
                  ] as [string, unknown]
                ])
          ])
        ]
      ])
    case "changes":
      return new Map([["Changes", encodeChanges(outcome.changes)]])
    case "unrecognized":
      return new Map([[outcome.tag, outcome.value]])
  }
}

function decodeSources(map: CborMap, context: string): SessionSourcesView {
  const lane = map.get("lane")
  const sources = readList(map, "sources", context, decodeSourceFrontier)
  if (lane === undefined || lane === null) return { sources }
  const values = expectArray(lane, `${context}.lane`)
  if (values.length !== 3) throw new CodecError("a session lane has three fields", context, "lane")
  return {
    sources,
    lane: [
      expectU32(values[0], context),
      expectU64(values[1], context),
      expectU32(values[2], context)
    ]
  }
}

export function decodeSessionOutcome(value: unknown, context: string): SessionOutcome {
  const [tag, inner] = singleVariantTag(value, context)
  const map = (): CborMap => expectMap(inner, `${context}.${tag}`)
  switch (tag) {
    case "Info":
      return { kind: "info", info: decodeSessionInfo(map(), context) }
    case "Page":
      return { kind: "page", page: decodeSessionPage(map(), context) }
    case "Events":
      return { kind: "events", page: decodeEventsPage(map(), context) }
    case "State":
      return { kind: "state", state: decodeStateView(map(), context) }
    case "Links":
      return { kind: "links", links: decodeLinksView(map(), context) }
    case "Sources":
      return {
        kind: "sources",
        sources: decodeSources(map(), context)
      }
    case "Changes":
      return { kind: "changes", changes: decodeChanges(map(), context) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export function encodeSessionError(error: SessionError): Map<string, unknown> {
  if (error.kind === "unrecognized") return new Map([[error.tag, error.value]])
  const tag = SESSION_ERROR_TAGS.find(([kind]) => kind === error.kind)?.[1]
  if (tag === undefined) throw new InvalidError(`unknown session error ${error.kind}`)
  return new Map([[tag, error.message]])
}

export function decodeSessionError(value: unknown, context: string): SessionError {
  const [tag, inner] = singleVariantTag(value, context)
  const kind = SESSION_ERROR_TAGS.find(([, wire]) => wire === tag)?.[0]
  if (kind === undefined || typeof inner !== "string")
    return { kind: "unrecognized", tag, value: inner }
  return { kind, message: inner }
}

export function encodeSessionReply(reply: SessionReply): Map<string, unknown> {
  switch (reply.kind) {
    case "ok":
      return new Map([["Ok", encodeSessionOutcome(reply.outcome)]])
    case "err":
      return new Map([["Err", encodeSessionError(reply.error)]])
    case "unrecognized":
      return new Map([[reply.tag, reply.value]])
  }
}

export function decodeSessionReply(value: unknown, context: string): SessionReply {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Ok":
      return { kind: "ok", outcome: decodeSessionOutcome(inner, `${context}.Ok`) }
    case "Err":
      return { kind: "err", error: decodeSessionError(inner, `${context}.Err`) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

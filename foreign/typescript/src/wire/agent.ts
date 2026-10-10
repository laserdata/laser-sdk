import { CodecError, InvalidError } from "../client/errors.js"
import {
  type CborMap,
  decodeOne,
  expectMap,
  expectString,
  expectU32,
  expectU64,
  field,
  singleVariantTag
} from "./cbor.js"
import { ContentType } from "./content.js"
import { ChannelId, ConversationId, CorrelationId, RecordId } from "./ids.js"
import type { LogPosition } from "./ids.js"
import { logPositionFromBytes, logPositionToBytes } from "./ids.js"
import {
  decodeProducer,
  decodeSourceRef,
  encodeProducer,
  encodeSourceRef,
  type ProducerInfo,
  type SourceRef
} from "./graph.js"
import { validateNamespace } from "./kv.js"
import { validatePortableJson } from "./json-patch.js"
import {
  MAX_AGENT_STRING_BYTES,
  MAX_BODY_REFERENCE_BYTES,
  MAX_CARD_CAPABILITIES,
  MAX_IDEMPOTENCY_KEY_BYTES,
  MAX_MANIFEST_FRAGMENTS,
  MAX_MEMORY_TAGS,
  MAX_MEMORY_TAG_BYTES,
  MAX_METADATA_ENTRIES,
  MAX_METADATA_KEY_BYTES,
  MAX_METADATA_TOTAL_BYTES,
  MAX_METADATA_VALUE_BYTES,
  MAX_SESSION_LABEL_BYTES,
  MAX_STATE_DOCUMENT_BYTES,
  MAX_STATE_PATCH_OPS
} from "./limits.js"
import { type Value, decodeValue, encodeValue } from "./value.js"

export const AgentKind = {
  Command: "command",
  Response: "response",
  Event: "event",
  Chunk: "chunk",
  Status: "status",
  Error: "error"
} as const
export type AgentKind = (typeof AgentKind)[keyof typeof AgentKind]

const AGENT_KINDS: ReadonlySet<string> = new Set(Object.values(AgentKind))

export function parseAgentKind(value: string, context: string): AgentKind {
  if (!AGENT_KINDS.has(value)) {
    throw new CodecError(`\`${value}\` is not a recognized agent envelope kind`, context, "kind")
  }
  return value as AgentKind
}

export type IdempotencyKey = string & { readonly __brand: "IdempotencyKey" }

const textEncoder = new TextEncoder()

function utf8Length(value: string): number {
  return textEncoder.encode(value).length
}

export function parseIdempotencyKey(value: string): IdempotencyKey {
  if (value.length === 0) {
    throw new InvalidError("idempotency key must not be empty")
  }
  const bytes = utf8Length(value)
  if (bytes > MAX_IDEMPOTENCY_KEY_BYTES) {
    throw new InvalidError(
      `idempotency key is ${String(bytes)}B, exceeds cap ${String(MAX_IDEMPOTENCY_KEY_BYTES)}B`
    )
  }
  return value as IdempotencyKey
}

export type AgentId = string & { readonly __brand: "AgentId" }

export function parseAgentId(value: string): AgentId {
  if (value.length === 0) {
    throw new InvalidError("agent id must not be empty")
  }
  const bytes = utf8Length(value)
  if (bytes > MAX_AGENT_STRING_BYTES) {
    throw new InvalidError(
      `agent id is ${String(bytes)}B, exceeds cap ${String(MAX_AGENT_STRING_BYTES)}B`
    )
  }
  for (const char of value) {
    const code = char.charCodeAt(0)
    if (code < 0x20 || code === 0x7f || (code >= 0x80 && code <= 0x9f)) {
      throw new InvalidError(`agent id must not contain control characters (found ${char})`)
    }
  }
  return value as AgentId
}

export type TaskState =
  | { readonly kind: "known"; readonly name: keyof typeof TaskStateName }
  | { readonly kind: "unrecognized"; readonly code: number }

export type SessionStatus =
  "submitted" | "active" | "paused" | "completed" | "failed" | "canceled" | "unrecognized"

export function sessionStatusFromWire(value: string): SessionStatus {
  switch (value) {
    case "submitted":
    case "active":
    case "paused":
    case "completed":
    case "failed":
    case "canceled":
      return value
    default:
      return "unrecognized"
  }
}

export interface SdkInfo {
  readonly language: string
  readonly version: string
}

export interface Budget {
  readonly tokens?: bigint
  readonly costMicros?: bigint
}

export interface SessionStart {
  readonly label?: string
  readonly namespace?: string
  readonly agent: AgentId
  readonly sdk: SdkInfo
  readonly parent?: ConversationId
  readonly root?: ConversationId
  readonly idleTimeoutMicros?: bigint
  readonly budget?: Budget
  readonly tags: readonly string[]
}

export interface SessionTransition {
  readonly actor?: AgentId
  readonly acknowledges?: LogPosition
}

export interface SessionEnd {
  readonly reason?: string
  readonly error?: AgentErrorBody
}

/** The JSON body of a `session_pause` request on `agent.control`: the agents
 * whose acknowledgments complete the pause, frozen when the request is
 * written. An empty set names no agent, and every agent that receives work
 * for the session while it is paused acknowledges when it holds that work. */
export interface SessionPauseRequest {
  readonly participants: readonly AgentId[]
}

/** One work record an agent held while its session was paused. A
 * `session_parked` event on the session lane carries it, and a
 * `session_unparked` event with the same body records that the agent handled
 * the record after the resume. */
export interface SessionParking {
  readonly source: SourceRef
  readonly role: AgentId
  readonly request: LogPosition
}

export type Fragment =
  | {
      readonly kind: "message"
      readonly at: SourceRef
      readonly tokens: number
      readonly bytes: number
    }
  | {
      readonly kind: "memory"
      readonly id: string
      readonly version?: bigint
      readonly digest?: Uint8Array
      readonly tokens: number
      readonly bytes: number
    }
  | {
      readonly kind: "kv"
      readonly namespace: string
      readonly key: string
      readonly version?: bigint
      readonly digest?: Uint8Array
      readonly tokens: number
      readonly bytes: number
    }
  | {
      readonly kind: "state"
      readonly key: string
      readonly revision: bigint
      readonly tokens: number
      readonly bytes: number
    }
  | {
      readonly kind: "summary"
      readonly at: SourceRef
      readonly tokens: number
      readonly bytes: number
    }

export interface ContextManifest {
  readonly policy: string
  readonly policyVersion: string
  readonly fragments: readonly Fragment[]
  readonly tokens: bigint
  readonly bytes: bigint
  readonly frontier: readonly (readonly [number, number, bigint])[]
  readonly correlation?: CorrelationId
}

export interface ContextCompaction {
  readonly summaryAt: SourceRef
  readonly covered: readonly (readonly [number, number, bigint, bigint])[]
  readonly summarizer: ProducerInfo
}

export interface ContextRetrieval {
  readonly query?: string
  readonly items: readonly (readonly [string, number])[]
}

function encodeFragment(fragment: Fragment): Map<string, unknown> {
  const fields = new Map<string, unknown>()
  switch (fragment.kind) {
    case "message":
    case "summary":
      fields.set("at", encodeSourceRef(fragment.at))
      break
    case "memory":
      fields.set("id", fragment.id)
      if (fragment.version !== undefined) fields.set("version", fragment.version)
      if (fragment.digest !== undefined) {
        if (fragment.digest.length !== 32)
          throw new InvalidError("context fragment digest must be 32 bytes")
        fields.set("digest", fragment.digest)
      }
      break
    case "kv":
      fields.set("namespace", fragment.namespace)
      fields.set("key", fragment.key)
      if (fragment.version !== undefined) fields.set("version", fragment.version)
      if (fragment.digest !== undefined) {
        if (fragment.digest.length !== 32)
          throw new InvalidError("context fragment digest must be 32 bytes")
        fields.set("digest", fragment.digest)
      }
      break
    case "state":
      fields.set("key", fragment.key)
      fields.set("revision", fragment.revision)
      break
  }
  fields.set("tokens", BigInt(fragment.tokens))
  fields.set("bytes", BigInt(fragment.bytes))
  const tag =
    fragment.kind === "kv" ? "Kv" : fragment.kind.slice(0, 1).toUpperCase() + fragment.kind.slice(1)
  return new Map([[tag, fields]])
}

function decodeFragment(raw: unknown, context: string): Fragment {
  const [tag, inner] = singleVariantTag(raw, context)
  const fields = expectMap(inner, context)
  const tokens = field.requiredU32(fields, "tokens", context)
  const bytes = field.requiredU32(fields, "bytes", context)
  switch (tag) {
    case "Message":
    case "Summary":
      return {
        kind: tag === "Message" ? "message" : "summary",
        at: decodeSourceRef(fields.get("at"), context),
        tokens,
        bytes
      }
    case "Memory":
    case "Kv": {
      const version = field.optionalU64(fields, "version", context)
      const digest = field.optionalBytes(fields, "digest", context)
      if (digest !== undefined && digest.length !== 32)
        throw new CodecError("context fragment digest must be 32 bytes", context, "digest")
      const optional = {
        ...(version !== undefined ? { version } : {}),
        ...(digest !== undefined ? { digest } : {})
      }
      return tag === "Memory"
        ? {
            kind: "memory",
            id: field.requiredString(fields, "id", context),
            tokens,
            bytes,
            ...optional
          }
        : {
            kind: "kv",
            namespace: field.requiredString(fields, "namespace", context),
            key: field.requiredString(fields, "key", context),
            tokens,
            bytes,
            ...optional
          }
    }
    case "State":
      return {
        kind: "state",
        key: field.requiredString(fields, "key", context),
        revision: field.requiredU64(fields, "revision", context),
        tokens,
        bytes
      }
    default:
      throw new CodecError(`unknown context fragment ${tag}`, context, "fragment")
  }
}

export function encodeContextManifest(manifest: ContextManifest): Map<string, unknown> {
  if (manifest.fragments.length > MAX_MANIFEST_FRAGMENTS)
    throw new InvalidError("context manifest has too many fragments")
  const map = new Map<string, unknown>([
    ["policy", manifest.policy],
    ["policy_version", manifest.policyVersion],
    ["fragments", manifest.fragments.map(encodeFragment)],
    ["tokens", manifest.tokens],
    ["bytes", manifest.bytes],
    [
      "frontier",
      manifest.frontier.map(([topic, partition, offset]) => [
        BigInt(topic),
        BigInt(partition),
        offset
      ])
    ]
  ])
  if (manifest.correlation !== undefined) map.set("correlation", manifest.correlation.toBytes())
  return map
}

export function decodeContextManifest(map: CborMap, context: string): ContextManifest {
  const fragments = field.requiredArray(map, "fragments", context, (raw, index) =>
    decodeFragment(raw, `${context}.fragments[${String(index)}]`)
  )
  if (fragments.length > MAX_MANIFEST_FRAGMENTS)
    throw new CodecError("context manifest has too many fragments", context, "fragments")
  const correlation = field.optionalBytes(map, "correlation", context)
  return {
    policy: field.requiredString(map, "policy", context),
    policyVersion: field.requiredString(map, "policy_version", context),
    fragments,
    tokens: field.requiredU64(map, "tokens", context),
    bytes: field.requiredU64(map, "bytes", context),
    frontier: field.requiredArray(map, "frontier", context, (raw, index) => {
      if (!Array.isArray(raw) || raw.length !== 3)
        throw new CodecError("context frontier entry must have three integers", context, "frontier")
      return [
        expectU32(raw[0], `${context}.frontier[${String(index)}].topic`),
        expectU32(raw[1], `${context}.frontier[${String(index)}].partition`),
        expectU64(raw[2], `${context}.frontier[${String(index)}].offset`)
      ] as const
    }),
    ...(correlation !== undefined ? { correlation: CorrelationId.fromBytes(correlation) } : {})
  }
}

export function encodeContextCompaction(compaction: ContextCompaction): Map<string, unknown> {
  return new Map<string, unknown>([
    ["summary_at", encodeSourceRef(compaction.summaryAt)],
    [
      "covered",
      compaction.covered.map(([topic, partition, from, to]) => [
        BigInt(topic),
        BigInt(partition),
        from,
        to
      ])
    ],
    ["summarizer", encodeProducer(compaction.summarizer)]
  ])
}

export function decodeContextCompaction(map: CborMap, context: string): ContextCompaction {
  return {
    summaryAt: decodeSourceRef(map.get("summary_at"), `${context}.summary_at`),
    covered: field.requiredArray(map, "covered", context, (raw, index) => {
      if (!Array.isArray(raw) || raw.length !== 4)
        throw new CodecError("covered range must have four integers", context, "covered")
      return [
        expectU32(raw[0], `${context}.covered[${String(index)}].topic`),
        expectU32(raw[1], `${context}.covered[${String(index)}].partition`),
        expectU64(raw[2], `${context}.covered[${String(index)}].from`),
        expectU64(raw[3], `${context}.covered[${String(index)}].to`)
      ] as const
    }),
    summarizer: decodeProducer(
      field.requiredMap(map, "summarizer", context),
      `${context}.summarizer`
    )
  }
}

export function encodeContextRetrieval(retrieval: ContextRetrieval): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (retrieval.query !== undefined) map.set("query", retrieval.query)
  map.set(
    "items",
    retrieval.items.map(([id, score]) => [id, score])
  )
  return map
}

export function decodeContextRetrieval(map: CborMap, context: string): ContextRetrieval {
  const query = field.optionalString(map, "query", context)
  return {
    ...(query !== undefined ? { query } : {}),
    items: field.requiredArray(map, "items", context, (raw, index) => {
      if (!Array.isArray(raw) || raw.length !== 2 || typeof raw[1] !== "number")
        throw new CodecError("retrieval item must contain an id and score", context, "items")
      return [expectString(raw[0], `${context}.items[${String(index)}].id`), raw[1]] as const
    })
  }
}

export type PatchOp =
  | { readonly op: "add" | "replace" | "test"; readonly path: string; readonly value: unknown }
  | { readonly op: "remove"; readonly path: string }
  | { readonly op: "move" | "copy"; readonly from: string; readonly path: string }

export interface StateDelta {
  readonly baseRevision: bigint
  readonly patch: readonly PatchOp[]
  readonly opId: string
}

export interface StateSnapshot {
  readonly baseRevision: bigint
  readonly document: unknown
}

function jsonBytes(value: unknown): number {
  try {
    validatePortableJson(value)
    const json: unknown = JSON.stringify(value)
    if (typeof json !== "string") throw new Error("value is not JSON")
    return new TextEncoder().encode(json).length
  } catch (error) {
    if (error instanceof InvalidError) throw error
    throw new InvalidError("state value must be JSON")
  }
}

export function validateStateDelta(delta: StateDelta): void {
  if (delta.patch.length > MAX_STATE_PATCH_OPS)
    throw new InvalidError("state patch has too many operations")
  if (delta.opId.length === 0) throw new InvalidError("state patch operation id must not be empty")
  if (jsonBytes(delta.patch) > MAX_STATE_DOCUMENT_BYTES)
    throw new InvalidError("state patch exceeds the document byte cap")
}

export function validateStateSnapshot(snapshot: StateSnapshot): void {
  if (jsonBytes(snapshot.document) > MAX_STATE_DOCUMENT_BYTES)
    throw new InvalidError("state document exceeds its byte cap")
}

export function jsonToCbor(value: unknown): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value
  if (typeof value === "number" && Number.isFinite(value)) return value
  if (Array.isArray(value)) return value.map(jsonToCbor)
  if (typeof value === "object" && !(value instanceof Uint8Array)) {
    return new Map(
      Object.entries(value)
        .sort(([left], [right]) => compareUtf8(left, right))
        .map(([key, item]) => [key, jsonToCbor(item)])
    )
  }
  throw new InvalidError("state value must be JSON")
}

function compareUtf8(left: string, right: string): number {
  const first = textEncoder.encode(left)
  const second = textEncoder.encode(right)
  for (let index = 0; index < Math.min(first.length, second.length); index += 1) {
    const difference = (first[index] ?? 0) - (second[index] ?? 0)
    if (difference !== 0) return difference
  }
  return first.length - second.length
}

export function cborToJson(value: unknown, context: string): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value
  if (typeof value === "number") {
    if (Number.isInteger(value) && !Number.isSafeInteger(value))
      throw new CodecError(
        "state JSON integer is outside the exact number range",
        context,
        "document"
      )
    return value
  }
  if (typeof value === "bigint") {
    if (value < BigInt(Number.MIN_SAFE_INTEGER) || value > BigInt(Number.MAX_SAFE_INTEGER))
      throw new CodecError(
        "state JSON integer is outside the exact number range",
        context,
        "document"
      )
    return Number(value)
  }
  if (Array.isArray(value)) return value.map((item) => cborToJson(item, context))
  if (value instanceof Map) {
    const out = Object.create(null) as Record<string, unknown>
    for (const [key, item] of value) {
      if (typeof key !== "string")
        throw new CodecError("state object key must be text", context, "document")
      out[key] = cborToJson(item, context)
    }
    return out
  }
  throw new CodecError("state document must be JSON", context, "document")
}

function encodePatchOp(operation: PatchOp): Map<string, unknown> {
  if (typeof operation.path !== "string")
    throw new InvalidError("state patch path must be a string")
  const op: unknown = operation.op
  if (typeof op !== "string" || !["add", "remove", "replace", "move", "copy", "test"].includes(op))
    throw new InvalidError("unknown state patch operation")
  const map = new Map<string, unknown>([["op", operation.op]])
  if (operation.op === "move" || operation.op === "copy") {
    if (typeof operation.from !== "string")
      throw new InvalidError("state patch source must be a string")
    map.set("from", operation.from)
  }
  map.set("path", operation.path)
  if (operation.op === "add" || operation.op === "replace" || operation.op === "test") {
    if (!("value" in operation)) throw new InvalidError("state patch value is missing")
    map.set("value", jsonToCbor(operation.value))
  }
  return map
}

function decodePatchOp(value: unknown, context: string): PatchOp {
  const map = expectMap(value, context)
  const op = field.requiredString(map, "op", context)
  const path = field.requiredString(map, "path", context)
  switch (op) {
    case "add":
    case "replace":
    case "test":
      if (!map.has("value")) throw new CodecError("state patch value is missing", context, "value")
      return { op, path, value: cborToJson(map.get("value"), context) }
    case "remove":
      return { op, path }
    case "move":
    case "copy":
      return { op, path, from: field.requiredString(map, "from", context) }
    default:
      throw new CodecError(`unknown state patch operation ${op}`, context, "op")
  }
}

export function encodeStateDelta(delta: StateDelta): Map<string, unknown> {
  validateStateDelta(delta)
  return new Map<string, unknown>([
    ["base_revision", delta.baseRevision],
    ["patch", delta.patch.map(encodePatchOp)],
    ["op_id", delta.opId]
  ])
}

export function decodeStateDelta(map: CborMap, context: string): StateDelta {
  const delta = {
    baseRevision: field.requiredU64(map, "base_revision", context),
    patch: field.requiredArray(map, "patch", context, (item, index) =>
      decodePatchOp(item, `${context}.patch[${String(index)}]`)
    ),
    opId: field.requiredString(map, "op_id", context)
  }
  validateStateDelta(delta)
  return delta
}

export function encodeStateSnapshot(snapshot: StateSnapshot): Map<string, unknown> {
  validateStateSnapshot(snapshot)
  return new Map<string, unknown>([
    ["base_revision", snapshot.baseRevision],
    ["document", jsonToCbor(snapshot.document)]
  ])
}

export function decodeStateSnapshot(map: CborMap, context: string): StateSnapshot {
  const snapshot = {
    baseRevision: field.requiredU64(map, "base_revision", context),
    document: cborToJson(map.get("document"), context)
  }
  validateStateSnapshot(snapshot)
  return snapshot
}

export function validateSessionStart(start: SessionStart): void {
  if (
    start.label !== undefined &&
    (utf8Length(start.label) > MAX_SESSION_LABEL_BYTES || /\p{Cc}/u.test(start.label))
  )
    throw new InvalidError("session label exceeds its cap or contains a control character")
  if (start.namespace !== undefined) validateNamespace(start.namespace)
  if (start.root !== undefined && start.parent === undefined)
    throw new InvalidError("session root requires a parent")
  if (start.tags.length > MAX_MEMORY_TAGS) throw new InvalidError("session has too many tags")
  if (start.tags.some((tag) => utf8Length(tag) > MAX_MEMORY_TAG_BYTES))
    throw new InvalidError("session tag exceeds its byte cap")
}

function validateStartAncestry(start: SessionStart, envelope: AgentEnvelope): void {
  const sameParent =
    start.parent === undefined
      ? envelope.parent === undefined
      : envelope.parent?.equals(start.parent) === true
  const sameRoot =
    start.root === undefined
      ? envelope.root === undefined
      : envelope.root?.equals(start.root) === true
  if (!sameParent || !sameRoot)
    throw new InvalidError("session ancestry must match envelope parent and root")
}

export function encodeSdkInfo(sdk: SdkInfo): Map<string, unknown> {
  return new Map<string, unknown>([
    ["language", sdk.language],
    ["version", sdk.version]
  ])
}

export function decodeSdkInfo(map: CborMap, context: string): SdkInfo {
  return {
    language: field.requiredString(map, "language", context),
    version: field.requiredString(map, "version", context)
  }
}

export function encodeBudget(budget: Budget): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (budget.tokens !== undefined) map.set("tokens", budget.tokens)
  if (budget.costMicros !== undefined) map.set("cost_micros", budget.costMicros)
  return map
}

export function decodeBudget(map: CborMap, context: string): Budget {
  const tokens = field.optionalU64(map, "tokens", context)
  const costMicros = field.optionalU64(map, "cost_micros", context)
  return {
    ...(tokens !== undefined ? { tokens } : {}),
    ...(costMicros !== undefined ? { costMicros } : {})
  }
}

export function encodeSessionStart(start: SessionStart): Map<string, unknown> {
  validateSessionStart(start)
  const map = new Map<string, unknown>()
  if (start.label !== undefined) map.set("label", start.label)
  if (start.namespace !== undefined) map.set("namespace", start.namespace)
  map.set("agent", start.agent)
  map.set("sdk", encodeSdkInfo(start.sdk))
  if (start.parent !== undefined) map.set("parent", start.parent.toBytes())
  if (start.root !== undefined) map.set("root", start.root.toBytes())
  if (start.idleTimeoutMicros !== undefined) map.set("idle_timeout_micros", start.idleTimeoutMicros)
  if (start.budget !== undefined) map.set("budget", encodeBudget(start.budget))
  if (start.tags.length > 0) map.set("tags", [...start.tags])
  return map
}

export function decodeSessionStart(map: CborMap, context: string): SessionStart {
  const label = field.optionalString(map, "label", context)
  const namespace = field.optionalString(map, "namespace", context)
  const sdk = field.requiredMap(map, "sdk", context)
  const parent = field.optionalBytes(map, "parent", context)
  const root = field.optionalBytes(map, "root", context)
  const idleTimeoutMicros = field.optionalU64(map, "idle_timeout_micros", context)
  const budgetMap = field.optionalMap(map, "budget", context)
  const tags = field.optionalArray(map, "tags", context, (tag, index) => {
    if (typeof tag !== "string")
      throw new CodecError(`session tag ${String(index)} must be a string`, context, "tags")
    return tag
  })
  const start: SessionStart = {
    ...(label !== undefined ? { label } : {}),
    ...(namespace !== undefined ? { namespace } : {}),
    agent: parseAgentId(field.requiredString(map, "agent", context)),
    sdk: decodeSdkInfo(sdk, `${context}.sdk`),
    ...(parent !== undefined ? { parent: ConversationId.fromBytes(parent) } : {}),
    ...(root !== undefined ? { root: ConversationId.fromBytes(root) } : {}),
    ...(idleTimeoutMicros !== undefined ? { idleTimeoutMicros } : {}),
    ...(budgetMap !== undefined ? { budget: decodeBudget(budgetMap, `${context}.budget`) } : {}),
    tags
  }
  validateSessionStart(start)
  return start
}

export function encodeSessionTransition(transition: SessionTransition): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (transition.actor !== undefined) map.set("actor", transition.actor)
  if (transition.acknowledges !== undefined)
    map.set("acknowledges", logPositionToBytes(transition.acknowledges))
  return map
}

export function decodeSessionTransition(map: CborMap, context: string): SessionTransition {
  const actor = field.optionalString(map, "actor", context)
  const acknowledges = field.optionalBytes(map, "acknowledges", context)
  return {
    ...(actor !== undefined ? { actor: parseAgentId(actor) } : {}),
    ...(acknowledges !== undefined ? { acknowledges: logPositionFromBytes(acknowledges) } : {})
  }
}

/** The JSON text of a pause request. An empty participant set is omitted. */
export function encodeSessionPauseRequestJson(request: SessionPauseRequest): string {
  return request.participants.length === 0
    ? "{}"
    : JSON.stringify({ participants: request.participants })
}

/** Read a pause request body. An empty or absent body names no participant. */
export function decodeSessionPauseRequestJson(body: Uint8Array): SessionPauseRequest {
  if (body.byteLength === 0) return { participants: [] }
  let value: unknown
  try {
    value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(body))
  } catch (cause) {
    throw new CodecError("invalid session pause request", "agent", "pause", { cause })
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new CodecError("a session pause request is a JSON object", "agent", "pause")
  }
  const participants = (value as { readonly participants?: unknown }).participants
  if (participants === undefined) return { participants: [] }
  if (!Array.isArray(participants) || participants.some((agent) => typeof agent !== "string")) {
    throw new CodecError("pause participants are agent ids", "agent", "participants")
  }
  return { participants: (participants as string[]).map((agent) => parseAgentId(agent)) }
}

export function encodeSessionParking(parking: SessionParking): Map<string, unknown> {
  return new Map<string, unknown>([
    ["source", encodeSourceRef(parking.source)],
    ["role", parking.role],
    ["request", logPositionToBytes(parking.request)]
  ])
}

export function decodeSessionParking(map: CborMap, context: string): SessionParking {
  return {
    source: decodeSourceRef(map.get("source"), `${context}.source`),
    role: parseAgentId(field.requiredString(map, "role", context)),
    request: logPositionFromBytes(field.requiredBytes(map, "request", context))
  }
}

/** A parking must point at a log record with its topic generation. */
export function validateSessionParking(parking: SessionParking): void {
  if (parking.source.kind !== "message") {
    throw new InvalidError("a parked record must point at a log record")
  }
  if (parking.source.generation === undefined) {
    throw new InvalidError("a parked record must name its source topic generation")
  }
}

export function encodeSessionEnd(end: SessionEnd): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (end.reason !== undefined) map.set("reason", end.reason)
  if (end.error !== undefined) map.set("error", encodeAgentErrorBody(end.error))
  return map
}

export function decodeSessionEnd(map: CborMap, context: string): SessionEnd {
  const reason = field.optionalString(map, "reason", context)
  const error = field.optionalMap(map, "error", context)
  return {
    ...(reason !== undefined ? { reason } : {}),
    ...(error !== undefined ? { error: decodeAgentErrorBody(error, `${context}.error`) } : {})
  }
}

export const TaskStateName = {
  Submitted: 1,
  Working: 2,
  InputRequired: 3,
  Completed: 4,
  Canceled: 5,
  Failed: 6,
  Rejected: 7,
  AuthRequired: 8,
  Unknown: 9,
  Paused: 10
} as const

const TASK_STATE_DISPLAY: Readonly<Record<keyof typeof TaskStateName, string>> = {
  Submitted: "submitted",
  Working: "working",
  InputRequired: "input-required",
  Completed: "completed",
  Canceled: "canceled",
  Failed: "failed",
  Rejected: "rejected",
  AuthRequired: "auth-required",
  Unknown: "unknown",
  Paused: "paused"
}

const TASK_STATE_NAME_BY_CODE: ReadonlyMap<number, keyof typeof TaskStateName> = new Map(
  Object.entries(TaskStateName).map(([name, code]) => [code, name as keyof typeof TaskStateName])
)

const TERMINAL_TASK_STATES: ReadonlySet<keyof typeof TaskStateName> = new Set([
  "Completed",
  "Canceled",
  "Failed",
  "Rejected"
])

export function taskStateFromCode(code: number): TaskState {
  const name = TASK_STATE_NAME_BY_CODE.get(code)
  return name === undefined ? { kind: "unrecognized", code } : { kind: "known", name }
}

export function taskStateCode(state: TaskState): number {
  return state.kind === "known" ? TaskStateName[state.name] : state.code
}

export function taskStateDisplay(state: TaskState): string {
  return state.kind === "known"
    ? TASK_STATE_DISPLAY[state.name]
    : `unrecognized-${String(state.code)}`
}

export function taskStateIsTerminal(state: TaskState): boolean {
  return state.kind === "known" && TERMINAL_TASK_STATES.has(state.name)
}

export type AgentErrorCode =
  | { readonly kind: "known"; readonly name: keyof typeof AgentErrorCodeName }
  | { readonly kind: "unrecognized"; readonly code: number }

export const AgentErrorCodeName = {
  InvalidRequest: 1,
  Unauthorized: 2,
  Unsupported: 3,
  DeadlineExceeded: 4,
  Cancelled: 5,
  ToolFailure: 6,
  Internal: 7
} as const

const AGENT_ERROR_NAME_BY_CODE: ReadonlyMap<number, keyof typeof AgentErrorCodeName> = new Map(
  Object.entries(AgentErrorCodeName).map(([name, code]) => [
    code,
    name as keyof typeof AgentErrorCodeName
  ])
)

export function agentErrorCodeFromCode(code: number): AgentErrorCode {
  const name = AGENT_ERROR_NAME_BY_CODE.get(code)
  return name === undefined ? { kind: "unrecognized", code } : { kind: "known", name }
}

export function agentErrorCode(value: AgentErrorCode): number {
  return value.kind === "known" ? AgentErrorCodeName[value.name] : value.code
}

export type DeadLetterReason =
  | { readonly kind: "known"; readonly name: keyof typeof DeadLetterReasonName }
  | { readonly kind: "unrecognized"; readonly code: number }

export const DeadLetterReasonName = {
  RetryExhausted: 1,
  Rejected: 2,
  DecodeFailed: 3,
  DeadlineExceeded: 4
} as const

const DEAD_LETTER_NAME_BY_CODE: ReadonlyMap<number, keyof typeof DeadLetterReasonName> = new Map(
  Object.entries(DeadLetterReasonName).map(([name, code]) => [
    code,
    name as keyof typeof DeadLetterReasonName
  ])
)

export function deadLetterReasonFromCode(code: number): DeadLetterReason {
  const name = DEAD_LETTER_NAME_BY_CODE.get(code)
  return name === undefined ? { kind: "unrecognized", code } : { kind: "known", name }
}

export function deadLetterReasonCode(value: DeadLetterReason): number {
  return value.kind === "known" ? DeadLetterReasonName[value.name] : value.code
}

export type Health =
  | { readonly kind: "known"; readonly name: keyof typeof HealthName }
  | { readonly kind: "unrecognized"; readonly code: number }

export const HealthName = {
  Healthy: 1,
  Degraded: 2,
  Unavailable: 3
} as const

const HEALTH_NAME_BY_CODE: ReadonlyMap<number, keyof typeof HealthName> = new Map(
  Object.entries(HealthName).map(([name, code]) => [code, name as keyof typeof HealthName])
)

export function healthFromCode(code: number): Health {
  const name = HEALTH_NAME_BY_CODE.get(code)
  return name === undefined ? { kind: "unrecognized", code } : { kind: "known", name }
}

export function healthCode(value: Health): number {
  return value.kind === "known" ? HealthName[value.name] : value.code
}

export interface TokenUsage {
  readonly inputTokens: bigint
  readonly outputTokens: bigint
  readonly reasoningOutputTokens?: bigint
  readonly cacheReadInputTokens?: bigint
  readonly cacheCreationInputTokens?: bigint
  readonly costMicros?: bigint
}

export function encodeTokenUsage(usage: TokenUsage): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("input_tokens", usage.inputTokens)
  map.set("output_tokens", usage.outputTokens)
  if (usage.reasoningOutputTokens !== undefined) {
    map.set("reasoning_output_tokens", usage.reasoningOutputTokens)
  }
  if (usage.cacheReadInputTokens !== undefined) {
    map.set("cache_read_input_tokens", usage.cacheReadInputTokens)
  }
  if (usage.cacheCreationInputTokens !== undefined) {
    map.set("cache_creation_input_tokens", usage.cacheCreationInputTokens)
  }
  if (usage.costMicros !== undefined) map.set("cost_micros", usage.costMicros)
  return map
}

export function decodeTokenUsage(map: CborMap, context: string): TokenUsage {
  const reasoningOutputTokens = field.optionalU64(map, "reasoning_output_tokens", context)
  const cacheReadInputTokens = field.optionalU64(map, "cache_read_input_tokens", context)
  const cacheCreationInputTokens = field.optionalU64(map, "cache_creation_input_tokens", context)
  const costMicros = field.optionalU64(map, "cost_micros", context)
  return {
    inputTokens: field.requiredU64(map, "input_tokens", context),
    outputTokens: field.requiredU64(map, "output_tokens", context),
    ...(reasoningOutputTokens !== undefined ? { reasoningOutputTokens } : {}),
    ...(cacheReadInputTokens !== undefined ? { cacheReadInputTokens } : {}),
    ...(cacheCreationInputTokens !== undefined ? { cacheCreationInputTokens } : {}),
    ...(costMicros !== undefined ? { costMicros } : {})
  }
}

export interface AgentErrorBody {
  readonly code: AgentErrorCode
  readonly message?: string
  readonly retryable: boolean
  readonly detail?: ReadonlyMap<string, Value>
}

export function encodeAgentErrorBody(body: AgentErrorBody): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("code", agentErrorCode(body.code))
  if (body.message !== undefined) map.set("message", body.message)
  if (body.retryable) map.set("retryable", body.retryable)
  if (body.detail !== undefined) map.set("detail", encodeValueMap(body.detail))
  return map
}

export function encodeValueMap(values: ReadonlyMap<string, Value>): Map<string, unknown> {
  const map = new Map<string, unknown>()
  for (const [key, value] of values) map.set(key, encodeValue(value))
  return map
}

export function decodeValueMap(map: CborMap, context: string): ReadonlyMap<string, Value> {
  const result = new Map<string, Value>()
  for (const [key, value] of map) {
    if (typeof key !== "string") {
      throw new CodecError(`expected a string-keyed map in ${context}`, context, "map")
    }
    result.set(key, decodeValue(value, `${context}.${key}`))
  }
  return result
}

export function decodeAgentErrorBody(map: CborMap, context: string): AgentErrorBody {
  const message = field.optionalString(map, "message", context)
  const detailMap = field.optionalMap(map, "detail", context)
  return {
    code: agentErrorCodeFromCode(field.requiredU8(map, "code", context)),
    ...(message !== undefined ? { message } : {}),
    retryable: map.has("retryable") ? field.requiredBoolean(map, "retryable", context) : false,
    ...(detailMap !== undefined ? { detail: decodeValueMap(detailMap, context) } : {})
  }
}

export interface AgentDeadLetter {
  readonly source: LogPosition
  readonly reason: DeadLetterReason
  readonly attempts: number
  readonly detail?: string
  readonly payload: Uint8Array
}

export function encodeAgentDeadLetter(letter: AgentDeadLetter): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("source", logPositionToBytes(letter.source))
  map.set("reason", deadLetterReasonCode(letter.reason))
  map.set("attempts", letter.attempts)
  if (letter.detail !== undefined) map.set("detail", letter.detail)
  map.set("payload", letter.payload)
  return map
}

export function decodeAgentDeadLetter(map: CborMap, context: string): AgentDeadLetter {
  const detail = field.optionalString(map, "detail", context)
  return {
    source: logPositionFromBytes(field.requiredBytes(map, "source", context)),
    reason: deadLetterReasonFromCode(field.requiredU8(map, "reason", context)),
    attempts: field.requiredU32(map, "attempts", context),
    ...(detail !== undefined ? { detail } : {}),
    payload: field.requiredBytes(map, "payload", context)
  }
}

function parseContentTypeName(value: string, context: string): ContentType {
  if ((Object.values(ContentType) as string[]).includes(value)) {
    return value as ContentType
  }
  throw new CodecError(
    `unknown content type name \`${value}\` in ${context}`,
    context,
    "content_type"
  )
}

export type ContentRef =
  | { readonly kind: "contentType"; readonly value: ContentType }
  | { readonly kind: "schemaId"; readonly value: string }

export function encodeContentRef(ref: ContentRef): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (ref.kind === "contentType") {
    map.set("content_type", ref.value)
  } else {
    map.set("schema_id", ref.value)
  }
  return map
}

export function decodeContentRef(value: unknown, context: string): ContentRef {
  const map = expectMap(value, context)
  if (map.has("content_type")) {
    return {
      kind: "contentType",
      value: parseContentTypeName(field.requiredString(map, "content_type", context), context)
    }
  }
  if (map.has("schema_id")) {
    return { kind: "schemaId", value: field.requiredString(map, "schema_id", context) }
  }
  throw new CodecError(
    `content ref in ${context} must have \`content_type\` or \`schema_id\``,
    context,
    "content_ref"
  )
}

function cappedString(value: string | undefined, field_: string, cap: number): void {
  if (value !== undefined) {
    const bytes = utf8Length(value)
    if (bytes > cap) rejectValidation({ kind: "tooLarge", field: field_, size: bytes, cap })
  }
}

/**
 * A validity-matrix or cap violation of an agent message, card, presence, body
 * reference, or signature. The thrown `InvalidError` carries it as `context`.
 */
export type ValidateError =
  | { readonly kind: "missing"; readonly agentKind: AgentKind; readonly field: string }
  | { readonly kind: "forbidden"; readonly agentKind: AgentKind; readonly field: string }
  | {
      readonly kind: "tooLarge"
      readonly field: string
      readonly size: number
      readonly cap: number
    }
  | { readonly kind: "invalid"; readonly field: string; readonly reason: string }

function validateErrorMessage(error: ValidateError): string {
  switch (error.kind) {
    case "missing":
      return `${error.agentKind} requires \`${error.field}\``
    case "forbidden":
      return `\`${error.field}\` is invalid on ${error.agentKind}`
    case "tooLarge":
      return `\`${error.field}\` is ${String(error.size)}B, exceeds cap ${String(error.cap)}B`
    case "invalid":
      return `\`${error.field}\`: ${error.reason}`
  }
}

function rejectValidation(error: ValidateError): never {
  throw new InvalidError(validateErrorMessage(error), error)
}

export interface CapabilityDescriptor {
  readonly skillId: string
  readonly input?: ContentRef
  readonly output?: ContentRef
  readonly costClass?: number
  readonly latencyClass?: number
  readonly maxConcurrency?: number
  readonly health?: Health
  readonly load?: number
}

export function encodeCapabilityDescriptor(capability: CapabilityDescriptor): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("skill_id", capability.skillId)
  if (capability.input !== undefined) map.set("input", encodeContentRef(capability.input))
  if (capability.output !== undefined) map.set("output", encodeContentRef(capability.output))
  if (capability.costClass !== undefined) map.set("cost_class", capability.costClass)
  if (capability.latencyClass !== undefined) map.set("latency_class", capability.latencyClass)
  if (capability.maxConcurrency !== undefined) map.set("max_concurrency", capability.maxConcurrency)
  if (capability.health !== undefined) map.set("health", healthCode(capability.health))
  if (capability.load !== undefined) map.set("load", capability.load)
  return map
}

export function decodeCapabilityDescriptor(value: unknown, context: string): CapabilityDescriptor {
  const map = expectMap(value, context)
  const input = field.optionalMap(map, "input", context)
  const output = field.optionalMap(map, "output", context)
  const costClass = field.optionalU8(map, "cost_class", context)
  const latencyClass = field.optionalU8(map, "latency_class", context)
  const maxConcurrency = field.optionalU32(map, "max_concurrency", context)
  const health = field.optionalU8(map, "health", context)
  const load = field.optionalU16(map, "load", context)
  return {
    skillId: field.requiredString(map, "skill_id", context),
    ...(input !== undefined ? { input: decodeContentRef(input, `${context}.input`) } : {}),
    ...(output !== undefined ? { output: decodeContentRef(output, `${context}.output`) } : {}),
    ...(costClass !== undefined ? { costClass } : {}),
    ...(latencyClass !== undefined ? { latencyClass } : {}),
    ...(maxConcurrency !== undefined ? { maxConcurrency } : {}),
    ...(health !== undefined ? { health: healthFromCode(health) } : {}),
    ...(load !== undefined ? { load } : {})
  }
}

export interface AgentCard {
  readonly name?: string
  readonly version?: string
  readonly capabilities: readonly CapabilityDescriptor[]
  readonly ttlMicros?: bigint
}

export function validateAgentCard(card: AgentCard): void {
  cappedString(card.name, "name", MAX_AGENT_STRING_BYTES)
  cappedString(card.version, "version", MAX_AGENT_STRING_BYTES)
  if (card.capabilities.length > MAX_CARD_CAPABILITIES) {
    rejectValidation({
      kind: "tooLarge",
      field: "capabilities",
      size: card.capabilities.length,
      cap: MAX_CARD_CAPABILITIES
    })
  }
  for (const capability of card.capabilities) {
    cappedString(capability.skillId, "capability skill_id", MAX_AGENT_STRING_BYTES)
  }
}

export function encodeAgentCard(card: AgentCard): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (card.name !== undefined) map.set("name", card.name)
  if (card.version !== undefined) map.set("version", card.version)
  if (card.capabilities.length > 0) {
    map.set("capabilities", card.capabilities.map(encodeCapabilityDescriptor))
  }
  if (card.ttlMicros !== undefined) map.set("ttl_micros", card.ttlMicros)
  return map
}

export function decodeAgentCard(map: CborMap, context: string): AgentCard {
  const name = field.optionalString(map, "name", context)
  const version = field.optionalString(map, "version", context)
  const ttlMicros = field.optionalU64(map, "ttl_micros", context)
  return {
    ...(name !== undefined ? { name } : {}),
    ...(version !== undefined ? { version } : {}),
    capabilities: field.optionalArray(map, "capabilities", context, (item) =>
      decodeCapabilityDescriptor(item, `${context}.capabilities`)
    ),
    ...(ttlMicros !== undefined ? { ttlMicros } : {})
  }
}

export interface AgentPresence {
  readonly v: number
  readonly agent: AgentId
  readonly inbox?: string
}

export function newAgentPresence(agent: AgentId, inbox?: string): AgentPresence {
  return { v: 1, agent, ...(inbox !== undefined ? { inbox } : {}) }
}

export function validateAgentPresence(presence: AgentPresence): void {
  cappedString(presence.inbox, "inbox", MAX_AGENT_STRING_BYTES)
}

export function encodeAgentPresence(presence: AgentPresence): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", presence.v)
  map.set("agent", presence.agent)
  if (presence.inbox !== undefined) map.set("inbox", presence.inbox)
  return map
}

export function decodeAgentPresence(map: CborMap, context: string): AgentPresence {
  const inbox = field.optionalString(map, "inbox", context)
  return {
    v: field.requiredU32(map, "v", context),
    agent: parseAgentId(field.requiredString(map, "agent", context)),
    ...(inbox !== undefined ? { inbox } : {})
  }
}

const SHA256_BYTES = 32

export interface BodyRef {
  readonly reference: string
  readonly sizeBytes: bigint
  readonly sha256: Uint8Array
  readonly encryption?: number
}

export function newBodyRef(reference: string, sizeBytes: bigint, sha256: Uint8Array): BodyRef {
  return { reference, sizeBytes, sha256 }
}

export function validateBodyRef(ref: BodyRef): void {
  if (ref.reference.length === 0) {
    rejectValidation({
      kind: "invalid",
      field: "reference",
      reason: "reference must not be empty"
    })
  }
  const referenceBytes = utf8Length(ref.reference)
  if (referenceBytes > MAX_BODY_REFERENCE_BYTES) {
    rejectValidation({
      kind: "tooLarge",
      field: "reference",
      size: referenceBytes,
      cap: MAX_BODY_REFERENCE_BYTES
    })
  }
  if (ref.sha256.length !== SHA256_BYTES) {
    rejectValidation({
      kind: "invalid",
      field: "sha256",
      reason: `digest must be ${String(SHA256_BYTES)} bytes, got ${String(ref.sha256.length)}`
    })
  }
}

export function encodeBodyRef(ref: BodyRef): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("reference", ref.reference)
  map.set("size_bytes", ref.sizeBytes)
  map.set("sha256", ref.sha256)
  if (ref.encryption !== undefined) map.set("encryption", ref.encryption)
  return map
}

export function decodeBodyRef(map: CborMap, context: string): BodyRef {
  const encryption = field.optionalU8(map, "encryption", context)
  return {
    reference: field.requiredString(map, "reference", context),
    sizeBytes: field.requiredU64(map, "size_bytes", context),
    sha256: field.requiredBytes(map, "sha256", context),
    ...(encryption !== undefined ? { encryption } : {})
  }
}

export interface SignatureContext {
  readonly contentType?: number
  readonly agentVersion?: number
}

export function encodeSignatureContext(context: SignatureContext): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (context.contentType !== undefined) map.set("content_type", context.contentType)
  if (context.agentVersion !== undefined) map.set("agent_version", context.agentVersion)
  return map
}

export function decodeSignatureContext(map: CborMap, context: string): SignatureContext {
  const contentType = field.optionalU8(map, "content_type", context)
  const agentVersion = field.optionalU32(map, "agent_version", context)
  return {
    ...(contentType !== undefined ? { contentType } : {}),
    ...(agentVersion !== undefined ? { agentVersion } : {})
  }
}

export const SIGNATURE_SCHEME_ED25519 = 1
export const SIGNATURE_DOMAIN = new TextEncoder().encode("agdx.signature.v1")

const ED25519_KEY_ID_BYTES = 8
const ED25519_SIGNATURE_BYTES = 64

export interface Signature {
  readonly scheme: number
  readonly keyId: Uint8Array
  readonly bytes: Uint8Array
  readonly context?: SignatureContext
}

export function validateSignature(signature: Signature): void {
  if (signature.scheme !== SIGNATURE_SCHEME_ED25519) return
  if (signature.keyId.length !== ED25519_KEY_ID_BYTES) {
    rejectValidation({
      kind: "invalid",
      field: "key_id",
      reason: `Ed25519 key id must be ${String(ED25519_KEY_ID_BYTES)} bytes, got ${String(signature.keyId.length)}`
    })
  }
  if (signature.bytes.length !== ED25519_SIGNATURE_BYTES) {
    rejectValidation({
      kind: "invalid",
      field: "bytes",
      reason: `Ed25519 signature must be ${String(ED25519_SIGNATURE_BYTES)} bytes, got ${String(signature.bytes.length)}`
    })
  }
}

export function encodeSignature(signature: Signature): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("scheme", signature.scheme)
  map.set("key_id", signature.keyId)
  map.set("bytes", signature.bytes)
  if (signature.context !== undefined) map.set("context", encodeSignatureContext(signature.context))
  return map
}

export function decodeSignature(map: CborMap, context: string): Signature {
  const contextMap = field.optionalMap(map, "context", context)
  return {
    scheme: field.requiredU8(map, "scheme", context),
    keyId: field.requiredBytes(map, "key_id", context),
    bytes: field.requiredBytes(map, "bytes", context),
    ...(contextMap !== undefined
      ? { context: decodeSignatureContext(contextMap, `${context}.context`) }
      : {})
  }
}

export const features = {
  NONE: 0n
} as const

export const OPERATION_TASK = "task"
export const OPERATION_SESSION = "session"
/** The event that records one work record an agent held while its session
 * was paused. The body is a `SessionParking`. */
export const OPERATION_SESSION_PARKED = "session_parked"
/** The event that records that an agent handled a held record after the
 * resume. The body is the `SessionParking` of the held record. */
export const OPERATION_SESSION_UNPARKED = "session_unparked"
export const OPERATION_CARD = "card"
export const OPERATION_PROGRESS = "progress"
export const OPERATION_QUARANTINE = "quarantine"
export const OPERATION_UNQUARANTINE = "unquarantine"
export const OPERATION_CHAT = "chat"
export const OPERATION_REASONING = "reasoning"
export const OPERATION_TOOL_ARGS = "tool_args"
export const OPERATION_STATE_SNAPSHOT = "state_snapshot"
export const OPERATION_STATE_DELTA = "state_delta"

export const METADATA_ROLE = "role"
export const METADATA_BRIDGE_HOPS = "bridge_hops"
export const METADATA_DELEGATED_BY = "on_behalf_of"
export const METADATA_PURPOSE = "purpose"
export const METADATA_DATA_CLASSIFICATION = "data_classification"
export const METADATA_TASK_CONTEXT = "task_context"
export const METADATA_SESSION_INTENT = "session_intent"
/** The metadata key for a model call's requested model. */
export const METADATA_REQUEST_MODEL = "gen_ai.request.model"
/** The metadata key for the model that answered a call. */
export const METADATA_RESPONSE_MODEL = "gen_ai.response.model"
/** The metadata key for the provider that served a model call. */
export const METADATA_PROVIDER_NAME = "gen_ai.provider.name"
/** The metadata key that marks a command as the first command of a submitted
 * session. The receiving agent marks the session working when it picks the
 * command up. */
export const METADATA_SUBMITTED = "submitted"
/** The metadata key for a call's duration in microseconds, measured by the
 * application around the provider or tool call. */
export const METADATA_DURATION_MICROS = "duration_micros"

/** The estimated token count of `bytes` bytes of context: one token per four
 * bytes, rounded up. Every SDK and the session fold use this one estimate. */
export function estimateTokens(bytes: number): bigint {
  if (!Number.isSafeInteger(bytes) || bytes < 0)
    throw new InvalidError("a byte count must be a non-negative safe integer")
  return BigInt(Math.ceil(bytes / 4))
}

export interface AgentEnvelope {
  readonly kind: AgentKind
  readonly record?: RecordId
  readonly conversation: ConversationId
  readonly parent?: ConversationId
  readonly root?: ConversationId
  readonly source: AgentId
  readonly target?: AgentId
  readonly cause?: RecordId
  readonly causeAt?: LogPosition
  readonly correlation?: CorrelationId
  readonly channel?: ChannelId
  readonly idempotencyKey?: IdempotencyKey
  readonly deadlineMicros?: bigint
  readonly sequence?: bigint
  readonly last: boolean
  readonly finishReason?: string
  readonly taskState?: TaskState
  readonly operation?: string
  readonly tool?: string
  readonly usage?: TokenUsage
  readonly metadata?: ReadonlyMap<string, Value>
  readonly mustUnderstand: bigint
  readonly body: Uint8Array
  readonly signature?: Signature
}

function baseEnvelope(
  kind: AgentKind,
  conversation: ConversationId,
  source: AgentId
): AgentEnvelope {
  return { kind, conversation, source, last: false, mustUnderstand: 0n, body: new Uint8Array(0) }
}

export function commandEnvelope(
  record: RecordId,
  conversation: ConversationId,
  source: AgentId,
  correlation: CorrelationId,
  body: Uint8Array
): AgentEnvelope {
  return { ...baseEnvelope(AgentKind.Command, conversation, source), record, correlation, body }
}

export function responseEnvelope(
  record: RecordId,
  conversation: ConversationId,
  source: AgentId,
  correlation: CorrelationId,
  body: Uint8Array
): AgentEnvelope {
  return { ...baseEnvelope(AgentKind.Response, conversation, source), record, correlation, body }
}

export function eventEnvelope(
  record: RecordId,
  conversation: ConversationId,
  source: AgentId,
  body: Uint8Array
): AgentEnvelope {
  return { ...baseEnvelope(AgentKind.Event, conversation, source), record, body }
}

export function chunkEnvelope(
  conversation: ConversationId,
  source: AgentId,
  correlation: CorrelationId,
  channel: ChannelId,
  sequence: bigint,
  body: Uint8Array
): AgentEnvelope {
  return {
    ...baseEnvelope(AgentKind.Chunk, conversation, source),
    correlation,
    channel,
    sequence,
    body
  }
}

export function statusEnvelope(
  record: RecordId,
  conversation: ConversationId,
  source: AgentId,
  operation: string
): AgentEnvelope {
  return { ...baseEnvelope(AgentKind.Status, conversation, source), record, operation }
}

export function errorEnvelope(
  record: RecordId,
  conversation: ConversationId,
  source: AgentId,
  correlation: CorrelationId,
  body: Uint8Array
): AgentEnvelope {
  return { ...baseEnvelope(AgentKind.Error, conversation, source), record, correlation, body }
}

export function withTarget(envelope: AgentEnvelope, target: AgentId): AgentEnvelope {
  return { ...envelope, target }
}

export function withCause(
  envelope: AgentEnvelope,
  cause: RecordId,
  causeAt?: LogPosition
): AgentEnvelope {
  return { ...envelope, cause, ...(causeAt !== undefined ? { causeAt } : {}) }
}

export function withCorrelation(
  envelope: AgentEnvelope,
  correlation: CorrelationId
): AgentEnvelope {
  return { ...envelope, correlation }
}

export function withIdempotencyKey(envelope: AgentEnvelope, key: IdempotencyKey): AgentEnvelope {
  return { ...envelope, idempotencyKey: key }
}

export function withDeadlineMicros(envelope: AgentEnvelope, deadlineMicros: bigint): AgentEnvelope {
  return { ...envelope, deadlineMicros }
}

export function terminal(envelope: AgentEnvelope, finishReason: string): AgentEnvelope {
  return { ...envelope, last: true, finishReason }
}

export function withTaskState(envelope: AgentEnvelope, state: TaskState): AgentEnvelope {
  return { ...envelope, taskState: state }
}

export function withOperation(envelope: AgentEnvelope, operation: string): AgentEnvelope {
  return { ...envelope, operation }
}

export function withTool(envelope: AgentEnvelope, tool: string): AgentEnvelope {
  return { ...envelope, tool }
}

export function withUsage(envelope: AgentEnvelope, usage: TokenUsage): AgentEnvelope {
  return { ...envelope, usage }
}

export function withMetadata(envelope: AgentEnvelope, key: string, value: Value): AgentEnvelope {
  const metadata = new Map(envelope.metadata ?? [])
  metadata.set(key, value)
  return { ...envelope, metadata }
}

export function withSignature(envelope: AgentEnvelope, signature: Signature): AgentEnvelope {
  return { ...envelope, signature }
}

export function requiring(envelope: AgentEnvelope, bits: bigint): AgentEnvelope {
  return { ...envelope, mustUnderstand: bits }
}

export function unmetRequirements(envelope: AgentEnvelope, understood: bigint): bigint {
  return envelope.mustUnderstand & ~understood
}

export function encodeAgentEnvelope(envelope: AgentEnvelope): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("kind", envelope.kind)
  if (envelope.record !== undefined) map.set("record", envelope.record.toBytes())
  map.set("conversation", envelope.conversation.toBytes())
  if (envelope.parent !== undefined) map.set("parent", envelope.parent.toBytes())
  if (envelope.root !== undefined) map.set("root", envelope.root.toBytes())
  map.set("source", envelope.source)
  if (envelope.target !== undefined) map.set("target", envelope.target)
  if (envelope.cause !== undefined) map.set("cause", envelope.cause.toBytes())
  if (envelope.causeAt !== undefined) map.set("cause_at", logPositionToBytes(envelope.causeAt))
  if (envelope.correlation !== undefined) map.set("correlation", envelope.correlation.toBytes())
  if (envelope.channel !== undefined) map.set("channel", envelope.channel.toBytes())
  if (envelope.idempotencyKey !== undefined) map.set("idempotency_key", envelope.idempotencyKey)
  if (envelope.deadlineMicros !== undefined) map.set("deadline_micros", envelope.deadlineMicros)
  if (envelope.sequence !== undefined) map.set("sequence", envelope.sequence)
  if (envelope.last) map.set("last", envelope.last)
  if (envelope.finishReason !== undefined) map.set("finish_reason", envelope.finishReason)
  if (envelope.taskState !== undefined) map.set("task_state", taskStateCode(envelope.taskState))
  if (envelope.operation !== undefined) map.set("operation", envelope.operation)
  if (envelope.tool !== undefined) map.set("tool", envelope.tool)
  if (envelope.usage !== undefined) map.set("usage", encodeTokenUsage(envelope.usage))
  if (envelope.metadata !== undefined) map.set("metadata", encodeValueMap(envelope.metadata))
  if (envelope.mustUnderstand !== 0n) map.set("must_understand", envelope.mustUnderstand)
  if (envelope.body.length > 0) map.set("body", envelope.body)
  if (envelope.signature !== undefined) map.set("signature", encodeSignature(envelope.signature))
  return map
}

export function decodeAgentEnvelope(map: CborMap, context: string): AgentEnvelope {
  const record = field.optionalBytes(map, "record", context)
  const parent = field.optionalBytes(map, "parent", context)
  const root = field.optionalBytes(map, "root", context)
  const target = field.optionalString(map, "target", context)
  const cause = field.optionalBytes(map, "cause", context)
  const causeAt = field.optionalBytes(map, "cause_at", context)
  const correlation = field.optionalBytes(map, "correlation", context)
  const channel = field.optionalBytes(map, "channel", context)
  const idempotencyKey = field.optionalString(map, "idempotency_key", context)
  const deadlineMicros = field.optionalU64(map, "deadline_micros", context)
  const sequence = field.optionalU64(map, "sequence", context)
  const finishReason = field.optionalString(map, "finish_reason", context)
  const taskState = field.optionalU8(map, "task_state", context)
  const operation = field.optionalString(map, "operation", context)
  const tool = field.optionalString(map, "tool", context)
  const usageMap = field.optionalMap(map, "usage", context)
  const metadataMap = field.optionalMap(map, "metadata", context)
  const mustUnderstand = field.optionalU64(map, "must_understand", context)
  const body = field.optionalBytes(map, "body", context)
  const signatureMap = field.optionalMap(map, "signature", context)

  return {
    kind: parseAgentKind(field.requiredString(map, "kind", context), context),
    ...(record !== undefined ? { record: RecordId.fromBytes(record) } : {}),
    conversation: ConversationId.fromBytes(field.requiredBytes(map, "conversation", context)),
    ...(parent !== undefined ? { parent: ConversationId.fromBytes(parent) } : {}),
    ...(root !== undefined ? { root: ConversationId.fromBytes(root) } : {}),
    source: parseAgentId(field.requiredString(map, "source", context)),
    ...(target !== undefined ? { target: parseAgentId(target) } : {}),
    ...(cause !== undefined ? { cause: RecordId.fromBytes(cause) } : {}),
    ...(causeAt !== undefined ? { causeAt: logPositionFromBytes(causeAt) } : {}),
    ...(correlation !== undefined ? { correlation: CorrelationId.fromBytes(correlation) } : {}),
    ...(channel !== undefined ? { channel: ChannelId.fromBytes(channel) } : {}),
    ...(idempotencyKey !== undefined
      ? { idempotencyKey: parseIdempotencyKey(idempotencyKey) }
      : {}),
    ...(deadlineMicros !== undefined ? { deadlineMicros } : {}),
    ...(sequence !== undefined ? { sequence } : {}),
    last: map.has("last") ? field.requiredBoolean(map, "last", context) : false,
    ...(finishReason !== undefined ? { finishReason } : {}),
    ...(taskState !== undefined ? { taskState: taskStateFromCode(taskState) } : {}),
    ...(operation !== undefined ? { operation } : {}),
    ...(tool !== undefined ? { tool } : {}),
    ...(usageMap !== undefined ? { usage: decodeTokenUsage(usageMap, context) } : {}),
    ...(metadataMap !== undefined ? { metadata: decodeValueMap(metadataMap, context) } : {}),
    mustUnderstand: mustUnderstand ?? 0n,
    body: body ?? new Uint8Array(0),
    ...(signatureMap !== undefined ? { signature: decodeSignature(signatureMap, context) } : {})
  }
}

const CHUNK_STREAM_OPERATIONS: ReadonlySet<string> = new Set([
  OPERATION_CHAT,
  OPERATION_REASONING,
  OPERATION_TOOL_ARGS
])

const STATUS_OPERATIONS: ReadonlySet<string> = new Set([
  OPERATION_TASK,
  OPERATION_SESSION,
  OPERATION_CARD,
  OPERATION_PROGRESS,
  OPERATION_QUARANTINE,
  OPERATION_UNQUARANTINE
])

export function validateAgentEnvelope(envelope: AgentEnvelope): void {
  const kind = envelope.kind

  const require = (present: boolean, fieldName: string): void => {
    if (!present) rejectValidation({ kind: "missing", agentKind: kind, field: fieldName })
  }
  const forbid = (absent: boolean, fieldName: string): void => {
    if (!absent) rejectValidation({ kind: "forbidden", agentKind: kind, field: fieldName })
  }
  const invalid = (fieldName: string, reason: string): never =>
    rejectValidation({ kind: "invalid", field: fieldName, reason })

  if (kind !== AgentKind.Chunk) {
    require(envelope.record !== undefined, "record")
  }

  if (envelope.root !== undefined && envelope.parent === undefined)
    invalid("root", "root requires parent")
  if (
    envelope.parent?.equals(envelope.conversation) === true ||
    envelope.root?.equals(envelope.conversation) === true
  )
    invalid("parent", "parent and root must differ from conversation")

  switch (kind) {
    case AgentKind.Command:
    case AgentKind.Response:
    case AgentKind.Chunk:
    case AgentKind.Error:
      require(envelope.correlation !== undefined, "correlation")
      break
    case AgentKind.Status:
      if (envelope.operation === OPERATION_TASK) {
        require(envelope.correlation !== undefined, "correlation")
      }
      break
    case AgentKind.Event:
      break
  }

  if (kind === AgentKind.Chunk) {
    require(envelope.channel !== undefined, "channel")
    require(envelope.sequence !== undefined, "sequence")
  } else if (kind === AgentKind.Error) {
    if (envelope.sequence !== undefined && envelope.channel === undefined) {
      invalid("sequence", "sequence requires channel")
    }
  } else {
    forbid(envelope.channel === undefined, "channel")
    forbid(envelope.sequence === undefined, "sequence")
  }

  if (envelope.last && kind !== AgentKind.Chunk && kind !== AgentKind.Status) {
    rejectValidation({ kind: "forbidden", agentKind: kind, field: "last" })
  }

  if (kind === AgentKind.Chunk) {
    if (envelope.finishReason !== undefined && !envelope.last) {
      invalid("finish_reason", "finish_reason rides only the terminal chunk")
    }
  } else if (kind !== AgentKind.Response) {
    forbid(envelope.finishReason === undefined, "finish_reason")
  }

  if (kind === AgentKind.Chunk || kind === AgentKind.Status || kind === AgentKind.Error) {
    forbid(envelope.idempotencyKey === undefined, "idempotency_key")
  }

  if (
    kind === AgentKind.Response ||
    kind === AgentKind.Event ||
    kind === AgentKind.Status ||
    kind === AgentKind.Error
  ) {
    forbid(envelope.deadlineMicros === undefined, "deadline_micros")
  }
  if (
    kind === AgentKind.Chunk &&
    envelope.deadlineMicros !== undefined &&
    envelope.sequence !== 0n
  ) {
    invalid("deadline_micros", "the stream bound rides the opening chunk (sequence 0)")
  }

  if (kind === AgentKind.Status) {
    if (envelope.operation === OPERATION_TASK || envelope.operation === OPERATION_SESSION) {
      require(envelope.taskState !== undefined, "task_state")
    }
  } else if (kind !== AgentKind.Response && kind !== AgentKind.Error) {
    forbid(envelope.taskState === undefined, "task_state")
  }

  if (kind === AgentKind.Status) {
    require(envelope.operation !== undefined, "operation")
    if (envelope.operation !== undefined && !STATUS_OPERATIONS.has(envelope.operation)) {
      invalid(
        "operation",
        `status operation must be \`${OPERATION_TASK}\`, \`${OPERATION_SESSION}\`, \`${OPERATION_CARD}\`, \`${OPERATION_PROGRESS}\`, \`${OPERATION_QUARANTINE}\`, or \`${OPERATION_UNQUARANTINE}\`, got \`${envelope.operation}\``
      )
    }
  } else if (kind === AgentKind.Chunk) {
    if (envelope.sequence === 0n) {
      require(envelope.operation !== undefined, "operation")
    }
    if (envelope.operation !== undefined) {
      if (envelope.sequence !== 0n) {
        invalid("operation", "the stream purpose rides the opening chunk (sequence 0)")
      }
      if (!CHUNK_STREAM_OPERATIONS.has(envelope.operation)) {
        invalid(
          "operation",
          `chunk-stream purpose must be \`${OPERATION_CHAT}\`, \`${OPERATION_REASONING}\`, or \`${OPERATION_TOOL_ARGS}\`, got \`${envelope.operation}\``
        )
      }
    }
  }

  if (kind === AgentKind.Status) {
    forbid(envelope.tool === undefined, "tool")
  }

  if (kind === AgentKind.Command) {
    forbid(envelope.usage === undefined, "usage")
  } else if (kind === AgentKind.Chunk && envelope.usage !== undefined && !envelope.last) {
    invalid("usage", "whole-stream accounting rides the terminal chunk")
  }

  if (kind === AgentKind.Status && envelope.operation === OPERATION_SESSION) {
    require(envelope.body.length > 0, "body")
    const state = envelope.taskState
    if (state === undefined) return invalid("task_state", "session state is required")
    if (envelope.last !== taskStateIsTerminal(state))
      invalid("last", "session last must match terminal task state")
    try {
      const body = expectMap(decodeOne(envelope.body, "session body"), "session body")
      if (state.kind === "known" && state.name === "Submitted") {
        validateStartAncestry(decodeSessionStart(body, "session start"), envelope)
      } else if (state.kind === "known" && state.name === "Working") {
        if (body.has("agent") || body.has("sdk"))
          validateStartAncestry(decodeSessionStart(body, "session start"), envelope)
        else decodeSessionTransition(body, "session transition")
      } else if (taskStateIsTerminal(state)) {
        decodeSessionEnd(body, "session end")
      } else {
        decodeSessionTransition(body, "session transition")
      }
    } catch (error) {
      invalid("body", error instanceof Error ? error.message : String(error))
    }
  } else if (kind === AgentKind.Chunk) {
    if (envelope.body.length === 0 && !envelope.last) {
      rejectValidation({ kind: "missing", agentKind: kind, field: "body" })
    }
  } else if (kind !== AgentKind.Status) {
    require(envelope.body.length > 0, "body")
  }

  cappedString(envelope.operation, "operation", MAX_AGENT_STRING_BYTES)
  cappedString(envelope.tool, "tool", MAX_AGENT_STRING_BYTES)
  cappedString(envelope.finishReason, "finish_reason", MAX_AGENT_STRING_BYTES)

  if (envelope.metadata !== undefined) {
    if (envelope.metadata.size > MAX_METADATA_ENTRIES) {
      rejectValidation({
        kind: "tooLarge",
        field: "metadata",
        size: envelope.metadata.size,
        cap: MAX_METADATA_ENTRIES
      })
    }
    let total = 0
    for (const [key, value] of envelope.metadata) {
      const keySize = utf8Length(key)
      if (keySize > MAX_METADATA_KEY_BYTES) {
        rejectValidation({
          kind: "tooLarge",
          field: "metadata key",
          size: keySize,
          cap: MAX_METADATA_KEY_BYTES
        })
      }
      const size = valueSize(value)
      if (size > MAX_METADATA_VALUE_BYTES) {
        rejectValidation({
          kind: "tooLarge",
          field: "metadata value",
          size,
          cap: MAX_METADATA_VALUE_BYTES
        })
      }
      total += keySize + size
    }
    if (total > MAX_METADATA_TOTAL_BYTES) {
      rejectValidation({
        kind: "tooLarge",
        field: "metadata",
        size: total,
        cap: MAX_METADATA_TOTAL_BYTES
      })
    }
  }

  if (envelope.signature !== undefined) {
    validateSignature(envelope.signature)
  }
}

function valueSize(value: Value): number {
  if (value.kind === "str") return utf8Length(value.value)
  if (value.kind === "list") return value.value.reduce((sum, item) => sum + 1 + valueSize(item), 0)
  return 9
}

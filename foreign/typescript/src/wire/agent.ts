import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, expectMap, field } from "./cbor.js"
import { ContentType } from "./content.js"
import { ChannelId, ConversationId, CorrelationId, RecordId } from "./ids.js"
import type { LogPosition } from "./ids.js"
import { logPositionFromBytes, logPositionToBytes } from "./ids.js"
import {
  MAX_AGENT_STRING_BYTES,
  MAX_BODY_REFERENCE_BYTES,
  MAX_CARD_CAPABILITIES,
  MAX_IDEMPOTENCY_KEY_BYTES,
  MAX_METADATA_ENTRIES,
  MAX_METADATA_KEY_BYTES,
  MAX_METADATA_TOTAL_BYTES,
  MAX_METADATA_VALUE_BYTES
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

export const TaskStateName = {
  Submitted: 1,
  Working: 2,
  InputRequired: 3,
  Completed: 4,
  Canceled: 5,
  Failed: 6,
  Rejected: 7,
  AuthRequired: 8,
  Unknown: 9
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
  Unknown: "unknown"
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
  return map
}

export function decodeTokenUsage(map: CborMap, context: string): TokenUsage {
  const reasoningOutputTokens = field.optionalU64(map, "reasoning_output_tokens", context)
  const cacheReadInputTokens = field.optionalU64(map, "cache_read_input_tokens", context)
  const cacheCreationInputTokens = field.optionalU64(map, "cache_creation_input_tokens", context)
  return {
    inputTokens: field.requiredU64(map, "input_tokens", context),
    outputTokens: field.requiredU64(map, "output_tokens", context),
    ...(reasoningOutputTokens !== undefined ? { reasoningOutputTokens } : {}),
    ...(cacheReadInputTokens !== undefined ? { cacheReadInputTokens } : {}),
    ...(cacheCreationInputTokens !== undefined ? { cacheCreationInputTokens } : {})
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
    if (bytes > cap) {
      throw new InvalidError(`${field_} is ${String(bytes)}B, exceeds cap ${String(cap)}B`, {
        field: field_
      })
    }
  }
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
    throw new InvalidError(
      `capabilities has ${String(card.capabilities.length)} entries, exceeds cap ${String(MAX_CARD_CAPABILITIES)}`,
      { field: "capabilities" }
    )
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
    throw new InvalidError("reference must not be empty", { field: "reference" })
  }
  const referenceBytes = utf8Length(ref.reference)
  if (referenceBytes > MAX_BODY_REFERENCE_BYTES) {
    throw new InvalidError(
      `reference is ${String(referenceBytes)}B, exceeds cap ${String(MAX_BODY_REFERENCE_BYTES)}B`,
      { field: "reference" }
    )
  }
  if (ref.sha256.length !== SHA256_BYTES) {
    throw new InvalidError(
      `digest must be ${String(SHA256_BYTES)} bytes, got ${String(ref.sha256.length)}`,
      { field: "sha256" }
    )
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
    throw new InvalidError(
      `Ed25519 key id must be ${String(ED25519_KEY_ID_BYTES)} bytes, got ${String(signature.keyId.length)}`,
      { field: "key_id" }
    )
  }
  if (signature.bytes.length !== ED25519_SIGNATURE_BYTES) {
    throw new InvalidError(
      `Ed25519 signature must be ${String(ED25519_SIGNATURE_BYTES)} bytes, got ${String(signature.bytes.length)}`,
      { field: "bytes" }
    )
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
export const METADATA_RUN = "run"
export const METADATA_DELEGATED_BY = "on_behalf_of"
export const METADATA_PURPOSE = "purpose"
export const METADATA_DATA_CLASSIFICATION = "data_classification"
export const METADATA_TASK_CONTEXT = "task_context"
export const METADATA_SESSION_INTENT = "session_intent"

export interface AgentEnvelope {
  readonly kind: AgentKind
  readonly record?: RecordId
  readonly conversation: ConversationId
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
  OPERATION_CARD,
  OPERATION_PROGRESS,
  OPERATION_QUARANTINE,
  OPERATION_UNQUARANTINE
])

export function validateAgentEnvelope(envelope: AgentEnvelope): void {
  const kind = envelope.kind

  const require = (present: boolean, fieldName: string): void => {
    if (!present)
      throw new InvalidError(`${kind} requires \`${fieldName}\``, { kind, field: fieldName })
  }
  const forbid = (absent: boolean, fieldName: string): void => {
    if (!absent) {
      throw new InvalidError(`\`${fieldName}\` is invalid on ${kind}`, { kind, field: fieldName })
    }
  }
  const invalid = (fieldName: string, reason: string): never => {
    throw new InvalidError(`\`${fieldName}\`: ${reason}`, { field: fieldName })
  }

  if (kind !== AgentKind.Chunk) {
    require(envelope.record !== undefined, "record")
  }

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
    throw new InvalidError(`\`last\` is invalid on ${kind}`, { kind, field: "last" })
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
    if (envelope.operation === OPERATION_TASK) {
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
        `status operation must be \`${OPERATION_TASK}\`, \`${OPERATION_CARD}\`, \`${OPERATION_PROGRESS}\`, \`${OPERATION_QUARANTINE}\`, or \`${OPERATION_UNQUARANTINE}\`, got \`${envelope.operation}\``
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

  if (kind === AgentKind.Chunk) {
    if (envelope.body.length === 0 && !envelope.last) {
      throw new InvalidError(`${kind} requires \`body\``, { kind, field: "body" })
    }
  } else if (kind !== AgentKind.Status) {
    require(envelope.body.length > 0, "body")
  }

  cappedString(envelope.operation, "operation", MAX_AGENT_STRING_BYTES)
  cappedString(envelope.tool, "tool", MAX_AGENT_STRING_BYTES)
  cappedString(envelope.finishReason, "finish_reason", MAX_AGENT_STRING_BYTES)

  if (envelope.metadata !== undefined) {
    if (envelope.metadata.size > MAX_METADATA_ENTRIES) {
      throw new InvalidError(
        `\`metadata\` is ${String(envelope.metadata.size)} entries, exceeds cap ${String(MAX_METADATA_ENTRIES)}`,
        { field: "metadata" }
      )
    }
    let total = 0
    for (const [key, value] of envelope.metadata) {
      const keySize = utf8Length(key)
      if (keySize > MAX_METADATA_KEY_BYTES) {
        throw new InvalidError(
          `\`metadata key\` is ${String(keySize)}B, exceeds cap ${String(MAX_METADATA_KEY_BYTES)}B`,
          { field: "metadata key" }
        )
      }
      const size = valueSize(value)
      if (size > MAX_METADATA_VALUE_BYTES) {
        throw new InvalidError(
          `\`metadata value\` is ${String(size)}B, exceeds cap ${String(MAX_METADATA_VALUE_BYTES)}B`,
          { field: "metadata value" }
        )
      }
      total += keySize + size
    }
    if (total > MAX_METADATA_TOTAL_BYTES) {
      throw new InvalidError(
        `\`metadata\` is ${String(total)}B, exceeds cap ${String(MAX_METADATA_TOTAL_BYTES)}B`,
        { field: "metadata" }
      )
    }
  }

  if (envelope.signature !== undefined) {
    validateSignature(envelope.signature)
  }
}

function valueSize(value: Value): number {
  if (value.kind === "string") return utf8Length(value.value)
  if (value.kind === "list") return value.value.reduce((sum, item) => sum + 1 + valueSize(item), 0)
  return 9
}

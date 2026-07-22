import { CodecError } from "../client/errors.js"
import {
  decodeProjectionInfo,
  decodeSchemaInfo,
  encodeProjectionInfo,
  encodeSchemaInfo,
  type ProjectionInfo,
  type SchemaInfo
} from "./browse.js"
import { type CborMap, expectMap, field } from "./cbor.js"
import { decodeSchemaDef, encodeSchemaDef, type SchemaDef } from "./control.js"
import { decodeForkInfo, encodeForkInfo, type ForkInfo } from "./fork.js"
import {
  decodeBackendDescriptor,
  decodeOpVersions,
  encodeBackendDescriptor,
  encodeOpVersions,
  type BackendDescriptor,
  type OpVersions
} from "./hello.js"
import { decodeMemoryRowScope, encodeMemoryRowScope, type MemoryRowScope } from "./kv.js"
import {
  consistencyToWord,
  decodeQueryResult,
  encodeQueryResult,
  parseConsistency,
  type Consistency,
  type QueryResult
} from "./query.js"
import { decodeSourceRef, encodeSourceRef, type SourceRef } from "./graph.js"
import { type ResultCode } from "./result.js"
import { decodeWireTopology, encodeWireTopology, type WireTopology } from "./topology.js"

export const CAPABILITIES_PATH = "/agdx/capabilities"
export const QUERY_PATH = "/agdx/query"
export const PROJECTIONS_PATH = "/agdx/projections"
export const BINDINGS_PATH = "/agdx/bindings"
export const SCHEMAS_PATH = "/agdx/schemas"
export const KV_PATH = "/agdx/kv"
export const FORKS_PATH = "/agdx/forks"
export const GRAPHS_PATH = "/agdx/graphs"
export const CLIENTS_PATH = "/agdx/clients"
export const RUNS_PATH = "/agdx/runs"
export const AUTHZ_WHOAMI_PATH = "/agdx/authz/whoami"
export const AUTHZ_ROLES_PATH = "/agdx/authz/roles"

export const authzRolePath = (name: string): string => `${AUTHZ_ROLES_PATH}/${name}`
export const authzUserRolesPath = (userId: number): string =>
  `/agdx/authz/users/${String(userId)}/roles`
export const graphPath = (id: string): string => `${GRAPHS_PATH}/${id}`
export const graphQueryPath = (name: string): string => `/agdx/graph/${name}/query`
export const graphNeighborsPath = (name: string, node: string): string =>
  `/agdx/graph/${name}/neighbors/${node}`
export const projectionPath = (id: string): string => `${PROJECTIONS_PATH}/${id}`
export const schemaPath = (id: number): string => `${SCHEMAS_PATH}/${String(id)}`
export const schemaDecodePath = (id: number): string => `${schemaPath(id)}/decode`
export const kvNamespacePath = (namespace: string): string => `${KV_PATH}/${namespace}`
export const kvEntryPath = (namespace: string, key: string): string =>
  `${kvNamespacePath(namespace)}/${key}`
export const kvCasPath = (namespace: string, key: string): string =>
  `${kvEntryPath(namespace, key)}/cas`
export const forkPath = (id: string): string => `${FORKS_PATH}/${id}`
export const forkPromotePath = (id: string): string => `${forkPath(id)}/promote`
export const forkRowsPath = (id: string): string => `${forkPath(id)}/rows`
export const runPath = (id: string): string => `${RUNS_PATH}/${id}`
export const runCancelPath = (id: string): string => `${runPath(id)}/cancel`

export interface QueryCapsView {
  readonly available: boolean
  readonly projections: boolean
  readonly schemas: boolean
  readonly consistency: Consistency
  readonly keyword: boolean
}

export interface KvCapsView {
  readonly available: boolean
  readonly cas: boolean
  readonly casFenced: boolean
}

export interface HttpCapabilities {
  readonly managed: boolean
  readonly query: QueryCapsView
  readonly kv: KvCapsView
  readonly graph: boolean
  readonly fork: boolean
  readonly agentWorkflow: boolean
  readonly watch: boolean
  readonly authz: boolean
  readonly versions: OpVersions
  readonly backends: readonly BackendDescriptor[]
  readonly topology?: WireTopology
}

export interface KvEntryView {
  readonly key: string
  readonly value: string
  readonly expiresAtMicros?: bigint
  readonly scope?: MemoryRowScope
  readonly source?: SourceRef
}

export interface KvPageView {
  readonly entries: readonly KvEntryView[]
  readonly cursor?: string
}

export interface ErrorBody {
  readonly code: ResultCode
  readonly message: string
  readonly detail?: unknown
}

const RESULT_NAMES: ReadonlyMap<string, ResultCode> = new Map([
  ["ok", { kind: "known", name: "Ok" }],
  ["unsupported", { kind: "known", name: "Unsupported" }],
  ["not_found", { kind: "known", name: "NotFound" }],
  ["invalid_argument", { kind: "known", name: "InvalidArgument" }],
  ["too_large", { kind: "known", name: "TooLarge" }],
  ["conflict", { kind: "known", name: "Conflict" }],
  ["stale", { kind: "known", name: "Stale" }],
  ["version_skew", { kind: "known", name: "VersionSkew" }],
  ["unauthenticated", { kind: "known", name: "Unauthenticated" }],
  ["backend", { kind: "known", name: "Backend" }],
  ["forbidden", { kind: "known", name: "Forbidden" }],
  ["step_up_required", { kind: "known", name: "StepUpRequired" }]
])

const RESULT_WORDS: Readonly<Record<string, string>> = {
  Ok: "ok",
  Unsupported: "unsupported",
  NotFound: "not_found",
  InvalidArgument: "invalid_argument",
  TooLarge: "too_large",
  Conflict: "conflict",
  Stale: "stale",
  VersionSkew: "version_skew",
  Unauthenticated: "unauthenticated",
  Backend: "backend",
  Forbidden: "forbidden",
  StepUpRequired: "step_up_required"
}

function parseJson(text: string, context: string): unknown {
  try {
    return fromJsonValue(JSON.parse(text) as unknown, context)
  } catch (cause) {
    if (cause instanceof CodecError) throw cause
    throw new CodecError(`failed to decode ${context}`, context, "decode", { cause })
  }
}

function fromJsonValue(value: unknown, context: string): unknown {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new CodecError(`non-finite number in ${context}`, context, "number")
    if (!Number.isInteger(value)) return value
    if (!Number.isSafeInteger(value)) {
      throw new CodecError(
        `integer in ${context} exceeds JavaScript's exact range`,
        context,
        "number"
      )
    }
    return value
  }
  if (Array.isArray(value)) {
    if (value.every((item) => Number.isInteger(item) && Number(item) >= 0 && Number(item) <= 255)) {
      return Uint8Array.from(value as number[])
    }
    return value.map((item, index) => fromJsonValue(item, `${context}[${String(index)}]`))
  }
  if (typeof value === "object") {
    return new Map(
      Object.entries(value).map(([key, item]) => [key, fromJsonValue(item, `${context}.${key}`)])
    )
  }
  throw new CodecError(`unsupported JSON value in ${context}`, context, "value")
}

function toJsonValue(value: unknown, context: string): unknown {
  if (typeof value === "bigint") {
    const number = Number(value)
    if (!Number.isSafeInteger(number)) {
      throw new CodecError(
        `integer in ${context} exceeds JavaScript's exact range`,
        context,
        "number"
      )
    }
    return number
  }
  if (value instanceof Uint8Array) return [...value]
  if (value instanceof Map) {
    return Object.fromEntries(
      [...value].map(([key, item]) => {
        if (typeof key !== "string") {
          throw new CodecError(`JSON object key in ${context} must be a string`, context, "key")
        }
        return [key, toJsonValue(item, `${context}.${key}`)]
      })
    )
  }
  if (Array.isArray(value)) {
    return value.map((item, index) => toJsonValue(item, `${context}[${String(index)}]`))
  }
  return value
}

function encodeJson(value: unknown, context: string): string {
  return JSON.stringify(toJsonValue(value, context), null, 2)
}

function decodeList<T>(
  text: string,
  context: string,
  decode: (map: CborMap, context: string) => T
): T[] {
  const value = parseJson(text, context)
  if (!Array.isArray(value)) throw new CodecError(`${context} must be an array`, context, "array")
  return value.map((item, index) =>
    decode(expectMap(item, `${context}[${String(index)}]`), `${context}[${String(index)}]`)
  )
}

export const decodeProjectionListJson = (text: string): ProjectionInfo[] =>
  decodeList(text, "ProjectionInfo[]", decodeProjectionInfo)
export const encodeProjectionListJson = (items: readonly ProjectionInfo[]): string =>
  encodeJson(items.map(encodeProjectionInfo), "ProjectionInfo[]")
export const decodeSchemaListJson = (text: string): SchemaInfo[] =>
  decodeList(text, "SchemaInfo[]", decodeSchemaInfo)
export const encodeSchemaListJson = (items: readonly SchemaInfo[]): string =>
  encodeJson(items.map(encodeSchemaInfo), "SchemaInfo[]")

export function decodeSchemaDefJson(text: string): SchemaDef {
  return decodeSchemaDef(expectMap(parseJson(text, "SchemaDef"), "SchemaDef"), "SchemaDef")
}

export const encodeSchemaDefJson = (schema: SchemaDef): string =>
  encodeJson(encodeSchemaDef(schema), "SchemaDef")

export function decodeForkInfoJson(text: string): ForkInfo {
  return decodeForkInfo(expectMap(parseJson(text, "ForkInfo"), "ForkInfo"), "ForkInfo")
}

export const encodeForkInfoJson = (fork: ForkInfo): string =>
  encodeJson(encodeForkInfo(fork), "ForkInfo")

export function decodeQueryResultJson(text: string): QueryResult {
  return decodeQueryResult(expectMap(parseJson(text, "QueryResult"), "QueryResult"), "QueryResult")
}

export const encodeQueryResultJson = (result: QueryResult): string =>
  encodeJson(encodeQueryResult(result), "QueryResult")

function decodeQueryCaps(map: CborMap, context: string): QueryCapsView {
  return {
    available: field.requiredBoolean(map, "available", context),
    projections: field.requiredBoolean(map, "projections", context),
    schemas: field.requiredBoolean(map, "schemas", context),
    consistency: parseConsistency(
      field.optionalString(map, "consistency", context) ?? "eventual",
      context
    ),
    keyword: field.optionalBoolean(map, "keyword", context) ?? false
  }
}

function encodeQueryCaps(value: QueryCapsView): Map<string, unknown> {
  return new Map<string, unknown>([
    ["available", value.available],
    ["projections", value.projections],
    ["schemas", value.schemas],
    ["consistency", consistencyToWord(value.consistency)],
    ["keyword", value.keyword]
  ])
}

export function decodeCapabilitiesJson(text: string): HttpCapabilities {
  const context = "Capabilities"
  const map = expectMap(parseJson(text, context), context)
  const topology = field.optionalMap(map, "topology", context)
  return {
    managed: field.requiredBoolean(map, "managed", context),
    query: decodeQueryCaps(field.requiredMap(map, "query", context), `${context}.query`),
    kv: {
      available: field.requiredBoolean(
        field.requiredMap(map, "kv", context),
        "available",
        `${context}.kv`
      ),
      cas:
        field.optionalBoolean(field.requiredMap(map, "kv", context), "cas", `${context}.kv`) ??
        false,
      casFenced:
        field.optionalBoolean(
          field.requiredMap(map, "kv", context),
          "cas_fenced",
          `${context}.kv`
        ) ?? false
    },
    graph: field.optionalBoolean(map, "graph", context) ?? false,
    fork: field.requiredBoolean(map, "fork", context),
    agentWorkflow: field.optionalBoolean(map, "agent_workflow", context) ?? false,
    watch: field.optionalBoolean(map, "watch", context) ?? false,
    authz: field.optionalBoolean(map, "authz", context) ?? false,
    versions: decodeOpVersions(field.requiredMap(map, "versions", context), `${context}.versions`),
    backends: field.optionalArray(map, "backends", context, (item, index) =>
      decodeBackendDescriptor(item, `${context}.backends[${String(index)}]`)
    ),
    ...(topology !== undefined
      ? { topology: decodeWireTopology(topology, `${context}.topology`) }
      : {})
  }
}

export function encodeCapabilitiesJson(value: HttpCapabilities): string {
  const map = new Map<string, unknown>([
    ["managed", value.managed],
    ["query", encodeQueryCaps(value.query)],
    [
      "kv",
      new Map([
        ["available", value.kv.available],
        ["cas", value.kv.cas],
        ["cas_fenced", value.kv.casFenced]
      ])
    ],
    ["graph", value.graph],
    ["fork", value.fork],
    ["agent_workflow", value.agentWorkflow],
    ["watch", value.watch],
    ["authz", value.authz],
    ["versions", encodeOpVersions(value.versions)]
  ])
  if (value.backends.length > 0) map.set("backends", value.backends.map(encodeBackendDescriptor))
  if (value.topology !== undefined) map.set("topology", encodeWireTopology(value.topology))
  return encodeJson(map, "Capabilities")
}

function decodeKvEntry(map: CborMap, context: string): KvEntryView {
  const expiresAtMicros = field.optionalU64(map, "expires_at_micros", context)
  const scope = field.optionalMap(map, "scope", context)
  const source = field.optionalMap(map, "source", context)
  return {
    key: field.requiredString(map, "key", context),
    value: field.requiredString(map, "value", context),
    ...(expiresAtMicros !== undefined ? { expiresAtMicros } : {}),
    ...(scope !== undefined ? { scope: decodeMemoryRowScope(scope, `${context}.scope`) } : {}),
    ...(source !== undefined ? { source: decodeSourceRef(source, `${context}.source`) } : {})
  }
}

function encodeKvEntry(value: KvEntryView): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["key", value.key],
    ["value", value.value],
    ["expires_at_micros", value.expiresAtMicros ?? null]
  ])
  if (value.scope !== undefined) map.set("scope", encodeMemoryRowScope(value.scope))
  if (value.source !== undefined) map.set("source", encodeSourceRef(value.source))
  return map
}

export function decodeKvPageJson(text: string): KvPageView {
  const context = "KvPageView"
  const map = expectMap(parseJson(text, context), context)
  const cursor = field.optionalString(map, "cursor", context)
  return {
    entries: field.requiredArray(map, "entries", context, (item, index) =>
      decodeKvEntry(
        expectMap(item, `${context}.entries[${String(index)}]`),
        `${context}.entries[${String(index)}]`
      )
    ),
    ...(cursor !== undefined ? { cursor } : {})
  }
}

export function encodeKvPageJson(value: KvPageView): string {
  return encodeJson(
    new Map<string, unknown>([
      ["entries", value.entries.map(encodeKvEntry)],
      ["cursor", value.cursor ?? null]
    ]),
    "KvPageView"
  )
}

export function decodeErrorBodyJson(text: string): ErrorBody {
  const context = "ErrorBody"
  const map = expectMap(parseJson(text, context), context)
  const word = field.requiredString(map, "code", context)
  const code = RESULT_NAMES.get(word)
  if (code === undefined) throw new CodecError(`unknown result code \`${word}\``, context, "code")
  return {
    code,
    message: field.requiredString(map, "message", context),
    ...(map.has("detail") ? { detail: map.get("detail") } : {})
  }
}

export function encodeErrorBodyJson(value: ErrorBody): string {
  if (value.code.kind === "unrecognized") {
    throw new CodecError(
      "unrecognized numeric result codes have no JSON spelling",
      "ErrorBody",
      "code"
    )
  }
  const map = new Map<string, unknown>([
    ["code", RESULT_WORDS[value.code.name]],
    ["message", value.message]
  ])
  if (value.detail !== undefined) map.set("detail", value.detail)
  return encodeJson(map, "ErrorBody")
}

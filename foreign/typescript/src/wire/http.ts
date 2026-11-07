import { CodecError, InvalidError } from "../client/errors.js"
import {
  decodeProjectionPage,
  decodeSchemaInfo,
  encodeProjectionPage,
  encodeSchemaInfo,
  type ProjectionPage,
  type SchemaInfo
} from "./browse.js"
import { type CborMap, expectMap, field } from "./cbor.js"
import {
  type FilterCodec,
  type FilterGroupIdentity,
  type FilterState,
  decodeFilterCodec
} from "./filter.js"
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
import { type ResultCode, resultCodeFromWord, resultCodeWord } from "./result.js"
import { type SessionError, sessionErrorMessage, sessionErrorResultCode } from "./session.js"
import { decodeWireTopology, encodeWireTopology, type WireTopology } from "./topology.js"
import {
  bigIntToBytes16,
  bytes16ToBigInt,
  type ConversationId,
  crockfordDecode,
  crockfordEncode
} from "./ids.js"

export const CAPABILITIES_PATH = "/agdx/capabilities"
export const QUERY_PATH = "/agdx/query"
export const PROJECTIONS_PATH = "/agdx/projections"
export const BINDINGS_PATH = "/agdx/bindings"
export const SCHEMAS_PATH = "/agdx/schemas"
export const KV_PATH = "/agdx/kv"
export const FORKS_PATH = "/agdx/forks"
export const GRAPHS_PATH = "/agdx/graphs"
export const CLIENTS_PATH = "/agdx/clients"
export const SESSIONS_PATH = "/agdx/sessions"
/** `?status=` on a session list: one session status. */
export const PARAM_STATUS = "status"
/** `?agent=` on a session list: sessions this agent took part in. */
export const PARAM_AGENT = "agent"
/** `?text=` on a session list: a substring of the label or id. */
export const PARAM_TEXT = "text"
/** `?root=` on a session list: the root of the session tree. */
export const PARAM_ROOT = "root"
/** `?label_prefix=` on a session list: the start of the label. */
export const PARAM_LABEL_PREFIX = "label_prefix"
/** `?surface=` on session links: one link surface. */
export const PARAM_SURFACE = "surface"
/** `?as_of=` on a session list or timeline page: the bound of a walk, the
 * `as_of` its first page returned. */
export const PARAM_AS_OF = "as_of"
/** `?series=` on a session list or timeline page: the change series of the
 * walk's `as_of`. */
export const PARAM_SERIES = "series"
/** `?page=`: the 1-based page of a list. Zero or none reads the first page. */
export const PARAM_PAGE = "page"
/** `?results=`: the items per page of a list. Zero or none leaves it to the
 * server. */
export const PARAM_RESULTS = "results"
/** `?history_limit=` on session state: the most history rows to return. */
export const PARAM_HISTORY_LIMIT = "history_limit"
/** `?stream=` on the schema routes: the stream whose registry is addressed. */
export const PARAM_STREAM = "stream"
/** `?name_contains=` on a list: a substring of the name. */
export const PARAM_NAME_CONTAINS = "name_contains"

/** `GET /agdx/schemas` query. An absent `stream` lists the deployment-wide
 * registry. */
export interface SchemaListQuery {
  readonly nameContains?: string
  readonly stream?: string
}

/** `?stream=` on `GET`/`DELETE /agdx/schemas/{id}` and
 * `POST /agdx/schemas/{id}/decode`: the stream whose registry holds the id.
 * Absent addresses the deployment-wide registry. */
export interface SchemaQuery {
  readonly stream?: string
}

export function schemaListQueryParams(query: SchemaListQuery): URLSearchParams {
  const params = new URLSearchParams()
  setParam(params, PARAM_NAME_CONTAINS, query.nameContains)
  setParam(params, PARAM_STREAM, query.stream)
  return params
}

export function schemaQueryParams(query: SchemaQuery): URLSearchParams {
  const params = new URLSearchParams()
  setParam(params, PARAM_STREAM, query.stream)
  return params
}

/** `GET /agdx/filters` query. The owner filter is the five fields of a group
 * identity, all or none. */
export interface FilterListQuery {
  readonly nameContains?: string
  readonly state?: FilterState
  readonly ownerStreamId?: number
  readonly ownerStreamCreatedAtMicros?: bigint
  readonly ownerTopicId?: number
  readonly ownerTopicCreatedAtMicros?: bigint
  readonly ownerGroupId?: bigint
  readonly page?: number
  readonly results?: number
}

const FILTER_STATES: ReadonlySet<string> = new Set<FilterState>(["active", "archived", "dropped"])

/** The list query that selects the own filter of `identity`, as Rust `From<FilterGroupIdentity>`. */
export function filterListQueryOf(identity: FilterGroupIdentity): FilterListQuery {
  return {
    ownerStreamId: identity.streamId,
    ownerStreamCreatedAtMicros: identity.streamCreatedAtMicros,
    ownerTopicId: identity.topicId,
    ownerTopicCreatedAtMicros: identity.topicCreatedAtMicros,
    ownerGroupId: identity.groupId
  }
}

/** The owner filter of `query`, refused unless all five identity fields are set or none. */
export function filterListQueryOwner(query: FilterListQuery): FilterGroupIdentity | undefined {
  const fields = [
    query.ownerStreamId,
    query.ownerStreamCreatedAtMicros,
    query.ownerTopicId,
    query.ownerTopicCreatedAtMicros,
    query.ownerGroupId
  ]
  if (fields.every((value) => value === undefined)) return undefined
  if (
    query.ownerStreamId === undefined ||
    query.ownerStreamCreatedAtMicros === undefined ||
    query.ownerTopicId === undefined ||
    query.ownerTopicCreatedAtMicros === undefined ||
    query.ownerGroupId === undefined
  ) {
    throw new InvalidError(
      "the owner filter needs owner_stream_id, owner_stream_created_at_micros, owner_topic_id, owner_topic_created_at_micros and owner_group_id together"
    )
  }
  return {
    streamId: query.ownerStreamId,
    streamCreatedAtMicros: query.ownerStreamCreatedAtMicros,
    topicId: query.ownerTopicId,
    topicCreatedAtMicros: query.ownerTopicCreatedAtMicros,
    groupId: query.ownerGroupId
  }
}

export function filterListQueryParams(query: FilterListQuery): URLSearchParams {
  const params = new URLSearchParams()
  setParam(params, PARAM_NAME_CONTAINS, query.nameContains)
  setParam(params, "state", query.state)
  setParam(params, "owner_stream_id", query.ownerStreamId)
  setParam(params, "owner_stream_created_at_micros", query.ownerStreamCreatedAtMicros?.toString())
  setParam(params, "owner_topic_id", query.ownerTopicId)
  setParam(params, "owner_topic_created_at_micros", query.ownerTopicCreatedAtMicros?.toString())
  setParam(params, "owner_group_id", query.ownerGroupId?.toString())
  setParam(params, "page", query.page)
  setParam(params, "results", query.results)
  return params
}

export function decodeFilterListQuery(params: URLSearchParams): FilterListQuery {
  const state = params.get("state")
  if (state !== null && !FILTER_STATES.has(state)) {
    throw new CodecError(
      "query parameter state must be active, archived or dropped",
      "query",
      "state"
    )
  }
  const ownerStreamId = u32Param(params, "owner_stream_id")
  const ownerStreamCreatedAtMicros = u64Param(params, "owner_stream_created_at_micros")
  const ownerTopicId = u32Param(params, "owner_topic_id")
  const ownerTopicCreatedAtMicros = u64Param(params, "owner_topic_created_at_micros")
  const ownerGroupId = u64Param(params, "owner_group_id")
  const page = u32Param(params, "page")
  const results = u32Param(params, "results")
  return {
    ...stringParam(params, PARAM_NAME_CONTAINS, "nameContains"),
    ...(state !== null ? { state: state as FilterState } : {}),
    ...(ownerStreamId !== undefined ? { ownerStreamId } : {}),
    ...(ownerStreamCreatedAtMicros !== undefined ? { ownerStreamCreatedAtMicros } : {}),
    ...(ownerTopicId !== undefined ? { ownerTopicId } : {}),
    ...(ownerTopicCreatedAtMicros !== undefined ? { ownerTopicCreatedAtMicros } : {}),
    ...(ownerGroupId !== undefined ? { ownerGroupId } : {}),
    ...(page !== undefined ? { page } : {}),
    ...(results !== undefined ? { results } : {})
  }
}

/** Filters for `GET /agdx/sessions/{stream}`. The stream is the path segment.
 * A `limit` of zero or none leaves the page size to the server. */
/** Filters and page of `GET /agdx/sessions/{stream}`. A `page` of zero or none
 * reads the first page, and `results` of zero or none leaves the page size to
 * the server. */
export interface SessionsQuery {
  readonly status?: string
  readonly root?: string
  readonly labelPrefix?: string
  readonly agent?: string
  readonly text?: string
  readonly page?: number
  readonly results?: number
  readonly asOf?: bigint
  readonly series?: bigint
}

/** Page of `GET /agdx/sessions/{stream}/{id}/events`. A `page` of zero or none
 * reads the first page, `results` of zero or none leaves the page size to the
 * server, and `asOf` with `series` bounds a walk. */
export interface SessionEventsQuery {
  readonly page?: number
  readonly results?: number
  readonly asOf?: bigint
  readonly series?: bigint
}

/** Query of `GET /agdx/sessions/{stream}/{id}/state`. A `historyLimit` of zero
 * or none leaves the history size to the server. */
export interface SessionStateQuery {
  readonly historyLimit?: number
}

/** Query of `GET /agdx/sessions/{stream}/{id}/links`. */
export interface SessionLinksQuery {
  readonly surface?: string
}

/** Page of `GET /agdx/sessions/{stream}/changes`. A `page` of zero or none
 * reads the first page, and `results` of zero or none leaves the page size to
 * the server. */
export interface SessionChangesQuery {
  readonly page?: number
  readonly results?: number
}

export function sessionsQueryParams(query: SessionsQuery): URLSearchParams {
  const params = new URLSearchParams()
  setParam(params, PARAM_STATUS, query.status)
  setParam(params, PARAM_ROOT, query.root)
  setParam(params, PARAM_LABEL_PREFIX, query.labelPrefix)
  setParam(params, PARAM_AGENT, query.agent)
  setParam(params, PARAM_TEXT, query.text)
  setParam(params, PARAM_PAGE, query.page)
  setParam(params, PARAM_RESULTS, query.results)
  setParam(params, PARAM_AS_OF, query.asOf?.toString())
  setParam(params, PARAM_SERIES, query.series?.toString())
  return params
}

export function decodeSessionsQuery(params: URLSearchParams): SessionsQuery {
  const page = u32Param(params, PARAM_PAGE)
  const results = u32Param(params, PARAM_RESULTS)
  return {
    ...stringParam(params, PARAM_STATUS, "status"),
    ...stringParam(params, PARAM_ROOT, "root"),
    ...stringParam(params, PARAM_LABEL_PREFIX, "labelPrefix"),
    ...stringParam(params, PARAM_AGENT, "agent"),
    ...stringParam(params, PARAM_TEXT, "text"),
    ...(page !== undefined ? { page } : {}),
    ...(results !== undefined ? { results } : {}),
    ...boundParams(params)
  }
}

function boundParams(params: URLSearchParams): { asOf?: bigint; series?: bigint } {
  const asOf = u64Param(params, PARAM_AS_OF)
  const series = u64Param(params, PARAM_SERIES)
  return {
    ...(asOf !== undefined ? { asOf } : {}),
    ...(series !== undefined ? { series } : {})
  }
}

export function sessionEventsQueryParams(query: SessionEventsQuery): URLSearchParams {
  const params = new URLSearchParams()
  setParam(params, PARAM_PAGE, query.page)
  setParam(params, PARAM_RESULTS, query.results)
  setParam(params, PARAM_AS_OF, query.asOf?.toString())
  setParam(params, PARAM_SERIES, query.series?.toString())
  return params
}

export function decodeSessionEventsQuery(params: URLSearchParams): SessionEventsQuery {
  const page = u32Param(params, PARAM_PAGE)
  const results = u32Param(params, PARAM_RESULTS)
  return {
    ...(page !== undefined ? { page } : {}),
    ...(results !== undefined ? { results } : {}),
    ...boundParams(params)
  }
}

export function sessionStateQueryParams(query: SessionStateQuery): URLSearchParams {
  const params = new URLSearchParams()
  setParam(params, PARAM_HISTORY_LIMIT, query.historyLimit)
  return params
}

export function decodeSessionStateQuery(params: URLSearchParams): SessionStateQuery {
  const historyLimit = u32Param(params, PARAM_HISTORY_LIMIT)
  return historyLimit !== undefined ? { historyLimit } : {}
}

export function sessionLinksQueryParams(query: SessionLinksQuery): URLSearchParams {
  const params = new URLSearchParams()
  setParam(params, PARAM_SURFACE, query.surface)
  return params
}

export function decodeSessionLinksQuery(params: URLSearchParams): SessionLinksQuery {
  return stringParam(params, PARAM_SURFACE, "surface")
}

export function sessionChangesQueryParams(query: SessionChangesQuery): URLSearchParams {
  const params = new URLSearchParams()
  setParam(params, PARAM_PAGE, query.page)
  setParam(params, PARAM_RESULTS, query.results)
  return params
}

export function decodeSessionChangesQuery(params: URLSearchParams): SessionChangesQuery {
  const page = u32Param(params, PARAM_PAGE)
  const results = u32Param(params, PARAM_RESULTS)
  return {
    ...(page !== undefined ? { page } : {}),
    ...(results !== undefined ? { results } : {})
  }
}

export function sessionErrorBody(error: SessionError): ErrorBody {
  return { code: sessionErrorResultCode(error), message: sessionErrorMessage(error) }
}

function setParam(params: URLSearchParams, key: string, value: string | number | undefined): void {
  if (value !== undefined) params.set(key, String(value))
}

function stringParam<Key extends string>(
  params: URLSearchParams,
  key: string,
  name: Key
): Readonly<Partial<Record<Key, string>>> {
  const value = params.get(key)
  return (value === null ? {} : { [name]: value }) as Readonly<Partial<Record<Key, string>>>
}

function u32Param(params: URLSearchParams, key: string): number | undefined {
  const raw = params.get(key)
  if (raw === null) return undefined
  if (!/^\d+$/.test(raw) || Number(raw) > 0xffff_ffff) {
    throw new CodecError(`query parameter ${key} must be an unsigned 32-bit integer`, "query", key)
  }
  return Number(raw)
}

function u64Param(params: URLSearchParams, key: string): bigint | undefined {
  const raw = params.get(key)
  if (raw === null) return undefined
  if (!/^\d+$/.test(raw) || BigInt(raw) > 0xffff_ffff_ffff_ffffn) {
    throw new CodecError(`query parameter ${key} must be an unsigned 64-bit integer`, "query", key)
  }
  return BigInt(raw)
}

export const AUTHZ_WHOAMI_PATH = "/agdx/authz/whoami"
export const AUTHZ_ROLES_PATH = "/agdx/authz/roles"
export const FILTERS_PATH = "/agdx/filters"
export const FILTER_MUTATIONS_PATH = "/agdx/filters/mutations"
export const FILTER_VALIDATE_PATH = "/agdx/filters/validate"
export const FILTER_TEST_PATH = "/agdx/filters/test"
export const FILTER_PREVIEW_PATH = "/agdx/filters/preview"
export const FILTER_BINDINGS_PATH = "/agdx/filter-bindings"

export function filterPath(id: number): string {
  return `${FILTERS_PATH}/${String(id)}`
}

export function filterRevisionsPath(id: number): string {
  return `${FILTERS_PATH}/${String(id)}/revisions`
}

export function filterOperationPath(operationId: bigint): string {
  return `${FILTERS_PATH}/operations/${operationId.toString()}`
}

export function filterBindingPath(stream: string, topic: string, group: string): string {
  return `${FILTER_BINDINGS_PATH}/${encodeURIComponent(stream)}/${encodeURIComponent(topic)}/${encodeURIComponent(group)}`
}

export const authzRolePath = (name: string): string => `${AUTHZ_ROLES_PATH}/${pathSegment(name)}`
export const authzUserRolesPath = (userId: number): string =>
  `/agdx/authz/users/${String(userId)}/roles`
export const graphPath = (id: string): string => `${GRAPHS_PATH}/${pathSegment(id)}`
export const graphQueryPath = (name: string): string => `/agdx/graph/${pathSegment(name)}/query`
export const graphNeighborsPath = (name: string, node: string): string =>
  `/agdx/graph/${pathSegment(name)}/neighbors/${pathSegment(node)}`
export const projectionPath = (id: string): string => `${PROJECTIONS_PATH}/${pathSegment(id)}`
export const schemaPath = (id: number): string => `${SCHEMAS_PATH}/${String(id)}`
export const schemaDecodePath = (id: number): string => `${schemaPath(id)}/decode`
export const kvNamespacePath = (namespace: string): string => `${KV_PATH}/${pathSegment(namespace)}`
export const kvEntryPath = (namespace: string, key: string): string =>
  `${kvNamespacePath(namespace)}/${pathSegment(key)}`
export const kvCasPath = (namespace: string, key: string): string =>
  `${kvEntryPath(namespace, key)}/cas`
export const forkPath = (id: string): string => `${FORKS_PATH}/${pathSegment(id)}`
export const forkPromotePath = (id: string): string => `${forkPath(id)}/promote`
export const forkRowsPath = (id: string): string => `${forkPath(id)}/rows`
export function pathSegment(value: string): string {
  return encodeURIComponent(value).replace(
    /[!'()*]/g,
    (character) => `%${character.charCodeAt(0).toString(16).toUpperCase().padStart(2, "0")}`
  )
}

export const sessionsPath = (stream: string): string => `${SESSIONS_PATH}/${pathSegment(stream)}`
export const sessionChangesPath = (stream: string): string => `${sessionsPath(stream)}/changes`
export const sessionPath = (stream: string, id: ConversationId): string =>
  `${sessionsPath(stream)}/${pathSegment(id.toString())}`
export const sessionEventsPath = (stream: string, id: ConversationId): string =>
  `${sessionPath(stream, id)}/events`
export const sessionStatePath = (stream: string, id: ConversationId): string =>
  `${sessionPath(stream, id)}/state`
export const sessionLinksPath = (stream: string, id: ConversationId): string =>
  `${sessionPath(stream, id)}/links`
export const sessionSourcesPath = (stream: string, id: ConversationId): string =>
  `${sessionPath(stream, id)}/sources`
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
  readonly fencedLeases: boolean
}

export interface FilterCapsView {
  readonly native: boolean
  readonly groupPolicyReads: boolean
  readonly preview: boolean
  readonly primaryRouting: boolean
  readonly catalog: boolean
  readonly managedGroups: boolean
  readonly evaluatorVersion: number
  readonly codecs: readonly FilterCodec[]
}

export interface HttpCapabilities {
  readonly managed: boolean
  readonly query: QueryCapsView
  readonly kv: KvCapsView
  readonly graph: boolean
  readonly fork: boolean
  readonly sessions: boolean
  readonly watch: boolean
  readonly authz: boolean
  readonly filters: FilterCapsView
  readonly versions: OpVersions
  readonly backends: readonly BackendDescriptor[]
  readonly topology?: WireTopology
}

export interface KvEntryView {
  readonly key: string
  readonly value: string
  readonly expiresAtMicros?: bigint
  /** The entry's version, the value a compare-and-swap expects. */
  readonly version: bigint
  readonly scope?: MemoryRowScope
  readonly source?: SourceRef
}

/** One KV scan page on the HTTP surface, in key order. `page` counts from 1. */
export interface KvPageView {
  readonly items: readonly KvEntryView[]
  readonly page: number
  readonly totalPages: number
  readonly totalResults: number
}

export interface ErrorBody {
  readonly code: ResultCode
  readonly message: string
  readonly detail?: unknown
}

function parseJson(text: string, context: string): unknown {
  try {
    return fromJsonValue(JSON.parse(text) as unknown, context)
  } catch (cause) {
    if (cause instanceof CodecError) throw cause
    throw new CodecError(`failed to decode ${context}`, context, "decode", { cause })
  }
}

const READABLE_ID_FIELDS = new Set([
  "backend_resource_id",
  "cluster",
  "destination_id",
  "execution_id",
  "operation_id",
  "owner",
  "request_id",
  "resource_id"
])

function isReadableIdContext(context: string): boolean {
  const fieldName = /\.([^.[\]]+)$/.exec(context)?.[1]
  if (fieldName !== undefined && READABLE_ID_FIELDS.has(fieldName)) return true
  return context.endsWith(".destination.id") || /\.routes\[\d+\]\.id$/.test(context)
}

function readableIdFromJson(value: string, context: string): Uint8Array {
  try {
    return bigIntToBytes16(crockfordDecode(value))
  } catch (cause) {
    throw new CodecError(`${context} must be a Crockford base32 id`, context, "string", { cause })
  }
}

function fromJsonValue(value: unknown, context: string): unknown {
  if (typeof value === "string" && isReadableIdContext(context)) {
    return readableIdFromJson(value, context)
  }
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
    if (isReadableIdContext(context)) {
      throw new CodecError(`${context} must be a Crockford base32 id`, context, "string")
    }
    if (
      value.length > 0 &&
      value.every((item) => Number.isInteger(item) && Number(item) >= 0 && Number(item) <= 255)
    ) {
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
  if (value instanceof Uint8Array) {
    if (isReadableIdContext(context)) {
      if (value.length !== 16) {
        throw new CodecError(`${context} must contain 16 bytes`, context, "bytes")
      }
      return crockfordEncode(bytes16ToBigInt(value))
    }
    return [...value]
  }
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

export const decodeProjectionPageJson = (text: string): ProjectionPage =>
  decodeProjectionPage(
    expectMap(parseJson(text, "ProjectionPage"), "ProjectionPage"),
    "ProjectionPage"
  )
export const encodeProjectionPageJson = (page: ProjectionPage): string =>
  encodeJson(encodeProjectionPage(page), "ProjectionPage")
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
        ) ?? false,
      fencedLeases:
        field.optionalBoolean(
          field.requiredMap(map, "kv", context),
          "fenced_leases",
          `${context}.kv`
        ) ?? false
    },
    graph: field.optionalBoolean(map, "graph", context) ?? false,
    fork: field.requiredBoolean(map, "fork", context),
    sessions: field.optionalBoolean(map, "sessions", context) ?? false,
    watch: field.optionalBoolean(map, "watch", context) ?? false,
    authz: field.optionalBoolean(map, "authz", context) ?? false,
    filters: decodeFilterCaps(field.optionalMap(map, "filters", context), `${context}.filters`),
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
        ["cas_fenced", value.kv.casFenced],
        ["fenced_leases", value.kv.fencedLeases]
      ])
    ],
    ["graph", value.graph],
    ["fork", value.fork],
    ["sessions", value.sessions],
    ["watch", value.watch],
    ["authz", value.authz],
    ["filters", encodeFilterCaps(value.filters)],
    ["versions", encodeOpVersions(value.versions)]
  ])
  if (value.backends.length > 0) map.set("backends", value.backends.map(encodeBackendDescriptor))
  if (value.topology !== undefined) map.set("topology", encodeWireTopology(value.topology))
  return encodeJson(map, "Capabilities")
}

function decodeFilterCaps(map: CborMap | undefined, context: string): FilterCapsView {
  const flag = (key: string): boolean =>
    map === undefined ? false : (field.optionalBoolean(map, key, context) ?? false)
  return {
    native: flag("native"),
    groupPolicyReads: flag("group_policy_reads"),
    preview: flag("preview"),
    primaryRouting: flag("primary_routing"),
    catalog: flag("catalog"),
    managedGroups: flag("managed_groups"),
    evaluatorVersion:
      map === undefined ? 0 : (field.optionalU32(map, "evaluator_version", context) ?? 0),
    codecs:
      map === undefined
        ? []
        : field.optionalArray(map, "codecs", context, (item, index) =>
            decodeFilterCodec(item, `${context}.codecs[${String(index)}]`)
          )
  }
}

function encodeFilterCaps(value: FilterCapsView): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["native", value.native],
    ["group_policy_reads", value.groupPolicyReads],
    ["preview", value.preview],
    ["primary_routing", value.primaryRouting],
    ["catalog", value.catalog],
    ["managed_groups", value.managedGroups],
    ["evaluator_version", value.evaluatorVersion]
  ])
  if (value.codecs.length > 0) map.set("codecs", [...value.codecs])
  return map
}

function decodeKvEntry(map: CborMap, context: string): KvEntryView {
  const expiresAtMicros = field.optionalU64(map, "expires_at_micros", context)
  const scope = field.optionalMap(map, "scope", context)
  const source = field.optionalMap(map, "source", context)
  return {
    key: field.requiredString(map, "key", context),
    value: field.requiredString(map, "value", context),
    ...(expiresAtMicros !== undefined ? { expiresAtMicros } : {}),
    version: field.requiredU64(map, "version", context),
    ...(scope !== undefined ? { scope: decodeMemoryRowScope(scope, `${context}.scope`) } : {}),
    ...(source !== undefined ? { source: decodeSourceRef(source, `${context}.source`) } : {})
  }
}

function encodeKvEntry(value: KvEntryView): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["key", value.key],
    ["value", value.value],
    ["expires_at_micros", value.expiresAtMicros ?? null],
    ["version", value.version]
  ])
  if (value.scope !== undefined) map.set("scope", encodeMemoryRowScope(value.scope))
  if (value.source !== undefined) map.set("source", encodeSourceRef(value.source))
  return map
}

export function decodeKvPageJson(text: string): KvPageView {
  const context = "KvPageView"
  const map = expectMap(parseJson(text, context), context)
  return {
    items: field.requiredArray(map, "items", context, (item, index) =>
      decodeKvEntry(
        expectMap(item, `${context}.items[${String(index)}]`),
        `${context}.items[${String(index)}]`
      )
    ),
    page: field.requiredU32(map, "page", context),
    totalPages: field.requiredU32(map, "total_pages", context),
    totalResults: field.requiredU32(map, "total_results", context)
  }
}

export function encodeKvPageJson(value: KvPageView): string {
  return encodeJson(
    new Map<string, unknown>([
      ["items", value.items.map(encodeKvEntry)],
      ["page", value.page],
      ["total_pages", value.totalPages],
      ["total_results", value.totalResults]
    ]),
    "KvPageView"
  )
}

export function decodeErrorBodyJson(text: string): ErrorBody {
  const context = "ErrorBody"
  const map = expectMap(parseJson(text, context), context)
  const word = field.requiredString(map, "code", context)
  const code = resultCodeFromWord(word)
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
    ["code", resultCodeWord(value.code)],
    ["message", value.message]
  ])
  if (value.detail !== undefined) map.set("detail", value.detail)
  return encodeJson(map, "ErrorBody")
}

export const FILTER_METRICS_PATH = "/agdx/filters/metrics"

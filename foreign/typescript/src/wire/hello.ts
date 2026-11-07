import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, decodeOne, encodeNamed, expectMap, expectString, field } from "./cbor.js"
import { BackendResourceId } from "./ids.js"
import type { AggFunc, Consistency, SqlDialect } from "./query.js"
import type { LogicalTypeKind } from "./schema.js"
import { FILTER_EVALUATOR_VERSION, type FilterCodec, decodeFilterCodec } from "./filter.js"
import { type WireTopology, decodeWireTopology, encodeWireTopology } from "./topology.js"

export const feature = {
  KV_CAS: 1n << 0n,
  READ_YOUR_WRITES: 1n << 1n,
  // Retired with linearizable query reads. No server sets it and the bit is never reused.
  STRONG_CONSISTENCY: 1n << 2n,
  KV_CAS_FENCED: 1n << 3n,
  // Retired with the run registry. No server sets it and the bit is never reused.
  AGENT_WORKFLOW: 1n << 4n,
  KEYWORD_SEARCH: 1n << 5n,
  WATCH: 1n << 6n,
  AUTHZ: 1n << 7n,
  // Retired with lakehouse destinations. No server sets it and the bit is never reused.
  DESTINATIONS: 1n << 8n,
  KV_FENCED_LEASES: 1n << 9n,
  CONSUMER_FILTERS: 1n << 10n,
  GROUP_POLICY_READS: 1n << 11n,
  SESSIONS: 1n << 12n,
  MATERIALIZATIONS: 1n << 14n,
  // The managed backend runs in stream tenancy: every managed name is scoped
  // to one stream, and the change feed and dead letters of each stream ride
  // their own ops topics.
  STREAM_TENANCY: 1n << 13n
} as const

export interface OpVersions {
  readonly query: number
  readonly control: number
  readonly kv: number
  readonly fork: number
  readonly agent: number
  readonly graph: number
  readonly filter?: number
  readonly features: bigint
}
export function newOpVersions(
  query: number,
  control: number,
  kv: number,
  fork: number
): OpVersions {
  return { query, control, kv, fork, agent: 0, graph: 0, filter: 0, features: 0n }
}
export function opVersionsHasFeature(versions: OpVersions, bit: bigint): boolean {
  return (versions.features & bit) === bit
}
export function encodeOpVersions(value: OpVersions): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["query", value.query],
    ["control", value.control],
    ["kv", value.kv],
    ["fork", value.fork]
  ])
  if (value.agent !== 0) map.set("agent", value.agent)
  if (value.graph !== 0) map.set("graph", value.graph)
  if ((value.filter ?? 0) !== 0) map.set("filter", value.filter)
  if (value.features !== 0n) map.set("features", value.features)
  return map
}
export function decodeOpVersions(map: CborMap, context: string): OpVersions {
  return {
    query: field.requiredU32(map, "query", context),
    control: field.requiredU32(map, "control", context),
    kv: field.requiredU32(map, "kv", context),
    fork: field.requiredU32(map, "fork", context),
    agent: field.optionalU32(map, "agent", context) ?? 0,
    graph: field.optionalU32(map, "graph", context) ?? 0,
    filter: field.optionalU32(map, "filter", context) ?? 0,
    features: field.optionalU64(map, "features", context) ?? 0n
  }
}

export interface HelloReply {
  readonly versions: OpVersions
}
export const BACKEND_DESCRIPTOR_VERSION = 1
export type BackendMode = "operational"
export type BackendDesiredState = "disabled" | "enabled"
export type BackendObservedState = "disabled" | "starting" | "ready" | "degraded" | "unavailable"
export type BackendReadinessCode =
  | "disabled"
  | "configuration_pending"
  | "configuration_rejected"
  | "credential_unavailable"
  | "object_store_unavailable"
  | "catalog_unavailable"
  | "query_runtime_unavailable"
  | "generation_mismatch"
  | "probe_failed"
  | "query_identity_rejected"
export interface BackendImplementation {
  readonly kind: string
  readonly version: string
}
export interface BackendReadinessReason {
  readonly code: BackendReadinessCode
  readonly detail?: string
}
export interface BackendReadiness {
  readonly ready: boolean
  readonly reasons: readonly BackendReadinessReason[]
  readonly observedAtMicros: bigint
}
export interface QueryCapabilities {
  readonly dialects: readonly SqlDialect[]
  readonly consistency: readonly Consistency[]
  readonly logicalTypes: readonly LogicalTypeKind[]
  readonly rawSql: boolean
  readonly aggregates: readonly AggFunc[]
  readonly vectorSearch: boolean
  readonly textSearch: boolean
}
export interface SchemaCapabilities {
  readonly logicalSchema: boolean
  readonly arrowIpcStream: boolean
  readonly schemaEvolution: boolean
}
export interface BackendLimits {
  readonly maxQueryRows: bigint
  readonly maxQueryBytes: bigint
  readonly maxScanBytes: bigint
  readonly maxQueryMicros: bigint
  readonly maxConcurrentQueries: number
  readonly maxSchemaFields: number
  readonly maxPayloadBytes: bigint
}
export interface BackendDescriptor {
  readonly descriptorVersion: number
  readonly resourceId: BackendResourceId
  readonly mode: BackendMode
  readonly label: string
  readonly implementation: BackendImplementation
  readonly observedBackendGeneration: bigint
  readonly runtimeConfigurationRevision: bigint
  readonly desiredState: BackendDesiredState
  readonly observedState: BackendObservedState
  readonly readiness: BackendReadiness
  readonly query?: QueryCapabilities
  readonly schema: SchemaCapabilities
  readonly limits: BackendLimits
}
export interface BackendAnnounce {
  readonly versions: OpVersions
  readonly ready?: boolean
  readonly backends: readonly BackendDescriptor[]
  readonly topology?: WireTopology
  /**
   * The consumer-filter evaluation the streaming server serves, set with the
   * `CONSUMER_FILTERS` bit. Absent from an older server.
   */
  readonly filters?: FilterAnnounce
}

/**
 * The evaluator version and codecs a server serves. A client refuses a filter
 * the server would evaluate differently, before the first page.
 */
export interface FilterAnnounce {
  readonly evaluatorVersion: number
  readonly codecs: readonly FilterCodec[]
  /**
   * Longest filtered wait the server holds, in milliseconds. Zero means it
   * serves no filtered wait, so a reader paces idle rounds itself.
   */
  readonly maxWaitMs: number
}

/** What this build of the evaluator serves. */
export function filterAnnounceServed(): FilterAnnounce {
  return {
    evaluatorVersion: FILTER_EVALUATOR_VERSION,
    codecs: ["json", "headers_only", "avro", "protobuf", "cbor"],
    maxWaitMs: 0
  }
}

/** Whether a filter of `evaluatorVersion` and `codec` evaluates on the announcing server as it does here. */
export function filterAnnounceEvaluates(
  announce: FilterAnnounce,
  evaluatorVersion: number,
  codec: FilterCodec
): boolean {
  return announce.evaluatorVersion === evaluatorVersion && announce.codecs.includes(codec)
}

export function newBackendDescriptor(
  resourceId: BackendResourceId,
  mode: BackendMode,
  label: string,
  implementation: BackendImplementation,
  observedBackendGeneration: bigint,
  runtimeConfigurationRevision: bigint
): BackendDescriptor {
  return {
    descriptorVersion: BACKEND_DESCRIPTOR_VERSION,
    resourceId,
    mode,
    label,
    implementation,
    observedBackendGeneration,
    runtimeConfigurationRevision,
    desiredState: "disabled",
    observedState: "disabled",
    readiness: backendReadinessNotReady("disabled"),
    schema: { logicalSchema: false, arrowIpcStream: false, schemaEvolution: false },
    limits: {
      maxQueryRows: 0n,
      maxQueryBytes: 0n,
      maxScanBytes: 0n,
      maxQueryMicros: 0n,
      maxConcurrentQueries: 0,
      maxSchemaFields: 0,
      maxPayloadBytes: 0n
    }
  }
}
export function backendReadinessNotReady(code: BackendReadinessCode): BackendReadiness {
  return { ready: false, reasons: [{ code }], observedAtMicros: 0n }
}

export function backendDescriptorHasCapability(backend: BackendDescriptor, tag: string): boolean {
  return backend.query?.dialects.includes(tag as SqlDialect) === true
}
export function newBackendAnnounce(versions: OpVersions): BackendAnnounce {
  return { versions, ready: true, backends: [] }
}

function optional<T>(
  map: Map<string, unknown>,
  name: string,
  value: T | undefined,
  encode: (value: T) => unknown = (item) => item
): void {
  if (value !== undefined) map.set(name, encode(value))
}
function words<T extends string>(
  map: CborMap,
  name: string,
  context: string,
  allowed: readonly T[]
): T[] {
  return field.requiredArray(map, name, context, (item, index) => {
    const value = expectString(item, `${context}.${name}[${String(index)}]`)
    if (!allowed.includes(value as T))
      throw new CodecError(`unknown ${name} value \`${value}\``, context, name)
    return value as T
  })
}
function word<T extends string>(
  map: CborMap,
  name: string,
  context: string,
  allowed: readonly T[]
): T {
  const value = field.requiredString(map, name, context)
  if (!allowed.includes(value as T))
    throw new CodecError(`unknown ${name} value \`${value}\``, context, name)
  return value as T
}

export function encodeBackendDescriptor(value: BackendDescriptor): Map<string, unknown> {
  const readiness = new Map<string, unknown>([
    ["ready", value.readiness.ready],
    [
      "reasons",
      value.readiness.reasons.map((reason) => {
        const map = new Map<string, unknown>([["code", reason.code]])
        optional(map, "detail", reason.detail)
        return map
      })
    ],
    ["observed_at_micros", value.readiness.observedAtMicros]
  ])
  const map = new Map<string, unknown>([
    ["descriptor_version", value.descriptorVersion],
    ["resource_id", value.resourceId.toBytes()],
    ["mode", value.mode],
    ["label", value.label],
    [
      "implementation",
      new Map<string, unknown>([
        ["kind", value.implementation.kind],
        ["version", value.implementation.version]
      ])
    ],
    ["observed_backend_generation", value.observedBackendGeneration],
    ["runtime_configuration_revision", value.runtimeConfigurationRevision],
    ["desired_state", value.desiredState],
    ["observed_state", value.observedState],
    ["readiness", readiness]
  ])
  optional(
    map,
    "query",
    value.query,
    (item) =>
      new Map<string, unknown>([
        ["dialects", [...item.dialects]],
        ["consistency", [...item.consistency]],
        ["logical_types", [...item.logicalTypes]],
        ["raw_sql", item.rawSql],
        ...(item.aggregates.length > 0
          ? [["aggregates", [...item.aggregates]] as [string, unknown]]
          : []),
        ["vector_search", item.vectorSearch],
        ["text_search", item.textSearch]
      ])
  )
  map.set(
    "schema",
    new Map([
      ["logical_schema", value.schema.logicalSchema],
      ["arrow_ipc_stream", value.schema.arrowIpcStream],
      ["schema_evolution", value.schema.schemaEvolution]
    ])
  )
  map.set(
    "limits",
    new Map<string, unknown>([
      ["max_query_rows", value.limits.maxQueryRows],
      ["max_query_bytes", value.limits.maxQueryBytes],
      ["max_scan_bytes", value.limits.maxScanBytes],
      ["max_query_micros", value.limits.maxQueryMicros],
      ["max_concurrent_queries", value.limits.maxConcurrentQueries],
      ["max_schema_fields", value.limits.maxSchemaFields],
      ["max_payload_bytes", value.limits.maxPayloadBytes]
    ])
  )
  return map
}

export function decodeBackendDescriptor(value: unknown, context: string): BackendDescriptor {
  const map = expectMap(value, context)
  const implementation = field.requiredMap(map, "implementation", context)
  const readiness = field.requiredMap(map, "readiness", context)
  const query = field.optionalMap(map, "query", context)
  const schema = field.requiredMap(map, "schema", context)
  const limits = field.requiredMap(map, "limits", context)
  return {
    descriptorVersion: field.requiredU32(map, "descriptor_version", context),
    resourceId: BackendResourceId.fromBytes(field.requiredBytes(map, "resource_id", context)),
    mode: word(map, "mode", context, ["operational"]),
    label: field.requiredString(map, "label", context),
    implementation: {
      kind: field.requiredString(implementation, "kind", context),
      version: field.requiredString(implementation, "version", context)
    },
    observedBackendGeneration: field.requiredU64(map, "observed_backend_generation", context),
    runtimeConfigurationRevision: field.requiredU64(map, "runtime_configuration_revision", context),
    desiredState: word(map, "desired_state", context, ["disabled", "enabled"]),
    observedState: word(map, "observed_state", context, [
      "disabled",
      "starting",
      "ready",
      "degraded",
      "unavailable"
    ]),
    readiness: {
      ready: field.requiredBoolean(readiness, "ready", context),
      reasons: field.requiredArray(readiness, "reasons", context, (item, index) => {
        const reason = expectMap(item, `${context}.reasons[${String(index)}]`)
        const detail = field.optionalString(reason, "detail", context)
        return {
          code: word(reason, "code", context, [
            "disabled",
            "configuration_pending",
            "configuration_rejected",
            "credential_unavailable",
            "object_store_unavailable",
            "catalog_unavailable",
            "query_runtime_unavailable",
            "generation_mismatch",
            "probe_failed",
            "query_identity_rejected"
          ]),
          ...(detail === undefined ? {} : { detail })
        }
      }),
      observedAtMicros: field.requiredU64(readiness, "observed_at_micros", context)
    },
    ...(query === undefined
      ? {}
      : {
          query: {
            dialects: words(query, "dialects", context, ["postgres", "my_sql", "sqlite"]),
            consistency: words(query, "consistency", context, ["eventual", "read_your_writes"]),
            logicalTypes: words(query, "logical_types", context, [
              "boolean",
              "int",
              "long",
              "float",
              "double",
              "decimal",
              "date",
              "time_micros",
              "timestamp_micros",
              "timestamp_tz_micros",
              "string",
              "uuid",
              "fixed",
              "binary",
              "struct",
              "list",
              "map",
              "variant"
            ]),
            rawSql: field.requiredBoolean(query, "raw_sql", context),
            aggregates: query.has("aggregates")
              ? words(query, "aggregates", context, [
                  "count",
                  "count_distinct",
                  "sum",
                  "avg",
                  "min",
                  "max",
                  "percentile",
                  "std_dev"
                ])
              : [],
            vectorSearch: field.optionalBoolean(query, "vector_search", context) ?? false,
            textSearch: field.optionalBoolean(query, "text_search", context) ?? false
          }
        }),
    schema: {
      logicalSchema: field.requiredBoolean(schema, "logical_schema", context),
      arrowIpcStream: field.requiredBoolean(schema, "arrow_ipc_stream", context),
      schemaEvolution: field.requiredBoolean(schema, "schema_evolution", context)
    },
    limits: {
      maxPayloadBytes: field.optionalU64(limits, "max_payload_bytes", context) ?? 0n,
      maxQueryRows: field.requiredU64(limits, "max_query_rows", context),
      maxQueryBytes: field.requiredU64(limits, "max_query_bytes", context),
      maxScanBytes: field.requiredU64(limits, "max_scan_bytes", context),
      maxQueryMicros: field.requiredU64(limits, "max_query_micros", context),
      maxConcurrentQueries: field.requiredU32(limits, "max_concurrent_queries", context),
      maxSchemaFields: field.requiredU32(limits, "max_schema_fields", context)
    }
  }
}

export function validateBackendDescriptor(value: BackendDescriptor): void {
  if (
    value.descriptorVersion !== BACKEND_DESCRIPTOR_VERSION ||
    value.resourceId.asU128() === 0n ||
    value.observedBackendGeneration === 0n ||
    value.runtimeConfigurationRevision === 0n
  )
    throw new InvalidError("backend descriptor identity or version is invalid")
  if (
    value.label.length === 0 ||
    value.implementation.kind.length === 0 ||
    value.implementation.version.length === 0
  )
    throw new InvalidError("backend descriptor text is empty")
  if (
    value.readiness.ready &&
    (value.observedState !== "ready" ||
      value.readiness.observedAtMicros === 0n ||
      value.readiness.reasons.length !== 0)
  )
    throw new InvalidError("ready backend observation is inconsistent")
  if (!value.readiness.ready && value.readiness.reasons.length === 0)
    throw new InvalidError("not-ready backend requires a reason")
}
export function encodeHelloReply(reply: HelloReply): Uint8Array {
  return encodeNamed(new Map([["versions", encodeOpVersions(reply.versions)]]))
}
export function decodeHelloReply(bytes: Uint8Array): HelloReply {
  const map = expectMap(decodeOne(bytes, "HelloReply"), "HelloReply")
  return {
    versions: decodeOpVersions(
      field.requiredMap(map, "versions", "HelloReply"),
      "HelloReply.versions"
    )
  }
}
export function encodeBackendAnnounce(value: BackendAnnounce): Uint8Array {
  const map = new Map<string, unknown>([["versions", encodeOpVersions(value.versions)]])
  if (value.ready === false) map.set("ready", false)
  if (value.backends.length > 0) map.set("backends", value.backends.map(encodeBackendDescriptor))
  optional(map, "topology", value.topology, encodeWireTopology)
  if (value.filters !== undefined) {
    const filters = new Map<string, unknown>([
      ["evaluator_version", value.filters.evaluatorVersion]
    ])
    if (value.filters.codecs.length > 0) filters.set("codecs", [...value.filters.codecs])
    if (value.filters.maxWaitMs !== 0) filters.set("max_wait_ms", value.filters.maxWaitMs)
    map.set("filters", filters)
  }
  return encodeNamed(map)
}
export function decodeBackendAnnounce(bytes: Uint8Array): BackendAnnounce {
  const context = "BackendAnnounce"
  const map = expectMap(decodeOne(bytes, context), context)
  const topology = field.optionalMap(map, "topology", context)
  const filters = field.optionalMap(map, "filters", context)
  return {
    versions: decodeOpVersions(field.requiredMap(map, "versions", context), `${context}.versions`),
    ready: field.optionalBoolean(map, "ready", context) ?? true,
    backends: field.optionalArray(map, "backends", context, (item, index) =>
      decodeBackendDescriptor(item, `${context}.backends[${String(index)}]`)
    ),
    ...(topology === undefined
      ? {}
      : { topology: decodeWireTopology(topology, `${context}.topology`) }),
    ...(filters === undefined
      ? {}
      : {
          filters: {
            evaluatorVersion: field.requiredU32(filters, "evaluator_version", `${context}.filters`),
            codecs: field.optionalArray(filters, "codecs", `${context}.filters`, (item, index) =>
              decodeFilterCodec(item, `${context}.filters.codecs[${String(index)}]`)
            ),
            maxWaitMs: field.optionalU32(filters, "max_wait_ms", `${context}.filters`) ?? 0
          }
        })
  }
}

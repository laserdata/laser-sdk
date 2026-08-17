import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, decodeOne, encodeNamed, expectMap, expectString, field } from "./cbor.js"
import { BackendResourceId } from "./ids.js"
import type { Consistency, SqlDialect } from "./query.js"
import type { LogicalTypeKind } from "./schema.js"
import { type WireTopology, decodeWireTopology, encodeWireTopology } from "./topology.js"

export const Feature = {
  KV_CAS: 1n << 0n,
  READ_YOUR_WRITES: 1n << 1n,
  STRONG_CONSISTENCY: 1n << 2n,
  KV_CAS_FENCED: 1n << 3n,
  AGENT_WORKFLOW: 1n << 4n,
  KEYWORD_SEARCH: 1n << 5n,
  WATCH: 1n << 6n,
  AUTHZ: 1n << 7n,
  DESTINATIONS: 1n << 8n,
  KV_FENCED_LEASES: 1n << 9n
} as const

export interface OpVersions {
  readonly query: number
  readonly control: number
  readonly kv: number
  readonly fork: number
  readonly agent: number
  readonly graph: number
  readonly checkpoint?: number
  readonly features: bigint
}
export function newOpVersions(
  query: number,
  control: number,
  kv: number,
  fork: number
): OpVersions {
  return { query, control, kv, fork, agent: 0, graph: 0, checkpoint: 0, features: 0n }
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
  if ((value.checkpoint ?? 0) !== 0) map.set("checkpoint", value.checkpoint)
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
    checkpoint: field.optionalU32(map, "checkpoint", context) ?? 0,
    features: field.optionalU64(map, "features", context) ?? 0n
  }
}

export interface HelloReply {
  readonly versions: OpVersions
}
export const BACKEND_DESCRIPTOR_VERSION = 1
export type BackendMode = "operational" | "lakehouse"
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
export interface MaterializationCapability {
  readonly fileFormat: "parquet"
  readonly tableFormat: "iceberg_v2"
  readonly createTable: boolean
  readonly append: boolean
}
export type TimeTravelCapability = "snapshot_id" | "timestamp_micros"
export type QueryPagingCapability = "offset" | "cursor"
export interface QueryCapabilities {
  readonly dialects: readonly SqlDialect[]
  readonly timeTravel: readonly TimeTravelCapability[]
  readonly consistency: readonly Consistency[]
  readonly logicalTypes: readonly LogicalTypeKind[]
  readonly paging: readonly QueryPagingCapability[]
  readonly cancellation: boolean
  readonly executionStatus: boolean
  readonly rawSql: boolean
}
export interface SchemaCapabilities {
  readonly logicalSchema: boolean
  readonly arrowIpcStream: boolean
  readonly schemaEvolution: boolean
}
export interface MaintenanceCapabilities {
  readonly expireSnapshots: boolean
  readonly removeOrphanFiles: boolean
  readonly compactDataFiles: boolean
}
export interface BackendLimits {
  readonly maxQueryRows: bigint
  readonly maxQueryBytes: bigint
  readonly maxScanBytes: bigint
  readonly maxQueryMicros: bigint
  readonly maxConcurrentQueries: number
  readonly maxSchemaFields: number
  readonly maxMaterializationFileBytes: bigint
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
  readonly materialization: readonly MaterializationCapability[]
  readonly query?: QueryCapabilities
  readonly schema: SchemaCapabilities
  readonly maintenance: MaintenanceCapabilities
  readonly limits: BackendLimits
}
export interface BackendAnnounce {
  readonly versions: OpVersions
  readonly ready?: boolean
  readonly backends: readonly BackendDescriptor[]
  readonly topology?: WireTopology
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
    readiness: { ready: false, reasons: [{ code: "disabled" }], observedAtMicros: 0n },
    materialization: [],
    schema: { logicalSchema: false, arrowIpcStream: false, schemaEvolution: false },
    maintenance: { expireSnapshots: false, removeOrphanFiles: false, compactDataFiles: false },
    limits: {
      maxQueryRows: 0n,
      maxQueryBytes: 0n,
      maxScanBytes: 0n,
      maxQueryMicros: 0n,
      maxConcurrentQueries: 0,
      maxSchemaFields: 0,
      maxMaterializationFileBytes: 0n
    }
  }
}
export function backendDescriptorHasCapability(backend: BackendDescriptor, tag: string): boolean {
  return (
    backend.materialization.some((item) => item.fileFormat === tag || item.tableFormat === tag) ||
    backend.query?.dialects.includes(tag as SqlDialect) === true
  )
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
    ["readiness", readiness],
    [
      "materialization",
      value.materialization.map(
        (item) =>
          new Map<string, unknown>([
            ["file_format", item.fileFormat],
            ["table_format", item.tableFormat],
            ["create_table", item.createTable],
            ["append", item.append]
          ])
      )
    ]
  ])
  optional(
    map,
    "query",
    value.query,
    (item) =>
      new Map<string, unknown>([
        ["dialects", [...item.dialects]],
        ["time_travel", [...item.timeTravel]],
        ["consistency", [...item.consistency]],
        ["logical_types", [...item.logicalTypes]],
        ["paging", [...item.paging]],
        ["cancellation", item.cancellation],
        ["execution_status", item.executionStatus],
        ["raw_sql", item.rawSql]
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
    "maintenance",
    new Map([
      ["expire_snapshots", value.maintenance.expireSnapshots],
      ["remove_orphan_files", value.maintenance.removeOrphanFiles],
      ["compact_data_files", value.maintenance.compactDataFiles]
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
      ["max_materialization_file_bytes", value.limits.maxMaterializationFileBytes]
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
  const maintenance = field.requiredMap(map, "maintenance", context)
  const limits = field.requiredMap(map, "limits", context)
  return {
    descriptorVersion: field.requiredU32(map, "descriptor_version", context),
    resourceId: BackendResourceId.fromBytes(field.requiredBytes(map, "resource_id", context)),
    mode: word(map, "mode", context, ["operational", "lakehouse"]),
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
            "probe_failed"
          ]),
          ...(detail === undefined ? {} : { detail })
        }
      }),
      observedAtMicros: field.requiredU64(readiness, "observed_at_micros", context)
    },
    materialization: field.requiredArray(map, "materialization", context, (item, index) => {
      const capability = expectMap(item, `${context}.materialization[${String(index)}]`)
      return {
        fileFormat: word(capability, "file_format", context, ["parquet"]),
        tableFormat: word(capability, "table_format", context, ["iceberg_v2"]),
        createTable: field.requiredBoolean(capability, "create_table", context),
        append: field.requiredBoolean(capability, "append", context)
      }
    }),
    ...(query === undefined
      ? {}
      : {
          query: {
            dialects: words(query, "dialects", context, [
              "data_fusion",
              "postgres",
              "my_sql",
              "sqlite"
            ]),
            timeTravel: words(query, "time_travel", context, ["snapshot_id", "timestamp_micros"]),
            consistency: words(query, "consistency", context, [
              "eventual",
              "read_your_writes",
              "strong"
            ]),
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
              "map"
            ]),
            paging: words(query, "paging", context, ["offset", "cursor"]),
            cancellation: field.requiredBoolean(query, "cancellation", context),
            executionStatus: field.requiredBoolean(query, "execution_status", context),
            rawSql: field.requiredBoolean(query, "raw_sql", context)
          }
        }),
    schema: {
      logicalSchema: field.requiredBoolean(schema, "logical_schema", context),
      arrowIpcStream: field.requiredBoolean(schema, "arrow_ipc_stream", context),
      schemaEvolution: field.requiredBoolean(schema, "schema_evolution", context)
    },
    maintenance: {
      expireSnapshots: field.requiredBoolean(maintenance, "expire_snapshots", context),
      removeOrphanFiles: field.requiredBoolean(maintenance, "remove_orphan_files", context),
      compactDataFiles: field.requiredBoolean(maintenance, "compact_data_files", context)
    },
    limits: {
      maxQueryRows: field.requiredU64(limits, "max_query_rows", context),
      maxQueryBytes: field.requiredU64(limits, "max_query_bytes", context),
      maxScanBytes: field.requiredU64(limits, "max_scan_bytes", context),
      maxQueryMicros: field.requiredU64(limits, "max_query_micros", context),
      maxConcurrentQueries: field.requiredU32(limits, "max_concurrent_queries", context),
      maxSchemaFields: field.requiredU32(limits, "max_schema_fields", context),
      maxMaterializationFileBytes: field.requiredU64(
        limits,
        "max_materialization_file_bytes",
        context
      )
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
  return encodeNamed(map)
}
export function decodeBackendAnnounce(bytes: Uint8Array): BackendAnnounce {
  const context = "BackendAnnounce"
  const map = expectMap(decodeOne(bytes, context), context)
  const topology = field.optionalMap(map, "topology", context)
  return {
    versions: decodeOpVersions(field.requiredMap(map, "versions", context), `${context}.versions`),
    ready: field.optionalBoolean(map, "ready", context) ?? true,
    backends: field.optionalArray(map, "backends", context, (item, index) =>
      decodeBackendDescriptor(item, `${context}.backends[${String(index)}]`)
    ),
    ...(topology === undefined
      ? {}
      : { topology: decodeWireTopology(topology, `${context}.topology`) })
  }
}

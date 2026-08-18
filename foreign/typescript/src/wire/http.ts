import { CodecError } from "../client/errors.js"
import {
  decodeProjectionInfo,
  decodeSchemaInfo,
  encodeProjectionInfo,
  encodeSchemaInfo,
  type ProjectionInfo,
  type SchemaInfo
} from "./browse.js"
import { type CborMap, expectMap, expectString, field } from "./cbor.js"
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
  decodeQueryExecutionStatus,
  encodeQueryExecutionStatus,
  type QueryExecutionStatus,
  type QueryResult
} from "./query.js"
import { decodeSourceRef, encodeSourceRef, type SourceRef } from "./graph.js"
import { type ResultCode } from "./result.js"
import { decodeWireTopology, encodeWireTopology, type WireTopology } from "./topology.js"
import {
  bigIntToBytes16,
  bytes16ToBigInt,
  CheckpointRequestId,
  crockfordDecode,
  crockfordEncode,
  DestinationId,
  DestinationOperationId,
  type QueryExecutionId
} from "./ids.js"
import {
  decodeDestinationBlock,
  decodeDestinationCheckpointStatus,
  decodePreparedAttemptSummary,
  decodeRetentionGap,
  encodeDestinationBlock,
  encodeDestinationCheckpointStatus,
  encodePreparedAttemptSummary,
  encodeRetentionGap,
  type CheckpointReadConsistency,
  type DestinationBlock,
  type DestinationCheckpointStatus,
  type PreparedAttemptSummary,
  type RetentionGap
} from "./checkpoint.js"
import {
  decodeMaterializationDestination,
  decodeQueryRoute,
  encodeMaterializationDestination,
  encodeQueryRoute,
  type MaterializationDestination,
  type QueryRoute
} from "./destination.js"
import {
  decodeLogicalSchema,
  decodeTypedValue,
  encodeLogicalSchema,
  encodeTypedValue,
  type LogicalSchema,
  type TypedValue
} from "./schema.js"

export const CAPABILITIES_PATH = "/agdx/capabilities"
export const QUERY_PATH = "/agdx/query"
export const DESTINATIONS_PATH = "/agdx/destinations"
export const QUERY_ROUTES_PATH = "/agdx/query-routes"
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
export const destinationPath = (id: DestinationId): string =>
  `${DESTINATIONS_PATH}/${id.toString()}`
export const destinationEnablePath = (id: DestinationId): string => `${destinationPath(id)}/enable`
export const destinationDisablePath = (id: DestinationId): string =>
  `${destinationPath(id)}/disable`
export const destinationStatusPath = (id: DestinationId): string => `${destinationPath(id)}/status`
export const destinationCheckpointPath = (id: DestinationId): string =>
  `${destinationPath(id)}/checkpoint`
export const destinationRetentionGapPath = (id: DestinationId): string =>
  `${destinationPath(id)}/retention-gap`
export const destinationPreparedAttemptPath = (id: DestinationId): string =>
  `${destinationPath(id)}/prepared-attempt`
export const destinationTablePath = (id: DestinationId): string => `${destinationPath(id)}/table`
export const destinationTableSchemaPath = (id: DestinationId): string =>
  `${destinationTablePath(id)}/schema`
export const destinationSnapshotsPath = (id: DestinationId): string =>
  `${destinationTablePath(id)}/snapshots`
export const destinationCurrentSnapshotPath = (id: DestinationId): string =>
  `${destinationTablePath(id)}/current-snapshot`
export const destinationSnapshotPath = (id: DestinationId, snapshotId: bigint): string =>
  `${destinationSnapshotsPath(id)}/${snapshotId.toString()}`
export const destinationFilesPath = (id: DestinationId): string =>
  `${destinationTablePath(id)}/files`
export const destinationMetricsPath = (id: DestinationId): string =>
  `${destinationTablePath(id)}/metrics`
export const queryExecutionPath = (id: QueryExecutionId): string => `${QUERY_PATH}/${id.toString()}`
export const queryPagePath = (id: QueryExecutionId): string => `${queryExecutionPath(id)}/pages`
export const queryCancelPath = (id: QueryExecutionId): string => `${queryExecutionPath(id)}/cancel`
export const destinationOperationPath = (id: DestinationOperationId): string =>
  `${DESTINATIONS_PATH}/operations/${id.toString()}`

export interface QueryCapsView {
  readonly available: boolean
  readonly projections: boolean
  readonly schemas: boolean
  readonly consistency: Consistency
  readonly keyword: boolean
  readonly cursorPaging: boolean
  readonly cancellation: boolean
  readonly executionStatus: boolean
}

export interface DestinationCapsView {
  readonly available: boolean
  readonly lifecycle: boolean
  readonly checkpointStatus: boolean
  readonly queryRoutes: boolean
  readonly tableSchema: boolean
  readonly snapshots: boolean
  readonly files: boolean
  readonly metrics: boolean
  readonly strongestConsistency: "potentially_stale" | "linearizable"
}

export interface KvCapsView {
  readonly available: boolean
  readonly cas: boolean
  readonly casFenced: boolean
  readonly fencedLeases: boolean
}

export interface HttpCapabilities {
  readonly managed: boolean
  readonly query: QueryCapsView
  readonly destinations: DestinationCapsView
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

export interface DestinationView {
  readonly destination: MaterializationDestination
  readonly status: DestinationCheckpointStatus
}

export interface DestinationPageView {
  readonly destinations: readonly DestinationView[]
  readonly nextCursor?: string
  readonly globalStateRevision: bigint
  readonly consistency: CheckpointReadConsistency
}

export interface QueryRoutePageView {
  readonly routes: readonly QueryRoute[]
  readonly nextCursor?: string
  readonly definitionRevision: bigint
}

export interface TableView {
  readonly tableUuid: Uint8Array
  readonly destinationId: DestinationId
  readonly destinationGeneration: bigint
  readonly namespace: readonly string[]
  readonly table: string
  readonly currentSnapshotId: bigint
  readonly currentSchemaId: number
  readonly currentPartitionSpecId: number
  readonly metadataIdentity: string
  readonly properties: ReadonlyMap<string, string>
}

export interface TableSchemaView {
  readonly tableUuid: Uint8Array
  readonly icebergSchemaId: number
  readonly logicalSchema: LogicalSchema
}

export interface TableSnapshotView {
  readonly snapshotId: bigint
  readonly parentSnapshotId?: bigint
  readonly sequenceNumber: bigint
  readonly committedAtMicros: bigint
  readonly schemaId: number
  readonly partitionSpecId: number
  readonly materializationBoundaryDigest: Uint8Array
  readonly checkpointRevision: bigint
  readonly summary: ReadonlyMap<string, string>
}

export interface SnapshotPageView {
  readonly snapshots: readonly TableSnapshotView[]
  readonly nextBeforeSnapshotId?: bigint
}

export interface TableFileView {
  readonly objectIdentity: string
  readonly fileSizeBytes: bigint
  readonly rowCount: bigint
  readonly partition: ReadonlyMap<string, TypedValue>
  readonly lowerBounds: ReadonlyMap<number, TypedValue>
  readonly upperBounds: ReadonlyMap<number, TypedValue>
  readonly nullValueCounts: ReadonlyMap<number, bigint>
}

export interface TableFilePageView {
  readonly files: readonly TableFileView[]
  readonly nextCursor?: string
}

export interface TableMetricsView {
  readonly snapshotId: bigint
  readonly dataFileCount: bigint
  readonly deleteFileCount: bigint
  readonly totalRows: bigint
  readonly totalBytes: bigint
  readonly partitionCount: bigint
}

export type OperationState = "accepted" | "running" | "succeeded" | "failed" | "cancelled"

export interface OperationErrorView {
  readonly code: ResultCode
  readonly message: string
}

export interface AcceptedOperationView {
  readonly operationId: DestinationOperationId
  readonly requestId: CheckpointRequestId
  readonly state: OperationState
  readonly submittedAtMicros: bigint
  readonly completedAtMicros?: bigint
  readonly error?: OperationErrorView
}

export interface QueryExecutionView {
  readonly status: QueryExecutionStatus
  readonly result?: QueryResult
}

export interface DestinationIssueView {
  readonly retentionGap?: RetentionGap
  readonly preparedAttempt?: PreparedAttemptSummary
  readonly block?: DestinationBlock
  readonly checkpointRevision: bigint
  readonly consistency: CheckpointReadConsistency
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
  ["step_up_required", { kind: "known", name: "StepUpRequired" }],
  ["unavailable", { kind: "known", name: "Unavailable" }],
  ["resource_limit", { kind: "known", name: "ResourceLimit" }],
  ["cancelled", { kind: "known", name: "Cancelled" }],
  ["deadline_exceeded", { kind: "known", name: "DeadlineExceeded" }],
  ["expired_snapshot", { kind: "known", name: "ExpiredSnapshot" }],
  ["stale_generation", { kind: "known", name: "StaleGeneration" }],
  ["target_unavailable", { kind: "known", name: "TargetUnavailable" }]
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
  StepUpRequired: "step_up_required",
  Unavailable: "unavailable",
  ResourceLimit: "resource_limit",
  Cancelled: "cancelled",
  DeadlineExceeded: "deadline_exceeded",
  ExpiredSnapshot: "expired_snapshot",
  StaleGeneration: "stale_generation",
  TargetUnavailable: "target_unavailable"
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

function fixedBytes(value: Uint8Array, length: number, context: string): Uint8Array {
  if (value.length !== length) {
    throw new CodecError(`${context} must contain ${String(length)} bytes`, context, "bytes")
  }
  return value
}

function enumWord<T extends string>(
  map: CborMap,
  key: string,
  context: string,
  values: readonly T[]
): T {
  const value = field.requiredString(map, key, context)
  if (!values.includes(value as T)) {
    throw new CodecError(`unknown ${key} \`${value}\``, context, key)
  }
  return value as T
}

function parseResultCode(value: string, context: string): ResultCode {
  const code = RESULT_NAMES.get(value)
  if (code === undefined) throw new CodecError(`unknown result code \`${value}\``, context, "code")
  return code
}

function encodeResultCode(value: ResultCode, context: string): string {
  if (value.kind === "unrecognized") {
    throw new CodecError("unrecognized numeric result codes have no JSON spelling", context, "code")
  }
  const word = RESULT_WORDS[value.name]
  if (word === undefined) throw new CodecError("result code has no JSON spelling", context, "code")
  return word
}

function decodeStringMap(map: CborMap, context: string): Map<string, string> {
  return new Map(
    [...map].map(([key, value]) => {
      const name = expectString(key, `${context} key`)
      return [name, expectString(value, `${context}.${name}`)] as const
    })
  )
}

function encodeStringMap(value: ReadonlyMap<string, string>): Map<string, string> {
  return new Map([...value].sort(([left], [right]) => left.localeCompare(right)))
}

function decodeTypedValueMap(map: CborMap, context: string): Map<string, TypedValue> {
  return new Map(
    [...map].map(([key, value]) => {
      const name = expectString(key, `${context} key`)
      return [name, decodeTypedValue(value, `${context}.${name}`)] as const
    })
  )
}

function encodeTypedValueMap(
  value: ReadonlyMap<string, TypedValue>
): Map<string, Map<string, unknown>> {
  return new Map(
    [...value]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => [key, encodeTypedValue(item)])
  )
}

function parseFieldId(key: string, context: string): number {
  const value = Number(key)
  if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff || String(value) !== key) {
    throw new CodecError(`invalid field id \`${key}\``, context, "key")
  }
  return value
}

function decodeFieldValueMap(map: CborMap, context: string): Map<number, TypedValue> {
  return new Map(
    [...map].map(([key, value]) => {
      const name = expectString(key, `${context} key`)
      return [parseFieldId(name, context), decodeTypedValue(value, `${context}.${name}`)] as const
    })
  )
}

function encodeFieldValueMap(
  value: ReadonlyMap<number, TypedValue>
): Map<string, Map<string, unknown>> {
  return new Map(
    [...value]
      .sort(([left], [right]) => left - right)
      .map(([key, item]) => [String(key), encodeTypedValue(item)])
  )
}

function decodeFieldCountMap(map: CborMap, context: string): Map<number, bigint> {
  return new Map(
    [...map].map(([key]) => {
      const name = expectString(key, `${context} key`)
      return [parseFieldId(name, context), field.requiredU64(map, name, context)] as const
    })
  )
}

function encodeFieldCountMap(value: ReadonlyMap<number, bigint>): Map<string, bigint> {
  return new Map(
    [...value].sort(([left], [right]) => left - right).map(([key, item]) => [String(key), item])
  )
}

function decodeDestinationView(map: CborMap, context: string): DestinationView {
  return {
    destination: decodeMaterializationDestination(
      field.requiredMap(map, "destination", context),
      `${context}.destination`
    ),
    status: decodeDestinationCheckpointStatus(
      field.requiredMap(map, "status", context),
      `${context}.status`
    )
  }
}

function encodeDestinationView(value: DestinationView): Map<string, unknown> {
  return new Map([
    ["destination", encodeMaterializationDestination(value.destination)],
    ["status", encodeDestinationCheckpointStatus(value.status)]
  ])
}

export function decodeDestinationPageJson(text: string): DestinationPageView {
  const context = "DestinationPageView"
  const map = expectMap(parseJson(text, context), context)
  const nextCursor = field.optionalString(map, "next_cursor", context)
  return {
    destinations: field.requiredArray(map, "destinations", context, (item, index) =>
      decodeDestinationView(
        expectMap(item, `${context}.destinations[${String(index)}]`),
        `${context}.destinations[${String(index)}]`
      )
    ),
    ...(nextCursor === undefined ? {} : { nextCursor }),
    globalStateRevision: field.requiredU64(map, "global_state_revision", context),
    consistency: enumWord(map, "consistency", context, ["linearizable", "potentially_stale"])
  }
}

export function encodeDestinationPageJson(value: DestinationPageView): string {
  const map = new Map<string, unknown>([
    ["destinations", value.destinations.map(encodeDestinationView)]
  ])
  if (value.nextCursor !== undefined) map.set("next_cursor", value.nextCursor)
  map.set("global_state_revision", value.globalStateRevision)
  map.set("consistency", value.consistency)
  return encodeJson(map, "DestinationPageView")
}

export function decodeQueryRoutePageJson(text: string): QueryRoutePageView {
  const context = "QueryRoutePageView"
  const map = expectMap(parseJson(text, context), context)
  const nextCursor = field.optionalString(map, "next_cursor", context)
  return {
    routes: field.requiredArray(map, "routes", context, (item, index) =>
      decodeQueryRoute(
        expectMap(item, `${context}.routes[${String(index)}]`),
        `${context}.routes[${String(index)}]`
      )
    ),
    ...(nextCursor === undefined ? {} : { nextCursor }),
    definitionRevision: field.requiredU64(map, "definition_revision", context)
  }
}

export function encodeQueryRoutePageJson(value: QueryRoutePageView): string {
  const map = new Map<string, unknown>([["routes", value.routes.map(encodeQueryRoute)]])
  if (value.nextCursor !== undefined) map.set("next_cursor", value.nextCursor)
  map.set("definition_revision", value.definitionRevision)
  return encodeJson(map, "QueryRoutePageView")
}

export function decodeTableViewJson(text: string): TableView {
  const context = "TableView"
  const map = expectMap(parseJson(text, context), context)
  return {
    tableUuid: fixedBytes(
      field.requiredBytes(map, "table_uuid", context),
      16,
      `${context}.table_uuid`
    ),
    destinationId: DestinationId.fromBytes(field.requiredBytes(map, "destination_id", context)),
    destinationGeneration: field.requiredU64(map, "destination_generation", context),
    namespace: field.requiredArray(map, "namespace", context, (item, index) =>
      expectString(item, `${context}.namespace[${String(index)}]`)
    ),
    table: field.requiredString(map, "table", context),
    currentSnapshotId: field.requiredI64(map, "current_snapshot_id", context),
    currentSchemaId: field.requiredI32(map, "current_schema_id", context),
    currentPartitionSpecId: field.requiredI32(map, "current_partition_spec_id", context),
    metadataIdentity: field.requiredString(map, "metadata_identity", context),
    properties: decodeStringMap(
      field.requiredMap(map, "properties", context),
      `${context}.properties`
    )
  }
}

export function encodeTableViewJson(value: TableView): string {
  return encodeJson(
    new Map<string, unknown>([
      ["table_uuid", value.tableUuid],
      ["destination_id", value.destinationId.toBytes()],
      ["destination_generation", value.destinationGeneration],
      ["namespace", [...value.namespace]],
      ["table", value.table],
      ["current_snapshot_id", value.currentSnapshotId],
      ["current_schema_id", value.currentSchemaId],
      ["current_partition_spec_id", value.currentPartitionSpecId],
      ["metadata_identity", value.metadataIdentity],
      ["properties", encodeStringMap(value.properties)]
    ]),
    "TableView"
  )
}

export function decodeTableSchemaJson(text: string): TableSchemaView {
  const context = "TableSchemaView"
  const map = expectMap(parseJson(text, context), context)
  return {
    tableUuid: fixedBytes(
      field.requiredBytes(map, "table_uuid", context),
      16,
      `${context}.table_uuid`
    ),
    icebergSchemaId: field.requiredI32(map, "iceberg_schema_id", context),
    logicalSchema: decodeLogicalSchema(
      field.requiredMap(map, "logical_schema", context),
      `${context}.logical_schema`
    )
  }
}

export function encodeTableSchemaJson(value: TableSchemaView): string {
  return encodeJson(
    new Map<string, unknown>([
      ["table_uuid", value.tableUuid],
      ["iceberg_schema_id", value.icebergSchemaId],
      ["logical_schema", encodeLogicalSchema(value.logicalSchema)]
    ]),
    "TableSchemaView"
  )
}

function decodeTableSnapshot(map: CborMap, context: string): TableSnapshotView {
  const parentSnapshotId = field.optionalI64(map, "parent_snapshot_id", context)
  return {
    snapshotId: field.requiredI64(map, "snapshot_id", context),
    ...(parentSnapshotId === undefined ? {} : { parentSnapshotId }),
    sequenceNumber: field.requiredI64(map, "sequence_number", context),
    committedAtMicros: field.requiredU64(map, "committed_at_micros", context),
    schemaId: field.requiredI32(map, "schema_id", context),
    partitionSpecId: field.requiredI32(map, "partition_spec_id", context),
    materializationBoundaryDigest: fixedBytes(
      field.requiredBytes(map, "materialization_boundary_digest", context),
      32,
      `${context}.materialization_boundary_digest`
    ),
    checkpointRevision: field.requiredU64(map, "checkpoint_revision", context),
    summary: decodeStringMap(field.requiredMap(map, "summary", context), `${context}.summary`)
  }
}

function encodeTableSnapshot(value: TableSnapshotView): Map<string, unknown> {
  const map = new Map<string, unknown>([["snapshot_id", value.snapshotId]])
  if (value.parentSnapshotId !== undefined) map.set("parent_snapshot_id", value.parentSnapshotId)
  map.set("sequence_number", value.sequenceNumber)
  map.set("committed_at_micros", value.committedAtMicros)
  map.set("schema_id", value.schemaId)
  map.set("partition_spec_id", value.partitionSpecId)
  map.set("materialization_boundary_digest", value.materializationBoundaryDigest)
  map.set("checkpoint_revision", value.checkpointRevision)
  map.set("summary", encodeStringMap(value.summary))
  return map
}

export function decodeSnapshotPageJson(text: string): SnapshotPageView {
  const context = "SnapshotPageView"
  const map = expectMap(parseJson(text, context), context)
  const nextBeforeSnapshotId = field.optionalI64(map, "next_before_snapshot_id", context)
  return {
    snapshots: field.requiredArray(map, "snapshots", context, (item, index) =>
      decodeTableSnapshot(
        expectMap(item, `${context}.snapshots[${String(index)}]`),
        `${context}.snapshots[${String(index)}]`
      )
    ),
    ...(nextBeforeSnapshotId === undefined ? {} : { nextBeforeSnapshotId })
  }
}

export function encodeSnapshotPageJson(value: SnapshotPageView): string {
  const map = new Map<string, unknown>([["snapshots", value.snapshots.map(encodeTableSnapshot)]])
  if (value.nextBeforeSnapshotId !== undefined) {
    map.set("next_before_snapshot_id", value.nextBeforeSnapshotId)
  }
  return encodeJson(map, "SnapshotPageView")
}

function decodeTableFile(map: CborMap, context: string): TableFileView {
  return {
    objectIdentity: field.requiredString(map, "object_identity", context),
    fileSizeBytes: field.requiredU64(map, "file_size_bytes", context),
    rowCount: field.requiredU64(map, "row_count", context),
    partition: decodeTypedValueMap(
      field.requiredMap(map, "partition", context),
      `${context}.partition`
    ),
    lowerBounds: decodeFieldValueMap(
      field.requiredMap(map, "lower_bounds", context),
      `${context}.lower_bounds`
    ),
    upperBounds: decodeFieldValueMap(
      field.requiredMap(map, "upper_bounds", context),
      `${context}.upper_bounds`
    ),
    nullValueCounts: decodeFieldCountMap(
      field.requiredMap(map, "null_value_counts", context),
      `${context}.null_value_counts`
    )
  }
}

function encodeTableFile(value: TableFileView): Map<string, unknown> {
  return new Map<string, unknown>([
    ["object_identity", value.objectIdentity],
    ["file_size_bytes", value.fileSizeBytes],
    ["row_count", value.rowCount],
    ["partition", encodeTypedValueMap(value.partition)],
    ["lower_bounds", encodeFieldValueMap(value.lowerBounds)],
    ["upper_bounds", encodeFieldValueMap(value.upperBounds)],
    ["null_value_counts", encodeFieldCountMap(value.nullValueCounts)]
  ])
}

export function decodeTableFilePageJson(text: string): TableFilePageView {
  const context = "TableFilePageView"
  const map = expectMap(parseJson(text, context), context)
  const nextCursor = field.optionalString(map, "next_cursor", context)
  return {
    files: field.requiredArray(map, "files", context, (item, index) =>
      decodeTableFile(
        expectMap(item, `${context}.files[${String(index)}]`),
        `${context}.files[${String(index)}]`
      )
    ),
    ...(nextCursor === undefined ? {} : { nextCursor })
  }
}

export function encodeTableFilePageJson(value: TableFilePageView): string {
  const map = new Map<string, unknown>([["files", value.files.map(encodeTableFile)]])
  if (value.nextCursor !== undefined) map.set("next_cursor", value.nextCursor)
  return encodeJson(map, "TableFilePageView")
}

export function decodeTableMetricsJson(text: string): TableMetricsView {
  const context = "TableMetricsView"
  const map = expectMap(parseJson(text, context), context)
  return {
    snapshotId: field.requiredI64(map, "snapshot_id", context),
    dataFileCount: field.requiredU64(map, "data_file_count", context),
    deleteFileCount: field.requiredU64(map, "delete_file_count", context),
    totalRows: field.requiredU64(map, "total_rows", context),
    totalBytes: field.requiredU64(map, "total_bytes", context),
    partitionCount: field.requiredU64(map, "partition_count", context)
  }
}

export function encodeTableMetricsJson(value: TableMetricsView): string {
  return encodeJson(
    new Map<string, unknown>([
      ["snapshot_id", value.snapshotId],
      ["data_file_count", value.dataFileCount],
      ["delete_file_count", value.deleteFileCount],
      ["total_rows", value.totalRows],
      ["total_bytes", value.totalBytes],
      ["partition_count", value.partitionCount]
    ]),
    "TableMetricsView"
  )
}

export function decodeAcceptedOperationJson(text: string): AcceptedOperationView {
  const context = "AcceptedOperationView"
  const map = expectMap(parseJson(text, context), context)
  const completedAtMicros = field.optionalU64(map, "completed_at_micros", context)
  const error = field.optionalMap(map, "error", context)
  return {
    operationId: DestinationOperationId.fromBytes(
      field.requiredBytes(map, "operation_id", context)
    ),
    requestId: CheckpointRequestId.fromBytes(field.requiredBytes(map, "request_id", context)),
    state: enumWord(map, "state", context, [
      "accepted",
      "running",
      "succeeded",
      "failed",
      "cancelled"
    ]),
    submittedAtMicros: field.requiredU64(map, "submitted_at_micros", context),
    ...(completedAtMicros === undefined ? {} : { completedAtMicros }),
    ...(error === undefined
      ? {}
      : {
          error: {
            code: parseResultCode(field.requiredString(error, "code", context), `${context}.error`),
            message: field.requiredString(error, "message", context)
          }
        })
  }
}

export function encodeAcceptedOperationJson(value: AcceptedOperationView): string {
  const context = "AcceptedOperationView"
  const map = new Map<string, unknown>([
    ["operation_id", value.operationId.toBytes()],
    ["request_id", value.requestId.toBytes()],
    ["state", value.state],
    ["submitted_at_micros", value.submittedAtMicros]
  ])
  if (value.completedAtMicros !== undefined) map.set("completed_at_micros", value.completedAtMicros)
  if (value.error !== undefined) {
    map.set(
      "error",
      new Map<string, unknown>([
        ["code", encodeResultCode(value.error.code, `${context}.error`)],
        ["message", value.error.message]
      ])
    )
  }
  return encodeJson(map, "AcceptedOperationView")
}

export function decodeQueryExecutionJson(text: string): QueryExecutionView {
  const context = "QueryExecutionView"
  const map = expectMap(parseJson(text, context), context)
  const result = field.optionalMap(map, "result", context)
  return {
    status: decodeQueryExecutionStatus(
      field.requiredMap(map, "status", context),
      `${context}.status`
    ),
    ...(result === undefined ? {} : { result: decodeQueryResult(result, `${context}.result`) })
  }
}

export function encodeQueryExecutionJson(value: QueryExecutionView): string {
  const map = new Map<string, unknown>([["status", encodeQueryExecutionStatus(value.status)]])
  if (value.result !== undefined) map.set("result", encodeQueryResult(value.result))
  return encodeJson(map, "QueryExecutionView")
}

export function decodeDestinationIssueJson(text: string): DestinationIssueView {
  const context = "DestinationIssueView"
  const map = expectMap(parseJson(text, context), context)
  const retentionGap = field.optionalMap(map, "retention_gap", context)
  const preparedAttempt = field.optionalMap(map, "prepared_attempt", context)
  const block = field.optionalMap(map, "block", context)
  return {
    ...(retentionGap === undefined
      ? {}
      : { retentionGap: decodeRetentionGap(retentionGap, `${context}.retention_gap`) }),
    ...(preparedAttempt === undefined
      ? {}
      : {
          preparedAttempt: decodePreparedAttemptSummary(
            preparedAttempt,
            `${context}.prepared_attempt`
          )
        }),
    ...(block === undefined ? {} : { block: decodeDestinationBlock(block, `${context}.block`) }),
    checkpointRevision: field.requiredU64(map, "checkpoint_revision", context),
    consistency: enumWord(map, "consistency", context, ["linearizable", "potentially_stale"])
  }
}

export function encodeDestinationIssueJson(value: DestinationIssueView): string {
  const map = new Map<string, unknown>()
  if (value.retentionGap !== undefined) {
    map.set("retention_gap", encodeRetentionGap(value.retentionGap))
  }
  if (value.preparedAttempt !== undefined) {
    map.set("prepared_attempt", encodePreparedAttemptSummary(value.preparedAttempt))
  }
  if (value.block !== undefined) map.set("block", encodeDestinationBlock(value.block))
  map.set("checkpoint_revision", value.checkpointRevision)
  map.set("consistency", value.consistency)
  return encodeJson(map, "DestinationIssueView")
}

function decodeQueryCaps(map: CborMap, context: string): QueryCapsView {
  return {
    available: field.requiredBoolean(map, "available", context),
    projections: field.requiredBoolean(map, "projections", context),
    schemas: field.requiredBoolean(map, "schemas", context),
    consistency: parseConsistency(
      field.optionalString(map, "consistency", context) ?? "eventual",
      context
    ),
    keyword: field.optionalBoolean(map, "keyword", context) ?? false,
    cursorPaging: field.optionalBoolean(map, "cursor_paging", context) ?? false,
    cancellation: field.optionalBoolean(map, "cancellation", context) ?? false,
    executionStatus: field.optionalBoolean(map, "execution_status", context) ?? false
  }
}

function encodeQueryCaps(value: QueryCapsView): Map<string, unknown> {
  return new Map<string, unknown>([
    ["available", value.available],
    ["projections", value.projections],
    ["schemas", value.schemas],
    ["consistency", consistencyToWord(value.consistency)],
    ["keyword", value.keyword],
    ["cursor_paging", value.cursorPaging],
    ["cancellation", value.cancellation],
    ["execution_status", value.executionStatus]
  ])
}

function decodeDestinationCaps(map: CborMap, context: string): DestinationCapsView {
  const consistency =
    field.optionalString(map, "strongest_consistency", context) ?? "potentially_stale"
  if (consistency !== "potentially_stale" && consistency !== "linearizable") {
    throw new CodecError(
      `unknown checkpoint consistency \`${consistency}\``,
      context,
      "strongest_consistency"
    )
  }
  return {
    available: field.requiredBoolean(map, "available", context),
    lifecycle: field.optionalBoolean(map, "lifecycle", context) ?? false,
    checkpointStatus: field.optionalBoolean(map, "checkpoint_status", context) ?? false,
    queryRoutes: field.optionalBoolean(map, "query_routes", context) ?? false,
    tableSchema: field.optionalBoolean(map, "table_schema", context) ?? false,
    snapshots: field.optionalBoolean(map, "snapshots", context) ?? false,
    files: field.optionalBoolean(map, "files", context) ?? false,
    metrics: field.optionalBoolean(map, "metrics", context) ?? false,
    strongestConsistency: consistency
  }
}

function encodeDestinationCaps(value: DestinationCapsView): Map<string, unknown> {
  return new Map<string, unknown>([
    ["available", value.available],
    ["lifecycle", value.lifecycle],
    ["checkpoint_status", value.checkpointStatus],
    ["query_routes", value.queryRoutes],
    ["table_schema", value.tableSchema],
    ["snapshots", value.snapshots],
    ["files", value.files],
    ["metrics", value.metrics],
    ["strongest_consistency", value.strongestConsistency]
  ])
}

export function decodeCapabilitiesJson(text: string): HttpCapabilities {
  const context = "Capabilities"
  const map = expectMap(parseJson(text, context), context)
  const topology = field.optionalMap(map, "topology", context)
  return {
    managed: field.requiredBoolean(map, "managed", context),
    query: decodeQueryCaps(field.requiredMap(map, "query", context), `${context}.query`),
    destinations: decodeDestinationCaps(
      field.requiredMap(map, "destinations", context),
      `${context}.destinations`
    ),
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
    ["destinations", encodeDestinationCaps(value.destinations)],
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

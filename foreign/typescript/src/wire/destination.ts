import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, expectMap, expectString, field } from "./cbor.js"
import { BackendResourceId, DestinationId, QueryRouteId } from "./ids.js"
import { type LogicalSchemaRef, decodeLogicalSchemaRef, encodeLogicalSchemaRef } from "./schema.js"
import {
  type SourceIncarnation,
  type SourceScope,
  decodeSourceIncarnation,
  decodeSourceScope,
  encodeSourceIncarnation,
  encodeSourceScope,
  validateSourceIncarnation,
  validateSourceScope
} from "./source.js"

export const MAX_DESTINATION_NAME_BYTES = 255
export const MAX_NAMESPACE_PARTS = 16
export const MAX_TABLE_NAME_BYTES = 255
export const MAX_EXPLICIT_PARTITION_STARTS = 65_536
export interface ProjectionRef {
  readonly id: string
  readonly version: number
}
export interface BackendBinding {
  readonly resourceId: BackendResourceId
  readonly generation: bigint
}
export interface PhysicalTable {
  readonly namespace: readonly string[]
  readonly table: string
  readonly expectedTableUuid?: Uint8Array
}
export type StartPolicy =
  | { readonly kind: "beginning" | "captured_latest" }
  | { readonly kind: "explicit"; readonly partitions: readonly PartitionStart[] }
export interface PartitionStart {
  readonly incarnation: SourceIncarnation
  readonly nextOffset: bigint
}
export type NewPartitionPolicy = "beginning" | "captured_latest" | "reject"
export type FileFormat = "parquet"
export type TableFormat = "iceberg_v2"
export type RecreatedPartitionPolicy = "reject"
export type DestinationErrorPolicy = "block"
export type DestinationDesiredState = "disabled" | "enabled"
export interface MaterializationDestination {
  readonly id: DestinationId
  readonly generation: bigint
  readonly definitionRevision: bigint
  readonly name: string
  readonly source: SourceScope
  readonly recreatedPartitionPolicy: RecreatedPartitionPolicy
  readonly projection: ProjectionRef
  readonly schema: LogicalSchemaRef
  readonly backend: BackendBinding
  readonly table: PhysicalTable
  readonly fileFormat: FileFormat
  readonly tableFormat: TableFormat
  readonly startPolicy: StartPolicy
  readonly newPartitionPolicy: NewPartitionPolicy
  readonly errorPolicy: DestinationErrorPolicy
  readonly desiredState: DestinationDesiredState
}
export type QueryRouteTarget =
  | { readonly kind: "operational"; readonly index: string }
  | {
      readonly kind: "lakehouse"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
    }
export interface QueryRoute {
  readonly id: QueryRouteId
  readonly generation: bigint
  readonly definitionRevision: bigint
  readonly name: string
  readonly target: QueryRouteTarget
  readonly desiredState: DestinationDesiredState
}

function mapOf(entries: readonly (readonly [string, unknown])[]): Map<string, unknown> {
  return new Map(entries)
}
function enumWord<T extends string>(
  map: CborMap,
  key: string,
  context: string,
  allowed: readonly T[]
): T {
  const value = field.requiredString(map, key, context)
  if (!allowed.includes(value as T))
    throw new CodecError(`unknown ${key} \`${value}\``, context, key)
  return value as T
}
export function encodeProjectionRef(value: ProjectionRef): Map<string, unknown> {
  return mapOf([
    ["id", value.id],
    ["version", value.version]
  ])
}
export function decodeProjectionRef(map: CborMap, context: string): ProjectionRef {
  return {
    id: field.requiredString(map, "id", context),
    version: field.requiredU32(map, "version", context)
  }
}
export function encodeBackendBinding(value: BackendBinding): Map<string, unknown> {
  return mapOf([
    ["resource_id", value.resourceId.toBytes()],
    ["generation", value.generation]
  ])
}
export function decodeBackendBinding(map: CborMap, context: string): BackendBinding {
  return {
    resourceId: BackendResourceId.fromBytes(field.requiredBytes(map, "resource_id", context)),
    generation: field.requiredU64(map, "generation", context)
  }
}
export function encodePhysicalTable(value: PhysicalTable): Map<string, unknown> {
  const map = mapOf([
    ["namespace", [...value.namespace]],
    ["table", value.table]
  ])
  if (value.expectedTableUuid !== undefined) map.set("expected_table_uuid", value.expectedTableUuid)
  return map
}
export function decodePhysicalTable(map: CborMap, context: string): PhysicalTable {
  const uuid = field.optionalBytes(map, "expected_table_uuid", context)
  return {
    namespace: field.requiredArray(map, "namespace", context, (item, index) =>
      expectString(item, `${context}.namespace[${String(index)}]`)
    ),
    table: field.requiredString(map, "table", context),
    ...(uuid === undefined ? {} : { expectedTableUuid: uuid })
  }
}
export function encodeStartPolicy(value: StartPolicy): Map<string, unknown> {
  const map = new Map<string, unknown>([["kind", value.kind]])
  if (value.kind === "explicit")
    map.set(
      "partitions",
      value.partitions.map((item) =>
        mapOf([
          ["incarnation", encodeSourceIncarnation(item.incarnation)],
          ["next_offset", item.nextOffset]
        ])
      )
    )
  return map
}
export function decodeStartPolicy(map: CborMap, context: string): StartPolicy {
  const kind = field.requiredString(map, "kind", context)
  if (kind === "beginning" || kind === "captured_latest") return { kind }
  if (kind === "explicit")
    return {
      kind,
      partitions: field.requiredArray(map, "partitions", context, (item, index) => {
        const partition = expectMap(item, `${context}.partitions[${String(index)}]`)
        return {
          incarnation: decodeSourceIncarnation(
            field.requiredMap(partition, "incarnation", context),
            context
          ),
          nextOffset: field.requiredU64(partition, "next_offset", context)
        }
      })
    }
  throw new CodecError(`unknown start policy \`${kind}\``, context, "kind")
}
export function encodeMaterializationDestination(
  value: MaterializationDestination
): Map<string, unknown> {
  return mapOf([
    ["id", value.id.toBytes()],
    ["generation", value.generation],
    ["definition_revision", value.definitionRevision],
    ["name", value.name],
    ["source", encodeSourceScope(value.source)],
    ["recreated_partition_policy", value.recreatedPartitionPolicy],
    ["projection", encodeProjectionRef(value.projection)],
    ["schema", encodeLogicalSchemaRef(value.schema)],
    ["backend", encodeBackendBinding(value.backend)],
    ["table", encodePhysicalTable(value.table)],
    ["file_format", value.fileFormat],
    ["table_format", value.tableFormat],
    ["start_policy", encodeStartPolicy(value.startPolicy)],
    ["new_partition_policy", value.newPartitionPolicy],
    ["error_policy", value.errorPolicy],
    ["desired_state", value.desiredState]
  ])
}
export function decodeMaterializationDestination(
  map: CborMap,
  context: string
): MaterializationDestination {
  return {
    id: DestinationId.fromBytes(field.requiredBytes(map, "id", context)),
    generation: field.requiredU64(map, "generation", context),
    definitionRevision: field.requiredU64(map, "definition_revision", context),
    name: field.requiredString(map, "name", context),
    source: decodeSourceScope(field.requiredMap(map, "source", context), context),
    recreatedPartitionPolicy: enumWord(map, "recreated_partition_policy", context, ["reject"]),
    projection: decodeProjectionRef(field.requiredMap(map, "projection", context), context),
    schema: decodeLogicalSchemaRef(field.requiredMap(map, "schema", context), context),
    backend: decodeBackendBinding(field.requiredMap(map, "backend", context), context),
    table: decodePhysicalTable(field.requiredMap(map, "table", context), context),
    fileFormat: enumWord(map, "file_format", context, ["parquet"]),
    tableFormat: enumWord(map, "table_format", context, ["iceberg_v2"]),
    startPolicy: decodeStartPolicy(field.requiredMap(map, "start_policy", context), context),
    newPartitionPolicy: enumWord(map, "new_partition_policy", context, [
      "beginning",
      "captured_latest",
      "reject"
    ]),
    errorPolicy: enumWord(map, "error_policy", context, ["block"]),
    desiredState: enumWord(map, "desired_state", context, ["disabled", "enabled"])
  }
}
function encodeQueryRouteTarget(value: QueryRouteTarget): Map<string, unknown> {
  const map = new Map<string, unknown>([["kind", value.kind]])
  if (value.kind === "operational") map.set("index", value.index)
  else {
    map.set("destination_id", value.destinationId.toBytes())
    map.set("destination_generation", value.destinationGeneration)
  }
  return map
}
function decodeQueryRouteTarget(map: CborMap, context: string): QueryRouteTarget {
  const kind = field.requiredString(map, "kind", context)
  if (kind === "operational") return { kind, index: field.requiredString(map, "index", context) }
  if (kind === "lakehouse")
    return {
      kind,
      destinationId: DestinationId.fromBytes(field.requiredBytes(map, "destination_id", context)),
      destinationGeneration: field.requiredU64(map, "destination_generation", context)
    }
  throw new CodecError(`unknown query route target \`${kind}\``, context, "kind")
}
export function encodeQueryRoute(value: QueryRoute): Map<string, unknown> {
  return mapOf([
    ["id", value.id.toBytes()],
    ["generation", value.generation],
    ["definition_revision", value.definitionRevision],
    ["name", value.name],
    ["target", encodeQueryRouteTarget(value.target)],
    ["desired_state", value.desiredState]
  ])
}
export function decodeQueryRoute(map: CborMap, context: string): QueryRoute {
  return {
    id: QueryRouteId.fromBytes(field.requiredBytes(map, "id", context)),
    generation: field.requiredU64(map, "generation", context),
    definitionRevision: field.requiredU64(map, "definition_revision", context),
    name: field.requiredString(map, "name", context),
    target: decodeQueryRouteTarget(field.requiredMap(map, "target", context), context),
    desiredState: enumWord(map, "desired_state", context, ["disabled", "enabled"])
  }
}
export function validateMaterializationDestination(value: MaterializationDestination): void {
  if (
    value.id.asU128() === 0n ||
    value.generation === 0n ||
    value.definitionRevision === 0n ||
    value.name.length === 0 ||
    value.backend.resourceId.asU128() === 0n ||
    value.backend.generation === 0n ||
    value.schema.id.asU128() === 0n ||
    value.schema.version === 0 ||
    value.schema.fingerprint.length !== 32
  )
    throw new InvalidError("materialization destination identity or binding is invalid")
  validateText("destination name", value.name, MAX_DESTINATION_NAME_BYTES)
  validateSourceScope(value.source)
  if (value.projection.id.length === 0 || value.projection.version === 0) {
    throw new InvalidError("projection identity or version is invalid")
  }
  if (value.table.namespace.length < 1 || value.table.namespace.length > MAX_NAMESPACE_PARTS) {
    throw new InvalidError("physical table namespace count is invalid")
  }
  for (const part of value.table.namespace)
    validateText("namespace part", part, MAX_TABLE_NAME_BYTES)
  validateText("table name", value.table.table, MAX_TABLE_NAME_BYTES)
  if (value.table.expectedTableUuid !== undefined && value.table.expectedTableUuid.length !== 16) {
    throw new InvalidError("expected table UUID must contain 16 bytes")
  }
  validateStartPolicy(value.startPolicy)
}

export function validateQueryRoute(value: QueryRoute): void {
  if (value.id.asU128() === 0n || value.generation === 0n || value.definitionRevision === 0n) {
    throw new InvalidError("query route identity, generation, or definition revision is invalid")
  }
  validateText("query route name", value.name, MAX_DESTINATION_NAME_BYTES)
  if (value.target.kind === "operational") {
    validateText("operational index", value.target.index, MAX_TABLE_NAME_BYTES)
  } else if (
    value.target.destinationId.asU128() === 0n ||
    value.target.destinationGeneration === 0n
  ) {
    throw new InvalidError("query route destination identity or generation is invalid")
  }
}

function validateStartPolicy(value: StartPolicy): void {
  if (value.kind !== "explicit") return
  if (value.partitions.length < 1 || value.partitions.length > MAX_EXPLICIT_PARTITION_STARTS) {
    throw new InvalidError("explicit start partition count is invalid")
  }
  let previousPartition: number | undefined
  let namespace: string | undefined
  for (const partition of value.partitions) {
    validateSourceIncarnation(partition.incarnation)
    const current = [
      partition.incarnation.cluster.asU128().toString(),
      String(partition.incarnation.streamId),
      String(partition.incarnation.topicId)
    ].join(":")
    if (namespace !== undefined && namespace !== current) {
      throw new InvalidError("explicit start partitions must share one source namespace")
    }
    if (previousPartition !== undefined && previousPartition >= partition.incarnation.partitionId) {
      throw new InvalidError("explicit start partitions must be ordered and unique")
    }
    namespace = current
    previousPartition = partition.incarnation.partitionId
  }
}

function validateText(label: string, value: string, cap: number): void {
  const bytes = new TextEncoder().encode(value).length
  if (bytes < 1 || bytes > cap || hasControlCharacter(value)) {
    throw new InvalidError(`${label} is empty, oversized, or contains a control character`)
  }
}

function hasControlCharacter(value: string): boolean {
  for (const character of value) {
    const code = character.codePointAt(0) ?? 0
    if (code < 32 || code === 127) return true
  }
  return false
}

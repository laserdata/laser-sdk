import { CodecError, InvalidError } from "../client/errors.js"
import {
  type CborMap,
  decodeOne,
  encodeNamed,
  expectMap,
  expectString,
  field,
  singleVariantTag
} from "./cbor.js"
import { CHECKPOINT_OP_VERSION } from "./codes.js"
import {
  type BackendBinding,
  type DestinationDesiredState,
  type MaterializationDestination,
  type ProjectionRef,
  type QueryRoute,
  decodeBackendBinding,
  decodeMaterializationDestination,
  decodeProjectionRef,
  decodeQueryRoute,
  encodeBackendBinding,
  encodeMaterializationDestination,
  encodeProjectionRef,
  encodeQueryRoute,
  validateMaterializationDestination,
  validateQueryRoute
} from "./destination.js"
import {
  CheckpointOwnerId,
  CheckpointRequestId,
  DestinationId,
  PreparedAttemptId,
  QueryRouteId
} from "./ids.js"
import { MAX_PAGE_SIZE } from "./limits.js"
import {
  MAX_LOGICAL_SCHEMA_FIELDS,
  type LogicalSchemaRef,
  type TypedValue,
  decodeLogicalSchemaRef,
  decodeTypedValue,
  encodeLogicalSchemaRef,
  encodeTypedValue,
  validateTypedValue
} from "./schema.js"
import {
  type SourceIncarnation,
  decodeSourceIncarnation,
  encodeSourceIncarnation,
  validateSourceIncarnation
} from "./source.js"

export const MAX_CHECKPOINT_PARTITIONS = 65_536
export const MAX_ATTEMPT_OBJECTS = 100_000
export const MAX_CREDENTIAL_GENERATIONS = 32
export const MAX_MANIFEST_IDENTITY_BYTES = 4_096
export const MAX_CHECKPOINT_ERROR_BYTES = 4_096
export const MAX_REPAIR_DETAIL_BYTES = 4_096
export const MAX_CHECKPOINT_LEASE_DURATION_MICROS = 300_000_000n
export const SUPERVISOR_ASSERTION_VERSION = 1
export const MAX_SUPERVISOR_ASSERTION_TTL_MICROS = 300_000_000n
export const SUPERVISOR_ASSERTION_KEY_ID_BYTES = 8
export const SUPERVISOR_ASSERTION_SIGNATURE_BYTES = 64
export type CheckpointReadConsistency = "linearizable" | "potentially_stale"
export type DestinationEffectiveState =
  "disabled" | "waiting_for_backend" | "ready" | "running" | "blocked"
export type PartitionLifecycleState = "active" | "removed" | "recreated"
export type DestinationBlockCode =
  | "decode"
  | "schema"
  | "projection"
  | "value"
  | "size"
  | "retention_gap"
  | "prepared_attempt"
  | "backend_generation"
  | "backend_unavailable"
  | "table_identity"
  | "catalog_outcome_unknown"
  | "source_incarnation"
  | "authorization"
const DESTINATION_BLOCK_CODES: readonly DestinationBlockCode[] = [
  "decode",
  "schema",
  "projection",
  "value",
  "size",
  "retention_gap",
  "prepared_attempt",
  "backend_generation",
  "backend_unavailable",
  "table_identity",
  "catalog_outcome_unknown",
  "source_incarnation",
  "authorization"
]
export type RepairAction =
  | "reconciled_prepared_attempt"
  | "accepted_retention_gap"
  | "cleared_retryable_block"
  | "superseded_generation"

export interface PartitionCheckpoint {
  readonly incarnation: SourceIncarnation
  readonly startedAtOffset: bigint
  readonly nextOffset: bigint
  readonly lifecycle: PartitionLifecycleState
}
export interface SourceOffsetRange {
  readonly incarnation: SourceIncarnation
  readonly start: bigint
  readonly endExclusive: bigint
}
export interface CheckpointOwnerLease {
  readonly owner: CheckpointOwnerId
  readonly epoch: bigint
  readonly sequence: bigint
  readonly deadlineMicros: bigint
}
export interface CredentialGeneration {
  readonly role: string
  readonly generation: bigint
}
export interface AttemptColumnMetrics {
  readonly fieldId: number
  readonly valueCount: bigint
  readonly nullCount: bigint
  readonly nanCount: bigint
  readonly lowerBound?: TypedValue
  readonly upperBound?: TypedValue
}
export interface AttemptObject {
  readonly identity: string
  readonly sizeBytes: bigint
  readonly rowCount: bigint
  readonly sha256: Uint8Array
  readonly columns: readonly AttemptColumnMetrics[]
}
export type IcebergCommitRequirement =
  | { readonly kind: "assert_table_uuid"; readonly tableUuid: Uint8Array }
  | { readonly kind: "assert_metadata_identity"; readonly identity: string }
  | { readonly kind: "assert_current_snapshot"; readonly snapshotId?: bigint }
  | { readonly kind: "assert_current_schema"; readonly schemaId: number }
  | { readonly kind: "assert_default_partition_spec"; readonly partitionSpecId: number }
export interface PreparedTableRequirements {
  readonly tableUuid: Uint8Array
  readonly baseMetadataIdentity: string
  readonly baseSnapshotId?: bigint
  readonly schemaId: number
  readonly partitionSpecId: number
  readonly commitRequirements: readonly IcebergCommitRequirement[]
}
export interface PreparedAttempt {
  readonly id: PreparedAttemptId
  readonly destinationId: DestinationId
  readonly destinationGeneration: bigint
  readonly backend: BackendBinding
  readonly owner: CheckpointOwnerId
  readonly epoch: bigint
  readonly createdAtCheckpointRevision: bigint
  readonly table: PreparedTableRequirements
  readonly schemaFingerprint: Uint8Array
  readonly projection: ProjectionRef
  readonly ranges: readonly SourceOffsetRange[]
  readonly resultingBoundary: readonly PartitionCheckpoint[]
  readonly resultingBoundaryDigest: Uint8Array
  readonly manifestIdentity: string
  readonly manifestDigest: Uint8Array
  readonly objects: readonly AttemptObject[]
  readonly credentialGenerations: readonly CredentialGeneration[]
}
export interface PreparedAttemptSummary {
  readonly id: PreparedAttemptId
  readonly owner: CheckpointOwnerId
  readonly epoch: bigint
  readonly table: PreparedTableRequirements
  readonly schemaFingerprint: Uint8Array
  readonly projection: ProjectionRef
  readonly manifestIdentity: string
  readonly manifestDigest: Uint8Array
  readonly resultingBoundaryDigest: Uint8Array
  readonly ranges: readonly SourceOffsetRange[]
  readonly objectCount: number
  readonly credentialGenerations: readonly CredentialGeneration[]
}
export interface CompletedAttempt {
  readonly id: PreparedAttemptId
  readonly tableUuid: Uint8Array
  readonly snapshotId: bigint
  readonly manifestDigest: Uint8Array
  readonly resultingBoundaryDigest: Uint8Array
  readonly ranges: readonly SourceOffsetRange[]
  readonly completionRevision: bigint
}
export interface RetentionGap {
  readonly incarnation: SourceIncarnation
  readonly requiredNextOffset: bigint
  readonly retainedStart: bigint
}
export interface DestinationBlock {
  readonly code: DestinationBlockCode
  readonly message: string
  readonly incarnation?: SourceIncarnation
  readonly offset?: bigint
  readonly rowOrdinal?: number
}
export interface RepairRecord {
  readonly action: RepairAction
  readonly detail: string
}
export interface DestinationCheckpointStatus {
  readonly destinationId: DestinationId
  readonly destinationGeneration: bigint
  readonly backend: BackendBinding
  readonly schema: LogicalSchemaRef
  readonly projection: ProjectionRef
  readonly globalStateRevision: bigint
  readonly definitionRevision: bigint
  readonly checkpointRevision: bigint
  readonly desiredState: DestinationDesiredState
  readonly effectiveState: DestinationEffectiveState
  readonly tableUuid?: Uint8Array
  readonly owner?: CheckpointOwnerLease
  readonly partitions: readonly PartitionCheckpoint[]
  readonly preparedAttempt?: PreparedAttemptSummary
  readonly lastCompletion?: CompletedAttempt
  readonly retentionGap?: RetentionGap
  readonly block?: DestinationBlock
  readonly lastRepair?: RepairRecord
  readonly consistency: CheckpointReadConsistency
}

export interface SupervisorActorAssertion {
  readonly claims: {
    readonly v: number
    readonly requestId: CheckpointRequestId
    readonly deploymentId: number
    readonly cloudUserId: number
    readonly action: "accept_retention_gap" | "supersede_generation" | "record_repair"
    readonly destinationId: DestinationId
    readonly destinationGeneration: bigint
    readonly expectedRevision?: bigint
    readonly issuedAtMicros: bigint
    readonly expiresAtMicros: bigint
  }
  readonly keyId: Uint8Array
  readonly signature: Uint8Array
}

export type PublicCheckpointMutation =
  | { readonly kind: "register_destination"; readonly destination: MaterializationDestination }
  | { readonly kind: "register_query_route"; readonly route: QueryRoute }
  | {
      readonly kind: "remove_query_route"
      readonly routeId: QueryRouteId
      readonly routeGeneration: bigint
      readonly expectedDefinitionRevision: bigint
    }
  | {
      readonly kind: "bind_table"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly expectedDefinitionRevision: bigint
      readonly tableUuid: Uint8Array
    }
  | {
      readonly kind: "set_desired_state"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly expectedDefinitionRevision: bigint
      readonly desiredState: DestinationDesiredState
    }
  | {
      readonly kind: "add_partition" | "observe_partition_lifecycle"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly expectedCheckpointRevision: bigint
      readonly partitionId: number
    }
  | {
      readonly kind: "acquire_lease" | "takeover_lease"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly owner: CheckpointOwnerId
      readonly expectedLeaseSequence: bigint
      readonly leaseDurationMicros: bigint
    }
  | {
      readonly kind: "renew_lease"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly owner: CheckpointOwnerId
      readonly epoch: bigint
      readonly expectedLeaseSequence: bigint
      readonly leaseDurationMicros: bigint
    }
  | {
      readonly kind: "prepare"
      readonly expectedCheckpointRevision: bigint
      readonly attempt: PreparedAttempt
    }
  | {
      readonly kind: "complete"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly owner: CheckpointOwnerId
      readonly epoch: bigint
      readonly expectedCheckpointRevision: bigint
      readonly completion: CompletedAttempt
    }
  | {
      readonly kind: "record_block"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly expectedCheckpointRevision: bigint
      readonly block: DestinationBlock
    }
  | {
      readonly kind: "clear_block"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly expectedCheckpointRevision: bigint
      readonly expectedCode: DestinationBlockCode
    }
  | {
      readonly kind: "record_retention_gap"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly expectedCheckpointRevision: bigint
      readonly gap: RetentionGap
    }
  | {
      readonly kind: "accept_retention_gap"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly expectedCheckpointRevision: bigint
      readonly nextOffset: bigint
    }
  | {
      readonly kind: "supersede_generation"
      readonly expectedDefinitionRevision: bigint
      readonly replacement: MaterializationDestination
    }
  | {
      readonly kind: "record_repair"
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly expectedCheckpointRevision: bigint
      readonly repair: RepairRecord
    }

export interface CheckpointRequestEnvelope {
  readonly v: number
  readonly requestId: CheckpointRequestId
  readonly expectedGlobalStateRevision: bigint
  readonly mutation: PublicCheckpointMutation
  readonly supervisorAssertion?: SupervisorActorAssertion
}
export type CheckpointMutationResult =
  | {
      readonly kind: "destination"
      readonly requestId: CheckpointRequestId
      readonly destinationId: DestinationId
      readonly destinationGeneration: bigint
      readonly globalStateRevision: bigint
      readonly definitionRevision: bigint
      readonly checkpointRevision: bigint
      readonly lease?: CheckpointOwnerLease
    }
  | {
      readonly kind: "query_route"
      readonly requestId: CheckpointRequestId
      readonly routeId: QueryRouteId
      readonly routeGeneration: bigint
      readonly globalStateRevision: bigint
      readonly definitionRevision: bigint
    }
export type CheckpointError =
  | { readonly kind: "invalid"; readonly message: string }
  | { readonly kind: "unavailable"; readonly message: string }
  | { readonly kind: "not_found" }
  | { readonly kind: "lease_lost" }
  | { readonly kind: "unauthorized" }
  | { readonly kind: "conflict"; readonly observedRevision: bigint }
  | { readonly kind: "version"; readonly expected: number; readonly got: number }
export type CheckpointReply =
  | { readonly kind: "ok"; readonly result: CheckpointMutationResult }
  | { readonly kind: "err"; readonly error: CheckpointError }
export interface DestinationGetRequest {
  readonly v: number
  readonly destinationId: DestinationId
  readonly consistency: CheckpointReadConsistency
}
export interface DestinationListFilter {
  readonly sourceStream?: string
  readonly sourceTopic?: string
  readonly nameContains?: string
}
export interface DestinationListRequest {
  readonly v: number
  readonly filter: DestinationListFilter
  readonly after?: DestinationId
  readonly limit: number
  readonly consistency: CheckpointReadConsistency
}
export interface QueryRouteListRequest {
  readonly v: number
  readonly nameContains?: string
  readonly after?: QueryRouteId
  readonly limit: number
  readonly consistency: CheckpointReadConsistency
}
export interface DestinationCheckpointView {
  readonly destination: MaterializationDestination
  readonly status: DestinationCheckpointStatus
}
export interface DestinationCheckpointPage {
  readonly destinations: readonly DestinationCheckpointView[]
  readonly nextAfter?: DestinationId
  readonly globalStateRevision: bigint
  readonly consistency: CheckpointReadConsistency
}
export interface QueryRoutePage {
  readonly routes: readonly QueryRoute[]
  readonly nextAfter?: QueryRouteId
  readonly globalStateRevision: bigint
  readonly consistency: CheckpointReadConsistency
}
export type CheckpointReadReply =
  | { readonly kind: "destination"; readonly destination?: DestinationCheckpointView }
  | { readonly kind: "destinations"; readonly page: DestinationCheckpointPage }
  | { readonly kind: "query_routes"; readonly page: QueryRoutePage }
  | { readonly kind: "err"; readonly error: CheckpointError }

function mapOf(entries: readonly (readonly [string, unknown])[]): Map<string, unknown> {
  return new Map(entries)
}
function optional<T>(
  map: Map<string, unknown>,
  key: string,
  value: T | undefined,
  encode: (item: T) => unknown = (item) => item
): void {
  if (value !== undefined) map.set(key, encode(value))
}
function commonMutation(
  map: Map<string, unknown>,
  value: {
    readonly destinationId: DestinationId
    readonly destinationGeneration: bigint
    readonly expectedCheckpointRevision?: bigint
    readonly expectedDefinitionRevision?: bigint
  }
): void {
  map.set("destination_id", value.destinationId.toBytes())
  map.set("destination_generation", value.destinationGeneration)
  optional(map, "expected_checkpoint_revision", value.expectedCheckpointRevision)
  optional(map, "expected_definition_revision", value.expectedDefinitionRevision)
}
function decodeLease(map: CborMap, context: string): CheckpointOwnerLease {
  return {
    owner: CheckpointOwnerId.fromBytes(field.requiredBytes(map, "owner", context)),
    epoch: field.requiredU64(map, "epoch", context),
    sequence: field.requiredU64(map, "sequence", context),
    deadlineMicros: field.requiredU64(map, "deadline_micros", context)
  }
}
function encodeLease(value: CheckpointOwnerLease): Map<string, unknown> {
  return mapOf([
    ["owner", value.owner.toBytes()],
    ["epoch", value.epoch],
    ["sequence", value.sequence],
    ["deadline_micros", value.deadlineMicros]
  ])
}
function encodePartition(value: PartitionCheckpoint): Map<string, unknown> {
  return mapOf([
    ["incarnation", encodeSourceIncarnation(value.incarnation)],
    ["started_at_offset", value.startedAtOffset],
    ["next_offset", value.nextOffset],
    ["lifecycle", value.lifecycle]
  ])
}
function decodePartition(map: CborMap, context: string): PartitionCheckpoint {
  return {
    incarnation: decodeSourceIncarnation(field.requiredMap(map, "incarnation", context), context),
    startedAtOffset: field.requiredU64(map, "started_at_offset", context),
    nextOffset: field.requiredU64(map, "next_offset", context),
    lifecycle: enumWord(map, "lifecycle", context, ["active", "removed", "recreated"])
  }
}
function encodeRange(value: SourceOffsetRange): Map<string, unknown> {
  return mapOf([
    ["incarnation", encodeSourceIncarnation(value.incarnation)],
    ["start", value.start],
    ["end_exclusive", value.endExclusive]
  ])
}
function encodeRequirement(value: IcebergCommitRequirement): Map<string, unknown> {
  const map = new Map<string, unknown>([["kind", value.kind]])
  switch (value.kind) {
    case "assert_table_uuid":
      map.set("table_uuid", value.tableUuid)
      break
    case "assert_metadata_identity":
      map.set("identity", value.identity)
      break
    case "assert_current_snapshot":
      optional(map, "snapshot_id", value.snapshotId)
      break
    case "assert_current_schema":
      map.set("schema_id", value.schemaId)
      break
    case "assert_default_partition_spec":
      map.set("partition_spec_id", value.partitionSpecId)
      break
  }
  return map
}
function encodeTable(value: PreparedTableRequirements): Map<string, unknown> {
  const map = mapOf([
    ["table_uuid", value.tableUuid],
    ["base_metadata_identity", value.baseMetadataIdentity]
  ])
  optional(map, "base_snapshot_id", value.baseSnapshotId)
  map.set("schema_id", value.schemaId)
  map.set("partition_spec_id", value.partitionSpecId)
  map.set("commit_requirements", value.commitRequirements.map(encodeRequirement))
  return map
}
function encodeCompletion(value: CompletedAttempt): Map<string, unknown> {
  return mapOf([
    ["id", value.id.toBytes()],
    ["table_uuid", value.tableUuid],
    ["snapshot_id", value.snapshotId],
    ["manifest_digest", value.manifestDigest],
    ["resulting_boundary_digest", value.resultingBoundaryDigest],
    ["ranges", value.ranges.map(encodeRange)],
    ["completion_revision", value.completionRevision]
  ])
}
export function encodeRetentionGap(value: RetentionGap): Map<string, unknown> {
  return mapOf([
    ["incarnation", encodeSourceIncarnation(value.incarnation)],
    ["required_next_offset", value.requiredNextOffset],
    ["retained_start", value.retainedStart]
  ])
}
export function encodeDestinationBlock(value: DestinationBlock): Map<string, unknown> {
  const map = mapOf([
    ["code", value.code],
    ["message", value.message]
  ])
  optional(map, "incarnation", value.incarnation, encodeSourceIncarnation)
  optional(map, "offset", value.offset)
  optional(map, "row_ordinal", value.rowOrdinal)
  return map
}
function encodeRepair(value: RepairRecord): Map<string, unknown> {
  return mapOf([
    ["action", value.action],
    ["detail", value.detail]
  ])
}
function encodePreparedAttempt(value: PreparedAttempt): Map<string, unknown> {
  return mapOf([
    ["id", value.id.toBytes()],
    ["destination_id", value.destinationId.toBytes()],
    ["destination_generation", value.destinationGeneration],
    ["backend", encodeBackendBinding(value.backend)],
    ["owner", value.owner.toBytes()],
    ["epoch", value.epoch],
    ["created_at_checkpoint_revision", value.createdAtCheckpointRevision],
    ["table", encodeTable(value.table)],
    ["schema_fingerprint", value.schemaFingerprint],
    ["projection", encodeProjectionRef(value.projection)],
    ["ranges", value.ranges.map(encodeRange)],
    ["resulting_boundary", value.resultingBoundary.map(encodePartition)],
    ["resulting_boundary_digest", value.resultingBoundaryDigest],
    ["manifest_identity", value.manifestIdentity],
    ["manifest_digest", value.manifestDigest],
    [
      "objects",
      value.objects.map((object) =>
        mapOf([
          ["identity", object.identity],
          ["size_bytes", object.sizeBytes],
          ["row_count", object.rowCount],
          ["sha256", object.sha256],
          [
            "columns",
            object.columns.map((column) => {
              const map = mapOf([
                ["field_id", column.fieldId],
                ["value_count", column.valueCount],
                ["null_count", column.nullCount],
                ["nan_count", column.nanCount]
              ])
              optional(map, "lower_bound", column.lowerBound, encodeTypedValue)
              optional(map, "upper_bound", column.upperBound, encodeTypedValue)
              return map
            })
          ]
        ])
      )
    ],
    [
      "credential_generations",
      value.credentialGenerations.map((item) =>
        mapOf([
          ["role", item.role],
          ["generation", item.generation]
        ])
      )
    ]
  ])
}

export function encodePreparedAttemptSummary(value: PreparedAttemptSummary): Map<string, unknown> {
  return mapOf([
    ["id", value.id.toBytes()],
    ["owner", value.owner.toBytes()],
    ["epoch", value.epoch],
    ["table", encodeTable(value.table)],
    ["schema_fingerprint", value.schemaFingerprint],
    ["projection", encodeProjectionRef(value.projection)],
    ["manifest_identity", value.manifestIdentity],
    ["manifest_digest", value.manifestDigest],
    ["resulting_boundary_digest", value.resultingBoundaryDigest],
    ["ranges", value.ranges.map(encodeRange)],
    ["object_count", value.objectCount],
    [
      "credential_generations",
      value.credentialGenerations.map((item) =>
        mapOf([
          ["role", item.role],
          ["generation", item.generation]
        ])
      )
    ]
  ])
}

export function encodePublicCheckpointMutation(
  value: PublicCheckpointMutation
): Map<string, unknown> {
  const map = new Map<string, unknown>([["kind", value.kind]])
  switch (value.kind) {
    case "register_destination":
      map.set("destination", encodeMaterializationDestination(value.destination))
      break
    case "register_query_route":
      map.set("route", encodeQueryRoute(value.route))
      break
    case "remove_query_route":
      map.set("route_id", value.routeId.toBytes())
      map.set("route_generation", value.routeGeneration)
      map.set("expected_definition_revision", value.expectedDefinitionRevision)
      break
    case "bind_table":
      commonMutation(map, value)
      map.set("table_uuid", value.tableUuid)
      break
    case "set_desired_state":
      commonMutation(map, value)
      map.set("desired_state", value.desiredState)
      break
    case "add_partition":
    case "observe_partition_lifecycle":
      commonMutation(map, value)
      map.set("partition_id", value.partitionId)
      break
    case "acquire_lease":
    case "takeover_lease":
      commonMutation(map, value)
      map.set("owner", value.owner.toBytes())
      map.set("expected_lease_sequence", value.expectedLeaseSequence)
      map.set("lease_duration_micros", value.leaseDurationMicros)
      break
    case "renew_lease":
      commonMutation(map, value)
      map.set("owner", value.owner.toBytes())
      map.set("epoch", value.epoch)
      map.set("expected_lease_sequence", value.expectedLeaseSequence)
      map.set("lease_duration_micros", value.leaseDurationMicros)
      break
    case "prepare":
      map.set("expected_checkpoint_revision", value.expectedCheckpointRevision)
      map.set("attempt", encodePreparedAttempt(value.attempt))
      break
    case "complete":
      map.set("destination_id", value.destinationId.toBytes())
      map.set("destination_generation", value.destinationGeneration)
      map.set("owner", value.owner.toBytes())
      map.set("epoch", value.epoch)
      map.set("expected_checkpoint_revision", value.expectedCheckpointRevision)
      map.set("completion", encodeCompletion(value.completion))
      break
    case "record_block":
      commonMutation(map, value)
      map.set("block", encodeDestinationBlock(value.block))
      break
    case "clear_block":
      commonMutation(map, value)
      map.set("expected_code", value.expectedCode)
      break
    case "record_retention_gap":
      commonMutation(map, value)
      map.set("gap", encodeRetentionGap(value.gap))
      break
    case "accept_retention_gap":
      commonMutation(map, value)
      map.set("next_offset", value.nextOffset)
      break
    case "supersede_generation":
      map.set("expected_definition_revision", value.expectedDefinitionRevision)
      map.set("replacement", encodeMaterializationDestination(value.replacement))
      break
    case "record_repair":
      commonMutation(map, value)
      map.set("repair", encodeRepair(value.repair))
      break
  }
  return map
}

function decodePreparedAttempt(map: CborMap, context: string): PreparedAttempt {
  return {
    id: PreparedAttemptId.fromBytes(field.requiredBytes(map, "id", context)),
    destinationId: DestinationId.fromBytes(field.requiredBytes(map, "destination_id", context)),
    destinationGeneration: field.requiredU64(map, "destination_generation", context),
    backend: decodeBackendBinding(field.requiredMap(map, "backend", context), `${context}.backend`),
    owner: CheckpointOwnerId.fromBytes(field.requiredBytes(map, "owner", context)),
    epoch: field.requiredU64(map, "epoch", context),
    createdAtCheckpointRevision: field.requiredU64(map, "created_at_checkpoint_revision", context),
    table: decodeTable(field.requiredMap(map, "table", context), `${context}.table`),
    schemaFingerprint: field.requiredBytes(map, "schema_fingerprint", context),
    projection: decodeProjectionRef(
      field.requiredMap(map, "projection", context),
      `${context}.projection`
    ),
    ranges: field.requiredArray(map, "ranges", context, (item, index) =>
      decodeRange(expectMap(item, `${context}.ranges[${String(index)}]`), `${context}.ranges`)
    ),
    resultingBoundary: field.requiredArray(map, "resulting_boundary", context, (item, index) =>
      decodePartition(
        expectMap(item, `${context}.resulting_boundary[${String(index)}]`),
        `${context}.resulting_boundary`
      )
    ),
    resultingBoundaryDigest: field.requiredBytes(map, "resulting_boundary_digest", context),
    manifestIdentity: field.requiredString(map, "manifest_identity", context),
    manifestDigest: field.requiredBytes(map, "manifest_digest", context),
    objects: field.requiredArray(map, "objects", context, (item, index) =>
      decodeAttemptObject(
        expectMap(item, `${context}.objects[${String(index)}]`),
        `${context}.objects[${String(index)}]`
      )
    ),
    credentialGenerations: field.requiredArray(
      map,
      "credential_generations",
      context,
      (item, index) =>
        decodeCredential(
          expectMap(item, `${context}.credential_generations[${String(index)}]`),
          `${context}.credential_generations[${String(index)}]`
        )
    )
  }
}

function decodeAttemptObject(map: CborMap, context: string): AttemptObject {
  return {
    identity: field.requiredString(map, "identity", context),
    sizeBytes: field.requiredU64(map, "size_bytes", context),
    rowCount: field.requiredU64(map, "row_count", context),
    sha256: field.requiredBytes(map, "sha256", context),
    columns: field.requiredArray(map, "columns", context, (item, index) =>
      decodeAttemptColumnMetrics(
        expectMap(item, `${context}.columns[${String(index)}]`),
        `${context}.columns[${String(index)}]`
      )
    )
  }
}

function decodeAttemptColumnMetrics(map: CborMap, context: string): AttemptColumnMetrics {
  const lowerBound = map.get("lower_bound")
  const upperBound = map.get("upper_bound")
  return {
    fieldId: field.requiredU32(map, "field_id", context),
    valueCount: field.requiredU64(map, "value_count", context),
    nullCount: field.requiredU64(map, "null_count", context),
    nanCount: field.requiredU64(map, "nan_count", context),
    ...(lowerBound === undefined
      ? {}
      : { lowerBound: decodeTypedValue(lowerBound, `${context}.lower_bound`) }),
    ...(upperBound === undefined
      ? {}
      : { upperBound: decodeTypedValue(upperBound, `${context}.upper_bound`) })
  }
}

function decodeMutationDestination(map: CborMap, context: string) {
  return {
    destinationId: DestinationId.fromBytes(field.requiredBytes(map, "destination_id", context)),
    destinationGeneration: field.requiredU64(map, "destination_generation", context)
  }
}

export function decodePublicCheckpointMutation(
  map: CborMap,
  context: string
): PublicCheckpointMutation {
  const kind = field.requiredString(map, "kind", context)
  const destination = () => decodeMutationDestination(map, context)
  const checkpointRevision = () => field.requiredU64(map, "expected_checkpoint_revision", context)
  const definitionRevision = () => field.requiredU64(map, "expected_definition_revision", context)

  switch (kind) {
    case "register_destination":
      return {
        kind,
        destination: decodeMaterializationDestination(
          field.requiredMap(map, "destination", context),
          `${context}.destination`
        )
      }
    case "register_query_route":
      return {
        kind,
        route: decodeQueryRoute(field.requiredMap(map, "route", context), `${context}.route`)
      }
    case "remove_query_route":
      return {
        kind,
        routeId: QueryRouteId.fromBytes(field.requiredBytes(map, "route_id", context)),
        routeGeneration: field.requiredU64(map, "route_generation", context),
        expectedDefinitionRevision: definitionRevision()
      }
    case "bind_table":
      return {
        kind,
        ...destination(),
        expectedDefinitionRevision: definitionRevision(),
        tableUuid: field.requiredBytes(map, "table_uuid", context)
      }
    case "set_desired_state":
      return {
        kind,
        ...destination(),
        expectedDefinitionRevision: definitionRevision(),
        desiredState: enumWord(map, "desired_state", context, ["disabled", "enabled"])
      }
    case "add_partition":
    case "observe_partition_lifecycle":
      return {
        kind,
        ...destination(),
        expectedCheckpointRevision: checkpointRevision(),
        partitionId: field.requiredU32(map, "partition_id", context)
      }
    case "acquire_lease":
    case "takeover_lease":
      return {
        kind,
        ...destination(),
        owner: CheckpointOwnerId.fromBytes(field.requiredBytes(map, "owner", context)),
        expectedLeaseSequence: field.requiredU64(map, "expected_lease_sequence", context),
        leaseDurationMicros: field.requiredU64(map, "lease_duration_micros", context)
      }
    case "renew_lease":
      return {
        kind,
        ...destination(),
        owner: CheckpointOwnerId.fromBytes(field.requiredBytes(map, "owner", context)),
        epoch: field.requiredU64(map, "epoch", context),
        expectedLeaseSequence: field.requiredU64(map, "expected_lease_sequence", context),
        leaseDurationMicros: field.requiredU64(map, "lease_duration_micros", context)
      }
    case "prepare":
      return {
        kind,
        expectedCheckpointRevision: checkpointRevision(),
        attempt: decodePreparedAttempt(
          field.requiredMap(map, "attempt", context),
          `${context}.attempt`
        )
      }
    case "complete":
      return {
        kind,
        ...destination(),
        owner: CheckpointOwnerId.fromBytes(field.requiredBytes(map, "owner", context)),
        epoch: field.requiredU64(map, "epoch", context),
        expectedCheckpointRevision: checkpointRevision(),
        completion: decodeCompletion(
          field.requiredMap(map, "completion", context),
          `${context}.completion`
        )
      }
    case "record_block":
      return {
        kind,
        ...destination(),
        expectedCheckpointRevision: checkpointRevision(),
        block: decodeDestinationBlock(field.requiredMap(map, "block", context), `${context}.block`)
      }
    case "clear_block":
      return {
        kind,
        ...destination(),
        expectedCheckpointRevision: checkpointRevision(),
        expectedCode: enumWord(map, "expected_code", context, DESTINATION_BLOCK_CODES)
      }
    case "record_retention_gap":
      return {
        kind,
        ...destination(),
        expectedCheckpointRevision: checkpointRevision(),
        gap: decodeRetentionGap(field.requiredMap(map, "gap", context), `${context}.gap`)
      }
    case "accept_retention_gap":
      return {
        kind,
        ...destination(),
        expectedCheckpointRevision: checkpointRevision(),
        nextOffset: field.requiredU64(map, "next_offset", context)
      }
    case "supersede_generation":
      return {
        kind,
        expectedDefinitionRevision: definitionRevision(),
        replacement: decodeMaterializationDestination(
          field.requiredMap(map, "replacement", context),
          `${context}.replacement`
        )
      }
    case "record_repair":
      return {
        kind,
        ...destination(),
        expectedCheckpointRevision: checkpointRevision(),
        repair: decodeRepair(field.requiredMap(map, "repair", context), `${context}.repair`)
      }
    default:
      throw new CodecError(`unknown public checkpoint mutation \`${kind}\``, context, "kind")
  }
}

function encodeAssertion(value: SupervisorActorAssertion): Map<string, unknown> {
  const claims = mapOf([
    ["v", value.claims.v],
    ["request_id", value.claims.requestId.toBytes()],
    ["deployment_id", value.claims.deploymentId],
    ["cloud_user_id", value.claims.cloudUserId],
    ["action", value.claims.action],
    ["destination_id", value.claims.destinationId.toBytes()],
    ["destination_generation", value.claims.destinationGeneration]
  ])
  optional(claims, "expected_revision", value.claims.expectedRevision)
  claims.set("issued_at_micros", value.claims.issuedAtMicros)
  claims.set("expires_at_micros", value.claims.expiresAtMicros)
  return mapOf([
    ["claims", claims],
    ["key_id", value.keyId],
    ["signature", value.signature]
  ])
}

function decodeAssertion(map: CborMap, context: string): SupervisorActorAssertion {
  const claims = field.requiredMap(map, "claims", context)
  const expectedRevision = field.optionalU64(claims, "expected_revision", `${context}.claims`)
  return {
    claims: {
      v: field.requiredU32(claims, "v", `${context}.claims`),
      requestId: CheckpointRequestId.fromBytes(
        field.requiredBytes(claims, "request_id", `${context}.claims`)
      ),
      deploymentId: field.requiredU32(claims, "deployment_id", `${context}.claims`),
      cloudUserId: field.requiredU32(claims, "cloud_user_id", `${context}.claims`),
      action: enumWord(claims, "action", `${context}.claims`, [
        "accept_retention_gap",
        "supersede_generation",
        "record_repair"
      ]),
      destinationId: DestinationId.fromBytes(
        field.requiredBytes(claims, "destination_id", `${context}.claims`)
      ),
      destinationGeneration: field.requiredU64(
        claims,
        "destination_generation",
        `${context}.claims`
      ),
      ...(expectedRevision === undefined ? {} : { expectedRevision }),
      issuedAtMicros: field.requiredU64(claims, "issued_at_micros", `${context}.claims`),
      expiresAtMicros: field.requiredU64(claims, "expires_at_micros", `${context}.claims`)
    },
    keyId: field.requiredBytes(map, "key_id", context),
    signature: field.requiredBytes(map, "signature", context)
  }
}
export function encodeCheckpointRequest(value: CheckpointRequestEnvelope): Map<string, unknown> {
  const map = mapOf([
    ["v", value.v],
    ["request_id", value.requestId.toBytes()],
    ["expected_global_state_revision", value.expectedGlobalStateRevision],
    ["mutation", encodePublicCheckpointMutation(value.mutation)]
  ])
  optional(map, "supervisor_assertion", value.supervisorAssertion, encodeAssertion)
  return map
}
export function decodeCheckpointRequest(map: CborMap, context: string): CheckpointRequestEnvelope {
  const supervisorAssertion = field.optionalMap(map, "supervisor_assertion", context)
  return {
    v: field.requiredU32(map, "v", context),
    requestId: CheckpointRequestId.fromBytes(field.requiredBytes(map, "request_id", context)),
    expectedGlobalStateRevision: field.requiredU64(map, "expected_global_state_revision", context),
    mutation: decodePublicCheckpointMutation(
      field.requiredMap(map, "mutation", context),
      `${context}.mutation`
    ),
    ...(supervisorAssertion === undefined
      ? {}
      : {
          supervisorAssertion: decodeAssertion(
            supervisorAssertion,
            `${context}.supervisor_assertion`
          )
        })
  }
}
export function decodeCheckpointRequestFrame(bytes: Uint8Array): CheckpointRequestEnvelope {
  const context = "CheckpointRequestEnvelope"
  const value = decodeCheckpointRequest(expectMap(decodeOne(bytes, context), context), context)
  validateCheckpointRequest(value)
  return value
}
export function encodeCheckpointRequestFrame(value: CheckpointRequestEnvelope): Uint8Array {
  validateCheckpointRequest(value)
  return encodeNamed(encodeCheckpointRequest(value))
}
export function validateCheckpointRequest(value: CheckpointRequestEnvelope): void {
  if (value.v !== CHECKPOINT_OP_VERSION || value.requestId.asU128() === 0n)
    throw new InvalidError("checkpoint request version or identity is invalid")
  validatePublicCheckpointMutation(value.mutation)
  validateSupervisorAssertionBinding(value)
}

export function validatePublicCheckpointMutation(value: PublicCheckpointMutation): void {
  switch (value.kind) {
    case "register_destination":
      validateMaterializationDestination(value.destination)
      return
    case "register_query_route":
      validateQueryRoute(value.route)
      return
    case "remove_query_route":
      if (
        value.routeId.asU128() === 0n ||
        value.routeGeneration === 0n ||
        value.expectedDefinitionRevision === 0n
      )
        throw new InvalidError("query route identity, generation, and revision must be nonzero")
      return
    case "bind_table":
      validateDestinationRevision(
        value.destinationId,
        value.destinationGeneration,
        value.expectedDefinitionRevision,
        "definition"
      )
      fixedBytes(value.tableUuid, 16, "table UUID")
      return
    case "set_desired_state":
      validateDestinationRevision(
        value.destinationId,
        value.destinationGeneration,
        value.expectedDefinitionRevision,
        "definition"
      )
      return
    case "add_partition":
    case "observe_partition_lifecycle":
    case "clear_block":
    case "accept_retention_gap":
      validateDestinationRevision(
        value.destinationId,
        value.destinationGeneration,
        value.expectedCheckpointRevision,
        "checkpoint"
      )
      return
    case "acquire_lease":
    case "takeover_lease":
      validateLeaseRequest(value)
      return
    case "renew_lease":
      validateLeaseRequest(value)
      if (value.epoch === 0n) throw new InvalidError("lease epoch must be nonzero")
      return
    case "prepare":
      if (value.expectedCheckpointRevision === 0n)
        throw new InvalidError("expected checkpoint revision must be nonzero")
      validatePreparedAttempt(value.attempt)
      return
    case "complete":
      validateDestinationRevision(
        value.destinationId,
        value.destinationGeneration,
        value.expectedCheckpointRevision,
        "checkpoint"
      )
      if (value.owner.asU128() === 0n || value.epoch === 0n)
        throw new InvalidError("completion owner and epoch must be nonzero")
      validateCompletedAttempt(value.completion)
      return
    case "record_block":
      validateDestinationRevision(
        value.destinationId,
        value.destinationGeneration,
        value.expectedCheckpointRevision,
        "checkpoint"
      )
      validateDestinationBlock(value.block)
      return
    case "record_retention_gap":
      validateDestinationRevision(
        value.destinationId,
        value.destinationGeneration,
        value.expectedCheckpointRevision,
        "checkpoint"
      )
      validateRetentionGap(value.gap)
      return
    case "supersede_generation":
      if (value.expectedDefinitionRevision === 0n)
        throw new InvalidError("expected definition revision must be nonzero")
      validateMaterializationDestination(value.replacement)
      return
    case "record_repair":
      validateDestinationRevision(
        value.destinationId,
        value.destinationGeneration,
        value.expectedCheckpointRevision,
        "checkpoint"
      )
      validateRepairRecord(value.repair)
  }
}

function validateSupervisorAssertionBinding(value: CheckpointRequestEnvelope): void {
  let required:
    | readonly [SupervisorActorAssertion["claims"]["action"], DestinationId, bigint, bigint]
    | undefined
  switch (value.mutation.kind) {
    case "accept_retention_gap":
      required = [
        "accept_retention_gap",
        value.mutation.destinationId,
        value.mutation.destinationGeneration,
        value.mutation.expectedCheckpointRevision
      ]
      break
    case "supersede_generation":
      required = [
        "supersede_generation",
        value.mutation.replacement.id,
        value.mutation.replacement.generation,
        value.mutation.expectedDefinitionRevision
      ]
      break
    case "record_repair":
      required = [
        "record_repair",
        value.mutation.destinationId,
        value.mutation.destinationGeneration,
        value.mutation.expectedCheckpointRevision
      ]
      break
    case "register_destination":
    case "register_query_route":
    case "remove_query_route":
    case "bind_table":
    case "set_desired_state":
    case "add_partition":
    case "observe_partition_lifecycle":
    case "acquire_lease":
    case "takeover_lease":
    case "renew_lease":
    case "prepare":
    case "complete":
    case "record_block":
    case "clear_block":
    case "record_retention_gap":
      break
  }
  if (required === undefined) {
    if (value.supervisorAssertion !== undefined)
      throw new InvalidError("supervisor assertion is not accepted for this checkpoint mutation")
    return
  }
  const assertion = value.supervisorAssertion
  if (assertion === undefined)
    throw new InvalidError("high-risk checkpoint mutation requires a supervisor assertion")
  validateSupervisorAssertion(assertion)
  const [action, destinationId, destinationGeneration, expectedRevision] = required
  if (
    assertion.claims.requestId.asU128() !== value.requestId.asU128() ||
    assertion.claims.action !== action ||
    assertion.claims.destinationId.asU128() !== destinationId.asU128() ||
    assertion.claims.destinationGeneration !== destinationGeneration ||
    assertion.claims.expectedRevision !== expectedRevision
  )
    throw new InvalidError(
      "supervisor assertion is not bound to this request, action, and destination"
    )
}

export function validateSupervisorAssertion(value: SupervisorActorAssertion): void {
  const claims = value.claims
  if (claims.v !== SUPERVISOR_ASSERTION_VERSION)
    throw new InvalidError("supervisor assertion version is invalid")
  if (
    claims.requestId.asU128() === 0n ||
    claims.deploymentId === 0 ||
    claims.cloudUserId === 0 ||
    claims.destinationId.asU128() === 0n ||
    claims.destinationGeneration === 0n
  )
    throw new InvalidError("supervisor assertion identities must be nonzero")
  if (
    claims.issuedAtMicros === 0n ||
    claims.expiresAtMicros <= claims.issuedAtMicros ||
    claims.expiresAtMicros - claims.issuedAtMicros > MAX_SUPERVISOR_ASSERTION_TTL_MICROS
  )
    throw new InvalidError("supervisor assertion validity window is invalid")
  fixedBytes(value.keyId, SUPERVISOR_ASSERTION_KEY_ID_BYTES, "supervisor assertion key id")
  fixedBytes(
    value.signature,
    SUPERVISOR_ASSERTION_SIGNATURE_BYTES,
    "supervisor assertion signature"
  )
}

function validateDestinationRevision(
  destinationId: DestinationId,
  destinationGeneration: bigint,
  revision: bigint,
  revisionKind: string
): void {
  if (destinationId.asU128() === 0n || destinationGeneration === 0n)
    throw new InvalidError("checkpoint destination identity and generation must be nonzero")
  if (revision === 0n) throw new InvalidError(`expected ${revisionKind} revision must be nonzero`)
}

function validateLeaseRequest(value: {
  readonly destinationId: DestinationId
  readonly destinationGeneration: bigint
  readonly owner: CheckpointOwnerId
  readonly leaseDurationMicros: bigint
}): void {
  if (
    value.destinationId.asU128() === 0n ||
    value.destinationGeneration === 0n ||
    value.owner.asU128() === 0n ||
    value.leaseDurationMicros === 0n ||
    value.leaseDurationMicros > MAX_CHECKPOINT_LEASE_DURATION_MICROS
  )
    throw new InvalidError(
      "lease destination, owner, and generation must be nonzero, and duration must be within the configured cap"
    )
}

function validateBackendBinding(value: BackendBinding): void {
  if (value.resourceId.asU128() === 0n || value.generation === 0n)
    throw new InvalidError("backend binding identity and generation must be nonzero")
}

function validateProjectionRef(value: ProjectionRef): void {
  if (value.id.length === 0 || value.version === 0)
    throw new InvalidError("projection identity and version must be nonzero")
}

function validateLogicalSchemaRef(value: LogicalSchemaRef): void {
  if (value.id.asU128() === 0n || value.version === 0)
    throw new InvalidError("logical schema identity and version must be nonzero")
  fixedBytes(value.fingerprint, 32, "schema fingerprint")
}

function validatePartition(value: PartitionCheckpoint): void {
  validateSourceIncarnation(value.incarnation)
  if (value.startedAtOffset > value.nextOffset)
    throw new InvalidError("partition start offset must not exceed its next offset")
}

function validateRange(value: SourceOffsetRange): void {
  validateSourceIncarnation(value.incarnation)
  if (value.start >= value.endExclusive) throw new InvalidError("source range is empty or inverted")
}

function validateRanges(value: readonly SourceOffsetRange[]): void {
  if (value.length < 1 || value.length > MAX_CHECKPOINT_PARTITIONS)
    throw new InvalidError("source range partition count is invalid")
  let previousPartition: number | undefined
  let namespace: string | undefined
  for (const range of value) {
    validateRange(range)
    const current = sourceNamespace(range.incarnation)
    if (namespace !== undefined && namespace !== current)
      throw new InvalidError("source ranges must share one cluster, stream, and topic")
    if (previousPartition !== undefined && previousPartition >= range.incarnation.partitionId)
      throw new InvalidError("source ranges must be ordered by ascending partition id")
    namespace = current
    previousPartition = range.incarnation.partitionId
  }
}

function validatePreparedTable(value: PreparedTableRequirements): void {
  fixedBytes(value.tableUuid, 16, "table UUID")
  validateObjectIdentity(value.baseMetadataIdentity)
  if (
    (value.baseSnapshotId !== undefined && value.baseSnapshotId <= 0n) ||
    !Number.isInteger(value.schemaId) ||
    value.schemaId < 0 ||
    !Number.isInteger(value.partitionSpecId) ||
    value.partitionSpecId < 0
  )
    throw new InvalidError("prepared table snapshot, schema, or partition spec is invalid")
  const requirements = value.commitRequirements
  if (requirements.length !== 5)
    throw new InvalidError("Iceberg commit requirements are not canonical")
  const [table, metadata, snapshot, schema, spec] = requirements
  if (
    table?.kind !== "assert_table_uuid" ||
    !equalBytes(table.tableUuid, value.tableUuid) ||
    metadata?.kind !== "assert_metadata_identity" ||
    metadata.identity !== value.baseMetadataIdentity ||
    snapshot?.kind !== "assert_current_snapshot" ||
    snapshot.snapshotId !== value.baseSnapshotId ||
    schema?.kind !== "assert_current_schema" ||
    schema.schemaId !== value.schemaId ||
    spec?.kind !== "assert_default_partition_spec" ||
    spec.partitionSpecId !== value.partitionSpecId
  )
    throw new InvalidError(
      "Iceberg commit requirements must contain the exact frozen table preconditions in canonical order"
    )
}

function validateAttemptObject(value: AttemptObject): void {
  validateObjectIdentity(value.identity)
  if (value.sizeBytes === 0n || value.rowCount === 0n)
    throw new InvalidError("attempt object size and row count must be nonzero")
  fixedBytes(value.sha256, 32, "attempt object digest")
  if (value.columns.length > MAX_LOGICAL_SCHEMA_FIELDS)
    throw new InvalidError("attempt object column metric count exceeds the schema field cap")
  const fields = new Set<number>()
  for (const column of value.columns) {
    validateAttemptColumnMetrics(column)
    if (fields.has(column.fieldId)) throw new InvalidError("attempt object repeats a column metric")
    fields.add(column.fieldId)
  }
}

function validateAttemptColumnMetrics(value: AttemptColumnMetrics): void {
  if (
    value.fieldId === 0 ||
    value.nullCount > value.valueCount ||
    value.nanCount > value.valueCount - value.nullCount
  )
    throw new InvalidError("attempt column metrics are invalid")
  if (value.lowerBound !== undefined) validateTypedValue(value.lowerBound)
  if (value.upperBound !== undefined) validateTypedValue(value.upperBound)
  if ((value.lowerBound === undefined) !== (value.upperBound === undefined))
    throw new InvalidError("attempt column bounds must be both present or both absent")
}

function validateCredentialGenerations(value: readonly CredentialGeneration[]): void {
  if (value.length < 1 || value.length > MAX_CREDENTIAL_GENERATIONS)
    throw new InvalidError("credential generation role count is invalid")
  const roles = new Set<string>()
  for (const credential of value) {
    const bytes = new TextEncoder().encode(credential.role).length
    if (
      bytes < 1 ||
      bytes > 64 ||
      !/^[a-z0-9_-]+$/.test(credential.role) ||
      credential.generation === 0n
    )
      throw new InvalidError("credential role or generation is invalid")
    if (roles.has(credential.role)) throw new InvalidError("credential role appears more than once")
    roles.add(credential.role)
  }
}

function validatePreparedAttempt(value: PreparedAttempt): void {
  if (
    value.id.asU128() === 0n ||
    value.destinationId.asU128() === 0n ||
    value.destinationGeneration === 0n
  )
    throw new InvalidError("prepared attempt identity and destination must be nonzero")
  validateBackendBinding(value.backend)
  if (value.owner.asU128() === 0n || value.epoch === 0n)
    throw new InvalidError("prepared attempt owner and epoch must be nonzero")
  if (value.createdAtCheckpointRevision === 0n)
    throw new InvalidError("prepared attempt creation revision must be nonzero")
  validatePreparedTable(value.table)
  fixedBytes(value.schemaFingerprint, 32, "schema fingerprint")
  validateProjectionRef(value.projection)
  fixedBytes(value.resultingBoundaryDigest, 32, "resulting boundary digest")
  fixedBytes(value.manifestDigest, 32, "manifest digest")
  validateRanges(value.ranges)
  if (value.resultingBoundary.length !== value.ranges.length)
    throw new InvalidError("prepared attempt boundary must cover every source range exactly once")
  for (const [index, partition] of value.resultingBoundary.entries()) {
    validatePartition(partition)
    const range = value.ranges[index]
    if (
      range === undefined ||
      !equalIncarnation(partition.incarnation, range.incarnation) ||
      partition.nextOffset !== range.endExclusive ||
      partition.lifecycle !== "active"
    )
      throw new InvalidError("prepared attempt boundary does not match its source ranges")
  }
  validateObjectIdentity(value.manifestIdentity)
  if (value.objects.length < 1 || value.objects.length > MAX_ATTEMPT_OBJECTS)
    throw new InvalidError("prepared attempt object count is invalid")
  const identities = new Set<string>()
  for (const object of value.objects) {
    validateAttemptObject(object)
    if (identities.has(object.identity))
      throw new InvalidError("prepared attempt repeats an object identity")
    identities.add(object.identity)
  }
  validateCredentialGenerations(value.credentialGenerations)
}

function validatePreparedAttemptSummary(value: PreparedAttemptSummary): void {
  if (value.id.asU128() === 0n || value.owner.asU128() === 0n || value.epoch === 0n)
    throw new InvalidError("prepared attempt summary identity must be nonzero")
  validatePreparedTable(value.table)
  fixedBytes(value.schemaFingerprint, 32, "schema fingerprint")
  validateProjectionRef(value.projection)
  validateObjectIdentity(value.manifestIdentity)
  fixedBytes(value.manifestDigest, 32, "manifest digest")
  fixedBytes(value.resultingBoundaryDigest, 32, "resulting boundary digest")
  validateRanges(value.ranges)
  if (value.objectCount < 1 || value.objectCount > MAX_ATTEMPT_OBJECTS)
    throw new InvalidError("prepared attempt summary object count is invalid")
  validateCredentialGenerations(value.credentialGenerations)
}

function validateCompletedAttempt(value: CompletedAttempt): void {
  if (value.id.asU128() === 0n) throw new InvalidError("completed attempt id must be nonzero")
  fixedBytes(value.tableUuid, 16, "completed attempt table UUID")
  if (value.snapshotId <= 0n)
    throw new InvalidError("completed attempt snapshot id must be positive")
  fixedBytes(value.manifestDigest, 32, "manifest digest")
  fixedBytes(value.resultingBoundaryDigest, 32, "resulting boundary digest")
  if (value.completionRevision === 0n)
    throw new InvalidError("completed attempt revision must be nonzero")
  validateRanges(value.ranges)
}

function validateRetentionGap(value: RetentionGap): void {
  validateSourceIncarnation(value.incarnation)
  if (value.requiredNextOffset >= value.retainedStart)
    throw new InvalidError("retention gap requires next offset below retained start")
}

function validateDestinationBlock(value: DestinationBlock): void {
  validateBoundedText("checkpoint error message", value.message, MAX_CHECKPOINT_ERROR_BYTES)
  if (value.incarnation !== undefined) validateSourceIncarnation(value.incarnation)
}

function validateRepairRecord(value: RepairRecord): void {
  validateBoundedText("repair detail", value.detail, MAX_REPAIR_DETAIL_BYTES)
}

function validateLease(value: CheckpointOwnerLease): void {
  if (
    value.owner.asU128() === 0n ||
    value.epoch === 0n ||
    value.sequence === 0n ||
    value.deadlineMicros === 0n
  )
    throw new InvalidError("checkpoint lease identity and counters must be nonzero")
}

function validateObjectIdentity(value: string): void {
  const bytes = new TextEncoder().encode(value).length
  if (bytes < 1 || bytes > MAX_MANIFEST_IDENTITY_BYTES)
    throw new InvalidError("object identity length is invalid")
  if (value.includes("?") || value.includes("#") || value.includes("@") || value.includes("//"))
    throw new InvalidError("object identity must be canonical, provider-relative, and secret-free")
}

function validateBoundedText(label: string, value: string, cap: number): void {
  const bytes = new TextEncoder().encode(value).length
  if (bytes < 1 || bytes > cap) throw new InvalidError(`${label} length is invalid`)
}

function fixedBytes(value: Uint8Array, expected: number, label: string): void {
  if (value.length !== expected)
    throw new InvalidError(`${label} must contain ${String(expected)} bytes`)
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((byte, index) => byte === right[index])
}

function sourceNamespace(value: SourceIncarnation): string {
  return `${value.cluster.asU128().toString()}:${String(value.streamId)}:${String(value.topicId)}`
}

function equalIncarnation(left: SourceIncarnation, right: SourceIncarnation): boolean {
  return (
    left.cluster.asU128() === right.cluster.asU128() &&
    left.streamId === right.streamId &&
    left.topicId === right.topicId &&
    left.partitionId === right.partitionId &&
    left.partitionCreatedRevision === right.partitionCreatedRevision
  )
}

function decodeError(value: unknown, context: string): CheckpointError {
  if (typeof value === "string") {
    const kind = value.replaceAll(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase()
    if (kind === "not_found" || kind === "lease_lost" || kind === "unauthorized") return { kind }
    throw new CodecError(`unknown checkpoint error \`${value}\``, context, "error")
  }
  const [tag, body] = singleVariantTag(value, context)
  const kind = tag.replaceAll(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase()
  if (kind === "invalid" || kind === "unavailable")
    return { kind, message: expectString(body, context) }
  const map = expectMap(body, context)
  if (kind === "conflict")
    return { kind, observedRevision: field.requiredU64(map, "observed_revision", context) }
  if (kind === "version")
    return {
      kind,
      expected: field.requiredU32(map, "expected", context),
      got: field.requiredU32(map, "got", context)
    }
  throw new CodecError(`unknown checkpoint error \`${tag}\``, context, "error")
}

function encodeError(value: CheckpointError): unknown {
  if (value.kind === "invalid") return new Map([["Invalid", value.message]])
  if (value.kind === "unavailable") return new Map([["Unavailable", value.message]])
  if (value.kind === "not_found") return "NotFound"
  if (value.kind === "lease_lost") return "LeaseLost"
  if (value.kind === "unauthorized") return "Unauthorized"
  if (value.kind === "conflict") {
    return new Map([["Conflict", new Map([["observed_revision", value.observedRevision]])]])
  }
  return new Map([
    [
      "Version",
      new Map<string, unknown>([
        ["expected", value.expected],
        ["got", value.got]
      ])
    ]
  ])
}
export function decodeCheckpointMutationResult(
  map: CborMap,
  context: string
): CheckpointMutationResult {
  const kind = field.requiredString(map, "kind", context)
  const requestId = CheckpointRequestId.fromBytes(field.requiredBytes(map, "request_id", context))
  const globalStateRevision = field.requiredU64(map, "global_state_revision", context)
  const definitionRevision = field.requiredU64(map, "definition_revision", context)
  if (kind === "destination") {
    const lease = field.optionalMap(map, "lease", context)
    return {
      kind,
      requestId,
      destinationId: DestinationId.fromBytes(field.requiredBytes(map, "destination_id", context)),
      destinationGeneration: field.requiredU64(map, "destination_generation", context),
      globalStateRevision,
      definitionRevision,
      checkpointRevision: field.requiredU64(map, "checkpoint_revision", context),
      ...(lease === undefined ? {} : { lease: decodeLease(lease, context) })
    }
  }
  if (kind === "query_route") {
    return {
      kind,
      requestId,
      routeId: QueryRouteId.fromBytes(field.requiredBytes(map, "route_id", context)),
      routeGeneration: field.requiredU64(map, "route_generation", context),
      globalStateRevision,
      definitionRevision
    }
  }
  throw new CodecError(`unknown checkpoint mutation result kind \`${kind}\``, context, "kind")
}

export function encodeCheckpointMutationResult(
  value: CheckpointMutationResult
): Map<string, unknown> {
  validateCheckpointMutationResult(value)
  const map = mapOf([
    ["kind", value.kind],
    ["request_id", value.requestId.toBytes()]
  ])
  if (value.kind === "destination") {
    map.set("destination_id", value.destinationId.toBytes())
    map.set("destination_generation", value.destinationGeneration)
  } else {
    map.set("route_id", value.routeId.toBytes())
    map.set("route_generation", value.routeGeneration)
  }
  map.set("global_state_revision", value.globalStateRevision)
  map.set("definition_revision", value.definitionRevision)
  if (value.kind === "destination") {
    map.set("checkpoint_revision", value.checkpointRevision)
    optional(map, "lease", value.lease, encodeLease)
  }
  return map
}

export function encodeCheckpointReplyFrame(reply: CheckpointReply): Uint8Array {
  const value =
    reply.kind === "ok"
      ? new Map([["Ok", encodeCheckpointMutationResult(reply.result)]])
      : new Map([["Err", encodeError(reply.error)]])
  return encodeNamed(value)
}
export function decodeCheckpointReply(bytes: Uint8Array): CheckpointReply {
  const context = "CheckpointReply"
  const [tag, body] = singleVariantTag(decodeOne(bytes, context), context)
  if (tag === "Ok") {
    const result = decodeCheckpointMutationResult(expectMap(body, context), context)
    validateCheckpointMutationResult(result)
    return { kind: "ok", result }
  }
  if (tag === "Err") return { kind: "err", error: decodeError(body, context) }
  throw new CodecError(`unknown checkpoint reply \`${tag}\``, context, "reply")
}

export function decodeRetentionGap(map: CborMap, context: string): RetentionGap {
  return {
    incarnation: decodeSourceIncarnation(field.requiredMap(map, "incarnation", context), context),
    requiredNextOffset: field.requiredU64(map, "required_next_offset", context),
    retainedStart: field.requiredU64(map, "retained_start", context)
  }
}
export function decodeDestinationBlock(map: CborMap, context: string): DestinationBlock {
  const incarnation = field.optionalMap(map, "incarnation", context)
  const offset = field.optionalU64(map, "offset", context)
  const rowOrdinal = field.optionalU32(map, "row_ordinal", context)
  return {
    code: enumWord(map, "code", context, DESTINATION_BLOCK_CODES),
    message: field.requiredString(map, "message", context),
    ...(incarnation === undefined
      ? {}
      : { incarnation: decodeSourceIncarnation(incarnation, context) }),
    ...(offset === undefined ? {} : { offset }),
    ...(rowOrdinal === undefined ? {} : { rowOrdinal })
  }
}
function decodeRange(map: CborMap, context: string): SourceOffsetRange {
  return {
    incarnation: decodeSourceIncarnation(field.requiredMap(map, "incarnation", context), context),
    start: field.requiredU64(map, "start", context),
    endExclusive: field.requiredU64(map, "end_exclusive", context)
  }
}
function decodeRequirement(map: CborMap, context: string): IcebergCommitRequirement {
  const kind = enumWord(map, "kind", context, [
    "assert_table_uuid",
    "assert_metadata_identity",
    "assert_current_snapshot",
    "assert_current_schema",
    "assert_default_partition_spec"
  ])
  if (kind === "assert_table_uuid")
    return { kind, tableUuid: field.requiredBytes(map, "table_uuid", context) }
  if (kind === "assert_metadata_identity")
    return { kind, identity: field.requiredString(map, "identity", context) }
  if (kind === "assert_current_snapshot") {
    const snapshotId = field.optionalI64(map, "snapshot_id", context)
    return { kind, ...(snapshotId === undefined ? {} : { snapshotId }) }
  }
  if (kind === "assert_current_schema")
    return { kind, schemaId: field.requiredI32(map, "schema_id", context) }
  return { kind, partitionSpecId: field.requiredI32(map, "partition_spec_id", context) }
}
function decodeTable(map: CborMap, context: string): PreparedTableRequirements {
  const baseSnapshotId = field.optionalI64(map, "base_snapshot_id", context)
  return {
    tableUuid: field.requiredBytes(map, "table_uuid", context),
    baseMetadataIdentity: field.requiredString(map, "base_metadata_identity", context),
    ...(baseSnapshotId === undefined ? {} : { baseSnapshotId }),
    schemaId: field.requiredI32(map, "schema_id", context),
    partitionSpecId: field.requiredI32(map, "partition_spec_id", context),
    commitRequirements: field.requiredArray(map, "commit_requirements", context, (item, index) =>
      decodeRequirement(
        expectMap(item, `${context}.commit_requirements[${String(index)}]`),
        `${context}.commit_requirements[${String(index)}]`
      )
    )
  }
}
function decodeCredential(map: CborMap, context: string): CredentialGeneration {
  return {
    role: field.requiredString(map, "role", context),
    generation: field.requiredU64(map, "generation", context)
  }
}
export function decodePreparedAttemptSummary(
  map: CborMap,
  context: string
): PreparedAttemptSummary {
  return {
    id: PreparedAttemptId.fromBytes(field.requiredBytes(map, "id", context)),
    owner: CheckpointOwnerId.fromBytes(field.requiredBytes(map, "owner", context)),
    epoch: field.requiredU64(map, "epoch", context),
    table: decodeTable(field.requiredMap(map, "table", context), `${context}.table`),
    schemaFingerprint: field.requiredBytes(map, "schema_fingerprint", context),
    projection: decodeProjectionRef(
      field.requiredMap(map, "projection", context),
      `${context}.projection`
    ),
    manifestIdentity: field.requiredString(map, "manifest_identity", context),
    manifestDigest: field.requiredBytes(map, "manifest_digest", context),
    resultingBoundaryDigest: field.requiredBytes(map, "resulting_boundary_digest", context),
    ranges: field.requiredArray(map, "ranges", context, (item, index) =>
      decodeRange(
        expectMap(item, `${context}.ranges[${String(index)}]`),
        `${context}.ranges[${String(index)}]`
      )
    ),
    objectCount: field.requiredU32(map, "object_count", context),
    credentialGenerations: field.requiredArray(
      map,
      "credential_generations",
      context,
      (item, index) =>
        decodeCredential(
          expectMap(item, `${context}.credential_generations[${String(index)}]`),
          `${context}.credential_generations[${String(index)}]`
        )
    )
  }
}
function decodeCompletion(map: CborMap, context: string): CompletedAttempt {
  return {
    id: PreparedAttemptId.fromBytes(field.requiredBytes(map, "id", context)),
    tableUuid: field.requiredBytes(map, "table_uuid", context),
    snapshotId: field.requiredI64(map, "snapshot_id", context),
    manifestDigest: field.requiredBytes(map, "manifest_digest", context),
    resultingBoundaryDigest: field.requiredBytes(map, "resulting_boundary_digest", context),
    ranges: field.requiredArray(map, "ranges", context, (item, index) =>
      decodeRange(
        expectMap(item, `${context}.ranges[${String(index)}]`),
        `${context}.ranges[${String(index)}]`
      )
    ),
    completionRevision: field.requiredU64(map, "completion_revision", context)
  }
}
function decodeRepair(map: CborMap, context: string): RepairRecord {
  return {
    action: enumWord(map, "action", context, [
      "reconciled_prepared_attempt",
      "accepted_retention_gap",
      "cleared_retryable_block",
      "superseded_generation"
    ]),
    detail: field.requiredString(map, "detail", context)
  }
}
export function encodeDestinationCheckpointStatus(
  value: DestinationCheckpointStatus
): Map<string, unknown> {
  const map = mapOf([
    ["destination_id", value.destinationId.toBytes()],
    ["destination_generation", value.destinationGeneration],
    ["backend", encodeBackendBinding(value.backend)],
    ["schema", encodeLogicalSchemaRef(value.schema)],
    ["projection", encodeProjectionRef(value.projection)],
    ["global_state_revision", value.globalStateRevision],
    ["definition_revision", value.definitionRevision],
    ["checkpoint_revision", value.checkpointRevision],
    ["desired_state", value.desiredState],
    ["effective_state", value.effectiveState]
  ])
  optional(map, "table_uuid", value.tableUuid)
  optional(map, "owner", value.owner, encodeLease)
  map.set("partitions", value.partitions.map(encodePartition))
  optional(map, "prepared_attempt", value.preparedAttempt, encodePreparedAttemptSummary)
  optional(map, "last_completion", value.lastCompletion, encodeCompletion)
  optional(map, "retention_gap", value.retentionGap, encodeRetentionGap)
  optional(map, "block", value.block, encodeDestinationBlock)
  optional(map, "last_repair", value.lastRepair, encodeRepair)
  map.set("consistency", value.consistency)
  return map
}

export function decodeDestinationCheckpointStatus(
  map: CborMap,
  context: string
): DestinationCheckpointStatus {
  const tableUuid = field.optionalBytes(map, "table_uuid", context)
  const owner = field.optionalMap(map, "owner", context)
  const prepared = field.optionalMap(map, "prepared_attempt", context)
  const completion = field.optionalMap(map, "last_completion", context)
  const gap = field.optionalMap(map, "retention_gap", context)
  const block = field.optionalMap(map, "block", context)
  const repair = field.optionalMap(map, "last_repair", context)
  const value: DestinationCheckpointStatus = {
    destinationId: DestinationId.fromBytes(field.requiredBytes(map, "destination_id", context)),
    destinationGeneration: field.requiredU64(map, "destination_generation", context),
    backend: decodeBackendBinding(field.requiredMap(map, "backend", context), context),
    schema: decodeLogicalSchemaRef(field.requiredMap(map, "schema", context), context),
    projection: decodeProjectionRef(field.requiredMap(map, "projection", context), context),
    globalStateRevision: field.requiredU64(map, "global_state_revision", context),
    definitionRevision: field.requiredU64(map, "definition_revision", context),
    checkpointRevision: field.requiredU64(map, "checkpoint_revision", context),
    desiredState: enumWord(map, "desired_state", context, ["disabled", "enabled"]),
    effectiveState: enumWord(map, "effective_state", context, [
      "disabled",
      "waiting_for_backend",
      "ready",
      "running",
      "blocked"
    ]),
    ...(tableUuid === undefined ? {} : { tableUuid }),
    ...(owner === undefined ? {} : { owner: decodeLease(owner, context) }),
    partitions: field.requiredArray(map, "partitions", context, (item, index) =>
      decodePartition(expectMap(item, `${context}.partitions[${String(index)}]`), context)
    ),
    ...(prepared === undefined
      ? {}
      : {
          preparedAttempt: decodePreparedAttemptSummary(prepared, `${context}.prepared_attempt`)
        }),
    ...(completion === undefined
      ? {}
      : { lastCompletion: decodeCompletion(completion, `${context}.last_completion`) }),
    ...(gap === undefined ? {} : { retentionGap: decodeRetentionGap(gap, context) }),
    ...(block === undefined ? {} : { block: decodeDestinationBlock(block, context) }),
    ...(repair === undefined ? {} : { lastRepair: decodeRepair(repair, `${context}.last_repair`) }),
    consistency: enumWord(map, "consistency", context, ["linearizable", "potentially_stale"])
  }
  validateDestinationCheckpointStatus(value)
  return value
}
function decodeView(map: CborMap, context: string): DestinationCheckpointView {
  return {
    destination: decodeMaterializationDestination(
      field.requiredMap(map, "destination", context),
      context
    ),
    status: decodeDestinationCheckpointStatus(field.requiredMap(map, "status", context), context)
  }
}

export function validateCheckpointMutationResult(value: CheckpointMutationResult): void {
  if (
    value.requestId.asU128() === 0n ||
    value.globalStateRevision === 0n ||
    value.definitionRevision === 0n
  ) {
    throw new InvalidError(
      "checkpoint mutation result request identity, global revision, and definition revision must be nonzero"
    )
  }
  if (value.kind === "destination") {
    if (value.destinationId.asU128() === 0n || value.destinationGeneration === 0n) {
      throw new InvalidError(
        "checkpoint destination result identity and generation must be nonzero"
      )
    }
    if (value.lease !== undefined) validateLease(value.lease)
    return
  }
  if (value.routeId.asU128() === 0n || value.routeGeneration === 0n) {
    throw new InvalidError("checkpoint query route result identity and generation must be nonzero")
  }
}

export function validateDestinationCheckpointStatus(value: DestinationCheckpointStatus): void {
  if (value.destinationId.asU128() === 0n || value.destinationGeneration === 0n)
    throw new InvalidError("checkpoint status destination identity must be nonzero")
  validateBackendBinding(value.backend)
  validateLogicalSchemaRef(value.schema)
  validateProjectionRef(value.projection)
  if (value.globalStateRevision === 0n || value.definitionRevision === 0n)
    throw new InvalidError("checkpoint status global and definition revisions must be nonzero")
  if (value.partitions.length > MAX_CHECKPOINT_PARTITIONS)
    throw new InvalidError("checkpoint status partition count exceeds its bound")
  const partitions = new Set<number>()
  let namespace: string | undefined
  for (const partition of value.partitions) {
    validatePartition(partition)
    const current = sourceNamespace(partition.incarnation)
    if (namespace !== undefined && namespace !== current)
      throw new InvalidError("checkpoint partitions must share one cluster, stream, and topic")
    if (partitions.has(partition.incarnation.partitionId))
      throw new InvalidError("checkpoint status repeats a partition")
    namespace = current
    partitions.add(partition.incarnation.partitionId)
  }
  if (value.owner !== undefined) validateLease(value.owner)
  if (value.tableUuid !== undefined) fixedBytes(value.tableUuid, 16, "table UUID")
  if (value.preparedAttempt !== undefined) validatePreparedAttemptSummary(value.preparedAttempt)
  if (value.lastCompletion !== undefined) validateCompletedAttempt(value.lastCompletion)
  if (value.retentionGap !== undefined) validateRetentionGap(value.retentionGap)
  if (value.block !== undefined) validateDestinationBlock(value.block)
  if (value.lastRepair !== undefined) validateRepairRecord(value.lastRepair)
}

export function validateDestinationCheckpointView(value: DestinationCheckpointView): void {
  validateMaterializationDestination(value.destination)
  validateDestinationCheckpointStatus(value.status)
  if (
    value.destination.id.asU128() !== value.status.destinationId.asU128() ||
    value.destination.generation !== value.status.destinationGeneration ||
    value.destination.definitionRevision !== value.status.definitionRevision ||
    value.destination.backend.resourceId.asU128() !== value.status.backend.resourceId.asU128() ||
    value.destination.backend.generation !== value.status.backend.generation ||
    value.destination.schema.id.asU128() !== value.status.schema.id.asU128() ||
    value.destination.schema.version !== value.status.schema.version ||
    !equalBytes(value.destination.schema.fingerprint, value.status.schema.fingerprint) ||
    value.destination.projection.id !== value.status.projection.id ||
    value.destination.projection.version !== value.status.projection.version
  )
    throw new InvalidError(
      "destination declaration and checkpoint status do not describe the same generation"
    )
}

export function validateCheckpointReadReply(value: CheckpointReadReply): void {
  if (value.kind === "err") return
  if (value.kind === "destination") {
    if (value.destination !== undefined) validateDestinationCheckpointView(value.destination)
    return
  }
  if (value.kind === "destinations") {
    if (value.page.globalStateRevision === 0n || value.page.destinations.length > MAX_PAGE_SIZE)
      throw new InvalidError("destination page metadata is invalid")
    const ids = new Set<string>()
    for (const destination of value.page.destinations) {
      validateDestinationCheckpointView(destination)
      const id = destination.destination.id.asU128().toString()
      if (destination.status.globalStateRevision > value.page.globalStateRevision || ids.has(id))
        throw new InvalidError("destination page contains duplicate or future state")
      ids.add(id)
    }
    return
  }
  if (value.page.globalStateRevision === 0n || value.page.routes.length > MAX_PAGE_SIZE)
    throw new InvalidError("query route page metadata is invalid")
  const ids = new Set<string>()
  for (const route of value.page.routes) {
    validateQueryRoute(route)
    const id = route.id.asU128().toString()
    if (ids.has(id)) throw new InvalidError("query route page repeats a route id")
    ids.add(id)
  }
}

function validCheckpointReadReply(value: CheckpointReadReply): CheckpointReadReply {
  validateCheckpointReadReply(value)
  return value
}

export function decodeCheckpointReadReply(bytes: Uint8Array): CheckpointReadReply {
  const context = "CheckpointReadReply"
  const [tag, body] = singleVariantTag(decodeOne(bytes, context), context)
  if (tag === "Destination") {
    return validCheckpointReadReply(
      body === null
        ? { kind: "destination" }
        : { kind: "destination", destination: decodeView(expectMap(body, context), context) }
    )
  }
  if (tag === "Destinations") {
    const map = expectMap(body, context)
    const next = field.optionalBytes(map, "next_after", context)
    return validCheckpointReadReply({
      kind: "destinations",
      page: {
        destinations: field.requiredArray(map, "destinations", context, (item, index) =>
          decodeView(expectMap(item, `${context}.destinations[${String(index)}]`), context)
        ),
        ...(next === undefined ? {} : { nextAfter: DestinationId.fromBytes(next) }),
        globalStateRevision: field.requiredU64(map, "global_state_revision", context),
        consistency: enumWord(map, "consistency", context, ["linearizable", "potentially_stale"])
      }
    })
  }
  if (tag === "QueryRoutes") {
    const map = expectMap(body, context)
    const next = field.optionalBytes(map, "next_after", context)
    return validCheckpointReadReply({
      kind: "query_routes",
      page: {
        routes: field.requiredArray(map, "routes", context, (item, index) =>
          decodeQueryRoute(expectMap(item, `${context}.routes[${String(index)}]`), context)
        ),
        ...(next === undefined ? {} : { nextAfter: QueryRouteId.fromBytes(next) }),
        globalStateRevision: field.requiredU64(map, "global_state_revision", context),
        consistency: enumWord(map, "consistency", context, ["linearizable", "potentially_stale"])
      }
    })
  }
  if (tag === "Err") return { kind: "err", error: decodeError(body, context) }
  throw new CodecError(`unknown checkpoint read reply \`${tag}\``, context, "reply")
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
export function encodeDestinationGetRequest(value: DestinationGetRequest): Map<string, unknown> {
  validateDestinationGetRequest(value)
  return mapOf([
    ["v", value.v],
    ["destination_id", value.destinationId.toBytes()],
    ["consistency", value.consistency]
  ])
}
export function encodeDestinationListRequest(value: DestinationListRequest): Map<string, unknown> {
  validateDestinationListRequest(value)
  const filter = new Map<string, unknown>()
  optional(filter, "source_stream", value.filter.sourceStream)
  optional(filter, "source_topic", value.filter.sourceTopic)
  optional(filter, "name_contains", value.filter.nameContains)
  const map = mapOf([
    ["v", value.v],
    ["filter", filter]
  ])
  optional(map, "after", value.after, (item) => item.toBytes())
  map.set("limit", value.limit)
  map.set("consistency", value.consistency)
  return map
}
export function encodeQueryRouteListRequest(value: QueryRouteListRequest): Map<string, unknown> {
  validateQueryRouteListRequest(value)
  const map = mapOf([["v", value.v]])
  optional(map, "name_contains", value.nameContains)
  optional(map, "after", value.after, (item) => item.toBytes())
  map.set("limit", value.limit)
  map.set("consistency", value.consistency)
  return map
}

export function validateDestinationGetRequest(value: DestinationGetRequest): void {
  validateCheckpointReadVersion(value.v)
  if (value.destinationId.asU128() === 0n) throw new InvalidError("destination id must be nonzero")
}

export function validateDestinationListRequest(value: DestinationListRequest): void {
  validateCheckpointReadVersion(value.v)
  validateReadLimit(value.limit)
  validateOptionalFilter("source stream", value.filter.sourceStream)
  validateOptionalFilter("source topic", value.filter.sourceTopic)
  validateOptionalFilter("destination name", value.filter.nameContains)
}

export function validateQueryRouteListRequest(value: QueryRouteListRequest): void {
  validateCheckpointReadVersion(value.v)
  validateReadLimit(value.limit)
  validateOptionalFilter("query route name", value.nameContains)
}

function validateCheckpointReadVersion(value: number): void {
  if (value !== CHECKPOINT_OP_VERSION) throw new InvalidError("checkpoint read version is invalid")
}

function validateReadLimit(value: number): void {
  if (!Number.isInteger(value) || value < 1 || value > MAX_PAGE_SIZE)
    throw new InvalidError("checkpoint read limit is outside its bound")
}

function validateOptionalFilter(label: string, value: string | undefined): void {
  if (value === undefined) return
  const bytes = new TextEncoder().encode(value).length
  if (bytes < 1 || bytes > 255 || hasControlCharacter(value))
    throw new InvalidError(`${label} filter is invalid`)
}

function hasControlCharacter(value: string): boolean {
  for (const character of value) {
    const code = character.codePointAt(0) ?? 0
    if (code < 32 || code === 127) return true
  }
  return false
}

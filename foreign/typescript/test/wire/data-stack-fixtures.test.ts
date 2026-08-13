import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  type ArrowIpcRejectionCode,
  type ArrowTimestampUnit,
  decodeArrowIpcMessageMetadata,
  decodeArrowIpcPolicy,
  encodeArrowIpcMessageMetadata,
  encodeArrowIpcPolicy
} from "../../src/wire/arrow.js"
import {
  type CheckpointReadConsistency,
  type DestinationBlockCode,
  type DestinationEffectiveState,
  type PartitionLifecycleState,
  type RepairAction,
  decodeCheckpointRequestFrame,
  decodeDestinationCheckpointStatus,
  decodePublicCheckpointMutation,
  encodeCheckpointRequest,
  encodeCheckpointRequestFrame,
  encodeDestinationCheckpointStatus,
  encodePublicCheckpointMutation,
  validateCheckpointRequest,
  validateDestinationCheckpointStatus
} from "../../src/wire/checkpoint.js"
import { decodeOne, encodeNamed, encodeOne, expectMap } from "../../src/wire/cbor.js"
import {
  type DestinationDesiredState,
  type DestinationErrorPolicy,
  type FileFormat,
  type NewPartitionPolicy,
  type RecreatedPartitionPolicy,
  type TableFormat,
  decodeMaterializationDestination,
  decodeQueryRoute,
  decodeStartPolicy,
  encodeMaterializationDestination,
  encodeQueryRoute,
  encodeStartPolicy
} from "../../src/wire/destination.js"
import type {
  BackendDesiredState,
  BackendMode,
  BackendObservedState,
  BackendReadinessCode,
  QueryPagingCapability,
  TimeTravelCapability
} from "../../src/wire/hello.js"
import type { OperationState } from "../../src/wire/http.js"
import {
  type AggFunc,
  type BoundaryRelation,
  type CmpOp,
  type Consistency,
  type Dir,
  type QueryErrorCode,
  type QueryExecutionState,
  type SqlDialect,
  decodeQueryCancelEnvelopeFrame,
  decodeQueryError,
  decodeQueryPageEnvelopeFrame,
  decodeQueryStatusEnvelopeFrame,
  decodeQueryStatusReplyFrame,
  decodeQueryTarget,
  encodeQueryCancelEnvelopeFrame,
  encodeQueryError,
  encodeQueryPageEnvelopeFrame,
  encodeQueryStatusEnvelopeFrame,
  encodeQueryStatusReplyFrame,
  encodeQueryTarget
} from "../../src/wire/query.js"
import {
  type LogicalTypeKind,
  decodeLogicalSchema,
  decodeLogicalType,
  decodeTypedValue,
  encodeLogicalSchema,
  encodeLogicalType,
  encodeTypedValue,
  typedValueDiagnosticText
} from "../../src/wire/schema.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function fixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

function assertSame(actual: Uint8Array, expected: Uint8Array): void {
  assert.deepEqual(Buffer.from(actual), Buffer.from(expected))
}

void test("every data stack string discriminant matches the Rust fixture", async () => {
  const bytes = await fixture("data_stack_string_discriminants.bin")
  const map = expectMap(decodeOne(bytes, "DataStackStringDiscriminants"), "discriminants")
  const expected = {
    arrow_timestamp_units: ["microsecond"] as const satisfies readonly ArrowTimestampUnit[],
    arrow_rejections: [
      "file_format",
      "missing_schema",
      "missing_dictionary",
      "dictionary_delta",
      "dictionary_replacement",
      "union",
      "extension_type",
      "timestamp_unit",
      "decimal_width",
      "schema_fingerprint",
      "field_limit",
      "batch_limit",
      "row_limit",
      "byte_limit",
      "malformed_stream"
    ] as const satisfies readonly ArrowIpcRejectionCode[],
    file_formats: ["parquet"] as const satisfies readonly FileFormat[],
    table_formats: ["iceberg_v2"] as const satisfies readonly TableFormat[],
    new_partition_policies: [
      "beginning",
      "captured_latest",
      "reject"
    ] as const satisfies readonly NewPartitionPolicy[],
    recreated_partition_policies: ["reject"] as const satisfies readonly RecreatedPartitionPolicy[],
    destination_error_policies: ["block"] as const satisfies readonly DestinationErrorPolicy[],
    destination_desired_states: [
      "disabled",
      "enabled"
    ] as const satisfies readonly DestinationDesiredState[],
    partition_lifecycle_states: [
      "active",
      "removed",
      "recreated"
    ] as const satisfies readonly PartitionLifecycleState[],
    destination_block_codes: [
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
    ] as const satisfies readonly DestinationBlockCode[],
    checkpoint_read_consistency: [
      "linearizable",
      "potentially_stale"
    ] as const satisfies readonly CheckpointReadConsistency[],
    destination_effective_states: [
      "disabled",
      "waiting_for_backend",
      "ready",
      "running",
      "blocked"
    ] as const satisfies readonly DestinationEffectiveState[],
    partition_lifecycle_changes: ["removed", "recreated"],
    repair_actions: [
      "reconciled_prepared_attempt",
      "accepted_retention_gap",
      "cleared_retryable_block",
      "superseded_generation"
    ] as const satisfies readonly RepairAction[],
    sql_dialects: [
      "data_fusion",
      "postgres",
      "my_sql",
      "sqlite"
    ] as const satisfies readonly SqlDialect[],
    query_consistency: [
      "eventual",
      "read_your_writes",
      "strong"
    ] as const satisfies readonly Consistency[],
    comparison_operators: [
      "eq",
      "ne",
      "lt",
      "lte",
      "gt",
      "gte",
      "in",
      "contains",
      "prefix"
    ] as const satisfies readonly CmpOp[],
    sort_directions: ["asc", "desc"] as const satisfies readonly Dir[],
    aggregate_functions: [
      "count",
      "count_distinct",
      "sum",
      "avg",
      "min",
      "max",
      "percentile",
      "std_dev"
    ] as const satisfies readonly AggFunc[],
    boundary_relations: [
      "current",
      "historical",
      "ahead_of_observed_checkpoint"
    ] as const satisfies readonly BoundaryRelation[],
    query_execution_states: [
      "queued",
      "planning",
      "running",
      "completed",
      "cancelled",
      "failed",
      "expired"
    ] as const satisfies readonly QueryExecutionState[],
    query_error_codes: [
      "unsupported",
      "unauthorized",
      "index_not_found",
      "fork_not_found",
      "backend",
      "unavailable",
      "too_large",
      "version",
      "stale",
      "cancelled",
      "deadline_exceeded",
      "expired_snapshot",
      "stale_generation",
      "target_unavailable",
      "resource_limit"
    ] as const satisfies readonly QueryErrorCode[],
    logical_type_kinds: [
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
    ] as const satisfies readonly LogicalTypeKind[],
    backend_modes: ["operational", "lakehouse"] as const satisfies readonly BackendMode[],
    backend_desired_states: [
      "disabled",
      "enabled"
    ] as const satisfies readonly BackendDesiredState[],
    backend_observed_states: [
      "disabled",
      "starting",
      "ready",
      "degraded",
      "unavailable"
    ] as const satisfies readonly BackendObservedState[],
    backend_readiness_codes: [
      "disabled",
      "configuration_pending",
      "configuration_rejected",
      "credential_unavailable",
      "object_store_unavailable",
      "catalog_unavailable",
      "query_runtime_unavailable",
      "generation_mismatch",
      "probe_failed"
    ] as const satisfies readonly BackendReadinessCode[],
    time_travel_capabilities: [
      "snapshot_id",
      "timestamp_micros"
    ] as const satisfies readonly TimeTravelCapability[],
    query_paging_capabilities: [
      "offset",
      "cursor"
    ] as const satisfies readonly QueryPagingCapability[],
    operation_states: [
      "accepted",
      "running",
      "succeeded",
      "failed",
      "cancelled"
    ] as const satisfies readonly OperationState[]
  }
  for (const [key, values] of Object.entries(expected)) assert.deepEqual(map.get(key), values)
  assertSame(encodeOne(map), bytes)
})

void test("destination start policies match every Rust discriminant", async () => {
  const bytes = await fixture("start_policy_discriminants.bin")
  const encoded = decodeOne(bytes, "StartPolicy[]")
  assert.ok(Array.isArray(encoded))
  const policies = encoded.map((item, index) =>
    decodeStartPolicy(expectMap(item, `StartPolicy[${String(index)}]`), "StartPolicy")
  )
  assert.deepEqual(
    policies.map((policy) => policy.kind),
    ["beginning", "captured_latest", "explicit"]
  )
  assertSame(encodeOne(policies.map(encodeStartPolicy)), bytes)
})

void test("query targets and errors match every Rust discriminant", async () => {
  const targetBytes = await fixture("query_target_discriminants.bin")
  const encodedTargets = decodeOne(targetBytes, "QueryTarget[]")
  assert.ok(Array.isArray(encodedTargets))
  const targets = encodedTargets.map((item, index) =>
    decodeQueryTarget(expectMap(item, `QueryTarget[${String(index)}]`), "QueryTarget")
  )
  assert.deepEqual(
    targets.map((target) => target.kind),
    ["operational", "lakehouse", "lakehouse", "lakehouse"]
  )
  assertSame(encodeOne(targets.map(encodeQueryTarget)), targetBytes)

  const errorBytes = await fixture("query_error_discriminants.bin")
  const encodedErrors = decodeOne(errorBytes, "QueryError[]")
  assert.ok(Array.isArray(encodedErrors))
  const errors = encodedErrors.map((item, index) =>
    decodeQueryError(item, `QueryError[${String(index)}]`)
  )
  assert.equal(errors.length, 15)
  assert.equal(new Set(errors.map((error) => error.kind)).size, 15)
  assertSame(encodeOne(errors.map(encodeQueryError)), errorBytes)
})

void test("unknown closed data stack discriminants fail instead of changing meaning", () => {
  assert.throws(() => decodeStartPolicy(new Map([["kind", "future"]]), "StartPolicy"), /unknown/)
  assert.throws(() => decodeQueryTarget(new Map([["kind", "future"]]), "QueryTarget"), /unknown/)
  assert.throws(() => decodeQueryError(new Map([["Future", new Map()]]), "QueryError"), /unknown/)
})

void test("logical schema and every logical type discriminant match the Rust fixtures", async () => {
  const schemaBytes = await fixture("logical_schema.bin")
  const schema = decodeLogicalSchema(
    expectMap(decodeOne(schemaBytes, "LogicalSchema"), "LogicalSchema"),
    "LogicalSchema"
  )
  assert.equal(schema.fields.length, 2)
  assertSame(encodeNamed(encodeLogicalSchema(schema)), schemaBytes)

  const typeBytes = await fixture("logical_type_discriminants.bin")
  const encodedTypes = decodeOne(typeBytes, "LogicalType[]")
  assert.ok(Array.isArray(encodedTypes))
  const types = encodedTypes.map((item, index) =>
    decodeLogicalType(expectMap(item, `LogicalType[${String(index)}]`), "LogicalType")
  )
  assert.equal(types.length, 17)
  assertSame(encodeOne(types.map(encodeLogicalType)), typeBytes)
})

void test("every typed value discriminant preserves its Rust wire representation", async () => {
  const bytes = await fixture("typed_value_discriminants.bin")
  const encodedValues = decodeOne(bytes, "TypedValue[]")
  assert.ok(Array.isArray(encodedValues))
  const values = encodedValues.map((item, index) =>
    decodeTypedValue(item, `TypedValue[${String(index)}]`)
  )
  assert.equal(values.length, 18)
  assertSame(encodeOne(values.map(encodeTypedValue)), bytes)
})

void test("typed value diagnostics remain stable for scalar and nested values", () => {
  assert.equal(
    typedValueDiagnosticText({ kind: "uuid", value: Uint8Array.from({ length: 16 }, (_, i) => i) }),
    "00010203-0405-0607-0809-0a0b0c0d0e0f"
  )
  assert.equal(
    typedValueDiagnosticText({
      kind: "decimal",
      value: { unscaled: Uint8Array.of(0x7b), precision: 3, scale: 2 }
    }),
    "0x7b scale 2"
  )
  assert.equal(typedValueDiagnosticText({ kind: "binary", value: Uint8Array.of(1, 2) }), "0x0102")
  assert.match(
    typedValueDiagnosticText({
      kind: "list",
      value: [{ kind: "long", value: 7n }]
    }),
    /"long"/
  )
})

void test("destination and query route declarations match the Rust fixtures", async () => {
  const destinationBytes = await fixture("materialization_destination.bin")
  const destination = decodeMaterializationDestination(
    expectMap(decodeOne(destinationBytes, "MaterializationDestination"), "destination"),
    "MaterializationDestination"
  )
  assert.equal(destination.tableFormat, "iceberg_v2")
  assertSame(encodeNamed(encodeMaterializationDestination(destination)), destinationBytes)

  const routeBytes = await fixture("query_route.bin")
  const route = decodeQueryRoute(
    expectMap(decodeOne(routeBytes, "QueryRoute"), "QueryRoute"),
    "QueryRoute"
  )
  assert.equal(route.target.kind, "lakehouse")
  assertSame(encodeNamed(encodeQueryRoute(route)), routeBytes)
})

void test("Arrow metadata and the frozen policy match the Rust fixtures", async () => {
  const metadataBytes = await fixture("arrow_ipc_metadata.bin")
  const metadata = decodeArrowIpcMessageMetadata(
    expectMap(decodeOne(metadataBytes, "ArrowIpcMessageMetadata"), "metadata"),
    "ArrowIpcMessageMetadata"
  )
  assert.equal(metadata.encodedBytes, 4096n)
  assertSame(encodeNamed(encodeArrowIpcMessageMetadata(metadata)), metadataBytes)

  const policyBytes = await fixture("arrow_ipc_policy.bin")
  const policy = decodeArrowIpcPolicy(
    expectMap(decodeOne(policyBytes, "ArrowIpcPolicy"), "policy"),
    "ArrowIpcPolicy"
  )
  assert.equal(policy.timestampUnit, "microsecond")
  assertSame(encodeNamed(encodeArrowIpcPolicy(policy)), policyBytes)
})

void test("public checkpoint requests cannot be confused with replicated mutations", async () => {
  const publicBytes = await fixture("checkpoint_request_public.bin")
  const request = decodeCheckpointRequestFrame(publicBytes)
  assert.equal(request.mutation.kind, "register_destination")
  assertSame(encodeCheckpointRequestFrame(request), publicBytes)

  const replicatedBytes = await fixture("checkpoint_mutation_replicated.bin")
  assert.throws(() => decodeCheckpointRequestFrame(replicatedBytes))
})

void test("checkpoint request validation rejects invalid mutations and assertion placement", async () => {
  const bytes = await fixture("checkpoint_request_public.bin")
  const request = decodeCheckpointRequestFrame(bytes)
  const invalidRevision = {
    ...request,
    mutation: {
      kind: "bind_table" as const,
      destinationId:
        request.mutation.kind === "register_destination"
          ? request.mutation.destination.id
          : assert.fail("fixture must register a destination"),
      destinationGeneration: 1n,
      expectedDefinitionRevision: 0n,
      tableUuid: new Uint8Array(16)
    }
  }
  assert.throws(() => {
    validateCheckpointRequest(invalidRevision)
  }, /definition revision/)
  assert.throws(() => {
    decodeCheckpointRequestFrame(encodeNamed(encodeCheckpointRequest(invalidRevision)))
  }, /definition revision/)

  if (request.mutation.kind !== "register_destination") assert.fail("fixture shape changed")
  const highRisk = {
    ...request,
    mutation: {
      kind: "accept_retention_gap" as const,
      destinationId: request.mutation.destination.id,
      destinationGeneration: request.mutation.destination.generation,
      expectedCheckpointRevision: 1n,
      nextOffset: 10n
    }
  }
  assert.throws(() => {
    validateCheckpointRequest(highRisk)
  }, /requires a supervisor assertion/)
})

void test("every public checkpoint mutation discriminant matches the Rust fixture", async () => {
  const bytes = await fixture("checkpoint_public_mutation_discriminants.bin")
  const encoded = decodeOne(bytes, "PublicCheckpointMutation[]")
  assert.ok(Array.isArray(encoded))
  const mutations = encoded.map((item, index) =>
    decodePublicCheckpointMutation(
      expectMap(item, `PublicCheckpointMutation[${String(index)}]`),
      `PublicCheckpointMutation[${String(index)}]`
    )
  )
  assert.equal(mutations.length, 18)
  assert.equal(new Set(mutations.map((mutation) => mutation.kind)).size, 18)
  assertSame(encodeOne(mutations.map(encodePublicCheckpointMutation)), bytes)
})

void test("checkpoint status preserves the source boundary and read consistency", async () => {
  const bytes = await fixture("destination_checkpoint_status.bin")
  const status = decodeDestinationCheckpointStatus(
    expectMap(decodeOne(bytes, "DestinationCheckpointStatus"), "status"),
    "DestinationCheckpointStatus"
  )
  assert.equal(status.consistency, "linearizable")
  assert.equal(status.partitions[0]?.nextOffset, 150n)
  assertSame(encodeNamed(encodeDestinationCheckpointStatus(status)), bytes)

  assert.throws(() => {
    validateDestinationCheckpointStatus({ ...status, globalStateRevision: 0n })
  }, /global and definition revisions/)
  const partition = status.partitions[0]
  assert.ok(partition)
  assert.throws(() => {
    validateDestinationCheckpointStatus({
      ...status,
      partitions: [partition, partition]
    })
  }, /repeats a partition/)
})

void test("query page, cancel, status, and execution status match the Rust fixtures", async () => {
  const pageBytes = await fixture("query_page.bin")
  assertSame(encodeQueryPageEnvelopeFrame(decodeQueryPageEnvelopeFrame(pageBytes)), pageBytes)

  const cancelBytes = await fixture("query_cancel.bin")
  assertSame(
    encodeQueryCancelEnvelopeFrame(decodeQueryCancelEnvelopeFrame(cancelBytes)),
    cancelBytes
  )

  const statusBytes = await fixture("query_status.bin")
  assertSame(
    encodeQueryStatusEnvelopeFrame(decodeQueryStatusEnvelopeFrame(statusBytes)),
    statusBytes
  )

  const replyBytes = await fixture("query_status_reply.bin")
  const reply = decodeQueryStatusReplyFrame(replyBytes)
  assert.equal(reply.kind, "ok")
  assertSame(encodeQueryStatusReplyFrame(reply), replyBytes)
})

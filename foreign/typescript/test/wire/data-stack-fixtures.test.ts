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
import { decodeOne, encodeNamed, encodeOne, expectMap } from "../../src/wire/cbor.js"
import type {
  BackendDesiredState,
  BackendMode,
  BackendObservedState,
  BackendReadinessCode
} from "../../src/wire/hello.js"
import {
  type AggFunc,
  type CmpOp,
  type Consistency,
  type Dir,
  type QueryErrorCode,
  type SqlDialect,
  decodeQueryError,
  decodeQueryTarget,
  encodeQueryError,
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
    sql_dialects: ["postgres", "my_sql", "sqlite"] as const satisfies readonly SqlDialect[],
    query_consistency: ["eventual", "read_your_writes"] as const satisfies readonly Consistency[],
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
    query_error_codes: [
      "unsupported",
      "invalid",
      "unauthorized",
      "index_not_found",
      "fork_not_found",
      "backend",
      "unavailable",
      "too_large",
      "version",
      "stale",
      "deadline_exceeded",
      "stale_generation"
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
    backend_modes: ["operational"] as const satisfies readonly BackendMode[],
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
      "probe_failed",
      "query_identity_rejected"
    ] as const satisfies readonly BackendReadinessCode[]
  }
  for (const [key, values] of Object.entries(expected)) assert.deepEqual(map.get(key), values)
  assertSame(encodeOne(map), bytes)
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
    ["operational"]
  )
  assertSame(encodeOne(targets.map(encodeQueryTarget)), targetBytes)

  const errorBytes = await fixture("query_error_discriminants.bin")
  const encodedErrors = decodeOne(errorBytes, "QueryError[]")
  assert.ok(Array.isArray(encodedErrors))
  const errors = encodedErrors.map((item, index) =>
    decodeQueryError(item, `QueryError[${String(index)}]`)
  )
  assert.equal(errors.length, 12)
  assert.equal(new Set(errors.map((error) => error.kind)).size, 12)
  assertSame(encodeOne(errors.map(encodeQueryError)), errorBytes)
})

void test("unknown closed data stack discriminants fail instead of changing meaning", () => {
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
  assert.equal(types.length, 18)
  assert.deepEqual(types.at(-1), { kind: "variant" })
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

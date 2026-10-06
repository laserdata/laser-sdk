import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import {
  Digest32,
  SchemaFingerprint,
  UuidValue,
  decimalValueValidateCanonical,
  decodeLogicalSchema,
  decodeLogicalType,
  logicalSchemaCanonicalFingerprintBytes,
  logicalSchemaComputeFingerprint,
  logicalTypeAcceptsMapKey,
  logicalTypeKind,
  typedValueAsI64,
  typedValueAsStr,
  typedValueAsU64,
  typedValueValidateAgainst,
  typedValueValidateCanonical
} from "../../src/wire/schema.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_the_logical_schema_fixture_when_fingerprinted_then_should_match_the_rust_fingerprint", async () => {
  const bytes = await readFixture("logical_schema.bin")
  const schema = decodeLogicalSchema(expectMap(decodeOne(bytes, "schema"), "schema"), "schema")
  const canonical = logicalSchemaCanonicalFingerprintBytes(schema)
  assert.equal(new TextDecoder().decode(canonical.subarray(0, 15)), "AGDX-SCHEMA-V1\0")
  const fingerprint = logicalSchemaComputeFingerprint(schema)
  assert.equal(fingerprint.length, SchemaFingerprint.BYTES)
  assert.deepEqual(Buffer.from(fingerprint), Buffer.from(schema.schema.fingerprint))
})

void test("given_every_logical_type_when_asked_for_map_key_support_then_should_accept_deterministic_primitives", async () => {
  const bytes = await readFixture("logical_type_discriminants.bin")
  const encoded = decodeOne(bytes, "types")
  assert.ok(Array.isArray(encoded))
  const types = encoded.map((item) => decodeLogicalType(expectMap(item, "type"), "type"))
  const accepted = types.filter(logicalTypeAcceptsMapKey).map(logicalTypeKind)
  assert.deepEqual(accepted.sort(), [
    "binary",
    "boolean",
    "date",
    "decimal",
    "fixed",
    "int",
    "long",
    "string",
    "time_micros",
    "timestamp_micros",
    "timestamp_tz_micros",
    "uuid"
  ])
  assert.equal(logicalTypeKind({ kind: "fixed", length: 4 }), "fixed")
})

void test("given_typed_values_when_read_as_scalars_then_should_convert_only_in_range_integers", () => {
  assert.equal(typedValueAsStr({ kind: "string", value: "eu" }), "eu")
  assert.equal(typedValueAsStr({ kind: "int", value: 1 }), undefined)
  assert.equal(typedValueAsI64({ kind: "int", value: -3 }), -3n)
  assert.equal(typedValueAsI64({ kind: "long", value: 9n }), 9n)
  assert.equal(typedValueAsI64({ kind: "double", value: 1 }), undefined)
  assert.equal(typedValueAsU64({ kind: "long", value: 7n }), 7n)
  assert.equal(typedValueAsU64({ kind: "int", value: -1 }), undefined)
  assert.equal(typedValueAsU64({ kind: "time_micros", value: 1n }), undefined)
})

void test("given_typed_values_when_validated_then_should_enforce_canonical_form_and_the_logical_type", () => {
  typedValueValidateCanonical({ kind: "int", value: 5 })
  assert.throws(() => {
    typedValueValidateCanonical({ kind: "double", value: Number.NaN })
  })
  typedValueValidateAgainst({ kind: "long", value: 1n }, { kind: "long" }, true)
  typedValueValidateAgainst({ kind: "null" }, { kind: "long" }, false)
  assert.throws(() => {
    typedValueValidateAgainst({ kind: "null" }, { kind: "long" }, true)
  })
  assert.throws(() => {
    typedValueValidateAgainst({ kind: "int", value: 1 }, { kind: "long" }, true)
  })
})

void test("given_decimal_values_when_validated_then_should_reject_non_canonical_encodings", () => {
  decimalValueValidateCanonical({ unscaled: Uint8Array.of(0x7f), precision: 3, scale: 0 })
  assert.throws(() => {
    decimalValueValidateCanonical({ unscaled: Uint8Array.of(0, 1), precision: 3, scale: 0 })
  })
  assert.throws(() => {
    decimalValueValidateCanonical({ unscaled: Uint8Array.of(0x7f), precision: 2, scale: 0 })
  })
})

void test("given_the_fixed_width_byte_values_when_sized_then_should_match_the_wire_widths", () => {
  assert.equal(SchemaFingerprint.BYTES, 32)
  assert.equal(Digest32.BYTES, 32)
  assert.equal(UuidValue.BYTES, 16)
})

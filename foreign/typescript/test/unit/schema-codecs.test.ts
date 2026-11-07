import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import protobuf from "protobufjs"
import descriptor from "protobufjs/ext/descriptor.js"

import { CodecError, InvalidError } from "../../src/client/errors.js"
import { CompiledSchema } from "../../src/schema-codecs.js"
import type { SchemaDef, SchemaSource } from "../../src/wire/control.js"
import { parseCanonicalJson } from "../../src/wire/filter.js"

interface ValidationCase {
  readonly name: string
  readonly schema: unknown
  readonly accepted: readonly unknown[]
  readonly rejected: readonly unknown[]
}

void test("given_the_writer_schema_corpus_when_decoded_then_should_match_every_expected_value", async () => {
  const corpus = await writerSchemaCorpus()
  const failures: string[] = []
  for (const sample of corpus.cases) {
    const source = corpus.schemas.get(sample.schema)
    assert.ok(source !== undefined, sample.name)
    let compiled: CompiledSchema
    try {
      compiled = CompiledSchema.compile({ id: sample.schema, source })
    } catch (error) {
      if (!sample.refused || !(error instanceof InvalidError))
        failures.push(`${sample.name}: ${String(error)}`)
      continue
    }
    if (sample.refused) {
      failures.push(`${sample.name}: compiled`)
      continue
    }
    try {
      const value = compiled.decode(sample.payload)
      if (sample.expected === undefined || !sameValue(value, sample.expected))
        failures.push(`${sample.name}: ${describe(value)}`)
    } catch (error) {
      if (sample.expected !== undefined || !(error instanceof CodecError))
        failures.push(`${sample.name}: ${String(error)}`)
    }
  }
  assert.deepEqual(failures, [])
})

void test("given_64_bit_writer_schema_integers_when_decoded_then_should_be_bigint_and_narrower_ones_number", async () => {
  const corpus = await writerSchemaCorpus()
  const decode = (name: string): Record<string, unknown> => {
    const sample = corpus.cases.find((candidate) => candidate.name === name)
    assert.ok(sample !== undefined)
    const source = corpus.schemas.get(sample.schema)
    assert.ok(source !== undefined)
    return CompiledSchema.compile({ id: sample.schema, source }).decode(sample.payload) as Record<
      string,
      unknown
    >
  }
  const avro = decode("avro primitives keep exact longs")
  assert.equal(typeof avro["large"], "bigint")
  assert.equal(typeof avro["small"], "number")
  const reading = decode("protobuf common types")
  assert.equal(reading["cpu_usage"], -9007199254740993n)
  assert.equal(reading["total"], 18446744073709551615n)
  assert.equal(typeof reading["delta"], "number")
  assert.equal(reading["mode"], 2)
  assert.deepEqual(reading["digest"], [0, 255])
})

void test("given_an_avro_union_of_int_and_long_when_encoded_from_plain_values_then_should_take_the_first_branch_that_fits", () => {
  const compiled = CompiledSchema.compile({
    id: 11,
    source: {
      kind: "avro",
      schema: JSON.stringify({
        type: "record",
        name: "Count",
        fields: [{ name: "count", type: ["null", "int", "long"] }]
      })
    }
  })
  for (const count of [null, 5, 1n << 60n])
    assert.deepEqual(compiled.decode(compiled.encodeAvro({ count })), { count })
})

void test("given_invalid_utf8_inside_a_json_string_when_decoded_then_should_reject_the_payload", () => {
  const compiled = CompiledSchema.compile({
    id: 1,
    source: { kind: "jsonSchema", schema: '{"type":"string"}' }
  })
  assert.throws(() => compiled.decode(Uint8Array.of(34, 255, 34)), CodecError)
  assert.equal(compiled.validate(Uint8Array.of(34, 255, 34)), false)
})

void test("given_an_avro_datum_with_trailing_bytes_when_decoded_then_should_reject_the_payload", () => {
  const compiled = CompiledSchema.compile({ id: 1, source: { kind: "avro", schema: '"long"' } })
  assert.equal(compiled.decode(Uint8Array.of(14)), 7n)
  assert.throws(() => compiled.decode(Uint8Array.of(14, 0)), CodecError)
  assert.equal(compiled.validate(Uint8Array.of(14, 0)), false)
})

void test("given_valid_json_schema_forms_when_compiled_then_should_match_the_shared_validation_cases", async () => {
  const file = path.resolve(process.cwd(), "../../wire/fixtures/schema_validation_cases.json")
  const cases = JSON.parse(await readFile(file, "utf8")) as ValidationCase[]
  for (const sample of cases) {
    const compiled = CompiledSchema.compile({
      id: 1,
      source: { kind: "jsonSchema", schema: JSON.stringify(sample.schema) }
    })
    for (const value of sample.accepted) {
      assert.equal(compiled.validateValue(value), true, sample.name)
    }
    for (const value of sample.rejected) {
      assert.equal(compiled.validateValue(value), false, sample.name)
    }
  }
})

const READING_AVRO = JSON.stringify({
  type: "record",
  name: "Reading",
  fields: [
    { name: "host", type: "string" },
    { name: "cpu", type: "long" }
  ]
})

const ORDER_JSON_SCHEMA = JSON.stringify({
  type: "object",
  required: ["host", "cpu"],
  additionalProperties: false,
  properties: {
    host: { type: "string" },
    cpu: { type: "integer", minimum: 0 }
  }
})

function avroSchema(): SchemaDef {
  return { id: 7, source: { kind: "avro", schema: READING_AVRO } }
}

function protobufSchema(): {
  readonly schema: SchemaDef
  readonly reading: protobuf.Type
  readonly descriptorSet: Uint8Array
} {
  const root = new protobuf.Root()
  const reading = new protobuf.Type("Reading")
    .add(new protobuf.Field("host", 1, "string"))
    .add(new protobuf.Field("cpu", 2, "int64"))
  root.add(reading)
  const descriptorSet = descriptor.FileDescriptorSet.encode(root.toDescriptor("proto3")).finish()
  return {
    schema: {
      id: 8,
      source: { kind: "protobuf", descriptorSet, messageType: "Reading" }
    },
    reading,
    descriptorSet
  }
}

void test("given_an_avro_schema_when_encoded_then_should_decode_and_validate_the_datum", () => {
  const compiled = CompiledSchema.compile(avroSchema())
  const payload = compiled.encodeAvro({ host: "node-1", cpu: 42 })
  assert.equal(compiled.kind, "avro")
  assert.equal(compiled.validate(payload), true)
  assert.deepEqual(compiled.decode(payload), { host: "node-1", cpu: 42n })
})

void test("given_an_avro_mismatch_when_encoded_then_should_reject_before_publish", () => {
  const compiled = CompiledSchema.compile(avroSchema())
  assert.throws(() => compiled.encodeAvro({ unrelated: true }), CodecError)
  assert.equal(compiled.validate(new Uint8Array()), false)
})

void test("given_a_json_schema_when_used_then_should_validate_values_and_payloads", () => {
  const compiled = CompiledSchema.compile({
    id: 9,
    source: { kind: "jsonSchema", schema: ORDER_JSON_SCHEMA }
  })
  const value = { host: "node-2", cpu: 17 }
  const codec = compiled.codec((decoded) => decoded)
  const payload = codec.encode(value)
  assert.equal(compiled.kind, "jsonSchema")
  assert.equal(compiled.validateValue(value), true)
  assert.equal(compiled.validateValue({ host: "node-2", cpu: -1 }), false)
  assert.deepEqual(compiled.decode(payload), value)
  assert.throws(() => codec.encode({ host: "node-2", cpu: -1 }), CodecError)
  assert.throws(() => compiled.encodeAvro(value), InvalidError)
})

void test("given_a_protobuf_descriptor_when_used_then_should_resolve_encode_and_decode", () => {
  const { schema } = protobufSchema()
  const compiled = CompiledSchema.compile(schema)
  const payload = compiled.codec((decoded) => decoded).encode({ host: "node-3", cpu: 23 })
  assert.equal(compiled.kind, "protobuf")
  assert.equal(compiled.validate(payload), true)
  assert.deepEqual(compiled.decode(payload), { host: "node-3", cpu: 23n })
})

void test("given_malformed_or_missing_schema_sources_when_compiled_then_should_reject", () => {
  assert.throws(
    () => CompiledSchema.compile({ id: 1, source: { kind: "avro", schema: "{" } }),
    InvalidError
  )
  assert.throws(
    () => CompiledSchema.compile({ id: 2, source: { kind: "jsonSchema", schema: "[]" } }),
    InvalidError
  )
  const { descriptorSet } = protobufSchema()
  assert.throws(
    () =>
      CompiledSchema.compile({
        id: 2,
        source: { kind: "protobuf", descriptorSet, messageType: "Missing" }
      }),
    InvalidError
  )
  assert.throws(() => CompiledSchema.compile({ id: 3, source: { kind: "unknown" } }), InvalidError)
})

void test("given_avro_bytes_and_fixed_arrays_when_encoded_then_should_round_trip_without_changing_numeric_arrays", () => {
  const compiled = CompiledSchema.compile({
    id: 10,
    source: {
      kind: "avro",
      schema: JSON.stringify({
        type: "record",
        name: "BinaryReading",
        fields: [
          { name: "body", type: "bytes" },
          { name: "signature", type: { type: "fixed", name: "Signature", size: 2 } },
          { name: "counts", type: { type: "array", items: "int" } },
          { name: "optional", type: ["null", "bytes"] }
        ]
      })
    }
  })
  const value = { body: [0, 127, 255], signature: [1, 2], counts: [256, 1024], optional: [3, 4] }
  assert.deepEqual(compiled.decode(compiled.encodeAvro(value)), value)
  assert.throws(() => compiled.encodeAvro({ ...value, body: [256] }), CodecError)
  assert.throws(() => compiled.encodeAvro({ ...value, signature: [1] }), CodecError)
})

interface WriterSchemaCase {
  readonly name: string
  readonly schema: number
  readonly payload: Uint8Array
  readonly expected?: unknown
  readonly refused: boolean
}

async function writerSchemaCorpus(): Promise<{
  readonly schemas: ReadonlyMap<number, SchemaSource>
  readonly cases: readonly WriterSchemaCase[]
}> {
  const file = path.resolve(process.cwd(), "../../wire/fixtures/writer_schema_cases.json")
  const corpus = parseCanonicalJson(await readFile(file, "utf8")) as ReadonlyMap<string, unknown>
  const entries = (key: string): readonly ReadonlyMap<string, unknown>[] =>
    corpus.get(key) as readonly ReadonlyMap<string, unknown>[]
  const schemas = new Map<number, SchemaSource>()
  for (const definition of entries("schemas")) {
    const source = definition.get("source") as ReadonlyMap<string, unknown>
    const kind = source.get("kind")
    schemas.set(
      Number(definition.get("id")),
      kind === "protobuf"
        ? {
            kind: "protobuf",
            descriptorSet: Uint8Array.from(
              (source.get("descriptor_set") as readonly bigint[]).map(Number)
            ),
            messageType: source.get("message_type") as string
          }
        : {
            kind: kind === "avro" ? "avro" : "jsonSchema",
            schema: source.get("schema") as string
          }
    )
  }
  const cases = entries("cases").map((sample) => {
    const hex = (sample.get("payload_hex") as string | undefined) ?? ""
    return {
      name: sample.get("name") as string,
      schema: Number(sample.get("schema")),
      payload: Uint8Array.from(hex.match(/../g) ?? [], (pair) => Number.parseInt(pair, 16)),
      expected: sample.get("expected"),
      refused: sample.get("refused") === true
    }
  })
  return { schemas, cases }
}

// Numbers compare by value, so a `number` and a `bigint` match only when both
// are exact, and the corpus pins exactness without pinning the TypeScript type.
function sameValue(actual: unknown, expected: unknown): boolean {
  if (typeof expected === "bigint")
    return typeof actual === "bigint"
      ? actual === expected
      : typeof actual === "number" && Number.isSafeInteger(actual) && BigInt(actual) === expected
  if (Array.isArray(expected))
    return (
      Array.isArray(actual) &&
      actual.length === expected.length &&
      expected.every((item, index) => sameValue(actual[index], item))
    )
  if (expected instanceof Map) {
    if (actual === null || typeof actual !== "object" || Array.isArray(actual)) return false
    const keys = Object.keys(actual)
    return (
      keys.length === expected.size &&
      keys.every((key) => sameValue((actual as Record<string, unknown>)[key], expected.get(key)))
    )
  }
  return Object.is(actual, expected) || actual === expected
}

function describe(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) =>
    typeof item === "bigint" ? `${item.toString()}n` : item
  )
}

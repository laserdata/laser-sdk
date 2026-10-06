import assert from "node:assert/strict"
import { test } from "node:test"
import protobuf from "protobufjs"
import descriptor from "protobufjs/ext/descriptor.js"

import { CodecError, InvalidError } from "../../src/client/errors.js"
import { CompiledSchema } from "../../src/schema-codecs.js"
import type { SchemaDef } from "../../src/wire/control.js"

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
  const payload = compiled.encode({ host: "node-1", cpu: 42 })
  assert.equal(compiled.kind, "avro")
  assert.equal(compiled.validate(payload), true)
  assert.deepEqual(compiled.decode(payload), { host: "node-1", cpu: 42 })
})

void test("given_an_avro_mismatch_when_encoded_then_should_reject_before_publish", () => {
  const compiled = CompiledSchema.compile(avroSchema())
  assert.throws(() => compiled.encode({ unrelated: true }), CodecError)
  assert.equal(compiled.validate(new Uint8Array()), false)
})

void test("given_a_json_schema_when_used_then_should_validate_values_and_payloads", () => {
  const compiled = CompiledSchema.compile({
    id: 9,
    source: { kind: "jsonSchema", schema: ORDER_JSON_SCHEMA }
  })
  const value = { host: "node-2", cpu: 17 }
  const payload = compiled.encode(value)
  assert.equal(compiled.kind, "jsonSchema")
  assert.equal(compiled.validateValue(value), true)
  assert.equal(compiled.validateValue({ host: "node-2", cpu: -1 }), false)
  assert.deepEqual(compiled.decode(payload), value)
  assert.throws(() => compiled.encode({ host: "node-2", cpu: -1 }), CodecError)
})

void test("given_a_protobuf_descriptor_when_used_then_should_resolve_encode_and_decode", () => {
  const { schema } = protobufSchema()
  const compiled = CompiledSchema.compile(schema)
  const payload = compiled.encode({ host: "node-3", cpu: 23 })
  assert.equal(compiled.kind, "protobuf")
  assert.equal(compiled.validate(payload), true)
  assert.deepEqual(compiled.decode(payload), { host: "node-3", cpu: "23" })
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

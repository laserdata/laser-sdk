import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  IndexSchemaBuilder,
  ProjectionBindingBuilder,
  ProjectionBuilder,
  decodeControlEnvelope,
  decodeRetentionPolicy,
  decodeSchemaSource,
  encodeControlEnvelope,
  parseProjectionId,
  projectionKindFromCode,
  projectionKindIsRow,
  schemaDefContentType
} from "../../src/wire/control.js"
import { BackendResourceId } from "../../src/wire/ids.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

async function assertControlRoundTrips(
  name: string
): Promise<ReturnType<typeof decodeControlEnvelope>> {
  const bytes = await readFixture(name)
  const envelope = decodeControlEnvelope(expectMap(decodeOne(bytes, name), name), name)
  assert.deepEqual(Buffer.from(encodeNamed(encodeControlEnvelope(envelope))), Buffer.from(bytes))
  return envelope
}

void test("given_the_projection_control_fixture_when_decoded_then_should_preserve_extraction", async () => {
  const envelope = await assertControlRoundTrips("control_register_projection.bin")
  if (envelope.command.kind !== "registerProjection") throw new Error("wrong command")
  assert.equal(envelope.command.projection.id, "reading.v1")
  assert.equal(envelope.command.projection.extraction.fields.length, 3)
})

void test("given_the_binding_control_fixtures_when_decoded_then_should_preserve_routing", async () => {
  const applied = await assertControlRoundTrips("control_apply_binding.bin")
  if (applied.command.kind !== "applyBinding") throw new Error("wrong command")
  assert.equal(applied.command.binding.index, "readings_rows")
  assert.equal(applied.command.binding.retention?.kind, "timeToLive")

  const removed = await assertControlRoundTrips("control_remove_binding.bin")
  assert.equal(removed.command.kind, "removeBinding")
})

void test("given_each_schema_control_fixture_when_decoded_then_should_preserve_the_source", async () => {
  for (const name of [
    "control_register_schema_avro.bin",
    "control_register_schema_protobuf.bin",
    "control_register_schema_json.bin",
    "control_drop_schema.bin"
  ]) {
    await assertControlRoundTrips(name)
  }
})

void test("given_the_run_source_control_fixtures_when_decoded_then_should_preserve_the_topic", async () => {
  const registered = await assertControlRoundTrips("control_register_run_source.bin")
  const removed = await assertControlRoundTrips("control_remove_run_source.bin")
  assert.equal(registered.command.kind, "registerRunSource")
  assert.equal(removed.command.kind, "removeRunSource")
})

void test("given_unknown_additive_control_values_when_decoded_then_should_degrade_or_pass_through", () => {
  assert.deepEqual(decodeSchemaSource(new Map([["kind", "future"]]), "schema"), {
    kind: "unknown"
  })
  assert.deepEqual(decodeRetentionPolicy(new Map([["kind", "future"]]), "retention"), {
    kind: "unknown"
  })
  assert.deepEqual(projectionKindFromCode(99), { kind: "unrecognized", code: 99 })
})

void test("given_projection_ids_when_parsed_then_should_reject_only_the_empty_value", () => {
  assert.equal(parseProjectionId("reading.v1"), "reading.v1")
  assert.throws(() => parseProjectionId(""))
})

void test("given_the_canonical_projection_when_built_then_should_encode_the_golden_fixture", async () => {
  const bytes = await readFixture("control_register_projection.bin")
  const fixture = decodeControlEnvelope(
    expectMap(decodeOne(bytes, "fixture"), "fixture"),
    "fixture"
  )
  const projection = new ProjectionBuilder("reading.v1")
    .name("reading")
    .contentType("json")
    .field("reading_id")
    .fieldAt("host", "/host/id")
    .fieldAtTyped("cpu", "/cpu", "int")
    .vectorField("/embedding")
    .build()
  const envelope = {
    v: fixture.v,
    timestampMicros: fixture.timestampMicros,
    command: { kind: "registerProjection" as const, projection }
  }
  assert.deepEqual(Buffer.from(encodeNamed(encodeControlEnvelope(envelope))), Buffer.from(bytes))
})

void test("given_the_canonical_binding_when_built_then_should_encode_the_golden_fixture", async () => {
  const bytes = await readFixture("control_apply_binding.bin")
  const fixture = decodeControlEnvelope(
    expectMap(decodeOne(bytes, "fixture"), "fixture"),
    "fixture"
  )
  const binding = new ProjectionBindingBuilder()
    .source("fleet", "readings")
    .allow("reading.v1")
    .defaultProjection("reading.v1")
    .backend({ resourceId: BackendResourceId.fromU128(4n), generation: 2n })
    .index("readings_rows")
    .retention({ kind: "timeToLive", ttlMicros: 3_600_000_000n })
    .build()
  const envelope = {
    v: fixture.v,
    timestampMicros: fixture.timestampMicros,
    command: { kind: "applyBinding" as const, binding }
  }
  assert.deepEqual(Buffer.from(encodeNamed(encodeControlEnvelope(envelope))), Buffer.from(bytes))
})

void test("given_projection_builder_defaults_when_built_then_should_inline_a_row_projection", () => {
  const projection = new ProjectionBuilder("api.call.v1").fields(["a", "b"]).build()
  assert.equal(projection.version, 1)
  assert.equal(projection.contentType, "any")
  assert.ok(projectionKindIsRow(projection.kind))
  assert.ok(projection.inlinePayloadDefault)
  assert.ok(projection.extraction.inlinePayload)
  assert.deepEqual(projection.extraction.fields, [
    { name: "a", pointer: "/a" },
    { name: "b", pointer: "/b" }
  ])

  const indexOnly = new ProjectionBuilder("api.call.v1")
    .fieldTyped("n", "int")
    .graph({ nodes: [], edges: [] })
    .indexOnly()
    .build()
  assert.equal(indexOnly.inlinePayloadDefault, false)
  assert.equal(indexOnly.extraction.inlinePayload, false)
  assert.equal(projectionKindIsRow(indexOnly.kind), false)
  assert.deepEqual(indexOnly.extraction.fields, [{ name: "n", pointer: "/n", fieldType: "int" }])
  assert.equal(
    new ProjectionBuilder("x").indexOnly().inlinePayload().build().inlinePayloadDefault,
    true
  )
})

void test("given_a_binding_without_a_source_when_built_then_should_refuse", () => {
  assert.equal(new ProjectionBindingBuilder().tryBuild(), undefined)
  assert.throws(() => new ProjectionBindingBuilder().build(), /requires a source/)
  const binding = new ProjectionBindingBuilder()
    .selector({ stream: "s", topic: "t" })
    .notify()
    .build()
  assert.equal(binding.index, "t")
  assert.equal(binding.notify, true)
})

void test("given_an_index_schema_builder_when_built_then_should_expand_root_fields", () => {
  assert.deepEqual(
    new IndexSchemaBuilder()
      .field("host")
      .fieldAt("cpu_pct", "/cpu/value")
      .vectorField("/vec")
      .inlinePayload()
      .build(),
    {
      fields: [
        { name: "host", pointer: "/host" },
        { name: "cpu_pct", pointer: "/cpu/value" }
      ],
      vectorField: "/vec",
      inlinePayload: true
    }
  )
  assert.deepEqual(new IndexSchemaBuilder().build(), { fields: [], inlinePayload: false })
})

void test("given_each_schema_source_when_asked_for_the_content_type_then_should_name_its_codec", () => {
  const def = (source: Parameters<typeof schemaDefContentType>[0]["source"]) => ({ id: 1, source })
  assert.equal(schemaDefContentType(def({ kind: "avro", schema: "{}" })), "avro")
  assert.equal(
    schemaDefContentType(
      def({ kind: "protobuf", descriptorSet: new Uint8Array(), messageType: "m" })
    ),
    "protobuf"
  )
  assert.equal(schemaDefContentType(def({ kind: "jsonSchema", schema: "{}" })), "json")
  assert.equal(schemaDefContentType(def({ kind: "unknown" })), "any")
})

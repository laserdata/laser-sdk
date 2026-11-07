import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  IndexSchemaBuilder,
  ProjectionBindingBuilder,
  ProjectionBuilder,
  decodeControlEnvelope,
  decodeProjectionBinding,
  decodeProjectionHistory,
  decodeRetentionPolicy,
  decodeSchemaSource,
  encodeControlEnvelope,
  encodeProjectionBinding,
  encodeProjectionHistory,
  parseProjectionId,
  projectionKindFromCode,
  projectionKindIsRow,
  schemaDefContentType,
  validateControlCommand,
  validateProjection,
  validateSchemaSource,
  type RetentionPolicy
} from "../../src/wire/control.js"
import { AGDX_REGISTER_SCHEMA_CODE, isIdempotentManagedRequest } from "../../src/wire/codes.js"
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
  if (removed.command.kind !== "removeBinding") throw new Error("wrong command")
  assert.equal(removed.command.generation, undefined)

  const retired = await assertControlRoundTrips("control_generation_remove_binding.bin")
  if (retired.command.kind !== "removeBinding") throw new Error("wrong command")
  assert.deepEqual(retired.command.generation, {
    streamId: 3,
    createdAtMicros: 1_700_000_000_000_000n
  })
  assert.equal(retired.stream, undefined)
  assert.equal(retired.stamp?.caller, undefined)
})

void test("given_the_operator_control_fixtures_when_decoded_then_should_preserve_the_decision", async () => {
  const accepted = await assertControlRoundTrips("control_accept_gap.bin")
  if (accepted.command.kind !== "acceptGap") throw new Error("wrong command")
  assert.equal(accepted.command.acceptance.partitionId, 2)
  assert.equal(accepted.command.acceptance.fromOffset, 41n)
  assert.equal(accepted.command.acceptance.toOffset, 97n)

  const skipped = await assertControlRoundTrips("control_skip_record.bin")
  if (skipped.command.kind !== "skipRecord") throw new Error("wrong command")
  assert.equal(skipped.command.skip.offset, 58n)
  assert.equal(skipped.command.skip.target, "readings_rows")

  const retained = await assertControlRoundTrips("control_source_retained.bin")
  if (retained.command.kind !== "sourceRetained") throw new Error("wrong command")
  assert.equal(retained.command.retention.partitionId, 2)
  assert.equal(retained.command.retention.firstOffset, 59n)
})

void test("given_a_stamped_control_fixture_when_decoded_then_should_preserve_the_stamp", async () => {
  const stamped = await assertControlRoundTrips("control_stamped_remove_binding.bin")
  assert.equal(stamped.stream, "fleet")
  if (stamped.stamp === undefined) throw new Error("stamp missing")
  assert.equal(stamped.stamp.deploymentId, 7n)
  assert.equal(stamped.stamp.caller, undefined)
  assert.equal(stamped.stamp.operationId, 0x0123456789abcdef0123456789abcdefn)
  assert.equal(stamped.stamp.streamGeneration?.streamId, 3)
  assert.equal(stamped.stamp.keyId, 11n)
  const plain = await assertControlRoundTrips("control_remove_binding.bin")
  assert.equal(plain.stamp, undefined)
})

void test("given_a_control_stamp_with_a_caller_written_for_an_end_user_when_decoded_then_should_keep_both", async () => {
  const stamped = await assertControlRoundTrips("control_stamped_caller.bin")
  if (stamped.stamp === undefined) throw new Error("stamp missing")
  assert.deepEqual(stamped.stamp.caller, { userId: 3, onBehalfOf: "1793690355287073" })
  const unstamped = await assertControlRoundTrips("control_stamped_remove_binding.bin")
  assert.equal(unstamped.stamp?.caller, undefined)
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

void test("given_session_source_control_fixtures_when_decoded_then_should_preserve_stream_and_topics", async () => {
  const all = await assertControlRoundTrips("control_register_session_source_all.bin")
  const named = await assertControlRoundTrips("control_register_session_source_named.bin")
  const removed = await assertControlRoundTrips("control_remove_session_source.bin")
  assert.deepEqual(all.command, {
    kind: "registerSessionSource",
    stream: "agents",
    topics: { kind: "all" }
  })
  assert.deepEqual(named.command, {
    kind: "registerSessionSource",
    stream: "agents",
    topics: { kind: "named", names: ["agent.sessions"] }
  })
  assert.deepEqual(removed.command, { kind: "removeSessionSource", stream: "agents" })
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

void test("given_an_index_field_name_that_is_not_an_identifier_when_validated_then_should_refuse_it", () => {
  for (const name of ["user-id", "order.total", "9lives", "café"]) {
    assert.throws(() => {
      validateProjection(new ProjectionBuilder("orders").fieldAt(name, "/value").build())
    }, /must start with an ASCII letter/)
  }
  for (const name of ["user_id", "_private", "total2"])
    validateProjection(new ProjectionBuilder("orders").fieldAt(name, "/value").build())
})

void test("given_a_retention_that_never_or_always_prunes_when_validated_then_should_refuse_the_binding", () => {
  const applying = (retention: RetentionPolicy) => ({
    kind: "applyBinding" as const,
    binding: new ProjectionBindingBuilder().source("s", "t").retention(retention).build()
  })
  for (const retention of [
    { kind: "unknown" },
    { kind: "timeToLive", ttlMicros: 0n },
    { kind: "maxRows", rows: 0n }
  ] as const) {
    assert.throws(() => {
      validateControlCommand(applying(retention))
    }, /policy this build does not know|above zero/)
  }
  validateControlCommand(applying({ kind: "maxRows", rows: 1n }))
})

void test("given_a_binding_that_skips_rejected_records_when_encoded_then_should_round_trip_the_opt_in", () => {
  const plain = new ProjectionBindingBuilder().source("s", "t").build()
  assert.equal(plain.invalidRecords, "block")
  assert.equal(plain.oversize, "block")
  assert.equal(encodeProjectionBinding(plain).has("oversize"), false)
  assert.equal(encodeProjectionBinding(plain).has("invalid_records"), false)
  const opted = new ProjectionBindingBuilder()
    .source("s", "t")
    .invalidRecords("dead_letter")
    .oversize("reference")
    .build()
  const encoded = encodeProjectionBinding(opted)
  assert.equal(encoded.get("invalid_records"), "dead_letter")
  const decoded = decodeProjectionBinding(
    expectMap(decodeOne(encodeNamed(encoded), "binding"), "binding"),
    "binding"
  )
  assert.equal(decoded.invalidRecords, "dead_letter")
  assert.equal(decoded.oversize, "reference")
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

void test("given_the_allocate_schema_control_fixture_when_decoded_then_should_round_trip_without_an_id", async () => {
  const envelope = await assertControlRoundTrips("control_allocate_schema.bin")
  if (envelope.command.kind !== "allocateSchema") throw new Error("wrong command")
  assert.equal(envelope.command.allocation.name, "reading")
  assert.equal(envelope.command.allocation.version, 2)
  assert.equal(envelope.command.allocation.source.kind, "jsonSchema")
  assert.equal(envelope.stream, "fleet")
  assert.equal(isIdempotentManagedRequest(AGDX_REGISTER_SCHEMA_CODE), true)
})

void test("given_record_and_policy_fixtures_when_decoded_then_should_round_trip_the_materialization_contract", async () => {
  const projection = await assertControlRoundTrips("control_register_projection_record.bin")
  assert.equal(projection.command.kind, "registerProjection")
  assert.equal(projection.command.projection.kind.kind, "record")
  const binding = await assertControlRoundTrips("control_apply_binding_policies.bin")
  assert.equal(binding.command.kind, "applyBinding")
  assert.equal(binding.command.binding.invalidRecords, "dead_letter")
  assert.equal(binding.command.binding.oversize, "reference")
})

void test("given_a_keyed_projection_fixture_when_decoded_then_should_preserve_key_and_deletion_header", async () => {
  const envelope = await assertControlRoundTrips("control_register_projection_keyed.bin")
  if (envelope.command.kind !== "registerProjection") throw new Error("projection command")
  assert.deepEqual(envelope.command.projection.history, {
    kind: "keyed",
    key: { kind: "header", name: "reading_id" },
    deleteHeader: "deleted"
  })
})

void test("given_a_keyed_projection_fixture_without_a_delete_header_when_decoded_then_should_round_trip_without_it", async () => {
  const envelope = await assertControlRoundTrips(
    "control_register_projection_keyed_without_delete.bin"
  )
  if (envelope.command.kind !== "registerProjection") throw new Error("projection command")
  assert.deepEqual(envelope.command.projection.history, {
    kind: "keyed",
    key: { kind: "header", name: "reading_id" }
  })
})

void test("given_a_keyed_projection_without_a_delete_header_when_encoded_then_should_omit_it_and_validate", () => {
  const key = { kind: "header", name: "reading_id" } as const
  const projection = new ProjectionBuilder("latest_readings").record().keyed(key).build()
  validateProjection(projection)
  const history = encodeProjectionHistory(projection.history)
  assert.equal(history.has("delete_header"), false)
  assert.deepEqual(decodeProjectionHistory(history, "history"), { kind: "keyed", key })
  assert.throws(() => {
    validateProjection(new ProjectionBuilder("latest_readings").record().keyed(key, "").build())
  }, InvalidError)
})

void test("given_a_json_schema_dialect_with_trailing_hashes_when_validated_then_should_accept_it_and_refuse_other_dialects", () => {
  const dialect = "https://json-schema.org/draft/2020-12/schema"
  const hashes = "#".repeat(50_000)
  const accepts = (value: string): void => {
    validateSchemaSource({ kind: "jsonSchema", schema: JSON.stringify({ $schema: value }) })
  }
  accepts(dialect)
  accepts(`${dialect}#`)
  accepts(dialect + hashes)
  const started = performance.now()
  assert.throws(() => {
    accepts(`${dialect}${hashes}x`)
  }, InvalidError)
  assert.throws(() => {
    accepts("https://json-schema.org/draft-07/schema#")
  }, InvalidError)
  assert.ok(performance.now() - started < 1_000, "the dialect check stays linear in the hash run")
})

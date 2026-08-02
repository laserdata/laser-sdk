import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  decodeControlEnvelope,
  decodeRetentionPolicy,
  decodeSchemaSource,
  encodeControlEnvelope,
  parseProjectionId,
  projectionKindFromCode
} from "../../src/wire/control.js"

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
  assert.equal(envelope.command.projection.id, "order.v1")
  assert.equal(envelope.command.projection.extraction.fields.length, 3)
})

void test("given_the_binding_control_fixtures_when_decoded_then_should_preserve_routing", async () => {
  const applied = await assertControlRoundTrips("control_apply_binding.bin")
  if (applied.command.kind !== "applyBinding") throw new Error("wrong command")
  assert.equal(applied.command.binding.targets[0]?.table, "orders_rows")
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
  assert.equal(parseProjectionId("order.v1"), "order.v1")
  assert.throws(() => parseProjectionId(""))
})

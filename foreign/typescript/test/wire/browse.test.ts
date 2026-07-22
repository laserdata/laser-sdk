import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  decodeBrowseReply,
  decodeDecodeRecord,
  decodeRegisterSchema,
  encodeBrowseReply,
  encodeDecodeRecord,
  encodeRegisterSchema
} from "../../src/wire/browse.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

async function assertBrowseReplyRoundTrips(
  name: string
): Promise<ReturnType<typeof decodeBrowseReply>> {
  const bytes = await readFixture(name)
  const reply = decodeBrowseReply(decodeOne(bytes, name), name)
  assert.deepEqual(Buffer.from(encodeNamed(encodeBrowseReply(reply))), Buffer.from(bytes))
  return reply
}

void test("given_the_projection_browse_fixture_when_decoded_then_should_preserve_bindings", async () => {
  const reply = await assertBrowseReplyRoundTrips("browse_reply_projections.bin")
  if (reply.kind !== "ok" || reply.outcome.kind !== "projections") throw new Error("wrong reply")
  const [projection] = reply.outcome.projections
  assert.ok(projection !== undefined)
  const [binding] = projection.bindings
  assert.ok(binding !== undefined)
  const [target] = binding.targets
  assert.ok(target !== undefined)
  assert.equal(projection.projection.id, "order.v1")
  assert.equal(target.table, "orders_rows")
})

void test("given_the_schema_browse_fixture_when_decoded_then_should_preserve_lifecycle", async () => {
  const reply = await assertBrowseReplyRoundTrips("browse_reply_schemas.bin")
  if (reply.kind !== "ok" || reply.outcome.kind !== "schemas") throw new Error("wrong reply")
  assert.equal(reply.outcome.schemas.length, 3)
  assert.equal(reply.outcome.schemas[1]?.dropped, true)
})

void test("given_the_registered_schema_reply_when_decoded_then_should_preserve_the_allocated_id", async () => {
  const reply = await assertBrowseReplyRoundTrips("browse_reply_schema_registered.bin")
  assert.deepEqual(reply, { kind: "ok", outcome: { kind: "schemaRegistered", id: 7 } })
})

void test("given_the_decoded_record_reply_when_decoded_then_should_preserve_arbitrary_json", async () => {
  const reply = await assertBrowseReplyRoundTrips("browse_reply_decoded.bin")
  if (reply.kind !== "ok" || reply.outcome.kind !== "decoded") throw new Error("wrong reply")
  assert.ok(reply.outcome.value instanceof Map)
})

void test("given_the_managed_schema_registration_fixture_when_decoded_then_should_preserve_metadata", async () => {
  const bytes = await readFixture("register_schema_managed.bin")
  const request = decodeRegisterSchema(
    expectMap(decodeOne(bytes, "register_schema_managed"), "register_schema_managed"),
    "register_schema_managed"
  )
  assert.equal(request.source.kind, "avro")
  assert.equal(request.name, "fills")
  assert.equal(request.version, 1)
  assert.deepEqual(Buffer.from(encodeNamed(encodeRegisterSchema(request))), Buffer.from(bytes))
})

void test("given_the_decode_record_fixture_when_decoded_then_should_preserve_payload_bytes", async () => {
  const bytes = await readFixture("decode_record.bin")
  const request = decodeDecodeRecord(
    expectMap(decodeOne(bytes, "decode_record"), "decode_record"),
    "decode_record"
  )
  assert.equal(request.id, 7)
  assert.deepEqual(Buffer.from(request.payload), Buffer.from([0xff, 0, 0x10]))
  assert.deepEqual(Buffer.from(encodeNamed(encodeDecodeRecord(request))), Buffer.from(bytes))
})

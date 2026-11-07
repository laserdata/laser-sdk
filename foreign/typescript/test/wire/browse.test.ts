import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  type BindingStatus,
  decodeGetProjection,
  encodeGetProjection,
  bindingStatusCaughtUpTo,
  bindingStatusHistoryComplete,
  bindingStatusIsFailed,
  decodeBindingStatus,
  decodeBrowseReply,
  decodeDecodeRecord,
  decodeListProjections,
  decodeListSchemas,
  decodeRegisterSchema,
  encodeBindingStatus,
  encodeBrowseReply,
  encodeDecodeRecord,
  encodeListProjections,
  encodeListSchemas,
  encodeRegisterSchema,
  type BrowseReply
} from "../../src/wire/browse.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { describeQueryError } from "../../src/wire/query.js"

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
  const { page, totalPages, totalResults } = reply.outcome.page
  assert.deepEqual([page, totalPages, totalResults], [2, 3, 21])
  const [projection] = reply.outcome.page.items
  assert.ok(projection !== undefined)
  const [binding] = projection.bindings
  assert.ok(binding !== undefined)
  assert.equal(projection.projection.id, "reading.v1")
  assert.equal(binding.index, "readings_rows")
})

void test("given_the_schema_browse_fixture_when_decoded_then_should_preserve_lifecycle", async () => {
  const reply = await assertBrowseReplyRoundTrips("browse_reply_schemas.bin")
  if (reply.kind !== "ok" || reply.outcome.kind !== "schemas") throw new Error("wrong reply")
  assert.equal(reply.outcome.page.items.length, 3)
  assert.equal(reply.outcome.page.items[1]?.dropped, true)
  assert.equal(reply.outcome.page.totalResults, 3)
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

void test("given_materialization_status_fixtures_when_decoded_then_should_preserve_progress_and_refuse_unknown_history", async () => {
  const bytes = await readFixture("browse_get_projection_status.bin")
  const request = decodeGetProjection(expectMap(decodeOne(bytes, "status"), "status"), "status")
  assert.equal(request.status, true)
  assert.deepEqual(Buffer.from(encodeNamed(encodeGetProjection(request))), Buffer.from(bytes))
  const reply = await assertBrowseReplyRoundTrips("browse_reply_projection_status.bin")
  assert.equal(reply.kind, "ok")
  if (reply.outcome.kind !== "projection") throw new Error("projection reply")
  const status = reply.outcome.projection?.status[0]
  assert.ok(status)
  const realized = {
    ...status,
    realization: "realized" as const,
    holds: [],
    sourceIncarnation: 7n,
    acceptedGaps: [],
    skippedRecords: 0n,
    partitions: [{ partitionId: 0, headOffset: 12n, appliedOffset: 12n, firstOffset: 0n }]
  }
  assert.equal(bindingStatusCaughtUpTo(realized, realized), true)
  assert.equal(bindingStatusHistoryComplete(realized, "block"), true)
  assert.equal(
    bindingStatusHistoryComplete(
      { ...realized, partitions: [{ partitionId: 0, appliedOffset: 12n }] },
      "block"
    ),
    false
  )
  assert.equal(bindingStatusCaughtUpTo({ ...realized, sourceIncarnation: 8n }, realized), false)
  assert.equal(
    bindingStatusCaughtUpTo(
      { ...realized, holds: [{ partitionId: 0, offset: 13n, cause: "schema", reason: "invalid" }] },
      realized
    ),
    false
  )
})

void test("given_the_stalled_status_fixture_when_decoded_then_should_preserve_the_failure_stall_and_retired_hold", async () => {
  const bytes = await readFixture("browse_binding_status_stalled.bin")
  const status = decodeBindingStatus(expectMap(decodeOne(bytes, "stalled"), "stalled"), "stalled")
  assert.equal(status.failure, "schema")
  assert.equal(status.stall, "backpressure")
  assert.equal(status.holds[0]?.cause, "retired_incarnation")
  assert.equal(bindingStatusIsFailed(status), true)
  assert.equal(bindingStatusIsFailed({ ...status, failure: "transient" }), false)
  assert.deepEqual(Buffer.from(encodeNamed(encodeBindingStatus(status))), Buffer.from(bytes))
  const newer = decodeBindingStatus(
    new Map<string, unknown>([
      [
        "source",
        new Map([
          ["stream", "fleet"],
          ["topic", "readings"]
        ])
      ],
      ["index", "readings_rows"],
      ["realization", "pending"],
      ["failure", "quota"],
      ["stall", "paused"]
    ]),
    "newer"
  )
  assert.deepEqual([newer.failure, newer.stall], ["unknown", "unknown"])
})

void test("given_the_paged_listing_fixture_when_decoded_then_should_preserve_kinds_and_page", async () => {
  const bytes = await readFixture("browse_list_projections_page.bin")
  const request = decodeListProjections(
    expectMap(decodeOne(bytes, "listing"), "listing"),
    "listing"
  )
  assert.deepEqual(request.kinds, [{ kind: "graph" }])
  assert.deepEqual([request.page, request.results], [2, 25])
  assert.deepEqual(Buffer.from(encodeNamed(encodeListProjections(request))), Buffer.from(bytes))
})

void test("given_a_partition_whose_records_expired_unread_when_checked_then_should_not_claim_complete_history", () => {
  const complete: BindingStatus = {
    history: { kind: "append" },
    source: { stream: "fleet", topic: "readings" },
    index: "readings_rows",
    realization: "realized",
    sourceIncarnation: 7n,
    partitions: [{ partitionId: 0, headOffset: 3n, appliedOffset: 3n, firstOffset: 0n }],
    holds: [],
    acceptedGaps: [],
    skippedRecords: 0n
  }
  assert.equal(bindingStatusHistoryComplete(complete, "block"), true)
  const expired = {
    ...complete,
    partitions: [...complete.partitions, { partitionId: 1, firstOffset: 5n }]
  }
  assert.equal(bindingStatusHistoryComplete(expired, "block"), false)
  const neverWritten = { ...complete, partitions: [...complete.partitions, { partitionId: 1 }] }
  assert.equal(bindingStatusHistoryComplete(neverWritten, "block"), true)
})

void test("given_a_paged_schema_list_and_a_missing_schema_when_round_tripped_then_should_keep_the_page_and_the_id", () => {
  const request = { v: 1, nameContains: "order", stream: "acme", page: 2, results: 25 }
  const encoded = encodeNamed(encodeListSchemas(request))
  assert.deepEqual(
    decodeListSchemas(expectMap(decodeOne(encoded, "list"), "list"), "list"),
    request
  )
  const missing: BrowseReply = { kind: "err", error: { kind: "schema_not_found", id: 7 } }
  const bytes = encodeNamed(encodeBrowseReply(missing))
  assert.deepEqual(decodeBrowseReply(decodeOne(bytes, "reply"), "reply"), missing)
  assert.equal(
    describeQueryError({ kind: "schema_not_found", id: 7 }),
    "writer schema 7 is not registered"
  )
})

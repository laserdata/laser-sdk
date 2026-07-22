import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import {
  consistencyGateCheck,
  decodeFilter,
  decodeQueryEnvelope,
  decodeQueryReply,
  encodeQueryEnvelopeFrame,
  encodeQueryReplyFrame,
  pageAtLeast,
  pageTotalPages,
  type QueryEnvelope,
  type QueryReply
} from "../../src/wire/query.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

async function assertEnvelopeRoundTrips(
  name: string,
  assertEnvelope: (envelope: QueryEnvelope) => void
): Promise<void> {
  const bytes = await readFixture(name)
  const envelope = decodeQueryEnvelope(expectMap(decodeOne(bytes, name), name), name)
  assertEnvelope(envelope)
  assert.deepEqual(Buffer.from(encodeQueryEnvelopeFrame(envelope)), Buffer.from(bytes))
}

async function assertReplyRoundTrips(
  name: string,
  assertReply: (reply: QueryReply) => void
): Promise<void> {
  const bytes = await readFixture(name)
  const reply = decodeQueryReply(decodeOne(bytes, name), name)
  assertReply(reply)
  assert.deepEqual(Buffer.from(encodeQueryReplyFrame(reply)), Buffer.from(bytes))
}

void test("given_the_query_envelope_fixture_when_decoded_then_should_preserve_the_full_dsl", async () => {
  await assertEnvelopeRoundTrips("query_envelope.bin", (envelope) => {
    const { query } = envelope
    assert.equal(query.index, "orders")
    assert.deepEqual(query.byKey, [{ field: "customer_id", value: "alice" }])
    assert.equal(query.messageType, "order_created")
    assert.deepEqual(query.timeRange, [1000n, 2000n])
    assert.equal(query.filter?.kind, "all")
    const vector = query.vector
    assert.ok(vector !== undefined)
    assert.deepEqual(vector.embedding, [0.25, -0.5, 0.125])
    assert.equal(vector.topK, 5)
    assert.deepEqual(query.order, [{ field: "ts", dir: "desc" }])
    assert.equal(query.limit, 20)
    assert.equal(query.offset, 40)
    assert.equal(query.select.payload, true)
    assert.equal(query.fork, "agent-run-7")
  })
})

void test("given_the_aggregate_query_fixture_when_decoded_then_should_preserve_analytics", async () => {
  await assertEnvelopeRoundTrips("query_envelope_aggregate.bin", (envelope) => {
    const aggregate = envelope.query.aggregate
    assert.ok(aggregate !== undefined)
    assert.deepEqual(aggregate.groupBy, ["route"])
    assert.deepEqual(aggregate.funcs, [
      { func: "count", alias: "n" },
      { func: "percentile", field: "latency_ms", arg: 0.95, alias: "p95" }
    ])
    assert.deepEqual(aggregate.window, { field: "ts", everyMicros: 60_000_000n })
    assert.equal(envelope.query.having?.kind, "pred")
    assert.equal(envelope.query.distinct, true)
  })
})

void test("given_the_raw_sql_query_fixture_when_decoded_then_should_preserve_every_value_kind", async () => {
  await assertEnvelopeRoundTrips("query_envelope_raw_sql.bin", (envelope) => {
    assert.deepEqual(envelope.query.rawSql?.params, [
      { kind: "int", value: 100n },
      { kind: "int", value: (1n << 64n) - 1n },
      { kind: "float", value: 0.5 },
      { kind: "bool", value: true },
      { kind: "string", value: "x" },
      { kind: "null" },
      {
        kind: "list",
        value: [
          { kind: "int", value: 1n },
          { kind: "int", value: 2n }
        ]
      }
    ])
  })
})

void test("given_a_read_your_writes_query_when_decoded_then_should_preserve_consistency", async () => {
  await assertEnvelopeRoundTrips("query_envelope_read_your_writes.bin", (envelope) => {
    assert.equal(envelope.query.consistency, "readYourWrites")
  })
})

void test("given_a_text_query_when_decoded_then_should_preserve_field_and_text", async () => {
  await assertEnvelopeRoundTrips("query_envelope_text.bin", (envelope) => {
    assert.deepEqual(envelope.query.text, { field: "summary", query: "refund dispute" })
  })
})

void test("given_an_ok_query_reply_when_decoded_then_should_preserve_rows_and_page", async () => {
  await assertReplyRoundTrips("query_reply_ok.bin", (reply) => {
    if (reply.kind !== "ok") throw new Error("expected an ok query reply")
    assert.equal(reply.result.rows.length, 1)
    const row = reply.result.rows[0]
    assert.ok(row !== undefined)
    assert.deepEqual(Array.from(row.headers), [
      ["amount", "42"],
      ["customer", "alice"]
    ])
    assert.deepEqual(Array.from(row.metadata), [["agdx.ct", "1"]])
    assert.deepEqual(
      {
        partition: row.partition,
        offset: row.offset,
        stream: row.stream,
        topic: row.topic,
        score: row.score
      },
      { partition: 2, offset: 17n, stream: 5, topic: 3, score: 0.5 }
    )
    assert.deepEqual(Buffer.from(row.payload ?? []), Buffer.from('{"total":42}'))
    assert.deepEqual(reply.result.page, { offset: 0, limit: 50, total: 1n, hasMore: false })
  })
})

void test("given_a_too_large_query_reply_when_decoded_then_should_preserve_the_bound", async () => {
  await assertReplyRoundTrips("query_reply_err_too_large.bin", (reply) => {
    assert.deepEqual(reply, {
      kind: "err",
      error: { kind: "tooLarge", what: "limit", size: 2000, cap: 1000 }
    })
  })
})

void test("given_a_stale_query_reply_when_decoded_then_should_preserve_offsets", async () => {
  await assertReplyRoundTrips("query_reply_err_stale.bin", (reply) => {
    assert.deepEqual(reply, {
      kind: "err",
      error: { kind: "stale", what: "orders", applied: 41n, required: 57n }
    })
  })
})

void test("given_an_unrecognized_filter_tag_when_decoded_then_should_throw", () => {
  assert.throws(() => decodeFilter(new Map([["future", []]]), "filter"))
})

void test("given_a_consistency_gate_when_checked_then_should_fail_without_downgrading", () => {
  assert.deepEqual(consistencyGateCheck({ applied: 0n, required: 100n }, "eventual", "orders"), {
    ok: true
  })
  assert.deepEqual(
    consistencyGateCheck({ applied: 100n, required: 100n }, "readYourWrites", "orders"),
    { ok: true }
  )
  assert.deepEqual(consistencyGateCheck({ applied: 41n, required: 57n }, "strong", "orders"), {
    ok: false,
    error: { kind: "stale", what: "orders", applied: 41n, required: 57n }
  })
})

void test("given_page_metadata_when_inspected_then_should_compute_bounds_without_fabricating_totals", () => {
  assert.equal(pageAtLeast({ offset: 40, limit: 20, hasMore: true }, 20), 60)
  assert.equal(pageTotalPages({ offset: 0, limit: 3, total: 10n, hasMore: true }), 4n)
  assert.equal(pageTotalPages({ offset: 0, limit: 0, total: 10n, hasMore: false }), undefined)
  assert.equal(pageTotalPages({ offset: 0, limit: 3, hasMore: false }), undefined)
})

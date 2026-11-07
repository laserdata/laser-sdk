import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import test from "node:test"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import { QUERY_OP_VERSION } from "../../src/wire/codes.js"
import { BackendResourceId, QueryExecutionId } from "../../src/wire/ids.js"
import {
  consistencyGateCheck,
  consistencyIsEventual,
  decodeQueryEnvelope,
  decodeQueryResult,
  describeQueryError,
  decodeQueryEnvelopeFrame,
  encodeQueryEnvelopeFrame,
  encodeQueryResult,
  newQuery,
  operationalQuery,
  operationalTarget,
  pageAtLeast,
  pageTotalPages,
  queryResultFieldIndex,
  queryResultValue,
  queryResultValueI64,
  queryResultValueText,
  queryResultValueU64,
  validateQuery,
  validateQueryResult,
  type QueryResult
} from "../../src/wire/query.js"

void test("query envelope round trips the breaking target and typed-value contract", () => {
  const query = {
    ...newQuery(operationalTarget("readings"), QueryExecutionId.fromU128(1n), 10_000n),
    byKey: [{ field: "region", value: { kind: "string" as const, value: "eu-west" } }],
    page: { limit: 20, offset: 40n, wantTotal: true }
  }
  validateQuery(query)
  const bytes = encodeQueryEnvelopeFrame({ v: QUERY_OP_VERSION, query })
  const decoded = decodeQueryEnvelopeFrame(bytes)
  assert.deepEqual(decoded, { v: QUERY_OP_VERSION, query })
  assert.deepEqual(
    decodeQueryEnvelope(expectMap(decodeOne(bytes, "query"), "query"), "query"),
    decoded
  )
})

void test("typed result rows are positional and preserve non-string values", () => {
  const executionId = QueryExecutionId.fromU128(2n)
  const result: QueryResult = {
    fields: [
      { id: 1, name: "cpu", required: true, fieldType: { kind: "long" } },
      { id: 2, name: "payload", required: true, fieldType: { kind: "binary" } }
    ],
    rows: [
      {
        values: [
          { kind: "long", value: 42n },
          { kind: "binary", value: Uint8Array.of(1, 2) }
        ]
      }
    ],
    page: { offset: 0n, limit: 50, hasMore: false },
    context: {
      executionId,
      engine: { name: "datafusion", version: "50" },
      resolvedTarget: {
        kind: "operational",
        index: "readings",
        backendResourceId: BackendResourceId.fromU128(3n),
        backendGeneration: 1n,
        runtimeConfigurationRevision: 1n
      },
      requestedConsistency: "eventual",
      deliveredConsistency: "eventual",
      truncated: false,
      elapsedMicros: 1n,
      scannedBytes: 2n,
      producedBytes: 3n,
      rowCount: 1n,
      frontier: []
    }
  }
  const row = result.rows[0]
  assert.ok(row !== undefined)
  assert.deepEqual(queryResultValue(result, row, "cpu"), {
    kind: "long",
    value: 42n
  })
  assert.equal(queryResultFieldIndex(result, "payload"), 1)
  assert.equal(queryResultFieldIndex(result, "absent"), undefined)
  assert.equal(queryResultValueText(result, row, "cpu"), "42")
  assert.equal(queryResultValueText(result, row, "payload"), "0x0102")
  assert.equal(queryResultValueU64(result, row, "cpu"), 42n)
  assert.equal(queryResultValueI64(result, row, "cpu"), 42n)
  assert.equal(queryResultValueI64(result, row, "payload"), undefined)
  validateQueryResult(result)
})

void test("given_a_query_reply_with_a_frontier_when_round_tripped_then_should_keep_each_position", () => {
  const executionId = QueryExecutionId.fromU128(9n)
  const result: QueryResult = {
    fields: [{ id: 1, name: "cpu", required: true, fieldType: { kind: "long" } }],
    rows: [],
    page: { offset: 0n, limit: 50, hasMore: false },
    context: {
      executionId,
      engine: { name: "embedded", version: "1" },
      resolvedTarget: {
        kind: "operational",
        index: "readings",
        backendResourceId: BackendResourceId.fromU128(3n),
        backendGeneration: 1n,
        runtimeConfigurationRevision: 1n
      },
      requestedConsistency: "eventual",
      deliveredConsistency: "eventual",
      truncated: false,
      elapsedMicros: 1n,
      scannedBytes: 0n,
      producedBytes: 0n,
      rowCount: 0n,
      frontier: [
        {
          stream: "telemetry",
          topic: "readings",
          partitionId: 2,
          topicCreatedAtMicros: 77n,
          appliedOffset: 41n
        },
        { stream: "telemetry", topic: "readings", partitionId: 3, appliedOffset: 5n }
      ]
    }
  }
  const decoded = decodeQueryResult(encodeQueryResult(result), "result")
  assert.deepEqual(decoded.context.frontier, result.context.frontier)
  const plain = decodeQueryResult(
    encodeQueryResult({ ...result, context: { ...result.context, frontier: [] } }),
    "result"
  )
  assert.deepEqual(plain.context.frontier, [])
})

void test("query validation rejects ambiguous requests and malformed replies", () => {
  const query = {
    ...newQuery(operationalTarget("readings"), QueryExecutionId.fromU128(1n), 10_000n),
    rawSql: { dialect: "postgres" as const, sql: "SELECT * FROM readings", params: [] },
    select: { fields: ["id"], payload: false }
  }
  assert.throws(() => {
    validateQuery(query)
  }, /raw SQL cannot be combined/)

  const executionId = QueryExecutionId.fromU128(2n)
  const invalid: QueryResult = {
    fields: [{ id: 1, name: "id", required: true, fieldType: { kind: "long" } }],
    rows: [{ values: [] }],
    page: { limit: 50, hasMore: true },
    context: {
      executionId,
      engine: { name: "embedded", version: "1" },
      resolvedTarget: {
        kind: "operational",
        index: "readings",
        backendResourceId: BackendResourceId.fromU128(3n),
        backendGeneration: 1n,
        runtimeConfigurationRevision: 1n
      },
      requestedConsistency: "read_your_writes",
      deliveredConsistency: "eventual",
      truncated: false,
      elapsedMicros: 1n,
      scannedBytes: 1n,
      producedBytes: 1n,
      rowCount: 0n,
      frontier: []
    }
  }
  assert.throws(() => {
    validateQueryResult(invalid)
  }, /row value count/)
  assert.throws(() => {
    validateQueryResult({ ...invalid, rows: [], page: { limit: 50, hasMore: true } })
  }, /delivered consistency/)
})

void test("consistency and page helpers do not fabricate stronger reads or totals", () => {
  assert.equal(consistencyGateCheck(100n, 100n, "read_your_writes", "readings"), undefined)
  assert.deepEqual(consistencyGateCheck(41n, 57n, "read_your_writes", "readings"), {
    kind: "stale",
    what: "readings",
    applied: 41n,
    required: 57n
  })
  assert.equal(pageAtLeast({ offset: 40n, limit: 20, hasMore: false }, 20), 60n)
  assert.equal(pageTotalPages({ offset: 0n, limit: 3, total: 10n, hasMore: false }), 4n)
  assert.equal(pageAtLeast({ limit: 20, hasMore: false }, 20), undefined)
  assert.equal(pageAtLeast({ offset: (1n << 64n) - 1n, limit: 20, hasMore: false }, 1), undefined)
  assert.ok(consistencyIsEventual("eventual"))
  assert.equal(consistencyIsEventual("read_your_writes"), false)
})

void test("given_an_operational_query_when_built_then_should_target_the_index", () => {
  const executionId = QueryExecutionId.fromU128(9n)
  assert.deepEqual(
    operationalQuery(executionId, "readings", 5n),
    newQuery(operationalTarget("readings"), executionId, 5n)
  )
})

// Rust decodes an integer field only from a CBOR integer and a float field only
// from a CBOR float, so a re-encoded Rust frame must keep every byte.
void test("given_rust_query_envelope_fixtures_when_re_encoded_then_should_be_byte_identical", async () => {
  const fixtures = path.resolve(process.cwd(), "../../wire/fixtures")
  for (const name of [
    "query_envelope.bin",
    "query_envelope_aggregate.bin",
    "query_envelope_raw_sql.bin",
    "query_envelope_read_your_writes.bin",
    "query_envelope_text.bin"
  ]) {
    const bytes = new Uint8Array(await readFile(path.join(fixtures, name)))
    const reencoded = encodeQueryEnvelopeFrame(decodeQueryEnvelopeFrame(bytes))
    assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes), name)
  }
})

void test("given_a_query_with_integral_floats_when_encoded_then_should_keep_integers_and_floats_apart", () => {
  const query = {
    ...newQuery(operationalTarget("readings"), QueryExecutionId.fromU128(1n), 10_000n),
    byKey: [{ field: "cpu", value: { kind: "double" as const, value: 2 } }],
    vector: { field: "vec", embedding: [1, 0.5], topK: 3 }
  }
  const bytes = Buffer.from(encodeQueryEnvelopeFrame({ v: QUERY_OP_VERSION, query }))
  const after = (key: string): number => {
    const encoded = Buffer.concat([Buffer.from([0x60 + key.length]), Buffer.from(key)])
    const at = bytes.indexOf(encoded)
    assert.ok(at >= 0, key)
    return bytes[at + encoded.length] ?? -1
  }
  // map(2), "v", unsigned 1
  assert.deepEqual([...bytes.subarray(0, 4)], [0xa2, 0x61, 0x76, 0x01])
  assert.equal(after("top_k"), 0x03)
  assert.equal(after("limit"), 0x18)
  assert.equal(after("embedding"), 0x82)
  assert.equal(bytes[bytes.indexOf(Buffer.from("embedding")) + 10], 0xf9)
  const double = bytes.indexOf(Buffer.from("double"))
  assert.equal(bytes[bytes.indexOf(Buffer.from("value"), double) + 5], 0xf9)
})

void test("given_a_query_with_more_than_one_shape_when_validated_then_should_refuse_it", () => {
  const base = newQuery(operationalTarget("docs"), QueryExecutionId.fromU128(1n), 10_000n)
  const vector = { field: "embedding", embedding: [0.1, 0.2], topK: 10 }
  const text = { query: "incident" }
  const aggregate = { groupBy: [], funcs: [{ func: "count" as const, alias: "n" }] }
  for (const query of [
    { ...base, vector, aggregate },
    { ...base, text, vector },
    { ...base, text, distinct: true },
    { ...base, aggregate, distinct: true }
  ]) {
    assert.throws(() => {
      validateQuery(query)
    }, /at most one of text, vector, aggregate and distinct/)
  }
  validateQuery({ ...base, vector })
})

void test("given_query_errors_when_described_then_should_carry_the_server_text_like_rust", () => {
  assert.equal(
    describeQueryError({ kind: "invalid", message: "query limit 0 is outside 1..=1000" }),
    "invalid query: query limit 0 is outside 1..=1000"
  )
  assert.equal(
    describeQueryError({ kind: "stale", what: "shop/orders#0", applied: 3n, required: 5n }),
    "stale read: shop/orders#0 applied 3, required 5"
  )
  assert.equal(
    describeQueryError({ kind: "too_large", what: "offset", size: 2_000_000n, cap: 1_000_000n }),
    "result too large: offset 2000000 exceeds cap 1000000"
  )
})

void test("given_a_time_range_of_one_instant_when_validated_then_should_accept_it", () => {
  const base = newQuery(operationalTarget("docs"), QueryExecutionId.fromU128(1n), 10_000n)
  validateQuery({ ...base, timeRange: [2n, 2n] })
  assert.throws(() => {
    validateQuery({ ...base, timeRange: [3n, 2n] })
  }, /must not be after its end/)
})

void test("given_a_limit_or_top_k_that_is_not_a_whole_number_when_validated_then_should_refuse_it", () => {
  const base = newQuery(operationalTarget("docs"), QueryExecutionId.fromU128(1n), 10_000n)
  for (const limit of [Number.NaN, 2.5]) {
    assert.throws(() => {
      validateQuery({ ...base, page: { ...base.page, limit } })
    }, /page limit is invalid/)
    assert.throws(() => {
      validateQuery({ ...base, vector: { field: "embedding", embedding: [0.1], topK: limit } })
    }, /vector query is invalid/)
  }
})

void test("given_a_ranked_query_with_paging_it_cannot_honour_when_validated_then_should_refuse_it", () => {
  const base = newQuery(operationalTarget("docs"), QueryExecutionId.fromU128(1n), 10_000n)
  const vector = { field: "embedding", embedding: [0.1, 0.2], topK: 10 }
  const text = { query: "incident" }
  const order = [{ field: "ts", dir: "desc" as const }]
  const total = { ...base.page, wantTotal: true }
  for (const query of [
    { ...base, text, order },
    { ...base, text, page: total },
    { ...base, vector, order },
    { ...base, vector, page: total },
    { ...base, vector, page: { ...base.page, offset: 10n } }
  ]) {
    assert.throws(() => {
      validateQuery(query)
    }, /takes no/)
  }
  validateQuery({ ...base, text, page: { ...base.page, offset: 50n } })
})

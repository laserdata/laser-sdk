import assert from "node:assert/strict"
import test from "node:test"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import { QUERY_OP_VERSION } from "../../src/wire/codes.js"
import { BackendResourceId, QueryExecutionId } from "../../src/wire/ids.js"
import {
  consistencyGateCheck,
  decodeQueryEnvelope,
  decodeQueryEnvelopeFrame,
  encodeQueryEnvelopeFrame,
  newQuery,
  operationalTarget,
  pageAtLeast,
  pageTotalPages,
  queryResultValue,
  validateQuery,
  validateQueryExecutionStatus,
  validateQueryResult,
  type QueryResult
} from "../../src/wire/query.js"

void test("query envelope round trips the breaking target and typed-value contract", () => {
  const query = {
    ...newQuery(operationalTarget("orders"), QueryExecutionId.fromU128(1n), 10_000n),
    byKey: [{ field: "tenant", value: { kind: "string" as const, value: "acme" } }],
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
      { id: 1, name: "amount", required: true, fieldType: { kind: "long" } },
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
        index: "orders",
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
      rowCount: 1n
    }
  }
  const row = result.rows[0]
  assert.ok(row !== undefined)
  assert.deepEqual(queryResultValue(result, row, "amount"), {
    kind: "long",
    value: 42n
  })
  validateQueryResult(result)
})

void test("query validation rejects ambiguous requests and malformed replies", () => {
  const query = {
    ...newQuery(operationalTarget("orders"), QueryExecutionId.fromU128(1n), 10_000n),
    rawSql: { dialect: "data_fusion" as const, sql: "SELECT * FROM orders", params: [] },
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
        index: "orders",
        backendResourceId: BackendResourceId.fromU128(3n),
        backendGeneration: 1n,
        runtimeConfigurationRevision: 1n
      },
      requestedConsistency: "strong",
      deliveredConsistency: "eventual",
      truncated: false,
      elapsedMicros: 1n,
      scannedBytes: 1n,
      producedBytes: 1n,
      rowCount: 0n
    }
  }
  assert.throws(() => {
    validateQueryResult(invalid)
  }, /row value count/)
  assert.throws(() => {
    validateQueryResult({ ...invalid, rows: [], page: { limit: 50, hasMore: true } })
  }, /delivered consistency|has_more/)
  assert.throws(() => {
    validateQueryExecutionStatus({
      executionId,
      state: "failed",
      startedAtMicros: 10n,
      finishedAtMicros: 9n,
      scannedBytes: 0n,
      producedBytes: 0n,
      rowCount: 0n
    })
  }, /finish time/)
})

void test("consistency and page helpers do not fabricate stronger reads or totals", () => {
  assert.equal(consistencyGateCheck(100n, 100n, "read_your_writes", "orders"), undefined)
  assert.deepEqual(consistencyGateCheck(41n, 57n, "strong", "orders"), {
    kind: "stale",
    what: "orders",
    applied: 41n,
    required: 57n
  })
  assert.equal(pageAtLeast({ offset: 40n, limit: 20, hasMore: false }, 20), 60n)
  assert.equal(pageTotalPages({ offset: 0n, limit: 3, total: 10n, hasMore: false }), 4n)
})

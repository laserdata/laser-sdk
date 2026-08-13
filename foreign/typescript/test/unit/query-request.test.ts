import assert from "node:assert/strict"
import test from "node:test"
import { QueryRequest } from "../../src/managed/query.js"
import { BackendResourceId, QueryExecutionId } from "../../src/wire/ids.js"
import type { Query, QueryResult } from "../../src/wire/query.js"

function result(query: Query, cursor?: string): QueryResult {
  return {
    fields: [{ id: 1, name: "id", required: true, fieldType: { kind: "string" } }],
    rows: [{ values: [{ kind: "string", value: "o-7" }] }],
    page: {
      offset: 0n,
      limit: query.page.limit,
      hasMore: cursor !== undefined,
      ...(cursor === undefined ? {} : { nextCursor: cursor })
    },
    context: {
      executionId: query.executionId,
      engine: { name: "embedded", version: "1" },
      resolvedTarget: {
        kind: "operational",
        index: "orders",
        backendResourceId: BackendResourceId.fromU128(1n),
        backendGeneration: 1n,
        runtimeConfigurationRevision: 1n
      },
      requestedConsistency: query.consistency,
      deliveredConsistency: query.consistency,
      truncated: false,
      elapsedMicros: 1n,
      scannedBytes: 1n,
      producedBytes: 1n,
      rowCount: 1n
    }
  }
}

void test("query builder freezes target, paging, typed predicates, SQL dialect, and deadline", async () => {
  let observed: Query | undefined
  const request = new QueryRequest("orders", (query) => {
    observed = query
    return Promise.resolve(result(query))
  })
    .executionId(QueryExecutionId.fromU128(9n))
    .deadlineMicros(100n)
    .whereEq("tenant", "acme")
    .filterGte("amount", { kind: "long", value: 42n })
    .limit(20)
    .offset(40n)
    .readYourWrites()

  await request.fetch()
  assert.ok(observed !== undefined)
  assert.equal(observed.target.kind, "operational")
  assert.equal(observed.page.limit, 20)
  assert.equal(observed.page.offset, 40n)
  assert.equal(observed.consistency, "read_your_writes")
  assert.equal(observed.deadlineMicros, 100n)
})

void test("row iteration follows the opaque server cursor and honors its ceiling", async () => {
  const seen: Query[] = []
  const request = new QueryRequest("orders", (query) => {
    seen.push(query)
    return Promise.resolve(result(query, seen.length === 1 ? "next" : undefined))
  }).maxRows(2)
  const rows = []
  for await (const row of request.rows()) rows.push(row)
  assert.equal(rows.length, 2)
  assert.ok(seen[1] !== undefined)
  assert.equal(seen[1].page.cursor, "next")
  assert.equal(seen[1].page.offset, undefined)
})

void test("raw SQL carries an explicit dialect and typed parameters", () => {
  const query = new QueryRequest("orders", (value) => Promise.resolve(result(value)))
    .rawSql("SELECT 1 WHERE amount > ?", [{ kind: "long", value: 10n }], "data_fusion")
    .intoQuery()
  assert.ok(query.rawSql !== undefined)
  assert.equal(query.rawSql.dialect, "data_fusion")
  assert.deepEqual(query.rawSql.params, [{ kind: "long", value: 10n }])
})

void test("query replies and status remain bound to the requested execution identity", async () => {
  const executionId = QueryExecutionId.fromU128(9n)
  const wrongExecutionId = QueryExecutionId.fromU128(10n)
  const request = new QueryRequest(
    "orders",
    (query) =>
      Promise.resolve({
        ...result(query),
        context: { ...result(query).context, executionId: wrongExecutionId }
      }),
    () =>
      Promise.resolve({
        executionId: wrongExecutionId,
        state: "running",
        startedAtMicros: 1n,
        scannedBytes: 0n,
        producedBytes: 0n,
        rowCount: 0n
      })
  ).executionId(executionId)

  await assert.rejects(request.fetch(), /execution id does not match/)
  await assert.rejects(request.status(), /execution id does not match/)
})

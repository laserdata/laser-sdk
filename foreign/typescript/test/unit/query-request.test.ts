import assert from "node:assert/strict"
import test from "node:test"
import { ConfigError, ProtocolError } from "../../src/client/errors.js"
import { Json } from "../../src/stream/codecs.js"
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
        index: "readings",
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
  const request = QueryRequest.create("readings", (query) => {
    observed = query
    return Promise.resolve(result(query))
  })
    .deadlineMicros(100n)
    .whereEq("region", "eu-west")
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
  const request = QueryRequest.create("readings", (query) => {
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
  const query = QueryRequest.create("readings", (value) => Promise.resolve(result(value)))
    .rawSql("SELECT 1 WHERE amount > ?", [{ kind: "long", value: 10n }], "data_fusion")
    .intoQuery()
  assert.ok(query.rawSql !== undefined)
  assert.equal(query.rawSql.dialect, "data_fusion")
  assert.deepEqual(query.rawSql.params, [{ kind: "long", value: 10n }])
})

void test("query replies and status remain bound to the requested execution identity", async () => {
  const wrongExecutionId = QueryExecutionId.fromU128(10n)
  const request = QueryRequest.create(
    "readings",
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
  )

  await assert.rejects(request.fetch(), /execution id does not match/)
  await assert.rejects(request.status(), /execution id does not match/)
})

void test("given_an_execution_id_when_read_then_should_return_the_identity_the_request_carries", () => {
  const request = QueryRequest.create("readings", (query) => Promise.resolve(result(query)))
  const minted = request.executionId()
  assert.equal(request.intoQuery().executionId.asU128(), minted.asU128())
})

void test("given_an_empty_page_that_reports_more_when_walking_rows_then_should_stop", async () => {
  let calls = 0
  const request = QueryRequest.create("readings", (query) => {
    calls += 1
    return Promise.resolve({ ...result(query, "again"), rows: [] })
  }).maxRows(10)
  const rows = []
  for await (const row of request.rows()) rows.push(row)
  assert.equal(rows.length, 0)
  assert.equal(calls, 1)
})

void test("given_a_zero_page_limit_when_walking_rows_then_should_use_the_default_page_size", async () => {
  const seen: Query[] = []
  const request = QueryRequest.create("readings", (query) => {
    seen.push(query)
    return Promise.resolve(result(query))
  })
    .limit(0)
    .maxRows(1)
  const rows = []
  for await (const row of request.rows()) rows.push(row)
  assert.equal(rows.length, 1)
  assert.equal(seen[0]?.page.limit, 100)
})

void test("given_rows_without_a_usable_payload_when_decoded_then_should_raise_the_rust_error_classes", async () => {
  const withPayload = (query: Query, value: QueryResult["rows"][number]["values"][number]) => ({
    ...result(query),
    fields: [
      ...result(query).fields,
      {
        id: 2_000_000_012,
        name: "__laser_original_payload",
        required: false,
        fieldType: { kind: "binary" as const }
      }
    ],
    rows: [{ values: [{ kind: "string" as const, value: "o-7" }, value] }]
  })
  const fetchTyped = (answer: (query: Query) => QueryResult) =>
    QueryRequest.create("readings", (query) => Promise.resolve(answer(query))).fetchTyped(
      new Json()
    )
  await assert.rejects(fetchTyped(result), ConfigError)
  await assert.rejects(
    fetchTyped((query) => withPayload(query, { kind: "null" })),
    ConfigError
  )
  await assert.rejects(
    fetchTyped((query) => withPayload(query, { kind: "string", value: "{}" })),
    ProtocolError
  )
  await assert.rejects(
    fetchTyped((query) => ({ ...withPayload(query, { kind: "null" }), rows: [{ values: [] }] })),
    ProtocolError
  )
})

void test("given_a_repeated_cursor_when_walking_pages_then_should_end_the_walk", async () => {
  let calls = 0
  const request = QueryRequest.create("readings", (query) => {
    calls += 1
    return Promise.resolve(result(query, "same"))
  })
  const pages = []
  for await (const row of request.maxRows(10).rows()) pages.push(row)
  assert.equal(pages.length, 2)
  assert.equal(calls, 2)
})

void test("aggregates carry the Rust names and explicit aliases", async () => {
  let observed: Query | undefined
  await QueryRequest.create("readings", (query) => {
    observed = query
    return Promise.resolve(result(query))
  })
    .stddev("amount")
    .aggAs("percentile", "p95", { field: "amount", fraction: 0.95 })
    .fetch()
  assert.deepEqual(observed?.aggregate?.funcs, [
    { func: "std_dev", alias: "stddev", field: "amount" },
    { func: "percentile", alias: "p95", field: "amount", arg: 0.95 }
  ])
})

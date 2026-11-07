import assert from "node:assert/strict"
import test from "node:test"
import { ConfigError, InvalidError, ProtocolError } from "../../src/client/errors.js"
import { Json } from "../../src/stream/codecs.js"
import { QueryRequest, queryReplyWaitMs } from "../../src/managed/query.js"
import { QUERY_DEADLINE_MS, QUERY_REPLY_GRACE_MS } from "../../src/retry.js"
import { BackendResourceId, QueryExecutionId } from "../../src/wire/ids.js"
import {
  QueryBuilder,
  decodeQuery,
  encodeQuery,
  filterPred,
  newQuery,
  operationalTarget,
  validateQuery,
  type Query,
  type QueryResult
} from "../../src/wire/query.js"

function result(query: Query, hasMore = false): QueryResult {
  return {
    fields: [{ id: 1, name: "id", required: true, fieldType: { kind: "string" } }],
    rows: [{ values: [{ kind: "string", value: "o-7" }] }],
    page: {
      offset: query.page.offset ?? 0n,
      limit: query.page.limit,
      hasMore
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
      rowCount: 1n,
      frontier: []
    }
  }
}

void test("query builder freezes target, paging, typed predicates, SQL dialect, and deadline", async () => {
  let observed: Query | undefined
  const request = QueryRequest.create("readings", (query) => {
    observed = query
    return Promise.resolve(result(query))
  })
    .deadlineAtMicros(100n)
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

void test("given_a_window_in_milliseconds_when_bucketing_then_should_send_its_width_in_micros", async () => {
  let observed: Query | undefined
  const request = QueryRequest.create("readings", (query) => {
    observed = query
    return Promise.resolve(result(query))
  })
    .count()
    .window("ts", 1.5)
  await request.fetch()
  assert.equal(observed?.aggregate?.window?.everyMicros, 1_500n)
  assert.throws(() => request.window("ts", -1), InvalidError)
})

void test("given_more_rows_when_walking_then_should_page_by_offset_and_honor_the_ceiling", async () => {
  const seen: Query[] = []
  const request = QueryRequest.create("readings", (query) => {
    seen.push(query)
    return Promise.resolve(result(query, seen.length === 1))
  }).maxRows(2)
  const rows = []
  for await (const row of request.rows()) rows.push(row)
  assert.equal(rows.length, 2)
  assert.ok(seen[1] !== undefined)
  assert.equal(seen[1].page.offset, 1n)
})

void test("given_a_reused_request_when_fetched_later_then_should_send_a_fresh_deadline_and_execution_id", async (context) => {
  context.mock.timers.enable({ apis: ["Date"], now: 1_000_000 })
  const seen: Query[] = []
  const request = QueryRequest.create("readings", (query) => {
    seen.push(query)
    return Promise.resolve(result(query))
  }).deadline(5_000)
  await request.fetch()
  context.mock.timers.tick(60_000)
  await request.fetch()
  const [first, second] = seen
  assert.ok(first !== undefined && second !== undefined)
  assert.equal(first.deadlineMicros, 1_005_000_000n)
  assert.equal(second.deadlineMicros, 1_065_000_000n)
  assert.notEqual(second.executionId.asU128(), first.executionId.asU128())
})

void test("given_a_walk_when_paging_then_should_give_every_page_its_own_deadline_and_execution_id", async (context) => {
  context.mock.timers.enable({ apis: ["Date"], now: 1_000_000 })
  const seen: Query[] = []
  const request = QueryRequest.create("readings", (query) => {
    seen.push(query)
    context.mock.timers.tick(10_000)
    return Promise.resolve(result(query, seen.length < 3))
  })
  await request.fetchAll()
  const budget = BigInt(QUERY_DEADLINE_MS) * 1000n
  assert.deepEqual(
    seen.map((query) => query.deadlineMicros),
    [1_000_000_000n + budget, 1_010_000_000n + budget, 1_020_000_000n + budget]
  )
  assert.equal(new Set(seen.map((query) => query.executionId.asU128())).size, 3)
})

void test("given_a_typed_terminal_when_run_then_should_leave_the_request_unchanged", async () => {
  const seen: Query[] = []
  const request = QueryRequest.create("readings", (query) => {
    seen.push(query)
    return Promise.resolve(result(query))
  }).limit(20)
  await assert.rejects(request.fetchOne(new Json()), ConfigError)
  await request.fetch()
  const [one, plain] = seen
  assert.ok(one !== undefined && plain !== undefined)
  assert.equal(one.page.limit, 1)
  assert.equal(one.select.payload, true)
  assert.equal(plain.page.limit, 20)
  assert.equal(plain.select.payload, false)
})

void test("given_native_filter_values_when_set_then_should_infer_the_tag_like_rust_and_python", () => {
  const query = QueryRequest.create("readings", (value) => Promise.resolve(result(value)))
    .whereEq("count", 7)
    .filterGte("amount", 42)
    .filterLt("ratio", 0.5)
    .filterEq("healthy", true)
    .filterNe("big", 9_007_199_254_740_993n)
    .filterEq("region", "eu")
    .filterEq("tagged", { kind: "int", value: 3 })
    .filterIn("zone", ["a", 1])
    .rawSql("postgres", "SELECT 1 WHERE amount > $1", [10, null])
    .intoQuery()
  assert.deepEqual(query.byKey, [{ field: "count", value: { kind: "long", value: 7n } }])
  assert.deepEqual(query.filter, {
    kind: "all",
    filters: [
      filterPred("amount", "gte", { kind: "long", value: 42n }),
      filterPred("ratio", "lt", { kind: "double", value: 0.5 }),
      filterPred("healthy", "eq", { kind: "boolean", value: true }),
      filterPred("big", "ne", { kind: "long", value: 9_007_199_254_740_993n }),
      filterPred("region", "eq", { kind: "string", value: "eu" }),
      filterPred("tagged", "eq", { kind: "int", value: 3 }),
      filterPred("zone", "in", {
        kind: "list",
        value: [
          { kind: "string", value: "a" },
          { kind: "long", value: 1n }
        ]
      })
    ]
  })
  assert.deepEqual(query.rawSql?.params, [{ kind: "long", value: 10n }, { kind: "null" }])
  const request = QueryRequest.create("readings", (value) => Promise.resolve(result(value)))
  assert.throws(() => request.filterGt("amount", Number.NaN), InvalidError)
  assert.throws(() => request.filterGt("amount", 1n << 63n), InvalidError)
})

void test("given_a_walk_when_rows_are_read_then_should_bind_each_row_to_its_fields", async () => {
  const request = QueryRequest.create("readings", (query) =>
    Promise.resolve(result(query, query.page.offset === 0n))
  ).maxRows(5)
  const all = await request.fetchAll()
  assert.equal(all.rows.length, 2)
  assert.equal(all.frontierMoved, false)
  const [first] = all
  assert.ok(first !== undefined)
  assert.deepEqual(first.value("id"), { kind: "string", value: "o-7" })
  assert.equal(first.valueText("id"), "o-7")
  assert.equal(first.fieldIndex("id"), 0)
  assert.equal(first.value("absent"), undefined)
  assert.deepEqual(
    first.fields.map((field) => field.name),
    ["id"]
  )
  const walked = []
  for await (const row of request.rows()) walked.push(row.valueText("id"))
  assert.deepEqual(walked, ["o-7", "o-7"])
  const stepped = request.rows()
  assert.equal((await stepped.next())?.valueText("id"), "o-7")
  assert.equal((await stepped.next())?.valueI64("id"), undefined)
  assert.equal(await stepped.next(), undefined)
})

void test("given_a_frontier_that_moves_between_pages_when_walked_then_should_report_it", async () => {
  const frontier = (appliedOffset: bigint) => [
    { stream: "shop", topic: "orders", partitionId: 0, appliedOffset }
  ]
  const walk = (offsets: readonly bigint[]) => {
    let page = 0
    return QueryRequest.create("readings", (query) => {
      const applied = offsets[page] ?? 0n
      page += 1
      const answer = result(query, page < offsets.length)
      return Promise.resolve({
        ...answer,
        context: { ...answer.context, frontier: frontier(applied) }
      })
    }).maxRows(10)
  }
  assert.equal((await walk([5n, 5n]).fetchAll()).frontierMoved, false)
  assert.equal((await walk([5n, 6n]).fetchAll()).frontierMoved, true)
  const rows = walk([5n, 5n, 9n]).rows()
  let read = 0
  for await (const row of rows) {
    read += 1
    assert.equal(row.valueText("id"), "o-7")
  }
  assert.equal(read, 3)
  assert.equal(rows.frontierMoved, true)
})

void test("raw SQL carries an explicit dialect and typed parameters", () => {
  const query = QueryRequest.create("readings", (value) => Promise.resolve(result(value)))
    .rawSql("postgres", "SELECT 1 WHERE amount > $1", [{ kind: "long", value: 10n }])
    .intoQuery()
  assert.ok(query.rawSql !== undefined)
  assert.equal(query.rawSql.dialect, "postgres")
  assert.deepEqual(query.rawSql.params, [{ kind: "long", value: 10n }])
})

void test("query replies remain bound to the requested execution identity", async () => {
  const wrongExecutionId = QueryExecutionId.fromU128(10n)
  const request = QueryRequest.create("readings", (query) =>
    Promise.resolve({
      ...result(query),
      context: { ...result(query).context, executionId: wrongExecutionId }
    })
  )

  await assert.rejects(request.fetch(), /execution id does not match/)
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
    return Promise.resolve({ ...result(query, true), rows: [] })
  }).maxRows(10)
  const rows = []
  for await (const row of request.rows()) rows.push(row)
  assert.equal(rows.length, 0)
  assert.equal(calls, 1)
})

void test("given_a_zero_page_limit_when_validated_then_should_refuse_it", () => {
  const query = QueryRequest.create("readings", (value) => Promise.resolve(result(value)))
    .limit(0)
    .intoQuery()
  assert.throws(() => {
    validateQuery(query)
  }, /limit/)
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

void test("given_a_server_that_always_reports_more_when_walking_then_should_stop_at_the_ceiling", async () => {
  const offsets: (bigint | undefined)[] = []
  const request = QueryRequest.create("readings", (query) => {
    offsets.push(query.page.offset)
    return Promise.resolve(result(query, true))
  })
  const rows = []
  for await (const row of request.maxRows(3).rows()) rows.push(row)
  assert.equal(rows.length, 3)
  assert.deepEqual(offsets, [0n, 1n, 2n])
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

void test("given_a_query_builder_with_the_required_fields_when_built_then_should_match_the_new_query_defaults", () => {
  const executionId = QueryExecutionId.fromU128(7n)
  const built = new QueryBuilder()
    .executionId(executionId)
    .target(operationalTarget("readings"))
    .deadlineMicros(1_000n)
    .build()
  assert.deepEqual(built, newQuery(operationalTarget("readings"), executionId, 1_000n))
})

void test("given_a_query_builder_with_optional_fields_when_built_then_should_round_trip_through_the_wire", () => {
  const filter = filterPred("host", "eq", { kind: "string", value: "node-7" })
  const built = new QueryBuilder()
    .executionId(QueryExecutionId.fromU128(8n))
    .target(operationalTarget("readings"))
    .deadlineMicros(2_000n)
    .messageType("reading")
    .timeRange([10n, 20n])
    .filter(filter)
    .order([{ field: "ts", dir: "desc" }])
    .distinct(true)
    .fork("what-if")
    .consistency("read_your_writes")
    .build()
  validateQuery(built)
  assert.equal(built.messageType, "reading")
  assert.deepEqual(built.filter, filter)
  assert.equal(built.consistency, "read_your_writes")
  assert.deepEqual(decodeQuery(encodeQuery(built), "query"), built)
})

void test("given_an_optional_field_set_to_undefined_when_built_then_should_leave_it_out", () => {
  const built = new QueryBuilder()
    .executionId(QueryExecutionId.fromU128(9n))
    .target(operationalTarget("readings"))
    .deadlineMicros(3_000n)
    .fork("what-if")
    .fork(undefined)
    .build()
  assert.equal("fork" in built, false)
})

void test("given_a_query_builder_missing_a_required_field_when_built_then_should_throw_invalid", () => {
  assert.throws(
    () => new QueryBuilder().target(operationalTarget("readings")).deadlineMicros(1n).build(),
    InvalidError
  )
  assert.throws(
    () => new QueryBuilder().executionId(QueryExecutionId.fromU128(1n)).deadlineMicros(1n).build(),
    InvalidError
  )
  assert.throws(
    () =>
      new QueryBuilder()
        .executionId(QueryExecutionId.fromU128(1n))
        .target(operationalTarget("readings"))
        .build(),
    InvalidError
  )
})

void test("given_query_deadlines_when_turned_into_reply_waits_then_should_add_the_grace_up_to_the_cap", () => {
  const now = 1_000_000_000n
  assert.equal(queryReplyWaitMs(now + 5_000_000n, now), 5_000 + QUERY_REPLY_GRACE_MS)
  assert.equal(queryReplyWaitMs(now - 1n, now), QUERY_REPLY_GRACE_MS)
  assert.equal(
    queryReplyWaitMs(now + BigInt(QUERY_DEADLINE_MS) * 1000n, now),
    QUERY_DEADLINE_MS + QUERY_REPLY_GRACE_MS
  )
  assert.throws(() => queryReplyWaitMs(now + 60_000_000n, now), InvalidError)
})

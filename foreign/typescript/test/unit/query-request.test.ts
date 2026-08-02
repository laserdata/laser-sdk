import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import { QueryRequest } from "../../src/managed/query.js"
import {
  filterPred,
  type Filter,
  type Query,
  type QueryResult,
  type Row
} from "../../src/wire/query.js"

function result(rows: readonly Row[], offset: number, hasMore: boolean): QueryResult {
  return { rows, page: { offset, limit: 2, hasMore } }
}

function predicateCount(filter: Filter | undefined): number {
  if (filter === undefined) return 0
  switch (filter.kind) {
    case "pred":
      return 1
    case "not":
      return predicateCount(filter.filter)
    case "all":
    case "any":
      return filter.filters.reduce((total, child) => total + predicateCount(child), 0)
  }
}

function row(offset: bigint, payload?: string): Row {
  return {
    headers: new Map([["id", offset.toString()]]),
    metadata: new Map(),
    offset,
    ...(payload !== undefined ? { payload: new TextEncoder().encode(payload) } : {})
  }
}

void test("given_the_fluent_query_grammar_when_built_then_should_compose_filters_and_preserve_bigints", () => {
  const request = new QueryRequest("orders", () => Promise.resolve(result([], 0, false)))
    .whereEq("customer_id", "c-1")
    .conversation("01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .filterGte("total", { kind: "int", value: 10_000n })
    .filterPrefix("region", "eu-")
    .messageType("checkout")
    .timeRange(100n, 200n)
    .orderDesc("total")
    .withPayload()
    .withTotal()
    .readYourWrites()
    .textIn("description", "priority")
    .limit(50)

  const query = request.intoQuery()
  assert.equal(query.index, "orders")
  assert.equal(query.byKey.length, 2)
  assert.deepEqual(query.timeRange, [100n, 200n])
  assert.equal(query.filter?.kind, "all")
  assert.equal(query.select.payload, true)
  assert.equal(query.wantTotal, true)
  assert.equal(query.consistency, "readYourWrites")
  assert.deepEqual(query.text, { field: "description", query: "priority" })
})

void test("given_the_former_by_key_spelling_when_used_then_should_match_where_eq", () => {
  const executor = () => Promise.resolve(result([], 0, false))
  const legacy = new QueryRequest("orders", executor).byKey("status", "paid").intoQuery()
  const current = new QueryRequest("orders", executor).whereEq("status", "paid").intoQuery()

  assert.deepEqual(legacy.byKey, current.byKey)
})

void test("given_every_predicate_verb_when_chained_then_should_fold_them_into_one_filter_tree", () => {
  const query = new QueryRequest("orders", () => Promise.resolve(result([], 0, false)))
    .filterEq("status", { kind: "string", value: "paid" })
    .filterNe("status", { kind: "string", value: "void" })
    .filterGt("total", { kind: "int", value: 1n })
    .filterGte("total", { kind: "int", value: 2n })
    .filterLt("total", { kind: "int", value: 900n })
    .filterLte("total", { kind: "int", value: 800n })
    .filterIn("region", [
      { kind: "string", value: "eu" },
      { kind: "string", value: "us" }
    ])
    .filterContains("note", "urgent")
    .filterPrefix("sku", "kit-")
    .intoQuery()

  assert.equal(predicateCount(query.filter), 9)
})

void test("given_every_aggregate_verb_when_chained_then_should_declare_each_call_with_its_alias", () => {
  const query = new QueryRequest("orders", () => Promise.resolve(result([], 0, false)))
    .count()
    .countDistinct("customer_id")
    .sum("total")
    .avg("total")
    .min("total")
    .max("total")
    .stdDev("total")
    .percentile("total", 0.95)
    .groupBy(["status"])
    .having(filterPred("count", "gt", { kind: "int", value: 0n }))
    .window("ts", 60_000_000n)
    .distinct()
    .intoQuery()

  const { aggregate } = query
  assert.ok(aggregate, "the aggregate verbs declare one aggregate clause")
  assert.equal(aggregate.funcs.length, 8)
  assert.deepEqual(aggregate.groupBy, ["status"])
  assert.ok(aggregate.window, "window() reaches the aggregate")
  assert.equal(query.distinct, true)
  assert.ok(query.having, "having() rides the query, not the aggregate")
})

void test("given_the_read_shaping_verbs_when_chained_then_should_set_selection_paging_and_consistency", () => {
  const query = new QueryRequest("orders", () => Promise.resolve(result([], 0, false)))
    .selectFields(["id", "total"])
    .messageType("checkout")
    .orderAsc("id")
    .orderDesc("total")
    .limit(25)
    .offset(50)
    .withPayload()
    .withTotal()
    .consistency("strong")
    .fork("experiment-1")
    .conversation("01ARZ3NDEKTSV4RRFFQ69G5FAV")
    .intoQuery()

  assert.deepEqual(query.select.fields, ["id", "total"])
  assert.equal(query.select.payload, true)
  assert.equal(query.wantTotal, true)
  assert.equal(query.consistency, "strong")
  assert.equal(query.fork, "experiment-1")
  assert.equal(query.limit, 25)
  assert.equal(query.offset, 50)
  assert.equal(query.order.length, 2)
  assert.equal(query.messageType, "checkout")
})

void test("given_a_vector_or_lexical_search_when_declared_then_should_carry_the_search_clause", () => {
  const nearest = new QueryRequest("orders", () => Promise.resolve(result([], 0, false)))
    .nearest([0.1, 0.2], 5)
    .intoQuery()
  assert.ok(nearest.vector, "nearest() sets the vector clause")

  const scoped = new QueryRequest("orders", () => Promise.resolve(result([], 0, false)))
    .nearestIn("embedding", [0.1, 0.2], 5)
    .intoQuery()
  assert.ok(scoped.vector, "nearestIn() sets the vector clause")

  const lexical = new QueryRequest("orders", () => Promise.resolve(result([], 0, false)))
    .text("refund dispute")
    .intoQuery()
  assert.deepEqual(lexical.text, { query: "refund dispute" })
})

void test("given_raw_sql_when_supplied_then_should_pass_the_statement_and_its_parameters_through", () => {
  const query = new QueryRequest("orders", () => Promise.resolve(result([], 0, false)))
    .rawSql("select 1 where status = ?", [{ kind: "string", value: "paid" }])
    .intoQuery()

  const { rawSql } = query
  assert.ok(rawSql, "rawSql() sets the statement")
  assert.equal(rawSql.sql, "select 1 where status = ?")
  assert.deepEqual(rawSql.params, [{ kind: "string", value: "paid" }])
})

void test("given_read_your_writes_when_requested_then_should_raise_the_consistency_level", () => {
  const query = new QueryRequest("orders", () => Promise.resolve(result([], 0, false)))
    .readYourWrites()
    .intoQuery()

  assert.equal(query.consistency, "readYourWrites")
})

void test("given_one_matching_row_when_fetched_singly_then_should_decode_it_and_bound_the_page", async () => {
  const request = new QueryRequest("orders", () =>
    Promise.resolve(result([row(0n, '{"id":9}')], 0, false))
  )
  const codec = {
    encode: () => new Uint8Array(),
    decode: (bytes: Uint8Array) => JSON.parse(new TextDecoder().decode(bytes)) as { id: number }
  }

  assert.deepEqual(await request.fetchOne(codec), { id: 9 })
  assert.equal(request.intoQuery().limit, 1)

  const empty = new QueryRequest("orders", () => Promise.resolve(result([], 0, false)))
  assert.equal(await empty.fetchOne(codec), undefined)
})

void test("given_an_unbounded_materialization_when_asked_for_then_should_walk_every_page", async () => {
  const execute = (query: Query): Promise<QueryResult> =>
    Promise.resolve(
      query.offset === 0 ? result([row(0n, "{}"), row(1n, "{}")], 0, true) : result([], 2, false)
    )

  assert.equal((await new QueryRequest("orders", execute).limit(2).fetchAll()).length, 2)

  const typed = await new QueryRequest("orders", execute).limit(2).fetchAllTyped({
    encode: () => new Uint8Array(),
    decode: () => ({ ok: true })
  })
  assert.deepEqual(typed, [{ ok: true }, { ok: true }])
})

void test("given_a_row_without_a_payload_when_decoded_then_should_reject_rather_than_guess", async () => {
  const request = new QueryRequest("orders", () => Promise.resolve(result([row(0n)], 0, false)))

  await assert.rejects(
    () =>
      request.fetchTyped({
        encode: () => new Uint8Array(),
        decode: () => ({ id: 0 })
      }),
    InvalidError
  )
})

void test("given_rows_without_a_ceiling_when_started_then_should_reject_before_execution", () => {
  let executed = false
  const request = new QueryRequest("orders", () => {
    executed = true
    return Promise.resolve(result([], 0, false))
  })
  assert.throws(() => request.rows(), InvalidError)
  assert.equal(executed, false)
})

void test("given_a_bounded_row_walk_when_pages_have_more_then_should_advance_offsets_and_stop_at_the_ceiling", async () => {
  const offsets: number[] = []
  const execute = (query: Query): Promise<QueryResult> => {
    offsets.push(query.offset)
    return Promise.resolve(
      query.offset === 0
        ? result([row(0n), row(1n)], 0, true)
        : result([row(2n), row(3n)], 2, false)
    )
  }
  const records: Row[] = []
  for await (const record of new QueryRequest("orders", execute).limit(2).maxRows(3).rows()) {
    records.push(record)
  }

  assert.deepEqual(offsets, [0, 2])
  assert.deepEqual(
    records.map((record) => record.offset),
    [0n, 1n, 2n]
  )
})

void test("given_typed_fetch_when_payloads_are_returned_then_should_decode_with_the_explicit_codec", async () => {
  const request = new QueryRequest("orders", () =>
    Promise.resolve(result([row(0n, '{"id":7}')], 0, false))
  )
  const values = await request.fetchTyped({
    encode: () => new Uint8Array(),
    decode: (bytes) => {
      assert.equal(new TextDecoder().decode(bytes), '{"id":7}')
      return { id: 7 }
    }
  })
  assert.deepEqual(values, [{ id: 7 }])
  assert.equal(request.intoQuery().select.payload, true)
})

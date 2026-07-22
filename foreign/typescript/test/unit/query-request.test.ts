import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import { QueryRequest } from "../../src/managed/query.js"
import type { Query, QueryResult, Row } from "../../src/wire/query.js"

function result(rows: readonly Row[], offset: number, hasMore: boolean): QueryResult {
  return { rows, page: { offset, limit: 2, hasMore } }
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
    .byKey("customer_id", "c-1")
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

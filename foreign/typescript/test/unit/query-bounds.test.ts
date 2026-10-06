import assert from "node:assert/strict"
import { test } from "node:test"

import { InvalidError } from "../../src/client/errors.js"
import { QueryRequest } from "../../src/managed/query.js"
import { jsonCodec } from "../../src/stream/codecs.js"

void test("given_an_invalid_row_ceiling_when_configured_then_should_refuse_an_unbounded_walk", () => {
  for (const value of [NaN, Infinity, -1, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
    const request = new QueryRequest("readings", () =>
      Promise.reject(new Error("unexpected query"))
    )
    assert.throws(() => request.maxRows(value), InvalidError)
  }
})

void test("given_a_zero_row_ceiling_when_rows_are_read_then_should_return_without_querying", async () => {
  let calls = 0
  const request = () =>
    new QueryRequest("readings", () => {
      calls += 1
      return Promise.reject(new Error("unexpected query"))
    }).maxRows(0)
  for await (const row of request().rows()) assert.equal(row, undefined)
  for await (const row of request().rowsTyped(jsonCodec((value) => value)))
    assert.equal(row, undefined)
  assert.equal(calls, 0)
})

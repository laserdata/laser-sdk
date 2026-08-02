import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError, LaserError, UnsupportedError } from "../../src/client/errors.js"

void test("given_an_invalid_error_when_constructed_then_should_carry_kind_and_context", () => {
  const error = new InvalidError("partition must fit u32", { partition: -1 })
  assert.equal(error.kind, "invalid")
  assert.equal(error.name, "InvalidError")
  assert.deepEqual(error.context, { partition: -1 })
  assert.ok(error instanceof LaserError)
})

void test("given_an_unsupported_error_when_constructed_then_should_preserve_cause", () => {
  const cause = new Error("boom")
  const error = new UnsupportedError("query requires a managed host", { cause })
  assert.equal(error.kind, "unsupported")
  assert.equal(error.cause, cause)
})

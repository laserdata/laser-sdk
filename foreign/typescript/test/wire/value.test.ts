import assert from "node:assert/strict"
import { test } from "node:test"
import { decodeValue, encodeValue, valueFromInput } from "../../src/wire/value.js"

void test("given_typed_input_when_inferred_then_should_pick_the_narrowest_scalar", () => {
  assert.deepEqual(valueFromInput("null"), { kind: "null" })
  assert.deepEqual(valueFromInput("true"), { kind: "bool", value: true })
  assert.deepEqual(valueFromInput("false"), { kind: "bool", value: false })
  assert.deepEqual(valueFromInput("-42"), { kind: "int", value: -42n })
  assert.deepEqual(valueFromInput("+7"), { kind: "int", value: 7n })
  assert.deepEqual(valueFromInput("18446744073709551615"), {
    kind: "uint",
    value: 18_446_744_073_709_551_615n
  })
  assert.deepEqual(valueFromInput("18446744073709551616"), {
    kind: "float",
    value: 18_446_744_073_709_551_616
  })
  assert.deepEqual(valueFromInput("1.5"), { kind: "float", value: 1.5 })
  assert.deepEqual(valueFromInput(".5"), { kind: "float", value: 0.5 })
  assert.deepEqual(valueFromInput("1e9"), { kind: "str", value: "1e9" })
  assert.deepEqual(valueFromInput("inf"), { kind: "str", value: "inf" })
  assert.deepEqual(valueFromInput("1.2.3"), { kind: "str", value: "1.2.3" })
  assert.deepEqual(valueFromInput(""), { kind: "str", value: "" })
  assert.deepEqual(valueFromInput("+"), { kind: "str", value: "+" })
})

void test("given_integers_past_i64_when_decoded_then_should_read_as_uint_like_rust", () => {
  assert.deepEqual(decodeValue((1n << 63n) - 1n, "value"), { kind: "int", value: (1n << 63n) - 1n })
  assert.deepEqual(decodeValue(1n << 63n, "value"), { kind: "uint", value: 1n << 63n })
  assert.deepEqual(decodeValue("hi", "value"), { kind: "str", value: "hi" })
  assert.equal(encodeValue({ kind: "uint", value: 1n << 63n }), 1n << 63n)
})

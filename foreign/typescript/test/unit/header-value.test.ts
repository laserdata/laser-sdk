import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import { HeaderValue } from "../../src/stream/header-value.js"

void test("given_typed_header_values_when_built_then_should_validate_their_exact_ranges", () => {
  assert.deepEqual(HeaderValue.int8(-128), { kind: "int8", value: -128 })
  assert.deepEqual(HeaderValue.uint32(4_294_967_295), {
    kind: "uint32",
    value: 4_294_967_295
  })
  assert.deepEqual(HeaderValue.uint64((1n << 64n) - 1n), {
    kind: "uint64",
    value: (1n << 64n) - 1n
  })
  assert.throws(() => HeaderValue.int8(128), InvalidError)
  assert.throws(() => HeaderValue.uint16(-1), InvalidError)
  assert.throws(() => HeaderValue.uint64(1n << 64n), InvalidError)
  assert.throws(() => HeaderValue.int128(new Uint8Array(15)), InvalidError)
  assert.throws(() => HeaderValue.double(Number.POSITIVE_INFINITY), InvalidError)
})

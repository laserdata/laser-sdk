import assert from "node:assert/strict"
import { test } from "node:test"

import { CodecError } from "../../src/client/errors.js"
import { Msgpack } from "../../src/stream/codecs.js"

const codec = new Msgpack()

void test("given_numbers_and_bigints_when_messagepack_encoded_then_should_use_the_rust_integer_bytes", () => {
  const values: readonly [number | bigint, readonly number[]][] = [
    [1, [1]],
    [1n, [1]],
    [-1n, [255]],
    [128n, [204, 128]],
    [256n, [205, 1, 0]],
    [65536n, [206, 0, 1, 0, 0]],
    [4_294_967_296, [207, 0, 0, 0, 1, 0, 0, 0, 0]],
    [4_294_967_296n, [207, 0, 0, 0, 1, 0, 0, 0, 0]],
    [-2_147_483_649, [211, 255, 255, 255, 255, 127, 255, 255, 255]],
    [18_446_744_073_709_551_615n, [207, 255, 255, 255, 255, 255, 255, 255, 255]]
  ]
  for (const [value, expected] of values) assert.deepEqual([...codec.encode(value)], expected)
})

void test("given_nested_objects_and_binary_when_messagepack_encoded_then_should_preserve_names_and_exact_bytes", () => {
  assert.deepEqual(
    [...codec.encode({ count: 1n, data: new Uint8Array([2, 3]) })],
    [130, 165, 99, 111, 117, 110, 116, 1, 164, 100, 97, 116, 97, 196, 2, 2, 3]
  )
  assert.deepEqual(codec.encode(new Map([["count", 1n]])), codec.encode({ count: 1n }))
})

void test("given_an_integer_outside_64_bits_when_messagepack_encoded_then_should_refuse_before_truncating", () => {
  assert.throws(() => codec.encode(1n << 64n), CodecError)
  assert.throws(() => codec.encode(-(1n << 63n) - 1n), CodecError)
  assert.throws(() => codec.encode(new Map([[1, "value"]])), CodecError)
})

import assert from "node:assert/strict"
import { test } from "node:test"
import { decodeOne, encodeNamed, expectMap, field } from "../../src/wire/cbor.js"

void test("given_a_named_map_when_encoded_then_should_preserve_insertion_order_not_canonical_sort", () => {
  const entries = new Map<string, unknown>()
  entries.set("namespace", "sessions")
  entries.set("key", Uint8Array.of(1, 2, 3))
  entries.set("value", Uint8Array.of(9, 9))
  entries.set("version", 42n)

  const bytes = encodeNamed(entries)
  const decoded = expectMap(decodeOne(bytes, "test"), "test")
  assert.deepEqual([...decoded.keys()], ["namespace", "key", "value", "version"])
})

void test("given_a_round_trip_when_re_encoded_then_should_be_byte_identical", () => {
  const entries = new Map<string, unknown>()
  entries.set("a", "x")
  entries.set("b", 7n)
  const bytes = encodeNamed(entries)
  const decoded = expectMap(decodeOne(bytes, "test"), "test")
  const reencoded = encodeNamed(decoded as ReadonlyMap<string, unknown>)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_trailing_bytes_when_decoded_then_should_reject", () => {
  const bytes = encodeNamed(new Map([["a", 1n]]))
  const withTrailing = new Uint8Array([...bytes, 0])
  assert.throws(() => decodeOne(withTrailing, "test"))
})

void test("given_a_truncated_frame_when_decoded_then_should_reject", () => {
  const bytes = encodeNamed(new Map([["a", 1n]]))
  assert.throws(() => decodeOne(bytes.slice(0, bytes.length - 1), "test"))
})

void test("given_duplicate_map_keys_when_decoded_then_should_reject", () => {
  const duplicateKeyMap = Uint8Array.from(Buffer.from("a2616101616102", "hex"))
  assert.throws(() => decodeOne(duplicateKeyMap, "test"))
})

void test("given_field_readers_when_reading_a_map_then_should_distinguish_absent_from_wrong_type", () => {
  const map = expectMap(decodeOne(encodeNamed(new Map([["name", "orders"]])), "test"), "test")
  assert.equal(field.requiredString(map, "name", "test"), "orders")
  assert.equal(field.optionalString(map, "missing", "test"), undefined)
  assert.throws(() => field.requiredString(map, "missing", "test"))
  assert.throws(() => field.requiredBytes(map, "name", "test"))
})

void test("given_a_small_or_large_integer_when_read_as_u64_then_should_always_return_bigint", () => {
  const map = expectMap(
    decodeOne(
      encodeNamed(
        new Map([
          ["small", 42n],
          ["big", 18446744073709551615n]
        ])
      ),
      "test"
    ),
    "test"
  )
  assert.equal(field.requiredU64(map, "small", "test"), 42n)
  assert.equal(typeof field.requiredU64(map, "small", "test"), "bigint")
  assert.equal(field.requiredU64(map, "big", "test"), 18446744073709551615n)
})

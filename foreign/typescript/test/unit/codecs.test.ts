import assert from "node:assert/strict"
import { test } from "node:test"

import { CodecError } from "../../src/client/errors.js"
import {
  Bson,
  Cbor,
  Json,
  Msgpack,
  kvEntryDecodeValue,
  kvEntryDecodeValueWith
} from "../../src/stream/codecs.js"

interface Reading {
  readonly sensor: string
  readonly sequence: bigint
}

function decodeReading(value: unknown): Reading {
  if (
    value === null ||
    typeof value !== "object" ||
    !("sensor" in value) ||
    typeof value.sensor !== "string" ||
    !("sequence" in value) ||
    typeof value.sequence !== "bigint"
  ) {
    throw new TypeError("reading requires a string sensor and bigint sequence")
  }
  return { sensor: value.sensor, sequence: value.sequence }
}

void test("given_a_json_codec_when_the_shape_is_wrong_then_should_reject_at_decode", () => {
  const codec = new Json((value): { readonly id: string } => {
    if (
      value === null ||
      typeof value !== "object" ||
      !("id" in value) ||
      typeof value.id !== "string"
    ) {
      throw new TypeError("id must be a string")
    }
    return { id: value.id }
  })
  assert.deepEqual(codec.decode(codec.encode({ id: "one" })), { id: "one" })
  assert.throws(() => codec.decode(new TextEncoder().encode('{"id":1}')), TypeError)
})

void test("given_a_cbor_codec_when_round_tripped_then_should_preserve_bigints_and_objects", () => {
  const codec = new Cbor(decodeReading)
  const reading = { sensor: "s-1", sequence: 9_007_199_254_740_993n }
  assert.deepEqual(codec.decode(codec.encode(reading)), reading)
})

void test("given_a_messagepack_codec_when_round_tripped_then_should_preserve_bigints", () => {
  const codec = new Msgpack(decodeReading)
  const reading = { sensor: "s-2", sequence: 9_007_199_254_740_993n }
  assert.deepEqual(codec.decode(codec.encode(reading)), reading)
})

void test("given_each_builtin_codec_when_read_then_should_advertise_its_content_type", () => {
  assert.equal(new Json().contentType, "json")
  assert.equal(new Cbor().contentType, "cbor")
  assert.equal(new Msgpack().contentType, "msgpack")
  assert.equal(new Bson().contentType, "bson")
})

void test("given_a_document_when_bson_encoded_then_should_match_the_rust_bytes", () => {
  const codec = new Bson()
  const body = {
    id: 7,
    name: "alice",
    ok: true,
    none: null,
    ratio: 0.5,
    data: new Uint8Array([1, 2]),
    tags: ["a", 2],
    nested: { big: 2n ** 53n + 1n }
  }
  const payload = codec.encode(body)
  assert.deepEqual([...payload], RUST_BSON_BODY)
  assert.deepEqual(codec.decode(payload), body)
})

void test("given_a_non_document_or_malformed_bson_when_coded_then_should_reject", () => {
  const codec = new Bson()
  assert.throws(() => codec.encode([1, 2]), CodecError)
  assert.throws(() => codec.encode({ count: 1n << 63n }), CodecError)
  assert.throws(() => codec.decode(new Uint8Array([5, 0, 0, 0])), CodecError)
})

void test("given_a_kv_entry_when_decoded_then_should_read_json_or_the_given_decoder", () => {
  const entry = {
    key: new TextEncoder().encode("k"),
    value: new Json().encode({ sensor: "s-3" }),
    version: 1n
  }
  assert.deepEqual(kvEntryDecodeValue(entry), { sensor: "s-3" })
  assert.throws(() => kvEntryDecodeValue(entry, decodeReading), TypeError)
  const cbor = new Cbor(decodeReading)
  const reading = { sensor: "s-4", sequence: 9_007_199_254_740_995n }
  assert.deepEqual(kvEntryDecodeValueWith({ ...entry, value: cbor.encode(reading) }, cbor), reading)
})

// Bson.encode of the same body through the Rust codec (Python binding).
const RUST_BSON_BODY = [
  129, 0, 0, 0, 18, 105, 100, 0, 7, 0, 0, 0, 0, 0, 0, 0, 2, 110, 97, 109, 101, 0, 6, 0, 0, 0, 97,
  108, 105, 99, 101, 0, 8, 111, 107, 0, 1, 10, 110, 111, 110, 101, 0, 1, 114, 97, 116, 105, 111, 0,
  0, 0, 0, 0, 0, 0, 224, 63, 5, 100, 97, 116, 97, 0, 2, 0, 0, 0, 0, 1, 2, 4, 116, 97, 103, 115, 0,
  25, 0, 0, 0, 2, 48, 0, 2, 0, 0, 0, 97, 0, 18, 49, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 3, 110, 101, 115,
  116, 101, 100, 0, 18, 0, 0, 0, 18, 98, 105, 103, 0, 1, 0, 0, 0, 0, 0, 32, 0, 0, 0
]

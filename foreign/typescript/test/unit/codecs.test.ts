import assert from "node:assert/strict"
import { test } from "node:test"

import { cborCodec, jsonCodec, messagePackCodec } from "../../src/stream/codecs.js"

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
  const codec = jsonCodec((value): { readonly id: string } => {
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
  const codec = cborCodec(decodeReading)
  const reading = { sensor: "s-1", sequence: 9_007_199_254_740_993n }
  assert.deepEqual(codec.decode(codec.encode(reading)), reading)
})

void test("given_a_messagepack_codec_when_round_tripped_then_should_preserve_bigints", () => {
  const codec = messagePackCodec(decodeReading)
  const reading = { sensor: "s-2", sequence: 9_007_199_254_740_993n }
  assert.deepEqual(codec.decode(codec.encode(reading)), reading)
})

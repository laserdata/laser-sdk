import assert from "node:assert/strict"
import { test } from "node:test"
import {
  ContentType,
  contentTypeCode,
  contentTypeFromCode,
  isRawContentType
} from "../../src/wire/content.js"

// Mirrors `wire/src/content.rs`'s own fixed table exactly (the source of
// truth), not derived mechanically, because the mapping is a Rust match
// expression rather than an evaluable constant declaration.
const EXPECTED: readonly (readonly [ContentType, number])[] = [
  [ContentType.Raw, 0],
  [ContentType.Json, 1],
  [ContentType.Msgpack, 2],
  [ContentType.Cbor, 3],
  [ContentType.Bson, 4],
  [ContentType.Avro, 5],
  [ContentType.Protobuf, 6],
  [ContentType.Arrow, 7],
  [ContentType.Ref, 8],
  [ContentType.Any, 255]
]

void test("given_content_type_codes_when_mapped_then_should_match_the_fixed_dictionary", () => {
  for (const [contentType, code] of EXPECTED) {
    assert.equal(contentTypeCode(contentType), code)
    assert.equal(contentTypeFromCode(code), contentType)
  }
  assert.equal(contentTypeFromCode(9), undefined)
})

void test("given_the_raw_content_type_when_checked_then_should_report_is_raw", () => {
  assert.ok(isRawContentType(ContentType.Raw))
  assert.ok(!isRawContentType(ContentType.Json))
})

import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import { Record, recordHeaders } from "../../src/stream/record.js"
import { ContentType } from "../../src/wire/content.js"
import { MAX_INDEX_ENTRIES_PER_RECORD } from "../../src/wire/limits.js"

void test("given_a_record_when_lowered_then_should_stamp_compact_typed_headers", () => {
  const headers = recordHeaders(
    new Record()
      .contentType(ContentType.Json)
      .projectionRef("orders.v1")
      .schemaId(7)
      .inlinePayload()
      .index("order_id", "123")
      .header("trace", "abc")
  )
  assert.deepEqual(headers.get("agdx.ct"), { kind: "uint8", value: 1 })
  assert.deepEqual(headers.get("agdx.sid"), { kind: "uint32", value: 7 })
  assert.deepEqual(headers.get("agdx.inline"), { kind: "bool", value: true })
  assert.deepEqual(headers.get("agdx.ref"), { kind: "string", value: "orders.v1" })
  assert.deepEqual(headers.get("agdx.idx.order_id"), { kind: "string", value: "123" })
  assert.deepEqual(headers.get("trace"), { kind: "string", value: "abc" })
})

void test("given_reserved_or_oversized_record_headers_when_lowered_then_should_reject", () => {
  assert.throws(() => recordHeaders(new Record().header("agdx.ct", "json")), InvalidError)
  assert.throws(() => recordHeaders(new Record().index("agdx.idx.bad", "value")), InvalidError)
  const tooMany = new Record()
  for (let index = 0; index <= MAX_INDEX_ENTRIES_PER_RECORD; index += 1) {
    tooMany.index(`field_${String(index)}`, "value")
  }
  assert.throws(() => recordHeaders(tooMany), InvalidError)
  assert.throws(() => recordHeaders(new Record().header("large", "x".repeat(256))), InvalidError)
})

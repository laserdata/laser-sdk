import assert from "node:assert/strict"
import { test } from "node:test"
import { contentId } from "../../src/wire/hashing.js"
import { RecordId } from "../../src/wire/ids.js"

const utf8 = (text: string) => new TextEncoder().encode(text)

void test("given_the_same_segments_when_hashed_then_should_be_stable", () => {
  const a = contentId([utf8("owner"), Uint8Array.of(1), utf8("body")])
  const b = contentId([utf8("owner"), Uint8Array.of(1), utf8("body")])
  assert.equal(a, b)
})

void test("given_different_segments_when_hashed_then_should_differ", () => {
  assert.notEqual(contentId([utf8("a")]), contentId([utf8("b")]))
})

void test("given_the_pinned_memory_segments_when_hashed_then_should_match_the_golden_id", () => {
  const id = contentId([
    Uint8Array.of(0),
    utf8("agent"),
    Uint8Array.of(0),
    Uint8Array.of(1),
    utf8("x")
  ])
  const rendered = RecordId.fromU128(id).toString()
  assert.equal(rendered, "1A9GVS6SJ6SNS4KY0H19130WCW")
})

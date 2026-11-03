import assert from "node:assert/strict"
import { test } from "node:test"
import { compareCodePoints } from "../../src/runtime/compare.js"

void test("given_strings_when_compared_then_should_follow_utf8_byte_order_like_rust", () => {
  const sorted = ["z", "é", "a", "\u{1F600}", "～", "ab", ""].toSorted(compareCodePoints)
  assert.deepEqual(sorted, ["", "a", "ab", "z", "é", "～", "\u{1F600}"])
  assert.equal(compareCodePoints("same", "same"), 0)
})

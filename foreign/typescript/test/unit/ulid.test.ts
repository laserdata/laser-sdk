import assert from "node:assert/strict"
import { test } from "node:test"
import { mintUlidValue, type UlidSource } from "../../src/runtime/ulid.js"
import { ConversationId, IntentId } from "../../src/types/ids.js"

const source: UlidSource = {
  nowMilliseconds: () => 0x0190_3c1f_aa00,
  fillRandom: (bytes) => {
    bytes.fill(0)
    bytes[bytes.length - 1] = 1
  }
}

void test("given_a_deterministic_source_when_minting_then_should_compose_timestamp_and_entropy", () => {
  assert.equal(mintUlidValue(source), 0x0190_3c1f_aa00_0000_0000_0000_0000_0001n)
})

void test("given_a_deterministic_source_when_creating_sdk_ids_then_should_use_the_same_ulid_shape", () => {
  assert.equal(ConversationId.new(source).toString(), "01J0Y1ZAG00000000000000001")
  assert.equal(IntentId.new(source).toString(), "01J0Y1ZAG00000000000000001")
})

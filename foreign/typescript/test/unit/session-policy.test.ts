import assert from "node:assert/strict"
import { test } from "node:test"
import { conversationFor } from "../../src/provenance/session-policy.js"

void test("given_per_user_policy_when_deriving_for_a_key_then_should_be_stable_and_distinct", () => {
  assert.ok(conversationFor("perUser", "alice").equals(conversationFor("perUser", "alice")))
  assert.ok(!conversationFor("perUser", "alice").equals(conversationFor("perUser", "bob")))
})

void test("given_per_call_policy_when_deriving_twice_then_should_be_unique", () => {
  assert.ok(!conversationFor("perCall", "x").equals(conversationFor("perCall", "x")))
})

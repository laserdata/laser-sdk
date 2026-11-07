import assert from "node:assert/strict"
import { test } from "node:test"
import { AmbiguousMutationError, TransportError } from "../../src/client/errors.js"
import { MutationAttempts, backendUnavailable, openPositioned } from "../../src/iggy/apache-iggy.js"
import { encodeNamed } from "../../src/wire/cbor.js"

function reply(code: string): Uint8Array {
  return encodeNamed(
    new Map<string, unknown>([
      ["code", code],
      ["message", "reply"]
    ])
  )
}

void test("given_a_pending_attempt_when_a_later_one_is_forbidden_then_should_stay_ambiguous", () => {
  const attempts = new MutationAttempts()
  assert.equal(attempts.settle({ kind: "answered", reply: reply("pending") }), undefined)
  assert.throws(
    () => attempts.settle({ kind: "answered", reply: reply("forbidden") }),
    AmbiguousMutationError
  )
})

void test("given_a_lost_attempt_when_a_later_one_is_refused_by_the_transport_then_should_stay_ambiguous", () => {
  const attempts = new MutationAttempts()
  assert.equal(attempts.settle({ kind: "lost", message: "connection reset" }), undefined)
  assert.throws(
    () => attempts.settle({ kind: "refused", cause: new TransportError("refused", false) }),
    AmbiguousMutationError
  )
})

void test("given_a_first_attempt_when_it_is_forbidden_or_answered_then_should_settle_with_it", () => {
  const forbidden = reply("forbidden")
  assert.equal(new MutationAttempts().settle({ kind: "answered", reply: forbidden }), forbidden)
  const attempts = new MutationAttempts()
  attempts.settle({ kind: "answered", reply: reply("pending") })
  const applied = reply("ok")
  assert.equal(attempts.settle({ kind: "answered", reply: applied }), applied)
})

void test("given_a_backend_unavailable_reply_with_a_frontier_when_routed_then_should_read_like_a_plain_one", () => {
  const refusal = reply("backend_unavailable")
  const positioned = encodeNamed(
    new Map<string, unknown>([
      ["v", 1],
      ["frontier", new Map()],
      ["reply", refusal]
    ])
  )
  assert.equal(backendUnavailable(refusal), true)
  assert.equal(backendUnavailable(openPositioned(positioned).reply), true)
})

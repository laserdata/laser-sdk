import assert from "node:assert/strict"
import { test } from "node:test"
import { ContractBuilder } from "../../src/agent/contract.js"
import { routeTo } from "../../src/agent/router.js"
import { InvalidError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { AgentId } from "../../src/types/ids.js"

function builder(): ContractBuilder {
  return new ContractBuilder({} as Laser, routeTo(AgentId.new("worker")))
}

void test("given_fractional_expiry_when_configured_then_should_preserve_microseconds", () => {
  assert.doesNotThrow(() => builder().expireIfNotConsumed(0.125))
})

void test("given_unsafe_expiry_when_configured_then_should_raise_a_typed_error", () => {
  assert.throws(
    () => builder().expireIfNotConsumed(Number.MAX_SAFE_INTEGER),
    (error: unknown) => error instanceof InvalidError
  )
})

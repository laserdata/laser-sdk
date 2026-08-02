import assert from "node:assert/strict"
import path from "node:path"
import { test } from "node:test"
import * as limits from "../../src/wire/limits.js"
import {
  parseRustNumericConstants,
  parseRustStringConstants
} from "./support/rust-source-constants.js"

const LIMITS_RS = path.resolve(process.cwd(), "../../wire/src/limits.rs")

void test("given_ported_wire_caps_when_compared_to_wire_src_limits_rs_then_should_match_exactly", async () => {
  const rustNumeric = await parseRustNumericConstants(LIMITS_RS)
  const rustStrings = await parseRustStringConstants(LIMITS_RS)
  assert.ok(rustNumeric.size > 0, `expected to parse numeric constants from ${LIMITS_RS}`)
  assert.ok(rustStrings.size > 0, `expected to parse string constants from ${LIMITS_RS}`)

  const ported = Object.entries(limits)
  assert.ok(ported.length > 0, "expected limits.ts to export at least one constant")

  for (const [name, value] of ported) {
    if (typeof value === "string") {
      const rustValue = rustStrings.get(name)
      assert.ok(
        rustValue !== undefined,
        `${name} has no matching string constant in wire/src/limits.rs`
      )
      assert.equal(value, rustValue, `${name} diverges from wire/src/limits.rs`)
      continue
    }
    const rustValue = rustNumeric.get(name)
    assert.ok(rustValue !== undefined, `${name} has no matching constant in wire/src/limits.rs`)
    assert.equal(value, rustValue, `${name} diverges from wire/src/limits.rs`)
  }

  assert.equal(
    ported.length,
    rustNumeric.size + rustStrings.size,
    "limits.ts must port every constant in wire/src/limits.rs, no more and no fewer"
  )
})

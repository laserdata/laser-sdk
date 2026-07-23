import assert from "node:assert/strict"
import path from "node:path"
import { test } from "node:test"
import * as codes from "../../src/wire/codes.js"
import { parseRustNumericConstants } from "./support/rust-source-constants.js"

const CODES_RS = path.resolve(process.cwd(), "../../wire/src/codes.rs")

void test("given_ported_command_codes_when_compared_to_wire_src_codes_rs_then_should_match_exactly", async () => {
  const rustCodes = await parseRustNumericConstants(CODES_RS)
  assert.ok(rustCodes.size > 0, `expected to parse constants from ${CODES_RS}`)

  const ported = Object.entries(codes)
  assert.ok(ported.length > 0, "expected codes.ts to export at least one constant")

  for (const [name, value] of ported) {
    const rustValue = rustCodes.get(name)
    assert.ok(rustValue !== undefined, `${name} has no matching constant in wire/src/codes.rs`)
    assert.equal(value, rustValue, `${name} diverges from wire/src/codes.rs`)
  }

  assert.equal(
    ported.length,
    rustCodes.size,
    "codes.ts must port every constant in wire/src/codes.rs, no more and no fewer"
  )
})

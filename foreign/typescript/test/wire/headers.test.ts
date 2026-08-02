import assert from "node:assert/strict"
import path from "node:path"
import { test } from "node:test"
import * as headers from "../../src/wire/headers.js"
import {
  parseRustNumericConstants,
  parseRustStringConstants
} from "./support/rust-source-constants.js"

const HEADERS_RS = path.resolve(process.cwd(), "../../wire/src/headers.rs")

void test("given_ported_header_keys_and_caps_when_compared_to_wire_src_headers_rs_then_should_match_exactly", async () => {
  const rustNumeric = await parseRustNumericConstants(HEADERS_RS)
  const rustStrings = await parseRustStringConstants(HEADERS_RS)
  assert.ok(rustNumeric.size > 0, `expected to parse numeric constants from ${HEADERS_RS}`)
  assert.ok(rustStrings.size > 0, `expected to parse string constants from ${HEADERS_RS}`)

  const ported = Object.entries(headers)
  assert.ok(ported.length > 0, "expected headers.ts to export at least one constant")

  for (const [name, value] of ported) {
    if (typeof value === "string") {
      const rustValue = rustStrings.get(name)
      assert.ok(
        rustValue !== undefined,
        `${name} has no matching string constant in wire/src/headers.rs`
      )
      assert.equal(value, rustValue, `${name} diverges from wire/src/headers.rs`)
      continue
    }
    const rustValue = rustNumeric.get(name)
    assert.ok(rustValue !== undefined, `${name} has no matching constant in wire/src/headers.rs`)
    assert.equal(value, rustValue, `${name} diverges from wire/src/headers.rs`)
  }

  assert.equal(
    ported.length,
    rustNumeric.size + rustStrings.size,
    "headers.ts must port every constant in wire/src/headers.rs, no more and no fewer"
  )
})

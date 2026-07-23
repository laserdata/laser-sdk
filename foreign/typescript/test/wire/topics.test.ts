import assert from "node:assert/strict"
import path from "node:path"
import { test } from "node:test"
import * as topics from "../../src/wire/topics.js"
import { parseRustStringConstants } from "./support/rust-source-constants.js"

const TOPICS_RS = path.resolve(process.cwd(), "../../wire/src/topics.rs")

void test("given_ported_topic_names_when_compared_to_wire_src_topics_rs_then_should_match_exactly", async () => {
  const rustStrings = await parseRustStringConstants(TOPICS_RS)
  assert.ok(rustStrings.size > 0, `expected to parse string constants from ${TOPICS_RS}`)

  const ported = Object.entries(topics)
  assert.ok(ported.length > 0, "expected topics.ts to export at least one constant")

  for (const [name, value] of ported) {
    const rustValue = rustStrings.get(name)
    assert.ok(rustValue !== undefined, `${name} has no matching constant in wire/src/topics.rs`)
    assert.equal(value, rustValue, `${name} diverges from wire/src/topics.rs`)
  }

  assert.equal(
    ported.length,
    rustStrings.size,
    "topics.ts must port every constant in wire/src/topics.rs, no more and no fewer"
  )
})

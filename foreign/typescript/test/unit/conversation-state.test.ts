import assert from "node:assert/strict"
import { test } from "node:test"
import { resumeOffsets } from "../../src/conversation-state.js"
import { ConversationId } from "../../src/wire/ids.js"

void test("given_a_snapshot_when_resuming_then_should_start_after_each_folded_offset", () => {
  const offsets = resumeOffsets({
    conversation: ConversationId.fromU128(1n),
    asOf: new Map([
      [0, 899n],
      [2, 41n]
    ]),
    state: new Uint8Array()
  })
  assert.deepEqual(
    offsets,
    new Map([
      [0, 900n],
      [2, 42n]
    ])
  )
})

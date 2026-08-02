import assert from "node:assert/strict"
import { test } from "node:test"
import {
  ChunkAssembler,
  FINISH_REASON_ABANDONED,
  FINISH_REASON_GAP
} from "../../src/agent/assembler.js"
import { chunkEnvelope, errorEnvelope, parseAgentId } from "../../src/wire/agent.js"
import { ChannelId, ConversationId, CorrelationId, RecordId } from "../../src/wire/ids.js"

function chunk(sequence: bigint, body: string) {
  return chunkEnvelope(
    ConversationId.fromU128(1n),
    parseAgentId("source"),
    CorrelationId.fromU128(2n),
    ChannelId.fromU128(3n),
    sequence,
    new TextEncoder().encode(body)
  )
}

void test("given_an_ordered_stream_when_fed_then_should_emit_bodies_and_terminal", () => {
  const assembler = new ChunkAssembler()
  assert.equal(assembler.feed(chunk(0n, "a"))[0]?.kind, "body")
  const terminal = { ...chunk(1n, "tail"), last: true, finishReason: "stop" }
  assert.deepEqual(
    assembler.feed(terminal).map((event) => event.kind),
    ["body", "finished"]
  )
  assert.equal(assembler.isFinished, true)
})

void test("given_duplicates_and_a_gap_when_fed_then_should_count_and_end_synthetically", () => {
  const assembler = new ChunkAssembler()
  assembler.feed(chunk(0n, "a"))
  assert.deepEqual(assembler.feed(chunk(0n, "a")), [])
  assert.equal(assembler.duplicatesDropped, 1n)
  const gap = assembler.feed(chunk(2n, "c"))[0]
  assert.deepEqual(gap, { kind: "finished", finishReason: FINISH_REASON_GAP, synthetic: true })
  assembler.feed(chunk(1n, "b"))
  assert.equal(assembler.lateDropped, 1n)
})

void test("given_an_open_stream_when_abandoned_then_should_synthesize_only_once", () => {
  const assembler = new ChunkAssembler()
  assert.deepEqual(assembler.abandon(), {
    kind: "finished",
    finishReason: FINISH_REASON_ABANDONED,
    synthetic: true
  })
  assert.equal(assembler.abandon(), undefined)
})

void test("given_an_error_terminal_when_fed_then_should_fail_and_drop_late_messages", () => {
  const assembler = new ChunkAssembler()
  const body = new TextEncoder().encode("failure")
  const failure = {
    ...errorEnvelope(
      RecordId.fromU128(4n),
      ConversationId.fromU128(1n),
      parseAgentId("source"),
      CorrelationId.fromU128(2n),
      body
    ),
    channel: ChannelId.fromU128(3n),
    sequence: 1n
  }
  assert.deepEqual(assembler.feed(failure), [{ kind: "failed", body }])
  assert.deepEqual(assembler.feed(failure), [])
  assert.equal(assembler.lateDropped, 1n)
})

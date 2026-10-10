import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { test } from "node:test"
import {
  commandFromMessageSend,
  toolCallFromRequest,
  decodeSnapshot,
  encodeSnapshot
} from "../../src/index.js"
import { AgentKind, OPERATION_CHAT, parseAgentId } from "../../src/wire/agent.js"
import { ConversationId, CorrelationId, RecordId } from "../../src/wire/ids.js"
import { decodeFoldSnapshot } from "../../src/wire/snapshot.js"
import { CodecError } from "../../src/client/errors.js"
import { TestClock } from "../../src/runtime/clock.js"
import { InvalidError } from "../../src/client/errors.js"

void test("given_foreign_request_bytes_when_converted_then_should_preserve_ids_fields_and_bytes_without_a_transport", () => {
  const record = RecordId.fromU128(1n)
  const conversation = ConversationId.fromU128(2n)
  const source = parseAgentId("bridge")
  const correlation = CorrelationId.fromU128(3n)
  const payload = new TextEncoder().encode(' { "future": [1, 2], "_meta": {"opaque": true} } ')
  const a2a = commandFromMessageSend(record, conversation, source, correlation, payload)
  const mcp = toolCallFromRequest(record, conversation, source, correlation, "inspect", payload)
  for (const envelope of [a2a, mcp]) {
    assert.equal(envelope.kind, AgentKind.Command)
    assert.equal(envelope.record, record)
    assert.equal(envelope.conversation, conversation)
    assert.equal(envelope.source, source)
    assert.equal(envelope.correlation, correlation)
    assert.deepEqual(envelope.body, payload)
    assert.equal(envelope.mustUnderstand, 0n)
  }
  assert.equal(a2a.operation, OPERATION_CHAT)
  assert.equal(mcp.tool, "inspect")
  const original = payload.slice()
  payload.fill(0)
  assert.deepEqual(a2a.body, original)
  assert.deepEqual(mcp.body, original)
})

void test("given_the_rust_snapshot_fixture_when_public_helpers_round_trip_then_should_keep_exact_bytes", async () => {
  const bytes = new Uint8Array(await readFile("../../wire/fixtures/fold_snapshot.bin"))
  const snapshot = decodeSnapshot(bytes)
  assert.deepEqual(snapshot.asOf, [
    { topicId: 2, topicCreatedAtMicros: 20n, partitionId: 0, offset: 41n },
    { topicId: 2, topicCreatedAtMicros: 20n, partitionId: 1, offset: 9n }
  ])
  assert.deepEqual(Buffer.from(encodeSnapshot(snapshot)), Buffer.from(bytes))
  assert.throws(() => decodeSnapshot(new Uint8Array([0xff])), CodecError)
})

void test("given_a_snapshot_offset_above_u64_when_decoded_then_should_reject_it", () => {
  const map = new Map<string, unknown>([
    ["stream", "agents"],
    ["stream_id", 0n],
    ["stream_created_at_micros", 100n],
    ["conversation", ConversationId.fromU128(1n).toBytes()],
    ["fold", "planner"],
    ["as_of", [[2n, 20n, 0n, 1n << 64n]]],
    ["state", new Uint8Array()]
  ])
  assert.throws(() => decodeFoldSnapshot(map, "snapshot"), CodecError)
})

void test("given_a_test_clock_at_u64_max_when_advanced_then_should_match_native_wrapping", () => {
  const clock = new TestClock(0xffff_ffff_ffff_ffffn)
  clock.advance(1n)
  assert.equal(clock.nowMicros(), 0n)
  clock.set(42n)
  assert.equal(clock.nowMicros(), 42n)
  for (const value of [-1n, 1n << 64n]) {
    assert.throws(() => new TestClock(value), InvalidError)
    assert.throws(() => {
      clock.set(value)
    }, InvalidError)
    assert.throws(() => {
      clock.advance(value)
    }, InvalidError)
  }
})

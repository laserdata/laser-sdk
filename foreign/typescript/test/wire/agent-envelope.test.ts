import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  decodeAgentEnvelope,
  encodeAgentEnvelope,
  unmetRequirements,
  validateAgentEnvelope
} from "../../src/wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

async function decodeFixture(name: string) {
  const bytes = await readFixture(name)
  const map = expectMap(decodeOne(bytes, name), name)
  return { bytes, envelope: decodeAgentEnvelope(map, name) }
}

async function assertRoundTrips(name: string) {
  const { bytes, envelope } = await decodeFixture(name)
  validateAgentEnvelope(envelope)
  const reencoded = encodeNamed(encodeAgentEnvelope(envelope))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
  return envelope
}

void test("given_the_agent_command_fixture_when_decoded_then_should_validate_and_re_encode_byte_identically", async () => {
  const envelope = await assertRoundTrips("agent_command.bin")
  assert.equal(envelope.kind, "command")
  assert.equal(envelope.source, "source-agent")
  assert.equal(envelope.target, "target-agent")
  assert.equal(envelope.idempotencyKey, "order-123-attempt-2")
  assert.equal(envelope.deadlineMicros, 1_717_171_777_000_000n)
  assert.equal(envelope.operation, "chat")
  assert.deepEqual(envelope.metadata?.get("priority"), { kind: "string", value: "high" })
})

void test("given_the_agent_command_signed_fixture_when_decoded_then_should_preserve_the_signature", async () => {
  const envelope = await assertRoundTrips("agent_command_signed.bin")
  assert.ok(envelope.signature !== undefined)
  assert.equal(envelope.signature.scheme, 1)
  assert.equal(envelope.signature.keyId.length, 8)
  assert.equal(envelope.signature.bytes.length, 64)
})

void test("given_the_agent_response_fixture_when_decoded_then_should_preserve_cause_and_usage", async () => {
  const envelope = await assertRoundTrips("agent_response.bin")
  assert.deepEqual(envelope.taskState, { kind: "known", name: "Completed" })
  assert.ok(envelope.cause !== undefined)
  assert.ok(envelope.causeAt !== undefined)
  assert.equal(envelope.causeAt.offset, 41n)
  assert.ok(envelope.usage !== undefined)
  assert.equal(envelope.usage.inputTokens, 1200n)
  assert.equal(envelope.usage.outputTokens, 256n)
  assert.equal(envelope.usage.reasoningOutputTokens, 64n)
})

void test("given_the_agent_event_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const envelope = await assertRoundTrips("agent_event.bin")
  assert.equal(envelope.kind, "event")
  assert.equal(envelope.correlation, undefined)
})

void test("given_the_agent_must_understand_fixture_when_decoded_then_should_compute_unmet_requirements", async () => {
  const envelope = await assertRoundTrips("agent_must_understand.bin")
  assert.equal(envelope.mustUnderstand, 5n)
  assert.equal(unmetRequirements(envelope, 0n), 5n)
  assert.equal(unmetRequirements(envelope, 5n), 0n)
  assert.equal(unmetRequirements(envelope, 1n), 4n)
})

void test("given_the_agent_status_card_fixture_when_decoded_then_should_allow_no_body_or_task_state", async () => {
  const envelope = await assertRoundTrips("agent_status_card.bin")
  assert.equal(envelope.operation, "card")
  assert.equal(envelope.taskState, undefined)
  assert.equal(envelope.body.length, 0)
})

void test("given_the_agent_status_task_fixture_when_decoded_then_should_require_task_state", async () => {
  const envelope = await assertRoundTrips("agent_status_task.bin")
  assert.equal(envelope.operation, "task")
  assert.deepEqual(envelope.taskState, { kind: "known", name: "Working" })
})

void test("given_the_agent_chunk_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const envelope = await assertRoundTrips("agent_chunk.bin")
  assert.equal(envelope.kind, "chunk")
  assert.equal(envelope.sequence, 7n)
  assert.equal(new TextDecoder().decode(envelope.body), "tok")
})

void test("given_the_agent_chunk_open_fixture_when_decoded_then_should_carry_the_stream_purpose_and_deadline", async () => {
  const envelope = await assertRoundTrips("agent_chunk_open.bin")
  assert.equal(envelope.sequence, 0n)
  assert.equal(envelope.operation, "reasoning")
  assert.equal(envelope.deadlineMicros, 1_717_171_777_000_000n)
})

void test("given_the_agent_chunk_terminal_fixture_when_decoded_then_should_carry_last_and_usage", async () => {
  const envelope = await assertRoundTrips("agent_chunk_terminal.bin")
  assert.equal(envelope.sequence, 8n)
  assert.equal(envelope.last, true)
  assert.equal(envelope.finishReason, "stop")
  assert.ok(envelope.usage !== undefined)
  assert.equal(envelope.usage.cacheReadInputTokens, 900n)
})

void test("given_the_agent_error_fixture_when_decoded_then_should_carry_the_encoded_error_body_as_opaque_bytes", async () => {
  const envelope = await assertRoundTrips("agent_error.bin")
  assert.equal(envelope.kind, "error")
  assert.equal(envelope.source, "source-agent")
  assert.ok(envelope.body.length > 0)
})

void test("given_the_agent_status_run_metadata_fixture_when_decoded_then_should_preserve_metadata", async () => {
  const envelope = await assertRoundTrips("agent_status_run_metadata.bin")
  assert.deepEqual(envelope.metadata?.get("run"), { kind: "string", value: "run-7" })
})

const invalidFixtures: readonly [string, RegExp][] = [
  ["agent_invalid_command_no_correlation.bin", /command requires `correlation`/],
  ["agent_invalid_error_last.bin", /`last` is invalid on error/],
  ["agent_invalid_event_task_state.bin", /`task_state` is invalid on event/],
  ["agent_invalid_response_channel.bin", /`channel` is invalid on response/],
  ["agent_invalid_status_bad_operation.bin", /status operation must be/],
  ["agent_invalid_status_no_operation.bin", /status requires `operation`/],
  ["agent_invalid_chunk_no_sequence.bin", /chunk requires `sequence`/],
  ["agent_invalid_chunk_open_no_operation.bin", /chunk requires `operation`/],
  ["agent_invalid_chunk_late_deadline.bin", /stream bound rides the opening chunk/]
]

for (const [name, expected] of invalidFixtures) {
  void test(`given_the_${name}_fixture_when_validated_then_should_reject_with_the_expected_reason`, async () => {
    const { envelope } = await decodeFixture(name)
    assert.throws(() => {
      validateAgentEnvelope(envelope)
    }, expected)
  })
}

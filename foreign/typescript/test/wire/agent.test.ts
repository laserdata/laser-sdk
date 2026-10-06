import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  agentErrorCode,
  agentErrorCodeFromCode,
  deadLetterReasonCode,
  deadLetterReasonFromCode,
  decodeAgentCard,
  decodeAgentDeadLetter,
  decodeAgentErrorBody,
  decodeAgentPresence,
  decodeBodyRef,
  decodeSignature,
  encodeAgentCard,
  encodeAgentDeadLetter,
  encodeAgentErrorBody,
  encodeAgentPresence,
  encodeBodyRef,
  encodeSignature,
  healthCode,
  healthFromCode,
  newAgentPresence,
  parseAgentId,
  parseAgentKind,
  parseIdempotencyKey,
  taskStateCode,
  taskStateDisplay,
  taskStateFromCode,
  taskStateIsTerminal,
  validateAgentPresence
} from "../../src/wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

async function roundTrip<T>(
  fixtureName: string,
  decode: (map: ReturnType<typeof expectMap>, context: string) => T,
  encode: (value: T) => Map<string, unknown>
): Promise<T> {
  const bytes = await readFixture(fixtureName)
  const map = expectMap(decodeOne(bytes, fixtureName), fixtureName)
  const value = decode(map, fixtureName)
  const reencoded = encodeNamed(encode(value))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
  return value
}

void test("given_task_state_codes_when_mapped_then_should_match_the_pinned_dictionary_and_a2a_names", () => {
  const expected: readonly [string, number, string, boolean][] = [
    ["Submitted", 1, "submitted", false],
    ["Working", 2, "working", false],
    ["InputRequired", 3, "input-required", false],
    ["Completed", 4, "completed", true],
    ["Canceled", 5, "canceled", true],
    ["Failed", 6, "failed", true],
    ["Rejected", 7, "rejected", true],
    ["AuthRequired", 8, "auth-required", false],
    ["Unknown", 9, "unknown", false]
  ]
  for (const [name, code, display, terminal] of expected) {
    const state = taskStateFromCode(code)
    assert.deepEqual(state, { kind: "known", name })
    assert.equal(taskStateCode(state), code)
    assert.equal(taskStateDisplay(state), display)
    assert.equal(taskStateIsTerminal(state), terminal)
  }

  const future = taskStateFromCode(42)
  assert.deepEqual(future, { kind: "unrecognized", code: 42 })
  assert.equal(taskStateCode(future), 42)
  assert.equal(taskStateDisplay(future), "unrecognized-42")
  assert.equal(taskStateIsTerminal(future), false)
})

void test("given_agent_id_strings_when_parsed_then_should_accept_printable_and_reject_control_or_empty", () => {
  for (const value of ["planner", "planner@acme.example", "team/planner", "a:b"]) {
    assert.equal(parseAgentId(value), value)
  }
  assert.throws(() => parseAgentId(""), /must not be empty/)
  assert.throws(() => parseAgentId("bad\nid"), /control characters/)
})

void test("given_an_idempotency_key_when_parsed_then_should_reject_empty_and_oversized", () => {
  assert.equal(parseIdempotencyKey("job-123-attempt-2"), "job-123-attempt-2")
  assert.throws(() => parseIdempotencyKey(""), /must not be empty/)
  assert.throws(() => parseIdempotencyKey("x".repeat(65)), /exceeds cap/)
  assert.throws(() => parseIdempotencyKey("é".repeat(33)), /66B, exceeds cap/)
})

void test("given_multibyte_agent_ids_when_parsed_then_should_apply_the_utf8_byte_cap", () => {
  assert.equal(parseAgentId("é".repeat(128)), "é".repeat(128))
  assert.throws(() => parseAgentId("é".repeat(129)), /258B, exceeds cap/)
})

void test("given_the_agent_error_body_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const body = await roundTrip("agent_error_body.bin", decodeAgentErrorBody, encodeAgentErrorBody)
  assert.deepEqual(body.code, { kind: "known", name: "ToolFailure" })
  assert.equal(body.message, "search timed out")
  assert.equal(body.retryable, true)
  assert.deepEqual(body.detail?.get("attempt"), { kind: "int", value: 3n })
})

void test("given_the_agent_dead_letter_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const letter = await roundTrip(
    "agent_dead_letter.bin",
    decodeAgentDeadLetter,
    encodeAgentDeadLetter
  )
  assert.deepEqual(letter.reason, { kind: "known", name: "RetryExhausted" })
  assert.equal(letter.attempts, 5)
  assert.equal(letter.detail, "handler kept failing")
  assert.equal(letter.source.streamId, 1)
  assert.equal(letter.source.topicId, 2)
  assert.equal(letter.source.partitionId, 3)
  assert.equal(letter.source.offset, 99n)
})

void test("given_the_agent_card_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("agent_card.bin")
  const map = expectMap(decodeOne(bytes, "agent_card.bin"), "agent_card.bin")
  const card = decodeAgentCard(map, "agent_card.bin")
  assert.equal(card.name, "rollout-planner")
  assert.equal(card.version, "1.4.2")
  assert.equal(card.ttlMicros, 30_000_000n)
  assert.equal(card.capabilities.length, 2)

  const [chat, planRollout] = card.capabilities
  assert.ok(chat !== undefined)
  assert.ok(planRollout !== undefined)
  assert.equal(chat.skillId, "chat")
  assert.deepEqual(chat.input, { kind: "contentType", value: "json" })
  assert.deepEqual(chat.health, { kind: "known", name: "Healthy" })
  assert.equal(planRollout.skillId, "plan_rollout")
  assert.deepEqual(planRollout.input, { kind: "schemaId", value: "reading.v1" })
  assert.deepEqual(planRollout.health, { kind: "known", name: "Degraded" })

  const reencoded = encodeNamed(encodeAgentCard(card))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_agent_presence_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const presence = await roundTrip("agent_presence.bin", decodeAgentPresence, encodeAgentPresence)
  assert.equal(presence.v, 1)
  assert.equal(presence.agent, "source-agent")
  assert.equal(presence.inbox, "rollout-planner.work")
})

void test("given_the_agent_body_ref_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const ref = await roundTrip("agent_body_ref.bin", decodeBodyRef, encodeBodyRef)
  assert.equal(ref.reference, "s3://transcripts/conv-2/msg-9")
  assert.equal(ref.sizeBytes, 4_194_304n)
  assert.equal(ref.sha256.length, 32)
})

void test("given_the_agent_signature_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const signature = await roundTrip("agent_signature.bin", decodeSignature, encodeSignature)
  assert.equal(signature.scheme, 1)
  assert.equal(signature.keyId.length, 8)
  assert.equal(signature.bytes.length, 64)
  assert.equal(signature.context, undefined)
})

void test("given_a_recognized_kind_when_parsed_then_should_pass_through", () => {
  assert.equal(parseAgentKind("command", "test"), "command")
})

void test("given_an_unrecognized_kind_when_parsed_then_should_throw_rather_than_flow_misinterpreted", () => {
  assert.throws(() => {
    parseAgentKind("bogus", "test")
  }, /not a recognized agent envelope kind/)
})

void test("given_agent_dictionary_codes_when_mapped_then_should_match_the_pinned_codes_and_pass_unknown_through", () => {
  const errors = [
    "InvalidRequest",
    "Unauthorized",
    "Unsupported",
    "DeadlineExceeded",
    "Cancelled",
    "ToolFailure",
    "Internal"
  ] as const
  errors.forEach((name, index) => {
    const code = agentErrorCodeFromCode(index + 1)
    assert.deepEqual(code, { kind: "known", name })
    assert.equal(agentErrorCode(code), index + 1)
  })
  const reasons = ["RetryExhausted", "Rejected", "DecodeFailed", "DeadlineExceeded"] as const
  reasons.forEach((name, index) => {
    const reason = deadLetterReasonFromCode(index + 1)
    assert.deepEqual(reason, { kind: "known", name })
    assert.equal(deadLetterReasonCode(reason), index + 1)
  })
  const healths = ["Healthy", "Degraded", "Unavailable"] as const
  healths.forEach((name, index) => {
    const health = healthFromCode(index + 1)
    assert.deepEqual(health, { kind: "known", name })
    assert.equal(healthCode(health), index + 1)
  })
  for (const [fromCode, toCode] of [
    [agentErrorCodeFromCode, agentErrorCode],
    [deadLetterReasonFromCode, deadLetterReasonCode],
    [healthFromCode, healthCode]
  ] as const) {
    const future = fromCode(200)
    assert.deepEqual(future, { kind: "unrecognized", code: 200 })
    assert.equal(toCode(future as never), 200)
  }
})

void test("given_a_presence_built_with_new_when_encoded_then_should_match_the_rust_fixture", async () => {
  const presence = newAgentPresence(parseAgentId("source-agent"), "rollout-planner.work")
  validateAgentPresence(presence)
  const bytes = await readFixture("agent_presence.bin")
  assert.deepEqual(Buffer.from(encodeNamed(encodeAgentPresence(presence))), Buffer.from(bytes))
})

void test("given_a_presence_inbox_over_the_cap_when_validated_then_should_reject_it", () => {
  const presence = newAgentPresence(parseAgentId("worker"), "x".repeat(257))
  assert.throws(() => {
    validateAgentPresence(presence)
  }, /inbox/)
})

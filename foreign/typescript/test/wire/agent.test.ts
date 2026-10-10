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
  decodeSessionEnd,
  decodeSessionParking,
  decodeSessionPauseRequestJson,
  encodeSessionParking,
  encodeSessionPauseRequestJson,
  validateSessionParking,
  decodeContextManifest,
  decodeContextCompaction,
  decodeContextRetrieval,
  decodeStateDelta,
  decodeStateSnapshot,
  decodeSessionStart,
  decodeSessionTransition,
  decodeTokenUsage,
  decodeBodyRef,
  decodeSignature,
  encodeAgentCard,
  encodeAgentDeadLetter,
  encodeAgentErrorBody,
  encodeAgentPresence,
  encodeSessionEnd,
  encodeContextManifest,
  encodeContextCompaction,
  encodeContextRetrieval,
  encodeStateDelta,
  encodeStateSnapshot,
  encodeSessionStart,
  encodeSessionTransition,
  encodeTokenUsage,
  encodeBodyRef,
  encodeSignature,
  healthCode,
  healthFromCode,
  newAgentPresence,
  parseAgentId,
  parseAgentKind,
  parseIdempotencyKey,
  sessionStatusFromWire,
  OPERATION_SESSION,
  statusEnvelope,
  withTaskState,
  taskStateCode,
  taskStateDisplay,
  taskStateFromCode,
  taskStateIsTerminal,
  validateAgentEnvelope,
  validateAgentPresence,
  type SessionStart
} from "../../src/wire/agent.js"
import { decodeOne, encodeNamed, encodeOne, expectMap } from "../../src/wire/cbor.js"
import { ConversationId, RecordId, decodeSessionRef, encodeSessionRef } from "../../src/wire/ids.js"

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
    ["Unknown", 9, "unknown", false],
    ["Paused", 10, "paused", false]
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

void test("given_session_bodies_when_encoded_then_should_round_trip_and_validate_status", () => {
  const start: SessionStart = {
    label: "Research",
    namespace: "agents",
    agent: parseAgentId("planner"),
    sdk: { language: "typescript", version: "0.7.0" },
    idleTimeoutMicros: 300_000_000n,
    budget: { tokens: 1000n },
    tags: ["demo"]
  }
  const body = encodeNamed(encodeSessionStart(start))
  assert.deepEqual(decodeSessionStart(expectMap(decodeOne(body, "start"), "start"), "start"), start)
  const conversation = ConversationId.fromU128(2n)
  const source = parseAgentId("planner")
  const submitted = {
    ...withTaskState(
      statusEnvelope(RecordId.fromU128(1n), conversation, source, OPERATION_SESSION),
      { kind: "known", name: "Submitted" }
    ),
    body
  }
  validateAgentEnvelope(submitted)
  assert.throws(() => {
    validateAgentEnvelope({ ...submitted, parent: ConversationId.fromU128(8n) })
  })
  const transition = encodeNamed(encodeSessionTransition({ actor: source }))
  assert.deepEqual(
    decodeSessionTransition(
      expectMap(decodeOne(transition, "transition"), "transition"),
      "transition"
    ),
    { actor: source }
  )
  validateAgentEnvelope({
    ...submitted,
    taskState: { kind: "known", name: "Paused" },
    body: transition
  })
  const end = encodeNamed(encodeSessionEnd({ reason: "done" }))
  assert.deepEqual(decodeSessionEnd(expectMap(decodeOne(end, "end"), "end"), "end"), {
    reason: "done"
  })
  assert.throws(() => {
    validateAgentEnvelope({
      ...submitted,
      taskState: { kind: "known", name: "Completed" },
      body: end
    })
  })
  validateAgentEnvelope({
    ...submitted,
    taskState: { kind: "known", name: "Completed" },
    body: end,
    last: true
  })
  assert.equal(sessionStatusFromWire("paused"), "paused")
  assert.equal(sessionStatusFromWire("future"), "unrecognized")
})

void test("given_session_fixtures_when_decoded_then_should_re_encode_byte_identically", async () => {
  const start = await roundTrip("agent_session_start.bin", decodeSessionStart, encodeSessionStart)
  assert.equal(start.sdk.version, "0.7.0")
  const transition = await roundTrip(
    "agent_session_transition.bin",
    decodeSessionTransition,
    encodeSessionTransition
  )
  assert.equal(transition.acknowledges?.offset, 9n)
  const end = await roundTrip("agent_session_end.bin", decodeSessionEnd, encodeSessionEnd)
  assert.equal(end.reason, "done")
  const parking = await roundTrip(
    "agent_session_parking.bin",
    decodeSessionParking,
    encodeSessionParking
  )
  validateSessionParking(parking)
  assert.equal(parking.source.kind === "message" ? parking.source.offset : undefined, 41n)
  assert.equal(parking.request.offset, 7n)
  assert.throws(() => {
    validateSessionParking({ ...parking, source: { kind: "memory", id: "m" } })
  })
  assert.deepEqual(decodeSessionPauseRequestJson(new TextEncoder().encode("{}")), {
    participants: []
  })
  const named = { participants: [parseAgentId("worker")] }
  assert.equal(encodeSessionPauseRequestJson(named), '{"participants":["worker"]}')
  assert.deepEqual(
    decodeSessionPauseRequestJson(new TextEncoder().encode(encodeSessionPauseRequestJson(named))),
    named
  )
  const statusBytes = await readFixture("agent_session_status.bin")
  const status = decodeOne(statusBytes, "agent_session_status.bin")
  assert.equal(status, "paused")
  if (typeof status !== "string") throw new Error("session status fixture must be a string")
  assert.equal(sessionStatusFromWire(status), "paused")
  assert.deepEqual(Buffer.from(encodeOne(status)), Buffer.from(statusBytes))
  const ref = await roundTrip("session_ref.bin", decodeSessionRef, encodeSessionRef)
  assert.equal(ref.stream, "alpha")
  assert.equal(ref.session.asU128(), 3n)
  const usage = await roundTrip("agent_usage_cost.bin", decodeTokenUsage, encodeTokenUsage)
  assert.equal(usage.costMicros, 12_500n)
})

void test("given_context_and_state_fixtures_when_decoded_then_should_match_rust_bytes", async () => {
  const manifest = await roundTrip(
    "context_manifest.bin",
    decodeContextManifest,
    encodeContextManifest
  )
  assert.equal(manifest.fragments.length, 2)
  const compaction = await roundTrip(
    "context_compaction.bin",
    decodeContextCompaction,
    encodeContextCompaction
  )
  assert.equal(compaction.summarizer.name, "summarizer")
  const retrievalBytes = await readFixture("context_retrieval.bin")
  const retrieval = decodeContextRetrieval(
    expectMap(decodeOne(retrievalBytes, "context_retrieval"), "context_retrieval"),
    "context_retrieval"
  )
  assert.equal(retrieval.items[0]?.[1], 0.5)
  assert.deepEqual(
    Buffer.from(encodeNamed(encodeContextRetrieval(retrieval), { forceFloatNumbers: true })),
    Buffer.from(retrievalBytes)
  )
  const delta = await roundTrip("state_delta.bin", decodeStateDelta, encodeStateDelta)
  assert.equal(delta.opId, "patch-4")
  const snapshot = await roundTrip("state_snapshot.bin", decodeStateSnapshot, encodeStateSnapshot)
  assert.equal(snapshot.baseRevision, 4n)
})

void test("given_invalid_context_digest_or_patch_op_when_encoded_then_should_reject", async () => {
  const manifest = await roundTrip(
    "context_manifest.bin",
    decodeContextManifest,
    encodeContextManifest
  )
  const memory = manifest.fragments.find((fragment) => fragment.kind === "memory")
  assert.ok(memory)
  assert.throws(() =>
    encodeContextManifest({
      ...manifest,
      fragments: [{ ...memory, digest: new Uint8Array(31) }]
    })
  )
  const delta = await roundTrip("state_delta.bin", decodeStateDelta, encodeStateDelta)
  assert.throws(() =>
    encodeStateDelta({ ...delta, patch: [{ op: "unknown", path: "/a" }] as never })
  )
})

void test("given_inexact_state_integer_when_encoded_then_should_reject", () => {
  assert.throws(() =>
    encodeStateSnapshot({ baseRevision: 0n, document: { large: 9_007_199_254_740_992 } })
  )
  assert.throws(() =>
    encodeStateDelta({
      baseRevision: 0n,
      patch: [{ op: "add", path: "/large", value: 9_007_199_254_740_992 }],
      opId: "patch-1"
    })
  )
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

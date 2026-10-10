import assert from "node:assert/strict"
import { test } from "node:test"
import { IdError, ProvenanceError } from "../../src/client/errors.js"
import type { HeaderValue } from "../../src/stream/header-value.js"
import {
  decodeProvenanceHeaders,
  encodeProvenanceHeaders,
  provenancePartitionKey,
  type Provenance
} from "../../src/provenance/provenance.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { CONVERSATION_ID, DEADLINE, HEADER_VALUE_MAX } from "../../src/wire/headers.js"

void test("given_provenance_when_round_tripped_through_headers_then_should_preserve_every_field", () => {
  const conversationId = ConversationId.new()
  const provenance: Provenance = {
    conversationId,
    causalParent: { partitionId: 2, offset: 7n },
    agent: AgentId.new("planner"),
    targetAgentId: AgentId.new("executor"),
    idempotencyKey: "key-1",
    correlationId: "corr-1",
    fenceToken: 7n,
    usage: { inputTokens: 10n, outputTokens: 20n }
  }

  const headers = encodeProvenanceHeaders(provenance)
  const back = decodeProvenanceHeaders(headers)

  assert.ok(back.conversationId.equals(conversationId))
  assert.deepEqual(back.causalParent, { partitionId: 2, offset: 7n })
  assert.equal(back.agent?.asStr(), "planner")
  assert.equal(back.targetAgentId?.asStr(), "executor")
  assert.equal(back.idempotencyKey, "key-1")
  assert.equal(back.correlationId, "corr-1")
  assert.equal(back.fenceToken, 7n)
  assert.ok(back.usage !== undefined)
  assert.equal(back.usage.inputTokens, 10n)
  assert.equal(back.usage.outputTokens, 20n)
  assert.equal(provenancePartitionKey(provenance), conversationId.toString())
})

void test("given_a_malformed_fence_header_when_decoded_then_should_error_not_skip", () => {
  const headers = new Map<string, HeaderValue>([
    [CONVERSATION_ID, { kind: "string", value: ConversationId.new().toString() }],
    ["agdx.fence", { kind: "string", value: "not-a-number" }]
  ])
  assert.throws(() => decodeProvenanceHeaders(headers), ProvenanceError)
})

void test("given_a_message_without_a_conversation_id_when_decoded_then_should_error", () => {
  assert.throws(() => decodeProvenanceHeaders(new Map()), ProvenanceError)
})

void test("given_a_typed_non_string_header_when_decoded_then_should_skip_it_not_error", () => {
  const conversationId = ConversationId.new()
  const headers = new Map<string, HeaderValue>([
    [CONVERSATION_ID, { kind: "string", value: conversationId.toString() }],
    ["agdx.ct", { kind: "uint8", value: 7 }]
  ])
  const provenance = decodeProvenanceHeaders(headers)
  assert.ok(provenance.conversationId.equals(conversationId))
})

void test("given_a_known_key_with_the_wrong_value_kind_when_decoded_then_should_error", () => {
  const headers = new Map<string, HeaderValue>([
    [CONVERSATION_ID, { kind: "string", value: ConversationId.new().toString() }],
    [DEADLINE, { kind: "string", value: "42" }]
  ])
  assert.throws(() => decodeProvenanceHeaders(headers), ProvenanceError)
})

void test("given_typed_numbers_and_a_broadcast_addressee_when_round_tripped_then_should_keep_the_numbers_and_no_target", () => {
  const conversationId = ConversationId.new()
  const encoded = encodeProvenanceHeaders({
    conversationId,
    fenceToken: 7n,
    deadlineMicros: 42n,
    usage: { inputTokens: 3n, outputTokens: 4n, costUsd: 0.5 }
  })
  assert.deepEqual(encoded.get(DEADLINE), { kind: "uint64", value: 42n })
  const decoded = decodeProvenanceHeaders(
    new Map([...encoded, ["agdx.to", { kind: "string", value: "*" }]])
  )
  assert.equal(decoded.fenceToken, 7n)
  assert.equal(decoded.deadlineMicros, 42n)
  assert.equal(decoded.usage?.costUsd, 0.5)
  assert.equal(decoded.targetAgentId, undefined)
})

void test("given_an_oversized_idempotency_key_when_encoded_then_should_report_a_clear_error", () => {
  const provenance: Provenance = {
    conversationId: ConversationId.new(),
    idempotencyKey: "x".repeat(HEADER_VALUE_MAX + 1)
  }
  assert.throws(() => encodeProvenanceHeaders(provenance), ProvenanceError)
})

void test("given_an_empty_idempotency_key_when_encoded_then_should_report_a_clear_error", () => {
  const provenance: Provenance = {
    conversationId: ConversationId.new(),
    idempotencyKey: ""
  }
  assert.throws(() => encodeProvenanceHeaders(provenance), ProvenanceError)
})

void test("given_a_non_finite_cost_when_encoded_then_should_report_a_clear_error", () => {
  const provenance: Provenance = {
    conversationId: ConversationId.new(),
    usage: { costUsd: Number.POSITIVE_INFINITY }
  }
  assert.throws(() => encodeProvenanceHeaders(provenance), ProvenanceError)
})

void test("given_an_unparseable_agent_header_when_decoded_then_should_wrap_the_id_error", () => {
  const headers = new Map<string, HeaderValue>([
    [CONVERSATION_ID, { kind: "string", value: ConversationId.new().toString() }],
    ["gen_ai.agent.id", { kind: "string", value: "" }]
  ])
  assert.throws(
    () => decodeProvenanceHeaders(headers),
    (error: unknown) =>
      error instanceof ProvenanceError &&
      error.kind === "provenance" &&
      error.cause instanceof IdError &&
      error.message === "identifier must not be empty"
  )
})

void test("given_a_missing_conversation_id_when_decoded_then_should_name_the_header", () => {
  assert.throws(() => decodeProvenanceHeaders(new Map()), {
    message: "missing required header `gen_ai.conversation.id`"
  })
})

void test("given_the_current_header_names_when_checked_then_they_stay_current", () => {
  assert.equal(CONVERSATION_ID, "gen_ai.conversation.id")
  assert.equal(DEADLINE, "agdx.deadline")
})

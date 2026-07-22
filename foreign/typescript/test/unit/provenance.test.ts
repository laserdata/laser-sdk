import assert from "node:assert/strict"
import { test } from "node:test"
import { CodecError, InvalidError } from "../../src/client/errors.js"
import type { IggyHeaderValue } from "../../src/iggy/apache-iggy.js"
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
  assert.equal(back.agent?.asString(), "planner")
  assert.equal(back.targetAgentId?.asString(), "executor")
  assert.equal(back.idempotencyKey, "key-1")
  assert.equal(back.correlationId, "corr-1")
  assert.equal(back.fenceToken, 7n)
  assert.ok(back.usage !== undefined)
  assert.equal(back.usage.inputTokens, 10n)
  assert.equal(back.usage.outputTokens, 20n)
  assert.equal(provenancePartitionKey(provenance), conversationId.toString())
})

void test("given_a_malformed_fence_header_when_decoded_then_should_error_not_skip", () => {
  const headers = new Map<string, IggyHeaderValue>([
    [CONVERSATION_ID, { kind: "string", value: ConversationId.new().toString() }],
    ["agdx.fence", { kind: "string", value: "not-a-number" }]
  ])
  assert.throws(() => decodeProvenanceHeaders(headers), CodecError)
})

void test("given_a_message_without_a_conversation_id_when_decoded_then_should_error", () => {
  assert.throws(() => decodeProvenanceHeaders(new Map()), CodecError)
})

void test("given_a_typed_non_string_header_when_decoded_then_should_skip_it_not_error", () => {
  const conversationId = ConversationId.new()
  const headers = new Map<string, IggyHeaderValue>([
    [CONVERSATION_ID, { kind: "string", value: conversationId.toString() }],
    ["agdx.ct", { kind: "uint8", value: 7 }]
  ])
  const provenance = decodeProvenanceHeaders(headers)
  assert.ok(provenance.conversationId.equals(conversationId))
})

void test("given_a_known_key_with_a_non_string_value_when_decoded_then_should_error", () => {
  const headers = new Map<string, IggyHeaderValue>([
    [CONVERSATION_ID, { kind: "string", value: ConversationId.new().toString() }],
    [DEADLINE, { kind: "uint64", value: 42n }]
  ])
  assert.throws(() => decodeProvenanceHeaders(headers), CodecError)
})

void test("given_an_oversized_idempotency_key_when_encoded_then_should_report_a_clear_error", () => {
  const provenance: Provenance = {
    conversationId: ConversationId.new(),
    idempotencyKey: "x".repeat(HEADER_VALUE_MAX + 1)
  }
  assert.throws(() => encodeProvenanceHeaders(provenance), InvalidError)
})

void test("given_an_empty_idempotency_key_when_encoded_then_should_report_a_clear_error", () => {
  const provenance: Provenance = {
    conversationId: ConversationId.new(),
    idempotencyKey: ""
  }
  assert.throws(() => encodeProvenanceHeaders(provenance), InvalidError)
})

void test("given_a_non_finite_cost_when_encoded_then_should_report_a_clear_error", () => {
  const provenance: Provenance = {
    conversationId: ConversationId.new(),
    usage: { costUsd: Number.POSITIVE_INFINITY }
  }
  assert.throws(() => encodeProvenanceHeaders(provenance), InvalidError)
})

void test("given_the_current_header_names_when_checked_then_they_stay_current", () => {
  assert.equal(CONVERSATION_ID, "gen_ai.conversation.id")
  assert.equal(DEADLINE, "agdx.deadline")
})

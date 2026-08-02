import assert from "node:assert/strict"
import { test } from "node:test"
import {
  acceptFence,
  agentMessageBody,
  contentTypeOf,
  decodeAgentMessage,
  dedupKey,
  DEFAULT_RETRY_POLICY,
  provenanceAndEnvelope,
  provenanceFromEnvelope,
  isRetryable,
  retryBackoff,
  retryDelayMs,
  SlidingWindow,
  type FenceEntry,
  type FenceSweepState,
  type ReceivedAgentMessage
} from "../../src/agent/reliable-consumer.js"
import { RejectedError, TransportError } from "../../src/client/errors.js"
import type { IggyHeaderValue } from "../../src/iggy/apache-iggy.js"
import type { Provenance } from "../../src/provenance/provenance.js"
import { encodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import { AgentId, ConversationId as SdkConversationId } from "../../src/types/ids.js"
import {
  commandEnvelope,
  encodeAgentEnvelope,
  parseAgentId,
  withMetadata
} from "../../src/wire/agent.js"
import { CorrelationId, ConversationId, RecordId } from "../../src/wire/ids.js"
import { AGENT_VERSION, CONTENT_TYPE } from "../../src/wire/headers.js"
import { encodeNamed } from "../../src/wire/cbor.js"

function envelopePayload(envelope: ReturnType<typeof commandEnvelope>): Uint8Array {
  return encodeNamed(encodeAgentEnvelope(envelope))
}

void test("given_a_small_positive_fence_when_decoded_then_should_preserve_the_token", () => {
  const envelope = withMetadata(
    commandEnvelope(
      RecordId.fromU128(3n),
      ConversationId.fromU128(1n),
      parseAgentId("orchestrator"),
      CorrelationId.fromU128(2n),
      new Uint8Array()
    ),
    "agdx.fence",
    { kind: "int", value: 7n }
  )
  assert.equal(provenanceFromEnvelope(envelope).fenceToken, 7n)
})

void test("given_a_seen_key_when_observed_again_then_should_report_a_duplicate", async () => {
  const window = new SlidingWindow(8)
  assert.equal(await window.observe("a"), true)
  assert.equal(await window.observe("a"), false)
  assert.equal(await window.observe("b"), true)
})

void test("given_a_full_window_when_observing_then_should_evict_the_oldest_key", async () => {
  const window = new SlidingWindow(2)
  assert.equal(await window.observe("a"), true)
  assert.equal(await window.observe("b"), true)
  assert.equal(await window.observe("c"), true)
  assert.equal(await window.observe("a"), true)
})

void test("given_increasing_attempts_when_computing_backoff_then_should_grow_and_stay_bounded", () => {
  const policy = retryBackoff(5, 100)
  assert.equal(retryDelayMs(policy, 0), 100)
  assert.equal(retryDelayMs(policy, 1), 200)
  assert.equal(retryDelayMs(policy, 2), 400)
  assert.ok(retryDelayMs(policy, 60) >= retryDelayMs(policy, 2))
  assert.deepEqual(DEFAULT_RETRY_POLICY, { maxAttempts: 5, baseDelayMs: 200 })
})

void test("given_a_dedup_key_when_an_agent_is_set_then_should_scope_it_by_agent", () => {
  const provenance: Provenance = {
    conversationId: SdkConversationId.new(),
    agent: AgentId.new("planner"),
    idempotencyKey: "op-1"
  }
  assert.equal(dedupKey(provenance), "planner\u001fop-1")
})

void test("given_a_dedup_key_when_no_agent_is_set_then_should_use_the_bare_key", () => {
  const provenance: Provenance = {
    conversationId: SdkConversationId.new(),
    idempotencyKey: "op-1"
  }
  assert.equal(dedupKey(provenance), "op-1")
})

void test("given_no_idempotency_key_when_computing_a_dedup_key_then_should_return_undefined", () => {
  assert.equal(dedupKey({ conversationId: SdkConversationId.new() }), undefined)
})

void test("given_a_fresh_fence_when_accepted_then_should_advance_the_high_water_mark", () => {
  const highWater = new Map<string, FenceEntry>()
  const sweep: FenceSweepState = { lastSweepMicros: 0n }
  assert.equal(acceptFence(highWater, sweep, "task-1", 5n, 1_000n), true)
  assert.equal(acceptFence(highWater, sweep, "task-1", 5n, 2_000n), true)
  assert.equal(acceptFence(highWater, sweep, "task-1", 3n, 3_000n), false)
  assert.equal(acceptFence(highWater, sweep, "task-1", 9n, 4_000n), true)
})

void test("given_an_oversized_idle_fence_map_when_swept_then_should_retain_only_active_entries", () => {
  const highWater = new Map<string, FenceEntry>()
  for (let index = 0; index <= 16_384; index += 1) {
    highWater.set(`idle-${String(index)}`, { fence: 1n, touchedMicros: 0n })
  }
  const sweep: FenceSweepState = { lastSweepMicros: 0n }
  assert.equal(acceptFence(highWater, sweep, "active", 2n, 600_000_001n), true)
  assert.deepEqual([...highWater.keys()], ["active"])
})

void test("given_an_agdx_message_when_decoded_then_should_synthesize_provenance_from_the_envelope", () => {
  const conversation = ConversationId.fromU128(42n)
  const envelope = commandEnvelope(
    RecordId.fromU128(1n),
    conversation,
    parseAgentId("planner"),
    CorrelationId.fromU128(9n),
    new TextEncoder().encode("do-the-thing")
  )
  const payload = envelopePayload(envelope)
  const headers = new Map<string, IggyHeaderValue>([[AGENT_VERSION, { kind: "uint32", value: 1 }]])
  const { provenance, envelope: decoded } = provenanceAndEnvelope({
    payload,
    partitionId: 0,
    offset: 1n,
    headers
  })
  assert.ok(provenance.conversationId.equals(SdkConversationId.parse(conversation.toString())))
  assert.equal(provenance.agent?.asString(), "planner")
  assert.ok(decoded !== undefined)
  assert.deepEqual(
    agentMessageBody({
      provenance,
      payload,
      id: { partitionId: 0, offset: 1n },
      envelope: decoded
    }),
    new TextEncoder().encode("do-the-thing")
  )
})

void test("given_a_plain_message_when_decoded_then_should_read_provenance_from_headers", () => {
  const conversationId = SdkConversationId.new()
  const headers = encodeProvenanceHeaders({ conversationId })
  const { provenance, envelope } = provenanceAndEnvelope({
    payload: new TextEncoder().encode("hi"),
    partitionId: 0,
    offset: 0n,
    headers
  })
  assert.ok(provenance.conversationId.equals(conversationId))
  assert.equal(envelope, undefined)
})

void test("given_a_content_type_header_when_read_then_should_map_the_code", () => {
  const headers = new Map<string, IggyHeaderValue>([[CONTENT_TYPE, { kind: "uint8", value: 1 }]])
  assert.equal(
    contentTypeOf({ payload: new Uint8Array(), partitionId: 0, offset: 0n, headers }),
    "json"
  )
})

void test("given_an_undecodable_payload_when_decoding_an_agent_message_then_should_return_the_error_with_the_raw_payload", () => {
  const received: ReceivedAgentMessage = {
    payload: new TextEncoder().encode("not cbor at all, definitely"),
    partitionId: 0,
    offset: 5n,
    headers: new Map([[AGENT_VERSION, { kind: "uint32", value: 1 }]])
  }
  const result = decodeAgentMessage(received)
  assert.equal(result.kind, "error")
  assert.deepEqual(result.payload, received.payload)
})

void test("given_permanent_and_transient_errors_when_classified_then_should_retry_only_transient_failures", () => {
  assert.equal(isRetryable(new RejectedError("no")), false)
  assert.equal(isRetryable(new TransportError("temporary", true)), true)
  assert.equal(isRetryable(new TransportError("permanent", false)), false)
})

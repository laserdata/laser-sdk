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
  ReliableConsumer,
  SlidingWindow,
  type FenceEntry,
  type FenceSweepState,
  type ReceivedAgentMessage
} from "../../src/agent/reliable-consumer.js"
import {
  AmbiguousMutationError,
  AuthzExecutionError,
  GraphExecutionError,
  QueryExecutionError,
  RejectedError,
  TransportError
} from "../../src/client/errors.js"
import { CancelledError } from "../../src/client/errors.js"
import {
  INTERNAL_COMMIT_HANDLED,
  INTERNAL_NATIVE_CONSUMER,
  INTERNAL_TRANSPORT
} from "../../src/client/internals.js"
import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { SessionConfig } from "../../src/session.js"
import type { Laser } from "../../src/client/laser.js"
import type { HeaderValue } from "../../src/stream/header-value.js"
import { KeyRegistry, SigningKey } from "../../src/signing.js"
import type { Consumer, ConsumerMessage } from "../../src/stream/consumer.js"
import { ConsumerGroupName } from "../../src/types/ids.js"
import { AGENT_OP_VERSION } from "../../src/wire/codes.js"
import { ContentType, contentTypeCode } from "../../src/wire/content.js"
import type { Provenance } from "../../src/provenance/provenance.js"
import { encodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import { AgentId, ConversationId as SdkConversationId } from "../../src/types/ids.js"
import {
  commandEnvelope,
  encodeAgentEnvelope,
  parseAgentId,
  responseEnvelope,
  withMetadata,
  withOperation
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
  const headers = new Map<string, HeaderValue>([[AGENT_VERSION, { kind: "uint32", value: 1 }]])
  const { provenance, envelope: decoded } = provenanceAndEnvelope({
    payload,
    partitionId: 0,
    offset: 1n,
    headers
  })
  assert.ok(provenance.conversationId.equals(SdkConversationId.parse(conversation.toString())))
  assert.equal(provenance.agent?.asStr(), "planner")
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
  const headers = new Map<string, HeaderValue>([[CONTENT_TYPE, { kind: "uint8", value: 1 }]])
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
  assert.equal(isRetryable(new AmbiguousMutationError("unknown")), false)
  assert.equal(isRetryable(new TransportError("temporary", true)), true)
  assert.equal(isRetryable(new TransportError("permanent", false)), false)
  assert.equal(isRetryable(new QueryExecutionError("busy", { kind: "unavailable" })), true)
  assert.equal(isRetryable(new QueryExecutionError("fault", { kind: "backend" })), false)
  assert.equal(isRetryable(new GraphExecutionError("invalid", { kind: "invalidName" })), false)
  assert.equal(isRetryable(new AuthzExecutionError("forbidden", { kind: "unauthorized" })), false)
})

void test("given_a_replayed_signed_record_when_consumed_then_should_handle_it_once", async () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(61))
  const registry = new KeyRegistry()
  registry.enroll("caller", key.verifyingKey())
  const envelope = commandEnvelope(
    RecordId.fromU128(0x77n),
    ConversationId.fromU128(5n),
    parseAgentId("caller"),
    CorrelationId.fromU128(6n),
    new TextEncoder().encode("rotate")
  )
  const contentType = contentTypeCode(ContentType.Cbor)
  const signed = {
    ...envelope,
    signature: key.signWithContext(envelope, { contentType, agentVersion: AGENT_OP_VERSION })
  }
  // The same signed bytes, written twice to the topic.
  const deliveries = [0n, 1n].map(
    (offset) =>
      ({
        partitionId: 0,
        position: { partitionId: 0, offset },
        timestampMicros: BigInt(Date.now()) * 1000n,
        payload: envelopePayload(signed),
        headers: new Map<string, HeaderValue>([
          [AGENT_VERSION, { kind: "uint32", value: AGENT_OP_VERSION }],
          [CONTENT_TYPE, { kind: "uint8", value: contentType }]
        ])
      }) as unknown as ConsumerMessage
  )
  const stop = new AbortController()
  const consumer = {
    nextWithin: (_waitMs: number, options?: { signal?: AbortSignal }) => {
      const next = deliveries.shift()
      if (next !== undefined) return Promise.resolve(next)
      if (deliveries.length === 0) stop.abort()
      return new Promise<ConsumerMessage>((_resolve, reject) => {
        const abort = (): void => {
          reject(new CancelledError("stopped"))
        }
        options?.signal?.addEventListener("abort", abort, { once: true })
        if (options?.signal?.aborted === true) abort()
      })
    },
    commit: () => Promise.resolve(),
    [INTERNAL_COMMIT_HANDLED]: () => Promise.resolve(),
    shutdown: () => Promise.resolve()
  } as unknown as Consumer
  const laser = {
    defaultStream: "agents",
    topic: () => ({
      consumerGroup: () => ({ [INTERNAL_NATIVE_CONSUMER]: () => Promise.resolve(consumer) })
    }),
    capabilities: () => Promise.resolve({ ...OPEN_CAPABILITIES, hello: "rejected" }),
    sessions: (config?: SessionConfig) => ({
      config: config ?? new SessionConfig(),
      indexesSessions: () => Promise.resolve(false)
    }),
    [INTERNAL_TRANSPORT]: () => ({
      findTopicPartitionCount: () => Promise.resolve(undefined),
      resolveStreamTopicIds: () => Promise.resolve({ streamId: 1, topicId: 2 })
    })
  } as unknown as Laser
  let handled = 0
  await new ReliableConsumer({
    group: ConsumerGroupName.forAgent(AgentId.new("worker")),
    topic: "commands",
    verifier: registry,
    shutdownGraceMs: 100
  }).run(
    laser,
    {
      handle: () => {
        handled += 1
        return Promise.resolve()
      }
    },
    { signal: stop.signal }
  )
  assert.equal(handled, 1)
})

void test("given_records_of_every_dispatch_when_consumed_then_only_work_for_a_served_operation_should_reach_the_handler", async () => {
  const conversation = ConversationId.fromU128(9n)
  const caller = parseAgentId("caller")
  const command = (record: bigint, operation: string) =>
    withOperation(
      commandEnvelope(
        RecordId.fromU128(record),
        conversation,
        caller,
        CorrelationId.fromU128(record),
        new TextEncoder().encode(operation)
      ),
      operation
    )
  const envelopeHeaders = new Map<string, HeaderValue>([
    [AGENT_VERSION, { kind: "uint32", value: AGENT_OP_VERSION }],
    [CONTENT_TYPE, { kind: "uint8", value: contentTypeCode(ContentType.Raw) }]
  ])
  const sdkConversation = SdkConversationId.parse(conversation.toString())
  const generic = (provenance: Provenance, text: string) => ({
    payload: new TextEncoder().encode(text),
    headers: new Map(encodeProvenanceHeaders(provenance))
  })
  const records = [
    { payload: envelopePayload(command(1n, "summarize")), headers: envelopeHeaders },
    { payload: envelopePayload(command(2n, "chat")), headers: envelopeHeaders },
    {
      payload: encodeNamed(
        encodeAgentEnvelope(
          responseEnvelope(
            RecordId.fromU128(3n),
            conversation,
            caller,
            CorrelationId.fromU128(1n),
            new TextEncoder().encode("answer")
          )
        )
      ),
      headers: envelopeHeaders
    },
    generic(
      {
        conversationId: sdkConversation,
        causalParent: { partitionId: 0, offset: 0n },
        correlationId: "0:0"
      },
      "reply"
    ),
    generic({ conversationId: sdkConversation, targetAgentId: AgentId.new("other") }, "other"),
    generic({ conversationId: sdkConversation }, "plain")
  ]
  const deliveries = records.map(
    (record, offset) =>
      ({
        partitionId: 0,
        position: { partitionId: 0, offset: BigInt(offset) },
        timestampMicros: BigInt(Date.now()) * 1000n,
        ...record
      }) as unknown as ConsumerMessage
  )
  const stop = new AbortController()
  let committed = 0
  const consumer = {
    nextWithin: (_waitMs: number, options?: { signal?: AbortSignal }) => {
      const next = deliveries.shift()
      if (next !== undefined) return Promise.resolve(next)
      stop.abort()
      return new Promise<ConsumerMessage>((_resolve, reject) => {
        const abort = (): void => {
          reject(new CancelledError("stopped"))
        }
        options?.signal?.addEventListener("abort", abort, { once: true })
        if (options?.signal?.aborted === true) abort()
      })
    },
    commit: () => {
      committed += 1
      return Promise.resolve()
    },
    [INTERNAL_COMMIT_HANDLED]: () => {
      committed += 1
      return Promise.resolve()
    },
    shutdown: () => Promise.resolve()
  } as unknown as Consumer
  const laser = {
    defaultStream: "agents",
    topic: () => ({
      consumerGroup: () => ({ [INTERNAL_NATIVE_CONSUMER]: () => Promise.resolve(consumer) })
    }),
    capabilities: () => Promise.resolve({ ...OPEN_CAPABILITIES, hello: "rejected" }),
    sessions: (config?: SessionConfig) => ({
      config: config ?? new SessionConfig(),
      indexesSessions: () => Promise.resolve(false)
    }),
    [INTERNAL_TRANSPORT]: () => ({
      findTopicPartitionCount: () => Promise.resolve(undefined),
      resolveStreamTopicIds: () => Promise.resolve({ streamId: 1, topicId: 2 })
    })
  } as unknown as Laser
  const handled: string[] = []
  await new ReliableConsumer({
    group: ConsumerGroupName.forAgent(AgentId.new("worker")),
    topic: "agent.sessions",
    agent: AgentId.new("worker"),
    operations: ["summarize"],
    shutdownGraceMs: 100
  }).run(
    laser,
    {
      handle: (message) => {
        handled.push(new TextDecoder().decode(agentMessageBody(message)))
        return Promise.resolve()
      }
    },
    { signal: stop.signal }
  )
  assert.deepEqual(handled, ["summarize", "plain"])
  assert.equal(committed, records.length)
})

void test("given_an_enveloped_child_record_when_decoded_then_should_carry_its_parent_and_root", () => {
  const parent = ConversationId.fromU128(11n)
  const root = ConversationId.fromU128(12n)
  const envelope = {
    ...commandEnvelope(
      RecordId.fromU128(3n),
      ConversationId.fromU128(1n),
      parseAgentId("orchestrator"),
      CorrelationId.fromU128(2n),
      new Uint8Array()
    ),
    parent,
    root
  }
  const provenance = provenanceFromEnvelope(envelope)
  assert.equal(provenance.parentConversationId?.toString(), parent.toString())
  assert.equal(provenance.rootConversationId?.toString(), root.toString())
})

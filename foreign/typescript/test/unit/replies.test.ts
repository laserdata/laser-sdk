import assert from "node:assert/strict"
import { test } from "node:test"
import { TimeoutError } from "../../src/client/errors.js"
import type {
  ConsumerTarget,
  IggyHeaderValue,
  LaserTransport,
  PolledMessage
} from "../../src/iggy/apache-iggy.js"
import { agentMessageBody } from "../../src/agent/reliable-consumer.js"
import { ReplyHub } from "../../src/agent/replies.js"
import { encodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import { KeyRegistry, SigningKey } from "../../src/signing.js"
import { ConversationId } from "../../src/types/ids.js"
import type { PollingStrategy } from "../../src/stream/polling-strategy.js"
import { encodeAgentEnvelope, parseAgentId, responseEnvelope } from "../../src/wire/agent.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { AGENT_OP_VERSION } from "../../src/wire/codes.js"
import { ContentType, contentTypeCode } from "../../src/wire/content.js"
import { AGENT_VERSION, CONTENT_TYPE } from "../../src/wire/headers.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId
} from "../../src/wire/ids.js"

function replyMessage(correlationId: string, offset: bigint): PolledMessage {
  const headers = encodeProvenanceHeaders({
    conversationId: ConversationId.new(),
    correlationId
  })
  return {
    payload: new TextEncoder().encode("reply-body"),
    partitionId: 0,
    offset,
    headers
  }
}

interface ScriptedPoll {
  readonly strategy: PollingStrategy["kind"]
  readonly result: readonly PolledMessage[]
}

function fakeTransport(script: readonly ScriptedPoll[]): LaserTransport {
  const remaining = [...script]
  return {
    kind: "apache-iggy",
    get iggyClient(): never {
      throw new Error("unused")
    },
    sendManaged: () => Promise.reject(new Error("unused")),
    ensureStream: () => Promise.reject(new Error("unused")),
    ensureTopic: () => Promise.reject(new Error("unused")),
    findTopicPartitionCount: () => Promise.resolve(1),
    getTopicPartitionCount: () => Promise.resolve(1),
    sendMessages: () => Promise.reject(new Error("unused")),
    sendMessageWithHeaders: () => Promise.reject(new Error("unused")),
    sendMessagesWithHeaders: () => Promise.reject(new Error("unused")),
    pollMessages(
      _streamId: string,
      _topicId: string,
      _target: ConsumerTarget,
      strategy: PollingStrategy
    ): Promise<readonly PolledMessage[]> {
      const next = remaining.find((entry) => entry.strategy === strategy.kind)
      if (next === undefined) return Promise.resolve([])
      remaining.splice(remaining.indexOf(next), 1)
      return Promise.resolve(next.result)
    },
    storeOffset: () => Promise.reject(new Error("unused")),
    joinConsumerGroup: () => Promise.reject(new Error("unused")),
    leaveConsumerGroup: () => Promise.reject(new Error("unused")),
    close: () => Promise.reject(new Error("unused"))
  }
}

void test("given_a_matching_reply_when_dispatched_then_should_resolve_the_waiting_ticket", async () => {
  const transport = fakeTransport([
    { strategy: "last", result: [] },
    { strategy: "offset", result: [replyMessage("corr-1", 5n)] }
  ])
  const hub = await ReplyHub.create(transport, "stream", "replies")
  try {
    const ticket = hub.subscribe("corr-1")
    const reply = await ticket.wait(2_000)
    assert.equal(reply.provenance.correlationId, "corr-1")
    assert.equal(reply.id.offset, 5n)
  } finally {
    hub.stop()
  }
})

void test("given_no_matching_reply_when_waiting_then_should_time_out", async () => {
  const transport = fakeTransport([{ strategy: "last", result: [] }])
  const hub = await ReplyHub.create(transport, "stream", "replies")
  try {
    const ticket = hub.subscribe("corr-never")
    await assert.rejects(ticket.wait(50), TimeoutError)
  } finally {
    hub.stop()
  }
})

void test("given_a_reply_for_an_unknown_correlation_when_dispatched_then_should_be_dropped_silently", async () => {
  const transport = fakeTransport([
    { strategy: "last", result: [] },
    { strategy: "offset", result: [replyMessage("someone-elses-request", 1n)] }
  ])
  const hub = await ReplyHub.create(transport, "stream", "replies")
  try {
    const ticket = hub.subscribe("corr-mine")
    await assert.rejects(ticket.wait(50), TimeoutError)
  } finally {
    hub.stop()
  }
})

void test("given_two_concurrent_waiters_when_replies_arrive_then_each_should_resolve_its_own_ticket", async () => {
  const transport = fakeTransport([
    { strategy: "last", result: [] },
    { strategy: "offset", result: [replyMessage("corr-a", 1n), replyMessage("corr-b", 2n)] }
  ])
  const hub = await ReplyHub.create(transport, "stream", "replies")
  try {
    const ticketA = hub.subscribe("corr-a")
    const ticketB = hub.subscribe("corr-b")
    const [replyA, replyB] = await Promise.all([ticketA.wait(2_000), ticketB.wait(2_000)])
    assert.equal(replyA.provenance.correlationId, "corr-a")
    assert.equal(replyB.provenance.correlationId, "corr-b")
  } finally {
    hub.stop()
  }
})

void test("given_a_stream_waiter_when_two_replies_arrive_then_should_deliver_both_in_order", async () => {
  const transport = fakeTransport([
    { strategy: "last", result: [] },
    {
      strategy: "offset",
      result: [replyMessage("corr-stream", 3n), replyMessage("corr-stream", 4n)]
    }
  ])
  const hub = await ReplyHub.create(transport, "stream", "replies")
  try {
    const ticket = hub.subscribeStream("corr-stream")
    const first = await ticket.next(2_000)
    const second = await ticket.next(2_000)
    assert.equal(first.id.offset, 3n)
    assert.equal(second.id.offset, 4n)
    ticket.cancel()
  } finally {
    hub.stop()
  }
})

// An AGDX response echoing `correlation`, optionally signed, carried exactly as
// the broker would deliver it: named-CBOR payload, AGDX headers, a record
// timestamp for the key-lifecycle check.
function agdxReply(
  correlation: CorrelationId,
  body: string,
  offset: bigint,
  key?: SigningKey
): PolledMessage {
  const envelope = responseEnvelope(
    RecordId.fromU128(0x1000n + offset),
    WireConversationId.fromU128(0x2000n),
    parseAgentId("tool"),
    correlation,
    new TextEncoder().encode(body)
  )
  const signature = key?.signWithContext(envelope, {
    contentType: contentTypeCode(ContentType.Cbor),
    agentVersion: AGENT_OP_VERSION
  })
  const headers = new Map<string, IggyHeaderValue>([
    [AGENT_VERSION, { kind: "uint32", value: AGENT_OP_VERSION }],
    [CONTENT_TYPE, { kind: "uint8", value: contentTypeCode(ContentType.Cbor) }]
  ])
  return {
    payload: encodeNamed(
      encodeAgentEnvelope(signature === undefined ? envelope : { ...envelope, signature })
    ),
    partitionId: 0,
    offset,
    timestampMicros: BigInt(Date.now()) * 1000n,
    headers
  }
}

const toolKey = SigningKey.fromBytes(new Uint8Array(32).fill(41))
const otherKey = SigningKey.fromBytes(new Uint8Array(32).fill(42))

function enrolled(): KeyRegistry {
  const registry = new KeyRegistry()
  registry.enroll("tool", toolKey.verifyingKey())
  registry.enroll("other", otherKey.verifyingKey())
  return registry
}

void test("given_a_verifier_when_an_unsigned_reply_echoes_the_correlation_then_should_drop_it", async () => {
  const correlation = CorrelationId.fromU128(0x0401n)
  const transport = fakeTransport([
    { strategy: "last", result: [] },
    { strategy: "offset", result: [agdxReply(correlation, "forged-unsigned", 1n)] }
  ])
  const hub = await ReplyHub.create(transport, "stream", "replies", enrolled())
  try {
    const ticket = hub.subscribe(correlation.toString())
    await assert.rejects(ticket.wait(100), TimeoutError)
  } finally {
    hub.stop()
  }
})

void test("given_a_verifier_when_a_plain_reply_echoes_the_correlation_then_should_drop_it", async () => {
  const transport = fakeTransport([
    { strategy: "last", result: [] },
    { strategy: "offset", result: [replyMessage("corr-plain", 1n)] }
  ])
  const hub = await ReplyHub.create(transport, "stream", "replies", enrolled())
  try {
    const ticket = hub.subscribe("corr-plain")
    await assert.rejects(ticket.wait(100), TimeoutError)
  } finally {
    hub.stop()
  }
})

void test("given_a_bound_signer_when_a_different_enrolled_key_replies_then_should_drop_it", async () => {
  const correlation = CorrelationId.fromU128(0x0402n)
  const transport = fakeTransport([
    { strategy: "last", result: [] },
    { strategy: "offset", result: [agdxReply(correlation, "forged-wrong-signer", 1n, otherKey)] }
  ])
  const hub = await ReplyHub.create(transport, "stream", "replies", enrolled())
  try {
    const ticket = hub.subscribe(correlation.toString(), "tool")
    await assert.rejects(ticket.wait(100), TimeoutError)
  } finally {
    hub.stop()
  }
})

void test("given_a_verifier_when_the_bound_signer_replies_then_should_resolve_with_the_principal", async () => {
  const correlation = CorrelationId.fromU128(0x0403n)
  const transport = fakeTransport([
    { strategy: "last", result: [] },
    { strategy: "offset", result: [agdxReply(correlation, "honest", 1n, toolKey)] }
  ])
  const hub = await ReplyHub.create(transport, "stream", "replies", enrolled())
  try {
    const ticket = hub.subscribe(correlation.toString(), "tool")
    const reply = await ticket.wait(2_000)
    assert.equal(new TextDecoder().decode(agentMessageBody(reply)), "honest")
    assert.equal(reply.verifiedPrincipal, "tool")
  } finally {
    hub.stop()
  }
})

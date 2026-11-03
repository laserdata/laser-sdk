import assert from "node:assert/strict"
import { test } from "node:test"
import { TimeoutError } from "../../src/client/errors.js"
import type {
  ConsumerTarget,
  LaserTransport,
  PollWait,
  PolledMessage
} from "../../src/iggy/apache-iggy.js"
import type { HeaderValue } from "../../src/stream/header-value.js"
import { agentMessageBody } from "../../src/agent/reliable-consumer.js"
import { ReplyHub, findAgdxReply } from "../../src/agent/replies.js"
import { encodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import { KeyRegistry, SigningKey } from "../../src/signing.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import type { ConsumerStart } from "../../src/stream/consumer-start.js"
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
  readonly strategy: ConsumerStart["kind"]
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
    deleteStream: () => Promise.reject(new Error("unused")),
    ensureTopic: () => Promise.reject(new Error("unused")),
    ensureConsumerGroup: () => Promise.reject(new Error("unused")),
    findTopicPartitionCount: () => Promise.resolve(1),
    getTopicPartitionCount: () => Promise.resolve(1),
    sendMessages: () => Promise.reject(new Error("unused")),
    sendMessageWithHeaders: () => Promise.reject(new Error("unused")),
    sendMessagesWithHeaders: () => Promise.reject(new Error("unused")),
    pollMessages(
      _streamId: string,
      _topicId: string,
      _target: ConsumerTarget,
      strategy: ConsumerStart
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

void test("given_one_topic_for_request_and_reply_when_armed_then_should_skip_the_request_address", async () => {
  const transport = fakeTransport([
    { strategy: "last", result: [] },
    {
      strategy: "offset",
      result: [replyMessage("corr-self", 5n), replyMessage("corr-self", 6n)]
    }
  ])
  const hub = await ReplyHub.create(transport, "stream", "one-topic")
  try {
    const ticket = hub.subscribe("corr-self", undefined, true)
    await new Promise((resolve) => setTimeout(resolve, 30))
    ticket.arm({ partitionId: 0, offset: 5n })
    const reply = await ticket.wait(2_000)
    assert.equal(reply.id.offset, 6n)
  } finally {
    hub.stop()
  }
})

void test("given_different_reply_topic_when_offsets_match_then_should_accept_the_reply", async () => {
  const transport = fakeTransport([
    { strategy: "last", result: [] },
    { strategy: "offset", result: [replyMessage("corr-other-topic", 5n)] }
  ])
  const hub = await ReplyHub.create(transport, "stream", "reply-topic")
  try {
    const ticket = hub.subscribe("corr-other-topic", undefined, true)
    await new Promise((resolve) => setTimeout(resolve, 30))
    ticket.arm()
    const reply = await ticket.wait(2_000)
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
// the streaming server would deliver it: named-CBOR payload, AGDX headers, a record
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
  const headers = new Map<string, HeaderValue>([
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
  const hub = await ReplyHub.create(transport, "stream", "replies", undefined, enrolled())
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
  const hub = await ReplyHub.create(transport, "stream", "replies", undefined, enrolled())
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
  const hub = await ReplyHub.create(transport, "stream", "replies", undefined, enrolled())
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
  const hub = await ReplyHub.create(transport, "stream", "replies", undefined, enrolled())
  try {
    const ticket = hub.subscribe(correlation.toString(), "tool")
    const reply = await ticket.wait(2_000)
    assert.equal(new TextDecoder().decode(agentMessageBody(reply)), "honest")
    assert.equal(reply.verifiedPrincipal, "tool")
  } finally {
    hub.stop()
  }
})

function scannedTopic(messages: readonly PolledMessage[]): LaserTransport {
  const transport: Pick<LaserTransport, "findTopicPartitionCount" | "pollMessages"> = {
    findTopicPartitionCount: () => Promise.resolve(1),
    pollMessages(
      _streamId: string,
      _topicId: string,
      _target: ConsumerTarget,
      strategy: ConsumerStart
    ): Promise<readonly PolledMessage[]> {
      const from = strategy.kind === "offset" ? strategy.value : 0n
      return Promise.resolve(messages.filter((message) => message.offset >= from))
    }
  }
  return transport as LaserTransport
}

void test("given_two_verified_replies_when_looked_up_then_should_return_the_first_in_log_order", async () => {
  const correlation = CorrelationId.fromU128(0x0404n)
  const transport = scannedTopic([
    agdxReply(correlation, "first", 0n, toolKey),
    agdxReply(correlation, "second", 1n, toolKey)
  ])
  const found = await findAgdxReply(transport, "stream", "replies", correlation, enrolled())
  assert.equal(
    new TextDecoder().decode(found.kind === "found" ? found.envelope.body : undefined),
    "first"
  )
})

void test("given_a_verifier_when_only_an_unsigned_reply_exists_then_should_find_nothing", async () => {
  const correlation = CorrelationId.fromU128(0x0405n)
  const transport = scannedTopic([agdxReply(correlation, "forged-unsigned", 0n)])
  assert.deepEqual(await findAgdxReply(transport, "stream", "replies", correlation, enrolled()), {
    kind: "notFound"
  })
  const unverified = await findAgdxReply(transport, "stream", "replies", correlation)
  assert.equal(
    new TextDecoder().decode(unverified.kind === "found" ? unverified.envelope.body : undefined),
    "forged-unsigned"
  )
})

void test("given_a_forged_reply_before_a_verified_one_when_looked_up_then_should_skip_the_forgery", async () => {
  const correlation = CorrelationId.fromU128(0x0406n)
  const transport = scannedTopic([
    agdxReply(correlation, "forged", 0n),
    agdxReply(correlation, "honest", 1n, toolKey)
  ])
  const found = await findAgdxReply(transport, "stream", "replies", correlation, enrolled())
  assert.equal(
    new TextDecoder().decode(found.kind === "found" ? found.envelope.body : undefined),
    "honest"
  )
})

void test("given_an_expected_reply_when_dispatched_then_should_take_only_the_requesters_reply_in_its_session", async () => {
  const session = ConversationId.new()
  const generic = (offset: bigint, conversationId: ConversationId, target?: string) => ({
    payload: new TextEncoder().encode(String(offset)),
    partitionId: 0,
    offset,
    headers: encodeProvenanceHeaders({
      conversationId,
      correlationId: "corr-expected",
      ...(target !== undefined ? { targetAgentId: AgentId.new(target) } : {})
    })
  })
  const transport = fakeTransport([
    { strategy: "last", result: [] },
    {
      strategy: "offset",
      result: [
        generic(1n, ConversationId.new(), "planner"),
        generic(2n, session, "bystander"),
        generic(3n, session),
        generic(4n, session, "planner")
      ]
    }
  ])
  const hub = await ReplyHub.create(transport, "stream", "replies")
  try {
    const ticket = hub.subscribe("corr-expected", undefined, false, {
      session,
      requester: "planner"
    })
    const reply = await ticket.wait(2_000)
    assert.equal(reply.id.offset, 4n)
  } finally {
    hub.stop()
  }
})

void test("given_a_reply_deeper_than_a_thousand_records_per_pass_when_looked_up_then_should_scan_ten_thousand_per_pass", async () => {
  const correlation = CorrelationId.fromU128(0x0407n)
  const depth = 40_000n
  const reply = agdxReply(correlation, "deep", depth)
  const filler = (offset: bigint): PolledMessage => ({
    payload: new Uint8Array(),
    partitionId: 0,
    offset,
    headers: new Map()
  })
  const transport: Pick<LaserTransport, "findTopicPartitionCount" | "pollMessages"> = {
    findTopicPartitionCount: () => Promise.resolve(1),
    pollMessages(_streamId, _topicId, _target, strategy, count) {
      const from = strategy.kind === "offset" ? strategy.value : 0n
      const page: PolledMessage[] = []
      for (let offset = from; offset <= depth && page.length < count; offset += 1n) {
        page.push(offset === depth ? reply : filler(offset))
      }
      return Promise.resolve(page)
    }
  }
  const found = await findAgdxReply(transport as LaserTransport, "stream", "replies", correlation)
  assert.equal(
    new TextDecoder().decode(found.kind === "found" ? found.envelope.body : undefined),
    "deep"
  )
})

void test("given_a_reply_topic_longer_than_the_scan_bound_when_looked_up_then_should_report_incomplete", async () => {
  const correlation = CorrelationId.fromU128(0x0499n)
  const other = CorrelationId.fromU128(0x0500n)
  // Every poll returns another unrelated reply, so the scan never ends.
  const endless: Pick<LaserTransport, "findTopicPartitionCount" | "pollMessages"> = {
    findTopicPartitionCount: () => Promise.resolve(1),
    pollMessages(
      _streamId: string,
      _topicId: string,
      _target: ConsumerTarget,
      strategy: ConsumerStart
    ): Promise<readonly PolledMessage[]> {
      const from = strategy.kind === "offset" ? strategy.value : 0n
      return Promise.resolve([agdxReply(other, "unrelated", from)])
    }
  }
  assert.deepEqual(
    await findAgdxReply(endless as LaserTransport, "stream", "replies", correlation),
    { kind: "incomplete" }
  )
})

void test("given_no_waiter_when_the_hub_is_idle_then_should_not_poll_until_a_subscribe", async () => {
  const polls = { offset: 0 }
  const transport = fakeTransport([{ strategy: "last", result: [] }])
  const scripted = transport.pollMessages.bind(transport)
  // Assigned onto the fake, so its throwing getters are never read.
  Object.assign(transport, {
    pollMessages(
      streamId: string,
      topicId: string,
      target: ConsumerTarget,
      strategy: ConsumerStart
    ): Promise<readonly PolledMessage[]> {
      if (strategy.kind === "offset") polls.offset += 1
      return scripted(streamId, topicId, target, strategy, 1, false)
    }
  })
  const hub = await ReplyHub.create(transport, "stream", "idle")
  try {
    await new Promise((resolve) => setTimeout(resolve, 200))
    assert.equal(polls.offset, 0, "an idle hub parks")
    hub.subscribe("corr-idle")
    await new Promise((resolve) => setTimeout(resolve, 100))
    assert.notEqual(polls.offset, 0, "a subscribe wakes the dispatcher")
  } finally {
    hub.stop()
  }
})

void test("given_an_idle_pass_over_two_reply_partitions_when_a_reply_lands_on_one_then_should_deliver_it_while_the_other_read_is_held", async () => {
  const waits: number[] = []
  let cancelled = 0
  let held = 0
  const transport = fakeTransport([])
  Object.assign(transport, {
    findTopicPartitionCount: () => Promise.resolve(2),
    pollMessages(
      _streamId: string,
      _topicId: string,
      target: ConsumerTarget,
      strategy: ConsumerStart,
      _count: number,
      _commit: boolean,
      wait?: PollWait,
      signal?: AbortSignal
    ): Promise<readonly PolledMessage[]> {
      if (strategy.kind !== "offset") return Promise.resolve([])
      waits.push(wait?.maxWaitMs ?? 0)
      if ((wait?.maxWaitMs ?? 0) === 0)
        return new Promise((resolve) =>
          setTimeout(() => {
            resolve([])
          }, 1)
        )
      held += 1
      if (target.partitionId === 1 && held <= 2) {
        return new Promise((resolve) =>
          setTimeout(() => {
            resolve([{ ...replyMessage("corr-held", 0n), partitionId: 1 }])
          }, 10)
        )
      }
      return new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => {
          cancelled += 1
          reject(new Error("cancelled"))
        })
      })
    }
  })
  const hub = await ReplyHub.create(transport, "stream", "held")
  try {
    const started = performance.now()
    const reply = await hub.subscribe("corr-held").wait(2_000)
    assert.equal(reply.provenance.correlationId, "corr-held")
    assert.ok(
      performance.now() - started < 1_000,
      "the reply arrives without waiting out the other read"
    )
  } finally {
    hub.stop()
  }
  assert.deepEqual(
    waits.slice(0, 4),
    [0, 0, 5_000, 5_000],
    "an idle pass holds one read per partition"
  )
  assert.ok(cancelled >= 1, "the read still held is cancelled")
})

void test("given_a_hub_holding_reads_for_a_waiter_when_stopped_then_should_cancel_every_read_end_the_wait_and_keep_no_timer", async () => {
  let holding = 0
  let cancelled = 0
  let allHeld: () => void = () => undefined
  const held = new Promise<void>((resolve) => {
    allHeld = resolve
  })
  const transport = fakeTransport([])
  Object.assign(transport, {
    findTopicPartitionCount: () => Promise.resolve(2),
    pollMessages(
      _streamId: string,
      _topicId: string,
      _target: ConsumerTarget,
      strategy: ConsumerStart,
      _count: number,
      _commit: boolean,
      wait?: PollWait,
      signal?: AbortSignal
    ): Promise<readonly PolledMessage[]> {
      if (strategy.kind !== "offset" || (wait?.maxWaitMs ?? 0) === 0) return Promise.resolve([])
      holding += 1
      if (holding === 2) allHeld()
      return new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => {
          cancelled += 1
          reject(new Error("cancelled"))
        })
      })
    }
  })
  const timers = (): number =>
    process.getActiveResourcesInfo().filter((resource) => resource === "Timeout").length
  const before = timers()
  const hub = await ReplyHub.create(transport, "stream", "closing")
  const reply = hub.subscribe("corr-closing").wait(60_000)
  await held
  const started = performance.now()
  hub.stop()
  await assert.rejects(reply, TimeoutError)
  assert.ok(performance.now() - started < 1_000, "the wait ends with the hub, not at its timeout")
  await new Promise((resolve) => setImmediate(resolve))
  assert.equal(cancelled, 2, "every held read is cancelled")
  assert.equal(timers(), before, "a stopped hub keeps no timer")
})

void test("given_held_reads_for_reply_waiters_when_the_last_waiter_leaves_then_should_cancel_them_and_hold_nothing_while_idle", async () => {
  let holding = 0
  let cancelled = 0
  const transport = fakeTransport([])
  Object.assign(transport, {
    findTopicPartitionCount: () => Promise.resolve(2),
    pollMessages(
      _streamId: string,
      _topicId: string,
      _target: ConsumerTarget,
      strategy: ConsumerStart,
      _count: number,
      _commit: boolean,
      wait?: PollWait,
      signal?: AbortSignal
    ): Promise<readonly PolledMessage[]> {
      if (strategy.kind !== "offset" || (wait?.maxWaitMs ?? 0) === 0) return Promise.resolve([])
      holding += 1
      return new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => {
          cancelled += 1
          reject(new Error("cancelled"))
        })
      })
    }
  })
  const open = (): number => holding - cancelled
  const hub = await ReplyHub.create(transport, "stream", "released")
  try {
    const first = hub.subscribe("corr-first")
    const second = hub.subscribe("corr-second")
    await until(() => open() === 2, 1_000)
    await assert.rejects(first.wait(20), TimeoutError)
    await new Promise((resolve) => setTimeout(resolve, 50))
    assert.equal(open(), 2, "a remaining waiter keeps the reads held")
    second.cancel()
    await until(() => open() === 0, 1_000)
    await new Promise((resolve) => setTimeout(resolve, 200))
    assert.equal(holding, 2, "an idle hub holds nothing")
    hub.subscribe("corr-third")
    await until(() => open() === 2, 1_000)
  } finally {
    hub.stop()
  }
})

async function until(condition: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = performance.now() + timeoutMs
  while (!condition()) {
    if (performance.now() >= deadline) throw new Error(`not true within ${String(timeoutMs)} ms`)
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

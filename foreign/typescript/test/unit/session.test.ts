import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { test } from "node:test"
import { createAgdx } from "../../src/agent/agdx.js"
import { type LeaseRegistry, SessionLease } from "../../src/agent/lease.js"
import { HandlerError, InvalidError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Checkpoint } from "../../src/context.js"
import type { LaserTransport, MessageWithHeaders } from "../../src/iggy/apache-iggy.js"
import { MemoryId, MemoryKind } from "../../src/memory/types.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import {
  DEFAULT_SESSION_MEMORY_NAMESPACE,
  type Session,
  SessionConfig,
  Sessions,
  TopicRetention,
  deriveSessionId
} from "../../src/session.js"
import { ModelRequest, SessionState, defaultRedact } from "../../src/session-ops.js"
import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { Cursor } from "../../src/stream/cursor.js"
import { Topic } from "../../src/stream/topic.js"
import { SDK_VERSION } from "../../src/version.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import {
  type AgentEnvelope,
  METADATA_DURATION_MICROS,
  METADATA_PROVIDER_NAME,
  METADATA_REQUEST_MODEL,
  METADATA_RESPONSE_MODEL,
  METADATA_SUBMITTED,
  OPERATION_SESSION,
  OPERATION_STATE_DELTA,
  OPERATION_STATE_SNAPSHOT,
  commandEnvelope,
  decodeAgentEnvelope,
  decodeContextCompaction,
  decodeContextRetrieval,
  decodeSessionEnd,
  decodeStateDelta,
  encodeStateDelta,
  encodeStateSnapshot
} from "../../src/wire/agent.js"
import { OPERATION_EXECUTE_TOOL } from "../../src/wire/dispatch.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId
} from "../../src/wire/ids.js"

function nth<Item>(items: readonly Item[], index: number): Item {
  const item = items.at(index)
  assert.ok(item !== undefined, `expected an item at ${String(index)}`)
  return item
}

void test("given_the_defaults_when_read_then_should_match_the_documented_values", () => {
  const config = new SessionConfig()
  assert.equal(config.idleTimeoutValue, 300_000)
  assert.equal(config.heartbeatValue, 60_000)
  assert.equal(config.registersSource, true)
  assert.equal(config.failsOnDeadLetter, false)
  assert.deepEqual(config.layoutKind, { kind: "shared" })
  assert.equal(config.sdkInfo.language, "typescript")
  assert.equal(config.memoryNamespaceName, DEFAULT_SESSION_MEMORY_NAMESPACE)
  const manifest = JSON.parse(
    readFileSync(new URL("../../../package.json", import.meta.url), "utf8")
  ) as { readonly version: string }
  assert.equal(SDK_VERSION, manifest.version)
  assert.equal(config.sdkInfo.version, manifest.version)
})

void test("given_chained_setters_when_configuring_then_should_return_new_configs", () => {
  const base = new SessionConfig()
  const config = base
    .stream("support")
    .layout({ kind: "singlePartition" })
    .idleTimeout(1_000)
    .heartbeat(250)
    .registerSource(false)
    .failOnDeadLetter(true)
    .memoryNamespace("support.sessions")
    .contextTurns(10)
    .contextTokens(800)
    .sdk("custom", "1.2.3")
  assert.equal(config.streamName, "support")
  assert.deepEqual(config.layoutKind, { kind: "singlePartition" })
  assert.equal(config.idleTimeoutValue, 1_000)
  assert.equal(config.heartbeatValue, 250)
  assert.equal(config.registersSource, false)
  assert.equal(config.failsOnDeadLetter, true)
  assert.equal(config.memoryNamespaceName, "support.sessions")
  assert.equal(config.contextTurnBound, 10)
  assert.equal(config.contextTokenBound, 800)
  assert.deepEqual(config.sdkInfo, { language: "custom", version: "1.2.3" })
  assert.equal(base.streamName, undefined)
  assert.equal(base.idleTimeoutValue, 300_000)
  assert.throws(() => base.heartbeat(-1), InvalidError)
})

void test("given_labels_namespaces_and_streams_when_derived_then_should_separate_each_dimension", () => {
  const base = deriveSessionId("agents", "ops", "incident")
  assert.ok(base.equals(deriveSessionId("agents", "ops", "incident")))
  assert.ok(!base.equals(deriveSessionId("other", "ops", "incident")))
  assert.ok(!base.equals(deriveSessionId("agents", "desk", "incident")))
  assert.ok(!base.equals(deriveSessionId("agents", "ops", "refund")))
  assert.ok(!deriveSessionId("a", "bc", "d").equals(deriveSessionId("ab", "c", "d")))
  assert.ok(base.equals(ConversationId.derive("agents\u001fops\u001fincident")))
})

void test("given_an_unbounded_retention_when_built_then_should_be_refused", () => {
  assert.throws(() => TopicRetention.new(), InvalidError)
  assert.throws(() => TopicRetention.new(undefined, undefined), InvalidError)
  assert.throws(() => TopicRetention.new(undefined, 0n), InvalidError)
  assert.throws(() => TopicRetention.new(-1), InvalidError)
  const day = TopicRetention.new(86_400_000)
  assert.equal(day.expiry(), 86_400_000)
  assert.equal(day.maxSize(), undefined)
  const bounded = TopicRetention.new(undefined, 1_000n)
  assert.equal(bounded.expiry(), undefined)
  assert.equal(bounded.maxSize(), 1_000n)
  assert.equal(TopicRetention.expireAfter(86_400_000).expiry(), 86_400_000)
  assert.equal(TopicRetention.expireAfter(86_400_000).maxSize(), undefined)
  assert.equal(TopicRetention.expireAfter(0).expiry(), 0.001)
})

void test("given_a_checkpoint_when_round_tripped_through_json_then_should_keep_every_offset", () => {
  const checkpoint = Checkpoint.fromJSON({
    per_topic: { "agent.sessions": { "0": 5, "1": 2 }, "agent.streams": {} }
  })
  assert.deepEqual(
    checkpoint.topicOffsets("agent.sessions"),
    new Map([
      [0, 5n],
      [1, 2n]
    ])
  )
  assert.deepEqual(checkpoint.topicOffsets("agent.streams"), new Map())
  assert.equal(checkpoint.topicOffsets("agent.memory"), undefined)
  assert.equal(checkpoint.isEmpty(), false)
  assert.equal(Checkpoint.fromJSON({ per_topic: {} }).isEmpty(), true)
  const restored = Checkpoint.fromJSON(JSON.stringify(checkpoint))
  assert.deepEqual(restored.toJSON(), checkpoint.toJSON())
  assert.throws(() => Checkpoint.fromJSON(["nope"]), InvalidError)
  assert.throws(() => Checkpoint.fromJSON({ per_topic: { topic: { x: 1 } } }), InvalidError)
})

void test("given_the_rust_checkpoint_json_when_read_then_should_round_trip_byte_for_byte", () => {
  const rust =
    '{"per_topic":{"agent.sessions":{"0":5,"7":18446744073709551615},"agent.streams":{}}}'
  const checkpoint = Checkpoint.fromJSON(rust)
  assert.equal(checkpoint.topicOffsets("agent.sessions")?.get(7), 18446744073709551615n)
  assert.equal(JSON.stringify(checkpoint), rust)
})

void test("given_an_offset_that_is_not_a_u64_when_read_then_should_reject_it", () => {
  for (const offset of ["-1", "1.5", "18446744073709551616", '"5"']) {
    assert.throws(
      () => Checkpoint.fromJSON(`{"per_topic":{"agent.sessions":{"0":${offset}}}}`),
      InvalidError,
      offset
    )
  }
  assert.throws(() => Checkpoint.fromJSON('{"per_topic":{"t":{"4294967296":1}}}'), InvalidError)
  assert.throws(() => Checkpoint.fromJSON("{"), InvalidError)
  assert.throws(() => Checkpoint.fromJSON({ "agent.sessions": { "0": 5 } }), InvalidError)
})

interface Sent {
  readonly topic: string
  readonly envelope: AgentEnvelope
  readonly to: string | undefined
}

function fakeLaser(tails: ReadonlyMap<string, ReadonlyMap<number, bigint>> = new Map()): {
  readonly laser: Laser
  readonly sent: Sent[]
  readonly streams: string[]
} {
  const sent: Sent[] = []
  const streams: string[] = []
  const transport = (topic: string) =>
    ({
      sendMessagesWithHeaders(
        _stream: string,
        _topic: string,
        messages: readonly MessageWithHeaders[]
      ) {
        for (const message of messages) {
          const to = message.headers.get("agdx.to")
          sent.push({
            topic,
            envelope: decodeAgentEnvelope(
              expectMap(decodeOne(message.payload, "sent"), "sent"),
              "sent"
            ),
            to: to?.kind === "string" ? to.value : undefined
          })
        }
        return Promise.resolve({ confirmations: [] })
      }
    }) as unknown as LaserTransport
  const laser = {
    defaultStream: "agents",
    [INTERNAL_TRANSPORT]: () => ({
      findSnapshotStream: () => Promise.resolve({ id: 0, createdAtMicros: 1n }),
      findSnapshotTopic: () => Promise.resolve({ id: 0, createdAtMicros: 2n, partitions: 4 })
    }),
    capabilities: () => Promise.resolve(OPEN_CAPABILITIES),
    agdx(topic: string, source: AgentId, conversation: ConversationId) {
      return createAgdx(transport(topic), "agents", topic, source, conversation)
    },
    topic(name: string) {
      return {
        partitionCount: () => Promise.resolve(tails.has(name) ? tails.get(name)?.size : undefined),
        tailOffsets: () => Promise.resolve(tails.get(name) ?? new Map<number, bigint>())
      }
    },
    memory(namespace: string) {
      return { namespace, recall: () => fakeRecall(namespace) }
    },
    graph(name: string) {
      return { name }
    },
    withDefaultStream(stream: string) {
      streams.push(stream)
      return laser
    }
  }
  return { laser: laser as unknown as Laser, sent, streams }
}

function fakeRecall(namespace: string) {
  const builder = {
    conversation: () => builder,
    keyword: (text: string) => {
      builder.text = text
      return builder
    },
    limit: (limit: number) => {
      builder.max = limit
      return builder
    },
    fetch: () => Promise.resolve([{ namespace, text: builder.text, max: builder.max }]),
    text: "",
    max: 0
  }
  return builder
}

const planner = AgentId.new("planner")

function heldLease(session: ConversationId): { lease: SessionLease; released: () => boolean } {
  let released = false
  const registry = {
    release: () => {
      released = true
    }
  } as unknown as LeaseRegistry
  return {
    lease: SessionLease.create(registry, { stream: "agents", streamGeneration: 0n, session }),
    released: () => released
  }
}

void test("given_a_fake_laser_when_sessions_are_opened_then_should_derive_ids_and_append_to_the_lane", async () => {
  const { laser, sent, streams } = fakeLaser()
  const sessions = Sessions.create(laser, new SessionConfig().stream("support"))
  assert.deepEqual(streams, ["support"])
  assert.equal(sessions.stream(), "agents")
  const builder = sessions.create("agent-42").namespace("desk")
  assert.ok(builder.id().equals(deriveSessionId("agents", "desk", "agent-42")))
  assert.ok(
    sessions
      .create("agent-42")
      .id()
      .equals(deriveSessionId("agents", "", "agent-42"))
  )
  assert.ok(!sessions.start().id().equals(sessions.start().id()))
  const explicit = ConversationId.new()
  assert.ok(sessions.start().withId(explicit).id().equals(explicit))
  const session = sessions.open(builder.id())
  assert.equal(session.agent, undefined)
  const own = commandEnvelope(
    RecordId.fromU128(3n),
    WireConversationId.parse(session.conversation.toString()),
    planner.wireId(),
    CorrelationId.fromU128(4n),
    new Uint8Array([1])
  )
  const receipt = await session.append(own)
  assert.equal(receipt.record?.toString(), RecordId.fromU128(3n).toString())
  assert.deepEqual(
    sent.map((entry) => [entry.topic, entry.to]),
    [[AgentTopic.Sessions, "*"]]
  )
  const foreign = { ...own, conversation: WireConversationId.fromU128(9n) }
  await assert.rejects(session.append(foreign), InvalidError)
  assert.deepEqual((session.graph("kg") as unknown as { name: string }).name, "kg")
  assert.equal(session.config.memoryNamespaceName, DEFAULT_SESSION_MEMORY_NAMESPACE)
  await assert.rejects(sessions.start().begin(), InvalidError)
  await assert.rejects(session.end(), InvalidError)
})

void test("given_a_session_handle_when_ended_twice_then_should_repeat_the_record_and_refuse_another_verb", async () => {
  const { laser, sent } = fakeLaser()
  const session = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
  await session.end()
  await session.end()
  await assert.rejects(session.cancel(), InvalidError)
  assert.equal(sent.length, 2)
  const first = nth(sent, 0).envelope
  const second = nth(sent, 1).envelope
  assert.equal(first.record?.toString(), second.record?.toString())
  assert.equal(first.operation, OPERATION_SESSION)
  assert.equal(first.last, true)
  assert.deepEqual(first.taskState, { kind: "known", name: "Completed" })
  assert.deepEqual(decodeSessionEnd(expectMap(decodeOne(first.body, "end"), "end"), "end"), {})
  const copy = session.asAgent(AgentId.new("writer"))
  await assert.rejects(
    copy.fail({ code: { kind: "known", name: "Internal" }, retryable: false }),
    InvalidError
  )
})

void test("given_a_session_run_when_the_work_throws_then_should_fail_the_session_and_rethrow_the_original", async () => {
  const { laser, sent } = fakeLaser()
  const session = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
  const thrown = new TypeError("model provider exploded")
  const { lease, released } = heldLease(session.conversation)
  await assert.rejects(
    session.run(lease, () => {
      throw thrown
    }),
    (error) => error === thrown
  )
  assert.equal(released(), true)
  const envelope = nth(sent, 0).envelope
  assert.deepEqual(envelope.taskState, { kind: "known", name: "Failed" })
  const end = decodeSessionEnd(expectMap(decodeOne(envelope.body, "end"), "end"), "end")
  assert.equal(end.error?.message, "model provider exploded")
  assert.deepEqual(end.error.detail?.get("panic"), { kind: "bool", value: true })

  const failed = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
  const rejection = new HandlerError("tool timed out")
  await assert.rejects(
    failed.run(heldLease(failed.conversation).lease, () => Promise.reject(rejection)),
    (error) => error === rejection
  )
  const failure = decodeSessionEnd(
    expectMap(decodeOne(nth(sent, 1).envelope.body, "end"), "end"),
    "end"
  )
  assert.equal(failure.error?.message, "tool timed out")
  assert.equal(failure.error.detail, undefined)

  const done = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
  assert.equal(await done.run(heldLease(done.conversation).lease, () => 42), 42)
  assert.deepEqual(nth(sent, 2).envelope.taskState, { kind: "known", name: "Completed" })
})

void test("given_a_scoped_memory_when_searched_then_should_run_a_keyword_recall_in_the_namespace", async () => {
  const { laser } = fakeLaser()
  const session = Sessions.create(
    laser,
    new SessionConfig().memoryNamespace("support.sessions")
  ).open(ConversationId.new())
  const hits = (await session.memory().search("login bug", { limit: 3 })) as unknown as readonly {
    namespace: string
    text: string
    max: number
  }[]
  assert.deepEqual(hits, [{ namespace: "support.sessions", text: "login bug", max: 3 }])
  const defaults = (await session.memory("other").search("x")) as unknown as readonly {
    namespace: string
    max: number
  }[]
  assert.deepEqual(defaults, [{ namespace: "other", text: "x", max: 50 }])
})

void test("given_the_lane_tail_when_a_checkpoint_is_captured_then_should_record_each_partition", async () => {
  const { laser } = fakeLaser(
    new Map([
      [
        AgentTopic.Sessions,
        new Map([
          [0, 3n],
          [1, 0n]
        ])
      ]
    ])
  )
  const checkpoint = await Sessions.create(laser).open(ConversationId.new()).checkpoint()
  assert.deepEqual(Object.keys(checkpoint.toJSON().per_topic), [AgentTopic.Sessions])
  assert.deepEqual(
    checkpoint.topicOffsets(AgentTopic.Sessions),
    new Map([
      [0, 3n],
      [1, 0n]
    ])
  )
  assert.equal(
    Checkpoint.fromJSON(JSON.stringify(checkpoint)).topicOffsets(AgentTopic.Sessions)?.get(0),
    3n
  )
})

void test("given_a_cursor_with_an_upper_bound_when_polled_then_should_stop_each_partition_before_it", async () => {
  const polled: bigint[] = []
  const transport = {
    pollMessages(
      _stream: string,
      _topic: string,
      target: { partitionId: number },
      strategy: { value: bigint },
      count: number
    ) {
      polled.push(strategy.value)
      const messages = []
      for (let offset = strategy.value; offset < strategy.value + BigInt(count); offset += 1n) {
        messages.push({
          payload: new Uint8Array(),
          partitionId: target.partitionId,
          offset,
          headers: new Map()
        })
      }
      return Promise.resolve(messages)
    }
  }
  const cursor = Cursor.create(transport as unknown as LaserTransport, "s", "t", [0, 1]).batch(4)
  cursor.fromOffsets(new Map([[0, 2n]])).until(
    new Map([
      [0, 5n],
      [1, 0n]
    ])
  )
  const first = await cursor.poll()
  assert.deepEqual(
    first.map((message) => [message.id.partitionId, message.id.offset]),
    [
      [0, 2n],
      [0, 3n],
      [0, 4n]
    ]
  )
  assert.deepEqual(await cursor.poll(), [])
  assert.deepEqual(polled, [2n])
  assert.deepEqual(
    cursor.offsets,
    new Map([
      [0, 5n],
      [1, 0n]
    ])
  )
})

void test("given_a_topic_when_tails_are_read_then_should_report_the_next_offset_per_partition", async () => {
  const transport = {
    findTopicPartitionCount: (_stream: string, topic: string) =>
      Promise.resolve(topic === "present" ? 2 : undefined),
    pollMessages: (_stream: string, _topic: string, target: { partitionId: number }) =>
      Promise.resolve(
        target.partitionId === 0
          ? [{ payload: new Uint8Array(), partitionId: 0, offset: 6n, headers: new Map() }]
          : []
      )
  } as unknown as LaserTransport
  const present = Topic.create(transport, "s", "present")
  assert.equal(await present.partitionCount(), 2)
  assert.deepEqual(
    await present.tailOffsets(),
    new Map([
      [0, 7n],
      [1, 0n]
    ])
  )
  const missing = Topic.create(transport, "s", "missing")
  assert.equal(await missing.partitionCount(), undefined)
  assert.deepEqual(await missing.tailOffsets(), new Map())
})

void test("given_secret_keys_at_any_depth_when_redacted_then_should_drop_only_their_values", () => {
  const args = defaultRedact({
    query: "status",
    Authorization: "Bearer x",
    nested: { api_key: "k", keep: 1 },
    list: [{ password: "p" }]
  })
  assert.deepEqual(args, {
    query: "status",
    Authorization: "[redacted]",
    nested: { api_key: "[redacted]", keep: 1 },
    list: [{ password: "[redacted]" }]
  })
})

void test("given_state_deltas_when_written_then_should_chain_revisions_and_snapshot_every_64_and_on_end", async () => {
  const { laser, sent } = fakeLaser()
  const session = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
  const state = session.state()
  await assert.rejects(state.patch([{ op: "remove", path: "/missing" }]), InvalidError)
  assert.equal(sent.length, 0)
  for (let step = 0; step < 64; step += 1) await state.set("step", step)
  const operations = sent.map((entry) => entry.envelope.operation)
  assert.equal(operations.filter((operation) => operation === OPERATION_STATE_DELTA).length, 64)
  assert.equal(operations.at(-1), OPERATION_STATE_SNAPSHOT)
  const first = decodeStateDelta(
    expectMap(decodeOne(nth(sent, 0).envelope.body, "delta"), "delta"),
    "delta"
  )
  assert.equal(first.baseRevision, 0n)
  assert.deepEqual(first.patch, [{ op: "add", path: "/step", value: 0 }])
  await session.end()
  assert.equal(nth(sent, sent.length - 1).envelope.operation, OPERATION_SESSION)
  assert.equal(nth(sent, sent.length - 2).envelope.operation, OPERATION_STATE_SNAPSHOT)
  await state.set("a/b~c", true)
  const escaped = decodeStateDelta(
    expectMap(decodeOne(nth(sent, sent.length - 1).envelope.body, "delta"), "delta"),
    "delta"
  )
  assert.equal(escaped.baseRevision, 64n)
  assert.deepEqual(escaped.patch, [{ op: "add", path: "/a~1b~0c", value: true }])
})

void test("given_model_and_tool_calls_when_recorded_then_should_redact_correlate_and_measure", async () => {
  const { laser, sent } = fakeLaser()
  const session = Sessions.create(laser)
    .open(ConversationId.new())
    .asAgent(planner)
    .redact((value) => ({ ...(value as object), hidden: true }))
  const request = new ModelRequest(
    "gpt-test",
    new TextEncoder().encode('{"prompt":"hi"}'),
    "openai",
    "text_completion"
  )
  await session.recordModelCall(request, {
    body: new TextEncoder().encode("hello"),
    model: "gpt-test-1",
    finishReason: "stop",
    durationMs: 2
  })
  const [command, response] = [nth(sent, 0).envelope, nth(sent, 1).envelope]
  assert.equal(command.operation, "text_completion")
  assert.equal(command.target, "planner")
  assert.deepEqual(JSON.parse(new TextDecoder().decode(command.body)), {
    prompt: "hi",
    hidden: true
  })
  assert.deepEqual(command.metadata?.get(METADATA_REQUEST_MODEL), {
    kind: "str",
    value: "gpt-test"
  })
  assert.deepEqual(command.metadata.get(METADATA_PROVIDER_NAME), {
    kind: "str",
    value: "openai"
  })
  assert.equal(response.correlation?.toString(), command.correlation?.toString())
  assert.equal(response.finishReason, "stop")
  assert.deepEqual(response.metadata?.get(METADATA_DURATION_MICROS), {
    kind: "int",
    value: 2000n
  })
  assert.deepEqual(response.metadata.get(METADATA_RESPONSE_MODEL), {
    kind: "str",
    value: "gpt-test-1"
  })
  const tool = await session.tool("lookup", { id: 7 })
  await tool.fail({ code: { kind: "known", name: "ToolFailure" }, retryable: false })
  const [call, failure] = [nth(sent, 2).envelope, nth(sent, 3).envelope]
  assert.equal(call.tool, "lookup")
  assert.equal(call.operation, OPERATION_EXECUTE_TOOL)
  assert.equal(failure.tool, "lookup")
  assert.equal(failure.correlation?.toString(), tool.correlation().toString())
  assert.deepEqual(session.reference(), {
    stream: "agents",
    session: WireConversationId.parse(session.conversation.toString())
  })
})

void test("given_a_submission_when_sent_then_should_write_submitted_then_a_marked_command", async () => {
  const { laser, sent } = fakeLaser()
  const sessions = Sessions.create(laser)
  await assert.rejects(sessions.submit(planner, new Uint8Array()).send(), InvalidError)
  const submitted = await sessions
    .submit(planner, new TextEncoder().encode("{}"))
    .from(AgentId.new("client"))
    .label("ticket-7")
    .namespace("desk")
    .tag("vip")
    .send()
  assert.ok(submitted.session.equals(deriveSessionId("agents", "desk", "ticket-7")))
  const [status, command] = [nth(sent, 0).envelope, nth(sent, 1).envelope]
  assert.deepEqual(status.taskState, { kind: "known", name: "Submitted" })
  assert.equal(status.source, "client")
  assert.equal(command.operation, "invoke_agent")
  assert.equal(command.target, "planner")
  assert.equal(command.correlation?.toString(), submitted.correlation.toString())
  assert.deepEqual(command.metadata?.get(METADATA_SUBMITTED), { kind: "bool", value: true })
})

void test("given_operator_control_when_sent_then_should_write_control_requests_on_the_control_topic", async () => {
  const { laser, sent, streams } = fakeLaser()
  const session = ConversationId.new()
  const control = Sessions.create(laser).control("ops", session)
  assert.deepEqual(streams, ["ops"])
  await assert.rejects(control.participants([]).pause(), InvalidError)
  const operator = control.asOperator(AgentId.new("operator"))
  await operator.participants([AgentId.new("worker")]).pause()
  await operator.resume()
  await operator.forceCancel()
  assert.deepEqual(
    sent.map((entry) => [entry.topic, entry.envelope.operation]),
    [
      [AgentTopic.Control, "session_pause"],
      [AgentTopic.Control, "session_resume"],
      [AgentTopic.Control, OPERATION_SESSION]
    ]
  )
  assert.equal(new TextDecoder().decode(nth(sent, 0).envelope.body), '{"participants":["worker"]}')
  assert.equal(new TextDecoder().decode(nth(sent, 1).envelope.body), "{}")
  assert.deepEqual(nth(sent, 2).envelope.taskState, { kind: "known", name: "Canceled" })
  assert.equal(nth(sent, 2).envelope.last, true)
})

void test("given_a_retrieval_and_a_compaction_when_recorded_then_should_write_their_context_events", async () => {
  const { laser, sent } = fakeLaser()
  const session = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
  const item = {
    id: MemoryId.new(),
    payload: new Uint8Array(),
    provenance: { conversationId: session.conversation },
    kind: MemoryKind.Fact,
    signals: []
  }
  await session.recordRetrieval("deploy", [item, { ...item, score: 0.5 }])
  const compaction = {
    summaryAt: { kind: "memory" as const, id: "summary-1" },
    covered: [[1, 0, 0n, 3n] as const],
    summarizer: { name: "summarizer", version: "1" }
  }
  await session.recordCompaction(compaction)
  const [retrieved, compacted] = [nth(sent, 0).envelope, nth(sent, 1).envelope]
  assert.equal(retrieved.operation, "context_retrieved")
  assert.deepEqual(decodeContextRetrieval(expectMap(decodeOne(retrieved.body, "r"), "r"), "r"), {
    query: "deploy",
    items: [
      [item.id.toString(), 0],
      [item.id.toString(), 0.5]
    ]
  })
  assert.equal(compacted.operation, "context_compacted")
  assert.deepEqual(
    decodeContextCompaction(expectMap(decodeOne(compacted.body, "c"), "c"), "c"),
    compaction
  )
})

void test("given_a_stale_snapshot_after_the_baseline_when_folded_then_should_change_nothing", async () => {
  const snapshot = (baseRevision: bigint, document: unknown) => ({
    envelope: {
      operation: OPERATION_STATE_SNAPSHOT,
      body: encodeNamed(encodeStateSnapshot({ baseRevision, document }))
    }
  })
  const delta = (baseRevision: bigint, value: number) => ({
    envelope: {
      operation: OPERATION_STATE_DELTA,
      body: encodeNamed(
        encodeStateDelta({
          baseRevision,
          patch: [{ op: "add", path: "/count", value }],
          opId: `op-${String(value)}`
        })
      )
    }
  })
  const records = [
    snapshot(3n, { count: 3 }),
    delta(3n, 4),
    // A second writer's snapshot at a revision the fold already passed.
    snapshot(2n, { count: 2 }),
    delta(4n, 5)
  ]
  const session = {
    scope: { fetchWith: () => Promise.resolve(records) }
  } as unknown as Session
  const view = await SessionState.create(session, {
    revision: 0n,
    document: undefined,
    sinceSnapshot: 0
  }).get()
  assert.equal(view.revision, 5n)
  assert.equal(view.complete, true)
  assert.deepEqual(JSON.parse(JSON.stringify(view.document)), { count: 5 })
})

void test("given_a_lane_fold_and_a_managed_view_when_seeding_then_should_trust_the_fold_unless_the_view_is_not_behind", async () => {
  const delta = (baseRevision: bigint) => ({
    envelope: {
      operation: OPERATION_STATE_DELTA,
      body: encodeNamed(
        encodeStateDelta({
          baseRevision,
          patch: [{ op: "add", path: "/n", value: Number(baseRevision) }],
          opId: `op-${String(baseRevision)}`
        })
      )
    }
  })
  // A lane that no longer holds its first records folds incomplete at
  // revision 0, after the deltas based on revisions it never reached.
  const truncated = [delta(6n), delta(7n)]
  const seededAt = async (
    records: readonly unknown[],
    managed: () => Promise<{ readonly revision: bigint; readonly document: unknown } | undefined>
  ): Promise<bigint> => {
    const written: Uint8Array[] = []
    const session = {
      scope: { fetchWith: () => Promise.resolve(records) },
      managedStateView: managed,
      mintOpId: () => "op",
      writeEvent: (_operation: string, body: Uint8Array) => {
        written.push(body)
        return Promise.resolve({})
      }
    } as unknown as Session
    await SessionState.create(session, {
      revision: 0n,
      document: undefined,
      sinceSnapshot: 0
    }).set("k", 1)
    const [body] = written
    assert.ok(body !== undefined)
    return decodeStateDelta(expectMap(decodeOne(body, "delta"), "delta"), "delta").baseRevision
  }
  const ahead = () => Promise.resolve({ revision: 8n, document: { n: 7 } })
  assert.equal(await seededAt([delta(0n)], ahead), 1n, "a complete fold is used as it is")
  assert.equal(await seededAt(truncated, ahead), 8n, "a view not behind the fold is used")
  assert.equal(
    await seededAt(truncated, () => Promise.resolve(undefined)),
    0n,
    "without the sessions capability the fold is used"
  )
  assert.equal(
    await seededAt(truncated, () => Promise.reject(new Error("unavailable"))),
    0n,
    "a failed view read keeps the fold"
  )
})

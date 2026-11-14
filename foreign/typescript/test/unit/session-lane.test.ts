import assert from "node:assert/strict"
import { test } from "node:test"
import { createAgdx } from "../../src/agent/agdx.js"
import { controlPartition } from "../../src/agent/control.js"
import { InvalidError, SessionEndedError } from "../../src/client/errors.js"
import {
  INTERNAL_LAYOUTS,
  INTERNAL_SESSION_LEASES,
  INTERNAL_TRANSPORT
} from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import {
  Checkpoint,
  type ContextMessage,
  LastN,
  TokenBudget,
  contextMessageBody,
  replayContext
} from "../../src/context.js"
import type { LaserTransport, MessageWithHeaders } from "../../src/iggy/apache-iggy.js"
import { LaneLifecycle } from "../../src/agent/budget.js"
import {
  Session,
  SessionConfig,
  type SessionLayout,
  Sessions,
  deriveSessionId
} from "../../src/session.js"
import { ModelRequest, SessionState } from "../../src/session-ops.js"
import { Cursor } from "../../src/stream/cursor.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import {
  type AgentEnvelope,
  AgentKind,
  METADATA_RECORDED,
  OPERATION_CHAT,
  OPERATION_SESSION,
  OPERATION_STATE_DELTA,
  OPERATION_STATE_SNAPSHOT,
  commandEnvelope,
  decodeAgentEnvelope,
  decodeStateDelta,
  encodeSessionEnd,
  encodeSessionStart,
  encodeStateDelta,
  eventEnvelope,
  responseEnvelope,
  statusEnvelope
} from "../../src/wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  OPERATION_CONTEXT_ASSEMBLED,
  OPERATION_SESSION_CANCEL,
  classify
} from "../../src/wire/dispatch.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId
} from "../../src/wire/ids.js"
import { AGENT_CONTROL, AGENT_SESSIONS } from "../../src/wire/topics.js"

const planner = AgentId.new("planner")

void test("given_a_failed_delta_write_when_the_next_write_runs_then_should_reseed_from_the_folded_lane", async () => {
  const lane = [delta(0n, "a", 1)]
  const written: bigint[] = []
  let failNext = false
  const session = {
    conversation: ConversationId.new(),
    laneRecords: () => Promise.resolve(lane.map((envelope) => ({ envelope }))),
    managedStateView: () => Promise.resolve(undefined),
    mintOpId: () => "op",
    writeEvent: (_operation: string, body: Uint8Array) => {
      if (failNext) {
        failNext = false
        return Promise.reject(new Error("the publish failed"))
      }
      written.push(baseOf(body))
      return Promise.resolve({})
    }
  } as unknown as Session
  const state = SessionState.create(session, cursor())
  await state.set("b", 2)
  failNext = true
  await assert.rejects(state.set("c", 3), /the publish failed/)
  await state.set("d", 4)
  assert.deepEqual(
    written,
    [1n, 1n],
    "the write after the failure names the revision the lane is at"
  )
})

void test("given_an_undecodable_state_record_when_the_state_is_read_then_should_skip_it_and_keep_writing", async () => {
  const garbage = new Uint8Array([0xff, 0x00, 0x13])
  const lane = [
    delta(0n, "a", 1),
    { ...delta(1n, "bad", 0), body: garbage },
    { ...delta(1n, "worse", 0), operation: OPERATION_STATE_SNAPSHOT, body: garbage },
    delta(1n, "b", 2)
  ]
  const written: bigint[] = []
  const session = {
    conversation: ConversationId.new(),
    laneRecords: () => Promise.resolve(lane.map((envelope) => ({ envelope }))),
    managedStateView: () => Promise.resolve(undefined),
    mintOpId: () => "op",
    writeEvent: (_operation: string, body: Uint8Array) => {
      written.push(baseOf(body))
      return Promise.resolve({})
    }
  } as unknown as Session
  const state = SessionState.create(session, cursor())

  const view = await state.get()
  await state.set("c", 3)

  assert.equal(view.revision, 2n)
  assert.deepEqual(view.document, { a: 1, b: 2 })
  assert.deepEqual(written, [2n], "the next write names the revision past the skipped record")
})

void test("given_concurrent_state_writes_when_sent_then_should_publish_one_at_a_time_in_revision_order", async () => {
  const written: bigint[] = []
  let inFlight = 0
  const session = {
    conversation: ConversationId.new(),
    laneRecords: () => Promise.resolve([]),
    managedStateView: () => Promise.resolve(undefined),
    mintOpId: () => "op",
    writeEvent: async (_operation: string, body: Uint8Array) => {
      inFlight += 1
      assert.equal(inFlight, 1, "one state write is in flight at a time")
      await new Promise((resolve) => setTimeout(resolve, 1))
      written.push(baseOf(body))
      inFlight -= 1
      return {}
    }
  } as unknown as Session
  const shared = cursor()
  const first = SessionState.create(session, shared)
  const second = SessionState.create(session, shared)
  await Promise.all([first.set("a", 1), second.set("b", 2), first.set("c", 3)])
  assert.deepEqual(written, [0n, 1n, 2n])
})

void test("given_recorded_model_and_tool_calls_when_classified_by_their_own_agent_then_should_be_observational", async () => {
  const { laser, sent } = fakeLaser()
  const session = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
  await session.model(new ModelRequest("gpt-test", new TextEncoder().encode("hi")))
  await session.tool("lookup", { id: 7 })
  const commands = sent.filter((envelope) => envelope.kind === AgentKind.Command)
  assert.equal(commands.length, 2)
  for (const command of commands) {
    assert.deepEqual(command.metadata?.get(METADATA_RECORDED), { kind: "bool", value: true })
    assert.equal(classify(command, AGENT_SESSIONS, planner.wireId(), "any"), "observational")
  }
})

void test("given_session_records_when_assembled_then_should_select_conversation_records_and_size_their_bodies", async () => {
  const { laser } = fakeLaser()
  const session = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
  const history = sessionHistory(session)
  const lens = Object.assign(session, {
    laneContext: (policy: {
      select(history: readonly ContextMessage[]): readonly ContextMessage[]
    }) => Promise.resolve(policy.select(history))
  })
  const assembled = await lens.assemble(new LastN(10))
  assert.deepEqual(
    assembled.fragments.map((fragment) => new TextDecoder().decode(contextMessageBody(fragment))),
    ["hi", "hello"]
  )
  assert.equal(assembled.text(), "hi\nhello")
  assert.equal(assembled.manifest.bytes, 7n, "the manifest counts the bodies")
  assert.deepEqual(
    assembled.manifest.fragments.map((fragment) => fragment.bytes),
    [2, 5]
  )
})

void test("given_more_conversation_records_than_the_manifest_cap_when_assembled_then_should_refuse_before_writing", async () => {
  const { laser, sent } = fakeLaser()
  const session = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
  const records = Array.from({ length: 1025 }, (_, offset) =>
    message(session, offset, input(session, offset, "turn"))
  )
  const lens = Object.assign(session, {
    laneContext: () => Promise.resolve(records)
  })
  await assert.rejects(lens.assemble(new LastN(2000)), InvalidError)
  assert.equal(sent.length, 0)
})

void test("given_enveloped_messages_when_budgeted_then_should_count_the_body_not_the_envelope", () => {
  const { laser } = fakeLaser()
  const session = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
  const history = Array.from({ length: 5 }, (_, offset) =>
    message(session, offset, input(session, offset, "word"))
  )
  assert.ok(nth(history, 0).payload.byteLength > 20)
  assert.equal(new TokenBudget(5).select(history).length, 5)
})

void test("given_non_integer_bounds_when_building_context_policies_then_should_refuse_them", () => {
  for (const bound of [Number.NaN, -1, 1.5, Number.POSITIVE_INFINITY]) {
    assert.throws(() => new LastN(bound), InvalidError)
    assert.throws(() => new TokenBudget(bound), InvalidError)
  }
  assert.equal(new LastN(0).select([]).length, 0)
})

void test("given_a_per_agent_partition_layout_when_replaying_a_session_then_should_read_only_its_partitions", async () => {
  const conversation = ConversationId.new()
  const keyed = controlPartition(conversation.toString(), 4) ?? 0
  const declared = (keyed + 2) % 4
  const layout: SessionLayout = {
    kind: "perAgentPartition",
    partitions: new Map([["worker", declared]])
  }
  const { laser, polled } = partitionLaser(layout)
  await replayContext(
    {
      conversation,
      acrossSubconversations: false,
      topics: [AGENT_SESSIONS, AGENT_CONTROL],
      policy: new LastN(Number.MAX_SAFE_INTEGER),
      fromOffsets: new Map(),
      conversationPartitions: true
    },
    laser
  )
  assert.deepEqual(
    [...new Set(polled)].sort(),
    [
      `${AGENT_CONTROL}/${String(keyed)}`,
      `${AGENT_SESSIONS}/${String(declared)}`,
      `${AGENT_SESSIONS}/${String(keyed)}`
    ].sort()
  )
})

void test("given_a_per_agent_topic_layout_when_reading_a_session_then_should_read_the_declared_topics", async () => {
  const layout: SessionLayout = {
    kind: "perAgentTopic",
    topics: new Map([
      ["planner", "planner.inbox"],
      ["worker", "worker.inbox"]
    ])
  }
  const { laser, polled } = partitionLaser()
  const session = Sessions.create(laser, new SessionConfig().layout(layout)).open(
    ConversationId.new()
  )
  assert.deepEqual(session.laneTopics(), [AGENT_SESSIONS, "planner.inbox", "worker.inbox"])
  await session.state().get()
  const topics = [...new Set(polled.map((entry) => entry.split("/")[0]))].sort()
  assert.deepEqual(topics, [AGENT_SESSIONS, "planner.inbox", "worker.inbox"])
})

void test("given_a_cancel_for_another_agent_when_asked_then_should_not_report_a_cancel", async () => {
  const { laser } = fakeLaser()
  const session = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
  const cancel = (target: string | undefined) => {
    const envelope = commandEnvelope(
      RecordId.fromU128(BigInt(target === undefined ? 1 : 2)),
      WireConversationId.parse(session.conversation.toString()),
      AgentId.new("operator").wireId(),
      CorrelationId.fromU128(3n),
      new TextEncoder().encode("{}")
    )
    return {
      ...envelope,
      operation: OPERATION_SESSION_CANCEL,
      ...(target === undefined ? {} : { target: AgentId.new(target).wireId() })
    }
  }
  const lens = (records: readonly AgentEnvelope[]) =>
    Object.assign(Sessions.create(laser).open(session.conversation).asAgent(planner), {
      controlRecords: () =>
        Promise.resolve(
          records.map((envelope, offset) => ({
            envelope,
            timestampMicros: BigInt(offset),
            id: { partitionId: 0, offset: BigInt(offset) }
          }))
        )
    })
  assert.equal(await lens([cancel("critic")]).cancelRequested(), false)
  assert.equal(await lens([cancel("planner")]).cancelRequested(), true)
  assert.equal(await lens([cancel(undefined)]).cancelRequested(), true)
})

void test("given_an_ended_named_session_when_begun_then_should_refuse_it_and_resume_only_a_completed_or_failed_one", async () => {
  const { laser, sent } = fakeLaser()
  const sessions = Sessions.create(laser)
  const original = Object.getOwnPropertyDescriptor(Session.prototype, "endedAs")
  assert.ok(original !== undefined)
  try {
    for (const ended of ["completed", "failed", "canceled"] as const) {
      Session.prototype.endedAs = () => Promise.resolve(ended)
      await assert.rejects(
        sessions.create("ticket-7").agent(planner).begin(),
        (error: unknown) => error instanceof SessionEndedError && error.status === ended
      )
      if (ended === "canceled") {
        await assert.rejects(sessions.create("ticket-7").agent(planner).resume(), SessionEndedError)
      }
    }
    assert.equal(sent.length, 0, "a refused start writes nothing")
    Session.prototype.endedAs = () => Promise.reject(new Error("a fresh session reads nothing"))
    const fresh = await sessions.start().agent(planner).begin()
    fresh.lease.release()
  } finally {
    Object.defineProperty(Session.prototype, "endedAs", original)
  }
})

void test("given_a_lane_that_ended_and_started_again_when_folded_then_should_follow_the_index_rule", () => {
  const conversation = WireConversationId.fromU128(1n)
  const status = (name: "Working" | "Completed" | "Failed" | "Canceled", body: Uint8Array) => ({
    ...statusEnvelope(RecordId.fromU128(1n), conversation, planner.wireId(), OPERATION_SESSION),
    taskState: { kind: "known" as const, name },
    body
  })
  const start = (tokens: bigint) =>
    status(
      "Working",
      encodeNamed(
        encodeSessionStart({
          agent: planner.wireId(),
          sdk: { language: "typescript", version: "0" },
          budget: { tokens },
          tags: []
        })
      )
    )
  const end = (name: "Completed" | "Failed" | "Canceled") =>
    status(name, encodeNamed(encodeSessionEnd({})))
  const fold = (envelopes: readonly AgentEnvelope[]) => {
    const lifecycle = new LaneLifecycle()
    for (const envelope of envelopes) lifecycle.add(envelope)
    return lifecycle
  }
  assert.equal(fold([start(1n), end("Completed")]).ended, "completed")
  const resumed = fold([start(1n), end("Completed"), start(9n)])
  assert.equal(resumed.ended, undefined)
  assert.equal(resumed.budget?.tokens, 9n, "a resume carries its own budget")
  assert.equal(fold([start(1n), end("Failed"), start(9n), end("Completed")]).ended, "completed")
  const canceled = fold([start(1n), end("Canceled"), start(9n)])
  assert.equal(canceled.ended, "canceled", "a canceled session stays ended")
  assert.equal(canceled.budget?.tokens, 1n)
  assert.equal(fold([start(1n), start(5n)]).budget?.tokens, 1n, "an open session keeps its start")
})

void test("given_a_submission_when_its_command_fails_then_should_end_the_session_failed_and_name_its_id", async () => {
  const sent: AgentEnvelope[] = []
  const transport = {
    sendMessagesWithHeaders(
      _stream: string,
      _topic: string,
      messages: readonly MessageWithHeaders[]
    ) {
      for (const message of messages) {
        const envelope = decodeAgentEnvelope(
          expectMap(decodeOne(message.payload, "sent"), "sent"),
          "sent"
        )
        if (envelope.kind === AgentKind.Command)
          return Promise.reject(new Error("the inbox is gone"))
        sent.push(envelope)
      }
      return Promise.resolve({ confirmations: [] })
    }
  } as unknown as LaserTransport
  const { laser } = fakeLaser()
  const failing = Object.assign(Object.create(laser) as object, {
    agdx(topic: string, source: AgentId, conversation: ConversationId) {
      return createAgdx(transport, "agents", topic, source, conversation)
    }
  }) as unknown as Laser
  const builder = Sessions.create(failing)
    .submit(AgentId.new("worker"), new Uint8Array([1]))
    .from(planner)
  const id = builder.id()
  assert.ok(builder.id().equals(id), "an unlabeled submission keeps one id")
  await assert.rejects(builder.send(), /the inbox is gone/)
  assert.deepEqual(
    sent.map((envelope) => envelope.taskState),
    [
      { kind: "known", name: "Submitted" },
      { kind: "known", name: "Failed" }
    ]
  )
  assert.ok(sent.every((envelope) => envelope.conversation.toString() === id.toString()))
  const labeled = Sessions.create(laser)
    .submit(AgentId.new("worker"), new Uint8Array())
    .label("t-1")
  assert.ok(labeled.id().equals(deriveSessionId("agents", "", "t-1")))
})

void test("given_a_zero_idle_timeout_or_heartbeat_when_configured_then_should_refuse_it", () => {
  assert.throws(() => new SessionConfig().idleTimeout(0), InvalidError)
  assert.throws(() => new SessionConfig().heartbeat(0), InvalidError)
  const { laser } = fakeLaser()
  assert.throws(() => Sessions.create(laser).start().idleTimeout(0), InvalidError)
})

void test("given_a_checkpoint_of_another_stream_or_topic_generation_when_read_then_should_refuse_it", () => {
  const rust =
    '{"per_topic":{"agent.sessions":{"0":5}},"stream":"support","generations":{"agent.sessions":7}}'
  const checkpoint = Checkpoint.fromJSON(rust)
  assert.equal(JSON.stringify(checkpoint), rust)
  assert.doesNotThrow(() => {
    checkpoint.check("support", "agent.sessions", 7n)
  })
  assert.throws(() => {
    checkpoint.check("billing", "agent.sessions", 7n)
  }, InvalidError)
  assert.throws(() => {
    checkpoint.check("support", "agent.sessions", 8n)
  }, InvalidError)
  assert.doesNotThrow(() => {
    checkpoint.check("support", "agent.streams", 1n)
  })
})

function cursor(): {
  revision: bigint
  head: string | undefined
  document: unknown
  sinceSnapshot: number
  tail: Promise<void>
} {
  return {
    revision: 0n,
    head: undefined,
    document: undefined,
    sinceSnapshot: 0,
    tail: Promise.resolve()
  }
}

function delta(baseRevision: bigint, key: string, value: number): AgentEnvelope {
  return {
    ...eventEnvelope(
      RecordId.fromU128(baseRevision + 1n),
      WireConversationId.fromU128(1n),
      planner.wireId(),
      encodeNamed(
        encodeStateDelta({
          baseRevision,
          patch: [{ op: "add", path: `/${key}`, value }],
          opId: `op-${key}`
        })
      )
    ),
    operation: OPERATION_STATE_DELTA
  }
}

function baseOf(body: Uint8Array): bigint {
  return decodeStateDelta(expectMap(decodeOne(body, "delta"), "delta"), "delta").baseRevision
}

function input(session: Session, offset: number, text: string): AgentEnvelope {
  return {
    ...commandEnvelope(
      RecordId.fromU128(BigInt(offset) + 1n),
      WireConversationId.parse(session.conversation.toString()),
      AgentId.new("user").wireId(),
      CorrelationId.fromU128(BigInt(offset) + 1n),
      new TextEncoder().encode(text)
    ),
    operation: OPERATION_CHAT,
    target: planner.wireId()
  }
}

function sessionHistory(session: Session): readonly ContextMessage[] {
  const conversation = WireConversationId.parse(session.conversation.toString())
  const correlation = CorrelationId.fromU128(9n)
  const envelopes: AgentEnvelope[] = [
    {
      ...statusEnvelope(RecordId.fromU128(1n), conversation, planner.wireId(), OPERATION_SESSION),
      taskState: { kind: "known", name: "Working" }
    },
    input(session, 1, "hi"),
    {
      ...commandEnvelope(
        RecordId.fromU128(3n),
        conversation,
        planner.wireId(),
        correlation,
        new TextEncoder().encode("hi")
      ),
      operation: OPERATION_CHAT,
      metadata: new Map([[METADATA_RECORDED, { kind: "bool", value: true }]])
    },
    {
      ...eventEnvelope(RecordId.fromU128(4n), conversation, planner.wireId(), new Uint8Array()),
      operation: OPERATION_CONTEXT_ASSEMBLED
    },
    {
      ...responseEnvelope(
        RecordId.fromU128(5n),
        conversation,
        planner.wireId(),
        correlation,
        new TextEncoder().encode("hello")
      ),
      operation: OPERATION_CHAT
    },
    {
      ...eventEnvelope(RecordId.fromU128(6n), conversation, planner.wireId(), new Uint8Array()),
      operation: OPERATION_STATE_DELTA
    }
  ]
  return envelopes.map((envelope, offset) => message(session, offset, envelope))
}

function message(session: Session, offset: number, envelope: AgentEnvelope): ContextMessage {
  return {
    id: { partitionId: 0, offset: BigInt(offset) },
    provenance: { conversationId: session.conversation },
    payload: encodeNamed(
      new Map<string, unknown>([
        ["body", envelope.body],
        ["pad", "x".repeat(24)]
      ])
    ),
    envelope,
    topic: AGENT_SESSIONS,
    timestampMicros: BigInt(offset),
    streamId: 1,
    topicId: 2
  }
}

function nth<Item>(items: readonly Item[], index: number): Item {
  const item = items.at(index)
  assert.ok(item !== undefined, `expected an item at ${String(index)}`)
  return item
}

function fakeLaser(): { readonly laser: Laser; readonly sent: AgentEnvelope[] } {
  const sent: AgentEnvelope[] = []
  const transport = {
    sendMessagesWithHeaders(
      _stream: string,
      _topic: string,
      messages: readonly MessageWithHeaders[]
    ) {
      for (const message of messages) {
        sent.push(
          decodeAgentEnvelope(expectMap(decodeOne(message.payload, "sent"), "sent"), "sent")
        )
      }
      return Promise.resolve({ confirmations: [] })
    }
  } as unknown as LaserTransport
  const laser = {
    defaultStream: "agents",
    [INTERNAL_LAYOUTS]: () => new Map(),
    [INTERNAL_SESSION_LEASES]: () => ({ acquire: () => ({ release: () => undefined }) }),
    [INTERNAL_TRANSPORT]: () => ({
      findSnapshotStream: () => Promise.resolve({ id: 1, createdAtMicros: 1n }),
      findSnapshotTopic: () => Promise.resolve({ id: 2, createdAtMicros: 3n, partitions: 4 }),
      findTopicPartitionCount: () => Promise.resolve(1)
    }),
    capabilities: () => Promise.resolve(OPEN_CAPABILITIES),
    agdx(topic: string, source: AgentId, conversation: ConversationId) {
      return createAgdx(transport, "agents", topic, source, conversation)
    }
  }
  return { laser: laser as unknown as Laser, sent }
}

function partitionLaser(layout?: SessionLayout): {
  readonly laser: Laser
  readonly polled: string[]
} {
  const polled: string[] = []
  const layouts = new Map<string, SessionLayout>(layout === undefined ? [] : [["agents", layout]])
  const transport = {
    pollMessages: (_stream: string, topic: string, target: { readonly partitionId: number }) => {
      polled.push(`${topic}/${String(target.partitionId)}`)
      return Promise.resolve([])
    }
  } as unknown as LaserTransport
  const laser = {
    defaultStream: "agents",
    [INTERNAL_LAYOUTS]: () => layouts,
    [INTERNAL_TRANSPORT]: () => transport,
    capabilities: () => Promise.resolve(OPEN_CAPABILITIES),
    topic: (name: string) => ({
      replay: () => Promise.resolve(Cursor.create(transport, "agents", name, [0, 1, 2, 3])),
      partitionCount: () => Promise.resolve(4),
      tailOffsets: () =>
        Promise.resolve(
          new Map([
            [0, 5n],
            [1, 5n],
            [2, 5n],
            [3, 5n]
          ])
        )
    })
  }
  return { laser: laser as unknown as Laser, polled }
}

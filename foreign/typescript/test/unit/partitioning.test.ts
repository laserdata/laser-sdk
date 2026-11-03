import assert from "node:assert/strict"
import { test } from "node:test"
import { createAgdx } from "../../src/agent/agdx.js"
import { Agent } from "../../src/agent/builder.js"
import { ReliableConsumer } from "../../src/agent/reliable-consumer.js"
import {
  declaredAgentTopic,
  replyTopicFor,
  resolveAgentPartition,
  resolveAgentTopic
} from "../../src/agent/partitioning.js"
import { InvalidError } from "../../src/client/errors.js"
import {
  INTERNAL_ENSURE_RETAINED,
  INTERNAL_LAYOUTS,
  INTERNAL_TRANSPORT
} from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient, LaserTransport } from "../../src/iggy/apache-iggy.js"
import { SessionConfig, Sessions, type SessionLayout, TopicRetention } from "../../src/session.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { type AgentId as WireAgentId, AgentKind, parseAgentId } from "../../src/wire/agent.js"
import { CorrelationId } from "../../src/wire/ids.js"
import { AGENT_SESSIONS } from "../../src/wire/topics.js"

const layout: SessionLayout = {
  kind: "perAgentPartition",
  partitions: new Map([
    ["planner", 0],
    ["worker", 2]
  ])
}

void test("given_a_declared_partition_layout_when_routing_then_should_key_commands_by_addressee_and_replies_by_requester", () => {
  const route = (kind: AgentKind, target?: string): number | undefined =>
    resolveAgentPartition(
      layout,
      AGENT_SESSIONS,
      kind,
      target === undefined ? undefined : parseAgentId(target)
    )
  assert.equal(route(AgentKind.Command, "worker"), 2)
  assert.equal(route(AgentKind.Response, "planner"), 0)
  assert.equal(route(AgentKind.Command, "stranger"), undefined)
  assert.equal(route(AgentKind.Status), undefined)
  assert.equal(route(AgentKind.Event, "worker"), undefined)
  const worker: WireAgentId = parseAgentId("worker")
  assert.equal(resolveAgentPartition(layout, "agent.control", AgentKind.Command, worker), undefined)
  assert.equal(
    resolveAgentPartition(undefined, AGENT_SESSIONS, AgentKind.Command, worker),
    undefined
  )
  assert.equal(
    resolveAgentPartition({ kind: "shared" }, AGENT_SESSIONS, AgentKind.Command, worker),
    undefined
  )
})

void test("given_a_declared_layout_when_an_agdx_record_is_sent_then_should_pass_its_partition_to_the_transport", async () => {
  const sent: (number | undefined)[] = []
  const transport = {
    sendMessagesWithHeaders(
      _stream: string,
      _topic: string,
      _messages: unknown,
      _key: unknown,
      partition?: number
    ) {
      sent.push(partition)
      return Promise.resolve({ confirmations: [] })
    }
  } as unknown as LaserTransport
  const agdx = (topic: string) =>
    createAgdx(
      transport,
      "agents",
      topic,
      AgentId.new("planner"),
      ConversationId.new(),
      undefined,
      undefined,
      undefined,
      () => layout
    )
  const correlation = CorrelationId.fromU128(9n)
  await agdx(AGENT_SESSIONS)
    .command(correlation, new Uint8Array([1]))
    .withTarget(AgentId.new("worker"))
    .send()
  await agdx(AGENT_SESSIONS)
    .command(correlation, new Uint8Array([1]))
    .send()
  await agdx("agent.control")
    .command(correlation, new Uint8Array([1]))
    .withTarget(AgentId.new("worker"))
    .send()
  const stream = agdx(AGENT_SESSIONS)
    .stream(correlation, "chat")
    .withTarget(AgentId.new("planner"))
    .buffered(10, 10_000)
  await stream.write(new Uint8Array([1]))
  await stream.finish("stop")
  assert.deepEqual(sent, [2, undefined, undefined, 0])
})

void test("given_an_explicit_layout_when_sessions_open_then_should_declare_it_for_the_stream_only_when_set", async () => {
  const layouts = new Map<string, SessionLayout>()
  const bootstrapped: number[] = []
  const laser = {
    defaultStream: "agents",
    withDefaultStream: () => laser,
    bootstrap: (partitions: number) => {
      bootstrapped.push(partitions)
      return Promise.resolve()
    },
    capabilities: () => Promise.resolve({ sessions: false }),
    [INTERNAL_LAYOUTS]: () => layouts,
    [INTERNAL_TRANSPORT]: () => ({
      findSnapshotStream: () => Promise.resolve({ id: 0, createdAtMicros: 1n }),
      findSnapshotTopic: () =>
        Promise.resolve({ id: 0, createdAtMicros: 2n, partitions: bootstrapped.at(-1) ?? 4 })
    })
  } as unknown as Laser
  Sessions.create(laser)
  assert.equal(layouts.size, 0)
  assert.deepEqual(new SessionConfig().layoutKind, { kind: "shared" })
  Sessions.create(laser, new SessionConfig().layout(layout))
  assert.equal(layouts.get("agents"), layout)
  const single = Sessions.create(laser, new SessionConfig().layout({ kind: "singlePartition" }))
  await single.bootstrap(4, TopicRetention.expireAfter(60_000))
  await Sessions.create(laser).bootstrap(4, TopicRetention.expireAfter(60_000))
  assert.deepEqual(bootstrapped, [1, 4])
})

const topics: SessionLayout = {
  kind: "perAgentTopic",
  topics: new Map([
    ["planner", "planner.inbox"],
    ["worker", "worker.inbox"]
  ])
}

void test("given_a_declared_topic_layout_when_routing_then_should_move_only_addressed_work_off_the_lane", () => {
  const route = (kind: AgentKind | undefined, target?: string): string | undefined =>
    resolveAgentTopic(topics, AGENT_SESSIONS, kind, target)
  assert.equal(route(AgentKind.Command, "worker"), "worker.inbox")
  assert.equal(route(AgentKind.Response, "planner"), "planner.inbox")
  assert.equal(route(AgentKind.Error, "planner"), "planner.inbox")
  assert.equal(route(AgentKind.Chunk, "worker"), "worker.inbox")
  assert.equal(route(undefined, "worker"), "worker.inbox")
  assert.equal(route(AgentKind.Status, "worker"), undefined)
  assert.equal(route(AgentKind.Event, "worker"), undefined)
  assert.equal(route(AgentKind.Command, "stranger"), undefined)
  assert.equal(route(AgentKind.Command, "*"), undefined)
  assert.equal(route(AgentKind.Command), undefined)
  assert.equal(resolveAgentTopic(topics, "agent.control", AgentKind.Command, "worker"), undefined)
  assert.equal(resolveAgentTopic(layout, AGENT_SESSIONS, AgentKind.Command, "worker"), undefined)
  assert.equal(declaredAgentTopic(topics, "worker"), "worker.inbox")
  assert.equal(declaredAgentTopic(topics, "stranger"), undefined)
  assert.equal(declaredAgentTopic(undefined, "worker"), undefined)
  assert.equal(replyTopicFor(topics, AGENT_SESSIONS, "planner"), "planner.inbox")
  assert.equal(replyTopicFor(topics, "replies", "planner"), "replies")
  assert.equal(replyTopicFor(topics, AGENT_SESSIONS, "stranger"), AGENT_SESSIONS)
  assert.equal(replyTopicFor(topics, AGENT_SESSIONS, undefined), AGENT_SESSIONS)
})

void test("given_a_declared_topic_layout_when_agdx_records_are_sent_then_should_redirect_addressed_work_and_skip_the_lane_guard", async () => {
  const sent: { topic: string; key: unknown; partition: number | undefined }[] = []
  let guarded = 0
  const transport = {
    sendMessagesWithHeaders(
      _stream: string,
      topic: string,
      _messages: unknown,
      key: unknown,
      partition?: number,
      options?: { readonly beforeSend?: () => Promise<unknown> }
    ) {
      sent.push({ topic, key, partition })
      return (options?.beforeSend?.() ?? Promise.resolve()).then(() => ({ confirmations: [] }))
    }
  } as unknown as LaserTransport
  const conversation = ConversationId.new()
  const agdx = createAgdx(
    transport,
    "agents",
    AGENT_SESSIONS,
    AgentId.new("planner"),
    conversation,
    undefined,
    undefined,
    undefined,
    () => topics
  ).withLaneGuard(() => {
    guarded += 1
    return Promise.resolve({ streamId: 1, topicId: 2, partitions: 4 })
  })
  const correlation = CorrelationId.fromU128(9n)
  await agdx
    .command(correlation, new Uint8Array([1]))
    .withTarget(AgentId.new("worker"))
    .send()
  await agdx
    .respond(correlation, new Uint8Array([2]))
    .withTarget(AgentId.new("planner"))
    .send()
  await agdx
    .emit(new Uint8Array([5]))
    .withTarget(AgentId.new("worker"))
    .send()
  await agdx.command(correlation, new Uint8Array([3])).send()
  const stream = agdx
    .stream(correlation, "chat")
    .withTarget(AgentId.new("worker"))
    .buffered(10, 10_000)
  await stream.write(new Uint8Array([4]))
  await stream.finish("stop")
  assert.deepEqual(
    sent.map((entry) => entry.topic),
    ["worker.inbox", "planner.inbox", AGENT_SESSIONS, AGENT_SESSIONS, "worker.inbox"]
  )
  assert.ok(sent.every((entry) => entry.key === conversation.toString()))
  assert.ok(sent.every((entry) => entry.partition === undefined))
  assert.equal(guarded, 2)
})

void test("given_a_declared_topic_layout_when_bootstrapped_then_should_create_each_declared_topic_like_the_lane", async () => {
  const created: [string, number, TopicRetention][] = []
  const laser = {
    defaultStream: "agents",
    withDefaultStream: () => laser,
    bootstrap: () => Promise.resolve(),
    capabilities: () => Promise.resolve({ sessions: false }),
    [INTERNAL_LAYOUTS]: () => new Map<string, SessionLayout>(),
    [INTERNAL_ENSURE_RETAINED]: (topic: string, partitions: number, retention: TopicRetention) => {
      created.push([topic, partitions, retention])
      return Promise.resolve()
    },
    [INTERNAL_TRANSPORT]: () => ({
      findSnapshotStream: () => Promise.resolve({ id: 0, createdAtMicros: 1n }),
      findSnapshotTopic: () => Promise.resolve({ id: 0, createdAtMicros: 2n, partitions: 4 })
    })
  } as unknown as Laser
  const retention = TopicRetention.expireAfter(60_000)
  const shared: SessionLayout = {
    kind: "perAgentTopic",
    topics: new Map([
      ["planner", "planner.inbox"],
      ["worker", "worker.inbox"],
      ["reviewer", "worker.inbox"]
    ])
  }
  await Sessions.create(laser, new SessionConfig().layout(shared)).bootstrap(3, retention)
  assert.deepEqual(created, [
    ["planner.inbox", 3, retention],
    ["worker.inbox", 3, retention]
  ])
  for (const refused of [AGENT_SESSIONS, "agent.control", ""]) {
    const bad: SessionLayout = { kind: "perAgentTopic", topics: new Map([["worker", refused]]) }
    await assert.rejects(
      Sessions.create(laser, new SessionConfig().layout(bad)).bootstrap(3, retention),
      InvalidError
    )
  }
})

void test("given_a_declared_topic_layout_when_a_plain_agent_record_is_sent_then_should_land_on_the_addressee_topic", async () => {
  const topicsSent: string[] = []
  const client = {
    clientProvider: () => Promise.resolve({ protocol: "vsr" }),
    destroy: () => Promise.resolve(),
    topic: { get: () => Promise.resolve({ partitionsCount: 1 }) },
    message: {
      send: (request: { readonly topicId: string }) => {
        topicsSent.push(request.topicId)
        return Promise.resolve({ confirmations: [] })
      }
    }
  } as unknown as IggyClient
  await using root = await Laser.fromClient(client)
  const laser = root.withDefaultStream("agents")
  laser.sessions(new SessionConfig().layout(topics))
  const conversationId = ConversationId.new()
  await laser.sendAgent(AGENT_SESSIONS, new Uint8Array([1]), {
    conversationId,
    agent: AgentId.new("planner"),
    targetAgentId: AgentId.new("worker")
  })
  await laser.sendAgent(AGENT_SESSIONS, new Uint8Array([2]), {
    conversationId,
    agent: AgentId.new("planner")
  })
  await laser.sendAgent(AGENT_SESSIONS, new Uint8Array([3]), {
    conversationId,
    targetAgentId: AgentId.new("stranger")
  })
  assert.deepEqual(topicsSent, ["worker.inbox", AGENT_SESSIONS, AGENT_SESSIONS])
})

void test("given_a_declared_agent_of_a_topic_layout_when_spawned_on_the_lane_then_should_read_its_own_topic", async (t) => {
  const read: string[] = []
  t.mock.method(ReliableConsumer.prototype, "run", function (this: ReliableConsumer) {
    read.push((this as unknown as { readonly options: { readonly topic: string } }).options.topic)
    return Promise.resolve()
  })
  const client = {
    clientProvider: () => Promise.resolve({ protocol: "vsr" }),
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  await using root = await Laser.fromClient(client)
  const laser = root.withDefaultStream("agents")
  const config = new SessionConfig().layout(topics)
  for (const [id, listenOn] of [
    ["worker", AGENT_SESSIONS],
    ["stranger", AGENT_SESSIONS],
    ["worker", "custom.inbox"]
  ] as const) {
    const handle = Agent.builder()
      .id(AgentId.new(id))
      .listenOn(listenOn)
      .sessions(config)
      .handler({ handle: () => Promise.resolve() })
      .build()
      .spawn(laser)
    await handle.ready()
    await handle.shutdown()
  }
  assert.deepEqual(read, ["worker.inbox", AGENT_SESSIONS, "custom.inbox"])
})

import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { Checkpoint } from "../../src/context.js"
import type { LaserTransport } from "../../src/iggy/apache-iggy.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import {
  DEFAULT_SESSION_MEMORY_NAMESPACE,
  DEFAULT_SESSION_TOPICS,
  SessionConfig,
  Sessions,
  sessionTurnKind,
  sessionTurnTopic,
  type SessionTurnKind
} from "../../src/session.js"
import { Cursor } from "../../src/stream/cursor.js"
import { Topic } from "../../src/stream/topic.js"

const KINDS: readonly [SessionTurnKind, string][] = [
  ["instruction", AgentTopic.Commands],
  ["response", AgentTopic.Responses],
  ["model.response", AgentTopic.LlmIo],
  ["tool.call", AgentTopic.ToolCalls],
  ["tool.result", AgentTopic.ToolResults],
  ["human.input", AgentTopic.HumanInput]
]

void test("given_each_turn_kind_when_mapped_then_should_ride_its_topic_and_read_back", () => {
  for (const [kind, topic] of KINDS) {
    assert.equal(sessionTurnTopic(kind), topic)
    assert.equal(sessionTurnKind(topic), kind)
  }
  assert.equal(sessionTurnKind("agent.audit"), undefined)
  assert.deepEqual([...DEFAULT_SESSION_TOPICS].sort(), KINDS.map(([, topic]) => topic).sort())
})

void test("given_a_custom_layout_when_a_kind_moves_topic_then_should_map_both_ways", () => {
  const config = new SessionConfig({
    stream: "support",
    topics: { instruction: "support.turns" },
    memoryNamespace: "support.sessions",
    contextTurns: 10,
    contextTokens: 800
  })
  assert.equal(config.topicFor("instruction"), "support.turns")
  assert.equal(config.topicFor("tool.call"), AgentTopic.ToolCalls)
  assert.equal(config.kindFor("support.turns"), "instruction")
  assert.equal(config.kindFor(AgentTopic.Commands), undefined)
  assert.equal(config.stream, "support")
  assert.equal(config.memoryNamespace, "support.sessions")
  assert.equal(config.contextTurns, 10)
  assert.equal(config.contextTokens, 800)
  assert.deepEqual(new SessionConfig().topicList, DEFAULT_SESSION_TOPICS)
})

void test("given_two_kinds_on_one_topic_when_configured_then_should_be_rejected", () => {
  assert.throws(
    () => new SessionConfig({ topics: { response: AgentTopic.Commands } }),
    InvalidError
  )
})

void test("given_a_checkpoint_when_round_tripped_through_json_then_should_keep_every_offset", () => {
  const checkpoint = Checkpoint.fromJSON({
    "agent.commands": { "0": "5", "1": "2" },
    "agent.llm_io": {}
  })
  assert.deepEqual(
    checkpoint.topicOffsets("agent.commands"),
    new Map([
      [0, 5n],
      [1, 2n]
    ])
  )
  assert.deepEqual(checkpoint.topicOffsets("agent.llm_io"), new Map())
  assert.equal(checkpoint.topicOffsets("agent.tool_calls"), undefined)
  assert.equal(checkpoint.isEmpty(), false)
  assert.equal(Checkpoint.empty().isEmpty(), true)
  const restored = Checkpoint.fromJSON(JSON.stringify(checkpoint))
  assert.deepEqual(restored.toJSON(), checkpoint.toJSON())
  assert.throws(() => Checkpoint.fromJSON(["nope"]), TypeError)
  assert.throws(() => Checkpoint.fromJSON({ topic: { x: "1" } }), TypeError)
})

interface Call {
  readonly topic: string
  readonly payload: Uint8Array
}

function fakeLaser(tails: ReadonlyMap<string, ReadonlyMap<number, bigint>>): {
  readonly laser: Laser
  readonly calls: Call[]
  readonly streams: string[]
} {
  const calls: Call[] = []
  const streams: string[] = []
  const laser = {
    sendAgent(topic: string, payload: Uint8Array): Promise<void> {
      calls.push({ topic, payload })
      return Promise.resolve()
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
  return { laser: laser as unknown as Laser, calls, streams }
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

void test("given_a_fake_laser_when_sessions_are_opened_then_should_derive_and_route_turns", async () => {
  const { laser, calls, streams } = fakeLaser(new Map())
  const sessions = new Sessions(laser, {
    stream: "support",
    topics: { instruction: "support.turns" }
  })
  assert.deepEqual(streams, ["support"])
  const session = sessions.create("agent-42")
  assert.ok(sessions.create("agent-42").conversation.equals(session.conversation))
  assert.ok(sessions.open(session.conversation).conversation.equals(session.conversation))
  assert.ok(!sessions.start().conversation.equals(session.conversation))
  await session.append("instruction", new Uint8Array([1]))
  await session.append("tool.call", new Uint8Array([2]))
  assert.deepEqual(
    calls.map((call) => call.topic),
    ["support.turns", AgentTopic.ToolCalls]
  )
  assert.deepEqual((session.graph("kg") as unknown as { name: string }).name, "kg")
  assert.equal(session.config.memoryNamespace, DEFAULT_SESSION_MEMORY_NAMESPACE)
})

void test("given_a_scoped_memory_when_searched_then_should_run_a_keyword_recall_in_the_namespace", async () => {
  const { laser } = fakeLaser(new Map())
  const session = new Sessions(laser, { memoryNamespace: "support.sessions" }).start()
  const hits = (await session.memory().search("login bug", 3)) as unknown as readonly {
    namespace: string
    text: string
    max: number
  }[]
  assert.deepEqual(hits, [{ namespace: "support.sessions", text: "login bug", max: 3 }])
  const defaults = (await session.memory("other").search("x")) as unknown as readonly {
    namespace: string
    max: number
  }[]
  assert.deepEqual(defaults, [{ namespace: "other", text: "x", max: 0 }])
})

void test("given_topic_tails_when_a_checkpoint_is_captured_then_should_record_each_partition_and_skip_missing_topics", async () => {
  const { laser } = fakeLaser(
    new Map([
      [
        AgentTopic.Commands,
        new Map([
          [0, 3n],
          [1, 0n]
        ])
      ],
      [AgentTopic.LlmIo, new Map([[0, 1n]])]
    ])
  )
  const checkpoint = await new Sessions(laser).start().checkpoint()
  assert.deepEqual([...checkpoint.topics].sort(), [...DEFAULT_SESSION_TOPICS].sort())
  assert.deepEqual(
    checkpoint.topicOffsets(AgentTopic.Commands),
    new Map([
      [0, 3n],
      [1, 0n]
    ])
  )
  assert.deepEqual(checkpoint.topicOffsets(AgentTopic.ToolCalls), new Map())
  assert.equal(
    Checkpoint.fromJSON(JSON.stringify(checkpoint)).topicOffsets(AgentTopic.LlmIo)?.get(0),
    1n
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
  const cursor = new Cursor(transport as unknown as LaserTransport, "s", "t", [0, 1], {
    batchSize: 4
  })
  cursor.fromOffsets(new Map([[0, 2n]])).until(
    new Map([
      [0, 5n],
      [1, 0n]
    ])
  )
  const first = await cursor.poll()
  assert.deepEqual(
    first.map((message) => [message.partitionId, message.offset]),
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
  const present = new Topic(transport, "s", "present")
  assert.equal(await present.partitionCount(), 2)
  assert.deepEqual(
    await present.tailOffsets(),
    new Map([
      [0, 7n],
      [1, 0n]
    ])
  )
  const missing = new Topic(transport, "s", "missing")
  assert.equal(await missing.partitionCount(), undefined)
  assert.deepEqual(await missing.tailOffsets(), new Map())
})

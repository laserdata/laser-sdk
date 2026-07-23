import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { LastN, RoleFilter } from "../../src/context.js"
import { FULL_REPLAY } from "../../src/conversation-state.js"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

async function eventually<Value>(read: () => Promise<Value | undefined>): Promise<Value> {
  const deadline = performance.now() + 2_000
  let value = await read()
  while (value === undefined && performance.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 10))
    value = await read()
  }
  assert.ok(value !== undefined)
  return value
}

void test("given_interleaved_turns_when_fetching_context_then_should_order_and_filter_them", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(2)
    const conversation = ConversationId.new()
    const turns = [
      [AgentTopic.Commands, "planner", "draft"],
      [AgentTopic.Responses, "writer", "first draft"],
      [AgentTopic.Commands, "planner", "tighten"],
      [AgentTopic.Responses, "writer", "tightened"]
    ] as const
    for (const [topic, agent, payload] of turns) {
      await laser.sendAgent(topic, new TextEncoder().encode(payload), {
        conversationId: conversation,
        agent: AgentId.new(agent)
      })
      await new Promise((resolve) => setTimeout(resolve, 2))
    }
    const scope = laser.context(conversation)
    const history = await eventually(async () => {
      const messages = await scope.fetchWith(
        [AgentTopic.Commands, AgentTopic.Responses],
        new LastN(10)
      )
      return messages.length === 4 ? messages : undefined
    })
    assert.deepEqual(
      history.map((message) => new TextDecoder().decode(message.payload)),
      ["draft", "first draft", "tighten", "tightened"]
    )
    const writers = await scope.fetchWith(
      [AgentTopic.Commands, AgentTopic.Responses],
      new RoleFilter([AgentId.new("writer")])
    )
    assert.equal(writers.length, 2)
  } finally {
    await laser.close()
  }
})

void test("given_scoped_events_when_folding_state_then_should_replay_deterministically", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const scope = laser.context(ConversationId.new())
    for (let value = 1; value <= 5; value += 1) {
      await scope.append(AgentTopic.Commands, new TextEncoder().encode(String(value)))
    }
    const sum = await eventually(async () => {
      const folded = await scope.state(
        [AgentTopic.Commands],
        FULL_REPLAY,
        0,
        (total, message) => total + Number(new TextDecoder().decode(message.payload))
      )
      return folded === 15 ? folded : undefined
    })
    assert.equal(sum, 15)
    assert.equal(await scope.block([AgentTopic.Commands], 2), "4\n5")
  } finally {
    await laser.close()
  }
})

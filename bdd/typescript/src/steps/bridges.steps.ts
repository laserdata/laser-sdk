import assert from "node:assert/strict"
import { Then, When } from "@cucumber/cucumber"
import {
  A2aBridge,
  AgentId,
  AgentTopic,
  CorrelationId,
  InvalidError,
  OPERATION_CHAT,
  enterBridge
} from "@laserdata/laser-sdk"

import type { LaserWorld } from "../world.js"

const encoder = new TextEncoder()

When(
  /^bridge "([^"]+)" enters after hops "([^"]+)"$/,
  function (this: LaserWorld, bridge: string, hops: string) {
    this.bridgeHops = enterBridge(bridge, hops.split(","))
  }
)

Then(/^the bridge hops are "([^"]+)"$/, function (this: LaserWorld, hops: string) {
  assert.deepEqual(this.bridgeHops, hops.split(","))
})

When(
  /^bridge "([^"]+)" enters the same route$/,
  function (this: LaserWorld, bridge: string) {
    try {
      enterBridge(bridge, this.bridgeHops)
      this.bridgeLoopRejected = false
    } catch (error) {
      assert.ok(error instanceof InvalidError)
      this.bridgeLoopRejected = true
    }
  }
)

Then("the bridge route is rejected as a loop", function (this: LaserWorld) {
  assert.equal(this.bridgeLoopRejected, true)
})

When("I submit and cancel an A2A task", async function (this: LaserWorld) {
  const bridge = new A2aBridge(
    this.requireLaser(),
    AgentId.new("a2a-gateway"),
    AgentTopic.Commands,
    AgentTopic.Responses
  )
  const submitted = await bridge.submit({ message: { role: "user", text: "cancel me" } })
  await bridge.cancel(submitted.id)
  const replayed = await bridge.task(submitted.id)
  if (replayed.status.state.kind !== "known") {
    throw new Error("A2A task returned an unrecognized state")
  }
  this.bridgeTaskState = replayed.status.state.name
})

Then(
  /^the replayed A2A task state is "([^"]+)"$/,
  function (this: LaserWorld, state: string) {
    assert.equal(this.bridgeTaskState, state)
  }
)

When(
  "I publish an AG-UI count snapshot of 1 and replace it with 2",
  async function (this: LaserWorld) {
    const laser = this.requireLaser()
    const conversation = this.requireConversation()
    const source = AgentId.new("agui-gateway")
    await laser.publishStateSnapshot(AgentTopic.Audit, source, conversation, { count: 1 })
    await laser.publishStateDelta(AgentTopic.Audit, source, conversation, [
      { op: "replace", path: "/count", value: 2 }
    ])
    this.reconstructedState = await laser.reconstructState(conversation, AgentTopic.Audit)
  }
)

Then(/^the reconstructed AG-UI count is (\d+)$/, function (this: LaserWorld, count: string) {
  assert.deepEqual(this.reconstructedState, { count: Number(count) })
})

When(
  /^I stream chat chunks "([^"]+)" and "([^"]+)"$/,
  async function (this: LaserWorld, first: string, second: string) {
    const laser = this.requireLaser()
    const conversation = this.requireConversation()
    const stream = laser
      .agdx(AgentTopic.LlmIo, AgentId.new("assistant"), conversation)
      .stream(CorrelationId.parse(conversation.toString()), OPERATION_CHAT)
    await stream.write(encoder.encode(first))
    await stream.write(encoder.encode(second))
    await stream.finish("stop")
    this.aguiEventTypes = (await laser.aguiEvents(conversation, AgentTopic.LlmIo)).map(
      (event) => event.type
    )
  }
)

Then("AG-UI renders the chat lifecycle in order", function (this: LaserWorld) {
  assert.deepEqual(this.aguiEventTypes, [
    "TEXT_MESSAGE_START",
    "TEXT_MESSAGE_CONTENT",
    "TEXT_MESSAGE_CONTENT",
    "TEXT_MESSAGE_END"
  ])
})

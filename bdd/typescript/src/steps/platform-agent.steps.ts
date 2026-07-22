import assert from "node:assert/strict"
import { Given, Then, When } from "@cucumber/cucumber"
import {
  AgentId,
  AgentTopic,
  CorrelationId,
  RecordId,
  UnsupportedError,
  WireConversationId,
  eventEnvelope,
  parseWireAgentId,
  requiring,
  unmetRequirements
} from "@laserdata/laser-sdk"
import { eventual } from "../support/eventual.js"
import type { LaserWorld } from "../world.js"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

Given("a running data platform", function (this: LaserWorld) {
  assert.ok(this.endpoint.length > 0)
})

Given("a fresh stream", async function (this: LaserWorld) {
  await this.connect()
})

Given(
  /^a fresh stream bootstrapped with (\d+) partitions$/,
  async function (this: LaserWorld, partitions: string) {
    await this.connect()
    await this.requireLaser().bootstrap(Number(partitions))
  }
)

Given("a new conversation", function (this: LaserWorld) {
  this.newConversation()
})

When(
  /^I send agent commands "([^"]+)", "([^"]+)", "([^"]+)"$/,
  async function (this: LaserWorld, first: string, second: string, third: string) {
    for (const body of [first, second, third]) await sendCommand(this, body)
  }
)

When(
  /^I send an agent command "([^"]+)" with agent "([^"]+)" and idempotency key "([^"]+)"$/,
  async function (this: LaserWorld, body: string, agent: string, key: string) {
    await sendCommand(this, body, agent, { idempotencyKey: key })
  }
)

When(
  /^I send an agent command "([^"]+)" with agent "([^"]+)" and correlation id "([^"]+)"$/,
  async function (this: LaserWorld, body: string, agent: string, correlationId: string) {
    await sendCommand(this, body, agent, { correlationId })
  }
)

When("I start another conversation", function (this: LaserWorld) {
  this.newConversation()
})

When("I assemble the conversation", async function (this: LaserWorld) {
  this.assembled = await eventual(async () => {
    const messages = await this
      .requireLaser()
      .context(this.requireConversation())
      .fetch([AgentTopic.Commands, AgentTopic.Responses], 100)
    return messages.length > 0 ? messages : undefined
  }, "conversation assembly")
})

When(
  /^I publish an AGDX command "([^"]+)" via the typed producer$/,
  async function (this: LaserWorld, body: string) {
    await this
      .requireLaser()
      .agdx(AgentTopic.Commands, AgentId.new("producer"), this.requireConversation())
      .command(CorrelationId.fromU128(1n), encoder.encode(body))
      .send()
  }
)

Then(
  /^the assembled payloads are "([^"]+)", "([^"]+)", "([^"]+)" in order$/,
  function (this: LaserWorld, first: string, second: string, third: string) {
    assert.deepEqual(this.assembled.map((message) => decoder.decode(message.payload)), [
      first,
      second,
      third
    ])
  }
)

Then(/^the assembled message payload is "([^"]+)"$/, function (this: LaserWorld, body: string) {
  assert.deepEqual(this.assembled.map((message) => decoder.decode(message.payload)), [body])
})

Then(/^the AGDX command body is "([^"]+)"$/, function (this: LaserWorld, body: string) {
  assert.ok(this.assembled.some((message) => decoder.decode(message.envelope?.body) === body))
})

Then(/^the assembled message agent is "([^"]+)"$/, function (this: LaserWorld, agent: string) {
  assert.equal(this.assembled[0]?.provenance.agent?.asString(), agent)
})

Then(
  /^the assembled message idempotency key is "([^"]+)"$/,
  function (this: LaserWorld, key: string) {
    assert.equal(this.assembled[0]?.provenance.idempotencyKey, key)
  }
)

Then(
  /^the assembled message correlation id is "([^"]+)"$/,
  function (this: LaserWorld, correlation: string) {
    assert.equal(this.assembled[0]?.provenance.correlationId, correlation)
  }
)

Then("the assembled message belongs to the conversation", function (this: LaserWorld) {
  assert.ok(this.assembled[0]?.provenance.conversationId.equals(this.requireConversation()))
})

When("I build an agent event requiring feature bits the receiver lacks", function (this: LaserWorld) {
  const envelope = eventEnvelope(
    RecordId.fromU128(1n),
    WireConversationId.fromU128(2n),
    parseWireAgentId("sender"),
    encoder.encode("event")
  )
  this.understood = unmetRequirements(requiring(envelope, 1n << 8n), 0n) === 0n
})

When("I build a plain agent event", function (this: LaserWorld) {
  const envelope = eventEnvelope(
    RecordId.fromU128(1n),
    WireConversationId.fromU128(2n),
    parseWireAgentId("sender"),
    encoder.encode("event")
  )
  this.understood = unmetRequirements(envelope, 0n) === 0n
})

Then("the receiver rejects it as not understood", function (this: LaserWorld) {
  assert.equal(this.understood, false)
})

Then("the receiver understands it", function (this: LaserWorld) {
  assert.equal(this.understood, true)
})

Then("the call fails as unsupported", function (this: LaserWorld) {
  assert.ok(this.error instanceof UnsupportedError)
})

Then("the unified result code is unsupported", function (this: LaserWorld) {
  assert.equal(this.error?.kind, "unsupported")
})

async function sendCommand(
  world: LaserWorld,
  body: string,
  agent = "agent",
  extra: { readonly idempotencyKey?: string; readonly correlationId?: string } = {}
): Promise<void> {
  await world.requireLaser().sendAgent(AgentTopic.Commands, encoder.encode(body), {
    conversationId: world.requireConversation(),
    agent: AgentId.new(agent),
    ...extra
  })
}

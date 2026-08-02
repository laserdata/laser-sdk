import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId } from "../../src/types/ids.js"
import type { AgentCard } from "../../src/wire/agent.js"
import { Laser } from "../../src/client/laser.js"
import { KeyRegistry, SigningKey } from "../../src/signing.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

void test("given_a_published_card_and_quarantine_when_registry_refreshes_then_should_fold_incrementally", async () => {
  const streamName = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, streamName)
  try {
    await laser.stream(streamName).ensure()
    await laser.topic(AgentTopic.Registry).ensure(1)
    const planner = AgentId.new("planner")
    const operator = AgentId.new("operator")
    const card: AgentCard = {
      name: "Planner",
      capabilities: [{ skillId: "plan" }],
      ttlMicros: 60_000_000n
    }

    await laser.publishCard(planner, card)
    const registry = await laser.agentRegistry()
    assert.equal(await registry.refresh(1_000_000n), 1)
    assert.equal(registry.resolve("plan", 1_000_000n)[0]?.agent.asString(), "planner")

    await laser.quarantine(operator, planner)
    assert.equal(await registry.refresh(1_000_001n), 1)
    assert.equal(registry.isQuarantined(planner), true)
    assert.equal(registry.resolve("plan", 1_000_001n).length, 0)

    const resumed = await laser.agentRegistry()
    assert.equal(resumed.isQuarantined(planner), true)
    assert.equal(resumed.lookup(planner)?.agent.asString(), "planner")
  } finally {
    await laser.close()
  }
})

void test("given_a_verifying_registry_when_privileged_facts_arrive_then_should_require_a_valid_operator_signature", async () => {
  const streamName = `laser-ts-test-${randomUUID()}`
  const connected = await Laser.connectWithStream(CONNECTION_STRING, streamName)
  const operatorKey = SigningKey.fromBytes(new Uint8Array(32).fill(11))
  const agentKey = SigningKey.fromBytes(new Uint8Array(32).fill(12))
  const keys = new KeyRegistry()
  keys.enrollOperator("operator-principal", operatorKey.verifyingKey())
  keys.enroll("agent-principal", agentKey.verifyingKey())
  const laser = connected.withVerifier(keys)
  try {
    await laser.stream(streamName).ensure()
    await laser.topic(AgentTopic.Registry).ensure(1)
    const planner = AgentId.new("verified-planner")
    const operator = AgentId.new("operator")
    await laser.publishCard(planner, {
      name: "Verified planner",
      capabilities: [{ skillId: "plan" }],
      ttlMicros: 60_000_000n
    })
    const registry = await laser.agentRegistry()
    assert.equal(await registry.refresh(1_000_000n), 1)

    await laser.quarantineSigned(operator, planner, agentKey)
    assert.equal(await registry.refresh(1_000_001n), 0)
    assert.equal(registry.isQuarantined(planner), false)

    await laser.quarantineSigned(operator, planner, operatorKey)
    assert.equal(await registry.refresh(1_000_002n), 1)
    assert.equal(registry.isQuarantined(planner), true)

    await laser.unquarantine(operator, planner)
    assert.equal(await registry.refresh(1_000_003n), 0)
    assert.equal(registry.isQuarantined(planner), true)

    await laser.unquarantineSigned(operator, planner, operatorKey)
    assert.equal(await registry.refresh(1_000_004n), 1)
    assert.equal(registry.isQuarantined(planner), false)
  } finally {
    await connected.close()
  }
})

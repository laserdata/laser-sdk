import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Agent } from "../../src/agent/builder.js"
import { agentMessageBody } from "../../src/agent/reliable-consumer.js"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId } from "../../src/types/ids.js"
import type { AgentHandle } from "../../src/agent/builder.js"
import { routeTo } from "../../src/agent/router.js"
import { KeyRegistry, SigningKey } from "../../src/signing.js"

const CONNECTION_STRING =
  process.env["LASER_CONNECTION_STRING"] ?? "iggy://iggy:iggy@127.0.0.1:8090"

function fixed(topic: string): { readonly kind: "fixed"; readonly topic: string } {
  return { kind: "fixed", topic }
}

void test("given_a_directed_contract_when_the_agent_replies_then_should_complete_with_the_reply", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  let handle: AgentHandle | undefined
  try {
    await laser.bootstrap(1)
    const worker = AgentId.new("contract-worker")
    handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Commands)
      .respondOn(AgentTopic.Responses)
      .ackOnPickup()
      .pollInterval(5)
      .handler({
        handle(_message, context): Promise<void> {
          return context.respond(new TextEncoder().encode("completed"))
        }
      })
      .spawn(laser)
    await handle.ready()

    const outcome = await laser
      .contract(routeTo(worker))
      .from(AgentId.new("orchestrator"))
      .payload(new TextEncoder().encode("work"))
      .inboxRoute(fixed(AgentTopic.Commands))
      .expireIfNotConsumed(1_000)
      .deadline(2_000)
      .send()

    assert.equal(outcome.kind, "completed")
    assert.equal(new TextDecoder().decode(agentMessageBody(outcome.reply)), "completed")
  } finally {
    if (handle !== undefined) await handle.shutdown()
    await laser.close()
  }
})

void test("given_no_consumer_when_the_pickup_expiry_elapses_then_should_report_not_consumed", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const outcome = await laser
      .contract(routeTo(AgentId.new("absent-worker")))
      .from(AgentId.new("orchestrator"))
      .payload(new TextEncoder().encode("work"))
      .inboxRoute(fixed(AgentTopic.Commands))
      .expireIfNotConsumed(80)
      .deadline(1_000)
      .send()
    assert.deepEqual(outcome, { kind: "notConsumed" })
  } finally {
    await laser.close()
  }
})

void test("given_a_picked_up_contract_without_a_reply_when_deadline_elapses_then_should_report_timed_out", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  let handle: AgentHandle | undefined
  try {
    await laser.bootstrap(1)
    const worker = AgentId.new("silent-worker")
    handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Commands)
      .respondOn(AgentTopic.Responses)
      .ackOnPickup()
      .pollInterval(5)
      .handler({ handle: () => Promise.resolve() })
      .spawn(laser)
    await handle.ready()

    const outcome = await laser
      .contract(routeTo(worker))
      .from(AgentId.new("orchestrator"))
      .payload(new TextEncoder().encode("work"))
      .inboxRoute(fixed(AgentTopic.Commands))
      .expireIfNotConsumed(500)
      .deadline(150)
      .send()
    assert.deepEqual(outcome, { kind: "timedOut" })
  } finally {
    if (handle !== undefined) await handle.shutdown()
    await laser.close()
  }
})

void test("given_a_verified_contract_when_the_target_signs_then_should_bind_the_reply_to_its_identity", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  let handle: AgentHandle | undefined
  try {
    await laser.bootstrap(1)
    const worker = AgentId.new("signed-contract-worker")
    const key = SigningKey.fromBytes(new Uint8Array(32).fill(17))
    const registry = new KeyRegistry()
    registry.enroll(worker.asString(), key.verifyingKey())
    handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Commands)
      .respondOn(AgentTopic.Responses)
      .signingKey(key)
      .pollInterval(5)
      .handler({
        handle(_message, context): Promise<void> {
          return context.respond(new TextEncoder().encode("authenticated"))
        }
      })
      .spawn(laser)
    await handle.ready()

    const outcome = await laser
      .withVerifier(registry)
      .contract(routeTo(worker))
      .from(AgentId.new("orchestrator"))
      .payload(new TextEncoder().encode("work"))
      .inboxRoute(fixed(AgentTopic.Commands))
      .deadline(2_000)
      .send()
    assert.equal(outcome.kind, "completed")
    assert.equal(outcome.reply.verifiedPrincipal, worker.asString())
  } finally {
    if (handle !== undefined) await handle.shutdown()
    await laser.close()
  }
})

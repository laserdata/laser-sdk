import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { Agent } from "../../src/agent/builder.js"
import { AgentCtx, REQUIRE_ALL } from "../../src/agent/context.js"
import { decodeAgentMessage } from "../../src/agent/reliable-consumer.js"
import { capabilitySelector } from "../../src/agent/router.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { decodeProvenanceHeaders, type Provenance } from "../../src/provenance/provenance.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { TopicRetention } from "../../src/session.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

void test("given_an_agent_topology_when_bootstrapped_then_should_send_and_correlate_a_reply", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(2, TopicRetention.expireAfter(86_400_000))
    const commands = await laser.topic(AgentTopic.Sessions).replay()
    const provenance: Provenance = {
      conversationId: ConversationId.new(),
      agent: AgentId.new("requester")
    }

    const response = laser
      .agent(AgentId.new("requester"))
      .ask(
        AgentTopic.Sessions,
        AgentTopic.Sessions,
        new TextEncoder().encode("question"),
        { conversationId: provenance.conversationId },
        2_000
      )

    let request
    for (let attempt = 0; attempt < 40 && request === undefined; attempt += 1) {
      request = (await commands.poll())[0]
      if (request === undefined) await delay(20)
    }
    assert.ok(request !== undefined)
    const received = decodeProvenanceHeaders(request.headers)
    assert.ok(received.conversationId.equals(provenance.conversationId))
    assert.equal(received.agent?.asStr(), "requester")
    assert.ok(received.correlationId !== undefined)

    // A reply completes the request only when it is addressed to the requester.
    await laser.sendAgent(AgentTopic.Sessions, new TextEncoder().encode("answer"), {
      conversationId: received.conversationId,
      correlationId: received.correlationId,
      targetAgentId: AgentId.new("requester")
    })
    const reply = await response
    assert.equal(new TextDecoder().decode(reply.payload), "answer")
    assert.equal(reply.provenance.correlationId, received.correlationId)
  } finally {
    await laser.close()
  }
})

void test("given_attributed_provenance_when_spawning_then_should_preserve_root_and_agent", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const root = ConversationId.new()
    const parent: Provenance = {
      conversationId: ConversationId.new(),
      rootConversationId: root,
      agent: AgentId.new("orchestrator")
    }
    const child = laser.spawnSubconversation(parent, AgentId.new("worker"))
    assert.ok(child.parentConversationId?.equals(parent.conversationId))
    assert.ok(child.rootConversationId?.equals(root))
    assert.equal(child.agent?.asStr(), "worker")
    assert.ok(!child.conversationId.equals(parent.conversationId))
  } finally {
    await laser.close()
  }
})

void test("given_a_handled_message_when_responding_then_should_chain_causality_and_route_to_sender", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    const responses = await laser.topic(AgentTopic.Sessions).replay()
    await laser.sendAgent(AgentTopic.Sessions, new TextEncoder().encode("work"), {
      conversationId: ConversationId.new(),
      agent: AgentId.new("requester"),
      correlationId: "request-1"
    })
    const received = (await (await laser.topic(AgentTopic.Sessions).replay()).pollRecords())[0]
    assert.ok(received !== undefined)
    const decoded = decodeAgentMessage(received)
    assert.equal(decoded.kind, "message")

    const context = AgentCtx.create(laser, decoded.message, {
      agent: AgentId.new("worker"),
      respondOn: AgentTopic.Sessions
    })
    await context.respond(new TextEncoder().encode("complete"))

    const reply = (await responses.poll()).find(
      (record) => decodeProvenanceHeaders(record.headers).agent?.asStr() === "worker"
    )
    assert.ok(reply !== undefined)
    const provenance = decodeProvenanceHeaders(reply.headers)
    assert.equal(provenance.agent?.asStr(), "worker")
    assert.equal(provenance.targetAgentId?.asStr(), "requester")
    assert.equal(provenance.correlationId, "request-1")
    assert.deepEqual(provenance.causalParent, decoded.message.id)
  } finally {
    await laser.close()
  }
})

void test("given_two_capable_agents_when_fanning_out_then_should_gather_each_correlated_reply", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    await laser.topic(AgentTopic.Registry).ensure()
    await laser.topic("shared-inbox").ensure()
    for (const name of ["worker-a", "worker-b"]) {
      await laser.publishCard(AgentId.new(name), {
        capabilities: [{ skillId: "diagnose" }]
      })
    }
    const inbox = await laser.topic("shared-inbox").replay()
    const message = {
      provenance: {
        conversationId: ConversationId.new(),
        agent: AgentId.new("orchestrator")
      },
      payload: new TextEncoder().encode("parent"),
      id: { partitionId: 0, offset: 0n }
    }
    const context = AgentCtx.create(laser, message, {
      agent: AgentId.new("orchestrator"),
      respondOn: AgentTopic.Sessions,
      inboxRoute: { kind: "fixed", topic: "shared-inbox" }
    })
    const gathered = context.fanOut(
      capabilitySelector("diagnose", { kind: "any" }),
      new TextEncoder().encode("inspect"),
      REQUIRE_ALL,
      2_000
    )

    const requests = []
    for (let attempt = 0; attempt < 40 && requests.length < 2; attempt += 1) {
      requests.push(...(await inbox.poll()))
      if (requests.length < 2) await delay(20)
    }
    assert.equal(requests.length, 2)
    for (const request of requests) {
      const provenance = decodeProvenanceHeaders(request.headers)
      assert.ok(provenance.correlationId !== undefined)
      assert.ok(provenance.targetAgentId !== undefined)
      await laser.sendAgent(AgentTopic.Sessions, new TextEncoder().encode("ok"), {
        conversationId: provenance.conversationId,
        agent: provenance.targetAgentId,
        correlationId: provenance.correlationId,
        targetAgentId: AgentId.new("orchestrator")
      })
    }

    const result = await gathered
    assert.equal(result.ok.length, 2)
    assert.equal(result.failures.length, 0)
    assert.deepEqual(result.ok.map(([agent]) => agent.asStr()).sort(), ["worker-a", "worker-b"])
  } finally {
    await laser.close()
  }
})

void test("given_a_correlated_reply_addressed_to_another_agent_when_requesting_then_should_wait_for_the_requester_reply", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(4, TopicRetention.expireAfter(86_400_000))
    const decoder = new TextDecoder()
    await using tool = Agent.builder()
      .id(AgentId.new("tool"))
      .listenOn(AgentTopic.Sessions)
      .pollInterval(5)
      .handler({
        async handle(message, context): Promise<void> {
          if (decoder.decode(message.payload) !== "search") return
          await context.send(AgentTopic.Sessions, new TextEncoder().encode("not-yours"), {
            conversationId: message.provenance.conversationId,
            ...(message.provenance.correlationId !== undefined
              ? { correlationId: message.provenance.correlationId }
              : {}),
            agent: AgentId.new("tool"),
            targetAgentId: AgentId.new("bystander")
          })
          await context.replyOn(AgentTopic.Sessions, new TextEncoder().encode("yours"))
        }
      })
      .build()
      .spawn(laser)
    await tool.ready()
    const reply = await laser.request(
      AgentTopic.Sessions,
      AgentTopic.Sessions,
      new TextEncoder().encode("search"),
      { conversationId: ConversationId.new(), agent: AgentId.new("planner") },
      10_000
    )
    assert.equal(decoder.decode(reply.payload), "yours")
  } finally {
    await laser.close()
  }
})

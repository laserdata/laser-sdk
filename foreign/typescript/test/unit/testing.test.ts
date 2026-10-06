import assert from "node:assert/strict"
import { test } from "node:test"
import { AgentCtx } from "../../src/agent/context.js"
import { HandlerConfigError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { SigningKey } from "../../src/signing.js"
import { agentCtx, agentMessage } from "../../src/testing.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { commandEnvelope, parseAgentId } from "../../src/wire/agent.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId
} from "../../src/wire/ids.js"

void test("given_handler_test_inputs_when_helpers_build_them_then_should_preserve_identity_and_payload", () => {
  const laser = {} as Laser
  const conversationId = ConversationId.new()
  const agent = AgentId.new("test-worker")
  const message = agentMessage(new TextEncoder().encode("fixture"), {
    conversationId,
    agent
  })
  const context = agentCtx(laser, message, { agent, respondOn: "responses" })

  assert.equal(new TextDecoder().decode(message.payload), "fixture")
  assert.equal(message.id.partitionId, 0)
  assert.equal(message.id.offset, 0n)
  assert.equal(message.provenance.conversationId, conversationId)
  assert.equal(context.laser, laser)
  assert.equal(context.message, message)
  assert.equal(context.agent, agent)
  assert.equal(context.respondOn, "responses")
  assert.deepEqual(context.inboxRoute, { kind: "advertised" })
})

void test("given_a_signing_key_without_an_agent_id_when_answering_a_command_then_should_refuse_to_send_unsigned", async () => {
  let sent = 0
  const laser = {
    agdx: () => {
      sent += 1
      throw new Error("no AGDX reply may be built")
    },
    sendAgent: () => {
      sent += 1
      return Promise.resolve()
    }
  } as unknown as Laser
  const envelope = commandEnvelope(
    RecordId.fromU128(1n),
    WireConversationId.fromU128(2n),
    parseAgentId("caller"),
    CorrelationId.fromU128(3n),
    new TextEncoder().encode("do it")
  )
  const message = {
    ...agentMessage(new Uint8Array(), { conversationId: ConversationId.new() }),
    envelope
  }
  const context = AgentCtx.create(laser, message, {
    respondOn: "responses",
    signingKey: SigningKey.fromBytes(new Uint8Array(32).fill(3))
  })
  await assert.rejects(context.respond(new TextEncoder().encode("done")), HandlerConfigError)
  assert.equal(sent, 0)
})

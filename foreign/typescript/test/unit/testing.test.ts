import assert from "node:assert/strict"
import { test } from "node:test"
import type { Laser } from "../../src/client/laser.js"
import { agentContext, agentMessage } from "../../src/testing.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

void test("given_handler_test_inputs_when_helpers_build_them_then_should_preserve_identity_and_payload", () => {
  const laser = {} as Laser
  const conversationId = ConversationId.new()
  const agent = AgentId.new("test-worker")
  const message = agentMessage(new TextEncoder().encode("fixture"), {
    conversationId,
    agent
  })
  const context = agentContext(laser, message, { agent, respondOn: "responses" })

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

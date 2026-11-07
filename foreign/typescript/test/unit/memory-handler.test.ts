import assert from "node:assert/strict"
import { test } from "node:test"

import type { AgentCtx } from "../../src/agent/context.js"
import { MemoryHandler } from "../../src/agent/memory-handler.js"
import type { AgentHandler, AgentMessage } from "../../src/agent/reliable-consumer.js"
import { TransportError } from "../../src/client/errors.js"
import { MemoryHandle } from "../../src/memory/handle.js"
import { MemoryKind, type Memory } from "../../src/memory/types.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

const conversation = ConversationId.derive("handled")

const message = {
  provenance: { conversationId: conversation, agent: AgentId.new("sender") },
  payload: new TextEncoder().encode("disk pressure on node-7")
} as unknown as AgentMessage

const workerContext = { agent: AgentId.new("worker") } as unknown as AgentCtx

const inner: AgentHandler = { handle: () => Promise.resolve() }

void test("given_a_message_from_another_agent_when_auto_remembered_then_should_stamp_the_handling_agent", async () => {
  const memory = MemoryHandle.vector({ embed: () => Promise.resolve([1]) })
  await new MemoryHandler(inner, memory)
    .autoRemember(MemoryKind.Message)
    .handle(message, workerContext)
  const worker = await memory.recall(conversation).agent(AgentId.new("worker")).fetch()
  assert.equal(worker.length, 1)
  assert.equal(worker[0]?.provenance.agent?.asStr(), "worker")
  assert.deepEqual(await memory.recall(conversation).agent(AgentId.new("sender")).fetch(), [])
})

void test("given_a_failing_memory_write_when_auto_remembering_then_should_warn_and_resolve_the_turn", async () => {
  const failing = {
    remember: () => Promise.reject(new TransportError("memory down", false))
  } as unknown as Memory
  const warnings: Error[] = []
  const listener = (warning: Error) => warnings.push(warning)
  process.on("warning", listener)
  try {
    await new MemoryHandler(inner, MemoryHandle.custom(failing))
      .autoRemember(MemoryKind.Message)
      .handle(message, workerContext)
    await new Promise((resolve) => setImmediate(resolve))
  } finally {
    process.off("warning", listener)
  }
  const [warning] = warnings
  assert.equal(warnings.length, 1)
  assert.ok(warning !== undefined)
  assert.equal(warning.name, "LaserWarning")
  assert.match(warning.message, /memory down/)
})

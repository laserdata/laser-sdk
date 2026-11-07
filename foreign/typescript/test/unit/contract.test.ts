import assert from "node:assert/strict"
import { test } from "node:test"
import { createAgdx } from "../../src/agent/agdx.js"
import { ContractBuilder } from "../../src/agent/contract.js"
import { routeTo } from "../../src/agent/router.js"
import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { InvalidError } from "../../src/client/errors.js"
import {
  INTERNAL_LAYOUTS,
  INTERNAL_REPLY_HUB,
  INTERNAL_SESSION_LEASES,
  INTERNAL_TRANSPORT
} from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import type { LaserTransport, MessageWithHeaders } from "../../src/iggy/apache-iggy.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { Sessions } from "../../src/session.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { type AgentEnvelope, AgentKind, decodeAgentEnvelope } from "../../src/wire/agent.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"

function builder(): ContractBuilder {
  return ContractBuilder.create({} as Laser, routeTo(AgentId.new("worker")))
}

void test("given_fractional_expiry_when_configured_then_should_preserve_microseconds", () => {
  assert.doesNotThrow(() => builder().expireIfNotConsumed(0.125))
})

void test("given_unsafe_expiry_when_configured_then_should_raise_a_typed_error", () => {
  assert.throws(
    () => builder().expireIfNotConsumed(Number.MAX_SAFE_INTEGER),
    (error: unknown) => error instanceof InvalidError
  )
})

void test("given_a_child_contract_when_its_command_fails_to_publish_then_should_end_the_child_failed", async () => {
  const sent: AgentEnvelope[] = []
  const transport = {
    sendMessagesWithHeaders(
      _stream: string,
      _topic: string,
      messages: readonly MessageWithHeaders[]
    ) {
      for (const message of messages) {
        const envelope = decodeAgentEnvelope(
          expectMap(decodeOne(message.payload, "sent"), "sent"),
          "sent"
        )
        if (envelope.kind === AgentKind.Command) {
          return Promise.reject(new Error("the inbox is gone"))
        }
        sent.push(envelope)
      }
      return Promise.resolve({ confirmations: [] })
    }
  } as unknown as LaserTransport
  const laser = {
    defaultStream: "agents",
    [INTERNAL_LAYOUTS]: () => new Map(),
    [INTERNAL_SESSION_LEASES]: () => ({ acquire: () => ({ release: () => undefined }) }),
    [INTERNAL_TRANSPORT]: () => ({
      findSnapshotStream: () => Promise.resolve({ id: 1, createdAtMicros: 1n }),
      findSnapshotTopic: () => Promise.resolve({ id: 2, createdAtMicros: 3n, partitions: 4 }),
      findTopicPartitionCount: () => Promise.resolve(1)
    }),
    [INTERNAL_REPLY_HUB]: () =>
      Promise.resolve({
        subscribeStream: () => ({
          cancel: () => undefined,
          next: () => new Promise(() => undefined)
        })
      }),
    capabilities: () => Promise.resolve(OPEN_CAPABILITIES),
    agentRegistry: () =>
      Promise.resolve({
        refresh: () => Promise.resolve(0),
        isQuarantined: () => false,
        inboxFor: () => undefined,
        principalFor: () => undefined,
        resolve: () => []
      }),
    sessions: (): Sessions => Sessions.create(laser as unknown as Laser),
    agdx: (topic: string, source: AgentId, conversation: ConversationId) =>
      createAgdx(transport, "agents", topic, source, conversation)
  }
  const parent = ConversationId.new()
  const child = ConversationId.new()

  await assert.rejects(
    ContractBuilder.create(laser as unknown as Laser, routeTo(AgentId.new("worker")))
      .from(AgentId.new("planner"))
      .payload(new Uint8Array([1]))
      .inboxRoute({ kind: "fixed", topic: AgentTopic.Sessions })
      .conversation(child)
      .parent(parent, parent)
      .send(),
    /the inbox is gone/
  )

  assert.deepEqual(
    sent.map((envelope) => envelope.taskState),
    [
      { kind: "known", name: "Submitted" },
      { kind: "known", name: "Failed" }
    ]
  )
  assert.ok(sent.every((envelope) => envelope.conversation.toString() === child.toString()))
})

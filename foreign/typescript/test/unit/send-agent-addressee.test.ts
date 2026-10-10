import assert from "node:assert/strict"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

interface RawHeader {
  readonly key: { readonly value: string }
  readonly value: { readonly value: unknown }
}

interface RawSend {
  readonly messages: readonly { readonly headers: readonly RawHeader[] }[]
}

function capturingLaser(sends: RawSend[]) {
  const client = {
    clientProvider: () => Promise.resolve({}),
    topic: { get: () => Promise.resolve({ partitionsCount: 1 }) },
    message: {
      send: (request: RawSend) => {
        sends.push(request)
        return Promise.resolve({ confirmations: [] })
      }
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  return Laser.fromClient(client)
}

function addressee(send: RawSend | undefined): unknown {
  const headers = send?.messages[0]?.headers ?? []
  return headers.find((header) => header.key.value === "agdx.to")?.value.value
}

void test("given_an_untargeted_send_on_a_session_topic_when_published_then_should_broadcast", async () => {
  const sends: RawSend[] = []
  await using laser = (await capturingLaser(sends)).withDefaultStream("fleet")
  const provenance = { conversationId: ConversationId.new() }
  await laser.sendAgent("agent.sessions", new Uint8Array([1]), provenance)
  await laser.sendAgent("agent.control", new Uint8Array([2]), provenance)
  assert.equal(sends.length, 2)
  assert.equal(addressee(sends[0]), "*")
  assert.equal(addressee(sends[1]), "*")
})

void test("given_a_targeted_send_on_a_session_topic_when_published_then_should_keep_the_target", async () => {
  const sends: RawSend[] = []
  await using laser = (await capturingLaser(sends)).withDefaultStream("fleet")
  await laser.sendAgent("agent.sessions", new Uint8Array([1]), {
    conversationId: ConversationId.new(),
    targetAgentId: AgentId.new("planner")
  })
  assert.equal(addressee(sends[0]), "planner")
})

void test("given_an_untargeted_send_on_another_topic_when_published_then_should_omit_the_addressee", async () => {
  const sends: RawSend[] = []
  await using laser = (await capturingLaser(sends)).withDefaultStream("fleet")
  await laser.sendAgent("commands", new Uint8Array([1]), { conversationId: ConversationId.new() })
  assert.equal(addressee(sends[0]), undefined)
})

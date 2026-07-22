import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { AgentKind, decodeAgentEnvelope } from "../../src/wire/agent.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import { ContentType } from "../../src/wire/content.js"
import {
  AGENT_VERSION,
  CONTENT_TYPE,
  CONVERSATION_ID,
  TARGET_AGENT_ID
} from "../../src/wire/headers.js"
import { CorrelationId } from "../../src/wire/ids.js"
import { Laser } from "../../src/client/laser.js"

const CONNECTION_STRING =
  process.env["LASER_CONNECTION_STRING"] ?? "iggy://iggy:iggy@127.0.0.1:8090"

void test("given_agdx_messages_when_sent_through_iggy_then_should_preserve_headers_and_chunk_batch_order", async () => {
  const streamName = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, streamName)
  try {
    await laser.stream(streamName).ensure()
    const topic = laser.topic(AgentTopic.Commands)
    await topic.ensure(3)

    const conversation = ConversationId.new()
    const source = AgentId.new("source-agent")
    const target = AgentId.new("target-agent")
    const correlation = CorrelationId.fromU128(5n)
    const agdx = laser.agdx(AgentTopic.Commands, source, conversation)

    await agdx
      .command(correlation, new TextEncoder().encode("request"))
      .withTarget(target)
      .contentType(ContentType.Json)
      .send()
    const chunks = agdx.stream(correlation, "chat").withTarget(target).buffered(10, 10_000)
    await chunks.write(new TextEncoder().encode("one"))
    await chunks.write(new TextEncoder().encode("two"))
    await chunks.finish("stop")

    const cursor = await topic.replay()
    const messages = await cursor.poll()
    assert.equal(messages.length, 4)
    const envelopes = messages.map((message) => {
      assert.deepEqual(message.headers.get(AGENT_VERSION), { kind: "uint32", value: 1 })
      assert.equal(message.headers.get(CONVERSATION_ID)?.kind, "uint128")
      assert.deepEqual(message.headers.get(TARGET_AGENT_ID), {
        kind: "string",
        value: "target-agent"
      })
      const context = "AGDX integration message"
      return decodeAgentEnvelope(expectMap(decodeOne(message.payload, context), context), context)
    })

    assert.equal(envelopes[0]?.kind, AgentKind.Command)
    assert.deepEqual(messages[0]?.headers.get(CONTENT_TYPE), { kind: "uint8", value: 1 })
    assert.deepEqual(
      envelopes.slice(1).map((envelope) => envelope.sequence),
      [0n, 1n, 2n]
    )
    assert.equal(envelopes[3]?.last, true)
    assert.ok(
      envelopes.every((envelope) => envelope.conversation.toString() === conversation.toString())
    )
    const events = await laser.reassembleChannel(conversation, AgentTopic.Commands, chunks.channel)
    assert.deepEqual(
      events.map((event) => event.kind),
      ["body", "body", "finished"]
    )
  } finally {
    await laser.close()
  }
})

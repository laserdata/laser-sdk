import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import type { IggyHeaderValue } from "../../src/iggy/apache-iggy.js"
import { ApacheIggyTransport } from "../../src/iggy/apache-iggy.js"
import {
  decodeProvenanceHeaders,
  encodeProvenanceHeaders,
  type Provenance
} from "../../src/provenance/provenance.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

const CONNECTION_STRING =
  process.env["LASER_CONNECTION_STRING"] ?? "iggy://iggy:iggy@127.0.0.1:8090"

void test("given_a_message_sent_with_headers_when_polled_back_then_should_carry_every_header_kind", async () => {
  const transport = await ApacheIggyTransport.connect(CONNECTION_STRING)
  try {
    const streamName = `laser-ts-test-${randomUUID()}`
    await transport.ensureStream(streamName)
    await transport.ensureTopic(streamName, "events", 1)

    const headers = new Map<string, IggyHeaderValue>([
      ["agdx.string", { kind: "string", value: "hello" }],
      ["agdx.bool", { kind: "bool", value: true }],
      ["agdx.uint32", { kind: "uint32", value: 42 }],
      ["agdx.int64", { kind: "int64", value: -7n }]
    ])
    await transport.sendMessageWithHeaders(
      streamName,
      "events",
      new TextEncoder().encode("hello from provenance"),
      headers,
      "partition-key"
    )

    const polled = await transport.pollMessages(
      streamName,
      "events",
      { kind: "single", partitionId: 0 },
      { kind: "first" },
      10,
      false
    )

    assert.equal(polled.length, 1)
    const message = polled[0]
    assert.ok(message !== undefined)
    assert.equal(new TextDecoder().decode(message.payload), "hello from provenance")
    assert.deepEqual(message.headers.get("agdx.string"), { kind: "string", value: "hello" })
    assert.deepEqual(message.headers.get("agdx.bool"), { kind: "bool", value: true })
    assert.deepEqual(message.headers.get("agdx.uint32"), { kind: "uint32", value: 42 })
    assert.deepEqual(message.headers.get("agdx.int64"), { kind: "int64", value: -7n })
  } finally {
    await transport.close()
  }
})

void test("given_a_provenance_when_sent_and_polled_back_then_should_decode_to_the_same_provenance", async () => {
  const transport = await ApacheIggyTransport.connect(CONNECTION_STRING)
  try {
    const streamName = `laser-ts-test-${randomUUID()}`
    await transport.ensureStream(streamName)
    await transport.ensureTopic(streamName, "events", 1)

    const conversationId = ConversationId.new()
    const provenance: Provenance = {
      conversationId,
      agent: AgentId.new("planner"),
      idempotencyKey: "key-1",
      fenceToken: 3n
    }
    const headers = encodeProvenanceHeaders(provenance)
    await transport.sendMessageWithHeaders(
      streamName,
      "events",
      new TextEncoder().encode("payload"),
      headers,
      conversationId.toString()
    )

    const polled = await transport.pollMessages(
      streamName,
      "events",
      { kind: "single", partitionId: 0 },
      { kind: "first" },
      10,
      false
    )
    assert.equal(polled.length, 1)
    const message = polled[0]
    assert.ok(message !== undefined)
    const decoded = decodeProvenanceHeaders(message.headers)
    assert.ok(decoded.conversationId.equals(conversationId))
    assert.equal(decoded.agent?.asString(), "planner")
    assert.equal(decoded.idempotencyKey, "key-1")
    assert.equal(decoded.fenceToken, 3n)
  } finally {
    await transport.close()
  }
})

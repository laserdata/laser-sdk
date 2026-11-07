import assert from "node:assert/strict"
import { test } from "node:test"
import { createAgdx } from "../../src/agent/agdx.js"
import { PolicyBlockedError } from "../../src/client/errors.js"
import type { LaserTransport, MessageWithHeaders } from "../../src/iggy/apache-iggy.js"
import { SigningKey } from "../../src/signing.js"
import { ProducerMessage } from "../../src/stream/producer.js"
import { Topic } from "../../src/stream/topic.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { decodeAgentEnvelope, eventEnvelope, parseAgentId } from "../../src/wire/agent.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import { RecordId, ConversationId as WireConversationId } from "../../src/wire/ids.js"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

void test("given_a_claim_checked_publish_when_governed_then_should_govern_the_body_before_it_is_stored", async () => {
  const sent = recordingTransport()
  const seen: string[] = []
  const topic = Topic.create(sent.transport, "orders", "events", (_stream, _topic, payload) => {
    seen.push(decoder.decode(payload))
    return Promise.resolve(encoder.encode("redacted"))
  })
  const stored: string[] = []
  await topic
    .publish()
    .payload(encoder.encode("card 4111"))
    .claimCheck(
      {
        put: (payload) => {
          stored.push(decoder.decode(payload))
          return Promise.resolve("blob-1")
        },
        get: () => Promise.reject(new Error("unused"))
      },
      0
    )
    .send()
  assert.deepEqual(seen, ["card 4111"])
  assert.deepEqual(stored, ["redacted"])
  assert.equal(sent.payloads.length, 1)
  assert.notEqual(decoder.decode(sent.payloads[0]), "redacted")
})

void test("given_a_governed_topic_when_its_producer_sends_then_should_govern_every_record", async () => {
  const sent = recordingTransport()
  const seen: string[] = []
  const topic = Topic.create(sent.transport, "orders", "events", (_stream, _topic, payload) => {
    const text = decoder.decode(payload)
    seen.push(text)
    return text === "blocked"
      ? Promise.reject(new PolicyBlockedError("the governor blocked this action"))
      : Promise.resolve(encoder.encode(`${text}!`))
  })
  const producer = topic.producer({ createStream: false, createTopic: false })
  await producer.send(encoder.encode("one"))
  await producer.sendBatch([encoder.encode("two"), new ProducerMessage(encoder.encode("three"))])
  await assert.rejects(producer.send(encoder.encode("blocked")), PolicyBlockedError)
  assert.deepEqual(seen, ["one", "two", "three", "blocked"])
  assert.deepEqual(
    sent.payloads.map((payload) => decoder.decode(payload)),
    ["one!", "two!", "three!"]
  )
})

void test("given_a_prebuilt_envelope_when_published_then_should_govern_it_and_refuse_to_rewrite_a_signed_one", async () => {
  const sent = recordingTransport()
  const kinds: boolean[] = []
  const conversation = ConversationId.new()
  const agdx = createAgdx(
    sent.transport,
    "orders",
    "agent.sessions",
    AgentId.new("planner"),
    conversation,
    undefined,
    (_envelope, signed) => {
      kinds.push(signed)
      return Promise.resolve(encoder.encode("rewritten"))
    }
  )
  const envelope = eventEnvelope(
    RecordId.fromU128(7n),
    WireConversationId.parse(conversation.toString()),
    parseAgentId("planner"),
    encoder.encode("original")
  )
  await agdx.publishEnvelope(envelope)
  const published = sent.messages[0]
  assert.ok(published !== undefined)
  const context = "published envelope"
  const decoded = decodeAgentEnvelope(
    expectMap(decodeOne(published.payload, context), context),
    context
  )
  assert.equal(decoder.decode(decoded.body), "rewritten")
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(9))
  const signed = { ...envelope, signature: key.sign(envelope) }
  await assert.rejects(agdx.publishEnvelope(signed), PolicyBlockedError)
  assert.deepEqual(kinds, [false, true])
  assert.equal(sent.messages.length, 1)
})

function recordingTransport(): {
  readonly transport: LaserTransport
  readonly payloads: Uint8Array[]
  readonly messages: MessageWithHeaders[]
} {
  const payloads: Uint8Array[] = []
  const messages: MessageWithHeaders[] = []
  const confirm = (count: number) =>
    Promise.resolve({
      confirmations: Array.from({ length: count }, (_, index) => ({
        partitionId: 0,
        baseOffset: BigInt(index),
        count: 1
      }))
    })
  const transport = {
    sendMessages: (_stream: string, _topic: string, batch: readonly Uint8Array[]) => {
      payloads.push(...batch)
      return confirm(batch.length)
    },
    sendMessageWithHeaders: (_stream: string, _topic: string, payload: Uint8Array) => {
      payloads.push(payload)
      return confirm(1)
    },
    sendMessagesWithHeaders: (
      _stream: string,
      _topic: string,
      batch: readonly MessageWithHeaders[]
    ) => {
      messages.push(...batch)
      payloads.push(...batch.map((message) => message.payload))
      return confirm(batch.length)
    }
  } as unknown as LaserTransport
  return { transport, payloads, messages }
}

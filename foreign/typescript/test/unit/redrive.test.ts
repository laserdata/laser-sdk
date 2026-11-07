import assert from "node:assert/strict"
import { test } from "node:test"
import { createAgdx } from "../../src/agent/agdx.js"
import { InvalidError } from "../../src/client/errors.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import type {
  IggyClient,
  LaserTransport,
  MessageWithHeaders,
  PolledMessage
} from "../../src/iggy/apache-iggy.js"
import { SigningKey } from "../../src/signing.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { decodeAgentEnvelope, parseIdempotencyKey } from "../../src/wire/agent.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import { CorrelationId } from "../../src/wire/ids.js"

const encoder = new TextEncoder()

void test("given_an_agdx_dead_letter_when_redriven_then_should_rekey_the_envelope_body", async () => {
  const original = await recorded((send) =>
    send.withIdempotencyKey(parseIdempotencyKey("attempt-1"))
  )
  const { laser, sent } = await redriveLaser(original)
  await laser.redriveDeadLetter(capsule())
  const copy = sent[0]
  assert.ok(copy !== undefined)
  const before = envelopeOf(original.payload)
  const after = envelopeOf(copy)
  assert.equal(after.idempotencyKey, "attempt-1/redrive/0-7")
  assert.notEqual(after.record?.toString(), before.record?.toString())
  assert.deepEqual(after.body, before.body)
})

void test("given_an_undecodable_agdx_dead_letter_when_redriven_then_should_republish_its_bytes_unchanged", async () => {
  const garbage = encoder.encode("not cbor at all, definitely")
  const original = { ...(await recorded((send) => send)), payload: garbage }
  const { laser, sent } = await redriveLaser(original)
  await laser.redriveDeadLetter(capsule())
  assert.deepEqual(sent, [garbage])
})

void test("given_a_signed_or_expired_agdx_dead_letter_when_redriven_then_should_refuse_it", async () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(3))
  for (const original of [
    await recorded((send) => send.signedBy(key)),
    await recorded((send) => send.withDeadlineAtMicros(1n))
  ]) {
    const { laser, sent } = await redriveLaser(original)
    await assert.rejects(laser.redriveDeadLetter(capsule()), InvalidError)
    assert.equal(sent.length, 0)
  }
})

void test("given_a_capsule_naming_another_stream_when_redriven_then_should_refuse_it", async () => {
  const original = await recorded((send) => send)
  const { laser, sent } = await redriveLaser(original, "payments")
  await assert.rejects(laser.redriveDeadLetter(capsule()), InvalidError)
  assert.equal(sent.length, 0)
})

function capsule() {
  return {
    source: { streamId: 1, topicId: 2, partitionId: 0, offset: 7n },
    reason: { kind: "known", name: "Rejected" } as const,
    attempts: 1,
    payload: new Uint8Array()
  }
}

function envelopeOf(payload: Uint8Array) {
  const context = "redriven envelope"
  return decodeAgentEnvelope(expectMap(decodeOne(payload, context), context), context)
}

// One AGDX command as the log stores it, refined by `refine`.
async function recorded(
  refine: (
    send: ReturnType<ReturnType<typeof createAgdx>["command"]>
  ) => ReturnType<ReturnType<typeof createAgdx>["command"]>
): Promise<PolledMessage> {
  const captured: MessageWithHeaders[] = []
  const transport = {
    sendMessagesWithHeaders: (
      _stream: string,
      _topic: string,
      messages: readonly MessageWithHeaders[]
    ) => {
      captured.push(...messages)
      return Promise.resolve({ confirmations: [] })
    }
  } as unknown as LaserTransport
  const agdx = createAgdx(
    transport,
    "agents",
    "agent.sessions",
    AgentId.new("planner"),
    ConversationId.new()
  )
  await refine(agdx.command(CorrelationId.fromU128(5n), encoder.encode("work"))).send()
  const message = captured[0]
  assert.ok(message !== undefined)
  return {
    partitionId: 0,
    offset: 7n,
    payload: message.payload,
    headers: message.headers,
    timestampMicros: 1n
  }
}

async function redriveLaser(
  original: PolledMessage,
  sourceStream = "agents"
): Promise<{ readonly laser: Laser; readonly sent: Uint8Array[] }> {
  const client = { clientProvider: () => Promise.resolve({}) } as unknown as IggyClient
  const laser = (await Laser.fromClient(client)).withDefaultStream("agents")
  const transport = laser[INTERNAL_TRANSPORT]()
  const sent: Uint8Array[] = []
  transport.resolveStreamTopicNames = () =>
    Promise.resolve({ stream: sourceStream, topic: "agent.sessions" })
  transport.pollMessages = () => Promise.resolve([original])
  transport.sendMessageWithHeaders = (_stream, _topic, payload) => {
    sent.push(payload)
    return Promise.resolve({ confirmations: [] })
  }
  return { laser, sent }
}

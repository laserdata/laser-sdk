import assert from "node:assert/strict"
import { test } from "node:test"
import { createAgdx, MAX_CHUNK_BODY_BYTES, type Agdx } from "../../src/agent/agdx.js"
import { InvalidError, RejectedError } from "../../src/client/errors.js"
import { KeyRegistry, SigningKey } from "../../src/signing.js"
import type {
  ConsumerTarget,
  LaserTransport,
  MessageWithHeaders,
  PolledMessage
} from "../../src/iggy/apache-iggy.js"
import type { UlidSource } from "../../src/runtime/ulid.js"
import { AgentId, ConversationId as SdkConversationId } from "../../src/types/ids.js"
import {
  AgentErrorCodeName,
  AgentKind,
  decodeAgentEnvelope,
  encodeAgentEnvelope,
  errorEnvelope,
  parseAgentId,
  parseIdempotencyKey,
  responseEnvelope
} from "../../src/wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { ContentType } from "../../src/wire/content.js"
import {
  AGENT_VERSION,
  CONTENT_TYPE,
  CONVERSATION_ID,
  TARGET_AGENT_ID
} from "../../src/wire/headers.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId
} from "../../src/wire/ids.js"
import type { PollingStrategy } from "../../src/stream/polling-strategy.js"

interface SentBatch {
  readonly stream: string
  readonly topic: string
  readonly messages: readonly MessageWithHeaders[]
  readonly partitionKey?: string | Uint8Array
}

class FixedUlids implements UlidSource {
  private next = 1n
  private current = 0n

  nowMilliseconds(): number {
    this.current = 0x0190_3c1f_aa00_0000_0000_0000_0000_0000n | this.next
    return Number(this.current >> 80n)
  }

  fillRandom(bytes: Uint8Array): void {
    let value = this.current & ((1n << 80n) - 1n)
    for (let index = bytes.length - 1; index >= 0; index -= 1) {
      bytes[index] = Number(value & 0xffn)
      value >>= 8n
    }
    this.next += 1n
  }
}

class FakeTransport implements LaserTransport {
  readonly kind = "apache-iggy" as const
  readonly batches: SentBatch[] = []
  readonly replies: PolledMessage[] = []
  onSend: ((messages: readonly MessageWithHeaders[]) => void) | undefined

  get iggyClient(): never {
    throw new Error("unused")
  }

  sendManaged(): Promise<Uint8Array> {
    return Promise.reject(new Error("unused"))
  }

  ensureStream(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  ensureTopic(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  getTopicPartitionCount(): Promise<number> {
    return Promise.resolve(1)
  }

  findTopicPartitionCount(): Promise<number | undefined> {
    return Promise.resolve(1)
  }

  sendMessages(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  sendMessageWithHeaders(
    stream: string,
    topic: string,
    payload: Uint8Array,
    headers: MessageWithHeaders["headers"],
    partitionKey?: string | Uint8Array
  ): Promise<void> {
    return this.sendMessagesWithHeaders(stream, topic, [{ payload, headers }], partitionKey)
  }

  sendMessagesWithHeaders(
    stream: string,
    topic: string,
    messages: readonly MessageWithHeaders[],
    partitionKey?: string | Uint8Array
  ): Promise<void> {
    this.batches.push({
      stream,
      topic,
      messages,
      ...(partitionKey !== undefined ? { partitionKey } : {})
    })
    this.onSend?.(messages)
    return Promise.resolve()
  }

  pollMessages(
    _stream: string,
    _topic: string,
    _target: ConsumerTarget,
    strategy: PollingStrategy
  ): Promise<readonly PolledMessage[]> {
    if (strategy.kind === "last") return Promise.resolve([])
    return Promise.resolve(this.replies.splice(0))
  }

  storeOffset(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  joinConsumerGroup(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  leaveConsumerGroup(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  close(): Promise<void> {
    return Promise.resolve()
  }
}

function newAgdx(transport: FakeTransport): Agdx {
  const conversation = WireConversationId.fromU128(0x0190_3c1f_aa00_0000_0000_0000_0000_0002n)
  return createAgdx(
    transport,
    "agents",
    "agent.commands",
    AgentId.new("source-agent"),
    SdkConversationId.parse(conversation.toString()),
    new FixedUlids()
  )
}

function envelopeOf(message: MessageWithHeaders) {
  const context = "sent envelope"
  return decodeAgentEnvelope(expectMap(decodeOne(message.payload, context), context), context)
}

void test("given_a_refined_command_when_sent_then_should_stamp_typed_headers_and_valid_envelope", async () => {
  const transport = new FakeTransport()
  const agdx = newAgdx(transport)
  const correlation = CorrelationId.fromU128(5n)
  const record = await agdx
    .command(correlation, new TextEncoder().encode("request"))
    .withTarget(AgentId.new("target-agent"))
    .withIdempotencyKey(parseIdempotencyKey("attempt-1"))
    .withDeadlineMicros(1_717_171_777_000_000n)
    .withOperation("chat")
    .withMetadata("priority", { kind: "string", value: "high" })
    .contentType(ContentType.Json)
    .send()

  assert.equal(record?.toString(), "01J0Y1ZAG00000000000000001")
  assert.equal(transport.batches.length, 1)
  const batch = transport.batches[0]
  assert.ok(batch !== undefined)
  assert.equal(batch.stream, "agents")
  assert.equal(batch.topic, "agent.commands")
  assert.equal(batch.partitionKey, "01J0Y1ZAG00000000000000002")
  const sent = batch.messages[0]
  assert.ok(sent !== undefined)
  assert.deepEqual(sent.headers.get(AGENT_VERSION), { kind: "uint32", value: 1 })
  assert.deepEqual(sent.headers.get(CONTENT_TYPE), { kind: "uint8", value: 1 })
  assert.equal(sent.headers.get(CONVERSATION_ID)?.kind, "uint128")
  assert.deepEqual(sent.headers.get(TARGET_AGENT_ID), {
    kind: "string",
    value: "target-agent"
  })
  const envelope = envelopeOf(sent)
  assert.equal(envelope.kind, AgentKind.Command)
  assert.equal(envelope.correlation?.asU128(), 5n)
  assert.equal(envelope.idempotencyKey, "attempt-1")
  assert.equal(envelope.metadata?.get("priority")?.kind, "string")
})

void test("given_a_buffered_chunk_stream_when_finished_then_should_append_chunks_and_terminal_once", async () => {
  const transport = new FakeTransport()
  const stream = newAgdx(transport)
    .stream(CorrelationId.fromU128(7n), "chat")
    .withTarget(AgentId.new("target-agent"))
    .buffered(10, 10_000)

  await stream.write(new TextEncoder().encode("one"))
  await stream.write(new TextEncoder().encode("two"))
  assert.equal(transport.batches.length, 0)
  await stream.finish("stop", { inputTokens: 2n, outputTokens: 3n })

  assert.equal(transport.batches.length, 1)
  const batch = transport.batches[0]
  assert.ok(batch !== undefined)
  assert.equal(batch.messages.length, 3)
  const envelopes = batch.messages.map(envelopeOf)
  assert.deepEqual(
    envelopes.map((envelope) => envelope.sequence),
    [0n, 1n, 2n]
  )
  const [first, second, terminal] = envelopes
  assert.ok(first !== undefined)
  assert.ok(second !== undefined)
  assert.ok(terminal !== undefined)
  assert.equal(first.operation, "chat")
  assert.equal(second.operation, undefined)
  assert.equal(terminal.last, true)
  assert.equal(terminal.finishReason, "stop")
  assert.equal(terminal.usage?.outputTokens, 3n)
  assert.ok(envelopes.every((envelope) => envelope.channel?.equals(stream.channel) === true))
  await assert.rejects(stream.write(new Uint8Array()), InvalidError)
})

void test("given_an_oversized_chunk_when_written_then_should_reject_before_transport", async () => {
  const transport = new FakeTransport()
  const stream = newAgdx(transport).stream(CorrelationId.fromU128(7n), "chat")
  await assert.rejects(stream.write(new Uint8Array(MAX_CHUNK_BODY_BYTES + 1)), InvalidError)
  assert.equal(transport.batches.length, 0)
})

void test("given_a_send_builder_when_sent_twice_then_should_reject_the_second_effect", async () => {
  const send = newAgdx(new FakeTransport()).emit(new Uint8Array([1]))
  await send.send()
  await assert.rejects(send.send(), InvalidError)
})

void test("given_a_governed_signed_command_when_sent_then_should_sign_the_modified_body_and_context", async () => {
  const transport = new FakeTransport()
  let willSign = false
  const conversation = WireConversationId.fromU128(2n)
  const agdx = createAgdx(
    transport,
    "agents",
    "agent.commands",
    AgentId.new("source-agent"),
    SdkConversationId.parse(conversation.toString()),
    new FixedUlids(),
    (_envelope, signing) => {
      willSign = signing
      return Promise.resolve(new TextEncoder().encode("modified"))
    }
  )
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(7))
  await agdx
    .command(CorrelationId.fromU128(5n), new TextEncoder().encode("original"))
    .contentType(ContentType.Json)
    .signedBy(key)
    .send()
  const sent = transport.batches[0]?.messages[0]
  assert.ok(sent !== undefined)
  const envelope = envelopeOf(sent)
  assert.equal(willSign, true)
  assert.equal(new TextDecoder().decode(envelope.body), "modified")
  assert.deepEqual(envelope.signature?.context, { contentType: 1, agentVersion: 1 })
  const registry = new KeyRegistry()
  registry.enroll("source-principal", key.verifyingKey())
  assert.equal(registry.verify(envelope), "source-principal")
})

void test("given_a_correlated_input_response_when_requested_then_should_return_the_response_body", async () => {
  const transport = new FakeTransport()
  transport.onSend = (messages) => {
    const command = messages[0]
    assert.ok(command !== undefined)
    const correlation = envelopeOf(command).correlation
    assert.ok(correlation !== undefined)
    const reply = responseEnvelope(
      RecordId.fromU128(100n),
      WireConversationId.fromU128(2n),
      parseAgentId("human"),
      correlation,
      new TextEncoder().encode("approved")
    )
    transport.replies.push({
      payload: encodeNamed(encodeAgentEnvelope(reply)),
      partitionId: 0,
      offset: 0n,
      headers: new Map()
    })
  }

  const body = await newAgdx(transport).requestInput(
    "agent.human_input",
    new TextEncoder().encode("approve?"),
    1_000
  )
  assert.equal(new TextDecoder().decode(body), "approved")
})

void test("given_a_correlated_input_error_when_requested_then_should_surface_the_rejection", async () => {
  const transport = new FakeTransport()
  transport.onSend = (messages) => {
    const command = messages[0]
    assert.ok(command !== undefined)
    const correlation = envelopeOf(command).correlation
    assert.ok(correlation !== undefined)
    const reply = errorEnvelope(
      RecordId.fromU128(101n),
      WireConversationId.fromU128(2n),
      parseAgentId("human"),
      correlation,
      encodeNamed(
        new Map<string, unknown>([
          ["code", AgentErrorCodeName.InvalidRequest],
          ["message", "not approved"]
        ])
      )
    )
    transport.replies.push({
      payload: encodeNamed(encodeAgentEnvelope(reply)),
      partitionId: 0,
      offset: 0n,
      headers: new Map()
    })
  }

  await assert.rejects(
    newAgdx(transport).requestInput(
      "agent.human_input",
      new TextEncoder().encode("approve?"),
      1_000
    ),
    (error: unknown) => error instanceof RejectedError && error.message === "not approved"
  )
})

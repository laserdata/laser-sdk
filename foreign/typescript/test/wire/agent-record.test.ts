import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  commandEnvelope,
  encodeAgentEnvelope,
  parseAgentId,
  parseIdempotencyKey,
  withDeadlineMicros,
  withIdempotencyKey,
  withMetadata,
  withOperation,
  withTarget
} from "../../src/wire/agent.js"
import {
  canonicalAgentRecord,
  decodeCanonicalAgentEnvelope,
  decodeCanonicalAgentRecord,
  encodeCanonicalAgentRecord
} from "../../src/wire/agent-record.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { ContentType } from "../../src/wire/content.js"
import { ConversationId, CorrelationId, RecordId } from "../../src/wire/ids.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

function canonicalCommand() {
  const record = RecordId.fromU128(0x0190_3c1f_aa00_0000_0000_0000_0000_0001n)
  const conversation = ConversationId.fromU128(0x0190_3c1f_aa00_0000_0000_0000_0000_0002n)
  const correlation = CorrelationId.fromU128(0x0190_3c1f_aa00_0000_0000_0000_0000_0005n)
  let envelope = commandEnvelope(
    record,
    conversation,
    parseAgentId("source-agent"),
    correlation,
    new TextEncoder().encode('{"ask":"plan the trip"}')
  )
  envelope = withTarget(envelope, parseAgentId("target-agent"))
  envelope = withIdempotencyKey(envelope, parseIdempotencyKey("order-123-attempt-2"))
  envelope = withDeadlineMicros(envelope, 1_717_171_777_000_000n)
  envelope = withOperation(envelope, "chat")
  return withMetadata(envelope, "priority", { kind: "string", value: "high" })
}

void test("given_the_canonical_agent_record_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("agent_record.bin")
  const record = decodeCanonicalAgentRecord(
    expectMap(decodeOne(bytes, "agent_record.bin"), "agent_record.bin"),
    "agent_record.bin"
  )
  const envelope = decodeCanonicalAgentEnvelope(record, "agent_record.bin")

  assert.equal(record.partitionKey, envelope.conversation.toString())
  assert.equal(envelope.target, "target-agent")
  assert.deepEqual(Buffer.from(encodeNamed(encodeCanonicalAgentRecord(record))), Buffer.from(bytes))
})

void test("given_the_canonical_command_when_lowered_then_should_match_the_record_fixture", async () => {
  const fixture = await readFixture("agent_record.bin")
  const envelope = canonicalCommand()
  const record = canonicalAgentRecord(envelope, ContentType.Json)

  assert.deepEqual(
    Buffer.from(record.payload),
    Buffer.from(encodeNamed(encodeAgentEnvelope(envelope)))
  )
  assert.deepEqual(
    Buffer.from(encodeNamed(encodeCanonicalAgentRecord(record))),
    Buffer.from(fixture)
  )
})

void test("given_an_agent_envelope_when_canonicalized_then_should_pin_header_types_and_bytes", () => {
  const envelope = canonicalCommand()
  const headers = canonicalAgentRecord(envelope, ContentType.Json).headers
  const agentVersion = headers.get("agdx.av")
  const contentType = headers.get("agdx.ct")
  const conversation = headers.get("gen_ai.conversation.id")
  const target = headers.get("agdx.to")

  assert.ok(agentVersion !== undefined)
  assert.ok(contentType !== undefined)
  assert.ok(conversation !== undefined)
  assert.ok(target !== undefined)
  assert.equal(agentVersion.kind, "u32")
  assert.deepEqual(agentVersion.bytes, Uint8Array.of(1, 0, 0, 0))
  assert.equal(contentType.kind, "u8")
  assert.deepEqual(contentType.bytes, Uint8Array.of(1))
  assert.equal(conversation.kind, "uint128")
  assert.deepEqual(
    conversation.bytes,
    Uint8Array.of(2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 170, 31, 60, 144, 1)
  )
  assert.equal(target.kind, "string")
  assert.equal(new TextDecoder().decode(target.bytes), "target-agent")
})

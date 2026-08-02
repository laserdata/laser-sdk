import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { test } from "node:test"
import {
  KeyKind,
  KeyRecord,
  KeyRegistry,
  SignatureError,
  SigningKey,
  signCardValue,
  verifyCard,
  verifyDelegation
} from "../../src/index.js"
import {
  METADATA_DELEGATED_BY,
  commandEnvelope,
  parseAgentId,
  withMetadata,
  withSignature,
  type AgentEnvelope,
  type SignatureContext
} from "../../src/wire/agent.js"
import { ConversationId, CorrelationId, RecordId } from "../../src/wire/ids.js"

function envelope(): AgentEnvelope {
  return commandEnvelope(
    RecordId.fromU128(1n),
    ConversationId.fromU128(2n),
    parseAgentId("planner"),
    CorrelationId.fromU128(3n),
    new TextEncoder().encode("{}")
  )
}

void test("given_a_signed_envelope_when_verified_then_should_return_the_enrolled_principal", () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(7))
  const registry = new KeyRegistry()
  registry.enroll("user-42", key.verifyingKey())
  const message = envelope()
  const signed = withSignature(message, key.sign(message))
  assert.equal(registry.verify(signed), "user-42")
  assert.equal(key.keyId().byteLength, 8)
})

void test("given_tampering_context_and_lifecycle_failures_when_verified_then_should_reject", () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(21))
  const context: SignatureContext = { contentType: 1, agentVersion: 1 }
  const message = envelope()
  const signature = key.signWithContext(message, context)
  const registry = new KeyRegistry()
  registry.enrollRecord(KeyRecord.agent("user-1", key.verifyingKey()).validWindow(100n, 200n))
  assert.equal(registry.verifyAt(withSignature(message, signature), 150n).principal, "user-1")
  assert.throws(() => registry.verifyAt(withSignature(message, signature), 99n), SignatureError)
  assert.throws(
    () =>
      registry.verify(
        withSignature(message, { ...signature, context: { contentType: 3, agentVersion: 1 } })
      ),
    SignatureError
  )
  const revoked = new KeyRegistry()
  revoked.enrollRecord(KeyRecord.agent("user-1", key.verifyingKey()).revoke())
  assert.throws(() => revoked.verify(withSignature(message, signature)), /revoked/)
})

void test("given_operator_and_agent_records_when_verified_then_should_preserve_key_kind", () => {
  const operator = SigningKey.fromBytes(new Uint8Array(32).fill(11))
  const agent = SigningKey.fromBytes(new Uint8Array(32).fill(12))
  const registry = new KeyRegistry()
  registry.enrollOperator("operator-1", operator.verifyingKey())
  registry.enroll("agent-1", agent.verifyingKey())
  const message = envelope()
  assert.equal(
    registry.verifyAt(withSignature(message, operator.sign(message)), 1n).kind,
    KeyKind.Operator
  )
  assert.equal(
    registry.verifyAt(withSignature(message, agent.sign(message)), 1n).kind,
    KeyKind.Agent
  )
})

void test("given_a_signed_delegation_when_verified_then_should_bind_signer_and_user", () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(9))
  const registry = new KeyRegistry()
  registry.enroll("agent-a", key.verifyingKey())
  const delegated = withMetadata(envelope(), METADATA_DELEGATED_BY, {
    kind: "string",
    value: "user-7"
  })
  const signed = withSignature(delegated, key.sign(delegated))
  assert.deepEqual(verifyDelegation(registry, signed), ["agent-a", "user-7"])
  const forged = withMetadata(signed, METADATA_DELEGATED_BY, {
    kind: "string",
    value: "user-999"
  })
  assert.throws(() => verifyDelegation(registry, forged), SignatureError)
})

void test("given_a_signed_card_when_verified_then_should_reject_tampering_and_ignore_its_signature_slot", () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(7))
  const card = {
    name: "a2a-bridge",
    version: "0.0.1",
    supportedInterfaces: [{ url: "/", protocolBinding: "JSONRPC", protocolVersion: "1.0" }],
    skills: []
  }
  const signature = signCardValue(key, card)
  verifyCard(card, signature, key.verifyingKey())
  verifyCard({ ...card, signatures: [signature] }, signature, key.verifyingKey())
  assert.throws(() => {
    verifyCard({ ...card, name: "impostor" }, signature, key.verifyingKey())
  })
})

void test("given_the_rust_signature_vector_when_checked_then_should_cross_verify_both_directions", async () => {
  const vector = JSON.parse(
    await readFile(
      new URL("../../../../../sdk/tests/fixtures/typescript_signature.json", import.meta.url),
      "utf8"
    )
  ) as {
    readonly public_key_hex: string
    readonly key_id_hex: string
    readonly signing_input_hex: string
    readonly rust_signature_hex: string
    readonly typescript_signature_hex: string
  }
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(7))
  const message = envelope()
  assert.equal(hex(key.verifyingKey()), vector.public_key_hex)
  assert.equal(hex(key.keyId()), vector.key_id_hex)
  assert.equal(hex(key.sign(message).bytes), vector.typescript_signature_hex)
  const registry = new KeyRegistry()
  registry.enroll("rust-agent", key.verifyingKey())
  assert.equal(
    registry.verify(
      withSignature(message, {
        scheme: 1,
        keyId: fromHex(vector.key_id_hex),
        bytes: fromHex(vector.rust_signature_hex)
      })
    ),
    "rust-agent"
  )
})

function hex(value: Uint8Array): string {
  return [...value].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

function fromHex(value: string): Uint8Array {
  return Uint8Array.from(value.match(/.{2}/g)?.map((byte) => Number.parseInt(byte, 16)) ?? [])
}

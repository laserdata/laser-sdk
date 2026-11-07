import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { test } from "node:test"
import {
  InvalidError,
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

void test("given_generated_keys_when_used_then_should_differ_sign_and_rebuild_from_their_seed", () => {
  const first = SigningKey.generate()
  const second = SigningKey.generate()
  assert.notDeepEqual(first.toBytes(), second.toBytes())
  const rebuilt = SigningKey.fromBytes(first.toBytes())
  assert.deepEqual(rebuilt.keyId(), first.keyId())
  const registry = new KeyRegistry()
  registry.enroll("generated", first.verifyingKey())
  const message = envelope()
  assert.equal(registry.verify(withSignature(message, rebuilt.sign(message))), "generated")
})

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

void test("given_an_inverted_validity_window_when_a_record_is_built_then_should_refuse_it_as_invalid", () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(16))
  assert.throws(
    () => KeyRecord.agent("agent-b", key.verifyingKey()).validWindow(200n, 100n),
    InvalidError
  )
  assert.throws(
    () => KeyRecord.agent("agent-b", key.verifyingKey()).validWindow(100n, 100n),
    InvalidError
  )
  assert.equal(
    KeyRecord.agent("agent-b", key.verifyingKey()).validWindow(100n).validFromMicros,
    100n
  )
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
  const context: SignatureContext = { contentType: 1, agentVersion: 1 }
  const delegated = withMetadata(envelope(), METADATA_DELEGATED_BY, {
    kind: "str",
    value: "user-7"
  })
  const signed = withSignature(delegated, key.signWithContext(delegated, context))
  assert.deepEqual(verifyDelegation(registry, signed, context, 1n), ["agent-a", "user-7"])
  const forged = withMetadata(signed, METADATA_DELEGATED_BY, {
    kind: "str",
    value: "user-999"
  })
  assert.throws(() => verifyDelegation(registry, forged, context, 1n), SignatureError)
})

void test("given_an_expired_key_or_rewritten_headers_when_a_delegation_is_verified_then_should_reject_it", () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(10))
  const registry = new KeyRegistry()
  registry.enrollRecord(KeyRecord.agent("agent-a", key.verifyingKey()).validWindow(100n, 200n))
  const context: SignatureContext = { contentType: 1, agentVersion: 1 }
  const delegated = withMetadata(envelope(), METADATA_DELEGATED_BY, {
    kind: "str",
    value: "user-7"
  })
  const signed = withSignature(delegated, key.signWithContext(delegated, context))
  assert.deepEqual(verifyDelegation(registry, signed, context, 150n), ["agent-a", "user-7"])
  assert.throws(() => verifyDelegation(registry, signed, context, 200n), SignatureError)
  assert.throws(
    () => verifyDelegation(registry, signed, { contentType: 3, agentVersion: 1 }, 150n),
    SignatureError
  )
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

void test("given_the_shared_card_vector_when_signed_then_should_sign_the_rfc_8785_canonical_form", async () => {
  const vector = JSON.parse(
    await readFile(
      new URL("../../../../../sdk/tests/fixtures/card_signature.json", import.meta.url),
      "utf8"
    )
  ) as {
    readonly secret_seed_hex: string
    readonly card: unknown
    readonly canonical: string
    readonly protected: string
    readonly signature: string
  }
  const key = SigningKey.fromBytes(fromHex(vector.secret_seed_hex))
  const input = `${vector.protected}.${Buffer.from(vector.canonical).toString("base64url")}`
  const expected = Buffer.from(key.signBytes(new TextEncoder().encode(input))).toString("base64url")
  assert.deepEqual(signCardValue(key, vector.card), {
    protected: vector.protected,
    signature: expected
  })
  assert.equal(expected, vector.signature)
  verifyCard(
    vector.card,
    { protected: vector.protected, signature: vector.signature },
    key.verifyingKey()
  )
})

void test("given_a_card_signature_header_when_verified_then_should_require_eddsa_no_crit_and_the_key_id", () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(7))
  const other = SigningKey.fromBytes(new Uint8Array(32).fill(8))
  const card = { name: "planner", skills: [] }
  const signed = signCardValue(key, card)
  verifyCard(card, signed, key.verifyingKey())
  const reheader = (header: string) => {
    const protectedValue = Buffer.from(header).toString("base64url")
    const payload = Buffer.from('{"name":"planner","skills":[]}').toString("base64url")
    return {
      protected: protectedValue,
      signature: Buffer.from(
        key.signBytes(new TextEncoder().encode(`${protectedValue}.${payload}`))
      ).toString("base64url")
    }
  }
  verifyCard(card, reheader('{"alg":"EdDSA"}'), key.verifyingKey())
  assert.throws(() => {
    verifyCard(card, reheader('{"alg":"none"}'), key.verifyingKey())
  }, /alg EdDSA/)
  assert.throws(() => {
    verifyCard(card, reheader('{"alg":"EdDSA","crit":["exp"],"exp":1}'), key.verifyingKey())
  }, /critical extension/)
  assert.throws(() => {
    verifyCard(card, signed, other.verifyingKey())
  }, /another key/)
})

void test("given_a_lone_surrogate_when_a_card_is_signed_then_should_reject_it", () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(7))
  assert.throws(() => signCardValue(key, { name: "\uD800" }), SignatureError)
  assert.throws(() => signCardValue(key, { ["\uDC00"]: 1 }), SignatureError)
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

void test("given_public_key_bytes_when_building_a_key_record_then_should_accept_only_a_curve_point", () => {
  const key = SigningKey.fromBytes(new Uint8Array(32).fill(9))
  const record = KeyRecord.fromVerifyingBytes("planner", key.verifyingKey(), KeyKind.Operator)
  assert.equal(record.kind, KeyKind.Operator)
  assert.deepEqual(record.verifying, key.verifyingKey())
  assert.deepEqual(record.keyId(), key.keyId())
  assert.throws(() => KeyRecord.fromVerifyingBytes("planner", new Uint8Array(31), KeyKind.Agent), {
    message: "an Ed25519 public key requires exactly 32 bytes"
  })
  assert.throws(
    () => KeyRecord.fromVerifyingBytes("planner", new Uint8Array(32).fill(255), KeyKind.Agent),
    (error: unknown) =>
      error instanceof InvalidError && error.message === "invalid Ed25519 public key"
  )
})

import assert from "node:assert/strict"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom } from "../../src/client/capabilities.js"
import { SignatureError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { Kv } from "../../src/managed/kv.js"
import { KeyRecord, KvKeyRegistry, SigningKey } from "../../src/signing.js"
import { sha256 } from "@noble/hashes/sha2.js"
import { bytesToHex } from "@noble/hashes/utils.js"
import { commandEnvelope, parseAgentId } from "../../src/wire/agent.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { KvCasCommand } from "../../src/wire/commands.js"
import { ConversationId, CorrelationId, RecordId } from "../../src/wire/ids.js"
import { KeyKind, encodeStoredKeyRecord } from "../../src/wire/keys.js"
import { type KvOutcome, type KvReply, encodeKvReply } from "../../src/wire/kv.js"

const CAPS: Capabilities = (() => {
  const base = managedCapabilitiesFrom({
    versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
    backends: []
  })
  return { ...base, kv: { ...base.kv, cas: true, casFenced: true } }
})()

function okFrame(outcome: KvOutcome): Uint8Array {
  const value = encodeKvReply({ kind: "ok", outcome } satisfies KvReply)
  if (!(value instanceof Map)) throw new Error("expected a map-shaped reply")
  return encodeNamed(value)
}

function stubLaser(replies: readonly Uint8Array[]): {
  readonly laser: Laser
  readonly calls: { readonly code: number; readonly payload: Uint8Array }[]
} {
  const calls: { code: number; payload: Uint8Array }[] = []
  let next = 0
  const transport = {
    calls,
    sendManaged(code: number, payload: Uint8Array): Promise<Uint8Array> {
      calls.push({ code, payload })
      const reply = replies[next]
      next += 1
      if (reply === undefined) throw new Error("the fake transport ran out of scripted replies")
      return Promise.resolve(reply)
    }
  }
  const laser = {
    kv: (namespace: string) => new Kv(transport, () => Promise.resolve(CAPS), namespace)
  } as unknown as Laser
  return { laser, calls }
}

const signer = SigningKey.fromBytes(new Uint8Array(32).fill(81))
const record = KeyRecord.agent("enrollee", signer.verifyingKey())

function keyIdHex(): string {
  return bytesToHex(sha256(signer.verifyingKey()).slice(0, 8))
}

function storedBytes(revoked = false): Uint8Array {
  return encodeNamed(
    encodeStoredKeyRecord({
      v: 1,
      principal: "enrollee",
      keyId: sha256(signer.verifyingKey()).slice(0, 8),
      verifyingKey: signer.verifyingKey(),
      kind: KeyKind.Agent,
      validFromMicros: 0n,
      revoked
    })
  )
}

void test("given_no_prior_enrollment_when_enrolled_then_should_write_with_an_absent_precondition", async () => {
  const { laser, calls } = stubLaser([
    okFrame({ kind: "value" }),
    okFrame({ kind: "committed", version: 1n })
  ])
  const version = await new KvKeyRegistry(laser).enrollRecord(record)
  assert.equal(version, 1n)
  assert.equal(calls[1]?.code, KvCasCommand.code)
})

void test("given_a_prior_enrollment_when_re_enrolled_then_should_swap_against_its_version", async () => {
  const key = new TextEncoder().encode(keyIdHex())
  const { laser, calls } = stubLaser([
    okFrame({ kind: "value", entry: { key, value: storedBytes(), version: 4n } }),
    okFrame({ kind: "committed", version: 5n })
  ])
  const version = await new KvKeyRegistry(laser).enrollRecord(record)
  assert.equal(version, 5n)
  assert.equal(calls[1]?.code, KvCasCommand.code)
})

void test("given_an_enrolled_key_when_revoked_then_should_swap_a_revoked_record_in", async () => {
  const key = new TextEncoder().encode(keyIdHex())
  const { laser, calls } = stubLaser([
    okFrame({ kind: "value", entry: { key, value: storedBytes(), version: 4n } }),
    okFrame({ kind: "committed", version: 5n })
  ])
  const version = await new KvKeyRegistry(laser).revoke(signer.keyId())
  assert.equal(version, 5n)
  assert.equal(calls.length, 2)
})

void test("given_an_already_revoked_key_when_revoked_again_then_should_return_without_writing", async () => {
  const key = new TextEncoder().encode(keyIdHex())
  const { laser, calls } = stubLaser([
    okFrame({ kind: "value", entry: { key, value: storedBytes(true), version: 4n } })
  ])
  const version = await new KvKeyRegistry(laser).revoke(signer.keyId())
  assert.equal(version, 4n)
  assert.equal(calls.length, 1)
})

void test("given_a_record_stored_under_a_foreign_key_when_revoked_then_should_refuse", async () => {
  const other = SigningKey.fromBytes(new Uint8Array(32).fill(82))
  const key = new TextEncoder().encode(keyIdHex())
  const { laser } = stubLaser([
    okFrame({ kind: "value", entry: { key, value: storedBytes(), version: 4n } })
  ])
  await assert.rejects(new KvKeyRegistry(laser).revoke(other.keyId()), SignatureError)
})

void test("given_malformed_and_misplaced_records_when_snapshotting_then_should_skip_only_them", async () => {
  const key = new TextEncoder().encode(keyIdHex())
  const { laser } = stubLaser([
    okFrame({
      kind: "page",
      page: {
        entries: [
          { key, value: storedBytes(), version: 1n },
          {
            key: new TextEncoder().encode("misplaced"),
            value: storedBytes(true),
            version: 2n
          },
          {
            key: new TextEncoder().encode("garbage"),
            value: new TextEncoder().encode("not a key record"),
            version: 3n
          }
        ]
      }
    })
  ])
  const registry = await new KvKeyRegistry(laser).registry()
  // The surviving record verifies a signature by the enrolled key, proving it
  // round-tripped with its verifying key intact while corrupt entries are gone.
  const envelope = commandEnvelope(
    RecordId.fromU128(1n),
    ConversationId.fromU128(2n),
    parseAgentId("enrollee"),
    CorrelationId.fromU128(3n),
    new TextEncoder().encode("prove")
  )
  const signed = { ...envelope, signature: signer.sign(envelope) }
  assert.equal(registry.verify(signed), "enrollee")
})

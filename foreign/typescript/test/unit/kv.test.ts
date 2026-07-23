import assert from "node:assert/strict"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom } from "../../src/client/capabilities.js"
import { InvalidError, UnsupportedError } from "../../src/client/errors.js"
import { Kv } from "../../src/managed/kv.js"
import { encodeBatchReply } from "../../src/wire/batch.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import {
  KvCasCommand,
  KvCasFencedCommand,
  KvCopyCommand,
  KvMoveCommand,
  KvSetCommand
} from "../../src/wire/commands.js"
import { type KvOutcome, type KvReply, encodeKvReply } from "../../src/wire/kv.js"

const CAPS: Capabilities = managedCapabilitiesFrom({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
  backends: []
})
const CAS_CAPS: Capabilities = { ...CAPS, kv: { ...CAPS.kv, cas: true, casFenced: true } }

function replyFrame(reply: KvReply): Uint8Array {
  const value = encodeKvReply(reply)
  if (!(value instanceof Map)) throw new Error("expected a map-shaped reply")
  return encodeNamed(value)
}

function okFrame(outcome: KvOutcome): Uint8Array {
  return replyFrame({ kind: "ok", outcome })
}

function fakeTransport(scriptedReplies: readonly Uint8Array[]): {
  readonly calls: { readonly code: number; readonly payload: Uint8Array }[]
  sendManaged(code: number, payload: Uint8Array): Promise<Uint8Array>
} {
  const calls: { code: number; payload: Uint8Array }[] = []
  let next = 0
  return {
    calls,
    sendManaged(code, payload) {
      calls.push({ code, payload })
      const reply = scriptedReplies[next]
      next += 1
      if (reply === undefined) throw new Error("fake transport ran out of scripted replies")
      return Promise.resolve(reply)
    }
  }
}

function kv(namespace: string, replies: readonly Uint8Array[], capabilities: Capabilities = CAPS) {
  const transport = fakeTransport(replies)
  return { kv: new Kv(transport, () => Promise.resolve(capabilities), namespace), transport }
}

void test("given_a_value_outcome_when_get_entry_is_called_then_should_decode_the_entry", async () => {
  const { kv: store } = kv("sessions", [
    okFrame({
      kind: "value",
      entry: { key: Uint8Array.of(1), value: Uint8Array.of(2, 3), version: 5n }
    })
  ])
  const entry = await store.getEntry(Uint8Array.of(1))
  assert.ok(entry !== undefined)
  assert.deepEqual(entry.value, Uint8Array.of(2, 3))
  assert.equal(entry.version, 5n)
})

void test("given_an_absent_value_outcome_when_get_is_called_then_should_return_undefined", async () => {
  const { kv: store } = kv("sessions", [okFrame({ kind: "value" })])
  assert.equal(await store.get(Uint8Array.of(1)), undefined)
})

void test("given_an_empty_key_when_any_op_is_called_then_should_reject_before_the_transport", async () => {
  const { kv: store, transport } = kv("sessions", [])
  await assert.rejects(() => store.get(new Uint8Array()), InvalidError)
  assert.equal(transport.calls.length, 0)
})

void test("given_json_and_send_when_set_is_called_then_should_encode_and_send_the_exact_value", async () => {
  const { kv: store, transport } = kv("sessions", [okFrame({ kind: "written" })])
  await store.set(Uint8Array.of(1)).json({ id: 7 }).send()
  assert.equal(transport.calls.length, 1)
  assert.deepEqual(
    transport.calls[0]?.payload,
    KvSetCommand.encode({
      namespace: "sessions",
      key: Uint8Array.of(1),
      value: new TextEncoder().encode(JSON.stringify({ id: 7 }))
    })
  )
})

void test("given_a_pinned_clock_when_ttl_is_set_then_should_encode_an_absolute_expiry", async () => {
  const { kv: store, transport } = kv("sessions", [okFrame({ kind: "written" })])
  await store.set(Uint8Array.of(1)).bytes(Uint8Array.of(2)).ttl(50n, 100n).send()
  assert.deepEqual(
    transport.calls[0]?.payload,
    KvSetCommand.encode({
      namespace: "sessions",
      key: Uint8Array.of(1),
      value: Uint8Array.of(2),
      expiresAtMicros: 150n
    })
  )
})

void test("given_no_precondition_when_commit_is_called_then_should_reject_before_the_capability_gate", async () => {
  const { kv: store, transport } = kv("sessions", [], { ...CAPS, kv: { ...CAPS.kv, cas: false } })
  await assert.rejects(
    () => store.set(Uint8Array.of(1)).bytes(Uint8Array.of(2)).commit(),
    InvalidError
  )
  assert.equal(transport.calls.length, 0)
})

void test("given_cas_not_advertised_when_commit_is_called_then_should_reject_as_unsupported", async () => {
  const { kv: store, transport } = kv("sessions", [])
  await assert.rejects(
    () => store.set(Uint8Array.of(1)).bytes(Uint8Array.of(2)).expectVersion(3n).commit(),
    UnsupportedError
  )
  assert.equal(transport.calls.length, 0)
})

void test("given_cas_advertised_when_commit_is_called_then_should_send_the_cas_command_and_return_the_version", async () => {
  const { kv: store, transport } = kv(
    "sessions",
    [okFrame({ kind: "committed", version: 9n })],
    CAS_CAPS
  )
  const version = await store
    .set(Uint8Array.of(1))
    .bytes(Uint8Array.of(2))
    .expectVersion(3n)
    .commit()
  assert.equal(version, 9n)
  assert.equal(transport.calls[0]?.code, KvCasCommand.code)
})

void test("given_fenced_cas_not_advertised_when_commit_is_called_then_should_reject_as_unsupported", async () => {
  const { kv: store, transport } = kv("sessions", [])
  await assert.rejects(
    () =>
      store
        .casFenced(Uint8Array.of(1), Uint8Array.of(9), 4n)
        .bytes(Uint8Array.of(2))
        .expectAbsent()
        .commit(),
    UnsupportedError
  )
  assert.equal(transport.calls.length, 0)
})

void test("given_fenced_cas_advertised_when_commit_is_called_then_should_send_the_cas_fenced_command", async () => {
  const { kv: store, transport } = kv(
    "sessions",
    [okFrame({ kind: "committed", version: 1n })],
    CAS_CAPS
  )
  const version = await store
    .casFenced(Uint8Array.of(1), Uint8Array.of(9), 4n)
    .bytes(Uint8Array.of(2))
    .expectAbsent()
    .commit()
  assert.equal(version, 1n)
  assert.equal(transport.calls[0]?.code, KvCasFencedCommand.code)
})

void test("given_a_deleted_outcome_when_delete_is_called_then_should_return_whether_it_existed", async () => {
  const { kv: store } = kv("sessions", [okFrame({ kind: "deleted", removed: true })])
  assert.equal(await store.delete(Uint8Array.of(1)), true)
})

void test("given_a_metadata_outcome_when_exists_is_called_then_should_return_it", async () => {
  const { kv: store } = kv("sessions", [
    okFrame({ kind: "metadata", metadata: { version: 2n, sizeBytes: 4 } })
  ])
  const metadata = await store.exists(Uint8Array.of(1))
  assert.equal(metadata?.sizeBytes, 4)
})

void test("given_a_versioned_outcome_when_expire_is_called_then_should_return_the_version", async () => {
  const { kv: store } = kv("sessions", [okFrame({ kind: "versioned", version: 6n })])
  assert.equal(await store.expire(Uint8Array.of(1), 123n), 6n)
})

void test("given_a_versioned_outcome_when_patch_is_called_then_should_return_the_version", async () => {
  const { kv: store } = kv("sessions", [okFrame({ kind: "versioned", version: 7n })])
  assert.equal(await store.patch(Uint8Array.of(1), Uint8Array.of(9)), 7n)
})

void test("given_a_leased_outcome_when_lease_is_called_then_should_return_the_token_and_ttl", async () => {
  const { kv: store } = kv("sessions", [
    okFrame({ kind: "leased", leaseToken: 42n, grantedTtlMicros: 1_000n })
  ])
  const lease = await store.lease(Uint8Array.of(1), 5_000n)
  assert.deepEqual(lease, { token: 42n, grantedTtlMicros: 1_000n })
})

void test("given_a_released_outcome_when_release_is_called_then_should_return_whether_it_was_held", async () => {
  const { kv: store } = kv("sessions", [okFrame({ kind: "released", wasHeld: false })])
  assert.equal(await store.release(Uint8Array.of(1), 42n), false)
})

void test("given_copy_to_when_sent_then_should_use_the_copy_command", async () => {
  const { kv: store, transport } = kv("sessions", [okFrame({ kind: "committed", version: 2n })])
  const version = await store.copyTo(Uint8Array.of(1), Uint8Array.of(2)).send()
  assert.equal(version, 2n)
  assert.equal(transport.calls[0]?.code, KvCopyCommand.code)
})

void test("given_move_to_when_sent_then_should_use_the_move_command", async () => {
  const { kv: store, transport } = kv("sessions", [okFrame({ kind: "committed", version: 3n })])
  const version = await store.moveTo(Uint8Array.of(1), Uint8Array.of(2)).send()
  assert.equal(version, 3n)
  assert.equal(transport.calls[0]?.code, KvMoveCommand.code)
})

void test("given_a_deleted_many_outcome_when_delete_many_is_sent_then_should_return_the_count", async () => {
  const { kv: store } = kv("sessions", [okFrame({ kind: "deletedMany", count: 3 })])
  assert.equal(await store.deleteMany().prefix(Uint8Array.of(1)).send(), 3)
})

void test("given_two_pages_when_scan_entries_is_called_then_should_follow_the_cursor_until_exhausted", async () => {
  const entryA = { key: Uint8Array.of(1), value: Uint8Array.of(1), version: 1n }
  const entryB = { key: Uint8Array.of(2), value: Uint8Array.of(2), version: 1n }
  const { kv: store, transport } = kv("sessions", [
    okFrame({ kind: "page", page: { entries: [entryA], cursor: Uint8Array.of(9) } }),
    okFrame({ kind: "page", page: { entries: [entryB] } })
  ])
  const entries = await store.scan().limit(1).entries()
  assert.equal(entries.length, 2)
  assert.equal(transport.calls.length, 2)
})

void test("given_scan_fetch_when_called_then_should_return_one_page", async () => {
  const { kv: store } = kv("sessions", [okFrame({ kind: "page", page: { entries: [] } })])
  const page = await store.scan().fetch()
  assert.deepEqual(page.entries, [])
})

void test("given_an_invalid_namespace_when_a_call_is_made_then_should_reject_before_the_transport", async () => {
  const { kv: store, transport } = kv("", [])
  await assert.rejects(() => store.get(Uint8Array.of(1)), InvalidError)
  assert.equal(transport.calls.length, 0)
})

void test("given_multiple_keys_when_get_many_is_called_then_should_decode_each_batch_slot_in_order", async () => {
  const presentSlot = okFrame({
    kind: "value",
    entry: { key: Uint8Array.of(1), value: Uint8Array.of(9), version: 1n }
  })
  const absentSlot = okFrame({ kind: "value" })
  const batchFrame = encodeNamed(encodeBatchReply({ results: [presentSlot, absentSlot] }))
  const { kv: store, transport } = kv("sessions", [batchFrame])
  const values = await store.getMany([Uint8Array.of(1), Uint8Array.of(2)])
  assert.deepEqual(values, [Uint8Array.of(9), undefined])
  assert.equal(transport.calls.length, 1)
})

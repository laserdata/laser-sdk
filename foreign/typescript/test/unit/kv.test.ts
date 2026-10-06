import assert from "node:assert/strict"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom } from "../../src/client/capabilities.js"
import {
  ConfigError,
  InvalidError,
  KvExecutionError,
  UnsupportedError
} from "../../src/client/errors.js"
import { isUnsupported } from "../../src/client/error-classify.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"
import { FencedLeaseClient, LeaseCoordinator } from "../../src/managed/coordination.js"
import { Kv } from "../../src/managed/kv.js"
import { encodeBatchReply } from "../../src/wire/batch.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  KvCasCommand,
  KvCasFencedCommand,
  KvCopyCommand,
  KvExpireCommand,
  KvMoveCommand,
  KvSetCommand
} from "../../src/wire/commands.js"
import { type KvOutcome, type KvReply, decodeKvScan, encodeKvReply } from "../../src/wire/kv.js"
import { MAX_SCAN_LIMIT, MIN_LEASE_TTL_MICROS } from "../../src/wire/limits.js"
import { encode as encodeMessagePack } from "@msgpack/msgpack"

// The shortest lifetime the contract will accept, and twice it for a request the
// store can plausibly clamp down from.
const MIN_TTL_MICROS = BigInt(MIN_LEASE_TTL_MICROS)
const REQUESTED_TTL_MICROS = 2n * MIN_TTL_MICROS

const CAPS: Capabilities = managedCapabilitiesFrom({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
  backends: []
})
const CAS_CAPS: Capabilities = {
  ...CAPS,
  kv: { ...CAPS.kv, cas: true, casFenced: true, fencedLeases: true }
}

function replyFrame(reply: KvReply): Uint8Array {
  const value = encodeKvReply(reply)
  if (!(value instanceof Map)) throw new Error("expected a map-shaped reply")
  return encodeNamed(value)
}

function okFrame(outcome: KvOutcome): Uint8Array {
  return replyFrame({ kind: "ok", outcome })
}

function fakeTransport(scriptedReplies: readonly Uint8Array[]): {
  readonly calls: {
    readonly code: number
    readonly payload: Uint8Array
    readonly options?: { readonly retryAfterReconnect?: boolean }
  }[]
  sendManaged(
    code: number,
    payload: Uint8Array,
    options?: { readonly retryAfterReconnect?: boolean }
  ): Promise<Uint8Array>
} {
  const calls: {
    code: number
    payload: Uint8Array
    options?: { readonly retryAfterReconnect?: boolean }
  }[] = []
  let next = 0
  return {
    calls,
    sendManaged(code, payload, options) {
      calls.push({ code, payload, ...(options === undefined ? {} : { options }) })
      const reply = scriptedReplies[next]
      next += 1
      if (reply === undefined) throw new Error("fake transport ran out of scripted replies")
      return Promise.resolve(reply)
    }
  }
}

function kv(namespace: string, replies: readonly Uint8Array[], capabilities: Capabilities = CAPS) {
  const transport = fakeTransport(replies)
  return { kv: Kv.create(transport, () => Promise.resolve(capabilities), namespace), transport }
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
        .casFenced(Uint8Array.of(1), "coordination", Uint8Array.of(9), 4n)
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
    .casFenced(Uint8Array.of(1), "coordination", Uint8Array.of(9), 4n)
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

void test("given_an_injected_client_when_a_laser_kv_lease_is_acquired_then_should_refuse_without_a_connection_string", async () => {
  const client = {
    clientProvider: () => Promise.resolve({}),
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
  await using laser = await Laser.builder().client(client).capabilities(CAS_CAPS).connect()
  let sent = 0
  laser[INTERNAL_TRANSPORT]().sendManaged = () => {
    sent += 1
    return Promise.reject(new Error("the shared connection must not acquire leases"))
  }
  await assert.rejects(
    laser.kv("sessions").lease(Uint8Array.of(1), "worker-1", REQUESTED_TTL_MICROS),
    ConfigError
  )
  assert.equal(sent, 0)
})

void test("given_a_lease_coordinator_when_lease_is_called_then_should_validate_and_acquire_through_it", async () => {
  const position = { topicGeneration: 1n, partition: 0, offset: 7n }
  const frames: Uint8Array[] = []
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({
      send: (_code, frame) => {
        frames.push(frame)
        return Promise.resolve(
          okFrame({ kind: "leased", leaseToken: 5n, grantedTtlMicros: MIN_TTL_MICROS, position })
        )
      },
      reset: () => Promise.resolve()
    })
  )
  const shared = fakeTransport([])
  const store = Kv.create(shared, () => Promise.resolve(CAS_CAPS), "sessions", coordinator)
  const lease = await store.lease(Uint8Array.of(1), "worker-1", REQUESTED_TTL_MICROS)
  assert.deepEqual(lease, { token: 5n, grantedTtlMicros: MIN_TTL_MICROS, position })
  assert.equal(frames.length, 1)
  assert.equal(shared.calls.length, 0, "acquisition never rides the shared connection")
  await assert.rejects(
    store.lease(new Uint8Array(), "worker-1", REQUESTED_TTL_MICROS),
    InvalidError
  )
  const unadvertised = Kv.create(shared, () => Promise.resolve(CAPS), "sessions", coordinator)
  await assert.rejects(
    unadvertised.lease(Uint8Array.of(1), "worker-1", REQUESTED_TTL_MICROS),
    UnsupportedError
  )
  assert.equal(frames.length, 1)
})

void test("given_a_renewed_outcome_when_renew_lease_is_called_then_should_return_the_same_token", async () => {
  const position = { topicGeneration: 1n, partition: 0, offset: 513n }
  const { kv: store } = kv(
    "sessions",
    [okFrame({ kind: "renewed", leaseToken: 42n, grantedTtlMicros: MIN_TTL_MICROS, position })],
    CAS_CAPS
  )
  const lease = await store.renewLease(Uint8Array.of(1), "worker-1", 42n, REQUESTED_TTL_MICROS)
  assert.deepEqual(lease, { token: 42n, grantedTtlMicros: MIN_TTL_MICROS, position })
})

void test("given_lease_not_advertised_when_lease_is_called_then_should_reject_as_unsupported", async () => {
  const { kv: store, transport } = kv("sessions", [])
  await assert.rejects(
    () => store.lease(Uint8Array.of(1), "worker-1", REQUESTED_TTL_MICROS),
    UnsupportedError
  )
  assert.equal(transport.calls.length, 0)
})

void test("given_a_released_outcome_when_release_is_called_then_should_return_whether_it_was_held", async () => {
  const { kv: store } = kv("sessions", [okFrame({ kind: "released", wasHeld: false })], CAS_CAPS)
  assert.equal(await store.release(Uint8Array.of(1), "worker-1", 42n), false)
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

void test("given_a_scan_limit_when_set_then_should_clamp_to_the_cap_and_refuse_non_integers", async () => {
  const { kv: store, transport } = kv("sessions", [
    okFrame({ kind: "page", page: { entries: [] } })
  ])
  await store
    .scan()
    .limit(MAX_SCAN_LIMIT + 5)
    .fetch()
  const sent = decodeKvScan(
    expectMap(decodeOne(transport.calls[0]?.payload ?? new Uint8Array(), "scan"), "scan"),
    "scan"
  )
  assert.equal(sent.limit, MAX_SCAN_LIMIT)
  for (const limit of [-1, 1.5, Number.NaN]) {
    assert.throws(() => store.scan().limit(limit), InvalidError, String(limit))
  }
  assert.doesNotThrow(() => store.scan().limit(0))
})

void test("given_an_unsupported_kv_error_reply_when_called_then_should_keep_the_kv_class_and_classify_as_unsupported", async () => {
  const { kv: store } = kv("sessions", [
    replyFrame({ kind: "err", error: { kind: "unsupported", message: "not on this backend" } })
  ])
  await assert.rejects(
    store.get(Uint8Array.of(1)),
    (error: unknown) => error instanceof KvExecutionError && isUnsupported(error)
  )
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

void test("given_a_precondition_when_send_is_called_then_should_refuse_instead_of_dropping_it", async () => {
  const { kv: store, transport } = kv("config", [])
  await assert.rejects(
    store.set(Uint8Array.of(1)).bytes(Uint8Array.of(2)).expectVersion(3n).send(),
    (error: unknown) =>
      error instanceof InvalidError && error.message.includes("call commit() instead")
  )
  await assert.rejects(store.set(Uint8Array.of(1)).expectAbsent().send(), InvalidError)
  assert.equal(transport.calls.length, 0)
})

void test("given_a_ttl_when_expire_is_called_then_should_encode_now_plus_the_ttl", async () => {
  const { kv: store, transport } = kv("config", [okFrame({ kind: "versioned", version: 2n })])
  assert.equal(await store.expire(Uint8Array.of(1), 50n, 100n), 2n)
  assert.deepEqual(
    transport.calls[0]?.payload,
    KvExpireCommand.encode({ namespace: "config", key: Uint8Array.of(1), expiresAtMicros: 150n })
  )
})

void test("given_an_absolute_time_when_expire_at_is_called_then_should_encode_it_unchanged", async () => {
  const { kv: store, transport } = kv("config", [okFrame({ kind: "versioned", version: 4n })])
  assert.equal(await store.expireAt(Uint8Array.of(1), 999n), 4n)
  assert.deepEqual(
    transport.calls[0]?.payload,
    KvExpireCommand.encode({ namespace: "config", key: Uint8Array.of(1), expiresAtMicros: 999n })
  )
})

void test("given_msgpack_when_set_is_sent_then_should_encode_the_value_as_messagepack", async () => {
  const { kv: store, transport } = kv("config", [okFrame({ kind: "written" })])
  await store.set(Uint8Array.of(1)).msgpack({ log_level: "debug" }).send()
  assert.deepEqual(
    transport.calls[0]?.payload,
    KvSetCommand.encode({
      namespace: "config",
      key: Uint8Array.of(1),
      value: encodeMessagePack({ log_level: "debug" }, { useBigInt64: true })
    })
  )
})

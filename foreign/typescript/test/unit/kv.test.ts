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
import {
  code,
  isRetryable,
  isStale,
  isUnsupported,
  isVersionConflict
} from "../../src/client/error-classify.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"
import { FencedLeaseClient, LeaseCoordinator } from "../../src/managed/coordination.js"
import { Kv, KvCasRequest, LeaseGuard } from "../../src/managed/kv.js"
import { KvStore, type StateStore } from "../../src/state-store.js"
import { decodeBatchRequest, encodeBatchReply } from "../../src/wire/batch.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  KvCasCommand,
  KvCasFencedCommand,
  KvCopyCommand,
  KvDeleteCommand,
  KvExpireCommand,
  KvMoveCommand,
  KvPatchCommand,
  KvSetCommand
} from "../../src/wire/commands.js"
import { ConversationId as WireConversationId } from "../../src/wire/ids.js"
import {
  type KvOutcome,
  type KvReply,
  decodeKvCasFenced,
  decodeKvDelete,
  decodeKvGet,
  decodeKvPatch,
  decodeKvScan,
  encodeKvReply
} from "../../src/wire/kv.js"
import { MAX_SCAN_LIMIT, MIN_LEASE_TTL_MICROS } from "../../src/wire/limits.js"
import type { AppliedFrontier } from "../../src/wire/mutation.js"
import { encode as encodeMessagePack } from "@msgpack/msgpack"

// The shortest lifetime the contract will accept, and twice it for a request the
// store can plausibly clamp down from.
const MIN_TTL_MICROS = BigInt(MIN_LEASE_TTL_MICROS)
const REQUESTED_TTL_MS = Number(2n * MIN_TTL_MICROS) / 1000

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

void test("given_a_ttl_when_set_then_should_send_it_relative_for_the_server_to_resolve", async () => {
  const { kv: store, transport } = kv("sessions", [
    okFrame({ kind: "written" }),
    okFrame({ kind: "written" })
  ])
  await store.set(Uint8Array.of(1)).bytes(Uint8Array.of(2)).ttl(0.05).send()
  assert.deepEqual(
    transport.calls[0]?.payload,
    KvSetCommand.encode({
      namespace: "sessions",
      key: Uint8Array.of(1),
      value: Uint8Array.of(2),
      ttlMicros: 50n
    }),
    "a client clock never turns a TTL into an absolute expiry"
  )
  await store.set(Uint8Array.of(1)).bytes(Uint8Array.of(2)).ttl(0.05).expiresAt(7n).send()
  assert.deepEqual(
    transport.calls[1]?.payload,
    KvSetCommand.encode({
      namespace: "sessions",
      key: Uint8Array.of(1),
      value: Uint8Array.of(2),
      expiresAtMicros: 7n
    }),
    "the later of the two expiry forms wins"
  )
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
  assert.equal(await store.expire(Uint8Array.of(1), 123), 6n)
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
    laser.kv("sessions").lease(Uint8Array.of(1), "worker-1", REQUESTED_TTL_MS),
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
  const before = performance.now()
  const { validUntilMs, ...lease } = await store.lease(
    Uint8Array.of(1),
    "worker-1",
    REQUESTED_TTL_MS
  )
  assert.deepEqual(lease, { token: 5n, grantedTtlMs: MIN_LEASE_TTL_MICROS / 1_000, position })
  assert.ok(validUntilMs >= before + lease.grantedTtlMs)
  assert.equal(frames.length, 1)
  assert.equal(shared.calls.length, 0, "acquisition never rides the shared connection")
  await assert.rejects(store.lease(new Uint8Array(), "worker-1", REQUESTED_TTL_MS), InvalidError)
  const unadvertised = Kv.create(shared, () => Promise.resolve(CAPS), "sessions", coordinator)
  await assert.rejects(
    unadvertised.lease(Uint8Array.of(1), "worker-1", REQUESTED_TTL_MS),
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
  const before = performance.now()
  const { validUntilMs, ...lease } = await store.renewLease(
    Uint8Array.of(1),
    "worker-1",
    42n,
    REQUESTED_TTL_MS
  )
  const after = performance.now()
  assert.deepEqual(lease, { token: 42n, grantedTtlMs: MIN_LEASE_TTL_MICROS / 1_000, position })
  assert.ok(validUntilMs >= before + lease.grantedTtlMs)
  assert.ok(validUntilMs <= after + lease.grantedTtlMs)
})

void test("given_a_lease_granted_over_the_coordination_connection_when_acquired_then_should_raise_the_shared_kv_frontier", async () => {
  const position = { topicGeneration: 1n, partition: 0, offset: 9n }
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({
      send: () =>
        Promise.resolve(
          okFrame({ kind: "leased", leaseToken: 5n, grantedTtlMicros: MIN_TTL_MICROS, position })
        ),
      reset: () => Promise.resolve()
    })
  )
  const observed: AppliedFrontier[] = []
  const shared = {
    ...fakeTransport([]),
    observePositions: (frontier: AppliedFrontier) => {
      observed.push(frontier)
    }
  }
  const store = Kv.create(shared, () => Promise.resolve(CAS_CAPS), "sessions", coordinator)
  await store.lease(Uint8Array.of(1), "worker-1", REQUESTED_TTL_MS)
  assert.deepEqual(observed, [{ kv: position }], "a later read waits for the acquisition")
})

void test("given_lease_not_advertised_when_lease_is_called_then_should_reject_as_unsupported", async () => {
  const { kv: store, transport } = kv("sessions", [])
  await assert.rejects(
    () => store.lease(Uint8Array.of(1), "worker-1", REQUESTED_TTL_MS),
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

const emptyPage = { items: [], page: 1, totalPages: 0, totalResults: 0 }

void test("given_two_pages_when_scan_entries_is_called_then_should_read_each_page_number_until_the_last", async () => {
  const entryA = { key: Uint8Array.of(1), value: Uint8Array.of(1), version: 1n }
  const entryB = { key: Uint8Array.of(2), value: Uint8Array.of(2), version: 1n }
  const { kv: store, transport } = kv("sessions", [
    okFrame({ kind: "page", page: { items: [entryA], page: 1, totalPages: 2, totalResults: 2 } }),
    okFrame({ kind: "page", page: { items: [entryB], page: 2, totalPages: 2, totalResults: 2 } })
  ])
  const entries = await store.scan().results(1).entries()
  assert.equal(entries.length, 2)
  const pages = transport.calls.map(
    (call) => decodeKvScan(expectMap(decodeOne(call.payload, "scan"), "scan"), "scan").page
  )
  assert.deepEqual(pages, [1, 2], "the walk asks for page numbers, never a cursor")
})

void test("given_a_page_number_when_scan_fetch_is_called_then_should_send_it_and_return_the_totals", async () => {
  const { kv: store, transport } = kv("sessions", [
    okFrame({ kind: "page", page: { items: [], page: 4, totalPages: 3, totalResults: 21 } })
  ])
  const page = await store.scan().page(4).results(7).fetch()
  assert.deepEqual(page, { items: [], page: 4, totalPages: 3, totalResults: 21 })
  const sent = decodeKvScan(
    expectMap(decodeOne(transport.calls[0]?.payload ?? new Uint8Array(), "scan"), "scan"),
    "scan"
  )
  assert.deepEqual([sent.page, sent.results], [4, 7])
})

void test("given_a_scan_page_size_when_set_then_should_clamp_to_the_cap_and_refuse_non_integers", async () => {
  const { kv: store, transport } = kv("sessions", [okFrame({ kind: "page", page: emptyPage })])
  await store
    .scan()
    .results(MAX_SCAN_LIMIT + 5)
    .fetch()
  const sent = decodeKvScan(
    expectMap(decodeOne(transport.calls[0]?.payload ?? new Uint8Array(), "scan"), "scan"),
    "scan"
  )
  assert.equal(sent.results, MAX_SCAN_LIMIT)
  for (const results of [-1, 1.5, Number.NaN]) {
    assert.throws(() => store.scan().results(results), InvalidError, String(results))
  }
  for (const page of [-1, 1.5, 2 ** 32]) {
    assert.throws(() => store.scan().page(page), InvalidError, String(page))
  }
  assert.doesNotThrow(() => store.scan().results(0).page(0))
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

void test("given_a_precondition_when_building_then_should_yield_a_compare_and_swap_that_only_commits", async () => {
  const { kv: store, transport } = kv("config", [])
  const requests = [
    store.set(Uint8Array.of(1)).bytes(Uint8Array.of(2)).expectVersion(3n).ttl(5000),
    store.set(Uint8Array.of(1)).expectAbsent()
  ]
  for (const request of requests) {
    assert.ok(request instanceof KvCasRequest)
    assert.equal("send" in request, false, "a compare-and-swap cannot be sent unconditionally")
    await assert.rejects(
      request.commit(),
      (error: unknown) => error instanceof UnsupportedError && error.feature === "cas"
    )
  }
  assert.equal(transport.calls.length, 0)
})

void test("given_a_ttl_when_expire_is_called_then_should_send_it_relative", async () => {
  const { kv: store, transport } = kv("config", [okFrame({ kind: "versioned", version: 2n })])
  assert.equal(await store.expire(Uint8Array.of(1), 0.05), 2n)
  assert.deepEqual(
    transport.calls[0]?.payload,
    KvExpireCommand.encode({ namespace: "config", key: Uint8Array.of(1), ttlMicros: 50n })
  )
})

void test("given_a_session_linked_store_when_a_fenced_write_commits_then_should_carry_the_session_and_a_relative_ttl", async () => {
  const { kv: store, transport } = kv(
    "sessions",
    [okFrame({ kind: "committed", version: 1n })],
    CAS_CAPS
  )
  const session = {
    stream: "orders",
    session: WireConversationId.parse("01KWM3K3XEP3NP5TN850J17YBP")
  }
  await store
    .inSession(session)
    .casFenced(Uint8Array.of(1), "coord", Uint8Array.of(2), 3n)
    .bytes(Uint8Array.of(9))
    .ttl(1)
    .expectAbsent()
    .commit()
  const sent = decodeKvCasFenced(
    expectMap(decodeOne(transport.calls[0]?.payload ?? new Uint8Array(), "fenced"), "fenced"),
    "fenced"
  )
  assert.deepEqual(sent.session, session, "a fenced write keeps the store's session link")
  assert.equal(sent.ttlMicros, 1_000n)
  assert.equal(sent.expiresAtMicros, undefined)
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

void test("given_a_session_link_when_writing_then_should_stamp_the_session_on_every_mutation", async () => {
  const session = { stream: "agents", session: WireConversationId.fromU128(7n) }
  const { kv: store, transport } = kv(
    "sessions",
    [
      okFrame({ kind: "written" }),
      okFrame({ kind: "committed", version: 2n }),
      okFrame({ kind: "deleted", removed: true }),
      okFrame({ kind: "versioned", version: 3n })
    ],
    CAS_CAPS
  )
  const linked = store.inSession(session)
  assert.equal(store.session(), undefined)
  assert.equal(linked.session(), session)
  await linked.set(Uint8Array.of(1)).bytes(Uint8Array.of(2)).send()
  await linked.set(Uint8Array.of(1)).bytes(Uint8Array.of(3)).expectAbsent().commit()
  await linked.delete(Uint8Array.of(1))
  await linked.patch(Uint8Array.of(1), Uint8Array.of(4))
  assert.deepEqual(
    transport.calls[0]?.payload,
    KvSetCommand.encode({
      namespace: "sessions",
      session,
      key: Uint8Array.of(1),
      value: Uint8Array.of(2)
    })
  )
  assert.deepEqual(
    transport.calls[1]?.payload,
    KvCasCommand.encode({
      namespace: "sessions",
      session,
      key: Uint8Array.of(1),
      value: Uint8Array.of(3),
      expect: { kind: "absent" }
    })
  )
  assert.deepEqual(
    transport.calls[2]?.payload,
    KvDeleteCommand.encode({ namespace: "sessions", session, key: Uint8Array.of(1) })
  )
  assert.deepEqual(
    transport.calls[3]?.payload,
    KvPatchCommand.encode({
      namespace: "sessions",
      session,
      key: Uint8Array.of(1),
      patch: Uint8Array.of(4)
    })
  )
})

void test("given_a_kv_store_when_used_as_a_state_store_then_should_read_write_without_expiry_and_delete", async () => {
  const key = new TextEncoder().encode("cursor/agent-1")
  const { kv: store, transport } = kv("state", [
    okFrame({ kind: "written" }),
    okFrame({ kind: "value", entry: { key, value: Uint8Array.of(7), version: 1n } }),
    okFrame({ kind: "deleted", removed: true })
  ])
  const states: StateStore = new KvStore(store)
  await states.set("cursor/agent-1", Uint8Array.of(7))
  assert.deepEqual(
    transport.calls[0]?.payload,
    KvSetCommand.encode({ namespace: "state", key, value: Uint8Array.of(7) })
  )
  assert.deepEqual(await states.get("cursor/agent-1"), Uint8Array.of(7))
  await states.delete("cursor/agent-1")
  assert.equal(transport.calls.length, 3)
})

void test("given_a_held_lease_or_an_unreachable_barrier_when_classified_then_should_neither_retry_nor_report_a_version_conflict", () => {
  const held = new KvExecutionError("kv command failed: leaseHeld", {
    kind: "leaseHeld",
    expiresAtMicros: 5n
  })
  assert.deepEqual(code(held), { kind: "known", name: "Conflict" })
  assert.equal(isVersionConflict(held), false, "a held key is not a compare-and-swap miss")
  const unreachable = new KvExecutionError("kv command failed: barrierUnreachable", {
    kind: "barrierUnreachable",
    required: { topicGeneration: 1n, partition: 0, offset: 2n }
  })
  assert.deepEqual(code(unreachable), { kind: "known", name: "StaleGeneration" })
  assert.equal(isRetryable(unreachable), false, "no wait reaches a position on an older log")
  assert.equal(isStale(unreachable), false)
})

void test("given_a_version_when_deleting_or_patching_conditionally_then_should_send_it_as_if_match", async () => {
  const { kv: store, transport } = kv(
    "sessions",
    [okFrame({ kind: "deleted", removed: true }), okFrame({ kind: "versioned", version: 4n })],
    CAS_CAPS
  )
  assert.equal(await store.deleteIfVersion(Uint8Array.of(1), 3n), true)
  assert.equal(await store.patchIfVersion(Uint8Array.of(2), new TextEncoder().encode("{}"), 3n), 4n)
  const [deleted, patched] = transport.calls.map((call) =>
    expectMap(decodeOne(call.payload, "request"), "request")
  )
  assert.equal(decodeKvDelete(deleted ?? new Map(), "delete").ifMatch, 3n)
  assert.equal(decodeKvPatch(patched ?? new Map(), "patch").ifMatch, 3n)
})

void test("given_a_held_key_when_try_lease_is_called_then_should_answer_undefined_instead_of_failing", async () => {
  const coordinator = new LeaseCoordinator(
    new FencedLeaseClient({
      send: () =>
        Promise.resolve(
          replyFrame({ kind: "err", error: { kind: "leaseHeld", expiresAtMicros: 9n } })
        ),
      reset: () => Promise.resolve()
    })
  )
  const store = Kv.create(
    fakeTransport([]),
    () => Promise.resolve(CAS_CAPS),
    "sessions",
    coordinator
  )
  assert.equal(await store.tryLease(Uint8Array.of(1), "worker-1", REQUESTED_TTL_MS), undefined)
  await assert.rejects(
    store.lease(Uint8Array.of(1), "worker-1", REQUESTED_TTL_MS),
    (error: unknown) =>
      error instanceof KvExecutionError &&
      (error.detail as { readonly kind: string }).kind === "leaseHeld"
  )
})

void test("given_a_held_version_when_read_if_changed_then_should_send_if_none_match_and_type_both_answers", async () => {
  const entry = { key: Uint8Array.of(1), value: Uint8Array.of(9), version: 5n }
  const { kv: store, transport } = kv("sessions", [
    okFrame({ kind: "notModified" }),
    okFrame({ kind: "value", entry })
  ])
  assert.deepEqual(await store.getEntryIfChanged(Uint8Array.of(1), 4n), { kind: "notModified" })
  assert.deepEqual(await store.getEntryIfChanged(Uint8Array.of(1), 4n), { kind: "changed", entry })
  const sent = decodeKvGet(
    expectMap(decodeOne(transport.calls[0]?.payload ?? new Uint8Array(), "get"), "get"),
    "get"
  )
  assert.equal(sent.ifNoneMatch, 4n)
})

void test("given_a_seen_kv_frontier_when_get_many_is_called_then_should_carry_it_on_each_read", async () => {
  const position = { topicGeneration: 1n, partition: 0, offset: 41n }
  const slot = okFrame({ kind: "value" })
  const batchFrame = encodeNamed(encodeBatchReply({ results: [slot, slot] }))
  const minPositions = async (capabilities: Capabilities) => {
    const transport = { ...fakeTransport([batchFrame]), seenPositions: () => ({ kv: position }) }
    await Kv.create(transport, () => Promise.resolve(capabilities), "sessions").getMany([
      Uint8Array.of(1),
      Uint8Array.of(2)
    ])
    const batch = decodeBatchRequest(
      expectMap(decodeOne(transport.calls[0]?.payload ?? new Uint8Array(), "batch"), "batch"),
      "batch"
    )
    return batch.ops.map(
      (item) => decodeKvGet(expectMap(decodeOne(item.payload, "get"), "get"), "get").minPosition
    )
  }
  assert.deepEqual(
    await minPositions(CAS_CAPS),
    [position, position],
    "a batch read sees the writes"
  )
  assert.deepEqual(
    await minPositions(CAPS),
    [undefined, undefined],
    "a deployment without the barrier gets no frontier it would ignore"
  )
})

void test("given_a_lease_guard_when_disposed_then_should_release_once_and_never_after_the_lease_was_lost", async () => {
  const position = { topicGeneration: 1n, partition: 0, offset: 1n }
  const granted = {
    token: 7n,
    grantedTtlMs: 1_000,
    position,
    validUntilMs: performance.now() + 1_000
  }
  const released: bigint[] = []
  let renewal: "renewed" | "lost" = "renewed"
  const store = {
    renewLease: () =>
      renewal === "renewed"
        ? Promise.resolve({ ...granted, validUntilMs: performance.now() + 1_000 })
        : Promise.reject(
            new KvExecutionError("kv command failed: leaseLost", { kind: "leaseLost" })
          ),
    release: (_key: Uint8Array, _holder: string, token: bigint) => {
      released.push(token)
      return Promise.resolve(true)
    }
  }
  {
    await using guard = LeaseGuard.create(store, Uint8Array.of(1), "worker-1", granted)
    await guard.renew(1_000)
  }
  assert.deepEqual(released, [7n], "leaving the scope releases the lease")
  const explicit = LeaseGuard.create(store, Uint8Array.of(1), "worker-1", granted)
  assert.equal(await explicit.release(), true)
  await explicit[Symbol.asyncDispose]()
  assert.deepEqual(released, [7n, 7n], "a released guard does not release again")
  renewal = "lost"
  const lost = LeaseGuard.create(store, Uint8Array.of(1), "worker-1", granted)
  await assert.rejects(lost.renew(1_000), KvExecutionError)
  await lost[Symbol.asyncDispose]()
  assert.deepEqual(released, [7n, 7n], "a lost lease is not released over its successor")
})

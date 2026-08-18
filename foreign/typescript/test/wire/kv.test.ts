import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { CodecError, InvalidError } from "../../src/client/errors.js"
import {
  decodeKvCas,
  decodeKvCasFenced,
  decodeKvCopy,
  decodeKvDeleteMany,
  decodeKvEntry,
  decodeKvGet,
  decodeKvLease,
  decodeKvLeaseRenew,
  decodeKvMove,
  decodeKvPatch,
  decodeKvRelease,
  decodeKvReply,
  decodeKvScan,
  decodeKvSet,
  encodeKvCas,
  encodeKvCasFenced,
  encodeKvCopy,
  encodeKvDeleteMany,
  encodeKvEntry,
  encodeKvGet,
  encodeKvLease,
  encodeKvLeaseRenew,
  encodeKvMove,
  encodeKvNamespaces,
  encodeKvOutcome,
  encodeKvPatch,
  encodeKvRelease,
  encodeKvReply,
  encodeKvScan,
  encodeKvSet,
  kvEntryKeyString,
  validateNamespace
} from "../../src/wire/kv.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  MAX_HOLDER_ID_BYTES,
  MAX_LEASE_TTL_MICROS,
  MIN_LEASE_TTL_MICROS
} from "../../src/wire/limits.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_the_kv_set_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("kv_set.bin")
  const map = expectMap(decodeOne(bytes, "kv_set"), "kv_set")
  const set = decodeKvSet(map, "kv_set")
  assert.equal(set.namespace, "sessions")
  assert.deepEqual(set.key, new Uint8Array([0xff, 0x00, 0x6b]))
  assert.equal(set.expiresAtMicros, 1_700_000_000_000_000n)
  const reencoded = encodeNamed(encodeKvSet(set))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_kv_cas_fixture_when_decoded_then_should_preserve_the_match_precondition", async () => {
  const bytes = await readFixture("kv_cas.bin")
  const map = expectMap(decodeOne(bytes, "kv_cas"), "kv_cas")
  const cas = decodeKvCas(map, "kv_cas")
  assert.deepEqual(cas.expect, { kind: "match", version: 7n })
  const reencoded = encodeNamed(encodeKvCas(cas))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_kv_cas_fenced_fixture_when_decoded_then_should_preserve_the_absent_precondition_and_fence", async () => {
  const bytes = await readFixture("kv_cas_fenced.bin")
  const map = expectMap(decodeOne(bytes, "kv_cas_fenced"), "kv_cas_fenced")
  const cas = decodeKvCasFenced(map, "kv_cas_fenced")
  assert.deepEqual(cas.expect, { kind: "absent" })
  assert.equal(cas.fenceNamespace, "coordination")
  assert.equal(cas.fenceToken, 3n)
  const reencoded = encodeNamed(encodeKvCasFenced(cas))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_kv_lease_fixture_when_decoded_then_should_preserve_holder_and_subject", async () => {
  const bytes = await readFixture("kv_lease.bin")
  const map = expectMap(decodeOne(bytes, "kv_lease"), "kv_lease")
  const lease = decodeKvLease(map, "kv_lease")
  assert.equal(lease.holderId, "worker-1")
  assert.equal(lease.subjectUserId, 42)
  assert.equal(lease.leaseTtlMicros, 30_000_000n)
  const reencoded = encodeNamed(encodeKvLease(lease))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_kv_lease_renew_fixture_when_decoded_then_should_preserve_token_and_holder", async () => {
  const bytes = await readFixture("kv_lease_renew.bin")
  const map = expectMap(decodeOne(bytes, "kv_lease_renew"), "kv_lease_renew")
  const renew = decodeKvLeaseRenew(map, "kv_lease_renew")
  assert.equal(renew.holderId, "worker-1")
  assert.equal(renew.subjectUserId, undefined)
  assert.equal(renew.leaseToken, 3n)
  const reencoded = encodeNamed(encodeKvLeaseRenew(renew))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_kv_release_fixture_when_decoded_then_should_preserve_token_and_holder", async () => {
  const bytes = await readFixture("kv_release.bin")
  const map = expectMap(decodeOne(bytes, "kv_release"), "kv_release")
  const release = decodeKvRelease(map, "kv_release")
  assert.equal(release.leaseToken, 3n)
  assert.equal(release.holderId, "worker-1")
  const reencoded = encodeNamed(encodeKvRelease(release))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_invalid_lease_shapes_when_encoded_then_should_reject_before_transport", () => {
  const base = {
    namespace: "coordination",
    key: new TextEncoder().encode("owner"),
    leaseTtlMicros: 30_000_000n,
    holderId: "worker-1"
  }
  assert.throws(() => encodeKvLease({ ...base, holderId: "" }), InvalidError)
  assert.throws(
    () => encodeKvLease({ ...base, holderId: "h".repeat(MAX_HOLDER_ID_BYTES + 1) }),
    InvalidError
  )
  assert.throws(() => encodeKvLease({ ...base, leaseTtlMicros: 0n }), InvalidError)
  assert.throws(
    () =>
      encodeKvLeaseRenew({
        ...base,
        leaseToken: 0n
      }),
    InvalidError
  )
  assert.throws(() => encodeKvRelease({ ...base, leaseToken: 0n }), InvalidError)
})

void test("given_out_of_range_lease_ttls_when_encoded_then_should_reject_both_ends", () => {
  const base = {
    namespace: "coordination",
    key: new TextEncoder().encode("owner"),
    holderId: "worker-1"
  }
  const min = BigInt(MIN_LEASE_TTL_MICROS)
  const max = BigInt(MAX_LEASE_TTL_MICROS)
  // The bounds themselves are grantable. A step past either end is not.
  assert.doesNotThrow(() => encodeKvLease({ ...base, leaseTtlMicros: min }))
  assert.doesNotThrow(() => encodeKvLease({ ...base, leaseTtlMicros: max }))
  assert.throws(() => encodeKvLease({ ...base, leaseTtlMicros: min - 1n }), InvalidError)
  assert.throws(() => encodeKvLease({ ...base, leaseTtlMicros: max + 1n }), InvalidError)
  // Renewal shares the range, so a holder cannot extend past the ceiling.
  assert.throws(
    () => encodeKvLeaseRenew({ ...base, leaseToken: 7n, leaseTtlMicros: max + 1n }),
    InvalidError
  )
})

void test("given_a_v1_lease_shape_when_decoded_then_should_fail_closed", () => {
  const map = encodeKvLease({
    namespace: "coordination",
    key: new TextEncoder().encode("owner"),
    leaseTtlMicros: 30_000_000n,
    holderId: "worker-1"
  })
  map.set("v", 1)
  assert.throws(() => decodeKvLease(map, "lease"), CodecError)
})

void test("given_the_kv_get_barriered_fixture_when_decoded_then_should_preserve_the_minimum_position", async () => {
  const bytes = await readFixture("kv_get_barriered.bin")
  const map = expectMap(decodeOne(bytes, "kv_get_barriered"), "kv_get_barriered")
  const get = decodeKvGet(map, "kv_get_barriered")
  assert.deepEqual(get.minPosition, { topicGeneration: 1n, partition: 0, offset: 512n })
  const reencoded = encodeNamed(encodeKvGet(get))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_kv_copy_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("kv_copy.bin")
  const map = expectMap(decodeOne(bytes, "kv_copy"), "kv_copy")
  const copy = decodeKvCopy(map, "kv_copy")
  assert.equal(copy.toNamespace, "archive")
  const reencoded = encodeNamed(encodeKvCopy(copy))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_kv_move_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("kv_move.bin")
  const map = expectMap(decodeOne(bytes, "kv_move"), "kv_move")
  const move = decodeKvMove(map, "kv_move")
  assert.equal(move.toNamespace, undefined)
  const reencoded = encodeNamed(encodeKvMove(move))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_kv_scan_fixture_when_decoded_then_should_preserve_bounds_and_cursor", async () => {
  const bytes = await readFixture("kv_scan.bin")
  const map = expectMap(decodeOne(bytes, "kv_scan"), "kv_scan")
  const scan = decodeKvScan(map, "kv_scan")
  assert.equal(scan.keyContains, "admin")
  assert.equal(scan.limit, 50)
  const reencoded = encodeNamed(encodeKvScan(scan))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

async function assertKvReplyRoundTrips(name: string) {
  const bytes = await readFixture(name)
  const reply = decodeKvReply(decodeOne(bytes, name), name)
  const reencodedValue = encodeKvReply(reply)
  if (!(reencodedValue instanceof Map)) {
    throw new Error(`expected ${name} to re-encode to a map`)
  }
  const reencoded = encodeNamed(reencodedValue)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
  return reply
}

void test("given_the_kv_reply_committed_fixture_when_decoded_then_should_carry_the_version", async () => {
  const reply = await assertKvReplyRoundTrips("kv_reply_committed.bin")
  assert.deepEqual(reply, { kind: "ok", outcome: { kind: "committed", version: 8n } })
})

void test("given_the_kv_reply_namespaces_fixture_when_decoded_then_should_carry_every_namespace", async () => {
  const reply = await assertKvReplyRoundTrips("kv_reply_namespaces.bin")
  if (reply.kind !== "ok" || reply.outcome.kind !== "namespaces") throw new Error("wrong shape")
  assert.deepEqual(reply.outcome.namespaces, [
    { namespace: "concierge_sessions", entries: 12 },
    { namespace: "sessions", entries: 3 }
  ])
})

void test("given_the_kv_reply_page_fixture_when_decoded_then_should_carry_the_entry_and_cursor", async () => {
  const reply = await assertKvReplyRoundTrips("kv_reply_page.bin")
  if (reply.kind !== "ok" || reply.outcome.kind !== "page") throw new Error("wrong shape")
  assert.equal(reply.outcome.page.entries.length, 1)
  const [entry] = reply.outcome.page.entries
  assert.ok(entry !== undefined)
  assert.equal(kvEntryKeyString(entry), "user:1")
  assert.equal(entry.version, 0n)
})

void test("given_the_kv_reply_version_conflict_fixture_when_decoded_then_should_carry_the_current_version", async () => {
  const reply = await assertKvReplyRoundTrips("kv_reply_version_conflict.bin")
  assert.deepEqual(reply, { kind: "err", error: { kind: "versionConflict", current: 7n } })
})

void test("given_the_kv_reply_leased_fixture_when_decoded_then_should_carry_token_ttl_and_position", async () => {
  const reply = await assertKvReplyRoundTrips("kv_reply_leased.bin")
  assert.deepEqual(reply, {
    kind: "ok",
    outcome: {
      kind: "leased",
      leaseToken: 3n,
      grantedTtlMicros: 30_000_000n,
      position: { topicGeneration: 1n, partition: 0, offset: 512n }
    }
  })
})

void test("given_the_kv_reply_renewed_fixture_when_decoded_then_should_carry_the_same_token", async () => {
  const reply = await assertKvReplyRoundTrips("kv_reply_renewed.bin")
  assert.deepEqual(reply, {
    kind: "ok",
    outcome: {
      kind: "renewed",
      leaseToken: 3n,
      grantedTtlMicros: 30_000_000n,
      position: { topicGeneration: 1n, partition: 0, offset: 513n }
    }
  })
})

void test("given_the_kv_reply_stale_fixture_when_decoded_then_should_carry_the_required_position", async () => {
  const reply = await assertKvReplyRoundTrips("kv_reply_stale.bin")
  assert.deepEqual(reply, {
    kind: "err",
    error: { kind: "stale", required: { topicGeneration: 1n, partition: 0, offset: 512n } }
  })
})

void test("given_namespaces_when_validated_then_should_enforce_bounds", () => {
  assert.doesNotThrow(() => {
    validateNamespace("default")
  })
  assert.doesNotThrow(() => {
    validateNamespace("agent-abc/session")
  })
  assert.throws(() => {
    validateNamespace("")
  })
  assert.throws(() => {
    validateNamespace("bad\nns")
  })
})

void test("given_a_binary_key_when_read_as_a_string_then_should_return_undefined_for_non_utf8", () => {
  const entry = { key: new Uint8Array([0xff, 0x00, 0xfe]), value: new Uint8Array(), version: 0n }
  assert.equal(kvEntryKeyString(entry), undefined)
})

void test("given_the_kv_namespaces_fixture_when_encoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("kv_namespaces.bin")
  const reencoded = encodeNamed(encodeKvNamespaces())
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_kv_delete_many_when_round_tripped_then_should_preserve_bounds", () => {
  const request = {
    namespace: "sessions",
    prefix: new TextEncoder().encode("user:"),
    keyContains: "stale"
  }
  const bytes = encodeNamed(encodeKvDeleteMany(request))
  const back = decodeKvDeleteMany(expectMap(decodeOne(bytes, "test"), "test"), "test")
  assert.deepEqual(back.prefix, request.prefix)
  assert.equal(back.keyContains, "stale")
})

void test("given_kv_deleted_many_reply_when_round_tripped_then_should_preserve_count", () => {
  const reply = { kind: "ok" as const, outcome: { kind: "deletedMany" as const, count: 7 } }
  const bytes = encodeNamed(new Map([["Ok", encodeKvOutcome(reply.outcome)]]))
  const back = decodeKvReply(decodeOne(bytes, "test"), "test")
  if (back.kind !== "ok" || back.outcome.kind !== "deletedMany") throw new Error("wrong shape")
  assert.equal(back.outcome.count, 7)
})

void test("given_a_binary_key_entry_when_round_tripped_then_should_preserve_raw_bytes", () => {
  const entry = {
    key: new Uint8Array([0xff, 0x00, 0xfe]),
    value: new Uint8Array([0x00, 0x01, 0x02]),
    version: 0n
  }
  const bytes = encodeNamed(encodeKvEntry(entry))
  const back = decodeKvEntry(expectMap(decodeOne(bytes, "test"), "test"), "test")
  assert.deepEqual(back.key, entry.key)
  assert.equal(kvEntryKeyString(back), undefined, "non-UTF-8 key has no string form")
  assert.deepEqual(back.value, entry.value)
})

void test("given_an_exists_metadata_reply_when_round_tripped_then_should_preserve_metadata", () => {
  const outcome = {
    kind: "metadata" as const,
    metadata: { version: 4n, expiresAtMicros: 1_700_000_000_000_000n, sizeBytes: 128 }
  }
  const bytes = encodeNamed(new Map([["Ok", encodeKvOutcome(outcome)]]))
  const back = decodeKvReply(decodeOne(bytes, "test"), "test")
  if (back.kind !== "ok" || back.outcome.kind !== "metadata") throw new Error("wrong shape")
  assert.ok(back.outcome.metadata !== undefined)
  assert.equal(back.outcome.metadata.version, 4n)
  assert.equal(back.outcome.metadata.sizeBytes, 128)
})

void test("given_a_patch_request_when_round_tripped_then_should_preserve_patch_and_precondition", () => {
  const request = {
    namespace: "docs",
    key: new TextEncoder().encode("doc:1"),
    patch: new TextEncoder().encode('{"status":"closed"}'),
    ifMatch: 3n
  }
  const bytes = encodeNamed(encodeKvPatch(request))
  const back = decodeKvPatch(expectMap(decodeOne(bytes, "test"), "test"), "test")
  assert.deepEqual(back.patch, request.patch)
  assert.equal(back.ifMatch, 3n)
})

void test("given_a_lease_reply_when_round_tripped_then_should_preserve_token_ttl_and_position", () => {
  const outcome = {
    kind: "leased" as const,
    leaseToken: 77n,
    grantedTtlMicros: 30_000_000n,
    position: { topicGeneration: 3n, partition: 1, offset: 4_200n }
  }
  const bytes = encodeNamed(new Map([["Ok", encodeKvOutcome(outcome)]]))
  const back = decodeKvReply(decodeOne(bytes, "test"), "test")
  if (back.kind !== "ok" || back.outcome.kind !== "leased") throw new Error("wrong shape")
  assert.equal(back.outcome.leaseToken, 77n)
  assert.equal(back.outcome.grantedTtlMicros, 30_000_000n)
  assert.deepEqual(back.outcome.position, { topicGeneration: 3n, partition: 1, offset: 4_200n })
})

void test("given_a_conditional_get_when_round_tripped_then_should_preserve_if_none_match_and_omit_when_absent", () => {
  const request = {
    namespace: "sessions",
    key: new TextEncoder().encode("user:1"),
    ifNoneMatch: 5n
  }
  const bytes = encodeNamed(encodeKvGet(request))
  const back = decodeKvGet(expectMap(decodeOne(bytes, "test"), "test"), "test")
  assert.equal(back.ifNoneMatch, 5n)
  const plainBytes = encodeNamed(encodeKvGet({ namespace: "sessions", key: request.key }))
  const plainBack = decodeKvGet(expectMap(decodeOne(plainBytes, "test"), "test"), "test")
  assert.equal(plainBack.ifNoneMatch, undefined, "absent precondition omitted")
})

void test("given_a_versioned_entry_when_round_tripped_then_should_preserve_version_and_skip_zero", () => {
  const entry = {
    key: new TextEncoder().encode("k"),
    value: new TextEncoder().encode("v"),
    version: 5n
  }
  const bytes = encodeNamed(encodeKvEntry(entry))
  const back = decodeKvEntry(expectMap(decodeOne(bytes, "test"), "test"), "test")
  assert.equal(back.version, 5n)
  const unversionedBytes = encodeNamed(encodeKvEntry({ ...entry, version: 0n }))
  const unversionedBack = decodeKvEntry(
    expectMap(decodeOne(unversionedBytes, "test"), "test"),
    "test"
  )
  assert.equal(unversionedBack.version, 0n, "version 0 must be omitted")
})

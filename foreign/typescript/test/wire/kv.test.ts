import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  decodeKvCas,
  decodeKvCasFenced,
  decodeKvCopy,
  decodeKvDeleteMany,
  decodeKvEntry,
  decodeKvGet,
  decodeKvMove,
  decodeKvPatch,
  decodeKvReply,
  decodeKvScan,
  decodeKvSet,
  encodeKvCas,
  encodeKvCasFenced,
  encodeKvCopy,
  encodeKvDeleteMany,
  encodeKvEntry,
  encodeKvGet,
  encodeKvMove,
  encodeKvNamespaces,
  encodeKvOutcome,
  encodeKvPatch,
  encodeKvReply,
  encodeKvScan,
  encodeKvSet,
  kvEntryKeyString,
  validateNamespace
} from "../../src/wire/kv.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"

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
  assert.equal(cas.fenceToken, 3n)
  const reencoded = encodeNamed(encodeKvCasFenced(cas))
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

// Pure round-trip tests ported directly from `wire/src/kv.rs`'s own
// `#[cfg(test)] mod tests`, no fixture file needed for these: they prove
// behavior (bounds preservation, zero-omission) rather than pinning bytes.

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

void test("given_a_lease_reply_when_round_tripped_then_should_preserve_token_and_ttl", () => {
  const outcome = { kind: "leased" as const, leaseToken: 77n, grantedTtlMicros: 30_000_000n }
  const bytes = encodeNamed(new Map([["Ok", encodeKvOutcome(outcome)]]))
  const back = decodeKvReply(decodeOne(bytes, "test"), "test")
  if (back.kind !== "ok" || back.outcome.kind !== "leased") throw new Error("wrong shape")
  assert.equal(back.outcome.leaseToken, 77n)
  assert.equal(back.outcome.grantedTtlMicros, 30_000_000n)
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

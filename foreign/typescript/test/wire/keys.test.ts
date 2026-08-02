import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { decodeStoredKeyRecord, encodeStoredKeyRecord } from "../../src/wire/keys.js"

const FIXTURE = path.resolve(process.cwd(), "../../wire/fixtures/key_record.bin")

void test("given_the_key_record_fixture_when_decoded_then_should_preserve_lifecycle", async () => {
  const buffer = await readFile(FIXTURE)
  const bytes = new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  const record = decodeStoredKeyRecord(
    expectMap(decodeOne(bytes, "key_record"), "key_record"),
    "key_record"
  )
  assert.equal(record.v, 1)
  assert.equal(record.principal, "operator-1")
  assert.equal(record.keyId.byteLength, 8)
  assert.equal(record.verifyingKey.byteLength, 32)
  assert.deepEqual(Buffer.from(encodeNamed(encodeStoredKeyRecord(record))), Buffer.from(bytes))
})

void test("given_a_key_record_without_version_or_key_identity_when_decoded_then_should_reject", () => {
  const valid = new Map<string, unknown>([
    ["v", 1],
    ["principal", "operator-1"],
    ["key_id", new Uint8Array(8)],
    ["verifying_key", new Uint8Array(32)],
    ["kind", "operator"],
    ["valid_from_micros", 0n],
    ["revoked", false]
  ])
  const missingVersion = new Map(valid)
  missingVersion.delete("v")
  const missingKeyId = new Map(valid)
  missingKeyId.delete("key_id")
  assert.throws(() => decodeStoredKeyRecord(missingVersion, "key_record"), /v/)
  assert.throws(() => decodeStoredKeyRecord(missingKeyId, "key_record"), /key_id/)
})

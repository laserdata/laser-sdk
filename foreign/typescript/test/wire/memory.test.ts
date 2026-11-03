import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne } from "../../src/wire/cbor.js"
import {
  decodeMemoryRecord,
  encodeMemoryRecordFrame,
  type MemoryRecord
} from "../../src/wire/memory.js"

void test("given_each_memory_record_variant_when_round_tripped_then_should_preserve_fields", () => {
  const records: readonly MemoryRecord[] = [
    {
      kind: "item",
      id: "01KWM3K3XEP3NP5TN850J17YBP",
      memoryKind: "fact",
      body: new TextEncoder().encode("auth is slow")
    },
    { kind: "forget", target: "01KWM3K3XEP3NP5TN850J17YBP" },
    { kind: "feedback", target: "01KWM3K3XEP3NP5TN850J17YBP", weight: 1.5 }
  ]
  for (const record of records) {
    const bytes = encodeMemoryRecordFrame(record)
    assert.deepEqual(decodeMemoryRecord(decodeOne(bytes, "memory_record"), "memory_record"), record)
  }
})

void test("given_a_whole_number_memory_weight_when_encoded_then_should_remain_a_float", () => {
  const record: MemoryRecord = { kind: "feedback", target: "item", weight: 1 }
  const bytes = encodeMemoryRecordFrame(record)
  assert.equal(Buffer.from(bytes).includes(Buffer.from([0xf9, 0x3c, 0x00])), true)
  assert.deepEqual(decodeMemoryRecord(decodeOne(bytes, "memory_record"), "memory_record"), record)
})

void test("given_a_memory_origin_fixture_when_decoded_then_should_preserve_origin_and_producer", async () => {
  const buffer = await readFile(
    path.resolve(process.cwd(), "../../wire/fixtures/memory_item_origin.bin")
  )
  const bytes = new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  const record = decodeMemoryRecord(decodeOne(bytes, "memory_item_origin"), "memory_item_origin")
  assert.equal(record.kind, "item")
  assert.equal(record.origin?.kind, "message")
  assert.equal(record.producer?.name, "extractor")
  assert.deepEqual(Buffer.from(encodeMemoryRecordFrame(record)), Buffer.from(bytes))
})

void test("given_scoped_tombstone_fixtures_when_decoded_then_should_match_rust_bytes", async () => {
  for (const name of ["memory_forget_scoped.bin", "memory_feedback_scoped.bin"]) {
    const buffer = await readFile(path.resolve(process.cwd(), `../../wire/fixtures/${name}`))
    const bytes = new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
    const record = decodeMemoryRecord(decodeOne(bytes, name), name)
    assert.ok(record.kind === "forget" || record.kind === "feedback")
    assert.equal(record.conversation, "01KWM3K3XEP3NP5TN850J17YBQ")
    assert.deepEqual(Buffer.from(encodeMemoryRecordFrame(record)), Buffer.from(bytes), name)
  }
})

import assert from "node:assert/strict"
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
      body: new TextEncoder().encode("checkout is slow")
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

import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { decodeChangeRecord, encodeChangeRecord } from "../../src/wire/change.js"

const FIXTURE = path.resolve(process.cwd(), "../../wire/fixtures/change_record.bin")

void test("given_the_change_record_fixture_when_decoded_then_should_preserve_the_watermark", async () => {
  const buffer = await readFile(FIXTURE)
  const bytes = new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  const record = decodeChangeRecord(
    expectMap(decodeOne(bytes, "change_record"), "change_record"),
    "change_record"
  )
  assert.deepEqual(record, {
    v: 1,
    index: "orders_v1",
    partitionId: 3,
    fromOffset: 100n,
    toOffset: 141n,
    rows: 42
  })
  assert.deepEqual(Buffer.from(encodeNamed(encodeChangeRecord(record))), Buffer.from(bytes))
})

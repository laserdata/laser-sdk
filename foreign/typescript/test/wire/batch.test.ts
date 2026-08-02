import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  decodeBatchReply,
  decodeBatchRequest,
  encodeBatchReply,
  encodeBatchRequest
} from "../../src/wire/batch.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_the_batch_request_fixture_when_decoded_then_should_preserve_order_and_payloads", async () => {
  const bytes = await readFixture("batch_request.bin")
  const request = decodeBatchRequest(
    expectMap(decodeOne(bytes, "batch_request"), "batch_request"),
    "batch_request"
  )
  assert.equal(request.ops.length, 2)
  assert.deepEqual(
    Buffer.from(request.ops[0]?.payload ?? []),
    Buffer.from([0xa1, 0x61, 0x76, 0x01])
  )
  assert.deepEqual(Buffer.from(encodeNamed(encodeBatchRequest(request))), Buffer.from(bytes))
})

void test("given_the_batch_reply_fixture_when_decoded_then_should_preserve_empty_results", async () => {
  const bytes = await readFixture("batch_reply.bin")
  const reply = decodeBatchReply(
    expectMap(decodeOne(bytes, "batch_reply"), "batch_reply"),
    "batch_reply"
  )
  assert.deepEqual(
    reply.results.map((result) => result.byteLength),
    [5, 0]
  )
  assert.deepEqual(Buffer.from(encodeNamed(encodeBatchReply(reply))), Buffer.from(bytes))
})

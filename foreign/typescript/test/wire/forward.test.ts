import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  decodeForwardedCommand,
  decodeForwardedQuery,
  encodeForwardedCommand,
  encodeForwardedQuery
} from "../../src/wire/forward.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_the_forwarded_query_fixture_when_decoded_then_should_preserve_identity_and_bytes", async () => {
  const bytes = await readFixture("forwarded_query.bin")
  const query = decodeForwardedQuery(
    expectMap(decodeOne(bytes, "forwarded_query"), "forwarded_query"),
    "forwarded_query"
  )
  assert.deepEqual(
    { userId: query.userId, clientId: query.clientId, correlation: query.correlation },
    { userId: 7, clientId: 42n, correlation: "conv-1" }
  )
  assert.deepEqual(Buffer.from(query.queryEnvelope), Buffer.from([1, 2, 3, 4]))
  assert.deepEqual(Buffer.from(encodeNamed(encodeForwardedQuery(query))), Buffer.from(bytes))
})

void test("given_the_forwarded_command_fixture_when_decoded_then_should_preserve_null_correlation", async () => {
  const bytes = await readFixture("forwarded_command.bin")
  const command = decodeForwardedCommand(
    expectMap(decodeOne(bytes, "forwarded_command"), "forwarded_command"),
    "forwarded_command"
  )
  assert.equal(command.correlation, undefined)
  assert.equal(command.readAll, true)
  assert.deepEqual(Buffer.from(command.payload), Buffer.from([9, 9, 9]))
  assert.deepEqual(Buffer.from(encodeNamed(encodeForwardedCommand(command))), Buffer.from(bytes))
})

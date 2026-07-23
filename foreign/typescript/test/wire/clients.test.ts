import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  decodeClientMetadataList,
  decodeClientMetadataQuery,
  encodeClientMetadataList,
  encodeClientMetadataQuery
} from "../../src/wire/clients.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_the_client_metadata_query_fixture_when_decoded_then_should_preserve_filters", async () => {
  const bytes = await readFixture("client_metadata_query.bin")
  const query = decodeClientMetadataQuery(
    expectMap(decodeOne(bytes, "client_metadata_query"), "client_metadata_query"),
    "client_metadata_query"
  )
  assert.deepEqual(query, {
    v: 1,
    withMetadataOnly: true,
    userId: 42,
    afterClientId: 100,
    limit: 50
  })
  assert.deepEqual(Buffer.from(encodeNamed(encodeClientMetadataQuery(query))), Buffer.from(bytes))
})

void test("given_the_client_metadata_list_fixture_when_decoded_then_should_preserve_the_cursor", async () => {
  const bytes = await readFixture("client_metadata_list.bin")
  const list = decodeClientMetadataList(
    expectMap(decodeOne(bytes, "client_metadata_list"), "client_metadata_list"),
    "client_metadata_list"
  )
  assert.equal(list.clients.length, 2)
  const [first] = list.clients
  assert.ok(first !== undefined)
  assert.equal(first.address, "127.0.0.1:8090")
  assert.deepEqual(Buffer.from(first.metadata ?? []), Buffer.from('{"role":"planner"}'))
  assert.equal(list.nextCursor, 9)
  assert.deepEqual(Buffer.from(encodeNamed(encodeClientMetadataList(list))), Buffer.from(bytes))
})

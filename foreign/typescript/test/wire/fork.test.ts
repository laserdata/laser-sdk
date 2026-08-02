import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  decodeForkCreate,
  decodeForkPut,
  decodeForkReply,
  encodeForkCreate,
  encodeForkPut,
  encodeForkReply,
  validateForkId
} from "../../src/wire/fork.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { MAX_FORK_ID_BYTES } from "../../src/wire/limits.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_the_fork_create_fixture_when_decoded_then_should_preserve_kind", async () => {
  const bytes = await readFixture("fork_create.bin")
  const map = expectMap(decodeOne(bytes, "fork_create"), "fork_create")
  const create = decodeForkCreate(map, "fork_create")
  assert.equal(create.forkId, "agent-run-7")
  assert.equal(create.parent, "trunk")
  assert.equal(create.kind, "severed")
  assert.deepEqual(create.tables, ["orders_rows"])
  const reencoded = encodeNamed(encodeForkCreate(create))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_fork_put_fixture_when_decoded_then_should_preserve_fields_metadata_and_embedding", async () => {
  const bytes = await readFixture("fork_put.bin")
  const map = expectMap(decodeOne(bytes, "fork_put"), "fork_put")
  const put = decodeForkPut(map, "fork_put")
  assert.equal(put.projectionId, "order.v1")
  assert.equal(put.fields.get("amount"), "999")
  assert.equal(put.metadata.get("note"), "speculative")
  assert.equal(put.embedding, "[0.1,0.2]")
  assert.equal(put.tombstone, false)
  const reencoded = encodeNamed(encodeForkPut(put))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_fork_reply_created_fixture_when_decoded_then_should_preserve_info", async () => {
  const bytes = await readFixture("fork_reply_created.bin")
  const reply = decodeForkReply(decodeOne(bytes, "fork_reply_created"), "fork_reply_created")
  if (reply.kind !== "ok" || reply.outcome.kind !== "created") throw new Error("wrong shape")
  assert.equal(reply.outcome.info.userId, 5)
  assert.equal(reply.outcome.info.kind, "severed")
  assert.equal(reply.outcome.info.status, "open")
  const reencodedValue = encodeForkReply(reply)
  if (!(reencodedValue instanceof Map)) throw new Error("expected a map")
  const reencoded = encodeNamed(reencodedValue)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_fork_ids_when_validated_then_should_enforce_charset_and_length", () => {
  assert.doesNotThrow(() => {
    validateForkId("experiment-2026-q2")
  })
  assert.doesNotThrow(() => {
    validateForkId("run_7.v2")
  })
  assert.throws(() => {
    validateForkId("")
  }, /empty/)
  assert.throws(() => {
    validateForkId("bad id")
  })
  assert.throws(() => {
    validateForkId("o'brien; drop table")
  })
  assert.throws(() => {
    validateForkId("name/../etc")
  })
  assert.doesNotThrow(() => {
    validateForkId("f".repeat(MAX_FORK_ID_BYTES))
  })
  assert.throws(() => {
    validateForkId("f".repeat(MAX_FORK_ID_BYTES + 1))
  })
})

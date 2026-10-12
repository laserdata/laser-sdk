import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { CodecError } from "../../src/client/errors.js"
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
    {
      userId: query.userId,
      clientId: query.clientId,
      correlation: query.correlation,
      grants: query.grants
    },
    {
      userId: 7,
      clientId: 42n,
      correlation: "conv-1",
      grants: []
    }
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
  assert.equal(command.readBarrier, false)
  assert.equal(command.minPosition, undefined)
  assert.equal(command.positionedReply, false)
  assert.deepEqual(command.minSessions, [])
  assert.deepEqual(command.grants, [])
  assert.deepEqual(Buffer.from(command.payload), Buffer.from([9, 9, 9]))
  assert.deepEqual(Buffer.from(encodeNamed(encodeForwardedCommand(command))), Buffer.from(bytes))
  for (const [key, value] of [
    ["read_barrier", "true"],
    ["min_position", 1n],
    ["positioned_reply", 1n],
    ["min_sessions", [new Map()]]
  ] as const) {
    const malformed = new Map(expectMap(decodeOne(bytes, "forwarded"), "forwarded"))
    malformed.set(key, value)
    assert.throws(() => decodeForwardedCommand(malformed, "forwarded"), CodecError)
  }
})

void test("given_a_positioned_forwarded_command_when_round_tripped_then_should_preserve_every_barrier", async () => {
  const bytes = await readFixture("forwarded_command_positioned.bin")
  const command = decodeForwardedCommand(
    expectMap(decodeOne(bytes, "forwarded_command_positioned"), "forwarded_command_positioned"),
    "forwarded_command_positioned"
  )
  assert.equal(command.readBarrier, true)
  assert.equal(command.positionedReply, true)
  assert.deepEqual(command.minPosition, { topicGeneration: 9n, partition: 0, offset: 42n })
  assert.deepEqual(command.minSessions, [
    {
      streamId: 0,
      streamGeneration: 1_700_000_000_000_000n,
      topic: "agent.sessions",
      topicGeneration: 10n,
      partitionId: 0,
      offset: 43n
    }
  ])
  assert.deepEqual(Buffer.from(encodeNamed(encodeForwardedCommand(command))), Buffer.from(bytes))
})

void test("given_a_scoped_forwarded_command_when_decoded_then_should_keep_zero_stream_id", async () => {
  const bytes = await readFixture("forwarded_command_scoped.bin")
  const command = decodeForwardedCommand(
    expectMap(decodeOne(bytes, "forwarded_command_scoped"), "forwarded_command_scoped"),
    "forwarded_command_scoped"
  )
  assert.equal(command.scope?.streamId, 0)
  assert.equal(command.scope.stream, "alpha")
  assert.deepEqual(Buffer.from(encodeNamed(encodeForwardedCommand(command))), Buffer.from(bytes))
})

void test("given_a_scoped_stream_tenancy_query_when_decoded_then_should_keep_scope_and_tenancy", async () => {
  const bytes = await readFixture("forwarded_query_scoped.bin")
  const query = decodeForwardedQuery(
    expectMap(decodeOne(bytes, "forwarded_query_scoped"), "forwarded_query_scoped"),
    "forwarded_query_scoped"
  )
  assert.equal(query.tenancy, "stream")
  assert.equal(query.scope?.stream, "alpha")
  assert.equal(query.scope.streamId, 0)
  assert.deepEqual(Buffer.from(encodeNamed(encodeForwardedQuery(query))), Buffer.from(bytes))
})

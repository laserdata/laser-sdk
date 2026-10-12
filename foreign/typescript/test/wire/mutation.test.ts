import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  decodeManagedReadEnvelope,
  decodeManagedReplyEnvelope,
  decodeManagedRequestEnvelope,
  decodeMutationCommandEnvelope,
  encodeManagedReadEnvelope,
  encodeManagedReplyEnvelope,
  encodeManagedRequestEnvelope,
  encodeMutationCommandEnvelope,
  mergeFrontier
} from "../../src/wire/mutation.js"

const fixture = (name: string): string => path.resolve(process.cwd(), `../../wire/fixtures/${name}`)

async function fixtureBytes(name: string): Promise<Uint8Array> {
  const buffer = await readFile(fixture(name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_the_mutation_command_fixture_when_decoded_then_should_preserve_request_bytes", async () => {
  const bytes = await fixtureBytes("mutation_command.bin")
  const envelope = decodeMutationCommandEnvelope(
    expectMap(decodeOne(bytes, "mutation_command"), "mutation_command"),
    "mutation_command"
  )
  assert.equal(envelope.operationId, 42n)
  assert.equal(envelope.timestampMicros, 1_700_000_000_000_000n)
  assert.ok(envelope.payload.byteLength > 0)
  assert.deepEqual(
    Buffer.from(encodeNamed(encodeMutationCommandEnvelope(envelope))),
    Buffer.from(bytes)
  )
})

void test("given_a_scoped_mutation_command_when_decoded_then_should_keep_the_trusted_scope", async () => {
  const bytes = await fixtureBytes("mutation_command_scoped.bin")
  const envelope = decodeMutationCommandEnvelope(
    expectMap(decodeOne(bytes, "mutation_command_scoped"), "mutation_command_scoped"),
    "mutation_command_scoped"
  )
  assert.equal(envelope.scope?.streamId, 0)
  assert.equal(envelope.scope.streamCreatedAtMicros, 1_700_000_000_000_000n)
  assert.deepEqual(
    Buffer.from(encodeNamed(encodeMutationCommandEnvelope(envelope))),
    Buffer.from(bytes)
  )
})

void test("given_the_managed_request_fixture_when_decoded_then_should_preserve_operation_identity", async () => {
  const bytes = await fixtureBytes("managed_request.bin")
  const envelope = decodeManagedRequestEnvelope(
    expectMap(decodeOne(bytes, "managed_request"), "managed_request"),
    "managed_request"
  )
  assert.equal(envelope.operationId, 42n)
  assert.deepEqual(
    Buffer.from(encodeNamed(encodeManagedRequestEnvelope(envelope))),
    Buffer.from(bytes)
  )
})

void test("given_a_full_width_operation_identity_when_encoded_then_should_round_trip", () => {
  const operationId = (1n << 128n) - 1n
  const bytes = encodeNamed(
    encodeManagedRequestEnvelope({
      v: 1,
      operationId,
      payload: Uint8Array.of(1, 2, 3)
    })
  )
  const envelope = decodeManagedRequestEnvelope(
    expectMap(decodeOne(bytes, "managed_request"), "managed_request"),
    "managed_request"
  )
  assert.equal(envelope.operationId, operationId)
})

void test("given_missing_or_zero_operation_identity_when_decoded_then_should_reject", () => {
  const missing = new Map<string, unknown>([
    ["v", 1],
    ["payload", new Uint8Array()]
  ])
  const zero = new Map<string, unknown>([
    ["v", 1],
    ["operation_id", 0n],
    ["payload", new Uint8Array()]
  ])
  assert.throws(() => decodeManagedRequestEnvelope(missing, "managed_request"), /operation_id/)
  assert.throws(() => decodeManagedRequestEnvelope(zero, "managed_request"), /must not be zero/)
})

const SESSION_POSITION = {
  streamId: 2,
  streamGeneration: 1_700_000_000_000_001n,
  topic: "agent.sessions",
  topicGeneration: 1_700_000_000_000_002n,
  partitionId: 1,
  offset: 9n
}

void test("given_the_positioned_managed_fixtures_when_decoded_then_should_match_rust_bytes", async () => {
  const readBytes = await fixtureBytes("managed_read.bin")
  const read = decodeManagedReadEnvelope(
    expectMap(decodeOne(readBytes, "managed_read"), "managed_read"),
    "managed_read"
  )
  assert.equal(read.minPosition?.offset, 41n)
  assert.equal(read.logHead, true)
  assert.deepEqual(read.minSessions, [SESSION_POSITION])
  assert.deepEqual(
    Buffer.from(encodeNamed(encodeManagedReadEnvelope(read))),
    Buffer.from(readBytes)
  )

  const replyBytes = await fixtureBytes("managed_reply.bin")
  const reply = decodeManagedReplyEnvelope(
    expectMap(decodeOne(replyBytes, "managed_reply"), "managed_reply"),
    "managed_reply"
  )
  assert.equal(reply.frontier.kv?.offset, 41n)
  assert.equal(reply.frontier.fork, undefined)
  assert.equal(reply.frontier.graph?.offset, 3n)
  assert.deepEqual(reply.frontier.sessions, [SESSION_POSITION])
  assert.deepEqual(
    Buffer.from(encodeNamed(encodeManagedReplyEnvelope(reply))),
    Buffer.from(replyBytes)
  )
  assert.throws(() =>
    decodeManagedReplyEnvelope(new Map<unknown, unknown>([["Ok", 1n]]), "plain reply")
  )
})

void test("given_frontiers_when_merged_then_should_keep_the_later_position_per_surface", () => {
  const at = (topicGeneration: bigint, offset: bigint) => ({
    topicGeneration,
    partition: 0,
    offset
  })
  const seen = mergeFrontier(
    { kv: at(1n, 9n), fork: at(1n, 4n) },
    { kv: at(1n, 3n), fork: at(2n, 0n), graph: at(1n, 1n) }
  )
  assert.deepEqual(seen, { kv: at(1n, 9n), fork: at(2n, 0n), graph: at(1n, 1n) })
})

void test("given_session_positions_when_merged_then_should_keep_the_later_unit_and_drop_an_older_stream", () => {
  const at = (streamGeneration: bigint, topic: string, offset: bigint) => ({
    streamId: 4,
    streamGeneration,
    topic,
    topicGeneration: 1n,
    partitionId: 0,
    offset
  })
  const seen = mergeFrontier(
    { sessions: [at(1n, "lane", 5n), at(1n, "inbox", 2n)] },
    { sessions: [at(1n, "lane", 3n)] }
  )
  assert.deepEqual(seen.sessions, [at(1n, "lane", 5n), at(1n, "inbox", 2n)])
  assert.deepEqual(mergeFrontier(seen, { sessions: [at(2n, "lane", 0n)] }).sessions, [
    at(2n, "lane", 0n)
  ])
})

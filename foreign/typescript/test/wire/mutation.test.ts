import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  decodeManagedRequestEnvelope,
  decodeMutationCommandEnvelope,
  encodeManagedRequestEnvelope,
  encodeMutationCommandEnvelope
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

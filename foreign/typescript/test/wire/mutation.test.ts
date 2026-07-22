import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  decodeMutationCommandEnvelope,
  encodeMutationCommandEnvelope
} from "../../src/wire/mutation.js"

const FIXTURE = path.resolve(process.cwd(), "../../wire/fixtures/mutation_command.bin")

void test("given_the_mutation_command_fixture_when_decoded_then_should_preserve_request_bytes", async () => {
  const buffer = await readFile(FIXTURE)
  const bytes = new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  const envelope = decodeMutationCommandEnvelope(
    expectMap(decodeOne(bytes, "mutation_command"), "mutation_command"),
    "mutation_command"
  )
  assert.equal(envelope.timestampMicros, 1_700_000_000_000_000n)
  assert.ok(envelope.payload.byteLength > 0)
  assert.deepEqual(
    Buffer.from(encodeNamed(encodeMutationCommandEnvelope(envelope))),
    Buffer.from(bytes)
  )
})

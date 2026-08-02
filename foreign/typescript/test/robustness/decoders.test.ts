import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne } from "../../src/wire/cbor.js"
import { decodeBackendAnnounce, decodeHelloReply } from "../../src/wire/hello.js"
import { assertDecoderIsRobust } from "../wire/support/robustness.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_truncated_bit_flipped_or_trailing_corrupted_bytes_when_decoding_any_cbor_value_then_should_never_crash_unstructured", async () => {
  const bytes = await readFixture("backend_announce.bin")
  assertDecoderIsRobust(bytes, (candidate) => decodeOne(candidate, "robustness"))
})

void test("given_truncated_bit_flipped_or_trailing_corrupted_bytes_when_decoding_a_backend_announce_then_should_never_crash_unstructured", async () => {
  const bytes = await readFixture("backend_announce_topology.bin")
  assertDecoderIsRobust(bytes, decodeBackendAnnounce)
})

void test("given_truncated_bit_flipped_or_trailing_corrupted_bytes_when_decoding_a_hello_reply_then_should_never_crash_unstructured", async () => {
  const bytes = await readFixture("hello_reply_features.bin")
  assertDecoderIsRobust(bytes, decodeHelloReply)
})

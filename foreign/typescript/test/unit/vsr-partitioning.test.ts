import assert from "node:assert/strict"
import { test } from "node:test"
import { xxHash32 } from "../../src/iggy/apache-iggy.js"

const encoder = new TextEncoder()

void test("given_the_rust_xxhash_vectors_when_hashed_then_should_match_partition_key_results", () => {
  assert.equal(xxHash32(new Uint8Array()), 0x02cc5d05)
  assert.equal(xxHash32(encoder.encode("a")), 0x550d7456)
  assert.equal(xxHash32(encoder.encode("hello")), 0xfb0077f9)
  assert.equal(xxHash32(encoder.encode("The quick brown fox jumps over the lazy dog")), 0xe85ea4de)
})

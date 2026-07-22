import assert from "node:assert/strict"
import { test } from "node:test"
import { Rng, resolveConnectionString, streamFor } from "../src/common.js"

void test("given_the_canonical_seed_when_generating_values_then_should_match_cross_language_vectors", () => {
  const rng = new Rng(0x123456789abcdef0n)
  assert.deepEqual(
    Array.from({ length: 4 }, () => rng.nextU64().toString(16)),
    ["fa2854001aa80c5c", "c12e42f8d265c7d2", "471225b948609d82", "8499eaaa3223c11d"]
  )
})

void test("given_connection_environment_when_resolved_then_should_preserve_shared_conventions", () => {
  assert.equal(resolveConnectionString({}), "iggy://iggy:iggy@127.0.0.1:8090")
  assert.equal(
    resolveConnectionString({
      LASER_SERVER: "cloud.example",
      LASER_TOKEN: "secret"
    }),
    "iggy+tcp://secret@cloud.example:8090"
  )
  assert.equal(streamFor("interop", {}), "laser-interop")
  assert.equal(streamFor("interop", { LASER_STREAM: "tenant-stream" }), "tenant-stream")
})

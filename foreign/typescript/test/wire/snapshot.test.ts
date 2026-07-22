import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { ConversationId } from "../../src/wire/ids.js"
import {
  decodeFoldSnapshot,
  encodeFoldSnapshot,
  foldSnapshotResumeOffset
} from "../../src/wire/snapshot.js"

const FIXTURE = path.resolve(process.cwd(), "../../wire/fixtures/fold_snapshot.bin")

void test("given_the_fold_snapshot_fixture_when_decoded_then_should_preserve_offsets_and_state", async () => {
  const buffer = await readFile(FIXTURE)
  const bytes = new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
  const snapshot = decodeFoldSnapshot(
    expectMap(decodeOne(bytes, "fold_snapshot"), "fold_snapshot"),
    "fold_snapshot"
  )
  assert.deepEqual(Array.from(snapshot.asOf), [
    [0, 41n],
    [1, 9n]
  ])
  assert.equal(foldSnapshotResumeOffset(snapshot, 0), 42n)
  assert.equal(foldSnapshotResumeOffset(snapshot, 2), 0n)
  assert.deepEqual(Buffer.from(snapshot.state), Buffer.from('{"folded":true}'))
  assert.deepEqual(Buffer.from(encodeNamed(encodeFoldSnapshot(snapshot))), Buffer.from(bytes))
})

void test("given_the_maximum_folded_offset_when_resumed_then_should_saturate", () => {
  const snapshot = {
    conversation: ConversationId.fromU128(1n),
    asOf: new Map([[0, (1n << 64n) - 1n]]),
    state: new Uint8Array()
  }
  assert.equal(foldSnapshotResumeOffset(snapshot, 0), (1n << 64n) - 1n)
})

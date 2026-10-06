import assert from "node:assert/strict"
import { test } from "node:test"
import { CodecError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { ConversationState, resumeOffsets, type ReplayBound } from "../../src/conversation-state.js"
import type { SnapshotStore } from "../../src/snapshot.js"
import { ConversationId as SdkConversationId } from "../../src/types/ids.js"
import { ConversationId } from "../../src/wire/ids.js"

void test("given_a_snapshot_when_resuming_then_should_start_after_each_folded_offset", () => {
  const offsets = resumeOffsets({
    conversation: ConversationId.fromU128(1n),
    asOf: new Map([
      [0, 899n],
      [2, 41n]
    ]),
    state: new Uint8Array()
  })
  assert.deepEqual(
    offsets,
    new Map([
      [0, 900n],
      [2, 42n]
    ])
  )
})

// One topic, one partition holding 25,000 records. The stub records where each
// read starts and stops instead of serving records.
function readStarts(): { readonly laser: Laser; readonly reads: [bigint, bigint][] } {
  const reads: [bigint, bigint][] = []
  const tail = new Map([[0, 25_000n]])
  const laser = {
    topic: () => ({
      partitionCount: () => Promise.resolve(1),
      tailOffsets: () => Promise.resolve(tail),
      replay: () => {
        const cursor = {
          offsets: new Map([[0, 0n]]),
          start: 0n,
          batch() {
            return cursor
          },
          fromOffsets(starts: ReadonlyMap<number, bigint>) {
            cursor.start = starts.get(0) ?? 0n
            return cursor
          },
          until(ends: ReadonlyMap<number, bigint>) {
            reads.push([cursor.start, ends.get(0) ?? 0n])
            return cursor
          },
          poll: () => Promise.resolve([]),
          pollRecords: () => Promise.resolve([])
        }
        return Promise.resolve(cursor)
      }
    })
  } as unknown as Laser
  return { laser, reads }
}

void test("given_a_range_bound_longer_than_the_window_when_loaded_then_should_read_the_whole_range", async () => {
  const conversation = SdkConversationId.new()
  const bounds: readonly ReplayBound[] = [
    { kind: "full" },
    { kind: "from-offsets", offsets: new Map([[0, 7n]]) }
  ]
  const expected: readonly [bigint, bigint][] = [
    [0n, 25_000n],
    [7n, 25_000n]
  ]
  for (const [index, bound] of bounds.entries()) {
    const { laser, reads } = readStarts()
    await ConversationState.load(laser, conversation, ["agent.commands"], bound, 0, (n) => n + 1)
    assert.deepEqual(reads, [expected[index]])
  }
})

void test("given_a_last_bound_when_loaded_then_should_keep_the_context_window", async () => {
  const { laser, reads } = readStarts()
  await ConversationState.load(
    laser,
    SdkConversationId.new(),
    ["agent.commands"],
    { kind: "last", count: 3 },
    0,
    (n) => n + 1
  )
  assert.deepEqual(reads, [[15_000n, 25_000n]])
})

function snapshotOf(state: string): SnapshotStore {
  return {
    latest: () =>
      Promise.resolve({
        conversation: ConversationId.fromU128(1n),
        asOf: new Map([[0, 4n]]),
        state: new TextEncoder().encode(state)
      }),
    save: () => Promise.resolve()
  }
}

void test("given_a_json_snapshot_when_loaded_without_a_decoder_then_should_seed_from_its_json_state", async () => {
  const { laser, reads } = readStarts()
  const state = await ConversationState.loadWith(
    laser,
    snapshotOf('{"count":4}'),
    SdkConversationId.new(),
    ["agent.commands"],
    { count: 0 },
    (current) => ({ count: current.count + 1 })
  )
  assert.deepEqual(state, { count: 4 })
  assert.deepEqual(reads, [[5n, 25_000n]])
})

void test("given_a_snapshot_that_is_not_json_when_loaded_without_a_decoder_then_should_raise_a_codec_error", async () => {
  const { laser } = readStarts()
  await assert.rejects(
    ConversationState.loadWith(
      laser,
      snapshotOf("not json"),
      SdkConversationId.new(),
      ["agent.commands"],
      0,
      (n) => n + 1
    ),
    CodecError
  )
  const decoded = await ConversationState.loadWith(
    laser,
    snapshotOf("not json"),
    SdkConversationId.new(),
    ["agent.commands"],
    "",
    (current) => current,
    (bytes) => new TextDecoder().decode(bytes)
  )
  assert.equal(decoded, "not json")
})

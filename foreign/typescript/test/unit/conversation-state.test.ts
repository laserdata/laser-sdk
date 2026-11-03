import assert from "node:assert/strict"
import { test } from "node:test"
import { CodecError, InvalidError } from "../../src/client/errors.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import {
  ConversationState,
  checkpointFromSnapshot,
  resumeOffsets,
  snapshotFromCheckpoint,
  type ReplayBound
} from "../../src/conversation-state.js"
import { Checkpoint } from "../../src/context.js"
import type { SnapshotStore } from "../../src/snapshot.js"
import { ConversationId as SdkConversationId } from "../../src/types/ids.js"
import { ConversationId } from "../../src/wire/ids.js"

const snapshotConversation = SdkConversationId.parse(ConversationId.fromU128(1n).toString())

void test("given_a_snapshot_when_resuming_then_should_start_after_each_folded_offset", () => {
  const offsets = resumeOffsets({
    stream: "agents",
    streamId: 0,
    streamCreatedAtMicros: 100n,
    conversation: ConversationId.fromU128(1n),
    fold: "planner",
    asOf: [
      { topicId: 2, topicCreatedAtMicros: 20n, partitionId: 0, offset: 899n },
      { topicId: 2, topicCreatedAtMicros: 20n, partitionId: 2, offset: 41n }
    ],
    state: new Uint8Array()
  })
  assert.deepEqual(offsets, [
    { topicId: 2, topicCreatedAtMicros: 20n, partitionId: 0, offset: 900n },
    { topicId: 2, topicCreatedAtMicros: 20n, partitionId: 2, offset: 42n }
  ])
})

// One topic, one partition holding 25,000 records. The stub records where each
// read starts and stops instead of serving records.
function readStarts(): { readonly laser: Laser; readonly reads: [bigint, bigint][] } {
  const reads: [bigint, bigint][] = []
  const tail = new Map([[0, 25_000n]])
  const laser = {
    defaultStream: "agents",
    [INTERNAL_TRANSPORT]: () => ({
      findSnapshotStream: () => Promise.resolve({ id: 0, createdAtMicros: 100n }),
      findSnapshotTopic: () => Promise.resolve({ id: 2, createdAtMicros: 20n, partitions: 1 })
    }),
    topic: () => ({
      partitionCount: () => Promise.resolve(1),
      tailOffsets: () => Promise.resolve(tail),
      replay: () => {
        const cursor = {
          partitions: [0],
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
    { kind: "from-offsets", offsets: new Map([["agent.sessions", new Map([[0, 7n]])]]) }
  ]
  const expected: readonly [bigint, bigint][] = [
    [0n, 25_000n],
    [7n, 25_000n]
  ]
  for (const [index, bound] of bounds.entries()) {
    const { laser, reads } = readStarts()
    await ConversationState.load(laser, conversation, ["agent.sessions"], bound, 0, (n) => n + 1)
    assert.deepEqual(reads, [expected[index]])
  }
})

void test("given_a_last_bound_when_loaded_then_should_keep_the_context_window", async () => {
  const { laser, reads } = readStarts()
  await ConversationState.load(
    laser,
    SdkConversationId.new(),
    ["agent.sessions"],
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
        stream: "agents",
        streamId: 0,
        streamCreatedAtMicros: 100n,
        conversation: ConversationId.fromU128(1n),
        fold: "planner",
        asOf: [{ topicId: 2, topicCreatedAtMicros: 20n, partitionId: 0, offset: 4n }],
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
    snapshotConversation,
    ["agent.sessions"],
    { count: 0 },
    (current) => ({ count: current.count + 1 })
  )
  assert.deepEqual(state, { count: 4 })
  assert.deepEqual(reads, [[5n, 25_000n]])
})

void test("given_a_changed_source_generation_when_resuming_then_should_refuse_the_snapshot", async () => {
  const { laser } = readStarts()
  const snapshot = await snapshotOf("4").latest(snapshotConversation)
  assert.ok(snapshot !== undefined)
  const first = snapshot.asOf[0]
  assert.ok(first !== undefined)
  await assert.rejects(
    checkpointFromSnapshot(laser, { ...snapshot, streamCreatedAtMicros: 101n }, ["agent.sessions"]),
    InvalidError
  )
  await assert.rejects(
    checkpointFromSnapshot(
      laser,
      { ...snapshot, asOf: [{ ...first, topicCreatedAtMicros: 21n }] },
      ["agent.sessions"]
    ),
    InvalidError
  )
})

void test("given_a_snapshot_that_is_not_json_when_loaded_without_a_decoder_then_should_raise_a_codec_error", async () => {
  const { laser } = readStarts()
  await assert.rejects(
    ConversationState.loadWith(
      laser,
      snapshotOf("not json"),
      snapshotConversation,
      ["agent.sessions"],
      0,
      (n) => n + 1
    ),
    CodecError
  )
  const decoded = await ConversationState.loadWith(
    laser,
    snapshotOf("not json"),
    snapshotConversation,
    ["agent.sessions"],
    "",
    (current) => current,
    (bytes) => new TextDecoder().decode(bytes)
  )
  assert.equal(decoded, "not json")
})

void test("given_a_checkpoint_when_snapshotted_then_should_record_each_folded_offset_and_round_trip", async () => {
  const { laser } = readStarts()
  const checkpoint = Checkpoint.fromJSON({
    per_topic: { "agent.sessions": { "0": 25_000, "1": 0 } }
  })
  assert.deepEqual(
    [...checkpoint.topics()].map(([topic, offsets]) => [topic, [...offsets]]),
    [
      [
        "agent.sessions",
        [
          [0, 25_000n],
          [1, 0n]
        ]
      ]
    ]
  )
  const snapshot = await snapshotFromCheckpoint(
    laser,
    snapshotConversation,
    "planner",
    checkpoint,
    new TextEncoder().encode("4")
  )
  assert.equal(snapshot.stream, "agents")
  assert.equal(snapshot.streamCreatedAtMicros, 100n)
  assert.equal(snapshot.conversation.toString(), snapshotConversation.toString())
  assert.deepEqual(snapshot.asOf, [
    { topicId: 2, topicCreatedAtMicros: 20n, partitionId: 0, offset: 24_999n }
  ])
  const resumed = await checkpointFromSnapshot(laser, snapshot, ["agent.sessions"])
  assert.equal(resumed.topicOffsets("agent.sessions")?.get(0), 25_000n)
})

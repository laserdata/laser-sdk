import assert from "node:assert/strict"
import { test } from "node:test"
import { NoStreamError } from "../../src/client/errors.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import type { PolledMessage } from "../../src/iggy/apache-iggy.js"
import { KvSnapshotStore, TopicSnapshotStore, encodeSnapshot } from "../../src/snapshot.js"
import type { ConsumerStart } from "../../src/stream/consumer-start.js"
import { ConversationId } from "../../src/types/ids.js"
import { ConversationId as WireConversationId } from "../../src/wire/ids.js"

const mine = ConversationId.new()
const other = ConversationId.new()

function record(offset: bigint, conversation: ConversationId, state: string): PolledMessage {
  return {
    offset,
    timestampMicros: offset,
    partitionId: 0,
    headers: new Map(),
    payload: encodeSnapshot({
      stream: "orbit",
      streamId: 0,
      streamCreatedAtMicros: 100n,
      conversation: WireConversationId.parse(conversation.toString()),
      fold: "planner",
      asOf: [{ topicId: 2, topicCreatedAtMicros: 20n, partitionId: 0, offset }],
      state: new TextEncoder().encode(state)
    })
  }
}

function storeOver(log: readonly PolledMessage[], stream: string | null = "orbit") {
  const reads: (bigint | "last")[] = []
  const transport = {
    findTopicPartitionCount: () => Promise.resolve(1),
    pollMessages: (
      _stream: string,
      _topic: string,
      _target: unknown,
      strategy: ConsumerStart,
      count: number
    ): Promise<readonly PolledMessage[]> => {
      if (strategy.kind === "last") {
        reads.push("last")
        return Promise.resolve(log.slice(-1))
      }
      const from = strategy.kind === "offset" ? strategy.value : 0n
      reads.push(from)
      return Promise.resolve(log.filter((message) => message.offset >= from).slice(0, count))
    }
  }
  const laser = {
    defaultStream: stream ?? undefined,
    [INTERNAL_TRANSPORT]: () => transport
  } as unknown as Laser
  return { store: new TopicSnapshotStore(laser, "planner"), reads }
}

function topicOf(length: number, matches: ReadonlyMap<bigint, string>): PolledMessage[] {
  return Array.from({ length }, (_, index) => {
    const offset = BigInt(index)
    const state = matches.get(offset)
    return state === undefined ? record(offset, other, "noise") : record(offset, mine, state)
  })
}

void test("given_a_recent_snapshot_when_the_latest_is_read_then_should_stop_in_the_tail_window", async () => {
  const log = topicOf(
    600,
    new Map([
      [10n, "old"],
      [590n, "new"]
    ])
  )
  const { store, reads } = storeOver(log)
  const latest = await store.latest(mine)
  assert.equal(new TextDecoder().decode(latest?.state), "new")
  assert.deepEqual(reads, ["last", 344n])
})

void test("given_an_old_snapshot_when_the_latest_is_read_then_should_walk_back_window_by_window", async () => {
  const { store, reads } = storeOver(topicOf(600, new Map([[10n, "only"]])))
  const latest = await store.latest(mine)
  assert.equal(new TextDecoder().decode(latest?.state), "only")
  assert.deepEqual(reads, ["last", 344n, 88n, 0n])
})

void test("given_no_snapshot_or_no_stream_when_the_latest_is_read_then_should_answer_none_or_refuse", async () => {
  assert.equal(await storeOver(topicOf(3, new Map())).store.latest(mine), undefined)
  await assert.rejects(storeOver([], null).store.latest(mine), NoStreamError)
})

void test("given_snapshots_on_two_partitions_when_read_then_should_use_broker_time", async () => {
  const partitions = [
    [{ ...record(100n, mine, "older"), timestampMicros: 10n, partitionId: 0 }],
    [{ ...record(2n, mine, "newer"), timestampMicros: 20n, partitionId: 1 }]
  ]
  const transport = {
    findTopicPartitionCount: () => Promise.resolve(2),
    pollMessages: (
      _stream: string,
      _topic: string,
      target: { readonly partitionId: number },
      strategy: ConsumerStart
    ) => {
      const messages = partitions[target.partitionId] ?? []
      return Promise.resolve(strategy.kind === "last" ? messages.slice(-1) : messages)
    }
  }
  const laser = {
    defaultStream: "orbit",
    [INTERNAL_TRANSPORT]: () => transport
  } as unknown as Laser
  const latest = await new TopicSnapshotStore(laser, "planner").latest(mine)
  assert.equal(new TextDecoder().decode(latest?.state), "newer")
})

void test("given_two_folds_in_one_conversation_when_saved_then_should_keep_both_snapshots", async () => {
  const values = new Map<string, Uint8Array>()
  const laser = {
    defaultStream: "orbit",
    kv: () => ({
      get: (key: Uint8Array) => Promise.resolve(values.get(new TextDecoder().decode(key))),
      set: (key: Uint8Array) => ({
        bytes: (payload: Uint8Array) => ({
          send: () => {
            values.set(new TextDecoder().decode(key), payload)
            return Promise.resolve()
          }
        })
      })
    })
  } as unknown as Laser
  const conversation = WireConversationId.parse(mine.toString())
  const planner = new KvSnapshotStore(laser, "planner")
  const worker = new KvSnapshotStore(laser, "worker")
  const base = {
    stream: "orbit",
    streamId: 0,
    streamCreatedAtMicros: 100n,
    conversation,
    asOf: [{ topicId: 2, topicCreatedAtMicros: 20n, partitionId: 0, offset: 4n }]
  }
  await planner.save({ ...base, fold: "planner", state: new TextEncoder().encode("planned") })
  await worker.save({ ...base, fold: "worker", state: new TextEncoder().encode("worked") })
  assert.equal(new TextDecoder().decode((await planner.latest(mine))?.state), "planned")
  assert.equal(new TextDecoder().decode((await worker.latest(mine))?.state), "worked")
  assert.equal(values.size, 2)
})

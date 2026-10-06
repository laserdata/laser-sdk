import assert from "node:assert/strict"
import { test } from "node:test"
import { NoStreamError } from "../../src/client/errors.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import type { PolledMessage } from "../../src/iggy/apache-iggy.js"
import { TopicSnapshotStore, encodeSnapshot } from "../../src/snapshot.js"
import type { ConsumerStart } from "../../src/stream/consumer-start.js"
import { ConversationId } from "../../src/types/ids.js"
import { ConversationId as WireConversationId } from "../../src/wire/ids.js"

const mine = ConversationId.new()
const other = ConversationId.new()

function record(offset: bigint, conversation: ConversationId, state: string): PolledMessage {
  return {
    offset,
    partitionId: 0,
    headers: new Map(),
    payload: encodeSnapshot({
      conversation: WireConversationId.parse(conversation.toString()),
      asOf: new Map([[0, offset]]),
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
  return { store: new TopicSnapshotStore(laser), reads }
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

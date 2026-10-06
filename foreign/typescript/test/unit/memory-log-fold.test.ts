import assert from "node:assert/strict"
import { test } from "node:test"

import { CodecError } from "../../src/client/errors.js"
import { INTERNAL_GOVERN, INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import type { HeaderValue } from "../../src/stream/header-value.js"
import { LogMemory } from "../../src/memory/log-memory.js"
import { MemoryId, MemoryKind, RecallStrategy, type MemoryItem } from "../../src/memory/types.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { encodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import { ConversationId } from "../../src/types/ids.js"
import { MEMORY_NAMESPACE } from "../../src/wire/headers.js"
import { encodeMemoryRecordFrame, type MemoryRecord } from "../../src/wire/memory.js"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

interface Stored {
  readonly payload: Uint8Array
  readonly headers: ReadonlyMap<string, HeaderValue>
}

// An in-process memory topic: writes land on partition 0, polls read by offset.
// `partitions` null models a topic that does not exist.
class FakeTopic {
  readonly governed: Uint8Array[] = []
  polls = 0

  constructor(
    readonly partitions: Stored[][] | null = [[]],
    private readonly governor: (payload: Uint8Array) => Uint8Array = (payload) => payload
  ) {}

  memory(namespace = "notes"): LogMemory {
    const transport = {
      findTopicPartitionCount: () => Promise.resolve(this.partitions?.length ?? undefined),
      sendMessageWithHeaders: (
        _stream: string,
        _topic: string,
        payload: Uint8Array,
        headers: ReadonlyMap<string, HeaderValue>
      ) => {
        this.partitions?.[0]?.push({ payload: payload.slice(), headers })
        return Promise.resolve({ confirmations: [] })
      },
      pollMessages: (
        _stream: string,
        _topic: string,
        target: { readonly partitionId: number },
        strategy: { readonly value: bigint },
        count: number
      ) => {
        this.polls += 1
        const from = Number(strategy.value)
        const records = this.partitions?.[target.partitionId] ?? []
        return Promise.resolve(
          records.slice(from, from + count).map((record, index) => ({
            ...record,
            partitionId: target.partitionId,
            offset: BigInt(from + index)
          }))
        )
      }
    }
    const laser = {
      defaultStream: "records",
      [INTERNAL_TRANSPORT]: () => transport,
      [INTERNAL_GOVERN]: (action: { readonly payload: Uint8Array }) => {
        this.governed.push(action.payload.slice())
        return Promise.resolve(this.governor(action.payload.slice()))
      }
    } as unknown as Laser
    return new LogMemory(laser, namespace)
  }
}

function record(
  record: MemoryRecord,
  conversation = ConversationId.derive("fold"),
  namespace = "notes"
): Stored {
  const headers = new Map(encodeProvenanceHeaders({ conversationId: conversation }))
  headers.set(MEMORY_NAMESPACE, { kind: "string", value: namespace })
  return { payload: encodeMemoryRecordFrame(record), headers }
}

function item(id: MemoryId, body: string): Stored {
  return record({
    kind: "item",
    id: id.toString(),
    memoryKind: MemoryKind.Fact,
    body: encoder.encode(body)
  })
}

function bodies(items: readonly MemoryItem[]): readonly string[] {
  return items.map((recalled) => decoder.decode(recalled.payload))
}

void test("given_a_governor_when_remembering_on_the_log_then_should_see_and_rewrite_the_item_body", async () => {
  const topic = new FakeTopic([[]], (payload) =>
    decoder.decode(payload) === "card 4111" ? encoder.encode("card [redacted]") : payload
  )
  const memory = topic.memory()
  await memory.remember({}, encoder.encode("card 4111"))
  assert.deepEqual(
    topic.governed.map((payload) => decoder.decode(payload)),
    ["card 4111"]
  )
  assert.deepEqual(bodies(await memory.recallFolded({}, {})), ["card [redacted]"])
})

void test("given_feedback_folded_before_its_item_when_recalled_folded_then_should_still_rank_the_item", async () => {
  const boosted = MemoryId.fromU128(1n)
  const plain = MemoryId.fromU128(2n)
  const topic = new FakeTopic([
    [record({ kind: "feedback", target: boosted.toString(), weight: 2 })],
    [item(boosted, "boosted"), item(plain, "plain")]
  ])
  const items = await topic.memory().recallFolded({}, {})
  assert.deepEqual(bodies(items), ["boosted", "plain"])
  assert.equal(items[0]?.score, 2)
})

void test("given_concurrent_folded_recalls_when_feedback_exists_then_should_count_it_once", async () => {
  const topic = new FakeTopic()
  const memory = topic.memory()
  const target = await memory.remember({}, encoder.encode("fact"))
  await memory.improve({}, { target, weight: 1 })
  await Promise.all([memory.recallFolded({}, {}), memory.recallFolded({}, {})])
  const items = await memory.recallFolded({}, {})
  assert.equal(items[0]?.score, 1)
})

void test("given_a_missing_memory_topic_when_folded_then_should_read_as_empty", async () => {
  const topic = new FakeTopic(null)
  const memory = topic.memory()
  assert.deepEqual(await memory.recallFolded({}, {}), [])
  assert.equal(await memory.fetchNamedFolded("plan"), undefined)
  assert.equal(topic.polls, 0)
})

void test("given_feedback_when_recalled_folded_then_should_order_recent_by_recency_and_label_feedback_auto", async () => {
  const older = MemoryId.fromU128(1n)
  const newer = MemoryId.fromU128(2n)
  const topic = new FakeTopic([
    [
      item(older, "older"),
      item(newer, "newer"),
      record({ kind: "feedback", target: older.toString(), weight: 5 })
    ]
  ])
  const memory = topic.memory()
  const recent = await memory.recallFolded({}, { strategy: RecallStrategy.Recent })
  assert.deepEqual(bodies(recent), ["newer", "older"])
  assert.deepEqual(
    recent.map((recalled) => recalled.signals),
    [[], []]
  )
  const keyword = await memory.recallFolded({}, { strategy: RecallStrategy.Keyword })
  assert.deepEqual(bodies(keyword), ["older", "newer"])
  assert.deepEqual(keyword[0]?.signals, [{ strategy: RecallStrategy.Auto, rank: 0, score: 5 }])
})

void test("given_no_namespace_when_a_log_memory_is_opened_then_should_key_named_items_on_the_topic_name", () => {
  const laser = { defaultStream: "records" } as unknown as Laser
  const audit = new LogMemory(laser)
  assert.equal(audit.topic, AgentTopic.Audit)
  assert.equal(audit.namespace, AgentTopic.Audit)
  assert.equal(new LogMemory(laser, undefined, "incidents").namespace, "incidents")
  assert.equal(new LogMemory(laser, "notes", "incidents").namespace, "notes")
})

void test("given_a_named_item_when_forgotten_then_should_tombstone_its_key_and_drop_it_from_the_fold", async () => {
  const topic = new FakeTopic()
  const memory = topic.memory()
  await memory.setNamed("plan", encoder.encode('{"step":1}'))
  await memory.forgetNamed("plan")
  assert.equal(await memory.fetchNamedFolded("plan"), undefined)
  assert.equal(await topic.memory().fetchNamedFolded("plan"), undefined)
})

void test("given_a_patch_that_is_not_json_when_updating_then_should_fail_with_a_codec_error", async () => {
  const memory = new FakeTopic().memory()
  await assert.rejects(memory.updateNamed("plan", encoder.encode("{not json")), CodecError)
  await memory.setNamed("broken", encoder.encode("not json either"))
  await assert.rejects(memory.updateNamed("broken", encoder.encode("{}")), CodecError)
})

void test("given_reserved_object_keys_when_merge_patching_then_should_keep_them_as_ordinary_fields", async () => {
  const memory = new FakeTopic().memory()
  await memory.setNamed("plan", encoder.encode('{"keep":true,"drop":1}'))
  await memory.updateNamed(
    "plan",
    encoder.encode('{"drop":null,"constructor":1,"prototype":2,"__proto__":{"a":1}}')
  )
  const merged = await memory.fetchNamedFolded("plan")
  assert.equal(
    decoder.decode(merged),
    '{"keep":true,"constructor":1,"prototype":2,"__proto__":{"a":1}}'
  )
})

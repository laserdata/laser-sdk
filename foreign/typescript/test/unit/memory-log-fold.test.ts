import assert from "node:assert/strict"
import { test } from "node:test"

import { CodecError, InvalidError } from "../../src/client/errors.js"
import {
  INTERNAL_GOVERN,
  INTERNAL_MEMORY_WRITES,
  INTERNAL_TRANSPORT
} from "../../src/client/internals.js"
import { MemoryWrites } from "../../src/memory/writes.js"
import type { Laser } from "../../src/client/laser.js"
import type { HeaderValue } from "../../src/stream/header-value.js"
import { LogMemory } from "../../src/memory/log-memory.js"
import { MemoryId, MemoryKind, RecallStrategy, type MemoryItem } from "../../src/memory/types.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { encodeProvenanceHeaders } from "../../src/provenance/provenance.js"
import { ConversationId } from "../../src/types/ids.js"
import { MEMORY_NAMESPACE } from "../../src/wire/headers.js"
import { encodeMemoryRecordFrame, type MemoryRecord } from "../../src/wire/memory.js"

// A fake client that sends every resource name as written.
const BARE_NAMING = {
  resourceNameIn: (_stream: string | undefined, name: string) => name,
  resourceScope: () => undefined
}

const encoder = new TextEncoder()
const decoder = new TextDecoder()

interface Stored {
  readonly payload: Uint8Array
  readonly headers: ReadonlyMap<string, HeaderValue>
  readonly timestampMicros?: bigint
}

// An in-process memory topic: writes land on partition 0, polls read by offset.
// `partitions` null models a topic that does not exist.
class FakeTopic {
  readonly governed: Uint8Array[] = []
  // The create settings each write handed the transport for a missing topic.
  readonly created: unknown[] = []
  polls = 0

  constructor(
    readonly partitions: Stored[][] | null = [[]],
    private readonly governor: (payload: Uint8Array) => Uint8Array = (payload) => payload
  ) {}

  memory(namespace = "notes"): LogMemory {
    const transport = {
      findTopicPartitionCount: () => Promise.resolve(this.partitions?.length ?? undefined),
      sendMessagesWithHeaders: (
        _stream: string,
        _topic: string,
        messages: readonly {
          readonly payload: Uint8Array
          readonly headers: ReadonlyMap<string, HeaderValue>
        }[],
        _key: unknown,
        _partition: unknown,
        options: { readonly createTopic?: unknown }
      ) => {
        this.created.push(options.createTopic)
        for (const { payload, headers } of messages)
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
    const writes = new MemoryWrites()
    const laser = {
      defaultStream: "records",
      ...BARE_NAMING,
      // Apache Iggy without a managed backend: no key-value view.
      capabilities: () => Promise.resolve({ kv: { available: false } }),
      [INTERNAL_TRANSPORT]: () => transport,
      [INTERNAL_MEMORY_WRITES]: () => writes,
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

function at(stored: Stored, timestampMicros: bigint): Stored {
  return { ...stored, timestampMicros }
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

const FACTS = [
  "the user is allergic to peanuts",
  "green tea every morning",
  "the user lives in Lisbon"
]
const CITY = "which city does the user live in"

async function remembered(): Promise<{ readonly memory: LogMemory; readonly ids: MemoryId[] }> {
  const memory = new FakeTopic().memory()
  const ids: MemoryId[] = []
  for (const fact of FACTS) ids.push(await memory.remember({}, encoder.encode(fact)))
  return { memory, ids }
}

void test("given_a_semantic_query_when_recalled_folded_then_should_rank_every_fact_by_word_overlap", async () => {
  const { memory } = await remembered()

  for (const strategy of [RecallStrategy.Semantic, RecallStrategy.Hybrid, undefined]) {
    const items = await memory.recallFolded(
      {},
      { semantic: CITY, ...(strategy === undefined ? {} : { strategy }) }
    )
    assert.deepEqual(bodies(items), [FACTS[2], FACTS[0], FACTS[1]], String(strategy))
    assert.deepEqual(
      items.map((item) => item.score),
      [3 / 7, 2 / 7, 0]
    )
    assert.deepEqual(
      items.map((item) => item.signals),
      [
        [{ strategy: RecallStrategy.Keyword, rank: 0, score: 3 / 7 }],
        [{ strategy: RecallStrategy.Keyword, rank: 1, score: 2 / 7 }],
        [{ strategy: RecallStrategy.Keyword, rank: 2, score: 0 }]
      ]
    )
  }
  const limited = await memory.recallFolded(
    {},
    { semantic: CITY, strategy: RecallStrategy.Semantic, limit: 1 }
  )
  assert.deepEqual(bodies(limited), [FACTS[2]])
})

void test("given_a_semantic_query_without_a_word_when_recalled_folded_then_should_return_the_newest_first_unranked", async () => {
  const { memory } = await remembered()

  const items = await memory.recallFolded({}, { semantic: "?!", strategy: RecallStrategy.Semantic })

  assert.deepEqual(bodies(items), [FACTS[2], FACTS[1], FACTS[0]])
  assert.deepEqual(
    items.map((item) => [item.score, item.signals]),
    [
      [undefined, []],
      [undefined, []],
      [undefined, []]
    ]
  )
})

void test("given_feedback_when_a_semantic_query_is_recalled_folded_then_should_add_the_weight_to_the_overlap", async () => {
  const { memory, ids } = await remembered()
  await memory.improve({}, { target: ids[1] ?? MemoryId.new(), weight: 1 })

  const items = await memory.recallFolded({}, { semantic: CITY, strategy: RecallStrategy.Semantic })

  const [top] = items
  assert.ok(top !== undefined, "the recall returned items")
  assert.deepEqual(bodies(items), [FACTS[1], FACTS[2], FACTS[0]])
  assert.equal(top.score, 1)
  assert.deepEqual(top.signals, [
    { strategy: RecallStrategy.Keyword, rank: 0, score: 0 },
    { strategy: RecallStrategy.Auto, rank: 0, score: 1 }
  ])
})

void test("given_a_keyword_or_recent_query_when_recalled_folded_then_should_filter_or_ignore_the_text", async () => {
  const { memory } = await remembered()

  const keyword = await memory.recallFolded(
    {},
    { semantic: "Lisbon", strategy: RecallStrategy.Keyword }
  )
  const recent = await memory.recallFolded(
    {},
    { semantic: "Lisbon", strategy: RecallStrategy.Recent }
  )

  assert.deepEqual(bodies(keyword), [FACTS[2]])
  assert.deepEqual(bodies(recent), [FACTS[2], FACTS[1], FACTS[0]])
})

void test("given_a_deployment_without_a_key_value_view_when_recalled_by_default_then_should_fold_the_topic", async () => {
  const topic = new FakeTopic()
  const memory = topic.memory()
  await memory.remember({ user: "ana" }, encoder.encode("prefers aisle seats"))
  await memory.setNamed("plan", encoder.encode('{"step":1}'))

  const recalled = await memory.recall({ user: "ana" }, {})
  const named = await memory.fetchNamed("plan")

  assert.deepEqual(bodies(recalled), ["prefers aisle seats"])
  assert.equal(decoder.decode(named), '{"step":1}')
})

void test("given_any_memory_write_when_sent_then_should_ask_for_a_thirty_day_topic_if_it_is_missing", async () => {
  const topic = new FakeTopic()
  const memory = topic.memory()
  const id = await memory.remember({}, encoder.encode("prefers aisle seats"))
  await memory.forget({}, id)
  await memory.setNamed("plan", encoder.encode("{}"))
  assert.deepEqual(topic.created, [
    { messageExpiryMicros: 2_592_000_000_000n },
    { messageExpiryMicros: 2_592_000_000_000n },
    { messageExpiryMicros: 2_592_000_000_000n }
  ])
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

void test("given_folded_log_memory_when_searched_then_should_exclude_unrelated_items_before_limiting", async () => {
  const topic = new FakeTopic([
    [
      item(MemoryId.fromU128(1n), "apple"),
      item(MemoryId.fromU128(2n), "orange"),
      item(MemoryId.fromU128(3n), "pear")
    ]
  ])
  const memory = topic.memory()
  for (const strategy of [RecallStrategy.Keyword, RecallStrategy.Semantic, RecallStrategy.Hybrid]) {
    assert.deepEqual(
      bodies(await memory.recallFolded({}, { semantic: "apple", strategy, limit: 1 })),
      ["apple"]
    )
  }
})

void test("given_no_namespace_when_a_log_memory_is_opened_then_should_key_named_items_on_the_topic_name", () => {
  const laser = { defaultStream: "records", ...BARE_NAMING } as unknown as Laser
  const memory = new LogMemory(laser)
  assert.equal(memory.topic, AgentTopic.Memory)
  assert.equal(memory.namespace, AgentTopic.Memory)
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

void test("given_scoped_tombstones_and_feedback_when_folded_then_should_touch_only_their_conversation", async () => {
  const owner = ConversationId.derive("owner")
  const other = ConversationId.derive("other")
  const id = MemoryId.fromU128(7n)
  const topic = new FakeTopic([
    [
      record(
        {
          kind: "item",
          id: id.toString(),
          memoryKind: MemoryKind.Fact,
          body: encoder.encode("kept")
        },
        owner
      ),
      record(
        { kind: "feedback", target: id.toString(), weight: 4, conversation: other.toString() },
        other
      ),
      record({ kind: "forget", target: id.toString(), conversation: other.toString() }, other),
      record(
        { kind: "feedback", target: id.toString(), weight: 2, conversation: owner.toString() },
        owner
      )
    ]
  ])
  const memory = topic.memory()
  const kept = await memory.recallFolded({}, { strategy: RecallStrategy.Keyword })
  assert.deepEqual(bodies(kept), ["kept"])
  assert.equal(kept[0]?.score, 2)
  await memory.forget({ conversation: owner }, id)
  assert.deepEqual(await memory.recallFolded({}, {}), [])
})

void test("given_an_item_remembered_after_its_forget_when_folded_then_should_recall_it_again", async () => {
  const id = MemoryId.fromU128(7n)
  const topic = new FakeTopic([
    [
      at(item(id, "before"), 100n),
      at(record({ kind: "forget", target: id.toString() }), 200n),
      at(item(id, "after"), 300n)
    ]
  ])
  assert.deepEqual(bodies(await topic.memory().recallFolded({}, {})), ["after"])
})

void test("given_partitions_folded_out_of_time_order_when_a_forget_lands_then_should_drop_only_older_writes", async () => {
  const newer = MemoryId.fromU128(1n)
  const older = MemoryId.fromU128(2n)
  const topic = new FakeTopic([
    [at(item(newer, "newer"), 300n)],
    [
      at(record({ kind: "forget", target: newer.toString() }), 200n),
      at(record({ kind: "forget", target: older.toString() }), 200n)
    ],
    [at(item(older, "older"), 100n)]
  ])
  assert.deepEqual(bodies(await topic.memory().recallFolded({}, {})), ["newer"])
})

void test("given_a_scope_on_another_stream_when_writing_log_memory_then_should_refuse_it", async () => {
  const topic = new FakeTopic()
  const memory = topic.memory()
  const elsewhere = { stream: "fleet" }
  await assert.rejects(
    memory.append(elsewhere, MemoryId.new(), MemoryKind.Fact, encoder.encode("fact")),
    InvalidError
  )
  await assert.rejects(memory.forget(elsewhere, MemoryId.new()), InvalidError)
  await assert.rejects(
    memory.improve(elsewhere, { target: MemoryId.new(), weight: 1 }),
    InvalidError
  )
  const written = topic.partitions?.[0]
  assert.ok(written !== undefined)
  assert.deepEqual(written, [], "nothing reached the topic")
  await memory.append(
    { stream: "records" },
    MemoryId.new(),
    MemoryKind.Fact,
    encoder.encode("fact")
  )
  assert.equal(written.length, 1, "the handle's own stream is accepted")
})

void test("given_a_named_key_over_the_memory_id_cap_when_written_then_should_refuse_it_before_sending", async () => {
  const topic = new FakeTopic()
  const memory = topic.memory()
  const key = "k".repeat(256)
  await assert.rejects(memory.setNamed(key, encoder.encode("value")), InvalidError)
  await assert.rejects(memory.forgetNamed(key), InvalidError)
  assert.equal(await memory.fetchNamedFolded(key), undefined, "nothing reached the topic")
})

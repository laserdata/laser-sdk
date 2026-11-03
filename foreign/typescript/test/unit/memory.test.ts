import assert from "node:assert/strict"
import { test } from "node:test"
import { CodecError, ConfigError, InvalidError } from "../../src/client/errors.js"
import { INTERNAL_GOVERN } from "../../src/client/internals.js"
import {
  AgentId,
  ConversationId,
  Lifetime,
  MemoryClass,
  MemoryHandle,
  MemoryId,
  MemoryKind,
  MemoryTopicBuilder,
  RecallStrategy,
  VectorMemory,
  fuseReciprocalRank,
  memoryClass,
  memoryItemJson,
  memoryItemText,
  memoryKindCode,
  toContextBlock,
  type Embedder,
  type Laser,
  type MemoryItem,
  type MemoryScope
} from "../../src/index.js"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

class TokenEmbedder implements Embedder {
  embed(text: string): Promise<readonly number[]> {
    const lower = text.toLowerCase()
    return Promise.resolve([
      lower.includes("auth") ? 1 : 0,
      lower.includes("latency") ? 1 : 0,
      lower.includes("job") ? 1 : 0
    ])
  }
}

function bodies(items: readonly MemoryItem[]): readonly string[] {
  return items.map((item) => decoder.decode(item.payload))
}

void test("given_a_memory_topic_ttl_when_built_then_should_configure_microseconds", async () => {
  const ensured: {
    stream?: string
    topic?: string
    partitions?: number
    messageExpiryMicros?: bigint
  } = {}
  const laser = {
    defaultStream: "laser-memory",
    stream(stream: string) {
      return {
        ensure: () => Promise.resolve(),
        topic(topic: string) {
          return {
            ensureWithExpiry(partitions: number, messageExpiryMicros?: bigint): Promise<void> {
              Object.assign(ensured, {
                stream,
                topic,
                partitions,
                messageExpiryMicros
              })
              return Promise.resolve()
            }
          }
        }
      }
    }
  } as unknown as Laser

  await MemoryTopicBuilder.create(laser, "incidents").partitions(4).ttl(86_400_000).build()

  assert.deepEqual(ensured, {
    stream: "laser-memory",
    topic: "incidents",
    partitions: 4,
    messageExpiryMicros: 86_400_000_000n
  })
})

void test("given_a_fractional_memory_topic_ttl_when_built_then_should_keep_it_to_the_microsecond", async () => {
  const expiries: (bigint | undefined)[] = []
  const laser = {
    defaultStream: "laser-memory",
    stream() {
      return {
        ensure: () => Promise.resolve(),
        topic() {
          return {
            ensureWithExpiry(_partitions: number, messageExpiryMicros?: bigint) {
              expiries.push(messageExpiryMicros)
              return Promise.resolve()
            }
          }
        }
      }
    }
  } as unknown as Laser

  await MemoryTopicBuilder.create(laser, "incidents").ttl(1.5).build()
  await MemoryTopicBuilder.create(laser, "incidents").ttl(1.1).build()

  assert.deepEqual(expiries, [1_500n, 1_100n])
  assert.throws(() => MemoryTopicBuilder.create(laser, "incidents").ttl(0), InvalidError)
  assert.throws(() => MemoryTopicBuilder.create(laser, "incidents").ttl(-1), InvalidError)
  assert.throws(() => MemoryTopicBuilder.create(laser, "incidents").ttl(Number.NaN), InvalidError)
})

void test("given_a_memory_topic_without_expiry_when_built_then_should_ensure_a_topic_that_never_expires", async () => {
  const expiries: (bigint | undefined)[] = []
  const laser = {
    defaultStream: "laser-memory",
    stream() {
      return {
        ensure: () => Promise.resolve(),
        topic() {
          return {
            ensureWithExpiry(_partitions: number, messageExpiryMicros?: bigint) {
              expiries.push(messageExpiryMicros)
              return Promise.resolve()
            }
          }
        }
      }
    }
  } as unknown as Laser

  await MemoryTopicBuilder.create(laser, "incidents").noExpiry().build()

  // No expiry lets Topic.ensure create the topic with the never-expire setting.
  assert.deepEqual(expiries, [undefined])
})

void test("given_a_memory_owner_when_content_id_is_minted_then_should_match_the_cross_sdk_vector", () => {
  const cases: readonly (readonly [MemoryScope, MemoryKind, string, bigint])[] = [
    [
      { agent: AgentId.new("agent") },
      MemoryKind.Fact,
      "63FZWE3WTSCVXAY845QK4TMVWM",
      0xc37ff8e1f35966faaf2085bcc9aa6f94n
    ],
    [
      { stream: "laser", agent: AgentId.new("agent"), user: "alice", app: "console" },
      MemoryKind.Fact,
      "28429K3B8MPSM8WHDQR474827P",
      0x48209331ad14b6688e45b7c10e4408f6n
    ],
    [
      { user: "alice" },
      MemoryKind.Procedure,
      "3E6HJGHKFDBMQ91TCJEHV28X40",
      0x6e346508cded5d2e90e9927476247480n
    ]
  ]
  for (const [owner, kind, rendered, value] of cases) {
    const id = MemoryId.content(owner, kind, encoder.encode("x"))
    assert.equal(id.asU128(), value)
    assert.equal(id.toString(), rendered)
  }
})

void test("given_two_users_when_the_same_body_is_addressed_then_should_produce_distinct_ids", () => {
  const owner = (user: string, app: string): MemoryScope => ({
    agent: AgentId.new("agent"),
    user,
    app
  })
  const id = (scope: MemoryScope) => MemoryId.content(scope, MemoryKind.Fact, encoder.encode("x"))
  const alice = id(owner("alice", "console"))
  assert.equal(alice.equals(id(owner("bob", "console"))), false)
  assert.equal(alice.equals(id(owner("alice", "support"))), false)
  assert.equal(alice.equals(id(owner("alicecon", "sole"))), false)
  assert.equal(
    alice.equals(id({ ...owner("alice", "console"), conversation: ConversationId.derive("c") })),
    true
  )
})

void test("given_memory_kinds_when_classified_then_should_use_the_shared_taxonomy", () => {
  assert.equal(memoryClass(MemoryKind.Message), MemoryClass.Episodic)
  assert.equal(memoryClass(MemoryKind.Procedure), MemoryClass.Procedural)
  assert.equal(memoryClass(MemoryKind.Entity), MemoryClass.Semantic)
})

void test("given_each_memory_kind_when_coded_then_should_match_the_rust_discriminators", () => {
  assert.deepEqual(
    [
      MemoryKind.Fact,
      MemoryKind.Message,
      MemoryKind.Summary,
      MemoryKind.Entity,
      MemoryKind.Feedback,
      MemoryKind.Procedure
    ].map(memoryKindCode),
    [1, 2, 3, 4, 5, 6]
  )
  const body = encoder.encode("x")
  assert.equal(
    MemoryId.content({}, MemoryKind.Fact, body).equals(
      MemoryId.content({}, MemoryKind.Summary, body)
    ),
    false
  )
})

void test("given_content_dedup_when_remembering_twice_then_should_store_one_item", async () => {
  const handle = MemoryHandle.vector(new TokenEmbedder())
  await handle.remember(encoder.encode("the budget is 5000")).dedup().send()
  await handle.remember(encoder.encode("the budget is 5000")).dedup().send()
  assert.equal((await handle.recall().fetch()).length, 1)
})

void test("given_fresh_ids_when_remembering_twice_then_should_store_two_items", async () => {
  const handle = MemoryHandle.vector(new TokenEmbedder())
  await handle.remember(encoder.encode("the budget is 5000")).send()
  await handle.remember(encoder.encode("the budget is 5000")).send()
  assert.equal((await handle.recall().fetch()).length, 2)
})

void test("given_two_items_when_recalling_recent_then_should_return_newest_first", async () => {
  const handle = MemoryHandle.vector(new TokenEmbedder())
  await handle.remember(encoder.encode("first")).send()
  await handle.remember(encoder.encode("second")).send()
  assert.deepEqual(bodies(await handle.recall().recent().limit(2).fetch()), ["second", "first"])
})

void test("given_positive_feedback_when_recalling_then_should_promote_the_target", async () => {
  const handle = MemoryHandle.vector(new TokenEmbedder())
  const cat = await handle.remember(encoder.encode("cat")).send()
  await handle.remember(encoder.encode("dog")).send()
  await handle.improve({}, { target: cat, weight: 5 })
  assert.deepEqual(bodies(await handle.recall().limit(2).fetch()), ["cat", "dog"])
})

void test("given_a_tombstone_when_recalling_then_should_remove_the_target", async () => {
  const handle = MemoryHandle.vector(new TokenEmbedder())
  await handle.remember(encoder.encode("keep")).send()
  const drop = await handle.remember(encoder.encode("drop")).send()
  await handle.forget({}, drop)
  assert.deepEqual(bodies(await handle.recall().fetch()), ["keep"])
})

void test("given_semantic_memory_when_keyword_and_hybrid_recall_run_then_should_rank_matches", async () => {
  const handle = MemoryHandle.vector(new TokenEmbedder())
  await handle.remember(encoder.encode("the job JOB-77 was retried")).send()
  await handle.remember(encoder.encode("a routine turn with nothing notable")).send()
  assert.equal(
    bodies(await handle.recall().keyword("INV-77").fetch())[0],
    "the job JOB-77 was retried"
  )

  const semantic = MemoryHandle.vector(new TokenEmbedder())
  await semantic.remember(encoder.encode("auth latency traces to the database pool")).send()
  await semantic.remember(encoder.encode("a routine turn with nothing notable")).send()
  assert.equal(
    bodies(await semantic.recall().hybrid("auth latency").fetch())[0],
    "auth latency traces to the database pool"
  )
})

void test("given_optional_scope_dimensions_when_unset_then_should_widen_recall", async () => {
  const memory = new VectorMemory(new TokenEmbedder())
  const first = ConversationId.derive("first")
  const second = ConversationId.derive("second")
  await memory.append(
    { user: "u1", conversation: first, lifetime: Lifetime.Session },
    MemoryId.new(),
    MemoryKind.Fact,
    encoder.encode("first")
  )
  await memory.append(
    { user: "u2", conversation: second, lifetime: Lifetime.Durable },
    MemoryId.new(),
    MemoryKind.Fact,
    encoder.encode("second")
  )
  assert.equal((await memory.recall({}, {})).length, 2)
  assert.deepEqual(bodies(await memory.recall({ user: "u1" }, {})), ["first"])
  assert.deepEqual(
    bodies(
      await memory.recall({ lifetime: Lifetime.Durable }, { strategy: RecallStrategy.Recent })
    ),
    ["second", "first"]
  )
})

void test("given_a_token_budget_when_rendering_context_then_should_keep_first_and_mark_omissions", () => {
  const conversationId = ConversationId.derive("context")
  const items: MemoryItem[] = ["first item", "second item"].map((body) => ({
    id: MemoryId.new(),
    payload: encoder.encode(body),
    provenance: { conversationId },
    kind: MemoryKind.Fact,
    signals: []
  }))
  assert.equal(toContextBlock(items, 1), "first item\n\n[... 1 more recalled item(s) omitted ...]")
})

void test("given_ranked_signals_when_fused_then_should_reward_agreement", () => {
  const conversationId = ConversationId.derive("rrf")
  const common = MemoryId.new()
  const item = (
    id: MemoryId,
    body: string,
    strategy: typeof RecallStrategy.Semantic
  ): MemoryItem => ({
    id,
    payload: encoder.encode(body),
    provenance: { conversationId },
    kind: MemoryKind.Fact,
    score: 1,
    signals: [{ strategy, rank: 0, score: 1 }]
  })
  const fused = fuseReciprocalRank(
    [
      [
        item(common, "common", RecallStrategy.Semantic),
        item(MemoryId.new(), "only", RecallStrategy.Semantic)
      ],
      [item(common, "common", RecallStrategy.Semantic)]
    ],
    2
  )
  assert.equal(decoder.decode(fused[0]?.payload), "common")
  assert.equal(fused[0]?.signals.length, 2)
})

void test("given_more_items_than_the_limit_when_consolidated_then_should_forget_the_oldest", async () => {
  const store = new VectorMemory(new TokenEmbedder())
  await store.append({}, MemoryId.fromU128(1n), MemoryKind.Fact, encoder.encode("first"))
  await store.append({}, MemoryId.fromU128(2n), MemoryKind.Fact, encoder.encode("second"))
  await store.append({}, MemoryId.fromU128(3n), MemoryKind.Fact, encoder.encode("third"))
  const handle = MemoryHandle.create(store)
  assert.deepEqual(await handle.consolidate({}, 2), {
    summarized: 0,
    reweighted: 0,
    pruned: 1,
    derived: 0
  })
  assert.deepEqual(bodies(await handle.recall().fetch()), ["third", "second"])
})

void test("given_ranked_lists_when_fused_then_should_score_each_item_by_its_own_signal_rank_and_keep_its_signals", () => {
  const conversationId = ConversationId.derive("rrf-rank")
  const item = (id: MemoryId, signals: MemoryItem["signals"]): MemoryItem => ({
    id,
    payload: encoder.encode(id.toString()),
    provenance: { conversationId },
    kind: MemoryKind.Fact,
    signals
  })
  const deep = MemoryId.fromU128(1n)
  const top = MemoryId.fromU128(2n)
  const unranked = MemoryId.fromU128(3n)
  const fused = fuseReciprocalRank(
    [
      [
        item(deep, [{ strategy: RecallStrategy.Semantic, rank: 9, score: 0.4 }]),
        item(top, [
          { strategy: RecallStrategy.Semantic, rank: 0, score: 0.9 },
          { strategy: RecallStrategy.Keyword, rank: 3, score: 0.5 }
        ]),
        item(unranked, [])
      ]
    ],
    3
  )
  assert.deepEqual(
    fused.map((fusedItem) => [fusedItem.id.asU128(), fusedItem.score]),
    [
      [2n, 1 / 60],
      [1n, 1 / 69],
      [3n, 0]
    ]
  )
  assert.deepEqual(fused[0]?.signals, [
    { strategy: RecallStrategy.Semantic, rank: 0, score: 0.9 },
    { strategy: RecallStrategy.Keyword, rank: 3, score: 0.5 }
  ])
})

void test("given_concurrent_deduplicated_writes_when_the_embedder_is_slow_then_should_store_one_item", async () => {
  let release: () => void = () => undefined
  const gate = new Promise<void>((resolve) => {
    release = resolve
  })
  const memory = new VectorMemory({
    embed: async () => {
      await gate
      return [1]
    }
  })
  const handle = MemoryHandle.create(memory)
  const writes = [
    handle.remember(encoder.encode("one fact")).dedup().send(),
    handle.remember(encoder.encode("one fact")).dedup().send()
  ]
  release()
  const ids = await Promise.all(writes)
  assert.equal(ids[0]?.toString(), ids[1]?.toString())
  assert.equal(memory.size(), 1)
})

void test("given_a_governed_vector_memory_without_an_embedder_when_remembering_then_should_fail_with_a_config_error", async () => {
  const laser = {
    defaultStream: "records",
    [INTERNAL_GOVERN]: (action: { readonly payload: Uint8Array }) =>
      Promise.resolve(action.payload.slice())
  } as unknown as Laser
  const memory = MemoryHandle.governedVector(laser)
  await assert.rejects(memory.remember(encoder.encode("fact")).send(), ConfigError)
  assert.deepEqual(await memory.recall().keyword("fact").fetch(), [])
})

void test("given_feedback_when_recalling_recent_then_should_keep_write_order", async () => {
  const handle = MemoryHandle.vector(new TokenEmbedder())
  const first = await handle.remember(encoder.encode("first")).send()
  await handle.remember(encoder.encode("second")).send()
  await handle.improve({}, { target: first, weight: 5 })
  const recent = await handle.recall().recent().fetch()
  assert.deepEqual(bodies(recent), ["second", "first"])
  assert.deepEqual(
    recent.map((item) => item.signals),
    [[], []]
  )
  assert.deepEqual(bodies(await handle.recall().fetch()), ["first", "second"])
})

void test("given_a_memory_item_when_read_as_text_or_json_then_should_decode_its_payload", () => {
  const item: MemoryItem = {
    id: MemoryId.fromU128(1n),
    payload: encoder.encode('{"n":1}'),
    provenance: { conversationId: ConversationId.new() },
    kind: MemoryKind.Fact,
    signals: []
  }
  assert.equal(memoryItemText(item), '{"n":1}')
  assert.deepEqual(memoryItemJson(item), { n: 1 })
  assert.equal(
    memoryItemJson(item, (value) => (value as { n: number }).n + 1),
    2
  )
  const broken = { ...item, payload: Uint8Array.of(0xff) }
  assert.equal(memoryItemText(broken), "\ufffd")
  assert.throws(() => memoryItemJson(broken), CodecError)
})

void test("given_vector_memory_when_another_conversation_forgets_or_improves_then_should_keep_the_item", async () => {
  const memory = new VectorMemory(new TokenEmbedder())
  const owner = ConversationId.derive("owner")
  const other = ConversationId.derive("other")
  const id = await memory.append(
    { conversation: owner },
    MemoryId.new(),
    MemoryKind.Fact,
    encoder.encode("kept")
  )
  await memory.improve({ conversation: other }, { target: id, weight: 3 })
  await memory.forget({ conversation: other }, id)
  const kept = await memory.recall({}, {})
  assert.deepEqual(bodies(kept), ["kept"])
  await memory.forget({ conversation: owner }, id)
  assert.deepEqual(await memory.recall({}, {}), [])
})

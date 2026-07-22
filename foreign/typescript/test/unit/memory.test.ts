import assert from "node:assert/strict"
import { test } from "node:test"
import {
  AgentId,
  ConversationId,
  Lifetime,
  MemoryClass,
  MemoryHandle,
  MemoryId,
  MemoryKind,
  RecallStrategy,
  VectorMemory,
  fuseReciprocalRank,
  memoryClass,
  toContextBlock,
  type Embedder,
  type MemoryItem
} from "../../src/index.js"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

class TokenEmbedder implements Embedder {
  embed(text: string): Promise<readonly number[]> {
    const lower = text.toLowerCase()
    return Promise.resolve([
      lower.includes("checkout") ? 1 : 0,
      lower.includes("latency") ? 1 : 0,
      lower.includes("invoice") ? 1 : 0
    ])
  }
}

function bodies(items: readonly MemoryItem[]): readonly string[] {
  return items.map((item) => decoder.decode(item.payload))
}

void test("given_a_memory_owner_when_content_id_is_minted_then_should_match_the_cross_sdk_vector", () => {
  const id = MemoryId.content({ agent: AgentId.new("agent") }, MemoryKind.Fact, encoder.encode("x"))
  assert.equal(id.toString(), "1A9GVS6SJ6SNS4KY0H19130WCW")
})

void test("given_memory_kinds_when_classified_then_should_use_the_shared_taxonomy", () => {
  assert.equal(memoryClass(MemoryKind.Message), MemoryClass.Episodic)
  assert.equal(memoryClass(MemoryKind.Procedure), MemoryClass.Procedural)
  assert.equal(memoryClass(MemoryKind.Entity), MemoryClass.Semantic)
})

void test("given_content_dedup_when_remembering_twice_then_should_store_one_item", async () => {
  const handle = MemoryHandle.vector()
  await handle.remember(encoder.encode("the budget is 5000")).dedup().send()
  await handle.remember(encoder.encode("the budget is 5000")).dedup().send()
  assert.equal((await handle.recall().fetch()).length, 1)
})

void test("given_fresh_ids_when_remembering_twice_then_should_store_two_items", async () => {
  const handle = MemoryHandle.vector()
  await handle.remember(encoder.encode("the budget is 5000")).send()
  await handle.remember(encoder.encode("the budget is 5000")).send()
  assert.equal((await handle.recall().fetch()).length, 2)
})

void test("given_two_items_when_recalling_recent_then_should_return_newest_first", async () => {
  const handle = MemoryHandle.vector()
  await handle.remember(encoder.encode("first")).send()
  await handle.remember(encoder.encode("second")).send()
  assert.deepEqual(bodies(await handle.recall().recent().limit(2).fetch()), ["second", "first"])
})

void test("given_positive_feedback_when_recalling_then_should_promote_the_target", async () => {
  const handle = MemoryHandle.vector()
  const cat = await handle.remember(encoder.encode("cat")).send()
  await handle.remember(encoder.encode("dog")).send()
  await handle.improve({}, { target: cat, weight: 5 })
  assert.deepEqual(bodies(await handle.recall().limit(2).fetch()), ["cat", "dog"])
})

void test("given_a_tombstone_when_recalling_then_should_remove_the_target", async () => {
  const handle = MemoryHandle.vector()
  await handle.remember(encoder.encode("keep")).send()
  const drop = await handle.remember(encoder.encode("drop")).send()
  await handle.forget({}, drop)
  assert.deepEqual(bodies(await handle.recall().fetch()), ["keep"])
})

void test("given_semantic_memory_when_keyword_and_hybrid_recall_run_then_should_rank_matches", async () => {
  const handle = MemoryHandle.vector(new TokenEmbedder())
  await handle.remember(encoder.encode("the invoice INV-77 was disputed")).send()
  await handle.remember(encoder.encode("a routine turn with nothing notable")).send()
  assert.equal(
    bodies(await handle.recall().keyword("INV-77").fetch())[0],
    "the invoice INV-77 was disputed"
  )

  const semantic = MemoryHandle.vector(new TokenEmbedder())
  await semantic.remember(encoder.encode("checkout latency traces to the database pool")).send()
  await semantic.remember(encoder.encode("a routine turn with nothing notable")).send()
  assert.equal(
    bodies(await semantic.recall().hybrid("checkout latency").fetch())[0],
    "checkout latency traces to the database pool"
  )
})

void test("given_optional_scope_dimensions_when_unset_then_should_widen_recall", async () => {
  const memory = new VectorMemory()
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
    ["second"]
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
  const handle = MemoryHandle.vector()
  await handle.remember(encoder.encode("first")).send()
  await handle.remember(encoder.encode("second")).send()
  await handle.remember(encoder.encode("third")).send()
  assert.deepEqual(await handle.consolidate({}, 2), { scanned: 3, kept: 2, forgotten: 1 })
  assert.deepEqual(bodies(await handle.recall().fetch()), ["third", "second"])
})

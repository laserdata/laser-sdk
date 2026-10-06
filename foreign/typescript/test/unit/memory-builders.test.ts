import assert from "node:assert/strict"
import { test } from "node:test"
import { MemoryHandle } from "../../src/memory/handle.js"
import {
  MemoryId,
  MemoryKind,
  RecallStrategy,
  type Feedback,
  type Memory,
  type MemoryItem,
  type MemoryQuery,
  type MemoryScope
} from "../../src/memory/types.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

// The builders accumulate scope and query, then hand both to the backend in one
// call. A recording backend is all it takes to assert what they composed.
class RecordingMemory implements Memory {
  readonly remembered: { scope: MemoryScope; payload: Uint8Array }[] = []
  readonly recalled: { scope: MemoryScope; query: MemoryQuery }[] = []
  readonly improved: { scope: MemoryScope; feedback: Feedback }[] = []
  readonly forgotten: { scope: MemoryScope; id: MemoryId }[] = []
  items: readonly MemoryItem[] = []

  remember(scope: MemoryScope, payload: Uint8Array): Promise<MemoryId> {
    this.remembered.push({ scope, payload })
    return Promise.resolve(MemoryId.new())
  }

  recall(scope: MemoryScope, query: MemoryQuery): Promise<readonly MemoryItem[]> {
    this.recalled.push({ scope, query })
    return Promise.resolve(this.items)
  }

  improve(scope: MemoryScope, feedback: Feedback): Promise<MemoryId> {
    this.improved.push({ scope, feedback })
    return Promise.resolve(MemoryId.new())
  }

  forget(scope: MemoryScope, id: MemoryId): Promise<void> {
    this.forgotten.push({ scope, id })
    return Promise.resolve()
  }
}

function item(text: string, score?: number): MemoryItem {
  return {
    id: MemoryId.new(),
    payload: new TextEncoder().encode(text),
    provenance: { conversationId: ConversationId.new() },
    kind: MemoryKind.Fact,
    ...(score === undefined ? {} : { score }),
    signals: []
  }
}

void test("given_every_remember_option_when_sent_then_should_hand_the_backend_one_composed_scope", async () => {
  const backend = new RecordingMemory()
  const memory = MemoryHandle.custom(backend)
  const conversation = ConversationId.new()
  const agent = AgentId.new("triage")

  await memory
    .remember(new TextEncoder().encode("sits in the eu-west pool"))
    .scope(conversation)
    .user("u-42")
    .agent(agent)
    .application("fleet-ops")
    .stream("support")
    .durable()
    .kind(MemoryKind.Procedure)
    .dedup()
    .send()

  const [written] = backend.remembered
  assert.equal(backend.remembered.length, 1)
  assert.ok(written, "the builder sent exactly one remember")
  assert.equal(written.scope.conversation, conversation)
  assert.equal(written.scope.user, "u-42")
  assert.equal(written.scope.agent, agent)
  assert.equal(written.scope.app, "fleet-ops")
  assert.equal(written.scope.stream, "support")
  assert.ok(written.scope.lifetime, "durable() sets the lifetime")
})

void test("given_every_recall_option_when_fetched_then_should_hand_the_backend_one_composed_query", async () => {
  const backend = new RecordingMemory()
  const memory = MemoryHandle.custom(backend)
  const conversation = ConversationId.new()

  await memory
    .recall(conversation)
    .user("u-42")
    .agent(AgentId.new("triage"))
    .application("fleet-ops")
    .stream("support")
    .hybrid("seating preference")
    .limit(5)
    .fetch()

  const [read] = backend.recalled
  assert.ok(read, "the builder sent exactly one recall")
  assert.equal(read.scope.conversation, conversation)
  assert.equal(read.query.semantic, "seating preference")
  assert.equal(read.query.strategy, RecallStrategy.Hybrid)
  assert.equal(read.query.limit, 5)
  assert.equal(read.scope.stream, "support")
})

void test("given_each_recall_strategy_when_selected_then_should_set_that_ranking", async () => {
  const backend = new RecordingMemory()
  const memory = MemoryHandle.custom(backend)

  await memory.recall().recent().fetch()
  await memory.recall().semantic("why is auth slow").fetch()
  await memory.recall().keyword("auth").fetch()
  await memory.recall().strategy(RecallStrategy.Auto).fetch()

  assert.deepEqual(
    backend.recalled.map((call) => call.query.strategy),
    [RecallStrategy.Recent, RecallStrategy.Semantic, RecallStrategy.Keyword, RecallStrategy.Auto]
  )
})

void test("given_recalled_items_when_rendered_as_a_block_then_should_join_them_under_the_budget", async () => {
  const backend = new RecordingMemory()
  backend.items = [item("sits in the eu-west pool"), item("rotates keys monthly")]
  const memory = MemoryHandle.custom(backend)

  const block = await memory.recall().limit(2).block()
  assert.match(block, /sits in the eu-west pool/u)

  const budgeted = await memory.context({}, { tokenBudget: 4 })
  assert.ok(budgeted.length > 0, "a budget of four tokens still keeps one item")
})

void test("given_a_token_budget_when_a_recall_is_rendered_as_a_block_then_should_bound_the_block_by_it", async () => {
  const backend = new RecordingMemory()
  backend.items = [item("sits in the eu-west pool"), item("rotates keys monthly")]
  const memory = MemoryHandle.custom(backend)

  const bounded = await memory.recall().block(1)
  assert.equal(bounded, "sits in the eu-west pool\n\n[... 1 more recalled item(s) omitted ...]")
  const unbounded = await memory.recall().block()
  assert.equal(unbounded, "sits in the eu-west pool\n\nrotates keys monthly")
})

void test("given_feedback_and_a_tombstone_when_applied_then_should_reach_the_backend_verbatim", async () => {
  const backend = new RecordingMemory()
  const memory = MemoryHandle.custom(backend)
  const target = MemoryId.new()
  const scope: MemoryScope = { conversation: ConversationId.new() }

  await memory.improve(scope, { target, weight: 1 })
  await memory.forget(scope, target)

  const [improved] = backend.improved
  const [forgotten] = backend.forgotten
  assert.ok(improved, "improve() reached the backend")
  assert.ok(forgotten, "forget() reached the backend")
  assert.deepEqual(improved.feedback, { target, weight: 1 })
  assert.equal(forgotten.id, target)
})

void test("given_a_stale_scope_when_consolidated_then_should_forget_everything_past_the_ceiling", async () => {
  const backend = new RecordingMemory()
  backend.items = [item("newest"), item("middle"), item("oldest")]
  const memory = MemoryHandle.custom(backend)

  const report = await memory.consolidate({}, 1)

  assert.deepEqual(report, { summarized: 0, reweighted: 0, pruned: 2, derived: 0 })
  assert.equal(backend.forgotten.length, 2)
})

void test("given_items_in_backend_order_when_consolidated_then_should_prune_by_id_age", async () => {
  const backend = new RecordingMemory()
  const oldest = { ...item("oldest"), id: MemoryId.fromU128(1n) }
  const middle = { ...item("middle"), id: MemoryId.fromU128(2n) }
  const newest = { ...item("newest"), id: MemoryId.fromU128(3n) }
  backend.items = [oldest, newest, middle]
  const report = await MemoryHandle.custom(backend).consolidate({}, 1)
  assert.deepEqual(
    backend.forgotten.map(({ id }) => id.asU128()),
    [1n, 2n]
  )
  assert.deepEqual(report, { summarized: 0, reweighted: 0, pruned: 2, derived: 0 })
  assert.deepEqual(backend.items, [oldest, newest, middle])
})

void test("given_a_reranker_when_attached_then_should_reorder_semantic_recall_only", async () => {
  const backend = new RecordingMemory()
  backend.items = [item("second", 0.1), item("first", 0.9)]
  const reranked = MemoryHandle.custom(backend).reranker({
    rerank: (_query, items) =>
      Promise.resolve([...items].sort((left, right) => (right.score ?? 0) - (left.score ?? 0)))
  })

  const ranked = await reranked.recall().semantic("anything").fetch()
  assert.deepEqual(
    ranked.map((hit) => new TextDecoder().decode(hit.payload)),
    ["first", "second"]
  )

  const untouched = await reranked.recall().recent().fetch()
  assert.deepEqual(
    untouched.map((hit) => new TextDecoder().decode(hit.payload)),
    ["second", "first"]
  )
})

void test("given_a_custom_backend_when_asked_for_the_log_handle_then_should_report_none", () => {
  assert.equal(MemoryHandle.custom(new RecordingMemory()).logBackend(), undefined)
})

void test("given_a_folded_recall_when_the_backend_has_no_separate_fold_then_should_use_its_own_recall", async () => {
  const backend = new RecordingMemory()
  backend.items = [item("only fact")]
  const memory = MemoryHandle.custom(backend)

  const folded = await memory.recall().folded().limit(1).fetch()

  assert.equal(folded.length, 1)
  assert.equal(backend.recalled.length, 1, "the fold delegates rather than inventing a second read")
})

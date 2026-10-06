import assert from "node:assert/strict"
import { test } from "node:test"

import { ProtocolError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { LogMemory } from "../../src/memory/log-memory.js"
import { MemoryHandle } from "../../src/memory/handle.js"
import {
  Lifetime,
  MemoryId,
  MemoryKind,
  RecallStrategy,
  type MemoryScope,
  type Embedder
} from "../../src/memory/types.js"
import { VectorMemory } from "../../src/memory/vector-memory.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import type { KvEntry, KvPage } from "../../src/wire/kv.js"

function entry(id: bigint, conversation: ConversationId, user: string, app: string): KvEntry {
  return {
    key: new TextEncoder().encode(MemoryId.fromU128(id).toString()),
    value: new TextEncoder().encode("item-" + id.toString()),
    version: 1n,
    scope: { kind: "fact", user, app, conversation: conversation.toString() }
  }
}

function memory(pages: readonly KvPage[]) {
  const calls: { conversation?: string; cursor?: Uint8Array }[] = []
  const laser = {
    defaultStream: "records",
    kv: () => ({
      scan: () => {
        const current: { conversation?: string; cursor?: Uint8Array } = {}
        const scan = {
          conversation: (value: string) => {
            current.conversation = value
            return scan
          },
          cursor: (value: Uint8Array) => {
            current.cursor = value
            return scan
          },
          fetch: () => {
            const page = pages[calls.length]
            calls.push(current)
            return page === undefined
              ? Promise.reject(new Error("unexpected extra page"))
              : Promise.resolve(page)
          }
        }
        return scan
      }
    })
  } as unknown as Laser
  return { handle: new LogMemory(laser, "notes"), calls }
}

void test("given_paged_user_and_application_memory_when_recalled_without_conversation_then_should_keep_newest_matching_items", async () => {
  const first = ConversationId.derive("first")
  const second = ConversationId.derive("second")
  const { handle, calls } = memory([
    {
      entries: [
        entry(1n, first, "reader", "diagnostics"),
        entry(2n, first, "other", "diagnostics")
      ],
      cursor: new Uint8Array([2])
    },
    {
      entries: [
        entry(3n, second, "reader", "other"),
        entry(4n, second, "reader", "diagnostics"),
        entry(5n, first, "reader", "diagnostics")
      ]
    }
  ])
  const items = await handle.recall({ user: "reader", app: "diagnostics" }, { limit: 2 })
  assert.deepEqual(
    items.map((item) => item.id.asU128()),
    [5n, 4n]
  )
  assert.equal(items[0]?.provenance.conversationId.toString(), first.toString())
  assert.equal(items[1]?.provenance.conversationId.toString(), second.toString())
  assert.deepEqual(calls, [{}, { cursor: new Uint8Array([2]) }])
})

void test("given_a_conversation_filter_when_the_read_view_contains_other_rows_then_should_keep_only_its_scope", async () => {
  const first = ConversationId.derive("first")
  const second = ConversationId.derive("second")
  const { handle, calls } = memory([
    {
      entries: [
        entry(1n, first, "reader", "diagnostics"),
        entry(2n, second, "reader", "diagnostics")
      ]
    }
  ])
  const items = await handle.recall({ conversation: first }, { limit: 2 })
  assert.deepEqual(
    items.map((item) => item.id.asU128()),
    [1n]
  )
  assert.equal(calls[0]?.conversation, first.toString())
})

void test("given_a_repeating_scan_cursor_when_memory_is_recalled_then_should_refuse_to_loop", async () => {
  const page = { entries: [], cursor: new Uint8Array([1]) }
  const { handle } = memory([page, page])
  await assert.rejects(handle.recall({}, {}), ProtocolError)
})

void test("given_builtin_vector_memory_when_consolidated_then_should_append_summary_kind_in_a_durable_scope", async () => {
  const store = new VectorMemory({ embed: () => Promise.resolve([1]) })
  const handle = MemoryHandle.create(store)
  const conversation = ConversationId.derive("summary")
  await handle
    .remember(new TextEncoder().encode("turn"))
    .scope(conversation)
    .kind(MemoryKind.Message)
    .send()
  await handle.consolidate({ conversation }, 10, {
    summarizer: { summarize: () => Promise.resolve(new TextEncoder().encode("summary")) }
  })
  const items = await store.recall({ conversation, lifetime: Lifetime.Durable }, {})
  const summaries = items.filter((item) => item.kind === MemoryKind.Summary)
  assert.equal(summaries.length, 1)
  assert.equal(new TextDecoder().decode(summaries[0]?.payload), "summary")
})

void test("given_an_optional_custom_append_when_consolidation_summarizes_then_should_forward_kind_and_scope", async () => {
  const writes: { scope: MemoryScope; kind: MemoryKind }[] = []
  const conversation = ConversationId.derive("custom-summary")
  const item = {
    id: MemoryId.fromU128(1n),
    payload: new Uint8Array([1]),
    kind: MemoryKind.Message,
    provenance: { conversationId: conversation },
    signals: []
  }
  const handle = MemoryHandle.custom({
    remember: () => Promise.reject(new Error("typed append was bypassed")),
    append: (scope, id, kind) => {
      writes.push({ scope, kind })
      return Promise.resolve(id)
    },
    recall: () => Promise.resolve([item]),
    forget: () => Promise.resolve(),
    improve: () => Promise.resolve(MemoryId.new())
  })
  await handle.consolidate({ conversation }, 10, {
    summarizer: { summarize: () => Promise.resolve(new Uint8Array([2])) }
  })
  const firstWrite = writes[0]
  assert.ok(firstWrite !== undefined)
  assert.equal(firstWrite.kind, MemoryKind.Summary)
  assert.equal(firstWrite.scope.lifetime, Lifetime.Durable)
})

void test("given_recent_strategy_with_semantic_text_when_recalled_then_should_ignore_the_embedder_and_isolate_streams", async () => {
  let embeddings = 0
  const embedder: Embedder = {
    embed: () => {
      embeddings += 1
      return Promise.resolve([1])
    }
  }
  const memory = new VectorMemory(embedder)
  await memory.remember({ stream: "one" }, new TextEncoder().encode("old"))
  await memory.remember({ stream: "one" }, new TextEncoder().encode("new"))
  await memory.remember({ stream: "two" }, new TextEncoder().encode("other"))
  const items = await memory.recall(
    { stream: "one" },
    { strategy: RecallStrategy.Recent, semantic: "ignored", limit: 1 }
  )
  assert.equal(new TextDecoder().decode(items[0]?.payload), "new")
  assert.equal(embeddings, 3)
})

void test("given_hybrid_signals_when_recalled_then_should_report_additive_scores_and_individual_ranks", async () => {
  const embedder: Embedder = { embed: (text) => Promise.resolve(text === "auth" ? [1, 0] : [0, 1]) }
  const memory = new VectorMemory(embedder)
  const first = await memory.remember({}, new TextEncoder().encode("auth"))
  const second = await memory.remember({}, new TextEncoder().encode("auth latency"))
  await memory.improve({}, { target: second, weight: 1.5 })
  const items = await memory.recall({}, { strategy: RecallStrategy.Hybrid, semantic: "auth" })
  assert.deepEqual(
    items.map((item) => item.id.asU128()),
    [second.asU128(), first.asU128()]
  )
  for (const item of items)
    assert.equal(
      item.score,
      item.signals.reduce((total, signal) => total + (signal.score ?? 0), 0)
    )
  assert.equal(
    items[0]?.signals.find((signal) => signal.strategy === RecallStrategy.Semantic)?.rank,
    1
  )
  assert.equal(
    items[1]?.signals.find((signal) => signal.strategy === RecallStrategy.Semantic)?.rank,
    0
  )
})

void test("given_turns_in_two_conversations_when_consolidated_then_should_store_one_summary_each", async () => {
  const first = ConversationId.derive("first")
  const second = ConversationId.derive("second")
  const agent = AgentId.new("planner")
  const turn = (id: bigint, conversation: ConversationId, body: string) => ({
    id: MemoryId.fromU128(id),
    payload: new TextEncoder().encode(body),
    kind: MemoryKind.Message,
    provenance: { conversationId: conversation },
    signals: []
  })
  const writes: { scope: MemoryScope; kind: MemoryKind; body: string }[] = []
  const summarized: string[][] = []
  const handle = MemoryHandle.custom({
    remember: () => Promise.reject(new Error("typed append was bypassed")),
    append: (scope, id, kind, payload) => {
      writes.push({ scope, kind, body: new TextDecoder().decode(payload) })
      return Promise.resolve(id)
    },
    recall: () =>
      Promise.resolve([
        turn(4n, first, "f2"),
        turn(3n, second, "s1"),
        turn(2n, first, "f1"),
        { ...turn(1n, second, "fact"), kind: MemoryKind.Fact }
      ]),
    forget: () => Promise.resolve(),
    improve: () => Promise.resolve(MemoryId.new())
  })
  const report = await handle.consolidate({ agent }, 10, {
    summarizer: {
      summarize: (bodies) => {
        const texts = bodies.map((body) => new TextDecoder().decode(body))
        summarized.push(texts)
        return Promise.resolve(new TextEncoder().encode(texts.join("+")))
      }
    }
  })
  assert.equal(report.summarized, 3)
  assert.deepEqual(summarized, [["f2", "f1"], ["s1"]])
  assert.deepEqual(
    writes.map(({ scope, kind, body }) => [
      scope.conversation?.toString(),
      scope.agent?.asStr(),
      scope.lifetime,
      kind,
      body
    ]),
    [
      [first.toString(), "planner", Lifetime.Durable, MemoryKind.Summary, "f2+f1"],
      [second.toString(), "planner", Lifetime.Durable, MemoryKind.Summary, "s1"]
    ]
  )

  const store = new VectorMemory({ embed: () => Promise.resolve([1]) })
  const vector = MemoryHandle.create(store)
  await vector.remember(new TextEncoder().encode("a")).scope(first).kind(MemoryKind.Message).send()
  await vector.remember(new TextEncoder().encode("b")).scope(second).kind(MemoryKind.Message).send()
  await vector.consolidate({}, 10, {
    summarizer: { summarize: () => Promise.resolve(new TextEncoder().encode("summary")) }
  })
  for (const conversation of [first, second]) {
    const summaries = (await store.recall({ conversation }, {})).filter(
      (item) => item.kind === MemoryKind.Summary
    )
    assert.equal(summaries.length, 1)
    assert.equal(summaries[0]?.provenance.conversationId.toString(), conversation.toString())
  }
})

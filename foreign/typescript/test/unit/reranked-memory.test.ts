import assert from "node:assert/strict"
import { test } from "node:test"

import { UnsupportedError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { MemoryHandle, RerankedMemory } from "../../src/memory/handle.js"
import { LogMemory } from "../../src/memory/log-memory.js"
import {
  MemoryId,
  MemoryKind,
  type Embedder,
  type Memory,
  type MemoryItem,
  type MemoryScope
} from "../../src/memory/types.js"
import { VectorMemory } from "../../src/memory/vector-memory.js"
import { ConversationId } from "../../src/types/ids.js"

const embedder = { embed: () => Promise.resolve([1]) }

void test("given_rerankers_when_a_typed_deduplicated_memory_item_is_written_then_should_preserve_its_id_and_kind", async () => {
  const conversation = ConversationId.derive("incident-42")
  const payload = new TextEncoder().encode("disk pressure")
  const identity = {
    rerank: (_query: string, items: readonly MemoryItem[]) => Promise.resolve(items)
  }
  const memory = MemoryHandle.vector(embedder).reranker(identity).reranker(identity)
  const remember = () =>
    memory.remember(payload).scope(conversation).kind(MemoryKind.Message).dedup().send()
  const first = await remember()
  const second = await remember()
  assert.equal(first.asU128(), second.asU128())
  assert.equal(
    first.asU128(),
    MemoryId.content({ conversation }, MemoryKind.Message, payload).asU128()
  )
  const items = await memory.recall(conversation).fetch()
  assert.equal(items.length, 1)
  assert.equal(items[0]?.kind, MemoryKind.Message)
})

void test("given_an_explicit_custom_backend_when_it_uses_a_known_store_then_should_keep_the_custom_contract", () => {
  const memory = MemoryHandle.custom(new VectorMemory(embedder))
  assert.equal(memory.backend, "custom")
  assert.throws(() => memory.set("plan", new Uint8Array([1])), UnsupportedError)
  const reranked = memory.reranker({ rerank: (_query, items) => Promise.resolve(items) })
  assert.equal(reranked.backend, "custom")
  assert.throws(() => reranked.fetch("plan"), UnsupportedError)
})

class ListMemory implements Memory {
  readonly appended: MemoryKind[] = []

  constructor(private readonly items: readonly MemoryItem[]) {}

  append(scope: MemoryScope, id: MemoryId, kind: MemoryKind): Promise<MemoryId> {
    assert.deepEqual(scope, {})
    this.appended.push(kind)
    return Promise.resolve(id)
  }

  remember(): Promise<MemoryId> {
    return Promise.resolve(MemoryId.new())
  }

  recall(): Promise<readonly MemoryItem[]> {
    return Promise.resolve(this.items)
  }

  improve(): Promise<MemoryId> {
    return Promise.resolve(MemoryId.new())
  }

  forget(): Promise<void> {
    return Promise.resolve()
  }
}

function listed(text: string): MemoryItem {
  return {
    id: MemoryId.new(),
    payload: new TextEncoder().encode(text),
    provenance: { conversationId: ConversationId.derive("rerank") },
    kind: MemoryKind.Fact,
    signals: []
  }
}

void test("given_a_reranked_memory_when_recalled_then_should_rerank_only_a_semantic_query", async () => {
  const inner = new ListMemory([listed("first"), listed("second")])
  const reversing = {
    rerank: (_query: string, items: readonly MemoryItem[]) => Promise.resolve([...items].reverse())
  }
  const memory = new RerankedMemory(inner, reversing)
  const text = (items: readonly MemoryItem[]) =>
    items.map((recalled) => new TextDecoder().decode(recalled.payload))
  assert.deepEqual(text(await memory.recall({}, {})), ["first", "second"])
  assert.deepEqual(text(await memory.recall({}, { semantic: "disk" })), ["second", "first"])
  const id = MemoryId.new()
  assert.equal(await memory.append({}, id, MemoryKind.Summary, new Uint8Array([1])), id)
  assert.deepEqual(inner.appended, [MemoryKind.Summary])
})

void test("given_an_embedder_when_registered_on_a_vector_handle_then_should_rebuild_the_index_over_it", async () => {
  const calls: string[] = []
  const counting = (name: string): Embedder => ({
    embed: () => {
      calls.push(name)
      return Promise.resolve([1])
    }
  })
  const conversation = ConversationId.derive("embedder")
  const original = MemoryHandle.vector(counting("original"))
  await original.remember(new TextEncoder().encode("kept")).scope(conversation).send()
  const configured = original.embedder(counting("replacement"))
  assert.equal(configured.backend, "vector")
  assert.deepEqual(await configured.recall(conversation).fetch(), [])
  await configured.remember(new TextEncoder().encode("fresh")).scope(conversation).send()
  assert.deepEqual(calls, ["original", "replacement"])

  const identity = {
    rerank: (_query: string, items: readonly MemoryItem[]) => Promise.resolve(items)
  }
  const reranked = MemoryHandle.vector(counting("original")).reranker(identity)
  const rebuilt = reranked.embedder(counting("reranked"))
  await rebuilt.remember(new TextEncoder().encode("ranked")).send()
  assert.equal(calls.at(-1), "reranked")
  assert.equal(rebuilt.backend, "vector")
})

void test("given_an_embedder_when_registered_on_a_log_or_custom_handle_then_should_leave_the_handle_unchanged", () => {
  const log = MemoryHandle.create(new LogMemory({ defaultStream: "records" } as unknown as Laser))
  assert.equal(log.embedder(embedder), log)
  const custom = MemoryHandle.custom(new ListMemory([]))
  assert.equal(custom.embedder(embedder), custom)
})

import assert from "node:assert/strict"
import { test } from "node:test"

import { UnsupportedError } from "../../src/client/errors.js"
import { MemoryHandle } from "../../src/memory/handle.js"
import { MemoryId, MemoryKind, type MemoryItem } from "../../src/memory/types.js"
import { VectorMemory } from "../../src/memory/vector-memory.js"
import { ConversationId } from "../../src/types/ids.js"

void test("given_rerankers_when_a_typed_deduplicated_memory_item_is_written_then_should_preserve_its_id_and_kind", async () => {
  const conversation = ConversationId.derive("incident-42")
  const payload = new TextEncoder().encode("disk pressure")
  const identity = {
    rerank: (_query: string, items: readonly MemoryItem[]) => Promise.resolve(items)
  }
  const memory = MemoryHandle.vector().reranker(identity).reranker(identity)
  const remember = () =>
    memory.remember(payload).conversation(conversation).kind(MemoryKind.Message).dedup().send()
  const first = await remember()
  const second = await remember()
  assert.equal(first.asU128(), second.asU128())
  assert.equal(
    first.asU128(),
    MemoryId.content({ conversation }, MemoryKind.Message, payload).asU128()
  )
  const items = await memory.recall().conversation(conversation).fetch()
  assert.equal(items.length, 1)
  assert.equal(items[0]?.kind, MemoryKind.Message)
})

void test("given_an_explicit_custom_backend_when_it_uses_a_known_store_then_should_keep_the_custom_contract", () => {
  const memory = MemoryHandle.custom(new VectorMemory())
  assert.equal(memory.backend, "custom")
  assert.throws(() => memory.set("plan", new Uint8Array([1])), UnsupportedError)
  const reranked = memory.reranker({ rerank: (_query, items) => Promise.resolve(items) })
  assert.equal(reranked.backend, "custom")
  assert.throws(() => reranked.fetch("plan"), UnsupportedError)
})

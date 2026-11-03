import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"
import { MemoryBackend } from "../../src/memory/handle.js"
import { ConversationId } from "../../src/types/ids.js"

function fakeClient(): IggyClient {
  return {
    clientProvider: () => Promise.resolve({ protocol: "vsr" }),
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
}

const embedder = { embed: () => Promise.resolve([1, 0]) }

void test("given_a_vector_backend_without_an_embedder_when_opened_then_should_refuse_at_open", async () => {
  await using laser = (await Laser.fromClient(fakeClient())).withDefaultStream("fleet")
  assert.throws(() => laser.memoryWith("notes", MemoryBackend.Vector), InvalidError)
  assert.throws(() => laser.memoryWith("notes", MemoryBackend.Log, embedder), InvalidError)
  assert.equal(laser.memoryWith("notes", MemoryBackend.Vector, embedder).backend, "vector")
  const scope = laser.context(ConversationId.new())
  assert.throws(() => scope.memoryWith("notes", MemoryBackend.Vector), InvalidError)
  assert.equal(scope.memoryWith("notes", MemoryBackend.Vector, embedder).handle.backend, "vector")
  assert.equal(scope.memoryWith("notes", MemoryBackend.Log).handle.backend, "log")
})

void test("given_a_scoped_search_when_no_limit_is_given_then_should_recall_up_to_fifty_folded_on_request", async () => {
  await using laser = (await Laser.fromClient(fakeClient())).withDefaultStream("fleet")
  const memory = laser.memoryWith("notes", MemoryBackend.Vector, embedder)
  const conversation = ConversationId.new()
  for (let index = 0; index < 60; index += 1) {
    await memory
      .remember(new TextEncoder().encode(`login note ${String(index)}`))
      .scope(conversation)
      .send()
  }
  const scoped = laser.context(conversation).memory(memory)
  assert.equal((await scoped.search("login")).length, 50)
  assert.equal((await scoped.search("login", { limit: 3, folded: true })).length, 3)
})

void test("given_a_token_budget_when_fetching_context_then_should_chain_last_n_before_the_budget", async () => {
  await using laser = (await Laser.fromClient(fakeClient())).withDefaultStream("fleet")
  const scope = laser.context(ConversationId.new())
  const policies: string[] = []
  scope.fetchWith = (_topics, policy) => {
    policies.push(policy.name?.() ?? "")
    return Promise.resolve([])
  }
  await scope.fetch(["agent.sessions"], 5)
  await scope.fetch(["agent.sessions"], 5, 100)
  assert.equal(await scope.block(["agent.sessions"], 5, 100), "")
  assert.deepEqual(policies, [
    "last_n(5)",
    "chain(last_n(5),token_budget(100))",
    "chain(last_n(5),token_budget(100))"
  ])
})

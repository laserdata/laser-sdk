import assert from "node:assert/strict"
import { test } from "node:test"
import { TimeoutError } from "../../src/client/errors.js"
import { isRetryable } from "../../src/client/error-classify.js"
import {
  INTERNAL_GOVERN,
  INTERNAL_MEMORY_WRITES,
  INTERNAL_TRANSPORT
} from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import { LogMemory } from "../../src/memory/log-memory.js"
import { MemoryId } from "../../src/memory/types.js"
import { MemoryWrites } from "../../src/memory/writes.js"
import { ConversationId } from "../../src/types/ids.js"
import type { KvEntry, KvPage } from "../../src/wire/kv.js"

const encoder = new TextEncoder()

function row(id: MemoryId, body: string, user = "ana", conversation = "first"): KvEntry {
  return {
    key: encoder.encode(id.toString()),
    value: encoder.encode(body),
    version: 1n,
    scope: { kind: "fact", user, conversation: ConversationId.derive(conversation).toString() }
  }
}

function page(rows: readonly KvEntry[]): KvPage {
  return { items: rows, page: 1, totalPages: 1, totalResults: rows.length }
}

// A log memory whose view answers `views` in turn, the last one from then on,
// over one connection's write record.
function memoryOver(
  views: readonly (readonly KvEntry[])[],
  writes = new MemoryWrites()
): { readonly memory: LogMemory; readonly scans: () => number } {
  let scans = 0
  const laser = {
    defaultStream: "records",
    resourceNameIn: (_stream: string | undefined, name: string) => name,
    resourceScope: () => undefined,
    capabilities: () => Promise.resolve({ kv: { available: true } }),
    [INTERNAL_GOVERN]: (action: { readonly payload: Uint8Array }) =>
      Promise.resolve(action.payload),
    [INTERNAL_MEMORY_WRITES]: () => writes,
    [INTERNAL_TRANSPORT]: () => ({
      sendMessagesWithHeaders: () => Promise.resolve({ confirmations: [] })
    }),
    kv: () => ({
      scan: () => {
        const scan = {
          conversation: () => scan,
          lensStream: () => scan,
          memoryLens: () => scan,
          page: () => scan,
          fetch: () => {
            const rows = views[Math.min(scans, views.length - 1)] ?? []
            scans += 1
            return Promise.resolve(page(rows))
          }
        }
        return scan
      }
    })
  } as unknown as Laser
  return { memory: new LogMemory(laser, "prefs"), scans: () => scans }
}

const first = { user: "ana", conversation: ConversationId.derive("first") }

void test("given_an_own_remember_the_view_has_not_applied_when_recalled_then_should_scan_again_until_it_appears", async () => {
  const id = MemoryId.new()
  const writes = new MemoryWrites()
  const { memory: writer } = memoryOver([[]], writes)
  await writer.append(first, id, "fact", encoder.encode("prefers aisle seats"))
  const { memory, scans } = memoryOver([[], [row(id, "prefers aisle seats")]], writes)

  const items = await memory.recall({ user: "ana" }, {})

  assert.deepEqual(
    items.map((item) => item.id.toString()),
    [id.toString()]
  )
  assert.equal(scans(), 2)
})

void test("given_an_own_forget_the_view_still_shows_when_recalled_then_should_scan_again_until_it_is_gone", async () => {
  const old = MemoryId.new()
  const corrected = MemoryId.new()
  const { memory, scans } = memoryOver([
    [row(old, "allergic to peanuts")],
    [row(old, "allergic to peanuts"), row(corrected, "allergic to shellfish", "ana", "second")],
    [row(corrected, "allergic to shellfish", "ana", "second")]
  ])
  await memory.append(
    { user: "ana", conversation: ConversationId.derive("second") },
    corrected,
    "fact",
    encoder.encode("allergic to shellfish")
  )
  await memory.forget({ user: "ana" }, old)

  const items = await memory.recall({ user: "ana" }, {})

  assert.deepEqual(
    items.map((item) => item.id.toString()),
    [corrected.toString()]
  )
  assert.equal(scans(), 3)
})

void test("given_an_own_forget_without_a_scope_when_recalled_through_any_lens_then_should_scan_again_until_it_is_gone", async () => {
  for (const lens of [{ user: "ana" }, first]) {
    const old = MemoryId.new()
    const { memory, scans } = memoryOver([[row(old, "allergic to peanuts")], []])
    await memory.forget({}, old)

    assert.deepEqual(await memory.recall(lens, {}), [])
    assert.equal(scans(), 2)
  }
})

void test("given_own_writes_outside_the_recall_lenses_when_recalled_then_should_not_wait_for_them", async () => {
  const { memory, scans } = memoryOver([[]])
  await memory.append(
    { user: "bob", conversation: first.conversation },
    MemoryId.new(),
    "fact",
    encoder.encode("x")
  )
  const id = MemoryId.new()
  await memory.append(first, id, "fact", encoder.encode("y"))
  await memory.forget({}, id)

  assert.deepEqual(await memory.recall({ user: "ana" }, {}), [])
  assert.equal(scans(), 1)
})

void test("given_own_writes_when_a_recall_sets_lenses_then_should_wait_for_the_remembers_it_covers_and_every_forget", () => {
  const writes = new MemoryWrites()
  const conversation = ConversationId.derive("first").toString()
  writes.record({ view: "v", id: "1", forgotten: false, conversation, user: "ana" })
  writes.record({ view: "v", id: "2", forgotten: false, conversation: "other", user: "bo" })
  writes.record({ view: "v", id: "3", forgotten: true })
  writes.record({ view: "elsewhere", id: "4", forgotten: true })
  const ids = (lenses: { readonly user?: string; readonly conversation?: string }): string[] =>
    writes.within("v", lenses).map((write) => write.id)

  assert.deepEqual(
    [ids({ user: "ana" }), ids({}), ids({ conversation: "another" })],
    [["1", "3"], ["1", "2", "3"], ["3"]]
  )
})

void test("given_an_own_write_the_view_never_applies_when_recalled_then_should_time_out_retryable_once", async () => {
  const { memory, scans } = memoryOver([[]], new MemoryWrites(120))
  await memory.append(first, MemoryId.new(), "fact", encoder.encode("prefers aisle seats"))

  await assert.rejects(
    memory.recall({ user: "ana" }, {}),
    (error: unknown) =>
      error instanceof TimeoutError &&
      isRetryable(error) &&
      error.message ===
        "timed out waiting for the memory view to apply this connection's writes: a folded recall reads the topic directly"
  )
  const waited = scans()
  assert.ok(waited > 1, `scanned ${String(waited)} times`)
  assert.deepEqual(await memory.recall({ user: "ana" }, {}), [])
  assert.equal(scans(), waited + 1)
})

void test("given_more_writes_than_the_cap_when_recorded_then_should_drop_the_oldest", () => {
  const writes = new MemoryWrites()
  const ids = Array.from({ length: 4_097 }, () => MemoryId.new().toString())
  for (const id of ids) writes.record({ view: "v", id, forgotten: false })

  const kept = writes.within("v", {})

  assert.equal(kept.length, 4_096)
  assert.equal(kept[0]?.id, ids[1])
})

import assert from "node:assert/strict"
import { mkdtemp, readdir, rm, stat } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { test } from "node:test"
import { FileStore, InMemoryStore, type StateStore } from "../../src/state-store.js"

async function assertRoundTrip(store: StateStore): Promise<void> {
  assert.equal(await store.get("missing"), undefined)
  await store.set("../unsafe/key", new TextEncoder().encode("value"))
  assert.equal(new TextDecoder().decode(await store.get("../unsafe/key")), "value")
  await store.delete("../unsafe/key")
  assert.equal(await store.get("../unsafe/key"), undefined)
}

void test("given_an_in_memory_store_when_used_then_should_copy_and_round_trip", async () => {
  const store = new InMemoryStore()
  const input = new Uint8Array([1, 2])
  await store.set("key", input)
  input[0] = 9
  assert.deepEqual(await store.get("key"), new Uint8Array([1, 2]))
  await assertRoundTrip(store)
})

void test("given_a_file_store_when_used_then_should_encode_keys_and_write_atomically", async () => {
  const root = await mkdtemp(join(tmpdir(), "laser-ts-state-"))
  try {
    await assertRoundTrip(new FileStore(root))
    assert.deepEqual(await readdir(root), [])
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})

void test(
  "given_a_file_store_when_written_then_should_restrict_its_directory_to_the_owner",
  {
    skip: process.platform === "win32"
  },
  async () => {
    const parent = await mkdtemp(join(tmpdir(), "laser-ts-state-"))
    const root = join(parent, "state")
    try {
      await new FileStore(root).set("cursor", new TextEncoder().encode("7"))
      assert.equal((await stat(root)).mode & 0o777, 0o700, "dedup keys and cursors stay private")
    } finally {
      await rm(parent, { recursive: true, force: true })
    }
  }
)

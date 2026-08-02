import assert from "node:assert/strict"
import { Given, Then, When } from "@cucumber/cucumber"
import { MemoryHandle, type Embedder } from "@laserdata/laser-sdk"
import type { LaserWorld } from "../world.js"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

class TokenEmbedder implements Embedder {
  embed(text: string): Promise<readonly number[]> {
    const vector = new Array<number>(64).fill(0)
    for (const token of text.toLowerCase().split(/[^\p{L}\p{N}]+/u).filter(Boolean)) {
      let hash = 0xcbf29ce484222325n
      for (const byte of encoder.encode(token)) {
        hash = ((hash ^ BigInt(byte)) * 0x100000001b3n) & 0xffffffffffffffffn
      }
      const index = Number(hash % 64n)
      vector[index] = (vector[index] ?? 0) + 1
    }
    return Promise.resolve(vector)
  }
}

Given("an empty memory store", function (this: LaserWorld) {
  this.memory = MemoryHandle.vector()
  this.memoryIds.clear()
})

Given("an empty semantic memory", function (this: LaserWorld) {
  this.memory = MemoryHandle.vector(new TokenEmbedder())
  this.memoryIds.clear()
})

When(/^I remember "([^"]+)" with dedup$/, async function (this: LaserWorld, body: string) {
  const id = await requireMemory(this).remember(encoder.encode(body)).dedup().send()
  this.memoryIds.set(body, id)
})

When(/^I remember "([^"]+)"$/, async function (this: LaserWorld, body: string) {
  const id = await requireMemory(this).remember(encoder.encode(body)).send()
  this.memoryIds.set(body, id)
})

When(/^I remember the fact "([^"]+)"$/, async function (this: LaserWorld, body: string) {
  const id = await requireMemory(this).remember(encoder.encode(body)).send()
  this.memoryIds.set(body, id)
})

When(
  /^I give "([^"]+)" a feedback weight of (-?\d+)$/,
  async function (this: LaserWorld, body: string, weight: string) {
    const target = this.memoryIds.get(body)
    assert.ok(target !== undefined)
    await requireMemory(this).improve({}, { target, weight: Number(weight) })
  }
)

When(/^I forget "([^"]+)"$/, async function (this: LaserWorld, body: string) {
  const target = this.memoryIds.get(body)
  assert.ok(target !== undefined)
  await requireMemory(this).forget({}, target)
})

Then(/^the memory holds (\d+) items?$/, async function (this: LaserWorld, count: string) {
  assert.equal((await requireMemory(this).recall().fetch()).length, Number(count))
})

Then(
  /^recalling (\d+) items? returns "([^"]+)" then "([^"]+)"$/,
  async function (this: LaserWorld, limit: string, first: string, second: string) {
    assert.deepEqual(await recalled(this, Number(limit)), [first, second])
  }
)

Then(
  /^recalling (\d+) items? returns "([^"]+)"$/,
  async function (this: LaserWorld, limit: string, only: string) {
    assert.deepEqual(await recalled(this, Number(limit)), [only])
  }
)

Then(
  /^keyword recall for "([^"]+)" returns "([^"]+)" first$/,
  async function (this: LaserWorld, query: string, expected: string) {
    const items = await requireMemory(this).recall().keyword(query).fetch()
    assert.equal(decoder.decode(items[0]?.payload), expected)
  }
)

Then(
  /^hybrid recall for "([^"]+)" returns "([^"]+)" first$/,
  async function (this: LaserWorld, query: string, expected: string) {
    const items = await requireMemory(this).recall().hybrid(query).fetch()
    assert.equal(decoder.decode(items[0]?.payload), expected)
  }
)

function requireMemory(world: LaserWorld): MemoryHandle {
  if (world.memory === undefined) throw new Error("scenario has no memory")
  return world.memory
}

async function recalled(world: LaserWorld, limit: number): Promise<readonly string[]> {
  return (await requireMemory(world).recall().recent().limit(limit).fetch()).map((item) =>
    decoder.decode(item.payload)
  )
}

import assert from "node:assert/strict"
import { Then, When } from "@cucumber/cucumber"
import { jsonCodec } from "@laserdata/laser-sdk"
import { eventual } from "../support/eventual.js"
import type { LaserWorld } from "../world.js"

interface Event {
  readonly id: number
}

function decodeEvent(value: unknown): Event {
  if (value === null || typeof value !== "object" || !("id" in value) || typeof value.id !== "number") {
    throw new TypeError("event requires a numeric id")
  }
  return { id: value.id }
}

When(/^I bootstrap the stream with (\d+) partitions$/, async function (this: LaserWorld, count: string) {
  await this.requireLaser().bootstrap(Number(count))
  this.bootstrapped = true
})

Then("the stream is ready", function (this: LaserWorld) {
  assert.equal(this.bootstrapped, true)
})

When(/^I publish a JSON event to topic "([^"]+)"$/, async function (this: LaserWorld, topic: string) {
  const target = this.requireLaser().topic(topic)
  await target.ensure(1)
  await target.json(jsonCodec(decodeEvent)).publish({ id: this.published })
  this.published += 1
  this.lastTopic = topic
})

When(
  /^I publish a batch of (\d+) JSON events to topic "([^"]+)"$/,
  async function (this: LaserWorld, countText: string, topic: string) {
    const count = Number(countText)
    const target = this.requireLaser().topic(topic)
    await target.ensure(1)
    this.published = await target
      .json(jsonCodec(decodeEvent))
      .publishBatch(Array.from({ length: count }, (_, id) => ({ id })))
    this.lastTopic = topic
  }
)

Then("the publish succeeds", async function (this: LaserWorld) {
  assert.ok(this.published > 0)
  const topic = this.lastTopic
  assert.ok(topic !== undefined)
  await eventual(async () => {
    const records = await this.requireLaser().topic(topic).replay()
    return (await records.poll()).length > 0 ? true : undefined
  }, "published record visibility")
})

Then(/^all (\d+) events are published$/, async function (this: LaserWorld, countText: string) {
  const count = Number(countText)
  assert.equal(this.published, count)
  const topic = this.lastTopic
  assert.ok(topic !== undefined)
  await eventual(async () => {
    const records = await this.requireLaser().topic(topic).replay({ batchSize: count })
    return (await records.poll()).length === count ? true : undefined
  }, "batch visibility")
})

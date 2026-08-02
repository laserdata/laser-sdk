import assert from "node:assert/strict"
import { test } from "node:test"

import type { LaserTransport } from "../../src/iggy/apache-iggy.js"
import { Consumer } from "../../src/stream/consumer.js"

void test("given_a_group_consumer_when_asynchronously_disposed_then_should_leave_once", async () => {
  let leaves = 0
  const transport = {
    leaveConsumerGroup(): Promise<void> {
      leaves += 1
      return Promise.resolve()
    }
  } as unknown as LaserTransport
  const consumer = new Consumer(
    transport,
    "stream",
    "topic",
    { kind: "group", name: "workers" },
    {}
  )

  await consumer[Symbol.asyncDispose]()
  await consumer[Symbol.asyncDispose]()

  assert.equal(leaves, 1)
})

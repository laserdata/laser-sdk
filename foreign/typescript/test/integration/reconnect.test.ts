import assert from "node:assert/strict"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import type { Consumer } from "../../src/stream/consumer.js"
import { TestIggy, TestIggyCluster } from "../support/test-iggy.js"

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

void test(
  "given_three_node_cluster_when_follower_and_leader_restart_then_same_sdk_handle_should_continue_streaming",
  { timeout: 90_000 },
  async () => {
    const cluster = await TestIggyCluster.start()
    let laser: Laser | undefined
    const running = { value: true }
    let sent = 0
    let observed = 0
    let worker: Promise<void> | undefined
    try {
      const [leader, follower] = await cluster.leaderAndFollower()
      cluster.routeEndpointTo(leader)
      const endpoint = `${cluster.endpoint}?reconnection_retries=unlimited&reconnection_interval=100ms`
      laser = await Laser.connectWithStream(endpoint, "rolling_restart")
      const topic = laser.topic("pulse")
      await topic.ensure(1)
      worker = (async () => {
        while (running.value) {
          try {
            await within(
              topic.send(new Uint8Array(new BigUint64Array([BigInt(sent + 1)]).buffer)),
              10_000
            )
            sent += 1
            const cursor = await topic.replay()
            observed += (await within(cursor.poll(), 10_000)).length
          } catch (error) {
            console.error("rolling TypeScript publish failed", error)
          }
          await delay(25)
        }
      })()

      await waitForProgress(() => sent, 1)
      await cluster.restartNode(follower)
      await waitForProgress(() => sent, sent + 1)
      await cluster.restartNode(leader)
      await waitForProgress(() => sent, sent + 1)
      assert.ok(observed > 0)
    } finally {
      running.value = false
      await worker
      if (laser !== undefined) await laser.close().catch(() => undefined)
      await cluster.close()
    }
  }
)

async function waitForProgress(current: () => number, expected: number): Promise<void> {
  const deadline = Date.now() + 30_000
  while (current() < expected) {
    assert.ok(Date.now() < deadline, "streaming did not resume before the deadline")
    await delay(50)
  }
}

void test(
  "given_a_server_restart_when_reusing_the_same_client_then_should_publish_and_consume_again",
  { timeout: 45_000 },
  async () => {
    const iggy = await TestIggy.start()
    let laser: Laser | undefined
    let consumer: Consumer | undefined
    try {
      const connection = `${iggy.endpoint}?reconnection_retries=40&reconnection_interval=250ms`
      const stream = "reconnect_it"
      laser = await Laser.connectWithStream(connection, stream)
      await within(laser.stream(stream).ensure(), 3_000)
      const topic = laser.topic("pulse")
      await within(topic.ensure(1), 3_000)
      consumer = await topic.consumerGroup("restart-workers", {
        batchLength: 10,
        startFrom: { kind: "first" },
        pollIntervalMs: 10
      })
      await within(topic.send(new TextEncoder().encode("before-restart")), 3_000)
      const before = await consumer.nextWithin(3_000)
      assert.equal(new TextDecoder().decode(before?.payload), "before-restart")
      await within(consumer.shutdown(), 3_000)
      consumer = undefined

      await iggy.restart()
      await delay(500)
      await within(
        (async () => {
          await laser.stream(stream).ensure()
          await topic.ensure(1)
          await topic.send(new TextEncoder().encode("after-restart"))
        })(),
        8_000
      )

      const cursor = await within(topic.replay(), 3_000)
      const records = await within(cursor.poll(), 3_000)
      assert.ok(
        records.some((record) => new TextDecoder().decode(record.payload) === "after-restart")
      )
      consumer = await topic.consumerGroup("restart-workers", {
        batchLength: 10,
        startFrom: { kind: "next" },
        pollIntervalMs: 10
      })
      const after = await within(consumer.nextWithin(5_000), 6_000)
      assert.equal(new TextDecoder().decode(after?.payload), "after-restart")
    } finally {
      try {
        if (consumer !== undefined) await within(consumer.shutdown(), 500).catch(() => undefined)
        if (laser !== undefined) await within(laser.close(), 500).catch(() => undefined)
      } finally {
        await iggy.close()
      }
    }
  }
)

function within<Value>(promise: Promise<Value>, timeoutMs: number): Promise<Value> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`operation timed out after ${String(timeoutMs)}ms`))
    }, timeoutMs)
    promise.then(
      (value) => {
        clearTimeout(timer)
        resolve(value)
      },
      (error: unknown) => {
        clearTimeout(timer)
        reject(error instanceof Error ? error : new Error(String(error)))
      }
    )
  })
}

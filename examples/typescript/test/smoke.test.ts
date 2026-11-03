import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "@laserdata/laser-sdk"

import { run as runInterop } from "../src/interop/main.js"
import { run as runNativeStreaming } from "../src/native-streaming/main.js"
import { run as runLog } from "../src/log/main.js"
import { run as runRecall } from "../src/recall/main.js"
import { run as runContext } from "../src/context/main.js"
import { run as runAgent } from "../src/agent/main.js"
import { run as runCdc } from "../src/cdc/main.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

async function withLaser<T>(name: string, run: (laser: Laser) => Promise<T>): Promise<T> {
  const streamName = `laser-ts-example-${name}-${randomUUID()}`
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, streamName)
  const stream = laser.stream(streamName)
  await stream.ensure()
  try {
    return await run(laser)
  } finally {
    await stream.delete()
  }
}

void test(
  "given_a_small_native_workload_when_run_then_should_complete_against_apache_iggy",
  { concurrency: false },
  async () => {
    const previousMessages = process.env["LASER_MESSAGES"]
    const previousBatch = process.env["LASER_BATCH"]
    process.env["LASER_MESSAGES"] = "24"
    process.env["LASER_BATCH"] = "8"
    try {
      const summary = await withLaser("native", (laser) =>
        runNativeStreaming(laser, AbortSignal.timeout(30_000))
      )
      assert.deepEqual(summary, { published: 24, automatic: 24, manual: 24 })
    } finally {
      if (previousMessages === undefined) delete process.env["LASER_MESSAGES"]
      else process.env["LASER_MESSAGES"] = previousMessages
      if (previousBatch === undefined) delete process.env["LASER_BATCH"]
      else process.env["LASER_BATCH"] = previousBatch
    }
  }
)

void test(
  "given_the_interop_agents_when_run_then_should_complete_every_bridge_flow",
  { concurrency: false, timeout: 45_000 },
  async () => {
    const summary = await withLaser("interop", (laser) =>
      runInterop(laser, AbortSignal.timeout(40_000))
    )
    assert.notEqual(summary.a2a, "")
    assert.notEqual(summary.mcp, "")
    assert.ok(summary.aguiEvents > 0)
    assert.equal(summary.decision, "approved")
  }
)

void test(
  "given_the_log_primitive_when_run_then_should_publish_and_replay_both_readings",
  { concurrency: false },
  async () => {
    const { readings, appended } = await withLaser("log", async (laser) => {
      const topic = laser.topic("readings")
      await topic.ensure(2)
      const count = async (): Promise<bigint> => {
        const cursor = await topic.replay()
        let total = 0n
        for (;;) {
          const page = await cursor.poll()
          if (page.length === 0) return total
          total += BigInt(page.length)
        }
      }
      const before = await count()
      const readings = await runLog(laser, AbortSignal.timeout(30_000))
      return { readings, appended: (await count()) - before }
    })
    assert.equal(appended, 2n)
    const values = readings.map((reading) => `${reading.host}:${String(reading.cpu)}`)
    assert.ok(values.includes("node-1:42"))
    assert.ok(values.includes("node-2:91"))
  }
)

void test(
  "given_the_recall_primitive_when_run_then_should_remember_and_recall_in_process",
  { concurrency: false },
  async () => {
    const recalled = await withLaser("recall", (laser) =>
      runRecall(laser, AbortSignal.timeout(30_000))
    )
    assert.deepEqual(recalled, ["node-7 sits in the eu-west pool, rotates keys monthly"])
  }
)

void test(
  "given_the_context_primitive_when_run_then_should_assemble_the_conversation",
  { concurrency: false },
  async () => {
    const turns = await withLaser("context", (laser) =>
      runContext(laser, AbortSignal.timeout(30_000))
    )
    assert.deepEqual(turns, ["drain node-7", "drained, 0 connections left"])
  }
)

void test(
  "given_the_agent_primitive_when_run_then_should_complete_the_contract",
  { concurrency: false, timeout: 30_000 },
  async () => {
    const contract = await withLaser("agent", (laser) =>
      runAgent(laser, AbortSignal.timeout(25_000))
    )
    assert.deepEqual(contract, { kind: "completed", reply: "on it" })
  }
)

void test(
  "given_the_fleet_change_feed_when_filtered_then_should_deliver_only_safe_mode_records",
  { concurrency: false, timeout: 60_000 },
  async (t) => {
    const ran = await withLaser("cdc", (laser) => runCdc(laser, AbortSignal.timeout(55_000)))
    if (!ran) t.skip("consumer group filters need a deployment that serves the filter catalog")
  }
)

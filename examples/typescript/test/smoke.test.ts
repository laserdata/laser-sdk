import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Laser } from "@laserdata/laser-sdk"

import { run as runInterop } from "../src/interop/main.js"
import { run as runNativeStreaming } from "../src/native-streaming/main.js"

const CONNECTION_STRING =
  process.env["LASER_CONNECTION_STRING"] ?? "iggy://iggy:iggy@127.0.0.1:8090"

async function withLaser(name: string, run: (laser: Laser) => Promise<void>): Promise<void> {
  const laser = await Laser.connectWithStream(
    CONNECTION_STRING,
    `laser-ts-example-${name}-${randomUUID()}`
  )
  try {
    await laser.stream(laser.defaultStream ?? "").ensure()
    await run(laser)
  } finally {
    await laser.close()
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
      await withLaser("native", (laser) => runNativeStreaming(laser, AbortSignal.timeout(30_000)))
    } finally {
      if (previousMessages === undefined) delete process.env["LASER_MESSAGES"]
      else process.env["LASER_MESSAGES"] = previousMessages
      if (previousBatch === undefined) delete process.env["LASER_BATCH"]
      else process.env["LASER_BATCH"] = previousBatch
    }
    assert.ok(true)
  }
)

void test(
  "given_the_interop_agents_when_run_then_should_complete_every_bridge_flow",
  { concurrency: false, timeout: 45_000 },
  async () => {
    await withLaser("interop", (laser) => runInterop(laser, AbortSignal.timeout(40_000)))
    assert.ok(true)
  }
)

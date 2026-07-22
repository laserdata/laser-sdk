import assert from "node:assert/strict"
import { execFile } from "node:child_process"
import { randomUUID } from "node:crypto"
import { createServer } from "node:net"
import { promisify } from "node:util"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import type { Consumer } from "../../src/stream/consumer.js"

const exec = promisify(execFile)
const IGGY_IMAGE = process.env["LASER_TEST_IGGY_IMAGE"] ?? "apache/iggy:latest"

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

void test(
  "given_a_server_restart_when_reusing_the_same_client_then_should_publish_and_consume_again",
  { timeout: 25_000 },
  async () => {
    const name = `laser-ts-reconnect-${randomUUID()}`
    let laser: Laser | undefined
    let consumer: Consumer | undefined
    try {
      const port = await availablePort()
      await docker([
        "run",
        "-d",
        "--rm",
        "--name",
        name,
        "--cap-add",
        "SYS_NICE",
        "--security-opt",
        "seccomp=unconfined",
        "--ulimit",
        "memlock=-1:-1",
        "-p",
        `127.0.0.1:${String(port)}:3000`,
        "-e",
        "IGGY_ROOT_USERNAME=iggy",
        "-e",
        "IGGY_ROOT_PASSWORD=iggy",
        "-e",
        "IGGY_TCP_ENABLED=true",
        "-e",
        "IGGY_TCP_ADDRESS=0.0.0.0:3000",
        "-e",
        "IGGY_HTTP_ADDRESS=0.0.0.0:80",
        IGGY_IMAGE
      ])
      const connection = `iggy://iggy:iggy@127.0.0.1:${String(port)}`
      const stream = "reconnect_it"
      laser = await connectEventually(connection, stream)
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

      await docker(["restart", name], 8_000)
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
      const after = await within(consumer.nextWithin(5_000), 6_000)
      assert.equal(new TextDecoder().decode(after?.payload), "after-restart")
    } finally {
      try {
        if (consumer !== undefined) await consumer.shutdown().catch(() => undefined)
        if (laser !== undefined) await within(laser.close(), 500).catch(() => undefined)
      } finally {
        await docker(["stop", name], 3_000).catch(() => undefined)
      }
    }
  }
)

async function connectEventually(connection: string, stream: string): Promise<Laser> {
  const deadline = performance.now() + 8_000
  let lastError: unknown
  while (performance.now() < deadline) {
    try {
      return await within(Laser.connectWithStream(connection, stream), 1_000)
    } catch (error) {
      lastError = error
      await delay(100)
    }
  }
  throw new Error(`dedicated Iggy did not start: ${String(lastError)}`)
}

async function docker(args: readonly string[], timeout = 5_000): Promise<void> {
  await exec("docker", [...args], { timeout })
}

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

function availablePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer()
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      if (address === null || typeof address === "string") {
        server.close()
        reject(new Error("failed to reserve a TCP port"))
        return
      }
      server.close((error) => {
        if (error !== undefined) reject(error)
        else resolve(address.port)
      })
    })
  })
}

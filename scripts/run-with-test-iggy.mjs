#!/usr/bin/env node

import { spawn, execFile } from "node:child_process"
import { open, mkdtemp, readFile, rm } from "node:fs/promises"
import { createServer, createConnection } from "node:net"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { promisify } from "node:util"

const execute = promisify(execFile)
const repositoryRoot = resolve(import.meta.dirname, "..")
const resolver = join(repositoryRoot, "scripts", "resolve-test-iggy-server.sh")
const command = process.argv[2]
const args = process.argv.slice(3)

if (command === undefined) {
  console.error("usage: run-with-test-iggy.mjs <command> [args...]")
  process.exit(2)
}

function configuredEndpoint(env) {
  if (env["LASER_CONNECTION_STRING"]) return env["LASER_CONNECTION_STRING"]
  if (env["LASER_BDD_URL"]) return env["LASER_BDD_URL"]
  if (env["LASER_BDD_ADDR"]) return `iggy:iggy@${env["LASER_BDD_ADDR"]}`
  return undefined
}

function freePort() {
  return new Promise((resolvePort, reject) => {
    const server = createServer()
    server.once("error", reject)
    server.listen(0, "127.0.0.1", () => {
      const address = server.address()
      if (address === null || typeof address === "string") {
        server.close()
        reject(new Error("failed to reserve an Iggy test port"))
        return
      }
      server.close((error) => {
        if (error !== undefined) reject(error)
        else resolvePort(address.port)
      })
    })
  })
}

function waitForTcp(port, server, logPath) {
  const deadline = Date.now() + 30_000
  return new Promise((resolveReady, reject) => {
    const probe = () => {
      if (server.exitCode !== null) {
        readFile(logPath, "utf8").then(
          (log) =>
            reject(
              new Error(
                `Iggy test server exited with ${String(server.exitCode)}:\n${log}`,
              ),
            ),
          reject,
        )
        return
      }
      const socket = createConnection({ host: "127.0.0.1", port })
      socket.once("connect", () => {
        socket.destroy()
        resolveReady()
      })
      socket.once("error", () => {
        socket.destroy()
        if (Date.now() >= deadline) {
          reject(
            new Error(
              "Iggy test server did not accept TCP connections within 30s",
            ),
          )
        } else {
          setTimeout(probe, 100)
        }
      })
    }
    probe()
  })
}

function waitForExit(child) {
  if (child.exitCode !== null) {
    return Promise.resolve({ code: child.exitCode, signal: child.signalCode })
  }
  return new Promise((resolveExit) => {
    child.once("exit", (code, signal) => resolveExit({ code, signal }))
  })
}

async function stop(child) {
  if (child === undefined || child.exitCode !== null) return
  child.kill("SIGTERM")
  const exited = waitForExit(child)
  let timer
  const forced = new Promise((resolveForced) => {
    timer = setTimeout(() => {
      if (child.exitCode === null) child.kill("SIGKILL")
      resolveForced()
    }, 5_000)
  })
  try {
    await Promise.race([exited, forced])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

async function runChild(endpoint, extraEnv = {}) {
  const child = spawn(command, args, {
    cwd: process.cwd(),
    env: {
      ...process.env,
      ...extraEnv,
      LASER_CONNECTION_STRING: endpoint,
      LASER_BDD_URL: endpoint,
    },
    stdio: "inherit",
    shell: false,
  })
  const { code, signal } = await waitForExit(child)
  if (signal !== null) return 1
  return code ?? 1
}

const external = configuredEndpoint(process.env)
if (external !== undefined) {
  process.exitCode = await runChild(external)
} else {
  let server
  let directory
  let log
  try {
    const { stdout } = await execute(resolver, [], {
      cwd: repositoryRoot,
      env: process.env,
      timeout: 310_000,
    })
    const binary = stdout.trim()
    if (binary.length === 0)
      throw new Error("Iggy test server resolver returned no path")
    directory = await mkdtemp(join(tmpdir(), "laser-iggy-"))
    const port = await freePort()
    const logPath = join(directory, "test-server.log")
    log = await open(logPath, "w")
    server = spawn(binary, [], {
      env: {
        ...process.env,
        IGGY_SYSTEM_PATH: directory,
        IGGY_TCP_ADDRESS: `127.0.0.1:${String(port)}`,
        IGGY_HTTP_ENABLED: "false",
        IGGY_QUIC_ENABLED: "false",
        IGGY_WEBSOCKET_ENABLED: "false",
        IGGY_ROOT_USERNAME: "iggy",
        IGGY_ROOT_PASSWORD: "iggy",
        IGGY_SHARD_RUNTIME_CAPACITY: "256",
        IGGY_SYSTEM_SHARDING_RECONCILE_PERIODIC_INTERVAL: "200 ms",
      },
      stdio: ["ignore", log.fd, log.fd],
    })
    await waitForTcp(port, server, logPath)
    process.exitCode = await runChild(`iggy:iggy@127.0.0.1:${String(port)}`, {
      LASER_TEST_IGGY_SERVER: binary,
    })
  } finally {
    await stop(server)
    await log?.close()
    if (directory !== undefined)
      await rm(directory, { recursive: true, force: true })
  }
}

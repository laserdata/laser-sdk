import { execFile, spawn, type ChildProcess } from "node:child_process"
import { open, mkdtemp, readFile, rm, type FileHandle } from "node:fs/promises"
import { createConnection, createServer } from "node:net"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { promisify } from "node:util"

const execute = promisify(execFile)
const REPOSITORY_ROOT = resolve(import.meta.dirname, "../../../..")
const RESOLVER = join(REPOSITORY_ROOT, "scripts", "resolve-test-iggy-server.sh")

function freePort(): Promise<number> {
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

function waitForExit(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return Promise.resolve()
  return new Promise((resolveExit) => {
    child.once("exit", () => {
      resolveExit()
    })
  })
}

async function stop(child: ChildProcess | undefined): Promise<void> {
  if (child?.exitCode !== null) return
  child.kill("SIGTERM")
  let timer: NodeJS.Timeout | undefined
  const timeout = new Promise<void>((resolveTimeout) => {
    timer = setTimeout(() => {
      if (child.exitCode === null) child.kill("SIGKILL")
      resolveTimeout()
    }, 5_000)
  })
  try {
    await Promise.race([waitForExit(child), timeout])
  } finally {
    if (timer !== undefined) clearTimeout(timer)
  }
}

async function resolveBinary(): Promise<string> {
  const configured = process.env["LASER_TEST_IGGY_SERVER"]
  if (configured !== undefined && configured.length > 0) return configured
  const { stdout } = await execute(RESOLVER, [], {
    cwd: REPOSITORY_ROOT,
    env: process.env,
    timeout: 310_000
  })
  const binary = stdout.trim()
  if (binary.length === 0) throw new Error("Iggy test server resolver returned no path")
  return binary
}

export class TestIggy {
  private child: ChildProcess | undefined
  private closed = false

  private constructor(
    private readonly binary: string,
    private readonly directory: string,
    private readonly port: number,
    private readonly log: FileHandle
  ) {}

  get endpoint(): string {
    return `iggy:iggy@127.0.0.1:${String(this.port)}`
  }

  static async start(): Promise<TestIggy> {
    const binary = await resolveBinary()
    const directory = await mkdtemp(join(tmpdir(), "laser-iggy-"))
    const port = await freePort()
    const log = await open(join(directory, "test-server.log"), "w")
    const server = new TestIggy(binary, directory, port, log)
    try {
      await server.spawn()
      return server
    } catch (error) {
      await server.close()
      throw error
    }
  }

  async restart(): Promise<void> {
    await stop(this.child)
    this.child = undefined
    await this.spawn()
  }

  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    await stop(this.child)
    await this.log.close()
    await rm(this.directory, { recursive: true, force: true })
  }

  private async spawn(): Promise<void> {
    const logPath = join(this.directory, "test-server.log")
    this.child = spawn(this.binary, [], {
      env: {
        ...process.env,
        IGGY_SYSTEM_PATH: this.directory,
        IGGY_TCP_ADDRESS: `127.0.0.1:${String(this.port)}`,
        IGGY_HTTP_ENABLED: "false",
        IGGY_QUIC_ENABLED: "false",
        IGGY_WEBSOCKET_ENABLED: "false",
        IGGY_ROOT_USERNAME: "iggy",
        IGGY_ROOT_PASSWORD: "iggy",
        IGGY_SHARD_RUNTIME_CAPACITY: "256",
        IGGY_SYSTEM_SHARDING_RECONCILE_PERIODIC_INTERVAL: "200 ms"
      },
      stdio: ["ignore", this.log.fd, this.log.fd]
    })
    const deadline = Date.now() + 30_000
    while (Date.now() < deadline) {
      if (this.child.exitCode !== null) {
        const contents = await readFile(logPath, "utf8")
        throw new Error(`Iggy test server exited with ${String(this.child.exitCode)}:\n${contents}`)
      }
      const ready = await new Promise<boolean>((resolveReady) => {
        const socket = createConnection({ host: "127.0.0.1", port: this.port })
        socket.once("connect", () => {
          socket.destroy()
          resolveReady(true)
        })
        socket.once("error", () => {
          socket.destroy()
          resolveReady(false)
        })
      })
      if (ready) return
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 100))
    }
    throw new Error("Iggy test server did not accept TCP connections within 30s")
  }
}

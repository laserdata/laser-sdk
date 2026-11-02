import { execFile, spawn, type ChildProcess } from "node:child_process"
import { open, mkdtemp, readFile, rm, type FileHandle } from "node:fs/promises"
import { createConnection, createServer } from "node:net"
import { tmpdir } from "node:os"
import { join, resolve } from "node:path"
import { promisify } from "node:util"

const execute = promisify(execFile)
const REPOSITORY_ROOT = resolve(process.cwd(), "../..")
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

  /** Stops the server and keeps its data, so a client sees it gone. */
  async stop(): Promise<void> {
    await stop(this.child)
    this.child = undefined
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
        IGGY_PATH: join(this.directory, "data"),
        IGGY_TCP_ADDRESS: `127.0.0.1:${String(this.port)}`,
        IGGY_HTTP_ENABLED: "false",
        IGGY_QUIC_ENABLED: "false",
        IGGY_WEBSOCKET_ENABLED: "false",
        IGGY_ROOT_USERNAME: "iggy",
        IGGY_ROOT_PASSWORD: "iggy",
        IGGY_SHARD_RUNTIME_CAPACITY: "256",
        IGGY_SHARDING_CPU_ALLOCATION: "2",
        IGGY_SHARDING_PIN_CORES: "false",
        IGGY_SHARDING_RECONCILE_PERIODIC_INTERVAL: "200 ms",
        IGGY_CONSUMER_GROUP_REBALANCING_TIMEOUT: "1s"
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

export class TestIggyCluster {
  private readonly overrides = new Map<number, NodeJS.ProcessEnv>()
  private readonly children: (ChildProcess | undefined)[]
  private readonly directories: string[] = []
  private readonly logs: FileHandle[] = []
  private proxy: ReturnType<typeof createServer> | undefined
  private proxyTarget: number

  private constructor(
    private readonly binary: string,
    private readonly tcpPorts: readonly number[],
    private readonly replicaPorts: readonly number[],
    private readonly httpPorts: readonly number[],
    private readonly proxyPort: number
  ) {
    this.proxyTarget = required(tcpPorts[0], "cluster TCP port 0")
    this.children = tcpPorts.map(() => undefined)
  }

  /** How many nodes the cluster has. */
  get size(): number {
    return this.tcpPorts.length
  }

  get endpoint(): string {
    return `iggy:iggy@127.0.0.1:${String(this.proxyPort)}`
  }

  /** Starts `nodes` nodes, each with the extra variables `environment` gives it. */
  static async start(
    nodes = 3,
    environment?: (replicaId: number) => NodeJS.ProcessEnv
  ): Promise<TestIggyCluster> {
    const binary = await resolveBinary()
    const ports = await uniquePorts(nodes * 3 + 1)
    const cluster = new TestIggyCluster(
      binary,
      ports.slice(0, nodes),
      ports.slice(nodes, nodes * 2),
      ports.slice(nodes * 2, nodes * 3),
      required(ports[nodes * 3], "cluster proxy port")
    )
    if (environment !== undefined) {
      for (let replicaId = 0; replicaId < nodes; replicaId += 1)
        cluster.overrides.set(replicaId, environment(replicaId))
    }
    try {
      await cluster.startProxy()
      for (let replicaId = 0; replicaId < nodes; replicaId += 1) {
        const directory = await mkdtemp(join(tmpdir(), "laser-iggy-cluster-"))
        cluster.directories.push(directory)
        cluster.logs.push(await open(join(directory, "test-server.log"), "w"))
        cluster.spawn(replicaId)
      }
      await cluster.waitForMesh()
      return cluster
    } catch (error) {
      await cluster.close()
      throw error
    }
  }

  nodeEndpoint(replicaId: number): { readonly host: string; readonly port: number } {
    return { host: "127.0.0.1", port: required(this.tcpPorts[replicaId], "node TCP port") }
  }

  setNodeEnvironment(replicaId: number, variables: NodeJS.ProcessEnv): void {
    this.overrides.set(replicaId, variables)
  }

  async restartNode(replicaId: number): Promise<void> {
    await this.stopNode(replicaId)
    await this.startNode(replicaId)
  }

  async stopNode(replicaId: number): Promise<void> {
    await stop(this.children[replicaId])
    this.children[replicaId] = undefined
    await this.logs[replicaId]?.close()
    this.logs[replicaId] = await open(
      join(required(this.directories[replicaId], "cluster data directory"), "test-server.log"),
      "w"
    )
  }

  /** Kills the node with SIGKILL, a crash. */
  async killNode(replicaId: number): Promise<void> {
    const child = this.children[replicaId]
    if (child?.exitCode === null) {
      child.kill("SIGKILL")
      await waitForExit(child)
    }
    this.children[replicaId] = undefined
    await this.logs[replicaId]?.close()
    this.logs[replicaId] = await open(
      join(required(this.directories[replicaId], "cluster data directory"), "test-server.log"),
      "w"
    )
  }

  async startNode(replicaId: number): Promise<void> {
    this.spawn(replicaId)
    await this.waitForMesh(replicaId)
  }

  routeEndpointTo(replicaId: number): void {
    this.proxyTarget = required(this.tcpPorts[replicaId], "cluster route target")
  }

  async leaderAndFollower(via = 0): Promise<readonly [number, number]> {
    const baseUrl = `http://127.0.0.1:${String(required(this.httpPorts[via], `HTTP port ${String(via)}`))}`
    const deadline = Date.now() + 30_000
    while (Date.now() < deadline) {
      try {
        const login = await fetch(`${baseUrl}/users/login`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ username: "iggy", password: "iggy" })
        })
        if (!login.ok) throw new Error(`login returned ${String(login.status)}`)
        const identity = (await login.json()) as {
          access_token?: { token?: string }
        }
        const token = identity.access_token?.token
        if (token === undefined) throw new Error("login returned no access token")
        const response = await fetch(`${baseUrl}/cluster/metadata`, {
          headers: { authorization: `Bearer ${token}` }
        })
        if (!response.ok) throw new Error(`metadata returned ${String(response.status)}`)
        const metadata = (await response.json()) as {
          nodes?: readonly { name: string; role: string }[]
        }
        const leaderNode = metadata.nodes?.find((node) => node.role === "leader")
        if (leaderNode === undefined) throw new Error("metadata returned no leader")
        const leader = Number.parseInt(leaderNode.name.replace(/^node-/, ""), 10)
        if (!Number.isInteger(leader) || leader < 0 || leader >= this.size) {
          throw new Error(`invalid leader name ${leaderNode.name}`)
        }
        const follower = this.tcpPorts.map((_, node) => node).find((node) => node !== leader)
        return [leader, required(follower, "follower node")]
      } catch {
        await new Promise((resolveDelay) => setTimeout(resolveDelay, 100))
      }
    }
    throw new Error("Iggy cluster leader was not discoverable within 30s")
  }

  async close(): Promise<void> {
    for (const child of this.children) await stop(child)
    const proxy = this.proxy
    if (proxy !== undefined) {
      await new Promise<void>((resolveClose) => {
        proxy.close(() => {
          resolveClose()
        })
      })
      this.proxy = undefined
    }
    for (const log of this.logs) await log.close().catch(() => undefined)
    for (const directory of this.directories) {
      await rm(directory, { recursive: true, force: true })
    }
  }

  private startProxy(): Promise<void> {
    return new Promise((resolveListen, reject) => {
      const proxy = createServer((incoming) => {
        const upstream = createConnection({ host: "127.0.0.1", port: this.proxyTarget })
        incoming.pipe(upstream)
        upstream.pipe(incoming)
        upstream.once("error", () => incoming.destroy())
        incoming.once("error", () => upstream.destroy())
      })
      proxy.once("error", reject)
      proxy.listen(this.proxyPort, "127.0.0.1", () => {
        proxy.off("error", reject)
        this.proxy = proxy
        resolveListen()
      })
    })
  }

  private spawn(replicaId: number): void {
    const env: NodeJS.ProcessEnv = {
      ...process.env,
      ...this.overrides.get(replicaId),
      IGGY_PATH: join(required(this.directories[replicaId], "cluster data directory"), "data"),
      IGGY_CLUSTER_ENABLED: "true",
      IGGY_CLUSTER_NAME: "laser-sdk-rolling-restart",
      IGGY_MESSAGE_BUS_RECONNECT_PERIOD: "100ms",
      IGGY_HTTP_ENABLED: "true",
      IGGY_HTTP_ADDRESS: `127.0.0.1:${String(required(this.httpPorts[replicaId], "HTTP port"))}`,
      IGGY_QUIC_ENABLED: "false",
      IGGY_WEBSOCKET_ENABLED: "false",
      IGGY_ROOT_USERNAME: "iggy",
      IGGY_ROOT_PASSWORD: "iggy",
      IGGY_SHARD_RUNTIME_CAPACITY: "256",
      IGGY_SHARDING_CPU_ALLOCATION: "1",
      IGGY_SHARDING_PIN_CORES: "false",
      IGGY_SHARDING_RECONCILE_PERIODIC_INTERVAL: "200 ms",
      IGGY_CONSUMER_GROUP_REBALANCING_TIMEOUT: "1s"
    }
    for (let node = 0; node < this.size; node += 1) {
      const prefix = `IGGY_CLUSTER_NODES_${String(node)}`
      env[`${prefix}_NAME`] = `node-${String(node)}`
      env[`${prefix}_IP`] = "127.0.0.1"
      env[`${prefix}_REPLICA_ID`] = String(node)
      env[`${prefix}_PORTS_TCP`] = String(this.tcpPorts[node])
      env[`${prefix}_PORTS_TCP_REPLICA`] = String(this.replicaPorts[node])
      env[`${prefix}_PORTS_HTTP`] = String(this.httpPorts[node])
    }
    const log = this.logs[replicaId]
    if (log === undefined)
      throw new Error(`missing log handle for cluster node ${String(replicaId)}`)
    this.children[replicaId] = spawn(this.binary, ["--replica-id", String(replicaId)], {
      env,
      stdio: ["ignore", log.fd, log.fd]
    })
  }

  private async waitForMesh(replicaId?: number): Promise<void> {
    const nodes = replicaId === undefined ? this.tcpPorts.map((_, node) => node) : [replicaId]
    const deadline = Date.now() + 30_000
    while (Date.now() < deadline) {
      let ready = true
      for (const node of nodes) {
        const child = this.children[node]
        const directory = required(this.directories[node], "cluster data directory")
        if (child?.exitCode !== null) {
          const contents = await readFile(join(directory, "test-server.log"), "utf8")
          throw new Error(`Iggy cluster node exited with ${String(child?.exitCode)}:\n${contents}`)
        }
        const contents = await readFile(join(directory, "test-server.log"), "utf8")
        // The nodes inherit the environment, so a RUST_LOG that silences the
        // server's info lines never shows the marker.
        ready = ready && contents.includes("replica mesh complete")
      }
      if (ready) return
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 100))
    }
    throw new Error("Iggy cluster did not form its replica mesh within 30s")
  }
}

/**
 * The stand-in filter catalog (`laser-test-catalog`, built from the
 * workspace's `test-support` crate), one Unix socket per node. Its path comes
 * from `LASER_TEST_CATALOG`.
 */
export class TestCatalog {
  private constructor(
    private readonly child: ChildProcess,
    private readonly directory: string,
    readonly sockets: readonly string[]
  ) {}

  /** The catalog binary, or `undefined` when no test asked for one. */
  static binary(): string | undefined {
    const configured = process.env["LASER_TEST_CATALOG"]
    return configured === undefined || configured.length === 0 ? undefined : configured
  }

  static async start(nodes: number): Promise<TestCatalog> {
    const binary = TestCatalog.binary()
    if (binary === undefined) throw new Error("LASER_TEST_CATALOG names no catalog binary")
    const directory = await mkdtemp(join(tmpdir(), "laser-catalog-"))
    const sockets = Array.from({ length: nodes }, (_, node) =>
      join(directory, `backend-${String(node)}.sock`)
    )
    const child = spawn(binary, sockets, { stdio: ["ignore", "pipe", "ignore"] })
    const catalog = new TestCatalog(child, directory, sockets)
    try {
      await new Promise<void>((resolveReady, reject) => {
        let output = ""
        const timer = setTimeout(() => {
          reject(new Error("the catalog printed no ready line within 30s"))
        }, 30_000)
        child.stdout.on("data", (chunk: Buffer) => {
          output += chunk.toString("utf8")
          if (output.split("\n").includes("ready")) {
            clearTimeout(timer)
            resolveReady()
          }
        })
        child.once("exit", (code) => {
          clearTimeout(timer)
          reject(new Error(`the catalog exited with ${String(code)}`))
        })
      })
      return catalog
    } catch (error) {
      await catalog.close()
      throw error
    }
  }

  async close(): Promise<void> {
    await stop(this.child)
    await rm(this.directory, { recursive: true, force: true })
  }
}

function required<T>(value: T | undefined, name: string): T {
  if (value === undefined) throw new Error(`missing ${name}`)
  return value
}

async function uniquePorts(count: number): Promise<number[]> {
  const ports = new Set<number>()
  while (ports.size < count) ports.add(await freePort())
  return [...ports]
}

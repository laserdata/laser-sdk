import {
  Laser,
  type Capabilities,
  type CapabilitySurface,
  type GraphNode,
  type MemoryItem
} from "@laserdata/laser-sdk"

export const LOCAL_CONNECTION_STRING = "iggy:iggy@127.0.0.1:8090"
export const DEFAULT_PORT = 8090
export const DEFAULT_STREAM = "laser"
export const PARTITIONS = 4

export class AsyncResourceGroup implements AsyncDisposable {
  private readonly resources: AsyncDisposable[] = []

  add<T extends AsyncDisposable>(resource: T): T {
    this.resources.push(resource)
    return resource
  }

  async [Symbol.asyncDispose](): Promise<void> {
    let failure: unknown
    for (const resource of this.resources.reverse()) {
      try {
        await resource[Symbol.asyncDispose]()
      } catch (error) {
        failure ??= error
      }
    }
    if (failure !== undefined) throw failure
  }
}

/** Prints the shared phase heading used by all language examples. */
export function phase(title: string): void {
  const rule = "─".repeat([...title].length + 3)
  console.log(`\n\x1b[1;36m▸ ${title}\x1b[0m\n\x1b[36m${rule}\x1b[0m`)
}

export function streamFor(example: string, env: NodeJS.ProcessEnv = process.env): string {
  return envValue("LASER_STREAM", env) || `${DEFAULT_STREAM}-${example}`
}

export function resolveConnectionString(env: NodeJS.ProcessEnv = process.env): string {
  const explicit = envValue("LASER_CONNECTION_STRING", env)
  if (explicit.length > 0) return normalizeTarget(explicit, env)
  const server = envValue("LASER_SERVER", env)
  if (server.length === 0) return LOCAL_CONNECTION_STRING
  return normalizeTarget(`iggy+tcp://${resolveCredentials(env)}${server}`, env)
}

export async function connectExample(
  example: string,
  env: NodeJS.ProcessEnv = process.env
): Promise<Laser> {
  const stream = streamFor(example, env)
  const laser = await Laser.builder()
    .connectionString(resolveConnectionString(env))
    .defaultStream(stream)
    .connect()
  try {
    await laser.stream(stream).ensure()
    return laser
  } catch (error) {
    await laser.close()
    throw error
  }
}

export async function runExample(
  example: string,
  run: (laser: Laser, signal: AbortSignal) => Promise<void>
): Promise<void> {
  await using laser = await connectExample(example)
  using shutdown = installShutdownSignals()
  await run(laser, shutdown.signal)
}

export function managedGate(
  capabilities: Capabilities,
  feature: CapabilitySurface,
  example: string
): boolean {
  if (surfaceAvailable(capabilities, feature)) return true
  console.log(
    [
      "",
      `  ${feature} is a LaserData Cloud feature and the connected server is raw Apache Iggy,`,
      "  so this phase is skipped. Point the example at a deployment to run it live:",
      "",
      `    LASER_CONNECTION_STRING=user:pwd@your-host npm run example:${example}`,
      ""
    ].join("\n")
  )
  return false
}

export function messages(fallback: number, env: NodeJS.ProcessEnv = process.env): number {
  return Math.max(1, envInteger("LASER_MESSAGES", fallback, env))
}

export function batchSize(fallback: number, env: NodeJS.ProcessEnv = process.env): number {
  return Math.max(1, envInteger("LASER_BATCH", fallback, env))
}

export function concurrency(fallback: number, env: NodeJS.ProcessEnv = process.env): number {
  return Math.max(1, envInteger("LASER_CONCURRENCY", fallback, env))
}

export function payloadBytes(fallback: number, env: NodeJS.ProcessEnv = process.env): number {
  const value = envInteger("LASER_PAYLOAD_BYTES", fallback, env)
  return value >= 0 ? value : fallback
}

export function envInteger(
  name: string,
  fallback: number,
  env: NodeJS.ProcessEnv = process.env
): number {
  const raw = envValue(name, env)
  if (raw.length === 0) return fallback
  const value = Number(raw)
  return Number.isSafeInteger(value) ? value : fallback
}

export function envBoolean(
  name: string,
  fallback: boolean,
  env: NodeJS.ProcessEnv = process.env
): boolean {
  const raw = envValue(name, env).toLowerCase()
  return raw.length === 0 ? fallback : ["1", "true", "yes", "on"].includes(raw)
}

export class Rng {
  private state: bigint

  constructor(seed: bigint) {
    this.state = (seed | 1n) & MASK_64
  }

  nextU64(): bigint {
    let value = this.state
    value ^= (value << 13n) & MASK_64
    value ^= value >> 7n
    value ^= (value << 17n) & MASK_64
    this.state = value & MASK_64
    return (this.state * XORSHIFT_MULTIPLIER) & MASK_64
  }

  below(bound: number): number {
    if (!Number.isSafeInteger(bound) || bound < 1) throw new Error("bound must be positive")
    return Number(this.nextU64() % BigInt(bound))
  }

  pick<T>(values: readonly T[]): T {
    const value = values[this.below(values.length)]
    if (value === undefined) throw new Error("cannot choose from an empty list")
    return value
  }
}

export const utf8 = (value: string): Uint8Array => new TextEncoder().encode(value)
export const decodeUtf8 = (value: Uint8Array): string => new TextDecoder().decode(value)

export function printHits(label: string, hits: readonly MemoryItem[]): void {
  console.log(label)
  hits.forEach((hit, index) => {
    const score = (hit.score ?? 0).toFixed(3)
    console.log(`  ${String(index + 1)}. (${score}) ${decodeUtf8(hit.payload)}`)
  })
}

export function graphNodeValue(node: GraphNode): string {
  for (const [key, value] of node.attrs) {
    if (key === "value" && value.kind === "string") return value.value
  }
  return "?"
}

export function printNodes(label: string, nodes: readonly GraphNode[]): void {
  const values = nodes.map(graphNodeValue).sort()
  console.log(`${label}: ${values.join(", ")}`)
}

export function printNodesOf(label: string, kind: string, nodes: readonly GraphNode[]): void {
  const values = nodes
    .filter((node) => node.labels.includes(kind))
    .map(graphNodeValue)
    .sort()
  console.log(`${label}: ${values.join(", ")}`)
}

export function printTable(
  rows: readonly (readonly string[])[],
  rightAligned: readonly number[] = []
): void {
  const widths: number[] = []
  for (const row of rows) {
    row.forEach((cell, column) => {
      widths[column] = Math.max(widths[column] ?? 0, cell.length)
    })
  }
  for (const row of rows) {
    const line = row
      .map((cell, column) => {
        const width = widths[column] ?? cell.length
        return rightAligned.includes(column) ? cell.padStart(width) : cell.padEnd(width)
      })
      .join("  ")
    console.log(`  ${line.trimEnd()}`)
  }
}

export const PROJECTOR_TIMEOUT_MS = 60_000
export const PROJECTION_POLL_MS = 150

export async function waitForSchema(laser: Laser, schemaId: number): Promise<void> {
  const deadline = Date.now() + PROJECTOR_TIMEOUT_MS
  while (Date.now() < deadline) {
    if ((await laser.schemas().get(schemaId)) !== undefined) return
    await sleep(PROJECTION_POLL_MS)
  }
  throw new Error(`schema ${String(schemaId)} never appeared in the registry`)
}

export async function waitForProjection(
  laser: Laser,
  index: string,
  expected: number
): Promise<bigint> {
  const deadline = Date.now() + PROJECTOR_TIMEOUT_MS
  const capabilities = await laser.capabilities()
  const feed = capabilities.watch ? await laser.watch().index(index).records() : undefined
  let last = -1n
  while (true) {
    const advanced = last < 0n || feed === undefined || (await feed.poll()).length > 0
    if (advanced) {
      let total = 0n
      try {
        total = (await laser.query(index).withTotal().fetch()).page.total ?? 0n
      } catch {}
      if (total !== last) {
        console.log(`  projector materialized ${String(total)}/${String(expected)} rows`)
        last = total
      }
      if (total >= BigInt(expected)) return total
    }
    if (Date.now() >= deadline) {
      const indexed = last > 0n ? last : 0n
      throw new Error(
        `projector indexed only ${String(indexed)}/${String(expected)} rows in ` +
          `\`${index}\` before the deadline`
      )
    }
    await sleep(PROJECTION_POLL_MS)
  }
}

class ShutdownController extends AbortController implements Disposable {
  private readonly abortOnSignal = (signal: NodeJS.Signals): void => {
    this.abort(new Error(signal))
  }

  constructor() {
    super()
    process.once("SIGINT", this.abortOnSignal)
    process.once("SIGTERM", this.abortOnSignal)
  }

  [Symbol.dispose](): void {
    this.abort("example complete")
    process.removeListener("SIGINT", this.abortOnSignal)
    process.removeListener("SIGTERM", this.abortOnSignal)
  }
}

export function installShutdownSignals(): AbortController & Disposable {
  return new ShutdownController()
}

export function ensureDefaultPort(connectionString: string): string {
  const schemeEnd = connectionString.indexOf("://")
  if (schemeEnd < 0) return connectionString
  const scheme = connectionString.slice(0, schemeEnd)
  const remainder = connectionString.slice(schemeEnd + 3)
  const cut = remainder.search(/[/?]/u)
  const authority = cut < 0 ? remainder : remainder.slice(0, cut)
  const pathAndQuery = cut < 0 ? "" : remainder.slice(cut)
  const at = authority.lastIndexOf("@")
  const userInfo = at < 0 ? "" : authority.slice(0, at + 1)
  const hostAndPort = at < 0 ? authority : authority.slice(at + 1)
  if (splitHostPort(hostAndPort).port !== undefined) return connectionString
  return `${scheme}://${userInfo}${hostAndPort}:${String(DEFAULT_PORT)}${pathAndQuery}`
}

const MASK_64 = (1n << 64n) - 1n
const XORSHIFT_MULTIPLIER = 0x2545f4914f6cdd1dn

function envValue(name: string, env: NodeJS.ProcessEnv = process.env): string {
  return env[name]?.trim() ?? ""
}

function resolveCredentials(env: NodeJS.ProcessEnv): string {
  const token = envValue("LASER_TOKEN", env)
  if (token.length > 0) return `${token}@`
  const username = envValue("LASER_USERNAME", env)
  const password = envValue("LASER_PASSWORD", env)
  if (username.length > 0 && password.length > 0) return `${username}:${password}@`
  throw new Error(
    "LaserData Cloud needs credentials: set LASER_TOKEN, or LASER_USERNAME + LASER_PASSWORD"
  )
}

function normalizeTarget(target: string, env: NodeJS.ProcessEnv): string {
  const withScheme = target.includes("://") ? target : `iggy+tcp://${target}`
  return resolveTls(ensureDefaultPort(withScheme), env)
}

function resolveTls(connectionString: string, env: NodeJS.ProcessEnv): string {
  if (env["LASER_NO_TLS"] !== undefined || connectionString.includes("tls_ca_file=")) {
    return connectionString
  }
  if (!isLaserDataHost(hostOf(connectionString))) return connectionString
  let withTls = connectionString
  if (!withTls.includes("tls=")) {
    const separator = withTls.includes("?") ? "&" : "?"
    withTls = `${withTls}${separator}tls=true`
  }
  const cert = envValue("LASER_TLS_CERT", env)
  return cert.length > 0 ? `${withTls}&tls_ca_file=${cert}` : withTls
}

function isLaserDataHost(host: string): boolean {
  const normalized = host.toLowerCase()
  return (
    normalized === "laserdata.cloud" ||
    normalized.endsWith(".laserdata.cloud") ||
    normalized === "laserdata.com" ||
    normalized.endsWith(".laserdata.com")
  )
}

function hostOf(connectionString: string): string {
  const schemeEnd = connectionString.indexOf("://")
  const afterScheme = schemeEnd < 0 ? connectionString : connectionString.slice(schemeEnd + 3)
  const cut = afterScheme.search(/[/?]/u)
  const withUserInfo = cut < 0 ? afterScheme : afterScheme.slice(0, cut)
  const at = withUserInfo.lastIndexOf("@")
  const authority = at < 0 ? withUserInfo : withUserInfo.slice(at + 1)
  return splitHostPort(authority).host
}

function splitHostPort(authority: string): {
  readonly host: string
  readonly port?: string
} {
  if (authority.startsWith("[")) {
    const closing = authority.indexOf("]")
    if (closing >= 0) {
      const host = authority.slice(1, closing)
      const suffix = authority.slice(closing + 1)
      return suffix.startsWith(":") ? { host, port: suffix.slice(1) } : { host }
    }
  }
  const colon = authority.lastIndexOf(":")
  if (colon < 0) return { host: authority }
  return { host: authority.slice(0, colon), port: authority.slice(colon + 1) }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function surfaceAvailable(capabilities: Capabilities, feature: CapabilitySurface): boolean {
  switch (feature) {
    case "managed":
      return capabilities.managed
    case "query":
      return capabilities.query.available
    case "kv":
      return capabilities.kv.available
    case "kvCas":
      return capabilities.kv.available && capabilities.kv.cas
    case "kvCasFenced":
      return capabilities.kv.available && capabilities.kv.casFenced
    case "graph":
      return capabilities.graph
    case "forks":
      return capabilities.forks
    case "agentWorkflow":
      return capabilities.agentWorkflow
    case "watch":
      return capabilities.watch
    case "authz":
      return capabilities.authz
  }
}

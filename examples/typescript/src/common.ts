import { Laser, type Capabilities, type CapabilitySurface } from "@laserdata/laser-sdk"

export const LOCAL_CONNECTION_STRING = "iggy://iggy:iggy@127.0.0.1:8090"
export const DEFAULT_PORT = 8090
export const PARTITIONS = 4

const MASK_64 = (1n << 64n) - 1n
const XORSHIFT_MULTIPLIER = 0x2545f4914f6cdd1dn

function envValue(name: string, env: NodeJS.ProcessEnv = process.env): string {
  return env[name]?.trim() ?? ""
}

export function streamFor(example: string, env: NodeJS.ProcessEnv = process.env): string {
  return envValue("LASER_STREAM", env) || `laser-${example}`
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

export function messages(fallback: number, env: NodeJS.ProcessEnv = process.env): number {
  return Math.max(1, envInteger("LASER_MESSAGES", fallback, env))
}

export function batchSize(fallback: number, env: NodeJS.ProcessEnv = process.env): number {
  return Math.max(1, envInteger("LASER_BATCH", fallback, env))
}

export function resolveConnectionString(env: NodeJS.ProcessEnv = process.env): string {
  const explicit = envValue("LASER_CONNECTION_STRING", env)
  if (explicit.length > 0) return explicit
  const server = envValue("LASER_SERVER", env)
  if (server.length === 0) return LOCAL_CONNECTION_STRING
  const token = envValue("LASER_TOKEN", env)
  const username = envValue("LASER_USERNAME", env)
  const password = envValue("LASER_PASSWORD", env)
  const credentials =
    token.length > 0
      ? token
      : username.length > 0 && password.length > 0
        ? `${username}:${password}`
        : undefined
  if (credentials === undefined) {
    throw new Error("LaserData Cloud needs LASER_TOKEN or LASER_USERNAME and LASER_PASSWORD")
  }
  const authority = server.includes(":") ? server : `${server}:${String(DEFAULT_PORT)}`
  return `iggy+tcp://${credentials}@${authority}`
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

export function managedGate(
  capabilities: Capabilities,
  feature: CapabilitySurface,
  example: string
): boolean {
  const available =
    feature === "query"
      ? capabilities.query.available
      : feature === "kv"
        ? capabilities.kv.available
        : feature === "kvCas"
          ? capabilities.kv.cas
          : feature === "kvCasFenced"
            ? capabilities.kv.casFenced
            : feature === "agentWorkflow"
              ? capabilities.agentWorkflow
              : capabilities[feature]
  if (available) return true
  console.log(
    `${String(feature)} is unavailable on raw Apache Iggy; ${example}'s managed phase is skipped. ` +
      `Set LASER_CONNECTION_STRING to a LaserData Cloud deployment to run it.`
  )
  return false
}

export function installShutdownSignals(): AbortController {
  const controller = new AbortController()
  const abort = (signal: NodeJS.Signals): void => {
    controller.abort(new Error(signal))
  }
  process.once("SIGINT", abort)
  process.once("SIGTERM", abort)
  controller.signal.addEventListener(
    "abort",
    () => {
      process.removeListener("SIGINT", abort)
      process.removeListener("SIGTERM", abort)
    },
    { once: true }
  )
  return controller
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

export const PROJECTOR_TIMEOUT_MS = 60_000
export const PROJECTION_POLL_MS = 150

// The register reply carries a durable id, but the apply is asynchronous:
// read back until browse resolves it before the first publish against it.
export async function waitForSchema(laser: Laser, schemaId: number): Promise<void> {
  const deadline = Date.now() + PROJECTOR_TIMEOUT_MS
  while (Date.now() < deadline) {
    if ((await laser.schemas().get(schemaId)) !== undefined) return
    await new Promise((resolve) => setTimeout(resolve, PROJECTION_POLL_MS))
  }
  throw new Error(`schema ${String(schemaId)} never appeared in the registry`)
}

// Poll until the projector has indexed `expected` rows, tolerant of a
// not-yet-created index while the deployment applies the binding.
export async function waitForProjection(
  laser: Laser,
  index: string,
  expected: number
): Promise<void> {
  const deadline = Date.now() + PROJECTOR_TIMEOUT_MS
  while (Date.now() < deadline) {
    try {
      const result = await laser.query(index).withTotal().fetch()
      if ((result.page.total ?? 0n) >= BigInt(expected)) return
    } catch {
      // Registration is asynchronous; retry until the bounded deadline.
    }
    await new Promise((resolve) => setTimeout(resolve, PROJECTION_POLL_MS))
  }
  throw new Error(`projection \`${index}\` did not materialize before the deadline`)
}

export async function runExample(
  example: string,
  run: (laser: Laser, signal: AbortSignal) => Promise<void>
): Promise<void> {
  const shutdown = installShutdownSignals()
  const laser = await connectExample(example)
  try {
    await run(laser, shutdown.signal)
  } finally {
    shutdown.abort("example complete")
    await laser.close()
  }
}

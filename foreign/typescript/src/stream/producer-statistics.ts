import { randomUUID } from "node:crypto"
import type { LaserTransport, NodeConnection } from "../iggy/apache-iggy.js"
import { encodeNamed } from "../wire/cbor.js"
import { AGDX_HELLO_CODE, AGDX_SET_CLIENT_METADATA_CODE } from "../wire/codes.js"
import { decodeHelloReply } from "../wire/hello.js"

const registries = new WeakMap<LaserTransport, Registry>()
const capacity = 32
const sampleCapacity = 64

export class ProducerRecorder {
  readonly instanceId = randomUUID()
  readonly firstActivity = Date.now()
  lastActivity = this.firstActivity
  submittedRecords = 0n
  submittedBytes = 0n
  confirmedRecords = 0n
  confirmedBytes = 0n
  failures = 0n
  lastSuccess: number | null = null
  readonly samples: bigint[] = Array<bigint>(sampleCapacity).fill(0n)
  sampleCount = 0n
  closed = false

  constructor(
    readonly stream: string,
    readonly topic: string,
    transport: LaserTransport
  ) {
    const configured = configuredUnsigned("LASER_PRODUCER_TELEMETRY_INTERVAL_MS", 10_000)
    if (
      configured === 0 ||
      transport.openCoordinator === undefined ||
      transport.connectsNodes !== true
    )
      return
    const interval = Math.min(
      60_000,
      Math.max(1_000, Number.isFinite(configured) ? configured : 10_000)
    )
    let registry = registries.get(transport)
    if (registry === undefined) {
      registry = new Registry(transport, interval)
      registries.set(transport, registry)
    }
    registry.add(this)
  }

  begin(records: number, bytes: number): (success: boolean) => void {
    this.submittedRecords += BigInt(records)
    this.submittedBytes += BigInt(bytes)
    this.lastActivity = Date.now()
    const started = performance.now()
    return (success) => {
      this.lastActivity = Date.now()
      if (success) {
        this.confirmedRecords += BigInt(records)
        this.confirmedBytes += BigInt(bytes)
        this.lastSuccess = this.lastActivity
      } else this.failures += 1n
      const micros = Math.round((performance.now() - started) * 1_000)
      const bucket = Math.min(63, Math.ceil(Math.log2(Math.max(1, micros))))
      this.samples[bucket] = (this.samples[bucket] ?? 0n) + 1n
      this.sampleCount += 1n
    }
  }

  snapshot(): ReadonlyMap<string, unknown> {
    const percentile = (n: bigint): bigint | null => {
      if (
        this.sampleCount === 0n ||
        (n === 990n && this.sampleCount < 100n) ||
        (n === 999n && this.sampleCount < 1000n)
      )
        return null
      const rank = (this.sampleCount * n + 999n) / 1000n
      let cumulative = 0n
      for (let i = 0; i < this.samples.length; i += 1) {
        cumulative += this.samples[i] ?? 0n
        if (cumulative >= rank) return 1n << BigInt(i)
      }
      return null
    }
    return new Map<string, unknown>([
      ["instance_id", this.instanceId],
      ["stream", this.stream],
      ["topic", this.topic],
      ["first_activity_millis", this.firstActivity],
      ["last_activity_millis", this.lastActivity],
      ["submitted_records", this.submittedRecords],
      ["submitted_payload_bytes", this.submittedBytes],
      ["confirmed_records", this.confirmedRecords],
      ["confirmed_payload_bytes", this.confirmedBytes],
      ["retries", null],
      ["failed_calls", this.failures],
      ["last_success_millis", this.lastSuccess],
      [
        "latency",
        new Map<string, unknown>([
          ["samples", this.sampleCount],
          ["p50_micros", percentile(500n)],
          ["p99_micros", percentile(990n)],
          ["p999_micros", percentile(999n)]
        ])
      ]
    ])
  }
}

export async function closeProducerStatistics(transport: LaserTransport): Promise<void> {
  await registries.get(transport)?.close()
}

class Registry {
  private records: WeakRef<ProducerRecorder>[] = []
  private closed = false
  private timer: ReturnType<typeof setTimeout> | undefined
  private connection: NodeConnection | undefined

  constructor(
    private readonly transport: LaserTransport,
    private readonly interval: number
  ) {}

  add(record: ProducerRecorder): void {
    this.records = this.records.filter((reference) => reference.deref()?.closed === false)
    const configured = configuredUnsigned("LASER_PRODUCER_TELEMETRY_MAX_PRODUCERS", capacity)
    const limit = Math.min(
      capacity,
      Math.max(0, Number.isSafeInteger(configured) ? configured : capacity)
    )
    if (this.closed || this.records.length >= limit) return
    this.records.push(new WeakRef(record))
    this.schedule()
  }

  private schedule(): void {
    if (this.closed || this.timer !== undefined) return
    this.timer = setTimeout(() => {
      void this.tick()
    }, this.interval)
    this.timer.unref()
  }

  async close(): Promise<void> {
    this.closed = true
    clearTimeout(this.timer)
    this.records = []
    await this.connection?.close().catch(() => undefined)
    this.connection = undefined
  }

  private async tick(): Promise<void> {
    this.records = this.records.filter((reference) => reference.deref()?.closed === false)
    if (this.records.length === 0) {
      const connection = this.connection
      this.connection = undefined
      this.timer = undefined
      await connection?.close().catch(() => undefined)
      return
    }
    try {
      if (this.connection === undefined) {
        const opening = this.transport.openCoordinator?.()
        if (opening === undefined) return
        try {
          this.connection = await bounded(opening)
        } catch (error) {
          void opening.then(
            (connection) => connection.close(),
            () => undefined
          )
          throw error
        }
        if (this.closed) {
          await this.connection.close()
          this.connection = undefined
          return
        }
        decodeHelloReply(await bounded(this.connection.send(AGDX_HELLO_CODE, new Uint8Array())))
      }
      const payload = encodeNamed(
        new Map<string, unknown>([
          ["producer_presence_version", 1],
          ["observed_at_millis", Date.now()],
          ["expires_after_millis", this.interval * 3],
          [
            "producers",
            this.records.flatMap((reference) => {
              const record = reference.deref()
              return record === undefined ? [] : [record.snapshot()]
            })
          ]
        ])
      )
      if (payload.byteLength <= 65_536)
        await bounded(this.connection.send(AGDX_SET_CLIENT_METADATA_CODE, payload))
    } catch {
      await this.connection?.close().catch(() => undefined)
      this.connection = undefined
    } finally {
      this.timer = undefined
      this.schedule()
    }
  }
}

async function bounded<T>(operation: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      operation,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => {
          reject(new Error("producer telemetry timed out"))
        }, 3_000)
        timer.unref()
      })
    ])
  } finally {
    clearTimeout(timer)
  }
}

function configuredUnsigned(name: string, fallback: number): number {
  const configured = process.env[name]
  if (configured === undefined || !/^[+]?[0-9]+$/.test(configured)) return fallback
  const parsed = Number(configured)
  return Number.isSafeInteger(parsed) ? parsed : fallback
}

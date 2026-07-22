import {
  ContentType,
  jsonCodec,
  parseProjectionId,
  type Laser,
  type Projection,
  type ProjectionBinding
} from "@laserdata/laser-sdk"

import {
  batchSize,
  envBoolean,
  envInteger,
  managedGate,
  PARTITIONS,
  Rng,
  runExample,
  waitForProjection
} from "../common.js"

export const EXAMPLE = "firehose"
const SERVICES = ["api", "billing", "catalog", "worker"] as const
const REGIONS = ["us-east", "us-west", "eu-central"] as const

interface Telemetry {
  readonly org: string
  readonly service: string
  readonly region: string
  readonly status: number
  readonly latency_ms: number
  readonly ts: number
  readonly payload: string
}

function decodeTelemetry(value: unknown): Telemetry {
  if (value === null || typeof value !== "object")
    throw new TypeError("telemetry must be an object")
  const record = value as Partial<Telemetry>
  if (
    typeof record.org !== "string" ||
    typeof record.service !== "string" ||
    typeof record.region !== "string" ||
    !Number.isSafeInteger(record.status) ||
    !Number.isSafeInteger(record.latency_ms) ||
    !Number.isSafeInteger(record.ts) ||
    typeof record.payload !== "string"
  ) {
    throw new TypeError("telemetry fields are invalid")
  }
  return record as Telemetry
}

const TELEMETRY_CODEC = jsonCodec(decodeTelemetry)

function telemetry(org: string, sequence: number, payloadBytes: number, rng: Rng): Telemetry {
  return {
    org,
    service: rng.pick(SERVICES),
    region: rng.pick(REGIONS),
    status: rng.below(100) < 4 ? 500 : 200,
    latency_ms: 5 + rng.below(1_500),
    ts: 1_900_000_000_000_000 + sequence,
    payload: "x".repeat(payloadBytes)
  }
}

async function registerOrg(laser: Laser, topic: string): Promise<void> {
  const id = parseProjectionId(`${topic}.v1`)
  const projection: Projection = {
    id,
    name: topic,
    version: 1,
    kind: { kind: "row" },
    contentType: ContentType.Json,
    extraction: {
      fields: ["org", "service", "region", "status", "latency_ms", "ts"].map((name) => ({
        name,
        pointer: `/${name}`
      })),
      inlinePayload: false
    },
    inlinePayloadDefault: false
  }
  const binding: ProjectionBinding = {
    source: { stream: laser.defaultStream ?? "", topic },
    allowedProjections: [id],
    defaultProjection: id,
    targets: [
      {
        backend: "embedded",
        table: topic,
        role: "readWrite",
        delivery: "effectivelyOnce",
        required: true
      }
    ],
    notify: false
  }
  await laser.projections().register(projection)
  await laser.bindings().apply(binding)
}

async function publishOrg(
  laser: Laser,
  orgIndex: number,
  count: number,
  chunk: number,
  payloadBytes: number
): Promise<number> {
  const org = `org_${String(orgIndex).padStart(2, "0")}`
  const topic = laser.topic(org)
  await topic.ensure(Math.max(1, envInteger("LASER_FIREHOSE_PARTITIONS", PARTITIONS)))
  const typed = topic.json(TELEMETRY_CODEC)
  const rng = new Rng(0x1000n + BigInt(orgIndex))
  let sent = 0
  while (sent < count) {
    const size = Math.min(chunk, count - sent)
    const batch = Array.from({ length: size }, (_, offset) =>
      telemetry(org, sent + offset, payloadBytes, rng)
    )
    sent += await typed.publishBatch(batch)
  }
  return sent
}

async function boundedMap<T>(
  values: readonly T[],
  concurrency: number,
  operation: (value: T) => Promise<number>
): Promise<number> {
  let next = 0
  const totals = await Promise.all(
    Array.from({ length: Math.min(concurrency, values.length) }, async () => {
      let total = 0
      while (next < values.length) {
        const index = next
        next += 1
        const value = values[index]
        if (value !== undefined) total += await operation(value)
      }
      return total
    })
  )
  return totals.reduce((sum, value) => sum + value, 0)
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const orgs = Math.max(1, envInteger("LASER_FIREHOSE_ORGS", 4))
  const perOrg = Math.max(1, envInteger("LASER_FIREHOSE_MESSAGES", 10_000))
  const concurrency = Math.max(1, envInteger("LASER_FIREHOSE_CONCURRENCY", 4))
  const payloadBytes = Math.max(0, envInteger("LASER_FIREHOSE_PAYLOAD_BYTES", 128))
  const chunk = Math.max(1, envInteger("LASER_FIREHOSE_BATCH", batchSize(500)))
  const capabilities = await laser.capabilities()
  const topics = Array.from({ length: orgs }, (_, index) => `org_${String(index).padStart(2, "0")}`)
  if (envBoolean("LASER_FIREHOSE_REGISTER", true) && managedGate(capabilities, "query", EXAMPLE)) {
    for (const topic of topics) await registerOrg(laser, topic)
  }

  const started = performance.now()
  const total = await boundedMap(
    Array.from({ length: orgs }, (_, index) => index),
    concurrency,
    (index) => publishOrg(laser, index, perOrg, chunk, payloadBytes)
  )
  const seconds = Math.max((performance.now() - started) / 1_000, 0.001)
  console.log(
    `published ${String(total)} records across ${String(orgs)} orgs at ${Math.round(total / seconds).toString()} records/s`
  )

  if (capabilities.query.available && envBoolean("LASER_FIREHOSE_QUERY", true)) {
    await waitForProjection(laser, topics[0] ?? "org_00", 1)
    const sample = await laser
      .query(topics[0] ?? "org_00")
      .withTotal()
      .limit(5)
      .fetch()
    console.log(`sample index total: ${(sample.page.total ?? 0n).toString()}`)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

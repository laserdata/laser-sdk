import {
  ContentType,
  Json,
  parseProjectionId,
  type Laser,
  type Projection,
  type ProjectionBinding
} from "@laserdata/laser-sdk"

import {
  batchSize,
  envBoolean,
  envInteger,
  exampleStream,
  indexFor,
  managedGate,
  phase,
  Rng,
  runExample,
  waitForProjection
} from "../common.js"

export const EXAMPLE = "firehose"
const SERVICES = ["api", "storage", "metrics", "worker"] as const
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

const TELEMETRY_CODEC = new Json(decodeTelemetry)

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

async function registerOrg(laser: Laser, topic: string, index: string): Promise<void> {
  const id = parseProjectionId(`${index}.v1`)
  const projection: Projection = {
    id,
    name: index,
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
    source: { stream: exampleStream(laser), topic },
    allowedProjections: [id],
    defaultProjection: id,
    index,
    notify: true
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
  await topic.ensure(Math.max(1, envInteger("LASER_FIREHOSE_PARTITIONS", 8)))
  const rng = new Rng(0x1000n + BigInt(orgIndex))
  let sent = 0
  while (sent < count) {
    const size = Math.min(chunk, count - sent)
    const batch = Array.from({ length: size }, (_, offset) =>
      telemetry(org, sent + offset, payloadBytes, rng)
    )
    await topic.publishBatch().inlinePayload().extendJson(batch, TELEMETRY_CODEC).send()
    sent += size
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
  const orgs = Math.max(1, envInteger("LASER_FIREHOSE_ORGS", 8))
  // The total is split across the organizations, the same as Rust and Python.
  const messages = Math.max(1, envInteger("LASER_FIREHOSE_MESSAGES", 20_000))
  const concurrency = Math.max(1, envInteger("LASER_FIREHOSE_CONCURRENCY", 4))
  const payloadBytes = Math.max(0, envInteger("LASER_FIREHOSE_PAYLOAD_BYTES", 4096))
  const chunk = Math.max(1, envInteger("LASER_FIREHOSE_BATCH", batchSize(500)))
  const capabilities = await laser.capabilities()
  const topics = Array.from({ length: orgs }, (_, index) => `org_${String(index).padStart(2, "0")}`)
  phase("firehose: warming up")
  console.log(
    `${String(messages)} records across ${String(orgs)} orgs, batch ${String(chunk)}, ` +
      `concurrency ${String(concurrency)}, payload ${String(payloadBytes)} bytes`
  )
  if (envBoolean("LASER_FIREHOSE_REGISTER", true) && managedGate(capabilities, "query", EXAMPLE)) {
    phase("provisioning topics and indexes")
    for (const topic of topics) await registerOrg(laser, topic, indexFor(topic))
  }

  phase("firing the hose")
  const perOrg = Math.floor(messages / orgs)
  const remainder = messages % orgs
  const started = performance.now()
  const total = await boundedMap(
    Array.from({ length: orgs }, (_, index) => index),
    concurrency,
    (index) => publishOrg(laser, index, perOrg + (index < remainder ? 1 : 0), chunk, payloadBytes)
  )
  const seconds = Math.max((performance.now() - started) / 1_000, 0.001)
  console.log(
    `published ${String(total)} records across ${String(orgs)} orgs in ${seconds.toFixed(2)}s ` +
      `(${Math.round(total / seconds).toString()} records/s)`
  )

  if (capabilities.query.available && envBoolean("LASER_FIREHOSE_QUERY", true)) {
    for (const [org, topic] of topics.entries()) {
      const expected = perOrg + (org < remainder ? 1 : 0)
      if (expected > 0) await waitForProjection(laser, indexFor(topic), expected)
    }
    phase("sample analytics over the firehose")
    // Index names carry this run's token, so another run or another language's
    // firehose never shares an index (or its rows) with this one.
    const index = indexFor(topics[0] ?? "org_00")
    const sample = await laser.query(index).withTotal().limit(5).fetch()
    console.log(`sample index total: ${(sample.page.total ?? 0n).toString()}`)
    const payload = await laser.query(index).fetchOne(TELEMETRY_CODEC)
    if (payload === undefined) throw new Error("materialized telemetry returned no payload")
    console.log(`sample payload: ${payload.org}/${payload.service} in ${payload.region}`)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

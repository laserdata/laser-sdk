import {
  ContentType,
  parseProjectionId,
  queryResultValue,
  typedValueDiagnosticText,
  type Laser,
  type Projection,
  type ProjectionBinding,
  type QueryResult,
  Json
} from "@laserdata/laser-sdk"

import {
  batchSize,
  exampleStream,
  indexFor,
  managedGate,
  messages,
  PARTITIONS,
  phase,
  printTable,
  Rng,
  runExample,
  waitForProjection,
  waitForSchema
} from "../common.js"

export const EXAMPLE = "fleet-tape"
const FEED = "metrics_feed"
const TAPE = "readings"
const AVRO_TAPE = "readings_avro"
// Index names carry this run's token, so a rerun or another language's example on
// the same deployment never shares their rows.
const TAPE_INDEX = indexFor(TAPE)
const AVRO_TAPE_INDEX = indexFor(AVRO_TAPE)
const HOST = "host"
const CPU = "cpu"
const SAMPLES = "samples"
const LEVEL = "level"
const CPU_TOTAL = "cpu_total"
const MESSAGE_TYPE = "message_type"
const TIMESTAMP = "ts"
const SUM = "sum"
const COLUMNS = [HOST, CPU, SAMPLES, LEVEL, CPU_TOTAL, MESSAGE_TYPE, TIMESTAMP] as const
const OPENING = [
  ["node-1", 42],
  ["node-2", 57],
  ["node-3", 31],
  ["node-4", 68],
  ["node-5", 49]
] as const
const DEGRADED_CPU = 80
const BASE_MICROS = 1_900_000_000_000_000
const LIVE_TIMEOUT_MS = 15_000
const AVRO_READINGS_CAP = 500

type Level = "ok" | "degraded"

interface Reading {
  readonly host: string
  readonly cpu: number
  readonly samples: number
  readonly level: Level
  readonly cpu_total: number
  readonly message_type: "reading"
  readonly ts: number
}

function decodeReading(value: unknown): Reading {
  if (
    value === null ||
    typeof value !== "object" ||
    !("host" in value) ||
    typeof value.host !== "string" ||
    !("cpu" in value) ||
    !Number.isSafeInteger(value.cpu) ||
    !("samples" in value) ||
    !Number.isSafeInteger(value.samples) ||
    !("level" in value) ||
    (value.level !== "ok" && value.level !== "degraded") ||
    !("cpu_total" in value) ||
    !Number.isSafeInteger(value.cpu_total) ||
    !("message_type" in value) ||
    value.message_type !== "reading" ||
    !("ts" in value) ||
    !Number.isSafeInteger(value.ts)
  ) {
    throw new TypeError("reading fields are invalid")
  }
  return {
    host: value.host,
    cpu: value.cpu as number,
    samples: value.samples as number,
    level: value.level,
    cpu_total: value.cpu_total as number,
    message_type: value.message_type,
    ts: value.ts as number
  }
}

const READING_CODEC = new Json(decodeReading)

function readings(count: number): readonly Reading[] {
  const rng = new Rng(0x123456789abcdef0n)
  const load = new Map<string, number>(OPENING)
  let timestamp = BASE_MICROS
  return Array.from({ length: count }, () => {
    const host = rng.pick(OPENING)[0]
    const cpu = Math.min(100, Math.max(0, (load.get(host) ?? 0) + rng.below(15) - 7))
    const samples = 1 + rng.below(500)
    timestamp += 1 + rng.below(50_000)
    load.set(host, cpu)
    return {
      host,
      cpu,
      samples,
      level: cpu >= DEGRADED_CPU ? "degraded" : "ok",
      cpu_total: cpu * samples,
      message_type: "reading",
      ts: timestamp
    }
  })
}

async function registerTape(
  laser: Laser,
  topic: string,
  index: string,
  contentType: ContentType,
  inlinePayloadDefault = false
): Promise<void> {
  const id = parseProjectionId(`${index}.v1`)
  const projection: Projection = {
    id,
    name: index,
    version: 1,
    kind: { kind: "row" },
    contentType,
    extraction: {
      fields: COLUMNS.map((name) => ({ name, pointer: `/${name}` })),
      inlinePayload: inlinePayloadDefault
    },
    inlinePayloadDefault
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

async function publishFeed(laser: Laser, values: readonly Reading[], size: number): Promise<void> {
  const typed = laser.topic(FEED).json(READING_CODEC)
  for (let start = 0; start < values.length; start += size) {
    await typed.publishBatch(values.slice(start, start + size))
    await new Promise((resolve) => setTimeout(resolve, 120))
  }
}

async function publishTape(laser: Laser, values: readonly Reading[], size: number): Promise<void> {
  let published = 0
  for (let start = 0; start < values.length; start += size) {
    const chunk = values.slice(start, start + size)
    await laser.topic(TAPE).publishBatch().inlinePayload().extendJson(chunk, READING_CODEC).send()
    published += chunk.length
    console.log(`indexed ${String(published)}/${String(values.length)} readings to \`${TAPE}\``)
  }
}

type HostLoad = { samples: bigint; cpuTotal: bigint; last: number; degraded: number }

function applyReading(view: Map<string, HostLoad>, reading: Reading): void {
  const current = view.get(reading.host) ?? { samples: 0n, cpuTotal: 0n, last: 0, degraded: 0 }
  view.set(reading.host, {
    samples: current.samples + BigInt(reading.samples),
    cpuTotal: current.cpuTotal + BigInt(reading.cpu_total),
    last: reading.cpu,
    degraded: current.degraded + (reading.level === "degraded" ? 1 : 0)
  })
}

function printView(view: ReadonlyMap<string, HostLoad>): void {
  printTable([
    ["host", "last cpu", "mean cpu", "samples", "degraded"],
    ...[...view]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([host, load]) => [
        host,
        `${String(load.last)}%`,
        `${(load.samples === 0n ? 0n : load.cpuTotal / load.samples).toString()}%`,
        load.samples.toString(),
        String(load.degraded)
      ])
  ])
}

async function streamLiveView(
  laser: Laser,
  values: readonly Reading[],
  size: number
): Promise<void> {
  const records = (await laser.topic(FEED).json(READING_CODEC).records("fleet-tape-builder")).batch(
    Math.max(size, 256)
  )
  const publishing = publishFeed(laser, values, size)
  const view = new Map<string, HostLoad>()
  let seen = 0
  let lastProgress = Date.now()
  while (seen < values.length) {
    const batch = await records.poll()
    if (batch.length === 0) {
      if (Date.now() - lastProgress >= LIVE_TIMEOUT_MS) {
        throw new Error(
          `no reading arrived for ${String(LIVE_TIMEOUT_MS / 1_000)}s after ` +
            `${String(seen)}/${String(values.length)}`
        )
      }
      await new Promise((resolve) => setTimeout(resolve, 5))
      continue
    }
    for (const result of batch) {
      if (result.kind === "error") throw result.error
      applyReading(view, result.record.value)
      seen += 1
    }
    lastProgress = Date.now()
  }
  await publishing
  printView(view)
}

async function tapeHead(laser: Laser): Promise<ReadonlyMap<number, bigint>> {
  const records = await laser.topic(TAPE).json(READING_CODEC).records("fleet-tape-head")
  for (;;) {
    if ((await records.poll()).length === 0) return new Map(records.offsets)
  }
}

async function auditTape(
  laser: Laser,
  values: readonly Reading[],
  offsets: ReadonlyMap<number, bigint>
): Promise<void> {
  const records = await laser.topic(TAPE).json(READING_CODEC).records("fleet-tape-audit")
  records.fromOffsets(offsets)
  const actual = new Map<string, bigint>()
  let audited = 0
  let lastProgress = Date.now()
  while (audited < values.length) {
    const batch = await records.poll()
    if (batch.length === 0) {
      if (Date.now() - lastProgress >= LIVE_TIMEOUT_MS) break
      await new Promise((resolve) => setTimeout(resolve, 5))
      continue
    }
    for (const result of batch) {
      if (result.kind === "error") throw result.error
      const reading = result.record.value
      actual.set(reading.host, (actual.get(reading.host) ?? 0n) + BigInt(reading.cpu_total))
      audited += 1
    }
    lastProgress = Date.now()
  }

  const expected = new Map<string, bigint>()
  for (const reading of values) {
    expected.set(reading.host, (expected.get(reading.host) ?? 0n) + BigInt(reading.cpu_total))
  }
  for (const [host, total] of expected) {
    if (actual.get(host) !== total) {
      throw new Error(`typed tape audit disagrees for ${host}`)
    }
  }
  if (audited !== values.length) {
    throw new Error(`typed tape audit read ${String(audited)}/${String(values.length)} readings`)
  }
  console.log(`audited ${String(audited)} readings, every host's weighted CPU total matches`)
}

function groupTotals(result: QueryResult): ReadonlyMap<string, bigint> {
  const totals = new Map<string, bigint>()
  for (const row of result.rows) {
    const host = queryResultValue(result, row, HOST)
    const total = queryResultValue(result, row, SUM)
    if (host !== undefined && total !== undefined) {
      totals.set(typedValueDiagnosticText(host), BigInt(typedValueDiagnosticText(total)))
    }
  }
  return totals
}

async function reportSamplesAndMean(laser: Laser): Promise<void> {
  const samples = groupTotals(await laser.query(TAPE_INDEX).sum(SAMPLES).groupBy([HOST]).fetch())
  const cpuTotal = groupTotals(await laser.query(TAPE_INDEX).sum(CPU_TOTAL).groupBy([HOST]).fetch())
  printTable([
    ["host", "samples", "mean cpu"],
    ...[...samples]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([host, count]) => {
        const total = cpuTotal.get(host) ?? 0n
        const mean = count === 0n ? 0n : total / count
        return [host, count.toString(), `${mean.toString()}%`]
      })
  ])

  const payload = await laser.query(TAPE_INDEX).fetchOne(READING_CODEC)
  if (payload === undefined) throw new Error("materialized reading tape returned no payload")
  console.log(`payload round trip: ${payload.host} cpu ${String(payload.cpu)}% ${payload.level}`)
}

async function publishAvroTape(
  laser: Laser,
  values: readonly Reading[],
  size: number
): Promise<void> {
  const schemaId = await laser
    .schemas()
    .register({
      kind: "avro",
      schema: JSON.stringify({
        type: "record",
        name: "HostReading",
        fields: [
          { name: HOST, type: "string" },
          { name: CPU, type: "long" },
          { name: SAMPLES, type: "int" },
          { name: LEVEL, type: "string" },
          { name: CPU_TOTAL, type: "long" },
          { name: MESSAGE_TYPE, type: "string" },
          { name: TIMESTAMP, type: "long" }
        ]
      })
    })
    .name("fleet_reading")
    .version(1)
    .send()
  const avro = laser.topic(AVRO_TAPE)
  await avro.ensure(PARTITIONS)
  await registerTape(laser, AVRO_TAPE, AVRO_TAPE_INDEX, ContentType.Avro, true)
  await waitForSchema(laser, schemaId)
  const typed = await avro.schema(schemaId, decodeReading)
  const subset = values.slice(0, AVRO_READINGS_CAP)
  for (let start = 0; start < subset.length; start += size) {
    await typed.publishBatch(subset.slice(start, start + size))
  }
  await waitForProjection(laser, AVRO_TAPE_INDEX, subset.length)
  const totals = groupTotals(
    await laser.query(AVRO_TAPE_INDEX).sum(CPU_TOTAL).groupBy([HOST]).fetch()
  )
  printTable([
    ["host", "Avro weighted CPU total"],
    ...[...totals]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([host, total]) => [host, total.toString()])
  ])
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const count = messages(2_000)
  const chunk = Math.min(batchSize(100), count)
  const values = readings(count)
  const capabilities = await laser.capabilities()

  phase("warming up")
  await laser.topic(FEED).ensure(PARTITIONS)
  await laser.topic(TAPE).ensure(PARTITIONS)
  if (capabilities.query.available) await registerTape(laser, TAPE, TAPE_INDEX, ContentType.Json)

  phase("streaming a live telemetry feed")
  console.log(`${String(count)} readings across ${String(OPENING.length)} hosts`)
  await streamLiveView(laser, values, Math.min(chunk, 40))

  phase("publishing the readings to the durable reading tape")
  const offsets = await tapeHead(laser)
  await publishTape(laser, values, chunk)

  if (managedGate(capabilities, "query", EXAMPLE)) {
    await waitForProjection(laser, TAPE_INDEX, values.length)
    phase("reading-tape analytics")
    await reportSamplesAndMean(laser)
  }

  phase("typed tape audit: replay the log as Reading values")
  await auditTape(laser, values, offsets)

  if (capabilities.managed) {
    phase("schema-first tape: Avro readings decoded by a registered writer schema")
    await publishAvroTape(laser, values, chunk)
  } else {
    console.log("writer schemas need Laser Stack or LaserData Cloud, skipping the Avro tape")
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

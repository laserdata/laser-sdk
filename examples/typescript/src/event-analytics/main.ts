import {
  ContentType,
  InMemoryStore,
  parseProjectionId,
  type Codec,
  type Laser,
  type Projection,
  type ProjectionBinding,
  type QueryResult,
  type TypedRecords
} from "@laserdata/laser-sdk"
import {
  batchSize,
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

export const EXAMPLE = "event-analytics"
export const TOPIC = "clickstream"
const CHECKPOINT = "clickstream-export"
const GUARDED_TOPIC = "clickstream_guarded"
const USER_ID = "user_id"
const MESSAGE_TYPE = "message_type"
const ROUTE = "route"
const LATENCY_MS = "latency_ms"
const TS = "ts"
const COUNT = "count"
const WINDOW_START = "window_start"
const COLUMNS = [USER_ID, MESSAGE_TYPE, ROUTE, LATENCY_MS, TS] as const
const VISITORS = [
  "alice",
  "bob",
  "carol",
  "dave",
  "erin",
  "frank",
  "grace",
  "heidi",
  "ivan",
  "judy",
  "mallory",
  "oscar"
] as const
const ROUTES = [
  "/home",
  "/product/42",
  "/product/7",
  "/search",
  "/cart",
  "/checkout",
  "/pricing",
  "/docs"
] as const
const BASE_MICROS = 1_900_000_000_000_000
const ONE_MINUTE_MICROS = 60_000_000n
const LIVE_TIMEOUT_MS = 15_000
const LIVE_GROUP = "event-analytics-live"

export interface ClickEvent {
  readonly user_id: string
  readonly message_type: "page_view" | "add_to_cart" | "checkout"
  readonly route: string
  readonly latency_ms: number
  readonly ts: number
}

function decodeEvent(bytes: Uint8Array): ClickEvent {
  const value: unknown = JSON.parse(new TextDecoder().decode(bytes))
  if (typeof value !== "object" || value === null) throw new Error("event must be an object")
  const event = value as Partial<ClickEvent>
  if (
    typeof event.user_id !== "string" ||
    !["page_view", "add_to_cart", "checkout"].includes(event.message_type ?? "") ||
    typeof event.route !== "string" ||
    !Number.isSafeInteger(event.latency_ms) ||
    !Number.isSafeInteger(event.ts)
  ) {
    throw new Error("event fields are invalid")
  }
  return event as ClickEvent
}

function decodeEventValue(value: unknown): ClickEvent {
  return decodeEvent(new TextEncoder().encode(JSON.stringify(value)))
}

const CLICK_EVENT_SCHEMA = JSON.stringify({
  $schema: "https://json-schema.org/draft/2020-12/schema",
  type: "object",
  additionalProperties: false,
  required: ["user_id", "message_type", "route", "latency_ms", "ts"],
  properties: {
    user_id: { type: "string" },
    message_type: { enum: ["page_view", "add_to_cart", "checkout"] },
    route: { type: "string" },
    latency_ms: { type: "integer" },
    ts: { type: "integer" }
  }
})

export const CLICK_EVENT_CODEC: Codec<ClickEvent> = {
  encode: (event) => new TextEncoder().encode(JSON.stringify(event)),
  decode: decodeEvent
}

export function clickstream(count: number): readonly ClickEvent[] {
  const rng = new Rng(0x123456789abcdef0n)
  let timestamp = BASE_MICROS
  return Array.from({ length: count }, () => {
    const roll = rng.below(100)
    const messageType = roll < 70 ? "page_view" : roll < 92 ? "add_to_cart" : "checkout"
    const event: ClickEvent = {
      user_id: rng.pick(VISITORS),
      message_type: messageType,
      route: rng.pick(ROUTES),
      latency_ms: 30 + rng.below(600),
      ts: timestamp
    }
    timestamp += 1 + rng.below(30_000_000)
    return event
  })
}

async function registerProjection(
  laser: Laser,
  topic: string,
  inlinePayloadDefault = false
): Promise<void> {
  const id = parseProjectionId(`${topic}.v1`)
  const projection: Projection = {
    id,
    name: topic,
    version: 1,
    kind: { kind: "row" },
    contentType: ContentType.Json,
    extraction: {
      fields: COLUMNS.map((name) => ({
        name,
        pointer: `/${name}`
      })),
      inlinePayload: inlinePayloadDefault
    },
    inlinePayloadDefault
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
    notify: true
  }
  await laser.projections().register(projection)
  await laser.bindings().apply(binding)
}

async function publishClickstream(laser: Laser, events: readonly ClickEvent[]): Promise<void> {
  const chunkSize = Math.min(batchSize(30), events.length)
  let published = 0
  for (let start = 0; start < events.length; start += chunkSize) {
    const chunk = events.slice(start, start + chunkSize)
    await laser
      .topic(TOPIC)
      .publishBatch()
      .inlinePayload()
      .extendJson(chunk, CLICK_EVENT_CODEC)
      .send()
    published += chunk.length
    console.log(`published ${String(published)}/${String(events.length)} events to \`${TOPIC}\``)
  }
}

async function guardedIngest(laser: Laser, sample: ClickEvent): Promise<void> {
  const schemaId = await laser
    .schemas()
    .register({ kind: "jsonSchema", schema: CLICK_EVENT_SCHEMA })
    .name("ClickEvent")
    .version(1)
    .send()
  console.log(`allocated writer-schema id ${String(schemaId)} for the ClickEvent guard`)
  await laser.topic(GUARDED_TOPIC).ensure(PARTITIONS)
  await registerProjection(laser, GUARDED_TOPIC, true)
  await waitForSchema(laser, schemaId)
  const guarded = await laser.topic(GUARDED_TOPIC).schema(schemaId, decodeEventValue)
  await guarded.publish(sample)
  let rejected = false
  try {
    await guarded.publish({
      ...sample,
      latency_ms: "slow"
    } as unknown as ClickEvent)
  } catch {
    rejected = true
  }
  if (!rejected) throw new Error("the malformed event unexpectedly passed schema validation")
  await laser
    .topic(GUARDED_TOPIC)
    .publish()
    .rawBytes(
      new TextEncoder().encode(
        `{"user_id":"mallory","message_type":"checkout","route":"/checkout","latency_ms":"fast","ts":1}`
      ),
      ContentType.Json
    )
    .schemaId(schemaId)
    .send()
  await waitForProjection(laser, GUARDED_TOPIC, 1)
  await new Promise((resolve) => setTimeout(resolve, 1_000))
  const result = await laser.query(GUARDED_TOPIC).withTotal().fetch()
  if (result.page.total !== 1n) throw new Error("the malformed event must not materialize")
  console.log(`guarded JSON rows: ${result.page.total.toString()}`)
}

function scalar(result: QueryResult): string {
  return result.rows[0]?.headers.get(COUNT) ?? "0"
}

async function runAnalytics(laser: Laser): Promise<void> {
  const byKind = await laser.query(TOPIC).count().groupBy([MESSAGE_TYPE]).fetch()
  printTable([
    ["event", "count"],
    ...byKind.rows.map((row) => [
      row.headers.get(MESSAGE_TYPE) ?? "?",
      row.headers.get(COUNT) ?? "0"
    ])
  ])

  const slowest = await laser.query(TOPIC).orderDesc(LATENCY_MS).limit(3).fetch()
  console.log("slowest 3 routes")
  printTable([
    ["route", "latency"],
    ...slowest.rows.map((row) => [
      row.headers.get(ROUTE) ?? "?",
      `${row.headers.get(LATENCY_MS) ?? "?"}ms`
    ])
  ])

  const checkouts = await laser.query(TOPIC).messageType("checkout").count().fetch()
  console.log(`checkouts: ${scalar(checkouts)}`)

  const start = BigInt(BASE_MICROS)
  const firstWindow = await laser
    .query(TOPIC)
    .timeRange(start, start + 5n * ONE_MINUTE_MICROS)
    .count()
    .fetch()
  console.log(`events in the first 5 minutes: ${scalar(firstWindow)}`)

  const perMinute = await laser.query(TOPIC).count().window(TS, ONE_MINUTE_MICROS).fetch()
  console.log("events per minute")
  printTable([
    ["window start", "count"],
    ...perMinute.rows.map((row) => [
      row.headers.get(WINDOW_START) ?? "?",
      row.headers.get(COUNT) ?? "0"
    ])
  ])

  const metrics = await laser
    .query(TOPIC)
    .avg(LATENCY_MS)
    .countDistinct(ROUTE)
    .groupBy([MESSAGE_TYPE])
    .fetch()
  console.log("latency and route breadth by event")
  printTable([
    ["event", "avg latency", "routes"],
    ...metrics.rows.map((row) => [
      row.headers.get(MESSAGE_TYPE) ?? "?",
      `${row.headers.get("avg") ?? "?"}ms`,
      row.headers.get("count_distinct") ?? "?"
    ])
  ])

  const payload = await laser.query(TOPIC).fetchOne(CLICK_EVENT_CODEC)
  if (payload === undefined) throw new Error("materialized clickstream returned no payload")
  console.log(`payload round trip: ${payload.message_type} on ${payload.route}`)
}

async function drain<T>(records: TypedRecords<T>, limit?: number): Promise<number> {
  let total = 0
  for (;;) {
    const batch = await records.poll()
    if (batch.length === 0) return total
    for (const result of batch) if (result.kind === "error") throw result.error
    total += batch.length
    if (limit !== undefined && total >= limit) return total
  }
}

async function checkpointedExport(laser: Laser): Promise<void> {
  const total = await drain(
    await laser.topic(TOPIC).json(CLICK_EVENT_CODEC).records("event-analytics-count")
  )
  const state = new InMemoryStore()
  const records = await laser
    .topic(TOPIC)
    .json(CLICK_EVENT_CODEC)
    .records("event-analytics-export", { batchSize: 1 })
  const first = await drain(records, 1)
  const offsets = [...records.offsets].map(([partition, offset]) => [partition, offset.toString()])
  await state.set(CHECKPOINT, new TextEncoder().encode(JSON.stringify(offsets)))

  const saved = await state.get(CHECKPOINT)
  if (saved === undefined) throw new Error("checkpoint was not persisted")
  const parsed = JSON.parse(new TextDecoder().decode(saved)) as readonly (readonly [
    number,
    string
  ])[]
  const resumed = await laser.topic(TOPIC).json(CLICK_EVENT_CODEC).records("event-analytics-tail")
  resumed.fromOffsets(new Map(parsed.map(([partition, offset]) => [partition, BigInt(offset)])))
  const tail = await drain(resumed)
  if (first + tail !== total) throw new Error("checkpoint resume lost or duplicated records")
  console.log(
    `checkpointed export resumed with ${String(first + tail)}/${String(total)} records, no replay duplication`
  )
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const count = messages(180)
  const events = clickstream(count)
  const capabilities = await laser.capabilities()
  phase("warming up")
  await laser.topic(TOPIC).ensure(PARTITIONS)
  if (capabilities.query.available) await registerProjection(laser, TOPIC)

  await using live = await laser.topic(TOPIC).consumerGroup(LIVE_GROUP, {
    batchLength: 100,
    pollIntervalMs: 5
  })
  const publishing = publishClickstream(laser, events)
  phase("hot path: a live reader tails the stream while the producer runs")
  let seen = 0
  let checkouts = 0
  while (seen < count) {
    const message = await live.nextWithin(LIVE_TIMEOUT_MS)
    if (message === null) {
      throw new Error(
        `no event arrived for ${String(LIVE_TIMEOUT_MS / 1_000)}s after ${String(seen)}/${String(count)}`
      )
    }
    seen += 1
    if (CLICK_EVENT_CODEC.decode(message.payload).message_type === "checkout") checkouts += 1
  }
  await publishing
  console.log(`live fold: ${String(seen)} events, ${String(checkouts)} checkouts`)

  phase("read model: a resumable downstream reader")
  await checkpointedExport(laser)

  if (managedGate(capabilities, "query", EXAMPLE)) {
    phase("read model: ad-hoc analytics over the managed query layer")
    await waitForProjection(laser, TOPIC, count)
    await runAnalytics(laser)
    const sample = events[0]
    if (sample === undefined) throw new Error("the deterministic clickstream is empty")
    phase("validated ingest: a JSON Schema guards the index")
    await guardedIngest(laser, sample)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

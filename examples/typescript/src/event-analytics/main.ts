import {
  ContentType,
  InMemoryStore,
  parseProjectionId,
  type Codec,
  type Laser,
  type Projection,
  type ProjectionBinding,
  type TypedRecords
} from "@laserdata/laser-sdk"
import {
  managedGate,
  messages,
  PARTITIONS,
  Rng,
  runExample,
  waitForProjection,
  waitForSchema
} from "../common.js"

export const EXAMPLE = "event-analytics"
export const TOPIC = "clickstream"
const CHECKPOINT = "clickstream-export"
const GUARDED_TOPIC = "clickstream_guarded"
const VISITORS = ["alice", "bob", "carol", "dave", "erin", "frank"] as const
const ROUTES = ["/home", "/product/42", "/search", "/cart", "/checkout"] as const
const BASE_MICROS = 1_900_000_000_000_000
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

async function registerProjection(laser: Laser, topic: string): Promise<void> {
  const id = parseProjectionId(`${topic}.v1`)
  const projection: Projection = {
    id,
    name: topic,
    version: 1,
    kind: { kind: "row" },
    contentType: ContentType.Json,
    extraction: {
      fields: ["user_id", "message_type", "route", "latency_ms", "ts"].map((name) => ({
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
    notify: true
  }
  await laser.projections().register(projection)
  await laser.bindings().apply(binding)
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
  await registerProjection(laser, GUARDED_TOPIC)
  // The register reply carries a durable id, but the apply is asynchronous:
  // read back until browse resolves it before the first publish against it.
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
  // The raw path skips client-side validation, so the malformed body reaches
  // the deployment and only the server-side schema guard can reject it.
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
  // Give the projector a beat to (wrongly) materialize the malformed event
  // before pinning the count.
  await new Promise((resolve) => setTimeout(resolve, 1_000))
  const result = await laser.query(GUARDED_TOPIC).withTotal().fetch()
  if (result.page.total !== 1n) throw new Error("the malformed event must not materialize")
  console.log(`guarded JSON rows: ${result.page.total.toString()}`)
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

// A downstream export job tails the same log with an offsets checkpoint in a
// `StateStore`, so a restart resumes exactly where it stopped: the first poll
// plus the resumed tail must cover every record on the topic exactly once.
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
  const capabilities = await laser.capabilities()
  const typed = laser.topic(TOPIC).json(CLICK_EVENT_CODEC)
  await laser.topic(TOPIC).ensure(PARTITIONS)
  if (capabilities.query.available) await registerProjection(laser, TOPIC)

  // The hot path: a consumer-group reader folding a rolling ops ticker off
  // the raw log while the producer streams. The group starts at the tail and
  // commits server-side on each poll, so a re-run never re-reads old events.
  const live = await laser.topic(TOPIC).consumerGroup(LIVE_GROUP, {
    batchLength: 100,
    pollIntervalMs: 5
  })
  const publishing = typed.publishBatch(clickstream(count))
  let seen = 0
  let checkouts = 0
  try {
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
  } finally {
    await live.shutdown()
  }
  await publishing
  console.log(`live fold: ${String(seen)} events, ${String(checkouts)} checkouts`)

  await checkpointedExport(laser)

  if (managedGate(capabilities, "query", EXAMPLE)) {
    await waitForProjection(laser, TOPIC, count)
    const result = await laser.query(TOPIC).messageType("checkout").withTotal().fetch()
    console.log(`managed checkout rows: ${(result.page.total ?? 0n).toString()}`)
    const sample = clickstream(1)[0]
    if (sample === undefined) throw new Error("the deterministic clickstream is empty")
    await guardedIngest(laser, sample)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

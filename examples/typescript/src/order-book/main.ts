import {
  ContentType,
  parseProjectionId,
  type Laser,
  type Projection,
  type ProjectionBinding,
  type QueryResult,
  jsonCodec
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

export const EXAMPLE = "order-book"
const FEED = "md_feed"
const TAPE = "trades"
const AVRO_TAPE = "trades_avro"
const SYMBOL = "symbol"
const PRICE = "price_cents"
const QUANTITY = "qty"
const SIDE = "side"
const NOTIONAL = "notional_cents"
const MESSAGE_TYPE = "message_type"
const TIMESTAMP = "ts"
const SUM = "sum"
const COLUMNS = [SYMBOL, PRICE, QUANTITY, SIDE, NOTIONAL, MESSAGE_TYPE, TIMESTAMP] as const
const OPENING = [
  ["AAPL", 21_350],
  ["MSFT", 42_010],
  ["NVDA", 121_540],
  ["AMZN", 18_520],
  ["GOOG", 17_890]
] as const
const BASE_MICROS = 1_900_000_000_000_000
const LIVE_TIMEOUT_MS = 15_000
const AVRO_FILLS_CAP = 500

type Side = "buy" | "sell"

interface Fill {
  readonly symbol: string
  readonly price_cents: number
  readonly qty: number
  readonly side: Side
  readonly notional_cents: number
  readonly message_type: "fill"
  readonly ts: number
}

function decodeFill(value: unknown): Fill {
  if (
    value === null ||
    typeof value !== "object" ||
    !("symbol" in value) ||
    typeof value.symbol !== "string" ||
    !("price_cents" in value) ||
    !Number.isSafeInteger(value.price_cents) ||
    !("qty" in value) ||
    !Number.isSafeInteger(value.qty) ||
    !("side" in value) ||
    (value.side !== "buy" && value.side !== "sell") ||
    !("notional_cents" in value) ||
    !Number.isSafeInteger(value.notional_cents) ||
    !("message_type" in value) ||
    value.message_type !== "fill" ||
    !("ts" in value) ||
    !Number.isSafeInteger(value.ts)
  ) {
    throw new TypeError("fill fields are invalid")
  }
  return {
    symbol: value.symbol,
    price_cents: value.price_cents as number,
    qty: value.qty as number,
    side: value.side,
    notional_cents: value.notional_cents as number,
    message_type: value.message_type,
    ts: value.ts as number
  }
}

const FILL_CODEC = jsonCodec(decodeFill)

function fills(count: number): readonly Fill[] {
  const rng = new Rng(0x123456789abcdef0n)
  const prices = new Map<string, number>(OPENING)
  let timestamp = BASE_MICROS
  return Array.from({ length: count }, () => {
    const symbol = rng.pick(OPENING)[0]
    const price = Math.max(1, (prices.get(symbol) ?? 1) + rng.below(31) - 15)
    const quantity = 1 + rng.below(500)
    const side: Side = (rng.nextU64() & 1n) === 0n ? "buy" : "sell"
    timestamp += 1 + rng.below(50_000)
    prices.set(symbol, price)
    return {
      symbol,
      price_cents: price,
      qty: quantity,
      side,
      notional_cents: price * quantity,
      message_type: "fill",
      ts: timestamp
    }
  })
}

async function registerTape(
  laser: Laser,
  topic: string,
  contentType: ContentType,
  inlinePayloadDefault = false
): Promise<void> {
  const id = parseProjectionId(`${topic}.v1`)
  const projection: Projection = {
    id,
    name: topic,
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

async function publishFeed(laser: Laser, values: readonly Fill[], size: number): Promise<void> {
  const typed = laser.topic(FEED).json(FILL_CODEC)
  for (let start = 0; start < values.length; start += size) {
    await typed.publishBatch(values.slice(start, start + size))
    await new Promise((resolve) => setTimeout(resolve, 120))
  }
}

async function publishTape(laser: Laser, values: readonly Fill[], size: number): Promise<void> {
  let published = 0
  for (let start = 0; start < values.length; start += size) {
    const chunk = values.slice(start, start + size)
    await laser.topic(TAPE).publishBatch().inlinePayload().extendJson(chunk, FILL_CODEC).send()
    published += chunk.length
    console.log(`indexed ${String(published)}/${String(values.length)} fills to \`${TAPE}\``)
  }
}

type BookLevel = { volume: bigint; notional: bigint; last: number }

function applyFill(book: Map<string, BookLevel>, fill: Fill): void {
  const current = book.get(fill.symbol) ?? { volume: 0n, notional: 0n, last: 0 }
  book.set(fill.symbol, {
    volume: current.volume + BigInt(fill.qty),
    notional: current.notional + BigInt(fill.notional_cents),
    last: fill.price_cents
  })
}

function printBook(book: ReadonlyMap<string, BookLevel>): void {
  printTable([
    ["symbol", "last", "volume", "VWAP"],
    ...[...book]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([symbol, level]) => [
        symbol,
        (level.last / 100).toFixed(2),
        level.volume.toString(),
        (Number(level.volume === 0n ? 0n : level.notional / level.volume) / 100).toFixed(2)
      ])
  ])
}

async function streamLiveBook(laser: Laser, values: readonly Fill[], size: number): Promise<void> {
  const records = await laser
    .topic(FEED)
    .json(FILL_CODEC)
    .records("order-book-builder", {
      batchSize: Math.max(size, 256)
    })
  const publishing = publishFeed(laser, values, size)
  const book = new Map<string, BookLevel>()
  let seen = 0
  let lastProgress = Date.now()
  while (seen < values.length) {
    const batch = await records.poll()
    if (batch.length === 0) {
      if (Date.now() - lastProgress >= LIVE_TIMEOUT_MS) {
        throw new Error(
          `no fill arrived for ${String(LIVE_TIMEOUT_MS / 1_000)}s after ` +
            `${String(seen)}/${String(values.length)}`
        )
      }
      await new Promise((resolve) => setTimeout(resolve, 5))
      continue
    }
    for (const result of batch) {
      if (result.kind === "error") throw result.error
      applyFill(book, result.record.value)
      seen += 1
    }
    lastProgress = Date.now()
  }
  await publishing
  printBook(book)
}

async function tapeHead(laser: Laser): Promise<ReadonlyMap<number, bigint>> {
  const records = await laser.topic(TAPE).json(FILL_CODEC).records("order-book-head")
  for (;;) {
    if ((await records.poll()).length === 0) return new Map(records.offsets)
  }
}

async function auditTape(
  laser: Laser,
  values: readonly Fill[],
  offsets: ReadonlyMap<number, bigint>
): Promise<void> {
  const records = await laser.topic(TAPE).json(FILL_CODEC).records("order-book-audit")
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
      const fill = result.record.value
      actual.set(fill.symbol, (actual.get(fill.symbol) ?? 0n) + BigInt(fill.notional_cents))
      audited += 1
    }
    lastProgress = Date.now()
  }

  const expected = new Map<string, bigint>()
  for (const fill of values) {
    expected.set(fill.symbol, (expected.get(fill.symbol) ?? 0n) + BigInt(fill.notional_cents))
  }
  for (const [symbol, notional] of expected) {
    if (actual.get(symbol) !== notional) {
      throw new Error(`typed tape audit disagrees for ${symbol}`)
    }
  }
  if (audited !== values.length) {
    throw new Error(`typed tape audit read ${String(audited)}/${String(values.length)} fills`)
  }
  console.log(`audited ${String(audited)} fills, every symbol's notional matches`)
}

function groupTotals(result: QueryResult): ReadonlyMap<string, bigint> {
  const totals = new Map<string, bigint>()
  for (const row of result.rows) {
    const symbol = row.headers.get(SYMBOL)
    const total = row.headers.get(SUM)
    if (symbol !== undefined && total !== undefined) totals.set(symbol, BigInt(total))
  }
  return totals
}

async function reportVolumeAndVwap(laser: Laser): Promise<void> {
  const volume = groupTotals(await laser.query(TAPE).sum(QUANTITY).groupBy([SYMBOL]).fetch())
  const notional = groupTotals(await laser.query(TAPE).sum(NOTIONAL).groupBy([SYMBOL]).fetch())
  printTable([
    ["symbol", "volume", "VWAP"],
    ...[...volume]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([symbol, quantity]) => {
        const total = notional.get(symbol) ?? 0n
        const vwap = quantity === 0n ? 0 : Number(total / quantity) / 100
        return [symbol, quantity.toString(), vwap.toFixed(2)]
      })
  ])

  const payload = await laser.query(TAPE).fetchOne(FILL_CODEC)
  if (payload === undefined) throw new Error("materialized trade tape returned no payload")
  console.log(`payload round trip: ${payload.side} ${String(payload.qty)} ${payload.symbol}`)
}

async function publishAvroTape(laser: Laser, values: readonly Fill[], size: number): Promise<void> {
  const schemaId = await laser
    .schemas()
    .register({
      kind: "avro",
      schema: JSON.stringify({
        type: "record",
        name: "Fill",
        fields: [
          { name: SYMBOL, type: "string" },
          { name: PRICE, type: "long" },
          { name: QUANTITY, type: "int" },
          { name: SIDE, type: "string" },
          { name: NOTIONAL, type: "long" },
          { name: MESSAGE_TYPE, type: "string" },
          { name: TIMESTAMP, type: "long" }
        ]
      })
    })
    .name("Fill")
    .version(1)
    .send()
  const avro = laser.topic(AVRO_TAPE)
  await avro.ensure(PARTITIONS)
  await registerTape(laser, AVRO_TAPE, ContentType.Avro, true)
  await waitForSchema(laser, schemaId)
  const typed = await avro.schema(schemaId, decodeFill)
  const subset = values.slice(0, AVRO_FILLS_CAP)
  for (let start = 0; start < subset.length; start += size) {
    await typed.publishBatch(subset.slice(start, start + size))
  }
  await waitForProjection(laser, AVRO_TAPE, subset.length)
  const notionals = groupTotals(
    await laser.query(AVRO_TAPE).sum(NOTIONAL).groupBy([SYMBOL]).fetch()
  )
  printTable([
    ["symbol", "Avro notional"],
    ...[...notionals]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([symbol, total]) => [symbol, total.toString()])
  ])
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const count = messages(2_000)
  const chunk = Math.min(batchSize(100), count)
  const values = fills(count)
  const capabilities = await laser.capabilities()

  phase("warming up")
  await laser.topic(FEED).ensure(PARTITIONS)
  await laser.topic(TAPE).ensure(PARTITIONS)
  if (capabilities.query.available) await registerTape(laser, TAPE, ContentType.Json)

  phase("streaming a live market feed")
  console.log(`${String(count)} fills across ${String(OPENING.length)} symbols`)
  await streamLiveBook(laser, values, Math.min(chunk, 40))

  phase("publishing the fills to the durable trade tape")
  const offsets = await tapeHead(laser)
  await publishTape(laser, values, chunk)

  if (managedGate(capabilities, "query", EXAMPLE)) {
    await waitForProjection(laser, TAPE, values.length)
    phase("trade-tape analytics")
    await reportVolumeAndVwap(laser)
  }

  phase("typed tape audit: replay the log as Fill values")
  await auditTape(laser, values, offsets)

  if (capabilities.managed) {
    phase("schema-first tape: Avro fills decoded by a registered writer schema")
    await publishAvroTape(laser, values, chunk)
  } else {
    console.log("writer schemas live on LaserData Cloud, skipping the Avro tape")
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

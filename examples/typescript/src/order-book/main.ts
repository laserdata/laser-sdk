import {
  ContentType,
  parseProjectionId,
  type Laser,
  type Projection,
  type ProjectionBinding,
  jsonCodec
} from "@laserdata/laser-sdk"

import {
  batchSize,
  managedGate,
  messages,
  PARTITIONS,
  Rng,
  runExample,
  waitForProjection,
  waitForSchema
} from "../common.js"

export const EXAMPLE = "order-book"
const TAPE = "trades"
const AVRO_TAPE = "trades_avro"
const SYMBOLS = ["LASR", "IGGY", "DATA", "AGNT"] as const
const BASE_MICROS = 1_900_000_000_000_000

interface Fill {
  readonly symbol: string
  readonly price_cents: number
  readonly size: number
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
    !("size" in value) ||
    !Number.isSafeInteger(value.size) ||
    !("ts" in value) ||
    !Number.isSafeInteger(value.ts)
  ) {
    throw new TypeError("fill fields are invalid")
  }
  return {
    symbol: value.symbol,
    price_cents: value.price_cents as number,
    size: value.size as number,
    ts: value.ts as number
  }
}

const FILL_CODEC = jsonCodec(decodeFill)

function fills(count: number): readonly Fill[] {
  const rng = new Rng(0xfeed5eedn)
  const prices = new Map(SYMBOLS.map((symbol, index) => [symbol, 10_000 + index * 2_500]))
  return Array.from({ length: count }, (_, index) => {
    const symbol = rng.pick(SYMBOLS)
    const price = (prices.get(symbol) ?? 10_000) + rng.below(21) - 10
    prices.set(symbol, price)
    return {
      symbol,
      price_cents: price,
      size: 1 + rng.below(250),
      ts: BASE_MICROS + index * 1_000
    }
  })
}

async function registerTape(laser: Laser, topic: string, contentType: ContentType): Promise<void> {
  const id = parseProjectionId(`${topic}.v1`)
  const projection: Projection = {
    id,
    name: topic,
    version: 1,
    kind: { kind: "row" },
    contentType,
    extraction: {
      fields: ["symbol", "price_cents", "size", "ts"].map((name) => ({
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

async function publishInChunks(
  publish: (batch: readonly Fill[]) => Promise<number>,
  values: readonly Fill[],
  size: number
): Promise<void> {
  for (let start = 0; start < values.length; start += size) {
    await publish(values.slice(start, start + size))
  }
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const count = messages(1_000)
  const chunk = Math.min(batchSize(100), count)
  const values = fills(count)
  const tape = laser.topic(TAPE)
  await tape.ensure(PARTITIONS)
  const records = await tape.json(FILL_CODEC).records("order-book-fold", { batchSize: chunk })
  const publishing = publishInChunks(
    (batch) => tape.json(FILL_CODEC).publishBatch(batch),
    values,
    chunk
  )

  const totals = new Map<string, { volume: bigint; notional: bigint; last: number }>()
  let seen = 0
  while (seen < count) {
    for (const result of await records.poll()) {
      if (result.kind === "error") throw result.error
      const fill = result.record.value
      const current = totals.get(fill.symbol) ?? {
        volume: 0n,
        notional: 0n,
        last: 0
      }
      totals.set(fill.symbol, {
        volume: current.volume + BigInt(fill.size),
        notional: current.notional + BigInt(fill.price_cents) * BigInt(fill.size),
        last: fill.price_cents
      })
      seen += 1
    }
    if (seen < count) await new Promise((resolve) => setTimeout(resolve, 5))
  }
  await publishing
  for (const [symbol, total] of [...totals].sort(([left], [right]) => left.localeCompare(right))) {
    const vwap = total.volume === 0n ? 0n : total.notional / total.volume
    console.log(
      `${symbol}: last=${String(total.last)} volume=${total.volume.toString()} vwap=${vwap.toString()}`
    )
  }

  const capabilities = await laser.capabilities()
  if (!managedGate(capabilities, "query", EXAMPLE)) return
  await registerTape(laser, TAPE, ContentType.Json)
  const schemaId = await laser
    .schemas()
    .register({
      kind: "avro",
      schema: JSON.stringify({
        type: "record",
        name: "Fill",
        fields: [
          { name: "symbol", type: "string" },
          { name: "price_cents", type: "long" },
          { name: "size", type: "long" },
          { name: "ts", type: "long" }
        ]
      })
    })
    .name("Fill")
    .version(1)
    .send()
  const avro = laser.topic(AVRO_TAPE)
  await avro.ensure(PARTITIONS)
  await registerTape(laser, AVRO_TAPE, ContentType.Avro)
  await waitForSchema(laser, schemaId)
  const typedAvro = await avro.schema(schemaId, decodeFill)
  await publishInChunks((batch) => typedAvro.publishBatch(batch), values, chunk)
  await waitForProjection(laser, TAPE, values.length)
  const indexed = await laser.query(TAPE).withTotal().fetch()
  console.log(`managed JSON tape rows: ${(indexed.page.total ?? 0n).toString()}`)
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

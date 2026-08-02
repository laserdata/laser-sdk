import { jsonCodec, type Laser, type TypedRecord, type TypedRecords } from "@laserdata/laser-sdk"
import { phase, runExample } from "../common.js"

export const EXAMPLE = "log"
const STREAM = "shop"
const TOPIC = "orders"
const PARTITIONS = 2
const REPLAY_TIMEOUT_MS = 10_000

interface Order {
  readonly id: number
  readonly total: number
}

const ORDERS: readonly Order[] = [
  { id: 1, total: 99 },
  { id: 2, total: 42 }
]

// Types vanish at runtime, so a typed topic takes a codec that validates what
// came back off the log rather than asserting it.
const ORDER_CODEC = jsonCodec<Order>((value) => {
  if (typeof value !== "object" || value === null) throw new TypeError("order must be an object")
  const { id, total } = value as Record<string, unknown>
  if (typeof id !== "number" || typeof total !== "number") {
    throw new TypeError("order fields are invalid")
  }
  return { id, total }
})

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  phase("write two messages, then read them back")
  const topic = laser.stream(STREAM).topic(TOPIC)
  await topic.ensure(PARTITIONS)

  for (const order of ORDERS) {
    await topic.publish().json(order).send()
  }

  // One typed handle pins the contract: `Order` in on publish, `Order` out on
  // replay, read from offset 0 with the offsets staying caller-owned.
  const replay = await topic.json(ORDER_CODEC).records("log-example")
  for (const { value } of await drain(replay, ORDERS.length)) {
    console.log(`  order #${String(value.id)} total ${String(value.total)}`)
  }
}

/** Collects through the current tail. A poll reads at most one configured batch
 * per partition, so a bounded loop is still required for a larger replay. */
async function drain(
  replay: TypedRecords<Order>,
  expected: number
): Promise<readonly TypedRecord<Order>[]> {
  const records: TypedRecord<Order>[] = []
  const deadline = Date.now() + REPLAY_TIMEOUT_MS
  for (;;) {
    if (Date.now() >= deadline) throw new Error(`only ${String(records.length)} record(s) replayed`)
    const batch = await replay.poll()
    for (const result of batch) {
      if (result.kind === "record") records.push(result.record)
    }
    if (records.length >= expected && batch.length === 0) return records
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

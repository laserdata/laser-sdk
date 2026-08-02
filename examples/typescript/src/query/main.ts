import type { Laser } from "@laserdata/laser-sdk"
import {
  PARTITIONS,
  ensureView,
  indexFor,
  managedGate,
  phase,
  runExample,
  waitForProjection
} from "../common.js"

export const EXAMPLE = "query"
const TOPIC = "orders"
const INDEX = indexFor("orders_v1")
const FIELDS = ["id", "total", "status"]

interface Order {
  readonly id: number
  readonly total: number
  readonly status: string
}

const ORDERS: readonly Order[] = [
  { id: 1, total: 99, status: "paid" },
  { id: 2, total: 42, status: "pending" },
  { id: 3, total: 15, status: "paid" }
]

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const capabilities = await laser.capabilities()
  if (!managedGate(capabilities, "query", EXAMPLE)) return

  phase("keep a queryable view of a topic, then query it")
  await laser.topic(TOPIC).ensure(PARTITIONS)
  // Declare this run's `orders_v1_<token>` view over `orders`. From here the
  // view maintains itself: every record published to the topic lands in the
  // table, and the per-run name means the counts below are this run's alone.
  await ensureView(laser, TOPIC, INDEX, FIELDS)

  for (const order of ORDERS) {
    await laser.topic(TOPIC).publish().json(order).send()
  }
  await waitForProjection(laser, INDEX, ORDERS.length)

  // `whereEq` matches an indexed key, the cheap path a projection's key columns
  // answer directly. `filterEq` and its siblings cover the rest.
  const paid = await laser.query(INDEX).whereEq("status", "paid").limit(10).fetch()

  console.log(`  ${String(paid.rows.length)} of ${String(ORDERS.length)} orders are paid`)
  for (const row of paid.rows) {
    const id = row.headers.get("id") ?? "?"
    const total = row.headers.get("total") ?? "?"
    console.log(`    order #${id} total ${total}`)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

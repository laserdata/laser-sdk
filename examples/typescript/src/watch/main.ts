import type { ChangeRecord, Laser } from "@laserdata/laser-sdk"
import {
  PARTITIONS,
  ensureView,
  indexFor,
  managedGate,
  phase,
  runExample,
  sleep
} from "../common.js"

export const EXAMPLE = "watch"
const TOPIC = "orders"
const INDEX = indexFor("orders_v1")
const FIELDS = ["id", "total", "status"]
const CHANGE_TIMEOUT_MS = 10_000
const POLL_INTERVAL_MS = 200

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const capabilities = await laser.capabilities()
  if (
    !managedGate(capabilities, "query", EXAMPLE) ||
    !managedGate(capabilities, "watch", EXAMPLE)
  ) {
    return
  }

  phase("watch a view, then publish something that advances it")
  await laser.topic(TOPIC).ensure(PARTITIONS)
  // The same view shape the query example declares, under this run's own
  // name, so this entry point runs on its own with no shared state.
  await ensureView(laser, TOPIC, INDEX, FIELDS)

  const feed = await laser.watch().index(INDEX).records()

  await laser.topic(TOPIC).publish().json({ id: 4, total: 20, status: "paid" }).send()

  const deadline = Date.now() + CHANGE_TIMEOUT_MS
  let changes: readonly ChangeRecord[] = []
  while (changes.length === 0) {
    if (Date.now() >= deadline) throw new Error(`no change on \`${INDEX}\` arrived in time`)
    await sleep(POLL_INTERVAL_MS)
    changes = await feed.poll()
  }

  for (const change of changes) {
    const span = `${String(change.fromOffset)}..${String(change.toOffset)}`
    console.log(`  view advanced: ${String(change.rows)} row(s), source offsets ${span}`)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

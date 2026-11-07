import type { Laser } from "@laserdata/laser-sdk"
import { PARTITIONS, ensureView, indexFor, managedGate, phase, runExample } from "../common.js"

export const EXAMPLE = "watch"
const TOPIC = "readings"
const INDEX = indexFor("readings_v1")
const FIELDS = ["host", "cpu", "status"]
const CHANGE_TIMEOUT_MS = 10_000

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

  // Open at the feed's end before publishing, so the reader sees this run's
  // change and none of the history before it.
  const feed = await laser.watch().index(INDEX).fromEnd().records()

  await laser.topic(TOPIC).publish().json({ host: "node-4", cpu: 88, status: "degraded" }).send()

  const changes = await feed.pollWithin(CHANGE_TIMEOUT_MS)
  if (changes.length === 0) throw new Error(`no change on \`${INDEX}\` arrived in time`)
  for (const { change } of changes) {
    const span = `${String(change.fromOffset)}..${String(change.toOffset)}`
    console.log(`  view advanced: ${String(change.rows)} row(s), source offsets ${span}`)
  }
  if (feed.missed > 0n) {
    console.log(`  the feed lost ${String(feed.missed)} change(s) to retention, re-read the view`)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

import { queryResultValue, typedValueDiagnosticText, type Laser } from "@laserdata/laser-sdk"
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
const TOPIC = "readings"
const INDEX = indexFor("readings_v1")
const FIELDS = ["host", "cpu", "status"]

interface Reading {
  readonly host: string
  readonly cpu: number
  readonly status: string
}

const READINGS: readonly Reading[] = [
  { host: "node-1", cpu: 42, status: "ok" },
  { host: "node-2", cpu: 91, status: "degraded" },
  { host: "node-3", cpu: 17, status: "ok" }
]

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const capabilities = await laser.capabilities()
  if (!managedGate(capabilities, "query", EXAMPLE)) return

  phase("keep a queryable view of a topic, then query it")
  await laser.topic(TOPIC).ensure(PARTITIONS)
  // Declare this run's `readings_v1_<token>` view over `readings`. From here the
  // view maintains itself: every record published to the topic lands in the
  // table, and the per-run name means the counts below are this run's alone.
  await ensureView(laser, TOPIC, INDEX, FIELDS)

  for (const reading of READINGS) {
    await laser.topic(TOPIC).publish().json(reading).send()
  }
  await waitForProjection(laser, INDEX, READINGS.length)

  // `whereEq` matches an indexed key, the cheap path a projection's key columns
  // answer directly. `filterEq` and its siblings cover the rest.
  const degraded = await laser.query(INDEX).whereEq("status", "degraded").limit(10).fetch()

  console.log(`  ${String(degraded.rows.length)} of ${String(READINGS.length)} hosts are degraded`)
  for (const row of degraded.rows) {
    const host = queryResultValue(degraded, row, "host")
    const cpu = queryResultValue(degraded, row, "cpu")
    console.log(
      `    host ${host === undefined ? "?" : typedValueDiagnosticText(host)} cpu ${cpu === undefined ? "?" : typedValueDiagnosticText(cpu)}`
    )
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

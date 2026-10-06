import { jsonCodec, type Laser, type TypedRecord, type TypedRecords } from "@laserdata/laser-sdk"
import { phase, runExample } from "../common.js"

export const EXAMPLE = "log"
const STREAM = "fleet"
const TOPIC = "readings"
const PARTITIONS = 2
const REPLAY_TIMEOUT_MS = 10_000

interface Reading {
  readonly host: string
  readonly cpu: number
}

const READINGS: readonly Reading[] = [
  { host: "node-1", cpu: 42 },
  { host: "node-2", cpu: 91 }
]

// Types vanish at runtime, so a typed topic takes a codec that validates what
// came back off the log rather than asserting it.
const READING_CODEC = jsonCodec<Reading>((value) => {
  if (typeof value !== "object" || value === null) throw new TypeError("reading must be an object")
  const { host, cpu } = value as Record<string, unknown>
  if (typeof host !== "string" || typeof cpu !== "number") {
    throw new TypeError("reading fields are invalid")
  }
  return { host, cpu }
})

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  phase("write two messages, then read them back")
  const topic = laser.stream(STREAM).topic(TOPIC)
  await topic.ensure(PARTITIONS)

  for (const reading of READINGS) {
    await topic.publish().json(reading).send()
  }

  // One typed handle pins the contract: `Reading` in on publish, `Reading` out on
  // replay, read from offset 0 with the offsets staying caller-owned.
  const replay = await topic.json(READING_CODEC).records("log-example")
  for (const { value } of await drain(replay, READINGS.length)) {
    console.log(`  reading ${value.host} cpu ${String(value.cpu)}`)
  }
}

/** Collects through the current tail. A poll reads at most one configured batch
 * per partition, so a bounded loop is still required for a larger replay. */
async function drain(
  replay: TypedRecords<Reading>,
  expected: number
): Promise<readonly TypedRecord<Reading>[]> {
  const records: TypedRecord<Reading>[] = []
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

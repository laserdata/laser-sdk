import { runCodecs } from "./codecs.js"
import {
  ConsumerFilter,
  FilterExecutionError,
  FilterExpr,
  HeaderValue,
  type ConsumerGroup,
  type FilterBinding,
  type FilteredReader,
  type Laser,
  type MatchedRecord
} from "@laserdata/laser-sdk"
import { decodeUtf8, managedGate, phase, runExample, runToken, utf8 } from "../common.js"

// A satellite fleet streams the change feed of its mission-ops database: every
// battery reading, orbit maneuver, and ground-station status flip. The anomaly
// desk only wants satellites entering safe mode or leaving the fleet. The
// desk's consumer group owns that filter: the server evaluates it next to the
// data, so the desk receives a handful of records out of hundreds, and
// everything else never leaves the broker.
export const EXAMPLE = "cdc"
const TOPIC = "fleet_changes"
const ALERTS = "fleet_alerts"
const PARTITIONS = 3
const GROUP = "anomaly-desk"
const SATELLITES = 8
const FEED_SIZE = 240
const ROUTINE = 1
const CRITICAL = 2
const READ_TIMEOUT_MS = 15_000

type Mode = "nominal" | "maneuver" | "safe"
type Column = "mode" | "battery_pct" | "status"

interface Satellite {
  readonly id: string
  readonly name: string
  readonly mode: Mode
  readonly orbit: "leo" | "meo"
  readonly battery_pct: number
}

interface GroundStation {
  readonly id: string
  readonly status: "online" | "maintenance"
}

/** A captured row change, tagged by its table. */
type RowChange =
  | {
      readonly table: "satellites"
      readonly op: "u"
      readonly changed: readonly Column[]
      readonly after: Satellite
    }
  | { readonly table: "satellites"; readonly op: "d"; readonly before: { readonly id: string } }
  | {
      readonly table: "ground_stations"
      readonly op: "u"
      readonly changed: readonly Column[]
      readonly after: GroundStation
    }

/** A telemetry event a satellite reports. */
interface TelemetryEvent {
  readonly event: "satellite.telemetry_changed"
  readonly satellite_id: string
  readonly fields: { readonly mode?: Mode; readonly battery_pct?: number }
}

/** One record of the change feed. */
type FleetChange = RowChange | TelemetryEvent

interface Feed {
  readonly records: readonly FleetChange[]
  /** Two safe-mode transitions, one telemetry report of safe mode, one decommission. */
  readonly strictMatches: number
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const capabilities = await laser.capabilities()
  if (!managedGate(capabilities, "filterCatalog", EXAMPLE, "consumer group filters")) return
  const stream = laser.defaultStream ?? ""
  const topic = laser.stream(stream).topic(TOPIC)

  phase("publish a busy fleet change feed, keyed by satellite")
  const feed = fleetFeed()
  await topic.ensure(PARTITIONS)
  let publishedBytes = 0
  const byKey = new Map<string, FleetChange[]>()
  for (const change of feed.records) {
    publishedBytes += utf8(JSON.stringify(change)).byteLength
    const key = keyOf(change)
    byKey.set(key, [...(byKey.get(key) ?? []), change])
  }
  for (const [key, changes] of [...byKey].sort(([left], [right]) => left.localeCompare(right))) {
    const started = performance.now()
    const batch = topic.publishBatch().partitionKey(utf8(key))
    for (const change of changes) batch.addJson(change)
    await batch.send()
    console.log(
      `  ${key}: ${String(changes.length)} records in ${String(Math.round(performance.now() - started))} ms`
    )
  }
  console.log(
    `  ${String(feed.records.length)} records, ${String(publishedBytes)} bytes: battery readings, maneuvers, station flips, and ${String(feed.strictMatches)} safe-mode or decommission events`
  )

  phase("create the anomaly desk group with its filter")
  const desk = topic.consumerGroup(`${GROUP}-${runToken()}`)
  const created = await desk.create({ filter: safeModeFilter() })
  const binding = created.filter
  if (binding === undefined) throw new Error("the group was created unbound")
  console.log(
    `  group ${created.name} (${String(created.id)}) runs revision ${String(binding.revision)} of its own filter from now on`
  )

  phase("consume as the group: the application names the group, the server runs its filter")
  const consumer = await desk.consumer({ startFrom: { kind: "first" }, autoCommit: false })
  let deliveredBytes = 0
  try {
    for (let handled = 0; handled < feed.strictMatches; handled += 1) {
      const message = await consumer.nextWithin(READ_TIMEOUT_MS)
      if (message === null) throw new Error("the next matching record did not arrive")
      const change = JSON.parse(decodeUtf8(message.payload)) as FleetChange
      console.log(
        `  partition ${String(message.partitionId)} offset ${message.offset.toString()}: ${describe(change)}`
      )
      deliveredBytes += message.payload.byteLength
      await consumer.commit(message)
    }
  } finally {
    await consumer.shutdown()
  }
  console.log(
    `  delivered ${String(feed.strictMatches)} of ${String(feed.records.length)} records, ${String(deliveredBytes)} of ${String(publishedBytes)} payload bytes: ${((100 * (publishedBytes - deliveredBytes)) / publishedBytes).toFixed(1)}% stayed on the broker`
  )

  phase("page the matches again with the group reader and its own scan budget")
  const pager = await desk
    .reader()
    .start({ kind: "first" })
    .count(10)
    .maxExamined(100)
    .localGuard(true)
    .build()
  let paged: readonly MatchedRecord[]
  try {
    paged = await readMatches(pager, feed.strictMatches)
  } finally {
    await pager.close()
  }
  console.log(
    `  the reader handed out ${String(paged.length)} matches in pages, each acknowledged after handling`
  )

  phase("test the group's filter against a battery update of a satellite already in safe mode")
  const stillSafe = satelliteUpdate(2, "safe", 58, "battery_pct")
  const tested = await desk.filter().test(JSON.stringify(stillSafe))
  console.log(`  strict, transitions only: ${tested.explanation.verdict}`)

  phase("preview every partition, nothing is stored")
  for (let partitionId = 0; partitionId < PARTITIONS; partitionId += 1) {
    const preview = await desk.filter().preview(partitionId, { maxRecords: 10 })
    console.log(
      `  partition ${String(partitionId)}: examined ${String(preview.examined)}, matched ${String(preview.matched)}, stopped at ${preview.stop}`
    )
  }

  phase("route binary alerts on a header, their payload is never decoded")
  await routeAlerts(laser, stream)
  await runCodecs(laser, stream)

  await manageRevisions(laser, stream, desk, binding, feed.strictMatches)
}

// Binary alert frames carry their priority as a header. A pager group with a
// headers-only filter selects the critical ones without decoding a payload, so
// the alert topic can hold any format.
async function routeAlerts(laser: Laser, stream: string): Promise<void> {
  const alerts = laser.stream(stream).topic(ALERTS)
  await alerts.ensure(1)
  const producer = alerts.producer()
  for (const [priority, satelliteId] of [
    [ROUTINE, "sat-001"],
    [CRITICAL, "sat-003"],
    [ROUTINE, "sat-004"],
    [CRITICAL, "sat-007"]
  ] as const) {
    await producer.send(Uint8Array.of(0x0a, 0x07, ...utf8(satelliteId)), {
      key: utf8(satelliteId),
      headers: { priority: HeaderValue.uint8(priority) }
    })
  }
  const pagerGroup = alerts.consumerGroup(`${GROUP}-pager-${runToken()}`)
  await pagerGroup.create({
    filter: ConsumerFilter.headersOnly(FilterExpr.header("priority", "eq", CRITICAL))
  })
  const pager = await pagerGroup.reader().start({ kind: "first" }).build()
  try {
    for (let handled = 0; handled < 2; handled += 1) {
      const record = await pager.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
      console.log(
        `  critical alert at offset ${record.offset.toString()}: ${String(record.payload.byteLength)} opaque bytes`
      )
      await pager.ack(record)
    }
  } finally {
    await pager.close()
    await pagerGroup.filter().delete()
  }
}

// Draft a stricter revision on the desk's own filter, run the variant in its
// own group, pause and resume it, then release and delete both policies.
async function manageRevisions(
  laser: Laser,
  stream: string,
  desk: ConsumerGroup,
  binding: FilterBinding,
  expected: number
): Promise<void> {
  phase("draft a stricter revision: readers keep running the active one")
  const draft = await desk
    .filter()
    .revise(binding.revision, ConsumerFilter.json(safeModeTransition()))
  const revisions = await desk.filter().revisions({ page: 0, pageSize: 10 })
  console.log(
    `  revision ${String(draft.revision)} drafted, the group lists ${String(revisions.total)} revisions and still runs revision ${String(binding.revision)}`
  )

  phase("A/B: the transitions-only variant runs in its own group")
  const variantName = `${GROUP}-transitions-${runToken()}`
  const variant = laser.stream(stream).topic(TOPIC).consumerGroup(variantName)
  const variantBinding = (
    await variant.create({ filter: ConsumerFilter.json(safeModeTransition()) })
  ).filter
  if (variantBinding === undefined) throw new Error("the variant was created unbound")
  const reader = await variant.reader().count(1).localGuard(true).start({ kind: "first" }).build()
  try {
    const first = await reader.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
    const change = JSON.parse(decodeUtf8(first.payload)) as FleetChange
    console.log(`  ${variantName}: ${describe(change)}`)

    phase("pause the variant: new reads stop, in-flight work still acknowledges")
    await variant.filter().setRevisionEnabled(variantBinding.revision, false)
    await reader.ack(first)
    try {
      await reader.tryNextPage()
      throw new Error("a disabled revision kept reading")
    } catch (error) {
      if (!(error instanceof FilterExecutionError) || error.reason !== "revision_disabled")
        throw error
      console.log("  paused: the server refuses new reads with revision_disabled")
    }
    await variant.filter().setRevisionEnabled(variantBinding.revision, true)
    await readMatches(reader, 1)
    console.log(
      `  resumed: the desk handled ${String(expected)} broad events, the variant 2 transitions`
    )
  } finally {
    await reader.close()
  }

  phase("a group that runs a policy cannot be switched to another one")
  try {
    await desk.filter().configure(ConsumerFilter.json(safeModeTransition()))
    throw new Error("a running policy was replaced")
  } catch (error) {
    if (!(error instanceof FilterExecutionError) || error.reason !== "conflict") throw error
    console.log("  refused with conflict: create a new group for another policy")
  }

  phase("release both policies")
  const released = await desk.filter().release()
  await variant.filter().release()
  console.log(
    `  ${released.group.group} is unbound again and receives every record, its filter stays saved as revision ${String(released.revision)}`
  )

  phase("delete both filters: nothing of them stays in the catalog")
  await desk.filter().delete()
  await variant.filter().delete()
  console.log("  deleted with every revision, the groups keep reading everything")
}

// Read and acknowledge until `expected` safe-mode or decommission events arrived, decoding
// each one into the typed change it is.
async function readMatches(
  reader: FilteredReader,
  expected: number
): Promise<readonly MatchedRecord[]> {
  const delivered: MatchedRecord[] = []
  while (delivered.length < expected) {
    const record = await reader.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
    const change = record.json() as FleetChange
    console.log(
      `  partition ${String(record.partitionId)} offset ${record.offset.toString()}: ${describe(change)}`
    )
    await reader.ack(record)
    delivered.push(record)
  }
  return delivered
}

// What the desk sees, told from the typed record.
function describe(change: FleetChange): string {
  if ("event" in change) return `${change.satellite_id} reported ${change.fields.mode ?? "no"} mode`
  if (change.op === "d") return `${change.before.id} left the fleet`
  if (change.table === "satellites") {
    return `${change.after.name} entered ${change.after.mode} mode at ${String(change.after.battery_pct)}% battery`
  }
  return `${change.after.id} is ${change.after.status}`
}

// The partition key: every change of one satellite or station stays in order
// on one partition.
function keyOf(change: FleetChange): string {
  if ("event" in change) return change.satellite_id
  return change.op === "d" ? change.before.id : change.after.id
}

// Strict: a satellite update that marks its mode as changed to safe, a
// telemetry report of safe mode, or a satellite leaving the fleet.
function safeModeFilter(): ConsumerFilter {
  const satellites = (): FilterExpr => FilterExpr.pred("table", "eq", "satellites")
  return ConsumerFilter.json(
    FilterExpr.any([
      safeModeTransition(),
      FilterExpr.all([satellites(), FilterExpr.pred("op", "eq", "d")]),
      FilterExpr.all([
        FilterExpr.pred("event", "eq", "satellite.telemetry_changed"),
        FilterExpr.pred("fields.mode", "eq", "safe")
      ])
    ])
  )
}

// A transition must change the mode and set its new value to safe.
function safeModeTransition(): FilterExpr {
  return FilterExpr.all([
    FilterExpr.pred("table", "eq", "satellites"),
    FilterExpr.pred("op", "eq", "u"),
    FilterExpr.pred("changed", "contains", "mode"),
    FilterExpr.pred("after.mode", "eq", "safe")
  ])
}

// A deterministic feed: mostly battery telemetry, battery updates, orbit
// maneuvers, and ground-station status flips, with the four events the anomaly
// desk cares about spread through it.
function fleetFeed(): Feed {
  const modes: Mode[] = Array.from({ length: SATELLITES }, () => "nominal")
  const records: FleetChange[] = []
  let strictMatches = 0
  for (let tick = 0; tick < FEED_SIZE; tick += 1) {
    const index = tick % SATELLITES
    const battery = 90 - ((tick * 7) % 40)
    const mode = modes[index] ?? "nominal"
    if (tick === FEED_SIZE / 4 || tick === (3 * FEED_SIZE) / 4) {
      const entering = tick === FEED_SIZE / 4 ? 2 : 6
      modes[entering] = "safe"
      strictMatches += 1
      records.push(satelliteUpdate(entering, "safe", battery, "mode"))
    } else if (tick === FEED_SIZE / 2) {
      strictMatches += 1
      records.push(telemetry(4, { mode: "safe" }))
    } else if (tick === FEED_SIZE - 1) {
      strictMatches += 1
      records.push({ table: "satellites", op: "d", before: { id: satellite(7, "nominal", 0).id } })
    } else if (tick % 10 <= 4) {
      records.push(telemetry(index, { battery_pct: battery }))
    } else if (tick % 10 <= 7 || mode === "safe") {
      records.push(satelliteUpdate(index, mode, battery, "battery_pct"))
    } else if (tick % 10 === 8) {
      records.push({
        table: "ground_stations",
        op: "u",
        changed: ["status"],
        after: {
          id: ["svalbard", "kiruna", "punta-arenas"][tick % 3] ?? "svalbard",
          status: tick % 20 === 8 ? "maintenance" : "online"
        }
      })
    } else {
      records.push(satelliteUpdate(index, "maneuver", battery, "mode"))
    }
  }
  return { records, strictMatches }
}

function satelliteUpdate(index: number, mode: Mode, battery: number, changed: Column): RowChange {
  return {
    table: "satellites",
    op: "u",
    changed: [changed],
    after: satellite(index, mode, battery)
  }
}

function telemetry(index: number, fields: TelemetryEvent["fields"]): TelemetryEvent {
  return {
    event: "satellite.telemetry_changed",
    satellite_id: satellite(index, "nominal", 0).id,
    fields
  }
}

function satellite(index: number, mode: Mode, battery: number): Satellite {
  return {
    id: `sat-${String(index + 1).padStart(3, "0")}`,
    name: `Kestrel-${String(index + 1)}`,
    mode,
    orbit: index % 2 === 0 ? "leo" : "meo",
    battery_pct: battery
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

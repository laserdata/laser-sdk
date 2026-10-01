import { runCodecs } from "./codecs.js"
import {
  ConsumerFilter,
  FilterExecutionError,
  FilterExpr,
  HeaderValue,
  type FilterBinding,
  type FilteredReader,
  type Laser,
  type MatchedRecord
} from "@laserdata/laser-sdk"
import { decodeUtf8, managedGate, phase, runExample, runToken, utf8 } from "../common.js"

// A satellite fleet streams the change feed of its mission-ops database: every
// battery reading, orbit maneuver, and ground-station status flip. The anomaly
// desk only wants satellites entering safe mode or leaving the fleet. The
// server evaluates the filter next to the data, so the desk receives a handful
// of records out of hundreds, and everything else never leaves the broker.
export const EXAMPLE = "cdc"
const TOPIC = "fleet_changes"
const ALERTS = "fleet_alerts"
const PARTITIONS = 3
const GROUP = "anomaly-desk"
const BACKFILL = "safe-mode-backfill"
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
  if (!managedGate(capabilities, "filters", EXAMPLE, "consumer filters")) return
  const stream = laser.defaultStream ?? ""
  const topic = laser.stream(stream).topic(TOPIC)

  phase("publish a busy fleet change feed, keyed by satellite")
  const feed = fleetFeed()
  await topic.ensure(PARTITIONS)
  let publishedBytes = 0
  for (const change of feed.records) {
    publishedBytes += utf8(JSON.stringify(change)).byteLength
    await topic
      .publish()
      .partitionKey(utf8(keyOf(change)))
      .json(change)
      .send()
  }
  console.log(
    `  ${String(feed.records.length)} records, ${String(publishedBytes)} bytes: battery readings, maneuvers, station flips, and ${String(feed.strictMatches)} safe-mode or decommission events`
  )

  phase("read only the safe-mode or decommission events, no catalog needed")
  const backfill = await laser
    .filters()
    .reader(stream, TOPIC)
    .consumer(BACKFILL)
    .inline(safeModeFilter())
    .start({ kind: "first" })
    .build()
  let delivered: readonly MatchedRecord[]
  try {
    delivered = await readMatches(backfill, feed.strictMatches)
  } finally {
    await backfill.close()
  }
  const deliveredBytes = delivered.reduce((sum, record) => sum + record.payload.byteLength, 0)
  console.log(
    `  delivered ${String(delivered.length)} of ${String(feed.records.length)} records, ${String(deliveredBytes)} of ${String(publishedBytes)} payload bytes: ${((100 * (publishedBytes - deliveredBytes)) / publishedBytes).toFixed(1)}% stayed on the broker`
  )

  phase("test both filters against a battery update of a satellite already in safe mode")
  const stillSafe = satelliteUpdate(2, "safe", 58, "battery_pct")
  for (const [name, filter] of [
    ["strict, transitions only", safeModeFilter()],
    ["values only, current state", safeModeValuesFilter()]
  ] as const) {
    const tested = await laser.filters().test({ kind: "inline", filter }, JSON.stringify(stillSafe))
    console.log(`  ${name}: ${tested.explanation.verdict}`)
  }

  phase("preview every partition, nothing is stored")
  for (let partitionId = 0; partitionId < PARTITIONS; partitionId += 1) {
    const preview = await laser
      .filters()
      .preview(
        stream,
        TOPIC,
        partitionId,
        { kind: "inline", filter: safeModeFilter() },
        { maxRecords: 10 }
      )
    console.log(
      `  partition ${String(partitionId)}: examined ${String(preview.examined)}, matched ${String(preview.matched)}, stopped at ${preview.stop}`
    )
  }

  phase("route binary alerts on a header, their payload is never decoded")
  await routeAlerts(laser, stream)
  await runCodecs(laser, stream, capabilities.filters.catalog)

  if (!capabilities.filters.catalog) {
    console.log("  the saved-filter catalog needs a managed plane, skipping group bindings")
    return
  }
  await manageGroup(laser, stream, feed.strictMatches)
}

// Binary alert frames carry their priority as a header. A headers-only filter
// selects the critical ones without decoding a payload, so the alert topic can
// hold any format.
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
  const pager = await laser
    .filters()
    .reader(stream, ALERTS)
    .consumer(`${BACKFILL}-pager`)
    .inline(ConsumerFilter.headersOnly(FilterExpr.header("priority", "eq", CRITICAL)))
    .start({ kind: "first" })
    .build()
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
  }
}

// Save both filters, bind the anomaly desk to the strict one, consume as the
// group, then release everything this run created, also when a step fails.
async function manageGroup(laser: Laser, stream: string, expected: number): Promise<void> {
  phase("save both filters in the catalog")
  const filters = laser.filters()
  const saved: number[] = []
  const bindings: FilterBinding[] = []
  let failure: unknown
  try {
    const strict = await filters.register(`sats-safe-mode-${runToken()}`, safeModeFilter(), {
      description: "Satellites entering safe mode, reporting it, or leaving the fleet"
    })
    saved.push(strict.filterId)
    const values = await filters.register(
      `sats-safe-mode-values-${runToken()}`,
      safeModeValuesFilter(),
      { description: "Every update of a satellite whose current mode is safe" }
    )
    saved.push(values.filterId)
    console.log(
      `  strict is filter ${String(strict.filterId)} revision ${String(strict.revision)}, values only is filter ${String(values.filterId)} revision ${String(values.revision)}`
    )

    phase("bind the anomaly desk group to the strict filter")
    const binding = await filters.createConsumerGroup(
      { stream, topic: TOPIC, group: GROUP },
      strict.filterId,
      strict.revision
    )
    bindings.push(binding)
    console.log(`  ${GROUP} runs revision ${String(binding.revision)} from now on`)

    phase("consume as the group and acknowledge")
    const desk = await filters
      .reader(stream, TOPIC)
      .groupId(binding.identity.groupId)
      .localGuard(true)
      .start({ kind: "first" })
      .build()
    try {
      const handled = await readMatches(desk, expected)
      console.log(`  the desk handled ${String(handled.length)} safe-mode or decommission events`)
    } finally {
      await desk.close()
    }

    phase("A/B: a second revision runs in its own group")
    const second = await filters.revise(
      strict.filterId,
      strict.revision,
      ConsumerFilter.json(safeModeTransition())
    )
    const variantBinding = await filters.createConsumerGroup(
      { stream, topic: TOPIC, group: `${GROUP}-transitions` },
      second.filterId,
      second.revision
    )
    bindings.push(variantBinding)
    const variant = await filters
      .reader(stream, TOPIC)
      .groupId(variantBinding.identity.groupId)
      .count(1)
      .localGuard(true)
      .start({ kind: "first" })
      .build()
    try {
      const first = await variant.nextRecord({ timeoutMs: READ_TIMEOUT_MS })
      const change = JSON.parse(decodeUtf8(first.payload)) as FleetChange
      console.log(`  revision ${String(second.revision)}: ${describe(change)}`)
      await filters.setRevisionEnabled(second.filterId, second.revision, false)
      await variant.ack(first)
      try {
        await variant.tryNextPage()
        throw new Error("a disabled revision kept reading")
      } catch (error) {
        if (!(error instanceof FilterExecutionError) || error.reason !== "revision_disabled")
          throw error
        console.log("  paused: new reads stop, in-flight work can still be acknowledged")
      }
      await filters.setRevisionEnabled(second.filterId, second.revision, true)
      await readMatches(variant, 1)
      console.log(
        `  A/B groups handled ${String(expected)} broad events and 2 transitions independently`
      )
    } finally {
      await variant.close()
    }

    phase("a bound filter cannot be deleted")
    try {
      await filters.delete(strict.filterId)
      throw new Error("a bound filter was deleted")
    } catch (error) {
      if (!(error instanceof FilterExecutionError) || error.reason !== "conflict") throw error
      console.log(`  refused with conflict while ${GROUP} is bound`)
    }
  } catch (error) {
    failure = error
  } finally {
    phase("unbind, archive, delete")
    const release = async (action: () => Promise<unknown>): Promise<void> => {
      try {
        await action()
      } catch (error) {
        failure ??= error
      }
    }
    for (const binding of bindings) await release(() => filters.unbindBinding(binding))
    for (const filterId of saved) {
      await release(() => filters.archive(filterId))
      await release(() => filters.delete(filterId))
    }
    if (failure !== undefined) throw failure
    console.log("  both filters are gone, their names are never reused")
  }
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

// Values only: any update of a satellite whose current mode is safe.
function safeModeValuesFilter(): ConsumerFilter {
  return ConsumerFilter.json(
    FilterExpr.all([
      FilterExpr.pred("table", "eq", "satellites"),
      FilterExpr.pred("op", "eq", "u"),
      FilterExpr.pred("after.mode", "eq", "safe")
    ])
  )
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

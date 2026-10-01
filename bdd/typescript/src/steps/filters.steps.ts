import { randomUUID } from "node:crypto"
import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { Given, Then, When } from "@cucumber/cucumber"
import {
  CompiledFilter,
  ConsumerFilter,
  FilterExpr,
  UnsupportedError,
  consumerFilterDigest,
  consumerFilterJson,
  decodeConsumerFilterJson,
  type FilterHeader
} from "@laserdata/laser-sdk"
import type { LaserWorld } from "../world.js"

const TOPIC = "fleet_changes"
const READ_TIMEOUT_MS = 15_000
const UTF8 = new TextEncoder()

interface RecordFixtures {
  readonly records: Readonly<Record<string, { readonly text?: string; readonly hex?: string }>>
  readonly feed: readonly string[]
}

// The record payloads and the feed order, shared with the Rust and Python
// runners so every language evaluates byte-identical records.
const FIXTURES = JSON.parse(
  readFileSync(new URL("../../../scenarios/filter_records.json", import.meta.url), "utf8")
) as RecordFixtures
const RECORDS: Readonly<Record<string, Uint8Array>> = Object.fromEntries(
  Object.entries(FIXTURES.records).map(([name, payload]) => [
    name,
    payload.text !== undefined ? UTF8.encode(payload.text) : Buffer.from(payload.hex ?? "", "hex")
  ])
)
const FEED = FIXTURES.feed

function safeMode(): ConsumerFilter {
  const satellites = (): FilterExpr => FilterExpr.pred("table", "eq", "satellites")
  return ConsumerFilter.json(
    FilterExpr.any([
      FilterExpr.all([
        satellites(),
        FilterExpr.pred("op", "eq", "u"),
        FilterExpr.pred("changed", "contains", "mode"),
        FilterExpr.pred("after.mode", "eq", "safe")
      ]),
      FilterExpr.all([satellites(), FilterExpr.pred("op", "eq", "d")]),
      FilterExpr.all([
        FilterExpr.pred("event", "eq", "satellite.telemetry_changed"),
        FilterExpr.pred("fields.mode", "eq", "safe")
      ])
    ])
  )
}

const FILTERS: Readonly<Record<string, () => ConsumerFilter>> = {
  "mode present": () => ConsumerFilter.json(FilterExpr.present("fields.mode")),
  "safe mode": safeMode,
  "safe mode values": () =>
    ConsumerFilter.json(
      FilterExpr.all([
        FilterExpr.pred("table", "eq", "satellites"),
        FilterExpr.pred("op", "eq", "u"),
        FilterExpr.pred("after.mode", "eq", "safe")
      ])
    ),
  "mode is not safe": () =>
    ConsumerFilter.json(FilterExpr.negate(FilterExpr.pred("after.mode", "eq", "safe"))),
  "catalog number 9007199254740993": () =>
    ConsumerFilter.json(FilterExpr.pred("after.norad_id", "eq", 9007199254740993n)),
  "contact after noon UTC": () =>
    ConsumerFilter.json(
      FilterExpr.predAs("contact_at", "gt", "2026-09-21T12:00:00Z", {
        kind: "timestamp",
        format: "rfc3339"
      })
    ),
  "critical priority": () =>
    ConsumerFilter.headersOnly(FilterExpr.header("priority", "eq", "critical"))
}

function named<T>(catalogue: Readonly<Record<string, T>>, name: string): T {
  const value = catalogue[name]
  if (value === undefined) throw new Error(`no fixture named \`${name}\``)
  return value
}

function evaluate(world: LaserWorld, payload: Uint8Array, headers: readonly FilterHeader[]): void {
  if (world.filter === undefined) throw new Error("scenario has no filter")
  world.verdict = CompiledFilter.compile(world.filter).evaluate({ payload, headers })
}

Given(/^the "([^"]+)" filter$/, function (this: LaserWorld, name: string) {
  this.filter = named(FILTERS, name)()
})

When(/^it evaluates the "([^"]+)" record$/, function (this: LaserWorld, name: string) {
  evaluate(this, named(RECORDS, name), [])
})

When(
  /^it evaluates the "([^"]+)" record with header "([^"]+)" set to "([^"]+)"$/,
  function (this: LaserWorld, name: string, key: string, value: string) {
    evaluate(this, named(RECORDS, name), [{ key, value: { kind: "string", value } }])
  }
)

Then(/^the record is (selected|rejected|a fault)$/, function (this: LaserWorld, verdict: string) {
  assert.equal(this.verdict, verdict === "a fault" ? "fault" : verdict)
})

Then("its digest survives a round trip through its wire form", function (this: LaserWorld) {
  if (this.filter === undefined) throw new Error("scenario has no filter")
  const back = decodeConsumerFilterJson(consumerFilterJson(this.filter))
  assert.deepEqual(consumerFilterDigest(back), consumerFilterDigest(this.filter))
})

Then("a different fault policy gives a different digest", function (this: LaserWorld) {
  if (this.filter === undefined) throw new Error("scenario has no filter")
  const dropping = ConsumerFilter.json(this.filter.expr, "drop")
  assert.notDeepEqual(consumerFilterDigest(dropping), consumerFilterDigest(this.filter))
})

Given("a fresh fleet change feed", async function (this: LaserWorld) {
  await this.connect()
  const topic = this.requireLaser().topic(TOPIC)
  await topic.ensure(1)
  for (const name of FEED) await topic.send(named(RECORDS, name), { partition: 0 })
})

When(
  /^the anomaly desk reads the feed with the "([^"]+)" filter$/,
  async function (this: LaserWorld, name: string) {
    const reader = await this.requireLaser()
      .filters()
      .reader(this.stream ?? "", TOPIC)
      .consumer("anomaly-desk")
      .inline(named(FILTERS, name)())
      .start({ kind: "first" })
      .build()
    const page = await reader.nextPage({ timeoutMs: READ_TIMEOUT_MS })
    await reader.ackPage(page)
    await reader.close()
    this.filtered = page.records.map((record) => record.payload)
  }
)

Then(
  /^it receives the "([^"]+)", "([^"]+)", and "([^"]+)" records$/,
  function (this: LaserWorld, first: string, second: string, third: string) {
    assert.deepEqual(
      this.filtered.map((payload) => Buffer.from(payload).toString("utf8")),
      [first, second, third].map((name) => Buffer.from(named(RECORDS, name)).toString("utf8"))
    )
  }
)

When("the anomaly desk lists its saved filters", async function (this: LaserWorld) {
  try {
    await this.requireLaser().filters().list()
    this.error = undefined
  } catch (error) {
    if (!(error instanceof UnsupportedError)) throw error
    this.error = error
  }
})

Then("the catalog is refused as unsupported", function (this: LaserWorld) {
  assert.ok(this.error instanceof UnsupportedError)
})

When("the anomaly desk manages a saved policy by numeric group id", async function (this: LaserWorld) {
  const filters = this.requireLaser().filters()
  const stream = this.stream ?? ""
  const operationId = BigInt(`0x${randomUUID().replaceAll("-", "")}`)
  const mutation = { kind: "register" as const, name: `bdd-${stream}`, description: "", filter: safeMode() }
  const first = await filters.applyAs(operationId, mutation)
  assert.deepEqual(await filters.applyAs(operationId, mutation), first)
  assert.equal(first.kind, "registered")
  if (first.kind !== "registered") throw new Error("registration result expected")
  const saved = first.revision
  const binding = await filters.createConsumerGroup({ stream, topic: TOPIC, group: "managed-desk" }, saved.filterId, saved.revision)
  await using reader = await filters.reader(stream, TOPIC).groupId(binding.identity.groupId).build()
  const page = await reader.nextPage({ timeoutMs: READ_TIMEOUT_MS })
  await filters.setRevisionEnabled(saved.filterId, saved.revision, false)
  await reader.ackPage(page)
  await assert.rejects(reader.tryNextPage(), /revision_disabled/)
  await filters.setRevisionEnabled(saved.filterId, saved.revision, true)
  this.filtered = page.records.map((record) => record.payload)
  await reader.close()
  await filters.unbindBinding(binding)
  await filters.archive(saved.filterId)
  await filters.delete(saved.filterId)
})

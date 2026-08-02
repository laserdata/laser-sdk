import assert from "node:assert/strict"
import { Given, Then, When } from "@cucumber/cucumber"
import type { LaserWorld } from "../world.js"

Given(
  /^a query index "([^"]+)" seeded with sample api-call rows$/,
  function (this: LaserWorld, index: string) {
    this.query.seed(index, [
      { status: "200", latency_ms: "10" },
      { status: "200", latency_ms: "550" },
      { status: "500", latency_ms: "900" },
      { status: "200", latency_ms: "30" }
    ])
  }
)

When(
  /^I query "([^"]+)" for latency_ms greater than (\d+)$/,
  function (this: LaserWorld, index: string, bound: string) {
    this.queryResult = this.query.execute(index, { filter: ["latency_ms", Number(bound)] })
  }
)

When(
  /^I query "([^"]+)" ordered by latency_ms descending$/,
  function (this: LaserWorld, index: string) {
    this.queryResult = this.query.execute(index, { orderDesc: "latency_ms" })
  }
)

When(/^I query "([^"]+)" with limit (\d+)$/, function (this: LaserWorld, index: string, limit: string) {
  this.queryResult = this.query.execute(index, { limit: Number(limit) })
})

When(/^I count "([^"]+)" grouped by status$/, function (this: LaserWorld, index: string) {
  this.queryResult = this.query.execute(index, { groupBy: "status" })
})

Then(/^the query returns (\d+) rows$/, function (this: LaserWorld, count: string) {
  assert.equal(result(this).rows.length, Number(count))
})

Then("every returned row has latency_ms greater than 500", function (this: LaserWorld) {
  assert.ok(result(this).rows.every((row) => Number(row["latency_ms"]) > 500))
})

Then(
  /^the returned latency_ms values are "([^"]+)" in order$/,
  function (this: LaserWorld, values: string) {
    assert.deepEqual(result(this).rows.map((row) => row["latency_ms"]), values.split(", "))
  }
)

Then(/^the page total is (\d+)$/, function (this: LaserWorld, total: string) {
  assert.equal(result(this).total, Number(total))
})

Then(/^group "([^"]+)" has count (\d+)$/, function (this: LaserWorld, group: string, count: string) {
  const row = result(this).rows.find((candidate) => candidate["status"] === group)
  assert.equal(row?.["count"], String(count))
})

function result(world: LaserWorld) {
  if (world.queryResult === undefined) throw new Error("scenario has no query result")
  return world.queryResult
}

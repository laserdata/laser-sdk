import assert from "node:assert/strict"
import { Given, Then, When } from "@cucumber/cucumber"
import { DataStackModel, type LaserWorld } from "../world.js"

Given("a fresh data stack contract model", function (this: LaserWorld) {
  this.dataStack = new DataStackModel()
})

When(
  /^I register destination "([^"]+)" at global revision (\d+)$/,
  function (this: LaserWorld, name: string, revision: string) {
    this.dataStack.register(name, Number(revision))
  }
)

When(
  /^I enable destination "([^"]+)" at global revision (\d+) and definition revision (\d+)$/,
  function (this: LaserWorld, name: string, globalRevision: string, definitionRevision: string) {
    this.dataStack.enable(name, Number(globalRevision), Number(definitionRevision))
  }
)

When(
  /^I record a retention gap from required offset (\d+) to retained offset (\d+)$/,
  function (this: LaserWorld, _required: string, _retained: string) {
    this.dataStack.recordGap("orders-lakehouse")
  }
)

When(
  /^I accept the retention gap at next offset (\d+) and checkpoint revision (\d+)$/,
  function (this: LaserWorld, nextOffset: string, checkpointRevision: string) {
    this.dataStack.acceptGap(
      "orders-lakehouse",
      Number(nextOffset),
      Number(checkpointRevision)
    )
  }
)

Then("the operation is accepted with a stable identity", function (this: LaserWorld) {
  assert.ok(this.dataStack.operation?.id)
})

Then(
  /^destination "([^"]+)" is disabled at definition revision (\d+)$/,
  function (this: LaserWorld, name: string, revision: string) {
    const destination = this.dataStack.destination(name)
    assert.equal(destination?.effectiveState, "disabled")
    assert.equal(destination?.definitionRevision, Number(revision))
  }
)

Then(
  /^destination "([^"]+)" is running at definition revision (\d+)$/,
  function (this: LaserWorld, name: string, revision: string) {
    const destination = this.dataStack.destination(name)
    assert.equal(destination?.effectiveState, "running")
    assert.equal(destination?.definitionRevision, Number(revision))
  }
)

Then(
  /^the destination mutation conflicts with global revision (\d+)$/,
  function (this: LaserWorld, revision: string) {
    assert.deepEqual(this.dataStack.error, {
      kind: "conflict",
      observedRevision: Number(revision)
    })
  }
)

Then(
  /^destination "([^"]+)" is blocked by the retention gap at checkpoint revision (\d+)$/,
  function (this: LaserWorld, name: string, revision: string) {
    const destination = this.dataStack.destination(name)
    assert.equal(destination?.effectiveState, "blocked")
    assert.equal(destination?.checkpointRevision, Number(revision))
  }
)

Then(
  /^destination "([^"]+)" is running at next offset (\d+)$/,
  function (this: LaserWorld, name: string, nextOffset: string) {
    const destination = this.dataStack.destination(name)
    assert.equal(destination?.effectiveState, "running")
    assert.equal(destination?.nextOffset, Number(nextOffset))
  }
)

Given(/^a query execution with rows "([^"]+)"$/, function (this: LaserWorld, rows: string) {
  this.dataStack.seedQuery(rows.split(", "))
})

When(/^I read a query page with limit (\d+)$/, function (this: LaserWorld, limit: string) {
  this.dataStack.readPage(Number(limit))
})

Then(
  /^the query page contains "([^"]+)" and a continuation cursor$/,
  function (this: LaserWorld, rows: string) {
    assert.deepEqual(this.dataStack.queryPage?.rows, rows.split(", "))
    assert.notEqual(this.dataStack.queryPage?.cursor, undefined)
  }
)

When("I cancel the query execution", function (this: LaserWorld) {
  this.dataStack.cancelQuery()
})

Then("the query execution status is cancelled", function (this: LaserWorld) {
  assert.equal(this.dataStack.queryIsCancelled(), true)
})

Then("the continuation page is rejected as cancelled", function (this: LaserWorld) {
  this.dataStack.readPage(2)
  assert.deepEqual(this.dataStack.error, { kind: "cancelled" })
})

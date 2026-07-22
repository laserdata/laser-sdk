import assert from "node:assert/strict"
import { Given, Then, When } from "@cucumber/cucumber"
import { KvEngine, type LaserWorld } from "../world.js"

const NOW = 1_000

Given("an empty KV store", function (this: LaserWorld) {
  this.kv = new KvEngine()
})

Given(/^key "([^"]+)" holds "([^"]+)"$/, function (this: LaserWorld, key: string, value: string) {
  this.kv.set(key, value, NOW)
})

Given(
  /^key "([^"]+)" holds "([^"]+)" expiring at (\d+)$/,
  function (this: LaserWorld, key: string, value: string, expiry: string) {
    this.kv.set(key, value, NOW, Number(expiry))
  }
)

When(
  /^I create "([^"]+)" with "([^"]+)" if absent$/,
  function (this: LaserWorld, key: string, value: string) {
    this.cas = this.kv.cas(key, value, "absent", NOW)
  }
)

When(
  /^I create "([^"]+)" with "([^"]+)" if absent at (\d+)$/,
  function (this: LaserWorld, key: string, value: string, now: string) {
    this.cas = this.kv.cas(key, value, "absent", Number(now))
  }
)

When(
  /^I swap "([^"]+)" to "([^"]+)" expecting version (\d+)$/,
  function (this: LaserWorld, key: string, value: string, version: string) {
    this.cas = this.kv.cas(key, value, Number(version), NOW)
  }
)

When(
  /^I swap "([^"]+)" to "([^"]+)" expecting version (\d+) at (\d+)$/,
  function (this: LaserWorld, key: string, value: string, version: string, now: string) {
    this.cas = this.kv.cas(key, value, Number(version), Number(now))
  }
)

Then(/^the swap commits version (\d+)$/, function (this: LaserWorld, version: string) {
  assert.deepEqual(this.cas, { kind: "committed", version: Number(version) })
})

Then(
  /^the swap conflicts with current version (\d+)$/,
  function (this: LaserWorld, current: string) {
    assert.deepEqual(this.cas, { kind: "conflict", current: Number(current) })
  }
)

Then("the swap conflicts because the key is absent", function (this: LaserWorld) {
  assert.deepEqual(this.cas, { kind: "conflict" })
})

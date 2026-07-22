import assert from "node:assert/strict"
import { Given, Then, When } from "@cucumber/cucumber"
import type { LaserWorld } from "../world.js"

When("I read the negotiated capabilities", async function (this: LaserWorld) {
  this.capabilities = await this.requireLaser().capabilities()
})

Given(
  "a managed-query connection that does not advertise read-your-writes",
  async function (this: LaserWorld) {
    const laser = this.requireLaser()
    const capabilities = await laser.capabilities()
    this.laser = laser.withCapabilities({
      ...capabilities,
      managed: true,
      query: { ...capabilities.query, available: true, consistency: "eventual" }
    })
  }
)

Then("managed query is unavailable", function (this: LaserWorld) {
  assert.equal(this.capabilities?.query.available, false)
})

Then("managed key-value is unavailable", function (this: LaserWorld) {
  assert.equal(this.capabilities?.kv.available, false)
})

Then("forks are unavailable", function (this: LaserWorld) {
  assert.equal(this.capabilities?.forks, false)
})

Then("the coordination features are unavailable", function (this: LaserWorld) {
  assert.equal(this.capabilities?.kv.cas, false)
  assert.equal(this.capabilities?.query.consistency, "eventual")
})

When(/^I run a query against topic "([^"]+)"$/, async function (this: LaserWorld, topic: string) {
  await this.capture(() => this.requireLaser().query(topic).fetch())
})

When(
  /^I run a read-your-writes query against topic "([^"]+)"$/,
  async function (this: LaserWorld, topic: string) {
    await this.capture(() => this.requireLaser().query(topic).readYourWrites().fetch())
  }
)

When(
  /^I compare-and-swap key "([^"]+)" in namespace "([^"]+)" expecting it absent$/,
  async function (this: LaserWorld, key: string, namespace: string) {
    await this.capture(() =>
      this.requireLaser().kv(namespace).set(new TextEncoder().encode(key)).bytes(new Uint8Array([1])).expectAbsent().commit()
    )
  }
)

Then("the run registry is unavailable", async function (this: LaserWorld) {
  assert.equal((await this.requireLaser().capabilities()).agentWorkflow, false)
})

When(/^I submit a run to agent "([^"]+)"$/, async function (this: LaserWorld, agent: string) {
  await this.capture(() => this.requireLaser().runs().submit(agent))
})

When(/^I read the status of run "([^"]+)"$/, async function (this: LaserWorld, run: string) {
  await this.capture(() => this.requireLaser().runs().status(run))
})

When(/^I cancel run "([^"]+)"$/, async function (this: LaserWorld, run: string) {
  await this.capture(() => this.requireLaser().runs().cancel(run))
})

When("I list runs", async function (this: LaserWorld) {
  await this.capture(() => this.requireLaser().runs().list().fetch())
})

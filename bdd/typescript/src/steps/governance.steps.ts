import assert from "node:assert/strict"
import { Given, Then, When } from "@cucumber/cucumber"
import {
  ActionDecision,
  AgentTopic,
  GovernorMode,
  PolicyBlockedError,
  type ActionGovernor
} from "@laserdata/laser-sdk"
import { eventual } from "../support/eventual.js"
import type { LaserWorld } from "../world.js"

const encoder = new TextEncoder()

Given(
  /^the laser is governed by a policy that blocks "([^"]+)" in "([^"]+)" mode$/,
  function (this: LaserWorld, needle: string, mode: string) {
    const governor: ActionGovernor = {
      decide: (action) =>
        Promise.resolve(
          startsWith(action.payload, encoder.encode(needle))
            ? ActionDecision.block("blocked by policy")
            : ActionDecision.allow()
        )
    }
    this.laser = this
      .requireLaser()
      .withGovernor(
        governor,
        mode === GovernorMode.Observe ? GovernorMode.Observe : GovernorMode.Enforce
      )
  }
)

When(
  /^I send a governed agent command "([^"]+)"$/,
  async function (this: LaserWorld, body: string) {
    await this.capture(() =>
      this.requireLaser().sendAgent(AgentTopic.Commands, encoder.encode(body), {
        conversationId: this.requireConversation()
      })
    )
  }
)

When(
  /^I publish a governed business record "([^"]+)"$/,
  async function (this: LaserWorld, body: string) {
    const topic = this.requireLaser().topic("business.audit")
    await topic.ensure(1)
    await this.capture(() =>
      topic
        .publish()
        .provenance({ conversationId: this.requireConversation() })
        .payload(encoder.encode(body))
        .send()
    )
  }
)

Then("the send is rejected by policy", function (this: LaserWorld) {
  assert.ok(this.error instanceof PolicyBlockedError)
})

Then(
  /^the audit topic records a "([^"]+)" decision with outcome "([^"]+)"$/,
  async function (this: LaserWorld, decision: string, outcome: string) {
    await eventual(async () => {
      const evidence = await this.requireLaser().policyEvidence(this.requireConversation())
      return evidence.some((item) => item.decision === decision && item.outcome === outcome)
        ? true
        : undefined
    }, "policy evidence")
  }
)

function startsWith(value: Uint8Array, prefix: Uint8Array): boolean {
  if (prefix.byteLength > value.byteLength) return false
  return prefix.every((byte, index) => value[index] === byte)
}

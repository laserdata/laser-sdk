import assert from "node:assert/strict"
import { Then, When } from "@cucumber/cucumber"
import { sessionTurnText, type SessionTurn, type SessionTurnKind } from "@laserdata/laser-sdk"
import { eventual } from "../support/eventual.js"
import type { LaserWorld } from "../world.js"

const encoder = new TextEncoder()

const labelled = (turn: SessionTurn): string => `${turn.kind}:${sessionTurnText(turn)}`

When(/^I open the session "([^"]+)"$/, function (this: LaserWorld, id: string) {
  this.session = this.requireLaser().sessions().create(id)
})

When(
  /^I append an? "([^"]+)" turn "([^"]+)" to the session$/,
  async function (this: LaserWorld, kind: string, text: string) {
    await this.requireSession().append(kind as SessionTurnKind, encoder.encode(text))
    // The client reads record timestamps at millisecond precision, so turns on
    // different topics need distinct milliseconds to keep their append order.
    await new Promise((resolve) => setTimeout(resolve, 2))
  }
)

When(
  /^I take a session checkpoint after (\d+) turns?$/,
  async function (this: LaserWorld, turns: string) {
    const session = this.requireSession()
    this.checkpoint = await eventual(async () => {
      const checkpoint = await session.checkpoint()
      const seen = await session.turnsAt(checkpoint)
      return seen.length === Number(turns) ? checkpoint : undefined
    }, "the session checkpoint", 10_000)
  }
)

Then(
  /^the session context is "([^"]+)", "([^"]+)", "([^"]+)" in order$/,
  async function (this: LaserWorld, first: string, second: string, third: string) {
    const session = this.requireSession()
    const turns = await eventual(async () => {
      const turns = await session.context()
      return turns.length === 3 ? turns : undefined
    }, "the session context", 10_000)
    assert.deepEqual(turns.map(labelled), [first, second, third])
  }
)

Then(
  /^opening the session "([^"]+)" again reaches the same conversation$/,
  function (this: LaserWorld, id: string) {
    assert.ok(
      this.requireLaser().sessions().create(id).conversation.equals(this.requireSession().conversation)
    )
  }
)

Then(
  /^opening the session "([^"]+)" reaches a different conversation$/,
  function (this: LaserWorld, id: string) {
    assert.ok(
      !this.requireLaser().sessions().create(id).conversation.equals(this.requireSession().conversation)
    )
  }
)

Then(/^the turns since the checkpoint are "([^"]+)"$/, async function (this: LaserWorld, text: string) {
  const session = this.requireSession()
  const checkpoint = this.requireCheckpoint()
  const turns = await eventual(async () => {
    const turns = await session.turnsSince(checkpoint)
    return turns.length === 1 ? turns : undefined
  }, "the turns since the checkpoint", 10_000)
  assert.deepEqual(turns.map(sessionTurnText), [text])
})

Then(/^the turns at the checkpoint are "([^"]+)"$/, async function (this: LaserWorld, text: string) {
  const turns = await this.requireSession().turnsAt(this.requireCheckpoint())
  assert.deepEqual(turns.map(sessionTurnText), [text])
})

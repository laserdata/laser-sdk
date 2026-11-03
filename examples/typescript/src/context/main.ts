import {
  AgentTopic,
  Chain,
  ConversationId,
  LastN,
  TokenBudget,
  type Laser
} from "@laserdata/laser-sdk"
import { decodeUtf8, PARTITIONS, phase, runExample, SESSION_RETENTION, utf8 } from "../common.js"

export const EXAMPLE = "context"
const LAST_N = 20
const TOKEN_BUDGET = 4_000

/** The assembled turn texts, oldest first. */
export async function run(laser: Laser, _signal: AbortSignal): Promise<readonly string[]> {
  // Conversation turns ride the shared session topic, created once here.
  await laser.bootstrap(PARTITIONS, SESSION_RETENTION)
  const conversation = ConversationId.new()

  phase("append a conversation, then assemble it under a budget")
  const ctx = laser.context(conversation)
  await ctx.append(AgentTopic.Sessions, utf8("drain node-7"))
  await ctx.append(AgentTopic.Sessions, utf8("drained, 0 connections left"))

  // The shape of a prompt's context is a declared policy, not slicing logic
  // spread through the application: cap the turns, then fit the budget.
  const turns = await ctx.fetchWith(
    [AgentTopic.Sessions],
    new Chain([new LastN(LAST_N), new TokenBudget(TOKEN_BUDGET)])
  )

  console.log(`  ${String(turns.length)} turn(s) within ${String(TOKEN_BUDGET)} tokens:`)
  for (const turn of turns) {
    console.log(`    ${decodeUtf8(turn.payload)}`)
  }
  return turns.map((turn) => decodeUtf8(turn.payload))
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

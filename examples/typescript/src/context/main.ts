import {
  AgentTopic,
  ContextChain,
  ConversationId,
  LastN,
  TokenBudget,
  type Laser
} from "@laserdata/laser-sdk"
import { PARTITIONS, decodeUtf8, phase, runExample, utf8 } from "../common.js"

export const EXAMPLE = "context"
const LAST_N = 20
const TOKEN_BUDGET = 4_000

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  // Conversation turns ride the well-known agent topics, created once here.
  await laser.bootstrap(PARTITIONS)
  const conversation = ConversationId.new()

  phase("append a conversation, then assemble it under a budget")
  const ctx = laser.context(conversation)
  await ctx.append(AgentTopic.Commands, utf8("book me an aisle seat"))
  await ctx.append(AgentTopic.Responses, utf8("booked, aisle 12"))

  // The shape of a prompt's context is a declared policy, not slicing logic
  // spread through the application: cap the turns, then fit the budget.
  const turns = await ctx.fetchWith(
    [AgentTopic.Commands, AgentTopic.Responses],
    new ContextChain([new LastN(LAST_N), new TokenBudget(TOKEN_BUDGET)])
  )

  console.log(`  ${String(turns.length)} turn(s) within ${String(TOKEN_BUDGET)} tokens:`)
  for (const turn of turns) {
    console.log(`    ${decodeUtf8(turn.payload)}`)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

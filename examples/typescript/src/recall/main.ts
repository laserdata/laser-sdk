import { ConversationId, type Laser } from "@laserdata/laser-sdk"
import { PARTITIONS, decodeUtf8, phase, runExample, utf8 } from "../common.js"

export const EXAMPLE = "recall"
const NAMESPACE = "customer:42"
const FACT = "Prefers aisle seats, travels monthly"

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  // Memory records ride the well-known agent topics, created once here.
  await laser.bootstrap(PARTITIONS)
  const conversation = ConversationId.new()

  phase("all four verbs: remember, recall, improve, forget")
  const memory = laser.memory(NAMESPACE)

  const fact = await memory.remember(utf8(FACT)).conversation(conversation).send()

  // Durable log memory recalls the newest matching facts. Similarity ranking
  // is the vector/reranker path shown in the full memory example.
  const hits = await memory.recall().conversation(conversation).recent().limit(5).folded().fetch()

  console.log("  newest recalled fact(s):")
  for (const hit of hits) {
    console.log(`    ${decodeUtf8(hit.payload)}`)
  }

  // Reinforce what was useful, then retire it. Both are records on the memory
  // topic, so the store stays an auditable history, not a mutable cell.
  await memory.improve({ conversation }, { target: fact, weight: 1 })
  await memory.forget({ conversation }, fact)
  console.log(`  reinforced then forgot ${fact.toString()}`)
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

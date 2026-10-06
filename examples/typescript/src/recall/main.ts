import { ConversationId, type Laser } from "@laserdata/laser-sdk"
import { PARTITIONS, decodeUtf8, phase, runExample, utf8 } from "../common.js"

export const EXAMPLE = "recall"
const NAMESPACE = "host:node-7"
const FACT = "node-7 sits in the eu-west pool, rotates keys monthly"

/** The recalled fact texts. */
export async function run(laser: Laser, _signal: AbortSignal): Promise<readonly string[]> {
  // Memory records ride the well-known agent topics, created once here.
  await laser.bootstrap(PARTITIONS)
  const conversation = ConversationId.new()

  phase("all four verbs: remember, recall, improve, forget")
  const memory = laser.memory(NAMESPACE)

  const fact = await memory.remember(utf8(FACT)).scope(conversation).send()

  // Durable log memory recalls the newest matching facts. Similarity ranking
  // is the vector/reranker path shown in the full memory example.
  const hits = await memory.recall(conversation).recent().limit(5).folded().fetch()

  console.log("  newest recalled fact(s):")
  for (const hit of hits) {
    console.log(`    ${decodeUtf8(hit.payload)}`)
  }

  // Reinforce what was useful, then retire it. Both are records on the memory
  // topic, so the store stays an auditable history, not a mutable cell.
  await memory.improve({ conversation }, { target: fact, weight: 1 })
  await memory.forget({ conversation }, fact)
  console.log(`  reinforced then forgot ${fact.toString()}`)
  return hits.map((hit) => decodeUtf8(hit.payload))
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

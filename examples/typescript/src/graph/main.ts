import { graphNodeEntity, type Laser } from "@laserdata/laser-sdk"
import { graphNodeValue, managedGate, phase, runExample } from "../common.js"

export const EXAMPLE = "graph"
const GRAPH = "kg"
const CUSTOMER = "customer:42"
const RELATION = "purchased"
const PRODUCTS = ["product:7", "product:9"]

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const capabilities = await laser.capabilities()
  if (!managedGate(capabilities, "graph", EXAMPLE)) return
  const graph = laser.graph(GRAPH)

  phase("relate entities, then traverse from one of them")
  // `link` upserts both content-addressed entity nodes and the typed edge
  // between them, so re-linking the same triple converges instead of growing.
  for (const product of PRODUCTS) {
    await graph.link(CUSTOMER, RELATION, product)
  }

  // The same id `link` derived, rebuilt locally: a node is addressed by its
  // content, never by a server-assigned key.
  const customer = graphNodeEntity("customer", "42")
  const purchases = await graph.neighbors(customer.id, "out", RELATION, 1)

  console.log(`  ${CUSTOMER} ${RELATION}:`)
  for (const node of purchases.nodes) {
    console.log(`    ${node.labels[0] ?? "entity"}:${graphNodeValue(node)}`)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

import { graphNodeEntity, type Laser } from "@laserdata/laser-sdk"
import { graphNodeValue, managedGate, phase, runExample } from "../common.js"

export const EXAMPLE = "graph"
const GRAPH = "kg"
const HOST = "host:node-7"
const RELATION = "runs"
const SERVICES = ["service:auth", "service:metrics"]

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const capabilities = await laser.capabilities()
  if (!managedGate(capabilities, "graph", EXAMPLE)) return
  const graph = laser.graph(GRAPH)

  phase("relate entities, then traverse from one of them")
  // `link` upserts both content-addressed entity nodes and the typed edge
  // between them, so re-linking the same triple converges instead of growing.
  for (const service of SERVICES) {
    await graph.link(HOST, RELATION, service)
  }

  // The same id `link` derived, rebuilt locally: a node is addressed by its
  // content, never by a server-assigned key.
  const host = graphNodeEntity("host", "node-7")
  const services = await graph.neighbors(host.id, "out", RELATION, 1)

  console.log(`  ${HOST} ${RELATION}:`)
  for (const node of services.nodes) {
    console.log(`    ${node.labels[0] ?? "entity"}:${graphNodeValue(node)}`)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

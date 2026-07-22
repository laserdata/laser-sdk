import {
  ContentType,
  ConversationId,
  MemoryHandle,
  MemoryKind,
  graphNodeEntity,
  parseProjectionId,
  type Embedder,
  type Laser
} from "@laserdata/laser-sdk"

import { decodeUtf8, managedGate, runExample, utf8 } from "../common.js"

export const EXAMPLE = "memory"
const GRAPH = "ops-knowledge"

class DeterministicEmbedder implements Embedder {
  embed(text: string): Promise<readonly number[]> {
    const dimensions = Array.from({ length: 64 }, () => 0)
    for (const token of text.toLowerCase().split(/[^a-z0-9]+/u)) {
      if (token.length === 0) continue
      let hash = 2_166_136_261
      for (const byte of new TextEncoder().encode(token)) {
        hash = Math.imul(hash ^ byte, 16_777_619) >>> 0
      }
      const index = hash % dimensions.length
      dimensions[index] = (dimensions[index] ?? 0) + 1
    }
    const norm = Math.sqrt(dimensions.reduce((sum, value) => sum + value * value, 0)) || 1
    return Promise.resolve(dimensions.map((value) => value / norm))
  }
}

async function vectorPhase(): Promise<void> {
  const memory = MemoryHandle.vector(new DeterministicEmbedder())
  await memory.remember(utf8("checkout latency traces to the database pool")).dedup().send()
  const stale = await memory.remember(utf8("billing uses the legacy queue")).send()
  const durable = await memory
    .remember(utf8("billing uses the durable ledger"))
    .kind(MemoryKind.Fact)
    .durable()
    .send()
  const recent = await memory.recall().recent().limit(2).fetch()
  const semantic = await memory.recall().semantic("why is checkout slow").limit(1).fetch()
  await memory.improve({}, { target: durable, weight: 2, note: "confirmed by operator" })
  await memory.forget({}, stale)
  console.log(`recent: ${recent.map((item) => decodeUtf8(item.payload)).join(" | ")}`)
  console.log(`semantic: ${semantic.map((item) => decodeUtf8(item.payload)).join(" | ")}`)
}

async function durablePhase(laser: Laser): Promise<void> {
  const conversation = ConversationId.derive("memory-example")
  const scoped = laser.context(conversation).memory("ops-memory")
  await scoped.remember(utf8("service api depends on component database")).dedup().durable().send()
  const block = await scoped.context(128)
  console.log(`durable context: ${block}`)
}

async function graphPhase(laser: Laser): Promise<void> {
  // Register the graph first: the entity schema records the named knowledge
  // graph, and only a registered graph folds direct upserts. Registration is
  // applied asynchronously, so poll the browse until the apply lands.
  const id = parseProjectionId(`${GRAPH}.v1`)
  await laser.projections().registerGraph({
    id,
    name: GRAPH,
    version: 1,
    kind: { kind: "graph" },
    contentType: ContentType.Json,
    extraction: { fields: [], inlinePayload: false },
    entitySchema: {
      nodes: [
        { label: "Service", valuePointer: "/service" },
        { label: "Component", valuePointer: "/component" }
      ],
      edges: [{ edgeType: "depends_on", fromPointer: "/service", toPointer: "/component" }]
    },
    inlinePayloadDefault: false
  })
  const deadline = Date.now() + 60_000
  while ((await laser.projections().get(id)) === undefined) {
    if (Date.now() >= deadline) throw new Error(`graph \`${GRAPH}\` was never registered`)
    await new Promise((resolve) => setTimeout(resolve, 150))
  }
  const graph = laser.graph(GRAPH)
  await graph.link("Service:api", "depends_on", "Component:database")
  await graph.link("Service:billing", "depends_on", "Component:ledger")
  const api = graphNodeEntity("Service", "api")
  const neighbors = await graph.neighbors(api.id, "out", "depends_on", 1)
  console.log(`api dependency nodes: ${String(neighbors.nodes.length)}`)
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  await vectorPhase()
  const capabilities = await laser.capabilities()
  if (managedGate(capabilities, "query", EXAMPLE) && managedGate(capabilities, "kv", EXAMPLE)) {
    await laser.bootstrap(1)
    await durablePhase(laser)
  }
  if (managedGate(capabilities, "graph", EXAMPLE)) await graphPhase(laser)
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)

import {
  AgentTopic,
  ContentType,
  ConversationId,
  MemoryHandle,
  MemoryKind,
  filterPred,
  graphEdgeRelate,
  graphNodeEntity,
  parseProjectionId,
  type Embedder,
  type GraphEdge,
  type GraphNode,
  type Laser,
  type MemoryId
} from "@laserdata/laser-sdk"

import {
  connectExample,
  installShutdownSignals,
  managedGate,
  PARTITIONS,
  phase,
  printHits,
  printNodes,
  printNodesOf,
  utf8
} from "../common.js"

export const EXAMPLE = "memory"
const GRAPH = "ops"
const KNOWLEDGE = [
  "checkout latency spikes are usually database connection pool exhaustion",
  "billing double-charges trace back to retries without an idempotency key",
  "search returning stale results means the nightly index rebuild failed",
  "checkout pages recover fastest by failing over to the read replica",
  "auth token errors after a deploy come from the rotated signing key",
  "cart abandonment climbs when the cache eviction rate is set too aggressive",
  "inventory drift is the message queue dropping stock-adjustment events",
  "recommendation gaps appear when the search index lags behind the catalog"
] as const
const ENTITIES = [
  ["Service", "checkout"],
  ["Service", "billing"],
  ["Service", "search"],
  ["Service", "cart"],
  ["Service", "auth"],
  ["Service", "recommendations"],
  ["Service", "inventory"],
  ["Service", "notifications"],
  ["Component", "orders-db"],
  ["Component", "db-pool"],
  ["Component", "read-replica"],
  ["Component", "search-index"],
  ["Component", "signing-key"],
  ["Component", "cache"],
  ["Component", "payment-gateway"],
  ["Component", "message-queue"],
  ["Team", "payments"],
  ["Team", "search-platform"],
  ["Team", "core-platform"],
  ["Incident", "INC-101"],
  ["Incident", "INC-102"]
] as const
const RELATIONSHIPS = [
  ["checkout", "depends_on", "orders-db"],
  ["checkout", "depends_on", "db-pool"],
  ["checkout", "depends_on", "payment-gateway"],
  ["checkout", "mitigated_by", "read-replica"],
  ["billing", "depends_on", "orders-db"],
  ["billing", "depends_on", "signing-key"],
  ["billing", "depends_on", "payment-gateway"],
  ["search", "depends_on", "search-index"],
  ["search", "depends_on", "cache"],
  ["search", "mitigated_by", "cache"],
  ["cart", "depends_on", "cache"],
  ["cart", "depends_on", "orders-db"],
  ["auth", "depends_on", "signing-key"],
  ["recommendations", "depends_on", "search-index"],
  ["recommendations", "depends_on", "cache"],
  ["inventory", "depends_on", "orders-db"],
  ["inventory", "depends_on", "message-queue"],
  ["notifications", "depends_on", "message-queue"],
  ["read-replica", "replicates", "orders-db"],
  ["payments", "owns", "checkout"],
  ["payments", "owns", "billing"],
  ["search-platform", "owns", "search"],
  ["search-platform", "owns", "recommendations"],
  ["core-platform", "owns", "auth"],
  ["core-platform", "owns", "cart"],
  ["core-platform", "owns", "inventory"],
  ["core-platform", "owns", "notifications"],
  ["INC-101", "affected", "checkout"],
  ["INC-101", "affected", "db-pool"],
  ["INC-102", "affected", "search"],
  ["INC-102", "affected", "search-index"]
] as const
const MITIGATION_SINCE_US = 1_900_000_000_000_000n

class DeterministicEmbedder implements Embedder {
  embed(text: string): Promise<readonly number[]> {
    const dimensions = Array.from({ length: 64 }, () => 0)
    for (const word of text.toLowerCase().split(/\s+/u)) {
      const token = word.replace(/^[^a-z0-9]+|[^a-z0-9]+$/gu, "")
      if (token.length === 0) continue
      let hash = 0
      for (const byte of new TextEncoder().encode(token)) {
        hash = (Math.imul(hash, 31) + byte) >>> 0
      }
      const index = hash % dimensions.length
      dimensions[index] = (dimensions[index] ?? 0) + 1
    }
    const norm = Math.sqrt(dimensions.reduce((sum, value) => sum + value * value, 0)) || 1
    return Promise.resolve(dimensions.map((value) => value / norm))
  }
}

async function vectorPhase(conversation: ConversationId): Promise<void> {
  const memory = MemoryHandle.vector(new DeterministicEmbedder())
  phase("Remember")
  let replicaNote: MemoryId | undefined
  let staleNote: MemoryId | undefined
  for (const fact of KNOWLEDGE) {
    const id = await memory
      .remember(utf8(fact))
      .conversation(conversation)
      .kind(MemoryKind.Fact)
      .send()
    if (fact.includes("read replica")) replicaNote = id
    if (fact.includes("index rebuild")) staleNote = id
  }
  console.log(`remembered ${String(KNOWLEDGE.length)} facts`)

  phase("Recall")
  const question = "checkout is slow during the sale"
  const initial = await memory
    .recall()
    .conversation(conversation)
    .semantic(question)
    .limit(3)
    .fetch()
  printHits(`recall for "${question}"`, initial)

  if (replicaNote === undefined) throw new Error("the read-replica note was not remembered")
  phase("Improve")
  await memory.improve(
    { conversation },
    { target: replicaNote, weight: 1, note: "resolved the incident" }
  )
  const improved = await memory
    .recall()
    .conversation(conversation)
    .semantic(question)
    .limit(3)
    .fetch()
  printHits(`recall after feedback for "${question}"`, improved)
  if (!improved[0]?.id.equals(replicaNote)) {
    throw new Error("feedback did not rank the read-replica note first")
  }
  console.log("the upvoted note now ranks first")

  if (staleNote === undefined) throw new Error("the index-rebuild note was not remembered")
  phase("Forget")
  await memory.forget({ conversation }, staleNote)
  const remaining = await memory
    .recall()
    .conversation(conversation)
    .semantic("search results are stale")
    .limit(3)
    .fetch()
  if (remaining.some((item) => item.id.equals(staleNote))) {
    throw new Error("forgotten memory still appears in recall")
  }
  console.log("forgot the superseded index-rebuild note, it no longer recalls")
}

async function durablePhase(laser: Laser, conversation: ConversationId): Promise<void> {
  phase("Remember durable facts")
  const durable = await laser
    .memoryTopic("incidents")
    .partitions(PARTITIONS)
    .ttl(86_400_000)
    .build()
  for (const fact of KNOWLEDGE) {
    await durable.remember(utf8(fact)).conversation(conversation).durable().send()
  }
  const durableHits = await durable.recall().conversation(conversation).limit(3).fetch()
  console.log(
    `stored ${String(KNOWLEDGE.length)} durable facts, recalled ` +
      `${String(durableHits.length)} most-recent`
  )
  for (const hit of durableHits) {
    if (hit.source?.kind === "message") {
      console.log(
        `  recalled from source ${String(hit.source.stream)}/${String(hit.source.topic)} ` +
          `partition ${String(hit.source.partition)} offset ${hit.source.offset.toString()}`
      )
    }
  }

  phase("Scope the session: messages and memory under one conversation")
  const session = laser.context(conversation)
  await session.append(AgentTopic.Audit, utf8("incident opened: checkout slow"))
  const scopedHits = await session.memory(durable).recall().limit(3).fetch()
  const trail = await session.fetch([AgentTopic.Audit], 8)
  console.log(
    `one scope recalled ${String(scopedHits.length)} durable facts and read back ` +
      `${String(trail.length)} of the conversation's messages`
  )
}

function requiredNode(nodes: ReadonlyMap<string, GraphNode>, value: string): GraphNode {
  const node = nodes.get(value)
  if (node === undefined) throw new Error(`graph node \`${value}\` was not created`)
  return node
}

async function graphPhase(laser: Laser): Promise<void> {
  phase("Build the knowledge graph")
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
      edges: [
        {
          edgeType: "depends_on",
          fromPointer: "/service",
          toPointer: "/component"
        }
      ]
    },
    inlinePayloadDefault: false
  })
  const deadline = Date.now() + 60_000
  while ((await laser.projections().get(id)) === undefined) {
    if (Date.now() >= deadline) throw new Error(`graph \`${GRAPH}\` was never registered`)
    await new Promise((resolve) => setTimeout(resolve, 150))
  }

  const registry = laser.kv("topology")
  const byValue = new Map<string, GraphNode>()
  for (const [label, value] of ENTITIES) {
    await registry.set(utf8(value)).json({ kind: label, value }).send()
    byValue.set(value, {
      ...graphNodeEntity(label, value),
      source: { kind: "kv", namespace: "topology", key: value }
    })
  }

  const edges: GraphEdge[] = RELATIONSHIPS.map(([from, edgeType, to]) => {
    const edge = {
      ...graphEdgeRelate(requiredNode(byValue, from), edgeType, requiredNode(byValue, to)),
      source: { kind: "kv" as const, namespace: "topology", key: from }
    }
    return edgeType === "mitigated_by" ? { ...edge, validFrom: MITIGATION_SINCE_US } : edge
  })

  const graph = laser.graph(GRAPH)
  const nodes = [...byValue.values()]
  const checkout = requiredNode(byValue, "checkout")
  const incident = requiredNode(byValue, "INC-101")
  await graph.upsert(nodes, edges)
  const bitemporal = edges.filter((edge) => edge.validFrom !== undefined).length
  console.log(
    `upserted ${String(nodes.length)} nodes and ${String(edges.length)} edges ` +
      `(${String(bitemporal)} bitemporal)`
  )

  phase("Read a node's neighbors")
  const around = await graph.neighbors(checkout.id, "out", undefined, 1)
  printNodes("checkout's one-hop neighborhood", around.nodes)

  phase("Traverse from a predicate")
  const dependencies = await graph
    .startMatch(filterPred("label", "eq", { kind: "string", value: "Service" }))
    .out("depends_on")
    .limit(100)
    .fetch()
  printNodesOf("components every Service depends on", "Component", dependencies.nodes)

  phase("Trace an incident's blast radius")
  const blast = await laser.graph(GRAPH).startIds([incident.id]).out("affected").fetch()
  const touched = blast.nodes
    .filter((node) => !node.id.equals(incident.id))
    .map((node) => {
      const value = node.attrs.find(([key]) => key === "value")?.[1]
      return value?.kind === "string" ? value.value : "?"
    })
    .sort()
  console.log(`what INC-101 affected: ${touched.join(", ")}`)

  phase("Trace a node back to its source")
  const checkoutResult = around.nodes.find((node) => node.id.equals(checkout.id))
  if (checkoutResult?.source?.kind === "kv") {
    console.log(
      `checkout's source record is ${checkoutResult.source.namespace}/${checkoutResult.source.key}`
    )
  }

  phase("Read the graph as of a point in time")
  const before = await laser
    .graph(GRAPH)
    .startIds([checkout.id])
    .out("mitigated_by")
    .asOf(MITIGATION_SINCE_US - 1n)
    .fetch()
  const after = await laser
    .graph(GRAPH)
    .startIds([checkout.id])
    .out("mitigated_by")
    .asOf(MITIGATION_SINCE_US + 1n)
    .fetch()
  const reached = (nodes: readonly GraphNode[]): number =>
    nodes.filter((node) => !node.id.equals(checkout.id)).length
  console.log(
    `checkout mitigations before the rollout: ${String(reached(before.nodes))}, ` +
      `after: ${String(reached(after.nodes))}`
  )

  phase("Return whole paths")
  const paths = await laser
    .graph(GRAPH)
    .startIds([incident.id])
    .out("affected")
    .returnPaths()
    .fetch()
  console.log(`INC-101 reaches ${String(paths.paths.length)} components by a traced path`)
}

async function managedPhase(laser: Laser, conversation: ConversationId): Promise<void> {
  const capabilities = await laser.capabilities()
  if (managedGate(capabilities, "graph", EXAMPLE) && managedGate(capabilities, "kv", EXAMPLE)) {
    await laser.bootstrap(PARTITIONS)
    await durablePhase(laser, conversation)
    await graphPhase(laser)
  }
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const conversation = ConversationId.new()
  await vectorPhase(conversation)
  await managedPhase(laser, conversation)
  phase("done")
  console.log("memory recalls what is relevant, the graph shows how it connects")
}

async function main(): Promise<void> {
  const conversation = ConversationId.new()
  await vectorPhase(conversation)

  const target = process.env["LASER_CONNECTION_STRING"] ?? process.env["LASER_SERVER"] ?? ""
  if (target.trim().length === 0) {
    console.log(
      "\nDurable memory and the knowledge graph require LaserData Cloud. " +
        "Set LASER_CONNECTION_STRING=user:pwd@your-host to run them."
    )
    return
  }

  let laser: Laser
  try {
    laser = await connectExample(EXAMPLE)
  } catch {
    console.log(
      "\nDurable memory and the knowledge graph require LaserData Cloud. " +
        "Set LASER_CONNECTION_STRING=user:pwd@your-host to run them."
    )
    return
  }

  await using connection = laser
  using shutdown = installShutdownSignals()
  await managedPhase(connection, conversation)
  if (shutdown.signal.aborted) return

  phase("done")
  console.log("memory recalls what is relevant, the graph shows how it connects")
}

if (import.meta.url === `file://${process.argv[1]}`) await main()

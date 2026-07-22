import type { Capabilities } from "../client/capabilities.js"
import { GraphExecutionError, ProtocolError, UnsupportedError } from "../client/errors.js"
import { executeManaged, type ManagedTransport } from "../client/managed.js"
import {
  GraphNeighborsCommand,
  GraphQueryCommand,
  GraphUpsertCommand,
  type ManagedCommand
} from "../wire/commands.js"
import {
  type EdgeDir,
  type GraphEdge,
  type GraphNode,
  type GraphReply,
  type GraphResult,
  type GraphReturn,
  type GraphStart,
  type Hop,
  type NodeId,
  graphEdgeRelate,
  graphNodeEntity,
  validateGraphName
} from "../wire/graph.js"
import type { Filter } from "../wire/query.js"
import { DEFAULT_RECALL_LIMIT } from "../wire/limits.js"

export type GraphBackend = ManagedTransport

async function executeGraph<Request>(
  backend: GraphBackend,
  capabilities: Capabilities,
  command: ManagedCommand<Request, GraphReply>,
  request: Request
): Promise<GraphResult> {
  const reply = await executeManaged(backend, capabilities, command, request)
  if (reply.kind === "ok") return reply.result
  if (reply.kind === "err") {
    if (reply.error.kind === "unsupported") throw new UnsupportedError(reply.error.message)
    throw new GraphExecutionError(`graph command failed: ${reply.error.kind}`, reply.error)
  }
  throw new ProtocolError(`graph: unrecognized reply variant \`${reply.tag}\``, {
    commandCode: command.code
  })
}

// The entity node for a `kind:value` style id: the id string is both the
// label-ish identity and the `value` attribute, so `link`ed entities
// converge on one content-addressed node per id.
function entityNode(id: string): GraphNode {
  const separator = id.indexOf(":")
  return separator === -1
    ? graphNodeEntity("entity", id)
    : graphNodeEntity(id.slice(0, separator), id.slice(separator + 1))
}

function nowMicros(): bigint {
  return BigInt(Date.now()) * 1000n
}

// A fluent knowledge-graph traversal, created by `Laser.graph(name)`. Set a
// start (`.startIds`/`.startMatch`/`.startNearest`), add hops
// (`.out`/`.incoming`/`.both`), pick what to return, and finish with
// `.fetch()`. Gated on the `graph` capability: a traversal rides the
// managed binary transport.
export class GraphHandle {
  private startValue: GraphStart | undefined
  private hops: Hop[] = []
  private nodeFilterValue: Filter | undefined
  private edgeFilterValue: Filter | undefined
  private returnValue: GraphReturn = "nodes"
  private limitValue = DEFAULT_RECALL_LIMIT
  private asOfValue: bigint | undefined
  private conversationValue: string | undefined

  constructor(
    private readonly backend: GraphBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly name: string
  ) {}

  // Narrow the traversal to elements a single conversation asserted (the
  // conversation lens): only nodes and edges whose source records that
  // conversation are traversed and returned. Applies to both `.fetch` and
  // `.neighbors`. Reads the whole graph when unset.
  conversation(conversationId: string): this {
    this.conversationValue = conversationId
    return this
  }

  // Start the traversal from explicit node ids.
  startIds(ids: readonly NodeId[]): this {
    this.startValue = { kind: "ids", ids }
    return this
  }

  // Start from the nodes matching a predicate.
  startMatch(filter: Filter): this {
    this.startValue = { kind: "match", filter }
    return this
  }

  // Start from the nodes nearest an embedding (vector-seeded traversal).
  startNearest(embedding: readonly number[], k: number): this {
    this.startValue = { kind: "nearest", embedding, k }
    return this
  }

  // Follow outgoing edges of `edgeType` one hop.
  out(edgeType: string): this {
    this.hops.push({ edgeType, dir: "out", max: 1 })
    return this
  }

  // Follow incoming edges of `edgeType` one hop.
  incoming(edgeType: string): this {
    this.hops.push({ edgeType, dir: "in", max: 1 })
    return this
  }

  // Follow edges of `edgeType` one hop in both directions.
  both(edgeType: string): this {
    this.hops.push({ edgeType, dir: "both", max: 1 })
    return this
  }

  // Return the traversed edges instead of the reachable nodes.
  returnEdges(): this {
    this.returnValue = "edges"
    return this
  }

  // Return the traversed edges as `(source, type, destination)` triplets
  // instead of the reachable nodes.
  returnTriplets(): this {
    this.returnValue = "triplets"
    return this
  }

  // Return whole paths (node and edge id sequences) instead of nodes.
  returnPaths(): this {
    this.returnValue = "paths"
    return this
  }

  // Cap the number of elements returned.
  limit(limit: number): this {
    this.limitValue = limit
    return this
  }

  // Read the graph as of `micros` (valid-time, epoch micros): only edges
  // whose valid-time window contains that instant are traversed. Applies
  // to both `.fetch` and `.neighbors`.
  asOf(micros: bigint): this {
    this.asOfValue = micros
    return this
  }

  // Run the traversal. Requires the `graph` capability.
  async fetch(): Promise<GraphResult> {
    validateGraphName(this.name)
    const capabilities = await this.getCapabilities()
    return executeGraph(this.backend, capabilities, GraphQueryCommand, {
      graph: this.name,
      start: this.startValue ?? { kind: "ids", ids: [] },
      traverse: this.hops,
      return: this.returnValue,
      limit: this.limitValue,
      consistency: "eventual",
      ...(this.nodeFilterValue !== undefined ? { nodeFilter: this.nodeFilterValue } : {}),
      ...(this.edgeFilterValue !== undefined ? { edgeFilter: this.edgeFilterValue } : {}),
      ...(this.asOfValue !== undefined ? { asOf: this.asOfValue } : {}),
      ...(this.conversationValue !== undefined ? { conversation: this.conversationValue } : {})
    })
  }

  // Read a node's neighbors: the nodes reachable in `dir` over `edgeType`
  // (or any type when `undefined`), following the same hop `depth` times.
  // The cheap, common traversal. Requires the `graph` capability.
  async neighbors(
    node: NodeId,
    dir: EdgeDir,
    edgeType: string | undefined,
    depth: number
  ): Promise<GraphResult> {
    validateGraphName(this.name)
    const capabilities = await this.getCapabilities()
    return executeGraph(this.backend, capabilities, GraphNeighborsCommand, {
      graph: this.name,
      node,
      dir,
      depth,
      limit: this.limitValue,
      ...(edgeType !== undefined ? { edgeType } : {}),
      ...(this.asOfValue !== undefined ? { asOf: this.asOfValue } : {}),
      ...(this.conversationValue !== undefined ? { conversation: this.conversationValue } : {})
    })
  }

  // Relate two entities in one call: `.link("customer:42", "opened",
  // "ticket:7")` upserts both content-addressed entity nodes and the typed
  // edge between them. Sugar over `.upsert`, so re-linking the same triple
  // converges on the same nodes and edge. The entity ids double as their
  // labels' values (`kind:value` strings work well).
  async link(from: string, relation: string, to: string): Promise<void> {
    const fromNode = entityNode(from)
    const toNode = entityNode(to)
    const edge = graphEdgeRelate(fromNode, relation, toNode)
    await this.upsert([fromNode, toNode], [edge])
  }

  // Assert the latest value of a single-valued relationship: close every
  // live `relation` edge from `from` that points at a DIFFERENT target
  // (`validTo` now, the bitemporal supersede), then link `from` to `to`.
  // Returns how many edges were closed.
  async relink(from: string, relation: string, to: string): Promise<number> {
    const fromNode = entityNode(from)
    const toNode = entityNode(to)
    const live = await this.neighbors(fromNode.id, "out", relation, 1)
    const now = nowMicros()
    const superseded = live.edges
      .filter((edge) => !edge.to.equals(toNode.id) && edge.validTo === undefined)
      .map((edge) => ({ ...edge, validTo: now }))
    if (superseded.length > 0) await this.upsert([], superseded)
    await this.link(from, relation, to)
    return superseded.length
  }

  // Close the relationship `.link` opened: rewrite the edge with `validTo`
  // now, so the fact is superseded without being destroyed (a bitemporal
  // close). The nodes stay.
  async unlink(from: string, relation: string, to: string): Promise<void> {
    const fromNode = entityNode(from)
    const toNode = entityNode(to)
    const edge = { ...graphEdgeRelate(fromNode, relation, toNode), validTo: nowMicros() }
    await this.upsert([], [edge])
  }

  // Write `nodes` and `edges` into the graph: the projector path, surfaced
  // for callers that build the graph directly rather than through a
  // `graph` projection. Idempotent on content-addressed ids (`GraphNode`/
  // `GraphEdge` from `graphNodeEntity`/`graphEdgeRelate`), so re-applying
  // the same entities is a no-op. Requires the `graph` capability.
  async upsert(nodes: readonly GraphNode[], edges: readonly GraphEdge[]): Promise<void> {
    validateGraphName(this.name)
    const capabilities = await this.getCapabilities()
    await executeGraph(this.backend, capabilities, GraphUpsertCommand, {
      graph: this.name,
      nodes,
      edges
    })
  }
}

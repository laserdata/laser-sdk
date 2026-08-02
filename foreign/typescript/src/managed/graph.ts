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

function entityNode(id: string): GraphNode {
  const separator = id.indexOf(":")
  return separator === -1
    ? graphNodeEntity("entity", id)
    : graphNodeEntity(id.slice(0, separator), id.slice(separator + 1))
}

/** Builds and executes traversals against a named managed knowledge graph. */
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
    private readonly name: string,
    private readonly nowMicros: () => bigint = () => BigInt(Date.now()) * 1000n
  ) {}

  /** Restricts traversal to elements asserted by one conversation. */
  conversation(conversationId: string): this {
    this.conversationValue = conversationId
    return this
  }

  /** Starts from explicit node IDs. */
  startIds(ids: readonly NodeId[]): this {
    this.startValue = { kind: "ids", ids }
    return this
  }

  /** Starts from nodes matching a predicate. */
  startMatch(filter: Filter): this {
    this.startValue = { kind: "match", filter }
    return this
  }

  /** Starts from the nearest nodes to an embedding. */
  startNearest(embedding: readonly number[], k: number): this {
    this.startValue = { kind: "nearest", embedding, k }
    return this
  }

  /** Follows outgoing edges of one type. */
  out(edgeType: string): this {
    this.hops.push({ edgeType, dir: "out", max: 1 })
    return this
  }

  /** Follows incoming edges of one type. */
  incoming(edgeType: string): this {
    this.hops.push({ edgeType, dir: "in", max: 1 })
    return this
  }

  /** Follows edges of one type in both directions. */
  both(edgeType: string): this {
    this.hops.push({ edgeType, dir: "both", max: 1 })
    return this
  }

  /** Returns traversed edges. */
  returnEdges(): this {
    this.returnValue = "edges"
    return this
  }

  /** Returns traversed source, type, and destination triplets. */
  returnTriplets(): this {
    this.returnValue = "triplets"
    return this
  }

  /** Returns complete node and edge paths. */
  returnPaths(): this {
    this.returnValue = "paths"
    return this
  }

  /** Caps the number of returned elements. */
  limit(limit: number): this {
    this.limitValue = limit
    return this
  }

  /** Restricts traversal to edges valid at the given epoch microsecond. */
  asOf(micros: bigint): this {
    this.asOfValue = micros
    return this
  }

  /** Executes the configured traversal. */
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

  /** Returns nodes reachable within a bounded depth. */
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

  /** Upserts two kind:value entities and relates them. */
  async link(from: string, relation: string, to: string): Promise<void> {
    const fromNode = entityNode(from)
    const toNode = entityNode(to)
    const edge = graphEdgeRelate(fromNode, relation, toNode)
    await this.upsert([fromNode, toNode], [edge])
  }

  /** Supersedes other live targets before linking a single-valued relationship. */
  async relink(from: string, relation: string, to: string): Promise<number> {
    const fromNode = entityNode(from)
    const toNode = entityNode(to)
    const live = await this.neighbors(fromNode.id, "out", relation, 1)
    const now = this.nowMicros()
    const superseded = live.edges
      .filter((edge) => !edge.to.equals(toNode.id) && edge.validTo === undefined)
      .map((edge) => ({ ...edge, validTo: now }))
    if (superseded.length > 0) await this.upsert([], superseded)
    await this.link(from, relation, to)
    return superseded.length
  }

  /** Closes a relationship at the current valid time without deleting its nodes. */
  async unlink(from: string, relation: string, to: string): Promise<void> {
    const fromNode = entityNode(from)
    const toNode = entityNode(to)
    const edge = { ...graphEdgeRelate(fromNode, relation, toNode), validTo: this.nowMicros() }
    await this.upsert([], [edge])
  }

  /** Idempotently upserts content-addressed nodes and edges. */
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

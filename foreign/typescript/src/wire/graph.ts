import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, encodeNamed, expectMap, field, singleVariantTag } from "./cbor.js"
import { GRAPH_OP_VERSION } from "./codes.js"
import { contentId } from "./hashing.js"
import { bytes16ToBigInt, crockfordDecode, WireId } from "./ids.js"
import { MAX_GRAPH_NAME_BYTES } from "./limits.js"
import {
  consistencyToWord,
  decodeFilter,
  encodeFilter,
  parseConsistency,
  type Consistency,
  type Filter
} from "./query.js"
import { decodeValue, encodeValue, type Value } from "./value.js"

export class NodeId extends WireId<"NodeId"> {
  private constructor(value: bigint) {
    super(value)
  }

  static fromU128(value: bigint): NodeId {
    return new NodeId(checkedU128("NodeId", value))
  }

  static fromBytes(bytes: Uint8Array): NodeId {
    return NodeId.fromU128(bytes16ToBigInt(bytes))
  }

  static parse(text: string): NodeId {
    return NodeId.fromU128(crockfordDecode(text))
  }

  static content(label: string, value: Uint8Array): NodeId {
    const labelBytes = new TextEncoder().encode(label)
    return NodeId.fromU128(contentId([labelBytes, new Uint8Array([0]), value]))
  }
}

export class EdgeId extends WireId<"EdgeId"> {
  private constructor(value: bigint) {
    super(value)
  }

  static fromU128(value: bigint): EdgeId {
    return new EdgeId(checkedU128("EdgeId", value))
  }

  static fromBytes(bytes: Uint8Array): EdgeId {
    return EdgeId.fromU128(bytes16ToBigInt(bytes))
  }

  static parse(text: string): EdgeId {
    return EdgeId.fromU128(crockfordDecode(text))
  }

  static content(from: NodeId, edgeType: string, to: NodeId): EdgeId {
    return EdgeId.fromU128(
      contentId([
        from.toBytes(),
        new Uint8Array([0]),
        new TextEncoder().encode(edgeType),
        new Uint8Array([0]),
        to.toBytes()
      ])
    )
  }
}

function checkedU128(name: string, value: bigint): bigint {
  if (value < 0n || value > (1n << 128n) - 1n) {
    throw new InvalidError(`${name} value must fit in 128 bits`, { value: value.toString() })
  }
  return value
}

export type EdgeDir = "out" | "in" | "both"

function parseEdgeDir(word: string, context: string): EdgeDir {
  if (word !== "out" && word !== "in" && word !== "both") {
    throw new CodecError(`\`${word}\` is not a recognized edge direction`, context, "dir")
  }
  return word
}

export type GraphReturn = "nodes" | "edges" | "paths" | "triplets"

function parseGraphReturn(word: string, context: string): GraphReturn {
  if (word !== "nodes" && word !== "edges" && word !== "paths" && word !== "triplets") {
    throw new CodecError(`\`${word}\` is not a recognized graph return type`, context, "return")
  }
  return word
}

export interface Hop {
  readonly edgeType?: string
  readonly dir: EdgeDir
  readonly max: number
}

export function encodeHop(hop: Hop): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (hop.edgeType !== undefined) map.set("edge_type", hop.edgeType)
  if (hop.dir !== "out") map.set("dir", hop.dir)
  map.set("max", BigInt(hop.max))
  return map
}

export function decodeHop(map: CborMap, context: string): Hop {
  const edgeType = field.optionalString(map, "edge_type", context)
  const dir = field.optionalString(map, "dir", context)
  return {
    ...(edgeType !== undefined ? { edgeType } : {}),
    dir: dir !== undefined ? parseEdgeDir(dir, context) : "out",
    max: field.requiredU32(map, "max", context)
  }
}

export type GraphStart =
  | { readonly kind: "ids"; readonly ids: readonly NodeId[] }
  | { readonly kind: "match"; readonly filter: Filter }
  | { readonly kind: "nearest"; readonly embedding: readonly number[]; readonly k: number }

export function encodeGraphStart(start: GraphStart): Map<string, unknown> {
  switch (start.kind) {
    case "ids":
      return new Map([["Ids", start.ids.map((id) => id.toBytes())]])
    case "match":
      return new Map([["Match", encodeFilter(start.filter)]])
    case "nearest":
      return new Map([
        [
          "Nearest",
          new Map<string, unknown>([
            ["embedding", [...start.embedding]],
            ["k", BigInt(start.k)]
          ])
        ]
      ])
  }
}

export function decodeGraphStart(value: unknown, context: string): GraphStart {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Ids": {
      if (!Array.isArray(inner)) {
        throw new CodecError(`expected an array in ${context}.Ids`, context, "start")
      }
      return {
        kind: "ids",
        ids: inner.map((item, index) =>
          NodeId.fromBytes(expectBytes(item, `${context}.Ids[${String(index)}]`))
        )
      }
    }
    case "Match":
      return { kind: "match", filter: decodeFilter(inner, `${context}.Match`) }
    case "Nearest": {
      const map = expectMap(inner, `${context}.Nearest`)
      return {
        kind: "nearest",
        embedding: field.requiredArray(map, "embedding", context, (item, index) =>
          expectNumber(item, `${context}.Nearest.embedding[${String(index)}]`)
        ),
        k: field.requiredU32(map, "k", context)
      }
    }
    default:
      throw new CodecError(`\`${tag}\` is not a recognized graph start variant`, context, "start")
  }
}

export interface GraphQuery {
  readonly graph: string
  readonly start: GraphStart
  readonly traverse: readonly Hop[]
  readonly nodeFilter?: Filter
  readonly edgeFilter?: Filter
  readonly return: GraphReturn
  readonly limit: number
  readonly fork?: string
  readonly consistency: Consistency
  readonly asOf?: bigint
  readonly conversation?: string
}

export function encodeGraphQuery(query: GraphQuery): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", BigInt(GRAPH_OP_VERSION))
  map.set("graph", query.graph)
  map.set("start", encodeGraphStart(query.start))
  if (query.traverse.length > 0)
    map.set(
      "traverse",
      query.traverse.map((hop) => encodeHop(hop))
    )
  if (query.nodeFilter !== undefined) map.set("node_filter", encodeFilter(query.nodeFilter))
  if (query.edgeFilter !== undefined) map.set("edge_filter", encodeFilter(query.edgeFilter))
  if (query.return !== "nodes") map.set("return_", query.return)
  map.set("limit", BigInt(query.limit))
  if (query.fork !== undefined) map.set("fork", query.fork)
  if (query.consistency !== "eventual") map.set("consistency", consistencyToWord(query.consistency))
  if (query.asOf !== undefined) map.set("as_of", query.asOf)
  if (query.conversation !== undefined) map.set("conversation", query.conversation)
  return map
}

export function decodeGraphQuery(map: CborMap, context: string): GraphQuery {
  field.requiredU32(map, "v", context)
  const nodeFilter = map.get("node_filter")
  const edgeFilter = map.get("edge_filter")
  const returnWord = field.optionalString(map, "return_", context)
  const fork = field.optionalString(map, "fork", context)
  const consistency = field.optionalString(map, "consistency", context)
  const asOf = field.optionalU64(map, "as_of", context)
  const conversation = field.optionalString(map, "conversation", context)
  return {
    graph: field.requiredString(map, "graph", context),
    start: decodeGraphStart(field.requiredMap(map, "start", context), `${context}.start`),
    traverse: field.optionalArray(map, "traverse", context, (item, index) =>
      decodeHop(
        expectMap(item, `${context}.traverse[${String(index)}]`),
        `${context}.traverse[${String(index)}]`
      )
    ),
    ...(nodeFilter !== undefined
      ? { nodeFilter: decodeFilter(nodeFilter, `${context}.node_filter`) }
      : {}),
    ...(edgeFilter !== undefined
      ? { edgeFilter: decodeFilter(edgeFilter, `${context}.edge_filter`) }
      : {}),
    return: returnWord !== undefined ? parseGraphReturn(returnWord, context) : "nodes",
    limit: field.requiredU32(map, "limit", context),
    ...(fork !== undefined ? { fork } : {}),
    consistency: consistency !== undefined ? parseConsistency(consistency, context) : "eventual",
    ...(asOf !== undefined ? { asOf } : {}),
    ...(conversation !== undefined ? { conversation } : {})
  }
}

export function encodeGraphQueryFrame(query: GraphQuery): Uint8Array {
  return encodeNamed(encodeGraphQuery(query), { forceFloatNumbers: true })
}

export interface GraphNeighbors {
  readonly graph: string
  readonly node: NodeId
  readonly dir: EdgeDir
  readonly edgeType?: string
  readonly depth: number
  readonly limit: number
  readonly asOf?: bigint
  readonly conversation?: string
}

export function encodeGraphNeighbors(neighbors: GraphNeighbors): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", GRAPH_OP_VERSION)
  map.set("graph", neighbors.graph)
  map.set("node", neighbors.node.toBytes())
  if (neighbors.dir !== "out") map.set("dir", neighbors.dir)
  if (neighbors.edgeType !== undefined) map.set("edge_type", neighbors.edgeType)
  map.set("depth", neighbors.depth)
  map.set("limit", neighbors.limit)
  if (neighbors.asOf !== undefined) map.set("as_of", neighbors.asOf)
  if (neighbors.conversation !== undefined) map.set("conversation", neighbors.conversation)
  return map
}

export function decodeGraphNeighbors(map: CborMap, context: string): GraphNeighbors {
  const dirWord = field.optionalString(map, "dir", context)
  const edgeType = field.optionalString(map, "edge_type", context)
  const asOf = field.optionalU64(map, "as_of", context)
  const conversation = field.optionalString(map, "conversation", context)
  return {
    graph: field.requiredString(map, "graph", context),
    node: NodeId.fromBytes(field.requiredBytes(map, "node", context)),
    dir: dirWord !== undefined ? parseEdgeDir(dirWord, context) : "out",
    ...(edgeType !== undefined ? { edgeType } : {}),
    depth: field.requiredU32(map, "depth", context),
    limit: field.requiredU32(map, "limit", context),
    ...(asOf !== undefined ? { asOf } : {}),
    ...(conversation !== undefined ? { conversation } : {})
  }
}

export type SourceRef =
  | {
      readonly kind: "message"
      readonly stream: number
      readonly topic: number
      readonly partition: number
      readonly offset: bigint
      readonly conversation?: string
    }
  | { readonly kind: "kv"; readonly namespace: string; readonly key: string }
  | { readonly kind: "memory"; readonly id: string }

export function encodeSourceRef(source: SourceRef): Map<string, unknown> {
  switch (source.kind) {
    case "message": {
      const inner = new Map<string, unknown>([
        ["stream", BigInt(source.stream)],
        ["topic", BigInt(source.topic)],
        ["partition", BigInt(source.partition)],
        ["offset", source.offset]
      ])
      if (source.conversation !== undefined) inner.set("conversation", source.conversation)
      return new Map([["Message", inner]])
    }
    case "kv":
      return new Map([
        [
          "Kv",
          new Map<string, unknown>([
            ["namespace", source.namespace],
            ["key", source.key]
          ])
        ]
      ])
    case "memory":
      return new Map([["Memory", new Map<string, unknown>([["id", source.id]])]])
  }
}

export function decodeSourceRef(value: unknown, context: string): SourceRef {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Message": {
      const innerMap = expectMap(inner, context)
      const conversation = field.optionalString(innerMap, "conversation", context)
      return {
        kind: "message",
        stream: field.requiredU32(innerMap, "stream", context),
        topic: field.requiredU32(innerMap, "topic", context),
        partition: field.requiredU32(innerMap, "partition", context),
        offset: field.requiredU64(innerMap, "offset", context),
        ...(conversation !== undefined ? { conversation } : {})
      }
    }
    case "Kv": {
      const innerMap = expectMap(inner, context)
      return {
        kind: "kv",
        namespace: field.requiredString(innerMap, "namespace", context),
        key: field.requiredString(innerMap, "key", context)
      }
    }
    case "Memory": {
      const innerMap = expectMap(inner, context)
      return { kind: "memory", id: field.requiredString(innerMap, "id", context) }
    }
    default:
      throw new CodecError(`\`${tag}\` is not a recognized source ref variant`, context, "source")
  }
}

export type GraphAttr = readonly [string, Value]

function encodeAttrs(attrs: readonly GraphAttr[]): unknown[] {
  return attrs.map(([key, value]) => [key, encodeValue(value)])
}

function decodeAttrs(value: unknown, context: string): GraphAttr[] {
  if (!Array.isArray(value)) {
    throw new CodecError(`expected an array in ${context}`, context, "attrs")
  }
  return value.map((pair, index) => {
    if (!Array.isArray(pair) || pair.length !== 2) {
      throw new CodecError(
        `expected a [key, value] pair in ${context}[${String(index)}]`,
        context,
        "attrs"
      )
    }
    const [key, raw] = pair as [unknown, unknown]
    if (typeof key !== "string") {
      throw new CodecError(
        `attribute key in ${context}[${String(index)}] must be a string`,
        context,
        "attrs"
      )
    }
    return [key, decodeValue(raw, `${context}[${String(index)}]`)] as GraphAttr
  })
}

export interface GraphNode {
  readonly id: NodeId
  readonly labels: readonly string[]
  readonly attrs: readonly GraphAttr[]
  readonly embedding?: readonly number[]
  readonly source?: SourceRef
}

export function graphNodeEntity(label: string, value: string): GraphNode {
  const id = NodeId.content(label, new TextEncoder().encode(value))
  return { id, labels: [label], attrs: [["value", { kind: "string", value }]] }
}

export function encodeGraphNode(node: GraphNode): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("id", node.id.toBytes())
  if (node.labels.length > 0) map.set("labels", [...node.labels])
  if (node.attrs.length > 0) map.set("attrs", encodeAttrs(node.attrs))
  if (node.embedding !== undefined) map.set("embedding", [...node.embedding])
  if (node.source !== undefined) map.set("source", encodeSourceRef(node.source))
  return map
}

export function decodeGraphNode(map: CborMap, context: string): GraphNode {
  const attrsValue = map.get("attrs")
  const embedding = field.optionalArray(map, "embedding", context, (item, index) =>
    expectNumber(item, `${context}.embedding[${String(index)}]`)
  )
  return {
    id: NodeId.fromBytes(field.requiredBytes(map, "id", context)),
    labels: field.optionalArray(map, "labels", context, (item, index) =>
      expectString(item, `${context}.labels[${String(index)}]`)
    ),
    attrs: attrsValue !== undefined ? decodeAttrs(attrsValue, `${context}.attrs`) : [],
    ...(embedding.length > 0 ? { embedding } : {}),
    ...(map.has("source")
      ? { source: decodeSourceRef(map.get("source"), `${context}.source`) }
      : {})
  }
}

export function encodeGraphNodeFrame(node: GraphNode): Uint8Array {
  return encodeNamed(encodeGraphNode(node), { forceFloatNumbers: true })
}

function expectNumber(value: unknown, context: string): number {
  if (typeof value !== "number") {
    throw new CodecError(`expected a number in ${context}`, context, "value")
  }
  return value
}

function expectString(value: unknown, context: string): string {
  if (typeof value !== "string") {
    throw new CodecError(`expected a string in ${context}`, context, "value")
  }
  return value
}

export interface GraphEdge {
  readonly id: EdgeId
  readonly from: NodeId
  readonly to: NodeId
  readonly edgeType: string
  readonly weight: number
  readonly attrs: readonly GraphAttr[]
  readonly validFrom?: bigint
  readonly validTo?: bigint
  readonly source?: SourceRef
}

export function graphEdgeRelate(from: GraphNode, edgeType: string, to: GraphNode): GraphEdge {
  return {
    id: EdgeId.content(from.id, edgeType, to.id),
    from: from.id,
    to: to.id,
    edgeType,
    weight: 1,
    attrs: []
  }
}

export function graphEdgeValidAt(edge: GraphEdge, at: bigint): boolean {
  return (
    (edge.validFrom === undefined || at >= edge.validFrom) &&
    (edge.validTo === undefined || at < edge.validTo)
  )
}

export function encodeGraphEdge(edge: GraphEdge): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("id", edge.id.toBytes())
  map.set("from", edge.from.toBytes())
  map.set("to", edge.to.toBytes())
  map.set("edge_type", edge.edgeType)
  map.set("weight", edge.weight)
  if (edge.attrs.length > 0) map.set("attrs", encodeAttrs(edge.attrs))
  if (edge.validFrom !== undefined) map.set("valid_from", edge.validFrom)
  if (edge.validTo !== undefined) map.set("valid_to", edge.validTo)
  if (edge.source !== undefined) map.set("source", encodeSourceRef(edge.source))
  return map
}

export function decodeGraphEdge(map: CborMap, context: string): GraphEdge {
  const attrsValue = map.get("attrs")
  const validFrom = field.optionalU64(map, "valid_from", context)
  const validTo = field.optionalU64(map, "valid_to", context)
  return {
    id: EdgeId.fromBytes(field.requiredBytes(map, "id", context)),
    from: NodeId.fromBytes(field.requiredBytes(map, "from", context)),
    to: NodeId.fromBytes(field.requiredBytes(map, "to", context)),
    edgeType: field.requiredString(map, "edge_type", context),
    weight: expectNumber(map.get("weight"), `${context}.weight`),
    attrs: attrsValue !== undefined ? decodeAttrs(attrsValue, `${context}.attrs`) : [],
    ...(validFrom !== undefined ? { validFrom } : {}),
    ...(validTo !== undefined ? { validTo } : {}),
    ...(map.has("source")
      ? { source: decodeSourceRef(map.get("source"), `${context}.source`) }
      : {})
  }
}

export function encodeGraphEdgeFrame(edge: GraphEdge): Uint8Array {
  return encodeNamed(encodeGraphEdge(edge), { forceFloatNumbers: true })
}

export interface Path {
  readonly nodes: readonly NodeId[]
  readonly edges: readonly EdgeId[]
}

export function encodePath(path: Path): Map<string, unknown> {
  return new Map<string, unknown>([
    ["nodes", path.nodes.map((id) => id.toBytes())],
    ["edges", path.edges.map((id) => id.toBytes())]
  ])
}

export function decodePath(map: CborMap, context: string): Path {
  return {
    nodes: field.optionalArray(map, "nodes", context, (item, index) =>
      NodeId.fromBytes(expectBytes(item, `${context}.nodes[${String(index)}]`))
    ),
    edges: field.optionalArray(map, "edges", context, (item, index) =>
      EdgeId.fromBytes(expectBytes(item, `${context}.edges[${String(index)}]`))
    )
  }
}

function expectBytes(value: unknown, context: string): Uint8Array {
  if (!(value instanceof Uint8Array)) {
    throw new CodecError(`expected bytes in ${context}`, context, "value")
  }
  return value
}

export interface GraphResult {
  readonly nodes: readonly GraphNode[]
  readonly edges: readonly GraphEdge[]
  readonly paths: readonly Path[]
}

export function encodeGraphResult(result: GraphResult): Map<string, unknown> {
  const map = new Map<string, unknown>()
  if (result.nodes.length > 0)
    map.set(
      "nodes",
      result.nodes.map((node) => encodeGraphNode(node))
    )
  if (result.edges.length > 0)
    map.set(
      "edges",
      result.edges.map((edge) => encodeGraphEdge(edge))
    )
  if (result.paths.length > 0)
    map.set(
      "paths",
      result.paths.map((path) => encodePath(path))
    )
  return map
}

export function decodeGraphResult(map: CborMap, context: string): GraphResult {
  return {
    nodes: field.optionalArray(map, "nodes", context, (item, index) =>
      decodeGraphNode(
        expectMap(item, `${context}.nodes[${String(index)}]`),
        `${context}.nodes[${String(index)}]`
      )
    ),
    edges: field.optionalArray(map, "edges", context, (item, index) =>
      decodeGraphEdge(
        expectMap(item, `${context}.edges[${String(index)}]`),
        `${context}.edges[${String(index)}]`
      )
    ),
    paths: field.optionalArray(map, "paths", context, (item, index) =>
      decodePath(
        expectMap(item, `${context}.paths[${String(index)}]`),
        `${context}.paths[${String(index)}]`
      )
    )
  }
}

export interface GraphUpsert {
  readonly graph: string
  readonly nodes: readonly GraphNode[]
  readonly edges: readonly GraphEdge[]
}

export function encodeGraphUpsert(upsert: GraphUpsert): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("v", BigInt(GRAPH_OP_VERSION))
  map.set("graph", upsert.graph)
  if (upsert.nodes.length > 0)
    map.set(
      "nodes",
      upsert.nodes.map((node) => encodeGraphNode(node))
    )
  if (upsert.edges.length > 0)
    map.set(
      "edges",
      upsert.edges.map((edge) => encodeGraphEdge(edge))
    )
  return map
}

export function decodeGraphUpsert(map: CborMap, context: string): GraphUpsert {
  return {
    graph: field.requiredString(map, "graph", context),
    nodes: field.optionalArray(map, "nodes", context, (item, index) =>
      decodeGraphNode(
        expectMap(item, `${context}.nodes[${String(index)}]`),
        `${context}.nodes[${String(index)}]`
      )
    ),
    edges: field.optionalArray(map, "edges", context, (item, index) =>
      decodeGraphEdge(
        expectMap(item, `${context}.edges[${String(index)}]`),
        `${context}.edges[${String(index)}]`
      )
    )
  }
}

export function encodeGraphUpsertFrame(upsert: GraphUpsert): Uint8Array {
  return encodeNamed(encodeGraphUpsert(upsert), { forceFloatNumbers: true })
}

export type GraphError =
  | { readonly kind: "unsupported"; readonly message: string }
  | { readonly kind: "unauthorized"; readonly message: string }
  | { readonly kind: "invalidName"; readonly message: string }
  | { readonly kind: "notFound"; readonly message: string }
  | {
      readonly kind: "tooLarge"
      readonly what: string
      readonly size: number
      readonly cap: number
    }
  | { readonly kind: "backend"; readonly message: string }
  | { readonly kind: "version"; readonly expected: number; readonly got: number }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeGraphError(error: GraphError): unknown {
  switch (error.kind) {
    case "unsupported":
      return new Map([["Unsupported", error.message]])
    case "unauthorized":
      return new Map([["Unauthorized", error.message]])
    case "invalidName":
      return new Map([["InvalidName", error.message]])
    case "notFound":
      return new Map([["NotFound", error.message]])
    case "tooLarge":
      return new Map([
        [
          "TooLarge",
          new Map<string, unknown>([
            ["what", error.what],
            ["size", BigInt(error.size)],
            ["cap", BigInt(error.cap)]
          ])
        ]
      ])
    case "backend":
      return new Map([["Backend", error.message]])
    case "version":
      return new Map([
        [
          "Version",
          new Map<string, unknown>([
            ["expected", BigInt(error.expected)],
            ["got", BigInt(error.got)]
          ])
        ]
      ])
    case "unrecognized":
      return new Map([[error.tag, error.value]])
  }
}

export function decodeGraphError(value: unknown, context: string): GraphError {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Unsupported":
      return { kind: "unsupported", message: expectString(inner, context) }
    case "Unauthorized":
      return { kind: "unauthorized", message: expectString(inner, context) }
    case "InvalidName":
      return { kind: "invalidName", message: expectString(inner, context) }
    case "NotFound":
      return { kind: "notFound", message: expectString(inner, context) }
    case "TooLarge": {
      const tooLargeMap = expectMap(inner, context)
      return {
        kind: "tooLarge",
        what: field.requiredString(tooLargeMap, "what", context),
        size: field.requiredU32(tooLargeMap, "size", context),
        cap: field.requiredU32(tooLargeMap, "cap", context)
      }
    }
    case "Backend":
      return { kind: "backend", message: expectString(inner, context) }
    case "Version": {
      const versionMap = expectMap(inner, context)
      return {
        kind: "version",
        expected: field.requiredU32(versionMap, "expected", context),
        got: field.requiredU32(versionMap, "got", context)
      }
    }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export type GraphReply =
  | { readonly kind: "ok"; readonly result: GraphResult }
  | { readonly kind: "err"; readonly error: GraphError }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export function encodeGraphReply(reply: GraphReply): Map<string, unknown> {
  switch (reply.kind) {
    case "ok":
      return new Map([["Ok", encodeGraphResult(reply.result)]])
    case "err":
      return new Map([["Err", encodeGraphError(reply.error)]])
    case "unrecognized":
      return new Map([[reply.tag, reply.value]])
  }
}

export function decodeGraphReply(value: unknown, context: string): GraphReply {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Ok":
      return { kind: "ok", result: decodeGraphResult(expectMap(inner, context), context) }
    case "Err":
      return { kind: "err", error: decodeGraphError(inner, context) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export function encodeGraphReplyFrame(reply: GraphReply): Uint8Array {
  return encodeNamed(encodeGraphReply(reply), { forceFloatNumbers: true })
}

export function validateGraphName(name: string): void {
  if (name.length === 0) {
    throw new InvalidError("graph name must not be empty")
  }
  const bytes = new TextEncoder().encode(name)
  if (bytes.length > MAX_GRAPH_NAME_BYTES) {
    throw new InvalidError(
      `graph name is ${String(bytes.length)}B, exceeds cap ${String(MAX_GRAPH_NAME_BYTES)}B`
    )
  }
  for (const byte of bytes) {
    if (byte < 0x20 || byte === 0x7f) {
      throw new InvalidError("graph name must not contain ASCII control characters")
    }
  }
}

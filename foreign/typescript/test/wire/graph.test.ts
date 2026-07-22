import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  decodeGraphEdge,
  decodeGraphNeighbors,
  decodeGraphNode,
  decodeGraphQuery,
  decodeGraphReply,
  decodeGraphUpsert,
  encodeGraphEdgeFrame,
  encodeGraphNeighbors,
  encodeGraphNodeFrame,
  encodeGraphQueryFrame,
  encodeGraphReplyFrame,
  encodeGraphUpsertFrame,
  EdgeId,
  graphEdgeRelate,
  graphEdgeValidAt,
  graphNodeEntity,
  NodeId,
  validateGraphName
} from "../../src/wire/graph.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_the_same_entity_when_addressed_twice_then_should_converge_on_one_node_id", () => {
  const a = NodeId.content("Person", new TextEncoder().encode("Alice"))
  const b = NodeId.content("Person", new TextEncoder().encode("Alice"))
  assert.equal(a.equals(b), true)
  assert.equal(a.equals(NodeId.content("Company", new TextEncoder().encode("Alice"))), false)
})

void test("given_the_pinned_entity_when_addressed_then_should_match_the_golden_id", () => {
  // The cross-SDK golden vector: the Person entity "Alice".
  assert.equal(
    NodeId.content("Person", new TextEncoder().encode("Alice")).toString(),
    "13NCEPHNVFHHGNK9GD3MT0W1AB"
  )
})

void test("given_two_nodes_when_related_then_should_content_address_the_edge", () => {
  const alice = graphNodeEntity("Person", "Alice")
  const acme = graphNodeEntity("Company", "Acme")
  const one = graphEdgeRelate(alice, "works_at", acme)
  const two = graphEdgeRelate(alice, "works_at", acme)
  assert.equal(one.id.equals(two.id), true)
  const reverse = graphEdgeRelate(acme, "works_at", alice)
  assert.equal(one.id.equals(reverse.id), false)
})

void test("given_an_edge_validity_window_when_checked_then_should_hold_only_inside_it", () => {
  const alice = graphNodeEntity("User", "alice")
  const pro = graphNodeEntity("Plan", "pro")
  const edge = { ...graphEdgeRelate(alice, "on_plan", pro), validFrom: 100n, validTo: 200n }
  assert.equal(graphEdgeValidAt(edge, 99n), false)
  assert.equal(graphEdgeValidAt(edge, 100n), true)
  assert.equal(graphEdgeValidAt(edge, 150n), true)
  assert.equal(graphEdgeValidAt(edge, 200n), false)
  const open = graphEdgeRelate(alice, "on_plan", pro)
  assert.equal(graphEdgeValidAt(open, 0n) && graphEdgeValidAt(open, (1n << 64n) - 1n), true)
})

void test("given_the_graph_node_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("graph_node.bin")
  const map = expectMap(decodeOne(bytes, "graph_node"), "graph_node")
  const node = decodeGraphNode(map, "graph_node")
  assert.deepEqual(node.labels, ["Doc"])
  assert.deepEqual(node.attrs, [["value", { kind: "string", value: "spec" }]])
  assert.deepEqual(node.embedding, [0.10000000149011612, 0.20000000298023224, 0.30000001192092896])
  const reencoded = encodeGraphNodeFrame(node)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_graph_node_sourced_fixture_when_decoded_then_should_preserve_the_source_and_identity", async () => {
  const bytes = await readFixture("graph_node_sourced.bin")
  const map = expectMap(decodeOne(bytes, "graph_node_sourced"), "graph_node_sourced")
  const node = decodeGraphNode(map, "graph_node_sourced")
  assert.deepEqual(node.source, {
    kind: "message",
    stream: 7,
    topic: 2,
    partition: 3,
    offset: 4096n
  })
  assert.equal(
    node.id.equals(graphNodeEntity("Component", "cache").id),
    true,
    "source is not part of node identity"
  )
  const reencoded = encodeGraphNodeFrame(node)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_graph_node_conversation_fixture_when_decoded_then_should_preserve_the_conversation", async () => {
  const bytes = await readFixture("graph_node_conversation.bin")
  const map = expectMap(decodeOne(bytes, "graph_node_conversation"), "graph_node_conversation")
  const node = decodeGraphNode(map, "graph_node_conversation")
  if (node.source?.kind !== "message") throw new Error("wrong shape")
  assert.equal(node.source.conversation, "7ZZZZZZZZZZZZZZZZZZZZZZZZZ")
  const reencoded = encodeGraphNodeFrame(node)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_graph_edge_fixture_when_decoded_then_should_preserve_weight_and_validity", async () => {
  const bytes = await readFixture("graph_edge.bin")
  const map = expectMap(decodeOne(bytes, "graph_edge"), "graph_edge")
  const edge = decodeGraphEdge(map, "graph_edge")
  assert.equal(edge.weight, 1)
  assert.equal(edge.validFrom, 1000n)
  assert.equal(edge.validTo, 2000n)
  const reencoded = encodeGraphEdgeFrame(edge)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_graph_edge_sourced_fixture_when_decoded_then_should_preserve_the_source_and_identity", async () => {
  const bytes = await readFixture("graph_edge_sourced.bin")
  const map = expectMap(decodeOne(bytes, "graph_edge_sourced"), "graph_edge_sourced")
  const edge = decodeGraphEdge(map, "graph_edge_sourced")
  const alice = graphNodeEntity("Person", "Alice")
  const acme = graphNodeEntity("Company", "Acme")
  assert.equal(
    edge.id.equals(graphEdgeRelate(alice, "works_at", acme).id),
    true,
    "source is not part of edge identity"
  )
  const reencoded = encodeGraphEdgeFrame(edge)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_graph_edge_conversation_fixture_when_decoded_then_should_preserve_the_conversation", async () => {
  const bytes = await readFixture("graph_edge_conversation.bin")
  const map = expectMap(decodeOne(bytes, "graph_edge_conversation"), "graph_edge_conversation")
  const edge = decodeGraphEdge(map, "graph_edge_conversation")
  if (edge.source?.kind !== "message") throw new Error("wrong shape")
  assert.equal(edge.source.conversation, "7ZZZZZZZZZZZZZZZZZZZZZZZZZ")
  const reencoded = encodeGraphEdgeFrame(edge)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_graph_neighbors_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("graph_neighbors.bin")
  const map = expectMap(decodeOne(bytes, "graph_neighbors"), "graph_neighbors")
  const neighbors = decodeGraphNeighbors(map, "graph_neighbors")
  assert.equal(neighbors.graph, "knowledge")
  assert.equal(neighbors.edgeType, "works_at")
  assert.equal(neighbors.asOf, 1500n)
  const reencoded = encodeNamed(encodeGraphNeighbors(neighbors))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_graph_neighbors_conversation_fixture_when_decoded_then_should_preserve_the_conversation", async () => {
  const bytes = await readFixture("graph_neighbors_conversation.bin")
  const map = expectMap(
    decodeOne(bytes, "graph_neighbors_conversation"),
    "graph_neighbors_conversation"
  )
  const neighbors = decodeGraphNeighbors(map, "graph_neighbors_conversation")
  assert.equal(neighbors.conversation, "7ZZZZZZZZZZZZZZZZZZZZZZZZZ")
  const reencoded = encodeNamed(encodeGraphNeighbors(neighbors))
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_graph_reply_fixture_when_decoded_then_should_preserve_nodes_edges_and_paths", async () => {
  const bytes = await readFixture("graph_reply.bin")
  const reply = decodeGraphReply(decodeOne(bytes, "graph_reply"), "graph_reply")
  if (reply.kind !== "ok") throw new Error("wrong shape")
  assert.equal(reply.result.nodes.length, 2)
  assert.equal(reply.result.edges.length, 1)
  assert.equal(reply.result.edges[0]?.edgeType, "works_at")
  assert.equal(reply.result.paths.length, 1)
  const reencoded = encodeGraphReplyFrame(reply)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_graph_upsert_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("graph_upsert.bin")
  const map = expectMap(decodeOne(bytes, "graph_upsert"), "graph_upsert")
  const upsert = decodeGraphUpsert(map, "graph_upsert")
  assert.equal(upsert.graph, "knowledge")
  assert.equal(upsert.nodes.length, 2)
  assert.equal(upsert.edges.length, 1)
  const reencoded = encodeGraphUpsertFrame(upsert)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_graph_query_fixture_when_decoded_then_should_preserve_the_traversal", async () => {
  const bytes = await readFixture("graph_query.bin")
  const map = expectMap(decodeOne(bytes, "graph_query"), "graph_query")
  const query = decodeGraphQuery(map, "graph_query")
  assert.equal(query.graph, "knowledge")
  assert.equal(query.start.kind, "match")
  assert.deepEqual(query.traverse, [{ edgeType: "works_at", dir: "out", max: 2 }])
  assert.equal(query.return, "paths")
  assert.equal(query.limit, 100)
  assert.equal(query.consistency, "eventual")
  assert.equal(query.asOf, 1500n)
  assert.deepEqual(Buffer.from(encodeGraphQueryFrame(query)), Buffer.from(bytes))
})

void test("given_a_conversation_graph_query_when_decoded_then_should_preserve_the_lens", async () => {
  const bytes = await readFixture("graph_query_conversation.bin")
  const map = expectMap(decodeOne(bytes, "graph_query_conversation"), "graph_query_conversation")
  const query = decodeGraphQuery(map, "graph_query_conversation")
  assert.equal(query.return, "nodes")
  assert.equal(query.conversation, "7ZZZZZZZZZZZZZZZZZZZZZZZZZ")
  assert.deepEqual(Buffer.from(encodeGraphQueryFrame(query)), Buffer.from(bytes))
})

void test("given_a_whole_number_nearest_embedding_when_encoded_then_should_keep_float_major_types", () => {
  const bytes = encodeGraphQueryFrame({
    graph: "knowledge",
    start: { kind: "nearest", embedding: [1, 2], k: 5 },
    traverse: [],
    return: "nodes",
    limit: 10,
    consistency: "eventual"
  })
  const query = decodeGraphQuery(
    expectMap(decodeOne(bytes, "nearest_graph_query"), "nearest_graph_query"),
    "nearest_graph_query"
  )
  assert.deepEqual(query.start, { kind: "nearest", embedding: [1, 2], k: 5 })
  assert.deepEqual(Buffer.from(encodeGraphQueryFrame(query)), Buffer.from(bytes))
  assert.equal(Buffer.from(bytes).includes(Buffer.from([0xf9, 0x3c, 0x00])), true)
})

void test("given_graph_names_when_validated_then_should_enforce_bounds", () => {
  assert.doesNotThrow(() => {
    validateGraphName("knowledge")
  })
  assert.throws(() => {
    validateGraphName("")
  })
  assert.throws(() => {
    validateGraphName("bad\tname")
  })
})

void test("given_an_edge_id_when_round_tripped_through_a_string_then_should_be_equal", () => {
  const id = EdgeId.fromU128(987_654_321n)
  const parsed = EdgeId.parse(id.toString())
  assert.equal(parsed.equals(id), true)
})

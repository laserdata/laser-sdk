import assert from "node:assert/strict"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom, OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { GraphExecutionError, InvalidError, UnsupportedError } from "../../src/client/errors.js"
import { GraphHandle } from "../../src/managed/graph.js"
import {
  GraphNeighborsCommand,
  GraphQueryCommand,
  GraphUpsertCommand
} from "../../src/wire/commands.js"
import type { GraphEdge, GraphReply, GraphResult } from "../../src/wire/graph.js"
import {
  encodeGraphReplyFrame,
  encodeGraphUpsertFrame,
  graphEdgeRelate,
  graphEdgeWithSource,
  graphNodeEntity
} from "../../src/wire/graph.js"
import { ConversationId } from "../../src/wire/ids.js"
import { filterPred } from "../../src/wire/query.js"

const CAPS: Capabilities = managedCapabilitiesFrom({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
  backends: []
})

function replyFrame(reply: GraphReply): Uint8Array {
  return encodeGraphReplyFrame(reply)
}

function okFrame(result: GraphResult): Uint8Array {
  return replyFrame({ kind: "ok", result })
}

const EMPTY_RESULT: GraphResult = { nodes: [], edges: [], paths: [] }

function fakeTransport(scriptedReplies: readonly Uint8Array[]): {
  readonly calls: { readonly code: number; readonly payload: Uint8Array }[]
  sendManaged(code: number, payload: Uint8Array): Promise<Uint8Array>
} {
  const calls: { code: number; payload: Uint8Array }[] = []
  let next = 0
  return {
    calls,
    sendManaged(code, payload) {
      calls.push({ code, payload })
      const reply = scriptedReplies[next]
      next += 1
      if (reply === undefined) throw new Error("fake transport ran out of scripted replies")
      return Promise.resolve(reply)
    }
  }
}

function graph(
  name: string,
  replies: readonly Uint8Array[],
  capabilities: Capabilities = CAPS,
  nowMicros: () => bigint = () => 1_000n
) {
  const transport = fakeTransport(replies)
  return {
    graph: GraphHandle.create(transport, () => Promise.resolve(capabilities), name, nowMicros),
    transport
  }
}

void test("given_a_fetch_when_run_then_should_use_the_query_command_and_return_the_result", async () => {
  const { graph: handle, transport } = graph("kg", [okFrame(EMPTY_RESULT)])
  const result = await handle
    .startIds([])
    .out("relates_to")
    .returnEdges()
    .limit(5)
    .asOf(1n)
    .conversation("c-1")
    .fetch()
  assert.deepEqual(result, EMPTY_RESULT)
  assert.equal(transport.calls[0]?.code, GraphQueryCommand.code)
})

void test("given_an_invalid_graph_name_when_fetch_is_called_then_should_reject_before_the_transport", async () => {
  const { graph: handle, transport } = graph("", [])
  await assert.rejects(() => handle.fetch(), InvalidError)
  assert.equal(transport.calls.length, 0)
})

void test("given_open_capabilities_when_fetch_is_called_then_should_reject_before_the_transport", async () => {
  const { graph: handle, transport } = graph("kg", [], OPEN_CAPABILITIES)
  await assert.rejects(() => handle.fetch(), UnsupportedError)
  assert.equal(transport.calls.length, 0)
})

void test("given_a_neighbors_call_when_run_then_should_use_the_neighbors_command", async () => {
  const alice = graphNodeEntity("host", "node-7")
  const { graph: handle, transport } = graph("kg", [okFrame(EMPTY_RESULT)])
  const result = await handle.neighbors(alice.id, "out", "opened", 1)
  assert.deepEqual(result, EMPTY_RESULT)
  assert.equal(transport.calls[0]?.code, GraphNeighborsCommand.code)
})

void test("given_a_link_call_when_run_then_should_upsert_both_nodes_and_the_edge", async () => {
  const { graph: handle, transport } = graph("kg", [okFrame(EMPTY_RESULT)])
  await handle.link("host:node-7", "opened", "ticket:7")
  assert.equal(transport.calls[0]?.code, GraphUpsertCommand.code)
})

void test("given_a_stale_edge_when_relink_is_called_then_should_supersede_it_and_link_the_new_target", async () => {
  const alice = graphNodeEntity("host", "node-7")
  const ticket7 = graphNodeEntity("ticket", "7")
  const staleEdge: GraphEdge = graphEdgeRelate(alice, "opened", ticket7)
  const alreadyClosedEdge: GraphEdge = {
    ...graphEdgeRelate(alice, "opened", ticket7),
    validTo: 1n
  }
  const liveResult: GraphResult = { nodes: [], edges: [staleEdge, alreadyClosedEdge], paths: [] }
  const { graph: handle, transport } = graph("kg", [
    okFrame(liveResult),
    okFrame(EMPTY_RESULT),
    okFrame(EMPTY_RESULT)
  ])
  const closed = await handle.relink("host:node-7", "opened", "ticket:9")
  assert.equal(closed, 1)
  assert.equal(transport.calls.length, 3)
  assert.equal(transport.calls[0]?.code, GraphNeighborsCommand.code)
  assert.equal(transport.calls[1]?.code, GraphUpsertCommand.code)
  assert.equal(transport.calls[2]?.code, GraphUpsertCommand.code)
})

void test("given_an_already_matching_edge_when_relink_is_called_then_should_not_supersede_it", async () => {
  const alice = graphNodeEntity("host", "node-7")
  const ticket7 = graphNodeEntity("ticket", "7")
  const matchingEdge: GraphEdge = graphEdgeRelate(alice, "opened", ticket7)
  const liveResult: GraphResult = { nodes: [], edges: [matchingEdge], paths: [] }
  const { graph: handle } = graph("kg", [okFrame(liveResult), okFrame(EMPTY_RESULT)])
  const closed = await handle.relink("host:node-7", "opened", "ticket:7")
  assert.equal(closed, 0)
})

void test("given_an_unlink_call_when_run_then_should_upsert_a_closed_edge", async () => {
  const { graph: handle, transport } = graph("kg", [okFrame(EMPTY_RESULT)])
  await handle.unlink("host:node-7", "opened", "ticket:7")
  assert.equal(transport.calls[0]?.code, GraphUpsertCommand.code)
})

void test("given_an_err_reply_when_fetch_fails_then_should_wrap_it_as_a_graph_execution_error", async () => {
  const { graph: handle } = graph("kg", [
    replyFrame({ kind: "err", error: { kind: "notFound", message: "no such graph" } })
  ])
  await assert.rejects(() => handle.fetch(), GraphExecutionError)
})

void test("given_session_lineage_when_upserting_then_should_stamp_only_elements_without_their_own", async () => {
  const { graph: handle, transport } = graph("kg", [okFrame(EMPTY_RESULT)])
  const session = { stream: "agents", session: ConversationId.fromU128(9n) }
  const producer = { name: "sdk:planner", version: "0.8.0" }
  const source = { kind: "memory" as const, id: "m-1" }
  const own = { name: "importer", version: "1" }
  const node = graphNodeEntity("host", "node-7")
  const stamped = { ...graphNodeEntity("ticket", "7"), producer: own }
  const edge = graphEdgeRelate(node, "opened", stamped)
  await handle
    .inSession(session)
    .producedBy(producer)
    .sourcedFrom(source)
    .upsert([node, stamped], [edge])
  assert.deepEqual(
    transport.calls[0]?.payload,
    encodeGraphUpsertFrame({
      graph: "kg",
      session,
      nodes: [
        { ...node, producer, source },
        { ...stamped, source }
      ],
      edges: [{ ...edge, producer, source }]
    })
  )
})

void test("given_an_edge_when_a_source_is_attached_then_should_keep_its_id_and_carry_the_source", () => {
  const edge = graphEdgeRelate(
    graphNodeEntity("person", "ada"),
    "knows",
    graphNodeEntity("person", "alan")
  )
  const sourced = graphEdgeWithSource(edge, { kind: "kv", namespace: "people", key: "ada" })
  assert.equal(sourced.id.toString(), edge.id.toString())
  assert.deepEqual(sourced.source, { kind: "kv", namespace: "people", key: "ada" })
  assert.equal(edge.source, undefined)
})

void test("given_a_link_call_when_run_then_should_open_the_edge_at_the_current_valid_time", async () => {
  const { graph: handle, transport } = graph("kg", [okFrame(EMPTY_RESULT)], CAPS, () => 4_200n)
  await handle.link("host:node-7", "opened", "ticket:7")
  const host = graphNodeEntity("host", "node-7")
  const ticket = graphNodeEntity("ticket", "7")
  assert.deepEqual(
    transport.calls[0]?.payload,
    encodeGraphUpsertFrame({
      graph: "kg",
      nodes: [host, ticket],
      edges: [{ ...graphEdgeRelate(host, "opened", ticket), validFrom: 4_200n }]
    })
  )
})

void test("given_a_relink_call_when_run_then_should_close_and_open_at_the_same_instant", async () => {
  const host = graphNodeEntity("host", "node-7")
  const stale = graphEdgeRelate(host, "opened", graphNodeEntity("ticket", "7"))
  let tick = 7_000n
  const { graph: handle, transport } = graph(
    "kg",
    [
      okFrame({ nodes: [], edges: [stale], paths: [] }),
      okFrame(EMPTY_RESULT),
      okFrame(EMPTY_RESULT)
    ],
    CAPS,
    () => (tick += 1n)
  )
  await handle.relink("host:node-7", "opened", "ticket:9")
  const ticket9 = graphNodeEntity("ticket", "9")
  assert.deepEqual(
    transport.calls[1]?.payload,
    encodeGraphUpsertFrame({ graph: "kg", nodes: [], edges: [{ ...stale, validTo: 7_001n }] })
  )
  assert.deepEqual(
    transport.calls[2]?.payload,
    encodeGraphUpsertFrame({
      graph: "kg",
      nodes: [host, ticket9],
      edges: [{ ...graphEdgeRelate(host, "opened", ticket9), validFrom: 7_001n }]
    })
  )
})

void test("given_filters_and_a_multi_step_hop_when_building_then_should_carry_them_in_the_query", () => {
  const { graph: handle } = graph("kg", [])
  const nodeFilter = filterPred("label", "eq", { kind: "string", value: "Person" })
  const edgeFilter = filterPred("edge_type", "eq", { kind: "string", value: "knows" })
  const query = handle
    .startIds([])
    .hop("both", undefined, 3)
    .out("works_at")
    .nodeFilter(nodeFilter)
    .edgeFilter(edgeFilter)
    .limit(7)
    .intoQuery()
  assert.deepEqual(query, {
    v: 1,
    graph: "kg",
    start: { kind: "ids", ids: [] },
    traverse: [
      { dir: "both", max: 3 },
      { edgeType: "works_at", dir: "out", max: 1 }
    ],
    return: "nodes",
    limit: 7,
    nodeFilter,
    edgeFilter
  })
})

void test("given_an_invalid_hop_or_limit_when_fetching_then_should_reject_before_the_transport", async () => {
  for (const configure of [
    (handle: GraphHandle) => handle.hop("out", "knows", 0),
    (handle: GraphHandle) => handle.hop("out", "knows", 9),
    (handle: GraphHandle) => handle.hop("out", "knows", 5).hop("out", "likes", 4),
    (handle: GraphHandle) => handle.limit(Number.NaN),
    (handle: GraphHandle) => handle.limit(-1),
    (handle: GraphHandle) => handle.limit(1.5),
    (handle: GraphHandle) => handle.limit(10_001)
  ]) {
    const { graph: handle, transport } = graph("kg", [])
    await assert.rejects(() => configure(handle).fetch(), InvalidError)
    assert.equal(transport.calls.length, 0)
  }
})

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
import { encodeGraphReplyFrame, graphEdgeRelate, graphNodeEntity } from "../../src/wire/graph.js"

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

function graph(name: string, replies: readonly Uint8Array[], capabilities: Capabilities = CAPS) {
  const transport = fakeTransport(replies)
  return { graph: new GraphHandle(transport, () => Promise.resolve(capabilities), name), transport }
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
  const alice = graphNodeEntity("customer", "alice")
  const { graph: handle, transport } = graph("kg", [okFrame(EMPTY_RESULT)])
  const result = await handle.neighbors(alice.id, "out", "opened", 1)
  assert.deepEqual(result, EMPTY_RESULT)
  assert.equal(transport.calls[0]?.code, GraphNeighborsCommand.code)
})

void test("given_a_link_call_when_run_then_should_upsert_both_nodes_and_the_edge", async () => {
  const { graph: handle, transport } = graph("kg", [okFrame(EMPTY_RESULT)])
  await handle.link("customer:alice", "opened", "ticket:7")
  assert.equal(transport.calls[0]?.code, GraphUpsertCommand.code)
})

void test("given_a_stale_edge_when_relink_is_called_then_should_supersede_it_and_link_the_new_target", async () => {
  const alice = graphNodeEntity("customer", "alice")
  const ticket7 = graphNodeEntity("ticket", "7")
  const ticket9 = graphNodeEntity("ticket", "9")
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
  const closed = await handle.relink("customer:alice", "opened", "ticket:9")
  assert.equal(closed, 1)
  assert.equal(transport.calls.length, 3)
  assert.equal(transport.calls[0]?.code, GraphNeighborsCommand.code)
  assert.equal(transport.calls[1]?.code, GraphUpsertCommand.code)
  assert.equal(transport.calls[2]?.code, GraphUpsertCommand.code)
  void ticket9
})

void test("given_an_already_matching_edge_when_relink_is_called_then_should_not_supersede_it", async () => {
  const alice = graphNodeEntity("customer", "alice")
  const ticket7 = graphNodeEntity("ticket", "7")
  const matchingEdge: GraphEdge = graphEdgeRelate(alice, "opened", ticket7)
  const liveResult: GraphResult = { nodes: [], edges: [matchingEdge], paths: [] }
  const { graph: handle } = graph("kg", [okFrame(liveResult), okFrame(EMPTY_RESULT)])
  const closed = await handle.relink("customer:alice", "opened", "ticket:7")
  assert.equal(closed, 0)
})

void test("given_an_unlink_call_when_run_then_should_upsert_a_closed_edge", async () => {
  const { graph: handle, transport } = graph("kg", [okFrame(EMPTY_RESULT)])
  await handle.unlink("customer:alice", "opened", "ticket:7")
  assert.equal(transport.calls[0]?.code, GraphUpsertCommand.code)
})

void test("given_an_err_reply_when_fetch_fails_then_should_wrap_it_as_a_graph_execution_error", async () => {
  const { graph: handle } = graph("kg", [
    replyFrame({ kind: "err", error: { kind: "notFound", message: "no such graph" } })
  ])
  await assert.rejects(() => handle.fetch(), GraphExecutionError)
})

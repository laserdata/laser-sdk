import assert from "node:assert/strict"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom, OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { InvalidError, QueryExecutionError, UnsupportedError } from "../../src/client/errors.js"
import { Bindings, Projections, Schemas } from "../../src/managed/projections.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import type { BrowseOutcome, BrowseReply } from "../../src/wire/browse.js"
import { encodeBrowseReply } from "../../src/wire/browse.js"
import { ContentType } from "../../src/wire/content.js"
import type { ControlCommand, Projection } from "../../src/wire/control.js"

const CAPS: Capabilities = managedCapabilitiesFrom({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
  backends: []
})

function replyFrame(reply: BrowseReply): Uint8Array {
  const value = encodeBrowseReply(reply)
  return encodeNamed(value)
}

function okFrame(outcome: BrowseOutcome): Uint8Array {
  return replyFrame({ kind: "ok", outcome })
}

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

function fakePublishControl(): {
  readonly calls: ControlCommand[]
  readonly publish: (command: ControlCommand) => Promise<void>
} {
  const calls: ControlCommand[] = []
  const publish = (command: ControlCommand): Promise<void> => {
    calls.push(command)
    return Promise.resolve()
  }
  return { calls, publish }
}

const ROW_PROJECTION: Projection = {
  id: "orders.v1" as Projection["id"],
  name: "orders",
  version: 1,
  kind: { kind: "row" },
  contentType: ContentType.Json,
  extraction: { fields: [], inlinePayload: false },
  inlinePayloadDefault: false
}

const GRAPH_PROJECTION: Projection = {
  ...ROW_PROJECTION,
  id: "orders.graph" as Projection["id"],
  kind: { kind: "graph" },
  entitySchema: { nodes: [], edges: [] }
}

void test("given_a_row_projection_when_register_is_called_then_should_publish_the_control_command", async () => {
  const control = fakePublishControl()
  const projections = new Projections(
    { sendManaged: () => Promise.reject(new Error("unused")) },
    () => Promise.resolve(CAPS),
    control.publish
  )
  await projections.register(ROW_PROJECTION)
  assert.deepEqual(control.calls, [{ kind: "registerProjection", projection: ROW_PROJECTION }])
})

void test("given_a_graph_projection_when_register_is_called_then_should_reject_before_publishing", async () => {
  const control = fakePublishControl()
  const projections = new Projections(
    { sendManaged: () => Promise.reject(new Error("unused")) },
    () => Promise.resolve(CAPS),
    control.publish
  )
  await assert.rejects(() => projections.register(GRAPH_PROJECTION), InvalidError)
  assert.equal(control.calls.length, 0)
})

void test("given_a_non_graph_projection_when_register_graph_is_called_then_should_reject_before_publishing", async () => {
  const control = fakePublishControl()
  const projections = new Projections(
    { sendManaged: () => Promise.reject(new Error("unused")) },
    () => Promise.resolve(CAPS),
    control.publish
  )
  await assert.rejects(() => projections.registerGraph(ROW_PROJECTION), InvalidError)
  assert.equal(control.calls.length, 0)
})

void test("given_a_graph_projection_when_register_graph_is_called_then_should_publish_the_control_command", async () => {
  const control = fakePublishControl()
  const projections = new Projections(
    { sendManaged: () => Promise.reject(new Error("unused")) },
    () => Promise.resolve(CAPS),
    control.publish
  )
  await projections.registerGraph(GRAPH_PROJECTION)
  assert.deepEqual(control.calls, [{ kind: "registerGraph", projection: GRAPH_PROJECTION }])
})

void test("given_an_id_when_drop_and_drop_graph_are_called_then_should_publish_the_right_commands", async () => {
  const control = fakePublishControl()
  const projections = new Projections(
    { sendManaged: () => Promise.reject(new Error("unused")) },
    () => Promise.resolve(CAPS),
    control.publish
  )
  await projections.drop("orders.v1")
  await projections.dropGraph("orders.graph")
  assert.deepEqual(control.calls, [
    { kind: "dropProjection", id: "orders.v1" },
    { kind: "dropGraph", id: "orders.graph" }
  ])
})

void test("given_a_projection_outcome_when_get_is_called_then_should_return_it", async () => {
  const transport = fakeTransport([
    okFrame({ kind: "projection", projection: { projection: ROW_PROJECTION, bindings: [] } })
  ])
  const projections = new Projections(
    transport,
    () => Promise.resolve(CAPS),
    () => Promise.resolve()
  )
  const info = await projections.get("orders.v1")
  assert.deepEqual(info?.projection, ROW_PROJECTION)
})

void test("given_an_absent_projection_outcome_when_get_is_called_then_should_return_undefined", async () => {
  const transport = fakeTransport([okFrame({ kind: "projection" })])
  const projections = new Projections(
    transport,
    () => Promise.resolve(CAPS),
    () => Promise.resolve()
  )
  assert.equal(await projections.get("missing"), undefined)
})

void test("given_filters_when_list_is_fetched_then_should_send_them_and_return_the_matches", async () => {
  const transport = fakeTransport([
    okFrame({ kind: "projections", projections: [{ projection: ROW_PROJECTION, bindings: [] }] })
  ])
  const projections = new Projections(
    transport,
    () => Promise.resolve(CAPS),
    () => Promise.resolve()
  )
  const list = await projections
    .list()
    .forTopic("orders")
    .nameContains("ord")
    .idPrefix("orders")
    .search("ord")
    .fetch()
  assert.equal(list.length, 1)
  assert.equal(transport.calls.length, 1)
})

void test("given_open_capabilities_when_get_is_called_then_should_reject_before_the_transport", async () => {
  const transport = fakeTransport([])
  const projections = new Projections(
    transport,
    () => Promise.resolve(OPEN_CAPABILITIES),
    () => Promise.resolve()
  )
  await assert.rejects(() => projections.get("orders.v1"), UnsupportedError)
  assert.equal(transport.calls.length, 0)
})

void test("given_an_err_reply_when_get_fails_then_should_wrap_it_as_a_query_execution_error", async () => {
  const transport = fakeTransport([
    replyFrame({ kind: "err", error: { kind: "indexNotFound", message: "no such projection" } })
  ])
  const projections = new Projections(
    transport,
    () => Promise.resolve(CAPS),
    () => Promise.resolve()
  )
  await assert.rejects(() => projections.get("missing"), QueryExecutionError)
})

void test("given_a_binding_when_applied_and_removed_then_should_publish_the_right_commands", async () => {
  const control = fakePublishControl()
  const bindings = new Bindings(control.publish)
  const binding = {
    source: { stream: "orders", topic: "events" },
    allowedProjections: [ROW_PROJECTION.id],
    targets: [],
    notify: false
  }
  await bindings.apply(binding)
  await bindings.remove({ stream: "orders", topic: "events" }, "orders.v1")
  assert.deepEqual(control.calls, [
    { kind: "applyBinding", binding },
    {
      kind: "removeBinding",
      source: { stream: "orders", topic: "events" },
      projectionRef: "orders.v1"
    }
  ])
})

void test("given_a_schema_source_when_registered_then_should_return_the_allocated_id", async () => {
  const transport = fakeTransport([okFrame({ kind: "schemaRegistered", id: 7 })])
  const schemas = new Schemas(
    transport,
    () => Promise.resolve(CAPS),
    () => Promise.resolve()
  )
  const id = await schemas
    .register({ kind: "jsonSchema", schema: "{}" })
    .name("orders")
    .version(1)
    .send()
  assert.equal(id, 7)
})

void test("given_a_schema_id_when_dropped_gotten_and_listed_then_should_use_the_right_paths", async () => {
  const control = fakePublishControl()
  const transport = fakeTransport([
    okFrame({
      kind: "schema",
      schema: { schema: { id: 7, source: { kind: "jsonSchema", schema: "{}" } }, dropped: false }
    }),
    okFrame({
      kind: "schemas",
      schemas: [{ schema: { id: 7, source: { kind: "jsonSchema", schema: "{}" } }, dropped: false }]
    })
  ])
  const schemas = new Schemas(transport, () => Promise.resolve(CAPS), control.publish)
  await schemas.drop(7)
  const info = await schemas.get(7)
  const list = await schemas.list()
  assert.deepEqual(control.calls, [{ kind: "dropSchema", id: 7 }])
  assert.equal(info?.schema.id, 7)
  assert.equal(list.length, 1)
})

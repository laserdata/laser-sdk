import assert from "node:assert/strict"
import { test } from "node:test"
import { Laser } from "../../src/client/laser.js"
import { NoStreamError } from "../../src/client/errors.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"

// Accessors are free to construct: no IO happens until a terminal verb, so an
// injected client that never talks to a server is enough to exercise all of them.
function fakeClient(): IggyClient {
  return {
    clientProvider: () => Promise.resolve({ protocol: "vsr" }),
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
}

async function laserWithStream(): Promise<Laser> {
  return Laser.fromIggyClient(fakeClient(), { defaultStream: "fleet" })
}

void test("given_a_connected_client_when_reaching_the_log_then_should_address_streams_and_topics", async () => {
  await using laser = await laserWithStream()

  assert.equal(laser.defaultStream, "fleet")
  assert.equal(laser.stream("audit").name, "audit")
  assert.equal(laser.stream("audit").topic("events").name, "events")
  assert.equal(laser.topic("readings").name, "readings")
  assert.equal(laser.stream("fleet").topic("readings").streamName, "fleet")
})

void test("given_no_default_stream_when_using_the_shortcut_then_should_reject_with_no_stream", async () => {
  await using laser = await Laser.fromIggyClient(fakeClient())

  assert.equal(laser.defaultStream, undefined)
  assert.throws(() => laser.topic("readings"), NoStreamError)
})

void test("given_a_connected_client_when_reaching_the_managed_surfaces_then_should_build_every_handle", async () => {
  await using laser = await laserWithStream()

  assert.equal(laser.kv("profiles").namespace, "profiles")
  assert.equal(laser.fork("experiment-1").forkId, "experiment-1")
  assert.ok(laser.graph("kg"))
  assert.deepEqual(laser.query("readings_v1").intoQuery().target, {
    kind: "operational",
    index: "readings_v1"
  })
  assert.ok(laser.projections())
  assert.ok(laser.bindings())
  assert.ok(laser.schemas())
  assert.ok(laser.runs())
  assert.ok(laser.watch())
  assert.ok(laser.watch().index("readings_v1"))
})

void test("given_a_connected_client_when_reaching_the_fabric_then_should_scope_by_identity", async () => {
  await using laser = await laserWithStream()
  const conversation = ConversationId.new()

  assert.equal(laser.context(conversation).conversation, conversation)
  assert.ok(laser.agent(AgentId.new("triage")))
  assert.ok(laser.workflow("rollback"))
  assert.ok(laser.clientMetadata())
})

void test("given_a_connected_client_when_reaching_memory_then_should_build_each_backend_form", async () => {
  await using laser = await laserWithStream()

  assert.equal(laser.memory("host:node-7").logBackend()?.namespace, "host:node-7")
  assert.equal(laser.memory("host:node-7").logBackend()?.topic, AgentTopic.Audit)
  assert.equal(laser.memoryOnTopic("incidents").logBackend()?.topic, "incidents")
  assert.equal(laser.memoryOnTopic("incidents", "ops").logBackend()?.stream, "ops")

  const configured = laser.memoryTopic("incidents").stream("ops").partitions(4).ttl(86_400_000)
  assert.ok(configured, "the memory topic builder stays chainable")
})

void test("given_a_scoped_view_when_derived_then_should_keep_the_connection_and_change_only_the_scope", async () => {
  await using laser = await laserWithStream()

  const scoped = laser.withDefaultStream("audit")
  assert.equal(scoped.defaultStream, "audit")
  assert.equal(laser.defaultStream, "fleet")
  assert.equal(scoped.topic("events").streamName, "audit")
})

void test("given_a_context_scope_when_reaching_further_primitives_then_should_bind_the_conversation", async () => {
  await using laser = await laserWithStream()
  const conversation = ConversationId.new()
  const scope = laser.context(conversation)

  assert.equal(scope.memory("support") instanceof Object, true)
  assert.ok(scope.graph("services"))
})

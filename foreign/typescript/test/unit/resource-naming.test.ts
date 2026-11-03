import assert from "node:assert/strict"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom } from "../../src/client/capabilities.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import { decodeForkCreate } from "../../src/wire/fork.js"
import { decodeGraphQuery } from "../../src/wire/graph.js"
import {
  decodeKvCasFenced,
  decodeKvCopy,
  decodeKvDeleteMany,
  decodeKvScan
} from "../../src/wire/kv.js"
import { decodeGetSchema } from "../../src/wire/browse.js"

const CAPS: Capabilities = {
  ...managedCapabilitiesFrom({
    versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
    backends: []
  }),
  graph: true,
  forks: true
}
const CAS_CAPS: Capabilities = {
  ...CAPS,
  kv: { ...CAPS.kv, cas: true, casFenced: true, fencedLeases: true }
}

function fakeClient(): IggyClient {
  return {
    clientProvider: () => Promise.resolve({}),
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
}

// A client on stream `acme` whose managed requests are captured and refused.
async function scoped(): Promise<{
  readonly laser: Laser
  readonly sent: Map<string, unknown>[]
}> {
  const root = await Laser.builder().client(fakeClient()).capabilities(CAS_CAPS).connect()
  const sent: Map<string, unknown>[] = []
  root[INTERNAL_TRANSPORT]().sendManaged = (_code, payload) => {
    sent.push(expectMap(decodeOne(payload, "request"), "request") as Map<string, unknown>)
    return Promise.reject(new Error("captured"))
  }
  return { laser: root.withDefaultStream("acme"), sent }
}

async function captured<Value>(sent: Map<string, unknown>[], send: () => Promise<unknown>) {
  await send().catch(() => undefined)
  const request = sent.at(-1)
  assert.ok(request !== undefined)
  return request as unknown as Value
}

void test("given_a_default_stream_when_naming_resources_then_should_scope_once_and_opt_out_with_bare", async () => {
  const { laser } = await scoped()
  assert.equal(laser.resourceNaming(), "stream")
  assert.equal(laser.resourceName("agent.keys"), "stream:acme/agent.keys")
  assert.equal(laser.resourceName("stream:other/sessions"), "stream:other/sessions")
  const bare = laser.withResourceNaming("bare")
  assert.equal(bare.resourceName("agent.keys"), "agent.keys")
  assert.equal(bare.withDefaultStream("fleet").resourceNaming(), "bare")
  const streamless = await Laser.builder().client(fakeClient()).capabilities(CAPS).connect()
  assert.equal(streamless.resourceName("agent.keys"), "agent.keys")
})

void test("given_a_scoped_kv_when_built_then_should_send_scoped_names_and_keep_the_local_one", async () => {
  const { laser, sent } = await scoped()
  const kv = laser.kv("memory")
  assert.equal(kv.namespace, "memory")
  assert.equal(kv.resourceNamespace, "stream:acme/memory")
  assert.equal(laser.withResourceNaming("bare").kv("memory").resourceNamespace, "memory")
  const conversation = "01J00000000000000000000000"
  const scan = decodeKvScan(
    await captured(sent, () => kv.scan().conversation(conversation).fetch()),
    "scan"
  )
  assert.equal(scan.namespace, "stream:acme/memory")
  assert.equal(scan.stream, "acme")
  const unfiltered = decodeKvScan(await captured(sent, () => kv.scan().fetch()), "scan")
  assert.equal(unfiltered.stream, undefined)
  const many = decodeKvDeleteMany(
    await captured(sent, () => kv.deleteMany().conversation(conversation).send()),
    "delete many"
  )
  assert.equal(many.stream, "acme")
  const fenced = decodeKvCasFenced(
    await captured(sent, () =>
      kv
        .casFenced(Uint8Array.of(1), "coordination", Uint8Array.of(2), 1n)
        .bytes(Uint8Array.of(3))
        .expectAbsent()
        .commit()
    ),
    "cas fenced"
  )
  assert.equal(fenced.namespace, "stream:acme/memory")
  assert.equal(fenced.fenceNamespace, "stream:acme/coordination")
  const copy = decodeKvCopy(
    await captured(sent, () =>
      kv.copyTo(Uint8Array.of(1), Uint8Array.of(2)).intoNamespace("archive").send()
    ),
    "copy"
  )
  assert.equal(copy.toNamespace, "stream:acme/archive")
})

void test("given_listed_namespaces_and_forks_when_scoped_then_should_keep_only_the_own_stream", async () => {
  const root = await Laser.builder().client(fakeClient()).capabilities(CAPS).connect()
  const laser = root.withDefaultStream("acme")
  const kv = laser.kv("x")
  assert.equal(kv.namespace, "x")
  const fork = laser.fork("try-1")
  assert.equal(fork.id, "try-1")
  assert.equal(fork.resourceId, "stream:acme/try-1")
  assert.equal(laser.withResourceNaming("bare").fork("try-1").resourceId, "try-1")
})

void test("given_scoped_graph_fork_and_schema_requests_when_sent_then_should_name_the_stream", async () => {
  const { laser, sent } = await scoped()
  const query = decodeGraphQuery(
    await captured(sent, () => laser.graph("knowledge").conversation("c1").fetch()),
    "graph"
  )
  assert.equal(query.graph, "stream:acme/knowledge")
  assert.equal(query.stream, "acme")
  const create = decodeForkCreate(
    await captured(sent, () => laser.fork("try-1").create().parent("base").tables(["rows"]).send()),
    "fork"
  )
  assert.equal(create.forkId, "stream:acme/try-1")
  assert.equal(create.parent, "stream:acme/base")
  assert.deepEqual(create.tables, ["stream:acme/rows"])
  const schema = decodeGetSchema(await captured(sent, () => laser.schemas().get(7)), "schema")
  assert.equal(schema.stream, "acme")
  const bare = decodeGetSchema(
    await captured(sent, () => laser.withResourceNaming("bare").schemas().get(7)),
    "schema"
  )
  assert.equal(bare.stream, undefined)
})

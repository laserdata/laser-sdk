import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import {
  Feature,
  decodeBackendAnnounce,
  decodeHelloReply,
  encodeBackendAnnounce,
  encodeHelloReply,
  opVersionsHasFeature
} from "../../src/wire/hello.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

void test("given_the_backend_announce_fixture_when_decoded_then_should_preserve_backends_and_re_encode_byte_identically", async () => {
  const bytes = await readFixture("backend_announce.bin")
  const announce = decodeBackendAnnounce(bytes)

  assert.equal(announce.versions.query, 1)
  assert.equal(announce.versions.control, 1)
  assert.equal(announce.versions.kv, 1)
  assert.equal(announce.versions.fork, 1)
  assert.equal(announce.versions.checkpoint, 1)
  assert.ok(opVersionsHasFeature(announce.versions, Feature.KV_CAS))
  assert.ok(opVersionsHasFeature(announce.versions, Feature.DESTINATIONS))
  assert.equal(announce.topology, undefined)

  assert.equal(announce.backends.length, 2)
  const [embedded, warehouse] = announce.backends
  assert.ok(embedded !== undefined)
  assert.ok(warehouse !== undefined)
  assert.equal(embedded.resourceId.asU128(), 10n)
  assert.equal(embedded.label, "Embedded")
  assert.equal(warehouse.resourceId.asU128(), 11n)
  assert.equal(warehouse.implementation.kind, "columnar")
  assert.equal(warehouse.label, "Analytics warehouse")
  assert.equal(warehouse.implementation.version, "2.1.0")
  assert.ok(warehouse.query?.dialects.includes("data_fusion"))

  const reencoded = encodeBackendAnnounce(announce)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

void test("given_the_unavailable_backend_announce_fixture_when_decoded_then_should_stay_unavailable", async () => {
  const bytes = await readFixture("backend_announce_unavailable.bin")
  const announce = decodeBackendAnnounce(bytes)

  assert.equal(announce.ready, false)
  assert.deepEqual(Buffer.from(encodeBackendAnnounce(announce)), Buffer.from(bytes))
})

void test("given_the_minimal_hello_reply_fixture_when_decoded_then_should_re_encode_byte_identically", async () => {
  const bytes = await readFixture("hello_reply.bin")
  const reply = decodeHelloReply(bytes)
  assert.equal(reply.versions.query, 1)
  assert.equal(reply.versions.control, 1)
  assert.equal(reply.versions.kv, 1)
  assert.equal(reply.versions.fork, 1)
  assert.equal(reply.versions.agent, 0)
  assert.equal(reply.versions.features, 0n)
  assert.deepEqual(Buffer.from(encodeHelloReply(reply)), Buffer.from(bytes))
})

void test("given_the_hello_reply_agent_fixture_when_decoded_then_should_preserve_agent_version_and_re_encode_byte_identically", async () => {
  const bytes = await readFixture("hello_reply_agent.bin")
  const reply = decodeHelloReply(bytes)
  assert.equal(reply.versions.agent, 1)
  assert.deepEqual(Buffer.from(encodeHelloReply(reply)), Buffer.from(bytes))
})

void test("given_the_hello_reply_features_fixture_when_decoded_then_should_preserve_feature_bits_and_re_encode_byte_identically", async () => {
  const bytes = await readFixture("hello_reply_features.bin")
  const reply = decodeHelloReply(bytes)
  assert.equal(reply.versions.features, 259n)
  assert.equal(reply.versions.checkpoint, 1)
  assert.ok(opVersionsHasFeature(reply.versions, Feature.KV_CAS))
  assert.ok(opVersionsHasFeature(reply.versions, Feature.READ_YOUR_WRITES))
  assert.ok(!opVersionsHasFeature(reply.versions, Feature.STRONG_CONSISTENCY))
  assert.ok(opVersionsHasFeature(reply.versions, Feature.DESTINATIONS))
  assert.deepEqual(Buffer.from(encodeHelloReply(reply)), Buffer.from(bytes))
})

void test("given_the_backend_announce_topology_fixture_when_decoded_then_should_preserve_topology_and_re_encode_byte_identically", async () => {
  const bytes = await readFixture("backend_announce_topology.bin")
  const announce = decodeBackendAnnounce(bytes)

  assert.equal(announce.backends.length, 0)
  assert.ok(announce.topology !== undefined)
  assert.equal(announce.topology.opsStream, "acme-ops")
  assert.equal(announce.topology.controlTopic, "acme.control")
  assert.equal(announce.topology.dlqTopic, "acme.dlq")
  assert.equal(announce.topology.changesTopic, "acme.changes")
  assert.equal(announce.topology.kvMutationsTopic, "acme.kv.mutations")
  assert.equal(announce.topology.forkMutationsTopic, "acme.fork.mutations")
  assert.equal(announce.topology.runMutationsTopic, "acme.run.mutations")
  assert.equal(announce.topology.graphMutationsTopic, "acme.graph.mutations")

  const reencoded = encodeBackendAnnounce(announce)
  assert.deepEqual(Buffer.from(reencoded), Buffer.from(bytes))
})

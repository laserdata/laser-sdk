import assert from "node:assert/strict"
import { test } from "node:test"
import {
  Feature,
  backendDescriptorHasCapability,
  newBackendAnnounce,
  newBackendDescriptor,
  newOpVersions,
  opVersionsHasFeature
} from "../../src/wire/hello.js"

void test("given_advertised_features_when_checked_then_should_require_every_bit_present", () => {
  const versions = {
    ...newOpVersions(1, 1, 1, 1),
    features: Feature.KV_CAS | Feature.READ_YOUR_WRITES
  }
  assert.ok(opVersionsHasFeature(versions, Feature.KV_CAS))
  assert.ok(opVersionsHasFeature(versions, Feature.READ_YOUR_WRITES))
  assert.ok(!opVersionsHasFeature(versions, Feature.STRONG_CONSISTENCY))
  assert.ok(opVersionsHasFeature(versions, Feature.KV_CAS | Feature.READ_YOUR_WRITES))
  assert.ok(!opVersionsHasFeature(versions, Feature.KV_CAS | Feature.STRONG_CONSISTENCY))
})

void test("given_a_minimal_backend_descriptor_when_constructed_then_should_have_no_advisory_fields", () => {
  const backend = newBackendDescriptor("embedded", "embedded")
  assert.equal(backend.label, undefined)
  assert.equal(backend.version, undefined)
  assert.deepEqual(backend.capabilities, [])
})

void test("given_a_backend_descriptor_when_checked_for_a_capability_then_should_match_declared_tags", () => {
  const backend = {
    ...newBackendDescriptor("warehouse", "columnar"),
    capabilities: ["ingest", "query"]
  }
  assert.ok(backendDescriptorHasCapability(backend, "query"))
  assert.ok(!backendDescriptorHasCapability(backend, "vector_search"))
})

void test("given_a_new_backend_announce_when_constructed_then_should_have_no_backends_or_topology", () => {
  const announce = newBackendAnnounce(newOpVersions(1, 1, 1, 1))
  assert.deepEqual(announce.backends, [])
  assert.equal(announce.topology, undefined)
})

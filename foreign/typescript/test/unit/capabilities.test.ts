import assert from "node:assert/strict"
import { test } from "node:test"
import {
  OPEN_CAPABILITIES,
  managedCapabilitiesFrom,
  managedCapabilitiesWithUnknownVersions,
  mergeCapabilities,
  requireCapability,
  servesConsistency
} from "../../src/client/capabilities.js"
import { UnsupportedError } from "../../src/client/errors.js"
import { Feature } from "../../src/wire/hello.js"

void test("given_open_capabilities_when_a_managed_surface_is_required_then_should_reject_locally", () => {
  for (const surface of ["query", "kv", "forks", "graph", "authz", "agentWorkflow"] as const) {
    assert.throws(() => {
      requireCapability(OPEN_CAPABILITIES, surface)
    }, UnsupportedError)
  }
})

void test("given_a_legacy_managed_host_when_capabilities_are_built_then_should_enable_only_base_surfaces", () => {
  const capabilities = managedCapabilitiesWithUnknownVersions()
  assert.equal(capabilities.managed, true)
  assert.equal(capabilities.query.available, true)
  assert.equal(capabilities.kv.available, true)
  assert.equal(capabilities.forks, true)
  assert.equal(capabilities.kv.cas, false)
  assert.equal(capabilities.graph, false)
  assert.equal(capabilities.authz, false)
})

void test("given_feature_bits_when_capabilities_are_built_then_should_fold_nested_features_and_consistency", () => {
  const capabilities = managedCapabilitiesFrom({
    versions: {
      query: 1,
      control: 1,
      kv: 1,
      fork: 1,
      agent: 1,
      graph: 1,
      features:
        Feature.KV_CAS |
        Feature.KV_CAS_FENCED |
        Feature.STRONG_CONSISTENCY |
        Feature.AGENT_WORKFLOW |
        Feature.KEYWORD_SEARCH |
        Feature.WATCH |
        Feature.AUTHZ
    },
    backends: []
  })

  assert.deepEqual(capabilities.kv, { available: true, cas: true, casFenced: true })
  assert.deepEqual(capabilities.query, {
    available: true,
    consistency: "strong",
    keyword: true
  })
  assert.equal(capabilities.graph, true)
  assert.equal(capabilities.agentWorkflow, true)
  assert.equal(capabilities.watch, true)
  assert.equal(capabilities.authz, true)
  assert.equal(servesConsistency(capabilities, "readYourWrites"), true)
  assert.equal(servesConsistency(capabilities, "strong"), true)
})

void test("given_an_unavailable_backend_when_capabilities_are_built_then_should_fail_closed", () => {
  const capabilities = managedCapabilitiesFrom({
    versions: {
      query: 1,
      control: 1,
      kv: 1,
      fork: 1,
      agent: 1,
      graph: 1,
      features:
        Feature.KV_CAS |
        Feature.KV_CAS_FENCED |
        Feature.STRONG_CONSISTENCY |
        Feature.AGENT_WORKFLOW |
        Feature.KEYWORD_SEARCH |
        Feature.WATCH |
        Feature.AUTHZ
    },
    ready: false,
    backends: [{ id: "stale", kind: "embedded", capabilities: ["query"] }]
  })

  assert.equal(capabilities.managed, false)
  assert.deepEqual(capabilities.query, {
    available: false,
    consistency: "eventual",
    keyword: false
  })
  assert.deepEqual(capabilities.kv, { available: false, cas: false, casFenced: false })
  assert.equal(capabilities.graph, false)
  assert.equal(capabilities.forks, false)
  assert.equal(capabilities.agentWorkflow, false)
  assert.equal(capabilities.watch, false)
  assert.equal(capabilities.authz, true)
  assert.deepEqual(capabilities.backends, [])
})

void test("given_configured_capabilities_when_an_unavailable_announcement_is_merged_then_should_preserve_the_configuration", () => {
  const configured = {
    ...OPEN_CAPABILITIES,
    query: { ...OPEN_CAPABILITIES.query, available: true, consistency: "readYourWrites" as const },
    backends: [{ id: "configured", kind: "custom", capabilities: [] }]
  }
  const announced = managedCapabilitiesFrom({
    versions: {
      query: 1,
      control: 1,
      kv: 1,
      fork: 1,
      agent: 1,
      graph: 1,
      features: Feature.STRONG_CONSISTENCY | Feature.AUTHZ
    },
    ready: false,
    backends: [{ id: "stale", kind: "embedded", capabilities: [] }]
  })

  const capabilities = mergeCapabilities(configured, announced)

  assert.equal(capabilities.managed, false)
  assert.equal(capabilities.query.available, true)
  assert.equal(capabilities.query.consistency, "readYourWrites")
  assert.equal(capabilities.authz, true)
  assert.deepEqual(capabilities.backends, configured.backends)
  assert.deepEqual(capabilities.versions, announced.versions)
})

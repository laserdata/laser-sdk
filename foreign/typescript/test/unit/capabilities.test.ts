import assert from "node:assert/strict"
import { test } from "node:test"
import {
  OPEN_CAPABILITIES,
  managedCapabilitiesFrom,
  managedCapabilitiesWithUnknownVersions,
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

import assert from "node:assert/strict"
import test from "node:test"
import {
  OPEN_CAPABILITIES,
  advertisedTopology,
  backend,
  enabledBackends,
  isReady,
  isOpenOnly,
  managedCapabilitiesFrom,
  mergeCapabilities,
  readinessReasons,
  requireCapability,
  servesConsistency,
  unreadyBackends
} from "../../src/client/capabilities.js"
import { policyAware } from "../../src/stream/consumer-group.js"
import { TimeoutError, UnsupportedError } from "../../src/client/errors.js"
import { BackendResourceId } from "../../src/wire/ids.js"
import { feature, newBackendDescriptor, newOpVersions } from "../../src/wire/hello.js"
import { defaultWireTopology } from "../../src/wire/topology.js"

void test("given_sessions_bit_when_announced_then_should_enable_session_capability", () => {
  assert.equal(OPEN_CAPABILITIES.sessions, false)
  const announced = managedCapabilitiesFrom({
    versions: { ...newOpVersions(1, 1, 1, 1), features: feature.SESSIONS },
    backends: []
  })
  assert.equal(announced.sessions, true)
  assert.equal(announced.streamTenancy, false)
})

void test("given_the_stream_tenancy_bit_when_announced_then_should_enable_stream_tenancy", () => {
  assert.equal(OPEN_CAPABILITIES.streamTenancy, false)
  const announced = managedCapabilitiesFrom({
    versions: { ...newOpVersions(1, 1, 1, 1), features: feature.STREAM_TENANCY },
    backends: []
  })
  assert.equal(announced.streamTenancy, true)
  assert.equal(feature.STREAM_TENANCY, 1n << 13n)
})

void test("given_group_reads_without_an_evaluator_when_announced_then_should_preserve_catalog_and_policy_aware_consumption", () => {
  const capabilities = managedCapabilitiesFrom({
    versions: { ...newOpVersions(1, 1, 1, 1), filter: 1, features: feature.GROUP_POLICY_READS },
    ready: true,
    backends: []
  })
  assert.equal(capabilities.filters.native, false)
  assert.equal(capabilities.filters.catalog, true)
  assert.equal(capabilities.filters.groupPolicyReads, true)
  assert.equal(policyAware(capabilities), true)
  const unavailable = managedCapabilitiesFrom({
    versions: { ...newOpVersions(1, 1, 1, 1), filter: 1, features: feature.GROUP_POLICY_READS },
    ready: false,
    backends: []
  })
  assert.equal(unavailable.filters.catalog, false)
  assert.equal(unavailable.filters.groupPolicyReads, true)
})

void test("given_an_unconfirmed_probe_when_building_a_group_consumer_then_should_refuse_native_fallback", () => {
  for (const hello of ["unknown", "failed"] as const) {
    assert.throws(() => policyAware({ ...OPEN_CAPABILITIES, hello }), TimeoutError)
  }
  for (const hello of ["answered", "rejected"] as const) {
    assert.equal(policyAware({ ...OPEN_CAPABILITIES, hello }), false)
  }
})

void test("open capabilities reject every managed surface", () => {
  for (const surface of ["query", "destinations", "kv", "forks", "graph", "authz"] as const) {
    assert.throws(() => {
      requireCapability(OPEN_CAPABILITIES, surface)
    }, UnsupportedError)
  }
})

void test("structured ready backend drives query and destination capabilities", () => {
  const backend = {
    ...newBackendDescriptor(
      BackendResourceId.fromU128(1n),
      "lakehouse",
      "Warehouse",
      { kind: "iceberg", version: "1" },
      2n,
      3n
    ),
    desiredState: "enabled" as const,
    observedState: "ready" as const,
    readiness: { ready: true, reasons: [], observedAtMicros: 4n },
    query: {
      dialects: ["data_fusion" as const],
      timeTravel: ["snapshot_id" as const],
      consistency: ["eventual" as const, "read_your_writes" as const],
      logicalTypes: ["long" as const],
      paging: ["cursor" as const],
      cancellation: true,
      executionStatus: true,
      rawSql: true
    }
  }
  const capabilities = managedCapabilitiesFrom({
    versions: {
      ...newOpVersions(2, 2, 1, 1),
      checkpoint: 1,
      features: feature.STRONG_CONSISTENCY | feature.DESTINATIONS
    },
    backends: [backend]
  })
  assert.equal(capabilities.query.cursorPaging, true)
  assert.equal(capabilities.query.cancellation, true)
  assert.equal(capabilities.destinations.available, true)
  assert.equal(servesConsistency(capabilities, "read_your_writes"), true)
})

void test("unavailable announcement fails closed", () => {
  const resourceId = BackendResourceId.fromU128(9n)
  const descriptor = {
    ...newBackendDescriptor(
      resourceId,
      "operational",
      "Embedded",
      { kind: "turso", version: "1" },
      1n,
      1n
    ),
    desiredState: "enabled" as const,
    observedState: "unavailable" as const,
    readiness: {
      ready: false,
      reasons: [{ code: "probe_failed" as const }],
      observedAtMicros: 4n
    }
  }
  const capabilities = managedCapabilitiesFrom({
    versions: newOpVersions(2, 2, 1, 1),
    ready: false,
    backends: [descriptor]
  })
  assert.equal(capabilities.managed, false)
  assert.equal(capabilities.query.available, false)
  assert.equal(capabilities.destinations.available, false)
  assert.equal(capabilities.backends.length, 1)
  assert.equal(backend(capabilities, resourceId)?.label, "Embedded")
  assert.equal(enabledBackends(capabilities).length, 1)
  assert.equal(unreadyBackends(capabilities).length, 1)
  assert.equal(readinessReasons(capabilities, resourceId)?.[0]?.code, "probe_failed")
  assert.equal(isReady(capabilities), false)
})

function readyQueryBackend(
  id: bigint,
  consistency: readonly ("eventual" | "read_your_writes" | "strong")[]
) {
  return {
    ...newBackendDescriptor(
      BackendResourceId.fromU128(id),
      "operational",
      "Embedded",
      { kind: "embedded", version: "1" },
      1n,
      1n
    ),
    desiredState: "enabled" as const,
    observedState: "ready" as const,
    readiness: { ready: true, reasons: [], observedAtMicros: 1n },
    query: {
      dialects: ["sqlite" as const],
      timeTravel: [],
      consistency,
      logicalTypes: ["string" as const],
      paging: [],
      cancellation: false,
      executionStatus: false,
      rawSql: true
    }
  }
}

void test("given_a_ready_announcement_without_a_query_backend_when_merged_then_should_not_serve_query", () => {
  const capabilities = managedCapabilitiesFrom({
    versions: newOpVersions(1, 1, 1, 1),
    backends: []
  })
  assert.equal(capabilities.managed, true)
  assert.equal(capabilities.kv.available, true)
  assert.equal(capabilities.forks, true)
  assert.equal(capabilities.query.available, false)
})

void test("given_a_ready_query_backend_when_merged_then_should_take_its_strongest_consistency", () => {
  const capabilities = managedCapabilitiesFrom({
    versions: newOpVersions(1, 1, 1, 1),
    backends: [readyQueryBackend(1n, ["eventual", "strong"])]
  })
  assert.equal(capabilities.query.available, true)
  assert.equal(capabilities.query.consistency, "strong")
  const readYourWrites = managedCapabilitiesFrom({
    versions: newOpVersions(1, 1, 1, 1),
    backends: [readyQueryBackend(1n, ["read_your_writes"])]
  })
  assert.equal(readYourWrites.query.consistency, "read_your_writes")
})

void test("given_announced_destinations_when_merged_then_should_serve_linearizable_checkpoint_reads", () => {
  assert.equal(OPEN_CAPABILITIES.destinations.consistency, "potentially_stale")
  const capabilities = managedCapabilitiesFrom({
    versions: { ...newOpVersions(1, 1, 1, 1), checkpoint: 1, features: feature.DESTINATIONS },
    backends: []
  })
  assert.equal(capabilities.destinations.available, true)
  assert.equal(capabilities.destinations.consistency, "linearizable")
})

void test("given_a_seeded_surface_when_a_ready_announcement_merges_then_should_keep_it_and_apply_seed_dependent_rules", () => {
  const seed = {
    ...OPEN_CAPABILITIES,
    a2aGateway: true,
    destinations: { available: true, consistency: "potentially_stale" as const },
    filters: { native: true, catalog: false, groupPolicyReads: false }
  }
  const announce = {
    versions: { ...newOpVersions(1, 1, 1, 1), filter: 1 },
    backends: [],
    filters: { evaluatorVersion: 1, codecs: [] }
  }
  const merged = mergeCapabilities(seed, managedCapabilitiesFrom(announce))
  assert.equal(merged.a2aGateway, true)
  assert.equal(merged.hello, "answered")
  assert.equal(merged.destinations.consistency, "linearizable")
  assert.equal(merged.filters.catalog, true, "a seeded native filter surface enables the catalog")
  assert.deepEqual(merged.filters.evaluation, announce.filters)
})

void test("given_an_unready_announcement_when_merged_onto_a_seed_then_should_replace_backends_and_keep_the_seed", () => {
  const seededBackend = readyQueryBackend(7n, ["eventual"])
  const seed = { ...OPEN_CAPABILITIES, backends: [seededBackend] }
  const merged = mergeCapabilities(
    seed,
    managedCapabilitiesFrom({ versions: newOpVersions(1, 1, 1, 1), ready: false, backends: [] })
  )
  assert.equal(merged.managed, false)
  assert.deepEqual(merged.backends, [])
  assert.equal(merged.hello, "answered")
  const rejected = mergeCapabilities(seed, { ...OPEN_CAPABILITIES, hello: "rejected" })
  assert.deepEqual(rejected.backends, [seededBackend])
  assert.equal(rejected.hello, "rejected")
})

void test("given_an_announced_topology_when_merged_then_should_keep_it_off_the_public_capabilities", () => {
  const topology = { ...defaultWireTopology(), opsStream: "acme-ops" }
  const announced = managedCapabilitiesFrom({
    versions: newOpVersions(1, 1, 1, 1),
    backends: [],
    topology
  })
  assert.equal("topology" in announced, false)
  assert.equal(advertisedTopology(announced)?.opsStream, "acme-ops")
  const merged = mergeCapabilities(OPEN_CAPABILITIES, announced)
  assert.equal(advertisedTopology(merged)?.opsStream, "acme-ops")
  assert.equal(advertisedTopology({ ...OPEN_CAPABILITIES, hello: "failed" }), undefined)
})

void test("given_open_capabilities_when_the_probe_outcome_changes_then_should_still_be_open_only", () => {
  for (const hello of ["unknown", "answered", "rejected", "failed"] as const) {
    assert.equal(isOpenOnly({ ...OPEN_CAPABILITIES, hello }), true)
    assert.equal(isOpenOnly({ ...OPEN_CAPABILITIES, hello, managed: true }), false)
  }
})

import assert from "node:assert/strict"
import test from "node:test"
import {
  OPEN_CAPABILITIES,
  backend,
  enabledBackends,
  isReady,
  managedCapabilitiesFrom,
  readinessReasons,
  requireCapability,
  servesConsistency,
  unreadyBackends
} from "../../src/client/capabilities.js"
import { UnsupportedError } from "../../src/client/errors.js"
import { BackendResourceId } from "../../src/wire/ids.js"
import { Feature, newBackendDescriptor, newOpVersions } from "../../src/wire/hello.js"

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
      features: Feature.STRONG_CONSISTENCY | Feature.DESTINATIONS
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

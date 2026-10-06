import assert from "node:assert/strict"
import test from "node:test"
import { FILTER_EVALUATOR_VERSION } from "../../src/wire/filter.js"
import { BackendResourceId } from "../../src/wire/ids.js"
import {
  feature,
  backendDescriptorHasCapability,
  backendReadinessNotReady,
  filterAnnounceEvaluates,
  filterAnnounceServed,
  newBackendAnnounce,
  newBackendDescriptor,
  newOpVersions,
  opVersionsHasFeature,
  validateBackendDescriptor
} from "../../src/wire/hello.js"

void test("feature checks require every advertised bit", () => {
  const versions = {
    ...newOpVersions(2, 2, 1, 1),
    features: feature.KV_CAS | feature.READ_YOUR_WRITES
  }
  assert.ok(opVersionsHasFeature(versions, feature.KV_CAS))
  assert.ok(opVersionsHasFeature(versions, feature.KV_CAS | feature.READ_YOUR_WRITES))
  assert.ok(!opVersionsHasFeature(versions, feature.STRONG_CONSISTENCY))
})

void test("structured backend descriptor is secret-free and validates observed identity", () => {
  const backend = newBackendDescriptor(
    BackendResourceId.fromU128(1n),
    "lakehouse",
    "Warehouse",
    { kind: "iceberg", version: "1" },
    2n,
    3n
  )
  validateBackendDescriptor(backend)
  assert.equal(backend.resourceId.asU128(), 1n)
  assert.equal(backendDescriptorHasCapability(backend, "parquet"), false)
})

void test("a not-ready readiness names its one reason and no observation time", () => {
  assert.deepEqual(backendReadinessNotReady("probe_failed"), {
    ready: false,
    reasons: [{ code: "probe_failed" }],
    observedAtMicros: 0n
  })
})

void test("backend announcement starts without observed resources or topology", () => {
  const announce = newBackendAnnounce(newOpVersions(2, 2, 1, 1))
  assert.deepEqual(announce.backends, [])
  assert.equal(announce.topology, undefined)
})

void test("the served filter announcement evaluates every built-in codec at this evaluator version", () => {
  const served = filterAnnounceServed()
  assert.equal(served.evaluatorVersion, FILTER_EVALUATOR_VERSION)
  for (const codec of ["json", "headers_only", "avro", "protobuf", "cbor"] as const) {
    assert.equal(filterAnnounceEvaluates(served, FILTER_EVALUATOR_VERSION, codec), true, codec)
  }
  assert.equal(filterAnnounceEvaluates(served, FILTER_EVALUATOR_VERSION + 1, "json"), false)
})

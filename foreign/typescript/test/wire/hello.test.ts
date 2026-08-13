import assert from "node:assert/strict"
import test from "node:test"
import { BackendResourceId } from "../../src/wire/ids.js"
import {
  Feature,
  backendDescriptorHasCapability,
  newBackendAnnounce,
  newBackendDescriptor,
  newOpVersions,
  opVersionsHasFeature,
  validateBackendDescriptor
} from "../../src/wire/hello.js"

void test("feature checks require every advertised bit", () => {
  const versions = {
    ...newOpVersions(2, 2, 1, 1),
    features: Feature.KV_CAS | Feature.READ_YOUR_WRITES
  }
  assert.ok(opVersionsHasFeature(versions, Feature.KV_CAS))
  assert.ok(opVersionsHasFeature(versions, Feature.KV_CAS | Feature.READ_YOUR_WRITES))
  assert.ok(!opVersionsHasFeature(versions, Feature.STRONG_CONSISTENCY))
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

void test("backend announcement starts without observed resources or topology", () => {
  const announce = newBackendAnnounce(newOpVersions(2, 2, 1, 1))
  assert.deepEqual(announce.backends, [])
  assert.equal(announce.topology, undefined)
})

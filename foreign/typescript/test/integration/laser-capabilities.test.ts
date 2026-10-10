import assert from "node:assert/strict"
import { test, type TestContext } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { UnsupportedError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import type { LaserObserver } from "../../src/observe.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

function openOnly(capabilities: Capabilities, context: TestContext): boolean {
  if (!capabilities.managed) return true
  context.skip("this deployment includes a managed plane")
  return false
}

function probeObserver(onProbe: () => void): LaserObserver {
  return {
    start(operation) {
      if (operation === "laser.managed") onProbe()
      return { end: () => undefined }
    },
    event: () => undefined
  }
}

void test("given_no_managed_plane_when_refreshing_capabilities_then_should_preserve_the_detected_filter_profile", async (context) => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const capabilities = await laser.capabilities()
    if (!openOnly(capabilities, context)) return
    assert.equal(capabilities.managed, false)
    assert.deepEqual(capabilities.backends, [])
    assert.equal(capabilities.filters.catalog, false)
    const refreshed = await laser.refreshCapabilities()
    assert.equal(refreshed.managed, false)
    assert.deepEqual(refreshed.backends, [])
    assert.equal(refreshed.filters.native, capabilities.filters.native)
    assert.equal(refreshed.filters.groupPolicyReads, capabilities.filters.groupPolicyReads)
    assert.equal(refreshed.filters.catalog, false)
  } finally {
    await laser.close()
  }
})

void test("given_a_probed_connection_when_capabilities_is_called_again_then_should_not_reprobe", async () => {
  let probes = 0
  const laser = await Laser.builder()
    .connectionString(CONNECTION_STRING)
    .observer(probeObserver(() => probes++))
    .connect()
  try {
    await laser.capabilities()
    await laser.capabilities()
    assert.equal(probes, 1)
  } finally {
    await laser.close()
  }
})

void test("given_a_connection_when_closed_twice_then_should_be_idempotent", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  await laser.close()
  await laser.close()
})

void test("given_a_default_stream_scope_when_created_then_should_share_the_probed_capabilities", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const original = await laser.capabilities()
    const scoped = laser.withDefaultStream("fleet")
    assert.equal(scoped.defaultStream, "fleet")
    assert.equal(laser.defaultStream, undefined)
    const scopedCapabilities = await scoped.capabilities()
    assert.deepEqual(scopedCapabilities, original)
  } finally {
    await laser.close()
  }
})

void test("given_an_unmanaged_probe_when_refreshed_then_should_probe_again_and_stay_open", async (context) => {
  let probes = 0
  const laser = await Laser.builder()
    .connectionString(CONNECTION_STRING)
    .observer(probeObserver(() => probes++))
    .connect()
  try {
    if (!openOnly(await laser.capabilities(), context)) return
    const refreshed = await laser.refreshCapabilities()
    assert.equal(probes, 2)
    assert.equal(refreshed.managed, false)
  } finally {
    await laser.close()
  }
})

void test("given_an_unmanaged_probe_when_its_ttl_elapses_then_should_reprobe_on_the_next_read", async (context) => {
  // An unmanaged verdict is retried after a second, so a client that connected
  // during a backend startup race discovers readiness without reconnecting.
  let probes = 0
  const laser = await Laser.builder()
    .connectionString(CONNECTION_STRING)
    .observer(probeObserver(() => probes++))
    .connect()
  try {
    if (!openOnly(await laser.capabilities(), context)) return
    await new Promise((resolve) => setTimeout(resolve, 1_100))
    const second = await laser.capabilities()
    assert.equal(probes, 2)
    assert.equal(second.managed, false)
  } finally {
    await laser.close()
  }
})

void test("given_apache_iggy_when_query_is_fetched_then_should_return_unsupported", async (context) => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    if (!openOnly(await laser.capabilities(), context)) return
    await assert.rejects(laser.query("readings").limit(1).fetch(), UnsupportedError)
  } finally {
    await laser.close()
  }
})

void test("given_a_managed_plane_when_capabilities_are_refreshed_then_should_preserve_its_announced_readiness", async (context) => {
  await using laser = await Laser.connect(CONNECTION_STRING)
  const initial = await laser.capabilities()
  if (!initial.managed) {
    context.skip("this deployment has no managed plane")
    return
  }
  assert.equal(initial.query.available, true)
  assert.equal(initial.kv.available, true)
  const refreshed = await laser.refreshCapabilities()
  assert.equal(refreshed.managed, true)
  assert.equal(refreshed.kv.fencedLeases, initial.kv.fencedLeases)
  assert.equal(refreshed.hello, "answered")
})

import assert from "node:assert/strict"
import { test } from "node:test"
import { UnsupportedError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import type { LaserObserver } from "../../src/observe.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

function probeObserver(onProbe: () => void): LaserObserver {
  return {
    start(operation) {
      if (operation === "laser.managed") onProbe()
      return { end: () => undefined }
    },
    event: () => undefined
  }
}

void test("given_apache_iggy_when_probing_capabilities_then_should_report_open_not_managed", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const capabilities = await laser.capabilities()
    assert.equal(capabilities.managed, false)
    assert.deepEqual(capabilities.backends, [])
    assert.equal(capabilities.versions, undefined)
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
    await laser.capabilities()
    const scoped = laser.withDefaultStream("orders")
    assert.equal(scoped.defaultStream, "orders")
    assert.equal(laser.defaultStream, undefined)
    const scopedCapabilities = await scoped.capabilities()
    assert.equal(scopedCapabilities.managed, false)
  } finally {
    await laser.close()
  }
})

void test("given_an_unmanaged_probe_when_refreshed_then_should_probe_again_and_stay_open", async () => {
  let probes = 0
  const laser = await Laser.builder()
    .connectionString(CONNECTION_STRING)
    .observer(probeObserver(() => probes++))
    .connect()
  try {
    const refreshed = await laser.refreshCapabilities()
    assert.equal(probes, 2)
    assert.equal(refreshed.managed, false)
  } finally {
    await laser.close()
  }
})

void test("given_an_unmanaged_probe_when_its_ttl_elapses_then_should_reprobe_on_the_next_read", async () => {
  // An unmanaged verdict is retried after a second, so a client that connected
  // during a backend startup race discovers readiness without reconnecting.
  let probes = 0
  const laser = await Laser.builder()
    .connectionString(CONNECTION_STRING)
    .observer(probeObserver(() => probes++))
    .connect()
  try {
    await new Promise((resolve) => setTimeout(resolve, 1_100))
    const second = await laser.capabilities()
    assert.equal(probes, 2)
    assert.equal(second.managed, false)
  } finally {
    await laser.close()
  }
})

void test("given_apache_iggy_when_query_is_fetched_then_should_return_unsupported", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    await assert.rejects(laser.query("orders").limit(1).fetch(), UnsupportedError)
  } finally {
    await laser.close()
  }
})

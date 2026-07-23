import assert from "node:assert/strict"
import { test } from "node:test"
import { UnsupportedError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

void test("given_stock_apache_iggy_when_probing_capabilities_then_should_report_open_not_managed", async () => {
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
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    const first = await laser.capabilities()
    const second = await laser.capabilities()
    assert.equal(first, second)
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

void test("given_stock_apache_iggy_when_query_is_fetched_then_should_return_unsupported", async () => {
  const laser = await Laser.connect(CONNECTION_STRING)
  try {
    await assert.rejects(laser.query("orders").limit(1).fetch(), UnsupportedError)
  } finally {
    await laser.close()
  }
})

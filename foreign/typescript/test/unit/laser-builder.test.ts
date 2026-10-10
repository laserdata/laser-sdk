import assert from "node:assert/strict"
import { test } from "node:test"

import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { ConfigError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"
import type { LaserObserver } from "../../src/observe.js"

function fakeClient(onDestroy: () => void): IggyClient {
  return {
    clientProvider: () => Promise.resolve({}),
    destroy(): Promise<void> {
      onDestroy()
      return Promise.resolve()
    }
  } as unknown as IggyClient
}

void test("given_conflicting_builder_modes_when_connected_then_should_reject_before_io", async () => {
  const client = fakeClient(() => undefined)
  await assert.rejects(
    Laser.builder().connectionString("local").client(client).connect(),
    ConfigError
  )
  await assert.rejects(Laser.builder().credentials("user", "password").connect(), ConfigError)
  await assert.rejects(Laser.builder().opsStream("").connect(), ConfigError)
})

void test("given_an_address_without_credentials_or_with_bad_ones_when_connected_then_should_reject_before_io", async () => {
  await assert.rejects(Laser.builder().address("127.0.0.1").connect(), ConfigError)
  await assert.rejects(
    Laser.builder().address("127.0.0.1").credentials("user", "").connect(),
    ConfigError
  )
  await assert.rejects(
    Laser.builder().address("127.0.0.1").credentials("us@er", "password").connect(),
    ConfigError
  )
  await assert.rejects(
    Laser.builder().address("127.0.0.1", 0).credentials("user", "password").connect(),
    ConfigError
  )
})

void test("given_a_borrowed_injected_client_when_closed_then_should_leave_the_client_open", async () => {
  let destroys = 0
  const laser = (await Laser.fromClient(fakeClient(() => destroys++))).withDefaultStream("events")
  assert.equal(laser.defaultStream, "events")
  await laser.close()
  await laser.close()
  assert.equal(destroys, 0)
})

void test("given_an_owned_client_when_asynchronously_disposed_then_should_close_once", async () => {
  let destroys = 0
  const laser = await Laser.builder()
    .client(
      fakeClient(() => destroys++),
      { ownership: "owned" }
    )
    .connect()

  await laser[Symbol.asyncDispose]()
  await laser[Symbol.asyncDispose]()

  assert.equal(destroys, 1)
})

void test("given_an_owned_client_and_scoped_handle_when_disposed_then_should_only_close_from_the_root", async () => {
  let destroys = 0
  const laser = await Laser.builder()
    .client(
      fakeClient(() => destroys++),
      { ownership: "owned" }
    )
    .stream("events")
    .capabilities(OPEN_CAPABILITIES)
    .opsStream("ops-custom")
    .controlTopic("control-custom")
    .dlqTopic("dlq-custom")
    .changesTopic("changes-custom")
    .connect()
  const scoped = laser.withDefaultStream("other")

  assert.equal(laser.opsStream, "ops-custom")
  assert.equal(scoped.controlTopic, "control-custom")
  assert.equal(scoped.dlqTopic, "dlq-custom")
  assert.equal(scoped.changesTopic, "changes-custom")
  await scoped[Symbol.asyncDispose]()
  assert.equal(destroys, 0)
  await laser.close()
  await laser.close()
  assert.equal(destroys, 1)
})

void test("given_a_scoped_handle_when_explicitly_closed_then_should_close_the_shared_connection_once", async () => {
  let destroys = 0
  const laser = await Laser.fromClient(
    fakeClient(() => destroys++),
    { ownership: "owned" }
  )
  const scoped = laser.withDefaultStream("events")
  await Promise.all([scoped.close(), laser.close(), scoped.close()])
  assert.equal(destroys, 1)
  await assert.rejects(laser.stream("events").ensure(), /closed/)
})

void test("given_a_capability_override_when_refreshed_then_should_remain_authoritative", async () => {
  const laser = await Laser.fromClient(fakeClient(() => undefined))
  const configured = {
    ...OPEN_CAPABILITIES,
    query: { ...OPEN_CAPABILITIES.query, available: true }
  }
  const scoped = laser.withCapabilities(configured)

  assert.deepEqual(await scoped.refreshCapabilities(), configured)

  await laser.close()
})

void test("given_an_injected_observer_when_the_client_closes_then_should_record_the_operation", async () => {
  const calls: unknown[] = []
  const observer: LaserObserver = {
    start(operation, attributes) {
      calls.push(["start", operation, attributes])
      return {
        end(error) {
          calls.push(["end", error])
        }
      }
    },
    event: () => undefined
  }
  const laser = await Laser.builder()
    .client(
      fakeClient(() => undefined),
      { ownership: "owned" }
    )
    .observer(observer)
    .capabilities(OPEN_CAPABILITIES)
    .connect()

  await laser.close()

  assert.deepEqual(calls, [
    ["start", "laser.close", { operation: "close" }],
    ["end", undefined]
  ])
})

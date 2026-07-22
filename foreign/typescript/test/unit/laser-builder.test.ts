import assert from "node:assert/strict"
import { test } from "node:test"

import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { ConfigError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import type { IggyClient } from "../../src/iggy/apache-iggy.js"
import type { LaserObserver } from "../../src/observe.js"

function fakeClient(onDestroy: () => void): IggyClient {
  return {
    destroy(): Promise<void> {
      onDestroy()
      return Promise.resolve()
    }
  } as unknown as IggyClient
}

void test("given_conflicting_builder_modes_when_connected_then_should_reject_before_io", () => {
  const client = fakeClient(() => undefined)
  assert.throws(
    () => Laser.builder().connectionString("iggy://local").iggyClient(client).connect(),
    ConfigError
  )
  assert.throws(() => Laser.builder().credentials("user", "password").connect(), ConfigError)
  assert.throws(() => Laser.builder().opsStream("").connect(), ConfigError)
})

void test("given_a_borrowed_injected_client_when_closed_then_should_leave_the_client_open", async () => {
  let destroys = 0
  const laser = await Laser.fromIggyClient(
    fakeClient(() => destroys++),
    {
      defaultStream: "events"
    }
  )
  assert.equal(laser.defaultStream, "events")
  await laser.close()
  await laser.close()
  assert.equal(destroys, 0)
})

void test("given_an_owned_client_and_scoped_handle_when_closed_then_should_only_close_from_the_root", async () => {
  let destroys = 0
  const laser = await Laser.builder()
    .iggyClient(
      fakeClient(() => destroys++),
      { ownership: "owned" }
    )
    .defaultStream("events")
    .capabilities(OPEN_CAPABILITIES)
    .opsStream("ops-custom")
    .controlTopic("control-custom")
    .deadLetterTopic("dlq-custom")
    .changesTopic("changes-custom")
    .connect()
  const scoped = laser.withDefaultStream("other")

  assert.equal(laser.opsStream, "ops-custom")
  assert.equal(scoped.controlTopic, "control-custom")
  assert.equal(scoped.deadLetterTopic, "dlq-custom")
  assert.equal(scoped.changesTopic, "changes-custom")
  await scoped.close()
  assert.equal(destroys, 0)
  await laser.close()
  await laser.close()
  assert.equal(destroys, 1)
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
    .iggyClient(
      fakeClient(() => undefined),
      { ownership: "owned" }
    )
    .observer(observer)
    .connect()

  await laser.close()

  assert.deepEqual(calls, [
    ["start", "laser.close", { operation: "close" }],
    ["end", undefined]
  ])
})

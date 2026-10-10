import assert from "node:assert/strict"
import { test } from "node:test"
import { Agent, type AgentBuilder } from "../../src/agent/builder.js"
import {
  ReliableConsumer,
  type AgentHandler,
  type ReliableConsumerControl
} from "../../src/agent/reliable-consumer.js"
import { HandlerConfigError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import { ActionDecision, GovernorMode, type ActionGovernor } from "../../src/govern.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId } from "../../src/types/ids.js"

function builder(): AgentBuilder {
  return Agent.builder()
    .id(AgentId.new("configured-worker"))
    .listenOn(AgentTopic.Sessions)
    .handler({ handle: () => Promise.resolve() })
}

void test("given_invalid_numeric_policies_when_building_then_should_reject_before_io", () => {
  assert.throws(() => builder().pollInterval(-1).build(), /pollInterval/)
  assert.throws(() => builder().shutdownGrace(Number.NaN).build(), /shutdownGrace/)
  assert.throws(() => builder().dedupWindow(0).build(), /dedupWindow/)
  assert.throws(() => builder().retry({ maxAttempts: 0, baseDelayMs: 1 }).build(), /maxAttempts/)
  assert.throws(() => builder().retry({ maxAttempts: 1, baseDelayMs: -1 }).build(), /baseDelayMs/)
  assert.throws(
    () => builder().concurrency({ kind: "serial-per-partition", maxPartitions: 0 }).build(),
    /maxPartitions/
  )
})

void test("given_a_missing_handler_when_building_then_should_reject_with_handler_config", () => {
  assert.throws(
    () => Agent.builder().id(AgentId.new("w")).listenOn(AgentTopic.Sessions).build(),
    HandlerConfigError
  )
})

void test("given_an_agent_literal_when_constructed_then_should_validate_like_the_builder", () => {
  assert.throws(
    () =>
      new Agent({
        id: AgentId.new("literal-worker"),
        listenOn: AgentTopic.Sessions,
        handler: { handle: () => Promise.resolve() },
        shutdownGraceMs: -1,
        understoodFeatures: 0n,
        warmDedup: false,
        middleware: [],
        capabilities: [],
        ackOnPickup: false
      }),
    /shutdownGrace/
  )
})

void test(
  "given_a_governor_and_retention_when_spawning_then_should_scope_the_laser_with_both",
  { timeout: 1000 },
  async (t) => {
    t.mock.method(
      ReliableConsumer.prototype,
      "run",
      (_laser: Laser, _handler: AgentHandler, control: ReliableConsumerControl) => {
        control.ready?.()
        return Promise.resolve()
      }
    )
    const governor: ActionGovernor = { decide: () => Promise.resolve(ActionDecision.allow()) }
    const calls: unknown[][] = []
    const laser = {
      withGovernor(...args: unknown[]): Laser {
        calls.push(args)
        return laser
      }
    } as unknown as Laser
    const handle = builder()
      .governor([governor, GovernorMode.Enforce])
      .governorRetention({ capacity: 8 })
      .build()
      .spawn(laser)
    await handle.join()
    assert.deepEqual(calls, [[governor, GovernorMode.Enforce, { capacity: 8 }]])
  }
)

void test(
  "given_served_operations_when_spawning_then_should_hand_them_to_the_reliable_consumer",
  { timeout: 1000 },
  async (t) => {
    let operations: readonly string[] | undefined
    t.mock.method(
      ReliableConsumer.prototype,
      "run",
      function (
        this: ReliableConsumer,
        _laser: Laser,
        _handler: AgentHandler,
        control: ReliableConsumerControl
      ) {
        operations = (this as unknown as { options: { operations?: readonly string[] } }).options
          .operations
        control.ready?.()
        return Promise.resolve()
      }
    )
    const handle = builder()
      .operations(["summarize"])
      .build()
      .spawn({} as Laser)
    await handle.join()
    assert.deepEqual(operations, ["summarize"])
  }
)

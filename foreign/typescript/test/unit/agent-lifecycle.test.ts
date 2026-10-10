import assert from "node:assert/strict"
import { test, type TestContext } from "node:test"

import { Agent } from "../../src/agent/builder.js"
import {
  ReliableConsumer,
  type AgentHandler,
  type ReliableConsumerControl
} from "../../src/agent/reliable-consumer.js"
import { HandlerConfigError, NoStreamError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import type { ConsolidationReport } from "../../src/memory/types.js"
import { AgentId } from "../../src/types/ids.js"

function builder() {
  return Agent.builder()
    .id(AgentId.new("consolidating-worker"))
    .listenOn("agent.sessions")
    .handler({ handle: () => Promise.resolve() })
    .consolidateEvery(10)
}

void test(
  "given_a_stalled_consolidator_when_shutdown_runs_then_should_abort_its_signal_and_finish",
  { timeout: 1000 },
  async (t) => {
    t.mock.method(
      ReliableConsumer.prototype,
      "run",
      async (_laser: Laser, _handler: AgentHandler, control: ReliableConsumerControl) => {
        control.ready?.()
        await new Promise<void>((resolve) => {
          control.signal?.addEventListener(
            "abort",
            () => {
              resolve()
            },
            { once: true }
          )
        })
      }
    )
    let signal: AbortSignal | undefined
    const handle = builder()
      .consolidator({
        consolidate: (_scope, cancellation) => {
          signal = cancellation
          return new Promise<ConsolidationReport>(() => undefined)
        }
      })
      .build()
      .spawn({} as Laser)
    await handle.ready()
    await handle.shutdown()
    assert.equal(signal?.aborted, true)
  }
)

void test(
  "given_a_running_worker_when_join_is_waiting_then_should_keep_consolidation_active_until_the_worker_exits",
  { timeout: 1000 },
  async (t) => {
    let finish!: () => void
    t.mock.method(
      ReliableConsumer.prototype,
      "run",
      async (_laser: Laser, _handler: AgentHandler, control: ReliableConsumerControl) => {
        control.ready?.()
        await new Promise<void>((resolve) => {
          finish = resolve
        })
      }
    )
    let signal: AbortSignal | undefined
    const handle = builder()
      .consolidator({
        consolidate: (_scope, cancellation) => {
          signal = cancellation
          return new Promise<ConsolidationReport>(() => undefined)
        }
      })
      .build()
      .spawn({} as Laser)
    await handle.ready()
    const joined = handle.join()
    assert.ok(signal !== undefined)
    assert.equal(signal.aborted, false)
    finish()
    await joined
    assert.equal(signal.aborted, true)
  }
)

void test(
  "given_failed_agent_startup_when_joined_then_should_stop_the_stalled_consolidator",
  { timeout: 1000 },
  async () => {
    let signal: AbortSignal | undefined
    const handle = builder()
      .consolidator({
        consolidate: (_scope, cancellation) => {
          signal = cancellation
          return new Promise<ConsolidationReport>(() => undefined)
        }
      })
      .build()
      .spawn({} as Laser)
    await assert.rejects(handle.join(), NoStreamError)
    assert.equal(signal?.aborted, true)
    await assert.rejects(handle.ready(), NoStreamError)
  }
)

// The consumer reports ready and parks until shutdown, then drains for `settleMs`.
function parkedRun(t: TestContext, settleMs = 0): void {
  t.mock.method(
    ReliableConsumer.prototype,
    "run",
    async (_laser: Laser, _handler: AgentHandler, control: ReliableConsumerControl) => {
      control.ready?.()
      await new Promise<void>((resolve) => {
        control.signal?.addEventListener(
          "abort",
          () => {
            setTimeout(resolve, settleMs)
          },
          { once: true }
        )
      })
    }
  )
}

void test(
  "given_an_agent_with_an_id_when_consolidation_ticks_then_should_scope_the_pass_to_that_agent",
  { timeout: 1000 },
  async (t) => {
    parkedRun(t)
    const scopes: unknown[] = []
    const handle = builder()
      .consolidator({
        consolidate: (scope) => {
          scopes.push(scope)
          return Promise.resolve({ summarized: 0, reweighted: 0, pruned: 0, derived: 0 })
        }
      })
      .build()
      .spawn({} as Laser)
    await handle.ready()
    await handle.shutdown()
    assert.deepEqual(scopes[0], { agent: AgentId.new("consolidating-worker") })
  }
)

void test(
  "given_intervals_beyond_the_timer_range_when_running_then_should_neither_tick_again_nor_cut_the_grace_short",
  { timeout: 2000 },
  async (t) => {
    parkedRun(t, 30)
    let passes = 0
    const handle = builder()
      .consolidateEvery(2 ** 31 + 10)
      .shutdownGrace(2 ** 31 + 10)
      .consolidator({
        consolidate: () => {
          passes += 1
          return Promise.resolve({ summarized: 0, reweighted: 0, pruned: 0, derived: 0 })
        }
      })
      .build()
      .spawn({} as Laser)
    await handle.ready()
    await new Promise((resolve) => setTimeout(resolve, 50))
    assert.equal(passes, 1)
    await handle.shutdown()
  }
)

void test("given_a_zero_consolidation_period_when_built_then_should_refuse_it_as_config", () => {
  for (const period of [0, -1, Number.NaN]) {
    assert.throws(() => builder().consolidateEvery(period).build(), HandlerConfigError)
  }
})

import assert from "node:assert/strict"
import { test } from "node:test"

import { Agent } from "../../src/agent/builder.js"
import {
  ReliableConsumer,
  type AgentHandler,
  type ReliableConsumerControl
} from "../../src/agent/reliable-consumer.js"
import { NoStreamError } from "../../src/client/errors.js"
import type { Laser } from "../../src/client/laser.js"
import type { ConsolidationReport } from "../../src/memory/types.js"
import { AgentId } from "../../src/types/ids.js"

function builder() {
  return Agent.builder()
    .id(AgentId.new("consolidating-worker"))
    .listenOn("agent.commands")
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
      .spawn({} as Laser)
    await assert.rejects(handle.join(), NoStreamError)
    assert.equal(signal?.aborted, true)
    await assert.rejects(handle.ready(), NoStreamError)
  }
)

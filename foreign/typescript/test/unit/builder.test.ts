import assert from "node:assert/strict"
import { test } from "node:test"
import { Agent, type AgentBuilder } from "../../src/agent/builder.js"
import type { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId } from "../../src/types/ids.js"

const unusedLaser = undefined as unknown as Laser

function builder(): AgentBuilder {
  return Agent.builder()
    .id(AgentId.new("configured-worker"))
    .listenOn(AgentTopic.Commands)
    .handler({ handle: () => Promise.resolve() })
}

void test("given_invalid_numeric_policies_when_spawning_then_should_reject_before_io", () => {
  assert.throws(() => builder().pollInterval(-1).spawn(unusedLaser), /pollInterval/)
  assert.throws(() => builder().shutdownGrace(Number.NaN).spawn(unusedLaser), /shutdownGrace/)
  assert.throws(() => builder().dedupWindow(0).spawn(unusedLaser), /dedupWindow/)
  assert.throws(
    () => builder().retry({ maxAttempts: 0, baseDelayMs: 1 }).spawn(unusedLaser),
    /maxAttempts/
  )
  assert.throws(
    () => builder().retry({ maxAttempts: 1, baseDelayMs: -1 }).spawn(unusedLaser),
    /baseDelayMs/
  )
  assert.throws(
    () =>
      builder().concurrency({ kind: "serial-per-partition", maxPartitions: 0 }).spawn(unusedLaser),
    /maxPartitions/
  )
})

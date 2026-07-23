import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import { topologicalOrder } from "../../src/agent/workflow.js"
import { Workflow } from "../../src/agent/workflow.js"
import type { Laser } from "../../src/client/laser.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { routeTo } from "../../src/agent/router.js"

void test("given_a_linear_workflow_when_ordered_then_should_follow_dependencies", () => {
  const steps = [
    { label: "credit", after: ["diagnose"] },
    { label: "triage", after: [] },
    { label: "diagnose", after: ["triage"] }
  ]
  assert.deepEqual(
    topologicalOrder(steps).map((index) => steps[index]?.label),
    ["triage", "diagnose", "credit"]
  )
})

void test("given_independent_steps_when_ordered_then_should_preserve_authored_order", () => {
  assert.deepEqual(
    topologicalOrder([
      { label: "a", after: [] },
      { label: "b", after: [] },
      { label: "c", after: [] }
    ]),
    [0, 1, 2]
  )
})

void test("given_a_dependency_cycle_when_ordered_then_should_reject_it", () => {
  assert.throws(
    () =>
      topologicalOrder([
        { label: "a", after: ["b"] },
        { label: "b", after: ["a"] }
      ]),
    InvalidError
  )
})

void test("given_an_unknown_dependency_when_ordered_then_should_reject_it", () => {
  assert.throws(() => topologicalOrder([{ label: "a", after: ["missing"] }]), InvalidError)
})

void test("given_an_exclusive_namespace_when_dispatched_then_should_propagate_its_monotonic_fence", async () => {
  const leaseNamespaces: string[] = []
  const fences: bigint[] = []
  const contract = {
    from: () => contract,
    payload: () => contract,
    inboxRoute: () => contract,
    deadline: () => contract,
    conversation: () => contract,
    fence: (value: bigint) => {
      fences.push(value)
      return contract
    },
    send: () =>
      Promise.resolve({
        kind: "completed" as const,
        reply: {
          provenance: { conversationId: ConversationId.new() },
          payload: new TextEncoder().encode("committed"),
          id: { partitionId: 0, offset: 0n }
        }
      })
  }
  const fake = {
    capabilities: () => Promise.resolve({ kv: { casFenced: true } }),
    context: () => ({ fetch: () => Promise.resolve([]) }),
    kv: (namespace: string) => ({
      lease: () => {
        leaseNamespaces.push(namespace)
        return Promise.resolve({ token: 41n, grantedTtlMicros: 60_000_000n })
      }
    }),
    contract: () => contract,
    sendAgent: () => Promise.resolve()
  } as unknown as Laser

  const outcome = await new Workflow(fake, "orchestrator")
    .step("effect", routeTo(AgentId.new("worker")), () => new TextEncoder().encode("apply"))
    .exclusiveIn("incident-effects")
    .run()
  assert.equal(new TextDecoder().decode(outcome.outputs.get("effect")), "committed")
  assert.deepEqual(leaseNamespaces, ["incident-effects"])
  assert.deepEqual(fences, [41n])
})

import assert from "node:assert/strict"
import { test } from "node:test"
import { CancelledError, InvalidError } from "../../src/client/errors.js"
import { topologicalOrder } from "../../src/agent/workflow.js"
import { Workflow } from "../../src/agent/workflow.js"
import type { Contract } from "../../src/agent/contract.js"
import type { Laser } from "../../src/client/laser.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { routeTo } from "../../src/agent/router.js"

// A run session that records its lifecycle verbs.
function fakeSessions(verbs: string[] = [], cancelRequested = false): () => { start(): unknown } {
  const session = {
    cancelRequested: () => Promise.resolve(cancelRequested),
    budgetBreach: () => Promise.resolve(undefined),
    end: () => {
      verbs.push("end")
      return Promise.resolve()
    },
    cancel: () => {
      verbs.push("cancel")
      return Promise.resolve()
    },
    fail: () => {
      verbs.push("fail")
      return Promise.resolve()
    }
  }
  const builder = {
    withId: () => builder,
    agent: () => builder,
    budget: () => builder,
    begin: () => Promise.resolve({ session, lease: { release: () => undefined } })
  }
  return () => ({ start: () => builder })
}

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
  const events: string[] = []
  const leaseNamespaces: string[] = []
  const released: string[] = []
  const fences: bigint[] = []
  const contract = {
    from: () => contract,
    payload: () => contract,
    inboxRoute: () => contract,
    deadline: () => contract,
    conversation: () => contract,
    parent: () => contract,
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
    capabilities: () => Promise.resolve({ kv: { fencedLeases: true } }),
    context: () => ({ fetch: () => Promise.resolve([]) }),
    kv: (namespace: string) => ({
      lease: () => {
        leaseNamespaces.push(namespace)
        return Promise.resolve({
          token: 41n,
          grantedTtlMs: 60_000,
          position: { topicGeneration: 1n, partition: 0, offset: 1n }
        })
      },
      renewLease: () => Promise.reject(new Error("an immediate contract must not renew")),
      release: (_key: Uint8Array, holder: string) => {
        events.push("release")
        released.push(holder)
        return Promise.resolve(true)
      }
    }),
    contract: () => contract,
    sessions: fakeSessions(),
    sendAgent: () => {
      events.push("journal")
      return Promise.resolve()
    }
  } as unknown as Laser

  const outcome = await Workflow.create(fake, "orchestrator")
    .step("effect", routeTo(AgentId.new("worker")), () => new TextEncoder().encode("apply"))
    .exclusiveIn("incident-effects")
    .run()
  assert.equal(new TextDecoder().decode(outcome.outputs.get("effect")), "committed")
  assert.deepEqual(leaseNamespaces, ["incident-effects"])
  assert.deepEqual(fences, [41n])
  assert.equal(released.length, 1)
  assert.deepEqual(events, ["journal", "release"])
})

void test(
  "given_a_slow_renewal_when_the_contract_completes_then_should_journal_while_renewal_remains_in_flight",
  { timeout: 5_000 },
  async (context) => {
    context.mock.method(performance, "now", () => 0)
    const events: string[] = []
    let completeContract: ((value: Contract) => void) | undefined
    let completeRenewal: (() => void) | undefined
    let markRenewalStarted: (() => void) | undefined
    const contractResult = new Promise<Contract>((resolve) => {
      completeContract = resolve
    })
    const renewalResult = new Promise<void>((resolve) => {
      completeRenewal = resolve
    })
    const renewalStarted = new Promise<void>((resolve) => {
      markRenewalStarted = resolve
    })
    const contract = {
      from: () => contract,
      payload: () => contract,
      inboxRoute: () => contract,
      deadline: () => contract,
      conversation: () => contract,
      parent: () => contract,
      fence: () => contract,
      send: () => contractResult
    }
    const fake = {
      capabilities: () => Promise.resolve({ kv: { fencedLeases: true } }),
      context: () => ({ fetch: () => Promise.resolve([]) }),
      kv: () => ({
        lease: () =>
          Promise.resolve({
            token: 41n,
            grantedTtlMs: 100,
            position: { topicGeneration: 1n, partition: 0, offset: 1n }
          }),
        renewLease: async () => {
          events.push("renew-start")
          markRenewalStarted?.()
          await renewalResult
          events.push("renew-finish")
          return {
            token: 41n,
            grantedTtlMs: 60_000,
            position: { topicGeneration: 1n, partition: 0, offset: 2n }
          }
        },
        release: () => {
          events.push("release")
          return Promise.resolve(true)
        }
      }),
      contract: () => contract,
      sessions: fakeSessions(),
      sendAgent: () => {
        events.push("journal")
        return Promise.resolve()
      }
    } as unknown as Laser

    const running = Workflow.create(fake, "orchestrator")
      .step("effect", routeTo(AgentId.new("worker")), () => new Uint8Array())
      .exclusive()
      .run()
    const result = running.then(
      () => ({ ok: true as const }),
      (error: unknown) => ({ ok: false as const, error })
    )
    await renewalStarted
    assert.deepEqual(events, ["renew-start"])
    completeContract?.({
      kind: "completed",
      reply: {
        provenance: { conversationId: ConversationId.new() },
        payload: new TextEncoder().encode("done"),
        id: { partitionId: 0, offset: 0n }
      }
    })
    await new Promise((resolve) => setTimeout(resolve, 0))
    assert.deepEqual(events, ["renew-start", "journal"])
    completeRenewal?.()
    const outcome = await result
    if (!outcome.ok) throw outcome.error
    assert.deepEqual(events, ["renew-start", "journal", "renew-finish", "release"])
  }
)

void test("given_a_timed_out_exclusive_step_when_reassigned_then_should_release_and_use_a_fresh_holder", async () => {
  const holders: string[] = []
  const released: string[] = []
  const fences: bigint[] = []
  let sends = 0
  const contract = {
    from: () => contract,
    payload: () => contract,
    inboxRoute: () => contract,
    deadline: () => contract,
    conversation: () => contract,
    parent: () => contract,
    fence: (value: bigint) => {
      fences.push(value)
      return contract
    },
    send: () => {
      sends += 1
      if (sends === 1) return Promise.resolve({ kind: "timedOut" as const })
      return Promise.resolve({
        kind: "completed" as const,
        reply: {
          provenance: { conversationId: ConversationId.new() },
          payload: new TextEncoder().encode("reassigned"),
          id: { partitionId: 0, offset: 0n }
        }
      })
    }
  }
  const fake = {
    capabilities: () => Promise.resolve({ kv: { fencedLeases: true } }),
    context: () => ({ fetch: () => Promise.resolve([]) }),
    kv: () => ({
      lease: (_key: Uint8Array, holder: string) => {
        holders.push(holder)
        return Promise.resolve({
          token: BigInt(holders.length),
          grantedTtlMs: 60_000,
          position: { topicGeneration: 1n, partition: 0, offset: BigInt(holders.length) }
        })
      },
      renewLease: () => Promise.reject(new Error("an immediate contract must not renew")),
      release: (_key: Uint8Array, holder: string) => {
        released.push(holder)
        return Promise.resolve(true)
      }
    }),
    contract: () => contract,
    sessions: fakeSessions(),
    sendAgent: () => Promise.resolve()
  } as unknown as Laser

  const outcome = await Workflow.create(fake, "orchestrator")
    .step("effect", routeTo(AgentId.new("worker")), () => new TextEncoder().encode("apply"))
    .exclusive()
    .onTimeout("reassign")
    .run()
  assert.equal(new TextDecoder().decode(outcome.outputs.get("effect")), "reassigned")
  assert.equal(holders.length, 2)
  assert.notEqual(holders[0], holders[1])
  assert.deepEqual(released, holders)
  assert.deepEqual(fences, [1n, 2n])
})

void test("given_a_cancel_request_on_the_run_session_when_executed_then_should_cancel_and_name_the_run", async () => {
  const verbs: string[] = []
  const fake = {
    context: () => ({ fetch: () => Promise.resolve([]) }),
    sessions: fakeSessions(verbs, true)
  } as unknown as Laser
  const runId = ConversationId.new()
  await assert.rejects(
    Workflow.create(fake, "orchestrator")
      .runId(runId)
      .step("effect", routeTo(AgentId.new("worker")), () => new TextEncoder().encode("apply"))
      .run(),
    (error: unknown) => error instanceof CancelledError && error.run === runId.toString()
  )
  assert.deepEqual(verbs, ["cancel"])
})

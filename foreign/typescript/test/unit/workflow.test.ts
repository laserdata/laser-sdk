import assert from "node:assert/strict"
import { test } from "node:test"
import {
  AmbiguousMutationError,
  CancelledError,
  InvalidError,
  KvExecutionError,
  StepFailedError
} from "../../src/client/errors.js"
import { isLeaseLost, isRetryable } from "../../src/client/error-classify.js"
import { INTERNAL_REPLY_HUB, INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import {
  Workflow,
  decodeJournalEntry,
  dispatchIds,
  encodeJournalEntry,
  foldJournal,
  topologicalOrder
} from "../../src/agent/workflow.js"
import type { Contract } from "../../src/agent/contract.js"
import type { Laser } from "../../src/client/laser.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { routeTo } from "../../src/agent/router.js"
import { encodeNamed } from "../../src/wire/cbor.js"

const WORKER = AgentId.new("worker")

// A run session that records its lifecycle verbs.
function fakeSessions(
  verbs: string[] = [],
  cancelRequested: () => Promise<boolean> = () => Promise.resolve(false)
): () => { start(): unknown } {
  const session = {
    cancelRequested,
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
    begin: () => {
      verbs.push("begin")
      return Promise.resolve({ session, lease: { release: () => undefined } })
    },
    resume: () => {
      verbs.push("resume")
      return Promise.resolve({ session, lease: { release: () => undefined } })
    }
  }
  return () => ({ start: () => builder })
}

// What the engine set on a contract builder.
interface ContractCalls {
  [setter: string]: unknown
  payload?: unknown
  deadline?: unknown
  fence?: unknown
  idempotencyKey?: unknown
  resumeFrom?: unknown
  excluding?: unknown
}

// A contract builder whose every setter is recorded and whose send answers
// with `send`.
function fakeContract(
  send: (calls: Readonly<ContractCalls>) => Promise<Contract>,
  calls: ContractCalls = {}
): unknown {
  const record =
    (name: string) =>
    (value: unknown): unknown => {
      calls[name] = value
      return contract
    }
  const contract = {
    from: record("from"),
    payload: record("payload"),
    inboxRoute: record("inboxRoute"),
    deadline: record("deadline"),
    conversation: record("conversation"),
    parent: record("parent"),
    fence: record("fence"),
    correlation: record("correlation"),
    idempotencyKey: record("idempotencyKey"),
    resumeFrom: record("resumeFrom"),
    excluding: record("excluding"),
    send: () => send(calls),
    sendResolved: async () => ({ target: WORKER, contract: await send(calls) })
  }
  return contract
}

function completed(body: string): Contract {
  return {
    kind: "completed",
    reply: {
      provenance: { conversationId: ConversationId.new() },
      payload: new TextEncoder().encode(body),
      id: { partitionId: 0, offset: 0n }
    }
  }
}

// The connection a workflow needs besides its contracts: an empty journal, a
// reply hub, and a journal topic nobody trimmed.
function fakeLaser(overrides: Record<string, unknown>): Laser {
  return {
    defaultStream: "stream",
    capabilities: () => Promise.resolve({ kv: { fencedLeases: true } }),
    topic: () => ({
      partitionCount: () => Promise.resolve(undefined),
      tailOffsets: () => Promise.resolve(new Map())
    }),
    [INTERNAL_REPLY_HUB]: () => Promise.resolve({ positions: () => Promise.resolve([4n, 9n]) }),
    [INTERNAL_TRANSPORT]: () => ({ pollMessages: () => Promise.resolve([]) }),
    sessions: fakeSessions(),
    sendAgent: () => Promise.resolve(),
    ...overrides
  } as unknown as Laser
}

function lease(token: bigint, grantedTtlMs = 60_000): unknown {
  return {
    token,
    grantedTtlMs,
    position: { topicGeneration: 1n, partition: 0, offset: token },
    validUntilMs: performance.now() + grantedTtlMs
  }
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

void test("given_a_journal_when_folded_then_should_keep_completions_and_drop_compensated_steps", () => {
  const journal = foldJournal([
    { kind: "runStarted", workflow: "flow" },
    { kind: "stepDispatched", label: "triage", attempt: 1, replyFrom: [4n] },
    { kind: "stepCompleted", label: "triage", output: new Uint8Array([1]), agents: ["worker"] },
    { kind: "stepDispatched", label: "patch", attempt: 1, replyFrom: [] },
    { kind: "stepCompleted", label: "patch", output: new Uint8Array([2]), agents: ["worker"] },
    { kind: "stepCompensated", label: "patch" },
    { kind: "stepDispatched", label: "notify", attempt: 2, replyFrom: [4n, 9n] }
  ])
  assert.equal(journal.started, true)
  assert.deepEqual([...journal.outputs.keys()], ["triage"])
  assert.equal(journal.agents.get("triage")?.[0]?.asStr(), "worker")
  assert.equal(journal.attempts.get("patch"), 1)
  assert.deepEqual(journal.inFlight.get("notify"), { attempt: 2, replyFrom: [4n, 9n] })
  assert.equal(journal.inFlight.has("triage"), false)
})

void test("given_more_completions_than_the_default_context_window_when_folded_then_should_keep_every_one", () => {
  const entries = [
    { kind: "runStarted" as const, workflow: "flow" },
    ...Array.from({ length: 120 }, (_, index) => ({
      kind: "stepCompleted" as const,
      label: `step-${String(index)}`,
      output: new Uint8Array(),
      agents: []
    }))
  ]
  assert.equal(foldJournal(entries).outputs.size, 120)
})

void test("given_a_journal_entry_when_encoded_then_should_round_trip", () => {
  const entry = {
    kind: "stepDispatched" as const,
    label: "triage",
    attempt: 3,
    replyFrom: [4n, 9n]
  }
  assert.deepEqual(decodeJournalEntry(encodeJournalEntry(entry)), entry)
})

void test("given_a_rust_written_completion_when_decoded_then_should_read_its_integer_array_bytes", () => {
  const payload = encodeNamed(
    new Map([
      [
        "StepCompleted",
        new Map<string, unknown>([
          ["label", "triage"],
          ["output", [115, 101, 118]],
          ["agents", ["worker"]]
        ])
      ]
    ])
  )
  const entry = decodeJournalEntry(payload)
  assert.ok(entry.kind === "stepCompleted")
  assert.equal(new TextDecoder().decode(entry.output), "sev")
})

void test("given_a_dispatch_when_derived_then_should_stay_stable_and_differ_per_attempt", () => {
  const runId = ConversationId.new()
  const first = dispatchIds(runId, "a label longer than any idempotency key allows", 1)
  const again = dispatchIds(runId, "a label longer than any idempotency key allows", 1)
  const next = dispatchIds(runId, "a label longer than any idempotency key allows", 2)
  assert.equal(first.key, again.key)
  assert.equal(first.correlation.toString(), again.correlation.toString())
  assert.notEqual(first.key, next.key)
  assert.notEqual(first.correlation.toString(), first.conversation.toString())
})

void test("given_duplicate_labels_when_run_then_should_refuse_before_the_run_session_starts", async () => {
  const verbs: string[] = []
  const fake = fakeLaser({ sessions: fakeSessions(verbs) })
  await assert.rejects(
    Workflow.create(fake, "orchestrator")
      .step("a", routeTo(WORKER), () => new Uint8Array())
      .step("a", routeTo(WORKER), () => new Uint8Array())
      .run(),
    InvalidError
  )
  assert.deepEqual(verbs, [])
})

void test("given_a_step_without_a_reply_when_run_then_should_fail_it_typed_and_retryable", async () => {
  const fake = fakeLaser({
    contract: () => fakeContract(() => Promise.resolve({ kind: "timedOut" }))
  })
  await assert.rejects(
    Workflow.create(fake, "orchestrator")
      .step("triage", routeTo(WORKER), () => new Uint8Array())
      .run(),
    (error: unknown) =>
      error instanceof StepFailedError &&
      error.step === "triage" &&
      error.reason === "timed_out" &&
      isRetryable(error)
  )
})

void test("given_a_step_deadline_when_dispatched_then_should_bound_the_contract_by_it", async () => {
  const calls: ContractCalls = {}
  const fake = fakeLaser({
    contract: () => fakeContract(() => Promise.resolve(completed("done")), calls)
  })
  await Workflow.create(fake, "orchestrator")
    .step("slow", routeTo(WORKER), () => new Uint8Array())
    .deadline(90_000)
    .run()
  const deadline = calls.deadline as number
  assert.ok(deadline > 30_000 && deadline <= 90_000, `deadline ${String(deadline)}`)
  assert.equal(typeof calls.idempotencyKey, "string")
  assert.equal(calls.resumeFrom, undefined)
})

void test("given_a_rejected_reply_when_verified_then_should_compensate_that_step_too", async () => {
  const payloads: string[] = []
  const fake = fakeLaser({
    contract: () =>
      fakeContract((calls) => {
        payloads.push(new TextDecoder().decode(calls.payload as Uint8Array))
        return Promise.resolve(completed("done"))
      })
  })
  await assert.rejects(
    Workflow.create(fake, "orchestrator")
      .step("charge", routeTo(WORKER), () => new TextEncoder().encode("charge"))
      .verifyWith(() => false)
      .compensateWith(() => new TextEncoder().encode("refund"))
      .run(),
    (error: unknown) => error instanceof StepFailedError && error.reason === "unverified"
  )
  assert.deepEqual(payloads, ["charge", "refund"])
})

void test("given_a_failing_cancel_check_when_a_step_completed_then_should_compensate_before_failing", async () => {
  const payloads: string[] = []
  let checks = 0
  const checkError = new Error("the control read failed")
  const fake = fakeLaser({
    sessions: fakeSessions([], () => {
      checks += 1
      return checks === 1 ? Promise.resolve(false) : Promise.reject(checkError)
    }),
    contract: () =>
      fakeContract((calls) => {
        payloads.push(new TextDecoder().decode(calls.payload as Uint8Array))
        return Promise.resolve(completed("done"))
      })
  })
  await assert.rejects(
    Workflow.create(fake, "orchestrator")
      .step("charge", routeTo(WORKER), () => new TextEncoder().encode("charge"))
      .compensateWith(() => new TextEncoder().encode("refund"))
      .step("notify", routeTo(WORKER), () => new TextEncoder().encode("notify"))
      .run(),
    (error: unknown) => error === checkError
  )
  assert.deepEqual(payloads, ["charge", "refund"])
})

void test("given_a_lease_still_held_by_a_crashed_run_when_acquired_then_should_wait_and_proceed", async () => {
  let attempts = 0
  const fake = fakeLaser({
    kv: () => ({
      lease: () => {
        attempts += 1
        if (attempts === 1) {
          return Promise.reject(
            new KvExecutionError("held", { kind: "leaseHeld", expiresAtMicros: 5n })
          )
        }
        return Promise.resolve(lease(7n))
      },
      renewLeaseUntil: () => Promise.reject(new Error("an immediate contract must not renew")),
      release: () => Promise.resolve(true)
    }),
    contract: () => fakeContract(() => Promise.resolve(completed("committed")))
  })
  const outcome = await Workflow.create(fake, "orchestrator")
    .step("effect", routeTo(WORKER), () => new Uint8Array())
    .exclusive()
    .run()
  assert.equal(attempts, 2)
  assert.equal(new TextDecoder().decode(outcome.outputs.get("effect")), "committed")
})

void test("given_an_exclusive_namespace_when_dispatched_then_should_propagate_its_monotonic_fence", async () => {
  const events: string[] = []
  const leaseNamespaces: string[] = []
  const released: string[] = []
  const calls: ContractCalls = {}
  const fake = fakeLaser({
    kv: (namespace: string) => ({
      lease: () => {
        leaseNamespaces.push(namespace)
        return Promise.resolve(lease(41n))
      },
      renewLeaseUntil: () => Promise.reject(new Error("an immediate contract must not renew")),
      release: (_key: Uint8Array, holder: string) => {
        events.push("release")
        released.push(holder)
        return Promise.resolve(true)
      }
    }),
    contract: () => fakeContract(() => Promise.resolve(completed("committed")), calls),
    sendAgent: () => {
      events.push("journal")
      return Promise.resolve()
    }
  })

  const outcome = await Workflow.create(fake, "orchestrator")
    .step("effect", routeTo(WORKER), () => new TextEncoder().encode("apply"))
    .exclusiveIn("incident-effects")
    .run()
  assert.equal(new TextDecoder().decode(outcome.outputs.get("effect")), "committed")
  assert.deepEqual(leaseNamespaces, ["incident-effects"])
  assert.equal(calls.fence, 41n)
  assert.equal(released.length, 1)
  // The run start, the dispatch, the completion, then the release.
  assert.deepEqual(events, ["journal", "journal", "journal", "release"])
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
    const fake = fakeLaser({
      kv: () => ({
        lease: () =>
          Promise.resolve({
            token: 41n,
            grantedTtlMs: 100,
            position: { topicGeneration: 1n, partition: 0, offset: 1n },
            validUntilMs: 100
          }),
        renewLeaseUntil: async () => {
          events.push("renew-start")
          markRenewalStarted?.()
          await renewalResult
          events.push("renew-finish")
          return {
            token: 41n,
            grantedTtlMs: 60_000,
            position: { topicGeneration: 1n, partition: 0, offset: 2n },
            validUntilMs: 60_000
          }
        },
        release: () => {
          events.push("release")
          return Promise.resolve(true)
        }
      }),
      contract: () => fakeContract(() => contractResult),
      sendAgent: (_topic: string, payload: Uint8Array) => {
        const entry = decodeJournalEntry(payload)
        if (entry.kind === "stepCompleted") events.push("journal")
        return Promise.resolve()
      }
    })

    const running = Workflow.create(fake, "orchestrator")
      .step("effect", routeTo(WORKER), () => new Uint8Array())
      .exclusive()
      .run()
    const result = running.then(
      () => ({ ok: true as const }),
      (error: unknown) => ({ ok: false as const, error })
    )
    await renewalStarted
    assert.deepEqual(events, ["renew-start"])
    completeContract?.(completed("done"))
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
  const fences: unknown[] = []
  const excluded: unknown[] = []
  let sends = 0
  const fake = fakeLaser({
    kv: () => ({
      lease: (_key: Uint8Array, holder: string) => {
        holders.push(holder)
        return Promise.resolve(lease(BigInt(holders.length)))
      },
      renewLeaseUntil: () => Promise.reject(new Error("an immediate contract must not renew")),
      release: (_key: Uint8Array, holder: string) => {
        released.push(holder)
        return Promise.resolve(true)
      }
    }),
    contract: () =>
      fakeContract((calls) => {
        sends += 1
        fences.push(calls.fence)
        excluded.push(calls.excluding)
        return Promise.resolve(
          sends === 1 ? { kind: "timedOut" as const } : completed("reassigned")
        )
      })
  })

  const outcome = await Workflow.create(fake, "orchestrator")
    .step("effect", routeTo(WORKER), () => new TextEncoder().encode("apply"))
    .exclusive()
    .onTimeout("reassign")
    .run()
  assert.equal(new TextDecoder().decode(outcome.outputs.get("effect")), "reassigned")
  assert.equal(holders.length, 2)
  assert.notEqual(holders[0], holders[1])
  assert.deepEqual(released, holders)
  assert.deepEqual(fences, [1n, 2n])
  assert.deepEqual(
    (excluded[1] as readonly AgentId[]).map((agent) => agent.asStr()),
    ["worker"]
  )
})

void test("given_a_failed_exclusive_step_when_its_release_also_fails_then_should_surface_the_step_error", async () => {
  const stepError = new Error("the step failed")
  const fake = fakeLaser({
    kv: () => ({
      lease: () => Promise.resolve(lease(41n)),
      renewLeaseUntil: () => Promise.reject(new Error("an immediate failure must not renew")),
      release: () => Promise.reject(new Error("the release failed"))
    }),
    contract: () => fakeContract(() => Promise.reject(stepError))
  })

  await assert.rejects(
    Workflow.create(fake, "orchestrator")
      .step("effect", routeTo(WORKER), () => new Uint8Array())
      .exclusive()
      .run(),
    (error: unknown) => error === stepError
  )
})

void test(
  "given_an_ambiguous_shared_renewal_when_an_exclusive_step_renews_then_should_renew_through_the_coordinator_until_the_lease_deadline",
  { timeout: 5_000 },
  async () => {
    const deadlines: number[] = []
    let acquiredUntilMs: number | undefined
    let markRenewed: (() => void) | undefined
    const renewedOnce = new Promise<void>((resolve) => {
      markRenewed = resolve
    })
    const fake = fakeLaser({
      kv: () => ({
        lease: () => {
          const granted = lease(41n, 40) as { readonly validUntilMs: number }
          acquiredUntilMs = granted.validUntilMs
          return Promise.resolve(granted)
        },
        renewLease: () =>
          Promise.reject(
            new AmbiguousMutationError("renew failed mid-request", { operationId: 1n })
          ),
        renewLeaseUntil: (
          _key: Uint8Array,
          _holder: string,
          token: bigint,
          _ttlMs: number,
          deadlineMs: number
        ) => {
          deadlines.push(deadlineMs)
          markRenewed?.()
          return Promise.resolve(lease(token))
        },
        release: () => Promise.resolve(true)
      }),
      contract: () =>
        fakeContract(async () => {
          await renewedOnce
          return completed("committed")
        })
    })

    const outcome = await Workflow.create(fake, "orchestrator")
      .step("effect", routeTo(WORKER), () => new Uint8Array())
      .exclusive()
      .run()
    assert.equal(new TextDecoder().decode(outcome.outputs.get("effect")), "committed")
    assert.deepEqual(deadlines, [acquiredUntilMs], "the renewal runs until the lease deadline")
  }
)

void test("given_an_exclusive_lease_that_runs_out_during_renewal_when_dispatched_then_should_report_the_lease_lost", async () => {
  const fake = fakeLaser({
    kv: () => ({
      lease: () => Promise.resolve(lease(41n, 40)),
      renewLeaseUntil: () => new Promise(() => undefined),
      release: () => Promise.resolve(true)
    }),
    contract: () => fakeContract(() => new Promise<Contract>(() => undefined))
  })

  await assert.rejects(
    Workflow.create(fake, "orchestrator")
      .step("effect", routeTo(WORKER), () => new Uint8Array())
      .exclusive()
      .run(),
    (error: unknown) => isLeaseLost(error)
  )
})

void test("given_a_cancel_request_on_the_run_session_when_executed_then_should_cancel_and_name_the_run", async () => {
  const verbs: string[] = []
  const fake = fakeLaser({ sessions: fakeSessions(verbs, () => Promise.resolve(true)) })
  const runId = ConversationId.new()
  await assert.rejects(
    Workflow.create(fake, "orchestrator")
      .runId(runId)
      .step("effect", routeTo(WORKER), () => new TextEncoder().encode("apply"))
      .run(),
    (error: unknown) => error instanceof CancelledError && error.run === runId.toString()
  )
  assert.deepEqual(verbs, ["resume", "cancel"])
})

void test("given_a_verifier_that_never_settles_when_the_step_deadline_passes_then_should_fail_the_step_timed_out", async () => {
  const fake = fakeLaser({
    contract: () => fakeContract(() => Promise.resolve(completed("done")))
  })
  const run = Workflow.create(fake, "orchestrator")
    .step("judge", routeTo(WORKER), () => new Uint8Array())
    .verifyWith(() => new Promise<boolean>(() => undefined))
    .deadline(50)
    .run()
  let guard: ReturnType<typeof setTimeout> | undefined
  const hung = new Promise<"hung">((resolve) => {
    guard = setTimeout(() => {
      resolve("hung")
    }, 2_000)
  })
  try {
    const outcome = await Promise.race([
      run.then(
        () => "completed" as const,
        (error: unknown) => error
      ),
      hung
    ])
    assert.ok(
      outcome instanceof StepFailedError &&
        outcome.step === "judge" &&
        outcome.reason === "timed_out",
      `the hung verification ended as ${String(outcome)}`
    )
  } finally {
    clearTimeout(guard)
  }
})

import assert from "node:assert/strict"
import { createHash } from "node:crypto"
import { test } from "node:test"
import { utf8 } from "../../src/client/bytes.js"
import { BudgetExceededError, CancelledError, InvalidError } from "../../src/client/errors.js"
import { INTERNAL_REPLY_HUB, INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import {
  ConversationId,
  Workflow,
  WorkflowBudget,
  WorkflowRun,
  step,
  workflow
} from "../../src/index.js"
import {
  type JournalEntry,
  decodeJournalEntry,
  encodeJournalEntry
} from "../../src/agent/workflow.js"

const STEP_SETTLE_GRACE_MS = 5_000

void test("given_three_steps_when_the_workflow_is_called_then_should_return_its_value", async () => {
  const client = fakeClient()
  const ingest = step("ingest", () => ({ satellite: "sat-7", frames: 12 }))
  const analyze = step("analyze", async (batch: { satellite: string; frames: number }) => {
    await Promise.resolve()
    return { ...batch, anomalies: 2 }
  })
  const report = step(
    "report",
    (analysis: { satellite: string; frames: number; anomalies: number }) =>
      `${String(analysis.anomalies)} anomalies in ${String(analysis.frames)} frames from ${analysis.satellite}`
  )
  const pipeline = workflow("pipeline", async () => report(await analyze(await ingest())))

  const value = await pipeline.bind(client.laser)()

  assert.equal(value, "2 anomalies in 12 frames from sat-7")
  assert.deepEqual(journaled(client.journal), [
    ["runStarted"],
    ["stepCompleted", "ingest", '{"satellite":"sat-7","frames":12}'],
    ["stepCompleted", "analyze", '{"satellite":"sat-7","frames":12,"anomalies":2}'],
    ["stepCompleted", "report", '"2 anomalies in 12 frames from sat-7"']
  ])
  assert.deepEqual(client.verbs, ["begin", "end"])
})

void test("given_three_steps_when_run_then_should_report_run_id_status_steps_and_value", async () => {
  const client = fakeClient()
  const first = step("first", () => 1)
  const second = step("second", (value: number) => value + 1)
  const pipeline = workflow("pipeline", async () => second(await first()))

  const outcome = await pipeline.bind(client.laser).run()

  assert.ok(outcome.runId instanceof ConversationId)
  assert.equal(outcome.status, "completed")
  assert.deepEqual(outcome.completedSteps, ["first", "second"])
  assert.equal(outcome.value, 2)
})

void test("given_completed_steps_when_the_outcome_is_read_then_should_decode_json_and_carry_the_value", async () => {
  const client = fakeClient()
  const ingest = step("ingest", () => ({ satellite: "sat-7", frames: 12 }))
  const returned = { frames: 12, kept: new Set(["sat-7"]) }
  const pipeline = workflow("pipeline", async () => {
    await ingest()
    return returned
  })

  const outcome = await pipeline.bind(client.laser).run()

  assert.deepEqual(outcome.json("ingest"), { satellite: "sat-7", frames: 12 })
  assert.equal(outcome.json("absent"), undefined)
  assert.equal(outcome.value, returned)
  assert.equal(outcome.text("ingest"), '{"satellite":"sat-7","frames":12}')
})

void test("given_a_step_that_throws_when_run_then_should_reject_with_the_same_error_and_fail_the_run", async () => {
  const client = fakeClient()
  const failure = new Error("downlink lost")
  const ingest = step("ingest", () => 12)
  const analyze = step("analyze", (frames: number): number => {
    if (frames > 0) throw failure
    return frames
  })
  const pipeline = workflow("pipeline", async () => analyze(await ingest()))

  await assert.rejects(pipeline.bind(client.laser)(), (error: unknown) => error === failure)

  assert.deepEqual(journaled(client.journal), [["runStarted"], ["stepCompleted", "ingest", "12"]])
  assert.deepEqual(client.verbs, ["begin", "fail"])
})

void test("given_a_step_called_twice_when_run_then_should_journal_the_second_call_under_its_own_label", async () => {
  const client = fakeClient()
  const scan = step("scan", (band: string) => band)
  const pipeline = workflow("pipeline", async () => [
    await scan("s"),
    await scan("x"),
    await scan("k")
  ])

  const outcome = await pipeline.bind(client.laser).run()

  assert.deepEqual(outcome.completedSteps, ["scan", "scan#2", "scan#3"])
  assert.deepEqual(outcome.value, ["s", "x", "k"])
  assert.equal(outcome.json("scan#2"), "x")
})

void test("given_steps_started_together_when_run_then_should_label_them_in_call_order", async () => {
  const client = fakeClient()
  const scan = step("scan", async (band: string, delayMs: number) => {
    await new Promise((resolve) => setTimeout(resolve, delayMs))
    return band
  })
  const pipeline = workflow("pipeline", () => Promise.all([scan("slow", 20), scan("fast", 0)]))

  const outcome = await pipeline.bind(client.laser).run()

  assert.deepEqual(outcome.value, ["slow", "fast"])
  assert.equal(outcome.json("scan"), "slow")
  assert.equal(outcome.json("scan#2"), "fast")
  assert.deepEqual(outcome.completedSteps, ["scan#2", "scan"])
})

void test("given_a_step_called_outside_a_workflow_when_awaited_then_should_refuse_naming_both_ways_out", async () => {
  const ingest = step("ingest", () => 12)

  await assert.rejects(
    ingest(),
    (error: unknown) =>
      error instanceof InvalidError &&
      error.message ===
        "step `ingest` was called outside a workflow: call it inside a workflow function, or call ingest.fn(...) for the plain function"
  )
  assert.equal(ingest.fn(), 12)
})

void test("given_two_workflows_running_at_once_when_done_then_should_each_hold_only_their_own_steps", async () => {
  const pause = (milliseconds: number) =>
    new Promise((resolve) => setTimeout(resolve, milliseconds))
  const acquire = step("acquire", async () => {
    await pause(5)
    return "acquired"
  })
  const downlink = step("downlink", async () => {
    await pause(1)
    return "downlinked"
  })
  const calibrate = step("calibrate", () => "calibrated")
  const first = fakeClient()
  const second = fakeClient()
  const imaging = workflow("imaging", async () => {
    await acquire()
    await pause(5)
    return calibrate()
  })
  const telemetry = workflow("telemetry", async () => {
    await downlink()
    await pause(1)
    return downlink()
  })

  const [imaged, relayed] = await Promise.all([
    imaging.bind(first.laser).run(),
    telemetry.bind(second.laser).run()
  ])

  assert.deepEqual(imaged.completedSteps, ["acquire", "calibrate"])
  assert.deepEqual(relayed.completedSteps, ["downlink", "downlink#2"])
  assert.deepEqual(journaled(first.journal), [
    ["runStarted"],
    ["stepCompleted", "acquire", '"acquired"'],
    ["stepCompleted", "calibrate", '"calibrated"']
  ])
  assert.deepEqual(journaled(second.journal), [
    ["runStarted"],
    ["stepCompleted", "downlink", '"downlinked"'],
    ["stepCompleted", "downlink#2", '"downlinked"']
  ])
})

void test("given_a_step_returning_a_bigint_when_run_then_should_refuse_naming_the_step", async () => {
  const client = fakeClient()
  const count = step("count", () => 12n)
  const pipeline = workflow("pipeline", () => count())

  await assert.rejects(
    pipeline.bind(client.laser)(),
    (error: unknown) =>
      error instanceof InvalidError &&
      error.message ===
        "step `count` returned a value JSON cannot hold (a bigint). Return an object, array, string, number, boolean or null"
  )
  assert.deepEqual(journaled(client.journal), [["runStarted"]])
  assert.deepEqual(client.verbs, ["begin", "fail"])
})

void test("given_a_value_json_cannot_hold_when_a_step_returns_it_then_should_refuse_naming_what_it_is", async () => {
  const circular: Record<string, unknown> = {}
  circular["self"] = circular
  const refused: readonly (readonly [unknown, string])[] = [
    [undefined, "undefined"],
    [() => "plain", "a function"],
    [Symbol("frame"), "a symbol"],
    [{ frames: [1, 2n] }, "a bigint"],
    [{ decode: () => "plain" }, "a function"],
    [new Map([["sat-7", 12]]), "a Map"],
    [new Set(["sat-7"]), "a Set"],
    [new Uint8Array([7]), "a byte array"],
    [Number.NaN, "a number that is not finite"],
    [{ gain: Number.POSITIVE_INFINITY }, "a number that is not finite"],
    [circular, "a circular structure"]
  ]
  for (const [value, kind] of refused) {
    const client = fakeClient()
    const sample = step("sample", () => value)
    await assert.rejects(
      workflow("pipeline", () => sample()).bind(client.laser)(),
      (error: unknown) =>
        error instanceof InvalidError &&
        error.message ===
          `step \`sample\` returned a value JSON cannot hold (${kind}). Return an object, array, string, number, boolean or null`,
      kind
    )
    assert.deepEqual(journaled(client.journal), [["runStarted"]])
  }
})

void test("given_a_value_json_holds_when_a_step_returns_it_then_should_return_the_decoded_round_trip", async () => {
  const client = fakeClient()
  const observedAt = new Date("2026-11-18T06:30:00.000Z")
  const sample = step("sample", () => ({
    observedAt,
    frames: [1, undefined],
    skipped: undefined,
    band: null
  }))
  const pipeline = workflow("pipeline", () => sample())

  const value = await pipeline.bind(client.laser)()

  const decoded: string = value.observedAt
  assert.equal(decoded, "2026-11-18T06:30:00.000Z")
  assert.deepEqual(value, {
    observedAt: "2026-11-18T06:30:00.000Z",
    frames: [1, null],
    band: null
  })
})

void test("given_no_stream_configured_when_run_then_should_use_the_stream_named_after_the_workflow", async () => {
  const client = fakeClient()
  const ingest = step("ingest", () => 12)

  await withLaserStream(undefined, () => workflow("pipeline", () => ingest()).bind(client.laser)())

  assert.deepEqual(client.views, ["pipeline"])
  assert.deepEqual(client.verbs, ["begin", "end"])
})

void test("given_an_empty_laser_stream_when_run_then_should_count_it_as_unset", async () => {
  const client = fakeClient()
  const ingest = step("ingest", () => 12)

  await withLaserStream("", () => workflow("pipeline", () => ingest()).bind(client.laser)())

  assert.deepEqual(client.views, ["pipeline"])
})

void test("given_a_bound_client_with_a_default_stream_when_run_then_should_keep_it_and_stay_open", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const ingest = step("ingest", () => 12)

  await withLaserStream("from-env", () => workflow("pipeline", () => ingest()).bind(client.laser)())

  assert.deepEqual(client.views, [])
  assert.deepEqual(client.verbs, ["begin", "end"])
})

void test("given_laser_stream_and_a_client_without_a_default_when_run_then_should_use_laser_stream", async () => {
  const client = fakeClient()
  const ingest = step("ingest", () => 12)

  await withLaserStream("from-env", () => workflow("pipeline", () => ingest()).bind(client.laser)())

  assert.deepEqual(client.views, ["from-env"])
})

void test("given_a_stream_option_when_run_then_should_use_it_over_every_other_choice", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const ingest = step("ingest", () => 12)

  await withLaserStream("from-env", () =>
    workflow("pipeline", () => ingest(), { stream: "named" }).bind(client.laser)()
  )

  assert.deepEqual(client.views, ["named"])
})

void test("given_a_workflow_name_that_cannot_name_a_stream_when_run_then_should_refuse_naming_laser_stream", async () => {
  const client = fakeClient()
  const name = "s".repeat(256)
  const ingest = step("ingest", () => 12)

  await withLaserStream(undefined, () =>
    assert.rejects(
      workflow(name, () => ingest()).bind(client.laser)(),
      (error: unknown) =>
        error instanceof InvalidError &&
        error.message ===
          `workflow name \`${name}\` cannot name a stream: set LASER_STREAM, pass the stream option, or give the client a default stream`
    )
  )

  assert.deepEqual([client.views, client.verbs, client.journal], [[], [], []])
})

void test("given_a_client_without_a_default_stream_when_run_with_a_body_then_should_use_laser_stream_or_the_workflow_name", async () => {
  const cases: readonly (readonly [string | undefined, string])[] = [
    [undefined, "pipeline"],
    ["from-env", "from-env"],
    ["", "pipeline"]
  ]
  for (const [configured, stream] of cases) {
    const client = fakeClient()

    const outcome = await withLaserStream(configured, () =>
      Workflow.create(client.laser, "pipeline").runWith((run) => run.step("ingest", () => 12))
    )

    assert.equal(outcome.value, 12)
    assert.deepEqual([client.views, client.verbs], [[stream], ["begin", "end"]])
  }
})

void test("given_an_unbound_workflow_when_the_run_ends_then_should_close_its_connection", async (context) => {
  const client = fakeClient({ defaultStream: "fleet" })
  context.mock.method(Laser, "connectEnv", () => Promise.resolve(client.laser))
  const ingest = step("ingest", () => 12)

  assert.equal(await workflow("pipeline", () => ingest())(), 12)

  assert.deepEqual(client.verbs, ["begin", "end", "close"])
})

void test("given_an_unbound_workflow_when_the_run_fails_then_should_close_its_connection_and_keep_the_error", async (context) => {
  const client = fakeClient({ defaultStream: "fleet" })
  context.mock.method(Laser, "connectEnv", () => Promise.resolve(client.laser))
  const failure = new Error("downlink lost")
  const ingest = step("ingest", (): number => {
    throw failure
  })

  await assert.rejects(
    workflow("pipeline", () => ingest())(),
    (error: unknown) => error === failure
  )

  assert.deepEqual(client.verbs, ["begin", "fail", "close"])
})

void test("given_a_workflow_with_builder_steps_when_run_with_a_body_then_should_refuse_before_anything_is_written", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const builder = Workflow.create(client.laser, "pipeline")
  builder.stepLocal("ingest", () => "frames")

  await assert.rejects(
    builder.runWith(() => "value"),
    (error: unknown) =>
      error instanceof InvalidError &&
      error.message ===
        "workflow `pipeline` declares builder steps, so it cannot run a body: use the builder steps or runWith, not both"
  )

  assert.deepEqual([client.verbs, client.journal], [[], []])
})

void test("given_a_cancel_request_when_the_next_step_starts_then_should_stop_before_its_body", async () => {
  let checks = 0
  const client = fakeClient({
    defaultStream: "fleet",
    cancelRequested: () => {
      checks += 1
      return Promise.resolve(checks > 1)
    }
  })
  const ran: string[] = []
  const ingest = step("ingest", () => {
    ran.push("ingest")
    return 12
  })
  const analyze = step("analyze", (frames: number) => {
    ran.push("analyze")
    return frames
  })
  const pipeline = workflow("pipeline", async () => analyze(await ingest()))

  await assert.rejects(pipeline.bind(client.laser)(), CancelledError)

  assert.deepEqual(ran, ["ingest"])
  assert.deepEqual(journaled(client.journal), [["runStarted"], ["stepCompleted", "ingest", "12"]])
  assert.deepEqual(client.verbs, ["begin", "cancel"])
})

void test("given_a_spent_invocation_budget_when_the_next_step_starts_then_should_stop_before_its_body", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const ran: string[] = []

  await assert.rejects(
    Workflow.create(client.laser, "pipeline")
      .budget(WorkflowBudget.unlimited().invocations(1))
      .runWith(async (run) => {
        await run.step("ingest", () => ran.push("ingest"))
        await run.step("analyze", () => ran.push("analyze"))
      }),
    BudgetExceededError
  )

  assert.deepEqual(ran, ["ingest"])
  assert.deepEqual(client.verbs, ["begin", "failOverBudget"])
})

void test("given_a_step_still_running_when_the_body_fails_then_should_end_the_run_after_it_settles", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const failure = new Error("downlink lost")
  const slow = step("slow", async () => {
    await new Promise((resolve) => setTimeout(resolve, 20))
    client.verbs.push("slow finished")
    return 1
  })
  const broken = step("broken", (): number => {
    throw failure
  })
  const pipeline = workflow("pipeline", () => Promise.all([slow(), broken()]))

  await assert.rejects(pipeline.bind(client.laser)(), (error: unknown) => error === failure)

  assert.deepEqual(client.verbs, ["begin", "slow finished", "fail"])
  assert.deepEqual(journaled(client.journal), [["runStarted"], ["stepCompleted", "slow", "1"]])
})

void test("given_a_run_that_ended_when_a_step_is_called_then_should_refuse_without_running_it", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  let ended: WorkflowRun | undefined
  await Workflow.create(client.laser, "pipeline").runWith((run) => {
    ended = run
  })
  let ran = false

  await assert.rejects(
    ended?.step("late", () => {
      ran = true
      return 1
    }) ?? Promise.resolve(),
    (error: unknown) =>
      error instanceof InvalidError &&
      error.message === "step `late` was called after its workflow run ended"
  )

  assert.equal(ran, false)
})

void test("given_a_step_called_without_awaiting_when_the_body_returns_then_should_run_and_record_it", async () => {
  const client = fakeClient({ defaultStream: "fleet" })

  const outcome = await Workflow.create(client.laser, "pipeline").runWith((run) => {
    void run.step("late", () => 1)
  })

  assert.deepEqual(journaled(client.journal), [["runStarted"], ["stepCompleted", "late", "1"]])
  assert.deepEqual(outcome.completedSteps, ["late"])
  assert.deepEqual(client.verbs, ["begin", "end"])
})

void test("given_a_body_when_run_with_then_should_run_its_steps_and_expose_the_run", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const ingest = step("ingest", () => 12)
  const seen: ConversationId[] = []

  const outcome = await Workflow.create(client.laser, "pipeline").runWith(async (run) => {
    seen.push(run.runId)
    const frames = await run.step("frames", () => 6)
    return frames + (await ingest())
  })

  assert.equal(outcome.value, 18)
  assert.deepEqual(seen, [outcome.runId])
  assert.deepEqual(outcome.completedSteps, ["frames", "ingest"])
})

void test("given_fields_that_may_be_absent_when_a_step_returns_them_then_should_read_them_as_optional", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const sample = step("sample", (): { band?: string; gain: number | undefined } => ({
    gain: undefined
  }))
  const accepts = (reading: { band?: string; gain?: number }) => reading
  const pipeline = workflow("pipeline", async () => accepts(await sample()))

  assert.deepEqual(await pipeline.bind(client.laser)(), {})
})

void test("given_arguments_when_a_workflow_is_called_run_and_resumed_then_should_pass_them_to_its_function", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const scan = step("scan", (band: string, frames: number) => `${band}:${String(frames)}`)
  const pipeline = workflow("pipeline", (band: string, frames: number) => scan(band, frames)).bind(
    client.laser
  )

  const called = await pipeline("s-band", 12)
  const run = await pipeline.run("x-band", 6)
  const resumed = await pipeline.resume(ConversationId.new(), "k-band", 3)

  assert.deepEqual([called, run.value, resumed.value], ["s-band:12", "x-band:6", "k-band:3"])
  assert.equal(scan.name, "scan")
  assert.equal(pipeline.name, "pipeline")
  assert.equal(scan.fn("l-band", 1), "l-band:1")
})

void test("given_an_aborted_signal_when_a_run_step_is_called_then_should_stop_before_its_body", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const controller = new AbortController()
  controller.abort()
  let ran = false

  await assert.rejects(
    Workflow.create(client.laser, "pipeline").runWith((run) =>
      run.step(
        "ingest",
        () => {
          ran = true
          return 12
        },
        { signal: controller.signal }
      )
    ),
    CancelledError
  )

  assert.equal(ran, false)
  assert.deepEqual(client.verbs, ["begin"])
})

void test("given_an_aborted_run_signal_when_the_next_step_starts_then_should_write_no_terminal_state", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const controller = new AbortController()
  const ran: string[] = []

  await assert.rejects(
    Workflow.create(client.laser, "pipeline").runWith(
      async (run) => {
        await run.step("ingest", () => {
          ran.push("ingest")
          controller.abort()
          return 12
        })
        await run.step("analyze", () => ran.push("analyze"))
      },
      { signal: controller.signal }
    ),
    CancelledError
  )

  assert.deepEqual(ran, ["ingest"])
  assert.deepEqual(client.verbs, ["begin"])
})

void test("given_a_check_that_refused_a_step_when_retried_then_should_take_its_label_again", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const controller = new AbortController()
  controller.abort()

  const outcome = await Workflow.create(client.laser, "pipeline").runWith(async (run) => {
    await assert.rejects(
      run.step("ingest", () => 1, { signal: controller.signal }),
      CancelledError
    )
    return run.step("ingest", () => 2)
  })

  assert.equal(outcome.value, 2)
  assert.deepEqual(outcome.completedSteps, ["ingest"])
})

void test("given_a_cancel_request_that_refused_a_step_when_called_again_then_should_take_its_label_again", async () => {
  let checks = 0
  const client = fakeClient({
    defaultStream: "fleet",
    cancelRequested: () => {
      checks += 1
      return Promise.resolve(checks === 1)
    }
  })

  const outcome = await Workflow.create(client.laser, "pipeline").runWith(async (run) => {
    await assert.rejects(
      run.step("ingest", () => 1),
      CancelledError
    )
    return run.step("ingest", () => 2)
  })

  assert.equal(outcome.value, 2)
  assert.deepEqual(outcome.completedSteps, ["ingest"])
})

void test("given_a_builder_run_when_its_signal_aborts_then_should_end_the_run_as_canceled", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const controller = new AbortController()

  await assert.rejects(
    Workflow.create(client.laser, "pipeline")
      .stepLocal("ingest", () => {
        controller.abort()
        return "frames"
      })
      .stepLocal("analyze", () => "must not run")
      .after("ingest")
      .run({ signal: controller.signal }),
    CancelledError
  )

  assert.deepEqual(journaled(client.journal), [
    ["runStarted"],
    ["stepCompleted", "ingest", "frames"]
  ])
  assert.deepEqual(client.verbs, ["begin", "cancel"])
})

void test("given_a_step_body_that_failed_when_retried_then_should_take_its_label_again", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  let attempts = 0
  const uplink = step("uplink", () => {
    attempts += 1
    if (attempts === 1) throw new Error("no carrier")
    return attempts
  })
  const pipeline = workflow("pipeline", async () => {
    await assert.rejects(uplink())
    return [await uplink(), await uplink()]
  })

  const outcome = await pipeline.bind(client.laser).run()

  assert.deepEqual(outcome.value, [2, 3])
  assert.deepEqual(outcome.completedSteps, ["uplink", "uplink#2"])
})

void test("given_a_step_that_never_settles_when_another_fails_then_should_reject_with_that_failure_within_the_bound", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] })
  const client = fakeClient({ defaultStream: "fleet" })
  const failure = new Error("downlink lost")
  const never = step("never", () => new Promise<number>(() => undefined))
  const broken = step("broken", (): number => {
    throw failure
  })
  const pipeline = workflow("pipeline", () => Promise.all([never(), broken()]))
  let rejected: unknown
  void pipeline
    .bind(client.laser)()
    .catch((error: unknown) => {
      rejected = error
    })

  await drained()
  context.mock.timers.tick(STEP_SETTLE_GRACE_MS - 1)
  await drained()
  assert.equal(rejected, undefined)
  context.mock.timers.tick(1)
  await drained()

  assert.equal(rejected, failure)
  assert.deepEqual(client.verbs, ["begin", "fail"])
})

void test("given_a_step_that_finishes_after_the_bound_when_the_run_ended_then_should_not_journal_it", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] })
  const client = fakeClient({ defaultStream: "fleet" })
  let finish: (value: number) => void = () => undefined
  let began: () => void = () => undefined
  const running = new Promise<void>((resolve) => (began = resolve))
  const late = step(
    "late",
    () =>
      new Promise<number>((resolve) => {
        finish = resolve
        began()
      })
  )
  let lateCall: Promise<number> | undefined
  const pipeline = workflow("pipeline", async () => {
    lateCall = late()
    await running
    return "returned"
  })
  let value: unknown
  void pipeline
    .bind(client.laser)()
    .then((returned) => {
      value = returned
    })

  await drained()
  context.mock.timers.tick(STEP_SETTLE_GRACE_MS)
  await drained()
  assert.equal(value, "returned")
  finish(12)

  await assert.rejects(
    lateCall ?? Promise.resolve(),
    (error: unknown) =>
      error instanceof InvalidError &&
      error.message === "step `late` finished after its workflow run ended"
  )
  assert.deepEqual(journaled(client.journal), [["runStarted"]])
})

void test("given_a_step_record_still_being_written_when_the_bound_passes_then_should_end_the_run_after_the_record_lands", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] })
  let land: () => void = () => undefined
  const landed = new Promise<void>((resolve) => (land = resolve))
  const client = fakeClient({
    defaultStream: "fleet",
    journalWrite: (entry) => (entry.kind === "stepCompleted" ? landed : Promise.resolve())
  })
  let bodyRan: () => void = () => undefined
  const uplinked = new Promise<void>((resolve) => (bodyRan = resolve))
  const running = Workflow.create(client.laser, "pipeline").runWith(async (run) => {
    void run.step("uplink", () => {
      bodyRan()
      return "sent"
    })
    await uplinked
  })

  await drained()
  context.mock.timers.tick(STEP_SETTLE_GRACE_MS)
  await drained()
  const held = [journaled(client.journal), [...client.verbs]]
  land()
  const outcome = await running

  assert.deepEqual(held, [[["runStarted"]], ["begin"]])
  assert.deepEqual(journaled(client.journal), [
    ["runStarted"],
    ["stepCompleted", "uplink", '"sent"']
  ])
  assert.deepEqual(client.verbs, ["begin", "end"])
  assert.deepEqual(outcome.completedSteps, ["uplink"])
})

void test("given_a_step_call_when_journaled_then_should_record_the_digest_of_its_arguments", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const scan = step("scan", (band: string, frames: number | bigint) => `${band}:${String(frames)}`)
  const pipeline = workflow("pipeline", async () => [await scan("s-band", 12), await scan("x", 1n)])

  await pipeline.bind(client.laser)()

  assert.deepEqual(
    client.journal.flatMap((entry) => (entry.kind === "stepCompleted" ? [entry.input] : [])),
    [createHash("sha256").update('["s-band",12]').digest("hex").slice(0, 32), undefined]
  )
})

void test("given_a_run_step_and_a_builder_step_when_journaled_then_should_record_no_input", async () => {
  const client = fakeClient({ defaultStream: "fleet" })

  await Workflow.create(client.laser, "pipeline").runWith((run) => run.step("ingest", () => 12))
  await Workflow.create(client.laser, "pipeline")
    .stepLocal("ingest", () => "frames")
    .run()

  for (const entry of client.journal) {
    assert.equal("input" in entry, false)
  }
})

void test("given_a_completed_step_when_its_journal_record_is_encoded_then_should_keep_its_input_only_when_present", () => {
  const plain = { kind: "stepCompleted" as const, label: "scan", output: utf8("1"), agents: [] }
  const digested = { ...plain, input: "0123456789abcdef0123456789abcdef" }

  assert.deepEqual(decodeJournalEntry(encodeJournalEntry(digested)), digested)
  assert.deepEqual(decodeJournalEntry(encodeJournalEntry(plain)), plain)
})

void test("given_arguments_with_keys_in_another_order_when_digested_then_should_match_the_canonical_digest", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const scan = step("scan", (reading: unknown) => typeof reading)
  const pipeline = workflow("pipeline", async () => {
    await scan({ b: 1, a: "é" })
    await scan({ z: { y: 1, x: [{ b: 2, a: 3 }] }, a: null })
    await scan({ "\u{1F600}": 1, "\uffff": 2 })
  })

  await pipeline.bind(client.laser)()

  assert.deepEqual(
    client.journal.flatMap((entry) => (entry.kind === "stepCompleted" ? [entry.input] : [])),
    [
      "45f2c633499b74ab1aea24a8ff7e8f47",
      "d1c5afb6579d41acd212cce78d25f1a6",
      "2db473626f8575084cc9ebc0ca0279e6"
    ]
  )
})

void test("given_numbers_when_digested_then_should_match_the_digest_every_client_computes", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const scan = step("scan", (...readings: number[]) => readings.length)
  const pipeline = workflow("pipeline", () =>
    scan(1.0, 1.5e-7, 0.00001, 1e21, 2 ** 60, 0.1, -2.5, -0, 5e-324)
  )

  await pipeline.bind(client.laser)()

  // SHA-256 of [1,1.5e-7,0.00001,1000000000000000000000,1152921504606846976,0.1,-2.5,0,5e-324]
  assert.deepEqual(
    client.journal.flatMap((entry) => (entry.kind === "stepCompleted" ? [entry.input] : [])),
    ["ae96dd6c224f98f4ae7b89f2f91439db"]
  )
})

void test("given_a_number_that_is_not_finite_when_digested_then_should_record_no_digest", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const scan = step("scan", (reading: unknown) => typeof reading)
  const pipeline = workflow("pipeline", async () => {
    await scan(Number.NaN)
    await scan(Number.POSITIVE_INFINITY)
    await scan({ gain: [Number.NEGATIVE_INFINITY] })
    await scan(null)
  })

  await pipeline.bind(client.laser)()

  assert.deepEqual(
    client.journal.flatMap((entry) => (entry.kind === "stepCompleted" ? ["input" in entry] : [])),
    [false, false, false, true]
  )
})

void test("given_arguments_json_cannot_hold_when_digested_then_should_record_a_digest_only_for_plain_json_data", async () => {
  class Reading {
    constructor(readonly gain: number) {}
  }
  class Frame {
    toJSON(): { readonly frames: number } {
      return { frames: 12 }
    }
  }
  const bare: unknown = Object.setPrototypeOf({ band: "s" }, null)
  const circular: Record<string, unknown> = {}
  circular["self"] = circular
  const unreadable = {
    toJSON: (): never => {
      throw new Error("no text")
    }
  }
  const calls: readonly (readonly [readonly unknown[], string | undefined])[] = [
    [[null], "[null]"],
    [
      [true, 1.5, "s-band", [1, [2]], { band: "s", gain: { db: 3 } }],
      '[true,1.5,"s-band",[1,[2]],{"band":"s","gain":{"db":3}}]'
    ],
    [[bare], '[{"band":"s"}]'],
    [[{ band: undefined, decode: () => 1, tag: Symbol("s") }], "[{}]"],
    [[new Date("2026-11-18T06:30:00.000Z")], '["2026-11-18T06:30:00.000Z"]'],
    [[new Frame()], '[{"frames":12}]'],
    [[undefined], undefined],
    [[() => 1], undefined],
    [[Symbol("frame")], undefined],
    [[[1, undefined]], undefined],
    [[[() => 1]], undefined],
    [[[Symbol("frame")]], undefined],
    [[new Map([["band", "s"]])], undefined],
    [[new Set([1])], undefined],
    [[new Uint8Array([7])], undefined],
    [[new Reading(3)], undefined],
    [[12n], undefined],
    [[circular], undefined],
    [[unreadable], undefined],
    [[Number.NaN], undefined]
  ]
  const client = fakeClient({ defaultStream: "fleet" })
  const scan = step("scan", (...readings: unknown[]) => readings.length)
  const pipeline = workflow("pipeline", async () => {
    for (const [readings] of calls) await scan(...readings)
  })

  await pipeline.bind(client.laser)()

  assert.deepEqual(
    client.journal.flatMap((entry) => (entry.kind === "stepCompleted" ? [entry.input] : [])),
    calls.map(([, canonical]) => (canonical === undefined ? undefined : inputDigest(canonical)))
  )
})

void test("given_a_step_that_throws_the_sdk_cancelled_error_when_run_then_should_end_the_run_as_canceled", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const stopped = new CancelledError("the downlink window closed")
  const ingest = step("ingest", (): number => {
    throw stopped
  })

  await assert.rejects(
    workflow("pipeline", () => ingest()).bind(client.laser)(),
    (error: unknown) => error === stopped
  )

  assert.deepEqual(client.verbs, ["begin", "cancel"])
})

void test("given_an_aborted_run_signal_when_a_step_body_throws_cancelled_then_should_write_no_terminal_state", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const controller = new AbortController()

  await assert.rejects(
    Workflow.create(client.laser, "pipeline").runWith(
      (run) =>
        run.step("ingest", (): number => {
          controller.abort()
          throw new CancelledError("the read was aborted")
        }),
      { signal: controller.signal }
    ),
    CancelledError
  )

  assert.deepEqual(client.verbs, ["begin"])
})

void test("given_a_step_that_threw_cancelled_when_the_run_signal_aborts_during_the_settle_then_should_end_the_run_as_canceled", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout"] })
  const client = fakeClient({ defaultStream: "fleet" })
  const controller = new AbortController()
  const stopped = new CancelledError("the downlink window closed")
  let rejected: unknown
  void Workflow.create(client.laser, "pipeline")
    .runWith(
      (run) =>
        Promise.all([
          run.step("downlink", () => new Promise<number>(() => undefined)),
          run.step("uplink", (): number => {
            throw stopped
          })
        ]),
      { signal: controller.signal }
    )
    .catch((error: unknown) => {
      rejected = error
    })

  await drained()
  context.mock.timers.tick(1_000)
  controller.abort()
  context.mock.timers.tick(STEP_SETTLE_GRACE_MS - 1_000)
  await drained()

  assert.equal(rejected, stopped)
  assert.deepEqual(client.verbs, ["begin", "cancel"])
})

void test("given_an_aborted_run_signal_when_a_step_body_throws_another_error_then_should_write_no_terminal_state", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const controller = new AbortController()
  const failure = new DOMException("the read was aborted", "AbortError")

  await assert.rejects(
    Workflow.create(client.laser, "pipeline").runWith(
      (run) =>
        run.step("ingest", (): number => {
          controller.abort()
          throw failure
        }),
      { signal: controller.signal }
    ),
    (error: unknown) => error === failure
  )

  assert.deepEqual(client.verbs, ["begin"])
})

void test("given_an_aborted_step_signal_when_its_body_throws_cancelled_then_should_write_no_terminal_state", async () => {
  const client = fakeClient({ defaultStream: "fleet" })
  const controller = new AbortController()

  await assert.rejects(
    Workflow.create(client.laser, "pipeline").runWith((run) =>
      run.step(
        "ingest",
        (): number => {
          controller.abort()
          throw new CancelledError("the read was aborted")
        },
        { signal: controller.signal }
      )
    ),
    CancelledError
  )

  assert.deepEqual(client.verbs, ["begin"])
})

void test("given_a_step_name_holding_the_call_separator_when_called_then_should_refuse_it", async () => {
  const refusal = (error: unknown): boolean =>
    error instanceof InvalidError &&
    error.message ===
      "workflow step name `scan#2` cannot contain `#`, the journal numbers repeated calls with it"
  const client = fakeClient({ defaultStream: "fleet" })
  let ran = false
  const body = () => {
    ran = true
    return 12
  }
  const scan = step("scan#2", body)

  await assert.rejects(workflow("pipeline", () => scan()).bind(client.laser)(), refusal)
  await assert.rejects(
    Workflow.create(client.laser, "pipeline").runWith((run) => run.step("scan#2", body)),
    refusal
  )

  assert.equal(ran, false)
  assert.deepEqual(journaled(client.journal), [["runStarted"], ["runStarted"]])
  assert.deepEqual(client.verbs, ["begin", "fail", "begin", "fail"])
})

void test("given_a_record_under_the_second_label_when_two_calls_resume_then_should_run_the_first_under_the_free_label", async () => {
  const { run, written } = resumedRun([["downlink#2", "frame-2"]])

  const values = await Promise.all([
    run.step("downlink", () => "frame-1"),
    run.step("downlink", () => "must not run")
  ])

  assert.deepEqual(values, ["frame-1", "frame-2"])
  assert.deepEqual(written, ["downlink"])
})

void test("given_records_with_and_without_digests_when_calls_resume_then_should_claim_equal_arguments_first_and_walk_the_labels_for_the_rest", async () => {
  const { run, written } = resumedRun([
    ["scan#2", "recorded without arguments"],
    ["scan#3", "recorded for x-band", inputDigest('["x-band"]')],
    ["scan#4", "recorded for q-band", inputDigest('["q-band"]')]
  ])
  const scan = (band: string) => run.callStep("scan", [band], () => `fresh for ${band}`)

  const values = [
    await scan("x-band"),
    await scan("s-band"),
    await scan("k-band"),
    await scan("l-band"),
    await run.step("scan", () => "must not run")
  ]

  assert.deepEqual(values, [
    "recorded for x-band",
    "fresh for s-band",
    "recorded without arguments",
    "fresh for l-band",
    "recorded for q-band"
  ])
  assert.deepEqual(written, ["scan", "scan#5"])
})

void test("given_the_second_of_three_calls_failed_when_called_again_then_should_take_the_released_label_before_a_new_one", async () => {
  const { run, written } = resumedRun([])
  const failure = new Error("no carrier")

  const settled = await Promise.allSettled([
    run.step("uplink", () => 1),
    run.step("uplink", (): number => {
      throw failure
    }),
    run.step("uplink", () => 3)
  ])
  const retried = [await run.step("uplink", () => 2), await run.step("uplink", () => 4)]

  assert.deepEqual(settled, [
    { status: "fulfilled", value: 1 },
    { status: "rejected", reason: failure },
    { status: "fulfilled", value: 3 }
  ])
  assert.deepEqual(retried, [2, 4])
  assert.deepEqual(written, ["uplink", "uplink#3", "uplink#2", "uplink#4"])
})

void test("given_large_recorded_call_numbers_when_arguments_match_then_should_claim_in_numeric_order", async () => {
  const digest = inputDigest("[]")
  const { run, written } = resumedRun([
    ["scan#100000000000000000000", 3, digest],
    ["scan#99999999999999999999", 2, digest],
    ["scan#4294967296", 1, digest]
  ])
  const scan = () => run.callStep("scan", [], () => 4)
  assert.deepEqual([await scan(), await scan(), await scan(), await scan()], [1, 2, 3, 4])
  assert.deepEqual(written, ["scan"])
})

void test("given_recorded_labels_when_resumed_then_should_number_only_a_label_ending_in_a_whole_number_above_one", async () => {
  const named = resumedRun([
    ["scan#x", "not a call of scan"],
    ["scan#1", "not a call of scan"],
    ["scan#2.0", "not a call of scan"],
    ["scan#2", "second"]
  ])
  const unnamed = resumedRun([
    ["", "first"],
    ["#2", "second"]
  ])

  const values = [
    await named.run.step("scan", () => "first"),
    await named.run.step("scan", () => "must not run"),
    await named.run.step("scan", () => "third"),
    await unnamed.run.step("", () => "must not run"),
    await unnamed.run.step("", () => "must not run")
  ]

  assert.deepEqual(values, ["first", "second", "third", "first", "second"])
  assert.deepEqual([named.written, unnamed.written], [["scan", "scan#3"], []])
})

// Lets every promise that can settle without a timer settle.
function drained(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve))
}

// Each journal record as its kind, and for a completed step its label and output text.
function journaled(journal: readonly JournalEntry[]): readonly (readonly string[])[] {
  return journal.map((entry) =>
    entry.kind === "stepCompleted"
      ? [entry.kind, entry.label, new TextDecoder().decode(entry.output)]
      : [entry.kind]
  )
}

// The digest a step call records for arguments with this canonical JSON.
function inputDigest(canonical: string): string {
  return createHash("sha256").update(canonical).digest("hex").slice(0, 32)
}

// A run that opens with `recorded` as its journal, each entry a label, its value and the digest of its arguments.
function resumedRun(recorded: readonly (readonly [string, unknown, string?])[]): {
  readonly run: WorkflowRun
  readonly written: string[]
} {
  const written: string[] = []
  const run = WorkflowRun.create(
    ConversationId.new(),
    new Map(recorded.map(([label, value]) => [label, utf8(JSON.stringify(value))])),
    new Map(
      recorded.flatMap(([label, , input]) => (input === undefined ? [] : [[label, input] as const]))
    ),
    () => Promise.resolve(),
    (label) => {
      written.push(label)
      return Promise.resolve()
    }
  )
  return { run, written }
}

// Runs `work` with `LASER_STREAM` set to `value`, or removed for `undefined`.
async function withLaserStream<Result>(
  value: string | undefined,
  work: () => Promise<Result>
): Promise<Result> {
  const previous = process.env["LASER_STREAM"]
  if (value === undefined) delete process.env["LASER_STREAM"]
  else process.env["LASER_STREAM"] = value
  try {
    return await work()
  } finally {
    if (previous === undefined) delete process.env["LASER_STREAM"]
    else process.env["LASER_STREAM"] = previous
  }
}

interface FakeClientOptions {
  readonly defaultStream?: string
  readonly cancelRequested?: () => Promise<boolean>
  // Resolves when the journal write of `entry` may land.
  readonly journalWrite?: (entry: JournalEntry) => Promise<void>
}

interface FakeClient {
  readonly laser: Laser
  readonly journal: JournalEntry[]
  readonly verbs: string[]
  readonly views: string[]
}

// A connection that decodes every journal write and records the run session's lifecycle.
function fakeClient(options: FakeClientOptions = {}): FakeClient {
  const journal: JournalEntry[] = []
  const verbs: string[] = []
  const views: string[] = []
  const lifecycle = (verb: string) => () => {
    verbs.push(verb)
    return Promise.resolve()
  }
  const session = {
    cancelRequested: options.cancelRequested ?? (() => Promise.resolve(false)),
    budgetBreach: () => Promise.resolve(undefined),
    end: lifecycle("end"),
    cancel: lifecycle("cancel"),
    fail: lifecycle("fail"),
    failOverBudget: lifecycle("failOverBudget")
  }
  const opened = (verb: string) => () => {
    verbs.push(verb)
    return Promise.resolve({ session, lease: { release: () => undefined } })
  }
  const builder = {
    withId: () => builder,
    agent: () => builder,
    budget: () => builder,
    begin: opened("begin"),
    resume: opened("resume")
  }
  const view = (defaultStream: string | undefined): Laser =>
    ({
      defaultStream,
      topic: () => ({
        partitionCount: () => Promise.resolve(undefined),
        tailOffsets: () => Promise.resolve(new Map())
      }),
      [INTERNAL_REPLY_HUB]: () => Promise.resolve({ positions: () => Promise.resolve([]) }),
      [INTERNAL_TRANSPORT]: () => ({ pollMessages: () => Promise.resolve([]) }),
      sessions: () => ({ start: () => builder }),
      sendAgent: async (_topic: string, payload: Uint8Array) => {
        const entry = decodeJournalEntry(payload)
        await options.journalWrite?.(entry)
        journal.push(entry)
      },
      withDefaultStream: (stream: string) => {
        views.push(stream)
        return view(stream)
      },
      close: lifecycle("close")
    }) as unknown as Laser
  return { laser: view(options.defaultStream), journal, verbs, views }
}

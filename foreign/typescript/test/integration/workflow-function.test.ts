import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { type JournalEntry, decodeJournalEntry } from "../../src/agent/workflow.js"
import { CancelledError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { LastN, replayContext } from "../../src/context.js"
import { ConversationId, type WorkflowRun, step, workflow } from "../../src/index.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

void test("given_a_recorded_step_when_resumed_then_should_return_its_value_without_running_it", async () => {
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, uniqueName())
  const ran: string[] = []
  const ingest = step("ingest", () => {
    ran.push("ingest")
    return { satellite: "sat-7", frames: 12 }
  })
  const report = step("report", (batch: { satellite: string; frames: number }) => {
    ran.push("report")
    return `${String(batch.frames)} frames from ${batch.satellite}`
  })
  const pipeline = workflow("pipeline", async () => report(await ingest())).bind(laser)

  const first = await pipeline.run()
  const resumed = await pipeline.resume(first.runId)
  const resumedByText = await pipeline.resume(first.runId.toString())

  assert.deepEqual(ran, ["ingest", "report"])
  assert.equal(resumed.value, "12 frames from sat-7")
  assert.equal(resumedByText.value, "12 frames from sat-7")
  assert.deepEqual(resumed.completedSteps, ["ingest", "report"])
  assert.equal(resumed.runId.toString(), first.runId.toString())
})

void test("given_a_failed_run_when_resumed_then_should_run_only_the_failed_step_and_what_follows", async () => {
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, uniqueName())
  const ran: string[] = []
  const failure = new Error("downlink lost")
  let healthy = false
  const ingest = step("ingest", () => {
    ran.push("ingest")
    return 12
  })
  const analyze = step("analyze", (frames: number) => {
    ran.push("analyze")
    if (!healthy) throw failure
    return frames / 6
  })
  const report = step("report", (anomalies: number) => {
    ran.push("report")
    return `${String(anomalies)} anomalies`
  })
  const pipeline = workflow("pipeline", async () => report(await analyze(await ingest()))).bind(
    laser
  )
  const runId = ConversationId.new()

  await assert.rejects(pipeline.resume(runId), (error: unknown) => error === failure)
  healthy = true
  const outcome = await pipeline.resume(runId)

  assert.deepEqual(ran, ["ingest", "analyze", "analyze", "report"])
  assert.equal(outcome.value, "2 anomalies")
  assert.deepEqual(outcome.completedSteps, ["ingest", "analyze", "report"])
})

void test("given_steps_started_together_when_resumed_then_should_give_each_call_its_own_recorded_value", async () => {
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, uniqueName())
  let runs = 0
  const scan = step("scan", async (band: string, delayMs: number) => {
    runs += 1
    await new Promise((resolve) => setTimeout(resolve, delayMs))
    return band
  })
  const pipeline = workflow("pipeline", () =>
    Promise.all([scan("s-band", 30), scan("x-band", 0)])
  ).bind(laser)

  const first = await pipeline.run()
  const resumed = await pipeline.resume(first.runId)

  assert.equal(runs, 2)
  assert.deepEqual(resumed.value, ["s-band", "x-band"])
})

void test("given_a_function_form_run_when_its_journal_is_read_then_should_hold_the_records_of_a_builder_run", async () => {
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, uniqueName())
  const ingest = step("ingest", () => ({ frames: 12 }))
  const report = step("report", (batch: { frames: number }) => `${String(batch.frames)} frames`)
  const pipeline = workflow("pipeline", async () => report(await ingest())).bind(laser)

  const outcome = await pipeline.run()
  const journal = await journalOf(laser, outcome.runId)
  const builder = await laser
    .workflow("pipeline")
    .runId(outcome.runId)
    .stepLocal("ingest", () => "must not run")
    .stepLocal("report", () => "must not run")
    .after("ingest")
    .run()

  assert.deepEqual(
    journal.map((entry) =>
      entry.kind === "stepCompleted"
        ? [entry.kind, entry.label, new TextDecoder().decode(entry.output), entry.agents]
        : [entry.kind]
    ),
    [
      ["runStarted"],
      ["stepCompleted", "ingest", '{"frames":12}', []],
      ["stepCompleted", "report", '"12 frames"', []]
    ]
  )
  assert.equal(builder.text("ingest"), '{"frames":12}')
  assert.equal(builder.text("report"), '"12 frames"')
})

void test("given_no_stream_configured_when_an_unbound_workflow_runs_then_should_create_the_stream_named_after_it", async () => {
  const name = uniqueName()
  const previous = process.env["LASER_STREAM"]
  delete process.env["LASER_STREAM"]
  process.env["LASER_CONNECTION_STRING"] = CONNECTION_STRING
  await using laser = await Laser.connect(CONNECTION_STRING)
  try {
    const ingest = step("ingest", () => 12)

    const outcome = await workflow(name, () => ingest()).run()

    assert.equal(outcome.value, 12)
    assert.equal(await laser.stream(name).exists(), true)
    const topics = (await laser.stream(name).topics()).map((topic) => topic.name)
    assert.ok(topics.includes(AgentTopic.WorkflowJournal), topics.join(", "))
  } finally {
    if (previous !== undefined) process.env["LASER_STREAM"] = previous
    await laser.stream(name).delete()
  }
})

void test("given_a_bound_client_with_a_default_stream_when_run_then_should_use_it_and_leave_it_open", async () => {
  const name = uniqueName()
  const stream = uniqueName()
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  const ingest = step("ingest", () => 12)

  const outcome = await workflow(name, () => ingest())
    .bind(laser)
    .run()

  assert.equal(outcome.value, 12)
  assert.equal(await laser.stream(stream).exists(), true)
  assert.equal(await laser.stream(name).exists(), false)
  await laser.ping()
})

void test("given_concurrent_branches_finishing_in_another_order_when_resumed_then_should_return_each_branch_its_own_values", async () => {
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, uniqueName())
  const failure = new Error("downlink lost")
  const calls: string[] = []
  let healthy = false
  let secondBranchDone: () => void = () => undefined
  const secondBranch = new Promise<void>((resolve) => (secondBranchDone = resolve))
  const acquire = step("acquire", async (branch: number) => {
    calls.push(`acquire-${String(branch)}`)
    if (branch === 1) await secondBranch
    return `frames-${String(branch)}`
  })
  const calibrate = step("calibrate", (branch: number) => {
    calls.push(`calibrate-${String(branch)}`)
    if (branch === 2) secondBranchDone()
    return `gain-${String(branch)}`
  })
  const publish = step("publish", () => {
    if (!healthy) throw failure
    return "published"
  })
  const branch = async (number: number) => [await acquire(number), await calibrate(number)]
  const pipeline = workflow("pipeline", async () => {
    const branches = await Promise.all([branch(1), branch(2)])
    await publish()
    return branches
  }).bind(laser)
  const runId = ConversationId.new()

  await assert.rejects(pipeline.resume(runId), (error: unknown) => error === failure)
  healthy = true
  const resumed = await pipeline.resume(runId)

  assert.deepEqual(resumed.value, [
    ["frames-1", "gain-1"],
    ["frames-2", "gain-2"]
  ])
  assert.deepEqual(calls, ["acquire-1", "acquire-2", "calibrate-2", "calibrate-1"])
})

void test("given_a_retried_step_when_resumed_then_should_not_run_it_again", async () => {
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, uniqueName())
  const failure = new Error("downlink lost")
  let attempts = 0
  let effects = 0
  let healthy = false
  const uplink = step("uplink", () => {
    attempts += 1
    if (attempts === 1) throw new Error("no carrier")
    effects += 1
    return "sent"
  })
  const confirm = step("confirm", (sent: string) => {
    if (!healthy) throw failure
    return `${sent} and confirmed`
  })
  const pipeline = workflow("pipeline", async () => {
    let sent: string | undefined
    while (sent === undefined) {
      try {
        sent = await uplink()
      } catch (error) {
        if (attempts > 1) throw error
      }
    }
    return confirm(sent)
  }).bind(laser)
  const runId = ConversationId.new()

  await assert.rejects(pipeline.resume(runId), (error: unknown) => error === failure)
  healthy = true
  const resumed = await pipeline.resume(runId)

  assert.equal(effects, 1)
  assert.equal(resumed.value, "sent and confirmed")
  assert.deepEqual(resumed.completedSteps, ["uplink", "confirm"])
})

void test("given_the_first_of_two_calls_failed_after_the_second_recorded_when_resumed_then_should_run_only_the_first", async () => {
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, uniqueName())
  const failure = new Error("downlink lost")
  const completed: string[] = []
  let healthy = false
  const body = (run: WorkflowRun) => {
    const calls: Promise<string>[] = []
    calls.push(
      run.step("downlink", async () => {
        await calls[1]
        if (!healthy) throw failure
        completed.push("first")
        return "frame-1"
      })
    )
    calls.push(
      run.step("downlink", () => {
        completed.push("second")
        return "frame-2"
      })
    )
    return Promise.all(calls)
  }
  const runId = ConversationId.new()

  await assert.rejects(
    laser.workflow("pipeline").runId(runId).runWith(body),
    (error: unknown) => error === failure
  )
  const labels = (await journalOf(laser, runId)).flatMap((entry) =>
    entry.kind === "stepCompleted" ? [entry.label] : []
  )
  healthy = true
  const resumed = await laser.workflow("pipeline").runId(runId).runWith(body)

  assert.deepEqual(labels, ["downlink#2"])
  assert.deepEqual(resumed.value, ["frame-1", "frame-2"])
  assert.deepEqual(completed, ["second", "first"])
  assert.deepEqual(resumed.completedSteps, ["downlink#2", "downlink"])
})

void test("given_a_step_that_finishes_after_the_bound_when_resumed_then_should_run_it_again", async () => {
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, uniqueName())
  const failure = new Error("downlink lost")
  let healthy = false
  let slowRuns = 0
  let finishSlow: () => void = () => undefined
  const slowGate = new Promise<void>((resolve) => (finishSlow = resolve))
  const slow = step("slow", async () => {
    slowRuns += 1
    if (slowRuns === 1) await slowGate
    return "slow"
  })
  const quick = step("quick", () => {
    if (!healthy) throw failure
    return "quick"
  })
  const pipeline = workflow("pipeline", () => Promise.all([slow(), quick()])).bind(laser)
  const runId = ConversationId.new()

  try {
    await assert.rejects(
      within(8_000, pipeline.resume(runId)),
      (error: unknown) => error === failure
    )
  } finally {
    finishSlow()
  }
  await new Promise((resolve) => setImmediate(resolve))
  const labels = (await journalOf(laser, runId)).flatMap((entry) =>
    entry.kind === "stepCompleted" ? [entry.label] : []
  )
  healthy = true
  const resumed = await pipeline.resume(runId)

  assert.deepEqual(labels, [])
  assert.equal(slowRuns, 2)
  assert.deepEqual(resumed.value, ["slow", "quick"])
})

void test("given_an_aborted_run_when_resumed_then_should_continue_it", async () => {
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, uniqueName())
  const controller = new AbortController()
  const ran: string[] = []
  const body = async (run: WorkflowRun) => {
    const frames = await run.step("ingest", () => {
      ran.push("ingest")
      controller.abort()
      return 12
    })
    return run.step("analyze", () => {
      ran.push("analyze")
      return frames / 6
    })
  }
  const runId = ConversationId.new()

  await assert.rejects(
    laser.workflow("pipeline").runId(runId).runWith(body, { signal: controller.signal }),
    CancelledError
  )
  const resumed = await laser.workflow("pipeline").runId(runId).runWith(body)

  assert.deepEqual(ran, ["ingest", "analyze"])
  assert.equal(resumed.value, 2)
  assert.equal(resumed.status, "completed")
})

void test("given_a_journal_record_without_an_input_when_resumed_then_should_match_by_order", async () => {
  await using laser = await Laser.connectWithStream(CONNECTION_STRING, uniqueName())
  const builder = await laser
    .workflow("pipeline")
    .stepLocal("scan#2", () => '"x-band"')
    .stepLocal("scan", () => '"s-band"')
    .run()
  let runs = 0
  const scan = step("scan", (band: string) => {
    runs += 1
    return band
  })
  const pipeline = workflow("pipeline", async () => [
    await scan("first"),
    await scan("second")
  ]).bind(laser)

  const resumed = await pipeline.resume(builder.runId)
  const journal = await journalOf(laser, builder.runId)

  assert.equal(runs, 0)
  assert.deepEqual(resumed.value, ["s-band", "x-band"])
  assert.equal(
    journal.some((entry) => entry.kind === "stepCompleted" && entry.input !== undefined),
    false
  )
})

function within<Value>(milliseconds: number, work: Promise<Value>): Promise<Value> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const expired = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`still running after ${String(milliseconds)} ms`))
    }, milliseconds)
  })
  return Promise.race([work, expired]).finally(() => {
    clearTimeout(timer)
  })
}

function uniqueName(): string {
  return `laser-ts-test-${randomUUID()}`
}

async function journalOf(laser: Laser, runId: ConversationId): Promise<readonly JournalEntry[]> {
  const messages = await replayContext(
    {
      conversation: runId,
      acrossSubconversations: false,
      topics: [AgentTopic.WorkflowJournal],
      policy: new LastN(100),
      fromOffsets: new Map()
    },
    laser
  )
  return messages.map((message) => decodeJournalEntry(message.payload))
}

import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Agent, type AgentHandle } from "../../src/agent/builder.js"
import { decodeAgentMessage } from "../../src/agent/reliable-consumer.js"
import { WorkflowBudget } from "../../src/agent/workflow.js"
import { BudgetExceededError, CancelledError, StepFailedError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { AgentKind, decodeSessionStart } from "../../src/wire/agent.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import { routeTo } from "../../src/agent/router.js"
import { TopicRetention } from "../../src/session.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
const fixedCommands = { kind: "fixed" as const, topic: AgentTopic.Sessions }

void test("given_a_completed_workflow_when_resumed_then_should_replay_without_redispatching", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  let handle: AgentHandle | undefined
  try {
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    const worker = AgentId.new("workflow-worker")
    let calls = 0
    handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Sessions)
      .respondOn(AgentTopic.Sessions)
      .handler({
        handle(message, context): Promise<void> {
          calls += 1
          return context.respond(
            textEncoder.encode(`reply:${textDecoder.decode(message.envelope?.body)}`)
          )
        }
      })
      .build()
      .spawn(laser)
    await handle.ready()

    const first = await laser
      .workflow("orchestrator")
      .inboxRoute(fixedCommands)
      .step("triage", routeTo(worker), () => textEncoder.encode("triage"))
      .step("diagnose", routeTo(worker), ({ outputs }) =>
        textEncoder.encode(`diagnose:${textDecoder.decode(outputs.get("triage"))}`)
      )
      .after("triage")
      .run()
    assert.equal(calls, 2)
    assert.equal(textDecoder.decode(first.outputs.get("triage")), "reply:triage")
    assert.equal(textDecoder.decode(first.outputs.get("diagnose")), "reply:diagnose:reply:triage")

    const resumed = await laser
      .workflow("orchestrator")
      .inboxRoute(fixedCommands)
      .runId(first.runId)
      .step("triage", routeTo(worker), () => textEncoder.encode("must-not-run"))
      .step("diagnose", routeTo(worker), () => textEncoder.encode("must-not-run"))
      .after("triage")
      .run()
    assert.equal(calls, 2)
    assert.deepEqual(resumed.outputs, first.outputs)
  } finally {
    if (handle !== undefined) await handle.shutdown()
    await laser.close()
  }
})

void test("given_a_later_verifier_failure_when_running_then_should_compensate_completed_steps", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  let handle: AgentHandle | undefined
  try {
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    const worker = AgentId.new("saga-worker")
    const bodies: string[] = []
    handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Sessions)
      .respondOn(AgentTopic.Sessions)
      .handler({
        handle(message, context): Promise<void> {
          const body = textDecoder.decode(message.envelope?.body)
          bodies.push(body)
          return context.respond(textEncoder.encode(`ok:${body}`))
        }
      })
      .build()
      .spawn(laser)
    await handle.ready()

    await assert.rejects(
      laser
        .workflow("saga")
        .inboxRoute(fixedCommands)
        .step("apply", routeTo(worker), () => textEncoder.encode("apply"))
        .compensateWith(() => textEncoder.encode("undo-apply"))
        .step("verify", routeTo(worker), () => textEncoder.encode("verify"))
        .after("apply")
        .verifyWith(() => false)
        .run(),
      (error: unknown) => error instanceof StepFailedError && error.reason === "unverified"
    )
    assert.deepEqual(bodies, ["apply", "verify", "undo-apply"])
  } finally {
    if (handle !== undefined) await handle.shutdown()
    await laser.close()
  }
})

void test("given_a_spent_invocation_budget_when_running_then_should_fail_before_dispatch", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    await assert.rejects(
      laser
        .workflow("budgeted")
        .inboxRoute(fixedCommands)
        .budget(WorkflowBudget.unlimited().invocations(0))
        .step("blocked", routeTo(AgentId.new("never-called")), () => textEncoder.encode("work"))
        .run(),
      BudgetExceededError
    )
  } finally {
    await laser.close()
  }
})

async function eventually<Value>(read: () => Promise<Value | undefined>): Promise<Value> {
  const deadline = performance.now() + 10_000
  let value = await read()
  while (value === undefined && performance.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 50))
    value = await read()
  }
  assert.ok(value !== undefined)
  return value
}

void test("given_a_workflow_when_run_then_should_record_the_run_as_a_root_session_with_child_steps", async () => {
  const laser = await Laser.connectWithStream(CONNECTION_STRING, `laser-ts-test-${randomUUID()}`)
  try {
    await laser.bootstrap(4, TopicRetention.expireAfter(86_400_000))
    const worker = AgentId.new("rooted-triager")
    await using handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Sessions)
      .respondOn(AgentTopic.Sessions)
      .handler({ handle: (_message, context) => context.respond(textEncoder.encode("triaged")) })
      .build()
      .spawn(laser)
    await handle.ready()
    const outcome = await laser
      .workflow("rootedflow")
      .inboxRoute(fixedCommands)
      .step("triage", routeTo(worker), () => textEncoder.encode("incident"))
      .run()
    const run = outcome.runId
    const child = ConversationId.derive(`${run.toString()}/triage/1`)
    const states = async (session: ConversationId) =>
      (await laser.sessions().open(session).context()).flatMap((turn) => {
        const envelope = turn.message.envelope
        return envelope?.operation === "session" && envelope.taskState?.kind === "known"
          ? [{ state: envelope.taskState.name, body: envelope.body }]
          : []
      })
    const root = await eventually(async () => {
      const found = await states(run)
      return found.length >= 2 ? found : undefined
    })
    assert.equal(root[0]?.state, "Working")
    assert.equal(root.at(-1)?.state, "Completed")
    const step = await eventually(async () => {
      const found = await states(child)
      return found.some((entry) => entry.state === "Completed") ? found : undefined
    })
    const [submitted] = step
    assert.ok(submitted !== undefined)
    assert.equal(submitted.state, "Submitted")
    const start = decodeSessionStart(
      expectMap(decodeOne(submitted.body, "start"), "start"),
      "start"
    )
    assert.equal(start.parent?.toString(), run.toString())
    assert.equal(start.root?.toString(), run.toString())
  } finally {
    await laser.close()
  }
})

void test("given_a_cancel_request_on_the_control_topic_when_a_workflow_runs_then_should_stop_as_cancelled", async () => {
  const laser = await Laser.connectWithStream(CONNECTION_STRING, `laser-ts-test-${randomUUID()}`)
  try {
    await laser.bootstrap(4, TopicRetention.expireAfter(86_400_000))
    await laser.topic(AgentTopic.Control).ensure(4)
    const run = ConversationId.new()
    await laser
      .sessions()
      .control(laser.defaultStream ?? "", run)
      .asOperator(AgentId.new("operator"))
      .cancel()
    const error = await eventually(async () => {
      try {
        await laser
          .workflow("cancelledflow")
          .runId(run)
          .inboxRoute(fixedCommands)
          .step("never", routeTo(AgentId.new("nobody")), () => textEncoder.encode("x"))
          .run()
        return undefined
      } catch (error) {
        return error instanceof CancelledError ? error : undefined
      }
    })
    assert.equal(error.run, run.toString())
  } finally {
    await laser.close()
  }
})

void test("given_a_step_command_that_expired_unconsumed_when_a_fresh_process_resumes_the_run_then_should_run_the_resent_step", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const firstProcess = await Laser.connectWithStream(CONNECTION_STRING, stream)
  const resumingProcess = await Laser.connectWithStream(CONNECTION_STRING, stream)
  const workerProcess = await Laser.connectWithStream(CONNECTION_STRING, stream)
  let handle: AgentHandle | undefined
  try {
    await firstProcess.bootstrap(4, TopicRetention.expireAfter(86_400_000))
    const worker = AgentId.new("expiry-worker")
    const run = ConversationId.new()
    await assert.rejects(
      firstProcess
        .workflow("expiryflow")
        .runId(run)
        .inboxRoute(fixedCommands)
        .step("work", routeTo(worker), () => textEncoder.encode("go"))
        .deadline(1_000)
        .run(),
      (error: unknown) => error instanceof StepFailedError && error.reason === "timed_out"
    )

    // A fresh process resumes the run and sends the step again before its
    // worker starts, so the worker reads the expired copy first.
    const resumed = resumingProcess
      .workflow("expiryflow")
      .runId(run)
      .inboxRoute(fixedCommands)
      .step("work", routeTo(worker), () => textEncoder.encode("go"))
      .deadline(20_000)
      .run()
    await eventually(async () =>
      (await envelopesOnLog(firstProcess, AgentKind.Command, "expiry-worker")) === 2
        ? true
        : undefined
    )
    let calls = 0
    handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Sessions)
      .respondOn(AgentTopic.Sessions)
      .ackOnPickup()
      .handler({
        handle(_message, context): Promise<void> {
          calls += 1
          return context.respond(textEncoder.encode("counted"))
        }
      })
      .build()
      .spawn(workerProcess)
    const outcome = await resumed

    assert.equal(calls, 1)
    assert.equal(textDecoder.decode(outcome.outputs.get("work")), "counted")
  } finally {
    if (handle !== undefined) await handle.shutdown()
    await workerProcess.close()
    await resumingProcess.close()
    await firstProcess.close()
  }
})

void test("given_an_expired_step_command_answered_before_the_resume_when_a_fresh_process_resumes_the_run_then_should_run_the_resent_step", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const firstProcess = await Laser.connectWithStream(CONNECTION_STRING, stream)
  const workerProcess = await Laser.connectWithStream(CONNECTION_STRING, stream)
  const resumingProcess = await Laser.connectWithStream(CONNECTION_STRING, stream)
  let handle: AgentHandle | undefined
  try {
    await firstProcess.bootstrap(4, TopicRetention.expireAfter(86_400_000))
    const worker = AgentId.new("answered-worker")
    const run = ConversationId.new()
    await assert.rejects(
      firstProcess
        .workflow("answeredflow")
        .runId(run)
        .inboxRoute(fixedCommands)
        .step("work", routeTo(worker), () => textEncoder.encode("go"))
        .deadline(1_000)
        .run(),
      (error: unknown) => error instanceof StepFailedError && error.reason === "timed_out"
    )

    // The worker starts in its own process, drops the expired copy, and
    // answers that it was not consumed before the run resumes.
    let calls = 0
    handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Sessions)
      .respondOn(AgentTopic.Sessions)
      .ackOnPickup()
      .handler({
        handle(_message, context): Promise<void> {
          calls += 1
          return context.respond(textEncoder.encode("counted"))
        }
      })
      .build()
      .spawn(workerProcess)
    await eventually(async () =>
      (await envelopesOnLog(firstProcess, AgentKind.Error, "answeredflow")) === 1 ? true : undefined
    )

    const resumed = await resumingProcess
      .workflow("answeredflow")
      .runId(run)
      .inboxRoute(fixedCommands)
      .step("work", routeTo(worker), () => textEncoder.encode("go"))
      .deadline(20_000)
      .run()

    assert.equal(calls, 1)
    assert.equal(textDecoder.decode(resumed.outputs.get("work")), "counted")
  } finally {
    if (handle !== undefined) await handle.shutdown()
    await resumingProcess.close()
    await workerProcess.close()
    await firstProcess.close()
  }
})

// How many AGDX envelopes of `kind` addressed to `target` the sessions topic
// holds.
async function envelopesOnLog(laser: Laser, kind: AgentKind, target: string): Promise<number> {
  const records = await (await laser.topic(AgentTopic.Sessions).replay()).pollRecords()
  return records.filter((record) => {
    const decoded = decodeAgentMessage(record)
    return (
      decoded.kind === "message" &&
      decoded.message.envelope?.kind === kind &&
      decoded.message.envelope.target === target
    )
  }).length
}

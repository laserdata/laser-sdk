import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Agent, type AgentHandle } from "../../src/agent/builder.js"
import { WorkflowBudget } from "../../src/agent/workflow.js"
import { BudgetExceededError, CancelledError, HandlerConfigError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { decodeSessionStart } from "../../src/wire/agent.js"
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
      .pollInterval(5)
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
      .pollInterval(5)
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
      HandlerConfigError
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
      .pollInterval(5)
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

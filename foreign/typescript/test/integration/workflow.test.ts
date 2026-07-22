import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Agent, type AgentHandle } from "../../src/agent/builder.js"
import { Budget } from "../../src/agent/workflow.js"
import { BudgetExceededError, HandlerConfigError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId } from "../../src/types/ids.js"
import { routeTo } from "../../src/agent/router.js"

const CONNECTION_STRING =
  process.env["LASER_CONNECTION_STRING"] ?? "iggy://iggy:iggy@127.0.0.1:8090"

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()
const fixedCommands = { kind: "fixed" as const, topic: AgentTopic.Commands }

void test("given_a_completed_workflow_when_resumed_then_should_replay_without_redispatching", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  let handle: AgentHandle | undefined
  try {
    await laser.bootstrap(1)
    const worker = AgentId.new("workflow-worker")
    let calls = 0
    handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Commands)
      .respondOn(AgentTopic.Responses)
      .pollInterval(5)
      .handler({
        handle(message, context): Promise<void> {
          calls += 1
          return context.respond(
            textEncoder.encode(`reply:${textDecoder.decode(message.envelope?.body)}`)
          )
        }
      })
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
    await laser.bootstrap(1)
    const worker = AgentId.new("saga-worker")
    const bodies: string[] = []
    handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Commands)
      .respondOn(AgentTopic.Responses)
      .pollInterval(5)
      .handler({
        handle(message, context): Promise<void> {
          const body = textDecoder.decode(message.envelope?.body)
          bodies.push(body)
          return context.respond(textEncoder.encode(`ok:${body}`))
        }
      })
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
    await laser.bootstrap(1)
    await assert.rejects(
      laser
        .workflow("budgeted")
        .inboxRoute(fixedCommands)
        .budget(Budget.unlimited().invocations(0))
        .step("blocked", routeTo(AgentId.new("never-called")), () => textEncoder.encode("work"))
        .run(),
      BudgetExceededError
    )
  } finally {
    await laser.close()
  }
})

import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Agent } from "../../src/agent/builder.js"
import { routeTo } from "../../src/agent/router.js"
import { WorkflowBudget } from "../../src/agent/workflow.js"
import { BudgetExceededError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { ModelRequest, type ModelResponse } from "../../src/session-ops.js"
import { TopicRetention } from "../../src/session.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { AgentKind, type SessionEnd, decodeSessionEnd } from "../../src/wire/agent.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const AGENT = AgentId.new("budgeted")
const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

function usage(inputTokens: bigint, outputTokens: bigint, costMicros?: bigint): ModelResponse {
  return {
    body: textEncoder.encode("answer"),
    usage: { inputTokens, outputTokens, ...(costMicros !== undefined ? { costMicros } : {}) }
  }
}

function request(): ModelRequest {
  return new ModelRequest("m", textEncoder.encode("q"))
}

async function connect(): Promise<Laser> {
  const laser = await Laser.connectWithStream(CONNECTION_STRING, `laser-ts-budget-${randomUUID()}`)
  await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
  return laser
}

async function eventually<Value>(read: () => Promise<Value | undefined>): Promise<Value> {
  const deadline = Date.now() + 15_000
  for (;;) {
    const value = await read()
    if (value !== undefined) return value
    if (Date.now() > deadline) assert.fail("the condition did not hold in time")
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
}

void test("given_a_token_budget_when_usage_passes_it_then_should_fold_the_lane_over_budget", async () => {
  const laser = await connect()
  try {
    const { session, lease } = await laser
      .sessions()
      .start()
      .agent(AGENT)
      .budget({ tokens: 100n })
      .begin()
    await session.recordModelCall(request(), usage(40n, 20n))
    assert.equal(await session.overBudget(), false, "60 of 100 tokens is within the budget")
    await session.recordModelCall(request(), usage(30n, 10n))
    assert.equal(
      await session.overBudget(),
      false,
      "exactly 100 of 100 tokens is not over the budget"
    )
    await session.recordModelCall(request(), usage(1n, 0n))
    assert.equal(await session.overBudget(), true, "101 of 100 tokens is over the budget")
    lease.release()
  } finally {
    await laser.close()
  }
})

void test("given_a_cost_budget_or_none_when_usage_is_recorded_then_should_fold_each_ceiling", async () => {
  const laser = await connect()
  try {
    const costed = await laser.sessions().start().agent(AGENT).budget({ costMicros: 500n }).begin()
    await costed.session.recordModelCall(request(), usage(10_000n, 10_000n, 400n))
    assert.equal(
      await costed.session.overBudget(),
      false,
      "tokens without a token ceiling never breach"
    )
    await costed.session.recordModelCall(request(), usage(1n, 1n, 101n))
    assert.equal(await costed.session.overBudget(), true)

    const unbounded = await laser.sessions().start().agent(AGENT).begin()
    const half = (1n << 63n) - 1n
    await unbounded.session.recordModelCall(request(), usage(half, half, (1n << 64n) - 1n))
    assert.equal(
      await unbounded.session.overBudget(),
      false,
      "a session without a budget is never over it"
    )
    costed.lease.release()
    unbounded.lease.release()
  } finally {
    await laser.close()
  }
})

void test("given_a_run_over_its_token_budget_when_the_next_step_starts_then_should_compensate_and_fail_with_reason_budget", async () => {
  const laser = await connect()
  const spender = AgentId.new("spender")
  let shipped = 0
  let compensated = 0
  // The `charge` step records a model call on the run session that passes
  // the run's token budget, so the next step boundary stops.
  const worker = Agent.builder()
    .id(spender)
    .listenOn(AgentTopic.Sessions)
    .respondOn(AgentTopic.Sessions)
    .pollInterval(5)
    .handler({
      async handle(message, context): Promise<void> {
        const body = textDecoder.decode(message.envelope?.body)
        if (body === "undo") compensated += 1
        else if (body === "ship") shipped += 1
        else {
          const run = message.provenance.parentConversationId
          assert.ok(run !== undefined, "a workflow step names its run as parent")
          await laser
            .sessions()
            .open(run)
            .asAgent(spender)
            .recordModelCall(new ModelRequest("m", textEncoder.encode("charge")), usage(80n, 40n))
        }
        await context.respond(textEncoder.encode("done"))
      }
    })
    .build()
    .spawn(laser)
  try {
    await worker.ready()
    const run = ConversationId.new()
    await assert.rejects(
      laser
        .workflow("spendflow")
        .runId(run)
        .budget(WorkflowBudget.tokens(100n))
        .inboxRoute({ kind: "fixed", topic: AgentTopic.Sessions })
        .step("charge", routeTo(spender), () => textEncoder.encode("charge"))
        .compensateWith(() => textEncoder.encode("undo"))
        .step("ship", routeTo(spender), () => textEncoder.encode("ship"))
        .after("charge")
        .run(),
      (error: unknown) =>
        error instanceof BudgetExceededError && error.ceiling === 100n && error.spent === 120n
    )
    assert.equal(shipped, 0, "no step runs past the budget")
    await eventually(() => Promise.resolve(compensated === 1 ? true : undefined))
    const end = await eventually<SessionEnd>(async () => {
      const turns = await laser.sessions().open(run).context()
      for (const turn of turns) {
        const envelope = turn.message.envelope
        if (
          envelope?.kind === AgentKind.Status &&
          envelope.operation === "session" &&
          envelope.taskState?.kind === "known" &&
          envelope.taskState.name === "Failed"
        ) {
          return decodeSessionEnd(expectMap(decodeOne(envelope.body, "end"), "end"), "end")
        }
      }
      return undefined
    })
    assert.equal(end.reason, "budget")
    assert.ok(end.error?.message?.includes("budget"), "the failure names the budget")
  } finally {
    await worker.shutdown()
    await laser.close()
  }
})

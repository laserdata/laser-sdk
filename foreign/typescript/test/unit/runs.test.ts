import assert from "node:assert/strict"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom, OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { AgentWorkflowExecutionError, UnsupportedError } from "../../src/client/errors.js"
import { Runs } from "../../src/managed/runs.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import type { AgentReply, AgentRunInfo } from "../../src/wire/agent-workflow.js"
import { encodeAgentReply } from "../../src/wire/agent-workflow.js"
import {
  AgentCancelCommand,
  AgentListCommand,
  AgentStatusCommand,
  AgentSubmitCommand
} from "../../src/wire/commands.js"
import type { ControlCommand } from "../../src/wire/control.js"
import { Feature } from "../../src/wire/hello.js"

const CAPS: Capabilities = managedCapabilitiesFrom({
  versions: {
    query: 1,
    control: 1,
    kv: 1,
    fork: 1,
    agent: 1,
    graph: 1,
    features: Feature.AGENT_WORKFLOW
  },
  backends: []
})

function replyFrame(reply: AgentReply): Uint8Array {
  return encodeNamed(encodeAgentReply(reply))
}

function fakeTransport(scriptedReplies: readonly Uint8Array[]): {
  readonly calls: { readonly code: number; readonly payload: Uint8Array }[]
  sendManaged(code: number, payload: Uint8Array): Promise<Uint8Array>
} {
  const calls: { code: number; payload: Uint8Array }[] = []
  let next = 0
  return {
    calls,
    sendManaged(code, payload) {
      calls.push({ code, payload })
      const reply = scriptedReplies[next]
      next += 1
      if (reply === undefined) throw new Error("fake transport ran out of scripted replies")
      return Promise.resolve(reply)
    }
  }
}

function fakePublishControl(): {
  readonly calls: ControlCommand[]
  readonly publish: (command: ControlCommand) => Promise<void>
} {
  const calls: ControlCommand[] = []
  const publish = (command: ControlCommand): Promise<void> => {
    calls.push(command)
    return Promise.resolve()
  }
  return { calls, publish }
}

const RUN: AgentRunInfo = {
  runId: "run-1",
  agentId: "planner",
  userId: 7,
  state: "submitted",
  createdAtMicros: 1n,
  updatedAtMicros: 1n,
  cancelRequested: false
}

void test("given_a_submitted_outcome_when_submit_is_called_then_should_return_the_run_and_use_the_submit_command", async () => {
  const transport = fakeTransport([
    replyFrame({ kind: "ok", outcome: { kind: "submitted", run: RUN } })
  ])
  const control = fakePublishControl()
  const runs = new Runs(transport, () => Promise.resolve(CAPS), control.publish)
  const run = await runs.submit("planner", Uint8Array.of(1))
  assert.deepEqual(run, RUN)
  assert.equal(transport.calls[0]?.code, AgentSubmitCommand.code)
})

void test("given_submit_with_options_when_submitted_then_should_carry_run_id_params_and_budget", async () => {
  const transport = fakeTransport([
    replyFrame({ kind: "ok", outcome: { kind: "submitted", run: RUN } })
  ])
  const runs = new Runs(
    transport,
    () => Promise.resolve(CAPS),
    () => Promise.resolve()
  )
  await runs.submitWith("planner", {
    runId: "run-1",
    params: new Map([["k", "v"]]),
    budget: { maxEvents: 10n }
  })
  assert.deepEqual(
    transport.calls[0]?.payload,
    AgentSubmitCommand.encode({
      agentId: "planner",
      runId: "run-1",
      params: new Map([["k", "v"]]),
      budget: { maxEvents: 10n }
    })
  )
})

void test("given_a_budget_when_submit_budgeted_is_called_then_should_delegate_to_submit_with", async () => {
  const transport = fakeTransport([
    replyFrame({ kind: "ok", outcome: { kind: "submitted", run: RUN } })
  ])
  const runs = new Runs(
    transport,
    () => Promise.resolve(CAPS),
    () => Promise.resolve()
  )
  const run = await runs.submitBudgeted("planner", { maxEvents: 5n }, Uint8Array.of(1))
  assert.deepEqual(run, RUN)
})

void test("given_a_cancelled_outcome_when_cancel_is_called_then_should_use_the_cancel_command", async () => {
  const transport = fakeTransport([
    replyFrame({ kind: "ok", outcome: { kind: "cancelled", run: RUN } })
  ])
  const runs = new Runs(
    transport,
    () => Promise.resolve(CAPS),
    () => Promise.resolve()
  )
  const run = await runs.cancel("run-1")
  assert.deepEqual(run, RUN)
  assert.equal(transport.calls[0]?.code, AgentCancelCommand.code)
})

void test("given_a_status_outcome_when_status_is_called_then_should_use_the_status_command", async () => {
  const transport = fakeTransport([
    replyFrame({ kind: "ok", outcome: { kind: "status", run: RUN } })
  ])
  const runs = new Runs(
    transport,
    () => Promise.resolve(CAPS),
    () => Promise.resolve()
  )
  const run = await runs.status("run-1")
  assert.deepEqual(run, RUN)
  assert.equal(transport.calls[0]?.code, AgentStatusCommand.code)
})

void test("given_a_list_outcome_when_list_is_fetched_with_filters_then_should_return_the_page", async () => {
  const transport = fakeTransport([
    replyFrame({ kind: "ok", outcome: { kind: "list", page: { runs: [RUN] } } })
  ])
  const runs = new Runs(
    transport,
    () => Promise.resolve(CAPS),
    () => Promise.resolve()
  )
  const page = await runs.list().agent("planner").state("submitted").limit(10).fetch()
  assert.deepEqual(page.runs, [RUN])
  assert.equal(transport.calls[0]?.code, AgentListCommand.code)
})

void test("given_a_stream_and_topic_when_register_and_remove_source_are_called_then_should_publish_the_right_commands", async () => {
  const control = fakePublishControl()
  const runs = new Runs(
    { sendManaged: () => Promise.reject(new Error("unused")) },
    () => Promise.resolve(CAPS),
    control.publish
  )
  await runs.registerSource("orders", "events")
  await runs.removeSource("orders", "events")
  assert.deepEqual(control.calls, [
    { kind: "registerRunSource", source: { stream: "orders", topic: "events" } },
    { kind: "removeRunSource", source: { stream: "orders", topic: "events" } }
  ])
})

void test("given_open_capabilities_when_submit_is_called_then_should_reject_before_the_transport", async () => {
  const transport = fakeTransport([])
  const runs = new Runs(
    transport,
    () => Promise.resolve(OPEN_CAPABILITIES),
    () => Promise.resolve()
  )
  await assert.rejects(() => runs.submit("planner"), UnsupportedError)
  assert.equal(transport.calls.length, 0)
})

void test("given_an_err_reply_when_cancel_fails_then_should_wrap_it_as_an_agent_workflow_execution_error", async () => {
  const transport = fakeTransport([
    replyFrame({ kind: "err", error: { kind: "notFound", message: "no such run" } })
  ])
  const runs = new Runs(
    transport,
    () => Promise.resolve(CAPS),
    () => Promise.resolve()
  )
  await assert.rejects(() => runs.cancel("run-1"), AgentWorkflowExecutionError)
})

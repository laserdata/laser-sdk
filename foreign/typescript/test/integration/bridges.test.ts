import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Agent, type AgentHandle } from "../../src/agent/builder.js"
import { A2aBridge } from "../../src/bridges/a2a.js"
import { McpBridge } from "../../src/bridges/mcp.js"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"

const CONNECTION_STRING =
  process.env["LASER_CONNECTION_STRING"] ?? "iggy://iggy:iggy@127.0.0.1:8090"

const textEncoder = new TextEncoder()

void test("given_an_a2a_task_when_submitted_and_cancelled_then_should_replay_its_terminal_state", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const bridge = new A2aBridge(
      laser,
      AgentId.new("a2a-edge"),
      AgentTopic.Commands,
      AgentTopic.Responses
    )
    const submitted = await bridge.submit({ message: { role: "user", text: "hello" } })
    assert.equal(submitted.status.state.kind, "known")
    assert.deepEqual((await bridge.task(submitted.id)).status.state, {
      kind: "known",
      name: "Working"
    })

    const cancelled = await bridge.cancel(submitted.id)
    assert.deepEqual(cancelled.status.state, { kind: "known", name: "Canceled" })
    const replayed = await bridge.task(submitted.id)
    assert.deepEqual(replayed.status.state, { kind: "known", name: "Canceled" })
  } finally {
    await laser.close()
  }
})

void test("given_an_mcp_tool_call_when_bridged_then_should_return_the_correlated_agdx_result", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  let handle: AgentHandle | undefined
  try {
    await laser.bootstrap(1)
    const worker = AgentId.new("mcp-worker")
    handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.ToolCalls)
      .pollInterval(5)
      .handler({
        async handle(message, context): Promise<void> {
          const envelope = message.envelope
          assert.ok(envelope?.correlation !== undefined)
          await context.laser
            .agdx(AgentTopic.ToolResults, worker, message.provenance.conversationId)
            .respond(envelope.correlation, textEncoder.encode(`called:${envelope.tool ?? ""}`))
            .withTool(envelope.tool ?? "")
            .send()
        }
      })
      .spawn(laser)
    await handle.ready()
    const bridge = new McpBridge(
      laser,
      AgentId.new("mcp-edge"),
      AgentTopic.ToolCalls,
      AgentTopic.ToolResults,
      "laser-tools"
    ).withTimeout(2_000)
    const result = await bridge.callTool("search", {
      name: "search",
      arguments: { query: "laser" },
      _meta: { trace: "abc" }
    })
    assert.deepEqual(result, { content: [{ type: "text", text: "called:search" }] })
  } finally {
    if (handle !== undefined) await handle.shutdown()
    await laser.close()
  }
})

void test("given_state_snapshots_and_deltas_when_replayed_then_should_reconstruct_and_render_agui", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1)
    const conversation = ConversationId.new()
    const source = AgentId.new("agui-edge")
    await laser.publishStateSnapshot(AgentTopic.Audit, source, conversation, {
      phase: "start",
      items: ["one"]
    })
    await laser.publishStateDelta(AgentTopic.Audit, source, conversation, [
      { op: "replace", path: "/phase", value: "done" },
      { op: "add", path: "/items/-", value: "two" }
    ])
    assert.deepEqual(await laser.reconstructState(conversation, AgentTopic.Audit), {
      phase: "done",
      items: ["one", "two"]
    })
    const events = await laser.aguiEvents(conversation, AgentTopic.Audit)
    assert.deepEqual(
      events.map((event) => event.type),
      ["STATE_SNAPSHOT", "STATE_DELTA"]
    )
  } finally {
    await laser.close()
  }
})

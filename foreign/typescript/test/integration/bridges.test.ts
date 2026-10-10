import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Agent, type AgentHandle } from "../../src/agent/builder.js"
import { A2aBridge } from "../../src/bridges/a2a.js"
import { McpBridge } from "../../src/bridges/mcp.js"
import { Laser } from "../../src/client/laser.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { TopicRetention } from "../../src/session.js"
import { ContextAssembler } from "../../src/context.js"
import { decodeSessionStart } from "../../src/wire/agent.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"

const textEncoder = new TextEncoder()

void test("given_an_a2a_task_when_submitted_and_cancelled_then_should_replay_its_terminal_state", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    const bridge = new A2aBridge(
      laser,
      AgentId.new("a2a-edge"),
      AgentTopic.Sessions,
      AgentTopic.Sessions
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
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    const worker = AgentId.new("mcp-worker")
    handle = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Sessions)
      .pollInterval(5)
      .handler({
        async handle(message, context): Promise<void> {
          const envelope = message.envelope
          assert.ok(envelope?.correlation !== undefined)
          await context.laser
            .agdx(AgentTopic.Sessions, worker, message.provenance.conversationId)
            .respond(envelope.correlation, textEncoder.encode(`called:${envelope.tool ?? ""}`))
            .withTool(envelope.tool ?? "")
            .send()
        }
      })
      .build()
      .spawn(laser)
    await handle.ready()
    const bridge = new McpBridge(
      laser,
      AgentId.new("mcp-edge"),
      AgentTopic.Sessions,
      AgentTopic.Sessions,
      "laser-tools"
    ).withTimeout(2_000)
    const result = await bridge.callTool("search", {
      name: "search",
      arguments: { query: "laser" },
      _meta: { trace: "abc" }
    })
    assert.deepEqual(result, { content: [{ kind: "text", text: "called:search" }] })
  } finally {
    if (handle !== undefined) await handle.shutdown()
    await laser.close()
  }
})

void test("given_state_snapshots_and_deltas_when_replayed_then_should_reconstruct_and_render_agui", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  try {
    await laser.bootstrap(1, TopicRetention.expireAfter(86_400_000))
    const conversation = ConversationId.new()
    const source = AgentId.new("agui-edge")
    await laser.publishStateSnapshot(source, conversation, {
      phase: "start",
      items: ["one"]
    })
    await laser.publishStateDelta(source, conversation, [
      { op: "replace", path: "/phase", value: "done" },
      { op: "add", path: "/items/-", value: "two" }
    ])
    assert.deepEqual(await laser.reconstructState(conversation), {
      phase: "done",
      items: ["one", "two"]
    })
    // The snapshot is a replacing patch and a snapshot of its result, all on
    // the session lane.
    const events = await laser.aguiEvents(conversation, AgentTopic.Sessions)
    assert.deepEqual(
      events.map((event) => event.type),
      ["STATE_DELTA", "STATE_SNAPSHOT", "STATE_DELTA"]
    )
    // Decoded documents have no prototype, so compare their JSON.
    const plain = (value: unknown): unknown => JSON.parse(JSON.stringify(value))
    assert.deepEqual(plain(events[1]), {
      type: "STATE_SNAPSHOT",
      snapshot: { phase: "start", items: ["one"] }
    })
    assert.deepEqual(plain(events[2]), {
      type: "STATE_DELTA",
      delta: [
        { op: "replace", path: "/phase", value: "done" },
        { op: "add", path: "/items/-", value: "two" }
      ]
    })
  } finally {
    await laser.close()
  }
})

void test("given_a_tools_call_in_a_parent_session_when_the_tool_replies_then_should_run_as_a_completed_child", async () => {
  const laser = await Laser.connectWithStream(CONNECTION_STRING, `laser-ts-test-${randomUUID()}`)
  try {
    await laser.bootstrap(4, TopicRetention.expireAfter(86_400_000))
    const worker = AgentId.new("tool-worker")
    await using tool = Agent.builder()
      .id(worker)
      .listenOn(AgentTopic.Sessions)
      .pollInterval(5)
      .handler({
        async handle(message, context): Promise<void> {
          const command = message.envelope
          if (command?.correlation === undefined) throw new Error("expected an AGDX command")
          await context.laser
            .agdx(
              AgentTopic.Sessions,
              worker,
              ConversationId.parse(command.conversation.toString())
            )
            .respond(command.correlation, textEncoder.encode(`ran ${command.tool ?? ""}`))
            .send()
        }
      })
      .build()
      .spawn(laser)
    await tool.ready()
    const bridge = new McpBridge(
      laser,
      AgentId.new("mcp-bridge"),
      AgentTopic.Sessions,
      AgentTopic.Sessions,
      "test-server"
    ).withTimeout(15_000)
    const parent = ConversationId.new()
    const result = await bridge.callToolIn(
      parent,
      parent,
      "search",
      textEncoder.encode('{"q":"x"}')
    )
    assert.equal(result.isError, undefined)
    const deadline = performance.now() + 10_000
    let states: { readonly state: string; readonly body: Uint8Array }[] = []
    while (performance.now() < deadline) {
      const records = await ContextAssembler.builder()
        .conversationId(parent)
        .acrossSubconversations()
        .build()
        .assemble(laser)
      states = records.flatMap((record) => {
        const envelope = record.envelope
        return envelope?.operation === "session" && envelope.taskState?.kind === "known"
          ? [{ state: envelope.taskState.name, body: envelope.body }]
          : []
      })
      if (states.some((entry) => entry.state === "Completed")) break
      await new Promise((resolve) => setTimeout(resolve, 50))
    }
    const [submitted] = states
    assert.ok(submitted !== undefined)
    assert.equal(submitted.state, "Submitted")
    assert.ok(states.some((entry) => entry.state === "Completed"))
    const start = decodeSessionStart(
      expectMap(decodeOne(submitted.body, "start"), "start"),
      "start"
    )
    assert.equal(start.parent?.toString(), parent.toString())
  } finally {
    await laser.close()
  }
})

void test("given_two_tool_workers_when_a_call_is_addressed_to_one_then_only_that_worker_should_run_it", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  const handles: AgentHandle[] = []
  const calls = new Map<string, number>()
  try {
    await laser.bootstrap(2, TopicRetention.expireAfter(86_400_000))
    for (const name of ["alpha", "beta"]) {
      const worker = AgentId.new(name)
      const handle = Agent.builder()
        .id(worker)
        .listenOn(AgentTopic.Sessions)
        .pollInterval(5)
        .handler({
          async handle(message, context): Promise<void> {
            const envelope = message.envelope
            if (envelope?.correlation === undefined || envelope.tool === undefined) return
            calls.set(name, (calls.get(name) ?? 0) + 1)
            await context.laser
              .agdx(AgentTopic.Sessions, worker, message.provenance.conversationId)
              .respond(envelope.correlation, textEncoder.encode(name))
              .send()
          }
        })
        .build()
        .spawn(laser)
      handles.push(handle)
      await handle.ready()
    }
    const bridge = new McpBridge(
      laser,
      AgentId.new("mcp-edge"),
      AgentTopic.Sessions,
      AgentTopic.Sessions,
      "laser-tools"
    ).withTimeout(10_000)
    const toBeta = await bridge.callTool("search", { q: "x" }, { target: AgentId.new("beta") })
    assert.deepEqual(toBeta.content, [{ kind: "text", text: "beta" }])
    const parent = ConversationId.new()
    const toAlpha = await bridge.callToolIn(
      parent,
      parent,
      "search",
      { q: "y" },
      { target: AgentId.new("alpha") }
    )
    assert.deepEqual(toAlpha.content, [{ kind: "text", text: "alpha" }])
    assert.deepEqual(Object.fromEntries(calls), { alpha: 1, beta: 1 })
  } finally {
    for (const handle of handles) await handle.shutdown()
    await laser.close()
  }
})

import assert from "node:assert/strict"
import { test } from "node:test"
import { createAgdx, type Agdx } from "../../src/agent/agdx.js"
import { Agent } from "../../src/agent/builder.js"
import { AgentCtx, ApprovalSlot, REQUIRE_ALL } from "../../src/agent/context.js"
import { ContractBuilder, scatterReport } from "../../src/agent/contract.js"
import {
  AgentRegistry,
  ClientMetadataRequest,
  newRegistryCache,
  type PresenceServed,
  type RegisteredCard
} from "../../src/agent/registry.js"
import {
  ReliableConsumer,
  type AgentHandler,
  type AgentMessage,
  type ReliableConsumerControl
} from "../../src/agent/reliable-consumer.js"
import type { ReplyStreamTicket } from "../../src/agent/replies.js"
import {
  ANY_ROUTE_POLICY,
  capabilitySelector,
  routeTo,
  type InboxRoute,
  type Router
} from "../../src/agent/router.js"
import { AgentScope } from "../../src/agent/scope.js"
import { Workflow } from "../../src/agent/workflow.js"
import { A2aBridge } from "../../src/bridges/a2a.js"
import { McpBridge } from "../../src/bridges/mcp.js"
import type { CallOptions } from "../../src/client/call-options.js"
import { OPEN_CAPABILITIES } from "../../src/client/capabilities.js"
import { CancelledError } from "../../src/client/errors.js"
import {
  INTERNAL_REPLY_HUB,
  INTERNAL_TRANSPORT,
  INTERNAL_VERIFIER
} from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import type {
  IggyClient,
  LaserTransport,
  MessageWithHeaders,
  PolledMessage,
  SendMessagesResponse
} from "../../src/iggy/apache-iggy.js"
import { Cursor } from "../../src/stream/cursor.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import { commandEnvelope, parseAgentId } from "../../src/wire/agent.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { encodeClientMetadataList } from "../../src/wire/clients.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId
} from "../../src/wire/ids.js"

const WORKER = AgentId.new("worker")
const ALPHA = AgentId.new("alpha")
const BETA = AgentId.new("beta")
const SUMMARIZE = capabilitySelector("summarize", ANY_ROUTE_POLICY)
const FIXED_INBOX: InboxRoute = { kind: "fixed", topic: "work" }

void test("given_an_aborted_signal_when_agdx_records_are_sent_then_should_reject_with_cancelled_before_sending", async () => {
  const recorder = new RecordingTransport()
  const agdx = agdxOver(recorder)
  const signal = AbortSignal.abort()
  await assert.rejects(agdx.emit(bytes("event")).send({ signal }), CancelledError)
  await assert.rejects(agdx.status("task").sendReceipt({ signal }), CancelledError)
  const stream = agdx.stream(CorrelationId.fromU128(7n), "chat")
  await assert.rejects(stream.write(bytes("one"), { signal }), CancelledError)
  await assert.rejects(stream.finish("stop", undefined, { signal }), CancelledError)
  await assert.rejects(
    stream.fail({ code: { kind: "known", name: "Internal" }, retryable: false }, { signal }),
    CancelledError
  )
  const buffered = agdx.stream(CorrelationId.fromU128(8n), "chat").buffered(10, 60_000)
  await buffered.write(bytes("held"))
  await assert.rejects(buffered.flush({ signal }), CancelledError)
  await assert.rejects(
    agdx.requestInput("replies", bytes("approve?"), 1_000, { signal }),
    CancelledError
  )
  assert.equal(recorder.appends.length, 0)
  assert.equal(recorder.polls, 0)
  // An aborted terminal leaves the stream open.
  await stream.finish("stop")
  assert.equal(recorder.appends.length, 1)
})

void test(
  "given_an_agdx_send_waiting_for_its_append_when_the_signal_aborts_then_should_reject_with_cancelled",
  { timeout: 5_000 },
  async () => {
    const held = new Held()
    const recorder = new RecordingTransport()
    recorder.holdAppends = held
    const controller = new AbortController()
    const sending = agdxOver(recorder).emit(bytes("event")).send({ signal: controller.signal })
    await tick()
    assert.equal(recorder.appends.length, 1)
    controller.abort()
    try {
      await assert.rejects(sending, CancelledError)
    } finally {
      held.release()
    }
  }
)

void test(
  "given_an_aborted_signal_when_a_handler_replies_then_should_reject_with_cancelled_before_sending",
  { timeout: 5_000 },
  async () => {
    const held = new Held()
    const recorder = new RecordingTransport()
    const { laser, calls } = laserOver(recorder, new FakeHub(held))
    const ctx = AgentCtx.create(laser, commandMessage(), {
      agent: WORKER,
      respondOn: "replies",
      requestAt: { streamId: 1, topicId: 1, partitionId: 0, offset: 4n },
      approval: new ApprovalSlot("approvals", undefined)
    })
    const signal = AbortSignal.abort()
    try {
      await assert.rejects(ctx.respond(bytes("done"), { signal }), CancelledError)
      await assert.rejects(ctx.replyOn("elsewhere", bytes("done"), { signal }), CancelledError)
      await assert.rejects(
        ctx.send("elsewhere", bytes("note"), { conversationId: ConversationId.new() }, { signal }),
        CancelledError
      )
      await assert.rejects(ctx.respondInput("replies", bytes("yes"), { signal }), CancelledError)
      await assert.rejects(
        ctx.approvalGate(BETA, "replies", bytes("approve?"), 1_000, { signal }),
        CancelledError
      )
      await assert.rejects(
        ctx.approvalGate(BETA, "approvals", bytes("approve?"), 1_000, { signal }),
        CancelledError
      )
      await assert.rejects(
        ctx.fanOut(SUMMARIZE, bytes("work"), REQUIRE_ALL, 1_000, { signal }),
        CancelledError
      )
    } finally {
      held.release()
    }
    assert.deepEqual(calls, [])
    assert.equal(recorder.appends.length, 0)
    assert.equal(recorder.polls, 0)
  }
)

void test(
  "given_a_fan_out_waiting_on_its_branches_when_the_signal_aborts_then_should_reject_and_cancel_every_branch",
  { timeout: 5_000 },
  async () => {
    const held = new Held()
    const branches: AbortSignal[] = []
    const { laser } = laserOver(
      new RecordingTransport(),
      new FakeHub(held),
      [card(ALPHA), card(BETA)],
      {
        request: (...call: readonly unknown[]) => {
          const options = call[5] as CallOptions
          if (options.signal !== undefined) branches.push(options.signal)
          return held.promise()
        }
      }
    )
    const ctx = AgentCtx.create(laser, commandMessage(), { agent: WORKER, respondOn: "replies" })
    const controller = new AbortController()
    const gathering = ctx.fanOut(SUMMARIZE, bytes("work"), REQUIRE_ALL, 60_000, {
      signal: controller.signal
    })
    await tick()
    assert.equal(branches.length, 2)
    controller.abort()
    try {
      await assert.rejects(gathering, CancelledError)
      assert.ok(branches.every((branch) => branch.aborted))
    } finally {
      held.release()
    }
  }
)

void test(
  "given_an_aborted_signal_when_a_contract_or_scatter_is_sent_then_should_reject_with_cancelled_before_resolving",
  { timeout: 5_000 },
  async () => {
    const held = new Held()
    const recorder = new RecordingTransport()
    const hub = new FakeHub(held)
    const { laser, calls } = laserOver(recorder, hub, [card(ALPHA), card(BETA)])
    const signal = AbortSignal.abort()
    try {
      await assert.rejects(
        ContractBuilder.create(laser, routeTo(BETA))
          .from(WORKER)
          .payload(bytes("work"))
          .inboxRoute(FIXED_INBOX)
          .send({ signal }),
        CancelledError
      )
      await assert.rejects(
        scatterReport(
          laser,
          WORKER,
          SUMMARIZE,
          bytes("work"),
          FIXED_INBOX,
          REQUIRE_ALL,
          1_000,
          undefined,
          { signal }
        ),
        CancelledError
      )
    } finally {
      held.release()
    }
    assert.deepEqual(calls, [])
    assert.equal(hub.subscribed, 0)
    assert.equal(recorder.appends.length, 0)
  }
)

void test(
  "given_a_contract_waiting_for_its_reply_when_the_signal_aborts_then_should_reject_and_remove_the_waiter",
  { timeout: 5_000 },
  async () => {
    const held = new Held()
    const recorder = new RecordingTransport()
    const hub = new FakeHub(held)
    const { laser } = laserOver(recorder, hub)
    const controller = new AbortController()
    const sending = ContractBuilder.create(laser, routeTo(BETA))
      .from(WORKER)
      .payload(bytes("work"))
      .inboxRoute(FIXED_INBOX)
      .deadline(60_000)
      .send({ signal: controller.signal })
    await tick()
    assert.equal(recorder.appends.length, 1)
    assert.equal(hub.waits.length, 1)
    controller.abort()
    try {
      await assert.rejects(sending, CancelledError)
      await tick()
      assert.equal(hub.waits[0]?.aborted, true)
      assert.equal(hub.cancelled, 1)
    } finally {
      held.release()
    }
  }
)

void test(
  "given_a_scatter_waiting_on_its_branches_when_the_signal_aborts_then_should_reject_and_cancel_every_branch",
  { timeout: 5_000 },
  async () => {
    const held = new Held()
    const recorder = new RecordingTransport()
    const hub = new FakeHub(held)
    const { laser } = laserOver(recorder, hub, [card(ALPHA), card(BETA)])
    const controller = new AbortController()
    const gathering = scatterReport(
      laser,
      WORKER,
      SUMMARIZE,
      bytes("work"),
      FIXED_INBOX,
      REQUIRE_ALL,
      60_000,
      undefined,
      { signal: controller.signal }
    )
    await tick()
    assert.equal(hub.waits.length, 2)
    controller.abort()
    try {
      await assert.rejects(gathering, CancelledError)
      await tick()
      assert.equal(hub.cancelled, 2)
    } finally {
      held.release()
    }
  }
)

void test(
  "given_a_running_agent_when_a_ready_or_join_wait_aborts_then_should_reject_and_keep_the_agent_running",
  { timeout: 5_000 },
  async (t) => {
    let stopping: AbortSignal | undefined
    t.mock.method(
      ReliableConsumer.prototype,
      "run",
      async (_laser: Laser, _handler: AgentHandler, control: ReliableConsumerControl) => {
        stopping = control.signal
        await new Promise<void>((resolve) => {
          control.signal?.addEventListener(
            "abort",
            () => {
              resolve()
            },
            { once: true }
          )
        })
      }
    )
    const handle = Agent.builder()
      .id(WORKER)
      .listenOn("agent.sessions")
      .handler({ handle: () => Promise.resolve() })
      .build()
      .spawn({} as Laser)
    await assert.rejects(handle.ready({ signal: AbortSignal.abort() }), CancelledError)
    const controller = new AbortController()
    const ready = handle.ready({ signal: controller.signal })
    const joined = handle.join({ signal: controller.signal })
    controller.abort()
    await assert.rejects(ready, CancelledError)
    await assert.rejects(joined, CancelledError)
    assert.equal(stopping?.aborted, false)
    await handle.shutdown()
  }
)

void test("given_an_aborted_signal_when_the_registry_reads_then_should_reject_with_cancelled_before_reading", async () => {
  const recorder = new RecordingTransport()
  let probes = 0
  const registry = registryOver(recorder, () => {
    probes += 1
    return Promise.resolve("yes")
  })
  const signal = AbortSignal.abort()
  await assert.rejects(registry.refresh(undefined, { signal }), CancelledError)
  await assert.rejects(registry.refreshPresence({ signal }), CancelledError)
  const request = ClientMetadataRequest.create(recorder.transport)
  await assert.rejects(request.page({ signal }), CancelledError)
  await assert.rejects(request.all({ signal }), CancelledError)
  assert.equal(recorder.polls, 0)
  assert.equal(recorder.managedSends, 0)
  assert.equal(probes, 0)
})

void test(
  "given_a_presence_read_of_an_unproven_server_waiting_when_the_signal_aborts_then_should_reject_with_cancelled",
  { timeout: 5_000 },
  async () => {
    const held = new Held()
    const recorder = new RecordingTransport()
    recorder.holdManaged = held
    const registry = registryOver(recorder, () => Promise.resolve("unknown"))
    const controller = new AbortController()
    const reading = registry.refreshPresence({ signal: controller.signal })
    await tick()
    assert.equal(recorder.managedSends, 1)
    controller.abort()
    try {
      await assert.rejects(reading, CancelledError)
    } finally {
      held.release()
    }
  }
)

void test("given_client_metadata_paging_when_the_signal_aborts_then_should_reject_and_request_no_more_pages", async () => {
  const recorder = new RecordingTransport()
  const controller = new AbortController()
  recorder.managedAnswer = (send) => {
    controller.abort()
    return encodeNamed(
      encodeClientMetadataList({ clients: [], ...(send < 3 ? { nextCursor: send * 10 } : {}) })
    )
  }
  await assert.rejects(
    ClientMetadataRequest.create(recorder.transport).all({ signal: controller.signal }),
    CancelledError
  )
  assert.equal(recorder.managedSends, 1)
})

void test("given_an_aborted_signal_when_an_agent_scope_publishes_then_should_reject_with_cancelled_before_sending", async () => {
  const calls: string[] = []
  const laser = {
    sendAgent: () => {
      calls.push("send")
      return Promise.resolve()
    },
    publishCard: () => {
      calls.push("card")
      return Promise.resolve()
    },
    advertisePresence: () => {
      calls.push("presence")
      return Promise.resolve()
    }
  } as unknown as Laser
  const scope = AgentScope.create(laser, WORKER)
  const signal = AbortSignal.abort()
  await assert.rejects(
    scope.send("work", bytes("note"), { conversationId: ConversationId.new() }, { signal }),
    CancelledError
  )
  await assert.rejects(scope.publishCard({ capabilities: [] }, { signal }), CancelledError)
  await assert.rejects(scope.advertise("work", [], { signal }), CancelledError)
  assert.deepEqual(calls, [])
})

void test(
  "given_an_advertise_waiting_on_its_card_when_the_signal_aborts_then_should_reject_and_advertise_no_presence",
  { timeout: 5_000 },
  async () => {
    const calls: string[] = []
    let publish: (() => void) | undefined
    const laser = {
      publishCard: () => {
        calls.push("card")
        return new Promise<void>((resolve) => {
          publish = resolve
        })
      },
      advertisePresence: () => {
        calls.push("presence")
        return Promise.resolve()
      }
    } as unknown as Laser
    const held = new Held()
    held.keepAlive()
    const controller = new AbortController()
    const advertising = AgentScope.create(laser, WORKER).advertise("work", [], {
      signal: controller.signal
    })
    await tick()
    controller.abort()
    try {
      await assert.rejects(advertising, CancelledError)
      publish?.()
      await tick()
      assert.deepEqual(calls, ["card"])
    } finally {
      held.release()
    }
  }
)

void test(
  "given_an_aborted_signal_when_an_mcp_tool_is_called_then_should_reject_with_cancelled_before_sending",
  { timeout: 5_000 },
  async () => {
    const held = new Held()
    const recorder = new RecordingTransport()
    const hub = new FakeHub(held)
    const bridge = mcpOver(laserOver(recorder, hub).laser)
    const signal = AbortSignal.abort()
    const parent = ConversationId.new()
    try {
      await assert.rejects(bridge.callTool("search", { q: "x" }, { signal }), CancelledError)
      await assert.rejects(
        bridge.callToolWithHops(["a2a"], "search", { q: "x" }, { signal }),
        CancelledError
      )
      await assert.rejects(
        bridge.callToolIn(parent, parent, "search", { q: "x" }, { signal }),
        CancelledError
      )
      await assert.rejects(
        bridge.handleRpc(
          { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "search" } },
          { signal }
        ),
        CancelledError
      )
    } finally {
      held.release()
    }
    assert.equal(hub.subscribed, 0)
    assert.equal(recorder.appends.length, 0)
  }
)

void test(
  "given_an_mcp_tool_call_waiting_for_its_result_when_the_signal_aborts_then_should_reject_and_remove_the_waiter",
  { timeout: 5_000 },
  async () => {
    const held = new Held()
    const recorder = new RecordingTransport()
    const hub = new FakeHub(held)
    const bridge = mcpOver(laserOver(recorder, hub).laser)
    const controller = new AbortController()
    const calling = bridge.handleRpc(
      { jsonrpc: "2.0", id: 1, method: "tools/call", params: { name: "search" } },
      { signal: controller.signal }
    )
    await tick()
    assert.equal(recorder.appends.length, 1)
    controller.abort()
    try {
      await assert.rejects(calling, CancelledError)
      await tick()
      assert.equal(hub.waits[0]?.aborted, true)
      assert.equal(hub.cancelled, 1)
    } finally {
      held.release()
    }
  }
)

void test("given_an_aborted_signal_when_an_a2a_bridge_is_called_then_should_reject_with_cancelled_before_sending", async () => {
  const recorder = new RecordingTransport()
  const { laser } = laserOver(recorder, new FakeHub(new Held()))
  const bridge = new A2aBridge(laser, AgentId.new("bridge"), "agent.sessions", "agent.sessions")
  const task = ConversationId.new()
  const signal = AbortSignal.abort()
  for (const surface of [bridge, bridge.caller("alice")]) {
    await assert.rejects(surface.submit({ message: { text: "hi" } }, { signal }), CancelledError)
    await assert.rejects(
      surface.submitWithHops(["mcp"], { message: { text: "hi" } }, { signal }),
      CancelledError
    )
    await assert.rejects(
      surface.submitIn(task, task, { message: { text: "hi" } }, { signal }),
      CancelledError
    )
    await assert.rejects(surface.task(task.toString(), { signal }), CancelledError)
    await assert.rejects(surface.cancel(task.toString(), { signal }), CancelledError)
    await assert.rejects(
      surface.handleRpc(
        { jsonrpc: "2.0", id: 1, method: "GetTask", params: { id: task.toString() } },
        { signal }
      ),
      CancelledError
    )
  }
  assert.equal(recorder.appends.length, 0)
  assert.equal(recorder.polls, 0)
})

void test("given_an_a2a_task_lookup_scanning_the_log_when_the_signal_aborts_then_should_reject_and_poll_no_more", async () => {
  const recorder = new RecordingTransport()
  const controller = new AbortController()
  recorder.pollAnswer = (poll) => {
    if (poll === 3) controller.abort()
    return [{ payload: Uint8Array.of(1), partitionId: 0, offset: BigInt(poll), headers: new Map() }]
  }
  const { laser } = laserOver(recorder, new FakeHub(new Held()))
  const bridge = new A2aBridge(laser, AgentId.new("bridge"), "agent.sessions", "agent.sessions")
  await assert.rejects(
    bridge.task(ConversationId.new().toString(), { signal: controller.signal }),
    CancelledError
  )
  assert.equal(recorder.polls, 3)
})

void test("given_an_aborted_signal_when_a_workflow_runs_then_should_reject_with_cancelled_before_writing", async () => {
  const calls: string[] = []
  const laser = {
    sessions: () => {
      calls.push("sessions")
      throw new Error("the run reached the session")
    }
  } as unknown as Laser
  const workflow = Workflow.create(laser, "orchestrator")
  const step = workflow.step("triage", routeTo(WORKER), () => new Uint8Array())
  await assert.rejects(workflow.run({ signal: AbortSignal.abort() }), CancelledError)
  await assert.rejects(step.run({ signal: AbortSignal.abort() }), CancelledError)
  assert.deepEqual(calls, [])
})

void test("given_an_agent_write_waiting_out_its_retry_delay_when_the_signal_aborts_then_should_reject_and_send_no_retry", async () => {
  const conversation = ConversationId.new()
  const correlation = CorrelationId.fromU128(7n)
  const writing: readonly (readonly [
    string,
    (laser: Laser, signal: AbortSignal) => Promise<unknown>
  ])[] = [
    [
      "AgdxSend.send",
      (laser, signal) =>
        laser.agdx("agent.sessions", WORKER, conversation).emit(bytes("e")).send({ signal })
    ],
    [
      "AgdxStream.write",
      (laser, signal) =>
        laser
          .agdx("agent.sessions", WORKER, conversation)
          .stream(correlation, "chat")
          .write(bytes("c"), { signal })
    ],
    [
      "AgdxStream.flush",
      async (laser, signal) => {
        const stream = laser
          .agdx("agent.sessions", WORKER, conversation)
          .stream(correlation, "chat")
          .buffered(10, 60_000)
        await stream.write(bytes("c"))
        return stream.flush({ signal })
      }
    ],
    [
      "AgdxStream.finish",
      (laser, signal) =>
        laser
          .agdx("agent.sessions", WORKER, conversation)
          .stream(correlation, "chat")
          .finish("stop", undefined, { signal })
    ],
    [
      "Laser.sendAgent",
      (laser, signal) =>
        laser.sendAgent(
          "agent.sessions",
          bytes("s"),
          { conversationId: conversation, agent: WORKER },
          { signal }
        )
    ],
    [
      "Laser.publishCard",
      (laser, signal) => laser.publishCard(WORKER, card(WORKER).card, { signal })
    ],
    ["Laser.quarantine", (laser, signal) => laser.quarantine(ALPHA, BETA, { signal })],
    [
      "Laser.redriveDeadLetter",
      (laser, signal) => {
        const transport = laser[INTERNAL_TRANSPORT]()
        transport.resolveStreamTopicNames = () =>
          Promise.resolve({ stream: "agents", topic: "agent.sessions" })
        transport.pollMessages = () =>
          Promise.resolve([
            {
              partitionId: 0,
              offset: 7n,
              payload: bytes("w"),
              headers: new Map(),
              timestampMicros: 1n
            }
          ])
        return laser.redriveDeadLetter(
          {
            source: { streamId: 1, topicId: 2, partitionId: 0, offset: 7n },
            reason: { kind: "known", name: "Rejected" },
            attempts: 1,
            payload: new Uint8Array()
          },
          { signal }
        )
      }
    ]
  ]
  const late: string[] = []
  for (const [name, write] of writing) {
    const controller = new AbortController()
    let attempts = 0
    const sent = (): number => attempts
    const root = await Laser.builder()
      .client(refusingClient(() => (attempts += 1)))
      .capabilities(OPEN_CAPABILITIES)
      .publishRetryBackoff(40)
      .publishTimeout(1_000)
      .connect()
    try {
      const sending = write(root.withDefaultStream("agents"), controller.signal)
      for (let turn = 0; sent() === 0 && turn < 200; turn += 1) await tick()
      await tick()
      assert.equal(sent(), 1, `${name} sends its first attempt`)
      controller.abort()
      await assert.rejects(sending, CancelledError, name)
      await new Promise((resolve) => setTimeout(resolve, 100))
      if (sent() > 1) late.push(name)
    } finally {
      await root.close()
    }
  }
  assert.deepEqual(late, [], "no agent write starts an attempt after the abort")
})

// Promises that never settle, each with a timer that keeps the test alive
// while it waits.
class Held {
  private readonly timers: ReturnType<typeof setTimeout>[] = []

  keepAlive(): void {
    this.timers.push(setTimeout(() => undefined, 10_000))
  }

  promise<T>(): Promise<T> {
    this.keepAlive()
    return new Promise<T>(() => undefined)
  }

  release(): void {
    for (const timer of this.timers) clearTimeout(timer)
  }
}

// A transport that records every append, poll and managed request. An
// aborted poll fails before anything is sent.
class RecordingTransport {
  readonly appends: (readonly MessageWithHeaders[])[] = []
  polls = 0
  managedSends = 0
  holdAppends: Held | undefined
  holdManaged: Held | undefined
  pollAnswer: (poll: number) => readonly PolledMessage[] = () => []
  managedAnswer: (send: number) => Uint8Array = () =>
    encodeNamed(encodeClientMetadataList({ clients: [] }))

  get transport(): LaserTransport {
    return this as unknown as LaserTransport
  }

  findTopicPartitionCount(): Promise<number> {
    return Promise.resolve(1)
  }

  sendMessagesWithHeaders(
    _stream: string,
    _topic: string,
    messages: readonly MessageWithHeaders[]
  ): Promise<SendMessagesResponse> {
    this.appends.push(messages)
    return this.holdAppends?.promise() ?? Promise.resolve({ confirmations: [] })
  }

  pollMessages(
    _stream: string,
    _topic: string,
    _target: unknown,
    _strategy: unknown,
    _count: number,
    _autoCommit: boolean,
    _wait?: unknown,
    signal?: AbortSignal
  ): Promise<readonly PolledMessage[]> {
    if (signal?.aborted === true) return Promise.reject(new CancelledError("poll aborted"))
    this.polls += 1
    return Promise.resolve(this.pollAnswer(this.polls))
  }

  sendManaged(): Promise<Uint8Array> {
    this.managedSends += 1
    return this.holdManaged?.promise() ?? Promise.resolve(this.managedAnswer(this.managedSends))
  }
}

// A reply hub whose waits never answer and end on their signal, as the real
// hub's do.
class FakeHub {
  subscribed = 0
  cancelled = 0
  readonly waits: (AbortSignal | undefined)[] = []

  constructor(private readonly held: Held) {}

  subscribeStream(): ReplyStreamTicket {
    this.subscribed += 1
    return {
      next: (_timeoutMs: number, signal?: AbortSignal) => {
        this.waits.push(signal)
        this.held.keepAlive()
        return new Promise<AgentMessage>((_resolve, reject) => {
          if (signal?.aborted === true) reject(new CancelledError("reply wait aborted"))
          signal?.addEventListener(
            "abort",
            () => {
              reject(new CancelledError("reply wait aborted"))
            },
            { once: true }
          )
        })
      },
      cancel: () => {
        this.cancelled += 1
      }
    }
  }
}

// The `Laser` surface the agent runtime calls, naming each registry or reply
// hub it was asked for.
function laserOver(
  recorder: RecordingTransport,
  hub: FakeHub,
  cards: readonly RegisteredCard[] = [],
  extra: Readonly<Record<string, unknown>> = {}
): { readonly laser: Laser; readonly calls: string[] } {
  const calls: string[] = []
  const registry = {
    refresh: () => Promise.resolve(0),
    refreshPresence: () => Promise.resolve(0),
    resolve: () => cards,
    isQuarantined: () => false,
    principalFor: () => undefined,
    inboxFor: (agent: AgentId) => `${agent.asStr()}.inbox`
  }
  const laser = {
    defaultStream: "agents",
    agdx: (topic: string, source: AgentId, conversation: ConversationId) =>
      createAgdx(recorder.transport, "agents", topic, source, conversation),
    sendAgent: () => {
      calls.push("sendAgent")
      return Promise.resolve()
    },
    agentRegistry: () => {
      calls.push("agentRegistry")
      return Promise.resolve(registry)
    },
    contract: (router: Router) => ContractBuilder.create(laser, router),
    spawnSubconversation: () => ({ conversationId: ConversationId.new() }),
    [INTERNAL_TRANSPORT]: () => recorder.transport,
    [INTERNAL_VERIFIER]: () => undefined,
    [INTERNAL_REPLY_HUB]: () => {
      calls.push("replyHub")
      return Promise.resolve(hub)
    },
    ...extra
  } as unknown as Laser
  return { laser, calls }
}

function registryOver(
  recorder: RecordingTransport,
  presenceServed: () => Promise<PresenceServed>
): AgentRegistry {
  return AgentRegistry.create(
    Cursor.create(recorder.transport, "agents", "agent.registry", [0]),
    newRegistryCache(),
    () => ClientMetadataRequest.create(recorder.transport),
    () => 0n,
    undefined,
    presenceServed
  )
}

function agdxOver(recorder: RecordingTransport): Agdx {
  return createAgdx(recorder.transport, "agents", "agent.sessions", WORKER, ConversationId.new())
}

function mcpOver(laser: Laser): McpBridge {
  return new McpBridge(
    laser,
    AgentId.new("mcp"),
    "agent.sessions",
    "agent.sessions",
    "tools"
  ).withTool("search", undefined, { type: "object" })
}

function card(agent: AgentId): RegisteredCard {
  return {
    agent,
    card: { capabilities: [{ skillId: "summarize" }], inbox: `${agent.asStr()}.inbox` },
    observedAtMicros: 0n
  }
}

// A correlated command from `caller`, the record a handler answers.
function commandMessage(): AgentMessage {
  const conversation = ConversationId.new()
  return {
    provenance: { conversationId: conversation, agent: AgentId.new("caller") },
    payload: bytes("work"),
    id: { partitionId: 0, offset: 4n },
    envelope: commandEnvelope(
      RecordId.fromU128(1n),
      WireConversationId.parse(conversation.toString()),
      parseAgentId("caller"),
      CorrelationId.fromU128(9n),
      bytes("work")
    )
  }
}

function bytes(text: string): Uint8Array {
  return new TextEncoder().encode(text)
}

function tick(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve))
}

// A client whose every send is refused with a transient server code.
function refusingClient(sent: () => void): IggyClient {
  return {
    clientProvider: () => Promise.resolve({ protocol: "vsr" }),
    stream: {
      ensure: () => Promise.resolve(),
      get: () => Promise.resolve({ id: 1, name: "agents" })
    },
    topic: { get: () => Promise.resolve({ id: 2, name: "agent.registry", partitionsCount: 1 }) },
    message: {
      send: () => {
        sent()
        return Promise.reject(Object.assign(new Error("not accepted"), { errorCode: 58 }))
      }
    },
    destroy: () => Promise.resolve()
  } as unknown as IggyClient
}

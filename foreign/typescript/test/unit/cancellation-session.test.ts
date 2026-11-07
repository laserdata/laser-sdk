import assert from "node:assert/strict"
import { test } from "node:test"
import { createAgdx } from "../../src/agent/agdx.js"
import { Agent } from "../../src/agent/builder.js"
import { type LeaseRegistry, SessionLease } from "../../src/agent/lease.js"
import {
  ReliableConsumer,
  type AgentHandler,
  type ReliableConsumerControl
} from "../../src/agent/reliable-consumer.js"
import type { CallOptions } from "../../src/client/call-options.js"
import { OPEN_CAPABILITIES, type Capabilities } from "../../src/client/capabilities.js"
import { CancelledError } from "../../src/client/errors.js"
import {
  INTERNAL_LAYOUTS,
  INTERNAL_PUBLISH_CONTROL,
  INTERNAL_SESSION_LEASES,
  INTERNAL_TRANSPORT
} from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import { ContextScope } from "../../src/context-scope.js"
import { Checkpoint, LastN } from "../../src/context.js"
import {
  ApacheIggyTransport,
  type IggyClient,
  type LaserTransport,
  type MessageWithHeaders
} from "../../src/iggy/apache-iggy.js"
import { MemoryHandle } from "../../src/memory/handle.js"
import { MemoryTopicBuilder } from "../../src/memory/topic.js"
import {
  type ConsolidationReport,
  type Memory,
  type MemoryItem,
  MemoryId,
  MemoryKind
} from "../../src/memory/types.js"
import { ModelCall, ModelRequest, ToolCall } from "../../src/session-ops.js"
import { Sessions, TopicRetention } from "../../src/session.js"
import type { SnapshotStore } from "../../src/snapshot.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import {
  type AgentEnvelope,
  type AgentErrorBody,
  OPERATION_STATE_DELTA,
  commandEnvelope,
  decodeAgentEnvelope
} from "../../src/wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId
} from "../../src/wire/ids.js"
import {
  type SessionChangesView,
  type SessionReply,
  encodeSessionReply
} from "../../src/wire/session.js"

const CAPABILITIES: Capabilities = { ...OPEN_CAPABILITIES, managed: true, sessions: true }
const NOT_REGISTERED = reply({
  kind: "err",
  error: { kind: "notRegistered", message: "the stream is not registered yet" }
})
const FAILURE: AgentErrorBody = { code: { kind: "known", name: "Internal" }, retryable: false }
const planner = AgentId.new("planner")

void test("given_an_aborted_signal_when_a_session_index_read_is_called_then_should_reject_with_cancelled_before_sending", async () => {
  const { laser, calls } = indexLaser(() => Promise.resolve(NOT_REGISTERED))
  const sessions = Sessions.create(laser)
  const id = ConversationId.new()
  const options = { signal: AbortSignal.abort() }
  const reads: readonly (() => Promise<unknown>)[] = [
    () => sessions.get(id, options),
    () => sessions.state(id, 0, options),
    () => sessions.links(id, undefined, options),
    () => sessions.sources(id, options),
    () => sessions.changes(0, 1, options),
    () => sessions.list().fetch(options),
    () => sessions.events(id).fetch(options),
    () => sessions.watch(1, options),
    () => sessions.unregister(options),
    () => sessions.bootstrap(1, TopicRetention.expireAfter(60_000), options)
  ]
  for (const read of reads) await assert.rejects(read(), CancelledError)
  assert.deepEqual(calls, [])
})

void test(
  "given_a_session_index_read_waiting_for_its_reply_when_the_signal_aborts_then_should_reject_with_cancelled",
  { timeout: 5_000 },
  async () => {
    // The reply never comes, and the timer keeps the test alive while it waits.
    let held: ReturnType<typeof setTimeout> | undefined
    const { laser, calls } = indexLaser(
      () =>
        new Promise<Uint8Array>(() => {
          held = setTimeout(() => undefined, 60_000)
        })
    )
    const controller = new AbortController()
    const read = Sessions.create(laser).get(ConversationId.new(), { signal: controller.signal })
    await until(() => readsOf(calls) === 1)
    controller.abort()
    try {
      await assert.rejects(read, CancelledError)
    } finally {
      clearTimeout(held)
    }
  }
)

void test(
  "given_bootstrap_waiting_for_its_registration_when_the_signal_aborts_then_should_reject_with_cancelled_and_stop_polling",
  { timeout: 5_000 },
  async () => {
    const { laser, calls } = indexLaser(() => Promise.resolve(NOT_REGISTERED), 2_000)
    const controller = new AbortController()
    const bootstrap = Sessions.create(laser).bootstrap(1, TopicRetention.expireAfter(60_000), {
      signal: controller.signal
    })
    await until(() => readsOf(calls) === 2)
    controller.abort()
    await assert.rejects(bootstrap, CancelledError)
    const reads = readsOf(calls)
    await pause(250)
    assert.equal(readsOf(calls), reads, "bootstrap must not poll after the abort")
  }
)

void test(
  "given_a_session_watch_polling_for_changes_when_the_signal_aborts_then_should_reject_with_cancelled_and_stop_reading",
  { timeout: 5_000 },
  async () => {
    let held: ReturnType<typeof setTimeout> | undefined
    const replies = [reply(changes(changeView(7n))), reply(changes(changeView(7n)))]
    const { laser, calls } = indexLaser(() => {
      const next = replies.shift()
      if (next !== undefined) return Promise.resolve(next)
      return new Promise<Uint8Array>(() => {
        held = setTimeout(() => undefined, 60_000)
      })
    })
    const watch = await Sessions.create(laser).watch(1)
    const controller = new AbortController()
    const next = watch.next({ signal: controller.signal })
    await until(() => readsOf(calls) === 3)
    controller.abort()
    try {
      await assert.rejects(next, CancelledError)
      await pause(20)
      assert.equal(readsOf(calls), 3, "the watch must not read after the abort")
    } finally {
      clearTimeout(held)
    }
  }
)

void test("given_an_aborted_signal_when_a_session_call_is_made_then_should_reject_with_cancelled_before_any_io", async () => {
  const { laser, sent, touched, acquired } = laneLaser()
  const sessions = Sessions.create(laser)
  const session = sessions.open(ConversationId.new()).asAgent(planner)
  const state = session.state()
  const control = sessions
    .control("agents", session.conversation)
    .asOperator(AgentId.new("operator"))
  const request = new ModelRequest("model", new Uint8Array())
  const checkpoint = Checkpoint.fromJSON('{"per_topic":{}}')
  const fold = (count: number): number => count + 1
  const envelope = commandEnvelope(
    RecordId.fromU128(3n),
    WireConversationId.parse(session.conversation.toString()),
    planner.wireId(),
    CorrelationId.fromU128(4n),
    new Uint8Array([1])
  )
  const compaction = {
    summaryAt: { kind: "memory" as const, id: "summary-1" },
    covered: [[1, 0, 0n, 3n] as const],
    summarizer: { name: "summarizer", version: "1" }
  }
  const options = { signal: AbortSignal.abort() }
  const calls: readonly (() => Promise<unknown>)[] = [
    () => session.append(envelope, options),
    () => session.end(options),
    () => session.fail(FAILURE, options),
    () => session.cancel(options),
    () => session.tool("search", {}, options),
    () => session.model(request, undefined, options),
    () => session.recordModelCall(request, { body: new Uint8Array() }, undefined, options),
    () => session.recordRetrieval(undefined, [], options),
    () => session.recordCompaction(compaction, options),
    () =>
      ModelCall.create(session, CorrelationId.fromU128(5n), "chat").complete(
        { body: new Uint8Array() },
        options
      ),
    () => ModelCall.create(session, CorrelationId.fromU128(5n), "chat").fail(FAILURE, options),
    () =>
      ToolCall.create(session, CorrelationId.fromU128(6n), "search").complete(
        new Uint8Array(),
        options
      ),
    () => ToolCall.create(session, CorrelationId.fromU128(6n), "search").fail(FAILURE, options),
    () => state.set("key", 1, options),
    () => state.patch([{ op: "add", path: "/key", value: 1 }], options),
    () => state.replace({ key: 1 }, options),
    () => state.snapshot(options),
    () => state.get(options),
    () => session.assemble(new LastN(5), options),
    () => session.context(options),
    () => session.contextWith(new LastN(5), options),
    () => session.checkpoint(options),
    () => session.turnsAt(checkpoint, options),
    () => session.turnsSince(checkpoint, options),
    () => session.stateAt(checkpoint, 0, fold, options),
    () => session.replay(checkpoint, 0, fold, options),
    () => session.status(options),
    () => session.pendingControl(options),
    () => session.cancelRequested(options),
    () => session.overBudget(options),
    () => session.parked(options),
    () => sessions.start().agent(planner).begin(options),
    () => sessions.create("ticket").agent(planner).resume(options),
    () => sessions.submit(planner, new Uint8Array()).from(AgentId.new("client")).send(options),
    () => control.pause(options),
    () => control.resume(options),
    () => control.cancel(options),
    () => control.forceCancel(options)
  ]
  for (const call of calls) await assert.rejects(call(), CancelledError)
  assert.deepEqual(touched, [])
  assert.deepEqual(sent, [])
  assert.equal(acquired(), 0)
})

void test(
  "given_a_session_write_waiting_for_the_server_when_the_signal_aborts_then_should_reject_with_cancelled",
  { timeout: 5_000 },
  async () => {
    let held: ReturnType<typeof setTimeout> | undefined
    const { laser, sent } = laneLaser(
      () =>
        new Promise<void>(() => {
          held = setTimeout(() => undefined, 60_000)
        })
    )
    const session = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
    const controller = new AbortController()
    const call = session.tool("search", { query: "login" }, { signal: controller.signal })
    await until(() => sent.length === 1)
    controller.abort()
    try {
      await assert.rejects(call, CancelledError)
    } finally {
      clearTimeout(held)
    }
  }
)

void test(
  "given_a_session_begin_whose_start_write_is_in_flight_when_the_signal_aborts_then_should_reject_and_take_no_lease",
  { timeout: 5_000 },
  async () => {
    const gate = deferred<undefined>()
    const { laser, sent, acquired } = laneLaser(() => gate.promise)
    const controller = new AbortController()
    const begun = Sessions.create(laser).start().agent(planner).begin({ signal: controller.signal })
    await until(() => sent.length === 1)
    controller.abort()
    gate.resolve(undefined)
    await assert.rejects(begun, CancelledError)
    await pause(10)
    assert.equal(acquired(), 0, "an aborted begin must not leave a lease behind")
  }
)

void test(
  "given_a_session_run_whose_work_is_still_running_when_the_signal_aborts_then_should_reject_release_the_lease_and_write_no_terminal",
  { timeout: 5_000 },
  async () => {
    const { laser, sent } = laneLaser()
    const session = Sessions.create(laser).open(ConversationId.new()).asAgent(planner)
    const running = heldLease(session.conversation)
    const work = deferred<number>()
    const controller = new AbortController()
    const run = session.run(running.lease, () => work.promise, { signal: controller.signal })
    controller.abort()
    work.resolve(42)
    await assert.rejects(run, CancelledError)
    await pause(10)
    assert.equal(running.released(), true)
    assert.deepEqual(sent, [], "an aborted run writes no terminal")

    let started = false
    const early = heldLease(session.conversation)
    await assert.rejects(
      session.run(
        early.lease,
        () => {
          started = true
          return 1
        },
        { signal: AbortSignal.abort() }
      ),
      CancelledError
    )
    assert.equal(started, false, "work must not start after the abort")
    assert.equal(early.released(), true)
  }
)

void test(
  "given_a_state_write_queued_behind_another_when_its_signal_aborts_then_should_reject_and_never_write",
  { timeout: 5_000 },
  async () => {
    const gate = deferred<undefined>()
    let holding = false
    const { laser, sent } = laneLaser(() => {
      if (holding) return Promise.resolve()
      holding = true
      return gate.promise
    })
    const state = Sessions.create(laser).open(ConversationId.new()).asAgent(planner).state()
    const first = state.set("first", 1)
    const controller = new AbortController()
    const second = state.set("second", 2, { signal: controller.signal })
    await until(() => sent.length === 1)
    controller.abort()
    gate.resolve(undefined)
    await assert.rejects(second, CancelledError)
    await first
    await pause(10)
    assert.deepEqual(
      sent.map((envelope) => envelope.operation),
      [OPERATION_STATE_DELTA]
    )
  }
)

void test("given_an_aborted_signal_when_a_memory_call_is_made_then_should_reject_with_cancelled_before_reaching_the_backend", async () => {
  const touched: string[] = []
  const handle = MemoryHandle.custom(recordingBackend(touched))
  const named = MemoryHandle.log(untouchable(touched), "notes")
  const scoped = ContextScope.create(untouchable(touched), ConversationId.new()).memory(handle)
  const topic = MemoryTopicBuilder.create(untouchable(touched), "incidents").stream("memory")
  // Opening a log handle reads the default stream, which is no I/O.
  touched.length = 0
  const payload = new Uint8Array([1])
  const feedback = { target: MemoryId.new(), weight: 1 }
  const options = { signal: AbortSignal.abort() }
  const calls: readonly (() => Promise<unknown>)[] = [
    () => handle.remember({}, payload, options),
    () => handle.remember(payload).send(options),
    () => handle.recall({}, {}, options),
    () => handle.recall().fetch(options),
    () => handle.recall().block(undefined, options),
    () => handle.recallFolded({}, {}, options),
    () => handle.improve({}, feedback, options),
    () => handle.forget({}, MemoryId.new(), options),
    () => handle.context({}, {}, options),
    () => handle.consolidate({}, 1, options),
    () => handle.append({}, MemoryId.new(), MemoryKind.Fact, payload, options),
    () => named.set("plan", payload, options),
    () => named.fetch("plan", options),
    () => named.fetchFolded("plan", options),
    () => named.update("plan", payload, options),
    () => named.remove("plan", options),
    () => scoped.search("login", options),
    () => scoped.block(undefined, options),
    () => scoped.consolidate(1, options),
    () => scoped.forget(MemoryId.new(), options),
    () => scoped.improve(feedback, options),
    () => scoped.remember(payload).send(options),
    () => topic.build(options)
  ]
  for (const call of calls) await assert.rejects(call(), CancelledError)
  assert.deepEqual(touched, [])
})

void test(
  "given_a_memory_recall_waiting_on_its_backend_when_the_signal_aborts_then_should_reject_with_cancelled",
  { timeout: 5_000 },
  async () => {
    let held: ReturnType<typeof setTimeout> | undefined
    const touched: string[] = []
    const backend: Memory = {
      ...recordingBackend(touched),
      recall: () => {
        touched.push("recall")
        return new Promise<readonly MemoryItem[]>(() => {
          held = setTimeout(() => undefined, 60_000)
        })
      }
    }
    const controller = new AbortController()
    const recall = MemoryHandle.custom(backend).recall().fetch({ signal: controller.signal })
    await until(() => touched.length === 1)
    controller.abort()
    try {
      await assert.rejects(recall, CancelledError)
    } finally {
      clearTimeout(held)
    }
  }
)

void test("given_a_consolidation_pruning_items_when_the_signal_aborts_then_should_reject_with_cancelled_and_forget_nothing_more", async () => {
  const controller = new AbortController()
  const forgotten: MemoryId[] = []
  const items = [1n, 2n, 3n].map((id): MemoryItem => ({
    id: MemoryId.fromU128(id),
    payload: new Uint8Array(),
    provenance: { conversationId: ConversationId.new() },
    kind: MemoryKind.Fact,
    signals: []
  }))
  const backend: Memory = {
    ...recordingBackend([]),
    recall: () => Promise.resolve(items),
    forget: (_scope, id) => {
      forgotten.push(id)
      controller.abort()
      return Promise.resolve()
    }
  }
  await assert.rejects(
    MemoryHandle.custom(backend).consolidate({}, 0, { signal: controller.signal }),
    CancelledError
  )
  assert.equal(forgotten.length, 1)
})

void test("given_an_aborted_signal_when_a_context_scope_call_is_made_then_should_reject_with_cancelled_before_any_io", async () => {
  const touched: string[] = []
  const scope = ContextScope.create(untouchable(touched), ConversationId.new())
  const checkpoint = Checkpoint.fromJSON('{"per_topic":{}}')
  const fold = (count: number): number => count + 1
  const options = { signal: AbortSignal.abort() }
  const calls: readonly (() => Promise<unknown>)[] = [
    () => scope.append("notes", new Uint8Array([1]), options),
    () => scope.fetch(["notes"], 5, undefined, options),
    () => scope.fetchWith(["notes"], new LastN(5), options),
    () => scope.block(["notes"], 5, undefined, options),
    () => scope.checkpoint(["notes"], options),
    () => scope.state(["notes"], { kind: "at", checkpoint }, 0, fold, options),
    () =>
      scope.stateWith(
        untouchable(touched) as unknown as SnapshotStore,
        ["notes"],
        0,
        fold,
        undefined,
        options
      )
  ]
  for (const call of calls) await assert.rejects(call(), CancelledError)
  assert.deepEqual(touched, [])
})

void test(
  "given_a_consolidator_when_the_agent_shuts_down_then_should_abort_the_signal_of_its_call_options",
  { timeout: 1_000 },
  async (t) => {
    t.mock.method(
      ReliableConsumer.prototype,
      "run",
      async (_laser: Laser, _handler: AgentHandler, control: ReliableConsumerControl) => {
        control.ready?.()
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
    let received: CallOptions | undefined
    const handle = Agent.builder()
      .id(AgentId.new("consolidating-worker"))
      .listenOn("agent.sessions")
      .handler({ handle: () => Promise.resolve() })
      .consolidateEvery(10)
      .consolidator({
        consolidate: (_scope, options) => {
          received = options
          return new Promise<ConsolidationReport>(() => undefined)
        }
      })
      .build()
      .spawn({} as Laser)
    await handle.ready()
    await handle.shutdown()
    assert.equal(received?.signal?.aborted, true)
  }
)

void test("given_a_session_write_waiting_out_its_retry_delay_when_the_signal_aborts_then_should_reject_and_send_no_retry", async () => {
  const request = new ModelRequest("model", new Uint8Array([1]))
  const opened = (sessions: Sessions) => sessions.open(ConversationId.new()).asAgent(planner)
  const control = (sessions: Sessions) =>
    sessions.control("agents", ConversationId.new()).asOperator(AgentId.new("operator"))
  const writes: readonly (readonly [
    string,
    (sessions: Sessions, signal: AbortSignal) => Promise<unknown>
  ])[] = [
    [
      "Session.append",
      (sessions, signal) => {
        const session = opened(sessions)
        const envelope = commandEnvelope(
          RecordId.fromU128(3n),
          WireConversationId.parse(session.conversation.toString()),
          planner.wireId(),
          CorrelationId.fromU128(4n),
          new Uint8Array([1])
        )
        return session.append(envelope, { signal })
      }
    ],
    ["Session.end", (sessions, signal) => opened(sessions).end({ signal })],
    ["Session.tool", (sessions, signal) => opened(sessions).tool("search", {}, { signal })],
    ["Session.model", (sessions, signal) => opened(sessions).model(request, undefined, { signal })],
    [
      "Session.recordRetrieval",
      (sessions, signal) => opened(sessions).recordRetrieval(undefined, [], { signal })
    ],
    [
      "ModelCall.complete",
      (sessions, signal) =>
        ModelCall.create(opened(sessions), CorrelationId.fromU128(5n), "chat").complete(
          { body: new Uint8Array([1]) },
          { signal }
        )
    ],
    [
      "ModelCall.fail",
      (sessions, signal) =>
        ModelCall.create(opened(sessions), CorrelationId.fromU128(5n), "chat").fail(FAILURE, {
          signal
        })
    ],
    ["SessionState.set", (sessions, signal) => opened(sessions).state().set("key", 1, { signal })],
    [
      "SessionBuilder.begin",
      (sessions, signal) => sessions.start().agent(planner).begin({ signal })
    ],
    ["SessionControl.cancel", (sessions, signal) => control(sessions).cancel({ signal })],
    ["SessionControl.forceCancel", (sessions, signal) => control(sessions).forceCancel({ signal })]
  ]
  const late = await lateWrites(writes, async (client) => {
    const transport = await ApacheIggyTransport.fromClient(client, "owned", {
      timeoutMs: 1_000,
      maxRetries: 3,
      retryBackoffMs: 40
    })
    return { target: Sessions.create(laneOver(transport)), close: () => transport.close() }
  })
  assert.deepEqual(late, [], "no session write starts an attempt after the abort")
})

void test("given_a_memory_write_waiting_out_its_retry_delay_when_the_signal_aborts_then_should_reject_and_send_no_retry", async () => {
  const scope = { conversation: ConversationId.new() }
  const writes: readonly (readonly [
    string,
    (memory: MemoryHandle, signal: AbortSignal) => Promise<unknown>
  ])[] = [
    [
      "MemoryHandle.remember",
      (memory, signal) => memory.remember(scope, new Uint8Array([1]), { signal })
    ],
    [
      "RememberBuilder.send",
      (memory, signal) =>
        memory
          .remember(new Uint8Array([1]))
          .scope(scope.conversation)
          .send({ signal })
    ],
    [
      "MemoryHandle.improve",
      (memory, signal) => memory.improve(scope, { target: MemoryId.new(), weight: 1 }, { signal })
    ],
    ["MemoryHandle.forget", (memory, signal) => memory.forget(scope, MemoryId.new(), { signal })],
    ["MemoryHandle.set", (memory, signal) => memory.set("key", new Uint8Array([1]), { signal })],
    ["MemoryHandle.remove", (memory, signal) => memory.remove("key", { signal })]
  ]
  const late = await lateWrites(writes, async (client) => {
    const root = await Laser.builder()
      .client(client)
      .capabilities(OPEN_CAPABILITIES)
      .publishRetryBackoff(40)
      .publishTimeout(1_000)
      .connect()
    return { target: root.withDefaultStream("agents").memory("notes"), close: () => root.close() }
  })
  assert.deepEqual(late, [], "no memory write starts an attempt after the abort")
})

interface Lane {
  readonly laser: Laser
  readonly sent: AgentEnvelope[]
  readonly touched: string[]
  readonly acquired: () => number
}

// A session lane whose every read and write is recorded. `send` holds or
// releases each write after it is recorded.
function laneLaser(send: () => Promise<unknown> = () => Promise.resolve()): Lane {
  const sent: AgentEnvelope[] = []
  const touched: string[] = []
  let acquired = 0
  const read = <Value>(name: string, value: Value): Promise<Value> => {
    touched.push(name)
    return Promise.resolve(value)
  }
  const transport = {
    async sendMessagesWithHeaders(
      _stream: string,
      _topic: string,
      messages: readonly MessageWithHeaders[]
    ) {
      for (const message of messages) {
        sent.push(
          decodeAgentEnvelope(expectMap(decodeOne(message.payload, "sent"), "sent"), "sent")
        )
      }
      await send()
      return { confirmations: [] }
    }
  } as unknown as LaserTransport
  const laser: Record<PropertyKey, unknown> = {
    defaultStream: "agents",
    [INTERNAL_LAYOUTS]: () => new Map(),
    [INTERNAL_TRANSPORT]: () => ({
      findSnapshotStream: () => read("findSnapshotStream", { id: 0, createdAtMicros: 1n }),
      findSnapshotTopic: () =>
        read("findSnapshotTopic", { id: 0, createdAtMicros: 2n, partitions: 4 }),
      findTopicPartitionCount: () => read("findTopicPartitionCount", undefined)
    }),
    [INTERNAL_SESSION_LEASES]: () => ({
      acquire: (_laser: Laser, key: { readonly session: ConversationId }) => {
        acquired += 1
        return heldLease(key.session).lease
      }
    }),
    capabilities: () => read("capabilities", OPEN_CAPABILITIES),
    agdx: (topic: string, source: AgentId, conversation: ConversationId) =>
      createAgdx(transport, "agents", topic, source, conversation),
    topic: (name: string) => ({
      partitionCount: () => read(`partitionCount ${name}`, undefined),
      tailOffsets: () => read(`tailOffsets ${name}`, new Map<number, bigint>())
    }),
    sessions: () => Sessions.create(laser as unknown as Laser),
    withDefaultStream: () => laser
  }
  return { laser: laser as unknown as Laser, sent, touched, acquired: () => acquired }
}

// `laneLaser` with its session and control records sent through `transport`.
function laneOver(transport: LaserTransport): Laser {
  const { laser } = laneLaser()
  return Object.assign(laser, {
    agdx: (topic: string, source: AgentId, conversation: ConversationId) =>
      createAgdx(transport, "agents", topic, source, conversation)
  })
}

// The names of `writes` that sent another attempt after their signal aborted
// in the retry wait of a first attempt the server refused as transient.
async function lateWrites<Target>(
  writes: readonly (readonly [string, (target: Target, signal: AbortSignal) => Promise<unknown>])[],
  open: (
    client: IggyClient
  ) => Promise<{ readonly target: Target; readonly close: () => Promise<void> }>
): Promise<readonly string[]> {
  const late: string[] = []
  for (const [name, write] of writes) {
    const controller = new AbortController()
    let attempts = 0
    const sent = (): number => attempts
    const { target, close } = await open({
      clientProvider: () => Promise.resolve({ protocol: "vsr" }),
      stream: { get: () => Promise.resolve({ id: 1, name: "agents" }) },
      topic: { get: () => Promise.resolve({ id: 2, name: "agent.memory", partitionsCount: 1 }) },
      message: {
        send: () => {
          attempts += 1
          return Promise.reject(Object.assign(new Error("not accepted"), { errorCode: 58 }))
        }
      },
      destroy: () => Promise.resolve()
    } as unknown as IggyClient)
    try {
      const writing = write(target, controller.signal)
      for (let turn = 0; sent() === 0 && turn < 200; turn += 1) await pause(1)
      assert.equal(sent(), 1, `${name} sends its first attempt`)
      controller.abort()
      await assert.rejects(writing, CancelledError, name)
      await pause(100)
      if (sent() > 1) late.push(name)
    } finally {
      await close()
    }
  }
  return late
}

// A deployment that indexes sessions, recording each capability probe,
// control publish, and managed read in `calls`.
function indexLaser(
  sendManaged: () => Promise<Uint8Array>,
  publishTimeoutMs = 1_000
): { readonly laser: Laser; readonly calls: string[] } {
  const calls: string[] = []
  const laser = {
    defaultStream: "agents",
    bootstrap: () => {
      calls.push("bootstrap")
      return Promise.resolve()
    },
    capabilities: () => {
      calls.push("capabilities")
      return Promise.resolve(CAPABILITIES)
    },
    [INTERNAL_PUBLISH_CONTROL]: () => {
      calls.push("control")
      return Promise.resolve()
    },
    [INTERNAL_TRANSPORT]: () => ({
      publishTimeoutMs: () => publishTimeoutMs,
      findSnapshotStream: () => Promise.resolve({ id: 0, createdAtMicros: 1n }),
      findSnapshotTopic: () => Promise.resolve({ id: 0, createdAtMicros: 2n, partitions: 1 }),
      sendManaged: () => {
        calls.push("read")
        return sendManaged()
      }
    })
  } as unknown as Laser
  return { laser, calls }
}

// A laser whose every member records its name and fails, for calls that must
// reach nothing.
function untouchable(touched: string[]): Laser {
  return new Proxy(
    {},
    {
      get(_target, property) {
        touched.push(String(property))
        return () => Promise.reject(new Error(`unexpected ${String(property)}`))
      }
    }
  ) as unknown as Laser
}

function recordingBackend(touched: string[]): Memory {
  const record = <Value>(name: string, value: Value): Promise<Value> => {
    touched.push(name)
    return Promise.resolve(value)
  }
  return {
    append: () => record("append", MemoryId.new()),
    remember: () => record("remember", MemoryId.new()),
    recall: () => record("recall", []),
    improve: () => record("improve", MemoryId.new()),
    forget: () => record("forget", undefined)
  }
}

function heldLease(session: ConversationId): { lease: SessionLease; released: () => boolean } {
  let released = false
  const registry = {
    release: () => {
      released = true
    }
  } as unknown as LeaseRegistry
  return {
    lease: SessionLease.create(registry, { stream: "agents", streamGeneration: 0n, session }),
    released: () => released
  }
}

function reply(value: SessionReply): Uint8Array {
  return encodeNamed(encodeSessionReply(value))
}

function changeView(totalResults: bigint): SessionChangesView {
  return {
    items: [],
    page: 1,
    totalPages: Number((totalResults + 9n) / 10n),
    totalResults,
    results: 10,
    floor: totalResults + 1n,
    series: 1n
  }
}

function changes(view: SessionChangesView): SessionReply {
  return { kind: "ok", outcome: { kind: "changes", changes: view } }
}

function readsOf(calls: readonly string[]): number {
  return calls.filter((call) => call === "read").length
}

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((complete) => {
    resolve = complete
  })
  return { promise, resolve }
}

async function until(condition: () => boolean): Promise<void> {
  while (!condition()) await pause(1)
}

function pause(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Agent } from "../../src/agent/builder.js"
import type { PendingControl } from "../../src/agent/control.js"
import { decodeAgentMessage } from "../../src/agent/reliable-consumer.js"
import { HandlerError, InvalidError, RejectedError } from "../../src/client/errors.js"
import { isUnsupported } from "../../src/client/error-classify.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { Laser } from "../../src/client/laser.js"
import { Checkpoint, LastN } from "../../src/context.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { ModelRequest } from "../../src/session-ops.js"
import { SessionConfig, TopicRetention, deriveSessionId, type Session } from "../../src/session.js"
import { AgentId, ConversationId } from "../../src/types/ids.js"
import {
  AgentKind,
  type TaskState,
  commandEnvelope,
  decodeSessionEnd,
  decodeSessionStart
} from "../../src/wire/agent.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId
} from "../../src/wire/ids.js"
import { type SessionHeartbeat, decodeSessionHeartbeat } from "../../src/wire/session.js"

function nth<Item>(items: readonly Item[], index: number): Item {
  const item = items.at(index)
  assert.ok(item !== undefined, `expected an item at ${String(index)}`)
  return item
}

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const ONE_DAY = TopicRetention.expireAfter(86_400_000)
const planner = AgentId.new("planner")

async function eventually<Value>(read: () => Promise<Value | undefined>): Promise<Value> {
  const deadline = performance.now() + 5_000
  let value = await read()
  while (value === undefined && performance.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 20))
    value = await read()
  }
  assert.ok(value !== undefined)
  return value
}

async function connect(): Promise<Laser> {
  const laser = await Laser.connectWithStream(CONNECTION_STRING, `laser-ts-test-${randomUUID()}`)
  await laser.bootstrap(4, ONE_DAY)
  return laser
}

function decoded<Value>(
  body: Uint8Array,
  decode: (map: ReturnType<typeof expectMap>, context: string) => Value
): Value {
  return decode(expectMap(decodeOne(body, "body"), "body"), "body")
}

async function lifecycle(
  laser: Laser,
  session: ConversationId
): Promise<readonly (readonly [TaskState, Uint8Array])[]> {
  return eventually(async () => {
    const turns = await laser.sessions().open(session).context()
    const statuses = turns.flatMap((turn) => {
      const envelope = turn.message.envelope
      return envelope?.kind === AgentKind.Status && envelope.taskState !== undefined
        ? [[envelope.taskState, envelope.body] as const]
        : []
    })
    return statuses.length >= 2 ? statuses : undefined
  })
}

function own(session: Session, record: bigint, text: string) {
  return {
    ...commandEnvelope(
      RecordId.fromU128(record),
      WireConversationId.parse(session.conversation.toString()),
      planner.wireId(),
      CorrelationId.fromU128(record + 1n),
      new TextEncoder().encode(text)
    ),
    operation: "summarize"
  }
}

void test("given_a_started_session_when_ended_then_should_write_start_and_completed_on_the_lane", async () => {
  const laser = await connect()
  try {
    const { session, lease } = await laser
      .sessions()
      .create("incident")
      .agent(planner)
      .namespace("ops")
      .tag("vip")
      .begin()
    const stream = laser.defaultStream ?? ""
    assert.ok(session.conversation.equals(deriveSessionId(stream, "ops", "incident")))
    await session.end()
    await session.end()
    await assert.rejects(session.cancel(), InvalidError)
    lease.release()

    const statuses = await lifecycle(laser, session.conversation)
    assert.deepEqual(nth(statuses, 0)[0], { kind: "known", name: "Working" })
    const start = decoded(nth(statuses, 0)[1], decodeSessionStart)
    assert.equal(start.label, "incident")
    assert.equal(start.sdk.language, "typescript")
    assert.deepEqual(start.tags, ["vip"])
    assert.equal(start.idleTimeoutMicros, 300_000_000n)
    assert.deepEqual(nth(statuses, 1)[0], { kind: "known", name: "Completed" })
    const turns = await laser.sessions().open(session.conversation).context()
    // The repeated end is the same record written twice.
    assert.deepEqual(
      turns.map((turn) => turn.display),
      ["session.started", "session.completed", "session.completed"]
    )
    assert.equal(
      turns[1]?.message.envelope?.record?.toString(),
      turns[2]?.message.envelope?.record?.toString()
    )
  } finally {
    await laser.close()
  }
})

void test("given_a_session_run_when_the_work_throws_then_should_write_failed_and_rethrow", async () => {
  const laser = await connect()
  try {
    const { session, lease } = await laser.sessions().start().agent(planner).begin()
    const thrown = new TypeError("model provider exploded")
    await assert.rejects(
      session.run(lease, () => {
        throw thrown
      }),
      (error) => error === thrown
    )
    const statuses = await lifecycle(laser, session.conversation)
    assert.deepEqual(nth(statuses, 1)[0], { kind: "known", name: "Failed" })
    const end = decoded(nth(statuses, 1)[1], decodeSessionEnd)
    assert.equal(end.error?.message, "model provider exploded")
    assert.deepEqual(end.error.detail?.get("panic"), { kind: "bool", value: true })
  } finally {
    await laser.close()
  }
})

void test("given_a_session_run_when_the_work_rejects_then_should_return_the_work_error_and_fail_the_session", async () => {
  const laser = await connect()
  try {
    const { session, lease } = await laser.sessions().start().agent(planner).begin()
    const failure = new HandlerError("tool timed out")
    await assert.rejects(
      session.run(lease, () => Promise.reject(failure)),
      (error) => error === failure
    )
    const statuses = await lifecycle(laser, session.conversation)
    assert.deepEqual(nth(statuses, 1)[0], { kind: "known", name: "Failed" })
  } finally {
    await laser.close()
  }
})

void test("given_a_held_lease_when_the_interval_passes_then_should_list_the_session_in_a_heartbeat", async () => {
  const laser = await connect()
  try {
    const { session, lease } = await laser
      .sessions(new SessionConfig().heartbeat(50))
      .start()
      .agent(planner)
      .begin()
    const heartbeat = await eventually(async (): Promise<SessionHeartbeat | undefined> => {
      const cursor = (await laser.topic(AgentTopic.Heartbeats).replay()).batch(100)
      for (const record of await cursor.pollRecords()) {
        const message = decodeAgentMessage(record)
        if (message.kind !== "message" || message.message.envelope === undefined) continue
        try {
          return decoded(message.message.envelope.body, decodeSessionHeartbeat)
        } catch {
          continue
        }
      }
      return undefined
    })
    assert.ok(heartbeat.sessions.some((id) => id.toString() === session.conversation.toString()))
    assert.equal(heartbeat.stream, laser.defaultStream)
    lease.release()
  } finally {
    await laser.close()
  }
})

void test("given_an_envelope_of_another_session_when_appended_then_should_be_refused", async () => {
  const laser = await connect()
  try {
    const session = laser.sessions().open(ConversationId.new())
    const foreign = {
      ...own(session, 1n, "{}"),
      conversation: WireConversationId.parse(ConversationId.new().toString())
    }
    await assert.rejects(session.append(foreign), InvalidError)
    const receipt = await session.append(own(session, 3n, "{}"))
    assert.equal(receipt.record?.toString(), RecordId.fromU128(3n).toString())
  } finally {
    await laser.close()
  }
})

void test("given_a_checkpoint_when_more_records_are_appended_then_turns_at_and_since_should_split_history_there", async () => {
  const laser = await connect()
  try {
    const session = laser.sessions(new SessionConfig().contextTurns(10)).open(ConversationId.new())
    await session.append(own(session, 10n, "first"))
    await session.append(own(session, 20n, "second"))
    const checkpoint = await eventually(async () => {
      const checkpoint = await session.checkpoint()
      const seen = await session.stateAt(checkpoint, 0, (count) => count + 1)
      return seen === 2 ? checkpoint : undefined
    })
    await session.append(own(session, 30n, "third"))
    const since = await eventually(async () => {
      const turns = await session.turnsSince(checkpoint)
      return turns.length === 1 ? turns : undefined
    })
    const body = (turn: (typeof since)[number]): string =>
      new TextDecoder().decode(turn.message.envelope?.body)
    const [third] = since
    assert.ok(third !== undefined)
    assert.equal(body(third), "third")
    assert.equal(third.display, "agent.message")
    const before = await session.turnsAt(Checkpoint.fromJSON(JSON.stringify(checkpoint)))
    assert.deepEqual(before.map(body), ["first", "second"])
    const replayed = await session.replay(checkpoint, [] as string[], (acc, turn) => [
      ...acc,
      body(turn)
    ])
    assert.deepEqual(replayed, ["third"])
  } finally {
    await laser.close()
  }
})

void test("given_model_and_tool_calls_when_recorded_then_should_write_redacted_requests_a_manifest_and_results", async () => {
  const laser = await connect()
  try {
    const { session, lease } = await laser.sessions().start().agent(planner).begin()
    const assembled = await eventually(async () => {
      const assembled = await session.assemble(new LastN(10))
      return assembled.manifest.fragments.length === 1 ? assembled : undefined
    })
    assert.equal(assembled.manifest.policy, "last_n(10)")
    const call = await session.model(
      new ModelRequest("gpt-test", new TextEncoder().encode('{"prompt":"hi","api_key":"k"}')),
      assembled
    )
    await call.complete({
      body: new TextEncoder().encode("hello"),
      model: "gpt-test-1",
      finishReason: "stop",
      usage: { inputTokens: 3n, outputTokens: 1n, costMicros: 20n }
    })
    const tool = await session.tool("lookup", { id: 7, token: "secret" })
    await tool.complete(new TextEncoder().encode("found"))
    lease.release()
    const turns = await eventually(async () => {
      const turns = await laser.sessions().open(session.conversation).contextWith(new LastN(20))
      return turns.length >= 6 ? turns : undefined
    })
    assert.deepEqual(
      turns.map((turn) => turn.display),
      [
        "session.started",
        "model.request",
        "context.assembled",
        "model.response",
        "tool.call",
        "tool.result"
      ]
    )
    const body = (index: number): string =>
      new TextDecoder().decode(nth(turns, index).message.envelope?.body)
    assert.ok(!body(1).includes('"k"'))
    assert.ok(body(4).includes("[redacted]"))
    const request = nth(turns, 1).message.envelope
    const response = nth(turns, 3).message.envelope
    assert.equal(response?.correlation?.toString(), request?.correlation?.toString())
    assert.equal(response?.usage?.costMicros, 20n)
    assert.equal(response.finishReason, "stop")
  } finally {
    await laser.close()
  }
})

void test("given_state_writes_when_folded_then_should_rebuild_the_document_and_snapshot_on_end", async () => {
  const laser = await connect()
  try {
    const { session, lease } = await laser.sessions().start().agent(planner).begin()
    const state = session.state()
    await state.set("tasks", ["triage"])
    await state.set("step", 2)
    const view = await eventually(async () => {
      const view = await state.get()
      return view.revision === 2n ? view : undefined
    })
    assert.equal(view.complete, true)
    assert.deepEqual(view.document, { tasks: ["triage"], step: 2 })
    await session.end()
    lease.release()
    const turns = await eventually(async () => {
      const turns = await laser.sessions().open(session.conversation).context()
      return turns.length >= 5 ? turns : undefined
    })
    assert.equal(nth(turns, 3).display, "state.updated")
    assert.equal(nth(turns, 4).display, "session.completed")
  } finally {
    await laser.close()
  }
})

void test("given_a_submitted_session_when_its_agent_picks_it_up_then_should_move_from_submitted_to_working", async () => {
  const laser = await connect()
  try {
    await using worker = Agent.builder()
      .id(AgentId.new("worker"))
      .listenOn(AgentTopic.Sessions)
      .pollInterval(5)
      .handler({ handle: (_message, context) => context.session().end() })
      .build()
      .spawn(laser)
    await worker.ready()
    const submitted = await laser
      .sessions()
      .submit(AgentId.new("worker"), new TextEncoder().encode("{}"))
      .from(AgentId.new("client"))
      .label("ticket-7")
      .send()
    assert.ok(submitted.session.equals(deriveSessionId(laser.defaultStream ?? "", "", "ticket-7")))
    const turns = await eventually(async () => {
      const turns = await laser.sessions().open(submitted.session).context()
      return turns.some((turn) => turn.display === "session.completed") ? turns : undefined
    })
    const displays = turns.map((turn) => turn.display)
    assert.equal(displays[0], "session.submitted")
    assert.ok(displays.includes("session.resumed"))
  } finally {
    await laser.close()
  }
})

void test("given_operator_control_when_sent_then_should_ride_the_control_topic", async () => {
  const laser = await connect()
  try {
    await laser.topic(AgentTopic.Control).ensure(4)
    const session = ConversationId.new()
    const control = laser
      .sessions()
      .control(laser.defaultStream ?? "", session)
      .asOperator(AgentId.new("operator"))
    await control.cancel()
    await control.forceCancel()
    const records = await eventually(async () => {
      const records = await laser.context(session).fetchWith([AgentTopic.Control], new LastN(10))
      return records.length === 2 ? records : undefined
    })
    assert.equal(nth(records, 0).envelope?.operation, "session_cancel")
    assert.deepEqual(nth(records, 1).envelope?.taskState, { kind: "known", name: "Canceled" })
  } finally {
    await laser.close()
  }
})

void test("given_a_declared_partition_layout_when_sending_then_should_route_commands_to_the_addressee_and_replies_to_the_requester", async () => {
  const laser = await Laser.connectWithStream(CONNECTION_STRING, `laser-ts-test-${randomUUID()}`)
  try {
    const worker = AgentId.new("worker")
    const sessions = laser.sessions(
      new SessionConfig().layout({
        kind: "perAgentPartition",
        partitions: new Map([
          [planner.asStr(), 0],
          [worker.asStr(), 2]
        ])
      })
    )
    await sessions.bootstrap(3, TopicRetention.expireAfter(3_600_000))
    const session = ConversationId.new()
    const correlation = CorrelationId.fromU128(9n)
    const command = await laser
      .agdx(AgentTopic.Sessions, planner, session)
      .command(correlation, new TextEncoder().encode("{}"))
      .withTarget(worker)
      .sendReceipt()
    const reply = await laser
      .agdx(AgentTopic.Sessions, worker, session)
      .respond(correlation, new TextEncoder().encode("{}"))
      .withTarget(planner)
      .sendReceipt()
    assert.equal(command.partitionId, 2)
    assert.equal(reply.partitionId, 0)
  } finally {
    await laser.close()
  }
})

void test("given_a_retrieval_and_a_compaction_when_recorded_then_should_show_on_the_lane_with_source_ids", async () => {
  const laser = await connect()
  try {
    const { session, lease } = await laser.sessions().start().agent(planner).begin()
    await session
      .memory()
      .remember(new TextEncoder().encode("the deploy key rotates on fridays"))
      .send()
    const items = await eventually(async () => {
      const items = await session.memory().recall().keyword("deploy").folded().fetch()
      return items.length > 0 ? items : undefined
    })
    assert.equal(items.length, 1)
    await session.recordRetrieval("deploy", items)
    await session.recordCompaction({
      summaryAt: {
        kind: "message",
        stream: 1,
        topic: 1,
        partition: 0,
        offset: 0n,
        conversation: session.conversation.toString()
      },
      covered: [[1, 0, 0n, 3n]],
      summarizer: { name: "summarizer", version: "1" }
    })
    lease.release()
    const turns = await eventually(async () => {
      const turns = await laser.sessions().open(session.conversation).contextWith(new LastN(20))
      return turns.some((turn) => turn.display === "context.compacted") ? turns : undefined
    })
    assert.ok(turns.some((turn) => turn.display === "context.retrieved"))
    const first = nth(turns, 0).message
    assert.ok(first.timestampMicros > 0n)
    assert.ok(turns.every((turn) => turn.message.topicId === first.topicId))
  } finally {
    await laser.close()
  }
})

void test("given_open_iggy_when_reading_the_session_index_then_should_be_unsupported", async (context) => {
  const laser = await connect()
  try {
    if ((await laser.capabilities()).sessions) {
      context.skip("this deployment includes a managed session index")
      return
    }
    const sessions = laser.sessions()
    const id = ConversationId.new()
    await assert.rejects(sessions.get(id), (error: unknown) => isUnsupported(error))
    await assert.rejects(sessions.list().fetch(), (error: unknown) => isUnsupported(error))
    await assert.rejects(sessions.changes(0n, 0), (error: unknown) => isUnsupported(error))
  } finally {
    await laser.close()
  }
})

void test("given_a_message_reference_when_read_at_then_should_return_that_record_while_the_topic_lives", async () => {
  const laser = await connect()
  try {
    const { session, lease } = await laser.sessions().start().agent(planner).begin()
    lease.release()
    const turns = await eventually(async () => {
      const turns = await session.context()
      return turns.length > 0 ? turns : undefined
    })
    const first = nth(turns, 0).message
    const topic = await laser[INTERNAL_TRANSPORT]().findSnapshotTopic?.(
      laser.defaultStream ?? "",
      AgentTopic.Sessions
    )
    assert.ok(topic !== undefined)
    const at = {
      kind: "message" as const,
      stream: first.streamId,
      topic: first.topicId,
      partition: first.id.partitionId,
      offset: first.id.offset,
      generation: topic.createdAtMicros
    }
    const record = await laser.readAt(at)
    assert.ok(record !== undefined)
    assert.deepEqual(record.id, first.id)
    assert.equal(record.envelope?.record?.toString(), first.envelope?.record?.toString())
    assert.equal(await laser.readAt({ ...at, generation: 1n }), undefined)
    await assert.rejects(laser.readAt({ kind: "memory", id: "m-1" }), InvalidError)
  } finally {
    await laser.close()
  }
})

void test("given_fail_on_dead_letter_when_a_record_of_a_session_dead_letters_then_should_fail_that_session", async () => {
  const laser = await connect()
  try {
    await using worker = Agent.builder()
      .id(AgentId.new("strict"))
      .listenOn(AgentTopic.Sessions)
      .sessions(new SessionConfig().failOnDeadLetter(true))
      .handler({ handle: () => Promise.reject(new RejectedError("permanent failure")) })
      .build()
      .spawn(laser)
    await worker.ready()
    const session = ConversationId.new()
    await laser
      .agdx(AgentTopic.Sessions, planner, session)
      .command(CorrelationId.fromU128(11n), new TextEncoder().encode("{}"))
      .withTarget(AgentId.new("strict"))
      .send()
    const turns = await eventually(async () => {
      const turns = await laser.sessions().open(session).context()
      return turns.some((turn) => turn.display === "session.failed") ? turns : undefined
    })
    const failed = turns.find((turn) => turn.display === "session.failed")?.message.envelope
    assert.ok(failed !== undefined)
    assert.equal(failed.source, "strict")
    const end = decoded(failed.body, decodeSessionEnd)
    assert.ok(end.error?.message?.includes("dead-lettered"))
  } finally {
    await laser.close()
  }
})

void test("given_operator_control_when_a_handler_reads_its_session_then_should_see_earlier_and_live_requests", async () => {
  const laser = await connect()
  try {
    await laser.topic(AgentTopic.Control).ensure(4)
    const stream = laser.defaultStream ?? ""
    const operator = AgentId.new("operator")
    const watcher = AgentId.new("watcher")
    // A request sent before the worker exists is folded from the log.
    const canceled = ConversationId.new()
    await laser.sessions().control(stream, canceled).asOperator(operator).cancel()

    const observed: (readonly [string, PendingControl])[] = []
    await using worker = Agent.builder()
      .id(watcher)
      .listenOn(AgentTopic.Sessions)
      .concurrency({ kind: "serial-per-partition", maxPartitions: 4 })
      .handler({
        async handle(message, context): Promise<void> {
          const conversation = message.provenance.conversationId.toString()
          const session = context.session()
          const initial = await session.pendingControl()
          observed.push([conversation, initial])
          if (initial.pauseRequested || initial.cancelRequested) return
          // Control never interrupts the handler: it reads the requests and
          // decides itself.
          const deadline = Date.now() + 10_000
          while (Date.now() < deadline) {
            const flags = await session.pendingControl()
            if (flags.pauseRequested !== initial.pauseRequested) {
              observed.push([conversation, flags])
              return
            }
            await new Promise((resolve) => setTimeout(resolve, 50))
          }
        }
      })
      .build()
      .spawn(laser)
    await worker.ready()
    let correlation = 20n
    const send = async (session: ConversationId): Promise<void> => {
      correlation += 1n
      await laser
        .agdx(AgentTopic.Sessions, planner, session)
        .command(CorrelationId.fromU128(correlation), new TextEncoder().encode("{}"))
        .withTarget(watcher)
        .send()
    }
    const next = (count: number): Promise<readonly [string, PendingControl]> =>
      eventually(() => Promise.resolve(observed.length >= count ? observed[count - 1] : undefined))

    await send(canceled)
    assert.deepEqual(await next(1), [
      canceled.toString(),
      { pauseRequested: false, cancelRequested: true }
    ])

    // A request sent while the handler runs reaches it through the control
    // subscription.
    const paused = ConversationId.new()
    await send(paused)
    assert.deepEqual(await next(2), [
      paused.toString(),
      { pauseRequested: false, cancelRequested: false }
    ])
    await laser.sessions().control(stream, paused).asOperator(operator).pause()
    assert.deepEqual(await next(3), [
      paused.toString(),
      { pauseRequested: true, cancelRequested: false }
    ])
  } finally {
    await laser.close()
  }
})

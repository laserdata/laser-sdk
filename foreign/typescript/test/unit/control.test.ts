import assert from "node:assert/strict"
import { test } from "node:test"
import {
  type ControlAction,
  ControlBook,
  type ControlRequest,
  NO_CONTROL,
  controlPartition,
  controlRequest,
  foldControl,
  pausedBy,
  xxh32
} from "../../src/agent/control.js"
import { SessionFacts, isWork, parkedOf, readParked } from "../../src/agent/lane-scan.js"
import { ControlFollower, RoleState } from "../../src/agent/pause.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import type { Laser } from "../../src/client/laser.js"
import { CONTEXT_READ_WINDOW } from "../../src/context.js"
import type { LaserTransport, PollWait, PolledMessage } from "../../src/iggy/apache-iggy.js"
import { ConversationId } from "../../src/types/ids.js"
import {
  type AgentEnvelope,
  OPERATION_CHAT,
  type SessionParking,
  commandEnvelope,
  parseAgentId
} from "../../src/wire/agent.js"
import {
  OPERATION_EXECUTE_TOOL,
  OPERATION_SESSION_CANCEL,
  OPERATION_SESSION_PAUSE,
  OPERATION_SESSION_RESUME
} from "../../src/wire/dispatch.js"
import {
  ConversationId as WireConversationId,
  CorrelationId,
  RecordId
} from "../../src/wire/ids.js"

function command(operation: string, target?: string, body = "{}"): AgentEnvelope {
  const envelope = commandEnvelope(
    RecordId.fromU128(1n),
    WireConversationId.fromU128(2n),
    parseAgentId("operator"),
    CorrelationId.fromU128(3n),
    new TextEncoder().encode(body)
  )
  return {
    ...envelope,
    operation,
    ...(target !== undefined ? { target: parseAgentId(target) } : {})
  }
}

function request(action: ControlAction): ControlRequest {
  return { action, named: false }
}

void test("given_control_commands_when_classified_then_should_map_each_request_and_skip_other_agents", () => {
  const me = parseAgentId("worker")
  const action = (envelope: AgentEnvelope, agent = me) => controlRequest(envelope, agent)?.action
  assert.equal(action(command(OPERATION_SESSION_PAUSE, "worker")), "pause")
  assert.equal(action(command(OPERATION_SESSION_RESUME)), "resume")
  assert.equal(controlRequest(command(OPERATION_SESSION_CANCEL), undefined)?.action, "cancel")
  assert.equal(action(command(OPERATION_SESSION_CANCEL, "critic")), undefined)
  assert.equal(action(command("chat")), undefined)
})

void test("given_a_pause_with_participants_when_classified_then_should_name_only_listed_agents", () => {
  const me = parseAgentId("worker")
  const pause = command(OPERATION_SESSION_PAUSE, undefined, '{"participants":["worker"]}')
  const named = controlRequest(pause, me)
  assert.equal(named?.named, true)
  assert.equal(named.record?.toString(), RecordId.fromU128(1n).toString())
  assert.equal(controlRequest(pause, parseAgentId("critic"))?.named, false)
  assert.equal(controlRequest(command(OPERATION_SESSION_PAUSE, "worker"), me)?.named, true)
  assert.equal(controlRequest(command(OPERATION_SESSION_PAUSE), me)?.named, false)
})

void test("given_pause_resume_and_cancel_when_folded_then_should_keep_only_the_latest_pause_state", () => {
  const [state, last] = foldControl(
    [
      [[0, 1n], request("pause")],
      [[0, 2n], request("resume")],
      [[0, 3n], request("cancel")]
    ],
    undefined
  )
  assert.deepEqual(state.flags, {
    pauseRequested: false,
    cancelRequested: true,
    controlConflict: false
  })
  assert.equal(pausedBy(state), undefined)
  assert.deepEqual(state.resume?.at, [0, 2n])
  assert.deepEqual(last, [0, 3n])
})

void test("given_live_requests_before_the_fold_when_installed_then_should_replay_only_newer_ones", () => {
  const book = new ControlBook()
  book.setLive(true)
  const session = ConversationId.new().toString()
  book.observe(session, [0, 4n], request("pause"))
  book.observe(session, [0, 6n], request("cancel"))
  assert.equal(book.state(session), undefined, "unfolded until the first read")
  const [state, last] = foldControl(
    [
      [[0, 4n], request("pause")],
      [[0, 5n], request("resume")]
    ],
    undefined
  )
  assert.deepEqual(book.install(session, state, last).flags, {
    pauseRequested: false,
    cancelRequested: true,
    controlConflict: false
  })
  book.observe(session, [0, 7n], request("pause"))
  const current = book.state(session)
  assert.ok(current !== undefined)
  assert.deepEqual(current.flags, {
    pauseRequested: true,
    cancelRequested: true,
    controlConflict: false
  })
  assert.deepEqual(pausedBy(current)?.at, [0, 7n])
  book.setLive(false)
  assert.equal(book.state(session), undefined, "a stopped follower forces a fold")
  assert.deepEqual(
    book.install(session, NO_CONTROL, undefined),
    NO_CONTROL,
    "a stopped follower trusts the fold"
  )
})

void test("given_a_loaded_book_when_reading_an_unseen_session_then_should_report_no_requests", () => {
  const book = new ControlBook()
  const paused = ConversationId.new().toString()
  book.load([[paused, [1, 3n], request("pause")]])
  book.setLive(true)
  assert.ok(book.hasPauseHistory())
  assert.deepEqual(book.pausedOrResumed(), [paused])
  assert.deepEqual(book.state(ConversationId.new().toString()), NO_CONTROL)
  const later = ConversationId.new().toString()
  book.observe(later, [2, 9n], request("cancel"))
  assert.deepEqual(book.state(later)?.flags, {
    pauseRequested: false,
    cancelRequested: true,
    controlConflict: false
  })
})

function parking(offset: bigint, request: bigint): SessionParking {
  return {
    source: { kind: "message", stream: 1, topic: 2, partition: 0, offset, generation: 9n },
    role: parseAgentId("worker"),
    request: { streamId: 1, topicId: 3, partitionId: 0, offset: request }
  }
}

void test("given_parkings_and_completions_when_folded_then_should_keep_only_unhandled_records_by_source", () => {
  const facts = new SessionFacts()
  for (const [offset, at] of [
    [7n, 1n],
    [4n, 1n],
    [7n, 2n]
  ] as const) {
    const parked = parkedOf(parking(offset, at))
    assert.ok(parked !== undefined)
    facts.parked.set(parked.key, parked)
  }
  const handled = parkedOf(parking(4n, 1n))
  assert.ok(handled !== undefined)
  facts.handled.add(handled.key)
  assert.deepEqual(
    facts.pending().map((parked) => [parked.address.offset, parked.parking.request.offset]),
    [
      [7n, 1n],
      [7n, 2n]
    ]
  )
})

void test("given_a_lane_whose_head_expired_when_read_whole_then_should_report_the_parked_read_complete", async () => {
  const lane = (first: number, length: number): Laser => {
    const records: PolledMessage[] = Array.from({ length }, (_, index) => ({
      partitionId: 0,
      offset: BigInt(first + index),
      payload: new Uint8Array([0xff]),
      headers: new Map()
    }))
    const transport = {
      findTopicPartitionCount: () => Promise.resolve(1),
      pollMessages: (
        _stream: string,
        _topic: string,
        _target: unknown,
        strategy: { readonly kind: string; readonly value?: bigint },
        count: number
      ) => {
        if (strategy.kind === "first") return Promise.resolve(records.slice(0, 1))
        if (strategy.kind === "last") return Promise.resolve(records.slice(-1))
        const from = records.findIndex((record) => record.offset >= (strategy.value ?? 0n))
        return Promise.resolve(from < 0 ? [] : records.slice(from, from + count))
      }
    }
    return { defaultStream: "agents", [INTERNAL_TRANSPORT]: () => transport } as unknown as Laser
  }
  const session = ConversationId.new().toString()

  assert.equal((await readParked(lane(100, 10), session)).complete, true)
  assert.equal((await readParked(lane(0, CONTEXT_READ_WINDOW + 10), session)).complete, false)
})

void test("given_lane_operations_when_classified_then_should_count_only_work_commands", () => {
  assert.ok(isWork("invoke_agent"))
  assert.ok(isWork(""))
  assert.ok(!isWork(OPERATION_CHAT))
  assert.ok(!isWork(OPERATION_EXECUTE_TOOL))
  assert.ok(!isWork(OPERATION_SESSION_PAUSE))
})

void test("given_acknowledgments_when_recorded_then_should_keep_the_latest_on_the_lane", () => {
  const role = new RoleState()
  const ack = (state: "Paused" | "Working", offset: bigint) => ({
    state,
    request: [0, offset] as const,
    laneOffset: offset
  })
  const latest = (): string | undefined => role.ack?.state
  role.recordAck(ack("Working", 9n))
  role.recordAck(ack("Paused", 4n))
  assert.equal(latest(), "Working")
  role.recordAck(ack("Paused", 12n))
  assert.equal(latest(), "Paused")
})

void test("given_known_inputs_when_hashed_then_should_match_the_xxh32_reference", () => {
  const encode = (text: string): Uint8Array => new TextEncoder().encode(text)
  assert.equal(xxh32(encode("")), 0x02cc5d05)
  assert.equal(xxh32(encode("abc")), 0x32d153ff)
  assert.equal(xxh32(encode("Nobody inspects the spammish repetition")), 0xe2293b2f)
})

void test("given_a_request_off_the_control_route_when_recorded_then_should_flag_and_never_apply", () => {
  const session = "01KWM3K3XEP3NP5TN850J17YBP"
  const route = controlPartition(session, 2)
  assert.ok(route !== undefined)
  const other = 1 - route
  const book = new ControlBook()
  book.setPartitions(2)
  book.load([
    [session, [route, 1n], request("pause")],
    [session, [other, 2n], request("cancel")]
  ])
  book.setLive(true)
  book.observe(session, [other, 3n], request("resume"))
  const state = book.state(session)
  assert.ok(state !== undefined)
  assert.deepEqual(state.flags, {
    pauseRequested: true,
    cancelRequested: false,
    controlConflict: true
  })
  const [folded] = foldControl(
    [
      [[other, 5n], request("cancel")],
      [[route, 6n], request("pause")]
    ],
    route
  )
  assert.deepEqual(folded.flags, state.flags, "the log fold agrees with the book")
  book.reset()
  assert.equal(book.state(session), undefined, "a reset book folds again from the log")
})

void test("given_an_idle_control_topic_when_followed_then_should_hold_one_read_per_partition_and_stop_at_once", async () => {
  const waits: number[] = []
  let cancelled = 0
  const transport = {
    pollMessages(
      _stream: string,
      _topic: string,
      _target: unknown,
      _start: unknown,
      _count: number,
      _commit: boolean,
      wait?: PollWait,
      signal?: AbortSignal
    ): Promise<readonly PolledMessage[]> {
      waits.push(wait?.maxWaitMs ?? 0)
      const held = wait?.maxWaitMs ?? 0
      return new Promise((resolve, reject) => {
        const timer = setTimeout(
          () => {
            resolve([])
          },
          held === 0 ? 1 : held
        )
        signal?.addEventListener("abort", () => {
          clearTimeout(timer)
          if (held > 0) cancelled += 1
          reject(new Error("cancelled"))
        })
      })
    }
  } as unknown as LaserTransport
  const follower = new ControlFollower(
    transport,
    "orbit",
    {
      streamId: 1,
      topicId: 2,
      next: new Map([
        [0, 0n],
        [1, 0n]
      ]),
      truncated: false
    },
    undefined,
    new ControlBook(),
    undefined,
    4_000
  )
  await new Promise((resolve) => setTimeout(resolve, 30))
  const started = performance.now()
  await follower.stop()
  assert.ok(performance.now() - started < 1_000, "stopping cancels the held read")
  assert.deepEqual(
    waits.slice(0, 4),
    [0, 0, 4_000, 4_000],
    "an idle pass holds one read per partition"
  )
  assert.equal(cancelled, waits.filter((wait) => wait > 0).length, "no held read outlives the stop")
})

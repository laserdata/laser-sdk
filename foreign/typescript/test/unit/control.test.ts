import assert from "node:assert/strict"
import { test } from "node:test"
import {
  type ControlAction,
  ControlBook,
  type ControlRequest,
  NO_CONTROL,
  controlRequest,
  foldControl,
  pausedBy
} from "../../src/agent/control.js"
import { SessionFacts, isWork, parkedOf } from "../../src/agent/lane-scan.js"
import { RoleState } from "../../src/agent/pause.js"
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
  const [state, last] = foldControl([
    [[0, 1n], request("pause")],
    [[0, 2n], request("resume")],
    [[0, 3n], request("cancel")]
  ])
  assert.deepEqual(state.flags, { pauseRequested: false, cancelRequested: true })
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
  const [state, last] = foldControl([
    [[0, 4n], request("pause")],
    [[0, 5n], request("resume")]
  ])
  assert.deepEqual(book.install(session, state, last).flags, {
    pauseRequested: false,
    cancelRequested: true
  })
  book.observe(session, [0, 7n], request("pause"))
  const current = book.state(session)
  assert.ok(current !== undefined)
  assert.deepEqual(current.flags, { pauseRequested: true, cancelRequested: true })
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
  assert.deepEqual(book.state(later)?.flags, { pauseRequested: false, cancelRequested: true })
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

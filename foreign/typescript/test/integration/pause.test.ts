import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import { Agent, type AgentHandle } from "../../src/agent/builder.js"
import type { ParkedRecords } from "../../src/agent/lane-scan.js"
import type { AgentMessage, ConcurrencyPolicy } from "../../src/agent/reliable-consumer.js"
import { INTERNAL_TRANSPORT } from "../../src/client/internals.js"
import { InvalidError } from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { LastN } from "../../src/context.js"
import {
  ActionDecision,
  ActionKind,
  type ActionGovernor,
  type GovernedAction,
  GovernorMode
} from "../../src/govern.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import { type SessionControl, TopicRetention } from "../../src/session.js"
import { AgentId, ConversationId, MintUlid } from "../../src/types/ids.js"
import {
  type AgentEnvelope,
  AgentKind,
  OPERATION_SESSION,
  OPERATION_SESSION_PARKED,
  OPERATION_SESSION_UNPARKED,
  type SessionParking,
  decodeSessionParking,
  decodeSessionPauseRequestJson,
  decodeSessionTransition,
  encodeSessionParking
} from "../../src/wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import { ContentType } from "../../src/wire/content.js"
import { OPERATION_SESSION_PAUSE } from "../../src/wire/dispatch.js"
import { CorrelationId } from "../../src/wire/ids.js"
import { AGENT_CONTROL, AGENT_SESSIONS } from "../../src/wire/topics.js"

const CONNECTION_STRING = process.env["LASER_CONNECTION_STRING"] ?? "iggy:iggy@127.0.0.1:8090"
const WORKER = AgentId.new("pauser")
const CLIENT = AgentId.new("client")
const OPERATOR = AgentId.new("operator")
const ACKNOWLEDGMENT = "acknowledgment"
const SERIAL: ConcurrencyPolicy = { kind: "serial" }

type Position = readonly [number, bigint]
type State = "Paused" | "Working"

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function eventually<Value>(read: () => Promise<Value | undefined>): Promise<Value> {
  const deadline = Date.now() + 15_000
  for (;;) {
    const value = await read()
    if (value !== undefined) return value
    if (Date.now() > deadline) assert.fail("the condition did not hold in time")
    await delay(50)
  }
}

// Every work body the handler saw, in order, and an optional hang: the first
// time the handler sees `hangOn` it records the effect and never returns, a
// process that dies between the effect and its completion.
class Recorder {
  readonly bodies: string[] = []
  reached = false

  constructor(private hangOn?: string) {}

  count(body: string): number {
    return this.bodies.filter((seen) => seen === body).length
  }

  handle(message: AgentMessage): Promise<void> {
    const body = new TextDecoder().decode(message.envelope?.body ?? message.payload)
    this.bodies.push(body)
    if (this.hangOn === body) {
      this.hangOn = undefined
      this.reached = true
      return new Promise<void>(() => undefined)
    }
    return Promise.resolve()
  }
}

// Fails the publish of the armed record shapes: a parking, a completion, or a
// pause or resume acknowledgment. The governor runs before every AGDX
// publish, so an armed shape never reaches the log.
class Faults implements ActionGovernor {
  private readonly armed = new Set<string>()
  readonly refused: string[] = []

  arm(shape: string): void {
    this.armed.add(shape)
  }

  disarm(): void {
    this.armed.clear()
  }

  decide(action: GovernedAction): Promise<ActionDecision> {
    const shape = shapeOf(action)
    if (shape !== undefined && this.armed.has(shape)) {
      this.refused.push(shape)
      return Promise.reject(new InvalidError("injected publish fault"))
    }
    return Promise.resolve(ActionDecision.allow())
  }
}

function shapeOf(action: GovernedAction): string | undefined {
  if (action.kind === ActionKind.Event && action.operation === OPERATION_SESSION_PARKED) {
    return OPERATION_SESSION_PARKED
  }
  if (action.kind === ActionKind.Event && action.operation === OPERATION_SESSION_UNPARKED) {
    return OPERATION_SESSION_UNPARKED
  }
  if (action.kind !== ActionKind.Status || action.operation !== OPERATION_SESSION) return undefined
  try {
    const transition = decodeSessionTransition(
      expectMap(decodeOne(action.payload, "transition"), "transition"),
      "transition"
    )
    return transition.acknowledges === undefined ? undefined : ACKNOWLEDGMENT
  } catch {
    return undefined
  }
}

// A stream with the session topics and the control topic, read through a
// client the test brought, so the runtime reads its group natively.
async function setup(): Promise<{ readonly laser: Laser; readonly native: Laser }> {
  const stream = `laser-ts-pause-${randomUUID()}`
  const laser = await Laser.connectWithStream(CONNECTION_STRING, stream)
  await laser.bootstrap(4, TopicRetention.expireAfter(86_400_000))
  await laser.topic(AgentTopic.Control).ensure(4)
  const native = (await Laser.fromClient(laser.client)).withDefaultStream(stream)
  return { laser, native }
}

async function spawn(
  laser: Laser,
  recorder: Recorder,
  concurrency: ConcurrencyPolicy = SERIAL
): Promise<AgentHandle> {
  const worker = Agent.builder()
    .id(WORKER)
    .listenOn(AgentTopic.Sessions)
    .concurrency(concurrency)
    .handler(recorder)
    .build()
    .spawn(laser)
  await worker.ready()
  return worker
}

async function sendWork(laser: Laser, session: ConversationId, body: string): Promise<Position> {
  const receipt = await laser
    .agdx(AgentTopic.Sessions, CLIENT, session)
    .command(MintUlid.mint(CorrelationId), new TextEncoder().encode(body))
    .withTarget(WORKER)
    .sendReceipt()
  return position(receipt)
}

function position(receipt: { readonly partitionId?: number; readonly offset?: bigint }): Position {
  assert.ok(receipt.partitionId !== undefined && receipt.offset !== undefined)
  return [receipt.partitionId, receipt.offset]
}

function control(laser: Laser, session: ConversationId): SessionControl {
  return laser
    .sessions()
    .control(laser.defaultStream ?? "", session)
    .asOperator(OPERATOR)
    .participants([WORKER])
}

async function lane(laser: Laser, session: ConversationId): Promise<readonly AgentEnvelope[]> {
  const records = await laser
    .context(session)
    .fetchWith([AgentTopic.Sessions], new LastN(Number.MAX_SAFE_INTEGER))
  return records.flatMap((record) => (record.envelope === undefined ? [] : [record.envelope]))
}

// The worker's acknowledgments on the lane: the state and the control
// position each one answers.
async function acknowledgments(
  laser: Laser,
  session: ConversationId
): Promise<readonly (readonly [State, Position])[]> {
  return (await lane(laser, session)).flatMap((envelope) => {
    if (envelope.kind !== AgentKind.Status || envelope.operation !== OPERATION_SESSION) return []
    const state = envelope.taskState
    if (state?.kind !== "known" || (state.name !== "Paused" && state.name !== "Working")) return []
    try {
      const transition = decodeSessionTransition(
        expectMap(decodeOne(envelope.body, "transition"), "transition"),
        "transition"
      )
      const at = transition.acknowledges
      if (at === undefined || transition.actor !== WORKER.wireId()) return []
      return [[state.name, [at.partitionId, at.offset]] as const]
    } catch {
      return []
    }
  })
}

function sameAck(left: readonly [State, Position], right: readonly [State, Position]): boolean {
  return left[0] === right[0] && left[1][0] === right[1][0] && left[1][1] === right[1][1]
}

async function acknowledged(
  laser: Laser,
  session: ConversationId,
  state: State,
  request: Position
): Promise<boolean> {
  return (await acknowledgments(laser, session)).some((ack) => sameAck(ack, [state, request]))
}

async function waitAcknowledged(
  laser: Laser,
  session: ConversationId,
  state: State,
  request: Position
): Promise<void> {
  await eventually(async () =>
    (await acknowledged(laser, session, state, request)) ? true : undefined
  )
}

async function parkings(
  laser: Laser,
  session: ConversationId,
  operation: string
): Promise<readonly SessionParking[]> {
  return (await lane(laser, session))
    .filter((envelope) => envelope.kind === AgentKind.Event && envelope.operation === operation)
    .map((envelope) =>
      decodeSessionParking(expectMap(decodeOne(envelope.body, "parking"), "parking"), "parking")
    )
}

function parked(laser: Laser, session: ConversationId): Promise<ParkedRecords> {
  return laser.sessions().open(session).parked()
}

function waitParked(laser: Laser, session: ConversationId, count: number): Promise<ParkedRecords> {
  return eventually(async () => {
    const held = await parked(laser, session)
    return held.records.length === count ? held : undefined
  })
}

async function waitSeen(recorder: Recorder, body: string, count: number): Promise<void> {
  await eventually(() => Promise.resolve(recorder.count(body) >= count ? true : undefined))
  assert.equal(recorder.count(body), count, `\`${body}\` is handled ${String(count)} times`)
}

// Decoded records may carry null-prototype maps, so compare their JSON.
function plain(value: unknown): string {
  return JSON.stringify(value, (_key, field: unknown) =>
    typeof field === "bigint" ? field.toString() : field
  )
}

function offsetOf(parking: SessionParking): bigint {
  const source = parking.source
  if (source.kind !== "message") assert.fail("a parking names a log record")
  return source.offset
}

// Pause, hold one record, resume, and handle it, three times over.
async function pauseCycles(laser: Laser, runtime: Laser, concurrency: ConcurrencyPolicy) {
  const recorder = new Recorder()
  const worker = await spawn(runtime, recorder, concurrency)
  const session = ConversationId.new()
  const operator = control(laser, session)
  await sendWork(laser, session, "before")
  await waitSeen(recorder, "before", 1)
  for (let cycle = 0; cycle < 3; cycle += 1) {
    const pause = position(await operator.pause())
    await waitAcknowledged(laser, session, "Paused", pause)
    const body = `held-${String(cycle)}`
    const source = await sendWork(laser, session, body)
    const held = await waitParked(laser, session, 1)
    assert.ok(held.complete)
    const [record] = held.records
    assert.ok(record !== undefined)
    assert.equal(record.role, WORKER.wireId())
    assert.deepEqual([record.request.partitionId, record.request.offset], pause)
    assert.equal(offsetOf(record), source[1])
    assert.equal(recorder.count(body), 0, "a held record is not handled")
    const resume = position(await operator.resume())
    await waitAcknowledged(laser, session, "Working", resume)
    await waitSeen(recorder, body, 1)
    await waitParked(laser, session, 0)
  }
  await sendWork(laser, session, "after")
  await waitSeen(recorder, "after", 1)
  assert.deepEqual(
    recorder.bodies,
    ["before", "held-0", "held-1", "held-2", "after"],
    "every record is handled once, in order"
  )
  assert.equal((await parkings(laser, session, OPERATION_SESSION_PARKED)).length, 3)
  assert.equal((await parkings(laser, session, OPERATION_SESSION_UNPARKED)).length, 3)
  await worker.shutdown()
}

void test("given_repeated_pause_cycles_when_work_arrives_while_paused_then_should_park_it_and_handle_it_once_after_each_resume", async () => {
  const { laser, native } = await setup()
  try {
    await pauseCycles(laser, native, SERIAL)
  } finally {
    await laser.close()
  }
})

void test("given_the_group_engine_and_partition_lanes_when_paused_and_resumed_then_should_park_and_handle_once", async () => {
  const { laser } = await setup()
  try {
    await pauseCycles(laser, laser, { kind: "serial-per-partition", maxPartitions: 4 })
  } finally {
    await laser.close()
  }
})

void test("given_work_on_the_lane_when_paused_without_participants_then_should_name_the_working_agent_and_acknowledge", async () => {
  const { laser, native } = await setup()
  try {
    const recorder = new Recorder()
    const worker = await spawn(native, recorder)
    const session = ConversationId.new()
    await sendWork(laser, session, "first")
    await waitSeen(recorder, "first", 1)
    const pause = position(
      await laser
        .sessions()
        .control(laser.defaultStream ?? "", session)
        .asOperator(OPERATOR)
        .pause()
    )
    const request = (await laser.context(session).fetchWith([AgentTopic.Control], new LastN(10)))
      .flatMap((record) => (record.envelope === undefined ? [] : [record.envelope]))
      .find((envelope) => envelope.operation === OPERATION_SESSION_PAUSE)
    assert.ok(request !== undefined, "the pause request is on the control topic")
    assert.deepEqual(decodeSessionPauseRequestJson(request.body).participants, [WORKER.wireId()])
    await waitAcknowledged(laser, session, "Paused", pause)
    await worker.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_paused_session_when_canceled_then_should_end_canceled_and_keep_held_records_unprocessed", async () => {
  const { laser, native } = await setup()
  try {
    const recorder = new Recorder()
    const worker = await spawn(native, recorder)
    const session = ConversationId.new()
    const operator = control(laser, session)
    const pause = position(await operator.pause())
    await waitAcknowledged(laser, session, "Paused", pause)
    const source = await sendWork(laser, session, "held")
    await waitParked(laser, session, 1)
    await operator.cancel()
    await eventually(async () =>
      (await lane(laser, session)).some(
        (envelope) =>
          envelope.taskState?.kind === "known" &&
          envelope.taskState.name === "Canceled" &&
          envelope.source === WORKER.wireId()
      )
        ? true
        : undefined
    )
    const resume = position(await operator.resume())
    // Work after the resume is the handler's to judge, or held when it
    // arrives before the runtime reads the resume: control and work ride
    // different topics.
    await sendWork(laser, session, "late")
    const held = await eventually(async () => {
      const current = await parked(laser, session)
      return recorder.count("late") === 1 || current.records.length === 2 ? current : undefined
    })
    assert.equal(recorder.count("held"), 0, "a canceled session's held record is never handled")
    assert.ok(
      held.records.some((record) => offsetOf(record) === source[1]),
      "the held record stays listed"
    )
    assert.ok(held.complete)
    assert.ok(
      !(await acknowledged(laser, session, "Working", resume)),
      "a canceled session does not resume"
    )
    await worker.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_failed_parking_publish_when_work_arrives_while_paused_then_should_leave_it_uncommitted", async () => {
  const { laser, native } = await setup()
  try {
    const faults = new Faults()
    const governed = native.withGovernor(faults, GovernorMode.Enforce)
    const recorder = new Recorder()
    const worker = await spawn(governed, recorder)
    const session = ConversationId.new()
    const operator = control(laser, session)
    const pause = position(await operator.pause())
    await waitAcknowledged(laser, session, "Paused", pause)
    faults.arm(OPERATION_SESSION_PARKED)
    await sendWork(laser, session, "held")
    await assert.rejects(worker.join(), "a failed parking stops the runtime")
    assert.equal(recorder.count("held"), 0)
    assert.equal((await parked(laser, session)).records.length, 0)

    faults.disarm()
    const restarted = await spawn(governed, recorder)
    await waitParked(laser, session, 1)
    const resume = position(await operator.resume())
    await waitAcknowledged(laser, session, "Working", resume)
    await waitSeen(recorder, "held", 1)
    await waitParked(laser, session, 0)
    await restarted.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_failed_completion_publish_when_resumed_then_should_handle_the_held_record_again", async () => {
  const { laser, native } = await setup()
  try {
    const faults = new Faults()
    const governed = native.withGovernor(faults, GovernorMode.Enforce)
    const recorder = new Recorder()
    const worker = await spawn(governed, recorder)
    const session = ConversationId.new()
    const operator = control(laser, session)
    const pause = position(await operator.pause())
    await waitAcknowledged(laser, session, "Paused", pause)
    await sendWork(laser, session, "held")
    await waitParked(laser, session, 1)
    faults.arm(OPERATION_SESSION_UNPARKED)
    const resume = position(await operator.resume())
    await waitAcknowledged(laser, session, "Working", resume)
    await waitSeen(recorder, "held", 1)
    assert.equal(
      (await parked(laser, session)).records.length,
      1,
      "without its completion the record stays held"
    )

    faults.disarm()
    await sendWork(laser, session, "next")
    await waitSeen(recorder, "next", 1)
    assert.deepEqual(
      recorder.bodies,
      ["held", "held", "next"],
      "the held record is handled again before new work"
    )
    await waitParked(laser, session, 0)
    assert.equal((await parkings(laser, session, OPERATION_SESSION_UNPARKED)).length, 1)
    await worker.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_failed_pause_acknowledgment_when_work_arrives_then_should_leave_it_uncommitted_and_acknowledge_after_restart", async () => {
  const { laser, native } = await setup()
  try {
    const faults = new Faults()
    const governed = native.withGovernor(faults, GovernorMode.Enforce)
    const recorder = new Recorder()
    const worker = await spawn(governed, recorder)
    const session = ConversationId.new()
    const operator = control(laser, session)
    faults.arm(ACKNOWLEDGMENT)
    const pause = position(await operator.pause())
    // The refused eager acknowledgment proves the runtime read the pause, so
    // the work below meets a paused session.
    await eventually(() =>
      Promise.resolve(faults.refused.includes(ACKNOWLEDGMENT) ? true : undefined)
    )
    await sendWork(laser, session, "held")
    await assert.rejects(worker.join(), "a failed acknowledgment stops the runtime")
    assert.equal((await acknowledgments(laser, session)).length, 0)
    assert.equal((await parked(laser, session)).records.length, 0)

    faults.disarm()
    const restarted = await spawn(governed, recorder)
    await waitAcknowledged(laser, session, "Paused", pause)
    await waitParked(laser, session, 1)
    assert.deepEqual(
      await acknowledgments(laser, session),
      [["Paused", pause]],
      "the pause is acknowledged once"
    )
    const resume = position(await operator.resume())
    await waitAcknowledged(laser, session, "Working", resume)
    await waitSeen(recorder, "held", 1)
    await restarted.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_crash_after_the_effect_when_restarted_then_should_handle_the_held_record_again_and_complete_it", async () => {
  const { laser, native } = await setup()
  try {
    const recorder = new Recorder("held")
    const worker = await spawn(native, recorder)
    const session = ConversationId.new()
    const operator = control(laser, session)
    const pause = position(await operator.pause())
    await waitAcknowledged(laser, session, "Paused", pause)
    await sendWork(laser, session, "held")
    await waitParked(laser, session, 1)
    await operator.resume()
    await eventually(() => Promise.resolve(recorder.reached ? true : undefined))
    worker.abort()
    await worker.join().catch(() => undefined)
    assert.equal(recorder.count("held"), 1)
    assert.equal(
      (await parked(laser, session)).records.length,
      1,
      "a crash before the completion leaves the record held"
    )

    const restarted = await spawn(
      (await Laser.fromClient(laser.client)).withDefaultStream(laser.defaultStream ?? ""),
      recorder
    )
    await waitSeen(recorder, "held", 2)
    await waitParked(laser, session, 0)
    assert.equal((await parkings(laser, session, OPERATION_SESSION_UNPARKED)).length, 1)
    await restarted.shutdown()
  } finally {
    await laser.close()
  }
})

// The numeric ids of the stream and its session and control topics, with the
// session topic's generation.
async function topicIds(
  laser: Laser
): Promise<readonly [stream: number, sessions: number, generation: bigint, control: number]> {
  const stream = laser.defaultStream ?? ""
  const transport = laser[INTERNAL_TRANSPORT]()
  const sessions = await transport.resolveStreamTopicIds?.(stream, AGENT_SESSIONS)
  const control = await transport.resolveStreamTopicIds?.(stream, AGENT_CONTROL)
  const details = await transport.findSnapshotTopic?.(stream, AGENT_SESSIONS)
  assert.ok(sessions !== undefined && control !== undefined && details !== undefined)
  return [sessions.streamId, sessions.topicId, details.createdAtMicros, control.topicId]
}

async function writeParking(
  laser: Laser,
  session: ConversationId,
  parking: SessionParking
): Promise<void> {
  await laser
    .agdx(AgentTopic.Sessions, WORKER, session)
    .emit(encodeNamed(encodeSessionParking(parking)))
    .withOperation(OPERATION_SESSION_PARKED)
    .contentType(ContentType.Cbor)
    .send()
}

void test("given_a_crash_after_parking_before_the_commit_when_restarted_then_should_not_park_or_handle_twice", async () => {
  const { laser, native } = await setup()
  try {
    const session = ConversationId.new()
    const operator = control(laser, session)
    const pause = position(await operator.pause())
    // The log a worker leaves when it dies after appending the parking and
    // before committing the source: the record, its parking, no commit.
    const source = await sendWork(laser, session, "held")
    const [streamId, sessionsTopic, generation, controlTopic] = await topicIds(laser)
    await writeParking(laser, session, {
      source: {
        kind: "message",
        stream: streamId,
        topic: sessionsTopic,
        partition: source[0],
        offset: source[1],
        generation,
        conversation: session.toString()
      },
      role: WORKER.wireId(),
      request: { streamId, topicId: controlTopic, partitionId: pause[0], offset: pause[1] }
    })

    const recorder = new Recorder()
    const worker = await spawn(native, recorder)
    await waitAcknowledged(laser, session, "Paused", pause)
    const resume = position(await operator.resume())
    await waitAcknowledged(laser, session, "Working", resume)
    await waitSeen(recorder, "held", 1)
    // The source shares the session's partition, so once later work is
    // handled the redelivered source was settled too.
    await sendWork(laser, session, "after")
    await waitSeen(recorder, "after", 1)
    assert.deepEqual(recorder.bodies, ["held", "after"])
    assert.equal(
      (await parkings(laser, session, OPERATION_SESSION_PARKED)).length,
      1,
      "the redelivered record is not parked again"
    )
    assert.equal((await parkings(laser, session, OPERATION_SESSION_UNPARKED)).length, 1)
    await worker.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_resume_while_the_agent_was_offline_when_restarted_then_should_acknowledge_and_handle_the_held_record", async () => {
  const { laser, native } = await setup()
  try {
    const recorder = new Recorder()
    const worker = await spawn(native, recorder)
    const session = ConversationId.new()
    const operator = control(laser, session)
    const pause = position(await operator.pause())
    await waitAcknowledged(laser, session, "Paused", pause)
    await sendWork(laser, session, "held")
    await waitParked(laser, session, 1)
    await worker.shutdown()

    const resume = position(await operator.resume())
    const restarted = await spawn(native, recorder)
    await waitAcknowledged(laser, session, "Working", resume)
    await waitSeen(recorder, "held", 1)
    await waitParked(laser, session, 0)
    assert.deepEqual(
      await acknowledgments(laser, session),
      [
        ["Paused", pause],
        ["Working", resume]
      ],
      "each request is acknowledged once"
    )
    await restarted.shutdown()
  } finally {
    await laser.close()
  }
})

void test("given_a_held_record_of_a_recreated_topic_when_resumed_then_should_report_it_and_keep_it_listed", async () => {
  const { laser, native } = await setup()
  try {
    const session = ConversationId.new()
    const operator = control(laser, session)
    const pause = position(await operator.pause())
    const source = await sendWork(laser, session, "stale")
    const [streamId, sessionsTopic, generation, controlTopic] = await topicIds(laser)
    const parking: SessionParking = {
      source: {
        kind: "message",
        stream: streamId,
        topic: sessionsTopic,
        partition: source[0],
        offset: source[1],
        generation: generation + 1n,
        conversation: session.toString()
      },
      role: WORKER.wireId(),
      request: { streamId, topicId: controlTopic, partitionId: pause[0], offset: pause[1] }
    }
    await writeParking(laser, session, parking)
    const resume = position(await operator.resume())

    const recorder = new Recorder()
    const worker = await spawn(native, recorder)
    await sendWork(laser, session, "after")
    await waitSeen(recorder, "after", 1)
    const held = await parked(laser, session)
    assert.equal(plain(held.records), plain([parking]), "the unrecoverable record stays listed")
    assert.ok(!held.complete, "an unreadable held record makes the read incomplete")
    assert.ok(
      !(await acknowledged(laser, session, "Working", resume)),
      "a role that never acknowledged the pause does not acknowledge the resume"
    )
    await worker.shutdown()
  } finally {
    await laser.close()
  }
})

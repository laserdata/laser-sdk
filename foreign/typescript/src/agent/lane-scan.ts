import { INTERNAL_TRANSPORT } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { CONTEXT_READ_WINDOW, LastN } from "../context.js"
import type { LaserTransport, PolledMessage } from "../iggy/apache-iggy.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import type { ConversationId } from "../types/ids.js"
import {
  type AgentEnvelope,
  type AgentId,
  AgentKind,
  OPERATION_CHAT,
  OPERATION_SESSION,
  OPERATION_SESSION_PARKED,
  OPERATION_SESSION_UNPARKED,
  type SessionParking,
  decodeSessionParking,
  decodeSessionTransition,
  taskStateIsTerminal
} from "../wire/agent.js"
import { decodeOne, expectMap } from "../wire/cbor.js"
import {
  CONTROL_OPERATIONS,
  OPERATION_EXECUTE_TOOL,
  OPERATION_GENERATE_CONTENT,
  OPERATION_TEXT_COMPLETION
} from "../wire/dispatch.js"
import { logPositionToBytes } from "../wire/ids.js"
import { AGENT_SESSIONS } from "../wire/topics.js"
import type { ControlPosition } from "./control.js"
import { decodeAgentMessage } from "./decode.js"

/** The held work of one session that no agent reported handled, read by
 * `Session.parked`. */
export interface ParkedRecords {
  /** The held records, by source position. */
  readonly records: readonly SessionParking[]
  /** False when the bounded read of `agent.sessions` did not reach the oldest
   * retained record of the session's partition, so older held records may be
   * missing, or when a held record can no longer be read back because its
   * source expired or its topic was recreated. */
  readonly complete: boolean
}

// How many records one poll of a bounded read asks for.
const READ_BATCH = 1_000
const WINDOW = BigInt(CONTEXT_READ_WINDOW)

// The consumer names the bounded reads poll under. Reads never commit.
/** @internal */
export const CONTROL_READER = "laser-control-follower"
const LANE_READER = "laser-session-recovery"
/** @internal */
export const PARKED_READER = "laser-parked-reader"

/** Where one held record sits: its stream, topic, partition, offset, and the
 * topic generation that proves the numeric topic id.
 * @internal */
export interface SourceAddress {
  readonly stream: number
  readonly topic: number
  readonly partition: number
  readonly offset: bigint
  readonly generation?: bigint
}

/** One parking with its identity: the source address, the role, and the
 * pause request position.
 * @internal */
export interface Parked {
  readonly key: string
  readonly address: SourceAddress
  readonly parking: SessionParking
}

/** A role's acknowledgment of one control request.
 * @internal */
export interface Ack {
  readonly state: "Paused" | "Working"
  readonly request: ControlPosition
  readonly laneOffset?: bigint
}

/** The parking of `parking` with its identity, `undefined` when it names no
 * log record.
 * @internal */
export function parkedOf(parking: SessionParking): Parked | undefined {
  const source = parking.source
  if (source.kind !== "message") return undefined
  const address: SourceAddress = {
    stream: source.stream,
    topic: source.topic,
    partition: source.partition,
    offset: source.offset,
    ...(source.generation !== undefined ? { generation: source.generation } : {})
  }
  const request = Buffer.from(logPositionToBytes(parking.request)).toString("hex")
  return { key: `${addressKey(address)}|${parking.role}|${request}`, address, parking }
}

/** The text key of one source address.
 * @internal */
export function addressKey(address: SourceAddress): string {
  return [
    String(address.stream),
    String(address.topic),
    String(address.partition),
    address.offset.toString(),
    address.generation?.toString() ?? ""
  ].join(":")
}

/** Order source addresses by stream, topic, partition, and offset.
 * @internal */
export function compareAddresses(left: SourceAddress, right: SourceAddress): number {
  return (
    left.stream - right.stream ||
    left.topic - right.topic ||
    left.partition - right.partition ||
    (left.offset < right.offset ? -1 : left.offset > right.offset ? 1 : 0)
  )
}

/** The pause facts one bounded read found for one session.
 * @internal */
export class SessionFacts {
  partition: number | undefined
  ack: Ack | undefined
  readonly parked = new Map<string, Parked>()
  readonly handled = new Set<string>()
  terminal = false

  /** The parkings without a completion. */
  pending(): readonly Parked[] {
    return [...this.parked.values()].filter((parked) => !this.handled.has(parked.key))
  }
}

/** The pause facts a bounded read of `agent.sessions` found.
 * @internal */
export class LaneScan {
  readonly sessions = new Map<string, SessionFacts>()
  /** Partitions whose read did not reach their oldest retained record. */
  readonly truncated = new Set<number>()

  completeFor(session: string): boolean {
    const partition = this.sessions.get(session)?.partition
    return partition === undefined ? this.truncated.size === 0 : !this.truncated.has(partition)
  }

  // Fold one lane record. `role` keeps only that role's facts. `only` keeps
  // only one session and notes its partition.
  fold(
    partition: number,
    message: PolledMessage,
    role: AgentId | undefined,
    only: string | undefined
  ): void {
    const decoded = decodeAgentMessage({ ...message, partitionId: partition })
    if (decoded.kind !== "message") return
    const envelope = decoded.message.envelope
    if (envelope === undefined) return
    const session = envelope.conversation.toString()
    if (only !== undefined && only !== session) return
    if (only !== undefined) this.facts(session).partition = partition
    if (envelope.kind === AgentKind.Status && envelope.operation === OPERATION_SESSION) {
      this.foldStatus(session, envelope, message.offset, role)
      return
    }
    if (
      envelope.kind === AgentKind.Event &&
      (envelope.operation === OPERATION_SESSION_PARKED ||
        envelope.operation === OPERATION_SESSION_UNPARKED)
    ) {
      let parking: SessionParking
      try {
        parking = decodeSessionParking(
          expectMap(decodeOne(envelope.body, "parking"), "parking"),
          "parking"
        )
      } catch {
        return
      }
      if (role !== undefined && parking.role !== role) return
      const parked = parkedOf(parking)
      if (parked === undefined) return
      const facts = this.facts(session)
      if (envelope.operation === OPERATION_SESSION_PARKED) facts.parked.set(parked.key, parked)
      else facts.handled.add(parked.key)
    }
  }

  private foldStatus(
    session: string,
    envelope: AgentEnvelope,
    offset: bigint,
    role: AgentId | undefined
  ): void {
    const state = envelope.taskState
    if (state === undefined) return
    if (taskStateIsTerminal(state)) {
      if (role !== undefined && envelope.source === role) this.facts(session).terminal = true
      return
    }
    if (state.kind !== "known" || (state.name !== "Paused" && state.name !== "Working")) return
    if (role === undefined) return
    let transition
    try {
      transition = decodeSessionTransition(
        expectMap(decodeOne(envelope.body, "transition"), "transition"),
        "transition"
      )
    } catch {
      return
    }
    if (transition.actor !== role || transition.acknowledges === undefined) return
    this.facts(session).ack = {
      state: state.name,
      request: [transition.acknowledges.partitionId, transition.acknowledges.offset],
      laneOffset: offset
    }
  }

  private facts(session: string): SessionFacts {
    let facts = this.sessions.get(session)
    if (facts === undefined) {
      facts = new SessionFacts()
      this.sessions.set(session, facts)
    }
    return facts
  }
}

/** The newest records of one partition, up to the context read window.
 * @internal */
export interface Window {
  readonly messages: readonly PolledMessage[]
  readonly nextOffset: bigint
  /** The partition holds or held records before the window. */
  readonly truncated: boolean
}

/** Read the newest records of one partition, up to the context read window.
 * @internal */
export async function readWindow(
  transport: LaserTransport,
  stream: string,
  topic: string,
  reader: string,
  partition: number
): Promise<Window> {
  const target = { kind: "single", partitionId: partition, name: reader } as const
  const [head] = await transport.pollMessages(stream, topic, target, { kind: "first" }, 1, false)
  if (head === undefined) return { messages: [], nextOffset: 0n, truncated: false }
  const first = head.offset
  const [last] = await transport.pollMessages(stream, topic, target, { kind: "last" }, 1, false)
  const tail = (last?.offset ?? first) + 1n
  const start = tail - WINDOW > first ? tail - WINDOW : first
  const drained = await drainPartition(transport, stream, topic, reader, partition, start)
  return {
    messages: drained.messages,
    nextOffset: drained.nextOffset,
    truncated: first > 0n || start > first
  }
}

/** Read one partition from `from` until no record is left.
 * @internal */
export async function drainPartition(
  transport: LaserTransport,
  stream: string,
  topic: string,
  reader: string,
  partition: number,
  from: bigint
): Promise<{ readonly messages: readonly PolledMessage[]; readonly nextOffset: bigint }> {
  const target = { kind: "single", partitionId: partition, name: reader } as const
  const messages: PolledMessage[] = []
  let next = from
  for (;;) {
    const batch = await transport.pollMessages(
      stream,
      topic,
      target,
      { kind: "offset", value: next },
      READ_BATCH,
      false
    )
    const fresh = batch.filter((message) => message.offset >= next)
    if (fresh.length === 0) return { messages, nextOffset: next }
    messages.push(...fresh)
    next = (fresh.at(-1)?.offset ?? next) + 1n
  }
}

/** Read the newest records of every `agent.sessions` partition, up to the
 * context read window, and fold the pause facts of `role` (every role when
 * absent) for `only` (every session when absent).
 * @internal */
export async function scanLane(
  laser: Laser,
  role: AgentId | undefined,
  only: string | undefined
): Promise<LaneScan> {
  const scan = new LaneScan()
  const stream = laser.defaultStream
  if (stream === undefined) return scan
  const transport = laser[INTERNAL_TRANSPORT]()
  const partitions = await transport.findTopicPartitionCount(stream, AGENT_SESSIONS)
  for (let partition = 0; partition < (partitions ?? 0); partition += 1) {
    const window = await readWindow(transport, stream, AGENT_SESSIONS, LANE_READER, partition)
    if (window.truncated) scan.truncated.add(partition)
    for (const message of window.messages) scan.fold(partition, message, role, only)
  }
  return scan
}

/** The work records `session`'s agents held while it was paused and have not
 * reported handled.
 * @internal */
export async function readParked(laser: Laser, session: string): Promise<ParkedRecords> {
  const scan = await scanLane(laser, undefined, session)
  let complete = scan.completeFor(session)
  const pending = [...(scan.sessions.get(session)?.pending() ?? [])].sort((left, right) =>
    compareAddresses(left.address, right.address)
  )
  const retained = new SourceCheck(laser[INTERNAL_TRANSPORT]())
  for (const parked of pending) {
    if (!(await retained.readable(parked.address))) complete = false
  }
  return { records: pending.map((parked) => parked.parking), complete }
}

// Whether held records can still be read back from their sources, with the
// topic generations and partition starts looked up once each.
class SourceCheck {
  private readonly topics = new Map<
    string,
    { readonly stream: string; readonly topic: string; readonly generation: bigint } | undefined
  >()
  private readonly first = new Map<string, bigint | undefined>()

  constructor(private readonly transport: LaserTransport) {}

  async readable(address: SourceAddress): Promise<boolean> {
    const topicKey = `${String(address.stream)}:${String(address.topic)}`
    if (!this.topics.has(topicKey)) this.topics.set(topicKey, await this.describe(address))
    const topic = this.topics.get(topicKey)
    if (topic === undefined || topic.generation !== address.generation) return false
    const partitionKey = `${topicKey}:${String(address.partition)}`
    if (!this.first.has(partitionKey)) {
      const [head] = await this.transport.pollMessages(
        topic.stream,
        topic.topic,
        { kind: "single", partitionId: address.partition, name: PARKED_READER },
        { kind: "first" },
        1,
        false
      )
      this.first.set(partitionKey, head?.offset)
    }
    const first = this.first.get(partitionKey)
    return first !== undefined && first <= address.offset
  }

  private async describe(
    address: SourceAddress
  ): Promise<
    { readonly stream: string; readonly topic: string; readonly generation: bigint } | undefined
  > {
    const names = await this.transport.resolveStreamTopicNames?.(address.stream, address.topic)
    if (names === undefined) return undefined
    const details = await this.transport.findSnapshotTopic?.(names.stream, names.topic)
    return details === undefined
      ? undefined
      : { stream: names.stream, topic: names.topic, generation: details.createdAtMicros }
  }
}

/** The agents the lane of `session` shows working on it: the addressees of
 * its work commands and the agents that picked it up or acknowledged a
 * control request. Model, tool, and control records are not work.
 * @internal */
export async function laneParticipants(
  laser: Laser,
  session: ConversationId
): Promise<readonly AgentId[]> {
  const records = await laser
    .context(session)
    .fetchWith([AgentTopic.Sessions], new LastN(Number.MAX_SAFE_INTEGER))
  const participants = new Set<AgentId>()
  for (const record of records) {
    const envelope = record.envelope
    if (envelope === undefined) continue
    const operation = envelope.operation ?? ""
    if (envelope.kind === AgentKind.Command && isWork(operation)) {
      if (envelope.target !== undefined) participants.add(envelope.target)
    } else if (envelope.kind === AgentKind.Status && operation === OPERATION_SESSION) {
      try {
        const actor = decodeSessionTransition(
          expectMap(decodeOne(envelope.body, "transition"), "transition"),
          "transition"
        ).actor
        if (actor !== undefined) participants.add(actor)
      } catch {
        // A status without a transition names no participant.
      }
    }
  }
  return [...participants].sort()
}

/** Whether a command operation is work, not a model, tool, or control call.
 * @internal */
export function isWork(operation: string): boolean {
  return (
    !CONTROL_OPERATIONS.includes(operation) &&
    operation !== OPERATION_CHAT &&
    operation !== OPERATION_TEXT_COMPLETION &&
    operation !== OPERATION_GENERATE_CONTENT &&
    operation !== OPERATION_EXECUTE_TOOL
  )
}

import { INTERNAL_TRANSPORT } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import type { LaserTransport, PolledMessage } from "../iggy/apache-iggy.js"
import type { Session, Sessions } from "../session.js"
import { type AgentId, ConversationId } from "../types/ids.js"
import {
  type AgentId as WireAgentId,
  OPERATION_SESSION,
  OPERATION_SESSION_PARKED,
  OPERATION_SESSION_UNPARKED,
  type SessionParking,
  TaskStateName,
  encodeSessionParking,
  encodeSessionTransition,
  taskStateFromCode
} from "../wire/agent.js"
import { encodeNamed } from "../wire/cbor.js"
import { ContentType } from "../wire/content.js"
import type { LogPosition } from "../wire/ids.js"
import { AGENT_CONTROL } from "../wire/topics.js"
import {
  type ControlBook,
  type ControlPosition,
  type ControlRequest,
  type ControlState,
  type PlacedRequest,
  controlRequest,
  pausedBy
} from "./control.js"
import {
  type Ack,
  CONTROL_READER,
  type LaneScan,
  PARKED_READER,
  type Parked,
  type SourceAddress,
  addressKey,
  compareAddresses,
  drainPartition,
  parkedOf,
  readWindow,
  scanLane
} from "./lane-scan.js"
import { type AgentMessage, type ReceivedAgentMessage, decodeAgentMessage } from "./decode.js"

// The consecutive read failures after which the control follower stops.
const MAX_CONSECUTIVE_READ_ERRORS = 10

/** Handles a held record again after the resume, through the same decode,
 * gates, handler, retry, and dead letter as live delivery, but without the
 * pause check. Rejects only when a dead letter could not be published.
 * @internal */
export interface Replay {
  replay(received: ReceivedAgentMessage): Promise<void>
}

/** What the pause check decided for one work record: handle it now, or
 * commit it without handling because it is durably parked or was already
 * handled through its parking.
 * @internal */
export type Hold = "handle" | "commit"

/** Where a runtime's work records come from.
 * @internal */
export interface SourceTopic {
  readonly stream: string
  readonly topic: string
  readonly streamId: number
  readonly topicId: number
  /** The topic's creation time, which proves the numeric topic id. */
  readonly generation: bigint
}

/** `agent.control` as the startup read found it: its ids and the next offset
 * to follow on each partition.
 * @internal */
export interface ControlFeed {
  readonly streamId: number
  readonly topicId: number
  readonly next: Map<number, bigint>
  /** Some partition holds more than the bounded read covered, or already
   * expired records, so requests older than the read may be missing. */
  readonly truncated: boolean
}

/** Read the newest records of every `agent.control` partition into `book`
 * and return where to follow from, `undefined` when the stream has no
 * control topic. The book is not marked live here.
 * @internal */
export async function loadControl(
  laser: Laser,
  me: WireAgentId | undefined,
  book: ControlBook
): Promise<ControlFeed | undefined> {
  const stream = laser.defaultStream
  if (stream === undefined) return undefined
  const transport = laser[INTERNAL_TRANSPORT]()
  const partitions = await transport.findTopicPartitionCount(stream, AGENT_CONTROL)
  if (partitions === undefined) return undefined
  const ids = (await transport.resolveStreamTopicIds?.(stream, AGENT_CONTROL)) ?? {
    streamId: 0,
    topicId: 0
  }
  const next = new Map<number, bigint>()
  let truncated = false
  const records: (readonly [string, ControlPosition, ControlRequest])[] = []
  for (let partition = 0; partition < partitions; partition += 1) {
    const window = await readWindow(transport, stream, AGENT_CONTROL, CONTROL_READER, partition)
    truncated ||= window.truncated
    next.set(partition, window.nextOffset)
    for (const message of window.messages) {
      const read = readControl(message, partition, me)
      if (read !== undefined) records.push(read)
    }
  }
  book.load(records)
  return { streamId: ids.streamId, topicId: ids.topicId, next, truncated }
}

function readControl(
  message: PolledMessage,
  partition: number,
  me: WireAgentId | undefined
): readonly [string, ControlPosition, ControlRequest] | undefined {
  const decoded = decodeAgentMessage({ ...message, partitionId: partition })
  if (decoded.kind !== "message") return undefined
  const envelope = decoded.message.envelope
  if (envelope === undefined) return undefined
  const request = controlRequest(envelope, me)
  return request === undefined
    ? undefined
    : [envelope.conversation.toString(), [partition, message.offset], request]
}

/** The control follower: polls every `agent.control` partition from where the
 * startup read ended and records each request for `me` in the book, so every
 * instance of a role sees every request whatever partitions its consumer group
 * assigns it. Each request is handed to `notify`. When the follower stops, the
 * book stops claiming to be current and every read folds the log.
 * @internal */
export class ControlFollower {
  private readonly stopper = new AbortController()
  private readonly done: Promise<void>

  constructor(
    private readonly transport: LaserTransport,
    private readonly stream: string,
    private readonly feed: ControlFeed,
    private readonly me: WireAgentId | undefined,
    private readonly book: ControlBook,
    private readonly notify: ((session: string) => void) | undefined,
    private readonly pollIntervalMs: number
  ) {
    book.setLive(true)
    this.done = this.follow()
  }

  /** Stop following and wait for the follower to finish. */
  async stop(): Promise<void> {
    this.stopper.abort()
    await this.done
  }

  /** Stop following without waiting. */
  abort(): void {
    this.stopper.abort()
  }

  private async follow(): Promise<void> {
    const signal = this.stopper.signal
    let failures = 0
    try {
      while (!signal.aborted) {
        let read = false
        try {
          for (const [partition, next] of this.feed.next) {
            const batch = await drainPartition(
              this.transport,
              this.stream,
              AGENT_CONTROL,
              CONTROL_READER,
              partition,
              next
            )
            read ||= batch.messages.length > 0
            for (const message of batch.messages) this.observe(message, partition)
            this.feed.next.set(partition, batch.nextOffset)
          }
          failures = 0
        } catch {
          failures += 1
          if (failures >= MAX_CONSECUTIVE_READ_ERRORS) return
          await pause(backoffFor(failures), signal)
          continue
        }
        if (!read) await pause(this.pollIntervalMs, signal)
      }
    } finally {
      this.book.setLive(false)
    }
  }

  private observe(message: PolledMessage, partition: number): void {
    const read = readControl(message, partition, this.me)
    if (read === undefined) return
    const [session, at, request] = read
    this.book.observe(session, at, request)
    this.notify?.(session)
  }
}

// Capped exponential backoff between consecutive read failures: 50ms, 100ms,
// 200ms, up to one second.
function backoffFor(attempt: number): number {
  return Math.min(50 * 2 ** Math.min(Math.max(attempt - 1, 0), 16), 1_000)
}

function pause(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve()
  return new Promise((resolve) => {
    const finish = (): void => {
      clearTimeout(timer)
      signal.removeEventListener("abort", finish)
      resolve()
    }
    const timer = setTimeout(finish, ms)
    signal.addEventListener("abort", finish, { once: true })
  })
}

/** What one role knows of one session's pause history.
 * @internal */
export class RoleState {
  /** The role's newest acknowledgment. */
  ack: Ack | undefined
  /** Held records without a completion. */
  readonly parked = new Map<string, Parked>()
  /** Held records with a completion. */
  readonly handled = new Set<string>()
  /** Held records whose source cannot be read back. They stay listed. */
  readonly unrecoverable = new Set<string>()
  /** The role wrote a terminal status. */
  terminal = false

  /** Keep the acknowledgment latest on the lane. */
  recordAck(ack: Ack): void {
    if (this.ack !== undefined && laterOnLane(this.ack, ack)) return
    this.ack = ack
  }
}

function laterOnLane(current: Ack, ack: Ack): boolean {
  if (current.laneOffset === undefined) return false
  return ack.laneOffset === undefined || current.laneOffset > ack.laneOffset
}

/** A FIFO lock, one per key.
 * @internal */
class KeyedLock {
  private readonly tails = new Map<string, Promise<void>>()

  async acquire(key: string): Promise<() => void> {
    const previous = this.tails.get(key) ?? Promise.resolve()
    let release = (): void => undefined
    const held = new Promise<void>((resolve) => {
      release = resolve
    })
    const tail = previous.then(() => held)
    this.tails.set(key, tail)
    await previous
    return () => {
      release()
      if (this.tails.get(key) === tail) this.tails.delete(key)
    }
  }
}

/** The pause and resume runtime of one agent role. Work for a paused session
 * is durably parked on the session lane before its source offset is
 * committed, the role acknowledges each pause and resume request it takes part
 * in on the lane, and a resume handles the held work again before new work for
 * the session. Every decision is rebuilt from the log after a restart, a
 * rebalance, or a reopened consumer.
 * @internal */
export class PauseRuntime {
  private readonly gates = new KeyedLock()
  private readonly roles = new Map<string, RoleState>()
  private assignment: ReadonlySet<number> | undefined
  private epoch = 1
  private recoveredEpoch = 0
  private recovering: Promise<void> | undefined
  private readonly meWire: WireAgentId

  constructor(
    private readonly laser: Laser,
    private readonly sessions: Sessions,
    private readonly me: AgentId,
    private readonly book: ControlBook,
    private readonly control: readonly [number, number],
    private readonly controlTruncated: boolean,
    private readonly source: SourceTopic,
    private readonly reconcileLater: (session: string) => void
  ) {
    this.meWire = me.wireId()
  }

  /** Serialize the work and control handling of one session in this runtime.
   * A pause acknowledgment taken under the gate comes after every record of
   * the session already in flight. Resolves to the release. */
  gate(session: string): Promise<() => void> {
    return this.gates.acquire(session)
  }

  /** Decide what to do with one work record of a session, under its gate. A
   * paused session acknowledges the pause and parks the record. A resumed
   * session acknowledges the resume and handles its held records first. A
   * failed acknowledgment, parking, or completion rejects, so the record stays
   * uncommitted. */
  async hold(worker: Replay, message: AgentMessage): Promise<Hold> {
    const session = message.provenance.conversationId.toString()
    await this.ensureRecovered()
    for (;;) {
      const control = await this.controlState(session)
      const pauseRequest = pausedBy(control)
      if (pauseRequest !== undefined) {
        await this.acknowledge(session, "Paused", pauseRequest)
        if (control.flags.cancelRequested) await this.cancelPaused(session, true)
        await this.park(session, message, pauseRequest)
        return "commit"
      }
      await this.acknowledgeResume(session, control)
      if (
        !control.flags.cancelRequested &&
        this.pending(session).length > 0 &&
        (await this.drain(worker, session, false)) === "interrupted"
      ) {
        continue
      }
      // A record redelivered after a crash between its parking and its
      // commit was just handled through the parking.
      return this.handledAt(session, this.address(message)) ? "commit" : "handle"
    }
  }

  /** Bring one session up to date with its control requests outside the work
   * path: acknowledge a pause the role is named in or already takes part in,
   * end a paused session canceled, acknowledge a resume, and handle held
   * records on partitions this member reads. */
  async reconcile(worker: Replay, session: string): Promise<void> {
    const release = await this.gate(session)
    try {
      await this.reconcileLocked(worker, session)
    } catch {
      // The next record or request for the session retries.
    } finally {
      release()
    }
  }

  /** Run the recovery read before the first dispatch. */
  recover(): Promise<void> {
    return this.ensureRecovered()
  }

  /** Note the partitions this member reads. A changed assignment means
   * another member may have written held work for a session this member now
   * reads, so the next dispatch reads the lane again. */
  observeAssignment(assigned: ReadonlySet<number> | undefined): void {
    if (assigned === undefined) return
    const known = this.assignment
    this.assignment = new Set(assigned)
    if (known !== undefined && !sameSet(known, assigned)) this.invalidate()
  }

  /** Read the lane again before the next dispatch, after the consumer was
   * reopened. */
  invalidate(): void {
    this.epoch += 1
  }

  private async reconcileLocked(worker: Replay, session: string): Promise<void> {
    await this.ensureRecovered()
    const control = await this.controlState(session)
    const pauseRequest = pausedBy(control)
    if (pauseRequest !== undefined) {
      if (control.flags.cancelRequested) {
        await this.cancelPaused(session, pauseRequest.named)
      } else if (pauseRequest.named || this.involved(session)) {
        await this.acknowledge(session, "Paused", pauseRequest)
      }
      return
    }
    await this.acknowledgeResume(session, control)
    if (!control.flags.cancelRequested && this.pending(session).length > 0) {
      await this.drain(worker, session, true)
    }
  }

  // The bounded recovery read of the lane, once per assignment epoch, and
  // only when some session has a pause history or the control read could not
  // prove there is none.
  private async ensureRecovered(): Promise<void> {
    while (this.recoveredEpoch < this.epoch) {
      if (this.recovering !== undefined) {
        await this.recovering
        continue
      }
      const epoch = this.epoch
      this.recovering = this.recoverAt(epoch)
      try {
        await this.recovering
      } finally {
        this.recovering = undefined
      }
    }
  }

  private async recoverAt(epoch: number): Promise<void> {
    if (this.controlTruncated || this.book.hasPauseHistory()) {
      const scan = await scanLane(this.laser, this.meWire, undefined)
      const touched = new Set(scan.sessions.keys())
      this.merge(scan)
      for (const session of this.book.pausedOrResumed()) touched.add(session)
      for (const session of touched) this.reconcileLater(session)
    }
    this.recoveredEpoch = Math.max(this.recoveredEpoch, epoch)
  }

  private merge(scan: LaneScan): void {
    for (const [session, facts] of scan.sessions) {
      const role = this.role(session)
      for (const key of facts.handled) role.handled.add(key)
      for (const [key, parked] of facts.parked) {
        if (!role.parked.has(key)) role.parked.set(key, parked)
      }
      for (const key of role.handled) role.parked.delete(key)
      role.terminal ||= facts.terminal
      if (facts.ack !== undefined) role.recordAck(facts.ack)
    }
  }

  private controlState(session: string): Promise<ControlState> {
    return this.lens(session).controlState()
  }

  private lens(session: string): Session {
    return this.sessions.open(ConversationId.parse(session)).asAgent(this.me).withControl(this.book)
  }

  private controlPosition(at: ControlPosition): LogPosition {
    return {
      streamId: this.control[0],
      topicId: this.control[1],
      partitionId: at[0],
      offset: at[1]
    }
  }

  // Write `status(session, state)` acknowledging `request`, unless this
  // role's newest acknowledgment already is that one.
  private async acknowledge(
    session: string,
    state: "Paused" | "Working",
    request: PlacedRequest
  ): Promise<void> {
    const ack = this.roles.get(session)?.ack
    if (
      ack?.state === state &&
      ack.request[0] === request.at[0] &&
      ack.request[1] === request.at[1]
    ) {
      return
    }
    const at = this.controlPosition(request.at)
    let status = this.lens(session)
      .lane()
      .status(OPERATION_SESSION)
      .withTaskState(taskStateFromCode(TaskStateName[state]))
      .body(encodeNamed(encodeSessionTransition({ actor: this.meWire, acknowledges: at })))
      .contentType(ContentType.Cbor)
    if (request.record !== undefined) status = status.withCause(request.record, at)
    const receipt = await status.sendReceipt()
    this.role(session).recordAck({
      state,
      request: request.at,
      ...(receipt.offset !== undefined ? { laneOffset: receipt.offset } : {})
    })
  }

  // Acknowledge the resume when this role acknowledged a pause it lifts,
  // unless the session was canceled.
  private async acknowledgeResume(session: string, control: ControlState): Promise<void> {
    const paused =
      !control.flags.cancelRequested && this.roles.get(session)?.ack?.state === "Paused"
    if (paused && control.resume !== undefined) {
      await this.acknowledge(session, "Working", control.resume)
    }
  }

  // Append and confirm the parking record of `message` under `pause`. A
  // source this role already parked or handled is not parked again.
  private async park(session: string, message: AgentMessage, pause: PlacedRequest): Promise<void> {
    const address = this.address(message)
    const role = this.roles.get(session)
    const sourceKey = addressKey(address)
    const known =
      role !== undefined &&
      ([...role.parked.values()].some((parked) => addressKey(parked.address) === sourceKey) ||
        [...role.handled].some((key) => key.startsWith(`${sourceKey}|`)))
    if (known) return
    const parking: SessionParking = {
      source: {
        kind: "message",
        stream: address.stream,
        topic: address.topic,
        partition: address.partition,
        offset: address.offset,
        ...(address.generation !== undefined ? { generation: address.generation } : {}),
        conversation: session
      },
      role: this.meWire,
      request: this.controlPosition(pause.at)
    }
    const parked = parkedOf(parking)
    if (parked === undefined) return
    let event = this.lens(session)
      .lane()
      .emit(encodeNamed(encodeSessionParking(parking)))
      .withOperation(OPERATION_SESSION_PARKED)
      .contentType(ContentType.Cbor)
    const record = message.envelope?.record
    if (record !== undefined) {
      event = event.withCause(record, {
        streamId: address.stream,
        topicId: address.topic,
        partitionId: address.partition,
        offset: address.offset
      })
    }
    await event.sendReceipt()
    this.role(session).parked.set(parked.key, parked)
  }

  // Handle the held records of `session` once each, oldest source first, and
  // confirm a completion for each. Stops when the session is paused or
  // canceled again. A record whose source expired or whose topic was
  // recreated is left held.
  private async drain(
    worker: Replay,
    session: string,
    assignedOnly: boolean
  ): Promise<"all" | "interrupted"> {
    for (const held of this.pending(session)) {
      const address = held[0]?.address
      if (address === undefined) continue
      if (assignedOnly && !this.reads(address.partition)) continue
      const control = await this.controlState(session)
      if (pausedBy(control) !== undefined || control.flags.cancelRequested) return "interrupted"
      const received = await this.fetchSource(address)
      if (received === undefined) {
        const role = this.role(session)
        for (const parked of held) role.unrecoverable.add(parked.key)
        continue
      }
      await worker.replay(received)
      for (const parked of held) {
        await this.lens(session)
          .lane()
          .emit(encodeNamed(encodeSessionParking(parked.parking)))
          .withOperation(OPERATION_SESSION_UNPARKED)
          .contentType(ContentType.Cbor)
          .sendReceipt()
        const role = this.role(session)
        role.parked.delete(parked.key)
        role.handled.add(parked.key)
      }
    }
    return "all"
  }

  // The held records of `session` this runtime can handle: from its own
  // source topic and not already found unrecoverable, grouped by source and
  // oldest first.
  private pending(session: string): readonly (readonly Parked[])[] {
    const role = this.roles.get(session)
    if (role === undefined) return []
    const groups = new Map<string, Parked[]>()
    for (const parked of role.parked.values()) {
      if (
        parked.address.stream !== this.source.streamId ||
        parked.address.topic !== this.source.topicId ||
        role.unrecoverable.has(parked.key)
      ) {
        continue
      }
      const key = addressKey(parked.address)
      const group = groups.get(key)
      if (group === undefined) groups.set(key, [parked])
      else group.push(parked)
    }
    return [...groups.values()].sort((left, right) =>
      compareAddresses(left[0]?.address ?? NO_ADDRESS, right[0]?.address ?? NO_ADDRESS)
    )
  }

  private address(message: AgentMessage): SourceAddress {
    return {
      stream: this.source.streamId,
      topic: this.source.topicId,
      partition: message.id.partitionId,
      offset: message.id.offset,
      generation: this.source.generation
    }
  }

  private handledAt(session: string, address: SourceAddress): boolean {
    const role = this.roles.get(session)
    if (role === undefined) return false
    const prefix = `${addressKey(address)}|`
    return [...role.handled].some((key) => key.startsWith(prefix))
  }

  private involved(session: string): boolean {
    const role = this.roles.get(session)
    return role !== undefined && (role.ack !== undefined || role.parked.size > 0)
  }

  private reads(partition: number): boolean {
    return this.assignment === undefined || this.assignment.has(partition)
  }

  // End a paused session canceled as this role, once. Best effort: a failed
  // write is retried by the next record or request.
  private async cancelPaused(session: string, named: boolean): Promise<void> {
    const role = this.roles.get(session)
    const involved = role !== undefined && (role.ack !== undefined || role.parked.size > 0)
    if (role?.terminal === true || !(named || involved)) return
    try {
      await this.lens(session).cancel()
      this.role(session).terminal = true
    } catch {
      // The next record or request for the session retries.
    }
  }

  // Read one held record back from its source. `undefined` when the record
  // no longer exists there or the topic was recreated since it was parked.
  private async fetchSource(address: SourceAddress): Promise<ReceivedAgentMessage | undefined> {
    if (address.generation !== this.source.generation) return undefined
    const polled = await this.laser[INTERNAL_TRANSPORT]().pollMessages(
      this.source.stream,
      this.source.topic,
      { kind: "single", partitionId: address.partition, name: PARKED_READER },
      { kind: "offset", value: address.offset },
      1,
      false
    )
    const message = polled.find((candidate) => candidate.offset === address.offset)
    return message === undefined ? undefined : { ...message, partitionId: address.partition }
  }

  private role(session: string): RoleState {
    let role = this.roles.get(session)
    if (role === undefined) {
      role = new RoleState()
      this.roles.set(session, role)
    }
    return role
  }
}

const NO_ADDRESS: SourceAddress = { stream: 0, topic: 0, partition: 0, offset: 0n }

function sameSet(left: ReadonlySet<number>, right: ReadonlySet<number>): boolean {
  return left.size === right.size && [...left].every((value) => right.has(value))
}

/** The pause driver: brings each session a control request names up to date,
 * one task per request, until stopped.
 * @internal */
export class PauseDriver {
  private readonly tasks = new Set<Promise<void>>()
  private stopped = false

  constructor(
    private readonly runtime: PauseRuntime,
    private readonly worker: Replay
  ) {}

  /** Bring `session` up to date in the background. */
  request(session: string): void {
    if (this.stopped) return
    const task = this.runtime.reconcile(this.worker, session).finally(() => {
      this.tasks.delete(task)
    })
    this.tasks.add(task)
  }

  /** Stop taking requests and wait for those in flight. */
  async stop(): Promise<void> {
    this.stopped = true
    await Promise.all([...this.tasks])
  }
}

/** The control requests the follower hands the pause driver, queued until the
 * driver starts.
 * @internal */
export class PauseRequests {
  private driver: PauseDriver | undefined
  private queued: string[] = []

  /** Bring `session` up to date once the driver runs. */
  request(session: string): void {
    if (this.driver === undefined) this.queued.push(session)
    else this.driver.request(session)
  }

  /** Start handing requests to `driver`, the queued ones first. */
  attach(driver: PauseDriver): void {
    this.driver = driver
    const queued = this.queued
    this.queued = []
    for (const session of queued) driver.request(session)
  }
}

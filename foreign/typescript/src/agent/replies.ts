import { setTimeout as sleep } from "node:timers/promises"
import { CancelledError, TimeoutError } from "../client/errors.js"
import {
  type ConsumerTarget,
  type LaserTransport,
  ONE_SHOT_POLL,
  type PollWait,
  type PolledMessage
} from "../iggy/apache-iggy.js"
import { NOOP_OBSERVER, type LaserObserver } from "../observe.js"
import type { KeyRegistry } from "../signing.js"
import { Cursor, heldRead } from "../stream/cursor.js"
import { AgentKind, type AgentEnvelope } from "../wire/agent.js"
import type { CorrelationId } from "../wire/ids.js"
import type { ConversationId, MessageId } from "../types/ids.js"
import {
  decodeAgentMessage,
  type AgentMessage,
  type DecodedAgentMessage
} from "./reliable-consumer.js"
import { longTimeout } from "./timer.js"

const REPLY_BATCH = 200
// How long the server holds a read of an idle reply topic, as Rust.
const REPLY_WAIT: PollWait = { ...ONE_SHOT_POLL, maxWaitMs: 5_000 }
const REPLY_RETRY_INTERVAL_MS = 20

// A point lookup reads at most this many batches per partition, like Rust.
const MAX_LOOKUP_PASSES = 32
const LOOKUP_BATCH = 1_000

// Ceiling on replies buffered for one subscribed correlation with no consumer
// pulling them. A flood must not be retained in full.
const MAX_QUEUED_REPLIES = 1_000
const MAX_PENDING_REQUEST_REPLIES = 64

// A pause that a stopping hub ends early, so a stopped hub keeps no timer.
function delay(ms: number, signal: AbortSignal): Promise<void> {
  return sleep(ms, undefined, { signal }).catch(() => undefined)
}

function raceWithTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  onDone: () => void,
  signal?: AbortSignal
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const abort = (): void => {
      timer.cancel()
      onDone()
      reject(new CancelledError("reply wait aborted", { cause: signal?.reason }))
    }
    const timer = longTimeout(() => {
      signal?.removeEventListener("abort", abort)
      onDone()
      reject(new TimeoutError("reply"))
    }, timeoutMs)
    if (signal?.aborted === true) {
      abort()
      return
    }
    signal?.addEventListener("abort", abort, { once: true })
    promise
      .then((value) => {
        timer.cancel()
        signal?.removeEventListener("abort", abort)
        resolve(value)
      })
      .catch((error: unknown) => {
        timer.cancel()
        signal?.removeEventListener("abort", abort)
        reject(error instanceof Error ? error : new Error(String(error)))
      })
  })
}

export interface ReplyTicket {
  wait(timeoutMs: number, signal?: AbortSignal): Promise<AgentMessage>
  arm(request?: MessageId): void
  cancel(): void
}

export interface ReplyStreamTicket {
  next(timeoutMs: number, signal?: AbortSignal): Promise<AgentMessage>
  cancel(): void
}

interface PendingReply {
  readonly settle: (message: AgentMessage) => void
  readonly fail: (error: Error) => void
}

interface StreamWaiter {
  readonly queued: AgentMessage[]
  readonly pending: PendingReply[]
  readonly expectedSigner?: string
}

/** What a record must carry to answer one request: the request's session, a
 * reply kind, and, when the request named its sender, that sender as the
 * addressee.
 * @internal */
export interface ExpectedReply {
  readonly session: ConversationId
  readonly requester?: string
}

function accepts(expected: ExpectedReply, message: AgentMessage): boolean {
  if (!message.provenance.conversationId.equals(expected.session)) return false
  const envelope = message.envelope
  let addressee: string | undefined
  if (envelope !== undefined) {
    if (envelope.kind !== AgentKind.Response && envelope.kind !== AgentKind.Error) return false
    addressee = envelope.target
  } else {
    addressee = message.provenance.targetAgentId?.asStr()
  }
  return expected.requester === undefined || expected.requester === addressee
}

interface ReplyWaiter extends PendingReply {
  readonly expectedSigner?: string
  readonly expected?: ExpectedReply
  readonly deferred: boolean
  readonly pending: AgentMessage[]
  armed: boolean
  request?: MessageId
}

function sameAddress(left: MessageId, right: MessageId): boolean {
  return left.partitionId === right.partitionId && left.offset === right.offset
}

/** What a bounded reply lookup established, as Rust `ReplyLookup`.
 * `incomplete` means the scan bound ended before the reply topic did, which is
 * not proof that the task is still working. */
export type ReplyLookup =
  | { readonly kind: "found"; readonly envelope: AgentEnvelope }
  | { readonly kind: "notFound" }
  | { readonly kind: "incomplete" }

/** The first AGDX response or error carrying `correlation` on a reply topic,
 * read forward from the start of each partition. With a verifier only a reply
 * that verifies counts, so a forged answer cannot settle the lookup. */
export async function findAgdxReply(
  transport: LaserTransport,
  stream: string,
  topic: string,
  correlation: CorrelationId,
  verifier?: KeyRegistry
): Promise<ReplyLookup> {
  const partitions = await transport.findTopicPartitionCount(stream, topic)
  if (partitions === undefined) return { kind: "notFound" }
  // Each pass drains every partition up to 10,000 records, like Rust's
  // reply reader.
  const cursor = Cursor.create(
    transport,
    stream,
    topic,
    Array.from({ length: partitions }, (_, partition) => partition)
  ).batch(LOOKUP_BATCH)
  for (let pass = 0; pass < MAX_LOOKUP_PASSES; pass += 1) {
    const messages = await cursor.pollRecords()
    if (messages.length === 0) return { kind: "notFound" }
    for (const message of messages) {
      const envelope = matchingReply(decodeAgentMessage(message), correlation, verifier)
      if (envelope !== undefined) return { kind: "found", envelope }
    }
  }
  return { kind: "incomplete" }
}

export class ReplyHub {
  private readonly waiters = new Map<string, ReplyWaiter>()
  private readonly streamWaiters = new Map<string, StreamWaiter>()
  private stopped = false
  // Cancels the read the server holds when the hub stops.
  private readonly stopper = new AbortController()
  // Resolves the parked dispatcher. Set only while nobody waits, so an idle
  // hub does not poll the reply topic.
  private wake: (() => void) | undefined
  // Cancels the held read once the last waiter leaves, so an idle hub holds
  // no request on any node.
  private holding: AbortController | undefined

  private constructor(
    private readonly transport: LaserTransport,
    private readonly stream: string,
    private readonly topic: string,
    private readonly observer: LaserObserver,
    private readonly verifier?: KeyRegistry
  ) {}

  static async create(
    transport: LaserTransport,
    stream: string,
    topic: string,
    observer: LaserObserver = NOOP_OBSERVER,
    verifier?: KeyRegistry
  ): Promise<ReplyHub> {
    const hub = new ReplyHub(transport, stream, topic, observer, verifier)
    const offsets = await hub.seedTailOffsets()
    // An escaped rejection here would be fatal under Node and would silently
    // stop reply delivery, leaving every pending wait to expire on timeout.
    // Fail the outstanding waiters instead, so callers see the real cause.
    void hub.runDispatchLoop(offsets).catch((error: unknown) => {
      hub.abandon(error)
    })
    return hub
  }

  // The dispatch loop died, so no further reply can be delivered. Report it and
  // drop the registrations rather than letting an unhandled rejection take the
  // process down and leaving every waiter to expire on its timeout with no
  // explanation.
  private abandon(error: unknown): void {
    this.observer.event("error", "laser.reply_hub.stopped", {
      stream: this.stream,
      topic: this.topic,
      error: error instanceof Error ? error.message : String(error)
    })
    this.waiters.clear()
    this.streamWaiters.clear()
  }

  subscribe(
    correlation: string,
    expectedSigner?: string,
    deferUntilArmed = false,
    expected?: ExpectedReply
  ): ReplyTicket {
    let settle: ((message: AgentMessage) => void) | undefined
    let fail: ((error: Error) => void) | undefined
    const reply = new Promise<AgentMessage>((resolve, reject) => {
      settle = resolve
      fail = reject
    })
    // A hub that stops before the caller waits must not leave an unhandled rejection.
    reply.catch(() => undefined)
    const waiter: ReplyWaiter = {
      settle: settle as (message: AgentMessage) => void,
      fail: fail as (error: Error) => void,
      ...(expectedSigner !== undefined ? { expectedSigner } : {}),
      ...(expected !== undefined ? { expected } : {}),
      deferred: deferUntilArmed,
      pending: [],
      armed: !deferUntilArmed
    }
    this.waiters.set(correlation, waiter)
    this.wake?.()
    return {
      wait: (timeoutMs: number, signal?: AbortSignal) =>
        raceWithTimeout(reply, timeoutMs, () => this.release(correlation), signal),
      arm: (request?: MessageId) => {
        if (this.waiters.get(correlation) !== waiter) return
        waiter.armed = true
        if (request !== undefined) waiter.request = request
        const found = waiter.pending.find(
          (message) => request === undefined || !sameAddress(message.id, request)
        )
        waiter.pending.length = 0
        if (found !== undefined) {
          this.release(correlation)
          waiter.settle(found)
        }
      },
      cancel: () => this.release(correlation)
    }
  }

  subscribeStream(correlation: string, expectedSigner?: string): ReplyStreamTicket {
    const waiter: StreamWaiter = {
      queued: [],
      pending: [],
      ...(expectedSigner !== undefined ? { expectedSigner } : {})
    }
    this.streamWaiters.set(correlation, waiter)
    this.wake?.()
    return {
      next: (timeoutMs: number, signal?: AbortSignal) => {
        const queued = waiter.queued.shift()
        if (queued !== undefined) return Promise.resolve(queued)
        if (this.stopped) return Promise.reject(new TimeoutError("reply"))
        let settle: (message: AgentMessage) => void = () => undefined
        let fail: (error: Error) => void = () => undefined
        const reply = new Promise<AgentMessage>((resolve, reject) => {
          settle = resolve
          fail = reject
        })
        const pending: PendingReply = { settle, fail }
        waiter.pending.push(pending)
        return raceWithTimeout(
          reply,
          timeoutMs,
          () => {
            const index = waiter.pending.indexOf(pending)
            if (index !== -1) waiter.pending.splice(index, 1)
          },
          signal
        )
      },
      cancel: () => {
        const removed = this.streamWaiters.delete(correlation)
        this.releaseIfIdle()
        return removed
      }
    }
  }

  private release(correlation: string): boolean {
    const removed = this.waiters.delete(correlation)
    this.releaseIfIdle()
    return removed
  }

  private releaseIfIdle(): void {
    if (this.waiters.size === 0 && this.streamWaiters.size === 0) this.holding?.abort()
  }

  // Every wait ends at once with a reply timeout, as when Rust drops the hub.
  stop(): void {
    this.stopped = true
    this.stopper.abort()
    this.holding?.abort()
    this.wake?.()
    for (const waiter of this.waiters.values()) waiter.fail(new TimeoutError("reply"))
    this.waiters.clear()
    for (const waiter of this.streamWaiters.values()) {
      for (const pending of waiter.pending) pending.fail(new TimeoutError("reply"))
    }
    this.streamWaiters.clear()
  }

  private async seedTailOffsets(): Promise<bigint[]> {
    const partitionCount = await this.transport.findTopicPartitionCount(this.stream, this.topic)
    if (partitionCount === undefined) return []
    const offsets = new Array<bigint>(partitionCount).fill(0n)
    for (let partitionId = 0; partitionId < partitionCount; partitionId += 1) {
      const target: ConsumerTarget = { kind: "single", partitionId }
      const polled = await this.transport.pollMessages(
        this.stream,
        this.topic,
        target,
        { kind: "last" },
        1,
        false
      )
      const last = polled[polled.length - 1]
      if (last !== undefined) offsets[partitionId] = last.offset + 1n
    }
    return offsets
  }

  // Every pass reads each partition once and answers at once. After a pass
  // that found nothing, the server holds one fetch per partition primary
  // until a reply lands on any of them or the last waiter leaves, so a reply
  // arrives at once and an idle hub holds nothing.
  private async runDispatchLoop(initialOffsets: readonly bigint[]): Promise<void> {
    let offsets = [...initialOffsets]
    while (!this.stopped) {
      // Nobody waits: park until a subscribe or stop. The check and the
      // parking run without an await between them, so no wake is missed.
      if (this.waiters.size === 0 && this.streamWaiters.size === 0) {
        await new Promise<void>((resolve) => {
          this.wake = () => {
            this.wake = undefined
            resolve()
          }
        })
        continue
      }
      let partitionCount: number | undefined
      try {
        partitionCount = await this.transport.findTopicPartitionCount(this.stream, this.topic)
      } catch {
        await delay(REPLY_RETRY_INTERVAL_MS, this.stopper.signal)
        continue
      }
      if (partitionCount === undefined) {
        await delay(REPLY_RETRY_INTERVAL_MS, this.stopper.signal)
        continue
      }
      if (offsets.length < partitionCount) {
        offsets = [...offsets, ...new Array<bigint>(partitionCount - offsets.length).fill(0n)]
      }
      let failed = false
      let read = false
      for (let partitionId = 0; partitionId < partitionCount; partitionId += 1) {
        const outcome = await this.dispatchPartition(partitionId, offsets, ONE_SHOT_POLL)
        failed ||= outcome === "failed"
        read ||= outcome === "records"
      }
      if (failed) {
        await delay(REPLY_RETRY_INTERVAL_MS, this.stopper.signal)
        continue
      }
      if (read) continue
      // Checked with no await before the hold, so a waiter that left during
      // the pass, or a stop that cleared them, never leaves a read held.
      if (this.waiters.size === 0 && this.streamWaiters.size === 0) continue
      const holding = new AbortController()
      this.holding = holding
      let held: Awaited<ReturnType<typeof heldRead>>
      try {
        held = await heldRead(
          this.transport,
          this.stream,
          this.topic,
          new Map(offsets.map((offset, partitionId) => [partitionId, offset] as const)),
          REPLY_BATCH,
          REPLY_WAIT,
          holding.signal
        )
      } catch {
        if (!holding.signal.aborted) await delay(REPLY_RETRY_INTERVAL_MS, this.stopper.signal)
        continue
      } finally {
        if (this.holding === holding) this.holding = undefined
      }
      // An idle wait yields to timers even when the reads answer at once.
      if (held === undefined) {
        await delay(0, this.stopper.signal)
        continue
      }
      for (const message of held.messages) {
        offsets[held.partitionId] = message.offset + 1n
        this.dispatchMessage(held.partitionId, message)
      }
    }
  }

  private async dispatchPartition(
    partitionId: number,
    offsets: bigint[],
    wait: PollWait
  ): Promise<"records" | "empty" | "failed"> {
    const target: ConsumerTarget = { kind: "single", partitionId }
    const from = offsets[partitionId] ?? 0n
    let batch: readonly PolledMessage[]
    try {
      batch = await this.transport.pollMessages(
        this.stream,
        this.topic,
        target,
        { kind: "offset", value: from },
        REPLY_BATCH,
        false,
        wait,
        this.stopper.signal
      )
    } catch {
      return "failed"
    }
    for (const message of batch) {
      offsets[partitionId] = message.offset + 1n
      this.dispatchMessage(partitionId, message)
    }
    return batch.length > 0 ? "records" : "empty"
  }

  private dispatchMessage(partitionId: number, message: PolledMessage): boolean {
    const decoded = decodeAgentMessage({ ...message, partitionId })
    if (decoded.kind === "error") return false
    let reply = decoded.message
    if (reply.envelope?.kind === AgentKind.Command) return false
    const correlation = reply.provenance.correlationId
    if (correlation === undefined) return false
    const subscribed = this.waiters.get(correlation)
    const streamWaiter = this.streamWaiters.get(correlation)
    const expectedSigner = subscribed?.expectedSigner ?? streamWaiter?.expectedSigner
    if (this.verifier !== undefined) {
      if (
        reply.envelope === undefined ||
        decoded.signatureContext === undefined ||
        decoded.observedAtMicros === undefined
      ) {
        return false
      }
      try {
        const principal = this.verifier.verifyObservedAt(
          reply.envelope,
          decoded.signatureContext,
          decoded.observedAtMicros
        ).principal
        if (expectedSigner !== undefined && expectedSigner !== principal) return false
        reply = { ...reply, verifiedPrincipal: principal }
      } catch {
        return false
      }
    }
    // A request waiter takes only a reply to its own session, addressed to its
    // requester when the request named one.
    const waiter =
      subscribed?.expected === undefined || accepts(subscribed.expected, reply)
        ? subscribed
        : undefined
    if (waiter !== undefined) {
      if (waiter.deferred && !waiter.armed) {
        waiter.pending.push(reply)
        if (waiter.pending.length > MAX_PENDING_REQUEST_REPLIES) waiter.pending.shift()
      } else if (waiter.request === undefined || !sameAddress(reply.id, waiter.request)) {
        this.release(correlation)
        waiter.settle(reply)
      }
    }
    if (streamWaiter !== undefined) {
      const pending = streamWaiter.pending.shift()
      if (pending === undefined) {
        // Bounded: a flood on a subscribed correlation must not retain every
        // reply. The oldest is dropped, since a stream consumer wants the
        // newest.
        streamWaiter.queued.push(reply)
        if (streamWaiter.queued.length > MAX_QUEUED_REPLIES) streamWaiter.queued.shift()
      } else pending.settle(reply)
    }
    return waiter !== undefined || streamWaiter !== undefined
  }
}

function matchingReply(
  decoded: DecodedAgentMessage,
  correlation: CorrelationId,
  verifier: KeyRegistry | undefined
): AgentEnvelope | undefined {
  if (decoded.kind === "error") return undefined
  const envelope = decoded.message.envelope
  if (
    envelope?.correlation?.equals(correlation) !== true ||
    (envelope.kind !== AgentKind.Response && envelope.kind !== AgentKind.Error)
  ) {
    return undefined
  }
  if (verifier === undefined) return envelope
  if (decoded.signatureContext === undefined || decoded.observedAtMicros === undefined) {
    return undefined
  }
  try {
    verifier.verifyObservedAt(envelope, decoded.signatureContext, decoded.observedAtMicros)
  } catch {
    return undefined
  }
  return envelope
}

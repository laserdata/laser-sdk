import { CancelledError, TimeoutError } from "../client/errors.js"
import type { ConsumerTarget, LaserTransport, PolledMessage } from "../iggy/apache-iggy.js"
import { NOOP_OBSERVER, type LaserObserver } from "../observe.js"
import type { KeyRegistry } from "../signing.js"
import { Cursor } from "../stream/cursor.js"
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
const REPLY_POLL_INTERVAL_MS = 20

// A point lookup reads at most this many batches per partition, like Rust.
const MAX_LOOKUP_PASSES = 32
const LOOKUP_BATCH = 1_000

// Ceiling on replies buffered for one subscribed correlation with no consumer
// pulling them. A flood must not be retained in full.
const MAX_QUEUED_REPLIES = 1_000
const MAX_PENDING_REQUEST_REPLIES = 64

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
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

interface StreamWaiter {
  readonly queued: AgentMessage[]
  readonly pending: ((message: AgentMessage) => void)[]
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

interface ReplyWaiter {
  readonly settle: (message: AgentMessage) => void
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

/** The first AGDX response or error carrying `correlation` on a reply topic,
 * read forward from the start of each partition. With a verifier only a reply
 * that verifies counts, so a forged answer cannot settle the lookup. */
export async function findAgdxReply(
  transport: LaserTransport,
  stream: string,
  topic: string,
  correlation: CorrelationId,
  verifier?: KeyRegistry
): Promise<AgentEnvelope | undefined> {
  const partitions = await transport.findTopicPartitionCount(stream, topic)
  if (partitions === undefined) return undefined
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
    if (messages.length === 0) return undefined
    for (const message of messages) {
      const envelope = matchingReply(decodeAgentMessage(message), correlation, verifier)
      if (envelope !== undefined) return envelope
    }
  }
  return undefined
}

export class ReplyHub {
  private readonly waiters = new Map<string, ReplyWaiter>()
  private readonly streamWaiters = new Map<string, StreamWaiter>()
  private stopped = false

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
    const reply = new Promise<AgentMessage>((resolve) => {
      settle = resolve
    })
    const waiter: ReplyWaiter = {
      settle: settle as (message: AgentMessage) => void,
      ...(expectedSigner !== undefined ? { expectedSigner } : {}),
      ...(expected !== undefined ? { expected } : {}),
      deferred: deferUntilArmed,
      pending: [],
      armed: !deferUntilArmed
    }
    this.waiters.set(correlation, waiter)
    return {
      wait: (timeoutMs: number, signal?: AbortSignal) =>
        raceWithTimeout(reply, timeoutMs, () => this.waiters.delete(correlation), signal),
      arm: (request?: MessageId) => {
        if (this.waiters.get(correlation) !== waiter) return
        waiter.armed = true
        if (request !== undefined) waiter.request = request
        const found = waiter.pending.find(
          (message) => request === undefined || !sameAddress(message.id, request)
        )
        waiter.pending.length = 0
        if (found !== undefined) {
          this.waiters.delete(correlation)
          waiter.settle(found)
        }
      },
      cancel: () => this.waiters.delete(correlation)
    }
  }

  subscribeStream(correlation: string, expectedSigner?: string): ReplyStreamTicket {
    const waiter: StreamWaiter = {
      queued: [],
      pending: [],
      ...(expectedSigner !== undefined ? { expectedSigner } : {})
    }
    this.streamWaiters.set(correlation, waiter)
    return {
      next: (timeoutMs: number, signal?: AbortSignal) => {
        const queued = waiter.queued.shift()
        if (queued !== undefined) return Promise.resolve(queued)
        let settle: ((message: AgentMessage) => void) | undefined
        const reply = new Promise<AgentMessage>((resolve) => {
          settle = resolve
        })
        const pending = settle as (message: AgentMessage) => void
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
      cancel: () => this.streamWaiters.delete(correlation)
    }
  }

  stop(): void {
    this.stopped = true
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

  private async runDispatchLoop(initialOffsets: readonly bigint[]): Promise<void> {
    let offsets = [...initialOffsets]
    while (!this.stopped) {
      let partitionCount: number | undefined
      try {
        partitionCount = await this.transport.findTopicPartitionCount(this.stream, this.topic)
      } catch {
        await delay(REPLY_POLL_INTERVAL_MS)
        continue
      }
      if (partitionCount === undefined) {
        await delay(REPLY_POLL_INTERVAL_MS)
        continue
      }
      if (offsets.length < partitionCount) {
        offsets = [...offsets, ...new Array<bigint>(partitionCount - offsets.length).fill(0n)]
      }
      let dispatched = false
      for (let partitionId = 0; partitionId < partitionCount; partitionId += 1) {
        dispatched = (await this.dispatchPartition(partitionId, offsets)) || dispatched
      }
      if (!dispatched) await delay(REPLY_POLL_INTERVAL_MS)
    }
  }

  private async dispatchPartition(partitionId: number, offsets: bigint[]): Promise<boolean> {
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
        false
      )
    } catch {
      return false
    }
    let dispatched = false
    for (const message of batch) {
      offsets[partitionId] = message.offset + 1n
      if (this.dispatchMessage(partitionId, message)) dispatched = true
    }
    return dispatched
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
        this.waiters.delete(correlation)
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
      } else pending(reply)
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

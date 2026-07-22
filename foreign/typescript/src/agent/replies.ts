import { CancelledError, TimeoutError } from "../client/errors.js"
import type { ConsumerTarget, LaserTransport, PolledMessage } from "../iggy/apache-iggy.js"
import { decodeAgentMessage, type AgentMessage } from "./reliable-consumer.js"

const REPLY_BATCH = 200
const REPLY_POLL_INTERVAL_MS = 20

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
      clearTimeout(timer)
      onDone()
      reject(new CancelledError("reply wait aborted", { cause: signal?.reason }))
    }
    const timer = setTimeout(() => {
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
        clearTimeout(timer)
        signal?.removeEventListener("abort", abort)
        resolve(value)
      })
      .catch((error: unknown) => {
        clearTimeout(timer)
        signal?.removeEventListener("abort", abort)
        reject(error instanceof Error ? error : new Error(String(error)))
      })
  })
}

// A registered wait for one correlated reply, returned by
// `ReplyHub.subscribe`. `.wait()` awaits it up to a timeout, then
// deregisters.
export interface ReplyTicket {
  wait(timeoutMs: number, signal?: AbortSignal): Promise<AgentMessage>
  cancel(): void
}

export interface ReplyStreamTicket {
  next(timeoutMs: number, signal?: AbortSignal): Promise<AgentMessage>
  cancel(): void
}

interface StreamWaiter {
  readonly queued: AgentMessage[]
  readonly pending: ((message: AgentMessage) => void)[]
}

// One shared reply consumer per `(stream, reply topic)`. It decodes each
// record once and completes the one waiter whose correlation matches, so
// N concurrent requests on the same reply topic read the topic once
// between them instead of each running its own tail-seeded scan. Driven
// by a background poll loop started at `.create()` and stopped by
// `.stop()`.
//
// Node has no `Drop`: an abandoned ticket whose `.wait()` is never called
// leaks its waiter entry until `.stop()` tears the whole hub down (Rust's
// `Drop for ReplyTicket` deregisters even an un-awaited ticket
// deterministically). Every real caller does call `.wait()` immediately
// after subscribing, so this is a documented edge case, not a fix
// invented beyond what a callback-free host language can guarantee.
export class ReplyHub {
  private readonly waiters = new Map<string, (message: AgentMessage) => void>()
  private readonly streamWaiters = new Map<string, StreamWaiter>()
  private stopped = false

  private constructor(
    private readonly transport: LaserTransport,
    private readonly stream: string,
    private readonly topic: string
  ) {}

  // Create the hub and start its dispatcher, seeded at the reply topic's
  // tail so it reads only records appended after creation. A waiter must
  // `.subscribe()` before sending its request (registration is
  // synchronous), so a reply, which cannot exist until the request is
  // sent, is dispatched rather than missed.
  static async create(transport: LaserTransport, stream: string, topic: string): Promise<ReplyHub> {
    const hub = new ReplyHub(transport, stream, topic)
    const offsets = await hub.seedTailOffsets()
    void hub.runDispatchLoop(offsets)
    return hub
  }

  // Register a waiter for `correlation`, returning a ticket. Registration
  // happens synchronously here (not inside `.wait()`), so a caller that
  // subscribes before sending its request never races an early reply.
  subscribe(correlation: string): ReplyTicket {
    let settle: ((message: AgentMessage) => void) | undefined
    const reply = new Promise<AgentMessage>((resolve) => {
      settle = resolve
    })
    this.waiters.set(correlation, settle as (message: AgentMessage) => void)
    return {
      wait: (timeoutMs: number, signal?: AbortSignal) =>
        raceWithTimeout(reply, timeoutMs, () => this.waiters.delete(correlation), signal),
      cancel: () => this.waiters.delete(correlation)
    }
  }

  subscribeStream(correlation: string): ReplyStreamTicket {
    const waiter: StreamWaiter = { queued: [], pending: [] }
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

  // Stop the dispatch loop. Any outstanding ticket then times out rather
  // than hanging forever.
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

  // Poll the reply topic forward and dispatch each record to its waiter.
  // The reply topic may not exist at hub creation (a fan-out reply topic
  // is created by the first reply), so partitions are resolved every
  // pass until it does.
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
    const reply = decoded.message
    const correlation = reply.provenance.correlationId
    if (correlation === undefined) return false
    const waiter = this.waiters.get(correlation)
    if (waiter !== undefined) {
      this.waiters.delete(correlation)
      waiter(reply)
    }
    const streamWaiter = this.streamWaiters.get(correlation)
    if (streamWaiter !== undefined) {
      const pending = streamWaiter.pending.shift()
      if (pending === undefined) streamWaiter.queued.push(reply)
      else pending(reply)
    }
    return waiter !== undefined || streamWaiter !== undefined
  }
}

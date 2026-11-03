import { SessionError, UnsupportedError } from "../client/errors.js"
import { INTERNAL_TRANSPORT } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { mintUlidValue } from "../runtime/ulid.js"
import { type AgentId, ConversationId } from "../types/ids.js"
import { OPERATION_PROGRESS } from "../wire/agent.js"
import { encodeNamed } from "../wire/cbor.js"
import { ContentType } from "../wire/content.js"
import { ConversationId as WireConversationId, crockfordEncode } from "../wire/ids.js"
import { MAX_HEARTBEAT_SESSIONS } from "../wire/limits.js"
import { encodeSessionHeartbeat } from "../wire/session.js"

/** Ownership of one session's liveness. While any lease on a session is held,
 * the process lists the session in its heartbeat on `agent.heartbeats`. Call
 * `release` when the process stops working on the session. A terminal verb
 * on the session does not release leases held elsewhere in the process. */
export class SessionLease {
  private released = false

  private constructor(
    private readonly registry: LeaseRegistry,
    private readonly key: LeaseKey
  ) {}

  /** @internal */
  static create(registry: LeaseRegistry, key: LeaseKey): SessionLease {
    return new SessionLease(registry, key)
  }

  /** The leased session. */
  get session(): ConversationId {
    return this.key.session
  }

  /** The stream the leased session lives in. */
  get stream(): string {
    return this.key.stream
  }

  /** Releases the lease now. Releasing twice is a no-op. */
  release(): void {
    if (this.released) return
    this.released = true
    this.registry.release(this.key)
  }

  [Symbol.dispose](): void {
    this.release()
  }
}

// One lease identity: the stream by name and creation time, and the session.
// A recreated stream is a different lease scope.
/** @internal */
export interface LeaseKey {
  readonly stream: string
  readonly streamGeneration: bigint
  readonly session: ConversationId
}

interface LeaseEntry {
  holders: number
  readonly agent: AgentId
  intervalMs: number
  readonly key: LeaseKey
}

function keyText(key: LeaseKey): string {
  return `${key.stream}\u0000${key.streamGeneration.toString()}\u0000${key.session.toString()}`
}

/** The per-connection lease map the heartbeat timer reads every tick.
 * @internal */
export class LeaseRegistry {
  readonly process = crockfordEncode(mintUlidValue())
  private readonly leases = new Map<string, LeaseEntry>()
  private timer: ReturnType<typeof setTimeout> | undefined
  private running = false
  private closed = false

  /** Takes a lease on `key` for `agent`, starting the heartbeat timer when it
   * is not already running. */
  acquire(laser: Laser, key: LeaseKey, agent: AgentId, intervalMs: number): SessionLease {
    const text = keyText(key)
    const entry = this.leases.get(text)
    if (entry === undefined) {
      this.leases.set(text, { holders: 1, agent, intervalMs, key })
    } else {
      entry.holders += 1
      entry.intervalMs = Math.min(entry.intervalMs, intervalMs)
    }
    if (!this.running && !this.closed) {
      this.running = true
      this.schedule(laser)
    }
    return SessionLease.create(this, key)
  }

  /** @internal */
  release(key: LeaseKey): void {
    const text = keyText(key)
    const entry = this.leases.get(text)
    if (entry === undefined) return
    entry.holders -= 1
    if (entry.holders === 0) this.leases.delete(text)
  }

  /** Stops the heartbeat timer and forgets every lease. */
  close(): void {
    this.closed = true
    this.running = false
    clearTimeout(this.timer)
    this.timer = undefined
    this.leases.clear()
  }

  // One heartbeat per stream per tick, at the shortest interval among the held
  // leases. The timer stops when the last lease is released, so a process that
  // holds no lease publishes nothing. A failed publish is retried on the next
  // tick and never fails a session. The timer never keeps Node alive.
  private schedule(laser: Laser): void {
    const interval = this.interval()
    if (interval === undefined || this.closed) {
      this.running = false
      return
    }
    this.timer = setTimeout(() => {
      void this.tick(laser)
    }, interval)
    this.timer.unref()
  }

  private async tick(laser: Laser): Promise<void> {
    for (const [stream, generation, agent, sessions] of this.snapshot()) {
      try {
        await this.beat(laser, stream, generation, agent, sessions)
      } catch {
        // Retried on the next tick.
      }
    }
    this.schedule(laser)
  }

  private interval(): number | undefined {
    let interval: number | undefined
    for (const entry of this.leases.values()) {
      interval = interval === undefined ? entry.intervalMs : Math.min(interval, entry.intervalMs)
    }
    return interval
  }

  // The held sessions grouped by stream, so one stream's ids never ride
  // another stream's heartbeat.
  private snapshot(): readonly (readonly [string, bigint, AgentId, readonly ConversationId[]])[] {
    const groups = new Map<
      string,
      { stream: string; generation: bigint; agent: AgentId; sessions: Map<string, ConversationId> }
    >()
    for (const entry of this.leases.values()) {
      const identity = `${entry.key.stream}/${String(entry.key.streamGeneration)}`
      let group = groups.get(identity)
      if (group === undefined) {
        group = {
          stream: entry.key.stream,
          generation: entry.key.streamGeneration,
          agent: entry.agent,
          sessions: new Map()
        }
        groups.set(identity, group)
      }
      group.sessions.set(entry.key.session.toString(), entry.key.session)
    }
    return [...groups]
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(
        ([, group]) =>
          [
            group.stream,
            group.generation,
            group.agent,
            [...group.sessions]
              .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
              .map(([, session]) => session)
          ] as const
      )
  }

  private async beat(
    laser: Laser,
    stream: string,
    generation: bigint,
    agent: AgentId,
    sessions: readonly ConversationId[]
  ): Promise<void> {
    const details = await laser[INTERNAL_TRANSPORT]().findSnapshotStream?.(stream)
    if (details?.createdAtMicros !== generation) return
    const conversation = ConversationId.derive(`heartbeat/${this.process}`)
    const producer = laser
      .withDefaultStream(stream)
      .agdx(AgentTopic.Heartbeats, agent, conversation)
      .withLaneGuard(async () => {
        const transport = laser[INTERNAL_TRANSPORT]()
        if (transport.findSnapshotStream === undefined || transport.findSnapshotTopic === undefined)
          throw new UnsupportedError("the heartbeat source identity is unavailable")
        const details = await transport.findSnapshotStream(stream)
        const topic = await transport.findSnapshotTopic(stream, AgentTopic.Heartbeats)
        if (details?.createdAtMicros !== generation)
          throw new SessionError(
            "stale session generation: the heartbeat stream generation changed",
            { kind: "stale", message: "the heartbeat stream generation changed" }
          )
        if (topic === undefined || topic.partitions === 0)
          throw new SessionError("stale session generation: the heartbeat topic does not exist", {
            kind: "stale",
            message: "the heartbeat topic does not exist"
          })
        return { streamId: details.id, topicId: topic.id, partitions: topic.partitions }
      })
    for (let start = 0; start < sessions.length; start += MAX_HEARTBEAT_SESSIONS) {
      const body = encodeNamed(
        encodeSessionHeartbeat({
          process: this.process,
          stream,
          sessions: sessions
            .slice(start, start + MAX_HEARTBEAT_SESSIONS)
            .map((session) => WireConversationId.parse(session.toString()))
        })
      )
      await producer.status(OPERATION_PROGRESS).body(body).contentType(ContentType.Cbor).send()
    }
  }
}

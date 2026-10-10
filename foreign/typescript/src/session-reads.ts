import { millis } from "./client/duration.js"
import { ProtocolError, SessionError } from "./client/errors.js"
import { INTERNAL_TRANSPORT } from "./client/internals.js"
import { executeManaged } from "./client/managed.js"
import type { Laser } from "./client/laser.js"
import type { AgentId, SessionStatus } from "./wire/agent.js"
import {
  type ManagedCommand,
  SessionChangesCommand,
  SessionEventsCommand,
  SessionListCommand
} from "./wire/commands.js"
import { ConversationId as WireConversationId } from "./wire/ids.js"
import type { ConversationId } from "./types/ids.js"
import {
  type SessionChangesView,
  type SessionEventsPage,
  type SessionOutcome,
  type SessionPage,
  type SessionReply,
  sessionErrorMessage
} from "./wire/session.js"

/** Send one session read and return its outcome. Refused with
 * `UnsupportedError` when the deployment does not index sessions.
 * @internal */
export async function readSession<Request>(
  laser: Laser,
  command: ManagedCommand<Request, SessionReply>,
  request: Request
): Promise<SessionOutcome> {
  const reply = await executeManaged(
    laser[INTERNAL_TRANSPORT](),
    await laser.capabilities(),
    command,
    request
  )
  switch (reply.kind) {
    case "ok":
      return reply.outcome
    case "err":
      throw new SessionError(`session: ${sessionErrorMessage(reply.error)}`, reply.error)
    case "unrecognized":
      throw unexpected("reply")
  }
}

/** @internal */
export function unexpected(read: string): ProtocolError {
  return new ProtocolError(`session ${read}: unexpected reply shape`)
}

/** A session list read. Build it with `Sessions.list`. */
export class SessionListRequest {
  private statusValue: SessionStatus | undefined
  private rootValue: WireConversationId | undefined
  private labelPrefixValue: string | undefined
  private agentValue: AgentId | undefined
  private textValue: string | undefined
  private cursorValue: string | undefined
  private limitValue = 0
  private totalValue = false

  private constructor(
    private readonly laser: Laser,
    private readonly stream: string
  ) {}

  /** @internal */
  static create(laser: Laser, stream: string): SessionListRequest {
    return new SessionListRequest(laser, stream)
  }

  /** Only sessions in `status`. */
  status(status: SessionStatus): this {
    this.statusValue = status
    return this
  }

  /** Only sessions in the tree rooted at `root`. */
  root(root: ConversationId): this {
    this.rootValue = WireConversationId.parse(root.toString())
    return this
  }

  /** Only sessions whose label starts with `prefix`. */
  labelPrefix(prefix: string): this {
    this.labelPrefixValue = prefix
    return this
  }

  /** Only sessions `agent` took part in. */
  agent(agent: AgentId): this {
    this.agentValue = agent
    return this
  }

  /** Only sessions whose label or id contains `text`. */
  text(text: string): this {
    this.textValue = text
    return this
  }

  /** Continue after the page that returned `cursor`. */
  cursor(cursor: string): this {
    this.cursorValue = cursor
    return this
  }

  /** The page size. Zero leaves it to the server. */
  limit(limit: number): this {
    this.limitValue = limit
    return this
  }

  /** Also count every match. */
  total(): this {
    this.totalValue = true
    return this
  }

  /** Read the page. */
  async fetch(): Promise<SessionPage> {
    const outcome = await readSession(this.laser, SessionListCommand, {
      stream: this.stream,
      ...(this.statusValue !== undefined ? { status: this.statusValue } : {}),
      ...(this.rootValue !== undefined ? { root: this.rootValue } : {}),
      ...(this.labelPrefixValue !== undefined ? { labelPrefix: this.labelPrefixValue } : {}),
      ...(this.agentValue !== undefined ? { agent: this.agentValue } : {}),
      ...(this.textValue !== undefined ? { text: this.textValue } : {}),
      ...(this.cursorValue !== undefined ? { cursor: this.cursorValue } : {}),
      limit: this.limitValue,
      wantTotal: this.totalValue
    })
    if (outcome.kind !== "page") throw unexpected("list")
    return outcome.page
  }
}

/** A session events read. Build it with `Sessions.events`. */
export class SessionEventsRequest {
  private cursorValue: string | undefined
  private limitValue = 0
  private fixedFrontierValue = false

  private constructor(
    private readonly laser: Laser,
    private readonly stream: string,
    private readonly id: WireConversationId
  ) {}

  /** @internal */
  static create(laser: Laser, stream: string, id: WireConversationId): SessionEventsRequest {
    return new SessionEventsRequest(laser, stream, id)
  }

  /** Continue after the page that returned `cursor`. */
  cursor(cursor: string): this {
    this.cursorValue = cursor
    return this
  }

  /** The page size. Zero leaves it to the server. */
  limit(limit: number): this {
    this.limitValue = limit
    return this
  }

  /** Pin the fold frontier of the first page, for a historical walk. */
  fixedFrontier(): this {
    this.fixedFrontierValue = true
    return this
  }

  /** Read the page. */
  async fetch(): Promise<SessionEventsPage> {
    const outcome = await readSession(this.laser, SessionEventsCommand, {
      stream: this.stream,
      id: this.id,
      ...(this.cursorValue !== undefined ? { cursor: this.cursorValue } : {}),
      limit: this.limitValue,
      fixedFrontier: this.fixedFrontierValue
    })
    if (outcome.kind !== "events") throw unexpected("events")
    return outcome.page
  }
}

/** What a `SessionWatch` reports: the sessions that changed, each named once,
 * or a resync when the watch fell below the retained change floor and the
 * sessions should be listed again. */
export type SessionChange =
  | { readonly kind: "changed"; readonly sessions: readonly WireConversationId[] }
  | { readonly kind: "resync" }

/** A follower of one stream's session changes. Build it with `Sessions.watch`. */
export class SessionWatch {
  private readonly ready: SessionChange[] = []

  private constructor(
    private readonly laser: Laser,
    private readonly stream: string,
    private readonly pollEveryMs: number,
    private after: bigint
  ) {}

  /** A watch anchored at the change rows retained when it returns, so a
   * change made after it is never missed.
   * @internal */
  static async create(laser: Laser, stream: string, pollEveryMs: number): Promise<SessionWatch> {
    millis(pollEveryMs, "session watch pollEveryMs")
    const watch = new SessionWatch(laser, stream, pollEveryMs, 0n)
    for (;;) {
      const page = await watch.changes(watch.after)
      const seq = page.rows.reduce<bigint | undefined>(
        (max, row) => (max === undefined || row.seq > max ? row.seq : max),
        undefined
      )
      if (seq === undefined) return watch
      watch.after = seq
    }
  }

  /** The next change, waiting until one lands. Aborting `signal` stops the
   * wait. */
  async next(options: { readonly signal?: AbortSignal } = {}): Promise<SessionChange> {
    for (;;) {
      options.signal?.throwIfAborted()
      const ready = this.ready.shift()
      if (ready !== undefined) return ready
      const page = await this.changes(this.after)
      if (page.resync) {
        this.after = page.floor
        return { kind: "resync" }
      }
      if (page.rows.length === 0) {
        await pause(this.pollEveryMs, options.signal)
        continue
      }
      const seen = new Set<string>()
      const changed: WireConversationId[] = []
      for (const row of page.rows) {
        if (row.seq > this.after) this.after = row.seq
        for (const session of row.sessions) {
          if (!seen.has(session.toString())) {
            seen.add(session.toString())
            changed.push(session)
          }
        }
      }
      this.ready.push({ kind: "changed", sessions: changed })
    }
  }

  private async changes(after: bigint): Promise<SessionChangesView> {
    const outcome = await readSession(this.laser, SessionChangesCommand, {
      stream: this.stream,
      after,
      limit: 0
    })
    if (outcome.kind !== "changes") throw unexpected("changes")
    return outcome.changes
  }
}

function pause(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", abort)
      resolve()
    }, ms)
    const abort = (): void => {
      clearTimeout(timer)
      reject(signal?.reason as Error)
    }
    signal?.addEventListener("abort", abort, { once: true })
  })
}

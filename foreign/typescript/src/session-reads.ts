import { type CallOptions, abortable, delay, throwIfAborted } from "./client/call-options.js"
import { millis } from "./client/duration.js"
import { InvalidError, ProtocolError, SessionError } from "./client/errors.js"
import { INTERNAL_TRANSPORT } from "./client/internals.js"
import { code } from "./client/error-classify.js"
import { executeManaged } from "./client/managed.js"
import type { Laser } from "./client/laser.js"
import type { ManagedSendOptions } from "./iggy/apache-iggy.js"
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
  request: Request,
  options?: ManagedSendOptions & CallOptions
): Promise<SessionOutcome> {
  throwIfAborted(options?.signal, "session read")
  const reply = await executeManaged(
    laser[INTERNAL_TRANSPORT](),
    await abortable(laser.capabilities(), options?.signal, "session read"),
    command,
    request,
    options
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
  private pageValue = 0
  private resultsValue = 0
  private bound: { readonly asOf: bigint; readonly series: bigint } | undefined

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

  /** The 1-based page to read. Zero reads the first page. */
  page(page: number): this {
    this.pageValue = page
    return this
  }

  /** The sessions per page. Zero leaves it to the server. */
  results(results: number): this {
    this.resultsValue = results
    return this
  }

  /** Read within the bound of a walk: the `asOf` and `series` its first page
   * returned, so sessions folded after it stay out of every page. */
  asOf(asOf: bigint, series: bigint): this {
    this.bound = { asOf, series }
    return this
  }

  /** Read the page. */
  async fetch(options: CallOptions = {}): Promise<SessionPage> {
    const outcome = await readSession(
      this.laser,
      SessionListCommand,
      {
        stream: this.stream,
        ...(this.statusValue !== undefined ? { status: this.statusValue } : {}),
        ...(this.rootValue !== undefined ? { root: this.rootValue } : {}),
        ...(this.labelPrefixValue !== undefined ? { labelPrefix: this.labelPrefixValue } : {}),
        ...(this.agentValue !== undefined ? { agent: this.agentValue } : {}),
        ...(this.textValue !== undefined ? { text: this.textValue } : {}),
        page: this.pageValue,
        results: this.resultsValue,
        ...(this.bound ?? {})
      },
      options
    )
    if (outcome.kind !== "page") throw unexpected("list")
    return outcome.page
  }
}

/** A session events read. Build it with `Sessions.events`. */
export class SessionEventsRequest {
  private pageValue = 0
  private resultsValue = 0
  private bound: { readonly asOf: bigint; readonly series: bigint } | undefined

  private constructor(
    private readonly laser: Laser,
    private readonly stream: string,
    private readonly id: WireConversationId
  ) {}

  /** @internal */
  static create(laser: Laser, stream: string, id: WireConversationId): SessionEventsRequest {
    return new SessionEventsRequest(laser, stream, id)
  }

  /** The 1-based page to read. Zero reads the first page. */
  page(page: number): this {
    this.pageValue = page
    return this
  }

  /** The events per page. Zero leaves it to the server. */
  results(results: number): this {
    this.resultsValue = results
    return this
  }

  /** Read within the bound of a walk: the `asOf` and `series` its first page
   * returned, so events folded after it stay out of every page. */
  asOf(asOf: bigint, series: bigint): this {
    this.bound = { asOf, series }
    return this
  }

  /** Read the page. */
  async fetch(options: CallOptions = {}): Promise<SessionEventsPage> {
    const outcome = await readSession(
      this.laser,
      SessionEventsCommand,
      {
        stream: this.stream,
        id: this.id,
        page: this.pageValue,
        results: this.resultsValue,
        ...(this.bound ?? {})
      },
      options
    )
    if (outcome.kind !== "events") throw unexpected("events")
    return outcome.page
  }
}

/** What a `SessionWatch` reports: the sessions that changed, each named once,
 * or a resync when the retained feed cannot identify every changed session.
 * List the sessions again after a resync. */
export type SessionChange =
  | { readonly kind: "changed"; readonly sessions: readonly WireConversationId[] }
  | { readonly kind: "resync" }

/** A follower of one stream's session changes. Build it with `Sessions.watch`.
 * `for await (const change of watch)` yields each change as it lands and
 * follows changes until the loop stops. */
export class SessionWatch implements AsyncIterable<SessionChange> {
  private readonly ready: SessionChange[] = []

  private constructor(
    private readonly laser: Laser,
    private readonly stream: string,
    private readonly pollEveryMs: number,
    // The next change sequence to report. The page to read follows from it.
    private nextSequence: bigint,
    private results: number,
    // Sequence numbers of another node's or a restored index never continue
    // this watch.
    private series: bigint
  ) {}

  /** A watch anchored at the newest change when it returns, so a change made
   * after it is never missed. The start is one read: any page names the
   * series, the page size and the newest sequence.
   * @internal */
  static async create(
    laser: Laser,
    stream: string,
    pollEveryMs: number,
    options: CallOptions = {}
  ): Promise<SessionWatch> {
    if (millis(pollEveryMs, "session watch pollEveryMs") === 0) {
      throw new InvalidError("session watch pollEveryMs must be above zero")
    }
    const start = await readChanges(laser, stream, 1, 0, options)
    return new SessionWatch(
      laser,
      stream,
      pollEveryMs,
      start.totalResults + 1n,
      start.results,
      start.series
    )
  }

  async *[Symbol.asyncIterator](): AsyncIterator<SessionChange> {
    for (;;) yield await this.next()
  }

  /** The next change, waiting until one lands. Aborting `signal` stops the
   * wait. */
  async next(options: CallOptions = {}): Promise<SessionChange> {
    for (;;) {
      throwIfAborted(options.signal, "session watch")
      const ready = this.ready.shift()
      if (ready !== undefined) return ready
      const size = BigInt(Math.max(this.results, 1))
      let page: SessionChangesView
      try {
        page = await readChanges(
          this.laser,
          this.stream,
          Number((this.nextSequence - 1n) / size + 1n),
          this.results,
          options
        )
      } catch (error) {
        // A node that has not folded this stream far enough yet, typically
        // the one a move just reached, answers Stale. The watch waits for it
        // as it waits for a new change.
        if (!isStale(error)) throw error
        await delay(this.pollEveryMs, options.signal, "session watch")
        continue
      }
      this.results = page.results
      // Another series numbers another index's changes, and a floor past the
      // next sequence means retention pruned changes not reported.
      if (page.series !== this.series || page.floor > this.nextSequence) {
        this.series = page.series
        this.nextSequence = page.totalResults + 1n
        return { kind: "resync" }
      }
      const rows = page.items.filter((row) => row.seq >= this.nextSequence)
      const last = rows.at(-1)
      if (last === undefined) {
        await delay(this.pollEveryMs, options.signal, "session watch")
        continue
      }
      this.nextSequence = last.seq + 1n
      // A truncated row does not name every changed session. The caller lists
      // again after the resync, which covers these rows.
      if (rows.some((row) => row.truncated)) return { kind: "resync" }
      const seen = new Set<string>()
      const changed: WireConversationId[] = []
      for (const row of rows) {
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
}

async function readChanges(
  laser: Laser,
  stream: string,
  page: number,
  results: number,
  options: CallOptions
): Promise<SessionChangesView> {
  const outcome = await readSession(
    laser,
    SessionChangesCommand,
    { stream, page, results },
    options
  )
  if (outcome.kind !== "changes") throw unexpected("changes")
  return outcome.changes
}

function isStale(error: unknown): boolean {
  const result = code(error)
  return result.kind === "known" && result.name === "Stale"
}

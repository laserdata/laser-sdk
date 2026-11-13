import { type BytesLike, ownedBytes } from "./client/bytes.js"
import { type CallOptions, abortable, cancellable, throwIfAborted } from "./client/call-options.js"
import { InvalidError, SessionError } from "./client/errors.js"
import type { AgdxReceipt } from "./agent/agdx.js"
import { type ContextMessage, contextMessageBody } from "./context.js"
import { warn } from "./runtime/warn.js"
import type { Session } from "./session.js"
import {
  type AgentErrorBody,
  type ContextManifest,
  OPERATION_CHAT,
  OPERATION_STATE_DELTA,
  OPERATION_STATE_SNAPSHOT,
  type PatchOp,
  type TokenUsage,
  decodeStateDelta,
  decodeStateSnapshot,
  encodeStateDelta,
  encodeStateSnapshot
} from "./wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "./wire/cbor.js"
import { OPERATION_EXECUTE_TOOL } from "./wire/dispatch.js"
import type { CorrelationId } from "./wire/ids.js"
import { applyJsonPatch, equalJson } from "./wire/json-patch.js"
import type { SessionStateView } from "./wire/session.js"

// Keys whose values the default redaction drops from tool arguments and model
// request bodies before they are published.
const REDACTED_KEYS: ReadonlySet<string> = new Set([
  "authorization",
  "api_key",
  "token",
  "password",
  "secret",
  "cookie"
])

// Automatic state snapshots are written after this many deltas.
const SNAPSHOT_EVERY_DELTAS = 64

/** Drop the values of keys such as `authorization`, `api_key`, `token`,
 * `password`, `secret`, and `cookie`, at any depth. A convenience, not a
 * guarantee: a secret under another key is published as it is. Returns the
 * redacted copy. */
export function defaultRedact(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(defaultRedact)
  if (typeof value !== "object" || value === null) return value
  return Object.fromEntries(
    Object.entries(value).map(([key, field]) => [
      key,
      REDACTED_KEYS.has(key.toLowerCase()) ? "[redacted]" : defaultRedact(field)
    ])
  )
}

// The state one session handle has written: the revision it expects next, the
// document after its own deltas, how many deltas since the last snapshot, and
// the write in flight, which the next write waits for.
/** @internal */
export interface StateCursor {
  revision: bigint
  document: unknown
  sinceSnapshot: number
  tail: Promise<void>
}

/** One model call request. The SDK never calls a model: the application calls
 * its provider and records the call through the session. */
export class ModelRequest {
  /** A request to `model` with `body` as the prompt, served by `provider`
   * when given, as a `chat` operation unless `operation` names another, such
   * as `text_completion`. */
  constructor(
    readonly model: string,
    readonly body: Uint8Array,
    readonly provider?: string,
    readonly operation: string = OPERATION_CHAT
  ) {}
}

/** One model call result. */
export interface ModelResponse {
  readonly body: Uint8Array
  /** The model that answered. */
  readonly model?: string
  readonly finishReason?: string
  readonly usage?: TokenUsage
  /** The call's duration in milliseconds, when the application measured it itself.
   * Left unset, `recordModelCall` records a duration of 0. */
  readonly durationMs?: number
}

/** The context a model call received, assembled without writing anything. */
export class AssembledContext {
  private constructor(
    readonly fragments: readonly ContextMessage[],
    readonly manifest: ContextManifest
  ) {}

  /** @internal */
  static create(fragments: readonly ContextMessage[], manifest: ContextManifest): AssembledContext {
    return new AssembledContext(fragments, manifest)
  }

  /** The fragments' bodies as UTF-8, one per line. */
  text(): string {
    const decoder = new TextDecoder()
    return this.fragments.map((fragment) => decoder.decode(contextMessageBody(fragment))).join("\n")
  }
}

/** A model call in flight. Finish it with `complete` or `fail`. */
export class ModelCall {
  private readonly started = performance.now()

  private constructor(
    private readonly session: Session,
    private readonly callCorrelation: CorrelationId,
    private readonly operation: string
  ) {}

  /** @internal */
  static create(session: Session, correlation: CorrelationId, operation: string): ModelCall {
    return new ModelCall(session, correlation, operation)
  }

  /** The call's correlation, shared by its request, manifest, and result. */
  correlation(): CorrelationId {
    return this.callCorrelation
  }

  /** Record the model's response, with its usage, the answering model, the
   * finish reason, and the call duration. */
  complete(response: ModelResponse, options: CallOptions = {}): Promise<AgdxReceipt> {
    return this.session.writeResult(
      this.callCorrelation,
      this.operation,
      undefined,
      response,
      response.durationMs ?? performance.now() - this.started,
      options
    )
  }

  /** Record that the call failed. */
  fail(error: AgentErrorBody, options: CallOptions = {}): Promise<AgdxReceipt> {
    return this.session.writeFailure(
      this.callCorrelation,
      this.operation,
      undefined,
      error,
      options
    )
  }
}

/** A tool call in flight. Finish it with `complete` or `fail`. */
export class ToolCall {
  private readonly started = performance.now()

  private constructor(
    private readonly session: Session,
    private readonly callCorrelation: CorrelationId,
    private readonly tool: string
  ) {}

  /** @internal */
  static create(session: Session, correlation: CorrelationId, tool: string): ToolCall {
    return new ToolCall(session, correlation, tool)
  }

  /** The call's correlation, shared by its command and result. */
  correlation(): CorrelationId {
    return this.callCorrelation
  }

  /** Record the tool's result and the measured duration. */
  complete(result: BytesLike, options: CallOptions = {}): Promise<AgdxReceipt> {
    return this.session.writeResult(
      this.callCorrelation,
      OPERATION_EXECUTE_TOOL,
      this.tool,
      { body: ownedBytes(result) },
      performance.now() - this.started,
      options
    )
  }

  /** Record that the tool failed. */
  fail(error: AgentErrorBody, options: CallOptions = {}): Promise<AgdxReceipt> {
    return this.session.writeFailure(
      this.callCorrelation,
      OPERATION_EXECUTE_TOOL,
      this.tool,
      error,
      options
    )
  }
}

/** The session's state document: RFC 6902 patches on the session lane, applied
 * in lane order, with snapshots as checkpoints. */
export class SessionState {
  private constructor(
    private readonly session: Session,
    private readonly cursor: StateCursor
  ) {}

  /** @internal */
  static create(session: Session, cursor: StateCursor): SessionState {
    return new SessionState(session, cursor)
  }

  /** Set `key` to `value`. */
  set(key: string, value: unknown, options: CallOptions = {}): Promise<AgdxReceipt> {
    const path = `/${key.replaceAll("~", "~0").replaceAll("/", "~1")}`
    return this.patch([{ op: "add", path, value }], options)
  }

  /** Apply `patch` atomically against the revision this handle last saw.
   * Append success does not prove the patch applied: a reader learns the
   * outcome from the folded state. */
  patch(patch: readonly PatchOp[], options: CallOptions = {}): Promise<AgdxReceipt> {
    return this.exclusive(options, async () => {
      await this.seed(options)
      return this.writeDelta(patch, options)
    })
  }

  /** Replace the whole document with `document`, a JSON object, as one
   * revision-guarded patch that removes the keys it drops and sets every key
   * it holds. */
  replace(
    document: Readonly<Record<string, unknown>>,
    options: CallOptions = {}
  ): Promise<AgdxReceipt> {
    if (typeof document !== "object" || Array.isArray(document)) {
      return Promise.reject(new InvalidError("a state document is a JSON object"))
    }
    return this.exclusive(options, async () => {
      await this.seed(options)
      const current = this.cursor.document
      const pointer = (key: string): string => `/${key.replaceAll("~", "~0").replaceAll("/", "~1")}`
      const operations: PatchOp[] = []
      if (typeof current === "object" && current !== null && !Array.isArray(current)) {
        for (const key of Object.keys(current)) {
          if (!Object.hasOwn(document, key)) operations.push({ op: "remove", path: pointer(key) })
        }
      }
      for (const [key, value] of Object.entries(document)) {
        operations.push({ op: "add", path: pointer(key), value })
      }
      return this.writeDelta(operations, options)
    })
  }

  /** Write the whole document this handle holds as a snapshot. Fails with
   * the stale session error, writing nothing, when the session's document is
   * no longer the one this handle holds, so a handle whose earlier write did
   * not apply never overwrites a later change. */
  snapshot(options: CallOptions = {}): Promise<AgdxReceipt> {
    return this.exclusive(options, async () => {
      if (!(await this.holdsCurrent(options))) {
        const message = "the session state changed since this handle's last write"
        throw new SessionError(`stale session generation: ${message}`, { kind: "stale", message })
      }
      return this.writeSnapshot(options)
    })
  }

  // Every write runs after the earlier writes of this cursor settled, so the
  // writes of one handle and the handles derived from it land in revision
  // order. A write aborted while it waits for its turn never starts.
  private exclusive<T>(options: CallOptions, write: () => Promise<T>): Promise<T> {
    return cancellable(options.signal, "session state write", () => {
      const run = this.cursor.tail.then(() => {
        throwIfAborted(options.signal, "session state write")
        return write()
      })
      this.cursor.tail = run.then(
        () => undefined,
        () => undefined
      )
      return run
    })
  }

  // A failed publish leaves the lane revision unknown, so the next write
  // reseeds from the folded lane instead of naming a revision nothing was
  // written at.
  private async writeDelta(patch: readonly PatchOp[], options: CallOptions): Promise<AgdxReceipt> {
    const next = applyJsonPatch(this.cursor.document ?? {}, patch)
    const delta = {
      baseRevision: this.cursor.revision,
      patch,
      opId: this.session.mintOpId()
    }
    let receipt: AgdxReceipt
    try {
      receipt = await this.session.writeEvent(
        OPERATION_STATE_DELTA,
        encodeNamed(encodeStateDelta(delta)),
        undefined,
        options
      )
    } catch (error) {
      this.cursor.document = undefined
      throw error
    }
    this.cursor.revision += 1n
    this.cursor.document = next
    this.cursor.sinceSnapshot += 1
    if (this.cursor.sinceSnapshot >= SNAPSHOT_EVERY_DELTAS) {
      try {
        if (await this.holdsCurrent(options)) await this.writeSnapshot(options)
      } catch (error) {
        warn(
          `Writing the automatic state snapshot of session ${this.session.conversation.toString()} failed, the next write retries it: ${String(error)}`
        )
      }
    }
    return receipt
  }

  private async writeSnapshot(options: CallOptions): Promise<AgdxReceipt> {
    const receipt = await this.session.writeEvent(
      OPERATION_STATE_SNAPSHOT,
      encodeNamed(
        encodeStateSnapshot({
          baseRevision: this.cursor.revision,
          document: this.cursor.document ?? {}
        })
      ),
      undefined,
      options
    )
    this.cursor.sinceSnapshot = 0
    return receipt
  }

  // A handle that has not written yet, or whose last write failed, starts from
  // the folded lane, so its next delta names the revision the lane is at.
  private async seed(options: CallOptions): Promise<void> {
    if (this.seeded()) return
    const view = await this.currentView(options)
    this.cursor.revision = view.revision
    this.cursor.document = view.document ?? {}
  }

  /** The lane fold when it is complete or the deployment does not index
   * sessions. Otherwise the managed state view, used only when it answers and
   * is not behind the lane fold.
   * @internal */
  async currentView(options: CallOptions = {}): Promise<SessionStateView> {
    const lane = await this.get(options)
    if (lane.complete) return lane
    try {
      const view = await this.session.managedStateView(options)
      return view !== undefined && view.revision >= lane.revision ? view : lane
    } catch {
      throwIfAborted(options.signal, "session state read")
      return lane
    }
  }

  private seeded(): boolean {
    return this.cursor.document !== undefined
  }

  /** A stale handle ends its session without writing state, as a stale delta
   * leaves the document to the writer that won.
   * @internal */
  snapshotIfChanged(options: CallOptions = {}): Promise<void> {
    return this.exclusive(options, async () => {
      if (this.cursor.sinceSnapshot === 0) return
      if (await this.holdsCurrent(options)) await this.writeSnapshot(options)
    })
  }

  // Whether the session's current document is the one this handle holds. A
  // delta written against a stale base never applied, yet the handle counted
  // it, so its revision can name the revision the session reached through
  // another writer with a document nobody applied. An unseeded handle is
  // seeded and holds the current document by construction. A stale handle
  // reseeds on its next write.
  private async holdsCurrent(options: CallOptions): Promise<boolean> {
    if (!this.seeded()) {
      await this.seed(options)
      return true
    }
    const view = await this.currentView(options)
    if (
      view.revision === this.cursor.revision &&
      equalJson(view.document ?? {}, this.cursor.document)
    ) {
      return true
    }
    this.cursor.document = undefined
    return false
  }

  /** Fold the state from the whole retained session lane: the newest snapshot
   * plus the deltas after it. `complete` is false when retention removed the
   * records the document starts from. A state record that does not decode is
   * skipped, as the session index skips it. */
  async get(options: CallOptions = {}): Promise<SessionStateView> {
    throwIfAborted(options.signal, "session state read")
    const records = await abortable(
      this.session.laneRecords(),
      options.signal,
      "session state read"
    )
    let document: unknown = {}
    let revision = 0n
    let complete = false
    for (const record of records) {
      const envelope = record.envelope
      if (envelope === undefined) continue
      const context = "session state"
      if (envelope.operation === OPERATION_STATE_SNAPSHOT) {
        const snapshot = decodeState(() =>
          decodeStateSnapshot(expectMap(decodeOne(envelope.body, context), context), context)
        )
        if (snapshot === undefined) continue
        // The first snapshot is the baseline. A later one counts only at the
        // revision the fold has reached and with the document it holds, so a
        // stale snapshot from a second writer changes nothing.
        if (
          complete &&
          (snapshot.baseRevision !== revision || !equalJson(snapshot.document, document))
        )
          continue
        document = snapshot.document
        revision = snapshot.baseRevision
        complete = true
      } else if (envelope.operation === OPERATION_STATE_DELTA) {
        const delta = decodeState(() =>
          decodeStateDelta(expectMap(decodeOne(envelope.body, context), context), context)
        )
        if (delta === undefined) continue
        if (delta.baseRevision === 0n && revision === 0n) complete = true
        if (delta.baseRevision !== revision) continue
        try {
          document = applyJsonPatch(document, delta.patch)
          revision += 1n
        } catch {
          // A delta that does not apply leaves the document unchanged.
        }
      }
    }
    return { revision, document, history: [], complete }
  }
}

function decodeState<Record>(decode: () => Record): Record | undefined {
  try {
    return decode()
  } catch {
    return undefined
  }
}

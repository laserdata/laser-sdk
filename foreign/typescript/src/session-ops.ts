import { type BytesLike, ownedBytes } from "./client/bytes.js"
import { InvalidError } from "./client/errors.js"
import type { AgdxReceipt } from "./agent/agdx.js"
import { type ContextMessage, LastN } from "./context.js"
import { AgentTopic } from "./provenance/agent-topic.js"
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
import { applyJsonPatch } from "./wire/json-patch.js"
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
// document after its own deltas, and how many deltas since the last snapshot.
/** @internal */
export interface StateCursor {
  revision: bigint
  document: unknown
  sinceSnapshot: number
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

  /** The fragments' payloads as UTF-8, one per line. */
  text(): string {
    const decoder = new TextDecoder()
    return this.fragments.map((fragment) => decoder.decode(fragment.payload)).join("\n")
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
  complete(response: ModelResponse): Promise<AgdxReceipt> {
    return this.session.writeResult(
      this.callCorrelation,
      this.operation,
      undefined,
      response,
      response.durationMs ?? performance.now() - this.started
    )
  }

  /** Record that the call failed. */
  fail(error: AgentErrorBody): Promise<AgdxReceipt> {
    return this.session.writeFailure(this.callCorrelation, this.operation, undefined, error)
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
  complete(result: BytesLike): Promise<AgdxReceipt> {
    return this.session.writeResult(
      this.callCorrelation,
      OPERATION_EXECUTE_TOOL,
      this.tool,
      { body: ownedBytes(result) },
      performance.now() - this.started
    )
  }

  /** Record that the tool failed. */
  fail(error: AgentErrorBody): Promise<AgdxReceipt> {
    return this.session.writeFailure(this.callCorrelation, OPERATION_EXECUTE_TOOL, this.tool, error)
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
  set(key: string, value: unknown): Promise<AgdxReceipt> {
    const path = `/${key.replaceAll("~", "~0").replaceAll("/", "~1")}`
    return this.patch([{ op: "add", path, value }])
  }

  /** Apply `patch` atomically against the revision this handle last saw.
   * Append success does not prove the patch applied: a reader learns the
   * outcome from the folded state. */
  async patch(patch: readonly PatchOp[]): Promise<AgdxReceipt> {
    await this.seed()
    const next = applyJsonPatch(this.cursor.document ?? {}, patch)
    const delta = {
      baseRevision: this.cursor.revision,
      patch,
      opId: this.session.mintOpId()
    }
    this.cursor.revision += 1n
    this.cursor.document = next
    this.cursor.sinceSnapshot += 1
    const due = this.cursor.sinceSnapshot >= SNAPSHOT_EVERY_DELTAS
    const receipt = await this.session.writeEvent(
      OPERATION_STATE_DELTA,
      encodeNamed(encodeStateDelta(delta))
    )
    if (due) await this.snapshot()
    return receipt
  }

  /** Replace the whole document with `document`, a JSON object, as one
   * revision-guarded patch that removes the keys it drops and sets every key
   * it holds. */
  async replace(document: Readonly<Record<string, unknown>>): Promise<AgdxReceipt> {
    if (typeof document !== "object" || Array.isArray(document)) {
      throw new InvalidError("a state document is a JSON object")
    }
    await this.seed()
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
    return this.patch(operations)
  }

  /** Write the whole document this handle holds as a snapshot. */
  async snapshot(): Promise<AgdxReceipt> {
    await this.seed()
    const revision = this.cursor.revision
    const receipt = await this.session.writeEvent(
      OPERATION_STATE_SNAPSHOT,
      encodeNamed(
        encodeStateSnapshot({ baseRevision: revision, document: this.cursor.document ?? {} })
      )
    )
    if (this.cursor.revision === revision) this.cursor.sinceSnapshot = 0
    return receipt
  }

  // A handle that has not written yet starts from the folded lane, so its
  // first delta names the revision the lane is at.
  private async seed(): Promise<void> {
    if (this.seeded()) return
    const view = await this.currentView()
    // A concurrent write may have seeded the cursor meanwhile.
    if (this.seeded()) return
    this.cursor.revision = view.revision
    this.cursor.document = view.document ?? {}
  }

  /** The lane fold when it is complete or the deployment does not index
   * sessions. Otherwise the managed state view, used only when it answers and
   * is not behind the lane fold.
   * @internal */
  async currentView(): Promise<SessionStateView> {
    const lane = await this.get()
    if (lane.complete) return lane
    try {
      const view = await this.session.managedStateView()
      return view !== undefined && view.revision >= lane.revision ? view : lane
    } catch {
      return lane
    }
  }

  private seeded(): boolean {
    return this.cursor.document !== undefined
  }

  /** @internal */
  async snapshotIfChanged(): Promise<void> {
    if (this.cursor.sinceSnapshot > 0) await this.snapshot()
  }

  /** Fold the state from the retained session lane: the newest snapshot plus
   * the deltas after it. `complete` is false when the lane no longer holds the
   * records the document starts from. */
  async get(): Promise<SessionStateView> {
    const records = await this.session.scope.fetchWith(
      [AgentTopic.Sessions],
      new LastN(Number.MAX_SAFE_INTEGER)
    )
    let document: unknown = {}
    let revision = 0n
    let complete = false
    for (const record of records) {
      const envelope = record.envelope
      if (envelope === undefined) continue
      const context = "session state"
      if (envelope.operation === OPERATION_STATE_SNAPSHOT) {
        const snapshot = decodeStateSnapshot(
          expectMap(decodeOne(envelope.body, context), context),
          context
        )
        // The first snapshot is the baseline. A later one counts only at the
        // revision the fold has reached, so a stale snapshot from a second
        // writer changes nothing.
        if (complete && snapshot.baseRevision !== revision) continue
        document = snapshot.document
        revision = snapshot.baseRevision
        complete = true
      } else if (envelope.operation === OPERATION_STATE_DELTA) {
        const delta = decodeStateDelta(
          expectMap(decodeOne(envelope.body, context), context),
          context
        )
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

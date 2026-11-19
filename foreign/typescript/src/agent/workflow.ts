import { AsyncLocalStorage } from "node:async_hooks"
import { sha256 } from "@noble/hashes/sha2.js"
import { bytesToHex } from "@noble/hashes/utils.js"
import { breachError, exceededBreach } from "./budget.js"
import { decodeUtf8, ownedBytes, utf8, type BytesLike } from "../client/bytes.js"
import { throwIfAborted } from "../client/call-options.js"
import {
  BudgetExceededError,
  CancelledError,
  InvalidError,
  JournalTruncatedError,
  KvExecutionError,
  StepFailedError,
  type StepFailure,
  UnsupportedError
} from "../client/errors.js"
import { INTERNAL_REPLY_HUB, INTERNAL_TRANSPORT } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { LastN, replayContext } from "../context.js"
import { LeaseGuard, type Lease } from "../managed/kv.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { warn } from "../runtime/warn.js"
import { type Session, isMissingLane } from "../session.js"
import type { SessionLease } from "./lease.js"
import { AgentId, ConversationId } from "../types/ids.js"
import { parseIdempotencyKey, type IdempotencyKey, type SessionStatus } from "../wire/agent.js"
import { decodeOne, encodeNamed, expectMap, field, type CborMap } from "../wire/cbor.js"
import { CorrelationId } from "../wire/ids.js"
import { REQUIRE_ALL } from "./context.js"
import type { Contract } from "./contract.js"
import { agentMessageBody } from "./reliable-consumer.js"
import {
  ADVERTISED_INBOX_ROUTE,
  routeTo,
  routeToPrincipal,
  type InboxRoute,
  type Router
} from "./router.js"

const STEP_BUDGET_FLOOR_MS = 100
const DEFAULT_STEP_DEADLINE_MS = 30_000
const MAX_REASSIGNMENTS = 2
export const WORKFLOW_FENCE_NAMESPACE = "agdx.workflow.fence"
// Shorter than the default step deadline, so a resumed run waits out the lease
// a crashed run left behind instead of failing the step.
const WORKFLOW_LEASE_TTL_MS = 15_000
const LEASE_RETRY_INTERVAL_MS = 500
const CURRENT_RUN = new AsyncLocalStorage<WorkflowRun>()
const STEP_CALL_SEPARATOR = "#"
const STEP_INPUT_DIGEST_CHARS = 32
const STEP_SETTLE_GRACE_MS = 5_000
const LASER_STREAM_VARIABLE = "LASER_STREAM"
const MAX_STREAM_NAME_BYTES = 255

export class WorkflowBudget {
  /** @internal */
  readonly tokenLimit: bigint | undefined
  /** @internal */
  readonly wallClockLimitMs: number | undefined
  /** @internal */
  readonly invocationLimit: number | undefined

  private constructor(tokenLimit?: bigint, wallClockLimitMs?: number, invocationLimit?: number) {
    this.tokenLimit = tokenLimit
    this.wallClockLimitMs = wallClockLimitMs
    this.invocationLimit = invocationLimit
  }

  static unlimited(): WorkflowBudget {
    return new WorkflowBudget()
  }

  static tokens(tokens: bigint): WorkflowBudget {
    if (tokens < 0n) throw new InvalidError("token budget must be non-negative")
    return new WorkflowBudget(tokens)
  }

  wallClock(wallClockMs: number): WorkflowBudget {
    if (!Number.isFinite(wallClockMs) || wallClockMs < 0) {
      throw new InvalidError("wall-clock budget must be a non-negative finite number")
    }
    return new WorkflowBudget(this.tokenLimit, wallClockMs, this.invocationLimit)
  }

  invocations(invocations: number): WorkflowBudget {
    if (!Number.isSafeInteger(invocations) || invocations < 0) {
      throw new InvalidError("invocation budget must be a non-negative safe integer")
    }
    return new WorkflowBudget(this.tokenLimit, this.wallClockLimitMs, invocations)
  }
}

export interface StepContext {
  /** Every earlier step's output by label, as bytes. */
  readonly outputs: ReadonlyMap<string, Uint8Array>
}

/** A step body: a routed step's command payload, or a local step's output.
 * A plain function or an async function, returning a string or bytes
 * directly or as a promise. Text becomes its UTF-8 bytes. */
export type StepFn = (context: StepContext) => BytesLike | string | Promise<BytesLike | string>
export type Verifier = (output: Uint8Array) => boolean | Promise<boolean>
export type OnTimeout = "fail" | "reassign"

// A completed step of a replayed journal, with its call number and the digest of its arguments.
interface RecordedStep {
  readonly label: string
  readonly input: string | undefined
  readonly output: Uint8Array
}

// What a run knows about a step call: the record it answers from, or the label it is due under.
type StepCall =
  | { readonly kind: "recorded"; readonly label: string; readonly output: Uint8Array }
  | { readonly kind: "due"; readonly label: string; readonly call: number }

// A step without a target runs its body in process.
interface Step {
  readonly label: string
  readonly target: Router | undefined
  readonly build: StepFn
  readonly after: string[]
  verifier?: Verifier
  exclusive: boolean
  fenceNamespace?: string
  onTimeout: OnTimeout
  compensation?: StepFn
  deadlineMs: number
}

/** The result of a completed run: each step's output by label, the labels in
 * completion order (a resumed run's journaled steps first), the run session's
 * end status, and the run id. A failed, canceled, or over-budget run throws
 * instead, so `status` reads `completed`. */
export interface WorkflowOutcome<Value = undefined> {
  /** The workflow function's return value, `undefined` for a builder run. */
  readonly value: Value
  /** Each completed step's output by label, as bytes. */
  readonly outputs: ReadonlyMap<string, Uint8Array>
  /** The completed step labels, in completion order. */
  readonly completedSteps: readonly string[]
  /** The run's end status, `completed` for a returned outcome. */
  readonly status: SessionStatus
  /** The run id, which resumes the same run when passed to `Workflow.runId`. */
  readonly runId: ConversationId
  /** The output of step `label` as UTF-8 text, invalid sequences replaced,
   * or `undefined` for a label that did not complete. */
  text(label: string): string | undefined
  /** The output of step `label` decoded as JSON, or `undefined` for a label
   * that did not complete. */
  json(label: string): unknown
}

/** What a value of type `T` reads as after a JSON round trip. */
export type JsonRoundTrip<T> = unknown extends T
  ? T
  : T extends string | number | boolean | null
    ? T
    : T extends { toJSON(): infer Json }
      ? JsonRoundTrip<Json>
      : T extends readonly unknown[]
        ? {
            -readonly [Index in keyof T]: undefined extends T[Index]
              ? JsonRoundTrip<Exclude<T[Index], undefined>> | null
              : JsonRoundTrip<T[Index]>
          }
        : T extends bigint | symbol | undefined | ((...args: never[]) => unknown)
          ? never
          : T extends ReadonlyMap<unknown, unknown> | ReadonlySet<unknown> | ArrayBufferView
            ? never
            : {
                -readonly [Key in keyof T as undefined extends T[Key] ? never : Key]: JsonRoundTrip<
                  T[Key]
                >
              } & {
                -readonly [
                  Key in keyof T as undefined extends T[Key] ? Key : never
                ]?: JsonRoundTrip<Exclude<T[Key], undefined>>
              }

/** One workflow-journal record, keyed on the log by the run id. The encoding
 * matches the Rust engine, so either client resumes a run the other started.
 * @internal */
export type JournalEntry =
  | { readonly kind: "runStarted"; readonly workflow: string }
  | {
      readonly kind: "stepDispatched"
      readonly label: string
      readonly attempt: number
      readonly replyFrom: readonly bigint[]
    }
  | {
      readonly kind: "stepCompleted"
      readonly label: string
      readonly output: Uint8Array
      readonly agents: readonly string[]
      readonly input?: string
    }
  | { readonly kind: "stepCompensated"; readonly label: string }

interface InFlight {
  readonly attempt: number
  readonly replyFrom: readonly bigint[]
}

/** What a run's journal records: whether it holds the run's first record, each
 * completed step's output and the agents that ran it, the dispatch a stop left
 * without an outcome, and the last attempt of every step.
 * @internal */
export interface Journal {
  readonly started: boolean
  readonly records: number
  readonly outputs: Map<string, Uint8Array>
  readonly inputs: Map<string, string>
  readonly agents: Map<string, readonly AgentId[]>
  readonly inFlight: Map<string, InFlight>
  readonly attempts: Map<string, number>
}

interface Attempt {
  readonly number: number
  readonly replyFrom?: readonly bigint[]
}

interface StepDone {
  readonly output: Uint8Array
  readonly tokens: bigint
  readonly agents: readonly AgentId[]
}

interface Effect {
  readonly index: number
  readonly agents: readonly AgentId[]
}

type AttemptOutcome =
  | { readonly kind: "done"; readonly done: StepDone }
  | { readonly kind: "timedOut"; readonly agent: AgentId }

// A step that did not complete, with the agents that ran it when its effect
// happened anyway (a rejected or unrecorded reply).
class StepError extends Error {
  constructor(
    readonly error: unknown,
    readonly effect?: readonly AgentId[]
  ) {
    super(error instanceof Error ? error.message : String(error))
  }
}

/** @internal */
export function decodeJournalEntry(payload: Uint8Array): JournalEntry {
  const context = "workflow journal entry"
  const outer = expectMap(decodeOne(payload, context), context)
  if (outer.has("RunStarted")) {
    const started = field.requiredMap(outer, "RunStarted", context)
    return { kind: "runStarted", workflow: field.requiredString(started, "workflow", context) }
  }
  if (outer.has("StepDispatched")) {
    const dispatched = field.requiredMap(outer, "StepDispatched", context)
    return {
      kind: "stepDispatched",
      label: field.requiredString(dispatched, "label", context),
      attempt: Number(field.requiredU64(dispatched, "attempt", context)),
      replyFrom: field.requiredArray(dispatched, "reply_from", context, (item) =>
        unsignedItem(item, context)
      )
    }
  }
  if (outer.has("StepCompensated")) {
    const compensated = field.requiredMap(outer, "StepCompensated", context)
    return { kind: "stepCompensated", label: field.requiredString(compensated, "label", context) }
  }
  const completed = field.requiredMap(outer, "StepCompleted", context)
  const input = completed.get("input")
  return {
    kind: "stepCompleted",
    label: field.requiredString(completed, "label", context),
    output: journalBytes(completed, "output", context),
    agents: field.requiredArray(completed, "agents", context, (item) => {
      if (typeof item !== "string") throw new InvalidError(`${context} agent must be a string`)
      return item
    }),
    ...(typeof input === "string" ? { input } : {})
  }
}

/** @internal */
export function encodeJournalEntry(entry: JournalEntry): Uint8Array {
  switch (entry.kind) {
    case "runStarted":
      return encodeNamed(new Map([["RunStarted", new Map([["workflow", entry.workflow]])]]))
    case "stepDispatched":
      return encodeNamed(
        new Map([
          [
            "StepDispatched",
            new Map<string, unknown>([
              ["label", entry.label],
              ["attempt", entry.attempt],
              ["reply_from", [...entry.replyFrom]]
            ])
          ]
        ])
      )
    case "stepCompleted":
      return encodeNamed(
        new Map([
          [
            "StepCompleted",
            new Map<string, unknown>([
              ["label", entry.label],
              ["output", entry.output],
              ["agents", [...entry.agents]],
              ...(entry.input === undefined ? [] : [["input", entry.input] as const])
            ])
          ]
        ])
      )
    case "stepCompensated":
      return encodeNamed(new Map([["StepCompensated", new Map([["label", entry.label]])]]))
  }
}

/** Fold a run's journal entries in log order.
 * @internal */
export function foldJournal(entries: Iterable<JournalEntry>): Journal {
  let started = false
  let records = 0
  const outputs = new Map<string, Uint8Array>()
  const inputs = new Map<string, string>()
  const agents = new Map<string, readonly AgentId[]>()
  const inFlight = new Map<string, InFlight>()
  const attempts = new Map<string, number>()
  for (const entry of entries) {
    records += 1
    switch (entry.kind) {
      case "runStarted":
        started = true
        break
      case "stepDispatched":
        attempts.set(entry.label, Math.max(attempts.get(entry.label) ?? 0, entry.attempt))
        inFlight.set(entry.label, { attempt: entry.attempt, replyFrom: entry.replyFrom })
        break
      case "stepCompleted":
        inFlight.delete(entry.label)
        agents.set(
          entry.label,
          entry.agents.flatMap((agent) => {
            try {
              return [AgentId.new(agent)]
            } catch {
              return []
            }
          })
        )
        outputs.set(entry.label, entry.output)
        if (entry.input === undefined) inputs.delete(entry.label)
        else inputs.set(entry.label, entry.input)
        break
      case "stepCompensated":
        inFlight.delete(entry.label)
        agents.delete(entry.label)
        outputs.delete(entry.label)
        inputs.delete(entry.label)
        break
    }
  }
  return { started, records, outputs, inputs, agents, inFlight, attempts }
}

// Rust writes a byte field as an array of integers, TypeScript as a byte
// string. Both decode.
function journalBytes(map: CborMap, key: string, context: string): Uint8Array {
  const value = map.get(key)
  if (Array.isArray(value)) {
    return Uint8Array.from(value.map((item) => Number(unsignedItem(item, context))))
  }
  return field.requiredBytes(map, key, context)
}

function unsignedItem(item: unknown, context: string): bigint {
  if (typeof item === "number" && Number.isSafeInteger(item) && item >= 0) return BigInt(item)
  if (typeof item === "bigint" && item >= 0n) return item
  throw new InvalidError(`${context} holds a value that is not an unsigned integer`)
}

/** The ids one attempt keeps across every send: its child conversation, its
 * correlation, and the idempotency key the target deduplicates by.
 * @internal */
export function dispatchIds(
  runId: ConversationId,
  label: string,
  attempt: number
): {
  readonly conversation: ConversationId
  readonly correlation: CorrelationId
  readonly key: IdempotencyKey
} {
  const seed = `${runId.toString()}/${label}/${String(attempt)}`
  const conversation = ConversationId.derive(seed)
  const correlation = CorrelationId.parse(ConversationId.derive(`${seed}/correlation`).toString())
  return {
    conversation,
    correlation,
    key: parseIdempotencyKey(`workflow:${conversation.toString()}`)
  }
}

export function topologicalOrder(
  steps: readonly Pick<Step, "label" | "after">[]
): readonly number[] {
  const indexOf = new Map(steps.map((step, index) => [step.label, index]))
  const inDegree = new Array<number>(steps.length).fill(0)
  const dependents = Array.from({ length: steps.length }, () => [] as number[])
  for (const [index, step] of steps.entries()) {
    for (const dependency of step.after) {
      const dependencyIndex = indexOf.get(dependency)
      if (dependencyIndex === undefined) {
        throw new InvalidError(
          `workflow step \`${step.label}\` depends on unknown step \`${dependency}\``
        )
      }
      inDegree[index] = (inDegree[index] ?? 0) + 1
      dependents[dependencyIndex]?.push(index)
    }
  }
  const ready = steps.flatMap((_step, index) => (inDegree[index] === 0 ? [index] : []))
  const order: number[] = []
  for (const index of ready) {
    order.push(index)
    for (const dependent of dependents[index] ?? []) {
      const degree = (inDegree[dependent] ?? 0) - 1
      inDegree[dependent] = degree
      if (degree === 0) ready.push(dependent)
    }
  }
  if (order.length !== steps.length) {
    throw new InvalidError("workflow steps form a dependency cycle")
  }
  return order
}

/** A journaled workflow from `laser.workflow(name)`, returned at once. Chain
 * `stepLocal` or `step` calls, then await `run()` for a `WorkflowOutcome`:
 *
 * ```ts
 * const outcome = await laser
 *   .workflow("pipeline")
 *   .stepLocal("ingest", () => "frames from sat-7")
 *   .stepLocal("analyze", async ({ outputs }) => {
 *     await new Promise((resolve) => setTimeout(resolve, 1000))
 *     return `analyzed ${new TextDecoder().decode(outputs.get("ingest"))}`
 *   })
 *   .after("ingest")
 *   .run()
 * console.log(outcome.status, outcome.completedSteps, outcome.text("analyze"))
 * ```
 *
 * A step's body is a plain function or an async function. It gets the
 * earlier outputs by label as bytes and returns a string or bytes. Steps run
 * in the order `after` declares, a routed step sends its task to an agent
 * instead, and each step can carry a verifier panel, exclusivity (a fenced
 * at-most-once effect), an on-timeout policy, and a compensation (the saga
 * rollback). `budget` caps the run. */
export class Workflow {
  private budgetValue = WorkflowBudget.unlimited()
  private route: InboxRoute = ADVERTISED_INBOX_ROUTE
  private resumeId: ConversationId | undefined
  private readonly steps: Step[] = []

  private constructor(
    private laser: Laser,
    private readonly name: string
  ) {}

  /** @internal */
  static create(laser: Laser, name: string): Workflow {
    return new Workflow(laser, name)
  }

  budget(budget: WorkflowBudget): this {
    this.budgetValue = budget
    return this
  }

  inboxRoute(route: InboxRoute): this {
    this.route = route
    return this
  }

  /** Run under `runId`: the engine replays that run's whole journal, skips
   * the steps recorded complete, and dispatches only the unfinished ones. A
   * step dispatched before the run stopped is answered by its reply if one
   * landed, and sent again under the same idempotency key otherwise. A
   * journal that retention shortened past the run's first record throws
   * `JournalTruncatedError`. The run session reopens through `resume()`, so a
   * run that ended completed or failed runs again under the same id and a
   * canceled one throws `SessionEndedError`. */
  runId(runId: ConversationId): this {
    this.resumeId = runId
    return this
  }

  step(label: string, target: Router, build: StepFn): StepHandle {
    const step: Step = {
      label,
      target,
      build,
      after: [],
      exclusive: false,
      onTimeout: "fail",
      deadlineMs: DEFAULT_STEP_DEADLINE_MS
    }
    this.steps.push(step)
    return StepHandle.create(this, step)
  }

  /** Add a step whose body `run` runs in this process: its result is the
   * step output, so the step needs no agent. `run` is a plain function or an
   * async function, gets `{ outputs }` with every earlier step's output by
   * label as bytes, and returns a string or bytes, directly or as a promise.
   * `WorkflowOutcome.text(label)` decodes one output. The output is
   * journaled like a routed step's reply, so a resumed run skips
   * it once recorded and runs it again when it stopped before its record. It
   * runs inside the step deadline, a compensation of it runs in process too,
   * and it cannot be exclusive. */
  stepLocal(label: string, run: StepFn): StepHandle {
    const step: Step = {
      label,
      target: undefined,
      build: run,
      after: [],
      exclusive: false,
      onTimeout: "fail",
      deadlineMs: DEFAULT_STEP_DEADLINE_MS
    }
    this.steps.push(step)
    return StepHandle.create(this, step)
  }

  /** Await it to run the workflow to completion: it resolves to a
   * `WorkflowOutcome` with `runId`, `status` (`completed`), `completedSteps`
   * in completion order, `outputs` (label to bytes), and `text(label)`. A step
   * that does not complete throws `StepFailedError` after the compensations
   * of the steps with an effect ran in reverse. A misconfigured workflow (a
   * duplicate label, a broadcast step, an exclusive all-capable step,
   * reassignment without exclusivity, an exclusive step without fenced
   * leases, a dependency cycle) is refused before anything is written. The
   * run is a session on `agent.sessions` whose id is the run id, and every
   * step and compensation is a child session of it. Between steps the engine
   * checks `agent.control` for a cancel request, which compensates and
   * throws `CancelledError`, and then whether the run session is over its
   * budget, which compensates and throws `BudgetExceededError`. A failed
   * check compensates before its error is thrown too. Any budget breach ends
   * the run session failed with reason `budget`. Aborting `signal` cancels
   * the run at the next step boundary and compensates. A lifecycle record
   * that fails to publish surfaces as the error, except when the run itself
   * already failed. */
  async run(options: { readonly signal?: AbortSignal } = {}): Promise<WorkflowOutcome> {
    throwIfAborted(options.signal, "workflow run")
    const source = AgentId.new(this.name)
    const order = await this.validate()
    const resumed = this.resumeId !== undefined
    const runId = this.resumeId ?? ConversationId.new()
    this.resumeId = runId
    const journal = await this.replay(runId, resumed)
    const { session, lease } = await this.openSession(source, runId, resumed)
    try {
      let outcome: WorkflowOutcome
      try {
        outcome = await this.execute(source, order, runId, session, journal, options.signal)
      } catch (error) {
        await endFailed(session, error)
        throw error
      }
      await session.end()
      return outcome
    } finally {
      lease.release()
    }
  }

  /** Run `body` as this workflow and resolve to its outcome, with `value` set
   * to what `body` returned. Without a default stream on the client the run
   * uses `LASER_STREAM`, else a stream named after the workflow. */
  async runWith<Result>(
    body: (run: WorkflowRun) => Result,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<WorkflowOutcome<Awaited<Result>>> {
    throwIfAborted(options.signal, "workflow run")
    if (this.steps.length > 0) {
      throw new InvalidError(
        `workflow \`${this.name}\` declares builder steps, so it cannot run a body: use the builder steps or runWith, not both`
      )
    }
    this.laser = this.streamClient()
    const source = AgentId.new(this.name)
    const resumed = this.resumeId !== undefined
    const runId = this.resumeId ?? ConversationId.new()
    this.resumeId = runId
    const journal = await this.replay(runId, resumed)
    const { session, lease } = await this.openSession(source, runId, resumed)
    try {
      const started = performance.now()
      const cancelRequests = new WeakSet<CancelledError>()
      let invocations = 0
      const run = WorkflowRun.create(
        runId,
        journal.outputs,
        journal.inputs,
        async () => {
          throwIfAborted(options.signal, "workflow run")
          try {
            await this.boundary(session, runId, started, undefined)
          } catch (error) {
            if (error instanceof CancelledError) cancelRequests.add(error)
            throw error
          }
          invocations += 1
          this.checkBudget(invocations, 0n, started)
        },
        (label, input, output) =>
          this.record(runId, {
            kind: "stepCompleted",
            label,
            output,
            agents: [],
            ...(input === undefined ? {} : { input })
          })
      )
      let value: Awaited<Result>
      try {
        if (!journal.started) {
          await this.record(runId, { kind: "runStarted", workflow: this.name })
        }
        value = await CURRENT_RUN.run(run, () => body(run))
        await run.settle()
      } catch (error) {
        // Decided before the settle. An aborted run writes no terminal state, as a stopped
        // process, so a resume continues it.
        const aborted =
          options.signal?.aborted === true ||
          (error instanceof CancelledError && !cancelRequests.has(error) && run.stepAborted(error))
        await run.settle()
        if (!aborted) await endFailed(session, error)
        throw error
      }
      await session.end()
      return workflowOutcome(runId, new Map(journal.outputs), value)
    } finally {
      lease.release()
    }
  }

  // The client's default stream, `LASER_STREAM`, then the workflow's own name.
  private streamClient(): Laser {
    if (this.laser.defaultStream !== undefined) return this.laser
    const configured = process.env[LASER_STREAM_VARIABLE]
    if (configured !== undefined && configured.length > 0) {
      return this.laser.withDefaultStream(configured)
    }
    const nameBytes = utf8(this.name).byteLength
    if (nameBytes === 0 || nameBytes > MAX_STREAM_NAME_BYTES) {
      throw new InvalidError(
        `workflow name \`${this.name}\` cannot name a stream: set ${LASER_STREAM_VARIABLE}, pass the stream option, or give the client a default stream`
      )
    }
    return this.laser.withDefaultStream(this.name)
  }

  // The run session, new or reopened. A run id the caller named may name a run
  // that already ended completed or failed, which a resume reopens. A canceled
  // run stays ended.
  private async openSession(
    source: AgentId,
    runId: ConversationId,
    resumed: boolean
  ): Promise<{ readonly session: Session; readonly lease: SessionLease }> {
    const tokens = this.budgetValue.tokenLimit
    const open = (): Promise<{ readonly session: Session; readonly lease: SessionLease }> => {
      let builder = this.laser.sessions().start().withId(runId).agent(source)
      if (tokens !== undefined) builder = builder.budget({ tokens })
      return resumed ? builder.resume() : builder.begin()
    }
    try {
      return await open()
    } catch (error) {
      // A stream without agent topics gets them with the bootstrap defaults.
      if (!isMissingLane(error)) throw error
      await this.laser.bootstrap()
      return open()
    }
  }

  // Every configuration error, before the run writes anything.
  private async validate(): Promise<readonly number[]> {
    const labels = new Set<string>()
    for (const step of this.steps) {
      if (labels.has(step.label)) {
        throw new InvalidError(`workflow step label \`${step.label}\` is declared twice`)
      }
      labels.add(step.label)
      if (step.target === undefined) {
        if (step.exclusive || step.onTimeout === "reassign") {
          throw new InvalidError("a local step runs in process, so it cannot be exclusive")
        }
        continue
      }
      if (step.target.kind === "broadcast") {
        throw new InvalidError("a broadcast workflow step has no gather target")
      }
      if (step.exclusive && step.target.kind === "allCapable") {
        throw new InvalidError("an exclusive step must be directed (to / toCapable)")
      }
      if (!step.exclusive && step.onTimeout === "reassign") {
        throw new InvalidError('onTimeout("reassign") needs an exclusive step')
      }
    }
    if (this.steps.some((step) => step.exclusive)) {
      const capabilities = await this.laser.capabilities()
      if (!capabilities.kv.fencedLeases) {
        throw new UnsupportedError(
          "an exclusive step needs the managed backend's monotonic fence sequence"
        )
      }
    }
    return topologicalOrder(this.steps)
  }

  private async execute(
    source: AgentId,
    order: readonly number[],
    runId: ConversationId,
    session: Session,
    journal: Journal,
    signal: AbortSignal | undefined
  ): Promise<WorkflowOutcome> {
    const started = performance.now()
    const outputs = journal.outputs
    if (!journal.started) {
      await this.record(runId, { kind: "runStarted", workflow: this.name })
    }
    const effects: Effect[] = order.flatMap((index) => {
      const label = this.steps[index]?.label ?? ""
      return outputs.has(label) ? [{ index, agents: journal.agents.get(label) ?? [] }] : []
    })
    let tokensSpent = 0n
    let invocations = 0

    for (const index of order) {
      const step = this.steps[index]
      if (step === undefined || outputs.has(step.label)) continue
      try {
        await this.boundary(session, runId, started, signal)
        invocations += 1
        this.checkBudget(invocations, tokensSpent, started)
      } catch (error) {
        await this.compensate(effects, outputs, runId)
        throw error
      }
      const inFlight = journal.inFlight.get(step.label)
      const attempt: Attempt =
        inFlight === undefined
          ? { number: (journal.attempts.get(step.label) ?? 0) + 1 }
          : { number: inFlight.attempt, replyFrom: inFlight.replyFrom }
      let done: StepDone
      try {
        done = await this.runStep(source, step, outputs, runId, attempt, started)
      } catch (error) {
        if (error instanceof StepError && error.effect !== undefined) {
          effects.push({ index, agents: error.effect })
        }
        await this.compensate(effects, outputs, runId)
        throw error instanceof StepError ? error.error : error
      }
      tokensSpent += done.tokens
      outputs.set(step.label, done.output)
      effects.push({ index, agents: done.agents })
      try {
        this.checkBudget(invocations, tokensSpent, started)
      } catch (error) {
        await this.compensate(effects, outputs, runId)
        throw error
      }
    }
    return workflowOutcome(runId, outputs, undefined)
  }

  // The step-boundary checks: an aborted signal or a cancel request, a run
  // session over its budget, and a wall-clock budget too spent to dispatch
  // with.
  private async boundary(
    session: Session,
    runId: ConversationId,
    started: number,
    signal: AbortSignal | undefined
  ): Promise<void> {
    if (signal?.aborted === true) {
      throw new CancelledError(`workflow \`${this.name}\` was cancelled`, {
        cause: signal.reason,
        run: runId.toString()
      })
    }
    if (await session.cancelRequested()) {
      throw new CancelledError(`workflow run \`${runId.toString()}\` was cancelled`, {
        run: runId.toString()
      })
    }
    const breach = await session.budgetBreach()
    if (breach !== undefined) throw breachError(breach)
    const ceiling = this.budgetValue.wallClockLimitMs
    if (ceiling !== undefined && ceiling - (performance.now() - started) < STEP_BUDGET_FLOOR_MS) {
      throw new BudgetExceededError(
        BigInt(Math.trunc(ceiling * 1000)),
        BigInt(Math.trunc((performance.now() - started) * 1000))
      )
    }
  }

  // One step: build its payload, dispatch it, verify the reply, and journal
  // the completion, all inside the step deadline. A local step's body result
  // is its output, verified and journaled the same way, with no dispatch.
  private async runStep(
    source: AgentId,
    step: Step,
    outputs: ReadonlyMap<string, Uint8Array>,
    runId: ConversationId,
    attempt: Attempt,
    started: number
  ): Promise<StepDone> {
    const deadlineAt = performance.now() + this.stepDeadline(step, started)
    const built = await withinDeadline(deadlineAt, Promise.resolve(step.build({ outputs })))
    if (built.kind === "timedOut") throw new StepError(this.failed(runId, step, "timed_out"))
    const payload = stepBytes(built.value)
    const route = step.target
    if (route === undefined) return this.finish(runId, step, payload, 0n, [], deadlineAt)
    if (route.kind === "allCapable") {
      return this.runAllCapable(source, step, route, payload, runId, attempt, deadlineAt)
    }
    if (step.exclusive) {
      return this.runExclusive(source, step, route, payload, runId, attempt, started, deadlineAt)
    }
    return this.runDirected(source, step, route, payload, runId, attempt, deadlineAt)
  }

  private async runDirected(
    source: AgentId,
    step: Step,
    route: Router,
    payload: Uint8Array,
    runId: ConversationId,
    attempt: Attempt,
    deadlineAt: number
  ): Promise<StepDone> {
    const ids = dispatchIds(runId, step.label, attempt.number)
    const replyFrom = await this.dispatchPoint(source, runId, step, attempt)
    const { target, contract } = await this.laser
      .contract(route)
      .from(source)
      .payload(payload)
      .inboxRoute(this.route)
      .deadline(Math.max(0, deadlineAt - performance.now()))
      .conversation(ids.conversation)
      .parent(runId, runId)
      .correlation(ids.correlation)
      .idempotencyKey(ids.key)
      .resumeFrom(replyFrom)
      .sendResolved()
    const { output, tokens } = this.completion(runId, step, contract)
    return this.finish(runId, step, output, tokens, [target], deadlineAt)
  }

  // An exclusive step claims a fenced lease, pins a task conversation stable
  // across re-dispatch, and on a timeout may reassign under a new holder,
  // preferring a capable agent other than the one that timed out.
  private async runExclusive(
    source: AgentId,
    step: Step,
    route: Router,
    payload: Uint8Array,
    runId: ConversationId,
    first: Attempt,
    started: number,
    firstDeadlineAt: number
  ): Promise<StepDone> {
    const taskConversation = ConversationId.derive(`${runId.toString()}/${step.label}`)
    const last = step.onTimeout === "reassign" ? first.number + MAX_REASSIGNMENTS : first.number
    const timedOut: AgentId[] = []
    let attempt = first
    let deadlineAt = firstDeadlineAt
    for (;;) {
      const outcome = await this.exclusiveAttempt(
        source,
        step,
        route,
        payload,
        runId,
        taskConversation,
        attempt,
        timedOut,
        deadlineAt
      )
      if (outcome.kind === "done") return outcome.done
      if (attempt.number >= last) throw new StepError(this.failed(runId, step, "timed_out"))
      timedOut.push(outcome.agent)
      attempt = { number: attempt.number + 1 }
      deadlineAt = performance.now() + this.stepDeadline(step, started)
    }
  }

  // Claim, maintain, and release one assignment's lease. A live lease of an
  // earlier holder, a crashed run being resumed, is waited out inside the
  // step deadline. Renewal stops at the step deadline, so a hung verifier
  // cannot hold the lease forever.
  private async exclusiveAttempt(
    source: AgentId,
    step: Step,
    route: Router,
    payload: Uint8Array,
    runId: ConversationId,
    taskConversation: ConversationId,
    attempt: Attempt,
    avoid: readonly AgentId[],
    deadlineAt: number
  ): Promise<AttemptOutcome> {
    const kv = this.laser.kv(step.fenceNamespace ?? WORKFLOW_FENCE_NAMESPACE)
    const key = new TextEncoder().encode(runId.toString())
    const holder = `workflow:${runId.toString()}:${String(attempt.number)}`
    const acquired = await acquireLease(kv, key, holder, deadlineAt)
    if (acquired === undefined) throw new StepError(this.failed(runId, step, "timed_out"))
    // Any exit that skips the releases below frees the key at once instead of
    // holding it until the lease lapses.
    await using guard = LeaseGuard.create(kv, key, holder, acquired)
    let lease = acquired
    const ids = dispatchIds(runId, step.label, attempt.number)
    const completed = (async (): Promise<AttemptOutcome> => {
      const replyFrom = await this.dispatchPoint(source, runId, step, attempt)
      const { target, contract } = await this.laser
        .contract(route)
        .from(source)
        .payload(payload)
        .inboxRoute(this.route)
        .deadline(Math.max(0, deadlineAt - performance.now()))
        .fence(acquired.token)
        .conversation(taskConversation)
        .parent(runId, runId)
        .correlation(ids.correlation)
        .idempotencyKey(ids.key)
        .resumeFrom(replyFrom)
        .excluding(avoid)
        .sendResolved()
      if (contract.kind === "timedOut") return { kind: "timedOut", agent: target }
      const { output, tokens } = this.completion(runId, step, contract)
      const done = await this.finish(runId, step, output, tokens, [target], deadlineAt)
      return { kind: "done", done }
    })().then(
      (outcome) => ({ kind: "contract" as const, ok: true as const, outcome }),
      (error: unknown) => ({ kind: "contract" as const, ok: false as const, error })
    )
    let attemptResult:
      | { readonly ok: true; readonly outcome: AttemptOutcome }
      | { readonly ok: false; readonly error: unknown }
    try {
      for (;;) {
        const tick = renewalTick(renewalDelayMs(lease.grantedTtlMs))
        const next = await Promise.race([
          completed,
          tick.promise.then(() => ({ kind: "renew" as const }))
        ])
        tick.cancel()
        if (next.kind === "contract") {
          attemptResult = next.ok
            ? { ok: true, outcome: next.outcome }
            : { ok: false, error: next.error }
          break
        }
        const renewalBound = Math.min(
          lease.validUntilMs - performance.now(),
          deadlineAt - performance.now()
        )
        if (renewalBound < 1) {
          throw new KvExecutionError("the exclusive workflow lease expired before renewal", {
            kind: "leaseLost"
          })
        }
        // An ambiguous renewal repeats under its identity until the lease or the
        // step runs out.
        const renewal = kv
          .renewLeaseUntil(
            key,
            holder,
            lease.token,
            WORKFLOW_LEASE_TTL_MS,
            Math.min(lease.validUntilMs, deadlineAt)
          )
          .then(
            (value) => ({ kind: "renewed" as const, ok: true as const, value }),
            (error: unknown) => ({ kind: "renewed" as const, ok: false as const, error })
          )
        const expiry = renewalTick(renewalBound)
        const pending = await Promise.race([
          completed,
          renewal,
          expiry.promise.then(() => ({ kind: "expired" as const }))
        ])
        if (pending.kind === "contract") {
          const settled = await Promise.race([
            renewal,
            expiry.promise.then(() => ({ kind: "expired" as const }))
          ])
          expiry.cancel()
          if (settled.kind === "expired") {
            throw new KvExecutionError("the exclusive workflow lease expired during renewal", {
              kind: "leaseLost"
            })
          }
          if (!settled.ok) throw settled.error
          lease = settled.value
          attemptResult = pending.ok
            ? { ok: true, outcome: pending.outcome }
            : { ok: false, error: pending.error }
          break
        }
        expiry.cancel()
        if (pending.kind === "expired") {
          throw new KvExecutionError("the exclusive workflow lease expired during renewal", {
            kind: "leaseLost"
          })
        }
        if (!pending.ok) throw pending.error
        lease = pending.value
      }
    } catch (error) {
      attemptResult = { ok: false, error }
    }
    // A failed step keeps its own error. Its release only frees the key early.
    if (!attemptResult.ok) {
      await guard.release().catch(() => undefined)
      throw attemptResult.error
    }
    const outcome = attemptResult.outcome
    const effect = outcome.kind === "done" ? outcome.done.agents : undefined
    let released: boolean
    try {
      // A renewal keeps the token, so the guard releases the lease it holds.
      released = await guard.release()
    } catch (error) {
      throw new StepError(error, effect)
    }
    if (!released) {
      throw new StepError(
        new KvExecutionError("the exclusive workflow lease was lost", { kind: "leaseLost" }),
        effect
      )
    }
    return outcome
  }

  // Scatter to every agent advertising the selector's skill and fold the
  // completed replies, one per line. A scatter in flight when the run
  // stopped is scattered again on resume.
  private async runAllCapable(
    source: AgentId,
    step: Step,
    target: Extract<Router, { readonly kind: "allCapable" }>,
    payload: Uint8Array,
    runId: ConversationId,
    attempt: Attempt,
    deadlineAt: number
  ): Promise<StepDone> {
    if (attempt.replyFrom === undefined) {
      await this.record(runId, {
        kind: "stepDispatched",
        label: step.label,
        attempt: attempt.number,
        replyFrom: []
      })
    }
    const report = await this.laser.scatterReport(
      source,
      target.selector,
      payload,
      this.route,
      REQUIRE_ALL,
      Math.max(0, deadlineAt - performance.now())
    )
    const completed = report.completed()
    if (completed.length === 0) throw new StepError(this.failed(runId, step, "no_completion"))
    const folded = new TextEncoder().encode(
      completed.map(({ reply }) => new TextDecoder().decode(agentMessageBody(reply))).join("\n")
    )
    return this.finish(
      runId,
      step,
      folded,
      0n,
      completed.map(({ agent }) => agent),
      deadlineAt
    )
  }

  // Where the reply to this attempt lands at or after. A fresh attempt
  // journals the reply topic's tail before it sends, a resumed one hands the
  // journaled positions to the contract.
  private async dispatchPoint(
    source: AgentId,
    runId: ConversationId,
    step: Step,
    attempt: Attempt
  ): Promise<readonly bigint[] | undefined> {
    if (attempt.replyFrom !== undefined) return attempt.replyFrom
    const hub = await this.laser[INTERNAL_REPLY_HUB](AgentTopic.Sessions, source)
    await this.record(runId, {
      kind: "stepDispatched",
      label: step.label,
      attempt: attempt.number,
      replyFrom: await hub.positions()
    })
    return undefined
  }

  private completion(
    runId: ConversationId,
    step: Step,
    contract: Contract
  ): { readonly output: Uint8Array; readonly tokens: bigint } {
    switch (contract.kind) {
      case "completed": {
        const usage = contract.reply.envelope?.usage
        return {
          output: agentMessageBody(contract.reply),
          tokens: usage === undefined ? 0n : usage.inputTokens + usage.outputTokens
        }
      }
      case "failed":
        throw new StepError(this.failed(runId, step, "failed"))
      case "notConsumed":
        throw new StepError(this.failed(runId, step, "not_consumed"))
      case "timedOut":
        throw new StepError(this.failed(runId, step, "timed_out"))
    }
  }

  // Verify the reply and journal the completion inside the step deadline. A
  // reply the step cannot accept still had its effect, so the failure carries
  // the agents that ran it for the rollback.
  private async finish(
    runId: ConversationId,
    step: Step,
    output: Uint8Array,
    tokens: bigint,
    agents: readonly AgentId[],
    deadlineAt: number
  ): Promise<StepDone> {
    try {
      if (step.verifier !== undefined) {
        const verdict = await withinDeadline(deadlineAt, Promise.resolve(step.verifier(output)))
        if (verdict.kind === "timedOut") throw this.failed(runId, step, "timed_out")
        if (!verdict.value) throw this.failed(runId, step, "unverified")
      }
      await this.record(runId, {
        kind: "stepCompleted",
        label: step.label,
        output,
        agents: agents.map((agent) => agent.asStr())
      })
    } catch (error) {
      throw new StepError(error, agents)
    }
    return { output, tokens, agents }
  }

  // Replay this run's whole journal, never a window. A freshly minted run id
  // has no records. A journal whose retained records do not begin with the
  // run's start, or an empty one while retention has removed the head of the
  // journal topic, cannot say which steps completed.
  private async replay(runId: ConversationId, resumed: boolean): Promise<Journal> {
    if (!resumed) return foldJournal([])
    const messages = await replayContext(
      {
        conversation: runId,
        acrossSubconversations: false,
        topics: [AgentTopic.WorkflowJournal],
        policy: new LastN(Number.MAX_SAFE_INTEGER),
        fromOffsets: new Map()
      },
      this.laser
    )
    const entries: JournalEntry[] = []
    for (const message of messages) {
      try {
        entries.push(decodeJournalEntry(message.payload))
      } catch {
        // Ignore malformed or foreign journal records.
      }
    }
    const journal = foldJournal(entries)
    if (!journal.started && (journal.records > 0 || (await this.journalTrimmed()))) {
      throw new JournalTruncatedError(runId.toString())
    }
    return journal
  }

  // Whether retention removed records from the head of any journal
  // partition.
  private async journalTrimmed(): Promise<boolean> {
    const stream = this.laser.defaultStream
    if (stream === undefined) return false
    const tails = await this.laser.topic(AgentTopic.WorkflowJournal).tailOffsets()
    const transport = this.laser[INTERNAL_TRANSPORT]()
    for (const partitionId of tails.keys()) {
      const first = await transport.pollMessages(
        stream,
        AgentTopic.WorkflowJournal,
        { kind: "single", partitionId, name: "laser-workflow-journal" },
        { kind: "first" },
        1,
        false
      )
      if ((first[0]?.offset ?? 0n) > 0n) return true
    }
    return false
  }

  private record(runId: ConversationId, entry: JournalEntry): Promise<void> {
    return this.laser.sendAgent(AgentTopic.WorkflowJournal, encodeJournalEntry(entry), {
      conversationId: runId
    })
  }

  // The compensations of the steps with an effect in reverse order, each to
  // the agents that ran its step under the step's principal requirement, or in
  // process for a local step, whose compensation result is ignored. A
  // compensation that completes is journaled, so a resume runs the undone
  // step again. One that does not is reported and the rollback goes on.
  private async compensate(
    effects: readonly Effect[],
    outputs: ReadonlyMap<string, Uint8Array>,
    runId: ConversationId
  ): Promise<void> {
    for (const effect of [...effects].reverse()) {
      const step = this.steps[effect.index]
      if (step?.compensation === undefined) continue
      const deadlineAt = performance.now() + step.deadlineMs
      let payload: Uint8Array
      try {
        const built = await withinDeadline(
          deadlineAt,
          Promise.resolve(step.compensation({ outputs }))
        )
        if (built.kind === "timedOut") {
          report(runId, step, "timed out building its compensation")
          continue
        }
        payload = stepBytes(built.value)
      } catch (error) {
        report(runId, step, "failed to build its compensation", error)
        continue
      }
      const route = step.target
      if (route === undefined) {
        await this.recordCompensated(runId, step)
        continue
      }
      const targets =
        effect.agents.length === 0
          ? [route]
          : effect.agents.map((agent) => compensationRoute(route, agent))
      let undone = true
      for (const [index, target] of targets.entries()) {
        try {
          const outcome = await this.laser
            .contract(target)
            .from(AgentId.new(this.name))
            .payload(payload)
            .inboxRoute(this.route)
            .deadline(Math.max(0, deadlineAt - performance.now()))
            .conversation(
              ConversationId.derive(`${runId.toString()}/${step.label}/compensate/${String(index)}`)
            )
            .parent(runId, runId)
            .send()
          if (outcome.kind !== "completed") {
            undone = false
            report(runId, step, `compensation ended ${outcome.kind}`)
          }
        } catch (error) {
          undone = false
          report(runId, step, "compensation failed", error)
        }
      }
      if (undone) await this.recordCompensated(runId, step)
    }
  }

  private recordCompensated(runId: ConversationId, step: Step): Promise<void> {
    return this.record(runId, { kind: "stepCompensated", label: step.label }).catch(
      (error: unknown) => {
        report(runId, step, "failed to journal its compensation", error)
      }
    )
  }

  private failed(runId: ConversationId, step: Step, reason: StepFailure): StepFailedError {
    return new StepFailedError(
      runId.toString(),
      step.label,
      reason,
      step.target === undefined ? "local step" : targetText(step.target),
      Math.trunc(step.deadlineMs)
    )
  }

  // The step's completion deadline, never past what the wall-clock budget
  // has left.
  private stepDeadline(step: Step, started: number): number {
    const ceiling = this.budgetValue.wallClockLimitMs
    return ceiling === undefined
      ? step.deadlineMs
      : Math.min(Math.max(0, ceiling - (performance.now() - started)), step.deadlineMs)
  }

  private checkBudget(invocations: number, tokensSpent: bigint, started: number): void {
    const invocationLimit = this.budgetValue.invocationLimit
    if (invocationLimit !== undefined && invocations > invocationLimit) {
      throw new BudgetExceededError(BigInt(invocationLimit), BigInt(invocations))
    }
    const tokenLimit = this.budgetValue.tokenLimit
    if (tokenLimit !== undefined && tokensSpent > tokenLimit) {
      throw new BudgetExceededError(tokenLimit, tokensSpent)
    }
    const wallClockLimitMs = this.budgetValue.wallClockLimitMs
    const elapsed = performance.now() - started
    if (wallClockLimitMs !== undefined && elapsed > wallClockLimitMs) {
      throw new BudgetExceededError(
        BigInt(Math.trunc(wallClockLimitMs * 1000)),
        BigInt(Math.trunc(elapsed * 1000))
      )
    }
  }
}

export class StepHandle {
  private constructor(
    private readonly owner: Workflow,
    private readonly current: Step
  ) {}

  /** @internal */
  static create(owner: Workflow, current: Step): StepHandle {
    return new StepHandle(owner, current)
  }

  after(label: string): this {
    this.current.after.push(label)
    return this
  }

  verifyWith(verifier: Verifier): this {
    this.current.verifier = verifier
    return this
  }

  exclusive(): this {
    this.current.exclusive = true
    return this
  }

  exclusiveIn(namespace: string): this {
    this.current.exclusive = true
    this.current.fenceNamespace = namespace
    return this
  }

  onTimeout(onTimeout: OnTimeout): this {
    this.current.onTimeout = onTimeout
    return this
  }

  /** Bound the step's build, dispatch, verification, and completion record
   * by `deadlineMs` (default 30000), never past the wall-clock budget left.
   * The command carries it, so a target that picks the task up later drops
   * it. An exclusive step that resumes a crashed run waits out that run's
   * 15 s lease inside this deadline. */
  deadline(deadlineMs: number): this {
    if (!Number.isFinite(deadlineMs) || deadlineMs < 0) {
      throw new InvalidError("step deadline must be a non-negative finite number")
    }
    this.current.deadlineMs = deadlineMs
    return this
  }

  compensateWith(compensation: StepFn): this {
    this.current.compensation = compensation
    return this
  }

  budget(budget: WorkflowBudget): this {
    this.owner.budget(budget)
    return this
  }

  inboxRoute(route: InboxRoute): this {
    this.owner.inboxRoute(route)
    return this
  }

  runId(runId: ConversationId): this {
    this.owner.runId(runId)
    return this
  }

  step(label: string, target: Router, build: StepFn): StepHandle {
    return this.owner.step(label, target, build)
  }

  stepLocal(label: string, run: StepFn): StepHandle {
    return this.owner.stepLocal(label, run)
  }

  run(options: { readonly signal?: AbortSignal } = {}): Promise<WorkflowOutcome> {
    return this.owner.run(options)
  }
}

/** One run of a workflow body, handed to the body of `Workflow.runWith`. */
export class WorkflowRun {
  private readonly unclaimed: Map<string, RecordedStep[]>
  private readonly attempting = new Set<string>()
  // The lowest call number of each step name that may still be free.
  private readonly nextCall = new Map<string, number>()
  private readonly running: Promise<unknown>[] = []
  private readonly recording: Promise<void>[] = []
  private readonly abortedSteps = new WeakSet<CancelledError>()
  private ended = false
  private closed = false

  private constructor(
    /** The run id, which resumes the same run when passed to `Workflow.runId`. */
    readonly runId: ConversationId,
    private readonly outputs: Map<string, Uint8Array>,
    inputs: ReadonlyMap<string, string>,
    private readonly checkpoint: () => Promise<void>,
    private readonly journal: (
      label: string,
      input: string | undefined,
      output: Uint8Array
    ) => Promise<void>
  ) {
    this.unclaimed = recordedSteps(outputs, inputs)
  }

  /** @internal */
  static create(
    runId: ConversationId,
    outputs: Map<string, Uint8Array>,
    inputs: ReadonlyMap<string, string>,
    checkpoint: () => Promise<void>,
    journal: (label: string, input: string | undefined, output: Uint8Array) => Promise<void>
  ): WorkflowRun {
    return new WorkflowRun(runId, outputs, inputs, checkpoint, journal)
  }

  /** Run `fn` as the step `label` and resolve to its value after a JSON round
   * trip. */
  step<Result>(
    label: string,
    fn: () => Result,
    options: { readonly signal?: AbortSignal } = {}
  ): Promise<JsonRoundTrip<Awaited<Result>>> {
    return this.track(this.attempt(label, undefined, fn, options.signal))
  }

  /** Run `fn` as a call of the step `label` identified by its arguments.
   * @internal */
  callStep<Result>(
    label: string,
    args: readonly unknown[],
    fn: () => Result
  ): Promise<JsonRoundTrip<Awaited<Result>>> {
    return this.track(this.attempt(label, argumentsDigest(args), fn, undefined))
  }

  /** Refuse new steps, give the ones still running a bounded time to finish,
   * then wait for the records already being written.
   * @internal */
  async settle(): Promise<void> {
    this.ended = true
    const grace = renewalTick(STEP_SETTLE_GRACE_MS)
    await Promise.race([Promise.allSettled(this.running), grace.promise])
    grace.cancel()
    this.closed = true
    await Promise.allSettled(this.recording)
  }

  /** Whether `error` stopped a step whose own signal aborted.
   * @internal */
  stepAborted(error: CancelledError): boolean {
    return this.abortedSteps.has(error)
  }

  private track<Value>(running: Promise<Value>): Promise<Value> {
    this.running.push(running)
    return running
  }

  private async attempt<Result>(
    name: string,
    input: string | undefined,
    fn: () => Result,
    signal: AbortSignal | undefined
  ): Promise<JsonRoundTrip<Awaited<Result>>> {
    try {
      return await this.runAttempt(name, input, fn, signal)
    } catch (error) {
      if (signal?.aborted === true && error instanceof CancelledError) {
        this.abortedSteps.add(error)
      }
      throw error
    }
  }

  // Runs synchronously up to the first await, so call order decides the labels.
  private async runAttempt<Result>(
    name: string,
    input: string | undefined,
    fn: () => Result,
    signal: AbortSignal | undefined
  ): Promise<JsonRoundTrip<Awaited<Result>>> {
    if (name.includes(STEP_CALL_SEPARATOR)) {
      throw new InvalidError(
        `workflow step name \`${name}\` cannot contain \`${STEP_CALL_SEPARATOR}\`, the journal numbers repeated calls with it`
      )
    }
    if (this.ended) {
      throw new InvalidError(`step \`${name}\` was called after its workflow run ended`)
    }
    throwIfAborted(signal, "workflow step")
    const stepCall = this.claim(name, input)
    if (stepCall.kind === "recorded") {
      return decodedOutput(stepCall.label, stepCall.output) as JsonRoundTrip<Awaited<Result>>
    }
    const { label, call } = stepCall
    this.attempting.add(label)
    try {
      await this.checkpoint()
      const encoded = encodedStepValue(name, await fn())
      if (this.closed) {
        throw new InvalidError(`step \`${name}\` finished after its workflow run ended`)
      }
      const recorded = this.journal(label, input, encoded).then(() => {
        this.outputs.set(label, encoded)
      })
      this.recording.push(recorded)
      await recorded
      return decodedOutput(label, encoded) as JsonRoundTrip<Awaited<Result>>
    } finally {
      this.attempting.delete(label)
      // A failed attempt gives its label back.
      if (!this.outputs.has(label)) {
        this.nextCall.set(name, Math.min(this.nextCall.get(name) ?? call, call))
      }
    }
  }

  // Equal arguments first. Else the labels `name`, `name#2`, `name#3` in
  // order, up to the first that holds an unclaimed record where a side has no
  // digest, which the call claims, or that is free, under which it is due.
  private claim(name: string, input: string | undefined): StepCall {
    const entries = this.unclaimed.get(name) ?? []
    let recorded = input === undefined ? undefined : entries.find((entry) => entry.input === input)
    if (recorded === undefined) {
      recorded = entries.find((entry) => entry.input === undefined || input === undefined)
      let call = this.nextCall.get(name) ?? 1
      for (; ; call += 1) {
        const label = call === 1 ? name : `${name}${STEP_CALL_SEPARATOR}${String(call)}`
        if (recorded !== undefined && compareCallLabels(recorded.label, label) <= 0) break
        if (this.outputs.has(label) || this.attempting.has(label)) continue
        this.nextCall.set(name, call + 1)
        return { kind: "due", label, call }
      }
      this.nextCall.set(name, call)
    }
    entries.splice(entries.indexOf(recorded), 1)
    return { kind: "recorded", label: recorded.label, output: recorded.output }
  }
}

/** The run the calling code executes in, if any.
 * @internal */
export function currentWorkflowRun(): WorkflowRun | undefined {
  return CURRENT_RUN.getStore()
}

// The journal's completed steps per step name, each name's entries in call order.
function recordedSteps(
  outputs: ReadonlyMap<string, Uint8Array>,
  inputs: ReadonlyMap<string, string>
): Map<string, RecordedStep[]> {
  const recorded = new Map<string, RecordedStep[]>()
  for (const [label, output] of outputs) {
    const separator = label.lastIndexOf(STEP_CALL_SEPARATOR)
    const digits = label.slice(separator + 1)
    const numbered = separator >= 0 && digits !== "1" && /^[1-9][0-9]*$/u.test(digits)
    const name = numbered ? label.slice(0, separator) : label
    const entries = recorded.get(name) ?? []
    entries.push({ label, output, input: inputs.get(label) })
    recorded.set(name, entries)
  }
  for (const entries of recorded.values()) {
    entries.sort((first, second) => compareCallLabels(first.label, second.label))
  }
  return recorded
}

// For one step name, decimal suffixes sort by length, then lexically.
function compareCallLabels(first: string, second: string): number {
  if (first.length !== second.length) return first.length - second.length
  if (first === second) return 0
  return first < second ? -1 : 1
}

// Arguments that are not plain JSON data leave the call without a digest, so it matches by order.
function argumentsDigest(args: readonly unknown[]): string | undefined {
  try {
    const canonical = canonicalJson(JSON.parse(JSON.stringify(args, plainJsonData)) as unknown)
    return bytesToHex(sha256(utf8(canonical))).slice(0, STEP_INPUT_DIGEST_CHARS)
  } catch {
    return undefined
  }
}

// JSON writes these as null or as another shape, which is another argument. An object drops a
// property JSON cannot hold, so only an array refuses one.
function plainJsonData(this: unknown, _key: string, value: unknown): unknown {
  const absent = value === undefined || typeof value === "function" || typeof value === "symbol"
  const prototype: unknown =
    typeof value === "object" && value !== null && !Array.isArray(value)
      ? Object.getPrototypeOf(value)
      : null
  if (
    (absent && Array.isArray(this)) ||
    (typeof value === "number" && !Number.isFinite(value)) ||
    (prototype !== null && prototype !== Object.prototype)
  ) {
    throw new InvalidError("arguments that are not plain JSON data have no digest")
  }
  return value
}

// Keys sort by code point and a whole number is written as its digits, so every
// client hashes equal arguments to the same digest.
function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${(value as readonly unknown[]).map((item) => canonicalJson(item)).join(",")}]`
  }
  if (typeof value === "number" && Number.isInteger(value)) return BigInt(value).toString()
  if (typeof value !== "object" || value === null) return JSON.stringify(value)
  const entries = Object.entries(value).sort(([first], [second]) =>
    Buffer.compare(Buffer.from(first), Buffer.from(second))
  )
  return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJson(item)}`).join(",")}}`
}

async function endFailed(session: Session, error: unknown): Promise<void> {
  try {
    if (error instanceof CancelledError) await session.cancel()
    else if (error instanceof BudgetExceededError) {
      await session.failOverBudget(exceededBreach(error.ceiling, error.spent))
    } else
      await session.fail({
        code: { kind: "known", name: "Internal" },
        message: error instanceof Error ? error.message : String(error),
        retryable: error instanceof StepFailedError && error.reason === "timed_out"
      })
  } catch {
    // The run's error wins over a lifecycle error.
  }
}

// A label enters `outputs` once, when its step completes, so key order is
// completion order.
function workflowOutcome<Value>(
  runId: ConversationId,
  outputs: ReadonlyMap<string, Uint8Array>,
  value: Value
): WorkflowOutcome<Value> {
  return {
    value,
    outputs,
    completedSteps: [...outputs.keys()],
    status: "completed",
    runId,
    text: (label) => {
      const output = outputs.get(label)
      return output === undefined ? undefined : decodeUtf8(output)
    },
    json: (label) => {
      const output = outputs.get(label)
      return output === undefined ? undefined : decodedOutput(label, output)
    }
  }
}

// `JSON.stringify` drops or rewrites these without a word, so each is refused.
function encodedStepValue(label: string, value: unknown): Uint8Array {
  const refused = (kind: string): InvalidError =>
    new InvalidError(
      `step \`${label}\` returned a value JSON cannot hold (${kind}). Return an object, array, string, number, boolean or null`
    )
  let encoded: unknown
  try {
    encoded = JSON.stringify(value, (_key, item: unknown) => {
      const kind = unholdableKind(item)
      if (kind !== undefined) throw refused(kind)
      return item
    })
  } catch (error) {
    if (error instanceof TypeError && error.message.includes("circular")) {
      throw refused("a circular structure")
    }
    throw error
  }
  if (typeof encoded !== "string") throw refused("undefined")
  return utf8(encoded)
}

function unholdableKind(item: unknown): string | undefined {
  if (typeof item === "bigint") return "a bigint"
  if (typeof item === "function") return "a function"
  if (typeof item === "symbol") return "a symbol"
  if (typeof item === "number" && !Number.isFinite(item)) return "a number that is not finite"
  if (item instanceof Map) return "a Map"
  if (item instanceof Set) return "a Set"
  return ArrayBuffer.isView(item) ? "a byte array" : undefined
}

function decodedOutput(label: string, output: Uint8Array): unknown {
  try {
    return JSON.parse(decodeUtf8(output))
  } catch (error) {
    throw new InvalidError(`step \`${label}\` has an output that is not JSON`, undefined, {
      cause: error
    })
  }
}

// Acquire the step's lease, waiting out a live lease of an earlier holder
// until `deadlineAt`. `undefined` when the deadline passed first.
async function acquireLease(
  kv: ReturnType<Laser["kv"]>,
  key: Uint8Array,
  holder: string,
  deadlineAt: number
): Promise<Lease | undefined> {
  for (;;) {
    try {
      return await kv.lease(key, holder, WORKFLOW_LEASE_TTL_MS)
    } catch (error) {
      const held =
        error instanceof KvExecutionError &&
        (error.detail as { readonly kind?: string } | undefined)?.kind === "leaseHeld"
      if (!held) throw error
      if (performance.now() + LEASE_RETRY_INTERVAL_MS >= deadlineAt) return undefined
      await renewalTick(LEASE_RETRY_INTERVAL_MS).promise
    }
  }
}

// The route a compensation takes to `agent`, keeping the principal the step's
// own route required.
function compensationRoute(target: Router, agent: AgentId): Router {
  const principal =
    target.kind === "toPrincipal"
      ? target.principal
      : target.kind === "toCapable" || target.kind === "allCapable"
        ? target.selector.principal
        : undefined
  return principal === undefined ? routeTo(agent) : routeToPrincipal(agent, principal)
}

function stepBytes(value: BytesLike | string): Uint8Array {
  return typeof value === "string" ? utf8(value) : ownedBytes(value)
}

// The step's route in words, for a failure message.
function targetText(target: Router): string {
  switch (target.kind) {
    case "to":
      return `agent \`${target.agent.asStr()}\``
    case "toPrincipal":
      return `agent \`${target.agent.asStr()}\` as principal ${target.principal.toString()}`
    case "toCapable":
      return `an agent with capability \`${target.selector.skill}\``
    case "allCapable":
      return `every agent with capability \`${target.selector.skill}\``
    case "broadcast":
      return "every agent"
  }
}

function report(runId: ConversationId, step: Step, what: string, error?: unknown): void {
  const cause = error instanceof Error ? `: ${error.message}` : ""
  warn(`Workflow run ${runId.toString()} step ${step.label} ${what}${cause}`)
}

async function withinDeadline<T>(
  deadlineAt: number,
  work: Promise<T>
): Promise<{ readonly kind: "done"; readonly value: T } | { readonly kind: "timedOut" }> {
  const timer = renewalTick(Math.max(0, deadlineAt - performance.now()))
  try {
    return await Promise.race([
      work.then((value) => ({ kind: "done" as const, value })),
      timer.promise.then(() => ({ kind: "timedOut" as const }))
    ])
  } finally {
    timer.cancel()
  }
}

function renewalDelayMs(grantedTtlMs: number): number {
  return Math.floor(grantedTtlMs / 2)
}

function renewalTick(milliseconds: number): { readonly promise: Promise<void>; cancel(): void } {
  let timer: ReturnType<typeof setTimeout> | undefined
  return {
    promise: new Promise((resolve) => {
      timer = setTimeout(resolve, milliseconds)
    }),
    cancel: () => {
      if (timer !== undefined) clearTimeout(timer)
    }
  }
}

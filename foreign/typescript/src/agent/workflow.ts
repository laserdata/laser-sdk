import { breachError, exceededBreach } from "./budget.js"
import { ownedBytes, type BytesLike } from "../client/bytes.js"
import {
  BudgetExceededError,
  CancelledError,
  HandlerConfigError,
  InvalidError,
  UnsupportedError
} from "../client/errors.js"
import type { Laser } from "../client/laser.js"
import { AgentTopic } from "../provenance/agent-topic.js"
import { AgentId, ConversationId } from "../types/ids.js"
import { decodeOne, encodeNamed, expectMap, field } from "../wire/cbor.js"
import { agentMessageBody } from "./reliable-consumer.js"
import type { Contract } from "./contract.js"
import type { Session } from "../session.js"
import { ADVERTISED_INBOX_ROUTE, type InboxRoute, type Router } from "./router.js"

const STEP_BUDGET_FLOOR_MS = 100
const DEFAULT_STEP_DEADLINE_MS = 30_000
const MAX_REASSIGNMENTS = 2
export const WORKFLOW_FENCE_NAMESPACE = "agdx.workflow.fence"
const WORKFLOW_LEASE_TTL_MS = 60_000

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
  readonly outputs: ReadonlyMap<string, Uint8Array>
}

export type StepFn = (context: StepContext) => BytesLike | Promise<BytesLike>
export type Verifier = (output: Uint8Array) => boolean | Promise<boolean>
export type OnTimeout = "fail" | "reassign"

interface Step {
  readonly label: string
  readonly target: Router
  readonly build: StepFn
  readonly after: string[]
  verifier?: Verifier
  exclusive: boolean
  fenceNamespace?: string
  onTimeout: OnTimeout
  compensation?: StepFn
}

export interface WorkflowOutcome {
  readonly outputs: ReadonlyMap<string, Uint8Array>
  readonly runId: ConversationId
}

interface CompletedDispatch {
  readonly kind: "completed"
  readonly body: Uint8Array
  readonly tokens: bigint
  readonly finalized?: true
}

type StepDispatch =
  CompletedDispatch | { readonly kind: "timedOut" } | { readonly kind: "notCompleted" }

function stepDispatch(contract: Contract): StepDispatch {
  if (contract.kind === "completed") {
    const usage = contract.reply.envelope?.usage
    return {
      kind: "completed",
      body: agentMessageBody(contract.reply),
      tokens: usage === undefined ? 0n : usage.inputTokens + usage.outputTokens
    }
  }
  return contract.kind === "timedOut" ? { kind: "timedOut" } : { kind: "notCompleted" }
}

function decodeJournalEntry(payload: Uint8Array): {
  readonly label: string
  readonly output: Uint8Array
} {
  const context = "workflow journal entry"
  const outer = expectMap(decodeOne(payload, context), context)
  const completed = field.requiredMap(outer, "StepCompleted", context)
  return {
    label: field.requiredString(completed, "label", context),
    output: field.requiredBytes(completed, "output", context)
  }
}

function encodeJournalEntry(label: string, output: Uint8Array): Uint8Array {
  return encodeNamed(
    new Map([
      [
        "StepCompleted",
        new Map<string, unknown>([
          ["label", label],
          ["output", output]
        ])
      ]
    ])
  )
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

export class Workflow {
  private budgetValue = WorkflowBudget.unlimited()
  private route: InboxRoute = ADVERTISED_INBOX_ROUTE
  private resumeId: ConversationId | undefined
  private readonly steps: Step[] = []

  private constructor(
    private readonly laser: Laser,
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
      onTimeout: "fail"
    }
    this.steps.push(step)
    return StepHandle.create(this, step)
  }

  /** Run the workflow to completion, returning each step's output. The run is
   * a session on `agent.sessions` whose id is the run id, and every step and
   * compensation is a child session of it. The run session starts before the
   * first step and ends by the outcome. Between steps the engine checks
   * `agent.control` for a cancel request, which compensates and throws
   * `CancelledError`, and then whether the run session is over its budget,
   * which compensates and throws `BudgetExceededError`. Any budget breach ends
   * the run session failed with reason `budget`. Aborting `signal` cancels
   * the run, as dropping the run future does in Rust. A lifecycle record that
   * fails to publish surfaces as the error, except when the run itself
   * already failed. */
  async run(options: { readonly signal?: AbortSignal } = {}): Promise<WorkflowOutcome> {
    const source = AgentId.new(this.name)
    const runId = this.resumeId ?? ConversationId.new()
    this.resumeId = runId
    let builder = this.laser.sessions().start().withId(runId).agent(source)
    const tokens = this.budgetValue.tokenLimit
    if (tokens !== undefined) builder = builder.budget({ tokens })
    const { session, lease } = await builder.begin()
    try {
      let outcome: WorkflowOutcome
      try {
        outcome = await this.execute(source, runId, session, options.signal)
      } catch (error) {
        try {
          if (error instanceof CancelledError) await session.cancel()
          else if (error instanceof BudgetExceededError) {
            await session.failOverBudget(exceededBreach(error.ceiling, error.spent))
          } else
            await session.fail({
              code: { kind: "known", name: "Internal" },
              message: error instanceof Error ? error.message : String(error),
              retryable: false
            })
        } catch {
          // The run's error wins over a lifecycle error.
        }
        throw error
      }
      await session.end()
      return outcome
    } finally {
      lease.release()
    }
  }

  private async execute(
    source: AgentId,
    runId: ConversationId,
    session: Session,
    signal: AbortSignal | undefined
  ): Promise<WorkflowOutcome> {
    const order = topologicalOrder(this.steps)
    const outputs = await this.replay(runId)
    const completed = order.filter((index) => outputs.has(this.steps[index]?.label ?? ""))
    const started = performance.now()
    let tokensSpent = 0n
    let invocations = 0

    for (const index of order) {
      const step = this.steps[index]
      if (step === undefined || outputs.has(step.label)) continue
      if (signal?.aborted === true) {
        await this.compensate(completed, outputs, runId)
        throw new CancelledError(`workflow \`${this.name}\` was cancelled`, {
          cause: signal.reason,
          run: runId.toString()
        })
      }
      if (await session.cancelRequested()) {
        await this.compensate(completed, outputs, runId)
        throw new CancelledError(`workflow run \`${runId.toString()}\` was cancelled`, {
          run: runId.toString()
        })
      }
      const breach = await session.budgetBreach()
      if (breach !== undefined) {
        await this.compensate(completed, outputs, runId)
        throw breachError(breach)
      }
      try {
        this.validateStep(step)
        this.checkDispatchFloor(started)
        invocations += 1
        this.checkBudget(invocations, tokensSpent, started)
        const payload = ownedBytes(await step.build({ outputs }))
        const dispatched = await this.dispatch(source, step, payload, started, runId)
        tokensSpent += dispatched.tokens
        if (dispatched.finalized !== true) {
          await this.finalizeStep(runId, step, dispatched.body)
        }
        outputs.set(step.label, dispatched.body)
        completed.push(index)
        this.checkBudget(invocations, tokensSpent, started)
      } catch (error) {
        await this.compensate(completed, outputs, runId)
        throw error
      }
    }
    return { outputs, runId }
  }

  private async finalizeStep(runId: ConversationId, step: Step, output: Uint8Array): Promise<void> {
    if (step.verifier !== undefined && !(await step.verifier(output))) {
      throw new HandlerConfigError(`workflow step \`${step.label}\` failed verification`)
    }
    await this.journal(runId, step.label, output)
  }

  private validateStep(step: Step): void {
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

  private checkDispatchFloor(started: number): void {
    const ceiling = this.budgetValue.wallClockLimitMs
    if (ceiling !== undefined && ceiling - (performance.now() - started) < STEP_BUDGET_FLOOR_MS) {
      throw new BudgetExceededError(
        BigInt(Math.trunc(ceiling * 1000)),
        BigInt(Math.trunc((performance.now() - started) * 1000))
      )
    }
  }

  private async dispatch(
    source: AgentId,
    step: Step,
    payload: Uint8Array,
    started: number,
    runId: ConversationId
  ): Promise<CompletedDispatch> {
    if (step.target.kind === "allCapable") {
      const bodies = await this.laser.scatter(
        source,
        step.target.selector,
        payload,
        this.route,
        this.stepDeadline(started)
      )
      if (bodies.length === 0) {
        throw new HandlerConfigError("no capable agent completed the all-capable step")
      }
      return {
        kind: "completed",
        body: new TextEncoder().encode(
          bodies.map((body) => new TextDecoder().decode(body)).join("\n")
        ),
        tokens: 0n
      }
    }
    if (step.exclusive) return this.dispatchExclusive(source, step, payload, started, runId)
    const outcome = stepDispatch(
      await this.laser
        .contract(step.target)
        .from(source)
        .payload(payload)
        .inboxRoute(this.route)
        .deadline(this.stepDeadline(started))
        // The step's child session id derives from the run, so a resumed run
        // reaches the same child instead of starting another.
        .conversation(ConversationId.derive(`${runId.toString()}/${step.label}/1`))
        .parent(runId, runId)
        .send()
    )
    if (outcome.kind !== "completed") {
      throw new HandlerConfigError("a workflow step did not complete")
    }
    return outcome
  }

  private async dispatchExclusive(
    source: AgentId,
    step: Step,
    payload: Uint8Array,
    started: number,
    runId: ConversationId
  ): Promise<CompletedDispatch> {
    const capabilities = await this.laser.capabilities()
    if (!capabilities.kv.fencedLeases) {
      throw new UnsupportedError("an exclusive step needs the plane's monotonic fence sequence")
    }
    const taskConversation = ConversationId.derive(`${runId.toString()}/${step.label}`)
    const attempts = step.onTimeout === "reassign" ? MAX_REASSIGNMENTS + 1 : 1
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const kv = this.laser.kv(step.fenceNamespace ?? WORKFLOW_FENCE_NAMESPACE)
      const key = new TextEncoder().encode(runId.toString())
      const holder = `workflow:${runId.toString()}:${String(attempt + 1)}`
      let lease = await kv.lease(key, holder, WORKFLOW_LEASE_TTL_MS)
      let leaseExpiresAt = performance.now() + leaseDurationMs(lease.grantedTtlMs)
      const contract = this.laser
        .contract(step.target)
        .from(source)
        .payload(payload)
        .inboxRoute(this.route)
        .deadline(this.stepDeadline(started))
        .fence(lease.token)
        .conversation(taskConversation)
        .parent(runId, runId)
        .send()
      const completed = contract.then(
        async (outcome) => {
          try {
            const dispatch = stepDispatch(outcome)
            if (dispatch.kind === "completed") {
              await this.finalizeStep(runId, step, dispatch.body)
            }
            return { kind: "contract" as const, ok: true as const, dispatch }
          } catch (error) {
            return { kind: "contract" as const, ok: false as const, error }
          }
        },
        (error: unknown) => ({ kind: "contract" as const, ok: false as const, error })
      )
      let attemptResult:
        | { readonly ok: true; readonly dispatch: StepDispatch }
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
              ? { ok: true, dispatch: next.dispatch }
              : { ok: false, error: next.error }
            break
          }
          const renewalBound = Math.min(
            leaseExpiresAt - performance.now(),
            this.stepDeadline(started)
          )
          if (renewalBound < 1) {
            throw new HandlerConfigError(
              "the exclusive workflow lease cannot be renewed before expiry"
            )
          }
          const renewal = kv.renewLease(key, holder, lease.token, WORKFLOW_LEASE_TTL_MS).then(
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
              throw new HandlerConfigError("the exclusive workflow lease expired during renewal")
            }
            if (!settled.ok) throw settled.error
            lease = settled.value
            attemptResult = pending.ok
              ? { ok: true, dispatch: pending.dispatch }
              : { ok: false, error: pending.error }
            break
          }
          expiry.cancel()
          if (pending.kind === "expired") {
            throw new HandlerConfigError("the exclusive workflow lease expired during renewal")
          }
          if (!pending.ok) throw pending.error
          lease = pending.value
          leaseExpiresAt = performance.now() + leaseDurationMs(lease.grantedTtlMs)
        }
      } catch (error) {
        attemptResult = { ok: false, error }
      }
      const released = await kv.release(key, holder, lease.token)
      if (!released) throw new HandlerConfigError("the exclusive workflow lease was lost")
      if (!attemptResult.ok) throw attemptResult.error
      const dispatch = attemptResult.dispatch
      if (dispatch.kind === "completed") return { ...dispatch, finalized: true }
      if (dispatch.kind !== "timedOut") break
    }
    throw new HandlerConfigError("an exclusive workflow step did not complete")
  }

  private async replay(runId: ConversationId): Promise<Map<string, Uint8Array>> {
    const messages = await this.laser
      .context(runId)
      .fetch([AgentTopic.WorkflowJournal], Number.MAX_SAFE_INTEGER)
    const outputs = new Map<string, Uint8Array>()
    for (const message of messages) {
      try {
        const entry = decodeJournalEntry(message.payload)
        outputs.set(entry.label, entry.output)
      } catch {
        // Ignore malformed or foreign journal records.
      }
    }
    return outputs
  }

  private journal(runId: ConversationId, label: string, output: Uint8Array): Promise<void> {
    return this.laser.sendAgent(AgentTopic.WorkflowJournal, encodeJournalEntry(label, output), {
      conversationId: runId
    })
  }

  private async compensate(
    completed: readonly number[],
    outputs: ReadonlyMap<string, Uint8Array>,
    runId: ConversationId
  ): Promise<void> {
    for (const index of [...completed].reverse()) {
      const step = this.steps[index]
      if (step?.compensation === undefined) continue
      try {
        const payload = ownedBytes(await step.compensation({ outputs }))
        await this.laser
          .contract(step.target)
          .from(AgentId.new(this.name))
          .payload(payload)
          .inboxRoute(this.route)
          .deadline(DEFAULT_STEP_DEADLINE_MS)
          .conversation(ConversationId.derive(`${runId.toString()}/${step.label}/compensate`))
          .parent(runId, runId)
          .send()
      } catch {
        // Compensation is best effort and cannot replace the original failure.
      }
    }
  }

  private stepDeadline(started: number): number {
    const ceiling = this.budgetValue.wallClockLimitMs
    return ceiling === undefined
      ? DEFAULT_STEP_DEADLINE_MS
      : Math.min(Math.max(0, ceiling - (performance.now() - started)), DEFAULT_STEP_DEADLINE_MS)
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

function renewalDelayMs(grantedTtlMs: number): number {
  return Math.floor(grantedTtlMs / 2)
}

function leaseDurationMs(grantedTtlMs: number): number {
  return Math.ceil(grantedTtlMs)
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

  run(options: { readonly signal?: AbortSignal } = {}): Promise<WorkflowOutcome> {
    return this.owner.run(options)
  }
}

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
import { markRun, type Contract } from "./contract.js"
import { ADVERTISED_INBOX_ROUTE, type InboxRoute, type Router } from "./router.js"

const STEP_BUDGET_FLOOR_MS = 100
const DEFAULT_STEP_DEADLINE_MS = 30_000
const MAX_REASSIGNMENTS = 2
const WORKFLOW_FENCE_NAMESPACE = "agdx.workflow.fence"
const WORKFLOW_LEASE_TTL_MICROS = 60_000_000n

export class Budget {
  private constructor(
    readonly tokenLimit?: bigint,
    readonly wallClockLimitMs?: number,
    readonly invocationLimit?: number
  ) {}

  static unlimited(): Budget {
    return new Budget()
  }

  static tokens(tokens: bigint): Budget {
    if (tokens < 0n) throw new InvalidError("token budget must be non-negative")
    return new Budget(tokens)
  }

  wallClock(milliseconds: number): Budget {
    if (!Number.isFinite(milliseconds) || milliseconds < 0) {
      throw new InvalidError("wall-clock budget must be a non-negative finite number")
    }
    return new Budget(this.tokenLimit, milliseconds, this.invocationLimit)
  }

  invocations(invocations: number): Budget {
    if (!Number.isSafeInteger(invocations) || invocations < 0) {
      throw new InvalidError("invocation budget must be a non-negative safe integer")
    }
    return new Budget(this.tokenLimit, this.wallClockLimitMs, invocations)
  }
}

export interface StepContext {
  readonly outputs: ReadonlyMap<string, Uint8Array>
}

export type StepFn = (context: StepContext) => BytesLike | Promise<BytesLike>
export type WorkflowVerifier = (output: Uint8Array) => boolean | Promise<boolean>
export type OnTimeout = "fail" | "reassign"

interface Step {
  readonly label: string
  readonly target: Router
  readonly build: StepFn
  readonly after: string[]
  verifier?: WorkflowVerifier
  exclusive: boolean
  fenceNamespace?: string
  onTimeout: OnTimeout
  compensation?: StepFn
}

export interface WorkflowOutcome {
  readonly outputs: ReadonlyMap<string, Uint8Array>
  readonly runId: ConversationId
}

export interface WorkflowRunOptions {
  readonly signal?: AbortSignal
}

interface CompletedDispatch {
  readonly kind: "completed"
  readonly body: Uint8Array
  readonly tokens: bigint
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
  private budgetValue = Budget.unlimited()
  private route: InboxRoute = ADVERTISED_INBOX_ROUTE
  private resumeId: ConversationId | undefined
  private readonly steps: Step[] = []
  private registerRun = false

  constructor(
    private readonly laser: Laser,
    readonly name: string
  ) {}

  budget(budget: Budget): this {
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

  registered(): this {
    this.registerRun = true
    return this
  }

  step(label: string, target: Router, build: StepFn): StepBuilder {
    const step: Step = {
      label,
      target,
      build,
      after: [],
      exclusive: false,
      onTimeout: "fail"
    }
    this.steps.push(step)
    return new StepBuilder(this, step)
  }

  async run(options: WorkflowRunOptions = {}): Promise<WorkflowOutcome> {
    if (this.registerRun && !(await this.laser.capabilities()).agentWorkflow) {
      throw new UnsupportedError(
        "a registered workflow requires a plane that serves the run registry"
      )
    }
    const source = AgentId.new(this.name)
    const runId = this.resumeId ?? ConversationId.new()
    let registeredRun: string | undefined
    if (this.registerRun) {
      registeredRun = (await this.laser.runs().submitWith(this.name, { runId: runId.toString() }))
        .runId
      await markRun(this.laser, source, runId, registeredRun, { kind: "known", name: "Working" })
    }
    try {
      const outcome = await this.execute(source, runId, registeredRun, options.signal)
      if (registeredRun !== undefined) {
        await markRun(this.laser, source, runId, registeredRun, {
          kind: "known",
          name: "Completed"
        })
      }
      return outcome
    } catch (error) {
      if (registeredRun !== undefined) {
        const cancelled = error instanceof CancelledError
        try {
          await markRun(
            this.laser,
            source,
            runId,
            registeredRun,
            { kind: "known", name: cancelled ? "Canceled" : "Failed" },
            cancelled ? undefined : error instanceof Error ? error.message : String(error)
          )
        } catch {
          // The workflow failure remains primary when terminal reporting also fails.
        }
      }
      throw error
    }
  }

  private async execute(
    source: AgentId,
    runId: ConversationId,
    registeredRun: string | undefined,
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
        await this.compensate(completed, outputs)
        throw new CancelledError(`workflow \`${this.name}\` was cancelled`, {
          cause: signal.reason
        })
      }
      if (
        registeredRun !== undefined &&
        (await this.laser.runs().status(registeredRun)).cancelRequested
      ) {
        await this.compensate(completed, outputs)
        throw new CancelledError(`workflow run \`${registeredRun}\` was cancelled`)
      }
      try {
        this.validateStep(step)
        this.checkDispatchFloor(started)
        invocations += 1
        this.checkBudget(invocations, tokensSpent, started)
        const payload = ownedBytes(await step.build({ outputs }))
        const dispatched = await this.dispatch(source, step, payload, started, runId)
        tokensSpent += dispatched.tokens
        if (step.verifier !== undefined && !(await step.verifier(dispatched.body))) {
          throw new HandlerConfigError(`workflow step \`${step.label}\` failed verification`)
        }
        await this.journal(runId, step.label, dispatched.body)
        outputs.set(step.label, dispatched.body)
        completed.push(index)
        this.checkBudget(invocations, tokensSpent, started)
      } catch (error) {
        await this.compensate(completed, outputs)
        throw error
      }
    }
    return { outputs, runId }
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
    if (!capabilities.kv.casFenced) {
      throw new UnsupportedError("an exclusive step needs the plane's monotonic fence sequence")
    }
    const taskConversation = ConversationId.derive(`${runId.toString()}/${step.label}`)
    const attempts = step.onTimeout === "reassign" ? MAX_REASSIGNMENTS + 1 : 1
    for (let attempt = 0; attempt < attempts; attempt += 1) {
      const lease = await this.laser
        .kv(step.fenceNamespace ?? WORKFLOW_FENCE_NAMESPACE)
        .lease(new TextEncoder().encode(runId.toString()), WORKFLOW_LEASE_TTL_MICROS)
      const outcome = stepDispatch(
        await this.laser
          .contract(step.target)
          .from(source)
          .payload(payload)
          .inboxRoute(this.route)
          .deadline(this.stepDeadline(started))
          .fence(lease.token)
          .conversation(taskConversation)
          .send()
      )
      if (outcome.kind === "completed") return outcome
      if (outcome.kind !== "timedOut") break
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
        // A malformed or foreign journal record does not prevent replaying later entries.
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
    outputs: ReadonlyMap<string, Uint8Array>
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
          .send()
      } catch {
        // Compensation is best-effort and never masks the original failure.
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

export class StepBuilder {
  constructor(
    private readonly owner: Workflow,
    private readonly current: Step
  ) {}

  after(label: string): this {
    this.current.after.push(label)
    return this
  }

  verifyWith(verifier: WorkflowVerifier): this {
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

  budget(budget: Budget): this {
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

  registered(): this {
    this.owner.registered()
    return this
  }

  step(label: string, target: Router, build: StepFn): StepBuilder {
    return this.owner.step(label, target, build)
  }

  done(): Workflow {
    return this.owner
  }

  run(options: WorkflowRunOptions = {}): Promise<WorkflowOutcome> {
    return this.owner.run(options)
  }
}

import type { Capabilities } from "../client/capabilities.js"
import { AgentWorkflowExecutionError, ProtocolError, UnsupportedError } from "../client/errors.js"
import { executeManaged, type ManagedTransport } from "../client/managed.js"
import type {
  AgentOutcome,
  AgentReply,
  AgentRunInfo,
  AgentRunState,
  RunBudget,
  RunPage
} from "../wire/agent-workflow.js"
import {
  AgentCancelCommand,
  AgentListCommand,
  AgentStatusCommand,
  AgentSubmitCommand,
  type ManagedCommand
} from "../wire/commands.js"
import type { PublishControl } from "./projections.js"

export type RunsBackend = ManagedTransport

function unexpected(op: string, outcome: AgentOutcome): ProtocolError {
  return new ProtocolError(`agent ${op}: unexpected outcome \`${outcome.kind}\``)
}

async function executeAgent<Request>(
  backend: RunsBackend,
  capabilities: Capabilities,
  command: ManagedCommand<Request, AgentReply>,
  request: Request
): Promise<AgentOutcome> {
  const reply = await executeManaged(backend, capabilities, command, request)
  if (reply.kind === "ok") return reply.outcome
  if (reply.kind === "err") {
    if (reply.error.kind === "unsupported") throw new UnsupportedError(reply.error.message)
    throw new AgentWorkflowExecutionError(`agent command failed: ${reply.error.kind}`, reply.error)
  }
  throw new ProtocolError(`agent: unrecognized reply variant \`${reply.tag}\``, {
    commandCode: command.code
  })
}

export interface SubmitOptions {
  readonly runId?: string
  readonly input?: Uint8Array
  readonly params?: ReadonlyMap<string, string>
  readonly budget?: RunBudget
}

// The managed run registry: submit a run to an agent or workflow, cancel
// it, read its state, or list runs. Build it with `Laser.runs()`. Gated on
// the `agentWorkflow` capability, so a plane that does not serve the band
// throws `UnsupportedError`.
export class Runs {
  constructor(
    private readonly backend: RunsBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly publishControl: PublishControl
  ) {}

  // Submit `input` to the agent `agentId`, returning the run's metadata.
  // The backend mints the run id by content-addressing the submit identity,
  // so a retried submit converges on the same run.
  async submit(agentId: string, input?: Uint8Array): Promise<AgentRunInfo> {
    return this.submitWith(agentId, { ...(input !== undefined ? { input } : {}) })
  }

  // Submit with a caller-assigned `runId`, explicit `params`, and optional
  // `input`, for full control over the run request. An absent `runId` lets
  // the backend mint one from the submit identity.
  async submitWith(agentId: string, options: SubmitOptions = {}): Promise<AgentRunInfo> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeAgent(this.backend, capabilities, AgentSubmitCommand, {
      agentId,
      params: options.params ?? new Map(),
      ...(options.runId !== undefined ? { runId: options.runId } : {}),
      ...(options.input !== undefined ? { input: options.input } : {}),
      ...(options.budget !== undefined ? { budget: options.budget } : {})
    })
    if (outcome.kind === "submitted") return outcome.run
    throw unexpected("submit", outcome)
  }

  // Submit with a multi-dimensional per-run `RunBudget`. A run that crosses
  // any cap is failed by the run governor.
  async submitBudgeted(
    agentId: string,
    budget: RunBudget,
    input?: Uint8Array
  ): Promise<AgentRunInfo> {
    return this.submitWith(agentId, { budget, ...(input !== undefined ? { input } : {}) })
  }

  // Record the cancel intent on `runId` and return the run. The engine
  // observes the intent at its next step boundary, so the state moves only
  // when the engine reports it.
  async cancel(runId: string): Promise<AgentRunInfo> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeAgent(this.backend, capabilities, AgentCancelCommand, { runId })
    if (outcome.kind === "cancelled") return outcome.run
    throw unexpected("cancel", outcome)
  }

  // Read the current state of `runId`.
  async status(runId: string): Promise<AgentRunInfo> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeAgent(this.backend, capabilities, AgentStatusCommand, { runId })
    if (outcome.kind === "status") return outcome.run
    throw unexpected("status", outcome)
  }

  // List runs, newest first. Fluent filters narrow the page, `.fetch()`
  // returns one `RunPage` whose `cursor` feeds the next call.
  list(): RunListRequest {
    return new RunListRequest(this.backend, this.getCapabilities)
  }

  // Register `stream`/`topic` as a run-status source: the deployment folds
  // the run-tagged agent records published there into the run registry. A
  // control command with 202-accepted semantics, idempotent by source.
  async registerSource(stream: string, topic: string): Promise<void> {
    await this.publishControl({ kind: "registerRunSource", source: { stream, topic } })
  }

  // Stop folding run-status records from `stream`/`topic`. Idempotent.
  async removeSource(stream: string, topic: string): Promise<void> {
    await this.publishControl({ kind: "removeRunSource", source: { stream, topic } })
  }
}

// Fluent builder for `Runs.list`, finished with `.fetch()`.
export class RunListRequest {
  private agentIdFilter: string | undefined
  private stateFilter: AgentRunState | undefined
  private limitValue: number | undefined
  private cursorValue: Uint8Array | undefined

  constructor(
    private readonly backend: RunsBackend,
    private readonly getCapabilities: () => Promise<Capabilities>
  ) {}

  // Keep only runs submitted to this agent.
  agent(agentId: string): this {
    this.agentIdFilter = agentId
    return this
  }

  // Keep only runs in this state.
  state(state: AgentRunState): this {
    this.stateFilter = state
    return this
  }

  // Page size, clamped server-side to the wire page cap.
  limit(limit: number): this {
    this.limitValue = limit
    return this
  }

  // Resume from a previous page's opaque cursor.
  cursor(cursor: Uint8Array): this {
    this.cursorValue = cursor
    return this
  }

  // Run the request, returning one page.
  async fetch(): Promise<RunPage> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeAgent(this.backend, capabilities, AgentListCommand, {
      ...(this.agentIdFilter !== undefined ? { agentId: this.agentIdFilter } : {}),
      ...(this.stateFilter !== undefined ? { state: this.stateFilter } : {}),
      ...(this.limitValue !== undefined ? { limit: this.limitValue } : {}),
      ...(this.cursorValue !== undefined ? { cursor: this.cursorValue } : {})
    })
    if (outcome.kind === "list") return outcome.page
    throw unexpected("list", outcome)
  }
}
